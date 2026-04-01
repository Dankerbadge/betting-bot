#!/usr/bin/env python3
"""Fail if sensitive local files are tracked in git."""

from __future__ import annotations

import fnmatch
import pathlib
import re
import subprocess
import sys
from collections.abc import Iterable

FORBIDDEN_GLOBS = (
    "data/research/account_onboarding.local.env",
    ".secrets/*",
    "*.pem",
    "*.p8",
    "*key*.txt",
)

ALLOWLIST = {
    ".secrets/README.md",
}

MAX_SCAN_BYTES = 1_000_000
PRIVATE_KEY_BLOCK_RE = re.compile(
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----\s+"
    r"[A-Za-z0-9+/=\s]+?"
    r"-----END [A-Z0-9 ]*PRIVATE KEY-----",
    re.MULTILINE,
)


def _git_file_list(*args: str) -> list[str]:
    try:
        result = subprocess.run(
            ["git", *args, "-z"],
            check=True,
            capture_output=True,
            text=False,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [item.decode("utf-8") for item in result.stdout.split(b"\x00") if item]


def _is_forbidden(path: str) -> bool:
    if path in ALLOWLIST:
        return False
    return any(fnmatch.fnmatch(path, pattern) for pattern in FORBIDDEN_GLOBS)


def find_offending_paths(paths: Iterable[str]) -> list[str]:
    return sorted({path for path in paths if _is_forbidden(path)})


def _path_contains_private_key_block(root: pathlib.Path, path: str) -> bool:
    candidate = root / path
    if not candidate.is_file():
        return False
    try:
        with candidate.open("rb") as handle:
            raw = handle.read(MAX_SCAN_BYTES + 1)
    except OSError:
        return False
    if len(raw) > MAX_SCAN_BYTES or b"\x00" in raw:
        return False
    text = raw.decode("utf-8", errors="ignore")
    return PRIVATE_KEY_BLOCK_RE.search(text) is not None


def find_private_key_material_paths(
    paths: Iterable[str],
    *,
    root: pathlib.Path | None = None,
) -> list[str]:
    search_root = root or pathlib.Path.cwd()
    return sorted(
        {
            path
            for path in paths
            if path not in ALLOWLIST and _path_contains_private_key_block(search_root, path)
        }
    )


def _candidate_paths() -> list[str]:
    tracked = set(_git_file_list("ls-files"))
    staged = set(_git_file_list("diff", "--cached", "--name-only"))
    return sorted(tracked | staged)


def main() -> int:
    candidates = _candidate_paths()
    if not candidates:
        print("No git tracked or staged files found (or git unavailable); skipping secret path check.")
        return 0

    path_offending = find_offending_paths(candidates)
    content_offending = find_private_key_material_paths(candidates)
    if not path_offending and not content_offending:
        print("No tracked secret-path or private-key-content violations found.")
        return 0

    if path_offending:
        print("Tracked secret-path violations detected:", file=sys.stderr)
        for path in path_offending:
            print(f"- {path}", file=sys.stderr)
    if content_offending:
        print("Tracked private-key content detected:", file=sys.stderr)
        for path in content_offending:
            print(f"- {path}", file=sys.stderr)
    print("Remove these files from git tracking and keep secrets in ignored paths.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
