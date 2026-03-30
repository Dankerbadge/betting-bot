#!/usr/bin/env python3
"""Fail if sensitive local files are tracked in git."""

from __future__ import annotations

import fnmatch
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


def _candidate_paths() -> list[str]:
    tracked = set(_git_file_list("ls-files"))
    staged = set(_git_file_list("diff", "--cached", "--name-only"))
    return sorted(tracked | staged)


def main() -> int:
    candidates = _candidate_paths()
    if not candidates:
        print("No git tracked or staged files found (or git unavailable); skipping secret path check.")
        return 0

    offending = find_offending_paths(candidates)
    if not offending:
        print("No tracked secret-path violations found.")
        return 0

    print("Tracked secret-path violations detected:", file=sys.stderr)
    for path in offending:
        print(f"- {path}", file=sys.stderr)
    print(
        "Remove these files from git tracking and keep secrets in ignored paths.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
