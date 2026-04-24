from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys

RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY_KEY = "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY"


def _prepare_fake_repo(tmp_path: Path, *, template_lines: list[str]) -> tuple[Path, Path]:
    root = Path(__file__).resolve().parents[1]
    source_script = root / "infra" / "digitalocean" / "bootstrap_ubuntu_2404.sh"
    assert source_script.exists(), f"expected script missing: {source_script}"

    repo_dir = tmp_path / "fake_repo"
    (repo_dir / ".git").mkdir(parents=True, exist_ok=True)
    (repo_dir / "requirements.txt").write_text("\n", encoding="utf-8")

    script_dir = repo_dir / "infra" / "digitalocean"
    script_dir.mkdir(parents=True, exist_ok=True)

    script_path = script_dir / source_script.name
    shutil.copy2(source_script, script_path)
    script_path.chmod(0o755)

    template_path = script_dir / "temperature-shadow.env.example"
    template_path.write_text("\n".join(template_lines) + "\n", encoding="utf-8")
    return repo_dir, script_path


def _write_shim(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _write_python3_shim(path: Path) -> None:
    real_python = sys.executable
    _write_shim(
        path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "",
                'if [[ "${1:-}" == "-m" && "${2:-}" == "venv" ]]; then',
                '  target="${3:-.venv}"',
                '  mkdir -p "$target/bin"',
                '  cat > "$target/bin/activate" <<\'ACT\'',
                'VIRTUAL_ENV="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"',
                "export VIRTUAL_ENV",
                'export PATH="$VIRTUAL_ENV/bin:$PATH"',
                "ACT",
                '  cat > "$target/bin/pip" <<\'PIP\'',
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "exit 0",
                "PIP",
                '  chmod +x "$target/bin/pip"',
                "  exit 0",
                "fi",
                "",
                f"exec {real_python!r} \"$@\"",
                "",
            ]
        ),
    )


def _write_sudo_shim(path: Path) -> None:
    _write_shim(
        path,
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'args=("$@")',
                "env_prefix=()",
                'while [[ ${#args[@]} -gt 0 && "${args[0]}" == *=* && "${args[0]}" != -* ]]; do',
                '  env_prefix+=("${args[0]}")',
                '  args=("${args[@]:1}")',
                "done",
                "",
                "if [[ ${#args[@]} -eq 0 ]]; then",
                "  exit 0",
                "fi",
                "",
                'if [[ "${args[0]}" == "chown" ]]; then',
                "  exit 0",
                "fi",
                "",
                "if [[ ${#env_prefix[@]} -gt 0 ]]; then",
                '  exec env "${env_prefix[@]}" "${args[@]}"',
                "fi",
                'exec "${args[@]}"',
                "",
            ]
        ),
    )


def _prepare_tool_shims(tmp_path: Path) -> Path:
    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)
    _write_shim(tool_dir / "apt-get", "#!/usr/bin/env bash\nexit 0\n")
    _write_shim(tool_dir / "pip", "#!/usr/bin/env bash\nexit 0\n")
    _write_python3_shim(tool_dir / "python3")
    _write_sudo_shim(tool_dir / "sudo")
    return tool_dir


def _run_bootstrap(
    *,
    script_path: Path,
    repo_dir: Path,
    tool_dir: Path,
    etc_dir: Path,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    env["BETBOT_ETC_DIR"] = str(etc_dir)
    return subprocess.run(
        ["/bin/bash", str(script_path), str(repo_dir)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(repo_dir),
        check=False,
    )


def _assert_summary_key_enabled_exactly_once(env_path: Path) -> None:
    text = env_path.read_text(encoding="utf-8")
    expected_line = f"{RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY_KEY}=1"
    assert expected_line in text
    assert text.count(f"{RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY_KEY}=") == 1


def test_fresh_bootstrap_creates_env_with_strict_effectiveness_summary_enabled(tmp_path: Path) -> None:
    repo_dir, script_path = _prepare_fake_repo(
        tmp_path,
        template_lines=[
            "BETBOT_ROOT=/tmp/old",
            "OUTPUT_DIR=/tmp/old/out",
            "BETBOT_ENV_FILE=/tmp/old/account.env",
            "OTHER_FLAG=ok",
        ],
    )
    tool_dir = _prepare_tool_shims(tmp_path)
    etc_dir = tmp_path / "etc" / "betbot"

    result = _run_bootstrap(
        script_path=script_path,
        repo_dir=repo_dir,
        tool_dir=tool_dir,
        etc_dir=etc_dir,
    )

    assert result.returncode == 0
    assert "Bootstrap complete for" in result.stdout

    env_path = etc_dir / "temperature-shadow.env"
    assert env_path.exists()
    _assert_summary_key_enabled_exactly_once(env_path)


def test_bootstrap_overrides_template_summary_key_zero_to_one(tmp_path: Path) -> None:
    repo_dir, script_path = _prepare_fake_repo(
        tmp_path,
        template_lines=[
            "BETBOT_ROOT=/tmp/old",
            "OUTPUT_DIR=/tmp/old/out",
            "BETBOT_ENV_FILE=/tmp/old/account.env",
            "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY=0",
            "OTHER_FLAG=ok",
        ],
    )
    tool_dir = _prepare_tool_shims(tmp_path)
    etc_dir = tmp_path / "etc" / "betbot"

    result = _run_bootstrap(
        script_path=script_path,
        repo_dir=repo_dir,
        tool_dir=tool_dir,
        etc_dir=etc_dir,
    )

    assert result.returncode == 0
    env_path = etc_dir / "temperature-shadow.env"
    assert env_path.exists()
    _assert_summary_key_enabled_exactly_once(env_path)
