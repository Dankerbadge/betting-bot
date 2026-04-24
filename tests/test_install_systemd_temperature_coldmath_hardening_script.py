from __future__ import annotations

import os
from pathlib import Path
import pwd
import shutil
import subprocess

STRICT_FAIL_KEY = "COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR"
REQUIRE_SUMMARY_KEY = "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY"


def _write_shim(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _prepare_script_bundle(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    root = Path(__file__).resolve().parents[1]
    src_script = root / "infra" / "digitalocean" / "install_systemd_temperature_coldmath_hardening.sh"
    assert src_script.exists(), f"expected script missing: {src_script}"

    repo_dir = tmp_path / "fake_repo"
    script_dir = repo_dir / "infra" / "digitalocean"
    script_dir.mkdir(parents=True, exist_ok=True)

    script_path = script_dir / src_script.name
    shutil.copy2(src_script, script_path)

    run_script = script_dir / "run_temperature_coldmath_hardening.sh"
    run_script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    run_script.chmod(0o755)

    remediation_script = script_dir / "set_coldmath_recovery_env_persistence_gate.sh"
    remediation_script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    remediation_script.chmod(0o755)

    env_file = tmp_path / "etc" / "betbot" / "temperature-shadow.env"
    env_file.parent.mkdir(parents=True, exist_ok=True)

    service_file = tmp_path / "systemd" / "betbot-temperature-coldmath-hardening.service"
    timer_file = tmp_path / "systemd" / "betbot-temperature-coldmath-hardening.timer"
    service_file.parent.mkdir(parents=True, exist_ok=True)

    script_text = script_path.read_text(encoding="utf-8")
    script_text = script_text.replace(
        'ENV_FILE="/etc/betbot/temperature-shadow.env"',
        f'ENV_FILE="{env_file}"',
    )
    script_text = script_text.replace(
        'SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"',
        f'SERVICE_FILE="{service_file}"',
    )
    script_text = script_text.replace(
        'TIMER_FILE="/etc/systemd/system/${TIMER_NAME}"',
        f'TIMER_FILE="{timer_file}"',
    )
    script_path.write_text(script_text, encoding="utf-8")
    script_path.chmod(0o755)

    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)

    _write_shim(tool_dir / "sudo", "#!/bin/sh\nexec \"$@\"\n")
    _write_shim(
        tool_dir / "systemctl",
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'args=("$@")',
                'while [[ ${#args[@]} -gt 0 && "${args[0]}" == -* ]]; do',
                '  args=("${args[@]:1}")',
                "done",
                'cmd="${args[0]:-}"',
                'if [[ "$cmd" == "status" ]]; then',
                '  echo "stub status"',
                "fi",
                "exit 0",
                "",
            ]
        ),
    )
    _write_shim(tool_dir / "journalctl", "#!/bin/sh\necho \"stub journal\"\nexit 0\n")

    return script_path, tool_dir, env_file, repo_dir


def _write_env_file(env_file: Path, lines: list[str]) -> None:
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_installer(script_path: Path, tool_dir: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    env["BETBOT_DEPLOY_USER"] = pwd.getpwuid(os.getuid()).pw_name
    return subprocess.run(
        ["/bin/bash", str(script_path)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(script_path.parent),
        check=False,
    )


def test_installer_reports_success_when_strict_recovery_gates_enabled(tmp_path: Path) -> None:
    script_path, tool_dir, env_file, _ = _prepare_script_bundle(tmp_path)
    _write_env_file(
        env_file,
        [
            "OUTPUT_DIR=/tmp/out",
            f"{STRICT_FAIL_KEY}=true",
            f"{REQUIRE_SUMMARY_KEY}=on",
        ],
    )

    result = _run_installer(script_path, tool_dir)
    combined_output = f"{result.stdout}\n{result.stderr}"

    assert result.returncode == 0
    assert "Strict recovery gates enabled" in result.stdout
    assert "WARNING:" not in combined_output


def test_installer_warns_and_prints_remediation_when_recovery_gate_keys_drift(tmp_path: Path) -> None:
    script_path, tool_dir, env_file, repo_dir = _prepare_script_bundle(tmp_path)
    _write_env_file(
        env_file,
        [
            "OUTPUT_DIR=/tmp/out",
            "COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ONERROR=true",
            "RECOVERY_REQUIRE_EFFECTIVE_SUMMARY=true",
        ],
    )

    result = _run_installer(script_path, tool_dir)
    combined_output = f"{result.stdout}\n{result.stderr}"
    remediation_command = (
        f"bash {repo_dir}/infra/digitalocean/set_coldmath_recovery_env_persistence_gate.sh "
        f"--enable {env_file}"
    )

    assert result.returncode == 0
    assert "WARNING:" in combined_output
    assert STRICT_FAIL_KEY in combined_output
    assert REQUIRE_SUMMARY_KEY in combined_output
    assert remediation_command in combined_output
