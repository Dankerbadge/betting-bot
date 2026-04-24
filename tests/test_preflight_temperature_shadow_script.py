from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

STRICT_FAIL_KEY = "COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR"
REQUIRE_SUMMARY_KEY = "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY"


def _write_shim(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _prepare_script_bundle(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    root = Path(__file__).resolve().parents[1]
    src_script = root / "infra" / "digitalocean" / "preflight_temperature_shadow.sh"
    assert src_script.exists(), f"expected script missing: {src_script}"

    repo_dir = tmp_path / "fake_repo"
    script_dir = repo_dir / "infra" / "digitalocean"
    script_dir.mkdir(parents=True, exist_ok=True)

    script_path = script_dir / src_script.name
    shutil.copy2(src_script, script_path)
    script_path.chmod(0o755)

    (repo_dir / "betbot").mkdir(parents=True, exist_ok=True)

    required_scripts = [
        "run_temperature_shadow_loop.sh",
        "run_temperature_coldmath_hardening.sh",
        "run_temperature_pipeline_recovery.sh",
        "run_temperature_recovery_chaos_check.sh",
        "run_temperature_stale_metrics_drill.sh",
        "set_coldmath_recovery_env_persistence_gate.sh",
    ]
    for script_name in required_scripts:
        _write_shim(script_dir / script_name, "#!/usr/bin/env bash\nexit 0\n")

    python_bin = repo_dir / ".venv" / "bin" / "python"
    python_bin.parent.mkdir(parents=True, exist_ok=True)
    _write_shim(python_bin, "#!/usr/bin/env bash\nexit 0\n")

    env_file = tmp_path / "etc" / "betbot" / "temperature-shadow.env"
    env_file.parent.mkdir(parents=True, exist_ok=True)

    betbot_env_file = tmp_path / "etc" / "betbot" / "betbot.env"
    betbot_env_file.parent.mkdir(parents=True, exist_ok=True)
    betbot_env_file.write_text("DUMMY=1\n", encoding="utf-8")

    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)
    _write_shim(tool_dir / "curl", "#!/usr/bin/env bash\nexit 0\n")
    _write_shim(
        tool_dir / "systemctl",
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'cmd="${1:-}"',
                'if [[ "$cmd" == "is-enabled" ]]; then',
                '  echo "enabled"',
                "  exit 0",
                "fi",
                'if [[ "$cmd" == "is-active" ]]; then',
                '  echo "active"',
                "  exit 0",
                "fi",
                "exit 0",
                "",
            ]
        ),
    )

    return script_path, tool_dir, env_file, repo_dir, betbot_env_file


def _write_env_file(
    *,
    env_file: Path,
    repo_dir: Path,
    betbot_env_file: Path,
    extra_lines: list[str],
) -> None:
    lines = [
        f"BETBOT_ROOT={repo_dir}",
        f"OUTPUT_DIR={repo_dir / 'output'}",
        f"BETBOT_ENV_FILE={betbot_env_file}",
        "ALPHA_WORKER_ENABLED=0",
        "COLDMATH_HARDENING_ENABLED=0",
        "ALLOW_LIVE_ORDERS=0",
        "ADAPTIVE_MAX_MARKETS_ENABLED=1",
    ]
    lines.extend(extra_lines)
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_preflight(script_path: Path, env_file: Path, tool_dir: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    return subprocess.run(
        ["/bin/bash", str(script_path), str(env_file)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(script_path.parent),
        check=False,
    )


def test_preflight_reports_pass_when_strict_recovery_gates_enabled(tmp_path: Path) -> None:
    script_path, tool_dir, env_file, repo_dir, betbot_env_file = _prepare_script_bundle(tmp_path)
    _write_env_file(
        env_file=env_file,
        repo_dir=repo_dir,
        betbot_env_file=betbot_env_file,
        extra_lines=[
            f"{STRICT_FAIL_KEY}=true",
            f"{REQUIRE_SUMMARY_KEY}=ON",
        ],
    )

    result = _run_preflight(script_path, env_file, tool_dir)
    combined_output = f"{result.stdout}\n{result.stderr}"

    assert result.returncode == 0
    assert "PASS: strict recovery gates enabled:" in result.stdout
    assert f"WARN: {STRICT_FAIL_KEY}" not in combined_output
    assert f"WARN: {REQUIRE_SUMMARY_KEY}" not in combined_output
    assert "WARN: strict recovery gate remediation:" not in combined_output


def test_preflight_warns_and_shows_remediation_when_gate_missing_or_disabled(tmp_path: Path) -> None:
    script_path, tool_dir, env_file, repo_dir, betbot_env_file = _prepare_script_bundle(tmp_path)
    _write_env_file(
        env_file=env_file,
        repo_dir=repo_dir,
        betbot_env_file=betbot_env_file,
        extra_lines=[
            f"{STRICT_FAIL_KEY}=0",
        ],
    )

    result = _run_preflight(script_path, env_file, tool_dir)
    combined_output = f"{result.stdout}\n{result.stderr}"
    remediation_command = (
        f"bash {repo_dir}/infra/digitalocean/set_coldmath_recovery_env_persistence_gate.sh "
        f"--enable {env_file}"
    )

    assert result.returncode == 0
    assert f"WARN: {STRICT_FAIL_KEY} is disabled" in combined_output
    assert f"WARN: {REQUIRE_SUMMARY_KEY} is missing" in combined_output
    assert remediation_command in combined_output


def test_preflight_treats_invalid_gate_value_as_disabled(tmp_path: Path) -> None:
    script_path, tool_dir, env_file, repo_dir, betbot_env_file = _prepare_script_bundle(tmp_path)
    _write_env_file(
        env_file=env_file,
        repo_dir=repo_dir,
        betbot_env_file=betbot_env_file,
        extra_lines=[
            f"{STRICT_FAIL_KEY}=definitely",
            f"{REQUIRE_SUMMARY_KEY}=yes",
        ],
    )

    result = _run_preflight(script_path, env_file, tool_dir)
    combined_output = f"{result.stdout}\n{result.stderr}"
    remediation_command = (
        f"bash {repo_dir}/infra/digitalocean/set_coldmath_recovery_env_persistence_gate.sh "
        f"--enable {env_file}"
    )

    assert result.returncode == 0
    assert f"WARN: {STRICT_FAIL_KEY} has invalid value 'definitely'" in combined_output
    assert f"WARN: {REQUIRE_SUMMARY_KEY}" not in combined_output
    assert remediation_command in combined_output
