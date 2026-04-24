from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

STRICT_FAIL_KEY = "COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR"
REQUIRE_SUMMARY_KEY = "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY"
MANAGED_KEYS = (STRICT_FAIL_KEY, REQUIRE_SUMMARY_KEY)


def _prepare_script_bundle(tmp_path: Path) -> tuple[Path, Path]:
    root = Path(__file__).resolve().parents[1]
    src_script = root / "infra" / "digitalocean" / "set_coldmath_recovery_env_persistence_gate.sh"
    assert src_script.exists(), f"expected script missing: {src_script}"

    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    script_path = bundle_dir / src_script.name
    shutil.copy2(src_script, script_path)
    script_path.chmod(0o755)

    invocation_log = bundle_dir / "check_invocations.log"
    _write_fake_check_script(
        bundle_dir / "check_temperature_shadow.sh",
        invocation_log=invocation_log,
        fail_code_env_var="FAKE_CHECK_TEMPERATURE_SHADOW_EXIT_CODE",
    )
    _write_fake_check_script(
        bundle_dir / "check_temperature_shadow_quick.sh",
        invocation_log=invocation_log,
        fail_code_env_var="FAKE_CHECK_TEMPERATURE_SHADOW_QUICK_EXIT_CODE",
    )
    return script_path, invocation_log


def _write_fake_check_script(
    script_path: Path,
    *,
    invocation_log: Path,
    fail_code_env_var: str,
) -> None:
    script_path.write_text(
        (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            f'echo "$(basename "$0")|$*" >> "{invocation_log}"\n'
            f'exit "${{{fail_code_env_var}:-0}}"\n'
        ),
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _write_env_file(env_path: Path, *, current_value: str) -> None:
    env_path.write_text(
        "\n".join(
            [
                "OUTPUT_DIR=/tmp/out",
                f"{STRICT_FAIL_KEY}={current_value}",
                f"{REQUIRE_SUMMARY_KEY}={current_value}",
                "ANOTHER_KEY=1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _run_script(
    script_path: Path,
    args: list[str],
    *,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["/bin/bash", str(script_path), *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(script_path.parent),
        check=False,
    )


def _read_invocations(invocation_log: Path) -> list[str]:
    if not invocation_log.exists():
        return []
    return [
        line.strip()
        for line in invocation_log.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _assert_env_values(env_path: Path, expected_value: str) -> None:
    text = env_path.read_text(encoding="utf-8")
    for key in MANAGED_KEYS:
        expected_line = f"{key}={expected_value}"
        assert expected_line in text
        assert text.count(f"{key}=") == 1


def _assert_backup_created(env_path: Path) -> None:
    backups = sorted(env_path.parent.glob(f"{env_path.name}.bak_*"))
    assert backups, "expected timestamped backup to be created"


def test_default_enable_sets_env_and_runs_both_strict_checks(tmp_path: Path) -> None:
    script_path, invocation_log = _prepare_script_bundle(tmp_path)
    env_path = tmp_path / "temperature-shadow.env"
    _write_env_file(env_path, current_value="0")

    result = _run_script(script_path, [str(env_path)])

    assert result.returncode == 0
    _assert_env_values(env_path, "1")
    _assert_backup_created(env_path)
    assert f"{STRICT_FAIL_KEY}=1" in result.stdout
    assert f"{REQUIRE_SUMMARY_KEY}=1" in result.stdout
    assert _read_invocations(invocation_log) == [
        f"check_temperature_shadow.sh|--strict --env {env_path}",
        f"check_temperature_shadow_quick.sh|--strict --env {env_path}",
    ]


def test_disable_with_skip_checks_sets_zero_and_skips_strict_checks(tmp_path: Path) -> None:
    script_path, invocation_log = _prepare_script_bundle(tmp_path)
    env_path = tmp_path / "temperature-shadow.env"
    _write_env_file(env_path, current_value="1")

    result = _run_script(script_path, ["--disable", "--skip-checks", str(env_path)])

    assert result.returncode == 0
    _assert_env_values(env_path, "0")
    _assert_backup_created(env_path)
    assert f"{STRICT_FAIL_KEY}=0" in result.stdout
    assert f"{REQUIRE_SUMMARY_KEY}=0" in result.stdout
    assert _read_invocations(invocation_log) == []


def test_check_failure_returns_nonzero_while_env_update_persists(tmp_path: Path) -> None:
    script_path, invocation_log = _prepare_script_bundle(tmp_path)
    env_path = tmp_path / "temperature-shadow.env"
    _write_env_file(env_path, current_value="0")

    result = _run_script(
        script_path,
        [str(env_path)],
        extra_env={"FAKE_CHECK_TEMPERATURE_SHADOW_QUICK_EXIT_CODE": "7"},
    )

    assert result.returncode != 0
    _assert_env_values(env_path, "1")
    _assert_backup_created(env_path)
    assert _read_invocations(invocation_log) == [
        f"check_temperature_shadow.sh|--strict --env {env_path}",
        f"check_temperature_shadow_quick.sh|--strict --env {env_path}",
    ]


def test_missing_env_file_returns_nonzero_and_message(tmp_path: Path) -> None:
    script_path, invocation_log = _prepare_script_bundle(tmp_path)
    missing_env = tmp_path / "does-not-exist.env"

    result = _run_script(script_path, [str(missing_env)])

    assert result.returncode != 0
    combined_output = f"{result.stdout}\n{result.stderr}".lower()
    assert "env" in combined_output
    assert "missing" in combined_output or "not found" in combined_output
    assert _read_invocations(invocation_log) == []
