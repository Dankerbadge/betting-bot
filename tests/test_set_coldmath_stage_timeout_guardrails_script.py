from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess


TARGET_KEYS = [
    "COLDMATH_STAGE_TIMEOUT_SECONDS",
    "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS",
    "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS",
    "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS",
    "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS",
    "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS",
]


def _prepare_script_bundle(tmp_path: Path) -> Path:
    root = Path(__file__).resolve().parents[1]
    src_script = root / "infra" / "digitalocean" / "set_coldmath_stage_timeout_guardrails.sh"
    assert src_script.exists(), f"expected script missing: {src_script}"

    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    script_path = bundle_dir / src_script.name
    shutil.copy2(src_script, script_path)
    script_path.chmod(0o755)
    return script_path


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


def _write_env_file(env_path: Path, *, values: dict[str, str] | None = None) -> None:
    lines = [
        "OUTPUT_DIR=/tmp/out",
        "ANOTHER_KEY=1",
        "",
    ]
    if values:
        for key, value in values.items():
            lines.insert(1, f"{key}={value}")
    env_path.write_text("\n".join(lines), encoding="utf-8")


def _read_env_text(env_path: Path) -> str:
    return env_path.read_text(encoding="utf-8")


def _assert_exact_key_values(env_text: str, expected: dict[str, str]) -> None:
    for key, value in expected.items():
        expected_line = f"{key}={value}"
        assert expected_line in env_text
        assert env_text.count(f"{key}=") == 1


def _assert_backup_created(env_path: Path) -> None:
    backups = sorted(env_path.parent.glob(f"{env_path.name}.bak_*"))
    assert backups, "expected timestamped backup to be created"


def test_default_invocation_updates_env_with_expected_defaults_and_creates_backup(
    tmp_path: Path,
) -> None:
    script_path = _prepare_script_bundle(tmp_path)
    env_path = tmp_path / "temperature-shadow.env"
    _write_env_file(
        env_path,
        values={
            "COLDMATH_STAGE_TIMEOUT_SECONDS": "9",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS": "8",
        },
    )

    result = _run_script(script_path, [str(env_path)])

    assert result.returncode == 0
    assert "updated:" in result.stdout
    assert "backup:" in result.stdout
    env_text = _read_env_text(env_path)
    _assert_exact_key_values(
        env_text,
        {
            "COLDMATH_STAGE_TIMEOUT_SECONDS": "0",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS": "0",
            "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS": "0",
            "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS": "0",
            "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS": "0",
            "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS": "0",
        },
    )
    assert "ANOTHER_KEY=1" in env_text
    _assert_backup_created(env_path)


def test_custom_per_stage_values_are_applied_exactly_once(tmp_path: Path) -> None:
    script_path = _prepare_script_bundle(tmp_path)
    env_path = tmp_path / "temperature-shadow.env"
    _write_env_file(
        env_path,
        values={
            "COLDMATH_STAGE_TIMEOUT_SECONDS": "99",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS": "98",
            "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS": "97",
            "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS": "96",
            "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS": "95",
            "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS": "94",
        },
    )

    result = _run_script(
        script_path,
        [
            "--global-seconds",
            "11",
            "--snapshot-seconds",
            "2",
            "--market-ingest-seconds",
            "3",
            "--advisor-seconds",
            "4",
            "--loop-seconds",
            "5",
            "--campaign-seconds",
            "6",
            str(env_path),
        ],
    )

    assert result.returncode == 0
    env_text = _read_env_text(env_path)
    _assert_exact_key_values(
        env_text,
        {
            "COLDMATH_STAGE_TIMEOUT_SECONDS": "11",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS": "2",
            "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS": "3",
            "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS": "4",
            "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS": "5",
            "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS": "6",
        },
    )
    _assert_backup_created(env_path)


def test_disable_all_sets_all_values_to_zero(tmp_path: Path) -> None:
    script_path = _prepare_script_bundle(tmp_path)
    env_path = tmp_path / "temperature-shadow.env"
    _write_env_file(
        env_path,
        values={
            "COLDMATH_STAGE_TIMEOUT_SECONDS": "9",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS": "8",
            "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS": "7",
            "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS": "6",
            "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS": "5",
            "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS": "4",
        },
    )

    result = _run_script(script_path, ["--disable-all", str(env_path)])

    assert result.returncode == 0
    env_text = _read_env_text(env_path)
    _assert_exact_key_values(
        env_text,
        {
            "COLDMATH_STAGE_TIMEOUT_SECONDS": "0",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS": "0",
            "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS": "0",
            "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS": "0",
            "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS": "0",
            "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS": "0",
        },
    )
    _assert_backup_created(env_path)


def test_invalid_numeric_arg_returns_nonzero_and_does_not_modify_env(
    tmp_path: Path,
) -> None:
    script_path = _prepare_script_bundle(tmp_path)
    env_path = tmp_path / "temperature-shadow.env"
    original_text = "\n".join(
        [
            "OUTPUT_DIR=/tmp/out",
            "COLDMATH_STAGE_TIMEOUT_SECONDS=41",
            "ANOTHER_KEY=1",
            "",
        ]
    )
    env_path.write_text(original_text, encoding="utf-8")

    result = _run_script(
        script_path,
        ["--global-seconds", "abc", str(env_path)],
    )

    assert result.returncode != 0
    combined_output = f"{result.stdout}\n{result.stderr}".lower()
    assert "invalid timeout value" in combined_output
    assert _read_env_text(env_path) == original_text
    assert not list(env_path.parent.glob(f"{env_path.name}.bak_*"))


def test_missing_env_file_returns_nonzero(tmp_path: Path) -> None:
    script_path = _prepare_script_bundle(tmp_path)
    missing_env = tmp_path / "does-not-exist.env"

    result = _run_script(script_path, [str(missing_env)])

    assert result.returncode != 0
    combined_output = f"{result.stdout}\n{result.stderr}".lower()
    assert "missing env file" in combined_output

