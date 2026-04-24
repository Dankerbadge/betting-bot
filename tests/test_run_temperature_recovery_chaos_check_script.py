from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def _prepare_fake_betbot_root(tmp_path: Path) -> Path:
    fake_root = tmp_path / "fake_betbot_root"
    python_bin = fake_root / ".venv" / "bin" / "python"
    python_bin.parent.mkdir(parents=True, exist_ok=True)
    python_bin.symlink_to(Path(sys.executable))
    return fake_root


def _write_systemctl_shim(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'args=("$@")',
                'while [[ ${#args[@]} -gt 0 && "${args[0]}" == -* ]]; do',
                '  args=("${args[@]:1}")',
                "done",
                'cmd="${args[0]:-}"',
                'case "$cmd" in',
                "  is-active)",
                '    echo "${MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT:-active}"',
                "    exit 0",
                "    ;;",
                "  stop|restart|start|enable|reset-failed|cat|show)",
                "    exit 0",
                "    ;;",
                "  *)",
                "    exit 0",
                "    ;;",
                "esac",
                "",
            ]
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_curl_capture_shim(path: Path, *, payload_capture: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'payload=""',
                'args=("$@")',
                "for ((idx=0; idx<${#args[@]}; idx++)); do",
                '  if [[ "${args[$idx]}" == "--data-binary" ]]; then',
                "    next_idx=$((idx + 1))",
                "    if (( next_idx < ${#args[@]} )); then",
                '      payload="${args[$next_idx]}"',
                "    fi",
                "    break",
                "  fi",
                "done",
                f"printf '%s' \"$payload\" > {str(payload_capture)!r}",
                "exit 0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_flock_shim(path: Path) -> None:
    path.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    path.chmod(0o755)


def _write_fake_recovery_script(path: Path, *, payload: dict[str, object], exit_code: int = 0) -> None:
    serialized = json.dumps(payload, indent=2)
    path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'env_file="${1:-}"',
                'if [[ -z "$env_file" || ! -f "$env_file" ]]; then',
                "  exit 64",
                "fi",
                "# shellcheck disable=SC1090",
                'source "$env_file"',
                'mkdir -p "$OUTPUT_DIR/health/recovery"',
                'cat > "$OUTPUT_DIR/health/recovery/recovery_latest.json" <<\'JSON\'',
                serialized,
                "JSON",
                f"exit {exit_code}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _prepare_script_bundle(tmp_path: Path) -> tuple[Path, Path]:
    root = Path(__file__).resolve().parents[1]
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    src_script = root / "infra" / "digitalocean" / "run_temperature_recovery_chaos_check.sh"
    script_path = bundle_dir / src_script.name
    shutil.copy2(src_script, script_path)
    script_path.chmod(0o755)

    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)
    _write_systemctl_shim(tool_dir / "systemctl")
    _write_flock_shim(tool_dir / "flock")
    return script_path, tool_dir


def _write_env_file(
    *,
    env_file: Path,
    betbot_root: Path,
    output_dir: Path,
    recovery_script: Path,
    extra_lines: tuple[str, ...] = (),
) -> None:
    lines = [
        f'BETBOT_ROOT="{betbot_root}"',
        f'OUTPUT_DIR="{output_dir}"',
        f'RECOVERY_SCRIPT="{recovery_script}"',
        "ALLOW_LIVE_ORDERS=0",
        "RECOVERY_CHAOS_STOP_SECONDS=0",
        "RECOVERY_CHAOS_WAIT_RECOVER_SECONDS=1",
        "RECOVERY_CHAOS_WORKER_WAIT_RECOVER_SECONDS=1",
        "RECOVERY_CHAOS_ENABLE_WORKER_DRILLS=0",
    ]
    lines.extend(extra_lines)
    lines.append("")
    env_file.write_text("\n".join(lines), encoding="utf-8")


def _run_script(*, script_path: Path, env_file: Path, tool_dir: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    return subprocess.run(
        ["/bin/bash", str(script_path), str(env_file)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        cwd=str(script_path.parent),
    )


def _load_json(path: Path) -> dict[str, object]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_chaos_check_strict_effectiveness_gap_fails_and_records_artifact(tmp_path: Path) -> None:
    script_path, tool_dir = _prepare_script_bundle(tmp_path)
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    output_dir = tmp_path / "out"
    recovery_script = tmp_path / "fake_recovery.sh"
    _write_fake_recovery_script(
        recovery_script,
        payload={
            "status": "ready",
            "recovery_effectiveness": {
                "strict_required": True,
                "gap_detected": True,
                "gap_reason": "summary_missing",
                "stale": False,
                "file_age_seconds": 42,
                "stale_threshold_seconds": 600,
                "summary_available": False,
                "demoted_action_count": 2,
                "harmful_action_count": 3,
            },
        },
    )

    env_file = tmp_path / "chaos.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        recovery_script=recovery_script,
    )

    result = _run_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert result.returncode == 2

    latest = _load_json(output_dir / "health" / "recovery" / "chaos_check_latest.json")
    assert latest.get("passed") is False
    assert "recovery_effectiveness_gap_detected" in str(latest.get("failure_reason") or "")
    notes = latest.get("notes")
    assert isinstance(notes, list)
    assert "recovery_effectiveness_gap_detected" in notes

    effectiveness = latest.get("recovery_effectiveness")
    assert isinstance(effectiveness, dict)
    assert effectiveness.get("strict_required") is True
    assert effectiveness.get("gap_detected") is True
    assert effectiveness.get("gap_reason") == "summary_missing"
    assert effectiveness.get("stale") is False
    assert effectiveness.get("file_age_seconds") == 42
    assert effectiveness.get("stale_threshold_seconds") == 600
    assert effectiveness.get("summary_available") is False
    assert effectiveness.get("demoted_action_count") == 2
    assert effectiveness.get("harmful_action_count") == 3


def test_chaos_check_without_strict_effectiveness_gap_passes(tmp_path: Path) -> None:
    script_path, tool_dir = _prepare_script_bundle(tmp_path)
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    output_dir = tmp_path / "out"
    recovery_script = tmp_path / "fake_recovery.sh"
    _write_fake_recovery_script(
        recovery_script,
        payload={
            "status": "ready",
            "recovery_effectiveness": {
                "strict_required": True,
                "gap_detected": False,
                "gap_reason": "none",
                "stale": False,
                "file_age_seconds": 15,
                "stale_threshold_seconds": 600,
                "summary_available": True,
                "demoted_action_count": 0,
                "harmful_action_count": 1,
            },
        },
    )

    payload_capture = tmp_path / "captured_chaos_webhook_payload.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=payload_capture)
    env_file = tmp_path / "chaos.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        recovery_script=recovery_script,
        extra_lines=(
            'RECOVERY_CHAOS_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_CHAOS_NOTIFY_ON_PASS=1",
            "RECOVERY_CHAOS_WEBHOOK_MESSAGE_MODE=concise",
        ),
    )

    result = _run_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert result.returncode == 0

    latest = _load_json(output_dir / "health" / "recovery" / "chaos_check_latest.json")
    assert latest.get("passed") is True
    assert str(latest.get("failure_reason") or "") == ""
    effectiveness = latest.get("recovery_effectiveness")
    assert isinstance(effectiveness, dict)
    assert effectiveness.get("strict_required") is True
    assert effectiveness.get("gap_detected") is False
    assert effectiveness.get("summary_available") is True
    assert effectiveness.get("demoted_action_count") == 0
    assert effectiveness.get("harmful_action_count") == 1

    payload = _load_json(payload_capture)
    content = str(payload.get("content") or "")
    assert "Effectiveness:" in content
    assert "gap=no (none)" in content
