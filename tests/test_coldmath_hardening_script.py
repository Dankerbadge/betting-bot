from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import subprocess
import time
from threading import Thread

import pytest


def _write_fake_betbot_cli(fake_root: Path) -> None:
    package_dir = fake_root / "betbot"
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "cli.py").write_text(
        """
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time


def _option_value(args: list[str], key: str, default: str = "") -> str:
    try:
        idx = args.index(key)
    except ValueError:
        return default
    if idx + 1 >= len(args):
        return default
    return str(args[idx + 1] or default)


def main() -> int:
    args = list(sys.argv[1:])
    command = args[0] if args else ""
    if command == "kalshi-temperature-recovery-loop":
        sleep_seconds_text = str(
            os.environ.get("COLDMATH_FAKE_RECOVERY_LOOP_SLEEP_SECONDS") or ""
        ).strip()
        sleep_marker_path = Path.cwd() / "recovery_loop_sleep.marker"
        sleep_seconds = 0.0
        if sleep_seconds_text:
            try:
                sleep_seconds = max(0.0, float(sleep_seconds_text))
            except ValueError:
                sleep_seconds = 0.0
        elif sleep_marker_path.exists():
            try:
                sleep_seconds = max(
                    0.0,
                    float(sleep_marker_path.read_text(encoding="utf-8").strip() or "0.5"),
                )
            except ValueError:
                sleep_seconds = 0.5
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        output_dir = Path(_option_value(args, "--output-dir", "."))
        payload = {
            "command": command,
            "args": args,
            "max_iterations": _option_value(args, "--max-iterations", ""),
            "stall_iterations": _option_value(args, "--stall-iterations", ""),
            "min_gap_improvement": _option_value(args, "--min-gap-improvement", ""),
        }
        health_dir = output_dir / "health"
        health_dir.mkdir(parents=True, exist_ok=True)
        (health_dir / "fake_recovery_loop_invocation_latest.json").write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
        return 0
    if command == "coldmath-snapshot-summary":
        snapshot_sleep_seconds_text = str(
            os.environ.get("COLDMATH_FAKE_SNAPSHOT_SLEEP_SECONDS") or ""
        ).strip()
        if snapshot_sleep_seconds_text:
            try:
                snapshot_sleep_seconds = max(0.0, float(snapshot_sleep_seconds_text))
            except ValueError:
                snapshot_sleep_seconds = 0.0
            if snapshot_sleep_seconds > 0:
                time.sleep(snapshot_sleep_seconds)
    if command != "coldmath-snapshot-summary":
        return 0

    output_dir = Path(_option_value(args, "--output-dir", "."))
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    payload = {
        "status": "ready",
        "positions_rows": 250,
        "closed_positions_rows": 0,
        "snapshot_age_hours": 1.0,
        "is_stale": False,
    }
    output_path = output_dir / f"coldmath_snapshot_summary_{stamp}.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_env_file(
    *,
    env_file: Path,
    betbot_root: Path,
    output_dir: Path,
    webhook_url: str,
    state_file: Path,
    degraded_streak_threshold: int,
    degraded_streak_notify_every: int,
) -> None:
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{betbot_root}"',
                f'OUTPUT_DIR="{output_dir}"',
                "COLDMATH_WALLET_ADDRESS=0x594edb9112f526fa6a80b8f858a6379c8a2c1c11",
                "COLDMATH_MARKET_INGEST_ENABLED=0",
                "COLDMATH_REPLICATION_ENABLED=0",
                "COLDMATH_EXECUTION_COST_TAPE_ENABLED=0",
                "COLDMATH_DECISION_MATRIX_ENABLED=0",
                "COLDMATH_ACTIONABLE_REQUIRE_INGEST=0",
                "COLDMATH_ACTIONABLE_REQUIRE_REPLICATION=0",
                "COLDMATH_ACTIONABLE_REQUIRE_DECISION_MATRIX=1",
                "COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP=0",
                "COLDMATH_LANE_ALERT_ENABLED=1",
                "COLDMATH_LANE_ALERT_NOTIFY_STATUS_CHANGE_ONLY=1",
                f"COLDMATH_LANE_ALERT_WEBHOOK_URL={webhook_url}",
                "COLDMATH_LANE_ALERT_WEBHOOK_TIMEOUT_SECONDS=3",
                "COLDMATH_LANE_ALERT_MESSAGE_MODE=detailed",
                f"COLDMATH_LANE_ALERT_STATE_FILE={state_file}",
                "COLDMATH_LANE_ALERT_DEGRADED_STATUSES=matrix_failed,bootstrap_blocked",
                f"COLDMATH_LANE_ALERT_DEGRADED_STREAK_THRESHOLD={degraded_streak_threshold}",
                f"COLDMATH_LANE_ALERT_DEGRADED_STREAK_NOTIFY_EVERY={degraded_streak_notify_every}",
                "COLDMATH_COVERAGE_VELOCITY_REPORT_ENABLED=1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _run_hardening_script(
    *,
    root: Path,
    env_file: Path,
    fake_module_root: Path,
    timeout_seconds: float | None = None,
) -> None:
    script = root / "infra" / "digitalocean" / "run_temperature_coldmath_hardening.sh"
    env = dict(os.environ)
    existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
    env["PYTHONPATH"] = (
        f"{fake_module_root}:{existing_pythonpath}"
        if existing_pythonpath
        else str(fake_module_root)
    )
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(fake_module_root),
        timeout=timeout_seconds,
    )


def _write_decision_matrix_fixture(output_dir: Path, payload: dict[str, object]) -> Path:
    health_dir = output_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)
    path = health_dir / "decision_matrix_hardening_20990101_000000.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _load_coldmath_hardening_latest(output_dir: Path) -> dict[str, object]:
    latest_path = output_dir / "health" / "coldmath_hardening_latest.json"
    assert latest_path.exists()
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _load_hardening_stage(payload: dict[str, object], stage_name: str) -> dict[str, object]:
    stages = payload.get("stages")
    assert isinstance(stages, list)
    stage = next((item for item in stages if isinstance(item, dict) and item.get("stage") == stage_name), None)
    assert isinstance(stage, dict)
    return stage


def _supports_coldmath_stage_timeouts(root: Path) -> bool:
    script = root / "infra" / "digitalocean" / "run_temperature_coldmath_hardening.sh"
    if not script.exists():
        return False
    text = script.read_text(encoding="utf-8")
    return (
        "COLDMATH_STAGE_TIMEOUT_SECONDS" in text
        and "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS" in text
        and "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS" in text
    )


def test_coldmath_hardening_lane_alert_triggers_on_degraded_streak_threshold(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    captured_payloads: list[dict[str, object]] = []

    class _CaptureHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length).decode("utf-8")
            parsed: dict[str, object]
            try:
                payload = json.loads(body)
                parsed = payload if isinstance(payload, dict) else {"raw": body}
            except Exception:
                parsed = {"raw": body}
            captured_payloads.append(parsed)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = HTTPServer(("127.0.0.1", 0), _CaptureHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env_file = tmp_path / "coldmath_hardening.env"
        _write_env_file(
            env_file=env_file,
            betbot_root=root,
            output_dir=output_dir,
            webhook_url=f"http://127.0.0.1:{server.server_port}/lane",
            state_file=state_file,
            degraded_streak_threshold=2,
            degraded_streak_notify_every=2,
        )
        _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)
        _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert len(captured_payloads) == 1
    payload_text = str(captured_payloads[0].get("text") or "")
    assert "Notify reason: degraded_streak" in payload_text
    assert "Degraded streak: 2 (threshold 2, every 2)." in payload_text

    state_payload = json.loads(state_file.read_text(encoding="utf-8"))
    assert state_payload.get("degraded_streak_count") == 2
    assert state_payload.get("last_notify_reason") == "degraded_streak"
    assert state_payload.get("last_notified") is True
    assert state_payload.get("last_degraded_streak_notified_count") == 2

    log_file = output_dir / "logs" / "coldmath_hardening.log"
    assert log_file.exists()
    log_text = log_file.read_text(encoding="utf-8")
    assert "streak=2" in log_text
    assert "streak_triggered=1" in log_text
    assert "notify_reason=degraded_streak" in log_text


def test_coldmath_hardening_lane_alert_repeats_every_n_degraded_runs(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    captured_payloads: list[dict[str, object]] = []

    class _CaptureHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length).decode("utf-8")
            parsed: dict[str, object]
            try:
                payload = json.loads(body)
                parsed = payload if isinstance(payload, dict) else {"raw": body}
            except Exception:
                parsed = {"raw": body}
            captured_payloads.append(parsed)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = HTTPServer(("127.0.0.1", 0), _CaptureHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env_file = tmp_path / "coldmath_hardening.env"
        _write_env_file(
            env_file=env_file,
            betbot_root=root,
            output_dir=output_dir,
            webhook_url=f"http://127.0.0.1:{server.server_port}/lane",
            state_file=state_file,
            degraded_streak_threshold=2,
            degraded_streak_notify_every=2,
        )
        for _ in range(4):
            _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert len(captured_payloads) == 2
    payload_texts = [str(item.get("text") or "") for item in captured_payloads]
    assert any("Degraded streak: 2 (threshold 2, every 2)." in text for text in payload_texts)
    assert any("Degraded streak: 4 (threshold 2, every 2)." in text for text in payload_texts)

    state_payload = json.loads(state_file.read_text(encoding="utf-8"))
    assert state_payload.get("degraded_streak_count") == 4
    assert state_payload.get("last_degraded_streak_notified_count") == 4


def test_coldmath_hardening_auto_applies_recovery_recommended_env_by_default(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_ADVISOR_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_CAMPAIGN_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS=11\n")
        handle.write("COLDMATH_RECOVERY_LOOP_STALL_ITERATIONS=7\n")
        handle.write("COLDMATH_RECOVERY_LOOP_MIN_GAP_IMPROVEMENT=0.111\n")

    recommended_env = output_dir / "health" / "kalshi_temperature_recovery_recommended.env"
    recommended_env.parent.mkdir(parents=True, exist_ok=True)
    recommended_env.write_text(
        "\n".join(
            [
                "COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS=3",
                "COLDMATH_RECOVERY_LOOP_STALL_ITERATIONS=2",
                "COLDMATH_RECOVERY_LOOP_MIN_GAP_IMPROVEMENT=0.0075",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)

    invocation_path = output_dir / "health" / "fake_recovery_loop_invocation_latest.json"
    assert invocation_path.exists()
    invocation = json.loads(invocation_path.read_text(encoding="utf-8"))
    assert invocation.get("max_iterations") == "3"
    assert invocation.get("stall_iterations") == "2"
    assert invocation.get("min_gap_improvement") == "0.0075"

    log_file = output_dir / "logs" / "coldmath_hardening.log"
    assert log_file.exists()
    log_text = log_file.read_text(encoding="utf-8")
    assert "applied recovery env overrides" in log_text

    payload = _load_coldmath_hardening_latest(output_dir)
    persistence = payload.get("recovery_env_persistence")
    assert isinstance(persistence, dict)
    assert persistence.get("status") == "disabled"
    assert persistence.get("changed") is False
    stages = payload.get("stages")
    assert isinstance(stages, list)
    persistence_stage = next((item for item in stages if isinstance(item, dict) and item.get("stage") == "recovery_env_persistence"), None)
    assert isinstance(persistence_stage, dict)
    assert persistence_stage.get("status") == "skipped"


def test_coldmath_hardening_times_out_recovery_loop_stage_without_hanging(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    if not _supports_coldmath_stage_timeouts(root):
        pytest.xfail("coldmath hardening shell script has not gained stage timeout support yet")

    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_DECISION_MATRIX_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_ADVISOR_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_LOOP_ENABLED=1\n")
        handle.write("COLDMATH_RECOVERY_CAMPAIGN_ENABLED=0\n")
        handle.write("COLDMATH_COVERAGE_VELOCITY_REPORT_ENABLED=0\n")
        handle.write("COLDMATH_STAGE_TIMEOUT_SECONDS=1\n")
        handle.write("COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS=1\n")
        handle.write("COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS=0\n")

    (fake_module_root / "recovery_loop_sleep.marker").write_text("2.0\n", encoding="utf-8")

    start = time.monotonic()
    _run_hardening_script(
        root=root,
        env_file=env_file,
        fake_module_root=fake_module_root,
        timeout_seconds=15,
    )
    assert time.monotonic() - start < 15

    payload = _load_coldmath_hardening_latest(output_dir)
    recovery_loop_stage = _load_hardening_stage(payload, "kalshi_temperature_recovery_loop")
    assert recovery_loop_stage.get("status") == "timeout"
    assert recovery_loop_stage.get("exit_code") == 124


def test_coldmath_hardening_runs_recovery_loop_normally_when_timeouts_disabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_DECISION_MATRIX_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_ADVISOR_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_LOOP_ENABLED=1\n")
        handle.write("COLDMATH_RECOVERY_CAMPAIGN_ENABLED=0\n")
        handle.write("COLDMATH_COVERAGE_VELOCITY_REPORT_ENABLED=0\n")
        handle.write("COLDMATH_STAGE_TIMEOUT_SECONDS=0\n")
        handle.write("COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS=0\n")
        handle.write("COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS=0\n")

    (fake_module_root / "recovery_loop_sleep.marker").write_text("0.2\n", encoding="utf-8")

    _run_hardening_script(
        root=root,
        env_file=env_file,
        fake_module_root=fake_module_root,
        timeout_seconds=15,
    )

    payload = _load_coldmath_hardening_latest(output_dir)
    recovery_loop_stage = _load_hardening_stage(payload, "kalshi_temperature_recovery_loop")
    assert recovery_loop_stage.get("status") == "ok"
    assert recovery_loop_stage.get("exit_code") == 0


def test_coldmath_hardening_times_out_required_snapshot_stage_and_fails_fast(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    if not _supports_coldmath_stage_timeouts(root):
        pytest.xfail("coldmath hardening shell script has not gained stage timeout support yet")

    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=1\n")
        handle.write("COLDMATH_STAGE_TIMEOUT_SECONDS=0\n")

    previous_fake_snapshot_sleep = os.environ.get("COLDMATH_FAKE_SNAPSHOT_SLEEP_SECONDS")
    os.environ["COLDMATH_FAKE_SNAPSHOT_SLEEP_SECONDS"] = "2.0"
    start = time.monotonic()
    try:
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            _run_hardening_script(
                root=root,
                env_file=env_file,
                fake_module_root=fake_module_root,
                timeout_seconds=15,
            )
    finally:
        if previous_fake_snapshot_sleep is None:
            os.environ.pop("COLDMATH_FAKE_SNAPSHOT_SLEEP_SECONDS", None)
        else:
            os.environ["COLDMATH_FAKE_SNAPSHOT_SLEEP_SECONDS"] = previous_fake_snapshot_sleep
    elapsed_seconds = time.monotonic() - start
    assert elapsed_seconds < 15
    assert int(exc_info.value.returncode) == 124

    log_file = output_dir / "logs" / "coldmath_hardening.log"
    assert log_file.exists()
    log_text = log_file.read_text(encoding="utf-8")
    assert "stage timeout after 1s" in log_text
    assert "coldmath-snapshot-summary" in log_text


def test_coldmath_hardening_runs_coverage_velocity_report_stage_when_enabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_DECISION_MATRIX_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_ADVISOR_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_LOOP_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_CAMPAIGN_ENABLED=0\n")
        handle.write("COLDMATH_COVERAGE_VELOCITY_REPORT_ENABLED=1\n")

    _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)

    payload = _load_coldmath_hardening_latest(output_dir)
    stages = payload.get("stages")
    assert isinstance(stages, list)
    coverage_stage = next((item for item in stages if isinstance(item, dict) and item.get("stage") == "coverage_velocity_report"), None)
    assert isinstance(coverage_stage, dict)
    assert coverage_stage.get("status") == "ok"
    campaign_stage_index = next(
        index
        for index, item in enumerate(stages)
        if isinstance(item, dict) and item.get("stage") == "kalshi_temperature_recovery_campaign"
    )
    coverage_stage_index = next(
        index
        for index, item in enumerate(stages)
        if isinstance(item, dict) and item.get("stage") == "coverage_velocity_report"
    )
    persistence_stage_index = next(
        index
        for index, item in enumerate(stages)
        if isinstance(item, dict) and item.get("stage") == "recovery_env_persistence"
    )
    assert campaign_stage_index < coverage_stage_index < persistence_stage_index


def test_coldmath_hardening_skips_coverage_velocity_report_stage_when_disabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_DECISION_MATRIX_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_ADVISOR_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_LOOP_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_CAMPAIGN_ENABLED=0\n")
        handle.write("COLDMATH_COVERAGE_VELOCITY_REPORT_ENABLED=0\n")

    _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)

    payload = _load_coldmath_hardening_latest(output_dir)
    stages = payload.get("stages")
    assert isinstance(stages, list)
    coverage_stage = next((item for item in stages if isinstance(item, dict) and item.get("stage") == "coverage_velocity_report"), None)
    assert isinstance(coverage_stage, dict)
    assert coverage_stage.get("status") == "skipped"


def test_coldmath_hardening_persists_recovery_recommended_env_when_enabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)

    runtime_target_env = tmp_path / "runtime.env"
    runtime_target_env.write_text(
        "\n".join(
            [
                "UNCHANGED_SETTING=keep_me",
                "COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS=11",
                "COLDMATH_RECOVERY_LOOP_STALL_ITERATIONS=7",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    persist_backup_dir = tmp_path / "persist_backups"

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_ADVISOR_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_LOOP_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_CAMPAIGN_ENABLED=0\n")
        handle.write("COLDMATH_RECOVERY_AUTO_APPLY_RECOMMENDED_ENV=0\n")
        handle.write("COLDMATH_RECOVERY_PERSIST_RECOMMENDED_ENV=1\n")
        handle.write(f"COLDMATH_RECOVERY_PERSIST_TARGET_ENV_FILE={runtime_target_env}\n")
        handle.write(f"COLDMATH_RECOVERY_PERSIST_BACKUP_DIR={persist_backup_dir}\n")

    recommended_env = output_dir / "health" / "kalshi_temperature_recovery_recommended.env"
    recommended_env.parent.mkdir(parents=True, exist_ok=True)
    recommended_env.write_text(
        "\n".join(
            [
                "COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS=3",
                "COLDMATH_RECOVERY_LOOP_STALL_ITERATIONS=2",
                "COLDMATH_RECOVERY_ADVISOR_OPTIMIZER_TOP_N=8",
                "NOT_ALLOWED_KEY=123",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)

    runtime_text = runtime_target_env.read_text(encoding="utf-8")
    assert "UNCHANGED_SETTING=keep_me" in runtime_text
    assert "COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS=3" in runtime_text
    assert "COLDMATH_RECOVERY_LOOP_STALL_ITERATIONS=2" in runtime_text
    assert "COLDMATH_RECOVERY_ADVISOR_OPTIMIZER_TOP_N=8" in runtime_text
    assert "NOT_ALLOWED_KEY=123" not in runtime_text

    backups = sorted(persist_backup_dir.glob("runtime.env.*.bak"))
    assert backups
    backup_text = backups[-1].read_text(encoding="utf-8")
    assert "COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS=11" in backup_text
    assert "COLDMATH_RECOVERY_LOOP_STALL_ITERATIONS=7" in backup_text

    log_file = output_dir / "logs" / "coldmath_hardening.log"
    assert log_file.exists()
    log_text = log_file.read_text(encoding="utf-8")
    assert "persisted recovery env overrides" in log_text

    payload = _load_coldmath_hardening_latest(output_dir)
    persistence = payload.get("recovery_env_persistence")
    assert isinstance(persistence, dict)
    assert persistence.get("status") == "persisted"
    assert persistence.get("changed") is True
    assert persistence.get("updated_count") == 3
    assert persistence.get("replaced_count") == 2
    assert persistence.get("added_count") == 1
    assert str(persistence.get("target_file") or "") == str(runtime_target_env)
    assert str(persistence.get("backup_file") or "").endswith(".bak")
    stages = payload.get("stages")
    assert isinstance(stages, list)
    persistence_stage = next((item for item in stages if isinstance(item, dict) and item.get("stage") == "recovery_env_persistence"), None)
    assert isinstance(persistence_stage, dict)
    assert persistence_stage.get("status") == "ok"


def test_coldmath_hardening_sets_bootstrap_lane_when_strict_signal_is_off(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    bootstrap_state_file = tmp_path / "bootstrap_guard_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)
    _write_decision_matrix_fixture(
        output_dir,
        {
            "status": "ready",
            "matrix_health_status": "red",
            "matrix_score": 20,
            "supports_consistency_and_profitability": False,
            "supports_bootstrap_progression": True,
            "critical_blockers_count": 3,
            "observed_metrics": {
                "settled_outcomes": 4,
            },
        },
    )

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP=1\n")
        handle.write("COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_MAX_HOURS=336\n")
        handle.write("COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_DISABLE_AT_SETTLED_OUTCOMES=25\n")
        handle.write(f"COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_STATE_FILE={bootstrap_state_file}\n")

    _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)

    payload = _load_coldmath_hardening_latest(output_dir)
    targeted = payload.get("targeted_trading_support")
    assert isinstance(targeted, dict)
    lane = targeted.get("decision_matrix_lane")
    assert isinstance(lane, dict)
    checks = targeted.get("checks")
    assert isinstance(checks, dict)
    observed = targeted.get("observed")
    assert isinstance(observed, dict)

    assert targeted.get("supports_targeted_trading") is True
    assert lane.get("status") == "bootstrap"
    assert "bootstrap pass" in str(lane.get("summary_line") or "").lower()
    assert checks.get("decision_matrix_strict_signal") is False
    assert checks.get("decision_matrix_bootstrap_signal_raw") is True
    assert checks.get("decision_matrix_bootstrap_signal") is True
    assert observed.get("decision_matrix_bootstrap_guard_status") == "active"

    assert bootstrap_state_file.exists()
    bootstrap_state_payload = json.loads(bootstrap_state_file.read_text(encoding="utf-8"))
    assert bootstrap_state_payload.get("status") == "active"
    assert int(bootstrap_state_payload.get("first_seen_epoch") or 0) > 0


def test_coldmath_hardening_blocks_bootstrap_lane_when_settled_outcomes_threshold_reached(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    bootstrap_state_file = tmp_path / "bootstrap_guard_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)
    _write_decision_matrix_fixture(
        output_dir,
        {
            "status": "ready",
            "matrix_health_status": "red",
            "matrix_score": 15,
            "supports_consistency_and_profitability": False,
            "supports_bootstrap_progression": True,
            "critical_blockers_count": 4,
            "observed_metrics": {
                "settled_outcomes": 30,
            },
        },
    )

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP=1\n")
        handle.write("COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_MAX_HOURS=336\n")
        handle.write("COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_DISABLE_AT_SETTLED_OUTCOMES=10\n")
        handle.write(f"COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_STATE_FILE={bootstrap_state_file}\n")

    _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)

    payload = _load_coldmath_hardening_latest(output_dir)
    targeted = payload.get("targeted_trading_support")
    assert isinstance(targeted, dict)
    lane = targeted.get("decision_matrix_lane")
    assert isinstance(lane, dict)
    checks = targeted.get("checks")
    assert isinstance(checks, dict)
    observed = targeted.get("observed")
    assert isinstance(observed, dict)

    assert targeted.get("supports_targeted_trading") is False
    assert "decision_matrix_signal_too_weak" in list(targeted.get("failed_checks") or [])
    assert lane.get("status") == "bootstrap_blocked"
    assert "bootstrap blocked" in str(lane.get("summary_line") or "").lower()
    assert checks.get("decision_matrix_strict_signal") is False
    assert checks.get("decision_matrix_bootstrap_signal_raw") is True
    assert checks.get("decision_matrix_bootstrap_signal") is False
    assert observed.get("decision_matrix_bootstrap_guard_status") == "blocked"
    assert "settled_outcomes_reached_disable_threshold" in list(
        observed.get("decision_matrix_bootstrap_guard_reasons") or []
    )

    assert bootstrap_state_file.exists()
    bootstrap_state_payload = json.loads(bootstrap_state_file.read_text(encoding="utf-8"))
    assert bootstrap_state_payload.get("status") == "blocked"
    assert "settled_outcomes_reached_disable_threshold" in list(
        bootstrap_state_payload.get("bootstrap_guard_reasons") or []
    )


def test_coldmath_hardening_blocks_bootstrap_lane_when_bootstrap_window_expires(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    bootstrap_state_file = tmp_path / "bootstrap_guard_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)
    _write_decision_matrix_fixture(
        output_dir,
        {
            "status": "ready",
            "matrix_health_status": "red",
            "matrix_score": 20,
            "supports_consistency_and_profitability": False,
            "supports_bootstrap_progression": True,
            "critical_blockers_count": 2,
            "observed_metrics": {
                "settled_outcomes": 3,
            },
        },
    )
    bootstrap_state_file.write_text(
        json.dumps(
            {
                "status": "active",
                "first_seen_epoch": 1,
                "last_seen_epoch": 1,
                "first_seen_at": "1970-01-01T00:00:01+00:00",
                "last_seen_at": "1970-01-01T00:00:01+00:00",
                "bootstrap_guard_reasons": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP=1\n")
        handle.write("COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_MAX_HOURS=1\n")
        handle.write("COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_DISABLE_AT_SETTLED_OUTCOMES=1000\n")
        handle.write(f"COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_STATE_FILE={bootstrap_state_file}\n")

    _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)

    payload = _load_coldmath_hardening_latest(output_dir)
    targeted = payload.get("targeted_trading_support")
    assert isinstance(targeted, dict)
    lane = targeted.get("decision_matrix_lane")
    assert isinstance(lane, dict)
    checks = targeted.get("checks")
    assert isinstance(checks, dict)
    observed = targeted.get("observed")
    assert isinstance(observed, dict)

    assert targeted.get("supports_targeted_trading") is False
    assert lane.get("status") == "bootstrap_blocked"
    assert "bootstrap blocked" in str(lane.get("summary_line") or "").lower()
    assert checks.get("decision_matrix_bootstrap_signal_raw") is True
    assert checks.get("decision_matrix_bootstrap_signal") is False
    assert observed.get("decision_matrix_bootstrap_guard_status") == "blocked"
    assert "bootstrap_window_expired" in list(
        observed.get("decision_matrix_bootstrap_guard_reasons") or []
    )

    assert bootstrap_state_file.exists()
    bootstrap_state_payload = json.loads(bootstrap_state_file.read_text(encoding="utf-8"))
    assert bootstrap_state_payload.get("status") == "blocked"
    assert "bootstrap_window_expired" in list(
        bootstrap_state_payload.get("bootstrap_guard_reasons") or []
    )


def test_coldmath_hardening_resets_bootstrap_state_when_strict_lane_recovers(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "lane_state.json"
    bootstrap_state_file = tmp_path / "bootstrap_guard_state.json"
    fake_module_root = tmp_path / "fake_module_root"
    _write_fake_betbot_cli(fake_module_root)
    _write_decision_matrix_fixture(
        output_dir,
        {
            "status": "ready",
            "matrix_health_status": "green",
            "matrix_score": 95,
            "supports_consistency_and_profitability": True,
            "supports_bootstrap_progression": False,
            "critical_blockers_count": 0,
            "observed_metrics": {
                "settled_outcomes": 50,
            },
        },
    )
    bootstrap_state_file.write_text(
        json.dumps(
            {
                "status": "active",
                "first_seen_epoch": 12345,
                "last_seen_epoch": 12345,
                "bootstrap_guard_reasons": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    env_file = tmp_path / "coldmath_hardening.env"
    _write_env_file(
        env_file=env_file,
        betbot_root=root,
        output_dir=output_dir,
        webhook_url="",
        state_file=state_file,
        degraded_streak_threshold=2,
        degraded_streak_notify_every=2,
    )
    with env_file.open("a", encoding="utf-8") as handle:
        handle.write("COLDMATH_LANE_ALERT_ENABLED=0\n")
        handle.write("COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP=1\n")
        handle.write("COLDMATH_ACTIONABLE_MIN_MATRIX_SCORE=75\n")
        handle.write("COLDMATH_ACTIONABLE_ALLOWED_MATRIX_STATUSES=green,yellow\n")
        handle.write("COLDMATH_ACTIONABLE_REQUIRE_MATRIX_SUPPORTS=1\n")
        handle.write(f"COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_STATE_FILE={bootstrap_state_file}\n")

    _run_hardening_script(root=root, env_file=env_file, fake_module_root=fake_module_root)

    payload = _load_coldmath_hardening_latest(output_dir)
    targeted = payload.get("targeted_trading_support")
    assert isinstance(targeted, dict)
    lane = targeted.get("decision_matrix_lane")
    assert isinstance(lane, dict)
    observed = targeted.get("observed")
    assert isinstance(observed, dict)

    assert targeted.get("supports_targeted_trading") is True
    assert lane.get("status") == "strict"
    assert observed.get("decision_matrix_bootstrap_guard_status") == "strict_signal_ready"
    assert bootstrap_state_file.exists() is False
