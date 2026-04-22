from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import subprocess
from threading import Thread


def _write_fake_betbot_cli(fake_root: Path) -> None:
    package_dir = fake_root / "betbot"
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "cli.py").write_text(
        """
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys


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
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _run_hardening_script(*, root: Path, env_file: Path, fake_module_root: Path) -> None:
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
