from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import subprocess
import sys
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
import os
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


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = list(sys.argv[1:])
    command = args[0] if args else ""
    output_dir = Path(_option_value(args, "--output-dir", "."))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

    if command == "kalshi-temperature-live-readiness":
        pipeline_status = (os.environ.get("FAKE_LIVE_READINESS_STATUS") or "green").strip().lower()
        pipeline_reason = (os.environ.get("FAKE_LIVE_READINESS_REASON") or "").strip()
        payload: dict[str, object] = {
            "status": "ready",
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "executive_summary": {
                "shortest_horizon_pipeline_status": pipeline_status,
                "shortest_horizon_pipeline_reason": pipeline_reason,
            },
        }
        _write_json(output_dir / f"kalshi_temperature_live_readiness_{stamp}.json", payload)
        return 0

    if command == "kalshi-temperature-go-live-gate":
        _write_json(output_dir / f"kalshi_temperature_go_live_gate_{stamp}.json", {"status": "ready"})
        return 0

    if command == "kalshi-temperature-bankroll-validation":
        _write_json(output_dir / f"kalshi_temperature_bankroll_validation_{stamp}.json", {"status": "ready"})
        return 0

    if command == "kalshi-temperature-alpha-gap-report":
        _write_json(output_dir / f"kalshi_temperature_alpha_gap_report_{stamp}.json", {"status": "ready"})
        return 0

    if command == "kalshi-temperature-execution-cost-tape":
        _write_json(output_dir / "health" / f"execution_cost_tape_{stamp}.json", {"status": "ready"})
        return 0

    if command == "decision-matrix-hardening":
        _write_json(output_dir / "health" / f"decision_matrix_hardening_{stamp}.json", {"status": "ready"})
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_fake_summarize_window(fake_root: Path) -> None:
    script_path = fake_root / "infra" / "digitalocean" / "summarize_window.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text(
        """
from __future__ import annotations

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
    out_dir = _option_value(args, "--out-dir", "")
    output_path_text = _option_value(args, "--output", "")
    output_path = Path(output_path_text) if output_path_text else None
    payload = {
        "status": "ready",
        "out_dir": out_dir,
        "totals": {"intents_total": 0},
        "policy_reason_counts": {"approved": 0},
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _prepare_fake_betbot_root(tmp_path: Path) -> Path:
    fake_root = tmp_path / "fake_betbot_root"
    python_bin = fake_root / ".venv" / "bin" / "python"
    python_bin.parent.mkdir(parents=True, exist_ok=True)
    python_bin.symlink_to(Path(sys.executable))
    _write_fake_betbot_cli(fake_root)
    _write_fake_summarize_window(fake_root)
    return fake_root


def _prepare_tool_shims(tmp_path: Path) -> Path:
    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)
    flock_shim = tool_dir / "flock"
    flock_shim.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    flock_shim.chmod(0o755)
    return tool_dir


def _write_env_file(*, env_file: Path, betbot_root: Path, output_dir: Path, webhook_url: str, state_file: Path) -> None:
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{betbot_root}"',
                f'OUTPUT_DIR="{output_dir}"',
                f"PIPELINE_ALERT_WEBHOOK_URL={webhook_url}",
                "PIPELINE_ALERT_NOTIFY_STATUS_CHANGE_ONLY=1",
                f"PIPELINE_ALERT_STATE_FILE={state_file}",
                "PIPELINE_ALERT_MESSAGE_MODE=concise",
                "READINESS_FAIL_ON_PIPELINE_RED=0",
                "READINESS_RUNNER_HEARTBEAT_SECONDS=0",
                "BLOCKER_AUDIT_ENABLED=0",
                "EXECUTION_COST_TAPE_ENABLED=0",
                "DECISION_MATRIX_HARDENING_ENABLED=0",
                "WINDOW_HOURS_LIST=1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _run_readiness_cycle(
    *,
    root: Path,
    env_file: Path,
    tool_dir: Path,
    live_readiness_status: str,
    live_readiness_reason: str,
) -> subprocess.CompletedProcess[str]:
    script = root / "infra" / "digitalocean" / "run_temperature_readiness_reports.sh"
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    env["FAKE_LIVE_READINESS_STATUS"] = live_readiness_status
    env["FAKE_LIVE_READINESS_REASON"] = live_readiness_reason
    return subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_readiness_reports_alert_deduplicates_same_red_reason(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_root = _prepare_fake_betbot_root(tmp_path)
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "pipeline_alert_state.json"
    tool_dir = _prepare_tool_shims(tmp_path)

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
        env_file = tmp_path / "readiness.env"
        _write_env_file(
            env_file=env_file,
            betbot_root=fake_root,
            output_dir=output_dir,
            webhook_url=f"http://127.0.0.1:{server.server_port}/pipeline",
            state_file=state_file,
        )

        first = _run_readiness_cycle(
            root=root,
            env_file=env_file,
            tool_dir=tool_dir,
            live_readiness_status="red",
            live_readiness_reason="missing_metar_summary",
        )
        second = _run_readiness_cycle(
            root=root,
            env_file=env_file,
            tool_dir=tool_dir,
            live_readiness_status="red",
            live_readiness_reason="missing_metar_summary",
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert first.returncode == 0
    assert second.returncode == 0
    assert len(captured_payloads) == 1
    payload_text = str(captured_payloads[0].get("text") or "")
    assert "Pipeline Health Alert" in payload_text
    assert "METAR summary missing." in payload_text

    assert state_file.exists()
    state_payload = json.loads(state_file.read_text(encoding="utf-8"))
    assert isinstance(state_payload.get("last_fingerprint"), str)
    assert state_payload.get("last_fingerprint")


def test_readiness_reports_resets_dedupe_after_green_then_realerts(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_root = _prepare_fake_betbot_root(tmp_path)
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = tmp_path / "pipeline_alert_state.json"
    tool_dir = _prepare_tool_shims(tmp_path)

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
        env_file = tmp_path / "readiness.env"
        _write_env_file(
            env_file=env_file,
            betbot_root=fake_root,
            output_dir=output_dir,
            webhook_url=f"http://127.0.0.1:{server.server_port}/pipeline",
            state_file=state_file,
        )

        red_one = _run_readiness_cycle(
            root=root,
            env_file=env_file,
            tool_dir=tool_dir,
            live_readiness_status="red",
            live_readiness_reason="missing_shadow_summary",
        )
        green = _run_readiness_cycle(
            root=root,
            env_file=env_file,
            tool_dir=tool_dir,
            live_readiness_status="green",
            live_readiness_reason="",
        )
        red_two = _run_readiness_cycle(
            root=root,
            env_file=env_file,
            tool_dir=tool_dir,
            live_readiness_status="red",
            live_readiness_reason="missing_shadow_summary",
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert red_one.returncode == 0
    assert green.returncode == 0
    assert red_two.returncode == 0
    assert len(captured_payloads) == 2
    first_text = str(captured_payloads[0].get("text") or "")
    second_text = str(captured_payloads[1].get("text") or "")
    assert "shadow summary missing" in first_text.lower()
    assert "shadow summary missing" in second_text.lower()
    assert state_file.exists()
