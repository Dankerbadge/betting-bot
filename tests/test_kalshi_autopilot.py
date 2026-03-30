from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from typing import Any

from betbot.kalshi_autopilot import run_kalshi_autopilot


def _green_autopilot_payload() -> dict[str, Any]:
    return {
        "status": "ready",
        "effective_allow_live_orders": True,
        "preflight_gate_pass": True,
        "supervisor_status": "ready",
        "cycles_with_failures": 0,
        "cycles_with_unremediated_failures": 0,
    }


class KalshiAutopilotTests(unittest.TestCase):
    def test_run_kalshi_autopilot_forces_guarded_dry_run_when_preflight_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            supervisor_kwargs: dict[str, Any] = {}

            def fake_dns_doctor(**kwargs: Any) -> dict[str, Any]:
                return {
                    "status": "failed",
                    "output_file": str(base / "dns_doctor.json"),
                    "hosts_checked": 2,
                    "hosts_healthy": 0,
                }

            def fake_live_smoke(**kwargs: Any) -> dict[str, Any]:
                return {
                    "status": "passed",
                    "checks_failed": [],
                    "output_file": str(base / "live_smoke.json"),
                }

            def fake_ws_collect(**kwargs: Any) -> dict[str, Any]:
                return {
                    "status": "ready",
                    "gate_pass": True,
                    "events_logged": 24,
                    "ws_url_used": "wss://example",
                    "output_file": str(base / "ws_collect.json"),
                    "ws_state_json": str(base / "kalshi_ws_state_latest.json"),
                }

            def fake_supervisor(**kwargs: Any) -> dict[str, Any]:
                supervisor_kwargs.update(kwargs)
                return {
                    "status": "ready",
                    "cycles_with_failures": 0,
                    "cycles_with_unremediated_failures": 0,
                    "output_file": str(base / "kalshi_supervisor_summary.json"),
                }

            summary = run_kalshi_autopilot(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                preflight_self_heal_attempts=0,
                dns_doctor_runner=fake_dns_doctor,
                live_smoke_runner=fake_live_smoke,
                ws_collect_runner=fake_ws_collect,
                supervisor_runner=fake_supervisor,
                now=datetime(2026, 3, 30, 1, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "guarded_dry_run")
            self.assertFalse(summary["preflight_gate_pass"])
            self.assertFalse(summary["effective_allow_live_orders"])
            self.assertIn("dns_doctor_failed", summary["preflight_blockers"])
            self.assertFalse(supervisor_kwargs["allow_live_orders"])
            self.assertTrue(Path(summary["output_file"]).exists())
            loaded = json.loads(Path(summary["output_file"]).read_text(encoding="utf-8"))
            self.assertEqual(loaded["status"], "guarded_dry_run")

    def test_run_kalshi_autopilot_self_heals_preflight_upstream(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            ws_state_path = base / "kalshi_ws_state_latest.json"

            smoke_calls = 0
            dns_remediation_calls = 0
            supervisor_kwargs: dict[str, Any] = {}

            def fake_dns_doctor(**kwargs: Any) -> dict[str, Any]:
                nonlocal dns_remediation_calls
                dns_remediation_calls += 1
                return {
                    "status": "passed",
                    "output_file": str(base / f"dns_{dns_remediation_calls}.json"),
                }

            def fake_live_smoke(**kwargs: Any) -> dict[str, Any]:
                nonlocal smoke_calls
                smoke_calls += 1
                if smoke_calls == 1:
                    return {
                        "status": "upstream_error",
                        "checks_failed": ["kalshi"],
                        "output_file": str(base / "smoke_fail.json"),
                    }
                return {
                    "status": "passed",
                    "checks_failed": [],
                    "output_file": str(base / "smoke_pass.json"),
                }

            def fake_supervisor(**kwargs: Any) -> dict[str, Any]:
                supervisor_kwargs.update(kwargs)
                return {
                    "status": "ready",
                    "cycles_with_failures": 0,
                    "cycles_with_unremediated_failures": 0,
                    "output_file": str(base / "kalshi_supervisor_summary.json"),
                }

            summary = run_kalshi_autopilot(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                preflight_self_heal_attempts=1,
                preflight_self_heal_pause_seconds=0.0,
                dns_doctor_runner=fake_dns_doctor,
                live_smoke_runner=fake_live_smoke,
                ws_collect_runner=lambda **_: {
                    "status": "ready",
                    "gate_pass": True,
                    "events_logged": 10,
                    "ws_url_used": "wss://api.elections.kalshi.com/trade-api/ws/v2",
                    "output_file": str(base / "ws.json"),
                    "ws_state_json": str(ws_state_path),
                },
                supervisor_runner=fake_supervisor,
                now=datetime(2026, 3, 30, 1, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertTrue(summary["preflight_gate_pass"])
            self.assertTrue(summary["preflight_self_healed"])
            self.assertEqual(summary["preflight_attempts_total"], 2)
            self.assertEqual(summary["preflight_self_heal_used"], 1)
            self.assertEqual(smoke_calls, 2)
            self.assertEqual(dns_remediation_calls, 2)
            self.assertEqual(summary["preflight_dns_remediation_skipped_runs"], 1)
            self.assertTrue(supervisor_kwargs["allow_live_orders"])

    def test_run_kalshi_autopilot_allows_live_when_preflight_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            ws_state_path = base / "kalshi_ws_state_latest.json"

            supervisor_kwargs: dict[str, Any] = {}

            def fake_supervisor(**kwargs: Any) -> dict[str, Any]:
                supervisor_kwargs.update(kwargs)
                return {
                    "status": "ready",
                    "cycles_with_failures": 0,
                    "cycles_with_unremediated_failures": 0,
                    "output_file": str(base / "kalshi_supervisor_summary.json"),
                }

            summary = run_kalshi_autopilot(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                dns_doctor_runner=lambda **_: {"status": "passed", "output_file": str(base / "dns.json")},
                live_smoke_runner=lambda **_: {"status": "passed", "output_file": str(base / "smoke.json")},
                ws_collect_runner=lambda **_: {
                    "status": "ready",
                    "gate_pass": True,
                    "events_logged": 33,
                    "ws_url_used": "wss://api.elections.kalshi.com/trade-api/ws/v2",
                    "output_file": str(base / "ws.json"),
                    "ws_state_json": str(ws_state_path),
                },
                supervisor_runner=fake_supervisor,
                now=datetime(2026, 3, 30, 1, 15, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertTrue(summary["preflight_gate_pass"])
            self.assertTrue(summary["effective_allow_live_orders"])
            self.assertEqual(summary["effective_ws_state_json"], str(ws_state_path))
            self.assertTrue(supervisor_kwargs["allow_live_orders"])
            self.assertTrue(supervisor_kwargs["enforce_ws_state_authority"])
            self.assertEqual(supervisor_kwargs["ws_state_json"], str(ws_state_path))

    def test_run_kalshi_autopilot_applies_progressive_scaling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            for index in range(3):
                path = base / f"kalshi_autopilot_summary_20260330_010{index}_000.json"
                path.write_text(json.dumps(_green_autopilot_payload(), indent=2), encoding="utf-8")
                mtime = 1_700_000_000 + index
                os.utime(path, (mtime, mtime))

            supervisor_kwargs: dict[str, Any] = {}

            def fake_supervisor(**kwargs: Any) -> dict[str, Any]:
                supervisor_kwargs.update(kwargs)
                return {
                    "status": "ready",
                    "cycles_with_failures": 0,
                    "cycles_with_unremediated_failures": 0,
                    "output_file": str(base / "kalshi_supervisor_summary.json"),
                }

            summary = run_kalshi_autopilot(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=False,
                preflight_run_dns_doctor=False,
                preflight_run_live_smoke=False,
                preflight_run_ws_state_collect=False,
                max_live_submissions_per_day=3,
                max_live_cost_per_day_dollars=3.0,
                daily_risk_cap_dollars=3.0,
                scaling_green_runs_per_step=2,
                scaling_step_live_submissions=2,
                scaling_step_live_cost_dollars=1.5,
                scaling_step_daily_risk_cap_dollars=0.5,
                supervisor_runner=fake_supervisor,
                now=datetime(2026, 3, 30, 1, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["consecutive_green_runs"], 3)
            self.assertEqual(summary["scaling_steps_applied"], 1)
            self.assertEqual(summary["effective_max_live_submissions_per_day"], 5)
            self.assertEqual(summary["effective_max_live_cost_per_day_dollars"], 4.5)
            self.assertEqual(summary["effective_daily_risk_cap_dollars"], 3.5)
            self.assertEqual(supervisor_kwargs["max_live_submissions_per_day"], 5)
            self.assertEqual(supervisor_kwargs["max_live_cost_per_day_dollars"], 4.5)
            self.assertEqual(supervisor_kwargs["daily_risk_cap_dollars"], 3.5)

    def test_run_kalshi_autopilot_respects_scaling_hard_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            for index in range(8):
                path = base / f"kalshi_autopilot_summary_20260330_020{index}_000.json"
                path.write_text(json.dumps(_green_autopilot_payload(), indent=2), encoding="utf-8")
                mtime = 1_700_100_000 + index
                os.utime(path, (mtime, mtime))

            supervisor_kwargs: dict[str, Any] = {}

            def fake_supervisor(**kwargs: Any) -> dict[str, Any]:
                supervisor_kwargs.update(kwargs)
                return {
                    "status": "ready",
                    "cycles_with_failures": 0,
                    "cycles_with_unremediated_failures": 0,
                    "output_file": str(base / "kalshi_supervisor_summary.json"),
                }

            summary = run_kalshi_autopilot(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=False,
                preflight_run_dns_doctor=False,
                preflight_run_live_smoke=False,
                preflight_run_ws_state_collect=False,
                max_live_submissions_per_day=3,
                max_live_cost_per_day_dollars=3.0,
                daily_risk_cap_dollars=3.0,
                scaling_green_runs_per_step=1,
                scaling_step_live_submissions=2,
                scaling_step_live_cost_dollars=1.0,
                scaling_step_daily_risk_cap_dollars=1.0,
                scaling_hard_max_live_submissions_per_day=10,
                scaling_hard_max_live_cost_per_day_dollars=8.0,
                scaling_hard_max_daily_risk_cap_dollars=7.0,
                supervisor_runner=fake_supervisor,
                now=datetime(2026, 3, 30, 1, 45, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["scaling_steps_applied"], 8)
            self.assertEqual(summary["effective_max_live_submissions_per_day"], 10)
            self.assertEqual(summary["effective_max_live_cost_per_day_dollars"], 8.0)
            self.assertEqual(summary["effective_daily_risk_cap_dollars"], 7.0)
            self.assertEqual(supervisor_kwargs["max_live_submissions_per_day"], 10)
            self.assertEqual(supervisor_kwargs["max_live_cost_per_day_dollars"], 8.0)
            self.assertEqual(supervisor_kwargs["daily_risk_cap_dollars"], 7.0)

    def test_run_kalshi_autopilot_marks_degraded_when_supervisor_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            summary = run_kalshi_autopilot(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                preflight_run_dns_doctor=False,
                preflight_run_live_smoke=False,
                preflight_run_ws_state_collect=False,
                supervisor_runner=lambda **_: {
                    "status": "degraded",
                    "cycles_with_failures": 1,
                    "cycles_with_unremediated_failures": 1,
                    "output_file": str(base / "kalshi_supervisor_summary.json"),
                },
                now=datetime(2026, 3, 30, 2, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "degraded")
            self.assertEqual(summary["supervisor_status"], "degraded")
            self.assertTrue(summary["preflight_gate_pass"])
            self.assertTrue(summary["effective_allow_live_orders"])


if __name__ == "__main__":
    unittest.main()
