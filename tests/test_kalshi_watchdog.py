from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from typing import Any

from betbot.kalshi_watchdog import run_kalshi_watchdog


def _upstream_autopilot_summary(base: Path, suffix: str) -> dict[str, Any]:
    return {
        "status": "guarded_dry_run",
        "preflight_gate_pass": False,
        "preflight_blockers": ["dns_doctor_failed", "ws_state_upstream_error"],
        "preflight": {
            "dns_doctor": {"status": "failed"},
            "ws_state_collect": {"status": "upstream_error"},
        },
        "supervisor_status": "ready",
        "supervisor_summary": {
            "cycle_summaries": [
                {
                    "exchange_status": {
                        "dns_error": True,
                        "network_error": "nodename nor servname provided, or not known",
                    },
                    "final_failure_reasons": ["capture_status:upstream_error"],
                }
            ]
        },
        "output_file": str(base / f"autopilot_upstream_{suffix}.json"),
    }


def _upstream_autopilot_summary_without_dns(base: Path, suffix: str) -> dict[str, Any]:
    return {
        "status": "guarded_dry_run",
        "preflight_gate_pass": False,
        "preflight_blockers": ["ws_state_upstream_error"],
        "preflight": {
            "ws_state_collect": {"status": "upstream_error"},
        },
        "supervisor_status": "ready",
        "supervisor_summary": {
            "cycle_summaries": [
                {
                    "exchange_status": {
                        "dns_error": False,
                        "network_error": "temporary upstream timeout",
                    },
                    "final_failure_reasons": ["capture_status:upstream_error"],
                }
            ]
        },
        "output_file": str(base / f"autopilot_upstream_no_dns_{suffix}.json"),
    }


def _healthy_autopilot_summary(base: Path, suffix: str) -> dict[str, Any]:
    return {
        "status": "ready",
        "preflight_gate_pass": True,
        "preflight_blockers": [],
        "preflight": {},
        "supervisor_status": "ready",
        "supervisor_summary": {"cycle_summaries": []},
        "output_file": str(base / f"autopilot_ready_{suffix}.json"),
    }


class KalshiWatchdogTests(unittest.TestCase):
    def test_watchdog_engages_kill_switch_and_forces_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            allow_live_args: list[bool] = []

            def fake_autopilot(**kwargs: Any) -> dict[str, Any]:
                allow_live_args.append(bool(kwargs["allow_live_orders"]))
                return _upstream_autopilot_summary(base, str(len(allow_live_args)))

            summary = run_kalshi_watchdog(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                loops=2,
                upstream_incident_threshold=1,
                kill_switch_cooldown_seconds=3600.0,
                self_heal_attempts_per_run=0,
                run_dns_doctor_on_upstream=False,
                autopilot_runner=fake_autopilot,
                sleep_fn=lambda _seconds: None,
                now=datetime(2026, 3, 30, 3, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(allow_live_args, [True, False])
            self.assertTrue(summary["kill_switch_active"])
            self.assertEqual(summary["kill_switch_engagements"], 1)
            self.assertEqual(summary["loops_run"], 2)

            state_path = Path(summary["kill_switch_state_json"])
            self.assertTrue(state_path.exists())
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertTrue(state["kill_switch_active"])
            self.assertEqual(state["consecutive_upstream_failures"], 2)

    def test_watchdog_clears_kill_switch_after_healthy_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            state_path = base / "kalshi_live_kill_switch_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "kill_switch_active": True,
                        "kill_switch_reason": "manual_test_seed",
                        "kill_switch_engaged_at": "2026-03-30T00:00:00+00:00",
                        "kill_switch_until": "2026-03-30T06:00:00+00:00",
                        "consecutive_upstream_failures": 0,
                        "consecutive_healthy_runs": 0,
                        "total_upstream_failures": 0,
                        "total_runs": 0,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            allow_live_args: list[bool] = []

            def fake_autopilot(**kwargs: Any) -> dict[str, Any]:
                allow_live_args.append(bool(kwargs["allow_live_orders"]))
                return _healthy_autopilot_summary(base, str(len(allow_live_args)))

            summary = run_kalshi_watchdog(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                loops=2,
                healthy_runs_to_clear_kill_switch=1,
                upstream_incident_threshold=2,
                kill_switch_state_json=str(state_path),
                run_dns_doctor_on_upstream=False,
                autopilot_runner=fake_autopilot,
                sleep_fn=lambda _seconds: None,
                now=datetime(2026, 3, 30, 3, 10, tzinfo=timezone.utc),
            )

            self.assertEqual(allow_live_args, [False, True])
            self.assertFalse(summary["kill_switch_active"])
            self.assertEqual(summary["kill_switch_releases"], 1)

            updated_state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertFalse(updated_state["kill_switch_active"])
            self.assertIsNone(updated_state["kill_switch_until"])

    def test_watchdog_uses_upstream_backoff_and_dns_remediation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            autopilot_calls = 0
            sleep_calls: list[float] = []
            dns_calls = 0

            def fake_autopilot(**kwargs: Any) -> dict[str, Any]:
                nonlocal autopilot_calls
                autopilot_calls += 1
                if autopilot_calls == 1:
                    return _upstream_autopilot_summary_without_dns(base, "first")
                return _healthy_autopilot_summary(base, "second")

            def fake_dns_doctor(**kwargs: Any) -> dict[str, Any]:
                nonlocal dns_calls
                dns_calls += 1
                return {
                    "status": "passed",
                    "output_file": str(base / "dns_doctor_remediation.json"),
                }

            summary = run_kalshi_watchdog(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                loops=2,
                sleep_between_loops_seconds=90.0,
                upstream_incident_threshold=3,
                upstream_retry_backoff_base_seconds=5.0,
                upstream_retry_backoff_max_seconds=30.0,
                self_heal_attempts_per_run=0,
                autopilot_runner=fake_autopilot,
                dns_doctor_runner=fake_dns_doctor,
                sleep_fn=lambda seconds: sleep_calls.append(float(seconds)),
                now=datetime(2026, 3, 30, 3, 20, tzinfo=timezone.utc),
            )

            self.assertEqual(autopilot_calls, 2)
            self.assertEqual(dns_calls, 1)
            self.assertEqual(sleep_calls, [5.0])
            self.assertEqual(summary["dns_remediations_attempted"], 1)
            self.assertFalse(summary["kill_switch_active"])
            self.assertEqual(summary["run_summaries"][0]["remediation_dns_status"], "passed")

    def test_watchdog_self_heals_in_loop_before_escalation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            allow_live_args: list[bool] = []
            autopilot_calls = 0
            dns_calls = 0

            def fake_autopilot(**kwargs: Any) -> dict[str, Any]:
                nonlocal autopilot_calls
                autopilot_calls += 1
                allow_live_args.append(bool(kwargs["allow_live_orders"]))
                if autopilot_calls == 1:
                    return _upstream_autopilot_summary(base, "initial")
                return _healthy_autopilot_summary(base, "recovered")

            def fake_dns_doctor(**kwargs: Any) -> dict[str, Any]:
                nonlocal dns_calls
                dns_calls += 1
                return {
                    "status": "passed",
                    "output_file": str(base / "dns_doctor_self_heal.json"),
                }

            summary = run_kalshi_watchdog(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                loops=1,
                upstream_incident_threshold=2,
                self_heal_attempts_per_run=2,
                self_heal_pause_seconds=0.0,
                autopilot_runner=fake_autopilot,
                dns_doctor_runner=fake_dns_doctor,
                sleep_fn=lambda _seconds: None,
                now=datetime(2026, 3, 30, 3, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(autopilot_calls, 2)
            self.assertEqual(allow_live_args, [True, True])
            self.assertEqual(dns_calls, 0)
            self.assertFalse(summary["kill_switch_active"])
            run_summary = summary["run_summaries"][0]
            self.assertTrue(run_summary["self_healed"])
            self.assertEqual(run_summary["self_heal_attempts_used"], 1)
            self.assertEqual(run_summary["autopilot_attempts_total"], 2)
            self.assertEqual(run_summary["autopilot_status"], "ready")
            self.assertEqual(run_summary["remediation_dns_skipped_due_autopilot"], 1)

    def test_watchdog_skips_dns_remediation_if_autopilot_already_checked_dns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            dns_calls = 0

            def fake_dns_doctor(**kwargs: Any) -> dict[str, Any]:
                nonlocal dns_calls
                dns_calls += 1
                return {
                    "status": "passed",
                    "output_file": str(base / "dns_doctor_watchdog.json"),
                }

            summary = run_kalshi_watchdog(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                loops=1,
                self_heal_attempts_per_run=0,
                autopilot_runner=lambda **_: _upstream_autopilot_summary(base, "single"),
                dns_doctor_runner=fake_dns_doctor,
                sleep_fn=lambda _seconds: None,
                now=datetime(2026, 3, 30, 3, 40, tzinfo=timezone.utc),
            )

            self.assertEqual(dns_calls, 0)
            self.assertEqual(summary["dns_remediations_attempted"], 0)
            self.assertEqual(summary["dns_remediations_skipped_due_autopilot"], 1)
            run_summary = summary["run_summaries"][0]
            self.assertEqual(run_summary["remediation_dns_skipped_due_autopilot"], 1)
            self.assertEqual(
                run_summary["remediation_dns_runs"][0]["status"],
                "skipped_already_covered_by_autopilot",
            )


if __name__ == "__main__":
    unittest.main()
