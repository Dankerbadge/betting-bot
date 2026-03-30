from datetime import datetime, timezone
import json
import tempfile
from pathlib import Path
from typing import Any
import unittest
from unittest.mock import patch
from urllib.error import URLError

from betbot.kalshi_supervisor import _read_exchange_status, _resolve_output_dir, run_kalshi_supervisor


class KalshiSupervisorTests(unittest.TestCase):
    def test_resolve_output_dir_normalizes_file_like_path(self) -> None:
        resolved, warning = _resolve_output_dir("outputs/kalshi_supervisor_summary_20260328_210004.json")
        self.assertEqual(resolved, Path("outputs"))
        self.assertEqual(
            warning,
            "normalized_file_like_output_dir:outputs/kalshi_supervisor_summary_20260328_210004.json",
        )

    def test_read_exchange_status_handles_dns_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("[Errno 8] nodename nor servname provided, or not known")

            with patch("betbot.kalshi_supervisor.time.sleep") as mock_sleep:
                status = _read_exchange_status(
                    env_file=str(env_file),
                    timeout_seconds=5.0,
                    http_get_json=fake_http_get_json,
                )

            self.assertIsNone(status["http_status"])
            self.assertFalse(status["trading_active"])
            self.assertTrue(status["dns_error"])
            self.assertIn("nodename nor servname", str(status["network_error"]))
            self.assertGreaterEqual(mock_sleep.call_count, 2)
            self.assertGreaterEqual(len(status.get("api_roots_attempted", [])), 1)

    def test_read_exchange_status_flags_temporary_failure_dns_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("[Errno -3] Temporary failure in name resolution")

            with patch("betbot.kalshi_supervisor.time.sleep"):
                status = _read_exchange_status(
                    env_file=str(env_file),
                    timeout_seconds=5.0,
                    http_get_json=fake_http_get_json,
                )

            self.assertIsNone(status["http_status"])
            self.assertTrue(status["dns_error"])
            self.assertIn("temporary failure in name resolution", str(status["network_error"]).lower())

    def test_read_exchange_status_retries_then_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            attempts = 0

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise URLError("[Errno 8] nodename nor servname provided, or not known")
                return 200, {"trading_active": True}

            with patch("betbot.kalshi_supervisor.time.sleep") as mock_sleep:
                status = _read_exchange_status(
                    env_file=str(env_file),
                    timeout_seconds=5.0,
                    http_get_json=fake_http_get_json,
                )

            self.assertEqual(status["http_status"], 200)
            self.assertTrue(status["trading_active"])
            self.assertFalse(status["dns_error"])
            self.assertIsNone(status["network_error"])
            self.assertEqual(mock_sleep.call_count, 2)

    def test_run_kalshi_supervisor_completes_when_exchange_status_dns_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("[Errno 8] nodename nor servname provided, or not known")

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.return_value = {
                    "status": "dry_run",
                    "output_file": str(base / "trader.json"),
                }
                summary = run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=True,
                    exchange_status_self_heal_attempts=0,
                    run_arb_scan_each_cycle=False,
                    http_get_json=fake_http_get_json,
                    now=datetime(2026, 3, 28, 20, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["cycles_run"], 1)
            cycle = summary["cycle_summaries"][0]
            self.assertFalse(cycle["live_orders_enabled_for_cycle"])
            self.assertTrue(cycle["exchange_status"]["dns_error"])
            self.assertEqual(cycle["prior_trader_status"], "dry_run")
            self.assertTrue(Path(summary["output_file"]).exists())
            loaded = json.loads(Path(summary["output_file"]).read_text(encoding="utf-8"))
            self.assertEqual(loaded["status"], "ready")

    def test_run_kalshi_supervisor_self_heals_exchange_status_before_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            attempts = 0
            dns_calls = 0
            dns_timeouts: list[float] = []
            seen_timeouts: list[float] = []

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                nonlocal attempts
                attempts += 1
                seen_timeouts.append(float(timeout_seconds))
                if attempts <= 6:
                    raise URLError("[Errno 8] nodename nor servname provided, or not known")
                return 200, {"trading_active": True}

            def fake_dns_doctor(**kwargs: Any) -> dict[str, Any]:
                nonlocal dns_calls
                dns_calls += 1
                dns_timeouts.append(float(kwargs["timeout_seconds"]))
                return {"status": "passed", "output_file": str(base / "dns_doctor.json")}

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.return_value = {
                    "status": "dry_run",
                    "output_file": str(base / "trader.json"),
                }
                summary = run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=True,
                    timeout_seconds=10.0,
                    exchange_status_self_heal_attempts=1,
                    exchange_status_self_heal_pause_seconds=0.0,
                    exchange_status_self_heal_timeout_multiplier=2.0,
                    exchange_status_self_heal_timeout_cap_seconds=25.0,
                    run_arb_scan_each_cycle=False,
                    dns_doctor_runner=fake_dns_doctor,
                    http_get_json=fake_http_get_json,
                    now=datetime(2026, 3, 28, 20, 10, tzinfo=timezone.utc),
                )

            self.assertEqual(dns_calls, 1)
            kwargs = mock_trader.call_args.kwargs
            self.assertTrue(kwargs["allow_live_orders"])
            cycle = summary["cycle_summaries"][0]
            self.assertTrue(cycle["exchange_status_remediation_applied"])
            self.assertTrue(cycle["exchange_status_remediation_recovered"])
            self.assertEqual(cycle["exchange_status_remediation_attempts_used"], 1)
            self.assertGreaterEqual(len(cycle["exchange_status_history"]), 2)
            self.assertIn(20.0, seen_timeouts)
            self.assertEqual(dns_timeouts, [3.0])
            self.assertEqual(cycle["exchange_status_remediation_actions"][0]["retry_timeout_seconds"], 20.0)
            self.assertEqual(cycle["exchange_status_remediation_actions"][0]["dns_doctor_timeout_seconds"], 3.0)

    def test_run_kalshi_supervisor_normalizes_file_like_output_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            requested_output_dir = base / "kalshi_supervisor_summary_20260328_210004.json"

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.return_value = {
                    "status": "hold",
                    "output_file": str(base / "trader.json"),
                }
                summary = run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(requested_output_dir),
                    cycles=1,
                    allow_live_orders=False,
                    run_arb_scan_each_cycle=False,
                    http_get_json=lambda *_: (200, {"trading_active": False}),
                    now=datetime(2026, 3, 28, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["requested_output_dir"], str(requested_output_dir))
            self.assertEqual(summary["resolved_output_dir"], str(base))
            self.assertEqual(
                summary["output_dir_warning"],
                f"normalized_file_like_output_dir:{requested_output_dir}",
            )
            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertFalse(requested_output_dir.exists())
            kwargs = mock_trader.call_args.kwargs
            self.assertEqual(kwargs["output_dir"], str(base))
            self.assertFalse(kwargs["cancel_resting_immediately"])
            self.assertEqual(kwargs["resting_hold_seconds"], 0.0)

    def test_run_kalshi_supervisor_passes_resting_controls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.return_value = {
                    "status": "hold",
                    "output_file": str(base / "trader.json"),
                }
                run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=False,
                    cancel_resting_immediately=True,
                    resting_hold_seconds=30.0,
                    run_arb_scan_each_cycle=False,
                    http_get_json=lambda *_: (200, {"trading_active": False}),
                    now=datetime(2026, 3, 28, 22, 0, tzinfo=timezone.utc),
                )

            kwargs = mock_trader.call_args.kwargs
            self.assertTrue(kwargs["cancel_resting_immediately"])
            self.assertEqual(kwargs["resting_hold_seconds"], 30.0)

    def test_run_kalshi_supervisor_passes_ws_authority_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            ws_state_path = base / "kalshi_ws_state_latest.json"

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.return_value = {
                    "status": "dry_run",
                    "output_file": str(base / "trader.json"),
                }
                run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=False,
                    enforce_ws_state_authority=False,
                    ws_state_json=str(ws_state_path),
                    ws_state_max_age_seconds=12.0,
                    run_arb_scan_each_cycle=False,
                    http_get_json=lambda *_: (200, {"trading_active": False}),
                    now=datetime(2026, 3, 28, 22, 30, tzinfo=timezone.utc),
                )

            kwargs = mock_trader.call_args.kwargs
            self.assertFalse(kwargs["enforce_ws_state_authority"])
            self.assertEqual(kwargs["ws_state_json"], str(ws_state_path))
            self.assertEqual(kwargs["ws_state_max_age_seconds"], 12.0)

    def test_run_kalshi_supervisor_remediates_transient_trader_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            first_summary = {
                "status": "rate_limited",
                "capture_status": "ready",
                "capture_scan_search_health_status": "degraded_retrying",
                "capture_scan_search_retries_total": 2,
                "capture_scan_page_requests": 5,
                "capture_scan_rate_limit_retries_used": 2,
                "capture_scan_network_retries_used": 0,
                "capture_scan_transient_http_retries_used": 0,
                "prior_execute_status": "rate_limited",
                "auto_priors_status": "ready",
                "output_file": str(base / "trader_1.json"),
            }
            second_summary = {
                "status": "dry_run",
                "capture_status": "ready",
                "capture_scan_search_health_status": "ready",
                "capture_scan_search_retries_total": 0,
                "capture_scan_page_requests": 3,
                "capture_scan_rate_limit_retries_used": 0,
                "capture_scan_network_retries_used": 0,
                "capture_scan_transient_http_retries_used": 0,
                "prior_execute_status": "dry_run",
                "auto_priors_status": "ready",
                "output_file": str(base / "trader_2.json"),
            }

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.side_effect = [first_summary, second_summary]
                summary = run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=False,
                    timeout_seconds=10.0,
                    failure_remediation_max_retries=2,
                    failure_remediation_backoff_seconds=0.0,
                    failure_remediation_timeout_multiplier=2.0,
                    failure_remediation_timeout_cap_seconds=25.0,
                    run_arb_scan_each_cycle=False,
                    http_get_json=lambda *_: (200, {"trading_active": False}),
                    now=datetime(2026, 3, 29, 2, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(mock_trader.call_count, 2)
            first_call = mock_trader.call_args_list[0].kwargs
            second_call = mock_trader.call_args_list[1].kwargs
            self.assertEqual(first_call["timeout_seconds"], 10.0)
            self.assertEqual(second_call["timeout_seconds"], 20.0)
            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["cycles_with_remediation"], 1)
            self.assertEqual(summary["cycles_with_unremediated_failures"], 0)
            self.assertEqual(summary["failure_remediation_timeout_multiplier"], 2.0)
            self.assertEqual(summary["failure_remediation_timeout_cap_seconds"], 25.0)
            cycle = summary["cycle_summaries"][0]
            self.assertTrue(cycle["remediation_applied"])
            self.assertTrue(cycle["remediation_recovered"])
            self.assertEqual(cycle["prior_trader_status"], "dry_run")
            self.assertFalse(cycle["unremediated_failure"])
            self.assertIn("prior_trader_status:rate_limited", cycle["initial_failure_reasons"])
            self.assertEqual(cycle["final_failure_reasons"], [])
            self.assertEqual(cycle["capture_scan_search_health_status"], "ready")
            self.assertEqual(cycle["capture_scan_page_requests"], 3)
            self.assertEqual(cycle["trader_attempts"][0]["prior_trader_summary_file"], str(base / "trader_1.json"))
            self.assertEqual(cycle["trader_attempts"][1]["prior_trader_summary_file"], str(base / "trader_2.json"))
            self.assertEqual(cycle["trader_attempts"][0]["capture_scan_search_health_status"], "degraded_retrying")
            self.assertEqual(cycle["trader_attempts"][1]["capture_scan_search_health_status"], "ready")
            self.assertEqual(cycle["trader_attempts"][0]["timeout_seconds"], 10.0)
            self.assertEqual(cycle["trader_attempts"][1]["timeout_seconds"], 20.0)
            self.assertEqual(cycle["remediation_actions"][0]["retry_timeout_seconds"], 20.0)
            self.assertTrue(cycle["remediation_actions"][0]["exchange_status_refresh_skipped"])
            self.assertEqual(
                cycle["remediation_actions"][0]["exchange_status_refresh_skip_reason"],
                "live_orders_not_requested",
            )
            self.assertEqual(len(cycle["exchange_status_history"]), 1)

    def test_run_kalshi_supervisor_forces_dry_run_after_capture_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            first_summary = {
                "status": "upstream_error",
                "capture_status": "upstream_error",
                "prior_execute_status": "upstream_error",
                "auto_priors_status": "ready",
                "output_file": str(base / "trader_1.json"),
            }
            second_summary = {
                "status": "dry_run",
                "capture_status": "ready",
                "prior_execute_status": "dry_run",
                "auto_priors_status": "ready",
                "output_file": str(base / "trader_2.json"),
            }

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.side_effect = [first_summary, second_summary]
                summary = run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=True,
                    failure_remediation_max_retries=1,
                    failure_remediation_backoff_seconds=0.0,
                    run_arb_scan_each_cycle=False,
                    http_get_json=lambda *_: (200, {"trading_active": True}),
                    now=datetime(2026, 3, 29, 2, 30, tzinfo=timezone.utc),
                )

            self.assertEqual(mock_trader.call_count, 2)
            first_call = mock_trader.call_args_list[0].kwargs
            second_call = mock_trader.call_args_list[1].kwargs
            self.assertTrue(first_call["allow_live_orders"])
            self.assertTrue(first_call["capture_before_execute"])
            self.assertFalse(second_call["allow_live_orders"])
            self.assertFalse(second_call["capture_before_execute"])
            self.assertEqual(summary["status"], "ready")
            cycle = summary["cycle_summaries"][0]
            self.assertTrue(cycle["remediation_applied"])
            self.assertIn("force_dry_run_stale_capture", cycle["remediation_actions"][0]["actions"])
            self.assertTrue(cycle["remediation_actions"][0]["exchange_status_refresh_skipped"])
            self.assertEqual(
                cycle["remediation_actions"][0]["exchange_status_refresh_skip_reason"],
                "forced_dry_run",
            )

    def test_run_kalshi_supervisor_does_not_retry_no_real_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.return_value = {
                    "status": "hold",
                    "capture_status": "ready",
                    "prior_execute_status": "no_candidates",
                    "prior_trade_gate_status": "no_candidates",
                    "auto_priors_status": "ready",
                    "output_file": str(base / "trader.json"),
                }
                summary = run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=False,
                    failure_remediation_max_retries=2,
                    failure_remediation_backoff_seconds=0.0,
                    run_arb_scan_each_cycle=False,
                    http_get_json=lambda *_: (200, {"trading_active": False}),
                    now=datetime(2026, 3, 29, 3, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(mock_trader.call_count, 1)
            cycle = summary["cycle_summaries"][0]
            self.assertTrue(cycle["no_real_candidates"])
            self.assertFalse(cycle["failure_detected"])
            self.assertFalse(cycle["remediation_applied"])
            self.assertEqual(cycle["initial_failure_reasons"], [])
            self.assertEqual(cycle["final_failure_reasons"], [])

    def test_run_kalshi_supervisor_marks_degraded_when_failure_unremediated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            with patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader:
                mock_trader.return_value = {
                    "status": "upstream_error",
                    "capture_status": "upstream_error",
                    "prior_execute_status": "upstream_error",
                    "auto_priors_status": "ready",
                    "output_file": str(base / "trader.json"),
                }
                summary = run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=False,
                    failure_remediation_max_retries=1,
                    failure_remediation_backoff_seconds=0.0,
                    run_arb_scan_each_cycle=False,
                    http_get_json=lambda *_: (200, {"trading_active": False}),
                    now=datetime(2026, 3, 29, 4, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(mock_trader.call_count, 2)
            self.assertEqual(summary["status"], "degraded_ready")
            self.assertEqual(summary["cycles_with_unremediated_failures"], 1)
            cycle = summary["cycle_summaries"][0]
            self.assertTrue(cycle["failure_detected"])
            self.assertTrue(cycle["unremediated_failure"])
            self.assertTrue(cycle["remediation_applied"])
            self.assertFalse(cycle["remediation_recovered"])
            self.assertIn("prior_trader_status:upstream_error", cycle["final_failure_reasons"])

    def test_run_kalshi_supervisor_counts_remediated_arb_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            with (
                patch("betbot.kalshi_supervisor.run_kalshi_micro_prior_trader") as mock_trader,
                patch("betbot.kalshi_supervisor.run_kalshi_arb_scan") as mock_arb_scan,
            ):
                mock_trader.return_value = {
                    "status": "dry_run",
                    "capture_status": "ready",
                    "prior_execute_status": "dry_run",
                    "auto_priors_status": "ready",
                    "output_file": str(base / "trader.json"),
                }
                mock_arb_scan.side_effect = [
                    {"status": "upstream_error", "output_file": str(base / "arb_1.json")},
                    {"status": "ready", "output_file": str(base / "arb_2.json")},
                ]
                summary = run_kalshi_supervisor(
                    env_file=str(env_file),
                    output_dir=str(base),
                    cycles=1,
                    allow_live_orders=False,
                    timeout_seconds=10.0,
                    failure_remediation_max_retries=1,
                    failure_remediation_backoff_seconds=0.0,
                    failure_remediation_timeout_multiplier=2.0,
                    failure_remediation_timeout_cap_seconds=25.0,
                    run_arb_scan_each_cycle=True,
                    http_get_json=lambda *_: (200, {"trading_active": False}),
                    now=datetime(2026, 3, 29, 5, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(mock_arb_scan.call_args_list[0].kwargs["timeout_seconds"], 10.0)
            self.assertEqual(mock_arb_scan.call_args_list[1].kwargs["timeout_seconds"], 20.0)
            self.assertEqual(summary["cycles_with_failures"], 1)
            self.assertEqual(summary["cycles_with_remediation"], 1)
            self.assertEqual(summary["cycles_with_unremediated_failures"], 0)
            cycle = summary["cycle_summaries"][0]
            self.assertTrue(cycle["failure_detected"])
            self.assertFalse(cycle["unremediated_failure"])
            self.assertIn("arb_scan_status:upstream_error", cycle["initial_failure_reasons"])
            self.assertEqual(cycle["final_failure_reasons"], [])
            self.assertTrue(cycle["remediation_applied"])
            self.assertEqual(cycle["arb_attempts"][0]["arb_scan_summary_file"], str(base / "arb_1.json"))
            self.assertEqual(cycle["arb_attempts"][1]["arb_scan_summary_file"], str(base / "arb_2.json"))
            self.assertEqual(cycle["arb_attempts"][0]["timeout_seconds"], 10.0)
            self.assertEqual(cycle["arb_attempts"][1]["timeout_seconds"], 20.0)


if __name__ == "__main__":
    unittest.main()
