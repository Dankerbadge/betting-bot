from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_watch import run_kalshi_micro_watch


class KalshiMicroWatchTests(unittest.TestCase):
    def test_run_kalshi_micro_watch_uses_capture_scan_for_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            status_kwargs: dict[str, object] = {}

            summary = run_kalshi_micro_watch(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(base / "history.csv"),
                watch_history_csv=str(base / "watch_history.csv"),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "scan_status": "ready",
                    "scan_error": None,
                    "scan_summary_file": str(base / "capture.json"),
                    "scan_output_csv": str(base / "scan.csv"),
                    "history_csv": str(base / "history.csv"),
                },
                status_runner=lambda **kwargs: (
                    status_kwargs.update(kwargs) or {
                        "recommendation": "hold_penny_markets_only",
                        "trade_gate_status": "no_meaningful_candidates",
                        "reused_scan_csv": kwargs.get("scan_csv"),
                        "top_category": "Politics",
                        "top_category_label": "watch",
                        "category_concentration_warning": "Two-sided liquidity is heavily concentrated in Politics.",
                        "board_regime": "concentrated_penny_noise",
                        "board_regime_reason": "Recent runs are still concentrated in one thin category with no meaningful candidates.",
                        "watch_history_csv": str(base / "watch_history.csv"),
                        "watch_history_summary": {"watch_runs_total": 1},
                        "output_file": str(base / "status.json"),
                    }
                ),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(status_kwargs["scan_csv"], str(base / "scan.csv"))
            self.assertEqual(status_kwargs["watch_history_csv"], str(base / "watch_history.csv"))
            self.assertEqual(summary["status_reused_scan_csv"], str(base / "scan.csv"))
            self.assertEqual(summary["status_top_category"], "Politics")
            self.assertEqual(summary["status_board_regime"], "concentrated_penny_noise")
            self.assertEqual(summary["watch_history_summary"]["watch_runs_total"], 1)
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
