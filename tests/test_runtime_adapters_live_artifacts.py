from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from betbot.adapters.base import AdapterContext
from betbot.adapters.curated_news import CuratedNewsAdapter
from betbot.adapters.kalshi_market_data import KalshiMarketDataAdapter
from betbot.adapters.opticodds_consensus import OpticOddsConsensusAdapter
from betbot.adapters.therundown_mapping import TheRundownMappingAdapter


def _context(output_dir: Path, now_utc: datetime) -> AdapterContext:
    return AdapterContext(
        run_id="r1",
        cycle_id="c1",
        lane="observe",
        now_iso=now_utc.isoformat(),
        output_dir=str(output_dir),
    )


class RuntimeAdaptersLiveArtifactTests(unittest.TestCase):
    def test_kalshi_market_data_adapter_uses_ws_state_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            captured_at = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
            payload = {
                "captured_at": captured_at.isoformat(),
                "status": "ready",
                "gate_pass": True,
                "market_count": 18,
                "desynced_market_count": 0,
                "last_event_age_seconds": 2.0,
                "websocket_lag_ms": 250.0,
            }
            (out_dir / "kalshi_ws_state_collect_summary_20260421_120000.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            adapter = KalshiMarketDataAdapter()
            result = adapter.fetch(_context(out_dir, captured_at + timedelta(seconds=30)))
            self.assertEqual(result.status, "ok")
            self.assertEqual(result.coverage_ratio, 1.0)
            self.assertEqual((result.payload or {}).get("market_count"), 18)

    def test_opticodds_consensus_adapter_detects_partial_consensus(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            captured_at = datetime(2026, 4, 21, 13, 0, tzinfo=timezone.utc)
            payload = {
                "captured_at": captured_at.isoformat(),
                "status": "ready",
                "candidates_written": 0,
                "market_pairs_seen": 12,
                "market_pairs_with_consensus": 6,
                "positive_ev_candidates": 0,
            }
            (out_dir / "live_candidates_summary_4_2026-04-21_20260421_130000.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            adapter = OpticOddsConsensusAdapter()
            result = adapter.fetch(_context(out_dir, captured_at + timedelta(seconds=20)))
            self.assertEqual(result.status, "partial")
            self.assertGreaterEqual(result.coverage_ratio, 0.49)
            self.assertEqual((result.payload or {}).get("market_pairs_with_consensus"), 6)

    def test_curated_news_adapter_reads_auto_priors_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            captured_at = datetime(2026, 4, 21, 14, 0, tzinfo=timezone.utc)
            payload = {
                "captured_at": captured_at.isoformat(),
                "status": "ready",
                "generated_priors": 7,
                "candidate_markets": 9,
                "skipped_markets": 2,
                "top_market_ticker": "KXRAINNYCM-26JUN-1",
            }
            (out_dir / "kalshi_nonsports_auto_priors_summary_20260421_140000.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            adapter = CuratedNewsAdapter()
            result = adapter.fetch(_context(out_dir, captured_at + timedelta(seconds=15)))
            self.assertEqual(result.status, "ok")
            self.assertEqual((result.payload or {}).get("generated_priors"), 7)

    def test_therundown_mapping_adapter_reads_mapping_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            captured_at = datetime(2026, 4, 21, 15, 0, tzinfo=timezone.utc)
            payload = {
                "captured_at": captured_at.isoformat(),
                "status": "ready",
                "event_date": "2026-04-21",
                "mapped_rows": 11,
                "positive_best_entry_rows": 3,
            }
            (out_dir / "kalshi_mlb_map_summary_2026-04-21_20260421_150000.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            adapter = TheRundownMappingAdapter()
            result = adapter.fetch(_context(out_dir, captured_at + timedelta(seconds=10)))
            self.assertEqual(result.status, "ok")
            self.assertEqual((result.payload or {}).get("mapped_rows"), 11)


if __name__ == "__main__":
    unittest.main()

