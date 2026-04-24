from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path

from betbot.kalshi_temperature_settled_outcome_throughput import (
    run_kalshi_temperature_settled_outcome_throughput,
)


def _build_profitability_payload(
    *,
    captured_at: datetime,
    settled_outcomes: int,
    combined_regimes: dict[str, dict[str, object]],
) -> dict[str, object]:
    settlement_station: dict[str, dict[str, object]] = {}
    local_hour: dict[str, dict[str, object]] = {}
    signal_bucket: dict[str, dict[str, object]] = {}

    for entry in combined_regimes.values():
        station_key = str(entry.get("settlement_station") or "UNKNOWN")
        hour_key = str(entry.get("local_hour") or "unknown")
        signal_key = str(entry.get("signal_bucket") or "unknown")
        settlement_station.setdefault(station_key, {"trades": 0})
        local_hour.setdefault(hour_key, {"trades": 0})
        signal_bucket.setdefault(signal_key, {"trades": 0})
        settlement_station[station_key]["trades"] = int(settlement_station[station_key]["trades"]) + int(
            entry.get("trades") or 0
        )
        local_hour[hour_key]["trades"] = int(local_hour[hour_key]["trades"]) + int(entry.get("trades") or 0)
        signal_bucket[signal_key]["trades"] = int(signal_bucket[signal_key]["trades"]) + int(entry.get("trades") or 0)

    return {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "orders_settled_with_numeric_pnl": int(settled_outcomes),
        "regime_breakdown": {
            "dimension_regimes": {
                "settlement_station": settlement_station,
                "local_hour": local_hour,
                "signal_bucket": signal_bucket,
            },
            "combined_regimes": combined_regimes,
        },
    }


def test_settled_outcome_throughput_builds_coverage_growth_and_targeted_constraints(tmp_path: Path) -> None:
    baseline_7d_payload = _build_profitability_payload(
        captured_at=datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc),
        settled_outcomes=2,
        combined_regimes={
            "KMIA|no|15|no_interval_infeasible": {
                "settlement_station": "KMIA",
                "side": "no",
                "local_hour": "15",
                "signal_bucket": "no_interval_infeasible",
                "trades": 2,
                "wins": 1,
                "losses": 1,
                "pushes": 0,
                "realized_pnl_sum": 0.1,
            }
        },
    )
    baseline_24h_payload = _build_profitability_payload(
        captured_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        settled_outcomes=6,
        combined_regimes={
            "KBUF|yes|14|yes_impossible": {
                "settlement_station": "KBUF",
                "side": "yes",
                "local_hour": "14",
                "signal_bucket": "yes_impossible",
                "trades": 1,
                "wins": 0,
                "losses": 1,
                "pushes": 0,
                "realized_pnl_sum": -0.3,
            },
            "KMIA|no|15|no_interval_infeasible": {
                "settlement_station": "KMIA",
                "side": "no",
                "local_hour": "15",
                "signal_bucket": "no_interval_infeasible",
                "trades": 5,
                "wins": 3,
                "losses": 2,
                "pushes": 0,
                "realized_pnl_sum": 0.5,
            },
        },
    )
    latest_payload = _build_profitability_payload(
        captured_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
        settled_outcomes=9,
        combined_regimes={
            "KBUF|yes|14|yes_impossible": {
                "settlement_station": "KBUF",
                "side": "yes",
                "local_hour": "14",
                "signal_bucket": "yes_impossible",
                "trades": 1,
                "wins": 0,
                "losses": 1,
                "pushes": 0,
                "realized_pnl_sum": -0.3,
            },
            "KDEN|yes|16|yes_impossible": {
                "settlement_station": "KDEN",
                "side": "yes",
                "local_hour": "16",
                "signal_bucket": "yes_impossible",
                "trades": 2,
                "wins": 1,
                "losses": 1,
                "pushes": 0,
                "realized_pnl_sum": -0.1,
            },
            "KMIA|no|15|no_interval_infeasible": {
                "settlement_station": "KMIA",
                "side": "no",
                "local_hour": "15",
                "signal_bucket": "no_interval_infeasible",
                "trades": 5,
                "wins": 3,
                "losses": 2,
                "pushes": 0,
                "realized_pnl_sum": 0.5,
            },
        },
    )

    (tmp_path / "kalshi_temperature_profitability_summary_20260416_120000.json").write_text(
        json.dumps(baseline_7d_payload),
        encoding="utf-8",
    )
    (tmp_path / "kalshi_temperature_profitability_summary_20260422_120000.json").write_text(
        json.dumps(baseline_24h_payload),
        encoding="utf-8",
    )
    (tmp_path / "kalshi_temperature_profitability_summary_20260423_120000.json").write_text(
        json.dumps(latest_payload),
        encoding="utf-8",
    )

    (tmp_path / "kalshi_temperature_constraint_scan_20260423_115500.csv").write_text(
        (
            "market_ticker,settlement_station,constraint_status\n"
            "KXHIGH-BUF-1,KBUF,yes_impossible\n"
            "KXHIGH-BUF-2,KBUF,no_interval_infeasible\n"
            "KXHIGH-DEN-1,KDEN,yes_impossible\n"
            "KXHIGH-MIA-1,KMIA,no_interval_infeasible\n"
        ),
        encoding="utf-8",
    )

    payload = run_kalshi_temperature_settled_outcome_throughput(
        output_dir=str(tmp_path),
        min_trades_per_bucket=3,
        top_n_bottlenecks=2,
    )

    assert payload["status"] == "ready"
    coverage = payload["coverage"]
    assert coverage["settled_outcomes"] == 9
    assert coverage["station_bucket_count"] == 3
    assert coverage["local_hour_bucket_count"] == 3
    assert coverage["signal_bucket_count"] == 2
    assert coverage["combined_bucket_count"] == 3

    growth = payload["growth_deltas"]
    assert growth["settled_outcomes_delta_24h"] == 3
    assert growth["settled_outcomes_delta_7d"] == 7
    assert growth["combined_bucket_count_delta_24h"] == 1
    assert growth["station_bucket_count_delta_7d"] == 2

    bottlenecks = payload["top_bottlenecks"]
    assert len(bottlenecks) == 2
    assert bottlenecks[0]["settlement_station"] == "KBUF"
    assert bottlenecks[0]["local_hour"] == "14"
    assert bottlenecks[0]["signal_bucket"] == "yes_impossible"
    assert bottlenecks[0]["coverage_gap_to_target_trades"] == 2
    assert bottlenecks[1]["settlement_station"] == "KDEN"
    assert bottlenecks[1]["coverage_gap_to_target_trades"] == 1

    targeted_csv = Path(payload["targeted_constraint_csv"])
    assert targeted_csv.exists()
    assert payload["targeting"]["targeted_filter_mode"] == "station_and_signal"
    assert payload["targeted_constraint_rows"] == 2

    with targeted_csv.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    tickers = [str(row.get("market_ticker")) for row in rows]
    assert tickers == ["KXHIGH-BUF-1", "KXHIGH-DEN-1"]


def test_settled_outcome_throughput_handles_missing_profitability_artifacts(tmp_path: Path) -> None:
    payload = run_kalshi_temperature_settled_outcome_throughput(output_dir=str(tmp_path))

    assert payload["status"] == "missing_profitability_summary"
    assert payload["coverage"]["settled_outcomes"] == 0
    assert payload["top_bottlenecks"] == []
    assert payload["targeted_constraint_csv"] == ""
    assert payload["targeted_constraint_rows"] == 0
    assert payload["growth_deltas"]["settled_outcomes_delta_24h"] is None
    assert Path(payload["output_file"]).exists()
    assert Path(payload["latest_file"]).exists()


def test_settled_outcome_throughput_bootstraps_station_bottlenecks_when_settled_history_is_empty(
    tmp_path: Path,
) -> None:
    empty_settled_payload = _build_profitability_payload(
        captured_at=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
        settled_outcomes=0,
        combined_regimes={},
    )
    (tmp_path / "kalshi_temperature_profitability_summary_20260423_120000.json").write_text(
        json.dumps(empty_settled_payload),
        encoding="utf-8",
    )
    (tmp_path / "kalshi_temperature_constraint_scan_20260423_115500.csv").write_text(
        (
            "market_ticker,settlement_station,constraint_status\n"
            "KXHIGH-BUF-1,KBUF,yes_impossible\n"
            "KXHIGH-BUF-2,KBUF,no_interval_infeasible\n"
            "KXHIGH-DEN-1,KDEN,yes_impossible\n"
            "KXHIGH-MIA-1,KMIA,no_interval_infeasible\n"
        ),
        encoding="utf-8",
    )

    payload = run_kalshi_temperature_settled_outcome_throughput(
        output_dir=str(tmp_path),
        min_trades_per_bucket=3,
        top_n_bottlenecks=2,
    )

    assert payload["status"] == "ready"
    assert payload["bottleneck_source"] == "constraint_station_bootstrap"
    bottlenecks = payload["top_bottlenecks"]
    assert len(bottlenecks) == 2
    assert bottlenecks[0]["settlement_station"] == "KBUF"
    assert bottlenecks[0]["trades"] == 0
    assert bottlenecks[0]["coverage_gap_to_target_trades"] == 3
    assert bottlenecks[0]["bootstrap_market_count"] == 2
    assert bottlenecks[1]["settlement_station"] == "KDEN"
    assert bottlenecks[1]["bootstrap_market_count"] == 1

    targeted_csv = Path(payload["targeted_constraint_csv"])
    assert targeted_csv.exists()
    with targeted_csv.open("r", newline="", encoding="utf-8") as handle:
        targeted_rows = list(csv.DictReader(handle))
    assert len(targeted_rows) == 2
    targeted_stations = {str(row.get("settlement_station")) for row in targeted_rows}
    assert targeted_stations == {"KBUF", "KDEN"}
