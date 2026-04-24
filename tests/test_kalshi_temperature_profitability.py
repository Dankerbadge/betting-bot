from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path

from betbot.kalshi_execution_journal import append_execution_events, ensure_execution_journal_schema
from betbot.kalshi_temperature_profitability import (
    run_kalshi_temperature_profitability,
    run_kalshi_temperature_refill_trial_balance,
)


def _write_temperature_plan_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "source_strategy",
        "market_ticker",
        "temperature_client_order_id",
        "maker_entry_edge_conservative_net_total",
        "estimated_entry_cost_dollars",
        "captured_at_utc",
    ]
    extra_fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames and key not in extra_fieldnames:
                extra_fieldnames.append(key)
    fieldnames = fieldnames + extra_fieldnames
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _set_file_time(path: Path, ts: float) -> None:
    os.utime(path, (ts, ts))


def test_temperature_profitability_expected_only_without_settlements(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-abc",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
            }
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["planned_orders_total"] == 1
    assert summary["expected_edge_total_dollars"] == 0.08
    assert summary["expected_cost_total_dollars"] == 0.95
    assert summary["orders_settled"] == 0
    assert summary["win_rate"] is None


def test_temperature_profitability_prelive_calibration_tracks_conversion_ratios(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-plan-a",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "temperature_client_order_id": "temp-plan-b",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.85",
            },
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-prelive",
                "captured_at_utc": now.isoformat(),
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-plan-a",
                "exchange_order_id": "ord-prelive-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-prelive",
                "captured_at_utc": now.isoformat(),
                "event_type": "partial_fill",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-plan-a",
                "exchange_order_id": "ord-prelive-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-prelive",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-plan-a",
                "exchange_order_id": "ord-prelive-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    calibration = summary["prelive_calibration"]
    assert calibration["planned_orders_total"] == 2
    assert calibration["orders_submitted"] == 1
    assert calibration["orders_with_fills"] == 1
    assert calibration["orders_settled"] == 1
    assert calibration["expected_edge_density"] == 0.1
    assert calibration["submission_conversion"] == 0.5
    assert calibration["fill_conversion"] == 1.0
    assert calibration["settlement_conversion"] == 1.0
    assert calibration["execution_gap"] == {
        "submission": False,
        "fill": False,
        "settlement": False,
    }
    assert summary["realized_pnl_total_dollars"] == 0.2
    assert summary["orders_settled"] == 1
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 1


def test_temperature_profitability_prelive_calibration_handles_zero_denominators_and_gaps(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-gap",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
            }
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    calibration = summary["prelive_calibration"]
    assert calibration["planned_orders_total"] == 1
    assert calibration["orders_submitted"] == 0
    assert calibration["orders_with_fills"] == 0
    assert calibration["orders_settled"] == 0
    assert calibration["expected_edge_density"] == 0.08
    assert calibration["submission_conversion"] == 0.0
    assert calibration["fill_conversion"] is None
    assert calibration["settlement_conversion"] is None
    assert calibration["execution_gap"] == {
        "submission": True,
        "fill": False,
        "settlement": False,
    }

    empty_out_dir = tmp_path / "empty_outputs"
    empty_summary = run_kalshi_temperature_profitability(
        output_dir=str(empty_out_dir),
        hours=24.0,
        now=now,
    )
    empty_calibration = empty_summary["prelive_calibration"]
    assert empty_calibration["planned_orders_total"] == 0
    assert empty_calibration["expected_edge_density"] is None
    assert empty_calibration["submission_conversion"] is None
    assert empty_calibration["fill_conversion"] is None
    assert empty_calibration["settlement_conversion"] is None
    assert empty_calibration["execution_gap"] == {
        "submission": False,
        "fill": False,
        "settlement": False,
    }


def test_temperature_profitability_uses_row_and_filename_timestamps_not_mtime(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-stable-ts",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-13T05:59:01+00:00",
            }
        ],
    )
    # Simulate sync/copy drift where mtime is stale but artifact/row timestamps are valid.
    _set_file_time(plan_csv, datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc).timestamp())

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["planned_orders_total"] == 1
    assert summary["expected_edge_total_dollars"] == 0.08


def test_temperature_profitability_excludes_stale_stamped_plan_even_if_mtime_recent(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    # Filename stamp is stale/out-of-window, but mtime is fresh to emulate copy/sync.
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260401_010000.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-stale-stamp",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "",
            }
        ],
    )
    _set_file_time(plan_csv, (now - timedelta(hours=1)).timestamp())

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["planned_orders_total"] == 0
    assert summary["expected_edge_total_dollars"] == 0.0
    assert summary["expected_cost_total_dollars"] == 0.0


def test_temperature_profitability_excludes_future_plan_rows(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-future-plan",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-13T08:00:00+00:00",
            }
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["planned_orders_total"] == 0
    assert summary["expected_edge_total_dollars"] == 0.0


def test_temperature_profitability_realized_win_rate_and_expected_delta(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-abc",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-1",
                "captured_at_utc": now.isoformat(),
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-abc",
                "exchange_order_id": "ord-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-1",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-abc",
                "exchange_order_id": "ord-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-2",
                "captured_at_utc": now.isoformat(),
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-def",
                "exchange_order_id": "ord-2",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-2",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-def",
                "exchange_order_id": "ord-2",
                "realized_pnl_dollars": -0.1,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["orders_settled"] == 2
    assert summary["orders_settled_with_numeric_pnl"] == 2
    assert summary["wins"] == 1
    assert summary["losses"] == 1
    assert summary["win_rate"] == 0.5
    assert summary["realized_pnl_total_dollars"] == 0.1
    expected_vs_realized = summary["expected_vs_realized"]
    assert expected_vs_realized["matched_settled_orders"] == 1
    assert expected_vs_realized["matched_expected_edge_total_dollars"] == 0.08
    assert expected_vs_realized["matched_realized_pnl_total_dollars"] == 0.2
    assert expected_vs_realized["matched_realized_minus_expected_dollars"] == 0.12


def test_temperature_profitability_prefers_latest_plan_row_for_duplicate_client_order_id(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    first_plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_040000.csv"
    second_plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        first_plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-dup-client",
                "maker_entry_edge_conservative_net_total": "0.05",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T04:00:00+00:00",
            }
        ],
    )
    _write_temperature_plan_csv(
        second_plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-dup-client",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.92",
                "captured_at_utc": "2026-04-13T05:59:01+00:00",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-latest-map",
                "captured_at_utc": now.isoformat(),
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-dup-client",
                "exchange_order_id": "ord-latest-map-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-latest-map",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-dup-client",
                "exchange_order_id": "ord-latest-map-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["planned_orders_total"] == 2
    expected_vs_realized = summary["expected_vs_realized"]
    assert expected_vs_realized["matched_settled_orders"] == 1
    assert expected_vs_realized["matched_expected_edge_total_dollars"] == 0.12
    assert expected_vs_realized["matched_realized_minus_expected_dollars"] == 0.08


def test_temperature_profitability_matches_expected_by_client_and_ticker_when_client_reused(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-reused-client",
                "maker_entry_edge_conservative_net_total": "0.05",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T05:58:00+00:00",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "temperature_client_order_id": "temp-reused-client",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-13T05:59:00+00:00",
            },
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-reused-client",
                "captured_at_utc": now.isoformat(),
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-reused-client",
                "exchange_order_id": "ord-reused-client-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-reused-client",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-reused-client",
                "exchange_order_id": "ord-reused-client-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    expected_vs_realized = summary["expected_vs_realized"]
    assert expected_vs_realized["matched_settled_orders"] == 1
    assert expected_vs_realized["matched_expected_edge_total_dollars"] == 0.05
    assert expected_vs_realized["matched_realized_minus_expected_dollars"] == 0.15


def test_temperature_profitability_excludes_future_execution_events(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-abc",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": now.isoformat(),
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-future",
                "captured_at_utc": "2026-04-13T08:00:00+00:00",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-abc",
                "exchange_order_id": "ord-future-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-future",
                "captured_at_utc": "2026-04-13T08:00:00+00:00",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-abc",
                "exchange_order_id": "ord-future-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 0
    assert summary["orders_settled"] == 0
    assert summary["orders_settled_with_numeric_pnl"] == 0
    assert summary["realized_pnl_total_dollars"] == 0.0


def test_temperature_profitability_parses_zulu_event_timestamps_with_correct_windowing(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-zulu",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-13T05:55:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-zulu-in",
                "captured_at_utc": "2026-04-13T05:40:00Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-zulu",
                "exchange_order_id": "ord-zulu-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-zulu-in",
                "captured_at_utc": "2026-04-13T05:50:00Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-zulu",
                "exchange_order_id": "ord-zulu-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-zulu-future",
                "captured_at_utc": "2026-04-13T08:00:00Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-zulu-future",
                "exchange_order_id": "ord-zulu-2",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-zulu-future",
                "captured_at_utc": "2026-04-13T08:00:00Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-zulu-future",
                "exchange_order_id": "ord-zulu-2",
                "realized_pnl_dollars": 0.5,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 1
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["wins"] == 1
    assert summary["realized_pnl_total_dollars"] == 0.2


def test_temperature_profitability_excludes_non_temperature_kx_rows_without_strategy_or_temp_id(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "",
                "market_ticker": "KXRAINNY-26APR13-B0.5",
                "temperature_client_order_id": "rain-abc",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-13T05:55:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-rain",
                "captured_at_utc": "2026-04-13T05:50:00Z",
                "event_type": "order_submitted",
                "market_ticker": "KXRAINNY-26APR13-B0.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "rain-abc",
                "exchange_order_id": "ord-rain-1",
                "payload": {},
            },
            {
                "run_id": "run-rain",
                "captured_at_utc": "2026-04-13T05:51:00Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXRAINNY-26APR13-B0.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "rain-abc",
                "exchange_order_id": "ord-rain-1",
                "realized_pnl_dollars": 0.3,
                "payload": {},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["planned_orders_total"] == 0
    assert summary["expected_edge_total_dollars"] == 0.0
    assert summary["orders_submitted"] == 0
    assert summary["orders_settled"] == 0
    assert summary["realized_pnl_total_dollars"] == 0.0


def test_temperature_profitability_ticker_fallback_keeps_hourly_temp_and_rejects_macro(tmp_path: Path) -> None:
    now = datetime(2026, 4, 15, 20, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260415_195901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "",
                "market_ticker": "KXTEMPNYCH-26APR1514-T80.99",
                "temperature_client_order_id": "",
                "maker_entry_edge_conservative_net_total": "0.07",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-15T19:59:00Z",
            },
            {
                "source_strategy": "",
                "market_ticker": "KXHIGHINFLATION-26DEC-T3.0",
                "temperature_client_order_id": "",
                "maker_entry_edge_conservative_net_total": "0.11",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-15T19:59:10Z",
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["planned_orders_total"] == 1
    assert summary["expected_edge_total_dollars"] == 0.07
    assert summary["expected_cost_total_dollars"] == 0.9


def test_temperature_profitability_excludes_strategy_tagged_macro_tickers(tmp_path: Path) -> None:
    now = datetime(2026, 4, 15, 20, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260415_195901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHINFLATION-26DEC-T3.0",
                "temperature_client_order_id": "temp-macro",
                "maker_entry_edge_conservative_net_total": "0.11",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-15T19:59:20Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-macro",
                "captured_at_utc": "2026-04-15T19:59:21Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHINFLATION-26DEC-T3.0",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-macro",
                "exchange_order_id": "ord-macro-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-macro",
                "captured_at_utc": "2026-04-15T19:59:30Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHINFLATION-26DEC-T3.0",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-macro",
                "exchange_order_id": "ord-macro-1",
                "realized_pnl_dollars": 0.4,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["planned_orders_total"] == 0
    assert summary["expected_edge_total_dollars"] == 0.0
    assert summary["orders_submitted"] == 0
    assert summary["orders_settled"] == 0
    assert summary["realized_pnl_total_dollars"] == 0.0


def test_temperature_profitability_uses_payload_market_ticker_when_top_level_missing(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-payload-fallback",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-payload-fallback",
                "captured_at_utc": "2026-04-16T01:59:10Z",
                "event_type": "order_submitted",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-payload-fallback",
                "exchange_order_id": "ord-payload-fallback",
                "payload": {
                    "source_strategy": "temperature_constraints",
                    "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                },
            },
            {
                "run_id": "run-payload-fallback",
                "captured_at_utc": "2026-04-16T01:59:50Z",
                "event_type": "settlement_outcome",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-payload-fallback",
                "exchange_order_id": "ord-payload-fallback",
                "realized_pnl_dollars": 0.2,
                "payload": {
                    "source_strategy": "temperature_constraints",
                    "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                },
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 1
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["wins"] == 1
    assert summary["realized_pnl_total_dollars"] == 0.2


def test_temperature_profitability_accepts_temp_strategy_events_with_missing_ticker_fields(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-missing-ticker",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-missing-ticker",
                "captured_at_utc": "2026-04-16T01:59:10Z",
                "event_type": "order_submitted",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-missing-ticker",
                "exchange_order_id": "ord-missing-ticker",
                "payload": {
                    "source_strategy": "temperature_constraints",
                },
            },
            {
                "run_id": "run-missing-ticker",
                "captured_at_utc": "2026-04-16T01:59:50Z",
                "event_type": "settlement_outcome",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-missing-ticker",
                "exchange_order_id": "ord-missing-ticker",
                "realized_pnl_dollars": 0.2,
                "payload": {
                    "source_strategy": "temperature_constraints",
                },
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 1
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["wins"] == 1
    assert summary["realized_pnl_total_dollars"] == 0.2
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 1
    assert summary["expected_vs_realized"]["matched_expected_edge_total_dollars"] == 0.08


def test_temperature_profitability_uses_payload_client_order_id_when_top_level_missing(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-payload-client",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-payload-client",
                "captured_at_utc": "2026-04-16T01:59:10Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "",
                "exchange_order_id": "",
                "payload": {
                    "source_strategy": "temperature_constraints",
                    "client_order_id": "temp-payload-client",
                },
            },
            {
                "run_id": "run-payload-client",
                "captured_at_utc": "2026-04-16T01:59:50Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "",
                "exchange_order_id": "",
                "realized_pnl_dollars": 0.2,
                "payload": {
                    "source_strategy": "temperature_constraints",
                    "client_order_id": "temp-payload-client",
                },
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 1
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["wins"] == 1
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 1
    assert summary["expected_vs_realized"]["matched_expected_edge_total_dollars"] == 0.1


def test_temperature_profitability_bridges_exchange_to_client_for_settlement_matching(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-bridge-client",
                "maker_entry_edge_conservative_net_total": "0.09",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-bridge",
                "captured_at_utc": "2026-04-16T01:59:05Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-bridge-client",
                "exchange_order_id": "",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-bridge",
                "captured_at_utc": "2026-04-16T01:59:20Z",
                "event_type": "partial_fill",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-bridge-client",
                "exchange_order_id": "ord-bridge-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-bridge",
                "captured_at_utc": "2026-04-16T01:59:50Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "",
                "exchange_order_id": "ord-bridge-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 1
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 1
    assert summary["expected_vs_realized"]["matched_expected_edge_total_dollars"] == 0.09
    assert summary["expected_vs_realized"]["matched_realized_minus_expected_dollars"] == 0.11


def test_temperature_profitability_matches_expected_with_case_mismatched_tickers(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "kxhighmia-26apr16-b80.5",
                "temperature_client_order_id": "temp-case-match",
                "maker_entry_edge_conservative_net_total": "0.11",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-case-match",
                "captured_at_utc": "2026-04-16T01:59:10Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-case-match",
                "exchange_order_id": "ord-case-match-1",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-case-match",
                "captured_at_utc": "2026-04-16T01:59:50Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-case-match",
                "exchange_order_id": "ord-case-match-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_settled"] == 1
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 1
    assert summary["expected_vs_realized"]["matched_expected_edge_total_dollars"] == 0.11
    assert summary["expected_vs_realized"]["matched_realized_minus_expected_dollars"] == 0.09


def test_temperature_profitability_retains_exchange_only_settlement_after_client_linking_fill(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-link-fill",
                "maker_entry_edge_conservative_net_total": "0.13",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-link-fill",
                "captured_at_utc": "2026-04-16T01:59:05Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-link-fill",
                "exchange_order_id": "",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-link-fill",
                "captured_at_utc": "2026-04-16T01:59:10Z",
                "event_type": "partial_fill",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-link-fill",
                "exchange_order_id": "ord-link-fill-1",
                "payload": {},
            },
            {
                "run_id": "run-link-fill",
                "captured_at_utc": "2026-04-16T01:59:50Z",
                "event_type": "settlement_outcome",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "",
                "exchange_order_id": "ord-link-fill-1",
                "realized_pnl_dollars": 0.2,
                "payload": {},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 1
    assert summary["orders_with_fills"] >= 1
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["wins"] == 1
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 1
    assert summary["expected_vs_realized"]["matched_expected_edge_total_dollars"] == 0.13
    assert summary["expected_vs_realized"]["matched_realized_minus_expected_dollars"] == 0.07


def test_temperature_profitability_counts_fills_when_submit_client_key_and_fill_exchange_key(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-fill-count",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-fill-count",
                "captured_at_utc": "2026-04-16T01:59:01Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-fill-count",
                "exchange_order_id": "",
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-fill-count",
                "captured_at_utc": "2026-04-16T01:59:02Z",
                "event_type": "partial_fill",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-fill-count",
                "exchange_order_id": "ord-fill-count-1",
                "payload": {},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 1
    assert summary["orders_with_fills"] == 1
    assert summary["orders_settled"] == 0


def test_temperature_profitability_handles_out_of_order_settlement_before_fill_link_event(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-oo-client",
                "maker_entry_edge_conservative_net_total": "0.14",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                # Settlement arrives first and has exchange id only.
                "run_id": "run-oo",
                "captured_at_utc": "2026-04-16T01:59:10Z",
                "event_type": "settlement_outcome",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "",
                "exchange_order_id": "ord-oo-1",
                "realized_pnl_dollars": 0.2,
                "payload": {},
            },
            {
                # Fill event later provides exchange<->client linkage.
                "run_id": "run-oo",
                "captured_at_utc": "2026-04-16T01:59:20Z",
                "event_type": "partial_fill",
                "market_ticker": "",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-oo-client",
                "exchange_order_id": "ord-oo-1",
                "payload": {},
            },
            {
                # Submitted context also lands later in window.
                "run_id": "run-oo",
                "captured_at_utc": "2026-04-16T01:59:25Z",
                "event_type": "order_submitted",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-oo-client",
                "exchange_order_id": "",
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_submitted"] == 1
    assert summary["orders_with_fills"] == 1
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 1
    assert summary["expected_vs_realized"]["matched_expected_edge_total_dollars"] == 0.14
    assert summary["expected_vs_realized"]["matched_realized_minus_expected_dollars"] == 0.06


def test_temperature_profitability_falls_back_to_unique_market_ticker_when_client_missing(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-unique-ticker",
                "maker_entry_edge_conservative_net_total": "0.16",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-unique-ticker",
                "captured_at_utc": "2026-04-16T01:59:50Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "",
                "exchange_order_id": "",
                "realized_pnl_dollars": 0.2,
                "payload": {},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 1
    assert summary["expected_vs_realized"]["matched_expected_edge_total_dollars"] == 0.16
    assert summary["expected_vs_realized"]["matched_realized_minus_expected_dollars"] == 0.04


def test_temperature_profitability_does_not_ticker_fallback_when_ticker_is_ambiguous(tmp_path: Path) -> None:
    now = datetime(2026, 4, 16, 2, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260416_015901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-ambig-a",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:58:00Z",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "temperature_client_order_id": "temp-ambig-b",
                "maker_entry_edge_conservative_net_total": "0.22",
                "estimated_entry_cost_dollars": "0.95",
                "captured_at_utc": "2026-04-16T01:59:00Z",
            },
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-ambig-ticker",
                "captured_at_utc": "2026-04-16T01:59:50Z",
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR16-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "",
                "exchange_order_id": "",
                "realized_pnl_dollars": 0.2,
                "payload": {},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["orders_settled"] == 1
    assert summary["orders_settled_with_numeric_pnl"] == 1
    assert summary["expected_vs_realized"]["matched_settled_orders"] == 0
    assert summary["expected_vs_realized"]["matched_expected_edge_total_dollars"] == 0.0
    assert summary["expected_vs_realized"]["matched_realized_minus_expected_dollars"] == 0.0


def test_temperature_profitability_regime_breakdown_ranks_negative_and_positive_groups(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-neg-1",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T05:40:00+00:00",
                "settlement_station": "KBUF",
                "side": "yes",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_impossible",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B62.5",
                "temperature_client_order_id": "temp-neg-2",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T05:41:00+00:00",
                "settlement_station": "KBUF",
                "side": "yes",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_impossible",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "temperature_client_order_id": "temp-pos-1",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.88",
                "captured_at_utc": "2026-04-13T05:42:00+00:00",
                "settlement_station": "KMIA",
                "side": "no",
                "policy_metar_local_hour": "15",
                "constraint_status": "no_interval_infeasible",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B81.5",
                "temperature_client_order_id": "temp-pos-2",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.88",
                "captured_at_utc": "2026-04-13T05:43:00+00:00",
                "settlement_station": "KMIA",
                "side": "no",
                "policy_metar_local_hour": "15",
                "constraint_status": "no_interval_infeasible",
            },
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-neg-1",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-neg-1",
                "exchange_order_id": "ord-neg-1",
                "realized_pnl_dollars": -0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-neg-2",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B62.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-neg-2",
                "exchange_order_id": "ord-neg-2",
                "realized_pnl_dollars": -0.1,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-pos-1",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-pos-1",
                "exchange_order_id": "ord-pos-1",
                "realized_pnl_dollars": 0.25,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-pos-2",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B81.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-pos-2",
                "exchange_order_id": "ord-pos-2",
                "realized_pnl_dollars": 0.15,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    regime_breakdown = summary["regime_breakdown"]
    assert regime_breakdown["minimum_trades_for_ranked_regimes"] == 2
    settlement_station = regime_breakdown["dimension_regimes"]["settlement_station"]
    assert settlement_station["KBUF"]["trades"] == 2
    assert settlement_station["KMIA"]["trades"] == 2
    assert settlement_station["KBUF"]["realized_pnl_sum"] == -0.3
    assert settlement_station["KMIA"]["realized_pnl_sum"] == 0.4

    side = regime_breakdown["dimension_regimes"]["side"]
    assert side["yes"]["trades"] == 2
    assert side["no"]["trades"] == 2

    local_hour = regime_breakdown["dimension_regimes"]["local_hour"]
    assert local_hour["14"]["trades"] == 2
    assert local_hour["15"]["trades"] == 2

    signal_bucket = regime_breakdown["dimension_regimes"]["signal_bucket"]
    assert signal_bucket["yes_impossible"]["trades"] == 2
    assert signal_bucket["no_interval_infeasible"]["trades"] == 2

    top_negative_regimes = summary["top_negative_regimes"]
    top_positive_regimes = summary["top_positive_regimes"]
    assert top_negative_regimes[0]["settlement_station"] == "KBUF"
    assert top_negative_regimes[0]["realized_pnl_sum"] == -0.3
    assert top_positive_regimes[0]["settlement_station"] == "KMIA"
    assert top_positive_regimes[0]["realized_pnl_sum"] == 0.4
    assert top_negative_regimes[0]["edge_realization_ratio"] == -1.5
    assert top_positive_regimes[0]["edge_realization_ratio"] == 1.666667


def test_temperature_profitability_regime_breakdown_sparse_falls_back_cleanly(tmp_path: Path) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-sparse",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T05:40:00+00:00",
                "settlement_station": "KBUF",
                "side": "yes",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_impossible",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-sparse",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-sparse",
                "exchange_order_id": "ord-sparse-1",
                "realized_pnl_dollars": -0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            }
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    regime_breakdown = summary["regime_breakdown"]
    assert regime_breakdown["minimum_trades_for_ranked_regimes"] == 2
    assert summary["top_negative_regimes"] == []
    assert summary["top_positive_regimes"] == []
    assert regime_breakdown["dimension_regimes"]["settlement_station"]["KBUF"]["trades"] == 1
    assert regime_breakdown["dimension_regimes"]["side"]["yes"]["trades"] == 1
    assert regime_breakdown["dimension_regimes"]["local_hour"]["14"]["trades"] == 1
    assert regime_breakdown["dimension_regimes"]["signal_bucket"]["yes_impossible"]["trades"] == 1


def test_temperature_profitability_regime_breakdown_uses_signal_bucket_column_when_constraint_status_missing(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "temperature_client_order_id": "temp-signal-bucket-only",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.88",
                "captured_at_utc": "2026-04-13T05:42:00+00:00",
                "settlement_station": "KMIA",
                "side": "no",
                "policy_metar_local_hour": "15",
                "signal_bucket": "no_interval_infeasible",
            }
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-signal-bucket-only",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-signal-bucket-only",
                "exchange_order_id": "ord-signal-bucket-only",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            }
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    signal_bucket = summary["regime_breakdown"]["dimension_regimes"]["signal_bucket"]
    assert signal_bucket["no_interval_infeasible"]["trades"] == 1


def test_temperature_profitability_risk_off_diagnostics_triggers_for_concentration_and_stale_metar(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-risk-neg-1",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T05:40:00+00:00",
                "settlement_station": "KBUF",
                "side": "yes",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_impossible",
                "metar_observation_age_minutes": "90",
                "policy_metar_max_age_minutes_applied": "20",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B62.5",
                "temperature_client_order_id": "temp-risk-neg-2",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T05:41:00+00:00",
                "settlement_station": "KBUF",
                "side": "yes",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_impossible",
                "metar_observation_age_minutes": "85",
                "policy_metar_max_age_minutes_applied": "20",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "temperature_client_order_id": "temp-risk-pos-1",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.88",
                "captured_at_utc": "2026-04-13T05:42:00+00:00",
                "settlement_station": "KMIA",
                "side": "no",
                "policy_metar_local_hour": "15",
                "constraint_status": "no_interval_infeasible",
                "metar_observation_age_minutes": "10",
                "policy_metar_max_age_minutes_applied": "20",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B81.5",
                "temperature_client_order_id": "temp-risk-pos-2",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.88",
                "captured_at_utc": "2026-04-13T05:43:00+00:00",
                "settlement_station": "KMIA",
                "side": "no",
                "policy_metar_local_hour": "15",
                "constraint_status": "no_interval_infeasible",
                "metar_observation_age_minutes": "9",
                "policy_metar_max_age_minutes_applied": "20",
            },
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-risk-neg-1",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-risk-neg-1",
                "exchange_order_id": "ord-risk-neg-1",
                "realized_pnl_dollars": -0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-risk-neg-2",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B62.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-risk-neg-2",
                "exchange_order_id": "ord-risk-neg-2",
                "realized_pnl_dollars": -0.1,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-risk-pos-1",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-risk-pos-1",
                "exchange_order_id": "ord-risk-pos-1",
                "realized_pnl_dollars": 0.2,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-risk-pos-2",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B81.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-risk-pos-2",
                "exchange_order_id": "ord-risk-pos-2",
                "realized_pnl_dollars": 0.1,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    diagnostics = summary["risk_off_diagnostics"]
    assert diagnostics["temporary_risk_off_recommended"] is True
    assert diagnostics["temporary_risk_off_reason_codes"] == [
        "negative_regime_concentration",
        "stale_metar_regime_stress",
    ]
    assert diagnostics["negative_regime_concentration"]["triggered"] is True
    assert diagnostics["negative_regime_concentration"]["worst_negative_loss_share"] == 1.0
    assert diagnostics["stale_metar_regime_stress"]["dimensions_available"] is True
    assert diagnostics["stale_metar_regime_stress"]["triggered"] is True
    assert diagnostics["stale_metar_regime_stress"]["stale_trade_count"] == 2
    assert diagnostics["stale_metar_regime_stress"]["stale_negative_share_of_negative_trades"] == 1.0


def test_temperature_profitability_risk_off_diagnostics_does_not_trigger_for_balanced_losses(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260413_055901.csv"
    _write_temperature_plan_csv(
        plan_csv,
        [
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "temperature_client_order_id": "temp-balanced-a1",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T05:40:00+00:00",
                "settlement_station": "KBUF",
                "side": "yes",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_impossible",
                "metar_observation_age_minutes": "10",
                "policy_metar_max_age_minutes_applied": "20",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHLAX-26APR13-B62.5",
                "temperature_client_order_id": "temp-balanced-a2",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.90",
                "captured_at_utc": "2026-04-13T05:41:00+00:00",
                "settlement_station": "KBUF",
                "side": "yes",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_impossible",
                "metar_observation_age_minutes": "12",
                "policy_metar_max_age_minutes_applied": "20",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "temperature_client_order_id": "temp-balanced-b1",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.88",
                "captured_at_utc": "2026-04-13T05:42:00+00:00",
                "settlement_station": "KMIA",
                "side": "no",
                "policy_metar_local_hour": "15",
                "constraint_status": "no_interval_infeasible",
                "metar_observation_age_minutes": "11",
                "policy_metar_max_age_minutes_applied": "20",
            },
            {
                "source_strategy": "temperature_constraints",
                "market_ticker": "KXHIGHMIA-26APR13-B81.5",
                "temperature_client_order_id": "temp-balanced-b2",
                "maker_entry_edge_conservative_net_total": "0.12",
                "estimated_entry_cost_dollars": "0.88",
                "captured_at_utc": "2026-04-13T05:43:00+00:00",
                "settlement_station": "KMIA",
                "side": "no",
                "policy_metar_local_hour": "15",
                "constraint_status": "no_interval_infeasible",
                "metar_observation_age_minutes": "9",
                "policy_metar_max_age_minutes_applied": "20",
            },
        ],
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)
    append_execution_events(
        journal_db_path=journal_path,
        events=[
            {
                "run_id": "run-balanced-a1",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B61.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-balanced-a1",
                "exchange_order_id": "ord-balanced-a1",
                "realized_pnl_dollars": -0.1,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-balanced-a2",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHLAX-26APR13-B62.5",
                "event_family": "weather_climate",
                "side": "yes",
                "client_order_id": "temp-balanced-a2",
                "exchange_order_id": "ord-balanced-a2",
                "realized_pnl_dollars": -0.1,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-balanced-b1",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B80.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-balanced-b1",
                "exchange_order_id": "ord-balanced-b1",
                "realized_pnl_dollars": -0.1,
                "payload": {"source_strategy": "temperature_constraints"},
            },
            {
                "run_id": "run-balanced-b2",
                "captured_at_utc": now.isoformat(),
                "event_type": "settlement_outcome",
                "market_ticker": "KXHIGHMIA-26APR13-B81.5",
                "event_family": "weather_climate",
                "side": "no",
                "client_order_id": "temp-balanced-b2",
                "exchange_order_id": "ord-balanced-b2",
                "realized_pnl_dollars": -0.1,
                "payload": {"source_strategy": "temperature_constraints"},
            },
        ],
    )

    summary = run_kalshi_temperature_profitability(
        output_dir=str(out_dir),
        hours=24.0,
        now=now,
    )

    diagnostics = summary["risk_off_diagnostics"]
    assert diagnostics["temporary_risk_off_recommended"] is False
    assert diagnostics["temporary_risk_off_reason_codes"] == []
    assert diagnostics["temporary_risk_off_reason"] == ""
    assert diagnostics["negative_regime_concentration"]["triggered"] is False
    assert diagnostics["negative_regime_concentration"]["ranked_negative_regime_count"] == 2
    assert diagnostics["negative_regime_concentration"]["worst_negative_loss_share"] == 0.5
    assert diagnostics["stale_metar_regime_stress"]["dimensions_available"] is True
    assert diagnostics["stale_metar_regime_stress"]["stale_trade_count"] == 0
    assert diagnostics["stale_metar_regime_stress"]["triggered"] is False


def test_refill_trial_balance_writes_state_file(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 10, 30, tzinfo=timezone.utc)

    summary = run_kalshi_temperature_refill_trial_balance(
        output_dir=str(out_dir),
        starting_balance_dollars=1250.0,
        reason="strategy_v2_trial",
        now=now,
    )

    state_file = out_dir / "checkpoints" / "trial_balance_state.json"
    assert state_file.exists()
    payload = summary["previous_state"]
    assert isinstance(payload, dict)
    assert summary["starting_balance_dollars"] == 1250.0
    assert summary["reset_reason"] == "strategy_v2_trial"
    assert summary["state_file"] == str(state_file)


def test_refill_trial_balance_overwrites_readonly_state_inode(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    state_file = out_dir / "checkpoints" / "trial_balance_state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "starting_balance_dollars": 900.0,
                "reset_epoch": 1.0,
                "reset_at_utc": "2026-04-01T00:00:00+00:00",
                "reset_reason": "old",
            }
        ),
        encoding="utf-8",
    )
    os.chmod(state_file, 0o444)

    now = datetime(2026, 4, 13, 11, 0, tzinfo=timezone.utc)
    summary = run_kalshi_temperature_refill_trial_balance(
        output_dir=str(out_dir),
        starting_balance_dollars=1400.0,
        reason="readonly_rewrite",
        now=now,
    )

    payload = json.loads(state_file.read_text(encoding="utf-8"))
    assert summary["status"] == "ready"
    assert summary["previous_state"]["starting_balance_dollars"] == 900.0
    assert payload["starting_balance_dollars"] == 1400.0
    assert payload["reset_reason"] == "readonly_rewrite"


def test_refill_trial_balance_rejects_non_positive_balance(tmp_path: Path) -> None:
    try:
        run_kalshi_temperature_refill_trial_balance(
            output_dir=str(tmp_path / "outputs"),
            starting_balance_dollars=0.0,
        )
    except ValueError:
        return
    raise AssertionError("expected ValueError for non-positive starting balance")
