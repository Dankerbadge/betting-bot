from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import csv


def test_summarize_window_emits_extended_trial_balance_horizons(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    windows = (
        payload.get("profitability_overview", {})
        .get("trial_balance", {})
        .get("windows", {})
    )

    required_keys = {"1d", "7d", "14d", "21d", "28d", "3mo", "6mo", "1yr"}
    assert required_keys.issubset(set(windows.keys()))


def test_summarize_window_emits_last_settled_selection_keys(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    shadow = (
        payload.get("profitability_overview", {})
        .get("shadow_settled_reference", {})
    )

    assert "last_resolved_unique_market_side" in shadow
    assert "last_resolved_unique_shadow_order" in shadow
    assert "recent_resolved_unique_market_sides" in shadow
    assert "recent_resolved_unique_shadow_orders" in shadow

    assert shadow.get("last_resolved_unique_market_side") is None
    assert shadow.get("last_resolved_unique_shadow_order") is None
    assert isinstance(shadow.get("recent_resolved_unique_market_sides"), list)
    assert isinstance(shadow.get("recent_resolved_unique_shadow_orders"), list)

    trial = (
        payload.get("profitability_overview", {})
        .get("trial_balance", {})
    )
    assert isinstance(trial.get("duplicate_shadow_order_ids"), dict)
    assert isinstance(trial.get("duplicate_shadow_order_ids_total_unique"), int)
    assert isinstance(trial.get("duplicate_shadow_order_ids_returned"), int)
    assert isinstance(trial.get("duplicate_shadow_order_ids_truncated"), bool)
    assert isinstance(trial.get("duplicate_shadow_order_ids_truncated_count"), int)
    assert trial.get("duplicate_shadow_order_ids_top_n_limit") == 250
    assert trial.get("duplicate_shadow_order_ids_returned") <= 250
    cash_constrained = trial.get("cash_constrained", {})
    assert isinstance(cash_constrained, dict)
    assert isinstance(cash_constrained.get("current_balance_dollars"), (int, float, type(None)))
    assert isinstance(cash_constrained.get("skipped_for_insufficient_cash_count"), int)
    assert isinstance(cash_constrained.get("windows"), dict)

    profitability_file = payload.get("profitability_file")
    profitability_payload = (
        json.loads(Path(profitability_file).read_text(encoding="utf-8"))
        if isinstance(profitability_file, str) and profitability_file
        else {}
    )
    attribution = profitability_payload.get("attribution", {})
    assert "by_underlying_family" in attribution
    settled_shadow_attribution = (
        payload.get("profitability_overview", {})
        .get("settled_shadow_attribution", {})
    )
    assert settled_shadow_attribution.get("default_prediction_quality_basis") == "unique_market_side"
    assert isinstance(settled_shadow_attribution.get("layers"), dict)
    assert isinstance(settled_shadow_attribution.get("top_contributors"), dict)


def test_summarize_window_emits_approval_parameter_audit_and_detects_mismatch(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"
    intents_path = out_dir / "kalshi_temperature_trade_intents_20260418_000000.csv"

    fieldnames = [
        "intent_id",
        "policy_approved",
        "policy_reason",
        "market_ticker",
        "underlying_key",
        "side",
        "revalidation_status",
        "metar_observation_age_minutes",
        "policy_metar_max_age_minutes_applied",
        "constraint_status",
    ]
    with intents_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "intent_id": "intent-1",
                "policy_approved": "true",
                "policy_reason": "approved",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "no",
                "revalidation_status": "approved",
                "metar_observation_age_minutes": "42",
                "policy_metar_max_age_minutes_applied": "22.5",
                "constraint_status": "yes_impossible",
            }
        )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    audit = (
        payload.get("profitability_overview", {})
        .get("approval_parameter_audit", {})
    )

    assert audit.get("approved_rows") == 1
    assert audit.get("approved_rows_with_gate_mismatch") == 1
    assert audit.get("status") == "mismatch_detected"
    assert isinstance(audit.get("mismatch_by_gate"), dict)
    assert audit.get("mismatch_by_gate", {}).get("metar_age") == 1
    gate_metrics = audit.get("gate_metrics", {})
    assert gate_metrics.get("metar_age", {}).get("approved_rows_fail") == 1
    assert gate_metrics.get("metar_age", {}).get("approved_rows_with_threshold") == 1
    assert gate_metrics.get("metar_age", {}).get("approved_rows_missing_threshold") == 0
    assert gate_metrics.get("metar_age", {}).get("approved_evaluable_given_threshold_rate") == 1.0


def test_summarize_window_trial_balance_cache_hits_on_second_run(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path_first = tmp_path / "window_first.json"
    output_path_second = tmp_path / "window_second.json"
    plans_path = out_dir / "kalshi_temperature_trade_plan_20260418_000000.csv"

    fieldnames = [
        "source_strategy",
        "temperature_intent_id",
        "temperature_client_order_id",
        "client_order_id",
        "market_ticker",
        "temperature_underlying_key",
        "side",
        "maker_entry_price_dollars",
    ]
    with plans_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-1",
                "temperature_client_order_id": "temp-order-1",
                "client_order_id": "temp-order-1",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "temperature_underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "no",
                "maker_entry_price_dollars": "0.42",
            }
        )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    common_args = [
        "--out-dir",
        str(out_dir),
        "--start-epoch",
        "0",
        "--end-epoch",
        str(time.time()),
        "--label",
        "test",
    ]

    subprocess.run(
        [sys.executable, str(script_path), *common_args, "--output", str(output_path_first)],
        check=True,
        capture_output=True,
        text=True,
    )
    first_payload = json.loads(output_path_first.read_text(encoding="utf-8"))
    first_trial = (
        first_payload.get("profitability_overview", {})
        .get("trial_balance", {})
    )
    assert first_trial.get("cache_files_total") == 1
    assert first_trial.get("cache_files_parsed_this_run") == 1
    assert first_trial.get("cache_status") in {
        "initial_build",
        "rebuild_after_cache_version_mismatch",
        "rebuild_after_reset_epoch_changed",
    }

    subprocess.run(
        [sys.executable, str(script_path), *common_args, "--output", str(output_path_second)],
        check=True,
        capture_output=True,
        text=True,
    )
    second_payload = json.loads(output_path_second.read_text(encoding="utf-8"))
    second_trial = (
        second_payload.get("profitability_overview", {})
        .get("trial_balance", {})
    )
    assert second_trial.get("cache_status") == "cache_hit_no_new_files"
    assert second_trial.get("cache_files_total") == 1
    assert second_trial.get("cache_files_parsed_this_run") == 0
    assert second_trial.get("cache_files_skipped_this_run") == 1
    assert second_trial.get("cache_write_skipped") is True


def test_summarize_window_plan_attribution_prefers_approved_context_for_reused_intent(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"
    intents_path = out_dir / "kalshi_temperature_trade_intents_20260418_000000.csv"
    plans_path = out_dir / "kalshi_temperature_trade_plan_20260418_000000.csv"

    intent_fieldnames = [
        "intent_id",
        "policy_approved",
        "policy_reason",
        "market_ticker",
        "underlying_key",
        "side",
        "settlement_station",
        "policy_metar_local_hour",
        "constraint_status",
        "max_entry_price_dollars",
    ]
    with intents_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=intent_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "intent_id": "intent-1",
                "policy_approved": "true",
                "policy_reason": "approved",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "no",
                "settlement_station": "KNYC",
                "policy_metar_local_hour": "16",
                "constraint_status": "yes_impossible",
                "max_entry_price_dollars": "0.40",
            }
        )
        writer.writerow(
            {
                "intent_id": "intent-1",
                "policy_approved": "false",
                "policy_reason": "expected_edge_below_min",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "no",
                "settlement_station": "KNYC",
                "policy_metar_local_hour": "16",
                "constraint_status": "yes_impossible",
                "max_entry_price_dollars": "",
            }
        )

    plan_fieldnames = [
        "source_strategy",
        "temperature_intent_id",
        "temperature_client_order_id",
        "client_order_id",
        "market_ticker",
        "temperature_underlying_key",
        "side",
        "maker_entry_edge_conservative_net_total",
        "estimated_entry_cost_dollars",
        "maker_entry_price_dollars",
    ]
    with plans_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=plan_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-1",
                "temperature_client_order_id": "temp-order-1",
                "client_order_id": "temp-order-1",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "temperature_underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "no",
                "maker_entry_edge_conservative_net_total": "0.20",
                "estimated_entry_cost_dollars": "1.00",
                "maker_entry_price_dollars": "0.40",
            }
        )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    profitability_payload = json.loads(Path(payload["profitability_file"]).read_text(encoding="utf-8"))
    by_reason = (
        profitability_payload.get("attribution", {})
        .get("by_policy_reason", {})
    )
    assert by_reason.get("approved", {}).get("planned_orders") == 1
    assert by_reason.get("expected_edge_below_min", {}).get("planned_orders", 0) == 0


def test_summarize_window_includes_same_second_fractional_mtime(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"

    end_epoch = int(time.time())
    start_epoch = end_epoch - 3600
    intents_summary_path = out_dir / "kalshi_temperature_trade_intents_summary_20260418_000000.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "captured_at": "2026-04-18T00:00:00+00:00",
                "intents_total": 50,
                "intents_approved": 0,
                "planned_orders": 0,
                "policy_reason_counts": {"metar_observation_stale": 50},
            }
        ),
        encoding="utf-8",
    )
    # Simulate a file produced inside the terminal second represented by
    # end_epoch (e.g., mtime includes fractional component .4).
    os.utime(intents_summary_path, (end_epoch + 0.4, end_epoch + 0.4))

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            str(start_epoch),
            "--end-epoch",
            str(end_epoch),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload.get("files_count", {}).get("intents") == 1
    assert payload.get("totals", {}).get("intents_total") == 50
    assert payload.get("policy_reason_counts", {}).get("metar_observation_stale") == 50


def test_summarize_window_trial_balance_cache_overwrites_readonly_inode(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir = out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"

    # Seed a read-only cache file to emulate ownership/permission drift on
    # the existing inode. The writer should recover via atomic replace.
    trial_cache_path = checkpoints_dir / "trial_balance_cache.json"
    trial_cache_path.write_text(
        json.dumps(
            {
                "version": 1,
                "reset_epoch": 0.0,
                "parsed_files": {},
                "planned_rows_total": 0,
                "occurrence_counts": {},
                "canonical_rows": {},
            }
        ),
        encoding="utf-8",
    )
    trial_cache_path.chmod(0o444)

    plans_path = out_dir / "kalshi_temperature_trade_plan_20260418_000000.csv"
    fieldnames = [
        "source_strategy",
        "temperature_intent_id",
        "temperature_client_order_id",
        "client_order_id",
        "market_ticker",
        "temperature_underlying_key",
        "side",
        "maker_entry_price_dollars",
    ]
    with plans_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-ro-cache-1",
                "temperature_client_order_id": "temp-ro-cache-1",
                "client_order_id": "temp-ro-cache-1",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "temperature_underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "no",
                "maker_entry_price_dollars": "0.42",
            }
        )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    trial = (
        payload.get("profitability_overview", {})
        .get("trial_balance", {})
    )
    assert trial.get("cache_write_ok") is True
    assert trial.get("cache_write_error") is None
    rewritten_cache = json.loads(trial_cache_path.read_text(encoding="utf-8"))
    assert rewritten_cache.get("planned_rows_total", 0) >= 1


def test_summarize_window_summary_metrics_cache_hits_on_second_run(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path_first = tmp_path / "window_first.json"
    output_path_second = tmp_path / "window_second.json"
    intents_summary_path = out_dir / "kalshi_temperature_trade_intents_summary_20260418_000000.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "intents_total": 10,
                "intents_approved": 7,
                "intents_revalidated": 7,
                "revalidation_invalidated": 0,
                "status": "ready",
                "settlement_state_loaded": True,
                "policy_reason_counts": {"approved": 7, "metar_observation_stale": 3},
            }
        ),
        encoding="utf-8",
    )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    common_args = [
        "--out-dir",
        str(out_dir),
        "--start-epoch",
        "0",
        "--end-epoch",
        str(time.time()),
        "--label",
        "test",
    ]

    subprocess.run(
        [sys.executable, str(script_path), *common_args, "--output", str(output_path_first)],
        check=True,
        capture_output=True,
        text=True,
    )
    first_payload = json.loads(output_path_first.read_text(encoding="utf-8"))
    first_cache = first_payload.get("summary_metrics_cache", {})
    assert first_cache.get("misses", 0) >= 1
    assert first_cache.get("entries", 0) >= 1

    subprocess.run(
        [sys.executable, str(script_path), *common_args, "--output", str(output_path_second)],
        check=True,
        capture_output=True,
        text=True,
    )
    second_payload = json.loads(output_path_second.read_text(encoding="utf-8"))
    second_cache = second_payload.get("summary_metrics_cache", {})
    assert second_cache.get("hits", 0) >= 1
    assert second_cache.get("write_skipped") is True


def test_summarize_window_emits_previous_window_comparison_metrics(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir = out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    intents_summary_path = out_dir / "kalshi_temperature_trade_intents_summary_20260418_030000.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "intents_total": 100,
                "intents_approved": 10,
                "intents_revalidated": 10,
                "revalidation_invalidated": 0,
                "status": "ready",
                "settlement_state_loaded": True,
                "policy_reason_counts": {"approved": 10, "metar_observation_stale": 30},
            }
        ),
        encoding="utf-8",
    )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    output_path_first = checkpoints_dir / "station_tuning_window_12h_20260418_030001.json"
    output_path_second = checkpoints_dir / "station_tuning_window_12h_20260418_030002.json"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "12h",
            "--output",
            str(output_path_first),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    intents_summary_path.write_text(
        json.dumps(
            {
                "intents_total": 140,
                "intents_approved": 28,
                "intents_revalidated": 28,
                "revalidation_invalidated": 0,
                "status": "ready",
                "settlement_state_loaded": True,
                "policy_reason_counts": {"approved": 28, "metar_observation_stale": 20},
            }
        ),
        encoding="utf-8",
    )

    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "12h",
            "--output",
            str(output_path_second),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    second_payload = json.loads(output_path_second.read_text(encoding="utf-8"))
    comparison = second_payload.get("window_comparison", {})
    assert comparison.get("has_previous") is True
    assert str(comparison.get("previous_file", "")).endswith(output_path_first.name)
    metrics = comparison.get("metrics", {})
    intents_total = metrics.get("intents_total", {})
    assert intents_total.get("current") == 140
    assert intents_total.get("previous") == 100
    assert intents_total.get("delta") == 40
    approval_rate = metrics.get("approval_rate", {})
    assert approval_rate.get("current") == 0.2
    assert approval_rate.get("previous") == 0.1
    assert approval_rate.get("delta_percentage_points") == 10.0


def test_summarize_window_policy_loaded_string_false_remains_false(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"
    intents_summary_path = out_dir / "kalshi_temperature_trade_intents_summary_20260418_010000.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "intents_total": 1,
                "intents_approved": 1,
                "intents_revalidated": 1,
                "revalidation_invalidated": 0,
                "status": "ready",
                "settlement_state_loaded": True,
                "policy_reason_counts": {"approved": 1},
                "metar_age_policy_json": "metar_age_policy.json",
                "metar_age_policy_loaded": "false",
                "policy_version": "v-test",
            }
        ),
        encoding="utf-8",
    )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    profitability_path = Path(payload.get("profitability_file"))
    profitability_payload = json.loads(profitability_path.read_text(encoding="utf-8"))
    policy_provenance = (
        profitability_payload.get("provenance", {})
        .get("policy", {})
    )
    assert policy_provenance.get("active_policy_loaded") is False


def test_summarize_window_settlement_state_loaded_string_false_counts_as_false(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"
    intents_summary_path = out_dir / "kalshi_temperature_trade_intents_summary_20260418_020000.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "intents_total": 1,
                "intents_approved": 0,
                "intents_revalidated": 0,
                "revalidation_invalidated": 0,
                "status": "ready",
                "settlement_state_loaded": "false",
                "policy_reason_counts": {"settlement_finalization_blocked": 1},
            }
        ),
        encoding="utf-8",
    )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time()),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload.get("settlement_state_loaded_false_files") == 1


def test_summarize_window_trial_balance_threshold_fallback_resolves_out_of_window_plan(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"

    plans_path = out_dir / "kalshi_temperature_trade_plan_20260418_000000.csv"
    plans_fieldnames = [
        "source_strategy",
        "temperature_intent_id",
        "temperature_client_order_id",
        "client_order_id",
        "market_ticker",
        "temperature_underlying_key",
        "side",
        "maker_entry_price_dollars",
    ]
    with plans_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=plans_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-old-1",
                "temperature_client_order_id": "temp-order-old-1",
                "client_order_id": "temp-order-old-1",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "temperature_underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "yes",
                "maker_entry_price_dollars": "0.40",
            }
        )

    specs_path = out_dir / "kalshi_temperature_contract_specs_20260418_000000.csv"
    specs_fieldnames = ["market_ticker", "threshold_expression"]
    with specs_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=specs_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "threshold_expression": "at_least:70.5",
            }
        )

    settlement_path = out_dir / "kalshi_temperature_settlement_state_20260418_000000.json"
    settlement_path.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHNY|KNYC|2026-04-18": {"final_truth_value": 72.0},
                }
            }
        ),
        encoding="utf-8",
    )

    old_epoch = time.time() - 3600
    os.utime(plans_path, (old_epoch, old_epoch))
    os.utime(specs_path, (old_epoch, old_epoch))
    os.utime(settlement_path, (old_epoch, old_epoch))

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            str(time.time() - 60),
            "--end-epoch",
            str(time.time()),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    trial = payload.get("profitability_overview", {}).get("trial_balance", {})
    assert trial.get("resolved_counterfactual_trades_since_reset") == 1


def test_summarize_window_trial_balance_cash_replay_respects_contracts_per_order(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"
    checkpoints_dir = out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    # Simulate a small cash account that cannot afford a 2-contract order.
    (checkpoints_dir / "trial_balance_state.json").write_text(
        json.dumps(
            {
                "starting_balance_dollars": 0.50,
                "reset_epoch": 0.0,
                "reset_at_utc": "1970-01-01T00:00:00+00:00",
                "reset_reason": "unit_test",
            }
        ),
        encoding="utf-8",
    )

    plans_path = out_dir / "kalshi_temperature_trade_plan_20260418_000000.csv"
    plans_fieldnames = [
        "source_strategy",
        "temperature_intent_id",
        "temperature_client_order_id",
        "client_order_id",
        "market_ticker",
        "temperature_underlying_key",
        "side",
        "maker_entry_price_dollars",
        "contracts_per_order",
    ]
    with plans_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=plans_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-size-1",
                "temperature_client_order_id": "temp-size-1",
                "client_order_id": "temp-size-1",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "temperature_underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "yes",
                "maker_entry_price_dollars": "0.30",
                "contracts_per_order": "2",
            }
        )

    specs_path = out_dir / "kalshi_temperature_contract_specs_20260418_000000.csv"
    with specs_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["market_ticker", "threshold_expression"])
        writer.writeheader()
        writer.writerow(
            {
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "threshold_expression": "at_least:70.5",
            }
        )

    settlement_path = out_dir / "kalshi_temperature_settlement_state_20260418_000000.json"
    settlement_path.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHNY|KNYC|2026-04-18": {"final_truth_value": 72.0},
                }
            }
        ),
        encoding="utf-8",
    )

    now_epoch = time.time()
    old_epoch = now_epoch - 60
    os.utime(plans_path, (old_epoch, old_epoch))
    os.utime(specs_path, (old_epoch, old_epoch))
    os.utime(settlement_path, (old_epoch, old_epoch))

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(now_epoch + 5),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    trial = payload.get("profitability_overview", {}).get("trial_balance", {})
    cash = trial.get("cash_constrained", {})

    # Unconstrained reference executes 2 contracts and wins: (1 - 0.30) * 2 = +1.40
    assert trial.get("resolved_counterfactual_trades_since_reset") == 1
    assert trial.get("cumulative_counterfactual_pnl_dollars") == 1.4

    # Cash-constrained replay should skip because required entry cost is 0.60 > 0.50.
    assert cash.get("resolved_counterfactual_trades_since_reset") == 0
    assert cash.get("skipped_for_insufficient_cash_count") == 1
    assert cash.get("cumulative_counterfactual_pnl_dollars") == 0.0
    assert cash.get("current_balance_dollars") == 0.5


def test_summarize_window_scans_settlement_history_for_missing_latest_truth(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"

    plans_path = out_dir / "kalshi_temperature_trade_plan_20260418_000000.csv"
    plans_fieldnames = [
        "source_strategy",
        "temperature_intent_id",
        "temperature_client_order_id",
        "client_order_id",
        "market_ticker",
        "temperature_underlying_key",
        "side",
        "maker_entry_price_dollars",
    ]
    with plans_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=plans_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-backfill-1",
                "temperature_client_order_id": "temp-order-backfill-1",
                "client_order_id": "temp-order-backfill-1",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "temperature_underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "yes",
                "maker_entry_price_dollars": "0.40",
            }
        )

    specs_path = out_dir / "kalshi_temperature_contract_specs_20260418_000000.csv"
    with specs_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["market_ticker", "threshold_expression"])
        writer.writeheader()
        writer.writerow(
            {
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "threshold_expression": "at_least:70.5",
            }
        )

    settlement_older_path = out_dir / "kalshi_temperature_settlement_state_20260418_000000.json"
    settlement_older_path.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHNY|KNYC|2026-04-18": {"final_truth_value": 72.0},
                }
            }
        ),
        encoding="utf-8",
    )

    settlement_latest_path = out_dir / "kalshi_temperature_settlement_state_20260418_000100.json"
    settlement_latest_path.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHNY|KNYC|2026-04-18": {"final_truth_value": None},
                }
            }
        ),
        encoding="utf-8",
    )

    now_epoch = time.time()
    older_epoch = now_epoch - 120
    latest_epoch = now_epoch - 30
    os.utime(plans_path, (latest_epoch, latest_epoch))
    os.utime(specs_path, (latest_epoch, latest_epoch))
    os.utime(settlement_older_path, (older_epoch, older_epoch))
    os.utime(settlement_latest_path, (latest_epoch, latest_epoch))

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(now_epoch + 5),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    shadow = payload.get("profitability_overview", {}).get("shadow_settled_reference", {})
    trial = payload.get("profitability_overview", {}).get("trial_balance", {})

    assert shadow.get("resolved_unique_market_sides") == 1
    assert shadow.get("wins_unique_market_sides") == 1
    assert shadow.get("settlement_state_files_scanned_for_truth", 0) >= 2
    assert trial.get("resolved_counterfactual_trades_since_reset") == 1


def test_summarize_window_profitability_csv_parse_cache_hits_on_second_run(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path_first = tmp_path / "window_first.json"
    output_path_second = tmp_path / "window_second.json"

    plans_path = out_dir / "kalshi_temperature_trade_plan_20260418_000000.csv"
    plans_fieldnames = [
        "source_strategy",
        "temperature_intent_id",
        "temperature_client_order_id",
        "client_order_id",
        "market_ticker",
        "temperature_underlying_key",
        "side",
        "maker_entry_price_dollars",
        "maker_entry_edge_conservative_net_total",
        "estimated_entry_cost_dollars",
    ]
    with plans_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=plans_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-cache-1",
                "temperature_client_order_id": "temp-cache-1",
                "client_order_id": "temp-cache-1",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "temperature_underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "yes",
                "maker_entry_price_dollars": "0.40",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "10",
            }
        )

    intents_path = out_dir / "kalshi_temperature_trade_intents_20260418_000000.csv"
    intents_fieldnames = [
        "intent_id",
        "policy_approved",
        "policy_reason",
        "settlement_station",
        "policy_metar_local_hour",
        "constraint_status",
        "market_ticker",
        "underlying_key",
        "side",
        "max_entry_price_dollars",
        "metar_observation_age_minutes",
        "policy_metar_max_age_minutes_applied",
        "policy_expected_edge_net",
        "policy_min_expected_edge_net_required",
    ]
    with intents_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=intents_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "intent_id": "intent-cache-1",
                "policy_approved": "true",
                "policy_reason": "approved",
                "settlement_station": "KNYC",
                "policy_metar_local_hour": "12",
                "constraint_status": "yes_impossible",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "yes",
                "max_entry_price_dollars": "0.40",
                "metar_observation_age_minutes": "10",
                "policy_metar_max_age_minutes_applied": "22.5",
                "policy_expected_edge_net": "0.08",
                "policy_min_expected_edge_net_required": "0.02",
            }
        )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    end_epoch = str(time.time() + 30)
    common_args = [
        "--out-dir",
        str(out_dir),
        "--start-epoch",
        "0",
        "--end-epoch",
        end_epoch,
        "--label",
        "test",
    ]

    subprocess.run(
        [sys.executable, str(script_path), *common_args, "--output", str(output_path_first)],
        check=True,
        capture_output=True,
        text=True,
    )
    first_payload = json.loads(output_path_first.read_text(encoding="utf-8"))
    first_cache = first_payload.get("profitability_overview", {}).get("csv_parse_cache", {})
    assert first_cache.get("enabled") is True
    assert first_cache.get("plan_misses", 0) >= 1
    assert first_cache.get("intents_misses", 0) >= 1

    subprocess.run(
        [sys.executable, str(script_path), *common_args, "--output", str(output_path_second)],
        check=True,
        capture_output=True,
        text=True,
    )
    second_payload = json.loads(output_path_second.read_text(encoding="utf-8"))
    second_cache = second_payload.get("profitability_overview", {}).get("csv_parse_cache", {})
    assert second_cache.get("enabled") is True
    assert second_cache.get("plan_hits", 0) >= 1
    assert second_cache.get("intents_hits", 0) >= 1


def test_summarize_window_profitability_cache_falls_back_when_preferred_readonly(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir = out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "window.json"

    preferred_cache = checkpoints_dir / "profitability_csv_parse_cache.sqlite3"
    preferred_cache.write_text("seed", encoding="utf-8")
    preferred_cache.chmod(0o444)

    plans_path = out_dir / "kalshi_temperature_trade_plan_20260418_000000.csv"
    plans_fieldnames = [
        "source_strategy",
        "temperature_intent_id",
        "temperature_client_order_id",
        "client_order_id",
        "market_ticker",
        "temperature_underlying_key",
        "side",
        "maker_entry_price_dollars",
        "maker_entry_edge_conservative_net_total",
        "estimated_entry_cost_dollars",
    ]
    with plans_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=plans_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-fallback-1",
                "temperature_client_order_id": "temp-fallback-1",
                "client_order_id": "temp-fallback-1",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "temperature_underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "yes",
                "maker_entry_price_dollars": "0.40",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "10",
            }
        )

    intents_path = out_dir / "kalshi_temperature_trade_intents_20260418_000000.csv"
    intents_fieldnames = [
        "intent_id",
        "policy_approved",
        "policy_reason",
        "settlement_station",
        "policy_metar_local_hour",
        "constraint_status",
        "market_ticker",
        "underlying_key",
        "side",
        "max_entry_price_dollars",
        "metar_observation_age_minutes",
        "policy_metar_max_age_minutes_applied",
        "policy_expected_edge_net",
        "policy_min_expected_edge_net_required",
    ]
    with intents_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=intents_fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "intent_id": "intent-fallback-1",
                "policy_approved": "true",
                "policy_reason": "approved",
                "settlement_station": "KNYC",
                "policy_metar_local_hour": "12",
                "constraint_status": "yes_impossible",
                "market_ticker": "KXHIGHNY-26APR18-B70.5",
                "underlying_key": "KXHIGHNY|KNYC|2026-04-18",
                "side": "yes",
                "max_entry_price_dollars": "0.40",
                "metar_observation_age_minutes": "10",
                "policy_metar_max_age_minutes_applied": "22.5",
                "policy_expected_edge_net": "0.08",
                "policy_min_expected_edge_net_required": "0.02",
            }
        )

    script_path = Path(__file__).resolve().parents[1] / "infra" / "digitalocean" / "summarize_window.py"
    subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--out-dir",
            str(out_dir),
            "--start-epoch",
            "0",
            "--end-epoch",
            str(time.time() + 30),
            "--label",
            "test",
            "--output",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    csv_cache = payload.get("profitability_overview", {}).get("csv_parse_cache", {})
    assert csv_cache.get("preferred_file") == str(preferred_cache)
    assert csv_cache.get("path_fallback_reason") == "preferred_not_writable"
    assert csv_cache.get("file") != str(preferred_cache)
    assert csv_cache.get("puts_ok", 0) >= 2
