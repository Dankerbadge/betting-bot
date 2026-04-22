from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread


def test_alpha_summary_script_emits_concise_health_and_checkin_lines(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    latest = out_dir / "health" / "alpha_summary_latest.json"
    assert latest.exists()
    payload = json.loads(latest.read_text(encoding="utf-8"))

    message_summary = payload.get("message_summary", {})
    assert message_summary.get("mode") == "concise"
    assert message_summary.get("mode_selected") == "concise"
    discord_obj = payload.get("discord", {})
    assert isinstance(discord_obj, dict)
    assert discord_obj.get("mode") == "concise"
    assert isinstance(discord_obj.get("message_concise"), str)
    assert discord_obj.get("message_concise") == payload.get("discord_message_concise")
    assert discord_obj.get("message") == payload.get("discord_message")

    concise = message_summary.get("concise", "")
    assert isinstance(concise, str)
    assert "Status now:" in concise
    assert "live status file missing" not in concise.lower()
    assert (
        "Execution mode: shadow-only simulation (counterfactual basis; live fills not included)."
        in concise
    )
    assert (
        "Persistent trial (since reset):" in concise
        or "Scenario balance (stress replay):" in concise
    )
    assert (
        "Check-in PnL (counterfactual):" in concise
        or "Scenario check-in PnL (stress replay):" in concise
    )
    assert "Settled confidence: pending" in concise
    assert "Confidence:" in concise
    assert "12h blockers:" in concise
    assert "top3 " in concise
    assert "Top 3 optimization moves:" in concise
    assert "\n1. " in concise
    detailed = message_summary.get("detailed", "")
    assert isinstance(detailed, str)
    assert "Projected bankroll PnL model ($1,000, if deployed):" not in detailed
    if "Projected bankroll PnL model ($1,000" in detailed:
        assert "Projected bankroll PnL model ($1,000, deployment model):" in detailed
    if "Persistent trial balance (stress replay, cash-constrained rows):" in detailed:
        assert "Check-in (counterfactual PnL):" in detailed
        assert "mode stress replay" in detailed

    headline = payload.get("headline_metrics", {})
    assert headline.get("selection_confidence_gate_coverage_basis") == "no_intents"
    assert "live_status_missing" not in ((payload.get("health", {}) or {}).get("issues") or [])
    assert isinstance(headline.get("selection_confidence_gate_coverage_ratio"), (int, float))
    assert isinstance(headline.get("deploy_confidence_score"), (int, float))
    assert isinstance(headline.get("deploy_confidence_score_uncapped"), (int, float))
    assert headline.get("deployment_confidence_cap_applied") is True
    cap_value = headline.get("deployment_confidence_cap_value")
    assert isinstance(cap_value, (int, float))
    assert 20.0 <= float(cap_value) <= 45.0
    assert headline.get("settled_evidence_confidence_score") is None
    approval_auto_apply = payload.get("approval_auto_apply", {}) or {}
    assert isinstance(approval_auto_apply.get("stability_enabled"), bool)
    assert isinstance(approval_auto_apply.get("stability_windows_required"), int)
    assert isinstance(approval_auto_apply.get("stability_streak"), int)
    assert isinstance(approval_auto_apply.get("stability_ready"), bool)
    assert isinstance(approval_auto_apply.get("stability_reason"), str)


def test_alpha_summary_script_stale_blocker_maps_to_risk_and_action(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc) - timedelta(minutes=5)
    token = now_utc.strftime("%Y%m%d_%H%M%S")
    intents_summary_path = out_dir / f"kalshi_temperature_trade_intents_summary_{token}.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "captured_at": now_utc.isoformat(),
                "intents_total": 120,
                "intents_approved": 0,
                "planned_orders": 0,
                "policy_reason_counts": {"metar_observation_stale": 120},
            }
        ),
        encoding="utf-8",
    )
    now_epoch = now_utc.timestamp()
    os.utime(intents_summary_path, (now_epoch, now_epoch))

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    concise = (payload.get("message_summary", {}) or {}).get("concise", "")
    assert "Primary risk: weather freshness blocker remains dominant" in concise
    assert "Best next action:" in concise
    assert "stale blocks with station/hour freshness tuning." in concise
    best_action_line = next(
        (line for line in concise.splitlines() if line.startswith("Best next action:")),
        "",
    )
    assert "freshness" in best_action_line.lower()
    first_suggestion_line = next(
        (line for line in concise.splitlines() if line.startswith("1. ")),
        "",
    )
    if best_action_line and first_suggestion_line:
        assert (
            first_suggestion_line.removeprefix("1. ").strip().lower()
            != best_action_line.removeprefix("Best next action:").strip().lower()
        )


def test_alpha_summary_overrides_stale_limiting_factor_without_settled_breadth(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    checkpoints_dir = out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc) - timedelta(minutes=3)
    token = now_utc.strftime("%Y%m%d_%H%M%S")
    window_path = checkpoints_dir / f"station_tuning_window_14h_{token}.json"
    window_path.write_text(
        json.dumps(
            {
                "captured_at": now_utc.isoformat(),
                "window_hours": 14,
                "totals": {
                    "intents_total": 605,
                    "intents_approved": 100,
                    "planned_orders_total": 25,
                },
                "rates": {
                    "approval_rate": 0.165289,
                    "stale_block_rate": 0.008264,
                },
                "policy_reason_counts": {
                    "approved": 100,
                    "no_side_interval_overlap_still_possible": 500,
                    "metar_observation_stale": 5,
                },
            }
        ),
        encoding="utf-8",
    )
    now_epoch = now_utc.timestamp()
    os.utime(window_path, (now_epoch, now_epoch))

    bankroll_path = out_dir / f"kalshi_temperature_bankroll_validation_{token}.json"
    bankroll_path.write_text(
        json.dumps(
            {
                "captured_at": now_utc.isoformat(),
                "opportunity_breadth": {
                    "resolved_unique_market_sides": 12,
                    "unresolved_unique_market_sides": 4,
                },
                "viability_summary": {
                    "main_limiting_factor": "stale_suppression",
                    "would_plausibly_beat_hysa_after_slippage_fees": False,
                    "what_return_would_have_been_produced_on_bankroll": 0.01,
                    "what_pct_of_bankroll_would_have_been_utilized_avg": 0.12,
                },
                "hit_rate_quality": {
                    "unique_market_side": {
                        "wins": 7,
                        "losses": 5,
                        "pushes": 0,
                        "win_rate": 0.583333,
                    }
                },
                "expected_vs_shadow_settled": {
                    "by_aggregation_layer": {
                        "unique_market_side": {
                            "trade_count": 12,
                            "shadow_settled_pnl": 2.5,
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    os.utime(bankroll_path, (now_epoch, now_epoch))

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    headline = payload.get("headline_metrics", {}) or {}
    assert headline.get("display_limiting_factor") == "insufficient_settled_breadth"
    assert headline.get("display_limiting_factor") != "stale_suppression"
    concise = (payload.get("message_summary", {}) or {}).get("concise", "")
    assert "Limiting factor: insufficient settled breadth" in concise


def test_alpha_summary_script_falls_back_to_checkpoint_live_status(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    checkpoints_dir = out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    (checkpoints_dir / "live_status_latest.json").write_text(
        json.dumps(
            {
                "status": "GREEN",
                "yellow_reasons": [],
                "red_reasons": [],
            }
        ),
        encoding="utf-8",
    )

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    concise = (payload.get("message_summary", {}) or {}).get("concise", "")
    assert "Health note: live status" not in concise
    assert (payload.get("headline_metrics", {}) or {}).get("health_status") in {"GREEN", "YELLOW"}
    assert "live_status_missing" not in ((payload.get("health", {}) or {}).get("issues") or [])
    source_live_status_file = _normalize_path((payload.get("source_files", {}) or {}).get("live_status_file"))
    assert source_live_status_file.endswith("/checkpoints/live_status_latest.json")
    source_files = payload.get("source_files", {}) or {}
    for key in ("metar_summary_file", "settlement_summary_file", "shadow_summary_file"):
        value = _normalize_path(source_files.get(key))
        if value:
            assert Path(value).is_file()
    assert _normalize_path(source_files.get("metar_summary_file_resolution")) in {
        "live_status_latest_artifacts",
        "output_dir_glob_fallback",
        "live_status_path_missing",
        "missing",
    }


def test_alpha_summary_script_concise_mode_honors_line_cap(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
                "ALPHA_SUMMARY_CONCISE_MAX_LINES=12",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    concise = (payload.get("message_summary", {}) or {}).get("concise", "")
    lines = [line for line in concise.splitlines() if line.strip()]
    assert 1 <= len(lines) <= 12
    assert lines[0].startswith("BetBot Alpha Summary (")
    assert any(line.startswith("Best next action:") for line in lines)
    assert any(line.startswith("Top 3 optimization moves:") for line in lines)
    assert any(line.startswith("1. ") for line in lines)
    message_summary = payload.get("message_summary", {})
    assert message_summary.get("msg_quality_pass") is True
    assert int(message_summary.get("msg_quality_fail_count") or 0) == 0


def test_alpha_summary_prioritizes_global_only_selection_drift(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    # Seed prior summary so current run can compute drift vs previous.
    (health_dir / "alpha_summary_latest.json").write_text(
        json.dumps(
            {
                "headline_metrics": {
                    "selection_quality_rows_adjusted": 1000,
                    "selection_quality_rows_adjusted_global_only": 50,
                    "selection_quality_global_only_adjusted_share": 0.05,
                }
            }
        ),
        encoding="utf-8",
    )

    # Force a synthetic high global-only pressure file to be selected
    # over newly-generated default files in this test run.
    synthetic_selection_quality = out_dir / "kalshi_temperature_selection_quality_20990101_000000.json"
    synthetic_selection_quality.write_text(
        json.dumps(
            {
                "status": "ready",
                "captured_at": "2099-01-01T00:00:00+00:00",
                "intent_window": {
                    "rows_total": 2400,
                    "rows_adjusted": 800,
                    "rows_adjusted_bucket_backed": 560,
                    "rows_adjusted_global_only": 240,
                    "adjusted_rate": 0.333333,
                    "adjusted_bucket_backed_rate": 0.70,
                },
            }
        ),
        encoding="utf-8",
    )
    future_epoch = (datetime.now(timezone.utc) + timedelta(hours=12)).timestamp()
    os.utime(synthetic_selection_quality, (future_epoch, future_epoch))

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((health_dir / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    headline = payload.get("headline_metrics", {}) or {}
    assert headline.get("selection_quality_global_only_pressure_active") is True
    assert headline.get("selection_quality_global_only_drift_rising") is True
    assert headline.get("selection_quality_global_only_alert_active") is True
    assert headline.get("selection_quality_global_only_alert_level") in {"yellow", "red"}
    health_issues = ((payload.get("health", {}) or {}).get("issues") or [])
    assert "selection_quality_global_only_drift" in health_issues

    top_suggestions = payload.get("suggestions_structured", []) or []
    global_only_rows = [
        row for row in top_suggestions if isinstance(row, dict) and row.get("key") == "selection_quality_global_only_drift"
    ]
    assert global_only_rows, "expected selection_quality_global_only_drift in top suggestions"
    assert int(global_only_rows[0].get("rank") or 99) <= 2


def test_alpha_summary_surfaces_edge_floor_bucket_tuning_when_edge_blocker_dominates(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc) - timedelta(minutes=2)
    token = now_utc.strftime("%Y%m%d_%H%M%S")
    intents_summary_path = out_dir / f"kalshi_temperature_trade_intents_summary_{token}.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "captured_at": now_utc.isoformat(),
                "intents_total": 240,
                "intents_approved": 40,
                "planned_orders": 0,
                "policy_reason_counts": {
                    "approved": 40,
                    "expected_edge_below_min": 200,
                },
            }
        ),
        encoding="utf-8",
    )
    now_epoch = now_utc.timestamp()
    os.utime(intents_summary_path, (now_epoch, now_epoch))

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    ranked_rows = payload.get("suggestions_structured_ranked_all", []) or []
    edge_rows = [
        row for row in ranked_rows if isinstance(row, dict) and row.get("key") == "edge_floor_bucket_tuning"
    ]
    assert edge_rows, "expected edge_floor_bucket_tuning suggestion when edge blocker dominates"
    edge_row = edge_rows[0]
    assert float(edge_row.get("impact_points") or 0.0) > 0.0
    assert _normalize_path(edge_row.get("metric_key")) == "edge_gate_blocked_share_of_blocked"
    assert edge_row.get("metric_direction") == "down"
    headline = payload.get("headline_metrics", {}) or {}
    assert headline.get("best_next_action_key") == "edge_floor_bucket_tuning"
    assert headline.get("best_next_action_quantified") is True
    tracking = payload.get("suggestion_tracking_summary", {}) or {}
    assert tracking.get("best_next_action_key") == "edge_floor_bucket_tuning"


def test_alpha_summary_concise_includes_quality_risk_alert_when_active(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc) - timedelta(minutes=2)
    token = now_utc.strftime("%Y%m%d_%H%M%S")
    intents_summary_path = out_dir / f"kalshi_temperature_trade_intents_summary_{token}.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "captured_at": now_utc.isoformat(),
                "intents_total": 200,
                "intents_approved": 150,
                "planned_orders": 0,
                "policy_reason_counts": {
                    "approved": 150,
                    "expected_edge_below_min": 40,
                    "metar_observation_stale": 10,
                },
            }
        ),
        encoding="utf-8",
    )
    now_epoch = now_utc.timestamp()
    os.utime(intents_summary_path, (now_epoch, now_epoch))

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
                "ALPHA_SUMMARY_CONCISE_MAX_LINES=10",
                "ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_ABS_INTENTS=10",
                "ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_RATIO_TO_WINDOW=0.05",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    concise = (payload.get("message_summary", {}) or {}).get("concise", "")
    assert "Quality-risk alert:" in concise
    message_quality = payload.get("message_quality_summary", {}) or {}
    assert message_quality.get("overall_pass") is True
    assert int(message_quality.get("failed_check_count") or 0) == 0


def test_alpha_summary_concise_blockers_line_uses_short_labels_without_ellipsis(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc) - timedelta(minutes=2)
    token = now_utc.strftime("%Y%m%d_%H%M%S")
    intents_summary_path = out_dir / f"kalshi_temperature_trade_intents_summary_{token}.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "captured_at": now_utc.isoformat(),
                "intents_total": 1200,
                "intents_approved": 150,
                "planned_orders": 40,
                "policy_reason_counts": {
                    "approved": 150,
                    "expected_edge_below_min": 700,
                    "settlement_confidence_below_min": 250,
                    "probability_confidence_below_min": 80,
                    "metar_observation_stale": 20,
                },
            }
        ),
        encoding="utf-8",
    )
    now_epoch = now_utc.timestamp()
    os.utime(intents_summary_path, (now_epoch, now_epoch))

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    concise = (payload.get("message_summary", {}) or {}).get("concise", "")
    blockers_line = next((line for line in concise.splitlines() if line.startswith("12h blockers:")), "")
    assert blockers_line, "expected concise blockers line"
    assert "..." not in blockers_line
    assert "settlement low" in blockers_line
    assert "edge low" in blockers_line


def test_alpha_summary_boosts_priority_for_measured_12h_regression(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    health_dir = out_dir / "health"
    out_dir.mkdir(parents=True, exist_ok=True)
    health_dir.mkdir(parents=True, exist_ok=True)

    (health_dir / "alpha_summary_latest.json").write_text(
        json.dumps(
            {
                "suggestions_structured_ranked_all": [
                    {
                        "key": "stale_freshness",
                        "gap_to_target": 0.10,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    now_utc = datetime.now(timezone.utc) - timedelta(minutes=2)
    token = now_utc.strftime("%Y%m%d_%H%M%S")
    intents_summary_path = out_dir / f"kalshi_temperature_trade_intents_summary_{token}.json"
    intents_summary_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "captured_at": now_utc.isoformat(),
                "intents_total": 220,
                "intents_approved": 20,
                "planned_orders": 0,
                "policy_reason_counts": {
                    "approved": 20,
                    "metar_observation_stale": 200,
                },
            }
        ),
        encoding="utf-8",
    )
    now_epoch = now_utc.timestamp()
    os.utime(intents_summary_path, (now_epoch, now_epoch))

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    ranked_rows = payload.get("suggestions_structured_ranked_all", []) or []
    stale_rows = [row for row in ranked_rows if isinstance(row, dict) and row.get("key") == "stale_freshness"]
    assert stale_rows, "expected stale_freshness suggestion in stale-dominant case"
    stale_row = stale_rows[0]
    assert stale_row.get("measurable_12h_delta") is True
    assert _normalize_path(stale_row.get("tracking_trend")) == "regressing"
    assert float(stale_row.get("measured_delta_priority_points") or 0.0) > 0.0
    assert float(stale_row.get("priority_score_effective") or 0.0) > float(stale_row.get("priority_score") or 0.0)

    tracking = payload.get("suggestion_tracking_summary", {}) or {}
    assert int(tracking.get("measured_delta_count") or 0) >= 1
    assert int(tracking.get("measured_delta_regressing_count") or 0) >= 1


def test_alpha_summary_ops_webhook_includes_decision_matrix_lane_line(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    (health_dir / "coldmath_hardening_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "targeted_trading_support": {
                    "checks": {
                        "decision_matrix_strict_signal": False,
                        "decision_matrix_bootstrap_signal_raw": True,
                        "decision_matrix_bootstrap_signal": True,
                    },
                    "observed": {
                        "decision_matrix_bootstrap_guard_status": "active",
                        "decision_matrix_bootstrap_guard_reasons": [],
                        "decision_matrix_bootstrap_guard_elapsed_hours": 10.0,
                    },
                    "thresholds": {
                        "matrix_bootstrap_max_hours": 336.0,
                    },
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (health_dir / ".decision_matrix_lane_alert_state.json").write_text(
        json.dumps(
            {
                "last_lane_status": "bootstrap_blocked",
                "degraded_statuses": ["matrix_failed", "bootstrap_blocked"],
                "degraded_streak_count": 4,
                "degraded_streak_threshold": 3,
                "degraded_streak_notify_every": 3,
                "last_notify_reason": "degraded_streak",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    captured_payloads: list[dict[str, object]] = []

    class _CaptureHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = {"raw": body}
            if isinstance(parsed, dict):
                captured_payloads.append(parsed)
            else:
                captured_payloads.append({"raw": body})
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = HTTPServer(("127.0.0.1", 0), _CaptureHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env_file = tmp_path / "alpha_summary.env"
        env_file.write_text(
            "\n".join(
                [
                    f'BETBOT_ROOT="{root}"',
                    f'OUTPUT_DIR="{out_dir}"',
                    "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                    "ALPHA_SUMMARY_SEND_ALPHA_WEBHOOK=0",
                    "ALPHA_SUMMARY_SEND_OPS_WEBHOOK=1",
                    f"ALPHA_SUMMARY_WEBHOOK_OPS_URL=http://127.0.0.1:{server.server_port}/ops",
                    "ALPHA_SUMMARY_DISCORD_MODE=concise",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
        subprocess.run(
            ["/bin/bash", str(script), str(env_file)],
            check=True,
            capture_output=True,
            text=True,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert captured_payloads, "expected ops webhook payload"
    message_text = str(captured_payloads[-1].get("text") or "")
    assert "Decision matrix lane: bootstrap pass" in message_text
    assert "Decision matrix degraded streak: 4 run(s)" in message_text
    assert "[streak alert fired]" in message_text


def test_alpha_summary_flags_critical_parse_error_and_caps_confidence(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    token = now_utc.strftime("%Y%m%d_%H%M%S")
    malformed_intents_summary = out_dir / f"kalshi_temperature_trade_intents_summary_{token}.json"
    malformed_intents_summary.write_text("{\n", encoding="utf-8")
    future_epoch = (now_utc + timedelta(hours=4)).timestamp()
    os.utime(malformed_intents_summary, (future_epoch, future_epoch))

    env_file = tmp_path / "alpha_summary.env"
    env_file.write_text(
        "\n".join(
            [
                f'BETBOT_ROOT="{root}"',
                f'OUTPUT_DIR="{out_dir}"',
                "ALPHA_SUMMARY_SEND_WEBHOOK=0",
                "ALPHA_SUMMARY_DISCORD_MODE=concise",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    script = root / "infra" / "digitalocean" / "run_temperature_alpha_summary.sh"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads((out_dir / "health" / "alpha_summary_latest.json").read_text(encoding="utf-8"))
    health = payload.get("health", {}) or {}
    issues = health.get("issues") or []
    assert "artifact_parse_error" in issues
    assert "critical_artifact_parse_error" in issues

    source_files = payload.get("source_files", {}) or {}
    assert source_files.get("latest_intents_summary_load_status") == "parse_error"
    critical_keys = source_files.get("critical_artifact_parse_error_keys") or []
    assert "latest_intents_summary" in critical_keys

    headline = payload.get("headline_metrics", {}) or {}
    assert headline.get("deployment_confidence_cap_applied") is True
    cap_reason_tokens = str(headline.get("deployment_confidence_cap_reason") or "").split("+")
    assert "critical_artifact_parse_error" in cap_reason_tokens
    cap_value = headline.get("deployment_confidence_cap_value")
    assert isinstance(cap_value, (int, float))
    assert float(cap_value) <= 35.0


def _normalize_path(value: object) -> str:
    return str(value or "").replace("\\\\", "/")
