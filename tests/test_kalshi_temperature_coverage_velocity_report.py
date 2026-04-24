from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from betbot.kalshi_temperature_coverage_velocity_report import (
    run_kalshi_temperature_coverage_velocity_report,
    summarize_kalshi_temperature_coverage_velocity_report,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_coverage_velocity_state(
    out_dir: Path,
    *,
    captured_at: datetime,
    filename: str = "decision_matrix_coverage_velocity_state_latest.json",
    guardrail_active: bool,
    guardrail_cleared: bool,
    positive_streak: int,
    non_positive_streak: int,
    required_positive_streak: int,
    selected_growth_delta_24h: int | None,
    selected_growth_delta_7d: int | None,
    selected_combined_bucket_count_delta_24h: int | None,
    selected_combined_bucket_count_delta_7d: int | None,
    selected_growth_source: str = "settled_outcome_throughput",
) -> Path:
    path = out_dir / "health" / filename
    payload: dict[str, object] = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "source": str(path),
        "evidence_available": True,
        "selected_growth_source": selected_growth_source,
        "selected_growth_source_file": str(out_dir / "health" / "kalshi_temperature_settled_outcome_throughput_latest.json"),
        "selected_growth_delta_24h": selected_growth_delta_24h,
        "selected_growth_delta_7d": selected_growth_delta_7d,
        "selected_combined_bucket_count_delta_24h": selected_combined_bucket_count_delta_24h,
        "selected_combined_bucket_count_delta_7d": selected_combined_bucket_count_delta_7d,
        "positive_streak": positive_streak,
        "non_positive_streak": non_positive_streak,
        "required_positive_streak": required_positive_streak,
        "guardrail_active": guardrail_active,
        "guardrail_cleared": guardrail_cleared,
        "last_evidence_direction": "positive" if positive_streak > 0 else "non_positive",
    }
    _write_json(path, payload)
    return path


def _write_hardening_artifact(
    out_dir: Path,
    *,
    captured_at: datetime,
    filename: str,
    selected_growth_delta_24h: int | None,
    selected_growth_delta_7d: int | None,
    selected_combined_bucket_count_delta_24h: int | None,
    selected_combined_bucket_count_delta_7d: int | None,
    positive_streak: int,
    non_positive_streak: int,
    required_positive_streak: int,
    guardrail_active: bool,
    guardrail_cleared: bool,
) -> Path:
    path = out_dir / "health" / filename
    _write_json(
        path,
        {
            "status": "ready",
            "captured_at": captured_at.isoformat(),
            "observed_metrics": {
                "coverage_velocity_selected_growth_delta_24h": selected_growth_delta_24h,
                "coverage_velocity_selected_growth_delta_7d": selected_growth_delta_7d,
                "coverage_velocity_selected_combined_bucket_count_delta_24h": selected_combined_bucket_count_delta_24h,
                "coverage_velocity_selected_combined_bucket_count_delta_7d": selected_combined_bucket_count_delta_7d,
                "coverage_velocity_positive_streak": positive_streak,
                "coverage_velocity_non_positive_streak": non_positive_streak,
                "coverage_velocity_required_positive_streak": required_positive_streak,
                "coverage_velocity_guardrail_active": guardrail_active,
                "coverage_velocity_guardrail_cleared": guardrail_cleared,
                "settled_outcome_growth_source": "settled_outcome_throughput",
                "settled_outcome_growth_source_file": str(out_dir / "health" / "kalshi_temperature_settled_outcome_throughput_latest.json"),
            },
        },
    )
    return path


def test_coverage_velocity_report_active_estimates_runs_and_hours_to_clear(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    _write_coverage_velocity_state(
        out_dir,
        captured_at=now,
        guardrail_active=True,
        guardrail_cleared=False,
        positive_streak=1,
        non_positive_streak=0,
        required_positive_streak=3,
        selected_growth_delta_24h=4,
        selected_growth_delta_7d=9,
        selected_combined_bucket_count_delta_24h=2,
        selected_combined_bucket_count_delta_7d=5,
    )
    for idx, hours_back in enumerate((24, 18, 12, 6), start=1):
        captured_at = now - timedelta(hours=hours_back)
        _write_hardening_artifact(
            out_dir,
            captured_at=captured_at,
            filename=f"decision_matrix_hardening_{captured_at.strftime('%Y%m%d_%H%M%S')}.json",
            selected_growth_delta_24h=idx,
            selected_growth_delta_7d=idx + 1,
            selected_combined_bucket_count_delta_24h=idx - 1,
            selected_combined_bucket_count_delta_7d=idx,
            positive_streak=idx,
            non_positive_streak=0,
            required_positive_streak=3,
            guardrail_active=idx < 3,
            guardrail_cleared=idx >= 3,
        )

    summary = run_kalshi_temperature_coverage_velocity_report(output_dir=str(out_dir), history_limit=24, now=now)

    assert summary["status"] == "ready"
    assert summary["coverage_velocity_state"]["guardrail_active"] is True
    assert summary["coverage_velocity_state"]["positive_streak"] == 1
    assert summary["coverage_velocity_state"]["required_positive_streak"] == 3
    assert summary["trend"]["evidence_run_count"] == 4
    assert summary["trend"]["positive_run_count"] == 4
    assert summary["trend"]["non_positive_run_count"] == 0
    assert summary["trend"]["estimated_runs_to_clear"] == 2
    assert summary["trend"]["estimated_hours_to_clear"] == 12.0
    assert summary["trend"]["median_run_cadence_hours"] == 6.0
    assert summary["report_summary"].startswith("guardrail=active streak=1/3 evidence=4")
    assert Path(summary["output_file"]).exists()
    assert Path(summary["latest_file"]).exists()


def test_coverage_velocity_report_cleared_has_zero_estimate(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    _write_coverage_velocity_state(
        out_dir,
        captured_at=now,
        guardrail_active=False,
        guardrail_cleared=True,
        positive_streak=3,
        non_positive_streak=0,
        required_positive_streak=3,
        selected_growth_delta_24h=1,
        selected_growth_delta_7d=2,
        selected_combined_bucket_count_delta_24h=1,
        selected_combined_bucket_count_delta_7d=1,
    )
    for idx, hours_back in enumerate((12, 6), start=1):
        captured_at = now - timedelta(hours=hours_back)
        _write_hardening_artifact(
            out_dir,
            captured_at=captured_at,
            filename=f"decision_matrix_hardening_{captured_at.strftime('%Y%m%d_%H%M%S')}.json",
            selected_growth_delta_24h=idx,
            selected_growth_delta_7d=idx,
            selected_combined_bucket_count_delta_24h=idx,
            selected_combined_bucket_count_delta_7d=idx,
            positive_streak=idx,
            non_positive_streak=0,
            required_positive_streak=3,
            guardrail_active=False,
            guardrail_cleared=True,
        )

    summary = run_kalshi_temperature_coverage_velocity_report(output_dir=str(out_dir), history_limit=24, now=now)

    assert summary["coverage_velocity_state"]["guardrail_cleared"] is True
    assert summary["trend"]["estimated_runs_to_clear"] == 0
    assert summary["trend"]["estimated_hours_to_clear"] == 0.0
    assert summary["trend"]["positive_run_count"] == 2
    assert summary["trend"]["non_positive_run_count"] == 0
    assert "guardrail=cleared streak=3/3" in summary["report_summary"]


def test_coverage_velocity_report_writes_artifacts(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    _write_coverage_velocity_state(
        out_dir,
        captured_at=now,
        guardrail_active=True,
        guardrail_cleared=False,
        positive_streak=1,
        non_positive_streak=1,
        required_positive_streak=2,
        selected_growth_delta_24h=-1,
        selected_growth_delta_7d=0,
        selected_combined_bucket_count_delta_24h=-1,
        selected_combined_bucket_count_delta_7d=0,
    )
    _write_hardening_artifact(
        out_dir,
        captured_at=now - timedelta(hours=6),
        filename="decision_matrix_hardening_20260423_060000.json",
        selected_growth_delta_24h=-1,
        selected_growth_delta_7d=0,
        selected_combined_bucket_count_delta_24h=-1,
        selected_combined_bucket_count_delta_7d=0,
        positive_streak=0,
        non_positive_streak=1,
        required_positive_streak=2,
        guardrail_active=True,
        guardrail_cleared=False,
    )

    summary_text = summarize_kalshi_temperature_coverage_velocity_report(
        output_dir=str(out_dir),
        history_limit=24,
        now=now,
    )
    payload = json.loads(summary_text)

    assert Path(payload["output_file"]).exists()
    assert Path(payload["latest_file"]).exists()
    latest_payload = json.loads(Path(payload["latest_file"]).read_text(encoding="utf-8"))
    assert latest_payload["report_summary"] == payload["report_summary"]
    assert latest_payload["trend"]["estimated_runs_to_clear"] == 1
