from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any

from betbot.config import load_config
from betbot.io import load_candidates
from betbot.live_candidates import run_live_candidates
from betbot.live_enrich import run_live_candidate_enrichment
from betbot.live_smoke import HttpGetter, _http_get_json
from betbot.paper import run_paper


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None
    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return None


def _load_latest_cached_candidate_summary(
    *,
    output_dir: str,
    sport_id: int,
    event_date: str,
) -> dict[str, Any] | None:
    out_dir = Path(output_dir)
    if not out_dir.exists():
        return None

    pattern = f"live_candidates_summary_{sport_id}_{event_date}_*.json"
    summary_paths = sorted(out_dir.glob(pattern), reverse=True)
    now = datetime.now().astimezone()
    for summary_path in summary_paths:
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(payload, dict):
            continue
        source_status = str(payload.get("status") or "").strip().lower()
        if source_status not in {"ready", "empty"}:
            continue
        output_csv = payload.get("output_csv")
        if not isinstance(output_csv, str) or output_csv.strip() == "":
            continue
        csv_path = Path(output_csv)
        if not csv_path.exists():
            continue
        captured_at = _parse_iso_datetime(payload.get("captured_at"))
        cache_age_seconds: float | None = None
        if captured_at is not None:
            if captured_at.tzinfo is None:
                captured_at = captured_at.replace(tzinfo=now.tzinfo)
            cache_age_seconds = max(0.0, round((now - captured_at).total_seconds(), 3))
        return {
            "summary": payload,
            "summary_file": str(summary_path),
            "output_csv": str(csv_path),
            "source_status": source_status,
            "cache_age_seconds": cache_age_seconds,
        }
    return None


def _write_live_paper_summary(output_dir: str, summary: dict[str, Any]) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    event_date = str(summary.get("event_date") or "unknown-date")
    safe_event_date = event_date.replace("/", "-")
    sport_id = str(summary.get("sport_id") or "unknown-sport")
    output_path = out_dir / f"live_paper_summary_{sport_id}_{safe_event_date}_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary


def run_live_paper(
    *,
    env_file: str,
    sport_id: int,
    event_date: str,
    starting_bankroll: float,
    config_path: str | None = None,
    output_dir: str = "outputs",
    affiliate_ids: tuple[str, ...] = ("19", "22", "23"),
    market_ids: tuple[int, ...] = (1, 2, 3),
    min_books: int = 2,
    offset_minutes: int = 300,
    include_in_play: bool = False,
    enrich_candidates: bool = False,
    enrichment_csv: str | None = None,
    enrichment_freshness_hours: float = 12.0,
    enrichment_max_logit_shift: float = 0.35,
    enrichment_moneyline_only: bool = True,
    timeout_seconds: float = 15.0,
    http_get_json: HttpGetter = _http_get_json,
) -> dict[str, Any]:
    used_cached_candidates = False
    candidate_pull_error: str | None = None
    try:
        candidate_summary = run_live_candidates(
            env_file=env_file,
            sport_id=sport_id,
            event_date=event_date,
            output_dir=output_dir,
            affiliate_ids=affiliate_ids,
            market_ids=market_ids,
            min_books=min_books,
            offset_minutes=offset_minutes,
            include_in_play=include_in_play,
            timeout_seconds=timeout_seconds,
            http_get_json=http_get_json,
        )
    except ValueError as exc:
        candidate_pull_error = str(exc)
        cached = _load_latest_cached_candidate_summary(
            output_dir=output_dir,
            sport_id=sport_id,
            event_date=event_date,
        )
        if cached is not None:
            used_cached_candidates = True
            cached_summary = dict(cached["summary"])
            cached_summary.update(
                {
                    "status": "cached_fallback",
                    "source_status": cached["source_status"],
                    "output_file": cached["summary_file"],
                    "output_csv": cached["output_csv"],
                    "cache_age_seconds": cached["cache_age_seconds"],
                    "fallback_error": candidate_pull_error,
                }
            )
            candidate_summary = cached_summary
        else:
            return _write_live_paper_summary(output_dir, {
                "status": "error",
                "env_file": env_file,
                "sport_id": sport_id,
                "event_date": event_date,
                "candidate_pull": None,
                "candidate_enrichment": None,
                "candidate_csv_used": None,
                "paper_run": None,
                "config_path": config_path,
                "starting_bankroll": starting_bankroll,
                "error": candidate_pull_error,
                "used_cached_candidates": False,
            })

    if used_cached_candidates:
        source_status = str(candidate_summary.get("source_status") or "").strip().lower()
        if source_status != "ready":
            return _write_live_paper_summary(output_dir, {
                "status": "stale_empty",
                "env_file": env_file,
                "sport_id": sport_id,
                "event_date": event_date,
                "candidate_pull": candidate_summary,
                "candidate_enrichment": None,
                "candidate_csv_used": candidate_summary.get("output_csv"),
                "paper_run": None,
                "config_path": config_path,
                "starting_bankroll": starting_bankroll,
                "error": candidate_pull_error,
                "used_cached_candidates": True,
            })
    elif candidate_summary["status"] != "ready":
        return _write_live_paper_summary(output_dir, {
            "status": candidate_summary["status"],
            "env_file": env_file,
            "sport_id": sport_id,
            "event_date": event_date,
            "candidate_pull": candidate_summary,
            "candidate_enrichment": None,
            "candidate_csv_used": candidate_summary.get("output_csv"),
            "paper_run": None,
            "config_path": config_path,
            "starting_bankroll": starting_bankroll,
            "used_cached_candidates": False,
        })

    candidate_enrichment = None
    candidate_csv_for_paper = str(candidate_summary["output_csv"])
    if enrich_candidates:
        candidate_enrichment = run_live_candidate_enrichment(
            candidate_csv=str(candidate_summary["output_csv"]),
            output_dir=output_dir,
            evidence_csv=enrichment_csv,
            freshness_hours=enrichment_freshness_hours,
            max_logit_shift=enrichment_max_logit_shift,
            moneyline_only=enrichment_moneyline_only,
            apply_to_decision_prob=True,
        )
        enrichment_output_csv = candidate_enrichment.get("output_csv")
        if isinstance(enrichment_output_csv, str) and enrichment_output_csv.strip():
            candidate_csv_for_paper = enrichment_output_csv

    cfg = load_config(config_path)
    candidates = load_candidates(candidate_csv_for_paper)
    paper_summary = run_paper(
        candidates=candidates,
        cfg=cfg,
        starting_bankroll=starting_bankroll,
        output_dir=output_dir,
        simulate_with_outcomes=False,
    )

    return _write_live_paper_summary(output_dir, {
        "status": "stale_ready" if used_cached_candidates else "ready",
        "env_file": env_file,
        "sport_id": sport_id,
        "event_date": event_date,
        "candidate_pull": candidate_summary,
        "candidate_enrichment": candidate_enrichment,
        "candidate_csv_used": candidate_csv_for_paper,
        "paper_run": paper_summary,
        "config_path": config_path,
        "starting_bankroll": starting_bankroll,
        "error": candidate_pull_error,
        "used_cached_candidates": used_cached_candidates,
    })
