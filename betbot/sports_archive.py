from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from betbot.live_paper import run_live_paper
from betbot.live_smoke import HttpGetter, _http_get_json


ARCHIVE_FIELDNAMES = [
    "recorded_at",
    "sport_id",
    "event_date",
    "status",
    "error",
    "candidate_status",
    "events_fetched",
    "candidates_written",
    "positive_ev_candidates",
    "accepted",
    "rejected",
    "avg_ev_accepted",
    "top_candidate_selection",
    "top_candidate_market",
    "top_candidate_book",
    "top_candidate_estimated_ev",
    "top_candidate_edge_rank_score",
    "top_candidate_consensus_book_count",
    "top_candidate_consensus_stability",
    "top_candidate_consensus_prob_range",
    "live_paper_summary_file",
    "candidate_summary_file",
    "paper_summary_file",
]


LivePaperRunner = Callable[..., dict[str, Any]]


def default_sports_archive_path(output_dir: str) -> Path:
    return Path(output_dir) / "live_paper_archive.csv"


def _normalize_archive_csv(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        return

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    if not rows:
        return

    header = rows[0]
    data_rows = rows[1:]
    if header == ARCHIVE_FIELDNAMES and all(len(row) == len(ARCHIVE_FIELDNAMES) for row in data_rows):
        return

    normalized_rows: list[dict[str, Any]] = []
    for row in data_rows:
        if not row:
            continue
        normalized = {key: "" for key in ARCHIVE_FIELDNAMES}
        if len(row) == len(ARCHIVE_FIELDNAMES):
            for index, value in enumerate(row):
                normalized[ARCHIVE_FIELDNAMES[index]] = value
        else:
            for index, name in enumerate(header):
                if index >= len(row) or name not in normalized:
                    continue
                normalized[name] = row[index]
        normalized_rows.append(normalized)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ARCHIVE_FIELDNAMES)
        writer.writeheader()
        for row in normalized_rows:
            writer.writerow(row)


def _append_archive_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _normalize_archive_csv(path)
    needs_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ARCHIVE_FIELDNAMES)
        if needs_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in ARCHIVE_FIELDNAMES})


def _count_archive_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", newline="", encoding="utf-8") as handle:
        return max(0, sum(1 for _ in csv.DictReader(handle)))


def _read_archive_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    _normalize_archive_csv(path)
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _to_int(value: Any) -> int | None:
    if value in {"", None}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value in {"", None}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _delta(current: Any, previous: Any) -> float | int | None:
    current_num = _to_float(current)
    previous_num = _to_float(previous)
    if current_num is None or previous_num is None:
        return None
    delta = round(current_num - previous_num, 6)
    if float(delta).is_integer():
        return int(delta)
    return delta


def _build_recent_history_summary(
    prior_rows: list[dict[str, str]],
    *,
    sport_id: int,
    lookback_rows: int = 10,
) -> dict[str, Any]:
    same_sport_rows = [row for row in prior_rows if str(row.get("sport_id") or "") == str(sport_id)]
    recent_rows = same_sport_rows[-lookback_rows:]
    ready_rows = [row for row in recent_rows if row.get("status") == "ready"]
    error_rows = [row for row in recent_rows if row.get("status") == "error"]
    empty_rows = [row for row in recent_rows if row.get("status") == "empty"]
    last_ready_row = ready_rows[-1] if ready_rows else None
    last_ready_edge = _to_float(last_ready_row.get("top_candidate_estimated_ev")) if last_ready_row else None
    last_ready_consensus_books = (
        _to_int(last_ready_row.get("top_candidate_consensus_book_count")) if last_ready_row else None
    )
    return {
        "lookback_rows": len(recent_rows),
        "ready_rows": len(ready_rows),
        "empty_rows": len(empty_rows),
        "error_rows": len(error_rows),
        "latest_recorded_at": recent_rows[-1].get("recorded_at") if recent_rows else None,
        "latest_ready_recorded_at": last_ready_row.get("recorded_at") if last_ready_row else None,
        "latest_ready_event_date": last_ready_row.get("event_date") if last_ready_row else None,
        "latest_ready_top_candidate_estimated_ev": last_ready_edge,
        "latest_ready_top_candidate_consensus_book_count": last_ready_consensus_books,
    }


def _preferred_top_candidate(candidate_pull: dict[str, Any]) -> dict[str, Any]:
    top_positive = candidate_pull.get("top_positive_ev_candidate")
    if isinstance(top_positive, dict) and top_positive:
        return top_positive
    top_positive_decision = candidate_pull.get("top_positive_decision_ev_candidate")
    if isinstance(top_positive_decision, dict) and top_positive_decision:
        return top_positive_decision
    top_candidates = candidate_pull.get("top_candidates")
    if isinstance(top_candidates, list):
        first = top_candidates[0] if top_candidates else {}
        if isinstance(first, dict):
            return first
    return {}


def run_sports_archive(
    *,
    env_file: str,
    sport_id: int,
    event_dates: tuple[str, ...],
    starting_bankroll: float,
    config_path: str | None = None,
    output_dir: str = "outputs",
    archive_csv: str | None = None,
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
    live_paper_runner: LivePaperRunner = run_live_paper,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    archive_path = Path(archive_csv) if archive_csv else default_sports_archive_path(output_dir)
    prior_rows = _read_archive_rows(archive_path)
    rows_to_append: list[dict[str, Any]] = []
    run_summaries: list[dict[str, Any]] = []

    for event_date in event_dates:
        try:
            summary = live_paper_runner(
                env_file=env_file,
                sport_id=sport_id,
                event_date=event_date,
                starting_bankroll=starting_bankroll,
                config_path=config_path,
                output_dir=output_dir,
                affiliate_ids=affiliate_ids,
                market_ids=market_ids,
                min_books=min_books,
                offset_minutes=offset_minutes,
                include_in_play=include_in_play,
                enrich_candidates=enrich_candidates,
                enrichment_csv=enrichment_csv,
                enrichment_freshness_hours=enrichment_freshness_hours,
                enrichment_max_logit_shift=enrichment_max_logit_shift,
                enrichment_moneyline_only=enrichment_moneyline_only,
                timeout_seconds=timeout_seconds,
                http_get_json=http_get_json,
            )
            candidate_pull = summary.get("candidate_pull") if isinstance(summary.get("candidate_pull"), dict) else {}
            paper_run = summary.get("paper_run") if isinstance(summary.get("paper_run"), dict) else {}
            top_candidate = _preferred_top_candidate(candidate_pull)
            error = str(summary.get("error") or "")
            row = {
                "recorded_at": captured_at.isoformat(),
                "sport_id": sport_id,
                "event_date": event_date,
                "status": summary.get("status"),
                "error": error,
                "candidate_status": candidate_pull.get("status"),
                "events_fetched": candidate_pull.get("events_fetched", 0),
                "candidates_written": candidate_pull.get("candidates_written", 0),
                "positive_ev_candidates": candidate_pull.get("positive_ev_candidates", 0),
                "accepted": paper_run.get("accepted", 0),
                "rejected": paper_run.get("rejected", 0),
                "avg_ev_accepted": paper_run.get("avg_ev_accepted", 0.0),
                "top_candidate_selection": top_candidate.get("selection", ""),
                "top_candidate_market": top_candidate.get("market", ""),
                "top_candidate_book": top_candidate.get("book", ""),
                "top_candidate_estimated_ev": top_candidate.get("estimated_ev", ""),
                "top_candidate_edge_rank_score": top_candidate.get("edge_rank_score", ""),
                "top_candidate_consensus_book_count": top_candidate.get("consensus_book_count", ""),
                "top_candidate_consensus_stability": top_candidate.get("consensus_stability", ""),
                "top_candidate_consensus_prob_range": top_candidate.get("consensus_prob_range", ""),
                "live_paper_summary_file": summary.get("output_file", ""),
                "candidate_summary_file": candidate_pull.get("output_file", ""),
                "paper_summary_file": paper_run.get("output_file", "") if isinstance(paper_run, dict) else "",
            }
            previous_row = next(
                (
                    candidate
                    for candidate in reversed(prior_rows)
                    if str(candidate.get("sport_id") or "") == str(sport_id)
                    and str(candidate.get("event_date") or "") == event_date
                ),
                None,
            )
            run_summaries.append(
                {
                    "event_date": event_date,
                    "status": summary.get("status"),
                    "error": error or None,
                    "candidate_summary_file": candidate_pull.get("output_file"),
                    "paper_summary_file": paper_run.get("output_file") if isinstance(paper_run, dict) else None,
                    "positive_ev_candidates": candidate_pull.get("positive_ev_candidates", 0),
                    "accepted": paper_run.get("accepted", 0),
                    "top_candidate_selection": top_candidate.get("selection"),
                    "top_candidate_estimated_ev": top_candidate.get("estimated_ev"),
                    "top_candidate_edge_rank_score": top_candidate.get("edge_rank_score"),
                    "top_candidate_consensus_book_count": top_candidate.get("consensus_book_count"),
                    "top_candidate_consensus_stability": top_candidate.get("consensus_stability"),
                    "top_candidate_consensus_prob_range": top_candidate.get("consensus_prob_range"),
                    "live_paper_summary_file": summary.get("output_file"),
                    "previous_recorded_at": previous_row.get("recorded_at") if previous_row else None,
                    "previous_status": previous_row.get("status") if previous_row else None,
                    "status_changed": (
                        bool(previous_row) and str(previous_row.get("status") or "") != str(summary.get("status") or "")
                    ),
                    "candidates_written_delta": _delta(row["candidates_written"], previous_row.get("candidates_written") if previous_row else None),
                    "positive_ev_candidates_delta": _delta(
                        row["positive_ev_candidates"], previous_row.get("positive_ev_candidates") if previous_row else None
                    ),
                    "accepted_delta": _delta(row["accepted"], previous_row.get("accepted") if previous_row else None),
                    "top_candidate_estimated_ev_delta": _delta(
                        row["top_candidate_estimated_ev"], previous_row.get("top_candidate_estimated_ev") if previous_row else None
                    ),
                    "top_candidate_consensus_book_count_delta": _delta(
                        row["top_candidate_consensus_book_count"],
                        previous_row.get("top_candidate_consensus_book_count") if previous_row else None,
                    ),
                    "top_candidate_consensus_stability_delta": _delta(
                        row["top_candidate_consensus_stability"],
                        previous_row.get("top_candidate_consensus_stability") if previous_row else None,
                    ),
                    "top_candidate_consensus_prob_range_delta": _delta(
                        row["top_candidate_consensus_prob_range"],
                        previous_row.get("top_candidate_consensus_prob_range") if previous_row else None,
                    ),
                    "top_candidate_book_changed": (
                        bool(previous_row)
                        and str(previous_row.get("top_candidate_book") or "") != str(row["top_candidate_book"] or "")
                    ),
                }
            )
        except Exception as exc:  # pragma: no cover - defensive runtime path
            row = {
                "recorded_at": captured_at.isoformat(),
                "sport_id": sport_id,
                "event_date": event_date,
                "status": "error",
                "error": str(exc),
                "candidate_status": "",
                "events_fetched": 0,
                "candidates_written": 0,
                "positive_ev_candidates": 0,
                "accepted": 0,
                "rejected": 0,
                "avg_ev_accepted": 0.0,
                "top_candidate_selection": "",
                "top_candidate_market": "",
                "top_candidate_book": "",
                "top_candidate_estimated_ev": "",
                "top_candidate_edge_rank_score": "",
                "top_candidate_consensus_book_count": "",
                "top_candidate_consensus_stability": "",
                "top_candidate_consensus_prob_range": "",
                "live_paper_summary_file": "",
                "candidate_summary_file": "",
                "paper_summary_file": "",
            }
            run_summaries.append(
                {
                    "event_date": event_date,
                    "status": "error",
                    "error": str(exc),
                }
            )
        rows_to_append.append(row)

    _append_archive_rows(archive_path, rows_to_append)

    ready_rows = [row for row in rows_to_append if row["status"] == "ready"]
    empty_rows = [row for row in rows_to_append if row["status"] == "empty"]
    error_rows = [row for row in rows_to_append if row["status"] == "error"]
    top_ready_row = max(
        ready_rows,
        key=lambda row: float(row["top_candidate_estimated_ev"] or 0.0),
        default=None,
    )

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "sport_id": sport_id,
        "event_dates": list(event_dates),
        "starting_bankroll": starting_bankroll,
        "config_path": config_path,
        "archive_csv": str(archive_path),
        "rows_appended": len(rows_to_append),
        "archive_rows_total": _count_archive_rows(archive_path),
        "dates_ready": len(ready_rows),
        "dates_empty": len(empty_rows),
        "dates_error": len(error_rows),
        "total_positive_ev_candidates": sum(int(row["positive_ev_candidates"] or 0) for row in rows_to_append),
        "total_paper_accepts": sum(int(row["accepted"] or 0) for row in rows_to_append),
        "top_ready_event_date": top_ready_row["event_date"] if isinstance(top_ready_row, dict) else None,
        "top_ready_candidate_selection": top_ready_row["top_candidate_selection"] if isinstance(top_ready_row, dict) else None,
        "top_ready_candidate_estimated_ev": (
            float(top_ready_row["top_candidate_estimated_ev"]) if isinstance(top_ready_row, dict) and top_ready_row["top_candidate_estimated_ev"] not in {"", None} else None
        ),
        "top_ready_candidate_edge_rank_score": (
            float(top_ready_row["top_candidate_edge_rank_score"])
            if isinstance(top_ready_row, dict) and top_ready_row["top_candidate_edge_rank_score"] not in {"", None}
            else None
        ),
        "top_ready_candidate_consensus_book_count": (
            int(top_ready_row["top_candidate_consensus_book_count"])
            if isinstance(top_ready_row, dict) and top_ready_row["top_candidate_consensus_book_count"] not in {"", None}
            else None
        ),
        "top_ready_candidate_consensus_stability": (
            float(top_ready_row["top_candidate_consensus_stability"])
            if isinstance(top_ready_row, dict) and top_ready_row["top_candidate_consensus_stability"] not in {"", None}
            else None
        ),
        "top_ready_candidate_consensus_prob_range": (
            float(top_ready_row["top_candidate_consensus_prob_range"])
            if isinstance(top_ready_row, dict) and top_ready_row["top_candidate_consensus_prob_range"] not in {"", None}
            else None
        ),
        "runs": run_summaries,
        "recent_history": _build_recent_history_summary(prior_rows, sport_id=sport_id),
        "status": "ready" if ready_rows else ("error" if error_rows and not empty_rows else "empty"),
    }

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"sports_archive_summary_{sport_id}_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
