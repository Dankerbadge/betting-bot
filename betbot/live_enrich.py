from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any


REQUIRED_CANDIDATE_COLUMNS = {"timestamp", "event_id", "selection", "odds", "model_prob"}
EVIDENCE_COLUMNS = {
    "event_id",
    "selection",
    "team",
    "sport_id",
    "observed_at",
    "availability_signal",
    "lineup_signal",
    "news_signal",
    "source_confidence",
    "source_count",
    "conflict_flag",
    "source_note",
}


def _parse_iso_timestamp(value: str) -> datetime | None:
    text = (value or "").strip()
    if text == "":
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _parse_float(value: str | None, *, default: float = 0.0) -> float:
    if value is None:
        return default
    text = value.strip()
    if text == "":
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _parse_int(value: str | None, *, default: int = 0) -> int:
    if value is None:
        return default
    text = value.strip()
    if text == "":
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def _parse_bool(value: str | None) -> bool:
    text = (value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "t"}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _logit(probability: float) -> float:
    p = _clamp(probability, 1e-6, 1.0 - 1e-6)
    return math.log(p / (1.0 - p))


def _sigmoid(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


def _load_candidate_rows(candidate_csv: Path) -> tuple[list[dict[str, str]], list[str]]:
    with candidate_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        missing = REQUIRED_CANDIDATE_COLUMNS - set(fieldnames)
        if missing:
            raise ValueError(f"Missing required candidate columns: {sorted(missing)}")
        return list(reader), fieldnames


def _load_evidence_rows(evidence_csv: Path | None) -> list[dict[str, str]]:
    if evidence_csv is None or not evidence_csv.exists():
        return []
    with evidence_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        if not fieldnames:
            return []
        if not (fieldnames & EVIDENCE_COLUMNS):
            return []
        return [dict(row) for row in reader]


def _normalize_team_from_selection(selection: str) -> str:
    cleaned = selection.strip()
    if cleaned.endswith(" ML"):
        cleaned = cleaned[:-3].strip()
    return cleaned.lower()


def _find_matching_evidence(
    *,
    candidate_row: dict[str, str],
    evidence_rows: list[dict[str, str]],
) -> dict[str, str] | None:
    event_id = str(candidate_row.get("event_id") or "").strip()
    selection = str(candidate_row.get("selection") or "").strip().lower()
    selection_team = _normalize_team_from_selection(selection)
    sport_id = str(candidate_row.get("sport_id") or "").strip()
    best_team_match: dict[str, str] | None = None

    for evidence in evidence_rows:
        ev_event_id = str(evidence.get("event_id") or "").strip()
        ev_selection = str(evidence.get("selection") or "").strip().lower()
        ev_team = str(evidence.get("team") or "").strip().lower()
        ev_sport_id = str(evidence.get("sport_id") or "").strip()
        sport_matches = ev_sport_id == "" or sport_id == "" or ev_sport_id == sport_id

        if ev_event_id and event_id and ev_selection and ev_event_id == event_id and ev_selection == selection:
            if sport_matches:
                return evidence
            continue
        if ev_selection and ev_selection == selection:
            if sport_matches:
                return evidence
            continue
        if ev_team and (ev_team in selection_team or selection_team in ev_team):
            if sport_matches and best_team_match is None:
                best_team_match = evidence

    return best_team_match


def _enriched_fieldnames(existing: list[str]) -> list[str]:
    extras = [
        "model_prob_base",
        "decision_prob_base",
        "model_prob_shift",
        "enrichment_applied",
        "enrichment_reason",
        "enrichment_logit_delta",
        "enrichment_signal_raw",
        "enrichment_signal_confidence",
        "evidence_observed_at",
        "evidence_age_hours",
        "evidence_source_count",
        "evidence_conflict_flag",
        "evidence_source_note",
        "evidence_match_type",
    ]
    fieldnames = list(existing)
    for name in extras:
        if name not in fieldnames:
            fieldnames.append(name)
    return fieldnames


def _write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def run_live_candidate_enrichment(
    *,
    candidate_csv: str,
    output_dir: str = "outputs",
    evidence_csv: str | None = None,
    freshness_hours: float = 12.0,
    max_logit_shift: float = 0.35,
    moneyline_only: bool = True,
    apply_to_decision_prob: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    candidate_path = Path(candidate_csv)
    evidence_path = Path(evidence_csv) if evidence_csv else None
    rows, fieldnames = _load_candidate_rows(candidate_path)
    evidence_rows = _load_evidence_rows(evidence_path)
    enriched_rows: list[dict[str, str]] = []

    counts = {
        "rows_total": 0,
        "rows_adjusted": 0,
        "rows_moneyline_filtered": 0,
        "rows_no_evidence": 0,
        "rows_stale_evidence": 0,
        "rows_conflict_flag": 0,
        "rows_missing_timestamp": 0,
    }

    for row in rows:
        counts["rows_total"] += 1
        updated = dict(row)
        market = str(updated.get("market") or "").strip().lower()
        base_prob = _parse_float(updated.get("model_prob"), default=0.0)
        decision_prob = _parse_float(updated.get("decision_prob"), default=base_prob)
        updated["model_prob_base"] = f"{base_prob:.6f}"
        updated["decision_prob_base"] = f"{decision_prob:.6f}"
        updated["model_prob_shift"] = "0.000000"
        updated["enrichment_applied"] = "false"
        updated["enrichment_reason"] = "no_evidence"
        updated["enrichment_logit_delta"] = "0.000000"
        updated["enrichment_signal_raw"] = "0.000000"
        updated["enrichment_signal_confidence"] = "0.000000"
        updated["evidence_observed_at"] = ""
        updated["evidence_age_hours"] = ""
        updated["evidence_source_count"] = ""
        updated["evidence_conflict_flag"] = ""
        updated["evidence_source_note"] = ""
        updated["evidence_match_type"] = ""

        if moneyline_only and market not in {"moneyline", ""}:
            counts["rows_moneyline_filtered"] += 1
            updated["enrichment_reason"] = "market_not_moneyline"
            enriched_rows.append(updated)
            continue

        evidence = _find_matching_evidence(candidate_row=updated, evidence_rows=evidence_rows)
        if evidence is None:
            counts["rows_no_evidence"] += 1
            enriched_rows.append(updated)
            continue

        observed_at = _parse_iso_timestamp(str(evidence.get("observed_at") or ""))
        conflict_flag = _parse_bool(evidence.get("conflict_flag"))
        availability_signal = _parse_float(evidence.get("availability_signal"), default=0.0)
        lineup_signal = _parse_float(evidence.get("lineup_signal"), default=0.0)
        news_signal = _parse_float(evidence.get("news_signal"), default=0.0)
        source_confidence = _clamp(_parse_float(evidence.get("source_confidence"), default=1.0), 0.0, 1.0)
        source_count = _parse_int(evidence.get("source_count"), default=0)
        signal_raw = availability_signal + lineup_signal + news_signal

        updated["enrichment_signal_raw"] = f"{signal_raw:.6f}"
        updated["enrichment_signal_confidence"] = f"{source_confidence:.6f}"
        updated["evidence_source_count"] = str(source_count) if source_count > 0 else ""
        updated["evidence_conflict_flag"] = "true" if conflict_flag else "false"
        updated["evidence_source_note"] = str(evidence.get("source_note") or "")
        updated["evidence_observed_at"] = str(evidence.get("observed_at") or "")
        updated["evidence_match_type"] = "selection_or_team"

        if conflict_flag:
            counts["rows_conflict_flag"] += 1
            updated["enrichment_reason"] = "conflicting_evidence"
            enriched_rows.append(updated)
            continue

        if observed_at is None:
            counts["rows_missing_timestamp"] += 1
            updated["enrichment_reason"] = "missing_observed_at"
            enriched_rows.append(updated)
            continue

        age_hours = (captured_at - observed_at.astimezone(timezone.utc)).total_seconds() / 3600.0
        updated["evidence_age_hours"] = f"{age_hours:.3f}"
        if freshness_hours > 0 and age_hours > freshness_hours:
            counts["rows_stale_evidence"] += 1
            updated["enrichment_reason"] = "stale_evidence"
            enriched_rows.append(updated)
            continue

        logit_delta = _clamp(signal_raw * source_confidence, -abs(max_logit_shift), abs(max_logit_shift))
        adjusted_prob = _clamp(_sigmoid(_logit(base_prob) + logit_delta), 0.0, 1.0)
        prob_shift = adjusted_prob - base_prob

        updated["model_prob"] = f"{adjusted_prob:.6f}"
        if apply_to_decision_prob and "decision_prob" in updated:
            updated["decision_prob"] = f"{adjusted_prob:.6f}"
        updated["model_prob_shift"] = f"{prob_shift:.6f}"
        updated["enrichment_logit_delta"] = f"{logit_delta:.6f}"
        updated["enrichment_applied"] = "true"
        updated["enrichment_reason"] = "adjusted"
        counts["rows_adjusted"] += 1
        enriched_rows.append(updated)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    output_csv = out_dir / f"live_candidates_enriched_{stamp}.csv"
    summary_path = out_dir / f"live_candidates_enrichment_summary_{stamp}.json"
    _write_rows(output_csv, _enriched_fieldnames(fieldnames), enriched_rows)

    status = "ready"
    if not evidence_rows:
        status = "missing_evidence"
    elif counts["rows_adjusted"] <= 0:
        status = "no_adjustments"

    summary = {
        "captured_at": captured_at.isoformat(),
        "status": status,
        "candidate_csv": str(candidate_path),
        "evidence_csv": str(evidence_path) if evidence_path else None,
        "output_csv": str(output_csv),
        "freshness_hours": freshness_hours,
        "max_logit_shift": max_logit_shift,
        "moneyline_only": moneyline_only,
        "apply_to_decision_prob": apply_to_decision_prob,
        **counts,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
