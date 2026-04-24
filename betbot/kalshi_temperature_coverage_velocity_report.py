from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import statistics
import re
from typing import Any


_COVERAGE_VELOCITY_STATE_PATTERNS = (
    "health/decision_matrix_coverage_velocity_state_latest.json",
    "health/decision_matrix_coverage_velocity_state_*.json",
)
_HARDENING_HISTORY_PATTERN = "health/decision_matrix_hardening_*.json"
_STAMP_SUFFIX_RE = re.compile(r"(?:^|_)(\d{8}_\d{6})$")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _parse_optional_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or abs(parsed) == float("inf"):
        return None
    return float(parsed)


def _parse_boolish(value: Any) -> bool | None:
    text = _normalize_text(value).lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _parse_iso_utc(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _artifact_timestamp_utc(path: Path) -> datetime | None:
    match = _STAMP_SUFFIX_RE.search(path.stem)
    if not match:
        return None
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def _artifact_epoch(path: Path) -> float:
    stamped = _artifact_timestamp_utc(path)
    if isinstance(stamped, datetime):
        return stamped.timestamp()
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _artifact_age_hours(path_text: str, *, now: datetime) -> float | None:
    if not path_text:
        return None
    path = Path(path_text)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    now_utc = now.astimezone(timezone.utc)
    return round(max(0.0, (now_utc.timestamp() - float(mtime)) / 3600.0), 6)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{path.name}.tmp-{os.getpid()}-{datetime.now(timezone.utc).timestamp():.6f}"
    tmp_path = path.with_name(tmp_name)
    try:
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True))


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_payload(output_dir: Path, patterns: tuple[str, ...]) -> tuple[dict[str, Any] | None, str]:
    seen: set[str] = set()
    for pattern in patterns:
        candidates = sorted(output_dir.glob(pattern), key=_artifact_epoch, reverse=True)
        for path in candidates:
            if not path.is_file():
                continue
            resolved = str(path.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            payload = _load_json(path)
            if payload:
                return payload, str(path)
    return None, ""


def _find_history_files(output_dir: Path, limit: int) -> list[Path]:
    seen: set[str] = set()
    candidates: list[Path] = []
    for path in output_dir.glob(_HARDENING_HISTORY_PATTERN):
        if not path.is_file() or path.name.endswith("_latest.json"):
            continue
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        candidates.append(path)
    candidates.sort(key=lambda item: (_artifact_epoch(item), item.name))
    if limit > 0:
        candidates = candidates[-limit:]
    return candidates


def _extract_metric_int(sections: list[dict[str, Any]], keys: tuple[str, ...]) -> int | None:
    for section in sections:
        for key in keys:
            if key not in section:
                continue
            parsed = _parse_optional_int(section.get(key))
            if isinstance(parsed, int):
                return parsed
    return None


def _extract_metric_bool(sections: list[dict[str, Any]], keys: tuple[str, ...]) -> bool | None:
    for section in sections:
        for key in keys:
            if key not in section:
                continue
            parsed = _parse_boolish(section.get(key))
            if parsed is not None:
                return parsed
    return None


def _read_coverage_velocity_state(output_dir: Path, *, now: datetime) -> dict[str, Any]:
    payload, source_file = _latest_payload(output_dir, _COVERAGE_VELOCITY_STATE_PATTERNS)
    if not isinstance(payload, dict):
        return {
            "source": "",
            "status": "missing",
            "age_hours": None,
            "evidence_available": False,
            "selected_growth_source": "",
            "selected_growth_source_file": "",
            "selected_growth_delta_24h": None,
            "selected_growth_delta_7d": None,
            "selected_combined_bucket_count_delta_24h": None,
            "selected_combined_bucket_count_delta_7d": None,
            "positive_streak": 0,
            "non_positive_streak": 0,
            "required_positive_streak": 2,
            "guardrail_active": False,
            "guardrail_cleared": False,
            "last_evidence_direction": "missing",
        }

    return {
        "source": source_file,
        "status": _normalize_text(payload.get("status")).lower() or "unknown",
        "age_hours": _artifact_age_hours(source_file, now=now),
        "evidence_available": bool(payload.get("evidence_available")),
        "selected_growth_source": _normalize_text(payload.get("selected_growth_source")),
        "selected_growth_source_file": _normalize_text(payload.get("selected_growth_source_file")),
        "selected_growth_delta_24h": _parse_optional_int(payload.get("selected_growth_delta_24h")),
        "selected_growth_delta_7d": _parse_optional_int(payload.get("selected_growth_delta_7d")),
        "selected_combined_bucket_count_delta_24h": _parse_optional_int(
            payload.get("selected_combined_bucket_count_delta_24h")
        ),
        "selected_combined_bucket_count_delta_7d": _parse_optional_int(
            payload.get("selected_combined_bucket_count_delta_7d")
        ),
        "positive_streak": max(0, _parse_int(payload.get("positive_streak"))),
        "non_positive_streak": max(0, _parse_int(payload.get("non_positive_streak"))),
        "required_positive_streak": max(1, _parse_int(payload.get("required_positive_streak")) or 2),
        "guardrail_active": bool(payload.get("guardrail_active")),
        "guardrail_cleared": bool(payload.get("guardrail_cleared")),
        "last_evidence_direction": _normalize_text(payload.get("last_evidence_direction")).lower() or "missing",
    }


def _read_hardening_history(output_dir: Path, *, history_limit: int, now: datetime) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in _find_history_files(output_dir, history_limit):
        payload = _load_json(path)
        if not payload:
            continue
        captured_at = (
            _parse_iso_utc(payload.get("captured_at"))
            or _parse_iso_utc(payload.get("captured_at_utc"))
            or _artifact_timestamp_utc(path)
            or datetime.fromtimestamp(_artifact_epoch(path), tz=timezone.utc)
        )
        observed_metrics = payload.get("observed_metrics")
        if not isinstance(observed_metrics, dict):
            observed_metrics = payload
        sections = [observed_metrics, payload]
        selected_growth_delta_24h = _extract_metric_int(
            sections,
            (
                "coverage_velocity_selected_growth_delta_24h",
                "settled_outcome_growth_delta_24h",
                "settled_outcome_throughput_growth_delta_24h",
            ),
        )
        selected_growth_delta_7d = _extract_metric_int(
            sections,
            (
                "coverage_velocity_selected_growth_delta_7d",
                "settled_outcome_growth_delta_7d",
                "settled_outcome_throughput_growth_delta_7d",
            ),
        )
        selected_combined_bucket_count_delta_24h = _extract_metric_int(
            sections,
            (
                "coverage_velocity_selected_combined_bucket_count_delta_24h",
                "settled_outcome_growth_combined_bucket_count_delta_24h",
                "settled_outcome_throughput_combined_bucket_count_delta_24h",
            ),
        )
        selected_combined_bucket_count_delta_7d = _extract_metric_int(
            sections,
            (
                "coverage_velocity_selected_combined_bucket_count_delta_7d",
                "settled_outcome_growth_combined_bucket_count_delta_7d",
                "settled_outcome_throughput_combined_bucket_count_delta_7d",
            ),
        )
        positive_streak = _extract_metric_int(sections, ("coverage_velocity_positive_streak", "positive_streak"))
        non_positive_streak = _extract_metric_int(
            sections,
            ("coverage_velocity_non_positive_streak", "non_positive_streak"),
        )
        required_positive_streak = _extract_metric_int(
            sections,
            ("coverage_velocity_required_positive_streak", "required_positive_streak"),
        )
        guardrail_active = _extract_metric_bool(
            sections,
            ("coverage_velocity_guardrail_active", "guardrail_active"),
        )
        guardrail_cleared = _extract_metric_bool(
            sections,
            ("coverage_velocity_guardrail_cleared", "guardrail_cleared"),
        )
        selected_growth_source = _normalize_text(
            observed_metrics.get("settled_outcome_growth_source")
            or observed_metrics.get("settled_outcome_throughput_bottleneck_source")
            or payload.get("settled_outcome_growth_source")
            or payload.get("settled_outcome_throughput_bottleneck_source")
        )
        selected_growth_source_file = _normalize_text(
            observed_metrics.get("settled_outcome_growth_source_file")
            or observed_metrics.get("settled_outcome_throughput_source")
            or payload.get("settled_outcome_growth_source_file")
            or payload.get("settled_outcome_throughput_source")
        )
        records.append(
            {
                "source": str(path),
                "captured_at": captured_at,
                "age_hours": round(max(0.0, (now - captured_at).total_seconds() / 3600.0), 6),
                "selected_growth_delta_24h": selected_growth_delta_24h,
                "selected_growth_delta_7d": selected_growth_delta_7d,
                "selected_combined_bucket_count_delta_24h": selected_combined_bucket_count_delta_24h,
                "selected_combined_bucket_count_delta_7d": selected_combined_bucket_count_delta_7d,
                "positive_streak": max(0, _parse_int(positive_streak)),
                "non_positive_streak": max(0, _parse_int(non_positive_streak)),
                "required_positive_streak": max(1, _parse_int(required_positive_streak) or 2),
                "guardrail_active": bool(guardrail_active),
                "guardrail_cleared": bool(guardrail_cleared),
                "selected_growth_source": selected_growth_source,
                "selected_growth_source_file": selected_growth_source_file,
            }
        )
    records.sort(key=lambda row: row["captured_at"])
    return records


def _compute_median_run_cadence_hours(records: list[dict[str, Any]]) -> float | None:
    if len(records) < 2:
        return None
    deltas: list[float] = []
    for previous, current in zip(records, records[1:]):
        delta_hours = (current["captured_at"] - previous["captured_at"]).total_seconds() / 3600.0
        if delta_hours > 0.0:
            deltas.append(delta_hours)
    if not deltas:
        return None
    return round(float(statistics.median(deltas)), 6)


def _format_delta(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+d}"


def _build_summary_string(
    *,
    state: dict[str, Any],
    trend: dict[str, Any],
) -> str:
    guardrail_status = "cleared" if state.get("guardrail_cleared") else ("active" if state.get("guardrail_active") else "missing")
    cadence_hours = trend.get("median_run_cadence_hours")
    eta_hours = trend.get("estimated_hours_to_clear")
    cadence_text = f"{cadence_hours:.2f}h" if isinstance(cadence_hours, float) else "n/a"
    eta_text = f"{eta_hours:.2f}h" if isinstance(eta_hours, float) else "n/a"
    delta_text = ",".join(
        (
            f"24h={_format_delta(state.get('selected_growth_delta_24h'))}",
            f"7d={_format_delta(state.get('selected_growth_delta_7d'))}",
            f"c24h={_format_delta(state.get('selected_combined_bucket_count_delta_24h'))}",
            f"c7d={_format_delta(state.get('selected_combined_bucket_count_delta_7d'))}",
        )
    )
    source = _normalize_text(state.get("selected_growth_source")) or "unknown"
    return (
        f"guardrail={guardrail_status} "
        f"streak={int(state.get('positive_streak') or 0)}/{int(state.get('required_positive_streak') or 0)} "
        f"evidence={int(trend.get('evidence_run_count') or 0)} "
        f"pos_runs={int(trend.get('positive_run_count') or 0)} "
        f"non_pos_runs={int(trend.get('non_positive_run_count') or 0)} "
        f"deltas[{delta_text}] "
        f"eta={int(trend.get('estimated_runs_to_clear') or 0)} runs/{eta_text} "
        f"cadence={cadence_text} source={source}"
    )


def _build_payload(*, output_dir: str, history_limit: int, now: datetime) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    state = _read_coverage_velocity_state(out_dir, now=now)
    history_records = _read_hardening_history(out_dir, history_limit=max(0, int(history_limit)), now=now)
    evidence_records = [
        row
        for row in history_records
        if any(
            isinstance(row.get(key), int)
            for key in (
                "selected_growth_delta_24h",
                "selected_growth_delta_7d",
                "selected_combined_bucket_count_delta_24h",
                "selected_combined_bucket_count_delta_7d",
            )
        )
    ]
    positive_run_count = sum(
        1
        for row in evidence_records
        if any(
            isinstance(row.get(key), int) and int(row.get(key)) > 0
            for key in (
                "selected_growth_delta_24h",
                "selected_growth_delta_7d",
                "selected_combined_bucket_count_delta_24h",
                "selected_combined_bucket_count_delta_7d",
            )
        )
    )
    non_positive_run_count = max(0, len(evidence_records) - positive_run_count)
    median_run_cadence_hours = _compute_median_run_cadence_hours(history_records)
    estimated_runs_to_clear = 0
    estimated_hours_to_clear: float | None = 0.0 if state.get("guardrail_cleared") else None
    if state.get("guardrail_active"):
        estimated_runs_to_clear = max(0, int(state.get("required_positive_streak") or 0) - int(state.get("positive_streak") or 0))
        if estimated_runs_to_clear > 0 and isinstance(median_run_cadence_hours, float):
            estimated_hours_to_clear = round(float(estimated_runs_to_clear) * float(median_run_cadence_hours), 6)
        elif estimated_runs_to_clear == 0:
            estimated_hours_to_clear = 0.0
        else:
            estimated_hours_to_clear = None
    else:
        estimated_runs_to_clear = 0
        estimated_hours_to_clear = 0.0

    trend = {
        "history_limit": max(0, int(history_limit)),
        "history_records_considered": int(len(history_records)),
        "evidence_run_count": int(len(evidence_records)),
        "positive_run_count": int(positive_run_count),
        "non_positive_run_count": int(non_positive_run_count),
        "median_run_cadence_hours": median_run_cadence_hours,
        "estimated_runs_to_clear": int(estimated_runs_to_clear),
        "estimated_hours_to_clear": estimated_hours_to_clear,
    }

    report_summary = _build_summary_string(state=state, trend=trend)
    captured_at = now.astimezone(timezone.utc)
    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    output_path = health_dir / f"kalshi_temperature_coverage_velocity_report_{stamp}.json"
    latest_path = health_dir / "kalshi_temperature_coverage_velocity_report_latest.json"
    payload: dict[str, Any] = {
        "status": "ready" if state.get("source") else "missing_coverage_velocity_state",
        "captured_at": captured_at.isoformat(),
        "output_dir": str(out_dir),
        "health_dir": str(health_dir),
        "source_files": {
            "coverage_velocity_state": state.get("source") or "",
            "decision_matrix_hardening_history": [row["source"] for row in history_records],
        },
        "coverage_velocity_state": state,
        "trend": trend,
        "report_summary": report_summary,
        "output_file": str(output_path),
        "latest_file": str(latest_path),
    }
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    _write_text_atomic(output_path, encoded)
    _write_text_atomic(latest_path, encoded)
    return payload


def run_kalshi_temperature_coverage_velocity_report(
    *,
    output_dir: str = "outputs",
    history_limit: int = 24,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    return _build_payload(output_dir=output_dir, history_limit=history_limit, now=captured_at)


def summarize_kalshi_temperature_coverage_velocity_report(
    *,
    output_dir: str = "outputs",
    history_limit: int = 24,
    now: datetime | None = None,
) -> str:
    payload = run_kalshi_temperature_coverage_velocity_report(
        output_dir=output_dir,
        history_limit=history_limit,
        now=now,
    )
    return json.dumps(payload, indent=2, sort_keys=True)
