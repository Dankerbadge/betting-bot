from __future__ import annotations

import csv
import itertools
import json
import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


_DEFAULT_THRESHOLD_CANDIDATE_LIMIT = 24
_WEATHER_PATTERN_ARTIFACT_PATTERNS = (
    "health/kalshi_temperature_weather_pattern_latest.json",
    "health/kalshi_temperature_weather_pattern_*.json",
    "**/health/kalshi_temperature_weather_pattern_latest.json",
    "**/health/kalshi_temperature_weather_pattern_*.json",
)
_EXECUTION_COST_TAPE_ARTIFACT_PATTERNS = (
    "health/execution_cost_tape_latest.json",
    "health/execution_cost_tape_*.json",
    "**/health/execution_cost_tape_latest.json",
    "**/health/execution_cost_tape_*.json",
    "health/kalshi_temperature_execution_cost_tape_latest.json",
    "health/kalshi_temperature_execution_cost_tape_*.json",
    "**/health/kalshi_temperature_execution_cost_tape_latest.json",
    "**/health/kalshi_temperature_execution_cost_tape_*.json",
)
_NEGATIVE_EXPECTANCY_ATTEMPT_SHARE_KEYS = (
    "negative_expectancy_attempt_share",
    "negative_expectancy_regime_concentration",
    "negative_expectancy_regime_share",
    "negative_expectancy_share",
    "regime_negative_expectancy_concentration",
    "regime_negative_expectancy_share",
)
_STALE_METAR_NEGATIVE_ATTEMPT_SHARE_KEYS = (
    "stale_metar_negative_attempt_share",
    "stale_metar_negative_expectancy_attempt_share",
    "negative_expectancy_stale_metar_share",
    "stale_metar_share",
    "stale_metar_negative_share",
    "stale_metar_attempt_share",
)
_HIGH_ENTRY_LOW_EDGE_EDGE_TO_RISK_CAP = 0.12
_HIGH_ENTRY_LOW_EDGE_EXPECTED_EDGE_MAX = 0.06
_HIGH_ENTRY_LOW_EDGE_IMPLIED_RISK_MIN = 0.30
_LOW_EDGE_TO_RISK_QUALITY_FLOOR = 0.08
_REPEAT_PRESSURE_SOFT_SHARE = 0.50
_REPEAT_PRESSURE_HARD_SHARE = 0.85
_REPEAT_PRESSURE_EDGE_MEAN_MAX = 0.09
_TAF_MISSING_STATION_SOFT_SHARE = 0.40
_TAF_MISSING_STATION_HARD_SHARE = 0.85
_TAF_MISSING_STATION_HARD_MIN_ROWS = 6
_EXECUTION_FRICTION_HARD_BLOCK_SEVERE_THRESHOLD = 0.80
_EXECUTION_FRICTION_HARD_BLOCK_MIN_EVIDENCE_COVERAGE = 0.50
_EXECUTION_FRICTION_HARD_BLOCK_WEATHER_ELEVATED_THRESHOLD = 0.30


@dataclass(frozen=True)
class LoadedTradeIntent:
    source_file: str
    row_number: int
    intent_id: str
    underlying_key: str
    settlement_station: str
    probability_confidence: float
    expected_edge_net: float
    edge_to_risk_ratio: float
    prelive_submission_ratio: float | None
    prelive_fill_ratio: float | None
    prelive_settlement_ratio: float | None
    taf_status: str
    policy_approved: bool


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return float(parsed)


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _normalize_text(value).lower()
    if text in {"1", "true", "t", "yes", "y", "approved", "approve", "pass", "selected"}:
        return True
    if text in {"0", "false", "f", "no", "n", "blocked", "reject", "rejected", "fail"}:
        return False
    return False


def _first_text(row: dict[str, str], keys: Iterable[str]) -> str:
    for key in keys:
        text = _normalize_text(row.get(key))
        if text:
            return text
    return ""


def _first_float(row: dict[str, str], keys: Iterable[str]) -> float | None:
    for key in keys:
        parsed = _safe_float(row.get(key))
        if isinstance(parsed, float):
            return parsed
    return None


def _first_ratio(row: dict[str, str], keys: Iterable[str]) -> float | None:
    for key in keys:
        parsed = _safe_float(row.get(key))
        if isinstance(parsed, float):
            return max(0.0, min(1.0, float(parsed)))
    return None


def _normalize_status_token(value: Any) -> str:
    return _normalize_text(value).lower().replace("-", "_").replace(" ", "_")


def _is_taf_missing_station_status(value: Any) -> bool:
    status = _normalize_status_token(value)
    if not status:
        return False
    return "missing_station" in status or status in {"station_missing", "missingstation"}


def _build_underlying_key(row: dict[str, str]) -> str:
    explicit = _first_text(row, ("underlying_key",))
    if explicit:
        return explicit
    series_ticker = _first_text(row, ("series_ticker",))
    station = _first_text(row, ("settlement_station",))
    target_date = _first_text(row, ("target_date_local",))
    market_ticker = _first_text(row, ("market_ticker",))
    components = [item for item in (series_ticker, station, target_date) if item]
    if components:
        return "|".join(components)
    if market_ticker:
        return market_ticker
    return "unknown_underlying"


def _build_station_key(row: dict[str, str]) -> str:
    return _first_text(row, ("settlement_station", "station")) or "unknown_station"


def _build_intent_id(row: dict[str, str], *, source_file: str, row_number: int) -> str:
    explicit = _first_text(row, ("intent_id", "temperature_intent_id", "client_order_id"))
    if explicit:
        return explicit
    components = [
        _first_text(row, ("market_ticker",)),
        _first_text(row, ("settlement_station", "station")),
        _first_text(row, ("target_date_local",)),
        _first_text(row, ("policy_version",)),
        str(row_number),
        Path(source_file).name,
    ]
    return "|".join(item for item in components if item) or f"{Path(source_file).name}:{row_number}"


def _parse_loaded_trade_intent(
    row: dict[str, str],
    *,
    source_file: str,
    row_number: int,
) -> tuple[LoadedTradeIntent | None, str | None]:
    probability_confidence = _first_float(
        row,
        ("policy_probability_confidence", "probability_confidence"),
    )
    expected_edge_net = _first_float(
        row,
        ("policy_expected_edge_net", "expected_edge_net", "expected_edge_dollars"),
    )
    edge_to_risk_ratio = _first_float(
        row,
        ("policy_edge_to_risk_ratio", "edge_to_risk_ratio"),
    )
    prelive_submission_ratio = _first_ratio(
        row,
        (
            "prelive_submission_ratio",
            "submission_ratio",
            "prelive_submission_fill_ratio",
        ),
    )
    prelive_fill_ratio = _first_ratio(
        row,
        (
            "prelive_fill_ratio",
            "fill_ratio",
        ),
    )
    prelive_settlement_ratio = _first_ratio(
        row,
        (
            "prelive_settlement_ratio",
            "settlement_ratio",
        ),
    )
    taf_status = _normalize_status_token(
        _first_text(
            row,
            (
                "taf_status",
                "policy_taf_status",
                "temperature_taf_status",
                "forecast_model_status",
                "forecast_status",
                "temperature_forecast_model_status",
            ),
        )
    ) or "unknown"
    if (
        probability_confidence is None
        or expected_edge_net is None
        or edge_to_risk_ratio is None
    ):
        return None, "missing_metric"

    intent = LoadedTradeIntent(
        source_file=source_file,
        row_number=row_number,
        intent_id=_build_intent_id(row, source_file=source_file, row_number=row_number),
        underlying_key=_build_underlying_key(row),
        settlement_station=_build_station_key(row),
        probability_confidence=float(probability_confidence),
        expected_edge_net=float(expected_edge_net),
        edge_to_risk_ratio=float(edge_to_risk_ratio),
        prelive_submission_ratio=prelive_submission_ratio,
        prelive_fill_ratio=prelive_fill_ratio,
        prelive_settlement_ratio=prelive_settlement_ratio,
        taf_status=taf_status,
        policy_approved=_safe_bool(
            _first_text(row, ("policy_approved", "approved", "is_approved"))
        ),
    )
    return intent, None


def _load_trade_intents(
    input_paths: Sequence[str | Path],
) -> tuple[list[LoadedTradeIntent], dict[str, Any]]:
    valid_records_by_id: dict[str, LoadedTradeIntent] = {}
    load_errors: list[dict[str, Any]] = []
    row_status_counts: Counter[str] = Counter()
    discovered_files: list[str] = []
    seen_inputs: set[str] = set()

    def _append_input_error(*, input_path: str, reason: str) -> None:
        load_errors.append({"input": input_path, "reason": reason})
        row_status_counts[reason] += 1

    for raw_input in input_paths:
        path = Path(raw_input)
        if path.is_dir():
            files = sorted(path.glob("kalshi_temperature_trade_intents_*.csv"))
            if not files:
                _append_input_error(input_path=str(path), reason="empty_directory")
            for file_path in files:
                normalized = str(file_path.resolve())
                if normalized in seen_inputs:
                    continue
                seen_inputs.add(normalized)
                discovered_files.append(normalized)
        else:
            if not path.exists():
                _append_input_error(input_path=str(path), reason="missing_input_file")
                continue
            normalized = str(path.resolve())
            if normalized not in seen_inputs:
                seen_inputs.add(normalized)
                discovered_files.append(normalized)

    for file_name in sorted(discovered_files):
        file_path = Path(file_name)
        try:
            handle = file_path.open("r", newline="", encoding="utf-8-sig")
        except OSError:
            _append_input_error(input_path=str(file_path), reason="unreadable_input_file")
            continue

        with handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                _append_input_error(input_path=str(file_path), reason="empty_csv_header")
                continue
            for row_number, row in enumerate(reader, start=2):
                if not isinstance(row, dict):
                    row_status_counts["malformed_row"] += 1
                    continue
                loaded, error_reason = _parse_loaded_trade_intent(
                    row,
                    source_file=str(file_path),
                    row_number=row_number,
                )
                if loaded is None:
                    row_status_counts[str(error_reason or "invalid_row")] += 1
                    continue
                valid_records_by_id[loaded.intent_id] = loaded
                row_status_counts["valid"] += 1

    rows = sorted(
        valid_records_by_id.values(),
        key=lambda item: (item.source_file, item.row_number, item.intent_id),
    )
    load_summary = {
        "input_files": discovered_files,
        "input_files_count": len(discovered_files),
        "raw_row_status_counts": dict(sorted(row_status_counts.items(), key=lambda item: (-item[1], item[0]))),
        "rows_valid": len(rows),
        "rows_unique": len(rows),
        "rows_deduplicated": max(0, int(row_status_counts.get("valid", 0)) - len(rows)),
        "load_errors": load_errors,
    }
    return rows, load_summary


def _path_epoch(path: Path) -> float:
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _unit_float(value: Any) -> float | None:
    parsed = _safe_float(value)
    if not isinstance(parsed, float):
        return None
    return max(0.0, min(1.0, float(parsed)))


def _safe_bool_with_numbers(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value) > 0.0
    return _safe_bool(value)


def _input_search_roots(input_paths: Sequence[str | Path]) -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()
    for raw_input in input_paths:
        path = Path(raw_input)
        root = path if path.is_dir() else path.parent
        normalized = str(root.resolve()) if root.exists() else str(root)
        if normalized in seen:
            continue
        seen.add(normalized)
        roots.append(root)
    return roots


def _weather_pattern_sections(payload: dict[str, Any]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    queue: list[dict[str, Any]] = [payload]
    seen: set[int] = set()
    nested_keys = (
        "headline_metrics",
        "summary",
        "metrics",
        "regime_summary",
        "weather_regime",
        "coverage_summary",
        "pattern_summary",
        "overall",
        "profile",
        "regime_risk",
        "recommendations",
        "weather_risk",
        "risk_off",
        "risk_off_recommendation",
    )
    while queue:
        section = queue.pop(0)
        section_id = id(section)
        if section_id in seen:
            continue
        seen.add(section_id)
        sections.append(section)
        for key in nested_keys:
            nested = section.get(key)
            if isinstance(nested, dict):
                queue.append(nested)
    return sections


def _weather_first_ratio(sections: Sequence[dict[str, Any]], keys: Sequence[str]) -> float | None:
    for section in sections:
        for key in keys:
            if key not in section:
                continue
            parsed = _unit_float(section.get(key))
            if isinstance(parsed, float):
                return parsed
    return None


def _weather_first_ratio_key_priority(sections: Sequence[dict[str, Any]], keys: Sequence[str]) -> float | None:
    for key in keys:
        for section in sections:
            if key not in section:
                continue
            parsed = _unit_float(section.get(key))
            if isinstance(parsed, float):
                return parsed
    return None


def _confidence_adjusted_ratio_aliases(keys: Sequence[str]) -> tuple[str, ...]:
    aliases: list[str] = []
    seen: set[str] = set()
    for key in keys:
        adjusted = f"{key}_confidence_adjusted"
        if adjusted not in seen:
            seen.add(adjusted)
            aliases.append(adjusted)
    return tuple(aliases)


def _weather_first_bool(sections: Sequence[dict[str, Any]], keys: Sequence[str]) -> tuple[bool | None, bool]:
    for section in sections:
        for key in keys:
            if key not in section:
                continue
            value = section.get(key)
            if isinstance(value, (dict, list, tuple, set)):
                continue
            text = _normalize_text(value)
            if not text and not isinstance(value, (bool, int, float)):
                continue
            return _safe_bool_with_numbers(value), True
    return None, False


def _weather_first_text(sections: Sequence[dict[str, Any]], keys: Sequence[str]) -> str:
    for section in sections:
        for key in keys:
            text = _normalize_text(section.get(key))
            if text:
                return text
    return ""


def _execution_cost_sections(payload: dict[str, Any]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    queue: list[dict[str, Any]] = [payload]
    seen: set[int] = set()
    nested_keys = (
        "summary",
        "metrics",
        "headline_metrics",
        "overall",
        "coverage_summary",
        "spread_summary",
        "execution_summary",
        "tape_summary",
        "profile",
        "health",
    )
    while queue:
        section = queue.pop(0)
        section_id = id(section)
        if section_id in seen:
            continue
        seen.add(section_id)
        sections.append(section)
        for key in nested_keys:
            nested = section.get(key)
            if isinstance(nested, dict):
                queue.append(nested)
        for value in section.values():
            if isinstance(value, dict):
                queue.append(value)
    return sections


def _execution_first_float(sections: Sequence[dict[str, Any]], keys: Sequence[str]) -> float | None:
    for section in sections:
        for key in keys:
            if key not in section:
                continue
            parsed = _safe_float(section.get(key))
            if isinstance(parsed, float):
                return float(parsed)
    return None


def _execution_first_ratio(sections: Sequence[dict[str, Any]], keys: Sequence[str]) -> float | None:
    for section in sections:
        for key in keys:
            if key not in section:
                continue
            parsed = _unit_float(section.get(key))
            if isinstance(parsed, float):
                return float(parsed)
    return None


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _scale_to_unit(value: float | None, *, floor: float, ceiling: float) -> float:
    if not isinstance(value, float):
        return 0.0
    if ceiling <= floor:
        return 0.0
    return _clamp01((float(value) - float(floor)) / max(1e-9, float(ceiling - floor)))


def _execution_top_ticker_max_share(payload: dict[str, Any]) -> float | None:
    sections = _execution_cost_sections(payload)
    direct = _execution_first_ratio(
        sections,
        (
            "top_ticker_max_share",
            "top_tickers_max_share",
            "ticker_concentration_max_share",
            "max_ticker_share",
        ),
    )
    if isinstance(direct, float):
        return direct
    for section in sections:
        top_tickers = section.get("top_tickers")
        if not isinstance(top_tickers, list):
            continue
        observed_max = 0.0
        found = False
        for row in top_tickers:
            if not isinstance(row, dict):
                continue
            share = _unit_float(
                row.get("share")
                or row.get("tape_share")
                or row.get("quote_share")
                or row.get("fraction")
            )
            if isinstance(share, float):
                observed_max = max(observed_max, float(share))
                found = True
        if found:
            return observed_max
    return None


def _parse_execution_cost_tape_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    sections = _execution_cost_sections(payload)
    spread_median_dollars = _execution_first_float(
        sections,
        (
            "spread_median_dollars",
            "median_spread_dollars",
            "spread_median",
        ),
    )
    spread_p90_dollars = _execution_first_float(
        sections,
        (
            "spread_p90_dollars",
            "p90_spread_dollars",
            "spread_p90",
        ),
    )
    quote_two_sided_ratio = _execution_first_ratio(
        sections,
        (
            "quote_two_sided_ratio",
            "two_sided_quote_ratio",
            "two_sided_ratio",
            "quote_coverage_ratio",
        ),
    )
    expected_edge_below_min_share = _execution_first_ratio(
        sections,
        (
            "expected_edge_below_min_share",
            "edge_below_min_share",
            "below_min_expected_edge_share",
            "latest_expected_edge_pressure_share_of_blocked",
            "expected_edge_pressure_share_of_blocked",
            "expected_edge_blocked_share",
            "expected_edge_blocking_share",
        ),
    )
    top_ticker_max_share = _execution_top_ticker_max_share(payload)

    spread_median_penalty = _scale_to_unit(spread_median_dollars, floor=0.010, ceiling=0.080)
    spread_p90_penalty = _scale_to_unit(spread_p90_dollars, floor=0.020, ceiling=0.150)
    quote_coverage_penalty = (
        _scale_to_unit(
            1.0 - quote_two_sided_ratio,
            floor=0.15,
            ceiling=0.50,
        )
        if isinstance(quote_two_sided_ratio, float)
        else 0.0
    )
    expected_edge_below_min_penalty = _scale_to_unit(
        expected_edge_below_min_share,
        floor=0.10,
        ceiling=0.60,
    )
    top_ticker_concentration_penalty = _scale_to_unit(
        top_ticker_max_share,
        floor=0.35,
        ceiling=0.75,
    )

    weighted_penalty = (
        0.35 * spread_median_penalty
        + 0.20 * spread_p90_penalty
        + 0.30 * quote_coverage_penalty
        + 0.10 * expected_edge_below_min_penalty
        + 0.05 * top_ticker_concentration_penalty
    )
    spread_quote_stress = min(spread_median_penalty, quote_coverage_penalty)
    if spread_quote_stress > 0.0:
        weighted_penalty += 0.20 * spread_quote_stress

    core_metric_count = 0
    for value in (
        spread_median_dollars,
        spread_p90_dollars,
        quote_two_sided_ratio,
        expected_edge_below_min_share,
    ):
        if isinstance(value, float):
            core_metric_count += 1
    evidence_coverage = float(core_metric_count) / 4.0
    # Slightly dampen penalties when coverage is partial to avoid overreaction.
    friction_penalty = _clamp01(weighted_penalty * (0.75 + (0.25 * evidence_coverage)))

    return {
        "status": _normalize_text(payload.get("status")).lower() or "unknown",
        "spread_median_dollars": spread_median_dollars,
        "spread_p90_dollars": spread_p90_dollars,
        "quote_two_sided_ratio": quote_two_sided_ratio,
        "expected_edge_below_min_share": expected_edge_below_min_share,
        "top_ticker_max_share": top_ticker_max_share,
        "core_metric_count": int(core_metric_count),
        "core_metric_total": 4,
        "evidence_coverage": round(float(evidence_coverage), 6),
        "penalty": round(float(friction_penalty), 6),
        "spread_median_penalty": round(float(spread_median_penalty), 6),
        "spread_p90_penalty": round(float(spread_p90_penalty), 6),
        "quote_coverage_penalty": round(float(quote_coverage_penalty), 6),
        "expected_edge_below_min_penalty": round(float(expected_edge_below_min_penalty), 6),
        "top_ticker_concentration_penalty": round(float(top_ticker_concentration_penalty), 6),
        "spread_quote_stress": round(float(spread_quote_stress), 6),
    }


def _parse_weather_pattern_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    sections = _weather_pattern_sections(payload)
    negative_expectancy_attempt_share_confidence_adjusted_observed = _weather_first_ratio_key_priority(
        sections,
        _confidence_adjusted_ratio_aliases(_NEGATIVE_EXPECTANCY_ATTEMPT_SHARE_KEYS),
    )
    negative_expectancy_attempt_share_raw_observed = _weather_first_ratio_key_priority(
        sections,
        _NEGATIVE_EXPECTANCY_ATTEMPT_SHARE_KEYS,
    )
    negative_expectancy_attempt_share = (
        negative_expectancy_attempt_share_confidence_adjusted_observed
        if isinstance(negative_expectancy_attempt_share_confidence_adjusted_observed, float)
        else negative_expectancy_attempt_share_raw_observed
    )
    negative_expectancy_attempt_share_source = (
        "confidence_adjusted"
        if isinstance(negative_expectancy_attempt_share_confidence_adjusted_observed, float)
        else "raw"
        if isinstance(negative_expectancy_attempt_share_raw_observed, float)
        else "missing"
    )

    stale_metar_negative_attempt_share_confidence_adjusted_observed = _weather_first_ratio_key_priority(
        sections,
        _confidence_adjusted_ratio_aliases(_STALE_METAR_NEGATIVE_ATTEMPT_SHARE_KEYS),
    )
    stale_metar_negative_attempt_share_raw_observed = _weather_first_ratio_key_priority(
        sections,
        _STALE_METAR_NEGATIVE_ATTEMPT_SHARE_KEYS,
    )
    stale_metar_negative_attempt_share = (
        stale_metar_negative_attempt_share_confidence_adjusted_observed
        if isinstance(stale_metar_negative_attempt_share_confidence_adjusted_observed, float)
        else stale_metar_negative_attempt_share_raw_observed
    )
    stale_metar_negative_attempt_share_source = (
        "confidence_adjusted"
        if isinstance(stale_metar_negative_attempt_share_confidence_adjusted_observed, float)
        else "raw"
        if isinstance(stale_metar_negative_attempt_share_raw_observed, float)
        else "missing"
    )

    risk_off_score = _weather_first_ratio(
        sections,
        (
            "risk_off_score",
            "risk_off_confidence",
            "risk_off_probability",
            "risk_off_pressure",
            "risk_off_strength",
            "risk_off_recommendation_strength",
            "score",
            "confidence",
            "strength",
        ),
    )
    risk_off_recommended, has_explicit_risk_off = _weather_first_bool(
        sections,
        (
            "risk_off_recommended",
            "risk_off",
            "recommend_risk_off",
            "risk_off_active",
            "risk_off_enabled",
            "active",
            "recommended",
        ),
    )
    recommendation_text = _weather_first_text(
        sections,
        (
            "recommendation",
            "recommended_action",
            "action",
            "advice",
            "regime_recommendation",
        ),
    ).lower()
    if not has_explicit_risk_off and recommendation_text:
        if "risk_off" in recommendation_text or "risk-off" in recommendation_text:
            risk_off_recommended = True
            has_explicit_risk_off = True
    if risk_off_recommended is None:
        risk_off_recommended = False
    if risk_off_recommended and not isinstance(risk_off_score, float):
        risk_off_score = 1.0
    return {
        "status": _normalize_text(payload.get("status")).lower() or "unknown",
        "negative_expectancy_attempt_share": negative_expectancy_attempt_share,
        "negative_expectancy_attempt_share_source": negative_expectancy_attempt_share_source,
        "negative_expectancy_attempt_share_confidence_adjusted_observed": negative_expectancy_attempt_share_confidence_adjusted_observed,
        "negative_expectancy_attempt_share_raw_observed": negative_expectancy_attempt_share_raw_observed,
        "stale_metar_negative_attempt_share": stale_metar_negative_attempt_share,
        "stale_metar_negative_attempt_share_source": stale_metar_negative_attempt_share_source,
        "stale_metar_negative_attempt_share_confidence_adjusted_observed": stale_metar_negative_attempt_share_confidence_adjusted_observed,
        "stale_metar_negative_attempt_share_raw_observed": stale_metar_negative_attempt_share_raw_observed,
        "risk_off_recommended": bool(risk_off_recommended),
        "risk_off_score": risk_off_score,
    }


def _load_weather_pattern_artifact(
    input_paths: Sequence[str | Path],
) -> dict[str, Any]:
    candidate_files: dict[str, Path] = {}
    for root in _input_search_roots(input_paths):
        for pattern in _WEATHER_PATTERN_ARTIFACT_PATTERNS:
            for candidate in root.glob(pattern):
                if not candidate.is_file():
                    continue
                normalized = str(candidate.resolve())
                candidate_files[normalized] = candidate
    ordered_candidates = sorted(
        candidate_files.values(),
        key=lambda path: (_path_epoch(path), str(path)),
        reverse=True,
    )

    load_errors: list[dict[str, Any]] = []
    selected_payload: dict[str, Any] | None = None
    selected_source = ""
    for candidate in ordered_candidates:
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            load_errors.append(
                {
                    "source_file": str(candidate),
                    "reason": "weather_pattern_artifact_unreadable",
                }
            )
            continue
        if not isinstance(payload, dict):
            load_errors.append(
                {
                    "source_file": str(candidate),
                    "reason": "weather_pattern_artifact_invalid",
                }
            )
            continue
        selected_payload = payload
        selected_source = str(candidate)
        break

    if selected_payload is None:
        return {
            "available": False,
            "status": "missing",
            "source_file": None,
            "files_discovered": len(ordered_candidates),
            "load_errors": load_errors,
            "negative_expectancy_attempt_share": None,
            "negative_expectancy_attempt_share_source": "missing",
            "negative_expectancy_attempt_share_confidence_adjusted_observed": None,
            "negative_expectancy_attempt_share_raw_observed": None,
            "stale_metar_negative_attempt_share": None,
            "stale_metar_negative_attempt_share_source": "missing",
            "stale_metar_negative_attempt_share_confidence_adjusted_observed": None,
            "stale_metar_negative_attempt_share_raw_observed": None,
            "risk_off_recommended": False,
            "risk_off_score": None,
        }

    parsed = _parse_weather_pattern_artifact(selected_payload)
    return {
        "available": True,
        "status": parsed["status"],
        "source_file": selected_source,
        "files_discovered": len(ordered_candidates),
        "load_errors": load_errors,
        "negative_expectancy_attempt_share": parsed["negative_expectancy_attempt_share"],
        "negative_expectancy_attempt_share_source": parsed["negative_expectancy_attempt_share_source"],
        "negative_expectancy_attempt_share_confidence_adjusted_observed": parsed[
            "negative_expectancy_attempt_share_confidence_adjusted_observed"
        ],
        "negative_expectancy_attempt_share_raw_observed": parsed["negative_expectancy_attempt_share_raw_observed"],
        "stale_metar_negative_attempt_share": parsed["stale_metar_negative_attempt_share"],
        "stale_metar_negative_attempt_share_source": parsed["stale_metar_negative_attempt_share_source"],
        "stale_metar_negative_attempt_share_confidence_adjusted_observed": parsed[
            "stale_metar_negative_attempt_share_confidence_adjusted_observed"
        ],
        "stale_metar_negative_attempt_share_raw_observed": parsed["stale_metar_negative_attempt_share_raw_observed"],
        "risk_off_recommended": parsed["risk_off_recommended"],
        "risk_off_score": parsed["risk_off_score"],
    }


def _load_execution_cost_tape_artifact(
    input_paths: Sequence[str | Path],
) -> dict[str, Any]:
    candidate_files: dict[str, Path] = {}
    for root in _input_search_roots(input_paths):
        for pattern in _EXECUTION_COST_TAPE_ARTIFACT_PATTERNS:
            for candidate in root.glob(pattern):
                if not candidate.is_file():
                    continue
                normalized = str(candidate.resolve())
                candidate_files[normalized] = candidate
    ordered_candidates = sorted(
        candidate_files.values(),
        key=lambda path: (_path_epoch(path), str(path)),
        reverse=True,
    )

    load_errors: list[dict[str, Any]] = []
    selected_payload: dict[str, Any] | None = None
    selected_source = ""
    for candidate in ordered_candidates:
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            load_errors.append(
                {
                    "source_file": str(candidate),
                    "reason": "execution_cost_tape_artifact_unreadable",
                }
            )
            continue
        if not isinstance(payload, dict):
            load_errors.append(
                {
                    "source_file": str(candidate),
                    "reason": "execution_cost_tape_artifact_invalid",
                }
            )
            continue
        selected_payload = payload
        selected_source = str(candidate)
        break

    if selected_payload is None:
        return {
            "available": False,
            "status": "missing",
            "source_file": None,
            "files_discovered": len(ordered_candidates),
            "load_errors": load_errors,
            "spread_median_dollars": None,
            "spread_p90_dollars": None,
            "quote_two_sided_ratio": None,
            "expected_edge_below_min_share": None,
            "top_ticker_max_share": None,
            "core_metric_count": 0,
            "core_metric_total": 4,
            "evidence_coverage": 0.0,
            "penalty": 0.0,
            "spread_median_penalty": 0.0,
            "spread_p90_penalty": 0.0,
            "quote_coverage_penalty": 0.0,
            "expected_edge_below_min_penalty": 0.0,
            "top_ticker_concentration_penalty": 0.0,
            "spread_quote_stress": 0.0,
        }

    parsed = _parse_execution_cost_tape_artifact(selected_payload)
    return {
        "available": True,
        "status": parsed["status"],
        "source_file": selected_source,
        "files_discovered": len(ordered_candidates),
        "load_errors": load_errors,
        "spread_median_dollars": parsed["spread_median_dollars"],
        "spread_p90_dollars": parsed["spread_p90_dollars"],
        "quote_two_sided_ratio": parsed["quote_two_sided_ratio"],
        "expected_edge_below_min_share": parsed["expected_edge_below_min_share"],
        "top_ticker_max_share": parsed["top_ticker_max_share"],
        "core_metric_count": parsed["core_metric_count"],
        "core_metric_total": parsed["core_metric_total"],
        "evidence_coverage": parsed["evidence_coverage"],
        "penalty": parsed["penalty"],
        "spread_median_penalty": parsed["spread_median_penalty"],
        "spread_p90_penalty": parsed["spread_p90_penalty"],
        "quote_coverage_penalty": parsed["quote_coverage_penalty"],
        "expected_edge_below_min_penalty": parsed["expected_edge_below_min_penalty"],
        "top_ticker_concentration_penalty": parsed["top_ticker_concentration_penalty"],
        "spread_quote_stress": parsed["spread_quote_stress"],
    }


def _candidate_thresholds(
    values: Sequence[float],
    *,
    limit: int = _DEFAULT_THRESHOLD_CANDIDATE_LIMIT,
) -> list[float]:
    safe_limit = max(1, int(limit))
    normalized = sorted({round(float(value), 6) for value in values if isinstance(value, (int, float)) and math.isfinite(float(value))})
    if not normalized:
        return [0.0]
    if len(normalized) <= safe_limit:
        candidates = list(normalized)
        if 0.0 not in candidates:
            candidates.append(0.0)
        return sorted(set(round(float(item), 6) for item in candidates))

    sample_count = max(1, safe_limit - 1)
    sampled: list[float] = []
    if sample_count == 1:
        sampled.append(normalized[0])
    else:
        for index in range(sample_count):
            position = round(index * (len(normalized) - 1) / float(sample_count - 1))
            sampled.append(normalized[int(position)])
    sampled.append(0.0)
    return sorted(set(round(float(item), 6) for item in sampled))


def _concentration_stats(selected_rows: Sequence[LoadedTradeIntent]) -> dict[str, Any]:
    total = len(selected_rows)
    if total <= 0:
        return {
            "total": 0,
            "unique": 0,
            "max_share": 0.0,
            "hhi": 0.0,
            "top": [],
        }
    counts = Counter(selected_rows)
    total_float = float(total)
    ranked = sorted(
        ((str(key), int(count)) for key, count in counts.items()),
        key=lambda item: (-item[1], item[0]),
    )
    hhi = 0.0
    top_entries: list[dict[str, Any]] = []
    for key, count in ranked:
        share = float(count) / total_float
        hhi += share * share
        if len(top_entries) < 5:
            top_entries.append(
                {
                    "key": key,
                    "count": int(count),
                    "share": round(float(share), 6),
                }
            )
    max_share = float(ranked[0][1]) / total_float if ranked else 0.0
    return {
        "total": int(total),
        "unique": len(ranked),
        "max_share": round(float(max_share), 6),
        "hhi": round(float(hhi), 6),
        "top": top_entries,
    }


def _concentration_limit(selected_count: int) -> float:
    if selected_count <= 1:
        return 1.0
    if selected_count == 2:
        return 0.95
    if selected_count == 3:
        return 0.90
    if selected_count <= 5:
        return 0.85
    if selected_count <= 10:
        return 0.75
    return 0.65


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return float(ordered[0])
    safe_percentile = max(0.0, min(1.0, float(percentile)))
    index = int(round(safe_percentile * float(len(ordered) - 1)))
    return float(ordered[index])


def _mean_or_none(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(sum(float(value) for value in values) / float(len(values)))


def _candidate_conversion_ratios(selected: Sequence[LoadedTradeIntent]) -> dict[str, Any]:
    submission_values = [row.prelive_submission_ratio for row in selected if isinstance(row.prelive_submission_ratio, float)]
    fill_values = [row.prelive_fill_ratio for row in selected if isinstance(row.prelive_fill_ratio, float)]
    settlement_values = [
        row.prelive_settlement_ratio
        for row in selected
        if isinstance(row.prelive_settlement_ratio, float)
    ]
    per_row_values: list[float] = []
    for row in selected:
        ratios = [
            value
            for value in (
                row.prelive_submission_ratio,
                row.prelive_fill_ratio,
                row.prelive_settlement_ratio,
            )
            if isinstance(value, float)
        ]
        if ratios:
            per_row_values.append(float(sum(ratios) / float(len(ratios))))
    return {
        "available": bool(submission_values or fill_values or settlement_values),
        "selected_rows_with_conversion_data": len(per_row_values),
        "submission_ratio_mean": _mean_or_none(submission_values),
        "fill_ratio_mean": _mean_or_none(fill_values),
        "settlement_ratio_mean": _mean_or_none(settlement_values),
        "per_row_quality_mean": _mean_or_none(per_row_values),
        "per_row_quality_median": _percentile(per_row_values, 0.5),
        "per_row_quality_min": min(per_row_values) if per_row_values else None,
    }


def _weather_risk_metrics(
    weather_pattern_artifact: dict[str, Any] | None,
    *,
    negative_expectancy_soft_limit: float,
    stale_metar_soft_limit: float,
    risk_off_hard_threshold: float,
) -> dict[str, Any]:
    artifact = weather_pattern_artifact if isinstance(weather_pattern_artifact, dict) else {}
    available = bool(artifact.get("available"))
    negative_share = _unit_float(artifact.get("negative_expectancy_attempt_share"))
    negative_share_source = _normalize_text(artifact.get("negative_expectancy_attempt_share_source")).lower() or "missing"
    negative_share_confidence_adjusted_observed = _unit_float(
        artifact.get("negative_expectancy_attempt_share_confidence_adjusted_observed")
    )
    negative_share_raw_observed = _unit_float(artifact.get("negative_expectancy_attempt_share_raw_observed"))
    stale_metar_share = _unit_float(artifact.get("stale_metar_negative_attempt_share"))
    stale_metar_share_source = _normalize_text(artifact.get("stale_metar_negative_attempt_share_source")).lower() or "missing"
    stale_metar_share_confidence_adjusted_observed = _unit_float(
        artifact.get("stale_metar_negative_attempt_share_confidence_adjusted_observed")
    )
    stale_metar_share_raw_observed = _unit_float(
        artifact.get("stale_metar_negative_attempt_share_raw_observed")
    )
    risk_off_recommended = bool(artifact.get("risk_off_recommended"))
    risk_off_score = _unit_float(artifact.get("risk_off_score"))
    if risk_off_recommended and not isinstance(risk_off_score, float):
        risk_off_score = 1.0

    safe_negative_expectancy_soft_limit = max(0.0, min(0.99, float(negative_expectancy_soft_limit)))
    safe_stale_metar_soft_limit = max(0.0, min(0.99, float(stale_metar_soft_limit)))
    safe_risk_off_hard_threshold = max(0.0, min(1.0, float(risk_off_hard_threshold)))

    negative_expectancy_penalty = 0.0
    if isinstance(negative_share, float):
        negative_expectancy_penalty = max(
            0.0,
            min(
                1.0,
                (negative_share - safe_negative_expectancy_soft_limit)
                / max(0.01, 1.0 - safe_negative_expectancy_soft_limit),
            ),
        )
    stale_metar_penalty = 0.0
    if isinstance(stale_metar_share, float):
        stale_metar_penalty = max(
            0.0,
            min(
                1.0,
                (stale_metar_share - safe_stale_metar_soft_limit)
                / max(0.01, 1.0 - safe_stale_metar_soft_limit),
            ),
        )
    risk_off_penalty = 0.0
    if risk_off_recommended:
        risk_off_penalty = float(risk_off_score) if isinstance(risk_off_score, float) else 1.0
    hard_block_active = bool(
        risk_off_recommended
        and isinstance(risk_off_score, float)
        and float(risk_off_score) >= safe_risk_off_hard_threshold
    )
    return {
        "available": bool(available),
        "status": _normalize_text(artifact.get("status")).lower() or "missing",
        "source_file": _normalize_text(artifact.get("source_file")) or None,
        "negative_expectancy_attempt_share": negative_share,
        "negative_expectancy_attempt_share_source": negative_share_source,
        "negative_expectancy_attempt_share_confidence_adjusted_observed": negative_share_confidence_adjusted_observed,
        "negative_expectancy_attempt_share_raw_observed": negative_share_raw_observed,
        "stale_metar_negative_attempt_share": stale_metar_share,
        "stale_metar_negative_attempt_share_source": stale_metar_share_source,
        "stale_metar_negative_attempt_share_confidence_adjusted_observed": stale_metar_share_confidence_adjusted_observed,
        "stale_metar_negative_attempt_share_raw_observed": stale_metar_share_raw_observed,
        "risk_off_recommended": bool(risk_off_recommended),
        "risk_off_score": risk_off_score,
        "negative_expectancy_soft_limit": round(float(safe_negative_expectancy_soft_limit), 6),
        "stale_metar_soft_limit": round(float(safe_stale_metar_soft_limit), 6),
        "risk_off_hard_threshold": round(float(safe_risk_off_hard_threshold), 6),
        "negative_expectancy_penalty": round(float(negative_expectancy_penalty), 6),
        "stale_metar_penalty": round(float(stale_metar_penalty), 6),
        "risk_off_penalty": round(float(risk_off_penalty), 6),
        "hard_block_active": hard_block_active,
    }


def _execution_friction_metrics(
    execution_cost_tape_artifact: dict[str, Any] | None,
) -> dict[str, Any]:
    artifact = execution_cost_tape_artifact if isinstance(execution_cost_tape_artifact, dict) else {}
    available = bool(artifact.get("available"))
    penalty = _unit_float(artifact.get("penalty")) if available else 0.0
    evidence_coverage = _unit_float(artifact.get("evidence_coverage")) if available else 0.0
    spread_quote_stress = _unit_float(artifact.get("spread_quote_stress")) if available else 0.0
    severe = bool(
        available
        and isinstance(penalty, float)
        and penalty >= _EXECUTION_FRICTION_HARD_BLOCK_SEVERE_THRESHOLD
        and isinstance(evidence_coverage, float)
        and evidence_coverage >= _EXECUTION_FRICTION_HARD_BLOCK_MIN_EVIDENCE_COVERAGE
    )
    return {
        "available": bool(available),
        "status": _normalize_text(artifact.get("status")).lower() or "missing",
        "source_file": _normalize_text(artifact.get("source_file")) or None,
        "spread_median_dollars": _safe_float(artifact.get("spread_median_dollars")),
        "spread_p90_dollars": _safe_float(artifact.get("spread_p90_dollars")),
        "quote_two_sided_ratio": _unit_float(artifact.get("quote_two_sided_ratio")),
        "expected_edge_below_min_share": _unit_float(artifact.get("expected_edge_below_min_share")),
        "top_ticker_max_share": _unit_float(artifact.get("top_ticker_max_share")),
        "core_metric_count": int(artifact.get("core_metric_count") or 0),
        "core_metric_total": int(artifact.get("core_metric_total") or 4),
        "evidence_coverage": round(float(evidence_coverage if isinstance(evidence_coverage, float) else 0.0), 6),
        "penalty": round(float(penalty if isinstance(penalty, float) else 0.0), 6),
        "spread_median_penalty": round(float(_unit_float(artifact.get("spread_median_penalty")) or 0.0), 6),
        "spread_p90_penalty": round(float(_unit_float(artifact.get("spread_p90_penalty")) or 0.0), 6),
        "quote_coverage_penalty": round(float(_unit_float(artifact.get("quote_coverage_penalty")) or 0.0), 6),
        "expected_edge_below_min_penalty": round(
            float(_unit_float(artifact.get("expected_edge_below_min_penalty")) or 0.0),
            6,
        ),
        "top_ticker_concentration_penalty": round(
            float(_unit_float(artifact.get("top_ticker_concentration_penalty")) or 0.0),
            6,
        ),
        "spread_quote_stress": round(
            float(spread_quote_stress if isinstance(spread_quote_stress, float) else 0.0),
            6,
        ),
        "severe": severe,
    }


def _robustness_metrics(
    candidate: dict[str, Any],
    *,
    calibration_available: bool,
    weather_pattern_artifact: dict[str, Any] | None,
    execution_cost_tape_artifact: dict[str, Any] | None,
    robustness_conversion_weight: float,
    robustness_concentration_weight: float,
    robustness_edge_guardrail_weight: float,
    robustness_weather_negative_expectancy_weight: float,
    robustness_weather_stale_metar_weight: float,
    robustness_weather_risk_off_weight: float,
    robustness_execution_friction_weight: float,
    robustness_bonus_cap: float,
    robustness_edge_median_floor: float,
    robustness_edge_median_target: float,
    robustness_tail_ratio_floor: float,
    weather_negative_expectancy_attempt_share_soft_limit: float,
    weather_stale_metar_negative_attempt_share_soft_limit: float,
    weather_risk_off_hard_threshold: float,
) -> dict[str, Any]:
    weather_risk = _weather_risk_metrics(
        weather_pattern_artifact,
        negative_expectancy_soft_limit=weather_negative_expectancy_attempt_share_soft_limit,
        stale_metar_soft_limit=weather_stale_metar_negative_attempt_share_soft_limit,
        risk_off_hard_threshold=weather_risk_off_hard_threshold,
    )
    execution_friction = _execution_friction_metrics(execution_cost_tape_artifact)
    robustness_enabled = bool(calibration_available or weather_risk["available"] or execution_friction["available"])
    weather_penalty_weighted = 0.0
    if weather_risk["available"]:
        weather_penalty_weighted = (
            float(max(0.0, robustness_weather_negative_expectancy_weight))
            * float(weather_risk["negative_expectancy_penalty"])
            + float(max(0.0, robustness_weather_stale_metar_weight))
            * float(weather_risk["stale_metar_penalty"])
            + float(max(0.0, robustness_weather_risk_off_weight))
            * float(weather_risk["risk_off_penalty"])
        )
    execution_friction_penalty_weighted = 0.0
    if execution_friction["available"]:
        execution_friction_penalty_weighted = (
            float(max(0.0, robustness_execution_friction_weight))
            * float(execution_friction["penalty"])
        )
    if not robustness_enabled:
        return {
            "enabled": False,
            "calibration_available": bool(calibration_available),
            "score_multiplier": 1.0,
            "score_penalty": 0.0,
            "score_bonus": 0.0,
            "conversion": {
                "available": False,
                "selected_rows_with_conversion_data": 0,
                "submission_ratio_mean": None,
                "fill_ratio_mean": None,
                "settlement_ratio_mean": None,
                "per_row_quality_mean": None,
                "per_row_quality_median": None,
                "per_row_quality_min": None,
                "quality_score": None,
                "penalty": 0.0,
                "bonus": 0.0,
            },
            "concentration": {
                "underlying_max_share": candidate.get("selected_underlying_max_share"),
                "station_max_share": candidate.get("selected_station_max_share"),
                "underlying_hhi": candidate.get("selected_underlying_hhi"),
                "station_hhi": candidate.get("selected_station_hhi"),
                "safety_limit_max_share": candidate.get("safety_limit_max_share"),
                "fragility_score": 0.0,
                "penalty": 0.0,
                "bonus": 0.0,
            },
            "edge_guardrail": {
                "selected_expected_edge_median": None,
                "selected_expected_edge_p10": None,
                "median_quality": None,
                "tail_quality": None,
                "fragility_score": 0.0,
                "penalty": 0.0,
                "bonus": 0.0,
            },
            "weather_risk": {
                "available": bool(weather_risk["available"]),
                "status": weather_risk["status"],
                "source_file": weather_risk["source_file"],
                "negative_expectancy_attempt_share": weather_risk["negative_expectancy_attempt_share"],
                "negative_expectancy_attempt_share_source": weather_risk["negative_expectancy_attempt_share_source"],
                "negative_expectancy_attempt_share_confidence_adjusted_observed": weather_risk[
                    "negative_expectancy_attempt_share_confidence_adjusted_observed"
                ],
                "negative_expectancy_attempt_share_raw_observed": weather_risk[
                    "negative_expectancy_attempt_share_raw_observed"
                ],
                "stale_metar_negative_attempt_share": weather_risk["stale_metar_negative_attempt_share"],
                "stale_metar_negative_attempt_share_source": weather_risk[
                    "stale_metar_negative_attempt_share_source"
                ],
                "stale_metar_negative_attempt_share_confidence_adjusted_observed": weather_risk[
                    "stale_metar_negative_attempt_share_confidence_adjusted_observed"
                ],
                "stale_metar_negative_attempt_share_raw_observed": weather_risk[
                    "stale_metar_negative_attempt_share_raw_observed"
                ],
                "risk_off_recommended": bool(weather_risk["risk_off_recommended"]),
                "risk_off_score": weather_risk["risk_off_score"],
                "negative_expectancy_soft_limit": weather_risk["negative_expectancy_soft_limit"],
                "stale_metar_soft_limit": weather_risk["stale_metar_soft_limit"],
                "risk_off_hard_threshold": weather_risk["risk_off_hard_threshold"],
                "negative_expectancy_penalty": weather_risk["negative_expectancy_penalty"],
                "stale_metar_penalty": weather_risk["stale_metar_penalty"],
                "risk_off_penalty": weather_risk["risk_off_penalty"],
                "weighted_penalty": round(float(weather_penalty_weighted), 6),
                "hard_block_active": bool(weather_risk["hard_block_active"]),
            },
            "execution_friction": {
                "available": bool(execution_friction["available"]),
                "status": execution_friction["status"],
                "source_file": execution_friction["source_file"],
                "spread_median_dollars": execution_friction["spread_median_dollars"],
                "spread_p90_dollars": execution_friction["spread_p90_dollars"],
                "quote_two_sided_ratio": execution_friction["quote_two_sided_ratio"],
                "expected_edge_below_min_share": execution_friction["expected_edge_below_min_share"],
                "top_ticker_max_share": execution_friction["top_ticker_max_share"],
                "core_metric_count": execution_friction["core_metric_count"],
                "core_metric_total": execution_friction["core_metric_total"],
                "evidence_coverage": execution_friction["evidence_coverage"],
                "penalty": execution_friction["penalty"],
                "spread_median_penalty": execution_friction["spread_median_penalty"],
                "spread_p90_penalty": execution_friction["spread_p90_penalty"],
                "quote_coverage_penalty": execution_friction["quote_coverage_penalty"],
                "expected_edge_below_min_penalty": execution_friction["expected_edge_below_min_penalty"],
                "top_ticker_concentration_penalty": execution_friction["top_ticker_concentration_penalty"],
                "spread_quote_stress": execution_friction["spread_quote_stress"],
                "severe": bool(execution_friction["severe"]),
                "weighted_penalty": round(float(execution_friction_penalty_weighted), 6),
            },
        }

    selected_rows_with_conversion_data = int(candidate.get("selected_rows_with_conversion_data") or 0)
    conversion_quality = candidate.get("selected_conversion_quality_mean")
    conversion_penalty = 0.0
    conversion_bonus = 0.0
    conversion_available = bool(selected_rows_with_conversion_data > 0 and isinstance(conversion_quality, (int, float)))
    if conversion_available:
        conversion_quality = max(0.0, min(1.0, float(conversion_quality)))
        conversion_penalty = max(0.0, 1.0 - conversion_quality)
        conversion_bonus = max(0.0, conversion_quality - 0.85)

    selected_underlying_max_share = float(candidate.get("selected_underlying_max_share") or 0.0)
    selected_station_max_share = float(candidate.get("selected_station_max_share") or 0.0)
    selected_underlying_hhi = float(candidate.get("selected_underlying_hhi") or 0.0)
    selected_station_hhi = float(candidate.get("selected_station_hhi") or 0.0)
    safety_limit_max_share = float(candidate.get("safety_limit_max_share") or 0.0)
    concentration_fragility = 0.0
    concentration_penalty = 0.0
    concentration_bonus = 0.0

    selected_expected_edge_median = candidate.get("selected_expected_edge_median")
    selected_expected_edge_p10 = candidate.get("selected_expected_edge_p10")
    selected_expected_edge_mean = float(candidate.get("selected_expected_edge_mean") or 0.0)
    if isinstance(selected_expected_edge_median, (int, float)):
        selected_expected_edge_median = float(selected_expected_edge_median)
    if isinstance(selected_expected_edge_p10, (int, float)):
        selected_expected_edge_p10 = float(selected_expected_edge_p10)
    median_quality = None
    tail_quality = None
    edge_guardrail_fragility = 0.0
    edge_guardrail_penalty = 0.0
    edge_guardrail_bonus = 0.0

    if calibration_available:
        concentration_reference = max(
            0.35,
            min(0.90, float(safety_limit_max_share) if safety_limit_max_share > 0.0 else 0.75),
        )
        concentration_fragility = max(
            0.0,
            min(
                1.0,
                (
                    max(0.0, selected_underlying_max_share - concentration_reference)
                    / max(0.01, 1.0 - concentration_reference)
                )
                * 0.50
                + (
                    max(0.0, selected_station_max_share - concentration_reference)
                    / max(0.01, 1.0 - concentration_reference)
                )
                * 0.50,
            ),
        )
        hhi_floor = 0.0
        if selected_count := int(candidate.get("intents_selected") or 0):
            hhi_floor = 1.0 / float(max(1, selected_count))
        hhi_fragility = max(
            0.0,
            min(
                1.0,
                (selected_underlying_hhi - hhi_floor) * 1.5
                + (selected_station_hhi - hhi_floor) * 1.5,
            ),
        )
        concentration_fragility = max(
            0.0,
            min(1.0, (0.70 * concentration_fragility) + (0.30 * hhi_fragility)),
        )
        concentration_penalty = concentration_fragility
        concentration_bonus = max(
            0.0,
            min(
                float(robustness_bonus_cap),
                0.15 * max(0.0, 1.0 - max(selected_underlying_max_share, selected_station_max_share)),
            ),
        )

        thin_margin_penalty = 0.0
        tail_fragility_penalty = 0.0
        edge_bonus = 0.0
        if isinstance(selected_expected_edge_median, float) and selected_expected_edge_median > 0.0:
            median_quality = max(
                0.0,
                min(
                    1.0,
                    (selected_expected_edge_median - float(robustness_edge_median_floor))
                    / max(0.01, float(robustness_edge_median_target) - float(robustness_edge_median_floor)),
                ),
            )
            thin_margin_penalty = 1.0 - median_quality
            edge_bonus = max(0.0, median_quality - 0.85)
        if (
            isinstance(selected_expected_edge_p10, float)
            and selected_expected_edge_median
            and selected_expected_edge_median > 0.0
        ):
            tail_quality = max(
                0.0,
                min(1.0, float(selected_expected_edge_p10) / float(selected_expected_edge_median)),
            )
            tail_fragility_penalty = 1.0 - tail_quality
            edge_bonus = max(edge_bonus, max(0.0, tail_quality - float(robustness_tail_ratio_floor)))
        edge_guardrail_fragility = max(
            0.0,
            min(1.0, (0.55 * thin_margin_penalty) + (0.45 * tail_fragility_penalty)),
        )
        edge_guardrail_penalty = edge_guardrail_fragility
        edge_guardrail_bonus = min(float(robustness_bonus_cap), 0.12 * edge_bonus)

    score_penalty = (
        float(robustness_conversion_weight) * conversion_penalty
        + float(robustness_concentration_weight) * concentration_penalty
        + float(robustness_edge_guardrail_weight) * edge_guardrail_penalty
        + float(weather_penalty_weighted)
        + float(execution_friction_penalty_weighted)
    )
    score_bonus = (
        min(float(robustness_bonus_cap), 0.12 * conversion_bonus)
        + concentration_bonus
        + edge_guardrail_bonus
    )
    score_multiplier = max(
        0.35,
        min(1.18, 1.0 - score_penalty + score_bonus),
    )

    return {
        "enabled": True,
        "calibration_available": bool(calibration_available),
        "score_multiplier": round(float(score_multiplier), 6),
        "score_penalty": round(float(score_penalty), 6),
        "score_bonus": round(float(score_bonus), 6),
        "conversion": {
            "available": bool(conversion_available),
            "selected_rows_with_conversion_data": int(selected_rows_with_conversion_data),
            "submission_ratio_mean": (
                round(float(candidate["selected_submission_ratio_mean"]), 6)
                if isinstance(candidate.get("selected_submission_ratio_mean"), (int, float))
                else None
            ),
            "fill_ratio_mean": (
                round(float(candidate["selected_fill_ratio_mean"]), 6)
                if isinstance(candidate.get("selected_fill_ratio_mean"), (int, float))
                else None
            ),
            "settlement_ratio_mean": (
                round(float(candidate["selected_settlement_ratio_mean"]), 6)
                if isinstance(candidate.get("selected_settlement_ratio_mean"), (int, float))
                else None
            ),
            "per_row_quality_mean": (
                round(float(conversion_quality), 6) if isinstance(conversion_quality, float) else None
            ),
            "per_row_quality_median": (
                round(float(candidate["selected_conversion_quality_median"]), 6)
                if isinstance(candidate.get("selected_conversion_quality_median"), (int, float))
                else None
            ),
            "per_row_quality_min": (
                round(float(candidate["selected_conversion_quality_min"]), 6)
                if isinstance(candidate.get("selected_conversion_quality_min"), (int, float))
                else None
            ),
            "quality_score": (
                round(float(conversion_quality), 6) if isinstance(conversion_quality, float) else None
            ),
            "penalty": round(float(conversion_penalty), 6),
            "bonus": round(float(conversion_bonus), 6),
        },
        "concentration": {
            "underlying_max_share": round(float(selected_underlying_max_share), 6),
            "station_max_share": round(float(selected_station_max_share), 6),
            "underlying_hhi": round(float(selected_underlying_hhi), 6),
            "station_hhi": round(float(selected_station_hhi), 6),
            "safety_limit_max_share": round(float(safety_limit_max_share), 6),
            "fragility_score": round(float(concentration_fragility), 6),
            "penalty": round(float(concentration_penalty), 6),
            "bonus": round(float(concentration_bonus), 6),
        },
        "edge_guardrail": {
            "selected_expected_edge_mean": round(float(selected_expected_edge_mean), 6),
            "selected_expected_edge_median": (
                round(float(selected_expected_edge_median), 6)
                if isinstance(selected_expected_edge_median, float)
                else None
            ),
            "selected_expected_edge_p10": (
                round(float(selected_expected_edge_p10), 6)
                if isinstance(selected_expected_edge_p10, float)
                else None
            ),
            "median_quality": round(float(median_quality), 6) if isinstance(median_quality, float) else None,
            "tail_quality": round(float(tail_quality), 6) if isinstance(tail_quality, float) else None,
            "fragility_score": round(float(edge_guardrail_fragility), 6),
            "penalty": round(float(edge_guardrail_penalty), 6),
            "bonus": round(float(edge_guardrail_bonus), 6),
        },
        "weather_risk": {
            "available": bool(weather_risk["available"]),
            "status": weather_risk["status"],
            "source_file": weather_risk["source_file"],
            "negative_expectancy_attempt_share": weather_risk["negative_expectancy_attempt_share"],
            "negative_expectancy_attempt_share_source": weather_risk["negative_expectancy_attempt_share_source"],
            "negative_expectancy_attempt_share_confidence_adjusted_observed": weather_risk[
                "negative_expectancy_attempt_share_confidence_adjusted_observed"
            ],
            "negative_expectancy_attempt_share_raw_observed": weather_risk[
                "negative_expectancy_attempt_share_raw_observed"
            ],
            "stale_metar_negative_attempt_share": weather_risk["stale_metar_negative_attempt_share"],
            "stale_metar_negative_attempt_share_source": weather_risk[
                "stale_metar_negative_attempt_share_source"
            ],
            "stale_metar_negative_attempt_share_confidence_adjusted_observed": weather_risk[
                "stale_metar_negative_attempt_share_confidence_adjusted_observed"
            ],
            "stale_metar_negative_attempt_share_raw_observed": weather_risk[
                "stale_metar_negative_attempt_share_raw_observed"
            ],
            "risk_off_recommended": bool(weather_risk["risk_off_recommended"]),
            "risk_off_score": weather_risk["risk_off_score"],
            "negative_expectancy_soft_limit": weather_risk["negative_expectancy_soft_limit"],
            "stale_metar_soft_limit": weather_risk["stale_metar_soft_limit"],
            "risk_off_hard_threshold": weather_risk["risk_off_hard_threshold"],
            "negative_expectancy_penalty": weather_risk["negative_expectancy_penalty"],
            "stale_metar_penalty": weather_risk["stale_metar_penalty"],
            "risk_off_penalty": weather_risk["risk_off_penalty"],
            "weighted_penalty": round(float(weather_penalty_weighted), 6),
            "hard_block_active": bool(weather_risk["hard_block_active"]),
        },
        "execution_friction": {
            "available": bool(execution_friction["available"]),
            "status": execution_friction["status"],
            "source_file": execution_friction["source_file"],
            "spread_median_dollars": execution_friction["spread_median_dollars"],
            "spread_p90_dollars": execution_friction["spread_p90_dollars"],
            "quote_two_sided_ratio": execution_friction["quote_two_sided_ratio"],
            "expected_edge_below_min_share": execution_friction["expected_edge_below_min_share"],
            "top_ticker_max_share": execution_friction["top_ticker_max_share"],
            "core_metric_count": execution_friction["core_metric_count"],
            "core_metric_total": execution_friction["core_metric_total"],
            "evidence_coverage": execution_friction["evidence_coverage"],
            "penalty": execution_friction["penalty"],
            "spread_median_penalty": execution_friction["spread_median_penalty"],
            "spread_p90_penalty": execution_friction["spread_p90_penalty"],
            "quote_coverage_penalty": execution_friction["quote_coverage_penalty"],
            "expected_edge_below_min_penalty": execution_friction["expected_edge_below_min_penalty"],
            "top_ticker_concentration_penalty": execution_friction["top_ticker_concentration_penalty"],
            "spread_quote_stress": execution_friction["spread_quote_stress"],
            "severe": bool(execution_friction["severe"]),
            "weighted_penalty": round(float(execution_friction_penalty_weighted), 6),
        },
    }


def _evaluate_candidate(
    rows: Sequence[LoadedTradeIntent],
    *,
    min_probability_confidence: float,
    min_expected_edge_net: float,
    min_edge_to_risk_ratio: float,
) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if row.probability_confidence >= min_probability_confidence
        and row.expected_edge_net >= min_expected_edge_net
        and row.edge_to_risk_ratio >= min_edge_to_risk_ratio
    ]
    selected_count = len(selected)
    total_count = len(rows)
    selected_edge_sum = float(sum(row.expected_edge_net for row in selected))
    selected_edge_values = [float(row.expected_edge_net) for row in selected]
    selected_edge_mean = (
        float(selected_edge_sum / selected_count) if selected_count > 0 else None
    )
    selected_edge_median = _percentile(selected_edge_values, 0.5)
    selected_edge_p10 = _percentile(selected_edge_values, 0.1)
    selected_edge_to_risk_mean = (
        float(sum(row.edge_to_risk_ratio for row in selected) / selected_count)
        if selected_count > 0
        else None
    )
    implied_entry_risk_values: list[float] = []
    high_entry_low_edge_count = 0
    low_edge_to_risk_quality_count = 0
    for row in selected:
        edge = float(row.expected_edge_net)
        edge_to_risk = float(row.edge_to_risk_ratio)
        if edge_to_risk <= _LOW_EDGE_TO_RISK_QUALITY_FLOOR:
            low_edge_to_risk_quality_count += 1
        if edge_to_risk > 0.0 and edge > 0.0:
            implied_entry_risk = float(edge / edge_to_risk)
            if math.isfinite(implied_entry_risk) and implied_entry_risk > 0.0:
                implied_entry_risk_values.append(implied_entry_risk)
                if (
                    edge_to_risk <= _HIGH_ENTRY_LOW_EDGE_EDGE_TO_RISK_CAP
                    and edge <= _HIGH_ENTRY_LOW_EDGE_EXPECTED_EDGE_MAX
                    and implied_entry_risk >= _HIGH_ENTRY_LOW_EDGE_IMPLIED_RISK_MIN
                ):
                    high_entry_low_edge_count += 1
    selected_implied_entry_risk_mean = _mean_or_none(implied_entry_risk_values)
    selected_implied_entry_risk_median = _percentile(implied_entry_risk_values, 0.5)
    selected_high_entry_low_edge_share = (
        float(high_entry_low_edge_count / selected_count) if selected_count > 0 else 0.0
    )
    selected_low_edge_to_risk_quality_share = (
        float(low_edge_to_risk_quality_count / selected_count) if selected_count > 0 else 0.0
    )
    selected_probability_confidence_mean = (
        float(sum(row.probability_confidence for row in selected) / selected_count)
        if selected_count > 0
        else None
    )
    selected_taf_missing_station_count = sum(
        1 for row in selected if _is_taf_missing_station_status(row.taf_status)
    )
    selected_taf_missing_station_share = (
        float(selected_taf_missing_station_count / selected_count) if selected_count > 0 else 0.0
    )
    conversion_ratios = _candidate_conversion_ratios(selected)
    selected_underlying = _concentration_stats([row.underlying_key for row in selected])
    selected_station = _concentration_stats([row.settlement_station for row in selected])
    selected_repeat_pressure = _concentration_stats(
        [f"{row.underlying_key}|{row.settlement_station}" for row in selected]
    )
    selected_repeat_pressure_top = selected_repeat_pressure["top"]
    selected_repeat_pressure_top_count = (
        int(selected_repeat_pressure_top[0]["count"])
        if selected_repeat_pressure_top
        else 0
    )
    selected_repeat_pressure_max_share = float(selected_repeat_pressure["max_share"])
    if selected_count <= 0:
        safety_limit = 0.0
    else:
        safety_limit = _concentration_limit(selected_count)
    blockers: list[str] = []
    if selected_count <= 0:
        blockers.append("selected_zero_intents")
    if selected_edge_sum <= 0.0:
        blockers.append("selected_expected_edge_sum_non_positive")
    if selected_edge_mean is None or selected_edge_mean <= 0.0:
        blockers.append("selected_expected_edge_mean_non_positive")
    if selected_edge_to_risk_mean is None or selected_edge_to_risk_mean <= 0.0:
        blockers.append("selected_edge_to_risk_mean_non_positive")
    if selected_count > 0 and float(selected_underlying["max_share"]) > float(safety_limit):
        blockers.append("underlying_concentration_limit_exceeded")
    if selected_count > 0 and float(selected_station["max_share"]) > float(safety_limit):
        blockers.append("station_concentration_limit_exceeded")
    support_count_gap = (
        max(0.0, float(3 - selected_count) / 3.0) if selected_count > 0 else 1.0
    )
    support_underlying_gap = (
        max(0.0, float(2 - int(selected_underlying["unique"])) / 2.0)
        if selected_count > 0
        else 1.0
    )
    support_station_gap = (
        max(0.0, float(2 - int(selected_station["unique"])) / 2.0)
        if selected_count > 0
        else 1.0
    )
    thin_sample_support_penalty = max(
        0.0,
        min(
            1.0,
            (0.55 * support_count_gap)
            + (0.25 * support_underlying_gap)
            + (0.20 * support_station_gap),
        ),
    )
    if selected_count == 1:
        blockers.append("thin_sample_support_severe")
    if (
        selected_count > 0
        and selected_high_entry_low_edge_share >= 0.85
        and isinstance(selected_edge_mean, float)
        and selected_edge_mean <= _HIGH_ENTRY_LOW_EDGE_EXPECTED_EDGE_MAX
    ):
        blockers.append("high_entry_low_edge_regime")
    if (
        selected_count > 0
        and selected_low_edge_to_risk_quality_share >= 0.90
        and isinstance(selected_edge_to_risk_mean, float)
        and selected_edge_to_risk_mean <= _LOW_EDGE_TO_RISK_QUALITY_FLOOR
    ):
        blockers.append("low_edge_to_risk_quality")
    if (
        selected_count >= 3
        and selected_repeat_pressure_max_share >= _REPEAT_PRESSURE_HARD_SHARE
        and isinstance(selected_edge_mean, float)
        and selected_edge_mean <= _REPEAT_PRESSURE_EDGE_MEAN_MAX
    ):
        blockers.append("repeat_pressure_concentration")
    if (
        selected_count >= _TAF_MISSING_STATION_HARD_MIN_ROWS
        and selected_taf_missing_station_share >= _TAF_MISSING_STATION_HARD_SHARE
    ):
        blockers.append("taf_missing_station_concentration")

    return {
        "min_probability_confidence": round(float(min_probability_confidence), 6),
        "min_expected_edge_net": round(float(min_expected_edge_net), 6),
        "min_edge_to_risk_ratio": round(float(min_edge_to_risk_ratio), 6),
        "intents_total": int(total_count),
        "intents_selected": int(selected_count),
        "selected_rate": round(float(selected_count / total_count), 6) if total_count > 0 else 0.0,
        "selected_expected_edge_sum": round(float(selected_edge_sum), 6),
        "selected_expected_edge_mean": (
            round(float(selected_edge_mean), 6) if selected_edge_mean is not None else None
        ),
        "selected_expected_edge_median": (
            round(float(selected_edge_median), 6) if selected_edge_median is not None else None
        ),
        "selected_expected_edge_p10": (
            round(float(selected_edge_p10), 6) if selected_edge_p10 is not None else None
        ),
        "selected_edge_to_risk_mean": (
            round(float(selected_edge_to_risk_mean), 6)
            if selected_edge_to_risk_mean is not None
            else None
        ),
        "selected_implied_entry_risk_mean": (
            round(float(selected_implied_entry_risk_mean), 6)
            if selected_implied_entry_risk_mean is not None
            else None
        ),
        "selected_implied_entry_risk_median": (
            round(float(selected_implied_entry_risk_median), 6)
            if selected_implied_entry_risk_median is not None
            else None
        ),
        "selected_high_entry_low_edge_count": int(high_entry_low_edge_count),
        "selected_high_entry_low_edge_share": round(float(selected_high_entry_low_edge_share), 6),
        "selected_low_edge_to_risk_quality_count": int(low_edge_to_risk_quality_count),
        "selected_low_edge_to_risk_quality_share": round(
            float(selected_low_edge_to_risk_quality_share),
            6,
        ),
        "selected_thin_sample_support_penalty": round(float(thin_sample_support_penalty), 6),
        "selected_probability_confidence_mean": (
            round(float(selected_probability_confidence_mean), 6)
            if selected_probability_confidence_mean is not None
            else None
        ),
        "selected_taf_missing_station_count": int(selected_taf_missing_station_count),
        "selected_taf_missing_station_share": round(float(selected_taf_missing_station_share), 6),
        "selected_underlying_count": int(selected_underlying["unique"]),
        "selected_station_count": int(selected_station["unique"]),
        "selected_underlying_max_share": float(selected_underlying["max_share"]),
        "selected_station_max_share": float(selected_station["max_share"]),
        "selected_underlying_hhi": float(selected_underlying["hhi"]),
        "selected_station_hhi": float(selected_station["hhi"]),
        "selected_underlying_top": selected_underlying["top"],
        "selected_station_top": selected_station["top"],
        "selected_repeat_pressure_top_count": int(selected_repeat_pressure_top_count),
        "selected_repeat_pressure_max_share": round(float(selected_repeat_pressure_max_share), 6),
        "selected_repeat_pressure_hhi": float(selected_repeat_pressure["hhi"]),
        "selected_repeat_pressure_top": selected_repeat_pressure_top,
        "selected_rows_with_conversion_data": int(conversion_ratios["selected_rows_with_conversion_data"]),
        "selected_submission_ratio_mean": (
            round(float(conversion_ratios["submission_ratio_mean"]), 6)
            if isinstance(conversion_ratios["submission_ratio_mean"], float)
            else None
        ),
        "selected_fill_ratio_mean": (
            round(float(conversion_ratios["fill_ratio_mean"]), 6)
            if isinstance(conversion_ratios["fill_ratio_mean"], float)
            else None
        ),
        "selected_settlement_ratio_mean": (
            round(float(conversion_ratios["settlement_ratio_mean"]), 6)
            if isinstance(conversion_ratios["settlement_ratio_mean"], float)
            else None
        ),
        "selected_conversion_quality_mean": (
            round(float(conversion_ratios["per_row_quality_mean"]), 6)
            if isinstance(conversion_ratios["per_row_quality_mean"], float)
            else None
        ),
        "selected_conversion_quality_median": (
            round(float(conversion_ratios["per_row_quality_median"]), 6)
            if isinstance(conversion_ratios["per_row_quality_median"], float)
            else None
        ),
        "selected_conversion_quality_min": (
            round(float(conversion_ratios["per_row_quality_min"]), 6)
            if isinstance(conversion_ratios["per_row_quality_min"], float)
            else None
        ),
        "safety_limit_max_share": round(float(safety_limit), 6),
        "blockers": blockers,
        "viable": len(blockers) == 0,
    }


def _score_candidate(
    candidate: dict[str, Any],
    *,
    max_selected_expected_edge_sum: float,
    max_selected_expected_edge_mean: float,
    max_selected_edge_to_risk_mean: float,
    max_selected_rate: float,
    calibration_available: bool,
    weather_pattern_artifact: dict[str, Any] | None,
    execution_cost_tape_artifact: dict[str, Any] | None,
    robustness_conversion_weight: float,
    robustness_concentration_weight: float,
    robustness_edge_guardrail_weight: float,
    robustness_weather_negative_expectancy_weight: float,
    robustness_weather_stale_metar_weight: float,
    robustness_weather_risk_off_weight: float,
    robustness_execution_friction_weight: float,
    robustness_bonus_cap: float,
    robustness_edge_median_floor: float,
    robustness_edge_median_target: float,
    robustness_tail_ratio_floor: float,
    weather_negative_expectancy_attempt_share_soft_limit: float,
    weather_stale_metar_negative_attempt_share_soft_limit: float,
    weather_risk_off_hard_threshold: float,
) -> dict[str, Any]:
    selected_count = int(candidate.get("intents_selected") or 0)
    if selected_count <= 0:
        base_score = 0.0
        score_components = {
            "quality_score": 0.0,
            "throughput_score": 0.0,
            "breadth_bonus": 0.0,
            "selected_expected_edge_sum_norm": 0.0,
            "selected_expected_edge_mean_norm": 0.0,
            "selected_edge_to_risk_mean_norm": 0.0,
            "selected_rate_norm": 0.0,
            "robustness_multiplier": 1.0,
            "siphon_penalty": 0.0,
            "siphon_multiplier": 1.0,
            "robustness_score_penalty": 0.0,
            "robustness_score_bonus": 0.0,
        }
    else:
        edge_sum_norm = (
            float(candidate["selected_expected_edge_sum"]) / float(max_selected_expected_edge_sum)
            if max_selected_expected_edge_sum > 0.0
            else 0.0
        )
        edge_mean = candidate.get("selected_expected_edge_mean")
        edge_mean_norm = (
            float(edge_mean) / float(max_selected_expected_edge_mean)
            if isinstance(edge_mean, (int, float)) and max_selected_expected_edge_mean > 0.0
            else 0.0
        )
        edge_to_risk_mean = candidate.get("selected_edge_to_risk_mean")
        edge_to_risk_norm = (
            float(edge_to_risk_mean) / float(max_selected_edge_to_risk_mean)
            if isinstance(edge_to_risk_mean, (int, float)) and max_selected_edge_to_risk_mean > 0.0
            else 0.0
        )
        selected_rate = float(candidate.get("selected_rate") or 0.0)
        selected_rate_norm = (
            selected_rate / float(max_selected_rate)
            if max_selected_rate > 0.0
            else 0.0
        )
        quality_score = (
            0.45 * edge_mean_norm
            + 0.35 * edge_sum_norm
            + 0.20 * edge_to_risk_norm
        )
        breadth_bonus = max(
            0.25,
            1.0 - (0.35 * float(candidate["selected_underlying_max_share"]))
            - (0.35 * float(candidate["selected_station_max_share"])),
        )
        throughput_score = selected_rate_norm
        base_score = quality_score * (0.65 + 0.35 * throughput_score) * breadth_bonus
        score_components = {
            "quality_score": round(float(quality_score), 6),
            "throughput_score": round(float(throughput_score), 6),
            "breadth_bonus": round(float(breadth_bonus), 6),
            "selected_expected_edge_sum_norm": round(float(edge_sum_norm), 6),
            "selected_expected_edge_mean_norm": round(float(edge_mean_norm), 6),
            "selected_edge_to_risk_mean_norm": round(float(edge_to_risk_norm), 6),
            "selected_rate_norm": round(float(selected_rate_norm), 6),
            "robustness_multiplier": 1.0,
            "siphon_penalty": 0.0,
            "siphon_multiplier": 1.0,
            "robustness_score_penalty": 0.0,
            "robustness_score_bonus": 0.0,
        }
    siphon_high_entry_low_edge_share = float(candidate.get("selected_high_entry_low_edge_share") or 0.0)
    siphon_low_edge_to_risk_quality_share = float(
        candidate.get("selected_low_edge_to_risk_quality_share") or 0.0
    )
    siphon_thin_sample_support_penalty = float(candidate.get("selected_thin_sample_support_penalty") or 0.0)
    siphon_repeat_pressure_share = float(candidate.get("selected_repeat_pressure_max_share") or 0.0)
    siphon_repeat_pressure_penalty = max(
        0.0,
        min(
            1.0,
            (siphon_repeat_pressure_share - _REPEAT_PRESSURE_SOFT_SHARE)
            / max(0.01, 1.0 - _REPEAT_PRESSURE_SOFT_SHARE),
        ),
    )
    siphon_taf_missing_station_share = float(candidate.get("selected_taf_missing_station_share") or 0.0)
    siphon_taf_missing_station_penalty = max(
        0.0,
        min(
            1.0,
            (siphon_taf_missing_station_share - _TAF_MISSING_STATION_SOFT_SHARE)
            / max(0.01, 1.0 - _TAF_MISSING_STATION_SOFT_SHARE),
        ),
    )
    siphon_penalty = max(
        0.0,
        min(
            0.90,
            (0.50 * siphon_high_entry_low_edge_share)
            + (0.20 * siphon_low_edge_to_risk_quality_share)
            + (0.18 * siphon_thin_sample_support_penalty)
            + (0.12 * siphon_repeat_pressure_penalty)
            + (0.08 * siphon_taf_missing_station_penalty),
        ),
    )
    siphon_multiplier = max(0.45, 1.0 - siphon_penalty)
    robustness = _robustness_metrics(
        candidate,
        calibration_available=bool(calibration_available),
        weather_pattern_artifact=weather_pattern_artifact,
        execution_cost_tape_artifact=execution_cost_tape_artifact,
        robustness_conversion_weight=robustness_conversion_weight,
        robustness_concentration_weight=robustness_concentration_weight,
        robustness_edge_guardrail_weight=robustness_edge_guardrail_weight,
        robustness_weather_negative_expectancy_weight=robustness_weather_negative_expectancy_weight,
        robustness_weather_stale_metar_weight=robustness_weather_stale_metar_weight,
        robustness_weather_risk_off_weight=robustness_weather_risk_off_weight,
        robustness_execution_friction_weight=robustness_execution_friction_weight,
        robustness_bonus_cap=robustness_bonus_cap,
        robustness_edge_median_floor=robustness_edge_median_floor,
        robustness_edge_median_target=robustness_edge_median_target,
        robustness_tail_ratio_floor=robustness_tail_ratio_floor,
        weather_negative_expectancy_attempt_share_soft_limit=weather_negative_expectancy_attempt_share_soft_limit,
        weather_stale_metar_negative_attempt_share_soft_limit=weather_stale_metar_negative_attempt_share_soft_limit,
        weather_risk_off_hard_threshold=weather_risk_off_hard_threshold,
    )
    if bool(robustness.get("weather_risk", {}).get("hard_block_active")):
        blockers = candidate.setdefault("blockers", [])
        if "weather_risk_off_recommended" not in blockers:
            blockers.append("weather_risk_off_recommended")
        candidate["viable"] = False
    execution_friction_penalty = float(robustness.get("execution_friction", {}).get("penalty") or 0.0)
    execution_friction_evidence_coverage = float(robustness.get("execution_friction", {}).get("evidence_coverage") or 0.0)
    execution_friction_severe = bool(robustness.get("execution_friction", {}).get("severe"))
    weather_elevated = bool(
        float(robustness.get("weather_risk", {}).get("weighted_penalty") or 0.0)
        >= _EXECUTION_FRICTION_HARD_BLOCK_WEATHER_ELEVATED_THRESHOLD
        or bool(robustness.get("weather_risk", {}).get("risk_off_recommended"))
    )
    if (
        bool(robustness.get("execution_friction", {}).get("available"))
        and execution_friction_severe
        and execution_friction_penalty >= _EXECUTION_FRICTION_HARD_BLOCK_SEVERE_THRESHOLD
        and execution_friction_evidence_coverage >= _EXECUTION_FRICTION_HARD_BLOCK_MIN_EVIDENCE_COVERAGE
        and weather_elevated
    ):
        blockers = candidate.setdefault("blockers", [])
        if "execution_friction_weather_elevated" not in blockers:
            blockers.append("execution_friction_weather_elevated")
        candidate["viable"] = False
    candidate_score = (
        float(base_score)
        * float(siphon_multiplier)
        * float(robustness.get("score_multiplier") or 1.0)
    )
    if float(siphon_thin_sample_support_penalty) >= 0.55:
        blockers = candidate.setdefault("blockers", [])
        if "thin_sample_support_severe" not in blockers:
            blockers.append("thin_sample_support_severe")
        candidate["viable"] = False
    score_components["robustness_multiplier"] = round(float(robustness.get("score_multiplier") or 1.0), 6)
    score_components["siphon_penalty"] = round(float(siphon_penalty), 6)
    score_components["siphon_multiplier"] = round(float(siphon_multiplier), 6)
    score_components["siphon_repeat_pressure_penalty"] = round(float(siphon_repeat_pressure_penalty), 6)
    score_components["siphon_taf_missing_station_penalty"] = round(
        float(siphon_taf_missing_station_penalty),
        6,
    )
    score_components["robustness_score_penalty"] = round(float(robustness.get("score_penalty") or 0.0), 6)
    score_components["robustness_score_bonus"] = round(float(robustness.get("score_bonus") or 0.0), 6)
    score_components["robustness_weather_penalty"] = round(
        float(robustness.get("weather_risk", {}).get("weighted_penalty") or 0.0),
        6,
    )
    score_components["robustness_execution_friction_penalty"] = round(
        float(robustness.get("execution_friction", {}).get("weighted_penalty") or 0.0),
        6,
    )
    candidate["score"] = round(float(candidate_score), 6)
    candidate["score_components"] = score_components
    candidate["siphon_markers"] = {
        "high_entry_low_edge_share": round(float(siphon_high_entry_low_edge_share), 6),
        "low_edge_to_risk_quality_share": round(float(siphon_low_edge_to_risk_quality_share), 6),
        "thin_sample_support_penalty": round(float(siphon_thin_sample_support_penalty), 6),
        "repeat_pressure_share": round(float(siphon_repeat_pressure_share), 6),
        "repeat_pressure_penalty": round(float(siphon_repeat_pressure_penalty), 6),
        "taf_missing_station_share": round(float(siphon_taf_missing_station_share), 6),
        "taf_missing_station_penalty": round(float(siphon_taf_missing_station_penalty), 6),
        "score_penalty": round(float(siphon_penalty), 6),
        "score_multiplier": round(float(siphon_multiplier), 6),
    }
    candidate["robustness"] = robustness
    candidate["score_key"] = (
        float(candidate["score"]),
        float(candidate.get("selected_expected_edge_sum") or 0.0),
        float(candidate.get("selected_expected_edge_mean") or 0.0),
        float(candidate.get("selected_rate") or 0.0),
        -float(candidate.get("selected_underlying_max_share") or 0.0),
        -float(candidate.get("selected_station_max_share") or 0.0),
        float(candidate.get("min_probability_confidence") or 0.0),
        float(candidate.get("min_expected_edge_net") or 0.0),
        float(candidate.get("min_edge_to_risk_ratio") or 0.0),
    )
    return candidate


def _summarize_blockers(candidates: Sequence[dict[str, Any]], *, top_n: int = 5) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    examples: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        for blocker in candidate.get("blockers", []):
            blocker_text = _normalize_text(blocker)
            if not blocker_text:
                continue
            counts[blocker_text] += 1
            if blocker_text not in examples:
                examples[blocker_text] = {
                    "min_probability_confidence": candidate.get("min_probability_confidence"),
                    "min_expected_edge_net": candidate.get("min_expected_edge_net"),
                    "min_edge_to_risk_ratio": candidate.get("min_edge_to_risk_ratio"),
                    "intents_selected": candidate.get("intents_selected"),
                    "selected_expected_edge_sum": candidate.get("selected_expected_edge_sum"),
                    "selected_rate": candidate.get("selected_rate"),
                    "selected_underlying_max_share": candidate.get("selected_underlying_max_share"),
                    "selected_station_max_share": candidate.get("selected_station_max_share"),
                }
    ranked = []
    for reason, count in counts.most_common(top_n):
        ranked.append({"reason": reason, "count": int(count), "example": examples.get(reason, {})})
    return ranked


def run_kalshi_temperature_growth_optimizer(
    *,
    input_paths: Sequence[str | Path],
    top_n: int = 10,
    threshold_candidate_limit: int = _DEFAULT_THRESHOLD_CANDIDATE_LIMIT,
    robustness_conversion_weight: float = 0.35,
    robustness_concentration_weight: float = 0.40,
    robustness_edge_guardrail_weight: float = 0.25,
    robustness_weather_negative_expectancy_weight: float = 0.20,
    robustness_weather_stale_metar_weight: float = 0.10,
    robustness_weather_risk_off_weight: float = 0.25,
    robustness_execution_friction_weight: float = 0.30,
    robustness_bonus_cap: float = 0.12,
    robustness_edge_median_floor: float = 0.02,
    robustness_edge_median_target: float = 0.08,
    robustness_tail_ratio_floor: float = 0.70,
    weather_negative_expectancy_attempt_share_soft_limit: float = 0.35,
    weather_stale_metar_negative_attempt_share_soft_limit: float = 0.20,
    weather_risk_off_hard_threshold: float = 0.80,
) -> dict[str, Any]:
    rows, load_summary = _load_trade_intents(input_paths)
    weather_pattern_artifact = _load_weather_pattern_artifact(input_paths)
    execution_cost_tape_artifact = _load_execution_cost_tape_artifact(input_paths)
    load_summary["weather_pattern_artifact"] = {
        "available": bool(weather_pattern_artifact.get("available")),
        "status": weather_pattern_artifact.get("status"),
        "source_file": weather_pattern_artifact.get("source_file"),
        "files_discovered": int(weather_pattern_artifact.get("files_discovered") or 0),
        "negative_expectancy_attempt_share": weather_pattern_artifact.get("negative_expectancy_attempt_share"),
        "negative_expectancy_attempt_share_source": weather_pattern_artifact.get(
            "negative_expectancy_attempt_share_source"
        ),
        "negative_expectancy_attempt_share_confidence_adjusted_observed": weather_pattern_artifact.get(
            "negative_expectancy_attempt_share_confidence_adjusted_observed"
        ),
        "negative_expectancy_attempt_share_raw_observed": weather_pattern_artifact.get(
            "negative_expectancy_attempt_share_raw_observed"
        ),
        "stale_metar_negative_attempt_share": weather_pattern_artifact.get("stale_metar_negative_attempt_share"),
        "stale_metar_negative_attempt_share_source": weather_pattern_artifact.get(
            "stale_metar_negative_attempt_share_source"
        ),
        "stale_metar_negative_attempt_share_confidence_adjusted_observed": weather_pattern_artifact.get(
            "stale_metar_negative_attempt_share_confidence_adjusted_observed"
        ),
        "stale_metar_negative_attempt_share_raw_observed": weather_pattern_artifact.get(
            "stale_metar_negative_attempt_share_raw_observed"
        ),
        "risk_off_recommended": bool(weather_pattern_artifact.get("risk_off_recommended")),
        "risk_off_score": weather_pattern_artifact.get("risk_off_score"),
    }
    load_summary["execution_cost_tape_artifact"] = {
        "available": bool(execution_cost_tape_artifact.get("available")),
        "status": execution_cost_tape_artifact.get("status"),
        "source_file": execution_cost_tape_artifact.get("source_file"),
        "files_discovered": int(execution_cost_tape_artifact.get("files_discovered") or 0),
        "spread_median_dollars": execution_cost_tape_artifact.get("spread_median_dollars"),
        "spread_p90_dollars": execution_cost_tape_artifact.get("spread_p90_dollars"),
        "quote_two_sided_ratio": execution_cost_tape_artifact.get("quote_two_sided_ratio"),
        "expected_edge_below_min_share": execution_cost_tape_artifact.get("expected_edge_below_min_share"),
        "top_ticker_max_share": execution_cost_tape_artifact.get("top_ticker_max_share"),
        "core_metric_count": int(execution_cost_tape_artifact.get("core_metric_count") or 0),
        "core_metric_total": int(execution_cost_tape_artifact.get("core_metric_total") or 4),
        "evidence_coverage": execution_cost_tape_artifact.get("evidence_coverage"),
        "penalty": execution_cost_tape_artifact.get("penalty"),
    }
    safe_top_n = max(1, int(top_n))
    calibration_available = bool(
        any(
            row.prelive_submission_ratio is not None
            or row.prelive_fill_ratio is not None
            or row.prelive_settlement_ratio is not None
            for row in rows
        )
    )
    if not rows:
        blockers = load_summary.get("load_errors") or [{"reason": "no_valid_trade_intents_loaded"}]
        empty_robustness = {
            "calibration_available": False,
            "candidate_count_with_robustness": 0,
            "score_multiplier_min": 1.0,
            "score_multiplier_avg": 1.0,
            "score_multiplier_max": 1.0,
            "calibration_knobs": {
                "conversion_weight": round(float(max(0.0, robustness_conversion_weight)), 6),
                "concentration_weight": round(float(max(0.0, robustness_concentration_weight)), 6),
                "edge_guardrail_weight": round(float(max(0.0, robustness_edge_guardrail_weight)), 6),
                "weather_negative_expectancy_weight": round(
                    float(max(0.0, robustness_weather_negative_expectancy_weight)),
                    6,
                ),
                "weather_stale_metar_weight": round(float(max(0.0, robustness_weather_stale_metar_weight)), 6),
                "weather_risk_off_weight": round(float(max(0.0, robustness_weather_risk_off_weight)), 6),
                "execution_friction_weight": round(float(max(0.0, robustness_execution_friction_weight)), 6),
                "bonus_cap": round(float(max(0.0, robustness_bonus_cap)), 6),
                "edge_median_floor": round(float(max(0.0, robustness_edge_median_floor)), 6),
                "edge_median_target": round(float(max(0.0, robustness_edge_median_target)), 6),
                "tail_ratio_floor": round(float(max(0.0, robustness_tail_ratio_floor)), 6),
                "weather_negative_expectancy_attempt_share_soft_limit": round(
                    float(max(0.0, weather_negative_expectancy_attempt_share_soft_limit)),
                    6,
                ),
                "weather_stale_metar_negative_attempt_share_soft_limit": round(
                    float(max(0.0, weather_stale_metar_negative_attempt_share_soft_limit)),
                    6,
                ),
                "weather_risk_off_hard_threshold": round(float(max(0.0, weather_risk_off_hard_threshold)), 6),
            },
            "weather_risk": {
                "available": bool(weather_pattern_artifact.get("available")),
                "status": weather_pattern_artifact.get("status"),
                "source_file": weather_pattern_artifact.get("source_file"),
                "negative_expectancy_attempt_share": weather_pattern_artifact.get("negative_expectancy_attempt_share"),
                "negative_expectancy_attempt_share_source": weather_pattern_artifact.get(
                    "negative_expectancy_attempt_share_source"
                ),
                "negative_expectancy_attempt_share_confidence_adjusted_observed": weather_pattern_artifact.get(
                    "negative_expectancy_attempt_share_confidence_adjusted_observed"
                ),
                "negative_expectancy_attempt_share_raw_observed": weather_pattern_artifact.get(
                    "negative_expectancy_attempt_share_raw_observed"
                ),
                "stale_metar_negative_attempt_share": weather_pattern_artifact.get("stale_metar_negative_attempt_share"),
                "stale_metar_negative_attempt_share_source": weather_pattern_artifact.get(
                    "stale_metar_negative_attempt_share_source"
                ),
                "stale_metar_negative_attempt_share_confidence_adjusted_observed": weather_pattern_artifact.get(
                    "stale_metar_negative_attempt_share_confidence_adjusted_observed"
                ),
                "stale_metar_negative_attempt_share_raw_observed": weather_pattern_artifact.get(
                    "stale_metar_negative_attempt_share_raw_observed"
                ),
                "risk_off_recommended": bool(weather_pattern_artifact.get("risk_off_recommended")),
                "risk_off_score": weather_pattern_artifact.get("risk_off_score"),
                "candidate_weighted_penalty_min": 0.0,
                "candidate_weighted_penalty_avg": 0.0,
                "candidate_weighted_penalty_max": 0.0,
                "hard_block_candidate_count": 0,
            },
            "execution_friction": {
                "available": bool(execution_cost_tape_artifact.get("available")),
                "status": execution_cost_tape_artifact.get("status"),
                "source_file": execution_cost_tape_artifact.get("source_file"),
                "spread_median_dollars": execution_cost_tape_artifact.get("spread_median_dollars"),
                "spread_p90_dollars": execution_cost_tape_artifact.get("spread_p90_dollars"),
                "quote_two_sided_ratio": execution_cost_tape_artifact.get("quote_two_sided_ratio"),
                "expected_edge_below_min_share": execution_cost_tape_artifact.get("expected_edge_below_min_share"),
                "top_ticker_max_share": execution_cost_tape_artifact.get("top_ticker_max_share"),
                "core_metric_count": int(execution_cost_tape_artifact.get("core_metric_count") or 0),
                "core_metric_total": int(execution_cost_tape_artifact.get("core_metric_total") or 4),
                "evidence_coverage": execution_cost_tape_artifact.get("evidence_coverage"),
                "penalty": execution_cost_tape_artifact.get("penalty"),
                "candidate_weighted_penalty_min": 0.0,
                "candidate_weighted_penalty_avg": 0.0,
                "candidate_weighted_penalty_max": 0.0,
                "hard_block_candidate_count": 0,
            },
            "recommended_configuration": None,
        }
        return {
            "status": "no_viable_config",
            "captured_at": None,
            "inputs": load_summary,
            "search": {
                "candidate_configurations_considered": 0,
                "threshold_candidates": {
                    "min_probability_confidence": [],
                    "min_expected_edge_net": [],
                    "min_edge_to_risk_ratio": [],
                },
                "threshold_candidate_limit": int(max(1, int(threshold_candidate_limit))),
                "robustness": empty_robustness,
            },
            "robustness": empty_robustness,
            "top_candidates": [],
            "recommended_configuration": None,
            "blockers": blockers,
        }

    probability_candidates = _candidate_thresholds(
        [row.probability_confidence for row in rows],
        limit=threshold_candidate_limit,
    )
    expected_edge_candidates = _candidate_thresholds(
        [row.expected_edge_net for row in rows],
        limit=threshold_candidate_limit,
    )
    edge_to_risk_candidates = _candidate_thresholds(
        [row.edge_to_risk_ratio for row in rows],
        limit=threshold_candidate_limit,
    )

    candidate_results: list[dict[str, Any]] = []
    for min_probability_confidence, min_expected_edge_net, min_edge_to_risk_ratio in itertools.product(
        probability_candidates,
        expected_edge_candidates,
        edge_to_risk_candidates,
    ):
        candidate_results.append(
            _evaluate_candidate(
                rows,
                min_probability_confidence=float(min_probability_confidence),
                min_expected_edge_net=float(min_expected_edge_net),
                min_edge_to_risk_ratio=float(min_edge_to_risk_ratio),
            )
        )

    selected_count_candidates = [
        candidate for candidate in candidate_results if int(candidate.get("intents_selected") or 0) > 0
    ]
    max_selected_expected_edge_sum = max(
        (float(candidate.get("selected_expected_edge_sum") or 0.0) for candidate in selected_count_candidates),
        default=0.0,
    )
    max_selected_expected_edge_mean = max(
        (
            float(candidate.get("selected_expected_edge_mean") or 0.0)
            for candidate in selected_count_candidates
            if candidate.get("selected_expected_edge_mean") is not None
        ),
        default=0.0,
    )
    max_selected_edge_to_risk_mean = max(
        (
            float(candidate.get("selected_edge_to_risk_mean") or 0.0)
            for candidate in selected_count_candidates
            if candidate.get("selected_edge_to_risk_mean") is not None
        ),
        default=0.0,
    )
    max_selected_rate = max(
        (float(candidate.get("selected_rate") or 0.0) for candidate in selected_count_candidates),
        default=0.0,
    )

    scored_candidates = [
        _score_candidate(
            candidate,
            max_selected_expected_edge_sum=max_selected_expected_edge_sum,
            max_selected_expected_edge_mean=max_selected_expected_edge_mean,
            max_selected_edge_to_risk_mean=max_selected_edge_to_risk_mean,
            max_selected_rate=max_selected_rate,
            calibration_available=calibration_available,
            weather_pattern_artifact=weather_pattern_artifact,
            execution_cost_tape_artifact=execution_cost_tape_artifact,
            robustness_conversion_weight=max(0.0, float(robustness_conversion_weight)),
            robustness_concentration_weight=max(0.0, float(robustness_concentration_weight)),
            robustness_edge_guardrail_weight=max(0.0, float(robustness_edge_guardrail_weight)),
            robustness_weather_negative_expectancy_weight=max(
                0.0, float(robustness_weather_negative_expectancy_weight)
            ),
            robustness_weather_stale_metar_weight=max(0.0, float(robustness_weather_stale_metar_weight)),
            robustness_weather_risk_off_weight=max(0.0, float(robustness_weather_risk_off_weight)),
            robustness_execution_friction_weight=max(0.0, float(robustness_execution_friction_weight)),
            robustness_bonus_cap=max(0.0, float(robustness_bonus_cap)),
            robustness_edge_median_floor=max(0.0, float(robustness_edge_median_floor)),
            robustness_edge_median_target=max(
                max(0.0, float(robustness_edge_median_floor)) + 0.001,
                float(robustness_edge_median_target),
            ),
            robustness_tail_ratio_floor=max(0.0, float(robustness_tail_ratio_floor)),
            weather_negative_expectancy_attempt_share_soft_limit=max(
                0.0, float(weather_negative_expectancy_attempt_share_soft_limit)
            ),
            weather_stale_metar_negative_attempt_share_soft_limit=max(
                0.0, float(weather_stale_metar_negative_attempt_share_soft_limit)
            ),
            weather_risk_off_hard_threshold=max(0.0, float(weather_risk_off_hard_threshold)),
        )
        for candidate in candidate_results
    ]
    scored_candidates.sort(key=lambda item: item["score_key"], reverse=True)

    top_candidates = []
    for rank, candidate in enumerate(scored_candidates[:safe_top_n], start=1):
        entry = dict(candidate)
        entry.pop("score_key", None)
        entry["rank"] = rank
        top_candidates.append(entry)

    viable_candidates = [candidate for candidate in scored_candidates if bool(candidate.get("viable"))]
    recommended_configuration = None
    if viable_candidates:
        recommended_configuration = dict(viable_candidates[0])
        recommended_configuration.pop("score_key", None)
        recommended_configuration["rank"] = 1

    blockers: list[dict[str, Any]]
    if recommended_configuration is None:
        blockers = _summarize_blockers(scored_candidates)
        if not blockers:
            blockers = [{"reason": "no_viable_config", "count": len(scored_candidates), "example": {}}]
    else:
        blockers = []

    candidate_multiplier_values = [
        float(candidate.get("robustness", {}).get("score_multiplier") or 1.0)
        for candidate in scored_candidates
    ]
    candidate_with_robustness = [
        candidate for candidate in scored_candidates if bool(candidate.get("robustness", {}).get("enabled"))
    ]
    candidate_weather_penalty_values = [
        float(candidate.get("robustness", {}).get("weather_risk", {}).get("weighted_penalty") or 0.0)
        for candidate in scored_candidates
    ]
    candidate_execution_friction_penalty_values = [
        float(candidate.get("robustness", {}).get("execution_friction", {}).get("weighted_penalty") or 0.0)
        for candidate in scored_candidates
    ]
    weather_hard_block_candidate_count = sum(
        1
        for candidate in scored_candidates
        if bool(candidate.get("robustness", {}).get("weather_risk", {}).get("hard_block_active"))
    )
    execution_friction_hard_block_candidate_count = sum(
        1
        for candidate in scored_candidates
        if "execution_friction_weather_elevated" in (candidate.get("blockers") or [])
    )
    recommended_robustness = (
        recommended_configuration.get("robustness") if isinstance(recommended_configuration, dict) else None
    )
    search_robustness = {
        "calibration_available": bool(calibration_available),
        "candidate_count_with_robustness": int(len(candidate_with_robustness)),
        "score_multiplier_min": round(
            float(min(candidate_multiplier_values) if candidate_multiplier_values else 1.0),
            6,
        ),
        "score_multiplier_avg": round(
            float(sum(candidate_multiplier_values) / len(candidate_multiplier_values))
            if candidate_multiplier_values
            else 1.0,
            6,
        ),
        "score_multiplier_max": round(
            float(max(candidate_multiplier_values) if candidate_multiplier_values else 1.0),
            6,
        ),
        "calibration_knobs": {
            "conversion_weight": round(float(max(0.0, robustness_conversion_weight)), 6),
            "concentration_weight": round(float(max(0.0, robustness_concentration_weight)), 6),
            "edge_guardrail_weight": round(float(max(0.0, robustness_edge_guardrail_weight)), 6),
            "weather_negative_expectancy_weight": round(
                float(max(0.0, robustness_weather_negative_expectancy_weight)),
                6,
            ),
            "weather_stale_metar_weight": round(float(max(0.0, robustness_weather_stale_metar_weight)), 6),
            "weather_risk_off_weight": round(float(max(0.0, robustness_weather_risk_off_weight)), 6),
            "execution_friction_weight": round(float(max(0.0, robustness_execution_friction_weight)), 6),
            "bonus_cap": round(float(max(0.0, robustness_bonus_cap)), 6),
            "edge_median_floor": round(float(max(0.0, robustness_edge_median_floor)), 6),
            "edge_median_target": round(
                float(max(max(0.0, float(robustness_edge_median_floor)) + 0.001, float(robustness_edge_median_target))),
                6,
            ),
            "tail_ratio_floor": round(float(max(0.0, robustness_tail_ratio_floor)), 6),
            "weather_negative_expectancy_attempt_share_soft_limit": round(
                float(max(0.0, weather_negative_expectancy_attempt_share_soft_limit)),
                6,
            ),
            "weather_stale_metar_negative_attempt_share_soft_limit": round(
                float(max(0.0, weather_stale_metar_negative_attempt_share_soft_limit)),
                6,
            ),
            "weather_risk_off_hard_threshold": round(float(max(0.0, weather_risk_off_hard_threshold)), 6),
        },
        "weather_risk": {
            "available": bool(weather_pattern_artifact.get("available")),
            "status": weather_pattern_artifact.get("status"),
            "source_file": weather_pattern_artifact.get("source_file"),
            "negative_expectancy_attempt_share": weather_pattern_artifact.get("negative_expectancy_attempt_share"),
            "negative_expectancy_attempt_share_source": weather_pattern_artifact.get(
                "negative_expectancy_attempt_share_source"
            ),
            "negative_expectancy_attempt_share_confidence_adjusted_observed": weather_pattern_artifact.get(
                "negative_expectancy_attempt_share_confidence_adjusted_observed"
            ),
            "negative_expectancy_attempt_share_raw_observed": weather_pattern_artifact.get(
                "negative_expectancy_attempt_share_raw_observed"
            ),
            "stale_metar_negative_attempt_share": weather_pattern_artifact.get("stale_metar_negative_attempt_share"),
            "stale_metar_negative_attempt_share_source": weather_pattern_artifact.get(
                "stale_metar_negative_attempt_share_source"
            ),
            "stale_metar_negative_attempt_share_confidence_adjusted_observed": weather_pattern_artifact.get(
                "stale_metar_negative_attempt_share_confidence_adjusted_observed"
            ),
            "stale_metar_negative_attempt_share_raw_observed": weather_pattern_artifact.get(
                "stale_metar_negative_attempt_share_raw_observed"
            ),
            "risk_off_recommended": bool(weather_pattern_artifact.get("risk_off_recommended")),
            "risk_off_score": weather_pattern_artifact.get("risk_off_score"),
            "candidate_weighted_penalty_min": round(
                float(min(candidate_weather_penalty_values) if candidate_weather_penalty_values else 0.0),
                6,
            ),
            "candidate_weighted_penalty_avg": round(
                float(sum(candidate_weather_penalty_values) / len(candidate_weather_penalty_values))
                if candidate_weather_penalty_values
                else 0.0,
                6,
            ),
            "candidate_weighted_penalty_max": round(
                float(max(candidate_weather_penalty_values) if candidate_weather_penalty_values else 0.0),
                6,
            ),
            "hard_block_candidate_count": int(weather_hard_block_candidate_count),
        },
        "execution_friction": {
            "available": bool(execution_cost_tape_artifact.get("available")),
            "status": execution_cost_tape_artifact.get("status"),
            "source_file": execution_cost_tape_artifact.get("source_file"),
            "spread_median_dollars": execution_cost_tape_artifact.get("spread_median_dollars"),
            "spread_p90_dollars": execution_cost_tape_artifact.get("spread_p90_dollars"),
            "quote_two_sided_ratio": execution_cost_tape_artifact.get("quote_two_sided_ratio"),
            "expected_edge_below_min_share": execution_cost_tape_artifact.get("expected_edge_below_min_share"),
            "top_ticker_max_share": execution_cost_tape_artifact.get("top_ticker_max_share"),
            "core_metric_count": int(execution_cost_tape_artifact.get("core_metric_count") or 0),
            "core_metric_total": int(execution_cost_tape_artifact.get("core_metric_total") or 4),
            "evidence_coverage": execution_cost_tape_artifact.get("evidence_coverage"),
            "penalty": execution_cost_tape_artifact.get("penalty"),
            "candidate_weighted_penalty_min": round(
                float(min(candidate_execution_friction_penalty_values) if candidate_execution_friction_penalty_values else 0.0),
                6,
            ),
            "candidate_weighted_penalty_avg": round(
                float(sum(candidate_execution_friction_penalty_values) / len(candidate_execution_friction_penalty_values))
                if candidate_execution_friction_penalty_values
                else 0.0,
                6,
            ),
            "candidate_weighted_penalty_max": round(
                float(max(candidate_execution_friction_penalty_values) if candidate_execution_friction_penalty_values else 0.0),
                6,
            ),
            "hard_block_candidate_count": int(execution_friction_hard_block_candidate_count),
        },
        "recommended_configuration": recommended_robustness,
    }

    return {
        "status": "ready" if recommended_configuration is not None else "no_viable_config",
        "captured_at": None,
        "inputs": load_summary,
        "search": {
            "candidate_configurations_considered": len(scored_candidates),
            "threshold_candidate_limit": int(max(1, int(threshold_candidate_limit))),
            "threshold_candidates": {
                "min_probability_confidence": probability_candidates,
                "min_expected_edge_net": expected_edge_candidates,
                "min_edge_to_risk_ratio": edge_to_risk_candidates,
            },
            "max_selected_expected_edge_sum": round(float(max_selected_expected_edge_sum), 6),
            "max_selected_expected_edge_mean": round(float(max_selected_expected_edge_mean), 6),
            "max_selected_edge_to_risk_mean": round(float(max_selected_edge_to_risk_mean), 6),
            "max_selected_rate": round(float(max_selected_rate), 6),
            "viable_candidate_count": len(viable_candidates),
            "robustness": search_robustness,
        },
        "robustness": search_robustness,
        "top_candidates": top_candidates,
        "recommended_configuration": recommended_configuration,
        "blockers": blockers,
    }


def summarize_kalshi_temperature_growth_optimizer(
    *,
    input_paths: Sequence[str | Path],
    top_n: int = 10,
    threshold_candidate_limit: int = _DEFAULT_THRESHOLD_CANDIDATE_LIMIT,
) -> str:
    payload = run_kalshi_temperature_growth_optimizer(
        input_paths=input_paths,
        top_n=top_n,
        threshold_candidate_limit=threshold_candidate_limit,
    )
    return json.dumps(payload, indent=2, sort_keys=True)
