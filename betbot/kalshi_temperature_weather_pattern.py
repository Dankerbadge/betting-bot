from __future__ import annotations

import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo


_INTENT_PATTERN = "kalshi_temperature_trade_intents_*.csv"
_PROFITABILITY_SUMMARY_PATTERN = "kalshi_temperature_profitability_summary_*.json"
_PROFITABILITY_CHECKPOINT_PATTERN = "checkpoints/profitability_*.json"
_SETTLED_CSV_PATTERN = "kalshi_temperature_profitability_settled_*.csv"
_HEALTH_DIR_NAME = "health"
_STAMP_SUFFIX_RE = re.compile(r"(\d{8}_\d{6})")
_NEGATIVE_EXPECTANCY_THRESHOLD = -0.05
_STALE_METAR_BUCKETS = frozenset({"61-120m", "121-240m", "240m+"})
_WILSON_LOWER_BOUND_Z = 1.2815515655446004
_HARD_BLOCK_MIN_REALIZED_TRADES = 3
_HARD_BLOCK_MIN_REALIZED_COVERAGE = 0.5
_HARD_BLOCK_MIN_REALIZED_COVERAGE_CONFIDENCE = 0.3
_HARD_BLOCK_MIN_RISK_SCORE = 0.15
_HARD_BLOCK_REALIZED_PER_TRADE_THRESHOLD = -0.03
_HARD_BLOCK_EDGE_REALIZATION_RATIO_THRESHOLD = 0.75
_HARD_BLOCK_EXPECTED_ONLY_MIN_ATTEMPTS = 36
_HARD_BLOCK_EXPECTED_ONLY_MIN_CONFIDENCE = 0.9
_HARD_BLOCK_EXPECTED_ONLY_EDGE_THRESHOLD = -0.09
_STATION_CONCENTRATION_SOFT_MIN_ATTEMPTS = 40
_STATION_CONCENTRATION_SOFT_MIN_STALE_NEGATIVE_ATTEMPTS = 8
_STATION_CONCENTRATION_SOFT_STALE_NEGATIVE_ATTEMPT_SHARE = 0.12
_STATION_CONCENTRATION_SOFT_MAX_SHARE = 0.75
_STATION_CONCENTRATION_SOFT_HHI = 0.60
_STATION_CONCENTRATION_HARD_MIN_ATTEMPTS = 36
_STATION_CONCENTRATION_HARD_MIN_STALE_NEGATIVE_ATTEMPTS = 12
_STATION_CONCENTRATION_HARD_STALE_NEGATIVE_ATTEMPT_SHARE = 0.20
_STATION_CONCENTRATION_HARD_MAX_SHARE = 0.85
_STATION_CONCENTRATION_HARD_HHI = 0.72


@dataclass(frozen=True)
class WeatherPatternIntent:
    source_file: str
    row_number: int
    join_keys: tuple[str, ...]
    settlement_station: str
    local_hour: int | None
    constraint_status: str
    signal_type: str
    side: str
    policy_approved: bool
    probability_confidence: float | None
    expected_edge_net: float | None
    edge_to_risk_ratio: float | None
    metar_age_minutes: float | None
    forecast_status: str


@dataclass(frozen=True)
class RealizedOutcome:
    source_file: str
    row_number: int
    join_keys: tuple[str, ...]
    realized_pnl_dollars: float | None
    expected_edge_dollars: float | None
    captured_at_utc: datetime | None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return float(parsed)


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _normalize_text(value).lower()
    if text in {"1", "true", "t", "yes", "y", "approved", "approve", "pass", "selected"}:
        return True
    if text in {"0", "false", "f", "no", "n", "blocked", "reject", "rejected", "fail"}:
        return False
    return False


def _parse_int(value: Any) -> int | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> datetime | None:
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
    if match:
        try:
            parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
        except ValueError:
            return None
        return parsed.replace(tzinfo=timezone.utc)
    return None


def _artifact_epoch(path: Path) -> float:
    inferred = _artifact_timestamp_utc(path)
    if isinstance(inferred, datetime):
        return inferred.timestamp()
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _load_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _first_text(row: dict[str, str], keys: Iterable[str]) -> str:
    for key in keys:
        text = _normalize_text(row.get(key))
        if text:
            return text
    return ""


def _first_float(row: dict[str, str], keys: Iterable[str]) -> float | None:
    for key in keys:
        parsed = _parse_float(row.get(key))
        if isinstance(parsed, float):
            return parsed
    return None


def _parse_identity_keys(row: dict[str, str]) -> tuple[str, ...]:
    keys: list[str] = []
    for candidate in (
        _first_text(row, ("temperature_client_order_id", "client_order_id")),
        _first_text(row, ("intent_id",)),
        _first_text(row, ("market_ticker",)),
    ):
        if candidate and candidate not in keys:
            keys.append(candidate)
    return tuple(keys)


def _parse_hour(row: dict[str, str]) -> int | None:
    for candidate in (
        row.get("policy_metar_local_hour"),
        row.get("metar_local_hour"),
        row.get("temperature_metar_local_hour"),
        row.get("local_hour"),
        row.get("hour"),
    ):
        parsed = _parse_int(candidate)
        if parsed is not None and 0 <= parsed <= 23:
            return parsed

    tz_name = _first_text(row, ("settlement_timezone", "timezone", "local_timezone"))
    base_dt = (
        _parse_ts(row.get("temperature_metar_observation_time_utc"))
        or _parse_ts(row.get("metar_observation_time_utc"))
        or _parse_ts(row.get("captured_at_utc"))
        or _parse_ts(row.get("captured_at"))
    )
    if base_dt is None:
        return None
    if tz_name:
        try:
            base_dt = base_dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
    return int(base_dt.hour)


def _parse_signal_type(row: dict[str, str]) -> str:
    return (
        _first_text(row, ("signal_type", "temperature_signal_type", "policy_reason", "constraint_status"))
        or "unknown"
    ).lower()


def _parse_constraint_status(row: dict[str, str]) -> str:
    return (
        _first_text(row, ("constraint_status", "policy_reason", "signal_type"))
        or "unknown"
    ).lower()


def _parse_side(row: dict[str, str]) -> str:
    return (_first_text(row, ("side", "policy_side")) or "unknown").lower()


def _parse_station(row: dict[str, str]) -> str:
    return _first_text(row, ("settlement_station", "station")) or "unknown_station"


def _parse_metar_age_minutes(row: dict[str, str]) -> float | None:
    return _first_float(
        row,
        (
            "metar_observation_age_minutes",
            "temperature_metar_observation_age_minutes",
            "policy_metar_observation_age_minutes",
        ),
    )


def _parse_forecast_status(row: dict[str, str]) -> str:
    return (
        _first_text(
            row,
            (
                "forecast_model_status",
                "forecast_status",
                "taf_status",
                "temperature_forecast_model_status",
            ),
        )
        or "unknown"
    ).lower()


def _metar_age_bucket(age_minutes: float | None) -> str:
    if age_minutes is None:
        return "unknown"
    if age_minutes <= 30.0:
        return "0-30m"
    if age_minutes <= 60.0:
        return "31-60m"
    if age_minutes <= 120.0:
        return "61-120m"
    if age_minutes <= 240.0:
        return "121-240m"
    return "240m+"


def _forecast_bucket(status: str) -> str:
    return _normalize_text(status).lower() or "unknown"


def _bucket_expectancy_per_trade(bucket: dict[str, Any]) -> float | None:
    realized_per_trade = bucket.get("realized_per_trade")
    if isinstance(realized_per_trade, (int, float)):
        return round(float(realized_per_trade), 6)
    expected_edge_mean = bucket.get("expected_edge_mean")
    if isinstance(expected_edge_mean, (int, float)):
        return round(float(expected_edge_mean), 6)
    return None


def _expectancy_raise_profile(expectancy: float | None) -> tuple[float, float]:
    if not isinstance(expectancy, float):
        return 0.0, 0.0
    if expectancy >= _NEGATIVE_EXPECTANCY_THRESHOLD:
        return 0.0, 0.0
    shortfall = _NEGATIVE_EXPECTANCY_THRESHOLD - float(expectancy)
    probability_raise = round(min(0.04, max(0.0, shortfall * 0.30)), 6)
    expected_edge_raise = round(min(0.008, max(0.0, shortfall * 0.06)), 6)
    return probability_raise, expected_edge_raise


def _normalize_profile_dimension_name(dimension: str) -> str:
    text = _normalize_text(dimension)
    if text == "settlement_station":
        return "station"
    if text == "constraint_status":
        return "signal_type"
    return text


def _normalize_profile_bucket_key(*, dimension: str, key: str) -> str:
    text = _normalize_text(key)
    if not text:
        return "unknown"
    if dimension == "station":
        return text.upper()
    if dimension in {"signal_type", "side", "weather_evidence_tier", "metar_age_bucket"}:
        return text.lower()
    return text


def _hard_block_lookup(hard_block_candidates: list[dict[str, Any]]) -> set[tuple[str, str]]:
    lookup: set[tuple[str, str]] = set()
    for candidate in hard_block_candidates:
        dimension = _normalize_profile_dimension_name(_normalize_text(candidate.get("dimension")))
        if not dimension:
            continue
        key = _normalize_profile_bucket_key(
            dimension=dimension,
            key=_normalize_text(candidate.get("bucket")),
        )
        lookup.add((dimension, key))
    return lookup


def _build_weather_pattern_profile(
    *,
    captured_at: datetime,
    bucket_dimensions: dict[str, dict[str, dict[str, Any]]],
    hard_block_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    hard_block_hits = _hard_block_lookup(hard_block_candidates)
    bucket_profiles: dict[str, dict[str, dict[str, Any]]] = {}
    normalized_dimensions: dict[str, list[str]] = {
        "station": ["settlement_station"],
        "local_hour": ["local_hour"],
        # Keep compatibility with runtime profile matching that maps
        # `constraint_status` keys into `signal_type` profile lookups.
        "signal_type": ["constraint_status", "signal_type"],
        "side": ["side"],
        "weather_evidence_tier": ["weather_evidence_tier"],
        "metar_age_bucket": ["metar_age_bucket"],
    }

    for normalized_dimension, source_dimensions in normalized_dimensions.items():
        entries: dict[str, dict[str, Any]] = {}
        for source_dimension in source_dimensions:
            buckets = bucket_dimensions.get(source_dimension)
            if not isinstance(buckets, dict):
                continue
            for raw_key, bucket in sorted(buckets.items(), key=lambda item: str(item[0])):
                if not isinstance(bucket, dict):
                    continue
                normalized_key = _normalize_profile_bucket_key(
                    dimension=normalized_dimension,
                    key=_normalize_text(raw_key),
                )
                if not normalized_key or normalized_key in entries:
                    continue
                samples = max(0, int(bucket.get("attempts") or 0))
                realized_trade_count = max(0, int(bucket.get("realized_trade_count") or 0))
                expectancy = _bucket_expectancy_per_trade(bucket)
                probability_raise, expected_edge_raise = _expectancy_raise_profile(expectancy)
                entry: dict[str, Any] = {
                    "samples": samples,
                    "expectancy_per_trade": expectancy,
                    "probability_raise": probability_raise,
                    "expected_edge_raise": expected_edge_raise,
                    "approval_rate": _parse_float(bucket.get("approval_rate")),
                    "expected_edge_mean": _parse_float(bucket.get("expected_edge_mean")),
                    "realized_trade_count": realized_trade_count,
                    "realized_coverage": _share(realized_trade_count, samples),
                    "realized_per_trade": _parse_float(bucket.get("realized_per_trade")),
                    "realized_per_attempt": _parse_float(bucket.get("realized_per_attempt")),
                    "edge_realization_ratio": _parse_float(bucket.get("edge_realization_ratio")),
                    "probability_confidence_mean": _parse_float(bucket.get("probability_confidence_mean")),
                    "edge_to_risk_ratio_mean": _parse_float(bucket.get("edge_to_risk_ratio_mean")),
                    "metar_age_minutes_mean": _parse_float(bucket.get("metar_age_minutes_mean")),
                    "metar_age_bucket": _normalize_text(bucket.get("metar_age_bucket")) or "unknown",
                    "forecast_status": _normalize_text(bucket.get("forecast_status")) or "unknown",
                    "signal_type": _normalize_text(bucket.get("signal_type")) or "unknown",
                    "side": _normalize_text(bucket.get("side")) or "unknown",
                    "sample_ok": bool(bucket.get("sample_ok")),
                    "bucket_risk_score": _parse_float(bucket.get("bucket_risk_score")),
                }
                if (normalized_dimension, normalized_key) in hard_block_hits:
                    entry["hard_block_candidate"] = True
                entries[normalized_key] = entry
        bucket_profiles[normalized_dimension] = entries

    return {
        "captured_at": captured_at.isoformat(),
        "source_age_hours": 0.0,
        "bucket_profiles": bucket_profiles,
    }


def _is_stale_metar_bucket(bucket: str) -> bool:
    return _normalize_text(bucket).lower() in _STALE_METAR_BUCKETS


def _share(part: int, whole: int) -> float:
    if whole <= 0:
        return 0.0
    return round(float(max(0, int(part)) / float(max(1, int(whole)))), 6)


def _wilson_lower_bound(part: int, whole: int, *, z: float = _WILSON_LOWER_BOUND_Z) -> float:
    total = max(0, int(whole))
    if total <= 0:
        return 0.0
    successes = max(0, min(total, int(part)))
    probability = float(successes) / float(total)
    z_score = max(0.0, float(z))
    z2 = z_score * z_score
    denominator = 1.0 + (z2 / float(total))
    center = probability + (z2 / (2.0 * float(total)))
    margin = z_score * math.sqrt((probability * (1.0 - probability) + (z2 / (4.0 * float(total)))) / float(total))
    lower_bound = (center - margin) / denominator
    return round(max(0.0, min(1.0, float(lower_bound))), 6)


def _concentration_from_station_attempts(
    station_attempts: Counter[str],
) -> tuple[float, float, list[dict[str, Any]], int]:
    total_attempts = max(0, int(sum(int(value) for value in station_attempts.values())))
    if total_attempts <= 0:
        return 0.0, 0.0, [], 0
    ranked = sorted(
        ((str(station), max(0, int(attempts))) for station, attempts in station_attempts.items()),
        key=lambda item: (-item[1], item[0]),
    )
    if not ranked:
        return 0.0, 0.0, [], 0
    max_share = float(ranked[0][1]) / float(total_attempts)
    hhi = 0.0
    top: list[dict[str, Any]] = []
    for station, attempts in ranked:
        if attempts <= 0:
            continue
        share = float(attempts) / float(total_attempts)
        hhi += share * share
        if len(top) < 5:
            top.append(
                {
                    "station": station,
                    "attempts": int(attempts),
                    "share": round(float(share), 6),
                }
            )
    return round(float(max_share), 6), round(float(hhi), 6), top, int(total_attempts)


def _regime_concentration_metrics(
    *,
    bucket_dimensions: dict[str, dict[str, dict[str, Any]]],
    attempts_total: int,
    intents: list[WeatherPatternIntent] | None = None,
) -> dict[str, Any]:
    weather_tiers = bucket_dimensions.get("weather_evidence_tier", {})
    metar_age_buckets = bucket_dimensions.get("metar_age_bucket", {})
    negative_expectancy_attempts = 0
    stale_metar_attempts = 0
    stale_metar_negative_attempts = 0
    stale_negative_tier_keys: set[str] = set()

    if isinstance(weather_tiers, dict):
        for tier_key, bucket in weather_tiers.items():
            if not isinstance(bucket, dict):
                continue
            bucket_key = _normalize_text(tier_key)
            metar_bucket = bucket_key.split("|", 1)[0] if bucket_key else ""
            is_stale = _is_stale_metar_bucket(metar_bucket)
            expectancy = _bucket_expectancy_per_trade(bucket)
            if isinstance(expectancy, float) and expectancy < 0.0:
                negative_expectancy_attempts += max(0, int(bucket.get("attempts") or 0))
                if is_stale:
                    stale_negative_tier_keys.add(bucket_key)

    if isinstance(metar_age_buckets, dict):
        for key, bucket in metar_age_buckets.items():
            if not isinstance(bucket, dict):
                continue
            attempts = max(0, int(bucket.get("attempts") or 0))
            is_stale = _is_stale_metar_bucket(str(key))
            if is_stale:
                stale_metar_attempts += attempts
            expectancy = _bucket_expectancy_per_trade(bucket)
            if is_stale and isinstance(expectancy, float) and expectancy < 0.0:
                stale_metar_negative_attempts += attempts

    stale_negative_station_attempts = Counter[str]()
    if intents and stale_negative_tier_keys:
        for intent in intents:
            tier_key = f"{_metar_age_bucket(intent.metar_age_minutes)}|{_forecast_bucket(intent.forecast_status)}"
            if tier_key not in stale_negative_tier_keys:
                continue
            station_key = _normalize_text(intent.settlement_station) or "unknown_station"
            stale_negative_station_attempts[station_key] += 1

    (
        stale_negative_station_max_share,
        stale_negative_station_hhi,
        stale_negative_station_top,
        stale_negative_station_attempts_total,
    ) = _concentration_from_station_attempts(stale_negative_station_attempts)

    total_attempts = max(0, int(attempts_total))
    negative_expectancy_attempt_share = _share(negative_expectancy_attempts, total_attempts)
    stale_metar_negative_attempt_share = _share(stale_metar_negative_attempts, total_attempts)
    stale_metar_attempt_share = _share(stale_metar_attempts, total_attempts)
    return {
        "negative_expectancy_attempt_share": negative_expectancy_attempt_share,
        "stale_metar_negative_attempt_share": stale_metar_negative_attempt_share,
        "stale_metar_attempt_share": stale_metar_attempt_share,
        "negative_expectancy_attempt_share_confidence_adjusted": _wilson_lower_bound(
            negative_expectancy_attempts,
            total_attempts,
        ),
        "stale_metar_negative_attempt_share_confidence_adjusted": _wilson_lower_bound(
            stale_metar_negative_attempts,
            total_attempts,
        ),
        "stale_metar_attempt_share_confidence_adjusted": _wilson_lower_bound(
            stale_metar_attempts,
            total_attempts,
        ),
        "stale_negative_station_attempts": int(stale_negative_station_attempts_total),
        "stale_negative_station_max_share": stale_negative_station_max_share,
        "stale_negative_station_hhi": stale_negative_station_hhi,
        "stale_negative_station_top": stale_negative_station_top,
        "confidence_adjustment_method": "wilson_lower_bound",
        "confidence_adjustment_z_score": round(float(_WILSON_LOWER_BOUND_Z), 6),
    }


def _build_risk_off_recommendation(
    *,
    attempts_total: int,
    negative_expectancy_attempt_share: float,
    stale_metar_negative_attempt_share: float,
    stale_metar_attempt_share: float,
    negative_expectancy_attempt_share_confidence_adjusted: float | None = None,
    stale_metar_negative_attempt_share_confidence_adjusted: float | None = None,
    stale_metar_attempt_share_confidence_adjusted: float | None = None,
    stale_negative_station_max_share: float | None = None,
    stale_negative_station_hhi: float | None = None,
    stale_negative_station_attempts: int | None = None,
    hard_block_candidate_count: int,
) -> dict[str, Any]:
    attempts = max(0, int(attempts_total))
    observed_negative_expectancy_attempt_share = max(0.0, min(1.0, float(negative_expectancy_attempt_share)))
    observed_stale_metar_negative_attempt_share = max(0.0, min(1.0, float(stale_metar_negative_attempt_share)))
    observed_stale_metar_attempt_share = max(0.0, min(1.0, float(stale_metar_attempt_share)))
    observed_stale_negative_station_max_share = (
        max(0.0, min(1.0, float(stale_negative_station_max_share)))
        if isinstance(stale_negative_station_max_share, (int, float))
        else 0.0
    )
    observed_stale_negative_station_hhi = (
        max(0.0, min(1.0, float(stale_negative_station_hhi)))
        if isinstance(stale_negative_station_hhi, (int, float))
        else 0.0
    )
    station_attempts = max(0, int(stale_negative_station_attempts or 0))
    effective_negative_expectancy_attempt_share = (
        max(0.0, min(1.0, float(negative_expectancy_attempt_share_confidence_adjusted)))
        if isinstance(negative_expectancy_attempt_share_confidence_adjusted, (int, float))
        else observed_negative_expectancy_attempt_share
    )
    effective_stale_metar_negative_attempt_share = (
        max(0.0, min(1.0, float(stale_metar_negative_attempt_share_confidence_adjusted)))
        if isinstance(stale_metar_negative_attempt_share_confidence_adjusted, (int, float))
        else observed_stale_metar_negative_attempt_share
    )
    effective_stale_metar_attempt_share = (
        max(0.0, min(1.0, float(stale_metar_attempt_share_confidence_adjusted)))
        if isinstance(stale_metar_attempt_share_confidence_adjusted, (int, float))
        else observed_stale_metar_attempt_share
    )
    effective_stale_negative_station_max_share = observed_stale_negative_station_max_share
    effective_stale_negative_station_hhi = observed_stale_negative_station_hhi
    min_attempts_soft = 12
    min_attempts_hard = 20
    recommendation: dict[str, Any] = {
        "status": "monitor_only",
        "active": False,
        "hard_block": False,
        "reason": "insufficient_attempts",
        "attempts_considered": attempts,
        "min_attempts_soft": min_attempts_soft,
        "min_attempts_hard": min_attempts_hard,
        "negative_expectancy_attempt_share_observed": round(float(observed_negative_expectancy_attempt_share), 6),
        "negative_expectancy_attempt_share_effective": round(float(effective_negative_expectancy_attempt_share), 6),
        "stale_metar_negative_attempt_share_observed": round(float(observed_stale_metar_negative_attempt_share), 6),
        "stale_metar_negative_attempt_share_effective": round(float(effective_stale_metar_negative_attempt_share), 6),
        "stale_metar_attempt_share_observed": round(float(observed_stale_metar_attempt_share), 6),
        "stale_metar_attempt_share_effective": round(float(effective_stale_metar_attempt_share), 6),
        "stale_negative_station_attempts": int(station_attempts),
        "stale_negative_station_max_share_observed": round(float(observed_stale_negative_station_max_share), 6),
        "stale_negative_station_max_share_effective": round(float(effective_stale_negative_station_max_share), 6),
        "stale_negative_station_hhi_observed": round(float(observed_stale_negative_station_hhi), 6),
        "stale_negative_station_hhi_effective": round(float(effective_stale_negative_station_hhi), 6),
        "stale_negative_station_min_attempts_soft": int(_STATION_CONCENTRATION_SOFT_MIN_STALE_NEGATIVE_ATTEMPTS),
        "stale_negative_station_min_attempts_hard": int(_STATION_CONCENTRATION_HARD_MIN_STALE_NEGATIVE_ATTEMPTS),
        "confidence_adjustment_applied": (
            not math.isclose(
                effective_negative_expectancy_attempt_share,
                observed_negative_expectancy_attempt_share,
                abs_tol=1e-9,
            )
            or not math.isclose(
                effective_stale_metar_negative_attempt_share,
                observed_stale_metar_negative_attempt_share,
                abs_tol=1e-9,
            )
            or not math.isclose(
                effective_stale_metar_attempt_share,
                observed_stale_metar_attempt_share,
                abs_tol=1e-9,
            )
        ),
        "probability_raise": 0.0,
        "expected_edge_raise": 0.0,
    }

    if attempts < min_attempts_soft:
        return recommendation

    legacy_hard_condition = attempts >= min_attempts_hard and hard_block_candidate_count >= 1 and (
        effective_stale_metar_negative_attempt_share >= 0.30
        or effective_negative_expectancy_attempt_share >= 0.65
        or (
            hard_block_candidate_count >= 2
            and effective_negative_expectancy_attempt_share >= 0.45
        )
    )
    station_hard_condition = (
        attempts >= max(min_attempts_hard, _STATION_CONCENTRATION_HARD_MIN_ATTEMPTS)
        and station_attempts >= _STATION_CONCENTRATION_HARD_MIN_STALE_NEGATIVE_ATTEMPTS
        and hard_block_candidate_count >= 1
        and observed_stale_metar_negative_attempt_share >= _STATION_CONCENTRATION_HARD_STALE_NEGATIVE_ATTEMPT_SHARE
        and effective_stale_negative_station_max_share >= _STATION_CONCENTRATION_HARD_MAX_SHARE
        and effective_stale_negative_station_hhi >= _STATION_CONCENTRATION_HARD_HHI
    )
    legacy_soft_condition = (
        effective_negative_expectancy_attempt_share >= 0.35
        or effective_stale_metar_negative_attempt_share >= 0.18
        or effective_stale_metar_attempt_share >= 0.55
    )
    station_soft_condition = (
        attempts >= max(min_attempts_soft, _STATION_CONCENTRATION_SOFT_MIN_ATTEMPTS)
        and station_attempts >= _STATION_CONCENTRATION_SOFT_MIN_STALE_NEGATIVE_ATTEMPTS
        and observed_stale_metar_negative_attempt_share >= _STATION_CONCENTRATION_SOFT_STALE_NEGATIVE_ATTEMPT_SHARE
        and effective_stale_negative_station_max_share >= _STATION_CONCENTRATION_SOFT_MAX_SHARE
        and effective_stale_negative_station_hhi >= _STATION_CONCENTRATION_SOFT_HHI
    )
    hard_condition = legacy_hard_condition or station_hard_condition
    soft_condition = legacy_soft_condition or station_soft_condition

    if hard_condition:
        recommendation["status"] = "risk_off_hard"
        recommendation["active"] = True
        recommendation["hard_block"] = True
        recommendation["reason"] = (
            "concentrated_negative_expectancy"
            if legacy_hard_condition
            else "stale_negative_station_concentration"
        )
        recommendation["probability_raise"] = 0.03
        recommendation["expected_edge_raise"] = 0.006
        return recommendation

    if soft_condition:
        recommendation["status"] = "risk_off_soft"
        recommendation["active"] = True
        recommendation["reason"] = (
            "emerging_negative_concentration"
            if legacy_soft_condition
            else "stale_negative_station_concentration_emerging"
        )
        recommendation["probability_raise"] = 0.015
        recommendation["expected_edge_raise"] = 0.003
        return recommendation

    recommendation["status"] = "normal"
    recommendation["reason"] = "concentration_within_limits"
    return recommendation


def _load_intent_rows(
    *,
    output_dir: Path,
    window_start: datetime,
) -> tuple[list[WeatherPatternIntent], dict[str, Any]]:
    rows: list[WeatherPatternIntent] = []
    files_scanned: list[str] = []
    row_status_counts: Counter[str] = Counter()
    for path in sorted(output_dir.glob(_INTENT_PATTERN), key=_artifact_epoch):
        if _artifact_epoch(path) < window_start.timestamp():
            continue
        files_scanned.append(str(path))
        try:
            with path.open("r", newline="", encoding="utf-8-sig") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    row_status_counts["empty_csv_header"] += 1
                    continue
                for row_number, row in enumerate(reader, start=2):
                    if not isinstance(row, dict):
                        row_status_counts["malformed_row"] += 1
                        continue
                    captured_at = _parse_ts(row.get("captured_at_utc")) or _parse_ts(row.get("captured_at"))
                    if captured_at is not None and captured_at < window_start:
                        continue
                    intent = WeatherPatternIntent(
                        source_file=str(path),
                        row_number=row_number,
                        join_keys=_parse_identity_keys(row),
                        settlement_station=_parse_station(row),
                        local_hour=_parse_hour(row),
                        constraint_status=_parse_constraint_status(row),
                        signal_type=_parse_signal_type(row),
                        side=_parse_side(row),
                        policy_approved=_parse_bool(row.get("policy_approved")),
                        probability_confidence=_first_float(
                            row,
                            ("policy_probability_confidence", "probability_confidence"),
                        ),
                        expected_edge_net=_first_float(
                            row,
                            ("policy_expected_edge_net", "expected_edge_net", "expected_edge_dollars"),
                        ),
                        edge_to_risk_ratio=_first_float(
                            row,
                            ("policy_edge_to_risk_ratio", "edge_to_risk_ratio"),
                        ),
                        metar_age_minutes=_parse_metar_age_minutes(row),
                        forecast_status=_parse_forecast_status(row),
                    )
                    rows.append(intent)
                    row_status_counts["valid"] += 1
        except OSError:
            row_status_counts["unreadable_input_file"] += 1
    payload = {
        "input_files_count": len(files_scanned),
        "input_files": files_scanned,
        "rows_total": len(rows),
        "raw_row_status_counts": dict(sorted(row_status_counts.items(), key=lambda item: (-item[1], item[0]))),
    }
    return rows, payload


def _load_profitability_sources(
    *,
    output_dir: Path,
    window_start: datetime,
    now_utc: datetime,
    max_profile_age_hours: float,
) -> tuple[list[RealizedOutcome], dict[str, Any]]:
    summary_paths = sorted(
        list(output_dir.glob(_PROFITABILITY_SUMMARY_PATTERN))
        + list(output_dir.glob(_PROFITABILITY_CHECKPOINT_PATTERN)),
        key=_artifact_epoch,
    )
    loadable_csv_paths: list[Path] = []
    summary_files: list[str] = []
    summary_status: list[dict[str, Any]] = []
    max_age_seconds = max(0.0, float(max_profile_age_hours)) * 3600.0
    now_epoch = now_utc.timestamp()

    for path in summary_paths:
        artifact_epoch = _artifact_epoch(path)
        if artifact_epoch and (now_epoch - artifact_epoch) > max_age_seconds:
            continue
        payload = _load_json_dict(path)
        if payload is None:
            continue
        summary_files.append(str(path))
        output_csv = _normalize_text(payload.get("output_csv"))
        if output_csv:
            csv_path = Path(output_csv)
            if not csv_path.is_absolute():
                csv_path = (path.parent / csv_path).resolve()
            if csv_path.exists():
                loadable_csv_paths.append(csv_path)
        summary_status.append(
            {
                "file": str(path),
                "captured_at": _normalize_text(payload.get("captured_at")),
                "status": _normalize_text(payload.get("status")),
            }
        )

    if not loadable_csv_paths:
        loadable_csv_paths = sorted(
            [path for path in output_dir.glob(_SETTLED_CSV_PATTERN) if _artifact_epoch(path) >= window_start.timestamp()],
            key=_artifact_epoch,
        )

    realized_rows: list[RealizedOutcome] = []
    row_status_counts: Counter[str] = Counter()
    for path in loadable_csv_paths:
        file_reference_captured_at = _artifact_timestamp_utc(path)
        if file_reference_captured_at is None:
            file_epoch = _artifact_epoch(path)
            if file_epoch > 0:
                file_reference_captured_at = datetime.fromtimestamp(file_epoch, tz=timezone.utc)
        try:
            with path.open("r", newline="", encoding="utf-8-sig") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    row_status_counts["empty_csv_header"] += 1
                    continue
                for row_number, row in enumerate(reader, start=2):
                    if not isinstance(row, dict):
                        row_status_counts["malformed_row"] += 1
                        continue
                    captured_at = _parse_ts(row.get("captured_at_utc")) or _parse_ts(row.get("captured_at"))
                    row_reference_captured_at = captured_at or file_reference_captured_at
                    if row_reference_captured_at is not None and row_reference_captured_at < window_start:
                        row_status_counts["outside_window"] += 1
                        continue
                    order_key = _normalize_text(row.get("order_key"))
                    identity_keys = []
                    if order_key.startswith("client:"):
                        identity_keys.append(order_key.split("client:", 1)[1])
                    elif order_key.startswith("order:"):
                        identity_keys.append(order_key)
                    client_order_id = _first_text(row, ("client_order_id",))
                    market_ticker = _first_text(row, ("market_ticker",))
                    for candidate in (client_order_id, market_ticker):
                        if candidate and candidate not in identity_keys:
                            identity_keys.append(candidate)
                    realized_rows.append(
                        RealizedOutcome(
                            source_file=str(path),
                            row_number=row_number,
                            join_keys=tuple(identity_keys),
                            realized_pnl_dollars=_parse_float(row.get("realized_pnl_dollars")),
                            expected_edge_dollars=_parse_float(row.get("expected_edge_dollars")),
                            captured_at_utc=row_reference_captured_at,
                        )
                    )
                    row_status_counts["valid"] += 1
        except OSError:
            row_status_counts["unreadable_output_file"] += 1

    payload = {
        "summary_files_count": len(summary_files),
        "summary_files": summary_files,
        "summary_status": summary_status,
        "realized_files_count": len(loadable_csv_paths),
        "realized_files": [str(path) for path in loadable_csv_paths],
        "rows_total": len(realized_rows),
        "raw_row_status_counts": dict(sorted(row_status_counts.items(), key=lambda item: (-item[1], item[0]))),
    }
    return realized_rows, payload


def _bucket_record_template() -> dict[str, Any]:
    return {
        "attempts": 0,
        "approved": 0,
        "probability_confidence_sum": 0.0,
        "probability_confidence_count": 0,
        "expected_edge_sum": 0.0,
        "expected_edge_count": 0,
        "edge_to_risk_ratio_sum": 0.0,
        "edge_to_risk_ratio_count": 0,
        "metar_age_minutes_sum": 0.0,
        "metar_age_minutes_count": 0,
        "realized_trade_count": 0,
        "realized_pnl_sum": 0.0,
        "realized_expected_edge_sum": 0.0,
        "realized_expected_edge_count": 0,
        "policy_reason_counts": Counter(),
        "side_counts": Counter(),
        "signal_counts": Counter(),
        "forecast_status_counts": Counter(),
        "metar_age_bucket_counts": Counter(),
    }


def _update_bucket(bucket: dict[str, Any], intent: WeatherPatternIntent, realized: RealizedOutcome | None) -> None:
    bucket["attempts"] += 1
    if intent.policy_approved:
        bucket["approved"] += 1
    if isinstance(intent.probability_confidence, float):
        bucket["probability_confidence_sum"] += float(intent.probability_confidence)
        bucket["probability_confidence_count"] += 1
    if isinstance(intent.expected_edge_net, float):
        bucket["expected_edge_sum"] += float(intent.expected_edge_net)
        bucket["expected_edge_count"] += 1
    if isinstance(intent.edge_to_risk_ratio, float):
        bucket["edge_to_risk_ratio_sum"] += float(intent.edge_to_risk_ratio)
        bucket["edge_to_risk_ratio_count"] += 1
    if isinstance(intent.metar_age_minutes, float):
        bucket["metar_age_minutes_sum"] += float(intent.metar_age_minutes)
        bucket["metar_age_minutes_count"] += 1
    bucket["policy_reason_counts"][intent.constraint_status] += 1
    bucket["signal_counts"][intent.signal_type] += 1
    bucket["side_counts"][intent.side] += 1
    bucket["forecast_status_counts"][intent.forecast_status] += 1
    bucket["metar_age_bucket_counts"][_metar_age_bucket(intent.metar_age_minutes)] += 1
    if realized is None:
        return
    if isinstance(realized.realized_pnl_dollars, float):
        bucket["realized_trade_count"] += 1
        bucket["realized_pnl_sum"] += float(realized.realized_pnl_dollars)
        if isinstance(realized.expected_edge_dollars, float):
            bucket["realized_expected_edge_sum"] += float(realized.expected_edge_dollars)
            bucket["realized_expected_edge_count"] += 1


def _finalize_bucket(
    *,
    dimension: str,
    key: str,
    bucket: dict[str, Any],
    min_bucket_samples: int,
) -> dict[str, Any]:
    attempts = int(bucket["attempts"])
    approved = int(bucket["approved"])
    approval_rate = round(float(approved / attempts), 6) if attempts > 0 else 0.0
    expected_edge_sum = round(float(bucket["expected_edge_sum"]), 6)
    expected_edge_mean = (
        round(float(expected_edge_sum / attempts), 6) if attempts > 0 else None
    )
    realized_pnl_sum = (
        round(float(bucket["realized_pnl_sum"]), 6) if int(bucket["realized_trade_count"]) > 0 else None
    )
    realized_per_trade = (
        round(float(bucket["realized_pnl_sum"]) / float(bucket["realized_trade_count"]), 6)
        if int(bucket["realized_trade_count"]) > 0
        else None
    )
    realized_per_attempt = (
        round(float(bucket["realized_pnl_sum"]) / float(attempts), 6)
        if attempts > 0 and int(bucket["realized_trade_count"]) > 0
        else None
    )
    edge_realization_ratio = (
        round(float(bucket["realized_pnl_sum"]) / float(bucket["expected_edge_sum"]), 6)
        if abs(float(bucket["expected_edge_sum"])) > 1e-12 and int(bucket["realized_trade_count"]) > 0
        else None
    )
    probability_confidence_mean = (
        round(
            float(bucket["probability_confidence_sum"]) / float(bucket["probability_confidence_count"]),
            6,
        )
        if int(bucket["probability_confidence_count"]) > 0
        else None
    )
    edge_to_risk_ratio_mean = (
        round(
            float(bucket["edge_to_risk_ratio_sum"]) / float(bucket["edge_to_risk_ratio_count"]),
            6,
        )
        if int(bucket["edge_to_risk_ratio_count"]) > 0
        else None
    )
    metar_age_minutes_mean = (
        round(float(bucket["metar_age_minutes_sum"]) / float(bucket["metar_age_minutes_count"]), 6)
        if int(bucket["metar_age_minutes_count"]) > 0
        else None
    )
    metar_age_bucket = _most_common_bucket(bucket["metar_age_bucket_counts"])
    forecast_status = _most_common_bucket(bucket["forecast_status_counts"])
    signal_type = _most_common_bucket(bucket["signal_counts"])
    side = _most_common_bucket(bucket["side_counts"])
    policy_reason = _most_common_bucket(bucket["policy_reason_counts"])
    sample_ok = attempts >= int(min_bucket_samples)
    bucket_risk_score = 0.0
    if isinstance(expected_edge_mean, float):
        bucket_risk_score += max(0.0, -expected_edge_mean)
    if isinstance(realized_per_trade, float):
        bucket_risk_score += max(0.0, -realized_per_trade)
    if isinstance(edge_realization_ratio, float):
        bucket_risk_score += max(0.0, 1.0 - edge_realization_ratio)
    if attempts > 0:
        bucket_risk_score += max(0.0, (approved / attempts) - 0.5) * 0.1
    return {
        "dimension": dimension,
        "bucket": key,
        "attempts": attempts,
        "approved": approved,
        "approval_rate": approval_rate,
        "expected_edge_sum": expected_edge_sum,
        "expected_edge_mean": expected_edge_mean,
        "realized_trade_count": int(bucket["realized_trade_count"]),
        "realized_pnl_sum": realized_pnl_sum,
        "realized_per_trade": realized_per_trade,
        "realized_per_attempt": realized_per_attempt,
        "edge_realization_ratio": edge_realization_ratio,
        "probability_confidence_mean": probability_confidence_mean,
        "edge_to_risk_ratio_mean": edge_to_risk_ratio_mean,
        "metar_age_minutes_mean": metar_age_minutes_mean,
        "metar_age_bucket": metar_age_bucket,
        "forecast_status": forecast_status,
        "signal_type": signal_type,
        "side": side,
        "policy_reason": policy_reason,
        "sample_ok": sample_ok,
        "bucket_risk_score": round(float(bucket_risk_score), 6),
    }


def _most_common_bucket(counter: Counter[str]) -> str:
    if not counter:
        return "unknown"
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _bucketize(
    intents: list[WeatherPatternIntent],
    realized_by_key: dict[str, RealizedOutcome],
    *,
    min_bucket_samples: int,
) -> dict[str, dict[str, dict[str, Any]]]:
    dimensions: dict[str, dict[str, dict[str, Any]]] = {
        "settlement_station": defaultdict(_bucket_record_template),
        "local_hour": defaultdict(_bucket_record_template),
        "constraint_status": defaultdict(_bucket_record_template),
        "signal_type": defaultdict(_bucket_record_template),
        "side": defaultdict(_bucket_record_template),
        "weather_evidence_tier": defaultdict(_bucket_record_template),
        "metar_age_bucket": defaultdict(_bucket_record_template),
    }
    for intent in intents:
        realized = None
        for key in intent.join_keys:
            realized = realized_by_key.get(key)
            if realized is not None:
                break
        keys = {
            "settlement_station": intent.settlement_station,
            "local_hour": str(intent.local_hour) if intent.local_hour is not None else "unknown",
            "constraint_status": intent.constraint_status,
            "signal_type": intent.signal_type,
            "side": intent.side,
            "weather_evidence_tier": f"{_metar_age_bucket(intent.metar_age_minutes)}|{_forecast_bucket(intent.forecast_status)}",
            "metar_age_bucket": _metar_age_bucket(intent.metar_age_minutes),
        }
        for dimension, key in keys.items():
            _update_bucket(dimensions[dimension][key], intent, realized)

    finalized: dict[str, dict[str, dict[str, Any]]] = {}
    for dimension, buckets in dimensions.items():
        finalized[dimension] = {
            key: _finalize_bucket(
                dimension=dimension,
                key=key,
                bucket=bucket,
                min_bucket_samples=min_bucket_samples,
            )
            for key, bucket in sorted(buckets.items(), key=lambda item: item[0])
        }
    return finalized


def _collect_bucket_views(
    bucket_dimensions: dict[str, dict[str, dict[str, Any]]],
    *,
    min_bucket_samples: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    all_buckets: list[dict[str, Any]] = []
    for dimension in sorted(bucket_dimensions.keys()):
        for bucket in bucket_dimensions[dimension].values():
            all_buckets.append(dict(bucket))
    all_buckets.sort(
        key=lambda item: (
            -float(item.get("bucket_risk_score") or 0.0),
            -int(item.get("attempts") or 0),
            _normalize_text(item.get("dimension")),
            _normalize_text(item.get("bucket")),
        )
    )
    negative_expectancy = [
        bucket
        for bucket in all_buckets
        if int(bucket.get("attempts") or 0) >= int(min_bucket_samples)
        and (
            (isinstance(bucket.get("expected_edge_mean"), (int, float)) and float(bucket.get("expected_edge_mean")) < 0.0)
            or (
                isinstance(bucket.get("realized_per_trade"), (int, float))
                and float(bucket.get("realized_per_trade")) < 0.0
            )
            or (
                isinstance(bucket.get("edge_realization_ratio"), (int, float))
                and float(bucket.get("edge_realization_ratio")) < 1.0
            )
        )
    ]
    elevated_risk = [
        bucket
        for bucket in all_buckets
        if int(bucket.get("attempts") or 0) >= int(min_bucket_samples)
        and (
            (isinstance(bucket.get("realized_per_trade"), (int, float)) and float(bucket.get("realized_per_trade")) < 0.0)
            or (isinstance(bucket.get("edge_realization_ratio"), (int, float)) and float(bucket.get("edge_realization_ratio")) < 0.75)
            or float(bucket.get("bucket_risk_score") or 0.0) >= 0.25
        )
    ]
    return all_buckets, negative_expectancy[:25], elevated_risk[:25]


def _threshold_raise_candidates(
    buckets: list[dict[str, Any]],
    *,
    min_bucket_samples: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for bucket in buckets:
        attempts = int(bucket.get("attempts") or 0)
        if attempts < int(min_bucket_samples):
            continue
        expected_edge_mean = (
            float(bucket.get("expected_edge_mean"))
            if isinstance(bucket.get("expected_edge_mean"), (int, float))
            else None
        )
        expected_edge_only_negative = (
            isinstance(expected_edge_mean, float) and expected_edge_mean < -0.02
        )
        if not (
            (isinstance(bucket.get("realized_per_trade"), (int, float)) and float(bucket.get("realized_per_trade")) < 0.0)
            or (
                isinstance(bucket.get("edge_realization_ratio"), (int, float))
                and float(bucket.get("edge_realization_ratio")) < 1.0
            )
            or expected_edge_only_negative
        ):
            continue
        prob_mean = bucket.get("probability_confidence_mean")
        edge_mean = expected_edge_mean
        risk_mean = bucket.get("edge_to_risk_ratio_mean")
        if isinstance(prob_mean, float):
            candidates.append(
                {
                    "threshold": "min_probability_confidence",
                    "suggested_minimum": round(min(0.999, prob_mean + 0.02), 6),
                    "dimension": bucket.get("dimension"),
                    "bucket": bucket.get("bucket"),
                    "reason": "negative_expectancy_bucket",
                    "evidence": {
                        "attempts": attempts,
                        "approval_rate": bucket.get("approval_rate"),
                        "realized_per_trade": bucket.get("realized_per_trade"),
                        "expected_edge_mean": edge_mean,
                    },
                }
            )
        if isinstance(edge_mean, float):
            candidates.append(
                {
                    "threshold": "min_expected_edge_net",
                    "suggested_minimum": round(max(0.0, edge_mean + 0.005), 6),
                    "dimension": bucket.get("dimension"),
                    "bucket": bucket.get("bucket"),
                    "reason": "negative_expectancy_bucket",
                    "evidence": {
                        "attempts": attempts,
                        "approval_rate": bucket.get("approval_rate"),
                        "realized_per_trade": bucket.get("realized_per_trade"),
                        "expected_edge_mean": edge_mean,
                    },
                }
            )
        if isinstance(risk_mean, float):
            candidates.append(
                {
                    "threshold": "min_edge_to_risk_ratio",
                    "suggested_minimum": round(max(0.0, min(9.999, risk_mean + 0.03)), 6),
                    "dimension": bucket.get("dimension"),
                    "bucket": bucket.get("bucket"),
                    "reason": "negative_expectancy_bucket",
                    "evidence": {
                        "attempts": attempts,
                        "approval_rate": bucket.get("approval_rate"),
                        "realized_per_trade": bucket.get("realized_per_trade"),
                        "expected_edge_mean": edge_mean,
                    },
                }
            )
    candidates.sort(
        key=lambda item: (
            _normalize_text(item.get("threshold")),
            -int(item.get("evidence", {}).get("attempts") or 0),
            _normalize_text(item.get("dimension")),
            _normalize_text(item.get("bucket")),
        )
    )
    return candidates[:25]


def _hard_block_candidates(
    buckets: list[dict[str, Any]],
    *,
    min_bucket_samples: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for bucket in buckets:
        attempts = int(bucket.get("attempts") or 0)
        if attempts < int(min_bucket_samples):
            continue
        realized_per_trade = bucket.get("realized_per_trade")
        edge_ratio = bucket.get("edge_realization_ratio")
        realized_trade_count = int(bucket.get("realized_trade_count") or 0)
        probability_confidence_mean = (
            float(bucket.get("probability_confidence_mean"))
            if isinstance(bucket.get("probability_confidence_mean"), (int, float))
            else None
        )
        expected_edge_mean = (
            float(bucket.get("expected_edge_mean"))
            if isinstance(bucket.get("expected_edge_mean"), (int, float))
            else None
        )

        realized_coverage = float(realized_trade_count) / float(max(1, attempts))
        realized_coverage_confidence = _wilson_lower_bound(realized_trade_count, attempts)
        has_realized_signal = isinstance(realized_per_trade, float) and float(realized_per_trade) < 0.0
        strong_realized_signal = (
            has_realized_signal
            and realized_trade_count >= max(_HARD_BLOCK_MIN_REALIZED_TRADES, int(min_bucket_samples))
            and realized_coverage >= _HARD_BLOCK_MIN_REALIZED_COVERAGE
            and realized_coverage_confidence >= _HARD_BLOCK_MIN_REALIZED_COVERAGE_CONFIDENCE
            and (
                float(realized_per_trade) <= _HARD_BLOCK_REALIZED_PER_TRADE_THRESHOLD
                or (
                    isinstance(edge_ratio, float)
                    and float(edge_ratio) <= _HARD_BLOCK_EDGE_REALIZATION_RATIO_THRESHOLD
                )
            )
        )
        # Model-only pressure must clear substantially higher persistence and
        # confidence to qualify as a hard block candidate.
        strong_expected_only_signal = (
            not has_realized_signal
            and isinstance(expected_edge_mean, float)
            and expected_edge_mean <= _HARD_BLOCK_EXPECTED_ONLY_EDGE_THRESHOLD
            and attempts >= max(int(min_bucket_samples), _HARD_BLOCK_EXPECTED_ONLY_MIN_ATTEMPTS)
            and isinstance(probability_confidence_mean, float)
            and probability_confidence_mean >= _HARD_BLOCK_EXPECTED_ONLY_MIN_CONFIDENCE
        )
        if not strong_realized_signal and not strong_expected_only_signal:
            continue

        severity = max(0.0, -float(realized_per_trade)) if isinstance(realized_per_trade, float) else 0.0
        if strong_expected_only_signal and isinstance(expected_edge_mean, float):
            severity += max(0.0, -float(expected_edge_mean))
        if isinstance(edge_ratio, float):
            severity += max(0.0, 1.0 - float(edge_ratio))
        risk_score = float(bucket.get("bucket_risk_score") or 0.0)
        min_risk_score = _HARD_BLOCK_MIN_RISK_SCORE if strong_realized_signal else max(_HARD_BLOCK_MIN_RISK_SCORE, 0.2)
        if risk_score < min_risk_score:
            continue
        candidates.append(
            {
                "dimension": bucket.get("dimension"),
                "bucket": bucket.get("bucket"),
                "reason": (
                    "negative_realized_per_trade"
                    if strong_realized_signal
                    else "negative_expected_edge_mean"
                ),
                "attempts": attempts,
                "approved": bucket.get("approved"),
                "approval_rate": bucket.get("approval_rate"),
                "expected_edge_mean": expected_edge_mean,
                "realized_per_trade": realized_per_trade,
                "realized_trade_count": realized_trade_count,
                "realized_coverage": round(float(realized_coverage), 6),
                "realized_coverage_confidence": round(float(realized_coverage_confidence), 6),
                "probability_confidence_mean": probability_confidence_mean,
                "edge_realization_ratio": edge_ratio,
                "bucket_risk_score": round(float(risk_score), 6),
                "severity": round(float(severity), 6),
            }
        )
    candidates.sort(
        key=lambda item: (
            -float(item.get("severity") or 0.0),
            -int(item.get("attempts") or 0),
            _normalize_text(item.get("dimension")),
            _normalize_text(item.get("bucket")),
        )
    )
    return candidates[:25]


def _match_realized_outcomes(intents: list[WeatherPatternIntent], realized_rows: list[RealizedOutcome]) -> dict[str, RealizedOutcome]:
    realized_by_key: dict[str, RealizedOutcome] = {}
    for row in realized_rows:
        for key in row.join_keys:
            if key and key not in realized_by_key:
                realized_by_key[key] = row
    return realized_by_key


def run_kalshi_temperature_weather_pattern(
    *,
    output_dir: str,
    window_hours: float,
    min_bucket_samples: int,
    max_profile_age_hours: float,
) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    health_dir = out_dir / _HEALTH_DIR_NAME
    health_dir.mkdir(parents=True, exist_ok=True)
    safe_window_hours = max(1.0, float(window_hours))
    safe_min_bucket_samples = max(1, int(min_bucket_samples))
    safe_max_profile_age_hours = max(0.0, float(max_profile_age_hours))
    window_start = now_utc - timedelta(hours=safe_window_hours)

    intents, intent_source = _load_intent_rows(output_dir=out_dir, window_start=window_start)
    realized_rows, realized_source = _load_profitability_sources(
        output_dir=out_dir,
        window_start=window_start,
        now_utc=now_utc,
        max_profile_age_hours=safe_max_profile_age_hours,
    )
    realized_by_key = _match_realized_outcomes(intents, realized_rows)
    bucket_dimensions = _bucketize(
        intents,
        realized_by_key,
        min_bucket_samples=safe_min_bucket_samples,
    )
    all_buckets, negative_expectancy_buckets, elevated_risk_buckets = _collect_bucket_views(
        bucket_dimensions,
        min_bucket_samples=safe_min_bucket_samples,
    )
    hard_block_candidates = _hard_block_candidates(
        all_buckets,
        min_bucket_samples=safe_min_bucket_samples,
    )
    threshold_raise_candidates = _threshold_raise_candidates(
        negative_expectancy_buckets or all_buckets,
        min_bucket_samples=safe_min_bucket_samples,
    )

    attempts_total = len(intents)
    weather_pattern_profile = _build_weather_pattern_profile(
        captured_at=now_utc,
        bucket_dimensions=bucket_dimensions,
        hard_block_candidates=hard_block_candidates,
    )
    concentration_metrics = _regime_concentration_metrics(
        bucket_dimensions=bucket_dimensions,
        attempts_total=attempts_total,
        intents=intents,
    )
    risk_off_recommendation = _build_risk_off_recommendation(
        attempts_total=attempts_total,
        negative_expectancy_attempt_share=float(concentration_metrics["negative_expectancy_attempt_share"]),
        stale_metar_negative_attempt_share=float(concentration_metrics["stale_metar_negative_attempt_share"]),
        stale_metar_attempt_share=float(concentration_metrics["stale_metar_attempt_share"]),
        negative_expectancy_attempt_share_confidence_adjusted=float(
            concentration_metrics["negative_expectancy_attempt_share_confidence_adjusted"]
        ),
        stale_metar_negative_attempt_share_confidence_adjusted=float(
            concentration_metrics["stale_metar_negative_attempt_share_confidence_adjusted"]
        ),
        stale_metar_attempt_share_confidence_adjusted=float(
            concentration_metrics["stale_metar_attempt_share_confidence_adjusted"]
        ),
        stale_negative_station_max_share=float(concentration_metrics["stale_negative_station_max_share"]),
        stale_negative_station_hhi=float(concentration_metrics["stale_negative_station_hhi"]),
        stale_negative_station_attempts=int(concentration_metrics["stale_negative_station_attempts"]),
        hard_block_candidate_count=len(hard_block_candidates),
    )

    approved_total = sum(1 for intent in intents if intent.policy_approved)
    approval_rate = round(float(approved_total / attempts_total), 6) if attempts_total > 0 else 0.0
    expected_edge_sum = round(
        float(sum(float(intent.expected_edge_net) for intent in intents if isinstance(intent.expected_edge_net, float))),
        6,
    )
    expected_edge_mean = (
        round(expected_edge_sum / attempts_total, 6) if attempts_total > 0 else None
    )
    realized_pnl_sum = round(
        float(sum(float(row.realized_pnl_dollars) for row in realized_rows if isinstance(row.realized_pnl_dollars, float))),
        6,
    )
    realized_trade_count = sum(1 for row in realized_rows if isinstance(row.realized_pnl_dollars, float))
    realized_per_trade = (
        round(realized_pnl_sum / realized_trade_count, 6)
        if realized_trade_count > 0
        else None
    )
    edge_realization_ratio = (
        round(realized_pnl_sum / expected_edge_sum, 6)
        if abs(expected_edge_sum) > 1e-12 and realized_trade_count > 0
        else None
    )
    metar_age_bucket_counts = Counter(_metar_age_bucket(intent.metar_age_minutes) for intent in intents)
    forecast_status_counts = Counter(intent.forecast_status for intent in intents)

    payload = {
        "status": "ready",
        "captured_at": now_utc.isoformat(),
        "output_dir": str(out_dir),
        "health_dir": str(health_dir),
        "inputs": {
            "window_hours": round(float(safe_window_hours), 3),
            "min_bucket_samples": int(safe_min_bucket_samples),
            "max_profile_age_hours": round(float(safe_max_profile_age_hours), 3),
        },
        "sources": {
            "intents": intent_source,
            "profitability": realized_source,
        },
        "weather_pattern_profile": weather_pattern_profile,
        "overall": {
            "attempts_total": int(attempts_total),
            "approved_total": int(approved_total),
            "approval_rate": approval_rate,
            "expected_edge_sum": expected_edge_sum,
            "expected_edge_mean": expected_edge_mean,
            "realized_pnl_sum": realized_pnl_sum if realized_trade_count > 0 else None,
            "realized_trade_count": int(realized_trade_count),
            "realized_per_trade": realized_per_trade,
            "edge_realization_ratio": edge_realization_ratio,
            "stale_negative_station_max_share": concentration_metrics["stale_negative_station_max_share"],
            "stale_negative_station_hhi": concentration_metrics["stale_negative_station_hhi"],
            "stale_negative_station_top": concentration_metrics["stale_negative_station_top"],
            "metar_age_bucket_counts": dict(sorted(metar_age_bucket_counts.items(), key=lambda item: item[0])),
            "forecast_status_counts": dict(sorted(forecast_status_counts.items(), key=lambda item: item[0])),
        },
        "profile": {
            "bucket_dimensions": bucket_dimensions,
            "negative_expectancy_buckets": negative_expectancy_buckets,
            "elevated_risk_buckets": elevated_risk_buckets,
            "recommendations": {
                "hard_block_candidates": hard_block_candidates,
                "threshold_raise_candidates": threshold_raise_candidates,
            },
            "regime_risk": {
                "negative_expectancy_bucket_count": len(negative_expectancy_buckets),
                "elevated_risk_bucket_count": len(elevated_risk_buckets),
                "hard_block_candidate_count": len(hard_block_candidates),
                "threshold_raise_candidate_count": len(threshold_raise_candidates),
                "negative_expectancy_attempt_share": concentration_metrics["negative_expectancy_attempt_share"],
                "stale_metar_negative_attempt_share": concentration_metrics["stale_metar_negative_attempt_share"],
                "stale_metar_attempt_share": concentration_metrics["stale_metar_attempt_share"],
                "negative_expectancy_attempt_share_confidence_adjusted": (
                    concentration_metrics["negative_expectancy_attempt_share_confidence_adjusted"]
                ),
                "stale_metar_negative_attempt_share_confidence_adjusted": (
                    concentration_metrics["stale_metar_negative_attempt_share_confidence_adjusted"]
                ),
                "stale_metar_attempt_share_confidence_adjusted": (
                    concentration_metrics["stale_metar_attempt_share_confidence_adjusted"]
                ),
                "stale_negative_station_attempts": concentration_metrics["stale_negative_station_attempts"],
                "stale_negative_station_max_share": concentration_metrics["stale_negative_station_max_share"],
                "stale_negative_station_hhi": concentration_metrics["stale_negative_station_hhi"],
                "stale_negative_station_top": concentration_metrics["stale_negative_station_top"],
                "concentration_confidence_adjustment_method": concentration_metrics["confidence_adjustment_method"],
                "concentration_confidence_adjustment_z_score": concentration_metrics["confidence_adjustment_z_score"],
            },
            "risk_off_recommendation": risk_off_recommendation,
        },
    }

    stamp = now_utc.strftime("%Y%m%d_%H%M%S")
    output_path = health_dir / f"kalshi_temperature_weather_pattern_{stamp}.json"
    latest_path = health_dir / "kalshi_temperature_weather_pattern_latest.json"
    payload["output_file"] = str(output_path)
    payload["latest_file"] = str(latest_path)
    text = json.dumps(payload, indent=2, sort_keys=True)
    _write_text_atomic(output_path, text)
    _write_text_atomic(latest_path, text)
    return payload


def summarize_kalshi_temperature_weather_pattern(
    *,
    output_dir: str,
    window_hours: float,
    min_bucket_samples: int,
    max_profile_age_hours: float,
) -> str:
    payload = run_kalshi_temperature_weather_pattern(
        output_dir=output_dir,
        window_hours=window_hours,
        min_bucket_samples=min_bucket_samples,
        max_profile_age_hours=max_profile_age_hours,
    )
    return json.dumps(payload, indent=2, sort_keys=True)
