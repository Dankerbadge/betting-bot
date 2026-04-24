from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _normalize_ratio(value: float | None) -> float | None:
    if not isinstance(value, float):
        return None
    if value < 0.0:
        return 0.0
    if value <= 1.0:
        return round(float(value), 6)
    if value <= 100.0:
        return round(float(value) / 100.0, 6)
    return None


def _latest_payload(output_dir: Path, patterns: tuple[str, ...]) -> tuple[dict[str, Any] | None, str]:
    for pattern in patterns:
        candidates = sorted(
            output_dir.glob(pattern),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                return payload, str(path)
    return None, ""


def _artifact_age_hours(path_text: str, *, now: datetime) -> float | None:
    if not path_text:
        return None
    path = Path(path_text)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    now_utc = now.astimezone(timezone.utc)
    age_hours = (now_utc.timestamp() - float(mtime)) / 3600.0
    return round(max(0.0, age_hours), 6)


def _load_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_timestamp(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    normalized = text
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _infer_settled_outcomes_from_profitability_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sections: list[tuple[str, dict[str, Any]]] = [("root", payload)]
    for section_name in (
        "headline_metrics",
        "profitability_overview",
        "prelive_calibration",
        "shadow_settled_reference",
        "counterfactual_live_from_selections",
        "realized_settled",
        "expected_vs_realized",
    ):
        section = payload.get(section_name)
        if isinstance(section, dict):
            sections.append((section_name, section))
    shadow_settled_reference = payload.get("shadow_settled_reference")
    if isinstance(shadow_settled_reference, dict):
        shadow_headline = shadow_settled_reference.get("headline")
        if isinstance(shadow_headline, dict):
            sections.append(("shadow_settled_reference.headline", shadow_headline))
    counterfactual_live = payload.get("counterfactual_live_from_selections")
    if isinstance(counterfactual_live, dict):
        counterfactual_headline = counterfactual_live.get("headline")
        if isinstance(counterfactual_headline, dict):
            sections.append(("counterfactual_live_from_selections.headline", counterfactual_headline))

    unique_market_side_keys = (
        "settled_unique_market_side_resolved_predictions",
        "resolved_unique_market_sides",
        "settled_unique_market_sides",
        "resolved_predictions",
        "resolved_planned_rows",
    )
    order_settlement_keys = (
        "orders_settled_with_numeric_pnl",
        "orders_settled",
        "resolved_order_instances",
        "resolved_planned_orders",
        "matched_settled_orders",
    )

    best_unique_value = 0
    best_unique_key = ""
    observed_unique_key = ""
    best_order_value = 0
    best_order_key = ""
    observed_order_key = ""
    for section_name, section in sections:
        for metric_key in unique_market_side_keys:
            if metric_key not in section:
                continue
            resolved_key = (
                f"{section_name}.{metric_key}" if section_name != "root" else metric_key
            )
            if not observed_unique_key:
                observed_unique_key = resolved_key
            metric_value = max(0, _parse_int(section.get(metric_key)))
            if metric_value > best_unique_value:
                best_unique_value = int(metric_value)
                best_unique_key = resolved_key
        for metric_key in order_settlement_keys:
            if metric_key not in section:
                continue
            resolved_key = (
                f"{section_name}.{metric_key}" if section_name != "root" else metric_key
            )
            if not observed_order_key:
                observed_order_key = resolved_key
            metric_value = max(0, _parse_int(section.get(metric_key)))
            if metric_value > best_order_value:
                best_order_value = int(metric_value)
                best_order_key = resolved_key

    if best_unique_value > 0:
        return {
            "settled_outcomes": int(best_unique_value),
            "metric_kind": "unique_market_side",
            "metric_key": best_unique_key or observed_unique_key,
        }
    if best_order_value > 0:
        return {
            "settled_outcomes": int(best_order_value),
            "metric_kind": "orders_settled",
            "metric_key": best_order_key or observed_order_key,
        }
    if observed_unique_key:
        return {
            "settled_outcomes": 0,
            "metric_kind": "unique_market_side",
            "metric_key": observed_unique_key,
        }
    if observed_order_key:
        return {
            "settled_outcomes": 0,
            "metric_kind": "orders_settled",
            "metric_key": observed_order_key,
        }
    return {
        "settled_outcomes": 0,
        "metric_kind": "missing",
        "metric_key": "",
    }


def _read_profitability_settled_outcomes_fallback(output_dir: Path, *, now: datetime) -> dict[str, Any]:
    profitability_patterns = (
        "health/kalshi_temperature_profitability_summary_latest.json",
        "health/kalshi_temperature_profitability_summary_*.json",
        "checkpoints/profitability_168h_latest.json",
        "checkpoints/profitability_168h_*.json",
        "checkpoints/profitability_24h_latest.json",
        "checkpoints/profitability_24h_*.json",
        "checkpoints/profitability_12h_latest.json",
        "checkpoints/profitability_12h_*.json",
        "checkpoints/profitability_4h_latest.json",
        "checkpoints/profitability_4h_*.json",
        "checkpoints/profitability_1h_latest.json",
        "checkpoints/profitability_1h_*.json",
        "checkpoints/profitability_*_latest.json",
        "checkpoints/profitability_*.json",
        "kalshi_temperature_profitability_summary_latest.json",
        "kalshi_temperature_profitability_summary_*.json",
    )

    def _mtime(path: Path) -> float:
        try:
            return float(path.stat().st_mtime)
        except OSError:
            return 0.0

    candidate_paths: list[Path] = []
    seen_paths: set[str] = set()
    for pattern in profitability_patterns:
        for path in sorted(output_dir.glob(pattern), key=_mtime, reverse=True):
            path_text = str(path)
            if path_text in seen_paths:
                continue
            seen_paths.add(path_text)
            candidate_paths.append(path)

    records: list[dict[str, Any]] = []
    for path in candidate_paths:
        payload = _load_json_file(path)
        if not payload:
            continue
        inferred = _infer_settled_outcomes_from_profitability_payload(payload)
        captured_at = (
            _parse_timestamp(payload.get("captured_at"))
            or _parse_timestamp(payload.get("captured_at_utc"))
            or _parse_timestamp(payload.get("window_end_utc"))
        )
        if captured_at is None:
            try:
                captured_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                continue
        records.append(
            {
                "path": str(path),
                "status": _normalize_text(payload.get("status")).lower() or "unknown",
                "captured_at": captured_at,
                "settled_outcomes": int(max(0, _parse_int(inferred.get("settled_outcomes")))),
                "metric_kind": _normalize_text(inferred.get("metric_kind")).lower() or "missing",
                "metric_key": _normalize_text(inferred.get("metric_key")),
            }
        )

    if not records:
        return {
            "source": "",
            "status": "missing",
            "age_hours": None,
            "captured_at": None,
            "settled_outcomes": 0,
            "settled_outcomes_metric_kind": "missing",
            "settled_outcomes_metric_key": "",
            "trend_delta_24h": None,
            "trend_delta_7d": None,
            "trend_baseline_24h_source": "",
            "trend_baseline_24h_settled_outcomes": None,
            "trend_baseline_7d_source": "",
            "trend_baseline_7d_settled_outcomes": None,
            "artifacts_considered": int(len(candidate_paths)),
            "artifacts_parsed": 0,
        }

    records.sort(key=lambda item: item["captured_at"])
    latest_record = records[-1]

    def _trend_delta(window_hours: float) -> tuple[int | None, dict[str, Any] | None]:
        cutoff = latest_record["captured_at"] - timedelta(hours=float(window_hours))
        baseline: dict[str, Any] | None = None
        for record in records:
            if record["captured_at"] <= cutoff:
                baseline = record
            else:
                break
        if baseline is None:
            return None, None
        return int(latest_record["settled_outcomes"]) - int(baseline["settled_outcomes"]), baseline

    trend_delta_24h, baseline_24h = _trend_delta(24.0)
    trend_delta_7d, baseline_7d = _trend_delta(168.0)

    latest_source = _normalize_text(latest_record.get("path"))
    return {
        "source": latest_source,
        "status": _normalize_text(latest_record.get("status")).lower() or "unknown",
        "age_hours": _artifact_age_hours(latest_source, now=now),
        "captured_at": latest_record["captured_at"].isoformat(),
        "settled_outcomes": int(latest_record["settled_outcomes"]),
        "settled_outcomes_metric_kind": _normalize_text(latest_record.get("metric_kind")).lower() or "missing",
        "settled_outcomes_metric_key": _normalize_text(latest_record.get("metric_key")),
        "trend_delta_24h": int(trend_delta_24h) if isinstance(trend_delta_24h, int) else None,
        "trend_delta_7d": int(trend_delta_7d) if isinstance(trend_delta_7d, int) else None,
        "trend_baseline_24h_source": _normalize_text(baseline_24h.get("path")) if isinstance(baseline_24h, dict) else "",
        "trend_baseline_24h_settled_outcomes": (
            int(baseline_24h["settled_outcomes"]) if isinstance(baseline_24h, dict) else None
        ),
        "trend_baseline_7d_source": _normalize_text(baseline_7d.get("path")) if isinstance(baseline_7d, dict) else "",
        "trend_baseline_7d_settled_outcomes": (
            int(baseline_7d["settled_outcomes"]) if isinstance(baseline_7d, dict) else None
        ),
        "artifacts_considered": int(len(candidate_paths)),
        "artifacts_parsed": int(len(records)),
    }


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


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


def _extract_settled_outcome_count(payload: dict[str, Any]) -> tuple[int, bool]:
    if not isinstance(payload, dict):
        return 0, False

    sections: list[dict[str, Any]] = [payload]
    for key in (
        "headline_metrics",
        "shadow_settled_reference",
        "expected_vs_realized",
        "opportunity_breadth",
        "profitability_overview",
        "summary",
        "metrics",
        "overview",
    ):
        section = payload.get(key)
        if isinstance(section, dict):
            sections.append(section)
    profitability_overview = payload.get("profitability_overview")
    if isinstance(profitability_overview, dict):
        for key in ("shadow_settled_reference", "expected_vs_realized", "summary", "metrics", "overview"):
            section = profitability_overview.get(key)
            if isinstance(section, dict):
                sections.append(section)

    preferred_candidates = (
        "settled_unique_market_side_resolved_predictions",
        "settled_unique_market_side_total",
        "resolved_unique_market_sides",
    )
    fallback_candidates = (
        "orders_settled_with_numeric_pnl",
        "orders_settled",
        "matched_settled_orders",
    )

    for section in sections:
        for candidate in preferred_candidates:
            parsed = _parse_optional_int(section.get(candidate))
            if isinstance(parsed, int):
                return max(0, parsed), True

    fallback_best = 0
    fallback_found = False
    for section in sections:
        for candidate in fallback_candidates:
            parsed = _parse_optional_int(section.get(candidate))
            if not isinstance(parsed, int):
                continue
            fallback_found = True
            fallback_best = max(fallback_best, max(0, parsed))
    return fallback_best, fallback_found


def _read_settled_outcome_backfill(output_dir: Path, *, now: datetime) -> dict[str, Any]:
    patterns = (
        "kalshi_temperature_profitability_summary_*.json",
        "checkpoints/profitability_14h_latest.json",
        "checkpoints/profitability_14h_*.json",
        "checkpoints/profitability_168h_latest.json",
        "checkpoints/profitability_168h_*.json",
        "checkpoints/profitability_*.json",
    )
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(path for path in output_dir.glob(pattern) if path.is_file())
    if not candidates:
        return {
            "status": "missing",
            "source": "",
            "age_hours": None,
            "settled_outcomes": 0,
            "available": False,
            "sample_points": 0,
            "settled_outcomes_delta_24h": None,
            "settled_outcomes_delta_7d": None,
        }

    rows: list[dict[str, Any]] = []
    for path in sorted(candidates, key=lambda candidate: candidate.stat().st_mtime, reverse=True):
        payload = _load_json_file(path)
        settled_outcomes, found = _extract_settled_outcome_count(payload)
        if not found:
            continue
        captured_at = (
            _parse_iso_utc(payload.get("captured_at"))
            or _parse_iso_utc(payload.get("captured_at_utc"))
            or _parse_iso_utc(payload.get("window_end_utc"))
        )
        if captured_at is None:
            try:
                captured_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                captured_at = now
        rows.append(
            {
                "path": str(path),
                "captured_at": captured_at,
                "settled_outcomes": max(0, int(settled_outcomes)),
            }
        )
    if not rows:
        return {
            "status": "missing",
            "source": "",
            "age_hours": None,
            "settled_outcomes": 0,
            "available": False,
            "sample_points": 0,
            "settled_outcomes_delta_24h": None,
            "settled_outcomes_delta_7d": None,
        }

    rows = sorted(rows, key=lambda row: row["captured_at"], reverse=True)
    latest = rows[0]
    latest_captured_at = latest["captured_at"]
    age_hours = round(
        max(0.0, (now - latest_captured_at).total_seconds() / 3600.0),
        6,
    )

    baseline_24h = next(
        (
            row
            for row in rows[1:]
            if (latest_captured_at - row["captured_at"]).total_seconds() >= 24.0 * 3600.0
        ),
        None,
    )
    baseline_7d = next(
        (
            row
            for row in rows[1:]
            if (latest_captured_at - row["captured_at"]).total_seconds() >= 7.0 * 24.0 * 3600.0
        ),
        None,
    )

    settled_outcomes_delta_24h = (
        int(latest["settled_outcomes"]) - int(baseline_24h["settled_outcomes"])
        if isinstance(baseline_24h, dict)
        else None
    )
    settled_outcomes_delta_7d = (
        int(latest["settled_outcomes"]) - int(baseline_7d["settled_outcomes"])
        if isinstance(baseline_7d, dict)
        else None
    )

    return {
        "status": "ready",
        "source": str(latest["path"]),
        "age_hours": age_hours,
        "settled_outcomes": int(latest["settled_outcomes"]),
        "available": True,
        "sample_points": int(len(rows)),
        "settled_outcomes_delta_24h": settled_outcomes_delta_24h,
        "settled_outcomes_delta_7d": settled_outcomes_delta_7d,
        "baseline_24h_source": str(baseline_24h["path"]) if isinstance(baseline_24h, dict) else "",
        "baseline_7d_source": str(baseline_7d["path"]) if isinstance(baseline_7d, dict) else "",
        "baseline_24h_settled_outcomes": (
            int(baseline_24h["settled_outcomes"]) if isinstance(baseline_24h, dict) else None
        ),
        "baseline_7d_settled_outcomes": (
            int(baseline_7d["settled_outcomes"]) if isinstance(baseline_7d, dict) else None
        ),
    }


def _read_settled_outcome_throughput_artifact(output_dir: Path, *, now: datetime) -> dict[str, Any]:
    payload, source_file = _latest_payload(
        output_dir,
        (
            "health/kalshi_temperature_settled_outcome_throughput_latest.json",
            "health/kalshi_temperature_settled_outcome_throughput_*.json",
        ),
    )
    if not isinstance(payload, dict):
        return {
            "source": "",
            "status": "missing",
            "age_hours": None,
            "coverage_settled_outcomes": None,
            "growth_deltas_settled_outcomes_delta_24h": None,
            "growth_deltas_settled_outcomes_delta_7d": None,
            "growth_deltas_combined_bucket_count_delta_24h": None,
            "growth_deltas_combined_bucket_count_delta_7d": None,
            "targeting_targeted_constraint_rows": None,
            "top_bottlenecks_count": 0,
            "bottleneck_source": "",
        }

    coverage = payload.get("coverage")
    coverage = dict(coverage) if isinstance(coverage, dict) else {}
    growth_deltas = payload.get("growth_deltas")
    growth_deltas = dict(growth_deltas) if isinstance(growth_deltas, dict) else {}
    targeting = payload.get("targeting")
    targeting = dict(targeting) if isinstance(targeting, dict) else {}
    top_bottlenecks = payload.get("top_bottlenecks")
    top_bottlenecks_count = len(top_bottlenecks) if isinstance(top_bottlenecks, list) else _parse_int(
        payload.get("top_bottlenecks_count")
    )

    return {
        "source": source_file,
        "status": _normalize_text(payload.get("status")).lower() or "unknown",
        "age_hours": _artifact_age_hours(source_file, now=now),
        "coverage_settled_outcomes": _parse_optional_int(
            coverage.get("settled_outcomes")
            if "settled_outcomes" in coverage
            else payload.get("coverage_settled_outcomes")
        ),
        "growth_deltas_settled_outcomes_delta_24h": _parse_optional_int(
            growth_deltas.get("settled_outcomes_delta_24h")
            if "settled_outcomes_delta_24h" in growth_deltas
            else payload.get("settled_outcomes_delta_24h")
        ),
        "growth_deltas_settled_outcomes_delta_7d": _parse_optional_int(
            growth_deltas.get("settled_outcomes_delta_7d")
            if "settled_outcomes_delta_7d" in growth_deltas
            else payload.get("settled_outcomes_delta_7d")
        ),
        "growth_deltas_combined_bucket_count_delta_24h": _parse_optional_int(
            growth_deltas.get("combined_bucket_count_delta_24h")
            if "combined_bucket_count_delta_24h" in growth_deltas
            else payload.get("combined_bucket_count_delta_24h")
        ),
        "growth_deltas_combined_bucket_count_delta_7d": _parse_optional_int(
            growth_deltas.get("combined_bucket_count_delta_7d")
            if "combined_bucket_count_delta_7d" in growth_deltas
            else payload.get("combined_bucket_count_delta_7d")
        ),
        "targeting_targeted_constraint_rows": _parse_optional_int(
            targeting.get("targeted_constraint_rows")
            if "targeted_constraint_rows" in targeting
            else payload.get("targeted_constraint_rows")
        ),
        "top_bottlenecks_count": int(max(0, top_bottlenecks_count)),
        "bottleneck_source": _normalize_text(
            payload.get("bottleneck_source") or targeting.get("bottleneck_source")
        ),
    }


def _read_coverage_velocity_state(output_dir: Path, *, now: datetime) -> dict[str, Any]:
    payload, source_file = _latest_payload(
        output_dir,
        (
            "health/decision_matrix_coverage_velocity_state_latest.json",
            "health/decision_matrix_coverage_velocity_state_*.json",
        ),
    )
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


def _read_weather_pattern_artifact(output_dir: Path, *, now: datetime) -> dict[str, Any]:
    payload, source_file = _latest_payload(
        output_dir,
        (
            "health/kalshi_temperature_weather_pattern_latest.json",
            "health/kalshi_temperature_weather_pattern_*.json",
        ),
    )
    source_status = "missing"
    age_hours = None
    negative_expectancy_regime_concentration = None
    negative_expectancy_regime_concentration_source = "missing"
    negative_expectancy_regime_count = 0
    negative_expectancy_regime_total = 0
    weather_bucket_coverage_ratio = None
    weather_bucket_coverage_count = 0
    weather_bucket_total = 0
    metar_observation_stale_share = None
    metar_observation_stale_share_source = "missing"
    metar_observation_stale_count = 0
    weather_risk_off_sample_count = 0
    if source_file:
        age_hours = _artifact_age_hours(source_file, now=now)
    if isinstance(payload, dict):
        source_status = _normalize_text(payload.get("status")).lower() or "unknown"
        sections: list[dict[str, Any]] = [payload]
        for key in (
            "headline_metrics",
            "summary",
            "metrics",
            "regime_summary",
            "weather_regime",
            "coverage_summary",
            "pattern_summary",
            "overall",
            "risk_summary",
            "risk_profile",
        ):
            section = payload.get(key)
            if isinstance(section, dict):
                sections.append(section)
        profile_section = payload.get("profile")
        if isinstance(profile_section, dict):
            sections.append(profile_section)
            regime_risk = profile_section.get("regime_risk")
            if isinstance(regime_risk, dict):
                sections.append(regime_risk)
            risk_off_recommendation = profile_section.get("risk_off_recommendation")
            if isinstance(risk_off_recommendation, dict):
                sections.append(risk_off_recommendation)
        weather_pattern_profile = payload.get("weather_pattern_profile")
        if isinstance(weather_pattern_profile, dict):
            sections.append(weather_pattern_profile)
            weather_pattern_regime = weather_pattern_profile.get("regime_risk")
            if isinstance(weather_pattern_regime, dict):
                sections.append(weather_pattern_regime)
            weather_pattern_risk_off = weather_pattern_profile.get("risk_off_recommendation")
            if isinstance(weather_pattern_risk_off, dict):
                sections.append(weather_pattern_risk_off)

        def _first_float(candidates: tuple[str, ...]) -> float | None:
            for section in sections:
                for candidate in candidates:
                    parsed = _parse_float(section.get(candidate))
                    if isinstance(parsed, float):
                        return parsed
            return None

        def _first_int(candidates: tuple[str, ...]) -> int:
            for section in sections:
                for candidate in candidates:
                    value = section.get(candidate)
                    try:
                        parsed = int(float(value))
                    except (TypeError, ValueError):
                        continue
                    return parsed
            return 0

        confidence_adjusted_negative_expectancy_regime_concentration = _first_float(
            (
                "negative_expectancy_attempt_share_confidence_adjusted",
                "negative_expectancy_regime_concentration_confidence_adjusted",
                "negative_expectancy_share_confidence_adjusted",
                "negative_expectancy_attempt_share_effective",
                "negative_expectancy_regime_concentration_effective",
            )
        )
        legacy_negative_expectancy_regime_concentration = _first_float(
            (
                "negative_expectancy_regime_concentration",
                "negative_expectancy_regime_share",
                "negative_expectancy_share",
                "regime_negative_expectancy_concentration",
                "regime_negative_expectancy_share",
            )
        )
        negative_expectancy_regime_concentration = (
            confidence_adjusted_negative_expectancy_regime_concentration
            if isinstance(confidence_adjusted_negative_expectancy_regime_concentration, float)
            else legacy_negative_expectancy_regime_concentration
        )
        if isinstance(confidence_adjusted_negative_expectancy_regime_concentration, float):
            negative_expectancy_regime_concentration_source = "confidence_adjusted"
        elif isinstance(legacy_negative_expectancy_regime_concentration, float):
            negative_expectancy_regime_concentration_source = "raw"
        weather_bucket_coverage_ratio = _first_float(
            (
                "weather_bucket_coverage_ratio",
                "bucket_coverage_ratio",
                "coverage_ratio",
                "regime_bucket_coverage_ratio",
            )
        )
        weather_bucket_coverage_count = _first_int(
            (
                "weather_bucket_coverage_count",
                "bucket_coverage_count",
                "covered_bucket_count",
                "coverage_bucket_count",
            )
        )
        weather_bucket_total = _first_int(
            (
                "weather_bucket_total",
                "weather_bucket_count",
                "bucket_total",
                "bucket_count",
                "total_bucket_count",
            )
        )
        negative_expectancy_regime_count = _first_int(
            (
                "negative_expectancy_regime_count",
                "negative_expectancy_count",
                "regime_negative_expectancy_count",
            )
        )
        negative_expectancy_regime_total = _first_int(
            (
                "negative_expectancy_regime_total",
                "negative_expectancy_total",
                "regime_negative_expectancy_total",
                "weather_regime_total",
                "regime_total",
                "total_regime_count",
            )
        )
        confidence_adjusted_metar_observation_stale_share = _normalize_ratio(
            _first_float(
                (
                    "stale_metar_attempt_share_confidence_adjusted",
                    "metar_observation_stale_share_confidence_adjusted",
                    "stale_metar_share_confidence_adjusted",
                    "stale_metar_attempt_share_effective",
                    "metar_observation_stale_share_effective",
                )
            )
        )
        legacy_metar_observation_stale_share = _normalize_ratio(
            _first_float(
                (
                    "metar_observation_stale_share",
                    "metar_observation_stale_rate",
                    "stale_metar_share",
                    "stale_metar_ratio",
                    "metar_stale_share",
                    "metar_stale_rate",
                )
            )
        )
        metar_observation_stale_share = (
            confidence_adjusted_metar_observation_stale_share
            if isinstance(confidence_adjusted_metar_observation_stale_share, float)
            else legacy_metar_observation_stale_share
        )
        if isinstance(confidence_adjusted_metar_observation_stale_share, float):
            metar_observation_stale_share_source = "confidence_adjusted"
        elif isinstance(legacy_metar_observation_stale_share, float):
            metar_observation_stale_share_source = "raw"
        metar_observation_stale_count = _first_int(
            (
                "metar_observation_stale_count",
                "stale_metar_count",
                "stale_metar_attempts",
                "metar_stale_count",
            )
        )
        weather_risk_off_sample_count = _first_int(
            (
                "weather_risk_off_sample_count",
                "risk_off_sample_count",
                "attempts_total",
                "weather_attempts_total",
                "sample_count",
                "sample_size",
                "observations_total",
                "total_attempts",
                "attempts_considered",
            )
        )
        if not isinstance(weather_bucket_coverage_ratio, float) and weather_bucket_total > 0:
            weather_bucket_coverage_ratio = round(
                float(weather_bucket_coverage_count) / float(weather_bucket_total),
                6,
            )
        if not isinstance(negative_expectancy_regime_concentration, float) and negative_expectancy_regime_total > 0:
            negative_expectancy_regime_concentration = round(
                float(negative_expectancy_regime_count) / float(negative_expectancy_regime_total),
                6,
            )

        # Normalize newer weather-pattern payload shape where bucket metrics are nested
        # under profile.bucket_dimensions and profile.regime_risk.
        profile = payload.get("profile")
        if isinstance(profile, dict):
            bucket_dimensions = profile.get("bucket_dimensions")
            flattened_buckets: list[dict[str, Any]] = []
            if isinstance(bucket_dimensions, dict):
                for dimension_rows in bucket_dimensions.values():
                    if not isinstance(dimension_rows, dict):
                        continue
                    for row in dimension_rows.values():
                        if isinstance(row, dict):
                            flattened_buckets.append(row)

                signal_rows = bucket_dimensions.get("signal_type")
                signal_total_attempts = 0
                signal_stale_attempts = 0
                stale_bucket_present = False
                if isinstance(signal_rows, dict):
                    for bucket_name, row in signal_rows.items():
                        if not isinstance(row, dict):
                            continue
                        attempts = max(0, _parse_int(row.get("attempts")))
                        signal_total_attempts += attempts
                        bucket_key = _coerce_reason(str(bucket_name))
                        signal_key = _coerce_reason(str(row.get("signal_type") or row.get("policy_reason")))
                        if bucket_key == "metar_observation_stale" or signal_key == "metar_observation_stale":
                            stale_bucket_present = True
                            signal_stale_attempts += attempts
                if not weather_risk_off_sample_count and signal_total_attempts > 0:
                    weather_risk_off_sample_count = int(signal_total_attempts)
                if not metar_observation_stale_count and stale_bucket_present:
                    metar_observation_stale_count = int(signal_stale_attempts)

            if not weather_bucket_total and flattened_buckets:
                weather_bucket_total = len(flattened_buckets)
            if not weather_bucket_coverage_count and flattened_buckets:
                covered = sum(1 for row in flattened_buckets if bool(row.get("sample_ok")))
                weather_bucket_coverage_count = int(covered)
            if (
                not isinstance(weather_bucket_coverage_ratio, float)
                and weather_bucket_total > 0
                and weather_bucket_coverage_count >= 0
            ):
                weather_bucket_coverage_ratio = round(
                    float(weather_bucket_coverage_count) / float(weather_bucket_total),
                    6,
                )

            negative_rows = profile.get("negative_expectancy_buckets")
            negative_buckets = [row for row in negative_rows if isinstance(row, dict)] if isinstance(negative_rows, list) else []
            if not negative_expectancy_regime_count and negative_buckets:
                negative_expectancy_regime_count = len(negative_buckets)
            if not negative_expectancy_regime_total and weather_bucket_total > 0:
                negative_expectancy_regime_total = int(weather_bucket_total)
            if not isinstance(negative_expectancy_regime_concentration, float):
                total_attempts = sum(
                    int(_parse_float(row.get("attempts")) or 0)
                    for row in flattened_buckets
                )
                negative_attempts = sum(
                    int(_parse_float(row.get("attempts")) or 0)
                    for row in negative_buckets
                )
                if total_attempts > 0:
                    negative_expectancy_regime_concentration = round(
                        float(negative_attempts) / float(total_attempts),
                        6,
                    )
                    if negative_expectancy_regime_concentration_source == "missing":
                        negative_expectancy_regime_concentration_source = "derived"
                elif negative_expectancy_regime_total > 0:
                    negative_expectancy_regime_concentration = round(
                        float(negative_expectancy_regime_count) / float(negative_expectancy_regime_total),
                        6,
                    )
                    if negative_expectancy_regime_concentration_source == "missing":
                        negative_expectancy_regime_concentration_source = "derived"

        if (
            not isinstance(metar_observation_stale_share, float)
            and weather_risk_off_sample_count > 0
            and metar_observation_stale_count >= 0
        ):
            metar_observation_stale_share = round(
                float(metar_observation_stale_count) / float(weather_risk_off_sample_count),
                6,
            )
            if metar_observation_stale_share_source == "missing":
                metar_observation_stale_share_source = "derived"

    return {
        "source": source_file,
        "status": source_status,
        "age_hours": age_hours,
        "negative_expectancy_regime_concentration": negative_expectancy_regime_concentration,
        "negative_expectancy_regime_concentration_source": negative_expectancy_regime_concentration_source,
        "negative_expectancy_regime_count": negative_expectancy_regime_count,
        "negative_expectancy_regime_total": negative_expectancy_regime_total,
        "weather_bucket_coverage_ratio": weather_bucket_coverage_ratio,
        "weather_bucket_coverage_count": weather_bucket_coverage_count,
        "weather_bucket_total": weather_bucket_total,
        "metar_observation_stale_share": metar_observation_stale_share,
        "metar_observation_stale_share_source": metar_observation_stale_share_source,
        "metar_observation_stale_count": metar_observation_stale_count,
        "weather_risk_off_sample_count": weather_risk_off_sample_count,
    }


def _coerce_reason(reason: str) -> str:
    return _normalize_text(reason).lower().replace(" ", "_")


def _append_blocker(
    *,
    rows: list[dict[str, Any]],
    key: str,
    severity: str,
    summary: str,
    recommendation: str,
    observed_value: Any = None,
    threshold: Any = None,
) -> None:
    rows.append(
        {
            "key": key,
            "severity": severity,
            "summary": summary,
            "recommendation": recommendation,
            "observed_value": observed_value,
            "threshold": threshold,
        }
    )


def _build_pipeline_backlog(blockers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = {_normalize_text(row.get("key")) for row in blockers}
    backlog: list[dict[str, Any]] = []
    if "blocker_concentration_high" in keys or "expected_edge_gate_dominance" in keys:
        backlog.append(
            {
                "id": "execution_cost_tape",
                "priority": "P0",
                "title": "Execution Cost Tape",
                "objective": "Collect spread/slippage/fill realism telemetry by ticker-hour to recalibrate expected-edge floors.",
                "data_sources": [
                    "kalshi_ws_state_latest.json",
                    "kalshi_temperature_trade_intents_summary_*.json",
                    "kalshi_temperature_shadow_watch_summary_*.json",
                ],
                "success_metric": "expected_edge_below_min share falls below 45% of blocked flow over 7d.",
            }
        )
    if "historical_quality_hard_block_dominance" in keys:
        backlog.append(
            {
                "id": "bucket_settlement_attribution",
                "priority": "P0",
                "title": "Bucket Settlement Attribution",
                "objective": "Expand bucket-backed settled evidence by station-hour-strike to reduce generic hard blocks.",
                "data_sources": [
                    "profitability_*.json",
                    "station_tuning_window_*.json",
                    "kalshi_temperature_bankroll_validation_*.json",
                ],
                "success_metric": "historical quality hard-block share drops below 20% of blocked flow over 14d.",
            }
        )
    if "insufficient_settled_outcomes" in keys:
        backlog.append(
            {
                "id": "independent_breadth_sampler",
                "priority": "P0",
                "title": "Independent Breadth Sampler",
                "objective": "Increase independent settled outcomes across underlying families/hours before scaling risk.",
                "data_sources": [
                    "kalshi_temperature_trade_intents_summary_*.json",
                    "profitability_*.json",
                ],
                "success_metric": "settled unique market-side outcomes exceed configured minimum in rolling window.",
            }
        )
    if "settled_outcome_growth_stalled" in keys:
        backlog.append(
            {
                "id": "settled_outcome_growth_recovery",
                "priority": "P0",
                "title": "Settled Outcome Growth Recovery",
                "objective": "Recover settled-outcome growth momentum so low-coverage hardening can clear with positive trend evidence.",
                "data_sources": [
                    "health/alpha_summary_latest.json",
                    "kalshi_temperature_profitability_summary_*.json",
                    "checkpoints/profitability_*.json",
                ],
                "success_metric": "settled outcomes remain below threshold only briefly, with positive 24h and 7d deltas before crossing minimum.",
            }
        )
    if "coverage_velocity_guardrail_not_cleared" in keys:
        backlog.append(
            {
                "id": "coverage_velocity_recovery",
                "priority": "P0",
                "title": "Coverage Velocity Recovery",
                "objective": "Sustain consecutive positive settled-outcome growth runs until the velocity guardrail clears.",
                "data_sources": [
                    "health/decision_matrix_coverage_velocity_state_latest.json",
                    "health/kalshi_temperature_settled_outcome_throughput_latest.json",
                    "kalshi_temperature_profitability_summary_*.json",
                ],
                "success_metric": "positive settled-outcome growth streak reaches the required threshold and guardrail clears.",
            }
        )
    if "approval_rate_below_floor" in keys or "quality_drift_alert_active" in keys:
        backlog.append(
            {
                "id": "gate_false_negative_review",
                "priority": "P1",
                "title": "Gate False-Negative Review",
                "objective": "Audit rejected intents near threshold boundaries and tighten/relax gate floors with evidence.",
                "data_sources": [
                    "kalshi_temperature_trade_intents_summary_*.json",
                    "blocker_audit_*_latest.json",
                    "alpha_summary_latest.json",
                ],
                "success_metric": "approval rate returns to guardrail band without rising quality-risk alerts.",
            }
        )
    if "projected_pnl_non_positive" in keys:
        backlog.append(
            {
                "id": "profitability_calibration_loop",
                "priority": "P1",
                "title": "Profitability Calibration Loop",
                "objective": "Track projected-vs-settled drift and re-weight model terms where projections stay flat/negative.",
                "data_sources": [
                    "alpha_summary_latest.json",
                    "profitability_*.json",
                    "kalshi_temperature_bankroll_validation_*.json",
                ],
                "success_metric": "projected PnL stays positive with improving settled attribution reliability.",
            }
        )
    if "missing_weather_pattern_artifact" in keys or "stale_weather_pattern_artifact" in keys:
        backlog.append(
            {
                "id": "weather_pattern_refresh",
                "priority": "P0",
                "title": "Weather Pattern Refresh",
                "objective": "Publish a fresh weather-pattern artifact with regime and bucket coverage metrics so hardening can gate on current weather evidence.",
                "data_sources": [
                    "health/kalshi_temperature_weather_pattern_latest.json",
                    "health/kalshi_temperature_weather_pattern_*.json",
                    "health/alpha_summary_latest.json",
                ],
                "success_metric": "weather pattern artifact is present, ready, and within freshness budget.",
            }
        )
    if "weather_negative_expectancy_regime_concentration_high" in keys:
        backlog.append(
            {
                "id": "weather_regime_deconcentration",
                "priority": "P1",
                "title": "Weather Regime Deconcentration",
                "objective": "Rebucket weather regimes so negative expectancy is not concentrated in a narrow regime slice.",
                "data_sources": [
                    "health/kalshi_temperature_weather_pattern_latest.json",
                    "health/kalshi_temperature_weather_pattern_*.json",
                    "kalshi_temperature_bankroll_validation_*.json",
                ],
                "success_metric": "negative expectancy concentration falls below the hardening threshold.",
            }
        )
    if "weather_bucket_coverage_insufficient" in keys:
        backlog.append(
            {
                "id": "weather_bucket_coverage_expansion",
                "priority": "P1",
                "title": "Weather Bucket Coverage Expansion",
                "objective": "Expand weather-bucket coverage so the matrix can rely on broader regime-level evidence.",
                "data_sources": [
                    "health/kalshi_temperature_weather_pattern_latest.json",
                    "health/kalshi_temperature_weather_pattern_*.json",
                    "kalshi_temperature_bankroll_validation_*.json",
                ],
                "success_metric": "weather bucket coverage exceeds the minimum hardening threshold.",
            }
        )
    if "weather_confidence_adjusted_signal_fallback_persistent" in keys:
        backlog.append(
            {
                "id": "weather_confidence_adjusted_signal_repair",
                "priority": "P0",
                "title": "Weather Confidence Signal Repair",
                "objective": "Restore confidence-adjusted weather regime metrics so hardening does not rely on repeated raw fallbacks.",
                "data_sources": [
                    "health/kalshi_temperature_weather_pattern_latest.json",
                    "health/kalshi_temperature_weather_pattern_*.json",
                    "health/decision_matrix_weather_confidence_state_latest.json",
                ],
                "success_metric": "confidence-adjusted weather shares are available and raw fallback streak resets to zero.",
            }
        )
    return backlog


def _assess_bootstrap_progression(
    *,
    blocker_keys: set[str],
    data_gaps: list[str],
    critical_count: int,
    largest_blocker_share: float,
    approval_rate: float,
    intents_total: int,
    min_intents_sample: int,
    sparse_edge_block_share: float | None,
    settled_outcomes: int,
    min_settled_outcomes: int,
    bootstrap_max_top_blocker_share: float,
    bootstrap_min_approval_rate: float,
    bootstrap_max_sparse_edge_block_share: float,
    bootstrap_max_critical_blockers: int,
    weather_risk_off_recommended: bool,
) -> dict[str, Any]:
    allowed_blocker_keys = {
        "approval_rate_below_floor",
        "blocker_concentration_high",
        "execution_cost_calibration_not_ready",
        "expected_edge_gate_dominance",
        "insufficient_settled_outcomes",
        "projected_pnl_non_positive",
        "quality_drift_alert_active",
        "sparse_edge_block_share_high",
    }
    disallowed_blocker_keys = sorted(
        key for key in blocker_keys if key and key not in allowed_blocker_keys
    )

    reasons: list[str] = []
    if data_gaps:
        reasons.append("data_pipeline_gaps_present")
    if "insufficient_settled_outcomes" not in blocker_keys:
        reasons.append("settled_outcomes_not_primary_bootstrap_constraint")
    if settled_outcomes >= int(min_settled_outcomes):
        reasons.append("settled_outcomes_threshold_already_met")
    if disallowed_blocker_keys:
        reasons.append("disallowed_blockers_present")
    if critical_count > int(max(0, bootstrap_max_critical_blockers)):
        reasons.append("critical_blocker_count_above_bootstrap_cap")

    safe_bootstrap_top_share = float(max(0.0, min(1.0, float(bootstrap_max_top_blocker_share))))
    if float(largest_blocker_share) > safe_bootstrap_top_share:
        reasons.append("top_blocker_share_above_bootstrap_cap")

    safe_bootstrap_min_approval = float(max(0.0, min(1.0, float(bootstrap_min_approval_rate))))
    if intents_total >= int(max(1, min_intents_sample)) and approval_rate < safe_bootstrap_min_approval:
        reasons.append("approval_rate_below_bootstrap_floor")

    safe_bootstrap_sparse_share = float(max(0.0, min(1.0, float(bootstrap_max_sparse_edge_block_share))))
    if isinstance(sparse_edge_block_share, float) and sparse_edge_block_share > safe_bootstrap_sparse_share:
        reasons.append("sparse_edge_block_share_above_bootstrap_cap")
    if weather_risk_off_recommended:
        reasons.append("weather_risk_off_recommended")

    supports_bootstrap_progression = not reasons
    return {
        "status": "ready" if supports_bootstrap_progression else "blocked",
        "supports_bootstrap_progression": supports_bootstrap_progression,
        "allowed_blocker_keys": sorted(allowed_blocker_keys),
        "disallowed_blocker_keys": disallowed_blocker_keys,
        "reasons": reasons,
        "constraints": {
            "bootstrap_max_critical_blockers": int(max(0, bootstrap_max_critical_blockers)),
            "bootstrap_max_top_blocker_share": round(float(safe_bootstrap_top_share), 6),
            "bootstrap_min_approval_rate": round(float(safe_bootstrap_min_approval), 6),
            "bootstrap_max_sparse_edge_block_share": round(float(safe_bootstrap_sparse_share), 6),
            "bootstrap_requires_settled_outcomes_below_threshold": True,
        },
        "observed": {
            "critical_blockers_count": int(critical_count),
            "largest_blocker_share_of_blocked": round(float(largest_blocker_share), 6),
            "approval_rate": round(float(approval_rate), 6),
            "intents_total": int(intents_total),
            "sparse_hardening_expected_edge_block_share": (
                round(float(sparse_edge_block_share), 6) if isinstance(sparse_edge_block_share, float) else None
            ),
            "settled_outcomes": int(settled_outcomes),
            "min_settled_outcomes": int(min_settled_outcomes),
            "data_pipeline_gaps_count": int(len(data_gaps)),
            "weather_risk_off_recommended": bool(weather_risk_off_recommended),
        },
    }


def run_decision_matrix_hardening(
    *,
    output_dir: str = "outputs",
    window_hours: float = 168.0,
    min_settled_outcomes: int = 25,
    max_top_blocker_share: float = 0.55,
    min_approval_rate: float = 0.03,
    min_intents_sample: int = 1000,
    max_sparse_edge_block_share: float = 0.80,
    min_execution_cost_candidate_samples: int = 200,
    min_execution_cost_quote_coverage_ratio: float = 0.60,
    weather_pattern_max_age_hours: float = 24.0,
    weather_negative_expectancy_regime_concentration_max: float = 0.50,
    weather_bucket_coverage_min_ratio: float = 0.70,
    weather_risk_off_stale_metar_share_min: float = 0.60,
    weather_risk_off_sample_floor: int = 200,
    weather_confidence_adjusted_fallback_consecutive_threshold: int = 3,
    coverage_velocity_required_positive_streak: int = 2,
    now: datetime | None = None,
    bootstrap_max_top_blocker_share: float = 0.70,
    bootstrap_min_approval_rate: float = 0.01,
    bootstrap_max_sparse_edge_block_share: float = 1.0,
    bootstrap_max_critical_blockers: int = 4,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)
    out_dir = Path(output_dir)
    checkpoints_dir = out_dir / "checkpoints"
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    window_int = max(1, int(round(float(window_hours))))
    blocker_patterns = (
        f"checkpoints/blocker_audit_{window_int}h_latest.json",
        f"checkpoints/blocker_audit_{window_int}h_*.json",
        "checkpoints/blocker_audit_168h_latest.json",
        "checkpoints/blocker_audit_168h_*.json",
    )
    alpha_patterns = (
        "health/alpha_summary_latest.json",
        "checkpoints/alpha_summary_12h_latest.json",
        "checkpoints/alpha_summary_12h_*.json",
    )
    execution_cost_patterns = (
        "health/execution_cost_tape_latest.json",
        "health/execution_cost_tape_*.json",
    )

    blocker_payload, blocker_file = _latest_payload(out_dir, blocker_patterns)
    alpha_payload, alpha_file = _latest_payload(out_dir, alpha_patterns)
    execution_cost_payload, execution_cost_file = _latest_payload(out_dir, execution_cost_patterns)
    weather_pattern_artifact = _read_weather_pattern_artifact(out_dir, now=captured_at)

    blockers: list[dict[str, Any]] = []
    data_gaps: list[str] = []

    largest_blocker_reason = ""
    largest_blocker_share = 0.0
    blocked_total = 0
    top_blocker_count = 0
    if isinstance(blocker_payload, dict):
        headline = blocker_payload.get("headline")
        headline = dict(headline) if isinstance(headline, dict) else {}
        largest_blocker_reason = _normalize_text(
            headline.get("largest_blocker_reason_raw")
            or headline.get("largest_blocker_reason")
        )
        largest_blocker_share = float(
            _parse_float(
                headline.get("largest_blocker_share_of_blocked_raw")
                or headline.get("largest_blocker_share_of_blocked")
            )
            or 0.0
        )
        blocked_total = _parse_int(headline.get("blocked_total"))
        top_blocker_count = _parse_int(headline.get("largest_blocker_count_raw") or headline.get("largest_blocker_count"))
    else:
        data_gaps.append("missing_blocker_audit_artifact")

    approval_rate = 0.0
    intents_total = 0
    settled_outcomes = 0
    projected_pnl = None
    quality_drift_alert = False
    sparse_edge_block_share = None
    alpha_top_blocker_reason = ""
    alpha_top_blocker_share = 0.0
    if isinstance(alpha_payload, dict):
        headline_metrics = alpha_payload.get("headline_metrics")
        headline_metrics = dict(headline_metrics) if isinstance(headline_metrics, dict) else {}
        intents_total = _parse_int(headline_metrics.get("intents_total"))
        approval_rate = float(_parse_float(headline_metrics.get("approval_rate")) or 0.0)
        settled_outcomes = _parse_int(
            headline_metrics.get("settled_unique_market_side_resolved_predictions")
            or headline_metrics.get("resolved_unique_market_sides")
        )
        projected_pnl = _parse_float(headline_metrics.get("projected_pnl_on_reference_bankroll_dollars"))
        quality_drift_alert = bool(headline_metrics.get("quality_drift_alert_active"))
        sparse_edge_block_share = _parse_float(headline_metrics.get("sparse_hardening_expected_edge_block_share"))
        alpha_top_blocker_reason = _normalize_text(headline_metrics.get("top_blocker_reason"))
        alpha_top_blocker_share = float(_parse_float(headline_metrics.get("top_blocker_share_of_blocked")) or 0.0)
    else:
        data_gaps.append("missing_alpha_summary_artifact")

    profitability_fallback_artifact = _read_profitability_settled_outcomes_fallback(out_dir, now=captured_at)
    settled_outcome_throughput_artifact = _read_settled_outcome_throughput_artifact(out_dir, now=captured_at)
    fallback_settled_outcomes = max(0, _parse_int(profitability_fallback_artifact.get("settled_outcomes")))
    fallback_trend_delta_24h_raw = profitability_fallback_artifact.get("trend_delta_24h")
    fallback_trend_delta_7d_raw = profitability_fallback_artifact.get("trend_delta_7d")
    fallback_trend_delta_24h = (
        _parse_int(fallback_trend_delta_24h_raw) if fallback_trend_delta_24h_raw is not None else None
    )
    fallback_trend_delta_7d = (
        _parse_int(fallback_trend_delta_7d_raw) if fallback_trend_delta_7d_raw is not None else None
    )
    settled_outcomes_source = "alpha_summary"
    settled_outcomes_source_file = alpha_file
    if fallback_settled_outcomes > settled_outcomes:
        settled_outcomes = int(fallback_settled_outcomes)
        settled_outcomes_source = "profitability_fallback"
        settled_outcomes_source_file = _normalize_text(profitability_fallback_artifact.get("source"))

    throughput_growth_deltas = [
        delta
        for delta in (
            settled_outcome_throughput_artifact.get("growth_deltas_settled_outcomes_delta_24h"),
            settled_outcome_throughput_artifact.get("growth_deltas_settled_outcomes_delta_7d"),
            settled_outcome_throughput_artifact.get("growth_deltas_combined_bucket_count_delta_24h"),
            settled_outcome_throughput_artifact.get("growth_deltas_combined_bucket_count_delta_7d"),
        )
        if isinstance(delta, int)
    ]
    fallback_growth_deltas = [
        delta
        for delta in (fallback_trend_delta_24h, fallback_trend_delta_7d)
        if isinstance(delta, int)
    ]
    if throughput_growth_deltas:
        settled_outcome_growth_deltas = throughput_growth_deltas
        settled_outcome_growth_source = "settled_outcome_throughput"
        settled_outcome_growth_source_file = _normalize_text(
            settled_outcome_throughput_artifact.get("source")
        ) or None
        settled_outcome_growth_selected_24h = settled_outcome_throughput_artifact.get(
            "growth_deltas_settled_outcomes_delta_24h"
        )
        settled_outcome_growth_selected_7d = settled_outcome_throughput_artifact.get(
            "growth_deltas_settled_outcomes_delta_7d"
        )
        settled_outcome_growth_combined_bucket_count_delta_24h = settled_outcome_throughput_artifact.get(
            "growth_deltas_combined_bucket_count_delta_24h"
        )
        settled_outcome_growth_combined_bucket_count_delta_7d = settled_outcome_throughput_artifact.get(
            "growth_deltas_combined_bucket_count_delta_7d"
        )
    else:
        settled_outcome_growth_deltas = fallback_growth_deltas
        settled_outcome_growth_source = "profitability_fallback"
        settled_outcome_growth_source_file = _normalize_text(
            profitability_fallback_artifact.get("source")
        ) or None
        settled_outcome_growth_selected_24h = fallback_trend_delta_24h
        settled_outcome_growth_selected_7d = fallback_trend_delta_7d
        settled_outcome_growth_combined_bucket_count_delta_24h = None
        settled_outcome_growth_combined_bucket_count_delta_7d = None

    coverage_velocity_state_file = health_dir / "decision_matrix_coverage_velocity_state_latest.json"
    coverage_velocity_previous_state = _read_coverage_velocity_state(out_dir, now=captured_at)
    coverage_velocity_required_positive_streak = max(1, int(coverage_velocity_required_positive_streak))
    selected_growth_deltas = [
        delta
        for delta in (
            settled_outcome_growth_selected_24h,
            settled_outcome_growth_selected_7d,
            settled_outcome_growth_combined_bucket_count_delta_24h,
            settled_outcome_growth_combined_bucket_count_delta_7d,
        )
        if isinstance(delta, int)
    ]
    coverage_velocity_evidence_available = bool(selected_growth_deltas)
    coverage_velocity_selected_growth_positive = bool(
        coverage_velocity_evidence_available and any(delta > 0 for delta in selected_growth_deltas)
    )
    coverage_velocity_previous_positive_streak = max(0, _parse_int(coverage_velocity_previous_state.get("positive_streak")))
    coverage_velocity_previous_non_positive_streak = max(
        0, _parse_int(coverage_velocity_previous_state.get("non_positive_streak"))
    )
    if coverage_velocity_evidence_available:
        coverage_velocity_positive_streak = (
            coverage_velocity_previous_positive_streak + 1
            if coverage_velocity_selected_growth_positive
            else 0
        )
        coverage_velocity_non_positive_streak = (
            0
            if coverage_velocity_selected_growth_positive
            else coverage_velocity_previous_non_positive_streak + 1
        )
        coverage_velocity_guardrail_cleared = (
            coverage_velocity_positive_streak >= coverage_velocity_required_positive_streak
        )
        coverage_velocity_guardrail_active = not coverage_velocity_guardrail_cleared
        coverage_velocity_last_evidence_direction = (
            "positive" if coverage_velocity_selected_growth_positive else "non_positive"
        )
    else:
        coverage_velocity_positive_streak = coverage_velocity_previous_positive_streak
        coverage_velocity_non_positive_streak = coverage_velocity_previous_non_positive_streak
        coverage_velocity_guardrail_active = bool(coverage_velocity_previous_state.get("guardrail_active"))
        coverage_velocity_guardrail_cleared = bool(coverage_velocity_previous_state.get("guardrail_cleared"))
        coverage_velocity_last_evidence_direction = (
            _normalize_text(coverage_velocity_previous_state.get("last_evidence_direction")).lower()
            or "missing"
        )
    coverage_velocity_state_payload = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "source": str(coverage_velocity_state_file),
        "evidence_available": bool(coverage_velocity_evidence_available),
        "selected_growth_source": settled_outcome_growth_source,
        "selected_growth_source_file": settled_outcome_growth_source_file or None,
        "selected_growth_delta_24h": (
            int(settled_outcome_growth_selected_24h)
            if isinstance(settled_outcome_growth_selected_24h, int)
            else None
        ),
        "selected_growth_delta_7d": (
            int(settled_outcome_growth_selected_7d)
            if isinstance(settled_outcome_growth_selected_7d, int)
            else None
        ),
        "selected_combined_bucket_count_delta_24h": (
            int(settled_outcome_growth_combined_bucket_count_delta_24h)
            if isinstance(settled_outcome_growth_combined_bucket_count_delta_24h, int)
            else None
        ),
        "selected_combined_bucket_count_delta_7d": (
            int(settled_outcome_growth_combined_bucket_count_delta_7d)
            if isinstance(settled_outcome_growth_combined_bucket_count_delta_7d, int)
            else None
        ),
        "positive_streak": int(coverage_velocity_positive_streak),
        "non_positive_streak": int(coverage_velocity_non_positive_streak),
        "required_positive_streak": int(coverage_velocity_required_positive_streak),
        "guardrail_active": bool(coverage_velocity_guardrail_active),
        "guardrail_cleared": bool(coverage_velocity_guardrail_cleared),
        "last_evidence_direction": coverage_velocity_last_evidence_direction,
    }
    _write_json_file(coverage_velocity_state_file, coverage_velocity_state_payload)

    execution_cost_status = ""
    execution_cost_candidate_rows = 0
    execution_cost_quote_coverage_ratio = None
    execution_cost_meets_candidate_samples = False
    execution_cost_meets_quote_coverage = False
    if isinstance(execution_cost_payload, dict):
        readiness = execution_cost_payload.get("calibration_readiness")
        readiness = dict(readiness) if isinstance(readiness, dict) else {}
        execution_cost_status = _normalize_text(readiness.get("status")).lower()
        execution_cost_candidate_rows = _parse_int(readiness.get("candidate_rows"))
        execution_cost_quote_coverage_ratio = _parse_float(readiness.get("quote_coverage_ratio"))
        execution_cost_meets_candidate_samples = bool(readiness.get("meets_candidate_samples") is True)
        execution_cost_meets_quote_coverage = bool(readiness.get("meets_quote_coverage") is True)

    weather_pattern_source = _normalize_text(weather_pattern_artifact.get("source"))
    weather_pattern_status = _normalize_text(weather_pattern_artifact.get("status")).lower() or "missing"
    weather_pattern_age_hours = _parse_float(weather_pattern_artifact.get("age_hours"))
    negative_expectancy_regime_concentration = _parse_float(
        weather_pattern_artifact.get("negative_expectancy_regime_concentration")
    )
    weather_bucket_coverage_ratio = _parse_float(weather_pattern_artifact.get("weather_bucket_coverage_ratio"))
    weather_bucket_coverage_count = _parse_int(weather_pattern_artifact.get("weather_bucket_coverage_count"))
    weather_bucket_total = _parse_int(weather_pattern_artifact.get("weather_bucket_total"))
    metar_observation_stale_share = _normalize_ratio(
        _parse_float(weather_pattern_artifact.get("metar_observation_stale_share"))
    )
    weather_negative_expectancy_regime_concentration_source = (
        _normalize_text(weather_pattern_artifact.get("negative_expectancy_regime_concentration_source")).lower()
        or "missing"
    )
    weather_metar_observation_stale_share_source = (
        _normalize_text(weather_pattern_artifact.get("metar_observation_stale_share_source")).lower() or "missing"
    )
    metar_observation_stale_count = _parse_int(weather_pattern_artifact.get("metar_observation_stale_count"))
    weather_risk_off_sample_count = _parse_int(weather_pattern_artifact.get("weather_risk_off_sample_count"))
    weather_pattern_is_fresh = (
        bool(weather_pattern_source)
        and weather_pattern_status in {"ready", "ready_partial"}
        and isinstance(weather_pattern_age_hours, float)
        and weather_pattern_age_hours <= float(max(0.0, weather_pattern_max_age_hours))
    )
    weather_confidence_state_file = health_dir / "decision_matrix_weather_confidence_state_latest.json"
    weather_confidence_previous_state = _load_json_file(weather_confidence_state_file)
    weather_confidence_previous_raw_fallback_count = max(
        0,
        _parse_int(weather_confidence_previous_state.get("raw_fallback_consecutive_count")),
    )
    weather_confidence_adjusted_fallback_threshold = max(
        1,
        int(weather_confidence_adjusted_fallback_consecutive_threshold),
    )
    weather_confidence_adjusted_raw_fallback_active = bool(
        weather_pattern_is_fresh
        and (
            weather_negative_expectancy_regime_concentration_source == "raw"
            or weather_metar_observation_stale_share_source == "raw"
        )
    )
    weather_confidence_adjusted_raw_fallback_consecutive_count = (
        weather_confidence_previous_raw_fallback_count + 1
        if weather_confidence_adjusted_raw_fallback_active
        else 0
    )
    weather_confidence_adjusted_raw_fallback_persistent = bool(
        weather_confidence_adjusted_raw_fallback_active
        and weather_confidence_adjusted_raw_fallback_consecutive_count
        >= weather_confidence_adjusted_fallback_threshold
    )
    _write_json_file(
        weather_confidence_state_file,
        {
            "status": "ready",
            "captured_at": captured_at.isoformat(),
            "weather_pattern_is_fresh": bool(weather_pattern_is_fresh),
            "raw_fallback_active": bool(weather_confidence_adjusted_raw_fallback_active),
            "raw_fallback_consecutive_count": int(weather_confidence_adjusted_raw_fallback_consecutive_count),
            "raw_fallback_persistent": bool(weather_confidence_adjusted_raw_fallback_persistent),
            "raw_fallback_consecutive_threshold": int(weather_confidence_adjusted_fallback_threshold),
            "negative_expectancy_regime_concentration_source": weather_negative_expectancy_regime_concentration_source,
            "metar_observation_stale_share_source": weather_metar_observation_stale_share_source,
        },
    )
    if not weather_pattern_source:
        data_gaps.append("missing_weather_pattern_artifact")
        _append_blocker(
            rows=blockers,
            key="missing_weather_pattern_artifact",
            severity="critical",
            summary="Weather pattern artifact is missing from health.",
            recommendation="Publish kalshi_temperature_weather_pattern_latest.json before relying on weather-regime hardening.",
            observed_value="missing",
            threshold="fresh weather-pattern artifact available",
        )
    elif not weather_pattern_is_fresh:
        _append_blocker(
            rows=blockers,
            key="stale_weather_pattern_artifact",
            severity="high",
            summary=(
                "Weather pattern artifact is stale or not ready "
                f"(status={weather_pattern_status}, age_hours={weather_pattern_age_hours})."
            ),
            recommendation="Refresh the weather pattern artifact before using weather-regime blockers for gating.",
            observed_value={
                "status": weather_pattern_status,
                "age_hours": (
                    round(float(weather_pattern_age_hours), 6)
                    if isinstance(weather_pattern_age_hours, float)
                    else None
                ),
            },
            threshold={
                "max_age_hours": float(weather_pattern_max_age_hours),
                "allowed_statuses": ["ready", "ready_partial"],
            },
        )
    if weather_confidence_adjusted_raw_fallback_persistent:
        _append_blocker(
            rows=blockers,
            key="weather_confidence_adjusted_signal_fallback_persistent",
            severity="high",
            summary=(
                "Weather hardening is repeatedly falling back to raw regime shares "
                f"({weather_confidence_adjusted_raw_fallback_consecutive_count} consecutive runs)."
            ),
            recommendation=(
                "Repair confidence-adjusted weather-pattern signals and rerun decision-matrix hardening "
                "before trusting weather blocker/risk-off decisions."
            ),
            observed_value={
                "negative_expectancy_regime_concentration_source": weather_negative_expectancy_regime_concentration_source,
                "metar_observation_stale_share_source": weather_metar_observation_stale_share_source,
                "raw_fallback_consecutive_count": int(weather_confidence_adjusted_raw_fallback_consecutive_count),
            },
            threshold={
                "raw_fallback_consecutive_threshold": int(weather_confidence_adjusted_fallback_threshold),
                "requires_confidence_adjusted_source": True,
            },
        )

    if (
        weather_pattern_is_fresh
        and isinstance(negative_expectancy_regime_concentration, float)
        and negative_expectancy_regime_concentration >= float(weather_negative_expectancy_regime_concentration_max)
    ):
        _append_blocker(
            rows=blockers,
            key="weather_negative_expectancy_regime_concentration_high",
            severity="critical",
            summary=(
                "Negative-expectancy weather regime concentration is too high "
                f"({negative_expectancy_regime_concentration*100.0:.1f}%)."
            ),
            recommendation=(
                "Broaden weather regime bucketing or split concentrated negative regimes before scaling."
            ),
            observed_value=round(float(negative_expectancy_regime_concentration), 6),
            threshold=float(weather_negative_expectancy_regime_concentration_max),
        )
    if (
        weather_pattern_is_fresh
        and isinstance(weather_bucket_coverage_ratio, float)
        and weather_bucket_coverage_ratio < float(weather_bucket_coverage_min_ratio)
    ):
        _append_blocker(
            rows=blockers,
            key="weather_bucket_coverage_insufficient",
            severity="high",
            summary=(
                "Weather bucket coverage is below threshold "
                f"({weather_bucket_coverage_ratio*100.0:.1f}%)."
            ),
            recommendation=(
                "Collect broader bucket coverage so weather-regime hardening has enough evidence."
            ),
            observed_value=round(float(weather_bucket_coverage_ratio), 6),
            threshold=float(weather_bucket_coverage_min_ratio),
        )
    weather_risk_off_recommended = bool(
        weather_pattern_is_fresh
        and isinstance(negative_expectancy_regime_concentration, float)
        and negative_expectancy_regime_concentration >= float(weather_negative_expectancy_regime_concentration_max)
        and isinstance(metar_observation_stale_share, float)
        and metar_observation_stale_share >= float(weather_risk_off_stale_metar_share_min)
        and weather_risk_off_sample_count >= int(max(0, weather_risk_off_sample_floor))
    )
    if weather_risk_off_recommended:
        _append_blocker(
            rows=blockers,
            key="weather_global_risk_off_recommended",
            severity="critical",
            summary=(
                "Weather artifact recommends immediate global risk-off "
                "(concentrated negative expectancy with elevated stale-METAR share)."
            ),
            recommendation=(
                "Switch to global risk-off immediately, restore METAR freshness, and re-run weather-pattern "
                "profiling before resuming normal directional risk."
            ),
            observed_value={
                "negative_expectancy_regime_concentration": round(float(negative_expectancy_regime_concentration), 6),
                "metar_observation_stale_share": round(float(metar_observation_stale_share), 6),
                "weather_risk_off_sample_count": int(weather_risk_off_sample_count),
            },
            threshold={
                "negative_expectancy_regime_concentration_min": float(
                    weather_negative_expectancy_regime_concentration_max
                ),
                "metar_observation_stale_share_min": float(weather_risk_off_stale_metar_share_min),
                "weather_risk_off_sample_floor": int(max(0, weather_risk_off_sample_floor)),
            },
        )

    if largest_blocker_reason and largest_blocker_share >= float(max_top_blocker_share):
        _append_blocker(
            rows=blockers,
            key="blocker_concentration_high",
            severity="critical",
            summary=(
                f"Single blocker dominates blocked flow: {largest_blocker_reason} "
                f"at {largest_blocker_share*100.0:.1f}%."
            ),
            recommendation="Reduce blocker concentration before increasing throughput or bankroll utilization.",
            observed_value=round(float(largest_blocker_share), 6),
            threshold=float(max_top_blocker_share),
        )

    reason_key = _coerce_reason(largest_blocker_reason or alpha_top_blocker_reason)
    if reason_key == "expected_edge_below_min":
        _append_blocker(
            rows=blockers,
            key="expected_edge_gate_dominance",
            severity="critical",
            summary="Expected-edge gate remains the primary blocker to approvals.",
            recommendation="Recalibrate expected-edge floor with execution realism and bucket-level markout evidence.",
            observed_value=reason_key,
            threshold="expected_edge_below_min should not be dominant",
        )
        if not isinstance(execution_cost_payload, dict):
            data_gaps.append("missing_execution_cost_tape_artifact")
        elif not execution_cost_meets_candidate_samples or not execution_cost_meets_quote_coverage:
            _append_blocker(
                rows=blockers,
                key="execution_cost_calibration_not_ready",
                severity="high",
                summary=(
                    "Execution-cost calibration tape is not ready for expected-edge floor retuning "
                    f"(status={execution_cost_status or 'unknown'})."
                ),
                recommendation=(
                    "Increase candidate sample density and two-sided quote coverage in the execution-cost tape "
                    "before changing expected-edge gates."
                ),
                observed_value={
                    "candidate_rows": int(execution_cost_candidate_rows),
                    "quote_coverage_ratio": (
                        round(float(execution_cost_quote_coverage_ratio), 6)
                        if isinstance(execution_cost_quote_coverage_ratio, float)
                        else None
                    ),
                },
                threshold={
                    "min_candidate_rows": int(min_execution_cost_candidate_samples),
                    "min_quote_coverage_ratio": float(min_execution_cost_quote_coverage_ratio),
                },
            )
    if reason_key in {"historical_quality_signal_type_hard_block", "historical_quality_global_only_pressure"}:
        _append_blocker(
            rows=blockers,
            key="historical_quality_hard_block_dominance",
            severity="critical",
            summary="Historical-quality hard block dominates decision gating.",
            recommendation="Increase bucket-backed settled evidence coverage to replace generic hard-block pressure.",
            observed_value=reason_key,
            threshold="historical-quality hard block should be minority blocker",
        )

    if intents_total >= int(min_intents_sample) and approval_rate < float(min_approval_rate):
        _append_blocker(
            rows=blockers,
            key="approval_rate_below_floor",
            severity="high",
            summary=(
                f"Approval rate is below floor: {approval_rate*100.0:.2f}% on {intents_total:,} intents."
            ),
            recommendation="Run threshold-boundary audits to recover throughput without increasing quality drift.",
            observed_value=round(float(approval_rate), 6),
            threshold=float(min_approval_rate),
        )

    if settled_outcomes < int(min_settled_outcomes):
        _append_blocker(
            rows=blockers,
            key="insufficient_settled_outcomes",
            severity="critical",
            summary=(
                f"Settled independent outcomes are too low: {settled_outcomes:,} < {int(min_settled_outcomes):,}."
            ),
            recommendation="Expand independent breadth across families/hours until settled evidence threshold is met.",
            observed_value=int(settled_outcomes),
            threshold=int(min_settled_outcomes),
        )
    settled_outcome_growth_stalled = bool(settled_outcome_growth_deltas) and all(
        delta <= 0 for delta in settled_outcome_growth_deltas
    )
    if settled_outcomes < int(min_settled_outcomes) and settled_outcome_growth_stalled:
        _append_blocker(
            rows=blockers,
            key="settled_outcome_growth_stalled",
            severity="high",
            summary=(
                "Settled-outcome growth is stalled while coverage remains below threshold "
                f"({settled_outcomes:,} < {int(min_settled_outcomes):,})."
            ),
            recommendation=(
                "Prioritize settlement throughput and breadth expansion until 24h/7d settled-outcome deltas turn positive."
            ),
            observed_value={
                "settled_outcomes": int(settled_outcomes),
                "settled_outcomes_source": settled_outcomes_source,
                "growth_source": settled_outcome_growth_source,
                "growth_source_file": settled_outcome_growth_source_file,
                "growth_delta_24h": (
                    int(settled_outcome_growth_selected_24h)
                    if isinstance(settled_outcome_growth_selected_24h, int)
                    else None
                ),
                "growth_delta_7d": (
                    int(settled_outcome_growth_selected_7d)
                    if isinstance(settled_outcome_growth_selected_7d, int)
                    else None
                ),
                "combined_bucket_count_delta_24h": (
                    int(settled_outcome_growth_combined_bucket_count_delta_24h)
                    if isinstance(settled_outcome_growth_combined_bucket_count_delta_24h, int)
                    else None
                ),
                "combined_bucket_count_delta_7d": (
                    int(settled_outcome_growth_combined_bucket_count_delta_7d)
                    if isinstance(settled_outcome_growth_combined_bucket_count_delta_7d, int)
                    else None
                ),
            },
            threshold={
                "min_settled_outcomes": int(min_settled_outcomes),
                "requires_positive_growth_delta": True,
            },
        )
    if coverage_velocity_evidence_available and coverage_velocity_guardrail_active:
        _append_blocker(
            rows=blockers,
            key="coverage_velocity_guardrail_not_cleared",
            severity="critical",
            summary=(
                "Coverage-velocity guardrail is still gated until consecutive positive settled-outcome "
                f"growth reaches {coverage_velocity_required_positive_streak} runs."
            ),
            recommendation=(
                "Keep scaling gated and recover settled-outcome momentum with repeated positive throughput runs."
            ),
            observed_value={
                "evidence_available": True,
                "positive_streak": int(coverage_velocity_positive_streak),
                "non_positive_streak": int(coverage_velocity_non_positive_streak),
                "guardrail_active": bool(coverage_velocity_guardrail_active),
                "guardrail_cleared": bool(coverage_velocity_guardrail_cleared),
                "selected_growth_delta_24h": (
                    int(settled_outcome_growth_selected_24h)
                    if isinstance(settled_outcome_growth_selected_24h, int)
                    else None
                ),
                "selected_growth_delta_7d": (
                    int(settled_outcome_growth_selected_7d)
                    if isinstance(settled_outcome_growth_selected_7d, int)
                    else None
                ),
                "selected_combined_bucket_count_delta_24h": (
                    int(settled_outcome_growth_combined_bucket_count_delta_24h)
                    if isinstance(settled_outcome_growth_combined_bucket_count_delta_24h, int)
                    else None
                ),
                "selected_combined_bucket_count_delta_7d": (
                    int(settled_outcome_growth_combined_bucket_count_delta_7d)
                    if isinstance(settled_outcome_growth_combined_bucket_count_delta_7d, int)
                    else None
                ),
            },
            threshold={
                "required_positive_streak": int(coverage_velocity_required_positive_streak),
                "guardrail_active": True,
                "guardrail_cleared": False,
            },
        )

    if isinstance(projected_pnl, float) and projected_pnl <= 0.0 and intents_total >= int(min_intents_sample):
        _append_blocker(
            rows=blockers,
            key="projected_pnl_non_positive",
            severity="high",
            summary=f"Projected bankroll PnL is non-positive ({projected_pnl:.2f}) with large sample coverage.",
            recommendation="Prioritize calibration loop on projected-vs-settled drift before scaling.",
            observed_value=round(float(projected_pnl), 6),
            threshold="> 0.0",
        )

    if quality_drift_alert:
        _append_blocker(
            rows=blockers,
            key="quality_drift_alert_active",
            severity="high",
            summary="Approval quality drift alert is active.",
            recommendation="Stabilize approval-rate drift across windows before raising approval throughput.",
            observed_value=True,
            threshold=False,
        )

    if isinstance(sparse_edge_block_share, float) and sparse_edge_block_share >= float(max_sparse_edge_block_share):
        _append_blocker(
            rows=blockers,
            key="sparse_edge_block_share_high",
            severity="high",
            summary=(
                f"Sparse-evidence hardening edge-block share is high ({sparse_edge_block_share*100.0:.1f}%)."
            ),
            recommendation="Collect stronger per-bucket profitability evidence to avoid blanket edge hardening.",
            observed_value=round(float(sparse_edge_block_share), 6),
            threshold=float(max_sparse_edge_block_share),
        )

    severity_score = {"critical": 22, "high": 12, "medium": 7, "low": 4}
    matrix_score = 100
    for row in blockers:
        matrix_score -= severity_score.get(_normalize_text(row.get("severity")), 7)
    if data_gaps:
        matrix_score -= min(20, 8 * len(data_gaps))
    matrix_score = max(0, min(100, int(matrix_score)))

    critical_count = sum(1 for row in blockers if _normalize_text(row.get("severity")) == "critical")
    if critical_count > 0 or matrix_score < 50:
        matrix_health_status = "red"
    elif matrix_score < 75:
        matrix_health_status = "yellow"
    else:
        matrix_health_status = "green"

    blocker_keys = {_normalize_text(row.get("key")) for row in blockers}
    supports_consistency_and_profitability = bool(
        matrix_health_status != "red"
        and critical_count == 0
        and not data_gaps
        and "coverage_velocity_guardrail_not_cleared" not in blocker_keys
    )
    bootstrap_signal = _assess_bootstrap_progression(
        blocker_keys=blocker_keys,
        data_gaps=data_gaps,
        critical_count=critical_count,
        largest_blocker_share=float(largest_blocker_share or alpha_top_blocker_share),
        approval_rate=approval_rate,
        intents_total=intents_total,
        min_intents_sample=int(min_intents_sample),
        sparse_edge_block_share=sparse_edge_block_share,
        settled_outcomes=settled_outcomes,
        min_settled_outcomes=int(min_settled_outcomes),
        bootstrap_max_top_blocker_share=bootstrap_max_top_blocker_share,
        bootstrap_min_approval_rate=bootstrap_min_approval_rate,
        bootstrap_max_sparse_edge_block_share=bootstrap_max_sparse_edge_block_share,
        bootstrap_max_critical_blockers=int(bootstrap_max_critical_blockers),
        weather_risk_off_recommended=weather_risk_off_recommended,
    )
    supports_bootstrap_progression = bool(bootstrap_signal.get("supports_bootstrap_progression") is True)
    backlog = _build_pipeline_backlog(blockers)

    payload: dict[str, Any] = {
        "status": "ready",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "window_hours": float(window_hours),
        "window_label": f"{window_int}h",
        "matrix_health_status": matrix_health_status,
        "matrix_score": matrix_score,
        "supports_consistency_and_profitability": supports_consistency_and_profitability,
        "supports_bootstrap_progression": supports_bootstrap_progression,
        "critical_blockers_count": critical_count,
        "blocking_factors": blockers,
        "data_pipeline_gaps": sorted(set(data_gaps)),
        "data_sources": {
            "blocker_audit": {
                "source": blocker_file,
                "status": _normalize_text(blocker_payload.get("status")).lower() if isinstance(blocker_payload, dict) else "missing",
                "age_hours": _artifact_age_hours(blocker_file, now=captured_at),
            },
            "alpha_summary": {
                "source": alpha_file,
                "status": _normalize_text(alpha_payload.get("status")).lower() if isinstance(alpha_payload, dict) else "missing",
                "age_hours": _artifact_age_hours(alpha_file, now=captured_at),
            },
            "execution_cost_tape": {
                "source": execution_cost_file,
                "status": execution_cost_status or ("missing" if not execution_cost_file else "unknown"),
                "age_hours": _artifact_age_hours(execution_cost_file, now=captured_at),
            },
            "settled_outcome_throughput": settled_outcome_throughput_artifact,
            "coverage_velocity_state": {
                "source": str(coverage_velocity_state_file),
                "status": "ready",
                "age_hours": _artifact_age_hours(str(coverage_velocity_state_file), now=captured_at),
            },
            "profitability_fallback": profitability_fallback_artifact,
            "weather_pattern": weather_pattern_artifact,
            "weather_confidence_state": {
                "source": str(weather_confidence_state_file),
                "status": "ready",
                "age_hours": _artifact_age_hours(str(weather_confidence_state_file), now=captured_at),
            },
        },
        "bootstrap_signal": bootstrap_signal,
        "pipeline_backlog": backlog,
        "observed_metrics": {
            "largest_blocker_reason": largest_blocker_reason or alpha_top_blocker_reason,
            "largest_blocker_share_of_blocked": round(float(largest_blocker_share or alpha_top_blocker_share), 6),
            "largest_blocker_count": int(top_blocker_count),
            "blocked_total": int(blocked_total),
            "approval_rate": round(float(approval_rate), 6),
            "intents_total": int(intents_total),
            "settled_outcomes": int(settled_outcomes),
            "settled_outcomes_source": settled_outcomes_source,
            "settled_outcomes_source_file": settled_outcomes_source_file or None,
            "settled_outcomes_fallback_source": _normalize_text(profitability_fallback_artifact.get("source")) or None,
            "settled_outcomes_fallback_value": int(fallback_settled_outcomes),
            "settled_outcomes_fallback_trend_delta_24h": (
                int(fallback_trend_delta_24h) if isinstance(fallback_trend_delta_24h, int) else None
            ),
            "settled_outcomes_fallback_trend_delta_7d": (
                int(fallback_trend_delta_7d) if isinstance(fallback_trend_delta_7d, int) else None
            ),
            "settled_outcome_throughput_status": settled_outcome_throughput_artifact.get("status") or None,
            "settled_outcome_throughput_source": _normalize_text(
                settled_outcome_throughput_artifact.get("source")
            )
            or None,
            "settled_outcome_throughput_age_hours": (
                round(float(settled_outcome_throughput_artifact.get("age_hours")), 6)
                if isinstance(settled_outcome_throughput_artifact.get("age_hours"), float)
                else None
            ),
            "settled_outcome_throughput_coverage_settled_outcomes": (
                int(settled_outcome_throughput_artifact["coverage_settled_outcomes"])
                if isinstance(settled_outcome_throughput_artifact.get("coverage_settled_outcomes"), int)
                else None
            ),
            "settled_outcome_throughput_growth_delta_24h": (
                int(settled_outcome_throughput_artifact["growth_deltas_settled_outcomes_delta_24h"])
                if isinstance(
                    settled_outcome_throughput_artifact.get("growth_deltas_settled_outcomes_delta_24h"),
                    int,
                )
                else None
            ),
            "settled_outcome_throughput_growth_delta_7d": (
                int(settled_outcome_throughput_artifact["growth_deltas_settled_outcomes_delta_7d"])
                if isinstance(
                    settled_outcome_throughput_artifact.get("growth_deltas_settled_outcomes_delta_7d"),
                    int,
                )
                else None
            ),
            "settled_outcome_throughput_combined_bucket_count_delta_24h": (
                int(settled_outcome_throughput_artifact["growth_deltas_combined_bucket_count_delta_24h"])
                if isinstance(
                    settled_outcome_throughput_artifact.get(
                        "growth_deltas_combined_bucket_count_delta_24h"
                    ),
                    int,
                )
                else None
            ),
            "settled_outcome_throughput_combined_bucket_count_delta_7d": (
                int(settled_outcome_throughput_artifact["growth_deltas_combined_bucket_count_delta_7d"])
                if isinstance(
                    settled_outcome_throughput_artifact.get(
                        "growth_deltas_combined_bucket_count_delta_7d"
                    ),
                    int,
                )
                else None
            ),
            "settled_outcome_throughput_targeted_constraint_rows": (
                int(settled_outcome_throughput_artifact["targeting_targeted_constraint_rows"])
                if isinstance(settled_outcome_throughput_artifact.get("targeting_targeted_constraint_rows"), int)
                else None
            ),
            "settled_outcome_throughput_top_bottlenecks_count": int(
                settled_outcome_throughput_artifact.get("top_bottlenecks_count", 0) or 0
            ),
            "settled_outcome_throughput_bottleneck_source": (
                _normalize_text(settled_outcome_throughput_artifact.get("bottleneck_source")) or None
            ),
            "settled_outcome_growth_source": settled_outcome_growth_source,
            "settled_outcome_growth_source_file": settled_outcome_growth_source_file,
            "settled_outcome_growth_delta_24h": (
                int(settled_outcome_growth_selected_24h)
                if isinstance(settled_outcome_growth_selected_24h, int)
                else None
            ),
            "settled_outcome_growth_delta_7d": (
                int(settled_outcome_growth_selected_7d)
                if isinstance(settled_outcome_growth_selected_7d, int)
                else None
            ),
            "settled_outcome_growth_combined_bucket_count_delta_24h": (
                int(settled_outcome_growth_combined_bucket_count_delta_24h)
                if isinstance(settled_outcome_growth_combined_bucket_count_delta_24h, int)
                else None
            ),
            "settled_outcome_growth_combined_bucket_count_delta_7d": (
                int(settled_outcome_growth_combined_bucket_count_delta_7d)
                if isinstance(settled_outcome_growth_combined_bucket_count_delta_7d, int)
                else None
            ),
            "coverage_velocity_positive_streak": int(coverage_velocity_positive_streak),
            "coverage_velocity_non_positive_streak": int(coverage_velocity_non_positive_streak),
            "coverage_velocity_required_positive_streak": int(coverage_velocity_required_positive_streak),
            "coverage_velocity_guardrail_active": bool(coverage_velocity_guardrail_active),
            "coverage_velocity_guardrail_cleared": bool(coverage_velocity_guardrail_cleared),
            "coverage_velocity_selected_growth_delta_24h": (
                int(settled_outcome_growth_selected_24h)
                if isinstance(settled_outcome_growth_selected_24h, int)
                else None
            ),
            "coverage_velocity_selected_growth_delta_7d": (
                int(settled_outcome_growth_selected_7d)
                if isinstance(settled_outcome_growth_selected_7d, int)
                else None
            ),
            "coverage_velocity_selected_combined_bucket_count_delta_24h": (
                int(settled_outcome_growth_combined_bucket_count_delta_24h)
                if isinstance(settled_outcome_growth_combined_bucket_count_delta_24h, int)
                else None
            ),
            "coverage_velocity_selected_combined_bucket_count_delta_7d": (
                int(settled_outcome_growth_combined_bucket_count_delta_7d)
                if isinstance(settled_outcome_growth_combined_bucket_count_delta_7d, int)
                else None
            ),
            "projected_pnl_on_reference_bankroll_dollars": (
                round(float(projected_pnl), 6) if isinstance(projected_pnl, float) else None
            ),
            "quality_drift_alert_active": bool(quality_drift_alert),
            "sparse_hardening_expected_edge_block_share": (
                round(float(sparse_edge_block_share), 6) if isinstance(sparse_edge_block_share, float) else None
            ),
            "execution_cost_tape_status": execution_cost_status or None,
            "execution_cost_candidate_rows": int(execution_cost_candidate_rows),
            "execution_cost_quote_coverage_ratio": (
                round(float(execution_cost_quote_coverage_ratio), 6)
                if isinstance(execution_cost_quote_coverage_ratio, float)
                else None
            ),
            "execution_cost_meets_candidate_samples": bool(execution_cost_meets_candidate_samples),
            "execution_cost_meets_quote_coverage": bool(execution_cost_meets_quote_coverage),
            "weather_pattern_source": weather_pattern_source or None,
            "weather_pattern_status": weather_pattern_status or None,
            "weather_pattern_age_hours": (
                round(float(weather_pattern_age_hours), 6) if isinstance(weather_pattern_age_hours, float) else None
            ),
            "weather_negative_expectancy_regime_concentration": (
                round(float(negative_expectancy_regime_concentration), 6)
                if isinstance(negative_expectancy_regime_concentration, float)
                else None
            ),
            "weather_negative_expectancy_regime_concentration_source": (
                weather_negative_expectancy_regime_concentration_source or "missing"
            ),
            "weather_bucket_coverage_ratio": (
                round(float(weather_bucket_coverage_ratio), 6)
                if isinstance(weather_bucket_coverage_ratio, float)
                else None
            ),
            "weather_bucket_coverage_count": int(weather_bucket_coverage_count),
            "weather_bucket_total": int(weather_bucket_total),
            "weather_metar_observation_stale_share": (
                round(float(metar_observation_stale_share), 6)
                if isinstance(metar_observation_stale_share, float)
                else None
            ),
            "weather_metar_observation_stale_share_source": (
                weather_metar_observation_stale_share_source or "missing"
            ),
            "weather_metar_observation_stale_count": int(metar_observation_stale_count),
            "weather_risk_off_sample_count": int(weather_risk_off_sample_count),
            "weather_risk_off_recommended": bool(weather_risk_off_recommended),
            "weather_confidence_adjusted_raw_fallback_active": bool(
                weather_confidence_adjusted_raw_fallback_active
            ),
            "weather_confidence_adjusted_raw_fallback_consecutive_count": int(
                weather_confidence_adjusted_raw_fallback_consecutive_count
            ),
            "weather_confidence_adjusted_raw_fallback_persistent": bool(
                weather_confidence_adjusted_raw_fallback_persistent
            ),
        },
        "thresholds": {
            "max_top_blocker_share": float(max_top_blocker_share),
            "min_approval_rate": float(min_approval_rate),
            "min_intents_sample": int(min_intents_sample),
            "min_settled_outcomes": int(min_settled_outcomes),
            "max_sparse_edge_block_share": float(max_sparse_edge_block_share),
            "min_execution_cost_candidate_samples": int(min_execution_cost_candidate_samples),
            "min_execution_cost_quote_coverage_ratio": float(min_execution_cost_quote_coverage_ratio),
            "coverage_velocity_required_positive_streak": int(coverage_velocity_required_positive_streak),
            "weather_pattern_max_age_hours": float(weather_pattern_max_age_hours),
            "weather_negative_expectancy_regime_concentration_max": float(
                weather_negative_expectancy_regime_concentration_max
            ),
            "weather_bucket_coverage_min_ratio": float(weather_bucket_coverage_min_ratio),
            "weather_risk_off_stale_metar_share_min": float(weather_risk_off_stale_metar_share_min),
            "weather_risk_off_sample_floor": int(max(0, weather_risk_off_sample_floor)),
            "weather_confidence_adjusted_fallback_consecutive_threshold": int(
                weather_confidence_adjusted_fallback_threshold
            ),
            "bootstrap_max_top_blocker_share": float(bootstrap_max_top_blocker_share),
            "bootstrap_min_approval_rate": float(bootstrap_min_approval_rate),
            "bootstrap_max_sparse_edge_block_share": float(bootstrap_max_sparse_edge_block_share),
            "bootstrap_max_critical_blockers": int(bootstrap_max_critical_blockers),
        },
        "source_files": {
            "blocker_audit": blocker_file,
            "alpha_summary": alpha_file,
            "execution_cost_tape": execution_cost_file,
            "settled_outcome_throughput": _normalize_text(settled_outcome_throughput_artifact.get("source")),
            "coverage_velocity_state": str(coverage_velocity_state_file),
            "profitability_fallback": _normalize_text(profitability_fallback_artifact.get("source")),
            "weather_pattern": weather_pattern_source,
            "weather_confidence_state": str(weather_confidence_state_file),
        },
        "weather_pattern_artifact": weather_pattern_artifact,
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = health_dir / f"decision_matrix_hardening_{stamp}.json"
    latest_path = health_dir / "decision_matrix_hardening_latest.json"
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    output_path.write_text(encoded, encoding="utf-8")
    latest_path.write_text(encoded, encoding="utf-8")
    payload["output_file"] = str(output_path)
    payload["latest_file"] = str(latest_path)
    return payload
