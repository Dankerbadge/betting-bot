from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from typing import Any

from betbot.decision_matrix_hardening import run_decision_matrix_hardening
from betbot.kalshi_temperature_growth_optimizer import run_kalshi_temperature_growth_optimizer
from betbot.kalshi_temperature_weather_pattern import run_kalshi_temperature_weather_pattern


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return float(parsed)


def _safe_int(value: Any) -> int:
    parsed = _safe_float(value)
    if parsed is None:
        return 0
    return int(round(parsed))


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _text(value).lower()
    if text in {"1", "true", "t", "yes", "y", "on", "active"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off", "inactive"}:
        return False
    return False


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = _text(value).lower()
    if text in {"1", "true", "t", "yes", "y", "on", "active"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off", "inactive"}:
        return False
    return None


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if isinstance(parsed, float):
            return parsed
    return None


def _is_settled_outcome_coverage_blocker(key: Any) -> bool:
    normalized = _text(key).lower()
    if not normalized:
        return False
    if normalized == "insufficient_settled_outcomes":
        return True
    references_settled_outcomes = "settled_outcome" in normalized or "settled_outcomes" in normalized
    references_stalled_growth = (
        ("growth" in normalized or "trend" in normalized) and ("stall" in normalized or "stalled" in normalized)
    )
    return references_settled_outcomes and references_stalled_growth


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name = f".{path.name}.tmp-{os.getpid()}-{datetime.now(timezone.utc).timestamp():.6f}"
    temp_path = path.with_name(temp_name)
    try:
        temp_path.write_text(text, encoding="utf-8")
        temp_path.replace(path)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass


def _load_json_file(path: Path) -> dict[str, Any]:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, dict) else {}


def _normalize_market_side_target(value: Any) -> str:
    raw_text = _text(value)
    raw_dict = _as_dict(value)
    if raw_dict:
        raw_text = _text(
            raw_dict.get("bucket")
            or raw_dict.get("market_side")
            or raw_dict.get("target")
            or raw_dict.get("ticker")
            or raw_dict.get("market")
            or raw_dict.get("selection")
            or raw_dict.get("side")
            or value
        )
        ticker = _text(
            raw_dict.get("ticker")
            or raw_dict.get("market_ticker")
            or raw_dict.get("market")
            or raw_dict.get("contract_ticker")
        ).upper()
        side = _text(raw_dict.get("side") or raw_dict.get("market_side") or raw_dict.get("selection")).lower()
        if ticker and side in {"yes", "no"}:
            return f"{ticker}|{side}"
    if not raw_text:
        return ""
    if "|" in raw_text:
        left_raw, right_raw = raw_text.split("|", 1)
        left = _text(left_raw).upper()
        right = _text(right_raw).lower()
        if left and right in {"yes", "no"}:
            return f"{left}|{right}"
        if left and right:
            return f"{left}|{right}"
    return raw_text


def _normalize_top_missing_market_side_targets(decision_metrics: dict[str, Any], limit: int = 20) -> list[str]:
    targets: list[str] = []
    seen: set[str] = set()

    def add_target(raw_value: Any) -> None:
        normalized = _normalize_market_side_target(raw_value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        targets.append(normalized)

    top_missing_coverage_buckets = _as_dict(decision_metrics.get("execution_cost_top_missing_coverage_buckets"))
    by_market_side = top_missing_coverage_buckets.get("by_market_side")
    if isinstance(by_market_side, list):
        for row_raw in by_market_side:
            add_target(row_raw)
            if len(targets) >= limit:
                return targets[:limit]

    if targets:
        return targets[:limit]

    fallback = decision_metrics.get("execution_cost_top_missing_market_side")
    if isinstance(fallback, list):
        for row_raw in fallback:
            add_target(row_raw)
            if len(targets) >= limit:
                return targets[:limit]
    else:
        add_target(fallback)

    return targets[:limit]


def _normalize_market_ticker_target(value: Any) -> str:
    raw_dict = _as_dict(value)
    raw_text = ""
    if raw_dict:
        ticker = _text(
            raw_dict.get("ticker")
            or raw_dict.get("market_ticker")
            or raw_dict.get("contract_ticker")
            or raw_dict.get("market")
            or raw_dict.get("bucket")
            or raw_dict.get("target")
        )
        if ticker:
            raw_text = ticker
        else:
            raw_text = _text(
                raw_dict.get("bucket")
                or raw_dict.get("market")
                or raw_dict.get("ticker")
                or raw_dict.get("market_ticker")
                or raw_dict.get("contract_ticker")
                or raw_dict.get("target")
                or value
            )
    else:
        raw_text = _text(value)
    if not raw_text:
        return ""
    if "|" in raw_text:
        raw_text = raw_text.split("|", 1)[0]
    return raw_text.strip().upper()


def _normalize_top_missing_market_tickers(decision_metrics: dict[str, Any], limit: int = 20) -> list[str]:
    tickers: list[str] = []
    seen: set[str] = set()

    def add_ticker(raw_value: Any) -> None:
        normalized = _normalize_market_ticker_target(raw_value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        tickers.append(normalized)

    top_missing_coverage_buckets = _as_dict(decision_metrics.get("execution_cost_top_missing_coverage_buckets"))
    by_market = top_missing_coverage_buckets.get("by_market")
    if isinstance(by_market, list):
        for row_raw in by_market:
            add_ticker(row_raw)
            if len(tickers) >= limit:
                return tickers[:limit]

    return tickers[:limit]


def _resolve_execution_cost_source_file(*, source_path: str, output_dir: Path | None) -> Path | None:
    source_text = _text(source_path)
    if not source_text:
        return None
    raw_path = Path(source_text)
    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        if isinstance(output_dir, Path):
            candidates.extend(
                [
                    output_dir / source_text,
                    output_dir / "health" / source_text,
                    output_dir.parent / source_text,
                ]
            )
        candidates.append(raw_path)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _latest_execution_cost_tape_artifact(output_dir: Path) -> Path | None:
    preferred = output_dir / "health" / "execution_cost_tape_latest.json"
    if preferred.is_file():
        return preferred
    matches = list((output_dir / "health").glob("execution_cost_tape_*.json"))
    if not matches:
        return None
    return max(matches, key=lambda path: (path.stat().st_mtime, path.name))


def _latest_execution_cost_exclusions_state_artifact(output_dir: Path) -> Path | None:
    preferred = output_dir / "health" / "execution_cost_exclusions_state_latest.json"
    if preferred.is_file():
        return preferred
    fallback = output_dir / "execution_cost_exclusions_state_latest.json"
    if fallback.is_file():
        return fallback
    matches = list((output_dir / "health").glob("execution_cost_exclusions_state_*.json"))
    if not matches:
        return None
    return max(matches, key=lambda path: (path.stat().st_mtime, path.name))


def _normalize_execution_siphon_trend(*payloads: dict[str, Any]) -> dict[str, Any]:
    trend_payloads: list[dict[str, Any]] = []

    def _add_trend_payload(raw: Any) -> None:
        payload = _as_dict(raw)
        if payload:
            trend_payloads.append(payload)

    def _first_present(container: dict[str, Any], keys: tuple[str, ...]) -> Any:
        for key in keys:
            if key in container:
                return container.get(key)
        return None

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        _add_trend_payload(payload.get("execution_siphon_trend"))
        _add_trend_payload(payload.get("siphon_trend"))
        execution_siphon_pressure_payload = _as_dict(payload.get("execution_siphon_pressure"))
        _add_trend_payload(execution_siphon_pressure_payload.get("execution_siphon_trend"))
        _add_trend_payload(execution_siphon_pressure_payload.get("trend"))

    trend_payload = trend_payloads[0] if trend_payloads else {}

    trend_status = ""
    for current in trend_payloads:
        trend_status = _text(current.get("status") or current.get("trend_status"))
        if trend_status:
            break
    if not trend_status:
        for payload in payloads:
            trend_status = _text(_as_dict(payload).get("execution_siphon_trend_status"))
            if trend_status:
                break

    baseline_file = ""
    for current in trend_payloads:
        baseline_file = _text(current.get("baseline_file") or current.get("baseline_source"))
        if baseline_file:
            break
    if not baseline_file:
        for payload in payloads:
            baseline_file = _text(_as_dict(payload).get("execution_siphon_trend_baseline_file"))
            if baseline_file:
                break

    worsening = _optional_bool(
        _first_present(
            trend_payload,
            ("worsening", "trend_worsening", "is_worsening"),
        )
    )
    if worsening is None:
        for current in trend_payloads:
            worsening = _optional_bool(
                _first_present(
                    current,
                    ("worsening", "trend_worsening", "is_worsening"),
                )
            )
            if worsening is not None:
                break
    if worsening is None:
        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            worsening = _optional_bool(payload.get("execution_siphon_trend_worsening"))
            if worsening is not None:
                break
    if worsening is None:
        direction = _text(trend_payload.get("direction") or trend_payload.get("trend") or trend_payload.get("status")).lower()
        if direction in {"worsening", "worse", "up", "increasing", "higher", "hotter", "deteriorating"}:
            worsening = True
        elif direction in {"improving", "better", "down", "decreasing", "lower", "cooling", "recovering"}:
            worsening = False

    quote_coverage_ratio_delta = _first_float(
        trend_payload.get("quote_coverage_ratio_delta"),
        trend_payload.get("coverage_ratio_delta"),
        trend_payload.get("quote_coverage_delta"),
        *(
            _as_dict(payload).get("execution_siphon_trend_quote_coverage_ratio_delta")
            for payload in payloads
            if isinstance(payload, dict)
        ),
        *(
            _as_dict(payload).get("execution_siphon_quote_coverage_ratio_delta")
            for payload in payloads
            if isinstance(payload, dict)
        ),
    )
    if quote_coverage_ratio_delta is None:
        for current in trend_payloads:
            quote_coverage_ratio_delta = _first_float(
                current.get("quote_coverage_ratio_delta"),
                current.get("coverage_ratio_delta"),
                current.get("quote_coverage_delta"),
            )
            if isinstance(quote_coverage_ratio_delta, float):
                break

    pressure_delta = _first_float(
        trend_payload.get("siphon_pressure_score_delta"),
        trend_payload.get("pressure_score_delta"),
        trend_payload.get("pressure_delta"),
        trend_payload.get("siphon_pressure_delta"),
        trend_payload.get("execution_siphon_pressure_delta"),
        *(
            _as_dict(payload).get("execution_siphon_trend_siphon_pressure_score_delta")
            for payload in payloads
            if isinstance(payload, dict)
        ),
        *(
            _as_dict(payload).get("execution_siphon_pressure_score_delta")
            for payload in payloads
            if isinstance(payload, dict)
        ),
        *(
            _as_dict(payload).get("execution_siphon_trend_pressure_delta")
            for payload in payloads
            if isinstance(payload, dict)
        ),
        *(
            _as_dict(payload).get("execution_siphon_pressure_delta")
            for payload in payloads
            if isinstance(payload, dict)
        ),
    )
    if pressure_delta is None:
        for current in trend_payloads:
            pressure_delta = _first_float(
                current.get("siphon_pressure_score_delta"),
                current.get("pressure_score_delta"),
                current.get("pressure_delta"),
                current.get("siphon_pressure_delta"),
                current.get("execution_siphon_pressure_delta"),
            )
            if isinstance(pressure_delta, float):
                break

    candidate_rows_delta = _first_float(
        trend_payload.get("candidate_rows_delta"),
        trend_payload.get("execution_candidate_rows_delta"),
        *(
            _as_dict(payload).get("execution_siphon_trend_candidate_rows_delta")
            for payload in payloads
            if isinstance(payload, dict)
        ),
        *(
            _as_dict(payload).get("execution_siphon_candidate_rows_delta")
            for payload in payloads
            if isinstance(payload, dict)
        ),
    )
    if candidate_rows_delta is None:
        for current in trend_payloads:
            candidate_rows_delta = _first_float(
                current.get("candidate_rows_delta"),
                current.get("execution_candidate_rows_delta"),
            )
            if isinstance(candidate_rows_delta, float):
                break
    candidate_rows_delta_int = (
        int(round(float(candidate_rows_delta)))
        if isinstance(candidate_rows_delta, float)
        else None
    )

    has_any_delta = bool(
        isinstance(quote_coverage_ratio_delta, float)
        or isinstance(pressure_delta, float)
        or isinstance(candidate_rows_delta_int, int)
    )
    if worsening is None and has_any_delta:
        worsening = bool(
            (
                isinstance(quote_coverage_ratio_delta, float)
                and quote_coverage_ratio_delta < 0.0
            )
            or (
                isinstance(pressure_delta, float)
                and pressure_delta > 0.0
            )
            or (
                isinstance(candidate_rows_delta_int, int)
                and candidate_rows_delta_int < 0
            )
        )

    material_worsening = bool(
        worsening is True
        and (
            (
                isinstance(quote_coverage_ratio_delta, float)
                and quote_coverage_ratio_delta <= -0.05
            )
            or (
                isinstance(pressure_delta, float)
                and pressure_delta >= 0.05
            )
            or (
                isinstance(candidate_rows_delta_int, int)
                and candidate_rows_delta_int <= -20
            )
        )
    )

    return {
        "status": trend_status or None,
        "baseline_file": baseline_file or None,
        "worsening": worsening,
        "quote_coverage_ratio_delta": quote_coverage_ratio_delta,
        "pressure_delta": pressure_delta,
        "siphon_pressure_score_delta": pressure_delta,
        "candidate_rows_delta": candidate_rows_delta_int,
        "material_worsening": material_worsening,
    }


def _normalize_execution_siphon_side_pressure(*payloads: dict[str, Any]) -> dict[str, Any]:
    side_pressure_payloads: list[dict[str, Any]] = []
    source_payloads: list[dict[str, Any]] = []

    def _add_side_pressure_payload(raw: Any) -> None:
        payload = _as_dict(raw)
        if payload:
            side_pressure_payloads.append(payload)

    def _normalize_ratio(value: Any) -> float | None:
        parsed = _safe_float(value)
        if not isinstance(parsed, float):
            return None
        if parsed > 1.0 and parsed <= 100.0:
            parsed = float(parsed) / 100.0
        return round(max(0.0, min(1.0, float(parsed))), 6)

    def _normalize_side(value: Any) -> str | None:
        side = _text(value).lower()
        if not side:
            return None
        return side

    def _first_from_keys(
        containers: list[dict[str, Any]],
        keys: tuple[str, ...],
        normalizer: Any,
    ) -> Any:
        for container in containers:
            for key in keys:
                if key not in container:
                    continue
                normalized = normalizer(container.get(key))
                if normalized is not None:
                    return normalized
        return None

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        source_payloads.append(payload)
        _add_side_pressure_payload(payload.get("execution_siphon_side_pressure"))
        _add_side_pressure_payload(payload.get("siphon_side_pressure"))
        _add_side_pressure_payload(payload.get("execution_side_pressure"))
        _add_side_pressure_payload(payload.get("side_pressure"))
        execution_siphon_payload = _as_dict(payload.get("execution_siphon_pressure"))
        if execution_siphon_payload:
            source_payloads.append(execution_siphon_payload)
            _add_side_pressure_payload(execution_siphon_payload.get("execution_siphon_side_pressure"))
            _add_side_pressure_payload(execution_siphon_payload.get("siphon_side_pressure"))
            _add_side_pressure_payload(execution_siphon_payload.get("execution_side_pressure"))
            _add_side_pressure_payload(execution_siphon_payload.get("side_pressure"))
            if any(
                key in execution_siphon_payload
                for key in (
                    "dominant_side",
                    "dominant_market_side",
                    "dominant_uncovered_side",
                    "dominant_side_share",
                    "dominant_share",
                    "dominant_market_side_share",
                    "dominant_uncovered_side_share",
                    "side_imbalance",
                    "imbalance",
                    "imbalance_share",
                    "side_imbalance_magnitude",
                    "side_pressure_score",
                    "imbalance_pressure_score",
                    "side_imbalance_pressure_score",
                    "side_pressure_score_contribution",
                )
            ):
                side_pressure_payloads.append(execution_siphon_payload)

    dominant_side = _first_from_keys(
        side_pressure_payloads,
        (
            "dominant_side",
            "dominant_market_side",
            "dominant_uncovered_side",
            "dominant_execution_side",
            "top_side",
            "primary_side",
            "side",
        ),
        _normalize_side,
    )
    if dominant_side is None:
        dominant_side = _first_from_keys(
            source_payloads,
            (
                "execution_siphon_side_pressure_dominant_side",
                "execution_side_pressure_dominant_side",
                "execution_siphon_dominant_side",
                "siphon_side_pressure_dominant_side",
            ),
            _normalize_side,
        )

    dominant_side_share = _first_from_keys(
        side_pressure_payloads,
        (
            "dominant_side_share",
            "dominant_share",
            "dominant_side_ratio",
            "dominant_market_side_share",
            "dominant_uncovered_side_share",
            "dominant_execution_side_share",
            "side_share",
        ),
        _normalize_ratio,
    )
    if dominant_side_share is None:
        dominant_side_share = _first_from_keys(
            source_payloads,
            (
                "execution_siphon_side_pressure_dominant_side_share",
                "execution_side_pressure_dominant_side_share",
                "execution_siphon_dominant_side_share",
                "siphon_side_pressure_dominant_side_share",
            ),
            _normalize_ratio,
        )

    side_imbalance = _first_from_keys(
        side_pressure_payloads,
        (
            "side_imbalance",
            "imbalance",
            "imbalance_share",
            "side_imbalance_magnitude",
            "side_share_imbalance",
            "dominant_side_imbalance",
            "absolute_side_imbalance",
        ),
        _normalize_ratio,
    )
    if side_imbalance is None:
        side_imbalance = _first_from_keys(
            source_payloads,
            (
                "execution_siphon_side_pressure_imbalance",
                "execution_side_pressure_imbalance",
                "execution_siphon_side_imbalance",
                "siphon_side_pressure_imbalance",
            ),
            _normalize_ratio,
        )

    side_pressure_score = _first_from_keys(
        side_pressure_payloads,
        (
            "side_pressure_score",
            "side_pressure_score_contribution",
            "imbalance_pressure_score",
            "side_imbalance_pressure_score",
            "side_imbalance_score",
            "pressure_score",
            "score",
        ),
        _normalize_ratio,
    )
    if side_pressure_score is None:
        side_pressure_score = _first_from_keys(
            source_payloads,
            (
                "execution_siphon_side_pressure_score",
                "execution_side_pressure_score",
                "execution_siphon_side_imbalance_pressure_score",
                "siphon_side_pressure_score",
            ),
            _normalize_ratio,
        )

    materially_high = bool(
        (
            isinstance(side_pressure_score, float)
            and side_pressure_score >= 0.65
        )
        or (
            isinstance(side_imbalance, float)
            and side_imbalance >= 0.30
        )
        or (
            isinstance(dominant_side_share, float)
            and dominant_side_share >= 0.70
            and isinstance(side_imbalance, float)
            and side_imbalance >= 0.20
        )
    )

    return {
        "dominant_side": dominant_side,
        "dominant_side_share": dominant_side_share,
        "side_imbalance": side_imbalance,
        "side_pressure_score": side_pressure_score,
        "materially_high": materially_high,
    }


def _normalize_execution_cost_exclusion_side_state(
    execution_cost_exclusions_state_payload: dict[str, Any],
) -> dict[str, Any]:
    payload = _as_dict(execution_cost_exclusions_state_payload)
    adaptive_downshift = _as_dict(payload.get("adaptive_downshift"))
    side_targets_payload = _as_dict(payload.get("side_targets"))
    adaptive_side_targets_payload = _as_dict(adaptive_downshift.get("side_targets"))
    sources = [payload, side_targets_payload, adaptive_downshift, adaptive_side_targets_payload]

    def _first_present_count(
        keys: tuple[str, ...],
    ) -> tuple[int, bool]:
        for source in sources:
            if not source:
                continue
            for key in keys:
                if key not in source:
                    continue
                raw_value = source.get(key)
                if isinstance(raw_value, list):
                    return max(0, len(raw_value)), True
                if isinstance(raw_value, dict):
                    return max(0, len(raw_value)), True
                return max(0, _safe_int(raw_value)), True
        return 0, False

    active_side_target_count, active_present = _first_present_count(
        (
            "active_market_side_target_count",
            "active_side_target_count",
            "active_market_side_targets_count",
            "active_side_targets_count",
            "market_side_target_active_count",
            "market_side_targets_active_count",
            "side_target_active_count",
            "side_targets_active_count",
            "execution_cost_exclusion_active_market_side_target_count",
            "active_market_side_targets",
            "active_side_targets",
            "active_market_side_target_keys",
            "active_side_target_keys",
            "market_side_targets_active",
            "side_targets_active",
            "execution_cost_exclusion_active_market_side_targets",
        )
    )
    candidate_side_target_count, candidate_present = _first_present_count(
        (
            "candidate_market_side_target_count",
            "candidate_side_target_count",
            "candidate_market_side_targets_count",
            "candidate_side_targets_count",
            "market_side_target_candidate_count",
            "market_side_targets_candidate_count",
            "side_target_candidate_count",
            "side_targets_candidate_count",
            "execution_cost_exclusion_candidate_market_side_target_count",
            "candidate_market_side_targets",
            "candidate_side_targets",
            "candidate_market_side_target_keys",
            "candidate_side_target_keys",
            "market_side_targets_candidate",
            "side_targets_candidate",
            "execution_cost_exclusion_candidate_market_side_targets",
        )
    )
    recently_downshifted_side_target_count, downshift_present = _first_present_count(
        (
            "recently_downshifted_market_side_target_count",
            "recently_downshifted_side_target_count",
            "recent_downshifted_market_side_target_count",
            "recent_downshifted_side_target_count",
            "last_downshifted_market_side_target_count",
            "last_downshifted_side_target_count",
            "last_drop_market_side_target_count",
            "last_drop_side_target_count",
            "last_drop_market_side_count",
            "last_drop_market_side_targets",
            "last_drop_side_targets",
            "recently_downshifted_market_side_targets",
            "recently_downshifted_side_targets",
            "recent_downshifted_market_side_targets",
            "recent_downshifted_side_targets",
            "last_downshifted_market_side_targets",
            "last_downshifted_side_targets",
        )
    )

    return {
        "active_side_target_count": int(active_side_target_count),
        "candidate_side_target_count": int(candidate_side_target_count),
        "recently_downshifted_side_target_count": int(recently_downshifted_side_target_count),
        "has_side_state": bool(active_present or candidate_present or downshift_present),
    }


def _latest_summary_artifact(output_dir: Path) -> tuple[Path | None, str]:
    search_roots = [output_dir, output_dir / "health"]

    preferred_candidates = [
        ("kalshi_temperature_trade_intents_summary_latest.json", "kalshi_temperature_trade_intents_summary_*.json"),
        ("kalshi_temperature_trade_plan_summary_latest.json", "kalshi_temperature_trade_plan_summary_*.json"),
    ]
    for exact_name, _ in preferred_candidates:
        for root in search_roots:
            path = root / exact_name
            if path.is_file():
                return path, "exact"

    artifact_matches: list[Path] = []
    for _, pattern in preferred_candidates:
        matches: list[Path] = []
        for root in search_roots:
            matches.extend(path for path in root.glob(pattern) if path.is_file())
        if matches:
            artifact_matches = matches
            break
    if not artifact_matches:
        return None, "missing"
    return max(artifact_matches, key=lambda path: (path.stat().st_mtime, path.name)), "glob"


def _normalize_suppression_metrics(output_dir: Path) -> dict[str, Any]:
    summary_path, source = _latest_summary_artifact(output_dir)
    summary_payload = _load_json_file(summary_path) if isinstance(summary_path, Path) else {}

    enabled = _safe_bool(summary_payload.get("weather_pattern_negative_regime_suppression_enabled"))
    active = _safe_bool(summary_payload.get("weather_pattern_negative_regime_suppression_active"))
    status = _text(summary_payload.get("weather_pattern_negative_regime_suppression_status")).lower() or "unknown"
    candidate_count = max(
        0,
        _safe_int(summary_payload.get("weather_pattern_negative_regime_suppression_candidate_count")),
    )
    blocked_count = max(
        0,
        _safe_int(summary_payload.get("weather_pattern_negative_regime_suppression_blocked_count")),
    )
    blocked_share = None
    denominator = max(candidate_count, blocked_count)
    if denominator > 0:
        blocked_share = round(float(blocked_count) / float(denominator), 6)

    return {
        "summary_file_used": str(summary_path) if isinstance(summary_path, Path) else "",
        "summary_available": bool(summary_payload),
        "summary_source": source,
        "enabled": enabled,
        "active": active,
        "status": status,
        "candidate_count": candidate_count,
        "blocked_count": blocked_count,
        "blocked_share": blocked_share,
    }


def _normalize_trade_plan_blocker_metrics(output_dir: Path) -> dict[str, Any]:
    summary_path, source = _latest_summary_artifact(output_dir)
    summary_payload = _load_json_file(summary_path) if isinstance(summary_path, Path) else {}
    intent_summary = _as_dict(summary_payload.get("intent_summary"))

    policy_reason_counts_raw = summary_payload.get("policy_reason_counts")
    if not isinstance(policy_reason_counts_raw, dict):
        policy_reason_counts_raw = intent_summary.get("policy_reason_counts")
    policy_reason_counts: dict[str, int] = {}
    if isinstance(policy_reason_counts_raw, dict):
        for key_raw, value_raw in policy_reason_counts_raw.items():
            key = _text(key_raw)
            if not key:
                continue
            policy_reason_counts[key] = max(0, _safe_int(value_raw))

    intents_total = _safe_int(summary_payload.get("intents_total"))
    if "intents_total" not in summary_payload:
        intents_total = _safe_int(intent_summary.get("intents_total"))

    intents_approved = _safe_int(summary_payload.get("intents_approved"))
    if "intents_approved" not in summary_payload:
        intents_approved = _safe_int(intent_summary.get("intents_approved"))

    return {
        "summary_file_used": str(summary_path) if isinstance(summary_path, Path) else "",
        "summary_available": bool(summary_payload),
        "summary_source": source,
        "intents_total": max(0, intents_total),
        "intents_approved": max(0, intents_approved),
        "policy_reason_counts": dict(sorted(policy_reason_counts.items(), key=lambda item: item[0])),
    }


def _normalize_recovery_watchdog_metrics(output_dir: Path) -> dict[str, Any]:
    summary_path = output_dir / "health" / "recovery" / "recovery_latest.json"
    summary_payload = _load_json_file(summary_path) if summary_path.is_file() else {}

    latest_stage_timeout_repair_action = ""
    latest_stage_timeout_repair_status = ""
    actions_attempted = summary_payload.get("actions_attempted")
    if isinstance(actions_attempted, list):
        for raw_action in reversed(actions_attempted):
            action = _text(raw_action)
            if not action.startswith("repair_coldmath_stage_timeout_guardrails:"):
                continue
            latest_stage_timeout_repair_action = action
            latest_stage_timeout_repair_status = action.rsplit(":", 1)[-1].strip().lower()
            break

    severe_issue = latest_stage_timeout_repair_status in {"missing_script", "failed"}
    return {
        "summary_file_used": str(summary_path) if summary_path.is_file() else "",
        "summary_available": bool(summary_payload),
        "latest_stage_timeout_repair_action": latest_stage_timeout_repair_action,
        "latest_stage_timeout_repair_status": latest_stage_timeout_repair_status,
        "severe_issue": severe_issue,
    }


def _latest_recovery_loop_summary_artifact(output_dir: Path) -> tuple[Path | None, str]:
    health_dir = output_dir / "health"
    latest_path = health_dir / "kalshi_temperature_recovery_loop_latest.json"
    if latest_path.is_file():
        return latest_path, "exact"

    matches = [path for path in health_dir.glob("kalshi_temperature_recovery_loop_*.json") if path.is_file()]
    if not matches:
        return None, "missing"
    return max(matches, key=lambda path: (path.stat().st_mtime, path.name)), "glob"


def _normalize_recovery_effectiveness_metrics(output_dir: Path) -> dict[str, Any]:
    summary_path, source = _latest_recovery_loop_summary_artifact(output_dir)
    summary_payload = _load_json_file(summary_path) if isinstance(summary_path, Path) else {}

    thresholds_payload = _as_dict(summary_payload.get("adaptive_effectiveness_thresholds"))
    min_executions = max(1, _safe_int(thresholds_payload.get("min_executions")) or 3)
    min_worsening_ratio = max(
        0.0,
        min(1.0, float(_safe_float(thresholds_payload.get("min_worsening_ratio")) or 0.8)),
    )
    min_average_delta = float(_safe_float(thresholds_payload.get("min_average_negative_share_delta")) or 0.0)
    thresholds_used = {
        "min_executions": int(min_executions),
        "min_worsening_ratio": round(float(min_worsening_ratio), 6),
        "min_average_negative_share_delta": round(float(min_average_delta), 6),
    }

    scoreboard: dict[str, dict[str, Any]] = {}
    persistently_harmful_actions: list[str] = []
    action_effectiveness = summary_payload.get("action_effectiveness")
    if isinstance(action_effectiveness, dict):
        for action_key_raw, row_raw in sorted(action_effectiveness.items(), key=lambda item: _text(item[0])):
            action_key = _text(action_key_raw)
            if not action_key:
                continue
            row = _as_dict(row_raw)
            executed_count = max(0, _safe_int(row.get("executed_count")))
            worsening_count = max(0, _safe_int(row.get("worsening_count")))
            worsening_ratio = (
                float(worsening_count) / float(executed_count)
                if executed_count > 0
                else 0.0
            )
            average_delta = float(_safe_float(row.get("average_negative_share_delta")) or 0.0)
            persistently_harmful = bool(
                executed_count >= min_executions
                and worsening_ratio >= min_worsening_ratio
                and average_delta > min_average_delta
            )
            scoreboard[action_key] = {
                "executed_count": int(executed_count),
                "worsening_count": int(worsening_count),
                "worsening_ratio": round(float(worsening_ratio), 6),
                "average_negative_share_delta": round(float(average_delta), 6),
                "persistently_harmful": persistently_harmful,
            }
            if persistently_harmful:
                persistently_harmful_actions.append(action_key)

    min_repeated_no_effect_count = max(2, _safe_int(summary_payload.get("min_repeated_no_effect_count")) or 2)
    no_effect_thresholds = {
        "min_repeated_no_effect_count": int(min_repeated_no_effect_count),
    }
    no_effect_tracker: dict[str, dict[str, Any]] = {}
    iteration_logs = summary_payload.get("iteration_logs")
    if isinstance(iteration_logs, list):
        for iteration_row_raw in iteration_logs:
            iteration_row = _as_dict(iteration_row_raw)
            if not iteration_row:
                continue
            iteration_number = max(0, _safe_int(iteration_row.get("iteration")))
            executed_actions = iteration_row.get("executed_actions")
            if not isinstance(executed_actions, list):
                continue
            for action_row_raw in executed_actions:
                action_row = _as_dict(action_row_raw)
                action_key = _text(action_row.get("key"))
                if not action_key:
                    continue
                effect_status = _text(action_row.get("effect_status")).lower()
                if effect_status != "no_effect":
                    continue
                effect_reason = _text(action_row.get("effect_reason")).lower() or "unspecified"
                tracker_row = no_effect_tracker.setdefault(
                    action_key,
                    {
                        "no_effect_count": 0,
                        "latest_effect_reason": "",
                        "latest_iteration": 0,
                        "reason_counts": {},
                    },
                )
                tracker_row["no_effect_count"] = max(0, _safe_int(tracker_row.get("no_effect_count"))) + 1
                tracker_row["latest_effect_reason"] = effect_reason
                tracker_row["latest_iteration"] = max(
                    max(0, _safe_int(tracker_row.get("latest_iteration"))),
                    iteration_number,
                )
                reason_counts = _as_dict(tracker_row.get("reason_counts"))
                reason_counts[effect_reason] = max(0, _safe_int(reason_counts.get(effect_reason))) + 1
                tracker_row["reason_counts"] = reason_counts

    no_effect_actions: dict[str, dict[str, Any]] = {}
    repeated_no_effect_blockers: list[dict[str, Any]] = []
    for action_key in sorted(no_effect_tracker):
        row = _as_dict(no_effect_tracker.get(action_key))
        no_effect_count = max(0, _safe_int(row.get("no_effect_count")))
        latest_effect_reason = _text(row.get("latest_effect_reason")).lower() or "unspecified"
        latest_iteration = max(0, _safe_int(row.get("latest_iteration")))
        reason_counts_raw = row.get("reason_counts")
        reason_counts: dict[str, int] = {}
        if isinstance(reason_counts_raw, dict):
            for reason_key_raw, count_raw in reason_counts_raw.items():
                reason_key = _text(reason_key_raw).lower()
                if not reason_key:
                    continue
                reason_counts[reason_key] = max(0, _safe_int(count_raw))
        repeated_no_effect = no_effect_count >= min_repeated_no_effect_count
        no_effect_actions[action_key] = {
            "no_effect_count": no_effect_count,
            "latest_effect_reason": latest_effect_reason,
            "latest_iteration": latest_iteration,
            "reason_counts": dict(sorted(reason_counts.items(), key=lambda item: item[0])),
            "repeated_no_effect": repeated_no_effect,
        }
        if repeated_no_effect:
            repeated_no_effect_blockers.append(
                {
                    "action_key": action_key,
                    "no_effect_count": no_effect_count,
                    "latest_effect_reason": latest_effect_reason,
                    "latest_iteration": latest_iteration,
                    "reason_counts": dict(sorted(reason_counts.items(), key=lambda item: item[0])),
                    "summary": (
                        f"{action_key} returned no_effect {no_effect_count} times "
                        f"(latest reason: {latest_effect_reason})."
                    ),
                }
            )

    repeated_no_effect_blockers.sort(
        key=lambda row: (
            -max(0, _safe_int(row.get("no_effect_count"))),
            -max(0, _safe_int(row.get("latest_iteration"))),
            _text(row.get("action_key")),
        )
    )
    repeated_no_effect_actions = [
        _text(row.get("action_key"))
        for row in repeated_no_effect_blockers
        if _text(row.get("action_key"))
    ]

    return {
        "summary_file_used": str(summary_path) if isinstance(summary_path, Path) else "",
        "summary_available": bool(summary_payload),
        "summary_source": source,
        "thresholds_used": thresholds_used,
        "scoreboard": scoreboard,
        "persistently_harmful_actions": persistently_harmful_actions,
        "no_effect_thresholds": no_effect_thresholds,
        "no_effect_actions": no_effect_actions,
        "repeated_no_effect_actions": repeated_no_effect_actions,
        "repeated_no_effect_blockers": repeated_no_effect_blockers,
    }


def _normalize_weather_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    profile = _as_dict(payload.get("profile"))
    regime_risk = _as_dict(profile.get("regime_risk"))
    risk_off = _as_dict(profile.get("risk_off_recommendation"))
    weather_pattern_profile = _as_dict(payload.get("weather_pattern_profile"))
    weather_pattern_regime = _as_dict(weather_pattern_profile.get("regime_risk"))
    overall = _as_dict(payload.get("overall"))

    observed_negative_expectancy_attempt_share = _first_float(
        regime_risk.get("negative_expectancy_attempt_share"),
        weather_pattern_regime.get("negative_expectancy_attempt_share"),
        payload.get("negative_expectancy_attempt_share"),
    )
    observed_stale_metar_negative_attempt_share = _first_float(
        regime_risk.get("stale_metar_negative_attempt_share"),
        weather_pattern_regime.get("stale_metar_negative_attempt_share"),
        payload.get("stale_metar_negative_attempt_share"),
    )
    observed_stale_metar_attempt_share = _first_float(
        regime_risk.get("stale_metar_attempt_share"),
        weather_pattern_regime.get("stale_metar_attempt_share"),
        payload.get("stale_metar_attempt_share"),
    )
    confidence_adjusted_negative_expectancy_attempt_share = _first_float(
        regime_risk.get("negative_expectancy_attempt_share_confidence_adjusted"),
        weather_pattern_regime.get("negative_expectancy_attempt_share_confidence_adjusted"),
        payload.get("negative_expectancy_attempt_share_confidence_adjusted"),
    )
    confidence_adjusted_stale_metar_negative_attempt_share = _first_float(
        regime_risk.get("stale_metar_negative_attempt_share_confidence_adjusted"),
        weather_pattern_regime.get("stale_metar_negative_attempt_share_confidence_adjusted"),
        payload.get("stale_metar_negative_attempt_share_confidence_adjusted"),
    )
    confidence_adjusted_stale_metar_attempt_share = _first_float(
        regime_risk.get("stale_metar_attempt_share_confidence_adjusted"),
        weather_pattern_regime.get("stale_metar_attempt_share_confidence_adjusted"),
        payload.get("stale_metar_attempt_share_confidence_adjusted"),
    )
    stale_negative_station_max_share = _first_float(
        regime_risk.get("stale_negative_station_max_share"),
        weather_pattern_regime.get("stale_negative_station_max_share"),
        payload.get("stale_negative_station_max_share"),
    )
    stale_negative_station_hhi = _first_float(
        regime_risk.get("stale_negative_station_hhi"),
        weather_pattern_regime.get("stale_negative_station_hhi"),
        payload.get("stale_negative_station_hhi"),
    )
    stale_negative_station_top_raw = regime_risk.get("stale_negative_station_top")
    if stale_negative_station_top_raw is None:
        stale_negative_station_top_raw = weather_pattern_regime.get("stale_negative_station_top")
    if stale_negative_station_top_raw is None:
        stale_negative_station_top_raw = payload.get("stale_negative_station_top")

    stale_negative_station_top: Any
    if isinstance(stale_negative_station_top_raw, dict):
        stale_negative_station_top = dict(stale_negative_station_top_raw)
    elif isinstance(stale_negative_station_top_raw, list):
        normalized_top_rows: list[Any] = []
        for row_raw in stale_negative_station_top_raw:
            if isinstance(row_raw, dict):
                normalized_top_rows.append(dict(row_raw))
                continue
            row_text = _text(row_raw)
            if row_text:
                normalized_top_rows.append(row_text)
        stale_negative_station_top = normalized_top_rows
    else:
        top_text = _text(stale_negative_station_top_raw)
        stale_negative_station_top = top_text or None
    negative_expectancy_attempt_share = (
        confidence_adjusted_negative_expectancy_attempt_share
        if isinstance(confidence_adjusted_negative_expectancy_attempt_share, float)
        else observed_negative_expectancy_attempt_share
    )
    stale_metar_negative_attempt_share = (
        confidence_adjusted_stale_metar_negative_attempt_share
        if isinstance(confidence_adjusted_stale_metar_negative_attempt_share, float)
        else observed_stale_metar_negative_attempt_share
    )
    stale_metar_attempt_share = (
        confidence_adjusted_stale_metar_attempt_share
        if isinstance(confidence_adjusted_stale_metar_attempt_share, float)
        else observed_stale_metar_attempt_share
    )
    attempts_total = _safe_int(
        overall.get("attempts_total")
        or regime_risk.get("attempts_total")
        or risk_off.get("sample_count")
        or risk_off.get("attempts_total")
    )

    risk_off_status = _text(risk_off.get("status")).lower() or "unknown"
    risk_off_reason = _text(risk_off.get("reason")).lower() or "unspecified"
    risk_off_active = bool(_safe_bool(risk_off.get("active")))
    if not risk_off_active:
        risk_off_active = risk_off_status in {
            "active",
            "risk_off",
            "risk_off_active",
            "risk_off_soft",
            "risk_off_hard",
            "risk_off_recommended",
            "risk_off_triggered",
        }

    return {
        "negative_expectancy_attempt_share": negative_expectancy_attempt_share,
        "stale_metar_negative_attempt_share": stale_metar_negative_attempt_share,
        "stale_metar_attempt_share": stale_metar_attempt_share,
        "stale_negative_station_max_share": stale_negative_station_max_share,
        "stale_negative_station_hhi": stale_negative_station_hhi,
        "stale_negative_station_top": stale_negative_station_top,
        "negative_expectancy_attempt_share_observed": observed_negative_expectancy_attempt_share,
        "stale_metar_negative_attempt_share_observed": observed_stale_metar_negative_attempt_share,
        "stale_metar_attempt_share_observed": observed_stale_metar_attempt_share,
        "negative_expectancy_attempt_share_confidence_adjusted": (
            confidence_adjusted_negative_expectancy_attempt_share
        ),
        "stale_metar_negative_attempt_share_confidence_adjusted": (
            confidence_adjusted_stale_metar_negative_attempt_share
        ),
        "stale_metar_attempt_share_confidence_adjusted": confidence_adjusted_stale_metar_attempt_share,
        "attempts_total": attempts_total,
        "risk_off": {
            "active": risk_off_active,
            "status": risk_off_status,
            "reason": risk_off_reason,
        },
    }


def _normalize_decision_matrix_metrics(payload: dict[str, Any], *, output_dir: Path | None = None) -> dict[str, Any]:
    observed_metrics = _as_dict(payload.get("observed_metrics"))
    bootstrap_signal = _as_dict(payload.get("bootstrap_signal"))
    bootstrap_observed = _as_dict(bootstrap_signal.get("observed"))
    thresholds = _as_dict(payload.get("thresholds"))
    data_sources = _as_dict(payload.get("data_sources"))
    blocker_audit_source = _as_dict(data_sources.get("blocker_audit"))
    execution_cost_source = _as_dict(data_sources.get("execution_cost_tape"))
    execution_cost_source_path = _text(execution_cost_source.get("source"))
    execution_cost_source_file = _resolve_execution_cost_source_file(
        source_path=execution_cost_source_path,
        output_dir=output_dir,
    )
    supplemental_execution_cost_payload = (
        _load_json_file(execution_cost_source_file)
        if isinstance(execution_cost_source_file, Path)
        else {}
    )
    if not supplemental_execution_cost_payload and isinstance(output_dir, Path):
        latest_execution_cost_path = _latest_execution_cost_tape_artifact(output_dir)
        if isinstance(latest_execution_cost_path, Path):
            supplemental_execution_cost_payload = _load_json_file(latest_execution_cost_path)
    execution_cost_exclusions_state_payload = _as_dict(execution_cost_source.get("execution_cost_exclusions_state"))
    if not execution_cost_exclusions_state_payload:
        execution_cost_exclusions_state_payload = _as_dict(
            supplemental_execution_cost_payload.get("execution_cost_exclusions_state")
        )
    if not execution_cost_exclusions_state_payload and isinstance(output_dir, Path):
        latest_execution_cost_exclusions_state_path = _latest_execution_cost_exclusions_state_artifact(output_dir)
        if isinstance(latest_execution_cost_exclusions_state_path, Path):
            execution_cost_exclusions_state_payload = _load_json_file(latest_execution_cost_exclusions_state_path)
    data_pipeline_gaps_raw = payload.get("data_pipeline_gaps")
    data_pipeline_gaps: list[str] = []
    if isinstance(data_pipeline_gaps_raw, list):
        for gap_raw in data_pipeline_gaps_raw:
            gap = _text(gap_raw).lower()
            if gap:
                data_pipeline_gaps.append(gap)
    pipeline_backlog = payload.get("pipeline_backlog")
    weather_confidence_state = _as_dict(payload.get("weather_confidence_state"))
    if not weather_confidence_state:
        weather_confidence_state = _as_dict(_as_dict(payload.get("data_sources")).get("weather_confidence_state"))
    coverage_velocity_state = _as_dict(payload.get("coverage_velocity_state"))
    if not coverage_velocity_state:
        coverage_velocity_state = _as_dict(_as_dict(payload.get("data_sources")).get("coverage_velocity_state"))

    risk_off_recommended_raw: Any = observed_metrics.get("weather_risk_off_recommended")
    if risk_off_recommended_raw is None:
        risk_off_recommended_raw = bootstrap_observed.get("weather_risk_off_recommended")
    weather_risk_off_recommended = _safe_bool(risk_off_recommended_raw)

    settled_outcomes_delta_24h = _safe_float(observed_metrics.get("settled_outcomes_delta_24h"))
    settled_outcomes_delta_7d = _safe_float(observed_metrics.get("settled_outcomes_delta_7d"))
    combined_bucket_count_delta_24h = _safe_int(observed_metrics.get("combined_bucket_count_delta_24h"))
    combined_bucket_count_delta_7d = _safe_int(observed_metrics.get("combined_bucket_count_delta_7d"))
    targeted_constraint_rows = _safe_int(observed_metrics.get("targeted_constraint_rows"))
    top_bottlenecks_count = _safe_int(observed_metrics.get("top_bottlenecks_count"))
    bottleneck_source = _text(observed_metrics.get("bottleneck_source"))

    blocker_rows = payload.get("blocking_factors")
    normalized_blockers: list[dict[str, Any]] = []
    blocker_keys: list[str] = []
    weather_blocker_keys: list[str] = []
    weather_confidence_adjusted_signal_fallback_persistent = False
    settled_outcomes_blocker_present = False
    if isinstance(blocker_rows, list):
        for row in blocker_rows:
            row_dict = _as_dict(row)
            if not row_dict:
                continue
            key = _text(row_dict.get("key"))
            severity = _text(row_dict.get("severity")).lower() or "unknown"
            summary = _text(row_dict.get("summary"))
            normalized_blockers.append(
                {
                    "key": key,
                    "severity": severity,
                    "summary": summary,
                }
            )
            key_lower = key.lower()
            if key_lower:
                blocker_keys.append(key_lower)
            if "weather" in key_lower or "metar" in key_lower:
                weather_blocker_keys.append(key)
            if key_lower == "weather_confidence_adjusted_signal_fallback_persistent":
                weather_confidence_adjusted_signal_fallback_persistent = True
            if _is_settled_outcome_coverage_blocker(key_lower):
                settled_outcomes_blocker_present = True

    raw_fallback_persistent_raw: Any = observed_metrics.get("weather_confidence_adjusted_raw_fallback_persistent")
    if raw_fallback_persistent_raw is None:
        raw_fallback_persistent_raw = bootstrap_observed.get("weather_confidence_adjusted_raw_fallback_persistent")
    if raw_fallback_persistent_raw is None:
        raw_fallback_persistent_raw = weather_confidence_state.get("raw_fallback_persistent")

    raw_fallback_active_raw: Any = observed_metrics.get("weather_confidence_adjusted_raw_fallback_active")
    if raw_fallback_active_raw is None:
        raw_fallback_active_raw = bootstrap_observed.get("weather_confidence_adjusted_raw_fallback_active")
    if raw_fallback_active_raw is None:
        raw_fallback_active_raw = weather_confidence_state.get("raw_fallback_active")

    raw_fallback_count_raw: Any = observed_metrics.get("weather_confidence_adjusted_raw_fallback_consecutive_count")
    if raw_fallback_count_raw is None:
        raw_fallback_count_raw = bootstrap_observed.get("weather_confidence_adjusted_raw_fallback_consecutive_count")
    if raw_fallback_count_raw is None:
        raw_fallback_count_raw = weather_confidence_state.get("raw_fallback_consecutive_count")

    raw_fallback_threshold_raw: Any = thresholds.get("weather_confidence_adjusted_fallback_consecutive_threshold")
    if raw_fallback_threshold_raw is None:
        raw_fallback_threshold_raw = weather_confidence_state.get("raw_fallback_consecutive_threshold")

    raw_fallback_count = max(0, _safe_int(raw_fallback_count_raw))
    raw_fallback_threshold = max(1, _safe_int(raw_fallback_threshold_raw))
    weather_confidence_adjusted_signal_fallback_persistent = bool(
        weather_confidence_adjusted_signal_fallback_persistent
        or _safe_bool(raw_fallback_persistent_raw)
        or (_safe_bool(raw_fallback_active_raw) and raw_fallback_count >= raw_fallback_threshold)
    )

    coverage_velocity_active_raw: Any = observed_metrics.get("coverage_velocity_guardrail_active")
    if coverage_velocity_active_raw is None:
        coverage_velocity_active_raw = coverage_velocity_state.get("guardrail_active")
    coverage_velocity_cleared_raw: Any = observed_metrics.get("coverage_velocity_guardrail_cleared")
    if coverage_velocity_cleared_raw is None:
        coverage_velocity_cleared_raw = coverage_velocity_state.get("guardrail_cleared")
    coverage_velocity_positive_streak_raw: Any = observed_metrics.get("coverage_velocity_positive_streak")
    if coverage_velocity_positive_streak_raw is None:
        coverage_velocity_positive_streak_raw = coverage_velocity_state.get("positive_streak")
    coverage_velocity_non_positive_streak_raw: Any = observed_metrics.get("coverage_velocity_non_positive_streak")
    if coverage_velocity_non_positive_streak_raw is None:
        coverage_velocity_non_positive_streak_raw = coverage_velocity_state.get("non_positive_streak")
    coverage_velocity_required_positive_streak_raw: Any = thresholds.get(
        "coverage_velocity_required_positive_streak"
    )
    if coverage_velocity_required_positive_streak_raw is None:
        coverage_velocity_required_positive_streak_raw = coverage_velocity_state.get("required_positive_streak")
    coverage_velocity_selected_growth_delta_24h_raw: Any = observed_metrics.get(
        "coverage_velocity_selected_growth_delta_24h"
    )
    if coverage_velocity_selected_growth_delta_24h_raw is None:
        coverage_velocity_selected_growth_delta_24h_raw = coverage_velocity_state.get(
            "selected_growth_delta_24h"
        )
    coverage_velocity_selected_growth_delta_7d_raw: Any = observed_metrics.get(
        "coverage_velocity_selected_growth_delta_7d"
    )
    if coverage_velocity_selected_growth_delta_7d_raw is None:
        coverage_velocity_selected_growth_delta_7d_raw = coverage_velocity_state.get(
            "selected_growth_delta_7d"
        )
    coverage_velocity_selected_combined_bucket_count_delta_24h_raw: Any = observed_metrics.get(
        "coverage_velocity_selected_combined_bucket_count_delta_24h"
    )
    if coverage_velocity_selected_combined_bucket_count_delta_24h_raw is None:
        coverage_velocity_selected_combined_bucket_count_delta_24h_raw = coverage_velocity_state.get(
            "selected_combined_bucket_count_delta_24h"
        )
    coverage_velocity_selected_combined_bucket_count_delta_7d_raw: Any = observed_metrics.get(
        "coverage_velocity_selected_combined_bucket_count_delta_7d"
    )
    if coverage_velocity_selected_combined_bucket_count_delta_7d_raw is None:
        coverage_velocity_selected_combined_bucket_count_delta_7d_raw = coverage_velocity_state.get(
            "selected_combined_bucket_count_delta_7d"
        )

    settled_outcomes_raw: Any = observed_metrics.get("settled_outcomes")
    if settled_outcomes_raw is None:
        settled_outcomes_raw = bootstrap_observed.get("settled_outcomes")

    min_settled_outcomes_raw: Any = thresholds.get("min_settled_outcomes")
    if min_settled_outcomes_raw is None:
        min_settled_outcomes_raw = bootstrap_observed.get("min_settled_outcomes")

    settled_outcomes = _safe_float(settled_outcomes_raw)
    min_settled_outcomes = _safe_float(min_settled_outcomes_raw)
    settled_outcome_shortfall_detected = bool(
        isinstance(settled_outcomes, float)
        and isinstance(min_settled_outcomes, float)
        and min_settled_outcomes > 0.0
        and settled_outcomes < min_settled_outcomes
    )

    if "settled_outcome_growth_stalled" in observed_metrics:
        settled_outcome_growth_stalled = _safe_bool(observed_metrics.get("settled_outcome_growth_stalled"))
    else:
        settled_outcome_growth_stalled = bool(
            any(_text(row.get("key")).lower() == "settled_outcome_growth_stalled" for row in normalized_blockers)
        )

    telemetry_gap_keys: set[str] = set()
    telemetry_missing_contexts: set[str] = set()
    telemetry_related_blocker_keys = sorted(
        {
            key
            for key in blocker_keys
            if "execution_cost" in key
            or "ws_state" in key
            or "execution_journal" in key
            or "telemetry" in key
            or "blocker_audit" in key
        }
    )

    for gap_key in data_pipeline_gaps:
        if (
            "execution_cost" in gap_key
            or "ws_state" in gap_key
            or "execution_journal" in gap_key
            or "telemetry" in gap_key
            or "blocker_audit" in gap_key
        ):
            telemetry_gap_keys.add(gap_key)
        if "ws_state" in gap_key:
            telemetry_missing_contexts.add("ws_state")
        if "execution_journal" in gap_key:
            telemetry_missing_contexts.add("execution_journal")
        if "blocker_audit" in gap_key:
            telemetry_missing_contexts.add("blocker_audit")
        if "execution_cost_tape" in gap_key:
            telemetry_missing_contexts.add("execution_cost_tape")

    for blocker_key in telemetry_related_blocker_keys:
        telemetry_gap_keys.add(blocker_key)

    blocker_audit_status = _text(blocker_audit_source.get("status")).lower()
    blocker_audit_source_path = _text(blocker_audit_source.get("source"))
    blocker_audit_context_present = bool(blocker_audit_source) or any(
        "blocker_audit" in gap for gap in telemetry_gap_keys
    )
    if blocker_audit_context_present and (
        blocker_audit_status in {"missing", "stale", "failed", "error", "unknown", "unavailable", "degraded"}
        or (blocker_audit_status == "ready" and not blocker_audit_source_path)
    ):
        telemetry_missing_contexts.add("blocker_audit")
        if blocker_audit_status:
            telemetry_gap_keys.add(f"blocker_audit_status:{blocker_audit_status}")

    execution_cost_status = _text(execution_cost_source.get("status")).lower()
    if not execution_cost_status:
        execution_cost_status = _text(observed_metrics.get("execution_cost_tape_status")).lower()
    if not execution_cost_status:
        execution_cost_status = _text(supplemental_execution_cost_payload.get("status")).lower()
    if not execution_cost_source_path:
        execution_cost_source_path = _text(supplemental_execution_cost_payload.get("source"))
    execution_cost_meets_candidate_samples = (
        _safe_bool(observed_metrics.get("execution_cost_meets_candidate_samples"))
        if "execution_cost_meets_candidate_samples" in observed_metrics
        else None
    )
    execution_cost_meets_quote_coverage = (
        _safe_bool(observed_metrics.get("execution_cost_meets_quote_coverage"))
        if "execution_cost_meets_quote_coverage" in observed_metrics
        else None
    )
    execution_cost_calibration_readiness = _as_dict(execution_cost_source.get("calibration_readiness"))
    if not execution_cost_calibration_readiness:
        execution_cost_calibration_readiness = _as_dict(supplemental_execution_cost_payload.get("calibration_readiness"))
    execution_cost_observations = _as_dict(execution_cost_source.get("execution_cost_observations"))
    if not execution_cost_observations:
        execution_cost_observations = _as_dict(supplemental_execution_cost_payload.get("execution_cost_observations"))
    execution_siphon_payload = _as_dict(execution_cost_source.get("execution_siphon_pressure"))
    if not execution_siphon_payload:
        execution_siphon_payload = _as_dict(supplemental_execution_cost_payload.get("execution_siphon_pressure"))
    execution_siphon_trend = _normalize_execution_siphon_trend(
        observed_metrics,
        payload,
        execution_cost_source,
        execution_siphon_payload,
        supplemental_execution_cost_payload,
    )
    execution_siphon_side_pressure = _normalize_execution_siphon_side_pressure(
        observed_metrics,
        payload,
        execution_cost_source,
        execution_siphon_payload,
        supplemental_execution_cost_payload,
    )
    execution_siphon_side_pressure_dominant_side = (
        _text(execution_siphon_side_pressure.get("dominant_side")).lower() or None
    )
    execution_siphon_side_pressure_dominant_side_share = _safe_float(
        execution_siphon_side_pressure.get("dominant_side_share")
    )
    execution_siphon_side_pressure_imbalance = _safe_float(
        execution_siphon_side_pressure.get("side_imbalance")
    )
    execution_siphon_side_pressure_score = _safe_float(
        execution_siphon_side_pressure.get("side_pressure_score")
    )
    execution_siphon_side_pressure_materially_high = bool(
        execution_siphon_side_pressure.get("materially_high")
    )
    execution_cost_quote_coverage_decomposition = _as_dict(
        execution_cost_observations.get("quote_coverage_decomposition")
    )
    execution_cost_quote_coverage_by_event_type_raw = execution_cost_observations.get("quote_coverage_by_event_type")
    execution_cost_quote_coverage_by_event_type: list[dict[str, Any]] = []
    if isinstance(execution_cost_quote_coverage_by_event_type_raw, list):
        for row_raw in execution_cost_quote_coverage_by_event_type_raw:
            row = _as_dict(row_raw)
            if not row:
                continue
            execution_cost_quote_coverage_by_event_type.append(
                {
                    "event_type": _text(row.get("event_type")),
                    "rows": max(0, _safe_int(row.get("rows"))),
                    "rows_with_any_two_sided_quote": max(
                        0,
                        _safe_int(row.get("rows_with_any_two_sided_quote")),
                    ),
                    "rows_without_two_sided_quote": max(
                        0,
                        _safe_int(row.get("rows_without_two_sided_quote")),
                    ),
                    "quote_coverage_ratio": _safe_float(row.get("quote_coverage_ratio")),
                }
            )
    execution_cost_top_missing_coverage_buckets = _as_dict(
        execution_cost_observations.get("top_missing_coverage_buckets")
    )
    execution_cost_top_missing_market_tickers = _normalize_top_missing_market_tickers(
        {
            "execution_cost_top_missing_coverage_buckets": execution_cost_top_missing_coverage_buckets
        }
    )
    execution_cost_top_missing_market_side = ""
    execution_cost_top_missing_market_side_share: float | None = None
    top_missing_market_side_rows = execution_cost_top_missing_coverage_buckets.get("by_market_side")
    if isinstance(top_missing_market_side_rows, list):
        first_row = _as_dict(top_missing_market_side_rows[0]) if top_missing_market_side_rows else {}
        execution_cost_top_missing_market_side = _text(first_row.get("bucket"))
        execution_cost_top_missing_market_side_share = _safe_float(first_row.get("share_of_uncovered_rows"))
    if not execution_cost_top_missing_market_side:
        execution_cost_top_missing_market_side = _text(
            observed_metrics.get("execution_cost_top_missing_market_side")
        )
    if not execution_cost_top_missing_market_side:
        execution_cost_top_missing_market_side = _text(execution_cost_source.get("top_missing_market_side"))
    if not execution_cost_top_missing_market_side:
        execution_cost_top_missing_market_side = _text(
            supplemental_execution_cost_payload.get("top_missing_market_side")
        )
    if not execution_cost_top_missing_market_side:
        execution_cost_top_missing_market_side = _text(
            execution_cost_calibration_readiness.get("top_missing_market_side")
        )
    execution_siphon_pressure = _safe_float(observed_metrics.get("execution_siphon_pressure"))
    if execution_siphon_pressure is None:
        execution_siphon_pressure = _safe_float(payload.get("execution_siphon_pressure"))
    if execution_siphon_pressure is None:
        execution_siphon_pressure = _safe_float(execution_siphon_payload.get("pressure_score"))
    if execution_siphon_pressure is None:
        execution_siphon_pressure = _safe_float(execution_siphon_payload.get("siphon_pressure_score"))
    execution_cost_exclusion_side_state = _normalize_execution_cost_exclusion_side_state(
        execution_cost_exclusions_state_payload
    )
    execution_cost_exclusion_state_active_count = max(
        0,
        _safe_int(execution_cost_exclusions_state_payload.get("active_count")),
    )
    execution_cost_exclusion_state_candidate_count = max(
        0,
        _safe_int(execution_cost_exclusions_state_payload.get("candidate_count")),
    )
    execution_cost_exclusion_state_side_active_count = max(
        0,
        _safe_int(execution_cost_exclusion_side_state.get("active_side_target_count")),
    )
    execution_cost_exclusion_state_side_candidate_count = max(
        0,
        _safe_int(execution_cost_exclusion_side_state.get("candidate_side_target_count")),
    )
    execution_cost_exclusion_state_side_recent_downshift_count = max(
        0,
        _safe_int(execution_cost_exclusion_side_state.get("recently_downshifted_side_target_count")),
    )
    execution_cost_exclusion_state_side_state_present = bool(
        execution_cost_exclusion_side_state.get("has_side_state")
    )
    low_coverage_wide_spread_ticker_count = max(
        0,
        _safe_int(observed_metrics.get("low_coverage_wide_spread_ticker_count")),
    )
    if "low_coverage_wide_spread_ticker_count" not in observed_metrics:
        low_coverage_wide_spread_ticker_count = max(
            0,
            _safe_int(payload.get("low_coverage_wide_spread_ticker_count")),
        )
    if (
        "low_coverage_wide_spread_ticker_count" not in observed_metrics
        and "low_coverage_wide_spread_ticker_count" not in payload
    ):
        low_coverage_wide_spread_ticker_count = max(
            0,
            _safe_int(execution_siphon_payload.get("low_coverage_wide_spread_ticker_count")),
        )
    low_coverage_wide_spread_tickers_raw = observed_metrics.get("low_coverage_wide_spread_tickers")
    if low_coverage_wide_spread_tickers_raw is None:
        low_coverage_wide_spread_tickers_raw = payload.get("low_coverage_wide_spread_tickers")
    if low_coverage_wide_spread_tickers_raw is None:
        low_coverage_wide_spread_tickers_raw = execution_siphon_payload.get("low_coverage_wide_spread_tickers")
    low_coverage_wide_spread_tickers: list[str] = []
    if isinstance(low_coverage_wide_spread_tickers_raw, list):
        seen_low_coverage_wide_spread_tickers: set[str] = set()
        for row_raw in low_coverage_wide_spread_tickers_raw:
            ticker = _normalize_market_ticker_target(row_raw)
            if not ticker or ticker in seen_low_coverage_wide_spread_tickers:
                continue
            seen_low_coverage_wide_spread_tickers.add(ticker)
            low_coverage_wide_spread_tickers.append(ticker)
            if len(low_coverage_wide_spread_tickers) >= 20:
                break
    execution_cost_quote_coverage_ratio = _safe_float(observed_metrics.get("execution_cost_quote_coverage_ratio"))
    if execution_cost_quote_coverage_ratio is None:
        execution_cost_quote_coverage_ratio = _safe_float(observed_metrics.get("quote_coverage_ratio"))
    if execution_cost_quote_coverage_ratio is None:
        execution_cost_quote_coverage_ratio = _safe_float(execution_cost_source.get("quote_coverage_ratio"))
    if execution_cost_quote_coverage_ratio is None:
        execution_cost_quote_coverage_ratio = _safe_float(
            execution_cost_calibration_readiness.get("quote_coverage_ratio")
        )
    if execution_cost_quote_coverage_ratio is None:
        execution_cost_quote_coverage_ratio = _safe_float(
            supplemental_execution_cost_payload.get("quote_coverage_ratio")
        )
    execution_cost_min_quote_coverage_ratio = _safe_float(
        observed_metrics.get("execution_cost_min_quote_coverage_ratio")
    )
    if execution_cost_min_quote_coverage_ratio is None:
        execution_cost_min_quote_coverage_ratio = _safe_float(
            observed_metrics.get("min_quote_coverage_ratio")
        )
    if execution_cost_min_quote_coverage_ratio is None:
        execution_cost_min_quote_coverage_ratio = _safe_float(
            execution_cost_source.get("min_quote_coverage_ratio")
        )
    if execution_cost_min_quote_coverage_ratio is None:
        execution_cost_min_quote_coverage_ratio = _safe_float(
            execution_cost_calibration_readiness.get("min_quote_coverage_ratio")
        )
    if execution_cost_min_quote_coverage_ratio is None:
        execution_cost_min_quote_coverage_ratio = _safe_float(
            supplemental_execution_cost_payload.get("min_quote_coverage_ratio")
        )
    if execution_cost_meets_quote_coverage is None:
        execution_cost_meets_quote_coverage_raw = execution_cost_calibration_readiness.get(
            "meets_quote_coverage"
        )
        if execution_cost_meets_quote_coverage_raw is not None:
            execution_cost_meets_quote_coverage = _safe_bool(execution_cost_meets_quote_coverage_raw)
    execution_cost_quote_coverage_shortfall = (
        round(
            max(
                0.0,
                float(execution_cost_min_quote_coverage_ratio) - float(execution_cost_quote_coverage_ratio),
            ),
            6,
        )
        if (
            isinstance(execution_cost_quote_coverage_ratio, float)
            and isinstance(execution_cost_min_quote_coverage_ratio, float)
        )
        else None
    )
    if (
        execution_cost_meets_quote_coverage is None
        and isinstance(execution_cost_quote_coverage_shortfall, float)
    ):
        execution_cost_meets_quote_coverage = bool(execution_cost_quote_coverage_shortfall <= 0.0)
    execution_cost_context_present = (
        bool(execution_cost_source)
        or any("execution_cost" in gap for gap in telemetry_gap_keys)
        or any("execution_cost" in key for key in telemetry_related_blocker_keys)
        or "execution_cost_tape_status" in observed_metrics
    )
    if execution_cost_context_present and (
        execution_cost_status in {"missing", "stale", "failed", "error", "unknown", "unavailable", "degraded"}
        or (
            execution_cost_status == "ready"
            and (
                not execution_cost_source_path
                or execution_cost_meets_candidate_samples is False
                or execution_cost_meets_quote_coverage is False
            )
        )
    ):
        telemetry_missing_contexts.add("execution_cost_tape")
        if execution_cost_status:
            telemetry_gap_keys.add(f"execution_cost_tape_status:{execution_cost_status}")
    execution_cost_calibration_starvation_detected = bool(
        execution_cost_context_present
        and execution_cost_status in {"red", "yellow"}
        and (
            execution_cost_meets_candidate_samples is False
            or execution_cost_meets_quote_coverage is False
        )
    )
    execution_cost_quote_coverage_starvation_detected = bool(
        execution_cost_context_present
        and (
            execution_cost_meets_quote_coverage is False
            or (
                isinstance(execution_cost_quote_coverage_shortfall, float)
                and execution_cost_quote_coverage_shortfall > 0.0
            )
        )
    )
    if execution_cost_quote_coverage_starvation_detected:
        telemetry_gap_keys.add("execution_cost_quote_coverage_below_min")
    if execution_cost_calibration_starvation_detected and execution_cost_status:
        telemetry_gap_keys.add(f"execution_cost_tape_calibration:{execution_cost_status}")

    ws_state_status = _text(execution_cost_source.get("ws_state_status")).lower()
    execution_journal_status = _text(execution_cost_source.get("execution_journal_status")).lower()
    telemetry_expected_contexts: set[str] = set()
    if isinstance(pipeline_backlog, list):
        for backlog_row_raw in pipeline_backlog:
            backlog_row = _as_dict(backlog_row_raw)
            backlog_id = _text(backlog_row.get("id")).lower()
            if not backlog_id:
                continue
            if "execution_cost" not in backlog_id and "telemetry" not in backlog_id:
                continue
            telemetry_gap_keys.add(backlog_id)
            backlog_sources = backlog_row.get("data_sources")
            if not isinstance(backlog_sources, list):
                continue
            for source_raw in backlog_sources:
                source = _text(source_raw).lower()
                if not source:
                    continue
                if "ws_state" in source:
                    telemetry_expected_contexts.add("ws_state")
                if "execution_journal" in source or ("execution" in source and "journal" in source):
                    telemetry_expected_contexts.add("execution_journal")
                if "blocker_audit" in source:
                    telemetry_expected_contexts.add("blocker_audit")
                if "execution_cost_tape" in source:
                    telemetry_expected_contexts.add("execution_cost_tape")

    execution_cost_payload_gaps = execution_cost_source.get("data_pipeline_gaps")
    if not isinstance(execution_cost_payload_gaps, list):
        execution_cost_payload_gaps = execution_cost_source.get("data_gaps")
    if isinstance(execution_cost_payload_gaps, list):
        for gap_raw in execution_cost_payload_gaps:
            gap_key = _text(gap_raw).lower()
            if not gap_key:
                continue
            telemetry_gap_keys.add(gap_key)
            if "ws_state" in gap_key:
                telemetry_missing_contexts.add("ws_state")
            if "execution_journal" in gap_key:
                telemetry_missing_contexts.add("execution_journal")

    if "ws_state" in telemetry_expected_contexts and (
        ws_state_status in {"missing", "stale", "failed", "error", "unknown", "unavailable", "degraded"}
        or "ws_state" in telemetry_missing_contexts
        or "execution_cost_tape" in telemetry_missing_contexts
    ):
        telemetry_missing_contexts.add("ws_state")
    if "execution_journal" in telemetry_expected_contexts and (
        execution_journal_status in {"missing", "stale", "failed", "error", "unknown", "unavailable", "degraded"}
        or "execution_journal" in telemetry_missing_contexts
        or "execution_cost_tape" in telemetry_missing_contexts
    ):
        telemetry_missing_contexts.add("execution_journal")

    telemetry_starvation_detected = bool(
        telemetry_missing_contexts
        and (
            telemetry_gap_keys
            or bool(telemetry_related_blocker_keys)
            or bool(blocker_audit_source)
            or bool(execution_cost_source)
            or "execution_cost_tape_status" in observed_metrics
        )
    )

    return {
        "status": _text(payload.get("status")).lower() or "unknown",
        "weather_risk_off_recommended": weather_risk_off_recommended,
        "weather_confidence_adjusted_signal_fallback_persistent": (
            weather_confidence_adjusted_signal_fallback_persistent
        ),
        "coverage_velocity_guardrail_active": _safe_bool(coverage_velocity_active_raw),
        "coverage_velocity_guardrail_cleared": _safe_bool(coverage_velocity_cleared_raw),
        "coverage_velocity_positive_streak": max(0, _safe_int(coverage_velocity_positive_streak_raw)),
        "coverage_velocity_non_positive_streak": max(0, _safe_int(coverage_velocity_non_positive_streak_raw)),
        "coverage_velocity_required_positive_streak": max(
            1, _safe_int(coverage_velocity_required_positive_streak_raw) or 2
        ),
        "coverage_velocity_selected_growth_delta_24h": (
            _safe_int(coverage_velocity_selected_growth_delta_24h_raw)
            if coverage_velocity_selected_growth_delta_24h_raw is not None
            else None
        ),
        "coverage_velocity_selected_growth_delta_7d": (
            _safe_int(coverage_velocity_selected_growth_delta_7d_raw)
            if coverage_velocity_selected_growth_delta_7d_raw is not None
            else None
        ),
        "coverage_velocity_selected_combined_bucket_count_delta_24h": (
            _safe_int(coverage_velocity_selected_combined_bucket_count_delta_24h_raw)
            if coverage_velocity_selected_combined_bucket_count_delta_24h_raw is not None
            else None
        ),
        "coverage_velocity_selected_combined_bucket_count_delta_7d": (
            _safe_int(coverage_velocity_selected_combined_bucket_count_delta_7d_raw)
            if coverage_velocity_selected_combined_bucket_count_delta_7d_raw is not None
            else None
        ),
        "settled_outcomes_insufficient": bool(
            settled_outcomes_blocker_present or settled_outcome_shortfall_detected
        ),
        "settled_outcomes_delta_24h": settled_outcomes_delta_24h,
        "settled_outcomes_delta_7d": settled_outcomes_delta_7d,
        "combined_bucket_count_delta_24h": combined_bucket_count_delta_24h,
        "combined_bucket_count_delta_7d": combined_bucket_count_delta_7d,
        "targeted_constraint_rows": targeted_constraint_rows,
        "top_bottlenecks_count": top_bottlenecks_count,
        "bottleneck_source": bottleneck_source,
        "settled_outcome_growth_stalled": bool(settled_outcome_growth_stalled),
        "telemetry_starvation_detected": telemetry_starvation_detected,
        "execution_cost_calibration_starvation_detected": execution_cost_calibration_starvation_detected,
        "execution_cost_quote_coverage_starvation_detected": execution_cost_quote_coverage_starvation_detected,
        "execution_cost_meets_quote_coverage": execution_cost_meets_quote_coverage,
        "execution_cost_quote_coverage_ratio": execution_cost_quote_coverage_ratio,
        "execution_cost_min_quote_coverage_ratio": execution_cost_min_quote_coverage_ratio,
        "execution_cost_quote_coverage_shortfall": execution_cost_quote_coverage_shortfall,
        "execution_cost_quote_coverage_decomposition": execution_cost_quote_coverage_decomposition,
        "execution_cost_quote_coverage_by_event_type": execution_cost_quote_coverage_by_event_type,
        "execution_cost_top_missing_coverage_buckets": execution_cost_top_missing_coverage_buckets,
        "execution_cost_top_missing_market_tickers": execution_cost_top_missing_market_tickers,
        "execution_cost_top_missing_market_side": execution_cost_top_missing_market_side,
        "execution_cost_top_missing_market_side_share": execution_cost_top_missing_market_side_share,
        "execution_siphon_pressure": execution_siphon_pressure,
        "execution_siphon_trend_status": execution_siphon_trend.get("status"),
        "execution_siphon_trend_baseline_file": execution_siphon_trend.get("baseline_file"),
        "execution_siphon_trend_worsening": execution_siphon_trend.get("worsening"),
        "execution_siphon_trend_material_worsening": execution_siphon_trend.get("material_worsening"),
        "execution_siphon_trend_quote_coverage_ratio_delta": execution_siphon_trend.get(
            "quote_coverage_ratio_delta"
        ),
        "execution_siphon_trend_pressure_delta": execution_siphon_trend.get("pressure_delta"),
        "execution_siphon_trend_siphon_pressure_score_delta": execution_siphon_trend.get(
            "siphon_pressure_score_delta"
        ),
        "execution_siphon_trend_candidate_rows_delta": execution_siphon_trend.get("candidate_rows_delta"),
        "execution_siphon_side_pressure_dominant_side": execution_siphon_side_pressure_dominant_side,
        "execution_siphon_side_pressure_dominant_side_share": (
            execution_siphon_side_pressure_dominant_side_share
        ),
        "execution_siphon_side_pressure_imbalance": execution_siphon_side_pressure_imbalance,
        "execution_siphon_side_pressure_score": execution_siphon_side_pressure_score,
        "execution_siphon_side_pressure_materially_high": execution_siphon_side_pressure_materially_high,
        "execution_cost_exclusion_state_active_count": execution_cost_exclusion_state_active_count,
        "execution_cost_exclusion_state_candidate_count": execution_cost_exclusion_state_candidate_count,
        "execution_cost_exclusion_state_side_active_count": (
            execution_cost_exclusion_state_side_active_count
        ),
        "execution_cost_exclusion_state_side_candidate_count": (
            execution_cost_exclusion_state_side_candidate_count
        ),
        "execution_cost_exclusion_state_side_recent_downshift_count": (
            execution_cost_exclusion_state_side_recent_downshift_count
        ),
        "execution_cost_exclusion_state_side_state_present": (
            execution_cost_exclusion_state_side_state_present
        ),
        "low_coverage_wide_spread_ticker_count": low_coverage_wide_spread_ticker_count,
        "low_coverage_wide_spread_tickers": low_coverage_wide_spread_tickers,
        "telemetry_gap_keys": sorted(telemetry_gap_keys),
        "telemetry_related_blocker_keys": telemetry_related_blocker_keys,
        "telemetry_missing_contexts": sorted(telemetry_missing_contexts),
        "telemetry_source_statuses": {
            "blocker_audit": blocker_audit_status or None,
            "execution_cost_tape": execution_cost_status or None,
            "ws_state": ws_state_status or None,
            "execution_journal": execution_journal_status or None,
        },
        "blockers": normalized_blockers,
        "weather_blocker_keys": sorted(set(weather_blocker_keys)),
    }


def _normalize_growth_optimizer_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    search = _as_dict(payload.get("search"))
    robustness = _as_dict(search.get("robustness"))
    if not robustness:
        robustness = _as_dict(payload.get("robustness"))
    weather_risk = _as_dict(robustness.get("weather_risk"))
    execution_friction = _as_dict(robustness.get("execution_friction"))
    if not execution_friction:
        execution_friction = _as_dict(search.get("execution_friction"))
    if not execution_friction:
        execution_friction = _as_dict(payload.get("execution_friction"))
    inputs = _as_dict(payload.get("inputs"))
    weather_input = _as_dict(inputs.get("weather_pattern_artifact"))

    hard_block_active = _safe_bool(weather_risk.get("hard_block_active"))
    risk_off_recommended = _safe_bool(weather_risk.get("risk_off_recommended"))
    if not risk_off_recommended:
        risk_off_recommended = _safe_bool(weather_input.get("risk_off_recommended"))

    return {
        "status": _text(payload.get("status")).lower() or "unknown",
        "weather_risk": {
            "hard_block_active": hard_block_active,
            "risk_off_recommended": risk_off_recommended,
        },
        "execution_friction": {
            "available": _safe_bool(execution_friction.get("available")),
            "severe": _safe_bool(execution_friction.get("severe")),
            "penalty": _safe_float(execution_friction.get("penalty")),
            "weighted_penalty": _safe_float(execution_friction.get("weighted_penalty")),
            "evidence_coverage": _safe_float(execution_friction.get("evidence_coverage")),
            "quote_two_sided_ratio": _safe_float(execution_friction.get("quote_two_sided_ratio")),
            "spread_median_dollars": _safe_float(execution_friction.get("spread_median_dollars")),
            "spread_p90_dollars": _safe_float(execution_friction.get("spread_p90_dollars")),
        },
    }


def _max_gap(current: float | None, target_max: float) -> float | None:
    if not isinstance(current, float):
        return None
    return round(max(0.0, float(current) - float(target_max)), 6)


def _min_gap(current: int, target_min: int) -> int:
    return max(0, int(target_min) - int(current))


def _build_prioritized_actions(
    *,
    status: str,
    output_dir: Path,
    weather_metrics: dict[str, Any],
    suppression_metrics: dict[str, Any],
    trade_plan_metrics: dict[str, Any],
    recovery_watchdog_metrics: dict[str, Any],
    recovery_effectiveness_metrics: dict[str, Any],
    decision_metrics: dict[str, Any],
    optimizer_metrics: dict[str, Any],
    gap_to_clear: dict[str, Any],
    demotion_metadata: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    seen: set[str] = set()

    quoted_output_dir = json.dumps(str(output_dir))
    settled_outcomes_insufficient = bool(decision_metrics.get("settled_outcomes_insufficient"))
    coverage_velocity_guardrail_active = bool(decision_metrics.get("coverage_velocity_guardrail_active"))
    coverage_velocity_guardrail_cleared = bool(decision_metrics.get("coverage_velocity_guardrail_cleared"))
    weather_min_attempts_gap = max(0, _safe_int(gap_to_clear.get("weather_min_attempts")))
    settled_outcome_coverage_command_hint = (
        "python3 -m betbot.cli kalshi-temperature-settlement-state "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-settled-outcome-throughput "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-profitability "
        f"--output-dir {quoted_output_dir} --hours 720 && "
        "python3 -m betbot.cli decision-matrix-hardening "
        f"--output-dir {quoted_output_dir}"
    )
    bootstrap_shadow_trade_intents_command_hint = (
        "python3 -m betbot.cli kalshi-temperature-trader "
        f"--output-dir {quoted_output_dir} --intents-only "
        "--disable-weather-pattern-hardening "
        "--no-weather-pattern-risk-off-enabled "
        "--no-weather-pattern-negative-bucket-suppression-enabled "
        "--disable-historical-selection-quality "
        "--disable-enforce-probability-edge-thresholds"
    )
    refresh_market_horizon_inputs_command_hint = (
        "python3 -m betbot.cli kalshi-temperature-contract-specs "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-constraint-scan "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-settlement-state "
        f"--output-dir {quoted_output_dir}"
    )
    repair_metar_ingest_quality_pipeline_command_hint = (
        "python3 -m betbot.cli kalshi-temperature-metar-ingest "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-weather-pattern "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-trader "
        f"--output-dir {quoted_output_dir} --intents-only "
        "--weather-pattern-hardening-enabled"
    )
    reduce_stale_station_concentration_command_hint = (
        "python3 -m betbot.cli kalshi-temperature-metar-ingest "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-weather-pattern "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-trader "
        f"--output-dir {quoted_output_dir} --intents-only "
        "--weather-pattern-hardening-enabled"
    )
    repair_taf_station_mapping_pipeline_command_hint = (
        "python3 -m betbot.cli kalshi-temperature-contract-specs "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-constraint-scan "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-settlement-state "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-trader "
        f"--output-dir {quoted_output_dir} --intents-only "
        "--weather-pattern-hardening-enabled"
    )
    reduce_execution_friction_pressure_command_hint = (
        "python3 -m betbot.cli kalshi-temperature-execution-cost-tape "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-trader "
        f"--output-dir {quoted_output_dir} --intents-only "
        "--weather-pattern-hardening-enabled"
    )
    rebalance_weather_pattern_hard_block_pressure_command_hint = (
        "python3 -m betbot.cli kalshi-temperature-trader "
        f"--output-dir {quoted_output_dir} --intents-only "
        "--weather-pattern-hardening-enabled "
        "--weather-pattern-negative-bucket-suppression-enabled "
        "--weather-pattern-negative-bucket-suppression-min-samples 20 "
        "--weather-pattern-negative-bucket-suppression-negative-expectancy-threshold -0.08 && "
        "python3 -m betbot.cli kalshi-temperature-weather-pattern "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli decision-matrix-hardening "
        f"--output-dir {quoted_output_dir}"
    )
    repair_execution_telemetry_pipeline_command_hint = (
        "python3 -m betbot.cli kalshi-ws-state-collect "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-trader "
        f"--output-dir {quoted_output_dir} "
        "--disable-weather-pattern-hardening "
        "--no-weather-pattern-risk-off-enabled "
        "--no-weather-pattern-negative-bucket-suppression-enabled "
        "--disable-historical-selection-quality "
        "--disable-enforce-probability-edge-thresholds && "
        "python3 -m betbot.cli kalshi-temperature-execution-cost-tape "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli decision-matrix-hardening "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-recovery-advisor "
        f"--output-dir {quoted_output_dir}"
    )
    improve_execution_quote_coverage_shadow_command_hint = (
        "python3 -m betbot.cli kalshi-ws-state-collect "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-trader "
        f"--output-dir {quoted_output_dir} "
        "--disable-weather-pattern-hardening "
        "--no-weather-pattern-risk-off-enabled "
        "--no-weather-pattern-negative-bucket-suppression-enabled "
        "--disable-historical-selection-quality "
        "--disable-enforce-probability-edge-thresholds && "
        "python3 -m betbot.cli kalshi-temperature-execution-cost-tape "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli decision-matrix-hardening "
        f"--output-dir {quoted_output_dir} && "
        "python3 -m betbot.cli kalshi-temperature-recovery-advisor "
        f"--output-dir {quoted_output_dir}"
    )
    improve_execution_quote_coverage_shadow_details = {}
    top_missing_market_side_targets = _normalize_top_missing_market_side_targets(decision_metrics)
    if top_missing_market_side_targets:
        improve_execution_quote_coverage_shadow_details = {
            "top_missing_market_side_targets": top_missing_market_side_targets
        }

    def _add_action(*, key: str, command_hint: str, details: dict[str, Any] | None = None) -> None:
        if key in seen:
            return
        seen.add(key)
        action: dict[str, Any] = {"key": key, "command_hint": command_hint}
        if details:
            action["details"] = details
        actions.append(action)

    policy_reason_counts = (
        trade_plan_metrics.get("policy_reason_counts")
        if isinstance(trade_plan_metrics.get("policy_reason_counts"), dict)
        else {}
    )
    intents_total = max(0, _safe_int(trade_plan_metrics.get("intents_total")))
    metar_ingest_quality_insufficient = max(
        0,
        _safe_int(policy_reason_counts.get("metar_ingest_quality_insufficient")),
    )
    metar_ingest_quality_share = (
        float(metar_ingest_quality_insufficient) / float(intents_total)
        if intents_total > 0
        else 0.0
    )
    metar_ingest_quality_pipeline_pressure = bool(
        metar_ingest_quality_insufficient >= 8 and metar_ingest_quality_share >= 0.50
    )
    taf_station_missing = max(0, _safe_int(policy_reason_counts.get("taf_station_missing")))
    taf_station_missing_share = (
        float(taf_station_missing) / float(intents_total)
        if intents_total > 0
        else 0.0
    )
    taf_station_mapping_pressure = bool(
        taf_station_missing >= 8 and taf_station_missing_share >= 0.30
    )
    weather_pattern_multi_bucket_hard_block = max(
        0,
        _safe_int(policy_reason_counts.get("weather_pattern_multi_bucket_hard_block")),
    )
    weather_pattern_negative_regime_bucket_suppressed = max(
        0,
        _safe_int(policy_reason_counts.get("weather_pattern_negative_regime_bucket_suppressed")),
    )
    weather_pattern_hard_block_overconcentration_count = (
        weather_pattern_multi_bucket_hard_block + weather_pattern_negative_regime_bucket_suppressed
    )
    weather_pattern_hard_block_overconcentration_share = (
        float(weather_pattern_hard_block_overconcentration_count) / float(intents_total)
        if intents_total > 0
        else 0.0
    )
    weather_pattern_hard_block_overconcentration_pressure = bool(
        weather_pattern_hard_block_overconcentration_count >= 10
        and weather_pattern_hard_block_overconcentration_share >= 0.35
    )
    telemetry_starvation_detected = bool(decision_metrics.get("telemetry_starvation_detected"))
    execution_cost_calibration_starvation_detected = bool(
        decision_metrics.get("execution_cost_calibration_starvation_detected")
    )
    execution_cost_quote_coverage_starvation_detected = bool(
        decision_metrics.get("execution_cost_quote_coverage_starvation_detected")
    )
    execution_cost_meets_quote_coverage_raw = decision_metrics.get("execution_cost_meets_quote_coverage")
    execution_cost_meets_quote_coverage = (
        _safe_bool(execution_cost_meets_quote_coverage_raw)
        if execution_cost_meets_quote_coverage_raw is not None
        else None
    )
    execution_cost_quote_coverage_shortfall = _safe_float(
        decision_metrics.get("execution_cost_quote_coverage_shortfall")
    )
    execution_cost_top_missing_market_side = _text(decision_metrics.get("execution_cost_top_missing_market_side"))
    execution_cost_top_missing_market_side_share = _safe_float(
        decision_metrics.get("execution_cost_top_missing_market_side_share")
    )
    severe_quote_coverage_shortfall = bool(
        isinstance(execution_cost_quote_coverage_shortfall, float)
        and execution_cost_quote_coverage_shortfall >= 0.2
    )
    concentrated_quote_coverage_shortfall = bool(
        execution_cost_top_missing_market_side
        and isinstance(execution_cost_top_missing_market_side_share, float)
        and execution_cost_top_missing_market_side_share >= 0.25
    )
    telemetry_missing_contexts = decision_metrics.get("telemetry_missing_contexts")
    telemetry_gap_pressure = bool(
        telemetry_starvation_detected
        or execution_cost_calibration_starvation_detected
        or (
            isinstance(telemetry_missing_contexts, list)
            and any(_text(context) for context in telemetry_missing_contexts)
        )
    )
    quote_coverage_pressure = bool(
        execution_cost_quote_coverage_starvation_detected
        or execution_cost_meets_quote_coverage is False
        or (
            isinstance(execution_cost_quote_coverage_shortfall, float)
            and execution_cost_quote_coverage_shortfall > 0.0
        )
    )
    execution_siphon_side_pressure_dominant_side = _text(
        decision_metrics.get("execution_siphon_side_pressure_dominant_side")
    ).lower()
    execution_siphon_side_pressure_dominant_side_share = _safe_float(
        decision_metrics.get("execution_siphon_side_pressure_dominant_side_share")
    )
    execution_siphon_side_pressure_imbalance = _safe_float(
        decision_metrics.get("execution_siphon_side_pressure_imbalance")
    )
    execution_siphon_side_pressure_score = _safe_float(
        decision_metrics.get("execution_siphon_side_pressure_score")
    )
    execution_siphon_side_pressure_materially_high = bool(
        decision_metrics.get("execution_siphon_side_pressure_materially_high")
    )
    if not execution_siphon_side_pressure_materially_high:
        execution_siphon_side_pressure_materially_high = bool(
            (
                isinstance(execution_siphon_side_pressure_score, float)
                and execution_siphon_side_pressure_score >= 0.65
            )
            or (
                isinstance(execution_siphon_side_pressure_imbalance, float)
                and execution_siphon_side_pressure_imbalance >= 0.30
            )
            or (
                isinstance(execution_siphon_side_pressure_dominant_side_share, float)
                and execution_siphon_side_pressure_dominant_side_share >= 0.70
                and isinstance(execution_siphon_side_pressure_imbalance, float)
                and execution_siphon_side_pressure_imbalance >= 0.20
            )
        )
    siphon_side_pressure_context_details: dict[str, Any] = {}
    if execution_siphon_side_pressure_dominant_side:
        siphon_side_pressure_context_details["dominant_side"] = execution_siphon_side_pressure_dominant_side
    if isinstance(execution_siphon_side_pressure_dominant_side_share, float):
        siphon_side_pressure_context_details["dominant_side_share"] = round(
            float(execution_siphon_side_pressure_dominant_side_share),
            6,
        )
    if isinstance(execution_siphon_side_pressure_imbalance, float):
        siphon_side_pressure_context_details["side_imbalance"] = round(
            float(execution_siphon_side_pressure_imbalance),
            6,
        )
    if isinstance(execution_siphon_side_pressure_score, float):
        siphon_side_pressure_context_details["side_pressure_score"] = round(
            float(execution_siphon_side_pressure_score),
            6,
        )
    if siphon_side_pressure_context_details:
        improve_execution_quote_coverage_shadow_details = dict(improve_execution_quote_coverage_shadow_details)
        improve_execution_quote_coverage_shadow_details["siphon_side_pressure_context"] = dict(
            siphon_side_pressure_context_details
        )

    latest_repair_status = _text(recovery_watchdog_metrics.get("latest_stage_timeout_repair_status")).lower()
    if latest_repair_status == "missing_script":
        _add_action(
            key="restore_stage_timeout_guardrail_script",
            command_hint=(
                "bash infra/digitalocean/set_coldmath_stage_timeout_guardrails.sh "
                "--global-seconds 900 /etc/betbot/temperature-shadow.env && "
                "python3 -m betbot.cli coldmath-hardening "
                f"--output-dir {quoted_output_dir}"
            ),
        )
    elif latest_repair_status == "failed":
        _add_action(
            key="rerun_stage_timeout_guardrail_hardening",
            command_hint=(
                "bash infra/digitalocean/set_coldmath_stage_timeout_guardrails.sh "
                "--global-seconds 900 /etc/betbot/temperature-shadow.env && "
                "python3 -m betbot.cli coldmath-hardening "
                f"--output-dir {quoted_output_dir} && "
                "python3 -m betbot.cli kalshi-temperature-recovery-advisor "
                f"--output-dir {quoted_output_dir}"
            ),
        )

    if status == "insufficient_data":
        _add_action(
            key="bootstrap_shadow_trade_intents",
            command_hint=bootstrap_shadow_trade_intents_command_hint,
        )
        if telemetry_gap_pressure:
            _add_action(
                key="repair_execution_telemetry_pipeline",
                command_hint=repair_execution_telemetry_pipeline_command_hint,
            )
        if quote_coverage_pressure:
            _add_action(
                key="improve_execution_quote_coverage_shadow",
                command_hint=improve_execution_quote_coverage_shadow_command_hint,
                details=improve_execution_quote_coverage_shadow_details,
            )
        if metar_ingest_quality_pipeline_pressure:
            _add_action(
                key="repair_metar_ingest_quality_pipeline",
                command_hint=repair_metar_ingest_quality_pipeline_command_hint,
            )
        if settled_outcomes_insufficient:
            _add_action(
                key="increase_settled_outcome_coverage",
                command_hint=settled_outcome_coverage_command_hint,
            )
        if coverage_velocity_guardrail_active and not coverage_velocity_guardrail_cleared:
            _add_action(
                key="recover_settled_outcome_velocity",
                command_hint=(
                    "python3 -m betbot.cli kalshi-temperature-settled-outcome-throughput "
                    f"--output-dir {quoted_output_dir} && "
                    "python3 -m betbot.cli kalshi-temperature-profitability "
                    f"--output-dir {quoted_output_dir} --hours 720 && "
                    "python3 -m betbot.cli decision-matrix-hardening "
                    f"--output-dir {quoted_output_dir}"
                ),
            )
        _add_action(
            key="increase_weather_sample_coverage",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-weather-pattern "
                f"--output-dir {quoted_output_dir} --window-hours 1440"
            ),
        )
        _add_action(
            key="refresh_decision_matrix_weather_signals",
            command_hint=(
                "python3 -m betbot.cli decision-matrix-hardening "
                f"--output-dir {quoted_output_dir}"
            ),
        )
        return actions

    if status != "risk_off_active":
        if settled_outcomes_insufficient:
            _add_action(
                key="increase_settled_outcome_coverage",
                command_hint=settled_outcome_coverage_command_hint,
            )
        if coverage_velocity_guardrail_active and not coverage_velocity_guardrail_cleared:
            _add_action(
                key="recover_settled_outcome_velocity",
                command_hint=(
                    "python3 -m betbot.cli kalshi-temperature-settled-outcome-throughput "
                    f"--output-dir {quoted_output_dir} && "
                    "python3 -m betbot.cli kalshi-temperature-profitability "
                    f"--output-dir {quoted_output_dir} --hours 720 && "
                    "python3 -m betbot.cli decision-matrix-hardening "
                    f"--output-dir {quoted_output_dir}"
                ),
            )
        return actions

    if telemetry_gap_pressure:
        _add_action(
            key="repair_execution_telemetry_pipeline",
            command_hint=repair_execution_telemetry_pipeline_command_hint,
        )

    if weather_pattern_hard_block_overconcentration_pressure:
        _add_action(
            key="rebalance_weather_pattern_hard_block_pressure",
            command_hint=rebalance_weather_pattern_hard_block_pressure_command_hint,
        )

    stale_station_max_share = _safe_float(weather_metrics.get("stale_negative_station_max_share"))
    stale_station_hhi = _safe_float(weather_metrics.get("stale_negative_station_hhi"))
    stale_station_attempts = max(0, _safe_int(weather_metrics.get("attempts_total")))
    stale_station_max_share_threshold = 0.45
    stale_station_hhi_threshold = 0.30
    stale_station_hhi_min_attempts = 200
    stale_station_concentration_breach = bool(
        (isinstance(stale_station_max_share, float) and stale_station_max_share >= stale_station_max_share_threshold)
        or (
            isinstance(stale_station_hhi, float)
            and stale_station_hhi >= stale_station_hhi_threshold
            and stale_station_attempts >= stale_station_hhi_min_attempts
        )
    )
    if stale_station_concentration_breach:
        _add_action(
            key="reduce_stale_station_concentration",
            command_hint=reduce_stale_station_concentration_command_hint,
        )
    if taf_station_mapping_pressure:
        _add_action(
            key="repair_taf_station_mapping_pipeline",
            command_hint=repair_taf_station_mapping_pipeline_command_hint,
        )
    execution_friction = _as_dict(optimizer_metrics.get("execution_friction"))
    execution_friction_available = _safe_bool(execution_friction.get("available"))
    execution_friction_severe = _safe_bool(execution_friction.get("severe"))
    execution_friction_penalty = _safe_float(execution_friction.get("penalty"))
    execution_friction_weighted_penalty = _safe_float(execution_friction.get("weighted_penalty"))
    execution_friction_evidence_coverage = _safe_float(execution_friction.get("evidence_coverage"))
    execution_siphon_pressure = _safe_float(decision_metrics.get("execution_siphon_pressure"))
    execution_siphon_trend_status = _text(decision_metrics.get("execution_siphon_trend_status")).lower()
    execution_siphon_trend_worsening_raw = decision_metrics.get("execution_siphon_trend_worsening")
    execution_siphon_trend_worsening = (
        _safe_bool(execution_siphon_trend_worsening_raw)
        if execution_siphon_trend_worsening_raw is not None
        else None
    )
    execution_siphon_trend_quote_coverage_ratio_delta_raw = decision_metrics.get(
        "execution_siphon_trend_quote_coverage_ratio_delta"
    )
    execution_siphon_trend_quote_coverage_ratio_delta = (
        _safe_float(execution_siphon_trend_quote_coverage_ratio_delta_raw)
        if execution_siphon_trend_quote_coverage_ratio_delta_raw is not None
        else None
    )
    execution_siphon_trend_pressure_delta_raw = decision_metrics.get(
        "execution_siphon_trend_siphon_pressure_score_delta"
    )
    if execution_siphon_trend_pressure_delta_raw is None:
        execution_siphon_trend_pressure_delta_raw = decision_metrics.get("execution_siphon_trend_pressure_delta")
    execution_siphon_trend_pressure_delta = (
        _safe_float(execution_siphon_trend_pressure_delta_raw)
        if execution_siphon_trend_pressure_delta_raw is not None
        else None
    )
    execution_siphon_trend_candidate_rows_delta_raw = decision_metrics.get(
        "execution_siphon_trend_candidate_rows_delta"
    )
    execution_siphon_trend_candidate_rows_delta = (
        _safe_int(execution_siphon_trend_candidate_rows_delta_raw)
        if execution_siphon_trend_candidate_rows_delta_raw is not None
        else None
    )
    execution_cost_exclusion_state_active_count = max(
        0,
        _safe_int(decision_metrics.get("execution_cost_exclusion_state_active_count")),
    )
    execution_cost_exclusion_state_candidate_count = max(
        0,
        _safe_int(decision_metrics.get("execution_cost_exclusion_state_candidate_count")),
    )
    execution_cost_exclusion_state_side_active_count = max(
        0,
        _safe_int(decision_metrics.get("execution_cost_exclusion_state_side_active_count")),
    )
    execution_cost_exclusion_state_side_candidate_count = max(
        0,
        _safe_int(decision_metrics.get("execution_cost_exclusion_state_side_candidate_count")),
    )
    execution_cost_exclusion_state_side_recent_downshift_count = max(
        0,
        _safe_int(decision_metrics.get("execution_cost_exclusion_state_side_recent_downshift_count")),
    )
    execution_cost_exclusion_state_side_state_present = bool(
        decision_metrics.get("execution_cost_exclusion_state_side_state_present")
    )
    execution_siphon_side_pressure_side_state_weak = bool(
        execution_cost_exclusion_state_side_state_present
        and execution_cost_exclusion_state_side_active_count <= 2
    )
    execution_siphon_side_pressure_weak_state_promote_early = bool(
        execution_siphon_side_pressure_materially_high
        and (
            (execution_cost_exclusion_state_side_state_present and execution_siphon_side_pressure_side_state_weak)
            or not execution_cost_exclusion_state_side_state_present
        )
    )
    low_coverage_wide_spread_ticker_count = max(
        0,
        _safe_int(decision_metrics.get("low_coverage_wide_spread_ticker_count")),
    )
    execution_friction_penalty_pressure = bool(
        execution_friction_severe
        or (
            isinstance(execution_friction_penalty, float)
            and execution_friction_penalty >= 0.12
        )
        or (
            isinstance(execution_friction_weighted_penalty, float)
            and execution_friction_weighted_penalty >= 0.08
        )
    )
    execution_siphon_pressure_hot = bool(
        (
            isinstance(execution_siphon_pressure, float)
            and execution_siphon_pressure >= 0.6
        )
        or low_coverage_wide_spread_ticker_count >= 5
    )
    execution_friction_evidence_ready = bool(
        isinstance(execution_friction_evidence_coverage, float)
        and execution_friction_evidence_coverage >= 0.35
    )
    execution_siphon_trend_worsening_material = bool(
        decision_metrics.get("execution_siphon_trend_material_worsening")
    )
    if not execution_siphon_trend_worsening_material and execution_siphon_trend_worsening is True:
        execution_siphon_trend_worsening_material = bool(
            (
                isinstance(execution_siphon_trend_quote_coverage_ratio_delta, float)
                and execution_siphon_trend_quote_coverage_ratio_delta <= -0.05
            )
            or (
                isinstance(execution_siphon_trend_pressure_delta, float)
                and execution_siphon_trend_pressure_delta >= 0.05
            )
            or (
                isinstance(execution_siphon_trend_candidate_rows_delta, int)
                and execution_siphon_trend_candidate_rows_delta <= -20
            )
        )
    execution_siphon_pressure_promote_early = bool(
        (
            isinstance(execution_siphon_pressure, float)
            and execution_siphon_pressure >= 0.4
        )
        or execution_siphon_trend_worsening_material
        or execution_siphon_side_pressure_weak_state_promote_early
        or execution_cost_exclusion_state_active_count >= 10
    )
    top_missing_market_tickers = _normalize_top_missing_market_tickers(decision_metrics)
    reduce_execution_friction_pressure_details = {}
    if top_missing_market_tickers:
        reduce_execution_friction_pressure_details["top_missing_market_tickers"] = top_missing_market_tickers
    low_coverage_wide_spread_tickers_raw = decision_metrics.get("low_coverage_wide_spread_tickers")
    low_coverage_wide_spread_tickers: list[str] = []
    if isinstance(low_coverage_wide_spread_tickers_raw, list):
        seen_low_coverage_wide_spread_tickers: set[str] = set()
        for row_raw in low_coverage_wide_spread_tickers_raw:
            ticker = _normalize_market_ticker_target(row_raw)
            if not ticker or ticker in seen_low_coverage_wide_spread_tickers:
                continue
            seen_low_coverage_wide_spread_tickers.add(ticker)
            low_coverage_wide_spread_tickers.append(ticker)
            if len(low_coverage_wide_spread_tickers) >= 20:
                break
    if low_coverage_wide_spread_tickers:
        reduce_execution_friction_pressure_details["low_coverage_wide_spread_tickers"] = (
            low_coverage_wide_spread_tickers
        )
    if isinstance(execution_siphon_pressure, float):
        reduce_execution_friction_pressure_details["siphon_pressure_score"] = round(
            float(execution_siphon_pressure),
            6,
        )
    if execution_cost_exclusion_state_active_count > 0 or execution_cost_exclusion_state_candidate_count > 0:
        reduce_execution_friction_pressure_details["exclusion_state_active_count"] = (
            execution_cost_exclusion_state_active_count
        )
    if execution_siphon_trend_worsening_raw is not None:
        reduce_execution_friction_pressure_details["siphon_trend_worsening"] = execution_siphon_trend_worsening
    if execution_siphon_trend_quote_coverage_ratio_delta_raw is not None:
        reduce_execution_friction_pressure_details["siphon_trend_quote_coverage_ratio_delta"] = (
            execution_siphon_trend_quote_coverage_ratio_delta
        )
    if execution_siphon_trend_pressure_delta_raw is not None:
        reduce_execution_friction_pressure_details["siphon_trend_pressure_delta"] = (
            execution_siphon_trend_pressure_delta
        )
    if execution_siphon_trend_candidate_rows_delta_raw is not None:
        reduce_execution_friction_pressure_details["siphon_trend_candidate_rows_delta"] = (
            execution_siphon_trend_candidate_rows_delta
        )
    trend_context_details: dict[str, Any] = {}
    if execution_siphon_trend_status:
        trend_context_details["status"] = execution_siphon_trend_status
    if execution_siphon_trend_worsening_raw is not None:
        trend_context_details["worsening"] = execution_siphon_trend_worsening
    if execution_siphon_trend_quote_coverage_ratio_delta_raw is not None:
        trend_context_details["quote_coverage_ratio_delta"] = execution_siphon_trend_quote_coverage_ratio_delta
    if execution_siphon_trend_pressure_delta_raw is not None:
        trend_context_details["siphon_pressure_score_delta"] = execution_siphon_trend_pressure_delta
    if execution_siphon_trend_candidate_rows_delta_raw is not None:
        trend_context_details["candidate_rows_delta"] = execution_siphon_trend_candidate_rows_delta
    if execution_siphon_trend_worsening_material:
        trend_context_details["material_worsening"] = True
    if execution_siphon_trend_worsening_material and trend_context_details:
        reduce_execution_friction_pressure_details["siphon_trend_context"] = dict(trend_context_details)
        improve_execution_quote_coverage_shadow_details = dict(improve_execution_quote_coverage_shadow_details)
        improve_execution_quote_coverage_shadow_details["siphon_trend_context"] = dict(trend_context_details)
    if siphon_side_pressure_context_details:
        reduce_execution_friction_pressure_details["siphon_side_pressure_context"] = dict(
            siphon_side_pressure_context_details
        )
    siphon_side_exclusion_state_context_details: dict[str, Any] = {}
    if execution_cost_exclusion_state_side_state_present:
        siphon_side_exclusion_state_context_details = {
            "active_side_target_count": int(execution_cost_exclusion_state_side_active_count),
            "candidate_side_target_count": int(execution_cost_exclusion_state_side_candidate_count),
            "recently_downshifted_side_target_count": int(
                execution_cost_exclusion_state_side_recent_downshift_count
            ),
            "weak_side_target_state": bool(execution_siphon_side_pressure_side_state_weak),
        }
    if siphon_side_exclusion_state_context_details:
        improve_execution_quote_coverage_shadow_details = dict(improve_execution_quote_coverage_shadow_details)
        improve_execution_quote_coverage_shadow_details["siphon_side_exclusion_state_context"] = dict(
            siphon_side_exclusion_state_context_details
        )
        reduce_execution_friction_pressure_details["siphon_side_exclusion_state_context"] = dict(
            siphon_side_exclusion_state_context_details
        )
    execution_siphon_side_pressure_weak_state_friction_reorder = bool(
        execution_siphon_side_pressure_materially_high
        and execution_siphon_side_pressure_side_state_weak
        and quote_coverage_pressure
        and (
            execution_cost_quote_coverage_starvation_detected
            or severe_quote_coverage_shortfall
        )
    )
    execution_siphon_side_pressure_weak_state_friction_reorder_context: dict[str, Any] = {}
    if execution_siphon_side_pressure_weak_state_friction_reorder:
        execution_siphon_side_pressure_weak_state_friction_reorder_context = {
            "active": True,
            "reason": "weak_side_target_state_with_material_side_pressure_under_quote_coverage_stress",
            "quote_coverage_starvation_detected": bool(execution_cost_quote_coverage_starvation_detected),
            "severe_quote_coverage_shortfall": bool(severe_quote_coverage_shortfall),
        }
        if isinstance(execution_cost_quote_coverage_shortfall, float):
            execution_siphon_side_pressure_weak_state_friction_reorder_context[
                "quote_coverage_shortfall"
            ] = round(float(execution_cost_quote_coverage_shortfall), 6)
        reduce_execution_friction_pressure_details["execution_friction_ordering_context"] = dict(
            execution_siphon_side_pressure_weak_state_friction_reorder_context
        )
        improve_execution_quote_coverage_shadow_details = dict(improve_execution_quote_coverage_shadow_details)
        improve_execution_quote_coverage_shadow_details["execution_friction_ordering_context"] = dict(
            execution_siphon_side_pressure_weak_state_friction_reorder_context
        )
    execution_friction_pressure = bool(
        execution_friction_available
        and execution_friction_penalty_pressure
        and execution_friction_evidence_ready
    )
    if quote_coverage_pressure and (
        severe_quote_coverage_shortfall
        or concentrated_quote_coverage_shortfall
        or execution_siphon_trend_worsening_material
        or execution_siphon_side_pressure_materially_high
    ):
        _add_action(
            key="improve_execution_quote_coverage_shadow",
            command_hint=improve_execution_quote_coverage_shadow_command_hint,
            details=improve_execution_quote_coverage_shadow_details,
        )
    if execution_friction_pressure:
        _add_action(
            key="reduce_execution_friction_pressure",
            command_hint=reduce_execution_friction_pressure_command_hint,
            details=reduce_execution_friction_pressure_details,
        )
    elif (
        execution_siphon_pressure_hot
        or execution_siphon_trend_worsening_material
        or execution_siphon_side_pressure_materially_high
    ):
        _add_action(
            key="reduce_execution_friction_pressure",
            command_hint=reduce_execution_friction_pressure_command_hint,
            details=reduce_execution_friction_pressure_details,
        )
    if metar_ingest_quality_pipeline_pressure:
        _add_action(
            key="repair_metar_ingest_quality_pipeline",
            command_hint=repair_metar_ingest_quality_pipeline_command_hint,
        )

    if settled_outcomes_insufficient:
        _add_action(
            key="increase_settled_outcome_coverage",
            command_hint=settled_outcome_coverage_command_hint,
        )
    if coverage_velocity_guardrail_active and not coverage_velocity_guardrail_cleared:
        _add_action(
            key="recover_settled_outcome_velocity",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-settled-outcome-throughput "
                f"--output-dir {quoted_output_dir} && "
                "python3 -m betbot.cli kalshi-temperature-profitability "
                f"--output-dir {quoted_output_dir} --hours 720 && "
                "python3 -m betbot.cli decision-matrix-hardening "
                f"--output-dir {quoted_output_dir}"
            ),
        )
    if quote_coverage_pressure:
        _add_action(
            key="improve_execution_quote_coverage_shadow",
            command_hint=improve_execution_quote_coverage_shadow_command_hint,
            details=improve_execution_quote_coverage_shadow_details,
        )
    if weather_min_attempts_gap > 0:
        _add_action(
            key="bootstrap_shadow_trade_intents",
            command_hint=bootstrap_shadow_trade_intents_command_hint,
        )

    repeated_no_effect_blockers = recovery_effectiveness_metrics.get("repeated_no_effect_blockers")
    has_repeated_no_effect_trader_actions = bool(
        isinstance(repeated_no_effect_blockers, list)
        and any(
            bool(_text(_as_dict(row).get("action_key")))
            and max(0, _safe_int(_as_dict(row).get("no_effect_count"))) > 0
            for row in repeated_no_effect_blockers
        )
    )
    if has_repeated_no_effect_trader_actions:
        _add_action(
            key="refresh_market_horizon_inputs",
            command_hint=refresh_market_horizon_inputs_command_hint,
        )

    settlement_finalization_blocked = max(0, _safe_int(policy_reason_counts.get("settlement_finalization_blocked")))
    inside_cutoff_window = max(0, _safe_int(policy_reason_counts.get("inside_cutoff_window")))
    expected_edge_pressure = (
        max(0, _safe_int(policy_reason_counts.get("expected_edge_below_min")))
        + max(0, _safe_int(policy_reason_counts.get("historical_profitability_expected_edge_below_min")))
    )
    stale_policy_blocked = (
        max(0, _safe_int(policy_reason_counts.get("metar_observation_stale")))
        + max(0, _safe_int(policy_reason_counts.get("metar_observation_age_unknown")))
        + max(0, _safe_int(policy_reason_counts.get("metar_freshness_boundary_quality_insufficient")))
    )
    if settlement_finalization_blocked > 0 or inside_cutoff_window > 0:
        _add_action(
            key="refresh_market_horizon_inputs",
            command_hint=refresh_market_horizon_inputs_command_hint,
        )

    weather_risk_off = _as_dict(weather_metrics.get("risk_off"))
    decision_weather_recommended = bool(decision_metrics.get("weather_risk_off_recommended"))
    optimizer_weather = _as_dict(optimizer_metrics.get("weather_risk"))
    weather_blockers = decision_metrics.get("weather_blocker_keys")
    weather_confidence_adjusted_fallback_persistent = bool(
        decision_metrics.get("weather_confidence_adjusted_signal_fallback_persistent")
    )

    if (
        bool(weather_risk_off.get("active"))
        or decision_weather_recommended
        or bool(optimizer_weather.get("risk_off_recommended"))
    ):
        _add_action(
            key="clear_weather_risk_off_state",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-weather-pattern "
                f"--output-dir {quoted_output_dir}"
            ),
        )

    if weather_confidence_adjusted_fallback_persistent:
        _add_action(
            key="repair_weather_confidence_adjusted_signal_pipeline",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-weather-pattern "
                f"--output-dir {quoted_output_dir} --window-hours 1440 && "
                "python3 -m betbot.cli decision-matrix-hardening "
                f"--output-dir {quoted_output_dir} && "
                "python3 -m betbot.cli kalshi-temperature-recovery-advisor "
                f"--output-dir {quoted_output_dir}"
            ),
        )

    if _safe_float(gap_to_clear.get("weather_negative_expectancy_attempt_share")):
        _add_action(
            key="reduce_negative_expectancy_regimes",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-trader "
                f"--output-dir {quoted_output_dir} --weather-pattern-risk-off-enabled"
            ),
        )

    negative_gap = _safe_float(gap_to_clear.get("weather_negative_expectancy_attempt_share"))
    negative_current = _safe_float(weather_metrics.get("negative_expectancy_attempt_share"))
    should_plateau_break = (
        (isinstance(negative_gap, float) and negative_gap >= 0.35)
        or (isinstance(negative_current, float) and negative_current >= 0.85)
    )
    if should_plateau_break:
        _add_action(
            key="plateau_break_negative_expectancy_share",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-recovery-loop "
                f"--output-dir {quoted_output_dir} --max-iterations 6 "
                "--stall-iterations 3 --min-gap-improvement 0.0025 --execute-actions"
            ),
        )

    suppression_enabled = bool(suppression_metrics.get("enabled"))
    suppression_active = bool(suppression_metrics.get("active"))
    suppression_candidate_count = max(0, _safe_int(suppression_metrics.get("candidate_count")))
    suppression_blocked_count = _safe_int(suppression_metrics.get("blocked_count"))
    suppression_blocked_share = _safe_float(suppression_metrics.get("blocked_share"))
    suppression_overblocking = (
        suppression_candidate_count > 0
        and isinstance(suppression_blocked_share, float)
        and suppression_blocked_share >= 0.25
        and (
            (isinstance(negative_gap, float) and negative_gap >= 0.35)
            or (isinstance(negative_current, float) and negative_current >= 0.85)
        )
    )
    suppression_underblocking = suppression_blocked_count <= 0
    if (
        isinstance(negative_gap, float)
        and negative_gap > 0.0
        and suppression_enabled
        and suppression_active
        and suppression_candidate_count > 0
        and (suppression_underblocking or suppression_overblocking)
    ):
        retuned_top_n = 4 if suppression_overblocking else 16
        _add_action(
            key="retune_negative_regime_suppression",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-recovery-loop "
                f"--output-dir {quoted_output_dir} --max-iterations 6 "
                "--stall-iterations 2 --min-gap-improvement 0.0025 --execute-actions "
                "--weather-window-hours 336 "
                "--plateau-negative-regime-suppression-enabled "
                "--plateau-negative-regime-suppression-min-bucket-samples 14 "
                "--plateau-negative-regime-suppression-expectancy-threshold -0.045 "
                f"--plateau-negative-regime-suppression-top-n {retuned_top_n}"
            ),
        )

    stale_negative_gap = _safe_float(gap_to_clear.get("weather_stale_metar_negative_attempt_share"))
    stale_attempt_gap = _safe_float(gap_to_clear.get("weather_stale_metar_attempt_share"))
    if stale_negative_gap or stale_attempt_gap or stale_policy_blocked > 0:
        _add_action(
            key="reduce_stale_metar_pressure",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-metar-ingest "
                f"--output-dir {quoted_output_dir}"
            ),
        )

    if isinstance(weather_blockers, list) and weather_blockers:
        _add_action(
            key="resolve_decision_matrix_weather_blockers",
            command_hint=(
                "python3 -m betbot.cli decision-matrix-hardening "
                f"--output-dir {quoted_output_dir}"
            ),
        )

    if bool(optimizer_weather.get("hard_block_active")):
        _add_action(
            key="clear_optimizer_weather_hard_block",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-recovery-advisor "
                f"--output-dir {quoted_output_dir} --optimizer-top-n 10"
            ),
        )

    if weather_min_attempts_gap > 0:
        _add_action(
            key="increase_weather_sample_coverage",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-weather-pattern "
                f"--output-dir {quoted_output_dir} --window-hours 1440"
            ),
        )

    # Keep expected-edge relief actions near the end so strict plateau/retune
    # runs don't overwrite the most recent probe/relief trader outputs.
    expected_edge_relief_suppressed_for_negative_pressure = should_plateau_break
    if expected_edge_pressure > 0 and not expected_edge_relief_suppressed_for_negative_pressure:
        _add_action(
            key="probe_expected_edge_floor_with_hardening_disabled",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-trader "
                f"--output-dir {quoted_output_dir} --intents-only "
                "--no-weather-pattern-risk-off-enabled "
                "--no-weather-pattern-negative-bucket-suppression-enabled "
                "--disable-weather-pattern-hardening "
                "--disable-historical-selection-quality "
                "--disable-enforce-probability-edge-thresholds"
            ),
        )
        _add_action(
            key="apply_expected_edge_relief_shadow_profile",
            command_hint=(
                "python3 -m betbot.cli kalshi-temperature-trader "
                f"--output-dir {quoted_output_dir} --intents-only "
                "--disable-weather-pattern-hardening "
                "--no-weather-pattern-risk-off-enabled "
                "--no-weather-pattern-negative-bucket-suppression-enabled "
                "--disable-historical-selection-quality "
                "--disable-enforce-probability-edge-thresholds"
            ),
        )

    if not actions:
        _add_action(
            key="refresh_recovery_stack",
            command_hint=(
                "python3 -m betbot.cli decision-matrix-hardening "
                f"--output-dir {quoted_output_dir}"
            ),
        )

    severe_negative_expectancy_pressure = bool(
        should_plateau_break
        and (
            (isinstance(negative_current, float) and negative_current >= 0.95)
            or (isinstance(negative_gap, float) and negative_gap >= 0.45)
        )
    )
    if severe_negative_expectancy_pressure:
        escalation_priority = {
            "improve_execution_quote_coverage_shadow": 0,
            "retune_negative_regime_suppression": 1,
            "plateau_break_negative_expectancy_share": 2,
            "reduce_negative_expectancy_regimes": 3,
        }
        if any(_text(row.get("key")) == "retune_negative_regime_suppression" for row in actions):
            indexed_actions = list(enumerate(actions))
            indexed_actions.sort(
                key=lambda item: (
                    escalation_priority.get(_text(item[1].get("key")), 999),
                    int(item[0]),
                )
            )
            actions = [row for _, row in indexed_actions]

    if execution_siphon_pressure_promote_early:
        friction_priority = (
            {
                "reduce_execution_friction_pressure": 0,
                "improve_execution_quote_coverage_shadow": 1,
            }
            if execution_siphon_side_pressure_weak_state_friction_reorder
            else {
                "improve_execution_quote_coverage_shadow": 0,
                "reduce_execution_friction_pressure": 1,
            }
        )
        if any(_text(row.get("key")) in friction_priority for row in actions):
            indexed_actions = list(enumerate(actions))
            indexed_actions.sort(
                key=lambda item: (
                    friction_priority.get(_text(item[1].get("key")), 999),
                    int(item[0]),
                )
            )
            actions = [row for _, row in indexed_actions]

    protected_action_keys = {
        "restore_stage_timeout_guardrail_script",
        "rerun_stage_timeout_guardrail_hardening",
    }
    harmful_actions = recovery_effectiveness_metrics.get("persistently_harmful_actions")
    harmful_action_keys = {
        _text(action_key)
        for action_key in harmful_actions
        if _text(action_key) and _text(action_key) not in protected_action_keys
    } if isinstance(harmful_actions, list) else set()

    demoted_actions_for_effectiveness: list[str] = []
    if harmful_action_keys:
        protected_rows: list[dict[str, Any]] = []
        normal_rows: list[dict[str, Any]] = []
        harmful_rows: list[dict[str, Any]] = []
        for row in actions:
            action_key = _text(row.get("key"))
            if action_key in protected_action_keys:
                protected_rows.append(row)
                continue
            if action_key in harmful_action_keys:
                harmful_rows.append(row)
                demoted_actions_for_effectiveness.append(action_key)
                continue
            normal_rows.append(row)
        actions = protected_rows + normal_rows + harmful_rows

    if isinstance(demotion_metadata, dict):
        demotion_metadata["demoted_actions_for_effectiveness"] = demoted_actions_for_effectiveness

    return actions


def run_kalshi_temperature_recovery_advisor(
    *,
    output_dir: str,
    weather_window_hours: float = 720.0,
    weather_min_bucket_samples: int = 10,
    weather_max_profile_age_hours: float = 336.0,
    weather_negative_expectancy_attempt_share_target: float = 0.50,
    weather_stale_metar_negative_attempt_share_target: float = 0.60,
    weather_stale_metar_attempt_share_target: float = 0.65,
    weather_min_attempts_target: int = 200,
    optimizer_top_n: int = 5,
) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    safe_weather_window_hours = max(1.0, float(weather_window_hours))
    safe_weather_min_bucket_samples = max(1, int(weather_min_bucket_samples))
    safe_weather_max_profile_age_hours = max(0.0, float(weather_max_profile_age_hours))
    safe_negative_target = max(0.0, min(1.0, float(weather_negative_expectancy_attempt_share_target)))
    safe_stale_negative_target = max(0.0, min(1.0, float(weather_stale_metar_negative_attempt_share_target)))
    safe_stale_target = max(0.0, min(1.0, float(weather_stale_metar_attempt_share_target)))
    safe_weather_min_attempts_target = max(1, int(weather_min_attempts_target))
    safe_optimizer_top_n = max(1, int(optimizer_top_n))

    weather_payload = run_kalshi_temperature_weather_pattern(
        output_dir=str(out_dir),
        window_hours=safe_weather_window_hours,
        min_bucket_samples=safe_weather_min_bucket_samples,
        max_profile_age_hours=safe_weather_max_profile_age_hours,
    )
    decision_payload = run_decision_matrix_hardening(
        output_dir=str(out_dir),
        weather_negative_expectancy_regime_concentration_max=safe_negative_target,
        weather_risk_off_stale_metar_share_min=safe_stale_negative_target,
        weather_risk_off_sample_floor=safe_weather_min_attempts_target,
    )
    growth_payload = run_kalshi_temperature_growth_optimizer(
        input_paths=[str(out_dir)],
        top_n=safe_optimizer_top_n,
    )

    weather_metrics = _normalize_weather_metrics(weather_payload)
    suppression_metrics = _normalize_suppression_metrics(out_dir)
    trade_plan_metrics = _normalize_trade_plan_blocker_metrics(out_dir)
    recovery_watchdog_metrics = _normalize_recovery_watchdog_metrics(out_dir)
    recovery_effectiveness_metrics = _normalize_recovery_effectiveness_metrics(out_dir)
    decision_metrics = _normalize_decision_matrix_metrics(decision_payload, output_dir=out_dir)
    optimizer_metrics = _normalize_growth_optimizer_metrics(growth_payload)

    negative_share = _safe_float(weather_metrics.get("negative_expectancy_attempt_share"))
    stale_negative_share = _safe_float(weather_metrics.get("stale_metar_negative_attempt_share"))
    stale_share = _safe_float(weather_metrics.get("stale_metar_attempt_share"))
    attempts_total = _safe_int(weather_metrics.get("attempts_total"))

    targets = {
        "weather_negative_expectancy_attempt_share": {
            "target_max": round(float(safe_negative_target), 6),
            "current": round(float(negative_share), 6) if isinstance(negative_share, float) else None,
        },
        "weather_stale_metar_negative_attempt_share": {
            "target_max": round(float(safe_stale_negative_target), 6),
            "current": round(float(stale_negative_share), 6) if isinstance(stale_negative_share, float) else None,
        },
        "weather_stale_metar_attempt_share": {
            "target_max": round(float(safe_stale_target), 6),
            "current": round(float(stale_share), 6) if isinstance(stale_share, float) else None,
        },
        "weather_min_attempts": {
            "target_min": int(safe_weather_min_attempts_target),
            "current": int(attempts_total),
        },
        "weather_risk_off_active": {
            "target": False,
            "current": bool(_as_dict(weather_metrics.get("risk_off")).get("active")),
        },
    }

    gap_to_clear = {
        "weather_negative_expectancy_attempt_share": _max_gap(negative_share, safe_negative_target),
        "weather_stale_metar_negative_attempt_share": _max_gap(stale_negative_share, safe_stale_negative_target),
        "weather_stale_metar_attempt_share": _max_gap(stale_share, safe_stale_target),
        "weather_min_attempts": _min_gap(attempts_total, safe_weather_min_attempts_target),
    }

    has_core_weather_metrics = all(
        isinstance(value, float)
        for value in (
            negative_share,
            stale_negative_share,
            stale_share,
        )
    )
    insufficient_data = not has_core_weather_metrics or attempts_total < safe_weather_min_attempts_target

    weather_risk_off = _as_dict(weather_metrics.get("risk_off"))
    optimizer_weather_risk = _as_dict(optimizer_metrics.get("weather_risk"))
    risk_off_signal = any(
        [
            bool(weather_risk_off.get("active")),
            bool(decision_metrics.get("weather_risk_off_recommended")),
            bool(decision_metrics.get("weather_confidence_adjusted_signal_fallback_persistent")),
            bool(optimizer_weather_risk.get("hard_block_active")),
            bool(optimizer_weather_risk.get("risk_off_recommended")),
        ]
    )
    threshold_breach = any(
        (isinstance(value, (int, float)) and float(value) > 0.0)
        for value in gap_to_clear.values()
    )

    # Preserve weather-driven remediation when any upstream layer has already
    # raised an explicit risk-off signal, even if some weather shares are
    # temporarily unavailable.
    if risk_off_signal:
        remediation_status = "risk_off_active"
    elif insufficient_data:
        remediation_status = "insufficient_data"
    elif threshold_breach:
        remediation_status = "risk_off_active"
    else:
        remediation_status = "risk_off_cleared"

    demotion_metadata: dict[str, Any] = {}
    prioritized_actions = _build_prioritized_actions(
        status=remediation_status,
        output_dir=out_dir,
        weather_metrics=weather_metrics,
        suppression_metrics=suppression_metrics,
        trade_plan_metrics=trade_plan_metrics,
        recovery_watchdog_metrics=recovery_watchdog_metrics,
        recovery_effectiveness_metrics=recovery_effectiveness_metrics,
        decision_metrics=decision_metrics,
        optimizer_metrics=optimizer_metrics,
        gap_to_clear=gap_to_clear,
        demotion_metadata=demotion_metadata,
    )

    captured_at = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "output_dir": str(out_dir),
        "health_dir": str(health_dir),
        "inputs": {
            "weather_window_hours": round(float(safe_weather_window_hours), 6),
            "weather_min_bucket_samples": int(safe_weather_min_bucket_samples),
            "weather_max_profile_age_hours": round(float(safe_weather_max_profile_age_hours), 6),
            "weather_negative_expectancy_attempt_share_target": round(float(safe_negative_target), 6),
            "weather_stale_metar_negative_attempt_share_target": round(float(safe_stale_negative_target), 6),
            "weather_stale_metar_attempt_share_target": round(float(safe_stale_target), 6),
            "weather_min_attempts_target": int(safe_weather_min_attempts_target),
            "optimizer_top_n": int(safe_optimizer_top_n),
        },
        "sequence": {
            "weather_pattern": {
                "status": _text(weather_payload.get("status")).lower() or "unknown",
            },
            "decision_matrix": {
                "status": _text(decision_payload.get("status")).lower() or "unknown",
            },
            "growth_optimizer": {
                "status": _text(growth_payload.get("status")).lower() or "unknown",
            },
        },
        "metrics": {
            "weather": weather_metrics,
            "suppression": suppression_metrics,
            "trade_plan_blockers": trade_plan_metrics,
            "recovery_watchdog": recovery_watchdog_metrics,
            "recovery_effectiveness": recovery_effectiveness_metrics,
            "decision_matrix": decision_metrics,
            "growth_optimizer": optimizer_metrics,
        },
        "remediation_plan": {
            "status": remediation_status,
            "prioritized_actions": prioritized_actions,
            "demoted_actions_for_effectiveness": list(
                demotion_metadata.get("demoted_actions_for_effectiveness") or []
            ),
            "targets": targets,
            "gap_to_clear": gap_to_clear,
        },
    }

    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    output_path = health_dir / f"kalshi_temperature_recovery_advisor_{stamp}.json"
    latest_path = health_dir / "kalshi_temperature_recovery_advisor_latest.json"
    payload["output_file"] = str(output_path)
    payload["latest_file"] = str(latest_path)

    encoded = json.dumps(payload, indent=2, sort_keys=True)
    _write_text_atomic(output_path, encoded)
    _write_text_atomic(latest_path, encoded)
    return payload


def summarize_kalshi_temperature_recovery_advisor(
    *,
    output_dir: str,
    weather_window_hours: float = 720.0,
    weather_min_bucket_samples: int = 10,
    weather_max_profile_age_hours: float = 336.0,
    weather_negative_expectancy_attempt_share_target: float = 0.50,
    weather_stale_metar_negative_attempt_share_target: float = 0.60,
    weather_stale_metar_attempt_share_target: float = 0.65,
    weather_min_attempts_target: int = 200,
    optimizer_top_n: int = 5,
) -> str:
    payload = run_kalshi_temperature_recovery_advisor(
        output_dir=output_dir,
        weather_window_hours=weather_window_hours,
        weather_min_bucket_samples=weather_min_bucket_samples,
        weather_max_profile_age_hours=weather_max_profile_age_hours,
        weather_negative_expectancy_attempt_share_target=weather_negative_expectancy_attempt_share_target,
        weather_stale_metar_negative_attempt_share_target=weather_stale_metar_negative_attempt_share_target,
        weather_stale_metar_attempt_share_target=weather_stale_metar_attempt_share_target,
        weather_min_attempts_target=weather_min_attempts_target,
        optimizer_top_n=optimizer_top_n,
    )
    return json.dumps(payload, indent=2, sort_keys=True)
