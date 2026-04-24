from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        return [piece.strip() for piece in text.split(",") if piece.strip()]
    return []


def _coerce_ticker_list(value: Any) -> list[str]:
    return [
        _normalize_text(item).upper()
        for item in _coerce_string_list(value)
        if _normalize_text(item)
    ]


def _load_excluded_market_tickers(
    *,
    excluded_market_tickers: list[str] | None,
    excluded_market_tickers_file: str | None,
) -> tuple[list[str], str, str]:
    values: list[str] = _coerce_ticker_list(excluded_market_tickers or [])
    source_status = "none"
    source_file = _normalize_text(excluded_market_tickers_file)
    if not source_file:
        deduped = sorted(set(values))
        return deduped, source_file, source_status

    path = Path(source_file)
    if not path.exists():
        deduped = sorted(set(values))
        return deduped, source_file, "missing_file"

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError:
        deduped = sorted(set(values))
        return deduped, source_file, "read_error"

    loaded_from_file: list[str] = []
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, dict):
        recommended = parsed.get("recommended_exclusions")
        if isinstance(recommended, dict):
            loaded_from_file.extend(_coerce_ticker_list(recommended.get("market_tickers")))
        loaded_from_file.extend(_coerce_ticker_list(parsed.get("market_tickers")))
        source_status = "loaded_json"
    elif isinstance(parsed, list):
        loaded_from_file.extend(_coerce_ticker_list(parsed))
        source_status = "loaded_json"
    else:
        if raw_text.strip():
            segments = raw_text.replace("\n", ",").replace("\t", ",")
            loaded_from_file.extend(
                _coerce_ticker_list([piece.strip() for piece in segments.split(",") if piece.strip()])
            )
            source_status = "loaded_text"

    values.extend(loaded_from_file)
    deduped = sorted(set(values))
    return deduped, source_file, source_status


def _latest_payload(
    *,
    output_dir: Path,
    patterns: tuple[str, ...],
) -> tuple[dict[str, Any] | None, str]:
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(output_dir.glob(pattern))
    if not candidates:
        return None, ""
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return payload, str(path)
    return None, ""


def _discover_market_tickers_from_artifacts(
    *,
    output_dir: Path,
    max_files_per_pattern: int = 4,
    max_tickers: int = 1500,
) -> tuple[list[str], list[str]]:
    csv_patterns = (
        "kalshi_temperature_constraint_scan_*.csv",
        "kalshi_temperature_contract_specs_*.csv",
        "kalshi_temperature_trade_intents_*.csv",
        "kalshi_temperature_trade_plan_*.csv",
    )
    discovered: list[str] = []
    source_files: list[str] = []
    seen: set[str] = set()
    for pattern in csv_patterns:
        candidates = sorted(
            output_dir.glob(pattern),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for candidate in candidates[: max(1, int(max_files_per_pattern))]:
            try:
                with candidate.open("r", newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        if not isinstance(row, dict):
                            continue
                        ticker = _normalize_text(row.get("market_ticker") or row.get("ticker")).upper()
                        if not ticker or not ticker.startswith("KX") or ticker in seen:
                            continue
                        seen.add(ticker)
                        discovered.append(ticker)
                        if len(discovered) >= max(1, int(max_tickers)):
                            source_files.append(str(candidate))
                            return discovered, source_files
                source_files.append(str(candidate))
            except OSError:
                continue
    return discovered, source_files


def _load_ws_market_books(
    *,
    output_dir: Path,
    ws_summary: dict[str, Any] | None,
) -> tuple[dict[str, dict[str, Any]], str]:
    candidates: list[Path] = []
    if isinstance(ws_summary, dict):
        explicit = _normalize_text(ws_summary.get("ws_state_json"))
        if explicit:
            candidates.append(Path(explicit))
    candidates.append(output_dir / "kalshi_ws_state_latest.json")
    source_file = ""
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        raw_markets = payload.get("markets")
        if not isinstance(raw_markets, dict):
            continue
        source_file = str(path)
        market_books: dict[str, dict[str, Any]] = {}
        for ticker, row in raw_markets.items():
            ticker_text = _normalize_text(ticker).upper()
            if not ticker_text or not isinstance(row, dict):
                continue
            market_books[ticker_text] = dict(row)
        if market_books:
            return market_books, source_file
    return {}, source_file


def _ticker_family_key(ticker: str) -> str:
    text = str(ticker).strip().upper()
    if not text:
        return ""
    parts = [piece for piece in text.split("-") if piece]
    if len(parts) >= 3:
        return "-".join(parts[:-1])
    if len(parts) == 2:
        return parts[0] if any(char.isdigit() for char in parts[1]) else text
    return text


def _strike_sort_key(ticker: str) -> tuple[int, float, str]:
    text = _normalize_text(ticker).upper()
    parts = [piece for piece in text.split("-") if piece]
    tail = parts[-1] if parts else text
    prefix_rank = 2
    if tail.startswith("B"):
        prefix_rank = 0
    elif tail.startswith("T"):
        prefix_rank = 1
    match = re.search(r"-?\d+(?:\.\d+)?", tail)
    numeric = _parse_float(match.group(0)) if match else None
    return (int(prefix_rank), float(numeric if isinstance(numeric, float) else 0.0), text)


def _sorted_family_tickers(tickers: list[str]) -> list[str]:
    return sorted([str(item).strip() for item in tickers if str(item).strip()], key=_strike_sort_key)


def _theme_hint_text(snapshot_summary: dict[str, Any], polymarket_summary: dict[str, Any]) -> str:
    texts: list[str] = []
    family_behavior = (
        snapshot_summary.get("family_behavior")
        if isinstance(snapshot_summary.get("family_behavior"), dict)
        else {}
    )
    top_families = family_behavior.get("families")
    if isinstance(top_families, list):
        for row in top_families[:8]:
            if isinstance(row, dict):
                texts.append(_normalize_text(row.get("family_key")))
                texts.append(_normalize_text(row.get("event_slug")))
    alignment = (
        polymarket_summary.get("coldmath_temperature_alignment")
        if isinstance(polymarket_summary.get("coldmath_temperature_alignment"), dict)
        else {}
    )
    top_positions = alignment.get("top_matched_positions")
    if isinstance(top_positions, list):
        for row in top_positions[:12]:
            if isinstance(row, dict):
                texts.append(_normalize_text(row.get("market_slug")))
                texts.append(_normalize_text(row.get("question")))
                texts.append(_normalize_text(row.get("event_title")))
    return " ".join(texts).lower()


def _infer_theme(snapshot_summary: dict[str, Any], polymarket_summary: dict[str, Any]) -> str:
    text = _theme_hint_text(snapshot_summary, polymarket_summary)
    if not text:
        return "mixed"
    temperature_markers = (
        "temperature",
        "highest temp",
        "high temperature",
        "daily high",
        "lowest temp",
        "daily low",
    )
    rain_markers = (
        "rain",
        "rainfall",
        "precip",
        "precipitation",
    )
    temperature_hits = sum(1 for marker in temperature_markers if marker in text)
    rain_hits = sum(1 for marker in rain_markers if marker in text)
    if temperature_hits > rain_hits and temperature_hits > 0:
        return "temperature"
    if rain_hits > temperature_hits and rain_hits > 0:
        return "rain"
    if temperature_hits > 0:
        return "temperature"
    if rain_hits > 0:
        return "rain"
    return "mixed"


def _theme_match_bonus(theme: str, family_key: str) -> float:
    family = str(family_key).upper()
    if not family:
        return 0.0
    if theme == "temperature":
        return 0.25 if any(token in family for token in ("KXH", "TEMP", "HIGH", "LOW")) else -0.03
    if theme == "rain":
        return 0.25 if "RAIN" in family else -0.03
    return 0.0


def _market_liquidity_profile(
    *,
    side: str,
    market_book: dict[str, Any] | None,
    require_two_sided_quotes: bool,
    max_spread_dollars: float,
    min_liquidity_score: float,
) -> dict[str, Any]:
    top = market_book.get("top_of_book") if isinstance(market_book, dict) else {}
    top = dict(top) if isinstance(top, dict) else {}

    best_yes_bid = _parse_float(top.get("best_yes_bid_dollars"))
    best_yes_ask = _parse_float(top.get("best_yes_ask_dollars"))
    best_no_bid = _parse_float(top.get("best_no_bid_dollars"))
    best_no_ask = _parse_float(top.get("best_no_ask_dollars"))
    yes_spread = _parse_float(top.get("yes_spread_dollars"))

    normalized_side = "no" if str(side).strip().lower() == "no" else "yes"
    if normalized_side == "no":
        best_bid = best_no_bid
        best_ask = best_no_ask
    else:
        best_bid = best_yes_bid
        best_ask = best_yes_ask

    spread = None
    if isinstance(best_bid, float) and isinstance(best_ask, float):
        spread = max(0.0, best_ask - best_bid)
    elif isinstance(yes_spread, float):
        spread = max(0.0, yes_spread)

    safe_max_spread = max(0.001, float(max_spread_dollars))
    safe_min_liquidity_score = max(0.0, min(1.0, float(min_liquidity_score)))
    liquidity_score = 0.0
    if isinstance(best_ask, float):
        liquidity_score += 0.45
    if isinstance(best_bid, float):
        liquidity_score += 0.35
    if isinstance(spread, float):
        liquidity_score += 0.2 * max(0.0, 1.0 - min(1.0, float(spread) / safe_max_spread))
    liquidity_score = max(0.0, min(1.0, liquidity_score))

    reasons: list[str] = []
    if not isinstance(best_ask, float):
        reasons.append("missing_best_ask")
    if require_two_sided_quotes and not isinstance(best_bid, float):
        reasons.append("missing_best_bid")
    if isinstance(spread, float) and spread > safe_max_spread:
        reasons.append("spread_too_wide")
    if liquidity_score < safe_min_liquidity_score:
        reasons.append("liquidity_score_below_min")

    return {
        "side": normalized_side,
        "best_bid_dollars": best_bid,
        "best_ask_dollars": best_ask,
        "spread_dollars": (round(float(spread), 6) if isinstance(spread, float) else None),
        "liquidity_score": round(float(liquidity_score), 6),
        "is_tradable": len(reasons) == 0,
        "reasons": reasons,
    }


def _strategy_role_priority(role: str) -> int:
    normalized = str(role).strip().lower()
    if normalized == "center_bracket_yes":
        return 0
    if normalized == "adjacent_bracket_no":
        return 1
    return 2


def _no_side_metrics(snapshot_summary: dict[str, Any]) -> tuple[bool, float, float]:
    family_behavior = (
        snapshot_summary.get("family_behavior")
        if isinstance(snapshot_summary.get("family_behavior"), dict)
        else {}
    )
    behavior_tags = {
        str(item).strip().lower()
        for item in list(family_behavior.get("behavior_tags") or [])
        if str(item).strip()
    }
    no_outcome_ratio = _parse_float(family_behavior.get("no_outcome_ratio"))
    high_price_no_positions = int(_parse_float(family_behavior.get("positions_with_high_price_no")) or 0)
    no_side_bias = (
        "no_side_bias" in behavior_tags
        or "high_price_no_inventory" in behavior_tags
        or (isinstance(no_outcome_ratio, float) and no_outcome_ratio >= 0.55)
        or high_price_no_positions >= 5
    )
    no_bias_strength = 0.0
    if isinstance(no_outcome_ratio, float):
        no_bias_strength += max(0.0, min(0.5, (no_outcome_ratio - 0.5) * 1.6))
    if high_price_no_positions >= 3:
        no_bias_strength += 0.2
    if high_price_no_positions >= 8:
        no_bias_strength += 0.2
    if high_price_no_positions >= 12:
        no_bias_strength += 0.1
    no_bias_strength = max(0.0, min(1.0, no_bias_strength))
    no_side_max_cost = 0.68
    if isinstance(no_outcome_ratio, float):
        no_side_max_cost = max(no_side_max_cost, 0.55 + (0.45 * max(0.0, min(1.0, no_outcome_ratio))))
    if high_price_no_positions >= 3:
        no_side_max_cost = max(no_side_max_cost, 0.9)
    if high_price_no_positions >= 8:
        no_side_max_cost = max(no_side_max_cost, 0.94)
    if high_price_no_positions >= 12:
        no_side_max_cost = max(no_side_max_cost, 0.97)
    no_side_max_cost = round(max(0.5, min(0.99, no_side_max_cost)), 6)
    return no_side_bias, no_bias_strength, no_side_max_cost


def build_coldmath_replication_candidates(
    *,
    snapshot_summary: dict[str, Any],
    polymarket_summary: dict[str, Any],
    market_tickers: list[str],
    market_books_by_ticker: dict[str, dict[str, Any]] | None = None,
    top_n: int = 12,
    require_liquidity_filter: bool = True,
    require_two_sided_quotes: bool = True,
    max_spread_dollars: float = 0.18,
    min_liquidity_score: float = 0.45,
    max_family_candidates: int = 3,
    max_family_share: float = 0.6,
) -> dict[str, Any]:
    theme = _infer_theme(snapshot_summary, polymarket_summary)
    no_side_bias, no_bias_strength, no_side_max_cost = _no_side_metrics(snapshot_summary)
    preferred_side = "no" if no_side_bias else "yes"
    preferred_max_cost = no_side_max_cost if preferred_side == "no" else 0.72
    normalized_books = {
        _normalize_text(key).upper(): dict(value)
        for key, value in dict(market_books_by_ticker or {}).items()
        if _normalize_text(key) and isinstance(value, dict)
    }
    safe_top_n = max(1, int(top_n))
    safe_family_cap = max(1, int(max_family_candidates))
    safe_family_share = max(0.1, min(1.0, float(max_family_share)))
    share_cap = max(1, int(round(safe_top_n * safe_family_share)))
    effective_family_cap = max(1, min(safe_family_cap, share_cap))

    alignment = (
        polymarket_summary.get("coldmath_temperature_alignment")
        if isinstance(polymarket_summary.get("coldmath_temperature_alignment"), dict)
        else {}
    )
    matched_ratio = _parse_float(alignment.get("matched_ratio")) or 0.0
    matched_ratio = max(0.0, min(1.0, matched_ratio))

    families: dict[str, list[str]] = {}
    for ticker in market_tickers:
        family = _ticker_family_key(ticker)
        families.setdefault(family, []).append(ticker)

    ranked: list[tuple[float, str, str, int]] = []
    for family_key, tickers in families.items():
        family_size = len(tickers)
        score = 0.2 + min(0.5, 0.08 * float(family_size))
        score += _theme_match_bonus(theme, family_key)
        score += 0.15 * matched_ratio
        score += 0.2 * no_bias_strength if preferred_side == "no" else 0.0
        ranked.append((score, family_key, tickers[0], family_size))

    ranked.sort(key=lambda item: (float(item[0]), int(item[3])), reverse=True)
    raw_candidates: list[dict[str, Any]] = []
    filtered_reasons: dict[str, int] = {}
    family_sorted_tickers: dict[str, list[str]] = {
        family_key: _sorted_family_tickers(tickers)
        for family_key, tickers in families.items()
    }
    for family_rank, (_, family_key, _first_ticker, family_size) in enumerate(ranked):
        tickers = family_sorted_tickers.get(family_key) or families.get(family_key, [])
        center_ticker = ""
        adjacent_tickers: set[str] = set()
        if bool(no_side_bias) and len(tickers) >= 3:
            center_index = int(len(tickers) // 2)
            center_ticker = str(tickers[center_index])
            if center_index - 1 >= 0:
                adjacent_tickers.add(str(tickers[center_index - 1]))
            if center_index + 1 < len(tickers):
                adjacent_tickers.add(str(tickers[center_index + 1]))
        for local_index, ticker in enumerate(tickers):
            penalty = 0.02 * float(family_rank) + 0.012 * float(local_index)
            candidate_score = float(max(0.05, min(0.99, ranked[family_rank][0] - penalty)))
            candidate_side = preferred_side
            candidate_max_cost = float(preferred_max_cost)
            strategy_role = "default"
            if center_ticker and ticker == center_ticker:
                candidate_side = "yes"
                strategy_role = "center_bracket_yes"
                candidate_score += 0.18
                candidate_max_cost = max(0.65, min(0.99, float(preferred_max_cost) - 0.02))
            elif center_ticker and ticker in adjacent_tickers:
                candidate_side = "no"
                strategy_role = "adjacent_bracket_no"
                candidate_score += 0.04
            liquidity = _market_liquidity_profile(
                side=candidate_side,
                market_book=normalized_books.get(_normalize_text(ticker).upper()),
                require_two_sided_quotes=bool(require_two_sided_quotes),
                max_spread_dollars=float(max_spread_dollars),
                min_liquidity_score=float(min_liquidity_score),
            )
            if require_liquidity_filter and not bool(liquidity.get("is_tradable")):
                for reason in list(liquidity.get("reasons") or []):
                    reason_text = _normalize_text(reason) or "unknown"
                    filtered_reasons[reason_text] = int(filtered_reasons.get(reason_text, 0) + 1)
                continue
            candidate_score += 0.08 * float(_parse_float(liquidity.get("liquidity_score")) or 0.0)
            raw_candidates.append(
                {
                    "market": ticker,
                    "side": candidate_side,
                    "max_cost": round(float(candidate_max_cost), 6),
                    "score": round(float(max(0.01, min(1.0, candidate_score))), 6),
                    "family_key": family_key,
                    "family_size": int(family_size),
                    "theme": theme,
                    "strategy_role": strategy_role,
                    "liquidity": {
                        "best_bid_dollars": liquidity.get("best_bid_dollars"),
                        "best_ask_dollars": liquidity.get("best_ask_dollars"),
                        "spread_dollars": liquidity.get("spread_dollars"),
                        "liquidity_score": liquidity.get("liquidity_score"),
                        "is_tradable": bool(liquidity.get("is_tradable")),
                        "reasons": list(liquidity.get("reasons") or []),
                    },
                    "rationale": (
                        "ColdMath replication planner ranked this market from live family clustering"
                        f" (theme={theme}, family={family_key}, family_size={family_size}, matched_ratio={matched_ratio:.3f}, role={strategy_role})"
                    ),
                }
            )

    by_family: dict[str, list[dict[str, Any]]] = {}
    family_order: list[str] = []
    for row in sorted(raw_candidates, key=lambda item: float(item.get("score") or 0.0), reverse=True):
        family_key = _normalize_text(row.get("family_key")) or _ticker_family_key(_normalize_text(row.get("market")))
        if family_key not in by_family:
            by_family[family_key] = []
            family_order.append(family_key)
        by_family[family_key].append(row)
    for family_key, rows in by_family.items():
        by_family[family_key] = sorted(
            rows,
            key=lambda item: (
                int(_strategy_role_priority(_normalize_text(item.get("strategy_role")))),
                -float(item.get("score") or 0.0),
            ),
        )

    candidates: list[dict[str, Any]] = []
    family_counts: dict[str, int] = {}
    while len(candidates) < safe_top_n:
        progressed = False
        for family_key in family_order:
            if len(candidates) >= safe_top_n:
                break
            if int(family_counts.get(family_key, 0)) >= int(effective_family_cap):
                continue
            rows = by_family.get(family_key) or []
            if not rows:
                continue
            row = rows.pop(0)
            candidates.append(row)
            family_counts[family_key] = int(family_counts.get(family_key, 0) + 1)
            progressed = True
        if not progressed:
            break

    if len(candidates) < safe_top_n:
        selected_markets = {_normalize_text(row.get("market")) for row in candidates}
        remaining = [
            row
            for row in sorted(raw_candidates, key=lambda item: float(item.get("score") or 0.0), reverse=True)
            if _normalize_text(row.get("market")) and _normalize_text(row.get("market")) not in selected_markets
        ]
        for row in remaining:
            if len(candidates) >= safe_top_n:
                break
            candidates.append(row)

    return {
        "theme": theme,
        "matched_ratio": round(float(matched_ratio), 6),
        "no_side_bias": bool(no_side_bias),
        "no_bias_strength": round(float(no_bias_strength), 6),
        "preferred_side": preferred_side,
        "preferred_max_cost": round(float(preferred_max_cost), 6),
        "liquidity_filter": {
            "enabled": bool(require_liquidity_filter),
            "require_two_sided_quotes": bool(require_two_sided_quotes),
            "max_spread_dollars": round(max(0.001, float(max_spread_dollars)), 6),
            "min_liquidity_score": round(max(0.0, min(1.0, float(min_liquidity_score))), 6),
            "filtered_out": max(0, int(len(market_tickers) - len(raw_candidates))),
            "filtered_reasons": dict(sorted(filtered_reasons.items(), key=lambda item: item[0])),
        },
        "risk_caps": {
            "max_family_candidates": int(safe_family_cap),
            "max_family_share": round(float(safe_family_share), 6),
            "effective_family_cap": int(effective_family_cap),
            "family_counts": dict(sorted({key: int(value) for key, value in family_counts.items()}.items(), key=lambda item: item[0])),
        },
        "candidates": candidates,
    }


def run_coldmath_replication_plan(
    *,
    output_dir: str = "outputs",
    top_n: int = 12,
    market_tickers: list[str] | None = None,
    excluded_market_tickers: list[str] | None = None,
    excluded_market_tickers_file: str | None = None,
    require_liquidity_filter: bool = True,
    require_two_sided_quotes: bool = True,
    max_spread_dollars: float = 0.18,
    min_liquidity_score: float = 0.45,
    max_family_candidates: int = 3,
    max_family_share: float = 0.6,
) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    captured_at = datetime.now(timezone.utc)

    snapshot_summary, snapshot_file = _latest_payload(
        output_dir=out_dir,
        patterns=("coldmath_snapshot_summary_latest.json", "coldmath_snapshot_summary_*.json"),
    )
    polymarket_summary, polymarket_file = _latest_payload(
        output_dir=out_dir,
        patterns=("polymarket_temperature_markets_summary_*.json",),
    )
    ws_summary, ws_summary_file = _latest_payload(
        output_dir=out_dir,
        patterns=("kalshi_ws_state_collect_summary_*.json", "kalshi_ws_state_summary_*.json"),
    )
    ws_market_books, ws_market_books_file = _load_ws_market_books(
        output_dir=out_dir,
        ws_summary=ws_summary if isinstance(ws_summary, dict) else None,
    )

    effective_market_tickers = list(market_tickers or [])
    if not effective_market_tickers and isinstance(ws_summary, dict):
        effective_market_tickers = _coerce_string_list(ws_summary.get("market_tickers"))
    artifact_discovered_tickers, artifact_ticker_source_files = _discover_market_tickers_from_artifacts(
        output_dir=out_dir,
    )
    if artifact_discovered_tickers:
        merged_tickers = list(effective_market_tickers)
        merged_seen = {str(item).strip().upper() for item in merged_tickers if str(item).strip()}
        for ticker in artifact_discovered_tickers:
            normalized = _normalize_text(ticker).upper()
            if not normalized or normalized in merged_seen:
                continue
            merged_seen.add(normalized)
            merged_tickers.append(normalized)
        effective_market_tickers = merged_tickers

    excluded_tickers_resolved, excluded_tickers_source_file, excluded_tickers_source_status = _load_excluded_market_tickers(
        excluded_market_tickers=excluded_market_tickers,
        excluded_market_tickers_file=excluded_market_tickers_file,
    )
    excluded_ticker_set = {str(item).upper() for item in excluded_tickers_resolved if str(item).strip()}
    pre_exclusion_market_ticker_count = len(effective_market_tickers)
    if excluded_ticker_set:
        effective_market_tickers = [
            ticker
            for ticker in effective_market_tickers
            if _normalize_text(ticker).upper() not in excluded_ticker_set
        ]
    excluded_market_ticker_count = max(0, int(pre_exclusion_market_ticker_count - len(effective_market_tickers)))

    status = "ready"
    errors: list[str] = []
    if excluded_tickers_source_status == "missing_file":
        errors.append("excluded_market_tickers_file_missing")
    elif excluded_tickers_source_status == "read_error":
        errors.append("excluded_market_tickers_file_unreadable")
    if not isinstance(snapshot_summary, dict):
        status = "missing_snapshot_summary"
        errors.append("coldmath_snapshot_summary_missing")
    if not isinstance(polymarket_summary, dict):
        status = "missing_polymarket_summary" if status == "ready" else status
        errors.append("polymarket_temperature_summary_missing")
    if not effective_market_tickers:
        if pre_exclusion_market_ticker_count > 0 and excluded_market_ticker_count >= pre_exclusion_market_ticker_count:
            status = "all_market_tickers_excluded" if status == "ready" else status
            errors.append("all_market_tickers_excluded_by_filter")
        else:
            status = "missing_market_tickers" if status == "ready" else status
            errors.append("kalshi_market_tickers_missing")

    plan_data: dict[str, Any] = {
        "theme": "mixed",
        "matched_ratio": 0.0,
        "no_side_bias": False,
        "no_bias_strength": 0.0,
        "preferred_side": "yes",
        "preferred_max_cost": 0.72,
        "candidates": [],
    }
    if (
        status == "ready"
        and isinstance(snapshot_summary, dict)
        and isinstance(polymarket_summary, dict)
    ):
        plan_data = build_coldmath_replication_candidates(
            snapshot_summary=snapshot_summary,
            polymarket_summary=polymarket_summary,
            market_tickers=effective_market_tickers,
            market_books_by_ticker=ws_market_books,
            top_n=top_n,
            require_liquidity_filter=require_liquidity_filter,
            require_two_sided_quotes=require_two_sided_quotes,
            max_spread_dollars=max_spread_dollars,
            min_liquidity_score=min_liquidity_score,
            max_family_candidates=max_family_candidates,
            max_family_share=max_family_share,
        )
        if not list(plan_data.get("candidates") or []):
            status = "no_candidates_after_filters"
            errors.append("replication_candidates_filtered_or_empty")

    payload: dict[str, Any] = {
        "status": status,
        "captured_at": captured_at.isoformat(),
        "output_dir": str(out_dir),
        "source_files": {
            "coldmath_snapshot_summary": snapshot_file,
            "polymarket_temperature_summary": polymarket_file,
            "kalshi_ws_state_summary": ws_summary_file,
            "kalshi_ws_state_books": ws_market_books_file,
            "excluded_market_tickers_file": excluded_tickers_source_file,
            "market_ticker_artifacts": artifact_ticker_source_files,
        },
        "artifact_discovered_market_ticker_count": len(artifact_discovered_tickers),
        "excluded_market_tickers_source_status": excluded_tickers_source_status,
        "excluded_market_tickers_input_count": len(excluded_tickers_resolved),
        "excluded_market_tickers_input": excluded_tickers_resolved,
        "pre_exclusion_market_ticker_count": int(pre_exclusion_market_ticker_count),
        "excluded_market_ticker_count": int(excluded_market_ticker_count),
        "market_ticker_count": len(effective_market_tickers),
        "market_books_count": len(ws_market_books),
        "errors": errors,
        "theme": plan_data.get("theme"),
        "matched_ratio": plan_data.get("matched_ratio"),
        "no_side_bias": plan_data.get("no_side_bias"),
        "no_bias_strength": plan_data.get("no_bias_strength"),
        "preferred_side": plan_data.get("preferred_side"),
        "preferred_max_cost": plan_data.get("preferred_max_cost"),
        "liquidity_filter": plan_data.get("liquidity_filter"),
        "risk_caps": plan_data.get("risk_caps"),
        "candidate_count": len(plan_data.get("candidates") or []),
        "candidates": list(plan_data.get("candidates") or []),
    }

    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"coldmath_replication_plan_{stamp}.json"
    latest_path = out_dir / "coldmath_replication_plan_latest.json"
    text = json.dumps(payload, indent=2, sort_keys=True)
    output_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    payload["output_file"] = str(output_path)
    payload["latest_file"] = str(latest_path)
    return payload
