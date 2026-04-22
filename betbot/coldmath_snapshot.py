from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


JsonGetter = Callable[[str, float], tuple[int, Any]]

POLYMARKET_DATA_API_BASE_URL = "https://data-api.polymarket.com"
POLYMARKET_BROWSER_HEADERS = {
    "Accept": "application/json,text/plain,*/*",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

EQUITY_FIELDNAMES = [
    "cashBalance",
    "positionsValue",
    "equity",
    "valuationTime",
    "wallet",
    "positionsCount",
    "dataApiBaseUrl",
    "source",
]

POSITIONS_FIELDNAMES = [
    "conditionId",
    "asset",
    "size",
    "curPrice",
    "valuationTime",
    "eventSlug",
    "slug",
    "title",
    "outcome",
    "endDate",
    "currentValue",
    "avgPrice",
    "initialValue",
    "totalBought",
    "cashPnl",
    "realizedPnl",
    "percentPnl",
    "percentRealizedPnl",
    "redeemable",
    "mergeable",
    "negativeRisk",
]

TRADES_FIELDNAMES = [
    "capturedAt",
    "queryScope",
    "tradeId",
    "timestamp",
    "marketSlug",
    "eventSlug",
    "title",
    "outcome",
    "side",
    "size",
    "price",
    "usdcSize",
    "transactionHash",
    "conditionId",
    "asset",
]

ACTIVITY_FIELDNAMES = [
    "capturedAt",
    "activityId",
    "timestamp",
    "type",
    "marketSlug",
    "eventSlug",
    "title",
    "outcome",
    "side",
    "size",
    "price",
    "usdcSize",
    "transactionHash",
    "conditionId",
    "asset",
]

LEDGER_EVENTS_FIELDNAMES = [
    "capturedAt",
    "eventKey",
    "eventTimestamp",
    "eventType",
    "eventClass",
    "source",
    "sourceRowId",
    "sourceQueryScope",
    "marketSlug",
    "eventSlug",
    "title",
    "outcome",
    "side",
    "size",
    "price",
    "usdcSize",
    "transactionHash",
    "conditionId",
    "asset",
    "accountingDirection",
    "dedupeStatus",
    "isTradeLike",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_text(value: Any) -> str:
    parsed = _parse_float(value)
    if parsed is None:
        return ""
    text = f"{parsed:.12f}".rstrip("0").rstrip(".")
    return text if text else "0"


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        try:
            epoch = float(value)
            if epoch > 10_000_000_000:
                epoch /= 1000.0
            return datetime.fromtimestamp(epoch, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            epoch = float(text)
            if epoch > 10_000_000_000:
                epoch /= 1000.0
            return datetime.fromtimestamp(epoch, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _decode_http_payload(raw: bytes) -> Any:
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw_text_excerpt": text[:500]}


def _http_get_json(url: str, timeout_seconds: float) -> tuple[int, Any]:
    request = Request(url=url, headers=dict(POLYMARKET_BROWSER_HEADERS), method="GET")
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            status = int(getattr(response, "status", 200) or 200)
            return status, _decode_http_payload(response.read())
    except HTTPError as exc:
        return int(exc.code), _decode_http_payload(exc.read())
    except Exception as exc:
        return 0, {"error": str(exc)}


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = _normalize_text(value).lower()
    return text in {"1", "true", "yes", "y", "on"}


def _query_bool(value: bool) -> str:
    return "true" if bool(value) else "false"


def _extract_market_metadata(raw_row: dict[str, Any]) -> dict[str, str]:
    market_slug = _normalize_text(raw_row.get("slug") or raw_row.get("marketSlug"))
    event_slug = _normalize_text(raw_row.get("eventSlug"))
    title = _normalize_text(raw_row.get("title"))
    outcome = _normalize_text(raw_row.get("outcome"))
    condition_id = _normalize_text(raw_row.get("conditionId"))
    asset = _normalize_text(raw_row.get("asset") or raw_row.get("assetId"))

    market_payload = raw_row.get("market")
    if isinstance(market_payload, dict):
        if not market_slug:
            market_slug = _normalize_text(
                market_payload.get("slug") or market_payload.get("marketSlug")
            )
        if not event_slug:
            event_slug = _normalize_text(market_payload.get("eventSlug"))
        if not title:
            title = _normalize_text(market_payload.get("title"))
        if not outcome:
            outcome = _normalize_text(market_payload.get("outcome"))
        if not condition_id:
            condition_id = _normalize_text(market_payload.get("conditionId"))
        if not asset:
            asset = _normalize_text(
                market_payload.get("asset") or market_payload.get("assetId")
            )

    return {
        "market_slug": market_slug,
        "event_slug": event_slug,
        "title": title,
        "outcome": outcome,
        "condition_id": condition_id,
        "asset": asset,
    }


def _normalize_trade_row(
    *,
    raw_row: dict[str, Any],
    captured_at: datetime,
    query_scope: str,
) -> dict[str, str]:
    market = _extract_market_metadata(raw_row)
    return {
        "capturedAt": captured_at.isoformat(),
        "queryScope": _normalize_text(query_scope),
        "tradeId": _normalize_text(raw_row.get("id") or raw_row.get("tradeId")),
        "timestamp": _normalize_text(raw_row.get("timestamp") or raw_row.get("createdAt")),
        "marketSlug": market["market_slug"],
        "eventSlug": market["event_slug"],
        "title": market["title"],
        "outcome": market["outcome"],
        "side": _normalize_text(raw_row.get("side")),
        "size": _float_text(raw_row.get("size")),
        "price": _float_text(raw_row.get("price")),
        "usdcSize": _float_text(raw_row.get("usdcSize")),
        "transactionHash": _normalize_text(raw_row.get("transactionHash")),
        "conditionId": market["condition_id"],
        "asset": market["asset"],
    }


def _normalize_activity_row(
    *,
    raw_row: dict[str, Any],
    captured_at: datetime,
) -> dict[str, str]:
    market = _extract_market_metadata(raw_row)
    return {
        "capturedAt": captured_at.isoformat(),
        "activityId": _normalize_text(raw_row.get("id") or raw_row.get("activityId")),
        "timestamp": _normalize_text(raw_row.get("timestamp") or raw_row.get("createdAt")),
        "type": _normalize_text(raw_row.get("type")),
        "marketSlug": market["market_slug"],
        "eventSlug": market["event_slug"],
        "title": market["title"],
        "outcome": market["outcome"],
        "side": _normalize_text(raw_row.get("side")),
        "size": _float_text(raw_row.get("size")),
        "price": _float_text(raw_row.get("price")),
        "usdcSize": _float_text(raw_row.get("usdcSize")),
        "transactionHash": _normalize_text(raw_row.get("transactionHash")),
        "conditionId": market["condition_id"],
        "asset": market["asset"],
    }


def _activity_accounting_direction(activity_type: str, side: str) -> str:
    normalized_type = _normalize_text(activity_type).upper()
    normalized_side = _normalize_text(side).lower()
    if normalized_type == "TRADE":
        if normalized_side == "buy":
            return "debit"
        if normalized_side == "sell":
            return "credit"
        return "mixed"
    if normalized_type in {"REDEEM", "REWARD", "MAKER_REBATE", "REFERRAL_REWARD"}:
        return "credit"
    if normalized_type in {"SPLIT", "MERGE", "CONVERSION"}:
        return "structural"
    return "unknown"


def _canonical_event_key(row: dict[str, str], *, event_type: str) -> str:
    tx_hash = _normalize_text(row.get("transactionHash")).lower()
    market_slug = _normalize_text(row.get("marketSlug")).lower()
    timestamp = _normalize_text(row.get("timestamp"))
    side = _normalize_text(row.get("side")).lower()
    outcome = _normalize_text(row.get("outcome")).lower()
    size = _float_text(row.get("size"))
    price = _float_text(row.get("price"))
    usdc_size = _float_text(row.get("usdcSize"))
    condition_id = _normalize_text(row.get("conditionId")).lower()
    asset = _normalize_text(row.get("asset")).lower()
    source_id = _normalize_text(row.get("tradeId") or row.get("activityId")).lower()
    normalized_event_type = _normalize_text(event_type).upper() or "UNKNOWN"
    parts = [
        normalized_event_type,
        tx_hash,
        timestamp,
        market_slug,
        condition_id,
        asset,
        outcome,
        side,
        size,
        price,
        usdc_size,
    ]
    if all(not part for part in parts[1:]):
        parts.append(source_id)
    return "|".join(parts)


def _event_sort_key(row: dict[str, str]) -> tuple[float, int]:
    timestamp = _parse_ts(row.get("eventTimestamp"))
    if isinstance(timestamp, datetime):
        return (timestamp.timestamp(), 0)
    return (0.0, 1)


def build_public_observed_ledger_events(
    *,
    trades_rows: list[dict[str, str]],
    activity_rows: list[dict[str, str]],
) -> dict[str, Any]:
    candidates: list[dict[str, str]] = []
    for row in trades_rows:
        event_type = "TRADE"
        candidates.append(
            {
                "capturedAt": _normalize_text(row.get("capturedAt")),
                "eventTimestamp": _normalize_text(row.get("timestamp")),
                "eventType": event_type,
                "eventClass": "trade",
                "source": "trades",
                "sourceRowId": _normalize_text(row.get("tradeId")),
                "sourceQueryScope": _normalize_text(row.get("queryScope")),
                "marketSlug": _normalize_text(row.get("marketSlug")),
                "eventSlug": _normalize_text(row.get("eventSlug")),
                "title": _normalize_text(row.get("title")),
                "outcome": _normalize_text(row.get("outcome")),
                "side": _normalize_text(row.get("side")),
                "size": _float_text(row.get("size")),
                "price": _float_text(row.get("price")),
                "usdcSize": _float_text(row.get("usdcSize")),
                "transactionHash": _normalize_text(row.get("transactionHash")),
                "conditionId": _normalize_text(row.get("conditionId")),
                "asset": _normalize_text(row.get("asset")),
                "accountingDirection": _activity_accounting_direction(
                    event_type,
                    _normalize_text(row.get("side")),
                ),
                "isTradeLike": "1",
            }
        )

    for row in activity_rows:
        event_type = _normalize_text(row.get("type")).upper() or "UNKNOWN"
        is_trade_like = event_type == "TRADE"
        candidates.append(
            {
                "capturedAt": _normalize_text(row.get("capturedAt")),
                "eventTimestamp": _normalize_text(row.get("timestamp")),
                "eventType": event_type,
                "eventClass": ("trade" if is_trade_like else "activity"),
                "source": "activity",
                "sourceRowId": _normalize_text(row.get("activityId")),
                "sourceQueryScope": "",
                "marketSlug": _normalize_text(row.get("marketSlug")),
                "eventSlug": _normalize_text(row.get("eventSlug")),
                "title": _normalize_text(row.get("title")),
                "outcome": _normalize_text(row.get("outcome")),
                "side": _normalize_text(row.get("side")),
                "size": _float_text(row.get("size")),
                "price": _float_text(row.get("price")),
                "usdcSize": _float_text(row.get("usdcSize")),
                "transactionHash": _normalize_text(row.get("transactionHash")),
                "conditionId": _normalize_text(row.get("conditionId")),
                "asset": _normalize_text(row.get("asset")),
                "accountingDirection": _activity_accounting_direction(
                    event_type,
                    _normalize_text(row.get("side")),
                ),
                "isTradeLike": ("1" if is_trade_like else "0"),
            }
        )

    # Prefer explicit trade rows over mirrored TRADE activity rows when keys collide.
    candidates.sort(
        key=lambda row: (
            0 if _normalize_text(row.get("source")) == "trades" else 1,
            _event_sort_key(row),
        )
    )

    canonical_rows: list[dict[str, str]] = []
    duplicate_count = 0
    seen_keys: set[str] = set()
    duplicates_by_source: dict[str, int] = {}
    raw_source_counts: dict[str, int] = {}
    canonical_source_counts: dict[str, int] = {}
    raw_type_counts: dict[str, int] = {}
    canonical_type_counts: dict[str, int] = {}

    for candidate in candidates:
        source = _normalize_text(candidate.get("source")).lower() or "unknown"
        event_type = _normalize_text(candidate.get("eventType")).upper() or "UNKNOWN"
        raw_source_counts[source] = raw_source_counts.get(source, 0) + 1
        raw_type_counts[event_type] = raw_type_counts.get(event_type, 0) + 1

        event_key = _canonical_event_key(candidate, event_type=event_type)
        if event_key in seen_keys:
            duplicate_count += 1
            duplicates_by_source[source] = duplicates_by_source.get(source, 0) + 1
            continue
        seen_keys.add(event_key)

        canonical_source_counts[source] = canonical_source_counts.get(source, 0) + 1
        canonical_type_counts[event_type] = canonical_type_counts.get(event_type, 0) + 1

        canonical = dict(candidate)
        canonical["eventKey"] = event_key
        canonical["dedupeStatus"] = "canonical"
        canonical_rows.append(canonical)

    canonical_rows.sort(key=_event_sort_key)

    return {
        "rows": canonical_rows,
        "raw_rows_total": len(candidates),
        "canonical_rows_total": len(canonical_rows),
        "duplicates_dropped": duplicate_count,
        "raw_source_counts": raw_source_counts,
        "canonical_source_counts": canonical_source_counts,
        "raw_type_counts": raw_type_counts,
        "canonical_type_counts": canonical_type_counts,
        "duplicates_by_source": duplicates_by_source,
    }


def _resolve_public_profile_wallet(
    *,
    wallet_address: str,
    data_api_base_url: str,
    timeout_seconds: float,
    http_get_json: JsonGetter,
) -> dict[str, Any]:
    requested = _normalize_text(wallet_address).lower()
    if not requested:
        return {
            "status": "missing_wallet",
            "requested_wallet": requested,
            "normalized_wallet": "",
            "source": "",
        }

    base = _normalize_text(data_api_base_url).rstrip("/")
    attempts = [
        ("user", f"{base}/profile?{urlencode({'user': requested})}"),
        ("address", f"{base}/profile?{urlencode({'address': requested})}"),
    ]
    for query_kind, url in attempts:
        status_code, payload = http_get_json(url, timeout_seconds)
        if status_code != 200:
            continue
        candidate: dict[str, Any] | None = None
        if isinstance(payload, dict):
            candidate = payload
        elif isinstance(payload, list) and payload and isinstance(payload[0], dict):
            candidate = payload[0]
        if not isinstance(candidate, dict):
            continue
        proxy_wallet = _normalize_text(
            candidate.get("proxyWallet")
            or candidate.get("proxy_wallet")
            or candidate.get("proxyAddress")
            or candidate.get("wallet")
            or candidate.get("address")
        ).lower()
        if proxy_wallet:
            return {
                "status": "resolved",
                "requested_wallet": requested,
                "normalized_wallet": proxy_wallet,
                "source": f"profile_{query_kind}",
                "http_status": status_code,
            }

    return {
        "status": "unresolved",
        "requested_wallet": requested,
        "normalized_wallet": requested,
        "source": "fallback_input",
    }


def _fetch_data_api_rows(
    *,
    endpoint: str,
    wallet_address: str,
    base_url: str,
    timeout_seconds: float,
    page_size: int,
    max_pages: int,
    http_get_json: JsonGetter,
    extra_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    page_limit = max(1, int(page_size))
    max_page_count = max(1, int(max_pages))
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    http_status = 200
    pages_fetched = 0

    for page_index in range(max_page_count):
        params: list[tuple[str, str]] = [
            ("user", wallet_address),
            ("limit", str(page_limit)),
            ("offset", str(page_index * page_limit)),
        ]
        if isinstance(extra_params, dict):
            for key, raw_value in extra_params.items():
                value = raw_value
                if isinstance(raw_value, bool):
                    value = _query_bool(raw_value)
                params.append((str(key), _normalize_text(value)))

        url = f"{base_url}/{endpoint}?{urlencode(params)}"
        status_code, payload = http_get_json(url, timeout_seconds)
        pages_fetched += 1
        if status_code != 200:
            http_status = status_code
            errors.append(f"{endpoint}_http_{status_code}")
            break
        if not isinstance(payload, list):
            errors.append(f"{endpoint}_payload_invalid")
            break
        if not payload:
            break

        rows.extend(item for item in payload if isinstance(item, dict))
        if len(payload) < page_limit:
            break

    status = "ready"
    if errors and rows:
        status = "partial"
    elif errors:
        status = "error"
    elif not rows:
        status = "empty"

    last_offset = ((pages_fetched - 1) * page_limit) if pages_fetched > 0 else 0
    next_offset = pages_fetched * page_limit
    return {
        "status": status,
        "rows": rows,
        "errors": errors,
        "http_status": http_status,
        "page_size": page_limit,
        "max_pages": max_page_count,
        "pages_fetched": pages_fetched,
        "last_offset": last_offset,
        "next_offset": next_offset,
    }


def _combine_fetch_status(statuses: list[str]) -> str:
    cleaned = [str(status or "").strip() for status in statuses if str(status or "").strip()]
    if not cleaned:
        return "skipped"
    if all(status == "skipped" for status in cleaned):
        return "skipped"
    actionable = [status for status in cleaned if status != "skipped"]
    if not actionable:
        return "skipped"
    if any(status == "error" for status in actionable):
        if any(status in {"ready", "partial", "empty"} for status in actionable):
            return "partial"
        return "error"
    if any(status == "partial" for status in actionable):
        return "partial"
    if any(status == "ready" for status in actionable):
        return "ready"
    return "empty"


def _extract_strike_slug(row: dict[str, str]) -> str:
    event_slug = _normalize_text(row.get("eventSlug")).lower()
    slug = _normalize_text(row.get("slug")).lower()
    if event_slug and slug.startswith(f"{event_slug}-"):
        return slug[len(event_slug) + 1 :]
    if slug:
        match = re.search(
            r"(\d+(?:\.\d+)?(?:-\d+(?:\.\d+)?)?[fc](?:-or-higher|-or-lower)?)$",
            slug,
        )
        if match:
            return _normalize_text(match.group(1))
    return ""


def _infer_family_key(row: dict[str, str]) -> str:
    event_slug = _normalize_text(row.get("eventSlug")).lower()
    if event_slug:
        return event_slug
    slug = _normalize_text(row.get("slug")).lower()
    if slug:
        strike = _extract_strike_slug(row)
        if strike and slug.endswith(f"-{strike}"):
            return slug[: -(len(strike) + 1)]
        return slug
    condition_id = _normalize_text(row.get("conditionId")).lower()
    if condition_id:
        return f"condition::{condition_id[:16]}"
    return "unknown_family"


def summarize_family_behavior(
    *,
    positions_rows: list[dict[str, str]],
) -> dict[str, Any]:
    if not positions_rows:
        return {
            "family_count": 0,
            "multi_strike_family_count": 0,
            "positions_with_yes_outcome": 0,
            "positions_with_no_outcome": 0,
            "positions_with_high_price_no": 0,
            "no_outcome_ratio": 0.0,
            "behavior_tags": [],
            "families": [],
        }

    by_family: dict[str, list[dict[str, str]]] = {}
    for row in positions_rows:
        key = _infer_family_key(row)
        by_family.setdefault(key, []).append(row)

    families: list[dict[str, Any]] = []
    yes_count = 0
    no_count = 0
    high_price_no_count = 0

    for family_key, rows in by_family.items():
        strike_labels: set[str] = set()
        condition_ids: set[str] = set()
        gross_current_value = 0.0
        gross_estimated_notional = 0.0
        prices: list[float] = []
        family_yes_count = 0
        family_no_count = 0
        family_high_price_no_count = 0

        for row in rows:
            condition_id = _normalize_text(row.get("conditionId"))
            if condition_id:
                condition_ids.add(condition_id)

            size = abs(_parse_float(row.get("size")) or 0.0)
            cur_price = _parse_float(row.get("curPrice"))
            current_value = _parse_float(row.get("currentValue"))
            estimated_notional = (size * cur_price) if isinstance(cur_price, float) else 0.0
            if isinstance(cur_price, float):
                prices.append(cur_price)
            if isinstance(current_value, float):
                gross_current_value += abs(current_value)
            else:
                gross_current_value += abs(estimated_notional)
            gross_estimated_notional += abs(estimated_notional)

            outcome = _normalize_text(row.get("outcome")).lower()
            if outcome == "yes":
                family_yes_count += 1
                yes_count += 1
            elif outcome == "no":
                family_no_count += 1
                no_count += 1
                if isinstance(cur_price, float) and cur_price >= 0.9:
                    family_high_price_no_count += 1
                    high_price_no_count += 1

            strike_slug = _extract_strike_slug(row)
            if strike_slug:
                strike_labels.add(strike_slug)

        families.append(
            {
                "family_key": family_key,
                "event_slug": _normalize_text(rows[0].get("eventSlug")),
                "positions_count": len(rows),
                "unique_condition_count": len(condition_ids),
                "unique_strike_count": len(strike_labels),
                "strike_labels_preview": sorted(strike_labels)[:12],
                "outcome_yes_count": family_yes_count,
                "outcome_no_count": family_no_count,
                "high_price_no_count": family_high_price_no_count,
                "gross_current_value": round(gross_current_value, 6),
                "gross_estimated_notional": round(gross_estimated_notional, 6),
                "avg_cur_price": (
                    round(sum(prices) / float(len(prices)), 6)
                    if prices
                    else None
                ),
                "min_cur_price": (round(min(prices), 6) if prices else None),
                "max_cur_price": (round(max(prices), 6) if prices else None),
            }
        )

    families.sort(
        key=lambda item: (
            float(item.get("gross_current_value") or 0.0),
            int(item.get("positions_count") or 0),
        ),
        reverse=True,
    )

    decisive_count = yes_count + no_count
    no_ratio = (no_count / float(decisive_count)) if decisive_count > 0 else 0.0
    multi_strike_family_count = sum(
        1 for family in families if int(family.get("unique_strike_count") or 0) >= 2
    )

    behavior_tags: list[str] = []
    if no_ratio >= 0.6:
        behavior_tags.append("no_side_bias")
    if multi_strike_family_count > 0:
        behavior_tags.append("multi_strike_clustering")
    if high_price_no_count >= 3:
        behavior_tags.append("high_price_no_inventory")

    return {
        "family_count": len(families),
        "multi_strike_family_count": multi_strike_family_count,
        "positions_with_yes_outcome": yes_count,
        "positions_with_no_outcome": no_count,
        "positions_with_high_price_no": high_price_no_count,
        "no_outcome_ratio": round(no_ratio, 6),
        "behavior_tags": behavior_tags,
        "families": families[:25],
    }


def fetch_polymarket_wallet_snapshot(
    *,
    wallet_address: str,
    data_api_base_url: str = POLYMARKET_DATA_API_BASE_URL,
    timeout_seconds: float = 20.0,
    positions_page_size: int = 500,
    positions_max_pages: int = 20,
    refresh_trades: bool = True,
    refresh_activity: bool = True,
    include_taker_only_trades: bool = True,
    include_all_trade_roles: bool = True,
    trades_page_size: int = 500,
    trades_max_pages: int = 20,
    activity_page_size: int = 500,
    activity_max_pages: int = 20,
    now: datetime | None = None,
    http_get_json: JsonGetter = _http_get_json,
) -> dict[str, Any]:
    requested_wallet = _normalize_text(wallet_address).lower()
    if not requested_wallet:
        raise ValueError("wallet_address is required")

    captured_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    valuation_time = captured_at.isoformat()
    base_url = _normalize_text(data_api_base_url).rstrip("/")
    profile_resolution = _resolve_public_profile_wallet(
        wallet_address=requested_wallet,
        data_api_base_url=base_url,
        timeout_seconds=timeout_seconds,
        http_get_json=http_get_json,
    )
    wallet = _normalize_text(profile_resolution.get("normalized_wallet")).lower() or requested_wallet

    errors: list[str] = []
    value_url = f"{base_url}/value?{urlencode({'user': wallet})}"
    value_status, value_payload = http_get_json(value_url, timeout_seconds)

    wallet_value: float | None = None
    if value_status == 200:
        if isinstance(value_payload, list) and value_payload:
            first = value_payload[0]
            if isinstance(first, dict):
                wallet_value = _parse_float(first.get("value"))
        elif isinstance(value_payload, dict):
            wallet_value = _parse_float(value_payload.get("value"))
    else:
        errors.append(f"value_http_{value_status}")

    page_size = max(1, int(positions_page_size))
    max_pages = max(1, int(positions_max_pages))
    positions_rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str]] = set()

    positions_status = 200
    pages_fetched = 0
    for page_index in range(max_pages):
        offset = page_index * page_size
        positions_url = f"{base_url}/positions?{urlencode({'user': wallet, 'limit': page_size, 'offset': offset})}"
        status_code, payload = http_get_json(positions_url, timeout_seconds)
        pages_fetched += 1
        if status_code != 200:
            positions_status = status_code
            errors.append(f"positions_http_{status_code}")
            break
        if not isinstance(payload, list):
            errors.append("positions_payload_invalid")
            break
        if not payload:
            break

        for raw_row in payload:
            if not isinstance(raw_row, dict):
                continue
            condition_id = _normalize_text(raw_row.get("conditionId"))
            asset = _normalize_text(raw_row.get("asset"))
            outcome = _normalize_text(raw_row.get("outcome"))
            dedupe_key = (condition_id.lower(), asset, outcome.lower())
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            positions_rows.append(
                {
                    "conditionId": condition_id,
                    "asset": asset,
                    "size": _float_text(raw_row.get("size")),
                    "curPrice": _float_text(raw_row.get("curPrice")),
                    "valuationTime": valuation_time,
                    "eventSlug": _normalize_text(raw_row.get("eventSlug")),
                    "slug": _normalize_text(raw_row.get("slug")),
                    "title": _normalize_text(raw_row.get("title")),
                    "outcome": outcome,
                    "endDate": _normalize_text(raw_row.get("endDate")),
                    "currentValue": _float_text(raw_row.get("currentValue")),
                    "avgPrice": _float_text(raw_row.get("avgPrice")),
                    "initialValue": _float_text(raw_row.get("initialValue")),
                    "totalBought": _float_text(raw_row.get("totalBought")),
                    "cashPnl": _float_text(raw_row.get("cashPnl")),
                    "realizedPnl": _float_text(raw_row.get("realizedPnl")),
                    "percentPnl": _float_text(raw_row.get("percentPnl")),
                    "percentRealizedPnl": _float_text(raw_row.get("percentRealizedPnl")),
                    "redeemable": "1" if _coerce_bool(raw_row.get("redeemable")) else "0",
                    "mergeable": "1" if _coerce_bool(raw_row.get("mergeable")) else "0",
                    "negativeRisk": "1" if _coerce_bool(raw_row.get("negativeRisk")) else "0",
                }
            )

        if len(payload) < page_size:
            break

    positions_value = sum(_parse_float(row.get("currentValue")) or 0.0 for row in positions_rows)
    cash_balance: float | None = None
    if isinstance(wallet_value, float):
        cash_balance = wallet_value - positions_value

    equity_row = {
        "cashBalance": _float_text(cash_balance),
        "positionsValue": _float_text(positions_value),
        "equity": _float_text(wallet_value),
        "valuationTime": valuation_time,
        "wallet": wallet,
        "requestedWallet": requested_wallet,
        "positionsCount": str(len(positions_rows)),
        "dataApiBaseUrl": base_url,
        "source": "polymarket_data_api",
    }

    if errors and not positions_rows and wallet_value is None:
        status = "error"
    elif errors:
        status = "partial"
    else:
        status = "ready"

    trades_rows: list[dict[str, str]] = []
    activity_rows: list[dict[str, str]] = []
    taker_only_trade_fetch: dict[str, Any] = {"status": "skipped", "rows": 0}
    all_roles_trade_fetch: dict[str, Any] = {"status": "skipped", "rows": 0}
    trade_fetch_statuses: list[str] = ["skipped"]
    if refresh_trades:
        trade_fetch_statuses = []
        if include_taker_only_trades:
            taker_only_payload = _fetch_data_api_rows(
                endpoint="trades",
                wallet_address=wallet,
                base_url=base_url,
                timeout_seconds=timeout_seconds,
                page_size=trades_page_size,
                max_pages=trades_max_pages,
                http_get_json=http_get_json,
                extra_params={"takerOnly": True},
            )
            taker_only_trade_fetch = {
                "status": taker_only_payload.get("status"),
                "rows": len(taker_only_payload.get("rows") or []),
                "http_status": taker_only_payload.get("http_status"),
                "pages_fetched": taker_only_payload.get("pages_fetched"),
                "last_offset": taker_only_payload.get("last_offset"),
                "next_offset": taker_only_payload.get("next_offset"),
                "errors": list(taker_only_payload.get("errors") or []),
            }
            trade_fetch_statuses.append(str(taker_only_payload.get("status") or ""))
            for raw_row in taker_only_payload.get("rows") or []:
                if isinstance(raw_row, dict):
                    trades_rows.append(
                        _normalize_trade_row(
                            raw_row=raw_row,
                            captured_at=captured_at,
                            query_scope="taker_only",
                        )
                    )

        if include_all_trade_roles:
            all_roles_payload = _fetch_data_api_rows(
                endpoint="trades",
                wallet_address=wallet,
                base_url=base_url,
                timeout_seconds=timeout_seconds,
                page_size=trades_page_size,
                max_pages=trades_max_pages,
                http_get_json=http_get_json,
                extra_params={"takerOnly": False},
            )
            all_roles_trade_fetch = {
                "status": all_roles_payload.get("status"),
                "rows": len(all_roles_payload.get("rows") or []),
                "http_status": all_roles_payload.get("http_status"),
                "pages_fetched": all_roles_payload.get("pages_fetched"),
                "last_offset": all_roles_payload.get("last_offset"),
                "next_offset": all_roles_payload.get("next_offset"),
                "errors": list(all_roles_payload.get("errors") or []),
            }
            trade_fetch_statuses.append(str(all_roles_payload.get("status") or ""))
            for raw_row in all_roles_payload.get("rows") or []:
                if isinstance(raw_row, dict):
                    trades_rows.append(
                        _normalize_trade_row(
                            raw_row=raw_row,
                            captured_at=captured_at,
                            query_scope="all_roles",
                        )
                    )

    activity_fetch_payload: dict[str, Any] = {"status": "skipped", "rows": [], "errors": []}
    if refresh_activity:
        activity_fetch_payload = _fetch_data_api_rows(
            endpoint="activity",
            wallet_address=wallet,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            page_size=activity_page_size,
            max_pages=activity_max_pages,
            http_get_json=http_get_json,
        )
        for raw_row in activity_fetch_payload.get("rows") or []:
            if isinstance(raw_row, dict):
                activity_rows.append(
                    _normalize_activity_row(
                        raw_row=raw_row,
                        captured_at=captured_at,
                    )
                )

    trades_fetch = {
        "status": _combine_fetch_status(trade_fetch_statuses),
        "taker_only": taker_only_trade_fetch,
        "all_roles": all_roles_trade_fetch,
        "rows_total": len(trades_rows),
        "non_taker_trade_delta": max(
            0,
            int(all_roles_trade_fetch.get("rows") or 0)
            - int(taker_only_trade_fetch.get("rows") or 0),
        ),
    }

    activity_fetch = {
        "status": str(activity_fetch_payload.get("status") or "skipped"),
        "rows_total": len(activity_rows),
        "http_status": activity_fetch_payload.get("http_status"),
        "pages_fetched": activity_fetch_payload.get("pages_fetched"),
        "last_offset": activity_fetch_payload.get("last_offset"),
        "next_offset": activity_fetch_payload.get("next_offset"),
        "errors": list(activity_fetch_payload.get("errors") or []),
    }

    ledger_events_payload = build_public_observed_ledger_events(
        trades_rows=trades_rows,
        activity_rows=activity_rows,
    )

    ledger_fetch = {
        "status": _combine_fetch_status(
            [
                str(trades_fetch.get("status") or ""),
                str(activity_fetch.get("status") or ""),
            ]
        ),
        "trades": trades_fetch,
        "activity": activity_fetch,
        "events": {
            "raw_rows_total": ledger_events_payload.get("raw_rows_total"),
            "canonical_rows_total": ledger_events_payload.get("canonical_rows_total"),
            "duplicates_dropped": ledger_events_payload.get("duplicates_dropped"),
            "raw_source_counts": dict(ledger_events_payload.get("raw_source_counts") or {}),
            "canonical_source_counts": dict(
                ledger_events_payload.get("canonical_source_counts") or {}
            ),
            "raw_type_counts": dict(ledger_events_payload.get("raw_type_counts") or {}),
            "canonical_type_counts": dict(
                ledger_events_payload.get("canonical_type_counts") or {}
            ),
            "duplicates_by_source": dict(
                ledger_events_payload.get("duplicates_by_source") or {}
            ),
        },
    }

    return {
        "status": status,
        "wallet_address": wallet,
        "requested_wallet_address": requested_wallet,
        "normalized_wallet_address": wallet,
        "profile_wallet_resolution": profile_resolution,
        "captured_at": valuation_time,
        "data_api_base_url": base_url,
        "value_endpoint_status": value_status,
        "positions_endpoint_status": positions_status,
        "positions_pages_fetched": pages_fetched,
        "positions_page_size": page_size,
        "positions_max_pages": max_pages,
        "equity_row": equity_row,
        "positions_rows": positions_rows,
        "trades_rows": trades_rows,
        "activity_rows": activity_rows,
        "ledger_events_rows": list(ledger_events_payload.get("rows") or []),
        "ledger_fetch": ledger_fetch,
        "observability_mode": "public_observed_ledger",
        "private_order_lifecycle_observable": False,
        "errors": errors,
    }


def write_coldmath_snapshot_csvs(
    *,
    snapshot_dir: str | Path,
    equity_row: dict[str, Any],
    positions_rows: list[dict[str, Any]],
    trades_rows: list[dict[str, Any]] | None = None,
    activity_rows: list[dict[str, Any]] | None = None,
    ledger_events_rows: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    snapshot_path = Path(snapshot_dir)
    snapshot_path.mkdir(parents=True, exist_ok=True)

    equity_path = snapshot_path / "equity.csv"
    with equity_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EQUITY_FIELDNAMES)
        writer.writeheader()
        writer.writerow({field: _normalize_text(equity_row.get(field)) for field in EQUITY_FIELDNAMES})

    positions_path = snapshot_path / "positions.csv"
    with positions_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=POSITIONS_FIELDNAMES)
        writer.writeheader()
        for row in positions_rows:
            writer.writerow({field: _normalize_text(row.get(field)) for field in POSITIONS_FIELDNAMES})

    trades_path = snapshot_path / "trades.csv"
    if trades_rows is not None:
        with trades_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TRADES_FIELDNAMES)
            writer.writeheader()
            for row in trades_rows:
                writer.writerow({field: _normalize_text(row.get(field)) for field in TRADES_FIELDNAMES})

    activity_path = snapshot_path / "activity.csv"
    if activity_rows is not None:
        with activity_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=ACTIVITY_FIELDNAMES)
            writer.writeheader()
            for row in activity_rows:
                writer.writerow({field: _normalize_text(row.get(field)) for field in ACTIVITY_FIELDNAMES})

    ledger_events_path = snapshot_path / "ledger_events.csv"
    if ledger_events_rows is not None:
        with ledger_events_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=LEDGER_EVENTS_FIELDNAMES)
            writer.writeheader()
            for row in ledger_events_rows:
                writer.writerow(
                    {field: _normalize_text(row.get(field)) for field in LEDGER_EVENTS_FIELDNAMES}
                )

    written = {
        "snapshot_dir": str(snapshot_path),
        "equity_csv": str(equity_path),
        "positions_csv": str(positions_path),
    }
    if trades_rows is not None:
        written["trades_csv"] = str(trades_path)
    if activity_rows is not None:
        written["activity_csv"] = str(activity_path)
    if ledger_events_rows is not None:
        written["ledger_events_csv"] = str(ledger_events_path)
    return written


def summarize_coldmath_snapshot_files(
    *,
    equity_csv: str | Path,
    positions_csv: str | Path,
    trades_csv: str | Path | None = None,
    activity_csv: str | Path | None = None,
    ledger_events_csv: str | Path | None = None,
    wallet_address: str = "",
    stale_hours: float = 48.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    stale_seconds_threshold = max(0.0, float(stale_hours)) * 3600.0

    equity_path = Path(equity_csv)
    positions_path = Path(positions_csv)
    trades_path = Path(trades_csv) if trades_csv else None
    activity_path = Path(activity_csv) if activity_csv else None
    ledger_events_path = Path(ledger_events_csv) if ledger_events_csv else None

    equity_rows = _read_csv_rows(equity_path)
    positions_rows = _read_csv_rows(positions_path)
    trades_rows = _read_csv_rows(trades_path) if isinstance(trades_path, Path) else []
    activity_rows = _read_csv_rows(activity_path) if isinstance(activity_path, Path) else []
    ledger_events_rows = (
        _read_csv_rows(ledger_events_path) if isinstance(ledger_events_path, Path) else []
    )

    missing_files: list[str] = []
    if not equity_path.exists():
        missing_files.append(str(equity_path))
    if not positions_path.exists():
        missing_files.append(str(positions_path))

    equity_row = equity_rows[0] if equity_rows else {}
    valuation_time = _parse_ts(equity_row.get("valuationTime"))
    if valuation_time is None:
        for row in positions_rows[:25]:
            valuation_time = _parse_ts(row.get("valuationTime"))
            if isinstance(valuation_time, datetime):
                break

    stale_seconds: float | None = None
    if isinstance(valuation_time, datetime):
        stale_seconds = max(0.0, (now_utc - valuation_time).total_seconds())

    cash_balance = _parse_float(equity_row.get("cashBalance"))
    positions_value = _parse_float(equity_row.get("positionsValue"))
    equity_value = _parse_float(equity_row.get("equity"))

    priced_positions = 0
    unpriced_positions = 0
    positions_with_current_value = 0
    notional_sum = 0.0
    current_value_sum = 0.0
    top_positions: list[dict[str, Any]] = []
    for row in positions_rows:
        size = _parse_float(row.get("size")) or 0.0
        price = _parse_float(row.get("curPrice"))
        current_value = _parse_float(row.get("currentValue"))

        if isinstance(price, float) and price > 0:
            priced_positions += 1
            estimated_notional = float(size) * float(price)
        else:
            unpriced_positions += 1
            estimated_notional = 0.0

        if isinstance(current_value, float):
            positions_with_current_value += 1
            current_value_sum += current_value
            notional_reference = current_value
        else:
            notional_reference = estimated_notional

        notional_sum += estimated_notional
        top_positions.append(
            {
                "condition_id": _normalize_text(row.get("conditionId")),
                "asset": _normalize_text(row.get("asset")),
                "event_slug": _normalize_text(row.get("eventSlug")),
                "slug": _normalize_text(row.get("slug")),
                "outcome": _normalize_text(row.get("outcome")),
                "size": round(float(size), 6),
                "cur_price": round(float(price), 8) if isinstance(price, float) else None,
                "current_value": (
                    round(float(current_value), 6)
                    if isinstance(current_value, float)
                    else None
                ),
                "estimated_notional": round(estimated_notional, 6),
                "sorting_notional": round(float(notional_reference), 6),
            }
        )
    top_positions.sort(
        key=lambda item: abs(float(item.get("sorting_notional") or 0.0)),
        reverse=True,
    )
    for row in top_positions:
        row.pop("sorting_notional", None)

    family_behavior = summarize_family_behavior(positions_rows=positions_rows)
    if not ledger_events_rows and (trades_rows or activity_rows):
        ledger_events_rows = list(
            build_public_observed_ledger_events(
                trades_rows=trades_rows,
                activity_rows=activity_rows,
            ).get("rows")
            or []
        )

    trade_scope_counts: dict[str, int] = {}
    trade_side_counts: dict[str, int] = {}
    trade_notional_sum = 0.0
    unique_trade_tx_hashes: set[str] = set()
    trade_timestamps: list[datetime] = []
    for row in trades_rows:
        scope = _normalize_text(row.get("queryScope")).lower() or "unknown"
        trade_scope_counts[scope] = trade_scope_counts.get(scope, 0) + 1

        side = _normalize_text(row.get("side")).lower() or "unknown"
        trade_side_counts[side] = trade_side_counts.get(side, 0) + 1

        usdc_size = _parse_float(row.get("usdcSize"))
        size = _parse_float(row.get("size"))
        price = _parse_float(row.get("price"))
        if isinstance(usdc_size, float):
            trade_notional_sum += abs(usdc_size)
        elif isinstance(size, float) and isinstance(price, float):
            trade_notional_sum += abs(size * price)

        tx_hash = _normalize_text(row.get("transactionHash")).lower()
        if tx_hash:
            unique_trade_tx_hashes.add(tx_hash)
        timestamp = _parse_ts(row.get("timestamp"))
        if isinstance(timestamp, datetime):
            trade_timestamps.append(timestamp)

    activity_type_counts: dict[str, int] = {}
    unique_activity_tx_hashes: set[str] = set()
    activity_timestamps: list[datetime] = []
    for row in activity_rows:
        activity_type = _normalize_text(row.get("type")).upper() or "UNKNOWN"
        activity_type_counts[activity_type] = activity_type_counts.get(activity_type, 0) + 1

        tx_hash = _normalize_text(row.get("transactionHash")).lower()
        if tx_hash:
            unique_activity_tx_hashes.add(tx_hash)
        timestamp = _parse_ts(row.get("timestamp"))
        if isinstance(timestamp, datetime):
            activity_timestamps.append(timestamp)

    ledger_event_type_counts: dict[str, int] = {}
    ledger_event_source_counts: dict[str, int] = {}
    ledger_event_class_counts: dict[str, int] = {}
    ledger_event_timestamps: list[datetime] = []
    canonical_event_keys: set[str] = set()
    duplicate_event_rows = 0
    trade_like_event_count = 0
    for row in ledger_events_rows:
        event_type = _normalize_text(row.get("eventType")).upper() or "UNKNOWN"
        event_source = _normalize_text(row.get("source")).lower() or "unknown"
        event_class = _normalize_text(row.get("eventClass")).lower() or "unknown"
        ledger_event_type_counts[event_type] = ledger_event_type_counts.get(event_type, 0) + 1
        ledger_event_source_counts[event_source] = (
            ledger_event_source_counts.get(event_source, 0) + 1
        )
        ledger_event_class_counts[event_class] = ledger_event_class_counts.get(event_class, 0) + 1
        if _coerce_bool(row.get("isTradeLike")):
            trade_like_event_count += 1
        event_key = _normalize_text(row.get("eventKey"))
        if event_key:
            if event_key in canonical_event_keys:
                duplicate_event_rows += 1
            canonical_event_keys.add(event_key)
        timestamp = _parse_ts(row.get("eventTimestamp"))
        if isinstance(timestamp, datetime):
            ledger_event_timestamps.append(timestamp)

    ledger_missing_files: list[str] = []
    if isinstance(trades_path, Path) and not trades_path.exists():
        ledger_missing_files.append(str(trades_path))
    if isinstance(activity_path, Path) and not activity_path.exists():
        ledger_missing_files.append(str(activity_path))
    if not trades_path and not activity_path and not ledger_events_path:
        ledger_status = "not_configured"
    elif ledger_missing_files:
        ledger_status = "missing"
    elif trades_rows or activity_rows or ledger_events_rows:
        ledger_status = "ready"
    else:
        ledger_status = "empty"

    if missing_files:
        status = "missing"
    elif stale_seconds is not None and stale_seconds_threshold > 0 and stale_seconds > stale_seconds_threshold:
        status = "stale"
    elif equity_rows or positions_rows:
        status = "ready"
    else:
        status = "empty"

    return {
        "status": status,
        "wallet_address": _normalize_text(wallet_address).lower(),
        "equity_csv": str(equity_path),
        "positions_csv": str(positions_path),
        "trades_csv": str(trades_path) if isinstance(trades_path, Path) else "",
        "activity_csv": str(activity_path) if isinstance(activity_path, Path) else "",
        "ledger_events_csv": (
            str(ledger_events_path) if isinstance(ledger_events_path, Path) else ""
        ),
        "observability_mode": "public_observed_ledger",
        "private_order_lifecycle_observable": False,
        "equity_rows": len(equity_rows),
        "positions_rows": len(positions_rows),
        "missing_files": missing_files,
        "valuation_time": valuation_time.isoformat() if isinstance(valuation_time, datetime) else "",
        "stale_seconds": stale_seconds,
        "stale_hours_threshold": float(stale_hours),
        "cash_balance": cash_balance,
        "positions_value": positions_value,
        "equity": equity_value,
        "priced_positions": priced_positions,
        "unpriced_positions": unpriced_positions,
        "positions_with_current_value": positions_with_current_value,
        "priced_ratio": (
            round(priced_positions / float(len(positions_rows)), 6)
            if positions_rows
            else 0.0
        ),
        "estimated_notional_sum": round(notional_sum, 6),
        "current_value_sum": round(current_value_sum, 6),
        "top_positions_by_size": top_positions[:10],
        "family_behavior": family_behavior,
        "ledger": {
            "status": ledger_status,
            "missing_files": ledger_missing_files,
            "trades_rows_total": len(trades_rows),
            "activity_rows_total": len(activity_rows),
            "trade_scope_counts": trade_scope_counts,
            "trade_side_counts": trade_side_counts,
            "activity_type_counts": activity_type_counts,
            "trade_notional_sum": round(trade_notional_sum, 6),
            "unique_trade_transaction_hashes": len(unique_trade_tx_hashes),
            "unique_activity_transaction_hashes": len(unique_activity_tx_hashes),
            "trade_timestamp_min": (
                min(trade_timestamps).isoformat() if trade_timestamps else ""
            ),
            "trade_timestamp_max": (
                max(trade_timestamps).isoformat() if trade_timestamps else ""
            ),
            "activity_timestamp_min": (
                min(activity_timestamps).isoformat() if activity_timestamps else ""
            ),
            "activity_timestamp_max": (
                max(activity_timestamps).isoformat() if activity_timestamps else ""
            ),
            "events_rows_total": len(ledger_events_rows),
            "trade_like_events_total": trade_like_event_count,
            "event_type_counts": ledger_event_type_counts,
            "event_source_counts": ledger_event_source_counts,
            "event_class_counts": ledger_event_class_counts,
            "event_timestamp_min": (
                min(ledger_event_timestamps).isoformat() if ledger_event_timestamps else ""
            ),
            "event_timestamp_max": (
                max(ledger_event_timestamps).isoformat() if ledger_event_timestamps else ""
            ),
            "event_keys_unique": len(canonical_event_keys),
            "event_duplicate_rows_detected": duplicate_event_rows,
        },
    }


def run_coldmath_snapshot_summary(
    *,
    snapshot_dir: str = "tmp/coldmath_snapshot",
    equity_csv: str | None = None,
    positions_csv: str | None = None,
    trades_csv: str | None = None,
    activity_csv: str | None = None,
    ledger_events_csv: str | None = None,
    wallet_address: str = "",
    stale_hours: float = 48.0,
    output_dir: str = "outputs",
    now: datetime | None = None,
    refresh_from_api: bool = False,
    refresh_trades_from_api: bool = True,
    refresh_activity_from_api: bool = True,
    include_taker_only_trades: bool = True,
    include_all_trade_roles: bool = True,
    data_api_base_url: str = POLYMARKET_DATA_API_BASE_URL,
    api_timeout_seconds: float = 20.0,
    positions_page_size: int = 500,
    positions_max_pages: int = 20,
    trades_page_size: int = 500,
    trades_max_pages: int = 20,
    activity_page_size: int = 500,
    activity_max_pages: int = 20,
    build_public_ledger_events: bool = True,
    http_get_json: JsonGetter = _http_get_json,
) -> dict[str, Any]:
    captured_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    snapshot_path = Path(snapshot_dir)
    effective_equity_csv = equity_csv or str(snapshot_path / "equity.csv")
    effective_positions_csv = positions_csv or str(snapshot_path / "positions.csv")
    effective_trades_csv = trades_csv or str(snapshot_path / "trades.csv")
    effective_activity_csv = activity_csv or str(snapshot_path / "activity.csv")
    effective_ledger_events_csv = ledger_events_csv or str(snapshot_path / "ledger_events.csv")
    effective_wallet_address = _normalize_text(wallet_address).lower()

    api_fetch_summary: dict[str, Any] | None = None
    if refresh_from_api:
        if not _normalize_text(wallet_address):
            raise ValueError("wallet_address is required when refresh_from_api is enabled")
        api_snapshot = fetch_polymarket_wallet_snapshot(
            wallet_address=wallet_address,
            data_api_base_url=data_api_base_url,
            timeout_seconds=api_timeout_seconds,
            positions_page_size=positions_page_size,
            positions_max_pages=positions_max_pages,
            refresh_trades=refresh_trades_from_api,
            refresh_activity=refresh_activity_from_api,
            include_taker_only_trades=include_taker_only_trades,
            include_all_trade_roles=include_all_trade_roles,
            trades_page_size=trades_page_size,
            trades_max_pages=trades_max_pages,
            activity_page_size=activity_page_size,
            activity_max_pages=activity_max_pages,
            now=captured_at,
            http_get_json=http_get_json,
        )
        api_fetch_summary = {
            "status": api_snapshot.get("status"),
            "captured_at": api_snapshot.get("captured_at"),
            "value_endpoint_status": api_snapshot.get("value_endpoint_status"),
            "positions_endpoint_status": api_snapshot.get("positions_endpoint_status"),
            "positions_pages_fetched": api_snapshot.get("positions_pages_fetched"),
            "errors": list(api_snapshot.get("errors") or []),
            "positions_rows": len(api_snapshot.get("positions_rows") or []),
            "ledger_fetch": dict(api_snapshot.get("ledger_fetch") or {}),
            "requested_wallet_address": api_snapshot.get("requested_wallet_address"),
            "normalized_wallet_address": api_snapshot.get("normalized_wallet_address"),
            "profile_wallet_resolution": dict(
                api_snapshot.get("profile_wallet_resolution") or {}
            ),
            "observability_mode": api_snapshot.get("observability_mode"),
            "private_order_lifecycle_observable": api_snapshot.get(
                "private_order_lifecycle_observable"
            ),
        }

        should_write_api_snapshot = api_snapshot.get("status") in {"ready", "partial"} and bool(
            api_snapshot.get("equity_row")
            or api_snapshot.get("positions_rows")
            or api_snapshot.get("trades_rows")
            or api_snapshot.get("activity_rows")
        )
        if should_write_api_snapshot:
            written = write_coldmath_snapshot_csvs(
                snapshot_dir=snapshot_path,
                equity_row=dict(api_snapshot.get("equity_row") or {}),
                positions_rows=list(api_snapshot.get("positions_rows") or []),
                trades_rows=(
                    list(api_snapshot.get("trades_rows") or [])
                    if refresh_trades_from_api
                    else None
                ),
                activity_rows=(
                    list(api_snapshot.get("activity_rows") or [])
                    if refresh_activity_from_api
                    else None
                ),
                ledger_events_rows=(
                    list(api_snapshot.get("ledger_events_rows") or [])
                    if build_public_ledger_events
                    else None
                ),
            )
            effective_equity_csv = written["equity_csv"]
            effective_positions_csv = written["positions_csv"]
            if "trades_csv" in written:
                effective_trades_csv = written["trades_csv"]
            if "activity_csv" in written:
                effective_activity_csv = written["activity_csv"]
            if "ledger_events_csv" in written:
                effective_ledger_events_csv = written["ledger_events_csv"]
            api_fetch_summary["equity_csv"] = effective_equity_csv
            api_fetch_summary["positions_csv"] = effective_positions_csv
            api_fetch_summary["trades_csv"] = effective_trades_csv
            api_fetch_summary["activity_csv"] = effective_activity_csv
            api_fetch_summary["ledger_events_csv"] = effective_ledger_events_csv
            effective_wallet_address = _normalize_text(
                api_snapshot.get("normalized_wallet_address")
                or api_snapshot.get("wallet_address")
                or wallet_address
            ).lower()

    summary = summarize_coldmath_snapshot_files(
        equity_csv=effective_equity_csv,
        positions_csv=effective_positions_csv,
        trades_csv=effective_trades_csv,
        activity_csv=effective_activity_csv,
        ledger_events_csv=effective_ledger_events_csv,
        wallet_address=effective_wallet_address,
        stale_hours=stale_hours,
        now=captured_at,
    )
    summary["requested_wallet_address"] = _normalize_text(wallet_address).lower()
    summary["normalized_wallet_address"] = effective_wallet_address
    summary["captured_at"] = captured_at.isoformat()
    summary["snapshot_dir"] = str(snapshot_path)
    summary["refresh_from_api"] = bool(refresh_from_api)
    if api_fetch_summary is not None:
        summary["api_fetch"] = api_fetch_summary
        summary["profile_wallet_resolution"] = dict(
            api_fetch_summary.get("profile_wallet_resolution") or {}
        )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"coldmath_snapshot_summary_{stamp}.json"
    latest_path = out_dir / "coldmath_snapshot_summary_latest.json"
    encoded = json.dumps(summary, indent=2, sort_keys=True)
    output_path.write_text(encoded, encoding="utf-8")
    latest_path.write_text(encoded, encoding="utf-8")
    summary["output_file"] = str(output_path)
    summary["latest_file"] = str(latest_path)
    return summary
