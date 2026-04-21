from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from betbot.coldmath_snapshot import run_coldmath_snapshot_summary, summarize_coldmath_snapshot_files


JsonGetter = Callable[[str, float], tuple[int, dict[str, Any] | list[Any] | Any]]


POLYMARKET_MARKET_FIELDNAMES = [
    "captured_at",
    "market_id",
    "market_slug",
    "question",
    "event_title",
    "rules_primary",
    "end_date",
    "active",
    "closed",
    "accepting_orders",
    "outcomes",
    "clob_token_ids",
    "condition_id",
    "source_url",
]


def _http_get_json(url: str, timeout_seconds: float) -> tuple[int, dict[str, Any] | list[Any] | Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "betbot/1.0",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            status = int(getattr(response, "status", 200) or 200)
            payload = json.loads(response.read().decode("utf-8"))
            return (status, payload)
    except Exception as exc:
        return (0, {"error": str(exc)})


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        return [item.strip() for item in text.split(",") if item.strip()]
    return []


def _extract_rules_text(row: dict[str, Any]) -> str:
    for key in (
        "rules",
        "description",
        "resolutionDescription",
        "resolutionCriteria",
    ):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_source_url(row: dict[str, Any]) -> str:
    value = row.get("resolutionSource")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return ""


def _is_temperature_market(row: dict[str, Any]) -> bool:
    question = str(row.get("question") or row.get("title") or "")
    rules = _extract_rules_text(row)
    merged = " ".join((question, rules)).lower()
    if "temperature" not in merged:
        return False
    if any(token in merged for token in ("highest temperature", "high temperature", "daily high", "lowest temperature", "daily low")):
        return True

    tags = row.get("tags")
    if isinstance(tags, list):
        tag_text = " ".join(str(tag.get("name") or "") for tag in tags if isinstance(tag, dict)).lower()
        if "weather" in tag_text and "temperature" in merged:
            return True
    return False


def normalize_polymarket_market_row(row: dict[str, Any], *, captured_at: datetime) -> dict[str, Any]:
    outcomes = _coerce_string_list(row.get("outcomes"))
    clob_token_ids = _coerce_string_list(row.get("clobTokenIds"))
    event_title = ""
    event_payload = row.get("event")
    if isinstance(event_payload, dict):
        event_title = str(event_payload.get("title") or event_payload.get("name") or "").strip()

    return {
        "captured_at": captured_at.isoformat(),
        "market_id": str(row.get("id") or row.get("marketId") or "").strip(),
        "market_slug": str(row.get("slug") or row.get("marketSlug") or "").strip(),
        "question": str(row.get("question") or row.get("title") or "").strip(),
        "event_title": event_title,
        "rules_primary": _extract_rules_text(row),
        "end_date": str(row.get("endDate") or row.get("end_date") or "").strip(),
        "active": _coerce_bool(row.get("active")),
        "closed": _coerce_bool(row.get("closed")),
        "accepting_orders": _coerce_bool(row.get("acceptingOrders")),
        "outcomes": outcomes,
        "clob_token_ids": clob_token_ids,
        "condition_id": str(row.get("conditionId") or "").strip(),
        "source_url": _extract_source_url(row),
    }


def fetch_polymarket_markets_page(
    *,
    offset: int,
    page_size: int,
    only_active: bool = True,
    gamma_base_url: str = "https://gamma-api.polymarket.com",
    timeout_seconds: float = 15.0,
    http_get_json: JsonGetter = _http_get_json,
) -> dict[str, Any]:
    clean_base = str(gamma_base_url or "").rstrip("/")
    params: list[tuple[str, str]] = [
        ("limit", str(max(1, int(page_size)))),
        ("offset", str(max(0, int(offset)))),
    ]
    if only_active:
        params.append(("active", "true"))

    url = f"{clean_base}/markets?{urlencode(params)}"
    status, payload = http_get_json(url, timeout_seconds)
    if status != 200:
        return {
            "status": "request_failed",
            "url": url,
            "http_status": status,
            "error": str(payload.get("error") if isinstance(payload, dict) else ""),
            "markets": [],
        }

    if isinstance(payload, list):
        markets = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        candidates = payload.get("markets")
        markets = [item for item in candidates if isinstance(item, dict)] if isinstance(candidates, list) else []
    else:
        markets = []

    return {
        "status": "ready",
        "url": url,
        "http_status": status,
        "markets": markets,
    }


def fetch_polymarket_temperature_markets(
    *,
    max_markets: int = 500,
    page_size: int = 200,
    max_pages: int = 10,
    only_active: bool = True,
    gamma_base_url: str = "https://gamma-api.polymarket.com",
    timeout_seconds: float = 15.0,
    http_get_json: JsonGetter = _http_get_json,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

    normalized_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    request_count = 0

    offset = 0
    for _ in range(max(1, int(max_pages))):
        if len(normalized_rows) >= max(1, int(max_markets)):
            break

        page_payload = fetch_polymarket_markets_page(
            offset=offset,
            page_size=page_size,
            only_active=only_active,
            gamma_base_url=gamma_base_url,
            timeout_seconds=timeout_seconds,
            http_get_json=http_get_json,
        )
        request_count += 1
        if str(page_payload.get("status") or "") != "ready":
            errors.append(
                {
                    "offset": offset,
                    "status": page_payload.get("status"),
                    "http_status": page_payload.get("http_status"),
                    "error": page_payload.get("error", ""),
                }
            )
            break

        markets = page_payload.get("markets")
        if not isinstance(markets, list) or not markets:
            break

        for row in markets:
            if not isinstance(row, dict):
                continue
            if not _is_temperature_market(row):
                continue
            normalized_rows.append(normalize_polymarket_market_row(row, captured_at=captured_at))
            if len(normalized_rows) >= max(1, int(max_markets)):
                break

        if len(markets) < max(1, int(page_size)):
            break
        offset += int(page_size)

    status = "ready"
    if errors and not normalized_rows:
        status = "request_failed"
    elif errors:
        status = "ready_partial"
    elif not normalized_rows:
        status = "no_markets"

    return {
        "status": status,
        "captured_at": captured_at.isoformat(),
        "markets": normalized_rows,
        "markets_count": len(normalized_rows),
        "request_count": request_count,
        "errors": errors,
    }


def _write_markets_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=POLYMARKET_MARKET_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            encoded = dict(row)
            encoded["outcomes"] = json.dumps(encoded.get("outcomes") or [])
            encoded["clob_token_ids"] = json.dumps(encoded.get("clob_token_ids") or [])
            writer.writerow(encoded)


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def summarize_coldmath_temperature_alignment(
    *,
    positions_csv: str | Path,
    markets: list[dict[str, Any]],
) -> dict[str, Any]:
    positions_path = Path(positions_csv)
    if not positions_path.exists():
        return {
            "status": "missing_positions_csv",
            "positions_csv": str(positions_path),
            "positions_rows": 0,
            "matched_positions": 0,
            "matched_ratio": 0.0,
            "unmatched_positions": 0,
            "top_matched_positions": [],
            "unmatched_condition_ids_preview": [],
        }

    condition_to_market: dict[str, dict[str, Any]] = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        condition_id = str(market.get("condition_id") or "").strip().lower()
        if condition_id and condition_id not in condition_to_market:
            condition_to_market[condition_id] = market

    with positions_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        positions_rows = [dict(row) for row in reader]

    matched_rows: list[dict[str, Any]] = []
    unmatched_condition_ids: list[str] = []
    for row in positions_rows:
        condition_id = str(row.get("conditionId") or "").strip().lower()
        if not condition_id:
            continue
        market = condition_to_market.get(condition_id)
        size = _parse_float(row.get("size")) or 0.0
        current_price = _parse_float(row.get("curPrice"))
        notional_estimate = abs(size * current_price) if isinstance(current_price, float) else None
        if market is None:
            unmatched_condition_ids.append(condition_id)
            continue
        matched_rows.append(
            {
                "condition_id": condition_id,
                "size": size,
                "cur_price": current_price,
                "estimated_notional": notional_estimate,
                "market_id": str(market.get("market_id") or ""),
                "market_slug": str(market.get("market_slug") or ""),
                "question": str(market.get("question") or ""),
                "event_title": str(market.get("event_title") or ""),
            }
        )

    matched_rows.sort(
        key=lambda row: abs(float(row.get("estimated_notional") or 0.0)),
        reverse=True,
    )
    matched_count = len(matched_rows)
    total_count = len(positions_rows)

    return {
        "status": "ready" if total_count > 0 else "empty_positions",
        "positions_csv": str(positions_path),
        "positions_rows": total_count,
        "matched_positions": matched_count,
        "matched_ratio": (round(matched_count / float(total_count), 6) if total_count > 0 else 0.0),
        "unmatched_positions": max(0, total_count - matched_count),
        "unique_matched_markets": len({str(row.get("market_slug") or "") for row in matched_rows if row.get("market_slug")}),
        "top_matched_positions": matched_rows[:10],
        "unmatched_condition_ids_preview": unmatched_condition_ids[:15],
    }


def run_polymarket_market_data_ingest(
    *,
    output_dir: str = "outputs",
    max_markets: int = 500,
    page_size: int = 200,
    max_pages: int = 10,
    only_active: bool = True,
    gamma_base_url: str = "https://gamma-api.polymarket.com",
    timeout_seconds: float = 15.0,
    http_get_json: JsonGetter = _http_get_json,
    coldmath_snapshot_dir: str | None = None,
    coldmath_equity_csv: str | None = None,
    coldmath_positions_csv: str | None = None,
    coldmath_wallet_address: str = "",
    coldmath_stale_hours: float = 48.0,
    coldmath_refresh_from_api: bool = False,
    coldmath_data_api_base_url: str = "https://data-api.polymarket.com",
    coldmath_api_timeout_seconds: float = 20.0,
    coldmath_positions_page_size: int = 500,
    coldmath_positions_max_pages: int = 20,
    coldmath_refresh_trades_from_api: bool = True,
    coldmath_refresh_activity_from_api: bool = True,
    coldmath_include_taker_only_trades: bool = True,
    coldmath_include_all_trade_roles: bool = True,
    coldmath_trades_page_size: int = 500,
    coldmath_trades_max_pages: int = 20,
    coldmath_activity_page_size: int = 500,
    coldmath_activity_max_pages: int = 20,
    now: datetime | None = None,
) -> dict[str, Any]:
    result = fetch_polymarket_temperature_markets(
        max_markets=max_markets,
        page_size=page_size,
        max_pages=max_pages,
        only_active=only_active,
        gamma_base_url=gamma_base_url,
        timeout_seconds=timeout_seconds,
        http_get_json=http_get_json,
        now=now,
    )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    csv_path = output_path / f"polymarket_temperature_markets_{stamp}.csv"
    _write_markets_csv(csv_path, result.get("markets") if isinstance(result.get("markets"), list) else [])

    summary = dict(result)
    summary["output_csv"] = str(csv_path)
    snapshot_dir_path: Path | None = None
    if coldmath_snapshot_dir:
        snapshot_dir_path = Path(coldmath_snapshot_dir)
    elif coldmath_equity_csv or coldmath_positions_csv:
        snapshot_dir_path = Path(".")
    if snapshot_dir_path is not None:
        effective_equity_csv = coldmath_equity_csv or str(snapshot_dir_path / "equity.csv")
        effective_positions_csv = coldmath_positions_csv or str(snapshot_dir_path / "positions.csv")
        if coldmath_refresh_from_api and str(coldmath_wallet_address or "").strip():
            coldmath_snapshot = run_coldmath_snapshot_summary(
                snapshot_dir=str(snapshot_dir_path),
                equity_csv=coldmath_equity_csv,
                positions_csv=coldmath_positions_csv,
                wallet_address=coldmath_wallet_address,
                stale_hours=coldmath_stale_hours,
                output_dir=output_dir,
                now=now,
                refresh_from_api=True,
                data_api_base_url=coldmath_data_api_base_url,
                api_timeout_seconds=coldmath_api_timeout_seconds,
                positions_page_size=coldmath_positions_page_size,
                positions_max_pages=coldmath_positions_max_pages,
                refresh_trades_from_api=coldmath_refresh_trades_from_api,
                refresh_activity_from_api=coldmath_refresh_activity_from_api,
                include_taker_only_trades=coldmath_include_taker_only_trades,
                include_all_trade_roles=coldmath_include_all_trade_roles,
                trades_page_size=coldmath_trades_page_size,
                trades_max_pages=coldmath_trades_max_pages,
                activity_page_size=coldmath_activity_page_size,
                activity_max_pages=coldmath_activity_max_pages,
            )
            summary["coldmath_snapshot"] = coldmath_snapshot
            effective_equity_csv = str(coldmath_snapshot.get("equity_csv") or effective_equity_csv)
            effective_positions_csv = str(coldmath_snapshot.get("positions_csv") or effective_positions_csv)
        else:
            summary["coldmath_snapshot"] = summarize_coldmath_snapshot_files(
                equity_csv=effective_equity_csv,
                positions_csv=effective_positions_csv,
                wallet_address=coldmath_wallet_address,
                stale_hours=coldmath_stale_hours,
                now=now or datetime.now(timezone.utc),
            )
        summary["coldmath_temperature_alignment"] = summarize_coldmath_temperature_alignment(
            positions_csv=effective_positions_csv,
            markets=summary.get("markets") if isinstance(summary.get("markets"), list) else [],
        )
    summary_path = output_path / f"polymarket_temperature_markets_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
