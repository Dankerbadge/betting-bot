from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen


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
    summary_path = output_path / f"polymarket_temperature_markets_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
