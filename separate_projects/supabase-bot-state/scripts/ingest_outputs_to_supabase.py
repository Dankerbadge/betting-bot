#!/usr/bin/env python3
"""Ingest local bot artifacts into a separate Supabase project.

This script intentionally uses OPSBOT_* environment variables and refuses to run
if the configured project appears to reference Zenith.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Iterator
import glob
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any
import urllib.error
import urllib.parse
import urllib.request


def _read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "t", "on"}:
        return True
    if text in {"0", "false", "no", "n", "f", "off"}:
        return False
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _derive_sizing_basis(payload: dict[str, Any]) -> str | None:
    direct = str(payload.get("sizing_basis") or "").strip()
    if direct:
        return direct
    start_dollars = _to_float(payload.get("shadow_bankroll_start_dollars"))
    if start_dollars is None:
        return None
    start_text = f"{max(0.0, start_dollars):.4f}".rstrip("0").rstrip(".") or "0"
    return f"shadow_{start_text}"


def _derive_execution_basis(payload: dict[str, Any]) -> str:
    direct = str(payload.get("execution_basis") or "").strip()
    if direct:
        return direct
    return "live_actual_balance"


def _chunked(items: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    if size <= 0:
        size = 250
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _assert_non_zenith(*, supabase_url: str, project_ref: str, forbidden_hint: str) -> None:
    hint = forbidden_hint.strip().lower()
    if not hint:
        return
    haystacks = [supabase_url.lower(), project_ref.lower()]
    for haystack in haystacks:
        if hint in haystack:
            raise RuntimeError(
                "Isolation guardrail failed: configured Supabase target appears to reference "
                f"'{hint}'. Use a new, separate project."
            )


class SupabaseRestClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        schema: str = "bot_ops",
        timeout_seconds: float = 20.0,
        network_retries: int = 5,
        retry_backoff_seconds: float = 2.0,
        max_retry_backoff_seconds: float = 30.0,
    ) -> None:
        cleaned = base_url.strip().rstrip("/")
        if not cleaned:
            raise ValueError("Missing Supabase URL")
        self.base_url = cleaned
        self.api_key = api_key.strip()
        self.schema = schema
        self.timeout_seconds = timeout_seconds
        self.network_retries = max(0, int(network_retries))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.max_retry_backoff_seconds = max(0.0, float(max_retry_backoff_seconds))
        if not self.api_key:
            raise ValueError("Missing Supabase service role key")

    @staticmethod
    def _is_retryable_http_error(exc: urllib.error.HTTPError) -> bool:
        return int(exc.code) in {408, 425, 429, 500, 502, 503, 504}

    def _retry_sleep_seconds(self, attempt: int) -> float:
        # attempt is 1-indexed.
        raw = self.retry_backoff_seconds * (2 ** max(0, attempt - 1))
        if self.max_retry_backoff_seconds > 0:
            return min(raw, self.max_retry_backoff_seconds)
        return raw

    def upsert_rows(self, *, table: str, rows: list[dict[str, Any]], on_conflict: str) -> None:
        if not rows:
            return
        query = urllib.parse.urlencode({"on_conflict": on_conflict})
        endpoint = f"{self.base_url}/rest/v1/{table}?{query}"
        payload = json.dumps(rows, separators=(",", ":")).encode("utf-8")
        total_attempts = 1 + self.network_retries
        for attempt in range(1, total_attempts + 1):
            request = urllib.request.Request(
                endpoint,
                method="POST",
                data=payload,
                headers={
                    "apikey": self.api_key,
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Accept-Profile": self.schema,
                    "Content-Profile": self.schema,
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    if response.status not in {200, 201, 204}:
                        body = response.read().decode("utf-8", errors="replace")
                        raise RuntimeError(
                            f"Unexpected Supabase response for table '{table}': "
                            f"status={response.status} body={body}"
                        )
                return
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                should_retry = self._is_retryable_http_error(exc) and attempt < total_attempts
                if not should_retry:
                    raise RuntimeError(
                        f"Supabase HTTP error for table '{table}': status={exc.code} body={body}"
                    ) from exc
                wait_seconds = self._retry_sleep_seconds(attempt)
                print(
                    f"[retry] {table}: transient HTTP {exc.code} on attempt {attempt}/{total_attempts}; "
                    f"sleeping {wait_seconds:.1f}s"
                )
                time.sleep(wait_seconds)
            except urllib.error.URLError as exc:
                if attempt >= total_attempts:
                    raise RuntimeError(
                        f"Supabase network error for table '{table}': {exc}"
                    ) from exc
                wait_seconds = self._retry_sleep_seconds(attempt)
                print(
                    f"[retry] {table}: transient network error on attempt {attempt}/{total_attempts}: {exc}; "
                    f"sleeping {wait_seconds:.1f}s"
                )
                time.sleep(wait_seconds)


def _extract_execution_journal_rows(*, outputs_dir: Path, limit: int) -> list[dict[str, Any]]:
    db_path = outputs_dir / "kalshi_execution_journal.sqlite3"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows: list[dict[str, Any]] = []
        sql = """
            SELECT *
            FROM execution_events
            ORDER BY event_id DESC
            LIMIT ?
        """
        for row in conn.execute(sql, (max(1, limit),)):
            item = dict(row)
            payload_raw = str(item.get("payload_json") or "")
            payload_json: dict[str, Any]
            try:
                payload_json = json.loads(payload_raw) if payload_raw else {}
            except json.JSONDecodeError:
                payload_json = {}
            event_id = item.get("event_id")
            run_id = str(item.get("run_id") or "unknown_run")
            external_event_key = f"execution_event::{run_id}::{event_id}"
            rows.append(
                {
                    "external_event_key": external_event_key,
                    "event_id": event_id,
                    "run_id": run_id,
                    "captured_at_utc": item.get("captured_at_utc"),
                    "event_type": item.get("event_type"),
                    "market_ticker": item.get("market_ticker"),
                    "event_family": item.get("event_family"),
                    "side": item.get("side"),
                    "limit_price_dollars": item.get("limit_price_dollars"),
                    "contracts_fp": item.get("contracts_fp"),
                    "client_order_id": item.get("client_order_id"),
                    "exchange_order_id": item.get("exchange_order_id"),
                    "parent_order_id": item.get("parent_order_id"),
                    "best_yes_bid_dollars": item.get("best_yes_bid_dollars"),
                    "best_yes_ask_dollars": item.get("best_yes_ask_dollars"),
                    "best_no_bid_dollars": item.get("best_no_bid_dollars"),
                    "best_no_ask_dollars": item.get("best_no_ask_dollars"),
                    "spread_dollars": item.get("spread_dollars"),
                    "visible_depth_contracts": item.get("visible_depth_contracts"),
                    "queue_position_contracts": item.get("queue_position_contracts"),
                    "signal_score": item.get("signal_score"),
                    "signal_age_seconds": item.get("signal_age_seconds"),
                    "time_to_close_seconds": item.get("time_to_close_seconds"),
                    "latency_ms": item.get("latency_ms"),
                    "websocket_lag_ms": item.get("websocket_lag_ms"),
                    "api_latency_ms": item.get("api_latency_ms"),
                    "fee_dollars": item.get("fee_dollars"),
                    "maker_fee_dollars": item.get("maker_fee_dollars"),
                    "taker_fee_dollars": item.get("taker_fee_dollars"),
                    "realized_pnl_dollars": item.get("realized_pnl_dollars"),
                    "markout_10s_dollars": item.get("markout_10s_dollars"),
                    "markout_60s_dollars": item.get("markout_60s_dollars"),
                    "markout_300s_dollars": item.get("markout_300s_dollars"),
                    "result": item.get("result"),
                    "status": item.get("status"),
                    "payload_json": payload_json,
                    "source_file": str(db_path),
                }
            )
    finally:
        conn.close()

    rows.reverse()
    return rows


def _extract_frontier_rows(*, outputs_dir: Path, limit: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pattern = str(outputs_dir / "execution_frontier_report_*.json")
    paths = sorted(Path(path) for path in glob.glob(pattern))
    if limit > 0:
        paths = paths[-limit:]

    report_rows: list[dict[str, Any]] = []
    bucket_rows: list[dict[str, Any]] = []

    for path in paths:
        payload = _read_json_file(path)
        if payload is None:
            continue
        run_id = str(payload.get("run_id") or path.stem)
        report_rows.append(
            {
                "run_id": run_id,
                "captured_at": payload.get("captured_at"),
                "status": str(payload.get("status") or "unknown"),
                "submitted_orders": payload.get("submitted_orders"),
                "filled_orders": payload.get("filled_orders"),
                "full_filled_orders": payload.get("full_filled_orders"),
                "fill_samples_with_markout": payload.get("fill_samples_with_markout"),
                "trusted_bucket_count": payload.get("frontier_trusted_bucket_count"),
                "untrusted_bucket_count": payload.get("frontier_untrusted_bucket_count"),
                "frontier_artifact_age_seconds": payload.get("frontier_artifact_age_seconds"),
                "frontier_selection_mode": payload.get("frontier_selection_mode"),
                "recommendations": payload.get("recommendations") or [],
                "source_strategy_counts": payload.get("source_strategy_counts") or {},
                "payload_json": payload,
                "source_file": str(path),
            }
        )

        bucket_payload = payload.get("bucket_rows")
        if not isinstance(bucket_payload, list):
            continue

        for bucket in bucket_payload:
            if not isinstance(bucket, dict):
                continue
            bucket_key = str(bucket.get("bucket") or "")
            if not bucket_key:
                continue
            bucket_rows.append(
                {
                    "frontier_run_id": run_id,
                    "bucket": bucket_key,
                    "orders_submitted": bucket.get("orders_submitted"),
                    "fill_rate": bucket.get("fill_rate"),
                    "full_fill_rate": bucket.get("full_fill_rate"),
                    "median_time_to_fill_seconds": bucket.get("median_time_to_fill_seconds") or None,
                    "p90_time_to_fill_seconds": bucket.get("p90_time_to_fill_seconds") or None,
                    "markout_10s_side_adjusted": bucket.get("markout_10s_side_adjusted") or None,
                    "markout_60s_side_adjusted": bucket.get("markout_60s_side_adjusted") or None,
                    "markout_300s_side_adjusted": bucket.get("markout_300s_side_adjusted") or None,
                    "markout_10s_samples": bucket.get("markout_10s_samples"),
                    "markout_60s_samples": bucket.get("markout_60s_samples"),
                    "markout_300s_samples": bucket.get("markout_300s_samples"),
                    "markout_horizons_trusted": _to_bool(bucket.get("markout_horizons_trusted")),
                    "markout_horizons_untrusted_reason": bucket.get("markout_horizons_untrusted_reason"),
                    "fee_spread_cancel_leakage_dollars_per_order": bucket.get(
                        "fee_spread_cancel_leakage_dollars_per_order"
                    ),
                    "expected_net_edge_after_costs_per_contract": bucket.get(
                        "expected_net_edge_after_costs_per_contract"
                    )
                    or None,
                    "break_even_edge_per_contract": bucket.get("break_even_edge_per_contract"),
                    "payload_json": bucket,
                }
            )

    return report_rows, bucket_rows


def _extract_climate_rows(*, outputs_dir: Path, limit: int) -> list[dict[str, Any]]:
    db_path = outputs_dir / "kalshi_climate_availability.sqlite3"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT *
            FROM climate_ticker_observations
            ORDER BY observation_id DESC
            LIMIT ?
        """
        rows: list[dict[str, Any]] = []
        for row in conn.execute(sql, (max(1, limit),)):
            item = dict(row)
            observation_id = item.get("observation_id")
            run_id = str(item.get("run_id") or "unknown_run")
            rows.append(
                {
                    "external_event_key": f"climate_observation::{run_id}::{observation_id}",
                    "observation_id": observation_id,
                    "run_id": run_id,
                    "observed_at_utc": item.get("observed_at_utc"),
                    "market_ticker": item.get("market_ticker"),
                    "contract_family": item.get("contract_family"),
                    "strip_key": item.get("strip_key"),
                    "event_type": item.get("event_type"),
                    "yes_bid_dollars": item.get("yes_bid_dollars"),
                    "yes_ask_dollars": item.get("yes_ask_dollars"),
                    "no_bid_dollars": item.get("no_bid_dollars"),
                    "no_ask_dollars": item.get("no_ask_dollars"),
                    "spread_dollars": item.get("spread_dollars"),
                    "has_quotes": _to_bool(item.get("has_quotes")),
                    "has_orderable_side": _to_bool(item.get("has_orderable_side")),
                    "non_endpoint_quote": _to_bool(item.get("non_endpoint_quote")),
                    "endpoint_only": _to_bool(item.get("endpoint_only")),
                    "two_sided_book": _to_bool(item.get("two_sided_book")),
                    "wakeup_transition": _to_bool(item.get("wakeup_transition")),
                    "public_trade_event": _to_bool(item.get("public_trade_event")),
                    "public_trade_contracts": item.get("public_trade_contracts"),
                    "updated_at_utc": item.get("updated_at_utc"),
                    "payload_json": item,
                    "source_file": str(db_path),
                }
            )
    finally:
        conn.close()

    rows.reverse()
    return rows


def _extract_overnight_run_row(*, outputs_dir: Path) -> list[dict[str, Any]]:
    path = outputs_dir / "overnight_alpha_latest.json"
    payload = _read_json_file(path)
    if payload is None:
        return []

    normalized_payload = dict(payload)
    sizing_basis = _derive_sizing_basis(normalized_payload)
    execution_basis = _derive_execution_basis(normalized_payload)
    if sizing_basis is not None and str(normalized_payload.get("sizing_basis") or "").strip() == "":
        normalized_payload["sizing_basis"] = sizing_basis
    if str(normalized_payload.get("execution_basis") or "").strip() == "":
        normalized_payload["execution_basis"] = execution_basis

    run_id = str(normalized_payload.get("run_id") or path.stem)
    row = {
        "run_id": run_id,
        "run_started_at_utc": normalized_payload.get("run_started_at_utc"),
        "run_finished_at_utc": normalized_payload.get("run_finished_at_utc"),
        "run_stamp_utc": normalized_payload.get("run_stamp_utc"),
        "overall_status": normalized_payload.get("overall_status"),
        "mode": normalized_payload.get("mode"),
        "pipeline_ready": _to_bool(normalized_payload.get("pipeline_ready")),
        "live_ready": _to_bool(normalized_payload.get("live_ready")),
        "frontier_trusted_bucket_count": normalized_payload.get("frontier_trusted_bucket_count"),
        "frontier_untrusted_bucket_count": normalized_payload.get("frontier_untrusted_bucket_count"),
        "daily_weather_market_availability_regime": normalized_payload.get("daily_weather_market_availability_regime"),
        "daily_weather_market_availability_regime_reason": normalized_payload.get(
            "daily_weather_market_availability_regime_reason"
        ),
        "climate_rows_total": normalized_payload.get("climate_rows_total"),
        "climate_tradable_positive_rows": normalized_payload.get("climate_tradable_positive_rows"),
        "climate_hot_positive_rows": normalized_payload.get("climate_hot_positive_rows"),
        "climate_router_pilot_status": normalized_payload.get("climate_router_pilot_status"),
        "climate_router_pilot_expected_value_dollars": normalized_payload.get(
            "climate_router_pilot_expected_value_dollars"
        ),
        "climate_router_pilot_total_risk_dollars": normalized_payload.get("climate_router_pilot_total_risk_dollars"),
        "climate_router_pilot_promoted_rows": normalized_payload.get("climate_router_pilot_promoted_rows"),
        "climate_router_pilot_attempted_orders": normalized_payload.get("climate_router_pilot_attempted_orders"),
        "climate_router_pilot_filled_orders": normalized_payload.get("climate_router_pilot_filled_orders"),
        "climate_router_pilot_realized_pnl_dollars": normalized_payload.get("climate_router_pilot_realized_pnl_dollars"),
        "router_vs_planner_gap_status": normalized_payload.get("router_vs_planner_gap_status"),
        "router_tradable_not_planned_count": normalized_payload.get("router_tradable_not_planned_count"),
        "payload_json": normalized_payload,
        "source_file": str(path),
    }
    return [row]


def _extract_pilot_scorecard_rows(*, outputs_dir: Path, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    overnight_latest = _read_json_file(outputs_dir / "overnight_alpha_latest.json") or {}
    fallback_run_id = str(overnight_latest.get("run_id") or "").strip()
    fallback_sizing_basis = _derive_sizing_basis(overnight_latest)
    fallback_execution_basis = _derive_execution_basis(overnight_latest)
    fallback_captured_at = overnight_latest.get("run_finished_at_utc") or overnight_latest.get("run_started_at_utc")

    alpha_paths = sorted(Path(path) for path in glob.glob(str(outputs_dir / "alpha_scoreboard_*.json")))
    autopilot_paths = sorted(Path(path) for path in glob.glob(str(outputs_dir / "kalshi_autopilot_summary_*.json")))

    if limit > 0:
        alpha_paths = alpha_paths[-limit:]
        autopilot_paths = autopilot_paths[-limit:]

    for path in alpha_paths:
        payload = _read_json_file(path)
        if payload is None:
            continue
        captured_at = payload.get("captured_at")
        annualized = None
        bankroll_projection = payload.get("bankroll_projection")
        if isinstance(bankroll_projection, dict):
            annualized = bankroll_projection.get("annualized_net_return_pct")
        rows.append(
            {
                "scorecard_key": f"alpha_scoreboard::{path.stem}",
                "captured_at": captured_at,
                "scorecard_type": "alpha_scoreboard",
                "status": payload.get("status"),
                "headline_metric_name": "annualized_net_return_pct",
                "headline_metric_value": annualized,
                "headline_metric_unit": "pct",
                "payload_json": payload,
                "source_file": str(path),
            }
        )

    for path in autopilot_paths:
        payload = _read_json_file(path)
        if payload is None:
            continue
        captured_at = payload.get("captured_at")
        gate_pass = _to_bool(payload.get("preflight_gate_pass"))
        rows.append(
            {
                "scorecard_key": f"autopilot_summary::{path.stem}",
                "captured_at": captured_at,
                "scorecard_type": "autopilot_summary",
                "status": payload.get("status"),
                "headline_metric_name": "preflight_gate_pass",
                "headline_metric_value": 1.0 if gate_pass else 0.0,
                "headline_metric_unit": "bool_01",
                "payload_json": payload,
                "source_file": str(path),
            }
        )

    evidence_path = outputs_dir / "pilot_execution_evidence_latest.json"
    evidence_payload = _read_json_file(evidence_path)
    if evidence_payload is not None:
        enriched_evidence_payload = dict(evidence_payload)
        if fallback_run_id and str(enriched_evidence_payload.get("run_id") or "").strip() == "":
            enriched_evidence_payload["run_id"] = fallback_run_id
        if (
            fallback_sizing_basis is not None
            and str(enriched_evidence_payload.get("sizing_basis") or "").strip() == ""
        ):
            enriched_evidence_payload["sizing_basis"] = fallback_sizing_basis
        if (
            fallback_execution_basis is not None
            and str(enriched_evidence_payload.get("execution_basis") or "").strip() == ""
        ):
            enriched_evidence_payload["execution_basis"] = fallback_execution_basis

        funnel = enriched_evidence_payload.get("pilot_funnel")
        attempted_orders = None
        if isinstance(funnel, dict):
            attempted_orders = funnel.get("attempted_orders")
        if attempted_orders is None:
            attempted_orders = enriched_evidence_payload.get("pilot_execution_attempted_orders")

        captured_at = (
            enriched_evidence_payload.get("generated_at_utc")
            or enriched_evidence_payload.get("captured_at")
            or fallback_captured_at
        )
        evidence_status = enriched_evidence_payload.get("pilot_execution_evidence_status")
        if evidence_status is None:
            first_attempt = enriched_evidence_payload.get("first_attempt_evidence")
            if isinstance(first_attempt, dict):
                evidence_status = first_attempt.get("status")
        evidence_key_seed = (
            str(enriched_evidence_payload.get("run_id") or "").strip()
            or str(enriched_evidence_payload.get("generated_at_utc") or "").strip()
            or evidence_path.stem
        )
        rows.append(
            {
                "scorecard_key": f"pilot_execution_evidence::{evidence_key_seed}",
                "captured_at": captured_at,
                "scorecard_type": "pilot_execution_evidence",
                "status": evidence_status,
                "headline_metric_name": "attempted_orders",
                "headline_metric_value": attempted_orders,
                "headline_metric_unit": "count",
                "payload_json": enriched_evidence_payload,
                "source_file": str(evidence_path),
            }
        )

    monthly_summary_path = outputs_dir / "monthly_climate_live_attempt_summary_latest.json"
    monthly_summary_payload = _read_json_file(monthly_summary_path)
    if monthly_summary_payload is not None:
        enriched_monthly_summary_payload = dict(monthly_summary_payload)
        if fallback_run_id and str(enriched_monthly_summary_payload.get("run_id") or "").strip() == "":
            enriched_monthly_summary_payload["run_id"] = fallback_run_id
        if (
            fallback_sizing_basis is not None
            and str(enriched_monthly_summary_payload.get("sizing_basis") or "").strip() == ""
        ):
            enriched_monthly_summary_payload["sizing_basis"] = fallback_sizing_basis
        if (
            fallback_execution_basis is not None
            and str(enriched_monthly_summary_payload.get("execution_basis") or "").strip() == ""
        ):
            enriched_monthly_summary_payload["execution_basis"] = fallback_execution_basis

        captured_at = (
            enriched_monthly_summary_payload.get("generated_at_utc")
            or enriched_monthly_summary_payload.get("captured_at")
            or fallback_captured_at
        )
        attempted_orders = enriched_monthly_summary_payload.get("climate_router_pilot_attempted_orders")
        summary_key_seed = (
            str(enriched_monthly_summary_payload.get("generated_at_utc") or "").strip()
            or str(enriched_monthly_summary_payload.get("run_id") or "").strip()
            or monthly_summary_path.stem
        )
        rows.append(
            {
                "scorecard_key": f"monthly_live_attempt_summary::{summary_key_seed}",
                "captured_at": captured_at,
                "scorecard_type": "monthly_live_attempt_summary",
                "status": enriched_monthly_summary_payload.get("status"),
                "headline_metric_name": "attempted_orders",
                "headline_metric_value": attempted_orders,
                "headline_metric_unit": "count",
                "payload_json": enriched_monthly_summary_payload,
                "source_file": str(monthly_summary_path),
            }
        )

    # Paper-live scorecards are derived from overnight payload fields so we can analyze
    # fill quality by family and ticker without adding new tables.
    overnight_payload = overnight_latest if isinstance(overnight_latest, dict) else {}
    overnight_run_id = str(overnight_payload.get("run_id") or fallback_run_id or "").strip()
    overnight_captured_at = (
        overnight_payload.get("run_finished_at_utc")
        or overnight_payload.get("run_started_at_utc")
        or fallback_captured_at
    )
    paper_live_status = str(overnight_payload.get("paper_live_status") or "").strip() or None
    paper_live_family_scorecards = overnight_payload.get("paper_live_family_scorecards")
    if isinstance(paper_live_family_scorecards, list):
        for item in paper_live_family_scorecards[:100]:
            if not isinstance(item, dict):
                continue
            family = str(item.get("family") or "").strip().lower()
            if not family:
                continue
            scorecard_key_seed = overnight_run_id or "unknown_run"
            enriched_payload = dict(item)
            if overnight_run_id and str(enriched_payload.get("run_id") or "").strip() == "":
                enriched_payload["run_id"] = overnight_run_id
            if (
                fallback_sizing_basis is not None
                and str(enriched_payload.get("sizing_basis") or "").strip() == ""
            ):
                enriched_payload["sizing_basis"] = fallback_sizing_basis
            if (
                fallback_execution_basis is not None
                and str(enriched_payload.get("execution_basis") or "").strip() == ""
            ):
                enriched_payload["execution_basis"] = fallback_execution_basis
            headline_value = enriched_payload.get("markout_300s_mean_dollars")
            if headline_value is None:
                headline_value = enriched_payload.get("expected_vs_realized_delta_dollars")
            rows.append(
                {
                    "scorecard_key": f"paper_live_family::{scorecard_key_seed}::{family}",
                    "captured_at": overnight_captured_at,
                    "scorecard_type": "paper_live_family",
                    "status": paper_live_status,
                    "headline_metric_name": "markout_300s_mean_dollars",
                    "headline_metric_value": headline_value,
                    "headline_metric_unit": "dollars",
                    "payload_json": enriched_payload,
                    "source_file": str(outputs_dir / "overnight_alpha_latest.json"),
                }
            )

    paper_live_ticker_scorecards = overnight_payload.get("paper_live_ticker_scorecards")
    if isinstance(paper_live_ticker_scorecards, list):
        for item in paper_live_ticker_scorecards[:200]:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            scorecard_key_seed = overnight_run_id or "unknown_run"
            enriched_payload = dict(item)
            if overnight_run_id and str(enriched_payload.get("run_id") or "").strip() == "":
                enriched_payload["run_id"] = overnight_run_id
            if (
                fallback_sizing_basis is not None
                and str(enriched_payload.get("sizing_basis") or "").strip() == ""
            ):
                enriched_payload["sizing_basis"] = fallback_sizing_basis
            if (
                fallback_execution_basis is not None
                and str(enriched_payload.get("execution_basis") or "").strip() == ""
            ):
                enriched_payload["execution_basis"] = fallback_execution_basis
            headline_value = enriched_payload.get("markout_300s_mean_dollars")
            if headline_value is None:
                headline_value = enriched_payload.get("expected_vs_realized_delta_dollars")
            rows.append(
                {
                    "scorecard_key": f"paper_live_ticker::{scorecard_key_seed}::{ticker}",
                    "captured_at": overnight_captured_at,
                    "scorecard_type": "paper_live_ticker",
                    "status": paper_live_status,
                    "headline_metric_name": "markout_300s_mean_dollars",
                    "headline_metric_value": headline_value,
                    "headline_metric_unit": "dollars",
                    "payload_json": enriched_payload,
                    "source_file": str(outputs_dir / "overnight_alpha_latest.json"),
                }
            )

    rows = [row for row in rows if row.get("captured_at")]
    rows.sort(key=lambda item: str(item.get("captured_at") or ""))
    return rows


def _extract_db_sync_scorecard_row(*, summary_path: Path) -> list[dict[str, Any]]:
    payload = _read_json_file(summary_path)
    if payload is None:
        return []
    run_ts = str(payload.get("run_ts_utc") or "").strip()
    if not run_ts:
        return []
    replication_state = str(payload.get("replication_state") or "unknown").strip() or "unknown"
    run_dir = str(payload.get("run_dir") or "").strip()
    row_key_seed = run_ts.replace(":", "_")
    return [
        {
            "scorecard_key": f"paper_live_db_sync::{row_key_seed}",
            "captured_at": run_ts,
            "scorecard_type": "paper_live_db_sync",
            "status": replication_state,
            "headline_metric_name": "hourly_rc",
            "headline_metric_value": _to_float(payload.get("hourly_rc")),
            "headline_metric_unit": "rc",
            "payload_json": payload,
            "source_file": run_dir or str(summary_path),
        }
    ]


def _upsert_table(
    *,
    table: str,
    rows: list[dict[str, Any]],
    on_conflict: str,
    client: SupabaseRestClient | None,
    dry_run: bool,
    batch_size: int,
) -> int:
    total = len(rows)
    if total == 0:
        print(f"[skip] {table}: no rows")
        return 0
    if dry_run:
        print(f"[dry-run] {table}: {total} rows")
        return total

    if client is None:
        raise RuntimeError(f"Supabase client is not configured for table '{table}'.")

    uploaded = 0
    for chunk in _chunked(rows, batch_size):
        client.upsert_rows(table=table, rows=chunk, on_conflict=on_conflict)
        uploaded += len(chunk)
    print(f"[ok] {table}: upserted {uploaded} rows")
    return uploaded


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest bot outputs into separate Supabase project")
    parser.add_argument("--outputs-dir", default="outputs", help="Path to local outputs directory")
    parser.add_argument("--dry-run", action="store_true", help="Print counts without network writes")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows per upsert request")
    parser.add_argument("--timeout-seconds", type=float, default=20.0, help="HTTP timeout per request")
    parser.add_argument(
        "--network-retries",
        type=int,
        default=int(os.getenv("OPSBOT_SUPABASE_NETWORK_RETRIES", "5")),
        help="Retries per upsert on transient HTTP/network errors",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=float(os.getenv("OPSBOT_SUPABASE_RETRY_BACKOFF_SECONDS", "2.0")),
        help="Base exponential backoff seconds for transient retries",
    )
    parser.add_argument(
        "--max-retry-backoff-seconds",
        type=float,
        default=float(os.getenv("OPSBOT_SUPABASE_MAX_RETRY_BACKOFF_SECONDS", "30.0")),
        help="Upper bound for retry backoff sleep seconds",
    )
    parser.add_argument("--max-execution-events", type=int, default=5000)
    parser.add_argument("--max-frontier-reports", type=int, default=200)
    parser.add_argument("--max-climate-events", type=int, default=5000)
    parser.add_argument("--max-scorecards", type=int, default=200)
    parser.add_argument(
        "--db-sync-summary-json",
        default=os.getenv("PAPER_LIVE_SYNC_LATEST_SUMMARY_PATH", "/tmp/paper_live_chain_db_sync_latest.json"),
        help="Path to paper-live DB sync summary JSON to ingest as a scorecard row when present",
    )
    parser.add_argument("--supabase-url", default=os.getenv("OPSBOT_SUPABASE_URL", ""))
    parser.add_argument(
        "--service-role-key",
        default=os.getenv("OPSBOT_SUPABASE_SERVICE_ROLE_KEY", ""),
    )
    parser.add_argument(
        "--project-ref",
        default=os.getenv("OPSBOT_SUPABASE_PROJECT_REF", ""),
    )
    parser.add_argument(
        "--forbidden-hint",
        default=os.getenv("OPSBOT_FORBIDDEN_PROJECT_HINT", "zenith"),
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    outputs_dir = Path(args.outputs_dir)
    if not outputs_dir.exists():
        print(f"Missing outputs directory: {outputs_dir}", file=sys.stderr)
        return 2

    try:
        _assert_non_zenith(
            supabase_url=args.supabase_url,
            project_ref=args.project_ref,
            forbidden_hint=args.forbidden_hint,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    client: SupabaseRestClient | None = None
    if not args.dry_run:
        if not args.supabase_url or not args.service_role_key:
            print(
                "Missing Supabase credentials. Set OPSBOT_SUPABASE_URL and OPSBOT_SUPABASE_SERVICE_ROLE_KEY, "
                "or use --dry-run.",
                file=sys.stderr,
            )
            return 2
        client = SupabaseRestClient(
            base_url=args.supabase_url,
            api_key=args.service_role_key,
            timeout_seconds=args.timeout_seconds,
            network_retries=args.network_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
            max_retry_backoff_seconds=args.max_retry_backoff_seconds,
        )

    execution_rows = _extract_execution_journal_rows(
        outputs_dir=outputs_dir,
        limit=args.max_execution_events,
    )
    frontier_report_rows, frontier_bucket_rows = _extract_frontier_rows(
        outputs_dir=outputs_dir,
        limit=args.max_frontier_reports,
    )
    climate_rows = _extract_climate_rows(outputs_dir=outputs_dir, limit=args.max_climate_events)
    overnight_rows = _extract_overnight_run_row(outputs_dir=outputs_dir)
    scorecard_rows = _extract_pilot_scorecard_rows(outputs_dir=outputs_dir, limit=args.max_scorecards)
    db_sync_scorecard_rows = _extract_db_sync_scorecard_row(summary_path=Path(args.db_sync_summary_json))
    if db_sync_scorecard_rows:
        scorecard_rows.extend(db_sync_scorecard_rows)

    totals: dict[str, int] = {}

    totals["execution_journal"] = _upsert_table(
        table="execution_journal",
        rows=execution_rows,
        on_conflict="external_event_key",
        client=client,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )
    totals["execution_frontier_reports"] = _upsert_table(
        table="execution_frontier_reports",
        rows=frontier_report_rows,
        on_conflict="run_id",
        client=client,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )
    totals["execution_frontier_report_buckets"] = _upsert_table(
        table="execution_frontier_report_buckets",
        rows=frontier_bucket_rows,
        on_conflict="frontier_run_id,bucket",
        client=client,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )
    totals["climate_availability_events"] = _upsert_table(
        table="climate_availability_events",
        rows=climate_rows,
        on_conflict="external_event_key",
        client=client,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )
    totals["overnight_runs"] = _upsert_table(
        table="overnight_runs",
        rows=overnight_rows,
        on_conflict="run_id",
        client=client,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )
    totals["pilot_scorecards"] = _upsert_table(
        table="pilot_scorecards",
        rows=scorecard_rows,
        on_conflict="scorecard_key",
        client=client,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )

    grand_total = sum(totals.values())
    print("\nIngestion summary:")
    for table, count in totals.items():
        print(f"- {table}: {count}")
    print(f"- total rows processed: {grand_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
