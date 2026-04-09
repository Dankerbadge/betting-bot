#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "outputs"
SYNC_SUMMARY_PATH = Path("/tmp/paper_live_chain_db_sync_latest.json")
OVERNIGHT_PATH = OUTPUTS / "overnight_alpha_latest.json"
PILOT_PATH = OUTPUTS / "pilot_execution_evidence_latest.json"
JOURNAL_PATH = OUTPUTS / "kalshi_execution_journal.sqlite3"


@dataclass
class JsonLoad:
    path: Path
    exists: bool
    size_bytes: int
    parsed: bool
    payload: dict[str, Any] | None
    error: str | None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> JsonLoad:
    if not path.exists():
        return JsonLoad(path=path, exists=False, size_bytes=0, parsed=False, payload=None, error="missing")
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as exc:  # pragma: no cover - defensive
        return JsonLoad(path=path, exists=True, size_bytes=path.stat().st_size, parsed=False, payload=None, error=str(exc))
    try:
        payload = json.loads(text)
    except Exception as exc:
        return JsonLoad(path=path, exists=True, size_bytes=path.stat().st_size, parsed=False, payload=None, error=f"json_parse_failed:{exc}")
    if not isinstance(payload, dict):
        return JsonLoad(path=path, exists=True, size_bytes=path.stat().st_size, parsed=False, payload=None, error="json_not_object")
    return JsonLoad(path=path, exists=True, size_bytes=path.stat().st_size, parsed=True, payload=payload, error=None)


def _first_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _extract_family_value(mapping: Any, family: str) -> Any:
    if isinstance(mapping, dict):
        if family in mapping:
            return mapping.get(family)
        for _, value in mapping.items():
            return value
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _collect_metrics(sync_payload: dict[str, Any] | None, overnight: dict[str, Any] | None, pilot: dict[str, Any] | None) -> dict[str, Any]:
    sync_payload = sync_payload or {}
    overnight = overnight or {}
    pilot = pilot or {}
    local_snapshot = sync_payload.get("local_snapshot") if isinstance(sync_payload.get("local_snapshot"), dict) else {}

    family = "monthly_climate_anomaly"
    attempts = _first_non_null(
        _to_int(local_snapshot.get("paper_live_order_attempts")),
        _to_int(overnight.get("paper_live_order_attempts")),
        _to_int(pilot.get("paper_live_order_attempts")),
    )
    fills = _first_non_null(
        _to_int(local_snapshot.get("paper_live_orders_filled")),
        _to_int(overnight.get("paper_live_orders_filled")),
        _to_int(pilot.get("paper_live_orders_filled")),
    )
    canceled = _first_non_null(
        _to_int(local_snapshot.get("paper_live_orders_canceled")),
        _to_int(overnight.get("paper_live_orders_canceled")),
        _to_int(pilot.get("paper_live_orders_canceled")),
    )

    fill_rate = None
    cancel_rate = None
    if isinstance(attempts, int) and attempts > 0:
        if isinstance(fills, int):
            fill_rate = fills / attempts
        if isinstance(canceled, int):
            cancel_rate = canceled / attempts

    execution_state = _first_non_null(
        local_snapshot.get("paper_live_execution_state"),
        _extract_family_value(local_snapshot.get("paper_live_family_execution_state"), family),
        overnight.get("paper_live_execution_state"),
        pilot.get("core_state"),
    )

    metrics = {
        "run_id": local_snapshot.get("run_id"),
        "attempts": attempts,
        "fills": fills,
        "fill_rate": fill_rate,
        "cancel_rate": cancel_rate,
        "markout_10s_dollars": _first_non_null(
            _to_float(local_snapshot.get("paper_live_markout_10s_dollars")),
            _to_float(overnight.get("paper_live_markout_10s_dollars")),
            _to_float(pilot.get("paper_live_markout_10s_dollars")),
        ),
        "markout_60s_dollars": _first_non_null(
            _to_float(local_snapshot.get("paper_live_markout_60s_dollars")),
            _to_float(overnight.get("paper_live_markout_60s_dollars")),
            _to_float(pilot.get("paper_live_markout_60s_dollars")),
        ),
        "markout_300s_dollars": _first_non_null(
            _to_float(local_snapshot.get("paper_live_markout_300s_dollars")),
            _to_float(overnight.get("paper_live_markout_300s_dollars")),
            _to_float(pilot.get("paper_live_markout_300s_dollars")),
        ),
        "markout_300s_per_risk_pct": _extract_family_value(
            local_snapshot.get("paper_live_family_markout_300s_per_risk_pct"), family
        ),
        "markout_300s_per_contract": _extract_family_value(
            local_snapshot.get("paper_live_family_markout_300s_per_contract"), family
        ),
        "mtm_per_risk_pct": _to_float(overnight.get("paper_live_mtm_per_risk_pct")),
        "realized_settlement_pnl": _first_non_null(
            _to_float(overnight.get("paper_live_realized_settlement_pnl")),
            _to_float(pilot.get("paper_live_settlement_pnl_dollars")),
        ),
        "expected_vs_realized_delta": _first_non_null(
            _to_float(overnight.get("paper_live_expected_vs_realized_delta")),
            _to_float(pilot.get("paper_live_expected_vs_realized_delta")),
        ),
        "execution_state": execution_state,
    }
    return metrics


def _journal_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "exists": False,
            "table_execution_events": False,
            "rows_total": 0,
            "event_type_counts": {},
            "order_terminal_rows": 0,
            "rows_last_7d": 0,
        }

    summary: dict[str, Any] = {
        "exists": True,
        "table_execution_events": False,
        "rows_total": 0,
        "event_type_counts": {},
        "order_terminal_rows": 0,
        "rows_last_7d": 0,
        "error": None,
    }

    try:
        with sqlite3.connect(path) as conn:
            conn.row_factory = sqlite3.Row
            table_row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='execution_events'"
            ).fetchone()
            if table_row is None:
                return summary
            summary["table_execution_events"] = True

            summary["rows_total"] = int(
                conn.execute("SELECT COUNT(*) FROM execution_events").fetchone()[0]
            )
            summary["order_terminal_rows"] = int(
                conn.execute(
                    "SELECT COUNT(*) FROM execution_events WHERE event_type='order_terminal'"
                ).fetchone()[0]
            )
            summary["rows_last_7d"] = int(
                conn.execute(
                    "SELECT COUNT(*) FROM execution_events WHERE captured_at_utc >= datetime('now','-7 days')"
                ).fetchone()[0]
            )

            counts: dict[str, int] = {}
            for row in conn.execute(
                "SELECT event_type, COUNT(*) AS n FROM execution_events GROUP BY event_type ORDER BY n DESC"
            ):
                counts[str(row["event_type"])] = int(row["n"])
            summary["event_type_counts"] = counts
    except Exception as exc:  # pragma: no cover - defensive
        summary["error"] = str(exc)

    return summary


def main() -> int:
    sync = _load_json(SYNC_SUMMARY_PATH)
    overnight = _load_json(OVERNIGHT_PATH)
    pilot = _load_json(PILOT_PATH)
    journal = _journal_summary(JOURNAL_PATH)

    metrics = _collect_metrics(sync.payload, overnight.payload, pilot.payload)

    required_metric_keys = (
        "attempts",
        "fills",
        "markout_10s_dollars",
        "markout_60s_dollars",
        "markout_300s_dollars",
    )
    missing_metrics = [key for key in required_metric_keys if metrics.get(key) is None]

    core_json_ready = all(record.parsed for record in (overnight, pilot))
    sync_ready = sync.parsed
    journal_ready = bool(journal.get("table_execution_events")) and int(journal.get("rows_total") or 0) > 0

    status = "ready"
    blockers: list[str] = []
    if not core_json_ready:
        status = "blocked"
        blockers.append("missing_or_invalid_core_outputs")
    if not sync_ready:
        status = "blocked"
        blockers.append("missing_or_invalid_sync_summary")
    if not journal_ready:
        status = "blocked"
        blockers.append("execution_journal_unavailable")
    if missing_metrics:
        status = "degraded" if status == "ready" else status

    summary = {
        "checked_at_utc": _now_iso(),
        "status": status,
        "blockers": blockers,
        "missing_metrics": missing_metrics,
        "files": {
            "sync_summary": {
                "path": str(sync.path),
                "exists": sync.exists,
                "size_bytes": sync.size_bytes,
                "parsed": sync.parsed,
                "error": sync.error,
            },
            "overnight_latest": {
                "path": str(overnight.path),
                "exists": overnight.exists,
                "size_bytes": overnight.size_bytes,
                "parsed": overnight.parsed,
                "error": overnight.error,
            },
            "pilot_execution_evidence_latest": {
                "path": str(pilot.path),
                "exists": pilot.exists,
                "size_bytes": pilot.size_bytes,
                "parsed": pilot.parsed,
                "error": pilot.error,
            },
            "execution_journal": {
                "path": str(JOURNAL_PATH),
                "exists": JOURNAL_PATH.exists(),
                "size_bytes": JOURNAL_PATH.stat().st_size if JOURNAL_PATH.exists() else 0,
            },
        },
        "journal": journal,
        "metrics_snapshot": metrics,
    }

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if status == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
