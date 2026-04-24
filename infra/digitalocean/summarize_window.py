#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
import pickle
import re
import sqlite3
import subprocess
import time
from collections import Counter, defaultdict
from pathlib import Path

TRIAL_BALANCE_CACHE_VERSION = 2
WINDOW_SUMMARY_CACHE_VERSION = 1
PROFITABILITY_CSV_CACHE_VERSION = 2


def _load(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _write_text_atomic(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}"
    tmp_path = path.with_name(tmp_name)
    try:
        with tmp_path.open("w", encoding=encoding) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_json_number(value):
    parsed = _parse_float(value)
    if isinstance(parsed, float):
        return float(round(parsed, 6))
    return None


def _cache_path_key(path: Path) -> str:
    """Stable cache key for paths without expensive symlink resolution."""
    try:
        if path.is_absolute():
            return str(path)
        return str(path.absolute())
    except OSError:
        return str(path)


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _parse_int(value):
    parsed = _parse_float(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except (TypeError, ValueError):
        return None


def _normalize_text(value):
    return str(value or "").strip()


def _parse_contract_count(row: dict) -> int:
    for key in (
        "contracts_per_order",
        "contracts",
        "contract_count",
        "order_count",
        "count",
    ):
        parsed = _parse_int(row.get(key))
        if isinstance(parsed, int) and parsed > 0:
            return int(parsed)
    return 1


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _find_repo_root(start: Path) -> Path | None:
    current = start.resolve()
    for candidate in [current, *current.parents]:
        if (candidate / ".git").exists():
            return candidate
    return None


def _git_provenance(out_dir: Path) -> dict:
    repo_root = _find_repo_root(out_dir)
    if repo_root is None:
        return {
            "repo_root": "",
            "baseline_commit": "",
            "baseline_commit_short": "",
            "git_dirty": None,
            "error": "repo_root_not_found",
        }
    try:
        commit = (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=str(repo_root),
                text=True,
                stderr=subprocess.DEVNULL,
            )
            .strip()
        )
    except Exception:
        commit = ""
    try:
        short = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(repo_root),
                text=True,
                stderr=subprocess.DEVNULL,
            )
            .strip()
        )
    except Exception:
        short = ""
    try:
        dirty_output = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=str(repo_root),
            text=True,
            stderr=subprocess.DEVNULL,
        )
        git_dirty = bool(_normalize_text(dirty_output))
    except Exception:
        git_dirty = None
    return {
        "repo_root": str(repo_root),
        "baseline_commit": commit,
        "baseline_commit_short": short,
        "git_dirty": git_dirty,
    }


def _policy_provenance(
    *,
    intents_files: list[Path],
    out_dir: Path,
    intents_metrics: list[dict] | None = None,
) -> dict:
    policy_paths_seen: list[str] = []
    latest_policy_path = ""
    latest_policy_loaded = None
    latest_policy_version = ""
    if intents_metrics:
        metrics_iterable = [item if isinstance(item, dict) else {} for item in intents_metrics]
    else:
        metrics_iterable = []
        for path in intents_files:
            payload = _load(path)
            if isinstance(payload, dict):
                metrics_iterable.append(payload)
    for payload in metrics_iterable:
        policy_path = _normalize_text(payload.get("metar_age_policy_json") or payload.get("policy_path"))
        if policy_path:
            policy_paths_seen.append(policy_path)
            latest_policy_path = policy_path
        if "metar_age_policy_loaded" in payload or "policy_loaded" in payload:
            raw_loaded = payload.get("metar_age_policy_loaded")
            if raw_loaded is None:
                raw_loaded = payload.get("policy_loaded")
            latest_policy_loaded = _parse_bool(raw_loaded)
        policy_version = _normalize_text(payload.get("policy_version"))
        if policy_version:
            latest_policy_version = policy_version

    resolved: list[dict] = []
    for raw in sorted(set(policy_paths_seen)):
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = (out_dir / raw).resolve()
        entry = {
            "path": raw,
            "resolved_path": str(candidate),
            "exists": candidate.exists(),
            "file_name": candidate.name,
            "sha256": "",
        }
        if candidate.exists():
            try:
                entry["sha256"] = _sha256_file(candidate)
            except OSError:
                entry["sha256"] = ""
        resolved.append(entry)

    active = {}
    if latest_policy_path:
        for item in resolved:
            if item.get("path") == latest_policy_path:
                active = dict(item)
                break
        if not active:
            candidate = Path(latest_policy_path)
            if not candidate.is_absolute():
                candidate = (out_dir / latest_policy_path).resolve()
            active = {
                "path": latest_policy_path,
                "resolved_path": str(candidate),
                "exists": candidate.exists(),
                "file_name": candidate.name,
                "sha256": _sha256_file(candidate) if candidate.exists() else "",
            }

    return {
        "policy_version": latest_policy_version,
        "active_policy": active,
        "active_policy_loaded": latest_policy_loaded,
        "policy_paths_seen": resolved,
    }


def _run_provenance(
    *,
    intents_files: list[Path],
    shadow_files: list[Path],
    micro_execute_files: list[Path],
) -> dict:
    latest_intents = _load(intents_files[-1]) if intents_files else {}
    latest_shadow = _load(shadow_files[-1]) if shadow_files else {}
    latest_execute = _load(micro_execute_files[-1]) if micro_execute_files else {}
    return {
        "latest_intents_summary_file": intents_files[-1].name if intents_files else "",
        "latest_shadow_summary_file": shadow_files[-1].name if shadow_files else "",
        "latest_micro_execute_summary_file": micro_execute_files[-1].name if micro_execute_files else "",
        "run_id": _normalize_text(latest_execute.get("run_id")),
        "execution_journal_run_id": _normalize_text(latest_execute.get("execution_journal_run_id")),
        "execution_journal_db_path": _normalize_text(latest_execute.get("execution_journal_db_path")),
        "intents_status": _normalize_text(latest_intents.get("status")),
        "shadow_status": _normalize_text(latest_shadow.get("status")),
        "micro_execute_status": _normalize_text(latest_execute.get("status")),
    }


def _extract_window_tag(label: str) -> str:
    text = _normalize_text(label).lower()
    match = re.search(r"(\d+h)", text)
    if match:
        return match.group(1)
    sanitized = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return sanitized or "window"


def _extract_timestamp_token(output_name: str) -> str:
    match = re.search(r"(\d{8}_\d{6})", _normalize_text(output_name))
    if match:
        return match.group(1)
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def _parse_ts(value) -> datetime | None:
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


def _file_captured_at_utc(path: Path) -> datetime:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    except OSError:
        return datetime.fromtimestamp(0, timezone.utc)


def _extract_row_metric(row: dict[str, str], keys: tuple[str, ...]) -> float:
    for key in keys:
        parsed = _parse_float(row.get(key))
        if isinstance(parsed, float):
            return parsed
    return 0.0


def _extract_row_metric_with_field(row: dict[str, str], keys: tuple[str, ...]) -> tuple[float, str]:
    for key in keys:
        parsed = _parse_float(row.get(key))
        if isinstance(parsed, float):
            return parsed, key
    return 0.0, ""


def _extract_first_text(row: dict[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = _normalize_text(row.get(key))
        if value:
            return value
    return ""


def _extract_first_float_with_field(row: dict[str, str], keys: tuple[str, ...]) -> tuple[float | None, str]:
    for key in keys:
        parsed = _parse_float(row.get(key))
        if isinstance(parsed, float):
            return parsed, key
    return None, ""


def _trial_balance_empty_cache(reset_epoch: float) -> dict:
    return {
        "version": TRIAL_BALANCE_CACHE_VERSION,
        "reset_epoch": float(reset_epoch),
        "parsed_files": {},
        "planned_rows_total": 0,
        "occurrence_counts": {},
        "canonical_rows": {},
    }


def _load_trial_balance_cache(cache_file: Path, reset_epoch: float) -> tuple[dict, str]:
    payload = _load(cache_file)
    if not isinstance(payload, dict) or not payload:
        return _trial_balance_empty_cache(reset_epoch), "cold_start"
    cached_version = _parse_int(payload.get("version"))
    if cached_version != TRIAL_BALANCE_CACHE_VERSION:
        return _trial_balance_empty_cache(reset_epoch), "cache_version_mismatch"
    cached_reset_epoch = _parse_float(payload.get("reset_epoch"))
    if not isinstance(cached_reset_epoch, float) or abs(cached_reset_epoch - float(reset_epoch)) > 1e-6:
        return _trial_balance_empty_cache(reset_epoch), "reset_epoch_changed"
    parsed_files = payload.get("parsed_files") if isinstance(payload.get("parsed_files"), dict) else {}
    occurrence_counts = payload.get("occurrence_counts") if isinstance(payload.get("occurrence_counts"), dict) else {}
    canonical_rows = payload.get("canonical_rows") if isinstance(payload.get("canonical_rows"), dict) else {}
    planned_rows_total = _parse_int(payload.get("planned_rows_total"))
    if planned_rows_total is None or planned_rows_total < 0:
        planned_rows_total = 0
    normalized_cache = {
        "version": TRIAL_BALANCE_CACHE_VERSION,
        "reset_epoch": float(reset_epoch),
        "parsed_files": dict(parsed_files),
        "planned_rows_total": int(planned_rows_total),
        "occurrence_counts": dict(occurrence_counts),
        "canonical_rows": dict(canonical_rows),
    }
    return normalized_cache, "warm_start"


def _parse_temperature_plan_rows(path: Path, captured_at_dt: datetime) -> list[dict]:
    rows: list[dict] = []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row_index, row in enumerate(reader, start=1):
                if _normalize_text(row.get("source_strategy")).lower() != "temperature_constraints":
                    continue
                intent_id = _normalize_text(row.get("temperature_intent_id"))
                client_order_id = _normalize_text(
                    row.get("temperature_client_order_id") or row.get("client_order_id")
                )
                shadow_order_id = client_order_id or (
                    f"missing_trial_shadow_order_id:{path.name}:{row_index}:"
                    f"{intent_id}:{_normalize_text(row.get('market_ticker'))}:{_normalize_text(row.get('side'))}"
                )
                rows.append(
                    {
                        "intent_id": intent_id,
                        "client_order_id": client_order_id,
                        "shadow_order_id": shadow_order_id,
                        "market_ticker": _normalize_text(row.get("market_ticker")),
                        "underlying_key": _normalize_text(row.get("temperature_underlying_key")),
                        "side": _normalize_text(row.get("side")).lower(),
                        "entry_price_dollars": _parse_float(row.get("maker_entry_price_dollars")),
                        "contracts_count": _parse_contract_count(row),
                        "captured_at_epoch": float(captured_at_dt.timestamp()),
                    }
                )
    except OSError:
        return []
    return rows


def _window_summary_empty_cache() -> dict:
    return {
        "version": WINDOW_SUMMARY_CACHE_VERSION,
        "files": {},
    }


def _load_window_summary_cache(cache_file: Path) -> tuple[dict, str]:
    payload = _load(cache_file)
    if not isinstance(payload, dict) or not payload:
        return _window_summary_empty_cache(), "cold_start"
    version = _parse_int(payload.get("version"))
    if version != WINDOW_SUMMARY_CACHE_VERSION:
        return _window_summary_empty_cache(), "cache_version_mismatch"
    files = payload.get("files") if isinstance(payload.get("files"), dict) else {}
    return {
        "version": WINDOW_SUMMARY_CACHE_VERSION,
        "files": dict(files),
    }, "warm_start"


def _extract_window_summary_metrics(kind: str, payload: dict) -> dict:
    if kind == "intents_summary":
        return {
            "intents_total": int(payload.get("intents_total") or 0),
            "intents_approved": int(payload.get("intents_approved") or 0),
            "intents_revalidated": int(payload.get("intents_revalidated") or 0),
            "revalidation_invalidated": int(payload.get("revalidation_invalidated") or 0),
            "status": _normalize_text(payload.get("status") or "unknown"),
            "settlement_state_loaded": _parse_bool(payload.get("settlement_state_loaded")),
            "policy_reason_counts": {
                str(key): int(value or 0)
                for key, value in (payload.get("policy_reason_counts") or {}).items()
            },
            "policy_path": _normalize_text(payload.get("metar_age_policy_json")),
            "policy_loaded": (
                _parse_bool(payload.get("metar_age_policy_loaded"))
                if "metar_age_policy_loaded" in payload
                else None
            ),
            "policy_version": _normalize_text(payload.get("policy_version")),
        }
    if kind == "plan_summary":
        return {
            "planned_orders": int(payload.get("planned_orders") or 0),
        }
    if kind == "shadow_summary":
        return {
            "cycle_status_counts": {
                str(key): int(value or 0)
                for key, value in (payload.get("cycle_status_counts") or {}).items()
            },
        }
    return {}


def _slice_bucket():
    return {
        "intents_total": 0,
        "approved_intents": 0,
        "planned_orders": 0,
        "expected_edge_total": 0.0,
        "estimated_entry_cost_total": 0.0,
        "orders_settled": 0,
        "realized_pnl_total": 0.0,
        "wins": 0,
        "losses": 0,
        "pushes": 0,
    }


def _finalize_slice_buckets(raw: dict[str, dict]) -> dict[str, dict]:
    finalized = {}
    for key in sorted(raw.keys(), key=lambda value: (value == "unknown", value)):
        item = dict(raw[key])
        intents_total = int(item.get("intents_total") or 0)
        approved_intents = int(item.get("approved_intents") or 0)
        cost_total = float(item.get("estimated_entry_cost_total") or 0.0)
        expected_total = float(item.get("expected_edge_total") or 0.0)
        settled_orders = int(item.get("orders_settled") or 0)
        realized_total = float(item.get("realized_pnl_total") or 0.0)
        item["approval_rate"] = round((approved_intents / intents_total), 6) if intents_total else 0.0
        item["expected_roi"] = round((expected_total / cost_total), 6) if cost_total > 0 else None
        item["pnl_per_settled_order"] = round((realized_total / settled_orders), 6) if settled_orders > 0 else None
        item["intents_total"] = intents_total
        item["approved_intents"] = approved_intents
        item["planned_orders"] = int(item.get("planned_orders") or 0)
        item["orders_settled"] = settled_orders
        item["wins"] = int(item.get("wins") or 0)
        item["losses"] = int(item.get("losses") or 0)
        item["pushes"] = int(item.get("pushes") or 0)
        item["expected_edge_total"] = round(expected_total, 6)
        item["estimated_entry_cost_total"] = round(cost_total, 6)
        item["realized_pnl_total"] = round(realized_total, 6)
        finalized[key] = item
    return finalized


def _derive_underlying_family(*, underlying_key: str, market_ticker: str) -> str:
    normalized_underlying = _normalize_text(underlying_key).upper()
    if normalized_underlying:
        parts = [token.strip().upper() for token in normalized_underlying.split("|") if token.strip()]
        if len(parts) >= 2:
            return f"{parts[0]}|{parts[1]}"
        return parts[0] if parts else "unknown"

    normalized_ticker = _normalize_text(market_ticker).upper()
    if normalized_ticker:
        return normalized_ticker.split("-", 1)[0] or "unknown"
    return "unknown"


def _settled_slice_bucket() -> dict[str, float | int]:
    return {
        "resolved_count": 0,
        "wins": 0,
        "losses": 0,
        "pushes": 0,
        "pnl_total": 0.0,
        "entry_cost_total": 0.0,
        "win_pnl_total": 0.0,
        "loss_abs_pnl_total": 0.0,
    }


def _finalize_settled_slice_buckets(raw: dict[str, dict]) -> dict[str, dict]:
    finalized = {}
    for key in sorted(raw.keys(), key=lambda value: (value == "unknown", value)):
        item = dict(raw[key])
        resolved_count = int(item.get("resolved_count") or 0)
        wins = int(item.get("wins") or 0)
        losses = int(item.get("losses") or 0)
        pushes = int(item.get("pushes") or 0)
        pnl_total = float(item.get("pnl_total") or 0.0)
        entry_cost_total = float(item.get("entry_cost_total") or 0.0)
        win_pnl_total = float(item.get("win_pnl_total") or 0.0)
        loss_abs_pnl_total = float(item.get("loss_abs_pnl_total") or 0.0)
        item["resolved_count"] = resolved_count
        item["wins"] = wins
        item["losses"] = losses
        item["pushes"] = pushes
        item["pnl_total"] = round(pnl_total, 6)
        item["entry_cost_total"] = round(entry_cost_total, 6)
        item["win_rate"] = round((wins / resolved_count), 6) if resolved_count > 0 else None
        item["avg_pnl_per_resolved"] = round((pnl_total / resolved_count), 6) if resolved_count > 0 else None
        item["avg_win"] = round((win_pnl_total / wins), 6) if wins > 0 else None
        item["avg_loss_abs"] = round((loss_abs_pnl_total / losses), 6) if losses > 0 else None
        item["profit_factor"] = (
            round((win_pnl_total / loss_abs_pnl_total), 6) if loss_abs_pnl_total > 0 else None
        )
        item["roi_on_entry_cost"] = (
            round((pnl_total / entry_cost_total), 6) if entry_cost_total > 0 else None
        )
        finalized[key] = item
    return finalized


def _top_contributors_by_pnl(
    finalized: dict[str, dict],
    *,
    direction: str,
    top_n: int,
) -> list[dict]:
    if top_n <= 0:
        return []
    rows = []
    for key, item in finalized.items():
        resolved_count = int(_parse_int(item.get("resolved_count")) or 0)
        if resolved_count <= 0:
            continue
        pnl_total = float(_parse_float(item.get("pnl_total")) or 0.0)
        rows.append(
            {
                "key": key,
                "resolved_count": resolved_count,
                "wins": int(_parse_int(item.get("wins")) or 0),
                "losses": int(_parse_int(item.get("losses")) or 0),
                "pushes": int(_parse_int(item.get("pushes")) or 0),
                "win_rate": _to_json_number(item.get("win_rate")),
                "pnl_total": round(pnl_total, 6),
                "avg_pnl_per_resolved": _to_json_number(item.get("avg_pnl_per_resolved")),
                "profit_factor": _to_json_number(item.get("profit_factor")),
                "roi_on_entry_cost": _to_json_number(item.get("roi_on_entry_cost")),
            }
        )
    reverse = direction == "best"
    rows.sort(key=lambda row: float(row.get("pnl_total") or 0.0), reverse=reverse)
    return rows[:top_n]


def _is_temperature_event(event: dict, submitted_keys: set[str]) -> bool:
    client_order_id = _normalize_text(event.get("client_order_id"))
    market_ticker = _normalize_text(event.get("market_ticker")).upper()
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    source_strategy = _normalize_text(payload.get("source_strategy")).lower()
    if source_strategy == "temperature_constraints":
        return True
    if client_order_id.startswith("temp-"):
        return True
    if market_ticker.startswith("KX"):
        return True
    return _normalize_text(event.get("order_key")) in submitted_keys


def _open_profitability_csv_cache(cache_file: Path) -> sqlite3.Connection | None:
    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(cache_file), timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS parsed_file_cache (
                kind TEXT NOT NULL,
                file_key TEXT NOT NULL,
                mtime REAL NOT NULL,
                size INTEGER NOT NULL,
                version INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                payload_blob BLOB,
                payload_format TEXT NOT NULL DEFAULT 'json',
                updated_at REAL NOT NULL,
                PRIMARY KEY(kind, file_key)
            )
            """
        )
        try:
            info_rows = conn.execute("PRAGMA table_info(parsed_file_cache)").fetchall()
            existing_columns = {str(row[1]) for row in info_rows if len(row) >= 2}
            if "payload_blob" not in existing_columns:
                conn.execute("ALTER TABLE parsed_file_cache ADD COLUMN payload_blob BLOB")
            if "payload_format" not in existing_columns:
                conn.execute("ALTER TABLE parsed_file_cache ADD COLUMN payload_format TEXT NOT NULL DEFAULT 'json'")
        except sqlite3.Error:
            # Keep cache usable in legacy schema mode.
            pass
        return conn
    except sqlite3.Error:
        return None


def _resolve_profitability_csv_cache_file(preferred: Path) -> tuple[Path, str]:
    """Pick a writable cache file path, falling back when preferred is read-only."""
    preferred = Path(preferred)
    preferred_target = preferred if preferred.exists() else preferred.parent
    if os.access(str(preferred_target), os.W_OK):
        return preferred, ""

    suffix = "".join(preferred.suffixes) or ".sqlite3"
    stem = preferred.name
    if suffix and preferred.name.endswith(suffix):
        stem = preferred.name[: -len(suffix)]
    fallback = preferred.with_name(f"{stem}_uid{os.getuid()}{suffix}")
    fallback_target = fallback if fallback.exists() else fallback.parent
    if os.access(str(fallback_target), os.W_OK):
        return fallback, "preferred_not_writable"
    return preferred, "no_writable_cache_path"


def _profitability_csv_cache_get(
    conn: sqlite3.Connection | None,
    *,
    kind: str,
    file_key: str,
    mtime: float,
    size: int,
) -> dict | None:
    if conn is None:
        return None
    try:
        row = conn.execute(
            """
            SELECT mtime, size, version, payload_blob, payload_format, payload_json
            FROM parsed_file_cache
            WHERE kind = ? AND file_key = ?
            """,
            (kind, file_key),
        ).fetchone()
    except sqlite3.Error:
        try:
            row = conn.execute(
                """
                SELECT mtime, size, version, payload_json
                FROM parsed_file_cache
                WHERE kind = ? AND file_key = ?
                """,
                (kind, file_key),
            ).fetchone()
            if not row:
                return None
            cached_mtime, cached_size, cached_version, payload_json = row
            payload_blob = None
            payload_format = "json"
        except sqlite3.Error:
            return None
    else:
        if not row:
            return None
        cached_mtime, cached_size, cached_version, payload_blob, payload_format, payload_json = row
    if not row:
        return None
    try:
        cached_version_int = int(cached_version)
    except (TypeError, ValueError):
        return None
    if cached_version_int != PROFITABILITY_CSV_CACHE_VERSION:
        return None
    try:
        cached_mtime_float = float(cached_mtime)
        cached_size_int = int(cached_size)
    except (TypeError, ValueError):
        return None
    if abs(cached_mtime_float - float(mtime)) > 1e-9:
        return None
    if cached_size_int != int(size):
        return None
    if _normalize_text(payload_format).lower() == "pickle" and payload_blob is not None:
        try:
            payload = pickle.loads(payload_blob)
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    try:
        payload = json.loads(payload_json or "{}")
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _profitability_csv_cache_put(
    conn: sqlite3.Connection | None,
    *,
    kind: str,
    file_key: str,
    mtime: float,
    size: int,
    payload: dict,
) -> tuple[bool, str]:
    if conn is None:
        return False, "cache_disabled"
    try:
        payload_blob = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        return False, "pickle_error"
    payload_json = "{}"
    try:
        conn.execute(
            """
            INSERT INTO parsed_file_cache
                (kind, file_key, mtime, size, version, payload_json, payload_blob, payload_format, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(kind, file_key) DO UPDATE SET
                mtime=excluded.mtime,
                size=excluded.size,
                version=excluded.version,
                payload_json=excluded.payload_json,
                payload_blob=excluded.payload_blob,
                payload_format=excluded.payload_format,
                updated_at=excluded.updated_at
            """,
            (
                kind,
                file_key,
                float(mtime),
                int(size),
                int(PROFITABILITY_CSV_CACHE_VERSION),
                payload_json,
                sqlite3.Binary(payload_blob),
                "pickle",
                float(time.time()),
            ),
        )
        return True, ""
    except sqlite3.Error as exc:
        return False, str(exc)


def _parse_plan_csv_metrics_file(
    *,
    path: Path,
    edge_keys: tuple[str, ...],
    cost_keys: tuple[str, ...],
    plan_policy_version_keys: tuple[str, ...],
) -> dict:
    file_captured_at_dt = _file_captured_at_utc(path)
    planned_orders_total = 0
    expected_edge_total = 0.0
    estimated_entry_cost_total = 0.0
    plan_policy_version_counts: Counter[str] = Counter()
    edge_field_usage: Counter[str] = Counter()
    plan_rows_missing_policy_version = 0
    plan_records: list[dict] = []
    planned_shadow_rows: list[dict] = []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if _normalize_text(row.get("source_strategy")).lower() != "temperature_constraints":
                    continue
                planned_orders_total += 1
                edge, edge_field = _extract_row_metric_with_field(row, edge_keys)
                cost = _extract_row_metric(row, cost_keys)
                if edge_field:
                    edge_field_usage[edge_field] += 1
                else:
                    edge_field_usage["none"] += 1
                expected_edge_total += edge
                estimated_entry_cost_total += cost
                intent_id = _normalize_text(row.get("temperature_intent_id"))
                client_order_id = _normalize_text(row.get("temperature_client_order_id") or row.get("client_order_id"))
                plan_policy_version = _extract_first_text(row, plan_policy_version_keys)
                if plan_policy_version:
                    plan_policy_version_counts[plan_policy_version] += 1
                else:
                    plan_rows_missing_policy_version += 1
                plan_records.append(
                    {
                        "intent_id": intent_id,
                        "client_order_id": client_order_id,
                        "expected_edge": float(edge),
                        "estimated_cost": float(cost),
                        "policy_version": plan_policy_version,
                        "edge_field": edge_field,
                    }
                )
                planned_shadow_rows.append(
                    {
                        "intent_id": intent_id,
                        "client_order_id": client_order_id,
                        "shadow_order_id": client_order_id,
                        "market_ticker": _normalize_text(row.get("market_ticker")),
                        "underlying_key": _normalize_text(row.get("temperature_underlying_key")),
                        "side": _normalize_text(row.get("side")).lower(),
                        "entry_price_dollars": _parse_float(row.get("maker_entry_price_dollars")),
                        "contracts_count": _parse_contract_count(row),
                        "confidence": _parse_float(row.get("confidence")),
                        "policy_version": plan_policy_version,
                        "edge_field": edge_field,
                        "captured_at_epoch": float(file_captured_at_dt.timestamp()),
                        "planned_at_utc": file_captured_at_dt.isoformat(),
                    }
                )
    except OSError:
        pass
    return {
        "planned_orders_total": int(planned_orders_total),
        "expected_edge_total": float(expected_edge_total),
        "estimated_entry_cost_total": float(estimated_entry_cost_total),
        "plan_policy_version_counts": dict(plan_policy_version_counts),
        "plan_rows_missing_policy_version": int(plan_rows_missing_policy_version),
        "edge_field_usage": dict(edge_field_usage),
        "plan_records": plan_records,
        "planned_shadow_rows": planned_shadow_rows,
    }


def _parse_intents_csv_metrics_file(
    *,
    path: Path,
    gate_inputs_config: dict[str, tuple[tuple[str, ...], tuple[str, ...]]],
    gate_operators: dict[str, str],
    intent_policy_version_keys: tuple[str, ...],
) -> dict:
    def _inc_bucket(bucket_map: dict[str, list[int]], key: str, approved: bool) -> None:
        entry = bucket_map.get(key)
        if entry is None:
            entry = [0, 0]
            bucket_map[key] = entry
        entry[0] += 1
        if approved:
            entry[1] += 1

    by_station: dict[str, list[int]] = {}
    by_local_hour: dict[str, list[int]] = {}
    by_signal_type: dict[str, list[int]] = {}
    by_policy_reason: dict[str, list[int]] = {}
    by_underlying_family: dict[str, list[int]] = {}
    intent_context_rows: list[dict] = []
    gate_metrics: dict[str, dict] = {}
    for gate_name, (value_keys, threshold_keys) in gate_inputs_config.items():
        gate_metrics[gate_name] = {
            "operator": _normalize_text(gate_operators.get(gate_name)),
            "value_field_candidates": list(value_keys),
            "threshold_field_candidates": list(threshold_keys),
            "rows_evaluated": 0,
            "rows_with_value": 0,
            "rows_missing_value": 0,
            "rows_with_threshold": 0,
            "rows_missing_threshold": 0,
            "approved_rows_evaluated": 0,
            "approved_rows_with_value": 0,
            "approved_rows_missing_value": 0,
            "approved_rows_with_threshold": 0,
            "approved_rows_missing_threshold": 0,
            "approved_rows_pass": 0,
            "approved_rows_fail": 0,
            "blocked_rows_evaluated": 0,
            "blocked_rows_with_value": 0,
            "blocked_rows_missing_value": 0,
            "blocked_rows_with_threshold": 0,
            "blocked_rows_missing_threshold": 0,
            "blocked_rows_pass": 0,
            "blocked_rows_fail": 0,
            "value_field_usage": {},
            "threshold_field_usage": {},
        }

    approved_rows_total = 0
    blocked_rows_total = 0
    approved_rows_with_no_evaluable_gates = 0
    approved_rows_with_revalidation_conflict = 0
    approved_rows_with_mismatch = 0
    gate_mismatch_by_gate: Counter[str] = Counter()
    gate_mismatch_samples: list[dict] = []
    intent_policy_version_counts: Counter[str] = Counter()
    intent_rows_missing_policy_version = 0
    file_rows_total = 0

    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = set(reader.fieldnames or [])
            file_gate_config: dict[str, dict] = {}
            static_absent_gates: list[str] = []
            for gate_name, (value_keys, threshold_keys) in gate_inputs_config.items():
                value_keys_present = tuple(key for key in value_keys if key in fieldnames)
                threshold_keys_present = tuple(key for key in threshold_keys if key in fieldnames)
                file_gate_config[gate_name] = {
                    "value_keys_present": value_keys_present,
                    "threshold_keys_present": threshold_keys_present,
                }
                if not value_keys_present and not threshold_keys_present:
                    static_absent_gates.append(gate_name)

            for row in reader:
                file_rows_total += 1
                intent_id = _normalize_text(row.get("intent_id"))
                station = _normalize_text(row.get("settlement_station")).upper() or "unknown"
                local_hour = _parse_int(row.get("policy_metar_local_hour"))
                hour_key = str(local_hour) if local_hour is not None and 0 <= local_hour <= 23 else "unknown"
                signal_type = _normalize_text(row.get("constraint_status")).lower() or "unknown"
                policy_reason = _normalize_text(row.get("policy_reason")).lower()
                underlying_family = _derive_underlying_family(
                    underlying_key=_normalize_text(row.get("underlying_key")),
                    market_ticker=_normalize_text(row.get("market_ticker")),
                )
                approved = _parse_bool(row.get("policy_approved"))
                if approved:
                    approved_rows_total += 1
                else:
                    blocked_rows_total += 1
                if approved and not policy_reason:
                    policy_reason = "approved"
                if not policy_reason:
                    policy_reason = "unknown"

                intent_policy_version = _extract_first_text(row, intent_policy_version_keys)
                if intent_policy_version:
                    intent_policy_version_counts[intent_policy_version] += 1
                else:
                    intent_rows_missing_policy_version += 1

                _inc_bucket(by_station, station, approved)
                _inc_bucket(by_local_hour, hour_key, approved)
                _inc_bucket(by_signal_type, signal_type, approved)
                _inc_bucket(by_policy_reason, policy_reason, approved)
                _inc_bucket(by_underlying_family, underlying_family, approved)

                gate_results: dict[str, dict] = {}
                for gate_name, gate_cfg in file_gate_config.items():
                    value_keys_present = gate_cfg.get("value_keys_present") or ()
                    threshold_keys_present = gate_cfg.get("threshold_keys_present") or ()
                    value, value_field = (
                        _extract_first_float_with_field(row, value_keys_present) if value_keys_present else (None, "")
                    )
                    threshold, threshold_field = (
                        _extract_first_float_with_field(row, threshold_keys_present)
                        if threshold_keys_present
                        else (None, "")
                    )
                    gate_metric = gate_metrics[gate_name]
                    if value is None:
                        if value_keys_present:
                            gate_metric["rows_missing_value"] += 1
                            if approved:
                                gate_metric["approved_rows_missing_value"] += 1
                            else:
                                gate_metric["blocked_rows_missing_value"] += 1
                    else:
                        gate_metric["rows_with_value"] += 1
                        if approved:
                            gate_metric["approved_rows_with_value"] += 1
                        else:
                            gate_metric["blocked_rows_with_value"] += 1
                        usage = gate_metric["value_field_usage"]
                        usage_key = value_field or "unknown"
                        usage[usage_key] = int(usage.get(usage_key) or 0) + 1
                    if threshold is None:
                        if threshold_keys_present:
                            gate_metric["rows_missing_threshold"] += 1
                            if approved:
                                gate_metric["approved_rows_missing_threshold"] += 1
                            else:
                                gate_metric["blocked_rows_missing_threshold"] += 1
                    else:
                        gate_metric["rows_with_threshold"] += 1
                        if approved:
                            gate_metric["approved_rows_with_threshold"] += 1
                        else:
                            gate_metric["blocked_rows_with_threshold"] += 1
                        usage = gate_metric["threshold_field_usage"]
                        usage_key = threshold_field or "unknown"
                        usage[usage_key] = int(usage.get(usage_key) or 0) + 1

                    gate_passes = None
                    if isinstance(value, float) and isinstance(threshold, float):
                        gate_metric["rows_evaluated"] += 1
                        operator = _normalize_text(gate_metric.get("operator"))
                        if operator == "<=":
                            gate_passes = value <= threshold
                        else:
                            gate_passes = value >= threshold
                        if approved:
                            gate_metric["approved_rows_evaluated"] += 1
                            if gate_passes:
                                gate_metric["approved_rows_pass"] += 1
                            else:
                                gate_metric["approved_rows_fail"] += 1
                        else:
                            gate_metric["blocked_rows_evaluated"] += 1
                            if gate_passes:
                                gate_metric["blocked_rows_pass"] += 1
                            else:
                                gate_metric["blocked_rows_fail"] += 1

                    gate_results[gate_name] = {
                        "passes": gate_passes,
                        "value": value,
                        "threshold": threshold,
                        "value_field": value_field,
                        "threshold_field": threshold_field,
                    }

                if approved:
                    failed_gates = [name for name, result in gate_results.items() if result.get("passes") is False]
                    evaluable_gate_count = sum(1 for result in gate_results.values() if isinstance(result.get("passes"), bool))
                    if evaluable_gate_count == 0:
                        approved_rows_with_no_evaluable_gates += 1
                    if failed_gates:
                        approved_rows_with_mismatch += 1
                        for gate_name in failed_gates:
                            gate_mismatch_by_gate[gate_name] += 1
                        if len(gate_mismatch_samples) < 20:
                            gate_mismatch_samples.append(
                                {
                                    "intent_id": intent_id,
                                    "market_ticker": _normalize_text(row.get("market_ticker")),
                                    "underlying_key": _normalize_text(row.get("underlying_key")),
                                    "side": _normalize_text(row.get("side")).lower(),
                                    "policy_reason": policy_reason,
                                    "failed_gates": failed_gates,
                                    "gate_details": {
                                        name: {
                                            "value": _to_json_number(result.get("value")),
                                            "threshold": _to_json_number(result.get("threshold")),
                                            "operator": _normalize_text(gate_metrics[name].get("operator")),
                                            "value_field": _normalize_text(result.get("value_field")) or None,
                                            "threshold_field": _normalize_text(result.get("threshold_field")) or None,
                                        }
                                        for name, result in gate_results.items()
                                    },
                                }
                            )
                    revalidation_status = _normalize_text(row.get("revalidation_status")).lower()
                    if revalidation_status in {"blocked", "invalidated", "rejected"}:
                        approved_rows_with_revalidation_conflict += 1

                max_entry_stored = _parse_float(row.get("max_entry_price_dollars")) if approved else None
                intent_context_rows.append(
                    {
                        "intent_id": intent_id,
                        "station": station,
                        "local_hour": hour_key,
                        "signal_type": signal_type,
                        "policy_reason": policy_reason,
                        "underlying_family": underlying_family,
                        "approved": bool(approved),
                        "market_ticker": _normalize_text(row.get("market_ticker")),
                        "underlying_key": _normalize_text(row.get("underlying_key")),
                        "side": _normalize_text(row.get("side")).lower(),
                        "max_entry_price_dollars": _to_json_number(max_entry_stored) if approved else None,
                        "settlement_confidence_score": _to_json_number(_parse_float(row.get("settlement_confidence_score"))),
                        "policy_version": intent_policy_version,
                    }
                )

            for gate_name in static_absent_gates:
                gate_metric = gate_metrics[gate_name]
                gate_metric["rows_missing_value"] += int(file_rows_total)
                gate_metric["rows_missing_threshold"] += int(file_rows_total)
                gate_metric["approved_rows_missing_value"] += int(approved_rows_total)
                gate_metric["approved_rows_missing_threshold"] += int(approved_rows_total)
                gate_metric["blocked_rows_missing_value"] += int(blocked_rows_total)
                gate_metric["blocked_rows_missing_threshold"] += int(blocked_rows_total)
    except OSError:
        pass

    return {
        "approved_rows_total": int(approved_rows_total),
        "blocked_rows_total": int(blocked_rows_total),
        "approved_rows_with_no_evaluable_gates": int(approved_rows_with_no_evaluable_gates),
        "approved_rows_with_revalidation_conflict": int(approved_rows_with_revalidation_conflict),
        "approved_rows_with_mismatch": int(approved_rows_with_mismatch),
        "gate_mismatch_by_gate": dict(gate_mismatch_by_gate),
        "gate_mismatch_samples": gate_mismatch_samples,
        "intent_policy_version_counts": dict(intent_policy_version_counts),
        "intent_rows_missing_policy_version": int(intent_rows_missing_policy_version),
        "gate_metrics": gate_metrics,
        "by_station": by_station,
        "by_local_hour": by_local_hour,
        "by_signal_type": by_signal_type,
        "by_policy_reason": by_policy_reason,
        "by_underlying_family": by_underlying_family,
        "intent_context_rows": intent_context_rows,
    }


def _compute_profitability_payload(
    *,
    out_dir: Path,
    start_epoch: float,
    end_epoch: float,
) -> dict:
    edge_keys = (
        "maker_entry_edge_conservative_net_total",
        "maker_entry_edge_net_total",
        "maker_entry_edge_conservative_net_fees",
        "maker_entry_edge_net_fees",
        "maker_entry_edge_conservative",
        "maker_entry_edge",
    )
    cost_keys = ("estimated_entry_cost_dollars", "cost_dollars")
    plan_policy_version_keys = (
        "temperature_policy_version",
        "policy_version",
        "temperature_model_version",
        "model_version",
    )
    intent_policy_version_keys = (
        "policy_version",
        "temperature_policy_version",
        "temperature_model_version",
        "model_version",
    )
    expected_edge_value_keys = (
        "policy_expected_edge_net",
        "policy_expected_edge",
        "expected_edge_net",
        "expected_edge",
    )
    expected_edge_threshold_keys = (
        "policy_min_expected_edge_net_required",
        "policy_min_expected_edge_required",
        "policy_expected_edge_threshold",
    )
    probability_value_keys = (
        "policy_probability_confidence",
        "probability_confidence",
        "confidence",
    )
    probability_threshold_keys = (
        "policy_min_probability_confidence_required",
        "policy_min_probability_required",
        "policy_probability_threshold",
    )
    alpha_strength_value_keys = (
        "policy_alpha_strength",
        "alpha_strength",
    )
    alpha_strength_threshold_keys = (
        "policy_min_alpha_strength_required",
        "policy_alpha_strength_threshold",
    )

    intents_csv_files = _files_in_window(out_dir, "kalshi_temperature_trade_intents_*.csv", start_epoch, end_epoch)
    plan_csv_files = _files_in_window(out_dir, "kalshi_temperature_trade_plan_*.csv", start_epoch, end_epoch)
    specs_csv_files = _files_in_window(out_dir, "kalshi_temperature_contract_specs_*.csv", 0, end_epoch)
    settlement_state_files = _files_in_window(out_dir, "kalshi_temperature_settlement_state_*.json", 0, end_epoch)
    preferred_csv_parse_cache_file = out_dir / "checkpoints" / "profitability_csv_parse_cache.sqlite3"
    csv_parse_cache_file, csv_cache_path_reason = _resolve_profitability_csv_cache_file(preferred_csv_parse_cache_file)
    cache_write_access_target = csv_parse_cache_file if csv_parse_cache_file.exists() else csv_parse_cache_file.parent
    cache_write_access = os.access(str(cache_write_access_target), os.W_OK)
    csv_parse_cache_conn = _open_profitability_csv_cache(csv_parse_cache_file)
    csv_parse_cache_stats = {
        "enabled": bool(csv_parse_cache_conn),
        "preferred_file": str(preferred_csv_parse_cache_file),
        "write_access": bool(cache_write_access),
        "write_access_target": str(cache_write_access_target),
        "path_fallback_reason": csv_cache_path_reason,
        "plan_hits": 0,
        "plan_misses": 0,
        "intents_hits": 0,
        "intents_misses": 0,
        "puts_ok": 0,
        "puts_failed": 0,
        "puts_failed_readonly": 0,
        "put_error_samples": [],
    }

    plan_by_intent_id: dict[str, list[dict]] = defaultdict(list)
    plan_by_client_order_id: dict[str, dict] = {}
    plan_rows_without_intent: list[dict] = []
    planned_shadow_rows: list[dict] = []
    planned_orders_total = 0
    expected_edge_total = 0.0
    estimated_entry_cost_total = 0.0
    plan_policy_version_counts: Counter[str] = Counter()
    intent_policy_version_counts: Counter[str] = Counter()
    edge_field_usage: Counter[str] = Counter()
    plan_rows_missing_policy_version = 0
    intent_rows_missing_policy_version = 0

    for path in plan_csv_files:
        try:
            stat_result = path.stat()
            file_mtime = float(stat_result.st_mtime)
            file_size = int(stat_result.st_size)
        except OSError:
            continue
        file_key = _cache_path_key(path)
        file_metrics = _profitability_csv_cache_get(
            csv_parse_cache_conn,
            kind="plan_csv_metrics_v1",
            file_key=file_key,
            mtime=file_mtime,
            size=file_size,
        )
        if isinstance(file_metrics, dict):
            csv_parse_cache_stats["plan_hits"] += 1
        else:
            csv_parse_cache_stats["plan_misses"] += 1
            file_metrics = _parse_plan_csv_metrics_file(
                path=path,
                edge_keys=edge_keys,
                cost_keys=cost_keys,
                plan_policy_version_keys=plan_policy_version_keys,
            )
            put_ok, put_err = _profitability_csv_cache_put(
                csv_parse_cache_conn,
                kind="plan_csv_metrics_v1",
                file_key=file_key,
                mtime=file_mtime,
                size=file_size,
                payload=file_metrics,
            )
            if put_ok:
                csv_parse_cache_stats["puts_ok"] += 1
            else:
                csv_parse_cache_stats["puts_failed"] += 1
                if "readonly" in _normalize_text(put_err).lower():
                    csv_parse_cache_stats["puts_failed_readonly"] += 1
                if len(csv_parse_cache_stats["put_error_samples"]) < 5:
                    csv_parse_cache_stats["put_error_samples"].append(
                        {
                            "kind": "plan_csv_metrics_v1",
                            "file_key": file_key,
                            "error": _normalize_text(put_err) or "unknown_error",
                        }
                    )

        planned_orders_total += int(file_metrics.get("planned_orders_total") or 0)
        expected_edge_total += float(file_metrics.get("expected_edge_total") or 0.0)
        estimated_entry_cost_total += float(file_metrics.get("estimated_entry_cost_total") or 0.0)
        plan_rows_missing_policy_version += int(file_metrics.get("plan_rows_missing_policy_version") or 0)
        for key, value in (file_metrics.get("plan_policy_version_counts") or {}).items():
            key_text = _normalize_text(key)
            if key_text:
                plan_policy_version_counts[key_text] += int(_parse_int(value) or 0)
        for key, value in (file_metrics.get("edge_field_usage") or {}).items():
            key_text = _normalize_text(key) or "none"
            edge_field_usage[key_text] += int(_parse_int(value) or 0)
        for plan_record in file_metrics.get("plan_records") or []:
            if not isinstance(plan_record, dict):
                continue
            intent_id = _normalize_text(plan_record.get("intent_id"))
            client_order_id = _normalize_text(plan_record.get("client_order_id"))
            normalized_record = {
                "expected_edge": float(_parse_float(plan_record.get("expected_edge")) or 0.0),
                "estimated_cost": float(_parse_float(plan_record.get("estimated_cost")) or 0.0),
                "intent_id": intent_id,
                "client_order_id": client_order_id,
                "policy_version": _normalize_text(plan_record.get("policy_version")),
                "edge_field": _normalize_text(plan_record.get("edge_field")),
            }
            if intent_id:
                plan_by_intent_id[intent_id].append(normalized_record)
            else:
                plan_rows_without_intent.append(normalized_record)
            if client_order_id:
                plan_by_client_order_id[client_order_id] = normalized_record
        for row in file_metrics.get("planned_shadow_rows") or []:
            if isinstance(row, dict):
                planned_shadow_rows.append(dict(row))

    by_station = defaultdict(_slice_bucket)
    by_local_hour = defaultdict(_slice_bucket)
    by_signal_type = defaultdict(_slice_bucket)
    by_policy_reason = defaultdict(_slice_bucket)
    by_underlying_family = defaultdict(_slice_bucket)
    plan_context_by_client_order_id: dict[str, dict] = {}
    plan_context_by_intent_id: dict[str, dict] = {}
    approved_selection_rows: list[dict] = []
    approved_max_entry_by_intent_id: dict[str, float] = {}
    gate_audit_metrics = {
        "metar_age": {
            "operator": "<=",
            "value_field_candidates": ["metar_observation_age_minutes"],
            "threshold_field_candidates": ["policy_metar_max_age_minutes_applied"],
            "rows_evaluated": 0,
            "rows_with_value": 0,
            "rows_missing_value": 0,
            "rows_with_threshold": 0,
            "rows_missing_threshold": 0,
            "approved_rows_evaluated": 0,
            "approved_rows_with_value": 0,
            "approved_rows_missing_value": 0,
            "approved_rows_with_threshold": 0,
            "approved_rows_missing_threshold": 0,
            "approved_rows_pass": 0,
            "approved_rows_fail": 0,
            "blocked_rows_evaluated": 0,
            "blocked_rows_with_value": 0,
            "blocked_rows_missing_value": 0,
            "blocked_rows_with_threshold": 0,
            "blocked_rows_missing_threshold": 0,
            "blocked_rows_pass": 0,
            "blocked_rows_fail": 0,
            "value_field_usage": Counter(),
            "threshold_field_usage": Counter(),
        },
        "expected_edge": {
            "operator": ">=",
            "value_field_candidates": list(expected_edge_value_keys),
            "threshold_field_candidates": list(expected_edge_threshold_keys),
            "rows_evaluated": 0,
            "rows_with_value": 0,
            "rows_missing_value": 0,
            "rows_with_threshold": 0,
            "rows_missing_threshold": 0,
            "approved_rows_evaluated": 0,
            "approved_rows_with_value": 0,
            "approved_rows_missing_value": 0,
            "approved_rows_with_threshold": 0,
            "approved_rows_missing_threshold": 0,
            "approved_rows_pass": 0,
            "approved_rows_fail": 0,
            "blocked_rows_evaluated": 0,
            "blocked_rows_with_value": 0,
            "blocked_rows_missing_value": 0,
            "blocked_rows_with_threshold": 0,
            "blocked_rows_missing_threshold": 0,
            "blocked_rows_pass": 0,
            "blocked_rows_fail": 0,
            "value_field_usage": Counter(),
            "threshold_field_usage": Counter(),
        },
        "probability_confidence": {
            "operator": ">=",
            "value_field_candidates": list(probability_value_keys),
            "threshold_field_candidates": list(probability_threshold_keys),
            "rows_evaluated": 0,
            "rows_with_value": 0,
            "rows_missing_value": 0,
            "rows_with_threshold": 0,
            "rows_missing_threshold": 0,
            "approved_rows_evaluated": 0,
            "approved_rows_with_value": 0,
            "approved_rows_missing_value": 0,
            "approved_rows_with_threshold": 0,
            "approved_rows_missing_threshold": 0,
            "approved_rows_pass": 0,
            "approved_rows_fail": 0,
            "blocked_rows_evaluated": 0,
            "blocked_rows_with_value": 0,
            "blocked_rows_missing_value": 0,
            "blocked_rows_with_threshold": 0,
            "blocked_rows_missing_threshold": 0,
            "blocked_rows_pass": 0,
            "blocked_rows_fail": 0,
            "value_field_usage": Counter(),
            "threshold_field_usage": Counter(),
        },
        "alpha_strength": {
            "operator": ">=",
            "value_field_candidates": list(alpha_strength_value_keys),
            "threshold_field_candidates": list(alpha_strength_threshold_keys),
            "rows_evaluated": 0,
            "rows_with_value": 0,
            "rows_missing_value": 0,
            "rows_with_threshold": 0,
            "rows_missing_threshold": 0,
            "approved_rows_evaluated": 0,
            "approved_rows_with_value": 0,
            "approved_rows_missing_value": 0,
            "approved_rows_with_threshold": 0,
            "approved_rows_missing_threshold": 0,
            "approved_rows_pass": 0,
            "approved_rows_fail": 0,
            "blocked_rows_evaluated": 0,
            "blocked_rows_with_value": 0,
            "blocked_rows_missing_value": 0,
            "blocked_rows_with_threshold": 0,
            "blocked_rows_missing_threshold": 0,
            "blocked_rows_pass": 0,
            "blocked_rows_fail": 0,
            "value_field_usage": Counter(),
            "threshold_field_usage": Counter(),
        },
    }
    gate_mismatch_samples: list[dict] = []
    gate_mismatch_by_gate: Counter[str] = Counter()
    approved_rows_total = 0
    blocked_rows_total = 0
    approved_rows_with_no_evaluable_gates = 0
    approved_rows_with_revalidation_conflict = 0
    approved_rows_with_mismatch = 0
    gate_inputs_config = {
        "metar_age": (
            ("metar_observation_age_minutes",),
            ("policy_metar_max_age_minutes_applied",),
        ),
        "expected_edge": (
            expected_edge_value_keys,
            expected_edge_threshold_keys,
        ),
        "probability_confidence": (
            probability_value_keys,
            probability_threshold_keys,
        ),
        "alpha_strength": (
            alpha_strength_value_keys,
            alpha_strength_threshold_keys,
        ),
    }
    gate_operators = {
        "metar_age": "<=",
        "expected_edge": ">=",
        "probability_confidence": ">=",
        "alpha_strength": ">=",
    }
    intent_context_rows_all: list[dict] = []

    for path in intents_csv_files:
        try:
            stat_result = path.stat()
            file_mtime = float(stat_result.st_mtime)
            file_size = int(stat_result.st_size)
        except OSError:
            continue
        file_key = _cache_path_key(path)
        file_metrics = _profitability_csv_cache_get(
            csv_parse_cache_conn,
            kind="intents_csv_metrics_v2",
            file_key=file_key,
            mtime=file_mtime,
            size=file_size,
        )
        if isinstance(file_metrics, dict):
            csv_parse_cache_stats["intents_hits"] += 1
        else:
            csv_parse_cache_stats["intents_misses"] += 1
            file_metrics = _parse_intents_csv_metrics_file(
                path=path,
                gate_inputs_config=gate_inputs_config,
                gate_operators=gate_operators,
                intent_policy_version_keys=intent_policy_version_keys,
            )
            put_ok, put_err = _profitability_csv_cache_put(
                csv_parse_cache_conn,
                kind="intents_csv_metrics_v2",
                file_key=file_key,
                mtime=file_mtime,
                size=file_size,
                payload=file_metrics,
            )
            if put_ok:
                csv_parse_cache_stats["puts_ok"] += 1
            else:
                csv_parse_cache_stats["puts_failed"] += 1
                if "readonly" in _normalize_text(put_err).lower():
                    csv_parse_cache_stats["puts_failed_readonly"] += 1
                if len(csv_parse_cache_stats["put_error_samples"]) < 5:
                    csv_parse_cache_stats["put_error_samples"].append(
                        {
                            "kind": "intents_csv_metrics_v2",
                            "file_key": file_key,
                            "error": _normalize_text(put_err) or "unknown_error",
                        }
                    )

        approved_rows_total += int(file_metrics.get("approved_rows_total") or 0)
        blocked_rows_total += int(file_metrics.get("blocked_rows_total") or 0)
        approved_rows_with_no_evaluable_gates += int(file_metrics.get("approved_rows_with_no_evaluable_gates") or 0)
        approved_rows_with_revalidation_conflict += int(file_metrics.get("approved_rows_with_revalidation_conflict") or 0)
        approved_rows_with_mismatch += int(file_metrics.get("approved_rows_with_mismatch") or 0)
        intent_rows_missing_policy_version += int(file_metrics.get("intent_rows_missing_policy_version") or 0)
        for key, value in (file_metrics.get("intent_policy_version_counts") or {}).items():
            key_text = _normalize_text(key)
            if key_text:
                intent_policy_version_counts[key_text] += int(_parse_int(value) or 0)
        for key, value in (file_metrics.get("gate_mismatch_by_gate") or {}).items():
            key_text = _normalize_text(key)
            if key_text:
                gate_mismatch_by_gate[key_text] += int(_parse_int(value) or 0)
        for sample in file_metrics.get("gate_mismatch_samples") or []:
            if len(gate_mismatch_samples) >= 20:
                break
            if isinstance(sample, dict):
                gate_mismatch_samples.append(dict(sample))

        for gate_name, metric in (file_metrics.get("gate_metrics") or {}).items():
            if gate_name not in gate_audit_metrics or not isinstance(metric, dict):
                continue
            target_metric = gate_audit_metrics[gate_name]
            for key in (
                "rows_evaluated",
                "rows_with_value",
                "rows_missing_value",
                "rows_with_threshold",
                "rows_missing_threshold",
                "approved_rows_evaluated",
                "approved_rows_with_value",
                "approved_rows_missing_value",
                "approved_rows_with_threshold",
                "approved_rows_missing_threshold",
                "approved_rows_pass",
                "approved_rows_fail",
                "blocked_rows_evaluated",
                "blocked_rows_with_value",
                "blocked_rows_missing_value",
                "blocked_rows_with_threshold",
                "blocked_rows_missing_threshold",
                "blocked_rows_pass",
                "blocked_rows_fail",
            ):
                target_metric[key] += int(_parse_int(metric.get(key)) or 0)
            for usage_key, usage_value in (metric.get("value_field_usage") or {}).items():
                usage_name = _normalize_text(usage_key) or "unknown"
                target_metric["value_field_usage"][usage_name] += int(_parse_int(usage_value) or 0)
            for usage_key, usage_value in (metric.get("threshold_field_usage") or {}).items():
                usage_name = _normalize_text(usage_key) or "unknown"
                target_metric["threshold_field_usage"][usage_name] += int(_parse_int(usage_value) or 0)

        def _merge_slice_counts(target: dict[str, dict], source: dict) -> None:
            for key, value in (source or {}).items():
                key_text = _normalize_text(key) or "unknown"
                intents_count = 0
                approved_count = 0
                if isinstance(value, list) and len(value) >= 2:
                    intents_count = int(_parse_int(value[0]) or 0)
                    approved_count = int(_parse_int(value[1]) or 0)
                elif isinstance(value, dict):
                    intents_count = int(_parse_int(value.get("intents_total")) or 0)
                    approved_count = int(_parse_int(value.get("approved_intents")) or 0)
                bucket = target[key_text]
                bucket["intents_total"] += intents_count
                bucket["approved_intents"] += approved_count

        _merge_slice_counts(by_station, file_metrics.get("by_station") or {})
        _merge_slice_counts(by_local_hour, file_metrics.get("by_local_hour") or {})
        _merge_slice_counts(by_signal_type, file_metrics.get("by_signal_type") or {})
        _merge_slice_counts(by_policy_reason, file_metrics.get("by_policy_reason") or {})
        _merge_slice_counts(by_underlying_family, file_metrics.get("by_underlying_family") or {})
        for row in file_metrics.get("intent_context_rows") or []:
            if isinstance(row, dict):
                intent_context_rows_all.append(dict(row))

    approved_context_by_intent_id: dict[str, dict] = {}
    fallback_context_by_intent_id: dict[str, dict] = {}
    for context_row in intent_context_rows_all:
        intent_id = _normalize_text(context_row.get("intent_id"))
        station = _normalize_text(context_row.get("station")).upper() or "unknown"
        hour_key = _normalize_text(context_row.get("local_hour")) or "unknown"
        signal_type = _normalize_text(context_row.get("signal_type")).lower() or "unknown"
        policy_reason = _normalize_text(context_row.get("policy_reason")).lower() or "unknown"
        underlying_family = _normalize_text(context_row.get("underlying_family")) or _derive_underlying_family(
            underlying_key=_normalize_text(context_row.get("underlying_key")),
            market_ticker=_normalize_text(context_row.get("market_ticker")),
        )
        approved = bool(context_row.get("approved"))
        context_payload = {
            "station": station,
            "local_hour": hour_key,
            "signal_type": signal_type,
            "policy_reason": policy_reason,
            "underlying_family": underlying_family,
        }
        if intent_id and intent_id not in fallback_context_by_intent_id:
            fallback_context_by_intent_id[intent_id] = dict(context_payload)

        if approved:
            max_entry_value = _parse_float(context_row.get("max_entry_price_dollars"))
            if intent_id and isinstance(max_entry_value, float):
                approved_max_entry_by_intent_id[intent_id] = max_entry_value
            if intent_id:
                approved_context_by_intent_id[intent_id] = dict(context_payload)
            approved_selection_rows.append(
                {
                    "intent_id": intent_id,
                    "market_ticker": _normalize_text(context_row.get("market_ticker")),
                    "underlying_key": _normalize_text(context_row.get("underlying_key")),
                    "side": _normalize_text(context_row.get("side")).lower(),
                    "max_entry_price_dollars": max_entry_value,
                    "settlement_confidence_score": _parse_float(context_row.get("settlement_confidence_score")),
                    "constraint_status": signal_type,
                    "policy_version": _normalize_text(context_row.get("policy_version")),
                }
            )

    unknown_context_payload = {
        "station": "unknown",
        "local_hour": "unknown",
        "signal_type": "unknown",
        "policy_reason": "unknown",
        "underlying_family": "unknown",
    }
    for intent_id, plan_rows in plan_by_intent_id.items():
        context_payload = (
            approved_context_by_intent_id.get(intent_id)
            or fallback_context_by_intent_id.get(intent_id)
            or unknown_context_payload
        )
        targets = (
            by_station[context_payload["station"]],
            by_local_hour[context_payload["local_hour"]],
            by_signal_type[context_payload["signal_type"]],
            by_policy_reason[context_payload["policy_reason"]],
            by_underlying_family[context_payload["underlying_family"]],
        )
        for plan_row in plan_rows:
            for bucket in targets:
                bucket["planned_orders"] += 1
                bucket["expected_edge_total"] += float(plan_row.get("expected_edge") or 0.0)
                bucket["estimated_entry_cost_total"] += float(plan_row.get("estimated_cost") or 0.0)
            client_order_id = _normalize_text(plan_row.get("client_order_id"))
            if client_order_id:
                plan_context_by_client_order_id[client_order_id] = dict(context_payload)
        if intent_id:
            plan_context_by_intent_id[intent_id] = dict(context_payload)

    if plan_rows_without_intent:
        unknown_targets = (
            by_station["unknown"],
            by_local_hour["unknown"],
            by_signal_type["unknown"],
            by_policy_reason["unknown"],
            by_underlying_family["unknown"],
        )
        for plan_row in plan_rows_without_intent:
            for bucket in unknown_targets:
                bucket["planned_orders"] += 1
                bucket["expected_edge_total"] += float(plan_row.get("expected_edge") or 0.0)
                bucket["estimated_entry_cost_total"] += float(plan_row.get("estimated_cost") or 0.0)
            client_order_id = _normalize_text(plan_row.get("client_order_id"))
            if client_order_id:
                plan_context_by_client_order_id[client_order_id] = dict(unknown_context_payload)

    gate_metrics_output: dict[str, dict] = {}
    for gate_name, metric in gate_audit_metrics.items():
        rows_evaluated = int(metric["rows_evaluated"])
        rows_with_value = max(int(metric["rows_with_value"]), rows_evaluated)
        rows_with_threshold = max(int(metric["rows_with_threshold"]), rows_evaluated)
        approved_rows_evaluated = int(metric["approved_rows_evaluated"])
        approved_rows_with_value = max(int(metric["approved_rows_with_value"]), approved_rows_evaluated)
        approved_rows_with_threshold = max(int(metric["approved_rows_with_threshold"]), approved_rows_evaluated)
        blocked_rows_evaluated = int(metric["blocked_rows_evaluated"])
        blocked_rows_with_value = max(int(metric["blocked_rows_with_value"]), blocked_rows_evaluated)
        blocked_rows_with_threshold = max(int(metric["blocked_rows_with_threshold"]), blocked_rows_evaluated)
        approved_rows_fail = int(metric["approved_rows_fail"])
        blocked_rows_fail = int(metric["blocked_rows_fail"])
        gate_metrics_output[gate_name] = {
            "operator": _normalize_text(metric.get("operator")),
            "value_field_candidates": list(metric.get("value_field_candidates") or []),
            "threshold_field_candidates": list(metric.get("threshold_field_candidates") or []),
            "rows_evaluated": rows_evaluated,
            "rows_with_value": rows_with_value,
            "rows_missing_value": int(metric["rows_missing_value"]),
            "rows_with_threshold": rows_with_threshold,
            "rows_missing_threshold": int(metric["rows_missing_threshold"]),
            "approved_rows_evaluated": approved_rows_evaluated,
            "approved_rows_with_value": approved_rows_with_value,
            "approved_rows_missing_value": int(metric["approved_rows_missing_value"]),
            "approved_rows_with_threshold": approved_rows_with_threshold,
            "approved_rows_missing_threshold": int(metric["approved_rows_missing_threshold"]),
            "approved_rows_pass": int(metric["approved_rows_pass"]),
            "approved_rows_fail": approved_rows_fail,
            "approved_fail_rate": round((approved_rows_fail / approved_rows_evaluated), 6)
            if approved_rows_evaluated > 0
            else None,
            "approved_evaluable_given_threshold_rate": round(
                (approved_rows_evaluated / approved_rows_with_threshold),
                6,
            )
            if approved_rows_with_threshold > 0
            else None,
            "blocked_rows_evaluated": blocked_rows_evaluated,
            "blocked_rows_with_value": blocked_rows_with_value,
            "blocked_rows_missing_value": int(metric["blocked_rows_missing_value"]),
            "blocked_rows_with_threshold": blocked_rows_with_threshold,
            "blocked_rows_missing_threshold": int(metric["blocked_rows_missing_threshold"]),
            "blocked_rows_pass": int(metric["blocked_rows_pass"]),
            "blocked_rows_fail": blocked_rows_fail,
            "blocked_fail_rate": round((blocked_rows_fail / blocked_rows_evaluated), 6)
            if blocked_rows_evaluated > 0
            else None,
            "blocked_evaluable_given_threshold_rate": round(
                (blocked_rows_evaluated / blocked_rows_with_threshold),
                6,
            )
            if blocked_rows_with_threshold > 0
            else None,
            "value_field_usage": dict(
                sorted(metric["value_field_usage"].items(), key=lambda item: (-int(item[1]), str(item[0])))
            ),
            "threshold_field_usage": dict(
                sorted(metric["threshold_field_usage"].items(), key=lambda item: (-int(item[1]), str(item[0])))
            ),
        }

    approval_parameter_audit = {
        "rows_total": int(approved_rows_total + blocked_rows_total),
        "approved_rows": int(approved_rows_total),
        "blocked_rows": int(blocked_rows_total),
        "approved_rows_with_gate_mismatch": int(approved_rows_with_mismatch),
        "approved_rows_with_gate_mismatch_rate": round((approved_rows_with_mismatch / approved_rows_total), 6)
        if approved_rows_total > 0
        else None,
        "approved_rows_with_no_evaluable_gates": int(approved_rows_with_no_evaluable_gates),
        "approved_rows_with_no_evaluable_gates_rate": round(
            approved_rows_with_no_evaluable_gates / approved_rows_total,
            6,
        )
        if approved_rows_total > 0
        else None,
        "approved_rows_with_revalidation_conflict": int(approved_rows_with_revalidation_conflict),
        "approved_rows_with_revalidation_conflict_rate": round(
            approved_rows_with_revalidation_conflict / approved_rows_total,
            6,
        )
        if approved_rows_total > 0
        else None,
        "mismatch_by_gate": dict(
            sorted(gate_mismatch_by_gate.items(), key=lambda item: (-int(item[1]), str(item[0])))
        ),
        "gate_metrics": gate_metrics_output,
        "sample_approved_mismatches": gate_mismatch_samples,
        "status": (
            "mismatch_detected"
            if approved_rows_with_mismatch > 0 or approved_rows_with_revalidation_conflict > 0
            else "ok"
        ),
    }

    plan_policy_versions = dict(sorted(plan_policy_version_counts.items(), key=lambda item: (-item[1], item[0])))
    intent_policy_versions = dict(sorted(intent_policy_version_counts.items(), key=lambda item: (-item[1], item[0])))
    edge_field_usage_sorted = dict(sorted(edge_field_usage.items(), key=lambda item: (-item[1], item[0])))
    plan_policy_version_set = set(plan_policy_versions.keys())
    intent_policy_version_set = set(intent_policy_versions.keys())
    overlap_policy_versions = sorted(plan_policy_version_set & intent_policy_version_set)
    plan_only_policy_versions = sorted(plan_policy_version_set - intent_policy_version_set)
    intent_only_policy_versions = sorted(intent_policy_version_set - plan_policy_version_set)
    model_lineage_warnings: list[str] = []
    if len(plan_policy_version_set) > 1:
        model_lineage_warnings.append("mixed_plan_policy_versions")
    if len(intent_policy_version_set) > 1:
        model_lineage_warnings.append("mixed_intent_policy_versions")
    if plan_policy_version_set and intent_policy_version_set and plan_policy_version_set != intent_policy_version_set:
        model_lineage_warnings.append("plan_intent_policy_version_mismatch")
    if plan_rows_missing_policy_version > 0:
        model_lineage_warnings.append("plan_rows_missing_policy_version")
    if intent_rows_missing_policy_version > 0:
        model_lineage_warnings.append("intent_rows_missing_policy_version")
    model_lineage = {
        "plan_policy_versions": plan_policy_versions,
        "intent_policy_versions": intent_policy_versions,
        "plan_rows_missing_policy_version": int(plan_rows_missing_policy_version),
        "intent_rows_missing_policy_version": int(intent_rows_missing_policy_version),
        "edge_field_usage": edge_field_usage_sorted,
        "mixed_plan_policy_versions": bool(len(plan_policy_version_set) > 1),
        "mixed_intent_policy_versions": bool(len(intent_policy_version_set) > 1),
        "plan_intent_policy_version_mismatch": bool(
            plan_policy_version_set
            and intent_policy_version_set
            and plan_policy_version_set != intent_policy_version_set
        ),
        "overlap_policy_versions": overlap_policy_versions,
        "plan_only_policy_versions": plan_only_policy_versions,
        "intent_only_policy_versions": intent_only_policy_versions,
        "warnings": model_lineage_warnings,
    }

    threshold_map: dict[str, str] = {}
    needed_market_tickers = {
        row["market_ticker"]
        for row in approved_selection_rows + planned_shadow_rows
        if row.get("market_ticker")
    }
    if needed_market_tickers:
        for path in reversed(specs_csv_files):
            if not needed_market_tickers:
                break
            try:
                with path.open("r", newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        ticker = _normalize_text(row.get("market_ticker"))
                        if not ticker or ticker not in needed_market_tickers:
                            continue
                        threshold = _normalize_text(row.get("threshold_expression"))
                        if threshold:
                            threshold_map[ticker] = threshold
                            needed_market_tickers.discard(ticker)
            except OSError:
                continue

    needed_shadow_underlyings = {
        _normalize_text(row.get("underlying_key"))
        for row in planned_shadow_rows
        if _normalize_text(row.get("underlying_key"))
    }
    final_truth_by_underlying, settlement_state_files_scanned_for_shadow_truth = _latest_settlement_truth_map_from_files(
        settlement_state_files,
        needed_underlyings=needed_shadow_underlyings if needed_shadow_underlyings else None,
    )
    settlement_state_files_scanned_for_trial_truth = 0

    def _threshold_yes_outcome(threshold_expression: str, observed: float) -> bool | None:
        text = _normalize_text(threshold_expression)
        if not text or ":" not in text:
            return None
        parts = [token.strip() for token in text.split(":")]
        if len(parts) < 2:
            return None
        kind = parts[0].lower()
        try:
            values = [float(token) for token in parts[1:] if token]
        except ValueError:
            return None
        if kind == "at_most" and len(values) >= 1:
            return observed <= values[0]
        if kind == "below" and len(values) >= 1:
            return observed < values[0]
        if kind == "at_least" and len(values) >= 1:
            return observed >= values[0]
        if kind == "above" and len(values) >= 1:
            return observed > values[0]
        if kind == "between" and len(values) >= 2:
            low = min(values[0], values[1])
            high = max(values[0], values[1])
            return low <= observed <= high
        if kind == "equal" and len(values) >= 1:
            return observed == values[0]
        return None

    shadow_confidence_samples: list[float] = []
    graded_shadow_rows: list[dict] = []
    for index, planned_order in enumerate(planned_shadow_rows):
        confidence = planned_order.get("confidence")
        if isinstance(confidence, float):
            shadow_confidence_samples.append(max(0.0, min(1.0, confidence)))

        market_ticker = _normalize_text(planned_order.get("market_ticker"))
        underlying_key = _normalize_text(planned_order.get("underlying_key"))
        side = _normalize_text(planned_order.get("side")).lower()
        threshold = _normalize_text(threshold_map.get(market_ticker))
        truth = final_truth_by_underlying.get(underlying_key)
        intent_id = _normalize_text(planned_order.get("intent_id"))
        shadow_order_id = _normalize_text(planned_order.get("shadow_order_id"))
        if not shadow_order_id:
            shadow_order_id = f"missing_shadow_order_id:{index}:{intent_id}:{market_ticker}:{side}"
        market_side_key = f"{market_ticker}|{side}" if market_ticker and side else ""
        captured_at_dt = planned_order.get("captured_at_dt")
        if not isinstance(captured_at_dt, datetime):
            captured_at_epoch = _parse_float(planned_order.get("captured_at_epoch"))
            if isinstance(captured_at_epoch, float):
                captured_at_dt = datetime.fromtimestamp(captured_at_epoch, timezone.utc)
            else:
                captured_at_dt = datetime.fromtimestamp(start_epoch, timezone.utc)

        row_result = {
            "row_index": index,
            "shadow_order_id": shadow_order_id,
            "market_side_key": market_side_key,
            "market_ticker": market_ticker,
            "underlying_key": underlying_key,
            "underlying_family": _derive_underlying_family(
                underlying_key=underlying_key,
                market_ticker=market_ticker,
            ),
            "side": side,
            "intent_id": intent_id,
            "client_order_id": _normalize_text(planned_order.get("client_order_id")),
            "planned_at_utc": _normalize_text(planned_order.get("planned_at_utc")) or captured_at_dt.isoformat(),
            "captured_at_epoch": captured_at_dt.timestamp(),
            "resolved": False,
            "resolution_reason": "",
            "win": None,
            "entry_price_dollars": None,
            "entry_cost_dollars": None,
            "contracts_count": max(1, int(_parse_int(planned_order.get("contracts_count")) or 1)),
            "counterfactual_pnl_dollars_if_live": None,
            "threshold_expression": threshold,
            "final_truth_value": truth if isinstance(truth, float) else None,
        }

        if not threshold or not isinstance(truth, float) or side not in {"yes", "no"}:
            row_result["resolution_reason"] = "missing_threshold_or_final_truth_or_side"
            graded_shadow_rows.append(row_result)
            continue

        yes_outcome = _threshold_yes_outcome(threshold, truth)
        if yes_outcome is None:
            row_result["resolution_reason"] = "unsupported_threshold_expression"
            graded_shadow_rows.append(row_result)
            continue

        entry_price = planned_order.get("entry_price_dollars")
        if not isinstance(entry_price, float):
            fallback_max_entry = approved_max_entry_by_intent_id.get(intent_id)
            entry_price = fallback_max_entry if isinstance(fallback_max_entry, float) else None
        if not isinstance(entry_price, float):
            row_result["resolution_reason"] = "missing_entry_price"
            graded_shadow_rows.append(row_result)
            continue

        entry_price = max(0.01, min(0.99, entry_price))
        contracts_count = max(1, int(_parse_int(planned_order.get("contracts_count")) or row_result["contracts_count"]))
        entry_cost = entry_price * contracts_count
        win = (side == "yes" and yes_outcome) or (side == "no" and not yes_outcome)
        pnl = ((1.0 - entry_price) if win else (-entry_price)) * contracts_count
        row_result.update(
            {
                "resolved": True,
                "resolution_reason": "resolved",
                "win": bool(win),
                "entry_price_dollars": entry_price,
                "entry_cost_dollars": float(entry_cost),
                "contracts_count": int(contracts_count),
                "counterfactual_pnl_dollars_if_live": float(pnl),
            }
        )
        graded_shadow_rows.append(row_result)

    def _aggregate_shadow_rows(rows: list[dict]) -> dict:
        resolved_rows = [row for row in rows if bool(row.get("resolved"))]
        unresolved_rows = [row for row in rows if not bool(row.get("resolved"))]
        resolved_count = len(resolved_rows)
        wins = sum(1 for row in resolved_rows if row.get("win") is True)
        losses = sum(1 for row in resolved_rows if row.get("win") is False)
        pushes = 0
        pnl_total = sum(float(row.get("counterfactual_pnl_dollars_if_live") or 0.0) for row in resolved_rows)
        return {
            "rows_total": len(rows),
            "resolved_count": resolved_count,
            "unresolved_count": len(unresolved_rows),
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate": round((wins / resolved_count), 6) if resolved_count > 0 else None,
            "pnl_total_dollars": round(pnl_total, 6),
            "pnl_per_resolved_order_dollars": round((pnl_total / resolved_count), 6) if resolved_count > 0 else None,
        }

    duplicate_shadow_order_ids_counter: Counter[str] = Counter()
    duplicate_shadow_order_ids: dict[str, int] = {}
    duplicate_warnings: list[str] = []
    canonical_shadow_order_map: dict[str, dict] = {}
    for row in graded_shadow_rows:
        shadow_order_id = _normalize_text(row.get("shadow_order_id"))
        if not shadow_order_id:
            continue
        if shadow_order_id in canonical_shadow_order_map:
            duplicate_shadow_order_ids_counter[shadow_order_id] += 1
            canonical = canonical_shadow_order_map[shadow_order_id]
            exact_duplicate = (
                _normalize_text(canonical.get("market_ticker")) == _normalize_text(row.get("market_ticker"))
                and _normalize_text(canonical.get("side")) == _normalize_text(row.get("side"))
                and _normalize_text(canonical.get("underlying_key")) == _normalize_text(row.get("underlying_key"))
                and bool(canonical.get("resolved")) == bool(row.get("resolved"))
                and canonical.get("win") == row.get("win")
                and _parse_float(canonical.get("entry_price_dollars")) == _parse_float(row.get("entry_price_dollars"))
                and _parse_float(canonical.get("counterfactual_pnl_dollars_if_live"))
                == _parse_float(row.get("counterfactual_pnl_dollars_if_live"))
            )
            warning_kind = "exact_duplicate" if exact_duplicate else "non_exact_reuse"
            duplicate_warnings.append(
                f"{warning_kind}: shadow_order_id={shadow_order_id} "
                f"kept_planned_at={_normalize_text(canonical.get('planned_at_utc'))} "
                f"dropped_planned_at={_normalize_text(row.get('planned_at_utc'))}"
            )
            continue
        canonical_shadow_order_map[shadow_order_id] = row

    if duplicate_shadow_order_ids_counter:
        duplicate_shadow_order_ids = {
            key: int(value + 1)
            for key, value in sorted(duplicate_shadow_order_ids_counter.items())
        }
    duplicate_count = int(sum(max(0, value - 1) for value in duplicate_shadow_order_ids.values()))

    canonical_shadow_rows = list(canonical_shadow_order_map.values())

    market_side_groups: dict[str, list[dict]] = defaultdict(list)
    for row in canonical_shadow_rows:
        market_side_key = _normalize_text(row.get("market_side_key"))
        if market_side_key:
            market_side_groups[market_side_key].append(row)
    canonical_market_side_rows: list[dict] = []
    for rows in market_side_groups.values():
        rows_sorted = sorted(rows, key=lambda item: float(item.get("captured_at_epoch") or 0.0))
        resolved_rows = [item for item in rows_sorted if bool(item.get("resolved"))]
        representative = resolved_rows[0] if resolved_rows else rows_sorted[0]
        canonical_market_side_rows.append(representative)

    row_metrics = _aggregate_shadow_rows(graded_shadow_rows)
    unique_shadow_order_metrics = _aggregate_shadow_rows(canonical_shadow_rows)
    unique_market_side_metrics = _aggregate_shadow_rows(canonical_market_side_rows)

    def _serialize_shadow_outcome(row: dict) -> dict:
        return {
            "shadow_order_id": _normalize_text(row.get("shadow_order_id")) or None,
            "market_side_key": _normalize_text(row.get("market_side_key")) or None,
            "market_ticker": _normalize_text(row.get("market_ticker")) or None,
            "underlying_key": _normalize_text(row.get("underlying_key")) or None,
            "underlying_family": _normalize_text(row.get("underlying_family")) or None,
            "side": _normalize_text(row.get("side")) or None,
            "client_order_id": _normalize_text(row.get("client_order_id")) or None,
            "planned_at_utc": _normalize_text(row.get("planned_at_utc")) or None,
            "resolution_reason": _normalize_text(row.get("resolution_reason")) or None,
            "win": row.get("win") if isinstance(row.get("win"), bool) else None,
            "contracts_count": int(_parse_int(row.get("contracts_count")) or 1),
            "entry_price_dollars": _to_json_number(row.get("entry_price_dollars")),
            "entry_cost_dollars": _to_json_number(row.get("entry_cost_dollars")),
            "counterfactual_pnl_dollars_if_live": _to_json_number(row.get("counterfactual_pnl_dollars_if_live")),
            "threshold_expression": _normalize_text(row.get("threshold_expression")) or None,
            "final_truth_value": _to_json_number(row.get("final_truth_value")),
            "captured_at_epoch": _to_json_number(row.get("captured_at_epoch")),
        }

    resolved_unique_market_side_rows = sorted(
        [row for row in canonical_market_side_rows if bool(row.get("resolved"))],
        key=lambda item: float(item.get("captured_at_epoch") or 0.0),
        reverse=True,
    )
    resolved_unique_shadow_order_rows = sorted(
        [row for row in canonical_shadow_rows if bool(row.get("resolved"))],
        key=lambda item: float(item.get("captured_at_epoch") or 0.0),
        reverse=True,
    )
    recent_resolved_unique_market_sides = [
        _serialize_shadow_outcome(row) for row in resolved_unique_market_side_rows[:10]
    ]
    recent_resolved_unique_shadow_orders = [
        _serialize_shadow_outcome(row) for row in resolved_unique_shadow_order_rows[:10]
    ]
    last_resolved_unique_market_side = recent_resolved_unique_market_sides[0] if recent_resolved_unique_market_sides else None
    last_resolved_unique_shadow_order = recent_resolved_unique_shadow_orders[0] if recent_resolved_unique_shadow_orders else None

    def _new_settled_slice_map() -> dict[str, defaultdict]:
        return {
            "by_station": defaultdict(_settled_slice_bucket),
            "by_local_hour": defaultdict(_settled_slice_bucket),
            "by_signal_type": defaultdict(_settled_slice_bucket),
            "by_policy_reason": defaultdict(_settled_slice_bucket),
            "by_underlying_family": defaultdict(_settled_slice_bucket),
        }

    def _resolve_settled_context(*, row: dict) -> tuple[dict[str, str], bool]:
        client_order_id = _normalize_text(row.get("client_order_id"))
        intent_id = _normalize_text(row.get("intent_id"))
        context = {}
        if client_order_id:
            context = dict(plan_context_by_client_order_id.get(client_order_id) or {})
        if not context and intent_id:
            context = dict(plan_context_by_intent_id.get(intent_id) or {})
        context_found = bool(context)
        station = _normalize_text(context.get("station")).upper() or "unknown"
        local_hour = _normalize_text(context.get("local_hour")) or "unknown"
        signal_type = _normalize_text(context.get("signal_type")).lower() or "unknown"
        policy_reason = _normalize_text(context.get("policy_reason")).lower() or "unknown"
        underlying_family = _normalize_text(context.get("underlying_family")) or _derive_underlying_family(
            underlying_key=_normalize_text(row.get("underlying_key")),
            market_ticker=_normalize_text(row.get("market_ticker")),
        )
        return {
            "by_station": station,
            "by_local_hour": local_hour,
            "by_signal_type": signal_type,
            "by_policy_reason": policy_reason,
            "by_underlying_family": underlying_family,
        }, context_found

    def _accumulate_settled_shadow_attribution(rows: list[dict]) -> tuple[dict[str, defaultdict], int]:
        output = _new_settled_slice_map()
        unresolved_context_rows = 0
        for row in rows:
            if not bool(row.get("resolved")):
                continue
            pnl_value = _parse_float(row.get("counterfactual_pnl_dollars_if_live"))
            if not isinstance(pnl_value, float):
                continue
            entry_cost_value = _parse_float(row.get("entry_cost_dollars"))
            win_value = row.get("win")
            context_keys, context_found = _resolve_settled_context(row=row)
            if not context_found:
                unresolved_context_rows += 1
            for dimension, bucket_key in context_keys.items():
                bucket = output[dimension][bucket_key]
                bucket["resolved_count"] += 1
                bucket["pnl_total"] += float(pnl_value)
                bucket["entry_cost_total"] += float(entry_cost_value or 0.0)
                if win_value is True:
                    bucket["wins"] += 1
                    bucket["win_pnl_total"] += float(max(pnl_value, 0.0))
                elif win_value is False:
                    bucket["losses"] += 1
                    bucket["loss_abs_pnl_total"] += float(abs(min(pnl_value, 0.0)))
                else:
                    bucket["pushes"] += 1
        return output, unresolved_context_rows

    settled_shadow_attr_rows_raw, settled_shadow_unknown_context_rows = _accumulate_settled_shadow_attribution(
        graded_shadow_rows
    )
    settled_shadow_attr_unique_order_raw, settled_shadow_unknown_context_unique_orders = _accumulate_settled_shadow_attribution(
        canonical_shadow_rows
    )
    settled_shadow_attr_unique_market_side_raw, settled_shadow_unknown_context_unique_market_sides = (
        _accumulate_settled_shadow_attribution(canonical_market_side_rows)
    )

    settled_shadow_attr_rows = {
        key: _finalize_settled_slice_buckets(value) for key, value in settled_shadow_attr_rows_raw.items()
    }
    settled_shadow_attr_unique_order = {
        key: _finalize_settled_slice_buckets(value) for key, value in settled_shadow_attr_unique_order_raw.items()
    }
    settled_shadow_attr_unique_market_side = {
        key: _finalize_settled_slice_buckets(value)
        for key, value in settled_shadow_attr_unique_market_side_raw.items()
    }

    settled_shadow_top_contributors = {
        "row_based": {
            key: {
                "best": _top_contributors_by_pnl(value, direction="best", top_n=5),
                "worst": _top_contributors_by_pnl(value, direction="worst", top_n=5),
            }
            for key, value in settled_shadow_attr_rows.items()
        },
        "unique_shadow_order": {
            key: {
                "best": _top_contributors_by_pnl(value, direction="best", top_n=5),
                "worst": _top_contributors_by_pnl(value, direction="worst", top_n=5),
            }
            for key, value in settled_shadow_attr_unique_order.items()
        },
        "unique_market_side": {
            key: {
                "best": _top_contributors_by_pnl(value, direction="best", top_n=5),
                "worst": _top_contributors_by_pnl(value, direction="worst", top_n=5),
            }
            for key, value in settled_shadow_attr_unique_market_side.items()
        },
    }

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    orders_submitted = {}
    orders_filled = set()
    settled_orders = {}
    if journal_path.exists():
        start_iso = datetime.fromtimestamp(start_epoch, timezone.utc).isoformat()
        end_iso = datetime.fromtimestamp(end_epoch, timezone.utc).isoformat()
        with sqlite3.connect(journal_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    captured_at_utc,
                    event_type,
                    market_ticker,
                    client_order_id,
                    exchange_order_id,
                    realized_pnl_dollars,
                    payload_json
                FROM execution_events
                WHERE captured_at_utc >= ?
                  AND captured_at_utc <= ?
                  AND event_type IN ('order_submitted', 'partial_fill', 'full_fill', 'settlement_outcome')
                ORDER BY captured_at_utc ASC
                """,
                (start_iso, end_iso),
            ).fetchall()
        for row in rows:
            event = dict(row)
            payload_text = _normalize_text(event.get("payload_json"))
            payload = {}
            if payload_text:
                try:
                    parsed = json.loads(payload_text)
                    if isinstance(parsed, dict):
                        payload = parsed
                except json.JSONDecodeError:
                    payload = {}
            event["payload"] = payload
            event_type = _normalize_text(event.get("event_type"))
            order_key = _normalize_text(event.get("exchange_order_id")) or _normalize_text(event.get("client_order_id"))
            if not order_key:
                order_key = (
                    f"{_normalize_text(event.get('market_ticker'))}:"
                    f"{_normalize_text(event.get('captured_at_utc'))}:"
                    f"{event_type}"
                )
            event["order_key"] = order_key
            submitted_keys = set(orders_submitted.keys())
            if not _is_temperature_event(event, submitted_keys):
                continue
            if event_type == "order_submitted":
                orders_submitted[order_key] = event
            elif event_type in {"partial_fill", "full_fill"}:
                orders_filled.add(order_key)
            elif event_type == "settlement_outcome":
                settled_orders[order_key] = event

    wins = 0
    losses = 0
    pushes = 0
    realized_pnl_total = 0.0
    settled_numeric = 0
    settled_rows = []

    for key, event in settled_orders.items():
        realized = _parse_float(event.get("realized_pnl_dollars"))
        client_order_id = _normalize_text(event.get("client_order_id"))
        if isinstance(realized, float):
            settled_numeric += 1
            realized_pnl_total += realized
            if realized > 0:
                wins += 1
                outcome = "win"
            elif realized < 0:
                losses += 1
                outcome = "loss"
            else:
                pushes += 1
                outcome = "push"
        else:
            outcome = "unknown"

        expected_plan = plan_by_client_order_id.get(client_order_id, {})
        context = plan_context_by_client_order_id.get(client_order_id, {}) if client_order_id else {}
        if (not isinstance(context, dict) or not context) and isinstance(expected_plan, dict):
            expected_intent_id = _normalize_text(expected_plan.get("intent_id"))
            if expected_intent_id:
                context = plan_context_by_intent_id.get(expected_intent_id, {})
        if isinstance(context, dict) and context and isinstance(realized, float):
            targets = (
                by_station[context["station"]],
                by_local_hour[context["local_hour"]],
                by_signal_type[context["signal_type"]],
                by_policy_reason[context["policy_reason"]],
                by_underlying_family[context["underlying_family"]],
            )
            for bucket in targets:
                bucket["orders_settled"] += 1
                bucket["realized_pnl_total"] += realized
                if outcome == "win":
                    bucket["wins"] += 1
                elif outcome == "loss":
                    bucket["losses"] += 1
                elif outcome == "push":
                    bucket["pushes"] += 1

        expected_edge = _parse_float(expected_plan.get("expected_edge"))
        expected_cost = _parse_float(expected_plan.get("estimated_cost"))
        realized_minus_expected = (
            round(realized - expected_edge, 6)
            if isinstance(realized, float) and isinstance(expected_edge, float)
            else None
        )
        settled_rows.append(
            {
                "order_key": key,
                "captured_at_utc": _normalize_text(event.get("captured_at_utc")),
                "market_ticker": _normalize_text(event.get("market_ticker")),
                "client_order_id": client_order_id,
                "realized_pnl_dollars": realized if isinstance(realized, float) else "",
                "expected_edge_dollars": expected_edge if isinstance(expected_edge, float) else "",
                "expected_cost_dollars": expected_cost if isinstance(expected_cost, float) else "",
                "realized_minus_expected_dollars": realized_minus_expected if isinstance(realized_minus_expected, float) else "",
                "outcome": outcome,
            }
        )

    expected_shadow = {
        "planned_orders": planned_orders_total,
        "expected_edge_total": round(expected_edge_total, 6),
        "estimated_entry_cost_total": round(estimated_entry_cost_total, 6),
        "expected_roi_on_deployed_capital": round((expected_edge_total / estimated_entry_cost_total), 6)
        if estimated_entry_cost_total > 0
        else None,
        "expected_roi_on_deployed_capital_percent": round((expected_edge_total / estimated_entry_cost_total) * 100.0, 4)
        if estimated_entry_cost_total > 0
        else None,
        "expected_edge_per_order": round((expected_edge_total / planned_orders_total), 6)
        if planned_orders_total > 0
        else None,
        "reference_account_dollars": 1000.0,
        "expected_pnl_dollars_at_current_plan_size": round(expected_edge_total, 6),
        "expected_roi_on_reference_account": round(expected_edge_total / 1000.0, 6),
        "expected_roi_on_reference_account_percent": round((expected_edge_total / 1000.0) * 100.0, 4),
        "expected_pnl_dollars_on_reference_account": round(expected_edge_total, 6),
        "expected_account_value_dollars_on_reference_account": round(1000.0 + expected_edge_total, 6),
        # Backward-compatible aliases for older consumers.
        "expected_roi": round((expected_edge_total / estimated_entry_cost_total), 6)
        if estimated_entry_cost_total > 0
        else None,
        "expected_roi_percent": round((expected_edge_total / estimated_entry_cost_total) * 100.0, 4)
        if estimated_entry_cost_total > 0
        else None,
        "expected_pnl_dollars_on_1000_account": round(expected_edge_total, 6),
        "expected_account_value_dollars_on_1000_account": round(1000.0 + expected_edge_total, 6),
        "metrics_basis": {
            "type": "planning_assumption",
            "description": "Expected values are proxy EV from trade-plan edge fields, not calibrated realized profitability.",
        },
        "model_lineage": model_lineage,
    }
    realized_settled = {
        "orders_submitted": len(orders_submitted),
        "orders_filled": len(orders_filled),
        "orders_settled": len(settled_orders),
        "orders_settled_with_numeric_pnl": settled_numeric,
        "realized_pnl_total": round(realized_pnl_total, 6),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": round((wins / settled_numeric), 6) if settled_numeric > 0 else None,
        "pnl_per_settled_order": round((realized_pnl_total / settled_numeric), 6) if settled_numeric > 0 else None,
    }
    expected_vs_realized = {
        "expected_edge_total": expected_shadow["expected_edge_total"],
        "realized_pnl_total": realized_settled["realized_pnl_total"],
        "delta_total": round(realized_settled["realized_pnl_total"] - expected_shadow["expected_edge_total"], 6),
        "delta_per_order": round(
            (realized_settled["realized_pnl_total"] - expected_shadow["expected_edge_total"]) / settled_numeric,
            6,
        )
        if settled_numeric > 0
        else None,
        "comparability_warning": (
            "Expected-vs-realized comparison includes mixed policy/model versions."
            if any(
                warning in model_lineage_warnings
                for warning in (
                    "mixed_plan_policy_versions",
                    "mixed_intent_policy_versions",
                    "plan_intent_policy_version_mismatch",
                )
            )
            else ""
        ),
    }
    shadow_window_duration_seconds = max(0.0, end_epoch - start_epoch)
    shadow_window_duration_hours = round(shadow_window_duration_seconds / 3600.0, 4)
    if abs(shadow_window_duration_hours - round(shadow_window_duration_hours)) < 1e-6:
        shadow_rolling_label = f"rolling_{int(round(shadow_window_duration_hours))}h"
    else:
        shadow_rolling_label = f"rolling_{shadow_window_duration_hours}h"

    shadow_settled_reference = {
        # Explicitly identify rolling window semantics to avoid "today" interpretation.
        "window_semantics": {
            "type": "rolling",
            "duration_seconds": int(shadow_window_duration_seconds),
            "duration_hours": shadow_window_duration_hours,
            "is_calendar_day": False,
            "rolling_label": shadow_rolling_label,
        },
        "planned_shadow_rows_total": len(graded_shadow_rows),
        "planned_unique_shadow_orders_total": len(canonical_shadow_rows),
        "planned_unique_market_sides_total": len(canonical_market_side_rows),
        "resolved_planned_rows": int(row_metrics["resolved_count"]),
        "unresolved_planned_rows": int(row_metrics["unresolved_count"]),
        "resolved_unique_shadow_orders": int(unique_shadow_order_metrics["resolved_count"]),
        "resolved_unique_market_sides": int(unique_market_side_metrics["resolved_count"]),
        "unresolved_unique_shadow_orders": int(unique_shadow_order_metrics["unresolved_count"]),
        "unresolved_unique_market_sides": int(unique_market_side_metrics["unresolved_count"]),
        "wins_rows": int(row_metrics["wins"]),
        "losses_rows": int(row_metrics["losses"]),
        "pushes_rows": int(row_metrics["pushes"]),
        "wins_unique_shadow_orders": int(unique_shadow_order_metrics["wins"]),
        "losses_unique_shadow_orders": int(unique_shadow_order_metrics["losses"]),
        "pushes_unique_shadow_orders": int(unique_shadow_order_metrics["pushes"]),
        "wins_unique_market_sides": int(unique_market_side_metrics["wins"]),
        "losses_unique_market_sides": int(unique_market_side_metrics["losses"]),
        "pushes_unique_market_sides": int(unique_market_side_metrics["pushes"]),
        "selection_win_rate_resolved_rows": row_metrics["win_rate"],
        "selection_win_rate_resolved_unique_shadow_orders": unique_shadow_order_metrics["win_rate"],
        "selection_win_rate_resolved_unique_market_sides": unique_market_side_metrics["win_rate"],
        "counterfactual_pnl_total_rows_dollars_if_live": row_metrics["pnl_total_dollars"],
        "counterfactual_pnl_total_unique_shadow_orders_dollars_if_live": unique_shadow_order_metrics["pnl_total_dollars"],
        "counterfactual_pnl_total_unique_market_sides_dollars_if_live": unique_market_side_metrics["pnl_total_dollars"],
        "counterfactual_pnl_per_resolved_row_dollars_if_live": row_metrics["pnl_per_resolved_order_dollars"],
        "counterfactual_pnl_per_resolved_unique_shadow_order_dollars_if_live": unique_shadow_order_metrics[
            "pnl_per_resolved_order_dollars"
        ],
        "counterfactual_pnl_per_resolved_unique_market_side_dollars_if_live": unique_market_side_metrics[
            "pnl_per_resolved_order_dollars"
        ],
        "recent_resolved_unique_market_sides": recent_resolved_unique_market_sides,
        "recent_resolved_unique_shadow_orders": recent_resolved_unique_shadow_orders,
        "last_resolved_unique_market_side": last_resolved_unique_market_side,
        "last_resolved_unique_shadow_order": last_resolved_unique_shadow_order,
        "duplicate_shadow_order_ids": duplicate_shadow_order_ids,
        "duplicate_count": duplicate_count,
        "warnings": duplicate_warnings,
        "projected_win_rate_from_confidence": round(
            sum(shadow_confidence_samples) / len(shadow_confidence_samples),
            6,
        )
        if shadow_confidence_samples
        else None,
        "settlement_state_files_scanned_for_truth": int(settlement_state_files_scanned_for_shadow_truth),
        # Conservative headline metrics: prediction accuracy should use unique market-side.
        "headline": {
            "prediction_accuracy_basis": "unique_market_side",
            "order_instance_accuracy_basis": "unique_shadow_order",
            "resolved_predictions": int(unique_market_side_metrics["resolved_count"]),
            "wins": int(unique_market_side_metrics["wins"]),
            "losses": int(unique_market_side_metrics["losses"]),
            "win_rate": unique_market_side_metrics["win_rate"],
            "resolved_order_instances": int(unique_shadow_order_metrics["resolved_count"]),
            "order_instance_win_rate": unique_shadow_order_metrics["win_rate"],
        },
        # Backward-compatible aliases for older consumers. Prefer new explicit keys above.
        "resolved_planned_orders": int(unique_shadow_order_metrics["resolved_count"]),
        "unresolved_planned_orders": int(unique_shadow_order_metrics["unresolved_count"]),
        "wins": int(unique_shadow_order_metrics["wins"]),
        "losses": int(unique_shadow_order_metrics["losses"]),
        "pushes": int(unique_shadow_order_metrics["pushes"]),
        "selection_win_rate_resolved": unique_shadow_order_metrics["win_rate"],
        "counterfactual_pnl_total_dollars_if_live": unique_shadow_order_metrics["pnl_total_dollars"],
        "counterfactual_pnl_per_resolved_order_dollars_if_live": unique_shadow_order_metrics[
            "pnl_per_resolved_order_dollars"
        ],
    }

    # Persistent trial balance tracks cumulative resolved counterfactual
    # outcomes from planned shadow orders since the last manual refill/reset.
    state_file = out_dir / "checkpoints" / "trial_balance_state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_payload = _load(state_file)
    start_balance = _parse_float(state_payload.get("starting_balance_dollars"))
    if not isinstance(start_balance, float) or start_balance <= 0:
        start_balance = 1000.0
    reset_epoch = _parse_float(state_payload.get("reset_epoch"))
    if not isinstance(reset_epoch, float) or reset_epoch < 0:
        reset_epoch = 0.0
    reset_at_utc = _normalize_text(state_payload.get("reset_at_utc"))
    if not reset_at_utc:
        reset_at_utc = datetime.fromtimestamp(reset_epoch, timezone.utc).isoformat()
    reset_reason = _normalize_text(state_payload.get("reset_reason")) or "initial_default"

    trial_balance_cache_file = out_dir / "checkpoints" / "trial_balance_cache.json"
    all_plan_files = _files_in_window(out_dir, "kalshi_temperature_trade_plan_*.csv", reset_epoch, end_epoch)
    trial_cache, trial_cache_load_status = _load_trial_balance_cache(trial_balance_cache_file, reset_epoch)
    parsed_files_cache = trial_cache.get("parsed_files") if isinstance(trial_cache.get("parsed_files"), dict) else {}
    parsed_files_cache = dict(parsed_files_cache)
    current_files_meta: dict[str, dict] = {}
    trial_cache_needs_full_rebuild = False
    trial_cache_rebuild_reason = ""
    for path in all_plan_files:
        file_key = _cache_path_key(path)
        try:
            stat_result = path.stat()
            mtime = float(stat_result.st_mtime)
            size = int(stat_result.st_size)
        except OSError:
            mtime = -1.0
            size = -1
        current_files_meta[file_key] = {
            "mtime": mtime,
            "size": size,
            "captured_at_epoch": mtime if mtime >= 0 else 0.0,
        }

    cached_file_keys = set(parsed_files_cache.keys())
    current_file_keys = set(current_files_meta.keys())
    if cached_file_keys - current_file_keys:
        trial_cache_needs_full_rebuild = True
        trial_cache_rebuild_reason = "file_removed_or_out_of_window"

    if not trial_cache_needs_full_rebuild:
        for file_key, cached_meta in parsed_files_cache.items():
            current_meta = current_files_meta.get(file_key)
            if not isinstance(cached_meta, dict) or not isinstance(current_meta, dict):
                trial_cache_needs_full_rebuild = True
                trial_cache_rebuild_reason = "file_metadata_invalid"
                break
            cached_mtime = _parse_float(cached_meta.get("mtime"))
            cached_size = _parse_int(cached_meta.get("size"))
            current_mtime = _parse_float(current_meta.get("mtime"))
            current_size = _parse_int(current_meta.get("size"))
            if (
                not isinstance(cached_mtime, float)
                or not isinstance(current_mtime, float)
                or cached_size is None
                or current_size is None
                or abs(cached_mtime - current_mtime) > 1e-9
                or int(cached_size) != int(current_size)
            ):
                trial_cache_needs_full_rebuild = True
                trial_cache_rebuild_reason = "file_changed"
                break

    if trial_cache_needs_full_rebuild:
        trial_cache = _trial_balance_empty_cache(reset_epoch)
        parsed_files_cache = {}

    trial_occurrence_counts = (
        trial_cache.get("occurrence_counts")
        if isinstance(trial_cache.get("occurrence_counts"), dict)
        else {}
    )
    trial_occurrence_counts = {
        str(key): max(0, int(_parse_int(value) or 0))
        for key, value in trial_occurrence_counts.items()
        if _normalize_text(key)
    }
    canonical_trial_plan_rows = (
        trial_cache.get("canonical_rows")
        if isinstance(trial_cache.get("canonical_rows"), dict)
        else {}
    )
    canonical_trial_plan_rows = {
        str(key): dict(value)
        for key, value in canonical_trial_plan_rows.items()
        if _normalize_text(key) and isinstance(value, dict)
    }
    trial_planned_rows_total_cached = _parse_int(trial_cache.get("planned_rows_total"))
    trial_planned_rows_total = (
        int(trial_planned_rows_total_cached)
        if isinstance(trial_planned_rows_total_cached, int) and trial_planned_rows_total_cached >= 0
        else 0
    )

    files_skipped_via_cache = 0
    files_parsed_this_run = 0
    rows_parsed_this_run = 0
    files_processed_order = [_cache_path_key(path) for path in all_plan_files]
    for path in all_plan_files:
        file_key = _cache_path_key(path)
        file_meta = current_files_meta.get(file_key, {})
        cached_meta = parsed_files_cache.get(file_key)
        if (
            isinstance(cached_meta, dict)
            and isinstance(file_meta, dict)
            and isinstance(_parse_float(cached_meta.get("mtime")), float)
            and isinstance(_parse_int(cached_meta.get("size")), int)
            and isinstance(_parse_float(file_meta.get("mtime")), float)
            and isinstance(_parse_int(file_meta.get("size")), int)
            and abs(float(_parse_float(cached_meta.get("mtime")) or 0.0) - float(_parse_float(file_meta.get("mtime")) or 0.0)) <= 1e-9
            and int(_parse_int(cached_meta.get("size")) or 0) == int(_parse_int(file_meta.get("size")) or 0)
        ):
            files_skipped_via_cache += 1
            continue

        captured_at_dt = _file_captured_at_utc(path)
        parsed_rows = _parse_temperature_plan_rows(path, captured_at_dt)
        files_parsed_this_run += 1
        rows_parsed_this_run += len(parsed_rows)
        trial_planned_rows_total += len(parsed_rows)
        for parsed_row in parsed_rows:
            shadow_order_id = _normalize_text(parsed_row.get("shadow_order_id"))
            if not shadow_order_id:
                continue
            previous_count = int(trial_occurrence_counts.get(shadow_order_id) or 0)
            trial_occurrence_counts[shadow_order_id] = previous_count + 1
            if previous_count == 0:
                canonical_trial_plan_rows[shadow_order_id] = {
                    "intent_id": _normalize_text(parsed_row.get("intent_id")),
                    "client_order_id": _normalize_text(parsed_row.get("client_order_id")),
                    "shadow_order_id": shadow_order_id,
                    "market_ticker": _normalize_text(parsed_row.get("market_ticker")),
                    "underlying_key": _normalize_text(parsed_row.get("underlying_key")),
                    "side": _normalize_text(parsed_row.get("side")).lower(),
                    "entry_price_dollars": _parse_float(parsed_row.get("entry_price_dollars")),
                    "contracts_count": max(1, int(_parse_int(parsed_row.get("contracts_count")) or 1)),
                    "captured_at_epoch": float(_parse_float(parsed_row.get("captured_at_epoch")) or 0.0),
                }

        parsed_files_cache[file_key] = {
            "mtime": float(_parse_float(file_meta.get("mtime")) or 0.0),
            "size": int(_parse_int(file_meta.get("size")) or 0),
            "captured_at_epoch": float(_parse_float(file_meta.get("captured_at_epoch")) or 0.0),
        }

    if files_parsed_this_run == 0:
        trial_cache_status = "cache_hit_no_new_files"
    elif trial_cache_needs_full_rebuild:
        trial_cache_status = f"full_rebuild:{trial_cache_rebuild_reason or 'unknown'}"
    elif trial_cache_load_status == "cold_start":
        trial_cache_status = "initial_build"
    elif trial_cache_load_status != "warm_start":
        trial_cache_status = f"rebuild_after_{trial_cache_load_status}"
    else:
        trial_cache_status = "incremental_update"

    trial_cache = {
        "version": TRIAL_BALANCE_CACHE_VERSION,
        "reset_epoch": float(reset_epoch),
        "parsed_files": parsed_files_cache,
        "planned_rows_total": int(trial_planned_rows_total),
        "occurrence_counts": trial_occurrence_counts,
        "canonical_rows": canonical_trial_plan_rows,
        "updated_at_epoch": float(time.time()),
    }
    trial_cache_dirty = bool(
        trial_cache_needs_full_rebuild
        or files_parsed_this_run > 0
        or trial_cache_load_status != "warm_start"
    )
    trial_cache_write_skipped = False
    trial_cache_write_error = ""
    if trial_cache_dirty:
        try:
            _write_text_atomic(
                trial_balance_cache_file,
                json.dumps(trial_cache),
            )
            trial_cache_write_ok = True
        except OSError as exc:
            trial_cache_write_ok = False
            trial_cache_write_error = str(exc)
    else:
        trial_cache_write_ok = True
        trial_cache_write_skipped = True

    all_needed_tickers = {
        _normalize_text(row.get("market_ticker"))
        for row in canonical_trial_plan_rows.values()
        if _normalize_text(row.get("market_ticker"))
    }
    all_threshold_map = dict(threshold_map)
    if all_needed_tickers:
        for path in reversed(specs_csv_files):
            if not all_needed_tickers:
                break
            try:
                with path.open("r", newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        ticker = _normalize_text(row.get("market_ticker"))
                        if not ticker or ticker not in all_needed_tickers:
                            continue
                        threshold = _normalize_text(row.get("threshold_expression"))
                        if threshold:
                            all_threshold_map[ticker] = threshold
                            all_needed_tickers.discard(ticker)
            except OSError:
                continue

    trial_duplicate_shadow_order_ids = {
        key: int(value)
        for key, value in sorted(
            (
                (_normalize_text(key), _parse_int(value) or 0)
                for key, value in trial_occurrence_counts.items()
            ),
            key=lambda item: item[0],
        )
        if _normalize_text(key) and int(value) > 1
    }
    trial_duplicate_count = int(sum(max(0, value - 1) for value in trial_duplicate_shadow_order_ids.values()))
    trial_duplicate_top_n = 250
    trial_duplicate_total_unique = len(trial_duplicate_shadow_order_ids)
    trial_duplicate_shadow_order_ids_top = {
        key: int(count)
        for key, count in sorted(
            trial_duplicate_shadow_order_ids.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:trial_duplicate_top_n]
    }
    trial_duplicate_truncated_count = max(0, trial_duplicate_total_unique - len(trial_duplicate_shadow_order_ids_top))

    missing_trial_underlyings = {
        _normalize_text(row.get("underlying_key"))
        for row in canonical_trial_plan_rows.values()
        if _normalize_text(row.get("underlying_key"))
        and _normalize_text(row.get("underlying_key")) not in final_truth_by_underlying
    }
    if missing_trial_underlyings:
        trial_truth_by_underlying, settlement_state_files_scanned_for_trial_truth = _latest_settlement_truth_map_from_files(
            settlement_state_files,
            needed_underlyings=missing_trial_underlyings,
        )
        if trial_truth_by_underlying:
            final_truth_by_underlying.update(trial_truth_by_underlying)

    resolved_rows = []
    for row in canonical_trial_plan_rows.values():
        ticker = _normalize_text(row.get("market_ticker"))
        threshold = _normalize_text(all_threshold_map.get(ticker))
        truth = final_truth_by_underlying.get(_normalize_text(row.get("underlying_key")))
        side = _normalize_text(row.get("side")).lower()
        if not threshold or not isinstance(truth, float) or side not in {"yes", "no"}:
            continue
        yes_outcome = _threshold_yes_outcome(threshold, truth)
        if yes_outcome is None:
            continue
        win = (side == "yes" and yes_outcome) or (side == "no" and not yes_outcome)
        entry_price = row.get("entry_price_dollars")
        if not isinstance(entry_price, float):
            fallback_max_entry = approved_max_entry_by_intent_id.get(_normalize_text(row.get("intent_id")))
            entry_price = fallback_max_entry if isinstance(fallback_max_entry, float) else None
        if not isinstance(entry_price, float):
            continue
        entry_price = max(0.01, min(0.99, entry_price))
        contracts_count = max(1, int(_parse_int(row.get("contracts_count")) or 1))
        entry_cost = entry_price * contracts_count
        pnl = ((1.0 - entry_price) if win else (-entry_price)) * contracts_count
        captured_dt = row.get("captured_at_dt")
        if not isinstance(captured_dt, datetime):
            captured_epoch = _parse_float(row.get("captured_at_epoch"))
            if not isinstance(captured_epoch, float):
                captured_epoch = float(reset_epoch)
            captured_dt = datetime.fromtimestamp(captured_epoch, timezone.utc)
        resolved_rows.append(
            {
                "captured_at_dt": captured_dt,
                "pnl": pnl,
                "win": win,
                "entry_price_dollars": float(entry_price),
                "entry_cost_dollars": float(entry_cost),
                "contracts_count": int(contracts_count),
            }
        )

    resolved_rows = sorted(
        resolved_rows,
        key=lambda row: row.get("captured_at_dt") if isinstance(row.get("captured_at_dt"), datetime) else datetime.fromtimestamp(reset_epoch, timezone.utc),
    )

    total_resolved = len(resolved_rows)
    total_wins = sum(1 for row in resolved_rows if row.get("win") is True)
    total_losses = sum(1 for row in resolved_rows if row.get("win") is False)
    cumulative_pnl = sum(float(row.get("pnl") or 0.0) for row in resolved_rows)
    current_balance = start_balance + cumulative_pnl
    win_rate_since_reset = (total_wins / total_resolved) if total_resolved > 0 else None

    end_dt = datetime.fromtimestamp(end_epoch, timezone.utc)
    cash_constrained_rows: list[dict] = []
    cash_balance = float(start_balance)
    cash_min_balance = float(start_balance)
    cash_max_balance = float(start_balance)
    cash_constrained_skipped_count = 0
    for row in resolved_rows:
        entry_cost = _parse_float(row.get("entry_cost_dollars"))
        if not isinstance(entry_cost, float):
            entry_price = _parse_float(row.get("entry_price_dollars"))
            contracts_count = max(1, int(_parse_int(row.get("contracts_count")) or 1))
            entry_cost = (float(entry_price) * contracts_count) if isinstance(entry_price, float) else None
        pnl_value = _parse_float(row.get("pnl"))
        captured_dt = row.get("captured_at_dt")
        if not isinstance(entry_cost, float) or not isinstance(pnl_value, float) or not isinstance(captured_dt, datetime):
            continue
        # Cash-constrained replay: only execute if entry cost can be funded.
        if cash_balance + 1e-9 < entry_cost:
            cash_constrained_skipped_count += 1
            continue
        cash_balance += pnl_value
        cash_min_balance = min(cash_min_balance, cash_balance)
        cash_max_balance = max(cash_max_balance, cash_balance)
        cash_constrained_rows.append(
            {
                "captured_at_dt": captured_dt,
                "pnl": pnl_value,
                "win": bool(row.get("win")),
            }
        )

    constrained_total_resolved = len(cash_constrained_rows)
    constrained_total_wins = sum(1 for row in cash_constrained_rows if row.get("win") is True)
    constrained_total_losses = sum(1 for row in cash_constrained_rows if row.get("win") is False)
    constrained_total_pushes = max(0, constrained_total_resolved - constrained_total_wins - constrained_total_losses)
    constrained_cumulative_pnl = float(cash_balance - start_balance)
    constrained_win_rate = (
        (constrained_total_wins / constrained_total_resolved)
        if constrained_total_resolved > 0
        else None
    )

    def _build_rolling_from_rows(rows: list[dict], *, base_balance: float) -> dict[str, dict]:
        rolling_local: dict[str, dict] = {}
        for label, days in rolling_window_specs:
            cutoff = end_dt.timestamp() - (days * 86400)
            window_rows = [
                row
                for row in rows
                if isinstance(row.get("captured_at_dt"), datetime)
                and row["captured_at_dt"].timestamp() >= cutoff
            ]
            pnl_window = sum(float(row.get("pnl") or 0.0) for row in window_rows)
            wins_window = sum(1 for row in window_rows if row.get("win") is True)
            losses_window = sum(1 for row in window_rows if row.get("win") is False)
            trades_window = len(window_rows)
            rolling_local[label] = {
                "pnl_dollars": round(pnl_window, 6),
                "resolved_trades": trades_window,
                "wins": wins_window,
                "losses": losses_window,
                "win_rate": round((wins_window / trades_window), 6) if trades_window > 0 else None,
                "return_on_starting_balance": round((pnl_window / base_balance), 6) if base_balance > 0 else None,
            }
        return rolling_local

    rolling = {}
    rolling_window_specs = (
        ("1d", 1),
        ("7d", 7),
        ("14d", 14),
        ("21d", 21),
        ("28d", 28),
        ("30d", 30),  # compatibility for existing consumers
        ("3mo", 90),
        ("6mo", 180),
        ("1yr", 365),
    )
    rolling = _build_rolling_from_rows(resolved_rows, base_balance=start_balance)
    rolling_cash_constrained = _build_rolling_from_rows(
        cash_constrained_rows,
        base_balance=start_balance,
    )

    trial_balance = {
        "state_file": str(state_file),
        "starting_balance_dollars": round(start_balance, 6),
        "reset_epoch": reset_epoch,
        "reset_at_utc": reset_at_utc,
        "reset_reason": reset_reason,
        "resolved_counterfactual_trades_since_reset": total_resolved,
        "wins_since_reset": total_wins,
        "losses_since_reset": total_losses,
        "win_rate_since_reset": round(win_rate_since_reset, 6) if isinstance(win_rate_since_reset, float) else None,
        "cumulative_counterfactual_pnl_dollars": round(cumulative_pnl, 6),
        "current_balance_dollars": round(current_balance, 6),
        "growth_since_reset_dollars": round(current_balance - start_balance, 6),
        "growth_since_reset_percent": round(((current_balance - start_balance) / start_balance) * 100.0, 6)
        if start_balance > 0
        else None,
        "counterfactual_mode": "unconstrained_legacy",
        "cash_constrained": {
            "resolved_counterfactual_trades_since_reset": int(constrained_total_resolved),
            "wins_since_reset": int(constrained_total_wins),
            "losses_since_reset": int(constrained_total_losses),
            "pushes_since_reset": int(constrained_total_pushes),
            "win_rate_since_reset": (
                round(constrained_win_rate, 6) if isinstance(constrained_win_rate, float) else None
            ),
            "cumulative_counterfactual_pnl_dollars": round(constrained_cumulative_pnl, 6),
            "current_balance_dollars": round(cash_balance, 6),
            "growth_since_reset_dollars": round(constrained_cumulative_pnl, 6),
            "growth_since_reset_percent": (
                round((constrained_cumulative_pnl / start_balance) * 100.0, 6) if start_balance > 0 else None
            ),
            "skipped_for_insufficient_cash_count": int(cash_constrained_skipped_count),
            "execution_rate_vs_unconstrained": (
                round((constrained_total_resolved / total_resolved), 6) if total_resolved > 0 else None
            ),
            "min_balance_dollars": round(cash_min_balance, 6),
            "max_balance_dollars": round(cash_max_balance, 6),
            "windows": rolling_cash_constrained,
        },
        "planned_rows_since_reset": int(trial_planned_rows_total),
        "unique_shadow_orders_since_reset": len(canonical_trial_plan_rows),
        "duplicate_shadow_order_ids": trial_duplicate_shadow_order_ids_top,
        "duplicate_shadow_order_ids_total_unique": trial_duplicate_total_unique,
        "duplicate_shadow_order_ids_returned": len(trial_duplicate_shadow_order_ids_top),
        "duplicate_shadow_order_ids_truncated": trial_duplicate_truncated_count > 0,
        "duplicate_shadow_order_ids_truncated_count": trial_duplicate_truncated_count,
        "duplicate_shadow_order_ids_top_n_limit": trial_duplicate_top_n,
        "duplicate_count": trial_duplicate_count,
        "cache_file": str(trial_balance_cache_file),
        "cache_status": trial_cache_status,
        "cache_load_status": trial_cache_load_status,
        "cache_write_ok": bool(trial_cache_write_ok),
        "cache_write_error": trial_cache_write_error or None,
        "cache_write_skipped": bool(trial_cache_write_skipped),
        "cache_dirty": bool(trial_cache_dirty),
        "cache_files_total": len(parsed_files_cache),
        "cache_files_parsed_this_run": int(files_parsed_this_run),
        "cache_files_skipped_this_run": int(files_skipped_via_cache),
        "cache_rows_parsed_this_run": int(rows_parsed_this_run),
        "cache_needs_full_rebuild": bool(trial_cache_needs_full_rebuild),
        "cache_rebuild_reason": trial_cache_rebuild_reason or None,
        "cache_processed_files_order_count": len(files_processed_order),
        "settlement_state_files_scanned_for_truth": int(
            settlement_state_files_scanned_for_shadow_truth + settlement_state_files_scanned_for_trial_truth
        ),
        "settlement_state_files_scanned_for_shadow_truth": int(settlement_state_files_scanned_for_shadow_truth),
        "settlement_state_files_scanned_for_trial_truth": int(settlement_state_files_scanned_for_trial_truth),
        "windows": rolling,
        "notes": [
            "Counterfactual balance uses unique shadow_order_id rows resolved against final_truth_value and entry price.",
            "cash_constrained subsection simulates no-leverage execution and skips trades when cash is insufficient.",
            "Reset/refill can be applied via: python -m betbot.cli kalshi-temperature-refill-trial-balance --output-dir <OUT>.",
        ],
    }

    csv_parse_cache_commit_ok = False
    if csv_parse_cache_conn is not None:
        try:
            csv_parse_cache_conn.commit()
            csv_parse_cache_commit_ok = True
        except sqlite3.Error:
            csv_parse_cache_commit_ok = False
        finally:
            try:
                csv_parse_cache_conn.close()
            except sqlite3.Error:
                pass
    csv_parse_cache_stats["commit_ok"] = bool(csv_parse_cache_commit_ok)
    csv_parse_cache_stats["file"] = str(csv_parse_cache_file)

    settled_default_layer = settled_shadow_attr_unique_market_side
    settled_default_worst_policy = _top_contributors_by_pnl(
        settled_default_layer.get("by_policy_reason", {}),
        direction="worst",
        top_n=1,
    )
    settled_default_worst_station = _top_contributors_by_pnl(
        settled_default_layer.get("by_station", {}),
        direction="worst",
        top_n=1,
    )
    primary_policy_leak = settled_default_worst_policy[0]["key"] if settled_default_worst_policy else ""
    primary_station_leak = settled_default_worst_station[0]["key"] if settled_default_worst_station else ""

    return {
        "expected_shadow": expected_shadow,
        "model_lineage": model_lineage,
        "approval_parameter_audit": approval_parameter_audit,
        "shadow_settled_reference": shadow_settled_reference,
        # Backward-compatible alias.
        "counterfactual_live_from_selections": shadow_settled_reference,
        "realized_settled": realized_settled,
        "expected_vs_realized": expected_vs_realized,
        "csv_parse_cache": csv_parse_cache_stats,
        "trial_balance": trial_balance,
        "attribution": {
            "by_station": _finalize_slice_buckets(by_station),
            "by_local_hour": _finalize_slice_buckets(by_local_hour),
            "by_signal_type": _finalize_slice_buckets(by_signal_type),
            "by_policy_reason": _finalize_slice_buckets(by_policy_reason),
            "by_underlying_family": _finalize_slice_buckets(by_underlying_family),
        },
        "settled_shadow_attribution": {
            "default_prediction_quality_basis": "unique_market_side",
            "layers": {
                "row_based": settled_shadow_attr_rows,
                "unique_shadow_order": settled_shadow_attr_unique_order,
                "unique_market_side": settled_shadow_attr_unique_market_side,
            },
            "unknown_context_rows": {
                "row_based": int(settled_shadow_unknown_context_rows),
                "unique_shadow_order": int(settled_shadow_unknown_context_unique_orders),
                "unique_market_side": int(settled_shadow_unknown_context_unique_market_sides),
            },
            "top_contributors": settled_shadow_top_contributors,
            "loss_leakage_summary": {
                "primary_policy_reason_leak": primary_policy_leak or None,
                "primary_station_leak": primary_station_leak or None,
                "resolved_unique_market_sides": int(unique_market_side_metrics["resolved_count"]),
                "resolved_unique_shadow_orders": int(unique_shadow_order_metrics["resolved_count"]),
                "repeat_multiplier_rows_over_unique_market_side": (
                    round((row_metrics["resolved_count"] / unique_market_side_metrics["resolved_count"]), 6)
                    if int(unique_market_side_metrics["resolved_count"]) > 0
                    else None
                ),
            },
        },
        "top_settled_orders": sorted(
            settled_rows,
            key=lambda row: abs(_parse_float(row.get("realized_pnl_dollars")) or 0.0),
            reverse=True,
        )[:20],
    }


def _files_in_window(out_dir: Path, pattern: str, start_epoch: float, end_epoch: float):
    files = []
    # Window endpoints are second-resolution in callers while filesystem mtimes
    # can include sub-second fractions. Treat the end second as inclusive by
    # accepting mtimes up to (end_epoch + 1s) to avoid dropping files created
    # within the same terminal second.
    end_epoch_inclusive = float(end_epoch) + 1.0
    for p in out_dir.glob(pattern):
        try:
            m = p.stat().st_mtime
        except OSError:
            continue
        if float(start_epoch) <= m < end_epoch_inclusive:
            files.append((m, p))
    files.sort(key=lambda x: x[0])
    return [p for _, p in files]


def _latest_settlement_truth_map_from_files(
    settlement_state_files: list[Path],
    *,
    needed_underlyings: set[str] | None = None,
) -> tuple[dict[str, float], int]:
    if not settlement_state_files:
        return {}, 0

    remaining = {_normalize_text(key) for key in (needed_underlyings or set()) if _normalize_text(key)}
    truth_map: dict[str, float] = {}
    files_scanned = 0
    for path in reversed(settlement_state_files):
        files_scanned += 1
        payload = _load(path)
        underlyings = payload.get("underlyings") if isinstance(payload.get("underlyings"), dict) else {}
        for key, entry in underlyings.items():
            if not isinstance(entry, dict):
                continue
            key_text = _normalize_text(key)
            if not key_text:
                continue
            if remaining and key_text not in remaining:
                continue
            if key_text in truth_map:
                continue
            truth = _parse_float(entry.get("final_truth_value"))
            if isinstance(truth, float):
                truth_map[key_text] = float(truth)
                if remaining:
                    remaining.discard(key_text)
        if needed_underlyings is not None and not remaining:
            break
    return truth_map, files_scanned


def _find_previous_window_summary(out_path: Path, window_tag: str) -> tuple[Path | None, dict]:
    directory = out_path.parent
    if not directory.exists():
        return None, {}
    candidates: list[tuple[float, Path]] = []
    pattern = f"station_tuning_window_{window_tag}_*.json"
    for path in directory.glob(pattern):
        if path == out_path or path.name.endswith("_latest.json"):
            continue
        try:
            mtime = float(path.stat().st_mtime)
        except OSError:
            continue
        candidates.append((mtime, path))
    candidates.sort(key=lambda row: row[0], reverse=True)
    for _, path in candidates:
        payload = _load(path)
        if isinstance(payload, dict):
            return path, payload
    return None, {}


def _window_snapshot(summary_payload: dict) -> dict:
    totals = summary_payload.get("totals") if isinstance(summary_payload.get("totals"), dict) else {}
    rates = summary_payload.get("rates") if isinstance(summary_payload.get("rates"), dict) else {}
    profitability = (
        summary_payload.get("profitability_overview")
        if isinstance(summary_payload.get("profitability_overview"), dict)
        else {}
    )
    shadow = (
        profitability.get("shadow_settled_reference")
        if isinstance(profitability.get("shadow_settled_reference"), dict)
        else {}
    )
    trial = profitability.get("trial_balance") if isinstance(profitability.get("trial_balance"), dict) else {}
    trial_windows = trial.get("windows") if isinstance(trial.get("windows"), dict) else {}
    trial_1d = trial_windows.get("1d") if isinstance(trial_windows.get("1d"), dict) else {}
    trial_7d = trial_windows.get("7d") if isinstance(trial_windows.get("7d"), dict) else {}
    return {
        "intents_total": _parse_int(totals.get("intents_total")),
        "intents_approved": _parse_int(totals.get("intents_approved")),
        "planned_orders_total": _parse_int(totals.get("planned_orders_total")),
        "approval_rate": _parse_float(rates.get("approval_rate")),
        "stale_block_rate": _parse_float(rates.get("stale_block_rate")),
        "resolved_unique_market_sides": _parse_int(shadow.get("resolved_unique_market_sides")),
        "resolved_unique_shadow_orders": _parse_int(shadow.get("resolved_unique_shadow_orders")),
        "counterfactual_pnl_unique_market_sides": _parse_float(
            shadow.get("counterfactual_pnl_total_unique_market_sides_dollars_if_live")
        ),
        "trial_balance_current": _parse_float(trial.get("current_balance_dollars")),
        "trial_balance_growth_since_reset": _parse_float(trial.get("growth_since_reset_dollars")),
        "trial_window_1d_pnl": _parse_float(trial_1d.get("pnl_dollars")),
        "trial_window_7d_pnl": _parse_float(trial_7d.get("pnl_dollars")),
    }


def _build_delta_metric_int(current_value, previous_value) -> dict:
    current = _parse_int(current_value)
    previous = _parse_int(previous_value)
    delta = None
    delta_percent = None
    if isinstance(current, int) and isinstance(previous, int):
        delta = current - previous
        if previous != 0:
            delta_percent = (current - previous) / float(previous)
    return {
        "current": current,
        "previous": previous,
        "delta": delta,
        "delta_percent": _to_json_number(delta_percent),
    }


def _build_delta_metric_float(current_value, previous_value, *, ratio: bool = False) -> dict:
    current = _parse_float(current_value)
    previous = _parse_float(previous_value)
    delta = None
    delta_percent = None
    delta_percentage_points = None
    if isinstance(current, float) and isinstance(previous, float):
        delta = current - previous
        if abs(previous) > 1e-12:
            delta_percent = (current - previous) / previous
        if ratio:
            delta_percentage_points = (current - previous) * 100.0
    payload = {
        "current": _to_json_number(current),
        "previous": _to_json_number(previous),
        "delta": _to_json_number(delta),
        "delta_percent": _to_json_number(delta_percent),
    }
    if ratio:
        payload["delta_percentage_points"] = _to_json_number(delta_percentage_points)
    return payload


def _build_window_comparison_payload(
    *,
    current_payload: dict,
    previous_payload: dict,
    previous_path: Path | None,
) -> dict:
    if not previous_payload or not isinstance(previous_payload, dict):
        return {
            "has_previous": False,
            "comparison_basis": "previous_same_label_summary",
            "previous_file": "",
            "previous_captured_at": "",
            "current_captured_at": _normalize_text(current_payload.get("captured_at")),
            "metrics": {},
        }

    current = _window_snapshot(current_payload)
    previous = _window_snapshot(previous_payload)
    return {
        "has_previous": True,
        "comparison_basis": "previous_same_label_summary",
        "previous_file": str(previous_path) if isinstance(previous_path, Path) else "",
        "previous_captured_at": _normalize_text(previous_payload.get("captured_at")),
        "current_captured_at": _normalize_text(current_payload.get("captured_at")),
        "metrics": {
            "intents_total": _build_delta_metric_int(current.get("intents_total"), previous.get("intents_total")),
            "intents_approved": _build_delta_metric_int(
                current.get("intents_approved"),
                previous.get("intents_approved"),
            ),
            "planned_orders_total": _build_delta_metric_int(
                current.get("planned_orders_total"),
                previous.get("planned_orders_total"),
            ),
            "approval_rate": _build_delta_metric_float(
                current.get("approval_rate"),
                previous.get("approval_rate"),
                ratio=True,
            ),
            "stale_block_rate": _build_delta_metric_float(
                current.get("stale_block_rate"),
                previous.get("stale_block_rate"),
                ratio=True,
            ),
            "resolved_unique_market_sides": _build_delta_metric_int(
                current.get("resolved_unique_market_sides"),
                previous.get("resolved_unique_market_sides"),
            ),
            "resolved_unique_shadow_orders": _build_delta_metric_int(
                current.get("resolved_unique_shadow_orders"),
                previous.get("resolved_unique_shadow_orders"),
            ),
            "counterfactual_pnl_total_unique_market_sides_dollars_if_live": _build_delta_metric_float(
                current.get("counterfactual_pnl_unique_market_sides"),
                previous.get("counterfactual_pnl_unique_market_sides"),
            ),
            "trial_balance_current_dollars": _build_delta_metric_float(
                current.get("trial_balance_current"),
                previous.get("trial_balance_current"),
            ),
            "trial_balance_growth_since_reset_dollars": _build_delta_metric_float(
                current.get("trial_balance_growth_since_reset"),
                previous.get("trial_balance_growth_since_reset"),
            ),
            "trial_window_1d_pnl_dollars": _build_delta_metric_float(
                current.get("trial_window_1d_pnl"),
                previous.get("trial_window_1d_pnl"),
            ),
            "trial_window_7d_pnl_dollars": _build_delta_metric_float(
                current.get("trial_window_7d_pnl"),
                previous.get("trial_window_7d_pnl"),
            ),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--start-epoch", type=float, required=True)
    ap.add_argument("--end-epoch", type=float, default=time.time())
    ap.add_argument("--label", default="window")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    start_epoch = float(args.start_epoch)
    end_epoch = float(args.end_epoch)

    intents_files = _files_in_window(out_dir, "kalshi_temperature_trade_intents_summary_*.json", start_epoch, end_epoch)
    plan_files = _files_in_window(out_dir, "kalshi_temperature_trade_plan_summary_*.json", start_epoch, end_epoch)
    shadow_files = _files_in_window(out_dir, "kalshi_temperature_shadow_watch_summary_*.json", start_epoch, end_epoch)
    metar_files = _files_in_window(out_dir, "kalshi_temperature_metar_summary_*.json", start_epoch, end_epoch)
    settlement_files = _files_in_window(out_dir, "kalshi_temperature_settlement_state_*.json", start_epoch, end_epoch)
    micro_execute_files = _files_in_window(out_dir, "kalshi_micro_execute_summary_*.json", start_epoch, end_epoch)

    window_summary_cache_file = out_dir / "checkpoints" / "window_summary_metrics_cache.json"
    window_summary_cache, window_summary_cache_load_status = _load_window_summary_cache(window_summary_cache_file)
    window_summary_cache_files = (
        window_summary_cache.get("files")
        if isinstance(window_summary_cache.get("files"), dict)
        else {}
    )
    window_summary_cache_files = dict(window_summary_cache_files)
    window_summary_cache_dirty = window_summary_cache_load_status != "warm_start"
    window_summary_cache_hits = 0
    window_summary_cache_misses = 0
    window_summary_cache_parse_errors = 0
    window_summary_cache_key_set_touched: set[str] = set()

    def _cached_metrics(path: Path, kind: str) -> dict:
        nonlocal window_summary_cache_dirty
        nonlocal window_summary_cache_hits
        nonlocal window_summary_cache_misses
        nonlocal window_summary_cache_parse_errors
        try:
            stat_result = path.stat()
            mtime = float(stat_result.st_mtime)
            size = int(stat_result.st_size)
        except OSError:
            return {}
        key = _cache_path_key(path)
        window_summary_cache_key_set_touched.add(key)
        cached = window_summary_cache_files.get(key)
        if (
            isinstance(cached, dict)
            and _normalize_text(cached.get("kind")) == kind
            and isinstance(_parse_float(cached.get("mtime")), float)
            and isinstance(_parse_int(cached.get("size")), int)
            and abs(float(_parse_float(cached.get("mtime")) or 0.0) - mtime) <= 1e-9
            and int(_parse_int(cached.get("size")) or 0) == size
            and isinstance(cached.get("metrics"), dict)
        ):
            window_summary_cache_hits += 1
            return dict(cached.get("metrics") or {})
        window_summary_cache_misses += 1
        payload = _load(path)
        if not isinstance(payload, dict):
            window_summary_cache_parse_errors += 1
            payload = {}
        metrics = _extract_window_summary_metrics(kind, payload)
        window_summary_cache_files[key] = {
            "kind": kind,
            "mtime": mtime,
            "size": size,
            "metrics": metrics,
        }
        window_summary_cache_dirty = True
        return metrics

    totals = {
        "intents_total": 0,
        "intents_approved": 0,
        "intents_revalidated": 0,
        "revalidation_invalidated": 0,
        "planned_orders_total": 0,
    }
    reason_counts: Counter[str] = Counter()
    intent_status_counts: Counter[str] = Counter()
    shadow_status_counts: Counter[str] = Counter()
    settlement_loaded_false = 0
    intent_metrics_rows: list[dict] = []

    for p in intents_files:
        d = _cached_metrics(p, "intents_summary")
        intent_metrics_rows.append(dict(d))
        totals["intents_total"] += int(d.get("intents_total") or 0)
        totals["intents_approved"] += int(d.get("intents_approved") or 0)
        totals["intents_revalidated"] += int(d.get("intents_revalidated") or 0)
        totals["revalidation_invalidated"] += int(d.get("revalidation_invalidated") or 0)
        intent_status_counts[str(d.get("status") or "unknown")] += 1
        if not _parse_bool(d.get("settlement_state_loaded")):
            settlement_loaded_false += 1
        for k, v in (d.get("policy_reason_counts") or {}).items():
            reason_counts[str(k)] += int(v or 0)

    for p in plan_files:
        d = _cached_metrics(p, "plan_summary")
        totals["planned_orders_total"] += int(d.get("planned_orders") or 0)

    for p in shadow_files:
        d = _cached_metrics(p, "shadow_summary")
        for k, v in (d.get("cycle_status_counts") or {}).items():
            shadow_status_counts[str(k)] += int(v or 0)

    stale_window_cache_keys = set(window_summary_cache_files.keys()) - window_summary_cache_key_set_touched
    if stale_window_cache_keys:
        for key in stale_window_cache_keys:
            window_summary_cache_files.pop(key, None)
        window_summary_cache_dirty = True

    window_summary_cache_write_ok = True
    window_summary_cache_write_skipped = False
    if window_summary_cache_dirty:
        try:
            _write_text_atomic(
                window_summary_cache_file,
                json.dumps(
                    {
                        "version": WINDOW_SUMMARY_CACHE_VERSION,
                        "files": window_summary_cache_files,
                        "updated_at_epoch": float(time.time()),
                    }
                ),
            )
        except OSError:
            window_summary_cache_write_ok = False
    else:
        window_summary_cache_write_skipped = True

    stale = int(reason_counts.get("metar_observation_stale", 0)) + int(
        reason_counts.get("metar_freshness_boundary_quality_insufficient", 0)
    )
    tot = int(totals["intents_total"])
    approved = int(totals["intents_approved"])

    window_duration_seconds = max(0.0, end_epoch - start_epoch)
    window_duration_hours = round(window_duration_seconds / 3600.0, 4)
    if abs(window_duration_hours - round(window_duration_hours)) < 1e-6:
        rolling_label = f"rolling_{int(round(window_duration_hours))}h"
    else:
        rolling_label = f"rolling_{window_duration_hours}h"

    payload = {
        "captured_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "label": args.label,
        "out_dir": str(out_dir),
        "window": {
            "start_epoch": start_epoch,
            "end_epoch": end_epoch,
            "duration_minutes": round((end_epoch - start_epoch) / 60.0, 2),
        },
        "window_semantics": {
            "type": "rolling",
            "rolling_label": rolling_label,
            "is_calendar_day": False,
        },
        "files_count": {
            "intents": len(intents_files),
            "plans": len(plan_files),
            "shadow": len(shadow_files),
            "metar": len(metar_files),
            "settlement": len(settlement_files),
        },
        "totals": totals,
        "rates": {
            "approval_rate": round((approved / tot), 4) if tot else 0.0,
            "stale_block_rate": round((stale / tot), 4) if tot else 0.0,
        },
        "policy_reason_counts": dict(reason_counts),
        "intent_status_counts": dict(intent_status_counts),
        "shadow_cycle_status_counts": dict(shadow_status_counts),
        "settlement_state_loaded_false_files": settlement_loaded_false,
        "latest_files": {
            "intents": intents_files[-1].name if intents_files else "",
            "plans": plan_files[-1].name if plan_files else "",
            "shadow": shadow_files[-1].name if shadow_files else "",
            "metar": metar_files[-1].name if metar_files else "",
            "settlement": settlement_files[-1].name if settlement_files else "",
        },
        "summary_metrics_cache": {
            "file": str(window_summary_cache_file),
            "load_status": window_summary_cache_load_status,
            "hits": int(window_summary_cache_hits),
            "misses": int(window_summary_cache_misses),
            "parse_errors": int(window_summary_cache_parse_errors),
            "entries": len(window_summary_cache_files),
            "stale_entries_removed": len(stale_window_cache_keys),
            "write_ok": bool(window_summary_cache_write_ok),
            "write_skipped": bool(window_summary_cache_write_skipped),
            "dirty": bool(window_summary_cache_dirty),
        },
    }

    out_path = Path(args.output)
    window_tag = _extract_window_tag(args.label)
    timestamp_token = _extract_timestamp_token(out_path.name)
    profitability_payload = _compute_profitability_payload(
        out_dir=out_dir,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
    )
    git_meta = _git_provenance(out_dir)
    policy_meta = _policy_provenance(
        intents_files=intents_files,
        out_dir=out_dir,
        intents_metrics=intent_metrics_rows,
    )
    run_meta = _run_provenance(
        intents_files=intents_files,
        shadow_files=shadow_files,
        micro_execute_files=micro_execute_files,
    )
    profitability_payload.update(
        {
            "captured_at": payload["captured_at"],
            "label": args.label,
            "window_tag": window_tag,
            "window": payload["window"],
            "window_semantics": payload["window_semantics"],
            "out_dir": str(out_dir),
            "provenance": {
                "git": git_meta,
                "policy": policy_meta,
                "run": run_meta,
            },
        }
    )
    profitability_path = out_path.parent / f"profitability_{window_tag}_{timestamp_token}.json"
    _write_text_atomic(
        profitability_path,
        json.dumps(profitability_payload, indent=2),
    )

    payload["profitability_file"] = str(profitability_path)
    payload["profitability_overview"] = {
        "expected_shadow": profitability_payload.get("expected_shadow", {}),
        "approval_parameter_audit": profitability_payload.get("approval_parameter_audit", {}),
        "shadow_settled_reference": profitability_payload.get("shadow_settled_reference", {}),
        "realized_settled": profitability_payload.get("realized_settled", {}),
        "expected_vs_realized": profitability_payload.get("expected_vs_realized", {}),
        "csv_parse_cache": profitability_payload.get("csv_parse_cache", {}),
        "trial_balance": profitability_payload.get("trial_balance", {}),
        "attribution": profitability_payload.get("attribution", {}),
        "settled_shadow_attribution": profitability_payload.get("settled_shadow_attribution", {}),
    }

    previous_summary_path, previous_summary_payload = _find_previous_window_summary(out_path, window_tag)
    payload["window_comparison"] = _build_window_comparison_payload(
        current_payload=payload,
        previous_payload=previous_summary_payload,
        previous_path=previous_summary_path,
    )

    _write_text_atomic(
        out_path,
        json.dumps(payload, indent=2),
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
