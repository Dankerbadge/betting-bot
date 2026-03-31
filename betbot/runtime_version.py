from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any


def _parse_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).isoformat()
    return value.astimezone(timezone.utc).isoformat()


def _git_stdout(args: list[str], *, cwd: Path | None = None) -> str | None:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(cwd) if cwd else None,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    text = (proc.stdout or "").strip()
    return text or None


def collect_git_identity(*, cwd: str | Path | None = None) -> dict[str, Any]:
    git_cwd = Path(cwd) if cwd else None
    sha = _git_stdout(["rev-parse", "--verify", "HEAD"], cwd=git_cwd)
    branch = _git_stdout(["rev-parse", "--abbrev-ref", "HEAD"], cwd=git_cwd)
    dirty_text = _git_stdout(["status", "--porcelain", "--untracked-files=no"], cwd=git_cwd)
    if dirty_text is None and sha is None:
        dirty = None
    else:
        dirty = bool(dirty_text)
    return {
        "git_sha": sha or "unknown",
        "git_branch": branch or "unknown",
        "git_dirty": dirty,
    }


def file_sha256(path: str | Path | None) -> str | None:
    if not path:
        return None
    try:
        payload = Path(path).read_bytes()
    except OSError:
        return None
    return hashlib.sha256(payload).hexdigest()


def canonical_json_sha256(payload: dict[str, Any]) -> str | None:
    try:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def file_mtime_utc(path: str | Path | None) -> str | None:
    if not path:
        return None
    target = Path(path)
    if not target.exists():
        return None
    try:
        mtime = datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return None
    return _iso_utc(mtime)


def file_age_seconds(path: str | Path | None, *, as_of: datetime | None = None) -> float | None:
    if not path:
        return None
    target = Path(path)
    if not target.exists():
        return None
    now_dt = as_of.astimezone(timezone.utc) if isinstance(as_of, datetime) else datetime.now(timezone.utc)
    try:
        mtime = datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return None
    return round(max(0.0, (now_dt - mtime).total_seconds()), 3)


def load_json_object(path: str | Path | None) -> dict[str, Any] | None:
    if not path:
        return None
    target = Path(path)
    if not target.exists():
        return None
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def infer_fill_model_mode(
    *,
    attempts: list[dict[str, Any]] | None = None,
    prefer_empirical_fill_model: Any = None,
    empirical_fill_enabled: Any = None,
) -> str:
    sources: list[str] = []
    if isinstance(attempts, list):
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            source = str(attempt.get("execution_fill_probability_source") or "").strip().lower()
            if source:
                sources.append(source)
    if any(source.startswith("empirical_primary") for source in sources):
        return "empirical_primary"
    if any(source.startswith("blended_") or source == "blended_empirical" for source in sources):
        return "empirical_blend"
    if bool(empirical_fill_enabled) and bool(prefer_empirical_fill_model):
        return "empirical_blend"
    return "heuristic_only"


def detect_weather_model_tags(rows: list[dict[str, Any]] | None) -> dict[str, str | None]:
    rain_model_tag: str | None = None
    temperature_model_tag: str | None = None
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            family = str(row.get("contract_family") or "").strip().lower()
            model_name = str(row.get("model_name") or "").strip() or None
            if family == "daily_rain" and not rain_model_tag:
                rain_model_tag = model_name
            elif family == "daily_temperature" and not temperature_model_tag:
                temperature_model_tag = model_name
            if rain_model_tag and temperature_model_tag:
                break
    return {
        "rain_model_tag": rain_model_tag,
        "temperature_model_tag": temperature_model_tag,
    }


def weather_priors_version(
    *,
    rain_model_tag: str | None = None,
    temperature_model_tag: str | None = None,
) -> str:
    rain = str(rain_model_tag or "none").strip() or "none"
    temp = str(temperature_model_tag or "none").strip() or "none"
    return f"weather_priors::{rain}::{temp}"


def build_frontier_artifact_identity(
    *,
    frontier_artifact_path: str | Path | None,
    frontier_selection_mode: str | None = None,
    as_of: datetime | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path_text = str(frontier_artifact_path) if frontier_artifact_path else None
    target = Path(path_text) if path_text else None
    report_payload = payload if isinstance(payload, dict) else load_json_object(path_text)
    trusted_bucket_count = 0
    untrusted_bucket_count = 0
    artifact_as_of_utc = None
    if isinstance(report_payload, dict):
        trusted_map = report_payload.get("trusted_break_even_edge_by_bucket")
        if isinstance(trusted_map, dict):
            trusted_bucket_count = len(trusted_map)
        trust_map = report_payload.get("bucket_markout_trust_by_bucket")
        if isinstance(trust_map, dict):
            untrusted_bucket_count = sum(
                1
                for item in trust_map.values()
                if not bool((item or {}).get("trusted"))
            )
        artifact_as_of_dt = _parse_timestamp(report_payload.get("captured_at"))
        artifact_as_of_utc = _iso_utc(artifact_as_of_dt)
    if artifact_as_of_utc is None and target and target.exists():
        try:
            artifact_as_of_utc = _iso_utc(datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc))
        except OSError:
            artifact_as_of_utc = None
    now_dt = as_of.astimezone(timezone.utc) if isinstance(as_of, datetime) else datetime.now(timezone.utc)
    artifact_as_of_dt = _parse_timestamp(artifact_as_of_utc)
    artifact_age_seconds = (
        round(max(0.0, (now_dt - artifact_as_of_dt).total_seconds()), 3)
        if isinstance(artifact_as_of_dt, datetime)
        else None
    )
    if path_text and target and target.exists():
        artifact_sha = file_sha256(target)
    elif isinstance(report_payload, dict):
        artifact_sha = canonical_json_sha256(report_payload)
    else:
        artifact_sha = None
    return {
        "frontier_artifact_path": path_text,
        "frontier_artifact_sha256": artifact_sha,
        "frontier_artifact_as_of_utc": artifact_as_of_utc,
        "frontier_artifact_age_seconds": artifact_age_seconds,
        "frontier_selection_mode": frontier_selection_mode or None,
        "frontier_trusted_bucket_count": trusted_bucket_count,
        "frontier_untrusted_bucket_count": untrusted_bucket_count,
    }


def build_runtime_version_block(
    *,
    run_started_at: datetime | str | None = None,
    run_id: str | None = None,
    git_cwd: str | Path | None = None,
    rain_model_tag: str | None = None,
    temperature_model_tag: str | None = None,
    fill_model_mode: str | None = None,
    prefer_empirical_fill_model: Any = None,
    weather_priors_version_name: str | None = None,
    frontier_artifact_path: str | Path | None = None,
    frontier_selection_mode: str | None = None,
    frontier_payload: dict[str, Any] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    if isinstance(run_started_at, datetime):
        run_started_at_utc = _iso_utc(run_started_at)
    else:
        run_started_at_utc = _iso_utc(_parse_timestamp(run_started_at))
    git_identity = collect_git_identity(cwd=git_cwd)
    frontier_identity = build_frontier_artifact_identity(
        frontier_artifact_path=frontier_artifact_path,
        frontier_selection_mode=frontier_selection_mode,
        payload=frontier_payload,
        as_of=as_of,
    )
    return {
        **git_identity,
        "run_id": run_id or None,
        "run_started_at_utc": run_started_at_utc,
        "rain_model_tag": rain_model_tag or None,
        "temperature_model_tag": temperature_model_tag or None,
        "fill_model_mode": fill_model_mode or None,
        "prefer_empirical_fill_model": (
            bool(prefer_empirical_fill_model) if isinstance(prefer_empirical_fill_model, (bool, int)) else None
        ),
        "weather_priors_version": weather_priors_version_name or None,
        **frontier_identity,
    }
