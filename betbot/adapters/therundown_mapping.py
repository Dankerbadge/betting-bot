from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.adapters.base import AdapterContext
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class TheRundownMappingAdapter:
    provider: str = "therundown_mapping"

    @staticmethod
    def _parse_ts(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _parse_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _latest_mapping_summary(output_dir: Path) -> dict[str, Any] | None:
        candidates = sorted(
            output_dir.glob("kalshi_mlb_map_summary_*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                payload["_source_file"] = str(path)
                return payload
        return None

    def fetch(self, context: AdapterContext) -> SourceResult[dict[str, object]]:
        payload = self._latest_mapping_summary(Path(context.output_dir))
        if payload is None:
            return SourceResult(
                provider=self.provider,
                status="failed",
                payload={"summary_file": "", "status": "missing"},
                coverage_ratio=0.0,
                stale_seconds=None,
                warnings=[],
                errors=["missing_mapping_summary"],
                failed_components=["therundown_mapping"],
                recovery_recommendation="Run kalshi-mlb-map to rebuild TheRundown mapping artifacts.",
            )

        now_utc = self._parse_ts(context.now_iso)
        captured_at = self._parse_ts(payload.get("captured_at"))
        stale_seconds: float | None = None
        if isinstance(now_utc, datetime) and isinstance(captured_at, datetime):
            stale_seconds = max(0.0, (now_utc - captured_at).total_seconds())

        mapped_rows = int(self._parse_float(payload.get("mapped_rows")) or 0)
        source_status = str(payload.get("status") or "").strip().lower()

        status = "degraded"
        coverage_ratio = 0.0
        warnings: list[str] = []
        errors: list[str] = []
        failed_components: list[str] = []
        recovery_recommendation: str | None = None

        if source_status == "ready" and mapped_rows > 0:
            status = "ok"
            coverage_ratio = 1.0
        elif source_status in {"empty", "stale_empty", "stale_ready"}:
            status = "partial"
            coverage_ratio = 0.5 if mapped_rows > 0 else 0.0
            warnings.append("mapping_partial_or_stale")
            recovery_recommendation = "Refresh TheRundown and Kalshi market pulls to rebuild stronger mapping coverage."
        else:
            status = "failed"
            coverage_ratio = 0.0
            errors.append(f"mapping_{source_status or 'unavailable'}")
            failed_components.append("therundown_mapping")
            recovery_recommendation = "Re-run mapping job and verify upstream API credentials/availability."

        normalized_payload: dict[str, object] = {
            "summary_file": str(payload.get("_source_file") or ""),
            "status": source_status,
            "event_date": str(payload.get("event_date") or ""),
            "mapped_rows": mapped_rows,
            "positive_best_entry_rows": int(self._parse_float(payload.get("positive_best_entry_rows")) or 0),
        }
        return SourceResult(
            provider=self.provider,
            status=status,
            payload=normalized_payload,
            coverage_ratio=coverage_ratio,
            stale_seconds=stale_seconds,
            warnings=warnings,
            errors=errors,
            failed_components=failed_components,
            recovery_recommendation=recovery_recommendation,
        )
