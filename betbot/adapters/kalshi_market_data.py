from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.adapters.base import AdapterContext
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class KalshiMarketDataAdapter:
    provider: str = "kalshi_market_data"

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
    def _coerce_string_list(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            return [piece.strip() for piece in text.split(",") if piece.strip()]
        return []

    @staticmethod
    def _latest_summary(output_dir: Path) -> dict[str, Any] | None:
        patterns = (
            "kalshi_ws_state_collect_summary_*.json",
            "kalshi_ws_state_summary_*.json",
        )
        candidates: list[Path] = []
        for pattern in patterns:
            candidates.extend(output_dir.glob(pattern))
        if not candidates:
            return None
        candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
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
        output_dir = Path(context.output_dir)
        payload = self._latest_summary(output_dir)
        if payload is None:
            return SourceResult(
                provider=self.provider,
                status="failed",
                payload={"summary_file": "", "status": "missing"},
                coverage_ratio=0.0,
                stale_seconds=None,
                warnings=[],
                errors=["missing_ws_state_summary"],
                failed_components=["ws_state_summary"],
                recovery_recommendation="Run kalshi-ws-state-collect to populate market data authority.",
            )

        captured_at = self._parse_ts(context.now_iso)
        source_captured_at = self._parse_ts(payload.get("captured_at"))
        stale_seconds: float | None = None
        if isinstance(captured_at, datetime) and isinstance(source_captured_at, datetime):
            stale_seconds = max(0.0, (captured_at - source_captured_at).total_seconds())

        market_count = int(self._parse_float(payload.get("market_count")) or 0)
        gate_pass = bool(payload.get("gate_pass"))
        source_status = str(payload.get("status") or "").strip().lower()

        status = "degraded"
        coverage_ratio = 0.0
        errors: list[str] = []
        warnings: list[str] = []
        failed_components: list[str] = []
        recovery_recommendation: str | None = None

        if gate_pass and source_status == "ready" and market_count > 0:
            status = "ok"
            coverage_ratio = 1.0
        elif market_count > 0 and source_status in {"ready", "stale"}:
            status = "partial"
            coverage_ratio = 0.6
            warnings.append("market_data_authority_partial")
            recovery_recommendation = "Reduce freshness lag and restore gate_pass to recover full authority."
        elif source_status in {"upstream_error", "invalid", "missing", "desynced", "empty"}:
            status = "failed"
            coverage_ratio = 0.0
            errors.append(f"ws_state_{source_status or 'unavailable'}")
            failed_components.append("ws_state_summary")
            recovery_recommendation = "Retry websocket collection and inspect ws_state authority file."
        else:
            status = "degraded"
            coverage_ratio = 0.2 if market_count > 0 else 0.0
            warnings.append("market_data_degraded")
            recovery_recommendation = "Inspect ws-state summary and reconnect websocket source."

        normalized_payload: dict[str, object] = {
            "summary_file": str(payload.get("_source_file") or ""),
            "status": source_status,
            "gate_pass": gate_pass,
            "market_count": market_count,
            "market_tickers": self._coerce_string_list(payload.get("market_tickers"))[:50],
            "desynced_market_count": int(self._parse_float(payload.get("desynced_market_count")) or 0),
            "last_event_age_seconds": self._parse_float(payload.get("last_event_age_seconds")),
            "websocket_lag_ms": self._parse_float(payload.get("websocket_lag_ms")),
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
