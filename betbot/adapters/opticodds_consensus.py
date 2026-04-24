from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.adapters.base import AdapterContext
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class OpticOddsConsensusAdapter:
    provider: str = "opticodds_consensus"

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
    def _latest_live_candidates_summary(output_dir: Path) -> dict[str, Any] | None:
        candidates = sorted(
            output_dir.glob("live_candidates_summary_*.json"),
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
        payload = self._latest_live_candidates_summary(Path(context.output_dir))
        if payload is None:
            return SourceResult(
                provider=self.provider,
                status="failed",
                payload={"summary_file": "", "status": "missing"},
                coverage_ratio=0.0,
                stale_seconds=None,
                warnings=[],
                errors=["missing_live_candidates_summary"],
                failed_components=["consensus_summary"],
                recovery_recommendation="Run live-candidates to produce consensus depth artifacts.",
            )

        now_utc = self._parse_ts(context.now_iso)
        captured_at = self._parse_ts(payload.get("captured_at"))
        stale_seconds: float | None = None
        if isinstance(now_utc, datetime) and isinstance(captured_at, datetime):
            stale_seconds = max(0.0, (now_utc - captured_at).total_seconds())

        candidates_written = int(self._parse_float(payload.get("candidates_written")) or 0)
        market_pairs_with_consensus = int(self._parse_float(payload.get("market_pairs_with_consensus")) or 0)
        market_pairs_seen = int(self._parse_float(payload.get("market_pairs_seen")) or 0)
        source_status = str(payload.get("status") or "").strip().lower()

        coverage_ratio = 0.0
        if market_pairs_seen > 0:
            coverage_ratio = min(1.0, max(0.0, market_pairs_with_consensus / float(market_pairs_seen)))

        status = "degraded"
        warnings: list[str] = []
        errors: list[str] = []
        failed_components: list[str] = []
        recovery_recommendation: str | None = None

        if source_status == "ready" and candidates_written > 0 and market_pairs_with_consensus > 0:
            status = "ok"
            coverage_ratio = max(coverage_ratio, 1.0)
        elif source_status == "ready" and market_pairs_with_consensus > 0:
            status = "partial"
            warnings.append("consensus_candidates_empty")
            recovery_recommendation = "Increase candidate breadth or lower strictness for consensus selection."
        elif source_status in {"empty", "no_candidates"}:
            status = "degraded"
            warnings.append("consensus_empty")
            recovery_recommendation = "Refresh event window and affiliate set to restore consensus candidates."
        else:
            status = "failed"
            errors.append(f"consensus_{source_status or 'unavailable'}")
            failed_components.append("consensus_summary")
            recovery_recommendation = "Re-run consensus ingestion and validate odds provider connectivity."

        normalized_payload: dict[str, object] = {
            "summary_file": str(payload.get("_source_file") or ""),
            "status": source_status,
            "candidates_written": candidates_written,
            "market_pairs_seen": market_pairs_seen,
            "market_pairs_with_consensus": market_pairs_with_consensus,
            "positive_ev_candidates": int(self._parse_float(payload.get("positive_ev_candidates")) or 0),
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
