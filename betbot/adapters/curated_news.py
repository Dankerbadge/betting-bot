from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.adapters.base import AdapterContext
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class CuratedNewsAdapter:
    provider: str = "curated_news"

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
    def _latest_auto_priors_summary(output_dir: Path) -> dict[str, Any] | None:
        candidates = sorted(
            output_dir.glob("kalshi_nonsports_auto_priors_summary_*.json"),
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
        payload = self._latest_auto_priors_summary(Path(context.output_dir))
        if payload is None:
            return SourceResult(
                provider=self.provider,
                status="failed",
                payload={"summary_file": "", "status": "missing"},
                coverage_ratio=0.0,
                stale_seconds=None,
                warnings=[],
                errors=["missing_auto_priors_summary"],
                failed_components=["news_auto_priors"],
                recovery_recommendation="Run kalshi-nonsports-auto-priors to refresh curated evidence.",
            )

        now_utc = self._parse_ts(context.now_iso)
        captured_at = self._parse_ts(payload.get("captured_at"))
        stale_seconds: float | None = None
        if isinstance(now_utc, datetime) and isinstance(captured_at, datetime):
            stale_seconds = max(0.0, (now_utc - captured_at).total_seconds())

        generated_priors = int(self._parse_float(payload.get("generated_priors")) or 0)
        candidate_markets = int(self._parse_float(payload.get("candidate_markets")) or 0)
        skipped_markets = int(self._parse_float(payload.get("skipped_markets")) or 0)
        source_status = str(payload.get("status") or "").strip().lower()

        coverage_ratio = 0.0
        if candidate_markets > 0:
            coverage_ratio = min(1.0, max(0.0, generated_priors / float(candidate_markets)))

        status = "degraded"
        warnings: list[str] = []
        errors: list[str] = []
        failed_components: list[str] = []
        recovery_recommendation: str | None = None

        if source_status == "ready" and generated_priors > 0:
            status = "ok"
            coverage_ratio = max(coverage_ratio, 1.0)
        elif source_status in {"no_auto_priors", "empty"}:
            status = "partial"
            warnings.append("news_evidence_low_coverage")
            recovery_recommendation = "Increase source coverage or relax evidence thresholds for refresh cycles."
        else:
            status = "failed"
            errors.append(f"news_{source_status or 'unavailable'}")
            failed_components.append("news_auto_priors")
            recovery_recommendation = "Re-run news auto-priors and inspect source allowlist availability."

        normalized_payload: dict[str, object] = {
            "summary_file": str(payload.get("_source_file") or ""),
            "status": source_status,
            "generated_priors": generated_priors,
            "candidate_markets": candidate_markets,
            "skipped_markets": skipped_markets,
            "top_market_ticker": str(payload.get("top_market_ticker") or ""),
            "top_market_fair_yes_probability": self._parse_float(payload.get("top_market_fair_yes_probability")),
            "top_market_confidence": self._parse_float(payload.get("top_market_confidence")),
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
