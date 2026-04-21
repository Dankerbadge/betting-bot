from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import uuid

from betbot.adapters.base import Adapter, AdapterContext, run_adapter
from betbot.execution.live_executor import (
    LiveExecutionResult,
    LiveExecutor,
    LiveVenueAdapter,
    LocalLiveVenueAdapter,
)
from betbot.execution.ticket import TicketProposal, create_ticket_proposal
from betbot.policy.approvals import ApprovalRecord
from betbot.policy.degraded_mode import DegradedSummary, summarize_source_results
from betbot.policy.engine import PolicyDecision, evaluate_policy_gate
from betbot.policy.lanes import LanePolicySet, load_lane_policy_set
from betbot.runtime.config_loader import load_effective_config
from betbot.runtime.event_store import EventStore
from betbot.runtime.events import EventEnvelope, new_event
from betbot.runtime.projections import build_board_projection, build_cycle_projection
from betbot.runtime.state_machine import RuntimeStateMachine


@dataclass(frozen=True)
class CycleRunnerConfig:
    lane: str = "observe"
    output_dir: str = "outputs"
    event_log_file: str = "runtime_events_latest.jsonl"
    cycle_report_file: str = "cycle_latest.json"
    board_report_file: str = "board_latest.json"
    repo_root: str | None = None
    lane_policy_path: str | None = None
    request_live_submit: bool = False
    live_env_file: str | None = None
    live_timeout_seconds: float = 10.0
    allow_simulated_live_adapter: bool = False
    hard_required_sources: tuple[str, ...] | None = None
    approval_json_path: str | None = None
    ticket_market: str = "SIM-MARKET"
    ticket_side: str = "yes"
    ticket_max_cost: float = 1.0
    ticket_expires_at: str | None = None


_DEFAULT_TICKET_MARKET = "SIM-MARKET"
_DEFAULT_TICKET_SIDE = "yes"
_DEFAULT_TICKET_MAX_COST = 1.0


@dataclass(frozen=True)
class RankedTicketCandidate:
    market: str
    side: str
    max_cost: float
    score: float
    source: str
    rationale: str


class CycleRunner:
    def __init__(
        self,
        *,
        adapters: list[Adapter],
        lane_policy_set: LanePolicySet | None = None,
        hard_required_sources: tuple[str, ...] = (),
        live_venue_adapter: LiveVenueAdapter | None = None,
    ) -> None:
        self.adapters = adapters
        self._injected_lane_policy_set = lane_policy_set
        self.hard_required_sources = tuple(hard_required_sources)
        self._live_venue_adapter = live_venue_adapter

    @staticmethod
    def _source_severity(status: str) -> str:
        if status == "blocked":
            return "block"
        if status == "failed":
            return "error"
        if status in {"partial", "degraded"}:
            return "warn"
        return "info"

    def _resolve_lane_policy_set(
        self,
        *,
        config: CycleRunnerConfig,
        policy_payload: dict[str, object],
    ) -> LanePolicySet:
        if self._injected_lane_policy_set is not None and not config.lane_policy_path:
            return self._injected_lane_policy_set

        lane_policy_path = config.lane_policy_path
        if lane_policy_path is None:
            lane_policy_path = str(policy_payload.get("lane_policy_path") or "").strip() or None
        return load_lane_policy_set(path=lane_policy_path)

    @staticmethod
    def _coerce_source_tuple(raw: object) -> tuple[str, ...]:
        if raw is None:
            return ()
        if isinstance(raw, (list, tuple, set)):
            return tuple(str(item).strip() for item in raw if str(item).strip())
        return ()

    def _resolve_hard_required_sources(
        self,
        *,
        config: CycleRunnerConfig,
        policy_payload: dict[str, object],
    ) -> tuple[str, ...]:
        if config.hard_required_sources is not None:
            return tuple(config.hard_required_sources)
        if self.hard_required_sources:
            return tuple(self.hard_required_sources)

        by_lane_raw = policy_payload.get("hard_required_sources_by_lane")
        if isinstance(by_lane_raw, dict):
            if config.lane in by_lane_raw:
                return self._coerce_source_tuple(by_lane_raw.get(config.lane))

        global_required = self._coerce_source_tuple(policy_payload.get("hard_required_sources"))
        return global_required

    @staticmethod
    def _coerce_bool(value: object, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return default

    def _resolve_approval_required(
        self,
        *,
        policy_payload: dict[str, object],
        lane: str,
    ) -> bool:
        by_lane = policy_payload.get("approval_required_by_lane")
        if isinstance(by_lane, dict) and lane in by_lane:
            return self._coerce_bool(by_lane.get(lane), default=True)
        return self._coerce_bool(policy_payload.get("approval_required", True), default=True)

    @staticmethod
    def _resolve_ticket_expires_at(ticket_expires_at: str | None) -> str:
        if ticket_expires_at:
            return str(ticket_expires_at)
        return (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()

    @staticmethod
    def _load_approval_record(approval_json_path: str | None) -> ApprovalRecord | None:
        if not approval_json_path:
            return None
        path = Path(approval_json_path)
        if not path.exists():
            return None
        payload = dict(json.loads(path.read_text(encoding="utf-8")))
        return ApprovalRecord(
            ticket_hash=str(payload.get("ticket_hash") or ""),
            market=str(payload.get("market") or ""),
            side=str(payload.get("side") or ""),
            max_cost=float(payload.get("max_cost") or 0.0),
            issued_at=str(payload.get("issued_at") or ""),
            expires_at=str(payload.get("expires_at") or ""),
            approved_by=str(payload.get("approved_by") or "unknown"),
        )

    @staticmethod
    def _coerce_float(value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _source_payload(
        source_results: dict[str, object],
        provider: str,
    ) -> dict[str, object]:
        raw = getattr(source_results.get(provider), "payload", None)
        if isinstance(raw, dict):
            return dict(raw)
        return {}

    @staticmethod
    def _coerce_market_tickers(value: object) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            return [piece.strip() for piece in text.split(",") if piece.strip()]
        return []

    @staticmethod
    def _ticker_family_key(ticker: str) -> str:
        text = str(ticker).strip().upper()
        if not text:
            return ""
        parts = [piece for piece in text.split("-") if piece]
        if len(parts) >= 3:
            return "-".join(parts[:-1])
        if len(parts) == 2:
            return parts[0] if any(char.isdigit() for char in parts[1]) else text
        return text

    @staticmethod
    def _coldmath_no_side_max_cost(
        *,
        no_outcome_ratio: float | None,
        high_price_no_positions: int,
    ) -> float:
        max_cost = 0.68
        if isinstance(no_outcome_ratio, float):
            max_cost = max(max_cost, 0.55 + (0.45 * max(0.0, min(1.0, no_outcome_ratio))))
        if high_price_no_positions >= 3:
            max_cost = max(max_cost, 0.9)
        if high_price_no_positions >= 8:
            max_cost = max(max_cost, 0.94)
        if high_price_no_positions >= 12:
            max_cost = max(max_cost, 0.97)
        return round(max(0.5, min(0.99, max_cost)), 6)

    @staticmethod
    def _latest_coldmath_snapshot_summary(output_dir: Path) -> dict[str, object] | None:
        candidates: list[Path] = []
        latest_path = output_dir / "coldmath_snapshot_summary_latest.json"
        if latest_path.exists():
            candidates.append(latest_path)
        candidates.extend(output_dir.glob("coldmath_snapshot_summary_*.json"))
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

    @staticmethod
    def _latest_coldmath_replication_plan(output_dir: Path) -> dict[str, object] | None:
        candidates: list[Path] = []
        latest_path = output_dir / "coldmath_replication_plan_latest.json"
        if latest_path.exists():
            candidates.append(latest_path)
        candidates.extend(output_dir.glob("coldmath_replication_plan_*.json"))
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

    def _coldmath_replication_signal(self, output_dir: Path) -> dict[str, object]:
        payload = self._latest_coldmath_snapshot_summary(output_dir)
        if payload is None:
            return {
                "status": "missing",
                "summary_file": "",
                "behavior_tags": [],
                "no_outcome_ratio": None,
                "positions_with_high_price_no": 0,
                "multi_strike_family_count": 0,
                "top_family_key": "",
            }

        family_behavior_raw = payload.get("family_behavior")
        family_behavior = dict(family_behavior_raw) if isinstance(family_behavior_raw, dict) else {}
        behavior_tags_raw = family_behavior.get("behavior_tags")
        behavior_tags = (
            [str(item).strip() for item in behavior_tags_raw if str(item).strip()]
            if isinstance(behavior_tags_raw, list)
            else []
        )
        families_raw = family_behavior.get("families")
        families = list(families_raw) if isinstance(families_raw, list) else []
        top_family_key = ""
        if families and isinstance(families[0], dict):
            top_family_key = str(families[0].get("family_key") or "").strip()

        return {
            "status": str(payload.get("status") or "").strip().lower() or "ready",
            "summary_file": str(payload.get("_source_file") or ""),
            "behavior_tags": behavior_tags,
            "no_outcome_ratio": self._coerce_float(family_behavior.get("no_outcome_ratio")),
            "positions_with_high_price_no": int(self._coerce_float(family_behavior.get("positions_with_high_price_no")) or 0),
            "multi_strike_family_count": int(self._coerce_float(family_behavior.get("multi_strike_family_count")) or 0),
            "top_family_key": top_family_key,
        }

    def _normalize_runtime_snapshots(
        self,
        *,
        source_results: dict[str, object],
        output_dir: Path,
    ) -> dict[str, object]:
        kalshi_payload = self._source_payload(source_results, "kalshi_market_data")
        curated_payload = self._source_payload(source_results, "curated_news")
        consensus_payload = self._source_payload(source_results, "opticodds_consensus")
        coldmath_replication = self._coldmath_replication_signal(output_dir)
        coldmath_replication_plan_raw = self._latest_coldmath_replication_plan(output_dir)
        coldmath_replication_plan = (
            dict(coldmath_replication_plan_raw) if isinstance(coldmath_replication_plan_raw, dict) else {}
        )
        plan_candidates_raw = list(coldmath_replication_plan.get("candidates") or [])
        plan_candidates = [dict(item) for item in plan_candidates_raw[:20] if isinstance(item, dict)]

        market_tickers = self._coerce_market_tickers(kalshi_payload.get("market_tickers"))
        if not market_tickers:
            market_tickers = self._coerce_market_tickers(kalshi_payload.get("market_tickers_preview"))

        news_top_market_ticker = str(curated_payload.get("top_market_ticker") or "").strip()
        news_fair_yes_probability = self._coerce_float(curated_payload.get("top_market_fair_yes_probability"))
        if isinstance(news_fair_yes_probability, float):
            news_fair_yes_probability = max(0.001, min(0.999, news_fair_yes_probability))
        news_confidence = self._coerce_float(curated_payload.get("top_market_confidence"))
        if isinstance(news_confidence, float):
            news_confidence = max(0.0, min(1.0, news_confidence))

        positive_ev_candidates = int(self._coerce_float(consensus_payload.get("positive_ev_candidates")) or 0)
        market_pairs_with_consensus = int(self._coerce_float(consensus_payload.get("market_pairs_with_consensus")) or 0)

        return {
            "market_tickers": market_tickers,
            "market_ticker_count": len(market_tickers),
            "news_top_market_ticker": news_top_market_ticker,
            "news_top_market_fair_yes_probability": news_fair_yes_probability,
            "news_top_market_confidence": news_confidence,
            "consensus_positive_ev_candidates": positive_ev_candidates,
            "consensus_market_pairs_with_consensus": market_pairs_with_consensus,
            "coldmath_replication": coldmath_replication,
            "coldmath_replication_plan": {
                "status": str(coldmath_replication_plan.get("status") or "").strip().lower() or "missing",
                "source_file": str(coldmath_replication_plan.get("_source_file") or ""),
                "theme": str(coldmath_replication_plan.get("theme") or "").strip().lower(),
                "preferred_side": str(coldmath_replication_plan.get("preferred_side") or "").strip().lower(),
                "candidate_count": int(self._coerce_float(coldmath_replication_plan.get("candidate_count")) or len(plan_candidates)),
                "candidates": plan_candidates,
            },
        }

    def _score_ticket_candidates(
        self,
        *,
        normalized_snapshot: dict[str, object],
    ) -> list[RankedTicketCandidate]:
        market_tickers = self._coerce_market_tickers(normalized_snapshot.get("market_tickers"))
        top_market = str(normalized_snapshot.get("news_top_market_ticker") or "").strip()
        fair_yes_probability = self._coerce_float(normalized_snapshot.get("news_top_market_fair_yes_probability"))
        confidence = self._coerce_float(normalized_snapshot.get("news_top_market_confidence"))
        positive_ev_candidates = int(self._coerce_float(normalized_snapshot.get("consensus_positive_ev_candidates")) or 0)
        coldmath_replication_raw = normalized_snapshot.get("coldmath_replication")
        coldmath_replication = (
            dict(coldmath_replication_raw) if isinstance(coldmath_replication_raw, dict) else {}
        )
        coldmath_replication_plan_raw = normalized_snapshot.get("coldmath_replication_plan")
        coldmath_replication_plan = (
            dict(coldmath_replication_plan_raw) if isinstance(coldmath_replication_plan_raw, dict) else {}
        )
        plan_status = str(coldmath_replication_plan.get("status") or "").strip().lower()
        plan_candidates_raw = list(coldmath_replication_plan.get("candidates") or [])
        behavior_tags = {
            str(item).strip().lower()
            for item in list(coldmath_replication.get("behavior_tags") or [])
            if str(item).strip()
        }
        no_outcome_ratio = self._coerce_float(coldmath_replication.get("no_outcome_ratio"))
        high_price_no_positions = int(self._coerce_float(coldmath_replication.get("positions_with_high_price_no")) or 0)
        multi_strike_family_count = int(self._coerce_float(coldmath_replication.get("multi_strike_family_count")) or 0)
        coldmath_no_side_bias = (
            "no_side_bias" in behavior_tags
            or "high_price_no_inventory" in behavior_tags
            or (isinstance(no_outcome_ratio, float) and no_outcome_ratio >= 0.55)
            or high_price_no_positions >= 5
        )
        coldmath_prefers_clustered_families = (
            "multi_strike_clustering" in behavior_tags
            or multi_strike_family_count >= 2
        )
        coldmath_replication_pressure = 0.0
        if "multi_strike_clustering" in behavior_tags:
            coldmath_replication_pressure += 0.03
        if "high_price_no_inventory" in behavior_tags:
            coldmath_replication_pressure += 0.06
        if isinstance(no_outcome_ratio, float):
            coldmath_replication_pressure += max(0.0, min(0.25, (no_outcome_ratio - 0.5) * 0.2))
        coldmath_no_side_max_cost = self._coldmath_no_side_max_cost(
            no_outcome_ratio=no_outcome_ratio,
            high_price_no_positions=high_price_no_positions,
        )

        market_families: dict[str, str] = {
            ticker: self._ticker_family_key(ticker)
            for ticker in market_tickers
        }
        family_counts: dict[str, int] = {}
        for family in market_families.values():
            if not family:
                continue
            family_counts[family] = family_counts.get(family, 0) + 1
        indexed_markets = list(enumerate(market_tickers))
        if coldmath_prefers_clustered_families and family_counts:
            indexed_markets.sort(
                key=lambda row: (
                    -int(family_counts.get(market_families.get(row[1], ""), 0)),
                    int(row[0]),
                )
            )
        ranked_markets = [market for _, market in indexed_markets]
        top_family_key = ""
        if ranked_markets:
            top_family_key = market_families.get(ranked_markets[0], "")

        candidates: list[RankedTicketCandidate] = []
        seen_markets: set[str] = set()
        if plan_status == "ready":
            for row in plan_candidates_raw:
                if not isinstance(row, dict):
                    continue
                market = str(row.get("market") or "").strip()
                if not market:
                    continue
                if market_tickers and market not in market_tickers:
                    continue
                if market in seen_markets:
                    continue
                side = str(row.get("side") or "").strip().lower()
                if side not in {"yes", "no"}:
                    side = str(coldmath_replication_plan.get("preferred_side") or "no").strip().lower()
                    if side not in {"yes", "no"}:
                        side = "no"
                max_cost = self._coerce_float(row.get("max_cost"))
                if not isinstance(max_cost, float):
                    max_cost = coldmath_no_side_max_cost if side == "no" else _DEFAULT_TICKET_MAX_COST
                score = self._coerce_float(row.get("score"))
                if not isinstance(score, float):
                    score = 0.4
                rationale = str(row.get("rationale") or "").strip() or "ColdMath replication plan candidate"
                family_key = str(row.get("family_key") or "").strip()
                if family_key:
                    rationale = f"{rationale} ({family_key})"
                candidates.append(
                    RankedTicketCandidate(
                        market=market,
                        side=side,
                        max_cost=round(max(0.01, min(0.99, float(max_cost))), 6),
                        score=round(max(0.01, min(1.0, float(score))), 6),
                        source="coldmath_replication_plan",
                        rationale=rationale,
                    )
                )
                seen_markets.add(market)

        top_market_in_universe = top_market in market_tickers if top_market else False
        allow_out_of_universe_curated = not market_tickers

        if top_market and (top_market_in_universe or allow_out_of_universe_curated):
            side = _DEFAULT_TICKET_SIDE
            max_cost = _DEFAULT_TICKET_MAX_COST
            if isinstance(fair_yes_probability, float):
                side = "yes" if fair_yes_probability >= 0.5 else "no"
                side_probability = fair_yes_probability if side == "yes" else 1.0 - fair_yes_probability
                max_cost = max(0.01, min(0.99, side_probability))
            if coldmath_no_side_bias and not (isinstance(fair_yes_probability, float) and fair_yes_probability >= 0.72):
                side = "no"
                if isinstance(fair_yes_probability, float):
                    max_cost = max(max_cost, max(0.01, min(0.99, 1.0 - fair_yes_probability)))
                else:
                    max_cost = max(max_cost, coldmath_no_side_max_cost)
                max_cost = max(max_cost, coldmath_no_side_max_cost)
            confidence_score = max(0.0, min(1.0, confidence if isinstance(confidence, float) else 0.35))
            score = 0.45 + (0.35 * confidence_score)
            if top_market_in_universe:
                score += 0.2
            else:
                score -= 0.1
            if positive_ev_candidates > 0:
                score += 0.05
            if coldmath_no_side_bias and side == "no":
                score += coldmath_replication_pressure
            score = max(0.01, min(1.0, score))
            candidates.append(
                RankedTicketCandidate(
                    market=top_market,
                    side=side,
                    max_cost=round(max_cost, 6),
                    score=round(score, 6),
                    source="curated_news",
                    rationale=(
                        "Curated-news prior selected top market ticker"
                        if top_market in market_tickers
                        else "Curated-news prior produced ticker outside current ws-state universe"
                    )
                    + (
                        "; ColdMath replication pressure nudged side toward NO"
                        if coldmath_no_side_bias and side == "no"
                        else ""
                    ),
                )
            )
            seen_markets.add(top_market)

        for index, market in enumerate(ranked_markets[:8]):
            if market in seen_markets:
                continue
            if top_market and market == top_market:
                continue
            fallback_score = 0.3 - (0.03 * index)
            if positive_ev_candidates > 0:
                fallback_score += 0.02
            fallback_side = _DEFAULT_TICKET_SIDE
            fallback_max_cost = _DEFAULT_TICKET_MAX_COST
            fallback_source = "kalshi_market_data"
            fallback_rationale = "Fallback to active Kalshi ws-state market universe"
            if coldmath_no_side_bias:
                fallback_side = "no"
                fallback_source = "coldmath_replication"
                fallback_max_cost = coldmath_no_side_max_cost
                family_key = market_families.get(market, "")
                family_count = int(family_counts.get(family_key, 0))
                family_suffix = (
                    f" ({family_key}, family_size={family_count})"
                    if family_key
                    else ""
                )
                fallback_rationale = (
                    "ColdMath replication signal favors NO-side inventory with multi-strike clustering"
                    + family_suffix
                )
                fallback_score += coldmath_replication_pressure
                if coldmath_prefers_clustered_families and family_key and family_key == top_family_key:
                    fallback_score += 0.04
            fallback_score = max(0.05, min(1.0, fallback_score))
            candidates.append(
                RankedTicketCandidate(
                    market=market,
                    side=fallback_side,
                    max_cost=round(float(fallback_max_cost), 6),
                    score=round(fallback_score, 6),
                    source=fallback_source,
                    rationale=fallback_rationale,
                )
            )
            seen_markets.add(market)

        candidates.sort(key=lambda row: row.score, reverse=True)
        return candidates

    def _resolve_ticket_inputs(
        self,
        *,
        config: CycleRunnerConfig,
        candidates: list[RankedTicketCandidate],
    ) -> tuple[str, str, float, dict[str, object]]:
        explicit_market = str(config.ticket_market).strip()
        explicit_side = str(config.ticket_side).strip().lower()
        explicit_max_cost = float(config.ticket_max_cost)
        if explicit_side not in {"yes", "no"}:
            explicit_side = _DEFAULT_TICKET_SIDE

        if explicit_market and explicit_market != _DEFAULT_TICKET_MARKET:
            return (
                explicit_market,
                explicit_side,
                explicit_max_cost,
                {
                    "selection_source": "config",
                    "selection_score": None,
                    "selection_rationale": "Explicit ticket config provided",
                    "market": explicit_market,
                    "side": explicit_side,
                    "max_cost": explicit_max_cost,
                },
            )

        if candidates:
            best = candidates[0]
            return (
                best.market,
                best.side,
                float(best.max_cost),
                {
                    "selection_source": best.source,
                    "selection_score": best.score,
                    "selection_rationale": best.rationale,
                    "market": best.market,
                    "side": best.side,
                    "max_cost": float(best.max_cost),
                },
            )

        return (
            explicit_market or _DEFAULT_TICKET_MARKET,
            explicit_side,
            explicit_max_cost,
            {
                "selection_source": "default",
                "selection_score": None,
                "selection_rationale": "No scored candidates available",
                "market": explicit_market or _DEFAULT_TICKET_MARKET,
                "side": explicit_side,
                "max_cost": explicit_max_cost,
            },
        )

    def run(self, config: CycleRunnerConfig) -> dict[str, object]:
        effective_config = load_effective_config(repo_root=config.repo_root)
        policy_payload = dict(effective_config.values.get("policy") or {})
        lane_policy_set = self._resolve_lane_policy_set(config=config, policy_payload=policy_payload)

        if not lane_policy_set.is_known_lane(config.lane):
            raise ValueError(f"Unknown permission lane: {config.lane}")

        hard_required_sources = self._resolve_hard_required_sources(
            config=config,
            policy_payload=policy_payload,
        )

        run_id = f"runtime::{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        cycle_id = str(uuid.uuid4())
        state = RuntimeStateMachine()
        events: list[EventEnvelope] = [
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type="cycle_started",
                phase=state.current_phase,
                lane=config.lane,
                data={"adapter_count": len(self.adapters)},
            )
        ]

        output_dir = Path(config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        store = EventStore(output_dir / config.event_log_file)

        state.transition("sources.fetching")
        events.append(
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type="sources_fetching",
                phase=state.current_phase,
                lane=config.lane,
            )
        )

        adapter_context = AdapterContext(
            run_id=run_id,
            cycle_id=cycle_id,
            lane=config.lane,
            now_iso=datetime.now(timezone.utc).isoformat(),
            output_dir=str(output_dir),
        )
        source_results = {adapter.provider: run_adapter(adapter, adapter_context) for adapter in self.adapters}
        missing_required_sources = tuple(
            provider for provider in hard_required_sources if provider not in source_results
        )

        any_partial = bool(missing_required_sources) or any(
            result.status in {"partial", "degraded", "failed", "blocked"}
            for result in source_results.values()
        )
        state.transition("sources.partial" if any_partial else "sources.ready")

        for provider, result in source_results.items():
            events.append(
                new_event(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    event_type="source_result",
                    phase=state.current_phase,
                    lane=config.lane,
                    source=provider,
                    severity=self._source_severity(result.status),
                    data={
                        "status": result.status,
                        "coverage_ratio": result.coverage_ratio,
                        "stale_seconds": result.stale_seconds,
                        "warnings": list(result.warnings),
                        "errors": list(result.errors),
                    },
                )
            )

        for provider in missing_required_sources:
            events.append(
                new_event(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    event_type="source_result",
                    phase=state.current_phase,
                    lane=config.lane,
                    source=provider,
                    severity="block",
                    data={
                        "status": "missing",
                        "coverage_ratio": 0.0,
                        "stale_seconds": None,
                        "warnings": [],
                        "errors": ["required_source_missing"],
                    },
                )
            )

        if state.current_phase == "sources.partial":
            state.transition("sources.ready")

        degraded_summary: DegradedSummary = summarize_source_results(
            source_results=source_results,
            phase=state.current_phase,
            hard_required_sources=hard_required_sources,
            missing_required_sources=missing_required_sources,
        )
        normalized_snapshot = self._normalize_runtime_snapshots(
            source_results=source_results,
            output_dir=output_dir,
        )
        ticket_candidates = self._score_ticket_candidates(normalized_snapshot=normalized_snapshot)
        coldmath_replication = (
            dict(normalized_snapshot.get("coldmath_replication"))
            if isinstance(normalized_snapshot.get("coldmath_replication"), dict)
            else {}
        )
        coldmath_replication_plan = (
            dict(normalized_snapshot.get("coldmath_replication_plan"))
            if isinstance(normalized_snapshot.get("coldmath_replication_plan"), dict)
            else {}
        )
        news_enrichment_summary = {
            "provider_status": degraded_summary.source_statuses.get("curated_news", "missing"),
            "top_market_ticker": normalized_snapshot.get("news_top_market_ticker"),
            "top_market_fair_yes_probability": normalized_snapshot.get("news_top_market_fair_yes_probability"),
            "top_market_confidence": normalized_snapshot.get("news_top_market_confidence"),
            "coldmath_behavior_tags": list(coldmath_replication.get("behavior_tags") or []),
            "coldmath_top_family_key": str(coldmath_replication.get("top_family_key") or ""),
            "coldmath_replication_plan_status": str(coldmath_replication_plan.get("status") or ""),
            "coldmath_replication_plan_theme": str(coldmath_replication_plan.get("theme") or ""),
            "coldmath_replication_plan_candidates": int(
                self._coerce_float(coldmath_replication_plan.get("candidate_count")) or 0
            ),
        }

        state.transition("news.enriching")
        events.append(
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type="news_enriching",
                phase=state.current_phase,
                lane=config.lane,
                severity="warn" if degraded_summary.overall_status == "degraded" else "info",
                data=news_enrichment_summary,
            )
        )

        state.transition("snapshots.normalized")
        events.append(
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type="snapshots_normalized",
                phase=state.current_phase,
                lane=config.lane,
                severity="info",
                data={
                    "market_ticker_count": normalized_snapshot.get("market_ticker_count"),
                    "news_top_market_ticker": normalized_snapshot.get("news_top_market_ticker"),
                    "consensus_positive_ev_candidates": normalized_snapshot.get("consensus_positive_ev_candidates"),
                    "consensus_market_pairs_with_consensus": normalized_snapshot.get(
                        "consensus_market_pairs_with_consensus"
                    ),
                    "coldmath_replication_status": coldmath_replication.get("status"),
                    "coldmath_no_outcome_ratio": coldmath_replication.get("no_outcome_ratio"),
                    "coldmath_replication_plan_status": coldmath_replication_plan.get("status"),
                    "coldmath_replication_plan_candidates": coldmath_replication_plan.get("candidate_count"),
                },
            )
        )

        state.transition("candidates.scored")
        events.append(
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type="candidates_scored",
                phase=state.current_phase,
                lane=config.lane,
                severity="info",
                data={
                    "candidate_count": len(ticket_candidates),
                    "top_candidate": (
                        None
                        if not ticket_candidates
                        else {
                            "market": ticket_candidates[0].market,
                            "side": ticket_candidates[0].side,
                            "max_cost": ticket_candidates[0].max_cost,
                            "score": ticket_candidates[0].score,
                            "source": ticket_candidates[0].source,
                        }
                    ),
                },
            )
        )
        state.transition("policy.checked")

        policy_decision: PolicyDecision = evaluate_policy_gate(
            lane=config.lane,
            lane_policy_set=lane_policy_set,
            degraded_summary=degraded_summary,
            request_live_submit=config.request_live_submit,
        )
        events.append(
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type="policy_decision",
                phase=state.current_phase,
                lane=config.lane,
                severity=("block" if policy_decision.status == "blocked" else "warn" if policy_decision.status == "degraded" else "info"),
                data={
                    "status": policy_decision.status,
                    "reason": policy_decision.reason,
                    "allowed_actions": list(policy_decision.allowed_actions),
                },
            )
        )

        approval_required = self._resolve_approval_required(
            policy_payload=policy_payload,
            lane=config.lane,
        )
        ticket_status = "not_built"
        approval_status = "not_requested"
        order_status = "not_submitted"
        execution_reason = "not_requested"
        execution_ack_status = "not_submitted"
        execution_external_order_id: str | None = None
        live_adapter_mode = "none"
        reconciliation_status = "not_requested"
        reconciliation_reason = "not_requested"
        reconciliation_mismatches = 0
        reconciliation_filled_quantity = 0.0
        reconciliation_remaining_quantity = 0.0
        position_status = "none"
        ticket: TicketProposal | None = None
        resolved_ticket_market, resolved_ticket_side, resolved_ticket_max_cost, ticket_selection = self._resolve_ticket_inputs(
            config=config,
            candidates=ticket_candidates,
        )

        if policy_decision.status == "blocked":
            state.transition("ticket.blocked")
            ticket_status = "blocked"
            events.append(
                new_event(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    event_type="ticket_blocked",
                    phase=state.current_phase,
                    lane=config.lane,
                    severity="block",
                    data={
                        "reason": policy_decision.reason,
                        "overall_status": "blocked",
                    },
                )
            )
            state.transition("cycle.finished")
            final_overall_status = "blocked"
        else:
            state.transition("ticket.ready")
            ticket_status = "ready"
            ticket_expires_at = self._resolve_ticket_expires_at(config.ticket_expires_at)
            ticket = create_ticket_proposal(
                market=resolved_ticket_market,
                side=resolved_ticket_side,
                max_cost=resolved_ticket_max_cost,
                lane=config.lane,
                source_run_id=run_id,
                expires_at=ticket_expires_at,
            )
            events.append(
                new_event(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    event_type="ticket_ready",
                    phase=state.current_phase,
                    lane=config.lane,
                    severity="warn" if policy_decision.status == "degraded" else "info",
                    data={
                        "reason": policy_decision.reason,
                        "ticket_hash": ticket.ticket_hash,
                        "market": ticket.market,
                        "side": ticket.side,
                        "max_cost": ticket.max_cost,
                        "expires_at": ticket.expires_at,
                        "selection_source": ticket_selection.get("selection_source"),
                        "selection_score": ticket_selection.get("selection_score"),
                        "selection_rationale": ticket_selection.get("selection_rationale"),
                    },
                )
            )
            if config.request_live_submit:
                state.transition("approval.waiting")
                approval = self._load_approval_record(config.approval_json_path)
                approval_status = "provided" if approval is not None else ("required" if approval_required else "not_required")
                events.append(
                    new_event(
                        run_id=run_id,
                        cycle_id=cycle_id,
                        event_type="approval_waiting",
                        phase=state.current_phase,
                        lane=config.lane,
                        severity="warn" if approval_required else "info",
                        data={
                            "approval_required": approval_required,
                            "approval_provided": approval is not None,
                            "approval_json_path": config.approval_json_path,
                        },
                    )
                )
                executor: LiveExecutor | None = None
                execution_result: LiveExecutionResult
                try:
                    venue_adapter = self._live_venue_adapter
                    if venue_adapter is not None:
                        live_adapter_mode = "injected"
                    if venue_adapter is None and config.live_env_file:
                        from betbot.execution.kalshi_live_venue_adapter import KalshiLiveVenueAdapter

                        venue_adapter = KalshiLiveVenueAdapter.from_env_file(
                            env_file=config.live_env_file,
                            timeout_seconds=config.live_timeout_seconds,
                        )
                        live_adapter_mode = "kalshi_env"
                    if venue_adapter is None:
                        execution_result = LiveExecutionResult(
                            status="blocked",
                            reason="live_adapter_required_for_live_execute",
                            submitted_at=datetime.now(timezone.utc).isoformat(),
                            market=ticket.market,
                            side=ticket.side,
                            ack_status="not_submitted",
                            external_order_id=None,
                        )
                    elif (
                        config.lane == "live_execute"
                        and isinstance(venue_adapter, LocalLiveVenueAdapter)
                        and not config.allow_simulated_live_adapter
                    ):
                        live_adapter_mode = "simulated_blocked"
                        execution_result = LiveExecutionResult(
                            status="blocked",
                            reason="simulated_live_adapter_not_allowed",
                            submitted_at=datetime.now(timezone.utc).isoformat(),
                            market=ticket.market,
                            side=ticket.side,
                            ack_status="not_submitted",
                            external_order_id=None,
                        )
                    else:
                        if isinstance(venue_adapter, LocalLiveVenueAdapter):
                            live_adapter_mode = "simulated_allowed"
                        executor = LiveExecutor(
                            lane_policy_set,
                            venue_adapter=venue_adapter,
                        )
                        execution_result = executor.submit(
                            lane=config.lane,
                            ticket=ticket,
                            approval=approval,
                            approval_required=approval_required,
                        )
                except Exception as exc:
                    execution_result = LiveExecutionResult(
                        status="blocked",
                        reason=f"live_adapter_init_failed:{str(exc) or 'unknown'}",
                        submitted_at=datetime.now(timezone.utc).isoformat(),
                        market=ticket.market,
                        side=ticket.side,
                        ack_status="not_submitted",
                        external_order_id=None,
                    )
                execution_reason = execution_result.reason
                execution_ack_status = execution_result.ack_status
                execution_external_order_id = execution_result.external_order_id
                if execution_result.status == "submitted":
                    if executor is None:
                        raise RuntimeError("live executor unavailable after successful submit status")
                    state.transition("order.submitted")
                    order_status = "submitted"
                    approval_status = "approved" if approval_required else approval_status
                    events.append(
                        new_event(
                            run_id=run_id,
                            cycle_id=cycle_id,
                            event_type="order_submitted",
                            phase=state.current_phase,
                            lane=config.lane,
                            severity="info",
                            data={
                                "market": execution_result.market,
                                "side": execution_result.side,
                                "submitted_at": execution_result.submitted_at,
                                "ack_status": execution_result.ack_status,
                                "external_order_id": execution_result.external_order_id,
                            },
                        )
                    )
                    reconciliation = executor.reconcile(
                        lane=config.lane,
                        ticket=ticket,
                        external_order_id=execution_result.external_order_id,
                    )
                    reconciliation_status = reconciliation.status
                    reconciliation_reason = reconciliation.reason
                    reconciliation_mismatches = int(reconciliation.mismatches)
                    reconciliation_filled_quantity = float(reconciliation.filled_quantity)
                    reconciliation_remaining_quantity = float(reconciliation.remaining_quantity)
                    position_status = str(reconciliation.position_status or "none")

                    if reconciliation.status == "resting":
                        state.transition("order.resting")
                        events.append(
                            new_event(
                                run_id=run_id,
                                cycle_id=cycle_id,
                                event_type="order_resting",
                                phase=state.current_phase,
                                lane=config.lane,
                                severity="info",
                                data={
                                    "market": execution_result.market,
                                    "side": execution_result.side,
                                    "external_order_id": reconciliation.external_order_id,
                                    "filled_quantity": reconciliation.filled_quantity,
                                    "remaining_quantity": reconciliation.remaining_quantity,
                                    "reason": reconciliation.reason,
                                },
                            )
                        )
                        state.transition("cycle.finished")
                        final_overall_status = "degraded" if policy_decision.status == "degraded" else "ok"
                    elif reconciliation.status == "partially_filled":
                        state.transition("order.partially_filled")
                        events.append(
                            new_event(
                                run_id=run_id,
                                cycle_id=cycle_id,
                                event_type="order_partially_filled",
                                phase=state.current_phase,
                                lane=config.lane,
                                severity="info",
                                data={
                                    "market": execution_result.market,
                                    "side": execution_result.side,
                                    "external_order_id": reconciliation.external_order_id,
                                    "filled_quantity": reconciliation.filled_quantity,
                                    "remaining_quantity": reconciliation.remaining_quantity,
                                    "reason": reconciliation.reason,
                                },
                            )
                        )
                        if position_status == "open":
                            state.transition("position.open")
                            events.append(
                                new_event(
                                    run_id=run_id,
                                    cycle_id=cycle_id,
                                    event_type="position_opened",
                                    phase=state.current_phase,
                                    lane=config.lane,
                                    severity="info",
                                    data={
                                        "market": execution_result.market,
                                        "side": execution_result.side,
                                        "external_order_id": reconciliation.external_order_id,
                                        "filled_quantity": reconciliation.filled_quantity,
                                    },
                                )
                            )
                        state.transition("cycle.finished")
                        final_overall_status = "degraded" if policy_decision.status == "degraded" else "ok"
                    elif reconciliation.status == "filled":
                        state.transition("order.filled")
                        events.append(
                            new_event(
                                run_id=run_id,
                                cycle_id=cycle_id,
                                event_type="order_filled",
                                phase=state.current_phase,
                                lane=config.lane,
                                severity="info",
                                data={
                                    "market": execution_result.market,
                                    "side": execution_result.side,
                                    "external_order_id": reconciliation.external_order_id,
                                    "filled_quantity": reconciliation.filled_quantity,
                                    "reason": reconciliation.reason,
                                },
                            )
                        )
                        if position_status == "settled":
                            state.transition("position.settled")
                            events.append(
                                new_event(
                                    run_id=run_id,
                                    cycle_id=cycle_id,
                                    event_type="position_settled",
                                    phase=state.current_phase,
                                    lane=config.lane,
                                    severity="info",
                                    data={
                                        "market": execution_result.market,
                                        "side": execution_result.side,
                                        "external_order_id": reconciliation.external_order_id,
                                    },
                                )
                            )
                        else:
                            state.transition("position.open")
                            events.append(
                                new_event(
                                    run_id=run_id,
                                    cycle_id=cycle_id,
                                    event_type="position_opened",
                                    phase=state.current_phase,
                                    lane=config.lane,
                                    severity="info",
                                    data={
                                        "market": execution_result.market,
                                        "side": execution_result.side,
                                        "external_order_id": reconciliation.external_order_id,
                                        "filled_quantity": reconciliation.filled_quantity,
                                    },
                                )
                            )
                        state.transition("cycle.finished")
                        final_overall_status = "degraded" if policy_decision.status == "degraded" else "ok"
                    elif reconciliation.status == "canceled":
                        state.transition("order.canceled")
                        events.append(
                            new_event(
                                run_id=run_id,
                                cycle_id=cycle_id,
                                event_type="order_canceled",
                                phase=state.current_phase,
                                lane=config.lane,
                                severity="warn",
                                data={
                                    "market": execution_result.market,
                                    "side": execution_result.side,
                                    "external_order_id": reconciliation.external_order_id,
                                    "filled_quantity": reconciliation.filled_quantity,
                                    "reason": reconciliation.reason,
                                },
                            )
                        )
                        state.transition("cycle.finished")
                        final_overall_status = "degraded" if policy_decision.status == "degraded" else "ok"
                    elif reconciliation.status == "mismatch":
                        events.append(
                            new_event(
                                run_id=run_id,
                                cycle_id=cycle_id,
                                event_type="order_reconcile_mismatch",
                                phase=state.current_phase,
                                lane=config.lane,
                                severity="error",
                                data={
                                    "market": execution_result.market,
                                    "side": execution_result.side,
                                    "external_order_id": reconciliation.external_order_id,
                                    "mismatches": reconciliation.mismatches,
                                    "reason": reconciliation.reason,
                                },
                            )
                        )
                        order_status = "reconcile_mismatch"
                        execution_reason = "reconcile_mismatch"
                        state.transition("cycle.failed")
                        final_overall_status = "failed"
                    else:
                        events.append(
                            new_event(
                                run_id=run_id,
                                cycle_id=cycle_id,
                                event_type="order_reconcile_unknown",
                                phase=state.current_phase,
                                lane=config.lane,
                                severity="warn",
                                data={
                                    "market": execution_result.market,
                                    "side": execution_result.side,
                                    "external_order_id": reconciliation.external_order_id,
                                    "reason": reconciliation.reason,
                                },
                            )
                        )
                        state.transition("cycle.finished")
                        final_overall_status = "degraded" if policy_decision.status == "degraded" else "ok"
                else:
                    order_status = "blocked"
                    if execution_result.reason.startswith("approval_"):
                        approval_status = execution_result.reason
                    events.append(
                        new_event(
                            run_id=run_id,
                            cycle_id=cycle_id,
                            event_type="order_blocked",
                            phase=state.current_phase,
                            lane=config.lane,
                            severity="block",
                            data={
                                "reason": execution_result.reason,
                                "market": execution_result.market,
                                "side": execution_result.side,
                                "ack_status": execution_result.ack_status,
                                "external_order_id": execution_result.external_order_id,
                            },
                        )
                    )
                    state.transition("cycle.finished")
                    final_overall_status = "blocked"
            else:
                execution_reason = "live_submit_not_requested"
                state.transition("cycle.finished")
                final_overall_status = "degraded" if policy_decision.status == "degraded" else "ok"

        final_event_type = "cycle_failed" if state.current_phase == "cycle.failed" else "cycle_finished"
        events.append(
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type=final_event_type,
                phase=state.current_phase,
                lane=config.lane,
                severity=(
                    "error"
                    if final_overall_status == "failed"
                    else "block"
                    if final_overall_status == "blocked"
                    else "warn"
                    if final_overall_status == "degraded"
                    else "info"
                ),
                data={
                    "overall_status": final_overall_status,
                    "policy_status": policy_decision.status,
                    "execution_reason": execution_reason,
                    "recovery_recommendation": degraded_summary.recovery_recommendation,
                },
            )
        )

        store.append_many(events)
        cycle_projection = build_cycle_projection(events)
        board_projection = build_board_projection(cycle_projection)

        allowed_actions = lane_policy_set.allowed_actions(config.lane)
        live_submit_enabled = bool(lane_policy_set.is_allowed(config.lane, "live_submit"))

        report = {
            "run_id": run_id,
            "cycle_id": cycle_id,
            "overall_status": final_overall_status,
            "phase": state.current_phase,
            "permission_lane": config.lane,
            "allowed_actions": allowed_actions,
            "approval_required": approval_required,
            "live_submit_enabled": live_submit_enabled,
            "source_health": degraded_summary.source_statuses,
            "degraded_summary": degraded_summary.to_dict(),
            "news_enrichment": news_enrichment_summary,
            "normalized_snapshot": normalized_snapshot,
            "candidate_scores": [asdict(candidate) for candidate in ticket_candidates[:10]],
            "ticket_selection": dict(ticket_selection),
            "policy_decisions": [asdict(policy_decision)],
            "ticket_status": ticket_status,
            "approval_status": approval_status,
            "order_status": order_status,
            "execution_reason": execution_reason,
            "execution_ack_status": execution_ack_status,
            "execution_external_order_id": execution_external_order_id,
            "live_adapter_mode": live_adapter_mode,
            "allow_simulated_live_adapter": bool(config.allow_simulated_live_adapter),
            "reconciliation_status": reconciliation_status,
            "reconciliation_reason": reconciliation_reason,
            "reconciliation_mismatches": reconciliation_mismatches,
            "reconciliation_filled_quantity": reconciliation_filled_quantity,
            "reconciliation_remaining_quantity": reconciliation_remaining_quantity,
            "position_status": position_status,
            "ticket_proposal": (None if ticket is None else ticket.to_dict()),
            "recovery_recommendation": degraded_summary.recovery_recommendation,
            "citations": [],
            "config_fingerprint": effective_config.config_fingerprint,
            "policy_fingerprint": effective_config.policy_fingerprint,
            "event_log_file": str(output_dir / config.event_log_file),
            "hard_required_sources": list(hard_required_sources),
        }

        cycle_path = output_dir / config.cycle_report_file
        cycle_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        board_path = output_dir / config.board_report_file
        board_path.write_text(json.dumps(board_projection, indent=2), encoding="utf-8")

        return report
