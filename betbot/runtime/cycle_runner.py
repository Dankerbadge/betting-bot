from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

from betbot.adapters.base import Adapter, AdapterContext, run_adapter
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
    hard_required_sources: tuple[str, ...] | None = None


class CycleRunner:
    def __init__(
        self,
        *,
        adapters: list[Adapter],
        lane_policy_set: LanePolicySet | None = None,
        hard_required_sources: tuple[str, ...] = (),
    ) -> None:
        self.adapters = adapters
        self._injected_lane_policy_set = lane_policy_set
        self.hard_required_sources = tuple(hard_required_sources)

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
            lane_specific = self._coerce_source_tuple(by_lane_raw.get(config.lane))
            if lane_specific:
                return lane_specific

        global_required = self._coerce_source_tuple(policy_payload.get("hard_required_sources"))
        return global_required

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

        state.transition("news.enriching")
        events.append(
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type="news_enriching",
                phase=state.current_phase,
                lane=config.lane,
                severity="warn" if degraded_summary.overall_status == "degraded" else "info",
            )
        )

        state.transition("snapshots.normalized")
        state.transition("candidates.scored")
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

        approval_required = bool(policy_payload.get("approval_required", True))
        ticket_status = "not_built"
        approval_status = "not_requested"
        order_status = "not_submitted"

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
            events.append(
                new_event(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    event_type="ticket_ready",
                    phase=state.current_phase,
                    lane=config.lane,
                    severity="warn" if policy_decision.status == "degraded" else "info",
                    data={"reason": policy_decision.reason},
                )
            )
            if config.request_live_submit:
                state.transition("approval.waiting")
                approval_status = "required" if approval_required else "not_required"
                events.append(
                    new_event(
                        run_id=run_id,
                        cycle_id=cycle_id,
                        event_type="approval_waiting",
                        phase=state.current_phase,
                        lane=config.lane,
                        severity="warn" if approval_required else "info",
                        data={"approval_required": approval_required},
                    )
                )
            state.transition("cycle.finished")
            final_overall_status = "degraded" if policy_decision.status == "degraded" else "ok"

        events.append(
            new_event(
                run_id=run_id,
                cycle_id=cycle_id,
                event_type="cycle_finished",
                phase=state.current_phase,
                lane=config.lane,
                severity=("block" if final_overall_status == "blocked" else "warn" if final_overall_status == "degraded" else "info"),
                data={
                    "overall_status": final_overall_status,
                    "policy_status": policy_decision.status,
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
            "policy_decisions": [asdict(policy_decision)],
            "ticket_status": ticket_status,
            "approval_status": approval_status,
            "order_status": order_status,
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
