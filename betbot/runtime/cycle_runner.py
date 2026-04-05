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
                market=config.ticket_market,
                side=config.ticket_side,
                max_cost=config.ticket_max_cost,
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
