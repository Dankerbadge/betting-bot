from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

from betbot.adapters.base import Adapter, AdapterContext, run_adapter
from betbot.policy.degraded_mode import DegradedSummary, summarize_source_results
from betbot.policy.lanes import LanePolicySet, load_lane_policy_set
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


class CycleRunner:
    def __init__(
        self,
        *,
        adapters: list[Adapter],
        lane_policy_set: LanePolicySet | None = None,
        hard_required_sources: tuple[str, ...] = (),
    ) -> None:
        self.adapters = adapters
        self.lane_policy_set = lane_policy_set or load_lane_policy_set()
        self.hard_required_sources = tuple(hard_required_sources)

    def run(self, config: CycleRunnerConfig) -> dict[str, object]:
        if not self.lane_policy_set.is_known_lane(config.lane):
            raise ValueError(f"Unknown permission lane: {config.lane}")

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

        any_partial = any(result.status in {"partial", "degraded"} for result in source_results.values())
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
                    severity="warn" if result.status in {"partial", "degraded"} else "info",
                    data={
                        "status": result.status,
                        "coverage_ratio": result.coverage_ratio,
                        "stale_seconds": result.stale_seconds,
                        "warnings": list(result.warnings),
                        "errors": list(result.errors),
                    },
                )
            )

        if state.current_phase == "sources.partial":
            state.transition("sources.ready")

        degraded_summary: DegradedSummary = summarize_source_results(
            source_results=source_results,
            phase=state.current_phase,
            hard_required_sources=self.hard_required_sources,
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

        allowed_actions = self.lane_policy_set.allowed_actions(config.lane)
        live_submit_enabled = bool(self.lane_policy_set.is_allowed(config.lane, "live_submit"))

        if degraded_summary.overall_status in {"blocked", "failed"}:
            state.transition("cycle.failed")
            events.append(
                new_event(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    event_type="cycle_failed",
                    phase=state.current_phase,
                    lane=config.lane,
                    severity="block",
                    data={
                        "overall_status": degraded_summary.overall_status,
                        "recovery_recommendation": degraded_summary.recovery_recommendation,
                    },
                )
            )
        else:
            state.transition("cycle.finished")
            events.append(
                new_event(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    event_type="cycle_finished",
                    phase=state.current_phase,
                    lane=config.lane,
                    severity="warn" if degraded_summary.overall_status == "degraded" else "info",
                    data={"overall_status": degraded_summary.overall_status},
                )
            )

        store.append_many(events)
        cycle_projection = build_cycle_projection(events)
        board_projection = build_board_projection(cycle_projection)

        report = {
            "run_id": run_id,
            "cycle_id": cycle_id,
            "overall_status": degraded_summary.overall_status,
            "phase": state.current_phase,
            "permission_lane": config.lane,
            "allowed_actions": allowed_actions,
            "approval_required": True,
            "live_submit_enabled": live_submit_enabled,
            "source_health": degraded_summary.source_statuses,
            "degraded_summary": degraded_summary.to_dict(),
            "policy_decisions": [],
            "ticket_status": "not_built",
            "approval_status": "not_requested",
            "order_status": "not_submitted",
            "recovery_recommendation": degraded_summary.recovery_recommendation,
            "citations": [],
            "config_fingerprint": "",
            "policy_fingerprint": "",
            "event_log_file": str(output_dir / config.event_log_file),
        }

        cycle_path = output_dir / config.cycle_report_file
        cycle_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        board_path = output_dir / config.board_report_file
        board_path.write_text(json.dumps(board_projection, indent=2), encoding="utf-8")

        return report
