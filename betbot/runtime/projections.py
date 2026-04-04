from __future__ import annotations

from collections import Counter

from betbot.runtime.events import EventEnvelope


def build_cycle_projection(events: list[EventEnvelope]) -> dict[str, object]:
    if not events:
        return {
            "run_id": "",
            "cycle_id": "",
            "event_count": 0,
            "overall_status": "failed",
            "last_phase": "",
            "severity_counts": {},
            "source_health": {},
        }

    first = events[0]
    last = events[-1]
    severity_counts = Counter(event.severity for event in events)
    source_health: dict[str, str] = {}
    for event in events:
        if event.source:
            maybe_status = str(event.data.get("status") or "")
            if maybe_status:
                source_health[event.source] = maybe_status

    overall_status = "ok"
    if any(sev in {"error", "block"} for sev in severity_counts):
        overall_status = "degraded"
    if any(event.phase == "ticket.blocked" for event in events):
        overall_status = "blocked"
    if last.phase == "cycle.failed":
        overall_status = "failed"

    return {
        "run_id": first.run_id,
        "cycle_id": first.cycle_id,
        "lane": last.lane,
        "started_at": first.ts,
        "finished_at": last.ts,
        "event_count": len(events),
        "overall_status": overall_status,
        "last_phase": last.phase,
        "severity_counts": dict(severity_counts),
        "source_health": source_health,
    }


def build_board_projection(cycle_projection: dict[str, object]) -> dict[str, object]:
    source_health = dict(cycle_projection.get("source_health") or {})
    degraded_sources = sorted(
        key for key, value in source_health.items() if value in {"partial", "degraded", "failed", "blocked"}
    )
    return {
        "run_id": cycle_projection.get("run_id"),
        "cycle_id": cycle_projection.get("cycle_id"),
        "overall_status": cycle_projection.get("overall_status"),
        "lane": cycle_projection.get("lane"),
        "phase": cycle_projection.get("last_phase"),
        "degraded_sources": degraded_sources,
        "source_health": source_health,
        "severity_counts": cycle_projection.get("severity_counts") or {},
    }
