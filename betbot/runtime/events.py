from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import uuid

from betbot.runtime.event_types import EventSeverity


@dataclass(frozen=True)
class EventEnvelope:
    schema_version: str
    event_id: str
    run_id: str
    cycle_id: str
    ts: str
    event_type: str
    phase: str
    severity: EventSeverity
    lane: str
    source: str | None
    data: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @staticmethod
    def from_dict(payload: dict[str, object]) -> "EventEnvelope":
        return EventEnvelope(
            schema_version=str(payload.get("schema_version") or "1.0"),
            event_id=str(payload.get("event_id") or ""),
            run_id=str(payload.get("run_id") or ""),
            cycle_id=str(payload.get("cycle_id") or ""),
            ts=str(payload.get("ts") or ""),
            event_type=str(payload.get("event_type") or ""),
            phase=str(payload.get("phase") or ""),
            severity=str(payload.get("severity") or "info"),
            lane=str(payload.get("lane") or "observe"),
            source=(None if payload.get("source") is None else str(payload.get("source"))),
            data=dict(payload.get("data") or {}),
        )


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_event(
    *,
    run_id: str,
    cycle_id: str,
    event_type: str,
    phase: str,
    lane: str,
    severity: EventSeverity = "info",
    source: str | None = None,
    data: dict[str, object] | None = None,
    schema_version: str = "1.0",
) -> EventEnvelope:
    return EventEnvelope(
        schema_version=schema_version,
        event_id=str(uuid.uuid4()),
        run_id=run_id,
        cycle_id=cycle_id,
        ts=now_utc_iso(),
        event_type=event_type,
        phase=phase,
        severity=severity,
        lane=lane,
        source=source,
        data=data or {},
    )
