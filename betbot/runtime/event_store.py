from __future__ import annotations

import json
from pathlib import Path

from betbot.runtime.events import EventEnvelope


class EventStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event: EventEnvelope) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_dict(), sort_keys=True))
            handle.write("\n")

    def append_many(self, events: list[EventEnvelope]) -> None:
        if not events:
            return
        with self.path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event.to_dict(), sort_keys=True))
                handle.write("\n")

    def load(self) -> list[EventEnvelope]:
        if not self.path.exists():
            return []
        rows: list[EventEnvelope] = []
        for raw in self.path.read_text(encoding="utf-8").splitlines():
            stripped = raw.strip()
            if not stripped:
                continue
            rows.append(EventEnvelope.from_dict(json.loads(stripped)))
        return rows
