from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from betbot.runtime.event_store import EventStore
from betbot.runtime.events import new_event


class EventStoreTests(unittest.TestCase):
    def test_event_store_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            store = EventStore(path)
            event = new_event(
                run_id="run-1",
                cycle_id="cycle-1",
                event_type="cycle_started",
                phase="cycle.started",
                lane="observe",
            )
            store.append(event)
            loaded = store.load()
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0].run_id, "run-1")
            self.assertEqual(loaded[0].phase, "cycle.started")


if __name__ == "__main__":
    unittest.main()
