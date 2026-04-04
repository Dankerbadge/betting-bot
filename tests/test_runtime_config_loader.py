from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from betbot.runtime.config_loader import load_effective_config


class RuntimeConfigLoaderTests(unittest.TestCase):
    def test_load_effective_config_merges_repo_layers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".betbot").mkdir(parents=True, exist_ok=True)
            (root / ".betbot.json").write_text(
                json.dumps({"runtime": {"default_lane": "observe"}, "policy": {"approval_required": True}}),
                encoding="utf-8",
            )
            (root / ".betbot" / "settings.json").write_text(
                json.dumps({"policy": {"approval_required": False, "hard_required_sources": ["a", "b"]}}),
                encoding="utf-8",
            )

            effective = load_effective_config(repo_root=str(root))

            self.assertEqual(effective.values["runtime"]["default_lane"], "observe")
            self.assertFalse(effective.values["policy"]["approval_required"])
            self.assertEqual(effective.values["policy"]["hard_required_sources"], ["a", "b"])
            self.assertGreater(len(effective.config_fingerprint), 10)
            self.assertGreater(len(effective.policy_fingerprint), 10)


if __name__ == "__main__":
    unittest.main()
