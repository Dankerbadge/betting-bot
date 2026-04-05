from __future__ import annotations

import unittest

from betbot.policy.lanes import load_lane_policy_set


class LanePolicyTests(unittest.TestCase):
    def test_live_execute_lane_allows_live_submit(self) -> None:
        policy_set = load_lane_policy_set()
        self.assertTrue(policy_set.is_known_lane("live_execute"))
        self.assertTrue(policy_set.is_allowed("live_execute", "live_submit"))

    def test_research_lane_blocks_live_submit(self) -> None:
        policy_set = load_lane_policy_set()
        self.assertFalse(policy_set.is_allowed("research", "live_submit"))
        self.assertIn("news_read", policy_set.allowed_actions("research"))


if __name__ == "__main__":
    unittest.main()
