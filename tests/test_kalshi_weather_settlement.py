from __future__ import annotations

import unittest

from betbot.kalshi_weather_settlement import (
    build_weather_settlement_spec,
    extract_threshold_expression,
    infer_contract_family,
    infer_settlement_sources,
    rule_text_hash_sha256,
)


class KalshiWeatherSettlementTests(unittest.TestCase):
    def test_infer_contract_family_detects_monthly_climate(self) -> None:
        family = infer_contract_family(
            market_ticker="KXHMONTHRANGE-26APR-B1.200",
            market_title="Apr 2026 temperature increase?",
            event_title="Apr 2026 temperature increase?",
            rules_primary="If the Land Ocean-Temperature Index for Apr 2026 is between 1.17-1.23, then the market resolves to Yes.",
        )
        self.assertEqual(family, "monthly_climate_anomaly")

    def test_infer_settlement_sources_detects_ncei_and_fallback(self) -> None:
        primary, fallback = infer_settlement_sources(
            "Resolved to NWS station report. If unavailable, NCEI published value will be used."
        )
        self.assertEqual(primary, "NWS")
        self.assertEqual(fallback, "NCEI")

    def test_extract_threshold_expression_parses_between(self) -> None:
        threshold = extract_threshold_expression(
            "If the Land Ocean-Temperature Index for Apr 2026 is between 1.17-1.23, then the market resolves to Yes."
        )
        self.assertEqual(threshold, "between:1.17:1.23")

    def test_rule_hash_is_stable(self) -> None:
        text_a = "If value is above 1.30 then resolve Yes."
        text_b = "If   value is above 1.30 then resolve Yes."
        self.assertEqual(rule_text_hash_sha256(text_a), rule_text_hash_sha256(text_b))

    def test_build_weather_settlement_spec_contains_core_fields(self) -> None:
        spec = build_weather_settlement_spec(
            {
                "market_ticker": "KXRAINNYC-26MAR31",
                "market_title": "Will it rain in NYC on Mar 31?",
                "event_title": "Will it rain in NYC on Mar 31?",
                "rules_primary": "If measurable rain is recorded at station KJFK, the market resolves to Yes.",
            }
        )
        self.assertEqual(spec["contract_family"], "daily_rain")
        self.assertEqual(spec["settlement_station"], "KJFK")
        self.assertEqual(spec["settlement_timezone"], "America/New_York")
        self.assertTrue(bool(spec["rule_text_hash_sha256"]))


if __name__ == "__main__":
    unittest.main()
