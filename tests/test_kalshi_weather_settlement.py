from __future__ import annotations

import unittest

from betbot.kalshi_weather_settlement import (
    build_weather_settlement_spec,
    extract_threshold_expression,
    infer_contract_family,
    infer_observation_window_local,
    infer_settlement_sources,
    infer_settlement_timezone,
    infer_timezone_from_station,
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

    def test_infer_settlement_sources_prefers_non_fallback_primary_when_clause_comes_first(self) -> None:
        primary, fallback = infer_settlement_sources(
            "If unavailable, NCEI published value will be used. Otherwise the market resolves according to National Weather Service station report."
        )
        self.assertEqual(primary, "NWS")
        self.assertEqual(fallback, "NCEI")

    def test_infer_settlement_sources_does_not_duplicate_single_source_fallback(self) -> None:
        primary, fallback = infer_settlement_sources(
            "If unavailable, NCEI published value will be used for settlement."
        )
        self.assertEqual(primary, "NCEI")
        self.assertEqual(fallback, "")

    def test_extract_threshold_expression_parses_between(self) -> None:
        threshold = extract_threshold_expression(
            "If the Land Ocean-Temperature Index for Apr 2026 is between 1.17-1.23, then the market resolves to Yes."
        )
        self.assertEqual(threshold, "between:1.17:1.23")

    def test_extract_threshold_expression_parses_at_most(self) -> None:
        threshold = extract_threshold_expression(
            "If the daily high is at most 72 then the market resolves to Yes."
        )
        self.assertEqual(threshold, "at_most:72")

    def test_rule_hash_is_stable(self) -> None:
        text_a = "If value is above 1.30 then resolve Yes."
        text_b = "If   value is above 1.30 then resolve Yes."
        self.assertEqual(rule_text_hash_sha256(text_a), rule_text_hash_sha256(text_b))

    def test_infer_observation_window_local_parses_clock_range(self) -> None:
        start, end, source = infer_observation_window_local(
            "Observation period is between 12:00 AM and 11:59 PM local time."
        )
        self.assertEqual(start, "00:00")
        self.assertEqual(end, "23:59")
        self.assertEqual(source, "rules_text")

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
        self.assertEqual(spec["observation_window_local_start"], "")
        self.assertEqual(spec["observation_window_local_end"], "")
        self.assertTrue(bool(spec["rule_text_hash_sha256"]))

    def test_build_weather_settlement_spec_infers_city_station_fallback(self) -> None:
        spec = build_weather_settlement_spec(
            {
                "market_ticker": "KXHIGHTATL-26APR10-B75.5",
                "market_title": "Will the high temp in Atlanta be 75-76° on Apr 10, 2026?",
                "event_title": "Highest temperature in Atlanta on Apr 10, 2026?",
                "rules_primary": (
                    "If the highest temperature recorded in Atlanta for April 10, 2026 "
                    "is between 75-76°, then the market resolves to Yes."
                ),
            }
        )
        self.assertEqual(spec["contract_family"], "daily_temperature")
        self.assertEqual(spec["settlement_station"], "KATL")

    def test_infer_settlement_timezone_avoids_la_substring_false_positive(self) -> None:
        timezone_name = infer_settlement_timezone(
            market_ticker="KXHIGHTOKC-26APR11-B82.5",
            market_title="Will the high temp in Oklahoma City be above 82?",
            event_title="Highest temperature in Oklahoma City?",
        )
        self.assertEqual(timezone_name, "America/Chicago")

        timezone_name_nola = infer_settlement_timezone(
            market_ticker="KXHIGHTNOLA-26APR11-B86.5",
            market_title="Will the high temp in New Orleans be above 86?",
            event_title="Highest temperature in New Orleans?",
        )
        self.assertEqual(timezone_name_nola, "America/Chicago")

    def test_build_weather_settlement_spec_falls_back_to_station_timezone(self) -> None:
        spec = build_weather_settlement_spec(
            {
                "market_ticker": "KXHIGHTSATX-26APR10-B82.5",
                "market_title": "Will station KSAT high be above 82 on Apr 10?",
                "event_title": "KSAT station daily high on Apr 10?",
                "rules_primary": "If station KSAT reports a high above 82, market resolves Yes.",
            }
        )
        self.assertEqual(spec["settlement_station"], "KSAT")
        self.assertEqual(spec["settlement_timezone"], "America/Chicago")
        self.assertEqual(infer_timezone_from_station("KSAT"), "America/Chicago")


if __name__ == "__main__":
    unittest.main()
