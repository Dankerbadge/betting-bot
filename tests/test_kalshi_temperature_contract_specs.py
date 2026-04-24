from __future__ import annotations

from datetime import datetime, timezone
import json
import unittest

from betbot.kalshi_temperature_contract_specs import (
    extract_kalshi_temperature_contract_specs,
    infer_target_date_from_event_ticker,
    settlement_confidence_score,
)


class KalshiTemperatureContractSpecsTests(unittest.TestCase):
    def test_extract_kalshi_temperature_contract_specs(self) -> None:
        events = [
            {
                "category": "Climate and Weather",
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "title": "Highest temperature in NYC on Apr 8?",
                "contract_terms_url": "https://example.test/terms",
                "settlement_sources": ["https://forecast.weather.gov/product.php?site=NWS"],
                "markets": [
                    {
                        "ticker": "KXHIGHNY-26APR08-B72",
                        "title": "72F or above",
                        "status": "active",
                        "close_time": "2026-04-09T00:00:00Z",
                        "strike_type": "greater",
                        "floor_strike": "72",
                        "floor_strike_fp": "720000",
                        "rules_primary": "Highest temperature recorded at station KNYC in local day.",
                        "rules_secondary": "If unavailable, NWS source used.",
                    }
                ],
            },
            {
                "category": "Crypto",
                "series_ticker": "KXBTC",
                "event_ticker": "KXBTC-26APR08",
                "title": "BTC close",
                "markets": [
                    {
                        "ticker": "KXBTC-26APR08-B100K",
                        "title": "Will BTC close above $100k?",
                        "status": "active",
                        "rules_primary": "Resolve to exchange close",
                    }
                ],
            },
        ]

        rows = extract_kalshi_temperature_contract_specs(
            events=events,
            captured_at=datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(len(rows), 1)

        row = rows[0]
        self.assertEqual(row["series_ticker"], "KXHIGHNY")
        self.assertEqual(row["market_ticker"], "KXHIGHNY-26APR08-B72")
        self.assertEqual(row["strike_type"], "greater")
        self.assertEqual(row["floor_strike"], "72")
        self.assertEqual(row["floor_strike_fp"], "720000")
        self.assertEqual(row["contract_terms_url"], "https://example.test/terms")
        self.assertEqual(json.loads(row["settlement_sources"]), ["https://forecast.weather.gov/product.php?site=NWS"])
        self.assertEqual(row["contract_family"], "daily_temperature")
        self.assertEqual(row["target_date_local"], "2026-04-08")
        self.assertGreater(float(row["settlement_confidence_score"]), 0.5)
        self.assertEqual(row["settlement_station"], "KNYC")

    def test_infer_target_date_from_event_ticker(self) -> None:
        self.assertEqual(infer_target_date_from_event_ticker("KXHIGHNY-26APR08"), "2026-04-08")
        self.assertEqual(infer_target_date_from_event_ticker("KXHIGHNY"), "")

    def test_settlement_confidence_score(self) -> None:
        score = settlement_confidence_score(
            {
                "settlement_source_primary": "NWS",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "threshold_expression": "at_least:72",
                "local_day_boundary": "local_day",
            }
        )
        self.assertEqual(score, 1.0)

    def test_extract_accepts_daily_temp_ticker_without_temperature_word(self) -> None:
        events = [
            {
                "category": "Climate and Weather",
                "series_ticker": "KXLOWTSFO",
                "event_ticker": "KXLOWTSFO-26APR10",
                "title": "Lowest in San Francisco on Apr 10?",
                "markets": [
                    {
                        "ticker": "KXLOWTSFO-26APR10-B50",
                        "title": "50F or below",
                        "status": "active",
                        "rules_primary": "Resolves using station KSFO local-day max/min windows.",
                    }
                ],
            }
        ]

        rows = extract_kalshi_temperature_contract_specs(
            events=events,
            captured_at=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["series_ticker"], "KXLOWTSFO")
        self.assertEqual(rows[0]["market_ticker"], "KXLOWTSFO-26APR10-B50")

    def test_extract_rejects_non_weather_kxhigh_prefix_markets(self) -> None:
        events = [
            {
                "category": "Economics",
                "series_ticker": "KXHIGHINFLATION",
                "event_ticker": "KXHIGHINFLATION-26DEC",
                "title": "How high will CPI get this year?",
                "markets": [
                    {
                        "ticker": "KXHIGHINFLATION-26DEC-T3.0",
                        "title": "Will CPI exceed 3.0?",
                        "status": "active",
                        "rules_primary": "If any CPI print is above 3.0, resolves Yes.",
                    }
                ],
            }
        ]

        rows = extract_kalshi_temperature_contract_specs(
            events=events,
            captured_at=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(rows, [])

    def test_extract_rejects_climate_macro_prefix_without_daily_date_token(self) -> None:
        events = [
            {
                "category": "Climate and Weather",
                "series_ticker": "KXHIGHINFLATION",
                "event_ticker": "KXHIGHINFLATION-26DEC",
                "title": "How high will CPI get this year?",
                "markets": [
                    {
                        "ticker": "KXHIGHINFLATION-26DEC-T3.0",
                        "title": "Will CPI exceed 3.0?",
                        "status": "active",
                        "rules_primary": "If any CPI print is above 3.0, resolves Yes.",
                    }
                ],
            }
        ]

        rows = extract_kalshi_temperature_contract_specs(
            events=events,
            captured_at=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(rows, [])

    def test_extract_rejects_monthly_climate_anomaly_markets(self) -> None:
        events = [
            {
                "category": "Climate and Weather",
                "series_ticker": "KXGTEMP",
                "event_ticker": "KXGTEMP-26APR",
                "title": "Global temperature anomaly in April 2026?",
                "markets": [
                    {
                        "ticker": "KXGTEMP-26APR-B0.5",
                        "title": "Will global temperature anomaly be above 0.5?",
                        "status": "active",
                        "strike_type": "greater",
                        "floor_strike": "0.5",
                        "rules_primary": "If global temperature anomaly in April is above 0.5 then Yes.",
                    }
                ],
            }
        ]

        rows = extract_kalshi_temperature_contract_specs(
            events=events,
            captured_at=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(rows, [])

    def test_extract_rejects_intraday_temperature_point_contracts(self) -> None:
        events = [
            {
                "category": "Climate and Weather",
                "series_ticker": "KXTEMPNYCH",
                "event_ticker": "KXTEMPNYCH-26APR1514",
                "title": "NYC temperature on Apr 15, 2026 at 2pm EDT?",
                "markets": [
                    {
                        "ticker": "KXTEMPNYCH-26APR1514-T80.99",
                        "title": "Will temp be above 80.99°?",
                        "status": "active",
                        "rules_primary": "Resolves from KNYC observed temp at 2pm EDT.",
                    }
                ],
            }
        ]

        rows = extract_kalshi_temperature_contract_specs(
            events=events,
            captured_at=datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
