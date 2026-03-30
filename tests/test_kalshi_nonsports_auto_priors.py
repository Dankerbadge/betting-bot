from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_auto_priors import _domain_from_url, run_kalshi_nonsports_auto_priors, _upsert_priors_csv


HISTORY_FIELDNAMES = [
    "captured_at",
    "category",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "event_title",
    "market_title",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_bid_size_contracts",
    "yes_ask_dollars",
    "yes_ask_size_contracts",
    "spread_dollars",
    "liquidity_dollars",
    "volume_24h_contracts",
    "open_interest_contracts",
    "ten_dollar_fillable_at_best_ask",
    "two_sided_book",
    "execution_fit_score",
]

PRIOR_FIELDNAMES = [
    "market_ticker",
    "fair_yes_probability",
    "fair_yes_probability_low",
    "fair_yes_probability_high",
    "confidence",
    "thesis",
    "source_note",
    "updated_at",
    "evidence_count",
    "evidence_quality",
    "source_type",
    "last_evidence_at",
]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _fake_news_getter(url: str, timeout_seconds: float) -> str:
    return (
        "<rss><channel>"
        "<item>"
        "<title>Alpha files for IPO in public filing</title>"
        "<link>https://www.reuters.com/markets/us/alpha-files-ipo/</link>"
        "<pubDate>Sat, 28 Mar 2026 11:00:00 GMT</pubDate>"
        "<description>IPO filing announced.</description>"
        "<source url='https://www.reuters.com'>Reuters</source>"
        "</item>"
        "<item>"
        "<title>Official statement confirms Alpha listing timeline update</title>"
        "<link>https://www.sec.gov/news/press-release-alpha-ipo</link>"
        "<pubDate>Sat, 28 Mar 2026 10:00:00 GMT</pubDate>"
        "<description>Official filing progress notice.</description>"
        "<source url='https://www.sec.gov'>SEC</source>"
        "</item>"
        "</channel></rss>"
    )


def _failing_news_getter(url: str, timeout_seconds: float) -> str:
    raise RuntimeError("simulated network failure")


class KalshiNonsportsAutoPriorsTests(unittest.TestCase):
    def test_domain_from_google_news_encoded_redirect_uses_publisher_domain(self) -> None:
        url = (
            "https://news.google.com/articles/"
            "CBMiY2h0dHBzOi8vd3d3LnNlYy5nb3YvbmV3cy9wcmVzcy1yZWxlYXNlLWFscGhhLWlwb9IBAA"
            "?hl=en-US&gl=US&ceid=US:en"
        )
        self.assertEqual(_domain_from_url(url), "sec.gov")

    def test_domain_from_google_news_query_redirect_uses_publisher_domain(self) -> None:
        url = (
            "https://news.google.com/rss/articles/CBMiT2h0dHBzOi8vbmV3cy5nb29nbGUuY29tL3Jzcy9hcnRpY2xlcy9DUk1pLi4u"
            "?url=https%3A%2F%2Fwww.reuters.com%2Fmarkets%2Fdeals%2Fsample-story%2F&hl=en-US&gl=US&ceid=US:en"
        )
        self.assertEqual(_domain_from_url(url), "reuters.com")

    def test_run_kalshi_nonsports_auto_priors_generates_and_upserts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-27T20:00:00+00:00",
                        "category": "Companies",
                        "series_ticker": "KXIPOALPHA",
                        "event_ticker": "KXIPOALPHA-26APR30",
                        "market_ticker": "KXIPOALPHA-26APR30",
                        "event_title": "Will Alpha announce an IPO by Apr 30?",
                        "market_title": "Will Alpha announce an IPO by Apr 30?",
                        "close_time": "2026-04-30T23:59:00+00:00",
                        "hours_to_close": "820",
                        "yes_bid_dollars": "0.03",
                        "yes_bid_size_contracts": "40",
                        "yes_ask_dollars": "0.04",
                        "yes_ask_size_contracts": "50",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "1000",
                        "volume_24h_contracts": "200",
                        "open_interest_contracts": "1200",
                        "ten_dollar_fillable_at_best_ask": "true",
                        "two_sided_book": "true",
                        "execution_fit_score": "60",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Companies",
                        "series_ticker": "KXIPOALPHA",
                        "event_ticker": "KXIPOALPHA-26APR30",
                        "market_ticker": "KXIPOALPHA-26APR30",
                        "event_title": "Will Alpha announce an IPO by Apr 30?",
                        "market_title": "Will Alpha announce an IPO by Apr 30?",
                        "close_time": "2026-04-30T23:59:00+00:00",
                        "hours_to_close": "819",
                        "yes_bid_dollars": "0.04",
                        "yes_bid_size_contracts": "41",
                        "yes_ask_dollars": "0.05",
                        "yes_ask_size_contracts": "51",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "1050",
                        "volume_24h_contracts": "210",
                        "open_interest_contracts": "1250",
                        "ten_dollar_fillable_at_best_ask": "true",
                        "two_sided_book": "true",
                        "execution_fit_score": "62",
                    },
                    {
                        "captured_at": "2026-03-27T22:00:00+00:00",
                        "category": "Companies",
                        "series_ticker": "KXIPOALPHA",
                        "event_ticker": "KXIPOALPHA-26APR30",
                        "market_ticker": "KXIPOALPHA-26APR30",
                        "event_title": "Will Alpha announce an IPO by Apr 30?",
                        "market_title": "Will Alpha announce an IPO by Apr 30?",
                        "close_time": "2026-04-30T23:59:00+00:00",
                        "hours_to_close": "818",
                        "yes_bid_dollars": "0.05",
                        "yes_bid_size_contracts": "42",
                        "yes_ask_dollars": "0.06",
                        "yes_ask_size_contracts": "52",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "1100",
                        "volume_24h_contracts": "220",
                        "open_interest_contracts": "1300",
                        "ten_dollar_fillable_at_best_ask": "true",
                        "two_sided_book": "true",
                        "execution_fit_score": "64",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            summary = run_kalshi_nonsports_auto_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                news_getter=_fake_news_getter,
                now=datetime(2026, 3, 28, 13, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["generated_priors"], 1)
            self.assertEqual(summary["inserted_rows"], 1)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())
            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_ticker"], "KXIPOALPHA-26APR30")
            self.assertEqual(rows[0]["source_type"], "auto")
            self.assertLessEqual(float(rows[0]["fair_yes_probability_low"]), float(rows[0]["fair_yes_probability"]))
            self.assertGreaterEqual(float(rows[0]["fair_yes_probability_high"]), float(rows[0]["fair_yes_probability"]))

    def test_run_kalshi_nonsports_auto_priors_can_restrict_to_mapped_live_tickers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            mapping_csv = base / "canonical_contract_mapping.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Companies",
                        "series_ticker": "KXIPOALPHA",
                        "event_ticker": "KXIPOALPHA-26APR30",
                        "market_ticker": "KXIPOALPHA-26APR30",
                        "event_title": "Will Alpha announce an IPO by Apr 30?",
                        "market_title": "Will Alpha announce an IPO by Apr 30?",
                        "close_time": "2026-04-30T23:59:00+00:00",
                        "hours_to_close": "819",
                        "yes_bid_dollars": "0.04",
                        "yes_bid_size_contracts": "41",
                        "yes_ask_dollars": "0.05",
                        "yes_ask_size_contracts": "51",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "1050",
                        "volume_24h_contracts": "210",
                        "open_interest_contracts": "1250",
                        "ten_dollar_fillable_at_best_ask": "true",
                        "two_sided_book": "true",
                        "execution_fit_score": "62",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "series_ticker": "KXPOL-1",
                        "event_ticker": "KXPOL-1-26APR30",
                        "market_ticker": "KXPOL-1-26APR30",
                        "event_title": "Will policy bill pass by Apr 30?",
                        "market_title": "Will policy bill pass by Apr 30?",
                        "close_time": "2026-04-30T23:59:00+00:00",
                        "hours_to_close": "819",
                        "yes_bid_dollars": "0.45",
                        "yes_bid_size_contracts": "41",
                        "yes_ask_dollars": "0.46",
                        "yes_ask_size_contracts": "51",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "1050",
                        "volume_24h_contracts": "210",
                        "open_interest_contracts": "1250",
                        "ten_dollar_fillable_at_best_ask": "true",
                        "two_sided_book": "true",
                        "execution_fit_score": "62",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])
            _write_csv(
                mapping_csv,
                [
                    "canonical_ticker",
                    "niche",
                    "execution_phase",
                    "market_description",
                    "settlement_source",
                    "settlement_source_url",
                    "release_time_et",
                    "schedule_source_url",
                    "schedule_needs_nightly_poll",
                    "schedule_holiday_shift_risk",
                    "source_timestamp_rule",
                    "mispricing_hypothesis",
                    "confounders",
                    "mapping_status",
                    "live_event_ticker",
                    "live_market_ticker",
                    "mapping_confidence",
                    "mapping_notes",
                    "last_mapped_at",
                ],
                [
                    {
                        "canonical_ticker": "MX_TEST",
                        "niche": "macro_release",
                        "execution_phase": "phase1_live",
                        "market_description": "test",
                        "settlement_source": "test",
                        "settlement_source_url": "https://example.com",
                        "release_time_et": "08:30",
                        "schedule_source_url": "https://example.com",
                        "schedule_needs_nightly_poll": "true",
                        "schedule_holiday_shift_risk": "true",
                        "source_timestamp_rule": "test",
                        "mispricing_hypothesis": "test",
                        "confounders": "test",
                        "mapping_status": "mapped",
                        "live_event_ticker": "KXIPOALPHA-26APR30",
                        "live_market_ticker": "KXIPOALPHA-26APR30",
                        "mapping_confidence": "0.9",
                        "mapping_notes": "test",
                        "last_mapped_at": "2026-03-28T00:00:00+00:00",
                    }
                ],
            )

            summary = run_kalshi_nonsports_auto_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                canonical_mapping_csv=str(mapping_csv),
                restrict_to_mapped_live_tickers=True,
                allowed_canonical_niches=("macro_release",),
                output_dir=str(base),
                news_getter=_fake_news_getter,
                now=datetime(2026, 3, 28, 13, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["generated_priors"], 1)
            self.assertEqual(summary["top_market_ticker"], "KXIPOALPHA-26APR30")
            self.assertGreaterEqual(int(summary["candidate_markets_filtered_out"] or 0), 1)

    def test_run_kalshi_nonsports_auto_priors_can_use_canonical_alias_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            mapping_csv = base / "canonical_contract_mapping.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Economics",
                        "series_ticker": "KXECONSTATCPIYOY",
                        "event_ticker": "KXECONSTATCPIYOY-26JUN",
                        "market_ticker": "KXECONSTATCPIYOY-26JUN-T3.3",
                        "event_title": "CPI year-over-year in Jun 2026?",
                        "market_title": "CPI year-over-year in Jun 2026?",
                        "close_time": "2026-07-10T12:29:00+00:00",
                        "hours_to_close": "900",
                        "yes_bid_dollars": "0.10",
                        "yes_bid_size_contracts": "41",
                        "yes_ask_dollars": "0.11",
                        "yes_ask_size_contracts": "51",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "1050",
                        "volume_24h_contracts": "210",
                        "open_interest_contracts": "1250",
                        "ten_dollar_fillable_at_best_ask": "true",
                        "two_sided_book": "true",
                        "execution_fit_score": "62",
                    }
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])
            _write_csv(
                mapping_csv,
                [
                    "canonical_ticker",
                    "niche",
                    "execution_phase",
                    "market_description",
                    "settlement_source",
                    "settlement_source_url",
                    "release_time_et",
                    "schedule_source_url",
                    "schedule_needs_nightly_poll",
                    "schedule_holiday_shift_risk",
                    "source_timestamp_rule",
                    "mispricing_hypothesis",
                    "confounders",
                    "mapping_status",
                    "live_event_ticker",
                    "live_market_ticker",
                    "mapping_confidence",
                    "mapping_notes",
                    "last_mapped_at",
                ],
                [
                    {
                        "canonical_ticker": "MX03_CPI_HEADLINE_YOY",
                        "niche": "macro_release",
                        "execution_phase": "phase1_live",
                        "market_description": "test",
                        "settlement_source": "test",
                        "settlement_source_url": "https://example.com",
                        "release_time_et": "08:30",
                        "schedule_source_url": "https://example.com",
                        "schedule_needs_nightly_poll": "true",
                        "schedule_holiday_shift_risk": "true",
                        "source_timestamp_rule": "test",
                        "mispricing_hypothesis": "test",
                        "confounders": "test",
                        "mapping_status": "mapped",
                        "live_event_ticker": "KXECONSTATCPIYOY-26MAY",
                        "live_market_ticker": "KXECONSTATCPIYOY-26MAY-T3.3",
                        "mapping_confidence": "0.9",
                        "mapping_notes": "test",
                        "last_mapped_at": "2026-03-28T00:00:00+00:00",
                    }
                ],
            )

            summary = run_kalshi_nonsports_auto_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                canonical_mapping_csv=str(mapping_csv),
                restrict_to_mapped_live_tickers=True,
                allowed_canonical_niches=("macro_release",),
                output_dir=str(base),
                news_getter=_fake_news_getter,
                now=datetime(2026, 3, 28, 13, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["generated_priors"], 1)
            self.assertEqual(summary["top_market_ticker"], "KXECONSTATCPIYOY-26JUN-T3.3")

    def test_refresh_fallback_does_not_write_back_zero_evidence_auto_prior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-27T22:00:00+00:00",
                        "category": "Politics",
                        "series_ticker": "KXGABBARDOUT",
                        "event_ticker": "KXGABBARDOUT-26",
                        "market_ticker": "KXGABBARDOUT-26-APR01",
                        "event_title": "Tulsi Gabbard out as director of national intelligence?",
                        "market_title": "Will Tulsi Gabbard leaves Director of National Intelligence (DNI) before Apr 1, 2026?",
                        "close_time": "2026-04-01T03:59:00+00:00",
                        "hours_to_close": "96",
                        "yes_bid_dollars": "0.02",
                        "yes_bid_size_contracts": "10",
                        "yes_ask_dollars": "0.03",
                        "yes_ask_size_contracts": "12",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "100",
                        "volume_24h_contracts": "40",
                        "open_interest_contracts": "500",
                        "ten_dollar_fillable_at_best_ask": "false",
                        "two_sided_book": "false",
                        "execution_fit_score": "25",
                    },
                ],
            )
            _write_csv(
                priors_csv,
                PRIOR_FIELDNAMES,
                [
                    {
                        "market_ticker": "KXGABBARDOUT-26-APR01",
                        "fair_yes_probability": "0.07",
                        "fair_yes_probability_low": "0.03",
                        "fair_yes_probability_high": "0.12",
                        "confidence": "0.41",
                        "thesis": "Older auto prior",
                        "source_note": "Older evidence",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                        "evidence_count": "3",
                        "evidence_quality": "0.8",
                        "source_type": "auto",
                        "last_evidence_at": "2026-03-27T20:00:00+00:00",
                    }
                ],
            )

            summary = run_kalshi_nonsports_auto_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                news_getter=_failing_news_getter,
                now=datetime(2026, 3, 28, 13, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["generated_priors"], 1)
            self.assertEqual(summary["updated_rows"], 0)
            self.assertEqual(summary["writeback_blocked_rows"], 1)
            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(rows[0]["fair_yes_probability"], "0.07")
            self.assertEqual(rows[0]["evidence_count"], "3")

    def test_upsert_priors_csv_protects_manual_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            priors_csv = base / "priors.csv"
            _write_csv(
                priors_csv,
                PRIOR_FIELDNAMES,
                [
                    {
                        "market_ticker": "KXIPOALPHA-26APR30",
                        "fair_yes_probability": "0.02",
                        "fair_yes_probability_low": "0.01",
                        "fair_yes_probability_high": "0.03",
                        "confidence": "0.75",
                        "thesis": "Manual thesis",
                        "source_note": "Manual source",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                        "evidence_count": "",
                        "evidence_quality": "",
                        "source_type": "manual",
                        "last_evidence_at": "",
                    }
                ],
            )
            result = _upsert_priors_csv(
                priors_path=priors_csv,
                auto_rows=[
                    {
                        "market_ticker": "KXIPOALPHA-26APR30",
                        "fair_yes_probability": 0.5,
                        "fair_yes_probability_low": 0.4,
                        "fair_yes_probability_high": 0.6,
                        "confidence": 0.4,
                        "thesis": "Auto thesis",
                        "source_note": "Auto source",
                        "updated_at": "2026-03-28T12:00:00+00:00",
                        "source_type": "auto",
                    }
                ],
                protect_manual=True,
            )

            self.assertEqual(result["skipped_manual"], 1)
            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(rows[0]["fair_yes_probability"], "0.02")
            self.assertEqual(rows[0]["thesis"], "Manual thesis")


if __name__ == "__main__":
    unittest.main()
