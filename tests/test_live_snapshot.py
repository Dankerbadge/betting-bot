import tempfile
import json
from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse

from betbot.live_snapshot import run_live_snapshot


class LiveSnapshotTests(unittest.TestCase):
    def test_live_snapshot_passes_with_fake_clients(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=prod\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=xyz789\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                parsed = urlparse(url)
                if parsed.netloc == "api.elections.kalshi.com":
                    return 200, {"balance": 150, "portfolio_value": 200, "updated_ts": 123}
                params = parse_qs(parsed.query)
                self.assertEqual(params["key"], ["xyz789"])
                return 200, {"sports": [{"sport_id": 4}, {"sport_id": 6}, {"sport_id": 8}]}

            summary = run_live_snapshot(
                env_file=str(env_file),
                output_dir=str(base),
                sports_preview_limit=2,
                http_get_json=fake_http_get_json,
                sign_request=lambda *_: "signed-payload",
            )

            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["checks_failed"], 0)
            self.assertEqual(summary["kalshi"]["balance_cents"], 150)
            self.assertEqual(summary["therundown"]["sports_count"], 3)
            self.assertEqual(len(summary["therundown"]["sports_preview"]), 2)
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_live_snapshot_fails_when_provider_unsupported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=prod\n"
                    "ODDS_PROVIDER=opticodds\n"
                    "OPTICODDS_API_KEY=opt123\n"
                ),
                encoding="utf-8",
            )

            summary = run_live_snapshot(
                env_file=str(env_file),
                output_dir=str(base),
                http_get_json=lambda *_: (200, {"balance": 150}),
                sign_request=lambda *_: "signed-payload",
            )

            self.assertEqual(summary["status"], "failed")
            self.assertEqual(summary["checks_failed"], 1)
            self.assertEqual(summary["failed"][0]["component"], "odds_provider")

    def test_live_snapshot_uses_provider_artifact_for_non_therundown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=prod\n"
                    "ODDS_PROVIDER=opticodds\n"
                    "OPTICODDS_API_KEY=opt123\n"
                ),
                encoding="utf-8",
            )
            (base / "live_candidates_summary_4_2026-04-21_20260421_010101.json").write_text(
                json.dumps(
                    {
                        "captured_at": datetime.now(timezone.utc).isoformat(),
                        "status": "ready",
                        "candidates_written": 4,
                        "market_pairs_with_consensus": 3,
                        "positive_ev_candidates": 1,
                    }
                ),
                encoding="utf-8",
            )

            summary = run_live_snapshot(
                env_file=str(env_file),
                output_dir=str(base),
                http_get_json=lambda *_: (200, {"balance": 150}),
                sign_request=lambda *_: "signed-payload",
            )

            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["checks_failed"], 0)
            self.assertTrue(summary["odds_provider_snapshot"]["ready_for_smoke"])

    def test_live_snapshot_retries_transient_network_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=prod\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=xyz789\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            attempts = {"kalshi": 0, "therundown": 0}

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                parsed = urlparse(url)
                if parsed.netloc == "api.elections.kalshi.com":
                    attempts["kalshi"] += 1
                    if attempts["kalshi"] < 2:
                        raise URLError("dns failed")
                    return 200, {"balance": 150, "portfolio_value": 200, "updated_ts": 123}
                attempts["therundown"] += 1
                if attempts["therundown"] < 2:
                    raise URLError("dns failed")
                return 200, {"sports": [{"sport_id": 4}]}

            with patch("betbot.live_snapshot.time.sleep") as mock_sleep:
                summary = run_live_snapshot(
                    env_file=str(env_file),
                    output_dir=str(base),
                    sports_preview_limit=2,
                    http_get_json=fake_http_get_json,
                    sign_request=lambda *_: "signed-payload",
                )

            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["checks_failed"], 0)
            self.assertEqual(attempts["kalshi"], 2)
            self.assertEqual(attempts["therundown"], 2)
            self.assertEqual(mock_sleep.call_count, 2)

    def test_live_snapshot_retries_transient_401_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=prod\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=xyz789\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            attempts = {"kalshi": 0, "therundown": 0}

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                parsed = urlparse(url)
                if parsed.netloc == "api.elections.kalshi.com":
                    attempts["kalshi"] += 1
                    if attempts["kalshi"] < 2:
                        return 401, {"error": "temporary auth failure"}
                    return 200, {"balance": 150, "portfolio_value": 200, "updated_ts": 123}
                attempts["therundown"] += 1
                if attempts["therundown"] < 2:
                    return 401, {"error": "temporary auth failure"}
                return 200, {"sports": [{"sport_id": 4}]}

            with patch("betbot.live_snapshot.time.sleep") as mock_sleep:
                summary = run_live_snapshot(
                    env_file=str(env_file),
                    output_dir=str(base),
                    sports_preview_limit=2,
                    http_get_json=fake_http_get_json,
                    sign_request=lambda *_: "signed-payload",
                )

            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["checks_failed"], 0)
            self.assertEqual(attempts["kalshi"], 2)
            self.assertEqual(attempts["therundown"], 2)
            self.assertEqual(mock_sleep.call_count, 2)

    def test_live_snapshot_fails_over_kalshi_host_when_primary_dns_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=prod\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=xyz789\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            seen_hosts: list[str] = []

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                _ = headers
                _ = timeout_seconds
                host = (urlparse(url).netloc or "").strip().lower()
                seen_hosts.append(host)
                if host == "api.elections.kalshi.com":
                    raise URLError("dns failed")
                if host == "trading-api.kalshi.com":
                    return 200, {"balance": 250, "portfolio_value": 300, "updated_ts": 123}
                return 200, {"sports": [{"sport_id": 4}]}

            with patch("betbot.live_snapshot.time.sleep") as mock_sleep:
                summary = run_live_snapshot(
                    env_file=str(env_file),
                    output_dir=str(base),
                    sports_preview_limit=2,
                    http_get_json=fake_http_get_json,
                    sign_request=lambda *_: "signed-payload",
                )

            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["kalshi"]["api_root_used"], "https://trading-api.kalshi.com/trade-api/v2")
            self.assertIn("api.elections.kalshi.com", seen_hosts)
            self.assertIn("trading-api.kalshi.com", seen_hosts)
            self.assertGreaterEqual(mock_sleep.call_count, 2)

    def test_live_snapshot_fails_after_network_retry_budget_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=prod\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=xyz789\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            with patch("betbot.live_snapshot.time.sleep") as mock_sleep:
                summary = run_live_snapshot(
                    env_file=str(env_file),
                    output_dir=str(base),
                    sports_preview_limit=2,
                    http_get_json=fake_http_get_json,
                    sign_request=lambda *_: "signed-payload",
            )

            self.assertEqual(summary["status"], "failed")
            self.assertEqual(summary["checks_failed"], 2)
            self.assertEqual(mock_sleep.call_count, 6)
            messages = [item["message"] for item in summary["failed"]]
            self.assertTrue(any("dns failed" in message for message in messages))


if __name__ == "__main__":
    unittest.main()
