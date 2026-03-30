import tempfile
import sys
from pathlib import Path
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse

from betbot.cli import main as cli_main
from betbot.live_smoke import _http_get_json, kalshi_api_root_candidates, run_live_smoke


class LiveSmokeTests(unittest.TestCase):
    def test_kalshi_api_root_candidates_include_prod_failover(self) -> None:
        roots = kalshi_api_root_candidates("prod")
        self.assertGreaterEqual(len(roots), 2)
        self.assertEqual(roots[0], "https://api.elections.kalshi.com/trade-api/v2")
        self.assertIn("https://trading-api.kalshi.com/trade-api/v2", roots)

    def test_run_live_smoke_fails_over_kalshi_api_root_after_dns_error(self) -> None:
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
            kalshi_hosts_seen: list[str] = []

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                parsed = urlparse(url)
                if parsed.path.endswith("/portfolio/balance"):
                    kalshi_hosts_seen.append(parsed.netloc)
                    if parsed.netloc == "api.elections.kalshi.com":
                        raise URLError("[Errno 8] nodename nor servname provided, or not known")
                    if parsed.netloc == "trading-api.kalshi.com":
                        return 200, {"balance": 777}
                return 200, {"sports": [{"sport_id": 4}]}

            summary = run_live_smoke(
                env_file=str(env_file),
                output_dir=str(base),
                http_get_json=fake_http_get_json,
                sign_request=lambda *_: "signed-payload",
            )

            self.assertEqual(summary["status"], "passed")
            self.assertIn("api.elections.kalshi.com", kalshi_hosts_seen)
            self.assertIn("trading-api.kalshi.com", kalshi_hosts_seen)
            kalshi_check = summary["checks"][0]
            self.assertEqual(kalshi_check["details"]["api_root_used"], "https://trading-api.kalshi.com/trade-api/v2")
            self.assertGreaterEqual(len(kalshi_check["details"]["attempted_api_roots"]), 2)

    def test_http_get_json_retries_transient_dns_then_succeeds(self) -> None:
        class _FakeResponse:
            def __enter__(self) -> "_FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"ok": true}'

        with patch(
            "betbot.live_smoke.urlopen",
            side_effect=[URLError("[Errno 8] nodename nor servname provided, or not known"), _FakeResponse()],
        ) as mock_urlopen, patch("betbot.live_smoke.time.sleep") as mock_sleep:
            status_code, payload = _http_get_json(
                "https://example.com/health",
                {"User-Agent": "test"},
                5.0,
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload, {"ok": True})
        self.assertEqual(mock_urlopen.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)

    def test_run_live_smoke_passes_with_fake_clients(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=demo\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=xyz789\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            captured_calls: list[tuple[str, dict[str, str], float]] = []

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                captured_calls.append((url, headers, timeout_seconds))
                parsed = urlparse(url)
                if parsed.netloc == "demo-api.kalshi.co":
                    return 200, {"balance": 12345}
                params = parse_qs(parsed.query)
                self.assertEqual(params["key"], ["xyz789"])
                return 200, {"sports": [{"sport_id": 4}, {"sport_id": 6}]}

            def fake_sign_request(
                private_key_path: str,
                timestamp_ms: str,
                method: str,
                path: str,
            ) -> str:
                self.assertEqual(str(key_path), private_key_path)
                self.assertEqual(method, "GET")
                self.assertEqual(path, "/trade-api/v2/portfolio/balance")
                self.assertTrue(timestamp_ms.isdigit())
                return "signed-payload"

            summary = run_live_smoke(
                env_file=str(env_file),
                output_dir=str(base),
                http_get_json=fake_http_get_json,
                sign_request=fake_sign_request,
            )

            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["checks_failed"], 0)
            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertEqual(len(captured_calls), 2)
            kalshi_headers = captured_calls[0][1]
            self.assertEqual(kalshi_headers["KALSHI-ACCESS-KEY"], "abc123")
            self.assertEqual(kalshi_headers["KALSHI-ACCESS-SIGNATURE"], "signed-payload")

    def test_run_live_smoke_fails_for_unsupported_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=demo\n"
                    "ODDS_PROVIDER=opticodds\n"
                    "OPTICODDS_API_KEY=opt123\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {"balance": 50}

            summary = run_live_smoke(
                env_file=str(env_file),
                output_dir=str(base),
                http_get_json=fake_http_get_json,
                sign_request=lambda *_: "signed-payload",
            )

            self.assertEqual(summary["status"], "failed")
            self.assertEqual(summary["checks_failed"], 1)
            self.assertEqual(summary["failed"][0]["component"], "odds_provider")

    def test_run_live_smoke_flags_kalshi_env_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=demo\n"
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
                if parsed.netloc == "demo-api.kalshi.co":
                    return 401, {"error": {"code": "authentication_error", "details": "NOT_FOUND"}}
                if parsed.netloc == "api.elections.kalshi.com":
                    return 200, {"balance": 500}
                return 200, {"sports": [{"sport_id": 4}]}

            summary = run_live_smoke(
                env_file=str(env_file),
                output_dir=str(base),
                http_get_json=fake_http_get_json,
                sign_request=lambda *_: "signed-payload",
            )

            self.assertEqual(summary["status"], "failed")
            kalshi_check = summary["checks"][0]
            self.assertIn("but 'prod' succeeded", kalshi_check["message"])
            self.assertEqual(kalshi_check["details"]["suggested_env"], "prod")

    def test_run_live_smoke_retries_transient_dns_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=demo\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=xyz789\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            counts = {"kalshi": 0, "therundown": 0}

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                parsed = urlparse(url)
                if parsed.netloc == "demo-api.kalshi.co":
                    counts["kalshi"] += 1
                    if counts["kalshi"] < 2:
                        raise URLError("[Errno 8] nodename nor servname provided, or not known")
                    return 200, {"balance": 12345}
                counts["therundown"] += 1
                if counts["therundown"] < 2:
                    raise URLError("[Errno 8] nodename nor servname provided, or not known")
                return 200, {"sports": [{"sport_id": 4}]}

            with patch("betbot.live_smoke.time.sleep") as mock_sleep:
                summary = run_live_smoke(
                    env_file=str(env_file),
                    output_dir=str(base),
                    http_get_json=fake_http_get_json,
                    sign_request=lambda *_: "signed-payload",
                )

            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["checks_failed"], 0)
            self.assertEqual(counts["kalshi"], 2)
            self.assertEqual(counts["therundown"], 2)
            self.assertEqual(mock_sleep.call_count, 2)

    def test_run_live_smoke_skips_odds_provider_check_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=demo\n"
                    "ODDS_PROVIDER=therundown\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {"balance": 123}

            summary = run_live_smoke(
                env_file=str(env_file),
                output_dir=str(base),
                include_odds_provider_check=False,
                http_get_json=fake_http_get_json,
                sign_request=lambda *_: "signed-payload",
            )

            self.assertEqual(summary["status"], "passed")
            self.assertEqual(summary["checks_failed"], 0)
            odds_checks = [row for row in summary["checks"] if row["component"] == "odds_provider"]
            self.assertEqual(len(odds_checks), 1)
            self.assertTrue(odds_checks[0]["ok"])
            self.assertIn("skipped", odds_checks[0]["message"].lower())

    def test_run_live_smoke_fails_after_dns_retry_budget_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            key_path = base / "kalshi.pem"
            key_path.write_text("dummy", encoding="utf-8")
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=abc123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_path}\n"
                    "KALSHI_ENV=demo\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=xyz789\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            call_count = 0

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                nonlocal call_count
                call_count += 1
                raise URLError("[Errno 8] nodename nor servname provided, or not known")

            with patch("betbot.live_smoke.time.sleep") as mock_sleep:
                summary = run_live_smoke(
                    env_file=str(env_file),
                    output_dir=str(base),
                    http_get_json=fake_http_get_json,
                    sign_request=lambda *_: "signed-payload",
                )

            self.assertEqual(summary["status"], "failed")
            self.assertEqual(summary["checks_failed"], 2)
            self.assertEqual(call_count, 6)
            self.assertEqual(mock_sleep.call_count, 4)
            messages = [item["message"] for item in summary["failed"]]
            self.assertTrue(all("Network error:" in message for message in messages))

    def test_live_smoke_cli_exits_nonzero_when_summary_failed(self) -> None:
        with patch(
            "betbot.cli.run_live_smoke",
            return_value={"status": "failed", "checks_failed": 1, "output_file": "outputs/live_smoke.json"},
        ), patch.object(
            sys,
            "argv",
            ["betbot", "live-smoke", "--env-file", "dummy.env"],
        ):
            stdout = StringIO()
            with self.assertRaises(SystemExit) as exc, redirect_stdout(stdout):
                cli_main()

        self.assertEqual(exc.exception.code, 1)
        self.assertIn('"status": "failed"', stdout.getvalue())

    def test_live_smoke_cli_keeps_zero_exit_when_summary_passed(self) -> None:
        with patch(
            "betbot.cli.run_live_smoke",
            return_value={"status": "passed", "checks_failed": 0, "output_file": "outputs/live_smoke.json"},
        ), patch.object(
            sys,
            "argv",
            ["betbot", "live-smoke", "--env-file", "dummy.env"],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        self.assertIn('"status": "passed"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
