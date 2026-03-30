import tempfile
from pathlib import Path
import socket
import unittest
from unittest.mock import patch
from urllib.error import URLError
from urllib.request import Request

from betbot.dns_guard import (
    create_connection_with_dns_recovery,
    run_dns_doctor,
    should_attempt_dns_recovery,
    urlopen_with_dns_recovery,
)


class _FakeResponse:
    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return b'{"ok": true}'


class DnsGuardTests(unittest.TestCase):
    def test_should_attempt_dns_recovery_scoped_to_supported_hosts(self) -> None:
        self.assertTrue(should_attempt_dns_recovery("api.elections.kalshi.com"))
        self.assertTrue(should_attempt_dns_recovery("api.therundown.io"))
        self.assertFalse(should_attempt_dns_recovery("example.com"))

    def test_urlopen_with_dns_recovery_retries_when_dns_fails(self) -> None:
        request = Request("https://api.elections.kalshi.com/health")
        calls: list[str] = []

        def fake_open(request_obj: Request, timeout: float):
            _ = timeout
            calls.append(str(request_obj.full_url))
            if len(calls) == 1:
                raise URLError("[Errno 8] nodename nor servname provided, or not known")
            return _FakeResponse()

        with patch("betbot.dns_guard.resolve_host_with_public_dns", return_value=("203.0.113.10",)):
            with urlopen_with_dns_recovery(request, timeout_seconds=3.0, urlopen_fn=fake_open) as response:
                payload = response.read()

        self.assertEqual(payload, b'{"ok": true}')
        self.assertEqual(len(calls), 2)

    def test_urlopen_with_dns_recovery_skips_non_supported_hosts(self) -> None:
        request = Request("https://example.com/health")

        def fake_open(request_obj: Request, timeout: float):
            _ = request_obj
            _ = timeout
            raise URLError("[Errno 8] nodename nor servname provided, or not known")

        with self.assertRaises(URLError):
            urlopen_with_dns_recovery(request, timeout_seconds=3.0, urlopen_fn=fake_open)

    def test_create_connection_with_dns_recovery_uses_resolved_ip_after_dns_error(self) -> None:
        attempted: list[str] = []
        sentinel = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        def fake_create_connection(target: tuple[str, int], timeout: float):
            _ = timeout
            attempted.append(target[0])
            if target[0] == "api.elections.kalshi.com":
                raise socket.gaierror("[Errno 8] nodename nor servname provided, or not known")
            return sentinel

        with patch("betbot.dns_guard.resolve_host_with_public_dns", return_value=("203.0.113.10",)):
            with patch("betbot.dns_guard.socket.create_connection", side_effect=fake_create_connection):
                sock = create_connection_with_dns_recovery(
                    host="api.elections.kalshi.com",
                    port=443,
                    timeout_seconds=2.0,
                )

        self.assertIs(sock, sentinel)
        self.assertEqual(attempted[0], "api.elections.kalshi.com")
        self.assertIn("203.0.113.10", attempted)
        try:
            sentinel.close()
        except OSError:
            pass

    def test_run_dns_doctor_reports_status_and_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                "\n".join(
                    [
                        "KALSHI_ENV=prod",
                        "THERUNDOWN_BASE_URL=https://therundown.io/api/v2",
                    ]
                ),
                encoding="utf-8",
            )

            def fake_system_resolve(host: str) -> tuple[tuple[str, ...], str]:
                if host == "api.elections.kalshi.com":
                    return (), "dns failed"
                return ("198.51.100.2",), ""

            with patch("betbot.dns_guard._system_resolve", side_effect=fake_system_resolve):
                with patch("betbot.dns_guard.resolve_host_with_public_dns", return_value=("203.0.113.10",)):
                    summary = run_dns_doctor(
                        env_file=str(env_file),
                        output_dir=str(base),
                        timeout_seconds=0.2,
                    )

            self.assertIn(summary["status"], {"healthy", "degraded"})
            self.assertGreaterEqual(summary["hosts_checked"], 1)
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
