from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


def _load_preflight_module():
    root = Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "automation_preflight.py"
    spec = importlib.util.spec_from_file_location("automation_preflight", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load automation_preflight.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AutomationPreflightTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_preflight_module()

    def test_trading_profile_ready_with_required_keys(self) -> None:
        module = self.module
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env_file = root / "account_onboarding.local.env"
            key_file = root / "kalshi_private_key.pem"
            key_file.write_text("dummy", encoding="utf-8")
            env_file.write_text(
                "\n".join(
                    [
                        "KALSHI_ACCESS_KEY_ID=abc123",
                        f"KALSHI_PRIVATE_KEY_PATH={key_file}",
                        "KALSHI_ENV=prod",
                        "ODDS_PROVIDER=therundown",
                        "THERUNDOWN_API_KEY=xyz789",
                        "THERUNDOWN_BASE_URL=https://therundown.io/api/v2",
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch.object(module, "_dns_check_host", return_value={"status": "system_ok", "host": "x"}):
                summary = module._run_trading_profile(
                    profile="hourly",
                    repo_root=root,
                    env_file=env_file,
                    timeout_seconds=0.1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["env_resolution"], "requested")

    def test_trading_profile_blocks_missing_required_key(self) -> None:
        module = self.module
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env_file = root / "account_onboarding.local.env"
            key_file = root / "kalshi_private_key.pem"
            key_file.write_text("dummy", encoding="utf-8")
            env_file.write_text(
                "\n".join(
                    [
                        "KALSHI_ACCESS_KEY_ID=abc123",
                        f"KALSHI_PRIVATE_KEY_PATH={key_file}",
                        "KALSHI_ENV=prod",
                        "ODDS_PROVIDER=therundown",
                        "THERUNDOWN_API_KEY=TODO",
                        "THERUNDOWN_BASE_URL=https://therundown.io/api/v2",
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch.object(module, "_dns_check_host", return_value={"status": "system_ok", "host": "x"}):
                summary = module._run_trading_profile(
                    profile="hourly",
                    repo_root=root,
                    env_file=env_file,
                    timeout_seconds=0.1,
                )

            self.assertEqual(summary["status"], "blocked")
            self.assertIn("missing_or_placeholder:THERUNDOWN_API_KEY", summary["errors"])

    def test_supabase_profile_blocks_project_ref_mismatch(self) -> None:
        module = self.module
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            secrets_file = root / "betting-bot-supabase.env"
            secrets_file.write_text(
                "\n".join(
                    [
                        'OPSBOT_SUPABASE_URL="https://abc.supabase.co"',
                        'OPSBOT_SUPABASE_SERVICE_ROLE_KEY="thisisalongservicerolekeyvalue"',
                        'OPSBOT_SUPABASE_PROJECT_REF="xyz"',
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch.object(module, "_dns_check_host", return_value={"status": "system_ok", "host": "abc.supabase.co"}):
                summary = module._run_supabase_sync_profile(
                    repo_root=root,
                    secrets_file=secrets_file,
                    timeout_seconds=0.1,
                )

            self.assertEqual(summary["status"], "blocked")
            self.assertTrue(
                any(str(error).startswith("supabase_url_project_ref_mismatch") for error in summary["errors"])
            )

    def test_main_writes_output_json(self) -> None:
        module = self.module
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            secrets_file = root / "betting-bot-supabase.env"
            output_json = root / "preflight.json"
            secrets_file.write_text(
                "\n".join(
                    [
                        'OPSBOT_SUPABASE_URL="https://abc.supabase.co"',
                        'OPSBOT_SUPABASE_SERVICE_ROLE_KEY="thisisalongservicerolekeyvalue"',
                        'OPSBOT_SUPABASE_PROJECT_REF="abc"',
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch.object(module, "_dns_check_host", return_value={"status": "system_ok", "host": "abc.supabase.co"}):
                rc = module.main(
                    [
                        "--profile",
                        "supabase_sync",
                        "--repo-root",
                        str(root),
                        "--secrets-file",
                        str(secrets_file),
                        "--output-json",
                        str(output_json),
                    ]
                )

            self.assertEqual(rc, 0)
            self.assertTrue(output_json.exists())

    def test_dns_check_host_recovers_via_cli_dns(self) -> None:
        module = self.module
        with mock.patch.object(module.socket, "getaddrinfo", side_effect=OSError("nodename nor servname provided")):
            with mock.patch.object(module, "resolve_host_with_public_dns", return_value=()):
                with mock.patch.object(module, "_resolve_host_with_cli_dns", return_value=("104.18.38.10",)):
                    result = module._dns_check_host("example.supabase.co", timeout_seconds=0.1)

        self.assertEqual(result["status"], "recovered_cli_dns")
        self.assertEqual(result["cli_dns_ips"], ["104.18.38.10"])
        self.assertEqual(result["public_dns_ips"], [])

    def test_dns_check_host_recovers_via_cached_dns(self) -> None:
        module = self.module
        with mock.patch.object(module.socket, "getaddrinfo", side_effect=OSError("nodename nor servname provided")):
            with mock.patch.object(module, "resolve_host_with_public_dns", return_value=()):
                with mock.patch.object(module, "_resolve_host_with_cli_dns", return_value=()):
                    with mock.patch.object(module, "_load_cached_dns_ips", return_value=("172.64.149.246",)):
                        result = module._dns_check_host("example.supabase.co", timeout_seconds=0.1)

        self.assertEqual(result["status"], "recovered_cached_dns")
        self.assertEqual(result["cached_dns_ips"], ["172.64.149.246"])
        self.assertEqual(result["cli_dns_ips"], [])


if __name__ == "__main__":
    unittest.main()
