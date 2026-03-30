import tempfile
from pathlib import Path
import unittest

from betbot.onboarding import run_onboarding_check


class OnboardingTests(unittest.TestCase):
    def test_onboarding_blocked_with_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ACCESS_KEY_ID=TODO\n"
                    "KALSHI_PRIVATE_KEY_PATH=TODO\n"
                    "KALSHI_ENV=demo\n"
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=TODO\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                    "BETBOT_JURISDICTION=new_york\n"
                ),
                encoding="utf-8",
            )
            summary = run_onboarding_check(env_file=str(env_file), output_dir=str(base))
            self.assertEqual(summary["status"], "blocked")
            self.assertGreater(summary["checks_failed"], 0)

    def test_onboarding_ready_when_all_present(self) -> None:
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
                    "BETBOT_TIMEZONE=America/New_York\n"
                    "BETBOT_JURISDICTION=new_york\n"
                ),
                encoding="utf-8",
            )
            summary = run_onboarding_check(env_file=str(env_file), output_dir=str(base))
            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["checks_failed"], 0)
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
