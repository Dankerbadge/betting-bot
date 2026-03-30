from __future__ import annotations

import contextlib
import importlib.util
import io
import pathlib
import unittest
from unittest import mock


def _load_secret_check_module():
    root = pathlib.Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "check_no_tracked_secrets.py"
    spec = importlib.util.spec_from_file_location("check_no_tracked_secrets", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load check_no_tracked_secrets.py module spec.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SecretPathGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_secret_check_module()

    def test_find_offending_paths_respects_allowlist(self) -> None:
        module = self.module
        offenders = module.find_offending_paths(
            [
                "data/research/account_onboarding.local.env",
                ".secrets/README.md",
                ".secrets/kalshi_private_key.pem",
                "notes.txt",
            ]
        )
        self.assertEqual(
            offenders,
            [
                ".secrets/kalshi_private_key.pem",
                "data/research/account_onboarding.local.env",
            ],
        )

    def test_main_returns_zero_when_no_candidates(self) -> None:
        module = self.module
        out = io.StringIO()
        with mock.patch.object(module, "_candidate_paths", return_value=[]):
            with contextlib.redirect_stdout(out):
                rc = module.main()
        self.assertEqual(rc, 0)
        self.assertIn("No git tracked or staged files found", out.getvalue())

    def test_main_returns_failure_on_offenders(self) -> None:
        module = self.module
        err = io.StringIO()
        with mock.patch.object(
            module,
            "_candidate_paths",
            return_value=["README.md", ".secrets/kalshi_private_key.pem"],
        ):
            with contextlib.redirect_stderr(err):
                rc = module.main()
        self.assertEqual(rc, 1)
        self.assertIn("Tracked secret-path violations detected", err.getvalue())


if __name__ == "__main__":
    unittest.main()
