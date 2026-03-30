from datetime import datetime
import tempfile
from pathlib import Path
import unittest

from betbot.research_audit import (
    COMPLIANCE_REQUIRED_TYPES,
    EXECUTION_REQUIRED_DIMENSIONS,
    SETTLEMENT_REQUIRED_KEYS,
    run_research_audit,
)


class ResearchAuditTests(unittest.TestCase):
    def _write_csv(self, path: Path, header: list[str], rows: list[list[str]]) -> None:
        content = [",".join(header)]
        for row in rows:
            content.append(",".join(row))
        path.write_text("\n".join(content) + "\n", encoding="utf-8")

    def test_research_audit_ready_when_all_confirmed(self) -> None:
        now = datetime.now().isoformat()
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            settlement_rows: list[list[str]] = []
            for key in SETTLEMENT_REQUIRED_KEYS:
                settlement_rows.append(
                    ["kalshi", "mlb", "moneyline", "pregame", key, "ok", "https://example.com", now, "confirmed", ""]
                )
            self._write_csv(
                base / "settlement_matrix.csv",
                [
                    "venue",
                    "league",
                    "market_type",
                    "timing",
                    "rule_key",
                    "rule_value",
                    "source_url",
                    "last_verified",
                    "status",
                    "notes",
                ],
                settlement_rows,
            )

            execution_rows: list[list[str]] = []
            for dim in EXECUTION_REQUIRED_DIMENSIONS:
                execution_rows.append(
                    ["kalshi", dim, "ok", "https://example.com", now, "confirmed", ""]
                )
            self._write_csv(
                base / "execution_envelope.csv",
                ["venue", "dimension", "value", "source_url", "last_verified", "status", "notes"],
                execution_rows,
            )

            compliance_rows: list[list[str]] = []
            for ctype in COMPLIANCE_REQUIRED_TYPES:
                compliance_rows.append(
                    ["new_york", "kalshi", ctype, "ok", "https://example.com", now, "confirmed", ""]
                )
            self._write_csv(
                base / "compliance_matrix.csv",
                [
                    "jurisdiction",
                    "venue",
                    "constraint_type",
                    "constraint",
                    "source_url",
                    "last_verified",
                    "status",
                    "notes",
                ],
                compliance_rows,
            )

            summary = run_research_audit(
                research_dir=str(base),
                venues=["kalshi"],
                jurisdictions=["new_york"],
                output_dir=str(base),
            )
            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["counts"]["findings_high"], 0)

    def test_research_audit_blocked_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "settlement_matrix.csv").write_text(
                "venue,league,market_type,timing,rule_key,rule_value,source_url,last_verified,status,notes\n",
                encoding="utf-8",
            )
            (base / "execution_envelope.csv").write_text(
                "venue,dimension,value,source_url,last_verified,status,notes\n",
                encoding="utf-8",
            )
            (base / "compliance_matrix.csv").write_text(
                "jurisdiction,venue,constraint_type,constraint,source_url,last_verified,status,notes\n",
                encoding="utf-8",
            )
            summary = run_research_audit(
                research_dir=str(base),
                venues=["kalshi"],
                jurisdictions=["new_york"],
                output_dir=str(base),
            )
            self.assertEqual(summary["status"], "blocked")
            self.assertGreater(summary["counts"]["findings_high"], 0)


if __name__ == "__main__":
    unittest.main()

