from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path


SETTLEMENT_REQUIRED_KEYS = (
    "game_start_day_rule",
    "postponed_rule",
    "suspended_rule",
    "minimum_innings_rule",
    "extra_innings_rule",
    "pitcher_change_rule",
    "prop_participation_rule",
    "unequivocally_determined_rule",
)

EXECUTION_REQUIRED_DIMENSIONS = (
    "read_rate_limit",
    "write_rate_limit",
    "min_order_size",
    "max_order_size",
    "price_bounds",
    "reject_reasons",
    "maintenance_window",
    "exchange_pause_behavior",
    "idempotency_key",
    "api_auth_method",
)

COMPLIANCE_REQUIRED_TYPES = (
    "jurisdiction_allowed",
    "tos_automation_allowed",
    "data_licensing_allowed",
    "account_kyc_required",
    "restricted_markets",
)

VALID_STATUS = {"confirmed", "in_progress", "todo", "blocked"}


@dataclass(frozen=True)
class AuditFinding:
    domain: str
    key: str
    severity: str
    message: str


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _parse_date_safe(value: str | None) -> bool:
    if not value:
        return False
    try:
        datetime.fromisoformat(value.strip())
        return True
    except ValueError:
        return False


def _validate_common_fields(row: dict, domain: str, key: str) -> list[AuditFinding]:
    findings: list[AuditFinding] = []
    status = _normalize(row.get("status"))
    if status not in VALID_STATUS:
        findings.append(
            AuditFinding(
                domain=domain,
                key=key,
                severity="high",
                message=f"Invalid status '{row.get('status')}' (expected one of {sorted(VALID_STATUS)}).",
            )
        )
    if not row.get("source_url", "").strip():
        findings.append(
            AuditFinding(
                domain=domain,
                key=key,
                severity="high",
                message="Missing source_url.",
            )
        )
    if not _parse_date_safe(row.get("last_verified")):
        findings.append(
            AuditFinding(
                domain=domain,
                key=key,
                severity="medium",
                message="Missing or invalid last_verified ISO datetime.",
            )
        )
    return findings


def _score_rows(confirmed: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return confirmed / total


def run_research_audit(
    *,
    research_dir: str,
    venues: list[str],
    jurisdictions: list[str],
    output_dir: str = "outputs",
) -> dict:
    venue_set = {_normalize(v) for v in venues if v.strip()}
    jurisdiction_set = {_normalize(j) for j in jurisdictions if j.strip()}
    if not venue_set:
        raise ValueError("At least one venue is required")
    if not jurisdiction_set:
        raise ValueError("At least one jurisdiction is required")

    base = Path(research_dir)
    settlement_rows = _read_csv_rows(base / "settlement_matrix.csv")
    execution_rows = _read_csv_rows(base / "execution_envelope.csv")
    compliance_rows = _read_csv_rows(base / "compliance_matrix.csv")

    findings: list[AuditFinding] = []

    settlement_index: dict[tuple[str, str], dict] = {}
    for row in settlement_rows:
        venue = _normalize(row.get("venue"))
        key = _normalize(row.get("rule_key"))
        if venue and key:
            settlement_index[(venue, key)] = row

    execution_index: dict[tuple[str, str], dict] = {}
    for row in execution_rows:
        venue = _normalize(row.get("venue"))
        dim = _normalize(row.get("dimension"))
        if venue and dim:
            execution_index[(venue, dim)] = row

    compliance_index: dict[tuple[str, str, str], dict] = {}
    for row in compliance_rows:
        jurisdiction = _normalize(row.get("jurisdiction"))
        venue = _normalize(row.get("venue"))
        ctype = _normalize(row.get("constraint_type"))
        if jurisdiction and venue and ctype:
            compliance_index[(jurisdiction, venue, ctype)] = row

    settlement_total = 0
    settlement_confirmed = 0
    for venue in venue_set:
        for key in SETTLEMENT_REQUIRED_KEYS:
            settlement_total += 1
            row = settlement_index.get((venue, key))
            identity = f"{venue}:{key}"
            if row is None:
                findings.append(
                    AuditFinding(
                        domain="settlement",
                        key=identity,
                        severity="high",
                        message="Missing required settlement rule entry.",
                    )
                )
                continue
            findings.extend(_validate_common_fields(row, "settlement", identity))
            status = _normalize(row.get("status"))
            if status == "confirmed":
                settlement_confirmed += 1
            elif status in {"blocked", "todo"}:
                findings.append(
                    AuditFinding(
                        domain="settlement",
                        key=identity,
                        severity="high",
                        message=f"Status is '{status}', not production ready.",
                    )
                )

    execution_total = 0
    execution_confirmed = 0
    for venue in venue_set:
        for dim in EXECUTION_REQUIRED_DIMENSIONS:
            execution_total += 1
            row = execution_index.get((venue, dim))
            identity = f"{venue}:{dim}"
            if row is None:
                findings.append(
                    AuditFinding(
                        domain="execution",
                        key=identity,
                        severity="high",
                        message="Missing required execution constraint entry.",
                    )
                )
                continue
            findings.extend(_validate_common_fields(row, "execution", identity))
            status = _normalize(row.get("status"))
            if status == "confirmed":
                execution_confirmed += 1
            elif status in {"blocked", "todo"}:
                findings.append(
                    AuditFinding(
                        domain="execution",
                        key=identity,
                        severity="high",
                        message=f"Status is '{status}', not production ready.",
                    )
                )

    compliance_total = 0
    compliance_confirmed = 0
    for jurisdiction in jurisdiction_set:
        for venue in venue_set:
            for ctype in COMPLIANCE_REQUIRED_TYPES:
                compliance_total += 1
                row = compliance_index.get((jurisdiction, venue, ctype))
                identity = f"{jurisdiction}:{venue}:{ctype}"
                if row is None:
                    findings.append(
                        AuditFinding(
                            domain="compliance",
                            key=identity,
                            severity="high",
                            message="Missing required compliance entry.",
                        )
                    )
                    continue
                findings.extend(_validate_common_fields(row, "compliance", identity))
                status = _normalize(row.get("status"))
                if status == "confirmed":
                    compliance_confirmed += 1
                elif status in {"blocked", "todo"}:
                    findings.append(
                        AuditFinding(
                            domain="compliance",
                            key=identity,
                            severity="high",
                            message=f"Status is '{status}', not production ready.",
                        )
                    )

    settlement_score = _score_rows(settlement_confirmed, settlement_total)
    execution_score = _score_rows(execution_confirmed, execution_total)
    compliance_score = _score_rows(compliance_confirmed, compliance_total)
    overall_score = (settlement_score + execution_score + compliance_score) / 3.0

    high_findings = [f for f in findings if f.severity == "high"]
    medium_findings = [f for f in findings if f.severity == "medium"]

    summary = {
        "research_dir": str(base),
        "venues": sorted(venue_set),
        "jurisdictions": sorted(jurisdiction_set),
        "scores": {
            "settlement": round(settlement_score, 6),
            "execution": round(execution_score, 6),
            "compliance": round(compliance_score, 6),
            "overall": round(overall_score, 6),
        },
        "status": "ready" if not high_findings else "blocked",
        "counts": {
            "findings_total": len(findings),
            "findings_high": len(high_findings),
            "findings_medium": len(medium_findings),
            "settlement_required": settlement_total,
            "execution_required": execution_total,
            "compliance_required": compliance_total,
        },
        "blockers": [
            {"domain": f.domain, "key": f.key, "message": f.message}
            for f in high_findings
        ],
        "warnings": [
            {"domain": f.domain, "key": f.key, "message": f.message}
            for f in medium_findings
        ],
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"research_audit_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary

