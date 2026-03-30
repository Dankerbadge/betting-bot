from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path


@dataclass(frozen=True)
class OddsIssue:
    issue_type: str
    severity: str
    key: str
    message: str


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.strip())


def _norm(value: str | None) -> str:
    return (value or "").strip()


def _safe_float(value: str | None) -> float | None:
    try:
        return float(_norm(value))
    except ValueError:
        return None


def run_odds_audit(
    *,
    input_csv: str,
    output_dir: str = "outputs",
    max_gap_minutes: float = 60.0,
) -> dict:
    path = Path(input_csv)
    if not path.exists():
        raise ValueError(f"Input not found: {input_csv}")
    if max_gap_minutes <= 0:
        raise ValueError("max_gap_minutes must be positive")

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"timestamp", "event_id", "market", "book", "odds"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")
        rows = list(reader)

    issues: list[OddsIssue] = []
    groups: dict[tuple[str, str, str], list[tuple[int, datetime, dict]]] = {}
    seen_exact: set[tuple[str, str, str, str]] = set()

    invalid_odds = 0
    malformed_timestamps = 0
    duplicate_exact = 0

    for idx, row in enumerate(rows, start=2):
        event_id = _norm(row.get("event_id"))
        market = _norm(row.get("market"))
        book = _norm(row.get("book"))
        raw_ts = _norm(row.get("timestamp"))
        key = f"{event_id}|{market}|{book}|{raw_ts}"

        if key in seen_exact:
            duplicate_exact += 1
            issues.append(
                OddsIssue(
                    issue_type="duplicate_exact",
                    severity="medium",
                    key=f"line:{idx}",
                    message=f"Duplicate row key {key}",
                )
            )
            continue
        seen_exact.add(key)

        try:
            ts = _parse_ts(raw_ts)
        except ValueError:
            malformed_timestamps += 1
            issues.append(
                OddsIssue(
                    issue_type="malformed_timestamp",
                    severity="high",
                    key=f"line:{idx}",
                    message=f"Invalid timestamp '{raw_ts}'",
                )
            )
            continue

        odds = _safe_float(row.get("odds"))
        if odds is None or odds <= 1.0:
            invalid_odds += 1
            issues.append(
                OddsIssue(
                    issue_type="invalid_odds",
                    severity="high",
                    key=f"line:{idx}",
                    message=f"Invalid odds '{row.get('odds')}'",
                )
            )

        group_key = (event_id, market, book)
        groups.setdefault(group_key, []).append((idx, ts, row))

    non_monotonic_count = 0
    large_gap_count = 0
    missing_close_count = 0
    total_gap_minutes = 0.0
    gap_samples = 0

    for group_key, values in groups.items():
        values.sort(key=lambda x: x[1])

        for i in range(1, len(values)):
            prev_idx, prev_ts, _ = values[i - 1]
            curr_idx, curr_ts, _ = values[i]
            if curr_ts < prev_ts:
                non_monotonic_count += 1
                issues.append(
                    OddsIssue(
                        issue_type="non_monotonic_timestamp",
                        severity="high",
                        key="|".join(group_key),
                        message=f"Timestamp moved backwards between lines {prev_idx} and {curr_idx}",
                    )
                )
            gap_minutes = (curr_ts - prev_ts).total_seconds() / 60.0
            if gap_minutes >= 0:
                total_gap_minutes += gap_minutes
                gap_samples += 1
            if gap_minutes > max_gap_minutes:
                large_gap_count += 1
                issues.append(
                    OddsIssue(
                        issue_type="large_gap",
                        severity="medium",
                        key="|".join(group_key),
                        message=f"Gap {gap_minutes:.2f}m exceeds threshold {max_gap_minutes:.2f}m",
                    )
                )

        # Closing integrity check if commence_time exists.
        has_commence = any(_norm(v[2].get("commence_time")) for v in values)
        if has_commence:
            commence_candidates = []
            for _, _, row in values:
                raw_commence = _norm(row.get("commence_time"))
                if not raw_commence:
                    continue
                try:
                    commence_candidates.append(_parse_ts(raw_commence))
                except ValueError:
                    issues.append(
                        OddsIssue(
                            issue_type="malformed_commence_time",
                            severity="medium",
                            key="|".join(group_key),
                            message=f"Malformed commence_time '{raw_commence}'",
                        )
                    )
            if commence_candidates:
                commence = min(commence_candidates)
                has_prestart_quote = any(ts <= commence for _, ts, _ in values)
                if not has_prestart_quote:
                    missing_close_count += 1
                    issues.append(
                        OddsIssue(
                            issue_type="missing_prestart_quote",
                            severity="high",
                            key="|".join(group_key),
                            message="No quote exists at or before commence_time",
                        )
                    )

    high_issues = [i for i in issues if i.severity == "high"]
    medium_issues = [i for i in issues if i.severity == "medium"]

    avg_gap = total_gap_minutes / gap_samples if gap_samples else None
    quality_score = 1.0
    if rows:
        penalty = (len(high_issues) * 2 + len(medium_issues)) / max(len(rows), 1)
        quality_score = max(0.0, 1.0 - penalty)

    summary = {
        "input_csv": str(path),
        "rows": len(rows),
        "groups": len(groups),
        "max_gap_minutes": max_gap_minutes,
        "metrics": {
            "quality_score": round(quality_score, 6),
            "invalid_odds_count": invalid_odds,
            "malformed_timestamps_count": malformed_timestamps,
            "duplicate_exact_count": duplicate_exact,
            "non_monotonic_count": non_monotonic_count,
            "large_gap_count": large_gap_count,
            "missing_prestart_quote_count": missing_close_count,
            "avg_gap_minutes": None if avg_gap is None else round(avg_gap, 6),
        },
        "status": "ready" if not high_issues else "blocked",
        "issues_high": [
            {"type": i.issue_type, "key": i.key, "message": i.message} for i in high_issues
        ],
        "issues_medium": [
            {"type": i.issue_type, "key": i.key, "message": i.message} for i in medium_issues
        ],
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / f"odds_audit_{stamp}.json"
    issues_path = out_dir / f"odds_audit_issues_{stamp}.csv"

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with issues_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["severity", "issue_type", "key", "message"])
        for issue in issues:
            writer.writerow([issue.severity, issue.issue_type, issue.key, issue.message])

    summary["output_file"] = str(summary_path)
    summary["issues_file"] = str(issues_path)
    return summary

