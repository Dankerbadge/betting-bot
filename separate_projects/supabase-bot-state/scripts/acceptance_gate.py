#!/usr/bin/env python3
"""Acceptance checks for the separate Supabase + dashboard stack.

Checks implemented:
- Isolation guardrail (non-Zenith target)
- Schema/read availability checks
- Dashboard credential read + write-denial probe
- Ingest idempotency (run ingest twice)
- Freshness snapshot extraction
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any
import urllib.error
import urllib.parse
import urllib.request


SCHEMA_NAME = "bot_ops"
EXPECTED_TABLES = [
    "execution_journal",
    "execution_frontier_reports",
    "execution_frontier_report_buckets",
    "climate_availability_events",
    "overnight_runs",
    "pilot_scorecards",
]
EXPECTED_VIEWS = [
    "v_latest_overnight_run",
    "v_frontier_recent",
    "v_climate_activity_24h",
    "v_pilot_scorecards_recent",
]


@dataclass
class HttpResult:
    status: int
    headers: dict[str, str]
    body_text: str
    json_body: Any


@dataclass
class AcceptanceState:
    checks_passed: list[str]
    checks_failed: list[str]
    checks_warned: list[str]

    def pass_check(self, message: str) -> None:
        self.checks_passed.append(message)
        print(f"[pass] {message}")

    def fail_check(self, message: str) -> None:
        self.checks_failed.append(message)
        print(f"[fail] {message}")

    def warn_check(self, message: str) -> None:
        self.checks_warned.append(message)
        print(f"[warn] {message}")


def _parse_json(text: str) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _request(
    *,
    method: str,
    url: str,
    api_key: str,
    schema: str,
    timeout_seconds: float,
    json_payload: Any | None = None,
    extra_headers: dict[str, str] | None = None,
) -> HttpResult:
    headers: dict[str, str] = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Accept-Profile": schema,
    }
    if extra_headers:
        headers.update(extra_headers)

    data = None
    if json_payload is not None:
        data = json.dumps(json_payload, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
        headers["Content-Profile"] = schema

    req = urllib.request.Request(url, method=method.upper(), headers=headers, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            body_text = response.read().decode("utf-8", errors="replace")
            return HttpResult(
                status=response.status,
                headers={k.lower(): v for k, v in response.headers.items()},
                body_text=body_text,
                json_body=_parse_json(body_text),
            )
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        return HttpResult(
            status=exc.code,
            headers={k.lower(): v for k, v in exc.headers.items()},
            body_text=body_text,
            json_body=_parse_json(body_text),
        )


def _rest_url(base_url: str, table_or_view: str, query: dict[str, str] | None = None) -> str:
    path = f"{base_url.rstrip('/')}/rest/v1/{table_or_view}"
    if not query:
        return path
    encoded = urllib.parse.urlencode(query)
    return f"{path}?{encoded}"


def _assert_non_zenith(*, supabase_url: str, project_ref: str, forbidden_hint: str) -> None:
    hint = forbidden_hint.strip().lower()
    if not hint:
        return
    if hint in supabase_url.lower() or hint in project_ref.lower():
        raise RuntimeError(
            "Isolation guardrail failed: configured target appears to reference "
            f"'{hint}'. Use a new, separate project."
        )


def _derive_project_ref_from_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    parsed = urllib.parse.urlparse(text)
    host = parsed.netloc or parsed.path
    host = host.strip().lower()
    if not host:
        return ""
    return host.split(".")[0]


def _extract_total_count(content_range: str | None) -> int | None:
    if not content_range:
        return None
    slash = content_range.rfind("/")
    if slash < 0:
        return None
    total_part = content_range[slash + 1 :].strip()
    if not total_part.isdigit():
        return None
    return int(total_part)


def _get_table_count(
    *,
    supabase_url: str,
    api_key: str,
    table: str,
    timeout_seconds: float,
) -> int:
    url = _rest_url(supabase_url, table, {"select": "id", "limit": "1"})
    result = _request(
        method="GET",
        url=url,
        api_key=api_key,
        schema=SCHEMA_NAME,
        timeout_seconds=timeout_seconds,
        extra_headers={"Prefer": "count=exact"},
    )
    if result.status not in {200, 206}:
        raise RuntimeError(f"count query failed for {table}: status={result.status} body={result.body_text}")

    count = _extract_total_count(result.headers.get("content-range"))
    if count is None:
        raise RuntimeError(f"count query missing Content-Range for {table}")
    return count


def _check_readable(
    *,
    supabase_url: str,
    api_key: str,
    object_name: str,
    timeout_seconds: float,
    label: str,
    state: AcceptanceState,
) -> None:
    url = _rest_url(supabase_url, object_name, {"select": "*", "limit": "1"})
    result = _request(
        method="GET",
        url=url,
        api_key=api_key,
        schema=SCHEMA_NAME,
        timeout_seconds=timeout_seconds,
    )
    if result.status == 200:
        state.pass_check(f"{label} '{object_name}' is readable")
        return
    state.fail_check(
        f"{label} '{object_name}' unreadable (status={result.status}) body={result.body_text}"
    )


def _run_ingest_once(
    *,
    ingest_script: Path,
    outputs_dir: Path,
    supabase_url: str,
    service_role_key: str,
    project_ref: str,
    forbidden_hint: str,
    timeout_seconds: float,
    batch_size: int,
    max_execution_events: int,
    max_frontier_reports: int,
    max_climate_events: int,
    max_scorecards: int,
) -> tuple[int, str]:
    cmd = [
        sys.executable,
        str(ingest_script),
        "--outputs-dir",
        str(outputs_dir),
        "--supabase-url",
        supabase_url,
        "--service-role-key",
        service_role_key,
        "--project-ref",
        project_ref,
        "--forbidden-hint",
        forbidden_hint,
        "--timeout-seconds",
        str(timeout_seconds),
        "--batch-size",
        str(batch_size),
        "--max-execution-events",
        str(max_execution_events),
        "--max-frontier-reports",
        str(max_frontier_reports),
        "--max-climate-events",
        str(max_climate_events),
        "--max-scorecards",
        str(max_scorecards),
    ]

    completed = subprocess.run(cmd, capture_output=True, text=True)
    output = (completed.stdout or "") + (completed.stderr or "")
    return completed.returncode, output.strip()


def _to_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_latest_timestamp(rows: Any, field: str) -> str | None:
    if isinstance(rows, list) and rows:
        first = rows[0]
        if isinstance(first, dict):
            value = first.get(field)
            if isinstance(value, str) and value.strip():
                return value
    return None


def _extract_balance_age_seconds(overnight_row: dict[str, Any] | None) -> float | None:
    if not isinstance(overnight_row, dict):
        return None
    payload = overnight_row.get("payload_json")
    if not isinstance(payload, dict):
        return None
    value = payload.get("balance_heartbeat_age_seconds")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Acceptance gate for separate Supabase/Vercel stack")
    parser.add_argument("--outputs-dir", default="outputs")
    parser.add_argument(
        "--ingest-script",
        default=str(Path(__file__).with_name("ingest_outputs_to_supabase.py")),
    )
    parser.add_argument("--supabase-url", default=os.getenv("OPSBOT_SUPABASE_URL", ""))
    parser.add_argument("--service-role-key", default=os.getenv("OPSBOT_SUPABASE_SERVICE_ROLE_KEY", ""))
    parser.add_argument(
        "--dashboard-url",
        default=os.getenv("OPSBOT_SUPABASE_DASHBOARD_URL", "") or os.getenv("OPSBOT_SUPABASE_URL", ""),
    )
    parser.add_argument(
        "--dashboard-key",
        default=os.getenv("OPSBOT_SUPABASE_DASHBOARD_ANON_KEY", "") or os.getenv("OPSBOT_SUPABASE_ANON_KEY", ""),
    )
    parser.add_argument(
        "--project-ref",
        default=os.getenv("OPSBOT_SUPABASE_PROJECT_REF", "") or _derive_project_ref_from_url(os.getenv("OPSBOT_SUPABASE_URL", "")),
    )
    parser.add_argument("--forbidden-hint", default=os.getenv("OPSBOT_FORBIDDEN_PROJECT_HINT", "zenith"))
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--max-execution-events", type=int, default=5000)
    parser.add_argument("--max-frontier-reports", type=int, default=200)
    parser.add_argument("--max-climate-events", type=int, default=5000)
    parser.add_argument("--max-scorecards", type=int, default=200)
    parser.add_argument("--skip-idempotency", action="store_true")
    parser.add_argument("--skip-write-probe", action="store_true")
    parser.add_argument("--strict-freshness", action="store_true")
    parser.add_argument("--output-json", default="")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    state = AcceptanceState(checks_passed=[], checks_failed=[], checks_warned=[])

    outputs_dir = Path(args.outputs_dir)
    ingest_script = Path(args.ingest_script)
    dashboard_url = str(args.dashboard_url or args.supabase_url or "").strip()
    project_ref = str(args.project_ref or "").strip() or _derive_project_ref_from_url(args.supabase_url)
    if not outputs_dir.exists():
        print(f"Missing outputs directory: {outputs_dir}", file=sys.stderr)
        return 2
    if not ingest_script.exists():
        print(f"Missing ingest script: {ingest_script}", file=sys.stderr)
        return 2

    required_missing = [
        name
        for name, value in [
            ("--supabase-url / OPSBOT_SUPABASE_URL", args.supabase_url),
            ("--service-role-key / OPSBOT_SUPABASE_SERVICE_ROLE_KEY", args.service_role_key),
            (
                "--dashboard-key / OPSBOT_SUPABASE_DASHBOARD_ANON_KEY or OPSBOT_SUPABASE_ANON_KEY",
                args.dashboard_key,
            ),
            ("--dashboard-url / OPSBOT_SUPABASE_DASHBOARD_URL", dashboard_url),
            ("--project-ref / OPSBOT_SUPABASE_PROJECT_REF", project_ref),
        ]
        if not str(value).strip()
    ]
    if required_missing:
        print("Missing required acceptance inputs:", file=sys.stderr)
        for item in required_missing:
            print(f"- {item}", file=sys.stderr)
        return 2

    try:
        _assert_non_zenith(
            supabase_url=args.supabase_url,
            project_ref=project_ref,
            forbidden_hint=args.forbidden_hint,
        )
        _assert_non_zenith(
            supabase_url=dashboard_url,
            project_ref=project_ref,
            forbidden_hint=args.forbidden_hint,
        )
        state.pass_check("isolation guardrail passed (non-Zenith target)")
    except RuntimeError as exc:
        state.fail_check(str(exc))
        summary = {
            "status": "failed",
            "passed": state.checks_passed,
            "failed": state.checks_failed,
            "warned": state.checks_warned,
        }
        print(json.dumps(summary, indent=2))
        return 1

    for table in EXPECTED_TABLES:
        _check_readable(
            supabase_url=args.supabase_url,
            api_key=args.service_role_key,
            object_name=table,
            timeout_seconds=args.timeout_seconds,
            label="table",
            state=state,
        )

    for view in EXPECTED_VIEWS:
        _check_readable(
            supabase_url=args.supabase_url,
            api_key=args.service_role_key,
            object_name=view,
            timeout_seconds=args.timeout_seconds,
            label="view",
            state=state,
        )

    # Dashboard credential should be able to read expected views.
    for view in EXPECTED_VIEWS:
        _check_readable(
            supabase_url=dashboard_url,
            api_key=args.dashboard_key,
            object_name=view,
            timeout_seconds=args.timeout_seconds,
            label="dashboard credential view",
            state=state,
        )

    if not args.skip_write_probe:
        probe_key = f"acceptance_probe_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        probe_payload = [
            {
                "scorecard_key": probe_key,
                "captured_at": _to_iso_now(),
                "scorecard_type": "acceptance_probe",
                "status": "probe",
                "headline_metric_name": "probe_write",
                "headline_metric_value": 1.0,
                "headline_metric_unit": "bool_01",
                "payload_json": {"probe": True, "created_at": _to_iso_now()},
                "source_file": "acceptance_gate.py",
            }
        ]
        probe_url = _rest_url(dashboard_url, "pilot_scorecards", {"on_conflict": "scorecard_key"})
        probe_result = _request(
            method="POST",
            url=probe_url,
            api_key=args.dashboard_key,
            schema=SCHEMA_NAME,
            timeout_seconds=args.timeout_seconds,
            json_payload=probe_payload,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )

        if probe_result.status in {200, 201, 204}:
            state.fail_check(
                "dashboard credential unexpectedly wrote to pilot_scorecards (read-only violation)"
            )
            cleanup_url = _rest_url(
                args.supabase_url,
                "pilot_scorecards",
                {"scorecard_key": f"eq.{probe_key}"},
            )
            _request(
                method="DELETE",
                url=cleanup_url,
                api_key=args.service_role_key,
                schema=SCHEMA_NAME,
                timeout_seconds=args.timeout_seconds,
            )
        else:
            state.pass_check(
                "dashboard credential write probe blocked as expected "
                f"(status={probe_result.status})"
            )

    counts_before: dict[str, int] = {}
    counts_after_first: dict[str, int] = {}
    counts_after_second: dict[str, int] = {}

    if not args.skip_idempotency:
        for table in EXPECTED_TABLES:
            counts_before[table] = _get_table_count(
                supabase_url=args.supabase_url,
                api_key=args.service_role_key,
                table=table,
                timeout_seconds=args.timeout_seconds,
            )

        first_code, first_output = _run_ingest_once(
            ingest_script=ingest_script,
            outputs_dir=outputs_dir,
            supabase_url=args.supabase_url,
            service_role_key=args.service_role_key,
            project_ref=project_ref,
            forbidden_hint=args.forbidden_hint,
            timeout_seconds=args.timeout_seconds,
            batch_size=args.batch_size,
            max_execution_events=args.max_execution_events,
            max_frontier_reports=args.max_frontier_reports,
            max_climate_events=args.max_climate_events,
            max_scorecards=args.max_scorecards,
        )
        if first_code != 0:
            state.fail_check("first ingest run failed during idempotency check")
            print(first_output)
        else:
            state.pass_check("first ingest run completed")

        for table in EXPECTED_TABLES:
            counts_after_first[table] = _get_table_count(
                supabase_url=args.supabase_url,
                api_key=args.service_role_key,
                table=table,
                timeout_seconds=args.timeout_seconds,
            )

        second_code, second_output = _run_ingest_once(
            ingest_script=ingest_script,
            outputs_dir=outputs_dir,
            supabase_url=args.supabase_url,
            service_role_key=args.service_role_key,
            project_ref=project_ref,
            forbidden_hint=args.forbidden_hint,
            timeout_seconds=args.timeout_seconds,
            batch_size=args.batch_size,
            max_execution_events=args.max_execution_events,
            max_frontier_reports=args.max_frontier_reports,
            max_climate_events=args.max_climate_events,
            max_scorecards=args.max_scorecards,
        )
        if second_code != 0:
            state.fail_check("second ingest run failed during idempotency check")
            print(second_output)
        else:
            state.pass_check("second ingest run completed")

        for table in EXPECTED_TABLES:
            counts_after_second[table] = _get_table_count(
                supabase_url=args.supabase_url,
                api_key=args.service_role_key,
                table=table,
                timeout_seconds=args.timeout_seconds,
            )

        for table in EXPECTED_TABLES:
            growth_second = counts_after_second[table] - counts_after_first[table]
            if growth_second == 0:
                state.pass_check(f"idempotency stable on table '{table}' (second run delta=0)")
            else:
                state.fail_check(
                    f"idempotency violated on table '{table}' (second run delta={growth_second})"
                )

    overnight_url = _rest_url(args.supabase_url, "v_latest_overnight_run", {"select": "*", "limit": "1"})
    overnight_result = _request(
        method="GET",
        url=overnight_url,
        api_key=args.service_role_key,
        schema=SCHEMA_NAME,
        timeout_seconds=args.timeout_seconds,
    )
    overnight_row = None
    if overnight_result.status == 200 and isinstance(overnight_result.json_body, list) and overnight_result.json_body:
        first = overnight_result.json_body[0]
        if isinstance(first, dict):
            overnight_row = first

    frontier_url = _rest_url(
        args.supabase_url,
        "v_frontier_recent",
        {"select": "captured_at", "order": "captured_at.desc", "limit": "1"},
    )
    frontier_result = _request(
        method="GET",
        url=frontier_url,
        api_key=args.service_role_key,
        schema=SCHEMA_NAME,
        timeout_seconds=args.timeout_seconds,
    )

    scorecard_url = _rest_url(
        args.supabase_url,
        "v_pilot_scorecards_recent",
        {"select": "captured_at", "order": "captured_at.desc", "limit": "1"},
    )
    scorecard_result = _request(
        method="GET",
        url=scorecard_url,
        api_key=args.service_role_key,
        schema=SCHEMA_NAME,
        timeout_seconds=args.timeout_seconds,
    )

    climate_url = _rest_url(
        args.supabase_url,
        "climate_availability_events",
        {"select": "observed_at_utc", "order": "observed_at_utc.desc", "limit": "1"},
    )
    climate_result = _request(
        method="GET",
        url=climate_url,
        api_key=args.service_role_key,
        schema=SCHEMA_NAME,
        timeout_seconds=args.timeout_seconds,
    )

    freshness = {
        "latest_overnight_run_finished_at_utc": overnight_row.get("run_finished_at_utc")
        if isinstance(overnight_row, dict)
        else None,
        "latest_frontier_captured_at_utc": _extract_latest_timestamp(frontier_result.json_body, "captured_at"),
        "latest_pilot_scorecard_captured_at_utc": _extract_latest_timestamp(
            scorecard_result.json_body, "captured_at"
        ),
        "latest_climate_observed_at_utc": _extract_latest_timestamp(
            climate_result.json_body, "observed_at_utc"
        ),
        "balance_heartbeat_age_seconds": _extract_balance_age_seconds(overnight_row),
    }

    required_freshness = [
        "latest_overnight_run_finished_at_utc",
        "latest_frontier_captured_at_utc",
        "latest_pilot_scorecard_captured_at_utc",
        "latest_climate_observed_at_utc",
    ]

    missing_freshness = [field for field in required_freshness if not freshness.get(field)]
    if missing_freshness:
        message = "freshness fields missing: " + ", ".join(missing_freshness)
        if args.strict_freshness:
            state.fail_check(message)
        else:
            state.warn_check(message)
    else:
        state.pass_check("freshness fields populated")

    summary = {
        "status": "passed" if not state.checks_failed else "failed",
        "generated_at_utc": _to_iso_now(),
        "passed": state.checks_passed,
        "failed": state.checks_failed,
        "warned": state.checks_warned,
        "counts_before": counts_before,
        "counts_after_first": counts_after_first,
        "counts_after_second": counts_after_second,
        "freshness": freshness,
    }

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"[info] wrote summary: {out_path}")

    print("\nAcceptance summary:")
    print(json.dumps(summary, indent=2))

    return 1 if state.checks_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
