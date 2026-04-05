#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from betbot.paper_live_regression import evaluate_paper_live_regression


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected object payload in {path}")
    return payload


def _run_overnight(repo_root: Path, timeout_seconds: int) -> tuple[int, dict[str, Any]]:
    env = dict(os.environ)
    env["BETBOT_MIN_SECONDS_BETWEEN_RUNS"] = "0"
    command = [str(repo_root / "scripts" / "hourly_alpha_overnight.sh"), "--force"]
    result = subprocess.run(
        command,
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    report_path = repo_root / "outputs" / "overnight_alpha_latest.json"
    if not report_path.exists():
        raise RuntimeError("Missing outputs/overnight_alpha_latest.json after forced run")
    return result.returncode, _read_json(report_path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run two forced overnight cycles and validate paper-live carryover regression invariants."
    )
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Repository root (defaults to script parent parent).",
    )
    parser.add_argument(
        "--state-file",
        default="outputs/overnight_alpha/paper_live_account_state.json",
        help="Paper-live state file relative to repo root.",
    )
    parser.add_argument(
        "--archive-state",
        action="store_true",
        help="Archive existing state file before reset.",
    )
    parser.add_argument(
        "--require-attempts",
        action="store_true",
        help="Require first run to have paper_live_order_attempts_run > 0.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=900,
        help="Timeout per forced overnight run.",
    )
    parser.add_argument(
        "--output-json",
        default="outputs/paper_live_regression_check_latest.json",
        help="Output summary JSON relative to repo root.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    state_file = (repo_root / args.state_file).resolve()
    output_json = (repo_root / args.output_json).resolve()

    state_archive: str | None = None
    if state_file.exists() and args.archive_state:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        archive_name = f"{state_file.stem}.regression_archive_{stamp}{state_file.suffix or '.json'}"
        archive_path = state_file.parent / archive_name
        shutil.copy2(state_file, archive_path)
        state_archive = str(archive_path)

    if state_file.exists():
        state_file.unlink()

    run1_rc, run1 = _run_overnight(repo_root, args.timeout_seconds)
    if run1_rc != 0:
        summary = {
            "status": "fail",
            "reason": "run1_failed",
            "run1_returncode": run1_rc,
            "state_archive": state_archive,
        }
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 1

    run2_rc, run2 = _run_overnight(repo_root, args.timeout_seconds)
    evaluation = evaluate_paper_live_regression(
        run1=run1,
        run2=run2,
        require_attempts=bool(args.require_attempts),
    )
    summary = {
        "status": evaluation.get("status"),
        "state_archive": state_archive,
        "run1_returncode": run1_rc,
        "run2_returncode": run2_rc,
        "run1_id": run1.get("run_id"),
        "run2_id": run2.get("run_id"),
        "evaluation": evaluation,
        "run1_fields": {
            "paper_live_order_attempts_run": run1.get("paper_live_order_attempts_run"),
            "paper_live_orders_filled_run": run1.get("paper_live_orders_filled_run"),
            "paper_live_orders_canceled_run": run1.get("paper_live_orders_canceled_run"),
            "paper_live_sizing_balance_dollars": run1.get("paper_live_sizing_balance_dollars"),
            "paper_live_post_trade_sizing_balance_dollars": run1.get(
                "paper_live_post_trade_sizing_balance_dollars"
            ),
        },
        "run2_fields": {
            "paper_live_order_attempts_run": run2.get("paper_live_order_attempts_run"),
            "paper_live_orders_filled_run": run2.get("paper_live_orders_filled_run"),
            "paper_live_orders_canceled_run": run2.get("paper_live_orders_canceled_run"),
            "paper_live_sizing_balance_dollars": run2.get("paper_live_sizing_balance_dollars"),
            "paper_live_post_trade_sizing_balance_dollars": run2.get(
                "paper_live_post_trade_sizing_balance_dollars"
            ),
        },
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0 if summary.get("status") == "pass" and run2_rc == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
