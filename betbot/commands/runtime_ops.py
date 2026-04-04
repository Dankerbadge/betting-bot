from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from betbot.policy.lanes import lane_policy_snapshot, load_lane_policy_set
from betbot.runtime.board import render_board_text
from betbot.runtime.config_loader import load_effective_config


def run_effective_config(*, repo_root: str | None = None) -> dict[str, object]:
    effective = load_effective_config(repo_root=repo_root)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "repo_root": effective.repo_root,
        "loaded_files": effective.loaded_files,
        "config_fingerprint": effective.config_fingerprint,
        "policy_fingerprint": effective.policy_fingerprint,
        "doctrine_path": effective.doctrine_path,
        "effective_config": effective.values,
    }


def run_policy_check(*, lane: str, lane_policy_path: str | None = None) -> dict[str, object]:
    lane_policy_set = load_lane_policy_set(path=lane_policy_path)
    known_lane = lane_policy_set.is_known_lane(lane)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lane": lane,
        "known_lane": known_lane,
        "allowed_actions": lane_policy_set.allowed_actions(lane),
        "live_submit_enabled": lane_policy_set.is_allowed(lane, "live_submit"),
        "policy_snapshot": lane_policy_snapshot(path=lane_policy_path),
    }


def run_render_board(
    *,
    board_json: str | None,
    cycle_json: str | None,
    output_dir: str = "outputs",
) -> dict[str, object]:
    output_path = Path(output_dir)
    board_path = Path(board_json) if board_json else (output_path / "board_latest.json")
    cycle_path = Path(cycle_json) if cycle_json else (output_path / "cycle_latest.json")

    payload: dict[str, object] = {}
    if board_path.exists():
        payload = dict(json.loads(board_path.read_text(encoding="utf-8")))
    elif cycle_path.exists():
        cycle_payload = dict(json.loads(cycle_path.read_text(encoding="utf-8")))
        payload = {
            "run_id": cycle_payload.get("run_id"),
            "cycle_id": cycle_payload.get("cycle_id"),
            "overall_status": cycle_payload.get("overall_status"),
            "phase": cycle_payload.get("phase"),
            "lane": cycle_payload.get("permission_lane"),
            "degraded_sources": sorted(
                provider
                for provider, status in dict(cycle_payload.get("source_health") or {}).items()
                if status in {"partial", "degraded", "failed", "blocked"}
            ),
        }
    else:
        payload = {
            "run_id": None,
            "cycle_id": None,
            "overall_status": "failed",
            "phase": "missing",
            "lane": "unknown",
            "degraded_sources": [],
        }

    board_text = render_board_text(payload)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "board_json": str(board_path),
        "cycle_json": str(cycle_path),
        "board": payload,
        "board_text": board_text,
    }
