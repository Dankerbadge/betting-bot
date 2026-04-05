from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _default_lane_policy_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "policy" / "lanes.yaml"


def _parse_bool(raw: str) -> bool:
    low = raw.strip().lower()
    if low in {"true", "yes", "1", "on"}:
        return True
    if low in {"false", "no", "0", "off"}:
        return False
    raise ValueError(f"Invalid boolean value in lane policy: {raw!r}")


def _parse_lanes_yaml(path: Path) -> dict[str, dict[str, bool]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    in_lanes = False
    current_lane: str | None = None
    payload: dict[str, dict[str, bool]] = {}

    for raw in lines:
        line = raw.split("#", 1)[0].rstrip("\n")
        if not line.strip():
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        stripped = line.strip()

        if indent == 0:
            in_lanes = stripped == "lanes:"
            current_lane = None
            continue
        if not in_lanes:
            continue
        if indent == 2 and stripped.endswith(":"):
            current_lane = stripped[:-1].strip()
            payload[current_lane] = {}
            continue
        if indent == 4 and current_lane and ":" in stripped:
            key, value = stripped.split(":", 1)
            payload[current_lane][key.strip()] = _parse_bool(value.strip())

    if not payload:
        raise ValueError(f"No lanes parsed from {path}")
    return payload


@dataclass(frozen=True)
class LanePolicy:
    name: str
    permissions: dict[str, bool]


@dataclass(frozen=True)
class LanePolicySet:
    lanes: dict[str, LanePolicy]

    def is_known_lane(self, lane: str) -> bool:
        return lane in self.lanes

    def is_allowed(self, lane: str, action: str) -> bool:
        policy = self.lanes.get(lane)
        if policy is None:
            return False
        return bool(policy.permissions.get(action, False))

    def allowed_actions(self, lane: str) -> list[str]:
        policy = self.lanes.get(lane)
        if policy is None:
            return []
        return sorted(action for action, allowed in policy.permissions.items() if allowed)


def load_lane_policy_set(path: str | Path | None = None) -> LanePolicySet:
    policy_path = Path(path) if path is not None else _default_lane_policy_path()
    raw = _parse_lanes_yaml(policy_path)
    lanes = {name: LanePolicy(name=name, permissions=dict(permissions)) for name, permissions in raw.items()}
    return LanePolicySet(lanes=lanes)


def lane_policy_snapshot(path: str | Path | None = None) -> dict[str, Any]:
    policy_set = load_lane_policy_set(path)
    return {
        "lanes": {lane_name: dict(policy.permissions) for lane_name, policy in policy_set.lanes.items()}
    }
