from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class EffectiveConfig:
    repo_root: str
    values: dict[str, Any]
    loaded_files: list[str]
    config_fingerprint: str
    policy_fingerprint: str
    doctrine_path: str | None


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(dict(out[key]), value)
        else:
            out[key] = value
    return out


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    return dict(json.loads(path.read_text(encoding="utf-8")))


def _fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def resolve_config_layers(repo_root: str | None = None) -> list[Path]:
    repo = Path(repo_root or Path.cwd()).resolve()
    home = Path.home()
    return [
        home / ".betbot.json",
        home / ".config" / "betbot" / "settings.json",
        repo / ".betbot.json",
        repo / ".betbot" / "settings.json",
        repo / ".betbot" / "settings.local.json",
    ]


def load_effective_config(repo_root: str | None = None) -> EffectiveConfig:
    repo = Path(repo_root or Path.cwd()).resolve()
    payload: dict[str, Any] = {}
    loaded_files: list[str] = []

    for candidate in resolve_config_layers(str(repo)):
        loaded = _load_json_if_exists(candidate)
        if loaded is None:
            continue
        payload = _deep_merge(payload, loaded)
        loaded_files.append(str(candidate))

    doctrine = repo / "BETBOT.md"
    doctrine_path = str(doctrine) if doctrine.exists() else None

    policy_payload = dict(payload.get("policy") or {})
    return EffectiveConfig(
        repo_root=str(repo),
        values=payload,
        loaded_files=loaded_files,
        config_fingerprint=_fingerprint(payload),
        policy_fingerprint=_fingerprint(policy_payload),
        doctrine_path=doctrine_path,
    )
