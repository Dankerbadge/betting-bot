from __future__ import annotations

from pathlib import Path


def _default_news_policy_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "policy" / "news_sources.yaml"


def load_news_allowlist(path: str | Path | None = None) -> dict[str, list[str]]:
    policy_path = Path(path) if path is not None else _default_news_policy_path()
    categories: dict[str, list[str]] = {}
    current_category: str | None = None

    for raw in policy_path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line.endswith(":"):
            current_category = line[:-1].strip()
            categories[current_category] = []
            continue
        if line.startswith("-") and current_category:
            domain = line[1:].strip().lower()
            if domain:
                categories[current_category].append(domain)
    return categories


def is_domain_allowlisted(
    domain: str,
    allowlist: dict[str, list[str]],
    *,
    category: str | None = None,
) -> bool:
    normalized = str(domain or "").strip().lower()
    if not normalized:
        return False
    if category:
        return normalized in set(allowlist.get(category) or [])
    return any(normalized in set(domains) for domains in allowlist.values())
