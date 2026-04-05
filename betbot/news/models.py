from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NewsArticle:
    article_id: str
    domain: str
    title: str
    url: str
    published_at: str
    matched_entities: list[str]
    freshness_minutes: float
    topic: str
    confidence: float
    citation_ready: bool
