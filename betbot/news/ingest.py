from __future__ import annotations

from betbot.news.allowlist import is_domain_allowlisted
from betbot.news.models import NewsArticle


def ingest_articles(
    *,
    articles: list[NewsArticle],
    allowlist: dict[str, list[str]],
) -> tuple[list[NewsArticle], list[NewsArticle]]:
    accepted: list[NewsArticle] = []
    rejected: list[NewsArticle] = []

    for article in articles:
        if article.citation_ready and is_domain_allowlisted(article.domain, allowlist):
            accepted.append(article)
        else:
            rejected.append(article)
    return accepted, rejected
