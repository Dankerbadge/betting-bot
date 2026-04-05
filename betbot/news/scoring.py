from __future__ import annotations

from betbot.news.models import NewsArticle


def score_article(article: NewsArticle) -> float:
    freshness_score = max(0.0, 1.0 - (float(article.freshness_minutes) / 720.0))
    confidence = max(0.0, min(1.0, float(article.confidence)))
    citation_bonus = 0.1 if article.citation_ready else 0.0
    return round((0.6 * confidence) + (0.4 * freshness_score) + citation_bonus, 6)


def summarize_news_quality(articles: list[NewsArticle]) -> dict[str, object]:
    if not articles:
        return {
            "status": "degraded",
            "count": 0,
            "mean_score": 0.0,
        }
    scores = [score_article(article) for article in articles]
    return {
        "status": "ok",
        "count": len(scores),
        "mean_score": round(sum(scores) / len(scores), 6),
        "max_score": round(max(scores), 6),
        "min_score": round(min(scores), 6),
    }
