"""
GA4 Data API — ページビューランキング取得スクリプト

GitHub Actions で使用:
  環境変数 GA_SERVICE_ACCOUNT_JSON にサービスアカウント JSON キーの中身を設定する。

出力: data/ranking.json
  {
    "weekly": [{"path": "/article.html?id=xxx", "pageviews": 123}, ...],
    "by_category": {
      "ニュース": [...],
      "リリース": [...],
      ...
    },
    "updated_at": "2026-04-13T00:00:00"
  }

必要な GitHub Secrets:
  GA_SERVICE_ACCOUNT_JSON  サービスアカウント JSON キーの中身
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

PROPERTY_ID = "532537859"
RANKING_PATH = "data/ranking.json"
ARTICLES_PATH = "data/articles.json"
TOP_N = 5
CATEGORIES = ["ニュース", "リリース", "ビーフ", "ライブ", "チャート", "ビジネス"]
LOOKBACK_DAYS = 7
# GA4 から取得するパス数の上限（カテゴリ別ランキングの精度に影響）
FETCH_LIMIT = 500


def _get_client():
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.oauth2 import service_account

    info = json.loads(os.environ["GA_SERVICE_ACCOUNT_JSON"])
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def _fetch_pageviews(client, days: int = LOOKBACK_DAYS, limit: int = FETCH_LIMIT) -> list[dict]:
    """article.html を含むパスのページビューを降順で取得する。"""
    from google.analytics.data_v1beta.types import (
        DateRange,
        Dimension,
        Filter,
        FilterExpression,
        Metric,
        OrderBy,
        RunReportRequest,
    )

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name="pagePathPlusQueryString")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="pagePathPlusQueryString",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.CONTAINS,
                    value="article.html",
                ),
            )
        ),
        order_bys=[
            OrderBy(
                metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"),
                desc=True,
            )
        ],
        limit=limit,
    )
    response = client.run_report(request)

    results = []
    for row in response.rows:
        path = row.dimension_values[0].value
        pv = int(row.metric_values[0].value)
        results.append({"path": path, "pageviews": pv})

    logger.info(f"GA4: {len(results)} article paths fetched (last {days} days)")
    return results


def _path_to_id(path: str) -> str | None:
    """/article.html?id=xxx → xxx を返す。"""
    if "id=" not in path:
        return None
    try:
        return path.split("id=", 1)[1].split("&")[0]
    except Exception:
        return None


def _build_ranking(pageviews: list[dict], articles: list[dict]) -> dict:
    id_to_article = {a["id"]: a for a in articles if "id" in a}

    # path × pageviews × category を紐付け
    enriched = []
    for item in pageviews:
        aid = _path_to_id(item["path"])
        if aid and aid in id_to_article:
            cat = id_to_article[aid].get("category", "")
            enriched.append({
                "path": item["path"],
                "pageviews": item["pageviews"],
                "id": aid,
                "category": cat,
            })

    # WEEKLY BEST 5（全カテゴリ合算、上位 TOP_N 件）
    weekly = [
        {"path": e["path"], "pageviews": e["pageviews"]}
        for e in enriched[:TOP_N]
    ]

    # カテゴリ別（各カテゴリ上位 TOP_N 件）
    by_category: dict[str, list] = {}
    for cat in CATEGORIES:
        cat_items = [e for e in enriched if e["category"] == cat][:TOP_N]
        by_category[cat] = [
            {"path": e["path"], "pageviews": e["pageviews"]}
            for e in cat_items
        ]

    logger.info(
        f"weekly={len(weekly)}, "
        + ", ".join(f"{c}={len(by_category[c])}" for c in CATEGORIES)
    )

    return {
        "weekly": weekly,
        "by_category": by_category,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    }


def main() -> None:
    if not os.environ.get("GA_SERVICE_ACCOUNT_JSON"):
        logger.info("GA_SERVICE_ACCOUNT_JSON not set — skipping GA4 ranking update")
        return

    if not Path(ARTICLES_PATH).exists():
        logger.warning(f"{ARTICLES_PATH} not found — skipping GA4 ranking update")
        return

    with open(ARTICLES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    articles = data.get("articles", [])

    client = _get_client()
    pageviews = _fetch_pageviews(client)
    ranking = _build_ranking(pageviews, articles)

    Path(RANKING_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(RANKING_PATH, "w", encoding="utf-8") as f:
        json.dump(ranking, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {RANKING_PATH}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    main()
