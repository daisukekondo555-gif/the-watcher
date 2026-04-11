"""
Export published articles from Notion → data/articles.json.
Run by GitHub Actions after main.py, then commit the JSON.

Only pages with ステータス = "公開" are exported.
Sorted by 公開日時 descending.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def _text(prop: dict) -> str:
    items = prop.get("rich_text") or prop.get("title") or []
    return items[0].get("text", {}).get("content", "") if items else ""


def _select(prop: dict) -> str:
    sel = prop.get("select")
    return sel.get("name", "") if sel else ""


def _url(prop: dict) -> str:
    return prop.get("url") or ""


def _date(prop: dict) -> str:
    d = prop.get("date")
    return d.get("start", "") if d else ""


def fetch_published(api_key: str, database_id: str) -> list[dict]:
    articles: list[dict] = []
    cursor = None

    while True:
        payload: dict = {
            "filter": {
                "property": "ステータス",
                "select": {"equals": "公開"},
            },
            "sorts": [{"property": "公開日時", "direction": "descending"}],
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor

        resp = requests.post(
            f"{NOTION_BASE}/databases/{database_id}/query",
            headers=_headers(api_key),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            p = page.get("properties", {})
            articles.append(
                {
                    "id": page["id"].replace("-", ""),
                    "title":        _text(p.get("タイトル", {})),
                    "summary":      _text(p.get("本文", {})),
                    "category":     _select(p.get("カテゴリ", {})),
                    "image_url":    _url(p.get("画像URL", {})),
                    "source_urls":  _text(p.get("元記事URL", {})),
                    "source_names": _text(p.get("ソースサイト名", {})),
                    "hashtags":     _text(p.get("ハッシュタグ", {})),
                    "published_at": _date(p.get("公開日時", {})),
                }
            )

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return articles


def generate_sitemap(articles: list[dict], site_url: str) -> None:
    """Generate sitemap.xml for Google Search Console."""
    from urllib.parse import quote

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
        # Homepage
        f'  <url><loc>{site_url}/</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>1.0</priority></url>',
    ]

    # Category pages
    categories = sorted(set(a["category"] for a in articles if a.get("category")))
    for cat in categories:
        cat_url = f'{site_url}/category.html?cat={quote(cat)}'
        lines.append(
            f'  <url><loc>{cat_url}</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>0.9</priority></url>'
        )

    # Article pages
    for article in articles:
        if not article.get("id"):
            continue
        art_url  = f'{site_url}/article.html?id={article["id"]}'
        lastmod  = (article.get("published_at") or today)[:10]
        lines.append(
            f'  <url><loc>{art_url}</loc><lastmod>{lastmod}</lastmod><changefreq>monthly</changefreq><priority>0.8</priority></url>'
        )

    lines.append('</urlset>')

    with open("sitemap.xml", "w", encoding="utf-8") as f:
        f.write('\n'.join(lines) + '\n')

    print(f"  Saved → sitemap.xml ({len(articles)} articles, {len(categories)} categories)")


def main() -> None:
    api_key  = os.environ.get("NOTION_API_KEY", "")
    db_id    = os.environ.get("NOTION_DATABASE_ID", "")
    site_url = os.environ.get("SITE_URL", "")

    if not api_key or not db_id:
        print("ERROR: NOTION_API_KEY / NOTION_DATABASE_ID not set", file=sys.stderr)
        sys.exit(1)

    # SITE_URL が env になければ config.json から読む
    if not site_url:
        try:
            with open("config.json", encoding="utf-8") as f:
                site_url = json.load(f).get("site_url", "")
        except Exception:
            pass

    print("Exporting published articles from Notion…")
    articles = fetch_published(api_key, db_id)
    print(f"  {len(articles)} articles found")

    Path("data").mkdir(exist_ok=True)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total": len(articles),
        "articles": articles,
    }
    with open("data/articles.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("  Saved → data/articles.json")

    if site_url:
        generate_sitemap(articles, site_url.rstrip("/"))
    else:
        print("  SITE_URL not configured — skipping sitemap.xml")


if __name__ == "__main__":
    main()
