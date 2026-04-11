"""
Export published articles from Notion → data/articles.json.
Run by GitHub Actions after main.py, then commit the JSON.

Only pages with ステータス = "公開" are exported.
Sorted by 公開日時 descending.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


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
    """Extract URL from a Notion `url` or `files` property.

    Notion property types that can hold image URLs:
      - url   : {"type": "url", "url": "https://..."}
      - files : {"type": "files", "files": [{"type": "external", "external": {"url": "..."}},
                                             {"type": "file",     "file":     {"url": "...", "expiry_time": "..."}}]}

    Notion-hosted files (type="file") are signed S3 URLs that expire in ~1 hour.
    External URLs (type="external") are permanent.
    We prefer external URLs; fall back to signed URLs if that's all there is.
    """
    prop_type = prop.get("type")

    if prop_type == "url":
        return prop.get("url") or ""

    if prop_type == "files":
        files = prop.get("files") or []
        external_url = ""
        signed_url = ""
        for f in files:
            if f.get("type") == "external":
                u = f.get("external", {}).get("url", "")
                if u and not external_url:
                    external_url = u
            elif f.get("type") == "file":
                u = f.get("file", {}).get("url", "")
                if u and not signed_url:
                    signed_url = u
        # Prefer permanent external URL over expiring signed URL
        return external_url or signed_url

    # Fallback: legacy usage where prop dict is passed directly with a "url" key
    return prop.get("url") or ""


def _is_expiring_url(url: str) -> bool:
    """Return True if the URL is a Notion-signed S3 URL that will expire."""
    return bool(url) and (
        "prod-files-secure.s3" in url
        or "secure.notion-static.com" in url
        or ("amazonaws.com" in url and "X-Amz-Expires" in url)
    )


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
            image_url = _url(p.get("画像URL", {}))

            # Warn about expiring Notion-signed URLs — these will break within ~1 hour
            if _is_expiring_url(image_url):
                title_preview = _text(p.get("タイトル", {}))[:50]
                logger.warning(
                    f"Expiring Notion-signed image URL detected for: {title_preview!r}. "
                    "Consider storing external URLs in '画像URL' instead of uploading files to Notion."
                )

            articles.append(
                {
                    "id": page["id"].replace("-", ""),
                    "title":        _text(p.get("タイトル", {})),
                    "summary":      _text(p.get("本文", {})),
                    "category":     _select(p.get("カテゴリ", {})),
                    "image_url":    image_url,
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

    logger.info(f"  Saved → sitemap.xml ({len(articles)} articles, {len(categories)} categories)")


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

    logger.info("Exporting published articles from Notion…")
    articles = fetch_published(api_key, db_id)
    total = len(articles)
    with_img = sum(1 for a in articles if a.get("image_url"))
    expiring = sum(1 for a in articles if _is_expiring_url(a.get("image_url", "")))
    logger.info(f"  {total} articles found")
    logger.info(f"  image_url present: {with_img}/{total} ({100 * with_img // max(total, 1)}%)")
    if expiring:
        logger.warning(f"  Expiring Notion-signed URLs: {expiring} — these will break within ~1 hour!")

    Path("data").mkdir(exist_ok=True)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total": len(articles),
        "articles": articles,
    }
    with open("data/articles.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    logger.info("  Saved → data/articles.json")

    if site_url:
        generate_sitemap(articles, site_url.rstrip("/"))
    else:
        logger.info("  SITE_URL not configured — skipping sitemap.xml")


if __name__ == "__main__":
    main()
