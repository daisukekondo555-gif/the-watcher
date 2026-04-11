"""
Notion database writer.
Uses the Notion REST API directly via requests to avoid notion-client
version compatibility issues.

Notion property types assumed in the target database:
  タイトル        : title
  本文            : rich_text
  カテゴリ        : select
  画像URL         : url
  元記事URL       : rich_text  (comma-separated, parallel to ソースサイト名)
  ソースサイト名  : rich_text  (comma-separated, parallel to 元記事URL)
  ステータス      : select
  公開日時        : date

`元記事URL` and `ソースサイト名` are parallel comma-separated lists:
  元記事URL      = "https://hiphopdx.com/...,https://xxlmag.com/..."
  ソースサイト名 = "HipHopDX,XXL"

Frontends can zip the two fields to render:
  via <a href="url[0]">HipHopDX</a> | via <a href="url[1]">XXL</a>
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"
MAX_RICH_TEXT = 2000
MAX_TITLE = 2000


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def _to_utc_iso(raw: Optional[str]) -> str:
    """Parse any date string and return UTC ISO-8601."""
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    try:
        dt = dateparser.parse(raw)
        if dt is None:
            raise ValueError("unparseable")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _article_exists(api_key: str, database_id: str, primary_url: str) -> bool:
    """Return True if a page with this URL already exists in the database."""
    url = f"{NOTION_BASE}/databases/{database_id}/query"
    payload = {
        "filter": {
            "property": "元記事URL",
            "rich_text": {"contains": primary_url[:100]},
        },
        "page_size": 1,
    }
    try:
        resp = requests.post(url, headers=_headers(api_key), json=payload, timeout=15)
        resp.raise_for_status()
        return len(resp.json().get("results", [])) > 0
    except Exception as e:
        logger.error(f"Notion query error: {e}")
        return False


def _build_properties(article: dict) -> dict:
    title_ja = (article.get("title_ja") or article.get("title") or "")[:MAX_TITLE]
    summary_ja = (article.get("summary_ja") or "")[:MAX_RICH_TEXT]
    category = article.get("category") or "ニュース"
    url_field = (article.get("url") or "")[:MAX_RICH_TEXT]
    source_names = (article.get("source_names") or article.get("source_name") or "")[:MAX_RICH_TEXT]
    image_url = article.get("image_url") or None
    published_iso = _to_utc_iso(article.get("published"))

    props: dict = {
        "タイトル": {
            "title": [{"text": {"content": title_ja}}]
        },
        "本文": {
            "rich_text": [{"text": {"content": summary_ja}}]
        },
        "カテゴリ": {
            "select": {"name": category}
        },
        "元記事URL": {
            "rich_text": [{"text": {"content": url_field}}]
        },
        "ソースサイト名": {
            "rich_text": [{"text": {"content": source_names}}]
        },
        "ステータス": {
            "select": {"name": "下書き"}
        },
        "公開日時": {
            "date": {"start": published_iso}
        },
    }

    if image_url:
        props["画像URL"] = {"url": image_url}

    return props


def save_article(api_key: str, database_id: str, article: dict) -> bool:
    """
    Save one article to Notion.
    Returns True if saved, False if skipped (already exists or error).
    """
    primary_url = (article.get("url") or "").split(",")[0].strip()

    if _article_exists(api_key, database_id, primary_url):
        logger.info(f"Skip (already in Notion): {article.get('title_ja', article.get('title', ''))[:60]}")
        return False

    payload = {
        "parent": {"database_id": database_id},
        "properties": _build_properties(article),
    }
    try:
        resp = requests.post(
            f"{NOTION_BASE}/pages",
            headers=_headers(api_key),
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        logger.info(f"Saved: {article.get('title_ja', article.get('title', ''))[:60]}")
        return True

    except requests.HTTPError as e:
        logger.error(f"Notion save error: {e.response.status_code} {e.response.text[:200]} | title={article.get('title', '')[:60]}")
        return False
    except Exception as e:
        logger.error(f"Unexpected save error: {e} | title={article.get('title', '')[:60]}")
        return False


def save_all(articles: list[dict], notion_api_key: str, database_id: str) -> int:
    """Save all articles to Notion. Returns the count of newly saved articles."""
    saved = 0
    for article in articles:
        if save_article(notion_api_key, database_id, article):
            saved += 1
    logger.info(f"Notion: saved {saved}/{len(articles)} articles")
    return saved
