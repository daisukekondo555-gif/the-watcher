"""
Notion Videos DB → data/videos.json エクスポート。

ステータス「公開」の動画を取得し、関連記事マッチングを行い、
videos.json として出力する。
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

ROOT = Path(__file__).resolve().parent.parent
ARTICLES_PATH = ROOT / "data" / "articles.json"
VIDEOS_PATH = ROOT / "data" / "videos.json"
NAME_MAPPING_PATH = ROOT / "data" / "name_mapping.json"

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"


def _notion_headers(api_key: str) -> dict:
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


def _multi_select(prop: dict) -> list[str]:
    return [o.get("name", "") for o in (prop.get("multi_select") or [])]


def _url(prop: dict) -> str:
    return prop.get("url") or ""


def _date(prop: dict) -> str:
    d = prop.get("date")
    return d.get("start", "") if d else ""


def _checkbox(prop: dict) -> bool:
    return prop.get("checkbox", False)


def _fetch_published_videos(api_key: str, db_id: str) -> list[dict]:
    """Notion Videos DB からステータス「公開」の動画を取得する。"""
    results: list[dict] = []
    cursor = None
    filter_ = {"property": "ステータス", "select": {"equals": "公開"}}

    while True:
        payload: dict = {
            "filter": filter_,
            "sorts": [{"property": "公開日", "direction": "descending"}],
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor
        resp = requests.post(
            f"{NOTION_BASE}/databases/{db_id}/query",
            headers=_notion_headers(api_key),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return results


def _page_to_video(page: dict) -> dict:
    p = page.get("properties", {})
    return {
        "id": _text(p.get("動画ID", {})),
        "title": _text(p.get("名前", {})),
        "channel_name": _text(p.get("チャンネル名", {})),
        "category": _select(p.get("カテゴリ", {})),
        "published_at": _date(p.get("公開日", {})),
        "thumbnail_url": _url(p.get("サムネURL", {})),
        "source": _select(p.get("ソース", {})),
        "editor_pick": _checkbox(p.get("Editor's Pick", {})),
        "artists": _multi_select(p.get("アーティスト名", {})),
        "manual_related": _text(p.get("関連記事URL", {})),
    }


def _load_articles() -> list[dict]:
    try:
        data = json.loads(ARTICLES_PATH.read_text(encoding="utf-8"))
        return data.get("articles", [])
    except Exception:
        return []


def _load_name_mapping() -> dict:
    try:
        return json.loads(NAME_MAPPING_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _find_related_articles(
    artists: list[str],
    articles: list[dict],
    name_mapping: dict,
    max_results: int = 3,
) -> list[dict]:
    """アーティスト名で関連記事を検索する。"""
    if not artists:
        return []

    search_terms: list[str] = []
    for artist in artists:
        search_terms.append(artist)
        katakana = name_mapping.get(artist, "")
        if katakana and not katakana.startswith("__"):
            search_terms.append(katakana)

    matched = []
    seen_ids: set[str] = set()
    for article in articles:
        title = article.get("title", "")
        hashtags = article.get("hashtags", "")
        text = f"{title} {hashtags}"
        for term in search_terms:
            if term.lower() in text.lower():
                aid = article.get("id", "")
                if aid and aid not in seen_ids:
                    matched.append({
                        "id": aid,
                        "title": article.get("title", ""),
                    })
                    seen_ids.add(aid)
                break
        if len(matched) >= max_results:
            break

    return matched


def main() -> None:
    api_key = os.environ.get("NOTION_API_KEY", "")
    db_id = os.environ.get("NOTION_VIDEO_DB_ID", "")

    if not api_key or not db_id:
        logger.error("NOTION_API_KEY / NOTION_VIDEO_DB_ID not set")
        sys.exit(1)

    logger.info("=== WATCH: Notion Videos DB → videos.json エクスポート ===")

    pages = _fetch_published_videos(api_key, db_id)
    logger.info(f"  公開動画: {len(pages)} 件")

    videos = [_page_to_video(p) for p in pages]
    videos = [v for v in videos if v.get("id")]

    articles = _load_articles()
    name_mapping = _load_name_mapping()

    for v in videos:
        if v.get("manual_related"):
            pass
        else:
            v["related_articles"] = _find_related_articles(
                v.get("artists", []), articles, name_mapping
            )

        v.pop("manual_related", None)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total": len(videos),
        "videos": videos,
    }

    VIDEOS_PATH.parent.mkdir(exist_ok=True)
    VIDEOS_PATH.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"  Saved → data/videos.json ({len(videos)} videos)")


if __name__ == "__main__":
    main()
