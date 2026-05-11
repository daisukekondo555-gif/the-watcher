"""
YouTube RSS → Notion Videos DB 書き込み。

各チャンネルの公開 RSS フィードから動画を取得し、
キーワードフィルタを通過した動画を Notion Videos DB に保存する。
既存の動画（動画ID一致）はスキップし、手動編集を上書きしない。
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
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
CHANNELS_PATH = ROOT / "config" / "channels.json"
NAME_MAPPING_PATH = ROOT / "data" / "name_mapping.json"

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"
YT_RSS_BASE = "https://www.youtube.com/feeds/videos.xml?channel_id="

MAX_AGE_DAYS = 14
FETCH_TIMEOUT = 20


def _notion_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def _load_channels() -> dict:
    return json.loads(CHANNELS_PATH.read_text(encoding="utf-8"))


def _load_name_mapping() -> dict:
    try:
        return json.loads(NAME_MAPPING_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _extract_artists(title: str, name_mapping: dict) -> list[str]:
    """動画タイトルからアーティスト名を抽出する。"""
    artists = []
    keep = name_mapping.get("__keep_english", [])
    all_names = list(name_mapping.keys()) + keep
    all_names = [n for n in all_names if not n.startswith("__")]

    for name in sorted(all_names, key=len, reverse=True):
        pattern = re.compile(re.escape(name), re.IGNORECASE)
        if pattern.search(title):
            artists.append(name)
            title = pattern.sub("", title)

    return artists[:5]


def _matches_filter(title: str, allow_kw: list[str], deny_kw: list[str]) -> bool:
    """許可/除外キーワードでフィルタ。"""
    title_lower = title.lower()
    for kw in deny_kw:
        if kw.lower() in title_lower:
            return False
    for kw in allow_kw:
        if kw.lower() in title_lower:
            return True
    return False


def _fetch_rss(channel_id: str) -> list[dict]:
    """YouTube RSS から動画リストを取得する。"""
    url = YT_RSS_BASE + channel_id
    try:
        resp = requests.get(url, timeout=FETCH_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"  RSS取得失敗 {channel_id}: {e}")
        return []

    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        logger.warning(f"  RSS パース失敗 {channel_id}: {e}")
        return []

    videos = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)

    for entry in root.findall("atom:entry", ns):
        video_id_el = entry.find("yt:videoId", ns)
        title_el = entry.find("atom:title", ns)
        published_el = entry.find("atom:published", ns)
        thumbnail_el = entry.find("media:group/media:thumbnail", ns)

        if video_id_el is None or title_el is None:
            continue

        video_id = video_id_el.text.strip()
        title = title_el.text.strip() if title_el.text else ""
        published = published_el.text.strip() if published_el is not None and published_el.text else ""

        if published:
            try:
                pub_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue
            except ValueError:
                pass

        thumbnail_url = ""
        if thumbnail_el is not None:
            thumbnail_url = thumbnail_el.get("url", "")
        if not thumbnail_url:
            thumbnail_url = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

        videos.append({
            "video_id": video_id,
            "title": title,
            "published_at": published,
            "thumbnail_url": thumbnail_url,
        })

    return videos


def _setup_database(api_key: str, db_id: str) -> None:
    """Notion Videos DB にプロパティが不足していれば追加する。"""
    required_props = {
        "動画ID": {"rich_text": {}},
        "動画URL": {"url": {}},
        "アーティスト名": {"multi_select": {}},
        "カテゴリ": {
            "select": {
                "options": [
                    {"name": "MV", "color": "purple"},
                    {"name": "INTERVIEWS", "color": "blue"},
                    {"name": "PODCASTS", "color": "green"},
                    {"name": "PERFORMANCES", "color": "orange"},
                ]
            }
        },
        "チャンネル名": {"rich_text": {}},
        "サムネURL": {"url": {}},
        "公開日": {"date": {}},
        "ステータス": {
            "select": {
                "options": [
                    {"name": "公開", "color": "green"},
                    {"name": "下書き", "color": "yellow"},
                    {"name": "非表示", "color": "red"},
                ]
            }
        },
        "Editor's Pick": {"checkbox": {}},
        "ソース": {
            "select": {
                "options": [
                    {"name": "RSS自動", "color": "gray"},
                    {"name": "手動追加", "color": "blue"},
                ]
            }
        },
        "関連記事URL": {"rich_text": {}},
    }

    resp = requests.get(
        f"{NOTION_BASE}/databases/{db_id}",
        headers=_notion_headers(api_key),
        timeout=30,
    )
    resp.raise_for_status()
    existing_props = resp.json().get("properties", {})

    missing = {k: v for k, v in required_props.items() if k not in existing_props}
    if not missing:
        logger.info("  Notion DB プロパティは全て存在")
        return

    logger.info(f"  Notion DB に {len(missing)} 個のプロパティを追加")
    update_resp = requests.patch(
        f"{NOTION_BASE}/databases/{db_id}",
        headers=_notion_headers(api_key),
        json={"properties": missing},
        timeout=30,
    )
    update_resp.raise_for_status()
    logger.info("  プロパティ追加完了")


def _get_existing_video_ids(api_key: str, db_id: str) -> set[str]:
    """Notion DB から既存の動画IDを全件取得する。"""
    ids: set[str] = set()
    cursor = None
    while True:
        payload: dict = {"page_size": 100}
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
        for page in data.get("results", []):
            props = page.get("properties", {})
            vid_prop = props.get("動画ID", {})
            rt = vid_prop.get("rich_text", [])
            if rt:
                vid = rt[0].get("text", {}).get("content", "")
                if vid:
                    ids.add(vid)
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return ids


def _create_video_page(
    api_key: str,
    db_id: str,
    video: dict,
    channel_name: str,
    category: str,
    artists: list[str],
) -> None:
    """Notion に動画ページを新規作成する。"""
    properties: dict = {
        "名前": {
            "title": [{"text": {"content": video["title"][:2000]}}]
        },
        "動画ID": {
            "rich_text": [{"text": {"content": video["video_id"]}}]
        },
        "動画URL": {
            "url": f"https://www.youtube.com/watch?v={video['video_id']}"
        },
        "チャンネル名": {
            "rich_text": [{"text": {"content": channel_name}}]
        },
        "カテゴリ": {
            "select": {"name": category}
        },
        "サムネURL": {
            "url": video["thumbnail_url"]
        },
        "ステータス": {
            "select": {"name": "公開"}
        },
        "Editor's Pick": {
            "checkbox": False
        },
        "ソース": {
            "select": {"name": "RSS自動"}
        },
    }

    if video.get("published_at"):
        pub = video["published_at"]
        if pub.endswith("Z"):
            pub = pub[:-1] + "+00:00"
        properties["公開日"] = {"date": {"start": pub}}

    if artists:
        properties["アーティスト名"] = {
            "multi_select": [{"name": a} for a in artists[:10]]
        }

    resp = requests.post(
        f"{NOTION_BASE}/pages",
        headers=_notion_headers(api_key),
        json={"parent": {"database_id": db_id}, "properties": properties},
        timeout=30,
    )
    resp.raise_for_status()


def main() -> None:
    api_key = os.environ.get("NOTION_API_KEY", "")
    db_id = os.environ.get("NOTION_VIDEO_DB_ID", "")

    if not api_key or not db_id:
        logger.error("NOTION_API_KEY / NOTION_VIDEO_DB_ID not set")
        sys.exit(1)

    config = _load_channels()
    channels = config.get("channels", [])
    allow_kw = config.get("filter", {}).get("allow_keywords", [])
    deny_kw = config.get("filter", {}).get("deny_keywords", [])
    name_mapping = _load_name_mapping()

    logger.info(f"=== WATCH: {len(channels)} チャンネルから動画を取得 ===")

    _setup_database(api_key, db_id)

    existing_ids = _get_existing_video_ids(api_key, db_id)
    logger.info(f"  Notion 既存動画: {len(existing_ids)} 件")

    total_new = 0
    total_skipped = 0
    total_filtered = 0

    for ch in channels:
        ch_name = ch["name"]
        ch_id = ch["channel_id"]
        ch_cat = ch["category"]

        logger.info(f"  [{ch_name}] RSS取得中...")
        videos = _fetch_rss(ch_id)
        logger.info(f"    取得: {len(videos)} 件")

        for v in videos:
            if v["video_id"] in existing_ids:
                total_skipped += 1
                continue

            if not ch.get("skip_filter") and not _matches_filter(v["title"], allow_kw, deny_kw):
                total_filtered += 1
                continue

            artists = _extract_artists(v["title"], name_mapping)

            try:
                _create_video_page(api_key, db_id, v, ch_name, ch_cat, artists)
                existing_ids.add(v["video_id"])
                total_new += 1
                logger.info(f"    + {v['title'][:60]}")
            except Exception as e:
                logger.warning(f"    Notion書き込み失敗: {v['title'][:40]}: {e}")

    logger.info(f"=== 完了: 新規{total_new}件, スキップ{total_skipped}件, フィルタ除外{total_filtered}件 ===")


if __name__ == "__main__":
    main()
