"""
Export published articles from Notion → data/articles.json (差分ビルド版).

ハイブリッド方式:
  1. Notion の last_edited_time フィルタで「前回同期以降に編集されたページ」だけ取得
     → API コール・通信量が O(変更数) に収まる
  2. 取得した各ページで content_hash を計算、既存値と比較
     → タイムスタンプだけ更新されて内容が変わっていないケースを除外、
        本当の「変更あり」だけを changed_ids として記録
  3. 定期的（24時間ごと）に Notion から公開ページ全IDを取得して
     articles.json にあるが Notion から消えたレコードを reconcile で削除

状態ファイル: data/sync_state.json
  {
    "last_sync_at":           "2026-04-14T03:00:00+00:00",
    "last_full_reconcile_at": "2026-04-14T00:00:00+00:00"
  }

初回 / 移行時（state なし or hash 欠落）は自動で全件 fetch + baseline 構築。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"

ARTICLES_PATH = Path("data/articles.json")
SYNC_STATE_PATH = Path("data/sync_state.json")

# Notion サーバと GH Actions ランナーの時計ずれ対策マージン
SYNC_MARGIN = timedelta(minutes=5)
# Notion から削除/アーカイブ/ステータス変更された記事を articles.json から
# 即時に消すため、毎回フルID リコンサイルを実行する (timedelta(0))。
# コスト: 公開ID 数百件のページネーション query 数回 = 数秒程度。
RECONCILE_INTERVAL = timedelta(0)

# content_hash に含めるフィールド
# → フロントエンド表示に影響するフィールドのみ。id は対象外（変わらない）、
#   last_edited_time も対象外（ハッシュに含めると毎回変わる）
HASH_FIELDS = (
    "title",
    "summary",
    "category",
    "image_url",
    "source_urls",
    "source_names",
    "hashtags",
    "published_at",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ── Notion レスポンスから値を取り出すヘルパ ──────────────────────────────
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
    """画像URL プロパティ (url型 or files型) から URL を取り出す。
    files 型では external (恒久) を signed file (~1時間失効) より優先。"""
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
        return external_url or signed_url
    return prop.get("url") or ""


def _is_expiring_url(url: str) -> bool:
    return bool(url) and (
        "prod-files-secure.s3" in url
        or "secure.notion-static.com" in url
        or ("amazonaws.com" in url and "X-Amz-Expires" in url)
    )


def _date(prop: dict) -> str:
    d = prop.get("date")
    return d.get("start", "") if d else ""


# ── コンテンツハッシュ ───────────────────────────────────────────────────
def content_hash(article: dict) -> str:
    """表示フィールドのハッシュを計算。sort_keys + ensure_ascii=False で正規化。"""
    blob = json.dumps(
        {k: article.get(k, "") for k in HASH_FIELDS},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


# ── 状態ファイル ─────────────────────────────────────────────────────────
def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _load_state() -> dict:
    if SYNC_STATE_PATH.exists():
        try:
            return json.loads(SYNC_STATE_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"sync_state.json が壊れているため無視: {e}")
    return {}


def _save_state(state: dict) -> None:
    SYNC_STATE_PATH.parent.mkdir(exist_ok=True)
    SYNC_STATE_PATH.write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_existing_articles() -> list[dict]:
    if not ARTICLES_PATH.exists():
        return []
    try:
        data = json.loads(ARTICLES_PATH.read_text(encoding="utf-8"))
        return data.get("articles", [])
    except Exception as e:
        logger.warning(f"articles.json が読めないため空として扱う: {e}")
        return []


# ── Notion fetch ─────────────────────────────────────────────────────────
def _page_to_article(page: dict) -> dict:
    p = page.get("properties", {})
    image_url = _url(p.get("画像URL", {}))
    if _is_expiring_url(image_url):
        title_preview = _text(p.get("タイトル", {}))[:50]
        logger.warning(
            f"Notion-signed な失効URLを検出: {title_preview!r}. "
            "画像URLは外部URLで保存することを推奨。"
        )
    # 翻訳警告 (入力切れ / 出力切れ / 両方 / 空)
    warning = _select(p.get("翻訳警告", {}))

    article = {
        "id":           page["id"].replace("-", ""),
        "title":        _text(p.get("タイトル", {})),
        "summary":      _text(p.get("本文", {})),
        "category":     _select(p.get("カテゴリ", {})),
        "image_url":    image_url,
        "source_urls":  _text(p.get("元記事URL", {})),
        "source_names": _text(p.get("ソースサイト名", {})),
        "hashtags":     _text(p.get("ハッシュタグ", {})),
        "published_at": _date(p.get("公開日時", {})),
        "imported_at":  page.get("created_time", ""),
    }
    if warning:
        article["translation_warning"] = warning
    return article


def _query_database(
    api_key: str,
    database_id: str,
    filter_: dict,
    include_sort: bool = True,
) -> list[dict]:
    """Notion database query をページネーション込みで全件取得する。"""
    results: list[dict] = []
    cursor = None
    while True:
        payload: dict = {"filter": filter_, "page_size": 100}
        if include_sort:
            payload["sorts"] = [{"property": "公開日時", "direction": "descending"}]
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
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return results


def fetch_published(
    api_key: str,
    database_id: str,
    since: datetime | None = None,
) -> list[dict]:
    """公開記事を取得。since を指定するとその時刻以降に編集されたものだけ。"""
    status_filter = {"property": "ステータス", "select": {"equals": "公開"}}
    if since is None:
        filter_ = status_filter
    else:
        filter_ = {
            "and": [
                status_filter,
                {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"on_or_after": since.isoformat()},
                },
            ]
        }
    pages = _query_database(api_key, database_id, filter_)
    return [_page_to_article(p) for p in pages]


def fetch_published_ids(api_key: str, database_id: str) -> set[str]:
    """削除リコンサイル用に、公開ページの ID 集合だけ取得する軽量クエリ。"""
    filter_ = {"property": "ステータス", "select": {"equals": "公開"}}
    pages = _query_database(api_key, database_id, filter_, include_sort=False)
    return {p["id"].replace("-", "") for p in pages}


# ── sitemap ──────────────────────────────────────────────────────────────
def generate_sitemap(articles: list[dict], site_url: str) -> None:
    from urllib.parse import quote

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
        f'  <url><loc>{site_url}/</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>1.0</priority></url>',
        f'  <url><loc>{site_url}/about.html</loc><lastmod>2026-04-22</lastmod><changefreq>monthly</changefreq><priority>0.5</priority></url>',
    ]
    categories = sorted({a["category"] for a in articles if a.get("category")})
    for cat in categories:
        cat_url = f"{site_url}/category.html?cat={quote(cat)}"
        lines.append(
            f'  <url><loc>{cat_url}</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>0.9</priority></url>'
        )
    for article in articles:
        if not article.get("id"):
            continue
        art_url = f'{site_url}/articles/{article["id"]}.html'
        lastmod = (article.get("published_at") or today)[:10]
        lines.append(
            f'  <url><loc>{art_url}</loc><lastmod>{lastmod}</lastmod><changefreq>monthly</changefreq><priority>0.8</priority></url>'
        )
    lines.append("</urlset>")
    Path("sitemap.xml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info(
        f"  Saved → sitemap.xml ({len(articles)} articles, {len(categories)} categories)"
    )


# ── メイン処理 ───────────────────────────────────────────────────────────
def main() -> None:
    api_key = os.environ.get("NOTION_API_KEY", "")
    db_id = os.environ.get("NOTION_DATABASE_ID", "")
    site_url = os.environ.get("SITE_URL", "")

    if not api_key or not db_id:
        print("ERROR: NOTION_API_KEY / NOTION_DATABASE_ID not set", file=sys.stderr)
        sys.exit(1)

    if not site_url:
        try:
            with open("config.json", encoding="utf-8") as f:
                site_url = json.load(f).get("site_url", "")
        except Exception:
            pass

    # 処理開始時刻（fetch 前に確定させ、取りこぼし防止）
    sync_start = datetime.now(timezone.utc)

    state = _load_state()
    existing = _load_existing_articles()
    by_id: dict[str, dict] = {a["id"]: a for a in existing if a.get("id")}

    last_sync_raw = state.get("last_sync_at")
    last_reconcile_raw = state.get("last_full_reconcile_at")
    last_sync = _parse_iso(last_sync_raw) if last_sync_raw else None
    last_reconcile = _parse_iso(last_reconcile_raw) if last_reconcile_raw else None

    # 初回 / 移行判定:
    #   state なし / articles.json 空 / いずれかの記事に content_hash が無い
    hashes_present = bool(existing) and all("content_hash" in a for a in existing)
    first_run = (last_sync is None) or (not existing) or (not hashes_present)

    changed_ids: list[str] = []

    if first_run:
        logger.info("=== 初回 / 移行モード: 全件 fetch + content_hash baseline を構築 ===")
        fetched = fetch_published(api_key, db_id)
        for a in fetched:
            a["content_hash"] = content_hash(a)
        by_id = {a["id"]: a for a in fetched}
        # 移行時は changed_ids を空に保つ（ダウンストリームを誤発火させない）
        do_reconcile = True
    else:
        since = last_sync - SYNC_MARGIN
        logger.info(
            f"=== 差分モード: last_edited_time >= {since.isoformat()} を fetch ==="
        )
        fetched = fetch_published(api_key, db_id, since=since)
        logger.info(f"  候補ページ: {len(fetched)} 件")

        for a in fetched:
            a["content_hash"] = content_hash(a)
            old = by_id.get(a["id"])
            if old is None:
                changed_ids.append(a["id"])  # 新規
                by_id[a["id"]] = a
            elif old.get("content_hash") != a["content_hash"]:
                changed_ids.append(a["id"])  # 内容変更
                by_id[a["id"]] = a
            # else: last_edited_time だけ更新されて中身は変わっていない → 無視

        do_reconcile = (
            last_reconcile is None
            or (sync_start - last_reconcile) >= RECONCILE_INTERVAL
        )

    # 削除リコンサイル
    if do_reconcile:
        logger.info("=== 削除リコンサイル: Notion 側の公開ID一覧と照合 ===")
        remote_ids = fetch_published_ids(api_key, db_id)
        removed = [aid for aid in by_id.keys() if aid not in remote_ids]
        for aid in removed:
            by_id.pop(aid, None)
        if removed:
            logger.info(f"  Notion から消えていた {len(removed)} 件を削除")
        else:
            logger.info("  削除対象なし")
        state["last_full_reconcile_at"] = sync_start.isoformat()

    # 取り込み順 (Notion ページ作成日時) 降順でソート。
    # imported_at が無い旧記事は published_at にフォールバック。
    articles = sorted(
        by_id.values(),
        key=lambda a: a.get("imported_at") or a.get("published_at") or "",
        reverse=True,
    )

    # articles.json 書き出し
    Path("data").mkdir(exist_ok=True)
    output = {
        "generated_at": sync_start.isoformat(),
        "total": len(articles),
        "articles": articles,
    }
    ARTICLES_PATH.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # サマリログ
    with_img = sum(1 for a in articles if a.get("image_url"))
    expiring = sum(1 for a in articles if _is_expiring_url(a.get("image_url", "")))
    logger.info(f"  Saved → data/articles.json ({len(articles)} articles)")
    logger.info(
        f"  image_url present: {with_img}/{len(articles)} "
        f"({100 * with_img // max(len(articles), 1)}%)"
    )
    if expiring:
        logger.warning(
            f"  Expiring Notion-signed URLs: {expiring} — これらは約1時間で失効します"
        )
    if changed_ids:
        logger.info(f"  変更検出: {len(changed_ids)} 件")
        for cid in changed_ids[:20]:
            logger.info(f"    - {cid}")
        if len(changed_ids) > 20:
            logger.info(f"    ... ほか {len(changed_ids) - 20} 件")
    else:
        logger.info("  変更検出: 0 件（差分なし）")

    # sitemap
    if site_url:
        generate_sitemap(articles, site_url.rstrip("/"))
    else:
        logger.info("  SITE_URL 未設定 — sitemap.xml をスキップ")

    # 状態ファイル更新（全処理成功後のみ）
    state["last_sync_at"] = sync_start.isoformat()
    _save_state(state)
    logger.info(f"  Saved → data/sync_state.json (last_sync_at={sync_start.isoformat()})")


if __name__ == "__main__":
    main()
