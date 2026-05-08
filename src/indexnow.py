"""
IndexNow プロトコルによる新規/更新記事の即時通知。

新規記事の URL を Bing / Yandex / Naver 等の IndexNow 参加検索エンジンに
一括通知する。data/indexnow_sent.json に送信済み URL を記録して重複通知を防止。

エラー時もメイン処理を止めない (全例外を catch してログ出力のみ)。
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

ROOT = Path(__file__).resolve().parent.parent
ARTICLES_JSON = ROOT / "data" / "articles.json"
SENT_FILE = ROOT / "data" / "indexnow_sent.json"

SITE_URL = "https://thewatcherjp.com"
INDEXNOW_KEY = "3490238cb3a04943932db648a6af084e"
INDEXNOW_ENDPOINT = "https://api.indexnow.org/indexnow"


def _load_sent() -> set[str]:
    try:
        return set(json.loads(SENT_FILE.read_text(encoding="utf-8")))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def _save_sent(sent: set[str]) -> None:
    SENT_FILE.parent.mkdir(exist_ok=True)
    SENT_FILE.write_text(
        json.dumps(sorted(sent), ensure_ascii=False), encoding="utf-8"
    )


def notify() -> None:
    """articles.json の全記事 URL のうち未送信分を IndexNow に通知する。"""
    try:
        data = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"IndexNow: articles.json 読み込み失敗: {e}")
        return

    articles = data.get("articles", [])
    all_urls = [
        f"{SITE_URL}/articles/{a['id']}.html"
        for a in articles
        if a.get("id")
    ]

    sent = _load_sent()
    new_urls = [u for u in all_urls if u not in sent]

    if not new_urls:
        logger.info("IndexNow: 新規 URL なし")
        return

    logger.info(f"IndexNow: {len(new_urls)} 件の新規 URL を通知")

    payload = {
        "host": "thewatcherjp.com",
        "key": INDEXNOW_KEY,
        "keyLocation": f"{SITE_URL}/{INDEXNOW_KEY}.txt",
        "urlList": new_urls[:10000],
    }

    try:
        resp = requests.post(
            INDEXNOW_ENDPOINT,
            json=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=30,
        )
        logger.info(f"IndexNow: HTTP {resp.status_code} ({len(new_urls)} URLs)")

        if resp.status_code in (200, 202):
            sent.update(new_urls)
            _save_sent(sent)
        else:
            logger.warning(f"IndexNow: 予期しないステータス {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"IndexNow: 通知失敗 (メイン処理に影響なし): {e}")

    # 古い URL を sent から削除 (articles.json にない URL は不要)
    current_url_set = set(all_urls)
    stale = sent - current_url_set
    if stale:
        sent -= stale
        _save_sent(sent)
        logger.info(f"IndexNow: 送信済みリストから {len(stale)} 件の古い URL を削除")


if __name__ == "__main__":
    notify()
