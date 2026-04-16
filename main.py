"""
THE WATCHER — Hip-Hop News Auto-Update
Entry point for GitHub Actions and local runs.

Required environment variables:
  ANTHROPIC_API_KEY
  NOTION_API_KEY
  NOTION_DATABASE_ID

Optional:
  CONFIG_PATH  (default: config.json in the repo root)
"""

import json
import logging
import os
import sys

from src.rss_fetcher import fetch_all
from src.duplicate_checker import deduplicate, filter_against_history
from src.translator import process_articles
from src.notion_writer import save_all

# Windows では標準出力が cp932 になるため UTF-8 に統一する
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        logger.error(f"Missing required environment variable: {name}")
        sys.exit(1)
    return val


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    config_path = os.environ.get("CONFIG_PATH", "config.json")
    config = load_config(config_path)

    anthropic_key = _require_env("ANTHROPIC_API_KEY")
    notion_key = _require_env("NOTION_API_KEY")
    notion_db = _require_env("NOTION_DATABASE_ID")

    site = config.get("site_name", "THE WATCHER")
    logger.info("=" * 50)
    logger.info(f" {site} - Auto Update")
    logger.info("=" * 50)

    # ── Step 1: Fetch RSS ───────────────────────────────────────────────────
    logger.info("STEP 1 / 5 - Fetching RSS feeds")
    articles = fetch_all(
        sources=config["sources"],
        max_articles=config.get("max_articles_per_source", 20),
        max_age_hours=config.get("max_age_hours", 30),
    )

    if not articles:
        logger.warning("No articles fetched. Nothing to do.")
        return

    # ── Step 2: Filter already-known URLs (コスト最適化) ─────────────────
    # data/articles.json に既に記録されている URL は翻訳・保存済み。
    # ここで事前除外しておかないと process_articles で Claude を
    # 呼んでしまい、毎 run $2〜3 の重複課金が発生する。
    # (既存の save_article にも _article_exists チェックはあるが、それは
    #  翻訳の "後" なので Claude 呼び出しを止められない)
    logger.info("STEP 2 / 6 - Filtering already-known URLs")
    known_urls: set[str] = set()
    past_articles: list[dict] = []
    try:
        with open("data/articles.json", encoding="utf-8") as f:
            past_articles = json.load(f).get("articles", [])
        for a in past_articles:
            for u in (a.get("source_urls") or "").split(","):
                u = u.strip()
                if u:
                    known_urls.add(u)
    except FileNotFoundError:
        logger.info("  data/articles.json 不在 → 全件を新規として扱う")
    except Exception as e:
        logger.warning(f"  data/articles.json 読み込み失敗 (全件処理続行): {e}")

    before = len(articles)
    articles = [a for a in articles if a.get("url") not in known_urls]
    logger.info(
        f"  既知URL除外: {before} → {len(articles)} 件 "
        f"(除外 {before - len(articles)} 件, 既知URL DB {len(known_urls)} 件)"
    )

    if not articles:
        logger.info("新規記事なし。翻訳・保存はスキップ。")
        return

    # ── Step 3: Deduplicate within current run ──────────────────────────────
    # 同一 cron run 内で複数サイトが同一話題を報じている場合の重複統合
    logger.info("STEP 3 / 6 - Deduplication (within-run)")
    articles = deduplicate(
        articles,
        threshold=config.get("duplicate_threshold", 0.8),
    )

    if not articles:
        logger.warning("All articles were duplicates. Nothing new to save.")
        return

    # ── Step 4: Filter cross-run duplicates against history ─────────────────
    # 過去 cron run で既に保存された記事と同一出来事を報じる新規記事を
    # 翻訳前に除外する (Claude コストとサイト上の記事重複を同時に防止)。
    # 言語混在のため英語固有名詞トークンの集合一致で判定。
    logger.info("STEP 4 / 6 - Filtering cross-run duplicates against history")
    articles = filter_against_history(articles, past_articles)

    if not articles:
        logger.info("全記事が履歴と重複。翻訳・保存はスキップ。")
        return

    # ── Step 5: Translate & Categorise ──────────────────────────────────────
    logger.info("STEP 5 / 6 - Translating & categorising with Claude")
    articles = process_articles(articles, anthropic_key)

    # ── Step 6: Save to Notion ───────────────────────────────────────────────
    logger.info("STEP 6 / 6 - Saving to Notion")
    saved = save_all(articles, notion_key, notion_db)

    logger.info("=" * 50)
    logger.info(f" Done. {saved} new article(s) saved to Notion.")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
