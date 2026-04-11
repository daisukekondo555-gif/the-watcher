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
from src.duplicate_checker import deduplicate
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
    logger.info("STEP 1 / 4 - Fetching RSS feeds")
    articles = fetch_all(
        sources=config["sources"],
        max_articles=config.get("max_articles_per_source", 20),
        max_age_hours=config.get("max_age_hours", 30),
    )

    if not articles:
        logger.warning("No articles fetched. Nothing to do.")
        return

    # ── Step 2: Deduplicate ─────────────────────────────────────────────────
    logger.info("STEP 2 / 4 - Deduplication")
    articles = deduplicate(
        articles,
        threshold=config.get("duplicate_threshold", 0.8),
    )

    if not articles:
        logger.warning("All articles were duplicates. Nothing new to save.")
        return

    # ── Step 3: Translate & Categorise ──────────────────────────────────────
    logger.info("STEP 3 / 4 - Translating & categorising with Claude")
    articles = process_articles(articles, anthropic_key)

    # ── Step 4: Save to Notion ───────────────────────────────────────────────
    logger.info("STEP 4 / 4 - Saving to Notion")
    saved = save_all(articles, notion_key, notion_db)

    logger.info("=" * 50)
    logger.info(f" Done. {saved} new article(s) saved to Notion.")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
