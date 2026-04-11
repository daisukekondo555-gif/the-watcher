"""
RSS fetch module.
Fetches articles from configured sources in parallel.
Image priority: RSS enclosure > media:content/thumbnail > content img > OGP scrape
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TheWatcher-Bot/1.0)"}
FETCH_TIMEOUT = 15


def _fetch_ogp_image(url: str) -> Optional[str]:
    """Scrape OGP / Twitter card image from article page."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for prop in ("og:image", "og:image:url"):
            tag = soup.find("meta", property=prop)
            if tag and tag.get("content"):
                return tag["content"]

        for name in ("twitter:image", "twitter:image:src"):
            tag = soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"):
                return tag["content"]

        return None
    except Exception as e:
        logger.debug(f"OGP fetch failed for {url}: {e}")
        return None


def _get_image_from_entry(entry) -> Optional[str]:
    """
    Extract image URL from RSS entry fields.
    Does NOT make any network requests.
    """
    # 1. enclosure (standard podcast/image attachment)
    for enc in getattr(entry, "enclosures", []):
        if enc.get("type", "").startswith("image/") and enc.get("url"):
            return enc["url"]

    # 2. media:content
    for media in getattr(entry, "media_content", []):
        t = media.get("type", "")
        if (media.get("medium") == "image" or t.startswith("image/")) and media.get("url"):
            return media["url"]

    # 3. media:thumbnail
    thumbnails = getattr(entry, "media_thumbnail", [])
    if thumbnails and thumbnails[0].get("url"):
        return thumbnails[0]["url"]

    # 4. First <img> inside content/summary HTML
    raw_html = ""
    if getattr(entry, "content", None):
        raw_html = entry.content[0].get("value", "")
    if not raw_html:
        raw_html = getattr(entry, "summary", "") or ""

    if raw_html:
        soup = BeautifulSoup(raw_html, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            src = img["src"]
            if src.startswith("http"):
                return src

    return None


def _parse_date(entry) -> str:
    """Return ISO-8601 UTC string for the entry's publish date."""
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                dt = datetime(*val[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass

    for attr in ("published", "updated", "created"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                dt = dateparser.parse(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass

    return datetime.now(timezone.utc).isoformat()


def _clean_text(html: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    text = BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)
    return re.sub(r"\s{2,}", " ", text)


def _fetch_source(source: dict, max_articles: int, max_age_hours: int) -> list[dict]:
    """Fetch and parse one RSS source. Returns list of article dicts."""
    name = source["name"]
    url = source["rss_url"]
    articles = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)

    logger.info(f"[{name}] Fetching {url}")
    try:
        feed = feedparser.parse(url)
        if feed.bozo and not feed.entries:
            logger.warning(f"[{name}] RSS parse error: {feed.bozo_exception}")
            return []

        for entry in feed.entries[:max_articles]:
            title = (entry.get("title") or "").strip()
            link = (entry.get("link") or "").strip()
            if not title or not link:
                continue

            published = _parse_date(entry)

            # Age filter
            try:
                pub_dt = dateparser.parse(published)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                if pub_dt < cutoff:
                    continue
            except Exception:
                pass

            # Content text
            raw_html = ""
            if getattr(entry, "content", None):
                raw_html = entry.content[0].get("value", "")
            if not raw_html:
                raw_html = getattr(entry, "summary", "") or ""
            content = _clean_text(raw_html)[:3000] if raw_html else ""

            # Image (no network yet)
            image_url = _get_image_from_entry(entry)

            articles.append({
                "title": title,
                "url": link,
                "published": published,
                "content": content,
                "image_url": image_url,
                "source_name": name,
            })

        logger.info(f"[{name}] {len(articles)} articles (within {max_age_hours}h)")
    except Exception as e:
        logger.error(f"[{name}] Unexpected error: {e}")

    return articles


def _enrich_image(article: dict) -> dict:
    """If image_url is still None, attempt OGP scrape (network call)."""
    if not article["image_url"] and article["url"]:
        logger.debug(f"OGP scrape: {article['url']}")
        article["image_url"] = _fetch_ogp_image(article["url"])
    return article


def fetch_all(sources: list[dict], max_articles: int = 20, max_age_hours: int = 30) -> list[dict]:
    """
    Fetch RSS from all sources in parallel, then OGP-enrich missing images.
    Returns a flat list of article dicts sorted newest-first.
    """
    all_articles: list[dict] = []

    # Parallel RSS fetch
    with ThreadPoolExecutor(max_workers=len(sources)) as pool:
        futures = {
            pool.submit(_fetch_source, src, max_articles, max_age_hours): src["name"]
            for src in sources
        }
        for future in as_completed(futures):
            try:
                all_articles.extend(future.result())
            except Exception as e:
                logger.error(f"RSS fetch thread error ({futures[future]}): {e}")

    logger.info(f"Total articles fetched (before dedup): {len(all_articles)}")

    # Parallel OGP enrichment for articles that still have no image
    needs_ogp = [a for a in all_articles if not a["image_url"]]
    if needs_ogp:
        logger.info(f"OGP scraping for {len(needs_ogp)} articles…")
        with ThreadPoolExecutor(max_workers=10) as pool:
            enriched = list(pool.map(_enrich_image, needs_ogp))
        # Merge back
        ogp_map = {a["url"]: a["image_url"] for a in enriched}
        for a in all_articles:
            if not a["image_url"] and a["url"] in ogp_map:
                a["image_url"] = ogp_map[a["url"]]

    # Sort newest-first
    all_articles.sort(key=lambda a: a["published"], reverse=True)
    return all_articles
