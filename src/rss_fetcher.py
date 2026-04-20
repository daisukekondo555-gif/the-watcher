"""
RSS fetch module.
Fetches articles from configured sources in parallel.
Image priority:
  1. RSS enclosure (image/* or image-extension URL)
  2. media:content / media:thumbnail
  3. First <img> in RSS content/summary HTML
  4. OGP (og:image, twitter:image, link[rel=image_src])
  5. First <img> on actual article page
"""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlparse, urlencode, parse_qsl

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

# Tracking query parameters to strip from article URLs
_TRACKING_PARAMS = frozenset({
    # Google UTM
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id",
    # Google / Facebook click IDs
    "gclid", "gclsrc", "fbclid", "msclkid",
    # Miscellaneous
    "ref", "referrer", "mc_cid", "mc_eid",
})


def _clean_url(url: str) -> str:
    """Strip tracking query parameters from a URL, preserving all others."""
    try:
        parsed = urlparse(url)
        filtered = [(k, v) for k, v in parse_qsl(parsed.query) if k.lower() not in _TRACKING_PARAMS]
        clean = parsed._replace(query=urlencode(filtered))
        return clean.geturl()
    except Exception:
        return url


# Mimic a real browser to avoid bot-detection blocks.
# Sec-Fetch-* / Sec-Ch-Ua は Cloudflare の Browser Integrity Check が検査するため必須。
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.com/",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}
FETCH_TIMEOUT = 20
MAX_RETRIES = 2
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif")

# WAF / CDN 系の瞬間ブロックで返りやすく、リトライする価値のあるステータス。
# 404/410 (不在) や 401 (認証) はリトライしても意味がないので対象外。
RETRYABLE_HTTP_CODES = frozenset({403, 429, 503, 520, 521, 522, 523, 524})


def _is_image_url(url: str) -> bool:
    """Return True if URL looks like a direct image link."""
    return any(url.lower().split("?")[0].endswith(ext) for ext in IMAGE_EXTENSIONS)


def _get_with_retry(url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """GET with retry on timeout / connection errors and WAF-style HTTP errors.

    HTTPError 分岐の挙動:
      - 403/429/503/520/522/524 (WAF/CDN 系の瞬間ブロック) → 漸増バックオフでリトライ
      - それ以外 (404/410/401 等) → リトライ無意味なので即 break
    """
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            if attempt < retries:
                logger.debug(f"Timeout ({attempt + 1}/{retries + 1}) for {url}, retrying…")
                time.sleep(1)
            else:
                logger.debug(f"Timeout after {retries + 1} attempts: {url}")
        except requests.exceptions.ConnectionError:
            if attempt < retries:
                logger.debug(f"Connection error ({attempt + 1}/{retries + 1}) for {url}, retrying…")
                time.sleep(1)
            else:
                logger.debug(f"Connection error after {retries + 1} attempts: {url}")
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code in RETRYABLE_HTTP_CODES and attempt < retries:
                # 3s → 6s の漸増バックオフ。WAF のレート窓を少し跨がせる。
                wait = 3 * (attempt + 1)
                logger.debug(
                    f"HTTP {code} ({attempt + 1}/{retries + 1}) for {url}, "
                    f"retrying after {wait}s…"
                )
                time.sleep(wait)
            else:
                reason = (
                    "attempts exhausted"
                    if code in RETRYABLE_HTTP_CODES
                    else "not retryable"
                )
                logger.debug(f"HTTP {code} for {url} ({reason})")
                break
        except Exception as e:
            logger.debug(f"Request failed for {url}: {e}")
            break
    return None


def _fetch_ogp_image(url: str) -> Optional[str]:
    """
    Scrape image from article page.
    Priority: og:image → twitter:image → link[rel=image_src] → first <img>
    """
    try:
        resp = _get_with_retry(url)
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")

        # og:image — both property= and name= variants
        for attr, values in [
            ("property", ["og:image", "og:image:url"]),
            ("name",     ["og:image", "og:image:url"]),
        ]:
            for val in values:
                tag = soup.find("meta", attrs={attr: val})
                if tag and tag.get("content"):
                    return tag["content"].strip()

        # twitter:image
        for attr, values in [
            ("name",     ["twitter:image", "twitter:image:src"]),
            ("property", ["twitter:image", "twitter:image:src"]),
        ]:
            for val in values:
                tag = soup.find("meta", attrs={attr: val})
                if tag and tag.get("content"):
                    return tag["content"].strip()

        # <link rel="image_src">
        link_tag = soup.find("link", rel="image_src")
        if link_tag and link_tag.get("href"):
            return link_tag["href"].strip()

        # First <img> with a reasonable size hint or just any src
        for img in soup.find_all("img", src=True):
            src = img["src"].strip()
            if not src.startswith("http"):
                continue
            # Skip tiny icons/tracking pixels
            width = img.get("width") or img.get("data-width") or ""
            height = img.get("height") or img.get("data-height") or ""
            try:
                if int(str(width)) < 100 or int(str(height)) < 100:
                    continue
            except (ValueError, TypeError):
                pass
            if any(skip in src for skip in ("pixel", "beacon", "tracker", "1x1", "spacer")):
                continue
            return src

        return None
    except Exception as e:
        logger.debug(f"OGP fetch failed for {url}: {e}")
        return None


def _get_image_from_entry(entry) -> Optional[str]:
    """
    Extract image URL from RSS entry fields (no network requests).
    Priority: enclosure → media:content → media:thumbnail → content/summary <img>
    """
    # 1. enclosure: image/* MIME or image-looking URL
    for enc in getattr(entry, "enclosures", []):
        url = enc.get("url", "").strip()
        if not url:
            continue
        mime = enc.get("type", "")
        if mime.startswith("image/") or _is_image_url(url):
            return url

    # 2. media:content
    for media in getattr(entry, "media_content", []):
        url = media.get("url", "").strip()
        if not url:
            continue
        mime = media.get("type", "")
        medium = media.get("medium", "")
        if medium == "image" or mime.startswith("image/") or _is_image_url(url):
            return url

    # 3. media:thumbnail
    thumbnails = getattr(entry, "media_thumbnail", [])
    if thumbnails and thumbnails[0].get("url"):
        return thumbnails[0]["url"].strip()

    # 4. First <img> inside RSS content/summary HTML
    raw_html = ""
    if getattr(entry, "content", None):
        raw_html = entry.content[0].get("value", "")
    if not raw_html:
        raw_html = getattr(entry, "summary", "") or ""

    if raw_html:
        soup = BeautifulSoup(raw_html, "html.parser")
        for img in soup.find_all("img", src=True):
            src = img["src"].strip()
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
            link = _clean_url((entry.get("link") or "").strip())
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
            CONTENT_LIMIT = 8000
            full_content = _clean_text(raw_html) if raw_html else ""
            content = full_content[:CONTENT_LIMIT]
            input_truncated = len(full_content) > CONTENT_LIMIT

            # Image from RSS fields (no network)
            image_url = _get_image_from_entry(entry)

            article_dict = {
                "title": title,
                "url": link,
                "published": published,
                "content": content,
                "image_url": image_url,
                "source_name": name,
            }
            if input_truncated:
                article_dict["input_truncated"] = True
            articles.append(article_dict)

        with_img = sum(1 for a in articles if a["image_url"])
        logger.info(
            f"[{name}] {len(articles)} articles (within {max_age_hours}h) "
            f"— {with_img}/{len(articles)} with image from RSS"
        )
    except Exception as e:
        logger.error(f"[{name}] Unexpected error: {e}")

    return articles


def _enrich_image(article: dict) -> dict:
    """If image_url is still None, scrape the article page for an image."""
    if not article["image_url"] and article["url"]:
        logger.debug(f"Page scrape for image: {article['url']}")
        article["image_url"] = _fetch_ogp_image(article["url"])
    return article


def fetch_all(sources: list[dict], max_articles: int = 20, max_age_hours: int = 30) -> list[dict]:
    """
    Fetch RSS from all sources in parallel, then scrape missing images.
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

    total = len(all_articles)
    with_img = sum(1 for a in all_articles if a["image_url"])
    logger.info(
        f"Total articles before dedup: {total} "
        f"— image hit rate from RSS: {with_img}/{total} ({100 * with_img // max(total, 1)}%)"
    )

    # Parallel page scrape for articles still missing an image
    needs_scrape = [a for a in all_articles if not a["image_url"]]
    if needs_scrape:
        logger.info(f"Scraping article pages for images: {len(needs_scrape)} articles…")
        # Throttle to 8 workers to avoid triggering rate limits
        with ThreadPoolExecutor(max_workers=8) as pool:
            enriched = list(pool.map(_enrich_image, needs_scrape))
        ogp_map = {a["url"]: a["image_url"] for a in enriched}
        for a in all_articles:
            if not a["image_url"] and a["url"] in ogp_map:
                a["image_url"] = ogp_map[a["url"]]

    final_with_img = sum(1 for a in all_articles if a["image_url"])
    logger.info(
        f"Image hit rate after page scrape: {final_with_img}/{total} "
        f"({100 * final_with_img // max(total, 1)}%)"
    )

    # Sort newest-first
    all_articles.sort(key=lambda a: a["published"], reverse=True)
    return all_articles
