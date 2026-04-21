"""
RSS fetch module.
Fetches articles from configured sources in parallel.

Image priority (3 phase):
  高信頼: RSS enclosure / media:content
  中信頼: og:image → twitter:image → schema.org/JSON-LD → link[image_src]
  低信頼: RSS thumbnail → RSS 本文 <img> → ページ <img>

Content enrichment:
  RSS content が 500 字未満の場合、trafilatura でページ本文を補完。
  trafilatura 結果が RSS より短ければ RSS を維持。
"""

import json as _json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlparse, urlencode, parse_qsl

import feedparser
import requests
import trafilatura
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
MAX_RETRIES = 3
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


def _is_bad_image(url: str) -> bool:
    """画像 URL が絵文字・ロゴ・広告・UI パーツなどの誤画像かを判定する。
    誤爆を避けるため、パターンは厳密に絞る。"""
    low = url.lower()

    # 最優先除外: WordPress 絵文字 (変な画像問題の主犯)
    if "s.w.org" in low:
        return True
    if "/emoji/" in low or "twemoji" in low:
        return True

    # 確実な広告ドメイン
    if "doubleclick.net" in low or "googlesyndication" in low:
        return True

    # サイトロゴ (厳密パターンのみ — "logo" 単体マッチは誤爆するため避ける)
    if "-logo." in low or "/site-logo/" in low or "-logo_" in low:
        return True

    # UI パーツ (パス区切り or 拡張子直前で判定して誤爆を防ぐ)
    if "/spinner." in low or "/loading-" in low or "/placeholder." in low:
        return True

    # トラッキングピクセル
    if "pixel" in low or "beacon" in low or "tracker" in low or "/1x1" in low or "spacer" in low:
        return True

    return False


def _extract_image_from_html(soup: BeautifulSoup) -> Optional[str]:
    """パース済み HTML から画像 URL を取得する (HTTP fetch は呼び出し元が行う)。
    優先度: og:image → twitter:image → schema.org/JSON-LD → link[image_src] → <img> フォールバック"""

    # ── 中信頼: OGP メタタグ ──
    for attr, values in [
        ("property", ["og:image", "og:image:url"]),
        ("name",     ["og:image", "og:image:url"]),
    ]:
        for val in values:
            tag = soup.find("meta", attrs={attr: val})
            if tag and tag.get("content"):
                img = tag["content"].strip()
                if img and not _is_bad_image(img):
                    return img

    # twitter:image
    for attr, values in [
        ("name",     ["twitter:image", "twitter:image:src"]),
        ("property", ["twitter:image", "twitter:image:src"]),
    ]:
        for val in values:
            tag = soup.find("meta", attrs={attr: val})
            if tag and tag.get("content"):
                img = tag["content"].strip()
                if img and not _is_bad_image(img):
                    return img

    # schema.org / JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = _json.loads(script.string or "")
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            img = ld.get("image")
            if isinstance(img, dict):
                img = img.get("url")
            elif isinstance(img, list):
                img = img[0] if img else None
                if isinstance(img, dict):
                    img = img.get("url")
            if img and isinstance(img, str) and not _is_bad_image(img):
                return img.strip()
        except Exception:
            pass

    # <link rel="image_src">
    link_tag = soup.find("link", rel="image_src")
    if link_tag and link_tag.get("href"):
        img = link_tag["href"].strip()
        if img and not _is_bad_image(img):
            return img

    # ── 低信頼: <img> フォールバック (厳格フィルタ) ──
    for img_tag in soup.find_all("img", src=True):
        src = img_tag["src"].strip()
        if not src.startswith("http"):
            continue
        if _is_bad_image(src):
            continue
        width = img_tag.get("width") or img_tag.get("data-width") or ""
        height = img_tag.get("height") or img_tag.get("data-height") or ""
        try:
            if int(str(width)) < 100 or int(str(height)) < 100:
                continue
        except (ValueError, TypeError):
            pass
        return src

    return None


def _enrich_content(article: dict, html: str) -> None:
    """RSS content が短い場合、trafilatura でページ本文を補完する。
    trafilatura 結果が RSS より短ければ RSS をそのまま維持 (フォールバック)。"""
    current = article.get("content", "")
    if len(current) >= 500:
        return

    if not html:
        return

    try:
        page_text = trafilatura.extract(
            html, include_comments=False, include_tables=False
        ) or ""
    except Exception as e:
        logger.debug(f"trafilatura failed for {article.get('url','')}: {e}")
        return

    if len(page_text) > len(current):
        article["content"] = page_text[:8000]


def _get_high_confidence_image(entry) -> Optional[str]:
    """RSS フィードから高信頼度の画像を取得 (ネットワーク不要)。
    enclosure と media:content のみ。これらが明示的に画像として提供されているため
    絵文字やロゴが混入するリスクがない。
    ここで画像が取れなかった記事は _fetch_ogp_image でスクレイプする。"""

    # 1. enclosure: image/* MIME or image-looking URL
    for enc in getattr(entry, "enclosures", []):
        url = enc.get("url", "").strip()
        if not url:
            continue
        mime = enc.get("type", "")
        if mime.startswith("image/") or _is_image_url(url):
            if not _is_bad_image(url):
                return url

    # 2. media:content
    for media in getattr(entry, "media_content", []):
        url = media.get("url", "").strip()
        if not url:
            continue
        mime = media.get("type", "")
        medium = media.get("medium", "")
        if medium == "image" or mime.startswith("image/") or _is_image_url(url):
            if not _is_bad_image(url):
                return url

    return None


def _get_low_confidence_image(entry) -> Optional[str]:
    """RSS フィードから低信頼度の画像を取得 (OGP スクレイプ失敗時のフォールバック)。
    media:thumbnail と RSS 本文内の <img> を対象とし、厳格なフィルタを適用する。"""

    # media:thumbnail
    thumbnails = getattr(entry, "media_thumbnail", [])
    if thumbnails and thumbnails[0].get("url"):
        url = thumbnails[0]["url"].strip()
        if url and not _is_bad_image(url):
            return url

    # RSS content/summary 内の <img> (絵文字・ロゴのリスクあり → 厳格フィルタ)
    raw_html = ""
    if getattr(entry, "content", None):
        raw_html = entry.content[0].get("value", "")
    if not raw_html:
        raw_html = getattr(entry, "summary", "") or ""
    if raw_html:
        soup = BeautifulSoup(raw_html, "html.parser")
        for img in soup.find_all("img", src=True):
            src = img["src"].strip()
            if not src.startswith("http"):
                continue
            if _is_bad_image(src):
                continue
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

            # 画像取得 Phase 1: 高信頼 RSS フィールドのみ (ネットワーク不要)
            # enclosure / media:content があれば確定。なければ None → Phase 2 で OGP スクレイプ。
            image_url = _get_high_confidence_image(entry)

            article_dict = {
                "title": title,
                "url": link,
                "published": published,
                "content": content,
                "image_url": image_url,
                "source_name": name,
                "_rss_entry": entry,  # Phase 2 フォールバック用に保持
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


def _enrich_article(article: dict) -> dict:
    """画像取得 + 本文補完を 1 回の HTTP fetch で行う。
    画像: 高信頼 RSS → OGP (中信頼) → 低信頼 RSS フォールバック
    本文: RSS content < 500 字 → trafilatura でページ本文補完"""
    needs_image = not article["image_url"]
    needs_content = len(article.get("content", "")) < 500

    # 画像も本文も不要 → スキップ
    if not needs_image and not needs_content:
        return article

    # 1 回の fetch で HTML を取得 (画像 + 本文の両方に使う)
    html = ""
    soup = None
    if article["url"]:
        try:
            resp = _get_with_retry(article["url"])
            if resp:
                html = resp.text
                soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            logger.debug(f"Page fetch failed for {article['url']}: {e}")

    # 画像取得 (中信頼: OGP)
    if needs_image and soup:
        ogp_img = _extract_image_from_html(soup)
        if ogp_img:
            article["image_url"] = ogp_img

    # 本文補完 (trafilatura)
    if needs_content:
        _enrich_content(article, html)

    # 画像がまだない → 低信頼 RSS フォールバック
    if not article["image_url"]:
        entry = article.pop("_rss_entry", None)
        if entry:
            low_img = _get_low_confidence_image(entry)
            if low_img:
                article["image_url"] = low_img

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

    # 画像取得 + 本文補完 (1 回の HTTP fetch で両方処理)
    needs_enrich = [a for a in all_articles
                    if not a["image_url"] or len(a.get("content", "")) < 500]
    if needs_enrich:
        logger.info(
            f"Enriching articles (image + content): {len(needs_enrich)} articles…"
        )
        with ThreadPoolExecutor(max_workers=8) as pool:
            enriched = list(pool.map(_enrich_article, needs_enrich))
        # enriched は同じオブジェクト参照なので all_articles にも反映済み
        # ただし ogp_map 方式で明示的にマージ (安全策)
        enrich_map = {a["url"]: a for a in enriched}
        for a in all_articles:
            if a["url"] in enrich_map:
                e = enrich_map[a["url"]]
                if not a["image_url"] and e["image_url"]:
                    a["image_url"] = e["image_url"]
                if len(a.get("content", "")) < len(e.get("content", "")):
                    a["content"] = e["content"]

    final_with_img = sum(1 for a in all_articles if a["image_url"])
    enriched_content = sum(1 for a in all_articles if len(a.get("content", "")) >= 500)
    logger.info(
        f"After enrichment: images {final_with_img}/{total} "
        f"({100 * final_with_img // max(total, 1)}%), "
        f"content>=500ch {enriched_content}/{total}"
    )

    # _rss_entry を後続処理に渡さないよう除去
    for a in all_articles:
        a.pop("_rss_entry", None)

    # Sort newest-first
    all_articles.sort(key=lambda a: a["published"], reverse=True)
    return all_articles
