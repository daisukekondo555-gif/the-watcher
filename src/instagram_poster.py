"""
Instagram carousel poster via Meta's Instagram Graph API.

Why not Buffer?
  Buffer's API returns {"error": "OIDC tokens are not accepted for direct API access"}
  — their backend was migrated to internal OIDC auth and public API access is blocked.

Why not carousels without Facebook Page?
  Instagram Graph API (and every third-party service that wraps it) requires an
  Instagram Business account linked to a Facebook Page to obtain content-publish
  permissions. This is a hard constraint from Meta, not a tooling limitation.

Carousel post flow (Meta Graph API):
  1. Upload each image as a media container (is_carousel_item=true)
  2. Poll container status until FINISHED
  3. Create a CAROUSEL container referencing all child IDs
  4. Publish via media_publish

Required GitHub Actions secrets:
  INSTAGRAM_ACCESS_TOKEN          Long-lived Page Access Token with
                                  pages_read_engagement +
                                  instagram_basic +
                                  instagram_content_publish permissions
  INSTAGRAM_BUSINESS_ACCOUNT_ID  Numeric IG Business Account ID

How to obtain credentials:
  1. Create a Meta Developer App at https://developers.facebook.com/
  2. Add "Instagram" product, connect via a Facebook Page
  3. In Graph API Explorer, select your app + page token
  4. Get long-lived token: GET /oauth/access_token?
       grant_type=fb_exchange_token&client_id={app-id}&client_secret={secret}
       &fb_exchange_token={short-token}
  5. Get IG account ID: GET /me/accounts → find page_id →
       GET /{page-id}?fields=instagram_business_account
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.facebook.com/v19.0"
# How many hours back to look for "new" articles (should exceed cron interval)
DEFAULT_LOOKBACK_HOURS = 7
# Instagram carousel: 2–10 items
MIN_CAROUSEL_ITEMS = 2
MAX_CAROUSEL_ITEMS = 10
# Instagram caption limit
CAPTION_MAX = 2200
# Seconds to wait between container status polls
STATUS_POLL_INTERVAL = 4
STATUS_POLL_MAX_TRIES = 15


# ── Helpers ────────────────────────────────────────────────────────────────────

def _graph(method: str, path: str, access_token: str, **kwargs) -> dict:
    """Thin wrapper around requests for Graph API calls."""
    url = f"{GRAPH_BASE}/{path.lstrip('/')}"
    params = kwargs.pop("params", {})
    params["access_token"] = access_token
    resp = getattr(requests, method)(url, params=params, timeout=30, **kwargs)
    data = resp.json()
    if not resp.ok or data.get("error"):
        err = data.get("error", {})
        raise RuntimeError(
            f"Graph API error [{resp.status_code}]: "
            f"{err.get('message', resp.text[:200])}"
        )
    return data


def _wait_for_container(container_id: str, access_token: str) -> None:
    """Poll until a media container reaches FINISHED status."""
    for attempt in range(1, STATUS_POLL_MAX_TRIES + 1):
        data = _graph("get", container_id, access_token,
                      params={"fields": "status_code"})
        status = data.get("status_code", "")
        if status == "FINISHED":
            return
        if status == "ERROR":
            raise RuntimeError(f"Container {container_id} failed: {data}")
        logger.debug(
            f"  Container {container_id} status={status} "
            f"(attempt {attempt}/{STATUS_POLL_MAX_TRIES})"
        )
        time.sleep(STATUS_POLL_INTERVAL)
    raise TimeoutError(
        f"Container {container_id} did not finish in time"
    )


# ── Article selection ──────────────────────────────────────────────────────────

def get_new_articles(
    articles_path: str,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
) -> list[dict]:
    """Return articles with images published within the last lookback_hours."""
    with open(articles_path, encoding="utf-8") as f:
        data = json.load(f)

    all_articles = data.get("articles", [])
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    new_with_img: list[dict] = []
    new_no_img: list[dict] = []

    for a in all_articles:
        pub = a.get("published_at", "")
        if not pub:
            continue
        try:
            pub_dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if pub_dt < cutoff:
            continue
        if a.get("image_url"):
            new_with_img.append(a)
        else:
            new_no_img.append(a)

    # Prefer articles that have images; supplement with image-less ones
    return (new_with_img + new_no_img)[:MAX_CAROUSEL_ITEMS]


# ── Caption builder ────────────────────────────────────────────────────────────

def build_caption(articles: list[dict], site_url: str) -> str:
    """Build an Instagram caption listing all article titles."""
    lines = ["🎤 THE WATCHER — 最新ヒップホップニュース\n"]

    for i, a in enumerate(articles, 1):
        title = a.get("title", "（タイトルなし）")
        lines.append(f"{i}. {title}")

    lines += [
        "",
        f"▶ 全記事は {site_url}",
        "",
        "#HipHop #HipHopNews #ヒップホップ #TheWatcher"
        " #Rap #NewMusic #RapNews",
    ]

    caption = "\n".join(lines)
    if len(caption) > CAPTION_MAX:
        caption = caption[: CAPTION_MAX - 3] + "..."
    return caption


# ── Posting ────────────────────────────────────────────────────────────────────

def upload_carousel_item(
    ig_user_id: str, image_url: str, access_token: str
) -> str:
    """Upload a single carousel item and return its container ID."""
    logger.info(f"  Uploading carousel item: {image_url[:80]}")
    data = _graph(
        "post", f"{ig_user_id}/media", access_token,
        data={
            "image_url": image_url,
            "is_carousel_item": "true",
        },
    )
    container_id = data["id"]
    _wait_for_container(container_id, access_token)
    logger.info(f"  Container ready: {container_id}")
    return container_id


def create_carousel(
    ig_user_id: str,
    child_ids: list[str],
    caption: str,
    access_token: str,
) -> str:
    """Create a CAROUSEL container from child container IDs."""
    data = _graph(
        "post", f"{ig_user_id}/media", access_token,
        data={
            "media_type": "CAROUSEL",
            "children": ",".join(child_ids),
            "caption": caption,
        },
    )
    carousel_id = data["id"]
    _wait_for_container(carousel_id, access_token)
    return carousel_id


def publish_container(
    ig_user_id: str, creation_id: str, access_token: str
) -> str:
    """Publish a media/carousel container. Returns the published post ID."""
    data = _graph(
        "post", f"{ig_user_id}/media_publish", access_token,
        data={"creation_id": creation_id},
    )
    return data["id"]


def post_single_image(
    ig_user_id: str,
    image_url: str,
    caption: str,
    access_token: str,
) -> str:
    """Post a single image (fallback when only 1 image available)."""
    logger.info(f"  Uploading single image: {image_url[:80]}")
    data = _graph(
        "post", f"{ig_user_id}/media", access_token,
        data={"image_url": image_url, "caption": caption},
    )
    container_id = data["id"]
    _wait_for_container(container_id, access_token)
    return publish_container(ig_user_id, container_id, access_token)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    access_token = os.environ.get("INSTAGRAM_ACCESS_TOKEN", "")
    ig_user_id = os.environ.get("INSTAGRAM_BUSINESS_ACCOUNT_ID", "")

    if not access_token or not ig_user_id:
        logger.info(
            "INSTAGRAM_ACCESS_TOKEN / INSTAGRAM_BUSINESS_ACCOUNT_ID not set "
            "— skipping Instagram post"
        )
        return

    articles_path = "data/articles.json"
    if not Path(articles_path).exists():
        logger.warning("articles.json not found — skipping Instagram post")
        return

    # Lookback window: slightly longer than cron interval to avoid gaps
    lookback = int(os.environ.get("IG_LOOKBACK_HOURS", DEFAULT_LOOKBACK_HOURS))
    new_articles = get_new_articles(articles_path, lookback_hours=lookback)

    if not new_articles:
        logger.info(
            f"No new articles in the last {lookback}h — skipping Instagram post"
        )
        return

    logger.info(
        f"Found {len(new_articles)} new article(s) to post (lookback={lookback}h)"
    )

    # Load site_url from config if available
    try:
        with open("config.json", encoding="utf-8") as f:
            cfg = json.load(f)
        site_url = cfg.get("site_url", "https://thewatcherjp.com")
    except Exception:
        site_url = "https://thewatcherjp.com"

    caption = build_caption(new_articles, site_url)
    articles_with_img = [a for a in new_articles if a.get("image_url")]

    try:
        if len(articles_with_img) >= MIN_CAROUSEL_ITEMS:
            # ── Carousel post ──────────────────────────────────────────────
            logger.info(
                f"Posting carousel with {len(articles_with_img)} images"
            )
            child_ids = [
                upload_carousel_item(
                    ig_user_id, a["image_url"], access_token
                )
                for a in articles_with_img[:MAX_CAROUSEL_ITEMS]
            ]
            carousel_id = create_carousel(
                ig_user_id, child_ids, caption, access_token
            )
            post_id = publish_container(ig_user_id, carousel_id, access_token)
            logger.info(f"Carousel published ✓  post_id={post_id}")

        elif len(articles_with_img) == 1:
            # ── Single image fallback ──────────────────────────────────────
            logger.info("Only 1 image available — posting single image")
            post_id = post_single_image(
                ig_user_id,
                articles_with_img[0]["image_url"],
                caption,
                access_token,
            )
            logger.info(f"Single image published ✓  post_id={post_id}")

        else:
            # ── No images: skip (Instagram requires at least 1 image) ──────
            logger.info(
                "No images found in new articles — skipping Instagram post "
                "(Instagram requires at least one image)"
            )

    except Exception as exc:
        # Log but do not re-raise — we don't want Instagram failures
        # to block the articles.json commit step in CI
        logger.error(f"Instagram post failed: {exc}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    main()
