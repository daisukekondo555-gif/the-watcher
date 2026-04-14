"""
記事ごとの静的HTMLをプリレンダリングするビルドスクリプト

article.html をテンプレートとして data/articles.json を読み込み、
各記事について articles/<id>.html を生成する。

生成ファイルは以下が article.html と異なる:
  1. OGP / Twitter Card メタタグに記事固有の値がstaticに埋め込まれる
     （X・LINE等のSNSクローラはJSを実行しないため、staticな値が必要）
  2. <title> に記事タイトル
  3. <base href="/"> を挿入して、一段深いパスでも相対リンクが正しく解決される
  4. <script>window.__ARTICLE_ID="<id>";</script> を埋め込み、
     JSがpathname解析なしで即座に記事IDを取得できるようにする

GitHub Actions で export_notion.py のあとに実行される想定。
"""

from __future__ import annotations

import html
import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = ROOT / "article.html"
ARTICLES_JSON = ROOT / "data" / "articles.json"
OUTPUT_DIR = ROOT / "articles"

SITE_URL = "https://thewatcherjp.com"
SITE_NAME = "THE WATCHER"
DEFAULT_OGP_IMAGE = f"{SITE_URL}/assets/logo-mark.png"
TWITTER_SITE = "@thewatcherjp"
DESC_MAX_LEN = 120


def _esc_attr(value: str) -> str:
    """HTML属性値としてエスケープ"""
    return html.escape(value or "", quote=True)


def _summarize(text: str, limit: int = DESC_MAX_LEN) -> str:
    s = (text or "").replace("\n", " ").replace("\r", " ").strip()
    if len(s) > limit:
        s = s[: limit - 1].rstrip() + "…"
    return s


def _proxy_image(url: str) -> str:
    """wsrv.nl 経由で WebP 変換。ホットリンク保護回避 + サイズ最適化。
    X の og:image は横 1200px 以上が推奨なので w=1200 を指定する。"""
    if not url:
        return DEFAULT_OGP_IMAGE
    if "wsrv.nl" in url:
        return url
    from urllib.parse import quote
    return f"https://wsrv.nl/?url={quote(url, safe='')}&w=1200&output=jpg"


def _replace_meta(html_text: str, selector: str, attr: str, new_content: str) -> str:
    """<meta {selector}> の content 属性を置換する。"""
    pattern = re.compile(
        r'(<meta\s+' + selector + r'[^>]*\scontent=)"[^"]*"',
        flags=re.IGNORECASE,
    )
    replacement = r'\1"' + _esc_attr(new_content).replace("\\", "\\\\") + '"'
    new_text, n = pattern.subn(replacement, html_text, count=1)
    if n == 0:
        logger.warning(f"meta tag not found for selector: {selector}")
    return new_text


def _render_one(template: str, article: dict) -> str:
    aid = article["id"]
    title = article.get("title", SITE_NAME)
    desc = _summarize(article.get("summary", ""))
    image_url = _proxy_image(article.get("image_url", ""))
    page_url = f"{SITE_URL}/articles/{aid}.html"
    full_title = f"{title} — {SITE_NAME}"

    out = template

    # <title>
    out = re.sub(
        r"<title>[^<]*</title>",
        f"<title>{_esc_attr(full_title)}</title>",
        out,
        count=1,
    )

    # <meta name="description">
    out = _replace_meta(out, r'name="description"', "content", desc)

    # OGP
    out = _replace_meta(out, r'property="og:title"', "content", full_title)
    out = _replace_meta(out, r'property="og:description"', "content", desc)
    out = _replace_meta(out, r'property="og:image"', "content", image_url)
    out = _replace_meta(out, r'property="og:url"', "content", page_url)

    # Twitter Card
    out = _replace_meta(out, r'name="twitter:title"', "content", full_title)
    out = _replace_meta(out, r'name="twitter:description"', "content", desc)
    out = _replace_meta(out, r'name="twitter:image"', "content", image_url)

    # <base href="/"> と window.__ARTICLE_ID を <head> 直後に挿入。
    # <base> により、一段深い articles/ 配下でも assets/ や data/ などの
    # 相対URLが正しくルート起点で解決される。
    inject = (
        '\n<base href="/">'
        f'\n<script>window.__ARTICLE_ID={json.dumps(aid)};</script>'
    )
    out = out.replace("<head>", "<head>" + inject, 1)

    return out


def build() -> None:
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(TEMPLATE_PATH)
    if not ARTICLES_JSON.exists():
        raise FileNotFoundError(ARTICLES_JSON)

    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    data = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    articles = data.get("articles", [])

    OUTPUT_DIR.mkdir(exist_ok=True)

    # 既存の articles/<id>.html をクリーンアップ（削除済み記事を残さない）
    existing_ids = {a["id"] for a in articles if a.get("id")}
    for path in OUTPUT_DIR.glob("*.html"):
        if path.stem not in existing_ids:
            path.unlink()
            logger.info(f"removed stale: {path.name}")

    count = 0
    for article in articles:
        aid = article.get("id")
        if not aid:
            continue
        rendered = _render_one(template, article)
        (OUTPUT_DIR / f"{aid}.html").write_text(rendered, encoding="utf-8")
        count += 1

    logger.info(f"generated {count} article pages in {OUTPUT_DIR}")


if __name__ == "__main__":
    build()
