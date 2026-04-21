"""
OGP共有ページ生成スクリプト（Phase 1・差分ビルド）

SNSクローラ (X / LINE Bot 等) は JS を実行しないため、SPA方式の
article.html?id=X では記事固有の OGP が反映されない。
この問題を既存URL構造を維持したまま解決するため、各記事に対して
SNSクローラ専用の静的ページ articles/<id>.html を生成する。

生成ページの動作:
  - <head> に記事固有の OGP / Twitter Card メタタグを埋め込み
    → SNSクローラはこれを読み取ってサムネイル・タイトルを表示
  - <link rel="canonical"> で "本物の記事URL" (article.html?id=X) を指示
    → 検索エンジンは article.html 側をインデックスし、SEO影響を最小化
  - <meta http-equiv="refresh" content="0; url=/article.html?id=X">
    → 通常ユーザー (JS実行可) は即座に本来の記事ページへ転送
  - SNSシェアボタンの URL だけ articles/<id>.html を指す
    → 一般ユーザーがアドレスバー経由でこの静的ページを踏むことはない

差分ビルド:
  各ファイル先頭に `<!-- content_hash: XXX -->` マーカーを埋め込み、
  articles.json 側の content_hash (既存実装済) と比較。
  ハッシュ一致なら skip、不一致なら再生成、消失した ID は削除。
  → 通常運用時は 0〜数件しか触らず、push 競合リスクを最小化する。

GitHub Actions の export_notion.py の後に実行される想定。
"""

from __future__ import annotations

import html
import json
import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

ROOT = Path(__file__).resolve().parent.parent
ARTICLES_JSON = ROOT / "data" / "articles.json"
OUTPUT_DIR = ROOT / "articles"

SITE_URL = "https://thewatcherjp.com"
SITE_NAME = "THE WATCHER"
SITE_TAGLINE = "ヒップホップ・ジャーナル"
DEFAULT_OGP_IMAGE = f"{SITE_URL}/assets/logo-mark.png"
LOGO_URL = f"{SITE_URL}/assets/logo-mark.png"
DESC_MAX_LEN = 120

# content_hash マーカー（ファイル2行目に埋め込む）
HASH_MARKER_RE = re.compile(r"<!--\s*content_hash:\s*([0-9a-f]+)\s*-->")


def _esc(value: str) -> str:
    """HTML属性値用エスケープ（&/ </ > /" /'）"""
    return html.escape(value or "", quote=True)


def _build_json_ld(article: dict) -> str:
    """記事の NewsArticle 構造化データ (JSON-LD) を生成する。"""
    aid = article["id"]
    title = article.get("title", SITE_NAME)
    full_title = f"{title} — {SITE_NAME}"
    desc = _summarize(article.get("summary", ""))
    image_url = article.get("image_url") or DEFAULT_OGP_IMAGE
    published = article.get("published_at", "")
    source_url = (article.get("source_urls") or "").split(",")[0].strip()

    ld = {
        "@context": "https://schema.org",
        "@type": "NewsArticle",
        "headline": full_title,
        "description": desc,
        "image": image_url,
        "datePublished": published,
        "dateModified": article.get("imported_at") or published,
        "author": {
            "@type": "Organization",
            "name": "THE WATCHER編集部",
            "url": f"{SITE_URL}/about.html",
        },
        "publisher": {
            "@type": "NewsMediaOrganization",
            "name": SITE_NAME,
            "url": SITE_URL,
            "logo": {
                "@type": "ImageObject",
                "url": LOGO_URL,
            },
        },
        "mainEntityOfPage": f"{SITE_URL}/articles/{aid}.html",
        "inLanguage": "ja",
    }
    if source_url:
        ld["isBasedOn"] = source_url

    return json.dumps(ld, ensure_ascii=False, separators=(",", ":"))


def _summarize(text: str, limit: int = DESC_MAX_LEN) -> str:
    """description 用に改行を半角空白に潰して指定文字数で丸める。"""
    s = (text or "").replace("\n", " ").replace("\r", " ").strip()
    # 連続空白を単一空白に
    s = re.sub(r"\s{2,}", " ", s)
    if len(s) > limit:
        s = s[: limit - 1].rstrip() + "…"
    return s


def _render_share_page(article: dict) -> str:
    """記事1件分の OGP 共有ページ HTML を生成する。"""
    aid = article["id"]
    raw_title = article.get("title", SITE_NAME)
    full_title = f"{raw_title} — {SITE_NAME}"
    desc = _summarize(article.get("summary", "")) or f"{SITE_NAME} — {SITE_TAGLINE}"
    image_url = article.get("image_url") or DEFAULT_OGP_IMAGE
    canonical_url = f"{SITE_URL}/article.html?id={aid}"
    content_hash = article.get("content_hash", "")

    # 値をすべてエスケープして埋め込み
    t = _esc(full_title)
    d = _esc(desc)
    img = _esc(image_url)
    url = _esc(canonical_url)
    noscript_url = _esc(f"/article.html?id={aid}")

    return f"""<!doctype html>
<!-- content_hash: {content_hash} -->
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{t}</title>
<meta name="description" content="{d}">
<link rel="canonical" href="{url}">
<meta name="robots" content="noindex,follow">

<!-- Open Graph Protocol -->
<meta property="og:type" content="article">
<meta property="og:site_name" content="{_esc(SITE_NAME)}">
<meta property="og:title" content="{t}">
<meta property="og:description" content="{d}">
<meta property="og:image" content="{img}">
<meta property="og:url" content="{url}">
<meta property="og:locale" content="ja_JP">

<!-- Twitter Card -->
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{t}">
<meta name="twitter:description" content="{d}">
<meta name="twitter:image" content="{img}">

<!-- 通常ユーザーは即座に本来の記事ページへ転送 (SNSクローラはここより上のOGPを読み取り済) -->
<meta http-equiv="refresh" content="0; url={noscript_url}">
<script>location.replace({json.dumps(f"/article.html?id={aid}")});</script>
<script type="application/ld+json">{_build_json_ld(article)}</script>
</head>
<body style="font-family:sans-serif;padding:2em;color:#333;">
<p>記事ページへ遷移しています…<br>
<a href="{noscript_url}">自動で遷移しない場合はこちら</a></p>
</body>
</html>
"""


def _read_existing_hash(path: Path) -> str | None:
    """既存の share page ファイルから content_hash マーカーを読み出す。"""
    try:
        # 先頭数行だけ読めば十分（マーカーは2行目）
        with path.open("r", encoding="utf-8") as f:
            head = "".join(next(f, "") for _ in range(5))
        m = HASH_MARKER_RE.search(head)
        return m.group(1) if m else None
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.warning(f"hash 読み取り失敗 {path.name}: {e}")
        return None


def _ensure_structured_data(path: Path, article: dict) -> bool:
    """既存 HTML に構造化データがなければ </head> 直前に追記する。
    既にあればスキップ (冪等性)。既存 HTML の他の部分は一切変更しない。"""
    try:
        file_html = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return False

    if "application/ld+json" in file_html:
        return False

    json_ld = _build_json_ld(article)
    script_tag = f'<script type="application/ld+json">{json_ld}</script>\n'
    new_html = file_html.replace("</head>", script_tag + "</head>", 1)
    path.write_text(new_html, encoding="utf-8")
    return True


def build() -> dict:
    """差分ビルドを実行。カウントを返す。"""
    if not ARTICLES_JSON.exists():
        logger.error(f"articles.json が見つからない: {ARTICLES_JSON}")
        sys.exit(1)

    data = json.loads(ARTICLES_JSON.read_text(encoding="utf-8"))
    articles = data.get("articles", [])
    if not articles:
        logger.warning("articles.json が空。何もしない。")
        return {"generated": 0, "skipped": 0, "deleted": 0, "enriched": 0}

    OUTPUT_DIR.mkdir(exist_ok=True)

    valid_ids: set[str] = set()
    generated = 0
    skipped = 0
    enriched = 0

    for article in articles:
        aid = article.get("id")
        if not aid:
            continue
        valid_ids.add(aid)

        current_hash = article.get("content_hash", "")
        out_path = OUTPUT_DIR / f"{aid}.html"
        existing_hash = _read_existing_hash(out_path)

        if existing_hash and current_hash and existing_hash == current_hash:
            # 既存ファイルに構造化データがなければ追記
            if _ensure_structured_data(out_path, article):
                enriched += 1
            skipped += 1
            continue

        out_path.write_text(_render_share_page(article), encoding="utf-8")
        generated += 1

    # 削除リコンサイル: articles/ 内で articles.json に存在しない id のファイルを除去
    deleted = 0
    for path in OUTPUT_DIR.glob("*.html"):
        if path.stem not in valid_ids:
            path.unlink()
            deleted += 1
            logger.info(f"  removed stale: {path.name}")

    logger.info(
        f"OGP共有ページ生成完了: generated={generated}, skipped={skipped}, "
        f"enriched={enriched}, deleted={deleted} (total={len(valid_ids)})"
    )
    return {"generated": generated, "skipped": skipped, "enriched": enriched, "deleted": deleted}


if __name__ == "__main__":
    build()
