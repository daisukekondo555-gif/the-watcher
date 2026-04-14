"""
週次まとめ記事の自動生成スクリプト。

毎週日曜 20:45 JST に起動 (weekly_summary.yml の cron トリガ) して、
直近の完了週 (月-日の7日間) のヒップホップ記事から以下3セクションを集計し、
Claude で編集部コラム風イントロを生成して Notion に保存する。

  §1 アクセスランキング TOP5        ← ranking.json の weekly
  §2 注目トピック (複数メディア掲載)  ← source_names のカンマ数 >=2
  §3 カテゴリ別ハイライト            ← 6カテゴリ × 1件

Notion に保存された記事は、直後に走る export_notion.py が articles.json に
反映するため、フロントエンドには即時公開される。

冪等性:
  合成URL https://thewatcherjp.com/weekly/<ISO週番号> を primary_url として
  Notion の _article_exists で重複判定 → 同じ週に再実行されても二重登録しない。

実行失敗時の挙動 (graceful degradation):
  - ranking.json が空/不在        → §1 スキップ
  - 注目トピック該当なし          → §2 スキップ
  - カテゴリ該当なし              → そのカテゴリ行スキップ
  - Claude イントロ生成失敗       → 固定フォールバック文言を使用
  - どのセクションも0件           → 例外的事態として exit 1
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# src/ ディレクトリ内から sibling module を取り込む
# (python src/build_weekly_summary.py で実行されると sys.path[0] = src/)
from notion_writer import save_article

import anthropic
from dateutil import parser as dateparser

JST = timezone(timedelta(hours=9))

ROOT = Path(__file__).resolve().parent.parent
ARTICLES_JSON = ROOT / "data" / "articles.json"
RANKING_JSON = ROOT / "data" / "ranking.json"

SITE_URL = "https://thewatcherjp.com"
CATEGORIES = ["ニュース", "リリース", "ビーフ", "ライブ", "チャート", "ビジネス"]

# 本文は Notion rich_text 制約 (2000字) 直下にマージン取って収める
MAX_BODY_LEN = 1950

INTRO_MODEL = "claude-sonnet-4-6"
INTRO_MAX_TOKENS = 500
INTRO_MAX_CHARS = 300

FALLBACK_INTRO = (
    "先週のヒップホップシーンでは、リリース・ライブ・アーティスト動向をめぐって"
    "さまざまな話題がファンの注目を集めた。以下、アクセスランキング TOP5、"
    "注目トピック、カテゴリ別ハイライトをまとめてお届けする。"
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


# ── 週範囲 ────────────────────────────────────────────────────────────
def compute_week_range() -> tuple[datetime, datetime]:
    """JST で直近の完了週 (月-日) を返す。
    日曜に実行 → 今日を含む週 (月-日) を対象。
    その他の曜日で実行 → 直近の完了済み週 (先週月-日) にフォールバック。"""
    now = datetime.now(JST)
    today = now.date()
    if today.weekday() == 6:  # Sunday
        sunday = today
    else:
        # 直近過去の日曜 (today-weekday-1 日前)
        sunday = today - timedelta(days=today.weekday() + 1)
    monday = sunday - timedelta(days=6)
    week_start = datetime(monday.year, monday.month, monday.day, 0, 0, 0, tzinfo=JST)
    week_end = datetime(
        sunday.year, sunday.month, sunday.day, 23, 59, 59, 999999, tzinfo=JST
    )
    return week_start, week_end


# ── JSON ロード ───────────────────────────────────────────────────────
def load_json(path: Path) -> dict:
    if not path.exists():
        logger.warning(f"{path} 不在")
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"{path} 読み込み失敗: {e}")
        return {}


def parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return dateparser.parse(s)
    except Exception:
        return None


def path_to_id(path: str) -> Optional[str]:
    """GA4 のパスから記事 id を抽出する。旧URL (?id=) / 新URL (/articles/X.html) の両対応。"""
    if not path:
        return None
    m = re.search(r"[?&]id=([^&]+)", path)
    if m:
        return m.group(1)
    m = re.search(r"/articles/([^/?.#]+)\.html", path)
    return m.group(1) if m else None


def filter_in_range(
    articles: list[dict], start: datetime, end: datetime
) -> list[dict]:
    """published_at が指定 JST 範囲内の記事だけ返す。"""
    result = []
    for a in articles:
        dt = parse_iso(a.get("published_at") or "")
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if start <= dt.astimezone(JST) <= end:
            result.append(a)
    return result


# ── 3 セクション集計 ──────────────────────────────────────────────────
def section_top5(ranking: dict, by_id: dict[str, dict]) -> list[dict]:
    """ranking.json の weekly TOP5 を articles と突合して返す。"""
    out = []
    for entry in (ranking.get("weekly") or [])[:5]:
        aid = path_to_id(entry.get("path", ""))
        if not aid:
            continue
        article = by_id.get(aid)
        if article:
            out.append(
                {
                    "article": article,
                    "pageviews": int(entry.get("pageviews", 0)),
                }
            )
    return out


def section_hot_topics(
    articles_in_range: list[dict], limit: int = 5
) -> list[dict]:
    """source_names カンマ数 >=2 の記事を、媒体数降順→公開日時降順で最大5件。"""
    out = []
    for a in articles_in_range:
        names = [
            s.strip()
            for s in (a.get("source_names") or "").split(",")
            if s.strip()
        ]
        if len(names) >= 2:
            out.append(
                {
                    "article": a,
                    "source_count": len(names),
                    "sources": names,
                }
            )
    out.sort(
        key=lambda x: (x["source_count"], x["article"].get("published_at") or ""),
        reverse=True,
    )
    return out[:limit]


def section_category_highlights(
    articles_in_range: list[dict],
    ranking: dict,
    by_id: dict[str, dict],
) -> list[dict]:
    """6カテゴリ × 1件。ranking.json の by_category[cat] を優先、
    見つからなければ期間内のそのカテゴリの最新記事にフォールバック。"""
    in_range_ids = {a.get("id") for a in articles_in_range if a.get("id")}
    out = []
    for cat in CATEGORIES:
        found: Optional[dict] = None
        for entry in (ranking.get("by_category") or {}).get(cat, []):
            aid = path_to_id(entry.get("path", ""))
            if aid and aid in in_range_ids:
                found = by_id.get(aid)
                if found:
                    break
        if not found:
            cat_articles = [
                a for a in articles_in_range if a.get("category") == cat
            ]
            cat_articles.sort(
                key=lambda a: a.get("published_at") or "", reverse=True
            )
            if cat_articles:
                found = cat_articles[0]
        if found:
            out.append({"category": cat, "article": found})
    return out


# ── Claude イントロ生成 ──────────────────────────────────────────────
def generate_intro(top5, hot_topics, category_highlights) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY 未設定 → フォールバック文言")
        return FALLBACK_INTRO

    material = []
    for t in top5[:3]:
        material.append(f"- {t['article'].get('title', '')}")
    for h in hot_topics[:3]:
        material.append(
            f"- {h['article'].get('title', '')} ({h['source_count']}媒体掲載)"
        )
    for h in category_highlights[:2]:
        material.append(f"- [{h['category']}] {h['article'].get('title', '')}")
    if not material:
        return FALLBACK_INTRO

    prompt = (
        "あなたはヒップホップ専門の日本語メディア「THE WATCHER」の編集者です。\n"
        "以下は先週のヒップホップシーンで話題になった記事のリストです:\n\n"
        + "\n".join(material)
        + "\n\n"
        "これをもとに、「先週のヒップホップシーンを振り返る」編集部コラム風の\n"
        "イントロ段落を日本語で書いてください。\n\n"
        "条件:\n"
        "- 3〜4文、合計200〜300字程度\n"
        "- 書き出しは「先週のヒップホップ界では」または類似の自然な語り出し\n"
        "- リスト中の固有名詞を1〜2個だけ具体的に触れる\n"
        "- 過度に煽らず、落ち着いた編集部トーン (である調)\n"
        "- プレーンテキストのみ。マークダウン・前置き・注釈は書かない\n"
        "- 本文のみを返す"
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=INTRO_MODEL,
            max_tokens=INTRO_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        # 想定を大きく超える長さは念のためクリップ
        return text[:INTRO_MAX_CHARS]
    except Exception as e:
        logger.warning(f"Claude イントロ生成失敗 → フォールバック: {e}")
        return FALLBACK_INTRO


# ── 本文組み立て ──────────────────────────────────────────────────────
def render_body(intro, top5, hot_topics, category_highlights) -> str:
    lines: list[str] = [intro, ""]

    if top5:
        lines.append("📊 アクセスランキング TOP5 (先週の人気記事)")
        lines.append("―――――――――――――――――――")
        for i, t in enumerate(top5, 1):
            title = (t["article"].get("title") or "")[:60]
            lines.append(f"{i}位 {title} (PV: {t['pageviews']})")
        lines.append("")

    if hot_topics:
        lines.append("🔥 注目トピック (複数メディア同時掲載)")
        lines.append("―――――――――――――――――――")
        for h in hot_topics:
            title = (h["article"].get("title") or "")[:50]
            sources = " / ".join(h["sources"])
            lines.append(
                f"・{title}  [{h['source_count']}媒体] {sources}"
            )
        lines.append("")

    if category_highlights:
        lines.append("📌 カテゴリ別ハイライト")
        lines.append("―――――――――――――――――――")
        for h in category_highlights:
            title = (h["article"].get("title") or "")[:55]
            lines.append(f"【{h['category']}】 {title}")
        lines.append("")

    lines.append("―")
    lines.append("本文中の各記事はTHE WATCHERサイト上で閲覧できます。")

    body = "\n".join(lines)
    if len(body) > MAX_BODY_LEN:
        # 想定外に長い場合はイントロを削って再構成
        overflow = len(body) - MAX_BODY_LEN
        new_intro_len = max(80, len(intro) - overflow - 10)
        lines[0] = intro[:new_intro_len] + "…"
        body = "\n".join(lines)
    return body[:MAX_BODY_LEN]


# ── エントリポイント ──────────────────────────────────────────────────
def main() -> None:
    notion_key = os.environ.get("NOTION_API_KEY", "")
    notion_db = os.environ.get("NOTION_DATABASE_ID", "")
    if not notion_key or not notion_db:
        logger.error("NOTION_API_KEY / NOTION_DATABASE_ID が未設定")
        sys.exit(1)

    week_start, week_end = compute_week_range()
    logger.info(f"対象週: {week_start.isoformat()} 〜 {week_end.isoformat()}")

    articles_data = load_json(ARTICLES_JSON)
    ranking_data = load_json(RANKING_JSON)
    all_articles = articles_data.get("articles", [])
    by_id: dict[str, dict] = {a["id"]: a for a in all_articles if a.get("id")}

    in_range = filter_in_range(all_articles, week_start, week_end)
    logger.info(
        f"期間内記事: {len(in_range)} / 全 {len(all_articles)} 件"
    )

    top5 = section_top5(ranking_data, by_id)
    hot_topics = section_hot_topics(in_range)
    cat_highlights = section_category_highlights(in_range, ranking_data, by_id)
    logger.info(
        f"TOP5={len(top5)}, 注目トピック={len(hot_topics)}, "
        f"カテゴリHL={len(cat_highlights)}"
    )

    if not (top5 or hot_topics or cat_highlights):
        logger.error("3セクションすべて空 → まとめ記事は生成しない")
        sys.exit(1)

    intro = generate_intro(top5, hot_topics, cat_highlights)
    body = render_body(intro, top5, hot_topics, cat_highlights)

    # タイトルと合成URL
    iso_year, iso_week, _ = week_start.isocalendar()
    week_tag = f"{iso_year}-W{iso_week:02d}"
    title_ja = (
        f"【今週のまとめ】"
        f"{week_start.year}/{week_start.month}/{week_start.day}〜"
        f"{week_end.month}/{week_end.day}のヒップホップ"
    )
    synthetic_url = f"{SITE_URL}/weekly/{week_tag}"

    # TOP5 1位記事の画像を流用 (無ければロゴ)
    top_image = None
    if top5:
        top_image = top5[0]["article"].get("image_url")
    if not top_image:
        top_image = f"{SITE_URL}/assets/logo-mark.png"

    weekly_article = {
        "title_ja": title_ja,
        "summary_ja": body,
        "category": "ニュース",
        "url": synthetic_url,
        "source_names": "THE WATCHER Editorial",
        "image_url": top_image,
        # 「今」を published に。articles.json は新着順なので先頭に並ぶ
        "published": datetime.now(timezone.utc).isoformat(),
        "hashtags": ["#WeeklyDigest", "#HipHopNews"],
    }

    logger.info(f"Notion保存開始: {title_ja}")
    ok = save_article(notion_key, notion_db, weekly_article)
    if ok:
        logger.info("Notion保存成功")
    else:
        # save_article は既存ありスキップ時も False を返す仕様。
        # 失敗とスキップは区別できないため、ログのみ出して継続。
        logger.info(
            "Notion保存スキップまたは失敗 (同週の重複登録防止による skip の可能性大)"
        )

    logger.info("build_weekly_summary 完了")


if __name__ == "__main__":
    main()
