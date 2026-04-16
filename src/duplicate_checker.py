"""
Duplicate detection using difflib.SequenceMatcher.

Rule: two articles are duplicates when BOTH conditions hold:
  1. Their normalised titles share at least one "artist token"
     (a capitalised word of 2+ chars)
  2. Title similarity >= threshold (default 0.80)

The first-seen article is kept; subsequent duplicates contribute
their source URL and site name (both appended comma-separated).

`url` and `source_names` are always kept in sync as parallel
comma-separated lists so the frontend can zip them together:
  url          = "https://hiphopdx.com/...,https://xxlmag.com/..."
  source_names = "HipHopDX,XXL"
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Optional

from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

# Words that look like common English words / stop words — not artist names
_STOPWORDS = {
    "the", "a", "an", "in", "on", "at", "to", "for", "of", "and", "or",
    "is", "was", "are", "has", "have", "had", "will", "with", "his", "her",
    "their", "he", "she", "they", "it", "its", "this", "that", "from",
    "new", "says", "about", "after", "over", "into", "up", "out", "off",
    "how", "why", "when", "what", "who", "not", "but", "by", "be", "been",
    "album", "single", "song", "track", "music", "rapper", "artist",
    "releases", "drops", "debut", "shares", "reveals", "announces",
}

# クロスラン重複判定で除外する追加 stopwords (汎用名詞・常出ハッシュタグ)
_HISTORY_STOPWORDS = _STOPWORDS | {
    "rap", "hop", "hip", "news", "video", "live", "show", "year", "day",
    "back", "make", "made", "first", "last", "feat", "feature", "ft",
    "hiphopnews", "hiphop",
}


def _normalise(title: str) -> str:
    title = title.lower()
    title = re.sub(r"[^\w\s]", " ", title)
    return re.sub(r"\s+", " ", title).strip()


def _artist_tokens(title: str) -> set[str]:
    """
    Return capitalised words from the ORIGINAL title as candidate artist names.
    Skips stop words, numbers, and single-char tokens.
    """
    tokens: set[str] = set()
    for word in title.split():
        clean = re.sub(r"[^\w]", "", word)
        if (
            len(clean) >= 2
            and word[0].isupper()
            and clean.lower() not in _STOPWORDS
            and not clean.isdigit()
        ):
            tokens.add(clean.lower())
    return tokens


def _similarity(t1: str, t2: str) -> float:
    return SequenceMatcher(None, _normalise(t1), _normalise(t2)).ratio()


def _shares_artist(t1: str, t2: str) -> bool:
    return bool(_artist_tokens(t1) & _artist_tokens(t2))


def _is_duplicate(a: dict, b: dict, threshold: float) -> bool:
    if _similarity(a["title"], b["title"]) < threshold:
        return False
    return _shares_artist(a["title"], b["title"])


def deduplicate(articles: list[dict], threshold: float = 0.8) -> list[dict]:
    """
    Remove duplicates. Keeps the first (newest) article.

    For each surviving article, `url` and `source_names` are kept as
    parallel comma-separated lists so URL[i] belongs to source_names[i].

    Example after merge:
      url          = "https://hiphopdx.com/...,https://xxlmag.com/..."
      source_names = "HipHopDX,XXL"
    """
    unique: list[dict] = []

    for article in articles:
        # Initialise source_names from source_name if not already set
        if "source_names" not in article:
            article = dict(article)
            article["source_names"] = article.get("source_name", "")

        merged = False
        for existing in unique:
            if _is_duplicate(existing, article, threshold):
                # Append URL if not already present
                existing_urls = [u.strip() for u in existing["url"].split(",")]
                new_url = article["url"].strip()
                if new_url not in existing_urls:
                    existing["url"] += "," + new_url
                    # Keep source_names in sync with url
                    existing["source_names"] += "," + article.get("source_names", article.get("source_name", ""))

                logger.info(
                    f"Duplicate merged:\n"
                    f"  keep → {existing['title'][:80]}\n"
                    f"  skip → {article['title'][:80]}"
                )
                merged = True
                break

        if not merged:
            entry = dict(article)
            if "source_names" not in entry:
                entry["source_names"] = entry.get("source_name", "")
            unique.append(entry)

    logger.info(f"Deduplication: {len(articles)} → {len(unique)} articles")
    return unique


# ─────────────────────────────────────────────────────────────────────
# クロスラン重複検出 (articles.json 履歴との照合)
# ─────────────────────────────────────────────────────────────────────
# 同一出来事を別ソースが時間差で報じたケースを検出する。
# 旧 _is_duplicate は同一 run 内のみが対象 (タイトル類似度) なので、
# 過去 cron run で既に Notion / articles.json に保存された記事との重複は
# 検出できなかった。ここで補完する。
#
# 言語: 新規記事は英語(RSS)、過去記事は日本語(タイトル/要約)+英語(ハッシュタグ)
# が混在する。直接の文字列類似度はクロス言語で機能しないため、
# **英語固有名詞トークンの集合論的一致** で判定する。
#
# 過去記事側の英語シグナル取得元:
#   1. ハッシュタグ (#KendrickLamar など、CamelCase 分解で {kendrick, lamar})
#   2. summary 内の "カタカナ（English）" 併記の英語部分
#      (translator.py のプロンプトで初出時の英語併記が義務化済)


def _significant_tokens_from_title(title: str) -> set[str]:
    """英語タイトルから "大文字始まり ≥3字 非ストップワード" を抽出。
    タイトルケース表記の RSS フィードでは固有名詞を効率的に拾える。"""
    tokens: set[str] = set()
    for word in (title or "").split():
        if not word or not word[0].isupper():
            continue
        clean = re.sub(r"[^\w]", "", word).lower()
        if (
            len(clean) >= 3
            and clean not in _HISTORY_STOPWORDS
            and not clean.isdigit()
        ):
            tokens.add(clean)
    return tokens


def _decompose_camel(s: str) -> list[str]:
    """KendrickLamar → ['Kendrick', 'Lamar'], BIGSEAN → ['BIGSEAN'],
    GRAMMYs2026 → ['GRAMMY', 's']"""
    return re.findall(r"[A-Z][a-z]+|[A-Z]+(?=[A-Z]|$)|[a-z]+", s)


def _signature_from_past(past: dict) -> set[str]:
    """過去記事から英語シグナルトークン集合を抽出。
    ハッシュタグ + summary 内の (English) 併記部分を対象。"""
    sig: set[str] = set()

    # ハッシュタグ (already English mostly): "#KendrickLamar #NotLikeUs"
    for tag in (past.get("hashtags") or "").split():
        clean = tag.lstrip("#").strip()
        for part in _decompose_camel(clean):
            p = part.lower()
            if (
                len(p) >= 3
                and p not in _HISTORY_STOPWORDS
                and not p.isdigit()
            ):
                sig.add(p)

    # summary の括弧内英語: 「ケンドリック・ラマー（Kendrick Lamar）」→ {kendrick, lamar}
    # 全角(）と半角() 両対応
    summary = past.get("summary") or ""
    for paren in re.findall(r"[（(]([A-Za-z][\w\s\-\.&']{1,60}?)[）)]", summary):
        for word in paren.split():
            clean = re.sub(r"[^\w]", "", word).lower()
            if (
                len(clean) >= 3
                and clean not in _HISTORY_STOPWORDS
                and not clean.isdigit()
            ):
                sig.add(clean)

    return sig


def _is_recent(published_at: str, cutoff: datetime) -> bool:
    if not published_at:
        return False
    try:
        dt = dateparser.parse(published_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= cutoff
    except Exception:
        return False


def filter_against_history(
    articles: list[dict],
    past_articles: list[dict],
    recency_days: int = 7,
    min_shared_tokens: int = 2,
) -> list[dict]:
    """新規 RSS 記事を articles.json の過去記事と照合し、
    同一出来事を報じた重複と判定されたものを除外する。

    判定:
      - 新規記事タイトルから固有名詞トークン群を抽出
      - 過去記事 (公開日時 が `recency_days` 日以内) のシグナルトークンと比較
      - 共通トークン数 >= `min_shared_tokens` で重複扱い

    引数:
      articles:      これから翻訳・保存しようとしている新規記事
      past_articles: 既に articles.json に存在する記事 (履歴)
      recency_days:  過去何日まで遡って比較するか (default 7)
      min_shared_tokens: 重複判定の閾値 (default 2)

    戻り値: 重複と判定されなかった記事のリスト
    """
    if not past_articles:
        return articles

    cutoff = datetime.now(timezone.utc) - timedelta(days=recency_days)

    # 過去記事のシグナルを事前計算 (新規記事数 × 過去記事数 のループのため)
    past_sigs: list[tuple[dict, set[str]]] = []
    for p in past_articles:
        if not _is_recent(p.get("published_at") or "", cutoff):
            continue
        sig = _signature_from_past(p)
        if sig:
            past_sigs.append((p, sig))

    if not past_sigs:
        return articles

    survivors: list[dict] = []
    skipped = 0
    for new in articles:
        new_tokens = _significant_tokens_from_title(new.get("title", ""))
        if len(new_tokens) < min_shared_tokens:
            # 判別材料が乏しいので除外せず通す (過剰除外を避ける)
            survivors.append(new)
            continue

        match = None
        for past, past_sig in past_sigs:
            overlap = new_tokens & past_sig
            if len(overlap) >= min_shared_tokens:
                match = (past, overlap)
                break

        if match:
            past, overlap = match
            logger.info(
                f"Cross-run dup skipped:\n"
                f"  new  → {new.get('title', '')[:80]}\n"
                f"  past → {past.get('title', '')[:80]}\n"
                f"  共通トークン: {sorted(overlap)}"
            )
            skipped += 1
        else:
            survivors.append(new)

    logger.info(
        f"Cross-run dedup: {len(articles)} → {len(survivors)} "
        f"(履歴重複を {skipped} 件除外、過去比較対象 {len(past_sigs)} 件)"
    )
    return survivors
