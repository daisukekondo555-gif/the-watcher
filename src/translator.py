"""
Translation & categorisation via Claude API.

Uses prompt caching on the system prompt to reduce cost when
processing many articles per run.
"""

import json
import logging
import time
from typing import Optional

import anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 4096
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # seconds

SYSTEM_PROMPT = """あなたはヒップホップ専門の日本語メディア「THE WATCHER」の編集者です。
英語のヒップホップニュース記事を受け取り、日本語に翻訳してカテゴリを判定し、ハッシュタグを生成します。

## 出力形式
必ず以下のJSONのみを返してください。余分なテキスト・Markdownコードブロックは不要です。

{
  "title_ja": "日本語タイトル（原題のニュアンスを保ちつつ自然な日本語で、50字以内）",
  "summary_ja": "元記事の情報をできる限り100%に近い形で日本語化する。元記事の文章をそのまま使うのではなく、内容・事実・発言・背景・文脈を全て自分の言葉で日本語に置き換える。省略・要約はしない。ヒップホップファンが元記事を読まなくても同等の情報が得られるレベルを目指す。",
  "category": "カテゴリ名",
  "hashtags": ["#TagOne", "#TagTwo", "#TagThree"]
}

## カテゴリ一覧と判定基準
- ニュース    : 訃報・逮捕・声明・一般的な話題
- リリース   : 新曲・EP・アルバム・MV・ビジュアルのリリース情報
- ビーフ     : アーティスト間のディス・対立・口論
- インタビュー: アーティストへのインタビュー・独占取材
- ライブ     : コンサート・フェス・ツアー・パフォーマンス
- ビジネス   : レーベル契約・企業買収・ブランド提携・金融
- チャート   : Billboard・Spotifyなどのチャート実績・セールス・ストリーミング数

## ハッシュタグ生成ルール
- 必ず3〜5個生成し、配列で返す
- 各タグは # で始め、スペースなしのキャメルケース
- 優先順位: ① 登場するアーティスト名 → ② 固有のイベント・作品名 → ③ トピック・テーマ
- アーティスト名は英語表記を使う（例: #KendrickLamar, #DrakeVsKendrick）
- 汎用すぎるタグは避ける（#Music, #Rap, #HipHop 単体はNG。#HipHopNews はOK）
- 良い例: ["#KendrickLamar", "#GRAMMYs", "#NotLikeUs", "#WestCoast", "#BeefSeason"]
- 良い例: ["#DJKhaled", "#Future", "#LilBaby", "#OneOfThem", "#MiamiBass"]

## 注意
- アーティスト名・地名・ブランド名はカタカナ表記を優先する（summary_ja / title_ja 内のみ）
- 日本語として不自然な直訳は避け、意訳を心がける
- JSONのみ返す（説明文・前置き・コードブロック不要）"""


def _normalise_hashtags(raw_tags) -> list[str]:
    """
    Sanitise hashtags from Claude's response.
    - Ensures each tag starts with exactly one #
    - Removes spaces inside tags
    - Deduplicates while preserving order
    - Returns 3–5 tags max
    """
    if not isinstance(raw_tags, list):
        return []

    seen: set[str] = set()
    result: list[str] = []
    for tag in raw_tags:
        tag = str(tag).strip()
        # Strip leading #s then re-add one
        tag = "#" + tag.lstrip("#").replace(" ", "")
        if len(tag) <= 1:          # empty after stripping
            continue
        lower = tag.lower()
        if lower not in seen:
            seen.add(lower)
            result.append(tag)
        if len(result) == 5:
            break

    return result


def _parse_json_response(raw: str) -> Optional[dict]:
    """Extract and parse JSON from Claude's response, handling code fences."""
    text = raw.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last fence lines
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(inner).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to extract a JSON object with regex as last resort
        import re
        match = re.search(r"\{[^{}]+\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return None


def _translate_one(article: dict, client: anthropic.Anthropic) -> dict:
    """
    Call Claude to translate, summarise, and categorise a single article.
    Returns the original dict enriched with title_ja, summary_ja, category.
    """
    title = article["title"]
    content = article.get("content", "")

    user_message = (
        f"タイトル: {title}\n\n"
        f"本文:\n{content[:5000] if content else '（本文なし）'}"
    )

    last_error: Optional[Exception] = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=[
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        # Prompt caching: system prompt is identical for every call,
                        # so Anthropic caches it after the first request in this run.
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_message}],
            )

            raw = response.content[0].text
            parsed = _parse_json_response(raw)

            if parsed:
                hashtags = _normalise_hashtags(parsed.get("hashtags", []))
                return {
                    **article,
                    "title_ja": str(parsed.get("title_ja", title))[:200],
                    "summary_ja": str(parsed.get("summary_ja", ""))[:2000],
                    "category": str(parsed.get("category", "ニュース")),
                    "hashtags": hashtags,
                }
            else:
                logger.warning(f"[attempt {attempt}] JSON parse failed for '{title}'. Raw: {raw[:200]}")

        except anthropic.RateLimitError:
            wait = RETRY_DELAY * attempt
            logger.warning(f"Rate limited. Waiting {wait}s before retry {attempt}/{RETRY_ATTEMPTS}…")
            time.sleep(wait)
            last_error = None  # will retry
        except anthropic.APIError as e:
            logger.error(f"[attempt {attempt}] Claude API error for '{title}': {e}")
            last_error = e
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)

    # Fallback: return with original title and raw content snippet
    logger.error(f"All attempts failed for '{title}'. Using fallback values.")
    return {
        **article,
        "title_ja": title,
        "summary_ja": content[:300] if content else "",
        "category": "ニュース",
        "hashtags": [],
    }


def process_articles(articles: list[dict], api_key: str) -> list[dict]:
    """Translate and categorise all articles. Returns enriched list."""
    client = anthropic.Anthropic(api_key=api_key)
    results = []

    for i, article in enumerate(articles, 1):
        logger.info(f"Translating [{i}/{len(articles)}]: {article['title'][:80]}")
        enriched = _translate_one(article, client)
        results.append(enriched)
        # Small courtesy delay to stay well within rate limits
        if i < len(articles):
            time.sleep(0.5)

    return results
