"""
Translation, categorisation & SNS summary via Claude API.

Uses prompt caching on the system prompt to reduce cost when
processing many articles per run.

Supports:
  - name_mapping dict for accurate proper noun transliteration
  - x_post / threads_post SNS summary generation (integrated into single API call)
  - off_topic detection for non-hip-hop articles
"""

import json
import logging
import re
import time
from typing import Optional
from urllib.parse import quote

import anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 2560
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # seconds

SITE_URL = "https://thewatcherjp.com"

# --- System prompt (template) ------------------------------------------------
# {name_dict_block} はランタイムで固有名詞辞書テーブルに置換される。
SYSTEM_PROMPT_TEMPLATE = """あなたはヒップホップ専門の日本語メディア「THE WATCHER」の編集者です。
英語のヒップホップニュース記事を受け取り、日本語に翻訳してカテゴリを判定し、ハッシュタグを生成し、SNS投稿用の要約も生成します。

## 関連性の判断（最初に実施）
記事がヒップホップ・音楽・エンターテインメントに関連しているか判断してください。
- **関連あり**: ヒップホップアーティスト / ラッパー / プロデューサー / DJ の活動、音楽業界の動向、コンサート・フェス、ストリーミング・チャート、ビーフ、ファッション・ブランドコラボなど
- **関連あり（例外）**: 記事の主題自体は政治・社会・スポーツ等でも、ヒップホップアーティストや音楽業界に具体的に言及している場合は関連ありと判断する
- **関連なし**: 上記に該当しない一般ニュース、政治、スポーツ、犯罪、テクノロジー等
関連なしと判断した場合は `"off_topic": true` を含む JSON を返してください（他フィールドは空文字でよい）。

## 出力形式
必ず以下のJSONのみを返してください。余分なテキスト・Markdownコードブロックは不要です。
**インデントや改行を入れず、1行のcompact JSONで返すこと。**

{{"title_ja":"日本語タイトル（原題のニュアンスを保ちつつ自然な日本語で、50字以内）","summary_ja":"元記事の情報をできる限り100%に近い形で日本語化する。省略・要約はしない。段落ごとに改行(\\n)を入れる。","category":"カテゴリ名","hashtags":["#TagOne","#TagTwo","#TagThree"],"off_topic":false,"x_post":"X投稿用の要約。記事の核心を1-2文で伝える。140字以内。URLやハッシュタグは含めない。読者が反応したくなる書き方。","threads_post":"Threads投稿用の要約。記事の要点を3-4文で伝える。400字程度。URLやハッシュタグは含めない。"}}

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

## summary_ja の記述ルール
1. **段落改行**: 話題の区切りごとに改行コード(\\n)を入れて段落分けする。
   1段落は2〜4文を目安に、出来事・引用・背景・反応などの切り替わりで改行する。
2. **アーティスト名の英語併記**:
   - summary_ja 内で各アーティスト名が**初出の1回目のみ**、
     カタカナ表記の直後に半角括弧で英語名を併記する。
     例: 「ケンドリック・ラマー（Kendrick Lamar）が新EPを発表」
   - 同じアーティストが2回目以降に登場する場合はカタカナ表記のみ。
   - 英語表記が定着している名前（YG, 6ix9ine, JID 等）は英語のまま使い、括弧併記は不要。
   - title_ja / x_post / threads_post には英語併記を入れない。

{name_dict_block}

## 注意
- アーティスト名・地名・ブランド名はカタカナ表記を優先する
- 日本語として不自然な直訳は避け、意訳を心がける
- 渡された本文に含まれていない情報について「不明」「情報は限られている」「詳細はない」等の推測・補足は一切書かない。渡されたテキストの範囲内のみを日本語化すること
- 「この記事はヒップホップとは関連がない」等の編集者コメントは summary_ja に書かない
- JSONのみ返す（説明文・前置き・コードブロック不要）"""


# --- Name mapping helpers ----------------------------------------------------

def _build_name_dict_block(name_mapping: dict) -> str:
    """name_mapping dict からプロンプト埋め込み用テキストブロックを生成する。"""
    if not name_mapping:
        return ""

    lines = ["## 固有名詞のカタカナ表記（必ずこの表記に従うこと）"]

    # English → Japanese mapping entries
    mapping = {k: v for k, v in name_mapping.items() if k != "__keep_english"}
    if mapping:
        lines.append("以下のアーティスト名は指定されたカタカナ表記を使用してください:")
        for en, ja in sorted(mapping.items()):
            lines.append(f"- {en} → {ja}")

    # Keep-as-English entries
    keep = name_mapping.get("__keep_english", [])
    if keep:
        lines.append("")
        lines.append("以下の名前は英語表記のまま使い、カタカナ化しないでください:")
        lines.append("- " + ", ".join(sorted(keep)))

    return "\n".join(lines)


def _apply_name_replacements(text: str, name_mapping: dict) -> str:
    """翻訳結果のカタカナ表記を辞書で後置換する安全ネット。
    Claude が辞書を見落とした場合の補正用。よくある誤表記パターンを修正。"""
    if not text or not name_mapping:
        return text

    # よくある誤変換パターン → 正しい表記 の追加マッピング
    corrections = {
        "カムロン": "キャムロン",
        "メークミル": "ミーク・ミル",
        "メーク・ミル": "ミーク・ミル",
        "メック・ミル": "ミーク・ミル",
        "メーガン・ジー・スタリオン": "メーガン・ザ・スタリオン",
        "メガン・ザ・スタリオン": "メーガン・ザ・スタリオン",
        "メガン・ジー・スタリオン": "メーガン・ザ・スタリオン",
        "エーサップ・ロッキー": "エイサップ・ロッキー",
        "タイダラーサイン": "タイ・ダラー・サイン",
        "リル・ウジ・バート": "リル・ウージー・ヴァート",
        "リル・ウジ・ヴァート": "リル・ウージー・ヴァート",
        "プレイボイ・カルティ": "プレイボーイ・カルティ",
        "ニッキ・ミナージュ": "ニッキー・ミナージュ",
    }

    for wrong, right in corrections.items():
        text = text.replace(wrong, right)

    return text


# --- Core functions ----------------------------------------------------------

def _normalise_hashtags(raw_tags) -> list[str]:
    if not isinstance(raw_tags, list):
        return []
    seen: set[str] = set()
    result: list[str] = []
    for tag in raw_tags:
        tag = str(tag).strip()
        tag = "#" + tag.lstrip("#").replace(" ", "")
        if len(tag) <= 1:
            continue
        lower = tag.lower()
        if lower not in seen:
            seen.add(lower)
            result.append(tag)
        if len(result) == 5:
            break
    return result


def _parse_json_response(raw: str) -> Optional[dict]:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(inner).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return None


def _translate_one(
    article: dict,
    client: anthropic.Anthropic,
    system_prompt: str,
    name_mapping: dict,
) -> dict:
    title = article["title"]
    content = article.get("content", "")

    user_message = (
        f"タイトル: {title}\n\n"
        f"本文:\n{content[:6000] if content else '（本文なし）'}"
    )

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=[
                    {
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_message}],
            )

            raw = response.content[0].text
            parsed = _parse_json_response(raw)

            if parsed:
                hashtags = _normalise_hashtags(parsed.get("hashtags", []))

                # 固有名詞の後置換 (安全ネット)
                title_ja = _apply_name_replacements(
                    str(parsed.get("title_ja", title))[:200], name_mapping
                )
                summary_ja = _apply_name_replacements(
                    str(parsed.get("summary_ja", ""))[:2000], name_mapping
                )
                x_post = _apply_name_replacements(
                    str(parsed.get("x_post", ""))[:140], name_mapping
                )
                threads_post = _apply_name_replacements(
                    str(parsed.get("threads_post", ""))[:500], name_mapping
                )

                result = {
                    **article,
                    "title_ja": title_ja,
                    "summary_ja": summary_ja,
                    "category": str(parsed.get("category", "ニュース")),
                    "hashtags": hashtags,
                    "x_post": x_post,
                    "threads_post": threads_post,
                }
                if parsed.get("off_topic"):
                    result["off_topic"] = True
                return result
            else:
                logger.warning(
                    f"[attempt {attempt}] JSON parse failed for '{title}'. "
                    f"Raw: {raw[:200]}"
                )

        except anthropic.RateLimitError:
            wait = RETRY_DELAY * attempt
            logger.warning(
                f"Rate limited. Waiting {wait}s before retry "
                f"{attempt}/{RETRY_ATTEMPTS}…"
            )
            time.sleep(wait)
        except anthropic.APIError as e:
            logger.error(
                f"[attempt {attempt}] Claude API error for '{title}': {e}"
            )
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)

    logger.error(
        f"All attempts failed for '{title}'. Marking as translation_failed."
    )
    return {
        **article,
        "title_ja": title,
        "summary_ja": content[:300] if content else "",
        "category": "ニュース",
        "hashtags": [],
        "x_post": "",
        "threads_post": "",
        "translation_failed": True,
    }


def process_articles(
    articles: list[dict],
    api_key: str,
    name_mapping: Optional[dict] = None,
) -> list[dict]:
    """Translate, categorise, and generate SNS summaries for all articles."""
    client = anthropic.Anthropic(api_key=api_key)
    name_mapping = name_mapping or {}

    # Build system prompt with name dictionary embedded (cached across calls)
    name_block = _build_name_dict_block(name_mapping)
    system_prompt = SYSTEM_PROMPT_TEMPLATE.replace("{name_dict_block}", name_block)

    results = []
    for i, article in enumerate(articles, 1):
        logger.info(f"Translating [{i}/{len(articles)}]: {article['title'][:80]}")
        enriched = _translate_one(article, client, system_prompt, name_mapping)
        results.append(enriched)
        if i < len(articles):
            time.sleep(0.5)

    return results
