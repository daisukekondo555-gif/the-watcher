"""
Phase 1: 手動 URL 投入版 — 動画から日本語記事下書きを生成する。

Usage:
    python tools/video_to_article.py <youtube_url>

フロー:
    1. yt-dlp で動画ダウンロード
    2. ffmpeg でフレーム抽出（字幕領域を crop）
    3. ローカル OCR で焼き込み字幕を読み取り
    4. 重複除去・クリーンアップ
    5. 日本語翻訳（Haiku 4.5）
    6. Notion に下書き保存
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.ocr_engine import extract_text

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "video_channels.json"
NAME_MAPPING_PATH = ROOT / "data" / "name_mapping.json"

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _load_name_mapping() -> dict:
    try:
        return json.loads(NAME_MAPPING_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _download_video(url: str, output_dir: str) -> str:
    """動画をダウンロードする。pytubefix → yt-dlp のフォールバック。"""
    output_path = os.path.join(output_dir, "video.mp4")

    try:
        from pytubefix import YouTube
        yt = YouTube(url)
        stream = yt.streams.filter(progressive=True, file_extension="mp4").order_by("resolution").first()
        if stream:
            logger.info(f"Downloading via pytubefix: {url}")
            stream.download(output_path=output_dir, filename="video.mp4")
            if os.path.exists(output_path):
                logger.info(f"Downloaded: {output_path}")
                return output_path
    except Exception as e:
        logger.warning(f"pytubefix failed, trying yt-dlp: {e}")

    cmd = [
        "yt-dlp",
        "-f", "worst[ext=mp4]/worst",
        "--no-playlist",
        "-o", output_path,
        url,
    ]
    logger.info(f"Downloading via yt-dlp: {url}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"yt-dlp stderr: {result.stderr[:500]}")
        raise RuntimeError(f"Download failed for {url}")
    if not os.path.exists(output_path):
        mp4s = list(Path(output_dir).glob("video.*"))
        if mp4s:
            output_path = str(mp4s[0])
    logger.info(f"Downloaded: {output_path}")
    return output_path


def _get_video_duration(video_path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", video_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return float(result.stdout.strip())


def _extract_frames(video_path: str, output_dir: str, interval: float, crop_ratio: float) -> list[str]:
    """ffmpeg でフレーム抽出（字幕領域のみ crop）。"""
    frames_dir = os.path.join(output_dir, "frames")
    os.makedirs(frames_dir, exist_ok=True)

    cmd_probe = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=height",
        "-of", "default=noprint_wrappers=1:nokey=1", video_path,
    ]
    result = subprocess.run(cmd_probe, capture_output=True, text=True)
    height = int(result.stdout.strip())
    crop_h = int(height * crop_ratio)
    crop_y = height - crop_h

    cmd = [
        "ffmpeg", "-i", video_path,
        "-vf", f"fps=1/{interval},crop=in_w:{crop_h}:0:{crop_y}",
        "-q:v", "2",
        os.path.join(frames_dir, "frame_%04d.jpg"),
        "-y",
    ]
    logger.info(f"Extracting frames (interval={interval}s, crop bottom {int(crop_ratio*100)}%)")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"ffmpeg returncode: {result.returncode}")
        logger.warning(f"ffmpeg stderr: {result.stderr[:500]}")
        raise RuntimeError(f"ffmpeg failed: exit {result.returncode}")

    frames = sorted(Path(frames_dir).glob("frame_*.jpg"))
    logger.info(f"Extracted {len(frames)} frames")
    return [str(f) for f in frames]


def _ocr_frames(frames: list[str], engine: str) -> list[str]:
    """全フレームを OCR し、テキストを返す。"""
    texts = []
    for f in frames:
        text = extract_text(f, engine=engine)
        if text:
            texts.append(text)
    logger.info(f"OCR completed: {len(texts)}/{len(frames)} frames with text")
    return texts


def _deduplicate_lines(texts: list[str]) -> list[str]:
    """重複行を除去し、字幕の時系列順序を維持する。"""
    seen = set()
    result = []
    for text in texts:
        for line in text.split("\n"):
            cleaned = re.sub(r"\s+", " ", line).strip()
            if not cleaned or len(cleaned) < 3:
                continue
            key = cleaned.lower()
            if key not in seen:
                seen.add(key)
                result.append(cleaned)
    return result


def _translate_to_japanese(lines: list[str], api_key: str, name_mapping: dict) -> str:
    """英語字幕を日本語に翻訳する（Haiku 4.5）。"""
    import anthropic

    if not lines:
        return ""

    transcript = "\n".join(lines)

    name_dict = ""
    mappings = {k: v for k, v in name_mapping.items() if not k.startswith("__")}
    keep = name_mapping.get("__keep_english", [])
    if mappings:
        name_dict = "\n## 固有名詞辞書\n" + "\n".join(f"- {k} → {v}" for k, v in mappings.items())
    if keep:
        name_dict += "\n- 以下はそのまま英語表記: " + ", ".join(keep)

    system_prompt = f"""あなたはヒップホップ専門の日本語メディア「THE WATCHER」の編集者です。
以下の英語字幕（ヒップホップ系 Podcast / インタビューの焼き込み字幕から OCR で取得）を、
日本語記事の本文として読めるように翻訳・編集してください。

## ルール
- 会話形式を維持（話者の区切りが推測できる場合は「——」で区切る）
- ヒップホップスラング・俗語は意味が通じる日本語に翻訳（直訳しすぎない）
- 皮肉やジョーク、ダブルミーニングはニュアンスを残す
- OCR 誤認識で意味が通じない部分は [...] で省略
- 段落ごとに改行を入れる
- 出力は日本語本文のみ（メタ情報・説明は不要）
{name_dict}"""

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        system=system_prompt,
        messages=[{"role": "user", "content": f"以下の英語字幕を日本語記事本文に翻訳してください:\n\n{transcript}"}],
    )
    return resp.content[0].text.strip()


def _generate_title(japanese_text: str, api_key: str) -> str:
    """日本語本文からタイトルを生成する。"""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=100,
        messages=[{
            "role": "user",
            "content": f"以下のヒップホップ系インタビュー/Podcast記事の本文から、50字以内の日本語タイトルを1つだけ生成してください。タイトルのみ返してください:\n\n{japanese_text[:1000]}",
        }],
    )
    return resp.content[0].text.strip()


def _save_to_notion(
    api_key: str, db_id: str,
    title: str, body: str, video_url: str, channel_name: str,
) -> str:
    """Notion に記事下書きを保存する。"""
    import requests

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }

    properties = {
        "名前": {"title": [{"text": {"content": title[:2000]}}]},
        "本文": {"rich_text": [{"text": {"content": body[:2000]}}]},
        "動画URL": {"url": video_url},
        "チャンネル名": {"rich_text": [{"text": {"content": channel_name}}]},
        "カテゴリ": {"select": {"name": "INTERVIEWS"}},
        "ステータス": {"select": {"name": "下書き"}},
        "ソース": {"select": {"name": "手動追加"}},
    }

    resp = requests.post(
        f"{NOTION_BASE}/pages",
        headers=headers,
        json={"parent": {"database_id": db_id}, "properties": properties},
        timeout=30,
    )
    resp.raise_for_status()
    page_id = resp.json()["id"]
    logger.info(f"Saved to Notion: {page_id}")
    return page_id


def _get_video_info(url: str) -> dict:
    """yt-dlp で動画メタデータを取得する。"""
    cmd = ["yt-dlp", "--dump-json", "--no-playlist", url]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(f"yt-dlp info returncode: {result.returncode}")
        logger.warning(f"yt-dlp stdout: {result.stdout[:500]}")
        logger.warning(f"yt-dlp stderr: {result.stderr[:500]}")
        raise RuntimeError(f"yt-dlp info failed: exit {result.returncode}")
    return json.loads(result.stdout)


def process_video(video_url: str) -> None:
    """1本の動画を処理して Notion 下書きを生成する。"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    notion_key = os.environ.get("NOTION_API_KEY", "")
    notion_db = os.environ.get("NOTION_VIDEO_DB_ID", "")

    if not api_key or not notion_key or not notion_db:
        logger.error("ANTHROPIC_API_KEY / NOTION_API_KEY / NOTION_DATABASE_ID not set")
        sys.exit(1)

    config = _load_config()
    ocr_engine = config.get("ocr_engine", "tesseract")
    interval = config.get("frame_interval_sec", 0.5)
    crop_ratio = config.get("subtitle_crop_ratio", 0.35)
    max_duration = config.get("max_video_duration_sec", 180)
    name_mapping = _load_name_mapping()

    channel_name = "Unknown"
    try:
        from pytubefix import YouTube
        yt = YouTube(video_url)
        channel_name = yt.author or "Unknown"
    except Exception:
        pass

    with tempfile.TemporaryDirectory() as tmpdir:
        video_path = _download_video(video_url, tmpdir)

        duration = 0
        try:
            dur_cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                       "-of", "default=noprint_wrappers=1:nokey=1", video_path]
            dur_result = subprocess.run(dur_cmd, capture_output=True, text=True)
            if dur_result.returncode == 0:
                duration = float(dur_result.stdout.strip())
        except Exception:
            pass

        logger.info(f"Video: {duration:.0f}s by {channel_name}")

        if duration > max_duration:
            logger.warning(f"Video too long ({duration:.0f}s > {max_duration}s), skipping")
            return
        frames = _extract_frames(video_path, tmpdir, interval, crop_ratio)

        if not frames:
            logger.warning("No frames extracted")
            return

        texts = _ocr_frames(frames, ocr_engine)
        lines = _deduplicate_lines(texts)

        if not lines:
            logger.warning("No subtitle text found")
            return

        logger.info(f"Subtitle lines: {len(lines)}")
        logger.info(f"Sample: {lines[0][:80]}")

        japanese = _translate_to_japanese(lines, api_key, name_mapping)
        if not japanese:
            logger.warning("Translation returned empty")
            return

        title = _generate_title(japanese, api_key)
        logger.info(f"Generated title: {title}")

        _save_to_notion(notion_key, notion_db, title, japanese, video_url, channel_name)
        logger.info("Done!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/video_to_article.py <youtube_url>")
        sys.exit(1)
    process_video(sys.argv[1])
