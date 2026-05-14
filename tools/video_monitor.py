"""
Phase 2: 半自動チャンネル監視 — 新着 Shorts から字幕付き動画を自動検出し、
字幕判定 AI でフィルターした動画のみ OCR → 翻訳 → Notion 下書き保存する。

GitHub Actions で 1日1回実行。
"""

from __future__ import annotations

import base64
import json
import logging
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.ocr_engine import extract_text
from tools.video_to_article import (
    _deduplicate_lines,
    _download_video,
    _extract_frames,
    _generate_title,
    _get_video_info,
    _ocr_frames,
    _save_to_notion,
    _translate_to_japanese,
)

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
PROCESSED_PATH = ROOT / "data" / "video_processed.json"

YT_RSS_BASE = "https://www.youtube.com/feeds/videos.xml?channel_id="


def _load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _load_name_mapping() -> dict:
    try:
        return json.loads(NAME_MAPPING_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_processed() -> set[str]:
    try:
        return set(json.loads(PROCESSED_PATH.read_text(encoding="utf-8")))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def _save_processed(ids: set[str]) -> None:
    PROCESSED_PATH.parent.mkdir(exist_ok=True)
    PROCESSED_PATH.write_text(
        json.dumps(sorted(ids), ensure_ascii=False), encoding="utf-8"
    )


def _fetch_recent_videos(channel_id: str, max_age_hours: int = 48) -> list[dict]:
    """YouTube RSS から最新動画を取得する。"""
    import requests

    url = YT_RSS_BASE + channel_id
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"RSS fetch failed for {channel_id}: {e}")
        return []

    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }

    root = ET.fromstring(resp.content)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    videos = []

    for entry in root.findall("atom:entry", ns):
        vid_el = entry.find("yt:videoId", ns)
        title_el = entry.find("atom:title", ns)
        pub_el = entry.find("atom:published", ns)

        if vid_el is None or title_el is None:
            continue

        video_id = vid_el.text.strip()
        title = title_el.text.strip() if title_el.text else ""
        published = pub_el.text.strip() if pub_el is not None and pub_el.text else ""

        if published:
            try:
                pub_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue
            except ValueError:
                pass

        videos.append({
            "video_id": video_id,
            "title": title,
            "url": f"https://www.youtube.com/watch?v={video_id}",
        })

    return videos


def _extract_judge_frames(video_url: str, tmpdir: str) -> list[str]:
    """判定用に 0s / 3s / 6s の3フレームを抽出する（字幕領域 crop）。"""
    video_path = _download_video(video_url, tmpdir)

    cmd_probe = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=height",
        "-of", "default=noprint_wrappers=1:nokey=1", video_path,
    ]
    result = subprocess.run(cmd_probe, capture_output=True, text=True)
    height = int(result.stdout.strip())
    crop_h = int(height * 0.35)
    crop_y = height - crop_h

    frames_dir = os.path.join(tmpdir, "judge_frames")
    os.makedirs(frames_dir, exist_ok=True)

    for i, t in enumerate([0, 3, 6]):
        cmd = [
            "ffmpeg", "-ss", str(t), "-i", video_path,
            "-vf", f"crop=in_w:{crop_h}:0:{crop_y}",
            "-frames:v", "1", "-q:v", "2",
            os.path.join(frames_dir, f"judge_{i}.jpg"),
            "-y",
        ]
        subprocess.run(cmd, capture_output=True, text=True)

    frames = sorted(Path(frames_dir).glob("judge_*.jpg"))
    return [str(f) for f in frames], video_path


def _judge_with_vision(frames: list[str], api_key: str) -> dict:
    """Claude Vision API で字幕判定を行う。"""
    import anthropic

    images = []
    for f in frames:
        with open(f, "rb") as fh:
            data = base64.standard_b64encode(fh.read()).decode("utf-8")
        images.append({
            "type": "image",
            "source": {"type": "base64", "media_type": "image/jpeg", "data": data},
        })

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=256,
        messages=[{
            "role": "user",
            "content": images + [{
                "type": "text",
                "text": (
                    "These are 3 frames from the subtitle area (bottom 35%) of a video. "
                    "Answer these 4 questions with YES or NO only, one per line:\n"
                    "1. Are there burned-in subtitles visible?\n"
                    "2. Is this conversational content (podcast/interview clip)?\n"
                    "3. Are the subtitles in English?\n"
                    "4. Is this Hip-Hop related content?\n"
                    "Format: just 4 lines of YES or NO."
                ),
            }],
        }],
    )

    text = resp.content[0].text.strip().upper()
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    result = {
        "has_subtitles": "YES" in lines[0] if len(lines) > 0 else False,
        "is_conversational": "YES" in lines[1] if len(lines) > 1 else False,
        "is_english": "YES" in lines[2] if len(lines) > 2 else False,
        "is_hiphop": "YES" in lines[3] if len(lines) > 3 else False,
    }
    result["pass"] = all(result.values())
    return result


def main() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    notion_key = os.environ.get("NOTION_API_KEY", "")
    notion_db = os.environ.get("NOTION_DATABASE_ID", "")

    if not api_key or not notion_key or not notion_db:
        logger.error("Required env vars not set")
        sys.exit(1)

    config = _load_config()
    ocr_engine = config.get("ocr_engine", "tesseract")
    interval = config.get("frame_interval_sec", 0.5)
    crop_ratio = config.get("subtitle_crop_ratio", 0.35)
    max_duration = config.get("max_video_duration_sec", 180)
    name_mapping = _load_name_mapping()
    processed = _load_processed()

    channels = [c for c in config.get("channels", []) if c.get("enabled")]
    logger.info(f"=== Video Monitor: {len(channels)} channels ===")

    total_new = 0
    total_skipped = 0
    total_filtered = 0
    total_processed = 0

    for ch in channels:
        ch_name = ch["name"]
        ch_id = ch["channel_id"]
        logger.info(f"  [{ch_name}] checking...")

        videos = _fetch_recent_videos(ch_id, max_age_hours=48)
        logger.info(f"    Recent videos: {len(videos)}")

        for v in videos:
            vid = v["video_id"]
            if vid in processed:
                total_skipped += 1
                continue

            total_new += 1
            logger.info(f"    Checking: {v['title'][:60]}")

            try:
                info = _get_video_info(v["url"])
                duration = info.get("duration", 0)
                if duration > max_duration:
                    logger.info(f"    Too long ({duration}s), skip")
                    processed.add(vid)
                    total_filtered += 1
                    continue
            except Exception as e:
                logger.warning(f"    Info fetch failed: {e}")
                processed.add(vid)
                total_filtered += 1
                continue

            with tempfile.TemporaryDirectory() as tmpdir:
                try:
                    judge_frames, video_path = _extract_judge_frames(v["url"], tmpdir)
                except Exception as e:
                    logger.warning(f"    Download/extract failed: {e}")
                    processed.add(vid)
                    total_filtered += 1
                    continue

                if not judge_frames:
                    processed.add(vid)
                    total_filtered += 1
                    continue

                judgment = _judge_with_vision(judge_frames, api_key)
                logger.info(f"    Judgment: {judgment}")

                if not judgment["pass"]:
                    processed.add(vid)
                    total_filtered += 1
                    continue

                logger.info(f"    → Passed filter, running full OCR...")
                frames = _extract_frames(video_path, tmpdir, interval, crop_ratio)
                texts = _ocr_frames(frames, ocr_engine)
                lines = _deduplicate_lines(texts)

                if not lines:
                    logger.info(f"    No subtitle text extracted")
                    processed.add(vid)
                    total_filtered += 1
                    continue

                logger.info(f"    Subtitle lines: {len(lines)}")
                japanese = _translate_to_japanese(lines, api_key, name_mapping)

                if not japanese:
                    logger.info(f"    Translation empty")
                    processed.add(vid)
                    continue

                title = _generate_title(japanese, api_key)
                _save_to_notion(notion_key, notion_db, title, japanese, v["url"], ch_name)
                processed.add(vid)
                total_processed += 1
                logger.info(f"    → Saved draft: {title[:40]}")

    _save_processed(processed)
    logger.info(
        f"=== Done: new={total_new}, filtered={total_filtered}, "
        f"processed={total_processed}, skipped={total_skipped} ==="
    )


if __name__ == "__main__":
    main()
