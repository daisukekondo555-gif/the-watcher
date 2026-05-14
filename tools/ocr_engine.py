"""
OCR エンジン抽象化モジュール。

焼き込み字幕の読み取りに使用する OCR エンジンを切り替え可能にする。
対応エンジン: tesseract / paddle / vision (Claude Vision API)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def extract_text(image_path: str, engine: str = "tesseract") -> str:
    if engine == "tesseract":
        return _ocr_tesseract(image_path)
    elif engine == "paddle":
        return _ocr_paddle(image_path)
    elif engine == "vision":
        return _ocr_vision(image_path)
    else:
        raise ValueError(f"Unknown OCR engine: {engine}")


def _ocr_tesseract(image_path: str) -> str:
    try:
        import pytesseract
        from PIL import Image
        img = Image.open(image_path)
        text = pytesseract.image_to_string(img, lang="eng", config="--psm 6")
        return text.strip()
    except Exception as e:
        logger.debug(f"Tesseract OCR failed for {image_path}: {e}")
        return ""


def _ocr_paddle(image_path: str) -> str:
    try:
        from paddleocr import PaddleOCR
        ocr = PaddleOCR(use_angle_cls=True, lang="en", show_log=False)
        result = ocr.ocr(image_path, cls=True)
        if not result or not result[0]:
            return ""
        lines = [line[1][0] for line in result[0] if line[1][0]]
        return "\n".join(lines)
    except Exception as e:
        logger.debug(f"PaddleOCR failed for {image_path}: {e}")
        return ""


def _ocr_vision(image_path: str) -> str:
    """Claude Vision API を OCR として使用する（コスト高、精度最高）。"""
    import base64
    import os
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        with open(image_path, "rb") as f:
            img_data = base64.standard_b64encode(f.read()).decode("utf-8")

        ext = Path(image_path).suffix.lower()
        media_type = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png"}.get(ext.lstrip("."), "image/png")

        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_data}},
                    {"type": "text", "text": "Read all text visible in this image. Return only the text, nothing else."},
                ],
            }],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.debug(f"Vision OCR failed for {image_path}: {e}")
        return ""
