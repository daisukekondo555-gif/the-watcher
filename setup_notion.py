"""
One-time setup script: create all required columns in the Notion database.

Run once before first use:
  py setup_notion.py
"""

import json
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"

api_key = os.environ.get("NOTION_API_KEY")
database_id = os.environ.get("NOTION_DATABASE_ID")

if not api_key or not database_id:
    print("ERROR: NOTION_API_KEY / NOTION_DATABASE_ID not set")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json",
    "Notion-Version": NOTION_VERSION,
}

# ------------------------------------------------------------------
# 1. Retrieve current properties to find the title property name
# ------------------------------------------------------------------
resp = requests.get(f"{NOTION_BASE}/databases/{database_id}", headers=headers)
resp.raise_for_status()
db = resp.json()

existing = db.get("properties", {})
title_prop_name = next(
    (name for name, prop in existing.items() if prop.get("type") == "title"),
    "名前"
)
print(f"Existing title property: '{title_prop_name}'")
print(f"Existing properties: {list(existing.keys())}")

# ------------------------------------------------------------------
# 2. PATCH the database to rename title + add all columns
# ------------------------------------------------------------------
patch_props: dict = {}

# Rename existing title property to "タイトル"
if title_prop_name != "タイトル":
    patch_props[title_prop_name] = {"name": "タイトル"}

# Add properties that don't exist yet
new_props = {
    "本文":           {"rich_text": {}},
    "カテゴリ":       {
        "select": {
            "options": [
                {"name": "ニュース",     "color": "blue"},
                {"name": "リリース",     "color": "green"},
                {"name": "ビーフ",       "color": "red"},
                {"name": "インタビュー", "color": "purple"},
                {"name": "ライブ",       "color": "orange"},
                {"name": "ビジネス",     "color": "yellow"},
                {"name": "チャート",     "color": "pink"},
            ]
        }
    },
    "画像URL":        {"url": {}},
    "元記事URL":      {"rich_text": {}},
    "ソースサイト名": {"rich_text": {}},
    "ステータス": {
        "select": {
            "options": [
                {"name": "下書き", "color": "gray"},
                {"name": "公開",   "color": "green"},
            ]
        }
    },
    "公開日時": {"date": {}},
}

for name, definition in new_props.items():
    if name not in existing:
        patch_props[name] = definition
        print(f"  + Adding: {name}")
    else:
        print(f"  = Already exists: {name}")

if not patch_props:
    print("Nothing to update. Database is already configured.")
    sys.exit(0)

patch_resp = requests.patch(
    f"{NOTION_BASE}/databases/{database_id}",
    headers=headers,
    json={"properties": patch_props},
)

if patch_resp.status_code == 200:
    print("\nDatabase setup complete!")
else:
    print(f"\nERROR {patch_resp.status_code}: {patch_resp.text}")
    sys.exit(1)
