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
from difflib import SequenceMatcher

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
