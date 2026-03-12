"""
extractor.py — Parse raw LinkedIn post HTML into structured data.

Uses BeautifulSoup to pull author name, post text, date, and post URL
from the HTML containers collected by scraper.py.  Also tags each post
with the keyword that triggered the match and filters by date range.

LinkedIn now uses obfuscated class names and Shadow DOM components,
so this module includes a text-based fallback that splits the full page
text into individual posts using regex patterns.
"""

import re
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("extractor")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MONTHS_LOOKBACK = 6  # filter window
KEYWORDS_SHAYAK = ["shayak mazumder", "shayak"]
KEYWORDS_ADYA = ["adya ai", "adya"]

# Pattern to detect LinkedIn relative timestamps in text
# Matches patterns like "7m", "5h", "3d", "2w", "1mo", "1yr"
TIMESTAMP_PATTERN = re.compile(
    r"(\d+)\s*(yr|mo|w|d|h|m)\b"
)

# Pattern to split full-page text into individual posts.
# Splits at double-newline BEFORE a line that is followed (within 1-2 lines) by
# the LinkedIn connection-degree marker "• 1st|2nd|3rd|Follow".
# This ensures we never cut mid-word through author names.
POST_SPLIT_PATTERN = re.compile(
    r"\n\n(?=[A-Z][^\n]{1,60}\n[\s\n]*•\s*(?:1st|2nd|3rd|Follow))"
)


# ---------------------------------------------------------------------------
# Helper — resolve relative dates ("3mo", "2w", "5d", "1yr")
# ---------------------------------------------------------------------------
def _parse_relative_date(text: str) -> Optional[datetime]:
    """
    Convert a LinkedIn-style relative time string into an approximate datetime.

    Examples: '3mo', '2w', '5d', '1yr', '12h', '3m' (minutes)
    """
    text = text.strip().lower()
    now = datetime.now()

    patterns = [
        (r"(\d+)\s*yr", lambda m: now - timedelta(days=int(m.group(1)) * 365)),
        (r"(\d+)\s*mo", lambda m: now - timedelta(days=int(m.group(1)) * 30)),
        (r"(\d+)\s*w",  lambda m: now - timedelta(weeks=int(m.group(1)))),
        (r"(\d+)\s*d",  lambda m: now - timedelta(days=int(m.group(1)))),
        (r"(\d+)\s*h",  lambda m: now - timedelta(hours=int(m.group(1)))),
        (r"(\d+)\s*m(?!o)",  lambda m: now - timedelta(minutes=int(m.group(1)))),
    ]

    for pattern, resolver in patterns:
        match = re.search(pattern, text)
        if match:
            return resolver(match)

    return None


# ---------------------------------------------------------------------------
# Helper — split a full page source into individual post HTML fragments
# ---------------------------------------------------------------------------
def _split_page_into_posts(page_html: str) -> List[str]:
    """
    Given a complete LinkedIn search results page, extract individual post
    HTML fragments using multiple selector strategies.
    Falls back to text-based splitting if CSS selectors fail.
    """
    soup = BeautifulSoup(page_html, "html.parser")
    fragments: List[str] = []

    # Strategy 1: Try CSS selectors
    selectors = [
        "div.feed-shared-update-v2",
        "div.occludable-update",
        "li.reusable-search__result-container",
        "div[data-urn]",
        "article",
        "main li",
    ]

    for sel in selectors:
        elements = soup.select(sel)
        valid = [el for el in elements if len(el.get_text(strip=True)) > 50]
        if valid:
            logger.info("Page split selector '%s' found %d elements.", sel, len(valid))
            for el in valid:
                fragments.append(str(el))
            return fragments

    # Strategy 2: Text-based splitting
    # Get the full page text and split into individual posts
    full_text = soup.get_text(" ", strip=True)
    if full_text:
        text_posts = _split_text_into_posts(full_text)
        if text_posts:
            logger.info("Text-based splitting found %d posts.", len(text_posts))
            # Wrap each text block in a simple div so downstream can parse
            for tp in text_posts:
                fragments.append(
                    '<div data-text-extracted="true">' + tp + "</div>"
                )
            return fragments

    # Strategy 3: Generic li fallback
    for li in soup.find_all("li"):
        text = li.get_text(strip=True)
        if len(text) > 100:
            fragments.append(str(li))

    return fragments


def _split_text_into_posts(full_text: str) -> List[str]:
    """
    Split a flat text string (from a full LinkedIn page) into individual
    post text blocks by detecting author name + connection-degree patterns.

    The typical pattern in LinkedIn search results text is:
        AuthorName
         • 1st|2nd|3rd ... <timestamp> • Follow
        <post content>
        Like Comment Repost ...

    Or on one line:
        AuthorName • 1st ... <timestamp>
    """
    # Primary pattern: double-newline before Name followed (within 1-2 lines) by • 1st/2nd/3rd/Follow
    # Using \n\n boundary prevents splitting mid-word through author names
    split_pattern = re.compile(
        r'\n\n(?=[A-Z][^\n]{1,60}\n[\s\n]*•\s*(?:1st|2nd|3rd|Follow))'
    )

    parts = split_pattern.split(full_text)
    posts = []
    for part in parts:
        part = part.strip()
        # A real post should have at least 80 chars and contain some content
        if len(part) > 80:
            posts.append(part)

    # If primary pattern found nothing useful, try an alternative:
    # Split on reaction/engagement footers (end of each post)
    if len(posts) <= 1 and len(full_text) > 500:
        footer_pattern = re.compile(
            r'(?<=(?:\d+ repost[s]?|\d+ comment[s]?|\d+ reaction[s]?))\s*(?=[A-Z])'
        )
        parts2 = footer_pattern.split(full_text)
        alt_posts = [p.strip() for p in parts2 if len(p.strip()) > 80]
        if len(alt_posts) > len(posts):
            posts = alt_posts

    return posts


# ---------------------------------------------------------------------------
# Core extraction from one HTML chunk
# ---------------------------------------------------------------------------
def extract_post_data(html: str, keyword: str, search_url: str) -> Optional[Dict]:
    """
    Parse a single raw HTML chunk and return a structured dict.

    Returns None if no meaningful text could be extracted.

    Keys returned:
        author, post_text, date_raw, date_parsed, post_url,
        keyword, mentions_shayak, mentions_adya
    """
    soup = BeautifulSoup(html, "html.parser")

    # Check if this is a text-extracted chunk (our fallback format)
    text_extracted = soup.find(attrs={"data-text-extracted": "true"})

    if text_extracted:
        return _extract_from_text_block(text_extracted.get_text(), keyword)

    # --- Standard HTML extraction ---
    # --- Author name ---
    author = "Unknown"
    author_selectors = [
        "span.feed-shared-actor__name",
        "span.update-components-actor__name",
        "a.app-aware-link span[dir='ltr']",
        "span.feed-shared-actor__title",
        "span.entity-result__title-text a span",
        "a.app-aware-link[href*='/in/'] span",
        "span[aria-hidden='true']",
    ]
    for sel in author_selectors:
        tags = soup.select(sel)
        for tag in tags:
            text = tag.get_text(strip=True)
            if text and 2 < len(text) < 60 and "\n" not in text:
                author = text
                break
        if author != "Unknown":
            break

    # --- Post text ---
    post_text = ""
    text_selectors = [
        "div.feed-shared-update-v2__description",
        "div.update-components-text",
        "span.break-words",
        "div.feed-shared-text",
        "div.feed-shared-inline-show-more-text",
        "p.entity-result__summary",
        "div.entity-result__content",
    ]
    for sel in text_selectors:
        tag = soup.select_one(sel)
        if tag and tag.get_text(strip=True):
            post_text = tag.get_text(" ", strip=True)
            break

    # Fallback: visible text or all text
    if not post_text:
        post_text = soup.get_text(" ", strip=True)

    if not post_text or len(post_text) < 20:
        return None

    # --- Date ---
    date_raw = ""
    date_parsed = None
    time_selectors = [
        "span.feed-shared-actor__sub-description",
        "time",
        "span.update-components-actor__sub-description",
    ]
    for sel in time_selectors:
        tag = soup.select_one(sel)
        if tag:
            date_raw = tag.get_text(strip=True)
            dt_attr = tag.get("datetime")
            if dt_attr:
                try:
                    date_parsed = datetime.fromisoformat(dt_attr)
                except ValueError:
                    pass
            if not date_parsed:
                date_parsed = _parse_relative_date(date_raw)
            if date_parsed:
                break

    # If no date from selectors, try to find timestamp in text
    if not date_parsed:
        ts_match = TIMESTAMP_PATTERN.search(post_text[:200])
        if ts_match:
            date_raw = ts_match.group(0)
            date_parsed = _parse_relative_date(date_raw)

    # --- Post URL ---
    post_url = ""
    link_tag = soup.find("a", href=re.compile(r"/feed/update/|/posts/"))
    if link_tag:
        href = link_tag.get("href", "")
        if href.startswith("/"):
            href = "https://www.linkedin.com" + href
        post_url = href.split("?")[0]

    # --- Keyword mention flags ---
    text_lower = post_text.lower()
    mentions_shayak = any(k in text_lower for k in KEYWORDS_SHAYAK)
    mentions_adya = any(k in text_lower for k in KEYWORDS_ADYA)

    return {
        "author": author,
        "post_text": post_text,
        "date_raw": date_raw,
        "date_parsed": date_parsed.strftime("%Y-%m-%d") if date_parsed else "",
        "post_url": post_url,
        "keyword": keyword,
        "mentions_shayak": mentions_shayak,
        "mentions_adya": mentions_adya,
    }


def _extract_from_text_block(text: str, keyword: str, pre_url: str = "") -> Optional[Dict]:
    """
    Extract post data from a plain-text block (produced by text-based splitting).

    Typical LinkedIn search result text format (may span multiple lines):
        AuthorName
         • 1st|2nd|3rd
        Title/Description
        <timestamp> • Edited|Follow
        <post body>
        Like Comment Repost ...

    Or single-line: AuthorName • 1st ...
    """
    if not text or len(text) < 50:
        return None

    # ----- Author extraction -----
    # Find the "• 1st/2nd/3rd/Follow" marker — everything before it is candidate author info
    author = "Unknown"
    bullet_match = re.search(r'•\s*(?:1st|2nd|3rd|Follow)', text[:400])
    if bullet_match:
        before_bullet = text[:bullet_match.start()].strip()
        # Get non-empty, short lines before the bullet
        name_lines = [l.strip() for l in before_bullet.split('\n') if l.strip()]
        if name_lines:
            # Take the last 1–2 line(s) — the author name
            candidate = ' '.join(name_lines[-2:]).strip() if len(name_lines) > 1 else name_lines[-1]
            # Clean up: remove leading/trailing punctuation, numbers, etc.
            candidate = re.sub(r'^[^A-Za-z]+', '', candidate)
            candidate = re.sub(r'\s+', ' ', candidate).strip()
            if 2 < len(candidate) < 60:
                author = candidate
    # Fallback: check for single-line "Name • 1st" on first line
    if author == "Unknown":
        first_line = text[:200]
        m = re.match(r'^([A-Za-z][A-Za-z\s\.\-\']+?)\s*•', first_line)
        if m:
            author = m.group(1).strip()

    # ----- Timestamp -----
    date_raw = ""
    date_parsed = None
    # Look for timestamp AFTER the connection indicator
    ts_search_start = bullet_match.end() if bullet_match else 0
    ts_match = TIMESTAMP_PATTERN.search(text[ts_search_start:ts_search_start + 300])
    if ts_match:
        date_raw = ts_match.group(0)
        date_parsed = _parse_relative_date(date_raw)
    # Fallback: search in first 300 chars
    if not date_parsed:
        ts_match2 = TIMESTAMP_PATTERN.search(text[:300])
        if ts_match2:
            date_raw = ts_match2.group(0)
            date_parsed = _parse_relative_date(date_raw)

    # ----- Post body extraction -----
    post_text = text
    # Find the main post body: after "Follow" or "Edited", before engagement footer
    follow_idx = text.find("Follow")
    edited_idx = text.find("Edited")
    body_after = max(follow_idx, edited_idx)
    if body_after >= 0:
        body_start = body_after + len("Follow") if follow_idx >= 0 else body_after + len("Edited")
    else:
        body_start = 0

    # Find engagement footer (reactions, comments, reposts)
    footer_match = re.search(
        r'\n\s*\d+\s*(?:reaction|comment|repost|like)',
        text[body_start:], re.IGNORECASE
    )
    body_end = body_start + footer_match.start() if footer_match else len(text)

    post_body = text[body_start:body_end].strip()
    if len(post_body) > 30:
        post_text = post_body

    # Clean up trailing "… more" / "...see more"
    post_text = re.sub(r"\s*…\s*more\s*$", "", post_text)
    post_text = re.sub(r"\s*\.\.\.see more\s*$", "", post_text, flags=re.IGNORECASE)

    if len(post_text) < 20:
        return None

    # ----- Post URL -----
    post_url = pre_url or ""
    # Accept /feed/update/, /posts/, and lnkd.in URLs
    if post_url and '/feed/update/' not in post_url and '/posts/' not in post_url:
        if 'lnkd.in' not in post_url:
            post_url = ""
    # Try to find a URL in the post text if none assigned
    if not post_url:
        url_match = re.search(
            r"(https?://(?:www\.)?linkedin\.com/feed/update/[^\s]+)", text
        )
        if url_match:
            post_url = url_match.group(1).split("?")[0]
    if not post_url:
        url_match = re.search(
            r"(https?://(?:www\.)?linkedin\.com/posts/[^\s]+)", text
        )
        if url_match:
            post_url = url_match.group(1).split("?")[0]
    if not post_url:
        lnkd_match = re.search(r"(https?://lnkd\.in/[^\s)+\]]+)", text)
        if lnkd_match:
            post_url = lnkd_match.group(1)

    # Keyword mention flags
    text_lower = post_text.lower()
    mentions_shayak = any(k in text_lower for k in KEYWORDS_SHAYAK)
    mentions_adya = any(k in text_lower for k in KEYWORDS_ADYA)

    return {
        "author": author,
        "post_text": post_text,
        "date_raw": date_raw,
        "date_parsed": date_parsed.strftime("%Y-%m-%d") if date_parsed else "",
        "post_url": post_url,
        "keyword": keyword,
        "mentions_shayak": mentions_shayak,
        "mentions_adya": mentions_adya,
    }


# ---------------------------------------------------------------------------
# Batch extraction + date filtering
# ---------------------------------------------------------------------------
def extract_all_posts(
    raw_results: List[Dict[str, str]],
    months_lookback: int = MONTHS_LOOKBACK,
) -> pd.DataFrame:
    """
    Process a list of raw scraper results into a clean DataFrame.

    The scraper now returns structured data with keys:
      keyword, text, post_url, html, search_url

    Steps:
        1. For each result, extract structured fields from text.
        2. Use pre-extracted post_url when available.
        3. De-duplicate by post_text hash.
        4. Filter to posts within the lookback window.

    Args:
        raw_results:    Output of scraper.scrape_linkedin_posts()
        months_lookback: Number of months to look back.

    Returns:
        A pandas DataFrame with columns matching the extracted fields.
    """
    cutoff = datetime.now() - timedelta(days=months_lookback * 30)
    records: List[Dict] = []
    seen_hashes: set = set()

    for item in raw_results:
        keyword = item.get("keyword", "")
        search_url = item.get("search_url", "")
        pre_text = item.get("text", "")
        pre_url = item.get("post_url", "")
        html = item.get("html", "")
        # List of post URLs discovered by the scraper's JS extraction
        # Accept /feed/update/, /posts/, and lnkd.in URLs
        raw_urls = item.get("post_urls", [])
        post_urls_list = [
            u for u in raw_urls
            if '/feed/update/' in u or '/posts/' in u or 'lnkd.in' in u
        ]

        # --- Per-container extraction (preferred: each post has its own URL) ---
        per_post = item.get("per_post_data", [])
        if per_post and len(per_post) > 3:
            logger.info(
                "Using per-container data: %d containers (%d with URLs).",
                len(per_post),
                sum(1 for p in per_post if p.get("url")),
            )
            for pp in per_post:
                pp_text = pp.get("text", "")
                pp_url = pp.get("url", "")
                if len(pp_text) < 50:
                    continue
                data = _extract_from_text_block(pp_text, keyword, pp_url)
                if data:
                    _add_record(data, cutoff, seen_hashes, records)

        # If the scraper already extracted text directly (new structured format)
        if pre_text and len(pre_text) > 50:
            # Try to split if this is a large block containing multiple posts
            if len(pre_text) > 2000:
                sub_posts = _split_text_into_posts(pre_text)
                if len(sub_posts) > 1:
                    logger.info(
                        "Split large text block into %d sub-posts (%d URLs available).",
                        len(sub_posts), len(post_urls_list),
                    )
                    url_idx = 0
                    for sp in sub_posts:
                        # Assign a URL from the ordered list if available
                        assigned_url = ""
                        if url_idx < len(post_urls_list):
                            assigned_url = post_urls_list[url_idx]
                            url_idx += 1
                        data = _extract_from_text_block(sp, keyword, assigned_url)
                        if data is None:
                            continue
                        # Use pre-extracted URL only for first sub-post as fallback
                        if not data["post_url"] and pre_url:
                            data["post_url"] = pre_url
                            pre_url = ""  # only assign to one
                        _add_record(data, cutoff, seen_hashes, records)
                    continue

            # Single post — extract from text
            assigned_url = post_urls_list[0] if post_urls_list else ""
            data = _extract_from_text_block(pre_text, keyword, assigned_url)
            if data:
                if not data["post_url"] and pre_url:
                    data["post_url"] = pre_url
                _add_record(data, cutoff, seen_hashes, records)
            continue
            continue

        # Fallback: HTML-based extraction (legacy path)
        if html:
            if "<html" in html[:500].lower() or len(html) > 50000:
                fragments = _split_page_into_posts(html)
                logger.info(
                    "Full page detected — split into %d post fragments.",
                    len(fragments),
                )
                for frag in fragments:
                    _process_fragment(
                        frag, keyword, search_url, cutoff, seen_hashes, records
                    )
            else:
                _process_fragment(
                    html, keyword, search_url, cutoff, seen_hashes, records
                )

    df = pd.DataFrame(records)
    if df.empty:
        logger.warning("No posts extracted — DataFrame is empty.")
        return pd.DataFrame(
            columns=[
                "author",
                "post_text",
                "date_raw",
                "date_parsed",
                "post_url",
                "keyword",
                "mentions_shayak",
                "mentions_adya",
            ]
        )

    logger.info(
        "Extracted %d unique posts within the last %d months.",
        len(df),
        months_lookback,
    )
    return df


def _add_record(
    data: Dict,
    cutoff: datetime,
    seen_hashes: set,
    records: List[Dict],
) -> None:
    """De-dup and date-filter a single extracted record, append to records."""
    if data is None:
        return

    text_hash = hash(data["post_text"][:200])
    if text_hash in seen_hashes:
        return
    seen_hashes.add(text_hash)

    if data["date_parsed"]:
        try:
            post_date = datetime.strptime(data["date_parsed"], "%Y-%m-%d")
            if post_date < cutoff:
                logger.debug("Skipping old post dated %s", data["date_parsed"])
                return
        except ValueError:
            pass

    records.append(data)


def _process_fragment(
    html: str,
    keyword: str,
    search_url: str,
    cutoff: datetime,
    seen_hashes: set,
    records: List[Dict],
) -> None:
    """Parse a single HTML fragment, de-dup, date-filter, and append to records."""
    data = extract_post_data(html, keyword, search_url)
    if data is None:
        return

    text_hash = hash(data["post_text"][:200])
    if text_hash in seen_hashes:
        return
    seen_hashes.add(text_hash)

    if data["date_parsed"]:
        try:
            post_date = datetime.strptime(data["date_parsed"], "%Y-%m-%d")
            if post_date < cutoff:
                logger.debug("Skipping old post dated %s", data["date_parsed"])
                return
        except ValueError:
            pass

    records.append(data)


# ---------------------------------------------------------------------------
# CLI entry point (standalone testing)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Quick test with dummy HTML
    sample_html = """
    <div class="feed-shared-update-v2">
        <span class="feed-shared-actor__name">John Doe</span>
        <span class="feed-shared-actor__sub-description">3mo</span>
        <div class="update-components-text">
            Great session with Shayak Mazumder about the future of Adya AI.
        </div>
        <a href="/feed/update/urn:li:activity:123456789">link</a>
    </div>
    """
    raw = [{"html": sample_html, "keyword": "Shayak Mazumder", "search_url": ""}]
    df = extract_all_posts(raw)
    print(df.to_string())
