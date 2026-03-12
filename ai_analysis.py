"""
ai_analysis.py — AI-powered analysis of LinkedIn posts using Google Gemini.

For every post this module produces:
  • A concise 1-2 sentence summary
  • Sentiment classification  (Positive / Neutral / Negative)
  • Topic classification      (e.g. "Product Launch", "Hiring", …)

All three are extracted in a single Gemini API call per post to minimise
token usage and latency.

Features:
  • Exponential backoff retry on 429 rate-limit errors (up to 5 attempts)
  • Automatic model fallback chain: gemini-2.0-flash-lite → gemini-2.0-flash
  • Local keyword-based fallback when API quota is exhausted
  • Configurable delay between API calls
"""

import os
import json
import re
import time
import logging
from typing import Dict, List, Optional

import pandas as pd

try:
    import google.generativeai as genai
except ImportError:
    genai = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _get_secret(key: str, default: str = "") -> str:
    """Read a secret from Streamlit secrets (cloud) or env vars (local)."""
    try:
        import streamlit as st
        return st.secrets.get(key, default)
    except Exception:
        return os.getenv(key, default)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("ai_analysis")

# ---------------------------------------------------------------------------
# Gemini configuration
# ---------------------------------------------------------------------------
GEMINI_API_KEY = _get_secret("GEMINI_API_KEY")

# Models to try in order (lite model has separate quota from full model)
MODEL_CHAIN: List[str] = [
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash",
]

RATE_LIMIT_DELAY = 4.0            # seconds between API calls (generous for free tier)
MAX_RETRIES = 2                   # max retry attempts per API call
INITIAL_BACKOFF = 3.0             # initial backoff in seconds for 429 errors

_active_model_name: str = MODEL_CHAIN[0]  # will be set during configure
_gemini_exhausted: bool = False  # Set True after first complete failure — skips all future API calls


def _configure_gemini() -> bool:
    """Initialise the Gemini client. Returns True on success."""
    if genai is None:
        logger.error("google-generativeai package is not installed.")
        return False

    if not GEMINI_API_KEY:
        logger.error(
            "GEMINI_API_KEY not found. "
            "Set it in your .env file or as an environment variable."
        )
        return False

    genai.configure(api_key=GEMINI_API_KEY)
    logger.info("Gemini API configured. Model chain: %s", MODEL_CHAIN)
    return True


# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------
ANALYSIS_PROMPT = """You are an expert analyst reviewing LinkedIn posts that mention "Shayak Mazumder" or "Adya AI / Adya".

Analyze the following LinkedIn post and return a JSON object with EXACTLY these three keys:

1. "summary"   — A concise 1-2 sentence summary of the post.
2. "sentiment" — One of: "Positive", "Neutral", or "Negative".
3. "topic"     — A short topic label such as "Product Launch", "Partnership",
                  "Hiring", "Thought Leadership", "Event", "Fundraising",
                  "Customer Testimonial", "General Mention", etc.

Rules:
- Return ONLY valid JSON — no markdown fences, no extra text.
- Keep the summary factual and concise.

--- POST START ---
{post_text}
--- POST END ---

JSON response:"""


# ---------------------------------------------------------------------------
# Local fallback analyzer (no API needed)
# ---------------------------------------------------------------------------
_POSITIVE_WORDS = {
    "excited", "thrilled", "proud", "congratulations", "congrats", "amazing",
    "incredible", "fantastic", "great", "excellent", "love", "happy", "delighted",
    "innovative", "groundbreaking", "impressive", "outstanding", "brilliant",
    "celebrate", "milestone", "success", "achievement", "award", "honored",
    "grateful", "thankful", "opportunity", "growth", "raised", "funding",
    "launched", "announcing", "pleased", "wonderful", "game-changer",
}
_NEGATIVE_WORDS = {
    "disappointed", "concerned", "worried", "problem", "issue", "failed",
    "unfortunately", "struggling", "decline", "loss", "risk", "breach",
    "vulnerability", "attack", "threat", "scam", "fraud", "warning", "bad",
    "poor", "terrible", "horrible", "worst", "mistake", "error", "critical",
}
_TOPIC_PATTERNS = [
    (r"(?:series [a-z]|funding|raised|invest|round|capital|valuation)", "Fundraising"),
    (r"(?:launch|released|announcing|new product|introducing|unveil)", "Product Launch"),
    (r"(?:partner|collaborat|alliance|integration|joint)", "Partnership"),
    (r"(?:hiring|join our|looking for|open position|career|recruit|team)", "Hiring"),
    (r"(?:event|conference|summit|webinar|keynote|panel|speaking)", "Event"),
    (r"(?:customer|testimonial|case study|client|success story)", "Customer Testimonial"),
    (r"(?:security|data protection|breach|compliance|governance|cyber)", "Data Security"),
    (r"(?:ai|artificial intelligence|machine learning|genai|llm)", "AI / Technology"),
    (r"(?:leader|thought|vision|insight|perspective|opinion|trend)", "Thought Leadership"),
]


def _local_analyze(post_text: str) -> Dict[str, str]:
    """
    Fallback keyword-based analysis when Gemini API is unavailable.

    Returns dict with summary, sentiment, topic — all derived locally.
    """
    text_lower = post_text.lower()
    words = set(re.findall(r'[a-z]+', text_lower))

    # --- Sentiment ---
    pos = len(words & _POSITIVE_WORDS)
    neg = len(words & _NEGATIVE_WORDS)
    if pos > neg:
        sentiment = "Positive"
    elif neg > pos:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"

    # --- Topic ---
    topic = "General Mention"
    for pattern, label in _TOPIC_PATTERNS:
        if re.search(pattern, text_lower):
            topic = label
            break

    # --- Summary (first 2 sentences, max 200 chars) ---
    sentences = re.split(r'(?<=[.!?])\s+', post_text.strip())
    summary = " ".join(sentences[:2])
    if len(summary) > 200:
        summary = summary[:197] + "…"

    return {"summary": summary, "sentiment": sentiment, "topic": topic}


def _parse_response(raw_text: str) -> Dict[str, str]:
    """Parse the Gemini response into a dict. Raises ValueError on failure."""
    text = raw_text.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
    if text.endswith("```"):
        text = text.rsplit("```", 1)[0]
    text = text.strip()

    # Try direct JSON parse
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON object from surrounding text
        match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
        if match:
            result = json.loads(match.group())
        else:
            raise ValueError(f"No JSON found in response: {text[:200]}")

    summary = result.get("summary", "")
    sentiment = result.get("sentiment", "Unknown")
    topic = result.get("topic", "Unknown")

    if sentiment not in ("Positive", "Neutral", "Negative"):
        sentiment = "Neutral"

    return {"summary": summary, "sentiment": sentiment, "topic": topic}


# ---------------------------------------------------------------------------
# Single-post analysis with retry + model fallback
# ---------------------------------------------------------------------------
def analyze_post(post_text: str) -> Dict[str, str]:
    """
    Send a single post to Gemini and return analysis results.

    Uses exponential backoff for 429 errors and falls back through
    MODEL_CHAIN if one model's quota is exhausted.

    Returns:
        Dict with keys: summary, sentiment, topic
        On failure returns defaults with empty summary and "Unknown" labels.
    """
    defaults = {"summary": "", "sentiment": "Unknown", "topic": "Unknown"}

    if not post_text or len(post_text.strip()) < 10:
        logger.warning("Post text too short to analyse.")
        return defaults

    prompt = ANALYSIS_PROMPT.format(post_text=post_text[:3000])

    global _gemini_exhausted
    if _gemini_exhausted:
        return _local_analyze(post_text)

    for model_name in MODEL_CHAIN:
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                result = _parse_response(response.text)
                logger.info("  Success with model=%s", model_name)
                return result

            except Exception as exc:
                err_str = str(exc)
                is_rate_limit = "429" in err_str or "quota" in err_str.lower()

                if is_rate_limit:
                    if attempt < MAX_RETRIES:
                        logger.warning(
                            "  Rate-limited (model=%s, attempt %d/%d). "
                            "Waiting %.1fs before retry…",
                            model_name, attempt, MAX_RETRIES, backoff,
                        )
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 120)  # cap at 2 minutes
                        continue
                    else:
                        logger.warning(
                            "  Rate limit exhausted for model=%s after %d attempts. "
                            "Trying next model…",
                            model_name, MAX_RETRIES,
                        )
                        break  # move to next model in chain
                else:
                    logger.error("  Gemini error (model=%s): %s", model_name, err_str[:300])
                    return defaults

    logger.warning("All Gemini models exhausted. Using local keyword-based analysis.")
    _gemini_exhausted = True  # Skip API for all remaining posts
    return _local_analyze(post_text)


# ---------------------------------------------------------------------------
# Batch analysis over a DataFrame
# ---------------------------------------------------------------------------
def analyze_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyse every row in *df* that has a 'post_text' column.

    Adds three new columns: ai_summary, ai_sentiment, ai_topic.
    Skips rows that already have valid analysis (non-"Unknown" sentiment).

    Args:
        df: DataFrame produced by extractor.extract_all_posts()

    Returns:
        The same DataFrame with AI columns appended.
    """
    if df.empty:
        df["ai_summary"] = []
        df["ai_sentiment"] = []
        df["ai_topic"] = []
        return df

    if not _configure_gemini():
        logger.warning("Gemini not configured — filling AI columns with defaults.")
        df["ai_summary"] = ""
        df["ai_sentiment"] = "Unknown"
        df["ai_topic"] = "Unknown"
        return df

    # Pre-fill columns if they don't exist
    if "ai_summary" not in df.columns:
        df["ai_summary"] = ""
    if "ai_sentiment" not in df.columns:
        df["ai_sentiment"] = "Unknown"
    if "ai_topic" not in df.columns:
        df["ai_topic"] = "Unknown"

    total = len(df)
    analysed = 0
    skipped = 0

    for idx, row in df.iterrows():
        # Skip rows already analysed successfully
        if (row.get("ai_sentiment", "Unknown") not in ("Unknown", "", None)
                and row.get("ai_topic", "Unknown") not in ("Unknown", "", None)):
            skipped += 1
            continue

        analysed += 1
        logger.info("Analysing post %d/%d (row %d) …", analysed, total - skipped, idx)
        result = analyze_post(row.get("post_text", ""))
        df.at[idx, "ai_summary"] = result["summary"]
        df.at[idx, "ai_sentiment"] = result["sentiment"]
        df.at[idx, "ai_topic"] = result["topic"]
        if not _gemini_exhausted:
            time.sleep(RATE_LIMIT_DELAY)  # respect rate limits (skip when using local)

    logger.info(
        "AI analysis complete: %d analysed, %d skipped (already done), %d total.",
        analysed, skipped, total,
    )
    return df


# ---------------------------------------------------------------------------
# CLI entry point (standalone testing)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    sample = (
        "Excited to announce that Adya AI just closed a $10M Series A! "
        "Huge thanks to Shayak Mazumder for his visionary leadership. "
        "The future of enterprise data security is here. #AdyaAI #startup"
    )
    if _configure_gemini():
        result = analyze_post(sample)
        print(json.dumps(result, indent=2))
    else:
        print("Set GEMINI_API_KEY to test.")
