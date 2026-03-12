"""
pipeline.py — Orchestrates the full LinkedIn Mention Intelligence pipeline.

Workflow:
    1. Scrape LinkedIn posts for configured keywords  (scraper.py)
    2. Extract & clean structured data                 (extractor.py)
    3. Save results to CSV                             (data/linkedin_mentions.csv)

Usage:
    python pipeline.py                     # full run (scrape → save)
    python pipeline.py --skip-scrape       # re-extract existing CSV only
    python pipeline.py --headless false     # run with visible browser
"""

import os
import sys
import argparse
import logging
from datetime import datetime

import pandas as pd

from scraper import scrape_linkedin_posts
from extractor import extract_all_posts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CSV_PATH = os.path.join(DATA_DIR, "linkedin_mentions.csv")


# ---------------------------------------------------------------------------
# Utility — merge new data with existing CSV (avoid duplicates)
# ---------------------------------------------------------------------------
def _merge_with_existing(new_df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """Load existing CSV (if any), append new rows, and de-duplicate."""
    if os.path.exists(csv_path):
        try:
            existing = pd.read_csv(csv_path)
            combined = pd.concat([existing, new_df], ignore_index=True)
            # De-duplicate on post_text (first 200 chars)
            combined["_dedup"] = combined["post_text"].str[:200]
            combined = combined.drop_duplicates(subset="_dedup", keep="last")
            combined = combined.drop(columns=["_dedup"])
            logger.info(
                "Merged: %d existing + %d new → %d unique rows.",
                len(existing), len(new_df), len(combined),
            )
            return combined
        except Exception as exc:
            logger.warning("Could not read existing CSV — overwriting. (%s)", exc)

    return new_df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def run_pipeline(
    skip_scrape: bool = False,
    headless: bool = True,
    max_scrolls: int = 25,
) -> pd.DataFrame:
    """
    Execute the full pipeline and return the final DataFrame.

    Args:
        skip_scrape: If True, skip scraping and re-analyse the existing CSV.
        headless:    Run the browser in headless mode.
        max_scrolls: Max scroll iterations per keyword search.

    Returns:
        DataFrame with all columns including AI analysis.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # Step 1 — Scrape (or load existing)
    # ------------------------------------------------------------------
    if skip_scrape:
        logger.info("--skip-scrape: loading existing CSV at %s", CSV_PATH)
        if not os.path.exists(CSV_PATH):
            logger.error("No existing CSV found at %s. Run without --skip-scrape first.", CSV_PATH)
            sys.exit(1)
        df = pd.read_csv(CSV_PATH)
        logger.info("Loaded %d rows from CSV.", len(df))
    else:
        logger.info("=" * 60)
        logger.info("STEP 1 / 3 — Scraping LinkedIn posts …")
        logger.info("=" * 60)
        raw_results = scrape_linkedin_posts(headless=headless, max_scrolls=max_scrolls)

        if not raw_results:
            logger.warning(
                "No raw results from scraper. "
                "This may happen if login failed or LinkedIn blocked the session."
            )
            # Create an empty DataFrame with expected columns
            df = pd.DataFrame(columns=[
                "author", "post_text", "date_raw", "date_parsed",
                "post_url", "keyword", "mentions_shayak", "mentions_adya",
            ])
        else:
            # ----------------------------------------------------------
            # Step 2 — Extract structured data
            # ----------------------------------------------------------
            logger.info("=" * 60)
            logger.info("STEP 2 / 3 — Extracting post data …")
            logger.info("=" * 60)
            df = extract_all_posts(raw_results)

    logger.info("Posts in DataFrame: %d", len(df))

    # ------------------------------------------------------------------
    # Step 3 — Save to CSV
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 3 / 3 — Saving results to CSV …")
    logger.info("=" * 60)

    if not skip_scrape:
        df = _merge_with_existing(df, CSV_PATH)

    df.to_csv(CSV_PATH, index=False)
    logger.info("Results saved → %s  (%d rows)", CSV_PATH, len(df))

    # Summary
    logger.info("-" * 60)
    logger.info("Pipeline complete!")
    logger.info("  Total posts       : %d", len(df))
    if "mentions_shayak" in df.columns:
        logger.info("  Shayak mentions   : %d", df["mentions_shayak"].sum())
    if "mentions_adya" in df.columns:
        logger.info("  Adya AI mentions  : %d", df["mentions_adya"].sum())
    logger.info("-" * 60)

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="LinkedIn Mention Intelligence Pipeline",
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip scraping and re-analyse existing CSV.",
    )
    parser.add_argument(
        "--headless",
        type=str,
        default="true",
        help="Run browser headless (true/false). Default: true.",
    )
    parser.add_argument(
        "--max-scrolls",
        type=int,
        default=25,
        help="Max scroll iterations per keyword search. Default: 25.",
    )
    args = parser.parse_args()
    headless = args.headless.lower() in ("true", "1", "yes")

    run_pipeline(
        skip_scrape=args.skip_scrape,
        headless=headless,
        max_scrolls=args.max_scrolls,
    )


if __name__ == "__main__":
    main()
