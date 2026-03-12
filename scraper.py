"""
scraper.py — LinkedIn Post Scraper using Selenium

Searches LinkedIn for posts mentioning specified keywords, scrolls through
results to collect post URLs and raw HTML for downstream extraction.
"""

import os
import time
import random
import logging
from datetime import datetime
from typing import List, Dict, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)

try:
    import undetected_chromedriver as uc
except ImportError:
    uc = None

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
except ImportError:
    webdriver = None

try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    ChromeDriverManager = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required when using st.secrets


def _get_secret(key: str, default: str = "") -> str:
    """Read a secret from Streamlit secrets (cloud) or env vars (local)."""
    try:
        import streamlit as st
        return st.secrets.get(key, default)
    except Exception:
        return os.getenv(key, default)

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("scraper")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LINKEDIN_LOGIN_URL = "https://www.linkedin.com/login"
# LinkedIn search URL templates — multiple strategies for wider time coverage
LINKEDIN_SEARCH_URLS = [
    # Strategy 1: Sort by date (gets most recent ~1-2 months)
    "https://www.linkedin.com/search/results/content/?keywords={query}&origin=GLOBAL_SEARCH_HEADER&sortBy=%22date_posted%22",
    # Strategy 2: Relevance sort (surfaces older/popular posts)
    "https://www.linkedin.com/search/results/content/?keywords={query}&origin=GLOBAL_SEARCH_HEADER",
]
SCROLL_PAUSE = 2          # seconds between scrolls
MAX_SCROLLS = 50          # max number of scroll actions per search
KEYWORDS = ["Shayak Mazumder", "Adya AI", "Adya"]

# Google site-search URL template for finding older LinkedIn posts
GOOGLE_SEARCH_URL = "https://www.google.com/search?q=site:linkedin.com+{query}&tbs=cdr:1,cd_min:{date_min},cd_max:{date_max}&num=50"


# ---------------------------------------------------------------------------
# Helper — human-like delay
# ---------------------------------------------------------------------------
def _random_sleep(low: float = 1.0, high: float = 3.0) -> None:
    """Sleep for a random interval to mimic human behaviour."""
    time.sleep(random.uniform(low, high))


# ---------------------------------------------------------------------------
# Driver factory
# ---------------------------------------------------------------------------
# Persistent Chrome profile directory (keeps cookies between runs)
CHROME_PROFILE_DIR = os.path.join(os.path.dirname(__file__), ".chrome_profile")


def create_driver(headless: bool = True):
    """
    Create and return a Chrome WebDriver instance.

    Uses undetected-chromedriver (preferred) to bypass bot detection by
    Google/LinkedIn, falling back to standard Selenium if unavailable.

    A persistent Chrome profile is used so that after one successful login,
    session cookies are preserved for future runs.

    Args:
        headless: Run browser in headless mode (no GUI). Default True.

    Returns:
        A configured Chrome WebDriver.
    """
    # ---- Strategy 1: undetected-chromedriver (bypasses Google block) ----
    if uc is not None:
        logger.info("Using undetected-chromedriver (anti-detection enabled).")
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        try:
            driver = uc.Chrome(
                options=options,
                user_data_dir=CHROME_PROFILE_DIR,
                use_subprocess=True,
            )
            logger.info("undetected-chromedriver ready.")
            return driver
        except Exception as exc:
            logger.warning("undetected-chromedriver failed (%s). Falling back to Selenium.", exc)

    # ---- Strategy 2: Standard Selenium (fallback) ----
    if webdriver is None:
        raise RuntimeError("Neither undetected-chromedriver nor selenium is installed.")

    logger.info("Using standard Selenium WebDriver.")
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    chrome_options.add_argument(f"--user-data-dir={CHROME_PROFILE_DIR}")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    try:
        if ChromeDriverManager:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            driver = webdriver.Chrome(options=chrome_options)
    except WebDriverException as exc:
        logger.error("Failed to create Chrome driver: %s", exc)
        raise

    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    return driver


# ---------------------------------------------------------------------------
# LinkedIn authentication
# ---------------------------------------------------------------------------
def login_to_linkedin(driver: webdriver.Chrome) -> bool:
    """
    Log in to LinkedIn using credentials from environment variables.

    Requires LINKEDIN_EMAIL and LINKEDIN_PASSWORD in .env or env vars.

    Returns:
        True on success, False on failure.
    """
    email = _get_secret("LINKEDIN_EMAIL")
    password = _get_secret("LINKEDIN_PASSWORD")

    if not email or not password:
        logger.error(
            "LinkedIn credentials not found. "
            "Set LINKEDIN_EMAIL and LINKEDIN_PASSWORD in .env or Streamlit secrets."
        )
        return False

    try:
        logger.info("Navigating to LinkedIn login page …")
        driver.get(LINKEDIN_LOGIN_URL)
        _random_sleep(4, 6)

        # Enter email — try multiple selectors
        email_field = None
        email_selectors = [
            (By.ID, "username"),
            (By.CSS_SELECTOR, "input[name='session_key']"),
            (By.CSS_SELECTOR, "input[autocomplete='username']"),
            (By.CSS_SELECTOR, "#login-email"),
        ]
        for by, sel in email_selectors:
            try:
                email_field = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((by, sel))
                )
                if email_field:
                    logger.info("Found email field with selector: %s", sel)
                    break
            except TimeoutException:
                continue

        if not email_field:
            logger.error("Could not find email input on login page. URL: %s", driver.current_url)
            # Save screenshot for debugging
            try:
                driver.save_screenshot("login_debug.png")
                logger.info("Saved debug screenshot to login_debug.png")
            except Exception:
                pass
            return False

        email_field.clear()
        email_field.send_keys(email)
        _random_sleep(0.5, 1.5)

        # Enter password — try multiple selectors
        password_field = None
        pw_selectors = [
            (By.ID, "password"),
            (By.CSS_SELECTOR, "input[name='session_password']"),
            (By.CSS_SELECTOR, "input[type='password']"),
        ]
        for by, sel in pw_selectors:
            try:
                password_field = driver.find_element(by, sel)
                if password_field:
                    break
            except NoSuchElementException:
                continue

        if not password_field:
            logger.error("Could not find password input on login page.")
            return False

        password_field.clear()
        password_field.send_keys(password)
        _random_sleep(0.5, 1.5)

        # Submit
        password_field.send_keys(Keys.RETURN)
        _random_sleep(5, 8)

        # Verify login succeeded
        if "feed" in driver.current_url or "mynetwork" in driver.current_url:
            logger.info("Successfully logged in to LinkedIn.")
            return True

        # Sometimes LinkedIn shows a security check
        if "checkpoint" in driver.current_url or "challenge" in driver.current_url:
            logger.warning(
                "LinkedIn security challenge detected — manual intervention needed."
            )
            signal_file = os.path.join(os.path.dirname(__file__), ".captcha_done")
            # Remove stale signal file
            if os.path.exists(signal_file):
                os.remove(signal_file)
            print("\n" + "=" * 60)
            print("  LinkedIn security challenge detected!")
            print("  Please solve the captcha in the Chrome window.")
            print("  The scraper will continue automatically once solved.")
            print("=" * 60)
            # Poll: check both the URL and a signal file
            for _ in range(600):  # up to 10 minutes
                time.sleep(1)
                try:
                    cur = driver.current_url
                except Exception:
                    break
                if "feed" in cur or "mynetwork" in cur:
                    logger.info("Challenge resolved — logged in (URL changed).")
                    return True
                if os.path.exists(signal_file):
                    os.remove(signal_file)
                    logger.info("Signal file detected — waiting for page load …")
                    _random_sleep(5, 8)
                    return True
            logger.error("Challenge was not resolved within 10 minutes.")
            return False

        logger.warning("Login may have failed — current URL: %s", driver.current_url)
        return True  # optimistic: continue anyway

    except TimeoutException:
        logger.error("Timed out waiting for login page elements.")
        return False
    except Exception as exc:
        logger.error("Unexpected error during login: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Scroll & collect post containers
# ---------------------------------------------------------------------------
def _scroll_and_collect(driver, max_scrolls: int = MAX_SCROLLS) -> List[Dict]:
    """
    Two-phase approach to collect LinkedIn search results:

    Phase 1 — SCROLL: Aggressively scroll the page to force LinkedIn to
    load all available results.  Stall detection is based on **page height**
    (not post count), so we don't bail out prematurely.

    Phase 2 — EXTRACT: Once the page is fully loaded, do a single JS pass
    to collect:
      * The full visible text from <main>
      * All unique post URLs discovered in <a> hrefs and data-urn attributes

    The downstream *extractor* splits the full text into individual posts
    and assigns URLs by position.

    Returns:
        List with one dict per keyword search:
          {text, url, html, post_urls: List[str]}
    """
    _random_sleep(4, 6)  # wait for initial page load

    # LinkedIn often uses an inner scroll container, which makes
    # document.body.scrollHeight unreliable (returns 0).  We use TWO
    # methods to scroll: JS scrollTo + keyboard End key, and track the
    # amount of *text* on the page to detect stalls.
    SCROLL_JS = "window.scrollTo(0, Math.max(document.body.scrollHeight||0, document.documentElement.scrollHeight||0, 50000))"

    # Measure text length as stall indicator — more reliable than page height
    last_text_len = len(driver.execute_script(
        "return (document.querySelector('main')||document.body).innerText||''"
    ))
    stall_count = 0
    total_scrolls = 0

    # ---- Phase 1: Scroll to load all content --------------------------
    for scroll_num in range(1, max_scrolls + 1):
        total_scrolls = scroll_num

        # Scroll using both JS and keyboard for maximum compatibility
        driver.execute_script(SCROLL_JS)
        try:
            from selenium.webdriver.common.keys import Keys
            body = driver.find_element(By.TAG_NAME, "body")
            body.send_keys(Keys.END)
        except Exception:
            pass
        _random_sleep(SCROLL_PAUSE, SCROLL_PAUSE + 2)

        # Try clicking "Show more results" / "See more results" buttons
        try:
            driver.execute_script("""
                var btns = document.querySelectorAll('button, a.artdeco-button');
                for (var i = 0; i < btns.length; i++) {
                    var t = (btns[i].innerText || '').toLowerCase().trim();
                    if (t.indexOf('show more') >= 0 || t === 'see more results' ||
                        t.indexOf('see more') >= 0 || t === 'load more') {
                        btns[i].scrollIntoView();
                        btns[i].click();
                        break;
                    }
                }
            """)
        except Exception:
            pass

        new_height = driver.execute_script(
            "return (document.querySelector('main')||document.body).innerText.length||0"
        )
        if new_height == last_text_len:
            stall_count += 1
        else:
            stall_count = 0
            last_text_len = new_height

        if stall_count >= 5:
            logger.info(
                "Page text stable for %d consecutive scrolls (scroll %d/%d). Done scrolling.",
                stall_count, scroll_num, max_scrolls,
            )
            break

        if scroll_num % 5 == 0:
            logger.info(
                "Scroll %d/%d — text length: %d chars",
                scroll_num, max_scrolls, new_height,
            )

    logger.info(
        "Scroll phase complete after %d scrolls. Final text length: %d chars",
        total_scrolls, last_text_len,
    )

    # ---- Phase 2: Extract from fully-loaded page ----------------------
    data = driver.execute_script("""
        var main = document.querySelector('main') || document.body;
        var fullText = main.innerText || '';

        // --- Collect all unique post URLs ---
        var urls = [];
        var seen = {};

        // Method 1: <a> hrefs with /feed/update/ or /posts/
        main.querySelectorAll('a').forEach(function(a) {
            var href = a.href || '';
            if (href.indexOf('/feed/update/') >= 0 || href.indexOf('/posts/') >= 0) {
                var clean = href.split('?')[0];
                if (!seen[clean]) { seen[clean] = true; urls.push(clean); }
            }
        });

        // Method 2: data-urn attributes containing activity URNs
        main.querySelectorAll('[data-urn]').forEach(function(el) {
            var urn = el.getAttribute('data-urn') || '';
            if (urn.indexOf('activity:') >= 0) {
                var url = 'https://www.linkedin.com/feed/update/' + urn;
                if (!seen[url]) { seen[url] = true; urls.push(url); }
            }
        });

        // Method 3: Scan all attributes for urn:li:activity:NNN
        var allEls = main.querySelectorAll('*');
        for (var i = 0; i < Math.min(allEls.length, 5000); i++) {
            var attrs = allEls[i].attributes || [];
            for (var j = 0; j < attrs.length; j++) {
                var val = attrs[j].value || '';
                var m = val.match(/urn:li:activity:(\\d+)/);
                if (m) {
                    var url = 'https://www.linkedin.com/feed/update/urn:li:activity:' + m[1];
                    if (!seen[url]) { seen[url] = true; urls.push(url); }
                }
            }
        }

        // Method 4: Extract lnkd.in short links from text
        var lnkdLinks = fullText.match(/https?:\\/\\/lnkd\\.in\\/[^\\s)]+/g) || [];
        lnkdLinks.forEach(function(link) {
            if (!seen[link]) { seen[link] = true; urls.push(link); }
        });

        // Method 5: Extract linkedin.com/feed/update links from text content
        var feedLinks = fullText.match(/https?:\\/\\/(?:www\\.)?linkedin\\.com\\/feed\\/update\\/[^\\s)]+/g) || [];
        feedLinks.forEach(function(link) {
            var clean = link.split('?')[0];
            if (!seen[clean]) { seen[clean] = true; urls.push(clean); }
        });

        // --- Per-container extraction for better URL assignment ---
        var perPosts = [];
        var containers = main.querySelectorAll(
            'li.reusable-search__result-container, div[data-urn], ' +
            'div.feed-shared-update-v2, div.occludable-update'
        );
        containers.forEach(function(container) {
            var cText = (container.innerText || '').trim();
            if (cText.length < 50) return;
            var cUrl = '';
            container.querySelectorAll('a').forEach(function(a) {
                if (cUrl) return;
                var href = a.href || '';
                if (href.indexOf('/feed/update/') >= 0 || href.indexOf('/posts/') >= 0) {
                    cUrl = href.split('?')[0];
                }
            });
            if (!cUrl) {
                var cAttrs = container.attributes || [];
                for (var ca = 0; ca < cAttrs.length; ca++) {
                    var cv = cAttrs[ca].value || '';
                    var cm = cv.match(/urn:li:activity:(\\d+)/);
                    if (cm) {
                        cUrl = 'https://www.linkedin.com/feed/update/urn:li:activity:' + cm[1];
                        break;
                    }
                }
            }
            if (!cUrl) {
                var urnEl = container.querySelector('[data-urn*="activity"]');
                if (urnEl) {
                    var u2 = urnEl.getAttribute('data-urn') || '';
                    var cm2 = u2.match(/urn:li:activity:(\\d+)/);
                    if (cm2) cUrl = 'https://www.linkedin.com/feed/update/urn:li:activity:' + cm2[1];
                }
            }
            perPosts.push({text: cText, url: cUrl});
        });

        return {text: fullText, urls: urls, perPosts: perPosts};
    """)

    text = data.get("text", "") if isinstance(data, dict) else ""
    urls = data.get("urls", []) if isinstance(data, dict) else []
    per_posts = data.get("perPosts", []) if isinstance(data, dict) else []

    logger.info(
        "Extracted page text (%d chars) with %d post URLs, %d per-container posts.",
        len(text), len(urls), len(per_posts),
    )

    return [{"text": text, "url": "", "html": "", "post_urls": urls, "per_post_data": per_posts}]


# ---------------------------------------------------------------------------
# Public API — search & scrape
# ---------------------------------------------------------------------------
def scrape_linkedin_posts(
    keywords: Optional[List[str]] = None,
    headless: bool = True,
    max_scrolls: int = MAX_SCROLLS,
) -> List[Dict[str, str]]:
    """
    End-to-end scraping pipeline:
      1. Launch browser & log in
      2. For each keyword, perform a content search
      3. Scroll and collect full page text + post URLs
      4. Use Google site-search to find older LinkedIn posts (>1 month)
      5. Return list of dicts per keyword

    Args:
        keywords:    List of search terms. Defaults to KEYWORDS.
        headless:    Run Chrome headless.
        max_scrolls: Maximum scroll iterations per keyword.

    Returns:
        List of dicts with keys: keyword, text, post_url, html, search_url, post_urls
    """
    if keywords is None:
        keywords = KEYWORDS

    driver = create_driver(headless=headless)
    results: List[Dict[str, str]] = []

    try:
        # Step 1 — Log in
        if not login_to_linkedin(driver):
            logger.error("Aborting scrape — login failed.")
            return results

        # Step 2 — LinkedIn native search with multiple strategies
        for kw in keywords:
            for strat_idx, url_tmpl in enumerate(LINKEDIN_SEARCH_URLS, 1):
                logger.info(
                    "Strategy %d/%d — Searching LinkedIn for: '%s'",
                    strat_idx, len(LINKEDIN_SEARCH_URLS), kw,
                )
                search_url = url_tmpl.format(query=kw.replace(" ", "%20"))
                driver.get(search_url)
                _random_sleep(3, 5)

                posts = _scroll_and_collect(driver, max_scrolls=max_scrolls)
                for post in posts:
                    results.append(
                        {
                            "keyword": kw,
                            "text": post.get("text", ""),
                            "post_url": post.get("url", ""),
                            "html": post.get("html", ""),
                            "search_url": search_url,
                            "post_urls": post.get("post_urls", []),
                            "per_post_data": post.get("per_post_data", []),
                        }
                    )

                _random_sleep(5, 10)

        # Step 3 — Google site-search for older LinkedIn posts (2-6 months back)
        logger.info("=" * 60)
        logger.info("Searching Google for older LinkedIn posts (months 2-6) …")
        logger.info("=" * 60)
        google_results = _google_site_search(driver, keywords)
        results.extend(google_results)

    except Exception as exc:
        logger.error("Error during scraping: %s", exc)
    finally:
        driver.quit()
        logger.info("Browser closed. Total raw results: %d", len(results))

    return results


# ---------------------------------------------------------------------------
# Google site-search for older LinkedIn posts
# ---------------------------------------------------------------------------
def _google_site_search(
    driver,
    keywords: List[str],
) -> List[Dict[str, str]]:
    """
    Use Google search with site:linkedin.com to find LinkedIn posts
    older than 1 month (which LinkedIn's native search doesn't return).

    Searches in monthly windows from 2 months ago to 6 months ago.
    Extracts LinkedIn post URLs from Google results.
    Then visits each post on LinkedIn to extract the full text.
    """
    from datetime import datetime, timedelta
    import urllib.parse

    results = []
    now = datetime.now()
    all_post_urls: Dict[str, str] = {}  # url -> keyword

    for kw in keywords:
        # Search in monthly windows: months 2-6
        for months_back in range(2, 7):
            date_max = now - timedelta(days=(months_back - 1) * 30)
            date_min = now - timedelta(days=months_back * 30)
            date_min_str = date_min.strftime("%m/%d/%Y")
            date_max_str = date_max.strftime("%m/%d/%Y")

            query = urllib.parse.quote(f'"{kw}"')
            google_url = GOOGLE_SEARCH_URL.format(
                query=query,
                date_min=date_min_str,
                date_max=date_max_str,
            )

            logger.info(
                "Google search: '%s' (%s to %s)",
                kw, date_min_str, date_max_str,
            )

            try:
                driver.get(google_url)
                _random_sleep(3, 5)

                # Extract LinkedIn URLs from Google results
                urls = driver.execute_script("""
                    var links = [];
                    var seen = {};
                    document.querySelectorAll('a').forEach(function(a) {
                        var href = a.href || '';
                        if (href.indexOf('linkedin.com/feed/update/') >= 0 ||
                            href.indexOf('linkedin.com/posts/') >= 0) {
                            // Clean tracking params
                            var clean = href.split('&')[0];
                            if (clean.indexOf('url=') >= 0) {
                                // Google wraps URLs — extract the actual URL
                                var m = clean.match(/url=(https[^&]+)/);
                                if (m) clean = decodeURIComponent(m[1]);
                            }
                            clean = clean.split('?')[0];
                            if (clean.indexOf('linkedin.com') >= 0 && !seen[clean]) {
                                seen[clean] = true;
                                links.push(clean);
                            }
                        }
                    });
                    return links;
                """)

                for url in (urls or []):
                    if url not in all_post_urls:
                        all_post_urls[url] = kw
                        logger.info("  Found: %s", url)

            except Exception as exc:
                logger.warning("Google search failed for '%s' month %d: %s", kw, months_back, exc)
                continue

            _random_sleep(2, 4)

    # Now visit each discovered LinkedIn post to extract full text
    logger.info("Found %d older LinkedIn posts via Google. Extracting text…", len(all_post_urls))

    for url, kw in all_post_urls.items():
        try:
            driver.get(url)
            _random_sleep(3, 5)

            # Extract post text from the individual post page
            post_data = driver.execute_script("""
                var main = document.querySelector('main') || document.body;
                var text = main.innerText || '';
                return text;
            """)

            if post_data and len(post_data) > 50:
                results.append({
                    "keyword": kw,
                    "text": post_data,
                    "post_url": url,
                    "html": "",
                    "search_url": url,
                    "post_urls": [url],
                })
                logger.info("  Extracted %d chars from %s", len(post_data), url)

        except Exception as exc:
            logger.warning("Failed to extract post %s: %s", url, exc)
            # Still add the URL even if text extraction fails
            results.append({
                "keyword": kw,
                "text": "",
                "post_url": url,
                "html": "",
                "search_url": url,
                "post_urls": [url],
            })

        _random_sleep(1, 3)

    return results


# ---------------------------------------------------------------------------
# CLI entry point (for standalone testing)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    posts = scrape_linkedin_posts(headless=False, max_scrolls=5)
    print(f"\nCollected {len(posts)} raw post containers.")
    for i, p in enumerate(posts[:3], 1):
        print(f"\n--- Post {i} (keyword={p['keyword']}) ---")
        print(p["html"][:300], "…")
