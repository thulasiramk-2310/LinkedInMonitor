# LinkedIn Mention Intelligence

Track and analyze LinkedIn posts mentioning Shayak Mazumder and Adya AI.

## Overview

This tool scrapes LinkedIn posts, categorizes them by topic segment (Fundraising, Tech/Product, Hiring, Partnerships, Events, Awards, Education), and displays analytics in an interactive Streamlit dashboard.

## 🚀 Live Dashboard

**View the live dashboard:** https://linkedinmonitor.streamlit.app/

The dashboard is deployed on Streamlit Cloud and displays real-time analytics of collected LinkedIn mentions.

## Prerequisites

- Python 3.9 or higher
- Google Chrome browser
- LinkedIn account
- Google Gemini API key (optional, for AI-powered analysis)

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/linkedinmonitor.git
cd linkedinmonitor
```

### 2. Create virtual environment (recommended)

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
LINKEDIN_EMAIL=your_linkedin_email@example.com
LINKEDIN_PASSWORD=your_linkedin_password
GEMINI_API_KEY=your_gemini_api_key_here
```

## Project Structure

```
linkedinmonitor/
│
├── dashboard.py        # Streamlit web dashboard
├── pipeline.py         # Main orchestrator script
├── scraper.py          # Selenium-based LinkedIn scraper
├── extractor.py        # Text parsing and data extraction
├── ai_analysis.py      # Gemini AI integration (optional)
│
├── data/
│   └── linkedin_mentions.csv   # Collected data (auto-generated)
│
├── .streamlit/
│   └── secrets.toml.example    # Streamlit Cloud secrets template
│
├── .env.example        # Environment variables template
├── .gitignore
├── requirements.txt
└── README.md
```

### Module Descriptions

| File | Purpose |
|------|---------|
| `pipeline.py` | Entry point. Orchestrates scraping, extraction, and saves to CSV |
| `scraper.py` | Uses Selenium to log into LinkedIn and scrape search results |
| `extractor.py` | Parses raw HTML/text into structured records (author, date, text) |
| `dashboard.py` | Streamlit app for visualizing and filtering the collected data |
| `ai_analysis.py` | Optional Gemini AI integration for enhanced text classification |

## Usage

### Running the Pipeline

Scrape LinkedIn posts and save to CSV:

```bash
# Basic run (headless browser)
python pipeline.py

# With visible browser (required for solving captchas)
python pipeline.py --headless false

# With more scrolling for larger datasets
python pipeline.py --headless false --max-scrolls 50

# Re-extract data without scraping again
python pipeline.py --skip-scrape
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--headless` | `true` | Run browser in headless mode |
| `--max-scrolls` | `30` | Number of scroll actions per search |
| `--skip-scrape` | `false` | Skip scraping, only re-process existing data |

### Running the Dashboard

```bash
streamlit run dashboard.py
```

Open http://localhost:8501 in your browser.

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `LINKEDIN_EMAIL` | Yes | LinkedIn account email |
| `LINKEDIN_PASSWORD` | Yes | LinkedIn account password |
| `GEMINI_API_KEY` | No | Google Gemini API key for AI analysis |

### Topic Segments

Posts are automatically categorized based on keyword matching:

| Segment | Example Keywords |
|---------|------------------|
| Fundraising | funding, raised, investment, series a/b/c, seed round |
| Tech/Product | launch, product, platform, LLM, machine learning, SaaS |
| Hiring | hiring, job, career, recruit, open position |
| Partnerships | partnership, collaboration, alliance, joint venture |
| Events | conference, summit, webinar, hackathon, workshop |
| Awards/Recognition | award, achievement, winner, honored |
| Education/Campus | student, campus, university, intern, training |

## Dashboard Features

- **KPI Cards**: Total posts, Shayak mentions, Adya AI mentions, unique authors, dated posts
- **Weekly Timeline**: Multi-line chart tracking mention trends over time
- **Topic Segments**: Donut chart showing distribution by category
- **Top 10 Authors**: Horizontal bar chart of most active posters
- **Filters**: Mention type, topic segment, text search, date range
- **Post Details**: Expandable cards with full text and LinkedIn links
- **Export**: Download filtered data as CSV

## Deployment to Streamlit Cloud

The dashboard can be deployed to Streamlit Community Cloud for public access.

### Step 1: Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/your-username/linkedinmonitor.git
git push -u origin main
```

### Step 2: Deploy on Streamlit Cloud

1. Go to https://share.streamlit.io
2. Sign in with GitHub
3. Click "New app"
4. Select your repository
5. Set main file path to `dashboard.py`
6. Click "Deploy"

### Updating Data

The scraper runs locally. To update the cloud dashboard:

```bash
# Run pipeline locally
python pipeline.py --headless false --max-scrolls 50

# Commit and push updated data
git add data/linkedin_mentions.csv
git commit -m "Update mention data"
git push
```

Streamlit Cloud will auto-refresh with the new data.

## Data Format

The CSV output contains these columns:

| Column | Type | Description |
|--------|------|-------------|
| `author` | string | LinkedIn username of the poster |
| `post_text` | string | Full text content of the post |
| `date_parsed` | string | Extracted date (YYYY-MM-DD) |
| `keyword` | string | Search keyword that found this post |
| `mentions_shayak` | boolean | Contains Shayak Mazumder mention |
| `mentions_adya` | boolean | Contains Adya AI mention |
| `post_url` | string | Direct link to the LinkedIn post |
| `segment` | string | Auto-assigned topic category |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Login fails / Captcha required | Run with `--headless false` and solve captcha manually |
| Rate limited by LinkedIn | Wait 24 hours before retrying |
| No data in dashboard | Run `python pipeline.py` first to populate the CSV |
| Missing dates on posts | Some LinkedIn posts don't show dates; check "Dated Posts" count |
| Chrome not found | Install Google Chrome or set `CHROME_PATH` environment variable |
