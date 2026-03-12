"""  
dashboard.py — Streamlit dashboard for LinkedIn Mention Intelligence.

Displays:
  • KPI cards  (Total posts, Adya AI mentions, Shayak Mazumder mentions, Unique Authors, Dated Posts)
  • Weekly Timeline of mentions
  • Topic Segments pie chart
  • Top 10 Authors bar chart
  • Full data table with filters
  • Post links

Launch:
    streamlit run dashboard.py
"""

import os
import re
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "linkedin_mentions.csv")

st.set_page_config(
    page_title="LinkedIn Mention Intelligence — Adya AI",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS for a polished look
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    /* Hide default Streamlit header */
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Clean metric styling */
    [data-testid="metric-container"] {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        padding: 15px;
        border-radius: 8px;
    }
    [data-testid="metric-container"] label {
        color: #6c757d;
        font-size: 0.85rem;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #212529;
        font-size: 2rem;
        font-weight: 600;
    }
    
    .stDataFrame { font-size: 0.85rem; }
    
    /* Sidebar styling */
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
    
    /* Section headers */
    h2 {
        font-weight: 500;
        color: #333;
        margin-top: 1.5rem;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Segment definitions  (keyword → category)
# ---------------------------------------------------------------------------
SEGMENT_RULES: dict[str, list[str]] = {
    "Fundraising": [
        "fundrais", "funding", "raised", "investment", "investor",
        "series a", "series b", "series c", "seed round", "pre-seed",
        "venture capital", "valuation", "ipo", "capital raise",
    ],
    "Tech / Product": [
        "launch lab", "product launch", "feature", "platform",
        "built", "engineering", "tech stack", "open source", "api",
        "llm", "machine learning", "deep learning", "model",
        "software", "saas", "deploy", "cloud", "infra",
        "generative ai", "gen ai", "genai", "automation",
    ],
    "Hiring": [
        "hiring", "job", "new position", "career", "recruit",
        "role", "vacancy", "looking for", "join our team",
        "open position", "we are hiring", "we're hiring",
        "starting a new position", "talent",
    ],
    "Partnerships": [
        "partnership", "collaborat", "partner", "mou",
        "tie-up", "tie up", "alliance", "joint venture",
    ],
    "Events": [
        "event", "conference", "summit", "webinar", "meetup",
        "workshop", "hackathon", "inaugurate", "bootcamp",
        "demo day", "buildathon",
    ],
    "Awards / Recognition": [
        "award", "recognition", "achievement", "honoured",
        "honored", "proud", "winner", "accolade", "felicitat",
    ],
    "Education / Campus": [
        "student", "campus", "institute", "university",
        "college", "learning", "training", "intern",
        "placement", "curriculum", "course",
    ],
}


def _classify_segment(text: str) -> str:
    """Return the best-matching segment for *text*, or 'Other'."""
    if not isinstance(text, str):
        return "Other"
    lower = text.lower()
    scores: dict[str, int] = {}
    for segment, keywords in SEGMENT_RULES.items():
        hits = sum(1 for kw in keywords if kw in lower)
        if hits:
            scores[segment] = hits
    if not scores:
        return "Other"
    return max(scores, key=scores.get)  # type: ignore[arg-type]


@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    """Load the CSV produced by pipeline.py."""
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    # Ensure boolean columns
    for col in ("mentions_shayak", "mentions_adya"):
        if col in df.columns:
            df[col] = df[col].astype(bool)
    # Add segment column
    if "post_text" in df.columns:
        df["segment"] = df["post_text"].apply(_classify_segment)
    else:
        df["segment"] = "Other"
    return df


df = load_data()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("LinkedIn Monitor")
st.sidebar.markdown("**Mention Intelligence — Adya AI**")
st.sidebar.markdown("---")

if df.empty:
    st.sidebar.warning("No data found. Run the pipeline first:\n```\npython pipeline.py\n```")

# Filters
if not df.empty:
    st.sidebar.subheader("Filters")

    # Mention type filter
    st.sidebar.markdown("**Mention type**")
    mention_type = st.sidebar.radio(
        "Mention type",
        ["All", "Shayak Mazumder only", "Adya AI only"],
        label_visibility="collapsed",
    )

    # Segment filter (dropdown)
    all_segments = sorted(df["segment"].unique().tolist()) if "segment" in df.columns else []
    st.sidebar.markdown("**Topic segment**")
    segment_filter = st.sidebar.selectbox(
        "Topic segment",
        options=["All"] + all_segments,
        label_visibility="collapsed",
    )

    # Search post text
    st.sidebar.markdown("**Search post text**")
    search_text = st.sidebar.text_input(
        "Search post text",
        placeholder="e.g. agentic, hiring...",
        label_visibility="collapsed",
    )

    # Date range filter
    st.sidebar.markdown("**Date range**")
    if "date_parsed" in df.columns:
        df["_date"] = pd.to_datetime(df["date_parsed"], errors="coerce")
        valid_dates = df[df["_date"].notna()]
        if not valid_dates.empty:
            min_date = valid_dates["_date"].min().date()
            max_date = valid_dates["_date"].max().date()
        else:
            min_date = datetime.now().date() - timedelta(days=180)
            max_date = datetime.now().date()
    else:
        min_date = datetime.now().date() - timedelta(days=180)
        max_date = datetime.now().date()
    
    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        label_visibility="collapsed",
    )

    # Apply filters
    filtered = df.copy()
    
    # Mention type filter
    if mention_type == "Shayak Mazumder only" and "mentions_shayak" in filtered.columns:
        filtered = filtered[filtered["mentions_shayak"] == True]
    elif mention_type == "Adya AI only" and "mentions_adya" in filtered.columns:
        filtered = filtered[filtered["mentions_adya"] == True]

    # Segment filter
    if segment_filter != "All" and "segment" in filtered.columns:
        filtered = filtered[filtered["segment"] == segment_filter]

    # Text search filter
    if search_text and "post_text" in filtered.columns:
        filtered = filtered[filtered["post_text"].str.contains(search_text, case=False, na=False)]

    # Date range filter
    if "date_parsed" in filtered.columns and len(date_range) == 2:
        filtered["_date"] = pd.to_datetime(filtered["date_parsed"], errors="coerce")
        start_date, end_date = date_range
        filtered = filtered[
            (filtered["_date"].dt.date >= start_date) & 
            (filtered["_date"].dt.date <= end_date)
        ]
else:
    filtered = df

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("LinkedIn Mention Intelligence")

# Calculate date range for subtitle
if "date_parsed" in df.columns:
    df_dates = pd.to_datetime(df["date_parsed"], errors="coerce")
    valid_dates = df_dates.dropna()
    if not valid_dates.empty:
        date_diff = (valid_dates.max() - valid_dates.min()).days
        months = max(1, date_diff // 30)
        time_period = f"last {months} months"
    else:
        time_period = "all time"
else:
    time_period = "all time"

st.markdown(f"Tracking **Shayak Mazumder** & **Adya AI** — {time_period} | **{len(df)} posts**")
st.markdown("---")

if df.empty:
    st.info(
        "**No data available yet.**\n\n"
        "Run the pipeline to start collecting mentions:\n"
        "```bash\n"
        "python pipeline.py\n"
        "```\n"
        "Then refresh this page."
    )
    st.stop()

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)

total_posts = len(filtered)
shayak_mentions = filtered["mentions_shayak"].sum() if "mentions_shayak" in filtered.columns else 0
adya_mentions = filtered["mentions_adya"].sum() if "mentions_adya" in filtered.columns else 0
unique_authors = filtered["author"].nunique() if "author" in filtered.columns else 0

# Count posts with valid dates
if "date_parsed" in filtered.columns:
    dated_posts = filtered[pd.to_datetime(filtered["date_parsed"], errors="coerce").notna()].shape[0]
else:
    dated_posts = 0

with col1:
    st.metric("Total Posts", total_posts)
with col2:
    st.metric("Shayak Mentions", int(shayak_mentions))
with col3:
    st.metric("Adya AI Mentions", int(adya_mentions))
with col4:
    st.metric("Unique Authors", unique_authors)
with col5:
    st.metric("Dated Posts", dated_posts)

st.markdown("---")

# ---------------------------------------------------------------------------
# Weekly Timeline and Topic Segments (side by side)
# ---------------------------------------------------------------------------
chart_col1, chart_col2 = st.columns([1.5, 1])

with chart_col1:
    st.subheader("Weekly Timeline")
    if "date_parsed" in filtered.columns:
        timeline_df = filtered[filtered["date_parsed"].notna() & (filtered["date_parsed"] != "")].copy()
        if not timeline_df.empty:
            timeline_df["date"] = pd.to_datetime(timeline_df["date_parsed"], errors="coerce")
            timeline_df = timeline_df.dropna(subset=["date"])
            if not timeline_df.empty:
                # Group by week
                timeline_df["week"] = timeline_df["date"].dt.to_period("W").apply(lambda x: x.start_time)
                
                # Total counts per week
                weekly_total = timeline_df.groupby("week").size().reset_index(name="Total")
                
                # Shayak counts per week
                if "mentions_shayak" in timeline_df.columns:
                    weekly_shayak = timeline_df[timeline_df["mentions_shayak"] == True].groupby("week").size().reset_index(name="Shayak")
                else:
                    weekly_shayak = pd.DataFrame(columns=["week", "Shayak"])
                
                # Adya AI counts per week
                if "mentions_adya" in timeline_df.columns:
                    weekly_adya = timeline_df[timeline_df["mentions_adya"] == True].groupby("week").size().reset_index(name="Adya AI")
                else:
                    weekly_adya = pd.DataFrame(columns=["week", "Adya AI"])
                
                # Merge all
                weekly = weekly_total.merge(weekly_shayak, on="week", how="left").merge(weekly_adya, on="week", how="left")
                weekly = weekly.fillna(0)
                
                # Create line chart with Plotly
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=weekly["week"], y=weekly["Total"], mode="lines+markers", name="Total", line=dict(color="#1f77b4")))
                fig.add_trace(go.Scatter(x=weekly["week"], y=weekly["Shayak"], mode="lines+markers", name="Shayak", line=dict(color="#ff7f0e")))
                fig.add_trace(go.Scatter(x=weekly["week"], y=weekly["Adya AI"], mode="lines+markers", name="Adya AI", line=dict(color="#2ca02c")))
                
                fig.update_layout(
                    xaxis_title="",
                    yaxis_title="Posts",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    margin=dict(l=0, r=0, t=30, b=0),
                    height=350,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No valid dates to plot.")
        else:
            st.info("No date data available for timeline.")
    else:
        st.info("No date column in data.")

with chart_col2:
    st.subheader("Topic Segments")
    if "segment" in filtered.columns:
        seg_counts = filtered["segment"].value_counts().reset_index()
        seg_counts.columns = ["Segment", "Count"]
        
        # Create pie chart with Plotly
        colors = px.colors.qualitative.Pastel
        fig = px.pie(
            seg_counts, 
            values="Count", 
            names="Segment",
            color_discrete_sequence=colors,
            hole=0.4,  # Donut chart style like in the image
        )
        fig.update_traces(textposition="outside", textinfo="percent+label")
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.05),
            margin=dict(l=0, r=0, t=30, b=0),
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No segment data available.")

st.markdown("---")

# ---------------------------------------------------------------------------
# Top 10 Authors
# ---------------------------------------------------------------------------
st.subheader("Top 10 Authors")
if "author" in filtered.columns:
    author_counts = filtered["author"].value_counts().head(10).reset_index()
    author_counts.columns = ["Author", "Posts"]
    
    # Horizontal bar chart
    fig = px.bar(
        author_counts,
        x="Posts",
        y="Author",
        orientation="h",
        color_discrete_sequence=["#667eea"],
    )
    fig.update_layout(
        yaxis=dict(categoryorder="total ascending"),
        xaxis_title="",
        yaxis_title="",
        margin=dict(l=0, r=0, t=10, b=0),
        height=300,
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No author data available.")

st.markdown("---")

# ---------------------------------------------------------------------------
# Detailed post cards
# ---------------------------------------------------------------------------
st.subheader("Post Details")

for idx, row in filtered.iterrows():
    with st.expander(
        f"**{row.get('author', 'Unknown')}** — {row.get('date_parsed', 'N/A')}"
    ):
        # Post text
        post_text = row.get("post_text", "")
        if len(post_text) > 500:
            st.markdown(f"**Post:** {post_text[:500]}…")
        else:
            st.markdown(f"**Post:** {post_text}")

        # Mention badges
        badges = []
        if row.get("mentions_shayak"):
            badges.append("Shayak Mazumder")
        if row.get("mentions_adya"):
            badges.append("Adya AI")
        if badges:
            st.markdown("**Mentions:** " + " · ".join(badges))

        # Segment badge
        seg = row.get("segment", "")
        if seg:
            st.markdown(f"**Segment:** {seg}")

        # Link — use actual URL if valid, otherwise link to a LinkedIn search
        url = row.get("post_url", "")
        author = row.get("author", "")
        keyword = row.get("keyword", "")
        if pd.notna(url) and isinstance(url, str) and url.startswith("http") and "1234567" not in url:
            st.markdown(f"[View on LinkedIn]({url})")
        else:
            import urllib.parse
            # Build a search query from keyword + author for best results
            parts = []
            if keyword:
                parts.append(str(keyword))
            if author and author not in ("Unknown", "Edited", ""):
                parts.append(str(author))
            if not parts:
                parts.append("Adya AI")
            search_q = urllib.parse.quote(" ".join(parts))
            search_url = f"https://www.linkedin.com/search/results/content/?keywords={search_q}&origin=GLOBAL_SEARCH_HEADER&sortBy=%22date_posted%22"
            st.markdown(f"[Search on LinkedIn]({search_url})")

st.markdown("---")

# ---------------------------------------------------------------------------
# Full data table
# ---------------------------------------------------------------------------
st.subheader("Full Data Table")

display_cols = [
    c for c in [
        "author", "date_parsed", "segment", "keyword", "mentions_shayak", "mentions_adya",
        "post_url",
    ]
    if c in filtered.columns
]

if display_cols:
    st.dataframe(
        filtered[display_cols].reset_index(drop=True),
        width="stretch",
        height=400,
    )

# ---------------------------------------------------------------------------
# Download button
# ---------------------------------------------------------------------------
st.sidebar.markdown("---")
if not filtered.empty:
    csv_data = filtered.to_csv(index=False)
    st.sidebar.download_button(
        label="Download filtered CSV",
        data=csv_data,
        file_name="linkedin_mentions_export.csv",
        mime="text/csv",
    )

# Footer with timestamp
st.sidebar.markdown("---")
if os.path.exists(CSV_PATH):
    mod_time = datetime.fromtimestamp(os.path.getmtime(CSV_PATH))
    st.sidebar.markdown(
        f"*Data updated: {mod_time.strftime('%d %b %Y, %H:%M')}*"
    )
