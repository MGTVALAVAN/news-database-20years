"""
Google News RSS Scraper
Fetches news items from Google News RSS feeds with date filtering.
Supports: past 24h, past week, custom date ranges (up to ~30 days back).
"""

import feedparser
import requests
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pandas as pd
from tqdm import tqdm

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CSV_COLUMNS, REQUEST_DELAY_SECONDS


# Google News RSS base URL
GOOGLE_NEWS_RSS = "https://news.google.com/rss"
GOOGLE_NEWS_SEARCH_RSS = "https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"


def _parse_google_date(date_str):
    """Parse Google News RSS date format."""
    try:
        return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")
    except Exception:
        try:
            return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z").strftime("%Y-%m-%d")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d")


def _extract_real_url(google_url):
    """Extract the actual article URL from Google News redirect URL."""
    # Google News URLs are redirects; try to get the real URL
    try:
        if "news.google.com" in google_url:
            # Sometimes the URL is encoded in the link
            resp = requests.get(google_url, allow_redirects=True, timeout=10)
            return resp.url
    except Exception:
        pass
    return google_url


def _extract_source(entry):
    """Extract source name from RSS entry."""
    if hasattr(entry, 'source') and hasattr(entry.source, 'title'):
        return entry.source.title
    # Try to extract from the title (Google News format: "headline - Source")
    if ' - ' in entry.get('title', ''):
        return entry['title'].rsplit(' - ', 1)[-1].strip()
    return "Unknown"


def _extract_headline(entry):
    """Extract clean headline from RSS entry."""
    title = entry.get('title', '')
    # Google News often appends " - Source" at the end
    if ' - ' in title:
        return title.rsplit(' - ', 1)[0].strip()
    return title.strip()


def _extract_summary(entry):
    """Extract summary from RSS entry description."""
    desc = entry.get('description', '') or entry.get('summary', '')
    if desc:
        # Remove HTML tags
        soup = BeautifulSoup(desc, 'html.parser')
        text = soup.get_text(separator=' ')
        # Limit to ~200 chars
        if len(text) > 200:
            text = text[:200].rsplit(' ', 1)[0] + '...'
        return text.strip()
    return ""


def fetch_top_news(max_items=100):
    """
    Fetch top news from Google News India RSS feed.
    Returns a list of dicts matching CSV_COLUMNS schema.
    """
    print(f"📰 Fetching top {max_items} news items from Google News...")
    
    feed = feedparser.parse(f"{GOOGLE_NEWS_RSS}?hl=en-IN&gl=IN&ceid=IN:en")
    
    news_items = []
    for entry in tqdm(feed.entries[:max_items], desc="Processing"):
        item = {
            "date": _parse_google_date(entry.get('published', '')),
            "headline": _extract_headline(entry),
            "source": _extract_source(entry),
            "category": "",      # Will be filled by categorizer
            "subcategory": "",
            "summary": _extract_summary(entry),
            "url": entry.get('link', ''),
            "relevance": "",
            "region": "",
            "keywords": "",
        }
        news_items.append(item)
        time.sleep(0.1)  # Gentle rate limiting
    
    print(f"✅ Fetched {len(news_items)} items")
    return news_items


def search_news_by_query(query, max_items=100):
    """
    Search Google News RSS by query string.
    """
    print(f"🔍 Searching Google News for: {query}")
    
    url = GOOGLE_NEWS_SEARCH_RSS.format(query=quote_plus(query))
    feed = feedparser.parse(url)
    
    news_items = []
    for entry in feed.entries[:max_items]:
        item = {
            "date": _parse_google_date(entry.get('published', '')),
            "headline": _extract_headline(entry),
            "source": _extract_source(entry),
            "category": "",
            "subcategory": "",
            "summary": _extract_summary(entry),
            "url": entry.get('link', ''),
            "relevance": "",
            "region": "",
            "keywords": "",
        }
        news_items.append(item)
    
    return news_items


def fetch_news_by_topics(topics=None, max_per_topic=50):
    """
    Fetch news across multiple UPSC-relevant topics.
    Uses Google News RSS search to get broader coverage.
    """
    if topics is None:
        topics = [
            "India politics parliament",
            "India economy budget GDP",
            "India Supreme Court judiciary",
            "India foreign policy international relations",
            "India environment climate",
            "India science technology ISRO",
            "India defence military",
            "India education health social",
            "India infrastructure development",
            "world affairs geopolitics",
        ]
    
    all_items = []
    seen_headlines = set()
    
    for topic in tqdm(topics, desc="Scraping topics"):
        items = search_news_by_query(topic, max_items=max_per_topic)
        for item in items:
            # Deduplicate by headline
            headline_key = item['headline'].lower().strip()
            if headline_key not in seen_headlines:
                seen_headlines.add(headline_key)
                all_items.append(item)
        
        time.sleep(REQUEST_DELAY_SECONDS)
    
    print(f"✅ Total unique items across all topics: {len(all_items)}")
    return all_items


def fetch_past_week_news(target_rows=700):
    """
    Fetch ~700 news items covering the past week.
    Uses multiple topic-based searches across different days.
    """
    print(f"📅 Fetching past week's news (target: {target_rows} rows)...")
    
    # Extended topic list for broader coverage
    topics = [
        # Core UPSC topics
        "India parliament legislation bill",
        "Supreme Court verdict judgment India",
        "India economy RBI monetary policy",
        "India budget fiscal policy tax",
        "India foreign affairs bilateral summit",
        "India defence procurement military exercise",
        "India environment pollution conservation",
        "ISRO space mission satellite India",
        "India education NEP university",
        "India healthcare policy scheme",
        "India infrastructure railway highway",
        "India election commission political party",
        # Specific recent news areas
        "India trade agreement WTO",
        "India climate change renewable energy",
        "India digital technology AI startup",
        "India agriculture MSP farmer",
        "India social welfare scheme pension",
        "India state elections results",
        "India cultural heritage UNESCO",
        "world geopolitics conflict diplomacy",
    ]
    
    # Add date-specific queries for past 7 days
    date_queries = []
    for i in range(7):
        date = datetime.now() - timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        date_queries.append(f"India news {date_str}")
    
    all_topics = topics + date_queries
    
    all_items = []
    seen_headlines = set()
    
    for topic in tqdm(all_topics, desc="Scraping past week"):
        items = search_news_by_query(topic, max_items=50)
        for item in items:
            headline_key = item['headline'].lower().strip()
            if headline_key not in seen_headlines:
                seen_headlines.add(headline_key)
                all_items.append(item)
        
        time.sleep(REQUEST_DELAY_SECONDS)
        
        if len(all_items) >= target_rows:
            break
    
    # Also get top news
    top = fetch_top_news(max_items=100)
    for item in top:
        headline_key = item['headline'].lower().strip()
        if headline_key not in seen_headlines:
            seen_headlines.add(headline_key)
            all_items.append(item)
    
    print(f"✅ Total items for past week: {len(all_items)}")
    return all_items[:target_rows]


def save_to_csv(news_items, filename):
    """Save news items to CSV."""
    df = pd.DataFrame(news_items, columns=CSV_COLUMNS)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"💾 Saved {len(df)} items to {filename}")
    return df


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Google News RSS Scraper")
    parser.add_argument("--mode", choices=["top100", "week"], default="top100")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    
    if args.mode == "top100":
        items = fetch_top_news(100)
        out = args.output or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "outputs", "news_top100.csv"
        )
    else:
        items = fetch_past_week_news(700)
        out = args.output or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "outputs", "news_past_week.csv"
        )
    
    os.makedirs(os.path.dirname(out), exist_ok=True)
    save_to_csv(items, out)
