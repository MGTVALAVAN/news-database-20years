"""
GDELT (Global Database of Events, Language, and Tone) Scraper

This is the PRIMARY source for the 20-year historical database.
GDELT monitors news media in 100+ languages and identifies events,
people, organizations, themes, and emotions.

Two approaches:
1. GDELT DOC API (free, no auth needed) — for article-level data
2. GDELT BigQuery (1 TB/month free) — for massive event-level queries

The DOC API is simpler and sufficient for our needs.
"""

import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CSV_COLUMNS, REQUEST_DELAY_SECONDS


# ─── GDELT DOC 2.0 API ──────────────────────────────────────
GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"


def _classify_theme(themes_str):
    """Map GDELT themes to our category system."""
    if not themes_str:
        return "General", ""
    
    themes = themes_str.lower()
    
    mapping = {
        "Education": ["education", "school", "university", "student"],
        "Polity": ["election", "parliament", "legislation", "judicial", "court", "governance", "political"],
        "Economy": ["economic", "finance", "trade", "tax", "inflation", "gdp", "banking", "market"],
        "International Relations": ["diplomacy", "bilateral", "un_", "nato", "summit", "foreign_policy"],
        "Environment": ["environment", "climate", "pollution", "forest", "wildlife", "conservation"],
        "Science & Technology": ["technology", "science", "space", "cyber", "digital", "ai_", "biotech"],
        "Defence & Security": ["military", "terror", "security", "armed", "defense", "weapon"],
        "Social Issues": ["health", "poverty", "gender", "women", "child", "welfare", "population"],
        "History & Culture": ["culture", "heritage", "religion", "festival", "art_", "museum"],
    }
    
    for category, keywords in mapping.items():
        for kw in keywords:
            if kw in themes:
                return category, ""
    
    return "General", ""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=6, max=60))
def _query_gdelt_doc(query, start_date, end_date, max_records=250, mode="artlist"):
    """
    Query GDELT DOC 2.0 API.
    
    Args:
        query: Search query (supports boolean operators)
        start_date: Start date (YYYYMMDDHHMMSS format)
        end_date: End date (YYYYMMDDHHMMSS format)
        max_records: Max records (up to 250 per call)
        mode: 'artlist' for article list, 'timeline' for counts
    
    Returns:
        dict: API response
    """
    params = {
        "query": query,
        "mode": mode,
        "maxrecords": max_records,
        "startdatetime": start_date,
        "enddatetime": end_date,
        "format": "json",
        "sort": "datedesc",
        "sourcelang": "eng",
    }
    
    resp = requests.get(GDELT_DOC_API, params=params, timeout=30)
    if resp.status_code == 429:
        time.sleep(10)  # Extra wait on rate limit
        raise requests.exceptions.HTTPError("Rate limited (429)")
    resp.raise_for_status()
    return resp.json()


def fetch_gdelt_articles(query="India", start_date=None, end_date=None, max_records=250):
    """
    Fetch articles from GDELT for a given query and date range.
    
    Args:
        query: Search term (e.g., "India economy" or "ISRO space mission")
        start_date: Start date as string "YYYY-MM-DD"
        end_date: End date as string "YYYY-MM-DD"
        max_records: Max articles to return
    
    Returns:
        list of dicts matching CSV schema
    """
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Convert to GDELT format (YYYYMMDDHHMMSS)
    start_gdelt = start_date.replace("-", "") + "000000"
    end_gdelt = end_date.replace("-", "") + "235959"
    
    try:
        data = _query_gdelt_doc(query, start_gdelt, end_gdelt, max_records)
    except Exception as e:
        print(f"  ⚠️ GDELT API error for '{query}': {e}")
        return []
    
    articles = data.get("articles", [])
    news_items = []
    
    for art in articles:
        category, subcategory = _classify_theme(art.get("domain", ""))
        
        # Parse the date
        date_str = art.get("seendate", "")
        try:
            parsed_date = datetime.strptime(date_str[:8], "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            parsed_date = start_date
        
        item = {
            "date": parsed_date,
            "headline": art.get("title", "").strip(),
            "source": art.get("domain", "").replace("www.", ""),
            "category": category,
            "subcategory": subcategory,
            "summary": "",  # GDELT doesn't provide summaries
            "url": art.get("url", ""),
            "relevance": "",
            "region": "National" if "india" in query.lower() else "International",
            "keywords": query,
        }
        
        if item["headline"]:  # Skip empty headlines
            # Filter out non-English articles (CJK, Arabic, etc.)
            try:
                item["headline"].encode('ascii')
                is_english = True
            except UnicodeEncodeError:
                # Allow common accented chars but skip CJK/Arabic
                import unicodedata
                non_ascii = [c for c in item["headline"] if ord(c) > 127]
                is_english = all(
                    unicodedata.category(c).startswith(('L', 'P', 'Z', 'S'))
                    and ord(c) < 0x2000
                    for c in non_ascii
                ) if non_ascii else True
            
            if is_english:
                news_items.append(item)
    
    return news_items


def fetch_gdelt_historical(
    start_year=2005,
    end_year=2025,
    items_per_month=50,
    india_focus=True
):
    """
    Fetch historical news from GDELT across a multi-year range.
    
    Strategy: Query month-by-month with rotating topic queries
    to maximize coverage and diversity.
    
    Args:
        start_year: Start year (default 2005)
        end_year: End year (default 2025)
        items_per_month: Target items per month
        india_focus: Whether to focus on India-related news
    
    Returns:
        list of dicts matching CSV schema
    """
    # Topic queries for comprehensive UPSC coverage
    if india_focus:
        topic_queries = [
            "India parliament",
            "India economy",
            "India diplomacy",
            "India environment",
            "India technology",
            "India defence",
        ]
    else:
        topic_queries = [
            "world politics geopolitics",
            "global economy trade",
            "United Nations international",
            "climate change global warming",
            "science technology breakthrough",
        ]
    
    all_items = []
    seen_headlines = set()
    
    # Calculate total months for progress bar
    total_months = (end_year - start_year + 1) * 12
    
    print(f"\n{'='*60}")
    print(f"📚 GDELT Historical Fetch: {start_year} → {end_year}")
    print(f"   Target: ~{items_per_month} items/month × {total_months} months")
    print(f"   Estimated total: ~{items_per_month * total_months} items")
    print(f"{'='*60}\n")
    
    with tqdm(total=total_months, desc="Months processed") as pbar:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                # Don't go past current date
                if year == end_year and month > datetime.now().month:
                    pbar.update(1)
                    continue
                
                # Calculate start/end dates for this month
                start_date = f"{year}-{month:02d}-01"
                if month == 12:
                    end_date = f"{year}-12-31"
                else:
                    next_month = datetime(year, month + 1, 1) - timedelta(days=1)
                    end_date = next_month.strftime("%Y-%m-%d")
                
                month_items = []
                items_per_topic = max(items_per_month // len(topic_queries), 10)
                
                for query in topic_queries:
                    try:
                        articles = fetch_gdelt_articles(
                            query=query,
                            start_date=start_date,
                            end_date=end_date,
                            max_records=items_per_topic
                        )
                        
                        for item in articles:
                            headline_key = item['headline'].lower().strip()[:80]
                            if headline_key and headline_key not in seen_headlines:
                                seen_headlines.add(headline_key)
                                month_items.append(item)
                        
                        time.sleep(6)  # GDELT requires min 5s between requests
                        
                    except Exception as e:
                        print(f"\n  ⚠️ Error on {year}-{month:02d} [{query}]: {e}")
                        time.sleep(5)
                
                all_items.extend(month_items)
                pbar.set_postfix({
                    "year": year,
                    "month": month,
                    "month_items": len(month_items),
                    "total": len(all_items)
                })
                pbar.update(1)
    
    print(f"\n✅ GDELT Historical Fetch Complete: {len(all_items)} total items")
    return all_items


def save_to_csv(news_items, filename):
    """Save news items to CSV."""
    df = pd.DataFrame(news_items, columns=CSV_COLUMNS)
    # Sort by date descending
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.sort_values('date', ascending=False).reset_index(drop=True)
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"💾 Saved {len(df)} items to {filename}")
    return df


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="GDELT Historical News Scraper")
    parser.add_argument("--start-year", type=int, default=2005)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--items-per-month", type=int, default=50)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    
    items = fetch_gdelt_historical(
        start_year=args.start_year,
        end_year=args.end_year,
        items_per_month=args.items_per_month,
    )
    
    out = args.output or os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "outputs", "news_database_full.csv"
    )
    os.makedirs(os.path.dirname(out), exist_ok=True)
    save_to_csv(items, out)
