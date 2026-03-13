"""
Wikipedia Current Events Scraper

Wikipedia's "Portal:Current events" page provides curated daily summaries
of significant world events. This is available from ~2004 onwards, making
it an excellent source for filling gaps in the 20-year database.

This scraper parses the monthly archive pages to extract individual events.
"""

import requests
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CSV_COLUMNS, REQUEST_DELAY_SECONDS


WIKI_EVENTS_URL = "https://en.wikipedia.org/wiki/Portal:Current_events/{month}_{year}"
WIKI_API = "https://en.wikipedia.org/api/rest_v1/page/html/Portal:Current_events/{month}_{year}"

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]


def _categorize_event(text):
    """Simple keyword-based categorization of Wikipedia events."""
    text_lower = text.lower()
    
    categories = {
        "Armed conflicts": ("Defence & Security", "Armed Conflict"),
        "Disasters": ("Environment", "Disasters"),
        "International relations": ("International Relations", "Diplomacy"),
        "Law and crime": ("Polity", "Law & Judiciary"),
        "Politics and elections": ("Polity", "Elections & Governance"),
        "Science and technology": ("Science & Technology", "Research"),
        "Business and economy": ("Economy", "Business"),
        "Arts and culture": ("History & Culture", "Arts"),
        "Sports": ("Sports", ""),
        "Health": ("Social Issues", "Health"),
        "Environment": ("Environment", "Climate & Ecology"),
    }
    
    for key, (cat, subcat) in categories.items():
        if key.lower() in text_lower:
            return cat, subcat
    
    # Keyword fallback
    if any(w in text_lower for w in ["india", "indian", "delhi", "mumbai"]):
        return "General", "India"
    if any(w in text_lower for w in ["election", "vote", "parliament", "president"]):
        return "Polity", ""
    if any(w in text_lower for w in ["economy", "gdp", "trade", "oil", "market"]):
        return "Economy", ""
    if any(w in text_lower for w in ["war", "military", "attack", "bomb", "missile"]):
        return "Defence & Security", ""
    
    return "General", ""


def _detect_region(text):
    """Detect if event is India-related, international, etc."""
    india_keywords = [
        "india", "indian", "delhi", "mumbai", "kolkata", "chennai",
        "bengaluru", "hyderabad", "modi", "bjp", "congress", "lok sabha",
        "rajya sabha", "supreme court of india", "isro",
    ]
    text_lower = text.lower()
    
    if any(kw in text_lower for kw in india_keywords):
        return "National"
    return "International"


def fetch_wikipedia_events_month(year, month):
    """
    Fetch all events from Wikipedia's Current Events for a specific month.
    
    Args:
        year: Year (e.g., 2020)
        month: Month number (1-12)
    
    Returns:
        list of dicts matching CSV schema
    """
    month_name = MONTHS[month - 1]
    url = f"https://en.wikipedia.org/wiki/Portal:Current_events/{month_name}_{year}"
    
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "NewsDatabase20Years/1.0 (Educational Research Project)"
        })
        if resp.status_code != 200:
            return []
    except Exception as e:
        print(f"  ⚠️ Failed to fetch {month_name} {year}: {e}")
        return []
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    events = []
    current_date = None
    current_category = "General"
    current_subcategory = ""
    
    # Look for date headings and event items
    # Wikipedia Current Events uses a specific structure
    for element in soup.find_all(['h3', 'h4', 'dl', 'li', 'p', 'div']):
        text = element.get_text(strip=True)
        
        # Try to detect date headers (e.g., "March 15, 2024")
        date_match = re.search(
            r'(\w+ \d{1,2},?\s*\d{4})',
            text
        )
        if date_match and element.name in ['h3', 'h4', 'div']:
            try:
                for fmt in ["%B %d, %Y", "%B %d %Y", "%d %B %Y"]:
                    try:
                        current_date = datetime.strptime(date_match.group(1), fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
            except Exception:
                pass
        
        # Detect category sections
        category_match = re.search(
            r'(Armed conflicts|Disasters|International relations|Law and crime|'
            r'Politics and elections|Science and technology|Business and economy|'
            r'Arts and culture|Sports|Health|Environment)',
            text, re.IGNORECASE
        )
        if category_match:
            current_category, current_subcategory = _categorize_event(category_match.group(1))
        
        # Extract event items (usually in <li> tags)
        if element.name == 'li' and len(text) > 30 and current_date:
            # Clean up the text
            headline = text[:200].strip()
            if headline.endswith('.'):
                headline = headline[:-1]
            
            # Extract links for URL
            link = element.find('a', href=True)
            url = ""
            if link and link.get('href', '').startswith('/wiki/'):
                url = f"https://en.wikipedia.org{link['href']}"
            
            # Extract keywords from links
            keywords = []
            for a_tag in element.find_all('a', href=True):
                if a_tag.get('href', '').startswith('/wiki/'):
                    keywords.append(a_tag.get_text(strip=True))
            
            region = _detect_region(text)
            
            item = {
                "date": current_date,
                "headline": headline,
                "source": "Wikipedia Current Events",
                "category": current_category,
                "subcategory": current_subcategory,
                "summary": text[:300] if len(text) > 200 else "",
                "url": url,
                "relevance": "",
                "region": region,
                "keywords": ", ".join(keywords[:5]),
            }
            events.append(item)
    
    return events


def fetch_wikipedia_events_range(start_year=2005, end_year=2025):
    """
    Fetch all Wikipedia Current Events from start_year to end_year.
    
    Returns:
        list of dicts matching CSV schema
    """
    all_events = []
    total_months = (end_year - start_year + 1) * 12
    
    print(f"\n{'='*60}")
    print(f"📖 Wikipedia Current Events: {start_year} → {end_year}")
    print(f"   Processing {total_months} months...")
    print(f"{'='*60}\n")
    
    with tqdm(total=total_months, desc="Wikipedia months") as pbar:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                # Don't go past current date
                if year == end_year and month > datetime.now().month:
                    pbar.update(1)
                    continue
                
                events = fetch_wikipedia_events_month(year, month)
                all_events.extend(events)
                
                pbar.set_postfix({
                    "year": year,
                    "month": month,
                    "month_events": len(events),
                    "total": len(all_events)
                })
                pbar.update(1)
                
                time.sleep(REQUEST_DELAY_SECONDS)
    
    print(f"\n✅ Wikipedia Events: {len(all_events)} total events fetched")
    return all_events


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    import pandas as pd
    
    parser = argparse.ArgumentParser(description="Wikipedia Current Events Scraper")
    parser.add_argument("--start-year", type=int, default=2005)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    
    events = fetch_wikipedia_events_range(args.start_year, args.end_year)
    
    out = args.output or os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "outputs", "news_wikipedia_events.csv"
    )
    os.makedirs(os.path.dirname(out), exist_ok=True)
    
    df = pd.DataFrame(events, columns=CSV_COLUMNS)
    df.to_csv(out, index=False, encoding='utf-8-sig')
    print(f"💾 Saved {len(df)} events to {out}")
