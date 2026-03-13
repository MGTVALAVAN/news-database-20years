"""
The Hindu & Indian Express Archive Scrapers

Scrapes historical news articles from:
- The Hindu (thehindu.com) — archives from ~2000 onwards
- Indian Express (indianexpress.com) — archives from ~2005 onwards
- PIB (Press Information Bureau) — government press releases from 1998+

These are used to supplement GDELT/Wikipedia for India-specific coverage.
"""

import requests
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from tqdm import tqdm

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CSV_COLUMNS

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


# ─── The Hindu Archive ───────────────────────────────────────

def _scrape_thehindu_archive_page(date_str):
    """
    Scrape The Hindu's archive page for a specific date.
    Archive URL format: https://www.thehindu.com/archive/web/YYYY/MM/DD/
    """
    try:
        url = f"https://www.thehindu.com/archive/web/{date_str.replace('-', '/')}/"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
    except Exception as e:
        return []
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    items = []
    
    # The Hindu archive lists articles with links
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = link.get_text(strip=True)
        
        # Filter for article links (they typically contain /article/ or specific patterns)
        if ('/article/' in href or '/news/' in href) and len(text) > 20:
            # Skip navigation links
            if any(skip in text.lower() for skip in ['home', 'menu', 'subscribe', 'login', 'privacy']):
                continue
            
            # Determine category from URL path
            category = "General"
            if '/national/' in href or '/india/' in href:
                category = "Polity"
            elif '/business/' in href or '/economy/' in href:
                category = "Economy"
            elif '/sci-tech/' in href or '/science/' in href:
                category = "Science & Technology"
            elif '/international/' in href or '/world/' in href:
                category = "International Relations"
            elif '/opinion/' in href or '/editorial/' in href:
                category = "General"
            elif '/sport/' in href:
                category = "Sports"
            elif '/entertainment/' in href:
                category = "History & Culture"
            elif '/environment/' in href:
                category = "Environment"
            
            item = {
                "date": date_str,
                "headline": text[:300],
                "source": "The Hindu",
                "category": category,
                "subcategory": "",
                "summary": "",
                "url": href if href.startswith('http') else f"https://www.thehindu.com{href}",
                "relevance": "",
                "region": "National",
                "keywords": "",
            }
            items.append(item)
    
    return items


def fetch_thehindu_archive(start_date, end_date, max_per_day=20):
    """
    Fetch articles from The Hindu archives for a date range.
    
    Args:
        start_date: "YYYY-MM-DD"
        end_date: "YYYY-MM-DD"
        max_per_day: Maximum articles to keep per day
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end - start).days + 1
    
    print(f"\n📰 The Hindu Archive: {start_date} → {end_date} ({total_days} days)")
    
    all_items = []
    current = start
    
    with tqdm(total=total_days, desc="The Hindu") as pbar:
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            items = _scrape_thehindu_archive_page(date_str)
            all_items.extend(items[:max_per_day])
            
            pbar.set_postfix({"date": date_str, "items": len(items), "total": len(all_items)})
            pbar.update(1)
            
            current += timedelta(days=1)
            time.sleep(2)  # Respectful rate limiting
    
    print(f"✅ The Hindu: {len(all_items)} articles fetched")
    return all_items


# ─── Indian Express Archive ──────────────────────────────────

def _scrape_indianexpress_archive_page(date_str):
    """
    Scrape Indian Express archive page.
    Archive URL: https://indianexpress.com/YYYY/MM/DD/
    """
    try:
        parts = date_str.split("-")
        url = f"https://indianexpress.com/{parts[0]}/{parts[1]}/{parts[2]}/"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
    except Exception:
        return []
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    items = []
    
    for article in soup.find_all(['h2', 'h3', 'article']):
        link = article.find('a', href=True)
        if not link:
            continue
        
        text = link.get_text(strip=True)
        href = link.get('href', '')
        
        if len(text) > 20 and 'indianexpress.com' in href:
            category = "General"
            if '/india/' in href:
                category = "Polity"
            elif '/business/' in href:
                category = "Economy"
            elif '/technology/' in href:
                category = "Science & Technology"
            elif '/world/' in href:
                category = "International Relations"
            elif '/explained/' in href:
                category = "General"
            
            item = {
                "date": date_str,
                "headline": text[:300],
                "source": "Indian Express",
                "category": category,
                "subcategory": "",
                "summary": "",
                "url": href,
                "relevance": "",
                "region": "National",
                "keywords": "",
            }
            items.append(item)
    
    return items


def fetch_indianexpress_archive(start_date, end_date, max_per_day=20):
    """Fetch articles from Indian Express archives."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end - start).days + 1
    
    print(f"\n📰 Indian Express Archive: {start_date} → {end_date} ({total_days} days)")
    
    all_items = []
    current = start
    
    with tqdm(total=total_days, desc="Indian Express") as pbar:
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            items = _scrape_indianexpress_archive_page(date_str)
            all_items.extend(items[:max_per_day])
            
            pbar.set_postfix({"date": date_str, "items": len(items), "total": len(all_items)})
            pbar.update(1)
            
            current += timedelta(days=1)
            time.sleep(2)
    
    print(f"✅ Indian Express: {len(all_items)} articles fetched")
    return all_items


# ─── PIB (Press Information Bureau) ──────────────────────────

def fetch_pib_releases(start_date, end_date):
    """
    Fetch PIB press releases.
    PIB has a search API that accepts date ranges.
    """
    print(f"\n📋 PIB Press Releases: {start_date} → {end_date}")
    
    all_items = []
    
    try:
        # PIB search results page
        url = "https://pib.gov.in/allRel.aspx"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            for item_div in soup.find_all('div', class_=re.compile('content|release|news')):
                link = item_div.find('a', href=True)
                if link:
                    text = link.get_text(strip=True)
                    if len(text) > 20:
                        item = {
                            "date": start_date,
                            "headline": text[:300],
                            "source": "PIB",
                            "category": "Governance",
                            "subcategory": "Government Press Release",
                            "summary": "",
                            "url": link.get('href', ''),
                            "relevance": "Prelims, Mains GS2",
                            "region": "National",
                            "keywords": "government, policy",
                        }
                        all_items.append(item)
    except Exception as e:
        print(f"  ⚠️ PIB scraping error: {e}")
    
    print(f"✅ PIB: {len(all_items)} releases fetched")
    return all_items


# ─── Combined Archive Fetcher ────────────────────────────────

def fetch_all_archives(start_date, end_date, sources=None):
    """
    Fetch from all newspaper archives.
    
    Args:
        start_date, end_date: "YYYY-MM-DD" format
        sources: list of sources to scrape (default: all)
    """
    if sources is None:
        sources = ["thehindu", "indianexpress", "pib"]
    
    all_items = []
    
    if "thehindu" in sources:
        all_items.extend(fetch_thehindu_archive(start_date, end_date))
    
    if "indianexpress" in sources:
        all_items.extend(fetch_indianexpress_archive(start_date, end_date))
    
    if "pib" in sources:
        all_items.extend(fetch_pib_releases(start_date, end_date))
    
    print(f"\n📊 Total from archives: {len(all_items)} items")
    return all_items


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Newspaper Archive Scraper")
    parser.add_argument("--source", choices=["thehindu", "indianexpress", "pib", "all"], default="all")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()
    
    sources = [args.source] if args.source != "all" else None
    items = fetch_all_archives(args.start, args.end, sources)
    
    import pandas as pd
    df = pd.DataFrame(items, columns=CSV_COLUMNS)
    out = os.path.join(os.path.dirname(os.path.dirname(__file__)), "outputs", f"archives_{args.start}_{args.end}.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False, encoding='utf-8-sig')
    print(f"💾 Saved to {out}")
