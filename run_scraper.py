#!/usr/bin/env python3
"""
📰 20-Year News Database — Main Orchestrator

Usage:
    python run_scraper.py --mode top100      # Top 100 Google News items
    python run_scraper.py --mode week        # Past week (~700 rows)
    python run_scraper.py --mode year        # Past year via GDELT
    python run_scraper.py --mode full        # Full 20 years (2005-2025)
    python run_scraper.py --mode wikipedia   # Wikipedia Current Events only
    
    # Custom GDELT range:
    python run_scraper.py --source gdelt --start 2010-01-01 --end 2015-12-31
"""

import os
import sys
import argparse
from datetime import datetime

import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import CSV_COLUMNS, OUTPUT_DIR, LOG_DIR
from scrapers.google_news_rss import (
    fetch_top_news, fetch_past_week_news, save_to_csv as rss_save
)
from scrapers.gdelt_scraper import (
    fetch_gdelt_historical, fetch_gdelt_articles, save_to_csv as gdelt_save
)
from scrapers.wikipedia_events import (
    fetch_wikipedia_events_range
)
from processors.categorizer import categorize_dataframe
from processors.deduplicator import deduplicate_dataframe


def ensure_dirs():
    """Create output and log directories if they don't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)


def log_run(message):
    """Append a log entry."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_file = os.path.join(LOG_DIR, "scrape_log.txt")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"📝 {message}")


def run_top100():
    """Phase 1a: Fetch top 100 Google News items."""
    print("\n" + "="*60)
    print("🚀 MODE: Top 100 Google News Items")
    print("="*60 + "\n")
    
    items = fetch_top_news(100)
    
    df = pd.DataFrame(items, columns=CSV_COLUMNS)
    df = categorize_dataframe(df)
    df = deduplicate_dataframe(df)
    
    output_file = os.path.join(OUTPUT_DIR, "news_top100.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    log_run(f"Top 100: Saved {len(df)} items to {output_file}")
    print(f"\n✅ Done! Output: {output_file}")
    return df


def run_week():
    """Phase 1b: Fetch past week's news (~700 rows)."""
    print("\n" + "="*60)
    print("🚀 MODE: Past Week (~700 rows)")
    print("="*60 + "\n")
    
    items = fetch_past_week_news(700)
    
    df = pd.DataFrame(items, columns=CSV_COLUMNS)
    df = categorize_dataframe(df)
    df = deduplicate_dataframe(df)
    
    output_file = os.path.join(OUTPUT_DIR, "news_past_week.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    log_run(f"Past Week: Saved {len(df)} items to {output_file}")
    print(f"\n✅ Done! Output: {output_file}")
    return df


def run_year():
    """Phase 2: Fetch past year via GDELT."""
    print("\n" + "="*60)
    print("🚀 MODE: Past Year (GDELT)")
    print("="*60 + "\n")
    
    current_year = datetime.now().year
    
    items = fetch_gdelt_historical(
        start_year=current_year - 1,
        end_year=current_year,
        items_per_month=100,
    )
    
    df = pd.DataFrame(items, columns=CSV_COLUMNS)
    df = categorize_dataframe(df)
    df = deduplicate_dataframe(df)
    
    output_file = os.path.join(OUTPUT_DIR, "news_past_year.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    log_run(f"Past Year: Saved {len(df)} items to {output_file}")
    print(f"\n✅ Done! Output: {output_file}")
    return df


def run_full():
    """
    Phase 3: Full 20-year database.
    Combines GDELT + Wikipedia Current Events for maximum coverage.
    """
    print("\n" + "="*60)
    print("🚀 MODE: Full 20-Year Database (2005-2025)")
    print("="*60)
    print("⚠️  This will take a while (estimated 1-3 hours)")
    print("    Sources: GDELT + Wikipedia Current Events")
    print("="*60 + "\n")
    
    all_items = []
    
    # Source 1: GDELT (primary — articles with real URLs)
    print("\n📡 Source 1/2: GDELT Project...")
    gdelt_items = fetch_gdelt_historical(
        start_year=2005,
        end_year=2025,
        items_per_month=30,  # ~30 per month × 240 months ≈ 7,200
    )
    all_items.extend(gdelt_items)
    log_run(f"GDELT: {len(gdelt_items)} items fetched")
    
    # Interim save (in case of interruption)
    interim_df = pd.DataFrame(all_items, columns=CSV_COLUMNS)
    interim_file = os.path.join(OUTPUT_DIR, "news_database_interim_gdelt.csv")
    interim_df.to_csv(interim_file, index=False, encoding='utf-8-sig')
    print(f"💾 Interim save: {interim_file}")
    
    # Source 2: Wikipedia Current Events (curated, high-quality)
    print("\n📖 Source 2/2: Wikipedia Current Events...")
    wiki_items = fetch_wikipedia_events_range(
        start_year=2005,
        end_year=2025,
    )
    all_items.extend(wiki_items)
    log_run(f"Wikipedia: {len(wiki_items)} events fetched")
    
    # Combine, categorize, deduplicate
    print(f"\n📊 Combining all sources: {len(all_items)} total raw items...")
    
    df = pd.DataFrame(all_items, columns=CSV_COLUMNS)
    df = categorize_dataframe(df)
    df = deduplicate_dataframe(df)
    
    # Sort by date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.sort_values('date', ascending=False).reset_index(drop=True)
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # Save final output
    output_csv = os.path.join(OUTPUT_DIR, "news_database_full.csv")
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    
    # Also save as Excel for easier viewing
    output_xlsx = os.path.join(OUTPUT_DIR, "news_database_full.xlsx")
    try:
        df.to_excel(output_xlsx, index=False, engine='openpyxl')
        print(f"📗 Excel: {output_xlsx}")
    except Exception as e:
        print(f"⚠️ Excel save failed (CSV is still saved): {e}")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"✅ 20-YEAR NEWS DATABASE COMPLETE!")
    print(f"{'='*60}")
    print(f"   Total items:     {len(df)}")
    print(f"   Date range:      {df['date'].min()} → {df['date'].max()}")
    print(f"   Sources:         GDELT + Wikipedia")
    print(f"   CSV output:      {output_csv}")
    print(f"   Excel output:    {output_xlsx}")
    print(f"\n   Category breakdown:")
    print(f"   {df['category'].value_counts().to_string()}")
    print(f"\n   Year distribution:")
    df['year'] = pd.to_datetime(df['date'], errors='coerce').dt.year
    print(f"   {df['year'].value_counts().sort_index().to_string()}")
    print(f"{'='*60}\n")
    
    log_run(f"Full 20-Year: Saved {len(df)} items to {output_csv}")
    return df


def run_wikipedia_only():
    """Run Wikipedia Current Events scraper only."""
    print("\n" + "="*60)
    print("🚀 MODE: Wikipedia Current Events Only")
    print("="*60 + "\n")
    
    events = fetch_wikipedia_events_range(2005, 2025)
    
    df = pd.DataFrame(events, columns=CSV_COLUMNS)
    df = categorize_dataframe(df)
    df = deduplicate_dataframe(df)
    
    output_file = os.path.join(OUTPUT_DIR, "news_wikipedia_events.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    log_run(f"Wikipedia: Saved {len(df)} items to {output_file}")
    print(f"\n✅ Done! Output: {output_file}")
    return df


def run_custom_gdelt(start_date, end_date):
    """Run GDELT scraper with custom date range."""
    start_year = int(start_date.split("-")[0])
    end_year = int(end_date.split("-")[0])
    
    print(f"\n🚀 MODE: Custom GDELT ({start_date} → {end_date})\n")
    
    items = fetch_gdelt_historical(
        start_year=start_year,
        end_year=end_year,
        items_per_month=50,
    )
    
    df = pd.DataFrame(items, columns=CSV_COLUMNS)
    df = categorize_dataframe(df)
    df = deduplicate_dataframe(df)
    
    output_file = os.path.join(
        OUTPUT_DIR, f"news_gdelt_{start_year}_{end_year}.csv"
    )
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    log_run(f"Custom GDELT: Saved {len(df)} items to {output_file}")
    print(f"\n✅ Done! Output: {output_file}")
    return df


# ─── Main ────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="📰 20-Year News Database Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_scraper.py --mode top100       # Quick: top 100 news
  python run_scraper.py --mode week         # Past week (~700 rows)
  python run_scraper.py --mode year         # Past year (GDELT)
  python run_scraper.py --mode full         # Full 20 years!
  python run_scraper.py --mode wikipedia    # Wikipedia events only
  python run_scraper.py --source gdelt --start 2010-01-01 --end 2015-12-31
        """
    )
    parser.add_argument(
        "--mode",
        choices=["top100", "week", "year", "full", "wikipedia"],
        default="top100",
        help="Scraping mode"
    )
    parser.add_argument("--source", choices=["gdelt", "wikipedia", "google"], default=None)
    parser.add_argument("--start", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    ensure_dirs()
    
    # Custom source with date range
    if args.source and args.start and args.end:
        if args.source == "gdelt":
            run_custom_gdelt(args.start, args.end)
        return
    
    # Mode-based execution
    mode_map = {
        "top100": run_top100,
        "week": run_week,
        "year": run_year,
        "full": run_full,
        "wikipedia": run_wikipedia_only,
    }
    
    runner = mode_map.get(args.mode)
    if runner:
        runner()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
