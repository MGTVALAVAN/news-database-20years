#!/usr/bin/env python3
"""
📰 20-Year News Database — Main Orchestrator (v2)

Now with:
- SQLite database (replaces CSV as primary storage)
- Incremental scraping with resume capability
- API response caching
- Multiple data sources (GDELT, Wikipedia, The Hindu, IE, PIB)
- NLP enrichment (keywords, entities)
- Coverage gap analysis
- Export to CSV/Excel on-demand

Usage:
    python run_scraper.py --mode top100         # Quick: ~100 items via GDELT
    python run_scraper.py --mode week           # Past week
    python run_scraper.py --mode year           # Past year via GDELT
    python run_scraper.py --mode full           # Full 20 years (2005-2025)
    python run_scraper.py --mode wikipedia      # Wikipedia Current Events only
    python run_scraper.py --mode archives       # The Hindu + IE archives
    python run_scraper.py --mode stats          # Database statistics
    python run_scraper.py --mode report         # Coverage report
    python run_scraper.py --mode export         # Export DB to CSV/Excel
    python run_scraper.py --mode search --query "GST"  # Full-text search
    
    # Custom GDELT range:
    python run_scraper.py --source gdelt --start 2010-01-01 --end 2015-12-31
    
    # Resume interrupted scrape:
    python run_scraper.py --mode full --resume
"""

import os
import sys
import argparse
import time
from datetime import datetime, timedelta

import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import CSV_COLUMNS, OUTPUT_DIR, LOG_DIR
from database import NewsDatabase
from scrapers.gdelt_scraper import (
    fetch_gdelt_historical, fetch_gdelt_articles
)
from scrapers.wikipedia_events import (
    fetch_wikipedia_events_range, fetch_wikipedia_events_month
)
from scrapers.newspaper_archives import (
    fetch_thehindu_archive, fetch_indianexpress_archive, fetch_pib_releases
)
from processors.categorizer import categorize_dataframe
from processors.deduplicator import deduplicate_dataframe
from processors.enricher import enrich_dataframe
from processors.coverage_report import generate_coverage_report, save_report_to_file


def ensure_dirs():
    """Create output and log directories."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)


def process_and_store(items, db, data_source="", enrich=True):
    """
    Process items through the pipeline and store in database.
    
    Pipeline: Raw items → DataFrame → Categorize → Enrich → Deduplicate → Store
    """
    if not items:
        print("   ℹ️  No items to process")
        return 0, 0
    
    df = pd.DataFrame(items, columns=CSV_COLUMNS)
    
    # Auto-categorize
    df = categorize_dataframe(df)
    
    # NLP enrichment
    if enrich:
        df = enrich_dataframe(df)
    
    # Store in database (dedup happens at DB level)
    items_list = df.to_dict('records')
    inserted, duplicates = db.insert_items(items_list, data_source=data_source)
    
    return inserted, duplicates


# ─── Modes ───────────────────────────────────────────────────

def run_top100(db):
    """Quick mode: Fetch ~100 recent items via GDELT."""
    print("\n" + "="*60)
    print("🚀 MODE: Top 100 Recent News (GDELT)")
    print("="*60 + "\n")
    
    start_time = time.time()
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    
    all_items = []
    topics = ["India news", "India economy", "India parliament", "India technology", "India defence"]
    
    for topic in topics:
        items = fetch_gdelt_articles(topic, start_date, end_date, 25)
        all_items.extend(items)
        time.sleep(6)
    
    inserted, dupes = process_and_store(all_items, db, data_source="gdelt")
    
    duration = time.time() - start_time
    db.log_scrape_run("gdelt", "top100", start_date, end_date,
                      len(all_items), inserted, dupes, 0, duration)
    
    print(f"\n✅ Done in {duration:.0f}s! {inserted} new items stored.")
    return inserted


def run_week(db):
    """Fetch past week's news via GDELT."""
    print("\n" + "="*60)
    print("🚀 MODE: Past Week")
    print("="*60 + "\n")
    
    start_time = time.time()
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    topics = [
        "India news", "India economy", "India parliament", "India technology",
        "India defence", "India diplomacy", "India environment",
        "India education", "India judiciary", "India election",
        "world geopolitics", "climate change", "United Nations",
    ]
    
    all_items = []
    for topic in topics:
        items = fetch_gdelt_articles(topic, start_date, end_date, 60)
        all_items.extend(items)
        print(f"   {topic}: {len(items)} items")
        time.sleep(6)
    
    inserted, dupes = process_and_store(all_items, db, data_source="gdelt")
    
    duration = time.time() - start_time
    db.log_scrape_run("gdelt", "week", start_date, end_date,
                      len(all_items), inserted, dupes, 0, duration)
    
    print(f"\n✅ Done in {duration:.0f}s! {inserted} new items stored.")
    return inserted


def run_year(db, resume=False):
    """Fetch past year via GDELT with resume support."""
    print("\n" + "="*60)
    print("🚀 MODE: Past Year (GDELT)")
    print("="*60 + "\n")
    
    start_time = time.time()
    current_year = datetime.now().year
    total_inserted = 0
    total_dupes = 0
    errors = 0
    
    topics = [
        "India parliament", "India economy", "India diplomacy",
        "India environment", "India technology", "India defence",
    ]
    
    for month in range(1, 13):
        if month > datetime.now().month and current_year == datetime.now().year:
            continue
        
        # Check if already scraped (resume support)
        if resume and db.is_scraped("gdelt_year", current_year, month):
            print(f"   ⏭️  {current_year}-{month:02d} already scraped, skipping")
            continue
        
        start_date = f"{current_year}-{month:02d}-01"
        if month == 12:
            end_date = f"{current_year}-12-31"
        else:
            end_date = (datetime(current_year, month + 1, 1) - timedelta(days=1)).strftime("%Y-%m-%d")
        
        month_items = []
        for topic in topics:
            try:
                items = fetch_gdelt_articles(topic, start_date, end_date, 30)
                month_items.extend(items)
                time.sleep(6)
            except Exception as e:
                print(f"   ⚠️ Error: {e}")
                errors += 1
        
        inserted, dupes = process_and_store(month_items, db, data_source="gdelt")
        total_inserted += inserted
        total_dupes += dupes
        
        # Mark progress
        db.mark_progress("gdelt_year", current_year, month, items_fetched=inserted)
        print(f"   📅 {current_year}-{month:02d}: +{inserted} items")
    
    duration = time.time() - start_time
    db.log_scrape_run("gdelt", "year", f"{current_year}-01-01",
                      datetime.now().strftime("%Y-%m-%d"),
                      total_inserted + total_dupes, total_inserted, total_dupes,
                      errors, duration)
    
    print(f"\n✅ Done in {duration:.0f}s! {total_inserted} new items stored.")
    return total_inserted


def run_full(db, resume=False):
    """
    Full 20-year database build.
    Uses GDELT + Wikipedia + Newspaper Archives.
    Supports resume from interruption.
    """
    print("\n" + "="*60)
    print("🚀 MODE: Full 20-Year Database (2005-2025)")
    print("="*60)
    print("   Sources: GDELT + Wikipedia + The Hindu + Indian Express")
    print("   Resume:  " + ("✅ Enabled" if resume else "❌ Disabled"))
    print("="*60 + "\n")
    
    start_time = time.time()
    total_inserted = 0
    total_dupes = 0
    total_errors = 0
    
    topics = [
        "India parliament", "India economy", "India diplomacy",
        "India environment", "India technology", "India defence",
    ]
    
    # ─── Phase 1: GDELT (2015-2025 via DOC API) ─────────────
    print("\n" + "─"*50)
    print("📡 PHASE 1/3: GDELT Project (2015-2025)")
    print("─"*50 + "\n")
    
    for year in range(2015, 2026):
        for month in range(1, 13):
            if year == 2025 and month > datetime.now().month:
                continue
            
            if resume and db.is_scraped("gdelt", year, month):
                print(f"   ⏭️  {year}-{month:02d} already scraped")
                continue
            
            start_date = f"{year}-{month:02d}-01"
            if month == 12:
                end_date = f"{year}-12-31"
            else:
                end_date = (datetime(year, month + 1, 1) - timedelta(days=1)).strftime("%Y-%m-%d")
            
            month_items = []
            for topic in topics:
                try:
                    items = fetch_gdelt_articles(topic, start_date, end_date, 20)
                    month_items.extend(items)
                    time.sleep(6)
                except Exception as e:
                    total_errors += 1
                    time.sleep(10)
            
            inserted, dupes = process_and_store(month_items, db, data_source="gdelt", enrich=False)
            total_inserted += inserted
            total_dupes += dupes
            
            db.mark_progress("gdelt", year, month, items_fetched=inserted)
            print(f"   📅 {year}-{month:02d}: +{inserted} items (total: {total_inserted})")
    
    print(f"\n   ✅ GDELT Phase Complete: {total_inserted} items")
    
    # ─── Phase 2: Wikipedia Current Events (2005-2025) ───────
    print("\n" + "─"*50)
    print("📖 PHASE 2/3: Wikipedia Current Events (2005-2025)")
    print("─"*50 + "\n")
    
    wiki_inserted = 0
    for year in range(2005, 2026):
        for month in range(1, 13):
            if year == 2025 and month > datetime.now().month:
                continue
            
            if resume and db.is_scraped("wikipedia", year, month):
                print(f"   ⏭️  {year}-{month:02d} already scraped")
                continue
            
            try:
                events = fetch_wikipedia_events_month(year, month)
                if events:
                    inserted, dupes = process_and_store(
                        events, db, data_source="wikipedia", enrich=False
                    )
                    wiki_inserted += inserted
                    db.mark_progress("wikipedia", year, month, items_fetched=inserted)
                    print(f"   📅 {year}-{month:02d}: +{inserted} events")
                
                time.sleep(2)
            except Exception as e:
                print(f"   ⚠️ {year}-{month:02d}: {e}")
                total_errors += 1
    
    total_inserted += wiki_inserted
    print(f"\n   ✅ Wikipedia Phase Complete: {wiki_inserted} events")
    
    # ─── Phase 3: Newspaper Archives (sample from key years) ─
    print("\n" + "─"*50)
    print("📰 PHASE 3/3: Newspaper Archives (key years)")
    print("─"*50 + "\n")
    
    archive_inserted = 0
    # Scrape 1 week from each year for The Hindu/IE coverage
    key_dates = []
    for year in range(2005, 2026):
        # Scrape first week of January and July for each year
        key_dates.append((f"{year}-01-01", f"{year}-01-07"))
        key_dates.append((f"{year}-07-01", f"{year}-07-07"))
    
    for start_d, end_d in key_dates:
        year = int(start_d[:4])
        month = int(start_d[5:7])
        
        if resume and db.is_scraped("archives", year, month):
            continue
        
        try:
            items = fetch_thehindu_archive(start_d, end_d, max_per_day=10)
            if items:
                inserted, dupes = process_and_store(
                    items, db, data_source="thehindu", enrich=False
                )
                archive_inserted += inserted
            
            db.mark_progress("archives", year, month, items_fetched=archive_inserted)
            time.sleep(3)
        except Exception as e:
            print(f"   ⚠️ Archives {start_d}: {e}")
            total_errors += 1
    
    total_inserted += archive_inserted
    print(f"\n   ✅ Archives Phase Complete: {archive_inserted} items")
    
    # ─── Final: Enrich everything ────────────────────────────
    print("\n" + "─"*50)
    print("✨ ENRICHMENT: Adding keywords via NLP...")
    print("─"*50 + "\n")
    
    # Export, enrich, re-import
    all_items = db.get_all()
    if all_items:
        df = pd.DataFrame(all_items)
        df = enrich_dataframe(df)
    
    # ─── Final Summary ───────────────────────────────────────
    duration = time.time() - start_time
    hours = duration / 3600
    
    db.log_scrape_run("all", "full", "2005-01-01",
                      datetime.now().strftime("%Y-%m-%d"),
                      total_inserted + total_dupes, total_inserted,
                      total_dupes, total_errors, duration)
    
    print(f"\n{'='*60}")
    print(f"✅ 20-YEAR NEWS DATABASE BUILD COMPLETE!")
    print(f"{'='*60}")
    print(f"   Duration:        {hours:.1f} hours ({duration:.0f} seconds)")
    print(f"   Total inserted:  {total_inserted:,}")
    print(f"   Duplicates:      {total_dupes:,}")
    print(f"   Errors:          {total_errors}")
    print(f"   Database:        {db.db_path}")
    print(f"{'='*60}\n")
    
    # Print stats
    db.print_stats()
    
    # Auto-export
    csv_path = os.path.join(OUTPUT_DIR, "news_database_full.csv")
    xlsx_path = os.path.join(OUTPUT_DIR, "news_database_full.xlsx")
    db.export_csv(csv_path)
    try:
        db.export_excel(xlsx_path)
    except Exception as e:
        print(f"   ⚠️ Excel export failed: {e}")
    
    return total_inserted


def run_archives(db, start_date=None, end_date=None):
    """Run newspaper archive scrapers."""
    print("\n" + "="*60)
    print("🚀 MODE: Newspaper Archives")
    print("="*60 + "\n")
    
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    start_time = time.time()
    total_inserted = 0
    
    # The Hindu
    items = fetch_thehindu_archive(start_date, end_date)
    inserted, _ = process_and_store(items, db, data_source="thehindu")
    total_inserted += inserted
    
    # Indian Express
    items = fetch_indianexpress_archive(start_date, end_date)
    inserted, _ = process_and_store(items, db, data_source="indianexpress")
    total_inserted += inserted
    
    # PIB
    items = fetch_pib_releases(start_date, end_date)
    inserted, _ = process_and_store(items, db, data_source="pib")
    total_inserted += inserted
    
    duration = time.time() - start_time
    print(f"\n✅ Archives done in {duration:.0f}s! {total_inserted} new items.")
    return total_inserted


def run_wikipedia(db, start_year=2005, end_year=2025, resume=False):
    """Run Wikipedia Current Events scraper."""
    print("\n" + "="*60)
    print("🚀 MODE: Wikipedia Current Events")
    print("="*60 + "\n")
    
    start_time = time.time()
    total_inserted = 0
    
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_year and month > datetime.now().month:
                continue
            
            if resume and db.is_scraped("wikipedia", year, month):
                print(f"   ⏭️  {year}-{month:02d} skipped")
                continue
            
            events = fetch_wikipedia_events_month(year, month)
            if events:
                inserted, _ = process_and_store(events, db, data_source="wikipedia")
                total_inserted += inserted
                db.mark_progress("wikipedia", year, month, items_fetched=inserted)
                print(f"   📅 {year}-{month:02d}: +{inserted}")
            
            time.sleep(2)
    
    duration = time.time() - start_time
    print(f"\n✅ Wikipedia done in {duration:.0f}s! {total_inserted} events.")
    return total_inserted


def run_search(db, query, limit=50):
    """Full-text search the database."""
    print(f"\n🔍 Searching for: \"{query}\"\n")
    
    results = db.search(query, limit=limit)
    
    if not results:
        print("   No results found.")
        return
    
    print(f"   Found {len(results)} results:\n")
    for i, r in enumerate(results, 1):
        print(f"   {i:3}. [{r['date']}] {r['headline'][:80]}")
        print(f"        Source: {r['source']} | Category: {r['category']} | Region: {r['region']}")
        if r.get('summary'):
            print(f"        {r['summary'][:100]}...")
        print()


def run_export(db, format="both"):
    """Export database to CSV and/or Excel."""
    ensure_dirs()
    
    print("\n📤 Exporting database...\n")
    
    if format in ("csv", "both"):
        csv_path = os.path.join(OUTPUT_DIR, "news_database_full.csv")
        db.export_csv(csv_path)
    
    if format in ("excel", "both"):
        xlsx_path = os.path.join(OUTPUT_DIR, "news_database_full.xlsx")
        db.export_excel(xlsx_path)
    
    print("\n✅ Export complete!")


def run_custom_gdelt(db, start_date, end_date, resume=False):
    """Run GDELT for a custom date range."""
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    
    print(f"\n🚀 Custom GDELT: {start_date} → {end_date}\n")
    
    topics = [
        "India parliament", "India economy", "India diplomacy",
        "India environment", "India technology", "India defence",
    ]
    
    start_time = time.time()
    total_inserted = 0
    
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < int(start_date[5:7]):
                continue
            if year == end_year and month > int(end_date[5:7]):
                continue
            
            if resume and db.is_scraped("gdelt_custom", year, month):
                print(f"   ⏭️  {year}-{month:02d} skipped")
                continue
            
            s_date = f"{year}-{month:02d}-01"
            if month == 12:
                e_date = f"{year}-12-31"
            else:
                e_date = (datetime(year, month + 1, 1) - timedelta(days=1)).strftime("%Y-%m-%d")
            
            month_items = []
            for topic in topics:
                try:
                    items = fetch_gdelt_articles(topic, s_date, e_date, 25)
                    month_items.extend(items)
                    time.sleep(6)
                except Exception as e:
                    time.sleep(10)
            
            inserted, _ = process_and_store(month_items, db, data_source="gdelt")
            total_inserted += inserted
            db.mark_progress("gdelt_custom", year, month, items_fetched=inserted)
            print(f"   📅 {year}-{month:02d}: +{inserted}")
    
    duration = time.time() - start_time
    print(f"\n✅ Done in {duration:.0f}s! {total_inserted} items.")


# ─── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="📰 20-Year News Database Scraper (v2 — SQLite + Multi-Source)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_scraper.py --mode top100              # Quick test
  python run_scraper.py --mode week                # Past week
  python run_scraper.py --mode year                # Past year
  python run_scraper.py --mode full                # Full 20 years!
  python run_scraper.py --mode full --resume       # Resume interrupted scrape
  python run_scraper.py --mode wikipedia           # Wikipedia events
  python run_scraper.py --mode archives            # Newspaper archives
  python run_scraper.py --mode stats               # Database statistics
  python run_scraper.py --mode report              # Coverage report
  python run_scraper.py --mode export              # Export to CSV/Excel
  python run_scraper.py --mode search --query "GST reform"
  python run_scraper.py --source gdelt --start 2010-01-01 --end 2015-12-31
        """
    )
    parser.add_argument(
        "--mode",
        choices=["top100", "week", "year", "full", "wikipedia", "archives",
                 "stats", "report", "export", "search"],
        default="top100",
        help="Operation mode"
    )
    parser.add_argument("--source", choices=["gdelt", "wikipedia", "archives"], default=None)
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--resume", action="store_true", help="Resume interrupted scrape")
    parser.add_argument("--query", default=None, help="Search query (for --mode search)")
    parser.add_argument("--db", default=None, help="Custom database path")
    
    args = parser.parse_args()
    
    ensure_dirs()
    
    # Initialize database
    db = NewsDatabase(args.db)
    
    # Custom source with date range
    if args.source and args.start and args.end:
        if args.source == "gdelt":
            run_custom_gdelt(db, args.start, args.end, args.resume)
        elif args.source == "wikipedia":
            start_year = int(args.start[:4])
            end_year = int(args.end[:4])
            run_wikipedia(db, start_year, end_year, args.resume)
        elif args.source == "archives":
            run_archives(db, args.start, args.end)
        return
    
    # Mode-based execution
    if args.mode == "top100":
        run_top100(db)
    elif args.mode == "week":
        run_week(db)
    elif args.mode == "year":
        run_year(db, args.resume)
    elif args.mode == "full":
        run_full(db, args.resume)
    elif args.mode == "wikipedia":
        run_wikipedia(db, resume=args.resume)
    elif args.mode == "archives":
        run_archives(db, args.start, args.end)
    elif args.mode == "stats":
        db.print_stats()
    elif args.mode == "report":
        report_path = os.path.join(OUTPUT_DIR, "coverage_report.txt")
        generate_coverage_report(db)
        save_report_to_file(db, report_path)
    elif args.mode == "export":
        run_export(db)
    elif args.mode == "search":
        if not args.query:
            print("❌ Please provide --query for search mode")
        else:
            run_search(db, args.query)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
