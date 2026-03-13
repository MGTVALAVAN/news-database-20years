"""
Coverage Report Generator

Analyzes the news database for:
- Year-by-year and month-by-month coverage
- Category distribution gaps
- Source diversity analysis
- Missing periods that need supplementation
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def generate_coverage_report(db):
    """
    Generate a comprehensive coverage report.
    
    Args:
        db: NewsDatabase instance
    """
    stats = db.get_stats()
    gaps = db.get_coverage_gaps(min_items_per_month=10)
    
    print(f"\n{'='*70}")
    print(f"📊 COVERAGE REPORT — 20-Year News Database")
    print(f"{'='*70}")
    
    # Overview
    print(f"\n📋 OVERVIEW")
    print(f"   Total items:     {stats['total_items']:,}")
    if stats['date_range'][0]:
        print(f"   Date range:      {stats['date_range'][0]} → {stats['date_range'][1]}")
    
    # Year distribution
    print(f"\n📅 YEAR-BY-YEAR COVERAGE")
    print(f"   {'Year':<8} {'Count':>8}  {'Bar'}")
    print(f"   {'─'*8} {'─'*8}  {'─'*30}")
    
    target_per_year = 500  # Ideal target
    for year in range(2005, 2026):
        count = stats['by_year'].get(str(year), 0)
        pct = min(count / target_per_year * 100, 100) if target_per_year else 0
        bar_len = int(pct / 3.33)
        bar = '█' * bar_len + '░' * (30 - bar_len)
        status = "✅" if pct >= 50 else "⚠️" if pct >= 10 else "❌"
        print(f"   {year:<8} {count:>8}  {bar} {pct:.0f}% {status}")
    
    # Category distribution
    print(f"\n📁 CATEGORY DISTRIBUTION")
    total = stats['total_items'] or 1
    for cat, count in stats['by_category'].items():
        pct = count / total * 100
        bar = '█' * int(pct / 2)
        print(f"   {cat:<25} {count:>6} ({pct:5.1f}%) {bar}")
    
    # Coverage gaps
    if gaps:
        print(f"\n⚠️  COVERAGE GAPS (< 10 items/month)")
        print(f"   Found {len(gaps)} months with insufficient data:")
        # Group by year
        gap_years = {}
        for g in gaps:
            yr = g['year']
            if yr not in gap_years:
                gap_years[yr] = []
            gap_years[yr].append(g['month'])
        
        for yr, months in sorted(gap_years.items()):
            month_names = [datetime(2000, m, 1).strftime('%b') for m in months]
            print(f"   {yr}: {', '.join(month_names)} ({len(months)} months)")
    else:
        print(f"\n✅ No significant coverage gaps found!")
    
    # Source diversity
    print(f"\n📰 TOP DATA SOURCES")
    for src, count in list(stats.get('by_data_source', {}).items())[:10]:
        pct = count / total * 100
        print(f"   {src or 'unknown':<25} {count:>6} ({pct:5.1f}%)")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS")
    
    if stats['total_items'] < 1000:
        print(f"   • Run full scrape: python run_scraper.py --mode full")
    
    if gaps:
        gap_ranges = []
        for g in gaps[:5]:
            gap_ranges.append(f"{g['year']}-{g['month']:02d}")
        print(f"   • Fill gaps for: {', '.join(gap_ranges)}")
        print(f"     Use: python run_scraper.py --source gdelt --start YYYY-MM-01 --end YYYY-MM-28")
    
    cat_counts = stats.get('by_category', {})
    weak_cats = [c for c, n in cat_counts.items() if n < total * 0.05 and c != 'General']
    if weak_cats:
        print(f"   • Weak categories (< 5%): {', '.join(weak_cats)}")
        print(f"     Consider targeted scraping for these topics")
    
    print(f"\n{'='*70}\n")
    
    return {
        'stats': stats,
        'gaps': gaps,
    }


def save_report_to_file(db, filepath):
    """Save coverage report to a text file."""
    import io
    from contextlib import redirect_stdout
    
    f = io.StringIO()
    with redirect_stdout(f):
        generate_coverage_report(db)
    
    report = f.getvalue()
    with open(filepath, 'w') as out:
        out.write(report)
    
    print(f"📄 Report saved to {filepath}")
    return report
