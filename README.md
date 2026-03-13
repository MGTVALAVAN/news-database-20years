# 📰 20-Year News Database (2005–2025)

A comprehensive, structured news database spanning **20 years** of Indian and global news — scraped, categorized, and tabulated for research, UPSC preparation, and data analysis.

## 🎯 Project Goal

Build a **single, unified CSV/Excel database** of major news events from 2005 to 2025, covering:
- Indian national news
- International affairs
- Economy & business
- Science & technology
- Environment & ecology
- Polity & governance
- Art, culture & society
- Sports

## 📊 Data Schema

Each row in the database contains:

| Column | Description | Example |
|--------|-------------|---------|
| `date` | Publication date (YYYY-MM-DD) | 2024-03-15 |
| `headline` | News headline | "Supreme Court upholds EWS reservation" |
| `source` | News source/publisher | The Hindu |
| `category` | Topic category | Polity |
| `subcategory` | Specific subcategory | Constitutional Law |
| `summary` | 2-3 sentence summary | "The Supreme Court in a 3:2 verdict..." |
| `url` | Source URL | https://... |
| `relevance` | UPSC relevance tag | Prelims, Mains GS2 |
| `region` | Geographic region | National / International / State |
| `keywords` | Key terms (comma-separated) | reservation, EWS, Supreme Court, Article 15 |

## 🏗️ Architecture & Data Sources

Since Google News doesn't provide a direct API for historical data beyond a few weeks, we use a **multi-source strategy**:

### Phase 1: Recent News (Past 1 Week) ✅
- **Google News RSS feeds** with date filters
- Direct scraping of Google News search results

### Phase 2: Past 1 Year (2024-2025)
- **GNews API** (free tier: 100 requests/day)
- **NewsAPI.org** (free tier: 100 requests/day)
- **Google News RSS** with `before:` and `after:` date parameters

### Phase 3: Historical (2005-2024) — The 20-Year Expansion
- **GDELT Project** (Global Database of Events, Language, and Tone)
  - Largest open database of global news events
  - 250+ million events from 1979 to present
  - Free BigQuery access (1 TB/month free)
- **Common Crawl / Wayback Machine**
  - Internet Archive's historical snapshots
  - Structured extraction from archived news pages
- **The Hindu Archives** (2005+)
- **Indian Express Archives** (free access to older articles)
- **Wikipedia Current Events Portal**
  - Monthly curated summaries of significant events
  - Excellent for filling gaps and cross-referencing

## 📁 Project Structure

```
news-database-20years/
├── README.md
├── requirements.txt
├── config.py                    # API keys, settings
├── scrapers/
│   ├── __init__.py
│   ├── google_news_rss.py       # Google News RSS scraper
│   ├── gnews_api.py             # GNews API client
│   ├── gdelt_scraper.py         # GDELT BigQuery/API client
│   ├── wikipedia_events.py      # Wikipedia Current Events parser
│   ├── newspaper_archives.py    # The Hindu, IE archive scrapers
│   └── common_crawl.py          # Common Crawl/Wayback Machine
├── processors/
│   ├── __init__.py
│   ├── deduplicator.py          # Remove duplicate entries
│   ├── categorizer.py           # Auto-categorize by topic
│   └── enricher.py              # Add summaries, keywords, relevance
├── outputs/
│   ├── news_database_week.csv   # Phase 1: past week
│   ├── news_database_year.csv   # Phase 2: past year
│   └── news_database_full.csv   # Phase 3: full 20 years
├── logs/
│   └── scrape_log.txt
└── run_scraper.py               # Main orchestrator script
```

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run recent news scraper (past week)
python run_scraper.py --mode week

# Run yearly scraper (past 1 year)
python run_scraper.py --mode year

# Run full historical scraper (20 years)
python run_scraper.py --mode full

# Run specific source
python run_scraper.py --source gdelt --start 2005-01-01 --end 2025-03-13
```

## 📋 Progress

- [x] Phase 1: Top 100 Google News items (CSV)
- [x] Phase 1: Extended to 700 rows (past 1 week)
- [ ] Phase 2: Past 1 year (~5,000+ rows)
- [ ] Phase 3: Full 20 years (~50,000+ rows)
- [ ] Deduplication & quality check
- [ ] UPSC relevance tagging
- [ ] Final database export

## 📝 License

MIT License — Free to use for educational and research purposes.
