"""
SQLite Database Manager for the 20-Year News Database.

Provides:
- Schema creation with indexes and FTS (Full-Text Search)
- CRUD operations (insert, update, query)
- Deduplication before insert
- Export to CSV/Excel
- Full-text search across headlines, summaries, keywords
- Statistics and coverage reporting
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime
from contextlib import contextmanager

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CSV_COLUMNS


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "news_database.db")


# ─── Schema ──────────────────────────────────────────────────

SCHEMA_SQL = """
-- Main news table
CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    headline TEXT NOT NULL,
    source TEXT,
    category TEXT DEFAULT 'General',
    subcategory TEXT DEFAULT '',
    summary TEXT DEFAULT '',
    url TEXT DEFAULT '',
    relevance TEXT DEFAULT '',
    region TEXT DEFAULT '',
    keywords TEXT DEFAULT '',
    data_source TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fast querying
CREATE INDEX IF NOT EXISTS idx_news_date ON news_items(date);
CREATE INDEX IF NOT EXISTS idx_news_category ON news_items(category);
CREATE INDEX IF NOT EXISTS idx_news_source ON news_items(source);
CREATE INDEX IF NOT EXISTS idx_news_region ON news_items(region);
CREATE INDEX IF NOT EXISTS idx_news_data_source ON news_items(data_source);
CREATE INDEX IF NOT EXISTS idx_news_headline ON news_items(headline);

-- Categories lookup table
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    upsc_paper TEXT,
    description TEXT DEFAULT ''
);

-- Scrape progress tracking (for incremental/resume)
CREATE TABLE IF NOT EXISTS scrape_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    query TEXT DEFAULT '',
    items_fetched INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT DEFAULT '',
    UNIQUE(source, year, month, query)
);

-- Scrape run log
CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    mode TEXT,
    start_date DATE,
    end_date DATE,
    total_items INTEGER DEFAULT 0,
    new_items INTEGER DEFAULT 0,
    duplicates_skipped INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    duration_seconds REAL DEFAULT 0,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cache table for API responses
CREATE TABLE IF NOT EXISTS api_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    query TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    response_json TEXT,
    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    UNIQUE(source, query, start_date, end_date)
);
"""

FTS_SQL = """
-- Full-text search virtual table
CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
    headline, summary, keywords,
    content='news_items',
    content_rowid='id'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS news_fts_insert AFTER INSERT ON news_items BEGIN
    INSERT INTO news_fts(rowid, headline, summary, keywords)
    VALUES (new.id, new.headline, new.summary, new.keywords);
END;

CREATE TRIGGER IF NOT EXISTS news_fts_delete BEFORE DELETE ON news_items BEGIN
    INSERT INTO news_fts(news_fts, rowid, headline, summary, keywords)
    VALUES ('delete', old.id, old.headline, old.summary, old.keywords);
END;

CREATE TRIGGER IF NOT EXISTS news_fts_update AFTER UPDATE ON news_items BEGIN
    INSERT INTO news_fts(news_fts, rowid, headline, summary, keywords)
    VALUES ('delete', old.id, old.headline, old.summary, old.keywords);
    INSERT INTO news_fts(rowid, headline, summary, keywords)
    VALUES (new.id, new.headline, new.summary, new.keywords);
END;
"""

SEED_CATEGORIES_SQL = """
INSERT OR IGNORE INTO categories (name, upsc_paper) VALUES
    ('Polity', 'Prelims, Mains GS2'),
    ('Economy', 'Prelims, Mains GS3'),
    ('International Relations', 'Prelims, Mains GS2'),
    ('Environment', 'Prelims, Mains GS3'),
    ('Science & Technology', 'Prelims, Mains GS3'),
    ('History & Culture', 'Prelims, Mains GS1'),
    ('Social Issues', 'Mains GS1, GS2'),
    ('Defence & Security', 'Prelims, Mains GS3'),
    ('Governance', 'Mains GS2'),
    ('General', 'General Awareness');
"""


class NewsDatabase:
    """SQLite database manager for news items."""
    
    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self._initialize()
    
    def _initialize(self):
        """Create database and tables if they don't exist."""
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)
            try:
                conn.executescript(FTS_SQL)
            except sqlite3.OperationalError:
                pass  # FTS tables may already exist
            conn.executescript(SEED_CATEGORIES_SQL)
        print(f"✅ Database initialized: {self.db_path}")
    
    @contextmanager
    def _connect(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent performance
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    # ─── Insert Operations ───────────────────────────────────
    
    def insert_items(self, items, data_source=""):
        """
        Insert news items into the database with deduplication.
        
        Args:
            items: list of dicts with CSV_COLUMNS keys
            data_source: Source identifier ('gdelt', 'wikipedia', etc.)
        
        Returns:
            tuple: (inserted_count, duplicate_count)
        """
        inserted = 0
        duplicates = 0
        
        with self._connect() as conn:
            for item in items:
                # Check for duplicate by headline similarity
                headline = item.get('headline', '').strip()
                if not headline:
                    continue
                
                # Exact duplicate check
                existing = conn.execute(
                    "SELECT id FROM news_items WHERE headline = ? LIMIT 1",
                    (headline,)
                ).fetchone()
                
                if existing:
                    duplicates += 1
                    continue
                
                # Insert
                conn.execute("""
                    INSERT INTO news_items 
                    (date, headline, source, category, subcategory, 
                     summary, url, relevance, region, keywords, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.get('date', ''),
                    headline,
                    item.get('source', ''),
                    item.get('category', 'General'),
                    item.get('subcategory', ''),
                    item.get('summary', ''),
                    item.get('url', ''),
                    item.get('relevance', ''),
                    item.get('region', ''),
                    item.get('keywords', ''),
                    data_source or item.get('data_source', ''),
                ))
                inserted += 1
        
        print(f"   📥 Inserted: {inserted}, Duplicates skipped: {duplicates}")
        return inserted, duplicates
    
    # ─── Query Operations ────────────────────────────────────
    
    def search(self, query, limit=50):
        """Full-text search across headlines, summaries, keywords."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT n.* FROM news_items n
                JOIN news_fts f ON n.id = f.rowid
                WHERE news_fts MATCH ?
                ORDER BY n.date DESC
                LIMIT ?
            """, (query, limit)).fetchall()
            return [dict(r) for r in rows]
    
    def query_by_date_range(self, start_date, end_date, category=None, limit=1000):
        """Query items within a date range, optionally filtered by category."""
        sql = "SELECT * FROM news_items WHERE date BETWEEN ? AND ?"
        params = [start_date, end_date]
        
        if category:
            sql += " AND category = ?"
            params.append(category)
        
        sql += " ORDER BY date DESC LIMIT ?"
        params.append(limit)
        
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
    
    def query_by_category(self, category, limit=500):
        """Get all items for a specific category."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM news_items WHERE category = ? ORDER BY date DESC LIMIT ?",
                (category, limit)
            ).fetchall()
            return [dict(r) for r in rows]
    
    def query_by_year(self, year, limit=5000):
        """Get all items for a specific year."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM news_items WHERE date LIKE ? ORDER BY date DESC LIMIT ?",
                (f"{year}%", limit)
            ).fetchall()
            return [dict(r) for r in rows]
    
    def get_all(self, limit=None):
        """Get all items."""
        sql = "SELECT * FROM news_items ORDER BY date DESC"
        if limit:
            sql += f" LIMIT {limit}"
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
            return [dict(r) for r in rows]
    
    # ─── Progress Tracking ───────────────────────────────────
    
    def mark_progress(self, source, year, month, query="", items_fetched=0, status="completed"):
        """Mark a scraping task as completed."""
        with self._connect() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO scrape_progress 
                (source, year, month, query, items_fetched, status, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (source, year, month, query, items_fetched, status, 
                  datetime.now().isoformat()))
    
    def is_scraped(self, source, year, month, query=""):
        """Check if a specific month/query has been scraped."""
        with self._connect() as conn:
            row = conn.execute("""
                SELECT status FROM scrape_progress 
                WHERE source = ? AND year = ? AND month = ? AND query = ?
            """, (source, year, month, query)).fetchone()
            return row and row['status'] == 'completed'
    
    def get_pending_tasks(self, source, start_year=2005, end_year=2025):
        """Get list of year/month combinations not yet scraped."""
        with self._connect() as conn:
            completed = set()
            rows = conn.execute("""
                SELECT year, month FROM scrape_progress 
                WHERE source = ? AND status = 'completed'
            """, (source,)).fetchall()
            for r in rows:
                completed.add((r['year'], r['month']))
        
        pending = []
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if (year, month) not in completed:
                    if year < end_year or month <= datetime.now().month:
                        pending.append((year, month))
        
        return pending
    
    def log_scrape_run(self, source, mode, start_date, end_date, 
                       total_items, new_items, duplicates, errors, duration):
        """Log a completed scrape run."""
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO scrape_log 
                (source, mode, start_date, end_date, total_items, 
                 new_items, duplicates_skipped, errors, duration_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (source, mode, start_date, end_date, total_items,
                  new_items, duplicates, errors, duration))
    
    # ─── Cache Operations ────────────────────────────────────
    
    def cache_get(self, source, query, start_date, end_date):
        """Get cached API response."""
        with self._connect() as conn:
            row = conn.execute("""
                SELECT response_json FROM api_cache 
                WHERE source = ? AND query = ? AND start_date = ? AND end_date = ?
                AND (expires_at IS NULL OR expires_at > ?)
            """, (source, query, start_date, end_date, 
                  datetime.now().isoformat())).fetchone()
            if row:
                import json
                return json.loads(row['response_json'])
        return None
    
    def cache_set(self, source, query, start_date, end_date, response, ttl_hours=24*7):
        """Cache an API response."""
        import json
        from datetime import timedelta
        expires = (datetime.now() + timedelta(hours=ttl_hours)).isoformat()
        
        with self._connect() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO api_cache 
                (source, query, start_date, end_date, response_json, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (source, query, start_date, end_date, 
                  json.dumps(response), expires))
    
    # ─── Export Operations ───────────────────────────────────
    
    def export_csv(self, filepath, filters=None):
        """Export database to CSV with optional filters."""
        df = self.to_dataframe(filters)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"💾 Exported {len(df)} items to {filepath}")
        return df
    
    def export_excel(self, filepath, filters=None):
        """Export database to Excel with optional filters."""
        df = self.to_dataframe(filters)
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            # Full database
            df.to_excel(writer, sheet_name='All News', index=False)
            
            # Category-wise sheets
            for cat in df['category'].unique():
                cat_df = df[df['category'] == cat]
                sheet_name = cat[:31]  # Excel sheet name limit
                cat_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Year-wise summary
            df['year'] = pd.to_datetime(df['date'], errors='coerce').dt.year
            summary = df.groupby(['year', 'category']).size().unstack(fill_value=0)
            summary.to_excel(writer, sheet_name='Summary by Year')
        
        print(f"📗 Exported {len(df)} items to {filepath}")
        return df
    
    def to_dataframe(self, filters=None):
        """Convert database to pandas DataFrame."""
        with self._connect() as conn:
            sql = "SELECT date, headline, source, category, subcategory, summary, url, relevance, region, keywords FROM news_items"
            conditions = []
            params = []
            
            if filters:
                if 'start_date' in filters:
                    conditions.append("date >= ?")
                    params.append(filters['start_date'])
                if 'end_date' in filters:
                    conditions.append("date <= ?")
                    params.append(filters['end_date'])
                if 'category' in filters:
                    conditions.append("category = ?")
                    params.append(filters['category'])
                if 'region' in filters:
                    conditions.append("region = ?")
                    params.append(filters['region'])
            
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            
            sql += " ORDER BY date DESC"
            
            df = pd.read_sql_query(sql, conn.connection if hasattr(conn, 'connection') else conn, params=params)
            return df
    
    # ─── Statistics ──────────────────────────────────────────
    
    def get_stats(self):
        """Get database statistics."""
        with self._connect() as conn:
            stats = {}
            
            # Total items
            stats['total_items'] = conn.execute(
                "SELECT COUNT(*) FROM news_items"
            ).fetchone()[0]
            
            # Date range
            row = conn.execute(
                "SELECT MIN(date) as min_date, MAX(date) as max_date FROM news_items"
            ).fetchone()
            stats['date_range'] = (row['min_date'], row['max_date']) if row else (None, None)
            
            # By category
            rows = conn.execute(
                "SELECT category, COUNT(*) as count FROM news_items GROUP BY category ORDER BY count DESC"
            ).fetchall()
            stats['by_category'] = {r['category']: r['count'] for r in rows}
            
            # By source
            rows = conn.execute(
                "SELECT source, COUNT(*) as count FROM news_items GROUP BY source ORDER BY count DESC LIMIT 20"
            ).fetchall()
            stats['top_sources'] = {r['source']: r['count'] for r in rows}
            
            # By year
            rows = conn.execute("""
                SELECT SUBSTR(date, 1, 4) as year, COUNT(*) as count 
                FROM news_items GROUP BY year ORDER BY year
            """).fetchall()
            stats['by_year'] = {r['year']: r['count'] for r in rows}
            
            # By data source
            rows = conn.execute(
                "SELECT data_source, COUNT(*) as count FROM news_items GROUP BY data_source ORDER BY count DESC"
            ).fetchall()
            stats['by_data_source'] = {r['data_source']: r['count'] for r in rows}
            
            # By region
            rows = conn.execute(
                "SELECT region, COUNT(*) as count FROM news_items GROUP BY region ORDER BY count DESC"
            ).fetchall()
            stats['by_region'] = {r['region']: r['count'] for r in rows}
            
            return stats
    
    def print_stats(self):
        """Print formatted database statistics."""
        stats = self.get_stats()
        
        print(f"\n{'='*60}")
        print(f"📊 NEWS DATABASE STATISTICS")
        print(f"{'='*60}")
        print(f"   Total items:  {stats['total_items']:,}")
        print(f"   Date range:   {stats['date_range'][0]} → {stats['date_range'][1]}")
        
        print(f"\n   📁 By Category:")
        for cat, count in stats['by_category'].items():
            bar = '█' * min(count // 50, 30)
            print(f"      {cat:<25} {count:>6} {bar}")
        
        print(f"\n   📅 By Year:")
        for year, count in stats['by_year'].items():
            bar = '█' * min(count // 20, 30)
            print(f"      {year}  {count:>6} {bar}")
        
        print(f"\n   🌐 By Region:")
        for region, count in stats['by_region'].items():
            print(f"      {region:<20} {count:>6}")
        
        print(f"\n   📡 By Data Source:")
        for src, count in stats['by_data_source'].items():
            print(f"      {src:<20} {count:>6}")
        
        print(f"\n   📰 Top Sources:")
        for src, count in list(stats['top_sources'].items())[:10]:
            print(f"      {src:<35} {count:>6}")
        
        print(f"{'='*60}\n")
    
    def get_coverage_gaps(self, start_year=2005, end_year=2025, min_items_per_month=5):
        """Identify months with insufficient data coverage."""
        gaps = []
        with self._connect() as conn:
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    if year == end_year and month > datetime.now().month:
                        continue
                    
                    count = conn.execute("""
                        SELECT COUNT(*) FROM news_items 
                        WHERE date LIKE ?
                    """, (f"{year}-{month:02d}%",)).fetchone()[0]
                    
                    if count < min_items_per_month:
                        gaps.append({
                            'year': year,
                            'month': month,
                            'items': count,
                            'deficit': min_items_per_month - count
                        })
        
        return gaps


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    db = NewsDatabase()
    db.print_stats()
