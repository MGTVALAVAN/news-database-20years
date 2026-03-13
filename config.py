"""
Configuration for the 20-Year News Database project.
API keys, paths, and scraper settings.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── API Keys ───────────────────────────────────────────────
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY", "")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "")

# ─── Output Paths ───────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")

# ─── CSV Column Schema ──────────────────────────────────────
CSV_COLUMNS = [
    "date",           # YYYY-MM-DD
    "headline",       # News headline
    "source",         # Publisher name
    "category",       # Main category (Polity, Economy, etc.)
    "subcategory",    # Specific subcategory
    "summary",        # 2-3 sentence summary
    "url",            # Source URL
    "relevance",      # UPSC relevance (Prelims/Mains GS1-4)
    "region",         # National / International / State
    "keywords",       # Comma-separated key terms
]

# ─── Categories for auto-classification ─────────────────────
CATEGORIES = {
    "Polity": [
        "supreme court", "parliament", "constitution", "bill", "act",
        "election", "governor", "president", "legislation", "fundamental rights",
        "amendment", "lok sabha", "rajya sabha", "judiciary", "high court",
        "panchayati raj", "municipal", "speaker", "opposition", "ruling party",
        "cag", "upsc", "election commission", "niti aayog", "cabinet",
    ],
    "Economy": [
        "gdp", "inflation", "rbi", "fiscal", "budget", "tax", "gst",
        "trade", "export", "import", "stock market", "sensex", "nifty",
        "banking", "monetary policy", "repo rate", "current account",
        "fdi", "disinvestment", "subsidy", "rupee", "dollar",
        "unemployment", "msme", "startup", "fintech",
    ],
    "International Relations": [
        "un", "united nations", "g20", "g7", "nato", "brics",
        "bilateral", "diplomacy", "foreign policy", "treaty",
        "pakistan", "china", "usa", "russia", "middle east",
        "sanctions", "trade war", "border", "lac", "loc",
        "quad", "asean", "saarc", "who", "wto", "imf",
    ],
    "Environment": [
        "climate", "pollution", "forest", "wildlife", "biodiversity",
        "carbon", "emission", "renewable", "solar", "wind energy",
        "deforestation", "conservation", "national park", "tiger",
        "cyclone", "flood", "drought", "earthquake", "tsunami",
        "cop", "paris agreement", "kyoto", "wetland", "coral",
    ],
    "Science & Technology": [
        "isro", "nasa", "satellite", "space", "rocket", "ai",
        "artificial intelligence", "machine learning", "quantum",
        "5g", "semiconductor", "chip", "cyber", "drone",
        "biotechnology", "genome", "vaccine", "drug", "research",
        "patent", "innovation", "digital", "blockchain", "iot",
    ],
    "History & Culture": [
        "heritage", "unesco", "archaeological", "monument", "temple",
        "festival", "art", "music", "dance", "literature",
        "museum", "excavation", "ancient", "medieval", "colonial",
        "independence", "freedom fighter", "cultural", "tradition",
    ],
    "Social Issues": [
        "education", "health", "poverty", "reservation", "caste",
        "gender", "women", "child", "nutrition", "sanitation",
        "rural", "urban", "migration", "ngo", "welfare",
        "disability", "minority", "tribal", "scheduled caste",
        "scheduled tribe", "obc", "ews",
    ],
    "Defence & Security": [
        "army", "navy", "air force", "military", "defence",
        "terrorism", "naxal", "insurgency", "border security",
        "nuclear", "missile", "submarine", "aircraft carrier",
        "ceasefire", "surgical strike", "drdo", "hal",
    ],
    "Governance": [
        "policy", "scheme", "yojana", "mission", "programme",
        "e-governance", "transparency", "corruption", "rti",
        "lokpal", "ombudsman", "accountability", "audit",
        "public service", "bureaucracy", "ias", "ips",
    ],
}

# ─── News Sources for Archive Scraping ──────────────────────
NEWS_SOURCES = {
    "the_hindu": "https://www.thehindu.com",
    "indian_express": "https://indianexpress.com",
    "mint": "https://www.livemint.com",
    "pib": "https://pib.gov.in",
    "down_to_earth": "https://www.downtoearth.org.in",
}

# ─── GDELT Settings ─────────────────────────────────────────
GDELT_INDIA_FILTER = "sourcelang:eng AND sourcecountry:IN"
GDELT_MAX_RECORDS = 250  # per query

# ─── Rate Limiting ──────────────────────────────────────────
REQUEST_DELAY_SECONDS = 2  # Delay between HTTP requests
API_CALLS_PER_DAY = {
    "gnews": 100,
    "newsapi": 100,
    "gdelt": 1000,  # effectively unlimited
}

# ─── Date Range ─────────────────────────────────────────────
HISTORICAL_START = "2005-01-01"
HISTORICAL_END = "2025-03-13"
