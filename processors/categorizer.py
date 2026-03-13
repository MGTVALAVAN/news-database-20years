"""
Auto-categorizer for news items.
Uses keyword matching against UPSC categories defined in config.py.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CATEGORIES


def categorize_item(headline, summary="", existing_category=""):
    """
    Auto-categorize a news item based on headline and summary text.
    
    Returns:
        tuple: (category, subcategory, relevance, region)
    """
    if existing_category and existing_category not in ("", "General"):
        return existing_category, "", "", ""
    
    text = f"{headline} {summary}".lower()
    
    best_category = "General"
    best_score = 0
    
    for category, keywords in CATEGORIES.items():
        score = sum(1 for kw in keywords if kw in text)
        if score > best_score:
            best_score = score
            best_category = category
    
    # Determine UPSC relevance
    relevance = _determine_relevance(best_category, text)
    
    # Determine region
    region = _determine_region(text)
    
    return best_category, "", relevance, region


def _determine_relevance(category, text):
    """Map category to UPSC relevance tags."""
    relevance_map = {
        "Polity": "Prelims, Mains GS2",
        "Economy": "Prelims, Mains GS3",
        "International Relations": "Prelims, Mains GS2",
        "Environment": "Prelims, Mains GS3",
        "Science & Technology": "Prelims, Mains GS3",
        "History & Culture": "Prelims, Mains GS1",
        "Social Issues": "Mains GS1, GS2",
        "Defence & Security": "Prelims, Mains GS3",
        "Governance": "Mains GS2",
    }
    return relevance_map.get(category, "General Awareness")


def _determine_region(text):
    """Detect if the news is National, International, or State-level."""
    india_keywords = [
        "india", "indian", "delhi", "mumbai", "kolkata", "chennai",
        "bengaluru", "hyderabad", "modi", "bjp", "congress", "lok sabha",
        "rajya sabha", "rupee", "rbi", "isro", "niti aayog",
    ]
    
    state_keywords = [
        "tamil nadu", "karnataka", "maharashtra", "uttar pradesh",
        "kerala", "andhra", "telangana", "rajasthan", "bihar",
        "west bengal", "gujarat", "odisha", "assam", "jharkhand",
    ]
    
    if any(kw in text for kw in state_keywords):
        return "State"
    if any(kw in text for kw in india_keywords):
        return "National"
    return "International"


def categorize_dataframe(df):
    """
    Auto-categorize all rows in a DataFrame.
    Fills in category, relevance, and region columns.
    """
    print(f"🏷️  Auto-categorizing {len(df)} items...")
    
    for idx, row in df.iterrows():
        if not row.get('category') or row['category'] in ('', 'General'):
            cat, subcat, rel, region = categorize_item(
                row.get('headline', ''),
                row.get('summary', ''),
                row.get('category', '')
            )
            df.at[idx, 'category'] = cat
            if not row.get('subcategory'):
                df.at[idx, 'subcategory'] = subcat
            if not row.get('relevance'):
                df.at[idx, 'relevance'] = rel
            if not row.get('region'):
                df.at[idx, 'region'] = region
    
    print(f"✅ Categorization complete. Distribution:")
    print(df['category'].value_counts().to_string())
    print()
    
    return df
