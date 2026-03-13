"""
Deduplicator for news items.
Removes duplicate entries based on headline similarity.
"""

import re
from difflib import SequenceMatcher


def _normalize_headline(headline):
    """Normalize headline for comparison."""
    text = headline.lower().strip()
    # Remove special characters, extra spaces
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def _are_similar(h1, h2, threshold=0.85):
    """Check if two headlines are similar enough to be duplicates."""
    n1 = _normalize_headline(h1)
    n2 = _normalize_headline(h2)
    
    # Exact match after normalization
    if n1 == n2:
        return True
    
    # Check if one is a substring of the other
    if n1 in n2 or n2 in n1:
        return True
    
    # Sequence similarity
    ratio = SequenceMatcher(None, n1, n2).ratio()
    return ratio >= threshold


def deduplicate_dataframe(df, similarity_threshold=0.85):
    """
    Remove duplicate rows from DataFrame based on headline similarity.
    
    Args:
        df: pandas DataFrame with 'headline' column
        similarity_threshold: How similar headlines need to be (0-1)
    
    Returns:
        DataFrame with duplicates removed
    """
    print(f"🔍 Deduplicating {len(df)} items (threshold={similarity_threshold})...")
    
    original_count = len(df)
    
    # First pass: exact duplicates
    df = df.drop_duplicates(subset=['headline'], keep='first')
    exact_dupes = original_count - len(df)
    
    # Second pass: near-duplicates (more expensive)
    if len(df) > 0:
        keep_indices = []
        seen_normalized = []
        
        for idx, row in df.iterrows():
            headline = row.get('headline', '')
            normalized = _normalize_headline(headline)
            
            is_dupe = False
            for seen in seen_normalized:
                if _are_similar(normalized, seen, similarity_threshold):
                    is_dupe = True
                    break
            
            if not is_dupe:
                keep_indices.append(idx)
                seen_normalized.append(normalized)
        
        df = df.loc[keep_indices]
    
    near_dupes = original_count - exact_dupes - len(df)
    
    print(f"   Exact duplicates removed: {exact_dupes}")
    print(f"   Near-duplicates removed: {near_dupes}")
    print(f"   ✅ Remaining: {len(df)} unique items")
    
    return df.reset_index(drop=True)
