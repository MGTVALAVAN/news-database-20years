"""
NLP-based Enricher for news items.

Provides:
- Keyword extraction using NLTK/spaCy NLP
- Summary generation from article text
- Named Entity Recognition (NER)
- Better categorization using NLP features
"""

import re
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CATEGORIES


def extract_keywords_nltk(text, top_n=5):
    """
    Extract keywords from text using NLTK TF-based approach.
    Falls back to simple extraction if NLTK isn't available.
    """
    try:
        import nltk
        from nltk.tokenize import word_tokenize
        from nltk.corpus import stopwords
        from collections import Counter
        
        # Ensure NLTK data is available
        try:
            stopwords.words('english')
        except LookupError:
            nltk.download('stopwords', quiet=True)
            nltk.download('punkt', quiet=True)
            nltk.download('punkt_tab', quiet=True)
        
        stop_words = set(stopwords.words('english'))
        # Add custom stop words for news
        stop_words.update([
            'said', 'also', 'would', 'could', 'may', 'new', 'one',
            'two', 'three', 'first', 'last', 'year', 'years', 'day',
            'says', 'told', 'report', 'reports', 'news', 'according',
            'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
            'saturday', 'sunday', 'january', 'february', 'march',
            'april', 'june', 'july', 'august', 'september',
            'october', 'november', 'december',
        ])
        
        # Tokenize and filter
        tokens = word_tokenize(text.lower())
        meaningful = [
            w for w in tokens 
            if w.isalpha() and len(w) > 2 and w not in stop_words
        ]
        
        # Get most common
        counter = Counter(meaningful)
        keywords = [word for word, count in counter.most_common(top_n)]
        return keywords
        
    except ImportError:
        return _simple_keyword_extract(text, top_n)


def _simple_keyword_extract(text, top_n=5):
    """Fallback keyword extraction without NLTK."""
    # Common English stop words
    stop_words = {
        'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
        'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
        'would', 'could', 'should', 'may', 'might', 'can', 'shall',
        'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from',
        'as', 'into', 'through', 'during', 'before', 'after', 'above',
        'below', 'between', 'out', 'off', 'over', 'under', 'again',
        'further', 'then', 'once', 'and', 'but', 'or', 'nor', 'not',
        'so', 'yet', 'both', 'each', 'few', 'more', 'most', 'other',
        'some', 'such', 'no', 'than', 'too', 'very', 'just', 'also',
        'said', 'says',
    }
    
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
    filtered = [w for w in words if w not in stop_words]
    
    from collections import Counter
    counter = Counter(filtered)
    return [w for w, _ in counter.most_common(top_n)]


def extract_entities(text):
    """
    Extract named entities (people, organizations, locations) using spaCy.
    Falls back gracefully if spaCy isn't available.
    """
    try:
        import spacy
        
        try:
            nlp = spacy.load("en_core_web_sm")
        except OSError:
            return {"persons": [], "organizations": [], "locations": []}
        
        doc = nlp(text[:1000])  # Limit text length for performance
        
        entities = {
            "persons": [],
            "organizations": [],
            "locations": [],
        }
        
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                entities["persons"].append(ent.text)
            elif ent.label_ == "ORG":
                entities["organizations"].append(ent.text)
            elif ent.label_ in ("GPE", "LOC"):
                entities["locations"].append(ent.text)
        
        # Deduplicate
        for key in entities:
            entities[key] = list(set(entities[key]))
        
        return entities
        
    except ImportError:
        return {"persons": [], "organizations": [], "locations": []}


def generate_summary(text, max_sentences=2):
    """
    Generate a brief summary by extracting the most important sentences.
    Uses a simple extractive approach based on keyword frequency.
    """
    if not text or len(text) < 50:
        return text
    
    # Split into sentences
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 20]
    
    if not sentences:
        return text[:200]
    
    if len(sentences) <= max_sentences:
        return '. '.join(sentences) + '.'
    
    # Score sentences by keyword overlap with full text
    keywords = set(_simple_keyword_extract(text, top_n=10))
    
    scored = []
    for i, sent in enumerate(sentences):
        words = set(re.findall(r'\b[a-zA-Z]{3,}\b', sent.lower()))
        score = len(words & keywords)
        # Boost earlier sentences (lead bias in news)
        score += max(0, 3 - i)
        scored.append((score, i, sent))
    
    scored.sort(reverse=True)
    top_sentences = sorted(scored[:max_sentences], key=lambda x: x[1])
    
    summary = '. '.join(s[2] for s in top_sentences)
    if not summary.endswith('.'):
        summary += '.'
    
    return summary[:500]


def enrich_dataframe(df):
    """
    Enrich a DataFrame of news items with:
    - Keywords (if missing)
    - Better categorization
    """
    print(f"✨ Enriching {len(df)} items with NLP features...")
    
    enriched = 0
    for idx, row in df.iterrows():
        headline = row.get('headline', '')
        summary = row.get('summary', '')
        text = f"{headline} {summary}"
        
        # Extract keywords if missing
        if not row.get('keywords') or row['keywords'] == '':
            keywords = extract_keywords_nltk(text, top_n=5)
            df.at[idx, 'keywords'] = ', '.join(keywords)
            enriched += 1
    
    print(f"   ✅ Enriched {enriched} items with keywords")
    return df


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    # Quick test
    test_text = "Supreme Court upholds EWS reservation in landmark 3-2 verdict, says economic criteria valid for affirmative action under Article 15(6)"
    
    print("Keywords:", extract_keywords_nltk(test_text))
    print("Entities:", extract_entities(test_text))
    print("Summary:", generate_summary(test_text))
