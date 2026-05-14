"""
Viralityy / Shortvity — Niche Engine v2 (Combined)
====================================================
Merges the best of both models:

  LAYER 1 — Static intelligence (from older Viralityy engine)
    · CPM ranges per niche         (30 pts of final score)
    · Competition + difficulty      (20 pts)
    · Evergreen flag                (bonus)
    · Plan-type compatibility       (Shorts / Longform / Combo)
    · Revenue estimates
    · Example video topics

  LAYER 2 — Live YouTube validation (from newer Shortvity analyser)
    · Real avg views from top 20 videos   (15 pts)
    · Engagement rate (likes+comments)    (15 pts)
    · Recency (% uploaded last 30 days)   (10 pts)
    · Short-form suitability (% <10 min)  (10 pts)
    · Top 5 performing videos
    · Common title words + tags

  COMBINED SCORE (out of 100):
    CPM / monetisation     30 pts
    Competition/difficulty 20 pts
    Live avg views         15 pts
    Live engagement        15 pts
    Recency                10 pts
    Short-form fit         10 pts
    Evergreen bonus        up to +5 pts (can exceed 100, capped)

  VERDICT:
    70–100  ✅ HOT      — push to pipeline immediately
    45–69   🟡 MODERATE — monitor before committing
    0–44    ❌ WEAK     — skip or try a sub-niche

USAGE:
  # Offline mode (no API key) — Layer 1 only
  engine = NicheEngine()
  results = engine.recommend(plan="combo_pro", count=10)

  # Full mode — both layers
  engine = NicheEngine(youtube_api_key="AIza...")
  result  = await engine.analyse("psychology facts", plan="combo_pro")
  results = await engine.analyse_batch(["stoicism", "ai tools"], plan="shorts_pro")

REQUIREMENTS:
  Already in requirements.txt:
    google-api-python-client==2.108.0
    requests==2.31.0
"""

import os
import re
import math
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from collections import Counter
from typing import Optional

try:
    from googleapiclient.discovery import build as yt_build
    HAS_YT = True
except ImportError:
    HAS_YT = False

logging.basicConfig(level=logging.INFO, format='[NicheV2] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
CACHE_TTL_HOURS  = 24       # live results cached this long
CACHE_TTL_DAYS   = 7        # auto-delete after this many days
MAX_RESULTS      = 20       # YouTube search results to fetch per keyword
MAX_BATCH        = 10       # max keywords in a single batch call
SHORTS_MAX_SECS  = 600      # videos ≤ 10 min counted as short-form compatible
RECENCY_DAYS     = 30       # "recent" = uploaded within this window

# Score weights — must sum to 100
W_CPM        = 30
W_OPPORTUNITY= 20
W_LIVE_VIEWS = 15
W_ENGAGEMENT = 15
W_RECENCY    = 10
W_SHORTFORM  = 10
EVERGREEN_BONUS = 5

# ---------------------------------------------------------------------------
# STATIC NICHE DATABASE
# (same 27 niches as original engine — CPM/competition/difficulty preserved)
# ---------------------------------------------------------------------------
NICHE_DATA = {
    # ── Finance & Business ──────────────────────────────────────────────────
    "personal_finance": {
        "label": "Personal Finance",
        "category": "Finance & Business",
        "cpm_low": 12.0, "cpm_high": 45.0,
        "trend_score": 8, "competition": "high", "difficulty": 7,
        "avg_views_per_video": 8500,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["budgeting", "saving", "investing", "money tips"],
        "evergreen": True,
        "search_keywords": ["personal finance tips", "money management", "budgeting"],
    },
    "investing_basics": {
        "label": "Investing for Beginners",
        "category": "Finance & Business",
        "cpm_low": 15.0, "cpm_high": 55.0,
        "trend_score": 7, "competition": "high", "difficulty": 6,
        "avg_views_per_video": 7200,
        "works_for": ["longform", "combo"],
        "tags": ["stocks", "ETFs", "index funds", "passive income"],
        "evergreen": True,
        "search_keywords": ["investing for beginners", "stock market basics", "ETF investing"],
    },
    "side_hustles": {
        "label": "Side Hustles & Online Income",
        "category": "Finance & Business",
        "cpm_low": 8.0, "cpm_high": 30.0,
        "trend_score": 9, "competition": "medium", "difficulty": 4,
        "avg_views_per_video": 15000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["make money online", "freelance", "passive income"],
        "evergreen": True,
        "search_keywords": ["side hustle ideas", "make money online", "online income"],
    },
    "crypto_basics": {
        "label": "Crypto & Web3",
        "category": "Finance & Business",
        "cpm_low": 10.0, "cpm_high": 40.0,
        "trend_score": 6, "competition": "medium", "difficulty": 5,
        "avg_views_per_video": 9000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["bitcoin", "ethereum", "blockchain", "defi"],
        "evergreen": False,
        "search_keywords": ["crypto for beginners", "bitcoin explained", "web3"],
    },

    # ── Health & Wellness ────────────────────────────────────────────────────
    "mental_health": {
        "label": "Mental Health & Mindfulness",
        "category": "Health & Wellness",
        "cpm_low": 6.0, "cpm_high": 22.0,
        "trend_score": 9, "competition": "medium", "difficulty": 3,
        "avg_views_per_video": 18000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["anxiety", "therapy", "meditation", "self care"],
        "evergreen": True,
        "search_keywords": ["mental health tips", "anxiety relief", "mindfulness"],
    },
    "fitness_home": {
        "label": "Home Fitness & Workouts",
        "category": "Health & Wellness",
        "cpm_low": 5.0, "cpm_high": 18.0,
        "trend_score": 7, "competition": "high", "difficulty": 5,
        "avg_views_per_video": 22000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["no equipment", "home workout", "weight loss", "HIIT"],
        "evergreen": True,
        "search_keywords": ["home workout", "no equipment exercise", "HIIT workout"],
    },
    "nutrition_diet": {
        "label": "Nutrition & Healthy Eating",
        "category": "Health & Wellness",
        "cpm_low": 5.0, "cpm_high": 20.0,
        "trend_score": 7, "competition": "medium", "difficulty": 4,
        "avg_views_per_video": 14000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["meal prep", "clean eating", "diet tips", "recipes"],
        "evergreen": True,
        "search_keywords": ["healthy eating tips", "meal prep", "nutrition advice"],
    },
    "sleep_optimization": {
        "label": "Sleep & Recovery",
        "category": "Health & Wellness",
        "cpm_low": 7.0, "cpm_high": 25.0,
        "trend_score": 8, "competition": "low", "difficulty": 2,
        "avg_views_per_video": 20000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["better sleep", "insomnia", "sleep tips", "recovery"],
        "evergreen": True,
        "search_keywords": ["sleep tips", "how to sleep better", "insomnia cure"],
    },

    # ── Technology ───────────────────────────────────────────────────────────
    "ai_tools": {
        "label": "AI Tools & Productivity",
        "category": "Technology",
        "cpm_low": 10.0, "cpm_high": 38.0,
        "trend_score": 10, "competition": "medium", "difficulty": 4,
        "avg_views_per_video": 25000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["ChatGPT", "AI apps", "productivity", "automation"],
        "evergreen": False,
        "search_keywords": ["AI tools 2025", "ChatGPT tips", "AI productivity"],
    },
    "software_tutorials": {
        "label": "Software & App Tutorials",
        "category": "Technology",
        "cpm_low": 8.0, "cpm_high": 28.0,
        "trend_score": 6, "competition": "medium", "difficulty": 5,
        "avg_views_per_video": 9500,
        "works_for": ["longform", "combo"],
        "tags": ["how to use", "tutorial", "review", "app guide"],
        "evergreen": True,
        "search_keywords": ["software tutorial", "app guide", "how to use"],
    },
    "smartphone_tips": {
        "label": "Smartphone Tips & Tricks",
        "category": "Technology",
        "cpm_low": 4.0, "cpm_high": 14.0,
        "trend_score": 7, "competition": "medium", "difficulty": 2,
        "avg_views_per_video": 30000,
        "works_for": ["shorts"],
        "tags": ["iPhone tips", "Android tricks", "hidden features"],
        "evergreen": True,
        "search_keywords": ["iPhone tips tricks", "Android hidden features", "phone hacks"],
    },
    "coding_beginners": {
        "label": "Learn to Code",
        "category": "Technology",
        "cpm_low": 9.0, "cpm_high": 32.0,
        "trend_score": 7, "competition": "high", "difficulty": 7,
        "avg_views_per_video": 8000,
        "works_for": ["longform", "combo"],
        "tags": ["Python", "web development", "programming for beginners"],
        "evergreen": True,
        "search_keywords": ["learn to code beginners", "Python for beginners", "programming tutorial"],
    },

    # ── Self Improvement ─────────────────────────────────────────────────────
    "productivity": {
        "label": "Productivity & Focus",
        "category": "Self Improvement",
        "cpm_low": 7.0, "cpm_high": 24.0,
        "trend_score": 8, "competition": "high", "difficulty": 5,
        "avg_views_per_video": 12000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["time management", "deep work", "focus", "habits"],
        "evergreen": True,
        "search_keywords": ["productivity tips", "time management", "focus habits"],
    },
    "stoicism_philosophy": {
        "label": "Stoicism & Philosophy",
        "category": "Self Improvement",
        "cpm_low": 5.0, "cpm_high": 18.0,
        "trend_score": 7, "competition": "low", "difficulty": 2,
        "avg_views_per_video": 28000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["stoic", "Marcus Aurelius", "life lessons", "mindset"],
        "evergreen": True,
        "search_keywords": ["stoicism explained", "Marcus Aurelius quotes", "stoic mindset"],
    },
    "book_summaries": {
        "label": "Book Summaries & Key Ideas",
        "category": "Self Improvement",
        "cpm_low": 5.0, "cpm_high": 16.0,
        "trend_score": 8, "competition": "medium", "difficulty": 2,
        "avg_views_per_video": 35000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["book review", "non-fiction", "key lessons", "reading"],
        "evergreen": True,
        "search_keywords": ["book summary", "book review", "best books"],
    },
    "morning_routines": {
        "label": "Morning Routines & Habits",
        "category": "Self Improvement",
        "cpm_low": 5.0, "cpm_high": 15.0,
        "trend_score": 7, "competition": "medium", "difficulty": 2,
        "avg_views_per_video": 22000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["morning routine", "habits", "discipline", "success"],
        "evergreen": True,
        "search_keywords": ["morning routine", "daily habits", "discipline tips"],
    },

    # ── Education & Facts ────────────────────────────────────────────────────
    "history_facts": {
        "label": "History & Surprising Facts",
        "category": "Education",
        "cpm_low": 4.0, "cpm_high": 12.0,
        "trend_score": 8, "competition": "low", "difficulty": 2,
        "avg_views_per_video": 45000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["history", "did you know", "facts", "interesting"],
        "evergreen": True,
        "search_keywords": ["history facts", "surprising historical facts", "did you know history"],
    },
    "science_explained": {
        "label": "Science Made Simple",
        "category": "Education",
        "cpm_low": 4.0, "cpm_high": 14.0,
        "trend_score": 7, "competition": "low", "difficulty": 3,
        "avg_views_per_video": 38000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["science", "explained", "how it works", "physics", "biology"],
        "evergreen": True,
        "search_keywords": ["science explained simply", "how does it work", "physics explained"],
    },
    "psychology_facts": {
        "label": "Psychology & Human Behaviour",
        "category": "Education",
        "cpm_low": 5.0, "cpm_high": 18.0,
        "trend_score": 9, "competition": "low", "difficulty": 2,
        "avg_views_per_video": 50000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["psychology", "human behaviour", "mind tricks", "social skills"],
        "evergreen": True,
        "search_keywords": ["psychology facts", "human behaviour explained", "mind tricks"],
    },
    "language_learning": {
        "label": "Language Learning Tips",
        "category": "Education",
        "cpm_low": 4.0, "cpm_high": 14.0,
        "trend_score": 7, "competition": "medium", "difficulty": 3,
        "avg_views_per_video": 18000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["learn Spanish", "language tips", "fluent", "vocabulary"],
        "evergreen": True,
        "search_keywords": ["language learning tips", "learn Spanish fast", "vocabulary tips"],
    },

    # ── Lifestyle ────────────────────────────────────────────────────────────
    "minimalism": {
        "label": "Minimalism & Simple Living",
        "category": "Lifestyle",
        "cpm_low": 4.0, "cpm_high": 14.0,
        "trend_score": 7, "competition": "low", "difficulty": 2,
        "avg_views_per_video": 20000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["minimalist", "declutter", "simple life", "less is more"],
        "evergreen": True,
        "search_keywords": ["minimalism tips", "declutter your life", "simple living"],
    },
    "travel_tips": {
        "label": "Budget Travel Tips",
        "category": "Lifestyle",
        "cpm_low": 3.0, "cpm_high": 12.0,
        "trend_score": 8, "competition": "medium", "difficulty": 4,
        "avg_views_per_video": 16000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["travel hacks", "cheap travel", "backpacking", "travel tips"],
        "evergreen": True,
        "search_keywords": ["budget travel tips", "travel hacks", "cheap flights"],
    },
    "relationships": {
        "label": "Relationships & Social Skills",
        "category": "Lifestyle",
        "cpm_low": 5.0, "cpm_high": 16.0,
        "trend_score": 8, "competition": "medium", "difficulty": 3,
        "avg_views_per_video": 32000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["dating tips", "communication", "friendships", "social skills"],
        "evergreen": True,
        "search_keywords": ["relationship advice", "social skills tips", "communication skills"],
    },
    "parenting_tips": {
        "label": "Parenting & Family",
        "category": "Lifestyle",
        "cpm_low": 5.0, "cpm_high": 18.0,
        "trend_score": 7, "competition": "low", "difficulty": 2,
        "avg_views_per_video": 14000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["parenting hacks", "kids", "family", "toddler tips"],
        "evergreen": True,
        "search_keywords": ["parenting tips", "toddler advice", "family hacks"],
    },

    # ── Business & Entrepreneurship ──────────────────────────────────────────
    "dropshipping": {
        "label": "Dropshipping & E-commerce",
        "category": "Business",
        "cpm_low": 10.0, "cpm_high": 35.0,
        "trend_score": 7, "competition": "high", "difficulty": 6,
        "avg_views_per_video": 9000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["dropshipping", "Shopify", "ecommerce", "online store"],
        "evergreen": False,
        "search_keywords": ["dropshipping tutorial", "Shopify store", "ecommerce tips"],
    },
    "freelancing": {
        "label": "Freelancing & Remote Work",
        "category": "Business",
        "cpm_low": 7.0, "cpm_high": 25.0,
        "trend_score": 8, "competition": "medium", "difficulty": 3,
        "avg_views_per_video": 12000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["freelance", "Upwork", "Fiverr", "remote work", "work from home"],
        "evergreen": True,
        "search_keywords": ["freelancing tips", "Upwork guide", "remote work jobs"],
    },
    "real_estate": {
        "label": "Real Estate Investing",
        "category": "Business",
        "cpm_low": 14.0, "cpm_high": 50.0,
        "trend_score": 7, "competition": "medium", "difficulty": 5,
        "avg_views_per_video": 8000,
        "works_for": ["longform", "combo"],
        "tags": ["real estate", "property investing", "rental income", "REITs"],
        "evergreen": True,
        "search_keywords": ["real estate investing beginners", "rental property", "REITs explained"],
    },
}

# Plan → content type mapping (mirrors server.js)
PLAN_CONTENT_TYPE = {
    "shorts_starter":  "shorts",
    "shorts_pro":      "shorts",
    "longform_starter":"longform",
    "longform_pro":    "longform",
    "combo_pro":       "combo",
    "combo_agency":    "combo",
}

# Stop-words to strip from title word frequency analysis
# ---------------------------------------------------------------------------
# DISCOVERY SEED LIST
# Broad topic seeds used by discover() to find real-time high-performing
# niches without the user having to search. Covers categories the engine
# already knows about plus open-ended trending areas. The live scoring
# does the heavy lifting — seeds that don't perform get filtered out.
# ---------------------------------------------------------------------------
DISCOVERY_SEEDS = [
    # Finance & Business
    "personal finance tips", "investing for beginners", "side hustle ideas",
    "passive income", "crypto explained", "real estate investing",
    "dropshipping 2025", "freelancing tips",
    # Health & Wellness
    "mental health tips", "home workout", "healthy eating", "sleep tips",
    "meditation for beginners", "weight loss tips",
    # Technology
    "AI tools 2025", "ChatGPT tips", "smartphone tricks",
    "Python for beginners", "software tutorial",
    # Self Improvement
    "productivity tips", "stoicism explained", "book summary",
    "morning routine", "discipline habits",
    # Education & Facts
    "history facts", "science explained", "psychology facts",
    "language learning tips", "did you know facts",
    # Lifestyle
    "minimalism tips", "budget travel", "relationship advice",
    "parenting hacks", "simple living",
    # Emerging / trending broad topics (refresh periodically)
    "digital nomad lifestyle", "financial independence", "longevity tips",
    "AI news", "electric vehicles explained", "remote work tips",
    "climate change facts", "space exploration news",
    "self defence tips", "chess for beginners",
]

TITLE_STOP_WORDS = {
    "the","a","an","and","or","but","in","on","at","to","for","of","with",
    "is","are","was","were","be","been","i","you","we","they","it","this",
    "that","my","your","how","what","why","when","who","which","not","no",
    "do","did","can","will","just","so","if","as","by","from","up","about",
    "into","than","then","its","like","get","got","has","have","had","s",
    "ve","re","ll","t","don","doesn","didn","isn","aren","wasn","won",
    "video","youtube","watch","subscribe","channel","new","best","top",
    "2024","2025","full","part","episode","series",
}


# ===========================================================================
# MAIN ENGINE
# ===========================================================================

class NicheEngine:
    """
    Combined two-layer niche scoring engine.

    Layer 1 (always runs): static monetisation + competition intelligence.
    Layer 2 (when API key provided): live YouTube data validation.

    In offline mode (no API key) the engine falls back gracefully to Layer 1
    only — all existing integrations continue to work unchanged.
    """

    def __init__(self, youtube_api_key: str = None):
        self.api_key   = youtube_api_key or os.environ.get("YOUTUBE_API_KEY")
        self._yt       = None
        self._cache    = {}   # in-memory TTL cache: key → {"data": ..., "expires_at": ISO str}

        # Pre-compute normalisation denominators from static data
        self.max_cpm   = max(n["cpm_high"] for n in NICHE_DATA.values())

        log.info(f"NicheEngine v2 ready — live mode: {'ON' if self.api_key else 'OFF (Layer 1 only)'}")

    # -----------------------------------------------------------------------
    # IN-MEMORY CACHE (replaces MongoDB motor cache — no extra deps needed)
    # -----------------------------------------------------------------------

    def _get_cached(self, keyword: str, plan: str) -> Optional[dict]:
        key = f"{keyword}::{plan}"
        entry = self._cache.get(key)
        if not entry:
            return None
        if entry["expires_at"] < datetime.now(timezone.utc).isoformat():
            del self._cache[key]
            return None
        return entry["data"]

    def _cache_result(self, keyword: str, plan: str, result: dict):
        key = f"{keyword}::{plan}"
        expires = (datetime.now(timezone.utc) + timedelta(hours=CACHE_TTL_HOURS)).isoformat()
        self._cache[key] = {"data": result, "expires_at": expires}

    # -----------------------------------------------------------------------
    # YOUTUBE CLIENT
    # -----------------------------------------------------------------------

    def _get_yt(self):
        if self._yt:
            return self._yt
        if not self.api_key:
            return None
        if not HAS_YT:
            log.warning("google-api-python-client not installed — live mode disabled")
            return None
        self._yt = yt_build("youtube", "v3", developerKey=self.api_key)
        return self._yt

    # =======================================================================
    # PUBLIC API — BACKWARDS-COMPATIBLE WITH v1
    # =======================================================================

    def recommend(self, plan: str, count: int = 10, category: str = None) -> list:
        """
        Synchronous ranked niche list for a plan — Layer 1 only.
        Returns combined_score (normalised 60-100) compatible with the frontend.
        """
        content_type = PLAN_CONTENT_TYPE.get(plan, "combo")
        candidates   = self._filter_by_plan(content_type, category)
        scored       = [self._layer1_score(nid, data) for nid, data in candidates]
        scored.sort(key=lambda x: x["total_score"], reverse=True)

        # Apply same 60-100 normalisation as analyse() so frontend filter works
        l1_max = W_CPM + W_OPPORTUNITY + EVERGREEN_BONUS
        result = []
        for n in scored[:count]:
            raw_sc   = n["_cpm_score"] + n["_opp_score"] + n["_ev_bonus"]
            raw      = round(min(raw_sc / l1_max * 100, 100), 1)
            norm     = ((raw - 40) / (100 - 40)) * 40 + 60
            cs       = round(max(60.0, min(100.0, norm)), 1)
            verdict, verdict_label = self._verdict(cs)
            result.append({
                **n,
                "combined_score":  cs,
                "trend_boost":     0,
                "live_data_used":  False,
                "verdict":         verdict,
                "verdict_label":   verdict_label,
                "niche_id":        n["id"],
                "keyword":         n["label"].lower(),
                "name":            n["label"],
            })
        return result

    def get_niche_detail(self, niche_id: str) -> dict:
        """Full static detail for one niche. Backwards-compatible."""
        if niche_id not in NICHE_DATA:
            return {"error": f"Niche '{niche_id}' not found"}
        data   = NICHE_DATA[niche_id]
        scored = self._layer1_score(niche_id, data)
        scored["example_topics"]         = self._example_topics(niche_id, data)
        scored["monthly_revenue_estimate"] = self._revenue_estimate(data)
        return scored

    def get_categories(self) -> list:
        """All niche categories. Backwards-compatible."""
        return sorted(set(n["category"] for n in NICHE_DATA.values()))

    def compare(self, niche_ids: list) -> list:
        """Side-by-side static comparison. Backwards-compatible."""
        results = [
            self._layer1_score(nid, NICHE_DATA[nid])
            for nid in niche_ids if nid in NICHE_DATA
        ]
        results.sort(key=lambda x: x["total_score"], reverse=True)
        return results

    # -----------------------------------------------------------------------
    # NEW: FULL TWO-LAYER ANALYSIS
    # -----------------------------------------------------------------------

    async def analyse(self, keyword: str, plan: str = "combo_pro",
                      force_refresh: bool = False) -> dict:
        """
        Full combined analysis for a single keyword.

        1. Checks MongoDB cache (returns instantly if fresh).
        2. Runs Layer 1 static scoring (CPM, competition, opportunity).
        3. Fetches live YouTube data and runs Layer 2 scoring.
        4. Merges both layers into a single NicheScore with full breakdown.
        5. Caches result for CACHE_TTL_HOURS hours.

        Returns a complete niche report dict.
        """
        keyword = keyword.strip().lower()

        # ── Cache check ──────────────────────────────────────────────────
        if not force_refresh:
            cached = self._get_cached(keyword, plan)
            if cached:
                log.info(f"Cache hit: '{keyword}'")
                cached["source"] = "cache"
                return cached

        # ── Layer 1: static ─────────────────────────────────────────────
        static = self._static_for_keyword(keyword, plan)

        # ── Layer 2: live YouTube ────────────────────────────────────────
        live = None
        yt   = self._get_yt()
        if yt:
            try:
                live = await asyncio.to_thread(self._fetch_yt_data, keyword)
            except Exception as e:
                log.warning(f"YouTube API error for '{keyword}': {e}")

        # ── Merge scores ─────────────────────────────────────────────────
        result = self._merge(keyword, plan, static, live)

        # ── Cache and return ─────────────────────────────────────────────
        self._cache_result(keyword, plan, result)
        result["source"] = "live"
        return result

    async def analyse_batch(self, keywords: list, plan: str = "combo_pro") -> list:
        """
        Analyse up to MAX_BATCH keywords concurrently.
        Returns list sorted by combined_score descending.
        """
        keywords = keywords[:MAX_BATCH]
        tasks    = [self.analyse(kw, plan) for kw in keywords]
        results  = await asyncio.gather(*tasks, return_exceptions=True)
        valid    = [r for r in results if isinstance(r, dict)]
        valid.sort(key=lambda x: x.get("combined_score", 0), reverse=True)
        return valid

    async def discover(self, plan: str = "combo_pro", top_n: int = 10,
                       min_score: float = 60.0,
                       force_refresh: bool = False) -> dict:
        """
        Real-time niche discovery — no keyword needed from the user.

        Probes DISCOVERY_SEEDS concurrently via the YouTube API, scores each
        one with the full two-layer engine, filters to HOT/strong-MODERATE
        results, and returns the top_n ranked by combined_score.

        Falls back to Layer-1-only offline ranking when YOUTUBE_API_KEY is
        missing (still useful — returns best static recommendations).

        Returns:
          {
            "top_niches":    [ ... ],      # top_n results, score desc
            "total_probed":  int,          # seeds actually scored
            "hot_count":     int,
            "moderate_count":int,
            "plan":          str,
            "live_data_used":bool,
            "generated_at":  str (ISO),
          }
        """
        # Check cache first (keyed on plan + "discovery")
        if not force_refresh:
            cached = self._get_cached(f"__discovery__{plan}", plan)
            if cached:
                cached["source"] = "cache"
                return cached

        yt = self._get_yt()

        if yt:
            # Live mode — probe all seeds concurrently
            tasks   = [self.analyse(seed, plan, force_refresh=True)
                       for seed in DISCOVERY_SEEDS]
            raw     = await asyncio.gather(*tasks, return_exceptions=True)
            results = [r for r in raw if isinstance(r, dict) and
                       r.get("combined_score", 0) >= min_score]
        else:
            # Offline fallback — return Layer-1 static recommendations
            log.info("discover(): no API key — returning Layer-1 rankings")
            content_type = PLAN_CONTENT_TYPE.get(plan, "combo")
            candidates   = self._filter_by_plan(content_type)
            scored       = [self._layer1_score(nid, data)
                            for nid, data in candidates]
            # Wrap to match analyse() output shape so callers get consistent keys
            results = []
            for s in scored:
                l1_max   = W_CPM + W_OPPORTUNITY + EVERGREEN_BONUS
                raw_sc   = s["_cpm_score"] + s["_opp_score"] + s["_ev_bonus"]
                raw_combined = round(min(raw_sc / l1_max * 100, 100), 1)
                _norm        = ((raw_combined - 40) / (100 - 40)) * 40 + 60
                combined     = round(max(60.0, min(100.0, _norm)), 1)
                if combined < min_score:
                    continue
                verdict, verdict_label = self._verdict(combined)
                results.append({
                    "keyword":         s["label"].lower(),
                    "niche_id":        s["id"],
                    "label":           s["label"],
                    "category":        s["category"],
                    "plan":            plan,
                    "combined_score":  combined,
                    "verdict":         verdict,
                    "verdict_label":   verdict_label,
                    "cpm_range":       s["cpm_range"],
                    "competition":     s["competition"],
                    "difficulty":      s["difficulty"],
                    "evergreen":       s["evergreen"],
                    "works_for":       s["works_for"],
                    "live_data_used":  False,
                    "live": {
                        "avg_views": None, "avg_engagement_pct": None,
                        "recency_pct": None, "shortform_pct": None,
                        "top_videos": [], "top_title_words": [],
                        "top_tags": [], "videos_analysed": 0,
                    },
                    "monthly_revenue_estimate": self._revenue_estimate(
                        NICHE_DATA.get(s["id"], {})
                    ),
                    "example_topics": self._example_topics(
                        s["id"], NICHE_DATA.get(s["id"], {})
                    ),
                    "contradiction_warning": None,
                    "analysed_at": datetime.now(timezone.utc).isoformat(),
                })

        results.sort(key=lambda x: x.get("combined_score", 0), reverse=True)
        top      = results[:top_n]
        hot_ct   = sum(1 for r in top if r.get("verdict") == "HOT")
        mod_ct   = sum(1 for r in top if r.get("verdict") == "MODERATE")

        output = {
            "top_niches":     top,
            "total_probed":   len(DISCOVERY_SEEDS) if yt else len(results),
            "hot_count":      hot_ct,
            "moderate_count": mod_ct,
            "plan":           plan,
            "live_data_used": yt is not None,
            "generated_at":   datetime.now(timezone.utc).isoformat(),
            "expires_at":     (datetime.now(timezone.utc) +
                               timedelta(hours=CACHE_TTL_HOURS)).isoformat(),
        }

        self._cache_result(f"__discovery__{plan}", plan, output)
        output["source"] = "live"
        return output

    async def search(self, keyword: str, plan: str = "combo_pro") -> dict:
        """
        Keyword search — user types any topic and gets a full scored report.

        Thin wrapper around analyse() that:
          · Strips and validates the keyword
          · Always goes through the full two-layer engine
          · Returns the same rich report shape as analyse()
          · Adds a `search_suggestions` list of related sub-niches to explore

        This is the method to call from the dashboard search bar.
        """
        keyword = keyword.strip()
        if not keyword:
            return {"error": "Keyword cannot be empty"}
        if len(keyword) > 100:
            return {"error": "Keyword too long (max 100 characters)"}

        result = await self.analyse(keyword, plan)

        # Generate related search suggestions from the keyword
        # These are lightweight — just string combinations, no extra API calls
        words      = keyword.lower().split()
        base       = words[0] if words else keyword
        suggestions = list({
            f"{base} tips",
            f"{base} for beginners",
            f"best {base} advice",
            f"{keyword} explained",
            f"how to {base}",
        } - {keyword.lower()})[:4]   # deduplicate vs original, cap at 4

        result["search_suggestions"] = suggestions
        result["searched_keyword"]   = keyword
        return result

    # =======================================================================
    # LAYER 1 — STATIC SCORING
    # =======================================================================

    def _filter_by_plan(self, content_type: str, category: str = None) -> list:
        out = []
        for nid, data in NICHE_DATA.items():
            if content_type != "combo" and content_type not in data["works_for"]:
                continue
            if category and data["category"] != category:
                continue
            out.append((nid, data))
        return out

    def _layer1_score(self, niche_id: str, data: dict) -> dict:
        """
        Scores a known niche from the static database.
        Returns a rich dict with score breakdown and metadata.
        Out of 100 (before adding live layer).
        """
        # CPM score — midpoint normalised to max, worth 30 pts
        mid_cpm   = (data["cpm_low"] + data["cpm_high"]) / 2
        cpm_score = (mid_cpm / self.max_cpm) * W_CPM

        # Opportunity score — inverse of competition × difficulty, worth 20 pts
        comp_map  = {"low": 1.0, "medium": 0.6, "high": 0.3}
        comp_fac  = comp_map.get(data["competition"], 0.5)
        diff_fac  = 1 - ((data["difficulty"] - 1) / 9)
        opp_score = ((comp_fac + diff_fac) / 2) * W_OPPORTUNITY

        # Evergreen bonus
        ev_bonus  = EVERGREEN_BONUS if data.get("evergreen") else 0

        # Layer 1 total (out of 50 pts — leaves 50 for live layer)
        l1_total  = cpm_score + opp_score + ev_bonus

        return {
            "id":           niche_id,
            "label":        data["label"],
            "category":     data["category"],
            "total_score":  round(min(l1_total, 100), 1),   # v1 compat field
            "score_breakdown": {
                "cpm":            round(cpm_score, 1),
                "opportunity":    round(opp_score, 1),
                "evergreen_bonus":ev_bonus,
            },
            "cpm_range":    f"${data['cpm_low']:.0f}–${data['cpm_high']:.0f}",
            "competition":  data["competition"],
            "difficulty":   data["difficulty"],
            "evergreen":    data.get("evergreen", False),
            "works_for":    data["works_for"],
            "tags":         data["tags"],
            # Raw values for Layer 2 merge
            "_cpm_score":   round(cpm_score, 2),
            "_opp_score":   round(opp_score, 2),
            "_ev_bonus":    ev_bonus,
            "_data":        data,
        }

    def _static_for_keyword(self, keyword: str, plan: str) -> dict:
        """
        Finds the closest matching static niche for a free-text keyword.
        Falls back to neutral defaults if no match.
        """
        content_type = PLAN_CONTENT_TYPE.get(plan, "combo")

        # Exact niche_id match
        if keyword.replace(" ", "_") in NICHE_DATA:
            nid  = keyword.replace(" ", "_")
            return self._layer1_score(nid, NICHE_DATA[nid])

        # Keyword appears in label, tags, or search_keywords
        kw_lower = keyword.lower()
        best_nid, best_data = None, None
        for nid, data in NICHE_DATA.items():
            haystack = (
                data["label"].lower() + " " +
                " ".join(data["tags"]).lower() + " " +
                " ".join(data.get("search_keywords", [])).lower()
            )
            if kw_lower in haystack:
                # Prefer content-type compatible match
                if content_type in data["works_for"] or "combo" in data["works_for"]:
                    best_nid, best_data = nid, data
                    break

        if best_nid:
            return self._layer1_score(best_nid, best_data)

        # No match — return neutral defaults (live layer carries the scoring)
        log.info(f"No static match for '{keyword}' — using neutral defaults")
        return {
            "id":           keyword.replace(" ", "_"),
            "label":        keyword.title(),
            "category":     "Unknown",
            "total_score":  0,
            "score_breakdown": {"cpm": 0, "opportunity": 0, "evergreen_bonus": 0},
            "cpm_range":    "Unknown",
            "competition":  "unknown",
            "difficulty":   5,
            "evergreen":    None,
            "works_for":    ["shorts", "longform", "combo"],
            "tags":         [],
            "_cpm_score":   0,
            "_opp_score":   0,
            "_ev_bonus":    0,
            "_data":        {},
        }

    # =======================================================================
    # LAYER 2 — LIVE YOUTUBE DATA
    # =======================================================================

    def _fetch_yt_data(self, keyword: str) -> dict:
        """
        Synchronous YouTube API call (run in thread via asyncio.to_thread).
        Fetches top MAX_RESULTS videos for the keyword and computes signals.
        """
        yt = self._get_yt()
        if not yt:
            return None

        # ── Search ──────────────────────────────────────────────────────
        search_resp = yt.search().list(
            q=keyword,
            part="id,snippet",
            type="video",
            maxResults=MAX_RESULTS,
            order="relevance",
            relevanceLanguage="en",
        ).execute()

        items = search_resp.get("items", [])
        if not items:
            return None

        video_ids = [i["id"]["videoId"] for i in items if i.get("id", {}).get("videoId")]
        if not video_ids:
            return None

        snippets = {i["id"]["videoId"]: i["snippet"] for i in items if i.get("id", {}).get("videoId")}

        # ── Statistics + Content Details ─────────────────────────────────
        stats_resp = yt.videos().list(
            id=",".join(video_ids),
            part="statistics,contentDetails,snippet",
        ).execute()

        videos_raw = stats_resp.get("items", [])

        # ── Parse each video ─────────────────────────────────────────────
        now    = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=RECENCY_DAYS)

        views_list, likes_list, comments_list, ratios_list = [], [], [], []
        recent_count    = 0
        short_count     = 0
        trending_count  = 0  # recent (last 30 days) videos with >100k views
        all_titles      = []
        all_tags        = []
        top_videos      = []

        for v in videos_raw:
            vid      = v["id"]
            stats    = v.get("statistics", {})
            details  = v.get("contentDetails", {})
            snippet  = v.get("snippet", snippets.get(vid, {}))

            views    = int(stats.get("viewCount",    0))
            likes    = int(stats.get("likeCount",    0))
            comments = int(stats.get("commentCount", 0))
            title    = snippet.get("title", "")
            pub_at   = snippet.get("publishedAt", "")
            tags     = snippet.get("tags", [])
            url      = f"https://youtube.com/watch?v={vid}"
            duration = details.get("duration", "PT0S")

            views_list.append(views)
            likes_list.append(likes)
            comments_list.append(comments)
            ratio = (likes + comments) / max(views, 1)
            ratios_list.append(ratio)

            # Recency + trend boost (recent viral = published in last 30 days AND >100k views)
            try:
                pub_dt = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
                if pub_dt >= cutoff:
                    recent_count += 1
                    if views >= 100_000:
                        trending_count += 1
            except Exception:
                pass

            # Duration → short-form check
            secs = self._iso_duration_to_seconds(duration)
            if secs <= SHORTS_MAX_SECS:
                short_count += 1

            # Title words (for frequency analysis)
            all_titles.append(title)
            all_tags.extend(tags)

            top_videos.append({
                "title":  title,
                "views":  views,
                "likes":  likes,
                "url":    url,
                "published": pub_at[:10] if pub_at else "",
            })

        n = len(videos_raw)
        if n == 0:
            return None

        top_videos.sort(key=lambda x: x["views"], reverse=True)

        # ── Aggregate signals ────────────────────────────────────────────
        avg_views      = sum(views_list) / n
        avg_engagement = sum(ratios_list) / n
        recency_pct    = recent_count / n
        shortform_pct  = short_count / n

        # ── Title word frequency ─────────────────────────────────────────
        all_words = []
        for title in all_titles:
            words = re.findall(r"[a-z]+", title.lower())
            all_words.extend(w for w in words if w not in TITLE_STOP_WORDS and len(w) > 2)
        top_words = [w for w, _ in Counter(all_words).most_common(10)]

        # ── Tag frequency ────────────────────────────────────────────────
        top_tags = [t for t, _ in Counter(all_tags).most_common(10)]

        return {
            "avg_views":        round(avg_views),
            "avg_engagement":   round(avg_engagement, 5),
            "recency_pct":      round(recency_pct, 3),
            "shortform_pct":    round(shortform_pct, 3),
            "trending_count":   trending_count,
            "top_videos":       top_videos[:5],
            "top_title_words":  top_words,
            "top_tags":         top_tags,
            "videos_analysed":  n,
        }

    def _iso_duration_to_seconds(self, iso: str) -> int:
        """Convert YouTube ISO 8601 duration (PT4M13S) to total seconds."""
        match = re.match(
            r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or ""
        )
        if not match:
            return 0
        h = int(match.group(1) or 0)
        m = int(match.group(2) or 0)
        s = int(match.group(3) or 0)
        return h * 3600 + m * 60 + s

    # =======================================================================
    # SCORE MERGE — combines Layer 1 + Layer 2
    # =======================================================================

    def _merge(self, keyword: str, plan: str, static: dict, live: Optional[dict]) -> dict:
        """
        Combines static and live scores into the final NicheScore report.
        If live data is unavailable, falls back to a Layer-1-only score.
        """
        cpm_score = static.get("_cpm_score", 0)
        opp_score = static.get("_opp_score", 0)
        ev_bonus  = static.get("_ev_bonus",  0)

        if live:
            # ── Live signal scoring ──────────────────────────────────────

            # View score (15 pts) — 500K+ views = full points
            view_score = min(live["avg_views"] / 500_000, 1.0) * W_LIVE_VIEWS

            # Engagement score (15 pts) — 5%+ engagement = full points
            eng_score  = min(live["avg_engagement"] / 0.05, 1.0) * W_ENGAGEMENT

            # Recency score (10 pts) — 100% recent = full points
            rec_score  = live["recency_pct"] * W_RECENCY

            # Short-form score (10 pts) — 100% short-form = full points
            sf_score   = live["shortform_pct"] * W_SHORTFORM

            # Trend boost (0-10 pts) — recent viral videos (>100k views, last 30 days)
            # 5+ such videos = max 10 pts; scales linearly below that
            trend_boost = round(min(live.get("trending_count", 0) / 5, 1.0) * 10, 1)

            combined = (
                cpm_score + opp_score + ev_bonus +
                view_score + eng_score + rec_score + sf_score + trend_boost
            )
            combined = round(min(combined, 100), 1)

            score_breakdown = {
                "cpm":            round(cpm_score, 1),
                "opportunity":    round(opp_score, 1),
                "live_views":     round(view_score, 1),
                "live_engagement":round(eng_score, 1),
                "recency":        round(rec_score, 1),
                "short_form_fit": round(sf_score, 1),
                "trend_boost":    trend_boost,
                "evergreen_bonus":ev_bonus,
            }
        else:
            # Layer 1 only — scale 50-pt max up to 100 proportionally
            l1_max   = W_CPM + W_OPPORTUNITY + EVERGREEN_BONUS
            combined = round(min((cpm_score + opp_score + ev_bonus) / l1_max * 100, 100), 1)
            trend_boost = 0
            score_breakdown = {
                "cpm":            round(cpm_score, 1),
                "opportunity":    round(opp_score, 1),
                "evergreen_bonus":ev_bonus,
                "live_views":     None,
                "live_engagement":None,
                "recency":        None,
                "short_form_fit": None,
                "trend_boost":    0,
            }
            view_score = eng_score = rec_score = sf_score = None

        # Normalise raw 0-100 score to display range 60-100.
        # Raw scores of 40+ map linearly to 60-100; anything below 40 floors at 60.
        # Genuinely top niches (raw >85) land at 90+ after normalisation.
        _raw_score = combined
        normalised = ((_raw_score - 40) / (100 - 40)) * 40 + 60
        combined   = round(max(60.0, min(100.0, normalised)), 1)

        verdict, verdict_label = self._verdict(combined)

        # Revenue estimate (from static data if available)
        data = static.get("_data", {})
        revenue = self._revenue_estimate(data) if data else None

        # Example topics
        niche_id = static.get("id", keyword.replace(" ", "_"))
        examples = self._example_topics(niche_id, data) if data else []

        # Contradiction flag — static says trending but live says dead
        contradiction = None
        if live and data:
            static_trend = data.get("trend_score", 5)
            if static_trend >= 8 and live["recency_pct"] < 0.15:
                contradiction = (
                    "Static data rates this niche as high-trend but fewer than 15% "
                    "of recent YouTube videos are newly uploaded — the trend may be fading."
                )
            elif static_trend <= 5 and live["recency_pct"] > 0.6:
                contradiction = (
                    "Static data rates this niche as low-trend but YouTube shows strong "
                    "recent upload activity — this niche may be surging."
                )

        result = {
            # Identity
            "keyword":          keyword,
            "niche_id":         niche_id,
            "label":            static.get("label", keyword.title()),
            "category":         static.get("category", "Unknown"),
            "plan":             plan,

            # Score
            "combined_score":   combined,
            "trend_boost":      trend_boost,
            "verdict":          verdict,
            "verdict_label":    verdict_label,
            "score_breakdown":  score_breakdown,
            "live_data_used":   live is not None,

            # Static intelligence
            "cpm_range":        static.get("cpm_range", "Unknown"),
            "competition":      static.get("competition", "unknown"),
            "difficulty":       static.get("difficulty", 5),
            "evergreen":        static.get("evergreen"),
            "works_for":        static.get("works_for", []),
            "monthly_revenue_estimate": revenue,
            "example_topics":   examples,

            # Live intelligence (None if offline)
            "live": {
                "avg_views":       live["avg_views"]       if live else None,
                "avg_engagement_pct": round(live["avg_engagement"] * 100, 2) if live else None,
                "recency_pct":     round(live["recency_pct"] * 100, 1) if live else None,
                "shortform_pct":   round(live["shortform_pct"] * 100, 1) if live else None,
                "top_videos":      live["top_videos"]      if live else [],
                "top_title_words": live["top_title_words"] if live else [],
                "top_tags":        live["top_tags"]        if live else [],
                "videos_analysed": live["videos_analysed"] if live else 0,
            },

            # Alerts
            "contradiction_warning": contradiction,

            # Metadata
            "analysed_at":  datetime.now(timezone.utc).isoformat(),
            "expires_at":   (datetime.now(timezone.utc) + timedelta(hours=CACHE_TTL_HOURS)).isoformat(),
        }

        return result

    def _verdict(self, score: float) -> tuple:
        if score >= 70:
            return "HOT",      "✅ HOT — push to pipeline immediately"
        elif score >= 45:
            return "MODERATE", "🟡 MODERATE — monitor before committing"
        else:
            return "WEAK",     "❌ WEAK — skip or try a sub-niche"

    # =======================================================================
    # CACHE helpers are defined near __init__ (in-memory dict — no MongoDB)
    # =======================================================================

    # =======================================================================
    # STATIC HELPERS (preserved from v1)
    # =======================================================================

    def _revenue_estimate(self, data: dict) -> Optional[dict]:
        if not data or "cpm_low" not in data:
            return None
        mid_cpm        = (data["cpm_low"] + data["cpm_high"]) / 2
        monthly_views  = data["avg_views_per_video"] * 100 * 0.4
        monthly_rev    = (monthly_views / 1000) * mid_cpm
        return {
            "low":  f"${monthly_rev * 0.5:,.0f}",
            "mid":  f"${monthly_rev:,.0f}",
            "high": f"${monthly_rev * 2:,.0f}",
            "note": "Estimate based on 100-video catalogue, 40% monetised views",
        }

    def _example_topics(self, niche_id: str, data: dict) -> list:
        templates = {
            "personal_finance":  ["5 money mistakes to stop making in your 20s","How I saved $10,000 in 6 months on a low income","The 50/30/20 rule explained in 60 seconds","Why your savings account is losing you money","How to build an emergency fund from $0"],
            "ai_tools":          ["10 AI tools that replaced my $500/month software stack","How I use ChatGPT to save 3 hours every day","The AI tool most people don't know about (but should)","Stop using Google — use these AI tools instead","I automated my whole business with AI (here's how)"],
            "psychology_facts":  ["3 psychology tricks that make people instantly like you","Why your brain lies to you every day","The psychology of procrastination (and how to beat it)","Why smart people make terrible decisions","The dark side of social media on your brain"],
            "book_summaries":    ["Atomic Habits in 3 minutes — the key idea","The 1 lesson from 'Think and Grow Rich' that changed my life","Why 'The Subtle Art of Not Giving a F*ck' is overrated","5 books that will change how you see money","The Psychology of Money — full summary in 5 minutes"],
            "stoicism_philosophy":["Marcus Aurelius' best advice for hard times","3 stoic habits that will transform your mornings","What Epictetus said about anxiety (still relevant today)","The stoic way to deal with toxic people","Stop worrying about things you can't control — stoicism explained"],
            "side_hustles":      ["5 side hustles you can start this weekend with $0","I made $3,000 last month from these 3 apps","The side hustle nobody talks about (but should)","Why most side hustles fail in 90 days","Best side hustles for introverts who hate people"],
            "mental_health":     ["5 signs you're burnt out (not just tired)","How to stop anxious thoughts in under 2 minutes","Why therapy is not what you think it is","The mental health habit that changed my life","How to actually enjoy being alone"],
            "fitness_home":      ["10-minute home workout that actually burns fat","Why I stopped going to the gym (and got fitter)","5 exercises you can do in a hotel room","The only home workout equipment you actually need","How to build muscle at home with no weights"],
        }
        if niche_id in templates:
            return templates[niche_id]
        tag = data.get("tags", [data.get("label", "this topic")])[0]
        return [
            f"5 things nobody tells you about {tag}",
            f"I tried {tag} for 30 days — here's what happened",
            f"The biggest {tag} mistake beginners make",
            f"How to get started with {tag} (complete beginner guide)",
            f"{tag} explained in under 60 seconds",
        ]


# ===========================================================================
# CLI — run directly for quick tests
# ===========================================================================
if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="Niche Engine v2 CLI")
    sub    = parser.add_subparsers(dest="cmd")

    # recommend (Layer 1 — offline, backwards compat)
    rec = sub.add_parser("recommend")
    rec.add_argument("--plan",     default="combo_pro")
    rec.add_argument("--count",    type=int, default=10)
    rec.add_argument("--category", default=None)

    # detail (Layer 1)
    det = sub.add_parser("detail")
    det.add_argument("--niche", required=True)

    # categories
    sub.add_parser("categories")

    # compare (Layer 1)
    cmp = sub.add_parser("compare")
    cmp.add_argument("--niches", required=True)

    # analyse (Layer 1 + 2 — requires YOUTUBE_API_KEY)
    ana = sub.add_parser("analyse")
    ana.add_argument("--keyword", required=True)
    ana.add_argument("--plan", default="combo_pro")

    # batch
    bat = sub.add_parser("batch")
    bat.add_argument("--keywords", required=True, help="comma-separated")
    bat.add_argument("--plan", default="combo_pro")

    # discover — real-time trending niche discovery (no keyword needed)
    dis = sub.add_parser("discover")
    dis.add_argument("--plan",      default="combo_pro")
    dis.add_argument("--top",       type=int,   default=10,   help="number of results")
    dis.add_argument("--min-score", type=float, default=60.0, help="minimum combined score")
    dis.add_argument("--force",     action="store_true",      help="bypass cache")

    # search — keyword search (user-facing)
    srch = sub.add_parser("search")
    srch.add_argument("--keyword", required=True)
    srch.add_argument("--plan", default="combo_pro")

    args   = parser.parse_args()
    engine = NicheEngine()

    if args.cmd == "recommend":
        results = engine.recommend(plan=args.plan, count=args.count, category=args.category)
        print(json.dumps(results, indent=2))

    elif args.cmd == "detail":
        print(json.dumps(engine.get_niche_detail(args.niche), indent=2))

    elif args.cmd == "categories":
        print(json.dumps(engine.get_categories(), indent=2))

    elif args.cmd == "compare":
        ids = [n.strip() for n in args.niches.split(",")]
        print(json.dumps(engine.compare(ids), indent=2))

    elif args.cmd == "analyse":
        result = asyncio.run(engine.analyse(args.keyword, args.plan))
        print(json.dumps(result, indent=2, default=str))

    elif args.cmd == "batch":
        keywords = [k.strip() for k in args.keywords.split(",")]
        results  = asyncio.run(engine.analyse_batch(keywords, args.plan))
        print(json.dumps(results, indent=2, default=str))

    elif args.cmd == "discover":
        result = asyncio.run(
            engine.discover(
                plan=args.plan,
                top_n=args.top,
                min_score=args.min_score,
                force_refresh=args.force,
            )
        )
        print(json.dumps(result, indent=2, default=str))

    elif args.cmd == "search":
        result = asyncio.run(engine.search(args.keyword, args.plan))
        print(json.dumps(result, indent=2, default=str))

    else:
        parser.print_help()
