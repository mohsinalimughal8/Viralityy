"""
Viralityy — M4a: Niche Suggestion Engine
-----------------------------------------
Scores YouTube niches using pre-loaded CPM data, competition analysis,
trend momentum, and content difficulty. No external API needed (M4b adds
live data later). Returns ranked niche recommendations per user plan.

Usage:
  from niche_engine import NicheEngine
  engine = NicheEngine()
  results = engine.recommend(plan="shorts_pro", count=10)
"""

import json
import math
from datetime import datetime

# ---------------------------------------------------------------------------
# PRE-LOADED NICHE DATABASE
# CPM ranges sourced from industry benchmarks (2024-2025).
# trend_score: 1-10 (10 = fastest growing right now)
# competition: "low" | "medium" | "high"
# difficulty: 1-10 (10 = hardest to rank / get views)
# avg_views_per_video: realistic expectation for new channel in 90 days
# ---------------------------------------------------------------------------
NICHE_DATA = {
    # --- Finance & Business ---
    "personal_finance": {
        "label": "Personal Finance",
        "category": "Finance & Business",
        "cpm_low": 12.0, "cpm_high": 45.0,
        "trend_score": 8,
        "competition": "high",
        "difficulty": 7,
        "avg_views_per_video": 8500,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["budgeting", "saving", "investing", "money tips"],
        "evergreen": True,
    },
    "investing_basics": {
        "label": "Investing for Beginners",
        "category": "Finance & Business",
        "cpm_low": 15.0, "cpm_high": 55.0,
        "trend_score": 7,
        "competition": "high",
        "difficulty": 6,
        "avg_views_per_video": 7200,
        "works_for": ["longform", "combo"],
        "tags": ["stocks", "ETFs", "index funds", "passive income"],
        "evergreen": True,
    },
    "side_hustles": {
        "label": "Side Hustles & Online Income",
        "category": "Finance & Business",
        "cpm_low": 8.0, "cpm_high": 30.0,
        "trend_score": 9,
        "competition": "medium",
        "difficulty": 4,
        "avg_views_per_video": 15000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["make money online", "freelance", "passive income"],
        "evergreen": True,
    },
    "crypto_basics": {
        "label": "Crypto & Web3",
        "category": "Finance & Business",
        "cpm_low": 10.0, "cpm_high": 40.0,
        "trend_score": 6,
        "competition": "medium",
        "difficulty": 5,
        "avg_views_per_video": 9000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["bitcoin", "ethereum", "blockchain", "defi"],
        "evergreen": False,
    },

    # --- Health & Wellness ---
    "mental_health": {
        "label": "Mental Health & Mindfulness",
        "category": "Health & Wellness",
        "cpm_low": 6.0, "cpm_high": 22.0,
        "trend_score": 9,
        "competition": "medium",
        "difficulty": 3,
        "avg_views_per_video": 18000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["anxiety", "therapy", "meditation", "self care"],
        "evergreen": True,
    },
    "fitness_home": {
        "label": "Home Fitness & Workouts",
        "category": "Health & Wellness",
        "cpm_low": 5.0, "cpm_high": 18.0,
        "trend_score": 7,
        "competition": "high",
        "difficulty": 5,
        "avg_views_per_video": 22000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["no equipment", "home workout", "weight loss", "HIIT"],
        "evergreen": True,
    },
    "nutrition_diet": {
        "label": "Nutrition & Healthy Eating",
        "category": "Health & Wellness",
        "cpm_low": 5.0, "cpm_high": 20.0,
        "trend_score": 7,
        "competition": "medium",
        "difficulty": 4,
        "avg_views_per_video": 14000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["meal prep", "clean eating", "diet tips", "recipes"],
        "evergreen": True,
    },
    "sleep_optimization": {
        "label": "Sleep & Recovery",
        "category": "Health & Wellness",
        "cpm_low": 7.0, "cpm_high": 25.0,
        "trend_score": 8,
        "competition": "low",
        "difficulty": 2,
        "avg_views_per_video": 20000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["better sleep", "insomnia", "sleep tips", "recovery"],
        "evergreen": True,
    },

    # --- Technology ---
    "ai_tools": {
        "label": "AI Tools & Productivity",
        "category": "Technology",
        "cpm_low": 10.0, "cpm_high": 38.0,
        "trend_score": 10,
        "competition": "medium",
        "difficulty": 4,
        "avg_views_per_video": 25000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["ChatGPT", "AI apps", "productivity", "automation"],
        "evergreen": False,
    },
    "software_tutorials": {
        "label": "Software & App Tutorials",
        "category": "Technology",
        "cpm_low": 8.0, "cpm_high": 28.0,
        "trend_score": 6,
        "competition": "medium",
        "difficulty": 5,
        "avg_views_per_video": 9500,
        "works_for": ["longform", "combo"],
        "tags": ["how to use", "tutorial", "review", "app guide"],
        "evergreen": True,
    },
    "smartphone_tips": {
        "label": "Smartphone Tips & Tricks",
        "category": "Technology",
        "cpm_low": 4.0, "cpm_high": 14.0,
        "trend_score": 7,
        "competition": "medium",
        "difficulty": 2,
        "avg_views_per_video": 30000,
        "works_for": ["shorts"],
        "tags": ["iPhone tips", "Android tricks", "hidden features"],
        "evergreen": True,
    },
    "coding_beginners": {
        "label": "Learn to Code",
        "category": "Technology",
        "cpm_low": 9.0, "cpm_high": 32.0,
        "trend_score": 7,
        "competition": "high",
        "difficulty": 7,
        "avg_views_per_video": 8000,
        "works_for": ["longform", "combo"],
        "tags": ["Python", "web development", "programming for beginners"],
        "evergreen": True,
    },

    # --- Self Improvement ---
    "productivity": {
        "label": "Productivity & Focus",
        "category": "Self Improvement",
        "cpm_low": 7.0, "cpm_high": 24.0,
        "trend_score": 8,
        "competition": "high",
        "difficulty": 5,
        "avg_views_per_video": 12000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["time management", "deep work", "focus", "habits"],
        "evergreen": True,
    },
    "stoicism_philosophy": {
        "label": "Stoicism & Philosophy",
        "category": "Self Improvement",
        "cpm_low": 5.0, "cpm_high": 18.0,
        "trend_score": 7,
        "competition": "low",
        "difficulty": 2,
        "avg_views_per_video": 28000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["stoic", "Marcus Aurelius", "life lessons", "mindset"],
        "evergreen": True,
    },
    "book_summaries": {
        "label": "Book Summaries & Key Ideas",
        "category": "Self Improvement",
        "cpm_low": 5.0, "cpm_high": 16.0,
        "trend_score": 8,
        "competition": "medium",
        "difficulty": 2,
        "avg_views_per_video": 35000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["book review", "non-fiction", "key lessons", "reading"],
        "evergreen": True,
    },
    "morning_routines": {
        "label": "Morning Routines & Habits",
        "category": "Self Improvement",
        "cpm_low": 5.0, "cpm_high": 15.0,
        "trend_score": 7,
        "competition": "medium",
        "difficulty": 2,
        "avg_views_per_video": 22000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["morning routine", "habits", "discipline", "success"],
        "evergreen": True,
    },

    # --- Education & Facts ---
    "history_facts": {
        "label": "History & Surprising Facts",
        "category": "Education",
        "cpm_low": 4.0, "cpm_high": 12.0,
        "trend_score": 8,
        "competition": "low",
        "difficulty": 2,
        "avg_views_per_video": 45000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["history", "did you know", "facts", "interesting"],
        "evergreen": True,
    },
    "science_explained": {
        "label": "Science Made Simple",
        "category": "Education",
        "cpm_low": 4.0, "cpm_high": 14.0,
        "trend_score": 7,
        "competition": "low",
        "difficulty": 3,
        "avg_views_per_video": 38000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["science", "explained", "how it works", "physics", "biology"],
        "evergreen": True,
    },
    "psychology_facts": {
        "label": "Psychology & Human Behaviour",
        "category": "Education",
        "cpm_low": 5.0, "cpm_high": 18.0,
        "trend_score": 9,
        "competition": "low",
        "difficulty": 2,
        "avg_views_per_video": 50000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["psychology", "human behaviour", "mind tricks", "social skills"],
        "evergreen": True,
    },
    "language_learning": {
        "label": "Language Learning Tips",
        "category": "Education",
        "cpm_low": 4.0, "cpm_high": 14.0,
        "trend_score": 7,
        "competition": "medium",
        "difficulty": 3,
        "avg_views_per_video": 18000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["learn Spanish", "language tips", "fluent", "vocabulary"],
        "evergreen": True,
    },

    # --- Lifestyle ---
    "minimalism": {
        "label": "Minimalism & Simple Living",
        "category": "Lifestyle",
        "cpm_low": 4.0, "cpm_high": 14.0,
        "trend_score": 7,
        "competition": "low",
        "difficulty": 2,
        "avg_views_per_video": 20000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["minimalist", "declutter", "simple life", "less is more"],
        "evergreen": True,
    },
    "travel_tips": {
        "label": "Budget Travel Tips",
        "category": "Lifestyle",
        "cpm_low": 3.0, "cpm_high": 12.0,
        "trend_score": 8,
        "competition": "medium",
        "difficulty": 4,
        "avg_views_per_video": 16000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["travel hacks", "cheap travel", "backpacking", "travel tips"],
        "evergreen": True,
    },
    "relationships": {
        "label": "Relationships & Social Skills",
        "category": "Lifestyle",
        "cpm_low": 5.0, "cpm_high": 16.0,
        "trend_score": 8,
        "competition": "medium",
        "difficulty": 3,
        "avg_views_per_video": 32000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["dating tips", "communication", "friendships", "social skills"],
        "evergreen": True,
    },
    "parenting_tips": {
        "label": "Parenting & Family",
        "category": "Lifestyle",
        "cpm_low": 5.0, "cpm_high": 18.0,
        "trend_score": 7,
        "competition": "low",
        "difficulty": 2,
        "avg_views_per_video": 14000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["parenting hacks", "kids", "family", "toddler tips"],
        "evergreen": True,
    },

    # --- Business & Entrepreneurship ---
    "dropshipping": {
        "label": "Dropshipping & E-commerce",
        "category": "Business",
        "cpm_low": 10.0, "cpm_high": 35.0,
        "trend_score": 7,
        "competition": "high",
        "difficulty": 6,
        "avg_views_per_video": 9000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["dropshipping", "Shopify", "ecommerce", "online store"],
        "evergreen": False,
    },
    "freelancing": {
        "label": "Freelancing & Remote Work",
        "category": "Business",
        "cpm_low": 7.0, "cpm_high": 25.0,
        "trend_score": 8,
        "competition": "medium",
        "difficulty": 3,
        "avg_views_per_video": 12000,
        "works_for": ["shorts", "longform", "combo"],
        "tags": ["freelance", "Upwork", "Fiverr", "remote work", "work from home"],
        "evergreen": True,
    },
    "real_estate": {
        "label": "Real Estate Investing",
        "category": "Business",
        "cpm_low": 14.0, "cpm_high": 50.0,
        "trend_score": 7,
        "competition": "medium",
        "difficulty": 5,
        "avg_views_per_video": 8000,
        "works_for": ["longform", "combo"],
        "tags": ["real estate", "property investing", "rental income", "REITs"],
        "evergreen": True,
    },
}

# ---------------------------------------------------------------------------
# PLAN CONSTRAINTS — mirrors server.js PLAN_DEFS
# ---------------------------------------------------------------------------
PLAN_CONTENT_TYPE = {
    "shorts_starter":    "shorts",
    "shorts_pro":        "shorts",
    "longform_starter":  "longform",
    "longform_pro":      "longform",
    "combo_pro":         "combo",
    "combo_agency":      "combo",
}


class NicheEngine:
    """
    Scores and ranks niches for a given user plan.

    Scoring formula (out of 100):
      - CPM score        (30 pts)  — higher CPM = more ad revenue per 1000 views
      - Trend score      (25 pts)  — how fast this niche is growing right now
      - Opportunity score(25 pts)  — inverse of competition × difficulty
      - View potential   (20 pts)  — avg views per video (normalised)

    Evergreen bonus: +5 pts for niches that stay relevant year-round.
    """

    def __init__(self):
        self.niches = NICHE_DATA
        self.max_cpm = max(n["cpm_high"] for n in self.niches.values())
        self.max_views = max(n["avg_views_per_video"] for n in self.niches.values())

    # ------------------------------------------------------------------
    # PUBLIC API
    # ------------------------------------------------------------------

    def recommend(self, plan: str, count: int = 10, category: str = None) -> list:
        """
        Returns top `count` niches for the given plan, optionally filtered
        by category. Each result includes a full breakdown of its score.
        """
        content_type = PLAN_CONTENT_TYPE.get(plan, "combo")
        candidates = self._filter_by_plan(content_type, category)
        scored = [self._score(niche_id, data) for niche_id, data in candidates]
        scored.sort(key=lambda x: x["total_score"], reverse=True)
        return scored[:count]

    def get_niche_detail(self, niche_id: str) -> dict:
        """Returns full detail for a single niche including example topics."""
        if niche_id not in self.niches:
            return {"error": f"Niche '{niche_id}' not found"}
        data = self.niches[niche_id]
        scored = self._score(niche_id, data)
        scored["example_topics"] = self._generate_example_topics(niche_id, data)
        scored["monthly_revenue_estimate"] = self._estimate_revenue(data)
        return scored

    def get_categories(self) -> list:
        """Returns all available niche categories."""
        return sorted(set(n["category"] for n in self.niches.values()))

    def compare(self, niche_ids: list) -> list:
        """Compare multiple niches side by side."""
        results = []
        for nid in niche_ids:
            if nid in self.niches:
                results.append(self._score(nid, self.niches[nid]))
        results.sort(key=lambda x: x["total_score"], reverse=True)
        return results

    # ------------------------------------------------------------------
    # INTERNAL SCORING
    # ------------------------------------------------------------------

    def _filter_by_plan(self, content_type: str, category: str = None) -> list:
        out = []
        for niche_id, data in self.niches.items():
            if content_type not in data["works_for"] and "combo" not in data["works_for"]:
                continue
            if category and data["category"] != category:
                continue
            out.append((niche_id, data))
        return out

    def _score(self, niche_id: str, data: dict) -> dict:
        # CPM score — use midpoint CPM, normalised to max
        mid_cpm = (data["cpm_low"] + data["cpm_high"]) / 2
        cpm_score = (mid_cpm / self.max_cpm) * 30

        # Trend score — direct (trend_score is 1-10)
        trend_score = (data["trend_score"] / 10) * 25

        # Opportunity score — low competition + low difficulty = high opportunity
        comp_map = {"low": 1.0, "medium": 0.6, "high": 0.3}
        comp_factor = comp_map.get(data["competition"], 0.5)
        diff_factor = 1 - ((data["difficulty"] - 1) / 9)  # 1=1.0, 10=0.0
        opportunity_score = ((comp_factor + diff_factor) / 2) * 25

        # View potential score
        view_score = (data["avg_views_per_video"] / self.max_views) * 20

        # Evergreen bonus
        evergreen_bonus = 5 if data.get("evergreen") else 0

        total = cpm_score + trend_score + opportunity_score + view_score + evergreen_bonus
        total = min(round(total, 1), 100)

        return {
            "id": niche_id,
            "label": data["label"],
            "category": data["category"],
            "total_score": total,
            "score_breakdown": {
                "cpm": round(cpm_score, 1),
                "trend": round(trend_score, 1),
                "opportunity": round(opportunity_score, 1),
                "view_potential": round(view_score, 1),
                "evergreen_bonus": evergreen_bonus,
            },
            "cpm_range": f"${data['cpm_low']:.0f}–${data['cpm_high']:.0f}",
            "competition": data["competition"],
            "difficulty": data["difficulty"],
            "trend_score": data["trend_score"],
            "avg_views_per_video": data["avg_views_per_video"],
            "tags": data["tags"],
            "evergreen": data.get("evergreen", False),
            "works_for": data["works_for"],
        }

    def _generate_example_topics(self, niche_id: str, data: dict) -> list:
        """Generate example video topics for a niche."""
        templates = {
            "personal_finance": [
                "5 money mistakes to stop making in your 20s",
                "How I saved $10,000 in 6 months on a low income",
                "The 50/30/20 rule explained in 60 seconds",
                "Why your savings account is losing you money",
                "How to build an emergency fund from $0",
            ],
            "ai_tools": [
                "10 AI tools that replaced my $500/month software stack",
                "How I use ChatGPT to save 3 hours every day",
                "The AI tool most people don't know about (but should)",
                "Stop using Google — use these AI tools instead",
                "I automated my whole business with AI (here's how)",
            ],
            "psychology_facts": [
                "3 psychology tricks that make people instantly like you",
                "Why your brain lies to you every day",
                "The psychology of procrastination (and how to beat it)",
                "Why smart people make terrible decisions",
                "The dark side of social media on your brain",
            ],
            "book_summaries": [
                "Atomic Habits in 3 minutes — the key idea",
                "The 1 lesson from 'Think and Grow Rich' that changed my life",
                "Why 'The Subtle Art of Not Giving a F*ck' is overrated",
                "5 books that will change how you see money",
                "The Psychology of Money — full summary in 5 minutes",
            ],
            "stoicism_philosophy": [
                "Marcus Aurelius' best advice for hard times",
                "3 stoic habits that will transform your mornings",
                "What Epictetus said about anxiety (still relevant today)",
                "The stoic way to deal with toxic people",
                "Stop worrying about things you can't control — stoicism explained",
            ],
        }
        # Return specific templates if available, else generate from tags
        if niche_id in templates:
            return templates[niche_id]
        tag = data["tags"][0] if data["tags"] else data["label"]
        return [
            f"5 things nobody tells you about {tag}",
            f"I tried {tag} for 30 days — here's what happened",
            f"The biggest {tag} mistake beginners make",
            f"How to get started with {tag} (beginner guide)",
            f"{tag} explained in under 60 seconds",
        ]

    def _estimate_revenue(self, data: dict) -> dict:
        """Rough monthly revenue estimate for a channel with 100 videos."""
        mid_cpm = (data["cpm_low"] + data["cpm_high"]) / 2
        # Conservative: 40% of avg_views reach monetisation threshold
        monthly_views = data["avg_views_per_video"] * 100 * 0.4
        monthly_revenue = (monthly_views / 1000) * mid_cpm
        return {
            "low": f"${monthly_revenue * 0.5:,.0f}",
            "mid": f"${monthly_revenue:,.0f}",
            "high": f"${monthly_revenue * 2:,.0f}",
            "note": "Estimate based on 100-video catalogue, 40% monetised views"
        }


# ---------------------------------------------------------------------------
# SIMPLE CLI TEST — run: python niche_engine.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    engine = NicheEngine()
    print("\n=== TOP 10 NICHES FOR COMBO PRO PLAN ===\n")
    results = engine.recommend(plan="combo_pro", count=10)
    for i, r in enumerate(results, 1):
        print(f"{i:>2}. {r['label']:<35} Score: {r['total_score']:>5} | CPM: {r['cpm_range']:<12} | Competition: {r['competition']}")

    print("\n=== NICHE DETAIL: Psychology Facts ===\n")
    detail = engine.get_niche_detail("psychology_facts")
    print(json.dumps(detail, indent=2))
