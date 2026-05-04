"""
Viralityy — Trend Scraper Agent (Tier 2, Agent 5)
==================================================
Monitors Reddit, Google Trends, and Twitter/X to surface topics that are
going viral on the wider internet BEFORE they appear on YouTube — giving
Viralityy channels a 24–48 hour head start on trending content.

Inspired by the ai-engineering-hub "Multiplatform Deep Researcher"
(BrightData multi-source research). This implementation follows the same
multi-source aggregation pattern: each platform is scraped concurrently,
results are scored independently, then merged into a unified Trend Signal
ranked by cross-platform momentum.

WHY THIS IS DIFFERENT FROM THE DAILY TOPIC SCOUT:
  - Topic Scout    → looks at what's already trending ON YouTube
  - Trend Scraper  → looks at what's about to trend ON YouTube
    (Reddit threads blow up → Twitter picks it up → YouTube covers it)
  - Typical lead time: 24–48 hours before the topic peaks on YouTube

SIGNAL SOURCES (run concurrently):

  1. Reddit        — hot posts in niche-relevant subreddits
                     (comment velocity + upvote ratio = engagement signal)
  2. Google Trends — rising search queries in the niche category
                     (rising = queries growing >50% week-over-week)
  3. Twitter/X     — trending hashtags and keyword mentions
                     (tweet velocity + retweet rate = virality signal)

WHAT IT PRODUCES:
  - TrendSignal list ranked by cross-platform momentum score (0–1)
  - Each signal includes: topic, source platforms, momentum score,
    suggested video angle, estimated YouTube timing (when to post),
    and example titles to use immediately

USAGE:
  from trend_scraper_agent import TrendScraperAgent
  agent = TrendScraperAgent()

  # Run for all active users (called by cron at 7 AM daily)
  await agent.run_all()

  # Run for one user (dashboard manual trigger)
  trends = await agent.run_for_user(user_id="abc123")

  # Get stored trend signals for a user
  trends = await agent.get_trends(user_id="abc123")

MONGO COLLECTIONS:
  trend_signals     — stored trend results (TTL: 24 hours)
  pipeline_queue    — top trends injected here for scripting

PLATFORM SETUP:
  Reddit API  : free, requires REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET
  Google Trends: pytrends library — no API key needed (free)
  Twitter/X   : requires TWITTER_BEARER_TOKEN (free basic tier)

  Without platform keys, the agent falls back to Reddit-only mode.
  Reddit alone still provides strong signal for most niches.

DEPENDENCIES:
  - requests        (already in requirements.txt)
  - motor/pymongo   (already in requirements.txt)
  - openai          (already in requirements.txt)
  - pytrends        (NEW — pip install pytrends)
  - REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET  (env)
  - TWITTER_BEARER_TOKEN                    (env, optional)
"""

import os
import asyncio
import logging
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

try:
    from openai import AsyncOpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

try:
    from pytrends.request import TrendReq
    HAS_PYTRENDS = True
except ImportError:
    HAS_PYTRENDS = False

logging.basicConfig(level=logging.INFO, format='[TrendScraper] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
TREND_TTL_HOURS      = 24      # trend signals expire after 24 hours
SIGNALS_PER_USER     = 8       # top trends to surface per run
MIN_CROSS_PLATFORM   = 1       # minimum platforms a topic must appear on
REDDIT_POST_LIMIT    = 25      # posts to pull per subreddit
TRENDS_LOOKBACK_DAYS = 7       # Google Trends lookback window

# Cross-platform momentum weights
PLATFORM_WEIGHTS = {
    "reddit":        0.40,
    "google_trends": 0.35,
    "twitter":       0.25,
}

# Niche → subreddit mapping
# Each niche gets 2–3 subreddits for signal diversity
NICHE_SUBREDDITS = {
    "psychology_facts":  ["psychology", "neuroscience", "selfimprovement"],
    "personal_finance":  ["personalfinance", "financialindependence", "frugal"],
    "investing_basics":  ["investing", "stocks", "Bogleheads"],
    "side_hustles":      ["sidehustle", "passive_income", "Entrepreneur"],
    "mental_health":     ["mentalhealth", "anxiety", "depression"],
    "stoicism_philosophy":["Stoicism", "philosophy", "selfimprovement"],
    "fitness_home":      ["bodyweightfitness", "homegym", "fitness"],
    "book_summaries":    ["books", "suggestmeabook", "GetMotivated"],
    "history_facts":     ["history", "todayilearned", "AskHistorians"],
    "ai_tools":          ["artificial", "ChatGPT", "MachineLearning"],
    "productivity":      ["productivity", "GetDisciplined", "selfimprovement"],
    "real_estate":       ["realestateinvesting", "realestate", "financialindependence"],
    "minimalism":        ["minimalism", "simpleliving", "declutter"],
    "relationships":     ["relationship_advice", "socialskills", "dating_advice"],
    "smartphone_tips":   ["androidquestions", "iphone", "tech"],
    "sleep_optimization":["sleep", "insomnia", "selfimprovement"],
    # Default for unmapped niches
    "_default":          ["selfimprovement", "GetMotivated", "todayilearned"],
}

# Niche → Google Trends category + keywords
NICHE_TRENDS_CONFIG = {
    "psychology_facts":  {"category": 0,   "keywords": ["psychology facts", "human behavior", "brain science"]},
    "personal_finance":  {"category": 7,   "keywords": ["personal finance tips", "money saving", "budgeting"]},
    "investing_basics":  {"category": 7,   "keywords": ["stock market", "ETF investing", "index funds"]},
    "side_hustles":      {"category": 16,  "keywords": ["side hustle", "make money online", "passive income"]},
    "mental_health":     {"category": 0,   "keywords": ["mental health tips", "anxiety relief", "mindfulness"]},
    "stoicism_philosophy":{"category": 0,  "keywords": ["stoicism", "Marcus Aurelius", "stoic mindset"]},
    "fitness_home":      {"category": 20,  "keywords": ["home workout", "no equipment exercise", "HIIT workout"]},
    "ai_tools":          {"category": 5,   "keywords": ["AI tools", "ChatGPT tips", "AI productivity"]},
    "productivity":      {"category": 0,   "keywords": ["productivity tips", "time management", "deep work"]},
    "_default":          {"category": 0,   "keywords": []},
}


# ===========================================================================
# TREND SCRAPER AGENT
# ===========================================================================

class TrendScraperAgent:

    def __init__(self, mongo_uri: str = None, openai_api_key: str = None,
                 reddit_client_id: str = None, reddit_client_secret: str = None,
                 twitter_bearer_token: str = None):
        self.mongo_uri      = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.openai_key     = openai_api_key or os.environ.get("OPENAI_API_KEY")
        self.reddit_id      = reddit_client_id or os.environ.get("REDDIT_CLIENT_ID")
        self.reddit_secret  = reddit_client_secret or os.environ.get("REDDIT_CLIENT_SECRET")
        self.twitter_token  = twitter_bearer_token or os.environ.get("TWITTER_BEARER_TOKEN")
        self._db            = None
        self._openai        = None
        self._reddit_token  = None
        self._reddit_token_expiry = 0

    # -----------------------------------------------------------------------
    # CONNECTIONS
    # -----------------------------------------------------------------------

    async def _get_db(self):
        if self._db:
            return self._db
        if not HAS_MOTOR:
            raise RuntimeError("pip install motor")
        self._db = AsyncIOMotorClient(self.mongo_uri)["viralityy"]
        return self._db

    def _get_openai(self):
        if self._openai:
            return self._openai
        if not HAS_OPENAI or not self.openai_key:
            return None
        self._openai = AsyncOpenAI(api_key=self.openai_key)
        return self._openai

    # -----------------------------------------------------------------------
    # PUBLIC API
    # -----------------------------------------------------------------------

    async def run_all(self) -> dict:
        """
        Runs the trend scraper for every active user.
        Called by the 7 AM cron job (1 hour after Topic Scout).
        """
        db    = await self._get_db()
        users = await db.users.find(
            {"nicheId": {"$exists": True, "$ne": ""}, "plan": {"$ne": "trial"}}
        ).to_list(length=1000)

        results = {
            "users_processed":  0,
            "signals_found":    0,
            "errors":           0,
            "run_at":           datetime.now(timezone.utc).isoformat(),
        }

        tasks = [self._run_for_user_safe(u) for u in users]
        outcomes = await asyncio.gather(*tasks)
        for o in outcomes:
            if o.get("error"):
                results["errors"] += 1
            else:
                results["users_processed"] += 1
                results["signals_found"]   += o.get("signal_count", 0)

        log.info(f"Trend scraper run complete: {results}")
        return results

    async def run_for_user(self, user_id: str) -> list:
        """Scrapes trends for one user and returns the signal list."""
        db   = await self._get_db()
        user = await db.users.find_one({"_id": user_id})
        if not user:
            return []
        return await self._scrape_for_user(db, user)

    async def get_trends(self, user_id: str,
                         min_score: float = 0.0) -> list:
        """Returns stored trend signals for a user."""
        db     = await self._get_db()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=TREND_TTL_HOURS)).isoformat()
        cursor = db.trend_signals.find(
            {"userId": user_id, "scrapedAt": {"$gte": cutoff},
             "momentumScore": {"$gte": min_score}},
            sort=[("momentumScore", -1)]
        ).limit(SIGNALS_PER_USER)
        docs = await cursor.to_list(length=SIGNALS_PER_USER)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    # -----------------------------------------------------------------------
    # CORE SCRAPE PIPELINE
    # -----------------------------------------------------------------------

    async def _run_for_user_safe(self, user: dict) -> dict:
        try:
            signals = await self._scrape_for_user(None, user)
            return {"user_id": str(user["_id"]), "signal_count": len(signals)}
        except Exception as e:
            log.error(f"Trend scrape failed for {user.get('_id')}: {e}")
            return {"error": str(e)}

    async def _scrape_for_user(self, db, user: dict) -> list:
        """Runs all three platform scrapers concurrently for one user."""
        if db is None:
            db = await self._get_db()

        user_id   = str(user["_id"])
        niche_id  = user.get("nicheId", "_default")
        niche_lbl = user.get("nicheName", "General")

        # Run all platform scrapers concurrently
        reddit_task  = asyncio.create_task(self._scrape_reddit(niche_id))
        trends_task  = asyncio.create_task(self._scrape_google_trends(niche_id, niche_lbl))
        twitter_task = asyncio.create_task(self._scrape_twitter(niche_id, niche_lbl))

        reddit_signals, trends_signals, twitter_signals = await asyncio.gather(
            reddit_task, trends_task, twitter_task
        )

        # Merge and score across platforms
        merged = self._merge_signals(
            reddit_signals, trends_signals, twitter_signals, niche_lbl
        )

        if not merged:
            log.info(f"No trend signals for user {user_id} / niche {niche_id}")
            return []

        # Enrich top signals with AI-generated video angles
        top    = merged[:SIGNALS_PER_USER]
        prompt_config = user.get("promptConfig", {})
        enriched = await self._enrich_signals(top, niche_lbl, prompt_config)

        # Store
        await self._store_signals(db, user_id, niche_id, niche_lbl, enriched)

        # Inject top 2 into pipeline_queue
        await self._queue_top_signals(db, user_id, enriched[:2])

        return enriched

    # -----------------------------------------------------------------------
    # PLATFORM 1 — REDDIT
    # -----------------------------------------------------------------------

    async def _scrape_reddit(self, niche_id: str) -> list:
        """
        Fetches hot posts from niche-relevant subreddits.
        Uses Reddit's public JSON API — no library needed.
        Falls back to an authenticated call if credentials are set.
        """
        subreddits = NICHE_SUBREDDITS.get(niche_id, NICHE_SUBREDDITS["_default"])
        all_posts  = []

        headers = {"User-Agent": "Viralityy/1.0 trend-scraper"}

        # Get OAuth token if credentials set
        if self.reddit_id and self.reddit_secret:
            token = await asyncio.to_thread(self._get_reddit_token)
            if token:
                headers["Authorization"] = f"Bearer {token}"

        for sub in subreddits:
            try:
                posts = await asyncio.to_thread(
                    self._fetch_subreddit_hot, sub, headers
                )
                all_posts.extend(posts)
            except Exception as e:
                log.warning(f"Reddit fetch failed for r/{sub}: {e}")

        return all_posts

    def _get_reddit_token(self) -> Optional[str]:
        """Gets a Reddit OAuth2 application-only token."""
        now = time.time()
        if self._reddit_token and now < self._reddit_token_expiry - 60:
            return self._reddit_token
        try:
            resp = requests.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=(self.reddit_id, self.reddit_secret),
                data={"grant_type": "client_credentials"},
                headers={"User-Agent": "Viralityy/1.0"},
                timeout=10,
            )
            data = resp.json()
            self._reddit_token        = data.get("access_token")
            self._reddit_token_expiry = now + data.get("expires_in", 3600)
            return self._reddit_token
        except Exception as e:
            log.warning(f"Reddit token fetch failed: {e}")
            return None

    def _fetch_subreddit_hot(self, subreddit: str, headers: dict) -> list:
        """Fetches hot posts from a subreddit and converts to signal format."""
        base = "https://oauth.reddit.com" if "Authorization" in headers \
               else "https://www.reddit.com"
        url  = f"{base}/r/{subreddit}/hot.json?limit={REDDIT_POST_LIMIT}"
        try:
            resp  = requests.get(url, headers=headers, timeout=12)
            data  = resp.json()
            posts = data.get("data", {}).get("children", [])
            now   = time.time()
            signals = []
            for p in posts:
                post = p.get("data", {})
                age_hours = (now - post.get("created_utc", now)) / 3600
                if age_hours > 72:
                    continue  # skip posts older than 3 days
                upvote_ratio = post.get("upvote_ratio", 0.5)
                score        = post.get("score", 0)
                comments     = post.get("num_comments", 0)
                # Comment velocity: comments per hour
                cmt_velocity = comments / max(age_hours, 1)
                # Reddit momentum: weighted combination
                raw_momentum = (
                    min(score / 5000, 1.0) * 0.4 +
                    upvote_ratio * 0.3 +
                    min(cmt_velocity / 20, 1.0) * 0.3
                )
                signals.append({
                    "topic":        post.get("title", ""),
                    "platform":     "reddit",
                    "subreddit":    subreddit,
                    "raw_momentum": round(raw_momentum, 4),
                    "score":        score,
                    "comments":     comments,
                    "cmt_velocity": round(cmt_velocity, 2),
                    "upvote_ratio": upvote_ratio,
                    "age_hours":    round(age_hours, 1),
                    "url":          f"https://reddit.com{post.get('permalink', '')}",
                })
            return signals
        except Exception as e:
            log.warning(f"r/{subreddit} parse failed: {e}")
            return []

    # -----------------------------------------------------------------------
    # PLATFORM 2 — GOOGLE TRENDS
    # -----------------------------------------------------------------------

    async def _scrape_google_trends(self, niche_id: str,
                                    niche_label: str) -> list:
        """
        Fetches rising search queries from Google Trends for the niche.
        Uses pytrends — no API key required.
        """
        if not HAS_PYTRENDS:
            log.warning("pytrends not installed — skipping Google Trends. pip install pytrends")
            return []

        config   = NICHE_TRENDS_CONFIG.get(niche_id, NICHE_TRENDS_CONFIG["_default"])
        keywords = config["keywords"]
        category = config["category"]

        if not keywords:
            keywords = [niche_label.lower(), niche_label.lower() + " tips"]

        try:
            signals = await asyncio.to_thread(
                self._fetch_trends, keywords, category
            )
            return signals
        except Exception as e:
            log.warning(f"Google Trends failed: {e}")
            return []

    def _fetch_trends(self, keywords: list, category: int) -> list:
        """Synchronous pytrends call."""
        pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 30))
        signals  = []

        # Get related rising queries for each keyword
        for kw in keywords[:3]:  # cap at 3 to avoid rate limits
            try:
                pytrends.build_payload(
                    [kw],
                    cat=category,
                    timeframe=f"now {TRENDS_LOOKBACK_DAYS}-d",
                    geo="",
                )
                related = pytrends.related_queries()
                rising  = related.get(kw, {}).get("rising")

                if rising is not None and not rising.empty:
                    for _, row in rising.head(5).iterrows():
                        topic   = str(row.get("query", ""))
                        value   = int(row.get("value", 0))
                        # value = % increase week-over-week; cap at 500% for normalisation
                        momentum = min(value / 500, 1.0)
                        signals.append({
                            "topic":        topic,
                            "platform":     "google_trends",
                            "parent_kw":    kw,
                            "raw_momentum": round(momentum, 4),
                            "growth_pct":   value,
                            "url":          f"https://trends.google.com/trends/explore?q={topic.replace(' ','+')}&geo=",
                        })

                time.sleep(1.5)  # respect rate limit between queries
            except Exception as e:
                log.warning(f"pytrends error for '{kw}': {e}")

        return signals

    # -----------------------------------------------------------------------
    # PLATFORM 3 — TWITTER / X
    # -----------------------------------------------------------------------

    async def _scrape_twitter(self, niche_id: str,
                              niche_label: str) -> list:
        """
        Fetches recent tweets for niche keywords using Twitter API v2.
        Requires TWITTER_BEARER_TOKEN. Returns empty list if not set.
        """
        if not self.twitter_token:
            log.info("TWITTER_BEARER_TOKEN not set — skipping Twitter")
            return []

        config   = NICHE_TRENDS_CONFIG.get(niche_id, NICHE_TRENDS_CONFIG["_default"])
        keywords = config["keywords"]
        if not keywords:
            keywords = [niche_label.lower()]

        all_signals = []
        for kw in keywords[:2]:  # cap at 2 to stay within free tier limits
            try:
                signals = await asyncio.to_thread(
                    self._fetch_twitter_keyword, kw
                )
                all_signals.extend(signals)
            except Exception as e:
                log.warning(f"Twitter fetch failed for '{kw}': {e}")

        return all_signals

    def _fetch_twitter_keyword(self, keyword: str) -> list:
        """
        Searches recent tweets for a keyword and scores momentum.
        Uses Twitter API v2 recent search endpoint.
        """
        headers = {"Authorization": f"Bearer {self.twitter_token}"}
        query   = f'"{keyword}" -is:retweet lang:en'
        url     = "https://api.twitter.com/2/tweets/search/recent"
        params  = {
            "query":       query,
            "max_results": 100,
            "tweet.fields":"public_metrics,created_at",
        }
        try:
            resp  = requests.get(url, headers=headers, params=params, timeout=12)
            data  = resp.json()
            tweets = data.get("data", [])
            if not tweets:
                return []

            # Aggregate metrics across all tweets for this keyword
            total_retweets = sum(
                t.get("public_metrics", {}).get("retweet_count", 0) for t in tweets
            )
            total_likes    = sum(
                t.get("public_metrics", {}).get("like_count", 0) for t in tweets
            )
            tweet_count    = len(tweets)

            # Momentum: normalised retweet rate + like rate
            momentum = min(
                (total_retweets / max(tweet_count, 1)) / 50 * 0.6 +
                (total_likes    / max(tweet_count, 1)) / 100 * 0.4,
                1.0
            )

            # Use the most-retweeted tweet text as the topic signal
            top_tweet = max(
                tweets,
                key=lambda t: t.get("public_metrics", {}).get("retweet_count", 0)
            )

            return [{
                "topic":        keyword,
                "platform":     "twitter",
                "top_tweet":    top_tweet.get("text", "")[:200],
                "raw_momentum": round(momentum, 4),
                "tweet_count":  tweet_count,
                "total_retweets": total_retweets,
                "total_likes":  total_likes,
                "url":          f"https://twitter.com/search?q={keyword.replace(' ','%20')}",
            }]
        except Exception as e:
            log.warning(f"Twitter search parse error for '{keyword}': {e}")
            return []

    # -----------------------------------------------------------------------
    # SIGNAL MERGE — combines all platforms into unified ranked list
    # -----------------------------------------------------------------------

    def _merge_signals(self, reddit: list, trends: list,
                       twitter: list, niche_label: str) -> list:
        """
        Merges signals from all platforms.
        Topics appearing on multiple platforms get a cross-platform bonus.
        Returns unified list sorted by combined momentum score.
        """
        import re

        def normalise(topic: str) -> str:
            """Normalise topic text for comparison."""
            return re.sub(r"[^a-z0-9\s]", "", topic.lower()).strip()

        # Group by normalised topic across all platforms
        topic_map = defaultdict(lambda: {
            "topic": "", "platforms": [], "raw_scores": [],
            "urls": [], "metadata": {}
        })

        all_signals = (
            [(s, "reddit",        PLATFORM_WEIGHTS["reddit"])        for s in reddit] +
            [(s, "google_trends", PLATFORM_WEIGHTS["google_trends"]) for s in trends] +
            [(s, "twitter",       PLATFORM_WEIGHTS["twitter"])       for s in twitter]
        )

        for signal, platform, weight in all_signals:
            topic     = signal.get("topic", "")
            norm_key  = normalise(topic)[:60]
            if not norm_key:
                continue

            entry = topic_map[norm_key]
            if not entry["topic"]:
                entry["topic"] = topic

            # Keep the most readable version of the topic
            if len(topic) > len(entry["topic"]):
                entry["topic"] = topic

            entry["platforms"].append(platform)
            entry["raw_scores"].append(signal.get("raw_momentum", 0) * weight)
            if signal.get("url"):
                entry["urls"].append(signal["url"])
            entry["metadata"][platform] = {
                k: v for k, v in signal.items()
                if k not in ("topic", "platform", "url", "raw_momentum")
            }

        # Score each merged topic
        merged = []
        for norm_key, entry in topic_map.items():
            platforms    = list(set(entry["platforms"]))
            base_score   = sum(entry["raw_scores"])
            # Cross-platform bonus: +15% per additional platform beyond first
            cross_bonus  = (len(platforms) - 1) * 0.15
            final_score  = min(base_score + cross_bonus, 1.0)

            merged.append({
                "topic":           entry["topic"],
                "momentumScore":   round(final_score, 4),
                "platforms":       platforms,
                "platformCount":   len(platforms),
                "isCrossPlatform": len(platforms) > 1,
                "urls":            entry["urls"][:3],
                "metadata":        entry["metadata"],
            })

        # Sort by momentum, filter minimum cross-platform requirement
        merged = [m for m in merged if m["platformCount"] >= MIN_CROSS_PLATFORM]
        merged.sort(key=lambda x: x["momentumScore"], reverse=True)
        return merged

    # -----------------------------------------------------------------------
    # ENRICHMENT — AI generates video angles and title suggestions
    # -----------------------------------------------------------------------

    async def _enrich_signals(self, signals: list, niche_label: str,
                              prompt_config: dict) -> list:
        """
        Uses OpenAI to generate:
          - A YouTube video angle for each trend signal
          - 2 ready-to-use video title suggestions
          - Estimated timing: when to post to catch the peak
        """
        client = self._get_openai()
        if not client or not signals:
            for s in signals:
                s["videoAngle"]     = f"Cover the trending topic: {s['topic']}"
                s["titleSuggestions"] = [
                    f"The truth about {s['topic'].lower()}",
                    f"Why everyone is talking about {s['topic'].lower()}",
                ]
                s["postTiming"]     = "Post within 24 hours to catch the trend peak"
            return signals

        topic_list = "\n".join([
            f"{i+1}. \"{s['topic']}\" — platforms: {', '.join(s['platforms'])} | score: {s['momentumScore']}"
            for i, s in enumerate(signals)
        ])

        title_pattern = prompt_config.get("title_pattern", "number")
        hook_style    = prompt_config.get("hook_style", "question")

        prompt = f"""You convert trending internet topics into YouTube video ideas for a {niche_label} channel.

These topics are currently trending across Reddit, Google Trends, and/or Twitter:
{topic_list}

For each trend, return a JSON array with:
- "topic": exact topic from above
- "video_angle": specific angle to cover for a YouTube {niche_label} audience (1 sentence)
- "title_suggestions": array of exactly 2 titles (max 70 chars each, {title_pattern} pattern)
- "post_timing": when to post relative to now ("post today", "post within 24h", "post within 48h")
- "why_it_works": one sentence explaining why this will perform for {niche_label} viewers

Return ONLY a JSON array. No markdown, no explanation."""

        try:
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1500,
            )
            raw      = resp.choices[0].message.content.strip()
            enriched = json.loads(raw)
            if isinstance(enriched, list):
                for i, item in enumerate(enriched[:len(signals)]):
                    signals[i]["videoAngle"]       = item.get("video_angle", "")
                    signals[i]["titleSuggestions"] = item.get("title_suggestions", [])
                    signals[i]["postTiming"]       = item.get("post_timing", "post within 24h")
                    signals[i]["whyItWorks"]       = item.get("why_it_works", "")
        except Exception as e:
            log.warning(f"Trend enrichment failed: {e}")
            for s in signals:
                s.setdefault("videoAngle",      f"Cover the trending topic in {niche_label}")
                s.setdefault("titleSuggestions",[f"Why {s['topic'].lower()} is trending right now"])
                s.setdefault("postTiming",      "post within 24h")
                s.setdefault("whyItWorks",      "Cross-platform trend signal")

        return signals

    # -----------------------------------------------------------------------
    # STORAGE
    # -----------------------------------------------------------------------

    async def _store_signals(self, db, user_id: str, niche_id: str,
                             niche_label: str, signals: list):
        """Stores trend signals with TTL."""
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=TREND_TTL_HOURS)).isoformat()
        docs = [
            {
                "userId":           user_id,
                "nicheId":          niche_id,
                "nicheLabel":       niche_label,
                **signal,
                "scrapedAt":        datetime.now(timezone.utc).isoformat(),
                "expiresAt":        expires_at,
                "usedInPipeline":   False,
            }
            for signal in signals
        ]
        if docs:
            await db.trend_signals.insert_many(docs)

    async def _queue_top_signals(self, db, user_id: str, signals: list):
        """Injects top trends into the pipeline queue for scripting."""
        docs = [
            {
                "userId":        user_id,
                "source":        "trend_scraper",
                "title":         (s.get("titleSuggestions") or [s["topic"]])[0],
                "scriptBrief":   s.get("videoAngle", ""),
                "trendReason":   s.get("whyItWorks", ""),
                "platforms":     s.get("platforms", []),
                "momentumScore": s.get("momentumScore", 0),
                "postTiming":    s.get("postTiming", "post within 24h"),
                "status":        "pending",
                "priority":      "high" if s.get("isCrossPlatform") else "normal",
                "queuedAt":      datetime.now(timezone.utc).isoformat(),
            }
            for s in signals
        ]
        if docs:
            await db.pipeline_queue.insert_many(docs)
            log.info(f"Queued {len(docs)} trend signals for pipeline — user {user_id}")

    # -----------------------------------------------------------------------
    # TTL INDEXES
    # -----------------------------------------------------------------------

    async def ensure_indexes(self):
        """Creates MongoDB indexes. Safe to call multiple times."""
        db = await self._get_db()
        await db.trend_signals.create_index("userId",    name="idx_trend_user")
        await db.trend_signals.create_index(
            "expiresAt", expireAfterSeconds=0, name="idx_trend_ttl"
        )
        await db.trend_signals.create_index(
            [("userId", 1), ("momentumScore", -1)],
            name="idx_trend_user_score"
        )
        log.info("Trend scraper indexes confirmed")


# ===========================================================================
# CLI
# ===========================================================================
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Trend Scraper Agent CLI")
    parser.add_argument("--run-all",  action="store_true", help="Run for all users")
    parser.add_argument("--run-user", type=str, metavar="USER_ID")
    parser.add_argument("--trends",   type=str, metavar="USER_ID",
                        help="Get stored trends for user")
    parser.add_argument("--min-score",type=float, default=0.0)
    parser.add_argument("--indexes",  action="store_true")
    args = parser.parse_args()

    agent = TrendScraperAgent()

    if args.indexes:
        asyncio.run(agent.ensure_indexes())
    elif args.run_all:
        result = asyncio.run(agent.run_all())
        print(json.dumps(result, indent=2, default=str))
    elif args.run_user:
        result = asyncio.run(agent.run_for_user(args.run_user))
        print(json.dumps(result, indent=2, default=str))
    elif args.trends:
        result = asyncio.run(agent.get_trends(args.trends, min_score=args.min_score))
        print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()
