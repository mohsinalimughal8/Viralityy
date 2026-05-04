"""
Viralityy — Daily Topic Scout Agent (Tier 1, Agent 2)
======================================================
Runs every morning (6 AM server time via cron), scans YouTube for what's
trending in each active user's niche, and feeds the best topics directly
into the script pipeline as ready-to-produce titles.

Inspired by the ai-engineering-hub "YouTube Trend Analysis" project
(CrewAI + BrightData). This implementation uses a two-pass analysis:

  Pass 1 — Trending Fetch  : pulls the last 48h of YouTube search results
                             for each active niche's seed keywords
  Pass 2 — Quality Filter  : scores each topic on momentum, freshness,
                             and fit with the user's existing calendar
                             to avoid duplicates and weak topics

WHAT IT PRODUCES per run per user:
  - Top 5 trending topics in their niche, ranked by momentum score
  - Each topic includes: suggested title, estimated viral potential,
    key angle (why it's trending), and a draft script brief
  - Automatically injected into the pipeline_queue collection
    so the script agent picks them up immediately

USAGE:
  from daily_topic_scout import DailyTopicScoutAgent
  agent = DailyTopicScoutAgent()

  # Run for all active users (called by cron at 6 AM)
  await agent.run_all()

  # Run for a single user (manual trigger from dashboard)
  topics = await agent.run_for_user(user_id="abc123")

  # Get today's scouted topics for a user (dashboard display)
  topics = await agent.get_todays_topics(user_id="abc123")

MONGO COLLECTIONS:
  scouted_topics    — daily results per user (TTL: 48 hours)
  pipeline_queue    — topics queued for the script agent

SCHEDULE:
  Called by cron in server.js: '0 6 * * *' (6 AM daily)
  Also callable via POST /api/agents/scout/run

DEPENDENCIES:
  - YouTube Data API v3   (YOUTUBE_API_KEY in env)
  - OpenAI API            (OPENAI_API_KEY in env)
  - MongoDB / Motor       (MONGODB_URI in env)
  - niche_engine_v2       (local, for niche search keywords)
"""

import os
import re
import asyncio
import logging
import json
from datetime import datetime, timezone, timedelta
from collections import Counter
from typing import Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

try:
    from googleapiclient.discovery import build as yt_build
    HAS_YT = True
except ImportError:
    HAS_YT = False

try:
    from openai import AsyncOpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

logging.basicConfig(level=logging.INFO, format='[TopicScout] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
SCOUT_TTL_HOURS    = 48     # scouted topics expire after 48 hours
TOPICS_PER_USER    = 5      # how many topics to surface per run
MAX_YT_RESULTS     = 25     # YouTube results to fetch per keyword
RECENCY_HOURS      = 48     # only videos published in last 48h count as "trending"
MIN_VIEW_VELOCITY  = 500    # minimum views in 48h to be considered trending
MOMENTUM_WEIGHTS   = {
    "view_velocity":   0.40,   # views per hour — strongest signal
    "engagement_rate": 0.30,   # likes+comments / views
    "comment_momentum":0.15,   # comments as proxy for discussion
    "recency":         0.15,   # freshness bonus
}

# ---------------------------------------------------------------------------
# STOP WORDS — filtered from title word frequency analysis
# ---------------------------------------------------------------------------
STOP_WORDS = {
    "the","a","an","and","or","but","in","on","at","to","for","of","with",
    "is","are","was","were","be","been","i","you","we","they","it","this",
    "that","my","your","how","what","why","when","who","which","not","no",
    "do","did","can","will","just","so","if","as","by","from","up","about",
    "into","than","then","its","like","get","got","has","have","had","s",
    "video","youtube","watch","subscribe","channel","new","best","top",
    "2024","2025","full","part","episode","series","don","doesn","didn",
}


# ===========================================================================
# DAILY TOPIC SCOUT AGENT
# ===========================================================================

class DailyTopicScoutAgent:

    def __init__(self, mongo_uri: str = None, youtube_api_key: str = None,
                 openai_api_key: str = None):
        self.mongo_uri   = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.yt_key      = youtube_api_key or os.environ.get("YOUTUBE_API_KEY")
        self.openai_key  = openai_api_key or os.environ.get("OPENAI_API_KEY")
        self._db         = None
        self._yt         = None
        self._openai     = None

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

    def _get_yt(self):
        if self._yt:
            return self._yt
        if not self.yt_key or not HAS_YT:
            return None
        self._yt = yt_build("youtube", "v3", developerKey=self.yt_key)
        return self._yt

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
        Runs the scout for every active user with a confirmed niche.
        Called by the 6 AM cron job. Returns a summary dict.
        """
        db    = await self._get_db()
        users = await db.users.find(
            {"nicheId": {"$exists": True, "$ne": ""},
             "plan":    {"$ne": "trial"}},
        ).to_list(length=1000)

        results = {"users_processed": 0, "topics_found": 0, "errors": 0,
                   "run_at": datetime.now(timezone.utc).isoformat()}

        tasks = [self._run_for_user_safe(u) for u in users]
        outcomes = await asyncio.gather(*tasks)

        for outcome in outcomes:
            if outcome.get("error"):
                results["errors"] += 1
            else:
                results["users_processed"] += 1
                results["topics_found"]    += outcome.get("topic_count", 0)

        log.info(f"Scout run complete: {results}")
        return results

    async def run_for_user(self, user_id: str) -> list:
        """
        Scouts trending topics for a single user.
        Returns list of scouted topic dicts.
        """
        db   = await self._get_db()
        user = await db.users.find_one({"_id": user_id})
        if not user:
            return []
        return await self._scout_user(db, user)

    async def get_todays_topics(self, user_id: str) -> list:
        """Returns today's scouted topics for a user (for dashboard display)."""
        db      = await self._get_db()
        cutoff  = (datetime.now(timezone.utc) - timedelta(hours=SCOUT_TTL_HOURS)).isoformat()
        cursor  = db.scouted_topics.find(
            {"userId": user_id, "scoutedAt": {"$gte": cutoff}},
            sort=[("momentumScore", -1)]
        ).limit(TOPICS_PER_USER)
        docs = await cursor.to_list(length=TOPICS_PER_USER)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    async def get_pipeline_queue(self, user_id: str, limit: int = 10) -> list:
        """Returns pending items in the pipeline queue for a user."""
        db     = await self._get_db()
        cursor = db.pipeline_queue.find(
            {"userId": user_id, "status": "pending"},
            sort=[("queuedAt", -1)]
        ).limit(limit)
        docs = await cursor.to_list(length=limit)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    # -----------------------------------------------------------------------
    # CORE SCOUT LOGIC
    # -----------------------------------------------------------------------

    async def _run_for_user_safe(self, user: dict) -> dict:
        """Wraps _scout_user with error handling for use in gather()."""
        try:
            topics = await self._scout_user(None, user)
            return {"user_id": str(user["_id"]), "topic_count": len(topics)}
        except Exception as e:
            log.error(f"Scout failed for user {user.get('_id')}: {e}")
            return {"error": str(e), "user_id": str(user.get("_id", ""))}

    async def _scout_user(self, db, user: dict) -> list:
        """Full scout pipeline for one user."""
        if db is None:
            db = await self._get_db()

        user_id    = str(user["_id"])
        niche_id   = user.get("nicheId", "")
        niche_label= user.get("nicheName", "")

        # Get search keywords for this niche
        keywords = self._get_niche_keywords(niche_id, niche_label)
        if not keywords:
            log.warning(f"No keywords for user {user_id} niche {niche_id}")
            return []

        # Pass 1: fetch trending videos from YouTube
        raw_videos = await self._fetch_trending_videos(keywords)
        if not raw_videos:
            log.warning(f"No trending videos found for niche {niche_id}")
            return []

        # Pass 2: score and filter
        scored = self._score_videos(raw_videos)

        # Get existing calendar to avoid topic duplication
        existing_titles = await self._get_existing_titles(db, user_id)

        # Filter out duplicates
        filtered = self._deduplicate(scored, existing_titles)

        # Take top N
        top_topics = filtered[:TOPICS_PER_USER]

        if not top_topics:
            return []

        # Enrich with AI-generated title suggestions and briefs
        enriched = await self._enrich_topics(top_topics, niche_label, user)

        # Store scouted topics
        await self._store_topics(db, user_id, niche_id, niche_label, enriched)

        # Inject top 3 into pipeline queue
        await self._queue_for_pipeline(db, user_id, enriched[:3])

        log.info(f"Scout complete for user {user_id}: {len(enriched)} topics found")
        return enriched

    # -----------------------------------------------------------------------
    # PASS 1 — YOUTUBE FETCH
    # -----------------------------------------------------------------------

    async def _fetch_trending_videos(self, keywords: list) -> list:
        """
        Fetches recent videos for each keyword and collects stats.
        Uses asyncio.to_thread to run the synchronous YouTube API calls.
        """
        yt = self._get_yt()
        if not yt:
            log.warning("YouTube API key missing — returning empty trending list")
            return []

        all_videos = []
        cutoff_dt  = datetime.now(timezone.utc) - timedelta(hours=RECENCY_HOURS)
        cutoff_str = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        for keyword in keywords[:4]:  # cap at 4 keywords to manage quota
            try:
                videos = await asyncio.to_thread(
                    self._yt_search_and_stats, yt, keyword, cutoff_str
                )
                all_videos.extend(videos)
            except Exception as e:
                log.warning(f"YT fetch failed for '{keyword}': {e}")

        return all_videos

    def _yt_search_and_stats(self, yt, keyword: str, published_after: str) -> list:
        """
        Synchronous YouTube API call — search + video stats in one pass.
        Fetches only videos published in the last 48 hours.
        """
        search_resp = yt.search().list(
            q=keyword,
            part="id,snippet",
            type="video",
            maxResults=MAX_YT_RESULTS,
            order="viewCount",
            publishedAfter=published_after,
            relevanceLanguage="en",
        ).execute()

        items = search_resp.get("items", [])
        if not items:
            return []

        video_ids = [
            i["id"]["videoId"] for i in items
            if i.get("id", {}).get("videoId")
        ]
        if not video_ids:
            return []

        # Batch stats fetch
        stats_resp = yt.videos().list(
            id=",".join(video_ids),
            part="statistics,contentDetails,snippet",
        ).execute()

        videos = []
        now = datetime.now(timezone.utc)

        for v in stats_resp.get("items", []):
            vid_id    = v["id"]
            stats     = v.get("statistics", {})
            snippet   = v.get("snippet", {})
            pub_str   = snippet.get("publishedAt", "")

            views    = int(stats.get("viewCount",    0))
            likes    = int(stats.get("likeCount",    0))
            comments = int(stats.get("commentCount", 0))
            title    = snippet.get("title", "")
            channel  = snippet.get("channelTitle", "")
            tags     = snippet.get("tags", [])

            # Calculate hours since published — view velocity
            hours_old = 1
            try:
                pub_dt    = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                hours_old = max(1, (now - pub_dt).total_seconds() / 3600)
            except Exception:
                pass

            view_velocity = views / hours_old  # views per hour

            videos.append({
                "videoId":        vid_id,
                "title":          title,
                "channelTitle":   channel,
                "views":          views,
                "likes":          likes,
                "comments":       comments,
                "tags":           tags[:10],
                "publishedAt":    pub_str,
                "hoursOld":       round(hours_old, 1),
                "viewVelocity":   round(view_velocity, 1),
                "url":            f"https://youtube.com/watch?v={vid_id}",
                "keyword":        keyword,
            })

        return videos

    # -----------------------------------------------------------------------
    # PASS 2 — SCORING & FILTERING
    # -----------------------------------------------------------------------

    def _score_videos(self, videos: list) -> list:
        """
        Scores each video on momentum and returns sorted list.
        Deduplicates by video ID first.
        """
        seen    = set()
        unique  = []
        for v in videos:
            if v["videoId"] not in seen:
                seen.add(v["videoId"])
                unique.append(v)

        # Normalise each metric 0–1 across the batch
        def norm(vals):
            mx = max(vals) if vals else 1
            return [v / max(mx, 1) for v in vals]

        velocities  = norm([v["viewVelocity"]   for v in unique])
        eng_rates   = norm([(v["likes"] + v["comments"]) / max(v["views"], 1) for v in unique])
        comment_mts = norm([v["comments"]        for v in unique])
        recency_sc  = norm([1 / max(v["hoursOld"], 1) for v in unique])  # fresher = higher

        for i, v in enumerate(unique):
            v["momentumScore"] = round(
                MOMENTUM_WEIGHTS["view_velocity"]    * velocities[i]   +
                MOMENTUM_WEIGHTS["engagement_rate"]  * eng_rates[i]    +
                MOMENTUM_WEIGHTS["comment_momentum"] * comment_mts[i]  +
                MOMENTUM_WEIGHTS["recency"]          * recency_sc[i],
                4
            )

        # Filter out low-velocity videos
        filtered = [v for v in unique if v["viewVelocity"] >= MIN_VIEW_VELOCITY]
        filtered.sort(key=lambda x: x["momentumScore"], reverse=True)
        return filtered

    def _deduplicate(self, scored: list, existing_titles: set) -> list:
        """
        Filters out topics that are too similar to existing calendar entries.
        Uses simple word overlap — avoids covering the same topic twice.
        """
        def title_words(title: str) -> set:
            words = re.findall(r"[a-z]+", title.lower())
            return {w for w in words if w not in STOP_WORDS and len(w) > 3}

        filtered = []
        for v in scored:
            v_words = title_words(v["title"])
            is_dup  = False
            for existing in existing_titles:
                e_words = title_words(existing)
                overlap = len(v_words & e_words)
                if overlap >= 3:   # 3+ shared meaningful words = likely duplicate
                    is_dup = True
                    break
            if not is_dup:
                filtered.append(v)
        return filtered

    # -----------------------------------------------------------------------
    # ENRICHMENT — AI generates title suggestions and brief
    # -----------------------------------------------------------------------

    async def _enrich_topics(self, topics: list, niche_label: str,
                             user: dict) -> list:
        """
        Uses OpenAI to generate:
          - A Viralityy-optimised title for each trending topic
          - A one-sentence script brief (angle + key insight)
          - Why this topic is trending right now
        """
        client = self._get_openai()
        if not client:
            # Return without AI enrichment
            for t in topics:
                t["suggestedTitle"] = t["title"]
                t["scriptBrief"]    = f"Cover the trending topic: {t['title']}"
                t["trendReason"]    = "Trending based on high recent engagement"
            return topics

        # Build topic list for batch prompt
        topic_list = "\n".join([
            f"{i+1}. \"{t['title']}\" ({t['views']:,} views, {t['viewVelocity']:.0f} views/hr)"
            for i, t in enumerate(topics)
        ])

        prompt_config = user.get("promptConfig", {})
        hook_style    = prompt_config.get("hook_style", "question")
        title_pattern = prompt_config.get("title_pattern", "number")

        prompt = f"""You analyse trending YouTube topics for a {niche_label} channel.

These videos are currently trending (last 48 hours):
{topic_list}

For each video, return a JSON array with:
- "original_title": the exact title above
- "suggested_title": a better title for our channel (max 70 chars, {title_pattern} pattern preferred)
- "script_brief": one sentence — the key angle and insight to cover (max 25 words)
- "trend_reason": one sentence — why this is trending right now (max 20 words)
- "hook_idea": opening 15 words using {hook_style} hook style

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
                for i, item in enumerate(enriched[:len(topics)]):
                    topics[i]["suggestedTitle"] = item.get("suggested_title", topics[i]["title"])
                    topics[i]["scriptBrief"]    = item.get("script_brief",    "")
                    topics[i]["trendReason"]    = item.get("trend_reason",    "")
                    topics[i]["hookIdea"]       = item.get("hook_idea",       "")
        except Exception as e:
            log.warning(f"AI enrichment failed: {e} — using raw titles")
            for t in topics:
                t["suggestedTitle"] = t["title"]
                t["scriptBrief"]    = f"Cover the trending angle: {t['title']}"
                t["trendReason"]    = "Trending based on view velocity"
                t["hookIdea"]       = ""

        return topics

    # -----------------------------------------------------------------------
    # STORAGE
    # -----------------------------------------------------------------------

    async def _store_topics(self, db, user_id: str, niche_id: str,
                            niche_label: str, topics: list):
        """Stores scouted topics with TTL."""
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=SCOUT_TTL_HOURS)).isoformat()
        docs = [
            {
                "userId":        user_id,
                "nicheId":       niche_id,
                "nicheLabel":    niche_label,
                "videoId":       t.get("videoId"),
                "originalTitle": t.get("title"),
                "suggestedTitle":t.get("suggestedTitle"),
                "scriptBrief":   t.get("scriptBrief"),
                "trendReason":   t.get("trendReason"),
                "hookIdea":      t.get("hookIdea"),
                "views":         t.get("views", 0),
                "viewVelocity":  t.get("viewVelocity", 0),
                "momentumScore": t.get("momentumScore", 0),
                "channelTitle":  t.get("channelTitle"),
                "url":           t.get("url"),
                "keyword":       t.get("keyword"),
                "tags":          t.get("tags", []),
                "scoutedAt":     datetime.now(timezone.utc).isoformat(),
                "expiresAt":     expires_at,
                "usedInScript":  False,
            }
            for t in topics
        ]
        if docs:
            await db.scouted_topics.insert_many(docs)

    async def _queue_for_pipeline(self, db, user_id: str, topics: list):
        """
        Injects the top scouted topics into pipeline_queue.
        The Script Research Agent picks these up next.
        """
        docs = [
            {
                "userId":        user_id,
                "source":        "daily_scout",
                "title":         t.get("suggestedTitle", t.get("title")),
                "scriptBrief":   t.get("scriptBrief", ""),
                "hookIdea":      t.get("hookIdea", ""),
                "trendReason":   t.get("trendReason", ""),
                "originalUrl":   t.get("url"),
                "momentumScore": t.get("momentumScore", 0),
                "status":        "pending",  # pending → researching → scripting → done
                "queuedAt":      datetime.now(timezone.utc).isoformat(),
                "priority":      "high",
            }
            for t in topics
        ]
        if docs:
            await db.pipeline_queue.insert_many(docs)
            log.info(f"Queued {len(docs)} topics for pipeline — user {user_id}")

    # -----------------------------------------------------------------------
    # HELPERS
    # -----------------------------------------------------------------------

    def _get_niche_keywords(self, niche_id: str, niche_label: str) -> list:
        """Gets search keywords for a niche from niche_engine_v2 data."""
        try:
            from niche_engine_v2 import NICHE_DATA
            data = NICHE_DATA.get(niche_id, {})
            keywords = data.get("search_keywords", [])
            if keywords:
                return keywords
        except ImportError:
            pass
        # Fallback: derive from label
        words  = niche_label.lower().split()
        base   = " ".join(words[:3])
        return [base, base + " tips", base + " 2025", base + " explained"]

    async def _get_existing_titles(self, db, user_id: str) -> set:
        """Gets titles from the user's existing content calendar."""
        try:
            calendar = await db.content_calendars.find_one(
                {"userId": user_id, "status": "active"}
            )
            if calendar:
                return {slot["title"] for slot in calendar.get("slots", [])}
        except Exception:
            pass
        return set()

    # -----------------------------------------------------------------------
    # TTL INDEX
    # -----------------------------------------------------------------------

    async def ensure_indexes(self):
        """Creates MongoDB indexes. Safe to call multiple times."""
        db = await self._get_db()
        await db.scouted_topics.create_index("userId",    name="idx_scout_user")
        await db.scouted_topics.create_index(
            "expiresAt", expireAfterSeconds=0, name="idx_scout_ttl"
        )
        await db.scouted_topics.create_index(
            [("userId", 1), ("momentumScore", -1)],
            name="idx_scout_user_momentum"
        )
        await db.pipeline_queue.create_index(
            [("userId", 1), ("status", 1), ("queuedAt", -1)],
            name="idx_queue_user_status"
        )
        log.info("Topic scout indexes confirmed")


# ===========================================================================
# CLI
# ===========================================================================
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Daily Topic Scout Agent CLI")
    parser.add_argument("--run-all",    action="store_true", help="Run scout for all active users")
    parser.add_argument("--run-user",   type=str, metavar="USER_ID", help="Run scout for one user")
    parser.add_argument("--topics",     type=str, metavar="USER_ID", help="Get today's topics for user")
    parser.add_argument("--queue",      type=str, metavar="USER_ID", help="Show pipeline queue for user")
    parser.add_argument("--indexes",    action="store_true", help="Create MongoDB indexes and exit")
    args = parser.parse_args()

    agent = DailyTopicScoutAgent()

    if args.indexes:
        asyncio.run(agent.ensure_indexes())
    elif args.run_all:
        result = asyncio.run(agent.run_all())
        print(json.dumps(result, indent=2, default=str))
    elif args.run_user:
        result = asyncio.run(agent.run_for_user(args.run_user))
        print(json.dumps(result, indent=2, default=str))
    elif args.topics:
        result = asyncio.run(agent.get_todays_topics(args.topics))
        print(json.dumps(result, indent=2, default=str))
    elif args.queue:
        result = asyncio.run(agent.get_pipeline_queue(args.queue))
        print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()
