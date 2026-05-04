"""
Viralityy — Content Planner Agent (Tier 1, Agent 1)
=====================================================
Generates a complete 30-day video content calendar for a user's channel.

Inspired by the ai-engineering-hub "Content Planner Flow" (CrewAI Flow pattern).
This implementation uses a three-stage planning pipeline:
  Stage 1 — Strategy      : reads niche data + channel config, sets themes
  Stage 2 — Topic Grid    : generates daily video slots with topics
  Stage 3 — Titles/Hooks  : writes scroll-stopping titles and opening hooks

Each stage passes context to the next — the output of the strategy stage
informs topic selection, and topic selection informs title/hook generation.
This produces far better calendars than a single prompt would.

WHAT IT PRODUCES per day:
  - Video title (optimised for click-through)
  - Opening hook (first 5 seconds of script)
  - Content angle (educational / story / listicle / challenge)
  - Format tag  (short / long-form)
  - Post time   (based on AI Optimisation engine's best_post_hour)
  - Keyword tags for SEO
  - Topic cluster (groups related videos for algorithm momentum)

USAGE:
  from content_planner_agent import ContentPlannerAgent
  agent = ContentPlannerAgent()

  # Generate calendar for one user
  calendar = await agent.generate(user_id="abc123")

  # Regenerate from a specific day
  calendar = await agent.generate(user_id="abc123", start_day=15, days=16)

  # Get stored calendar
  cal = await agent.get_calendar(user_id="abc123")

MONGO COLLECTIONS:
  content_calendars   — one doc per user, stores the full 30-day plan
  calendar_slots      — one doc per day per user (for pipeline to consume)

DEPENDENCIES (all already in stack):
  - OpenAI API          (OPENAI_API_KEY in env)
  - MongoDB / Motor     (MONGODB_URI in env)
  - niche_engine_v2     (local, already built)
  - learning_engine     (local, reads prompt_config for optimisation data)
"""

import os
import asyncio
import logging
import json
from datetime import datetime, timezone, timedelta
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

logging.basicConfig(level=logging.INFO, format='[ContentPlanner] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
CALENDAR_DAYS   = 30
CALENDAR_TTL_D  = 35    # auto-delete after this many days (TTL index)

# Content angle types — variety prevents repetitive calendars
ANGLE_TYPES = [
    "listicle",        # "5 reasons why..."
    "story",           # "I tried X for 30 days"
    "educational",     # "How X actually works"
    "challenge",       # "Most people get this wrong"
    "comparison",      # "X vs Y — which is better?"
    "reaction",        # "Scientists discovered..."
    "how_to",          # "Exactly how to..."
    "myth_bust",       # "Stop believing this about X"
]

# Topic clusters — groups related videos so algorithm builds momentum
# Each user's calendar rotates through 4–5 clusters
DEFAULT_CLUSTERS = [
    "core_concept",
    "psychology_insight",
    "practical_tip",
    "surprising_fact",
    "common_mistake",
]

# Post timing defaults (overridden by learning_engine data if available)
DEFAULT_POST_HOUR    = 15   # 3 PM
DEFAULT_BEST_DAYS    = ["tuesday", "wednesday", "thursday"]


# ===========================================================================
# CONTENT PLANNER AGENT
# ===========================================================================

class ContentPlannerAgent:

    def __init__(self, mongo_uri: str = None, openai_api_key: str = None):
        self.mongo_uri   = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.openai_key  = openai_api_key or os.environ.get("OPENAI_API_KEY")
        self._db         = None
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

    def _get_openai(self) -> "AsyncOpenAI":
        if self._openai:
            return self._openai
        if not HAS_OPENAI:
            raise RuntimeError("pip install openai")
        if not self.openai_key:
            raise RuntimeError("OPENAI_API_KEY not set")
        self._openai = AsyncOpenAI(api_key=self.openai_key)
        return self._openai

    # -----------------------------------------------------------------------
    # PUBLIC API
    # -----------------------------------------------------------------------

    async def generate(self, user_id: str,
                       start_day: int = 1,
                       days: int = CALENDAR_DAYS,
                       force: bool = False) -> dict:
        """
        Main entry point. Runs all three planning stages and stores results.

        Args:
            user_id   : Viralityy user ID
            start_day : day of the month to start from (1–30)
            days      : number of days to plan (default 30)
            force     : if True, regenerates even if a current calendar exists

        Returns:
            Full calendar dict with all day slots.
        """
        db = await self._get_db()

        # Return existing calendar if fresh
        if not force:
            existing = await db.content_calendars.find_one(
                {"userId": user_id, "status": "active"}
            )
            if existing:
                log.info(f"Returning existing calendar for user {user_id}")
                existing["_id"] = str(existing["_id"])
                return existing

        # Load user context
        ctx = await self._load_user_context(db, user_id)
        if ctx.get("error"):
            return ctx

        log.info(f"Generating {days}-day calendar for user {user_id} | niche: {ctx['niche_label']}")

        # ── Stage 1: Strategy ──────────────────────────────────────────────
        strategy = await self._stage_strategy(ctx)
        log.info(f"Stage 1 complete: {len(strategy.get('themes', []))} themes")

        # ── Stage 2: Topic Grid ────────────────────────────────────────────
        topic_grid = await self._stage_topic_grid(ctx, strategy, days)
        log.info(f"Stage 2 complete: {len(topic_grid)} topic slots")

        # ── Stage 3: Titles & Hooks ────────────────────────────────────────
        slots = await self._stage_titles_and_hooks(ctx, strategy, topic_grid)
        log.info(f"Stage 3 complete: {len(slots)} fully formed calendar slots")

        # ── Assemble and store calendar ────────────────────────────────────
        calendar = self._assemble_calendar(user_id, ctx, strategy, slots)
        await self._store_calendar(db, user_id, calendar)

        return calendar

    async def get_calendar(self, user_id: str) -> Optional[dict]:
        """Returns the active calendar for a user, or None."""
        db  = await self._get_db()
        doc = await db.content_calendars.find_one({"userId": user_id, "status": "active"})
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc

    async def get_slot(self, user_id: str, day: int) -> Optional[dict]:
        """Returns a single calendar slot by day number."""
        db  = await self._get_db()
        doc = await db.calendar_slots.find_one({"userId": user_id, "dayNumber": day})
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc

    async def get_upcoming_slots(self, user_id: str, limit: int = 7) -> list:
        """Returns the next N unposted slots from today onwards."""
        db      = await self._get_db()
        today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cursor  = db.calendar_slots.find(
            {"userId": user_id, "status": "pending", "scheduledDate": {"$gte": today}},
            sort=[("scheduledDate", 1)]
        ).limit(limit)
        docs = await cursor.to_list(length=limit)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    async def mark_slot_posted(self, user_id: str, day: int,
                               video_id: str = "") -> bool:
        """Mark a calendar slot as posted — called by the posting pipeline."""
        db = await self._get_db()
        res = await db.calendar_slots.update_one(
            {"userId": user_id, "dayNumber": day},
            {"$set": {
                "status":    "posted",
                "postedAt":  datetime.now(timezone.utc).isoformat(),
                "videoId":   video_id,
            }}
        )
        return res.modified_count > 0

    # -----------------------------------------------------------------------
    # STAGE 1 — STRATEGY
    # Determines the content direction for the full calendar period.
    # Sets thematic clusters, content mix ratios, and tone.
    # -----------------------------------------------------------------------

    async def _stage_strategy(self, ctx: dict) -> dict:
        """
        Uses OpenAI to build a high-level content strategy.
        Returns: { themes, content_mix, tone, clusters, posting_days }
        """
        client  = self._get_openai()
        niche   = ctx["niche_label"]
        tags    = ", ".join(ctx.get("niche_tags", [])[:6])
        top_sub = ", ".join(ctx.get("top_subtopics", [])[:5]) or "general topics"
        plan    = ctx["plan"]
        format_ = "Shorts (60–90 second videos)" if "shorts" in plan else \
                  "Long-form (8–25 minute videos)" if "longform" in plan else \
                  "Mixed (Shorts daily + Long-form weekly)"

        prompt = f"""You are a YouTube content strategist for an automated channel in the "{niche}" niche.
Channel format: {format_}
Top performing sub-topics from recent analytics: {top_sub}
Key niche tags: {tags}

Create a content strategy for the next 30 days. Return ONLY valid JSON, no markdown, no explanation.

{{
  "themes": ["<theme 1>", "<theme 2>", "<theme 3>", "<theme 4>", "<theme 5>"],
  "content_mix": {{
    "listicle": 25,
    "story": 20,
    "educational": 20,
    "challenge": 15,
    "myth_bust": 10,
    "how_to": 10
  }},
  "tone": "<warm and conversational | authoritative and data-driven | inspiring and motivational>",
  "clusters": ["<cluster 1>", "<cluster 2>", "<cluster 3>", "<cluster 4>", "<cluster 5>"],
  "posting_days": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
  "strategy_note": "<one sentence on the core content direction for this period>"
}}

Rules:
- content_mix values must sum to 100
- themes must be specific to {niche} (not generic)
- clusters must group related video ideas (e.g. "morning habits", "mindset shifts")
- posting_days must contain exactly 7 days"""

        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=600,
        )
        raw = resp.choices[0].message.content.strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Strategy JSON parse failed — using defaults")
            return {
                "themes": [niche, "practical tips", "surprising facts", "common mistakes", "expert insights"],
                "content_mix": {"listicle": 30, "educational": 25, "story": 20, "how_to": 15, "myth_bust": 10},
                "tone": "warm and conversational",
                "clusters": DEFAULT_CLUSTERS,
                "posting_days": ["tuesday", "wednesday", "thursday", "friday", "saturday"],
                "strategy_note": f"Focus on accessible, engaging content about {niche}.",
            }

    # -----------------------------------------------------------------------
    # STAGE 2 — TOPIC GRID
    # Distributes topics across the calendar following the strategy.
    # -----------------------------------------------------------------------

    async def _stage_topic_grid(self, ctx: dict, strategy: dict,
                                days: int) -> list:
        """
        Generates a grid of topics for each day — without titles yet.
        Returns a list of dicts: [{day, angle, cluster, topic, format}, ...]
        """
        client  = self._get_openai()
        niche   = ctx["niche_label"]
        themes  = strategy.get("themes", [niche])
        mix     = strategy.get("content_mix", {})
        tone    = strategy.get("tone", "engaging")
        plan    = ctx["plan"]
        is_shorts = "shorts" in plan or "combo" in plan

        # Build angle sequence from content_mix percentages
        angle_seq = []
        for angle, pct in mix.items():
            count = max(1, round(days * pct / 100))
            angle_seq.extend([angle] * count)
        angle_seq = (angle_seq * 2)[:days]  # repeat to fill days if needed

        prompt = f"""You are planning the topic grid for a {niche} YouTube channel.
Strategy themes: {', '.join(themes)}
Tone: {tone}
Total days to plan: {days}
Format: {"Shorts (60-90s)" if is_shorts else "Long-form"}

Return ONLY a JSON array with exactly {days} objects. No markdown, no explanation.

Each object:
{{
  "day": <1 to {days}>,
  "cluster": "<which theme cluster this belongs to>",
  "angle": "<listicle|story|educational|challenge|myth_bust|how_to|comparison|reaction>",
  "topic": "<specific topic in 3-8 words>",
  "format": "<short|long>"
}}

Rules:
- topic must be specific to {niche} (never generic)
- vary cluster every 3-4 days to maintain variety
- if format is long, topic should support 8-20 minutes of content
- if format is short, topic must be explainable in 60-90 seconds
- days 1-7 should build awareness (educational, how_to)
- days 8-14 should deepen engagement (story, challenge)
- days 15-21 should provide value (listicle, myth_bust)
- days 22-30 should drive action (comparison, how_to, educational)"""

        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.75,
            max_tokens=3000,
        )
        raw = resp.choices[0].message.content.strip()
        try:
            grid = json.loads(raw)
            if isinstance(grid, list):
                return grid[:days]
        except json.JSONDecodeError:
            pass

        # Fallback grid — deterministic, never crashes
        log.warning("Topic grid JSON parse failed — using fallback grid")
        return [
            {
                "day":     d,
                "cluster": DEFAULT_CLUSTERS[(d - 1) % len(DEFAULT_CLUSTERS)],
                "angle":   ANGLE_TYPES[(d - 1) % len(ANGLE_TYPES)],
                "topic":   f"{niche} — topic {d}",
                "format":  "short" if is_shorts else "long",
            }
            for d in range(1, days + 1)
        ]

    # -----------------------------------------------------------------------
    # STAGE 3 — TITLES & HOOKS
    # Converts the topic grid into scroll-stopping titles and opening hooks.
    # Batches 10 topics per call to stay within token limits.
    # -----------------------------------------------------------------------

    async def _stage_titles_and_hooks(self, ctx: dict, strategy: dict,
                                      topic_grid: list) -> list:
        """
        Generates the title and opening hook for every topic slot.
        Processes in batches of 10 to manage token usage.
        Returns: list of complete slot dicts.
        """
        client   = self._get_openai()
        niche    = ctx["niche_label"]
        tone     = strategy.get("tone", "engaging")
        hook_sty = ctx.get("hook_style", "question")   # from learning_engine
        ttl_pat  = ctx.get("title_pattern", "number")  # from learning_engine

        BATCH = 10
        all_slots = []

        for i in range(0, len(topic_grid), BATCH):
            batch = topic_grid[i:i + BATCH]
            topics_block = json.dumps(batch, indent=2)

            prompt = f"""You write YouTube video titles and hooks for a {niche} channel.
Tone: {tone}
Preferred hook style: {hook_sty} (question | story | shocking_fact | bold_claim)
Preferred title pattern: {ttl_pat} (number | question | challenge | secret)

For each topic in the JSON below, add:
- "title": a compelling YouTube title (max 70 chars, no clickbait, no ALL CAPS)
- "hook": the opening 1-2 sentences of the script (15-25 words, grabs attention immediately)
- "seo_tags": array of 4-6 keyword tags
- "post_time": "{ctx.get('post_hour', DEFAULT_POST_HOUR):02d}:00"

Return ONLY the same JSON array with those fields added. No markdown.

Topics:
{topics_block}"""

            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.8,
                max_tokens=2500,
            )
            raw = resp.choices[0].message.content.strip()
            try:
                enriched = json.loads(raw)
                if isinstance(enriched, list):
                    all_slots.extend(enriched)
                    continue
            except json.JSONDecodeError:
                pass

            # Fallback: keep original batch without titles
            log.warning(f"Title/hook batch {i // BATCH + 1} parse failed — using fallback")
            for item in batch:
                item["title"]     = f"{item.get('topic', niche).title()} — What Nobody Tells You"
                item["hook"]      = f"Most people get this completely wrong about {item.get('topic', niche).lower()}."
                item["seo_tags"]  = [niche.lower(), item.get("cluster", "tips"), "guide", "explained"]
                item["post_time"] = f"{ctx.get('post_hour', DEFAULT_POST_HOUR):02d}:00"
                all_slots.append(item)

        return all_slots

    # -----------------------------------------------------------------------
    # ASSEMBLY — builds the final calendar object and individual slot docs
    # -----------------------------------------------------------------------

    def _assemble_calendar(self, user_id: str, ctx: dict,
                           strategy: dict, slots: list) -> dict:
        """Combines all stages into a clean calendar document."""
        today       = datetime.now(timezone.utc)
        post_days   = strategy.get("posting_days", DEFAULT_BEST_DAYS)
        post_hour   = ctx.get("post_hour", DEFAULT_POST_HOUR)

        assembled_slots = []
        for slot in slots:
            day_num  = slot.get("day", len(assembled_slots) + 1)
            # Calculate actual post date
            offset   = day_num - 1
            post_dt  = today + timedelta(days=offset)
            # Snap to preferred posting day if within 1 day
            day_name = post_dt.strftime("%A").lower()
            scheduled_date = post_dt.replace(
                hour=post_hour, minute=0, second=0, microsecond=0
            ).isoformat()

            assembled_slots.append({
                "dayNumber":        day_num,
                "scheduledDate":    scheduled_date,
                "dayOfWeek":        day_name,
                "isPreferredDay":   day_name in post_days,
                "title":            slot.get("title", ""),
                "hook":             slot.get("hook", ""),
                "topic":            slot.get("topic", ""),
                "angle":            slot.get("angle", "educational"),
                "cluster":          slot.get("cluster", ""),
                "format":           slot.get("format", "short"),
                "seoTags":          slot.get("seo_tags", []),
                "postTime":         slot.get("post_time", f"{post_hour:02d}:00"),
                "status":           "pending",    # pending → scripted → posted
                "scriptId":         None,
                "videoId":          None,
                "postedAt":         None,
            })

        return {
            "userId":      user_id,
            "nicheId":     ctx.get("niche_id", ""),
            "nicheLabel":  ctx.get("niche_label", ""),
            "plan":        ctx.get("plan", ""),
            "status":      "active",
            "strategy":    strategy,
            "totalDays":   len(assembled_slots),
            "slots":       assembled_slots,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "expiresAt":   (datetime.now(timezone.utc) + timedelta(days=CALENDAR_TTL_D)).isoformat(),
        }

    async def _store_calendar(self, db, user_id: str, calendar: dict):
        """Stores the full calendar and writes individual slot documents."""
        # Deactivate old calendars
        await db.content_calendars.update_many(
            {"userId": user_id},
            {"$set": {"status": "archived"}}
        )

        # Insert new calendar
        result = await db.content_calendars.insert_one({
            **calendar,
            "slots": calendar["slots"],  # full slot data on calendar doc
        })
        log.info(f"Calendar stored: {result.inserted_id}")

        # Write individual slot docs for pipeline to consume
        await db.calendar_slots.delete_many({"userId": user_id, "status": "pending"})
        slot_docs = [
            {"userId": user_id, "calendarId": str(result.inserted_id), **slot}
            for slot in calendar["slots"]
        ]
        if slot_docs:
            await db.calendar_slots.insert_many(slot_docs)
        log.info(f"Wrote {len(slot_docs)} individual slot documents")

    # -----------------------------------------------------------------------
    # USER CONTEXT LOADER
    # -----------------------------------------------------------------------

    async def _load_user_context(self, db, user_id: str) -> dict:
        """
        Loads everything the planner needs about the user:
        niche data, plan, AI optimisation config, post timing.
        """
        user = await db.users.find_one({"_id": user_id})
        if not user:
            return {"error": f"User {user_id} not found"}

        niche_id    = user.get("nicheId", "")
        niche_label = user.get("nicheName", "General")
        plan        = user.get("plan", "shorts_starter")

        # Pull niche tags from niche engine data if available
        niche_tags = []
        try:
            from niche_engine_v2 import NICHE_DATA
            niche_tags = NICHE_DATA.get(niche_id, {}).get("tags", [])
        except ImportError:
            pass

        # Pull AI optimisation config (from learning_engine)
        prompt_config = user.get("promptConfig", {})

        return {
            "user_id":       user_id,
            "niche_id":      niche_id,
            "niche_label":   niche_label,
            "plan":          plan,
            "niche_tags":    niche_tags,
            "hook_style":    prompt_config.get("hook_style", "question"),
            "title_pattern": prompt_config.get("title_pattern", "number"),
            "top_subtopics": prompt_config.get("top_subtopics", []),
            "post_hour":     prompt_config.get("best_post_hour", DEFAULT_POST_HOUR),
            "post_days":     prompt_config.get("best_post_day",  "tuesday"),
        }

    # -----------------------------------------------------------------------
    # TTL INDEX — run once at startup
    # -----------------------------------------------------------------------

    async def ensure_indexes(self):
        """Creates MongoDB indexes. Safe to call multiple times."""
        db = await self._get_db()
        await db.content_calendars.create_index("userId", name="idx_cal_user")
        await db.content_calendars.create_index(
            "expiresAt", expireAfterSeconds=0, name="idx_cal_ttl"
        )
        await db.calendar_slots.create_index(
            [("userId", 1), ("scheduledDate", 1)],
            name="idx_slot_user_date"
        )
        await db.calendar_slots.create_index(
            [("userId", 1), ("status", 1)],
            name="idx_slot_user_status"
        )
        log.info("Content calendar indexes confirmed")


# ===========================================================================
# CLI
# ===========================================================================
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Content Planner Agent CLI")
    parser.add_argument("--generate", type=str, metavar="USER_ID",
                        help="Generate 30-day calendar for a user")
    parser.add_argument("--get",      type=str, metavar="USER_ID",
                        help="Get existing calendar for a user")
    parser.add_argument("--upcoming", type=str, metavar="USER_ID",
                        help="Get next 7 upcoming slots for a user")
    parser.add_argument("--days",     type=int, default=30,
                        help="Number of days to generate (default 30)")
    parser.add_argument("--force",    action="store_true",
                        help="Force regenerate even if calendar exists")
    parser.add_argument("--indexes",  action="store_true",
                        help="Create MongoDB indexes and exit")
    args = parser.parse_args()

    agent = ContentPlannerAgent()

    if args.indexes:
        asyncio.run(agent.ensure_indexes())
    elif args.generate:
        result = asyncio.run(
            agent.generate(args.generate, days=args.days, force=args.force)
        )
        print(json.dumps(result, indent=2, default=str))
    elif args.get:
        result = asyncio.run(agent.get_calendar(args.get))
        print(json.dumps(result, indent=2, default=str))
    elif args.upcoming:
        result = asyncio.run(agent.get_upcoming_slots(args.upcoming))
        print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()
