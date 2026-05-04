"""
Viralityy — Competitor Watcher Agent (Tier 1, Agent 3)
=======================================================
Monitors rival YouTube channels in each user's active niche and fires
an alert the moment a competitor posts something with unusually strong
early engagement — so Viralityy can produce a response video before
the opportunity fades.

Inspired by the ai-engineering-hub "Brand Monitoring" project
(automated, continuous multi-entity monitoring system). This
implementation adapts that pattern for YouTube channel surveillance:

  Every 4 hours:
    - Polls all watched competitor channels via YouTube Data API
    - Detects new videos published since last check
    - Calculates an Early Momentum Score (EMS) for each new video
    - Fires alerts for videos that cross the EMS threshold

  EMS (Early Momentum Score) is calculated from:
    - Views per hour since posting       (40%)
    - Engagement rate (likes/comments)   (30%)
    - Comment velocity                   (20%)
    - Thumbnail CTR proxy (title CTR)    (10%)

  Alert types:
    - "viral_signal"    : EMS > 0.75 — something is going viral right now
    - "strong_post"     : EMS > 0.50 — above-average performance
    - "competitor_trend": same topic detected on 2+ channels this week

USAGE:
  from competitor_watcher_agent import CompetitorWatcherAgent
  agent = CompetitorWatcherAgent()

  # Add competitor channels to watch
  await agent.add_competitor(user_id, channel_id="UCxxxxxx", channel_name="PsychFacts")

  # Run the monitoring check (called by cron every 4 hours)
  await agent.run_all()

  # Get alerts for a user
  alerts = await agent.get_alerts(user_id, unread_only=True)

  # Get all watched competitors for a user
  competitors = await agent.get_competitors(user_id)

  # Mark alerts as read
  await agent.mark_alerts_read(user_id)

MONGO COLLECTIONS:
  competitor_channels   — channels being watched (per user)
  competitor_videos     — videos seen from watched channels
  competitor_alerts     — alerts generated (dashboard notifications)

SCHEDULE:
  Called by cron: '0 */4 * * *' (every 4 hours)

DEPENDENCIES:
  - YouTube Data API v3   (YOUTUBE_API_KEY in env)
  - OpenAI API            (OPENAI_API_KEY in env) — for alert analysis
  - MongoDB / Motor       (MONGODB_URI in env)
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
    from googleapiclient.discovery import build as yt_build
    HAS_YT = True
except ImportError:
    HAS_YT = False

try:
    from openai import AsyncOpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

logging.basicConfig(level=logging.INFO, format='[CompetitorWatcher] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
CHECK_WINDOW_HOURS  = 4     # how far back to look for new videos each run
ALERT_TTL_DAYS      = 14    # alerts auto-delete after 14 days
VIDEO_TTL_DAYS      = 30    # competitor video records kept for 30 days
MAX_COMPETITORS     = 20    # max competitor channels per user
MAX_VIDEOS_PER_CH   = 10    # videos fetched per channel per check

# EMS thresholds
EMS_VIRAL_THRESHOLD  = 0.75
EMS_STRONG_THRESHOLD = 0.50

# EMS component weights
EMS_WEIGHTS = {
    "view_velocity":   0.40,
    "engagement_rate": 0.30,
    "comment_velocity":0.20,
    "recency_bonus":   0.10,
}

# Benchmark views/hr for normalisation (average for a mid-tier channel)
BENCHMARK_VIEWS_PER_HOUR = 200


# ===========================================================================
# COMPETITOR WATCHER AGENT
# ===========================================================================

class CompetitorWatcherAgent:

    def __init__(self, mongo_uri: str = None, youtube_api_key: str = None,
                 openai_api_key: str = None):
        self.mongo_uri  = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.yt_key     = youtube_api_key or os.environ.get("YOUTUBE_API_KEY")
        self.openai_key = openai_api_key or os.environ.get("OPENAI_API_KEY")
        self._db        = None
        self._yt        = None
        self._openai    = None

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
    # COMPETITOR MANAGEMENT
    # -----------------------------------------------------------------------

    async def add_competitor(self, user_id: str, channel_id: str,
                             channel_name: str = "",
                             notes: str = "") -> dict:
        """
        Adds a YouTube channel to the watch list.
        Verifies the channel exists via the API before adding.
        Returns the created competitor record.
        """
        db = await self._get_db()

        # Check limit
        count = await db.competitor_channels.count_documents({"userId": user_id})
        if count >= MAX_COMPETITORS:
            return {"error": f"Maximum {MAX_COMPETITORS} competitor channels per account"}

        # Check not already watching
        existing = await db.competitor_channels.find_one(
            {"userId": user_id, "channelId": channel_id}
        )
        if existing:
            return {"error": "Already watching this channel"}

        # Verify channel via API and fetch metadata
        metadata = await self._fetch_channel_metadata(channel_id)
        if not metadata:
            return {"error": f"Channel {channel_id} not found on YouTube"}

        doc = {
            "userId":          user_id,
            "channelId":       channel_id,
            "channelName":     metadata.get("title") or channel_name,
            "subscriberCount": metadata.get("subscriberCount", 0),
            "videoCount":      metadata.get("videoCount", 0),
            "thumbnailUrl":    metadata.get("thumbnailUrl", ""),
            "notes":           notes,
            "addedAt":         datetime.now(timezone.utc).isoformat(),
            "lastCheckedAt":   None,
            "alertCount":      0,
            "active":          True,
        }
        result = await db.competitor_channels.insert_one(doc)
        doc["_id"] = str(result.inserted_id)
        log.info(f"Added competitor {metadata.get('title')} for user {user_id}")
        return doc

    async def remove_competitor(self, user_id: str, channel_id: str) -> bool:
        """Removes a channel from the watch list."""
        db  = await self._get_db()
        res = await db.competitor_channels.update_one(
            {"userId": user_id, "channelId": channel_id},
            {"$set": {"active": False}}
        )
        return res.modified_count > 0

    async def get_competitors(self, user_id: str) -> list:
        """Returns all active competitor channels for a user."""
        db     = await self._get_db()
        cursor = db.competitor_channels.find(
            {"userId": user_id, "active": True},
            sort=[("channelName", 1)]
        )
        docs = await cursor.to_list(length=MAX_COMPETITORS)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    # -----------------------------------------------------------------------
    # MONITORING RUN
    # -----------------------------------------------------------------------

    async def run_all(self) -> dict:
        """
        Checks all watched channels for new videos.
        Called by cron every 4 hours. Returns a summary.
        """
        db    = await self._get_db()
        users = await db.users.find(
            {"plan": {"$ne": "trial"}}
        ).to_list(length=1000)

        results = {
            "users_checked":   0,
            "channels_checked":0,
            "new_videos_found":0,
            "alerts_fired":    0,
            "run_at":          datetime.now(timezone.utc).isoformat(),
        }

        for user in users:
            user_id     = str(user["_id"])
            competitors = await self.get_competitors(user_id)
            if not competitors:
                continue

            for channel in competitors:
                try:
                    r = await self._check_channel(db, user_id, channel)
                    results["channels_checked"]  += 1
                    results["new_videos_found"]  += r["new_videos"]
                    results["alerts_fired"]      += r["alerts_fired"]
                except Exception as e:
                    log.error(f"Channel check failed {channel['channelId']}: {e}")

            results["users_checked"] += 1

        log.info(f"Competitor watch run: {results}")
        return results

    async def _check_channel(self, db, user_id: str, channel: dict) -> dict:
        """
        Checks one competitor channel for new videos since last check.
        Scores new videos and fires alerts where EMS crosses thresholds.
        """
        channel_id   = channel["channelId"]
        channel_name = channel["channelName"]
        last_checked = channel.get("lastCheckedAt")

        # Only look at videos in the last check window
        lookback = CHECK_WINDOW_HOURS if last_checked else 24
        cutoff   = datetime.now(timezone.utc) - timedelta(hours=lookback)

        # Fetch recent videos from this channel
        yt = self._get_yt()
        if not yt:
            return {"new_videos": 0, "alerts_fired": 0}

        videos = await asyncio.to_thread(
            self._fetch_channel_videos, yt, channel_id, cutoff
        )

        new_count    = 0
        alert_count  = 0
        cross_channel_topics = await self._get_recent_topics_across_users(db, user_id)

        for video in videos:
            # Skip if already seen
            seen = await db.competitor_videos.find_one({"videoId": video["videoId"]})
            if seen:
                continue

            # Score and store
            ems = self._calculate_ems(video)
            video["ems"] = ems
            video["userId"]      = user_id
            video["channelName"] = channel_name
            video["detectedAt"]  = datetime.now(timezone.utc).isoformat()
            video["expiresAt"]   = (datetime.now(timezone.utc) + timedelta(days=VIDEO_TTL_DAYS)).isoformat()

            await db.competitor_videos.insert_one(video)
            new_count += 1

            # Check if same topic is trending across channels (cross-channel signal)
            is_cross_channel = self._check_cross_channel(video["title"], cross_channel_topics)

            # Fire alert if EMS crosses threshold
            if ems >= EMS_VIRAL_THRESHOLD:
                await self._fire_alert(db, user_id, video, "viral_signal", is_cross_channel)
                alert_count += 1
            elif ems >= EMS_STRONG_THRESHOLD:
                await self._fire_alert(db, user_id, video, "strong_post", is_cross_channel)
                alert_count += 1
            elif is_cross_channel:
                await self._fire_alert(db, user_id, video, "competitor_trend", True)
                alert_count += 1

        # Update last checked timestamp
        await db.competitor_channels.update_one(
            {"userId": user_id, "channelId": channel_id},
            {"$set": {"lastCheckedAt": datetime.now(timezone.utc).isoformat()},
             "$inc": {"alertCount": alert_count}}
        )

        return {"new_videos": new_count, "alerts_fired": alert_count}

    def _fetch_channel_videos(self, yt, channel_id: str,
                              cutoff: datetime) -> list:
        """Synchronous YouTube API call to get recent channel videos."""
        try:
            # Get the channel's uploads playlist ID
            ch_resp = yt.channels().list(
                id=channel_id,
                part="contentDetails,statistics"
            ).execute()

            items = ch_resp.get("items", [])
            if not items:
                return []

            uploads_pl = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
            sub_count  = int(items[0].get("statistics", {}).get("subscriberCount", 0))

            # Get recent videos from uploads playlist
            pl_resp = yt.playlistItems().list(
                playlistId=uploads_pl,
                part="snippet",
                maxResults=MAX_VIDEOS_PER_CH,
            ).execute()

            video_ids = [
                item["snippet"]["resourceId"]["videoId"]
                for item in pl_resp.get("items", [])
                if item.get("snippet", {}).get("resourceId", {}).get("videoId")
            ]
            if not video_ids:
                return []

            # Get stats for those videos
            stats_resp = yt.videos().list(
                id=",".join(video_ids),
                part="statistics,snippet,contentDetails"
            ).execute()

            cutoff_str = cutoff.isoformat().replace("+00:00", "Z")
            videos     = []
            now        = datetime.now(timezone.utc)

            for v in stats_resp.get("items", []):
                snippet   = v.get("snippet", {})
                stats     = v.get("statistics", {})
                pub_str   = snippet.get("publishedAt", "")

                # Only include videos within the check window
                try:
                    pub_dt    = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                    if pub_dt < cutoff:
                        continue
                    hours_old = max(1, (now - pub_dt).total_seconds() / 3600)
                except Exception:
                    continue

                views    = int(stats.get("viewCount",    0))
                likes    = int(stats.get("likeCount",    0))
                comments = int(stats.get("commentCount", 0))

                videos.append({
                    "videoId":         v["id"],
                    "channelId":       channel_id,
                    "title":           snippet.get("title", ""),
                    "description":     snippet.get("description", "")[:500],
                    "publishedAt":     pub_str,
                    "hoursOld":        round(hours_old, 1),
                    "views":           views,
                    "likes":           likes,
                    "comments":        comments,
                    "tags":            snippet.get("tags", [])[:10],
                    "url":             f"https://youtube.com/watch?v={v['id']}",
                    "channelSubscribers": sub_count,
                })
            return videos

        except Exception as e:
            log.warning(f"Channel video fetch failed for {channel_id}: {e}")
            return []

    # -----------------------------------------------------------------------
    # EMS CALCULATION
    # -----------------------------------------------------------------------

    def _calculate_ems(self, video: dict) -> float:
        """
        Early Momentum Score — how fast this video is gaining traction.
        Returns a 0.0–1.0 score.
        """
        hours     = max(1, video.get("hoursOld", 1))
        views     = video.get("views",    0)
        likes     = video.get("likes",    0)
        comments  = video.get("comments", 0)

        view_vel  = views / hours
        eng_rate  = (likes + comments) / max(views, 1)
        comment_v = comments / hours
        recency   = 1 / hours  # fresher = higher

        # Normalise against benchmarks (0–1 clipped)
        norm_view  = min(view_vel  / BENCHMARK_VIEWS_PER_HOUR, 1.0)
        norm_eng   = min(eng_rate  / 0.05, 1.0)    # 5% = full score
        norm_cmt   = min(comment_v / 10.0, 1.0)    # 10 comments/hr = full score
        norm_rec   = min(recency   / (1/2), 1.0)   # 2h old = full recency score

        ems = (
            EMS_WEIGHTS["view_velocity"]    * norm_view +
            EMS_WEIGHTS["engagement_rate"]  * norm_eng  +
            EMS_WEIGHTS["comment_velocity"] * norm_cmt  +
            EMS_WEIGHTS["recency_bonus"]    * norm_rec
        )
        return round(ems, 4)

    # -----------------------------------------------------------------------
    # ALERT SYSTEM
    # -----------------------------------------------------------------------

    async def _fire_alert(self, db, user_id: str, video: dict,
                          alert_type: str, is_cross_channel: bool):
        """Creates an alert document and optionally enriches it with AI insight."""
        alert_labels = {
            "viral_signal":    "Going viral right now",
            "strong_post":     "Strong early performance",
            "competitor_trend":"Multiple channels covering this",
        }
        priority = "critical" if alert_type == "viral_signal" else \
                   "high"     if alert_type == "competitor_trend" else "normal"

        # AI-generated action suggestion
        suggestion = await self._generate_action_suggestion(video, alert_type)

        alert = {
            "userId":           user_id,
            "alertType":        alert_type,
            "alertLabel":       alert_labels.get(alert_type, alert_type),
            "priority":         priority,
            "channelId":        video.get("channelId"),
            "channelName":      video.get("channelName"),
            "videoId":          video.get("videoId"),
            "videoTitle":       video.get("title"),
            "videoUrl":         video.get("url"),
            "ems":              video.get("ems", 0),
            "views":            video.get("views", 0),
            "hoursOld":         video.get("hoursOld", 0),
            "isCrossChannel":   is_cross_channel,
            "actionSuggestion": suggestion,
            "read":             False,
            "actedOn":          False,
            "firedAt":          datetime.now(timezone.utc).isoformat(),
            "expiresAt":        (datetime.now(timezone.utc) + timedelta(days=ALERT_TTL_DAYS)).isoformat(),
        }
        await db.competitor_alerts.insert_one(alert)
        log.info(f"Alert fired: {alert_type} | {video.get('title', '')[:50]} | EMS {video.get('ems',0)}")

    async def _generate_action_suggestion(self, video: dict,
                                          alert_type: str) -> str:
        """Uses OpenAI to suggest what action to take based on the alert."""
        client = self._get_openai()
        if not client:
            return "Consider producing a response video on this topic."

        title   = video.get("title", "")
        views   = video.get("views", 0)
        hours   = video.get("hoursOld", 1)
        channel = video.get("channelName", "a competitor")

        prompt = f"""{channel} just posted "{title}" — it has {views:,} views in {hours:.0f} hours.
Alert type: {alert_type}

In one sentence (max 20 words), what specific action should a competing YouTube channel take right now?
Return ONLY the one sentence. No markdown."""

        try:
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.6,
                max_tokens=60,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            return "Produce a response video on this topic within 24 hours."

    # -----------------------------------------------------------------------
    # ALERT RETRIEVAL
    # -----------------------------------------------------------------------

    async def get_alerts(self, user_id: str, unread_only: bool = False,
                         limit: int = 20) -> list:
        """Returns alerts for a user, most recent first."""
        db     = await self._get_db()
        query  = {"userId": user_id}
        if unread_only:
            query["read"] = False
        cursor = db.competitor_alerts.find(
            query, sort=[("firedAt", -1)]
        ).limit(limit)
        docs = await cursor.to_list(length=limit)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    async def get_unread_count(self, user_id: str) -> int:
        """Returns the count of unread alerts."""
        db = await self._get_db()
        return await db.competitor_alerts.count_documents(
            {"userId": user_id, "read": False}
        )

    async def mark_alerts_read(self, user_id: str,
                               alert_ids: list = None) -> int:
        """
        Marks alerts as read.
        If alert_ids is provided, only marks those alerts.
        If None, marks all unread alerts for the user.
        """
        db    = await self._get_db()
        query = {"userId": user_id, "read": False}
        if alert_ids:
            from bson import ObjectId
            query["_id"] = {"$in": [ObjectId(aid) for aid in alert_ids]}
        res = await db.competitor_alerts.update_many(
            query, {"$set": {"read": True}}
        )
        return res.modified_count

    async def mark_acted_on(self, user_id: str, alert_id: str) -> bool:
        """Marks a specific alert as acted on (user produced a response video)."""
        db = await self._get_db()
        from bson import ObjectId
        res = await db.competitor_alerts.update_one(
            {"_id": ObjectId(alert_id), "userId": user_id},
            {"$set": {"actedOn": True, "actedAt": datetime.now(timezone.utc).isoformat()}}
        )
        return res.modified_count > 0

    async def get_recent_competitor_videos(self, user_id: str,
                                           limit: int = 20) -> list:
        """Returns recently detected competitor videos for a user."""
        db     = await self._get_db()
        cursor = db.competitor_videos.find(
            {"userId": user_id},
            sort=[("detectedAt", -1)]
        ).limit(limit)
        docs = await cursor.to_list(length=limit)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    # -----------------------------------------------------------------------
    # CROSS-CHANNEL DETECTION
    # -----------------------------------------------------------------------

    async def _get_recent_topics_across_users(self, db,
                                              user_id: str) -> list:
        """Gets recent video titles from all competitors watched by this user."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        cursor = db.competitor_videos.find(
            {"userId": user_id, "detectedAt": {"$gte": cutoff}},
            {"title": 1}
        ).limit(200)
        docs = await cursor.to_list(length=200)
        return [d.get("title", "") for d in docs]

    def _check_cross_channel(self, title: str, existing_titles: list) -> bool:
        """
        Returns True if the same broad topic has been detected on
        other watched channels recently (3+ word overlap with 2+ videos).
        """
        import re as _re
        stop = {"the","a","an","and","or","in","of","to","is","this","that","for"}

        def keywords(t):
            words = _re.findall(r"[a-z]+", t.lower())
            return {w for w in words if w not in stop and len(w) > 3}

        t_kws  = keywords(title)
        matches = 0
        for et in existing_titles:
            if len(t_kws & keywords(et)) >= 3:
                matches += 1
        return matches >= 2

    # -----------------------------------------------------------------------
    # CHANNEL METADATA
    # -----------------------------------------------------------------------

    async def _fetch_channel_metadata(self, channel_id: str) -> Optional[dict]:
        """Fetches basic metadata for a YouTube channel to verify it exists."""
        yt = self._get_yt()
        if not yt:
            return {"title": channel_id, "subscriberCount": 0, "videoCount": 0}
        try:
            resp = await asyncio.to_thread(
                lambda: yt.channels().list(
                    id=channel_id, part="snippet,statistics"
                ).execute()
            )
            items = resp.get("items", [])
            if not items:
                return None
            item  = items[0]
            stats = item.get("statistics", {})
            return {
                "title":           item["snippet"]["title"],
                "description":     item["snippet"].get("description", "")[:200],
                "thumbnailUrl":    item["snippet"]["thumbnails"].get("default", {}).get("url", ""),
                "subscriberCount": int(stats.get("subscriberCount", 0)),
                "videoCount":      int(stats.get("videoCount", 0)),
            }
        except Exception as e:
            log.warning(f"Channel metadata fetch failed for {channel_id}: {e}")
            return None

    # -----------------------------------------------------------------------
    # TTL INDEXES
    # -----------------------------------------------------------------------

    async def ensure_indexes(self):
        """Creates MongoDB indexes. Safe to call multiple times."""
        db = await self._get_db()
        await db.competitor_channels.create_index(
            [("userId", 1), ("channelId", 1)],
            unique=True, name="idx_comp_user_channel"
        )
        await db.competitor_videos.create_index(
            "videoId", unique=True, name="idx_comp_video_id"
        )
        await db.competitor_videos.create_index(
            "expiresAt", expireAfterSeconds=0, name="idx_comp_video_ttl"
        )
        await db.competitor_videos.create_index(
            [("userId", 1), ("detectedAt", -1)],
            name="idx_comp_video_user_date"
        )
        await db.competitor_alerts.create_index(
            "expiresAt", expireAfterSeconds=0, name="idx_alert_ttl"
        )
        await db.competitor_alerts.create_index(
            [("userId", 1), ("read", 1), ("firedAt", -1)],
            name="idx_alert_user_read"
        )
        log.info("Competitor watcher indexes confirmed")


# ===========================================================================
# CLI
# ===========================================================================
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Competitor Watcher Agent CLI")
    parser.add_argument("--run-all",      action="store_true", help="Run check for all users")
    parser.add_argument("--add",          nargs=3,
                        metavar=("USER_ID","CHANNEL_ID","CHANNEL_NAME"),
                        help="Add a competitor channel to watch")
    parser.add_argument("--alerts",       type=str, metavar="USER_ID",
                        help="Get alerts for a user")
    parser.add_argument("--unread-only",  action="store_true",
                        help="Show unread alerts only")
    parser.add_argument("--competitors",  type=str, metavar="USER_ID",
                        help="List watched competitors for a user")
    parser.add_argument("--indexes",      action="store_true",
                        help="Create MongoDB indexes and exit")
    args = parser.parse_args()

    agent = CompetitorWatcherAgent()

    if args.indexes:
        asyncio.run(agent.ensure_indexes())
    elif args.run_all:
        result = asyncio.run(agent.run_all())
        print(json.dumps(result, indent=2, default=str))
    elif args.add:
        result = asyncio.run(agent.add_competitor(args.add[0], args.add[1], args.add[2]))
        print(json.dumps(result, indent=2, default=str))
    elif args.alerts:
        result = asyncio.run(agent.get_alerts(args.alerts, unread_only=args.unread_only))
        print(json.dumps(result, indent=2, default=str))
    elif args.competitors:
        result = asyncio.run(agent.get_competitors(args.competitors))
        print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()
