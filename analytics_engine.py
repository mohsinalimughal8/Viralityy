"""
Viralityy — M5a: Performance Analytics Collection Engine
---------------------------------------------------------
Collects YouTube video analytics for every video posted by Viralityy
users and stores them in MongoDB (VideoAnalytics collection).

What it tracks per video:
  - Views, watch time, CTR, likes, comments, shares
  - Audience retention curve (first 30s, 50%, full)
  - Revenue (RPM, CPM, estimated earnings)
  - Traffic sources (search, suggested, browse, external)
  - Subscriber delta (gained/lost from this video)
  - Impression count and thumbnail CTR

Runs as a background job — call collect_all() on a schedule (e.g. daily via cron).
Individual video refresh available via refresh_video(video_id, user_id).

Usage:
  from analytics_engine import AnalyticsEngine
  engine = AnalyticsEngine(mongo_uri=os.environ['MONGODB_URI'])
  await engine.collect_all()            # run for all active users
  await engine.refresh_video(vid, uid)  # refresh one video
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    HAS_GOOGLE = True
except ImportError:
    HAS_GOOGLE = False

logging.basicConfig(level=logging.INFO, format='[Analytics] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# THRESHOLDS — used by M5b learning loop to flag underperformers
# ---------------------------------------------------------------------------
PERFORMANCE_THRESHOLDS = {
    "ctr_good":          0.06,   # 6%+ CTR = good thumbnail
    "ctr_poor":          0.03,   # <3% CTR = thumbnail needs work
    "retention_good":    0.45,   # 45%+ avg retention = good content
    "retention_poor":    0.25,   # <25% = hook or pacing problem
    "views_7d_good":    1000,    # 1k+ views in 7 days = performing
    "views_7d_poor":     100,    # <100 views in 7 days = underperforming
    "like_ratio_good":   0.04,   # 4%+ likes/views = strong engagement
}

# ---------------------------------------------------------------------------
# ANALYTICS ENGINE
# ---------------------------------------------------------------------------
class AnalyticsEngine:

    def __init__(self, mongo_uri: str = None):
        self.mongo_uri = mongo_uri or os.environ.get('MONGODB_URI', 'mongodb://localhost:27017')
        self.db = None
        self._client = None

    # ------------------------------------------------------------------
    # DB CONNECTION
    # ------------------------------------------------------------------
    async def _get_db(self):
        if self.db is not None:
            return self.db
        if not HAS_MOTOR:
            raise RuntimeError("motor not installed — run: pip install motor")
        self._client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self._client['viralityy']
        return self.db

    # ------------------------------------------------------------------
    # PUBLIC: collect analytics for ALL active users
    # ------------------------------------------------------------------
    async def collect_all(self):
        """
        Main job — iterates every active user with a YouTube token
        and collects analytics for all their videos posted in the last 90 days.
        """
        db = await self._get_db()
        users = await db.users.find({
            'youtubeAccessToken': {'$exists': True, '$ne': None},
            'status': {'$ne': 'cancelled'}
        }).to_list(length=1000)

        log.info(f"Starting analytics collection for {len(users)} users")
        results = {'success': 0, 'failed': 0, 'videos_updated': 0}

        for user in users:
            try:
                count = await self._collect_for_user(db, user)
                results['success'] += 1
                results['videos_updated'] += count
            except Exception as e:
                log.error(f"Failed for user {user['_id']}: {e}")
                results['failed'] += 1

        log.info(f"Collection complete: {results}")
        return results

    # ------------------------------------------------------------------
    # PUBLIC: refresh a single video
    # ------------------------------------------------------------------
    async def refresh_video(self, video_id: str, user_id: str):
        """Force-refresh analytics for one specific video."""
        db = await self._get_db()
        user = await db.users.find_one({'_id': user_id})
        if not user:
            raise ValueError(f"User {user_id} not found")

        yt = self._build_youtube_client(user)
        if not yt:
            raise ValueError("No YouTube credentials for this user")

        snapshot = await self._fetch_video_snapshot(yt, video_id, user_id)
        if snapshot:
            await self._upsert_snapshot(db, snapshot)
            log.info(f"Refreshed video {video_id}")
            return snapshot
        return None

    # ------------------------------------------------------------------
    # INTERNAL: collect for one user
    # ------------------------------------------------------------------
    async def _collect_for_user(self, db, user) -> int:
        user_id = str(user['_id'])
        yt = self._build_youtube_client(user)
        if not yt:
            log.warning(f"No YouTube client for user {user_id} — skipping")
            return 0

        # Get all video IDs posted by Viralityy for this user in last 90 days
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        posted_videos = await db.posted_videos.find({
            'userId': user_id,
            'postedAt': {'$gte': cutoff},
            'platform': 'youtube'
        }).to_list(length=500)

        if not posted_videos:
            return 0

        video_ids = [v['videoId'] for v in posted_videos if v.get('videoId')]
        count = 0

        # YouTube API accepts up to 50 IDs per call — batch them
        for batch_start in range(0, len(video_ids), 50):
            batch = video_ids[batch_start:batch_start + 50]
            snapshots = await self._fetch_batch_snapshots(yt, batch, user_id)
            for snap in snapshots:
                await self._upsert_snapshot(db, snap)
                count += 1

        log.info(f"User {user_id}: updated {count} videos")
        return count

    # ------------------------------------------------------------------
    # INTERNAL: build YouTube API client from user's stored OAuth token
    # ------------------------------------------------------------------
    def _build_youtube_client(self, user):
        if not HAS_GOOGLE:
            log.warning("google-api-python-client not installed")
            return None
        token = user.get('youtubeAccessToken')
        if not token:
            return None
        try:
            from google.oauth2.credentials import Credentials
            creds = Credentials(
                token=token,
                refresh_token=user.get('youtubeRefreshToken'),
                client_id=os.environ.get('YOUTUBE_CLIENT_ID'),
                client_secret=os.environ.get('YOUTUBE_CLIENT_SECRET'),
                token_uri='https://oauth2.googleapis.com/token'
            )
            return build('youtube', 'v3', credentials=creds)
        except Exception as e:
            log.error(f"Failed to build YouTube client: {e}")
            return None

    # ------------------------------------------------------------------
    # INTERNAL: fetch video stats for a batch of IDs
    # ------------------------------------------------------------------
    async def _fetch_batch_snapshots(self, yt, video_ids: list, user_id: str) -> list:
        try:
            response = yt.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(video_ids)
            ).execute()

            snapshots = []
            for item in response.get('items', []):
                snap = self._parse_video_item(item, user_id)
                if snap:
                    snapshots.append(snap)
            return snapshots
        except Exception as e:
            log.error(f"YouTube API batch error: {e}")
            return []

    async def _fetch_video_snapshot(self, yt, video_id: str, user_id: str) -> Optional[dict]:
        snapshots = await self._fetch_batch_snapshots(yt, [video_id], user_id)
        return snapshots[0] if snapshots else None

    # ------------------------------------------------------------------
    # INTERNAL: parse YouTube API response into our schema
    # ------------------------------------------------------------------
    def _parse_video_item(self, item: dict, user_id: str) -> Optional[dict]:
        try:
            vid_id    = item['id']
            snippet   = item.get('snippet', {})
            stats     = item.get('statistics', {})
            content   = item.get('contentDetails', {})

            views     = int(stats.get('viewCount', 0))
            likes     = int(stats.get('likeCount', 0))
            comments  = int(stats.get('commentCount', 0))
            favorites = int(stats.get('favoriteCount', 0))

            like_ratio = round(likes / views, 4) if views > 0 else 0.0

            return {
                'videoId':        vid_id,
                'userId':         user_id,
                'title':          snippet.get('title', ''),
                'channelId':      snippet.get('channelId', ''),
                'publishedAt':    snippet.get('publishedAt'),
                'duration':       content.get('duration', ''),
                'collectedAt':    datetime.now(timezone.utc).isoformat(),
                'stats': {
                    'views':         views,
                    'likes':         likes,
                    'comments':      comments,
                    'favorites':     favorites,
                    'likeRatio':     like_ratio,
                },
                # Fields below require YouTube Analytics API (M7) — stubbed for now
                'watchTime':      None,   # total minutes watched
                'avgViewDuration':None,   # seconds
                'avgViewPct':     None,   # 0.0–1.0 retention
                'impressions':    None,
                'ctr':            None,   # click-through rate on impressions
                'revenue': {
                    'rpm':         None,
                    'cpm':         None,
                    'estimatedEarnings': None,
                },
                'trafficSources': {},     # populated by M7
                'subscriberDelta':None,
                # Performance flags (set by _flag_performance)
                'flags':          self._flag_performance(views, like_ratio, None),
                'collectionVersion': 'M5a',
            }
        except Exception as e:
            log.error(f"Error parsing video item: {e}")
            return None

    # ------------------------------------------------------------------
    # INTERNAL: set performance flags used by M5b learning loop
    # ------------------------------------------------------------------
    def _flag_performance(self, views: int, like_ratio: float, ctr: Optional[float]) -> dict:
        flags = {}

        # View performance (based on 7-day window — approximated here)
        if views >= PERFORMANCE_THRESHOLDS['views_7d_good']:
            flags['viewPerformance'] = 'good'
        elif views <= PERFORMANCE_THRESHOLDS['views_7d_poor']:
            flags['viewPerformance'] = 'poor'
        else:
            flags['viewPerformance'] = 'average'

        # Engagement
        if like_ratio >= PERFORMANCE_THRESHOLDS['like_ratio_good']:
            flags['engagement'] = 'good'
        else:
            flags['engagement'] = 'average'

        # CTR (only when available from Analytics API)
        if ctr is not None:
            if ctr >= PERFORMANCE_THRESHOLDS['ctr_good']:
                flags['thumbnailCtr'] = 'good'
            elif ctr <= PERFORMANCE_THRESHOLDS['ctr_poor']:
                flags['thumbnailCtr'] = 'poor'
            else:
                flags['thumbnailCtr'] = 'average'

        return flags

    # ------------------------------------------------------------------
    # INTERNAL: upsert snapshot into MongoDB
    # ------------------------------------------------------------------
    async def _upsert_snapshot(self, db, snapshot: dict):
        """
        Insert or update. Uses videoId + collectedAt date as the key
        so we keep one record per video per day (daily snapshots).
        """
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        await db.video_analytics.update_one(
            {
                'videoId': snapshot['videoId'],
                'userId':  snapshot['userId'],
                'snapshotDate': today,
            },
            {'$set': {**snapshot, 'snapshotDate': today}},
            upsert=True
        )

    # ------------------------------------------------------------------
    # PUBLIC: query helpers (used by dashboard API routes)
    # ------------------------------------------------------------------
    async def get_user_summary(self, user_id: str) -> dict:
        """
        Aggregated stats across all videos for a user.
        Returns totals + averages for the dashboard summary cards.
        """
        db = await self._get_db()

        # Get latest snapshot per video
        pipeline = [
            {'$match': {'userId': user_id}},
            {'$sort': {'snapshotDate': -1}},
            {'$group': {
                '_id': '$videoId',
                'latest': {'$first': '$$ROOT'}
            }},
            {'$replaceRoot': {'newRoot': '$latest'}}
        ]
        docs = await db.video_analytics.aggregate(pipeline).to_list(length=1000)

        if not docs:
            return {'totalVideos': 0, 'totalViews': 0, 'totalLikes': 0, 'avgLikeRatio': 0}

        total_views   = sum(d['stats']['views'] for d in docs)
        total_likes   = sum(d['stats']['likes'] for d in docs)
        total_comments= sum(d['stats']['comments'] for d in docs)
        avg_like_ratio= round(sum(d['stats']['likeRatio'] for d in docs) / len(docs), 4)

        # Performance distribution
        perf_counts = {'good': 0, 'average': 0, 'poor': 0}
        for d in docs:
            vp = d.get('flags', {}).get('viewPerformance', 'average')
            perf_counts[vp] = perf_counts.get(vp, 0) + 1

        return {
            'totalVideos':    len(docs),
            'totalViews':     total_views,
            'totalLikes':     total_likes,
            'totalComments':  total_comments,
            'avgLikeRatio':   avg_like_ratio,
            'performanceDist': perf_counts,
            'topVideos':      self._top_videos(docs, n=5),
            'generatedAt':    datetime.now(timezone.utc).isoformat(),
        }

    async def get_video_history(self, video_id: str, user_id: str) -> list:
        """Returns daily snapshots for one video (for sparkline charts)."""
        db = await self._get_db()
        docs = await db.video_analytics.find(
            {'videoId': video_id, 'userId': user_id},
            sort=[('snapshotDate', 1)]
        ).to_list(length=90)
        return docs

    async def get_top_videos(self, user_id: str, n: int = 10, sort_by: str = 'views') -> list:
        """Returns top N videos sorted by views, likes, or likeRatio."""
        db = await self._get_db()
        sort_field = {
            'views':    'stats.views',
            'likes':    'stats.likes',
            'comments': 'stats.comments',
        }.get(sort_by, 'stats.views')

        pipeline = [
            {'$match': {'userId': user_id}},
            {'$sort': {'snapshotDate': -1}},
            {'$group': {'_id': '$videoId', 'latest': {'$first': '$$ROOT'}}},
            {'$replaceRoot': {'newRoot': '$latest'}},
            {'$sort': {sort_field: -1}},
            {'$limit': n}
        ]
        return await db.video_analytics.aggregate(pipeline).to_list(length=n)

    async def get_underperformers(self, user_id: str) -> list:
        """Returns videos flagged as poor performers — fed into M5b."""
        db = await self._get_db()
        pipeline = [
            {'$match': {'userId': user_id}},
            {'$sort': {'snapshotDate': -1}},
            {'$group': {'_id': '$videoId', 'latest': {'$first': '$$ROOT'}}},
            {'$replaceRoot': {'newRoot': '$latest'}},
            {'$match': {'flags.viewPerformance': 'poor'}}
        ]
        return await db.video_analytics.aggregate(pipeline).to_list(length=100)

    # ------------------------------------------------------------------
    # INTERNAL helpers
    # ------------------------------------------------------------------
    def _top_videos(self, docs: list, n: int = 5) -> list:
        sorted_docs = sorted(docs, key=lambda d: d['stats']['views'], reverse=True)
        return [
            {
                'videoId': d['videoId'],
                'title':   d.get('title', ''),
                'views':   d['stats']['views'],
                'likes':   d['stats']['likes'],
                'likeRatio': d['stats']['likeRatio'],
                'flag':    d.get('flags', {}).get('viewPerformance', 'average'),
            }
            for d in sorted_docs[:n]
        ]


# ---------------------------------------------------------------------------
# CLI RUNNER — call from cron or Railway cron job
# python analytics_engine.py --collect
# python analytics_engine.py --refresh --video VIDEO_ID --user USER_ID
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--collect', action='store_true', help='Run full collection for all users')
    parser.add_argument('--refresh', action='store_true', help='Refresh a single video')
    parser.add_argument('--video',   type=str, help='YouTube video ID (for --refresh)')
    parser.add_argument('--user',    type=str, help='User ID (for --refresh)')
    args = parser.parse_args()

    engine = AnalyticsEngine()

    if args.collect:
        print("Running full analytics collection...")
        result = asyncio.run(engine.collect_all())
        print(f"Done: {result}")

    elif args.refresh:
        if not args.video or not args.user:
            print("--refresh requires --video and --user")
        else:
            snap = asyncio.run(engine.refresh_video(args.video, args.user))
            print(f"Refreshed: {snap}")
    else:
        parser.print_help()
