"""
Viralityy — M9: Video Preview Mode
------------------------------------
Holds generated videos in a 24-hour review queue before they are posted
to YouTube/TikTok/Instagram. Users can approve, skip, or request edits.

Lifecycle:
  generated → queued_for_preview → (approved|skipped|edit_requested)
           → (posted|dropped|regenerated)

Auto-post: if user hasn't acted within 24h AND they have auto-approve ON,
the video posts automatically. If auto-approve is OFF, it's skipped.

MongoDB collection: preview_queue
  Each doc represents one pending video with its script, metadata,
  thumbnail URL, and status.

Called by pipeline.py INSTEAD of posting directly:
  from preview_engine import PreviewEngine
  pe = PreviewEngine()
  await pe.queue_video(user_id, video_data)
"""

import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

logging.basicConfig(level=logging.INFO, format='[Preview] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# How long videos sit in the preview queue before auto-action
PREVIEW_WINDOW_HOURS = 24

# Valid user actions
VALID_ACTIONS = ('approve', 'skip', 'edit')


class PreviewEngine:

    def __init__(self, mongo_uri: str = None):
        self.mongo_uri = mongo_uri or os.environ.get('MONGODB_URI', 'mongodb://localhost:27017')
        self.db = None

    async def _get_db(self):
        if self.db:
            return self.db
        if not HAS_MOTOR:
            raise RuntimeError("pip install motor")
        client = AsyncIOMotorClient(self.mongo_uri)
        self.db = client['viralityy']
        return self.db

    # ==================================================================
    # QUEUE A VIDEO FOR PREVIEW
    # ==================================================================

    async def queue_video(self, user_id: str, video_data: dict) -> dict:
        """
        Called by pipeline.py instead of posting directly.
        Stores the video in preview_queue with status 'pending'.

        video_data should contain:
          title, script, thumbnail_url, video_url (or video_path),
          platform ('youtube'|'tiktok'|'instagram'), niche, video_type,
          scheduled_for (ISO string — when it was meant to post),
          pipeline_run_id (for tracing)
        """
        db = await self._get_db()
        user = await db.users.find_one({'_id': user_id})

        expires_at = (
            datetime.now(timezone.utc) + timedelta(hours=PREVIEW_WINDOW_HOURS)
        ).isoformat()

        # Determine auto-approve setting (default: True for smooth UX)
        auto_approve = True
        if user:
            auto_approve = user.get('previewSettings', {}).get('autoApprove', True)

        preview_doc = {
            'userId':         user_id,
            'status':         'pending',         # pending → approved|skipped|edit_requested
            'autoApprove':    auto_approve,

            # Video content
            'title':          video_data.get('title', ''),
            'script':         video_data.get('script', ''),
            'thumbnailUrl':   video_data.get('thumbnail_url', ''),
            'videoUrl':       video_data.get('video_url', ''),
            'videoPath':      video_data.get('video_path', ''),
            'platform':       video_data.get('platform', 'youtube'),
            'niche':          video_data.get('niche', ''),
            'videoType':      video_data.get('video_type', 'short'),
            'pipelineRunId':  video_data.get('pipeline_run_id', ''),

            # Scheduling
            'scheduledFor':   video_data.get('scheduled_for', ''),
            'queuedAt':       datetime.now(timezone.utc).isoformat(),
            'expiresAt':      expires_at,

            # User action tracking
            'userAction':     None,    # approve | skip | edit
            'userActionAt':   None,
            'editRequest':    None,    # user's edit instructions (if action=edit)
            'postedAt':       None,
            'youtubeVideoId': None,
        }

        result = await db.preview_queue.insert_one(preview_doc)
        preview_doc['_id'] = str(result.inserted_id)

        log.info(f"Queued video for user {user_id}: '{preview_doc['title'][:50]}' expires {expires_at}")
        return preview_doc

    # ==================================================================
    # USER ACTIONS
    # ==================================================================

    async def approve(self, preview_id: str, user_id: str) -> dict:
        """
        User approves the video — marks it ready to post.
        The posting scheduler picks this up and sends it to YouTube/TikTok.
        """
        return await self._set_action(preview_id, user_id, 'approve')

    async def skip(self, preview_id: str, user_id: str) -> dict:
        """User skips — video is dropped, slot is freed."""
        return await self._set_action(preview_id, user_id, 'skip')

    async def request_edit(self, preview_id: str, user_id: str, instructions: str) -> dict:
        """
        User requests an edit — pipeline will regenerate with the instructions.
        The video is re-queued as a new preview after regeneration.
        """
        db = await self._get_db()
        from bson import ObjectId
        doc = await db.preview_queue.find_one({
            '_id': ObjectId(preview_id), 'userId': user_id, 'status': 'pending'
        })
        if not doc:
            return {'error': 'Preview not found or already actioned'}

        await db.preview_queue.update_one(
            {'_id': ObjectId(preview_id)},
            {'$set': {
                'status':        'edit_requested',
                'userAction':    'edit',
                'userActionAt':  datetime.now(timezone.utc).isoformat(),
                'editRequest':   instructions[:1000],
            }}
        )

        # Log edit request for pipeline to pick up
        await db.edit_requests.insert_one({
            'previewId':    preview_id,
            'userId':       user_id,
            'instructions': instructions[:1000],
            'requestedAt':  datetime.now(timezone.utc).isoformat(),
            'status':       'pending',  # pipeline sets to 'processing' then 'done'
        })

        log.info(f"Edit requested for preview {preview_id}: {instructions[:80]}")
        return {'success': True, 'message': 'Edit request received. Video will be regenerated.'}

    async def _set_action(self, preview_id: str, user_id: str, action: str) -> dict:
        db = await self._get_db()
        try:
            from bson import ObjectId
            oid = ObjectId(preview_id)
        except Exception:
            return {'error': 'Invalid preview ID'}

        doc = await db.preview_queue.find_one({
            '_id': oid, 'userId': user_id, 'status': 'pending'
        })
        if not doc:
            return {'error': 'Preview not found or already actioned'}

        new_status = 'approved' if action == 'approve' else 'skipped'
        await db.preview_queue.update_one(
            {'_id': oid},
            {'$set': {
                'status':       new_status,
                'userAction':   action,
                'userActionAt': datetime.now(timezone.utc).isoformat(),
            }}
        )

        log.info(f"Preview {preview_id} {new_status} by user {user_id}")
        return {'success': True, 'status': new_status, 'previewId': preview_id}

    # ==================================================================
    # AUTO-ACTION ON EXPIRY (run by scheduler)
    # ==================================================================

    async def process_expired(self) -> dict:
        """
        Run every hour by scheduler.
        For expired previews:
          - autoApprove=True  → mark approved → will be posted
          - autoApprove=False → mark skipped → dropped
        """
        db = await self._get_db()
        now = datetime.now(timezone.utc).isoformat()

        expired = await db.preview_queue.find({
            'status':    'pending',
            'expiresAt': {'$lte': now},
        }).to_list(length=1000)

        approved = 0
        skipped  = 0

        for doc in expired:
            new_status = 'approved' if doc.get('autoApprove', True) else 'skipped'
            await db.preview_queue.update_one(
                {'_id': doc['_id']},
                {'$set': {
                    'status':       new_status,
                    'userAction':   'auto',
                    'userActionAt': now,
                }}
            )
            if new_status == 'approved':
                approved += 1
            else:
                skipped += 1

        result = {'expired': len(expired), 'auto_approved': approved, 'auto_skipped': skipped}
        if expired:
            log.info(f"Processed expired previews: {result}")
        return result

    # ==================================================================
    # DASHBOARD QUERIES
    # ==================================================================

    async def get_pending(self, user_id: str) -> list:
        """Returns all pending previews for a user, soonest expiry first."""
        db = await self._get_db()
        docs = await db.preview_queue.find(
            {'userId': user_id, 'status': 'pending'},
            sort=[('expiresAt', 1)]
        ).to_list(length=50)
        return [self._format(d) for d in docs]

    async def get_history(self, user_id: str, limit: int = 30) -> list:
        """Returns actioned previews — approved, skipped, posted."""
        db = await self._get_db()
        docs = await db.preview_queue.find(
            {'userId': user_id, 'status': {'$ne': 'pending'}},
            sort=[('userActionAt', -1)]
        ).to_list(length=limit)
        return [self._format(d) for d in docs]

    async def get_stats(self, user_id: str) -> dict:
        """Summary counts for the preview dashboard header."""
        db = await self._get_db()
        pipeline = [
            {'$match': {'userId': user_id}},
            {'$group': {'_id': '$status', 'count': {'$sum': 1}}}
        ]
        docs = await db.preview_queue.aggregate(pipeline).to_list(length=20)
        counts = {d['_id']: d['count'] for d in docs}
        return {
            'pending':       counts.get('pending', 0),
            'approved':      counts.get('approved', 0),
            'skipped':       counts.get('skipped', 0),
            'edit_requested':counts.get('edit_requested', 0),
            'posted':        counts.get('posted', 0),
        }

    # ==================================================================
    # MARK AS POSTED (called by posting scheduler after upload)
    # ==================================================================

    async def mark_posted(self, preview_id: str, youtube_video_id: str = '') -> bool:
        db = await self._get_db()
        try:
            from bson import ObjectId
            result = await db.preview_queue.update_one(
                {'_id': ObjectId(preview_id), 'status': 'approved'},
                {'$set': {
                    'status':         'posted',
                    'postedAt':       datetime.now(timezone.utc).isoformat(),
                    'youtubeVideoId': youtube_video_id,
                }}
            )
            return result.modified_count > 0
        except Exception as e:
            log.error(f"mark_posted error: {e}")
            return False

    # ==================================================================
    # USER SETTINGS
    # ==================================================================

    async def update_settings(self, user_id: str, auto_approve: bool,
                              notify_email: bool = True) -> bool:
        db = await self._get_db()
        await db.users.update_one(
            {'_id': user_id},
            {'$set': {'previewSettings': {
                'autoApprove':   auto_approve,
                'notifyEmail':   notify_email,
                'updatedAt':     datetime.now(timezone.utc).isoformat(),
            }}}
        )
        return True

    # ==================================================================
    # HELPERS
    # ==================================================================

    def _format(self, doc: dict) -> dict:
        """Cleans a MongoDB doc for API response."""
        doc['_id'] = str(doc['_id'])

        # Calculate time remaining
        expires = doc.get('expiresAt', '')
        if expires and doc['status'] == 'pending':
            try:
                exp_dt  = datetime.fromisoformat(expires.replace('Z', '+00:00'))
                remaining = exp_dt - datetime.now(timezone.utc)
                hours_left = max(0, remaining.total_seconds() / 3600)
                doc['hoursRemaining'] = round(hours_left, 1)
                doc['urgent'] = hours_left < 4
            except Exception:
                doc['hoursRemaining'] = None
                doc['urgent'] = False
        return doc


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse, json

    parser = argparse.ArgumentParser()
    parser.add_argument('--process-expired', action='store_true')
    parser.add_argument('--pending',  type=str, help='User ID')
    parser.add_argument('--stats',    type=str, help='User ID')
    args = parser.parse_args()

    engine = PreviewEngine()

    if args.process_expired:
        result = asyncio.run(engine.process_expired())
        print(json.dumps(result))
    elif args.pending:
        docs = asyncio.run(engine.get_pending(args.pending))
        print(json.dumps(docs, default=str))
    elif args.stats:
        stats = asyncio.run(engine.get_stats(args.stats))
        print(json.dumps(stats))
    else:
        parser.print_help()
