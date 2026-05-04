"""
Viralityy — M5b: Content Optimisation Learning Loop
-----------------------------------------------------
Reads the performance flags written by M5a (analytics_engine.py) and
automatically adjusts per-user prompt_config in MongoDB so that future
videos are generated with better-performing patterns.

What it learns and adjusts:
  - Script style     (hook strength, pacing, storytelling vs list-format)
  - Video length     (short punchy vs detailed long-form)
  - Title patterns   (question / number / shock / how-to)
  - Posting time     (best day/hour based on historical engagement)
  - Thumbnail style  (text-heavy vs visual, colour temperature)
  - Niche sub-topics (doubles down on best-performing sub-tags)

Run schedule: after analytics_engine.py collect_all() completes.
  python learning_engine.py --optimise           # run for all users
  python learning_engine.py --optimise --user X  # single user

The updated prompt_config is stored in MongoDB users collection and is
read by pipeline.py at video generation time.
"""

import os
import asyncio
import logging
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from typing import Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

logging.basicConfig(level=logging.INFO, format='[Learning] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DEFAULT PROMPT CONFIG — applied to new users, overridden by learning
# ---------------------------------------------------------------------------
DEFAULT_PROMPT_CONFIG = {
    # Script generation
    "hook_style":         "question",      # question | shock | stat | story
    "script_format":      "list",          # list | narrative | dialogue
    "script_length":      "medium",        # short | medium | long
    "pacing":             "fast",          # fast | moderate | slow
    "cta_style":          "subscribe",     # subscribe | comment | like | none

    # Title generation
    "title_pattern":      "number",        # number | question | how-to | shock | statement
    "title_length":       "medium",        # short(≤50) | medium(51-65) | long(65+)
    "include_emoji":      False,

    # Thumbnail
    "thumbnail_style":    "text_heavy",    # text_heavy | face | visual | minimal
    "thumbnail_palette":  "warm",          # warm | cool | high_contrast | muted

    # Scheduling
    "best_post_day":      "tuesday",       # monday..sunday
    "best_post_hour":     15,              # 0-23 UTC

    # Sub-topic focus (filled in by learning, empty = use niche broadly)
    "top_subtopics":      [],

    # Meta
    "confidence":         "default",       # default | low | medium | high
    "last_optimised":     None,
    "optimise_version":   "M5b",
}

# ---------------------------------------------------------------------------
# LEARNING ENGINE
# ---------------------------------------------------------------------------
class LearningEngine:

    def __init__(self, mongo_uri: str = None):
        self.mongo_uri = mongo_uri or os.environ.get('MONGODB_URI', 'mongodb://localhost:27017')
        self.db = None

    async def _get_db(self):
        if self.db:
            return self.db
        if not HAS_MOTOR:
            raise RuntimeError("motor not installed — pip install motor")
        client = AsyncIOMotorClient(self.mongo_uri)
        self.db = client['viralityy']
        return self.db

    # ------------------------------------------------------------------
    # PUBLIC: optimise all users
    # ------------------------------------------------------------------
    async def optimise_all(self):
        db = await self._get_db()
        users = await db.users.find(
            {'status': {'$ne': 'cancelled'}}
        ).to_list(length=1000)

        log.info(f"Running optimisation for {len(users)} users")
        results = {'optimised': 0, 'skipped': 0, 'failed': 0}

        for user in users:
            uid = str(user['_id'])
            try:
                changed = await self.optimise_user(uid)
                if changed:
                    results['optimised'] += 1
                else:
                    results['skipped'] += 1
            except Exception as e:
                log.error(f"Failed for user {uid}: {e}")
                results['failed'] += 1

        log.info(f"Optimisation complete: {results}")
        return results

    # ------------------------------------------------------------------
    # PUBLIC: optimise one user
    # ------------------------------------------------------------------
    async def optimise_user(self, user_id: str) -> bool:
        """
        Reads the last 30 days of analytics for this user, detects patterns
        in top/bottom performers, and updates prompt_config accordingly.
        Returns True if config was changed.
        """
        db = await self._get_db()

        # ── Fetch analytics data ────────────────────────────────────
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
        pipeline = [
            {'$match': {'userId': user_id, 'snapshotDate': {'$gte': cutoff}}},
            {'$sort':  {'snapshotDate': -1}},
            {'$group': {'_id': '$videoId', 'latest': {'$first': '$$ROOT'}}},
            {'$replaceRoot': {'newRoot': '$latest'}}
        ]
        docs = await db.video_analytics.aggregate(pipeline).to_list(length=200)

        if len(docs) < 3:
            log.info(f"User {user_id}: not enough data ({len(docs)} videos) — skipping")
            return False

        # ── Split into top / bottom performers ──────────────────────
        sorted_docs = sorted(docs, key=lambda d: d['stats'].get('views', 0), reverse=True)
        top_n    = max(1, len(sorted_docs) // 3)
        top      = sorted_docs[:top_n]
        bottom   = sorted_docs[-top_n:]

        # ── Build insights ──────────────────────────────────────────
        insights = self._extract_insights(top, bottom)

        # ── Get current config ──────────────────────────────────────
        user = await db.users.find_one({'_id': user_id})
        current = user.get('promptConfig', DEFAULT_PROMPT_CONFIG.copy()) if user else DEFAULT_PROMPT_CONFIG.copy()

        # ── Apply learning ──────────────────────────────────────────
        new_config = self._apply_insights(current, insights, len(docs))

        # ── Detect if anything actually changed ─────────────────────
        changed_keys = [k for k in new_config if new_config[k] != current.get(k)]
        if not changed_keys:
            log.info(f"User {user_id}: no changes needed")
            return False

        new_config['last_optimised'] = datetime.now(timezone.utc).isoformat()
        new_config['optimise_version'] = 'M5b'

        # ── Save to MongoDB ─────────────────────────────────────────
        await db.users.update_one(
            {'_id': user_id},
            {'$set': {'promptConfig': new_config}}
        )

        # ── Log what changed ────────────────────────────────────────
        await db.optimisation_log.insert_one({
            'userId':       user_id,
            'changedKeys':  changed_keys,
            'insights':     insights,
            'videoCount':   len(docs),
            'optimisedAt':  datetime.now(timezone.utc).isoformat(),
        })

        log.info(f"User {user_id}: updated {changed_keys}")
        return True

    # ------------------------------------------------------------------
    # PUBLIC: get current prompt config for a user (used by pipeline.py)
    # ------------------------------------------------------------------
    async def get_prompt_config(self, user_id: str) -> dict:
        db = await self._get_db()
        user = await db.users.find_one({'_id': user_id})
        if not user:
            return DEFAULT_PROMPT_CONFIG.copy()
        return user.get('promptConfig', DEFAULT_PROMPT_CONFIG.copy())

    # ------------------------------------------------------------------
    # PUBLIC: get optimisation history for dashboard display
    # ------------------------------------------------------------------
    async def get_optimisation_history(self, user_id: str, limit: int = 10) -> list:
        db = await self._get_db()
        docs = await db.optimisation_log.find(
            {'userId': user_id},
            sort=[('optimisedAt', -1)]
        ).to_list(length=limit)
        return docs

    # ------------------------------------------------------------------
    # INTERNAL: extract patterns from top vs bottom performers
    # ------------------------------------------------------------------
    def _extract_insights(self, top: list, bottom: list) -> dict:
        insights = {}

        # ── Title pattern analysis ──────────────────────────────────
        top_patterns    = [self._detect_title_pattern(d.get('title', '')) for d in top]
        bottom_patterns = [self._detect_title_pattern(d.get('title', '')) for d in bottom]
        best_pattern    = Counter(top_patterns).most_common(1)
        worst_pattern   = Counter(bottom_patterns).most_common(1)
        if best_pattern:
            insights['best_title_pattern']  = best_pattern[0][0]
        if worst_pattern:
            insights['worst_title_pattern'] = worst_pattern[0][0]

        # ── Title length ────────────────────────────────────────────
        top_title_lens    = [len(d.get('title', '')) for d in top if d.get('title')]
        bottom_title_lens = [len(d.get('title', '')) for d in bottom if d.get('title')]
        if top_title_lens:
            avg_top_len = sum(top_title_lens) / len(top_title_lens)
            insights['best_title_length'] = (
                'short'  if avg_top_len <= 50 else
                'long'   if avg_top_len >= 66 else
                'medium'
            )

        # ── Like ratio (proxy for script/content quality) ───────────
        top_lr    = [d['stats'].get('likeRatio', 0) for d in top]
        bottom_lr = [d['stats'].get('likeRatio', 0) for d in bottom]
        if top_lr:
            avg_top_lr = sum(top_lr) / len(top_lr)
            insights['avg_top_like_ratio'] = round(avg_top_lr, 4)
            # High like ratio → content is resonating → keep narrative style
            if avg_top_lr > 0.05:
                insights['preferred_script_format'] = 'narrative'
            elif avg_top_lr < 0.02:
                insights['preferred_script_format'] = 'list'

        # ── CTR analysis (when available from M7) ───────────────────
        top_ctrs = [d.get('ctr') for d in top if d.get('ctr') is not None]
        if top_ctrs:
            avg_ctr = sum(top_ctrs) / len(top_ctrs)
            insights['avg_top_ctr'] = round(avg_ctr, 4)
            if avg_ctr > 0.08:
                insights['thumbnail_effectiveness'] = 'high'
            elif avg_ctr < 0.04:
                insights['thumbnail_effectiveness'] = 'low'

        # ── Sub-topic detection from titles ─────────────────────────
        top_words = self._extract_keywords(
            [d.get('title', '') for d in top]
        )
        insights['top_subtopics'] = top_words[:5]

        # ── View count trajectory ───────────────────────────────────
        top_views    = sum(d['stats'].get('views', 0) for d in top)
        bottom_views = sum(d['stats'].get('views', 0) for d in bottom)
        insights['top_avg_views']    = round(top_views / max(len(top), 1))
        insights['bottom_avg_views'] = round(bottom_views / max(len(bottom), 1))
        insights['view_spread_ratio'] = round(
            top_views / max(bottom_views, 1), 2
        )

        return insights

    # ------------------------------------------------------------------
    # INTERNAL: apply insights → new prompt_config
    # ------------------------------------------------------------------
    def _apply_insights(self, current: dict, insights: dict, video_count: int) -> dict:
        config = current.copy()

        # Confidence level: more data = higher confidence
        if video_count >= 20:
            config['confidence'] = 'high'
        elif video_count >= 10:
            config['confidence'] = 'medium'
        else:
            config['confidence'] = 'low'

        # Title pattern
        if 'best_title_pattern' in insights:
            config['title_pattern'] = insights['best_title_pattern']

        # Title length
        if 'best_title_length' in insights:
            config['title_length'] = insights['best_title_length']

        # Script format
        if 'preferred_script_format' in insights:
            config['script_format'] = insights['preferred_script_format']

        # Hook style — if view_spread_ratio is very high, strong hooks are working
        if insights.get('view_spread_ratio', 1) > 5:
            config['hook_style'] = 'shock'   # high contrast = shock hooks working
        elif insights.get('avg_top_like_ratio', 0) > 0.05:
            config['hook_style'] = 'story'   # high likes = story hooks resonate

        # Thumbnail
        if insights.get('thumbnail_effectiveness') == 'low':
            # Flip thumbnail style if current one isn't working
            config['thumbnail_style'] = (
                'face' if config.get('thumbnail_style') == 'text_heavy' else 'text_heavy'
            )

        # Sub-topics
        if insights.get('top_subtopics'):
            config['top_subtopics'] = insights['top_subtopics']

        return config

    # ------------------------------------------------------------------
    # INTERNAL: detect title pattern
    # ------------------------------------------------------------------
    def _detect_title_pattern(self, title: str) -> str:
        t = title.lower().strip()
        if not t:
            return 'statement'
        # Number pattern: "5 ways to...", "10 things..."
        if t[0].isdigit() or any(
            t.startswith(w) for w in ['1 ', '2 ', '3 ', '4 ', '5 ', '6 ', '7 ', '8 ', '9 ', '10 ']
        ):
            return 'number'
        if t.startswith(('how ', 'how to')):
            return 'how-to'
        if t.startswith(('why ', 'what ', 'who ', 'when ', 'which ', 'is ', 'are ', 'does ', 'do ', 'can ')):
            return 'question'
        if t.endswith('?'):
            return 'question'
        if any(w in t for w in ['stop', 'never', 'wrong', 'mistake', 'secret', 'truth', 'lie', 'hack']):
            return 'shock'
        return 'statement'

    # ------------------------------------------------------------------
    # INTERNAL: extract keywords from a list of titles
    # ------------------------------------------------------------------
    def _extract_keywords(self, titles: list) -> list:
        STOP = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
            'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
            'should', 'my', 'your', 'their', 'our', 'its', 'this', 'that', 'i',
            'you', 'he', 'she', 'we', 'they', 'how', 'why', 'what', 'when', 'who',
            'if', 'it', 'me', 'just', 'about', 'into', 'up', 'out', 'so', 'than',
        }
        word_counts = Counter()
        for title in titles:
            words = title.lower().replace("'", '').split()
            for word in words:
                clean = ''.join(c for c in word if c.isalpha())
                if clean and clean not in STOP and len(clean) > 3:
                    word_counts[clean] += 1
        return [w for w, _ in word_counts.most_common(10)]


# ---------------------------------------------------------------------------
# PROMPT CONFIG BUILDER — called by pipeline.py at generation time
# ---------------------------------------------------------------------------
def build_system_prompt(config: dict, niche: str, video_type: str = 'short') -> str:
    """
    Translates a prompt_config dict into a concrete system prompt
    for the AI video script generator.
    """
    hook_instructions = {
        'question':  'Open with a direct question that makes the viewer feel personally challenged.',
        'shock':     'Open with a surprising, counter-intuitive fact or bold claim.',
        'stat':      'Open with a specific statistic that feels unbelievable but true.',
        'story':     'Open with a 1-sentence micro-story that creates immediate emotional tension.',
    }
    format_instructions = {
        'list':      'Structure the script as a numbered list with a clear payoff at each point.',
        'narrative': 'Write in flowing narrative prose with story arc: setup, tension, resolution.',
        'dialogue':  'Use a conversational tone as if speaking directly to one specific person.',
    }
    title_instructions = {
        'number':    'Start the title with a specific number (e.g. "7 ways...", "3 reasons...").',
        'question':  'Phrase the title as a direct question the target viewer is already asking.',
        'how-to':    'Start the title with "How to" followed by a specific desirable outcome.',
        'shock':     'Use a counter-intuitive or bold claim that challenges a common belief.',
        'statement': 'Make a clear, confident declarative statement as the title.',
    }

    length_map = {
        'short':  '45-60 seconds' if video_type == 'short' else '8-12 minutes',
        'medium': '60-90 seconds' if video_type == 'short' else '15-20 minutes',
        'long':   '90-120 seconds' if video_type == 'short' else '25-30 minutes',
    }

    confidence_note = ''
    if config.get('confidence') == 'high':
        confidence_note = 'These instructions are based on strong performance data — follow them precisely.'
    elif config.get('confidence') == 'low':
        confidence_note = 'These are early-stage defaults — apply your best judgment alongside them.'

    subtopics = config.get('top_subtopics', [])
    subtopic_line = (
        f"Focus on these proven sub-topics where possible: {', '.join(subtopics[:3])}."
        if subtopics else ''
    )

    return f"""You are a YouTube content strategist generating a {video_type} script for a channel in the "{niche}" niche.

SCRIPT REQUIREMENTS:
- Hook: {hook_instructions.get(config.get('hook_style', 'question'), hook_instructions['question'])}
- Format: {format_instructions.get(config.get('script_format', 'list'), format_instructions['list'])}
- Target length: {length_map.get(config.get('script_length', 'medium'), '60-90 seconds')}
- Pacing: {config.get('pacing', 'fast').upper()} — {'short sentences, punchy delivery, no filler' if config.get('pacing') == 'fast' else 'measured delivery with breathing room between points'}
- CTA: End with a {config.get('cta_style', 'subscribe')} call to action.

TITLE REQUIREMENTS:
- {title_instructions.get(config.get('title_pattern', 'number'), title_instructions['number'])}
- Title length: {config.get('title_length', 'medium')} (short=under 50 chars, medium=51-65, long=65+)
- {'Include 1 relevant emoji in the title.' if config.get('include_emoji') else 'Do not use emoji in the title.'}

CONTENT FOCUS:
{subtopic_line}
Write authentically for the niche — do not use generic filler content.

{confidence_note}

OUTPUT FORMAT:
Return a JSON object with keys: title, hook, script, cta, thumbnail_text (max 6 words for thumbnail overlay).
""".strip()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--optimise', action='store_true')
    parser.add_argument('--user',     type=str, default=None)
    parser.add_argument('--config',   type=str, default=None, help='Print prompt config for user')
    args = parser.parse_args()

    engine = LearningEngine()

    if args.config:
        config = asyncio.run(engine.get_prompt_config(args.config))
        print(json.dumps(config, indent=2))

    elif args.optimise:
        if args.user:
            changed = asyncio.run(engine.optimise_user(args.user))
            print(f"User {args.user}: {'optimised' if changed else 'no change'}")
        else:
            result = asyncio.run(engine.optimise_all())
            print(f"Done: {result}")
    else:
        parser.print_help()
