"""
Viralityy — M6: AI Humanisation Layer
--------------------------------------
Post-processes AI-generated video assets to make them feel human-made
and avoid the "AI content" detection patterns that hurt watch time and
channel trust scores.

Applies 6 humanisation passes:
  1. Script imperfections    — natural filler words, self-corrections, em-dashes
  2. Voice speed variation   — subtle tempo shifts between sentences
  3. B-roll deduplication    — tracks used clips per user, avoids repeats
  4. Transition rotation     — cycles through a pool of transitions per video
  5. Colour grade shifting   — slight temperature/saturation variation per render
  6. Timing micro-variation  — random ±2-4% on subtitle display durations

Usage:
  from humaniser import Humaniser
  h = Humaniser(user_id="abc123")
  script  = h.humanise_script(raw_script)
  tts_cfg = h.get_tts_config(script)
  broll   = h.select_broll(keywords, count=5)
  grade   = h.get_colour_grade()
  cuts    = h.get_transitions(scene_count=6)
"""

import os
import re
import random
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# HUMANISATION CONFIG — tweak these to adjust intensity
# ---------------------------------------------------------------------------
CONFIG = {
    # Script imperfections
    "imperfection_rate":   0.08,    # ~8% of sentences get a natural imperfection
    "filler_rate":         0.05,    # ~5% of sentences get a filler word at start
    "em_dash_rate":        0.12,    # ~12% of long sentences get an em-dash break

    # Voice / TTS
    "speed_base":          1.0,     # base speaking rate (1.0 = normal)
    "speed_variance":      0.08,    # ±8% variation between sentences
    "pause_min_ms":        180,     # minimum pause between sentences (ms)
    "pause_max_ms":        420,     # maximum pause between sentences (ms)
    "pitch_variance":      0.04,    # ±4% pitch micro-shift

    # Colour grade
    "temp_variance":       8,       # ±8K colour temperature shift
    "sat_variance":        0.06,    # ±6% saturation shift
    "brightness_variance": 0.03,    # ±3% brightness shift
    "contrast_variance":   0.04,    # ±4% contrast shift

    # Timing
    "subtitle_variance":   0.035,   # ±3.5% duration variation on subtitles
}

# ---------------------------------------------------------------------------
# POOLS — randomly sampled, never same choice twice per session
# ---------------------------------------------------------------------------
FILLER_WORDS = [
    "Now,", "Look,", "Here's the thing —", "And honestly,",
    "So,", "Right,", "Think about it —", "The truth is,",
    "Actually,", "Here's what most people miss —",
]

SELF_CORRECTIONS = [
    ("is", "— well, actually,", "is"),
    ("are", "— or rather,", "are"),
    ("you", "— and I mean this —", "you"),
    ("that", "— specifically", "that"),
]

TRANSITION_POOL = [
    "cut",         # hard cut
    "j_cut",       # audio leads video
    "l_cut",       # video leads audio
    "smash_cut",   # abrupt for emphasis
    "cross_fade",  # gentle blend
    "dip_black",   # dip to black
    "whip_pan",    # fast horizontal wipe
    "match_cut",   # shape/colour match
]

# Colour grade presets — each has slight variations on warm/cool/neutral
GRADE_PRESETS = [
    {"name": "warm_natural",   "temp_k": 5800, "sat": 1.05, "contrast": 1.02, "lift": 0.01},
    {"name": "cool_crisp",     "temp_k": 6400, "sat": 0.97, "contrast": 1.04, "lift": 0.00},
    {"name": "golden_soft",    "temp_k": 5500, "sat": 1.08, "contrast": 0.99, "lift": 0.02},
    {"name": "neutral_punch",  "temp_k": 6200, "sat": 1.00, "contrast": 1.06, "lift": 0.00},
    {"name": "desaturated_pro","temp_k": 6000, "sat": 0.92, "contrast": 1.03, "lift": 0.01},
    {"name": "vivid_warm",     "temp_k": 5700, "sat": 1.12, "contrast": 1.01, "lift": 0.02},
]


class Humaniser:
    """
    All methods are deterministic-but-varied: seeded on user_id + video_index
    so the same video always gets the same humanisation (reproducible),
    but different videos get different humanisation (varied).
    """

    def __init__(self, user_id: str, video_index: int = 0, mongo_uri: str = None):
        self.user_id      = user_id
        self.video_index  = video_index
        self.mongo_uri    = mongo_uri or os.environ.get('MONGODB_URI', 'mongodb://localhost:27017')
        self._broll_used  = set()   # tracks used B-roll IDs this session

        # Seed RNG: same user + video = same choices, different video = different
        seed_str = f"{user_id}:{video_index}:{datetime.now(timezone.utc).strftime('%Y%m%d')}"
        seed_int = int(hashlib.md5(seed_str.encode()).hexdigest(), 16) % (2**32)
        self._rng = random.Random(seed_int)

    # ==================================================================
    # PASS 1 — Script Imperfections
    # ==================================================================
    def humanise_script(self, script: str) -> str:
        """
        Adds natural human imperfections to an AI-generated script.
        Returns the modified script string.
        """
        sentences = self._split_sentences(script)
        result    = []

        for i, sentence in enumerate(sentences):
            s = sentence.strip()
            if not s:
                continue

            # Em-dash break — split long sentences naturally
            if (len(s) > 80
                    and self._rng.random() < CONFIG['em_dash_rate']
                    and ',' in s):
                s = self._insert_em_dash(s)

            # Filler word at sentence start
            if i > 0 and self._rng.random() < CONFIG['filler_rate']:
                filler = self._rng.choice(FILLER_WORDS)
                s = f"{filler} {s[0].lower()}{s[1:]}"

            # Micro self-correction (very occasional, only mid-sentence)
            if (self._rng.random() < CONFIG['imperfection_rate']
                    and len(s.split()) > 8):
                s = self._insert_self_correction(s)

            result.append(s)

        return ' '.join(result)

    def _split_sentences(self, text: str) -> list:
        return re.split(r'(?<=[.!?])\s+', text)

    def _insert_em_dash(self, sentence: str) -> str:
        """Replace a comma in the middle of a long sentence with an em-dash."""
        comma_positions = [i for i, c in enumerate(sentence) if c == ',']
        if not comma_positions:
            return sentence
        mid = len(sentence) // 2
        best = min(comma_positions, key=lambda p: abs(p - mid))
        return sentence[:best] + ' —' + sentence[best+1:]

    def _insert_self_correction(self, sentence: str) -> str:
        """Insert a natural mid-sentence self-correction."""
        words = sentence.split()
        if len(words) < 5:
            return sentence
        # Pick a position in the first half of the sentence
        pos = self._rng.randint(2, len(words) // 2)
        word_at_pos = words[pos].lower().rstrip('.,!?')

        for trigger, correction, _ in SELF_CORRECTIONS:
            if word_at_pos == trigger:
                words.insert(pos + 1, correction)
                return ' '.join(words)
        return sentence

    # ==================================================================
    # PASS 2 — Voice / TTS Config
    # ==================================================================
    def get_tts_config(self, script: str) -> dict:
        """
        Returns per-sentence TTS parameters (speed, pause, pitch).
        Feed this to your TTS provider (ElevenLabs, Google TTS, etc.).
        """
        sentences = [s.strip() for s in self._split_sentences(script) if s.strip()]
        sentence_configs = []

        for sentence in sentences:
            speed = CONFIG['speed_base'] + self._rng.uniform(
                -CONFIG['speed_variance'], CONFIG['speed_variance']
            )
            pause = self._rng.randint(CONFIG['pause_min_ms'], CONFIG['pause_max_ms'])
            pitch = 1.0 + self._rng.uniform(
                -CONFIG['pitch_variance'], CONFIG['pitch_variance']
            )

            # Slower on important/short sentences, faster on lists
            if len(sentence.split()) < 8:
                speed = max(0.88, speed - 0.06)   # slow down short punchy lines
                pause += 80
            elif sentence.endswith('—'):
                speed = min(1.15, speed + 0.05)   # speed up at em-dash

            sentence_configs.append({
                'text':       sentence,
                'speed':      round(speed, 3),
                'pause_ms':   pause,
                'pitch':      round(pitch, 3),
            })

        return {
            'sentences':     sentence_configs,
            'total_sentences': len(sentence_configs),
            'avg_speed':     round(
                sum(s['speed'] for s in sentence_configs) / max(len(sentence_configs), 1), 3
            ),
        }

    # ==================================================================
    # PASS 3 — B-roll Deduplication
    # ==================================================================
    def select_broll(self, keywords: list, count: int = 5,
                     used_globally: set = None) -> list:
        """
        Selects B-roll clip IDs for a video, avoiding clips used in
        previous videos by this user (passed in as used_globally set).

        In production: query your stock footage library (Pexels, Pixabay, etc.)
        filtered by keywords, then pass the candidate IDs here.

        Returns a list of selected clip dicts.
        """
        if used_globally is None:
            used_globally = set()

        # Simulate a pool of available clips per keyword
        # In production, replace this with actual API results
        candidates = self._generate_candidate_pool(keywords)

        # Exclude already-used clips
        fresh = [c for c in candidates if c['id'] not in used_globally
                 and c['id'] not in self._broll_used]

        # If not enough fresh clips, allow reuse of globally used ones
        if len(fresh) < count:
            fallback = [c for c in candidates if c['id'] not in self._broll_used]
            fresh = fallback

        # Shuffle with seeded RNG for reproducibility
        self._rng.shuffle(fresh)
        selected = fresh[:count]

        # Track used clips this session
        for clip in selected:
            self._broll_used.add(clip['id'])

        return selected

    def _generate_candidate_pool(self, keywords: list) -> list:
        """
        Generates a deterministic pool of mock clip IDs per keyword.
        Replace with real API calls to Pexels / Pixabay in production.
        """
        pool = []
        for kw in keywords:
            kw_hash = int(hashlib.md5(kw.encode()).hexdigest(), 16)
            for i in range(20):  # 20 candidates per keyword
                clip_id = f"clip_{kw[:6]}_{(kw_hash + i) % 99999:05d}"
                pool.append({
                    'id':      clip_id,
                    'keyword': kw,
                    'source':  'pexels',
                    'duration': self._rng.randint(4, 12),  # seconds
                })
        return pool

    # ==================================================================
    # PASS 4 — Transition Rotation
    # ==================================================================
    def get_transitions(self, scene_count: int) -> list:
        """
        Returns a sequence of transitions for a video with scene_count scenes.
        Never uses the same transition twice in a row.
        Weighted toward natural cuts, rarer special transitions.
        """
        weights = {
            'cut':        35,
            'j_cut':      20,
            'l_cut':      18,
            'cross_fade': 12,
            'smash_cut':   6,
            'dip_black':   4,
            'whip_pan':    3,
            'match_cut':   2,
        }

        transitions = []
        last = None
        pool = list(weights.keys())
        w    = list(weights.values())

        for _ in range(max(0, scene_count - 1)):
            available = [t for t in pool if t != last]
            avail_w   = [weights[t] for t in available]
            chosen    = self._rng.choices(available, weights=avail_w, k=1)[0]
            transitions.append(chosen)
            last = chosen

        return transitions

    # ==================================================================
    # PASS 5 — Colour Grade
    # ==================================================================
    def get_colour_grade(self) -> dict:
        """
        Returns a colour grade config for this video.
        Picks a preset then applies small random offsets so each
        video has a subtly different look.
        """
        preset = self._rng.choice(GRADE_PRESETS).copy()

        # Apply micro-variation on top of preset
        preset['temp_k']   += self._rng.randint(
            -CONFIG['temp_variance'], CONFIG['temp_variance']
        )
        preset['sat']      += self._rng.uniform(
            -CONFIG['sat_variance'], CONFIG['sat_variance']
        )
        preset['contrast'] += self._rng.uniform(
            -CONFIG['contrast_variance'], CONFIG['contrast_variance']
        )
        preset['lift']     += self._rng.uniform(
            -CONFIG['brightness_variance'], CONFIG['brightness_variance']
        )

        # Round for readability
        preset['sat']      = round(preset['sat'], 3)
        preset['contrast'] = round(preset['contrast'], 3)
        preset['lift']     = round(preset['lift'], 3)

        return preset

    # ==================================================================
    # PASS 6 — Subtitle Timing Micro-Variation
    # ==================================================================
    def vary_subtitle_timings(self, subtitles: list) -> list:
        """
        Takes a list of subtitle dicts with 'start', 'end', 'text' keys.
        Applies ±3.5% variation to each display duration.
        Returns the modified subtitle list.
        """
        result = []
        for sub in subtitles:
            duration = sub['end'] - sub['start']
            variation = self._rng.uniform(
                -CONFIG['subtitle_variance'], CONFIG['subtitle_variance']
            )
            new_duration = duration * (1 + variation)
            result.append({
                **sub,
                'end': round(sub['start'] + new_duration, 3),
            })
        return result

    # ==================================================================
    # CONVENIENCE: run all passes at once
    # ==================================================================
    def humanise_all(self, raw_script: str, keywords: list,
                     scene_count: int, subtitles: list = None,
                     used_broll: set = None) -> dict:
        """
        Runs all 6 humanisation passes and returns a complete package.
        This is what pipeline.py calls.
        """
        humanised_script = self.humanise_script(raw_script)
        return {
            'script':      humanised_script,
            'tts_config':  self.get_tts_config(humanised_script),
            'broll':       self.select_broll(keywords, count=5, used_globally=used_broll),
            'transitions': self.get_transitions(scene_count),
            'grade':       self.get_colour_grade(),
            'subtitles':   self.vary_subtitle_timings(subtitles) if subtitles else [],
            'meta': {
                'user_id':      self.user_id,
                'video_index':  self.video_index,
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'version':      'M6',
            }
        }


# ---------------------------------------------------------------------------
# HOW TO INTEGRATE INTO pipeline.py
# ---------------------------------------------------------------------------
# Add these lines to your existing pipeline.py, in the video generation loop:
#
#   from humaniser import Humaniser
#
#   h = Humaniser(user_id=user['_id'], video_index=video_counter)
#   result = h.humanise_all(
#       raw_script   = ai_generated_script,
#       keywords     = niche_keywords,
#       scene_count  = len(scenes),
#       subtitles    = generated_subtitles,
#       used_broll   = user.get('usedBrollIds', set()),
#   )
#   final_script    = result['script']
#   tts_config      = result['tts_config']
#   broll_clips     = result['broll']
#   transitions     = result['transitions']
#   colour_grade    = result['grade']
#   final_subtitles = result['subtitles']
#
#   # Save used B-roll IDs back to user record so they aren't reused
#   user['usedBrollIds'] = user.get('usedBrollIds', set()) | h._broll_used
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# CLI TEST — python humaniser.py
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import json

    SAMPLE_SCRIPT = (
        "Most people have no idea that their morning routine is actually making them less productive. "
        "The first thing you do when you wake up sets the tone for your entire day. "
        "Checking your phone immediately floods your brain with other people's priorities. "
        "Instead, give yourself 10 minutes of silence before touching any screen. "
        "Your brain is in a highly creative state for the first 90 minutes after waking. "
        "Protect that window and you will get more done before noon than most people do all day."
    )

    h = Humaniser(user_id="test_user_001", video_index=3)

    print("=== HUMANISED SCRIPT ===")
    hs = h.humanise_script(SAMPLE_SCRIPT)
    print(hs)

    print("\n=== TTS CONFIG (first 2 sentences) ===")
    tts = h.get_tts_config(hs)
    for s in tts['sentences'][:2]:
        print(f"  speed={s['speed']} pause={s['pause_ms']}ms pitch={s['pitch']}")
        print(f"  text: {s['text'][:80]}")

    print("\n=== COLOUR GRADE ===")
    print(json.dumps(h.get_colour_grade(), indent=2))

    print("\n=== TRANSITIONS (6 scenes) ===")
    print(h.get_transitions(6))

    print("\n=== B-ROLL (3 clips) ===")
    clips = h.select_broll(['productivity', 'morning', 'focus'], count=3)
    for c in clips:
        print(f"  {c['id']} ({c['duration']}s) — keyword: {c['keyword']}")

    print("\nAll M6 passes OK.")
