"""
Viralityy — Auto-Voiceover Agent (Tier 2, Agent 6)
===================================================
Converts finished scripts into natural-sounding audio files using
Google Gemini TTS, with per-channel voice assignment, humaniser.py
integration, and full audio asset management.

PROVIDER: Google Gemini TTS (gemini-2.5-flash-preview-tts)
  - Uses your existing Google AI / Gemini paid plan — no extra billing
  - 30 available voices with distinct characteristics
  - Native speaking style control per sentence
  - Returns raw PCM audio — assembled into WAV per video
  - No voice cloning needed — use voice assignment per channel instead

PIPELINE POSITION:
  Script → [humaniser.py: humanise_script()] → [THIS AGENT: render audio]
          → [video assembler: combine audio + video]

USAGE:
  from auto_voiceover_agent import AutoVoiceoverAgent
  agent = AutoVoiceoverAgent()

  audio = await agent.render(
      script="Your script...", user_id="abc123",
      channel_id="UC...", niche_id="psychology_facts",
  )

  await agent.assign_voice(user_id, channel_id, "Kore")

  voices = await agent.list_available_voices()

ENV VARS:
  GEMINI_API_KEY    — required (your existing Google AI key)
  GEMINI_VOICE_NAME — optional global default voice name (e.g. Kore)
  AUDIO_OUTPUT_DIR  — where WAV files are saved (default /tmp/viralityy_audio)
  AUDIO_CDN_BASE_URL— optional CDN prefix for audio URLs

DEPENDENCIES:
  google-generativeai  (add to requirements.txt: google-generativeai>=0.8.0)
  motor, wave (stdlib)
"""

import os
import wave
import asyncio
import logging
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

try:
    import google.generativeai as genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

logging.basicConfig(level=logging.INFO, format='[Voiceover] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
AUDIO_TTL_DAYS    = 30
MAX_RETRIES       = 3
RETRY_DELAY_S     = 2
WORDS_PER_MINUTE  = 150
GEMINI_TTS_MODEL  = "gemini-2.5-flash-preview-tts"
GEMINI_SAMPLE_RATE = 24000   # Hz — Gemini returns 24kHz LINEAR16 PCM
OUTPUT_DIR        = os.environ.get("AUDIO_OUTPUT_DIR", "/tmp/viralityy_audio")

# ---------------------------------------------------------------------------
# VOICE CATALOGUE — all 30 Gemini TTS voices
# ---------------------------------------------------------------------------
GEMINI_VOICES = {
    "Kore":           {"style": "calm",        "gender": "f", "tone": "warm"},
    "Aoede":          {"style": "calm",        "gender": "f", "tone": "clear"},
    "Leda":           {"style": "calm",        "gender": "f", "tone": "gentle"},
    "Zephyr":         {"style": "calm",        "gender": "f", "tone": "breathy"},
    "Autonoe":        {"style": "calm",        "gender": "f", "tone": "soft"},
    "Orus":           {"style": "firm",        "gender": "m", "tone": "deep"},
    "Fenrir":         {"style": "firm",        "gender": "m", "tone": "strong"},
    "Enceladus":      {"style": "firm",        "gender": "m", "tone": "clear"},
    "Iapetus":        {"style": "firm",        "gender": "m", "tone": "measured"},
    "Perseus":        {"style": "firm",        "gender": "m", "tone": "confident"},
    "Puck":           {"style": "upbeat",      "gender": "m", "tone": "bright"},
    "Charon":         {"style": "upbeat",      "gender": "m", "tone": "energetic"},
    "Rasalgethi":     {"style": "upbeat",      "gender": "m", "tone": "warm"},
    "Achird":         {"style": "upbeat",      "gender": "m", "tone": "cheerful"},
    "Schedar":        {"style": "friendly",    "gender": "f", "tone": "warm"},
    "Sulafat":        {"style": "friendly",    "gender": "f", "tone": "conversational"},
    "Gacrux":         {"style": "friendly",    "gender": "m", "tone": "approachable"},
    "Algieba":        {"style": "friendly",    "gender": "m", "tone": "smooth"},
    "Umbriel":        {"style": "gentle",      "gender": "f", "tone": "soft"},
    "Zubenelgenubi":  {"style": "gentle",      "gender": "f", "tone": "soothing"},
    "Vindemiatrix":   {"style": "gentle",      "gender": "f", "tone": "quiet"},
    "Achernar":       {"style": "neutral",     "gender": "f", "tone": "clear"},
    "Despina":        {"style": "neutral",     "gender": "f", "tone": "professional"},
    "Erinome":        {"style": "neutral",     "gender": "f", "tone": "balanced"},
    "Algenib":        {"style": "neutral",     "gender": "m", "tone": "clear"},
    "Pulcherrima":    {"style": "expressive",  "gender": "f", "tone": "vibrant"},
    "Sadachbia":      {"style": "expressive",  "gender": "m", "tone": "dynamic"},
    "Alnilam":        {"style": "informative", "gender": "m", "tone": "measured"},
    "Sheratan":       {"style": "informative", "gender": "f", "tone": "clear"},
    "Callirrhoe":     {"style": "casual",      "gender": "f", "tone": "relaxed"},
}

# Niche → best-fit Gemini voice
DEFAULT_VOICES = {
    "psychology_facts":   {"id": "Kore",          "name": "Kore"},
    "history_facts":      {"id": "Aoede",          "name": "Aoede"},
    "science_explained":  {"id": "Leda",           "name": "Leda"},
    "book_summaries":     {"id": "Autonoe",        "name": "Autonoe"},
    "personal_finance":   {"id": "Orus",           "name": "Orus"},
    "investing_basics":   {"id": "Fenrir",         "name": "Fenrir"},
    "real_estate":        {"id": "Enceladus",      "name": "Enceladus"},
    "side_hustles":       {"id": "Puck",           "name": "Puck"},
    "fitness_home":       {"id": "Charon",         "name": "Charon"},
    "nutrition_diet":     {"id": "Achird",         "name": "Achird"},
    "mental_health":      {"id": "Umbriel",        "name": "Umbriel"},
    "sleep_optimization": {"id": "Zubenelgenubi",  "name": "Zubenelgenubi"},
    "ai_tools":           {"id": "Alnilam",        "name": "Alnilam"},
    "smartphone_tips":    {"id": "Despina",        "name": "Despina"},
    "coding_beginners":   {"id": "Achernar",       "name": "Achernar"},
    "stoicism":           {"id": "Iapetus",        "name": "Iapetus"},
    "self_improvement":   {"id": "Schedar",        "name": "Schedar"},
    "_default":           {"id": "Kore",           "name": "Kore"},
}


# ===========================================================================
# AGENT
# ===========================================================================

class AutoVoiceoverAgent:

    def __init__(self, mongo_uri=None, gemini_api_key=None, audio_output_dir=None):
        self.mongo_uri  = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.gemini_key = gemini_api_key or os.environ.get("GEMINI_API_KEY")
        self.output_dir = audio_output_dir or OUTPUT_DIR
        self._db        = None
        self._configured = False
        os.makedirs(self.output_dir, exist_ok=True)

    # --- Connections ---

    async def _get_db(self):
        if self._db:
            return self._db
        if not HAS_MOTOR:
            raise RuntimeError("pip install motor")
        self._db = AsyncIOMotorClient(self.mongo_uri)["viralityy"]
        return self._db

    def _configure_genai(self):
        if not HAS_GENAI:
            raise RuntimeError("pip install google-generativeai")
        if not self.gemini_key:
            raise RuntimeError("GEMINI_API_KEY not set — add it in Railway env vars")
        if not self._configured:
            genai.configure(api_key=self.gemini_key)
            self._configured = True

    # --- Public API ---

    async def render(self, script: str, user_id: str, channel_id: str = "",
                     video_id: str = "", niche_id: str = "",
                     humanise: bool = True) -> dict:
        """Convert a script to audio. Returns render result dict."""
        if not self.gemini_key:
            raise RuntimeError("GEMINI_API_KEY not set")

        log.info(f"Rendering audio user={user_id} video={video_id}")

        # Humanise
        humanised_script = script
        tts_config = None
        if humanise:
            try:
                from humaniser import Humaniser
                h = Humaniser(user_id=user_id, video_index=hash(video_id) % 10000)
                humanised_script = h.humanise_script(script)
                tts_config = h.get_tts_config(humanised_script)
                log.info(f"Humaniser: {tts_config['total_sentences']} sentences")
            except ImportError:
                log.warning("humaniser.py not found")
        if tts_config is None:
            tts_config = self._basic_tts_config(humanised_script)

        # Get voice
        voice = await self._get_voice_for_channel(user_id, channel_id, niche_id)
        log.info(f"Voice: {voice['name']}")

        # Render
        audio_path, seg_count = await asyncio.to_thread(
            self._render_and_assemble, tts_config, voice, user_id, video_id
        )

        duration_s = round((len(humanised_script.split()) / WORDS_PER_MINUTE) * 60, 1)

        db = await self._get_db()
        render_id = await self._store_render(
            db, user_id, channel_id, video_id, niche_id,
            audio_path, voice, duration_s, tts_config
        )

        return {
            "renderId": render_id,
            "audioPath": audio_path,
            "audioUrl": self._path_to_url(audio_path),
            "durationSeconds": duration_s,
            "voiceId": voice["id"],
            "voiceName": voice["name"],
            "segmentCount": seg_count,
            "provider": "gemini",
            "renderedAt": datetime.now(timezone.utc).isoformat(),
        }

    async def estimate_duration(self, script: str) -> float:
        return round((len(script.split()) / WORDS_PER_MINUTE) * 60, 1)

    async def list_available_voices(self) -> list:
        return [
            {"id": n, "name": n, "style": i["style"],
             "gender": i["gender"], "tone": i["tone"], "provider": "gemini"}
            for n, i in GEMINI_VOICES.items()
        ]

    async def get_channel_voice(self, user_id: str, channel_id: str) -> Optional[dict]:
        db = await self._get_db()
        doc = await db.channel_voices.find_one({"userId": user_id, "channelId": channel_id})
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc

    async def assign_voice(self, user_id: str, channel_id: str,
                           voice_id: str, voice_name: str = "") -> dict:
        if voice_id not in GEMINI_VOICES:
            raise ValueError(f"Unknown voice '{voice_id}'. Run --list-voices to see options.")
        db = await self._get_db()
        doc = {
            "userId": user_id, "channelId": channel_id,
            "voiceId": voice_id, "voiceName": voice_name or voice_id,
            "provider": "gemini", "type": "assigned",
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        await db.channel_voices.update_one(
            {"userId": user_id, "channelId": channel_id},
            {"$set": doc}, upsert=True
        )
        log.info(f"Assigned voice '{voice_id}' to channel {channel_id}")
        return doc

    async def get_audio_files(self, user_id: str, limit: int = 20) -> list:
        db = await self._get_db()
        cursor = db.audio_renders.find(
            {"userId": user_id}, sort=[("renderedAt", -1)]
        ).limit(limit)
        docs = await cursor.to_list(length=limit)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    # --- Voice selection ---

    async def _get_voice_for_channel(self, user_id, channel_id, niche_id) -> dict:
        if channel_id:
            db = await self._get_db()
            saved = await db.channel_voices.find_one(
                {"userId": user_id, "channelId": channel_id}
            )
            if saved:
                return {"id": saved["voiceId"], "name": saved["voiceName"]}
        env_v = os.environ.get("GEMINI_VOICE_NAME")
        if env_v and env_v in GEMINI_VOICES:
            return {"id": env_v, "name": env_v}
        return DEFAULT_VOICES.get(niche_id, DEFAULT_VOICES["_default"])

    # --- Rendering ---

    def _render_and_assemble(self, tts_config, voice, user_id, video_id) -> tuple:
        sentences  = tts_config.get("sentences", [])
        timestamp  = int(time.time())
        safe_vid   = (video_id or "noref").replace("/", "_")[:40]
        out_dir    = os.path.join(self.output_dir, user_id[:8])
        os.makedirs(out_dir, exist_ok=True)
        final_path = os.path.join(out_dir, f"{safe_vid}_{timestamp}.wav")

        voice_name = voice["id"]
        voice_info = GEMINI_VOICES.get(voice_name, {})
        pcm_chunks = []
        seg_count  = 0

        for i, s in enumerate(sentences):
            text     = s.get("text", "").strip()
            speed    = s.get("speed", 1.0)
            pause_ms = s.get("pause_ms", 350)
            if not text:
                continue

            prompt = self._style_prompt(text, voice_info, speed)
            pcm    = self._tts_with_retry(prompt, voice_name)

            if pcm:
                pcm_chunks.append((pcm, pause_ms))
                seg_count += 1
                log.debug(f"Seg {i+1}/{len(sentences)}: {len(pcm)} bytes")
            else:
                log.warning(f"Seg {i+1} failed — skipped")

        self._write_wav(final_path, pcm_chunks)
        log.info(f"WAV assembled: {final_path} | {seg_count} segs | "
                 f"{os.path.getsize(final_path)//1024}KB")
        return final_path, seg_count

    def _style_prompt(self, text: str, voice_info: dict, speed: float) -> str:
        style_desc = {
            "calm": "calmly and clearly",
            "firm": "with authority and confidence",
            "upbeat": "with energy and enthusiasm",
            "gentle": "gently and warmly",
            "friendly": "in a warm conversational tone",
            "neutral": "clearly and professionally",
            "expressive": "expressively with natural variation",
            "informative": "informatively and clearly",
            "casual": "casually and naturally",
        }.get(voice_info.get("style", "neutral"), "clearly")

        pace = ("at a slightly faster pace" if speed > 1.1
                else "at a slightly slower pace" if speed < 0.9
                else "at a natural pace")
        return f"Read the following {style_desc}, {pace}: {text}"

    def _tts_with_retry(self, text: str, voice_name: str) -> Optional[bytes]:
        self._configure_genai()
        for attempt in range(MAX_RETRIES):
            try:
                client   = genai.Client()
                response = client.models.generate_content(
                    model=GEMINI_TTS_MODEL,
                    contents=text,
                    config=genai.types.GenerateContentConfig(
                        response_modalities=["AUDIO"],
                        speech_config=genai.types.SpeechConfig(
                            voice_config=genai.types.VoiceConfig(
                                prebuilt_voice_config=genai.types.PrebuiltVoiceConfig(
                                    voice_name=voice_name,
                                )
                            )
                        ),
                    ),
                )
                return response.candidates[0].content.parts[0].inline_data.data
            except Exception as e:
                err = str(e).lower()
                wait = RETRY_DELAY_S * (attempt + 2)
                if any(x in err for x in ["quota", "rate", "429", "resource_exhausted"]):
                    log.warning(f"Gemini rate limit — waiting {wait}s")
                    time.sleep(wait)
                else:
                    log.error(f"Gemini TTS error (attempt {attempt+1}): {e}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_S)
                    else:
                        break
        return None

    def _write_wav(self, path: str, pcm_chunks: list):
        all_pcm = bytearray()
        for pcm_bytes, pause_ms in pcm_chunks:
            all_pcm.extend(pcm_bytes)
            silence = b"\x00\x00" * int(GEMINI_SAMPLE_RATE * pause_ms / 1000)
            all_pcm.extend(silence)
        with wave.open(path, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(GEMINI_SAMPLE_RATE)
            wf.writeframes(bytes(all_pcm))

    def _basic_tts_config(self, script: str) -> dict:
        import re
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", script) if s.strip()]
        return {
            "sentences": [{"text": s, "speed": 1.0, "pause_ms": 350} for s in sentences],
            "total_sentences": len(sentences),
            "avg_speed": 1.0,
        }

    def _path_to_url(self, path: str) -> str:
        base = os.environ.get("AUDIO_CDN_BASE_URL", "")
        if base:
            rel = path.replace(self.output_dir, "").lstrip("/")
            return f"{base.rstrip('/')}/audio/{rel}"
        return f"file://{path}"

    async def _store_render(self, db, user_id, channel_id, video_id, niche_id,
                            audio_path, voice, duration_s, tts_config) -> str:
        doc = {
            "userId": user_id, "channelId": channel_id, "videoId": video_id,
            "nicheId": niche_id, "audioPath": audio_path,
            "audioUrl": self._path_to_url(audio_path),
            "voiceId": voice["id"], "voiceName": voice["name"],
            "provider": "gemini",
            "durationSeconds": round(duration_s, 1),
            "segmentCount": tts_config.get("total_sentences", 0),
            "renderedAt": datetime.now(timezone.utc).isoformat(),
            "expiresAt": (datetime.now(timezone.utc) + timedelta(days=AUDIO_TTL_DAYS)).isoformat(),
            "status": "ready",
        }
        result = await db.audio_renders.insert_one(doc)
        return str(result.inserted_id)

    async def ensure_indexes(self):
        db = await self._get_db()
        await db.channel_voices.create_index(
            [("userId", 1), ("channelId", 1)], unique=True, name="idx_voice_user_channel"
        )
        await db.audio_renders.create_index("userId", name="idx_render_user")
        await db.audio_renders.create_index(
            "expiresAt", expireAfterSeconds=0, name="idx_render_ttl"
        )
        log.info("Voiceover indexes confirmed")


# ===========================================================================
# CLI
# ===========================================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Auto-Voiceover Agent (Gemini TTS)")
    parser.add_argument("--render",       type=str, metavar="SCRIPT_FILE")
    parser.add_argument("--user",         type=str, default="test")
    parser.add_argument("--channel",      type=str, default="")
    parser.add_argument("--niche",        type=str, default="")
    parser.add_argument("--estimate",     type=str, metavar="SCRIPT_FILE")
    parser.add_argument("--list-voices",  action="store_true")
    parser.add_argument("--assign-voice", nargs=3,
                        metavar=("USER_ID", "CHANNEL_ID", "VOICE_NAME"))
    parser.add_argument("--audio-files",  type=str, metavar="USER_ID")
    parser.add_argument("--indexes",      action="store_true")
    args = parser.parse_args()

    agent = AutoVoiceoverAgent()

    if args.indexes:
        asyncio.run(agent.ensure_indexes())
    elif args.list_voices:
        voices = asyncio.run(agent.list_available_voices())
        for v in voices:
            print(f"  {v['id']:<22} style={v['style']:<12} gender={v['gender']} tone={v['tone']}")
    elif args.assign_voice:
        print(json.dumps(asyncio.run(agent.assign_voice(*args.assign_voice)), indent=2, default=str))
    elif args.estimate:
        with open(args.estimate) as f: script = f.read()
        d = asyncio.run(agent.estimate_duration(script))
        print(f"Estimated: {d}s ({d/60:.1f} min)")
    elif args.render:
        with open(args.render) as f: script = f.read()
        print(json.dumps(asyncio.run(agent.render(
            script=script, user_id=args.user,
            channel_id=args.channel, niche_id=args.niche
        )), indent=2, default=str))
    elif args.audio_files:
        print(json.dumps(asyncio.run(agent.get_audio_files(args.audio_files)), indent=2, default=str))
    else:
        parser.print_help()
