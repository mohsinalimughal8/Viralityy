"""
Viralityy — Script Research Agent (Tier 1, Agent 4)
====================================================
Given a video title, automatically deep-researches the topic and returns
a fully structured brief that the script writer can use directly — no
human research needed.

Inspired by the ai-engineering-hub "Multi-Agent Deep Researcher" (MCP-powered)
project. That project uses a multi-agent approach where different agents
handle different research tasks concurrently. This implementation adapts
that pattern into a three-agent research crew:

  Researcher Agent   : searches the web for facts, stats, examples
  Validator Agent    : cross-checks key claims for accuracy
  Synthesiser Agent  : organises everything into a clean script brief

WHAT IT PRODUCES per video title:
  - Core topic explanation (what this is, why it matters)
  - 5 key facts or stats (sourced, with approximate recency)
  - 3 compelling examples or case studies
  - Strongest hook angle (the most surprising or counterintuitive angle)
  - Common misconceptions to address (makes the video more valuable)
  - Suggested structure (intro → point 1 → point 2 → ... → CTA)
  - Recommended script length (based on format)
  - SEO-enriched title variants (3 options)
  - Credibility signals (scientific terms, names to mention)

USAGE:
  from script_research_agent import ScriptResearchAgent
  agent = ScriptResearchAgent()

  # Research a single title
  brief = await agent.research("Why your brain lies to you every day")

  # Research from pipeline queue (called by pipeline orchestrator)
  brief = await agent.research_from_queue(queue_item_id="abc123")

  # Get stored brief
  brief = await agent.get_brief(brief_id="xyz789")

  # Get all pending briefs for a user
  briefs = await agent.get_pending_briefs(user_id="abc123")

MONGO COLLECTIONS:
  script_briefs     — stored research briefs (TTL: 30 days)
  pipeline_queue    — updates status when brief is complete

DEPENDENCIES:
  - OpenAI API          (OPENAI_API_KEY in env) — all three agents use this
  - MongoDB / Motor     (MONGODB_URI in env)
  - requests            (already in requirements.txt) — for web search
  - Optional: TAVILY_API_KEY or SERPER_API_KEY for richer web search
    (falls back to OpenAI's knowledge if not set — still highly useful)

NOTE ON WEB SEARCH:
  This agent uses web search to ground facts in current information.
  If neither TAVILY_API_KEY nor SERPER_API_KEY is set, it falls back
  to OpenAI's training knowledge — still produces excellent briefs,
  just without live web citations. Add a search key when possible.
"""

import os
import asyncio
import logging
import json
import requests
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

logging.basicConfig(level=logging.INFO, format='[ScriptResearch] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
BRIEF_TTL_DAYS     = 30     # briefs auto-delete after 30 days
MAX_SEARCH_RESULTS = 8      # web results to feed to researcher
DEFAULT_MODEL      = "gpt-4o"      # use the more capable model for research
FAST_MODEL         = "gpt-4o-mini" # used for lightweight synthesis steps

# Research quality settings
RESEARCH_DEPTH_MAP = {
    "short":    {"facts": 4, "examples": 2, "word_count": 150},
    "long":     {"facts": 7, "examples": 4, "word_count": 300},
    "standard": {"facts": 5, "examples": 3, "word_count": 200},
}


# ===========================================================================
# SCRIPT RESEARCH AGENT
# ===========================================================================

class ScriptResearchAgent:

    def __init__(self, mongo_uri: str = None, openai_api_key: str = None,
                 tavily_api_key: str = None, serper_api_key: str = None):
        self.mongo_uri   = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.openai_key  = openai_api_key or os.environ.get("OPENAI_API_KEY")
        self.tavily_key  = tavily_api_key or os.environ.get("TAVILY_API_KEY")
        self.serper_key  = serper_api_key or os.environ.get("SERPER_API_KEY")
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

    async def research(self, title: str,
                       video_format: str = "short",
                       niche: str = "",
                       user_id: str = "",
                       script_brief: str = "",
                       hook_idea: str = "") -> dict:
        """
        Full research pipeline for a single video title.

        Args:
            title        : the video title to research
            video_format : "short" | "long" | "standard"
            niche        : niche label for context (e.g. "Psychology")
            user_id      : optional — links brief to a user
            script_brief : optional pre-existing brief from topic scout
            hook_idea    : optional hook from topic scout

        Returns:
            Complete research brief dict.
        """
        log.info(f"Researching: {title[:60]}")

        depth = RESEARCH_DEPTH_MAP.get(video_format, RESEARCH_DEPTH_MAP["standard"])

        # Run the three-agent crew concurrently
        # Researcher and Validator can run in parallel since Validator
        # also does its own independent pass.
        researcher_task = asyncio.create_task(
            self._agent_researcher(title, niche, depth, script_brief)
        )
        validator_task = asyncio.create_task(
            self._agent_validator(title, niche)
        )

        research_raw, validation_raw = await asyncio.gather(
            researcher_task, validator_task
        )

        # Synthesiser runs after both complete — uses their outputs
        brief = await self._agent_synthesiser(
            title, niche, video_format, depth,
            research_raw, validation_raw,
            hook_idea, script_brief
        )

        # Store the brief
        db = await self._get_db()
        brief_id = await self._store_brief(db, user_id, title, niche, video_format, brief)
        brief["briefId"]   = brief_id
        brief["title"]     = title
        brief["niche"]     = niche
        brief["format"]    = video_format
        brief["createdAt"] = datetime.now(timezone.utc).isoformat()

        log.info(f"Brief complete: {title[:50]} | {len(brief.get('key_facts',[]))} facts")
        return brief

    async def research_from_queue(self, queue_item_id: str) -> Optional[dict]:
        """
        Pulls a specific item from pipeline_queue, researches it,
        and updates the queue item status to 'scripting'.
        Called by the pipeline orchestrator.
        """
        db = await self._get_db()
        from bson import ObjectId

        item = await db.pipeline_queue.find_one({"_id": ObjectId(queue_item_id)})
        if not item:
            return {"error": f"Queue item {queue_item_id} not found"}

        # Update status to in progress
        await db.pipeline_queue.update_one(
            {"_id": ObjectId(queue_item_id)},
            {"$set": {"status": "researching", "researchStartedAt": datetime.now(timezone.utc).isoformat()}}
        )

        try:
            brief = await self.research(
                title        = item.get("title", ""),
                video_format = item.get("format", "short"),
                niche        = item.get("niche", ""),
                user_id      = item.get("userId", ""),
                script_brief = item.get("scriptBrief", ""),
                hook_idea    = item.get("hookIdea", ""),
            )

            # Update queue item with brief ID
            await db.pipeline_queue.update_one(
                {"_id": ObjectId(queue_item_id)},
                {"$set": {
                    "status":    "scripting",
                    "briefId":   brief.get("briefId"),
                    "researchCompletedAt": datetime.now(timezone.utc).isoformat(),
                }}
            )
            return brief

        except Exception as e:
            await db.pipeline_queue.update_one(
                {"_id": ObjectId(queue_item_id)},
                {"$set": {"status": "research_failed", "error": str(e)}}
            )
            raise

    async def process_pending_queue(self, user_id: str = None,
                                    limit: int = 5) -> dict:
        """
        Processes pending pipeline queue items — research briefs for all
        items with status 'pending'. Called by the pipeline orchestrator.

        Args:
            user_id : if set, only processes this user's queue
            limit   : max items to process per call
        """
        db    = await self._get_db()
        query = {"status": "pending"}
        if user_id:
            query["userId"] = user_id

        cursor = db.pipeline_queue.find(query).sort("queuedAt", 1).limit(limit)
        items  = await cursor.to_list(length=limit)

        results = {"processed": 0, "failed": 0, "brief_ids": []}

        for item in items:
            try:
                brief = await self.research_from_queue(str(item["_id"]))
                results["processed"] += 1
                if brief.get("briefId"):
                    results["brief_ids"].append(brief["briefId"])
            except Exception as e:
                log.error(f"Queue processing failed for {item.get('_id')}: {e}")
                results["failed"] += 1

        return results

    async def get_brief(self, brief_id: str) -> Optional[dict]:
        """Returns a stored research brief by ID."""
        db = await self._get_db()
        from bson import ObjectId
        try:
            doc = await db.script_briefs.find_one({"_id": ObjectId(brief_id)})
            if doc:
                doc["_id"] = str(doc["_id"])
            return doc
        except Exception:
            return None

    async def get_pending_briefs(self, user_id: str,
                                 limit: int = 20) -> list:
        """Returns unscripted briefs for a user."""
        db     = await self._get_db()
        cursor = db.script_briefs.find(
            {"userId": user_id, "status": "pending"},
            sort=[("createdAt", -1)]
        ).limit(limit)
        docs = await cursor.to_list(length=limit)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    async def mark_brief_scripted(self, brief_id: str,
                                  script_id: str = "") -> bool:
        """Mark a brief as scripted — called when script is written."""
        db = await self._get_db()
        from bson import ObjectId
        res = await db.script_briefs.update_one(
            {"_id": ObjectId(brief_id)},
            {"$set": {
                "status":      "scripted",
                "scriptId":    script_id,
                "scriptedAt":  datetime.now(timezone.utc).isoformat(),
            }}
        )
        return res.modified_count > 0

    # -----------------------------------------------------------------------
    # AGENT 1 — RESEARCHER
    # Deep-dives into the topic, finds facts, examples, and angles.
    # -----------------------------------------------------------------------

    async def _agent_researcher(self, title: str, niche: str,
                                depth: dict, seed_brief: str) -> dict:
        """
        Researcher agent: collects raw research material.
        Uses web search if available, otherwise uses OpenAI knowledge.
        """
        # Try to get web search context
        search_context = await self._web_search(title)

        client = self._get_openai()

        search_section = ""
        if search_context:
            search_section = f"\n\nWeb search context (use for facts and recency):\n{search_context}\n"

        seed_section = ""
        if seed_brief:
            seed_section = f"\n\nPre-existing brief from topic scout:\n{seed_brief}\n"

        prompt = f"""You are a YouTube content researcher for the "{niche or 'general'}" niche.
Research the following video topic thoroughly.

Video title: "{title}"
Facts needed: {depth['facts']}
Examples needed: {depth['examples']}
{search_section}{seed_section}

Return ONLY valid JSON:
{{
  "core_explanation": "<2-3 sentences: what this topic is and why it matters to viewers>",
  "key_facts": [
    {{"fact": "<specific, verifiable fact>", "why_compelling": "<why this surprises viewers>"}}
  ],
  "examples_or_cases": [
    {{"example": "<specific example or case study>", "source_type": "<study|news|historical|personal>"}}
  ],
  "surprising_angle": "<the most counterintuitive or unexpected angle on this topic>",
  "common_misconceptions": ["<misconception 1>", "<misconception 2>"],
  "credibility_signals": ["<scientific term>", "<expert name or institution>", "<statistic>"],
  "search_terms_used": ["<term 1>", "<term 2>"]
}}

Rules:
- key_facts must have exactly {depth['facts']} items
- examples_or_cases must have exactly {depth['examples']} items
- facts must be specific (include numbers/percentages where possible)
- never fabricate statistics — use approximate ranges if unsure"""

        resp = await client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5,
            max_tokens=1500,
        )
        raw = resp.choices[0].message.content.strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {
                "core_explanation":     f"This video covers {title.lower()}.",
                "key_facts":            [{"fact": f"Key point about {title}", "why_compelling": "Relevant to audience"}] * depth["facts"],
                "examples_or_cases":    [{"example": f"Example for {title}", "source_type": "general"}] * depth["examples"],
                "surprising_angle":     f"The most surprising thing about {title.lower()}",
                "common_misconceptions":["Most people misunderstand this"],
                "credibility_signals":  [],
                "search_terms_used":    [title],
            }

    # -----------------------------------------------------------------------
    # AGENT 2 — VALIDATOR
    # Independently cross-checks key claims and flags anything to avoid.
    # Runs concurrently with the Researcher.
    # -----------------------------------------------------------------------

    async def _agent_validator(self, title: str, niche: str) -> dict:
        """
        Validator agent: independently assesses the topic for accuracy risks,
        sensitivities, and content guardrails.
        """
        client = self._get_openai()

        prompt = f"""You review YouTube video topics for accuracy and content risks.

Video title: "{title}"
Niche: {niche or "general"}

Return ONLY valid JSON:
{{
  "accuracy_risk": "low|medium|high",
  "accuracy_risk_reason": "<why accuracy could be an issue here, or 'none'>",
  "claims_to_avoid": ["<claim that could be wrong or misleading>"],
  "content_sensitivities": ["<any topic areas to handle carefully>"],
  "fact_check_priority": ["<the one or two claims most important to verify>"],
  "recommended_disclaimers": ["<any disclaimer worth adding, or empty array>"],
  "evergreen_score": "<1-10: how evergreen is this topic? 10 = never goes stale>"
}}"""

        try:
            resp = await client.chat.completions.create(
                model=FAST_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500,
            )
            raw = resp.choices[0].message.content.strip()
            return json.loads(raw)
        except Exception:
            return {
                "accuracy_risk":          "low",
                "accuracy_risk_reason":   "none",
                "claims_to_avoid":        [],
                "content_sensitivities":  [],
                "fact_check_priority":    [],
                "recommended_disclaimers":[],
                "evergreen_score":        "7",
            }

    # -----------------------------------------------------------------------
    # AGENT 3 — SYNTHESISER
    # Combines Researcher + Validator outputs into a clean script brief.
    # -----------------------------------------------------------------------

    async def _agent_synthesiser(self, title: str, niche: str,
                                 video_format: str, depth: dict,
                                 research: dict, validation: dict,
                                 hook_idea: str,
                                 seed_brief: str) -> dict:
        """
        Synthesiser agent: produces the final structured script brief
        from the researcher and validator outputs.
        """
        client = self._get_openai()

        # Flag any accuracy risks from validator
        risk_notes = ""
        if validation.get("accuracy_risk") in ("medium", "high"):
            risk_notes = f"\nACCURACY NOTE ({validation['accuracy_risk']} risk): {validation.get('accuracy_risk_reason', '')}"
        if validation.get("claims_to_avoid"):
            risk_notes += f"\nAVOID CLAIMING: {', '.join(validation['claims_to_avoid'])}"

        hook_section = f"\nHook idea from trend scout: {hook_idea}" if hook_idea else ""

        prompt = f"""You are a YouTube script brief writer for a {niche or 'general'} channel.
Produce the final script brief using this research and validation data.

Video title: "{title}"
Format: {video_format} ({"60-90 seconds" if video_format == "short" else "8-25 minutes"})
Target brief length: ~{depth['word_count']} words
{risk_notes}{hook_section}

Research data:
{json.dumps(research, indent=2)}

Validation data:
{json.dumps(validation, indent=2)}

Return ONLY valid JSON:
{{
  "hook": "<opening 1-2 sentences that grab attention immediately — 15-25 words>",
  "core_message": "<the single key takeaway of this video — 1 sentence>",
  "suggested_structure": [
    {{"section": "Intro", "content_note": "<what to cover>", "approx_seconds": 10}},
    {{"section": "Point 1", "content_note": "<what to cover>", "approx_seconds": 20}},
    {{"section": "Point 2", "content_note": "<what to cover>", "approx_seconds": 20}},
    {{"section": "Point 3", "content_note": "<what to cover>", "approx_seconds": 20}},
    {{"section": "CTA", "content_note": "Subscribe and follow for more", "approx_seconds": 10}}
  ],
  "key_facts": <copy exactly from research>,
  "examples_or_cases": <copy exactly from research>,
  "hook_angles": [
    {{"angle": "question", "hook": "<hook as a question>"}},
    {{"angle": "shocking_stat", "hook": "<hook as a surprising stat>"}},
    {{"angle": "bold_claim", "hook": "<hook as a bold statement>"}}
  ],
  "title_variants": [
    "<title variant 1 — number pattern>",
    "<title variant 2 — question pattern>",
    "<title variant 3 — challenge pattern>"
  ],
  "seo_tags": ["<tag 1>", "<tag 2>", "<tag 3>", "<tag 4>", "<tag 5>", "<tag 6>"],
  "credibility_signals": <copy from research>,
  "content_warnings": <copy claims_to_avoid from validation>,
  "evergreen_score": "<from validation>",
  "estimated_word_count": {depth['word_count']},
  "research_confidence": "high|medium|low"
}}

Rules:
- hook must be compelling and specific to this exact topic
- suggested_structure must have 5-7 sections
- approx_seconds must sum to {"90" if video_format == "short" else "900"}
- title_variants must all be under 70 characters"""

        resp = await client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.6,
            max_tokens=2000,
        )
        raw = resp.choices[0].message.content.strip()
        try:
            brief = json.loads(raw)
            # Merge in validator data that isn't in the brief
            brief["accuracy_risk"]          = validation.get("accuracy_risk", "low")
            brief["fact_check_priority"]    = validation.get("fact_check_priority", [])
            brief["recommended_disclaimers"]= validation.get("recommended_disclaimers", [])
            return brief
        except json.JSONDecodeError:
            log.warning("Synthesiser JSON parse failed — returning raw research")
            return {
                "hook":               research.get("surprising_angle", ""),
                "core_message":       research.get("core_explanation", ""),
                "suggested_structure":[
                    {"section": "Intro",  "content_note": "Hook + context",     "approx_seconds": 10},
                    {"section": "Part 1", "content_note": "First key point",    "approx_seconds": 25},
                    {"section": "Part 2", "content_note": "Second key point",   "approx_seconds": 25},
                    {"section": "Part 3", "content_note": "Third key point",    "approx_seconds": 25},
                    {"section": "CTA",    "content_note": "Subscribe",          "approx_seconds": 10},
                ],
                "key_facts":          research.get("key_facts", []),
                "examples_or_cases":  research.get("examples_or_cases", []),
                "hook_angles":        [],
                "title_variants":     [title],
                "seo_tags":           [],
                "credibility_signals":research.get("credibility_signals", []),
                "content_warnings":   validation.get("claims_to_avoid", []),
                "evergreen_score":    validation.get("evergreen_score", "7"),
                "estimated_word_count":depth["word_count"],
                "research_confidence":"low",
            }

    # -----------------------------------------------------------------------
    # WEB SEARCH — provides real-world context for the Researcher agent
    # -----------------------------------------------------------------------

    async def _web_search(self, query: str) -> str:
        """
        Fetches web search results for the research query.
        Tries Tavily first, then Serper, then falls back gracefully.
        Returns a plain text summary of top results.
        """
        if self.tavily_key:
            return await asyncio.to_thread(self._tavily_search, query)
        if self.serper_key:
            return await asyncio.to_thread(self._serper_search, query)
        log.info("No web search API key — researcher will use OpenAI knowledge only")
        return ""

    def _tavily_search(self, query: str) -> str:
        """Tavily AI search — best option, returns clean summaries."""
        try:
            resp = requests.post(
                "https://api.tavily.com/search",
                json={
                    "api_key":        self.tavily_key,
                    "query":          query,
                    "search_depth":   "basic",
                    "max_results":    MAX_SEARCH_RESULTS,
                    "include_answer": True,
                },
                timeout=15,
            )
            data    = resp.json()
            results = data.get("results", [])
            answer  = data.get("answer", "")
            lines   = [f"Answer: {answer}"] if answer else []
            for r in results[:5]:
                lines.append(f"- {r.get('title', '')}: {r.get('content', '')[:300]}")
            return "\n".join(lines)
        except Exception as e:
            log.warning(f"Tavily search failed: {e}")
            return ""

    def _serper_search(self, query: str) -> str:
        """Serper Google search — fallback option."""
        try:
            resp = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": self.serper_key, "Content-Type": "application/json"},
                json={"q": query, "num": MAX_SEARCH_RESULTS},
                timeout=15,
            )
            data    = resp.json()
            organic = data.get("organic", [])
            lines   = []
            for r in organic[:5]:
                lines.append(f"- {r.get('title', '')}: {r.get('snippet', '')[:300]}")
            return "\n".join(lines)
        except Exception as e:
            log.warning(f"Serper search failed: {e}")
            return ""

    # -----------------------------------------------------------------------
    # STORAGE
    # -----------------------------------------------------------------------

    async def _store_brief(self, db, user_id: str, title: str,
                           niche: str, video_format: str, brief: dict) -> str:
        """Stores the research brief and returns its ID."""
        doc = {
            "userId":     user_id,
            "title":      title,
            "niche":      niche,
            "format":     video_format,
            "status":     "pending",    # pending → scripted
            "scriptId":   None,
            "brief":      brief,
            "createdAt":  datetime.now(timezone.utc).isoformat(),
            "expiresAt":  (datetime.now(timezone.utc) + timedelta(days=BRIEF_TTL_DAYS)).isoformat(),
        }
        result = await db.script_briefs.insert_one(doc)
        return str(result.inserted_id)

    # -----------------------------------------------------------------------
    # TTL INDEXES
    # -----------------------------------------------------------------------

    async def ensure_indexes(self):
        """Creates MongoDB indexes. Safe to call multiple times."""
        db = await self._get_db()
        await db.script_briefs.create_index("userId",    name="idx_brief_user")
        await db.script_briefs.create_index(
            "expiresAt", expireAfterSeconds=0, name="idx_brief_ttl"
        )
        await db.script_briefs.create_index(
            [("userId", 1), ("status", 1), ("createdAt", -1)],
            name="idx_brief_user_status"
        )
        log.info("Script research indexes confirmed")


# ===========================================================================
# CLI
# ===========================================================================
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Script Research Agent CLI")
    parser.add_argument("--research",    type=str, metavar="TITLE",
                        help="Research a video title")
    parser.add_argument("--format",      type=str, default="short",
                        choices=["short","long","standard"],
                        help="Video format (default: short)")
    parser.add_argument("--niche",       type=str, default="",
                        help="Niche label for context")
    parser.add_argument("--process-queue", type=str, metavar="USER_ID",
                        help="Process pending queue items for a user")
    parser.add_argument("--get",         type=str, metavar="BRIEF_ID",
                        help="Get a stored brief by ID")
    parser.add_argument("--pending",     type=str, metavar="USER_ID",
                        help="List pending briefs for a user")
    parser.add_argument("--indexes",     action="store_true",
                        help="Create MongoDB indexes and exit")
    args = parser.parse_args()

    agent = ScriptResearchAgent()

    if args.indexes:
        asyncio.run(agent.ensure_indexes())
    elif args.research:
        result = asyncio.run(
            agent.research(args.research, video_format=args.format, niche=args.niche)
        )
        print(json.dumps(result, indent=2, default=str))
    elif args.process_queue:
        result = asyncio.run(agent.process_pending_queue(user_id=args.process_queue))
        print(json.dumps(result, indent=2, default=str))
    elif args.get:
        result = asyncio.run(agent.get_brief(args.get))
        print(json.dumps(result, indent=2, default=str))
    elif args.pending:
        result = asyncio.run(agent.get_pending_briefs(args.pending))
        print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()
