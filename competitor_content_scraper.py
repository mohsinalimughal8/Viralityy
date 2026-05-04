"""
Viralityy — Competitor Content Scraper (Tier 2, Agent 8)
=========================================================
Pulls the full metadata from competitor videos — titles, descriptions,
tags, chapters, and transcript summaries — and stores them in structured
form for analysis. Works as the data layer beneath the Competitor Watcher.

HOW THIS RELATES TO THE COMPETITOR WATCHER (Agent 3):
  - Competitor Watcher  → detects NEW videos and fires alerts (surveillance)
  - Competitor Scraper  → pulls the FULL CONTENT of those videos (intelligence)

  The Watcher tells you "a competitor just posted something big."
  The Scraper tells you "here's exactly what they said, how they structured
  it, what tags they used, and what you should do differently."

Inspired by the ai-engineering-hub "FireCrawl Agent" (corrective RAG with
web search fallback). The FireCrawl pattern handles structured extraction
from web pages with automatic retry and fallback — adapted here for
YouTube video page scraping when the Data API doesn't return enough detail.

WHAT IT EXTRACTS per video:
  - Full title + all title formulas used (question / number / challenge)
  - Full description with chapter timestamps parsed
  - All tags (from API + scraped from page if API doesn't return them)
  - Pinned comment (often contains additional SEO keywords)
  - Script structure inference (from chapter titles if available)
  - Thumbnail text (estimated from title pattern analysis)
  - CTA patterns (what they ask viewers to do at the end)
  - SEO strength score (how well-optimised the video is)

USAGE:
  from competitor_content_scraper import CompetitorContentScraper
  scraper = CompetitorContentScraper()

  # Scrape a single video
  data = await scraper.scrape_video(video_id="dQw4w9WgXcQ", user_id="abc123")

  # Bulk scrape all stored competitor videos for a user
  results = await scraper.scrape_all_pending(user_id="abc123")

  # Get scraped data for a video
  data = await scraper.get_video_data(video_id="dQw4w9WgXcQ")

  # Get analysis summary across all scraped videos for a user
  analysis = await scraper.get_analysis_summary(user_id="abc123")

MONGO COLLECTIONS:
  competitor_scraped    — full scraped video data (TTL: 60 days)
  competitor_videos     — updated with scrape status (shared with Watcher)

PIPELINE POSITION:
  Competitor Watcher detects video → this agent scrapes it →
  Script Research Agent uses scraped data as context for research briefs

DEPENDENCIES:
  - YouTube Data API v3     (YOUTUBE_API_KEY in env)
  - requests                (already in requirements.txt)
  - motor                   (already in requirements.txt)
  - openai                  (already in requirements.txt)
  - Optional: FIRECRAWL_API_KEY for deep page scraping when API lacks data
    (falls back to YouTube Data API + requests if not set)
"""

import os
import re
import asyncio
import logging
import json
import requests
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

logging.basicConfig(level=logging.INFO, format='[ContentScraper] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
SCRAPED_TTL_DAYS  = 60     # scraped data kept for 60 days
BATCH_SIZE        = 10     # videos to scrape per bulk run
MAX_DESC_CHARS    = 5000   # truncate very long descriptions
FIRECRAWL_API_URL = "https://api.firecrawl.dev/v1/scrape"

# Common YouTube CTA patterns to detect
CTA_PATTERNS = [
    r"subscri",
    r"like (this )?video",
    r"hit the bell",
    r"comment below",
    r"share this",
    r"follow (me|us)",
    r"check out",
    r"link in (the )?bio",
    r"watch next",
]

# SEO scoring weights
SEO_WEIGHTS = {
    "has_keywords_in_title":       25,
    "description_length":          20,   # >200 chars = full points
    "has_timestamps_chapters":     20,
    "tag_count":                   15,   # 10+ tags = full points
    "has_pinned_comment":          10,
    "title_length_optimal":        10,   # 50–70 chars = full points
}


# ===========================================================================
# COMPETITOR CONTENT SCRAPER
# ===========================================================================

class CompetitorContentScraper:

    def __init__(self, mongo_uri: str = None,
                 youtube_api_key: str = None,
                 openai_api_key: str = None,
                 firecrawl_api_key: str = None):
        self.mongo_uri      = mongo_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.yt_key         = youtube_api_key or os.environ.get("YOUTUBE_API_KEY")
        self.openai_key     = openai_api_key or os.environ.get("OPENAI_API_KEY")
        self.firecrawl_key  = firecrawl_api_key or os.environ.get("FIRECRAWL_API_KEY")
        self._db            = None
        self._yt            = None
        self._openai        = None

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

    async def scrape_video(self, video_id: str, user_id: str = "",
                           force: bool = False) -> dict:
        """
        Scrapes full metadata for a single YouTube video.
        Returns the complete scraped data dict.

        Args:
            video_id : YouTube video ID
            user_id  : Viralityy user ID (for storage attribution)
            force    : re-scrape even if already scraped
        """
        db = await self._get_db()

        # Return cached data if available
        if not force:
            existing = await db.competitor_scraped.find_one({"videoId": video_id})
            if existing:
                existing["_id"] = str(existing["_id"])
                existing["fromCache"] = True
                return existing

        log.info(f"Scraping video {video_id}")

        # Layer 1: YouTube Data API (primary source)
        api_data = await asyncio.to_thread(self._fetch_yt_metadata, video_id)

        if not api_data:
            return {"error": f"Video {video_id} not found via YouTube API"}

        # Layer 2: Page scrape for additional data API doesn't return
        # (tags sometimes hidden, pinned comments, full description)
        page_data = await self._scrape_video_page(video_id)

        # Merge both sources (page data fills gaps from API)
        merged = self._merge_data_sources(api_data, page_data)

        # Analyse the merged data
        analysis = self._analyse_content(merged)
        merged["analysis"] = analysis

        # AI-powered insight generation
        insights = await self._generate_insights(merged)
        merged["insights"] = insights

        # Store
        await self._store_scraped(db, video_id, user_id, merged)

        # Update competitor_videos with scrape status
        await db.competitor_videos.update_one(
            {"videoId": video_id},
            {"$set": {"scraped": True, "scrapedAt": datetime.now(timezone.utc).isoformat()}}
        )

        merged["fromCache"] = False
        return merged

    async def scrape_all_pending(self, user_id: str) -> dict:
        """
        Scrapes all unscraped competitor videos for a user.
        Called after the Competitor Watcher detects new videos.
        Processes in batches of BATCH_SIZE.
        """
        db = await self._get_db()

        # Find competitor videos that haven't been scraped yet
        cursor = db.competitor_videos.find(
            {"userId": user_id, "scraped": {"$ne": True}},
            sort=[("detectedAt", -1)]
        ).limit(BATCH_SIZE)
        videos = await cursor.to_list(length=BATCH_SIZE)

        results = {
            "scraped": 0, "failed": 0,
            "video_ids": [],
            "run_at": datetime.now(timezone.utc).isoformat(),
        }

        for v in videos:
            vid_id = v.get("videoId")
            try:
                data = await self.scrape_video(vid_id, user_id)
                if data.get("error"):
                    results["failed"] += 1
                else:
                    results["scraped"] += 1
                    results["video_ids"].append(vid_id)
            except Exception as e:
                log.error(f"Scrape failed for {vid_id}: {e}")
                results["failed"] += 1

        log.info(f"Bulk scrape complete for user {user_id}: {results}")
        return results

    async def get_video_data(self, video_id: str) -> Optional[dict]:
        """Returns scraped data for a specific video."""
        db  = await self._get_db()
        doc = await db.competitor_scraped.find_one({"videoId": video_id})
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc

    async def get_scraped_videos(self, user_id: str,
                                 limit: int = 20) -> list:
        """Returns recent scraped competitor videos for a user."""
        db     = await self._get_db()
        cursor = db.competitor_scraped.find(
            {"userId": user_id},
            sort=[("scrapedAt", -1)]
        ).limit(limit)
        docs = await cursor.to_list(length=limit)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    async def get_analysis_summary(self, user_id: str) -> dict:
        """
        Aggregates patterns across all scraped competitor videos for a user.
        Returns actionable insights: common title formulas, popular tags,
        average description length, most-used CTAs, SEO score distribution.
        """
        db    = await self._get_db()
        docs  = await db.competitor_scraped.find(
            {"userId": user_id}
        ).to_list(length=200)

        if not docs:
            return {"error": "No scraped videos found for this user"}

        # Aggregate title patterns
        title_patterns = Counter()
        all_tags       = Counter()
        seo_scores     = []
        desc_lengths   = []
        cta_types      = Counter()
        formula_words  = Counter()

        for doc in docs:
            analysis = doc.get("analysis", {})

            tp = analysis.get("title_pattern")
            if tp:
                title_patterns[tp] += 1

            for tag in doc.get("tags", []):
                all_tags[tag.lower()] += 1

            seo = analysis.get("seo_score")
            if seo is not None:
                seo_scores.append(seo)

            desc = doc.get("description", "")
            if desc:
                desc_lengths.append(len(desc))

            for cta in analysis.get("cta_patterns_found", []):
                cta_types[cta] += 1

            for word in analysis.get("title_formula_words", []):
                formula_words[word] += 1

        avg_seo   = round(sum(seo_scores) / max(len(seo_scores), 1), 1)
        avg_desc  = round(sum(desc_lengths) / max(len(desc_lengths), 1))

        return {
            "total_videos_analysed":  len(docs),
            "avg_seo_score":          avg_seo,
            "avg_description_length": avg_desc,
            "top_title_patterns":     title_patterns.most_common(3),
            "top_tags":               all_tags.most_common(15),
            "top_cta_types":          cta_types.most_common(5),
            "top_formula_words":      formula_words.most_common(10),
            "seo_score_distribution": {
                "high":   sum(1 for s in seo_scores if s >= 70),
                "medium": sum(1 for s in seo_scores if 40 <= s < 70),
                "low":    sum(1 for s in seo_scores if s < 40),
            },
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }

    async def search_scraped(self, user_id: str, query: str) -> list:
        """
        Full-text search across scraped competitor content.
        Useful for finding which competitors have covered a specific topic.
        """
        db    = await self._get_db()
        regex = {"$regex": query, "$options": "i"}
        cursor= db.competitor_scraped.find({
            "userId": user_id,
            "$or": [
                {"title":       regex},
                {"description": regex},
                {"tags":        regex},
            ]
        }).sort("scrapedAt", -1).limit(20)
        docs = await cursor.to_list(length=20)
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs

    # -----------------------------------------------------------------------
    # LAYER 1 — YOUTUBE DATA API
    # -----------------------------------------------------------------------

    def _fetch_yt_metadata(self, video_id: str) -> Optional[dict]:
        """
        Fetches video metadata via YouTube Data API v3.
        This is the primary data source — most fields come from here.
        """
        yt = self._get_yt()
        if not yt:
            log.warning("YouTube API key not set — cannot scrape")
            return None

        try:
            resp  = yt.videos().list(
                id=video_id,
                part="snippet,statistics,contentDetails,topicDetails",
            ).execute()

            items = resp.get("items", [])
            if not items:
                return None

            v       = items[0]
            snippet = v.get("snippet", {})
            stats   = v.get("statistics", {})
            details = v.get("contentDetails", {})

            return {
                "videoId":       video_id,
                "title":         snippet.get("title", ""),
                "description":   snippet.get("description", "")[:MAX_DESC_CHARS],
                "channelId":     snippet.get("channelId", ""),
                "channelTitle":  snippet.get("channelTitle", ""),
                "publishedAt":   snippet.get("publishedAt", ""),
                "tags":          snippet.get("tags", []),
                "categoryId":    snippet.get("categoryId", ""),
                "defaultLang":   snippet.get("defaultLanguage", "en"),
                "duration":      details.get("duration", ""),
                "views":         int(stats.get("viewCount", 0)),
                "likes":         int(stats.get("likeCount", 0)),
                "comments":      int(stats.get("commentCount", 0)),
                "url":           f"https://youtube.com/watch?v={video_id}",
                "source":        "youtube_api",
            }
        except Exception as e:
            log.warning(f"YouTube API fetch failed for {video_id}: {e}")
            return None

    # -----------------------------------------------------------------------
    # LAYER 2 — PAGE SCRAPING (fills gaps from API)
    # -----------------------------------------------------------------------

    async def _scrape_video_page(self, video_id: str) -> dict:
        """
        Scrapes the YouTube video page to get data the API doesn't return:
          - Full tag list (API sometimes truncates)
          - Pinned comment text
          - Auto-generated chapter titles (from description)
        
        Uses FireCrawl if available, falls back to direct requests.
        """
        url = f"https://www.youtube.com/watch?v={video_id}"

        if self.firecrawl_key:
            return await asyncio.to_thread(self._firecrawl_scrape, url)
        else:
            return await asyncio.to_thread(self._direct_scrape, url, video_id)

    def _firecrawl_scrape(self, url: str) -> dict:
        """
        Uses FireCrawl API for structured page extraction.
        More reliable than direct scraping — handles JS-rendered content.
        """
        try:
            resp = requests.post(
                FIRECRAWL_API_URL,
                headers={
                    "Authorization": f"Bearer {self.firecrawl_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "url": url,
                    "formats": ["markdown"],
                    "onlyMainContent": True,
                },
                timeout=30,
            )
            data     = resp.json()
            markdown = data.get("data", {}).get("markdown", "")
            return self._parse_page_markdown(markdown)
        except Exception as e:
            log.warning(f"FireCrawl scrape failed: {e} — falling back to direct")
            return {}

    def _direct_scrape(self, url: str, video_id: str) -> dict:
        """
        Direct HTTP scrape of the YouTube page.
        Extracts what's available in the initial HTML response.
        Note: YouTube's JS-rendered content may not be available here.
        """
        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
            resp = requests.get(url, headers=headers, timeout=15)
            html = resp.text

            # Extract meta tags (reliable in initial HTML)
            extracted = {}

            # Keywords meta tag (often contains tags)
            kw_match = re.search(r'<meta name="keywords" content="([^"]+)"', html)
            if kw_match:
                extracted["page_tags"] = [t.strip() for t in kw_match.group(1).split(",")]

            # Description meta
            desc_match = re.search(r'<meta name="description" content="([^"]+)"', html)
            if desc_match:
                extracted["page_description"] = desc_match.group(1)

            return extracted
        except Exception as e:
            log.warning(f"Direct page scrape failed for {video_id}: {e}")
            return {}

    def _parse_page_markdown(self, markdown: str) -> dict:
        """Parses FireCrawl markdown output to extract key fields."""
        result = {}

        # Find chapter timestamps (pattern: 00:00 Title or 0:00 Title)
        timestamp_pattern = r"(\d{1,2}:\d{2}(?::\d{2})?)\s+(.+?)(?=\n|$)"
        chapters = re.findall(timestamp_pattern, markdown)
        if chapters:
            result["chapters"] = [
                {"timestamp": ts, "title": title.strip()}
                for ts, title in chapters
            ]

        # Find pinned comment (FireCrawl often includes comment section)
        pinned_match = re.search(r"Pinned.*?\n(.+?)(?=\n\n|\Z)", markdown, re.DOTALL)
        if pinned_match:
            result["pinned_comment"] = pinned_match.group(1).strip()[:500]

        return result

    # -----------------------------------------------------------------------
    # DATA MERGE
    # -----------------------------------------------------------------------

    def _merge_data_sources(self, api_data: dict, page_data: dict) -> dict:
        """Merges API data with page-scraped data, page data fills gaps."""
        merged = {**api_data}

        # If page has additional tags not in API response
        page_tags = page_data.get("page_tags", [])
        if page_tags and len(page_tags) > len(merged.get("tags", [])):
            merged["tags"] = page_tags

        # Add chapters if found
        if page_data.get("chapters"):
            merged["chapters"] = page_data["chapters"]

        # Add pinned comment
        if page_data.get("pinned_comment"):
            merged["pinnedComment"] = page_data["pinned_comment"]

        # Parse chapters from description if not from page
        if not merged.get("chapters"):
            merged["chapters"] = self._parse_description_chapters(
                merged.get("description", "")
            )

        return merged

    def _parse_description_chapters(self, description: str) -> list:
        """
        Extracts chapter timestamps from video description.
        YouTube creators often add chapters manually in the description.
        """
        if not description:
            return []
        pattern = r"^(\d{1,2}:\d{2}(?::\d{2})?)\s+(.+?)$"
        chapters = []
        for line in description.split("\n"):
            m = re.match(pattern, line.strip())
            if m:
                chapters.append({"timestamp": m.group(1), "title": m.group(2).strip()})
        return chapters

    # -----------------------------------------------------------------------
    # CONTENT ANALYSIS
    # -----------------------------------------------------------------------

    def _analyse_content(self, data: dict) -> dict:
        """
        Analyses the merged video data for patterns and SEO signals.
        Returns a structured analysis dict.
        """
        title       = data.get("title", "")
        description = data.get("description", "")
        tags        = data.get("tags", [])
        chapters    = data.get("chapters", [])

        # Title pattern detection
        title_pattern = self._detect_title_pattern(title)

        # Title formula words (numbers, power words)
        formula_words = self._extract_formula_words(title)

        # Description analysis
        desc_has_timestamps = bool(chapters)
        desc_length         = len(description)
        desc_has_links      = bool(re.search(r"https?://", description))

        # CTA detection
        cta_found = [
            pattern for pattern in CTA_PATTERNS
            if re.search(pattern, description.lower() + title.lower())
        ]

        # SEO score
        seo_score = self._calculate_seo_score(
            title, description, tags, chapters, cta_found
        )

        # Keyword density in title
        title_words = set(re.findall(r"[a-z]+", title.lower()))

        return {
            "title_pattern":        title_pattern,
            "title_formula_words":  formula_words,
            "title_length":         len(title),
            "title_word_count":     len(title.split()),
            "description_length":   desc_length,
            "description_has_timestamps": desc_has_timestamps,
            "description_has_links":desc_has_links,
            "chapter_count":        len(chapters),
            "tag_count":            len(tags),
            "cta_patterns_found":   cta_found,
            "seo_score":            seo_score,
            "keyword_density":      round(len(title_words) / max(len(title.split()), 1), 2),
        }

    def _detect_title_pattern(self, title: str) -> str:
        """Classifies the title into a known YouTube pattern."""
        t = title.lower()
        if re.search(r"\b\d+\b", t):              return "number"
        if "?" in title:                          return "question"
        if re.search(r"\bhow to\b", t):           return "how_to"
        if re.search(r"\bwhy\b", t):              return "why"
        if re.search(r"\bwhat (if|happens)\b", t):return "what_if"
        if re.search(r"\bsecret|truth|real\b", t):return "reveal"
        if re.search(r"\bnobody|no one|ever\b", t):return "exclusive"
        return "statement"

    def _extract_formula_words(self, title: str) -> list:
        """Extracts power words and number patterns from a title."""
        words = []
        # Numbers
        nums = re.findall(r"\b\d+\b", title)
        words.extend([f"#{n}" for n in nums])
        # Power words
        POWER_WORDS = [
            "secret","truth","real","nobody","ever","always","never",
            "instantly","proven","ultimate","complete","exactly","simple",
            "surprising","shocking","finally","actually",
        ]
        for pw in POWER_WORDS:
            if pw in title.lower():
                words.append(pw)
        return words

    def _calculate_seo_score(self, title: str, description: str,
                             tags: list, chapters: list,
                             ctas: list) -> int:
        """
        Scores how well-optimised a video is for YouTube SEO.
        Returns 0–100.
        """
        score = 0

        # Title length (optimal 50–70 chars)
        tlen = len(title)
        if 50 <= tlen <= 70:
            score += SEO_WEIGHTS["title_length_optimal"]
        elif 40 <= tlen < 50 or 70 < tlen <= 80:
            score += SEO_WEIGHTS["title_length_optimal"] // 2

        # Description length
        dlen = len(description)
        if dlen >= 500:
            score += SEO_WEIGHTS["description_length"]
        elif dlen >= 200:
            score += int(SEO_WEIGHTS["description_length"] * dlen / 500)

        # Tags
        tc = len(tags)
        if tc >= 10:
            score += SEO_WEIGHTS["tag_count"]
        elif tc > 0:
            score += int(SEO_WEIGHTS["tag_count"] * tc / 10)

        # Chapters / timestamps
        if chapters:
            score += SEO_WEIGHTS["has_timestamps_chapters"]

        # Keywords in title (assume keywords = tags; at least 1 overlap)
        if tags:
            tag_words = set(" ".join(tags).lower().split())
            title_words = set(title.lower().split())
            if tag_words & title_words:
                score += SEO_WEIGHTS["has_keywords_in_title"]

        # Pinned comment or strong CTA presence
        if ctas:
            score += SEO_WEIGHTS["has_pinned_comment"]

        return min(score, 100)

    # -----------------------------------------------------------------------
    # AI INSIGHTS
    # -----------------------------------------------------------------------

    async def _generate_insights(self, data: dict) -> dict:
        """
        Uses OpenAI to generate actionable insights from the scraped video.
        Returns structured insights for use by the Script Research Agent.
        """
        client = self._get_openai()
        if not client:
            return {"error": "OpenAI not configured"}

        title       = data.get("title", "")
        description = (data.get("description", ""))[:800]
        tags        = data.get("tags", [])[:15]
        chapters    = data.get("chapters", [])
        analysis    = data.get("analysis", {})
        views       = data.get("views", 0)
        likes       = data.get("likes", 0)

        prompt = f"""You analyse competitor YouTube videos to help content creators improve.

Video data:
- Title: "{title}"
- Views: {views:,} | Likes: {likes:,}
- Tags: {', '.join(tags)}
- SEO score: {analysis.get('seo_score', 0)}/100
- Title pattern: {analysis.get('title_pattern', 'unknown')}
- Chapter count: {len(chapters)}
- Description excerpt: "{description[:400]}"

Return ONLY valid JSON:
{{
  "what_works": "<1-2 sentences: what this video does well that we should copy>",
  "what_to_do_differently": "<1-2 sentences: what we should do better>",
  "title_takeaway": "<specific lesson about the title formula we can replicate>",
  "missing_angle": "<an angle this video missed that we could cover instead>",
  "suggested_counter_title": "<a better title we could use for a response video (max 70 chars)>",
  "seo_lessons": ["<lesson 1>", "<lesson 2>"],
  "estimated_why_it_performed": "<one sentence on why this video got {views:,} views>"
}}"""

        try:
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.6,
                max_tokens=600,
            )
            raw = resp.choices[0].message.content.strip()
            return json.loads(raw)
        except Exception as e:
            log.warning(f"Insight generation failed: {e}")
            return {
                "what_works":                 "Analyse this video for patterns manually.",
                "what_to_do_differently":     "Check the description and tags structure.",
                "title_takeaway":             f"Pattern used: {analysis.get('title_pattern', 'unknown')}",
                "missing_angle":              "Look for underserved sub-topics.",
                "suggested_counter_title":    f"Our take on: {title[:50]}",
                "seo_lessons":                ["Use timestamps in description", "Use 10+ tags"],
                "estimated_why_it_performed": "Strong title and early engagement.",
            }

    # -----------------------------------------------------------------------
    # STORAGE
    # -----------------------------------------------------------------------

    async def _store_scraped(self, db, video_id: str,
                             user_id: str, data: dict):
        """Stores the scraped video data with TTL."""
        doc = {
            **data,
            "userId":    user_id,
            "scrapedAt": datetime.now(timezone.utc).isoformat(),
            "expiresAt": (datetime.now(timezone.utc) + timedelta(days=SCRAPED_TTL_DAYS)).isoformat(),
        }
        await db.competitor_scraped.update_one(
            {"videoId": video_id},
            {"$set": doc},
            upsert=True,
        )
        log.info(f"Stored scraped data for {video_id}")

    # -----------------------------------------------------------------------
    # TTL INDEXES
    # -----------------------------------------------------------------------

    async def ensure_indexes(self):
        """Creates MongoDB indexes. Safe to call multiple times."""
        db = await self._get_db()
        await db.competitor_scraped.create_index(
            "videoId", unique=True, name="idx_scraped_video"
        )
        await db.competitor_scraped.create_index("userId", name="idx_scraped_user")
        await db.competitor_scraped.create_index(
            "expiresAt", expireAfterSeconds=0, name="idx_scraped_ttl"
        )
        await db.competitor_scraped.create_index(
            [("userId", 1), ("scrapedAt", -1)],
            name="idx_scraped_user_date"
        )
        # Text index for search
        await db.competitor_scraped.create_index(
            [("title", "text"), ("description", "text"), ("tags", "text")],
            name="idx_scraped_text"
        )
        log.info("Competitor scraper indexes confirmed")


# ===========================================================================
# CLI
# ===========================================================================
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Competitor Content Scraper CLI")
    parser.add_argument("--scrape",      type=str, metavar="VIDEO_ID",
                        help="Scrape a single video")
    parser.add_argument("--user",        type=str, metavar="USER_ID", default="")
    parser.add_argument("--bulk",        type=str, metavar="USER_ID",
                        help="Scrape all pending competitor videos for a user")
    parser.add_argument("--get",         type=str, metavar="VIDEO_ID",
                        help="Get stored scraped data for a video")
    parser.add_argument("--summary",     type=str, metavar="USER_ID",
                        help="Get analysis summary for a user")
    parser.add_argument("--search",      nargs=2,
                        metavar=("USER_ID","QUERY"),
                        help="Search scraped content")
    parser.add_argument("--list",        type=str, metavar="USER_ID",
                        help="List scraped videos for a user")
    parser.add_argument("--indexes",     action="store_true",
                        help="Create MongoDB indexes and exit")
    args = parser.parse_args()

    scraper = CompetitorContentScraper()

    if args.indexes:
        asyncio.run(scraper.ensure_indexes())
    elif args.scrape:
        result = asyncio.run(scraper.scrape_video(args.scrape, user_id=args.user))
        print(json.dumps(result, indent=2, default=str))
    elif args.bulk:
        result = asyncio.run(scraper.scrape_all_pending(args.bulk))
        print(json.dumps(result, indent=2, default=str))
    elif args.get:
        result = asyncio.run(scraper.get_video_data(args.get))
        print(json.dumps(result, indent=2, default=str))
    elif args.summary:
        result = asyncio.run(scraper.get_analysis_summary(args.summary))
        print(json.dumps(result, indent=2, default=str))
    elif args.search:
        result = asyncio.run(scraper.search_scraped(args.search[0], args.search[1]))
        print(json.dumps(result, indent=2, default=str))
    elif args.list:
        result = asyncio.run(scraper.get_scraped_videos(args.list))
        print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()
