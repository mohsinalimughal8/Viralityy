# Viralityy — Project Context File
**Paste this file at the start of every Claude session to resume instantly.**

---

## What is Viralityy?
An automated YouTube channel growth SaaS. Users connect their YouTube channel, pick a niche, and Viralityy's AI creates, quality-checks, and posts videos every day — Shorts and long-form. No filming, no editing required.

---

## Tech Stack
- **Backend:** Node.js + Express, MongoDB (Mongoose), Python engines
- **Frontend:** Vanilla HTML/CSS/JS (no framework)
- **Payments:** Stripe
- **AI:** OpenAI (scripts), ElevenLabs (voiceover), Pexels (footage)
- **Hosting:** Railway (backend), Netlify (frontend)
- **Payouts:** Payoneer (Pakistan — Stripe Connect unavailable)

---

## All Files Built

### Frontend (HTML)
| File | Description |
|---|---|
| `index.html` | Landing page — hero, features, pricing preview, FAQ, testimonials |
| `pricing.html` | 4-plan pricing page with monthly/annual toggle |
| `signup.html` | 5-step onboarding: Account → Plan → Niche → Channel → Launch |
| `login.html` | Email + Google OAuth login |
| `reset.html` | Password reset |
| `privacy.html` | Full privacy policy (GDPR + YouTube API compliant) |
| `terms.html` | Terms of service |
| `cookies.html` | Cookie policy with live toggles |
| `viralityy-prototype.html` | Full interactive dashboard (8 pages) |
| `shared.css` | Shared styles for auth pages |

### Backend (Node.js)
| File | Description |
|---|---|
| `server.js` | Main entry point — auth, billing, YouTube, niche, cron jobs |
| `server_m4a_routes.js` | Niche suggestion engine API routes |
| `server_m5a_routes.js` | Analytics collection API routes |
| `server_m5b_routes.js` | AI learning loop API routes |
| `server_m8_routes.js` | Affiliate programme API routes |
| `server_m9_routes.js` | Video preview queue API routes |
| `analytics_schema.js` | MongoDB schema + indexes |
| `niche_engine_bridge.js` | Node→Python bridge for niche engine |
| `package.json` | Node dependencies |
| `railway.json` | Railway deployment config |
| `Procfile` | Start command for Railway |

### Backend (Python engines)
| File | Description |
|---|---|
| `niche_engine.py` | 27 niches scored by CPM, trend, competition, views |
| `niche_engine_cli.py` | CLI wrapper called by Node bridge |
| `analytics_engine.py` | Daily YouTube analytics collection, performance flags |
| `learning_engine.py` | Weekly AI optimisation — adjusts scripts based on performance |
| `humaniser.py` | 6-pass quality layer (voice, footage, transitions, colour, subtitles) |
| `affiliate_engine.py` | 30% commission tracking, click/signup/conversion lifecycle |
| `preview_engine.py` | 24-hr preview queue, approve/skip/edit logic |
| `requirements.txt` | Python dependencies |

### Config
| File | Description |
|---|---|
| `.env.example` | All required environment variables with labels |
| `.gitignore` | Excludes .env and node_modules |
| `CONTEXT.md` | This file |

---

## Plans & Pricing

| Plan | Monthly | Annual/mo | Annual total | Channels | Shorts/day | Long-form/wk |
|---|---|---|---|---|---|---|
| Starter | $49 | $42 | $504 | 1 | 3 | — |
| Shorts Pro | $79 | $67 | $804 | 1 | 10 | — |
| Growth | $99 | $84 | $1,008 | 3 | 10 each | 2 each |
| Agency | $249 | $212 | $2,544 | 5 | 10 each | 4 each |

**Annual = 15% off monthly, billed upfront for 12 months.**

---

## Key Product Rules (all enforced in code)
- **Free trial:** 7 days, Starter features (3 Shorts/day, 1 channel), no credit card required
- **One trial per YouTube channel** — enforced server-side by channel ID in `TriedChannel` collection
- **Preview queue:** Locked on Starter. Available from Shorts Pro upward.
- **Niche changes post-setup:**
  - Starter → permanently locked (0 changes)
  - Shorts Pro → 2 changes/year
  - Growth → 3 changes/year
  - Agency → 3 changes/year
  - Quota resets every January 1st
- **Long-form videos:** YouTube only. TikTok/Instagram only receive Shorts + auto-extracted clips.
- **Per-channel quotas:** Video limits apply independently to each connected channel (e.g. Agency = 5 channels × 10 Shorts = 50 Shorts/day total)

---

## Dashboard Pages (viralityy-prototype.html)
8 pages in the sidebar:
1. **Dashboard** — stats overview, active channels, today's activity, top video
2. **My Channels** — per-channel TikTok/Instagram toggles, pause/resume, add channel
3. **Niche Finder** — 12 niches with quota enforcement (locked/has-changes/exhausted states)
4. **Analytics** — sortable/filterable video performance table, top 5, breakdown bars
5. **AI Optimisation** — what AI has learned, recent improvements with % gains, run now button
6. **Content Quality** — 6 quality passes explained in plain English, last video stats
7. **Preview Queue** — approve/skip/edit with card animations, badge count sync, auto-post toggle
8. **Affiliate** — referral link copy, commission table, monthly bars, payout modal

---

## Milestone Status

| # | Milestone | Status | Notes |
|---|---|---|---|
| M1 | Deploy backend to Railway | 🟡 In progress | server.js + all deploy files built. Awaiting Railway URL. |
| M2 | Set up MongoDB Atlas | ⬜ Not started | Need connection string from Atlas |
| M3 | Connect Google OAuth (YouTube) | ⬜ Not started | Need YOUTUBE_CLIENT_ID + SECRET |
| M4 | Activate Stripe billing | ⬜ Not started | Need 8 price IDs + webhook secret |
| M5 | Set up Payoneer payouts | ⬜ Not started | Confirm when done |
| M6 | Connect OpenAI | ⬜ Not started | Need OPENAI_API_KEY |
| M7 | Connect ElevenLabs | ⬜ Not started | Need ELEVENLABS_API_KEY |
| M8 | Connect Pexels | ⬜ Not started | Need PEXELS_API_KEY |
| M9 | TikTok posting (Phase 4) | ⬜ Not started | Needs developer app approval |
| M10 | Instagram posting (Phase 4) | ⬜ Not started | Needs Meta app review |

---

## Environment Variables Needed
Copy from `.env.example`. Critical ones for M1 launch (before others):
```
MONGODB_URI
JWT_SECRET
SESSION_SECRET
APP_URL
CRON_SECRET
IP_SALT
NODE_ENV=production
```

---

## Owner Info
- **Product name:** Viralityy
- **Owner background:** Senior Graphic Designer, Amazon A+ Content specialist, 8+ years experience
- **Target market:** Content creators and agencies wanting automated YouTube growth
- **Location:** Pakistan (Islamabad) — use Payoneer for payouts, not Stripe Connect

---

## How to Resume in a New Claude Session
1. Upload this `CONTEXT.md` file
2. Upload whichever project files are relevant to the task
3. Say what you want to work on next — e.g. *"Let's do M2, MongoDB setup"*

Claude will read this file and know exactly where everything stands.

---

## INSTRUCTION FOR CLAUDE (read this on every session start)
When this file is uploaded at the start of a session, automatically do the following **before anything else**:

1. Read the full CONTEXT.md to understand the project state
2. Generate the interactive milestone tracker widget (same format as previously built — expandable cards, progress bar, colour-coded status dots, "Mark as complete" buttons, "Ask a question" sendPrompt buttons)
3. Reflect the current milestone statuses from the table above (🟡 In progress, ⬜ Not started, ✅ Done)
4. Show it immediately so the owner can see exactly where the project stands and click into whichever milestone they want to work on next

Do not wait to be asked. Show the milestone tracker automatically whenever this file is uploaded.
