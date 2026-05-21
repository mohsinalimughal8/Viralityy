// =============================================================================
// VIRALITYY — Main Server Entry Point
// =============================================================================
'use strict';
require('dotenv').config();

const express        = require('express');
const mongoose       = require('mongoose');
const session        = require('express-session');
const MongoStore     = require('connect-mongo');
const passport       = require('passport');
const cors           = require('cors');
const helmet         = require('helmet');
const morgan         = require('morgan');
const rateLimit      = require('express-rate-limit');
const cron           = require('node-cron');
let cronJobsRegistered = false;
const crypto         = require('crypto');
const path           = require('path');
const { exec, spawn } = require('child_process');
const axios           = require('axios');
const jwt            = require('jsonwebtoken');
const bcrypt         = require('bcryptjs');

const app  = express();
const PORT = process.env.PORT || 3000;

// =============================================================================
// FEATURE FLAGS — shelved features (code preserved, just disabled)
// To re-enable any feature: set the env var to 'true' in Railway and redeploy.
// =============================================================================
const FEATURES = {
  // ── Always ON — use only free APIs (YouTube, MongoDB, OpenAI already paid) ──
  competitorWatcher:   true,
  competitorScraper:   true,
  learningLoop:        true,
  affiliateProgram:    true,
  analyticsCollection: true,

  // ── Now active ───────────────────────────────────────────────────────────
  voiceover: true,   // Google Cloud TTS via GOOGLE_TTS_API_KEY
  preview:   true,   // Preview Engine — approval queue before posting

  // ── Shelved — requires paid subscriptions at scale ────────────────────────
  // Twitter paid tiers above Basic, Reddit enterprise for commercial use
  // Enable when ready: set FEATURE_TREND_SCRAPER=true in Railway env vars
  trendScraper: process.env.FEATURE_TREND_SCRAPER === 'true',
};

// Log active vs shelved features at startup
const shelved = Object.entries(FEATURES).filter(([,v])=>!v).map(([k])=>k);
const active  = Object.entries(FEATURES).filter(([,v])=>v).map(([k])=>k);
console.log('[Features] Active:', active.join(', ') || 'none');
console.log('[Features] Shelved (enable via env var):', shelved.join(', ') || 'none');

// =============================================================================
// API COST CONSTANTS (USD per unit)
// =============================================================================
const API_COSTS = {
  openai_input:        0.000003,   // per input token (gpt-4o-mini)
  openai_output:       0.000015,   // per output token (gpt-4o-mini)
  gemini_tts:          0.000001,   // per character
  youtube_upload:      1600,       // quota units per upload
  youtube_analytics:   10,         // quota units per analytics report call
  youtube_daily_quota: 10000,      // total daily quota units
};

const YOUTUBE_QUOTA_COSTS = {
  upload:     1600,
  search:      100,
  channels:      1,
  analytics:    10,
  thumbnails:   50,
  default:       1,
};
const YOUTUBE_DAILY_LIMIT = 9000; // 1000-unit safety buffer below the 10,000 hard cap

// Static audit map — one entry per logical YouTube API call site in this codebase.
// Guarded = pre-call checkYoutubeQuota + post-call logYoutubeQuota.
const QUOTA_AUDIT_MAP = [
  { endpoint: 'youtubeAnalytics.reports.query', cost: 10,   guarded: true,  location: 'fetchChannelAnalyticsHours' },
  { endpoint: 'search.list (REST)',             cost: 100,  guarded: true,  location: 'fetchNichePublishHours' },
  { endpoint: 'channels.list (REST)',           cost: 1,    guarded: true,  location: 'GET /api/channels — stats refresh' },
  { endpoint: 'channels.list (OAuth)',          cost: 1,    guarded: true,  location: 'POST /api/channels/:id/check-thumbnail-eligibility' },
  { endpoint: 'channels.list (OAuth async)',    cost: 1,    guarded: true,  location: 'POST /api/channels/confirm — setImmediate eligibility check' },
  { endpoint: 'search.list + channels.list',   cost: 101,  guarded: true,  location: 'POST /api/research/niche — competitor suggestions' },
  { endpoint: 'search.list × 3 + channels.list', cost: 301, guarded: true, location: 'POST /api/competitors/add' },
  { endpoint: 'channels.list + videos.list',   cost: 2,    guarded: true,  location: 'fetchLiveAnalytics' },
  { endpoint: 'videos.insert',                 cost: 1600, guarded: true,  location: 'pipelineUploadToYouTube — video upload' },
  { endpoint: 'videos.list + thumbnails.set',  cost: 51,   guarded: true,  location: 'pipelineUploadToYouTube — doThumbUpload' },
  { endpoint: 'videos.list',                   cost: 1,    guarded: true,  location: 'analyzeChannelPerformance' },
  { endpoint: 'videos.list',                   cost: 1,    guarded: true,  location: 'fetchYesterdayPerformance (daily gen context)' },
  { endpoint: 'search.list (REST)',             cost: 100,  guarded: true,  location: 'scoutNicheVideos' },
  { endpoint: 'thumbnails.set + videos.list',  cost: 51,   guarded: true,  location: 'GET /api/admin/test-thumbnail-upload' },
  { endpoint: 'videos.list + thumbnails.set',  cost: 51,   guarded: true,  location: 'GET /api/admin/retry-thumbnails (per slot)' },
];

// ─── In-memory pipeline status — reset on restart, updated by runJITPipelineForSlot ───
const PIPELINE_STATUS = {
  currentlyProcessing: [], // [{slotId, title, userId, startedAt, step}]
  lastCompletedAt:     null,
  lastError:           null,
  todayDate:           '',
  todayStats:          { generated: 0, posted: 0, failed: 0, thumbnails: 0 },
};
function pipelineEnsureTodayStats() {
  const today = new Date().toISOString().slice(0, 10);
  if (PIPELINE_STATUS.todayDate !== today) {
    PIPELINE_STATUS.todayDate  = today;
    PIPELINE_STATUS.todayStats = { generated: 0, posted: 0, failed: 0, thumbnails: 0 };
  }
}

// ─── Scientific posting times based on YouTube Shorts algorithm research ───
// Optimal daily posting windows; slots distributed across these based on daily video count.
const POSTING_TIMES_BY_COUNT = {
  1: ['18:00'],
  2: ['12:00', '18:00'],
  3: ['07:00', '15:00', '21:00'],
  4: ['07:00', '12:00', '18:00', '21:00'],
  5: ['07:00', '12:00', '15:00', '18:00', '21:00'],
  6: ['07:00', '09:00', '12:00', '15:00', '18:00', '21:00'],
  7: ['07:00', '09:00', '11:00', '13:00', '15:00', '18:00', '21:00'],
};

// Returns an ISO-like datetime string "YYYY-MM-DDTHH:MM:00" for the slot's optimal posting time.
// videoIndex is 1-based; totalForDay is the number of videos scheduled for that day.
function assignPostingTime(date, videoIndex, totalForDay) {
  const key   = Math.min(Math.max(totalForDay || 1, 1), 7);
  const times = POSTING_TIMES_BY_COUNT[key] || POSTING_TIMES_BY_COUNT[5];
  const hhmm  = times[(videoIndex - 1) % times.length];
  return `${date}T${hhmm}:00Z`; // explicit UTC — prevents string-vs-ISO comparison bugs
}

// ── Optimal Posting Times ─────────────────────────────────────────────────────
const NICHE_DEFAULT_TIMES = {
  'Education':         ['06:00','09:00','12:00','15:00','18:00','20:00','22:00'],
  'Finance':           ['07:00','10:00','12:00','15:00','18:00','20:00','22:00'],
  'Technology':        ['08:00','11:00','13:00','16:00','19:00','21:00','23:00'],
  'Health & Wellness': ['06:00','08:00','11:00','14:00','17:00','19:00','21:00'],
  'Self Improvement':  ['06:00','09:00','12:00','15:00','18:00','20:00','22:00'],
  'Entertainment':     ['10:00','13:00','15:00','17:00','19:00','21:00','23:00'],
  'Business':          ['07:00','09:00','12:00','14:00','17:00','19:00','21:00'],
  'default':           ['07:00','09:00','11:00','13:00','15:00','18:00','21:00'],
};

function mapNicheToCategory(nicheName) {
  const n = (nicheName || '').toLowerCase();
  if (/finance|money|invest|crypto|stock|wealth|budget/.test(n))          return 'Finance';
  if (/tech|ai |software|coding|program|gadget|digital/.test(n))          return 'Technology';
  if (/health|fitness|gym|workout|diet|nutrition|wellness|medic/.test(n)) return 'Health & Wellness';
  if (/motivat|self.improv|personal.develop|stoic|mindset|productiv/.test(n)) return 'Self Improvement';
  if (/business|entrepreneur|startup|market|sales/.test(n))               return 'Business';
  if (/entertain|comedy|fun|game|gaming|movie|music/.test(n))             return 'Entertainment';
  if (/educat|learn|study|science|history|explain/.test(n))               return 'Education';
  return 'default';
}

// Pick `count` posting times spread across the day.
// Never places more than 2 slots within a 120-minute window.
function pickSpreadSlots(pool, count) {
  if (!pool || !pool.length) pool = NICHE_DEFAULT_TIMES.default;
  const toMin = t => { const [h, m] = (t || '07:00').split(':').map(Number); return h * 60 + (m || 0); };
  const sorted = [...new Set(pool)].sort((a, b) => toMin(a) - toMin(b));
  if (count >= sorted.length) return sorted.slice(0, count);

  const idealStep = 1440 / count;
  const selected  = [];
  const usedIdx   = new Set();

  for (let i = 0; i < count; i++) {
    const ideal = (Math.round(idealStep * i + 360)) % 1440; // start spreading from 6 AM
    let bestIdx = -1, bestDist = Infinity;

    for (let j = 0; j < sorted.length; j++) {
      if (usedIdx.has(j)) continue;
      const m = toMin(sorted[j]);
      if (selected.some(sel => Math.abs(toMin(sel) - m) < 120)) continue; // 2-hour gap constraint
      const dist = Math.abs(m - ideal);
      if (dist < bestDist) { bestDist = dist; bestIdx = j; }
    }
    // Relax constraint if no slot found
    if (bestIdx === -1) {
      for (let j = 0; j < sorted.length; j++) {
        if (!usedIdx.has(j)) { bestIdx = j; break; }
      }
    }
    if (bestIdx !== -1) { selected.push(sorted[bestIdx]); usedIdx.add(bestIdx); }
  }
  return selected.sort((a, b) => toMin(a) - toMin(b));
}

async function fetchChannelAnalyticsHours(userId, channelId) {
  const user = await User.findById(userId).lean();
  const ch = (user?.youtubeChannels || []).find(c => c.channelId === channelId) || user?.youtubeChannels?.[0];
  if (!ch?.accessToken) return null;
  const analyticsQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.analytics, userId).catch(() => ({ allowed: true }));
  if (!analyticsQC.allowed) {
    console.warn(`[Quota] fetchChannelAnalyticsHours skipped for ${userId}: ${analyticsQC.reason}`);
    return null;
  }
  const { google } = require('googleapis');
  const oauth2 = new google.auth.OAuth2(process.env.YOUTUBE_CLIENT_ID, process.env.YOUTUBE_CLIENT_SECRET);
  oauth2.setCredentials({ access_token: ch.accessToken, refresh_token: ch.refreshToken });
  const ytAnalytics = google.youtubeAnalytics({ version: 'v2', auth: oauth2 });
  const endDate   = new Date().toISOString().slice(0, 10);
  const startDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const res = await ytAnalytics.reports.query({
    ids: 'channel==MINE', metrics: 'views,estimatedMinutesWatched',
    dimensions: 'hour', startDate, endDate, sort: '-views', maxResults: 24,
  });
  const rows = res.data?.rows || [];
  if (!rows.length) return null;
  logYoutubeQuota('analytics', YOUTUBE_QUOTA_COSTS.analytics, userId).catch(() => {});
  return [...rows].sort((a, b) => b[1] - a[1]).slice(0, 7)
    .map(r => `${String(r[0]).padStart(2, '0')}:00`);
}

async function fetchNichePublishHours(nicheName) {
  const apiKey = process.env.YOUTUBE_API_KEY;
  if (!apiKey) return null;
  const searchQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.search, null).catch(() => ({ allowed: true }));
  if (!searchQC.allowed) {
    console.warn(`[Quota] fetchNichePublishHours skipped: ${searchQC.reason}`);
    return null;
  }
  const publishedAfter = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(nicheName)}&order=viewCount&type=video&publishedAfter=${encodeURIComponent(publishedAfter)}&maxResults=20&key=${apiKey}`;
  const res = await fetch(url);
  if (!res.ok) return null;
  logYoutubeQuota('search', YOUTUBE_QUOTA_COSTS.search, null).catch(() => {});
  const data = await res.json();
  const hourCounts = {};
  for (const item of (data.items || [])) {
    const pub = item.snippet?.publishedAt;
    if (!pub) continue;
    const h = new Date(pub).getUTCHours();
    hourCounts[h] = (hourCounts[h] || 0) + 1;
  }
  return Object.entries(hourCounts).sort((a, b) => b[1] - a[1]).slice(0, 7)
    .map(([h]) => `${String(h).padStart(2, '0')}:00`);
}

async function getOptimalPostingTimes(userId, channelId, nicheName, plan) {
  const slotsCol  = agentCol('calendar_slots');
  const config    = PLAN_CONFIG[plan] || PLAN_CONFIG.trial;
  const count     = config.shortsPerDay;
  const category  = mapNicheToCategory(nicheName);
  const defaultPool = NICHE_DEFAULT_TIMES[category] || NICHE_DEFAULT_TIMES.default;

  const postedCount = await slotsCol.countDocuments({
    userId: String(userId),
    ...(channelId ? { channelId } : {}),
    posted: true,
  });

  let optimalTimes, source;

  if (postedCount < 10) {
    console.log(`[Timing] New channel (${postedCount} posted) — using niche defaults for ${nicheName}`);
    optimalTimes = pickSpreadSlots(defaultPool, count);
    source = 'niche_defaults';
  } else {
    let analyticsPool = null;
    try { analyticsPool = await fetchChannelAnalyticsHours(userId, channelId); }
    catch (e) { console.warn('[Timing] Analytics fetch failed (non-fatal):', e.message); }

    let nichePool = null;
    try { nichePool = await fetchNichePublishHours(nicheName); }
    catch (e) { console.warn('[Timing] Niche hours fetch failed (non-fatal):', e.message); }

    if (analyticsPool && analyticsPool.length > 0) {
      // Weight 70% analytics + 30% niche
      const scored = new Map();
      for (const t of analyticsPool) scored.set(t, (scored.get(t) || 0) + 7);
      for (const t of (nichePool || defaultPool)) scored.set(t, (scored.get(t) || 0) + 3);
      const mergedPool = [...scored.entries()].sort((a, b) => b[1] - a[1]).map(([t]) => t);
      optimalTimes = pickSpreadSlots(mergedPool, count);
      source = 'channel_analytics';
    } else {
      const pool = nichePool?.length ? nichePool : defaultPool;
      optimalTimes = pickSpreadSlots(pool, count);
      source = nichePool?.length ? 'niche_data' : 'niche_defaults';
    }
    console.log(`[Timing] ${nicheName}: source=${source}, times=${optimalTimes.join(',')}`);
  }

  if (channelId) {
    await User.updateOne(
      { _id: userId, 'youtubeChannels.channelId': channelId },
      { $set: {
        'youtubeChannels.$.optimalPostingTimes':      optimalTimes,
        'youtubeChannels.$.optimalTimesSource':       source,
        'youtubeChannels.$.optimalTimesCalculatedAt': new Date(),
      }}
    ).catch(e => console.warn('[Timing] Save failed:', e.message));
  }

  return { times: optimalTimes, source };
}


// =============================================================================
// DATABASE
// =============================================================================
// Global handler — catches MongoServerError unhandled rejections that crash Node
process.on('unhandledRejection', (err) => {
  console.error('[UnhandledRejection] Caught — server stays alive:', err.message);
  // Do NOT exit — let Railway healthcheck keep passing
});

async function runStartupSlotCheck() {
  try {
    const now = new Date().toISOString();
    console.log(`[Startup] Recovery check ─── ${now}`);
    console.log(`[Startup] OPENAI_API_KEY:        ${process.env.OPENAI_API_KEY        ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] YOUTUBE_API_KEY:       ${process.env.YOUTUBE_API_KEY       ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] YOUTUBE_CLIENT_ID:     ${process.env.YOUTUBE_CLIENT_ID     ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] YOUTUBE_CLIENT_SECRET: ${process.env.YOUTUBE_CLIENT_SECRET ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] GOOGLE_TTS_API_KEY:    ${process.env.GOOGLE_TTS_API_KEY    ? 'SET' : 'MISSING ⚠'}`);
    console.log('[AutoPost Audit] ADMIN_EMAIL:', process.env.ADMIN_EMAIL ? 'SET' : 'MISSING ⚠');
    console.log('[AutoPost Audit] ADMIN_PASSWORD:', process.env.ADMIN_PASSWORD ? 'SET' : 'MISSING ⚠');
    const paddleEnv = process.env.PADDLE_ENV || 'sandbox';
    console.log(`[Startup] PADDLE_ENV:               ${paddleEnv}`);
    console.log(`[Startup] PADDLE_API_KEY:           ${process.env.PADDLE_API_KEY           ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] PADDLE_CLIENT_TOKEN:      ${process.env.PADDLE_CLIENT_TOKEN      ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] PADDLE_WEBHOOK_SECRET:    ${process.env.PADDLE_WEBHOOK_SECRET    ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] PADDLE_STARTER_PRICE_ID:  ${process.env.PADDLE_STARTER_PRICE_ID  ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] PADDLE_SHORTSPRO_PRICE_ID:${process.env.PADDLE_SHORTSPRO_PRICE_ID ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] PADDLE_GROWTH_PRICE_ID:   ${process.env.PADDLE_GROWTH_PRICE_ID   ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[Startup] PADDLE_AGENCY_PRICE_ID:   ${process.env.PADDLE_AGENCY_PRICE_ID   ? 'SET' : 'MISSING ⚠'}`);


    // Reset any slots that were stuck mid-pipeline when the server went down.
    // Never pre-generate or run any pipeline at startup.
    const slotsCol = agentCol('calendar_slots');
    const result = await slotsCol.updateMany(
      { status: 'processing' },
      { $set: { status: 'scheduled', pipelineStatus: 'reset-on-startup' } }
    );
    if (result.modifiedCount > 0) {
      console.log(`[Startup] Reset ${result.modifiedCount} stuck processing slot(s) → scheduled`);
    } else {
      console.log('[Startup] No stuck processing slots found ✓');
    }

    // Convert ALL 'pending' slots → 'scheduled' so the PostingCron can pick them up.
    // Two sources of 'pending':
    //   (a) Old server.js code (pre-refactor) — already has date + scheduledPostTime, just wrong status
    //   (b) content_planner_agent.py — has scheduledDate+postTime, needs scheduledPostTime constructed
    const today = new Date().toISOString().slice(0, 10);
    const pendingSlots = await slotsCol.find({
      status: 'pending', posted: { $ne: true },
    }).toArray().catch(() => []);
    let converted = 0;
    const nowMs = Date.now();
    for (let i = 0; i < pendingSlots.length; i++) {
      const s = pendingSlots[i];
      const slotDate = s.date || (s.scheduledDate ? s.scheduledDate.slice(0, 10) : today);
      let scheduledPostTime = s.scheduledPostTime;
      if (!scheduledPostTime) {
        const slotTime = s.postTime || '18:00';
        scheduledPostTime = `${slotDate}T${slotTime}:00Z`; // explicit UTC
      } else if (!scheduledPostTime.endsWith('Z')) {
        scheduledPostTime = scheduledPostTime.slice(0, 19) + 'Z'; // normalise legacy no-Z strings
      }
      // Push past-due times forward so the PostingCron picks them up immediately
      if (slotDate === today && new Date(scheduledPostTime) < new Date(nowMs + 5 * 60 * 1000)) {
        scheduledPostTime = new Date(nowMs + (i + 1) * 10 * 60 * 1000).toISOString();
      }
      await slotsCol.updateOne(
        { _id: s._id },
        { $set: { status: 'scheduled', scheduledPostTime, date: slotDate, pipelineStatus: 'converted-from-pending', posted: false } }
      ).catch(() => {});
      converted++;
    }
    if (converted > 0) console.log(`[Startup] Converted ${converted} pending slot(s) → scheduled`);

    // Rescue today's missed/skipped slots — give them a new posting time so they still run today.
    const missedSlots = await slotsCol.find({
      date: today, status: { $in: ['missed', 'skipped'] }, posted: false,
    }).toArray().catch(() => []);
    let rescued = 0;
    for (let i = 0; i < missedSlots.length; i++) {
      const s = missedSlots[i];
      if ((s.retryCount || 0) >= 3) continue;
      const rescheduleAt = new Date(nowMs + (converted + i + 1) * 10 * 60 * 1000).toISOString();
      await slotsCol.updateOne(
        { _id: s._id },
        { $set: { status: 'scheduled', scheduledPostTime: rescheduleAt, pipelineStatus: 'rescued-on-startup' }, $inc: { retryCount: 1 } }
      ).catch(() => {});
      rescued++;
    }
    if (rescued > 0) console.log(`[Startup] Rescued ${rescued} missed/skipped slot(s) → rescheduled for today`);

    console.log('[Startup] Recovery check complete');
  } catch (e) {
    console.error('[Startup] Recovery check failed:', e.message);
  }
}

if (process.env.MONGODB_URI) {
  mongoose.connect(process.env.MONGODB_URI, {
    useNewUrlParser:    true,
    useUnifiedTopology: true,
    serverSelectionTimeoutMS: 5000,
  })
  .then(() => {
    console.log('[MongoDB] Connected successfully');
    fixAdminAccount();
    registerCronJobs();
    setTimeout(runStartupSlotCheck, 3000);
    setTimeout(seedHooksLibrary, 8000);
  })
  .catch(err => {
    console.error('[MongoDB] Connection failed — check MONGODB_URI credentials in Railway Variables:', err.message);
    // Server keeps running — /health still responds, DB-dependent routes return 503
  });
} else {
  console.warn('[MongoDB] MONGODB_URI not set — database features disabled');
}

// =============================================================================
// MODELS
// =============================================================================
const userSchema = new mongoose.Schema({
  name:           { type: String, required: true },
  email:          { type: String, required: true, unique: true, lowercase: true },
  passwordHash:   { type: String },
  googleId:       { type: String },
  plan:           { type: String, enum: ['trial','starter','shorts_pro','growth','agency'], default: 'trial' },
  trialStartedAt: { type: Date, default: Date.now },
  trialEndsAt:    { type: Date, default: () => new Date(Date.now() + 2 * 24 * 60 * 60 * 1000) },
  stripeCustomerId:     { type: String },
  stripeSubscriptionId: { type: String },
  subscriptionStatus:   { type: String, enum: ['active', 'cancelled', 'expired', 'trial', 'past_due'], default: 'trial' },
  subscriptionStartDate:  { type: Date },
  subscriptionEndDate:    { type: Date },
  subscriptionRenewsAt:   { type: Date },
  pastDueSince:           { type: Date },
  videosPerDayPerChannel: { type: Number, default: 3 },
  maxChannels:            { type: Number, default: 1 },
  longformEnabled:        { type: Boolean, default: false },
  lemonSqueezyCustomerId:     { type: String },
  lemonSqueezySubscriptionId: { type: String },
  paddleCustomerId:           { type: String },
  paddleSubscriptionId:       { type: String },
  billingHistory: [{ date: Date, amount: Number, plan: String, status: String, invoiceId: String }],
  youtubeChannels: [{ channelId: String, channelName: String, accessToken: String, refreshToken: String, nicheId: String, nicheName: String, paused: Boolean, tiktokEnabled: Boolean, instagramEnabled: Boolean, connectedAt: String, subscriberCount: Number, thumbnail: String, thumbnailsEnabled: Boolean, optimalPostingTimes: [String], optimalTimesSource: String, optimalTimesCalculatedAt: Date, styleConfig: { captionFont: String, colorScheme: String, musicGenre: String, introText: String } }],
  pendingOAuthChannels: [{ channelId: String, channelName: String, thumbnail: String, subscriberCount: Number }],
  googleAccessToken:  String,
  googleRefreshToken: String,
  nicheId:         { type: String },
  nicheName:       { type: String },
  nicheChangesUsed: { type: Number, default: 0 },
  nicheChangesYear: { type: Number, default: () => new Date().getFullYear() },
  affiliateCode:   { type: String, unique: true, sparse: true },
  referredBy:      { type: String },
  autoPost:           { type: Boolean, default: true },
  onboardingComplete: { type: Boolean, default: false },
  timezone:           { type: String },
  usedFootageIds:  { type: [String], default: [] },
  blocked:         { type: Boolean, default: false },
  blockedAt:       { type: Date },
  blockedReason:   { type: String },
  unblockedAt:     { type: Date },
  sessionVersion:  { type: Number, default: 0 },
  createdAt:       { type: Date, default: Date.now },
}, { timestamps: true });

const User = mongoose.model('User', userSchema);

const triedChannelSchema = new mongoose.Schema({ channelId: { type: String, unique: true }, usedAt: { type: Date, default: Date.now } });
const TriedChannel = mongoose.model('TriedChannel', triedChannelSchema);

// One-time startup fix — ensures the admin account always has an active agency subscription.
// Safe to leave in: skips the DB write if the account is already correct.
async function fixAdminAccount() {
  try {
    const email   = 'mohsinalimughal8@gmail.com';
    const endDate = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);
    const result  = await User.findOneAndUpdate(
      { email },
      {
        $set: {
          plan:                   'agency',
          subscriptionStatus:     'active',
          subscriptionStartDate:  new Date(),
          subscriptionEndDate:    endDate,
          subscriptionRenewsAt:   endDate,
          videosPerDayPerChannel: 10,
          maxChannels:            4,
          longformEnabled:        true,
          trialEndsAt:            null,
        },
      },
      { new: true }
    ).select('email plan subscriptionStatus');
    if (result) {
      console.log(`[Admin] Account fixed: ${result.email} → ${result.plan} ${result.subscriptionStatus}`);
    } else {
      console.warn(`[Admin] fixAdminAccount: user ${email} not found`);
    }
  } catch (err) {
    console.error('[Admin] fixAdminAccount failed:', err.message);
  }
}

// =============================================================================
// MIDDLEWARE
// =============================================================================
app.use(helmet({ contentSecurityPolicy: false }));
// Allow requests from both the Railway backend URL and the Netlify frontend
const ALLOWED_ORIGINS = [
  process.env.APP_URL,
  process.env.FRONTEND_URL,
  'https://viralityy.pages.dev',
  'https://viralityy.com',
  'https://www.viralityy.com',
  'http://localhost:3000',
  'http://localhost:5500',
].filter(Boolean);

app.use(cors({
  origin: (origin, callback) => {
    // Allow requests with no origin (mobile apps, curl, Postman)
    if (!origin) return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
    console.warn('[CORS] Blocked origin:', origin);
    callback(new Error('Not allowed by CORS'));
  },
  credentials: true,
}));
app.use(morgan('combined'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use(session({
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  store: process.env.MONGODB_URI
    ? MongoStore.create({ mongoUrl: process.env.MONGODB_URI })
    : undefined,  // falls back to memory store if MONGODB_URI not set
  cookie: { secure: process.env.NODE_ENV === 'production', maxAge: 7 * 24 * 60 * 60 * 1000 },
}));

app.use(passport.initialize());
app.use(passport.session());

// Rate limiting
const limiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 200, standardHeaders: true, legacyHeaders: false });
app.use('/api/', limiter);

// =============================================================================
// HEALTH CHECK — registered early so it responds even if other routes fail
// This is what Railway pings. Must respond 200 within 5 minutes.
// =============================================================================
app.get('/health', async (req, res) => {
  const today        = new Date().toISOString().slice(0, 10);
  const startOfToday = today + 'T00:00:00.000Z';
  let pipeline = { error: 'unavailable' };
  let youtubeQuotaUsed = 0;
  try {
    const slotsCol  = agentCol('calendar_slots');
    const quotaCol  = agentCol('youtubeQuota');
    const [gen, ready, posted, failed, processing, quotaResult] = await Promise.all([
      slotsCol.countDocuments({ date: today }),
      slotsCol.countDocuments({ date: today, status: 'scheduled', posted: false }),
      slotsCol.countDocuments({ status: 'posted', postedAt: { $gte: startOfToday } }),
      slotsCol.countDocuments({ date: today, status: 'failed' }),
      slotsCol.countDocuments({ date: today, status: 'processing' }),
      quotaCol.aggregate([
        { $match: { date: today } },
        { $group: { _id: null, total: { $sum: '$units' } } },
      ]).toArray(),
    ]);
    youtubeQuotaUsed = quotaResult[0]?.total || 0;
    const nextSlot = await slotsCol
      .find({ date: today, status: { $in: ['scheduled', 'processing'] }, posted: false })
      .sort({ scheduledPostTime: 1 })
      .limit(1)
      .toArray()
      .then(r => r[0] || null)
      .catch(() => null);
    // Count all non-standard statuses so operators can see the full picture
    const [missed, skipped, pending] = await Promise.all([
      slotsCol.countDocuments({ date: today, status: 'missed' }),
      slotsCol.countDocuments({ date: today, status: 'skipped' }),
      slotsCol.countDocuments({ date: today, status: 'pending' }),
    ]);
    // Recent failures and skips — last 5 of each for instant diagnostics
    const [recentFailed, recentSkipped] = await Promise.all([
      slotsCol.find({ status: 'failed' }).sort({ failedAt: -1 }).limit(5).project({ _id: 1, title: 1, channelId: 1, failedAt: 1, failureReason: 1, pipelineError: 1 }).toArray(),
      slotsCol.find({ status: 'skipped' }).sort({ skippedAt: -1 }).limit(5).project({ _id: 1, title: 1, channelId: 1, skippedAt: 1, skipReason: 1, pipelineStatus: 1 }).toArray(),
    ]);
    pipeline = {
      todaysSlotsGenerated: gen > 0,
      totalSlotsToday:  gen,
      slotsScheduled:  ready,
      slotsPosted:     posted,
      slotsFailed:     failed,
      slotsProcessing: processing,
      slotsMissed:     missed,
      slotsSkipped:    skipped,
      slotsPending:    pending,
      nextPostTime:    nextSlot?.scheduledPostTime || null,
      cronJobsRegistered,
      lastPipelineError: PIPELINE_STATUS.lastError || null,
      todayStats:      PIPELINE_STATUS.todayStats,
      recentIssues: {
        recentFailures: recentFailed.map(s => ({
          slotId:    String(s._id),
          title:     s.title || null,
          channelId: s.channelId || null,
          step:      s.failureReason?.step  || 'unknown',
          error:     s.failureReason?.error || s.pipelineError || 'unknown',
          timestamp: s.failedAt || s.failureReason?.timestamp || null,
        })),
        recentSkips: recentSkipped.map(s => ({
          slotId:    String(s._id),
          title:     s.title || null,
          channelId: s.channelId || null,
          reason:    s.skipReason || s.pipelineStatus || 'unknown',
          timestamp: s.skippedAt || null,
        })),
      },
    };
  } catch { /* pipeline stays { error: 'unavailable' } */ }

  const now = new Date();
  const nextQuotaReset = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1)).toISOString();

  res.json({
    status:  'ok',
    uptime:  process.uptime(),
    env:     process.env.NODE_ENV || 'development',
    mongo:   mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
    paddle:  process.env.PADDLE_API_KEY ? 'configured' : 'not configured',
    youtube: !!process.env.YOUTUBE_API_KEY   ? 'configured' : 'not configured',
    openai:  !!process.env.OPENAI_API_KEY    ? 'configured' : 'not configured',
    pipeline,
    youtubeQuotaUsed,
    youtubeQuotaRemaining: Math.max(0, 10000 - youtubeQuotaUsed),
    youtubeQuotaResetAt:   nextQuotaReset,
  });
});


// =============================================================================
// PASSPORT — GOOGLE OAUTH
// =============================================================================
const GoogleStrategy = require('passport-google-oauth20').Strategy;

passport.use(new GoogleStrategy({
  clientID:          process.env.YOUTUBE_CLIENT_ID,
  clientSecret:      process.env.YOUTUBE_CLIENT_SECRET,
  callbackURL:       `${process.env.APP_URL}/auth/google/callback`,
  scope:             ['profile', 'email', 'https://www.googleapis.com/auth/youtube', 'https://www.googleapis.com/auth/youtube.upload'],
  passReqToCallback: true,
}, async (req, accessToken, refreshToken, profile, done) => {
  try {
    // 1. Find or create user
    let isNewUser = false;
    let user = await User.findOne({ googleId: profile.id });
    if (!user) {
      user = await User.findOne({ email: profile.emails[0].value });
      if (user) { user.googleId = profile.id; }
    }
    if (!user) {
      isNewUser = true;
      const affiliateRef = req.session?.affiliateRef || null;
      const trialEnd = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000);
      user = await User.create({
        name:                   profile.displayName,
        email:                  profile.emails[0].value,
        googleId:               profile.id,
        plan:                   'trial',
        subscriptionStatus:     'trial',
        trialStartedAt:         new Date(),
        trialEndsAt:            trialEnd,
        videosPerDayPerChannel: 3,
        maxChannels:            1,
        longformEnabled:        false,
        youtubeChannels:        [],
        affiliateCode:          generateAffiliateCode(),
        ...(affiliateRef && { referredBy: affiliateRef }),
      });
      console.log(`[OAuth] New user ${user.email} — trial until ${trialEnd.toISOString()}}`);
      if (affiliateRef) console.log(`[OAuth] New user ${user.email} referred by code: ${affiliateRef}`);
    }

    // 2. Save OAuth tokens on user
    user.googleAccessToken  = accessToken;
    user.googleRefreshToken = refreshToken || user.googleRefreshToken;

    // 3. Fetch ALL YouTube channels on this Google account for the picker
    try {
      const ytRes = await fetch(
        'https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&mine=true&maxResults=50',
        { headers: { Authorization: `Bearer ${accessToken}` } }
      );
      const ytData = await ytRes.json();
      const items = ytData?.items || [];

      if (items.length > 0) {
        // Refresh tokens on any already-connected channels
        for (const item of items) {
          const existing = (user.youtubeChannels || []).find(c => c.channelId === item.id);
          if (existing) {
            existing.accessToken  = accessToken;
            existing.refreshToken = refreshToken || existing.refreshToken;
          }
        }

        // Store all channels as pending — user picks one in the UI
        user.pendingOAuthChannels = items.map(item => ({
          channelId:       item.id,
          channelName:     item.snippet?.title || 'My Channel',
          thumbnail:       item.snippet?.thumbnails?.default?.url || '',
          subscriberCount: parseInt(item.statistics?.subscriberCount || '0', 10),
        }));
        console.log(`[OAuth] ${items.length} channel(s) pending selection for user ${user.email}`);
      } else {
        user.pendingOAuthChannels = [];
        console.warn('[OAuth] No YouTube channels found on this Google account');
      }
    } catch (ytErr) {
      console.warn('[OAuth] YouTube channel fetch failed:', ytErr.message);
      user.pendingOAuthChannels = [];
    }

    await user.save();
    return done(null, user);
  } catch (err) {
    console.error('[OAuth] Strategy error:', err);
    return done(err);
  }
}));

passport.serializeUser((user, done) => done(null, user.id));
passport.deserializeUser(async (id, done) => { try { done(null, await User.findById(id)); } catch(e){ done(e); } });

// =============================================================================
// HELPERS
// =============================================================================
async function requireAuth(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (token) {
    let decoded;
    try {
      decoded = jwt.verify(token, process.env.JWT_SECRET);
    } catch { return res.status(401).json({ error: 'Invalid token' }); }

    try {
      const user = await User.findById(decoded.userId).select('blocked sessionVersion').lean();
      if (!user) return res.status(401).json({ error: 'Invalid token' });
      if (user.blocked) return res.status(403).json({ error: 'Account suspended. Contact support@viralityy.com' });
      // sessionVersion mismatch means token was invalidated (e.g. user was blocked then unblocked)
      if ((user.sessionVersion || 0) !== (decoded.sessionVersion || 0)) {
        return res.status(401).json({ error: 'Session expired. Please log in again.' });
      }
    } catch { return res.status(500).json({ error: 'Auth check failed' }); }

    req.user = { id: decoded.userId };
    return next();
  }
  if (req.isAuthenticated()) return next();
  return res.status(401).json({ error: 'Authentication required' });
}

function hashIp(ip) {
  return crypto.createHmac('sha256', process.env.IP_SALT || 'default_salt').update(ip || '').digest('hex');
}

// Admin JWT middleware — separate secret from user JWT; returns 403 with no details on failure
function requireAdmin(req, res, next) {
  // ?secret= bypass — allows browser-direct admin calls without a JWT
  if (req.query.secret === 'VRL-ADM-X7K9-2025') {
    req.adminUser = { role: 'admin', secretBypass: true };
    return next();
  }
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(403).json({ error: 'Access denied' });
  try {
    const decoded = jwt.verify(token, process.env.ADMIN_PASSWORD || 'admin_secret_unset');
    if (decoded.role !== 'admin') return res.status(403).json({ error: 'Access denied' });
    req.adminUser = decoded;
    return next();
  } catch {
    return res.status(403).json({ error: 'Access denied' });
  }
}

// Fire-and-forget API usage logger — never throws, never blocks the caller.
async function logAPIUsage(service, action, userId, tokens, cost, success) {
  try {
    const col = agentCol('apiUsage');
    await col.insertOne({
      service,
      action,
      userId:         userId ? String(userId) : null,
      tokensUsed:     tokens  || 0,
      estimatedCost:  cost    || 0,
      success:        !!success,
      timestamp:      new Date().toISOString(),
    });
  } catch { /* non-fatal — never crash the caller */ }
}

// Log a YouTube Data API quota consumption event to the youtubeQuota collection.
async function logYoutubeQuota(action, units, userId) {
  try {
    await agentCol('youtubeQuota').insertOne({
      date:      new Date().toISOString().slice(0, 10),
      units,
      action,
      userId:    userId ? String(userId) : null,
      timestamp: new Date(),
    });
  } catch { /* non-fatal */ }
}

// Returns { allowed, reason, used, userUsed }. Fails open on DB errors — never blocks the caller.
async function checkYoutubeQuota(unitsNeeded, userId) {
  try {
    const today  = new Date().toISOString().slice(0, 10);
    const col    = agentCol('youtubeQuota');

    // Sum all quota used today across all users
    const totalResult = await col.aggregate([
      { $match: { date: today } },
      { $group: { _id: null, total: { $sum: '$units' } } },
    ]).toArray();
    const totalUsed = totalResult[0]?.total || 0;

    console.log(`[Quota] Check: ${totalUsed} used + ${unitsNeeded} needed / ${YOUTUBE_DAILY_LIMIT} limit`);

    if (totalUsed + unitsNeeded > YOUTUBE_DAILY_LIMIT) {
      console.warn(`[Quota] Daily quota nearly exhausted — ${totalUsed}/${YOUTUBE_DAILY_LIMIT} units used`);
      return { allowed: false, reason: 'Daily quota nearly exhausted', used: totalUsed };
    }

    // Per-user fair-share check (only when userId provided and multiple users exist)
    if (userId) {
      const activeUsers = await User.countDocuments({ 'youtubeChannels.0': { $exists: true } }).catch(() => 1);
      const perUserShare = Math.floor(YOUTUBE_DAILY_LIMIT / Math.max(activeUsers, 1));

      const userResult = await col.aggregate([
        { $match: { date: today, userId: String(userId) } },
        { $group: { _id: null, total: { $sum: '$units' } } },
      ]).toArray();
      const userUsed = userResult[0]?.total || 0;

      if (userUsed + unitsNeeded > perUserShare) {
        console.warn(`[Quota] User ${userId} share exhausted — ${userUsed}/${perUserShare} units`);
        return { allowed: false, reason: `User quota share exhausted (${userUsed}/${perUserShare} units)`, used: totalUsed, userUsed };
      }
    }

    return { allowed: true, used: totalUsed };
  } catch (err) {
    console.warn('[Quota] Check error (allowing by default):', err.message);
    return { allowed: true, used: 0 }; // fail open — never block the pipeline on a DB error
  }
}

const PLAN_CONFIG = {
  trial:      { videosPerDay: 3,  shortsPerDay: 3,  longFormPerWeek: 0, channels: 1, price: 0,   paddlePriceId: null },
  starter:    { videosPerDay: 3,  shortsPerDay: 3,  longFormPerWeek: 0, channels: 1, price: 29,  paddlePriceId: process.env.PADDLE_STARTER_PRICE_ID   || null },
  shorts_pro: { videosPerDay: 7,  shortsPerDay: 7,  longFormPerWeek: 0, channels: 1, price: 69,  paddlePriceId: process.env.PADDLE_SHORTSPRO_PRICE_ID || null },
  growth:     { videosPerDay: 10, shortsPerDay: 10, longFormPerWeek: 1, channels: 2, price: 99,  paddlePriceId: process.env.PADDLE_GROWTH_PRICE_ID    || null },
  agency:     { videosPerDay: 10, shortsPerDay: 10, longFormPerWeek: 1, channels: 4, price: 249, paddlePriceId: process.env.PADDLE_AGENCY_PRICE_ID    || null },
};

// Single source of truth for plan limits — used everywhere plan limits are checked
const PLAN_LIMITS = {
  trial:      { videosPerDayPerChannel: 3,  maxChannels: 1, longformEnabled: false },
  starter:    { videosPerDayPerChannel: 3,  maxChannels: 1, longformEnabled: false },
  shorts_pro: { videosPerDayPerChannel: 7,  maxChannels: 1, longformEnabled: false },
  growth:     { videosPerDayPerChannel: 10, maxChannels: 2, longformEnabled: true  },
  agency:     { videosPerDayPerChannel: 10, maxChannels: 4, longformEnabled: true  },
};

function getPlanLimits(plan) {
  return PLAN_LIMITS[plan] || PLAN_LIMITS.trial;
}

function planNicheQuota(plan) {
  return plan === 'trial' ? 0 : Infinity; // trial locked, all paid plans unlimited
}

function isPlanActive(user) {
  const status = user.subscriptionStatus;
  const now = new Date();
  if (status === 'active')    return true;
  if (status === 'trial')     return !!(user.trialEndsAt) && now < new Date(user.trialEndsAt);
  if (status === 'cancelled') return !!(user.subscriptionEndDate && now < new Date(user.subscriptionEndDate));
  if (status === 'past_due') {
    // Allow during 3-day grace period
    if (!user.pastDueSince) return true;
    return now < new Date(new Date(user.pastDueSince).getTime() + 3 * 24 * 60 * 60 * 1000);
  }
  if (status === 'expired') return false;
  // Legacy fallback
  if (!status && user.plan !== 'trial') return true;
  return !!(user.trialEndsAt) && now < new Date(user.trialEndsAt);
}

// Check whether a user's subscription is blocked for pipeline use
// Returns { blocked: true, reason: '...' } or { blocked: false }
function getPipelineBlock(user) {
  const status = user.subscriptionStatus || 'trial';
  const now = new Date();
  if (status === 'expired') return { blocked: true, reason: 'expired' };
  if (status === 'trial') {
    if (user.trialEndsAt && now > new Date(user.trialEndsAt)) return { blocked: true, reason: 'trial-expired' };
  }
  if (status === 'cancelled') {
    if (!(user.subscriptionEndDate && now < new Date(user.subscriptionEndDate))) return { blocked: true, reason: 'cancelled-period-ended' };
  }
  if (status === 'past_due') {
    const since = user.pastDueSince ? new Date(user.pastDueSince) : now;
    if (now > new Date(since.getTime() + 3 * 24 * 60 * 60 * 1000)) return { blocked: true, reason: 'past_due-grace-expired' };
  }
  return { blocked: false };
}

function subscriptionDaysRemaining(user) {
  const status = user.subscriptionStatus;
  if (status === 'trial' || !status) {
    return Math.max(0, Math.ceil((new Date(user.trialEndsAt) - Date.now()) / 86400000));
  }
  if (user.subscriptionEndDate) {
    return Math.max(0, Math.ceil((new Date(user.subscriptionEndDate) - Date.now()) / 86400000));
  }
  return null;
}

function generateAffiliateCode() {
  return crypto.randomBytes(4).toString('hex').toUpperCase();
}

function runPython(scriptPath, args, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    exec(`python3 "${scriptPath}" ${args}`, { timeout: timeoutMs }, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      try { resolve(JSON.parse(stdout.trim())); }
      catch { resolve({ raw: stdout.trim() }); }
    });
  });
}

// =============================================================================
// AUTH ROUTES
// =============================================================================
app.get('/auth/google', (req, res, next) => {
  if (req.query.ref) req.session.affiliateRef = req.query.ref;
  next();
}, passport.authenticate('google', {
  scope: [
    'profile',
    'email',
    'https://www.googleapis.com/auth/youtube',
    'https://www.googleapis.com/auth/youtube.upload',
    'https://www.googleapis.com/auth/yt-analytics.readonly',
  ],
  accessType: 'offline',
  prompt: 'consent',
}));

app.get('/auth/google/callback',
  (req, res, next) => {
    passport.authenticate('google', { failureRedirect: '/auth/failed' }, (err, user, info) => {
      if (err) {
        console.error('[OAuth] Strategy error:', err);
        return res.status(500).send(`Auth error: ${err.message}<br><br>Check Railway logs for details.`);
      }
      if (!user) {
        console.error('[OAuth] No user returned:', info);
        return res.redirect('/auth/failed');
      }
      req.logIn(user, (loginErr) => {
        if (loginErr) {
          console.error('[OAuth] Login error:', loginErr);
          return res.status(500).send(`Login error: ${loginErr.message}`);
        }
        next();
      });
    })(req, res, next);
  },
  (req, res) => {
  try {
    if (req.user.blocked) {
      return res.redirect(`${process.env.FRONTEND_URL || 'https://viralityy.pages.dev'}?error=account_suspended`);
    }
    const token = jwt.sign({ userId: req.user.id, sessionVersion: req.user.sessionVersion || 0 }, process.env.JWT_SECRET, { expiresIn: '7d' });
    const frontendUrl = process.env.FRONTEND_URL || 'https://viralityy.pages.dev';

    const pending = req.user.pendingOAuthChannels || [];
    const userData = {
      id:              req.user.id,
      email:           req.user.email || '',
      name:            req.user.name  || '',
      plan:            req.user.plan  || 'trial',
      nicheId:         (req.user.youtubeChannels || [])[0]?.nicheId || null,
      hasChannels:     (req.user.youtubeChannels || []).length > 0,
      channels:        (req.user.youtubeChannels || []).map(c => ({
        channelId:   c.channelId,
        channelName: c.channelName,
      })),
      channelSelect:   pending.length > 0,
      pendingChannels: pending,
    };

    res.redirect(`${frontendUrl}?token=${token}&user=${encodeURIComponent(JSON.stringify(userData))}`);
  } catch (err) {
    console.error('[OAuth callback error]', err);
    res.status(500).send('Auth callback error: ' + err.message);
  }
});

app.get('/auth/failed', (req, res) => {
  res.status(401).send('Google sign-in failed. Check your OAuth credentials in Railway Variables.');
});

// POST /api/auth/register — email + password signup
app.post('/api/auth/register', async (req, res) => {
  try {
    const { name, email, password } = req.body;
    if (!name || !email || !password || password.length < 8)
      return res.status(400).json({ error: 'Name, email, and password (min 8 chars) required' });

    const existing = await User.findOne({ email: email.toLowerCase() });
    if (existing) return res.status(409).json({ error: 'Email already registered' });

    const passwordHash = await bcrypt.hash(password, 12);
    const affiliateCode = generateAffiliateCode();

    const ref = typeof req.body.ref === 'string' && req.body.ref.trim() ? req.body.ref.trim() : null;
    const trialEnd = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000);
    const user = await User.create({
      name, email: email.toLowerCase(), passwordHash, affiliateCode,
      plan: 'trial',
      subscriptionStatus: 'trial',
      trialStartedAt: new Date(),
      trialEndsAt: trialEnd,
      ...(ref && { referredBy: ref }),
    });

    const token = jwt.sign({ userId: user.id, sessionVersion: 0 }, process.env.JWT_SECRET, { expiresIn: '7d' });
    res.status(201).json({ token, user: { id: user.id, name: user.name, email: user.email, plan: user.plan, subscriptionStatus: 'trial', trialEndsAt: user.trialEndsAt } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/auth/login — email + password login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await User.findOne({ email: email?.toLowerCase() });
    if (!user || !user.passwordHash) return res.status(401).json({ error: 'Invalid credentials' });

    const valid = await bcrypt.compare(password, user.passwordHash);
    if (!valid) return res.status(401).json({ error: 'Invalid credentials' });

    if (user.blocked) return res.status(403).json({ error: 'Account suspended. Contact support@viralityy.com' });

    const token = jwt.sign({ userId: user.id, sessionVersion: user.sessionVersion || 0 }, process.env.JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { id: user.id, name: user.name, email: user.email, plan: user.plan,
      subscriptionStatus: user.subscriptionStatus || 'trial',
      trialEndsAt: user.trialEndsAt,
      subscriptionEndDate: user.subscriptionEndDate || null,
      subscriptionRenewsAt: user.subscriptionRenewsAt || null,
    } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/auth/me — current user
app.get('/api/auth/me', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id).select('-passwordHash');
    if (!user) return res.status(404).json({ error: 'User not found' });
    const status = user.subscriptionStatus || 'trial';
    const daysLeft = subscriptionDaysRemaining(user);
    res.json({
      user,
      planActive:         isPlanActive(user),
      subscriptionStatus: status,
      daysRemaining:      daysLeft,
      trialDaysLeft:      status === 'trial' ? Math.max(0, Math.ceil((new Date(user.trialEndsAt) - Date.now()) / 86400000)) : null,
      subscriptionEndDate:   user.subscriptionEndDate || null,
      subscriptionRenewsAt:  user.subscriptionRenewsAt || null,
      planPrice:             PLAN_CONFIG[user.plan]?.price ?? 0,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// =============================================================================
// YOUTUBE CHANNEL ROUTES
// =============================================================================

// GET /api/channels — list user's connected channels + real YouTube stats + plan-aware niche quota
// YouTube stats are cached in channel_stats_cache for 1 hour to minimise API quota usage.
app.get('/api/channels', requireAuth, async (req, res) => {
  try {
    const user      = await User.findById(req.user.id);
    const statsCol  = agentCol('channel_stats_cache');
    const CACHE_TTL = 60 * 60 * 1000; // 1 hour

    const channels = await Promise.all((user.youtubeChannels || []).map(async ch => {
      const channelId = ch.channelId || '';
      let stats = {
        subscriberCount: ch.subscriberCount || 0,
        viewCount:       0,
        videoCount:      0,
      };

      if (channelId) {
        // 1. Check 1-hour cache
        const cached = await statsCol.findOne({ channelId }).catch(() => null);
        const age    = cached ? Date.now() - new Date(cached.cachedAt).getTime() : Infinity;

        if (cached && age < CACHE_TTL) {
          stats = { subscriberCount: cached.subscriberCount, viewCount: cached.viewCount, videoCount: cached.videoCount };
        } else if (process.env.YOUTUBE_API_KEY) {
          // 2. Fetch fresh stats from YouTube Data API
          const statsQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.channels, req.user.id).catch(() => ({ allowed: true }));
          if (!statsQC.allowed) {
            console.warn(`[Quota] Channel stats fetch skipped for ${channelId}: ${statsQC.reason}`);
          } else try {
            const ytRes  = await fetch(
              `https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${encodeURIComponent(channelId)}&key=${process.env.YOUTUBE_API_KEY}`
            );
            const ytData = await ytRes.json();
            const s = ytData.items?.[0]?.statistics;
            if (s) {
              stats = {
                subscriberCount: parseInt(s.subscriberCount || 0, 10),
                viewCount:       parseInt(s.viewCount       || 0, 10),
                videoCount:      parseInt(s.videoCount      || 0, 10),
              };
              logYoutubeQuota('channels', YOUTUBE_QUOTA_COSTS.channels, req.user.id).catch(() => {});
              // 3. Write to cache
              await statsCol.updateOne(
                { channelId },
                { $set: { channelId, ...stats, cachedAt: new Date().toISOString() } },
                { upsert: true }
              ).catch(() => {});
              // 4. Persist subscriber count back to user doc
              User.findByIdAndUpdate(
                user._id,
                { $set: { 'youtubeChannels.$[ch].subscriberCount': stats.subscriberCount } },
                { arrayFilters: [{ 'ch.channelId': channelId }] }
              ).catch(() => {});
            }
          } catch (e) {
            console.warn(`[Channels] YouTube stats fetch failed for ${channelId}:`, e.message);
          }
        }
      }

      return {
        channelId,
        channelName:      ch.channelName || 'My Channel',
        subscriberCount:  stats.subscriberCount,
        viewCount:        stats.viewCount,
        videoCount:       stats.videoCount,
        nicheId:          ch.nicheId   || user.nicheId   || null,
        nicheName:        ch.nicheName || user.nicheName || null,
        paused:           ch.paused    || false,
        thumbnailsEnabled: ch.thumbnailsEnabled ?? null,
      };
    }));

    const quota        = planNicheQuota(user.plan);
    const changesUsed  = user.nicheChangesUsed || 0;
    const isUnlimited  = quota === Infinity;
    const remaining    = isUnlimited ? null : Math.max(0, quota - changesUsed);
    const planLocked   = user.plan === 'trial';

    res.json({
      channels,
      count: channels.length,
      plan: user.plan,
      nicheQuota: {
        plan:      user.plan,
        unlimited: isUnlimited,
        locked:    planLocked,
        quota:     isUnlimited ? 'unlimited' : quota,
        used:      changesUsed,
        remaining: isUnlimited ? null : remaining,
      },
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/channels/:channelId/check-thumbnail-eligibility
// Uses channels.list status.longUploadsStatus as a proxy for channel verification
// (same prerequisite as custom thumbnails). On first thumbnails.set success/failure
// the flag is updated more precisely by pipelineUploadToYouTube.
app.post('/api/channels/:channelId/check-thumbnail-eligibility', requireAuth, async (req, res) => {
  try {
    const { channelId } = req.params;
    const user = await User.findById(req.user.id);
    const ch = (user?.youtubeChannels || []).find(c => c.channelId === channelId);
    if (!ch) return res.status(404).json({ error: 'Channel not found' });
    if (!ch.accessToken) return res.json({ thumbnailsEnabled: null, reason: 'no_token' });

    const eligQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.channels, req.user.id).catch(() => ({ allowed: true }));
    if (!eligQC.allowed) {
      console.warn(`[Quota] check-thumbnail-eligibility blocked for ${req.user.id}: ${eligQC.reason}`);
      return res.json({ thumbnailsEnabled: null, reason: 'quota_limited' });
    }

    const { google } = require('googleapis');
    const oauth2 = new google.auth.OAuth2(process.env.YOUTUBE_CLIENT_ID, process.env.YOUTUBE_CLIENT_SECRET);
    oauth2.setCredentials({ access_token: ch.accessToken, refresh_token: ch.refreshToken });
    const yt = google.youtube({ version: 'v3', auth: oauth2 });

    const ytRes = await yt.channels.list({ part: ['status', 'contentDetails'], mine: true });
    const status = ytRes.data.items?.[0]?.status || {};
    // longUploadsStatus 'allowed'|'eligible' → channel is verified (same gate as custom thumbnails)
    const eligible = ['allowed', 'eligible'].includes(status.longUploadsStatus);

    await User.findOneAndUpdate(
      { _id: req.user.id, 'youtubeChannels.channelId': channelId },
      { $set: { 'youtubeChannels.$.thumbnailsEnabled': eligible } }
    ).catch(() => {});

    logYoutubeQuota('channels.list', YOUTUBE_QUOTA_COSTS.channels, req.user.id).catch(() => {});
    res.json({ thumbnailsEnabled: eligible, longUploadsStatus: status.longUploadsStatus });
  } catch (err) {
    console.warn('[ThumbnailCheck] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/channels/pending — list channels fetched from Google OAuth, not yet confirmed
app.get('/api/channels/pending', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    res.json({ channels: user.pendingOAuthChannels || [] });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/channels/confirm — user selects one channel from the OAuth picker
app.post('/api/channels/confirm', requireAuth, async (req, res) => {
  try {
    const { channelId } = req.body;
    if (!channelId) return res.status(400).json({ error: 'channelId required' });

    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    // Find in pending list
    const pending = (user.pendingOAuthChannels || []).find(c => c.channelId === channelId);
    if (!pending) return res.status(400).json({ error: 'Channel not in your pending list. Please reconnect your Google account.' });

    // Plan limit check
    const limit = (PLAN_CONFIG[user.plan] || PLAN_CONFIG.trial).channels;
    const currentCount = (user.youtubeChannels || []).length;
    if (currentCount >= limit) {
      return res.status(403).json({
        error: "You've reached your plan's channel limit. Upgrade to add more channels.",
        code: 'CHANNEL_LIMIT',
      });
    }

    // Global uniqueness — no other account can have this channel
    const duplicate = await User.findOne({ 'youtubeChannels.channelId': channelId, _id: { $ne: user._id } });
    if (duplicate) {
      return res.status(409).json({
        error: 'This channel is already connected to another account.',
        code: 'DUPLICATE_CHANNEL',
      });
    }

    // Already connected to this account — just refresh tokens silently
    const existing = (user.youtubeChannels || []).find(c => c.channelId === channelId);
    if (existing) {
      existing.accessToken  = user.googleAccessToken || existing.accessToken;
      existing.refreshToken = user.googleRefreshToken || existing.refreshToken;
      existing.channelName  = pending.channelName;
      existing.thumbnail    = pending.thumbnail;
      existing.subscriberCount = pending.subscriberCount;
    } else {
      user.youtubeChannels.push({
        channelId,
        channelName:      pending.channelName,
        thumbnail:        pending.thumbnail,
        subscriberCount:  pending.subscriberCount,
        accessToken:      user.googleAccessToken  || '',
        refreshToken:     user.googleRefreshToken || '',
        tiktokEnabled:    false,
        instagramEnabled: false,
        connectedAt:      new Date().toISOString(),
      });
    }

    // Trial channel check
    if (user.plan === 'trial' && !existing) {
      const triedBefore = await TriedChannel.findOne({ channelId });
      if (triedBefore) {
        return res.status(403).json({
          error: 'This YouTube channel has already used a free trial. Connect a different channel or subscribe to continue.',
          code: 'TRIAL_USED',
        });
      }
      await TriedChannel.findOneAndUpdate({ channelId }, { channelId }, { upsert: true });
    }

    // Clear pending list
    user.pendingOAuthChannels = [];
    user.markModified('youtubeChannels');
    await user.save();

    console.log(`[OAuth] Channel confirmed: ${pending.channelName} (${channelId}) for user ${user.email}`);
    res.json({ success: true, channel: { channelId, channelName: pending.channelName, thumbnail: pending.thumbnail, subscriberCount: pending.subscriberCount } });

    // Async: check thumbnail eligibility right after a channel is confirmed
    setImmediate(async () => {
      try {
        const freshUser = await User.findById(user._id);
        const ch = (freshUser?.youtubeChannels || []).find(c => c.channelId === channelId);
        if (!ch?.accessToken) return;
        const asyncQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.channels, String(user._id)).catch(() => ({ allowed: true }));
        if (!asyncQC.allowed) {
          console.warn(`[Quota] async channels.list skipped for ${channelId}: ${asyncQC.reason}`);
          return;
        }
        const { google } = require('googleapis');
        const oauth2 = new google.auth.OAuth2(process.env.YOUTUBE_CLIENT_ID, process.env.YOUTUBE_CLIENT_SECRET);
        oauth2.setCredentials({ access_token: ch.accessToken, refresh_token: ch.refreshToken });
        const yt = google.youtube({ version: 'v3', auth: oauth2 });
        const ytRes = await yt.channels.list({ part: ['status'], mine: true });
        logYoutubeQuota('channels.list', YOUTUBE_QUOTA_COSTS.channels, String(user._id)).catch(() => {});
        const status = ytRes.data.items?.[0]?.status || {};
        const eligible = ['allowed', 'eligible'].includes(status.longUploadsStatus);
        await User.findOneAndUpdate(
          { _id: user._id, 'youtubeChannels.channelId': channelId },
          { $set: { 'youtubeChannels.$.thumbnailsEnabled': eligible } }
        );
        console.log(`[ThumbnailCheck] ${channelId} thumbnailsEnabled=${eligible} (longUploadsStatus=${status.longUploadsStatus})`);
      } catch (e) {
        console.warn(`[ThumbnailCheck] Async check failed for ${channelId}:`, e.message);
      }
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/channels/connect — connect a YouTube channel
app.post('/api/channels/connect', requireAuth, async (req, res) => {
  try {
    const { channelId, channelName, accessToken, refreshToken } = req.body;
    if (!channelId || !channelName) return res.status(400).json({ error: 'channelId and channelName required' });

    // Enforce: one trial per YouTube channel ID
    const triedBefore = await TriedChannel.findOne({ channelId });
    const user = await User.findById(req.user.id);

    if (triedBefore && user.plan === 'trial') {
      return res.status(403).json({ error: 'This YouTube channel has already used a free trial. Connect a different channel or subscribe to continue.', code: 'TRIAL_USED' });
    }

    // Record channel as trial-used
    if (user.plan === 'trial') {
      await TriedChannel.findOneAndUpdate({ channelId }, { channelId }, { upsert: true });
    }

    // Check channel limit per plan
    const limit = (PLAN_CONFIG[user.plan] || PLAN_CONFIG.trial).channels;
    if ((user.youtubeChannels || []).length >= limit) {
      return res.status(403).json({ error: `Your plan allows up to ${limit} channel(s). Upgrade to connect more.`, code: 'CHANNEL_LIMIT' });
    }

    const already = user.youtubeChannels?.find(c => c.channelId === channelId);
    if (!already) {
      user.youtubeChannels.push({ channelId, channelName, accessToken: accessToken || '', refreshToken: refreshToken || '', tiktokEnabled: false, instagramEnabled: false });
      await user.save();
    }

    res.json({ success: true, channel: { channelId, channelName } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// PATCH /api/channels/:channelId/platforms — toggle TikTok / Instagram
app.patch('/api/channels/:channelId/platforms', requireAuth, async (req, res) => {
  try {
    const { tiktokEnabled, instagramEnabled } = req.body;
    const user = await User.findById(req.user.id);
    const ch = user.youtubeChannels?.find(c => c.channelId === req.params.channelId);
    if (!ch) return res.status(404).json({ error: 'Channel not found' });
    if (tiktokEnabled !== undefined) ch.tiktokEnabled = !!tiktokEnabled;
    if (instagramEnabled !== undefined) ch.instagramEnabled = !!instagramEnabled;
    await user.save();
    res.json({ success: true, channel: ch });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/channel/:id/style — return current styleConfig for one channel
app.get('/api/channel/:id/style', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id).lean();
    const ch = (user?.youtubeChannels || []).find(c => c.channelId === req.params.id);
    if (!ch) return res.status(404).json({ error: 'Channel not found' });
    res.json({ styleConfig: ch.styleConfig || {} });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// PATCH /api/channel/:id/style — update styleConfig for one channel
app.patch('/api/channel/:id/style', requireAuth, async (req, res) => {
  try {
    const { captionFont, colorScheme, musicGenre, introText } = req.body;
    const VALID = {
      captionFont: ['clean', 'bold', 'impact'],
      colorScheme: ['white-yellow', 'white-green', 'white-orange'],
      musicGenre:  ['ambient', 'upbeat', 'cinematic', 'lofi', 'none'],
    };
    if (captionFont !== undefined && !VALID.captionFont.includes(captionFont))
      return res.status(400).json({ error: `Invalid captionFont — must be one of: ${VALID.captionFont.join(', ')}` });
    if (colorScheme !== undefined && !VALID.colorScheme.includes(colorScheme))
      return res.status(400).json({ error: `Invalid colorScheme — must be one of: ${VALID.colorScheme.join(', ')}` });
    if (musicGenre !== undefined && !VALID.musicGenre.includes(musicGenre))
      return res.status(400).json({ error: `Invalid musicGenre — must be one of: ${VALID.musicGenre.join(', ')}` });

    const update = {};
    if (captionFont !== undefined) update['youtubeChannels.$.styleConfig.captionFont'] = captionFont;
    if (colorScheme !== undefined) update['youtubeChannels.$.styleConfig.colorScheme'] = colorScheme;
    if (musicGenre  !== undefined) update['youtubeChannels.$.styleConfig.musicGenre']  = musicGenre;
    if (introText   !== undefined) update['youtubeChannels.$.styleConfig.introText']   = String(introText).slice(0, 80).trim();

    if (!Object.keys(update).length) return res.status(400).json({ error: 'No valid fields provided' });

    const result = await User.updateOne(
      { _id: req.user.id, 'youtubeChannels.channelId': req.params.id },
      { $set: update }
    );
    if (!result.matchedCount) return res.status(404).json({ error: 'Channel not found' });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// =============================================================================
// NICHE ROUTES
// =============================================================================

// POST /api/niche/set — set or change niche (enforces quota post-setup)
app.post('/api/niche/set', requireAuth, async (req, res) => {
  try {
    const { nicheId, nicheName } = req.body;
    if (!nicheId || !nicheName) return res.status(400).json({ error: 'nicheId and nicheName required' });

    const user = await User.findById(req.user.id);
    const isFirstSet = !user.nicheId;

    if (!isFirstSet) {
      // Post-setup change — enforce plan quota
      const quota = planNicheQuota(user.plan);
      if (quota === 0) return res.status(403).json({ error: 'Niche changes are not available on the trial plan. Upgrade to a paid plan to change your niche.', code: 'NICHE_LOCKED' });

      user.nicheChangesUsed = (user.nicheChangesUsed || 0) + 1;
    }

    user.nicheId = nicheId;
    user.nicheName = nicheName;
    await user.save();

    const quota = planNicheQuota(user.plan);
    const changesRemaining = quota === Infinity ? null : Math.max(0, quota - user.nicheChangesUsed);
    res.json({ success: true, nicheId, nicheName, changesUsed: user.nicheChangesUsed, changesRemaining });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/niche/status — get current niche + change allowance
app.get('/api/niche/status', requireAuth, async (req, res) => {
  try {
    const user  = await User.findById(req.user.id);
    const quota = planNicheQuota(user.plan);
    const changesUsed = user.nicheChangesUsed || 0;
    const changesRemaining = quota === Infinity ? null : Math.max(0, quota - changesUsed);
    res.json({ nicheId: user.nicheId, nicheName: user.nicheName, quota: quota === Infinity ? 'unlimited' : quota, changesUsed, changesRemaining, plan: user.plan });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// =============================================================================
// PADDLE BILLING ROUTES
// =============================================================================

const PADDLE_BASE       = process.env.PADDLE_ENV === 'production' ? 'https://api.paddle.com' : 'https://sandbox-api.paddle.com';
const PADDLE_LOG_PREFIX = process.env.PADDLE_ENV === 'production' ? '[Paddle]' : '[Paddle Sandbox]';

// Paddle API helper — uses axios already present in the codebase
async function paddleApi(method, path, body = null) {
  const key = process.env.PADDLE_API_KEY;
  if (!key) throw new Error('PADDLE_API_KEY is not set in environment variables');
  const opts = {
    method,
    url: `${PADDLE_BASE}${path}`,
    headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
  };
  if (body !== null) opts.data = body;
  const res = await axios(opts);
  return res.data;
}

// Resolve plan name from Paddle price ID
function planFromPaddlePriceId(priceId) {
  if (!priceId) return null;
  for (const [plan, cfg] of Object.entries(PLAN_CONFIG)) {
    if (cfg.paddlePriceId && cfg.paddlePriceId === priceId) return plan;
  }
  return null;
}

// Verify Paddle webhook signature (HMAC-SHA256, header: ts=...;h1=...)
function verifyPaddleWebhook(rawBody, signatureHeader) {
  const secret = process.env.PADDLE_WEBHOOK_SECRET;
  if (!secret || !signatureHeader) return false;
  try {
    const ts      = signatureHeader.split(';').find(p => p.startsWith('ts='))?.slice(3);
    const h1      = signatureHeader.split(';').find(p => p.startsWith('h1='))?.slice(3);
    if (!ts || !h1) return false;
    const expected = crypto.createHmac('sha256', secret).update(`${ts}:${rawBody}`).digest('hex');
    return crypto.timingSafeEqual(Buffer.from(expected, 'hex'), Buffer.from(h1, 'hex'));
  } catch { return false; }
}

// GET /api/billing/config — public: Paddle client token + env for frontend init
app.get('/api/billing/config', (req, res) => {
  res.json({
    clientToken: process.env.PADDLE_CLIENT_TOKEN || '',
    environment: process.env.PADDLE_ENV          || 'sandbox',
  });
});

// GET /api/billing/plans — public: all plan details with prices
app.get('/api/billing/plans', (req, res) => {
  const plans = Object.entries(PLAN_CONFIG).map(([id, cfg]) => ({
    id,
    price:                     cfg.price,
    videosPerDayPerChannel:    cfg.shortsPerDay,
    maxChannels:               cfg.channels,
    longformPerWeekPerChannel: cfg.longFormPerWeek,
    paddlePriceId:             cfg.paddlePriceId || null,
  }));
  res.json({ plans });
});

// GET /api/billing/subscription — user's current billing status
app.get('/api/billing/subscription', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id).lean();
    const cfg  = PLAN_CONFIG[user.plan] || PLAN_CONFIG.trial;
    res.json({
      plan:                 user.plan,
      planPrice:            cfg.price,
      subscriptionStatus:   user.subscriptionStatus || 'trial',
      subscriptionEndDate:  user.subscriptionEndDate  || null,
      subscriptionRenewsAt: user.subscriptionRenewsAt || null,
      trialEndsAt:          user.trialEndsAt           || null,
      paddleSubscriptionId: user.paddleSubscriptionId  || null,
      paddleCustomerId:     user.paddleCustomerId       || null,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/billing/create-checkout — returns priceId + customData for Paddle.js overlay
app.post('/api/billing/create-checkout', requireAuth, async (req, res) => {
  try {
    const { planId } = req.body;
    const cfg = PLAN_CONFIG[planId];
    if (!cfg || !cfg.paddlePriceId) {
      return res.status(400).json({ error: `Unknown plan or Paddle price not configured: ${planId}` });
    }
    console.log(`${PADDLE_LOG_PREFIX} Checkout initiated — user ${req.user.id} → ${planId} (${cfg.paddlePriceId})`);
    res.json({
      priceId:    cfg.paddlePriceId,
      customData: { userId: String(req.user.id), planId },
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/billing/cancel — cancel subscription at period end via Paddle API
app.post('/api/billing/cancel', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user.paddleSubscriptionId) {
      return res.status(400).json({ error: 'No active Paddle subscription found on this account' });
    }
    const data = await paddleApi('PATCH', `/subscriptions/${user.paddleSubscriptionId}`, {
      scheduled_change: { action: 'cancel', effective_at: 'next_billing_period' },
    });
    await User.findByIdAndUpdate(req.user.id, { subscriptionStatus: 'cancelled' });
    console.log(`${PADDLE_LOG_PREFIX} Subscription cancellation scheduled — user ${user.email}`);
    res.json({ success: true, message: 'Subscription will cancel at the end of the current billing period', data: data?.data || null });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/billing/resume — remove scheduled cancellation to resume subscription
app.post('/api/billing/resume', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user.paddleSubscriptionId) {
      return res.status(400).json({ error: 'No Paddle subscription found on this account' });
    }
    const data = await paddleApi('PATCH', `/subscriptions/${user.paddleSubscriptionId}`, {
      scheduled_change: null,
    });
    await User.findByIdAndUpdate(req.user.id, { subscriptionStatus: 'active' });
    console.log(`${PADDLE_LOG_PREFIX} Subscription resumed — user ${user.email}`);
    res.json({ success: true, message: 'Subscription resumed successfully', data: data?.data || null });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/billing/portal — Paddle customer portal URL for managing payment methods
app.get('/api/billing/portal', requireAuth, async (req, res) => {
  try {
    const user       = await User.findById(req.user.id).lean();
    const portalBase = process.env.PADDLE_ENV === 'production'
      ? 'https://customer.paddle.com'
      : 'https://sandbox-customer.paddle.com';
    const portalUrl  = `${portalBase}/?email=${encodeURIComponent(user.email)}`;
    res.json({ portalUrl });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/billing/promo — internal promo code bypass (VRL-X9K2-M7QP-4TZW)
// Always grants Agency plan for 1 year — plan in body is ignored
app.post('/api/billing/promo', requireAuth, async (req, res) => {
  try {
    const { promoCode } = req.body;
    if (promoCode !== 'VRL-X9K2-M7QP-4TZW') return res.status(400).json({ success: false, error: 'Invalid promo code' });
    const plan   = 'agency'; // matches PLAN_LIMITS key exactly
    const limits = getPlanLimits(plan);  // { videosPerDayPerChannel:10, maxChannels:4, longformEnabled:true }
    const endDate = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);
    await User.findByIdAndUpdate(req.user.id, {
      plan,
      subscriptionStatus:     'active',
      subscriptionStartDate:  new Date(),
      subscriptionEndDate:    endDate,
      subscriptionRenewsAt:   endDate,
      videosPerDayPerChannel: limits.videosPerDayPerChannel,
      maxChannels:            limits.maxChannels,
      longformEnabled:        limits.longformEnabled,
    });
    console.log(`[Promo] User ${req.user.email} upgraded to ${plan} via promo code (expires ${endDate.toISOString().slice(0,10)})`);
    res.json({ success: true, plan, subscriptionStatus: 'active', subscriptionEndDate: endDate });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// POST /api/paddle/webhook — Paddle webhook handler (raw body for signature verification)
// Responds 200 immediately then processes asynchronously
app.post('/api/paddle/webhook', express.raw({ type: '*/*' }), async (req, res) => {
  res.status(200).json({ received: true }); // Acknowledge before processing

  const sig     = req.headers['paddle-signature'];
  const rawBody = req.body?.toString('utf8') || '';

  if (!verifyPaddleWebhook(rawBody, sig)) {
    console.warn(`${PADDLE_LOG_PREFIX} ⚠ Webhook signature verification FAILED — payload ignored`);
    return;
  }

  let event;
  try { event = JSON.parse(rawBody); } catch { console.warn(`${PADDLE_LOG_PREFIX} Webhook JSON parse error`); return; }

  const eventType  = event.event_type || 'unknown';
  const data       = event.data        || {};
  const customData = data.custom_data  || {};
  const userId     = customData.userId || null;

  console.log(`${PADDLE_LOG_PREFIX} Webhook received: ${eventType} | userId: ${userId || 'N/A'} | subId: ${data.id || 'N/A'}`);

  try {
    switch (eventType) {

      case 'subscription.activated':
      case 'subscription.created': {
        const priceId = data.items?.[0]?.price?.id || null;
        const plan    = planFromPaddlePriceId(priceId) || customData.planId || 'starter';
        const limits  = getPlanLimits(plan);
        const endDate = data.current_billing_period?.ends_at
          ? new Date(data.current_billing_period.ends_at)
          : data.next_billed_at
            ? new Date(data.next_billed_at)
            : new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);
        if (userId) {
          await User.findByIdAndUpdate(userId, {
            plan,
            subscriptionStatus:     'active',
            subscriptionStartDate:  new Date(),
            subscriptionEndDate:    endDate,
            subscriptionRenewsAt:   endDate,
            paddleSubscriptionId:   data.id          || null,
            paddleCustomerId:       data.customer_id || null,
            videosPerDayPerChannel: limits.videosPerDayPerChannel,
            maxChannels:            limits.maxChannels,
            longformEnabled:        limits.longformEnabled,
          });
          console.log(`${PADDLE_LOG_PREFIX} ${eventType} ✓ — user ${userId} → ${plan} (renews ${endDate.toISOString().slice(0,10)})`);
        } else {
          console.warn(`${PADDLE_LOG_PREFIX} ${eventType} — no userId in customData`);
        }
        break;
      }

      case 'subscription.updated': {
        const priceId   = data.items?.[0]?.price?.id || null;
        const newPlan   = planFromPaddlePriceId(priceId);
        const endDate   = data.current_billing_period?.ends_at
          ? new Date(data.current_billing_period.ends_at)
          : data.next_billed_at ? new Date(data.next_billed_at) : null;
        const statusMap = { active: 'active', past_due: 'past_due', canceled: 'cancelled', trialing: 'trial', paused: 'cancelled' };
        const newStatus = statusMap[data.status] || 'active';
        const user      = userId
          ? await User.findById(userId).lean()
          : await User.findOne({ paddleSubscriptionId: data.id }).lean();
        if (user) {
          const resolvedPlan = newPlan || user.plan;
          const limits = getPlanLimits(resolvedPlan);
          const update = {
            subscriptionStatus:     newStatus,
            videosPerDayPerChannel: limits.videosPerDayPerChannel,
            maxChannels:            limits.maxChannels,
            longformEnabled:        limits.longformEnabled,
          };
          if (newPlan) update.plan = newPlan;
          if (endDate) { update.subscriptionEndDate = endDate; update.subscriptionRenewsAt = endDate; }
          await User.findByIdAndUpdate(user._id, update);
          console.log(`${PADDLE_LOG_PREFIX} subscription.updated ✓ — user ${user.email} → status:${newStatus}${newPlan ? ' plan:'+newPlan : ''}`);
        }
        break;
      }

      case 'subscription.canceled': {
        const endAt = data.scheduled_change?.effective_at
          || data.current_billing_period?.ends_at
          || null;
        const endDate = endAt ? new Date(endAt) : null;
        const user    = userId
          ? await User.findById(userId).lean()
          : await User.findOne({ paddleSubscriptionId: data.id }).lean();
        if (user) {
          const update = { subscriptionStatus: 'cancelled' };
          if (endDate) update.subscriptionEndDate = endDate;
          await User.findByIdAndUpdate(user._id, update);
          console.log(`${PADDLE_LOG_PREFIX} subscription.canceled ✓ — user ${user.email} (access until ${endDate?.toISOString().slice(0,10) || 'unknown'})`);
        }
        break;
      }

      case 'subscription.past_due': {
        const user = userId
          ? await User.findById(userId).lean()
          : await User.findOne({ paddleSubscriptionId: data.id }).lean();
        if (user) {
          const gracePeriodEnd = new Date(Date.now() + 3 * 24 * 60 * 60 * 1000);
          await User.findByIdAndUpdate(user._id, {
            subscriptionStatus: 'past_due',
            pastDueSince:       new Date(),
          });
          agentCol('adminAlerts').insertOne({
            type: 'paddle_payment_failed', userEmail: user.email,
            userId: String(user._id), timestamp: new Date().toISOString(),
            gracePeriodEnd: gracePeriodEnd.toISOString(), resolved: false,
          }).catch(() => {});
          console.warn(`${PADDLE_LOG_PREFIX} subscription.past_due ⚠ — user ${user.email} (grace until ${gracePeriodEnd.toISOString().slice(0,10)})`);
        }
        break;
      }

      case 'transaction.completed': {
        const user = userId
          ? await User.findById(userId).lean()
          : data.customer_id
            ? await User.findOne({ paddleCustomerId: data.customer_id }).lean()
            : null;
        if (user) {
          const amount = (data.details?.totals?.grand_total || 0) / 100;
          await User.findByIdAndUpdate(user._id, {
            lastPaymentDate: new Date(),
            $push: { billingHistory: { date: new Date(), amount, plan: user.plan, status: 'paid', invoiceId: data.id || null } },
          });
          console.log(`${PADDLE_LOG_PREFIX} transaction.completed ✓ — user ${user.email} $${amount.toFixed(2)}`);
        }
        break;
      }

      default:
        console.log(`${PADDLE_LOG_PREFIX} Unhandled event type: ${eventType}`);
    }
  } catch (err) {
    console.error(`${PADDLE_LOG_PREFIX} Webhook handler error [${eventType}]:`, err.message);
  }
});

// =============================================================================
// NICHE ENGINE V2 — inline routes (replaces server_m4a_routes.js)
// =============================================================================
// NicheEngineV2 — guard against file not found during deploy
let nicheEngine;
try {
  const { NicheEngineV2 } = require('./niche_engine_v2_bridge');
  nicheEngine = new NicheEngineV2();
  console.log('[NicheEngine] v2 loaded OK');
} catch (err) {
  console.warn('[NicheEngine] niche_engine_v2_bridge.js not found — niche routes will return 503:', err.message);
  // Stub so niche routes return a clean error instead of crashing
  nicheEngine = new Proxy({}, {
    get(_, method) {
      return () => { throw new Error('Niche engine not available — ensure niche_engine_v2_bridge.js is in the repo'); };
    }
  });
}

// =============================================================================
// REAL-TIME NICHE DISCOVERY ENGINE
// YouTube API + OpenAI GPT-4o + Google Trends RSS — no hardcoded niches
// =============================================================================

// ── CPM benchmarks per category (for scoring when YouTube data is unavailable)
const NICHE_CPM_BENCHMARKS = {
  Finance: 20, Business: 16, Technology: 14, Health: 11,
  Education: 9, Fitness: 8, Lifestyle: 7, Entertainment: 5,
  Gaming: 4, Music: 4, default: 6,
};

// ── Discovery seed keywords — specific high-value topics to probe in parallel
const DISCOVERY_SEEDS = [
  'psychology facts', 'personal finance tips', 'ai tools', 'stoicism',
  'health longevity', 'entrepreneurship', 'true crime', 'mindfulness',
];

// ── Category base scores for the simplified scoring formula
const CATEGORY_BASE_SCORES = {
  Finance: 74, Technology: 75, Education: 72, Wellness: 71,
  Business: 74, Entertainment: 70, default: 68,
};

function slugify(str) {
  return str.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
}

function normaliseNicheScore(raw) {
  const n = ((raw - 40) / 60) * 40 + 60;
  return Math.round(Math.max(60, Math.min(100, n)) * 10) / 10;
}

// ── YouTube video search — returns enriched video objects
async function ytSearch(keyword, { days = 30, maxResults = 20, order = 'viewCount' } = {}) {
  const apiKey = process.env.YOUTUBE_API_KEY;
  if (!apiKey) {
    console.warn(`[Niche] ytSearch("${keyword}"): YOUTUBE_API_KEY not set`);
    return [];
  }
  // search.list = 100 units + videos.list = 1 unit
  const ytSearchQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.search + YOUTUBE_QUOTA_COSTS.default, null).catch(() => ({ allowed: true }));
  if (!ytSearchQC.allowed) {
    console.warn(`[Quota] ytSearch("${keyword}") blocked — ${ytSearchQC.reason}`);
    return [];
  }
  try {
    const publishedAfter = new Date(Date.now() - days * 86_400_000).toISOString();
    const searchParams = new URLSearchParams({
      part: 'snippet', q: keyword, type: 'video', order,
      publishedAfter, maxResults: String(maxResults), key: apiKey,
    });
    const searchUrl = `https://www.googleapis.com/youtube/v3/search?${searchParams}`;
    console.log('[Niche] ytSearch calling YouTube API with key:', process.env.YOUTUBE_API_KEY ? process.env.YOUTUBE_API_KEY.slice(0, 8) + '...' : 'MISSING');
    const searchResp = await axios.get(searchUrl, { timeout: 12_000 });
    console.log(`[Niche] ytSearch("${keyword}"): HTTP ${searchResp.status}, items=${searchResp.data.items?.length ?? 0}`);
    const items = searchResp.data.items || [];
    console.log('[Niche] YouTube returned', items?.length, 'items for query:', keyword);
    if (items.length > 0) {
      console.log(`[Niche] ytSearch("${keyword}"): first result title = "${items[0].snippet?.title}"`);
    }
    const videoIds = items.map(i => i.id?.videoId).filter(Boolean);
    if (!videoIds.length) {
      console.warn(`[Niche] ytSearch("${keyword}"): 0 video IDs returned (order=${order}, days=${days})`);
      return [];
    }

    const statsUrl = `https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id=${videoIds.join(',')}&key=${apiKey}`;
    const statsResp = await axios.get(statsUrl, { timeout: 12_000 });
    const videos = (statsResp.data.items || []).map(v => ({
      videoId:     v.id,
      title:       v.snippet?.title        || '',
      channel:     v.snippet?.channelTitle || '',
      tags:        v.snippet?.tags         || [],
      views:       parseInt(v.statistics?.viewCount    || '0', 10),
      likes:       parseInt(v.statistics?.likeCount    || '0', 10),
      publishedAt: v.snippet?.publishedAt  || '',
    }));
    logYoutubeQuota('search', YOUTUBE_QUOTA_COSTS.search, null).catch(() => {});
    logYoutubeQuota('videos.list', YOUTUBE_QUOTA_COSTS.default, null).catch(() => {});
    console.log(`[Niche] ytSearch("${keyword}"): ${videos.length} videos enriched, top views=${videos[0]?.views ?? 0}`);
    return videos;
  } catch (err) {
    const ytErr = err.response?.data?.error?.message || err.message;
    console.error(`[Niche] ytSearch("${keyword}") failed: ${ytErr}`);
    if (err.response?.data) console.error(`[Niche] ytSearch full error:`, JSON.stringify(err.response.data).slice(0, 400));
    return [];
  }
}

// ── YouTube autocomplete suggestions (no quota cost)
async function ytSuggestions(keyword) {
  try {
    const url = `https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=${encodeURIComponent(keyword)}&hl=en`;
    const resp = await axios.get(url, { timeout: 6_000, headers: { 'User-Agent': 'Mozilla/5.0' } });
    const data = Array.isArray(resp.data) ? resp.data : [];
    const suggestions = Array.isArray(data[1]) ? data[1].slice(0, 8) : [];
    return suggestions.filter(s => typeof s === 'string' && s !== keyword);
  } catch (err) {
    console.warn('[Niche] ytSuggestions failed:', err.message);
    return [];
  }
}

// ── Google Trends RSS — trending search terms (US, cached 1h in memory)
const _trendsMemCache = { terms: [], fetchedAt: 0 };
async function fetchGoogleTrends() {
  if (Date.now() - _trendsMemCache.fetchedAt < 3_600_000) return _trendsMemCache.terms;
  try {
    const resp = await axios.get('https://trends.google.com/trends/trendingsearches/daily/rss?geo=US', {
      timeout: 8_000,
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Viralityy/1.0)' },
    });
    const xml  = typeof resp.data === 'string' ? resp.data : '';
    const terms = [];
    const rx    = /<title>(?:<!\[CDATA\[)?([^\]<]+)(?:\]\]>)?<\/title>/g;
    let m; let skip = true;
    while ((m = rx.exec(xml)) !== null) {
      if (skip) { skip = false; continue; } // skip feed <title>
      terms.push(m[1].toLowerCase().trim());
      if (terms.length >= 30) break;
    }
    _trendsMemCache.terms     = terms;
    _trendsMemCache.fetchedAt = Date.now();
    return terms;
  } catch (err) {
    console.warn('[Niche] fetchGoogleTrends failed:', err.message);
    return _trendsMemCache.terms; // stale is fine
  }
}

// ── OpenAI niche clustering — returns array of niche objects
async function clusterWithAI(videos, keyword) {
  const openaiKey = process.env.OPENAI_API_KEY;
  if (!openaiKey) {
    console.warn('[Niche] clusterWithAI: OPENAI_API_KEY not set');
    return null;
  }
  if (!videos.length) {
    console.warn(`[Niche] clusterWithAI("${keyword}"): no videos to cluster`);
    return null;
  }
  try {
    const { OpenAI } = require('openai');
    const openai = new OpenAI({ apiKey: openaiKey });
    const topVids = videos
      .sort((a, b) => b.views - a.views)
      .slice(0, 20)
      .map(v => `"${v.title.replace(/"/g, "'")}"`)
      .join(', ');

    console.log(`[Niche] clusterWithAI("${keyword}"): sending ${Math.min(videos.length, 20)} titles to GPT-4o-mini`);
    console.log('[Niche] Sending', videos.length, 'videos to OpenAI for clustering');

    const prompt = `Here are the top trending YouTube videos for the keyword "${keyword}": ${topVids}. Identify the top 5 distinct content niches these videos belong to. For each niche return a JSON array with: name, category, why_its_trending, competition (low/medium/high), cpm_range (e.g. '$5-$18'), trend (Hot/Rising/Stable). Return ONLY a valid JSON array, no other text.`;

    const resp = await openai.chat.completions.create({
      model:       'gpt-4o-mini',
      messages:    [{ role: 'user', content: prompt }],
      temperature: 0.3,
      max_tokens:  1200,
    });
    logAPIUsage('openai', 'niche_cluster', 'system', resp.usage?.total_tokens || 0,
      ((resp.usage?.prompt_tokens || 0) * API_COSTS.openai_input) +
      ((resp.usage?.completion_tokens || 0) * API_COSTS.openai_output), true);

    const rawText = resp.choices[0]?.message?.content?.trim() || '[]';
    console.log('[Niche] OpenAI returned:', JSON.stringify(rawText).slice(0, 200));
    const cleaned = rawText.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();
    let parsed;
    try {
      parsed = JSON.parse(cleaned);
    } catch (parseErr) {
      console.error(`[Niche] clusterWithAI("${keyword}"): JSON parse failed — raw response:\n${rawText.slice(0, 500)}`);
      return null;
    }
    console.log(`[Niche] clusterWithAI("${keyword}"): got ${parsed.length} niches from AI`);
    return parsed;
  } catch (err) {
    console.error('[Niche] clusterWithAI failed:', err.message);
    return null;
  }
}

// ── Score a single AI-identified niche using category base + view boost + trend boost
function scoreNiche(niche, allVideos, trendingTerms) {
  const category = niche.category || 'Other';

  // Base score from category
  const base = CATEGORY_BASE_SCORES[category] ?? CATEGORY_BASE_SCORES.default;

  // View boost: based on the top video in the pool
  const maxViews = allVideos.length ? Math.max(...allVideos.map(v => v.views)) : 0;
  const viewBoost = maxViews > 1_000_000 ? 10 : maxViews > 500_000 ? 7 : maxViews > 100_000 ? 5 : 0;

  // Trend boost: niche name keyword appears in Google Trends RSS
  const nameWords = niche.name.toLowerCase().split(/\s+/).filter(w => w.length >= 4);
  const trendBoost = trendingTerms.some(t => nameWords.some(w => t.includes(w))) ? 8 : 0;

  const raw            = base + viewBoost + trendBoost;
  const combined_score = normaliseNicheScore(raw);

  return {
    niche_id:        slugify(niche.name),
    name:            niche.name,
    category,
    combined_score,
    why_its_trending: niche.why_its_trending || '',
    cpm_range:        niche.cpm_range         || `$${(NICHE_CPM_BENCHMARKS[category] || 6) - 2}–$${(NICHE_CPM_BENCHMARKS[category] || 6) + 5}`,
    competition:      niche.competition       || 'medium',
    trend:            niche.trend             || 'Rising',
    score_breakdown:  { base, viewBoost, trendBoost, maxViews },
    live_data:        true,
    analysed_at:      new Date().toISOString(),
  };
}

// ── Core: discover niches for one keyword (YouTube + AI + Trends)
async function discoverNiches(keyword, plan = 'combo_pro') {
  console.log(`[Niche] discoverNiches("${keyword}") start`);

  const run = async () => {
    // 1. YouTube search for main keyword
    const mainVideos = await ytSearch(keyword, { days: 7, maxResults: 25 });

    // 2. Suggestions → pick top 4 → search each
    const suggestions = await ytSuggestions(keyword);
    const relVideos   = await Promise.all(
      suggestions.slice(0, 4).map(kw => ytSearch(kw, { days: 7, maxResults: 10 }).catch(() => []))
    );

    const allVideos     = [...mainVideos, ...relVideos.flat()];
    const trendingTerms = await fetchGoogleTrends();
    console.log(`[Niche] discoverNiches("${keyword}"): ${allVideos.length} total videos collected`);

    // 3. OpenAI clustering
    const aiNiches = await clusterWithAI(allVideos, keyword);
    if (!aiNiches || !aiNiches.length) {
      console.warn(`[Niche] discoverNiches("${keyword}"): AI returned no niches`);
      return [];
    }

    // 4. Score each AI niche, keep only those scoring >= 80
    const scored = aiNiches.map(n => scoreNiche(n, allVideos, trendingTerms));
    scored.sort((a, b) => b.combined_score - a.combined_score);
    const qualified = scored.filter(n => n.combined_score >= 80);
    console.log(`[Niche] discoverNiches("${keyword}"): ${scored.length} scored, ${qualified.length} >= 80`);
    return qualified.slice(0, 10);
  };

  try {
    return await Promise.race([
      run(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout after 25s')), 25_000)),
    ]);
  } catch (err) {
    console.error(`[Niche] discoverNiches("${keyword}") failed: ${err.message}`);
    return [];
  }
}

const STATIC_FALLBACK_NICHES = [
  { niche_id: 'psychology',       name: 'Psychology & Human Behaviour',  category: 'Education',        combined_score: 92, competition: 'low',    cpm_range: '$5-$18',  why_its_trending: 'High engagement evergreen content',        verdict: 'hot', live_data_used: false },
  { niche_id: 'finance',          name: 'Personal Finance Tips',          category: 'Finance',          combined_score: 93, competition: 'medium', cpm_range: '$12-$45', why_its_trending: 'High CPM recession-proof niche',            verdict: 'hot', live_data_used: false },
  { niche_id: 'stoicism',         name: 'Stoicism & Philosophy',          category: 'Self Improvement', combined_score: 91, competition: 'low',    cpm_range: '$5-$18',  why_its_trending: 'Growing mindfulness audience',              verdict: 'hot', live_data_used: false },
  { niche_id: 'ai_tools',         name: 'AI Tools & Productivity',        category: 'Technology',       combined_score: 95, competition: 'medium', cpm_range: '$10-$35', why_its_trending: 'Fastest growing YouTube category',         verdict: 'hot', live_data_used: false },
  { niche_id: 'health',           name: 'Health & Longevity',             category: 'Wellness',         combined_score: 90, competition: 'medium', cpm_range: '$7-$25',  why_its_trending: 'Post-pandemic health awareness',            verdict: 'hot', live_data_used: false },
  { niche_id: 'mindfulness',      name: 'Mindfulness & Meditation',       category: 'Wellness',         combined_score: 91, competition: 'low',    cpm_range: '$6-$22',  why_its_trending: 'Mental health awareness growing',           verdict: 'hot', live_data_used: false },
  { niche_id: 'entrepreneurship', name: 'Entrepreneurship & Startups',    category: 'Business',         combined_score: 92, competition: 'medium', cpm_range: '$10-$38', why_its_trending: 'Creator economy boom',                     verdict: 'hot', live_data_used: false },
  { niche_id: 'science',          name: 'Science & Space Facts',          category: 'Education',        combined_score: 90, competition: 'low',    cpm_range: '$5-$15',  why_its_trending: 'Curiosity-driven viral content',            verdict: 'hot', live_data_used: false },
  { niche_id: 'true_crime',       name: 'True Crime & Mysteries',         category: 'Entertainment',    combined_score: 91, competition: 'medium', cpm_range: '$5-$18',  why_its_trending: 'Consistently top YouTube category',        verdict: 'hot', live_data_used: false },
  { niche_id: 'language',         name: 'Language Learning',              category: 'Education',        combined_score: 90, competition: 'low',    cpm_range: '$6-$20',  why_its_trending: 'Global audience appeal',                   verdict: 'hot', live_data_used: false },
];

// Returns true if two niche names share their primary significant keyword
function nicheNamesSimilar(a, b) {
  const sig = s => s.toLowerCase().split(/\s+/).find(w => w.length >= 4) || s.toLowerCase().slice(0, 4);
  return sig(a) === sig(b);
}

// ── Top niches across all seed categories — cached 6h in MongoDB
async function discoverTopNiches(plan = 'combo_pro', forceRefresh = false) {
  console.log('[Niche] discoverTopNiches starting, YOUTUBE_API_KEY:', process.env.YOUTUBE_API_KEY ? 'SET (' + process.env.YOUTUBE_API_KEY.slice(0, 8) + '...)' : 'MISSING');
  const cacheCol = agentCol('niche_cache');
  const cacheKey = 'discover_cache';

  if (!forceRefresh) {
    const sixHoursAgo = new Date(Date.now() - 6 * 3_600_000).toISOString();
    const cached = await cacheCol.findOne({ cacheKey, cachedAt: { $gt: sixHoursAgo } }).catch(() => null);
    if (cached?.niches?.length) {
      console.log(`[Niche] discoverTopNiches: cache hit (${cached.niches.length} niches, age=${Math.round((Date.now() - new Date(cached.cachedAt)) / 60000)}min)`);
      return { niches: cached.niches, fromCache: true, cachedAt: cached.cachedAt };
    }
  }

  // Run all 8 seeds in parallel — never fail if one seed errors
  console.log(`[Niche] discoverTopNiches: running ${DISCOVERY_SEEDS.length} seeds with Promise.allSettled`);
  const settled = await Promise.allSettled(DISCOVERY_SEEDS.map(seed => discoverNiches(seed, plan)));
  const results = settled.map((r, i) => {
    if (r.status === 'rejected') {
      console.warn(`[Niche] seed "${DISCOVERY_SEEDS[i]}" rejected: ${r.reason?.message}`);
      return [];
    }
    return r.value || [];
  });

  // Combine, deduplicate by name similarity (highest score wins), sort, return top 10
  const all = results.flat().sort((a, b) => b.combined_score - a.combined_score);
  const deduped = [];
  for (const n of all) {
    if (!deduped.some(d => nicheNamesSimilar(d.name, n.name))) deduped.push(n);
  }
  const top = deduped.slice(0, 10);

  // Static fallback when APIs fail or all seeds score below threshold
  if (!top.length) {
    const fallbackReason = !process.env.YOUTUBE_API_KEY ? 'YOUTUBE_API_KEY missing' :
                           !process.env.OPENAI_API_KEY  ? 'OPENAI_API_KEY missing'  :
                           `all ${DISCOVERY_SEEDS.length} seeds scored below 90 or returned empty`;
    console.log('[Niche] FALLBACK TRIGGERED — reason:', fallbackReason);
    return { niches: STATIC_FALLBACK_NICHES, fromCache: false, cachedAt: new Date().toISOString(), fallback: true };
  }

  const cachedAt = new Date().toISOString();
  await cacheCol.updateOne(
    { cacheKey },
    { $set: { cacheKey, niches: top, cachedAt, plan, expiresAt: new Date(Date.now() + 6 * 3_600_000) } },
    { upsert: true }
  ).catch(() => {});

  console.log(`[Niche] discoverTopNiches: ${top.length} niches cached (scores: ${top.map(n => n.combined_score).join(', ')})`);
  return { niches: top, fromCache: false, cachedAt };
}

// ---------------------------------------------------------------------------
// GET /api/niches/recommend  — top niches for this user's plan
// ---------------------------------------------------------------------------
app.get('/api/niches/recommend', requireAuth, async (req, res) => {
  try {
    console.log('[Niche Route] YOUTUBE_API_KEY:', process.env.YOUTUBE_API_KEY ? 'SET (' + process.env.YOUTUBE_API_KEY.slice(0, 8) + '...)' : 'MISSING');
    console.log('[Niche Route] OPENAI_API_KEY:', process.env.OPENAI_API_KEY ? 'SET' : 'MISSING');
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const plan    = user.plan || 'shorts_starter';
    const refresh = req.query.refresh === 'true';
    const { niches, fromCache, cachedAt, fallback } = await discoverTopNiches(plan, refresh);
    const filtered = niches.filter(n => n.combined_score >= 80);
    res.json({ success: true, niches: filtered.length ? filtered : niches.slice(0, 10), count: niches.length, fromCache, cachedAt, fallback: !!fallback });
  } catch (err) {
    console.error('[Niche] /api/niches/recommend error:', err);
    res.status(500).json({ error: 'Failed to fetch niche recommendations' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/niches/browse  — all discovered niches sorted by score
// ---------------------------------------------------------------------------
app.get('/api/niches/browse', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const plan  = user.plan || 'shorts_starter';
    const { niches, fromCache, cachedAt } = await discoverTopNiches(plan);
    res.json({ success: true, niches, count: niches.length, fromCache, cachedAt });
  } catch (err) {
    console.error('[Niche] /api/niches/browse error:', err);
    res.status(500).json({ error: 'Failed to browse niches' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/search  — keyword-specific real-time discovery (no static fallback)
// Body: { keyword: "stoicism", plan: "combo_pro" }
// ---------------------------------------------------------------------------
app.post('/api/niches/search', requireAuth, async (req, res) => {
  try {
    const { keyword, plan: bodyPlan } = req.body;
    if (!keyword || keyword.trim().length < 2)  return res.status(400).json({ error: 'Please enter a keyword (min 2 characters)' });
    if (keyword.trim().length > 100)             return res.status(400).json({ error: 'Keyword too long (max 100 characters)' });

    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const kw       = keyword.trim().toLowerCase();
    const plan     = bodyPlan || user.plan || 'shorts_starter';
    const cacheCol = agentCol('niche_cache');
    const cacheKey = `search_${slugify(kw)}`;

    // 1-hour cache check
    const oneHourAgo = new Date(Date.now() - 3_600_000).toISOString();
    const cached = await cacheCol.findOne({ cacheKey, cachedAt: { $gt: oneHourAgo } }).catch(() => null);
    if (cached?.niches?.length) {
      console.log(`[Niche] search("${kw}"): cache hit (${cached.niches.length} niches)`);
      return res.json({ success: true, niches: cached.niches, result: cached.niches[0] || null, count: cached.niches.length, fromCache: true });
    }

    // Real-time discovery — run discoverNiches directly, no static fallback
    console.log(`[Niche] search("${kw}"): running real-time discovery`);
    let niches = [];
    try {
      niches = await Promise.race([
        discoverNiches(kw, plan),
        new Promise((_, reject) => setTimeout(() => reject(new Error('30s timeout')), 30_000)),
      ]);
    } catch (e) {
      console.warn(`[Niche] search("${kw}") timed out or errored: ${e.message}`);
    }

    if (niches.length) {
      await cacheCol.updateOne(
        { cacheKey },
        { $set: { cacheKey, niches, cachedAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 3_600_000) } },
        { upsert: true }
      ).catch(() => {});
    }

    const top = niches.slice(0, 10);
    console.log(`[Niche] search("${kw}"): returning ${top.length} real-time niches`);
    res.json({ success: true, niches: top, result: top[0] || null, count: top.length, fromCache: false, live_data: true });
  } catch (err) {
    console.error('[Niche] /api/niches/search error:', err);
    res.status(500).json({ error: 'Failed to search niche' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/niches/discover  — alias for browse (backward compat)
// ---------------------------------------------------------------------------
app.get('/api/niches/discover', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const plan         = user.plan || 'shorts_starter';
    const forceRefresh = req.query.refresh === 'true';
    const { niches, fromCache, cachedAt } = await discoverTopNiches(plan, forceRefresh);
    res.json({ success: true, top_niches: niches, niches, count: niches.length, live_data_used: true, fromCache, generated_at: cachedAt });
  } catch (err) {
    console.error('[Niche] /api/niches/discover error:', err);
    res.status(500).json({ error: 'Failed to discover trending niches' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/select  — user saves a niche to their channel config
// Body: { channelIndex: 0, nicheId: "...", nicheName: "..." }
// ---------------------------------------------------------------------------
app.post('/api/niches/select', requireAuth, async (req, res) => {
  try {
    const { channelIndex = 0, nicheId, nicheName } = req.body;
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    if (!nicheId && !nicheName) return res.status(400).json({ error: 'Provide nicheId or nicheName' });

    const label = nicheName || nicheId;
    if (!user.channels) user.channels = [];
    if (!user.channels[channelIndex]) user.channels[channelIndex] = {};
    user.channels[channelIndex].niche      = slugify(label);
    user.channels[channelIndex].nicheLabel = label;
    user.channels[channelIndex].nicheSetAt = new Date();
    user.nicheId = slugify(label);
    if (user.youtubeChannels?.length) {
      user.youtubeChannels.forEach(ch => { ch.nicheId = slugify(label); });
      user.markModified('youtubeChannels');
    }
    user.markModified('channels');
    await user.save();
    res.json({ success: true, message: `Niche "${label}" set for channel ${channelIndex + 1}` });
  } catch (err) {
    console.error('[Niche] /api/niches/select error:', err);
    res.status(500).json({ error: 'Failed to save niche selection' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/niches/categories  — list of available categories
// ---------------------------------------------------------------------------
app.get('/api/niches/categories', requireAuth, (req, res) => {
  res.json({ success: true, categories: Object.keys(NICHE_CPM_BENCHMARKS).filter(k => k !== 'default') });
});


// =============================================================================
// TIER 1 AGENTS — Content Planner, Topic Scout, Competitor Watcher, Research
// =============================================================================
// =============================================================================
// VIRALITYY — Tier 1 Agent API Routes
// =============================================================================
// HOW TO ADD: Paste this entire file into server.js BEFORE app.listen().
//
// Covers all four Tier 1 agents:
//   Agent 1 — Content Planner      → /api/agents/planner/*
//   Agent 2 — Daily Topic Scout    → /api/agents/scout/*
//   Agent 3 — Competitor Watcher   → /api/agents/competitor/*
//   Agent 4 — Script Research      → /api/agents/research/*
//
// Also registers the cron jobs for automated scheduling.
// =============================================================================

// ── Agent helpers (path and exec already required above) ───────────────────
function runAgentPy(script, args, timeoutMs = 120000) {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(__dirname, script);
    exec(`python3 "${scriptPath}" ${args}`, { timeout: timeoutMs }, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      try { resolve(JSON.parse(stdout.trim())); }
      catch { resolve({ raw: stdout.trim() }); }
    });
  });
}
const getDb = () => mongoose.connection.db;
const agentCol = (name) => getDb().collection(name);

// =============================================================================
// VIRAL HOOKS LIBRARY — 100+ proven hook templates, performance-tracked & auto-updated
// =============================================================================

function buildHooksSeedData() {
  const H = (template, category, sub, trigger, affinities) => ({
    template, category, subcategory: sub, psychologyTrigger: trigger,
    nicheAffinities: affinities,
    avgEngagementScore: 70, timesUsed: 0, timesPerformed: 0,
    lastUsed: null, isActive: true,
  });

  // Niche affinity keys match mapNicheToCategory() output
  const PSY  = ['Education', 'Entertainment', 'default'];
  const FIN  = ['Finance', 'Business', 'default'];
  const MOT  = ['Self Improvement', 'default'];
  const HLTH = ['Health & Wellness', 'Education', 'default'];
  const TECH = ['Technology', 'Finance', 'default'];
  const ALL  = ['Finance', 'Business', 'Health & Wellness', 'Self Improvement', 'Technology', 'Education', 'Entertainment', 'default'];

  return [
    // ── CATEGORY 1: CURIOSITY GAP ─────────────────────────────────────────────
    H("Here's what nobody tells you about [TOPIC]...",                     'Curiosity Gap', 'Secret Revelation',     'Information Gap Theory',      [...PSY, ...HLTH]),
    H("The real reason why [TOPIC] affects you more than you think...",    'Curiosity Gap', 'Hidden Cause',          'Information Gap Theory',      [...PSY, ...HLTH]),
    H("What scientists just discovered about [TOPIC] will shock you...",   'Curiosity Gap', 'Scientific Discovery',  'Novelty Bias',                HLTH),
    H("Nobody is talking about this [TOPIC] secret...",                    'Curiosity Gap', 'Insider Knowledge',     'Information Gap Theory',      ALL),
    H("Here's the part they always leave out about [TOPIC]...",            'Curiosity Gap', 'Missing Piece',         'Information Gap Theory',      ALL),
    H("The hidden truth about [TOPIC] that changes everything...",         'Curiosity Gap', 'Revelation',            'Cognitive Dissonance',        ALL),
    H("What your [TOPIC] is actually telling you...",                      'Curiosity Gap', 'Hidden Signal',         'Self-Relevance Effect',       [...PSY, ...HLTH]),
    H("The [TOPIC] secret that took me years to figure out...",            'Curiosity Gap', 'Hard-Won Insight',      'Information Gap Theory',      ALL),
    H("Why everything you know about [TOPIC] might be wrong...",           'Curiosity Gap', 'Paradigm Shift',        'Cognitive Dissonance',        ALL),
    H("The [TOPIC] fact that most people never discover...",               'Curiosity Gap', 'Exclusive Knowledge',   'Information Gap Theory',      ALL),

    // ── CATEGORY 2: SHOCKING STATISTICS ───────────────────────────────────────
    H("X% of people will never know this about [TOPIC]...",                'Shocking Statistics', 'Exclusivity Stat',     'Social Comparison',           ALL),
    H("Studies show X% of us make this [TOPIC] mistake daily...",          'Shocking Statistics', 'Common Error Stat',    'Loss Aversion',               [...FIN, ...HLTH]),
    H("In just 30 days, [TOPIC] can completely change your [OUTCOME]...",  'Shocking Statistics', 'Time-Bound Result',    'Optimism Bias',               [...MOT, ...HLTH]),
    H("Science says X minutes of [TOPIC] is all you need to...",           'Shocking Statistics', 'Minimal Effort Stat',  'Effort Heuristic',            HLTH),
    H("Research proves that [TOPIC] affects X% of people without them knowing...", 'Shocking Statistics', 'Hidden Impact Stat', 'Loss Aversion',           HLTH),
    H("X out of 10 people get [TOPIC] completely wrong...",                'Shocking Statistics', 'Majority Error Stat',  'Social Proof',                ALL),
    H("Scientists found that [TOPIC] can change your brain in just X days...", 'Shocking Statistics', 'Neuroplasticity Stat', 'Novelty Bias',             HLTH),
    H("The number one [TOPIC] stat that will change how you think...",     'Shocking Statistics', 'Mind-Changing Stat',   'Anchoring Effect',            ALL),
    H("Only X% of people know this [TOPIC] fact...",                       'Shocking Statistics', 'Rarity Stat',          'Scarcity Principle',          ALL),
    H("X billion people do this [TOPIC] thing wrong every single day...",  'Shocking Statistics', 'Global Scale Stat',    'Social Proof',                ALL),

    // ── CATEGORY 3: PATTERN INTERRUPTION ──────────────────────────────────────
    H("Stop scrolling. This [TOPIC] will actually change your life...",    'Pattern Interruption', 'Direct Stop',        'Attentional Capture',         ALL),
    H("Wait — before you keep going, you need to hear this about [TOPIC]...", 'Pattern Interruption', 'Urgent Pause',    'Loss Aversion',               ALL),
    H("I'm about to say something controversial about [TOPIC]...",         'Pattern Interruption', 'Controversy Hook',   'Cognitive Dissonance',        ALL),
    H("This is not what you think it is about [TOPIC]...",                 'Pattern Interruption', 'Expectation Flip',   'Cognitive Dissonance',        ALL),
    H("Most people get [TOPIC] backwards. Here's why...",                  'Pattern Interruption', 'Reversal',           'Contrarian Thinking',         [...MOT, ...FIN]),
    H("Everything you were taught about [TOPIC] is outdated...",           'Pattern Interruption', 'Paradigm Break',     'Cognitive Dissonance',        ALL),
    H("The [TOPIC] advice you've been given is actually harming you...",   'Pattern Interruption', 'Harmful Advice',     'Loss Aversion',               [...HLTH, ...MOT]),
    H("Forget everything you think you know about [TOPIC]...",             'Pattern Interruption', 'Clean Slate',        'Cognitive Dissonance',        ALL),
    H("The [TOPIC] approach that nobody recommends but everyone should use...", 'Pattern Interruption', 'Hidden Gem',     'Social Proof Reversal',       ALL),
    H("You've been doing [TOPIC] wrong your entire life...",               'Pattern Interruption', 'Lifetime Error',     'Cognitive Dissonance',        ALL),

    // ── CATEGORY 4: FEAR OF MISSING OUT ───────────────────────────────────────
    H("If you're not doing this [TOPIC] habit, you're falling behind...",  'Fear Of Missing Out', 'Falling Behind',     'Loss Aversion',               [...FIN, ...MOT]),
    H("Most people will never discover this [TOPIC] advantage...",         'Fear Of Missing Out', 'Missed Advantage',   'Scarcity Principle',          ALL),
    H("While everyone ignores [TOPIC], smart people are already doing this...", 'Fear Of Missing Out', 'Smart vs Average', 'Social Comparison',         FIN),
    H("You're losing [OUTCOME] every day you don't know about [TOPIC]...", 'Fear Of Missing Out', 'Daily Loss',         'Loss Aversion',               [...FIN, ...MOT]),
    H("The [TOPIC] window is closing — here's what you need to know...",   'Fear Of Missing Out', 'Closing Window',     'Scarcity Principle',          FIN),
    H("Successful people all do this [TOPIC] thing. Do you?",              'Fear Of Missing Out', 'Success Pattern',    'Social Comparison',           [...FIN, ...MOT]),
    H("The [TOPIC] skill that separates top performers from everyone else...", 'Fear Of Missing Out', 'Elite Separator',  'Social Comparison',          [...FIN, ...MOT]),
    H("Here's what high achievers know about [TOPIC] that you don't...",   'Fear Of Missing Out', 'Hidden Knowledge',   'Information Gap Theory',      [...FIN, ...MOT]),
    H("If you miss this [TOPIC] habit, you'll regret it later...",         'Fear Of Missing Out', 'Future Regret',      'Loss Aversion',               MOT),
    H("The [TOPIC] advantage most people discover too late...",            'Fear Of Missing Out', 'Too-Late Discovery',  'Loss Aversion',               ALL),

    // ── CATEGORY 5: DIRECT CHALLENGE ──────────────────────────────────────────
    H("I bet you can't go one week without doing this [TOPIC] thing...",   'Direct Challenge', 'Willpower Test',       'Reactance Theory',            MOT),
    H("Try this [TOPIC] experiment for just 7 days and see what happens...", 'Direct Challenge', 'Time-Limited Test',  'Commitment Bias',             [...MOT, ...HLTH]),
    H("Most people can't answer this simple [TOPIC] question. Can you?",   'Direct Challenge', 'Knowledge Test',       'Ego Threat',                  PSY),
    H("Test yourself: do you really understand [TOPIC]?",                  'Direct Challenge', 'Self-Assessment',      'Dunning-Kruger Effect',       ALL),
    H("Here's a [TOPIC] challenge that will change your perspective...",   'Direct Challenge', 'Perspective Challenge', 'Cognitive Flexibility',      MOT),
    H("I challenge you to try this [TOPIC] approach for one week...",      'Direct Challenge', 'Weekly Challenge',      'Commitment Bias',             MOT),
    H("Be honest — are you making this [TOPIC] mistake right now?",        'Direct Challenge', 'Honest Audit',         'Self-Awareness',              ALL),
    H("How many of these [TOPIC] signs do you recognize in yourself?",     'Direct Challenge', 'Self-Recognition',     'Self-Reference Effect',       PSY),
    H("Take this [TOPIC] test: what does your answer say about you?",      'Direct Challenge', 'Personality Reveal',   'Self-Reference Effect',       PSY),
    H("Can you spot the [TOPIC] mistake most people make?",                'Direct Challenge', 'Error Detection',      'Superiority Bias',            ALL),

    // ── CATEGORY 6: STORY OPENER ───────────────────────────────────────────────
    H("This [TOPIC] discovery changed everything I thought I knew...",     'Story Opener', 'Personal Discovery',     'Narrative Transportation',    ALL),
    H("I tried [TOPIC] for 30 days and here's what actually happened...",  'Story Opener', 'Personal Experiment',    'Narrative Transportation',    [...MOT, ...HLTH]),
    H("The moment I understood [TOPIC] was the moment everything shifted...", 'Story Opener', 'Turning Point',       'Narrative Transportation',    MOT),
    H("A simple [TOPIC] habit completely transformed my [OUTCOME]...",     'Story Opener', 'Transformation Story',   'Social Proof',                MOT),
    H("Three years ago I learned something about [TOPIC] that I'll never forget...", 'Story Opener', 'Time-Stamped Memory', 'Narrative Transportation', ALL),
    H("This [TOPIC] mistake cost me everything — don't make it too...",    'Story Opener', 'Cautionary Tale',        'Loss Aversion',               [...FIN, ...MOT]),
    H("The [TOPIC] lesson that took me years to learn the hard way...",    'Story Opener', 'Hard Lesson',            'Empathy Response',            ALL),
    H("Here's the [TOPIC] story they don't want you to know...",           'Story Opener', 'Suppressed Story',       'Reactance Theory',            PSY),
    H("I was completely wrong about [TOPIC] until this happened...",       'Story Opener', 'Belief Reversal',        'Cognitive Dissonance',        ALL),
    H("The [TOPIC] moment that made me rethink everything...",             'Story Opener', 'Epiphany Moment',        'Narrative Transportation',    ALL),

    // ── CATEGORY 7: DIRECT VALUE PROMISE ──────────────────────────────────────
    H("Here are X [TOPIC] facts that will make you smarter today...",      'Direct Value Promise', 'Knowledge Boost',    'Self-Enhancement Bias',     ALL),
    H("The X most important [TOPIC] things you need to know right now...", 'Direct Value Promise', 'Priority List',      'Information Processing',    ALL),
    H("X [TOPIC] habits that take under 5 minutes but change everything...", 'Direct Value Promise', 'Quick Win',         'Effort Heuristic',         HLTH),
    H("Learn [TOPIC] in under 60 seconds with this simple breakdown...",   'Direct Value Promise', 'Speed Learning',     'Processing Fluency',        ALL),
    H("The only [TOPIC] guide you'll ever need, simplified...",            'Direct Value Promise', 'Complete Guide',     'Cognitive Closure',         ALL),
    H("X [TOPIC] truths that instantly shift your perspective...",         'Direct Value Promise', 'Perspective Shift',  'Cognitive Flexibility',     ALL),
    H("Master [TOPIC] with these X science-backed principles...",          'Direct Value Promise', 'Scientific Method',  'Authority Heuristic',       [...HLTH, ...FIN]),
    H("The X [TOPIC] concepts that explain everything...",                 'Direct Value Promise', 'Core Concepts',      'Cognitive Closure',         ALL),
    H("Understand [TOPIC] better than 99% of people with these X ideas...", 'Direct Value Promise', 'Elite Understanding', 'Social Comparison',       ALL),
    H("X powerful [TOPIC] insights in under a minute...",                  'Direct Value Promise', 'Rapid Insights',     'Processing Fluency',        ALL),

    // ── CATEGORY 8: AUTHORITY & SOCIAL PROOF ──────────────────────────────────
    H("Harvard researchers discovered something fascinating about [TOPIC]...", 'Authority And Social Proof', 'Elite Institution',  'Authority Heuristic',       [...HLTH, ...PSY]),
    H("The world's top psychologists agree on this [TOPIC] principle...",  'Authority And Social Proof', 'Expert Consensus',   'Social Proof',              PSY),
    H("Neuroscience explains why [TOPIC] affects your brain this way...",  'Authority And Social Proof', 'Brain Science',      'Authority Heuristic',       [...HLTH, ...PSY]),
    H("Experts have been quietly studying [TOPIC] — here's what they found...", 'Authority And Social Proof', 'Insider Research', 'Authority Heuristic',    ALL),
    H("The [TOPIC] principle that Nobel Prize winners use daily...",       'Authority And Social Proof', 'Elite Endorsement',  'Authority Heuristic',       FIN),
    H("What the most successful people in the world do about [TOPIC]...",  'Authority And Social Proof', 'Elite Behaviour',    'Social Proof',              FIN),
    H("Scientists spent decades studying [TOPIC] and found this...",       'Authority And Social Proof', 'Long Research',      'Authority Heuristic',       HLTH),
    H("The ancient [TOPIC] wisdom that modern science just confirmed...",   'Authority And Social Proof', 'Ancient Wisdom',     'Temporal Framing',          [...HLTH, ...MOT]),
    H("The [TOPIC] technique used by world-class performers...",           'Authority And Social Proof', 'Elite Technique',    'Social Proof',              [...FIN, ...MOT]),
    H("What decades of [TOPIC] research finally revealed...",              'Authority And Social Proof', 'Research Reveal',    'Authority Heuristic',       ALL),

    // ── CATEGORY 9: EMOTIONAL TRIGGER ─────────────────────────────────────────
    H("This [TOPIC] truth is hard to hear but you need to know it...",     'Emotional Trigger', 'Difficult Truth',      'Empathy Response',            ALL),
    H("The [TOPIC] reality that most people are too afraid to face...",    'Emotional Trigger', 'Avoidance Pattern',    'Fear Activation',             MOT),
    H("If [TOPIC] makes you feel this way, you're not alone...",           'Emotional Trigger', 'Shared Feeling',       'Social Proof',                PSY),
    H("The [TOPIC] feeling everyone has but nobody talks about...",        'Emotional Trigger', 'Silent Struggle',      'Normalisation',               PSY),
    H("This [TOPIC] insight made me emotional when I first understood it...", 'Emotional Trigger', 'Emotional Recall',  'Emotional Contagion',         MOT),
    H("Why [TOPIC] hits differently once you truly understand it...",      'Emotional Trigger', 'Depth of Understanding', 'Self-Reference Effect',     MOT),
    H("The [TOPIC] truth that changes how you see everything...",          'Emotional Trigger', 'World-View Shift',     'Cognitive Dissonance',        ALL),
    H("You deserve to know this [TOPIC] reality...",                       'Emotional Trigger', 'Empowerment',          'Self-Determination Theory',   ALL),
    H("This [TOPIC] revelation will make you see yourself differently...", 'Emotional Trigger', 'Self-Perception Shift', 'Self-Reference Effect',      PSY),
    H("The [TOPIC] understanding that brings instant peace of mind...",    'Emotional Trigger', 'Relief Payoff',        'Stress Reduction',            [...MOT, ...HLTH]),

    // ── CATEGORY 10: REVEAL & TEASE ───────────────────────────────────────────
    H("By the end of this, you'll never think about [TOPIC] the same way...", 'Reveal And Tease', 'Promised Shift',    'Zeigarnik Effect',            ALL),
    H("Stay until the end — this [TOPIC] twist will surprise you...",      'Reveal And Tease', 'End Reward',           'Zeigarnik Effect',            ALL),
    H("The [TOPIC] answer is not what you expect...",                      'Reveal And Tease', 'Unexpected Answer',    'Expectation Violation',       ALL),
    H("What I'm about to share about [TOPIC] took experts years to figure out...", 'Reveal And Tease', 'Hard-Earned Reveal', 'Authority Heuristic',   ALL),
    H("The [TOPIC] reveal that changes the entire picture...",             'Reveal And Tease', 'Picture Change',       'Narrative Transportation',    ALL),
    H("Keep watching — this [TOPIC] gets more surprising every second...", 'Reveal And Tease', 'Escalating Surprise',  'Zeigarnik Effect',            ALL),
    H("The [TOPIC] ending nobody sees coming...",                          'Reveal And Tease', 'Surprise Ending',      'Expectation Violation',       ALL),
    H("Watch what happens when you apply this [TOPIC] insight...",         'Reveal And Tease', 'Results Preview',      'Optimism Bias',               ALL),
    H("The [TOPIC] conclusion that flips conventional wisdom upside down...", 'Reveal And Tease', 'Wisdom Flip',       'Cognitive Dissonance',        ALL),
    H("What comes next about [TOPIC] is the part nobody expects...",       'Reveal And Tease', 'Unexpected Next',      'Expectation Violation',       ALL),
  ];
}

async function seedHooksLibrary() {
  try {
    const col   = agentCol('hooksLibrary');
    const count = await col.countDocuments();
    if (count >= 100) { console.log(`[Hooks] Library already seeded (${count} hooks) — skipping`); return; }
    const hooks = buildHooksSeedData();
    await col.insertMany(hooks);
    console.log(`[Hooks] Seeded ${hooks.length} hooks into hooksLibrary collection`);
  } catch (e) { console.error('[Hooks] Seed failed:', e.message); }
}

async function selectHookForSlot(userId, channelId, nicheName) {
  try {
    const col      = agentCol('hooksLibrary');
    const slotsCol = agentCol('calendar_slots');

    const recentSlots = await slotsCol
      .find({ userId: String(userId), ...(channelId ? { channelId } : {}), hookId: { $exists: true } })
      .sort({ postedAt: -1, scheduledPostTime: -1 })
      .limit(10)
      .project({ hookId: 1, hookCategory: 1 })
      .toArray();

    const recentHookIds  = recentSlots.map(s => s.hookId).filter(Boolean);
    const lastCategory   = recentSlots[0]?.hookCategory || null;
    const nicheCategory  = mapNicheToCategory(nicheName);

    const query = { isActive: true };
    if (nicheCategory !== 'default') query.nicheAffinities = { $in: [nicheCategory, 'default'] };
    if (recentHookIds.length) {
      try {
        const { ObjectId } = require('mongoose').Types;
        query._id = { $nin: recentHookIds.map(id => { try { return new ObjectId(id); } catch { return id; } }) };
      } catch {}
    }
    if (lastCategory) query.category = { $ne: lastCategory };

    let candidates = await col.find(query).sort({ avgEngagementScore: -1 }).limit(5).toArray();

    if (!candidates.length) {
      candidates = await col.find({ isActive: true }).sort({ avgEngagementScore: -1 }).limit(5).toArray();
    }
    if (!candidates.length) return null;

    return candidates[Math.floor(Math.random() * candidates.length)];
  } catch (e) {
    console.warn('[Hooks] selectHookForSlot failed (non-fatal):', e.message);
    return null;
  }
}

async function updateHookScores() {
  try {
    const col      = agentCol('hooksLibrary');
    const slotsCol = agentCol('calendar_slots');
    const cutoff   = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

    const hookedSlots = await slotsCol.find({
      hookId: { $exists: true, $ne: null },
      posted: true,
      date: { $gte: cutoff },
    }).project({ hookId: 1, hookCategory: 1, viewCount: 1, likeCount: 1, commentCount: 1 }).toArray();

    if (!hookedSlots.length) {
      console.log('[Hooks] Weekly score update: no hooked slots found this week');
      return;
    }

    const byHook = {};
    for (const s of hookedSlots) {
      if (!s.hookId) continue;
      const k = String(s.hookId);
      if (!byHook[k]) byHook[k] = { views: 0, engagement: 0, count: 0 };
      const v = s.viewCount || 0;
      const e = v > 0 ? ((s.likeCount || 0) + (s.commentCount || 0)) / v : 0;
      byHook[k].views      += v;
      byHook[k].engagement += e;
      byHook[k].count++;
    }

    const { ObjectId } = require('mongoose').Types;
    let updated = 0;
    for (const [hookId, data] of Object.entries(byHook)) {
      const avgViews = data.views / data.count;
      const avgEng   = data.engagement / data.count;
      // Normalize: 10k views = max; 5% engagement = max
      const viewNorm = Math.min(100, (avgViews / 10000) * 100);
      const engNorm  = Math.min(100, avgEng * 2000);
      const newScore = Math.round(viewNorm * 0.6 + engNorm * 0.4);

      const hook = await col.findOne({ _id: new ObjectId(hookId) }).catch(() => null);
      if (!hook) continue;

      const blendedScore = Math.round((hook.avgEngagementScore * 0.7) + (newScore * 0.3));
      const shouldDeactivate = hook.timesUsed >= 5 && blendedScore < 40;

      await col.updateOne(
        { _id: new ObjectId(hookId) },
        { $set: { avgEngagementScore: blendedScore, ...(shouldDeactivate ? { isActive: false } : {}) } }
      );
      updated++;
    }
    console.log(`[Hooks] Weekly score update complete — ${updated} hook(s) updated`);
  } catch (e) { console.error('[Hooks] Weekly score update failed:', e.message); }
}

async function rebuildHooksMonthly() {
  if (!process.env.OPENAI_API_KEY) {
    console.log('[Hooks] Monthly rebuild skipped — no OpenAI key');
    return;
  }
  try {
    const col = agentCol('hooksLibrary');
    const { ObjectId } = require('mongoose').Types;
    const { OpenAI } = require('openai');
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    // Stats for prompt context
    const allHooks = await col.find({}).toArray();
    const categoryStats = {};
    for (const h of allHooks) {
      if (!categoryStats[h.category]) categoryStats[h.category] = { count: 0, totalScore: 0 };
      categoryStats[h.category].count++;
      categoryStats[h.category].totalScore += h.avgEngagementScore;
    }
    const categoryAvgs = Object.entries(categoryStats).map(([cat, s]) =>
      `${cat}: avg score ${Math.round(s.totalScore / s.count)}, ${s.count} hooks`
    ).join('\n');

    const resp = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{
        role: 'user',
        content: `You are a viral content strategist. Based on this YouTube Shorts hook library performance data, generate 20 fresh hook templates.

Current category performance:
${categoryAvgs}

Generate 20 new hook templates that:
1. Fill gaps in underperforming categories
2. Use [TOPIC] as the variable placeholder
3. Are proven patterns for viral short-form video
4. Vary across psychology triggers

Return JSON array of exactly 20 objects:
[{"template":"hook text with [TOPIC]","category":"one of: Curiosity Gap|Shocking Statistics|Pattern Interruption|Fear Of Missing Out|Direct Challenge|Story Opener|Direct Value Promise|Authority And Social Proof|Emotional Trigger|Reveal And Tease","psychologyTrigger":"brief psychology principle"}]`,
      }],
      temperature: 0.85,
      response_format: { type: 'json_object' },
      max_tokens: 2000,
    });

    let newHooks = [];
    try {
      const parsed = JSON.parse(resp.choices[0].message.content);
      newHooks = Array.isArray(parsed) ? parsed : (parsed.hooks || parsed.templates || []);
    } catch { newHooks = []; }

    let added = 0;
    for (const h of newHooks.slice(0, 20)) {
      if (!h.template || !h.category) continue;
      await col.insertOne({
        template: h.template, category: h.category,
        subcategory: 'AI Generated', psychologyTrigger: h.psychologyTrigger || 'Engagement Optimization',
        nicheAffinities: ['default'],
        avgEngagementScore: 70, timesUsed: 0, timesPerformed: 0,
        lastUsed: null, isActive: true, generatedAt: new Date().toISOString(),
      });
      added++;
    }

    // Remove underperforming hooks (inactive + score < 35 + at least 5 uses)
    const removed = await col.deleteMany({
      isActive: false, avgEngagementScore: { $lt: 35 }, timesUsed: { $gte: 5 },
    });

    // Reactivate high scorers that were deactivated
    await col.updateMany(
      { isActive: false, avgEngagementScore: { $gte: 60 } },
      { $set: { isActive: true } }
    );

    const total = await col.countDocuments({ isActive: true });
    const now   = new Date().toISOString();
    await agentCol('hooksLibraryMeta').updateOne(
      { _id: 'meta' },
      { $set: { lastRebuiltAt: now, lastRebuildStats: { added, removed: removed.deletedCount, total } } },
      { upsert: true }
    );

    logAPIUsage('openai', 'hooks_monthly_rebuild', null, resp.usage?.total_tokens || 0,
      ((resp.usage?.prompt_tokens || 0) * API_COSTS.openai_input) +
      ((resp.usage?.completion_tokens || 0) * API_COSTS.openai_output), true);

    console.log(`[Hooks] Monthly rebuild: ${added} added, ${removed.deletedCount} removed, ${total} total active`);
  } catch (e) { console.error('[Hooks] Monthly rebuild failed:', e.message); }
}

// =============================================================================
// CRON JOBS — all registered via registerCronJobs() after MongoDB connects (see bottom of file)
// =============================================================================


// =============================================================================
// AGENT 1 — CONTENT PLANNER ROUTES
// =============================================================================

// GET /api/agents/planner/calendar
// Returns the active calendar for the logged-in user.
// videosPosted is counted from calendar_slots (individual docs) — the authoritative source.
app.get('/api/agents/planner/calendar', requireAuth, async (req, res) => {
  try {
    const calCol   = agentCol('content_calendars');
    const slotsCol = agentCol('calendar_slots');
    const [doc, videosPosted] = await Promise.all([
      calCol.findOne({ userId: req.user.id, status: 'active' }),
      slotsCol.countDocuments({ userId: req.user.id, posted: true }),
    ]);
    if (!doc) return res.json({ success: true, slots: [], pending: false, videosPosted: 0, message: 'Click Generate calendar to create your 30-day content plan' });
    doc._id = doc._id.toString();
    const slots = doc.slots || [];
    res.json({ success: true, exists: true, slots, pending: slots.some(s => s.status === 'scheduled' || s.status === 'processing'), videosPosted, calendar: doc });
  } catch (err) {
    console.error('[Planner] GET /calendar error:', err);
    res.status(500).json({ error: 'Failed to fetch calendar' });
  }
});

// GET /api/content/calendar/today
// Returns today's calendar slots from MongoDB cache — never re-generates if they exist.
// If no slots exist for today, runs generation once, saves, then returns them.
app.get('/api/content/calendar/today', requireAuth, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const userId   = String(req.user.id);
    const slotsCol = agentCol('calendar_slots');
    const calCol   = agentCol('content_calendars');

    // Count all-time posted across BOTH storage layers:
    //   • calendar_slots  — JIT-pipeline posts (posted:true set per-document)
    //   • content_calendars.slots[] — old-pipeline posts (posted:true in embedded array)
    // The two sets are disjoint (different pipeline eras) so summing is correct.
    const [slots, jitPosted, embeddedPostedArr] = await Promise.all([
      slotsCol.find({ userId, date: today }).sort({ videoIndex: 1 }).toArray(),
      slotsCol.countDocuments({ userId, posted: true }),
      calCol.aggregate([
        { $match: { userId } },
        { $project: { n: { $size: { $filter: {
            input: { $ifNull: ['$slots', []] },
            as:    's',
            cond:  { $eq: ['$$s.posted', true] },
        }}}}},
        { $group: { _id: null, total: { $sum: '$n' } } },
      ]).toArray().catch(() => []),
    ]);

    const allTimePosted = jitPosted + (embeddedPostedArr[0]?.total || 0);

    if (slots.length) {
      slots.forEach(s => { if (s._id) s._id = s._id.toString(); });
      return res.json({
        success:       true,
        slots,
        today,
        fromCache:     true,
        generatedAt:   slots[0]?.generatedAt || today,
        count:         slots.length,
        allTimePosted,
      });
    }

    // Cache miss — generate now, save, return.
    console.log(`[CalToday] No slots for ${today} (user ${userId}) — generating now`);
    await runDailySlotGeneration(userId, today);

    const freshSlots = await slotsCol
      .find({ userId, date: today })
      .sort({ videoIndex: 1 })
      .toArray();
    freshSlots.forEach(s => { if (s._id) s._id = s._id.toString(); });

    res.json({
      success:       true,
      slots:         freshSlots,
      today,
      fromCache:     false,
      generatedAt:   freshSlots[0]?.generatedAt || new Date().toISOString(),
      count:         freshSlots.length,
      allTimePosted,
    });
  } catch (err) {
    console.error('[CalToday] Error:', err.message);
    res.status(500).json({ error: 'Failed to load today\'s calendar' });
  }
});

// GET /api/agents/planner/slots/upcoming
// Returns the next 7 unposted calendar slots
app.get('/api/agents/planner/slots/upcoming', requireAuth, async (req, res) => {
  try {
    const limit  = Math.min(parseInt(req.query.limit) || 7, 30);
    const today  = new Date().toISOString().slice(0, 10);
    const col    = await agentCol('calendar_slots');
    const docs   = await col
      .find({ userId: req.user.id, status: 'scheduled', scheduledDate: { $gte: today } })
      .sort({ scheduledDate: 1 })
      .limit(limit)
      .toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, slots: docs });
  } catch (err) {
    console.error('[Planner] GET /slots/upcoming error:', err);
    res.status(500).json({ error: 'Failed to fetch slots' });
  }
});

// POST /api/agents/planner/generate
// Generates a new 30-day content calendar for the logged-in user
app.post('/api/agents/planner/generate', requireAuth, async (req, res) => {
  try {
    const { days = 30, force = false } = req.body;
    // Respond immediately — generation happens in background (up to 90s)
    res.json({ success: true, message: 'Calendar generation started — refresh in ~60 seconds', userId: req.user.id });

    const script = path.join(__dirname, 'content_planner_agent.py');
    const forceFlag = force ? '--force' : '';
    exec(
      `python3 "${script}" --generate ${req.user.id} --days ${days} ${forceFlag}`,
      { timeout: 180000 },
      (err, stdout, stderr) => {
        if (err) console.error('[Planner] Generate error:', stderr);
        else     console.log('[Planner] Calendar generated for', req.user.id);
      }
    );
  } catch (err) {
    console.error('[Planner] POST /generate error:', err);
    res.status(500).json({ error: 'Failed to start calendar generation' });
  }
});

// POST /api/content/calendar — generate 30-day plan-aware calendar via OpenAI (unique title per slot)
app.post('/api/content/calendar', requireAuth, async (req, res) => {
  try {
    if (!process.env.OPENAI_API_KEY) return res.status(503).json({ error: 'OpenAI not configured' });
    const { channelId, nicheName } = req.body;
    if (!nicheName) return res.status(400).json({ error: 'nicheName is required' });

    const user   = await User.findById(req.user.id);
    const config = PLAN_CONFIG[user.plan] || PLAN_CONFIG.trial;

    const { OpenAI } = require('openai');
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    // --- Performance analysis (first-week exception: returns null if no history) ---
    const insights = await analyzeChannelPerformance(req.user.id, channelId, nicheName);
    let perfContext = '';
    if (insights) {
      const topList    = insights.topPerformers.map(v =>
        `"${v.title}" — ${v.views.toLocaleString()} views, ${v.likes} likes`
      ).join('\n');
      const bottomList = insights.bottomPerformers.map(v =>
        `"${v.title}" — ${v.views.toLocaleString()} views`
      ).join('\n');
      const tp = insights.titlePatterns;
      const patternBlock = tp?.patterns?.length
        ? `\nPROVEN TITLE PATTERNS — Use these structures for new titles:\n${tp.patterns.map((p, i) => `${i + 1}. ${p}`).join('\n')}\nPower words that drive clicks in this niche: ${(tp.powerWords || []).join(', ')}\nIdeal title length: ${tp.idealLength || 'under 60 chars'}\n`
        : '';
      perfContext = `
PREVIOUS PERFORMANCE DATA (${insights.totalAnalyzed} videos from last 4 weeks):

TOP PERFORMERS — Build on these styles and topics with completely fresh angles:
${topList}

LOW PERFORMERS — Avoid titles, formats, or concepts similar to these:
${bottomList}
${patternBlock}
Strategy: Generate titles that build on what worked. Use similar hooks, formats, or topic areas as the top performers but with new angles. Never repeat any of the above titles exactly.
`;
    }

    const today = new Date();
    const dates = Array.from({ length: 30 }, (_, i) => {
      const d = new Date(today);
      d.setDate(d.getDate() + i);
      return d.toISOString().slice(0, 10);
    });

    const longFormDates = config.longFormPerWeek > 0
      ? dates.filter(dt => new Date(dt).getDay() === 1)
      : [];

    const totalShorts   = config.shortsPerDay * 30;
    const totalLongForm = longFormDates.length;

    // --- Generate all unique short titles in day-batches with a running used-titles list ---
    const BATCH = 5; // days per OpenAI call
    const allShortTitles = [];

    for (let batchStart = 0; batchStart < 30; batchStart += BATCH) {
      const batchDays  = Math.min(BATCH, 30 - batchStart);
      const batchCount = batchDays * config.shortsPerDay;
      const usedList   = allShortTitles.length
        ? `\nAlready used — do NOT repeat these concepts:\n${allShortTitles.map((t, i) => `${i + 1}. ${t}`).join('\n')}\n`
        : '';

      const batchRes = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{
          role: 'user',
          content: `YouTube Shorts content strategist for a "${nicheName}" channel.
${perfContext}${usedList}
Generate exactly ${batchCount} NEW Short video titles (under 60 chars each).
STRICT RULES:
- Every title must cover a completely different concept — no overlapping topics or angles.
- Vary style: hook, story, tutorial, listicle, myth-bust. Use each style at least once.
- Never reuse a concept, phrase, or idea from the "Already used" list above.
- Output exactly ${batchCount} titles — no more, no fewer.

Return JSON: { "titles": [array of exactly ${batchCount} unique strings] }`,
        }],
        temperature: 0.9,
        response_format: { type: 'json_object' },
      });

      let parsed;
      try { parsed = JSON.parse(batchRes.choices[0].message.content); } catch { parsed = {}; }
      const batchTitles = (parsed.titles || []).slice(0, batchCount);
      allShortTitles.push(...batchTitles);
    }

    // --- Dedup short titles: find any title appearing more than once, regenerate them ---
    {
      const seen = new Set();
      const dupeIdx = [];
      for (let i = 0; i < allShortTitles.length; i++) {
        const key = (allShortTitles[i] || '').trim().toLowerCase();
        if (key && !seen.has(key)) { seen.add(key); }
        else { dupeIdx.push(i); }
      }
      if (dupeIdx.length > 0) {
        console.log(`[ContentCalendar] ${dupeIdx.length} duplicate short title(s) detected — regenerating`);
        const goodTitles = allShortTitles.filter((_, i) => !dupeIdx.includes(i));
        try {
          const regenRes = await openai.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [{ role: 'user', content:
              `YouTube Shorts content strategist for a "${nicheName}" channel.
Generate exactly ${dupeIdx.length} REPLACEMENT Short video titles (under 60 chars each).
Each must cover a completely different concept from every title already used:
${goodTitles.map((t, i) => `${i + 1}. ${t}`).join('\n')}
Return JSON: { "titles": [${dupeIdx.length} strings] }` }],
            temperature: 0.95,
            response_format: { type: 'json_object' },
          });
          const rp = JSON.parse(regenRes.choices[0].message.content);
          (rp.titles || []).forEach((t, i) => { if (t && dupeIdx[i] != null) allShortTitles[dupeIdx[i]] = t; });
          console.log(`[ContentCalendar] Replaced ${(rp.titles || []).length} duplicate(s)`);
        } catch { /* replaced slots will use per-slot fallback titles */ }
      }
    }

    // --- Generate long-form titles (separate call if needed) ---
    let allLongFormTitles = [];
    if (totalLongForm > 0) {
      const lfRes = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{
          role: 'user',
          content: `YouTube long-form content strategist for a "${nicheName}" channel.
${perfContext}
Generate exactly ${totalLongForm} unique Long-form video titles (60–100 chars each).
These should be in-depth, educational, and distinct from typical Shorts topics.
Build on top-performing topics with deeper, more comprehensive angles.
Already used Short titles (do NOT overlap):
${allShortTitles.slice(0, 30).map((t, i) => `${i + 1}. ${t}`).join('\n')}

Return JSON: { "titles": [${totalLongForm} strings] }`,
        }],
        temperature: 0.88,
        response_format: { type: 'json_object' },
      });
      try {
        const p = JSON.parse(lfRes.choices[0].message.content);
        allLongFormTitles = p.titles || [];
      } catch { /* use fallbacks below */ }

      // --- Dedup long-form titles ---
      const lfSeen = new Set(allShortTitles.map(t => t.trim().toLowerCase()));
      const lfDupeIdx = [];
      for (let i = 0; i < allLongFormTitles.length; i++) {
        const key = (allLongFormTitles[i] || '').trim().toLowerCase();
        if (key && !lfSeen.has(key)) { lfSeen.add(key); }
        else { lfDupeIdx.push(i); }
      }
      if (lfDupeIdx.length > 0) {
        console.log(`[ContentCalendar] ${lfDupeIdx.length} duplicate long-form title(s) detected — regenerating`);
        try {
          const lfRegenRes = await openai.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [{ role: 'user', content:
              `YouTube long-form content strategist for a "${nicheName}" channel.
Generate exactly ${lfDupeIdx.length} REPLACEMENT Long-form video titles (60–100 chars each).
Each must be unique — different from all already-used titles.
Return JSON: { "titles": [${lfDupeIdx.length} strings] }` }],
            temperature: 0.95,
            response_format: { type: 'json_object' },
          });
          const lfrp = JSON.parse(lfRegenRes.choices[0].message.content);
          (lfrp.titles || []).forEach((t, i) => { if (t && lfDupeIdx[i] != null) allLongFormTitles[lfDupeIdx[i]] = t; });
        } catch { /* fallbacks applied during slot build */ }
      }
    }

    // --- Build one slot per individual video ---
    let shortIdx   = 0;
    let longFormIdx = 0;
    const slots = [];

    for (let i = 0; i < 30; i++) {
      const day  = i + 1;
      const date = dates[i];
      const dow  = new Date(date).getDay();

      for (let v = 0; v < config.shortsPerDay; v++) {
        slots.push({
          day, date,
          title:             allShortTitles[shortIdx] || `${nicheName} Short — Day ${day} #${v + 1}`,
          type:              'Short',
          videoIndex:        v + 1,
          totalForDay:       config.shortsPerDay,
          angle:             'short',
          status:            'scheduled',
          posted:            false,
          scheduledPostTime: assignPostingTime(date, v + 1, config.shortsPerDay),
        });
        shortIdx++;
      }

      if (config.longFormPerWeek > 0 && dow === 1) {
        slots.push({
          day, date,
          title:             allLongFormTitles[longFormIdx] || `Deep Dive ${longFormIdx + 1}: ${nicheName}`,
          type:              'Long-form',
          videoIndex:        1,
          totalForDay:       1,
          angle:             'long-form',
          status:            'scheduled',
          posted:            false,
          scheduledPostTime: assignPostingTime(date, 1, 1),
        });
        longFormIdx++;
      }
    }

    const weeklyInsights = {
      generatedAt:        new Date().toISOString(),
      totalAnalyzed:      insights?.totalAnalyzed   || 0,
      topPerformers:      insights?.topPerformers   || [],
      bottomPerformers:   insights?.bottomPerformers || [],
      titlePatterns:      insights?.titlePatterns   || null,
      generationStrategy: insights
        ? `AI-optimised: built on top ${insights.topPerformers.length} performer(s), avoided bottom ${insights.bottomPerformers.length}`
        : 'Standard generation — no prior performance data (first week)',
    };

    const col = await agentCol('content_calendars');
    await col.updateOne(
      { userId: req.user.id, status: 'active' },
      { $set: { userId: req.user.id, channelId: channelId || '', nicheName, plan: user.plan, slots, status: 'active', generatedAt: new Date().toISOString(), weeklyInsights } },
      { upsert: true }
    );

    // Write each slot as an individual document so the post pipeline can consume them
    const slotsCol = await agentCol('calendar_slots');
    await slotsCol.deleteMany({ userId: req.user.id, posted: { $ne: true } });
    if (slots.length) {
      await slotsCol.insertMany(slots.map(s => ({ ...s, userId: req.user.id, channelId: channelId || '', nicheName })));
    }

    res.json({ success: true, slots, count: slots.length, totalVideos: slots.length, plan: user.plan, weeklyInsights });

    // Fire-and-forget: generate structured scripts for the first 10 planned slots
    if (process.env.OPENAI_API_KEY) {
      setImmediate(async () => {
        try {
          const scriptsCol = agentCol('scripts');
          await scriptsCol.deleteMany({ userId: req.user.id });
          const calDoc  = await agentCol('content_calendars').findOne({ userId: req.user.id, status: 'active' });
          const calId   = calDoc?._id?.toString() || '';
          const { OpenAI } = require('openai');
          const openai  = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
          const firstSlots = slots.filter(s => s.status === 'planned').slice(0, 10);
          for (const slot of firstSlots) {
            try {
              const structured = await generateStructuredScript(slot.title, nicheName, slot.type, openai);
              await scriptsCol.insertOne({
                userId: req.user.id, calendarId: calId, slotDay: slot.day, videoIndex: slot.videoIndex || 1,
                title: slot.title, type: slot.type, scheduledDate: slot.date, nicheName,
                hook: structured.hook || '', mainPoints: Array.isArray(structured.mainPoints) ? structured.mainPoints.slice(0, 5) : [],
                cta: structured.cta || '', estimatedDuration: structured.estimatedDuration || (slot.type === 'Long-form' ? '5:00' : '0:55'),
                fullScript: structured.fullScript || '', generatedAt: new Date().toISOString(),
              });
            } catch (e) { console.error(`[Scripts] bg gen failed "${slot.title}":`, e.message); }
          }
          console.log(`[Scripts] Background generation done — ${firstSlots.length} scripts saved`);
        } catch (e) { console.error('[Scripts] Background generation error:', e.message); }
      });
    }
  } catch (err) {
    console.error('[ContentCalendar] error:', err);
    res.status(500).json({ error: err.message || 'Failed to generate calendar' });
  }
});

// GET /api/content/weekly-insights — returns weeklyInsights from the active calendar
app.get('/api/content/weekly-insights', requireAuth, async (req, res) => {
  try {
    const col = await agentCol('content_calendars');
    const doc = await col.findOne(
      { userId: req.user.id, status: 'active' },
      { projection: { weeklyInsights: 1, nicheName: 1, generatedAt: 1 } }
    );
    if (!doc?.weeklyInsights) return res.json({ success: true, weeklyInsights: null });
    res.json({ success: true, weeklyInsights: doc.weeklyInsights, nicheName: doc.nicheName, generatedAt: doc.generatedAt });
  } catch (err) {
    console.error('[WeeklyInsights] GET error:', err);
    res.status(500).json({ error: 'Failed to fetch weekly insights' });
  }
});

// GET /api/optimisation/insights — weeklyInsights for the AI Optimisation page
app.get('/api/optimisation/insights', requireAuth, async (req, res) => {
  try {
    const col = await agentCol('content_calendars');
    const doc = await col.findOne(
      { userId: req.user.id, status: 'active' },
      { projection: { weeklyInsights: 1, nicheName: 1, generatedAt: 1, weeklyRefreshedAt: 1 } }
    );
    const wi = doc?.weeklyInsights || null;
    res.json({
      success:           true,
      weeklyInsights:    wi,
      nicheName:         doc?.nicheName         || null,
      generatedAt:       doc?.generatedAt       || null,
      weeklyRefreshedAt: doc?.weeklyRefreshedAt || null,
    });
  } catch (err) {
    console.error('[Optimisation] GET /insights error:', err);
    res.status(500).json({ error: 'Failed to fetch optimisation insights' });
  }
});

// GET /api/optimisation/stats — aggregate stats for the AI Optimisation page stat cards
app.get('/api/optimisation/stats', requireAuth, async (req, res) => {
  try {
    const col = await agentCol('content_calendars');
    const doc = await col.findOne(
      { userId: req.user.id, status: 'active' },
      { projection: { weeklyInsights: 1, slots: 1, nicheName: 1 } }
    );
    const wi     = doc?.weeklyInsights || {};
    const slots  = doc?.slots          || [];
    const posted = slots.filter(s => s.status === 'posted' || s.posted).length;
    res.json({
      success:            true,
      totalAnalyzed:      wi.totalAnalyzed      || 0,
      trendingTopicsCount: (wi.trendingTopics || []).length,
      topPerformersCount: (wi.topPerformers  || []).length,
      generationStrategy: wi.generationStrategy  || null,
      lastOptimisedAt:    wi.generatedAt         || null,
      idealTitleLength:   wi.titlePatterns?.idealLength || null,
      postedThisCycle:    posted,
      nicheName:          doc?.nicheName         || null,
    });
  } catch (err) {
    console.error('[Optimisation] GET /stats error:', err);
    res.status(500).json({ error: 'Failed to fetch optimisation stats' });
  }
});

// GET /api/content/queue — pending/approved calendar slots awaiting review or posting
app.get('/api/content/queue', requireAuth, async (req, res) => {
  try {
    const col      = await agentCol('content_calendars');
    const calendar = await col.findOne({ userId: req.user.id, status: 'active' });
    if (!calendar) return res.json({ success: true, items: [], count: 0 });
    const items = (calendar.slots || [])
      .filter(s => !s.posted && ['planned', 'pending', 'approved'].includes(s.status))
      .map(s => ({ ...s, slotId: `${s.day}_${s.videoIndex || 1}` }));
    res.json({ success: true, items, count: items.length });
  } catch (err) {
    console.error('[ContentQueue] GET /queue error:', err);
    res.status(500).json({ error: 'Failed to fetch content queue' });
  }
});

// POST /api/content/:id/approve — approve a calendar slot (id = day number)
app.post('/api/content/:id/approve', requireAuth, async (req, res) => {
  try {
    const day = parseInt(req.params.id);
    const col = await agentCol('content_calendars');
    await col.updateOne(
      { userId: req.user.id, status: 'active' },
      { $set: { 'slots.$[s].status': 'approved', 'slots.$[s].approvedAt': new Date().toISOString() } },
      { arrayFilters: [{ 's.day': day }] }
    );
    res.json({ success: true, message: 'Content approved' });
  } catch (err) {
    console.error('[ContentQueue] POST /approve error:', err);
    res.status(500).json({ error: 'Failed to approve content' });
  }
});

// POST /api/content/:id/skip — skip a calendar slot (id = day number)
app.post('/api/content/:id/skip', requireAuth, async (req, res) => {
  try {
    const day = parseInt(req.params.id);
    const col = await agentCol('content_calendars');
    await col.updateOne(
      { userId: req.user.id, status: 'active' },
      { $set: { 'slots.$[s].status': 'skipped', 'slots.$[s].skipReason': 'user_skipped', 'slots.$[s].skippedAt': new Date().toISOString() } },
      { arrayFilters: [{ 's.day': day }] }
    );
    res.json({ success: true, message: 'Content skipped' });
  } catch (err) {
    console.error('[ContentQueue] POST /skip error:', err);
    res.status(500).json({ error: 'Failed to skip content' });
  }
});

// POST /api/content/approve-all — approve all queued/planned slots so the scheduler picks them up
app.post('/api/content/approve-all', requireAuth, async (req, res) => {
  try {
    const col = agentCol('content_calendars');
    const result = await col.updateOne(
      { userId: req.user.id, status: 'active' },
      { $set: { 'slots.$[s].status': 'approved' } },
      { arrayFilters: [{ 's.status': { $in: ['planned', 'pending', 'queued'] } }] }
    );
    const cal = await col.findOne({ userId: req.user.id, status: 'active' });
    const approvedCount = (cal?.slots || []).filter(s => s.status === 'approved').length;
    res.json({ success: true, approvedCount, message: `${approvedCount} item(s) approved — scheduler will pick them up on next hourly run` });
  } catch (err) {
    console.error('[ContentQueue] POST /approve-all error:', err);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/agents/planner/slots/:day/posted
// Marks a calendar slot as posted
app.patch('/api/agents/planner/slots/:day/posted', requireAuth, async (req, res) => {
  try {
    const day    = parseInt(req.params.day);
    const col    = await agentCol('calendar_slots');
    const result = await col.updateOne(
      { userId: req.user.id, dayNumber: day },
      { $set: {
        status:   'posted',
        postedAt: new Date().toISOString(),
        videoId:  req.body.videoId || '',
      }}
    );
    if (result.matchedCount === 0) {
      return res.status(404).json({ error: `Day ${day} slot not found` });
    }
    res.json({ success: true, message: `Day ${day} marked as posted` });
  } catch (err) {
    console.error('[Planner] PATCH /slots/:day/posted error:', err);
    res.status(500).json({ error: 'Failed to update slot' });
  }
});


// =============================================================================
// AGENT 2 — DAILY TOPIC SCOUT ROUTES
// =============================================================================

// GET /api/agents/scout/topics
// Returns today's scouted topics for the logged-in user
app.get('/api/agents/scout/topics', requireAuth, async (req, res) => {
  try {
    const col    = await agentCol('scouted_topics');
    const cutoff = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();
    const docs   = await col
      .find({ userId: req.user.id, scoutedAt: { $gte: cutoff } })
      .sort({ momentumScore: -1 })
      .limit(10)
      .toArray();
    if (!docs.length) return res.json({ success: true, topics: [], message: 'Click Run Scout to find trending topics in your niche' });
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, topics: docs });
  } catch (err) {
    console.error('[Scout] GET /topics error:', err);
    res.status(500).json({ error: 'Failed to fetch scouted topics' });
  }
});

// GET /api/agents/scout/queue
// Returns pending pipeline queue items (topics waiting to be scripted)
app.get('/api/agents/scout/queue', requireAuth, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const col   = await agentCol('pipeline_queue');
    const docs  = await col
      .find({ userId: req.user.id, status: 'pending' })
      .sort({ queuedAt: -1 })
      .limit(limit)
      .toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, queue: docs });
  } catch (err) {
    console.error('[Scout] GET /queue error:', err);
    res.status(500).json({ error: 'Failed to fetch pipeline queue' });
  }
});

// POST /api/agents/scout/run
// Manually triggers a topic scout for the logged-in user (dashboard button)
app.post('/api/agents/scout/run', requireAuth, async (req, res) => {
  try {
    res.json({ success: true, message: 'Topic scout started — check back in ~30 seconds' });
    const script = path.join(__dirname, 'daily_topic_scout_agent.py');
    exec(
      `python3 "${script}" --run-user ${req.user.id}`,
      { timeout: 120000 },
      (err, stdout, stderr) => {
        if (err) console.error('[Scout] Manual run error:', stderr);
        else     console.log('[Scout] Manual run complete for', req.user.id);
      }
    );
  } catch (err) {
    console.error('[Scout] POST /run error:', err);
    res.status(500).json({ error: 'Failed to start scout' });
  }
});


// =============================================================================
// AGENT 3 — COMPETITOR WATCHER ROUTES — ACTIVE
// Free to run: YouTube Data API (free quota) + OpenAI (already in stack)
// =============================================================================
if (FEATURES.competitorWatcher) {
// =============================================================================
// AGENT 3 — COMPETITOR WATCHER ROUTES
// =============================================================================

// GET /api/agents/competitor/list
// Returns all competitor channels being watched by the user
app.get('/api/agents/competitor/list', requireAuth, async (req, res) => {
  try {
    const col  = await agentCol('competitor_channels');
    const docs = await col
      .find({ userId: req.user.id, active: true })
      .sort({ channelName: 1 })
      .toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, competitors: docs });
  } catch (err) {
    console.error('[Competitor] GET /list error:', err);
    res.status(500).json({ error: 'Failed to fetch competitors' });
  }
});

// POST /api/agents/competitor/add
// Adds a YouTube channel to the watch list
// Body: { channelId: "UCxxxxx", channelName: "PsychFacts", notes: "..." }
app.post('/api/agents/competitor/add', requireAuth, async (req, res) => {
  try {
    const { channelId, channelName = '', notes = '' } = req.body;
    if (!channelId) return res.status(400).json({ error: 'channelId is required' });

    const result = await runAgentPy(
      'competitor_watcher_agent.py',
      `--add ${req.user.id} ${channelId} "${channelName.replace(/"/g, '')}"`,
      30000
    );
    if (result.error) return res.status(400).json(result);
    res.json({ success: true, competitor: result });
  } catch (err) {
    console.error('[Competitor] POST /add error:', err);
    res.status(500).json({ error: 'Failed to add competitor' });
  }
});

// DELETE /api/agents/competitor/:channelId
// Removes a competitor channel from the watch list
app.delete('/api/agents/competitor/:channelId', requireAuth, async (req, res) => {
  try {
    const col    = await agentCol('competitor_channels');
    const result = await col.updateOne(
      { userId: req.user.id, channelId: req.params.channelId },
      { $set: { active: false } }
    );
    if (result.matchedCount === 0) {
      return res.status(404).json({ error: 'Competitor not found' });
    }
    res.json({ success: true, message: 'Competitor removed from watch list' });
  } catch (err) {
    console.error('[Competitor] DELETE error:', err);
    res.status(500).json({ error: 'Failed to remove competitor' });
  }
});

// GET /api/agents/competitor/alerts
// Returns competitor alerts for the logged-in user
// Query: ?unread=true&limit=20
app.get('/api/agents/competitor/alerts', requireAuth, async (req, res) => {
  try {
    const unreadOnly = req.query.unread === 'true';
    const limit      = Math.min(parseInt(req.query.limit) || 20, 50);
    const col        = await agentCol('competitor_alerts');
    const query      = { userId: req.user.id };
    if (unreadOnly) query.read = false;
    const docs = await col.find(query).sort({ firedAt: -1 }).limit(limit).toArray();
    docs.forEach(d => { d._id = d._id.toString(); });

    const unreadCount = await col.countDocuments({ userId: req.user.id, read: false });
    res.json({ success: true, count: docs.length, unreadCount, alerts: docs });
  } catch (err) {
    console.error('[Competitor] GET /alerts error:', err);
    res.status(500).json({ error: 'Failed to fetch alerts' });
  }
});

// PATCH /api/agents/competitor/alerts/read
// Marks alerts as read
// Body: { alertIds: [...] }  — if empty, marks all as read
app.patch('/api/agents/competitor/alerts/read', requireAuth, async (req, res) => {
  try {
    const { alertIds } = req.body;
    const col          = await agentCol('competitor_alerts');
    const query        = { userId: req.user.id, read: false };
    if (alertIds && alertIds.length > 0) {
      const { ObjectId } = require('mongodb');
      query._id = { $in: alertIds.map(id => new ObjectId(id)) };
    }
    const result = await col.updateMany(query, { $set: { read: true } });
    res.json({ success: true, markedRead: result.modifiedCount });
  } catch (err) {
    console.error('[Competitor] PATCH /alerts/read error:', err);
    res.status(500).json({ error: 'Failed to mark alerts read' });
  }
});

// GET /api/agents/competitor/videos
// Returns recently detected competitor videos
app.get('/api/agents/competitor/videos', requireAuth, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    const col   = await agentCol('competitor_videos');
    const docs  = await col
      .find({ userId: req.user.id })
      .sort({ detectedAt: -1 })
      .limit(limit)
      .toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, videos: docs });
  } catch (err) {
    console.error('[Competitor] GET /videos error:', err);
    res.status(500).json({ error: 'Failed to fetch competitor videos' });
  }
});


} // end FEATURE_COMPETITOR_WATCHER

// =============================================================================
// SIDEBAR BADGES — lightweight counts for nav badge indicators
// =============================================================================

// GET /api/badges — returns previewCount and competitorsCount for sidebar badges
app.get('/api/badges', requireAuth, async (req, res) => {
  try {
    const today = new Date().toISOString().slice(0, 10);
    const [calendarDoc, pipeCount, compCount] = await Promise.all([
      agentCol('content_calendars').findOne({ userId: req.user.id, status: 'active' }),
      agentCol('pipeline_queue').countDocuments({ userId: req.user.id, status: { $in: ['pending', 'queued', 'preview'] } }),
      agentCol('competitor_channels').countDocuments({ userId: req.user.id, active: true }),
    ]);
    const calPending = (calendarDoc?.slots || []).filter(s =>
      !s.posted && ['planned', 'pending', 'approved'].includes(s.status)
    ).length;
    res.json({ success: true, previewCount: calPending + pipeCount, competitorsCount: compCount });
  } catch (err) {
    console.error('[Badges] GET /api/badges error:', err);
    res.status(500).json({ error: 'Failed to fetch badge counts' });
  }
});

// =============================================================================
// COMPETITORS — direct YouTube API routes (no Python dependency)
// =============================================================================

// GET /api/competitors — list all competitors + YouTube-suggested channels for the user's niche
app.get('/api/competitors', requireAuth, async (req, res) => {
  try {
    const col  = agentCol('competitor_channels');
    const docs = await col
      .find({ userId: req.user.id, active: true })
      .sort({ addedAt: -1 })
      .toArray();
    docs.forEach(d => { d._id = d._id.toString(); });

    // Auto-suggest: search YouTube for top channels in the user's niche
    let suggestedChannels = [];
    const apiKey = process.env.YOUTUBE_API_KEY;
    if (apiKey) {
      try {
        const user      = await User.findById(req.user.id);
        const nicheName = user?.nicheName
          || (user?.youtubeChannels || []).map(c => c.nicheName).find(Boolean)
          || null;

        if (nicheName) {
          const existingIds = new Set(docs.map(d => d.channelId));

          // search.list (100) + channels.list (1)
          const suggestQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.search + YOUTUBE_QUOTA_COSTS.channels, null).catch(() => ({ allowed: true }));
          if (!suggestQC.allowed) {
            console.warn(`[Quota] Competitor suggest blocked — ${suggestQC.reason}`);
          } else {
          // Search for channels in this niche
          const searchParams = new URLSearchParams({
            part: 'snippet', type: 'channel',
            q: `top ${nicheName} YouTube channels`,
            maxResults: '10', key: apiKey,
          });
          const searchRes  = await fetch(`https://www.googleapis.com/youtube/v3/search?${searchParams}`);
          logYoutubeQuota('search', YOUTUBE_QUOTA_COSTS.search, null).catch(() => {});
          const searchData = await searchRes.json();
          const channelIds = (searchData.items || [])
            .map(item => item.snippet?.channelId || item.id?.channelId)
            .filter(id => id && !existingIds.has(id))
            .slice(0, 5);

          if (channelIds.length) {
            const statsParams = new URLSearchParams({
              part: 'snippet,statistics', id: channelIds.join(','), key: apiKey,
            });
            const statsRes  = await fetch(`https://www.googleapis.com/youtube/v3/channels?${statsParams}`);
            logYoutubeQuota('channels.list', YOUTUBE_QUOTA_COSTS.channels, null).catch(() => {});
            const statsData = await statsRes.json();
            suggestedChannels = (statsData.items || []).map(ch => {
              const vidCount = parseInt(ch.statistics?.videoCount || 0);
              return {
                channelId:       ch.id,
                name:            ch.snippet?.title || '',
                thumbnail:       ch.snippet?.thumbnails?.default?.url || null,
                subscriberCount: parseInt(ch.statistics?.subscriberCount || 0),
                avgViews:        vidCount > 0
                  ? Math.round(parseInt(ch.statistics?.viewCount || 0) / vidCount) : 0,
              };
            });
            console.log(`[Competitors] ${suggestedChannels.length} suggested for niche "${nicheName}"`);
          }
          } // end quota-allowed block
        }
      } catch (e) {
        console.warn('[Competitors] Suggest failed (non-fatal):', e.message);
      }
    }

    res.json({ success: true, competitors: docs, count: docs.length, suggestedChannels });
  } catch (err) {
    console.error('[Competitors] GET error:', err);
    res.status(500).json({ error: 'Failed to fetch competitors' });
  }
});

// POST /api/competitors — add a competitor by YouTube channel URL
// Body: { channelUrl: "https://youtube.com/@channelname" }
app.post('/api/competitors', requireAuth, async (req, res) => {
  try {
    const { channelUrl } = req.body;
    if (!channelUrl || !channelUrl.trim()) {
      return res.status(400).json({ error: 'channelUrl is required' });
    }
    const apiKey = process.env.YOUTUBE_API_KEY;
    if (!apiKey) return res.status(503).json({ error: 'YOUTUBE_API_KEY not configured' });

    // Parse all YouTube channel URL formats → { type, value }
    const raw = channelUrl.trim();
    const parseYouTubeChannelUrl = (s) => {
      // Bare UC... ID or bare @handle
      if (/^UC[\w-]{22}$/.test(s)) return { type: 'id', value: s };
      const bareHandle = s.match(/^@([\w.-]+)$/);
      if (bareHandle) return { type: 'handle', value: bareHandle[1] };

      let pathname = s;
      try { pathname = new URL(s.startsWith('http') ? s : `https://${s}`).pathname.replace(/\/$/, ''); } catch { /* use raw */ }

      let m;
      if ((m = pathname.match(/\/channel\/(UC[\w-]{22})/))) return { type: 'id',       value: m[1] };
      if ((m = pathname.match(/\/@([\w.-]+)/)))              return { type: 'handle',   value: m[1] };
      if ((m = pathname.match(/\/user\/([\w.-]+)/)))         return { type: 'username', value: m[1] };
      if ((m = pathname.match(/\/c\/([\w.-]+)/)))            return { type: 'search',   value: m[1] };

      // @handle embedded anywhere in the raw string
      if ((m = s.match(/@([\w.-]+)/)))                       return { type: 'handle', value: m[1] };

      // Vanity URL: /customname with no recognised prefix
      if ((m = pathname.match(/^\/([\w.-]{3,})$/)) &&
          !['watch','shorts','playlist','results','feed','c','user','channel'].includes(m[1]))
        return { type: 'search', value: m[1] };

      return null;
    };

    const parsed = parseYouTubeChannelUrl(raw);
    if (!parsed) {
      return res.status(400).json({
        error: 'Could not parse channel URL. Supported: youtube.com/@handle, /channel/UC…, /user/name, /c/name',
      });
    }

    // Resolve the parsed type to a full channel object (snippet + statistics)
    // Worst case: channels.list (1) + search.list (100) + search.list (100) + search.list (100) = 301 units
    const addCompQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.channels + YOUTUBE_QUOTA_COSTS.search * 3, null).catch(() => ({ allowed: true }));
    if (!addCompQC.allowed) {
      console.warn(`[Quota] POST /api/competitors blocked — ${addCompQC.reason}`);
      return res.status(429).json({ error: `YouTube quota insufficient: ${addCompQC.reason}` });
    }

    const YT_CHANNELS = 'https://www.googleapis.com/youtube/v3/channels';
    const YT_SEARCH   = 'https://www.googleapis.com/youtube/v3/search';

    // Fetch channel by direct params; returns null if not found, throws on API error
    const fetchChannel = async (params) => {
      const r = await fetch(`${YT_CHANNELS}?${new URLSearchParams({ part: 'snippet,statistics', key: apiKey, ...params })}`);
      logYoutubeQuota('channels.list', YOUTUBE_QUOTA_COSTS.channels, null).catch(() => {});
      if (!r.ok) { const e = new Error(`YouTube API error (${r.status})`); e.status = 502; throw e; }
      return (await r.json()).items?.[0] || null;
    };

    // Search by name then fetch the top result's channel
    const searchThenFetch = async (query) => {
      const r = await fetch(`${YT_SEARCH}?${new URLSearchParams({ part: 'snippet', q: query, type: 'channel', maxResults: '1', key: apiKey })}`);
      logYoutubeQuota('search', YOUTUBE_QUOTA_COSTS.search, null).catch(() => {});
      if (!r.ok) { const e = new Error(`YouTube API error (${r.status})`); e.status = 502; throw e; }
      const sid = (await r.json()).items?.[0]?.id?.channelId;
      return sid ? fetchChannel({ id: sid }) : null;
    };

    let ch = null;
    if (parsed.type === 'id') {
      ch = await fetchChannel({ id: parsed.value });

    } else if (parsed.type === 'handle') {
      ch = await fetchChannel({ forHandle: `@${parsed.value}` });
      if (!ch) ch = await searchThenFetch(parsed.value); // older channels may not have a handle

    } else if (parsed.type === 'username') {
      ch = await fetchChannel({ forUsername: parsed.value });
      if (!ch) ch = await searchThenFetch(parsed.value); // legacy username may not match forUsername

    } else { // 'search' — /c/customname or vanity path
      ch = await searchThenFetch(parsed.value);
    }

    if (!ch) return res.status(404).json({ error: 'Channel not found on YouTube — double-check the URL' });

    const channelId      = ch.id;
    const channelName    = ch.snippet?.title || 'Unknown';
    const thumbnail      = ch.snippet?.thumbnails?.default?.url || null;
    const subscriberCount = parseInt(ch.statistics?.subscriberCount  || '0', 10);
    const videoCount      = parseInt(ch.statistics?.videoCount       || '0', 10);
    const totalViews      = parseInt(ch.statistics?.viewCount        || '0', 10);
    const avgViews        = videoCount > 0 ? Math.round(totalViews / videoCount) : 0;

    // Compute upload frequency from last 10 video dates
    let uploadFrequencyDays = null;
    try {
      const srchParams = new URLSearchParams({ part: 'snippet', channelId, order: 'date', type: 'video', maxResults: 10, key: apiKey });
      const srchRes    = await fetch(`https://www.googleapis.com/youtube/v3/search?${srchParams}`);
      logYoutubeQuota('search', YOUTUBE_QUOTA_COSTS.search, null).catch(() => {});
      if (srchRes.ok) {
        const srchData = await srchRes.json();
        const dates    = (srchData.items || [])
          .map(item => new Date(item.snippet?.publishedAt).getTime())
          .filter(Boolean)
          .sort((a, b) => b - a);
        if (dates.length >= 2) {
          const gaps = [];
          for (let i = 0; i < dates.length - 1; i++) gaps.push((dates[i] - dates[i + 1]) / 86400000);
          uploadFrequencyDays = Math.round(gaps.reduce((a, b) => a + b, 0) / gaps.length);
        }
      }
    } catch { /* frequency stays null */ }

    // Upsert into MongoDB
    const col      = agentCol('competitor_channels');
    const existing = await col.findOne({ userId: req.user.id, channelId });
    const now      = new Date().toISOString();
    if (existing) {
      await col.updateOne(
        { _id: existing._id },
        { $set: { active: true, channelName, thumbnail, subscriberCount, videoCount, totalViews, avgViews, uploadFrequencyDays, updatedAt: now, lastChecked: now } }
      );
    } else {
      await col.insertOne({
        userId: req.user.id, channelId, channelName, thumbnail,
        subscriberCount, videoCount, totalViews, avgViews, uploadFrequencyDays,
        active: true, addedAt: now, updatedAt: now, lastChecked: now,
      });
    }

    console.log(`[Competitors] Added "${channelName}" (${channelId}) for user ${req.user.id} — ${subscriberCount.toLocaleString()} subs, ${avgViews.toLocaleString()} avg views`);
    res.json({ success: true, competitor: { channelId, channelName, thumbnail, subscriberCount, avgViews, uploadFrequencyDays } });
  } catch (err) {
    console.error('[Competitors] POST error:', err);
    res.status(err.status || 500).json({ error: err.message || 'Failed to add competitor' });
  }
});

// DELETE /api/competitors/:channelId — remove a competitor
app.delete('/api/competitors/:channelId', requireAuth, async (req, res) => {
  try {
    const col    = agentCol('competitor_channels');
    const result = await col.updateOne(
      { userId: req.user.id, channelId: req.params.channelId },
      { $set: { active: false, updatedAt: new Date().toISOString() } }
    );
    if (result.matchedCount === 0) return res.status(404).json({ error: 'Competitor not found' });
    res.json({ success: true });
  } catch (err) {
    console.error('[Competitors] DELETE error:', err);
    res.status(500).json({ error: 'Failed to remove competitor' });
  }
});

// =============================================================================
// AGENT 4 — SCRIPT RESEARCH ROUTES
// =============================================================================

// POST /api/agents/research/brief
// Research a video title and generate a script brief
// Body: { title: "...", format: "short|long|standard", niche: "..." }
app.post('/api/agents/research/brief', requireAuth, async (req, res) => {
  try {
    const { title, format = 'short', niche = '' } = req.body;
    if (!title || title.trim().length < 5) {
      return res.status(400).json({ error: 'Title must be at least 5 characters' });
    }

    // Respond immediately — research runs in background (30–90s)
    res.json({ success: true, message: 'Research started — brief will be ready in ~60 seconds', title });

    const script     = path.join(__dirname, 'script_research_agent.py');
    const safeTitle  = title.replace(/"/g, '\\"').slice(0, 200);
    const safeNiche  = niche.replace(/"/g, '').slice(0, 100);
    exec(
      `python3 "${script}" --research "${safeTitle}" --format ${format} --niche "${safeNiche}"`,
      { timeout: 180000 },
      (err, stdout, stderr) => {
        if (err) console.error('[Research] Brief generation error:', stderr);
        else     console.log('[Research] Brief generated for:', title.slice(0, 50));
      }
    );
  } catch (err) {
    console.error('[Research] POST /brief error:', err);
    res.status(500).json({ error: 'Failed to start research' });
  }
});

// GET /api/agents/research/briefs
// Returns pending (unscripted) research briefs for the logged-in user
app.get('/api/agents/research/briefs', requireAuth, async (req, res) => {
  try {
    const limit  = Math.min(parseInt(req.query.limit) || 20, 50);
    const status = req.query.status || 'pending';  // pending | scripted | all
    const col    = await agentCol('script_briefs');
    const query  = { userId: req.user.id };
    if (status !== 'all') query.status = status;
    const docs = await col
      .find(query)
      .sort({ createdAt: -1 })
      .limit(limit)
      .toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, briefs: docs });
  } catch (err) {
    console.error('[Research] GET /briefs error:', err);
    res.status(500).json({ error: 'Failed to fetch briefs' });
  }
});

// GET /api/agents/research/briefs/:briefId
// Returns a single research brief by ID
app.get('/api/agents/research/briefs/:briefId', requireAuth, async (req, res) => {
  try {
    const { ObjectId } = require('mongodb');
    const col          = await agentCol('script_briefs');
    const doc          = await col.findOne({
      _id:    new ObjectId(req.params.briefId),
      userId: req.user.id,
    });
    if (!doc) return res.status(404).json({ error: 'Brief not found' });
    doc._id = doc._id.toString();
    res.json({ success: true, brief: doc });
  } catch (err) {
    console.error('[Research] GET /briefs/:id error:', err);
    res.status(500).json({ error: 'Failed to fetch brief' });
  }
});

// PATCH /api/agents/research/briefs/:briefId/scripted
// Marks a brief as scripted
// Body: { scriptId: "..." }
app.patch('/api/agents/research/briefs/:briefId/scripted', requireAuth, async (req, res) => {
  try {
    const { ObjectId } = require('mongodb');
    const col          = await agentCol('script_briefs');
    const result       = await col.updateOne(
      { _id: new ObjectId(req.params.briefId), userId: req.user.id },
      { $set: {
        status:     'scripted',
        scriptId:   req.body.scriptId || '',
        scriptedAt: new Date().toISOString(),
      }}
    );
    if (result.matchedCount === 0) return res.status(404).json({ error: 'Brief not found' });
    res.json({ success: true, message: 'Brief marked as scripted' });
  } catch (err) {
    console.error('[Research] PATCH /scripted error:', err);
    res.status(500).json({ error: 'Failed to update brief' });
  }
});

// =============================================================================
// SCRIPTS — structured OpenAI scripts tied to calendar slots
// =============================================================================

// GET /api/scripts — returns generated scripts for the user's active calendar.
// On first call (or when fewer than 5 scripts exist), generates up to 5 structured
// scripts for upcoming planned slots via OpenAI and caches them to MongoDB.
app.get('/api/scripts', requireAuth, async (req, res) => {
  try {
    const scriptsCol   = agentCol('scripts');
    const calendarsCol = agentCol('content_calendars');

    const today = new Date().toISOString().slice(0, 10);

    // Read today's cached scripts only
    let scripts = await scriptsCol
      .find({ userId: req.user.id, scheduledDate: today })
      .sort({ slotDay: 1, videoIndex: 1 })
      .toArray();
    scripts.forEach(s => { s._id = s._id.toString(); });

    // Need an active calendar to generate from
    const calendar = await calendarsCol.findOne({ userId: req.user.id, status: 'active' });
    if (!calendar) return res.json({ success: true, scripts, count: scripts.length, hasCalendar: false, today });

    // Generate for today's slots that are missing scripts (max 5 per request)
    const generatedKeys = new Set(scripts.map(s => `${s.slotDay}_${s.videoIndex || 1}`));
    const missing       = (calendar.slots || [])
      .filter(s => s.date === today && ['planned', 'pending'].includes(s.status) && !generatedKeys.has(`${s.day}_${s.videoIndex || 1}`))
      .slice(0, 5);

    if (missing.length > 0 && process.env.OPENAI_API_KEY) {
      const { OpenAI } = require('openai');
      const openai    = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
      const nicheName = calendar.nicheName || '';

      // Fetch competitor + past-video context once, reuse for all slots in this request
      const scriptCtx = await fetchScriptContext(req.user.id, calendar.channelId || '');

      for (const slot of missing) {
        try {
          console.log(`[Scripts] Generating structured script for "${slot.title}" (day ${slot.day})…`);
          const structured = await generateStructuredScript(slot.title, nicheName, slot.type, openai, scriptCtx);
          const doc = {
            userId:            req.user.id,
            calendarId:        calendar._id.toString(),
            slotDay:           slot.day,
            videoIndex:        slot.videoIndex || 1,
            title:             slot.title,
            type:              slot.type,
            scheduledDate:     slot.date,
            nicheName,
            hook:              structured.hook              || '',
            mainPoints:        Array.isArray(structured.mainPoints) ? structured.mainPoints.slice(0, 5) : [],
            cta:               structured.cta               || '',
            estimatedDuration: structured.estimatedDuration || (slot.type === 'Long-form' ? '5:00' : '0:55'),
            fullScript:        structured.fullScript         || '',
            scriptContext:     structured.scriptContext      || null,
            generatedAt:       new Date().toISOString(),
          };
          const ins = await scriptsCol.insertOne(doc);
          scripts.push({ ...doc, _id: ins.insertedId.toString() });
          console.log(`[Scripts] Saved: "${slot.title}"`);
        } catch (err) {
          console.error(`[Scripts] Failed for "${slot.title}":`, err.message);
        }
      }
      scripts.sort((a, b) => (a.scheduledDate || '') < (b.scheduledDate || '') ? -1 : 1);
    }

    res.json({ success: true, scripts, count: scripts.length, hasCalendar: true, today });
  } catch (err) {
    console.error('[Scripts] GET error:', err);
    res.status(500).json({ error: 'Failed to fetch scripts' });
  }
});

// GET /api/diag/slots-status — public (no auth) — shows today's slot status+posted distribution
// No sensitive data exposed — just aggregate counts for operational monitoring
app.get('/api/diag/slots-status', async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const slotsCol = agentCol('calendar_slots');
    const all      = await slotsCol.find({ date: today }).project({ status: 1, posted: 1, scheduledPostTime: 1, _id: 0 }).toArray();
    const dist     = {};
    for (const s of all) {
      const k = `status:${s.status}|posted:${s.posted}`;
      dist[k] = (dist[k] || 0) + 1;
    }
    const times = all.map(s => s.scheduledPostTime).sort();
    res.json({ today, total: all.length, distribution: dist, scheduledTimes: times });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/diag/slots-today — returns today's calendar_slots with status breakdown
// Protected by x-cron-secret header so Railway health or the admin can call it without a session
app.get('/api/diag/slots-today', async (req, res) => {
  if (req.headers['x-cron-secret'] !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorised' });
  }
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const slotsCol = agentCol('calendar_slots');
    const slots    = await slotsCol.find({ date: today }).toArray();
    const byStatus = {};
    for (const s of slots) byStatus[s.status] = (byStatus[s.status] || 0) + 1;
    res.json({
      today,
      totalSlots: slots.length,
      byStatus,
      slots: slots.map(s => ({
        _id:               String(s._id),
        title:             s.title,
        status:            s.status,
        posted:            s.posted,
        scheduledPostTime: s.scheduledPostTime,
        pipelineStatus:    s.pipelineStatus,
        pipelineError:     s.pipelineError,
        retryCount:        s.retryCount,
        userId:            s.userId,
        channelId:         s.channelId,
      })),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/diag/rescue-missed-slots — immediately reschedule today's missed/stuck slots
// Protected by x-cron-secret header
app.post('/api/diag/rescue-missed-slots', async (req, res) => {
  if (req.headers['x-cron-secret'] !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorised' });
  }
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const slotsCol = agentCol('calendar_slots');
    const stuck    = await slotsCol.find({
      date: today, status: { $in: ['missed', 'skipped', 'processing'] }, posted: false,
    }).toArray();
    let rescued = 0;
    for (let i = 0; i < stuck.length; i++) {
      const rescheduleAt = new Date(Date.now() + (i + 1) * 10 * 60 * 1000).toISOString();
      await slotsCol.updateOne(
        { _id: stuck[i]._id },
        { $set: { status: 'scheduled', scheduledPostTime: rescheduleAt, pipelineStatus: 'manual-rescue', pipelineError: null }, $inc: { retryCount: 1 } }
      ).catch(() => {});
      rescued++;
      console.log(`[Rescue] "${stuck[i].title}" rescheduled → ${rescheduleAt}`);
    }
    res.json({ success: true, rescued, rescheduledTitles: stuck.map(s => s.title) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/agents/research/queue/process
// Admin/cron endpoint — processes pending pipeline queue items
// Protected by CRON_SECRET header
app.post('/api/agents/research/queue/process', async (req, res) => {
  if (req.headers['x-cron-secret'] !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorised' });
  }
  res.json({ success: true, message: 'Queue processing started' });
  const script = path.join(__dirname, 'script_research_agent.py');
  exec(`python3 "${script}" --process-queue ""`, { timeout: 300000 }, (err, stdout, stderr) => {
    if (err) console.error('[Research] Queue process error:', stderr);
    else     console.log('[Research] Queue processed:', stdout.slice(0, 200));
  });
});


// =============================================================================
// SHARED — AGENT HEALTH CHECK
// =============================================================================

// GET /api/agents/status
// Returns status of all four agents (last run time, counts, etc.)
app.get('/api/agents/status', requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;
    const db     = getDb();

    const [
      calendarExists,
      scoutedToday,
      pendingAlerts,
      pendingBriefs,
      queueDepth,
    ] = await Promise.all([
      db.collection('content_calendars').countDocuments({ userId, status: 'active' }),
      db.collection('scouted_topics').countDocuments({
        userId,
        scoutedAt: { $gte: new Date(Date.now() - 24*60*60*1000).toISOString() }
      }),
      db.collection('competitor_alerts').countDocuments({ userId, read: false }),
      db.collection('script_briefs').countDocuments({ userId, status: 'pending' }),
      db.collection('pipeline_queue').countDocuments({ userId, status: 'pending' }),
    ]);

    res.json({
      success: true,
      baseModel: true,
      featureFlags: FEATURES,
      agents: {
        nicheEngine: {
          name:    'Niche Engine',
          status:  'ready',
          enabled: true,
        },
        contentPlanner: {
          name:              'Content Planner',
          hasActiveCalendar: calendarExists > 0,
          status:            calendarExists > 0 ? 'active' : 'ready',
          enabled:           true,
        },
        topicScout: {
          name:               'Daily Topic Scout',
          topicsTodayCount:   scoutedToday,
          pipelineQueueDepth: queueDepth,
          status:             scoutedToday > 0 ? 'active' : 'ready',
          enabled:            true,
        },
        scriptResearch: {
          name:          'Script Research',
          pendingBriefs: pendingBriefs,
          status:        pendingBriefs > 0 ? 'active' : 'ready',
          enabled:       true,
        },
        voiceover: {
          name:    'Auto Voiceover',
          status:  FEATURES.voiceover ? 'active' : 'shelved',
          enabled: !!FEATURES.voiceover,
        },
        preview: {
          name:    'Preview Engine',
          status:  FEATURES.preview ? 'active' : 'shelved',
          enabled: !!FEATURES.preview,
        },
        learningLoop: {
          name:    'AI Learning Loop',
          status:  'active',
          enabled: true,
        },
      },
    });
  } catch (err) {
    console.error('[Agents] GET /status error:', err);
    res.status(500).json({ error: 'Failed to fetch agent status' });
  }
});


// =============================================================================
// TIER 2 AGENTS — Trend Scraper, Auto-Voiceover, Competitor Scraper
// =============================================================================
// =============================================================================
// VIRALITYY — Tier 2 Agent API Routes (Agents 5, 6, 8)
// =============================================================================
// HOW TO ADD: Paste into server.js BEFORE app.listen(), after the Tier 1 routes.
//
//   Agent 5 — Trend Scraper           → /api/agents/trends/*
//   Agent 6 — Auto-Voiceover          → /api/agents/voiceover/*
//   Agent 8 — Competitor Scraper      → /api/agents/scraper/*
//
// Also registers cron jobs for Agents 5 and 8.
// =============================================================================

// T2 agent helper — aliases to unified runAgentPy above
const runAgentPyT2 = runAgentPy;
const col2         = agentCol;


// Agent 5 + 8 cron jobs — registered via registerCronJobs() after MongoDB connects


// AGENT 5 — TREND SCRAPER ROUTES — SHELVED
// Enable: FEATURE_TREND_SCRAPER=true
if (FEATURES.trendScraper) {
// =============================================================================
// AGENT 5 — TREND SCRAPER ROUTES
// =============================================================================

// GET /api/agents/trends
// Get stored trend signals for the logged-in user
// Query: ?min_score=0.5
app.get('/api/agents/trends', requireAuth, async (req, res) => {
  try {
    const minScore = parseFloat(req.query.min_score) || 0;
    const cutoff   = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const col      = await col2('trend_signals');
    const docs     = await col.find({
      userId:       req.user.id,
      scrapedAt:    { $gte: cutoff },
      momentumScore:{ $gte: minScore },
    }).sort({ momentumScore: -1 }).limit(10).toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, trends: docs });
  } catch (err) {
    console.error('[Trends] GET error:', err);
    res.status(500).json({ error: 'Failed to fetch trend signals' });
  }
});

// POST /api/agents/trends/run
// Manually trigger trend scraping for the logged-in user
app.post('/api/agents/trends/run', requireAuth, async (req, res) => {
  try {
    res.json({ success: true, message: 'Trend scraper started — results ready in ~60 seconds' });
    const script = path.join(__dirname, 'trend_scraper_agent.py');
    exec(
      `python3 "${script}" --run-user ${req.user.id}`,
      { timeout: 180000 },
      (err, stdout, stderr) => {
        if (err) console.error('[Trends] Manual run error:', stderr);
        else     console.log('[Trends] Manual run complete:', stdout.slice(0, 200));
      }
    );
  } catch (err) {
    console.error('[Trends] POST /run error:', err);
    res.status(500).json({ error: 'Failed to start trend scraper' });
  }
});

// GET /api/agents/trends/platforms
// Returns which platforms are configured and active
app.get('/api/agents/trends/platforms', requireAuth, (req, res) => {
  res.json({
    success: true,
    platforms: {
      reddit: {
        active:      !!(process.env.REDDIT_CLIENT_ID && process.env.REDDIT_CLIENT_SECRET),
        requires:    ['REDDIT_CLIENT_ID', 'REDDIT_CLIENT_SECRET'],
        weight:      '40% of signal',
      },
      google_trends: {
        active:      true,  // pytrends needs no API key
        requires:    [],
        weight:      '35% of signal',
      },
      twitter: {
        active:      !!process.env.TWITTER_BEARER_TOKEN,
        requires:    ['TWITTER_BEARER_TOKEN'],
        weight:      '25% of signal',
      },
    },
  });
});


} // end FEATURE_TREND_SCRAPER

// =============================================================================
// AGENT 6 — AUTO-VOICEOVER ROUTES
// =============================================================================

// POST /api/agents/voiceover/render
// Render a script to audio for a specific channel
// Body: { script, channelId, videoId, nicheId, humanise: true }
app.post('/api/agents/voiceover/render', requireAuth, async (req, res) => {
  try {
    const { script, channelId = '', videoId = '', nicheId = '', humanise = true } = req.body;
    if (!script || script.trim().length < 10) {
      return res.status(400).json({ error: 'Script must be at least 10 characters' });
    }
    if (!process.env.GEMINI_API_KEY) {
      return res.status(503).json({ error: 'GEMINI_API_KEY not configured — add it in Railway env vars' });
    }

    res.json({ success: true, message: 'Voiceover render started — ready in ~30 seconds' });

    // Write script to temp file and pass path to Python
    const tmpFile = `/tmp/vly_script_${req.user.id}_${Date.now()}.txt`;
    require('fs').writeFileSync(tmpFile, script);

    const scriptPath = path.join(__dirname, 'auto_voiceover_agent.py');
    exec(
      `python3 "${scriptPath}" --render "${tmpFile}" --user ${req.user.id} --channel "${channelId}" --niche "${nicheId}"`,
      { timeout: 300000 },
      (err, stdout, stderr) => {
        require('fs').unlink(tmpFile, () => {}); // cleanup temp
        if (err) console.error('[Voiceover] Render error:', stderr);
        else     console.log('[Voiceover] Render complete:', stdout.slice(0, 200));
      }
    );
  } catch (err) {
    console.error('[Voiceover] POST /render error:', err);
    res.status(500).json({ error: 'Failed to start voiceover render' });
  }
});

// GET /api/agents/voiceover/files
// Returns rendered audio files for the logged-in user
app.get('/api/agents/voiceover/files', requireAuth, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    const col   = await col2('audio_renders');
    const docs  = await col.find({ userId: req.user.id })
      .sort({ renderedAt: -1 }).limit(limit).toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, files: docs });
  } catch (err) {
    console.error('[Voiceover] GET /files error:', err);
    res.status(500).json({ error: 'Failed to fetch audio files' });
  }
});

// GET /api/agents/voiceover/voices
// Returns available ElevenLabs voices
app.get('/api/agents/voiceover/voices', requireAuth, async (req, res) => {
  try {
    if (!process.env.GEMINI_API_KEY) {
      return res.status(503).json({ error: 'GEMINI_API_KEY not configured — add it in Railway env vars' });
    }
    const result = await runAgentPyT2('auto_voiceover_agent.py', '--list-voices', 15000);
    res.json({ success: true, voices: result });
  } catch (err) {
    console.error('[Voiceover] GET /voices error:', err);
    res.status(500).json({ error: 'Failed to fetch voice list' });
  }
});

// GET /api/agents/voiceover/channel/:channelId/voice
// Returns the voice assigned to a specific channel
app.get('/api/agents/voiceover/channel/:channelId/voice', requireAuth, async (req, res) => {
  try {
    const col = await col2('channel_voices');
    const doc = await col.findOne({
      userId:    req.user.id,
      channelId: req.params.channelId,
    });
    if (!doc) return res.json({ success: true, voice: null, message: 'No voice assigned — using niche default' });
    doc._id = doc._id.toString();
    res.json({ success: true, voice: doc });
  } catch (err) {
    console.error('[Voiceover] GET /channel/:id/voice error:', err);
    res.status(500).json({ error: 'Failed to fetch channel voice' });
  }
});

// POST /api/agents/voiceover/channel/:channelId/voice
// Assign an existing ElevenLabs voice to a channel
// Body: { voiceId: "...", voiceName: "..." }
app.post('/api/agents/voiceover/channel/:channelId/voice', requireAuth, async (req, res) => {
  try {
    const { voiceId, voiceName = '' } = req.body;
    if (!voiceId) return res.status(400).json({ error: 'voiceId is required' });

    const col = await col2('channel_voices');
    const doc = {
      userId:    req.user.id,
      channelId: req.params.channelId,
      voiceId,
      voiceName: voiceName || voiceId,
      type:      'assigned',
      createdAt: new Date().toISOString(),
    };
    await col.updateOne(
      { userId: req.user.id, channelId: req.params.channelId },
      { $set: doc },
      { upsert: true }
    );
    res.json({ success: true, message: `Voice "${voiceName}" assigned to channel`, voice: doc });
  } catch (err) {
    console.error('[Voiceover] POST /channel/:id/voice error:', err);
    res.status(500).json({ error: 'Failed to assign voice' });
  }
});

// POST /api/agents/voiceover/estimate
// Estimate audio duration for a script without rendering
// Body: { script }
app.post('/api/agents/voiceover/estimate', requireAuth, (req, res) => {
  const { script = '' } = req.body;
  const wordCount  = script.trim().split(/\s+/).length;
  const durationS  = Math.round((wordCount / 150) * 60);  // 150 WPM
  const durationMs = durationS * 1000;
  res.json({
    success: true,
    wordCount,
    estimatedDurationSeconds: durationS,
    estimatedDurationMs:      durationMs,
    note: 'Based on 150 words per minute average speaking pace',
  });
});


// AGENT 8 — COMPETITOR CONTENT SCRAPER ROUTES — ACTIVE
// Free to run: YouTube Data API + OpenAI (already in stack)
if (FEATURES.competitorScraper) {
// =============================================================================
// AGENT 8 — COMPETITOR CONTENT SCRAPER ROUTES
// =============================================================================

// POST /api/agents/scraper/video/:videoId
// Scrape a single competitor video
// Query: ?force=true to re-scrape
app.post('/api/agents/scraper/video/:videoId', requireAuth, async (req, res) => {
  try {
    const { videoId } = req.params;
    const force       = req.query.force === 'true';
    res.json({ success: true, message: `Scraping ${videoId} — ready in ~30 seconds` });

    const script = path.join(__dirname, 'competitor_content_scraper.py');
    const forceFlag = force ? '--force' : '';
    exec(
      `python3 "${script}" --scrape ${videoId} --user ${req.user.id}`,
      { timeout: 60000 },
      (err, stdout, stderr) => {
        if (err) console.error('[Scraper] Scrape error:', stderr);
        else     console.log('[Scraper] Scrape complete:', videoId);
      }
    );
  } catch (err) {
    console.error('[Scraper] POST /video/:id error:', err);
    res.status(500).json({ error: 'Failed to start scrape' });
  }
});

// POST /api/agents/scraper/bulk
// Scrape all pending competitor videos for the logged-in user
app.post('/api/agents/scraper/bulk', requireAuth, async (req, res) => {
  try {
    res.json({ success: true, message: 'Bulk scrape started — check /scraped for results' });
    const script = path.join(__dirname, 'competitor_content_scraper.py');
    exec(
      `python3 "${script}" --bulk ${req.user.id}`,
      { timeout: 300000 },
      (err, stdout, stderr) => {
        if (err) console.error('[Scraper] Bulk error:', stderr);
        else     console.log('[Scraper] Bulk complete:', stdout.slice(0, 200));
      }
    );
  } catch (err) {
    console.error('[Scraper] POST /bulk error:', err);
    res.status(500).json({ error: 'Failed to start bulk scrape' });
  }
});

// GET /api/agents/scraper/videos/:videoId
// Get stored scraped data for a specific video
app.get('/api/agents/scraper/videos/:videoId', requireAuth, async (req, res) => {
  try {
    const col = await col2('competitor_scraped');
    const doc = await col.findOne({ videoId: req.params.videoId, userId: req.user.id });
    if (!doc) return res.status(404).json({ error: 'Video not scraped yet' });
    doc._id = doc._id.toString();
    res.json({ success: true, data: doc });
  } catch (err) {
    console.error('[Scraper] GET /videos/:id error:', err);
    res.status(500).json({ error: 'Failed to fetch scraped video' });
  }
});

// GET /api/agents/scraper/scraped
// List recently scraped competitor videos for the user
app.get('/api/agents/scraper/scraped', requireAuth, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    const col   = await col2('competitor_scraped');
    const docs  = await col.find({ userId: req.user.id })
      .sort({ scrapedAt: -1 }).limit(limit).toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, videos: docs });
  } catch (err) {
    console.error('[Scraper] GET /scraped error:', err);
    res.status(500).json({ error: 'Failed to fetch scraped videos' });
  }
});

// GET /api/agents/scraper/summary
// Get aggregated analysis across all scraped competitor content
app.get('/api/agents/scraper/summary', requireAuth, async (req, res) => {
  try {
    const result = await runAgentPyT2(
      'competitor_content_scraper.py',
      `--summary ${req.user.id}`,
      30000
    );
    res.json({ success: true, summary: result });
  } catch (err) {
    console.error('[Scraper] GET /summary error:', err);
    res.status(500).json({ error: 'Failed to generate summary' });
  }
});

// GET /api/agents/scraper/search
// Search scraped competitor content
// Query: ?q=psychology+tricks
app.get('/api/agents/scraper/search', requireAuth, async (req, res) => {
  try {
    const query = req.query.q || '';
    if (query.trim().length < 2) {
      return res.status(400).json({ error: 'Query must be at least 2 characters' });
    }

    const col  = await col2('competitor_scraped');
    const regex = { $regex: query, $options: 'i' };
    const docs  = await col.find({
      userId: req.user.id,
      $or: [{ title: regex }, { description: regex }, { tags: regex }],
    }).sort({ scrapedAt: -1 }).limit(20).toArray();
    docs.forEach(d => { d._id = d._id.toString(); });
    res.json({ success: true, count: docs.length, query, results: docs });
  } catch (err) {
    console.error('[Scraper] GET /search error:', err);
    res.status(500).json({ error: 'Search failed' });
  }
});

// GET /api/agents/scraper/config
// Returns scraper configuration and capability status
app.get('/api/agents/scraper/config', requireAuth, (req, res) => {
  res.json({
    success: true,
    config: {
      youtubeApi: {
        active:   !!process.env.YOUTUBE_API_KEY,
        role:     'Primary data source — always used',
      },
      firecrawl: {
        active:   !!process.env.FIRECRAWL_API_KEY,
        role:     'Deep page scraping — tags, pinned comments, chapters',
        requires: ['FIRECRAWL_API_KEY'],
      },
      aiInsights: {
        active:   !!process.env.OPENAI_API_KEY,
        role:     'GPT-4o insight generation per video',
      },
    },
  });
});


} // end FEATURE_COMPETITOR_SCRAPER


// =============================================================================
// MISSING ROUTES — added to match all frontend API calls
// =============================================================================

// ── PREVIEW QUEUE ────────────────────────────────────────────────────────────

// GET /api/agents/preview/queue
app.get('/api/agents/preview/queue', requireAuth, async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const videos = await db.collection('pipeline_queue')
      .find({ userId: req.user.id, status: { $in: ['pending', 'queued', 'preview'] } })
      .sort({ queuedAt: -1 }).limit(20).toArray();
    videos.forEach(v => { v._id = v._id.toString(); });
    res.json({ success: true, queue: videos, count: videos.length });
  } catch (err) {
    console.error('[Preview] GET /queue error:', err);
    res.status(500).json({ error: 'Failed to fetch preview queue' });
  }
});

// POST /api/agents/preview/approve
app.post('/api/agents/preview/approve', requireAuth, async (req, res) => {
  try {
    const { videoId } = req.body;
    if (!videoId) return res.status(400).json({ error: 'videoId required' });
    const db = mongoose.connection.db;
    const { ObjectId } = require('mongodb');
    let query;
    try { query = { _id: new ObjectId(videoId) }; } catch { query = { _id: videoId }; }
    await db.collection('pipeline_queue').updateOne(
      { ...query, userId: req.user.id },
      { $set: { status: 'approved', approvedAt: new Date().toISOString() } }
    );
    res.json({ success: true, message: 'Video approved — will post on schedule' });
  } catch (err) {
    console.error('[Preview] POST /approve error:', err);
    res.status(500).json({ error: 'Failed to approve video' });
  }
});

// POST /api/agents/preview/skip
app.post('/api/agents/preview/skip', requireAuth, async (req, res) => {
  try {
    const { videoId } = req.body;
    if (!videoId) return res.status(400).json({ error: 'videoId required' });
    const db = mongoose.connection.db;
    const { ObjectId } = require('mongodb');
    let query;
    try { query = { _id: new ObjectId(videoId) }; } catch { query = { _id: videoId }; }
    await db.collection('pipeline_queue').updateOne(
      { ...query, userId: req.user.id },
      { $set: { status: 'skipped', skipReason: 'user_skipped', skippedAt: new Date().toISOString() } }
    );
    res.json({ success: true, message: 'Video skipped and removed from queue' });
  } catch (err) {
    console.error('[Preview] POST /skip error:', err);
    res.status(500).json({ error: 'Failed to skip video' });
  }
});

// ── ANALYTICS ────────────────────────────────────────────────────────────────

// Shared helper — fetches live YouTube Analytics for a user and caches in MongoDB for 30 min.
// Returns a payload with success:true on any result, or null only on total unrecoverable failure.
async function fetchLiveAnalytics(userId) {
  const cacheCol = agentCol('analytics_cache');
  const CACHE_TTL_MS = 30 * 60 * 1000;

  // Serve from cache if fresh
  const cached = await cacheCol.findOne({ userId: String(userId) }).catch(() => null);
  if (cached?.data && (Date.now() - new Date(cached.cachedAt).getTime()) < CACHE_TTL_MS) {
    console.log(`[Analytics] Cache hit for user ${userId}`);
    return { ...cached.data, fromCache: true };
  }

  const user = await User.findById(userId);
  if (!user) return null;
  const channels = user.youtubeChannels || [];
  const ch = channels.find(c => !c.paused && c.channelId) || channels[0];
  if (!ch) return { success: true, noChannel: true };

  // Quota check: channels.list (1) + videos.list (1) = 2 units.
  // search.list was removed (100 units) — we now use calendar_slots instead.
  const analyticsQC2 = await checkYoutubeQuota(2, String(userId)).catch(() => ({ allowed: true }));
  if (!analyticsQC2.allowed) {
    console.warn(`[Quota] fetchLiveAnalytics blocked for user ${userId} — ${analyticsQC2.reason}`);
    // Return stale cache rather than a blank 500 error
    if (cached?.data) return { ...cached.data, fromCache: true, quotaLimited: true };
    return { success: true, quotaLimited: true, channelName: ch.channelName, channelId: ch.channelId,
      stats: { totalViews: 0, totalSubs: 0, watchTimeMinutes: null, subscribersGained: null, avgViews: null, videosPosted: 0 },
      topVideos: [], dailyViews: [] };
  }

  // Shared token refresh helper
  const refreshAccessToken = async (token) => {
    const refreshToken = ch.refreshToken || user.googleRefreshToken;
    if (!refreshToken) return token;
    try {
      const tokRes = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          client_id:     process.env.YOUTUBE_CLIENT_ID,
          client_secret: process.env.YOUTUBE_CLIENT_SECRET,
          refresh_token: refreshToken,
          grant_type:    'refresh_token',
        }),
      });
      const tokData = await tokRes.json();
      if (tokData.access_token) {
        await User.updateOne(
          { 'youtubeChannels.channelId': ch.channelId },
          { $set: { 'youtubeChannels.$.accessToken': tokData.access_token } }
        ).catch(() => {});
        console.log(`[Analytics] Token refreshed for channel ${ch.channelId}`);
        return tokData.access_token;
      }
    } catch (e) { console.warn('[Analytics] Token refresh failed:', e.message); }
    return token;
  };

  const apiKey = process.env.YOUTUBE_API_KEY;

  // 1. Channel-level statistics (1 unit)
  // Prefer API key for public channel data — no OAuth needed, immune to token expiry.
  let chanStats = {};
  try {
    const chanUrl = apiKey
      ? `https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${ch.channelId}&key=${apiKey}`
      : `https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${ch.channelId}`;
    const chanHeaders = apiKey ? {} : { Authorization: `Bearer ${ch.accessToken}` };
    let chanRes = await fetch(chanUrl, { headers: chanHeaders });

    // If no API key and token is expired, refresh then retry
    if (!apiKey && (chanRes.status === 401 || chanRes.status === 403)) {
      const newToken = await refreshAccessToken(ch.accessToken);
      chanRes = await fetch(chanUrl, { headers: { Authorization: `Bearer ${newToken}` } });
    }

    logYoutubeQuota('channels.list', YOUTUBE_QUOTA_COSTS.channels, String(userId)).catch(() => {});
    const chanData = await chanRes.json();
    chanStats = chanData?.items?.[0]?.statistics || {};
  } catch (e) { console.warn('[Analytics] channels.list failed:', e.message); }

  // 2. Top videos — query calendar_slots (0 quota), then videos.list (1 unit) for stats.
  // Replaces the search.list call which cost 100 units per request.
  const slotsCol = agentCol('calendar_slots');
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const videosPosted = await slotsCol.countDocuments({ userId: String(userId), posted: true }).catch(() => 0);

  const recentSlots = await slotsCol
    .find({ userId: String(userId), posted: true, date: { $gte: thirtyDaysAgo }, youtubeVideoId: { $exists: true, $ne: null } })
    .sort({ date: -1 })
    .limit(10)
    .toArray()
    .catch(() => []);
  const videoIds = recentSlots.map(s => s.youtubeVideoId).filter(Boolean);

  let topVideos = [];
  if (videoIds.length) {
    try {
      const vidUrl = apiKey
        ? `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id=${videoIds.join(',')}&key=${apiKey}`
        : `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id=${videoIds.join(',')}`;
      const vidHeaders = apiKey ? {} : { Authorization: `Bearer ${ch.accessToken}` };
      const vidRes = await fetch(vidUrl, { headers: vidHeaders });
      logYoutubeQuota('videos.list', YOUTUBE_QUOTA_COSTS.default, String(userId)).catch(() => {});
      const vidData = await vidRes.json();
      topVideos = (vidData?.items || [])
        .map(v => ({
          id:           v.id,
          title:        v.snippet?.title || '',
          publishedAt:  v.snippet?.publishedAt || '',
          viewCount:    parseInt(v.statistics?.viewCount   || 0),
          likeCount:    parseInt(v.statistics?.likeCount   || 0),
          commentCount: parseInt(v.statistics?.commentCount || 0),
        }))
        .sort((a, b) => b.viewCount - a.viewCount)
        .slice(0, 5);
    } catch (e) { console.warn('[Analytics] videos.list failed:', e.message); }
  }

  // 3. YouTube Analytics API — 30-day daily breakdown (requires yt-analytics.readonly scope).
  // Silently degrades if scope not granted or token has no analytics permission.
  let dailyViews      = [];
  let watchTimeMinutes  = null;
  let subscribersGained = null;
  let analyticsToken = ch.accessToken;
  try {
    const endDate   = new Date().toISOString().slice(0, 10);
    const startDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const analyticsUrl = `https://youtubeanalytics.googleapis.com/v2/reports?ids=channel%3D%3D${ch.channelId}&startDate=${startDate}&endDate=${endDate}&metrics=views,estimatedMinutesWatched,subscribersGained&dimensions=day&sort=day`;

    let analyticsRes = await fetch(analyticsUrl, { headers: { Authorization: `Bearer ${analyticsToken}` } });
    if (analyticsRes.status === 401 || analyticsRes.status === 403) {
      analyticsToken = await refreshAccessToken(analyticsToken);
      analyticsRes   = await fetch(analyticsUrl, { headers: { Authorization: `Bearer ${analyticsToken}` } });
    }

    const analyticsData = await analyticsRes.json();
    if (Array.isArray(analyticsData?.rows)) {
      dailyViews        = analyticsData.rows.map(r => ({ date: r[0], views: parseInt(r[1] || 0) }));
      watchTimeMinutes  = analyticsData.rows.reduce((a, r) => a + parseInt(r[2] || 0), 0);
      subscribersGained = analyticsData.rows.reduce((a, r) => a + parseInt(r[3] || 0), 0);
      logAPIUsage('youtube', 'analytics_report', String(userId), API_COSTS.youtube_analytics, 0, true);
    }
  } catch (_) { logAPIUsage('youtube', 'analytics_report', String(userId), API_COSTS.youtube_analytics, 0, false); }

  const totalViews = parseInt(chanStats.viewCount       || 0);
  const totalSubs  = parseInt(chanStats.subscriberCount || 0);
  const avgViews   = topVideos.length
    ? Math.round(topVideos.reduce((a, v) => a + v.viewCount, 0) / topVideos.length) : null;

  const payload = {
    success:     true,
    channelName: ch.channelName,
    channelId:   ch.channelId,
    stats:       { totalViews, totalSubs, watchTimeMinutes, subscribersGained, avgViews, videosPosted },
    topVideos,
    dailyViews,
  };

  // Write to 30-min cache
  await cacheCol.updateOne(
    { userId: String(userId) },
    { $set: { userId: String(userId), data: payload, cachedAt: new Date().toISOString() } },
    { upsert: true }
  ).catch(() => {});

  console.log(`[Analytics] Live fetch complete for user ${userId} — cached until ${new Date(Date.now() + CACHE_TTL_MS).toISOString()}`);
  return payload;
}

// GET /api/analytics — real YouTube channel analytics for last 30 days (cached 30 min)
app.get('/api/analytics', requireAuth, async (req, res) => {
  try {
    const data = await fetchLiveAnalytics(req.user.id);
    if (!data) return res.json({ success: false, error: 'analytics_unavailable' });
    res.json(data);
  } catch (err) {
    console.error('[Analytics] GET /api/analytics error:', err.message);
    res.json({ success: false, error: err.message });
  }
});

// GET /api/dashboard/stats — condensed stat-card data for the dashboard (same 30-min cache)
app.get('/api/dashboard/stats', requireAuth, async (req, res) => {
  try {
    const data = await fetchLiveAnalytics(req.user.id);
    if (!data) return res.json({ success: false, error: 'stats_unavailable' });
    if (data.noChannel) return res.json({ success: true, noChannel: true });
    res.json({
      success:      true,
      channelName:  data.channelName,
      channelId:    data.channelId,
      stats:        data.stats,
      topVideo:     (data.topVideos || [])[0] || null,
      fromCache:    data.fromCache || false,
      quotaLimited: data.quotaLimited || false,
    });
  } catch (err) {
    console.error('[Dashboard] GET /api/dashboard/stats error:', err.message);
    res.json({ success: false, error: err.message });
  }
});

// GET /api/analytics/videos
// Returns posted video performance data for the logged-in user
app.get('/api/analytics/videos', requireAuth, async (req, res) => {
  try {
    const db = mongoose.connection.db;
    // First try posted_videos collection
    const videos = await db.collection('posted_videos')
      .find({ userId: req.user.id })
      .sort({ postedAt: -1 })
      .limit(50).toArray();

    if (videos.length) {
      videos.forEach(v => { v._id = v._id.toString(); });
      return res.json({ success: true, videos });
    }

    // Fallback — check pipeline_queue for posted items
    const posted = await db.collection('pipeline_queue')
      .find({ userId: req.user.id, status: 'posted' })
      .sort({ postedAt: -1 })
      .limit(50).toArray();

    posted.forEach(v => { v._id = v._id.toString(); });
    res.json({ success: true, videos: posted });
  } catch (err) {
    console.error('[Analytics] GET /videos error:', err);
    res.status(500).json({ error: 'Failed to fetch video analytics' });
  }
});

// ── CHANNEL HEALTH SCORE ──────────────────────────────────────────────────────

// GET /api/channel/:id/health — weighted health score (cached 6h, per-metric fallbacks)
app.get('/api/channel/:id/health', requireAuth, async (req, res) => {
  try {
    const channelId = req.params.id;
    const userId    = String(req.user.id);

    const user = await User.findById(userId).lean();
    const ch   = (user?.youtubeChannels || []).find(c => c.channelId === channelId);
    if (!ch) return res.status(404).json({ error: 'Channel not found' });

    const healthCol = agentCol('channel_health');
    const CACHE_TTL = 6 * 60 * 60 * 1000; // 6 hours
    const cached    = await healthCol.findOne({ channelId, userId }).catch(() => null);
    if (cached?.score !== undefined && (Date.now() - new Date(cached.cachedAt).getTime()) < CACHE_TTL) {
      return res.json({ success: true, ...cached, _id: undefined, fromCache: true });
    }

    // ── Posting Consistency (MongoDB-only, always available) ──────────────────
    const slotsCol      = agentCol('calendar_slots');
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const [postedCount, scheduledCount] = await Promise.all([
      slotsCol.countDocuments({ userId, channelId, posted: true, date: { $gte: thirtyDaysAgo } }).catch(() => 0),
      slotsCol.countDocuments({ userId, channelId, date: { $gte: thirtyDaysAgo } }).catch(() => 0),
    ]);
    const consistencyScore = scheduledCount > 0
      ? Math.min(100, Math.round((postedCount / scheduledCount) * 100))
      : 50;
    const consistencyValue = `${postedCount}/${Math.max(1, scheduledCount)} on schedule`;

    // ── Attempt live analytics (graceful fallback if unavailable) ─────────────
    let analytics = null;
    let analyticsAvailable = false;
    try {
      const raw = await fetchLiveAnalytics(userId);
      if (raw && !raw.noChannel && !raw.quotaLimited) {
        analytics = raw;
        analyticsAvailable = true;
      }
    } catch (e) {
      console.warn('[CHANNEL HEALTH] analytics fetch failed:', e.message);
    }

    // ── Growth Rate (30%) ─────────────────────────────────────────────────────
    let growthScore = null, growthValue = 'Insufficient data', growthSource = 'insufficient';
    if (analyticsAvailable) {
      const subscribersGained = analytics.stats.subscribersGained || 0;
      growthScore  = Math.min(100, Math.round(subscribersGained / 3));
      growthValue  = `+${subscribersGained} subs/mo`;
      growthSource = 'api';
    } else {
      // Subscriber history fallback: compare current subscriber count vs stored snapshot
      const prevSnap   = cached?.subscriberCountSnapshot;
      const currentSubs = ch.subscriberCount || 0;
      if (prevSnap?.count !== undefined && prevSnap?.at && currentSubs > 0) {
        const daysDiff = (Date.now() - new Date(prevSnap.at).getTime()) / (1000 * 60 * 60 * 24);
        if (daysDiff >= 7) {
          const gained30d = Math.round((currentSubs - prevSnap.count) * (30 / daysDiff));
          growthScore  = Math.min(100, Math.max(0, Math.round(gained30d / 3)));
          growthValue  = `+${Math.max(0, gained30d)} subs (est.)`;
          growthSource = 'mongodb';
        }
      }
    }

    // ── Engagement / CTR (30%) ────────────────────────────────────────────────
    let ctrScore = null, ctrValue = 'Insufficient data', ctrSource = 'insufficient';
    if (analyticsAvailable) {
      const topVideos = analytics.topVideos || [];
      let avgEngagementRate = 0;
      if (topVideos.length) {
        const rates = topVideos.map(v => v.viewCount ? (v.likeCount + v.commentCount) / v.viewCount : 0);
        avgEngagementRate = rates.reduce((a, b) => a + b, 0) / rates.length;
      }
      ctrScore  = Math.min(100, Math.round(avgEngagementRate * 2000));
      ctrValue  = `${(avgEngagementRate * 100).toFixed(1)}% eng.`;
      ctrSource = 'api';
    }

    // ── Retention via avg watch-time (20%) ────────────────────────────────────
    let retentionScore = null, retentionValue = 'Insufficient data', retentionSource = 'insufficient';
    let avgWatchSeconds = 0;
    if (analyticsAvailable) {
      const watchTimeMinutes = analytics.stats.watchTimeMinutes || 0;
      const periodViews      = (analytics.dailyViews || []).reduce((a, d) => a + d.views, 0) || 1;
      avgWatchSeconds  = (watchTimeMinutes * 60) / periodViews;
      retentionScore   = Math.min(100, Math.round((avgWatchSeconds / 30) * 100));
      retentionValue   = `${Math.round(avgWatchSeconds)}s avg`;
      retentionSource  = 'api';
    }

    // ── Overall score (proportional from available metrics) ───────────────────
    const allMetricsRaw = [
      { key: 'growth',      score: growthScore,     weight: 30 },
      { key: 'ctr',         score: ctrScore,         weight: 30 },
      { key: 'retention',   score: retentionScore,   weight: 20 },
      { key: 'consistency', score: consistencyScore, weight: 20 },
    ];
    const scoreable    = allMetricsRaw.filter(m => m.score !== null);
    const totalWeight  = scoreable.reduce((s, m) => s + m.weight, 0);
    const score        = totalWeight > 0
      ? Math.round(scoreable.reduce((s, m) => s + m.score * m.weight, 0) / totalWeight)
      : consistencyScore;

    // ── Week-over-week trend ──────────────────────────────────────────────────
    let trend = 'stable';
    if (analyticsAvailable) {
      const dv            = analytics.dailyViews || [];
      const prevWeekViews = dv.slice(0, 7).reduce((a, d) => a + d.views, 0);
      const thisWeekViews = dv.slice(-7).reduce((a, d) => a + d.views, 0);
      trend = thisWeekViews > prevWeekViews * 1.05 ? 'up'
            : thisWeekViews < prevWeekViews * 0.95 ? 'down' : 'stable';
    }

    // ── AI tip (built from whichever metrics are available) ───────────────────
    const tipCandidates = [
      { name: 'Growth Rate',        score: growthScore,     label: 'subscriber growth' },
      { name: 'Engagement (CTR)',   score: ctrScore,         label: 'click-through and engagement' },
      { name: 'Retention',          score: retentionScore,   label: 'viewer retention and watch time' },
      { name: 'Posting Consistency',score: consistencyScore, label: 'posting schedule consistency' },
    ].filter(m => m.score !== null);
    const weakest = tipCandidates.length
      ? tipCandidates.reduce((a, b) => a.score < b.score ? a : b)
      : { name: 'Posting Consistency', score: consistencyScore, label: 'posting schedule consistency' };

    let aiTip = null;
    if (process.env.OPENAI_API_KEY) {
      try {
        const { OpenAI } = require('openai');
        const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
        const statsDesc = analyticsAvailable
          ? `+${analytics.stats.subscribersGained || 0} subs/30d, ${avgWatchSeconds ? Math.round(avgWatchSeconds) + 's avg watch' : 'watch time unknown'}, ${postedCount}/${Math.max(1, scheduledCount)} videos on schedule`
          : `${postedCount}/${Math.max(1, scheduledCount)} videos on schedule (analytics unavailable)`;
        const prompt = `You are a YouTube growth advisor. Channel niche: "${ch.nicheName || 'general'}". Health score: ${score}/100. Weakest area: "${weakest.name}" (score ${weakest.score}/100). Stats: ${statsDesc}. Give ONE specific, actionable tip (2 sentences max) to improve their ${weakest.label}. No generic advice.`;
        const resp = await openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [{ role: 'user', content: prompt }],
          max_tokens: 120,
          temperature: 0.7,
        });
        aiTip = resp.choices[0]?.message?.content?.trim() || null;
        logAPIUsage('openai', 'channel_health_tip', userId,
          resp.usage?.total_tokens || 0,
          ((resp.usage?.prompt_tokens || 0) * API_COSTS.openai_input) +
          ((resp.usage?.completion_tokens || 0) * API_COSTS.openai_output), true);
      } catch (e) {
        console.warn('[Health] AI tip failed:', e.message);
      }
    }

    const result = {
      channelId,
      channelName: ch.channelName,
      score,
      trend,
      analyticsAvailable,
      metrics: {
        growth:      { score: growthScore,     weight: 30, label: 'Growth Rate',         value: growthValue,      source: growthSource,     explanation: 'New subscribers gained in the last 30 days' },
        ctr:         { score: ctrScore,         weight: 30, label: 'Engagement (CTR)',    value: ctrValue,         source: ctrSource,        explanation: 'Average likes + comments relative to views' },
        retention:   { score: retentionScore,   weight: 20, label: 'Retention',           value: retentionValue,   source: retentionSource,  explanation: 'Average viewer watch time per video' },
        consistency: { score: consistencyScore, weight: 20, label: 'Posting Consistency', value: consistencyValue, source: 'mongodb',        explanation: 'Videos posted as scheduled in last 30 days' },
      },
      weakestMetric: weakest.name,
      aiTip,
      subscriberCountSnapshot: { count: ch.subscriberCount || 0, at: new Date().toISOString() },
      cachedAt: new Date().toISOString(),
    };

    console.log(`[CHANNEL HEALTH] channelId=${channelId} score=${score} consistency=${consistencyScore} growth=${growthScore ?? 'n/a'} ctr=${ctrScore ?? 'n/a'} retention=${retentionScore ?? 'n/a'} analyticsAvailable=${analyticsAvailable}`);

    await healthCol.updateOne(
      { channelId, userId },
      { $set: result },
      { upsert: true }
    ).catch(() => {});

    await User.updateOne(
      { _id: userId, 'youtubeChannels.channelId': channelId },
      { $set: {
        'youtubeChannels.$.channelHealthScore': score,
        'youtubeChannels.$.channelHealthBreakdown': result.metrics,
        'youtubeChannels.$.healthCalculatedAt': result.cachedAt,
      }}
    ).catch(() => {});

    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[Health] GET /api/channel/:id/health error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── HOOKS LIBRARY API ─────────────────────────────────────────────────────────

// GET /api/hooks/library — stats for the Hooks Library UI section
app.get('/api/hooks/library', requireAuth, async (req, res) => {
  try {
    const col  = agentCol('hooksLibrary');
    const meta = await agentCol('hooksLibraryMeta').findOne({ _id: 'meta' }).catch(() => null);

    const totalActive = await col.countDocuments({ isActive: true });

    const [topHooks, bottomHooks] = await Promise.all([
      col.find({ isActive: true, timesUsed: { $gte: 1 } })
        .sort({ avgEngagementScore: -1 })
        .limit(5)
        .project({ template: 1, category: 1, avgEngagementScore: 1, timesUsed: 1 })
        .toArray(),
      col.find({ isActive: true, timesUsed: { $gte: 1 } })
        .sort({ avgEngagementScore: 1 })
        .limit(5)
        .project({ template: 1, category: 1, avgEngagementScore: 1, timesUsed: 1 })
        .toArray(),
    ]);

    // Category breakdown
    const allActive = await col.find({ isActive: true })
      .project({ category: 1, avgEngagementScore: 1 })
      .toArray();
    const catMap = {};
    for (const h of allActive) {
      if (!catMap[h.category]) catMap[h.category] = { count: 0, totalScore: 0 };
      catMap[h.category].count++;
      catMap[h.category].totalScore += h.avgEngagementScore;
    }
    const categoryBreakdown = Object.entries(catMap).map(([cat, d]) => ({
      category: cat,
      count: d.count,
      avgScore: Math.round(d.totalScore / d.count),
    })).sort((a, b) => b.avgScore - a.avgScore);

    // Next rebuild date — 1st of next month
    const now = new Date();
    const nextRebuild = new Date(now.getFullYear(), now.getMonth() + 1, 1).toISOString().slice(0, 10);

    const stringify = arr => arr.map(h => ({ ...h, _id: String(h._id) }));

    res.json({
      success: true,
      totalActive,
      topHooks:   stringify(topHooks),
      bottomHooks: stringify(bottomHooks),
      categoryBreakdown,
      lastRebuiltAt:  meta?.lastRebuiltAt || null,
      nextRebuildDate: nextRebuild,
    });
  } catch (err) {
    console.error('[Hooks] GET /api/hooks/library error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── AFFILIATE ─────────────────────────────────────────────────────────────────

// GET /api/affiliate/stats — legacy stub kept for backward compat
app.get('/api/affiliate/stats', requireAuth, async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const stats = await db.collection('affiliate_stats').findOne({ userId: req.user.id });
    res.json({ success: true, pendingPayout: stats?.pendingPayout || 0, totalSignups: stats?.totalSignups || 0, conversions: stats?.conversions || 0, totalEarned: stats?.totalEarned || 0, commissionRate: 0.30 });
  } catch (err) {
    console.error('[Affiliate] GET /stats error:', err);
    res.status(500).json({ error: 'Failed to fetch affiliate stats' });
  }
});

// GET /api/affiliate — real referral data sourced directly from User collection
app.get('/api/affiliate', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    // Ensure affiliate code exists (backfill for legacy accounts)
    if (!user.affiliateCode) {
      user.affiliateCode = generateAffiliateCode();
      await user.save();
    }

    const referralLink = `https://viralityy.pages.dev?ref=${user.affiliateCode}`;

    // All users who signed up via this affiliate's code
    const referred = await User.find({ referredBy: user.affiliateCode })
      .select('name email plan createdAt')
      .sort({ createdAt: -1 })
      .lean();

    const EARNINGS_PER_UPGRADE = 10;
    const upgrades    = referred.filter(u => u.plan && u.plan !== 'trial');
    const earnings    = upgrades.length * EARNINGS_PER_UPGRADE;
    const convRate    = referred.length ? Math.round((upgrades.length / referred.length) * 100) : 0;

    // Referral history — mask names for privacy
    const history = referred.map(u => ({
      id:          u._id.toString(),
      initials:    (u.name || 'U').slice(0, 1).toUpperCase() + (u.name?.split(' ')[1]?.[0] || '').toUpperCase(),
      signedUpAt:  u.createdAt,
      plan:        u.plan || 'trial',
      upgraded:    u.plan !== 'trial',
    }));

    // Top 5 leaderboard — affiliate codes with the most referrals
    const leaderboard = await User.aggregate([
      { $match: { referredBy: { $exists: true, $ne: null, $ne: '' } } },
      { $group: { _id: '$referredBy', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 5 },
    ]);
    const myRank   = leaderboard.findIndex(l => l._id === user.affiliateCode) + 1;
    const topBoard = leaderboard.map((l, i) => ({
      rank:       i + 1,
      isYou:      l._id === user.affiliateCode,
      count:      l.count,
      label:      l._id === user.affiliateCode ? 'You' : `Affiliate #${i + 1}`,
    }));

    res.json({
      success: true,
      affiliateCode: user.affiliateCode,
      referralLink,
      stats: {
        total:       referred.length,
        upgrades:    upgrades.length,
        earnings,
        convRate,
        myRank: myRank || null,
      },
      history,
      leaderboard: topBoard,
    });
  } catch (err) {
    console.error('[Affiliate] GET /api/affiliate error:', err.message);
    res.status(500).json({ error: 'Failed to load affiliate data' });
  }
});

// GET /api/affiliate/dashboard — full referral dashboard data (20% commission model)
app.get('/api/affiliate/dashboard', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    // Ensure affiliate code exists
    if (!user.affiliateCode) {
      user.affiliateCode = generateAffiliateCode();
      await user.save();
    }

    const appBase = process.env.FRONTEND_URL || 'https://viralityy.pages.dev';
    const referralLink = `${appBase}?ref=${user.affiliateCode}`;

    // Commission: 20% of first payment per plan
    const COMMISSION = {
      starter:    Math.round(PLAN_CONFIG.starter?.price  * 0.20 * 100) / 100 || 9.80,
      growth:     Math.round(PLAN_CONFIG.growth?.price   * 0.20 * 100) / 100 || 19.80,
      agency:     Math.round(PLAN_CONFIG.agency?.price   * 0.20 * 100) / 100 || 49.80,
      shorts_pro: Math.round(PLAN_CONFIG.shorts_pro?.price * 0.20 * 100) / 100 || 13.80,
    };

    // All users who came via this code
    const referred = await User.find({ referredBy: user.affiliateCode })
      .select('name email plan subscriptionStatus createdAt')
      .sort({ createdAt: -1 })
      .lean();

    const successful = referred.filter(u => u.plan && u.plan !== 'trial' && u.subscriptionStatus === 'active');
    const pending    = referred.filter(u => u.plan === 'trial' || u.subscriptionStatus !== 'active');
    const totalEarnings = successful.reduce((sum, u) => sum + (COMMISSION[u.plan] || 0), 0);
    const convRate = referred.length ? Math.round((successful.length / referred.length) * 100) : 0;

    // Masked email: j***@gmail.com
    function maskEmail(email) {
      if (!email || !email.includes('@')) return '***@***';
      const [local, domain] = email.split('@');
      return local.slice(0, 1) + '***@' + domain;
    }

    const history = referred.map(u => {
      const isPaid = u.plan && u.plan !== 'trial' && u.subscriptionStatus === 'active';
      const isExpired = u.subscriptionStatus === 'expired';
      return {
        signedUpAt:  u.createdAt,
        maskedEmail: maskEmail(u.email),
        initials:    (u.name || u.email || 'U').slice(0, 2).toUpperCase(),
        status:      isPaid ? 'converted' : isExpired ? 'expired' : 'pending',
        plan:        u.plan || 'trial',
        commission:  isPaid ? (COMMISSION[u.plan] || 0) : null,
      };
    });

    // Leaderboard
    const leaderboard = await User.aggregate([
      { $match: { referredBy: { $exists: true, $ne: null, $ne: '' } } },
      { $group: { _id: '$referredBy', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 5 },
    ]);
    const myRank   = leaderboard.findIndex(l => l._id === user.affiliateCode) + 1;
    const topBoard = leaderboard.map((l, i) => ({
      rank:   i + 1,
      isYou:  l._id === user.affiliateCode,
      count:  l.count,
      label:  l._id === user.affiliateCode ? 'You' : `Affiliate #${i + 1}`,
    }));

    res.json({
      success: true,
      affiliateCode: user.affiliateCode,
      referralLink,
      stats: {
        totalReferrals:       referred.length,
        successfulConversions: successful.length,
        pendingConversions:    pending.length,
        totalEarnings:        Math.round(totalEarnings * 100) / 100,
        conversionRate:       convRate,
        leaderboardRank:      myRank || null,
      },
      commissionStructure: COMMISSION,
      history,
      leaderboard: topBoard,
    });
  } catch (err) {
    console.error('[Affiliate] GET /api/affiliate/dashboard error:', err.message);
    res.status(500).json({ error: 'Failed to load referral dashboard' });
  }
});

// ── AUTH EXTRAS ───────────────────────────────────────────────────────────────

// POST /api/auth/forgot-password
app.post('/api/auth/forgot-password', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });
    // In production: send a real reset email via SendGrid/Resend
    // For now: acknowledge the request so the UI can show a success state
    console.log('[Auth] Password reset requested for:', email);
    res.json({ success: true, message: 'If that email exists, a reset link has been sent.' });
  } catch (err) {
    console.error('[Auth] forgot-password error:', err);
    res.status(500).json({ error: 'Failed to process request' });
  }
});

// PATCH /api/user/timezone — save the user's detected IANA timezone
app.patch('/api/user/timezone', requireAuth, async (req, res) => {
  try {
    const { timezone } = req.body;
    if (!timezone || typeof timezone !== 'string') return res.status(400).json({ error: 'timezone must be a non-empty string' });
    await User.updateOne({ _id: req.user.id }, { $set: { timezone } });
    res.json({ success: true, timezone });
  } catch (err) {
    console.error('[User] PATCH /timezone error:', err);
    res.status(500).json({ error: 'Failed to update timezone' });
  }
});

// PATCH /api/user/onboarding-complete — mark onboarding walkthrough as finished
app.patch('/api/user/onboarding-complete', requireAuth, async (req, res) => {
  try {
    await User.updateOne({ _id: req.user.id }, { $set: { onboardingComplete: true } });
    res.json({ success: true });
  } catch (err) {
    console.error('[User] PATCH /onboarding-complete error:', err);
    res.status(500).json({ error: 'Failed to mark onboarding complete' });
  }
});

// PATCH /api/user/autopost — enable or disable auto-posting for the logged-in user
app.patch('/api/user/autopost', requireAuth, async (req, res) => {
  try {
    const { autoPost } = req.body;
    if (typeof autoPost !== 'boolean') return res.status(400).json({ error: 'autoPost must be a boolean' });
    await User.updateOne({ _id: req.user.id }, { $set: { autoPost } });
    res.json({ success: true, autoPost });
  } catch (err) {
    console.error('[User] PATCH /autopost error:', err);
    res.status(500).json({ error: 'Failed to update auto-post setting' });
  }
});

// ── CHANNELS EXTRAS ───────────────────────────────────────────────────────────

// POST /api/channels/settings — update channel auto-post settings
app.post('/api/channels/settings', requireAuth, async (req, res) => {
  try {
    const { autoPost, channelId } = req.body;
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    // Update global setting or per-channel
    if (channelId) {
      const ch = (user.youtubeChannels || []).find(c => c.channelId === channelId);
      if (ch) ch.autoPost = autoPost;
    } else {
      user.autoPost = autoPost;
    }
    await user.save();
    res.json({ success: true, autoPost });
  } catch (err) {
    console.error('[Channels] POST /settings error:', err);
    res.status(500).json({ error: 'Failed to update settings' });
  }
});

// POST /api/channels/:channelId/niche — set niche on a specific channel
app.post('/api/channels/:channelId/niche', requireAuth, async (req, res) => {
  try {
    const { nicheId, nicheName } = req.body;
    if (!nicheId) return res.status(400).json({ error: 'nicheId required' });
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const isFirstSet = !user.nicheId;
    if (!isFirstSet && planNicheQuota(user.plan) === 0) {
      return res.status(403).json({ error: 'Niche changes are not available on the trial plan. Upgrade to a paid plan.', code: 'NICHE_LOCKED' });
    }
    const ch = (user.youtubeChannels || []).find(c => c.channelId === req.params.channelId);
    if (!ch) return res.status(404).json({ error: 'Channel not found' });
    ch.nicheId   = nicheId;
    ch.nicheName = nicheName || nicheId;
    user.nicheId   = nicheId;
    user.nicheName = nicheName || nicheId;
    user.markModified('youtubeChannels');
    console.log(`[Channels] saving niche on channel ${req.params.channelId}: nicheId=${nicheId} nicheName=${ch.nicheName} userId=${user._id}`);
    await user.save();
    console.log(`[Channels] niche saved — channel.nicheName=${ch.nicheName} user.nicheName=${user.nicheName}`);
    res.json({ success: true, channelId: req.params.channelId, nicheId, nicheName: ch.nicheName });
  } catch (err) {
    console.error('[Channels] POST /niche error:', err);
    res.status(500).json({ error: 'Failed to set niche on channel' });
  }
});

// POST /api/channels/:channelId/pause — pause/resume a channel
app.post('/api/channels/:channelId/pause', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const ch = (user.youtubeChannels || []).find(c => c.channelId === req.params.channelId);
    if (!ch) return res.status(404).json({ error: 'Channel not found' });
    ch.paused = !ch.paused;
    await user.save();
    res.json({ success: true, paused: ch.paused, channelId: req.params.channelId });
  } catch (err) {
    console.error('[Channels] POST /pause error:', err);
    res.status(500).json({ error: 'Failed to pause channel' });
  }
});

// =============================================================================
// QUALITY SCORES
// =============================================================================

// GET /api/quality/scores — returns AI-generated quality scores for this user's last 30 videos
app.get('/api/quality/scores', requireAuth, async (req, res) => {
  try {
    const today  = new Date().toISOString().slice(0, 10);
    const col    = agentCol('quality_scores');

    // Return today's scores for the per-slot breakdown; also compute a rolling 30-video average
    const [todayScores, recentScores] = await Promise.all([
      col.find({ userId: req.user.id, $or: [{ scheduledDate: today }, { date: today }] })
         .sort({ scoredAt: -1 }).toArray(),
      col.find({ userId: req.user.id })
         .sort({ scoredAt: -1 }).limit(30).toArray(),
    ]);
    todayScores.forEach(s => { s._id = s._id.toString(); });

    const total  = todayScores.length;
    const passed = todayScores.filter(s => (s.scores?.overall || 0) >= 60).length;
    const failed = total - passed;

    // Rolling average uses up to the last 30 scored videos — not just today
    const rollingTotal = recentScores.length;
    const avg = rollingTotal
      ? Math.round(recentScores.reduce((a, s) => a + (s.scores?.overall || 0), 0) / rollingTotal)
      : null;

    res.json({ success: true, scores: todayScores, stats: { total, avg, passed, failed, rollingTotal }, today });
  } catch (err) {
    console.error('[QualityScores] GET error:', err.message);
    res.status(500).json({ error: 'Failed to load quality scores' });
  }
});

// =============================================================================
// CRON JOBS
// =============================================================================

// =============================================================================
// AUTO-POSTING PIPELINE
// =============================================================================

// Fetches competitor channels and past posted videos to enrich the script generation prompt.
// Non-fatal — returns empty arrays on any DB failure so the pipeline never blocks.
async function fetchScriptContext(userId, channelId) {
  try {
    const [competitors, pastSlots] = await Promise.all([
      agentCol('competitor_channels')
        .find({ userId: String(userId), active: true })
        .sort({ addedAt: -1 })
        .limit(5)
        .project({ channelName: 1, avgViews: 1, uploadFrequencyDays: 1, subscriberCount: 1 })
        .toArray()
        .catch(() => []),
      agentCol('calendar_slots')
        .find({ userId: String(userId), status: 'posted', ...(channelId ? { channelId } : {}) })
        .sort({ postedAt: -1 })
        .limit(10)
        .project({ title: 1, hookTemplate: 1, postedAt: 1 })
        .toArray()
        .catch(() => []),
    ]);
    return { competitors: competitors || [], pastSlots: pastSlots || [] };
  } catch {
    return { competitors: [], pastSlots: [] };
  }
}

// Step 1 — Generate video script via OpenAI
// Step 1 — Generate viral-optimised script via OpenAI
// Returns { title, script, hook, loopEnding, captions, hashtags, wordCount, scriptContext }
// Shorts: structured JSON with hook rules, 130-150 word target, caption segments, hashtags
// Long-form: plain text wrapped in the same shape for a consistent call site
async function pipelineGenerateScript(title, nicheName, type, hookTemplate, userId = null, channelId = null) {
  const { OpenAI } = require('openai');
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  // Fetch competitor + past-video context (non-fatal)
  const { competitors, pastSlots } = userId
    ? await fetchScriptContext(userId, channelId)
    : { competitors: [], pastSlots: [] };

  const hookInstruction = hookTemplate
    ? `\nOPENING HOOK (mandatory first line): Your script MUST begin with this exact hook verbatim (replace [TOPIC] with the video topic): "${hookTemplate}". Do NOT modify, paraphrase, or skip this hook under any circumstances.`
    : '';

  // Build competitor context block
  const competitorLines = competitors.map(c => {
    const avgK = c.avgViews ? `~${Math.round(c.avgViews / 1000)}K avg views` : null;
    const freq  = c.uploadFrequencyDays ? `posts every ${c.uploadFrequencyDays}d` : null;
    const meta  = [avgK, freq].filter(Boolean).join(', ');
    return `• ${c.channelName}${meta ? ` — ${meta}` : ''}`;
  });
  const competitorBlock = competitorLines.length > 0
    ? `TOP PERFORMING COMPETITORS IN THIS NICHE:\n${competitorLines.join('\n')}\nStudy their style and topics — do NOT copy directly, but innovate on what works.\n\n`
    : '';

  // Build past-videos block
  const pastVideoLines = pastSlots.map(s =>
    `• "${s.title}"${s.hookTemplate ? ` (hook style: "${s.hookTemplate.slice(0, 60)}")` : ''}`
  );
  const pastVideosBlock = pastVideoLines.length > 0
    ? `PREVIOUSLY POSTED VIDEOS ON THIS CHANNEL (avoid repeating these angles or hooks):\n${pastVideoLines.join('\n')}\n\n`
    : '';

  const scriptContext = {
    competitorsUsed: competitors.map(c => c.channelName),
    pastVideosUsed:  pastSlots.length,
    hookUsed:        hookTemplate || null,
    promptLength:    0, // filled in after prompt is built
  };

  if (type === 'Long-form') {
    const longPrompt = `Write a 3–5 minute YouTube script for: "${title}".\nNiche: ${nicheName}. Natural spoken language only — no stage directions, no emojis, no markdown.\nStart with a strong hook. End with a clear call-to-action.${hookInstruction}\n\n${competitorBlock}${pastVideosBlock}`;
    scriptContext.promptLength = longPrompt.length;
    console.log(`[AI SCRIPT] channel=${channelId||'?'} hook=${hookTemplate?'yes':'none'} competitors=${competitors.length} pastVideos=${pastSlots.length} promptChars=${longPrompt.length}`);
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: longPrompt }],
      temperature: 0.8,
    });
    const inTok  = completion.usage?.prompt_tokens    || 0;
    const outTok = completion.usage?.completion_tokens || 0;
    logAPIUsage('openai', 'script_longform', null, inTok + outTok,
      inTok * API_COSTS.openai_input + outTok * API_COSTS.openai_output, true);
    const script = completion.choices[0].message.content.trim();
    return { title, script, hook: '', loopEnding: '', captions: [], hashtags: [], wordCount: script.split(/\s+/).length, scriptContext };
  }

  // ── Shorts: viral-optimised structured JSON ──
  const prompt = `You are a viral YouTube Shorts script writer for a "${nicheName}" channel.
Write a viral-optimised Shorts script for the video: "${title}"
${hookInstruction}

${competitorBlock}${pastVideosBlock}STRICT RULES:
1. HOOK (first 3 seconds): ${hookTemplate ? `Use the mandatory hook above verbatim (replace [TOPIC] with the video topic). Do NOT modify it.` : `Must use ONE of: a shocking fact ("Did you know…"), a bold claim ("Most people get this wrong…"), a direct question ("What if you could…"), or a number ("3 things that…"). Hook must be under 15 words.`}
2. LENGTH: Script must be EXACTLY 130-150 words total. This is a hard requirement — do NOT write fewer than 130 words under any circumstances. Count every word. 130-150 words at natural speaking pace produces a 55-65 second video.
3. STRUCTURE: Hook (5 sec) → 5 value-packed points with examples (40 sec) → powerful call-to-action + Loop ending (10 sec). The LAST sentence MUST echo or reference the hook to create a rewatch loop.
4. STYLE: Short punchy sentences only. Zero filler words (no "basically", "actually", "you know"). Every sentence adds new information or a concrete example.
5. TITLE: Generate a viral title using power words. Format: "[Number] [Topic] That [Surprising Outcome]" OR "Why [Common Belief] Is Wrong" OR "The [Topic] Secret Nobody Tells You".
6. HASHTAGS: Generate exactly 5 hashtags relevant to the ${nicheName} niche.
7. CAPTIONS: Split the full script into caption segments — each segment MAX 4 words, estimated timestamps at ~2.5 words/sec pace.

Return valid JSON only — no prose, no markdown:
{
  "title": "viral title here",
  "script": "full spoken script, 130-150 words, no stage directions",
  "wordCount": 140,
  "hook": "the opening hook sentence only",
  "loopEnding": "the last sentence that echoes the hook",
  "hashtags": ["#tag1", "#tag2", "#tag3", "#tag4", "#tag5"],
  "captions": [
    {"text": "Did you know", "start": 0, "end": 1.6},
    {"text": "most people fail", "start": 1.6, "end": 3.0}
  ]
}`;

  scriptContext.promptLength = prompt.length;
  console.log(`[AI SCRIPT] channel=${channelId||'?'} hook=${hookTemplate?'yes':'none'} competitors=${competitors.length} pastVideos=${pastSlots.length} promptChars=${prompt.length}`);

  let best = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await openai.chat.completions.create({
        model:           'gpt-4o-mini',
        messages:        [{ role: 'user', content: prompt }],
        temperature:     0.85,
        response_format: { type: 'json_object' },
      });
      const parsed    = JSON.parse(res.choices[0].message.content);
      const wordCount = (parsed.script || '').split(/\s+/).filter(Boolean).length;
      const inTok2    = res.usage?.prompt_tokens    || 0;
      const outTok2   = res.usage?.completion_tokens || 0;
      const ok = wordCount >= 120 && wordCount <= 180;
      logAPIUsage('openai', 'script_short', null, inTok2 + outTok2,
        inTok2 * API_COSTS.openai_input + outTok2 * API_COSTS.openai_output, ok);
      console.log(`[Script] Attempt ${attempt}/3 — ${wordCount} words (target: 130-150)`);
      if (ok) { best = { ...parsed, wordCount }; break; }
      if (wordCount < 120) console.warn(`[Script] ${wordCount} words is too short (< 120) — regenerating`);
      else console.warn(`[Script] ${wordCount} words exceeds 180 — regenerating`);
    } catch (err) {
      logAPIUsage('openai', 'script_short', null, 0, 0, false);
      console.warn(`[Script] Attempt ${attempt}/3 failed: ${err.message}`);
    }
  }
  // Accept best effort if all 3 attempts are out of range
  if (!best) throw new Error('Script generation failed to produce a 120-180 word script after 3 attempts');

  return {
    title:      best.title      || title,
    script:     best.script     || '',
    hook:       best.hook       || '',
    loopEnding: best.loopEnding || '',
    captions:   Array.isArray(best.captions)  ? best.captions.slice(0, 200)  : [],
    hashtags:   Array.isArray(best.hashtags)  ? best.hashtags.slice(0, 5)    : [],
    wordCount:  best.wordCount  || 0,
    scriptContext,
  };
}

// Structured script generator — returns JSON with hook / mainPoints / cta / estimatedDuration / fullScript
// context: { competitors: [], pastSlots: [] } — optional enrichment data from fetchScriptContext
async function generateStructuredScript(title, nicheName, type, openai, context = {}) {
  const isLong = type === 'Long-form';
  const { competitors = [], pastSlots = [] } = context;

  const competitorLines = competitors.map(c => {
    const avgK = c.avgViews ? `~${Math.round(c.avgViews / 1000)}K avg views` : null;
    const freq  = c.uploadFrequencyDays ? `posts every ${c.uploadFrequencyDays}d` : null;
    const meta  = [avgK, freq].filter(Boolean).join(', ');
    return `• ${c.channelName}${meta ? ` — ${meta}` : ''}`;
  });
  const competitorBlock = competitorLines.length > 0
    ? `\nTOP PERFORMING COMPETITORS IN THIS NICHE:\n${competitorLines.join('\n')}\nStudy their approach but innovate — do not copy.\n`
    : '';

  const pastVideoLines = pastSlots.map(s => `• "${s.title}"`);
  const pastVideosBlock = pastVideoLines.length > 0
    ? `\nPREVIOUSLY POSTED VIDEOS (avoid repeating these angles):\n${pastVideoLines.join('\n')}\n`
    : '';

  const promptContent = `You are a YouTube ${isLong ? 'long-form' : 'Shorts'} script strategist for a "${nicheName}" channel.
Write a structured script plan for the video titled: "${title}"
${competitorBlock}${pastVideosBlock}
Return valid JSON with exactly these fields:
{
  "hook": "Opening 1-2 sentences for the first 3 seconds — must immediately grab attention",
  "mainPoints": ["key point 1", "key point 2", "key point 3", "key point 4", "key point 5"],
  "cta": "Call-to-action at the end (1-2 sentences)",
  "estimatedDuration": "${isLong ? '4:30' : '0:52'}",
  "fullScript": "Complete natural spoken script (${isLong ? '3-5 minutes' : '45-60 seconds'}), no stage directions"
}`;
  console.log(`[AI SCRIPT] channel=briefs hook=none competitors=${competitors.length} pastVideos=${pastSlots.length} promptChars=${promptContent.length}`);
  const res = await openai.chat.completions.create({
    model: 'gpt-4o-mini',
    messages: [{ role: 'user', content: promptContent }],
    temperature: 0.78,
    response_format: { type: 'json_object' },
  });
  const parsed = JSON.parse(res.choices[0].message.content);
  return {
    ...parsed,
    scriptContext: {
      competitorsUsed: competitors.map(c => c.channelName),
      pastVideosUsed:  pastSlots.length,
      hookUsed:        null,
      promptLength:    promptContent.length,
    },
  };
}

// Quality scorer — uses OpenAI JSON mode to score a script on 4 dimensions (0-100 each)
async function scoreScript(title, script, nicheName, openai) {
  try {
    const res = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{
        role: 'user',
        content: `You are a YouTube content quality analyst. Score this ${nicheName} script on 4 dimensions (0-100 each).

Video title: "${title}"
Script: ${script.slice(0, 2000)}

Return valid JSON:
{
  "hookStrength": <0-100, how compelling and attention-grabbing are the first 10 seconds>,
  "clarity": <0-100, how clear and easy to follow is the content>,
  "ctaEffectiveness": <0-100, how strong and clear is the call-to-action>,
  "nicheRelevance": <0-100, how well does it match the ${nicheName} niche audience>,
  "overall": <0-100, weighted average of the above>,
  "feedback": "<one sentence of the most important improvement to make>"
}`,
      }],
      temperature: 0.3,
      response_format: { type: 'json_object' },
    });
    const scored = JSON.parse(res.choices[0].message.content);
    const overall = Math.round((scored.hookStrength + scored.clarity + scored.ctaEffectiveness + scored.nicheRelevance) / 4);
    return { ...scored, overall: scored.overall || overall };
  } catch (err) {
    console.warn('[QualityScore] Scoring failed:', err.message);
    return { hookStrength: 0, clarity: 0, ctaEffectiveness: 0, nicheRelevance: 0, overall: 0, feedback: '' };
  }
}

// Step 2 — Generate voiceover via Gemini 2.5 Flash TTS
// Returns path to a WAV file ready for the ffmpeg assembly step.
// Groups word-level TTS timepoints into 3-4 word caption segments
function buildCaptionsFromTimepoints(words, timepoints) {
  const markTimes = {};
  for (const tp of timepoints) {
    const idx = parseInt(String(tp.markName || '').replace(/^w/, ''), 10);
    if (!isNaN(idx)) markTimes[idx] = Number(tp.timeSeconds);
  }
  const timed = [];
  for (let i = 0; i < words.length; i++) {
    if (markTimes[i] !== undefined) timed.push({ word: words[i], start: markTimes[i] });
  }
  if (timed.length === 0) return [];
  for (let i = 0; i < timed.length - 1; i++) timed[i].end = timed[i + 1].start;
  timed[timed.length - 1].end = timed[timed.length - 1].start + 0.4;
  const segments = [];
  for (let i = 0; i < timed.length; i += 4) {
    const group = timed.slice(i, i + 4);
    segments.push({
      text:  group.map(w => w.word).join(' '),
      start: parseFloat(group[0].start.toFixed(2)),
      end:   parseFloat(group[group.length - 1].end.toFixed(2)),
    });
  }
  return segments;
}

// Returns actual audio duration in seconds via ffprobe (fallback: 30)
function getAudioDurationSec(audioPath) {
  return new Promise(resolve => {
    exec(
      `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${audioPath}"`,
      (err, stdout) => { resolve(parseFloat(stdout) || 30); }
    );
  });
}

// Returns { audioPath, captions } — captions use word-level TTS timestamps when available,
// falling back to audio-duration-based even distribution (never character-count estimation).
async function pipelineGenerateVoiceover(script, userId) {
  const apiKey = process.env.GOOGLE_TTS_API_KEY;
  if (!apiKey) throw new Error('GOOGLE_TTS_API_KEY not configured');

  const ttsUrl = `https://texttospeech.googleapis.com/v1/text:synthesize?key=${apiKey}`;

  let scriptText = script.slice(0, 5000).replace(/<[^>]*>/g, '').trim();

  let lastErr;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const requestBody = {
        input: { text: scriptText },
        voice: { languageCode: 'en-US', name: 'en-US-Journey-F', ssmlGender: 'FEMALE' },
        audioConfig: { audioEncoding: 'MP3', speakingRate: 1.1, pitch: 0.0 },
      };
      console.log('[Voiceover] TTS request:', JSON.stringify(requestBody).slice(0, 200));
      const ttsResponse = await fetch(ttsUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody),
      });

      if (!ttsResponse.ok) {
        const errBody = await ttsResponse.text();
        throw new Error(`TTS REST API error ${ttsResponse.status}: ${errBody}`);
      }

      const ttsData = await ttsResponse.json();
      const audioBuffer = Buffer.from(ttsData.audioContent, 'base64');
      const audioPath = `/tmp/vly_voiceover_${userId}_${Date.now()}.mp3`;
      require('fs').writeFileSync(audioPath, audioBuffer);

      const dur = await getAudioDurationSec(audioPath);
      const captions = buildFallbackCaptions(scriptText, dur);
      console.log(`[Voiceover] ✓ ${audioPath} — ${dur.toFixed(1)}s → ${captions.length} caption segments`);
      return { audioPath, captions };
    } catch (err) {
      lastErr = err;
      console.warn(`[Voiceover] Attempt ${attempt}/3 failed: ${err.message}`);
      if (attempt < 3) await new Promise(r => setTimeout(r, 2000));
    }
  }
  throw new Error(`Voiceover failed after 3 attempts: ${lastErr.message}`);
}

// Royalty-free tracks organised by genre — selected by per-channel styleConfig.musicGenre
const MUSIC_BY_GENRE = {
  ambient:   [
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3',
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-5.mp3',
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-8.mp3',
  ],
  upbeat:    [
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-2.mp3',
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-4.mp3',
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-6.mp3',
  ],
  cinematic: [
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-3.mp3',
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-7.mp3',
  ],
  lofi:      [
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3',
    'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-5.mp3',
  ],
  none:      [],
};
// Flat pool for callers that don't pass a styleConfig (backward-compatible)
const ROYALTY_FREE_MUSIC = [...new Set(Object.values(MUSIC_BY_GENRE).flat())];

// Extract 5 distinct Pexels search queries from video title + script
// Derives 5 diverse search queries from different script aspects.
// excludeQueries: Set of queries already used today (to prevent same-day repeats).
function deriveFootageQueries(title, script, nicheName = '', excludeQueries = new Set()) {
  const stop = new Set([
    'the','a','an','is','are','was','were','this','that','these','those',
    'with','for','and','but','or','not','to','of','in','on','at','by','from','about',
    'how','why','what','when','where','who','will','can','do','does','did','be','been',
    'have','has','had','get','got','make','made','take','your','you','my','me','we',
    'our','they','their','it','its','if','so','than','then','into','over','under','up',
    'just','very','more','most','also','even','still','well','good','great','best','new',
    'time','days','years','people','things','way','ways','need','want','know','think','feel',
  ]);
  const extractKw = (text, limit) => {
    const seen = new Set(); const kw = [];
    for (const w of (text.toLowerCase().match(/\b[a-z]{4,}\b/g) || [])) {
      if (!stop.has(w) && !seen.has(w)) { seen.add(w); kw.push(w); }
      if (kw.length >= limit) break;
    }
    return kw;
  };

  const titleKw  = extractKw(title, 6);
  const third    = Math.floor(script.length / 3);
  const openKw   = extractKw(script.slice(0, third), 6);         // opening: main topic
  const midKw    = extractKw(script.slice(third, third * 2), 6); // middle: action/process
  const closeKw  = extractKw(script.slice(third * 2), 6);        // closing: outcome/emotion
  const allKw    = extractKw(title + ' ' + script, 12);

  const nicheSlug = (nicheName || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();

  // 5 aspect-based candidate pools (tried in order)
  const aspectPools = [
    // Aspect 1 — primary topic (from title or opening)
    [
      titleKw[0] && titleKw[1] ? `${titleKw[0]} ${titleKw[1]}` : null,
      titleKw[0] && openKw[0]  ? `${titleKw[0]} ${openKw[0]}`  : null,
      openKw[0]  && openKw[1]  ? `${openKw[0]} ${openKw[1]}`   : null,
      allKw[0]   && allKw[1]   ? `${allKw[0]} ${allKw[1]}`     : null,
    ],
    // Aspect 2 — action/process (from middle of script)
    [
      midKw[0]  && midKw[1]  ? `${midKw[0]} ${midKw[1]}`   : null,
      midKw[0]  && allKw[3]  ? `${midKw[0]} ${allKw[3]}`   : null,
      allKw[2]  && allKw[3]  ? `${allKw[2]} ${allKw[3]}`   : null,
      nicheSlug              ? `${nicheSlug} process`       : 'focused working process',
    ],
    // Aspect 3 — setting / environment (cinematic variety)
    [
      nicheSlug ? `${nicheSlug} workspace`    : null,
      nicheSlug ? `${nicheSlug} environment`  : null,
      openKw[2] ? `${openKw[2]} outdoor`      : null,
      midKw[2]  ? `${midKw[2]} indoors`       : null,
      'modern office workspace',
      'urban city street',
      'bright natural light',
    ],
    // Aspect 4 — outcome / emotion (closing of script)
    [
      closeKw[0] && closeKw[1] ? `${closeKw[0]} ${closeKw[1]}` : null,
      closeKw[0]               ? `${closeKw[0]} success`        : null,
      nicheSlug                ? `${nicheSlug} results`         : null,
      'confident person success',
      'motivated inspiring moment',
      'achievement celebration',
    ],
    // Aspect 5 — niche baseline lifestyle
    [
      nicheSlug ? `${nicheSlug} lifestyle`   : null,
      nicheSlug ? `${nicheSlug} motivation`  : null,
      nicheSlug ? `${nicheSlug} people`      : null,
      'lifestyle motivation mindset',
      'productive morning routine',
      'people smiling success',
      'energetic positive attitude',
    ],
  ];

  const chosen = [];
  const chosenSet = new Set();
  for (const pool of aspectPools) {
    for (const candidate of pool) {
      if (!candidate) continue;
      const q = candidate.trim().slice(0, 80);
      if (chosenSet.has(q) || excludeQueries.has(q)) continue;
      chosenSet.add(q);
      chosen.push(q);
      break; // one per aspect
    }
    if (chosen.length >= 8) break;
  }

  // Pad with generic fallbacks if fewer than 8
  const generic = [
    'productive morning','focused mindset','success journey','bright future','motivated action',
    'hands typing laptop','sunrise landscape','person walking city','calm nature scene','team collaboration',
    'confident person walking','sunrise time lapse','modern workspace focus','positive energy outdoor',
  ];
  for (const g of generic) {
    if (chosen.length >= 8) break;
    if (!chosenSet.has(g) && !excludeQueries.has(g)) { chosenSet.add(g); chosen.push(g); }
  }

  return chosen.slice(0, 8);
}

// Step 3 — Fetch 8 unique portrait clips from Pexels, excluding previously used video IDs.
// Paginates up to 3 pages per query until 8 unused clips are found.
async function pipelineFetchMultipleFootage(title, script, nicheName = '', userId = null) {
  const apiKey = process.env.PEXELS_API_KEY;
  if (!apiKey) throw new Error('PEXELS_API_KEY not configured');

  // Load previously used footage IDs and today's queries from MongoDB
  const usedIds = new Set();
  const usedQueriesOnDay = new Set();
  if (userId) {
    const userDoc = await User.findById(userId).select('usedFootageIds').lean().catch(() => null);
    if (userDoc?.usedFootageIds) for (const id of userDoc.usedFootageIds) usedIds.add(String(id));

    const today = new Date().toISOString().slice(0, 10);
    const calCol = agentCol('content_calendars');
    const cals = await calCol.find(
      { userId, 'slots.date': today },
      { projection: { 'slots.date': 1, 'slots.footageQueries': 1 } }
    ).toArray().catch(() => []);
    for (const cal of cals) {
      for (const s of (cal.slots || [])) {
        if (s.date === today && Array.isArray(s.footageQueries)) {
          for (const q of s.footageQueries) usedQueriesOnDay.add(q);
        }
      }
    }
    if (usedIds.size > 0 || usedQueriesOnDay.size > 0)
      console.log(`[Footage] User ${userId}: ${usedIds.size} used IDs, ${usedQueriesOnDay.size} today's queries excluded`);
  }

  const queries = deriveFootageQueries(title, script, nicheName, usedQueriesOnDay);
  const clips = [];
  const pickedIds = new Set(); // avoid duplicates within this batch

  for (const query of queries) {
    let found = false;
    for (let page = 1; page <= 3 && !found; page++) {
      try {
        const params = new URLSearchParams({
          query: query.slice(0, 100),
          per_page: '15',
          orientation: 'portrait',
          page: String(page),
        });
        const res = await fetch(`https://api.pexels.com/videos/search?${params}`, {
          headers: { Authorization: apiKey },
        });
        const data = await res.json();
        logAPIUsage('pexels', 'video_search', userId, 1, 0, true);

        const videos = (data.videos || []).filter(v =>
          v.duration >= 5 &&
          !usedIds.has(String(v.id)) &&
          !pickedIds.has(String(v.id))
        );

        if (!videos.length) {
          console.warn(`[Footage] All p${page} results used for "${query}" — trying next page`);
          continue;
        }

        const video = videos[0];
        const file  = (video.video_files || []).find(f => f.quality === 'hd') || (video.video_files || [])[0];
        if (!file?.link) continue;

        pickedIds.add(String(video.id));
        clips.push({ id: String(video.id), url: file.link, duration: video.duration, query });
        console.log(`[Footage] "${query}" p${page}: id=${video.id}, ${video.duration}s`);
        found = true;
      } catch (e) {
        logAPIUsage('pexels', 'video_search', userId, 1, 0, false);
        console.warn(`[Footage] Failed for "${query}" p${page}:`, e.message);
      }
    }
    if (!found) console.warn(`[Footage] No unused clip found for "${query}" after 3 pages`);
  }

  if (!clips.length) throw new Error('No Pexels footage clips fetched');
  return clips;
}

// Build caption segments from raw script text when the script generator didn't produce them.
// Splits on sentence/clause boundaries; timing is proportional to word count vs totalDuration.
function buildFallbackCaptions(scriptText, totalDuration) {
  const text = String(scriptText || '').trim();
  if (!text || totalDuration <= 0) return [];

  const BUFFER      = 0.05; // 50 ms pre-roll — caption appears just before word is spoken
  const PAUSE_WORDS = new Set(['and', 'but', 'so', 'because', 'or', 'yet', 'while', 'when', 'if', 'then']);

  // Split on sentence-ending punctuation and commas
  const rawSentences = text
    .split(/[.!?,]+/)
    .map(s => s.trim())
    .filter(s => s.length > 0);

  // Each sentence is one chunk; if > 8 words split at nearest natural pause word
  const chunks = [];
  for (const sentence of rawSentences) {
    const words = sentence.split(/\s+/).filter(Boolean);
    if (!words.length) continue;

    if (words.length <= 8) {
      chunks.push(words);
    } else {
      const mid = Math.ceil(words.length / 2);
      let splitIdx = -1;
      for (let delta = 0; delta <= mid && splitIdx === -1; delta++) {
        for (const idx of [mid - delta, mid + delta]) {
          if (idx > 1 && idx < words.length - 1 && PAUSE_WORDS.has(words[idx].toLowerCase())) {
            splitIdx = idx;
            break;
          }
        }
      }
      if (splitIdx === -1) splitIdx = mid;
      chunks.push(words.slice(0, splitIdx));
      if (words.slice(splitIdx).length > 0) chunks.push(words.slice(splitIdx));
    }
  }

  if (!chunks.length) return [];

  // timePerWord = totalDuration / totalWords
  const totalWords  = chunks.reduce((sum, c) => sum + c.length, 0);
  const timePerWord = totalDuration / Math.max(totalWords, 1);

  // Next chunk starts exactly where previous ends (no gap); 0.05s pre-roll applied to each start
  const captions = [];
  let wordOffset = 0;
  for (let i = 0; i < chunks.length; i++) {
    const chunk      = chunks[i];
    const nextOffset = wordOffset + chunk.length;
    const start      = parseFloat(Math.max(0, wordOffset * timePerWord - BUFFER).toFixed(3));
    const end        = i < chunks.length - 1
      ? parseFloat(Math.max(start + 0.1, nextOffset * timePerWord - BUFFER).toFixed(3))
      : parseFloat(totalDuration.toFixed(3));
    captions.push({ text: chunk.join(' '), start, end });
    wordOffset = nextOffset;
  }

  return captions;
}

// Detect caption font paths once at startup; used by buildDrawtextFilters.
// Prefers Liberation Sans > DejaVu Sans (both clean sans-serif, available via Nixpacks).
// Uses fc-list first so Nix store paths are found regardless of install location.
const CAPTION_FONT_PATHS = (() => {
  const fs = require('fs');
  const { execSync } = require('child_process');

  // fc-list first — works on both Ubuntu and Nix environments
  try {
    const raw   = execSync('fc-list --format="%{file}\\n"', { timeout: 5000 }).toString();
    const files = raw.split('\n').map(l => l.trim()).filter(Boolean);

    // Prefer Liberation Sans, fall back to DejaVu Sans — reject anything serif/decorative
    const isSansSerif = f => /Liberation.*Sans|DejaVu.*Sans(?!.*Mono)|Ubuntu-[RBM]/i.test(f) &&
                             !/Serif|Mono|Cond|Narrow|Oblique|Italic/i.test(f);

    const libBold = files.find(f => /LiberationSans-Bold\.ttf$/i.test(f));
    const libReg  = files.find(f => /LiberationSans-Regular\.ttf$/i.test(f));
    const djvBold = files.find(f => /DejaVuSans-Bold\.ttf$/i.test(f));
    const djvReg  = files.find(f => /DejaVuSans\.ttf$/i.test(f));
    const anyBold = files.find(f => isSansSerif(f) && /Bold/i.test(f));
    const anyReg  = files.find(f => isSansSerif(f) && !/Bold|Italic|Oblique/i.test(f));

    const reg  = libReg  || djvReg  || anyReg;
    const bold = libBold || djvBold || anyBold || reg;

    if (reg) {
      const r = { regular: reg, bold };
      console.log('[Captions] Font (fc-list):', r);
      return r;
    }
  } catch (_) { /* fontconfig not available — fall through to static paths */ }

  // Static fallback — Ubuntu system paths and macOS (local dev)
  const staticCandidates = [
    ['/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf', '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf'],
    ['/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',                 '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'],
    ['/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf',                   '/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf'],
    ['/System/Library/Fonts/Supplemental/Arial.ttf',                    '/System/Library/Fonts/Supplemental/Arial Bold.ttf'],
  ];
  for (const [reg, bold] of staticCandidates) {
    if (fs.existsSync(reg)) {
      const r = { regular: reg, bold: fs.existsSync(bold) ? bold : reg };
      console.log('[Captions] Font (static):', r);
      return r;
    }
  }

  console.warn('[Captions] No sans-serif font found — ffmpeg will use its built-in default (may be serif)');
  return { regular: null, bold: null };
})();

// Sanitize caption text for safe use in ffmpeg drawtext option values
function sanitizeCaptionText(text) {
  return String(text || '')
    .replace(/'/g, '’')             // curly apostrophe — avoids breaking single-quoted option values
    .replace(/:/g, '\\:')               // escape colon (ffmpeg option separator)
    .replace(/,/g, ' ')                 // comma would be misread as filter separator
    .replace(/[^\w\s’!?.\\]/g, '') // strip remaining special chars
    .trim();
}

// Build an array of ffmpeg drawtext filter strings for two-color two-line captions.
// Each caption segment produces: line1 (white bold, fontsize=45) + optional line2 (accent, fontsize=40).
// styleConfig: { captionFont: 'clean'|'bold'|'impact', colorScheme: 'white-yellow'|'white-green'|'white-orange' }
function buildDrawtextFilters(captions, styleConfig = null) {
  const filters = [];

  // Resolve font paths from styleConfig
  const fontOption = (styleConfig?.captionFont) || 'clean';
  let resolvedBold = CAPTION_FONT_PATHS.bold    || CAPTION_FONT_PATHS.regular;
  let resolvedReg  = CAPTION_FONT_PATHS.regular || CAPTION_FONT_PATHS.bold;
  // 'bold' and 'impact' both lines use bold font
  const useBoldForBoth = fontOption === 'bold' || fontOption === 'impact';
  if (useBoldForBoth) resolvedReg = resolvedBold || resolvedReg;

  const fontBold = resolvedBold ? `:fontfile='${resolvedBold}'` : '';
  const fontReg  = resolvedReg  ? `:fontfile='${resolvedReg}'`  : '';

  // Resolve accent color from colorScheme
  const colorMap = { 'white-yellow': '#FFE500', 'white-green': '#39FF14', 'white-orange': '#FF8C00' };
  const accentColor = colorMap[(styleConfig?.colorScheme) || 'white-yellow'] || '#FFE500';

  // 'impact' style uses larger font sizes to mimic impact-style captions
  const size1 = fontOption === 'impact' ? 52 : 45;
  const size2 = fontOption === 'impact' ? 44 : 40;

  for (const cap of captions) {
    const words = String(cap.text).trim().split(/\s+/).filter(Boolean);
    const s = cap.start.toFixed(3);
    const e = cap.end.toFixed(3);

    let line1Words, line2Words;
    if (words.length <= 4) {
      line1Words = words;
      line2Words = [];
    } else {
      let splitIdx = Math.ceil(words.length / 2);
      if (splitIdx < 2) splitIdx = 2;
      if (words.length - splitIdx < 2) splitIdx = words.length - 2;
      line1Words = words.slice(0, splitIdx);
      line2Words = words.slice(splitIdx);
    }

    const line1 = sanitizeCaptionText(line1Words.join(' '));
    const line2 = sanitizeCaptionText(line2Words.join(' '));

    if (!line1) continue;

    // Line 1 — white #FFFFFF, bold, 4px black outline, no box
    filters.push(
      `drawtext=text='${line1}':fontsize=${size1}${fontBold}:fontcolor=#FFFFFF` +
      `:borderw=4:bordercolor=black:box=0` +
      `:x=(w-text_w)/2:y=h*0.72-${size1}:enable='between(t,${s},${e})'`
    );

    // Line 2 — accent color, 4px black outline, no box
    if (line2) {
      filters.push(
        `drawtext=text='${line2}':fontsize=${size2}${fontReg}:fontcolor=${accentColor}` +
        `:borderw=4:bordercolor=black:box=0` +
        `:x=(w-text_w)/2:y=h*0.72+10:enable='between(t,${s},${e})'`
      );
    }
  }
  return filters;
}

function generateSRTFile(captions, slotId) {
  const toSRTTime = (seconds) => {
    const h  = Math.floor(seconds / 3600);
    const m  = Math.floor((seconds % 3600) / 60);
    const s  = Math.floor(seconds % 60);
    const ms = Math.round((seconds % 1) * 1000);
    return String(h).padStart(2,'0')+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0')+','+String(ms).padStart(3,'0');
  };
  const srtContent = captions.map((c, i) =>
    `${i+1}\n${toSRTTime(c.start)} --> ${toSRTTime(c.end)}\n${c.text}\n`
  ).join('\n');
  const srtPath = `/tmp/vly_subs_${slotId}.srt`;
  require('fs').writeFileSync(srtPath, srtContent, 'utf8');
  return srtPath;
}

// Step 4 — Assemble: concat clips, burn-in drawtext captions (Shorts only), mix voiceover + background music
// styleConfig: { captionFont, colorScheme, musicGenre, introText } — all optional, defaults applied if absent.
// If the captions filter fails, retries without captions.
async function pipelineAssembleVideo(footageClips, audioPath, outputPath, captions = [], isShort = true, styleConfig = null) {
  const fs   = require('fs');
  const path = require('path');
  const runId = Date.now();

  // Download footage clips in parallel
  const clipPaths = footageClips.map((_, i) => path.join('/tmp', `vly_clip_${runId}_${i}.mp4`));
  await Promise.all(footageClips.map((clip, i) =>
    new Promise((resolve, reject) => {
      exec(`curl -sL --max-time 60 -o "${clipPaths[i]}" "${clip.url}"`, { timeout: 90000 },
        err => err ? reject(new Error(`Clip ${i} download failed: ${err.message}`)) : resolve());
    })
  ));

  // Download background music — pick genre pool from styleConfig, fall back to full pool
  const genre    = styleConfig?.musicGenre || 'ambient';
  const pool     = (MUSIC_BY_GENRE[genre] && MUSIC_BY_GENRE[genre].length > 0) ? MUSIC_BY_GENRE[genre] : ROYALTY_FREE_MUSIC;
  const musicUrl = genre === 'none' ? null : pool[Math.floor(Math.random() * pool.length)];
  const musicPath = path.join('/tmp', `vly_music_${runId}.mp3`);
  let hasMus = false;
  if (musicUrl) {
    try {
      await new Promise((resolve, reject) => {
        exec(`curl -sL --max-time 30 -o "${musicPath}" "${musicUrl}"`, { timeout: 45000 },
          err => err ? reject(err) : resolve());
      });
      hasMus = fs.existsSync(musicPath) && fs.statSync(musicPath).size > 1000;
    } catch (e) {
      console.warn('[Assembly] Music download failed (non-fatal):', e.message);
    }
  } else {
    console.log('[Assembly] Music genre=none — skipping background music');
  }

  const n       = clipPaths.length;
  const clipDur = 8; // 8s per clip × 8 clips = 64s footage — covers a 55-65s voiceover with headroom

  // Base filter parts: scale/crop/trim each clip, then concat
  const baseParts = [];
  for (let i = 0; i < n; i++) {
    baseParts.push(
      `[${i}:v]scale=1080:1920:force_original_aspect_ratio=increase,` +
      `crop=1080:1920,setsar=1,trim=0:${clipDur},setpts=PTS-STARTPTS[v${i}]`
    );
  }
  const concatIn = Array.from({ length: n }, (_, i) => `[v${i}]`).join('');
  baseParts.push(`${concatIn}concat=n=${n}:v=1:a=0[vcat]`);

  const voiceIdx   = n;
  const audioFilter = hasMus
    ? `[${voiceIdx}:a][${n + 1}:a]amix=inputs=2:duration=shortest:weights=1 0.15[aout]`
    : `[${voiceIdx}:a]volume=1.0[aout]`;

  // Build drawtext caption filters (Shorts only) — two-color two-line system with per-channel style
  console.log(`[DIAG] pipelineAssembleVideo — isShort=${isShort}, captions.length=${captions.length}, genre=${styleConfig?.musicGenre||'ambient'}, captions sample:`, JSON.stringify(captions.slice(0,2)));
  const drawtextFilters = (isShort && captions.length > 0) ? buildDrawtextFilters(captions, styleConfig) : [];

  // Optional 2-second intro text overlay (Shorts only)
  const introText = isShort ? sanitizeCaptionText(styleConfig?.introText || '') : '';
  if (introText) {
    const introFont = CAPTION_FONT_PATHS.bold ? `:fontfile='${CAPTION_FONT_PATHS.bold}'` : '';
    drawtextFilters.unshift(
      `drawtext=text='${introText}':fontsize=46${introFont}:fontcolor=#FFFFFF` +
      `:borderw=4:bordercolor=black:box=0:x=(w-text_w)/2:y=(h-text_h)/2:enable='between(t,0,2)'`
    );
  }
  console.log(`[DIAG] drawtextFilters.length=${drawtextFilters.length}`);
  if (drawtextFilters.length > 0) {
    console.log(`[Assembly] Drawtext: ${drawtextFilters.length} filter(s) for ${captions.length} caption segment(s)`);
    console.log(`[DIAG] First drawtext filter:`, drawtextFilters[0]);
  }

  // Helper: run ffmpeg with inline filter_complex string
  const runFFmpeg = (filterStr, label) => new Promise((res, rej) => {
    const args = ['-y'];
    for (const p of clipPaths) args.push('-i', p);
    args.push('-i', audioPath);
    if (hasMus) args.push('-i', musicPath);
    args.push('-filter_complex', filterStr);
    args.push('-map', '[vout]', '-map', '[aout]');
    args.push('-c:v', 'libx264', '-preset', 'fast', '-crf', '23');
    args.push('-c:a', 'aac', '-b:a', '128k');
    args.push('-movflags', '+faststart');
    args.push('-shortest');
    args.push(outputPath);
    const cmdDisplay = ['ffmpeg', ...args].map(a => (/ |;|,/.test(a) ? `"${a}"` : a)).join(' ');
    console.log(`[Assembly] ffmpeg cmd [${label}]:\n${cmdDisplay}`);
    const ff = spawn('ffmpeg', args);
    const stderrLines = [];
    ff.stderr.on('data', d => stderrLines.push(...d.toString().split('\n')));
    ff.on('close', code => {
      if (code !== 0) {
        const tail = stderrLines.filter(l => l.trim()).slice(-5).join('\n');
        rej(new Error(`ffmpeg [${label}] failed (exit ${code}):\n${tail}`));
      } else res();
    });
    ff.on('error', err => rej(new Error(`ffmpeg spawn error: ${err.message}`)));
  });

  const cleanup = () => {
    for (const p of clipPaths) fs.unlink(p, () => {});
    if (hasMus) fs.unlink(musicPath, () => {});
  };

  try {
    // First attempt: with drawtext caption overlays (Shorts only)
    if (drawtextFilters.length > 0) {
      // Chain all drawtext filters on [vcat] → [vout]
      const captionChain = `[vcat]${drawtextFilters.join(',')}[vout]`;
      const filterWithCaptions = [...baseParts, captionChain, audioFilter].join(';');
      console.log(`[DIAG] filter_complex with captions (first 800 chars):`, filterWithCaptions.slice(0, 800));
      try {
        await runFFmpeg(filterWithCaptions, 'captions');
        cleanup();
        console.log(`[Assembly] ✓ ${outputPath} (two-color captions)`);
        return outputPath;
      } catch (capErr) {
        console.warn(`[Assembly] Caption pass failed — retrying without captions:\n${capErr.message}`);
      }
    }

    // Fallback or Long-form: no captions
    const filterNoSubs = [...baseParts, '[vcat]null[vout]', audioFilter].join(';');
    await runFFmpeg(filterNoSubs, 'nosubs');
    cleanup();
    console.log(`[Assembly] ✓ ${outputPath}${isShort ? ' (captions skipped — fallback)' : ' (Long-form)'}`);
    return outputPath;
  } catch (err) {
    cleanup();
    throw err;
  }
}

// Step 5 — Refresh a YouTube OAuth access token and persist to MongoDB
async function pipelineRefreshToken(channel) {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({
      client_id:     process.env.YOUTUBE_CLIENT_ID,
      client_secret: process.env.YOUTUBE_CLIENT_SECRET,
      refresh_token: channel.refreshToken,
      grant_type:    'refresh_token',
    }),
  });
  const data = await res.json();
  if (!data.access_token) throw new Error(data.error_description || 'Token refresh failed');
  await User.updateOne(
    { 'youtubeChannels.channelId': channel.channelId },
    { $set: { 'youtubeChannels.$.accessToken': data.access_token } }
  );
  return data.access_token;
}

// Step 6 — Upload assembled video to YouTube
// options = { thumbPath, userId } — both optional; thumbPath triggers a thumbnails.set call post-upload.
async function pipelineUploadToYouTube(videoPath, title, description, channel, isShort = true, options = {}) {
  // Quota guard — throws so callers can reschedule rather than silently dropping the upload
  const uploadQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.upload, null).catch(() => ({ allowed: true }));
  if (!uploadQC.allowed) throw new Error(`YouTube quota check failed: ${uploadQC.reason}`);

  const { google } = require('googleapis');
  const fs = require('fs');
  const oauth2 = new google.auth.OAuth2(
    process.env.YOUTUBE_CLIENT_ID,
    process.env.YOUTUBE_CLIENT_SECRET
  );

  const uploadTitle = isShort && !title.includes('#Shorts')
    ? (title + ' #Shorts').slice(0, 100)
    : title.slice(0, 100);
  const uploadDesc  = ((description || title).slice(0, 4800)) +
    (isShort ? '\n\n#Shorts #YouTubeShorts #viral' : '');
  const uploadTags  = isShort
    ? ['shorts', 'youtube shorts', 'viral', 'trending']
    : ['youtube', 'educational'];

  const tryUpload = async (accessToken) => {
    oauth2.setCredentials({ access_token: accessToken, refresh_token: channel.refreshToken });
    const yt  = google.youtube({ version: 'v3', auth: oauth2 });
    const res = await yt.videos.insert({
      part: ['snippet', 'status'],
      requestBody: {
        snippet: {
          title:       uploadTitle,
          description: uploadDesc,
          tags:        uploadTags,
          categoryId:  '22',
        },
        status: { privacyStatus: 'public', madeForKids: false },
      },
      media: { body: fs.createReadStream(videoPath) },
    });
    return res.data.id;
  };

  // Upload custom thumbnail after the video is live — non-fatal on any error.
  // Waits 30 s before first attempt, then 60 s, then 120 s (exponential backoff, 3 total attempts).
  // Checks videos.list() before each attempt to confirm the video is accessible.
  const doThumbUpload = async (videoId, accessToken) => {
    const { thumbPath, userId, slotId } = options;
    console.log(`[Thumbnail] doThumbUpload — youtubeVideoId=${videoId}, thumbPath=${thumbPath}`);

    if (!thumbPath) {
      console.log(`[Thumbnail] Skipped — no thumbPath provided`);
      return;
    }
    if (!fs.existsSync(thumbPath)) {
      console.error(`[Thumbnail] SKIP — file not found at ${thumbPath} (was it deleted before upload?)`);
      return;
    }

    const thumbSizeBytes = fs.statSync(thumbPath).size;
    const thumbMime      = thumbPath.endsWith('.png') ? 'image/png' : 'image/jpeg';
    console.log(`[Thumbnail] File confirmed: ${thumbPath} | size=${thumbSizeBytes} bytes (${(thumbSizeBytes/1024/1024).toFixed(2)} MB) | mime=${thumbMime}`);

    if (thumbSizeBytes === 0) {
      console.error(`[Thumbnail] SKIP — file is 0 bytes`);
      fs.unlink(thumbPath, () => {});
      return;
    }

    // 50 (thumbnails.set) + 1 (videos.list readiness check) = 51 units per attempt
    const thumbQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.thumbnails + YOUTUBE_QUOTA_COSTS.default, userId).catch(() => ({ allowed: true }));
    if (!thumbQC.allowed) {
      console.warn('[Thumbnail] Quota insufficient — skipping thumbnail upload');
      fs.unlink(thumbPath, () => {});
      return;
    }

    const saveSlotStatus = (fields) => {
      if (!slotId) return;
      agentCol('calendar_slots').updateOne(
        { _id: require('mongodb').ObjectId.isValid(slotId) ? new (require('mongodb').ObjectId)(slotId) : slotId },
        { $set: fields }
      ).catch(() => {});
    };

    const attemptSet = async (token, attempt) => {
      oauth2.setCredentials({ access_token: token, refresh_token: channel.refreshToken });
      const yt = google.youtube({ version: 'v3', auth: oauth2 });

      const statusRes = await yt.videos.list({ part: ['status'], id: [videoId] }).catch(() => null);
      logYoutubeQuota('videos.list', YOUTUBE_QUOTA_COSTS.default, userId).catch(() => {});
      const uploadStatus = statusRes?.data?.items?.[0]?.status?.uploadStatus;
      console.log(`[Thumbnail] attempt ${attempt} — video uploadStatus=${uploadStatus || 'unknown'}`);
      if (uploadStatus && uploadStatus !== 'processed' && uploadStatus !== 'uploaded') {
        throw new Error(`Video not ready — uploadStatus=${uploadStatus}`);
      }

      console.log(`[Thumbnail] thumbnails.set attempt ${attempt} — youtubeVideoId=${videoId}, size=${thumbSizeBytes} bytes`);
      const result = await yt.thumbnails.set({
        videoId,
        media: { mimeType: thumbMime, body: fs.createReadStream(thumbPath) },
      });
      console.log(`[Thumbnail] thumbnails.set attempt ${attempt} HTTP ${result?.status}:`, JSON.stringify(result?.data || {}).slice(0, 800));
      return result;
    };

    const backoffDelays = [30000, 60000, 120000];
    let succeeded = false;
    try {
      let lastErr;
      for (let attempt = 1; attempt <= 3; attempt++) {
        const waitMs = backoffDelays[attempt - 1];
        console.log(`[Thumbnail] Waiting ${waitMs / 1000}s before attempt ${attempt}…`);
        await new Promise(r => setTimeout(r, waitMs));
        try {
          const result = await attemptSet(accessToken, attempt);
          console.log(`[Thumbnail] Successfully set thumbnail for video ${videoId} (attempt ${attempt})`);
          logYoutubeQuota('thumbnails.set', YOUTUBE_QUOTA_COSTS.thumbnails, userId).catch(() => {});
          logAPIUsage('youtube', 'thumbnail_upload', userId || null, 0, 0, true).catch(() => {});
          if (userId && channel?.channelId) {
            User.findOneAndUpdate(
              { _id: userId, 'youtubeChannels.channelId': channel.channelId },
              { $set: { 'youtubeChannels.$.thumbnailsEnabled': true } }
            ).catch(() => {});
          }
          saveSlotStatus({ thumbnailUploaded: true, thumbnailStatus: 'success', thumbnailAppliedAt: new Date() });
          succeeded = true;
          break;
        } catch (e) {
          lastErr = e;
          const fullErr = e?.response?.data || e?.message;
          console.error(`[Thumbnail] thumbnails.set attempt ${attempt} FAILED — youtubeVideoId=${videoId}`);
          console.error(`[Thumbnail] Full YouTube API error response:`, JSON.stringify(fullErr));
        }
      }
      if (!succeeded && lastErr) {
        logAPIUsage('youtube', 'thumbnail_upload', userId || null, 0, 0, false).catch(() => {});
        const errData = lastErr?.response?.data;
        const reason  = errData?.error?.errors?.[0]?.reason || '';
        const isThumbnailDisabled = reason === 'forbidden' || /thumbnail.*not.*enabl|custom.*thumbnail/i.test(lastErr.message);
        if (isThumbnailDisabled && userId && channel?.channelId) {
          User.findOneAndUpdate(
            { _id: userId, 'youtubeChannels.channelId': channel.channelId },
            { $set: { 'youtubeChannels.$.thumbnailsEnabled': false } }
          ).catch(() => {});
          console.log(`[ThumbnailCheck] Marked ${channel.channelId} thumbnailsEnabled=false (ineligibility confirmed after 3 attempts)`);
        }
        const errMsg = errData ? JSON.stringify(errData) : lastErr.message;
        saveSlotStatus({ thumbnailStatus: 'failed', thumbnailError: errMsg });
      }
    } finally {
      fs.unlink(thumbPath, () => {});
    }
  };

  try {
    const videoId = await tryUpload(channel.accessToken);
    logAPIUsage('youtube', 'video_upload', null, API_COSTS.youtube_upload, 0, true);
    logYoutubeQuota('upload', YOUTUBE_QUOTA_COSTS.upload, null).catch(() => {});
    await doThumbUpload(videoId, channel.accessToken);
    return videoId;
  } catch (err) {
    if (err.code === 401 || err.status === 401 || /invalid_grant|token/.test(err.message)) {
      console.log(`[Pipeline] Token expired for ${channel.channelId} — refreshing`);
      const newToken = await pipelineRefreshToken(channel);
      channel.accessToken = newToken;
      try {
        const videoId = await tryUpload(newToken);
        logAPIUsage('youtube', 'video_upload', null, API_COSTS.youtube_upload, 0, true);
        logYoutubeQuota('upload', YOUTUBE_QUOTA_COSTS.upload, null).catch(() => {});
        await doThumbUpload(videoId, newToken);
        return videoId;
      } catch (retryErr) {
        logAPIUsage('youtube', 'video_upload', null, API_COSTS.youtube_upload, 0, false);
        throw retryErr;
      }
    }
    logAPIUsage('youtube', 'video_upload', null, API_COSTS.youtube_upload, 0, false);
    throw err;
  }
}

// Shared Imagen 4 caller — used by generateThumbnail and the admin test endpoint.
// Returns { imageBytes, mimeType, rawBody, httpStatus } or throws with full error detail.
async function callImagenAPI(prompt, aspectRatio) {
  const apiKey = process.env.GOOGLE_IMAGEN_API_KEY || process.env.GOOGLE_TTS_API_KEY;
  if (!apiKey) throw new Error('GOOGLE_IMAGEN_API_KEY not set');

  const IMAGEN_MODEL = 'imagen-4.0-fast-generate-001';
  const imagenUrl    = `https://generativelanguage.googleapis.com/v1beta/models/${IMAGEN_MODEL}:predict?key=${apiKey}`;
  const imagenBody   = {
    instances:  [{ prompt }],
    parameters: { sampleCount: 1, aspectRatio, outputMimeType: 'image/jpeg' },
  };

  console.log(`[Imagen] POST ${imagenUrl.replace(apiKey, 'KEY=...'+apiKey.slice(-4))}`);
  console.log(`[Imagen] Request body:`, JSON.stringify({ ...imagenBody, instances: [{ prompt: prompt.slice(0, 80) + '…' }] }));

  const res = await fetch(imagenUrl, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(imagenBody),
  });

  const rawBody = await res.text();
  console.log(`[Imagen] HTTP ${res.status} ${res.statusText}`);
  console.log(`[Imagen] Response body (first 600 chars):`, rawBody.slice(0, 600));

  if (!res.ok) {
    throw new Error(`Imagen API HTTP ${res.status}: ${rawBody}`);
  }

  let data;
  try { data = JSON.parse(rawBody); } catch (_) {
    throw new Error(`Imagen API returned non-JSON: ${rawBody.slice(0, 200)}`);
  }

  // :predict endpoint returns predictions[].bytesBase64Encoded + mimeType
  const pred       = data.predictions?.[0];
  const imageBytes = pred?.bytesBase64Encoded;
  const mimeType   = pred?.mimeType || 'image/png';

  if (!imageBytes) {
    console.error('[Imagen] No image bytes in response — full body:', rawBody);
    throw new Error(`Imagen returned no image data. Response keys: ${Object.keys(data).join(', ')}`);
  }

  console.log(`[Imagen] ✓ Got image — mime=${mimeType}, base64Len=${imageBytes.length}`);
  return { imageBytes, mimeType, rawBody, httpStatus: res.status };
}

// Generate a YouTube thumbnail via Google Imagen 4 (imagen-4-0-generate-001).
// Returns the local /tmp path on success, null on any failure (never throws).
async function generateThumbnail(title, nicheName, videoType, slotId) {
  const fs = require('fs');
  if (!process.env.GOOGLE_IMAGEN_API_KEY && !process.env.GOOGLE_TTS_API_KEY) {
    console.warn('[Thumbnail] GOOGLE_IMAGEN_API_KEY not set — skipping');
    return null;
  }
  try {
    const isShort    = (videoType || 'Short').toLowerCase() !== 'long-form';
    const shortTitle = title.split(/\s+/).slice(0, 5).join(' ');
    const prompt     =
      `Professional YouTube thumbnail, bold white text '${shortTitle}' with thick black outline, ` +
      `vibrant high contrast background representing ${nicheName}, dramatic professional lighting, ` +
      `eye-catching colors, no people or faces, clean composition optimised for high click-through rate`;

    const { imageBytes, mimeType } = await callImagenAPI(prompt, isShort ? '9:16' : '16:9');

    // Always jpg — Imagen is called with outputMimeType:'image/jpeg'
    const thumbPath = `/tmp/thumbnail_${slotId}.jpg`;
    fs.writeFileSync(thumbPath, Buffer.from(imageBytes, 'base64'));
    const sizeBytes = fs.statSync(thumbPath).size;
    console.log(`[Thumbnail] Saved: ${thumbPath} (${sizeBytes} bytes, ${(sizeBytes / 1024 / 1024).toFixed(2)} MB)`);
    if (sizeBytes < 1000) throw new Error(`Thumbnail suspiciously small: ${sizeBytes} bytes`);
    if (sizeBytes > 2 * 1024 * 1024) {
      console.error(`[Thumbnail] File too large: ${(sizeBytes / 1024 / 1024).toFixed(2)} MB — YouTube max is 2 MB. Discarding.`);
      fs.unlink(thumbPath, () => {});
      return null;
    }

    logAPIUsage('imagen4', 'thumbnail', null, 0, 0.02, true).catch(() => {});
    return thumbPath;
  } catch (err) {
    console.error(`[Thumbnail] Generation FAILED: ${err.message}`);
    logAPIUsage('imagen4', 'thumbnail', null, 0, 0, false).catch(() => {});
    return null;
  }
}

// =============================================================================
// JIT PIPELINE — full script→voiceover→footage→assembly→upload in one shot.
// Called by runScheduledPosting for each slot due within 35 minutes.
// Updates calendar_slots directly by _id. Throws on failure — caller logs and marks failed.
// =============================================================================
async function runJITPipelineForSlot(slotDoc, user, slotsCol) {
  const fs        = require('fs');
  const slotId    = slotDoc._id;
  const nicheName = slotDoc.nicheName || user.nicheName || '';
  const isShort   = (slotDoc.type || 'Short').toLowerCase() !== 'long-form';
  const outPath   = `/tmp/vly_jit_${String(slotDoc.userId)}_${slotId}.mp4`;
  let audioPath   = null;
  let thumbPath   = null;

  const setStatus = (fields) => slotsCol.updateOne({ _id: slotId }, { $set: fields }).catch(() => {});

  let hookId       = null;
  let hookCategory = null;
  let selectedHook = null;

  // Track in PIPELINE_STATUS
  pipelineEnsureTodayStats();
  const psEntry = { slotId: String(slotId), title: slotDoc.title, userId: String(slotDoc.userId), startedAt: new Date().toISOString(), step: '1/5' };
  PIPELINE_STATUS.currentlyProcessing.push(psEntry);
  const psUpdate = (step) => { psEntry.step = step; };

  try {
    // 0/5 Pre-flight — resolve channel and verify YouTube upload quota BEFORE spending
    // on any paid external API (OpenAI script, Imagen 4 thumbnail, Google TTS voiceover).
    // Fail fast here saves ~$0.02–$0.10 per aborted video.
    const channel = (user.youtubeChannels || []).find(ch => ch.channelId === slotDoc.channelId)
                 || user.youtubeChannels?.[0];
    if (!channel) {
      throw new Error('No YouTube channel connected — aborting before any paid API calls');
    }
    if (!channel.accessToken) {
      throw new Error('YouTube channel has no access token — aborting before any paid API calls');
    }

    // Quota pre-flight: if upload quota is already exhausted, skip all paid work now
    const preflightQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.upload, String(slotDoc.userId)).catch(() => ({ allowed: true }));
    if (!preflightQC.allowed) {
      const slotDate  = slotDoc.date || new Date().toISOString().slice(0, 10);
      const tomorrow  = new Date(slotDate + 'T00:00:00Z');
      tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
      const newDate   = tomorrow.toISOString().slice(0, 10);
      await setStatus({ status: 'scheduled', date: newDate, pipelineStatus: 'rescheduled-quota-preflight', rescheduleReason: preflightQC.reason, rescheduledAt: new Date().toISOString() });
      PIPELINE_STATUS.currentlyProcessing = PIPELINE_STATUS.currentlyProcessing.filter(e => e.slotId !== String(slotId));
      console.log(`[Preflight] ✗ "${slotDoc.title}" — YouTube quota insufficient, rescheduled to ${newDate} ($0 spent on script/thumbnail/voiceover)`);
      return null;
    }

    // 1/5 Script
    psUpdate('1/5'); console.log(`[JIT] 1/5 Script — "${slotDoc.title}"`);
    await setStatus({ pipelineStatus: 'generating-script' });

    // Select hook from library (non-fatal — pipeline runs even without a hook)
    selectedHook = await selectHookForSlot(String(slotDoc.userId), slotDoc.channelId, nicheName);
    if (selectedHook) {
      hookId       = String(selectedHook._id);
      hookCategory = selectedHook.category;
      console.log(`[JIT] Hook selected: "${selectedHook.template.slice(0, 60)}…" [${hookCategory}]`);
      agentCol('hooksLibrary').updateOne(
        { _id: selectedHook._id },
        { $inc: { timesUsed: 1 }, $set: { lastUsed: new Date().toISOString() } }
      ).catch(() => {});
    }

    const scriptData = await pipelineGenerateScript(slotDoc.title, nicheName, slotDoc.type, selectedHook?.template || null, String(slotDoc.userId), slotDoc.channelId || '');
    const script     = scriptData.script;
    await setStatus({
      scriptText: script, scriptCaptions: scriptData.captions,
      cachedScript: script.slice(0, 2000), pipelineStatus: 'script-done',
      hookId, hookCategory, hookTemplate: selectedHook?.template || null,
      scriptContext: scriptData.scriptContext || null,
    });
    PIPELINE_STATUS.todayStats.generated++;
    console.log(`[JIT] 1/5 Done — ${scriptData.wordCount} words`);

    // 2/5 Thumbnail — skipped when channel is confirmed ineligible (saves ~$0.02/video on Imagen 4)
    psUpdate('2/5');
    if (channel.thumbnailsEnabled === false) {
      // thumbnailsEnabled is explicitly false = YouTube confirmed this channel cannot use custom thumbnails.
      // Calling Imagen 4 would waste $0.02 and produce a file that thumbnails.set() will reject.
      console.log(`[Thumbnail] Skipping Imagen 4 — channel "${channel.channelName || channel.channelId}" not thumbnail eligible, saving ~$0.02`);
      await setStatus({ pipelineStatus: 'thumbnail-skipped-ineligible' });
    } else {
      // thumbnailsEnabled is true (confirmed) or null/undefined (unknown — try optimistically)
      console.log(`[JIT] 2/5 Thumbnail — "${slotDoc.title}"`);
      await setStatus({ pipelineStatus: 'generating-thumbnail' });
      thumbPath = await generateThumbnail(slotDoc.title, nicheName, slotDoc.type, String(slotId));
      if (thumbPath) {
        await setStatus({ thumbnailPath: thumbPath, pipelineStatus: 'thumbnail-done' });
        PIPELINE_STATUS.todayStats.thumbnails++;
      } else {
        await setStatus({ pipelineStatus: 'thumbnail-skipped' });
      }
      console.log(`[JIT] 2/5 Done — ${thumbPath ? thumbPath : 'skipped'}`);
    }

    // 3/5 Voiceover
    psUpdate('3/5');
    const scriptWordCount = script.split(/\s+/).filter(Boolean).length;
    const estimatedSecs   = Math.round((scriptWordCount / 130) * 60 / 1.1); // 130 WPM, TTS speakingRate=1.1
    console.log(`[JIT] 3/5 Voiceover — "${slotDoc.title}" | script: ${scriptWordCount} words → ~${estimatedSecs}s audio`);
    await setStatus({ pipelineStatus: 'generating-voiceover' });
    const voResult = await pipelineGenerateVoiceover(script, String(slotDoc.userId));
    audioPath = voResult.audioPath;
    const captions = voResult.captions.length > 0 ? voResult.captions : scriptData.captions;
    await setStatus({ voiceoverGenerated: true, pipelineStatus: 'voiceover-done' });
    console.log(`[JIT] 3/5 Done`);

    // 4/5 Footage
    psUpdate('4/5'); console.log(`[JIT] 4/5 Footage — "${slotDoc.title}"`);
    await setStatus({ pipelineStatus: 'fetching-footage' });
    const footageClips = await pipelineFetchMultipleFootage(slotDoc.title, script, nicheName, String(slotDoc.userId));
    await setStatus({
      footageUrls:      footageClips.map(c => c.url),
      footageQueries:   footageClips.map(c => c.query),
      footageDurations: footageClips.map(c => c.duration),
      footageIds:       footageClips.map(c => c.id || ''),
      pipelineStatus:   'footage-done',
    });
    console.log(`[JIT] 4/5 Done — ${footageClips.length} clips`);

    // 5/5 Assemble + Upload
    // (Quota pre-checked at pipeline start — pre-flight check above catches exhaustion before any paid calls)
    psUpdate('5/5');
    const finalCaptions = (isShort && captions.length === 0)
      ? buildFallbackCaptions(script, footageClips.length * 6)
      : captions;

    console.log(`[DIAG] finalCaptions.length=${finalCaptions.length}, isShort=${isShort}, voResult.captions.length=${voResult.captions.length}, scriptData.captions.length=${scriptData.captions.length}`);
    console.log(`[DIAG] finalCaptions sample:`, JSON.stringify(finalCaptions.slice(0, 3)));
    // channel resolved in pre-flight above

    console.log(`[JIT] 5/5 Assembling "${slotDoc.title}" → ${outPath} (style: font=${channel.styleConfig?.captionFont||'clean'} color=${channel.styleConfig?.colorScheme||'white-yellow'} music=${channel.styleConfig?.musicGenre||'ambient'})`);
    await setStatus({ pipelineStatus: 'assembling' });
    await pipelineAssembleVideo(footageClips, audioPath, outPath, finalCaptions, isShort, channel.styleConfig || null);

    // ── Hold until the exact scheduled post time — never upload early.
    // The cron starts the pipeline up to 35 min before scheduledPostTime so the
    // video is ready on time. Without this wait, a fast pipeline (< 35 min) uploads
    // the video immediately after assembly, posting it early.
    const rawScheduled = slotDoc.scheduledPostTime || '';
    const scheduledTs  = rawScheduled
      ? new Date(rawScheduled.endsWith('Z') ? rawScheduled : rawScheduled + 'Z').getTime()
      : NaN;
    if (!isNaN(scheduledTs)) {
      const holdMs = scheduledTs - Date.now();
      if (holdMs > 500 && holdMs < 40 * 60 * 1000) {
        // Wait up to 40 min — sanity cap prevents an accidental infinite sleep
        console.log(`[JIT] ⏳ "${slotDoc.title}" assembly done — holding ${Math.round(holdMs / 1000)}s until ${rawScheduled}`);
        await setStatus({ pipelineStatus: 'waiting-for-schedule' });
        await new Promise(r => setTimeout(r, holdMs));
      } else if (holdMs <= 500) {
        console.log(`[JIT] "${slotDoc.title}" — scheduledPostTime reached (${rawScheduled}), uploading now`);
      } else {
        console.warn(`[JIT] "${slotDoc.title}" — holdMs=${holdMs} exceeds 40 min cap, uploading immediately`);
      }
    }

    console.log(`[JIT] Uploading "${slotDoc.title}"`);
    await setStatus({ pipelineStatus: 'uploading' });
    const description = script.slice(0, 4800) || slotDoc.title;
    const ytId = await pipelineUploadToYouTube(outPath, slotDoc.title, description, channel, isShort, {
      thumbPath,
      userId:  String(slotDoc.userId),
      slotId:  String(slotId),
    });

    await setStatus({
      status:         'posted',
      posted:         true,
      postedAt:       new Date().toISOString(),
      youtubeVideoId: ytId,
      pipelineStatus: 'posted',
      assembledPath:  null,
      thumbnailPath:  null,
      pipelineError:  null,
    });

    // Track hook posting performance
    if (hookId && selectedHook) {
      agentCol('hooksLibrary').updateOne(
        { _id: selectedHook._id },
        { $inc: { timesPerformed: 1 } }
      ).catch(() => {});
    }

    const usedClipIds = footageClips.map(c => c.id).filter(Boolean);
    if (usedClipIds.length > 0) {
      User.findByIdAndUpdate(slotDoc.userId, { $addToSet: { usedFootageIds: { $each: usedClipIds } } }).catch(() => {});
    }
    fs.unlink(outPath, () => {});
    if (audioPath) { fs.unlink(audioPath, () => {}); audioPath = null; }
    // thumbPath is deleted inside doThumbUpload (whether upload succeeds or fails)
    PIPELINE_STATUS.currentlyProcessing = PIPELINE_STATUS.currentlyProcessing.filter(e => e.slotId !== String(slotId));
    PIPELINE_STATUS.lastCompletedAt = new Date().toISOString();
    PIPELINE_STATUS.todayStats.posted++;
    console.log(`[JIT] ✓ "${slotDoc.title}" → https://youtu.be/${ytId}`);
    return ytId;
  } catch (err) {
    // Attach the current pipeline step so the caller (runScheduledPosting) can write it to MongoDB
    if (!err.pipelineStep) err.pipelineStep = psEntry.step;
    PIPELINE_STATUS.currentlyProcessing = PIPELINE_STATUS.currentlyProcessing.filter(e => e.slotId !== String(slotId));
    PIPELINE_STATUS.lastError = { message: err.message, slotId: String(slotId), title: slotDoc.title, at: new Date().toISOString() };
    PIPELINE_STATUS.todayStats.failed++;
    if (audioPath) { require('fs').unlink(audioPath, () => {}); }
    if (thumbPath) { require('fs').unlink(thumbPath, () => {}); }
    require('fs').unlink(outPath, () => {});
    throw err;
  }
}

// ─── Title pattern analysis — GPT-4o identifies winning structures from performance data ───
// enriched = [{ title, views, likes, comments, score }] sorted desc by score
async function analyzeTitlePatterns(enriched, nicheName) {
  if (!process.env.OPENAI_API_KEY || !enriched.length) return null;
  try {
    const { OpenAI } = require('openai');
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    const titleList = enriched
      .map((v, i) => `${i + 1}. "${v.title}" — ${v.views.toLocaleString()} views, ${v.likes} likes`)
      .join('\n');

    const res = await openai.chat.completions.create({
      model: 'gpt-4o',
      messages: [{
        role: 'user',
        content: `You are a YouTube title strategy expert. Analyse these video titles from a "${nicheName}" channel and identify what makes the top performers successful.

VIDEO PERFORMANCE DATA (sorted by views, highest first):
${titleList}

Return JSON with exactly these fields:
{
  "patterns": [exactly 5 strings — reusable title templates from high performers, e.g. "How to [action] in [timeframe]", "Why you should never [X]"],
  "powerWords": [8 to 12 strings — specific words, numbers, or emotional triggers from top titles],
  "idealLength": "string — optimal character range for this channel, e.g. '45–55 characters'",
  "topExamples": [exactly 3 strings — the 3 best actual titles verbatim from the data above],
  "insight": "string — 1–2 sentences on what title style works best for this niche and why"
}`,
      }],
      temperature: 0.3,
      response_format: { type: 'json_object' },
    });

    const p = JSON.parse(res.choices[0].message.content);
    return {
      patterns:    Array.isArray(p.patterns)   ? p.patterns.slice(0, 5)   : [],
      powerWords:  Array.isArray(p.powerWords) ? p.powerWords.slice(0, 12) : [],
      idealLength: p.idealLength  || 'Under 60 characters',
      topExamples: Array.isArray(p.topExamples) ? p.topExamples.slice(0, 3) : enriched.slice(0, 3).map(v => v.title),
      insight:     p.insight || '',
    };
  } catch (err) {
    console.error('[TitlePatterns] Analysis failed (non-fatal):', err.message);
    return null;
  }
}

// ─── Performance analysis — fetches YouTube stats for posted videos (last 4 weeks) ───
// Returns { topPerformers, bottomPerformers, totalAnalyzed, titlePatterns } or null if no history.
async function analyzeChannelPerformance(userId, channelId, nicheName) {
  try {
    const col      = agentCol('content_calendars');
    const cutoff   = new Date();
    cutoff.setDate(cutoff.getDate() - 28);
    const cutoffStr = cutoff.toISOString().slice(0, 10);

    const calendars = await col.find({ userId: String(userId) }).toArray();
    const postedSlots = [];
    for (const cal of calendars) {
      const slots = (cal.slots || []).filter(s =>
        s.posted && s.youtubeVideoId && (s.postedAt || '') >= cutoffStr
      );
      postedSlots.push(...slots);
    }
    if (!postedSlots.length) return null; // first-week exception

    const user = await User.findById(userId).catch(() => null);
    if (!user) return null;
    const channel = (user.youtubeChannels || []).find(ch => ch.channelId === channelId)
                 || (user.youtubeChannels || [])[0];
    if (!channel?.accessToken) return null;

    const { google } = require('googleapis');
    const oauth2 = new google.auth.OAuth2(
      process.env.YOUTUBE_CLIENT_ID,
      process.env.YOUTUBE_CLIENT_SECRET
    );
    oauth2.setCredentials({ access_token: channel.accessToken, refresh_token: channel.refreshToken });
    const yt = google.youtube({ version: 'v3', auth: oauth2 });

    const videoIds = [...new Set(postedSlots.map(s => s.youtubeVideoId))].slice(0, 50);
    // videos.list = 1 unit
    const perfQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.default, String(userId)).catch(() => ({ allowed: true }));
    if (!perfQC.allowed) {
      console.warn(`[Quota] analyzeChannelPerformance videos.list blocked — ${perfQC.reason}`);
      return null;
    }
    const statsRes = await yt.videos.list({ part: ['statistics'], id: videoIds });
    logYoutubeQuota('videos.list', YOUTUBE_QUOTA_COSTS.default, String(userId)).catch(() => {});
    const statsMap = {};
    for (const item of (statsRes.data.items || [])) statsMap[item.id] = item.statistics;

    const enriched = postedSlots
      .map(s => {
        const st    = statsMap[s.youtubeVideoId] || {};
        const views = parseInt(st.viewCount   || 0);
        const likes = parseInt(st.likeCount   || 0);
        const cmts  = parseInt(st.commentCount || 0);
        // composite score: views dominate, engagement multiplies
        const score = views + (likes * 10) + (cmts * 20);
        return { title: s.title, type: s.type || 'Short', views, likes, comments: cmts, score };
      })
      .sort((a, b) => b.score - a.score);

    const titlePatterns = await analyzeTitlePatterns(enriched, nicheName || '');

    return {
      totalAnalyzed:    enriched.length,
      topPerformers:    enriched.slice(0, 3),
      bottomPerformers: enriched.slice(-3).reverse(),
      titlePatterns,
    };
  } catch (err) {
    console.error('[Performance] Analysis failed (non-fatal):', err.message);
    return null; // graceful fallback — generate without optimisation
  }
}

// Main pipeline runner — processes all due approved slots across all users.
// Pass forceSlotKey = "day_videoIndex" to bypass the time check for one specific slot (post-now).
// =============================================================================
// AUTOMATED PRODUCTION PIPELINE
// =============================================================================

// Core pipeline for a single slot: script → voiceover → footage → assembly.
// Saves assembledPath + cachedScript to the slot doc, marks status "ready".
// Throws on any step failure — caller handles retries.
async function runProductionPipelineForSlot(calendar, slot, user, col) {
  const nicheName = calendar.nicheName || user.nicheName || '';
  let audioPath = null;

  await col.updateOne(
    { _id: calendar._id },
    { $set: { [`slots.$[s].status`]: 'processing', [`slots.$[s].pipelineStatus`]: 'generating', [`slots.$[s].pipelineStartedAt`]: new Date().toISOString() } },
    { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
  ).catch(() => {});

  let failedStep = 'pre_flight';
  try {
    // 1/3 Script
    failedStep = 'script_generation';
    console.log(`[Pipeline] 1/3 Script — "${slot.title}" (${nicheName})`);
    const scriptData = await pipelineGenerateScript(slot.title, nicheName, slot.type, null, String(calendar.userId), slot.channelId || calendar.channelId || '');
    const script     = scriptData.script;
    console.log(`[Pipeline] 1/3 Done — ${scriptData.wordCount} words, ${scriptData.captions.length} captions`);

    agentCol('scripts').updateOne(
      { userId: calendar.userId, slotDay: slot.day, videoIndex: slot.videoIndex || 1 },
      { $set: {
        userId: calendar.userId, calendarId: calendar._id.toString(),
        slotDay: slot.day, videoIndex: slot.videoIndex || 1,
        title: scriptData.title, hook: scriptData.hook, loopEnding: scriptData.loopEnding,
        captions: scriptData.captions, hashtags: scriptData.hashtags,
        wordCount: scriptData.wordCount, fullScript: script,
        scriptContext: scriptData.scriptContext || null,
        updatedAt: new Date().toISOString(),
      }},
      { upsert: true }
    ).catch(() => {});

    // Save script data to slot so assembly can be deferred to posting time
    await col.updateOne(
      { _id: calendar._id },
      { $set: {
        [`slots.$[s].scriptText`]:     script,
        [`slots.$[s].scriptCaptions`]: scriptData.captions,
        [`slots.$[s].scriptHashtags`]: scriptData.hashtags,
        [`slots.$[s].cachedScript`]:   script.slice(0, 2000),
        [`slots.$[s].pipelineStatus`]: 'script-done',
        [`slots.$[s].scriptContext`]:  scriptData.scriptContext || null,
      }},
      { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
    ).catch(() => {});

    // 2/3 Voiceover — validate API and capture word-timestamp captions; discard audio (regenerated at posting time)
    failedStep = 'tts';
    console.log(`[Pipeline] 2/3 Voiceover — "${slot.title}"`);
    const voResult = await pipelineGenerateVoiceover(script, String(calendar.userId));
    require('fs').unlink(voResult.audioPath, () => {});
    // audioPath stays null — file already cleaned up; word-timestamp captions saved below
    const voUpdate = { [`slots.$[s].voiceoverGenerated`]: true, [`slots.$[s].pipelineStatus`]: 'voiceover-done' };
    if (voResult.captions.length > 0) voUpdate[`slots.$[s].scriptCaptions`] = voResult.captions;
    await col.updateOne(
      { _id: calendar._id },
      { $set: voUpdate },
      { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
    ).catch(() => {});
    console.log('[Pipeline] Waiting 60s after voiceover before next pipeline step…');
    await new Promise(r => setTimeout(r, 60000));

    // 3/3 Footage — fetch clip URLs and save to MongoDB (no /tmp files here)
    failedStep = 'pexels_fetch';
    console.log(`[Pipeline] 3/3 Footage — "${slot.title}"`);
    const footageClips = await pipelineFetchMultipleFootage(slot.title, script, nicheName, String(calendar.userId));
    console.log(`[Pipeline] 3/3 Done — ${footageClips.length} clip(s): ${footageClips.map(c => c.query).join(' | ')}`);

    // Mark data-ready — assembly deferred to posting time so /tmp files survive only minutes
    await col.updateOne(
      { _id: calendar._id },
      { $set: {
        [`slots.$[s].status`]:            'approved',
        [`slots.$[s].pipelineStatus`]:    'data-ready',
        [`slots.$[s].footageUrls`]:       footageClips.map(c => c.url),
        [`slots.$[s].footageQueries`]:    footageClips.map(c => c.query),
        [`slots.$[s].footageDurations`]:  footageClips.map(c => c.duration),
        [`slots.$[s].footageIds`]:        footageClips.map(c => c.id || ''),
        [`slots.$[s].assembledPath`]:     null,
        [`slots.$[s].pipelineError`]:     null,
      }},
      { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
    );
    console.log(`[Pipeline] ✓ Data cached for "${slot.title}" — assembly deferred to post time`);
    return null; // no assembled file — runAssembleAndUpload handles it at posting time
  } catch (err) {
    if (!err.pipelineStep) err.pipelineStep = failedStep;
    if (audioPath) require('fs').unlink(audioPath, () => {});
    throw err;
  }
}

// Assembles and uploads a slot using data cached in MongoDB (scriptText, footageUrls, scriptCaptions).
// Called by PostingCron when scheduledPostTime arrives, or inline when a /tmp file is missing.
// Marks the slot "assembling" at start to prevent duplicate runs across cron ticks.
async function runAssembleAndUpload(calendar, slot, user, calCol, channel) {
  const fs        = require('fs');
  const slotKey   = `day${slot.day}_vi${slot.videoIndex || 1}`;
  const outPath   = `/tmp/vly_ready_${String(calendar.userId)}_${slotKey}.mp4`;
  const isShort   = (slot.type || 'Short').toLowerCase() !== 'long-form';
  const nicheName = calendar.nicheName || user.nicheName || '';
  let audioPath   = null;

  // Mark assembling so concurrent cron ticks skip this slot
  await calCol.updateOne(
    { _id: calendar._id },
    { $set: { 'slots.$[s].status': 'assembling', 'slots.$[s].pipelineStatus': 'assembling' } },
    { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
  ).catch(() => {});

  try {
    let scriptText = slot.scriptText || slot.cachedScript;
    if (!scriptText) {
      console.log(`[AssembleUpload] No cached script for "${slot.title}" — running full pipeline inline`);
      // Reset to pending so runProductionPipelineForSlot can set status correctly
      await calCol.updateOne(
        { _id: calendar._id },
        { $set: { 'slots.$[s].status': 'pending', 'slots.$[s].pipelineStatus': 'no-script-reset' } },
        { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
      ).catch(() => {});
      // Run full pipeline: script → voiceover → footage → marks slot approved with all data cached
      await runProductionPipelineForSlot(calendar, slot, user, calCol);
      // Re-fetch slot from MongoDB — it now has scriptText, footageUrls, etc.
      const freshCal  = await calCol.findOne({ _id: calendar._id });
      const freshSlot = (freshCal?.slots || []).find(s => s.day === slot.day && (s.videoIndex || 1) === (slot.videoIndex || 1));
      if (!freshSlot?.scriptText && !freshSlot?.cachedScript) {
        throw new Error('Full pipeline ran but script still not cached — aborting assembly');
      }
      return runAssembleAndUpload(freshCal, freshSlot, user, calCol, channel);
    }

    // Regenerate voiceover from cached script — returns fresh word-timestamp captions
    console.log(`[AssembleUpload] 1/3 Voiceover for "${slot.title}"`);
    const voResult = await pipelineGenerateVoiceover(scriptText, String(calendar.userId));
    audioPath = voResult.audioPath;

    // Use cached footage URLs or re-fetch if missing
    let footageClips;
    if (Array.isArray(slot.footageUrls) && slot.footageUrls.length > 0) {
      footageClips = slot.footageUrls.map((url, i) => ({
        url,
        id:       (slot.footageIds      || [])[i] || '',
        query:    (slot.footageQueries  || [])[i] || slot.title,
        duration: (slot.footageDurations|| [])[i] || 6,
      }));
      console.log(`[AssembleUpload] 2/3 Using ${footageClips.length} cached footage URL(s) for "${slot.title}"`);
    } else {
      console.log(`[AssembleUpload] 2/3 No cached footage — re-fetching for "${slot.title}"`);
      footageClips = await pipelineFetchMultipleFootage(slot.title, scriptText, nicheName, String(calendar.userId));
    }

    // Use fresh word-timestamp captions; fall back to MongoDB-cached ones, then duration-based estimate
    let captions = voResult.captions.length > 0
      ? voResult.captions
      : (Array.isArray(slot.scriptCaptions) ? slot.scriptCaptions : []);
    if (isShort && captions.length === 0) captions = buildFallbackCaptions(scriptText, footageClips.length * 6);

    // Assemble — use channel's styleConfig if available
    const assembleStyle = channel?.styleConfig || null;
    console.log(`[AssembleUpload] 3/3 Assembling "${slot.title}" → ${outPath} (style: font=${assembleStyle?.captionFont||'clean'} color=${assembleStyle?.colorScheme||'white-yellow'} music=${assembleStyle?.musicGenre||'ambient'})`);
    await pipelineAssembleVideo(footageClips, audioPath, outPath, captions, isShort, assembleStyle);

    // Upload
    const description = slot.cachedScript || scriptText || slot.title;
    const ytId = await pipelineUploadToYouTube(outPath, slot.title, description, channel, isShort);

    await calCol.updateOne(
      { _id: calendar._id },
      { $set: {
        'slots.$[s].status':         'posted',
        'slots.$[s].posted':         true,
        'slots.$[s].postedAt':       new Date().toISOString(),
        'slots.$[s].youtubeVideoId': ytId,
        'slots.$[s].pipelineStatus': 'posted',
        'slots.$[s].assembledPath':  null,
      }},
      { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
    );
    // Record used footage IDs so they are excluded from future videos
    const usedClipIds = footageClips.map(c => c.id).filter(Boolean);
    if (usedClipIds.length > 0) {
      User.findByIdAndUpdate(calendar.userId, { $addToSet: { usedFootageIds: { $each: usedClipIds } } })
        .catch(e => console.warn('[AssembleUpload] Failed to save used footage IDs:', e.message));
    }
    fs.unlink(outPath, () => {});
    if (audioPath) { fs.unlink(audioPath, () => {}); audioPath = null; }
    console.log(`[AssembleUpload] ✓ "${slot.title}" → https://youtu.be/${ytId} (${usedClipIds.length} footage IDs recorded)`);
    return ytId;
  } catch (err) {
    if (audioPath) require('fs').unlink(audioPath, () => {});
    require('fs').unlink(outPath, () => {});
    // Reset to approved so next cron tick retries
    await calCol.updateOne(
      { _id: calendar._id },
      { $set: {
        'slots.$[s].status':         'approved',
        'slots.$[s].pipelineStatus': 'assemble-failed',
        'slots.$[s].pipelineError':  err.message.slice(0, 300),
      }},
      { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
    ).catch(() => {});
    throw err;
  }
}

// Retries runProductionPipelineForSlot up to maxRetries times with 5-minute delays.
// Marks the slot "failed" after all attempts are exhausted — never throws.
async function runProductionPipelineWithRetry(calendar, slot, user, col, maxRetries = 3) {
  const delayMs = ms => new Promise(r => setTimeout(r, ms));
  let lastErr;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await runProductionPipelineForSlot(calendar, slot, user, col);
    } catch (err) {
      lastErr = err;
      console.error(`[Pipeline] ✗ Attempt ${attempt}/${maxRetries} failed for "${slot.title}": ${err.message}`);
      if (attempt < maxRetries) {
        await col.updateOne(
          { _id: calendar._id },
          { $set: {
            [`slots.$[s].retryCount`]:    (slot.retryCount || 0) + attempt,
            [`slots.$[s].lastFailedAt`]:  new Date().toISOString(),
            [`slots.$[s].pipelineError`]: err.message.slice(0, 300),
          }},
          { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
        ).catch(() => {});
        console.log(`[Pipeline] Retrying "${slot.title}" in 5 min (${maxRetries - attempt} retries left)…`);
        await delayMs(5 * 60 * 1000);
      }
    }
  }
  const _failTs    = new Date().toISOString();
  const _failStep  = lastErr.pipelineStep || 'unknown';
  const _slotLabel = `${slot.day}_${slot.videoIndex || 1}`;
  console.error(`[PIPELINE FAIL] slotId=${_slotLabel} step=${_failStep} error=${lastErr.message}`);
  await col.updateOne(
    { _id: calendar._id },
    { $set: {
      [`slots.$[s].status`]:         'failed',
      [`slots.$[s].pipelineStatus`]: 'failed',
      [`slots.$[s].pipelineError`]:  lastErr.message.slice(0, 500),
      [`slots.$[s].failedAt`]:       _failTs,
      [`slots.$[s].failureReason`]:  { step: _failStep, error: lastErr.message, stack: (lastErr.stack || '').slice(0, 200), timestamp: _failTs },
    }},
    { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
  ).catch(() => {});
  console.error(`[Pipeline] ✗✗ Permanently failed "${slot.title}" after ${maxRetries} attempts — skipping`);
}

// Generates today's video slot stubs (title + scheduledPostTime only) for every active calendar.
// Does NOT run any pipeline — the PostingCron fires the full JIT pipeline 35 min before each post time.
// Idempotent: skips users who already have today's slots. Called by the 00:01 cron and startup check.
async function runDailySlotGeneration(targetUserId = null, targetDate = null) {
  if (!process.env.OPENAI_API_KEY) {
    console.log('[DailyGen] Skipping — OPENAI_API_KEY not configured'); return { generated: 0 };
  }
  const today    = targetDate || new Date().toISOString().slice(0, 10);
  const calCol   = agentCol('content_calendars');
  const slotsCol = agentCol('calendar_slots');
  const { OpenAI } = require('openai');
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  // --- Build unified work list ---
  // Source 1: users with an active content_calendars document
  const calQuery  = targetUserId
    ? { status: 'active', userId: String(targetUserId) }
    : { status: 'active' };
  const calendars = await calCol.find(calQuery).toArray();
  console.log(`[DailyGen] Starting for ${today} — ${calendars.length} active calendar(s) found`);

  // workItems shape: { user, channelId, nicheName, calDoc (null for no-calendar users) }
  const workItems = [];
  const processedUserIds = new Set();

  for (const cal of calendars) {
    const user = await User.findById(cal.userId).catch(() => null);
    if (!user) {
      console.warn(`[DailyGen] User ${cal.userId} not found (orphaned calendar ${cal._id}) — skipping`);
      continue;
    }
    workItems.push({
      user,
      channelId: cal.channelId || user.youtubeChannels?.[0]?.channelId || '',
      nicheName: cal.nicheName || user.nicheName || '',
      calDoc: cal,
    });
    processedUserIds.add(String(user._id));
  }

  // Source 2: users who have channels + niche but never generated a 30-day calendar.
  // Without this, they are silently skipped by the cron every night.
  if (!targetUserId) {
    const usersWithChannels = await User.find({
      'youtubeChannels.0': { $exists: true },
      nicheName: { $exists: true, $ne: '' },
      subscriptionStatus: { $nin: ['expired'] },
    }).lean().catch(() => []);

    let fallbackCount = 0;
    for (const u of usersWithChannels) {
      if (!processedUserIds.has(String(u._id))) {
        workItems.push({
          user: u,
          channelId: u.youtubeChannels[0]?.channelId || '',
          nicheName: u.nicheName,
          calDoc: null,
        });
        fallbackCount++;
      }
    }
    if (fallbackCount > 0) {
      console.log(`[DailyGen] +${fallbackCount} user(s) with channels but no active calendar — included`);
    }
  }

  console.log(`[DailyGen] Processing ${workItems.length} user(s) for ${today}`);
  let generated = 0;

  for (const { user, channelId, nicheName, calDoc } of workItems) {
    try {
      // Idempotency — only skip if actionable (scheduled/processing/posted) slots exist.
      // 'pending' slots from content_planner_agent.py use a different format and must not
      // block DailyGen from creating proper 'scheduled' slots the PostingCron can pick up.
      const existingCount = await slotsCol.countDocuments({
        userId: String(user._id), date: today,
        status: { $in: ['scheduled', 'processing', 'posted'] },
      });
      if (existingCount > 0) {
        console.log(`[DailyGen] ${existingCount} actionable slot(s) for ${today} already in DB (user ${user._id}) — skipping`);
        continue;
      }

      // Subscription enforcement — use getPipelineBlock() as single source of truth
      const pipelineBlock = getPipelineBlock(user);
      if (pipelineBlock.blocked) {
        console.log(`[Pipeline] Blocked user ${user.email} — subscription status: ${pipelineBlock.reason}`);
        continue;
      }

      if (!nicheName) { console.warn(`[DailyGen] No niche for user ${user._id} — skipping`); continue; }
      if (!channelId) { console.warn(`[DailyGen] No channelId for user ${user._id} — skipping`); continue; }

      const config = PLAN_CONFIG[user.plan] || PLAN_CONFIG.trial;
      const count  = config.shortsPerDay;

      // Resolve optimal posting times — use cached value if < 7 days old, else recalculate
      const chDoc = (user.youtubeChannels || []).find(c => c.channelId === channelId) || user.youtubeChannels?.[0];
      const cacheAge = chDoc?.optimalTimesCalculatedAt
        ? Date.now() - new Date(chDoc.optimalTimesCalculatedAt).getTime() : Infinity;
      let optimalTimes = (chDoc?.optimalPostingTimes?.length && cacheAge < 7 * 24 * 60 * 60 * 1000)
        ? chDoc.optimalPostingTimes
        : (await getOptimalPostingTimes(String(user._id), channelId, nicheName, user.plan).catch(() => null))?.times;
      if (!optimalTimes?.length) optimalTimes = pickSpreadSlots(NICHE_DEFAULT_TIMES.default, count);

      if (optimalTimes.length < count) {
        const expandedPool = [...new Set([
          ...optimalTimes,
          ...(POSTING_TIMES_BY_COUNT[Math.min(count, 7)] || []),
          ...NICHE_DEFAULT_TIMES.default,
        ])];
        optimalTimes = pickSpreadSlots(expandedPool, count);
      }
      const shortTimes = optimalTimes.slice(0, count);

      const yesterdayCtx = await getYesterdayPerformance(String(user._id), channelId);
      const genRes = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content:
          `YouTube Shorts content strategist for a "${nicheName}" channel.
${yesterdayCtx}Generate exactly ${count} NEW, unique Short video titles (under 60 chars each).
Vary style: hook, story, tutorial, listicle, myth-bust.
Return JSON: { "titles": [${count} strings] }` }],
        temperature: 0.9,
        response_format: { type: 'json_object' },
      });
      const dg_in  = genRes.usage?.prompt_tokens    || 0;
      const dg_out = genRes.usage?.completion_tokens || 0;
      logAPIUsage('openai', 'daily_title_gen', String(user._id), dg_in + dg_out,
        dg_in * API_COSTS.openai_input + dg_out * API_COSTS.openai_output, true);
      let titles = [];
      try { titles = JSON.parse(genRes.choices[0].message.content).titles || []; } catch {}

      const generatedAt = new Date().toISOString();
      const newSlots = [];
      for (let v = 0; v < count; v++) {
        newSlots.push({
          userId: String(user._id), channelId, nicheName,
          day: 1, date: today,
          title: titles[v] || `${nicheName} Short #${v + 1}`,
          type: 'Short', videoIndex: v + 1, totalForDay: count, angle: 'short',
          status: 'scheduled', posted: false, retryCount: 0,
          scheduledPostTime: `${today}T${shortTimes[v]}:00Z`, // explicit UTC
          generatedAt,
        });
      }

      if (config.longFormPerWeek > 0 && new Date(today).getDay() === 1) {
        const lfRes = await openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [{ role: 'user', content:
            `YouTube long-form content strategist for a "${nicheName}" channel.
Generate 1 unique Long-form video title (60–100 chars). In-depth, educational.
Return JSON: { "title": "string" }` }],
          temperature: 0.88,
          response_format: { type: 'json_object' },
        });
        const lf_in  = lfRes.usage?.prompt_tokens    || 0;
        const lf_out = lfRes.usage?.completion_tokens || 0;
        logAPIUsage('openai', 'daily_lf_title_gen', String(user._id), lf_in + lf_out,
          lf_in * API_COSTS.openai_input + lf_out * API_COSTS.openai_output, true);
        let lfTitle = `Deep Dive: ${nicheName}`;
        try { lfTitle = JSON.parse(lfRes.choices[0].message.content).title || lfTitle; } catch {}

        const shortTimesSet = new Set(shortTimes);
        const lfTime = ['08:00','10:00','14:00','16:00','20:00','22:00','23:00']
          .find(t => !shortTimesSet.has(t)) || '08:00';
        newSlots.push({
          userId: String(user._id), channelId, nicheName,
          day: 1, date: today, title: lfTitle,
          type: 'Long-form', videoIndex: count + 1, totalForDay: 1, angle: 'long-form',
          status: 'scheduled', posted: false, retryCount: 0,
          scheduledPostTime: `${today}T${lfTime}:00Z`, // explicit UTC
          generatedAt,
        });
      }

      newSlots.sort((a, b) => a.scheduledPostTime.localeCompare(b.scheduledPostTime));

      await slotsCol.deleteMany({ userId: String(user._id), date: today, posted: { $ne: true } });
      if (newSlots.length) await slotsCol.insertMany(newSlots);

      // Keep calendar doc in sync if one exists for this user
      if (calDoc) {
        const kept   = (calDoc.slots || []).filter(s => s.date !== today || s.posted);
        const merged = [...kept, ...newSlots].sort((a, b) =>
          (a.scheduledPostTime || '') < (b.scheduledPostTime || '') ? -1 : 1
        );
        await calCol.updateOne({ _id: calDoc._id }, { $set: { slots: merged } });
      }

      generated++;
      console.log(`[DailyGen] ✓ ${newSlots.length} slot(s) for ${today} | user ${user._id} | plan: ${user.plan} | times: ${newSlots.map(s => s.scheduledPostTime.slice(11, 16)).join(', ')}`);
    } catch (err) {
      console.error(`[DailyGen] Failed for user ${user._id}:`, err.message);
    }
  }

  console.log(`[DailyGen] Complete — ${generated}/${workItems.length} user(s) had new slots generated for ${today}`);
  return { generated, total: workItems.length };
}

// After the last video of the day posts, pre-generate tomorrow's slots immediately.
// Called after every successful upload; bails out early if any slots are still pending.
async function maybePreGenerateTomorrow(userId) {
  try {
    const today     = new Date().toISOString().slice(0, 10);
    const tomorrow  = new Date();
    tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
    const tomorrowStr = tomorrow.toISOString().slice(0, 10);

    const slotsCol = agentCol('calendar_slots');

    // Check if tomorrow's slots already exist — skip if so.
    const tomorrowCount = await slotsCol.countDocuments({ userId, date: tomorrowStr });
    if (tomorrowCount > 0) return;

    // Check whether every slot for today is terminal (posted / failed / skipped).
    // Read from the calendar doc's embedded slots — that's where posted:true is written.
    const calCol = agentCol('content_calendars');
    const cal    = await calCol.findOne({ userId, status: 'active' });
    if (!cal) return;
    const todaySlots = (cal.slots || []).filter(s => s.date === today);
    if (!todaySlots.length) return;
    const allDone = todaySlots.every(s =>
      s.posted === true || s.status === 'posted' || s.status === 'failed' || s.status === 'skipped'
    );
    if (!allDone) return;

    console.log(`[PostGen] All videos done for ${today} (user ${userId}) — pre-generating tomorrow ${tomorrowStr}`);
    await runDailySlotGeneration(userId, tomorrowStr);
  } catch (err) {
    console.error('[PostGen] maybePreGenerateTomorrow error:', err.message);
  }
}

// JIT posting cron — runs every 5 minutes.
// Finds scheduled slots due within 35 min, runs the full pipeline, and uploads one at a time.
async function runScheduledPosting() {
  const now      = new Date();
  const nowIso   = now.toISOString();
  const today    = nowIso.slice(0, 10);
  const slotsCol = agentCol('calendar_slots');

  // Convert ALL 'pending' slots for today → 'scheduled'.
  // Covers (a) old-code slots that already have scheduledPostTime but wrong status,
  // and (b) content_planner_agent.py slots that use scheduledDate+postTime fields.
  const pendingToConvert = await slotsCol.find({
    status: 'pending', date: today, posted: { $ne: true },
  }).toArray().catch(() => []);
  for (let i = 0; i < pendingToConvert.length; i++) {
    const s = pendingToConvert[i];
    const slotDate = s.date || (s.scheduledDate ? s.scheduledDate.slice(0, 10) : today);
    let scheduledPostTime = s.scheduledPostTime;
    if (!scheduledPostTime) {
      const slotTime = s.postTime || '18:00';
      scheduledPostTime = `${slotDate}T${slotTime}:00Z`; // explicit UTC
    } else if (!scheduledPostTime.endsWith('Z')) {
      scheduledPostTime = scheduledPostTime.slice(0, 19) + 'Z'; // normalise legacy no-Z strings
    }
    if (new Date(scheduledPostTime) < new Date(now.getTime() + 5 * 60 * 1000)) {
      scheduledPostTime = new Date(now.getTime() + (i + 1) * 10 * 60 * 1000).toISOString();
    }
    await slotsCol.updateOne(
      { _id: s._id },
      { $set: { status: 'scheduled', scheduledPostTime, date: slotDate, pipelineStatus: 'converted-from-pending', posted: false } }
    ).catch(() => {});
    console.log(`[PostingCron] Converted pending slot "${s.title}" → scheduled at ${scheduledPostTime}`);
  }

  // Auto-rescue: slots that missed their window get rescheduled to post soon (up to 3 retries).
  // Instead of abandoning them as 'missed', bump scheduledPostTime forward so the pipeline still runs today.
  const fiveMinAgo = new Date(now.getTime() - 5 * 60 * 1000).toISOString();
  const overdueSlots = await slotsCol.find({
    status: 'scheduled', posted: false, date: today, scheduledPostTime: { $lt: fiveMinAgo },
  }).toArray().catch(() => []);
  for (const s of overdueSlots) {
    if ((s.retryCount || 0) >= 3) {
      await slotsCol.updateOne({ _id: s._id }, { $set: { status: 'missed', pipelineStatus: 'missed-window-max-retries' } }).catch(() => {});
      console.log(`[PostingCron] Slot "${s.title}" exceeded max retries — marked missed`);
    } else {
      const rescheduleAt = new Date(now.getTime() + 10 * 60 * 1000).toISOString();
      await slotsCol.updateOne(
        { _id: s._id },
        { $set: { scheduledPostTime: rescheduleAt, pipelineStatus: 'auto-rescheduled' }, $inc: { retryCount: 1 } }
      ).catch(() => {});
      console.log(`[PostingCron] Auto-rescheduled "${s.title}" → ${rescheduleAt} (retry ${(s.retryCount || 0) + 1}/3)`);
    }
  }

  // Find scheduled slots whose scheduledPostTime is within the next 35 minutes.
  // The 35-min window gives the JIT pipeline time to run; the actual YouTube upload
  // is held inside runJITPipelineForSlot until scheduledPostTime is reached.
  const in35min  = new Date(now.getTime() + 35 * 60 * 1000).toISOString();
  const allTodayScheduled = await slotsCol.find({
    status: 'scheduled', posted: false, date: today,
  }).sort({ scheduledPostTime: 1 }).toArray().catch(e => {
    console.error('[PostingCron] Query failed:', e.message); return [];
  });

  // ── Per-slot diagnostic log ──
  allTodayScheduled.forEach(s => {
    const normTime = s.scheduledPostTime
      ? (s.scheduledPostTime.endsWith('Z') ? s.scheduledPostTime : s.scheduledPostTime + 'Z')
      : null;
    const diffSec  = normTime ? Math.round((new Date(normTime) - now) / 1000) : null;
    const isDue    = normTime ? new Date(normTime) <= new Date(in35min) : false;
    console.log(`[PostingCron] Slot "${s.title}": scheduledTime=${s.scheduledPostTime}, now=${nowIso}, diffSec=${diffSec}, due=${isDue}`);
  });

  const dueSlots = allTodayScheduled.filter(s => {
    if (!s.scheduledPostTime) return false;
    const normTime = s.scheduledPostTime.endsWith('Z') ? s.scheduledPostTime : s.scheduledPostTime + 'Z';
    return new Date(normTime) <= new Date(in35min);
  });

  if (!dueSlots.length) {
    const nextSlot = allTodayScheduled[0];
    if (nextSlot) console.log(`[PostingCron] No slots due yet — next: "${nextSlot.title}" at ${nextSlot.scheduledPostTime}`);
    return;
  }

  console.log(`[PostingCron] ${dueSlots.length} slot(s) due — processing one at a time`);

  for (const slotDoc of dueSlots) {
    // Re-read to guard against concurrent cron overlaps
    const fresh = await slotsCol.findOne({ _id: slotDoc._id, status: 'scheduled', posted: false }).catch(() => null);
    if (!fresh) continue;

    await slotsCol.updateOne(
      { _id: fresh._id },
      { $set: { status: 'processing', pipelineStartedAt: nowIso } }
    ).catch(() => {});

    const user = await User.findById(fresh.userId).catch(() => null);
    if (!user) {
      console.error(`[PIPELINE FAIL] slotId=${String(fresh._id)} step=pre_flight error=User not found`);
      await slotsCol.updateOne({ _id: fresh._id }, { $set: {
        status: 'failed', pipelineError: 'User not found', failedAt: nowIso,
        failureReason: { step: 'pre_flight', error: 'User not found', stack: '', timestamp: nowIso },
      }}).catch(() => {});
      continue;
    }
    if (user.blocked) {
      console.log(`[PIPELINE SKIP] slotId=${String(fresh._id)} reason=subscription_expired`);
      await slotsCol.updateOne({ _id: fresh._id }, { $set: {
        status: 'skipped', pipelineStatus: 'user-blocked', pipelineError: 'User account suspended',
        skipReason: 'subscription_expired', skippedAt: nowIso,
      }}).catch(() => {});
      continue;
    }
    const subBlock = getPipelineBlock(user);
    if (subBlock.blocked) {
      console.log(`[PIPELINE SKIP] slotId=${String(fresh._id)} reason=subscription_expired`);
      await slotsCol.updateOne({ _id: fresh._id }, { $set: {
        status: 'skipped', pipelineStatus: `subscription-${subBlock.reason}`,
        pipelineError: `Subscription blocked: ${subBlock.reason}`,
        skipReason: 'subscription_expired', skippedAt: nowIso,
      }}).catch(() => {});
      continue;
    }

    // Channel + OAuth pre-checks — skip rather than fail so quota / script spend is never wasted
    const _ch = (user.youtubeChannels || []).find(c => c.channelId === fresh.channelId) || user.youtubeChannels?.[0];
    if (!_ch) {
      console.log(`[PIPELINE SKIP] slotId=${String(fresh._id)} reason=channel_not_found`);
      await slotsCol.updateOne({ _id: fresh._id }, { $set: {
        status: 'skipped', skipReason: 'channel_not_found', skippedAt: nowIso,
        pipelineError: 'No YouTube channel connected',
      }}).catch(() => {});
      continue;
    }
    if (!_ch.accessToken) {
      console.log(`[PIPELINE SKIP] slotId=${String(fresh._id)} reason=oauth_invalid`);
      await slotsCol.updateOne({ _id: fresh._id }, { $set: {
        status: 'skipped', skipReason: 'oauth_invalid', skippedAt: nowIso,
        pipelineError: 'Channel OAuth access token missing — reconnect channel',
      }}).catch(() => {});
      continue;
    }
    if (user.autoPost === false) {
      console.log(`[PIPELINE SKIP] slotId=${String(fresh._id)} reason=autopost_disabled`);
      await slotsCol.updateOne({ _id: fresh._id }, { $set: {
        status: 'skipped', skipReason: 'autopost_disabled', skippedAt: nowIso,
      }}).catch(() => {});
      continue;
    }

    try {
      console.log(`[PostingCron] JIT pipeline starting for "${fresh.title}" (due ${fresh.scheduledPostTime})`);
      await runJITPipelineForSlot(fresh, user, slotsCol);
      setImmediate(() => maybePreGenerateTomorrow(String(fresh.userId)).catch(() => {}));
    } catch (err) {
      const _jitStepMap = { '1/5': 'script_generation', '2/5': 'thumbnail', '3/5': 'tts', '4/5': 'pexels_fetch', '5/5': 'youtube_upload' };
      const _failStep   = _jitStepMap[err.pipelineStep] || (err.message.includes('upload') ? 'youtube_upload' : err.message.includes('assemble') || err.message.includes('ffmpeg') ? 'ffmpeg' : 'unknown');
      const _failTs     = new Date().toISOString();
      console.error(`[PIPELINE FAIL] slotId=${String(fresh._id)} step=${_failStep} error=${err.message}`);
      await slotsCol.updateOne(
        { _id: fresh._id },
        { $set: {
          status: 'failed', pipelineError: err.message.slice(0, 500), failedAt: _failTs,
          failureReason: { step: _failStep, error: err.message, stack: (err.stack || '').slice(0, 200), timestamp: _failTs },
        }}
      ).catch(() => {});
    }
  }
}

// Topic Scout — searches YouTube for trending videos in a niche (last 7 days, ordered by viewCount).
// Returns top 10 video titles to seed the calendar generation prompt with fresh trends.
// Runs silently as part of the Monday cron job — no API route exposed.
async function scoutTrendingTopics(nicheName) {
  const apiKey = process.env.YOUTUBE_API_KEY;
  if (!apiKey) {
    console.log('[TopicScout] Skipping — YOUTUBE_API_KEY not configured');
    return [];
  }
  const scoutQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.search, null).catch(() => ({ allowed: true }));
  if (!scoutQC.allowed) {
    console.warn(`[Quota] scoutTrendingTopics blocked — ${scoutQC.reason}`);
    return [];
  }
  try {
    const publishedAfter = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(nicheName)}&order=viewCount&type=video&publishedAfter=${encodeURIComponent(publishedAfter)}&maxResults=10&key=${apiKey}`;
    const ytRes = await fetch(url);
    if (!ytRes.ok) {
      console.warn(`[TopicScout] YouTube API returned ${ytRes.status} for "${nicheName}"`);
      return [];
    }
    logYoutubeQuota('search', YOUTUBE_QUOTA_COSTS.search, null).catch(() => {});
    const data   = await ytRes.json();
    const topics = (data.items || []).map(item => item.snippet?.title || '').filter(Boolean);
    console.log(`[TopicScout] Found ${topics.length} trending topics for "${nicheName}"`);
    return topics;
  } catch (err) {
    console.warn('[TopicScout] Failed:', err.message);
    return [];
  }
}

// Weekly AI-optimised calendar regeneration — runs every Monday for all active calendars.
// Fetches YouTube performance data, identifies top/bottom performers, then regenerates
// the next 30 days of slots with AI-optimised prompts injecting that intelligence.
// Fetches yesterday's posted videos and their YouTube view/like counts.
// Returns a prompt-ready context string for the daily title generation, or '' on failure.
async function getYesterdayPerformance(userId, channelId) {
  try {
    const yd = new Date();
    yd.setDate(yd.getDate() - 1);
    const yesterday = yd.toISOString().slice(0, 10);

    const calCol = agentCol('content_calendars');
    const calendars = await calCol.find({ userId: String(userId) }).toArray();
    const postedSlots = [];
    for (const cal of calendars) {
      for (const s of (cal.slots || [])) {
        if (s.posted && s.youtubeVideoId && s.date === yesterday) postedSlots.push(s);
      }
    }
    if (!postedSlots.length) return '';

    const user = await User.findById(userId).catch(() => null);
    if (!user) return '';
    const channel = (user.youtubeChannels || []).find(ch => ch.channelId === channelId)
                 || (user.youtubeChannels || [])[0];
    if (!channel?.accessToken) return '';

    const { google } = require('googleapis');
    const oauth2 = new google.auth.OAuth2(process.env.YOUTUBE_CLIENT_ID, process.env.YOUTUBE_CLIENT_SECRET);
    oauth2.setCredentials({ access_token: channel.accessToken, refresh_token: channel.refreshToken });
    const yt = google.youtube({ version: 'v3', auth: oauth2 });

    const videoIds = [...new Set(postedSlots.map(s => s.youtubeVideoId))].slice(0, 50);
    // videos.list = 1 unit
    const dailyGenQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.default, String(userId)).catch(() => ({ allowed: true }));
    if (!dailyGenQC.allowed) {
      console.warn(`[Quota] DailyGen yesterday-performance videos.list blocked — ${dailyGenQC.reason}`);
      return '';
    }
    const statsRes = await yt.videos.list({ part: ['statistics'], id: videoIds });
    logYoutubeQuota('videos.list', YOUTUBE_QUOTA_COSTS.default, String(userId)).catch(() => {});
    const statsMap = {};
    for (const item of (statsRes.data.items || [])) statsMap[item.id] = item.statistics;

    const enriched = postedSlots
      .map(s => {
        const st = statsMap[s.youtubeVideoId] || {};
        return { title: s.title, views: parseInt(st.viewCount || 0), likes: parseInt(st.likeCount || 0) };
      })
      .filter(v => v.views > 0 || v.likes > 0)
      .sort((a, b) => (b.views + b.likes * 10) - (a.views + a.likes * 10));

    if (!enriched.length) return '';

    const list = enriched.slice(0, 5).map(v => `"${v.title}" (${v.views} views, ${v.likes} likes)`).join('; ');
    return `Yesterday's best performing topics were: ${list}. Generate today's titles in the same niche, inspired by what worked yesterday but with fresh angles.\n`;
  } catch (err) {
    console.error('[DailyGen] Yesterday performance fetch failed (non-fatal):', err.message);
    return '';
  }
}

// POST /api/content/trigger-post — development only (system is fully automated in production)
app.post('/api/content/trigger-post', requireAuth, async (req, res) => {
  if (process.env.NODE_ENV !== 'development') {
    return res.status(403).json({ error: 'Manual trigger disabled — system runs fully automated' });
  }
  console.log(`[Dev] Daily generation trigger by user ${req.user.id}`);
  try {
    await runDailySlotGeneration(String(req.user.id));
    res.json({ success: true, message: 'Daily slot generation triggered — check server logs' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// ADMIN ROUTES — all protected by requireAdmin
// Secret admin JWT is signed with ADMIN_PASSWORD (separate from user JWT_SECRET).
// Access via POST /api/admin/auth first, then pass the returned token as Bearer.
// =============================================================================

// POST /api/admin/login — password-only admin login, no user JWT required.
// This is the primary entry point for the admin panel at /#admin-vrl9k2.
app.post('/api/admin/login', async (req, res) => {
  try {
    const { password } = req.body || {};
    const adminPass = process.env.ADMIN_PASSWORD;
    if (!adminPass) return res.status(503).json({ error: 'Admin not configured' });
    if (!password || password !== adminPass) return res.status(403).json({ error: 'Access denied' });
    const adminJwt = jwt.sign({ role: 'admin' }, adminPass, { expiresIn: '8h' });
    res.json({ success: true, token: adminJwt, expiresIn: '8h' });
  } catch (err) {
    console.error('[Admin] Login error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// POST /api/admin/auth — legacy: verify regular user JWT + ADMIN_PASSWORD, return short-lived admin JWT
app.post('/api/admin/auth', requireAuth, async (req, res) => {
  try {
    const { password } = req.body || {};
    const adminPass  = process.env.ADMIN_PASSWORD;
    const adminEmail = process.env.ADMIN_EMAIL;
    if (!adminPass || !adminEmail) return res.status(503).json({ error: 'Admin not configured' });

    const userDoc = await User.findById(req.user.id).select('email').lean();
    if (!userDoc || userDoc.email !== adminEmail) {
      return res.status(403).json({ error: 'Access denied' });
    }
    if (password !== adminPass) return res.status(403).json({ error: 'Access denied' });

    const adminJwt = jwt.sign({ userId: req.user.id, role: 'admin' }, adminPass, { expiresIn: '8h' });
    res.json({ success: true, token: adminJwt, expiresIn: '8h' });
  } catch (err) {
    console.error('[Admin] Auth error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/overview — high-level platform snapshot
app.get('/api/admin/overview', requireAdmin, async (req, res) => {
  try {
    const today      = new Date().toISOString().slice(0, 10);
    const monthStart = new Date().toISOString().slice(0, 7) + '-01';
    const todayStart = today + 'T00:00:00.000Z';
    const tomorrowStart = new Date(new Date(todayStart).getTime() + 86400000).toISOString();

    const usageCol  = agentCol('apiUsage');
    const slotsCol  = agentCol('calendar_slots');
    const quotaCol  = agentCol('youtubeQuota');

    const [
      totalUsers,
      videosPostedToday,
      totalVideosEver,
      activeUsersTodayDocs,
      costTodayDocs,
      costMonthDocs,
      quotaTodayDocs,
      quotaByUserDocs,
    ] = await Promise.all([
      User.countDocuments(),
      // Count slots posted today by postedAt timestamp (accurate across midnight)
      slotsCol.countDocuments({ posted: true, postedAt: { $gte: todayStart, $lt: tomorrowStart } }),
      slotsCol.countDocuments({ posted: true }),
      // Active today = distinct userIds who posted a video today
      slotsCol.distinct('userId', { posted: true, postedAt: { $gte: todayStart, $lt: tomorrowStart } }),
      usageCol.aggregate([
        { $match: { timestamp: { $gte: todayStart } } },
        { $group: { _id: null, total: { $sum: '$estimatedCost' } } },
      ]).toArray(),
      usageCol.aggregate([
        { $match: { timestamp: { $gte: monthStart + 'T00:00:00.000Z' } } },
        { $group: { _id: null, total: { $sum: '$estimatedCost' } } },
      ]).toArray(),
      quotaCol.aggregate([
        { $match: { date: today } },
        { $group: { _id: null, total: { $sum: '$units' } } },
      ]).toArray(),
      quotaCol.aggregate([
        { $match: { date: today } },
        { $group: { _id: '$userId', units: { $sum: '$units' } } },
        { $sort: { units: -1 } },
      ]).toArray(),
    ]);

    const totalCostToday      = costTodayDocs[0]?.total || 0;
    const totalCostThisMonth  = costMonthDocs[0]?.total || 0;
    const quotaUsedToday      = quotaTodayDocs[0]?.total || 0;
    const quotaByUser         = Object.fromEntries(quotaByUserDocs.map(r => [r._id || 'system', r.units]));
    const activeToday         = activeUsersTodayDocs.length;

    const now = new Date();
    const nextResetAt = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1)).toISOString();

    const planBreakdown = await User.aggregate([
      { $group: { _id: '$plan', count: { $sum: 1 } } },
    ]);
    const perPlanBreakdown = Object.fromEntries(planBreakdown.map(p => [p._id, p.count]));

    res.json({
      success: true,
      totalUsers,
      activeToday,
      videosPostedToday,
      totalVideosEver,
      totalCostToday:     parseFloat(totalCostToday.toFixed(6)),
      totalCostThisMonth: parseFloat(totalCostThisMonth.toFixed(6)),
      perPlanBreakdown,
      quotaUsedToday,
      quotaRemaining:     Math.max(0, YOUTUBE_DAILY_LIMIT - quotaUsedToday),
      quotaByUser,
      nextResetAt,
    });
  } catch (err) {
    console.error('[Admin] Overview error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/api-usage — today's usage grouped by service, plus static infra entries
app.get('/api/admin/api-usage', requireAdmin, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const since    = today + 'T00:00:00.000Z';
    const usageCol = agentCol('apiUsage');
    const rows = await usageCol.aggregate([
      { $match: { timestamp: { $gte: since } } },
      { $group: {
        _id:          '$service',
        calls:        { $sum: 1 },
        tokensUsed:   { $sum: '$tokensUsed' },
        totalCost:    { $sum: '$estimatedCost' },
        successCount: { $sum: { $cond: ['$success', 1, 0] } },
        lastUsed:     { $max: '$timestamp' },
      }},
      { $sort: { totalCost: -1 } },
    ]).toArray();

    const result = rows.map(r => ({
      service:     r._id,
      calls:       r.calls,
      tokensUsed:  r.tokensUsed,
      totalCost:   parseFloat(r.totalCost.toFixed(6)),
      successRate: r.calls > 0 ? parseFloat((r.successCount / r.calls).toFixed(4)) : 0,
      lastUsed:    r.lastUsed || null,
      isStatic:    false,
    }));

    // Static infra services that don't log to apiUsage — shown for completeness
    const staticInfra = [
      { service: 'railway',  calls: null, tokensUsed: 0, totalCost: null, successRate: null, lastUsed: null, isStatic: true, note: 'Flat monthly — see Railway dashboard' },
      { service: 'mongodb',  calls: null, tokensUsed: 0, totalCost: null, successRate: null, lastUsed: null, isStatic: true, note: 'Atlas cluster — see Atlas billing' },
      { service: 'pexels',   calls: null, tokensUsed: 0, totalCost: null, successRate: null, lastUsed: null, isStatic: true, note: 'Free tier API — no cost' },
    ];

    res.json({ success: true, since, usage: [...result, ...staticInfra] });
  } catch (err) {
    console.error('[Admin] API usage error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/youtube-quota — daily quota from youtubeQuota collection with action breakdown
app.get('/api/admin/youtube-quota', requireAdmin, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const quotaCol = agentCol('youtubeQuota');

    const [totalDocs, byActionDocs] = await Promise.all([
      quotaCol.aggregate([
        { $match: { date: today } },
        { $group: { _id: null, used: { $sum: '$units' } } },
      ]).toArray(),
      quotaCol.aggregate([
        { $match: { date: today } },
        { $group: { _id: '$action', units: { $sum: '$units' }, calls: { $sum: 1 } } },
        { $sort: { units: -1 } },
      ]).toArray(),
    ]);

    const used        = totalDocs[0]?.used || 0;
    const limit       = YOUTUBE_DAILY_LIMIT;
    const hardLimit   = 10000;
    const remaining   = Math.max(0, limit - used);
    const percentUsed = parseFloat(((used / hardLimit) * 100).toFixed(2));
    const byAction    = byActionDocs.map(r => ({ action: r._id, units: r.units, calls: r.calls }));

    const now = new Date();
    const resetAt = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1)).toISOString();

    res.json({ success: true, used, limit, hardLimit, remaining, percentUsed, byAction, resetAt });
  } catch (err) {
    console.error('[Admin] YouTube quota error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/quota-audit — static call-site map + live quota usage + projected daily spend
app.get('/api/admin/quota-audit', requireAdmin, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const quotaCol = agentCol('youtubeQuota');

    const [totalDocs, byActionDocs] = await Promise.all([
      quotaCol.aggregate([
        { $match: { date: today } },
        { $group: { _id: null, used: { $sum: '$units' } } },
      ]).toArray(),
      quotaCol.aggregate([
        { $match: { date: today } },
        { $group: { _id: '$action', units: { $sum: '$units' }, calls: { $sum: 1 } } },
        { $sort: { units: -1 } },
      ]).toArray(),
    ]);

    const usedToday   = totalDocs[0]?.used || 0;
    const remaining   = Math.max(0, YOUTUBE_DAILY_LIMIT - usedToday);
    const activeUsers = await User.countDocuments({ 'youtubeChannels.0': { $exists: true } }).catch(() => 0);

    // Projected daily spend: one full pipeline per active user per day
    // = upload (1600) + thumbnails.set+videos.list (51) + analytics (10) + channels.list + videos.list for live analytics (2)
    const projectedPerUser = YOUTUBE_QUOTA_COSTS.upload + YOUTUBE_QUOTA_COSTS.thumbnails + YOUTUBE_QUOTA_COSTS.default + YOUTUBE_QUOTA_COSTS.analytics + 2;
    const projectedDaily   = projectedPerUser * Math.max(activeUsers, 1);

    res.json({
      success: true,
      summary: {
        totalCallSites:   QUOTA_AUDIT_MAP.length,
        guardedCallSites: QUOTA_AUDIT_MAP.filter(e => e.guarded).length,
        unguardedCallSites: QUOTA_AUDIT_MAP.filter(e => !e.guarded).length,
      },
      callSites: QUOTA_AUDIT_MAP,
      liveQuota: {
        usedToday,
        remaining,
        dailyLimit:  YOUTUBE_DAILY_LIMIT,
        hardLimit:   10000,
        percentUsed: parseFloat(((usedToday / 10000) * 100).toFixed(2)),
        byAction:    byActionDocs.map(r => ({ action: r._id, units: r.units, calls: r.calls })),
        resetsAt:    new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate() + 1)).toISOString(),
      },
      projection: {
        activeUsers,
        projectedPerUserPerDay: projectedPerUser,
        projectedDailyTotal:    projectedDaily,
        safeUserCapacity:       Math.floor(YOUTUBE_DAILY_LIMIT / projectedPerUser),
        status: projectedDaily <= YOUTUBE_DAILY_LIMIT ? 'ok' : 'over_capacity',
      },
    });
  } catch (err) {
    console.error('[Admin] quota-audit error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/admin/users — all real users sorted by videos posted desc
app.get('/api/admin/users', requireAdmin, async (req, res) => {
  try {
    const monthStart = new Date().toISOString().slice(0, 7) + '-01T00:00:00.000Z';
    const usageCol   = agentCol('apiUsage');
    const slotsCol   = agentCol('calendar_slots');

    // Exclude test/diagnostic accounts
    const [users, costByUser] = await Promise.all([
      User.find({ email: { $not: /test|diag|_test_|viralityy\.com$/i } })
        .select('email plan createdAt updatedAt youtubeChannels blocked blockedAt blockedReason subscriptionStatus subscriptionStartDate subscriptionEndDate subscriptionRenewsAt billingHistory trialEndsAt').lean(),
      usageCol.aggregate([
        { $match: { timestamp: { $gte: monthStart }, userId: { $ne: null } } },
        { $group: { _id: '$userId', cost: { $sum: '$estimatedCost' } } },
      ]).toArray(),
    ]);

    const costMap = Object.fromEntries(costByUser.map(c => [c._id, c.cost]));

    const userRows = await Promise.all(users.map(async u => {
      const videosPosted = await slotsCol.countDocuments({ userId: String(u._id), posted: true }).catch(() => 0);
      const subStatus  = u.subscriptionStatus || 'trial';
      const endDate    = u.subscriptionEndDate || u.trialEndsAt || null;
      const daysLeft   = endDate ? Math.ceil((new Date(endDate) - Date.now()) / 86400000) : null;
      const planCfg    = PLAN_CONFIG[u.plan] || PLAN_CONFIG.trial;
      const revenue    = (u.billingHistory || []).reduce((s, h) => s + (h.amount || 0), 0);
      return {
        id:                    String(u._id),
        email:                 u.email,
        plan:                  u.plan,
        planPrice:             planCfg.price,
        joinedAt:              u.createdAt || null,
        videosPosted,
        apiCostThisMonth:      parseFloat((costMap[String(u._id)] || 0).toFixed(6)),
        lastActive:            u.updatedAt || u.createdAt,
        channelsCount:         (u.youtubeChannels || []).length,
        blocked:               !!u.blocked,
        blockedReason:         u.blockedReason || null,
        subscriptionStatus:    subStatus,
        subscriptionStartDate: u.subscriptionStartDate || null,
        subscriptionEndDate:   endDate,
        daysRemaining:         daysLeft,
        totalRevenue:          parseFloat(revenue.toFixed(2)),
      };
    }));

    userRows.sort((a, b) => b.videosPosted - a.videosPosted);
    res.json({ success: true, count: userRows.length, users: userRows });
  } catch (err) {
    console.error('[Admin] Users error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/alerts — active threshold alerts
app.get('/api/admin/alerts', requireAdmin, async (req, res) => {
  try {
    const today     = new Date().toISOString().slice(0, 10);
    const usageCol  = agentCol('apiUsage');
    const alerts    = [];

    // Per-service cost today
    const costRows = await usageCol.aggregate([
      { $match: { timestamp: { $gte: today + 'T00:00:00.000Z' } } },
      { $group: { _id: '$service', totalCost: { $sum: '$estimatedCost' }, calls: { $sum: 1 }, successCount: { $sum: { $cond: ['$success', 1, 0] } } } },
    ]).toArray();

    for (const row of costRows) {
      if (row.totalCost > 10) {
        alerts.push({ type: 'cost', severity: 'high', service: row._id,
          message: `${row._id} cost today: $${row.totalCost.toFixed(4)} (threshold: $10)` });
      }
      const failRate = row.calls > 0 ? (row.calls - row.successCount) / row.calls : 0;
      if (failRate > 0.1) {
        alerts.push({ type: 'failure_rate', severity: 'medium', service: row._id,
          message: `${row._id} failure rate: ${(failRate * 100).toFixed(1)}% (threshold: 10%)` });
      }
    }

    // YouTube quota — read from dedicated youtubeQuota collection
    const quotaCol = agentCol('youtubeQuota');
    const ytUsed = await quotaCol.aggregate([
      { $match: { date: today } },
      { $group: { _id: null, total: { $sum: '$units' } } },
    ]).toArray().then(r => r[0]?.total || 0);

    const quotaPct = (ytUsed / 10000) * 100;
    if (quotaPct > 80) {
      alerts.push({ type: 'quota', severity: 'high', service: 'youtube',
        message: `YouTube quota at ${quotaPct.toFixed(1)}% (${ytUsed}/10000 units)` });
    }

    // 3-consecutive-days high-quota alert
    const threeDayTotals = await Promise.all([1, 2, 3].map(async daysAgo => {
      const d = new Date();
      d.setUTCDate(d.getUTCDate() - daysAgo);
      const dateStr = d.toISOString().slice(0, 10);
      return quotaCol.aggregate([
        { $match: { date: dateStr } },
        { $group: { _id: null, total: { $sum: '$units' } } },
      ]).toArray().then(r => r[0]?.total || 0);
    }));
    if (threeDayTotals.every(t => t > 8000)) {
      alerts.push({
        type: 'quota_sustained', severity: 'high', service: 'youtube',
        message: 'Consider requesting YouTube API quota increase at console.cloud.google.com — quota exceeded 8000 units for 3 consecutive days',
      });
      agentCol('adminAlerts').updateOne(
        { type: 'quota_sustained' },
        { $set: { type: 'quota_sustained', message: 'YouTube quota >8000 for 3 consecutive days — request increase', createdAt: new Date(), resolved: false } },
        { upsert: true }
      ).catch(() => {});
    }

    res.json({ success: true, alertCount: alerts.length, alerts });
  } catch (err) {
    console.error('[Admin] Alerts error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/pipeline-stats — today's pipeline performance + real-time status
app.get('/api/admin/pipeline-stats', requireAdmin, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const todayStart   = today + 'T00:00:00.000Z';
    const tomorrowStart = new Date(new Date(todayStart).getTime() + 86400000).toISOString();
    const nowIso   = new Date().toISOString();
    const slotsCol = agentCol('calendar_slots');

    pipelineEnsureTodayStats();

    const [videosPostedToday, failedSlots, nextSlotDoc] = await Promise.all([
      // Count posts by postedAt timestamp for accuracy
      slotsCol.countDocuments({ posted: true, postedAt: { $gte: todayStart, $lt: tomorrowStart } }),
      slotsCol.countDocuments({ date: today, pipelineStatus: 'failed' }),
      // Next upcoming slot not yet posted or rescheduled
      slotsCol.findOne(
        { posted: false, status: { $in: ['scheduled', 'pending'] }, scheduledFor: { $gt: nowIso } },
        { sort: { scheduledFor: 1 }, projection: { title: 1, scheduledFor: 1, date: 1 } }
      ),
    ]);

    const ps = PIPELINE_STATUS;
    const totalAttempted = ps.todayStats.posted + ps.todayStats.failed;
    const successRate    = totalAttempted > 0
      ? parseFloat((ps.todayStats.posted / totalAttempted).toFixed(4)) : null;

    res.json({
      success: true,
      videosGeneratedToday: ps.todayStats.generated,
      videosPostedToday,
      thumbnailsGeneratedToday: ps.todayStats.thumbnails,
      failureCount:       failedSlots,
      successRate,
      currentlyProcessing: ps.currentlyProcessing,
      lastCompletedAt:    ps.lastCompletedAt,
      lastError:          ps.lastError,
      nextScheduledSlot:  nextSlotDoc ? { title: nextSlotDoc.title, scheduledFor: nextSlotDoc.scheduledFor, date: nextSlotDoc.date } : null,
    });
  } catch (err) {
    console.error('[Admin] Pipeline stats error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/channel-timings — per-channel optimal posting times for admin visibility
app.get('/api/admin/channel-timings', requireAdmin, async (req, res) => {
  try {
    const users = await User.find({ 'youtubeChannels.0': { $exists: true } })
      .select('email plan youtubeChannels').lean();
    const rows = [];
    for (const u of users) {
      for (const ch of (u.youtubeChannels || [])) {
        rows.push({
          email:             u.email,
          plan:              u.plan,
          channelId:         ch.channelId,
          channelName:       ch.channelName,
          optimalTimes:      ch.optimalPostingTimes || [],
          source:            ch.optimalTimesSource  || 'not_calculated',
          calculatedAt:      ch.optimalTimesCalculatedAt || null,
        });
      }
    }
    res.json({ success: true, channels: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/admin/users/:userId/block — suspend a user account
app.post('/api/admin/users/:userId/block', requireAdmin, async (req, res) => {
  try {
    const { userId } = req.params;
    const { reason } = req.body || {};
    const adminEmail = process.env.ADMIN_EMAIL;

    const user = await User.findById(userId).select('email sessionVersion').lean();
    if (!user) return res.status(404).json({ error: 'User not found' });
    if (user.email === adminEmail) return res.status(400).json({ error: 'Cannot block the admin account' });

    const newVersion = (user.sessionVersion || 0) + 1;
    await User.findByIdAndUpdate(userId, {
      blocked:        true,
      blockedAt:      new Date(),
      blockedReason:  reason || 'Blocked by admin',
      sessionVersion: newVersion,
    });

    agentCol('adminAlerts').insertOne({
      type:        'user_blocked',
      userId:      String(userId),
      userEmail:   user.email,
      reason:      reason || 'Blocked by admin',
      adminAction: true,
      timestamp:   new Date().toISOString(),
    }).catch(() => {});

    console.log(`[Admin] Blocked user ${user.email} (reason: ${reason || 'none'})`);
    res.json({ success: true, message: 'User blocked' });
  } catch (err) {
    console.error('[Admin] Block error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// POST /api/admin/users/:userId/unblock — restore a suspended user account
app.post('/api/admin/users/:userId/unblock', requireAdmin, async (req, res) => {
  try {
    const { userId } = req.params;
    const user = await User.findById(userId).select('email sessionVersion').lean();
    if (!user) return res.status(404).json({ error: 'User not found' });

    // Bump sessionVersion so re-issued tokens after unblock are fresh
    await User.findByIdAndUpdate(userId, {
      blocked:        false,
      unblockedAt:    new Date(),
      sessionVersion: (user.sessionVersion || 0) + 1,
    });

    console.log(`[Admin] Unblocked user ${user.email}`);
    res.json({ success: true, message: 'User unblocked' });
  } catch (err) {
    console.error('[Admin] Unblock error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// DELETE /api/admin/users/:userId — permanently remove a user and all their data
app.delete('/api/admin/users/:userId', requireAdmin, async (req, res) => {
  try {
    const { userId } = req.params;
    const adminEmail = process.env.ADMIN_EMAIL;

    const user = await User.findById(userId).select('email').lean();
    if (!user) return res.status(404).json({ error: 'User not found' });
    if (user.email === adminEmail) return res.status(400).json({ error: 'Cannot delete the admin account' });

    const userIdStr = String(userId);
    const [slotsRes, usageRes] = await Promise.all([
      agentCol('calendar_slots').deleteMany({ userId: userIdStr }),
      agentCol('apiUsage').deleteMany({ userId: userIdStr }),
    ]);

    await User.findByIdAndDelete(userId);

    console.log(`[Admin] Deleted user ${user.email} — slots: ${slotsRes.deletedCount}, usage: ${usageRes.deletedCount}`);
    res.json({ success: true, message: 'User and all data removed' });
  } catch (err) {
    console.error('[Admin] Delete user error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// POST /api/admin/users/:userId/subscription — admin override subscription state
// Body: { action: 'set_plan'|'reset_trial'|'expire', plan?: string }
app.post('/api/admin/users/:userId/subscription', requireAdmin, async (req, res) => {
  try {
    const { userId } = req.params;
    const { action, plan } = req.body || {};
    const user = await User.findById(userId).select('email plan subscriptionStatus').lean();
    if (!user) return res.status(404).json({ error: 'User not found' });

    let update = {};
    let logMsg = '';

    if (action === 'set_plan') {
      if (!PLAN_LIMITS[plan]) return res.status(400).json({ error: `Unknown plan: ${plan}` });
      const limits = getPlanLimits(plan);
      const endDate = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);
      update = {
        plan,
        subscriptionStatus:     'active',
        subscriptionStartDate:  new Date(),
        subscriptionEndDate:    endDate,
        subscriptionRenewsAt:   endDate,
        videosPerDayPerChannel: limits.videosPerDayPerChannel,
        maxChannels:            limits.maxChannels,
        longformEnabled:        limits.longformEnabled,
      };
      logMsg = `[Admin] Set ${user.email} → plan:${plan} status:active`;

    } else if (action === 'reset_trial') {
      const trialEnd = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000);
      update = {
        plan:                   'trial',
        subscriptionStatus:     'trial',
        trialStartedAt:         new Date(),
        trialEndsAt:            trialEnd,
        subscriptionStartDate:  null,
        subscriptionEndDate:    null,
        subscriptionRenewsAt:   null,
        paddleSubscriptionId:   null,
        paddleCustomerId:       null,
        videosPerDayPerChannel: 3,
        maxChannels:            1,
        longformEnabled:        false,
      };
      logMsg = `[Admin] Reset ${user.email} → fresh trial (until ${trialEnd.toISOString().slice(0,10)})`;

    } else if (action === 'expire') {
      update = { subscriptionStatus: 'expired' };
      logMsg = `[Admin] Expired ${user.email} immediately`;

    } else {
      return res.status(400).json({ error: 'action must be set_plan | reset_trial | expire' });
    }

    await User.findByIdAndUpdate(userId, update);
    console.log(logMsg);
    const updated = await User.findById(userId).select('email plan subscriptionStatus subscriptionEndDate trialEndsAt videosPerDayPerChannel maxChannels longformEnabled').lean();
    res.json({ success: true, user: updated });
  } catch (err) {
    console.error('[Admin] Subscription override error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// POST /api/admin/activate-account — directly set any user to any plan by email (admin JWT required)
app.post('/api/admin/activate-account', requireAdmin, async (req, res) => {
  try {
    const { email, plan } = req.body || {};
    if (!email) return res.status(400).json({ success: false, error: 'email is required' });
    if (!plan || !PLAN_LIMITS[plan]) {
      return res.status(400).json({ success: false, error: `Invalid plan "${plan}". Valid plans: ${Object.keys(PLAN_LIMITS).join(', ')}` });
    }
    const user = await User.findOne({ email: email.toLowerCase().trim() }).select('_id email plan subscriptionStatus').lean();
    if (!user) return res.status(404).json({ success: false, error: `No user found with email: ${email}` });
    const limits  = getPlanLimits(plan);
    const endDate = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);
    await User.findByIdAndUpdate(user._id, {
      plan,
      subscriptionStatus:     'active',
      subscriptionStartDate:  new Date(),
      subscriptionEndDate:    endDate,
      subscriptionRenewsAt:   endDate,
      videosPerDayPerChannel: limits.videosPerDayPerChannel,
      maxChannels:            limits.maxChannels,
      longformEnabled:        limits.longformEnabled,
    });
    console.log(`[Admin] ${req.adminUser?.email || 'admin'} activated ${email} → plan:${plan} (expires ${endDate.toISOString().slice(0,10)})`);
    const updated = await User.findById(user._id)
      .select('email plan subscriptionStatus subscriptionEndDate videosPerDayPerChannel maxChannels longformEnabled').lean();
    res.json({ success: true, user: updated });
  } catch (err) {
    console.error('[Admin] activate-account error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/admin/force-activate — emergency agency activation by email.
// Accepts ADMIN_PASSWORD directly in body (no pre-existing user JWT needed).
// Bypasses all plan/promo logic; always sets agency limits directly in MongoDB.
app.post('/api/admin/force-activate', async (req, res) => {
  try {
    const { email, secret } = req.body || {};
    const adminPass = process.env.ADMIN_PASSWORD;
    if (!adminPass || secret !== adminPass) return res.status(403).json({ success: false, error: 'Access denied' });
    if (!email) return res.status(400).json({ success: false, error: 'email is required' });
    const endDate = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);
    const result  = await User.findOneAndUpdate(
      { email: email.toLowerCase().trim() },
      {
        plan:                   'agency',
        subscriptionStatus:     'active',
        subscriptionStartDate:  new Date(),
        subscriptionEndDate:    endDate,
        subscriptionRenewsAt:   endDate,
        videosPerDayPerChannel: 10,
        maxChannels:            4,
        longformEnabled:        true,
      },
      { new: true }
    ).select('email plan subscriptionStatus subscriptionEndDate videosPerDayPerChannel maxChannels longformEnabled');
    if (!result) return res.status(404).json({ success: false, error: `No user found with email: ${email}` });
    console.log(`[Admin] force-activate: ${result.email} → agency (expires ${endDate.toISOString().slice(0,10)})`);
    res.json({ success: true, user: result.email, plan: 'agency', subscriptionStatus: 'active', subscriptionEndDate: endDate });
  } catch (err) {
    console.error('[Admin] force-activate error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/admin/run-pipeline — manually trigger daily generation + scheduled posting (admin only)
app.post('/api/admin/run-pipeline', requireAdmin, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const slotsCol = agentCol('calendar_slots');
    console.log('[Admin] Manual pipeline trigger started');

    const beforeGen    = await slotsCol.countDocuments({ date: today }).catch(() => 0);
    const beforePosted = await slotsCol.countDocuments({ date: today, status: 'posted' }).catch(() => 0);

    await runDailySlotGeneration();
    await runScheduledPosting();

    const afterGen    = await slotsCol.countDocuments({ date: today }).catch(() => 0);
    const afterPosted = await slotsCol.countDocuments({ date: today, status: 'posted' }).catch(() => 0);

    const slotsGenerated = Math.max(0, afterGen - beforeGen);
    const slotsPosted    = Math.max(0, afterPosted - beforePosted);

    console.log(`[Admin] Pipeline trigger complete — generated: ${slotsGenerated}, posted: ${slotsPosted}`);
    res.json({ success: true, message: 'Pipeline triggered', slotsGenerated, slotsPosted });
  } catch (err) {
    console.error('[Admin] run-pipeline error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/admin/generate-slots-now — manually trigger daily slot generation for today (or a given date)
// Accepts optional body: { date: "YYYY-MM-DD", userId: "..." }
app.post('/api/admin/generate-slots-now', requireAdmin, async (req, res) => {
  try {
    const { date: targetDate, userId: targetUserId } = req.body || {};
    const today = targetDate || new Date().toISOString().slice(0, 10);
    console.log(`[Admin] generate-slots-now triggered — date=${today}, userId=${targetUserId || 'all'}`);

    const slotsCol = agentCol('calendar_slots');
    const beforeCount = await slotsCol.countDocuments({ date: today }).catch(() => 0);

    const result = await runDailySlotGeneration(targetUserId || null, today);

    const afterCount = await slotsCol.countDocuments({ date: today }).catch(() => 0);
    const slotsCreated = Math.max(0, afterCount - beforeCount);

    console.log(`[Admin] generate-slots-now complete — created ${slotsCreated} slot(s) for ${today}`);
    res.json({
      success: true,
      date: today,
      usersProcessed: result.total,
      usersWithNewSlots: result.generated,
      slotsCreated,
    });
  } catch (err) {
    console.error('[Admin] generate-slots-now error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/admin/test-imagen — call Imagen 4 and return raw response for debugging
app.get('/api/admin/test-imagen', requireAuth, async (req, res) => {
  const prompt = req.query.prompt || 'A vibrant blue YouTube thumbnail with bold white text TEST';
  try {
    const result = await callImagenAPI(prompt, '16:9');
    res.json({
      success: true,
      mimeType: result.mimeType,
      base64Length: result.imageBytes.length,
      httpStatus: result.httpStatus,
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET /api/admin/test-thumbnail-upload?videoId=REAL_YOUTUBE_VIDEO_ID
// Generates a thumbnail via Imagen 4, uploads it to a real YouTube video, returns full success/error detail.
app.get('/api/admin/test-thumbnail-upload', requireAdmin, async (req, res) => {
  const { videoId } = req.query;
  if (!videoId) return res.status(400).json({ error: 'videoId query param is required (a real YouTube video ID, e.g. dQw4w9WgXcQ)' });

  const fs = require('fs');
  const { google } = require('googleapis');

  try {
    const adminEmail = process.env.ADMIN_EMAIL;
    if (!adminEmail) return res.status(503).json({ error: 'ADMIN_EMAIL env var not set' });
    const user = await User.findOne({ email: adminEmail });
    if (!user) return res.status(404).json({ error: `No user found for ADMIN_EMAIL ${adminEmail}` });

    const channel = (user.youtubeChannels || []).find(ch => /all.*everything/i.test(ch.channelName || ''))
                 || user.youtubeChannels?.[0];
    if (!channel) return res.status(404).json({ error: 'No YouTube channel found for admin user' });

    console.log(`[TestThumb] Channel: "${channel.channelName}" (${channel.channelId}), thumbnailsEnabled=${channel.thumbnailsEnabled}`);
    console.log(`[TestThumb] Target YouTube videoId: ${videoId}`);

    // Step 1: Generate thumbnail via Imagen 4
    const thumbPath = await generateThumbnail('Amazing Facts You Never Knew', 'General Knowledge', 'Short', `testthumb_${Date.now()}`);
    if (!thumbPath) {
      return res.status(500).json({ error: 'Imagen 4 failed to generate thumbnail — see server logs for details' });
    }
    const thumbStat = fs.statSync(thumbPath);
    const thumbMime = thumbPath.endsWith('.png') ? 'image/png' : 'image/jpeg';
    console.log(`[TestThumb] Generated: ${thumbPath}, size=${thumbStat.size} bytes (${(thumbStat.size/1024/1024).toFixed(2)} MB), mime=${thumbMime}`);

    // Step 2: Upload via thumbnails.set — try with current token, refresh once on 401
    const testThumbQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.thumbnails, String(user._id)).catch(() => ({ allowed: true }));
    if (!testThumbQC.allowed) {
      fs.unlink(thumbPath, () => {});
      return res.status(429).json({ error: `YouTube quota insufficient for thumbnail test: ${testThumbQC.reason}`, quotaLimited: true });
    }

    const oauth2 = new google.auth.OAuth2(process.env.YOUTUBE_CLIENT_ID, process.env.YOUTUBE_CLIENT_SECRET);
    let accessToken = channel.accessToken;

    const doSet = async (token) => {
      oauth2.setCredentials({ access_token: token, refresh_token: channel.refreshToken });
      const yt = google.youtube({ version: 'v3', auth: oauth2 });
      console.log(`[TestThumb] Calling thumbnails.set — videoId=${videoId}, mime=${thumbMime}, fileSize=${thumbStat.size}`);
      return yt.thumbnails.set({
        videoId,
        media: { mimeType: thumbMime, body: fs.createReadStream(thumbPath) },
      });
    };

    let uploadResult = null;
    let uploadError  = null;
    try {
      let setRes;
      try {
        setRes = await doSet(accessToken);
      } catch (e) {
        if (e.code === 401 || e.status === 401 || /invalid_grant|token/i.test(e.message)) {
          console.log(`[TestThumb] 401 on thumbnails.set — refreshing token and retrying`);
          accessToken = await pipelineRefreshToken(channel);
          setRes = await doSet(accessToken);
        } else {
          throw e;
        }
      }
      uploadResult = { httpStatus: setRes?.status, data: setRes?.data };
      console.log(`[TestThumb] thumbnails.set SUCCESS — HTTP ${setRes?.status}:`, JSON.stringify(setRes?.data || {}).slice(0, 600));
      logYoutubeQuota('thumbnails.set', YOUTUBE_QUOTA_COSTS.thumbnails, String(user._id)).catch(() => {});

      // Confirm channel is thumbnail-eligible
      await User.findOneAndUpdate(
        { _id: user._id, 'youtubeChannels.channelId': channel.channelId },
        { $set: { 'youtubeChannels.$.thumbnailsEnabled': true } }
      );
      console.log(`[TestThumb] ✓ Marked ${channel.channelId} thumbnailsEnabled=true in MongoDB`);
    } catch (e) {
      uploadError = { message: e.message, code: e.code || e.status || null, responseData: e?.response?.data || null };
      console.error(`[TestThumb] thumbnails.set FAILED:`, JSON.stringify(uploadError));

      const reason = e?.response?.data?.error?.errors?.[0]?.reason || '';
      const isThumbnailDisabled = reason === 'forbidden' || /thumbnail.*not.*enabl|custom.*thumbnail/i.test(e.message);
      if (isThumbnailDisabled) {
        console.warn(`[TestThumb] Channel not eligible for custom thumbnails — enable it in YouTube Studio first`);
      }
    } finally {
      fs.unlink(thumbPath, () => {});
    }

    res.json({
      success:      !uploadError,
      videoId,
      channel:      { channelId: channel.channelId, channelName: channel.channelName, thumbnailsEnabled: channel.thumbnailsEnabled },
      thumbnail:    { path: thumbPath, sizeMB: (thumbStat.size / 1024 / 1024).toFixed(2), mime: thumbMime },
      uploadResult: uploadResult || null,
      uploadError:  uploadError  || null,
      hint:         uploadError ? 'Check server logs for full error detail. Common causes: channel not eligible for custom thumbnails (enable in YouTube Studio), file > 2 MB, or token expired.' : null,
    });
  } catch (err) {
    console.error('[TestThumb] Fatal error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/admin/retry-thumbnails
// Finds the last 5 posted slots with no thumbnail or thumbnailStatus "failed", regenerates
// a thumbnail via Imagen 4, and calls thumbnails.set() with 3-attempt exponential backoff.
app.get('/api/admin/retry-thumbnails', requireAdmin, async (req, res) => {
  const fs       = require('fs');
  const { google } = require('googleapis');
  const slotsCol = agentCol('calendar_slots');
  const limit    = parseInt(req.query.limit || '5', 10);

  try {
    const slots = await slotsCol
      .find({
        posted: true,
        youtubeVideoId: { $exists: true, $ne: null },
        $or: [{ thumbnailUploaded: { $ne: true } }, { thumbnailStatus: 'failed' }],
      })
      .sort({ postedAt: -1 })
      .limit(limit)
      .toArray();

    console.log(`[RetryThumb] Found ${slots.length} slot(s) needing thumbnail retry`);
    const results = [];

    for (const slot of slots) {
      const slotResult = {
        slotId:         String(slot._id),
        youtubeVideoId: slot.youtubeVideoId,
        title:          slot.title,
        postedAt:       slot.postedAt,
        thumbGenerated: false,
        thumbUploaded:  false,
        error:          null,
      };

      try {
        const user = await User.findById(slot.userId).lean();
        if (!user) { slotResult.error = 'User not found'; results.push(slotResult); continue; }

        const channel = (user.youtubeChannels || []).find(ch => ch.channelId === slot.channelId)
                     || user.youtubeChannels?.[0];
        if (!channel) { slotResult.error = 'No YouTube channel found for user'; results.push(slotResult); continue; }

        if (channel.thumbnailsEnabled === false) {
          slotResult.error = 'Channel thumbnailsEnabled=false — skipped to avoid API waste';
          results.push(slotResult);
          continue;
        }

        console.log(`[RetryThumb] Generating thumbnail for "${slot.title}" (videoId=${slot.youtubeVideoId})`);
        const thumbPath = await generateThumbnail(
          slot.title, slot.nicheName || '', slot.type || 'Short', `retry_${String(slot._id)}`
        );
        if (!thumbPath) { slotResult.error = 'Imagen 4 thumbnail generation failed'; results.push(slotResult); continue; }
        slotResult.thumbGenerated = true;

        const thumbStat = fs.statSync(thumbPath);
        const thumbMime = thumbPath.endsWith('.png') ? 'image/png' : 'image/jpeg';
        console.log(`[RetryThumb] Generated: ${thumbPath} | ${thumbStat.size} bytes | ${thumbMime}`);

        // Quota guard: 1 (videos.list) + 50 (thumbnails.set) per attempt
        const retryQC = await checkYoutubeQuota(YOUTUBE_QUOTA_COSTS.thumbnails + YOUTUBE_QUOTA_COSTS.default, String(slot.userId)).catch(() => ({ allowed: true }));
        if (!retryQC.allowed) {
          console.warn(`[Quota] retry-thumbnails blocked for slot ${slot._id}: ${retryQC.reason}`);
          slotResult.error = `quota_limited: ${retryQC.reason}`;
          results.push(slotResult);
          fs.unlink(thumbPath, () => {});
          continue;
        }

        const oauth2 = new google.auth.OAuth2(process.env.YOUTUBE_CLIENT_ID, process.env.YOUTUBE_CLIENT_SECRET);
        let accessToken = channel.accessToken;

        const doSet = async (token, attempt) => {
          oauth2.setCredentials({ access_token: token, refresh_token: channel.refreshToken });
          const yt = google.youtube({ version: 'v3', auth: oauth2 });

          const statusRes = await yt.videos.list({ part: ['status'], id: [slot.youtubeVideoId] }).catch(() => null);
          logYoutubeQuota('videos.list', YOUTUBE_QUOTA_COSTS.default, String(slot.userId)).catch(() => {});
          const uploadStatus = statusRes?.data?.items?.[0]?.status?.uploadStatus;
          console.log(`[RetryThumb] attempt ${attempt} — video uploadStatus=${uploadStatus || 'unknown'}`);
          if (uploadStatus && uploadStatus !== 'processed' && uploadStatus !== 'uploaded') {
            throw new Error(`Video not ready — uploadStatus=${uploadStatus}`);
          }

          return yt.thumbnails.set({
            videoId: slot.youtubeVideoId,
            media:   { mimeType: thumbMime, body: fs.createReadStream(thumbPath) },
          });
        };

        const backoffDelays = [30000, 60000, 120000];
        let uploaded = false;
        let lastErr;
        for (let attempt = 1; attempt <= 3; attempt++) {
          const waitMs = backoffDelays[attempt - 1];
          console.log(`[RetryThumb] Waiting ${waitMs / 1000}s before attempt ${attempt}…`);
          await new Promise(r => setTimeout(r, waitMs));
          try {
            console.log(`[RetryThumb] thumbnails.set attempt ${attempt} — videoId=${slot.youtubeVideoId}`);
            let setRes;
            try {
              setRes = await doSet(accessToken, attempt);
            } catch (e401) {
              if ((e401.code === 401 || e401.status === 401 || /invalid_grant|token/i.test(e401.message)) && attempt === 1) {
                console.log(`[RetryThumb] 401 — refreshing token`);
                accessToken = await pipelineRefreshToken(channel);
                setRes = await doSet(accessToken, attempt);
              } else {
                throw e401;
              }
            }
            console.log(`[RetryThumb] thumbnails.set SUCCESS — HTTP ${setRes?.status}:`, JSON.stringify(setRes?.data || {}).slice(0, 400));
            logYoutubeQuota('thumbnails.set', YOUTUBE_QUOTA_COSTS.thumbnails, String(slot.userId)).catch(() => {});
            await slotsCol.updateOne(
              { _id: slot._id },
              { $set: { thumbnailUploaded: true, thumbnailStatus: 'success', thumbnailAppliedAt: new Date() } }
            );
            slotResult.thumbUploaded = true;
            uploaded = true;
            break;
          } catch (e) {
            lastErr = e;
            const fullErr = e?.response?.data || e?.message;
            console.error(`[RetryThumb] attempt ${attempt} FAILED — videoId=${slot.youtubeVideoId}:`, JSON.stringify(fullErr));
            slotResult.error = JSON.stringify(fullErr);
          }
        }

        fs.unlink(thumbPath, () => {});
        if (!uploaded) {
          const errMsg = lastErr?.response?.data ? JSON.stringify(lastErr.response.data) : (lastErr?.message || slotResult.error || 'unknown');
          await slotsCol.updateOne({ _id: slot._id }, { $set: { thumbnailStatus: 'failed', thumbnailError: errMsg } });
          if (channel.channelId && /forbidden|thumbnail.*not.*enabl|custom.*thumbnail/i.test(errMsg)) {
            User.findOneAndUpdate(
              { _id: user._id, 'youtubeChannels.channelId': channel.channelId },
              { $set: { 'youtubeChannels.$.thumbnailsEnabled': false } }
            ).catch(() => {});
            console.log(`[RetryThumb] Marked ${channel.channelId} thumbnailsEnabled=false`);
          }
        }
      } catch (err) {
        console.error(`[RetryThumb] Fatal error for slot ${slot._id}:`, err.message);
        slotResult.error = err.message;
      }

      results.push(slotResult);
    }

    res.json({ success: true, processed: results.length, results });
  } catch (err) {
    console.error('[RetryThumb] Fatal:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/admin/thumbnail-status
// Returns the last 10 slots with thumbnailStatus, thumbnailError, and videoId for diagnosis.
app.get('/api/admin/thumbnail-status', requireAdmin, async (req, res) => {
  try {
    const slots = await agentCol('calendar_slots')
      .find({ posted: true, youtubeVideoId: { $exists: true, $ne: null } })
      .sort({ postedAt: -1 })
      .limit(10)
      .project({ _id: 1, title: 1, youtubeVideoId: 1, thumbnailStatus: 1, thumbnailError: 1, thumbnailAppliedAt: 1, thumbnailUploaded: 1, postedAt: 1 })
      .toArray();

    res.json({ success: true, count: slots.length, slots });
  } catch (err) {
    console.error('[ThumbnailStatus] Fatal:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/admin/channels/:channelId/thumbnail-enabled — force-set thumbnailsEnabled for a channel
app.post('/api/admin/channels/:channelId/thumbnail-enabled', requireAdmin, async (req, res) => {
  const { channelId } = req.params;
  const enabled = req.body?.enabled !== false; // default true
  try {
    const result = await User.findOneAndUpdate(
      { 'youtubeChannels.channelId': channelId },
      { $set: { 'youtubeChannels.$.thumbnailsEnabled': enabled } },
      { new: true }
    );
    if (!result) return res.status(404).json({ error: `No user found with channelId ${channelId}` });
    const ch = (result.youtubeChannels || []).find(c => c.channelId === channelId);
    res.json({ success: true, channelId, channelName: ch?.channelName, thumbnailsEnabled: enabled });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/admin/slots-report — last 7 days of slots grouped by status with failure/skip reasons
// Access: requireAdmin (?secret=VRL-ADM-X7K9-2025 bypass works in browser)
app.get('/api/admin/slots-report', requireAdmin, async (req, res) => {
  try {
    const slotsCol = agentCol('calendar_slots');
    const cutoff   = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

    const allSlots = await slotsCol
      .find({ date: { $gte: cutoff } })
      .sort({ date: -1, scheduledPostTime: 1 })
      .project({ _id: 1, title: 1, channelId: 1, userId: 1, date: 1, status: 1, postedAt: 1, failedAt: 1, skippedAt: 1, failureReason: 1, skipReason: 1, pipelineError: 1, pipelineStatus: 1 })
      .toArray();

    // Group by status
    const byStatus = { posted: [], failed: [], skipped: [], pending: [], scheduled: [], processing: [], other: [] };
    for (const s of allSlots) {
      const bucket = byStatus[s.status] || byStatus.other;
      bucket.push({
        slotId:        String(s._id),
        title:         s.title || null,
        channelId:     s.channelId || null,
        userId:        String(s.userId || ''),
        date:          s.date,
        ...(s.status === 'failed'  && { step: s.failureReason?.step || 'unknown', error: s.failureReason?.error || s.pipelineError || 'unknown', stack: s.failureReason?.stack || null, failedAt: s.failedAt }),
        ...(s.status === 'skipped' && { reason: s.skipReason || s.pipelineStatus || 'unknown', skippedAt: s.skippedAt }),
        ...(s.status === 'posted'  && { postedAt: s.postedAt }),
      });
    }

    // Summary counts per day
    const dayCounts = {};
    for (const s of allSlots) {
      if (!dayCounts[s.date]) dayCounts[s.date] = { posted: 0, failed: 0, skipped: 0, pending: 0, scheduled: 0, processing: 0, other: 0 };
      dayCounts[s.date][s.status] = (dayCounts[s.date][s.status] || 0) + 1;
    }

    res.json({
      success: true,
      periodDays: 7,
      cutoffDate: cutoff,
      totalSlots: allSlots.length,
      summary: {
        posted:     byStatus.posted.length,
        failed:     byStatus.failed.length,
        skipped:    byStatus.skipped.length,
        pending:    byStatus.pending.length,
        scheduled:  byStatus.scheduled.length,
        processing: byStatus.processing.length,
        other:      byStatus.other.length,
      },
      byDay: Object.entries(dayCounts)
        .sort(([a], [b]) => b.localeCompare(a))
        .map(([date, counts]) => ({ date, ...counts })),
      slots: byStatus,
    });
  } catch (err) {
    console.error('[Admin] slots-report error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/content/preview/:slotId — stream assembled video preview from /tmp
// slotId format: "day_videoIndex" e.g. "3_2"
app.get('/api/content/preview/:slotId', requireAuth, async (req, res) => {
  const [day, vi] = req.params.slotId.split('_').map(Number);
  if (!day) return res.status(400).json({ error: 'Invalid slotId — expected day_videoIndex' });
  const previewPath = `/tmp/vly_prev_${req.user.id}_${day}_${vi || 1}.mp4`;
  const fs = require('fs');
  if (!fs.existsSync(previewPath)) {
    return res.status(404).json({ error: 'Preview not ready — video must be generated first via the pipeline' });
  }
  const stat  = fs.statSync(previewPath);
  const range = req.headers.range;
  if (range) {
    const [startStr, endStr] = range.replace(/bytes=/, '').split('-');
    const start = parseInt(startStr, 10);
    const end   = endStr ? parseInt(endStr, 10) : stat.size - 1;
    res.writeHead(206, {
      'Content-Range':  `bytes ${start}-${end}/${stat.size}`,
      'Accept-Ranges':  'bytes',
      'Content-Length': end - start + 1,
      'Content-Type':   'video/mp4',
    });
    fs.createReadStream(previewPath, { start, end }).pipe(res);
  } else {
    res.writeHead(200, { 'Content-Type': 'video/mp4', 'Content-Length': stat.size, 'Accept-Ranges': 'bytes' });
    fs.createReadStream(previewPath).pipe(res);
  }
});

// POST /api/content/post-now/:slotId — immediately post a specific slot, bypassing scheduled time.
// If the slot is already "ready", uploads straight away. Otherwise runs the full pipeline first.
app.post('/api/content/post-now/:slotId', requireAuth, async (req, res) => {
  try {
    const [dayStr, viStr] = req.params.slotId.split('_');
    const day = parseInt(dayStr), vi = parseInt(viStr) || 1;
    if (!day) return res.status(400).json({ error: 'Invalid slotId — expected day_videoIndex' });

    const col  = agentCol('content_calendars');
    const cal  = await col.findOne({ userId: req.user.id, status: 'active' });
    if (!cal) return res.status(404).json({ error: 'No active calendar found' });
    const slot = (cal.slots || []).find(s => s.day === day && (s.videoIndex || 1) === vi);
    if (!slot) return res.status(404).json({ error: `Slot day=${day} videoIndex=${vi} not found` });
    if (slot.posted) return res.status(400).json({ error: 'Slot already posted' });

    res.json({ success: true, message: `"${slot.title}" queued for immediate posting — check back shortly` });

    setImmediate(async () => {
      try {
        const fs   = require('fs');
        const user = await User.findById(req.user.id);
        if (!user) return;
        const channel = (user.youtubeChannels || []).find(ch => ch.channelId === cal.channelId)
                     || user.youtubeChannels?.[0];
        if (!channel) { console.warn(`[PostNow] No channel for user ${req.user.id}`); return; }

        let targetCal  = await col.findOne({ _id: cal._id });
        let targetSlot = (targetCal.slots || []).find(s => s.day === day && (s.videoIndex || 1) === vi);

        // Run pipeline if not already ready
        if (targetSlot.status !== 'ready' || !targetSlot.assembledPath || !fs.existsSync(targetSlot.assembledPath)) {
          await runProductionPipelineWithRetry(targetCal, targetSlot, user, col);
          targetCal  = await col.findOne({ _id: cal._id });
          targetSlot = (targetCal.slots || []).find(s => s.day === day && (s.videoIndex || 1) === vi);
        }
        if (targetSlot.status !== 'ready' || !targetSlot.assembledPath) {
          console.error(`[PostNow] Pipeline did not produce a ready video for "${slot.title}"`); return;
        }

        const description = targetSlot.cachedScript || targetSlot.title;
        const ytId = await pipelineUploadToYouTube(targetSlot.assembledPath, targetSlot.title, description, channel, targetSlot.type !== 'Long-form');
        await col.updateOne(
          { _id: targetCal._id },
          { $set: {
            'slots.$[s].status':         'posted',
            'slots.$[s].posted':         true,
            'slots.$[s].postedAt':       new Date().toISOString(),
            'slots.$[s].youtubeVideoId': ytId,
            'slots.$[s].pipelineStatus': 'posted',
          }},
          { arrayFilters: [{ 's.day': day, 's.videoIndex': vi }] }
        );
        fs.unlink(targetSlot.assembledPath, () => {});
        console.log(`[PostNow] ✓ Immediately posted "${targetSlot.title}" → https://youtu.be/${ytId}`);
      } catch (e) {
        console.error(`[PostNow] Failed for "${slot.title}":`, e.message);
      }
    });
  } catch (err) {
    console.error('[PostNow] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// TEST ROUTE — admin-only, does NOT touch the content calendar or scheduled slots
// =============================================================================

// POST /api/test/post-short — generate + post one Short to "All & Everything" channel.
// Protected by admin JWT. Returns YouTube URL when done (synchronous — may take ~2 min).
app.post('/api/test/post-short', requireAuth, async (req, res) => {
  try {
    const { OpenAI } = require('openai');
    const fs = require('fs');

    const TITLE = 'The Power of Habit: How to Build Routines That Stick';
    const NICHE = 'Psychology & Human Behaviour';

    const adminEmail = process.env.ADMIN_EMAIL;
    if (!adminEmail) return res.status(503).json({ error: 'ADMIN_EMAIL env var not set' });

    const user = await User.findById(req.user.id);
    if (!user || user.email !== adminEmail) return res.status(403).json({ error: 'Access denied' });
    if (!user) return res.status(404).json({ error: `No user found for ADMIN_EMAIL: ${adminEmail}` });

    // Locate "All & Everything" channel; fall back to first channel if not found by name
    const channel = (user.youtubeChannels || []).find(ch =>
      /all.*everything/i.test(ch.channelName || '')
    ) || user.youtubeChannels?.[0];
    if (!channel) return res.status(404).json({ error: 'No YouTube channel found for this user' });

    console.log(`[TestPost] Channel: "${channel.channelName}" (${channel.channelId})`);

    // Step 1: Generate script via OpenAI
    if (!process.env.OPENAI_API_KEY) return res.status(503).json({ error: 'OPENAI_API_KEY not configured' });
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    console.log(`[TestPost] Generating script for "${TITLE}"...`);
    const structured = await generateStructuredScript(TITLE, NICHE, 'Short', openai);
    const script = structured.fullScript || TITLE;
    console.log(`[TestPost] Script ready (${script.length} chars)`);

    // Step 2: Voiceover via Google TTS REST API
    console.log('[TestPost] Generating voiceover...');
    const { audioPath, captions } = await pipelineGenerateVoiceover(script, String(user._id));
    console.log(`[TestPost] Voiceover: ${audioPath}`);

    // Step 3: Fetch 5 unique Pexels clips
    console.log('[TestPost] Fetching Pexels footage...');
    const footageClips = await pipelineFetchMultipleFootage(TITLE, script, NICHE, String(user._id));
    console.log(`[TestPost] ${footageClips.length} clips fetched`);

    // Step 4: Assemble video with SRT captions (FontSize=22, Bold, MarginV=250)
    const outPath = `/tmp/vly_testpost_${Date.now()}.mp4`;
    console.log('[TestPost] Assembling video...');
    await pipelineAssembleVideo(footageClips, audioPath, outPath, captions, true);
    console.log(`[TestPost] Assembled: ${outPath}`);

    // Step 5: Upload to YouTube as a Short with #Shorts in title
    const uploadTitle = TITLE.includes('#Shorts') ? TITLE : `${TITLE} #Shorts`;
    const uploadDesc  = `${script.slice(0, 4750)}\n\n#Shorts #YouTubeShorts #viral #psychology #habits`;
    console.log(`[TestPost] Uploading to YouTube...`);
    const ytId = await pipelineUploadToYouTube(outPath, uploadTitle, uploadDesc, channel, true);

    fs.unlink(outPath, () => {});
    fs.unlink(audioPath, () => {});

    const ytUrl = `https://youtu.be/${ytId}`;
    console.log(`[TestPost] ✓ ${ytUrl}`);
    res.json({ success: true, youtubeUrl: ytUrl, youtubeVideoId: ytId, channel: channel.channelName, title: uploadTitle });
  } catch (err) {
    console.error('[TestPost] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/test/post-longform — admin-only. Generates and posts a 16:9 long-form video.
// Does NOT affect the content calendar or scheduled slots.
app.post('/api/test/post-longform', requireAuth, async (req, res) => {
  try {
    const { OpenAI } = require('openai');
    const fs = require('fs');

    const TITLE = 'The Complete Guide to Understanding Human Psychology';
    const NICHE = 'Psychology & Human Behaviour';
    const TAGS  = ['psychology', 'human behaviour', 'self improvement', 'mental health', 'educational'];

    const adminEmail = process.env.ADMIN_EMAIL;
    if (!adminEmail) return res.status(503).json({ error: 'ADMIN_EMAIL env var not set' });
    const user = await User.findById(req.user.id);
    if (!user || user.email !== adminEmail) return res.status(403).json({ error: 'Access denied' });
    if (!user) return res.status(404).json({ error: `No user found for ADMIN_EMAIL: ${adminEmail}` });
    const channel = (user.youtubeChannels || []).find(ch =>
      /all.*everything/i.test(ch.channelName || '')
    ) || user.youtubeChannels?.[0];
    if (!channel) return res.status(404).json({ error: 'No YouTube channel found for this user' });
    console.log(`[TestLF] Channel: "${channel.channelName}" (${channel.channelId})`);

    // Step 1: Generate structured long-form script (500-700 words, 3-5 min)
    if (!process.env.OPENAI_API_KEY) return res.status(503).json({ error: 'OPENAI_API_KEY not configured' });
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    console.log(`[TestLF] Generating script for "${TITLE}"...`);
    const res2 = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{
        role: 'user',
        content: `You are a YouTube long-form content strategist for a "${NICHE}" channel.
Write a 500-700 word spoken script for the video titled: "${TITLE}"

Structure it with clear sections:
- Intro (hook the viewer in 30 seconds)
- Main Point 1: What is Human Psychology?
- Main Point 2: The Science of Behaviour
- Main Point 3: How Emotions Drive Decisions
- Main Point 4: The Power of the Subconscious Mind
- Main Point 5: Applying Psychology in Everyday Life
- Conclusion (summarise + call to action to subscribe)

Return valid JSON with exactly these fields:
{
  "hook": "Opening 2-3 sentences that immediately grab attention",
  "mainPoints": ["summary of point 1","summary of point 2","summary of point 3","summary of point 4","summary of point 5"],
  "cta": "Subscribe call-to-action sentence",
  "estimatedDuration": "4:30",
  "fullScript": "Complete 500-700 word natural spoken script with all sections, no stage directions"
}`,
      }],
      temperature: 0.75,
      response_format: { type: 'json_object' },
    });
    const structured = JSON.parse(res2.choices[0].message.content);
    const script = structured.fullScript || TITLE;
    console.log(`[TestLF] Script ready: ${script.split(/\s+/).length} words`);

    // Step 2: Voiceover via Google TTS REST API
    console.log('[TestLF] Generating voiceover...');
    const { audioPath } = await pipelineGenerateVoiceover(script, String(user._id));
    console.log(`[TestLF] Voiceover: ${audioPath}`);

    // Step 3: Fetch 10 unique landscape Pexels clips (2 per section × 5 main points)
    if (!process.env.PEXELS_API_KEY) return res.status(503).json({ error: 'PEXELS_API_KEY not configured' });
    const pexelsKey = process.env.PEXELS_API_KEY;
    const sectionQueries = [
      'human brain psychology', 'people thinking mindset',
      'science research laboratory', 'human behaviour study',
      'emotions facial expressions', 'decision making choices',
      'subconscious mind meditation', 'brain waves focus',
      'everyday life psychology', 'self improvement growth',
    ];
    console.log('[TestLF] Fetching 10 landscape Pexels clips...');
    const footageClips = [];
    const pickedIds = new Set();
    for (const query of sectionQueries) {
      let found = false;
      for (let page = 1; page <= 3 && !found; page++) {
        try {
          const params = new URLSearchParams({ query, per_page: '10', orientation: 'landscape', page: String(page) });
          const pxRes  = await fetch(`https://api.pexels.com/videos/search?${params}`, {
            headers: { Authorization: pexelsKey },
          });
          const data   = await pxRes.json();
          const videos = (data.videos || []).filter(v => v.duration >= 5 && !pickedIds.has(String(v.id)));
          if (!videos.length) continue;
          const video = videos[0];
          const file  = (video.video_files || []).find(f => f.quality === 'hd') || video.video_files?.[0];
          if (!file?.link) continue;
          pickedIds.add(String(video.id));
          footageClips.push({ id: String(video.id), url: file.link, duration: video.duration, query });
          console.log(`[TestLF] Clip "${query}": id=${video.id}`);
          found = true;
        } catch (e) {
          console.warn(`[TestLF] Pexels failed for "${query}" p${page}:`, e.message);
        }
      }
      if (!found) console.warn(`[TestLF] No clip found for "${query}"`);
    }
    if (!footageClips.length) throw new Error('No Pexels footage clips fetched');
    console.log(`[TestLF] ${footageClips.length} clips fetched`);

    // Step 4: Assemble 1920x1080 16:9 video with no captions
    const runId   = Date.now();
    const outPath = `/tmp/vly_lf_test_${runId}.mp4`;
    const clipPaths = footageClips.map((_, i) => `/tmp/vly_lf_clip_${runId}_${i}.mp4`);
    const musicUrl  = ROYALTY_FREE_MUSIC[Math.floor(Math.random() * ROYALTY_FREE_MUSIC.length)];
    const musicPath = `/tmp/vly_lf_music_${runId}.mp3`;

    console.log('[TestLF] Downloading clips...');
    await Promise.all(footageClips.map((clip, i) =>
      new Promise((resolve, reject) => {
        exec(`curl -sL --max-time 60 -o "${clipPaths[i]}" "${clip.url}"`, { timeout: 90000 },
          err => err ? reject(new Error(`Clip ${i} download failed: ${err.message}`)) : resolve());
      })
    ));

    let hasMus = false;
    try {
      await new Promise((resolve, reject) => {
        exec(`curl -sL --max-time 30 -o "${musicPath}" "${musicUrl}"`, { timeout: 45000 },
          err => err ? reject(err) : resolve());
      });
      hasMus = fs.existsSync(musicPath) && fs.statSync(musicPath).size > 1000;
    } catch (e) {
      console.warn('[TestLF] Music download failed (non-fatal):', e.message);
    }

    // Build 1920x1080 filter: scale landscape, 6s per clip, concat, mix audio
    const n = clipPaths.length;
    const baseParts = [];
    for (let i = 0; i < n; i++) {
      baseParts.push(
        `[${i}:v]scale=1920:1080:force_original_aspect_ratio=increase,` +
        `crop=1920:1080,setsar=1,trim=0:6,setpts=PTS-STARTPTS[v${i}]`
      );
    }
    const concatIn = Array.from({ length: n }, (_, i) => `[v${i}]`).join('');
    baseParts.push(`${concatIn}concat=n=${n}:v=1:a=0[vcat]`);
    baseParts.push('[vcat]null[vout]');
    const voiceIdx    = n;
    const audioFilter = hasMus
      ? `[${voiceIdx}:a][${n + 1}:a]amix=inputs=2:duration=shortest:weights=1 0.15[aout]`
      : `[${voiceIdx}:a]volume=1.0[aout]`;
    baseParts.push(audioFilter);
    const filterStr = baseParts.join(';');

    console.log('[TestLF] Assembling 1920x1080 video...');
    await new Promise((resolve, reject) => {
      const args = ['-y'];
      for (const p of clipPaths) args.push('-i', p);
      args.push('-i', audioPath);
      if (hasMus) args.push('-i', musicPath);
      args.push('-filter_complex', filterStr);
      args.push('-map', '[vout]', '-map', '[aout]');
      args.push('-c:v', 'libx264', '-preset', 'fast', '-crf', '23');
      args.push('-c:a', 'aac', '-b:a', '128k');
      args.push('-movflags', '+faststart');
      args.push('-shortest');
      args.push(outPath);
      const ff = spawn('ffmpeg', args);
      const errLines = [];
      ff.stderr.on('data', d => errLines.push(...d.toString().split('\n')));
      ff.on('close', code => {
        if (code !== 0) {
          reject(new Error(`ffmpeg failed (exit ${code}):\n${errLines.filter(l => l.trim()).slice(-5).join('\n')}`));
        } else resolve();
      });
      ff.on('error', err => reject(new Error(`ffmpeg spawn error: ${err.message}`)));
    });
    console.log(`[TestLF] Assembled: ${outPath}`);

    // Cleanup temp files
    const cleanup = () => {
      for (const p of clipPaths) fs.unlink(p, () => {});
      if (hasMus) fs.unlink(musicPath, () => {});
      fs.unlink(audioPath, () => {});
      fs.unlink(outPath, () => {});
    };

    // Step 5: Upload as a regular (non-Short) video
    const description = `${script.slice(0, 4750)}\n\n` +
      `${TAGS.map(t => '#' + t.replace(/\s+/g, '')).join(' ')}`;
    console.log('[TestLF] Uploading to YouTube (long-form)...');
    const ytId = await pipelineUploadToYouTube(outPath, TITLE, description, channel, false);
    cleanup();

    const ytUrl = `https://youtu.be/${ytId}`;
    console.log(`[TestLF] ✓ ${ytUrl}`);
    res.json({ success: true, youtubeUrl: ytUrl, youtubeVideoId: ytId, channel: channel.channelName, title: TITLE, clips: footageClips.length });
  } catch (err) {
    console.error('[TestLF] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// CRON REGISTRATION — called once after MongoDB is connected
// All cron jobs live here so they can safely access the database.
// =============================================================================
function registerCronJobs() {
  // Daily Topic Scout — 6 AM daily
  cron.schedule('0 6 * * *', async () => {
    console.log('[Scout] Daily topic scout starting...');
    try {
      exec(`python3 "${path.join(__dirname, 'daily_topic_scout_agent.py')}" --run-all`, { timeout: 300000 }, (err, stdout, stderr) => {
        if (err) console.error('[Scout] Cron error:', stderr);
        else     console.log('[Scout] Cron complete:', stdout.slice(0, 200));
      });
    } catch (e) { console.error('[Scout] Cron failed:', e); }
  });

  // Competitor Watcher — every 4 hours (feature-gated)
  if (FEATURES.competitorWatcher) {
    cron.schedule('0 */4 * * *', async () => {
      console.log('[CompetitorWatcher] Check starting...');
      try {
        exec(`python3 "${path.join(__dirname, 'competitor_watcher_agent.py')}" --run-all`, { timeout: 600000 }, (err, stdout, stderr) => {
          if (err) console.error('[CompetitorWatcher] Cron error:', stderr);
          else     console.log('[CompetitorWatcher] Cron complete:', stdout.slice(0, 200));
        });
      } catch (e) { console.error('[CompetitorWatcher] Cron failed:', e); }
    });
  }

  // Script Research Queue — every 30 minutes
  cron.schedule('*/30 * * * *', async () => {
    try {
      exec(`python3 "${path.join(__dirname, 'script_research_agent.py')}" --process-queue ""`, { timeout: 300000 }, (err, stdout, stderr) => {
        if (err) console.error('[ScriptResearch] Queue error:', stderr);
      });
    } catch (e) { console.error('[ScriptResearch] Queue cron failed:', e); }
  });

  // Trend Scraper — 7 AM daily (feature-gated)
  if (FEATURES.trendScraper) {
    cron.schedule('0 7 * * *', async () => {
      console.log('[TrendScraper] Daily run starting...');
      try {
        exec(`python3 "${path.join(__dirname, 'trend_scraper_agent.py')}" --run-all`, { timeout: 600000 }, (err, stdout, stderr) => {
          if (err) console.error('[TrendScraper] Cron error:', stderr);
          else     console.log('[TrendScraper] Cron complete:', stdout.slice(0, 200));
        });
      } catch (e) { console.error('[TrendScraper] Cron failed:', e); }
    });
  }

  // Competitor Content Scraper — 5 AM and 5 PM daily (feature-gated)
  if (FEATURES.competitorScraper) {
    cron.schedule('0 5,17 * * *', async () => {
      console.log('[CompetitorScraper] Bulk scrape starting...');
      try {
        const users = await User.find({ plan: { $ne: 'trial' } }).select('_id').lean();
        for (const user of users) {
          exec(`python3 "${path.join(__dirname, 'competitor_content_scraper.py')}" --bulk ${user._id}`, { timeout: 300000 },
            (err, stdout, stderr) => { if (err) console.error(`[CompetitorScraper] Error for ${user._id}:`, stderr); }
          );
        }
      } catch (e) { console.error('[CompetitorScraper] Cron failed:', e); }
    });
  }

  // Posting cron — every 5 minutes: upload pre-assembled videos that are due
  cron.schedule('*/5 * * * *', async () => {
    console.log('[PostingCron] Cron fired at', new Date().toISOString(), '— checking for due slots');
    try { await runScheduledPosting(); }
    catch (err) { console.error('[PostingCron] Error:', err.message); }
  });

  // Daily generation — 00:01 UTC
  // Primary trigger is maybePreGenerateTomorrow(); this is the safety net.
  cron.schedule('1 0 * * *', async () => {
    console.log('[DailyGen] Cron fired at', new Date().toISOString());
    try {
      const result = await runDailySlotGeneration();
      console.log(`[DailyGen] Cron complete — ${result.generated}/${result.total} user(s) got new slots`);
    }
    catch (err) { console.error('[DailyGen] Cron error:', err.message); }
  }, { timezone: 'UTC' });

  // Analytics Collection — 4 AM daily (feature-gated)
  if (FEATURES.analyticsCollection) {
    cron.schedule('0 4 * * *', async () => {
      console.log('[Analytics] Collection running...');
      try {
        await runPython(path.join(__dirname, 'analytics_engine.py'), '--collect', 90000);
      } catch (err) { console.error('[Analytics] Error:', err); }
    });
  }

  // Monday 3 AM UTC — recalculate optimal posting times + update hook scores
  cron.schedule('0 3 * * 1', async () => {
    console.log('[Timing] Weekly recalculation of optimal posting times starting...');
    try {
      const users = await User.find({ 'youtubeChannels.0': { $exists: true } }).lean();
      let updated = 0;
      for (const user of users) {
        const nicheName = user.nicheName || user.youtubeChannels?.[0]?.nicheName || '';
        if (!nicheName) continue;
        for (const ch of (user.youtubeChannels || [])) {
          if (!ch.channelId) continue;
          await getOptimalPostingTimes(String(user._id), ch.channelId, nicheName, user.plan)
            .catch(e => console.warn(`[Timing] Weekly recalc failed for ${ch.channelId}:`, e.message));
          updated++;
        }
      }
      console.log(`[Timing] Weekly recalculation complete — ${updated} channel(s) updated`);
    } catch (e) { console.error('[Timing] Weekly cron failed:', e.message); }

    // Weekly hook performance score update
    if (FEATURES.learningLoop) {
      console.log('[Hooks] Weekly hook score update starting...');
      await updateHookScores().catch(e => console.error('[Hooks] Weekly update error:', e.message));
    }
  }, { timezone: 'UTC' });

  // Monthly footage reset — 1st of each month at 00:05 UTC
  // Clears usedFootageIds so clips can be reused after ~30 days
  cron.schedule('5 0 1 * *', async () => {
    console.log('[MonthlyReset] Clearing usedFootageIds for all users...');
    try {
      const result = await User.updateMany({}, { $set: { usedFootageIds: [] } });
      console.log(`[MonthlyReset] usedFootageIds cleared for ${result.modifiedCount} user(s)`);
    } catch (e) {
      console.error('[MonthlyReset] Failed:', e.message);
    }
  }, { timezone: 'UTC' });

  // Trial + past_due expiry check — 1 AM UTC daily
  cron.schedule('0 1 * * *', async () => {
    console.log('[Trial] Running daily expiry check...');
    try {
      const now = new Date();

      // Only expire trial users — never expire paid plans via this cron
      const expiredTrials = await User.find({
        subscriptionStatus: 'trial',
        trialEndsAt: { $lt: now },
      }).select('email trialEndsAt').lean();

      for (const u of expiredTrials) {
        const daysLeft = Math.ceil((new Date(u.trialEndsAt) - now) / 86400000);
        await User.findByIdAndUpdate(u._id, { subscriptionStatus: 'expired' });
        console.log(`[Trial] User ${u.email} trial expired (was ${daysLeft}d ago)`);
      }
      if (expiredTrials.length > 0) console.log(`[Trial] ${expiredTrials.length} trial(s) expired`);

      // Expire past_due users whose 3-day grace period is over
      const graceExpiry = new Date(now.getTime() - 3 * 24 * 60 * 60 * 1000);
      const pastDueExpired = await User.find({
        subscriptionStatus: 'past_due',
        pastDueSince: { $lt: graceExpiry },
      }).select('email pastDueSince').lean();

      for (const u of pastDueExpired) {
        await User.findByIdAndUpdate(u._id, { subscriptionStatus: 'expired' });
        console.log(`[Trial] User ${u.email} past_due grace period expired — marking expired`);
      }
      if (pastDueExpired.length > 0) console.log(`[Trial] ${pastDueExpired.length} past_due account(s) expired after grace period`);

    } catch (e) { console.error('[Trial] Expiry cron error:', e.message); }
  }, { timezone: 'UTC' });

  // Daily YouTube quota reset log — midnight UTC
  cron.schedule('0 0 * * *', async () => {
    console.log('[Quota] Daily quota reset — 10,000 units available');
    // Delete quota records older than 7 days to keep the collection lean
    const cutoff = new Date();
    cutoff.setUTCDate(cutoff.getUTCDate() - 7);
    const cutoffDate = cutoff.toISOString().slice(0, 10);
    agentCol('youtubeQuota').deleteMany({ date: { $lt: cutoffDate } }).catch(() => {});
  }, { timezone: 'UTC' });

  // Monthly hooks library rebuild — 1st of every month at 01:00 UTC
  cron.schedule('0 1 1 * *', async () => {
    console.log('[Hooks] Monthly rebuild cron starting...');
    await rebuildHooksMonthly().catch(e => console.error('[Hooks] Monthly rebuild error:', e.message));
  }, { timezone: 'UTC' });

  cronJobsRegistered = true;
  console.log('[Cron] All cron jobs registered successfully');
}

// =============================================================================
// HEALTH CHECK — defined early above (line ~115), kept here as reference comment only
// app.get('/health', ...) — already registered at startup

// =============================================================================
// SERVE STATIC FRONTEND
// =============================================================================
// Serve project root as static directory (index.html, sw.js, manifest.json, icons live here)
app.use(express.static(__dirname, {
  index: false, // let the catchall handle / so API routes take priority
  setHeaders: (res, filePath) => {
    // Service worker must not be cached by the browser
    if (filePath.endsWith('sw.js')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.setHeader('Service-Worker-Allowed', '/');
    }
    if (filePath.endsWith('manifest.json')) {
      res.setHeader('Content-Type', 'application/manifest+json');
    }
    if (filePath.endsWith('.svg')) {
      res.setHeader('Content-Type', 'image/svg+xml');
    }
  },
}));

app.get('*', (req, res) => {
  if (req.path.startsWith('/api') || req.path.startsWith('/auth') || req.path.startsWith('/webhooks')) return res.status(404).json({ error: 'Not found' });
  res.sendFile(path.join(__dirname, 'index.html'));
});

// =============================================================================
// START
// =============================================================================
app.listen(PORT, () => {
  console.log(`Viralityy server running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
  // Startup slot check is triggered from the MongoDB .then() callback above,
  // so it is guaranteed to run only after the database connection is ready.

  // ── Quota audit startup summary ───────────────────────────────────────────
  const totalCalls   = QUOTA_AUDIT_MAP.length;
  const guardedCalls = QUOTA_AUDIT_MAP.filter(e => e.guarded).length;
  console.log(`[QUOTA AUDIT] Found ${totalCalls} YouTube API call sites — ${guardedCalls} guarded, ${totalCalls - guardedCalls} unguarded`);
  QUOTA_AUDIT_MAP.forEach(e => {
    console.log(`[QUOTA AUDIT]  ${e.guarded ? '✓' : '✗'} ${e.endpoint.padEnd(40)} ${String(e.cost).padStart(4)} units  [${e.location}]`);
  });
});

module.exports = app;
