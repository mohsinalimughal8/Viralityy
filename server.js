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
// Stripe lazy-initialised — missing key does not crash startup
let _stripeInstance = null;
function getStripeClient() {
  if (_stripeInstance) return _stripeInstance;
  if (!process.env.STRIPE_SECRET_KEY) {
    throw new Error('STRIPE_SECRET_KEY not set — add it in Railway env vars before using billing');
  }
  _stripeInstance = require('stripe')(process.env.STRIPE_SECRET_KEY);
  return _stripeInstance;
}
const stripe = new Proxy({}, {
  get(_, prop) {
    return (...args) => getStripeClient()[prop](...args);
  }
});
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
  return `${date}T${hhmm}:00`;
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
    const today   = new Date().toISOString().slice(0, 10);
    const now     = new Date().toISOString();
    const calCol  = agentCol('content_calendars');
    const fs      = require('fs');
    const calendars = await calCol.find({ status: 'active' }).toArray();

    console.log(`[AutoPost Audit] ─── Server startup audit ─── ${now}`);
    console.log(`[AutoPost Audit] OPENAI_API_KEY:        ${process.env.OPENAI_API_KEY        ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[AutoPost Audit] YOUTUBE_API_KEY:       ${process.env.YOUTUBE_API_KEY       ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[AutoPost Audit] YOUTUBE_CLIENT_ID:     ${process.env.YOUTUBE_CLIENT_ID     ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[AutoPost Audit] YOUTUBE_CLIENT_SECRET: ${process.env.YOUTUBE_CLIENT_SECRET ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[AutoPost Audit] GOOGLE_TTS_API_KEY:    ${process.env.GOOGLE_TTS_API_KEY    ? 'SET' : 'MISSING ⚠'}`);
    console.log(`[AutoPost Audit] Active calendars: ${calendars.length}`);

    for (const cal of calendars) {
      const user    = await User.findById(cal.userId).catch(() => null);
      if (!user) { console.log(`[AutoPost Audit]   User ${cal.userId} not found — skipping`); continue; }
      const channel = (user.youtubeChannels || []).find(ch => ch.channelId === cal.channelId) || user.youtubeChannels?.[0];
      const todaySlots = (cal.slots || []).filter(s => s.date === today);

      console.log(`[AutoPost Audit]   User ${user._id} | plan=${user.plan} | autoPost=${user.autoPost !== false} | niche=${cal.nicheName || 'none'}`);
      console.log(`[AutoPost Audit]     Channel: ${channel?.channelName || 'NONE CONNECTED'} | accessToken=${channel?.accessToken ? 'present' : 'MISSING ⚠'} | refreshToken=${channel?.refreshToken ? 'present' : 'MISSING ⚠'}`);
      console.log(`[AutoPost Audit]     Today slots: ${todaySlots.length} | ${todaySlots.map(s => `${s.title?.slice(0,20)}→${s.status}`).join(', ') || 'none'}`);

      // Recovery 1: reset stuck "processing"/"assembling" slots — keep as "approved" if data cached
      const stuck = todaySlots.filter(s => s.status === 'processing' || s.status === 'assembling');
      if (stuck.length) {
        console.log(`[Startup Recovery] Resetting ${stuck.length} stuck processing/assembling slot(s) for user ${user._id}`);
        for (const s of stuck) {
          const hasData   = s.scriptText && Array.isArray(s.footageUrls) && s.footageUrls.length > 0;
          const newStatus = hasData ? 'approved' : 'pending';
          await calCol.updateOne(
            { _id: cal._id },
            { $set: { 'slots.$[s].status': newStatus, 'slots.$[s].pipelineStatus': `reset-on-startup-${newStatus}` } },
            { arrayFilters: [{ 's.day': s.day, 's.videoIndex': s.videoIndex || 1 }] }
          ).catch(() => {});
        }
      }

      // Recovery 2: "ready" slots whose /tmp file was wiped — promote to "approved" if data cached
      const readyMissing = todaySlots.filter(s =>
        s.status === 'ready' && !s.posted && (!s.assembledPath || !fs.existsSync(s.assembledPath))
      );
      if (readyMissing.length) {
        console.log(`[Startup Recovery] ${readyMissing.length} "ready" slot(s) missing /tmp file — promoting to approved if data cached`);
        for (const s of readyMissing) {
          const hasData = s.scriptText && Array.isArray(s.footageUrls) && s.footageUrls.length > 0;
          await calCol.updateOne(
            { _id: cal._id },
            { $set: {
              'slots.$[s].status':         hasData ? 'approved' : 'pending',
              'slots.$[s].pipelineStatus': hasData ? 'data-ready' : 'reset-missing-file',
              'slots.$[s].assembledPath':  null,
            }},
            { arrayFilters: [{ 's.day': s.day, 's.videoIndex': s.videoIndex || 1 }] }
          ).catch(() => {});
        }
      }

      // Recovery 6: reset today's failed slots → pending for one more retry
      const failedToday = todaySlots.filter(s => s.status === 'failed' && !s.posted);
      if (failedToday.length) {
        console.log(`[Startup Recovery] Resetting ${failedToday.length} failed slot(s) → pending for retry (user ${user._id})`);
        for (const s of failedToday) {
          await calCol.updateOne(
            { _id: cal._id },
            { $set: { 'slots.$[s].status': 'pending', 'slots.$[s].pipelineStatus': 'retry-on-startup', 'slots.$[s].pipelineError': null } },
            { arrayFilters: [{ 's.day': s.day, 's.videoIndex': s.videoIndex || 1 }] }
          ).catch(() => {});
        }
      }
    }

    // Recovery 3: generate today's slots for calendars that are missing them
    const freshCals = await calCol.find({ status: 'active' }).toArray();
    const missing   = freshCals.filter(c => !(c.slots || []).some(s => s.date === today));
    if (missing.length > 0) {
      console.log(`[Startup Recovery] ${missing.length} calendar(s) missing today's slots — generating now`);
      for (const cal of missing) {
        await runDailySlotGeneration(String(cal.userId)).catch(e =>
          console.error(`[Startup Recovery] Generation failed for calendar ${cal._id}:`, e.message)
        );
      }
    } else {
      console.log('[AutoPost Audit] All active calendars have today\'s slots ✓');
    }

    // Recovery 4: immediately post overdue "ready" (file exists) or "approved" (data cached) slots
    const finalCals = await calCol.find({ status: 'active' }).toArray();
    for (const cal of finalCals) {
      const user = await User.findById(cal.userId).catch(() => null);
      if (!user || user.autoPost === false) continue;
      const channel = (user.youtubeChannels || []).find(ch => ch.channelId === cal.channelId) || user.youtubeChannels?.[0];
      if (!channel?.accessToken) continue;
      const overdueReady = (cal.slots || []).filter(s =>
        s.date === today && s.status === 'ready' && !s.posted &&
        s.assembledPath && require('fs').existsSync(s.assembledPath) &&
        (s.scheduledPostTime || `${s.date}T18:00:00`) <= now
      );
      const overdueApproved = (cal.slots || []).filter(s =>
        s.date === today && s.status === 'approved' && !s.posted &&
        (s.scheduledPostTime || `${s.date}T18:00:00`) <= now
      );
      if (!overdueReady.length && !overdueApproved.length) continue;
      console.log(`[Startup Recovery] ${overdueReady.length} overdue ready + ${overdueApproved.length} overdue approved slot(s) for user ${user._id} — posting now`);
      const capturedCal4 = cal; const capturedUser4 = user; const capturedChan4 = channel;
      setImmediate(async () => {
        for (const slot of overdueReady) {
          try {
            const ytId = await pipelineUploadToYouTube(slot.assembledPath, slot.title, slot.cachedScript || slot.title, capturedChan4, slot.type !== 'Long-form');
            await calCol.updateOne(
              { _id: capturedCal4._id },
              { $set: { 'slots.$[s].status': 'posted', 'slots.$[s].posted': true, 'slots.$[s].postedAt': new Date().toISOString(), 'slots.$[s].youtubeVideoId': ytId } },
              { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
            );
            require('fs').unlink(slot.assembledPath, () => {});
            console.log(`[Startup Recovery] ✓ Posted "${slot.title}" → https://youtu.be/${ytId}`);
          } catch (e) {
            console.error(`[Startup Recovery] ✗ Failed to post "${slot.title}": ${e.message}`);
          }
        }
        for (const slot of overdueApproved) {
          try {
            await runAssembleAndUpload(capturedCal4, slot, capturedUser4, calCol, capturedChan4);
            console.log(`[Startup Recovery] ✓ Assembled+posted "${slot.title}"`);
          } catch (e) {
            console.error(`[Startup Recovery] ✗ Assemble+upload failed for "${slot.title}": ${e.message}`);
          }
        }
      });
    }

    // Recovery 5: run pipeline for any pending slots that were never processed
    const pipelineCals = await calCol.find({ status: 'active' }).toArray();
    for (const cal of pipelineCals) {
      const user = await User.findById(cal.userId).catch(() => null);
      if (!user) continue;
      const in30minStartup = new Date(Date.now() + 30 * 60 * 1000).toISOString();
      const pendingSlots = (cal.slots || []).filter(s => {
        if (s.date !== today || s.posted || s.status !== 'pending') return false;
        const postAt = s.scheduledPostTime || `${s.date}T18:00:00`;
        return postAt <= in30minStartup; // only within 30 min of post time (or overdue)
      });
      if (!pendingSlots.length) continue;
      console.log(`[Startup Recovery] Found ${pendingSlots.length} pending slot(s) due within 30 min — starting pipeline for user ${user._id}`);
      const capturedCal  = cal;
      const capturedUser = user;
      setImmediate(async () => {
        for (const s of pendingSlots) {
          try {
            const freshCal  = await calCol.findOne({ _id: capturedCal._id });
            const freshSlot = (freshCal?.slots || []).find(x => x.date === today && x.videoIndex === s.videoIndex);
            if (!freshSlot || freshSlot.status !== 'pending' || freshSlot.posted) continue;
            console.log(`[Startup Recovery] Running pipeline for pending slot: "${freshSlot.title}"`);
            await runProductionPipelineWithRetry(freshCal, freshSlot, capturedUser, calCol);
          } catch (e) {
            console.error(`[Startup Recovery] Pipeline failed for "${s.title}": ${e.message}`);
          }
        }
      });
    }

    console.log('[AutoPost Audit] ─── Startup audit complete ───');
  } catch (e) {
    console.error('[Startup] Slot check failed:', e.message);
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
    registerCronJobs();
    setTimeout(runStartupSlotCheck, 3000);
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
  trialEndsAt:    { type: Date, default: () => new Date(Date.now() + 7 * 24 * 60 * 60 * 1000) },
  stripeCustomerId:     { type: String },
  stripeSubscriptionId: { type: String },
  youtubeChannels: [{ channelId: String, channelName: String, accessToken: String, refreshToken: String, nicheId: String, nicheName: String, paused: Boolean, tiktokEnabled: Boolean, instagramEnabled: Boolean, connectedAt: String, subscriberCount: Number, thumbnail: String }],
  pendingOAuthChannels: [{ channelId: String, channelName: String, thumbnail: String, subscriberCount: Number }],
  googleAccessToken:  String,
  googleRefreshToken: String,
  nicheId:         { type: String },
  nicheName:       { type: String },
  nicheChangesUsed: { type: Number, default: 0 },
  nicheChangesYear: { type: Number, default: () => new Date().getFullYear() },
  affiliateCode:   { type: String, unique: true, sparse: true },
  referredBy:      { type: String },
  autoPost:        { type: Boolean, default: true },
  timezone:        { type: String },
  usedFootageIds:  { type: [String], default: [] },
  createdAt:       { type: Date, default: Date.now },
}, { timestamps: true });

const User = mongoose.model('User', userSchema);

const triedChannelSchema = new mongoose.Schema({ channelId: { type: String, unique: true }, usedAt: { type: Date, default: Date.now } });
const TriedChannel = mongoose.model('TriedChannel', triedChannelSchema);

// =============================================================================
// MIDDLEWARE
// =============================================================================
app.use(helmet({ contentSecurityPolicy: false }));
// Allow requests from both the Railway backend URL and the Netlify frontend
const ALLOWED_ORIGINS = [
  process.env.APP_URL,
  process.env.FRONTEND_URL,
  'https://viralityy.pages.dev',
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
  try {
    const slotsCol = agentCol('calendar_slots');
    const [gen, ready, posted, failed, processing] = await Promise.all([
      slotsCol.countDocuments({ date: today }),
      slotsCol.countDocuments({ date: today, status: 'ready', posted: false }),
      slotsCol.countDocuments({ status: 'posted', postedAt: { $gte: startOfToday } }),
      slotsCol.countDocuments({ date: today, status: 'failed' }),
      slotsCol.countDocuments({ date: today, status: 'processing' }),
    ]);
    const nextSlot = await slotsCol
      .find({ date: today, status: { $in: ['ready', 'pending'] }, posted: false })
      .sort({ scheduledPostTime: 1 })
      .limit(1)
      .toArray()
      .then(r => r[0] || null)
      .catch(() => null);
    pipeline = {
      todaysSlotsGenerated: gen > 0,
      slotsReady:      ready,
      slotsPosted:     posted,
      slotsFailed:     failed,
      slotsProcessing: processing,
      nextPostTime:    nextSlot?.scheduledPostTime || null,
      cronJobsRegistered,
    };
  } catch { /* pipeline stays { error: 'unavailable' } */ }

  res.json({
    status:  'ok',
    uptime:  process.uptime(),
    env:     process.env.NODE_ENV || 'development',
    mongo:   mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
    stripe:  !!process.env.STRIPE_SECRET_KEY ? 'configured' : 'not configured (billing disabled)',
    youtube: !!process.env.YOUTUBE_API_KEY   ? 'configured' : 'not configured',
    openai:  !!process.env.OPENAI_API_KEY    ? 'configured' : 'not configured',
    pipeline,
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
      user = await User.create({
        name:            profile.displayName,
        email:           profile.emails[0].value,
        googleId:        profile.id,
        plan:            'trial',
        trialStartedAt:  new Date(),
        trialEndsAt:     new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
        youtubeChannels: [],
        affiliateCode:   generateAffiliateCode(),
        ...(affiliateRef && { referredBy: affiliateRef }),
      });
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
function requireAuth(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (token) {
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      req.user = { id: decoded.userId };
      return next();
    } catch { return res.status(401).json({ error: 'Invalid token' }); }
  }
  if (req.isAuthenticated()) return next();
  return res.status(401).json({ error: 'Authentication required' });
}

function hashIp(ip) {
  return crypto.createHmac('sha256', process.env.IP_SALT || 'default_salt').update(ip || '').digest('hex');
}

// Admin JWT middleware — separate secret from user JWT; returns 403 with no details on failure
function requireAdmin(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(403).json({ error: 'Forbidden' });
  try {
    const decoded = jwt.verify(token, process.env.ADMIN_PASSWORD || 'admin_secret_unset');
    if (!decoded.isAdmin) return res.status(403).json({ error: 'Forbidden' });
    req.adminUser = decoded;
    return next();
  } catch {
    return res.status(403).json({ error: 'Forbidden' });
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

const PLAN_CONFIG = {
  trial:      { shortsPerDay: 1,  longFormPerWeek: 0, channels: 1 },
  starter:    { shortsPerDay: 3,  longFormPerWeek: 0, channels: 1 },
  shorts_pro: { shortsPerDay: 7,  longFormPerWeek: 0, channels: 1 },
  growth:     { shortsPerDay: 10, longFormPerWeek: 1,  channels: 3 },
  agency:     { shortsPerDay: 10, longFormPerWeek: 1,  channels: 5 },
};

function planNicheQuota(plan) {
  return plan === 'trial' ? 0 : Infinity; // trial locked, all paid plans unlimited
}

function isPlanActive(user) {
  if (user.plan === 'trial') return new Date() < new Date(user.trialEndsAt);
  return !!user.stripeSubscriptionId;
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
    const token = jwt.sign({ userId: req.user.id }, process.env.JWT_SECRET, { expiresIn: '7d' });
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
    const user = await User.create({
      name, email: email.toLowerCase(), passwordHash, affiliateCode,
      plan: 'trial',
      trialStartedAt: new Date(),
      trialEndsAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
      ...(ref && { referredBy: ref }),
    });

    const token = jwt.sign({ userId: user.id }, process.env.JWT_SECRET, { expiresIn: '7d' });
    res.status(201).json({ token, user: { id: user.id, name: user.name, email: user.email, plan: user.plan, trialEndsAt: user.trialEndsAt } });
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

    const token = jwt.sign({ userId: user.id }, process.env.JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { id: user.id, name: user.name, email: user.email, plan: user.plan, trialEndsAt: user.trialEndsAt } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/auth/me — current user
app.get('/api/auth/me', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id).select('-passwordHash');
    if (!user) return res.status(404).json({ error: 'User not found' });
    res.json({ user, planActive: isPlanActive(user), trialDaysLeft: Math.max(0, Math.ceil((new Date(user.trialEndsAt) - Date.now()) / 86400000)) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// =============================================================================
// YOUTUBE CHANNEL ROUTES
// =============================================================================

// GET /api/channels — list user's connected channels
app.get('/api/channels', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    const channels = (user.youtubeChannels || []).map(ch => ({
      channelId:      ch.channelId      || '',
      channelName:    ch.channelName    || 'My Channel',
      subscriberCount: ch.subscriberCount || 0,
      nicheId:        ch.nicheId        || user.nicheId   || null,
      nicheName:      ch.nicheName      || user.nicheName || null,
      paused:         ch.paused         || false,
    }));
    res.json({ channels, count: channels.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
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
// STRIPE BILLING ROUTES
// =============================================================================
const STRIPE_PLANS = {
  starter_monthly:    process.env.STRIPE_PRICE_STARTER_MONTHLY,
  starter_annual:     process.env.STRIPE_PRICE_STARTER_ANNUAL,
  shorts_pro_monthly: process.env.STRIPE_PRICE_SHORTS_PRO_MONTHLY,
  shorts_pro_annual:  process.env.STRIPE_PRICE_SHORTS_PRO_ANNUAL,
  growth_monthly:     process.env.STRIPE_PRICE_GROWTH_MONTHLY,
  growth_annual:      process.env.STRIPE_PRICE_GROWTH_ANNUAL,
  agency_monthly:     process.env.STRIPE_PRICE_AGENCY_MONTHLY,
  agency_annual:      process.env.STRIPE_PRICE_AGENCY_ANNUAL,
};

// POST /api/billing/checkout — create Stripe checkout session
app.post('/api/billing/checkout', requireAuth, async (req, res) => {
  try {
    const { planKey } = req.body; // e.g. 'growth_monthly'
    const priceId = STRIPE_PLANS[planKey];
    if (!priceId) return res.status(400).json({ error: `Unknown plan: ${planKey}` });

    const user = await User.findById(req.user.id);
    let customerId = user.stripeCustomerId;

    if (!customerId) {
      const customer = await stripe.customers.create({ email: user.email, name: user.name, metadata: { userId: user.id } });
      customerId = customer.id;
      user.stripeCustomerId = customerId;
      await user.save();
    }

    const session = await stripe.checkout.sessions.create({
      customer:   customerId,
      mode:       'subscription',
      line_items: [{ price: priceId, quantity: 1 }],
      success_url: `${process.env.APP_URL}/dashboard?subscribed=1`,
      cancel_url:  `${process.env.APP_URL}/pricing`,
      metadata:   { userId: user.id, planKey },
    });

    res.json({ url: session.url });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/billing/portal — customer portal for managing subscription
app.post('/api/billing/portal', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user.stripeCustomerId) return res.status(400).json({ error: 'No billing account found' });
    const session = await stripe.billingPortal.sessions.create({ customer: user.stripeCustomerId, return_url: `${process.env.APP_URL}/dashboard` });
    res.json({ url: session.url });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/billing/promo — promo code plan upgrade bypass
app.post('/api/billing/promo', requireAuth, async (req, res) => {
  try {
    const { promoCode, plan } = req.body;
    const validPlans = ['shorts_pro', 'growth', 'agency'];
    if (promoCode !== 'VRL-X9K2-M7QP-4TZW') return res.status(400).json({ success: false, error: 'Invalid promo code' });
    if (!validPlans.includes(plan)) return res.status(400).json({ success: false, error: 'Invalid plan' });
    await User.findByIdAndUpdate(req.user.id, { plan });
    res.json({ success: true, plan });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// POST /webhooks/stripe — Stripe event handler (raw body required)
app.post('/webhooks/stripe', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) { return res.status(400).send(`Webhook Error: ${err.message}`); }

  const planFromMetadata = (metadata) => {
    const key = metadata?.planKey || '';
    if (key.startsWith('starter'))    return 'starter';
    if (key.startsWith('shorts_pro')) return 'shorts_pro';
    if (key.startsWith('growth'))     return 'growth';
    if (key.startsWith('agency'))     return 'agency';
    return 'starter';
  };

  try {
    switch (event.type) {
      case 'checkout.session.completed': {
        const session = event.data.object;
        const userId  = session.metadata?.userId;
        if (userId) {
          const planName = planFromMetadata(session.metadata);
          await User.findByIdAndUpdate(userId, { plan: planName, stripeSubscriptionId: session.subscription, stripeCustomerId: session.customer });
          console.log(`User ${userId} subscribed to ${planName}`);
        }
        break;
      }
      case 'customer.subscription.deleted': {
        const sub = event.data.object;
        const user = await User.findOne({ stripeSubscriptionId: sub.id });
        if (user) { user.plan = 'trial'; user.stripeSubscriptionId = null; await user.save(); }
        break;
      }
      case 'invoice.payment_failed': {
        const invoice = event.data.object;
        const user = await User.findOne({ stripeCustomerId: invoice.customer });
        if (user) console.log(`Payment failed for user ${user.email}`);
        break;
      }
    }
    res.json({ received: true });
  } catch (err) { console.error('Webhook handler error:', err); res.status(500).json({ error: err.message }); }
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
    res.json({ success: true, exists: true, slots, pending: slots.some(s => s.status === 'pending'), videosPosted, calendar: doc });
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

    let [slots, allTimePosted] = await Promise.all([
      slotsCol.find({ userId, date: today }).sort({ videoIndex: 1 }).toArray(),
      slotsCol.countDocuments({ userId, posted: true }),
    ]);

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

    slots = await slotsCol
      .find({ userId, date: today })
      .sort({ videoIndex: 1 })
      .toArray();
    slots.forEach(s => { if (s._id) s._id = s._id.toString(); });

    res.json({
      success:       true,
      slots,
      today,
      fromCache:     false,
      generatedAt:   slots[0]?.generatedAt || new Date().toISOString(),
      count:         slots.length,
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
      .find({ userId: req.user.id, status: 'pending', scheduledDate: { $gte: today } })
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
          status:            'pending',
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
          status:            'pending',
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
      { $set: { 'slots.$[s].status': 'skipped', 'slots.$[s].skippedAt': new Date().toISOString() } },
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

          // Search for channels in this niche
          const searchParams = new URLSearchParams({
            part: 'snippet', type: 'channel',
            q: `top ${nicheName} YouTube channels`,
            maxResults: '10', key: apiKey,
          });
          const searchRes  = await fetch(`https://www.googleapis.com/youtube/v3/search?${searchParams}`);
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
    const YT_CHANNELS = 'https://www.googleapis.com/youtube/v3/channels';
    const YT_SEARCH   = 'https://www.googleapis.com/youtube/v3/search';

    // Fetch channel by direct params; returns null if not found, throws on API error
    const fetchChannel = async (params) => {
      const r = await fetch(`${YT_CHANNELS}?${new URLSearchParams({ part: 'snippet,statistics', key: apiKey, ...params })}`);
      if (!r.ok) { const e = new Error(`YouTube API error (${r.status})`); e.status = 502; throw e; }
      return (await r.json()).items?.[0] || null;
    };

    // Search by name then fetch the top result's channel
    const searchThenFetch = async (query) => {
      const r = await fetch(`${YT_SEARCH}?${new URLSearchParams({ part: 'snippet', q: query, type: 'channel', maxResults: '1', key: apiKey })}`);
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

      for (const slot of missing) {
        try {
          console.log(`[Scripts] Generating structured script for "${slot.title}" (day ${slot.day})…`);
          const structured = await generateStructuredScript(slot.title, nicheName, slot.type, openai);
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
      { $set: { status: 'skipped', skippedAt: new Date().toISOString() } }
    );
    res.json({ success: true, message: 'Video skipped and removed from queue' });
  } catch (err) {
    console.error('[Preview] POST /skip error:', err);
    res.status(500).json({ error: 'Failed to skip video' });
  }
});

// ── ANALYTICS ────────────────────────────────────────────────────────────────

// Shared helper — fetches live YouTube Analytics for a user and caches in MongoDB for 30 min.
// Returns the full analytics payload or null on total failure.
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

  // Refresh token if needed
  const probe = await fetch(
    `https://www.googleapis.com/youtube/v3/channels?part=id&mine=true`,
    { headers: { Authorization: `Bearer ${ch.accessToken}` } }
  );
  let accessToken = ch.accessToken;
  if (probe.status === 401) {
    const refreshToken = ch.refreshToken || user.googleRefreshToken;
    if (refreshToken) {
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
        accessToken = tokData.access_token;
        await User.updateOne(
          { 'youtubeChannels.channelId': ch.channelId },
          { $set: { 'youtubeChannels.$.accessToken': accessToken } }
        ).catch(() => {});
        console.log(`[Analytics] Token refreshed for channel ${ch.channelId}`);
      }
    }
  }

  const auth   = { Authorization: `Bearer ${accessToken}` };
  const apiKey = process.env.YOUTUBE_API_KEY ? `&key=${process.env.YOUTUBE_API_KEY}` : '';

  // 1. Channel-level statistics
  const chanRes   = await fetch(
    `https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${ch.channelId}${apiKey}`,
    { headers: auth }
  );
  const chanData  = await chanRes.json();
  const chanStats = chanData?.items?.[0]?.statistics || {};

  // 2. Top videos in last 30 days
  const publishedAfter = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const searchRes = await fetch(
    `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${ch.channelId}&type=video&order=viewCount&publishedAfter=${encodeURIComponent(publishedAfter)}&maxResults=10${apiKey}`,
    { headers: auth }
  );
  const searchData = await searchRes.json();
  const videoIds   = (searchData?.items || []).map(v => v.id?.videoId).filter(Boolean);

  let topVideos = [];
  if (videoIds.length) {
    const vidRes  = await fetch(
      `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id=${videoIds.join(',')}${apiKey}`,
      { headers: auth }
    );
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
  }

  // 3. YouTube Analytics API — 30-day daily breakdown (requires yt-analytics scope)
  let dailyViews      = [];
  let watchTimeMinutes  = null;
  let subscribersGained = null;
  try {
    const endDate   = new Date().toISOString().slice(0, 10);
    const startDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const analyticsRes = await fetch(
      `https://youtubeanalytics.googleapis.com/v2/reports?ids=channel%3D%3D${ch.channelId}&startDate=${startDate}&endDate=${endDate}&metrics=views,estimatedMinutesWatched,subscribersGained&dimensions=day&sort=day`,
      { headers: auth }
    );
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

  // Videos posted — count directly from calendar_slots (authoritative source)
  const slotsCol   = agentCol('calendar_slots');
  const videosPosted = await slotsCol.countDocuments({ userId: String(userId), posted: true }).catch(() => 0);

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
    if (!data) return res.status(500).json({ error: 'Analytics unavailable' });
    res.json(data);
  } catch (err) {
    console.error('[Analytics] GET /api/analytics error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/dashboard/stats — condensed stat-card data for the dashboard (same 30-min cache)
app.get('/api/dashboard/stats', requireAuth, async (req, res) => {
  try {
    const data = await fetchLiveAnalytics(req.user.id);
    if (!data) return res.status(500).json({ error: 'Stats unavailable' });
    if (data.noChannel) return res.json({ success: true, noChannel: true });
    res.json({
      success:     true,
      channelName: data.channelName,
      channelId:   data.channelId,
      stats:       data.stats,
      topVideo:    (data.topVideos || [])[0] || null,
      fromCache:   data.fromCache || false,
    });
  } catch (err) {
    console.error('[Dashboard] GET /api/dashboard/stats error:', err.message);
    res.status(500).json({ error: err.message });
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

// GET /api/quality/scores — returns AI-generated quality scores for this user's videos today
app.get('/api/quality/scores', requireAuth, async (req, res) => {
  try {
    const today  = new Date().toISOString().slice(0, 10);
    const col    = agentCol('quality_scores');
    const scores = await col
      .find({ userId: req.user.id, $or: [{ scheduledDate: today }, { date: today }] })
      .sort({ scoredAt: -1 })
      .toArray();
    scores.forEach(s => { s._id = s._id.toString(); });

    const total  = scores.length;
    const avg    = total ? Math.round(scores.reduce((a, s) => a + (s.scores?.overall || 0), 0) / total) : null;
    const passed = scores.filter(s => (s.scores?.overall || 0) >= 60).length;
    const failed = total - passed;

    res.json({ success: true, scores, stats: { total, avg, passed, failed }, today });
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

// Step 1 — Generate video script via OpenAI
// Step 1 — Generate viral-optimised script via OpenAI
// Returns { title, script, hook, loopEnding, captions, hashtags, wordCount }
// Shorts: structured JSON with hook rules, 60-80 word cap, caption segments, hashtags
// Long-form: plain text wrapped in the same shape for a consistent call site
async function pipelineGenerateScript(title, nicheName, type) {
  const { OpenAI } = require('openai');
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  if (type === 'Long-form') {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: `Write a 3–5 minute YouTube script for: "${title}".\nNiche: ${nicheName}. Natural spoken language only — no stage directions, no emojis, no markdown.\nStart with a strong hook. End with a clear call-to-action.` }],
      temperature: 0.8,
    });
    const inTok  = completion.usage?.prompt_tokens    || 0;
    const outTok = completion.usage?.completion_tokens || 0;
    logAPIUsage('openai', 'script_longform', null, inTok + outTok,
      inTok * API_COSTS.openai_input + outTok * API_COSTS.openai_output, true);
    const script = completion.choices[0].message.content.trim();
    return { title, script, hook: '', loopEnding: '', captions: [], hashtags: [], wordCount: script.split(/\s+/).length };
  }

  // ── Shorts: viral-optimised structured JSON ──
  const prompt = `You are a viral YouTube Shorts script writer for a "${nicheName}" channel.
Write a viral-optimised Shorts script for the video: "${title}"

STRICT RULES:
1. HOOK (first 3 seconds): Must use ONE of: a shocking fact ("Did you know…"), a bold claim ("Most people get this wrong…"), a direct question ("What if you could…"), or a number ("3 things that…"). Hook must be under 15 words.
2. LENGTH: Script must be 60-80 words total (hard limit — 20-30 seconds of speech). If you reach 100 words, stop.
3. STRUCTURE: Hook (3 sec) → 3 rapid value points (15 sec) → Loop ending (5 sec). The LAST sentence MUST echo or reference the hook to create a rewatch loop.
4. STYLE: Short punchy sentences only. Zero filler words (no "basically", "actually", "you know"). Every sentence adds new information.
5. TITLE: Generate a viral title using power words. Format: "[Number] [Topic] That [Surprising Outcome]" OR "Why [Common Belief] Is Wrong" OR "The [Topic] Secret Nobody Tells You".
6. HASHTAGS: Generate exactly 5 hashtags relevant to the ${nicheName} niche.
7. CAPTIONS: Split the full script into caption segments — each segment MAX 4 words, estimated timestamps at ~2.5 words/sec pace.

Return valid JSON only — no prose, no markdown:
{
  "title": "viral title here",
  "script": "full spoken script, 60-80 words, no stage directions",
  "wordCount": 72,
  "hook": "the opening hook sentence only",
  "loopEnding": "the last sentence that echoes the hook",
  "hashtags": ["#tag1", "#tag2", "#tag3", "#tag4", "#tag5"],
  "captions": [
    {"text": "Did you know", "start": 0, "end": 1.6},
    {"text": "most people fail", "start": 1.6, "end": 3.0}
  ]
}`;

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
      logAPIUsage('openai', 'script_short', null, inTok2 + outTok2,
        inTok2 * API_COSTS.openai_input + outTok2 * API_COSTS.openai_output, wordCount <= 100);
      console.log(`[Script] Attempt ${attempt}/3 — ${wordCount} words`);
      if (wordCount <= 100) { best = { ...parsed, wordCount }; break; }
      console.warn(`[Script] ${wordCount} words exceeds 100 — regenerating`);
    } catch (err) {
      logAPIUsage('openai', 'script_short', null, 0, 0, false);
      console.warn(`[Script] Attempt ${attempt}/3 failed: ${err.message}`);
    }
  }
  if (!best) throw new Error('Script generation failed to produce a sub-100-word script after 3 attempts');

  return {
    title:      best.title      || title,
    script:     best.script     || '',
    hook:       best.hook       || '',
    loopEnding: best.loopEnding || '',
    captions:   Array.isArray(best.captions)  ? best.captions.slice(0, 200)  : [],
    hashtags:   Array.isArray(best.hashtags)  ? best.hashtags.slice(0, 5)    : [],
    wordCount:  best.wordCount  || 0,
  };
}

// Structured script generator — returns JSON with hook / mainPoints / cta / estimatedDuration / fullScript
async function generateStructuredScript(title, nicheName, type, openai) {
  const isLong = type === 'Long-form';
  const res = await openai.chat.completions.create({
    model: 'gpt-4o-mini',
    messages: [{
      role: 'user',
      content: `You are a YouTube ${isLong ? 'long-form' : 'Shorts'} script strategist for a "${nicheName}" channel.
Write a structured script plan for the video titled: "${title}"

Return valid JSON with exactly these fields:
{
  "hook": "Opening 1-2 sentences for the first 3 seconds — must immediately grab attention",
  "mainPoints": ["key point 1", "key point 2", "key point 3", "key point 4", "key point 5"],
  "cta": "Call-to-action at the end (1-2 sentences)",
  "estimatedDuration": "${isLong ? '4:30' : '0:52'}",
  "fullScript": "Complete natural spoken script (${isLong ? '3-5 minutes' : '45-60 seconds'}), no stage directions"
}`,
    }],
    temperature: 0.78,
    response_format: { type: 'json_object' },
  });
  return JSON.parse(res.choices[0].message.content);
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
  if (!apiKey) throw new Error('GOOGLE_TTS_API_KEY not configured — add it in Railway env vars');

  const textToSpeech = require('@google-cloud/text-to-speech');
  const client = new textToSpeech.TextToSpeechClient({ apiKey });

  // Wrap each word in an SSML <mark> so TTS returns per-word timepoints
  const escSSML = w => w.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const words = script.slice(0, 5000).split(/\s+/).filter(Boolean);
  const ssml  = '<speak>' + words.map((w, i) => `<mark name="w${i}"/>${escSSML(w)}`).join(' ') + '</speak>';

  const request = {
    input: { ssml },
    voice: { languageCode: 'en-US', name: 'en-US-Journey-F', ssmlGender: 'FEMALE' },
    audioConfig: { audioEncoding: 'MP3', speakingRate: 1.1, pitch: 0, enableWordTimeOffsets: true },
  };

  let lastErr;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const [response] = await client.synthesizeSpeech(request);
      const audioPath = `/tmp/vly_voiceover_${userId}_${Date.now()}.mp3`;
      require('fs').writeFileSync(audioPath, response.audioContent, 'binary');

      const timepoints = Array.isArray(response.timepoints) ? response.timepoints : [];
      let captions;
      if (timepoints.length >= 3) {
        captions = buildCaptionsFromTimepoints(words, timepoints);
        console.log(`[Voiceover] ✓ ${audioPath} — ${timepoints.length} word marks → ${captions.length} caption segments`);
      } else {
        const dur = await getAudioDurationSec(audioPath);
        captions  = buildFallbackCaptions(script.slice(0, 5000), dur);
        console.log(`[Voiceover] ✓ ${audioPath} — no timepoints, fallback ${dur.toFixed(1)}s → ${captions.length} segments`);
      }
      return { audioPath, captions };
    } catch (err) {
      lastErr = err;
      console.warn(`[Voiceover] Attempt ${attempt}/3 failed: ${err.message}`);
      if (attempt < 3) await new Promise(r => setTimeout(r, 2000));
    }
  }
  throw new Error(`Voiceover failed after 3 attempts: ${lastErr.message}`);
}

// Royalty-free ambient/upbeat tracks for background music (mixed at 15%)
const ROYALTY_FREE_MUSIC = [
  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3',
  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-2.mp3',
  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-3.mp3',
  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-4.mp3',
  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-5.mp3',
  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-6.mp3',
  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-7.mp3',
  'https://www.soundhelix.com/examples/mp3/SoundHelix-Song-8.mp3',
];

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
    if (chosen.length >= 5) break;
  }

  // Pad with generic fallbacks if fewer than 5
  const generic = [
    'productive morning','focused mindset','success journey','bright future','motivated action',
    'hands typing laptop','sunrise landscape','person walking city','calm nature scene','team collaboration',
  ];
  for (const g of generic) {
    if (chosen.length >= 5) break;
    if (!chosenSet.has(g) && !excludeQueries.has(g)) { chosenSet.add(g); chosen.push(g); }
  }

  return chosen.slice(0, 5);
}

// Step 3 — Fetch 5 unique portrait clips from Pexels, excluding previously used video IDs.
// Paginates up to 3 pages per query until 5 unused clips are found.
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
// Splits into 4-word chunks and distributes timestamps evenly across totalDuration seconds.
function buildFallbackCaptions(scriptText, totalDuration) {
  const words  = String(scriptText || '').trim().split(/\s+/).filter(Boolean);
  const chunks = [];
  for (let i = 0; i < words.length; i += 4) chunks.push(words.slice(i, i + 4).join(' '));
  if (!chunks.length) return [];
  const segDur = totalDuration / chunks.length;
  return chunks.map((text, i) => ({
    text,
    start: parseFloat((i * segDur).toFixed(2)),
    end:   parseFloat(((i + 1) * segDur).toFixed(2)),
  }));
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

// Step 4 — Assemble: concat clips, burn-in SRT subtitles (Shorts only), mix voiceover + background music
// If the subtitles filter fails (e.g. libass not available), retries without subtitles.
async function pipelineAssembleVideo(footageClips, audioPath, outputPath, captions = [], isShort = true) {
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

  // Download background music (non-fatal)
  const musicUrl  = ROYALTY_FREE_MUSIC[Math.floor(Math.random() * ROYALTY_FREE_MUSIC.length)];
  const musicPath = path.join('/tmp', `vly_music_${runId}.mp3`);
  let hasMus = false;
  try {
    await new Promise((resolve, reject) => {
      exec(`curl -sL --max-time 30 -o "${musicPath}" "${musicUrl}"`, { timeout: 45000 },
        err => err ? reject(err) : resolve());
    });
    hasMus = fs.existsSync(musicPath) && fs.statSync(musicPath).size > 1000;
  } catch (e) {
    console.warn('[Assembly] Music download failed (non-fatal):', e.message);
  }

  const n       = clipPaths.length;
  const clipDur = 6;

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

  // Generate SRT file for Shorts captions (no escaping needed — it's a plain text file)
  let srtPath = null;
  if (isShort && captions.length > 0) {
    srtPath = generateSRTFile(captions, runId);
    console.log(`[Assembly] SRT written: ${srtPath} (${captions.length} segments)`);
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
    console.log(`[Assembly] ffmpeg [${label}]: ${n} clips, filter_len=${filterStr.length}`);
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
    if (srtPath) fs.unlink(srtPath, () => {});
    for (const p of clipPaths) fs.unlink(p, () => {});
    if (hasMus) fs.unlink(musicPath, () => {});
  };

  try {
    // First attempt: with SRT subtitles filter (Shorts only)
    if (srtPath) {
      const subsFilter = `[vcat]subtitles='${srtPath}':force_style='FontSize=18\\,PrimaryColour=&Hffffff\\,OutlineColour=&H000000\\,Outline=2\\,Alignment=2'[vout]`;
      const filterWithSubs = [...baseParts, subsFilter, audioFilter].join(';');
      try {
        await runFFmpeg(filterWithSubs, 'subs');
        cleanup();
        console.log(`[Assembly] ✓ ${outputPath} (with subtitles)`);
        return outputPath;
      } catch (subsErr) {
        console.warn(`[Assembly] Subtitles pass failed — retrying without: ${subsErr.message.slice(0, 150)}`);
      }
    }

    // Fallback or Long-form: no subtitles
    const filterNoSubs = [...baseParts, '[vcat]null[vout]', audioFilter].join(';');
    await runFFmpeg(filterNoSubs, 'nosubs');
    cleanup();
    console.log(`[Assembly] ✓ ${outputPath}${isShort ? ' (subtitles skipped — fallback)' : ' (Long-form)'}`);
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
async function pipelineUploadToYouTube(videoPath, title, description, channel, isShort = true) {
  const { google } = require('googleapis');
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
      media: { body: require('fs').createReadStream(videoPath) },
    });
    return res.data.id;
  };

  try {
    const videoId = await tryUpload(channel.accessToken);
    logAPIUsage('youtube', 'video_upload', null, API_COSTS.youtube_upload, 0, true);
    return videoId;
  } catch (err) {
    if (err.code === 401 || err.status === 401 || /invalid_grant|token/.test(err.message)) {
      console.log(`[Pipeline] Token expired for ${channel.channelId} — refreshing`);
      const newToken = await pipelineRefreshToken(channel);
      channel.accessToken = newToken;
      try {
        const videoId = await tryUpload(newToken);
        logAPIUsage('youtube', 'video_upload', null, API_COSTS.youtube_upload, 0, true);
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
    const statsRes = await yt.videos.list({ part: ['statistics'], id: videoIds });
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

  try {
    // 1/3 Script
    console.log(`[Pipeline] 1/3 Script — "${slot.title}" (${nicheName})`);
    const scriptData = await pipelineGenerateScript(slot.title, nicheName, slot.type);
    const script     = scriptData.script;
    console.log(`[Pipeline] 1/3 Done — ${scriptData.wordCount} words, ${scriptData.captions.length} captions`);

    agentCol('scripts').updateOne(
      { userId: calendar.userId, slotDay: slot.day, videoIndex: slot.videoIndex || 1 },
      { $set: {
        userId: calendar.userId, calendarId: calendar._id.toString(),
        slotDay: slot.day, videoIndex: slot.videoIndex || 1,
        title: scriptData.title, hook: scriptData.hook, loopEnding: scriptData.loopEnding,
        captions: scriptData.captions, hashtags: scriptData.hashtags,
        wordCount: scriptData.wordCount, fullScript: script, updatedAt: new Date().toISOString(),
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
      }},
      { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
    ).catch(() => {});

    // 2/3 Voiceover — validate API and capture word-timestamp captions; discard audio (regenerated at posting time)
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
    const scriptText = slot.scriptText || slot.cachedScript;
    if (!scriptText) throw new Error('No cached script text — run the pipeline first');

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

    // Assemble
    console.log(`[AssembleUpload] 3/3 Assembling "${slot.title}" → ${outPath}`);
    await pipelineAssembleVideo(footageClips, audioPath, outPath, captions, isShort);

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
  await col.updateOne(
    { _id: calendar._id },
    { $set: {
      [`slots.$[s].status`]:         'failed',
      [`slots.$[s].pipelineStatus`]: 'failed',
      [`slots.$[s].pipelineError`]:  lastErr.message.slice(0, 500),
      [`slots.$[s].failedAt`]:       new Date().toISOString(),
    }},
    { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
  ).catch(() => {});
  console.error(`[Pipeline] ✗✗ Permanently failed "${slot.title}" after ${maxRetries} attempts — skipping`);
}

// Generates today's video slots for every active user who has a niche + plan + channel,
// then immediately fires the production pipeline for each slot in the background.
// Idempotent: skips users who already have today's slots. Called by the 00:01 cron and startup check.
async function runDailySlotGeneration(targetUserId = null, targetDate = null) {
  if (!process.env.OPENAI_API_KEY) {
    console.log('[DailyGen] Skipping — OPENAI_API_KEY not configured'); return;
  }
  const today    = targetDate || new Date().toISOString().slice(0, 10);
  const calCol   = agentCol('content_calendars');
  const slotsCol = agentCol('calendar_slots');
  const { OpenAI } = require('openai');
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const calQuery  = targetUserId ? { status: 'active', userId: targetUserId } : { status: 'active' };
  const calendars = await calCol.find(calQuery).toArray();
  let generated = 0;

  for (const calendar of calendars) {
    try {
      // Idempotency: check the calendar_slots collection — the authoritative store.
      // Never regenerate if docs already exist for this user+date (posted or not).
      const existingCount = await slotsCol.countDocuments({ userId: String(calendar.userId), date: today });
      if (existingCount > 0) {
        console.log(`[DailyGen] ${existingCount} slot(s) for ${today} already in DB (user ${calendar.userId}) — skipping`);
        continue;
      }
      const user = await User.findById(calendar.userId).catch(() => null);
      if (!user) continue;
      const config    = PLAN_CONFIG[user.plan] || PLAN_CONFIG.trial;
      const nicheName = calendar.nicheName || user.nicheName || '';
      if (!nicheName) { console.warn(`[DailyGen] No niche for user ${calendar.userId} — skipping`); continue; }

      const count  = config.shortsPerDay;
      const yesterdayCtx = await getYesterdayPerformance(String(calendar.userId), calendar.channelId || '');
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
      logAPIUsage('openai', 'daily_title_gen', String(calendar.userId), dg_in + dg_out,
        dg_in * API_COSTS.openai_input + dg_out * API_COSTS.openai_output, true);
      let titles = [];
      try { titles = JSON.parse(genRes.choices[0].message.content).titles || []; } catch {}

      const generatedAt = new Date().toISOString();
      const newSlots = [];
      for (let v = 0; v < count; v++) {
        newSlots.push({
          userId: String(calendar.userId), channelId: calendar.channelId || '', nicheName,
          day: 1, date: today,
          title: titles[v] || `${nicheName} Short #${v + 1}`,
          type: 'Short', videoIndex: v + 1, totalForDay: count, angle: 'short',
          status: 'pending', posted: false, retryCount: 0,
          scheduledPostTime: assignPostingTime(today, v + 1, count),
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
        logAPIUsage('openai', 'daily_lf_title_gen', String(calendar.userId), lf_in + lf_out,
          lf_in * API_COSTS.openai_input + lf_out * API_COSTS.openai_output, true);
        let lfTitle = `Deep Dive: ${nicheName}`;
        try { lfTitle = JSON.parse(lfRes.choices[0].message.content).title || lfTitle; } catch {}
        newSlots.push({
          userId: String(calendar.userId), channelId: calendar.channelId || '', nicheName,
          day: 1, date: today, title: lfTitle,
          type: 'Long-form', videoIndex: count + 1, totalForDay: 1, angle: 'long-form',
          status: 'pending', posted: false, retryCount: 0,
          scheduledPostTime: assignPostingTime(today, 1, 1),
          generatedAt,
        });
      }

      await slotsCol.deleteMany({ userId: String(calendar.userId), date: today, posted: { $ne: true } });
      if (newSlots.length) await slotsCol.insertMany(newSlots);

      const kept   = (calendar.slots || []).filter(s => s.date !== today || s.posted);
      const merged = [...kept, ...newSlots].sort((a, b) =>
        (a.date || '') < (b.date || '') ? -1 : (a.date || '') > (b.date || '') ? 1 : (a.videoIndex || 0) - (b.videoIndex || 0)
      );
      await calCol.updateOne({ _id: calendar._id }, { $set: { slots: merged } });
      generated++;
      console.log(`[DailyGen] ✓ ${newSlots.length} slot(s) for ${today} | user ${calendar.userId} | plan: ${user.plan}`);
      console.log(`[DailyGen] ${newSlots.length} slot(s) saved — PostingCron will start pipeline 30 min before each scheduledPostTime`);
    } catch (err) {
      console.error(`[DailyGen] Failed for calendar ${calendar._id}:`, err.message);
    }
  }
  console.log(`[DailyGen] Complete — ${generated} calendar(s) had slots generated for ${today}`);
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

// Posts all due "ready" slots (autoPost users) or "approved" slots (manual-approval users).
// Only uploads pre-assembled videos — never re-generates. Runs every 5 minutes.
async function runScheduledPosting() {
  const now    = new Date().toISOString();
  const today  = now.slice(0, 10); // YYYY-MM-DD UTC — only process today's slots
  const calCol = agentCol('content_calendars');
  const fs     = require('fs');
  const calendars = await calCol.find({ status: 'active' }).toArray();
  let posted = 0;

  for (const calendar of calendars) {
    const user = await User.findById(calendar.userId).catch(() => null);
    if (!user) continue;
    const autoPostOn = user.autoPost !== false;

    const dueSlots = (calendar.slots || []).filter(s => {
      if (s.posted || s.status === 'posted') return false;
      if (s.date !== today) return false;
      const postAt = s.scheduledPostTime || `${s.date}T18:00:00`;
      if (postAt > now) return false; // never post before scheduledPostTime
      return s.status === 'ready' || s.status === 'approved';
    });

    // Start pipeline for pending slots whose scheduledPostTime is ≤30 min away (just-in-time prep)
    const in30min = new Date(Date.now() + 30 * 60 * 1000).toISOString();
    const pendingDue = (calendar.slots || []).filter(s =>
      !s.posted && s.status === 'pending' && s.date === today &&
      (s.scheduledPostTime || `${s.date}T18:00:00`) <= in30min
    );

    const channel = (user.youtubeChannels || []).find(ch => ch.channelId === calendar.channelId)
                 || (user.youtubeChannels || [])[0];
    if (!channel && !dueSlots.length && !pendingDue.length) continue;
    if (!channel) { console.warn(`[Posting] No channel for calendar ${calendar._id}`); continue; }

    // Fire pipeline in background for slots coming up within 30 min
    if (pendingDue.length > 0) {
      console.log(`[PostingCron] ${pendingDue.length} slot(s) due in ≤30 min — starting pipeline`);
      const capturedCal  = calendar;
      const capturedUser = user;
      setImmediate(async () => {
        for (const slot of pendingDue) {
          const freshCal  = await calCol.findOne({ _id: capturedCal._id });
          const freshSlot = (freshCal?.slots || []).find(s => s.day === slot.day && (s.videoIndex || 1) === (slot.videoIndex || 1));
          if (!freshSlot || freshSlot.status !== 'pending') continue;
          console.log(`[PostingCron] Pipeline starting for "${freshSlot.title}" (due ${freshSlot.scheduledPostTime || 'TBD'})`);
          await runProductionPipelineWithRetry(freshCal, freshSlot, capturedUser, calCol);
        }
      });
    }

    if (!dueSlots.length) continue;

    for (const slot of dueSlots) {
      const hasFile = slot.assembledPath && fs.existsSync(slot.assembledPath);
      if (!hasFile) {
        // No /tmp file (approved slot or file wiped) — assemble + upload now using cached MongoDB data
        console.log(`[PostingCron] No assembled file for "${slot.title}" (status=${slot.status}) — assembling inline`);
        const capturedSlot = slot;
        setImmediate(async () => {
          try {
            await runAssembleAndUpload(calendar, capturedSlot, user, calCol, channel);
            setImmediate(() => maybePreGenerateTomorrow(String(calendar.userId)).catch(() => {}));
          } catch (e) {
            console.error(`[PostingCron] ✗ Assemble+upload failed for "${capturedSlot.title}": ${e.message}`);
          }
        });
        continue;
      }
      // File exists — upload directly
      try {
        const description = slot.cachedScript || slot.title;
        const ytId = await pipelineUploadToYouTube(slot.assembledPath, slot.title, description, channel, slot.type !== 'Long-form');
        const postedAt = new Date().toISOString();
        await calCol.updateOne(
          { _id: calendar._id },
          { $set: {
            'slots.$[s].status':         'posted',
            'slots.$[s].posted':         true,
            'slots.$[s].postedAt':       postedAt,
            'slots.$[s].youtubeVideoId': ytId,
            'slots.$[s].pipelineStatus': 'posted',
          }},
          { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
        );
        posted++;
        fs.unlink(slot.assembledPath, () => {});
        console.log(`[Posting] ✓ "${slot.title}" → https://youtu.be/${ytId}`);
        setImmediate(() => maybePreGenerateTomorrow(String(calendar.userId)).catch(() => {}));
      } catch (err) {
        console.error(`[Posting] ✗ Upload failed for "${slot.title}": ${err.message}`);
        await calCol.updateOne(
          { _id: calendar._id },
          { $set: {
            'slots.$[s].pipelineStatus': 'upload-failed',
            'slots.$[s].pipelineError':  err.message.slice(0, 300),
          }},
          { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
        ).catch(() => {});
      }
    }
  }
  if (posted > 0) {
    console.log(`[Posting] Cycle complete — ${posted} video(s) posted`);
  } else {
    // Log the next upcoming post time so the Railway logs are easy to read
    let nextPostAt = null;
    for (const cal of calendars) {
      for (const s of (cal.slots || [])) {
        if (s.posted || s.status === 'posted') continue;
        const at = s.scheduledPostTime || `${s.date}T18:00:00`;
        if (at > now && (!nextPostAt || at < nextPostAt)) nextPostAt = at;
      }
    }
    if (nextPostAt) console.log(`[PostingCron] No slots due yet — next post at ${nextPostAt}`);
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
  try {
    const publishedAfter = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(nicheName)}&order=viewCount&type=video&publishedAfter=${encodeURIComponent(publishedAfter)}&maxResults=10&key=${apiKey}`;
    const ytRes = await fetch(url);
    if (!ytRes.ok) {
      console.warn(`[TopicScout] YouTube API returned ${ytRes.status} for "${nicheName}"`);
      return [];
    }
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
    const statsRes = await yt.videos.list({ part: ['statistics'], id: videoIds });
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

// POST /api/admin/auth — exchange ADMIN_PASSWORD for a short-lived admin JWT
app.post('/api/admin/auth', async (req, res) => {
  try {
    const { password } = req.body || {};
    const adminPass  = process.env.ADMIN_PASSWORD;
    const adminEmail = process.env.ADMIN_EMAIL;
    if (!adminPass) return res.status(503).json({ error: 'Admin not configured' });

    // Verify the token from the regular user session to confirm identity
    const userToken = req.headers.authorization?.split(' ')[1];
    let callerEmail = null;
    if (userToken) {
      try { callerEmail = (jwt.verify(userToken, process.env.JWT_SECRET) || {}).email; } catch {}
      if (!callerEmail) {
        try {
          const uid = jwt.verify(userToken, process.env.JWT_SECRET).userId;
          const u   = await User.findById(uid).select('email').lean();
          callerEmail = u?.email;
        } catch {}
      }
    }
    if (adminEmail && callerEmail !== adminEmail) {
      return res.status(403).json({ error: 'Forbidden' });
    }
    if (password !== adminPass) return res.status(403).json({ error: 'Forbidden' });

    const adminJwt = jwt.sign({ isAdmin: true, email: callerEmail }, adminPass, { expiresIn: '8h' });
    res.json({ success: true, token: adminJwt, expiresIn: '8h' });
  } catch (err) {
    console.error('[Admin] Auth error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/overview — high-level platform snapshot
app.get('/api/admin/overview', requireAdmin, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const monthStart = new Date().toISOString().slice(0, 7) + '-01';
    const todayStart = new Date(today + 'T00:00:00.000Z');

    const usageCol  = agentCol('apiUsage');
    const slotsCol  = agentCol('calendar_slots');

    const [
      totalUsers,
      activeToday,
      videosPostedToday,
      costTodayDocs,
      costMonthDocs,
    ] = await Promise.all([
      User.countDocuments(),
      User.countDocuments({ updatedAt: { $gte: todayStart } }),
      slotsCol.countDocuments({ posted: true, date: today }),
      usageCol.aggregate([
        { $match: { timestamp: { $gte: today + 'T00:00:00.000Z' } } },
        { $group: { _id: null, total: { $sum: '$estimatedCost' } } },
      ]).toArray(),
      usageCol.aggregate([
        { $match: { timestamp: { $gte: monthStart + 'T00:00:00.000Z' } } },
        { $group: { _id: null, total: { $sum: '$estimatedCost' } } },
      ]).toArray(),
    ]);

    const totalCostToday      = costTodayDocs[0]?.total || 0;
    const totalCostThisMonth  = costMonthDocs[0]?.total || 0;

    const planBreakdown = await User.aggregate([
      { $group: { _id: '$plan', count: { $sum: 1 } } },
    ]);
    const perPlanBreakdown = Object.fromEntries(planBreakdown.map(p => [p._id, p.count]));

    res.json({
      success: true,
      totalUsers,
      activeToday,
      videosPostedToday,
      totalCostToday:     parseFloat(totalCostToday.toFixed(6)),
      totalCostThisMonth: parseFloat(totalCostThisMonth.toFixed(6)),
      perPlanBreakdown,
    });
  } catch (err) {
    console.error('[Admin] Overview error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/api-usage — last 24 h grouped by service
app.get('/api/admin/api-usage', requireAdmin, async (req, res) => {
  try {
    const since   = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const usageCol = agentCol('apiUsage');
    const rows = await usageCol.aggregate([
      { $match: { timestamp: { $gte: since } } },
      { $group: {
        _id:          '$service',
        calls:        { $sum: 1 },
        tokensUsed:   { $sum: '$tokensUsed' },
        totalCost:    { $sum: '$estimatedCost' },
        successCount: { $sum: { $cond: ['$success', 1, 0] } },
      }},
      { $sort: { totalCost: -1 } },
    ]).toArray();

    const result = rows.map(r => ({
      service:     r._id,
      calls:       r.calls,
      tokensUsed:  r.tokensUsed,
      totalCost:   parseFloat(r.totalCost.toFixed(6)),
      successRate: r.calls > 0 ? parseFloat((r.successCount / r.calls).toFixed(4)) : 0,
    }));

    res.json({ success: true, since, usage: result });
  } catch (err) {
    console.error('[Admin] API usage error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/youtube-quota — daily quota consumption estimate
app.get('/api/admin/youtube-quota', requireAdmin, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const usageCol = agentCol('apiUsage');
    const rows = await usageCol.aggregate([
      { $match: { service: 'youtube', timestamp: { $gte: today + 'T00:00:00.000Z' } } },
      { $group: { _id: null, used: { $sum: '$tokensUsed' } } },
    ]).toArray();

    const used        = rows[0]?.used || 0;
    const limit       = API_COSTS.youtube_daily_quota;
    const remaining   = Math.max(0, limit - used);
    const percentUsed = parseFloat(((used / limit) * 100).toFixed(2));

    // Reset at midnight UTC
    const now    = new Date();
    const resetAt = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1))
      .toISOString();

    res.json({ success: true, used, limit, remaining, percentUsed, resetAt });
  } catch (err) {
    console.error('[Admin] YouTube quota error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/users — all users with cost + activity summary, sorted by monthly cost desc
app.get('/api/admin/users', requireAdmin, async (req, res) => {
  try {
    const monthStart = new Date().toISOString().slice(0, 7) + '-01T00:00:00.000Z';
    const usageCol   = agentCol('apiUsage');
    const slotsCol   = agentCol('calendar_slots');

    const [users, costByUser] = await Promise.all([
      User.find().select('email plan createdAt updatedAt youtubeChannels').lean(),
      usageCol.aggregate([
        { $match: { timestamp: { $gte: monthStart }, userId: { $ne: null } } },
        { $group: { _id: '$userId', cost: { $sum: '$estimatedCost' } } },
      ]).toArray(),
    ]);

    const costMap = Object.fromEntries(costByUser.map(c => [c._id, c.cost]));

    const userRows = await Promise.all(users.map(async u => {
      const videosPosted = await slotsCol.countDocuments({ userId: String(u._id), posted: true }).catch(() => 0);
      return {
        email:             u.email,
        plan:              u.plan,
        videosPosted,
        apiCostThisMonth:  parseFloat((costMap[String(u._id)] || 0).toFixed(6)),
        lastActive:        u.updatedAt || u.createdAt,
        channelsCount:     (u.youtubeChannels || []).length,
      };
    }));

    userRows.sort((a, b) => b.apiCostThisMonth - a.apiCostThisMonth);
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

    // YouTube quota
    const ytRow = costRows.find(r => r._id === 'youtube');
    const ytUsed = ytRow ? await usageCol.aggregate([
      { $match: { service: 'youtube', timestamp: { $gte: today + 'T00:00:00.000Z' } } },
      { $group: { _id: null, used: { $sum: '$tokensUsed' } } },
    ]).toArray().then(r => r[0]?.used || 0) : 0;
    const quotaPct = (ytUsed / API_COSTS.youtube_daily_quota) * 100;
    if (quotaPct > 80) {
      alerts.push({ type: 'quota', severity: 'high', service: 'youtube',
        message: `YouTube quota at ${quotaPct.toFixed(1)}% (${ytUsed}/${API_COSTS.youtube_daily_quota} units)` });
    }

    res.json({ success: true, alertCount: alerts.length, alerts });
  } catch (err) {
    console.error('[Admin] Alerts error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// GET /api/admin/pipeline-stats — today's pipeline performance summary
app.get('/api/admin/pipeline-stats', requireAdmin, async (req, res) => {
  try {
    const today    = new Date().toISOString().slice(0, 10);
    const slotsCol = agentCol('calendar_slots');
    const usageCol = agentCol('apiUsage');

    const [todaySlots, failedSlots, pipelineCalls] = await Promise.all([
      slotsCol.find({ date: today }).toArray(),
      slotsCol.countDocuments({ date: today, pipelineStatus: 'failed' }),
      usageCol.find({
        service: 'openai', action: 'script_short',
        timestamp: { $gte: today + 'T00:00:00.000Z' },
      }).toArray(),
    ]);

    const videosGeneratedToday = todaySlots.filter(s => s.pipelineStatus === 'ready' || s.status === 'ready').length;
    const videosPostedToday    = todaySlots.filter(s => s.posted).length;
    const totalAttempted       = pipelineCalls.length;
    const successfulCalls      = pipelineCalls.filter(c => c.success).length;
    const successRate          = totalAttempted > 0
      ? parseFloat((successfulCalls / totalAttempted).toFixed(4)) : null;

    res.json({
      success: true,
      videosGeneratedToday,
      videosPostedToday,
      failureCount:    failedSlots,
      avgGenerationTimeMs: null, // tracked via pipelineStartedAt/previewReadyAt — reserved for future
      successRate,
    });
  } catch (err) {
    console.error('[Admin] Pipeline stats error:', err.message);
    res.status(500).json({ error: 'Internal error' });
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

  // Daily generation fallback — 00:01 UTC
  // Primary trigger is maybePreGenerateTomorrow(); this is the safety net.
  cron.schedule('1 0 * * *', async () => {
    console.log('[DailyGen] Cron fired at', new Date().toISOString());
    try { await runDailySlotGeneration(); }
    catch (err) { console.error('[DailyGen] Cron error:', err.message); }
  });

  // Analytics Collection — 4 AM daily (feature-gated)
  if (FEATURES.analyticsCollection) {
    cron.schedule('0 4 * * *', async () => {
      console.log('[Analytics] Collection running...');
      try {
        await runPython(path.join(__dirname, 'analytics_engine.py'), '--collect', 90000);
      } catch (err) { console.error('[Analytics] Error:', err); }
    });
  }

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

  cronJobsRegistered = true;
  console.log('[Cron] All cron jobs registered successfully');
}

// =============================================================================
// HEALTH CHECK — defined early above (line ~115), kept here as reference comment only
// app.get('/health', ...) — already registered at startup

// =============================================================================
// SERVE STATIC FRONTEND
// =============================================================================
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  if (req.path.startsWith('/api') || req.path.startsWith('/auth') || req.path.startsWith('/webhooks')) return res.status(404).json({ error: 'Not found' });
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// =============================================================================
// START
// =============================================================================
app.listen(PORT, () => {
  console.log(`Viralityy server running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
  // Startup slot check is triggered from the MongoDB .then() callback above,
  // so it is guaranteed to run only after the database connection is ready.
});

module.exports = app;
