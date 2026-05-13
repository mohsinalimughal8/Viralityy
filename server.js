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
const crypto         = require('crypto');
const path           = require('path');
const { exec }       = require('child_process');
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
// DATABASE
// =============================================================================
// Global handler — catches MongoServerError unhandled rejections that crash Node
process.on('unhandledRejection', (err) => {
  console.error('[UnhandledRejection] Caught — server stays alive:', err.message);
  // Do NOT exit — let Railway healthcheck keep passing
});

if (process.env.MONGODB_URI) {
  mongoose.connect(process.env.MONGODB_URI, {
    useNewUrlParser:    true,
    useUnifiedTopology: true,
    serverSelectionTimeoutMS: 5000,  // fail fast so server startup isn't blocked
  })
  .then(() => console.log('[MongoDB] Connected successfully'))
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
  youtubeChannels: [{ channelId: String, channelName: String, accessToken: String, refreshToken: String, nicheId: String, nicheName: String, paused: Boolean, tiktokEnabled: Boolean, instagramEnabled: Boolean, connectedAt: String }],
  googleAccessToken:  String,
  googleRefreshToken: String,
  nicheId:         { type: String },
  nicheName:       { type: String },
  nicheChangesUsed: { type: Number, default: 0 },
  nicheChangesYear: { type: Number, default: () => new Date().getFullYear() },
  affiliateCode:   { type: String, unique: true, sparse: true },
  referredBy:      { type: String },
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
app.get('/health', (req, res) => {
  res.json({
    status:  'ok',
    uptime:  process.uptime(),
    env:     process.env.NODE_ENV || 'development',
    mongo:   mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
    stripe:  !!process.env.STRIPE_SECRET_KEY ? 'configured' : 'not configured (billing disabled)',
    youtube: !!process.env.YOUTUBE_API_KEY   ? 'configured' : 'not configured',
    openai:  !!process.env.OPENAI_API_KEY    ? 'configured' : 'not configured',
  });
});


// =============================================================================
// PASSPORT — GOOGLE OAUTH
// =============================================================================
const GoogleStrategy = require('passport-google-oauth20').Strategy;

passport.use(new GoogleStrategy({
  clientID:     process.env.YOUTUBE_CLIENT_ID,
  clientSecret: process.env.YOUTUBE_CLIENT_SECRET,
  callbackURL:  `${process.env.APP_URL}/auth/google/callback`,
  scope: ['profile', 'email', 'https://www.googleapis.com/auth/youtube', 'https://www.googleapis.com/auth/youtube.upload'],
}, async (accessToken, refreshToken, profile, done) => {
  try {
    // 1. Find or create user
    let user = await User.findOne({ googleId: profile.id });
    if (!user) {
      user = await User.findOne({ email: profile.emails[0].value });
      if (user) { user.googleId = profile.id; }
    }
    if (!user) {
      user = await User.create({
        name:           profile.displayName,
        email:          profile.emails[0].value,
        googleId:       profile.id,
        plan:           'trial',
        trialStartedAt: new Date(),
        trialEndsAt:    new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
        youtubeChannels: [],
      });
    }

    // 2. Save OAuth tokens on user
    user.googleAccessToken  = accessToken;
    user.googleRefreshToken = refreshToken || user.googleRefreshToken;

    // 3. Fetch their YouTube channel via YouTube Data API
    try {
      const ytRes = await fetch(
        'https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true',
        { headers: { Authorization: `Bearer ${accessToken}` } }
      );
      const ytData = await ytRes.json();
      const ytChannel = ytData?.items?.[0];

      if (ytChannel) {
        const channelId   = ytChannel.id;
        const channelName = ytChannel.snippet?.title || 'My Channel';

        // Add channel if not already connected
        const already = (user.youtubeChannels || []).find(c => c.channelId === channelId);
        if (!already) {
          if (!user.youtubeChannels) user.youtubeChannels = [];
          user.youtubeChannels.push({
            channelId,
            channelName,
            accessToken:       accessToken,
            refreshToken:      refreshToken || '',
            tiktokEnabled:     false,
            instagramEnabled:  false,
            connectedAt:       new Date().toISOString(),
          });
          console.log(`[OAuth] Channel auto-connected: ${channelName} (${channelId}) for user ${user.email}`);
        } else {
          // Update tokens on existing channel
          already.accessToken  = accessToken;
          already.refreshToken = refreshToken || already.refreshToken;
          console.log(`[OAuth] Tokens refreshed for channel: ${channelName}`);
        }
        // Flag for frontend to know a channel was just connected
        user._channelJustConnected = { channelId, channelName };
      }
    } catch (ytErr) {
      console.warn('[OAuth] YouTube channel fetch failed:', ytErr.message);
      // Not fatal — user still signs in, they can connect channel manually
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
app.get('/auth/google', passport.authenticate('google', {
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

    const userData = {
      id:          req.user.id,
      email:       req.user.email || '',
      name:        req.user.name  || '',
      plan:        req.user.plan  || 'trial',
      nicheId:     (req.user.youtubeChannels || [])[0]?.nicheId || null,
      hasChannels: (req.user.youtubeChannels || []).length > 0,
      channels:    (req.user.youtubeChannels || []).map(c => ({
        channelId:   c.channelId,
        channelName: c.channelName,
      })),
      // Tell the frontend if a channel was just connected in this OAuth flow
      channelJustConnected: req.user._channelJustConnected || null,
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
// VIRALITYY — Niche Engine v2 API Routes
// =============================================================================
// HOW TO ADD:
//   1. Replace niche_engine_bridge.js with niche_engine_v2_bridge.js
//   2. Replace niche_engine.py with niche_engine_v2.py
//   3. Paste this file's contents into server.js INSTEAD OF server_m4a_routes.js
//      (this file is a full replacement — it keeps all original routes and adds new ones)
//   4. Add YOUTUBE_API_KEY to your Railway env vars to enable live scoring
//
// All existing routes (/recommend, /:nicheId, /categories, /compare, /select)
// are preserved unchanged. Two new routes are added at the bottom.
// =============================================================================

// NicheEngineV2 instantiated above in ROUTE FILES section

// ---------------------------------------------------------------------------
// GET /api/niches/recommend
// Ranked niche list for the logged-in user's plan (Layer 1 — offline)
// ---------------------------------------------------------------------------
async function searchNichesParallel(keywords, plan) {
  const results = await Promise.all(
    keywords.map(kw =>
      nicheEngine.search(kw, plan)
        .then(r => r ? [r] : [])
        .catch(() => [])
    )
  );
  const seen = new Set();
  const niches = [];
  results.flat().forEach(n => {
    const id = n.niche_id || n.id || n.nicheId || n.label;
    if (id && !seen.has(id)) { seen.add(id); niches.push(n); }
  });
  niches.sort((a, b) => (b.combined_score || b.score || 0) - (a.combined_score || a.score || 0));
  return niches;
}

app.get('/api/niches/recommend', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const plan = user.plan || 'shorts_starter';
    const KEYWORDS = [
      'psychology', 'personal finance', 'stoicism', 'ai tools', 'fitness',
      'mental health', 'history facts', 'book summaries', 'investing', 'home fitness',
      'motivation', 'productivity', 'self improvement', 'mindset', 'money tips',
      'entrepreneurship', 'weight loss', 'relationships', 'life hacks', 'crypto',
    ];

    const allNiches = await searchNichesParallel(KEYWORDS, plan);
    const niches = allNiches.slice(0, 10);
    res.json({ success: true, niches, count: niches.length });
  } catch (err) {
    console.error('[NicheV2] /api/niches/recommend error:', err);
    res.status(500).json({ error: 'Failed to fetch niche recommendations' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/niches/categories
// All available categories (Layer 1)
// ---------------------------------------------------------------------------
app.get('/api/niches/categories', requireAuth, (req, res) => {
  try {
    const categories = nicheEngine.getCategories();
    res.json({ success: true, categories });
  } catch (err) {
    console.error('[NicheV2] /api/niches/categories error:', err);
    res.status(500).json({ error: 'Failed to fetch categories' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/compare
// Compare multiple niches (Layer 1)
// Body: { nicheIds: ["psychology_facts", "ai_tools"] }
// ---------------------------------------------------------------------------
app.post('/api/niches/compare', requireAuth, async (req, res) => {
  try {
    const { nicheIds } = req.body;
    if (!Array.isArray(nicheIds) || nicheIds.length < 2) {
      return res.status(400).json({ error: 'Provide at least 2 niche IDs to compare' });
    }
    if (nicheIds.length > 5) {
      return res.status(400).json({ error: 'Maximum 5 niches can be compared at once' });
    }
    const results = nicheEngine.compare(nicheIds);
    res.json({ success: true, comparison: results });
  } catch (err) {
    console.error('[NicheV2] /api/niches/compare error:', err);
    res.status(500).json({ error: 'Failed to compare niches' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/select
// User saves a niche to their channel config (Layer 1)
// Body: { channelIndex: 0, nicheId: "psychology_facts" }
// ---------------------------------------------------------------------------
app.post('/api/niches/select', requireAuth, async (req, res) => {
  try {
    const { channelIndex, nicheId } = req.body;
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const detail = nicheEngine.getNicheDetail(nicheId);
    if (detail.error) return res.status(404).json({ error: 'Niche not found' });

    if (!user.channels) user.channels = [];
    if (!user.channels[channelIndex]) user.channels[channelIndex] = {};
    user.channels[channelIndex].niche      = nicheId;
    user.channels[channelIndex].nicheLabel = detail.label;
    user.channels[channelIndex].nicheSetAt = new Date();

    user.nicheId = nicheId;
    if (user.youtubeChannels && user.youtubeChannels.length) {
      user.youtubeChannels.forEach(ch => { ch.nicheId = nicheId; });
      user.markModified('youtubeChannels');
    }
    user.markModified('channels');
    await user.save();

    res.json({
      success: true,
      message: `Niche "${detail.label}" set for channel ${channelIndex + 1}`,
      channel: user.channels[channelIndex],
    });
  } catch (err) {
    console.error('[NicheV2] /api/niches/select error:', err);
    res.status(500).json({ error: 'Failed to save niche selection' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/niches/:nicheId
// Full detail for a single known niche (Layer 1)
// Keep AFTER /categories route to avoid "categories" being parsed as nicheId
// ---------------------------------------------------------------------------
app.get('/api/niches/:nicheId', requireAuth, async (req, res) => {
  try {
    const detail = nicheEngine.getNicheDetail(req.params.nicheId);
    if (detail.error) return res.status(404).json(detail);
    res.json({ success: true, niche: detail });
  } catch (err) {
    console.error('[NicheV2] /api/niches/:id error:', err);
    res.status(500).json({ error: 'Failed to fetch niche detail' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/analyse        ← Layer 1 + 2
// Full combined analysis for a single free-text keyword.
// Body: { keyword: "stoicism", plan: "combo_pro" }
// ---------------------------------------------------------------------------
app.post('/api/niches/analyse', requireAuth, async (req, res) => {
  try {
    const { keyword, plan: bodyPlan } = req.body;
    if (!keyword || keyword.trim().length < 2) {
      return res.status(400).json({ error: 'Provide a keyword (at least 2 characters)' });
    }
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const plan   = bodyPlan || user.plan || 'shorts_starter';
    const result = await nicheEngine.analyse(keyword.trim(), plan);
    res.json({ success: true, analysis: result });
  } catch (err) {
    console.error('[NicheV2] /api/niches/analyse error:', err);
    res.status(500).json({ error: 'Failed to analyse niche' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/search          ← NEW — keyword search (dashboard search bar)
// User types any topic keyword and gets a full scored report plus
// related search suggestions to explore sub-niches.
//
// Body: { keyword: "stoicism", plan: "combo_pro" }
//
// Returns: full NicheScore report + search_suggestions array
// ---------------------------------------------------------------------------
app.post('/api/niches/search', requireAuth, async (req, res) => {
  try {
    const { keyword, plan: bodyPlan } = req.body;
    if (!keyword || keyword.trim().length < 2) {
      return res.status(400).json({ error: 'Please enter a keyword to search (min 2 characters)' });
    }
    if (keyword.trim().length > 100) {
      return res.status(400).json({ error: 'Keyword too long (max 100 characters)' });
    }

    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const plan = bodyPlan || user.plan || 'shorts_starter';
    const kw   = keyword.trim().toLowerCase();

    // Build 4 related keywords based on the primary keyword
    const RELATED_MAP = {
      'ai':                    ['ai tools', 'artificial intelligence', 'machine learning', 'automation'],
      'psychology':            ['human behaviour', 'cognitive bias', 'social psychology', 'mindset'],
      'finance':               ['personal finance', 'investing', 'stock market', 'financial freedom'],
      'fitness':               ['home fitness', 'weight loss', 'workout', 'nutrition'],
      'history':               ['history facts', 'ancient civilizations', 'world war', 'historical events'],
      'stoicism':              ['philosophy', 'marcus aurelius', 'self improvement', 'mindfulness'],
      'business':              ['entrepreneurship', 'side hustles', 'passive income', 'startup'],
      'health':                ['mental health', 'sleep optimization', 'stress management', 'wellness'],
      'investing':             ['stock market', 'crypto', 'real estate investing', 'index funds'],
      'crypto':                ['bitcoin', 'blockchain', 'defi', 'cryptocurrency'],
    };
    const related = RELATED_MAP[kw]
      || Object.entries(RELATED_MAP).find(([k]) => kw.includes(k))?.[1]
      || [`${kw} tips`, `${kw} for beginners`, `best ${kw}`, `${kw} facts`];

    const keywords = [kw, ...related.slice(0, 4)];
    const niches   = await searchNichesParallel(keywords, plan);
    res.json({ success: true, niches, result: niches[0] || null, count: niches.length });
  } catch (err) {
    console.error('[NicheV2] /api/niches/search error:', err);
    res.status(500).json({ error: 'Failed to search niche' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/niches/discover         ← NEW — real-time trending niche discovery
// No keyword needed. Engine probes ~38 broad seed topics via YouTube API,
// scores each with the full two-layer engine, and returns the top
// high-performing niches the user didn't have to search for.
//
// Query params:
//   top       (optional) — number of results, default 10, max 20
//   min_score (optional) — minimum combined score, default 60
//   refresh   (optional) — "true" to bypass 24h cache
//
// Example: GET /api/niches/discover?top=10&min_score=65
// Example: GET /api/niches/discover?refresh=true
//
// Returns:
//   { top_niches: [...], hot_count, moderate_count, total_probed,
//     live_data_used, generated_at }
// ---------------------------------------------------------------------------
app.get('/api/niches/discover', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const plan         = user.plan || 'shorts_starter';
    const topN         = Math.min(parseInt(req.query.top)       || 10,  20);
    const minScore     = Math.min(parseFloat(req.query.min_score) || 60, 90);
    const forceRefresh = req.query.refresh === 'true';

    const result = await nicheEngine.discover(plan, topN, minScore, forceRefresh);
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[NicheV2] /api/niches/discover error:', err);
    res.status(500).json({ error: 'Failed to discover trending niches' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/analyse/batch   ← Layer 1 + 2 batch
// Analyse up to 10 keywords concurrently.
// Body: { keywords: ["stoicism", "AI tools", "book summaries"], plan: "combo_pro" }
// ---------------------------------------------------------------------------
app.post('/api/niches/analyse/batch', requireAuth, async (req, res) => {
  try {
    const { keywords, plan: bodyPlan } = req.body;

    if (!Array.isArray(keywords) || keywords.length === 0) {
      return res.status(400).json({ error: 'Provide an array of keywords' });
    }
    if (keywords.length > 10) {
      return res.status(400).json({ error: 'Maximum 10 keywords per batch' });
    }

    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const plan    = bodyPlan || user.plan || 'shorts_starter';
    const results = await nicheEngine.analyseBatch(keywords, plan);

    res.json({
      success: true,
      count:   results.length,
      plan,
      results,
    });
  } catch (err) {
    console.error('[NicheV2] /api/niches/analyse/batch error:', err);
    res.status(500).json({ error: 'Failed to run batch analysis' });
  }
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
// CRON JOBS — scheduled automation
// =============================================================================

// Daily Topic Scout — runs every day at 6 AM server time
cron.schedule('0 6 * * *', async () => {
  console.log('[Scout] Daily topic scout starting...');
  try {
    const script = path.join(__dirname, 'daily_topic_scout_agent.py');
    exec(`python3 "${script}" --run-all`, { timeout: 300000 }, (err, stdout, stderr) => {
      if (err) console.error('[Scout] Cron error:', stderr);
      else     console.log('[Scout] Cron complete:', stdout.slice(0, 200));
    });
  } catch (e) { console.error('[Scout] Cron failed:', e); }
});

// Competitor Watcher — ACTIVE (free: YouTube API + OpenAI already in stack)
if (FEATURES.competitorWatcher) {
  cron.schedule('0 */4 * * *', async () => {
    console.log('[CompetitorWatcher] Check starting...');
    try {
      const script = path.join(__dirname, 'competitor_watcher_agent.py');
      exec(`python3 "${script}" --run-all`, { timeout: 600000 }, (err, stdout, stderr) => {
        if (err) console.error('[CompetitorWatcher] Cron error:', stderr);
        else     console.log('[CompetitorWatcher] Cron complete:', stdout.slice(0, 200));
      });
    } catch (e) { console.error('[CompetitorWatcher] Cron failed:', e); }
  });
}

// Script Research Queue Processor — runs every 30 minutes
cron.schedule('*/30 * * * *', async () => {
  try {
    const script = path.join(__dirname, 'script_research_agent.py');
    exec(`python3 "${script}" --process-queue ""`, { timeout: 300000 }, (err, stdout, stderr) => {
      if (err) console.error('[ScriptResearch] Queue error:', stderr);
    });
  } catch (e) { console.error('[ScriptResearch] Queue cron failed:', e); }
});


// =============================================================================
// AGENT 1 — CONTENT PLANNER ROUTES
// =============================================================================

// GET /api/agents/planner/calendar
// Returns the active 30-day calendar for the logged-in user
app.get('/api/agents/planner/calendar', requireAuth, async (req, res) => {
  try {
    const col = await agentCol('content_calendars');
    const doc = await col.findOne({ userId: req.user.id, status: 'active' });
    if (!doc) return res.json({ success: true, slots: [], pending: false, message: 'Click Generate calendar to create your 30-day content plan' });
    doc._id = doc._id.toString();
    const slots = doc.slots || [];
    res.json({ success: true, exists: true, slots, pending: slots.some(s => s.status === 'pending'), calendar: doc });
  } catch (err) {
    console.error('[Planner] GET /calendar error:', err);
    res.status(500).json({ error: 'Failed to fetch calendar' });
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
Generate exactly ${batchCount} NEW, completely unique Short video titles (under 60 chars each).
Vary the style across: hook, story, tutorial, listicle, myth-bust.
Each title must be specific, compelling, and cover a DIFFERENT concept.
Never repeat any previously used title or concept listed above.

Return JSON: { "titles": [${batchCount} strings] }`,
        }],
        temperature: 0.9,
        response_format: { type: 'json_object' },
      });

      let parsed;
      try { parsed = JSON.parse(batchRes.choices[0].message.content); } catch { parsed = {}; }
      const batchTitles = parsed.titles || [];
      allShortTitles.push(...batchTitles);
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
          title:       allShortTitles[shortIdx] || `${nicheName} Short — Day ${day} #${v + 1}`,
          type:        'Short',
          videoIndex:  v + 1,
          totalForDay: config.shortsPerDay,
          angle:       'short',
          status:      'planned',
        });
        shortIdx++;
      }

      if (config.longFormPerWeek > 0 && dow === 1) {
        slots.push({
          day, date,
          title:       allLongFormTitles[longFormIdx] || `Deep Dive ${longFormIdx + 1}: ${nicheName}`,
          type:        'Long-form',
          videoIndex:  1,
          totalForDay: 1,
          angle:       'long-form',
          status:      'planned',
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

    res.json({ success: true, slots, count: slots.length, totalVideos: slots.length, plan: user.plan, weeklyInsights });
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

// GET /api/content/queue — pending calendar slots awaiting review
app.get('/api/content/queue', requireAuth, async (req, res) => {
  try {
    const col      = await agentCol('content_calendars');
    const calendar = await col.findOne({ userId: req.user.id, status: 'active' });
    if (!calendar) return res.json({ success: true, items: [], count: 0 });
    const items = (calendar.slots || []).filter(s => s.status === 'planned' || s.status === 'pending');
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


// =============================================================================
// CRON JOBS — Agents 5 and 8
// =============================================================================

// Trend Scraper — SHELVED (enable: FEATURE_TREND_SCRAPER=true)
if (FEATURES.trendScraper) {
  cron.schedule('0 7 * * *', async () => {
    console.log('[TrendScraper] Daily run starting...');
    try {
      const script = path.join(__dirname, 'trend_scraper_agent.py');
      exec(`python3 "${script}" --run-all`, { timeout: 600000 }, (err, stdout, stderr) => {
        if (err) console.error('[TrendScraper] Cron error:', stderr);
        else     console.log('[TrendScraper] Cron complete:', stdout.slice(0, 200));
      });
    } catch (e) { console.error('[TrendScraper] Cron failed:', e); }
  });
}

// Competitor Content Scraper — ACTIVE (free: YouTube API + OpenAI already in stack)
if (FEATURES.competitorScraper) {
  cron.schedule('0 5,17 * * *', async () => {
    console.log('[CompetitorScraper] Bulk scrape starting...');
    try {
      const users = await User.find({ plan: { $ne: 'trial' } }).select('_id').lean();
      for (const user of users) {
        const script = path.join(__dirname, 'competitor_content_scraper.py');
        exec(`python3 "${script}" --bulk ${user._id}`, { timeout: 300000 },
          (err, stdout, stderr) => { if (err) console.error(`[CompetitorScraper] Error for ${user._id}:`, stderr); }
        );
      }
    } catch (e) { console.error('[CompetitorScraper] Cron failed:', e); }
  });
}


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

// GET /api/affiliate/stats
app.get('/api/affiliate/stats', requireAuth, async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const stats = await db.collection('affiliate_stats')
      .findOne({ userId: req.user.id });
    res.json({
      success:       true,
      pendingPayout: stats?.pendingPayout  || 0,
      totalSignups:  stats?.totalSignups   || 0,
      conversions:   stats?.conversions    || 0,
      totalEarned:   stats?.totalEarned    || 0,
      commissionRate: 0.30,
    });
  } catch (err) {
    console.error('[Affiliate] GET /stats error:', err);
    res.status(500).json({ error: 'Failed to fetch affiliate stats' });
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
// CRON JOBS
// =============================================================================

// =============================================================================
// AUTO-POSTING PIPELINE
// =============================================================================

// Step 1 — Generate video script via OpenAI
async function pipelineGenerateScript(title, nicheName, type) {
  const { OpenAI } = require('openai');
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const isLong = type === 'Long-form';
  const completion = await openai.chat.completions.create({
    model: 'gpt-4o-mini',
    messages: [{
      role: 'user',
      content: `Write a ${isLong ? '3–5 minute' : '45–60 second'} YouTube ${isLong ? '' : 'Shorts '}script for the video titled: "${title}".
Niche: ${nicheName}. Natural spoken language only — no stage directions, no emojis, no markdown.
Start with a strong hook sentence. End with a brief call-to-action.`,
    }],
    temperature: 0.8,
  });
  return completion.choices[0].message.content.trim();
}

// Step 2 — Generate voiceover via Google Cloud TTS (uses GEMINI_API_KEY)
async function pipelineGenerateVoiceover(script, userId) {
  const apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_TTS_KEY;
  if (!apiKey) throw new Error('GEMINI_API_KEY not configured for TTS');
  const res = await axios.post(
    `https://texttospeech.googleapis.com/v1/text:synthesize?key=${apiKey}`,
    {
      input:       { text: script.slice(0, 5000) },
      voice:       { languageCode: 'en-US', name: 'en-US-Neural2-D', ssmlGender: 'MALE' },
      audioConfig: { audioEncoding: 'MP3', speakingRate: 1.05 },
    }
  );
  const audioPath = `/tmp/vly_audio_${userId}_${Date.now()}.mp3`;
  require('fs').writeFileSync(audioPath, Buffer.from(res.data.audioContent, 'base64'));
  return audioPath;
}

// Step 3 — Fetch portrait stock footage from Pexels
async function pipelineFetchFootage(query) {
  const apiKey = process.env.PEXELS_API_KEY;
  if (!apiKey) throw new Error('PEXELS_API_KEY not configured');
  const res = await axios.get('https://api.pexels.com/videos/search', {
    headers: { Authorization: apiKey },
    params:  { query: query.slice(0, 100), per_page: 5, orientation: 'portrait' },
  });
  const videos = (res.data.videos || []);
  if (!videos.length) throw new Error(`No Pexels footage found for: ${query}`);
  const video  = videos[0];
  const file   = (video.video_files || []).find(f => f.quality === 'hd') ||
                 (video.video_files || [])[0];
  if (!file?.link) throw new Error('No usable Pexels video file');
  return { url: file.link, duration: video.duration };
}

// Step 4 — Assemble video: overlay audio on footage with ffmpeg
async function pipelineAssembleVideo(footageUrl, audioPath, outputPath) {
  return new Promise((resolve, reject) => {
    const footagePath = outputPath.replace('.mp4', '_raw.mp4');
    exec(
      `curl -sL -o "${footagePath}" "${footageUrl}" && ` +
      `ffmpeg -y -i "${footagePath}" -i "${audioPath}" ` +
      `-c:v copy -c:a aac -map 0:v:0 -map 1:a:0 -shortest "${outputPath}"`,
      { timeout: 300000 },
      (err, _stdout, stderr) => {
        require('fs').unlink(footagePath, () => {});
        if (err) return reject(new Error('ffmpeg assembly failed: ' + (stderr || err.message).slice(0, 300)));
        resolve(outputPath);
      }
    );
  });
}

// Step 5 — Refresh a YouTube OAuth access token and persist to MongoDB
async function pipelineRefreshToken(channel) {
  const res = await axios.post('https://oauth2.googleapis.com/token', {
    client_id:     process.env.YOUTUBE_CLIENT_ID,
    client_secret: process.env.YOUTUBE_CLIENT_SECRET,
    refresh_token: channel.refreshToken,
    grant_type:    'refresh_token',
  });
  const newToken = res.data.access_token;
  await User.updateOne(
    { 'youtubeChannels.channelId': channel.channelId },
    { $set: { 'youtubeChannels.$.accessToken': newToken } }
  );
  return newToken;
}

// Step 6 — Upload assembled video to YouTube
async function pipelineUploadToYouTube(videoPath, title, description, channel) {
  const { google } = require('googleapis');
  const oauth2 = new google.auth.OAuth2(
    process.env.YOUTUBE_CLIENT_ID,
    process.env.YOUTUBE_CLIENT_SECRET
  );

  const tryUpload = async (accessToken) => {
    oauth2.setCredentials({ access_token: accessToken, refresh_token: channel.refreshToken });
    const yt  = google.youtube({ version: 'v3', auth: oauth2 });
    const res = await yt.videos.insert({
      part: ['snippet', 'status'],
      requestBody: {
        snippet: {
          title:       title.slice(0, 100),
          description: description.slice(0, 5000),
          tags:        ['shorts', 'youtube shorts'],
          categoryId:  '22',
        },
        status: { privacyStatus: 'public' },
      },
      media: { body: require('fs').createReadStream(videoPath) },
    });
    return res.data.id;
  };

  try {
    return await tryUpload(channel.accessToken);
  } catch (err) {
    if (err.code === 401 || err.status === 401 || /invalid_grant|token/.test(err.message)) {
      console.log(`[Pipeline] Token expired for ${channel.channelId} — refreshing`);
      const newToken = await pipelineRefreshToken(channel);
      channel.accessToken = newToken;
      return await tryUpload(newToken);
    }
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

// Main pipeline runner — processes all due approved slots across all users
async function runAutoPostPipeline() {
  if (!process.env.OPENAI_API_KEY) {
    console.log('[AutoPost] Skipping — OPENAI_API_KEY not configured');
    return;
  }
  const col   = agentCol('content_calendars');
  const today = new Date().toISOString().slice(0, 10);
  const calendars = await col.find({ status: 'active' }).toArray();
  let processed = 0;

  for (const calendar of calendars) {
    const dueSlots = (calendar.slots || []).filter(s =>
      s.status === 'approved' && !s.posted && (s.date || s.scheduledDate || '') <= today
    );
    if (!dueSlots.length) continue;

    const user = await User.findById(calendar.userId).catch(() => null);
    if (!user) continue;
    const channel = (user.youtubeChannels || []).find(ch => ch.channelId === calendar.channelId)
                 || (user.youtubeChannels || [])[0];
    if (!channel) { console.warn(`[AutoPost] No channel found for calendar ${calendar._id}`); continue; }

    for (const slot of dueSlots) {
      const slotKey   = `day${slot.day}_vi${slot.videoIndex || 1}`;
      const outputPath = `/tmp/vly_video_${calendar._id}_${slotKey}_${Date.now()}.mp4`;
      let   audioPath  = null;
      console.log(`[AutoPost] Starting: "${slot.title}" (${slot.type}) for ${channel.channelName}`);

      // Mark in-progress
      await col.updateOne(
        { _id: calendar._id },
        { $set: { [`slots.$[s].pipelineStatus`]: 'generating' } },
        { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
      ).catch(() => {});

      try {
        // 1. Script
        const script = await pipelineGenerateScript(slot.title, calendar.nicheName || user.nicheName || '', slot.type);
        console.log(`[AutoPost] Script generated (${script.length} chars)`);

        // 2. Voiceover
        audioPath = await pipelineGenerateVoiceover(script, String(calendar.userId));
        console.log(`[AutoPost] Voiceover saved: ${audioPath}`);

        // 3. Pexels footage
        const footage = await pipelineFetchFootage(slot.title);
        console.log(`[AutoPost] Footage fetched: ${footage.url.slice(0, 60)}…`);

        // 4. Assemble
        await pipelineAssembleVideo(footage.url, audioPath, outputPath);
        console.log(`[AutoPost] Video assembled: ${outputPath}`);

        // 5. YouTube upload
        const ytId = await pipelineUploadToYouTube(outputPath, slot.title, script, channel);
        console.log(`[AutoPost] Uploaded → youtu.be/${ytId}`);

        // 6. Mark posted
        await col.updateOne(
          { _id: calendar._id },
          { $set: {
            'slots.$[s].status':          'posted',
            'slots.$[s].posted':          true,
            'slots.$[s].postedAt':        new Date().toISOString(),
            'slots.$[s].youtubeVideoId':  ytId,
            'slots.$[s].pipelineStatus':  'posted',
          }},
          { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
        );
        processed++;

      } catch (err) {
        console.error(`[AutoPost] Failed "${slot.title}": ${err.message}`);
        await col.updateOne(
          { _id: calendar._id },
          { $set: {
            'slots.$[s].pipelineStatus': 'failed',
            'slots.$[s].pipelineError':  err.message.slice(0, 500),
          }},
          { arrayFilters: [{ 's.day': slot.day, 's.videoIndex': slot.videoIndex || 1 }] }
        ).catch(() => {});
      } finally {
        if (audioPath) require('fs').unlink(audioPath, () => {});
        require('fs').unlink(outputPath, () => {});
      }
    }
  }
  console.log(`[AutoPost] Cycle complete — ${processed} video(s) posted`);
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
async function runWeeklyOptimizedCalendars() {
  console.log('[WeeklyOpt] Starting weekly AI-optimised calendar refresh...');
  if (!process.env.OPENAI_API_KEY) {
    console.log('[WeeklyOpt] Skipping — OPENAI_API_KEY not configured');
    return;
  }

  const { OpenAI } = require('openai');
  const openai     = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const col        = agentCol('content_calendars');
  const calendars  = await col.find({ status: 'active' }).toArray();
  let refreshed    = 0;

  for (const calendar of calendars) {
    try {
      const user = await User.findById(calendar.userId).catch(() => null);
      if (!user) continue;

      const nicheName = calendar.nicheName || '';
      if (!nicheName) continue;

      const config  = PLAN_CONFIG[user.plan] || PLAN_CONFIG.trial;
      const insights = await analyzeChannelPerformance(calendar.userId, calendar.channelId, nicheName);

      // Scout trending topics via YouTube Data API (last 7 days, ordered by viewCount)
      const trendingTopics = await scoutTrendingTopics(nicheName);
      const trendingContext = trendingTopics.length > 0
        ? `\nTRENDING THIS WEEK in "${nicheName}":\n${trendingTopics.map((t, i) => `${i + 1}. ${t}`).join('\n')}\nGenerate video titles inspired by these trends but with fresh, unique angles — do not copy them verbatim.\n`
        : '';

      // Build performance context (same logic as POST /api/content/calendar)
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
Strategy: Generate titles that build on what worked. Never repeat any of the above titles exactly.
`;
      }

      // Generate 30 new days of Shorts titles in 5-day batches
      const today = new Date();
      const dates = Array.from({ length: 30 }, (_, i) => {
        const d = new Date(today); d.setDate(d.getDate() + i);
        return d.toISOString().slice(0, 10);
      });
      const BATCH = 5;
      const allShortTitles = [];

      for (let batchStart = 0; batchStart < 30; batchStart += BATCH) {
        const batchDays  = Math.min(BATCH, 30 - batchStart);
        const batchCount = batchDays * config.shortsPerDay;
        const usedList   = allShortTitles.length
          ? `\nAlready used — do NOT repeat:\n${allShortTitles.map((t, i) => `${i + 1}. ${t}`).join('\n')}\n`
          : '';
        const batchRes = await openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [{ role: 'user', content:
            `YouTube Shorts content strategist for a "${nicheName}" channel.
${perfContext}${trendingContext}${usedList}
Generate exactly ${batchCount} NEW, completely unique Short video titles (under 60 chars each).
Vary style: hook, story, tutorial, listicle, myth-bust. Never repeat any used concept.
Return JSON: { "titles": [${batchCount} strings] }` }],
          temperature: 0.9,
          response_format: { type: 'json_object' },
        });
        try {
          const p = JSON.parse(batchRes.choices[0].message.content);
          allShortTitles.push(...(p.titles || []));
        } catch { /* fallback titles applied during slot build */ }
      }

      // Generate Long-form titles if plan includes them
      let allLongFormTitles = [];
      const longFormDates   = config.longFormPerWeek > 0
        ? dates.filter(dt => new Date(dt).getDay() === 1) : [];
      if (longFormDates.length > 0) {
        const lfRes = await openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [{ role: 'user', content:
            `YouTube long-form content strategist for a "${nicheName}" channel.
${perfContext}${trendingContext}
Generate exactly ${longFormDates.length} unique Long-form video titles (60–100 chars each).
In-depth, educational, distinct from Shorts. Build on top performers with deeper angles.
Already used Shorts (do NOT overlap): ${allShortTitles.slice(0, 20).map((t, i) => `${i + 1}. ${t}`).join('; ')}
Return JSON: { "titles": [${longFormDates.length} strings] }` }],
          temperature: 0.88,
          response_format: { type: 'json_object' },
        });
        try { allLongFormTitles = JSON.parse(lfRes.choices[0].message.content).titles || []; }
        catch { /* use fallbacks */ }
      }

      // Build slots
      let shortIdx = 0, longFormIdx = 0;
      const slots = [];
      for (let i = 0; i < 30; i++) {
        const day = i + 1, date = dates[i], dow = new Date(date).getDay();
        for (let v = 0; v < config.shortsPerDay; v++) {
          slots.push({ day, date, title: allShortTitles[shortIdx] || `${nicheName} Short — Day ${day} #${v + 1}`,
            type: 'Short', videoIndex: v + 1, totalForDay: config.shortsPerDay, angle: 'short', status: 'planned' });
          shortIdx++;
        }
        if (config.longFormPerWeek > 0 && dow === 1) {
          slots.push({ day, date, title: allLongFormTitles[longFormIdx] || `Deep Dive ${longFormIdx + 1}: ${nicheName}`,
            type: 'Long-form', videoIndex: 1, totalForDay: 1, angle: 'long-form', status: 'planned' });
          longFormIdx++;
        }
      }

      const weeklyInsights = {
        generatedAt:        new Date().toISOString(),
        totalAnalyzed:      insights?.totalAnalyzed   || 0,
        topPerformers:      insights?.topPerformers   || [],
        bottomPerformers:   insights?.bottomPerformers || [],
        titlePatterns:      insights?.titlePatterns   || null,
        trendingTopics:     trendingTopics,
        generationStrategy: insights
          ? `AI-optimised: built on top ${insights.topPerformers.length} performer(s), avoided bottom ${insights.bottomPerformers.length}`
          : 'Standard generation — no prior performance data (first week)',
      };

      await col.updateOne(
        { _id: calendar._id },
        { $set: { slots, generatedAt: new Date().toISOString(), weeklyInsights, weeklyRefreshedAt: new Date().toISOString() } }
      );
      refreshed++;
      console.log(`[WeeklyOpt] Refreshed calendar for user ${calendar.userId} — ${slots.length} slots, strategy: ${weeklyInsights.generationStrategy}`);
    } catch (err) {
      console.error(`[WeeklyOpt] Failed for calendar ${calendar._id}:`, err.message);
    }
  }
  console.log(`[WeeklyOpt] Complete — ${refreshed}/${calendars.length} calendar(s) refreshed`);
}

// Hourly auto-posting scheduler
cron.schedule('0 * * * *', async () => {
  console.log('[AutoPost] Hourly check running...');
  try { await runAutoPostPipeline(); }
  catch (err) { console.error('[AutoPost] Scheduler error:', err.message); }
});

// Learning Loop — Monday 3 AM: AI-optimised calendar regeneration based on YouTube performance
if (FEATURES.learningLoop) {
  cron.schedule('0 3 * * 1', async () => {
    console.log('[CRON] Weekly AI-optimised calendar regeneration running...');
    try {
      await runWeeklyOptimizedCalendars();
    } catch (err) { console.error('[CRON] Weekly optimisation error:', err.message); }
  });
}

// Analytics Collection — ACTIVE (free: YouTube Data API free quota)
if (FEATURES.analyticsCollection) {
  cron.schedule('0 4 * * *', async () => {
    console.log('[CRON] Analytics collection running...');
    try {
      await runPython(path.join(__dirname, 'analytics_engine.py'), '--collect', 90000);
    } catch (err) { console.error('[CRON] Analytics error:', err); }
  });
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
});

module.exports = app;
