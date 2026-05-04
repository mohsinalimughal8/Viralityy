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
const stripe         = require('stripe')(process.env.STRIPE_SECRET_KEY);
const jwt            = require('jsonwebtoken');
const bcrypt         = require('bcryptjs');

const app  = express();
const PORT = process.env.PORT || 3000;

// =============================================================================
// DATABASE
// =============================================================================
mongoose.connect(process.env.MONGODB_URI, {
  useNewUrlParser:    true,
  useUnifiedTopology: true,
}).then(() => console.log('MongoDB connected'))
  .catch(err => { console.error('MongoDB error:', err); process.exit(1); });

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
  youtubeChannels: [{ channelId: String, channelName: String, accessToken: String, refreshToken: String, tiktokEnabled: Boolean, instagramEnabled: Boolean }],
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
app.use(cors({ origin: process.env.APP_URL || '*', credentials: true }));
app.use(morgan('combined'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use(session({
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  store: MongoStore.create({ mongoUrl: process.env.MONGODB_URI }),
  cookie: { secure: process.env.NODE_ENV === 'production', maxAge: 7 * 24 * 60 * 60 * 1000 },
}));

app.use(passport.initialize());
app.use(passport.session());

// Rate limiting
const limiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 200, standardHeaders: true, legacyHeaders: false });
app.use('/api/', limiter);

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
    let user = await User.findOne({ googleId: profile.id });
    if (!user) {
      user = await User.findOne({ email: profile.emails[0].value });
      if (user) { user.googleId = profile.id; await user.save(); }
    }
    if (!user) {
      user = await User.create({
        name:      profile.displayName,
        email:     profile.emails[0].value,
        googleId:  profile.id,
        plan:      'trial',
        trialStartedAt: new Date(),
        trialEndsAt:    new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
      });
    }
    return done(null, user);
  } catch (err) { return done(err); }
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

function planNicheQuota(plan) {
  const q = { trial: 0, starter: 0, shorts_pro: 2, growth: 3, agency: 3 };
  return q[plan] ?? 0;
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
app.get('/auth/google', passport.authenticate('google'));

app.get('/auth/google/callback', passport.authenticate('google', { failureRedirect: '/login' }), (req, res) => {
  const token = jwt.sign({ userId: req.user.id }, process.env.JWT_SECRET, { expiresIn: '7d' });
  res.redirect(`${process.env.APP_URL}/dashboard?token=${token}`);
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

    const user = await User.create({
      name, email: email.toLowerCase(), passwordHash, affiliateCode,
      plan: 'trial',
      trialStartedAt: new Date(),
      trialEndsAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
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
    res.json({ channels: user.youtubeChannels || [] });
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
    const planLimits = { trial: 1, starter: 1, shorts_pro: 1, growth: 3, agency: 5 };
    const limit = planLimits[user.plan] || 1;
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
      if (quota === 0) return res.status(403).json({ error: 'Niche changes are not available on your plan. Upgrade to Shorts Pro or above.', code: 'NICHE_LOCKED' });

      // Reset counter if it's a new year
      const currentYear = new Date().getFullYear();
      if (user.nicheChangesYear !== currentYear) { user.nicheChangesUsed = 0; user.nicheChangesYear = currentYear; }

      if (user.nicheChangesUsed >= quota) {
        return res.status(403).json({ error: `You have used all ${quota} niche changes for this year. Your allowance resets on January 1st.`, code: 'NICHE_QUOTA_EXHAUSTED' });
      }

      user.nicheChangesUsed += 1;
    }

    user.nicheId = nicheId;
    user.nicheName = nicheName;
    await user.save();

    const quota = planNicheQuota(user.plan);
    res.json({ success: true, nicheId, nicheName, changesUsed: user.nicheChangesUsed, changesRemaining: Math.max(0, quota - user.nicheChangesUsed) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/niche/status — get current niche + change allowance
app.get('/api/niche/status', requireAuth, async (req, res) => {
  try {
    const user  = await User.findById(req.user.id);
    const quota = planNicheQuota(user.plan);
    const currentYear = new Date().getFullYear();
    const changesUsed = user.nicheChangesYear === currentYear ? (user.nicheChangesUsed || 0) : 0;
    res.json({ nicheId: user.nicheId, nicheName: user.nicheName, quota, changesUsed, changesRemaining: Math.max(0, quota - changesUsed), plan: user.plan });
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
// ROUTE FILES — paste engine-specific routes
// =============================================================================
require('./server_m4a_routes_wrapped')(app, requireAuth, User, mongoose, runPython);
require('./server_m5a_routes_wrapped')(app, requireAuth, User, mongoose, runPython);
require('./server_m5b_routes_wrapped')(app, requireAuth, User, mongoose, runPython);
require('./server_m8_routes_wrapped')(app, requireAuth, User, mongoose, runPython, stripe);
require('./server_m9_routes_wrapped')(app, requireAuth, User, mongoose, runPython);

// =============================================================================
// CRON JOBS
// =============================================================================

// Daily: run video generation pipeline per active channel
cron.schedule('0 6 * * *', async () => {
  if (req?.headers?.['x-cron-secret'] !== process.env.CRON_SECRET) {
    console.log('[CRON] Daily video generation running...');
    try {
      const users = await User.find({ $or: [{ plan: { $nin: ['trial'] } }, { trialEndsAt: { $gt: new Date() } }] });
      for (const user of users) {
        for (const channel of (user.youtubeChannels || [])) {
          console.log(`[CRON] Generating for channel ${channel.channelName} (${user.plan})`);
          // Trigger Python pipeline — implement social_pipeline.py separately
        }
      }
    } catch (err) { console.error('[CRON] Error:', err); }
  }
});

// Weekly: run AI optimisation (learning engine)
cron.schedule('0 3 * * 1', async () => {
  console.log('[CRON] Weekly AI optimisation running...');
  try {
    await runPython(path.join(__dirname, 'learning_engine.py'), '', 120000);
  } catch (err) { console.error('[CRON] Learning engine error:', err); }
});

// Daily: run analytics collection
cron.schedule('0 4 * * *', async () => {
  console.log('[CRON] Analytics collection running...');
  try {
    await runPython(path.join(__dirname, 'analytics_engine.py'), '--collect', 90000);
  } catch (err) { console.error('[CRON] Analytics error:', err); }
});

// =============================================================================
// HEALTH CHECK
// =============================================================================
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), env: process.env.NODE_ENV || 'development', mongo: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected' });
});

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
