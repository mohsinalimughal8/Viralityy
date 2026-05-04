// =============================================================================
// VIRALITYY — M8: AFFILIATE PROGRAMME API ROUTES
// =============================================================================
// Paste into server.js BEFORE app.listen(), AFTER your other routes.
//
// Also add to your Stripe webhook handler (where you process invoice.paid):
//   await affiliateEngine.record_commission(customerId, amount, plan, paymentId)
// =============================================================================

const { exec } = require('child_process');
const path     = require('path');
const crypto   = require('crypto');

// ── Reuse Python affiliate engine via subprocess ──────────────────────────
function runAffiliatePy(args, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const script = path.join(__dirname, 'affiliate_cli.py');
    exec(`python3 "${script}" ${args}`, { timeout: timeoutMs }, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      try { resolve(JSON.parse(stdout.trim())); }
      catch { resolve({ raw: stdout.trim() }); }
    });
  });
}

// ── Direct MongoDB queries for low-latency reads ────────────────────────
async function affiliateCol() {
  return mongoose.connection.db.collection('affiliates');
}
async function commissionsCol() {
  return mongoose.connection.db.collection('affiliate_commissions');
}
async function referralsCol() {
  return mongoose.connection.db.collection('affiliate_referrals');
}
async function clicksCol() {
  return mongoose.connection.db.collection('affiliate_clicks');
}

// ---------------------------------------------------------------------------
// GET /api/affiliate/join
// Create or fetch affiliate account for the logged-in user.
// First call creates the account and referral link.
// ---------------------------------------------------------------------------
app.get('/api/affiliate/join', requireAuth, async (req, res) => {
  try {
    const user   = await User.findById(req.user.id);
    const col    = await affiliateCol();
    let affiliate = await col.findOne({ userId: req.user.id });

    if (!affiliate) {
      // Generate unique referral code
      const raw  = crypto.createHash('sha256')
        .update(`${req.user.id}:${crypto.randomBytes(8).toString('hex')}`)
        .digest('hex');
      const code = raw.slice(0, 8).toUpperCase();

      affiliate = {
        userId:           req.user.id,
        email:            user.email,
        referralCode:     code,
        referralUrl:      `${process.env.APP_URL || 'https://viralityy.com'}/?ref=${code}`,
        status:           'active',
        joinedAt:         new Date().toISOString(),
        totalClicks:      0,
        totalSignups:     0,
        totalConversions: 0,
        totalEarnedUsd:   0,
        pendingPayoutUsd: 0,
        paidOutUsd:       0,
        payoutMethod:     null,
        payoutDetails:    {},
      };
      await col.insertOne(affiliate);
    }

    res.json({ success: true, affiliate });
  } catch (err) {
    console.error('[M8] /api/affiliate/join error:', err);
    res.status(500).json({ error: 'Failed to create affiliate account' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/affiliate/dashboard
// Full dashboard data: stats, referrals, commissions, monthly breakdown.
// ---------------------------------------------------------------------------
app.get('/api/affiliate/dashboard', requireAuth, async (req, res) => {
  try {
    const userId     = req.user.id;
    const col        = await affiliateCol();
    const comCol     = await commissionsCol();
    const refCol     = await referralsCol();
    const affiliate  = await col.findOne({ userId });

    if (!affiliate) {
      return res.json({ success: true, exists: false });
    }

    // Recent referrals (last 20)
    const referrals = await refCol
      .find({ affiliateUserId: userId })
      .sort({ signedUpAt: -1 })
      .limit(20)
      .toArray();

    // Recent commissions (last 20)
    const commissions = await comCol
      .find({ affiliateUserId: userId })
      .sort({ earnedAt: -1 })
      .limit(20)
      .toArray();

    // Monthly breakdown
    const monthly = await comCol.aggregate([
      { $match: { affiliateUserId: userId } },
      { $addFields: { month: { $substr: ['$earnedAt', 0, 7] } } },
      { $group: { _id: '$month', commissionUsd: { $sum: '$commissionUsd' }, conversions: { $sum: 1 } } },
      { $sort: { _id: -1 } },
      { $limit: 6 }
    ]).toArray();

    const convRate = affiliate.totalSignups > 0
      ? ((affiliate.totalConversions / affiliate.totalSignups) * 100).toFixed(1)
      : '0.0';

    res.json({
      success: true,
      exists:  true,
      referralCode:     affiliate.referralCode,
      referralUrl:      affiliate.referralUrl,
      totalClicks:      affiliate.totalClicks,
      totalSignups:     affiliate.totalSignups,
      totalConversions: affiliate.totalConversions,
      totalEarnedUsd:   affiliate.totalEarnedUsd,
      pendingPayoutUsd: affiliate.pendingPayoutUsd,
      paidOutUsd:       affiliate.paidOutUsd,
      conversionRate:   parseFloat(convRate),
      minPayoutUsd:     50,
      payoutReady:      affiliate.pendingPayoutUsd >= 50,
      recentReferrals:  referrals.map(r => ({
        userId:      r.referredUserId.slice(0, 8) + '…',
        status:      r.status,
        plan:        r.plan,
        signedUpAt:  r.signedUpAt?.slice(0, 10),
        commission:  r.lifetimeCommission || 0,
      })),
      recentCommissions: commissions.map(c => ({
        plan:          c.plan,
        paymentAmount: c.paymentAmountUsd,
        commission:    c.commissionUsd,
        status:        c.status,
        earnedAt:      c.earnedAt?.slice(0, 10),
      })),
      monthlyBreakdown: monthly.map(m => ({
        month: m._id, commissionUsd: m.commissionUsd, conversions: m.conversions
      })),
      joinedAt: affiliate.joinedAt,
    });
  } catch (err) {
    console.error('[M8] /api/affiliate/dashboard error:', err);
    res.status(500).json({ error: 'Failed to fetch affiliate dashboard' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/affiliate/payout-request
// User requests a payout of their pending balance.
// Body: { method: "payoneer" | "stripe" | "bank", details: { ... } }
// ---------------------------------------------------------------------------
app.post('/api/affiliate/payout-request', requireAuth, async (req, res) => {
  try {
    const col       = await affiliateCol();
    const affiliate = await col.findOne({ userId: req.user.id });

    if (!affiliate) return res.status(404).json({ error: 'No affiliate account found' });
    if (affiliate.pendingPayoutUsd < 50) {
      return res.status(400).json({
        error: `Minimum payout is $50. Current balance: $${affiliate.pendingPayoutUsd.toFixed(2)}`
      });
    }

    const { method, details } = req.body;
    if (!['payoneer', 'stripe', 'bank'].includes(method)) {
      return res.status(400).json({ error: 'Invalid payout method' });
    }

    await col.updateOne(
      { userId: req.user.id },
      { $set: {
        payoutMethod:       method,
        payoutDetails:      details || {},
        payoutRequestedAt:  new Date().toISOString(),
        payoutStatus:       'requested',
      }}
    );

    // TODO: notify admin via email/Slack for manual processing
    // sendAdminNotification('payout_requested', { userId: req.user.id, amount: affiliate.pendingPayoutUsd })

    res.json({
      success: true,
      message: `Payout request of $${affiliate.pendingPayoutUsd.toFixed(2)} submitted via ${method}. Processed within 7 business days.`,
      amount:  affiliate.pendingPayoutUsd,
    });
  } catch (err) {
    console.error('[M8] /api/affiliate/payout-request error:', err);
    res.status(500).json({ error: 'Failed to submit payout request' });
  }
});

// ---------------------------------------------------------------------------
// GET /r/:code   (PUBLIC)
// Referral link redirect — tracks the click then redirects to homepage.
// Example: https://viralityy.com/r/A3F8C21B
// ---------------------------------------------------------------------------
app.get('/r/:code', async (req, res) => {
  try {
    const code    = req.params.code?.toUpperCase();
    const col     = await affiliateCol();
    const clkCol  = await clicksCol();
    const affiliate = await col.findOne({ referralCode: code });

    if (affiliate) {
      // Hash IP for privacy — never store raw IPs
      const ipHash = crypto.createHash('sha256')
        .update(req.ip + process.env.IP_SALT || '')
        .digest('hex').slice(0, 16);

      // Dedup: same IP within 24h
      const cutoff = new Date(Date.now() - 86400000).toISOString();
      const dup    = await clkCol.findOne({ referralCode: code, ipHash, clickedAt: { $gte: cutoff } });

      if (!dup) {
        await clkCol.insertOne({
          referralCode: code,
          affiliateId:  affiliate._id.toString(),
          ipHash,
          userAgent:    (req.headers['user-agent'] || '').slice(0, 200),
          clickedAt:    new Date().toISOString(),
        });
        await col.updateOne({ referralCode: code }, { $inc: { totalClicks: 1 } });
      }
    }

    // Set referral cookie (30 days) and redirect
    res.cookie('ref', code, { maxAge: 30 * 24 * 60 * 60 * 1000, httpOnly: true, secure: true });
    res.redirect(302, `${process.env.APP_URL || 'https://viralityy.com'}/?ref=${code}`);
  } catch (err) {
    console.error('[M8] /r/:code error:', err);
    res.redirect(302, process.env.APP_URL || 'https://viralityy.com');
  }
});

// ---------------------------------------------------------------------------
// INTERNAL: call this inside your existing Stripe webhook handler
// when event type is 'invoice.paid' or 'checkout.session.completed'
// ---------------------------------------------------------------------------
async function handleAffiliateCommission(stripeCustomerId, amountUsd, plan, stripePaymentId) {
  try {
    const user = await User.findOne({ stripeCustomerId });
    if (!user || !user.referredBy) return;  // not a referred user

    const refCol    = await referralsCol();
    const referral  = await refCol.findOne({ referredUserId: user._id.toString() });
    if (!referral) return;

    const commissionUsd = Math.round(amountUsd * 0.30 * 100) / 100;
    const col = await commissionsCol();

    await col.insertOne({
      affiliateUserId:  referral.affiliateUserId,
      referralCode:     referral.referralCode,
      referredUserId:   user._id.toString(),
      plan,
      paymentAmountUsd: amountUsd,
      commissionUsd,
      commissionRate:   0.30,
      stripePaymentId,
      status:           'pending',
      earnedAt:         new Date().toISOString(),
      paidAt:           null,
    });

    const affCol = await affiliateCol();
    await affCol.updateOne(
      { userId: referral.affiliateUserId },
      { $inc: { totalEarnedUsd: commissionUsd, pendingPayoutUsd: commissionUsd, totalConversions: 1 } }
    );

    await refCol.updateOne(
      { referredUserId: user._id.toString() },
      { $set: { status: 'converted', plan }, $inc: { lifetimeCommission: commissionUsd } }
    );

    console.log(`[M8] Commission $${commissionUsd} recorded for affiliate ${referral.affiliateUserId}`);
  } catch (err) {
    console.error('[M8] handleAffiliateCommission error:', err);
  }
}

// Export so you can call it from your Stripe webhook:
// const { handleAffiliateCommission } = require('./server'); // or move to a shared module
module.exports = { handleAffiliateCommission };
