// =============================================================================
// VIRALITYY — M5a: ANALYTICS COLLECTION API ROUTES
// =============================================================================
// HOW TO ADD: Paste this entire block into your server.js file,
// AFTER your existing routes but BEFORE app.listen().
//
// Also add this near the top of server.js with your other requires:
//   const { execSync } = require('child_process');
// =============================================================================

// ── Helper: call analytics_engine.py as a subprocess ──────────────────────
const { exec } = require('child_process');
const path = require('path');

function runAnalyticsPython(args) {
  return new Promise((resolve, reject) => {
    const script = path.join(__dirname, 'analytics_engine.py');
    exec(`python3 "${script}" ${args}`, { timeout: 60000 }, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      try { resolve(JSON.parse(stdout)); }
      catch { resolve({ raw: stdout }); }
    });
  });
}

// ── Helper: query MongoDB directly from Node for dashboard reads ───────────
async function getAnalyticsCollection() {
  // Uses your existing mongoose/mongodb connection
  // Works with mongoose: mongoose.connection.db
  // Works with native driver: client.db('viralityy')
  const db = mongoose.connection.db;
  return db.collection('video_analytics');
}

// ---------------------------------------------------------------------------
// GET /api/analytics/summary
// Dashboard summary — total views, likes, top videos, performance breakdown
//
// Example: GET /api/analytics/summary
// ---------------------------------------------------------------------------
app.get('/api/analytics/summary', requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;
    const col    = await getAnalyticsCollection();

    // Get latest snapshot per video using aggregation
    const docs = await col.aggregate([
      { $match: { userId } },
      { $sort:  { snapshotDate: -1 } },
      { $group: { _id: '$videoId', latest: { $first: '$$ROOT' } } },
      { $replaceRoot: { newRoot: '$latest' } }
    ]).toArray();

    if (!docs.length) {
      return res.json({
        success: true,
        summary: {
          totalVideos: 0, totalViews: 0, totalLikes: 0,
          totalComments: 0, avgLikeRatio: 0,
          performanceDist: { good: 0, average: 0, poor: 0 },
          topVideos: [], generatedAt: new Date().toISOString()
        }
      });
    }

    const totalViews    = docs.reduce((s, d) => s + (d.stats?.views || 0), 0);
    const totalLikes    = docs.reduce((s, d) => s + (d.stats?.likes || 0), 0);
    const totalComments = docs.reduce((s, d) => s + (d.stats?.comments || 0), 0);
    const avgLikeRatio  = docs.reduce((s, d) => s + (d.stats?.likeRatio || 0), 0) / docs.length;

    const perfDist = { good: 0, average: 0, poor: 0 };
    docs.forEach(d => {
      const vp = d.flags?.viewPerformance || 'average';
      perfDist[vp] = (perfDist[vp] || 0) + 1;
    });

    const topVideos = [...docs]
      .sort((a, b) => (b.stats?.views || 0) - (a.stats?.views || 0))
      .slice(0, 5)
      .map(d => ({
        videoId:   d.videoId,
        title:     d.title || '',
        views:     d.stats?.views || 0,
        likes:     d.stats?.likes || 0,
        likeRatio: d.stats?.likeRatio || 0,
        flag:      d.flags?.viewPerformance || 'average',
      }));

    res.json({
      success: true,
      summary: {
        totalVideos: docs.length,
        totalViews,
        totalLikes,
        totalComments,
        avgLikeRatio: Math.round(avgLikeRatio * 10000) / 10000,
        performanceDist: perfDist,
        topVideos,
        generatedAt: new Date().toISOString(),
      }
    });
  } catch (err) {
    console.error('[M5a] /api/analytics/summary error:', err);
    res.status(500).json({ error: 'Failed to fetch analytics summary' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/analytics/videos
// List all tracked videos with latest stats for the logged-in user
//
// Query params:
//   sort    (optional) — 'views' | 'likes' | 'comments' | 'date', default 'views'
//   flag    (optional) — 'good' | 'average' | 'poor' (filter by performance flag)
//   limit   (optional) — max results, default 50
//
// Example: GET /api/analytics/videos?sort=views&flag=poor&limit=10
// ---------------------------------------------------------------------------
app.get('/api/analytics/videos', requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;
    const sort   = req.query.sort  || 'views';
    const flag   = req.query.flag  || null;
    const limit  = Math.min(parseInt(req.query.limit) || 50, 200);
    const col    = await getAnalyticsCollection();

    const sortField = {
      views:    'stats.views',
      likes:    'stats.likes',
      comments: 'stats.comments',
      date:     'publishedAt',
    }[sort] || 'stats.views';

    const matchStage = { userId };
    if (flag) matchStage['flags.viewPerformance'] = flag;

    const docs = await col.aggregate([
      { $match: { userId } },
      { $sort:  { snapshotDate: -1 } },
      { $group: { _id: '$videoId', latest: { $first: '$$ROOT' } } },
      { $replaceRoot: { newRoot: '$latest' } },
      ...(flag ? [{ $match: { 'flags.viewPerformance': flag } }] : []),
      { $sort:  { [sortField]: -1 } },
      { $limit: limit }
    ]).toArray();

    res.json({ success: true, count: docs.length, videos: docs });
  } catch (err) {
    console.error('[M5a] /api/analytics/videos error:', err);
    res.status(500).json({ error: 'Failed to fetch video analytics' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/analytics/videos/:videoId/history
// Daily snapshot history for one video (for sparkline / trend charts)
//
// Example: GET /api/analytics/videos/dQw4w9WgXcQ/history
// ---------------------------------------------------------------------------
app.get('/api/analytics/videos/:videoId/history', requireAuth, async (req, res) => {
  try {
    const userId  = req.user.id;
    const videoId = req.params.videoId;
    const col     = await getAnalyticsCollection();

    const docs = await col.find(
      { videoId, userId },
      { sort: { snapshotDate: 1 } }
    ).toArray();

    res.json({
      success: true,
      videoId,
      snapshots: docs.map(d => ({
        date:      d.snapshotDate,
        views:     d.stats?.views || 0,
        likes:     d.stats?.likes || 0,
        comments:  d.stats?.comments || 0,
        likeRatio: d.stats?.likeRatio || 0,
      }))
    });
  } catch (err) {
    console.error('[M5a] /api/analytics/videos/:id/history error:', err);
    res.status(500).json({ error: 'Failed to fetch video history' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/analytics/underperformers
// Returns videos flagged as poor performers — consumed by M5b learning loop
//
// Example: GET /api/analytics/underperformers
// ---------------------------------------------------------------------------
app.get('/api/analytics/underperformers', requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;
    const col    = await getAnalyticsCollection();

    const docs = await col.aggregate([
      { $match: { userId } },
      { $sort:  { snapshotDate: -1 } },
      { $group: { _id: '$videoId', latest: { $first: '$$ROOT' } } },
      { $replaceRoot: { newRoot: '$latest' } },
      { $match: { 'flags.viewPerformance': 'poor' } },
      { $sort:  { 'stats.views': 1 } },
      { $limit: 50 }
    ]).toArray();

    res.json({ success: true, count: docs.length, underperformers: docs });
  } catch (err) {
    console.error('[M5a] /api/analytics/underperformers error:', err);
    res.status(500).json({ error: 'Failed to fetch underperformers' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/analytics/collect
// Admin/cron trigger — manually kick off analytics collection for all users
// Protected: only callable with CRON_SECRET header
//
// Example: POST /api/analytics/collect
//   Headers: { x-cron-secret: "your_cron_secret" }
// ---------------------------------------------------------------------------
app.post('/api/analytics/collect', async (req, res) => {
  const secret = req.headers['x-cron-secret'];
  if (secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorised' });
  }

  // Fire and forget — runs in background, responds immediately
  res.json({ success: true, message: 'Analytics collection started in background' });

  try {
    const script = path.join(__dirname, 'analytics_engine.py');
    exec(`python3 "${script}" --collect`, { timeout: 300000 }, (err, stdout, stderr) => {
      if (err) console.error('[M5a] Collection job error:', stderr);
      else console.log('[M5a] Collection job complete:', stdout.trim());
    });
  } catch (err) {
    console.error('[M5a] Failed to start collection job:', err);
  }
});

// ---------------------------------------------------------------------------
// POST /api/analytics/refresh/:videoId
// Refresh analytics for a single video immediately
//
// Example: POST /api/analytics/refresh/dQw4w9WgXcQ
// ---------------------------------------------------------------------------
app.post('/api/analytics/refresh/:videoId', requireAuth, async (req, res) => {
  try {
    const userId  = req.user.id;
    const videoId = req.params.videoId;
    const script  = path.join(__dirname, 'analytics_engine.py');

    exec(
      `python3 "${script}" --refresh --video ${videoId} --user ${userId}`,
      { timeout: 30000 },
      (err, stdout, stderr) => {
        if (err) {
          console.error('[M5a] Refresh error:', stderr);
          return res.status(500).json({ error: 'Refresh failed' });
        }
        res.json({ success: true, message: `Video ${videoId} refreshed` });
      }
    );
  } catch (err) {
    console.error('[M5a] /api/analytics/refresh error:', err);
    res.status(500).json({ error: 'Failed to refresh video analytics' });
  }
});
