// =============================================================================
// VIRALITYY — M9: VIDEO PREVIEW MODE API ROUTES
// =============================================================================
// Paste into server.js BEFORE app.listen(), AFTER your other routes.
// =============================================================================

// ── Helpers ───────────────────────────────────────────────────────────────
async function previewCol() {
  return mongoose.connection.db.collection('preview_queue');
}

// ---------------------------------------------------------------------------
// GET /api/preview/pending
// All pending previews for the logged-in user, soonest expiry first.
// ---------------------------------------------------------------------------
app.get('/api/preview/pending', requireAuth, async (req, res) => {
  try {
    const col  = await previewCol();
    const now  = new Date().toISOString();
    const docs = await col
      .find({ userId: req.user.id, status: 'pending' })
      .sort({ expiresAt: 1 })
      .limit(50)
      .toArray();

    const formatted = docs.map(d => {
      const exp = new Date(d.expiresAt);
      const hoursLeft = Math.max(0, (exp - Date.now()) / 3600000);
      return {
        ...d,
        _id:            d._id.toString(),
        hoursRemaining: Math.round(hoursLeft * 10) / 10,
        urgent:         hoursLeft < 4,
      };
    });

    res.json({ success: true, count: formatted.length, previews: formatted });
  } catch (err) {
    console.error('[M9] /api/preview/pending error:', err);
    res.status(500).json({ error: 'Failed to fetch pending previews' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/preview/history
// Previously actioned previews (approved / skipped / posted).
// ---------------------------------------------------------------------------
app.get('/api/preview/history', requireAuth, async (req, res) => {
  try {
    const col  = await previewCol();
    const docs = await col
      .find({ userId: req.user.id, status: { $ne: 'pending' } })
      .sort({ userActionAt: -1 })
      .limit(parseInt(req.query.limit) || 30)
      .toArray();

    res.json({
      success: true,
      count:   docs.length,
      history: docs.map(d => ({ ...d, _id: d._id.toString() })),
    });
  } catch (err) {
    console.error('[M9] /api/preview/history error:', err);
    res.status(500).json({ error: 'Failed to fetch preview history' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/preview/stats
// Summary counts: pending, approved, skipped, posted.
// ---------------------------------------------------------------------------
app.get('/api/preview/stats', requireAuth, async (req, res) => {
  try {
    const col  = await previewCol();
    const agg  = await col.aggregate([
      { $match: { userId: req.user.id } },
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]).toArray();

    const counts = {};
    agg.forEach(d => { counts[d._id] = d.count; });

    res.json({
      success:        true,
      pending:        counts.pending        || 0,
      approved:       counts.approved       || 0,
      skipped:        counts.skipped        || 0,
      edit_requested: counts.edit_requested || 0,
      posted:         counts.posted         || 0,
    });
  } catch (err) {
    console.error('[M9] /api/preview/stats error:', err);
    res.status(500).json({ error: 'Failed to fetch preview stats' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/preview/:previewId/approve
// User approves the video — it will be posted by the scheduler.
// ---------------------------------------------------------------------------
app.post('/api/preview/:previewId/approve', requireAuth, async (req, res) => {
  try {
    const col    = await previewCol();
    const { ObjectId } = require('mongodb');
    const result = await col.updateOne(
      { _id: new ObjectId(req.params.previewId), userId: req.user.id, status: 'pending' },
      { $set: { status: 'approved', userAction: 'approve', userActionAt: new Date().toISOString() } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: 'Preview not found or already actioned' });
    }
    res.json({ success: true, message: 'Video approved — will be posted shortly' });
  } catch (err) {
    console.error('[M9] /approve error:', err);
    res.status(500).json({ error: 'Failed to approve video' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/preview/:previewId/skip
// User skips the video — it is dropped from the queue.
// ---------------------------------------------------------------------------
app.post('/api/preview/:previewId/skip', requireAuth, async (req, res) => {
  try {
    const col    = await previewCol();
    const { ObjectId } = require('mongodb');
    const result = await col.updateOne(
      { _id: new ObjectId(req.params.previewId), userId: req.user.id, status: 'pending' },
      { $set: { status: 'skipped', userAction: 'skip', userActionAt: new Date().toISOString() } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: 'Preview not found or already actioned' });
    }
    res.json({ success: true, message: 'Video skipped and removed from queue' });
  } catch (err) {
    console.error('[M9] /skip error:', err);
    res.status(500).json({ error: 'Failed to skip video' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/preview/:previewId/edit
// User requests an edit with instructions.
// Body: { instructions: "Make the hook stronger and change the title to..." }
// ---------------------------------------------------------------------------
app.post('/api/preview/:previewId/edit', requireAuth, async (req, res) => {
  try {
    const { instructions } = req.body;
    if (!instructions || instructions.trim().length < 5) {
      return res.status(400).json({ error: 'Please provide edit instructions (at least 5 characters)' });
    }

    const col    = await previewCol();
    const { ObjectId } = require('mongodb');
    const result = await col.updateOne(
      { _id: new ObjectId(req.params.previewId), userId: req.user.id, status: 'pending' },
      { $set: {
        status:       'edit_requested',
        userAction:   'edit',
        userActionAt: new Date().toISOString(),
        editRequest:  instructions.slice(0, 1000),
      }}
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: 'Preview not found or already actioned' });
    }

    // Log edit request for pipeline
    const editCol = mongoose.connection.db.collection('edit_requests');
    await editCol.insertOne({
      previewId:    req.params.previewId,
      userId:       req.user.id,
      instructions: instructions.slice(0, 1000),
      requestedAt:  new Date().toISOString(),
      status:       'pending',
    });

    res.json({ success: true, message: 'Edit request received — video will be regenerated with your instructions' });
  } catch (err) {
    console.error('[M9] /edit error:', err);
    res.status(500).json({ error: 'Failed to submit edit request' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/preview/settings
// Get user's preview settings (auto-approve on/off, email notifications).
// ---------------------------------------------------------------------------
app.get('/api/preview/settings', requireAuth, async (req, res) => {
  try {
    const user     = await User.findById(req.user.id);
    const settings = user?.previewSettings || { autoApprove: true, notifyEmail: true };
    res.json({ success: true, settings });
  } catch (err) {
    console.error('[M9] /api/preview/settings GET error:', err);
    res.status(500).json({ error: 'Failed to fetch settings' });
  }
});

// ---------------------------------------------------------------------------
// PATCH /api/preview/settings
// Update preview settings.
// Body: { autoApprove: false, notifyEmail: true }
// ---------------------------------------------------------------------------
app.patch('/api/preview/settings', requireAuth, async (req, res) => {
  try {
    const { autoApprove, notifyEmail } = req.body;
    await User.findByIdAndUpdate(req.user.id, {
      $set: {
        'previewSettings.autoApprove': autoApprove !== undefined ? !!autoApprove : true,
        'previewSettings.notifyEmail': notifyEmail !== undefined ? !!notifyEmail : true,
        'previewSettings.updatedAt':   new Date().toISOString(),
      }
    });
    res.json({ success: true, message: 'Preview settings updated' });
  } catch (err) {
    console.error('[M9] /api/preview/settings PATCH error:', err);
    res.status(500).json({ error: 'Failed to update settings' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/preview/process-expired   [CRON]
// Auto-approves or auto-skips previews past their 24h window.
// Run hourly. Protected by CRON_SECRET.
// ---------------------------------------------------------------------------
app.post('/api/preview/process-expired', async (req, res) => {
  if (req.headers['x-cron-secret'] !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorised' });
  }

  try {
    const col = await previewCol();
    const now = new Date().toISOString();

    const expired = await col.find({
      status: 'pending',
      expiresAt: { $lte: now },
    }).toArray();

    let autoApproved = 0, autoSkipped = 0;

    for (const doc of expired) {
      const newStatus = doc.autoApprove !== false ? 'approved' : 'skipped';
      await col.updateOne(
        { _id: doc._id },
        { $set: { status: newStatus, userAction: 'auto', userActionAt: now } }
      );
      newStatus === 'approved' ? autoApproved++ : autoSkipped++;
    }

    const result = { expired: expired.length, autoApproved, autoSkipped };
    console.log('[M9] process-expired:', result);
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[M9] /process-expired error:', err);
    res.status(500).json({ error: 'Failed to process expired previews' });
  }
});
