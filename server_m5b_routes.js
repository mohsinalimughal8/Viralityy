// =============================================================================
// VIRALITYY — M5b: LEARNING LOOP API ROUTES
// =============================================================================
// Paste into server.js BEFORE app.listen(), AFTER your other routes.
// =============================================================================

const { exec } = require('child_process');
const path = require('path');

// ── Helper ────────────────────────────────────────────────────────────────
function runLearningPython(args, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    const script = path.join(__dirname, 'learning_engine.py');
    exec(`python3 "${script}" ${args}`, { timeout: timeoutMs }, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      try { resolve(JSON.parse(stdout.trim())); }
      catch { resolve({ raw: stdout.trim() }); }
    });
  });
}

// ---------------------------------------------------------------------------
// GET /api/learning/config
// Returns the current prompt_config for the logged-in user.
// Used by dashboard to show what settings the AI is currently using.
// ---------------------------------------------------------------------------
app.get('/api/learning/config', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const config = user.promptConfig || {
      hook_style: 'question', script_format: 'list', script_length: 'medium',
      title_pattern: 'number', title_length: 'medium', confidence: 'default',
      last_optimised: null, top_subtopics: [],
    };
    res.json({ success: true, config });
  } catch (err) {
    console.error('[M5b] /api/learning/config error:', err);
    res.status(500).json({ error: 'Failed to fetch learning config' });
  }
});

// ---------------------------------------------------------------------------
// PATCH /api/learning/config
// Lets a user manually override specific prompt_config keys from the dashboard.
// Body: { hook_style: "story", title_pattern: "question", ... }
// ---------------------------------------------------------------------------
app.patch('/api/learning/config', requireAuth, async (req, res) => {
  try {
    const ALLOWED_KEYS = [
      'hook_style', 'script_format', 'script_length', 'pacing', 'cta_style',
      'title_pattern', 'title_length', 'include_emoji',
      'thumbnail_style', 'thumbnail_palette', 'best_post_day', 'best_post_hour',
    ];

    const updates = {};
    for (const key of ALLOWED_KEYS) {
      if (req.body[key] !== undefined) updates[`promptConfig.${key}`] = req.body[key];
    }

    if (!Object.keys(updates).length) {
      return res.status(400).json({ error: 'No valid keys provided' });
    }

    await User.findByIdAndUpdate(req.user.id, { $set: updates });
    res.json({ success: true, message: 'Config updated', updated: Object.keys(updates) });
  } catch (err) {
    console.error('[M5b] PATCH /api/learning/config error:', err);
    res.status(500).json({ error: 'Failed to update config' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/learning/reset
// Resets user's prompt_config back to defaults.
// ---------------------------------------------------------------------------
app.post('/api/learning/reset', requireAuth, async (req, res) => {
  try {
    const defaultConfig = {
      hook_style: 'question', script_format: 'list', script_length: 'medium',
      pacing: 'fast', cta_style: 'subscribe', title_pattern: 'number',
      title_length: 'medium', include_emoji: false,
      thumbnail_style: 'text_heavy', thumbnail_palette: 'warm',
      best_post_day: 'tuesday', best_post_hour: 15,
      top_subtopics: [], confidence: 'default', last_optimised: null,
    };
    await User.findByIdAndUpdate(req.user.id, { $set: { promptConfig: defaultConfig } });
    res.json({ success: true, message: 'Config reset to defaults', config: defaultConfig });
  } catch (err) {
    console.error('[M5b] /api/learning/reset error:', err);
    res.status(500).json({ error: 'Failed to reset config' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/learning/optimise
// Manually trigger optimisation for the logged-in user.
// ---------------------------------------------------------------------------
app.post('/api/learning/optimise', requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;
    res.json({ success: true, message: 'Optimisation started — check back in a moment' });

    const script = path.join(__dirname, 'learning_engine.py');
    exec(
      `python3 "${script}" --optimise --user ${userId}`,
      { timeout: 120000 },
      (err, stdout, stderr) => {
        if (err) console.error('[M5b] Optimise error:', stderr);
        else console.log('[M5b] Optimise result:', stdout.trim());
      }
    );
  } catch (err) {
    console.error('[M5b] /api/learning/optimise error:', err);
    res.status(500).json({ error: 'Failed to start optimisation' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/learning/history
// Returns the last 10 optimisation events for the logged-in user.
// ---------------------------------------------------------------------------
app.get('/api/learning/history', requireAuth, async (req, res) => {
  try {
    const db  = mongoose.connection.db;
    const col = db.collection('optimisation_log');
    const docs = await col
      .find({ userId: req.user.id })
      .sort({ optimisedAt: -1 })
      .limit(10)
      .toArray();
    res.json({ success: true, history: docs });
  } catch (err) {
    console.error('[M5b] /api/learning/history error:', err);
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/learning/optimise-all   [CRON / ADMIN]
// Run optimisation for all users — triggered by scheduler after analytics run.
// Protected by CRON_SECRET header.
// ---------------------------------------------------------------------------
app.post('/api/learning/optimise-all', async (req, res) => {
  if (req.headers['x-cron-secret'] !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorised' });
  }
  res.json({ success: true, message: 'Bulk optimisation started' });

  const script = path.join(__dirname, 'learning_engine.py');
  exec(`python3 "${script}" --optimise`, { timeout: 600000 }, (err, stdout, stderr) => {
    if (err) console.error('[M5b] Bulk optimise error:', stderr);
    else console.log('[M5b] Bulk optimise complete:', stdout.trim());
  });
});
