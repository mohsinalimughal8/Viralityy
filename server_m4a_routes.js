// =============================================================================
// VIRALITYY — M4a: NICHE SUGGESTION ENGINE ROUTES
// =============================================================================
// HOW TO ADD: Paste this entire block into your server.js file.
// Place it AFTER your existing route definitions but BEFORE the final
// app.listen() call at the bottom of server.js.
// =============================================================================

const { NicheEngine } = require('./niche_engine_bridge'); // see niche_engine_bridge.js
const nicheEngine = new NicheEngine();

// ---------------------------------------------------------------------------
// GET /api/niches/recommend
// Returns ranked niche list for the logged-in user's plan
//
// Query params:
//   count    (optional) — number of results, default 10, max 30
//   category (optional) — filter by category name e.g. "Education"
//
// Example: GET /api/niches/recommend?count=5&category=Finance%20%26%20Business
// ---------------------------------------------------------------------------
app.get('/api/niches/recommend', requireAuth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const plan = user.plan || 'shorts_starter';
    const count = Math.min(parseInt(req.query.count) || 10, 30);
    const category = req.query.category || null;

    const results = nicheEngine.recommend(plan, count, category);

    res.json({
      success: true,
      plan,
      count: results.length,
      niches: results,
    });
  } catch (err) {
    console.error('[M4a] /api/niches/recommend error:', err);
    res.status(500).json({ error: 'Failed to fetch niche recommendations' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/niches/:nicheId
// Returns full detail for a single niche including example topics & revenue est.
//
// Example: GET /api/niches/psychology_facts
// ---------------------------------------------------------------------------
app.get('/api/niches/:nicheId', requireAuth, async (req, res) => {
  try {
    const detail = nicheEngine.getNicheDetail(req.params.nicheId);
    if (detail.error) return res.status(404).json(detail);
    res.json({ success: true, niche: detail });
  } catch (err) {
    console.error('[M4a] /api/niches/:id error:', err);
    res.status(500).json({ error: 'Failed to fetch niche detail' });
  }
});

// ---------------------------------------------------------------------------
// GET /api/niches/categories
// Returns list of all available niche categories for the filter dropdown
//
// Example: GET /api/niches/categories
// ---------------------------------------------------------------------------
app.get('/api/niches/categories', requireAuth, (req, res) => {
  try {
    const categories = nicheEngine.getCategories();
    res.json({ success: true, categories });
  } catch (err) {
    console.error('[M4a] /api/niches/categories error:', err);
    res.status(500).json({ error: 'Failed to fetch categories' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/compare
// Compares multiple niches side by side
//
// Body: { nicheIds: ["psychology_facts", "ai_tools", "book_summaries"] }
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
    console.error('[M4a] /api/niches/compare error:', err);
    res.status(500).json({ error: 'Failed to compare niches' });
  }
});

// ---------------------------------------------------------------------------
// POST /api/niches/select
// User confirms a niche for a channel — saves to their channel config in DB
//
// Body: { channelIndex: 0, nicheId: "psychology_facts" }
// ---------------------------------------------------------------------------
app.post('/api/niches/select', requireAuth, async (req, res) => {
  try {
    const { channelIndex, nicheId } = req.body;
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    // Validate the niche exists
    const detail = nicheEngine.getNicheDetail(nicheId);
    if (detail.error) return res.status(404).json({ error: 'Niche not found' });

    // Store on the channel config (channels array on user document)
    if (!user.channels) user.channels = [];
    if (!user.channels[channelIndex]) user.channels[channelIndex] = {};
    user.channels[channelIndex].niche = nicheId;
    user.channels[channelIndex].nicheLabel = detail.label;
    user.channels[channelIndex].nicheSetAt = new Date();

    user.markModified('channels');
    await user.save();

    res.json({
      success: true,
      message: `Niche "${detail.label}" set for channel ${channelIndex + 1}`,
      channel: user.channels[channelIndex],
    });
  } catch (err) {
    console.error('[M4a] /api/niches/select error:', err);
    res.status(500).json({ error: 'Failed to save niche selection' });
  }
});
