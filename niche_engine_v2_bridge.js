/**
 * Viralityy / Shortvity — Niche Engine v2 Bridge (Node.js → Python)
 * -------------------------------------------------------------------
 * Drop-in replacement for niche_engine_bridge.js.
 * All existing calls (recommend, getNicheDetail, getCategories, compare)
 * continue to work unchanged — they hit Layer 1 only (offline, instant).
 *
 * New async methods (analyse, analyseBatch) hit both layers and require
 * YOUTUBE_API_KEY to be set in env for live YouTube data.
 *
 * Place this file alongside niche_engine_v2.py and server.js.
 */

const { execSync, exec } = require('child_process');
const path = require('path');

const PYTHON_SCRIPT = path.join(__dirname, 'niche_engine_v2.py');

// ── Synchronous helper (Layer 1 calls — fast, no API) ─────────────────────
function runPythonSync(args) {
  try {
    const cmd    = `python3 "${PYTHON_SCRIPT}" ${args}`;
    const output = execSync(cmd, { timeout: 10000 }).toString().trim();
    return JSON.parse(output);
  } catch (err) {
    console.error('[NicheV2 Bridge] Sync error:', err.message);
    throw new Error('Niche engine unavailable');
  }
}

// ── Async helper (Layer 1+2 calls — hits YouTube API) ─────────────────────
function runPythonAsync(args, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const cmd = `python3 "${PYTHON_SCRIPT}" ${args}`;
    exec(cmd, { timeout: timeoutMs }, (err, stdout, stderr) => {
      if (err) {
        console.error('[NicheV2 Bridge] Async error:', stderr || err.message);
        return reject(new Error('Niche engine unavailable'));
      }
      try {
        resolve(JSON.parse(stdout.trim()));
      } catch (parseErr) {
        reject(new Error('Niche engine returned invalid JSON'));
      }
    });
  });
}

class NicheEngineV2 {

  // ── Layer 1 — offline, backwards-compatible ──────────────────────────────

  recommend(plan, count = 10, category = null) {
    const cat = category ? `--category "${category}"` : '';
    return runPythonSync(`recommend --plan ${plan} --count ${count} ${cat}`);
  }

  getNicheDetail(nicheId) {
    return runPythonSync(`detail --niche ${nicheId}`);
  }

  getCategories() {
    return runPythonSync('categories');
  }

  compare(nicheIds) {
    return runPythonSync(`compare --niches ${nicheIds.join(',')}`);
  }

  // ── Layer 1+2 — async, live YouTube data ─────────────────────────────────

  /**
   * Full two-layer analysis for a single keyword.
   * Returns the combined NicheScore report.
   * Falls back to Layer 1 only if YOUTUBE_API_KEY is missing.
   *
   * @param {string} keyword   - e.g. "stoicism", "AI tools"
   * @param {string} plan      - e.g. "combo_pro", "shorts_pro"
   * @returns {Promise<object>}
   */
  async analyse(keyword, plan = 'combo_pro') {
    const kw = keyword.replace(/"/g, '');
    return runPythonAsync(`analyse --keyword "${kw}" --plan ${plan}`, 45000);
  }

  /**
   * Keyword search — user types any topic, gets a full scored report
   * plus related search suggestions.
   *
   * @param {string} keyword
   * @param {string} plan
   * @returns {Promise<object>}
   */
  async search(keyword, plan = 'combo_pro') {
    const kw = keyword.replace(/"/g, '');
    return runPythonAsync(`search --keyword "${kw}" --plan ${plan}`, 45000);
  }

  /**
   * Real-time niche discovery — no keyword needed.
   * Probes ~38 seed topics via YouTube API and returns the top
   * high-performing niches ranked by combined score.
   *
   * @param {string}  plan         - user's plan
   * @param {number}  topN         - how many results to return (default 10)
   * @param {number}  minScore     - minimum combined score threshold (default 60)
   * @param {boolean} forceRefresh - bypass 24h cache (default false)
   * @returns {Promise<object>}
   */
  async discover(plan = 'combo_pro', topN = 10, minScore = 60, forceRefresh = false) {
    const force = forceRefresh ? '--force' : '';
    return runPythonAsync(
      `discover --plan ${plan} --top ${topN} --min-score ${minScore} ${force}`,
      180000   // discovery probes ~38 seeds — allow up to 3 min
    );
  }

  /**
   * Batch analysis — up to 10 keywords concurrently.
   * Returns array sorted by combined_score descending.
   *
   * @param {string[]} keywords
   * @param {string}   plan
   * @returns {Promise<object[]>}
   */
  async analyseBatch(keywords, plan = 'combo_pro') {
    const kws = keywords.map(k => k.replace(/"/g, '')).join(',');
    return runPythonAsync(`batch --keywords "${kws}" --plan ${plan}`, 120000);
  }
}

module.exports = { NicheEngineV2 };
