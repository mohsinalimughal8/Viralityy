/**
 * Viralityy — M4a: Niche Engine Bridge (Node.js to Python)
 * Place this file in the SAME folder as niche_engine.py and server.js.
 */

const { execSync } = require('child_process');
const path = require('path');

const PYTHON_SCRIPT = path.join(__dirname, 'niche_engine_cli.py');

function runPython(args) {
  try {
    const cmd = `python3 "${PYTHON_SCRIPT}" ${args}`;
    const output = execSync(cmd, { timeout: 10000 }).toString().trim();
    return JSON.parse(output);
  } catch (err) {
    console.error('[NicheEngine Bridge] Python error:', err.message);
    throw new Error('Niche engine unavailable');
  }
}

class NicheEngine {
  recommend(plan, count = 10, category = null) {
    const cat = category ? `--category "${category}"` : '';
    return runPython(`recommend --plan ${plan} --count ${count} ${cat}`);
  }

  getNicheDetail(nicheId) {
    return runPython(`detail --niche ${nicheId}`);
  }

  getCategories() {
    return runPython('categories');
  }

  compare(nicheIds) {
    const ids = nicheIds.join(',');
    return runPython(`compare --niches ${ids}`);
  }
}

module.exports = { NicheEngine };
