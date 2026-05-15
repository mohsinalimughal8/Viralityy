#!/usr/bin/env node
// One-time fix: set plan=agency for a specific user.
// Usage on Railway shell: MONGODB_URI="..." node fix-user-plan.js
// Or locally:             MONGODB_URI="your-uri" node fix-user-plan.js
'use strict';
const { MongoClient, ObjectId } = require('mongodb');

const TARGET_USER_ID = '69f96df4338535c4793a02e1';
const TARGET_PLAN    = 'agency';

(async () => {
  const uri = process.env.MONGODB_URI;
  if (!uri) { console.error('ERROR: Set MONGODB_URI env var before running.'); process.exit(1); }

  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db();
    const result = await db.collection('users').updateOne(
      { _id: new ObjectId(TARGET_USER_ID) },
      { $set: { plan: TARGET_PLAN, planUpdatedAt: new Date() } }
    );
    if (result.matchedCount === 0) {
      console.error(`No user found with _id=${TARGET_USER_ID}`);
    } else {
      console.log(`✓ plan set to "${TARGET_PLAN}" for user ${TARGET_USER_ID} (modified: ${result.modifiedCount})`);
    }
  } finally {
    await client.close();
  }
})().catch(err => { console.error(err.message); process.exit(1); });
