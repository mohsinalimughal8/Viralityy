#!/usr/bin/env node
// One-time fix: delete the test_diag@viralityy.com user.
// Usage on Railway shell: MONGODB_URI="..." node delete-test-user.js
// Or locally:             MONGODB_URI="your-uri" node delete-test-user.js
'use strict';
const { MongoClient } = require('mongodb');

const TARGET_EMAIL = 'test_diag@viralityy.com';

(async () => {
  const uri = process.env.MONGODB_URI;
  if (!uri) { console.error('ERROR: Set MONGODB_URI env var before running.'); process.exit(1); }

  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db();
    const result = await db.collection('users').deleteOne({ email: TARGET_EMAIL });
    if (result.deletedCount === 0) {
      console.log(`No user found with email=${TARGET_EMAIL} — nothing deleted.`);
    } else {
      console.log(`✓ Deleted user ${TARGET_EMAIL}`);
    }
  } finally {
    await client.close();
  }
})().catch(err => { console.error(err.message); process.exit(1); });
