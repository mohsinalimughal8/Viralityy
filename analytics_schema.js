/**
 * Viralityy — M5a: VideoAnalytics MongoDB Schema & Indexes
 * ---------------------------------------------------------
 * HOW TO RUN:
 *   node analytics_schema.js
 *
 * Run this ONCE after deploying M5a to create the collection,
 * indexes, and validation rules in your MongoDB database.
 * Safe to run again — it skips anything already created.
 *
 * Requires MONGODB_URI in your .env file.
 */

require('dotenv').config();
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const DB_NAME   = 'viralityy';

async function setup() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db(DB_NAME);

  console.log('Connected to MongoDB:', DB_NAME);

  // ── 1. Create collection with schema validation ──────────────────
  const collections = await db.listCollections({ name: 'video_analytics' }).toArray();
  if (collections.length === 0) {
    await db.createCollection('video_analytics', {
      validator: {
        $jsonSchema: {
          bsonType: 'object',
          required: ['videoId', 'userId', 'snapshotDate', 'stats'],
          properties: {
            videoId: {
              bsonType: 'string',
              description: 'YouTube video ID — required'
            },
            userId: {
              bsonType: 'string',
              description: 'Viralityy user ID — required'
            },
            snapshotDate: {
              bsonType: 'string',
              pattern: '^\\d{4}-\\d{2}-\\d{2}$',
              description: 'Date of this snapshot YYYY-MM-DD — required'
            },
            title: { bsonType: 'string' },
            channelId: { bsonType: 'string' },
            publishedAt: { bsonType: 'string' },
            duration: { bsonType: 'string' },
            collectedAt: { bsonType: 'string' },
            collectionVersion: { bsonType: 'string' },

            stats: {
              bsonType: 'object',
              required: ['views', 'likes'],
              properties: {
                views:     { bsonType: 'int' },
                likes:     { bsonType: 'int' },
                comments:  { bsonType: 'int' },
                favorites: { bsonType: 'int' },
                likeRatio: { bsonType: 'double' },
              }
            },

            // Analytics API fields (populated by M7)
            watchTime:       { bsonType: ['double', 'null'] },
            avgViewDuration: { bsonType: ['double', 'null'] },
            avgViewPct:      { bsonType: ['double', 'null'] },
            impressions:     { bsonType: ['int', 'null'] },
            ctr:             { bsonType: ['double', 'null'] },

            revenue: {
              bsonType: 'object',
              properties: {
                rpm:                { bsonType: ['double', 'null'] },
                cpm:                { bsonType: ['double', 'null'] },
                estimatedEarnings:  { bsonType: ['double', 'null'] },
              }
            },

            trafficSources:   { bsonType: 'object' },
            subscriberDelta:  { bsonType: ['int', 'null'] },

            flags: {
              bsonType: 'object',
              properties: {
                viewPerformance: { bsonType: 'string', enum: ['good', 'average', 'poor'] },
                engagement:      { bsonType: 'string', enum: ['good', 'average', 'poor'] },
                thumbnailCtr:    { bsonType: 'string', enum: ['good', 'average', 'poor'] },
              }
            },
          }
        }
      },
      validationLevel: 'moderate',  // warn but don't block on invalid docs
      validationAction: 'warn',
    });
    console.log('✓ Created collection: video_analytics');
  } else {
    console.log('✓ Collection already exists: video_analytics');
  }

  const col = db.collection('video_analytics');

  // ── 2. Create indexes ────────────────────────────────────────────

  // Primary lookup: one snapshot per video per user per day
  await col.createIndex(
    { videoId: 1, userId: 1, snapshotDate: 1 },
    { unique: true, name: 'idx_video_user_date' }
  );
  console.log('✓ Index: idx_video_user_date (unique)');

  // Dashboard queries — all videos for a user, most recent first
  await col.createIndex(
    { userId: 1, snapshotDate: -1 },
    { name: 'idx_user_date_desc' }
  );
  console.log('✓ Index: idx_user_date_desc');

  // Performance filtering — find underperformers quickly
  await col.createIndex(
    { userId: 1, 'flags.viewPerformance': 1 },
    { name: 'idx_user_view_performance' }
  );
  console.log('✓ Index: idx_user_view_performance');

  // Sorting by views for top-video queries
  await col.createIndex(
    { userId: 1, 'stats.views': -1 },
    { name: 'idx_user_views_desc' }
  );
  console.log('✓ Index: idx_user_views_desc');

  // TTL index — auto-delete snapshots older than 365 days to control storage
  // collectedAt must be a proper Date for TTL to work
  await col.createIndex(
    { collectedAt: 1 },
    { expireAfterSeconds: 365 * 24 * 60 * 60, name: 'idx_ttl_365days' }
  );
  console.log('✓ Index: idx_ttl_365days (auto-delete after 1 year)');

  // ── 3. Also create posted_videos collection if missing ───────────
  // This is what analytics_engine.py reads to find which videos to track
  const pv = await db.listCollections({ name: 'posted_videos' }).toArray();
  if (pv.length === 0) {
    await db.createCollection('posted_videos');
    const pvCol = db.collection('posted_videos');
    await pvCol.createIndex({ userId: 1, platform: 1, postedAt: -1 }, { name: 'idx_pv_user_platform' });
    await pvCol.createIndex({ videoId: 1 }, { unique: true, sparse: true, name: 'idx_pv_videoid' });
    console.log('✓ Created collection: posted_videos (with indexes)');
  } else {
    console.log('✓ Collection already exists: posted_videos');
  }

  await client.close();
  console.log('\nM5a schema setup complete.');
}

setup().catch(err => {
  console.error('Setup failed:', err);
  process.exit(1);
});
