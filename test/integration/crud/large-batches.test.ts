/**
 * Large Batch Tests
 *
 * Tests for handling large batch operations that could previously cause
 * RangeError: Invalid string length due to JSON.stringify on entire collection.
 *
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

describe(`Large Batch Tests (${getTestModeName()})`, () => {
  let client: TestClient;
  let cleanup: () => Promise<void>;
  let dbName: string;

  before(async () => {
    const result = await createTestClient();
    client = result.client;
    cleanup = result.cleanup;
    dbName = result.dbName;
    await client.connect();
  });

  after(async () => {
    await cleanup();
  });

  describe('insertMany with large batches', () => {
    it('should insert 1000 documents successfully', async () => {
      const collection = client.db(dbName).collection('test_large_batch_1k');

      const docs = Array.from({ length: 1000 }, (_, i) => ({
        index: i,
        data: `document-${i}`,
      }));

      const result = await collection.insertMany(docs);

      assert.strictEqual(result.acknowledged, true);
      assert.strictEqual(result.insertedCount, 1000);

      const count = await collection.countDocuments();
      assert.strictEqual(count, 1000);
    });

    it('should insert 5000 documents with moderate-sized fields', async () => {
      const collection = client.db(dbName).collection('test_large_batch_5k');

      // Each document ~500 bytes, total ~2.5MB
      const docs = Array.from({ length: 5000 }, (_, i) => ({
        index: i,
        data: 'x'.repeat(400),
        timestamp: new Date().toISOString(),
      }));

      const result = await collection.insertMany(docs);

      assert.strictEqual(result.acknowledged, true);
      assert.strictEqual(result.insertedCount, 5000);

      const count = await collection.countDocuments();
      assert.strictEqual(count, 5000);
    });

    it('should insert documents with large fields', async () => {
      const collection = client.db(dbName).collection('test_large_fields');

      // 100 documents with 10KB fields each = ~1MB total
      const docs = Array.from({ length: 100 }, (_, i) => ({
        index: i,
        largeField: 'x'.repeat(10000),
      }));

      const result = await collection.insertMany(docs);

      assert.strictEqual(result.acknowledged, true);
      assert.strictEqual(result.insertedCount, 100);

      // Verify data integrity
      const retrieved = await collection.findOne({ index: 50 });
      assert.ok(retrieved);
      assert.strictEqual((retrieved.largeField as string).length, 10000);
    });

    it('should maintain data integrity after large batch insert', async () => {
      const collection = client.db(dbName).collection('test_integrity');

      const docs = Array.from({ length: 500 }, (_, i) => ({
        index: i,
        nested: {
          value: i * 2,
          array: [i, i + 1, i + 2],
        },
      }));

      await collection.insertMany(docs);

      // Verify first, middle, and last documents
      const first = await collection.findOne({ index: 0 });
      const middle = await collection.findOne({ index: 250 });
      const last = await collection.findOne({ index: 499 });

      assert.ok(first);
      const firstNested = first.nested as { value: number; array: number[] };
      assert.strictEqual(firstNested.value, 0);
      assert.deepStrictEqual(firstNested.array, [0, 1, 2]);

      assert.ok(middle);
      const middleNested = middle.nested as { value: number; array: number[] };
      assert.strictEqual(middleNested.value, 500);
      assert.deepStrictEqual(middleNested.array, [250, 251, 252]);

      assert.ok(last);
      const lastNested = last.nested as { value: number; array: number[] };
      assert.strictEqual(lastNested.value, 998);
      assert.deepStrictEqual(lastNested.array, [499, 500, 501]);
    });

    it('should handle multiple sequential large inserts', async () => {
      const collection = client.db(dbName).collection('test_sequential');

      // First batch
      const batch1 = Array.from({ length: 500 }, (_, i) => ({
        batch: 1,
        index: i,
      }));
      await collection.insertMany(batch1);

      // Second batch
      const batch2 = Array.from({ length: 500 }, (_, i) => ({
        batch: 2,
        index: i,
      }));
      await collection.insertMany(batch2);

      // Third batch
      const batch3 = Array.from({ length: 500 }, (_, i) => ({
        batch: 3,
        index: i,
      }));
      await collection.insertMany(batch3);

      const total = await collection.countDocuments();
      assert.strictEqual(total, 1500);

      const batch2Count = await collection.countDocuments({ batch: 2 });
      assert.strictEqual(batch2Count, 500);
    });
  });

  describe('operations after large batch insert', () => {
    it('should support find operations after large insert', async () => {
      const collection = client.db(dbName).collection('test_find_after_large');

      const docs = Array.from({ length: 1000 }, (_, i) => ({
        index: i,
        category: i % 10,
      }));
      await collection.insertMany(docs);

      const category5 = await collection.find({ category: 5 }).toArray();
      assert.strictEqual(category5.length, 100);
    });

    it('should support update operations after large insert', async () => {
      const collection = client.db(dbName).collection('test_update_after_large');

      const docs = Array.from({ length: 500 }, (_, i) => ({
        index: i,
        status: 'pending',
      }));
      await collection.insertMany(docs);

      await collection.updateMany({ index: { $lt: 100 } }, { $set: { status: 'processed' } });

      const processed = await collection.countDocuments({ status: 'processed' });
      assert.strictEqual(processed, 100);

      const pending = await collection.countDocuments({ status: 'pending' });
      assert.strictEqual(pending, 400);
    });

    it('should support delete operations after large insert', async () => {
      const collection = client.db(dbName).collection('test_delete_after_large');

      const docs = Array.from({ length: 500 }, (_, i) => ({
        index: i,
        toDelete: i >= 400,
      }));
      await collection.insertMany(docs);

      await collection.deleteMany({ toDelete: true });

      const remaining = await collection.countDocuments();
      assert.strictEqual(remaining, 400);
    });
  });

  describe('reading back large collections', () => {
    // The write path was hardened first (streaming writes); the read path used
    // to load the whole collection file into one string, so a collection that
    // grew past V8's max string length became permanently unreadable — and
    // unshrinkable, since a delete has to read the current documents first.
    // These run against both targets, so they assert behaviour rather than
    // MangoDB's storage format.
    it('should read back every document of a large collection', async () => {
      const collection = client.db(dbName).collection('test_read_back_large');

      const docs = Array.from({ length: 2000 }, (_, i) => ({
        index: i,
        payload: 'y'.repeat(500),
      }));
      await collection.insertMany(docs);

      const readBack = await collection.find({}).toArray();
      assert.strictEqual(readBack.length, 2000);

      const indexes = readBack.map((d) => d.index as number).sort((a, b) => a - b);
      assert.strictEqual(indexes[0], 0);
      assert.strictEqual(indexes[1999], 1999);
      assert.ok(
        readBack.every((d) => (d.payload as string).length === 500),
        'every document should survive the round trip intact'
      );
    });

    it('should shrink a large collection back down', async () => {
      const collection = client.db(dbName).collection('test_shrink_large');

      const docs = Array.from({ length: 1500 }, (_, i) => ({
        index: i,
        payload: 'z'.repeat(500),
      }));
      await collection.insertMany(docs);

      await collection.deleteMany({ index: { $lt: 1000 } });

      assert.strictEqual(await collection.countDocuments(), 500);
      const survivors = await collection.find({}).toArray();
      assert.ok(
        survivors.every((d) => (d.index as number) >= 1000),
        'only the documents outside the delete filter should remain'
      );
    });

    it('should report a non-zero document count in db.stats()', async () => {
      const db = client.db(dbName);
      const collection = db.collection('test_stats_large');

      const docs = Array.from({ length: 1200 }, (_, i) => ({
        index: i,
        payload: 'w'.repeat(500),
      }));
      await collection.insertMany(docs);

      const stats = await db.stats();
      assert.ok(
        stats.objects >= 1200,
        `stats().objects should count the documents, got ${stats.objects}`
      );
    });
  });
});
