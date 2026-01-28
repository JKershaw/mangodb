/**
 * Index Utilization Tests
 *
 * Tests for index-based query optimization.
 * These tests verify that indexes are used correctly for query execution.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from '../../test-harness.ts';

describe(`Index Utilization Tests (${getTestModeName()})`, () => {
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

  describe('Equality queries', () => {
    it('should find document using equality on indexed field', async () => {
      const collection = client.db(dbName).collection('idx_util_eq');

      // Create index
      await collection.createIndex({ email: 1 });

      // Insert test data
      await collection.insertMany([
        { email: 'alice@test.com', name: 'Alice' },
        { email: 'bob@test.com', name: 'Bob' },
        { email: 'charlie@test.com', name: 'Charlie' },
      ]);

      // Query using indexed field
      const result = await collection.findOne({ email: 'bob@test.com' });

      assert.ok(result);
      assert.strictEqual(result.name, 'Bob');
    });

    it('should find document using _id index', async () => {
      const collection = client.db(dbName).collection('idx_util_id');

      const insertResult = await collection.insertOne({ name: 'Test' });
      const id = insertResult.insertedId;

      const result = await collection.findOne({ _id: id });

      assert.ok(result);
      assert.strictEqual(result.name, 'Test');
    });

    it('should find documents using compound index prefix', async () => {
      const collection = client.db(dbName).collection('idx_util_compound');

      await collection.createIndex({ lastName: 1, firstName: 1 });

      await collection.insertMany([
        { firstName: 'John', lastName: 'Smith' },
        { firstName: 'Jane', lastName: 'Smith' },
        { firstName: 'John', lastName: 'Doe' },
      ]);

      // Query on first field only (prefix match)
      const results = await collection.find({ lastName: 'Smith' }).toArray();

      assert.strictEqual(results.length, 2);
      assert.ok(results.every((r) => (r as { lastName: string }).lastName === 'Smith'));
    });

    it('should find documents using full compound key', async () => {
      const collection = client.db(dbName).collection('idx_util_compound_full');

      await collection.createIndex({ lastName: 1, firstName: 1 });

      await collection.insertMany([
        { firstName: 'John', lastName: 'Smith' },
        { firstName: 'Jane', lastName: 'Smith' },
        { firstName: 'John', lastName: 'Doe' },
      ]);

      // Query on both fields
      const results = await collection
        .find({ lastName: 'Smith', firstName: 'John' })
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual((results[0] as { firstName: string }).firstName, 'John');
    });
  });

  describe('Range queries', () => {
    it('should find documents using $gt on indexed field', async () => {
      const collection = client.db(dbName).collection('idx_util_gt');

      await collection.createIndex({ age: 1 });

      await collection.insertMany([
        { name: 'Alice', age: 20 },
        { name: 'Bob', age: 30 },
        { name: 'Charlie', age: 40 },
        { name: 'Diana', age: 50 },
      ]);

      const results = await collection.find({ age: { $gt: 30 } }).toArray();

      assert.strictEqual(results.length, 2);
      assert.ok(results.every((r) => (r as { age: number }).age > 30));
    });

    it('should find documents using $gte on indexed field', async () => {
      const collection = client.db(dbName).collection('idx_util_gte');

      await collection.createIndex({ age: 1 });

      await collection.insertMany([
        { name: 'Alice', age: 20 },
        { name: 'Bob', age: 30 },
        { name: 'Charlie', age: 40 },
      ]);

      const results = await collection.find({ age: { $gte: 30 } }).toArray();

      assert.strictEqual(results.length, 2);
      assert.ok(results.every((r) => (r as { age: number }).age >= 30));
    });

    it('should find documents using $lt on indexed field', async () => {
      const collection = client.db(dbName).collection('idx_util_lt');

      await collection.createIndex({ age: 1 });

      await collection.insertMany([
        { name: 'Alice', age: 20 },
        { name: 'Bob', age: 30 },
        { name: 'Charlie', age: 40 },
      ]);

      const results = await collection.find({ age: { $lt: 30 } }).toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual((results[0] as { name: string }).name, 'Alice');
    });

    it('should find documents using $lte on indexed field', async () => {
      const collection = client.db(dbName).collection('idx_util_lte');

      await collection.createIndex({ age: 1 });

      await collection.insertMany([
        { name: 'Alice', age: 20 },
        { name: 'Bob', age: 30 },
        { name: 'Charlie', age: 40 },
      ]);

      const results = await collection.find({ age: { $lte: 30 } }).toArray();

      assert.strictEqual(results.length, 2);
      assert.ok(results.every((r) => (r as { age: number }).age <= 30));
    });

    it('should find documents using bounded range on indexed field', async () => {
      const collection = client.db(dbName).collection('idx_util_range');

      await collection.createIndex({ age: 1 });

      await collection.insertMany([
        { name: 'Alice', age: 20 },
        { name: 'Bob', age: 30 },
        { name: 'Charlie', age: 40 },
        { name: 'Diana', age: 50 },
      ]);

      const results = await collection
        .find({ age: { $gte: 25, $lte: 45 } })
        .toArray();

      assert.strictEqual(results.length, 2);
      const ages = results.map((r) => (r as { age: number }).age).sort((a, b) => a - b);
      assert.deepStrictEqual(ages, [30, 40]);
    });
  });

  describe('Fallback to full scan', () => {
    it('should handle $or queries correctly', async () => {
      const collection = client.db(dbName).collection('idx_util_or');

      await collection.createIndex({ email: 1 });

      await collection.insertMany([
        { email: 'alice@test.com', name: 'Alice' },
        { email: 'bob@test.com', name: 'Bob' },
        { email: 'charlie@test.com', name: 'Charlie' },
      ]);

      // $or requires full scan (not optimized with indexes yet)
      const results = await collection
        .find({ $or: [{ email: 'alice@test.com' }, { name: 'Charlie' }] })
        .toArray();

      assert.strictEqual(results.length, 2);
    });

    it('should handle $ne queries correctly', async () => {
      const collection = client.db(dbName).collection('idx_util_ne');

      await collection.createIndex({ status: 1 });

      await collection.insertMany([
        { status: 'active', name: 'Alice' },
        { status: 'inactive', name: 'Bob' },
        { status: 'active', name: 'Charlie' },
      ]);

      // $ne requires full scan
      const results = await collection.find({ status: { $ne: 'active' } }).toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual((results[0] as { name: string }).name, 'Bob');
    });

    it('should handle queries on non-indexed fields correctly', async () => {
      const collection = client.db(dbName).collection('idx_util_noindex');

      await collection.createIndex({ email: 1 });

      await collection.insertMany([
        { email: 'alice@test.com', city: 'NYC' },
        { email: 'bob@test.com', city: 'LA' },
        { email: 'charlie@test.com', city: 'NYC' },
      ]);

      // Query on non-indexed field requires full scan
      const results = await collection.find({ city: 'NYC' }).toArray();

      assert.strictEqual(results.length, 2);
    });
  });

  describe('Index maintenance', () => {
    it('should maintain index after insertOne', async () => {
      const collection = client.db(dbName).collection('idx_util_maint_insert');

      await collection.createIndex({ email: 1 });

      await collection.insertOne({ email: 'alice@test.com', name: 'Alice' });
      await collection.insertOne({ email: 'bob@test.com', name: 'Bob' });

      // Query should find the new document
      const result = await collection.findOne({ email: 'bob@test.com' });
      assert.ok(result);
      assert.strictEqual(result.name, 'Bob');
    });

    it('should maintain index after updateOne', async () => {
      const collection = client.db(dbName).collection('idx_util_maint_update');

      await collection.createIndex({ email: 1 });

      await collection.insertOne({ email: 'alice@test.com', name: 'Alice' });

      // Update the email
      await collection.updateOne(
        { email: 'alice@test.com' },
        { $set: { email: 'alice.new@test.com' } }
      );

      // Old email should not find document
      const oldResult = await collection.findOne({ email: 'alice@test.com' });
      assert.strictEqual(oldResult, null);

      // New email should find document
      const newResult = await collection.findOne({ email: 'alice.new@test.com' });
      assert.ok(newResult);
      assert.strictEqual(newResult.name, 'Alice');
    });

    it('should maintain index after deleteOne', async () => {
      const collection = client.db(dbName).collection('idx_util_maint_delete');

      await collection.createIndex({ email: 1 });

      await collection.insertMany([
        { email: 'alice@test.com', name: 'Alice' },
        { email: 'bob@test.com', name: 'Bob' },
      ]);

      await collection.deleteOne({ email: 'alice@test.com' });

      // Deleted document should not be found
      const result = await collection.findOne({ email: 'alice@test.com' });
      assert.strictEqual(result, null);

      // Other document should still be findable
      const otherResult = await collection.findOne({ email: 'bob@test.com' });
      assert.ok(otherResult);
    });

    it('should maintain index after replaceOne', async () => {
      const collection = client.db(dbName).collection('idx_util_maint_replace');

      await collection.createIndex({ email: 1 });

      await collection.insertOne({ email: 'alice@test.com', name: 'Alice' });

      // Replace with new email
      await collection.replaceOne(
        { email: 'alice@test.com' },
        { email: 'alice.replaced@test.com', name: 'Alice Replaced' }
      );

      // Old email should not find document
      const oldResult = await collection.findOne({ email: 'alice@test.com' });
      assert.strictEqual(oldResult, null);

      // New email should find document
      const newResult = await collection.findOne({ email: 'alice.replaced@test.com' });
      assert.ok(newResult);
      assert.strictEqual(newResult.name, 'Alice Replaced');
    });
  });

  describe('Result correctness', () => {
    it('should return same results with and without index', async () => {
      const collection1 = client.db(dbName).collection('idx_util_correct_indexed');
      const collection2 = client.db(dbName).collection('idx_util_correct_noindex');

      // Same data in both collections
      const docs = [
        { email: 'alice@test.com', age: 30 },
        { email: 'bob@test.com', age: 25 },
        { email: 'charlie@test.com', age: 35 },
        { email: 'diana@test.com', age: 30 },
      ];

      await collection1.createIndex({ age: 1 });
      // collection2 has no index

      await collection1.insertMany([...docs]);
      await collection2.insertMany([...docs]);

      // Query both collections
      const result1 = await collection1.find({ age: 30 }).toArray();
      const result2 = await collection2.find({ age: 30 }).toArray();

      // Results should match
      assert.strictEqual(result1.length, result2.length);
      const emails1 = result1.map((r) => (r as { email: string }).email).sort();
      const emails2 = result2.map((r) => (r as { email: string }).email).sort();
      assert.deepStrictEqual(emails1, emails2);
    });

    it('should apply remaining filter after index lookup', async () => {
      const collection = client.db(dbName).collection('idx_util_remaining');

      await collection.createIndex({ status: 1 });

      await collection.insertMany([
        { status: 'active', city: 'NYC', name: 'Alice' },
        { status: 'active', city: 'LA', name: 'Bob' },
        { status: 'inactive', city: 'NYC', name: 'Charlie' },
      ]);

      // Query with indexed field + non-indexed field
      const results = await collection
        .find({ status: 'active', city: 'NYC' })
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual((results[0] as { name: string }).name, 'Alice');
    });
  });

  describe('Edge cases', () => {
    it('should handle null values in indexed field', async () => {
      const collection = client.db(dbName).collection('idx_util_null');

      await collection.createIndex({ email: 1 });

      await collection.insertMany([
        { email: 'alice@test.com', name: 'Alice' },
        { email: null, name: 'Bob' },
        { name: 'Charlie' }, // missing email
      ]);

      // Query for null should match both null and missing
      const results = await collection.find({ email: null }).toArray();

      assert.strictEqual(results.length, 2);
    });

    it('should handle empty collection', async () => {
      const collection = client.db(dbName).collection('idx_util_empty');

      await collection.createIndex({ email: 1 });

      const results = await collection.find({ email: 'test@test.com' }).toArray();

      assert.strictEqual(results.length, 0);
    });

    it('should handle interleaved writes and reads', async () => {
      const collection = client.db(dbName).collection('idx_util_interleaved');

      await collection.createIndex({ id: 1 });

      // Interleave inserts and reads
      for (let i = 0; i < 10; i++) {
        await collection.insertOne({ id: i, value: `value_${i}` });
        const result = await collection.findOne({ id: i });
        assert.ok(result);
        assert.strictEqual(result.id, i);
      }

      // All documents should be findable
      const allResults = await collection.find({}).toArray();
      assert.strictEqual(allResults.length, 10);
    });
  });
});
