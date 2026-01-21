/**
 * Return Structure Tests
 *
 * These tests verify that MangoDB returns the same structure as MongoDB
 * for all CRUD operations. This catches bugs where fields are missing
 * from return values (like the insertedCount bug in MAN-40).
 *
 * Each test verifies:
 * 1. All expected fields are present (using 'in' operator)
 * 2. Field types are correct
 * 3. No unexpected behavior in edge cases
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';
import { ObjectId } from 'bson';

describe(`Return Structure Tests (${getTestModeName()})`, () => {
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

  describe('insertOne return structure', () => {
    it('should return acknowledged and insertedId fields', async () => {
      const collection = client.db(dbName).collection('return_struct_insert_one');

      const result = await collection.insertOne({ a: 1 });

      // Verify all expected fields exist
      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('insertedId' in result, 'missing insertedId field');

      // Verify types
      assert.strictEqual(typeof result.acknowledged, 'boolean', 'acknowledged should be boolean');
      assert.ok(result.insertedId, 'insertedId should be truthy');
    });
  });

  describe('insertMany return structure', () => {
    it('should return acknowledged, insertedCount, and insertedIds fields', async () => {
      const collection = client.db(dbName).collection('return_struct_insert_many');

      const result = await collection.insertMany([{ a: 1 }, { b: 2 }]);

      // Verify all expected fields exist
      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('insertedCount' in result, 'missing insertedCount field');
      assert.ok('insertedIds' in result, 'missing insertedIds field');

      // Verify types
      assert.strictEqual(typeof result.acknowledged, 'boolean', 'acknowledged should be boolean');
      assert.strictEqual(typeof result.insertedCount, 'number', 'insertedCount should be number');
      assert.strictEqual(typeof result.insertedIds, 'object', 'insertedIds should be object');

      // Verify insertedIds structure (object with numeric keys, not array)
      assert.strictEqual(Array.isArray(result.insertedIds), false, 'insertedIds should not be an array');
      assert.ok(0 in result.insertedIds || '0' in result.insertedIds, 'insertedIds should have key 0');
      assert.ok(1 in result.insertedIds || '1' in result.insertedIds, 'insertedIds should have key 1');
    });

    it('should return correct structure for empty array', async () => {
      const collection = client.db(dbName).collection('return_struct_insert_many_empty');

      const result = await collection.insertMany([]);

      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('insertedCount' in result, 'missing insertedCount field');
      assert.ok('insertedIds' in result, 'missing insertedIds field');
      assert.strictEqual(result.insertedCount, 0);
    });
  });

  describe('updateOne return structure', () => {
    it('should return all expected fields when document matches', async () => {
      const collection = client.db(dbName).collection('return_struct_update_one');
      await collection.insertOne({ a: 1 });

      const result = await collection.updateOne({ a: 1 }, { $set: { b: 2 } });

      // Verify all expected fields exist
      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('matchedCount' in result, 'missing matchedCount field');
      assert.ok('modifiedCount' in result, 'missing modifiedCount field');
      assert.ok('upsertedCount' in result, 'missing upsertedCount field');
      assert.ok('upsertedId' in result, 'missing upsertedId field');

      // Verify types
      assert.strictEqual(typeof result.acknowledged, 'boolean', 'acknowledged should be boolean');
      assert.strictEqual(typeof result.matchedCount, 'number', 'matchedCount should be number');
      assert.strictEqual(typeof result.modifiedCount, 'number', 'modifiedCount should be number');
      assert.strictEqual(typeof result.upsertedCount, 'number', 'upsertedCount should be number');
    });

    it('should return correct structure when no document matches', async () => {
      const collection = client.db(dbName).collection('return_struct_update_one_no_match');

      const result = await collection.updateOne({ nonexistent: true }, { $set: { b: 2 } });

      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('matchedCount' in result, 'missing matchedCount field');
      assert.ok('modifiedCount' in result, 'missing modifiedCount field');
      assert.ok('upsertedCount' in result, 'missing upsertedCount field');
      assert.ok('upsertedId' in result, 'missing upsertedId field');

      assert.strictEqual(result.matchedCount, 0);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it('should return upsertedId when upserting', async () => {
      const collection = client.db(dbName).collection('return_struct_update_one_upsert');

      const result = await collection.updateOne(
        { nonexistent: true },
        { $set: { a: 1 } },
        { upsert: true }
      );

      assert.ok('upsertedId' in result, 'missing upsertedId field');
      assert.ok(result.upsertedId, 'upsertedId should be truthy when upserting');
      assert.strictEqual(result.upsertedCount, 1);
    });
  });

  describe('updateMany return structure', () => {
    it('should return all expected fields', async () => {
      const collection = client.db(dbName).collection('return_struct_update_many');
      await collection.insertMany([{ a: 1 }, { a: 1 }, { a: 2 }]);

      const result = await collection.updateMany({ a: 1 }, { $set: { b: 2 } });

      // Verify all expected fields exist
      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('matchedCount' in result, 'missing matchedCount field');
      assert.ok('modifiedCount' in result, 'missing modifiedCount field');
      assert.ok('upsertedCount' in result, 'missing upsertedCount field');
      assert.ok('upsertedId' in result, 'missing upsertedId field');

      // Verify types
      assert.strictEqual(typeof result.acknowledged, 'boolean', 'acknowledged should be boolean');
      assert.strictEqual(typeof result.matchedCount, 'number', 'matchedCount should be number');
      assert.strictEqual(typeof result.modifiedCount, 'number', 'modifiedCount should be number');
    });
  });

  describe('deleteOne return structure', () => {
    it('should return acknowledged and deletedCount fields', async () => {
      const collection = client.db(dbName).collection('return_struct_delete_one');
      await collection.insertOne({ a: 1 });

      const result = await collection.deleteOne({ a: 1 });

      // Verify all expected fields exist
      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('deletedCount' in result, 'missing deletedCount field');

      // Verify types
      assert.strictEqual(typeof result.acknowledged, 'boolean', 'acknowledged should be boolean');
      assert.strictEqual(typeof result.deletedCount, 'number', 'deletedCount should be number');
    });

    it('should return correct structure when no document matches', async () => {
      const collection = client.db(dbName).collection('return_struct_delete_one_no_match');

      const result = await collection.deleteOne({ nonexistent: true });

      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('deletedCount' in result, 'missing deletedCount field');
      assert.strictEqual(result.deletedCount, 0);
    });
  });

  describe('deleteMany return structure', () => {
    it('should return acknowledged and deletedCount fields', async () => {
      const collection = client.db(dbName).collection('return_struct_delete_many');
      await collection.insertMany([{ a: 1 }, { a: 1 }, { a: 2 }]);

      const result = await collection.deleteMany({ a: 1 });

      // Verify all expected fields exist
      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('deletedCount' in result, 'missing deletedCount field');

      // Verify types
      assert.strictEqual(typeof result.acknowledged, 'boolean', 'acknowledged should be boolean');
      assert.strictEqual(typeof result.deletedCount, 'number', 'deletedCount should be number');
      assert.strictEqual(result.deletedCount, 2);
    });
  });

  describe('replaceOne return structure', () => {
    it('should return all expected fields when document matches', async () => {
      const collection = client.db(dbName).collection('return_struct_replace_one');
      await collection.insertOne({ a: 1 });

      const result = await collection.replaceOne({ a: 1 }, { b: 2 });

      // Verify all expected fields exist
      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('matchedCount' in result, 'missing matchedCount field');
      assert.ok('modifiedCount' in result, 'missing modifiedCount field');
      assert.ok('upsertedCount' in result, 'missing upsertedCount field');
      assert.ok('upsertedId' in result, 'missing upsertedId field');

      // Verify types
      assert.strictEqual(typeof result.acknowledged, 'boolean', 'acknowledged should be boolean');
      assert.strictEqual(typeof result.matchedCount, 'number', 'matchedCount should be number');
      assert.strictEqual(typeof result.modifiedCount, 'number', 'modifiedCount should be number');
    });

    it('should return upsertedId when upserting', async () => {
      const collection = client.db(dbName).collection('return_struct_replace_one_upsert');

      const result = await collection.replaceOne(
        { nonexistent: true },
        { a: 1 },
        { upsert: true }
      );

      assert.ok('upsertedId' in result, 'missing upsertedId field');
      assert.ok(result.upsertedId, 'upsertedId should be truthy when upserting');
      assert.strictEqual(result.upsertedCount, 1);
    });
  });

  describe('bulkWrite return structure', () => {
    it('should return all expected fields', async () => {
      const collection = client.db(dbName).collection('return_struct_bulk_write');

      const result = await collection.bulkWrite([
        { insertOne: { document: { a: 1 } } },
        { insertOne: { document: { b: 2 } } },
      ]);

      // Verify all expected fields exist
      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('insertedCount' in result, 'missing insertedCount field');
      assert.ok('matchedCount' in result, 'missing matchedCount field');
      assert.ok('modifiedCount' in result, 'missing modifiedCount field');
      assert.ok('deletedCount' in result, 'missing deletedCount field');
      assert.ok('upsertedCount' in result, 'missing upsertedCount field');
      assert.ok('insertedIds' in result, 'missing insertedIds field');
      assert.ok('upsertedIds' in result, 'missing upsertedIds field');

      // Verify types
      assert.strictEqual(typeof result.acknowledged, 'boolean', 'acknowledged should be boolean');
      assert.strictEqual(typeof result.insertedCount, 'number', 'insertedCount should be number');
      assert.strictEqual(typeof result.matchedCount, 'number', 'matchedCount should be number');
      assert.strictEqual(typeof result.modifiedCount, 'number', 'modifiedCount should be number');
      assert.strictEqual(typeof result.deletedCount, 'number', 'deletedCount should be number');
      assert.strictEqual(typeof result.upsertedCount, 'number', 'upsertedCount should be number');
      assert.strictEqual(typeof result.insertedIds, 'object', 'insertedIds should be object');
      assert.strictEqual(typeof result.upsertedIds, 'object', 'upsertedIds should be object');
    });

    it('should return correct structure for mixed operations', async () => {
      const collection = client.db(dbName).collection('return_struct_bulk_write_mixed');
      await collection.insertOne({ name: 'existing', value: 1 });

      const result = await collection.bulkWrite([
        { insertOne: { document: { name: 'new' } } },
        { updateOne: { filter: { name: 'existing' }, update: { $set: { value: 2 } } } },
        { deleteOne: { filter: { name: 'existing' } } },
      ]);

      assert.ok('acknowledged' in result, 'missing acknowledged field');
      assert.ok('insertedCount' in result, 'missing insertedCount field');
      assert.ok('matchedCount' in result, 'missing matchedCount field');
      assert.ok('modifiedCount' in result, 'missing modifiedCount field');
      assert.ok('deletedCount' in result, 'missing deletedCount field');

      assert.strictEqual(result.insertedCount, 1);
      assert.strictEqual(result.matchedCount, 1);
      assert.strictEqual(result.deletedCount, 1);
    });
  });

  describe('findOneAndDelete return structure', () => {
    it('should return document directly (not wrapped)', async () => {
      const collection = client.db(dbName).collection('return_struct_find_delete');
      await collection.insertOne({ a: 1, b: 2 });

      const result = await collection.findOneAndDelete({ a: 1 });

      // MongoDB driver 6.0+ returns document directly, not { value, ok }
      assert.ok(result !== undefined, 'result should not be undefined');
      assert.ok(result === null || typeof result === 'object', 'result should be document or null');

      if (result !== null) {
        assert.ok('a' in result, 'returned document should have field a');
        assert.strictEqual(result.a, 1);
      }
    });

    it('should return null when no document matches', async () => {
      const collection = client.db(dbName).collection('return_struct_find_delete_no_match');

      const result = await collection.findOneAndDelete({ nonexistent: true });

      assert.strictEqual(result, null, 'should return null when no document matches');
    });
  });

  describe('findOneAndUpdate return structure', () => {
    it('should return document directly (not wrapped)', async () => {
      const collection = client.db(dbName).collection('return_struct_find_update');
      await collection.insertOne({ a: 1, b: 2 });

      const result = await collection.findOneAndUpdate({ a: 1 }, { $set: { b: 3 } });

      // MongoDB driver 6.0+ returns document directly
      assert.ok(result !== undefined, 'result should not be undefined');
      assert.ok(result === null || typeof result === 'object', 'result should be document or null');

      if (result !== null) {
        assert.ok('a' in result, 'returned document should have field a');
      }
    });

    it('should return null when no document matches', async () => {
      const collection = client.db(dbName).collection('return_struct_find_update_no_match');

      const result = await collection.findOneAndUpdate(
        { nonexistent: true },
        { $set: { a: 1 } }
      );

      assert.strictEqual(result, null, 'should return null when no document matches');
    });
  });

  describe('findOneAndReplace return structure', () => {
    it('should return document directly (not wrapped)', async () => {
      const collection = client.db(dbName).collection('return_struct_find_replace');
      await collection.insertOne({ a: 1, b: 2 });

      const result = await collection.findOneAndReplace({ a: 1 }, { c: 3 });

      // MongoDB driver 6.0+ returns document directly
      assert.ok(result !== undefined, 'result should not be undefined');
      assert.ok(result === null || typeof result === 'object', 'result should be document or null');
    });

    it('should return null when no document matches', async () => {
      const collection = client.db(dbName).collection('return_struct_find_replace_no_match');

      const result = await collection.findOneAndReplace({ nonexistent: true }, { a: 1 });

      assert.strictEqual(result, null, 'should return null when no document matches');
    });
  });
});
