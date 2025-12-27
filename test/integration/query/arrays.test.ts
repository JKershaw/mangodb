/**
 * Phase 6: Array Operator Tests
 *
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

describe(`Array Query Tests (${getTestModeName()})`, () => {
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

  describe('$size operator', () => {
    it('should match arrays with exact size', async () => {
      const collection = client.db(dbName).collection('size_exact');
      await collection.insertMany([
        { tags: ['a', 'b'] },
        { tags: ['a', 'b', 'c'] },
        { tags: ['a'] },
      ]);

      const docs = await collection.find({ tags: { $size: 2 } }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.deepStrictEqual(docs[0].tags, ['a', 'b']);
    });

    it('should match empty arrays with size 0', async () => {
      const collection = client.db(dbName).collection('size_empty');
      await collection.insertMany([{ items: [] }, { items: ['a'] }, { items: ['a', 'b'] }]);

      const docs = await collection.find({ items: { $size: 0 } }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.deepStrictEqual(docs[0].items, []);
    });

    it('should not match non-array fields', async () => {
      const collection = client.db(dbName).collection('size_nonarray');
      await collection.insertMany([{ value: 'string' }, { value: 123 }, { value: { a: 1 } }]);

      const docs = await collection.find({ value: { $size: 1 } }).toArray();

      assert.strictEqual(docs.length, 0);
    });

    it('should not match missing fields', async () => {
      const collection = client.db(dbName).collection('size_missing');
      await collection.insertMany([{ other: 'field' }, { tags: ['a', 'b'] }]);

      const docs = await collection.find({ tags: { $size: 0 } }).toArray();

      // Missing field is not the same as empty array
      assert.strictEqual(docs.length, 0);
    });

    it('should not match null fields', async () => {
      const collection = client.db(dbName).collection('size_null');
      await collection.insertMany([{ items: null }, { items: [] }]);

      const docs = await collection.find({ items: { $size: 0 } }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.deepStrictEqual(docs[0].items, []);
    });

    it('should work with nested arrays using dot notation', async () => {
      const collection = client.db(dbName).collection('size_nested');
      await collection.insertMany([
        { user: { tags: ['a', 'b', 'c'] } },
        { user: { tags: ['a', 'b'] } },
      ]);

      const docs = await collection.find({ 'user.tags': { $size: 3 } }).toArray();

      assert.strictEqual(docs.length, 1);
    });
  });

  describe('$all operator', () => {
    it('should match arrays containing all specified elements', async () => {
      const collection = client.db(dbName).collection('all_basic');
      await collection.insertMany([
        { tags: ['a', 'b', 'c'] },
        { tags: ['a', 'd'] },
        { tags: ['b', 'c'] },
      ]);

      const docs = await collection.find({ tags: { $all: ['a', 'b'] } }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.deepStrictEqual(docs[0].tags, ['a', 'b', 'c']);
    });

    it('should not match when missing any element', async () => {
      const collection = client.db(dbName).collection('all_missing');
      await collection.insertMany([{ tags: ['a', 'b'] }, { tags: ['a', 'c'] }]);

      const docs = await collection.find({ tags: { $all: ['a', 'b', 'c'] } }).toArray();

      assert.strictEqual(docs.length, 0);
    });

    it('should ignore element order', async () => {
      const collection = client.db(dbName).collection('all_order');
      await collection.insertMany([{ tags: ['c', 'a', 'b'] }, { tags: ['a', 'b', 'c'] }]);

      const docs = await collection.find({ tags: { $all: ['b', 'a'] } }).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it('should match when array has extra elements', async () => {
      const collection = client.db(dbName).collection('all_extra');
      await collection.insertMany([{ tags: ['a', 'b', 'c', 'd', 'e'] }]);

      const docs = await collection.find({ tags: { $all: ['a', 'c'] } }).toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should handle single element in $all', async () => {
      const collection = client.db(dbName).collection('all_single');
      await collection.insertMany([{ tags: ['a', 'b'] }, { tags: ['c', 'd'] }]);

      const docs = await collection.find({ tags: { $all: ['a'] } }).toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should handle empty $all array (matches nothing)', async () => {
      const collection = client.db(dbName).collection('all_empty');
      await collection.insertMany([{ tags: ['a', 'b'] }, { tags: [] }, { value: 'not array' }]);

      const docs = await collection.find({ tags: { $all: [] } }).toArray();

      // Empty $all matches nothing (MongoDB behavior)
      assert.strictEqual(docs.length, 0);
    });

    it('should match scalar fields containing the value', async () => {
      const collection = client.db(dbName).collection('all_nonarray');
      await collection.insertMany([{ value: 'a' }, { value: 123 }]);

      // $all can work on non-array fields - scalar "a" contains "a"
      const docs = await collection.find({ value: { $all: ['a'] } }).toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should not match missing fields', async () => {
      const collection = client.db(dbName).collection('all_field_missing');
      await collection.insertMany([{ other: 'field' }]);

      const docs = await collection.find({ tags: { $all: ['a'] } }).toArray();

      assert.strictEqual(docs.length, 0);
    });

    it('should handle duplicate values in $all', async () => {
      const collection = client.db(dbName).collection('all_duplicates');
      await collection.insertMany([{ tags: ['a', 'b'] }]);

      // Duplicates in $all should work the same as without duplicates
      const docs = await collection.find({ tags: { $all: ['a', 'a', 'b'] } }).toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should match nested arrays', async () => {
      const collection = client.db(dbName).collection('all_nested_arr');
      await collection.insertMany([
        {
          matrix: [
            [1, 2],
            [3, 4],
          ],
        },
        { matrix: [[5, 6]] },
      ]);

      const docs = await collection.find({ matrix: { $all: [[1, 2]] } }).toArray();

      assert.strictEqual(docs.length, 1);
    });
  });

  describe('$elemMatch operator', () => {
    it('should match when single element satisfies all conditions', async () => {
      const collection = client.db(dbName).collection('elemmatch_basic');
      await collection.insertMany([
        {
          results: [
            { score: 80, passed: true },
            { score: 60, passed: false },
          ],
        },
        {
          results: [
            { score: 70, passed: true },
            { score: 90, passed: false },
          ],
        },
      ]);

      const docs = await collection
        .find({
          results: { $elemMatch: { score: { $gte: 80 }, passed: true } },
        })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should not match when conditions satisfied by different elements', async () => {
      const collection = client.db(dbName).collection('elemmatch_diff_elem');
      await collection.insertMany([
        {
          results: [
            { score: 80, passed: false },
            { score: 60, passed: true },
          ],
        },
      ]);

      // score >= 80 is on first element, passed: true is on second
      // $elemMatch requires SAME element to match all
      const docs = await collection
        .find({
          results: { $elemMatch: { score: { $gte: 80 }, passed: true } },
        })
        .toArray();

      assert.strictEqual(docs.length, 0);
    });

    it('should work with comparison operators', async () => {
      const collection = client.db(dbName).collection('elemmatch_compare');
      await collection.insertMany([
        { scores: [10, 20, 30] },
        { scores: [5, 15, 25] },
        { scores: [50, 60, 70] },
      ]);

      const docs = await collection
        .find({ scores: { $elemMatch: { $gte: 25, $lt: 35 } } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it('should work with equality on primitives', async () => {
      const collection = client.db(dbName).collection('elemmatch_primitive');
      await collection.insertMany([{ tags: ['red', 'blue'] }, { tags: ['green', 'yellow'] }]);

      const docs = await collection.find({ tags: { $elemMatch: { $eq: 'red' } } }).toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should not match empty arrays', async () => {
      const collection = client.db(dbName).collection('elemmatch_empty');
      await collection.insertMany([{ items: [] }, { items: [{ value: 10 }] }]);

      const docs = await collection
        .find({ items: { $elemMatch: { value: { $gt: 0 } } } })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should not match non-array fields', async () => {
      const collection = client.db(dbName).collection('elemmatch_nonarray');
      await collection.insertMany([{ value: { score: 100 } }, { value: [{ score: 100 }] }]);

      const docs = await collection.find({ value: { $elemMatch: { score: 100 } } }).toArray();

      // Only the array should match
      assert.strictEqual(docs.length, 1);
    });

    it('should not match missing fields', async () => {
      const collection = client.db(dbName).collection('elemmatch_missing');
      await collection.insertMany([{ other: 'field' }]);

      const docs = await collection.find({ items: { $elemMatch: { value: 1 } } }).toArray();

      assert.strictEqual(docs.length, 0);
    });

    it('should handle nested objects in array elements', async () => {
      const collection = client.db(dbName).collection('elemmatch_nested_obj');
      await collection.insertMany([
        {
          items: [{ product: { name: 'A', price: 10 } }, { product: { name: 'B', price: 20 } }],
        },
      ]);

      const docs = await collection
        .find({
          items: {
            $elemMatch: { 'product.name': 'A', 'product.price': { $lt: 15 } },
          },
        })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should work with $in inside $elemMatch', async () => {
      const collection = client.db(dbName).collection('elemmatch_in');
      await collection.insertMany([
        { items: [{ status: 'active' }, { status: 'pending' }] },
        { items: [{ status: 'deleted' }, { status: 'archived' }] },
      ]);

      const docs = await collection
        .find({
          items: { $elemMatch: { status: { $in: ['active', 'pending'] } } },
        })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });

    it('should handle empty $elemMatch object (matches nothing)', async () => {
      const collection = client.db(dbName).collection('elemmatch_empty_cond');
      await collection.insertMany([{ items: [1, 2, 3] }, { items: [] }, { value: 'not array' }]);

      const docs = await collection.find({ items: { $elemMatch: {} } }).toArray();

      // Empty $elemMatch matches nothing (per MongoDB behavior)
      assert.strictEqual(docs.length, 0);
    });

    it('should work with $not inside $elemMatch', async () => {
      const collection = client.db(dbName).collection('elemmatch_not');
      await collection.insertMany([
        { items: [{ status: 'active' }, { status: 'deleted' }] },
        { items: [{ status: 'deleted' }, { status: 'archived' }] },
        { items: [{ status: 'active' }, { status: 'pending' }] },
      ]);

      // Find documents with at least one item NOT deleted
      const docs = await collection
        .find({
          items: { $elemMatch: { status: { $not: { $eq: 'deleted' } } } },
        })
        .toArray();

      // First doc has "active", third has "active" and "pending"
      // Second doc only has "deleted" and "archived" - "archived" matches $not deleted
      assert.strictEqual(docs.length, 3);
    });
  });
});

describe(`Array Update Tests (${getTestModeName()})`, () => {
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

  describe('$push operator', () => {
    it('should append value to array', async () => {
      const collection = client.db(dbName).collection('push_basic');
      await collection.insertOne({ name: 'test', tags: ['a', 'b'] });

      await collection.updateOne({ name: 'test' }, { $push: { tags: 'c' } });

      const doc = await collection.findOne({ name: 'test' });
      assert.deepStrictEqual(doc?.tags, ['a', 'b', 'c']);
    });

    it('should create array if field missing', async () => {
      const collection = client.db(dbName).collection('push_create');
      await collection.insertOne({ name: 'test' });

      await collection.updateOne({ name: 'test' }, { $push: { tags: 'first' } });

      const doc = await collection.findOne({ name: 'test' });
      assert.deepStrictEqual(doc?.tags, ['first']);
    });

    it('should work with dot notation', async () => {
      const collection = client.db(dbName).collection('push_nested');
      await collection.insertOne({ user: { tags: ['a'] } });

      await collection.updateOne({}, { $push: { 'user.tags': 'b' } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual((doc?.user as { tags: string[] }).tags, ['a', 'b']);
    });

    it('should push object to array', async () => {
      const collection = client.db(dbName).collection('push_object');
      await collection.insertOne({ items: [{ id: 1 }] });

      await collection.updateOne({}, { $push: { items: { id: 2 } } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.items, [{ id: 1 }, { id: 2 }]);
    });

    it('should push array as single element (without $each)', async () => {
      const collection = client.db(dbName).collection('push_array_elem');
      await collection.insertOne({ matrix: [[1, 2]] });

      await collection.updateOne({}, { $push: { matrix: [3, 4] } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.matrix, [
        [1, 2],
        [3, 4],
      ]);
    });

    it('should handle $each modifier', async () => {
      const collection = client.db(dbName).collection('push_each');
      await collection.insertOne({ tags: ['a'] });

      await collection.updateOne({}, { $push: { tags: { $each: ['b', 'c', 'd'] } } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['a', 'b', 'c', 'd']);
    });

    it('should handle $each with empty array', async () => {
      const collection = client.db(dbName).collection('push_each_empty');
      await collection.insertOne({ tags: ['a', 'b'] });

      const result = await collection.updateOne({}, { $push: { tags: { $each: [] } } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['a', 'b']);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it('should create array with $each if field missing', async () => {
      const collection = client.db(dbName).collection('push_each_create');
      await collection.insertOne({ name: 'test' });

      await collection.updateOne({}, { $push: { tags: { $each: ['a', 'b'] } } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['a', 'b']);
    });

    it('should throw error for non-array field', async () => {
      const collection = client.db(dbName).collection('push_error');
      await collection.insertOne({ name: 'Alice' });

      await assert.rejects(
        async () => await collection.updateOne({}, { $push: { name: 'value' } }),
        (err: Error) => {
          // MongoDB error formats vary by version
          return (
            err.message.includes('must be an array') ||
            err.message.includes('non-array') ||
            err.message.includes('Cannot apply $push')
          );
        }
      );
    });

    it('should throw error for null field', async () => {
      const collection = client.db(dbName).collection('push_null');
      await collection.insertOne({ tags: null });

      await assert.rejects(
        async () => await collection.updateOne({}, { $push: { tags: 'x' } }),
        (err: Error) => {
          return (
            err.message.includes('must be an array') ||
            err.message.includes('non-array') ||
            err.message.includes('Cannot apply $push')
          );
        }
      );
    });

    it('should work with updateMany', async () => {
      const collection = client.db(dbName).collection('push_many');
      await collection.insertMany([
        { type: 'a', tags: ['x'] },
        { type: 'a', tags: ['y'] },
        { type: 'b', tags: ['z'] },
      ]);

      await collection.updateMany({ type: 'a' }, { $push: { tags: 'new' } });

      const docs = await collection.find({ type: 'a' }).toArray();
      assert.ok(docs.every((d) => (d.tags as string[]).includes('new')));
    });

    it('should work with deeply nested dot notation', async () => {
      const collection = client.db(dbName).collection('push_deep_nested');
      await collection.insertOne({ a: { b: { c: { tags: ['x'] } } } });

      await collection.updateOne({}, { $push: { 'a.b.c.tags': 'y' } });

      const doc = await collection.findOne({});
      const tags = (doc?.a as { b: { c: { tags: string[] } } }).b.c.tags;
      assert.deepStrictEqual(tags, ['x', 'y']);
    });

    it('should create deeply nested structure if missing', async () => {
      const collection = client.db(dbName).collection('push_deep_create');
      await collection.insertOne({ name: 'test' });

      await collection.updateOne({}, { $push: { 'a.b.c.tags': 'first' } });

      const doc = await collection.findOne({});
      const tags = (doc?.a as { b: { c: { tags: string[] } } }).b.c.tags;
      assert.deepStrictEqual(tags, ['first']);
    });
  });

  describe('$pull operator', () => {
    it('should remove matching values', async () => {
      const collection = client.db(dbName).collection('pull_basic');
      await collection.insertOne({ tags: ['a', 'b', 'c', 'b'] });

      await collection.updateOne({}, { $pull: { tags: 'b' } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['a', 'c']);
    });

    it('should remove values matching condition', async () => {
      const collection = client.db(dbName).collection('pull_condition');
      await collection.insertOne({ scores: [20, 60, 40, 80] });

      await collection.updateOne({}, { $pull: { scores: { $lt: 50 } } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.scores, [60, 80]);
    });

    it('should remove objects matching condition', async () => {
      const collection = client.db(dbName).collection('pull_objects');
      await collection.insertOne({
        items: [
          { name: 'a', status: 'active' },
          { name: 'b', status: 'deleted' },
          { name: 'c', status: 'active' },
        ],
      });

      await collection.updateOne({}, { $pull: { items: { status: 'deleted' } } });

      const doc = await collection.findOne({});
      const items = doc?.items as { name: string; status: string }[];
      assert.strictEqual(items.length, 2);
      assert.ok(items.every((i) => i.status === 'active'));
    });

    it('should handle missing field (no-op)', async () => {
      const collection = client.db(dbName).collection('pull_missing');
      await collection.insertOne({ name: 'test' });

      const result = await collection.updateOne({}, { $pull: { tags: 'x' } });

      assert.strictEqual(result.matchedCount, 1);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it('should handle empty array (no-op)', async () => {
      const collection = client.db(dbName).collection('pull_empty');
      await collection.insertOne({ tags: [] });

      await collection.updateOne({}, { $pull: { tags: 'x' } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, []);
    });

    it('should handle non-existent value (no-op)', async () => {
      const collection = client.db(dbName).collection('pull_nonexistent');
      await collection.insertOne({ tags: ['a', 'b'] });

      const result = await collection.updateOne({}, { $pull: { tags: 'x' } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['a', 'b']);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it('should pull all matching elements', async () => {
      const collection = client.db(dbName).collection('pull_all_match');
      await collection.insertOne({ scores: [1, 2, 3, 4, 5] });

      await collection.updateOne({}, { $pull: { scores: { $gte: 1 } } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.scores, []);
    });

    it('should work with dot notation', async () => {
      const collection = client.db(dbName).collection('pull_nested');
      await collection.insertOne({ user: { tags: ['a', 'b', 'c'] } });

      await collection.updateOne({}, { $pull: { 'user.tags': 'b' } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual((doc?.user as { tags: string[] }).tags, ['a', 'c']);
    });

    it('should throw error for non-array field', async () => {
      const collection = client.db(dbName).collection('pull_error');
      await collection.insertOne({ name: 'Alice' });

      await assert.rejects(
        async () => await collection.updateOne({}, { $pull: { name: 'x' } }),
        (err: Error) => {
          return (
            err.message.includes('non-array') ||
            err.message.includes('must be an array') ||
            err.message.includes('Cannot apply $pull')
          );
        }
      );
    });

    it('should throw error for null field', async () => {
      const collection = client.db(dbName).collection('pull_null_err');
      await collection.insertOne({ tags: null });

      await assert.rejects(
        async () => await collection.updateOne({}, { $pull: { tags: 'x' } }),
        (err: Error) => {
          return (
            err.message.includes('non-array') ||
            err.message.includes('must be an array') ||
            err.message.includes('Cannot apply $pull')
          );
        }
      );
    });

    it('should work with updateMany', async () => {
      const collection = client.db(dbName).collection('pull_many');
      await collection.insertMany([{ tags: ['a', 'remove', 'b'] }, { tags: ['c', 'remove', 'd'] }]);

      await collection.updateMany({}, { $pull: { tags: 'remove' } });

      const docs = await collection.find({}).toArray();
      assert.ok(docs.every((d) => !(d.tags as string[]).includes('remove')));
    });
  });

  describe('$addToSet operator', () => {
    it('should add value if not present', async () => {
      const collection = client.db(dbName).collection('addtoset_new');
      await collection.insertOne({ tags: ['a', 'b'] });

      await collection.updateOne({}, { $addToSet: { tags: 'c' } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['a', 'b', 'c']);
    });

    it('should not add duplicate value', async () => {
      const collection = client.db(dbName).collection('addtoset_dup');
      await collection.insertOne({ tags: ['a', 'b'] });

      const result = await collection.updateOne({}, { $addToSet: { tags: 'a' } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['a', 'b']);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it('should create array if field missing', async () => {
      const collection = client.db(dbName).collection('addtoset_create');
      await collection.insertOne({ name: 'test' });

      await collection.updateOne({}, { $addToSet: { tags: 'first' } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['first']);
    });

    it('should handle $each modifier', async () => {
      const collection = client.db(dbName).collection('addtoset_each');
      await collection.insertOne({ tags: ['a', 'b'] });

      await collection.updateOne({}, { $addToSet: { tags: { $each: ['b', 'c', 'd'] } } });

      const doc = await collection.findOne({});
      // "b" already exists, only "c" and "d" added
      assert.deepStrictEqual(doc?.tags, ['a', 'b', 'c', 'd']);
    });

    it('should preserve duplicates in $each when field is new', async () => {
      const collection = client.db(dbName).collection('addtoset_each_dup_new');
      await collection.insertOne({ name: 'test' });

      // $each with duplicates on a new field - only unique values should be added
      await collection.updateOne({}, { $addToSet: { tags: { $each: ['a', 'a', 'b', 'a', 'b'] } } });

      const doc = await collection.findOne({});
      // MongoDB adds unique values only, even for new field
      assert.deepStrictEqual(doc?.tags, ['a', 'b']);
    });

    it('should use deep equality for objects', async () => {
      const collection = client.db(dbName).collection('addtoset_object');
      await collection.insertOne({ items: [{ id: 1, name: 'a' }] });

      // Same object (deep equal) should not be added
      await collection.updateOne({}, { $addToSet: { items: { id: 1, name: 'a' } } });

      const doc = await collection.findOne({});
      assert.strictEqual((doc?.items as unknown[]).length, 1);
    });

    it('should treat objects with different key order as different', async () => {
      const collection = client.db(dbName).collection('addtoset_order');
      await collection.insertOne({ items: [{ id: 1, name: 'a' }] });

      // MongoDB treats { name: "a", id: 1 } as different from { id: 1, name: "a" }
      await collection.updateOne({}, { $addToSet: { items: { name: 'a', id: 1 } } });

      const doc = await collection.findOne({});
      assert.strictEqual((doc?.items as unknown[]).length, 2);
    });

    it('should add array as single element (without $each)', async () => {
      const collection = client.db(dbName).collection('addtoset_array');
      await collection.insertOne({ matrix: [[1, 2]] });

      await collection.updateOne({}, { $addToSet: { matrix: [3, 4] } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.matrix, [
        [1, 2],
        [3, 4],
      ]);
    });

    it('should not add duplicate array element', async () => {
      const collection = client.db(dbName).collection('addtoset_dup_arr');
      await collection.insertOne({
        matrix: [
          [1, 2],
          [3, 4],
        ],
      });

      await collection.updateOne({}, { $addToSet: { matrix: [1, 2] } });

      const doc = await collection.findOne({});
      assert.strictEqual((doc?.matrix as unknown[]).length, 2);
    });

    it('should throw error for non-array field', async () => {
      const collection = client.db(dbName).collection('addtoset_error');
      await collection.insertOne({ name: 'Alice' });

      await assert.rejects(
        async () => await collection.updateOne({}, { $addToSet: { name: 'x' } }),
        (err: Error) => {
          return (
            err.message.includes('must be an array') ||
            err.message.includes('non-array') ||
            err.message.includes('Cannot apply $addToSet')
          );
        }
      );
    });

    it('should throw error for null field', async () => {
      const collection = client.db(dbName).collection('addtoset_null');
      await collection.insertOne({ tags: null });

      await assert.rejects(
        async () => await collection.updateOne({}, { $addToSet: { tags: 'x' } }),
        (err: Error) => {
          return (
            err.message.includes('must be an array') ||
            err.message.includes('non-array') ||
            err.message.includes('Cannot apply $addToSet')
          );
        }
      );
    });

    it('should work with updateMany', async () => {
      const collection = client.db(dbName).collection('addtoset_many');
      await collection.insertMany([{ tags: ['a'] }, { tags: ['b'] }]);

      await collection.updateMany({}, { $addToSet: { tags: 'new' } });

      const docs = await collection.find({}).toArray();
      assert.ok(docs.every((d) => (d.tags as string[]).includes('new')));
    });
  });

  describe('$pop operator', () => {
    it('should remove last element with 1', async () => {
      const collection = client.db(dbName).collection('pop_last');
      await collection.insertOne({ items: ['a', 'b', 'c'] });

      await collection.updateOne({}, { $pop: { items: 1 } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.items, ['a', 'b']);
    });

    it('should remove first element with -1', async () => {
      const collection = client.db(dbName).collection('pop_first');
      await collection.insertOne({ items: ['a', 'b', 'c'] });

      await collection.updateOne({}, { $pop: { items: -1 } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.items, ['b', 'c']);
    });

    it('should handle empty array (no-op)', async () => {
      const collection = client.db(dbName).collection('pop_empty');
      await collection.insertOne({ items: [] });

      const result = await collection.updateOne({}, { $pop: { items: 1 } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.items, []);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it('should handle single element array', async () => {
      const collection = client.db(dbName).collection('pop_single');
      await collection.insertOne({ items: ['only'] });

      await collection.updateOne({}, { $pop: { items: 1 } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.items, []);
    });

    it('should handle missing field (no-op)', async () => {
      const collection = client.db(dbName).collection('pop_missing');
      await collection.insertOne({ name: 'test' });

      const result = await collection.updateOne({}, { $pop: { items: 1 } });

      assert.strictEqual(result.matchedCount, 1);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it('should work with dot notation', async () => {
      const collection = client.db(dbName).collection('pop_nested');
      await collection.insertOne({ user: { items: ['a', 'b', 'c'] } });

      await collection.updateOne({}, { $pop: { 'user.items': 1 } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual((doc?.user as { items: string[] }).items, ['a', 'b']);
    });

    it('should throw error for non-array field', async () => {
      const collection = client.db(dbName).collection('pop_error');
      await collection.insertOne({ name: 'Alice' });

      await assert.rejects(
        async () => await collection.updateOne({}, { $pop: { name: 1 } }),
        (err: Error) => {
          return (
            err.message.includes('non-array') ||
            err.message.includes('must be an array') ||
            err.message.includes('Cannot apply $pop')
          );
        }
      );
    });

    it('should throw error for null field', async () => {
      const collection = client.db(dbName).collection('pop_null');
      await collection.insertOne({ items: null });

      await assert.rejects(
        async () => await collection.updateOne({}, { $pop: { items: 1 } }),
        (err: Error) => {
          return (
            err.message.includes('non-array') ||
            err.message.includes('must be an array') ||
            err.message.includes('Cannot apply $pop')
          );
        }
      );
    });

    it('should work with updateMany', async () => {
      const collection = client.db(dbName).collection('pop_many');
      await collection.insertMany([{ items: ['a', 'b', 'c'] }, { items: ['x', 'y', 'z'] }]);

      await collection.updateMany({}, { $pop: { items: -1 } });

      const docs = await collection.find({}).toArray();
      assert.deepStrictEqual(docs[0].items, ['b', 'c']);
      assert.deepStrictEqual(docs[1].items, ['y', 'z']);
    });
  });

  describe('Combined array operators', () => {
    it('should combine $push with $set', async () => {
      const collection = client.db(dbName).collection('combined_push_set');
      await collection.insertOne({ name: 'test', tags: ['a'] });

      await collection.updateOne({}, { $push: { tags: 'b' }, $set: { name: 'updated' } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.name, 'updated');
      assert.deepStrictEqual(doc?.tags, ['a', 'b']);
    });

    it('should combine $pull with $inc', async () => {
      const collection = client.db(dbName).collection('combined_pull_inc');
      await collection.insertOne({ count: 10, tags: ['a', 'b', 'c'] });

      await collection.updateOne({}, { $pull: { tags: 'b' }, $inc: { count: 1 } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.count, 11);
      assert.deepStrictEqual(doc?.tags, ['a', 'c']);
    });

    it('should combine multiple array operators', async () => {
      const collection = client.db(dbName).collection('combined_multi');
      await collection.insertOne({
        tags: ['a', 'b'],
        scores: [10, 20],
        items: ['x', 'y', 'z'],
      });

      await collection.updateOne(
        {},
        {
          $push: { tags: 'c' },
          $addToSet: { scores: 20 }, // Already exists, no change
          $pop: { items: -1 },
        }
      );

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ['a', 'b', 'c']);
      assert.deepStrictEqual(doc?.scores, [10, 20]);
      assert.deepStrictEqual(doc?.items, ['y', 'z']);
    });
  });
});
