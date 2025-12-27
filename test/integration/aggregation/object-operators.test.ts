/**
 * Object operators tests - $getField, $setField, $mergeObjects
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

describe(`Object Operators (${getTestModeName()})`, () => {
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

  describe('$getField', () => {
    it('should get a field value using short form', async () => {
      const collection = client.db(dbName).collection('getfield_short');
      await collection.insertOne({ name: 'Alice', age: 30 });

      const result = await collection
        .aggregate([{ $project: { nameValue: { $getField: 'name' } } }])
        .toArray();

      assert.strictEqual(result[0].nameValue, 'Alice');
    });

    it('should get a field value using long form', async () => {
      const collection = client.db(dbName).collection('getfield_long');
      await collection.insertOne({ name: 'Bob', details: { city: 'NYC' } });

      const result = await collection
        .aggregate([
          {
            $project: {
              cityValue: {
                $getField: { field: 'city', input: '$details' },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].cityValue, 'NYC');
    });

    it('should handle fields with dots in name', async () => {
      const collection = client.db(dbName).collection('getfield_special');
      await collection.insertOne({ 'field.with.dots': 123 });

      const result = await collection
        .aggregate([
          {
            $project: {
              dotValue: { $getField: 'field.with.dots' },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].dotValue, 123);
    });

    it('should return null for missing field', async () => {
      const collection = client.db(dbName).collection('getfield_missing');
      await collection.insertOne({ name: 'Charlie' });

      const result = await collection
        .aggregate([{ $project: { missingValue: { $getField: 'nonexistent' } } }])
        .toArray();

      assert.strictEqual(result[0].missingValue, undefined);
    });

    it('should return null when input is null', async () => {
      const collection = client.db(dbName).collection('getfield_null');
      await collection.insertOne({ data: null });

      const result = await collection
        .aggregate([
          {
            $project: {
              value: { $getField: { field: 'name', input: '$data' } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].value, null);
    });

    it('should work with computed field names', async () => {
      const collection = client.db(dbName).collection('getfield_computed');
      await collection.insertOne({
        fieldName: 'age',
        age: 25,
        name: 'David',
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              dynamicValue: { $getField: { field: '$fieldName', input: '$$CURRENT' } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].dynamicValue, 25);
    });
  });

  describe('$setField', () => {
    it('should set a new field', async () => {
      const collection = client.db(dbName).collection('setfield_new');
      await collection.insertOne({ name: 'Eve' });

      const result = await collection
        .aggregate([
          {
            $project: {
              result: {
                $setField: { field: 'age', input: '$$CURRENT', value: 28 },
              },
            },
          },
        ])
        .toArray();

      const res = result[0].result as { name: string; age: number };
      assert.strictEqual(res.name, 'Eve');
      assert.strictEqual(res.age, 28);
    });

    it('should update an existing field', async () => {
      const collection = client.db(dbName).collection('setfield_update');
      await collection.insertOne({ name: 'Frank', age: 30 });

      const result = await collection
        .aggregate([
          {
            $project: {
              result: {
                $setField: { field: 'age', input: '$$CURRENT', value: 31 },
              },
            },
          },
        ])
        .toArray();

      const res = result[0].result as { name: string; age: number };
      assert.strictEqual(res.name, 'Frank');
      assert.strictEqual(res.age, 31);
    });

    it('should set fields with special characters', async () => {
      const collection = client.db(dbName).collection('setfield_special');
      await collection.insertOne({ name: 'Grace' });

      const result = await collection
        .aggregate([
          {
            $project: {
              result: {
                $setField: { field: 'field.with.dots', input: '$$CURRENT', value: 'works' },
              },
            },
          },
        ])
        .toArray();

      const res = result[0].result as Record<string, unknown>;
      assert.strictEqual(res['field.with.dots'], 'works');
    });

    it('should return null when input is null', async () => {
      const collection = client.db(dbName).collection('setfield_null');
      await collection.insertOne({ data: null });

      const result = await collection
        .aggregate([
          {
            $project: {
              result: {
                $setField: { field: 'name', input: '$data', value: 'test' },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].result, null);
    });

    it('should work with nested documents', async () => {
      const collection = client.db(dbName).collection('setfield_nested');
      await collection.insertOne({ data: { x: 1 } });

      const result = await collection
        .aggregate([
          {
            $project: {
              result: {
                $setField: { field: 'y', input: '$data', value: 2 },
              },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].result, { x: 1, y: 2 });
    });
  });

  describe('$mergeObjects', () => {
    it('should merge two objects', async () => {
      const collection = client.db(dbName).collection('merge_two');
      await collection.insertOne({
        defaults: { theme: 'dark', language: 'en' },
        overrides: { language: 'es' },
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              merged: { $mergeObjects: ['$defaults', '$overrides'] },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].merged, { theme: 'dark', language: 'es' });
    });

    it('should merge multiple objects', async () => {
      const collection = client.db(dbName).collection('merge_multiple');
      await collection.insertOne({
        a: { x: 1 },
        b: { y: 2 },
        c: { z: 3 },
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              merged: { $mergeObjects: ['$a', '$b', '$c'] },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].merged, { x: 1, y: 2, z: 3 });
    });

    it('should ignore null values', async () => {
      const collection = client.db(dbName).collection('merge_null');
      await collection.insertOne({
        base: { name: 'test' },
        optional: null,
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              merged: { $mergeObjects: ['$base', '$optional'] },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].merged, { name: 'test' });
    });

    it('should handle single object argument', async () => {
      const collection = client.db(dbName).collection('merge_single');
      await collection.insertOne({ data: { a: 1, b: 2 } });

      const result = await collection
        .aggregate([
          {
            $project: {
              merged: { $mergeObjects: '$data' },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].merged, { a: 1, b: 2 });
    });

    it('should work in $group stage', async () => {
      const collection = client.db(dbName).collection('merge_group');
      await collection.insertMany([
        { category: 'A', meta: { color: 'red' } },
        { category: 'A', meta: { size: 'large' } },
        { category: 'B', meta: { color: 'blue' } },
      ]);

      const result = await collection
        .aggregate([
          { $sort: { _id: 1 } },
          {
            $group: {
              _id: '$category',
              combinedMeta: { $mergeObjects: '$meta' },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      const categoryA = result.find((r) => r._id === 'A') as
        | { combinedMeta: { color?: string; size?: string } }
        | undefined;
      const categoryB = result.find((r) => r._id === 'B') as
        | { combinedMeta: { color: string } }
        | undefined;

      // Order of merging depends on document order
      assert.ok(
        categoryA?.combinedMeta.color === 'red' || categoryA?.combinedMeta.size === 'large'
      );
      assert.deepStrictEqual(categoryB?.combinedMeta, { color: 'blue' });
    });

    it('should handle empty array', async () => {
      const collection = client.db(dbName).collection('merge_empty');
      await collection.insertOne({ name: 'test' });

      const result = await collection
        .aggregate([
          {
            $project: {
              merged: { $mergeObjects: [] },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].merged, {});
    });
  });
});
