/**
 * Basic Aggregation Stages Tests
 *
 * Tests for aggregation stages: $sortByCount, $sample, $facet, $bucket, $bucketAuto, $unionWith
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

describe(`Basic Aggregation Stages (${getTestModeName()})`, () => {
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

  describe('$sortByCount stage', () => {
    it('should group and count by field value', async () => {
      const collection = client.db(dbName).collection('sortbycount_basic');
      await collection.insertMany([
        { category: 'A' },
        { category: 'B' },
        { category: 'A' },
        { category: 'C' },
        { category: 'A' },
        { category: 'B' },
      ]);

      const docs = await collection.aggregate([{ $sortByCount: '$category' }]).toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0]._id, 'A');
      assert.strictEqual(docs[0].count, 3);
      assert.strictEqual(docs[1]._id, 'B');
      assert.strictEqual(docs[1].count, 2);
      assert.strictEqual(docs[2]._id, 'C');
      assert.strictEqual(docs[2].count, 1);
    });

    it('should handle null values', async () => {
      const collection = client.db(dbName).collection('sortbycount_null');
      await collection.insertMany([
        { status: 'active' },
        { status: null },
        { status: 'active' },
        { status: null },
      ]);

      const docs = await collection.aggregate([{ $sortByCount: '$status' }]).toArray();

      assert.strictEqual(docs.length, 2);
      const activeDoc = docs.find((d) => d._id === 'active');
      const nullDoc = docs.find((d) => d._id === null);
      assert.strictEqual(activeDoc?.count, 2);
      assert.strictEqual(nullDoc?.count, 2);
    });
  });

  describe('$sample stage', () => {
    it('should return specified number of documents', async () => {
      const collection = client.db(dbName).collection('sample_basic');
      await collection.insertMany(Array.from({ length: 10 }, (_, i) => ({ index: i })));

      const docs = await collection.aggregate([{ $sample: { size: 3 } }]).toArray();

      assert.strictEqual(docs.length, 3);
    });

    it('should return all documents if size exceeds collection', async () => {
      const collection = client.db(dbName).collection('sample_all');
      await collection.insertMany([{ a: 1 }, { a: 2 }, { a: 3 }]);

      const docs = await collection.aggregate([{ $sample: { size: 10 } }]).toArray();

      assert.strictEqual(docs.length, 3);
    });

    it('should throw error or return empty for size 0', async () => {
      const collection = client.db(dbName).collection('sample_zero');
      await collection.insertMany([{ a: 1 }, { a: 2 }]);

      try {
        const docs = await collection.aggregate([{ $sample: { size: 0 } }]).toArray();
        assert.strictEqual(docs.length, 0);
      } catch (err) {
        assert.ok(
          (err as Error).message.includes('size argument to $sample must be a positive integer')
        );
      }
    });
  });

  describe('$facet stage', () => {
    it('should run multiple pipelines', async () => {
      const collection = client.db(dbName).collection('facet_basic');
      await collection.insertMany([
        { type: 'A', value: 10 },
        { type: 'B', value: 20 },
        { type: 'A', value: 30 },
        { type: 'B', value: 40 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $facet: {
              byType: [{ $group: { _id: '$type', total: { $sum: '$value' } } }],
              count: [{ $count: 'total' }],
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.ok(Array.isArray(docs[0].byType));
      assert.ok(Array.isArray(docs[0].count));
      assert.strictEqual(docs[0].byType.length, 2);
      assert.strictEqual(docs[0].count[0].total, 4);
    });

    it('should handle empty pipelines', async () => {
      const collection = client.db(dbName).collection('facet_empty');
      await collection.insertMany([{ a: 1 }, { a: 2 }]);

      const docs = await collection
        .aggregate([
          {
            $facet: {
              all: [],
              limited: [{ $limit: 1 }],
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual((docs[0].all as unknown[]).length, 2);
      assert.strictEqual((docs[0].limited as unknown[]).length, 1);
    });
  });

  describe('$bucket stage', () => {
    it('should group into buckets', async () => {
      const collection = client.db(dbName).collection('bucket_basic');
      await collection.insertMany([
        { score: 15 },
        { score: 25 },
        { score: 35 },
        { score: 45 },
        { score: 55 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: '$score',
              boundaries: [0, 20, 40, 60],
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0]._id, 0);
      assert.strictEqual(docs[0].count, 1);
      assert.strictEqual(docs[1]._id, 20);
      assert.strictEqual(docs[1].count, 2);
      assert.strictEqual(docs[2]._id, 40);
      assert.strictEqual(docs[2].count, 2);
    });

    it('should use default bucket for out-of-range values', async () => {
      const collection = client.db(dbName).collection('bucket_default');
      await collection.insertMany([{ score: 5 }, { score: 15 }, { score: 100 }]);

      const docs = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: '$score',
              boundaries: [10, 20],
              default: 'other',
            },
          },
        ])
        .toArray();

      const tenBucket = docs.find((d) => d._id === 10);
      const otherBucket = docs.find((d) => d._id === 'other');

      assert.strictEqual(tenBucket?.count, 1);
      assert.strictEqual(otherBucket?.count, 2);
    });

    it('should support custom output', async () => {
      const collection = client.db(dbName).collection('bucket_output');
      await collection.insertMany([
        { score: 15, value: 100 },
        { score: 25, value: 200 },
        { score: 35, value: 300 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: '$score',
              boundaries: [0, 20, 40],
              output: {
                count: { $sum: 1 },
                total: { $sum: '$value' },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0]._id, 0);
      assert.strictEqual(docs[0].count, 1);
      assert.strictEqual(docs[0].total, 100);
      assert.strictEqual(docs[1]._id, 20);
      assert.strictEqual(docs[1].count, 2);
      assert.strictEqual(docs[1].total, 500);
    });
  });

  describe('$bucketAuto stage', () => {
    it('should automatically create buckets', async () => {
      const collection = client.db(dbName).collection('bucketauto_basic');
      await collection.insertMany([
        { value: 10 },
        { value: 20 },
        { value: 30 },
        { value: 40 },
        { value: 50 },
        { value: 60 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $bucketAuto: {
              groupBy: '$value',
              buckets: 3,
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 3);
      for (const doc of docs) {
        const id = doc._id as { min: unknown; max: unknown };
        assert.ok(id.min !== undefined);
        assert.ok(id.max !== undefined);
        assert.ok((doc.count as number) >= 1);
      }
    });

    it('should handle fewer docs than buckets', async () => {
      const collection = client.db(dbName).collection('bucketauto_few');
      await collection.insertMany([{ value: 10 }, { value: 20 }]);

      const docs = await collection
        .aggregate([
          {
            $bucketAuto: {
              groupBy: '$value',
              buckets: 5,
            },
          },
        ])
        .toArray();

      assert.ok(docs.length <= 2);
    });
  });

  describe('$unionWith stage', () => {
    it('should combine documents from two collections', async () => {
      const collection1 = client.db(dbName).collection('union_source');
      const collection2 = client.db(dbName).collection('union_other');

      await collection1.insertMany([{ source: 'A', val: 1 }]);
      await collection2.insertMany([{ source: 'B', val: 2 }]);

      const docs = await collection1.aggregate([{ $unionWith: { coll: 'union_other' } }]).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.source === 'A'));
      assert.ok(docs.some((d) => d.source === 'B'));
    });

    it('should apply pipeline to unioned collection', async () => {
      const collection1 = client.db(dbName).collection('union_pipe_src');
      const collection2 = client.db(dbName).collection('union_pipe_other');

      await collection1.insertMany([{ val: 1 }]);
      await collection2.insertMany([{ val: 10 }, { val: 20 }, { val: 30 }]);

      const docs = await collection1
        .aggregate([
          {
            $unionWith: {
              coll: 'union_pipe_other',
              pipeline: [{ $match: { val: { $gt: 15 } } }],
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 3);
    });
  });
});
