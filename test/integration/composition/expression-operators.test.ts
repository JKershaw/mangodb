/**
 * Expression Operator Composition Tests
 *
 * Tests for nested and composed expression operators in aggregation pipelines.
 *
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

describe(`Expression Operator Composition (${getTestModeName()})`, () => {
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

  describe('Array expression chains', () => {
    it('should support $filter → $map chain', async () => {
      const collection = client.db(dbName).collection('filter_map');
      await collection.insertOne({
        _id: 1,
        items: [
          { name: 'apple', qty: 10 },
          { name: 'banana', qty: 0 },
          { name: 'cherry', qty: 5 },
        ],
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              names: {
                $map: {
                  input: {
                    $filter: {
                      input: '$items',
                      cond: { $gt: ['$$this.qty', 0] },
                    },
                  },
                  in: '$$this.name',
                },
              },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].names, ['apple', 'cherry']);
    });

    it('should support $filter → $size chain', async () => {
      const collection = client.db(dbName).collection('filter_size');
      await collection.insertOne({
        _id: 1,
        tags: ['', 'valid', '', 'also-valid', ''],
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              validCount: {
                $size: {
                  $filter: {
                    input: '$tags',
                    cond: { $ne: ['$$this', ''] },
                  },
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].validCount, 2);
    });

    it('should support $map → $reduce chain', async () => {
      const collection = client.db(dbName).collection('map_reduce');
      await collection.insertOne({
        _id: 1,
        values: [1, 2, 3, 4],
      });

      // Double each value, then sum
      const result = await collection
        .aggregate([
          {
            $project: {
              doubledSum: {
                $reduce: {
                  input: {
                    $map: { input: '$values', in: { $multiply: ['$$this', 2] } },
                  },
                  initialValue: 0,
                  in: { $add: ['$$value', '$$this'] },
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].doubledSum, 20); // (1+2+3+4)*2 = 20
    });

    it('should support $filter with complex condition', async () => {
      const collection = client.db(dbName).collection('filter_complex');
      await collection.insertOne({
        _id: 1,
        items: [
          { name: 'a', qty: 5, active: true },
          { name: 'b', qty: 15, active: true },
          { name: 'c', qty: 3, active: false },
          { name: 'd', qty: 20, active: true },
        ],
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              filtered: {
                $filter: {
                  input: '$items',
                  cond: {
                    $and: [{ $gt: ['$$this.qty', 10] }, { $eq: ['$$this.active', true] }],
                  },
                },
              },
            },
          },
        ])
        .toArray();

      const filtered = result[0].filtered as Array<{ name: string }>;
      assert.strictEqual(filtered.length, 2);
      assert.strictEqual(filtered[0].name, 'b');
      assert.strictEqual(filtered[1].name, 'd');
    });

    it('should support nested $map for matrices', async () => {
      const collection = client.db(dbName).collection('nested_map');
      await collection.insertOne({
        _id: 1,
        matrix: [
          [1, 2],
          [3, 4],
        ],
      });

      // Double all matrix values
      const result = await collection
        .aggregate([
          {
            $project: {
              doubled: {
                $map: {
                  input: '$matrix',
                  as: 'row',
                  in: {
                    $map: {
                      input: '$$row',
                      in: { $multiply: ['$$this', 2] },
                    },
                  },
                },
              },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].doubled, [
        [2, 4],
        [6, 8],
      ]);
    });

    it('should support $arrayElemAt with computed index', async () => {
      const collection = client.db(dbName).collection('arrayelemat_computed');
      await collection.insertOne({
        _id: 1,
        items: ['first', 'second', 'third', 'fourth'],
      });

      // Get last element using computed index
      const result = await collection
        .aggregate([
          {
            $project: {
              last: {
                $arrayElemAt: ['$items', { $subtract: [{ $size: '$items' }, 1] }],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].last, 'fourth');
    });

    it('should support $concatArrays with $filter results', async () => {
      const collection = client.db(dbName).collection('concat_filter');
      await collection.insertOne({
        _id: 1,
        positives: [1, -2, 3, -4],
        morePositives: [-5, 6, -7, 8],
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              allPositive: {
                $concatArrays: [
                  { $filter: { input: '$positives', cond: { $gt: ['$$this', 0] } } },
                  { $filter: { input: '$morePositives', cond: { $gt: ['$$this', 0] } } },
                ],
              },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(result[0].allPositive, [1, 3, 6, 8]);
    });

    it('should support $setUnion with $map results', async () => {
      const collection = client.db(dbName).collection('setunion_map');
      await collection.insertOne({
        _id: 1,
        a: [{ id: 1 }, { id: 2 }, { id: 3 }],
        b: [{ id: 2 }, { id: 3 }, { id: 4 }],
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              allIds: {
                $setUnion: [
                  { $map: { input: '$a', in: '$$this.id' } },
                  { $map: { input: '$b', in: '$$this.id' } },
                ],
              },
            },
          },
        ])
        .toArray();

      const ids = (result[0].allIds as number[]).sort();
      assert.deepStrictEqual(ids, [1, 2, 3, 4]);
    });
  });

  describe('Conditional expression nesting', () => {
    it('should support nested $cond', async () => {
      const collection = client.db(dbName).collection('nested_cond');
      await collection.insertMany([
        { _id: 1, score: 95 },
        { _id: 2, score: 85 },
        { _id: 3, score: 75 },
        { _id: 4, score: 65 },
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              grade: {
                $cond: [
                  { $gte: ['$score', 90] },
                  'A',
                  {
                    $cond: [
                      { $gte: ['$score', 80] },
                      'B',
                      { $cond: [{ $gte: ['$score', 70] }, 'C', 'D'] },
                    ],
                  },
                ],
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].grade, 'A');
      assert.strictEqual(result[1].grade, 'B');
      assert.strictEqual(result[2].grade, 'C');
      assert.strictEqual(result[3].grade, 'D');
    });

    it('should support $cond with $and/$or conditions', async () => {
      const collection = client.db(dbName).collection('cond_logical');
      await collection.insertMany([
        { _id: 1, age: 25, verified: true },
        { _id: 2, age: 15, verified: true },
        { _id: 3, age: 25, verified: false },
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              status: {
                $cond: [
                  {
                    $and: [{ $gte: ['$age', 18] }, { $eq: ['$verified', true] }],
                  },
                  'eligible',
                  'ineligible',
                ],
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].status, 'eligible');
      assert.strictEqual(result[1].status, 'ineligible');
      assert.strictEqual(result[2].status, 'ineligible');
    });

    it('should support $switch with expression cases', async () => {
      const collection = client.db(dbName).collection('switch_expr');
      await collection.insertMany([
        { _id: 1, value: -5 },
        { _id: 2, value: 0 },
        { _id: 3, value: 50 },
        { _id: 4, value: 150 },
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              category: {
                $switch: {
                  branches: [
                    { case: { $lt: ['$value', 0] }, then: 'negative' },
                    { case: { $eq: ['$value', 0] }, then: 'zero' },
                    {
                      case: {
                        $and: [{ $gt: ['$value', 0] }, { $lt: ['$value', 100] }],
                      },
                      then: 'small',
                    },
                  ],
                  default: 'large',
                },
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].category, 'negative');
      assert.strictEqual(result[1].category, 'zero');
      assert.strictEqual(result[2].category, 'small');
      assert.strictEqual(result[3].category, 'large');
    });

    it('should support $ifNull with nested expressions', async () => {
      const collection = client.db(dbName).collection('ifnull_nested');
      await collection.insertMany([
        { _id: 1, items: [{ name: 'first' }] },
        { _id: 2, items: [] },
        { _id: 3 }, // missing items field
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              firstItem: {
                $ifNull: [{ $arrayElemAt: ['$items', 0] }, { $literal: { name: 'default' } }],
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual((result[0].firstItem as { name: string }).name, 'first');
      assert.strictEqual((result[1].firstItem as { name: string }).name, 'default');
      assert.strictEqual((result[2].firstItem as { name: string }).name, 'default');
    });

    it('should support $cond in $project with multiple computed fields', async () => {
      const collection = client.db(dbName).collection('cond_multi');
      await collection.insertOne({ _id: 1, qty: 5, price: 10, discount: 0.1 });

      const result = await collection
        .aggregate([
          {
            $project: {
              status: { $cond: [{ $gt: ['$qty', 0] }, 'in-stock', 'out-of-stock'] },
              urgency: { $cond: [{ $lt: ['$qty', 10] }, 'low', 'ok'] },
              discounted: {
                $cond: [
                  { $gt: ['$discount', 0] },
                  { $multiply: ['$price', { $subtract: [1, '$discount'] }] },
                  '$price',
                ],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].status, 'in-stock');
      assert.strictEqual(result[0].urgency, 'low');
      assert.strictEqual(result[0].discounted, 9); // 10 * (1 - 0.1)
    });

    it('should support boolean expression composition', async () => {
      const collection = client.db(dbName).collection('bool_compose');
      await collection.insertMany([
        { _id: 1, a: 1, b: 2, c: 3 },
        { _id: 2, a: 1, b: 5, c: 3 },
        { _id: 3, a: 5, b: 2, c: 3 },
        { _id: 4, a: 5, b: 5, c: 5 },
      ]);

      // (a=1 OR b=2) AND NOT c=3
      const result = await collection
        .aggregate([
          {
            $project: {
              matches: {
                $and: [
                  { $or: [{ $eq: ['$a', 1] }, { $eq: ['$b', 2] }] },
                  { $not: { $eq: ['$c', 3] } },
                ],
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].matches, false); // a=1 but c=3
      assert.strictEqual(result[1].matches, false); // a=1 but c=3
      assert.strictEqual(result[2].matches, false); // b=2 but c=3
      assert.strictEqual(result[3].matches, false); // neither a=1 nor b=2
    });
  });

  describe('Arithmetic expression nesting', () => {
    it('should support nested arithmetic operations', async () => {
      const collection = client.db(dbName).collection('nested_arith');
      await collection.insertOne({
        _id: 1,
        price: 100,
        qty: 5,
        taxRate: 0.1,
      });

      // (price * qty) / (1 + taxRate)
      const result = await collection
        .aggregate([
          {
            $project: {
              netTotal: {
                $divide: [{ $multiply: ['$price', '$qty'] }, { $add: [1, '$taxRate'] }],
              },
            },
          },
        ])
        .toArray();

      // (100 * 5) / (1 + 0.1) = 500 / 1.1 ≈ 454.545...
      assert.ok(Math.abs((result[0].netTotal as number) - 454.545) < 0.01);
    });

    it('should support $round with computed value', async () => {
      const collection = client.db(dbName).collection('round_computed');
      await collection.insertOne({
        _id: 1,
        total: 123,
        count: 7,
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              average: {
                $round: [{ $divide: ['$total', '$count'] }, 2],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].average, 17.57);
    });

    it('should support $abs with subtraction', async () => {
      const collection = client.db(dbName).collection('abs_subtract');
      await collection.insertMany([
        { _id: 1, expected: 100, actual: 85 },
        { _id: 2, expected: 50, actual: 75 },
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              deviation: { $abs: { $subtract: ['$expected', '$actual'] } },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].deviation, 15);
      assert.strictEqual(result[1].deviation, 25);
    });

    it('should support complex formula', async () => {
      const collection = client.db(dbName).collection('complex_formula');
      await collection.insertOne({
        _id: 1,
        price: 100,
        qty: 3,
        discount: 0.2,
      });

      // price * qty - (price * qty * discount)
      const result = await collection
        .aggregate([
          {
            $project: {
              finalPrice: {
                $subtract: [
                  { $multiply: ['$price', '$qty'] },
                  {
                    $multiply: [{ $multiply: ['$price', '$qty'] }, '$discount'],
                  },
                ],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].finalPrice, 240); // 300 - 60
    });

    it('should support $mod with computed divisor', async () => {
      const collection = client.db(dbName).collection('mod_computed');
      await collection.insertOne({
        _id: 1,
        value: 17,
        base: 4,
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              remainder: { $mod: ['$value', { $add: ['$base', 1] }] },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].remainder, 2); // 17 % 5 = 2
    });
  });

  describe('String expression nesting', () => {
    it('should support $concat with $toUpper/$toLower', async () => {
      const collection = client.db(dbName).collection('concat_case');
      await collection.insertOne({
        _id: 1,
        firstName: 'john',
        lastName: 'DOE',
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              fullName: {
                $concat: [{ $toUpper: '$firstName' }, ' ', { $toLower: '$lastName' }],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].fullName, 'JOHN doe');
    });

    it('should support $substrCP with computed indices', async () => {
      const collection = client.db(dbName).collection('substr_computed');
      await collection.insertOne({
        _id: 1,
        email: 'user@example.com',
      });

      // Get domain part (everything after @)
      const result = await collection
        .aggregate([
          {
            $project: {
              domain: {
                $substrCP: ['$email', { $add: [{ $indexOfCP: ['$email', '@'] }, 1] }, 100],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].domain, 'example.com');
    });

    it('should support $trim with $concat result', async () => {
      const collection = client.db(dbName).collection('trim_concat');
      await collection.insertOne({
        _id: 1,
        prefix: '  hello',
        suffix: 'world  ',
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              trimmed: {
                $trim: {
                  input: { $concat: ['$prefix', ' ', '$suffix'] },
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].trimmed, 'hello world');
    });

    it('should support $split then $arrayElemAt', async () => {
      const collection = client.db(dbName).collection('split_elem');
      await collection.insertOne({
        _id: 1,
        email: 'user@example.com',
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              domain: {
                $arrayElemAt: [{ $split: ['$email', '@'] }, 1],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].domain, 'example.com');
    });

    it('should support $replaceAll with $toLower', async () => {
      const collection = client.db(dbName).collection('replace_lower');
      await collection.insertOne({
        _id: 1,
        title: 'Hello World Test',
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              slug: {
                $replaceAll: {
                  input: { $toLower: '$title' },
                  find: ' ',
                  replacement: '-',
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].slug, 'hello-world-test');
    });
  });

  describe('Date expression nesting', () => {
    it('should support $dateAdd with computed amount', async () => {
      const collection = client.db(dbName).collection('dateadd_computed');
      await collection.insertOne({
        _id: 1,
        startDate: new Date('2024-01-15'),
        weeks: 2,
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              endDate: {
                $dateAdd: {
                  startDate: '$startDate',
                  unit: 'day',
                  amount: { $multiply: ['$weeks', 7] },
                },
              },
            },
          },
        ])
        .toArray();

      const endDate = new Date(result[0].endDate as string | number | Date);
      assert.strictEqual(endDate.getUTCDate(), 29); // 15 + 14
    });

    it('should support $dateDiff in comparison', async () => {
      const collection = client.db(dbName).collection('datediff_compare');
      const now = new Date();
      const oldDate = new Date(now.getTime() - 45 * 24 * 60 * 60 * 1000); // 45 days ago
      const recentDate = new Date(now.getTime() - 10 * 24 * 60 * 60 * 1000); // 10 days ago

      await collection.insertMany([
        { _id: 1, created: oldDate },
        { _id: 2, created: recentDate },
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              isOld: {
                $gt: [{ $dateDiff: { startDate: '$created', endDate: '$$NOW', unit: 'day' } }, 30],
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].isOld, true); // 45 days > 30
      assert.strictEqual(result[1].isOld, false); // 10 days <= 30
    });

    it('should support $dateFromParts with field values', async () => {
      const collection = client.db(dbName).collection('dateparts_fields');
      await collection.insertOne({
        _id: 1,
        year: 2024,
        month: 5,
      });

      // First day of next month
      const result = await collection
        .aggregate([
          {
            $project: {
              firstOfNextMonth: {
                $dateFromParts: {
                  year: '$year',
                  month: { $add: ['$month', 1] },
                  day: 1,
                },
              },
            },
          },
        ])
        .toArray();

      const date = new Date(result[0].firstOfNextMonth as string | number | Date);
      assert.strictEqual(date.getUTCMonth(), 5); // June (0-indexed)
      assert.strictEqual(date.getUTCDate(), 1);
    });

    it('should support $cond with date comparison', async () => {
      const collection = client.db(dbName).collection('cond_date');
      const future = new Date(Date.now() + 24 * 60 * 60 * 1000); // tomorrow
      const past = new Date(Date.now() - 24 * 60 * 60 * 1000); // yesterday

      await collection.insertMany([
        { _id: 1, expiry: future },
        { _id: 2, expiry: past },
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              status: {
                $cond: [{ $lt: ['$expiry', '$$NOW'] }, 'expired', 'valid'],
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].status, 'valid');
      assert.strictEqual(result[1].status, 'expired');
    });
  });
});
