/**
 * Edge Cases Tests
 *
 * Tests for edge cases and corner cases that might expose bugs.
 *
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

describe(`Edge Cases (${getTestModeName()})`, () => {
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

  describe('Query edge cases', () => {
    describe('$all with $elemMatch', () => {
      it('should support $all with single $elemMatch', async () => {
        const collection = client.db(dbName).collection('all_elemmatch_single');
        await collection.insertMany([
          {
            _id: 1,
            items: [
              { a: 1, b: 2 },
              { a: 3, b: 4 },
            ],
          },
          {
            _id: 2,
            items: [
              { a: 1, b: 5 },
              { a: 6, b: 2 },
            ],
          },
          { _id: 3, items: [{ a: 7, b: 8 }] },
        ]);

        // Find docs where items has an element with both a=1 AND b=2
        const result = await collection
          .find({ items: { $all: [{ $elemMatch: { a: 1, b: 2 } }] } })
          .toArray();

        assert.strictEqual(result.length, 1);
        assert.strictEqual(result[0]._id, 1);
      });

      it('should support $all with multiple $elemMatch', async () => {
        const collection = client.db(dbName).collection('all_elemmatch_multi');
        await collection.insertMany([
          { _id: 1, items: [{ x: 1 }, { x: 2 }, { x: 3 }] },
          { _id: 2, items: [{ x: 1 }, { x: 2 }] },
          { _id: 3, items: [{ x: 2 }, { x: 3 }] },
        ]);

        // Must have element with x=1 AND element with x=3
        const result = await collection
          .find({
            items: {
              $all: [{ $elemMatch: { x: 1 } }, { $elemMatch: { x: 3 } }],
            },
          })
          .toArray();

        assert.strictEqual(result.length, 1);
        assert.strictEqual(result[0]._id, 1);
      });

      it('should support $all with $elemMatch containing operators', async () => {
        const collection = client.db(dbName).collection('all_elemmatch_ops');
        await collection.insertMany([
          { _id: 1, scores: [{ val: 85 }, { val: 92 }] },
          { _id: 2, scores: [{ val: 70 }, { val: 75 }] },
          { _id: 3, scores: [{ val: 95 }, { val: 60 }] },
        ]);

        // Must have element with val > 90
        const result = await collection
          .find({ scores: { $all: [{ $elemMatch: { val: { $gt: 90 } } }] } })
          .toArray();

        assert.strictEqual(result.length, 2);
        const ids = result.map((d) => d._id).sort();
        assert.deepStrictEqual(ids, [1, 3]);
      });
    });

    describe('$expr with array operators', () => {
      it('should support $expr with $in (expression form)', async () => {
        const collection = client.db(dbName).collection('expr_in_expr');
        await collection.insertMany([
          { _id: 1, status: 'active', allowed: ['active', 'pending'] },
          { _id: 2, status: 'deleted', allowed: ['active', 'pending'] },
          { _id: 3, status: 'pending', allowed: ['active'] },
        ]);

        // Find where status is in allowed array
        const result = await collection.find({ $expr: { $in: ['$status', '$allowed'] } }).toArray();

        assert.strictEqual(result.length, 1);
        assert.strictEqual(result[0]._id, 1);
      });

      it('should support $expr with $setIntersection', async () => {
        const collection = client.db(dbName).collection('expr_setintersect');
        await collection.insertMany([
          { _id: 1, tags: ['a', 'b', 'c'], required: ['a', 'd'] },
          { _id: 2, tags: ['x', 'y'], required: ['a', 'b'] },
          { _id: 3, tags: ['a', 'b'], required: ['a', 'b'] },
        ]);

        // Find where tags contains all required
        const result = await collection
          .find({
            $expr: {
              $eq: [
                { $size: { $setIntersection: ['$tags', '$required'] } },
                { $size: '$required' },
              ],
            },
          })
          .toArray();

        assert.strictEqual(result.length, 1);
        assert.strictEqual(result[0]._id, 3);
      });
    });

    describe('$regex edge cases', () => {
      it('should support $not with $regex', async () => {
        const collection = client.db(dbName).collection('not_regex');
        await collection.insertMany([
          { _id: 1, name: 'test-file' },
          { _id: 2, name: 'production' },
          { _id: 3, name: 'test-data' },
        ]);

        const result = await collection.find({ name: { $not: /^test/ } }).toArray();

        assert.strictEqual(result.length, 1);
        assert.strictEqual(result[0]._id, 2);
      });

      it('should support $regex inside $elemMatch', async () => {
        const collection = client.db(dbName).collection('elemmatch_regex');
        await collection.insertMany([
          { _id: 1, tags: [{ name: 'JavaScript' }, { name: 'Python' }] },
          { _id: 2, tags: [{ name: 'Ruby' }, { name: 'Go' }] },
        ]);

        const result = await collection
          .find({ tags: { $elemMatch: { name: { $regex: /^java/i } } } })
          .toArray();

        assert.strictEqual(result.length, 1);
        assert.strictEqual(result[0]._id, 1);
      });
    });
  });

  describe('Update edge cases', () => {
    describe('$push with all modifiers', () => {
      it('should support $push with $each, $slice, $sort combined', async () => {
        const collection = client.db(dbName).collection('push_all_mods');
        await collection.insertOne({
          _id: 1,
          scores: [{ val: 80 }, { val: 90 }],
        });

        await collection.updateOne(
          { _id: 1 },
          {
            $push: {
              scores: {
                $each: [{ val: 85 }, { val: 95 }, { val: 70 }],
                $sort: { val: -1 },
                $slice: 3,
              },
            },
          }
        );

        const doc = await collection.findOne({ _id: 1 });
        assert.ok(doc);
        // Should have top 3 scores: 95, 90, 85
        assert.deepStrictEqual(doc.scores, [{ val: 95 }, { val: 90 }, { val: 85 }]);
      });

      it('should support $push with $each, $position, $slice', async () => {
        const collection = client.db(dbName).collection('push_pos_slice');
        await collection.insertOne({
          _id: 1,
          items: ['c', 'd', 'e'],
        });

        await collection.updateOne(
          { _id: 1 },
          {
            $push: {
              items: {
                $each: ['a', 'b'],
                $position: 0,
                $slice: 4,
              },
            },
          }
        );

        const doc = await collection.findOne({ _id: 1 });
        assert.ok(doc);
        assert.deepStrictEqual(doc.items, ['a', 'b', 'c', 'd']);
      });
    });

    describe('$pull with complex conditions', () => {
      it('should support $pull with multiple field conditions', async () => {
        const collection = client.db(dbName).collection('pull_multi_cond');
        await collection.insertOne({
          _id: 1,
          items: [
            { name: 'a', qty: 10, status: 'active' },
            { name: 'b', qty: 5, status: 'deleted' },
            { name: 'c', qty: 3, status: 'active' },
            { name: 'd', qty: 8, status: 'deleted' },
          ],
        });

        // Pull items that are deleted OR have qty < 5
        await collection.updateOne(
          { _id: 1 },
          {
            $pull: {
              items: { $or: [{ status: 'deleted' }, { qty: { $lt: 5 } }] },
            },
          }
        );

        const doc = await collection.findOne({ _id: 1 });
        assert.ok(doc);
        const items = doc.items as Array<{ name: string }>;
        assert.strictEqual(items.length, 1);
        assert.strictEqual(items[0].name, 'a');
      });

      it('should support $pull with $elemMatch-style condition', async () => {
        const collection = client.db(dbName).collection('pull_elemmatch_style');
        await collection.insertOne({
          _id: 1,
          orders: [
            { product: 'A', qty: 10, price: 100 },
            { product: 'B', qty: 5, price: 200 },
            { product: 'C', qty: 20, price: 50 },
          ],
        });

        // Pull orders where qty > 5 AND price < 100
        await collection.updateOne(
          { _id: 1 },
          { $pull: { orders: { qty: { $gt: 5 }, price: { $lt: 100 } } } }
        );

        const doc = await collection.findOne({ _id: 1 });
        assert.ok(doc);
        const orders = doc.orders as Array<{ product: string }>;
        assert.strictEqual(orders.length, 2);
        const products = orders.map((o) => o.product);
        assert.deepStrictEqual(products.sort(), ['A', 'B']);
      });
    });

    describe('Positional operators with nested arrays', () => {
      it('should support $[] with nested array path', async () => {
        const collection = client.db(dbName).collection('pos_nested_all');
        await collection.insertOne({
          _id: 1,
          groups: [
            { name: 'G1', members: [{ score: 10 }, { score: 20 }] },
            { name: 'G2', members: [{ score: 30 }, { score: 40 }] },
          ],
        });

        // Double all scores in all groups
        await collection.updateOne({ _id: 1 }, { $mul: { 'groups.$[].members.$[].score': 2 } });

        const doc = await collection.findOne({ _id: 1 });
        assert.ok(doc);
        const groups = doc.groups as Array<{ members: Array<{ score: number }> }>;
        assert.strictEqual(groups[0].members[0].score, 20);
        assert.strictEqual(groups[0].members[1].score, 40);
        assert.strictEqual(groups[1].members[0].score, 60);
        assert.strictEqual(groups[1].members[1].score, 80);
      });

      it('should support multiple arrayFilters on nested arrays', async () => {
        const collection = client.db(dbName).collection('pos_nested_filter');
        await collection.insertOne({
          _id: 1,
          departments: [
            {
              name: 'Engineering',
              employees: [
                { name: 'Alice', level: 3 },
                { name: 'Bob', level: 5 },
              ],
            },
            {
              name: 'Sales',
              employees: [
                { name: 'Carol', level: 4 },
                { name: 'Dave', level: 2 },
              ],
            },
          ],
        });

        // Promote employees with level >= 4 in Engineering
        await collection.updateOne(
          { _id: 1 },
          { $inc: { 'departments.$[dept].employees.$[emp].level': 1 } },
          {
            arrayFilters: [{ 'dept.name': 'Engineering' }, { 'emp.level': { $gte: 4 } }],
          }
        );

        const doc = await collection.findOne({ _id: 1 });
        assert.ok(doc);
        const depts = doc.departments as Array<{ employees: Array<{ level: number }> }>;
        // Only Bob (level 5 in Engineering) should be promoted
        assert.strictEqual(depts[0].employees[0].level, 3); // Alice unchanged
        assert.strictEqual(depts[0].employees[1].level, 6); // Bob promoted
        assert.strictEqual(depts[1].employees[0].level, 4); // Carol unchanged (Sales)
      });
    });
  });

  describe('Variable scoping', () => {
    describe('$$ROOT usage', () => {
      it('should access $$ROOT in nested $map', async () => {
        const collection = client.db(dbName).collection('root_nested_map');
        await collection.insertOne({
          _id: 1,
          name: 'Doc1',
          values: [1, 2, 3],
        });

        const result = await collection
          .aggregate([
            {
              $project: {
                mapped: {
                  $map: {
                    input: '$values',
                    as: 'v',
                    in: {
                      value: '$$v',
                      docName: '$$ROOT.name',
                    },
                  },
                },
              },
            },
          ])
          .toArray();

        const mapped = result[0].mapped as Array<{ value: number; docName: string }>;
        assert.strictEqual(mapped.length, 3);
        assert.strictEqual(mapped[0].value, 1);
        assert.strictEqual(mapped[0].docName, 'Doc1');
      });

      it('should preserve $$ROOT through $unwind', async () => {
        const collection = client.db(dbName).collection('root_unwind');
        await collection.insertOne({
          _id: 1,
          name: 'Parent',
          items: ['a', 'b'],
        });

        const result = await collection
          .aggregate([
            { $unwind: '$items' },
            {
              $project: {
                item: '$items',
                originalName: '$$ROOT.name',
              },
            },
          ])
          .toArray();

        assert.strictEqual(result.length, 2);
        // $$ROOT should still reference the pre-unwind document
        assert.strictEqual(result[0].originalName, 'Parent');
        assert.strictEqual(result[1].originalName, 'Parent');
      });
    });

    describe('$let expressions', () => {
      it('should support $let with computed variables', async () => {
        const collection = client.db(dbName).collection('let_computed');
        await collection.insertOne({
          _id: 1,
          price: 100,
          qty: 5,
          taxRate: 0.1,
        });

        const result = await collection
          .aggregate([
            {
              $project: {
                total: {
                  $let: {
                    vars: {
                      subtotal: { $multiply: ['$price', '$qty'] },
                      taxMultiplier: { $add: [1, '$taxRate'] },
                    },
                    in: { $multiply: ['$$subtotal', '$$taxMultiplier'] },
                  },
                },
              },
            },
          ])
          .toArray();

        assert.strictEqual(result[0].total, 550); // 100 * 5 * 1.1
      });

      it('should support nested $let with variable shadowing', async () => {
        const collection = client.db(dbName).collection('let_shadow');
        await collection.insertOne({ _id: 1, x: 10 });

        const result = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $let: {
                    vars: { val: 5 },
                    in: {
                      outer: '$$val',
                      inner: {
                        $let: {
                          vars: { val: 20 }, // shadows outer val
                          in: '$$val',
                        },
                      },
                    },
                  },
                },
              },
            },
          ])
          .toArray();

        const res = result[0].result as { outer: number; inner: number };
        assert.strictEqual(res.outer, 5);
        assert.strictEqual(res.inner, 20);
      });
    });

    describe('Variable shadowing in $map/$filter', () => {
      it('should handle same variable name in nested $map', async () => {
        const collection = client.db(dbName).collection('map_shadow');
        await collection.insertOne({
          _id: 1,
          matrix: [
            [1, 2],
            [3, 4],
          ],
        });

        const result = await collection
          .aggregate([
            {
              $project: {
                doubled: {
                  $map: {
                    input: '$matrix',
                    as: 'item',
                    in: {
                      $map: {
                        input: '$$item',
                        as: 'item', // Same variable name - shadows outer
                        in: { $multiply: ['$$item', 2] },
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
    });
  });

  describe('Numeric precision', () => {
    it('should handle large integers near MAX_SAFE_INTEGER', async () => {
      const collection = client.db(dbName).collection('large_int');
      const largeNum = Number.MAX_SAFE_INTEGER - 10;
      await collection.insertOne({ _id: 1, value: largeNum });

      const result = await collection
        .aggregate([{ $project: { incremented: { $add: ['$value', 5] } } }])
        .toArray();

      assert.strictEqual(result[0].incremented, largeNum + 5);
    });

    it('should handle division resulting in repeating decimals', async () => {
      const collection = client.db(dbName).collection('div_repeat');
      await collection.insertOne({ _id: 1, num: 10, denom: 3 });

      const result = await collection
        .aggregate([
          {
            $project: {
              divided: { $divide: ['$num', '$denom'] },
              rounded: { $round: [{ $divide: ['$num', '$denom'] }, 4] },
            },
          },
        ])
        .toArray();

      // 10/3 = 3.333...
      assert.ok(Math.abs((result[0].divided as number) - 3.3333) < 0.001);
      assert.strictEqual(result[0].rounded, 3.3333);
    });

    it("should handle $round with 0.5 (banker's rounding check)", async () => {
      const collection = client.db(dbName).collection('round_half');
      await collection.insertMany([
        { _id: 1, val: 2.5 },
        { _id: 2, val: 3.5 },
        { _id: 3, val: -2.5 },
      ]);

      const result = await collection
        .aggregate([{ $project: { rounded: { $round: ['$val', 0] } } }, { $sort: { _id: 1 } }])
        .toArray();

      // MongoDB uses "round half to even" (banker's rounding)
      assert.strictEqual(result[0].rounded, 2); // 2.5 -> 2
      assert.strictEqual(result[1].rounded, 4); // 3.5 -> 4
      assert.strictEqual(result[2].rounded, -2); // -2.5 -> -2
    });

    it('should handle $mod with negative numbers', async () => {
      const collection = client.db(dbName).collection('mod_negative');
      await collection.insertMany([
        { _id: 1, a: -10, b: 3 },
        { _id: 2, a: 10, b: -3 },
        { _id: 3, a: -10, b: -3 },
      ]);

      const result = await collection
        .aggregate([{ $project: { mod: { $mod: ['$a', '$b'] } } }, { $sort: { _id: 1 } }])
        .toArray();

      // Remainder takes sign of dividend
      assert.strictEqual(result[0].mod, -1); // -10 % 3 = -1
      assert.strictEqual(result[1].mod, 1); // 10 % -3 = 1
      assert.strictEqual(result[2].mod, -1); // -10 % -3 = -1
    });
  });

  describe('Empty/null propagation', () => {
    it('should handle $in with empty array', async () => {
      const collection = client.db(dbName).collection('in_empty');
      await collection.insertMany([
        { _id: 1, val: 'a' },
        { _id: 2, val: 'b' },
      ]);

      const result = await collection.find({ val: { $in: [] } }).toArray();
      assert.strictEqual(result.length, 0);
    });

    it('should handle $all with empty array', async () => {
      const collection = client.db(dbName).collection('all_empty');
      await collection.insertMany([
        { _id: 1, tags: ['a', 'b'] },
        { _id: 2, tags: [] },
      ]);

      // $all: [] matches nothing (MongoDB behavior)
      const result = await collection.find({ tags: { $all: [] } }).toArray();
      assert.strictEqual(result.length, 0);
    });

    it('should handle $reduce with empty array', async () => {
      const collection = client.db(dbName).collection('reduce_empty');
      await collection.insertOne({ _id: 1, values: [] });

      const result = await collection
        .aggregate([
          {
            $project: {
              sum: {
                $reduce: {
                  input: '$values',
                  initialValue: 100,
                  in: { $add: ['$$value', '$$this'] },
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].sum, 100); // Returns initialValue
    });

    it('should handle null in $concatArrays', async () => {
      const collection = client.db(dbName).collection('concat_null');
      await collection.insertOne({ _id: 1, a: [1, 2], b: null });

      const result = await collection
        .aggregate([{ $project: { combined: { $concatArrays: ['$a', '$b'] } } }])
        .toArray();

      assert.strictEqual(result[0].combined, null);
    });

    it('should handle missing field in $ifNull chain', async () => {
      const collection = client.db(dbName).collection('ifnull_chain');
      await collection.insertOne({ _id: 1 }); // No fields

      const result = await collection
        .aggregate([
          {
            $project: {
              value: {
                $ifNull: ['$missing1', { $ifNull: ['$missing2', 'default'] }],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].value, 'default');
    });

    it('should handle empty object in $mergeObjects', async () => {
      const collection = client.db(dbName).collection('merge_empty');
      await collection.insertMany([
        { _id: 1, category: 'A', data: { x: 1 } },
        { _id: 2, category: 'A', data: {} },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: '$category',
              merged: { $mergeObjects: '$data' },
            },
          },
        ])
        .toArray();

      // Empty object should not overwrite existing keys
      assert.strictEqual((result[0].merged as { x: number }).x, 1);
    });
  });

  describe('Complex $lookup/$graphLookup', () => {
    it('should support $lookup with $expr in pipeline', async () => {
      const orders = client.db(dbName).collection('lookup_orders');
      const products = client.db(dbName).collection('lookup_products');

      await orders.insertMany([
        { _id: 1, productId: 'P1', qty: 5 },
        { _id: 2, productId: 'P2', qty: 3 },
      ]);
      await products.insertMany([
        { _id: 'P1', name: 'Widget', minQty: 3 },
        { _id: 'P2', name: 'Gadget', minQty: 5 },
      ]);

      const result = await orders
        .aggregate([
          {
            $lookup: {
              from: 'lookup_products',
              let: { pid: '$productId', orderQty: '$qty' },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [{ $eq: ['$_id', '$$pid'] }, { $gte: ['$$orderQty', '$minQty'] }],
                    },
                  },
                },
              ],
              as: 'validProduct',
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      // Order 1: qty 5 >= minQty 3, should match
      const vp0 = result[0].validProduct as Array<{ name: string }>;
      assert.strictEqual(vp0.length, 1);
      assert.strictEqual(vp0[0].name, 'Widget');

      // Order 2: qty 3 < minQty 5, should not match
      assert.strictEqual((result[1].validProduct as unknown[]).length, 0);
    });

    it('should support $graphLookup with restrictSearchWithMatch', async () => {
      const collection = client.db(dbName).collection('graph_restrict');
      await collection.insertMany([
        { _id: 1, name: 'A', parent: null, active: true },
        { _id: 2, name: 'B', parent: 'A', active: true },
        { _id: 3, name: 'C', parent: 'B', active: false },
        { _id: 4, name: 'D', parent: 'B', active: true },
        { _id: 5, name: 'E', parent: 'C', active: true },
      ]);

      const result = await collection
        .aggregate([
          { $match: { _id: 1 } },
          {
            $graphLookup: {
              from: 'graph_restrict',
              startWith: '$name',
              connectFromField: 'name',
              connectToField: 'parent',
              as: 'activeDescendants',
              restrictSearchWithMatch: { active: true },
            },
          },
        ])
        .toArray();

      // Should find B and D (active), but not C (inactive) or E (child of inactive)
      const descendants = result[0].activeDescendants as Array<{ name: string }>;
      const names = descendants.map((d) => d.name).sort();
      assert.deepStrictEqual(names, ['B', 'D']);
    });

    it('should support $lookup with complex pipeline and $group', async () => {
      const orders = client.db(dbName).collection('lookup_orders2');
      const items = client.db(dbName).collection('lookup_items2');

      await orders.insertOne({ _id: 1, orderId: 'O1' });
      await items.insertMany([
        { orderId: 'O1', product: 'A', qty: 2, price: 10 },
        { orderId: 'O1', product: 'B', qty: 3, price: 20 },
        { orderId: 'O2', product: 'C', qty: 1, price: 30 },
      ]);

      const result = await orders
        .aggregate([
          {
            $lookup: {
              from: 'lookup_items2',
              let: { oid: '$orderId' },
              pipeline: [
                { $match: { $expr: { $eq: ['$orderId', '$$oid'] } } },
                {
                  $group: {
                    _id: null,
                    total: { $sum: { $multiply: ['$qty', '$price'] } },
                    itemCount: { $sum: 1 },
                  },
                },
              ],
              as: 'summary',
            },
          },
        ])
        .toArray();

      const summary = result[0].summary as Array<{ total: number; itemCount: number }>;
      assert.strictEqual(summary.length, 1);
      assert.strictEqual(summary[0].total, 80); // 2*10 + 3*20
      assert.strictEqual(summary[0].itemCount, 2);
    });
  });
});
