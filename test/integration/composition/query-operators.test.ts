/**
 * Query Operator Composition Tests
 *
 * Tests for nested and composed query operators to verify they work
 * correctly when combined with each other.
 *
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "../../test-harness.ts";

describe(`Query Operator Composition (${getTestModeName()})`, () => {
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

  describe("$not composition", () => {
    it("should support $not wrapping $elemMatch", async () => {
      const collection = client.db(dbName).collection("not_elemmatch");
      await collection.insertMany([
        { _id: 1, items: [{ qty: 5 }, { qty: 15 }] },
        { _id: 2, items: [{ qty: 20 }, { qty: 25 }] },
        { _id: 3, items: [{ qty: 3 }, { qty: 8 }] },
      ]);

      // Find docs where NO item has qty > 10
      const result = await collection
        .find({ items: { $not: { $elemMatch: { qty: { $gt: 10 } } } } })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 3);
    });

    it("should support $not with $regex", async () => {
      const collection = client.db(dbName).collection("not_regex");
      await collection.insertMany([
        { _id: 1, name: "test-file.txt" },
        { _id: 2, name: "production.log" },
        { _id: 3, name: "test-data.json" },
      ]);

      // Find docs where name does NOT start with "test"
      const result = await collection
        .find({ name: { $not: /^test/ } })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 2);
    });

    it("should support $not with $in", async () => {
      const collection = client.db(dbName).collection("not_in");
      await collection.insertMany([
        { _id: 1, status: "active" },
        { _id: 2, status: "deleted" },
        { _id: 3, status: "archived" },
        { _id: 4, status: "pending" },
      ]);

      // Find docs where status is NOT in the list
      const result = await collection
        .find({ status: { $not: { $in: ["deleted", "archived"] } } })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 4]);
    });

    it("should support $not with $size", async () => {
      const collection = client.db(dbName).collection("not_size");
      await collection.insertMany([
        { _id: 1, tags: [] },
        { _id: 2, tags: ["a"] },
        { _id: 3, tags: ["a", "b"] },
      ]);

      // Find docs where tags array is NOT empty
      const result = await collection
        .find({ tags: { $not: { $size: 0 } } })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [2, 3]);
    });

    it("should support $not with $gt and $lt combined", async () => {
      const collection = client.db(dbName).collection("not_range");
      await collection.insertMany([
        { _id: 1, value: 5 },
        { _id: 2, value: 15 },
        { _id: 3, value: 25 },
      ]);

      // Find docs where value is NOT in range [10, 20]
      const result = await collection
        .find({ value: { $not: { $gte: 10, $lte: 20 } } })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 3]);
    });

    it("should support $not with $type", async () => {
      const collection = client.db(dbName).collection("not_type");
      await collection.insertMany([
        { _id: 1, value: "hello" },
        { _id: 2, value: 42 },
        { _id: 3, value: true },
      ]);

      // Find docs where value is NOT a string
      const result = await collection
        .find({ value: { $not: { $type: "string" } } })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [2, 3]);
    });
  });

  describe("$elemMatch deep nesting", () => {
    it("should support $elemMatch with $and", async () => {
      const collection = client.db(dbName).collection("elemmatch_and");
      await collection.insertMany([
        {
          _id: 1,
          items: [
            { qty: 5, price: 50 },   // qty NOT > 5
            { qty: 15, price: 150 }, // price NOT < 100
          ],
        },
        {
          _id: 2,
          items: [
            { qty: 10, price: 200 }, // price NOT < 100
            { qty: 3, price: 50 },   // qty NOT > 5
          ],
        },
        {
          _id: 3,
          items: [
            { qty: 10, price: 80 },  // qty > 5 AND price < 100 - MATCHES
            { qty: 5, price: 30 },
          ],
        },
      ]);

      // Find docs with an item having qty > 5 AND price < 100
      const result = await collection
        .find({
          items: {
            $elemMatch: { $and: [{ qty: { $gt: 5 } }, { price: { $lt: 100 } }] },
          },
        })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 3);
    });

    it("should support $elemMatch with $or", async () => {
      const collection = client.db(dbName).collection("elemmatch_or");
      await collection.insertMany([
        {
          _id: 1,
          items: [
            { status: "pending", priority: "low" },
            { status: "pending", priority: "low" },
          ],
        },
        {
          _id: 2,
          items: [
            { status: "active", priority: "low" },
            { status: "pending", priority: "high" },
          ],
        },
        {
          _id: 3,
          items: [
            { status: "complete", priority: "medium" },
            { status: "complete", priority: "medium" },
          ],
        },
      ]);

      // Find docs with an item that is active OR high priority
      const result = await collection
        .find({
          items: {
            $elemMatch: {
              $or: [{ status: "active" }, { priority: "high" }],
            },
          },
        })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 2);
    });

    it("should support $elemMatch with nested object path", async () => {
      const collection = client.db(dbName).collection("elemmatch_nested_path");
      await collection.insertMany([
        {
          _id: 1,
          orders: [
            { product: { category: "electronics", name: "phone" } },
            { product: { category: "clothing", name: "shirt" } },
          ],
        },
        {
          _id: 2,
          orders: [
            { product: { category: "food", name: "apple" } },
            { product: { category: "food", name: "bread" } },
          ],
        },
      ]);

      // Find docs with an order in electronics category
      const result = await collection
        .find({
          orders: { $elemMatch: { "product.category": "electronics" } },
        })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 1);
    });

    it("should support $elemMatch with $not inside", async () => {
      const collection = client.db(dbName).collection("elemmatch_not_inside");
      await collection.insertMany([
        {
          _id: 1,
          items: [
            { qty: 10, status: "cancelled" },
            { qty: 5, status: "active" },
          ],
        },
        {
          _id: 2,
          items: [
            { qty: 15, status: "active" },
            { qty: 20, status: "active" },
          ],
        },
        {
          _id: 3,
          items: [
            { qty: 0, status: "active" },
            { qty: 0, status: "pending" },
          ],
        },
      ]);

      // Find docs with an item having qty > 0 and status NOT cancelled
      const result = await collection
        .find({
          items: {
            $elemMatch: { qty: { $gt: 0 }, status: { $not: { $eq: "cancelled" } } },
          },
        })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 2]);
    });

    it("should support nested $elemMatch for array of arrays", async () => {
      const collection = client.db(dbName).collection("elemmatch_nested");
      await collection.insertMany([
        { _id: 1, matrix: [[1, 2, 3], [4, 5, 6]] },
        { _id: 2, matrix: [[7, 8, 9], [10, 11, 12]] },
        { _id: 3, matrix: [[1, 1, 1], [2, 2, 2]] },
      ]);

      // Find docs where some inner array has an element > 10
      const result = await collection
        .find({ matrix: { $elemMatch: { $elemMatch: { $gt: 10 } } } })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 2);
    });
  });

  describe("Deep logical nesting", () => {
    it("should support 3-level $and/$or nesting", async () => {
      const collection = client.db(dbName).collection("deep_and_or");
      await collection.insertMany([
        { _id: 1, a: 1, b: 2, c: 3, d: true },
        { _id: 2, a: 1, b: 5, c: 5, d: true },
        { _id: 3, a: 2, b: 2, c: 3, d: false },
        { _id: 4, a: 5, b: 5, c: 5, d: true },
      ]);

      // Complex: (a=1 OR (b=2 AND c=3)) AND d exists
      const result = await collection
        .find({
          $and: [
            { $or: [{ a: 1 }, { $and: [{ b: 2 }, { c: 3 }] }] },
            { d: { $exists: true } },
          ],
        })
        .toArray();

      assert.strictEqual(result.length, 3);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 2, 3]);
    });

    it("should support $nor with nested $and", async () => {
      const collection = client.db(dbName).collection("nor_and");
      await collection.insertMany([
        { _id: 1, a: 1, b: 2, c: 3 },
        { _id: 2, a: 1, b: 5, c: 5 },
        { _id: 3, a: 5, b: 5, c: 3 },
        { _id: 4, a: 5, b: 5, c: 5 },
      ]);

      // NOT ((a=1 AND b=2) OR c=3)
      const result = await collection
        .find({
          $nor: [{ $and: [{ a: 1 }, { b: 2 }] }, { c: 3 }],
        })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [2, 4]);
    });

    it("should support complex mixed nesting with $or, $and, $nor", async () => {
      const collection = client.db(dbName).collection("mixed_nesting");
      await collection.insertMany([
        { _id: 1, x: 5, y: 5, z: 1 },   // (5>0 AND 5<10) = TRUE, matches
        { _id: 2, x: -1, y: 15, z: 5 }, // x<=0, but z=5 NOT IN [1,2,3] = TRUE, matches
        { _id: 3, x: 5, y: 5, z: 2 },   // (5>0 AND 5<10) = TRUE, matches
        { _id: 4, x: 0, y: 0, z: 1 },   // x<=0, z=1 IS IN [1,2,3], NO MATCH
      ]);

      // (x>0 AND y<10) OR (z NOT IN [1,2,3])
      const result = await collection
        .find({
          $or: [
            { $and: [{ x: { $gt: 0 } }, { y: { $lt: 10 } }] },
            { $nor: [{ z: { $in: [1, 2, 3] } }] },
          ],
        })
        .toArray();

      assert.strictEqual(result.length, 3);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 2, 3]);
    });

    it("should support implicit $and with nested logical operators", async () => {
      const collection = client.db(dbName).collection("implicit_and_nested");
      await collection.insertMany([
        { _id: 1, status: "active", a: 1, b: 2, c: 3 },
        { _id: 2, status: "active", a: 5, b: 5, c: 3 },
        { _id: 3, status: "inactive", a: 1, b: 2, c: 3 },
        { _id: 4, status: "active", a: 5, b: 5, c: 5 },
      ]);

      // status=active AND ((a=1 AND b=2) OR c=3)
      const result = await collection
        .find({
          status: "active",
          $or: [{ $and: [{ a: 1 }, { b: 2 }] }, { c: 3 }],
        })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 2]);
    });

    it("should support 4-level deep nesting", async () => {
      const collection = client.db(dbName).collection("four_level");
      await collection.insertMany([
        { _id: 1, a: 1, b: 1, c: 1, d: 1 },
        { _id: 2, a: 2, b: 2, c: 2, d: 2 },
        { _id: 3, a: 1, b: 2, c: 1, d: 2 },
        { _id: 4, a: 2, b: 1, c: 2, d: 1 },
      ]);

      // ((a=1 OR (b=1 AND (c=1 OR d=1))) AND a exists)
      const result = await collection
        .find({
          $and: [
            {
              $or: [
                { a: 1 },
                { $and: [{ b: 1 }, { $or: [{ c: 1 }, { d: 1 }] }] },
              ],
            },
            { a: { $exists: true } },
          ],
        })
        .toArray();

      assert.strictEqual(result.length, 3);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 3, 4]);
    });
  });

  describe("$expr with query operators", () => {
    it("should support $expr with $and combining comparisons", async () => {
      const collection = client.db(dbName).collection("expr_and");
      await collection.insertMany([
        { _id: 1, a: 10, b: 5, c: 50 },
        { _id: 2, a: 3, b: 5, c: 50 },
        { _id: 3, a: 10, b: 5, c: 150 },
      ]);

      // a > b AND c < 100
      const result = await collection
        .find({
          $expr: {
            $and: [{ $gt: ["$a", "$b"] }, { $lt: ["$c", 100] }],
          },
        })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 1);
    });

    it("should support $expr with $or and field references", async () => {
      const collection = client.db(dbName).collection("expr_or");
      await collection.insertMany([
        { _id: 1, status: "active", priority: 3 },
        { _id: 2, status: "pending", priority: 7 },
        { _id: 3, status: "pending", priority: 2 },
      ]);

      // status = "active" OR priority >= 5
      const result = await collection
        .find({
          $expr: {
            $or: [
              { $eq: ["$status", "active"] },
              { $gte: ["$priority", 5] },
            ],
          },
        })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 2]);
    });

    it("should support $expr with $cond returning boolean", async () => {
      const collection = client.db(dbName).collection("expr_cond");
      await collection.insertMany([
        { _id: 1, age: 20 },
        { _id: 2, age: 15 },
        { _id: 3, age: 25 },
      ]);

      // Filter where age >= 18
      const result = await collection
        .find({
          $expr: { $cond: [{ $gte: ["$age", 18] }, true, false] },
        })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 3]);
    });

    it("should support mixed query + $expr", async () => {
      const collection = client.db(dbName).collection("mixed_expr");
      await collection.insertMany([
        { _id: 1, category: "A", price: 10, qty: 5, budget: 40 },
        { _id: 2, category: "A", price: 10, qty: 5, budget: 60 },
        { _id: 3, category: "B", price: 10, qty: 5, budget: 40 },
      ]);

      // category = "A" AND (price * qty) > budget
      const result = await collection
        .find({
          category: "A",
          $expr: { $gt: [{ $multiply: ["$price", "$qty"] }, "$budget"] },
        })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 1);
    });

    it("should support $expr with nested arithmetic", async () => {
      const collection = client.db(dbName).collection("expr_arithmetic");
      await collection.insertMany([
        { _id: 1, a: 10, b: 5, c: 2 },
        { _id: 2, a: 5, b: 10, c: 2 },
        { _id: 3, a: 20, b: 5, c: 3 },
      ]);

      // (a - b) * c > 10
      const result = await collection
        .find({
          $expr: {
            $gt: [{ $multiply: [{ $subtract: ["$a", "$b"] }, "$c"] }, 10],
          },
        })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 3);
    });
  });

  describe("Edge cases", () => {
    it("should reject empty $and array", async () => {
      const collection = client.db(dbName).collection("empty_and");
      await collection.insertMany([
        { _id: 1, x: 1 },
        { _id: 2, x: 2 },
      ]);

      await assert.rejects(
        collection.find({ $and: [] }).toArray(),
        /\$and.*non-empty/i
      );
    });

    it("should reject empty $or array", async () => {
      const collection = client.db(dbName).collection("empty_or");
      await collection.insertMany([
        { _id: 1, x: 1 },
        { _id: 2, x: 2 },
      ]);

      await assert.rejects(
        collection.find({ $or: [] }).toArray(),
        /\$or.*non-empty/i
      );
    });

    it("should handle single-element $and", async () => {
      const collection = client.db(dbName).collection("single_and");
      await collection.insertMany([
        { _id: 1, a: 1 },
        { _id: 2, a: 2 },
      ]);

      const result = await collection.find({ $and: [{ a: 1 }] }).toArray();
      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 1);
    });

    it("should handle single-element $or", async () => {
      const collection = client.db(dbName).collection("single_or");
      await collection.insertMany([
        { _id: 1, a: 1 },
        { _id: 2, a: 2 },
      ]);

      const result = await collection.find({ $or: [{ a: 1 }] }).toArray();
      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 1);
    });

    it("should handle null in nested operators", async () => {
      const collection = client.db(dbName).collection("null_nested");
      await collection.insertMany([
        { _id: 1, a: null, b: true },
        { _id: 2, a: 1, b: true },
        { _id: 3, a: null },
      ]);

      // a is null AND b exists
      const result = await collection
        .find({ $and: [{ a: null }, { b: { $exists: true } }] })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 1);
    });

    it("should handle undefined fields in nested operators", async () => {
      const collection = client.db(dbName).collection("undefined_nested");
      await collection.insertMany([
        { _id: 1, a: 1 },
        { _id: 2, a: 1, b: 2 },
        { _id: 3, b: 2 },
      ]);

      // a exists AND b does not exist
      const result = await collection
        .find({
          $and: [{ a: { $exists: true } }, { b: { $exists: false } }],
        })
        .toArray();

      assert.strictEqual(result.length, 1);
      assert.strictEqual(result[0]._id, 1);
    });
  });
});
