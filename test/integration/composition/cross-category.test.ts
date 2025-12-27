/**
 * Cross-Category Composition Tests
 *
 * Tests for compositions that span multiple operator categories:
 * - Query + Aggregation bridges
 * - Update + Query composition
 * - Type coercion in compositions
 * - Complex real-world scenarios
 *
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  isMongoDBMode,
  type TestClient,
} from "../../test-harness.ts";

describe(`Cross-Category Composition (${getTestModeName()})`, () => {
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

  describe("Query + Aggregation bridges", () => {
    it("should support $match with $expr containing $size", async () => {
      const collection = client.db(dbName).collection("match_expr_size");
      await collection.insertMany([
        { _id: 1, items: [1, 2, 3] },
        { _id: 2, items: [] },
        { _id: 3, items: [1, 2] },
      ]);

      const result = await collection
        .aggregate([
          {
            $match: {
              $expr: { $gt: [{ $size: "$items" }, 0] },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 3]);
    });

    it("should support $match with $expr and complex expression", async () => {
      const collection = client.db(dbName).collection("match_expr_complex");
      await collection.insertMany([
        { _id: 1, items: [{ price: 10 }, { price: 20 }], budget: 50 },
        { _id: 2, items: [{ price: 30 }, { price: 40 }], budget: 50 },
        { _id: 3, items: [{ price: 5 }, { price: 10 }], budget: 20 },
      ]);

      // Find docs where sum of item prices < budget
      const result = await collection
        .aggregate([
          {
            $match: {
              $expr: {
                $lt: [
                  {
                    $reduce: {
                      input: "$items",
                      initialValue: 0,
                      in: { $add: ["$$value", "$$this.price"] },
                    },
                  },
                  "$budget",
                ],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 3]); // 30 < 50, 15 < 20
    });

    it("should combine query-style and expression-style in pipeline", async () => {
      const collection = client.db(dbName).collection("combined_pipeline");
      await collection.insertMany([
        { _id: 1, status: "active", category: "A", items: [{ price: 100 }] },
        { _id: 2, status: "active", category: "B", items: [{ price: 200 }] },
        { _id: 3, status: "inactive", category: "A", items: [{ price: 300 }] },
        { _id: 4, status: "active", category: "A", items: [{ price: 50 }] },
      ]);

      const result = await collection
        .aggregate([
          // Query-style $match
          { $match: { status: "active", category: { $in: ["A", "B"] } } },
          // Expression-style $addFields
          {
            $addFields: {
              total: {
                $reduce: {
                  input: "$items",
                  initialValue: 0,
                  in: { $add: ["$$value", "$$this.price"] },
                },
              },
            },
          },
          // Expression-style $match
          { $match: { $expr: { $gt: ["$total", 75] } } },
        ])
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 2]); // 100 > 75, 200 > 75
    });

    it("should support find() with $expr in query", async () => {
      const collection = client.db(dbName).collection("find_expr");
      await collection.insertMany([
        { _id: 1, quantity: 10, minRequired: 5 },
        { _id: 2, quantity: 3, minRequired: 5 },
        { _id: 3, quantity: 20, minRequired: 15 },
      ]);

      // Find where quantity >= minRequired
      const result = await collection
        .find({
          $expr: { $gte: ["$quantity", "$minRequired"] },
        })
        .toArray();

      assert.strictEqual(result.length, 2);
      const ids = result.map((d) => d._id).sort();
      assert.deepStrictEqual(ids, [1, 3]);
    });
  });

  describe("Update + Query composition", () => {
    it("should support arrayFilters with multiple conditions", async () => {
      const collection = client.db(dbName).collection("arrayfilters_multi");
      await collection.insertOne({
        _id: 1,
        items: [
          { name: "a", qty: 10, status: "active" },
          { name: "b", qty: 5, status: "deleted" },
          { name: "c", qty: 15, status: "active" },
          { name: "d", qty: 3, status: "active" },
        ],
      });

      // Update only items that are active AND have qty > 5
      await collection.updateOne(
        { _id: 1 },
        { $set: { "items.$[elem].marked": true } },
        {
          arrayFilters: [
            { "elem.qty": { $gt: 5 }, "elem.status": { $ne: "deleted" } },
          ],
        }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      const items = doc.items as Array<{
        name: string;
        marked?: boolean;
      }>;

      assert.strictEqual(items[0].marked, true); // a: qty=10, active
      assert.strictEqual(items[1].marked, undefined); // b: deleted
      assert.strictEqual(items[2].marked, true); // c: qty=15, active
      assert.strictEqual(items[3].marked, undefined); // d: qty=3 <= 5
    });

    it("should support pipeline update with $cond", { skip: !isMongoDBMode() }, async () => {
      // Pipeline updates (aggregation pipeline as update) are MongoDB-only
      const collection = client.db(dbName).collection("pipeline_update");
      await collection.insertMany([
        { _id: 1, qty: 10 },
        { _id: 2, qty: 0 },
        { _id: 3, qty: 5 },
      ]);

      // Pipeline updates use array syntax (MongoDB-only feature)
      await collection.updateMany({}, [
        {
          $set: {
            status: { $cond: [{ $gt: ["$qty", 0] }, "in-stock", "out-of-stock"] },
          },
        },
      ] as unknown as import("mongodb").Document);

      const docs = await collection.find().sort({ _id: 1 }).toArray();

      assert.strictEqual(docs[0].status, "in-stock");
      assert.strictEqual(docs[1].status, "out-of-stock");
      assert.strictEqual(docs[2].status, "in-stock");
    });
  });

  describe("Type coercion in compositions", () => {
    it("should support $toInt in arithmetic chain", async () => {
      const collection = client.db(dbName).collection("toint_chain");
      await collection.insertOne({
        _id: 1,
        stringQty: "5",
        price: 10,
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              total: { $multiply: [{ $toInt: "$stringQty" }, "$price"] },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].total, 50);
    });

    it("should support $toString in $concat", async () => {
      const collection = client.db(dbName).collection("tostring_concat");
      await collection.insertOne({
        _id: 1,
        orderId: 12345,
        prefix: "ORD",
      });

      const result = await collection
        .aggregate([
          {
            $project: {
              orderCode: {
                $concat: ["$prefix", "-", { $toString: "$orderId" }],
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].orderCode, "ORD-12345");
    });

    it("should support type-safe comparison with $isNumber", async () => {
      const collection = client.db(dbName).collection("isnumber_safe");
      await collection.insertMany([
        { _id: 1, value: 10 },
        { _id: 2, value: "not a number" },
        { _id: 3, value: null },
        { _id: 4, value: 25 },
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              safeValue: {
                $cond: [
                  { $and: [{ $isNumber: "$value" }, { $gt: ["$value", 0] }] },
                  "$value",
                  0,
                ],
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].safeValue, 10);
      assert.strictEqual(result[1].safeValue, 0);
      assert.strictEqual(result[2].safeValue, 0);
      assert.strictEqual(result[3].safeValue, 25);
    });

    it("should support $convert with onError in expression", async () => {
      const collection = client.db(dbName).collection("convert_onerror");
      await collection.insertMany([
        { _id: 1, a: "10", b: "20" },
        { _id: 2, a: "abc", b: "30" },
        { _id: 3, a: "15", b: "xyz" },
      ]);

      const result = await collection
        .aggregate([
          {
            $project: {
              sum: {
                $add: [
                  { $convert: { input: "$a", to: "int", onError: 0 } },
                  { $convert: { input: "$b", to: "int", onError: 0 } },
                ],
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0].sum, 30); // 10 + 20
      assert.strictEqual(result[1].sum, 30); // 0 + 30
      assert.strictEqual(result[2].sum, 15); // 15 + 0
    });
  });

  describe("Complex real-world scenarios", () => {
    it("should calculate discounted totals with null handling (e-commerce)", async () => {
      const collection = client.db(dbName).collection("ecommerce_calc");
      await collection.insertMany([
        { _id: 1, orderId: "A", price: 100, qty: 2, discountRate: 0.1 },
        { _id: 2, orderId: "A", price: 50, qty: 3, discountRate: null },
        { _id: 3, orderId: "A", price: 75, qty: 1 }, // missing discountRate
        { _id: 4, orderId: "B", price: 200, qty: 1, discountRate: 0.2 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$orderId",
              subtotal: { $sum: { $multiply: ["$price", "$qty"] } },
              totalDiscount: {
                $sum: {
                  $multiply: [
                    { $multiply: ["$price", "$qty"] },
                    { $ifNull: ["$discountRate", 0] },
                  ],
                },
              },
              finalTotal: {
                $sum: {
                  $multiply: [
                    { $multiply: ["$price", "$qty"] },
                    { $subtract: [1, { $ifNull: ["$discountRate", 0] }] },
                  ],
                },
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      // Order A:
      // Subtotal: 200 + 150 + 75 = 425
      // Discounts: (200 * 0.1) + (150 * 0) + (75 * 0) = 20
      // Final: (200 * 0.9) + (150 * 1) + (75 * 1) = 180 + 150 + 75 = 405
      assert.strictEqual(result[0]._id, "A");
      assert.strictEqual(result[0].subtotal, 425);
      assert.strictEqual(result[0].totalDiscount, 20);
      assert.strictEqual(result[0].finalTotal, 405);

      // Order B:
      // Subtotal: 200
      // Discounts: 200 * 0.2 = 40
      // Final: 200 * 0.8 = 160
      assert.strictEqual(result[1]._id, "B");
      assert.strictEqual(result[1].subtotal, 200);
      assert.strictEqual(result[1].totalDiscount, 40);
      assert.strictEqual(result[1].finalTotal, 160);
    });

    it("should categorize with conditional bucketing and date math (analytics)", async () => {
      const now = new Date();
      const daysAgo = (n: number) =>
        new Date(now.getTime() - n * 24 * 60 * 60 * 1000);

      const collection = client.db(dbName).collection("analytics_bucket");
      await collection.insertMany([
        { _id: 1, name: "Recent1", createdAt: daysAgo(3) },
        { _id: 2, name: "Recent2", createdAt: daysAgo(5) },
        { _id: 3, name: "ThisMonth", createdAt: daysAgo(15) },
        { _id: 4, name: "Older", createdAt: daysAgo(45) },
      ]);

      const result = await collection
        .aggregate([
          {
            $addFields: {
              ageInDays: {
                $dateDiff: {
                  startDate: "$createdAt",
                  endDate: "$$NOW",
                  unit: "day",
                },
              },
            },
          },
          {
            $addFields: {
              bucket: {
                $switch: {
                  branches: [
                    { case: { $lte: ["$ageInDays", 7] }, then: "this-week" },
                    { case: { $lte: ["$ageInDays", 30] }, then: "this-month" },
                  ],
                  default: "older",
                },
              },
            },
          },
          {
            $group: {
              _id: "$bucket",
              count: { $sum: 1 },
              items: { $push: "$name" },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      const buckets = Object.fromEntries(result.map((r) => [r._id, r]));

      assert.strictEqual(buckets["this-week"].count, 2);
      assert.ok(buckets["this-week"].items.includes("Recent1"));
      assert.ok(buckets["this-week"].items.includes("Recent2"));

      assert.strictEqual(buckets["this-month"].count, 1);
      assert.ok(buckets["this-month"].items.includes("ThisMonth"));

      assert.strictEqual(buckets["older"].count, 1);
      assert.ok(buckets["older"].items.includes("Older"));
    });

    it("should transform and aggregate nested data (inventory)", async () => {
      const collection = client.db(dbName).collection("inventory_transform");
      await collection.insertMany([
        {
          _id: 1,
          warehouse: "A",
          products: [
            { sku: "P1", stock: 100, reserved: 20 },
            { sku: "P2", stock: 50, reserved: 10 },
          ],
        },
        {
          _id: 2,
          warehouse: "B",
          products: [
            { sku: "P1", stock: 75, reserved: 5 },
            { sku: "P3", stock: 200, reserved: 50 },
          ],
        },
      ]);

      const result = await collection
        .aggregate([
          { $unwind: "$products" },
          {
            $project: {
              warehouse: 1,
              sku: "$products.sku",
              available: {
                $subtract: ["$products.stock", "$products.reserved"],
              },
            },
          },
          {
            $group: {
              _id: "$sku",
              totalAvailable: { $sum: "$available" },
              warehouses: { $push: "$warehouse" },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      // P1: (100-20) + (75-5) = 80 + 70 = 150
      assert.strictEqual(result[0]._id, "P1");
      assert.strictEqual(result[0].totalAvailable, 150);
      assert.deepStrictEqual((result[0].warehouses as string[]).sort(), ["A", "B"]);

      // P2: 50-10 = 40
      assert.strictEqual(result[1]._id, "P2");
      assert.strictEqual(result[1].totalAvailable, 40);

      // P3: 200-50 = 150
      assert.strictEqual(result[2]._id, "P3");
      assert.strictEqual(result[2].totalAvailable, 150);
    });
  });
});
