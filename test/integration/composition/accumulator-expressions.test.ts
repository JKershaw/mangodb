/**
 * Accumulator Expression Composition Tests
 *
 * Tests for accumulators with nested expressions in $group stages.
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

describe(`Accumulator Expression Composition (${getTestModeName()})`, () => {
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

  describe("$sum with expressions", () => {
    it("should support $sum with $ifNull (null-safe sum)", async () => {
      const collection = client.db(dbName).collection("sum_ifnull");
      await collection.insertMany([
        { _id: 1, category: "A", value: 10 },
        { _id: 2, category: "A", value: null },
        { _id: 3, category: "A" }, // missing value
        { _id: 4, category: "A", value: 20 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              total: { $sum: { $ifNull: ["$value", 0] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].total, 30); // 10 + 0 + 0 + 20
    });

    it("should support $sum with $cond (conditional sum)", async () => {
      const collection = client.db(dbName).collection("sum_cond");
      await collection.insertMany([
        { _id: 1, category: "A", amount: 100, status: "active" },
        { _id: 2, category: "A", amount: 50, status: "inactive" },
        { _id: 3, category: "A", amount: 75, status: "active" },
        { _id: 4, category: "A", amount: 25, status: "inactive" },
      ]);

      // Sum only active amounts
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              activeTotal: {
                $sum: {
                  $cond: [{ $eq: ["$status", "active"] }, "$amount", 0],
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].activeTotal, 175); // 100 + 75
    });

    it("should support $sum with $multiply", async () => {
      const collection = client.db(dbName).collection("sum_multiply");
      await collection.insertMany([
        { _id: 1, category: "A", price: 10, qty: 2 },
        { _id: 2, category: "A", price: 15, qty: 3 },
        { _id: 3, category: "A", price: 20, qty: 1 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              revenue: { $sum: { $multiply: ["$price", "$qty"] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].revenue, 85); // (10*2) + (15*3) + (20*1)
    });

    it("should support $sum with $subtract (profit calculation)", async () => {
      const collection = client.db(dbName).collection("sum_subtract");
      await collection.insertMany([
        { _id: 1, category: "A", revenue: 100, cost: 60 },
        { _id: 2, category: "A", revenue: 80, cost: 50 },
        { _id: 3, category: "A", revenue: 120, cost: 70 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              totalProfit: { $sum: { $subtract: ["$revenue", "$cost"] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].totalProfit, 120); // 40 + 30 + 50
    });

    it("should support $sum with $abs", async () => {
      const collection = client.db(dbName).collection("sum_abs");
      await collection.insertMany([
        { _id: 1, category: "A", delta: 10 },
        { _id: 2, category: "A", delta: -15 },
        { _id: 3, category: "A", delta: 5 },
        { _id: 4, category: "A", delta: -20 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              totalMovement: { $sum: { $abs: "$delta" } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].totalMovement, 50); // 10 + 15 + 5 + 20
    });
  });

  describe("$avg with expressions", () => {
    it("should support $avg with $ifNull", async () => {
      const collection = client.db(dbName).collection("avg_ifnull");
      await collection.insertMany([
        { _id: 1, category: "A", score: 80 },
        { _id: 2, category: "A", score: null },
        { _id: 3, category: "A", score: 90 },
        { _id: 4, category: "A" }, // missing
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              avgScore: { $avg: { $ifNull: ["$score", 0] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].avgScore, 42.5); // (80 + 0 + 90 + 0) / 4
    });

    it("should support $avg with $cond", async () => {
      const collection = client.db(dbName).collection("avg_cond");
      await collection.insertMany([
        { _id: 1, category: "A", score: 80, valid: true },
        { _id: 2, category: "A", score: 50, valid: false },
        { _id: 3, category: "A", score: 90, valid: true },
        { _id: 4, category: "A", score: 30, valid: false },
      ]);

      // Average only valid scores, using null for invalid (which $avg ignores)
      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              avgValidScore: {
                $avg: {
                  $cond: [{ $eq: ["$valid", true] }, "$score", null],
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].avgValidScore, 85); // (80 + 90) / 2
    });

    it("should support $avg with division", async () => {
      const collection = client.db(dbName).collection("avg_divide");
      await collection.insertMany([
        { _id: 1, category: "A", total: 100, count: 4 },
        { _id: 2, category: "A", total: 150, count: 5 },
        { _id: 3, category: "A", total: 200, count: 8 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              avgPerItem: { $avg: { $divide: ["$total", "$count"] } },
            },
          },
        ])
        .toArray();

      // (100/4 + 150/5 + 200/8) / 3 = (25 + 30 + 25) / 3 = 26.67
      assert.ok(Math.abs((result[0].avgPerItem as number) - 26.67) < 0.01);
    });
  });

  describe("$push/$addToSet with expressions", () => {
    it("should support $push with computed value", async () => {
      const collection = client.db(dbName).collection("push_computed");
      await collection.insertMany([
        { _id: 1, category: "A", firstName: "John", lastName: "Doe" },
        { _id: 2, category: "A", firstName: "Jane", lastName: "Smith" },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              names: {
                $push: { $concat: ["$firstName", " ", "$lastName"] },
              },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual((result[0].names as string[]).sort(), ["Jane Smith", "John Doe"]);
    });

    it("should support $push with computed object", async () => {
      const collection = client.db(dbName).collection("push_object");
      await collection.insertMany([
        { _id: 1, category: "A", name: "Item1", price: 10, qty: 2 },
        { _id: 2, category: "A", name: "Item2", price: 15, qty: 3 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              items: {
                $push: {
                  name: "$name",
                  total: { $multiply: ["$price", "$qty"] },
                },
              },
            },
          },
        ])
        .toArray();

      const items = result[0].items as Array<{ name: string; total: number }>;
      assert.strictEqual(items.length, 2);

      const item1 = items.find((i) => i.name === "Item1");
      const item2 = items.find((i) => i.name === "Item2");
      assert.strictEqual(item1?.total, 20);
      assert.strictEqual(item2?.total, 45);
    });

    it("should support $addToSet with $toLower", async () => {
      const collection = client.db(dbName).collection("addtoset_lower");
      await collection.insertMany([
        { _id: 1, category: "A", tag: "JavaScript" },
        { _id: 2, category: "A", tag: "javascript" },
        { _id: 3, category: "A", tag: "JAVASCRIPT" },
        { _id: 4, category: "A", tag: "TypeScript" },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              uniqueTags: { $addToSet: { $toLower: "$tag" } },
            },
          },
        ])
        .toArray();

      const tags = result[0].uniqueTags as string[];
      assert.strictEqual(tags.length, 2);
      assert.ok(tags.includes("javascript"));
      assert.ok(tags.includes("typescript"));
    });
  });

  describe("$first/$last with expressions", () => {
    it("should support $first with $ifNull", async () => {
      const collection = client.db(dbName).collection("first_ifnull");
      await collection.insertMany([
        { _id: 1, category: "A", value: null, order: 1 },
        { _id: 2, category: "A", value: 10, order: 2 },
      ]);

      const result = await collection
        .aggregate([
          { $sort: { order: 1 } },
          {
            $group: {
              _id: "$category",
              firstValue: { $first: { $ifNull: ["$value", "default"] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].firstValue, "default");
    });

    it("should support $first with $concat", async () => {
      const collection = client.db(dbName).collection("first_concat");
      await collection.insertMany([
        { _id: 1, category: "A", prefix: "ID", num: "001", order: 1 },
        { _id: 2, category: "A", prefix: "ID", num: "002", order: 2 },
      ]);

      const result = await collection
        .aggregate([
          { $sort: { order: 1 } },
          {
            $group: {
              _id: "$category",
              firstId: { $first: { $concat: ["$prefix", "-", "$num"] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].firstId, "ID-001");
    });

    it("should support $last with computed value", async () => {
      const collection = client.db(dbName).collection("last_computed");
      await collection.insertMany([
        { _id: 1, category: "A", price: 10, qty: 2, order: 1 },
        { _id: 2, category: "A", price: 15, qty: 3, order: 2 },
        { _id: 3, category: "A", price: 20, qty: 1, order: 3 },
      ]);

      const result = await collection
        .aggregate([
          { $sort: { order: 1 } },
          {
            $group: {
              _id: "$category",
              lastTotal: { $last: { $multiply: ["$price", "$qty"] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].lastTotal, 20); // 20 * 1
    });
  });

  describe("$max/$min with expressions", () => {
    it("should support $max with computed value", async () => {
      const collection = client.db(dbName).collection("max_computed");
      await collection.insertMany([
        { _id: 1, category: "A", price: 10, qty: 5 },
        { _id: 2, category: "A", price: 8, qty: 10 },
        { _id: 3, category: "A", price: 15, qty: 2 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              maxRevenue: { $max: { $multiply: ["$price", "$qty"] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].maxRevenue, 80); // 8 * 10
    });

    it("should support $min with $subtract", async () => {
      const collection = client.db(dbName).collection("min_subtract");
      await collection.insertMany([
        { _id: 1, category: "A", price: 100, cost: 80 },
        { _id: 2, category: "A", price: 50, cost: 45 },
        { _id: 3, category: "A", price: 200, cost: 120 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              minProfit: { $min: { $subtract: ["$price", "$cost"] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(result[0].minProfit, 5); // 50 - 45
    });
  });

  describe("Multiple accumulators with expressions", () => {
    it("should support multiple accumulators in one $group", async () => {
      const collection = client.db(dbName).collection("multi_accum");
      await collection.insertMany([
        { _id: 1, category: "A", price: 10, qty: 2, discount: 0.1 },
        { _id: 2, category: "A", price: 20, qty: 3, discount: null },
        { _id: 3, category: "A", price: 15, qty: 4, discount: 0.2 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              totalRevenue: { $sum: { $multiply: ["$price", "$qty"] } },
              avgDiscount: { $avg: { $ifNull: ["$discount", 0] } },
              items: { $push: "$price" },
              maxProfit: {
                $max: {
                  $multiply: [
                    { $multiply: ["$price", "$qty"] },
                    { $subtract: [1, { $ifNull: ["$discount", 0] }] },
                  ],
                },
              },
            },
          },
        ])
        .toArray();

      // totalRevenue: (10*2) + (20*3) + (15*4) = 20 + 60 + 60 = 140
      assert.strictEqual(result[0].totalRevenue, 140);

      // avgDiscount: (0.1 + 0 + 0.2) / 3 = 0.1
      assert.ok(Math.abs((result[0].avgDiscount as number) - 0.1) < 0.0001);

      // items: [10, 20, 15]
      assert.strictEqual((result[0].items as unknown[]).length, 3);

      // maxProfit: max of (20*0.9, 60*1.0, 60*0.8) = max(18, 60, 48) = 60
      assert.strictEqual(result[0].maxProfit, 60);
    });

    it("should support nested expressions in all accumulators", async () => {
      const collection = client.db(dbName).collection("nested_all_accum");
      await collection.insertMany([
        { _id: 1, cat: "X", a: 10, b: 5, c: 2 },
        { _id: 2, cat: "X", a: 20, b: 10, c: 4 },
        { _id: 3, cat: "X", a: 30, b: 15, c: 6 },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$cat",
              sumProduct: { $sum: { $multiply: ["$a", "$b"] } },
              avgRatio: { $avg: { $divide: ["$a", "$c"] } },
              firstDiff: { $first: { $subtract: ["$a", "$b"] } },
              maxSum: { $max: { $add: ["$a", "$b", "$c"] } },
              minMod: { $min: { $mod: ["$a", "$c"] } },
            },
          },
        ])
        .toArray();

      // sumProduct: (10*5) + (20*10) + (30*15) = 50 + 200 + 450 = 700
      assert.strictEqual(result[0].sumProduct, 700);

      // avgRatio: (10/2 + 20/4 + 30/6) / 3 = (5 + 5 + 5) / 3 = 5
      assert.strictEqual(result[0].avgRatio, 5);

      // firstDiff: 10 - 5 = 5 (assuming natural order)
      // Note: order may vary, so just check it's a valid difference
      assert.ok([5, 10, 15].includes(result[0].firstDiff as number));

      // maxSum: max(10+5+2, 20+10+4, 30+15+6) = max(17, 34, 51) = 51
      assert.strictEqual(result[0].maxSum, 51);

      // minMod: min(10%2, 20%4, 30%6) = min(0, 0, 0) = 0
      assert.strictEqual(result[0].minMod, 0);
    });
  });

  describe("$count accumulator", () => {
    it("should work alongside complex accumulators", async () => {
      const collection = client.db(dbName).collection("count_with_complex");
      await collection.insertMany([
        { _id: 1, category: "A", value: 10, active: true },
        { _id: 2, category: "A", value: 20, active: true },
        { _id: 3, category: "A", value: 30, active: false },
        { _id: 4, category: "B", value: 40, active: true },
      ]);

      const result = await collection
        .aggregate([
          {
            $group: {
              _id: "$category",
              count: { $count: {} },
              activeSum: {
                $sum: {
                  $cond: [{ $eq: ["$active", true] }, "$value", 0],
                },
              },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(result[0]._id, "A");
      assert.strictEqual(result[0].count, 3);
      assert.strictEqual(result[0].activeSum, 30); // 10 + 20

      assert.strictEqual(result[1]._id, "B");
      assert.strictEqual(result[1].count, 1);
      assert.strictEqual(result[1].activeSum, 40);
    });
  });
});
