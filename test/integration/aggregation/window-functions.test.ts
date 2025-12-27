/**
 * Phase 10: Advanced Aggregation Pipeline Tests
 *
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
  type TestCollection,
  type Document,
} from "../../test-harness.ts";

describe(`Advanced Aggregation Pipeline Tests (${getTestModeName()})`, () => {
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

  // ==================== Expression Operators ====================

  describe("Expression Operators", () => {
    describe("$add", () => {
      it("should add numbers", async () => {
        const collection = client.db(dbName).collection("expr_add_nums");
        await collection.insertOne({ a: 10, b: 5 });

        const results = await collection
          .aggregate([
            { $project: { sum: { $add: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].sum, 15);
      });

      it("should add multiple numbers", async () => {
        const collection = client.db(dbName).collection("expr_add_multi");
        await collection.insertOne({ a: 1, b: 2, c: 3, d: 4 });

        const results = await collection
          .aggregate([
            { $project: { sum: { $add: ["$a", "$b", "$c", "$d"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].sum, 10);
      });

      it("should return null if any operand is null", async () => {
        const collection = client.db(dbName).collection("expr_add_null");
        await collection.insertOne({ a: 10, b: null });

        const results = await collection
          .aggregate([
            { $project: { sum: { $add: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].sum, null);
      });

      it("should return null if any operand is missing", async () => {
        const collection = client.db(dbName).collection("expr_add_missing");
        await collection.insertOne({ a: 10 });

        const results = await collection
          .aggregate([
            { $project: { sum: { $add: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].sum, null);
      });
    });

    describe("$subtract", () => {
      it("should subtract numbers", async () => {
        const collection = client.db(dbName).collection("expr_sub_nums");
        await collection.insertOne({ a: 10, b: 3 });

        const results = await collection
          .aggregate([
            { $project: { diff: { $subtract: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].diff, 7);
      });

      it("should return null if any operand is null", async () => {
        const collection = client.db(dbName).collection("expr_sub_null");
        await collection.insertOne({ a: 10, b: null });

        const results = await collection
          .aggregate([
            { $project: { diff: { $subtract: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].diff, null);
      });
    });

    describe("$multiply", () => {
      it("should multiply numbers", async () => {
        const collection = client.db(dbName).collection("expr_mul_nums");
        await collection.insertOne({ a: 6, b: 7 });

        const results = await collection
          .aggregate([
            { $project: { product: { $multiply: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].product, 42);
      });

      it("should multiply multiple numbers", async () => {
        const collection = client.db(dbName).collection("expr_mul_multi");
        await collection.insertOne({ a: 2, b: 3, c: 4 });

        const results = await collection
          .aggregate([
            { $project: { product: { $multiply: ["$a", "$b", "$c"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].product, 24);
      });

      it("should return null if any operand is null", async () => {
        const collection = client.db(dbName).collection("expr_mul_null");
        await collection.insertOne({ a: 10, b: null });

        const results = await collection
          .aggregate([
            { $project: { product: { $multiply: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].product, null);
      });
    });

    describe("$divide", () => {
      it("should divide numbers", async () => {
        const collection = client.db(dbName).collection("expr_div_nums");
        await collection.insertOne({ a: 20, b: 4 });

        const results = await collection
          .aggregate([
            { $project: { quotient: { $divide: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].quotient, 5);
      });

      it("should return null if any operand is null", async () => {
        const collection = client.db(dbName).collection("expr_div_null");
        await collection.insertOne({ a: 20, b: null });

        const results = await collection
          .aggregate([
            { $project: { quotient: { $divide: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].quotient, null);
      });
    });

    describe("$concat", () => {
      it("should concatenate strings", async () => {
        const collection = client.db(dbName).collection("expr_concat");
        await collection.insertOne({ first: "Hello", second: "World" });

        const results = await collection
          .aggregate([
            { $project: { result: { $concat: ["$first", " ", "$second"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].result, "Hello World");
      });

      it("should return null if any operand is null", async () => {
        const collection = client.db(dbName).collection("expr_concat_null");
        await collection.insertOne({ first: "Hello", second: null });

        const results = await collection
          .aggregate([
            { $project: { result: { $concat: ["$first", "$second"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe("$toUpper / $toLower", () => {
      it("should convert string to uppercase", async () => {
        const collection = client.db(dbName).collection("expr_toupper");
        await collection.insertOne({ text: "Hello World" });

        const results = await collection
          .aggregate([
            { $project: { upper: { $toUpper: "$text" }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].upper, "HELLO WORLD");
      });

      it("should convert string to lowercase", async () => {
        const collection = client.db(dbName).collection("expr_tolower");
        await collection.insertOne({ text: "Hello World" });

        const results = await collection
          .aggregate([
            { $project: { lower: { $toLower: "$text" }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].lower, "hello world");
      });

      it("should return empty string for null input", async () => {
        const collection = client.db(dbName).collection("expr_case_null");
        await collection.insertOne({ text: null });

        const results = await collection
          .aggregate([
            { $project: { upper: { $toUpper: "$text" }, _id: 0 } },
          ])
          .toArray();

        // MongoDB returns empty string for null input
        assert.strictEqual(results[0].upper, "");
      });
    });

    describe("$cond", () => {
      it("should return then value for truthy condition", async () => {
        const collection = client.db(dbName).collection("expr_cond_true");
        await collection.insertOne({ score: 80 });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $cond: [{ $gte: ["$score", 60] }, "pass", "fail"],
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, "pass");
      });

      it("should return else value for falsy condition", async () => {
        const collection = client.db(dbName).collection("expr_cond_false");
        await collection.insertOne({ score: 40 });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $cond: [{ $gte: ["$score", 60] }, "pass", "fail"],
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, "fail");
      });

      it("should support object syntax (if/then/else)", async () => {
        const collection = client.db(dbName).collection("expr_cond_obj");
        await collection.insertOne({ score: 80 });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $cond: {
                    if: { $gte: ["$score", 60] },
                    then: "pass",
                    else: "fail",
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, "pass");
      });
    });

    describe("$ifNull", () => {
      it("should return first non-null value", async () => {
        const collection = client.db(dbName).collection("expr_ifnull");
        await collection.insertOne({ a: null, b: "default" });

        const results = await collection
          .aggregate([
            { $project: { result: { $ifNull: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].result, "default");
      });

      it("should return first value if not null", async () => {
        const collection = client.db(dbName).collection("expr_ifnull_first");
        await collection.insertOne({ a: "value", b: "default" });

        const results = await collection
          .aggregate([
            { $project: { result: { $ifNull: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].result, "value");
      });

      it("should handle missing fields as null", async () => {
        const collection = client.db(dbName).collection("expr_ifnull_missing");
        await collection.insertOne({ b: "default" });

        const results = await collection
          .aggregate([
            { $project: { result: { $ifNull: ["$a", "$b"] }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].result, "default");
      });
    });

    describe("$size (expression)", () => {
      it("should return array length", async () => {
        const collection = client.db(dbName).collection("expr_size");
        await collection.insertOne({ items: [1, 2, 3, 4, 5] });

        const results = await collection
          .aggregate([
            { $project: { count: { $size: "$items" }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].count, 5);
      });

      it("should return 0 for empty array", async () => {
        const collection = client.db(dbName).collection("expr_size_empty");
        await collection.insertOne({ items: [] });

        const results = await collection
          .aggregate([
            { $project: { count: { $size: "$items" }, _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].count, 0);
      });
    });
  });

  // ==================== $group Stage ====================

  describe("$group Stage", () => {
    describe("Basic Grouping", () => {
      it("should group by single field", async () => {
        const collection = client.db(dbName).collection("group_single");
        await collection.insertMany([
          { category: "A", value: 10 },
          { category: "B", value: 20 },
          { category: "A", value: 30 },
          { category: "B", value: 40 },
        ]);

        const results = await collection
          .aggregate([
            { $group: { _id: "$category", count: { $sum: 1 } } },
            { $sort: { _id: 1 } },
          ])
          .toArray();

        assert.strictEqual(results.length, 2);
        assert.strictEqual(results[0]._id, "A");
        assert.strictEqual(results[0].count, 2);
        assert.strictEqual(results[1]._id, "B");
        assert.strictEqual(results[1].count, 2);
      });

      it("should group all with null _id", async () => {
        const collection = client.db(dbName).collection("group_null_id");
        await collection.insertMany([
          { value: 10 },
          { value: 20 },
          { value: 30 },
        ]);

        const results = await collection
          .aggregate([{ $group: { _id: null, total: { $sum: "$value" } } }])
          .toArray();

        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0]._id, null);
        assert.strictEqual(results[0].total, 60);
      });

      it("should return empty for empty input", async () => {
        const collection = client.db(dbName).collection("group_empty");
        // Don't insert anything

        const results = await collection
          .aggregate([{ $group: { _id: "$category", count: { $sum: 1 } } }])
          .toArray();

        assert.strictEqual(results.length, 0);
      });

      it("should group by compound _id", async () => {
        const collection = client.db(dbName).collection("group_compound");
        await collection.insertMany([
          { year: 2024, month: 1, amount: 100 },
          { year: 2024, month: 1, amount: 200 },
          { year: 2024, month: 2, amount: 150 },
        ]);

        const results = await collection
          .aggregate([
            {
              $group: {
                _id: { year: "$year", month: "$month" },
                total: { $sum: "$amount" },
              },
            },
            { $sort: { "_id.month": 1 } },
          ])
          .toArray();

        assert.strictEqual(results.length, 2);
        const jan = results.find(
          (r) => (r._id as { month: number }).month === 1
        );
        assert.strictEqual(jan?.total, 300);
      });
    });

    describe("$sum Accumulator", () => {
      it("should sum numeric values", async () => {
        const collection = client.db(dbName).collection("group_sum");
        await collection.insertMany([
          { category: "A", value: 10 },
          { category: "A", value: 20 },
          { category: "A", value: 30 },
        ]);

        const results = await collection
          .aggregate([{ $group: { _id: "$category", total: { $sum: "$value" } } }])
          .toArray();

        assert.strictEqual(results[0].total, 60);
      });

      it("should count with $sum: 1", async () => {
        const collection = client.db(dbName).collection("group_sum_count");
        await collection.insertMany([
          { status: "active" },
          { status: "active" },
          { status: "inactive" },
        ]);

        const results = await collection
          .aggregate([
            { $group: { _id: "$status", count: { $sum: 1 } } },
            { $sort: { _id: 1 } },
          ])
          .toArray();

        assert.strictEqual(results[0].count, 2); // active
        assert.strictEqual(results[1].count, 1); // inactive
      });
    });

    describe("$avg Accumulator", () => {
      it("should calculate average", async () => {
        const collection = client.db(dbName).collection("group_avg");
        await collection.insertMany([
          { category: "A", score: 80 },
          { category: "A", score: 90 },
          { category: "A", score: 100 },
        ]);

        const results = await collection
          .aggregate([{ $group: { _id: "$category", avg: { $avg: "$score" } } }])
          .toArray();

        assert.strictEqual(results[0].avg, 90);
      });

      it("should return null for no numeric values", async () => {
        const collection = client.db(dbName).collection("group_avg_empty");
        await collection.insertMany([
          { category: "A", score: null },
          { category: "A", score: null },
        ]);

        const results = await collection
          .aggregate([{ $group: { _id: "$category", avg: { $avg: "$score" } } }])
          .toArray();

        assert.strictEqual(results[0].avg, null);
      });
    });

    describe("$min / $max Accumulators", () => {
      it("should find minimum value", async () => {
        const collection = client.db(dbName).collection("group_min");
        await collection.insertMany([
          { category: "A", value: 30 },
          { category: "A", value: 10 },
          { category: "A", value: 20 },
        ]);

        const results = await collection
          .aggregate([{ $group: { _id: "$category", min: { $min: "$value" } } }])
          .toArray();

        assert.strictEqual(results[0].min, 10);
      });

      it("should find maximum value", async () => {
        const collection = client.db(dbName).collection("group_max");
        await collection.insertMany([
          { category: "A", value: 30 },
          { category: "A", value: 10 },
          { category: "A", value: 20 },
        ]);

        const results = await collection
          .aggregate([{ $group: { _id: "$category", max: { $max: "$value" } } }])
          .toArray();

        assert.strictEqual(results[0].max, 30);
      });
    });

    describe("$first / $last Accumulators", () => {
      it("should return first value in group", async () => {
        const collection = client.db(dbName).collection("group_first");
        await collection.insertMany([
          { category: "A", order: 1, name: "first" },
          { category: "A", order: 2, name: "second" },
          { category: "A", order: 3, name: "third" },
        ]);

        const results = await collection
          .aggregate([
            { $sort: { order: 1 } },
            { $group: { _id: "$category", firstName: { $first: "$name" } } },
          ])
          .toArray();

        assert.strictEqual(results[0].firstName, "first");
      });

      it("should return last value in group", async () => {
        const collection = client.db(dbName).collection("group_last");
        await collection.insertMany([
          { category: "A", order: 1, name: "first" },
          { category: "A", order: 2, name: "second" },
          { category: "A", order: 3, name: "third" },
        ]);

        const results = await collection
          .aggregate([
            { $sort: { order: 1 } },
            { $group: { _id: "$category", lastName: { $last: "$name" } } },
          ])
          .toArray();

        assert.strictEqual(results[0].lastName, "third");
      });
    });

    describe("$push Accumulator", () => {
      it("should collect all values into array", async () => {
        const collection = client.db(dbName).collection("group_push");
        await collection.insertMany([
          { category: "A", item: "x" },
          { category: "A", item: "y" },
          { category: "A", item: "z" },
        ]);

        const results = await collection
          .aggregate([
            { $sort: { item: 1 } },
            { $group: { _id: "$category", items: { $push: "$item" } } },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].items, ["x", "y", "z"]);
      });
    });

    describe("$addToSet Accumulator", () => {
      it("should collect unique values", async () => {
        const collection = client.db(dbName).collection("group_addtoset");
        await collection.insertMany([
          { category: "A", tag: "red" },
          { category: "A", tag: "blue" },
          { category: "A", tag: "red" }, // Duplicate
          { category: "A", tag: "green" },
        ]);

        const results = await collection
          .aggregate([{ $group: { _id: "$category", tags: { $addToSet: "$tag" } } }])
          .toArray();

        const tags = results[0].tags as string[];
        assert.strictEqual(tags.length, 3);
        assert.ok(tags.includes("red"));
        assert.ok(tags.includes("blue"));
        assert.ok(tags.includes("green"));
      });
    });
  });

  // ==================== $addFields / $set Stage ====================

  describe("$addFields / $set Stage", () => {
    it("should add new field with literal value", async () => {
      const collection = client.db(dbName).collection("addfields_literal");
      await collection.insertOne({ name: "Alice" });

      const results = await collection
        .aggregate([{ $addFields: { status: "active" } }])
        .toArray();

      assert.strictEqual(results[0].name, "Alice");
      assert.strictEqual(results[0].status, "active");
    });

    it("should add field with field reference", async () => {
      const collection = client.db(dbName).collection("addfields_ref");
      await collection.insertOne({ firstName: "Alice", lastName: "Smith" });

      const results = await collection
        .aggregate([{ $addFields: { name: "$firstName" } }])
        .toArray();

      assert.strictEqual(results[0].name, "Alice");
      assert.strictEqual(results[0].firstName, "Alice"); // Original preserved
    });

    it("should add field with expression", async () => {
      const collection = client.db(dbName).collection("addfields_expr");
      await collection.insertOne({ price: 100, tax: 10 });

      const results = await collection
        .aggregate([{ $addFields: { total: { $add: ["$price", "$tax"] } } }])
        .toArray();

      assert.strictEqual(results[0].total, 110);
      assert.strictEqual(results[0].price, 100); // Original preserved
    });

    it("should preserve existing fields", async () => {
      const collection = client.db(dbName).collection("addfields_preserve");
      await collection.insertOne({ a: 1, b: 2, c: 3 });

      const results = await collection
        .aggregate([{ $addFields: { d: 4 } }])
        .toArray();

      assert.strictEqual(results[0].a, 1);
      assert.strictEqual(results[0].b, 2);
      assert.strictEqual(results[0].c, 3);
      assert.strictEqual(results[0].d, 4);
    });

    it("should overwrite existing field", async () => {
      const collection = client.db(dbName).collection("addfields_overwrite");
      await collection.insertOne({ name: "Alice", status: "pending" });

      const results = await collection
        .aggregate([{ $addFields: { status: "active" } }])
        .toArray();

      assert.strictEqual(results[0].status, "active");
    });

    it("$set should behave identically to $addFields", async () => {
      const collection = client.db(dbName).collection("set_same");
      await collection.insertOne({ name: "Alice" });

      const results = await collection
        .aggregate([{ $set: { status: "active" } }])
        .toArray();

      assert.strictEqual(results[0].name, "Alice");
      assert.strictEqual(results[0].status, "active");
    });
  });

  // ==================== $replaceRoot Stage ====================

  describe("$replaceRoot Stage", () => {
    it("should replace document with embedded document", async () => {
      const collection = client.db(dbName).collection("replaceroot_basic");
      await collection.insertOne({
        name: "Alice",
        address: { city: "NYC", zip: "10001" },
      });

      const results = await collection
        .aggregate([{ $replaceRoot: { newRoot: "$address" } }])
        .toArray();

      assert.strictEqual(results[0].city, "NYC");
      assert.strictEqual(results[0].zip, "10001");
      assert.strictEqual(results[0].name, undefined);
    });

    it("should throw for null newRoot value", async () => {
      const collection = client.db(dbName).collection("replaceroot_null");
      await collection.insertOne({ name: "Alice", address: null });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $replaceRoot: { newRoot: "$address" } }])
            .toArray();
        },
        (err: Error) => {
          // MongoDB error message mentions 'newRoot' or 'object'
          assert.ok(
            err.message.toLowerCase().includes("newroot") ||
              err.message.toLowerCase().includes("object")
          );
          return true;
        }
      );
    });

    it("should throw for missing embedded document", async () => {
      const collection = client.db(dbName).collection("replaceroot_missing");
      await collection.insertOne({ name: "Alice" }); // No address field

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $replaceRoot: { newRoot: "$address" } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(
            err.message.toLowerCase().includes("newroot") ||
              err.message.toLowerCase().includes("object")
          );
          return true;
        }
      );
    });
  });

  // ==================== $lookup Stage ====================

  describe("$lookup Stage", () => {
    it("should join collections on matching field", async () => {
      const orders = client.db(dbName).collection("lookup_orders");
      const products = client.db(dbName).collection("lookup_products");

      await products.insertMany([
        { _id: "prod1" as unknown, name: "Widget", price: 10 },
        { _id: "prod2" as unknown, name: "Gadget", price: 20 },
      ]);

      await orders.insertOne({ orderId: 1, productId: "prod1", quantity: 5 });

      const results = await orders
        .aggregate([
          {
            $lookup: {
              from: "lookup_products",
              localField: "productId",
              foreignField: "_id",
              as: "product",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].orderId, 1);
      assert.ok(Array.isArray(results[0].product));
      assert.strictEqual((results[0].product as Document[]).length, 1);
      assert.strictEqual((results[0].product as Document[])[0].name, "Widget");
    });

    it("should return empty array for no matches", async () => {
      const orders = client.db(dbName).collection("lookup_orders_nomatch");
      const products = client.db(dbName).collection("lookup_products_nomatch");

      await products.insertOne({ _id: "prod1" as unknown, name: "Widget" });
      await orders.insertOne({ orderId: 1, productId: "prod_nonexistent" });

      const results = await orders
        .aggregate([
          {
            $lookup: {
              from: "lookup_products_nomatch",
              localField: "productId",
              foreignField: "_id",
              as: "product",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.deepStrictEqual(results[0].product, []);
    });

    it("should return multiple matches in array", async () => {
      const users = client.db(dbName).collection("lookup_users");
      const comments = client.db(dbName).collection("lookup_comments");

      await users.insertOne({ _id: "user1" as unknown, name: "Alice" });
      await comments.insertMany([
        { userId: "user1", text: "Comment 1" },
        { userId: "user1", text: "Comment 2" },
        { userId: "user1", text: "Comment 3" },
      ]);

      const results = await users
        .aggregate([
          {
            $lookup: {
              from: "lookup_comments",
              localField: "_id",
              foreignField: "userId",
              as: "comments",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual((results[0].comments as Document[]).length, 3);
    });

    it("should support pipeline form with let and pipeline", async () => {
      const orders = client.db(dbName).collection("lookup_pipe_orders");
      const inventory = client.db(dbName).collection("lookup_pipe_inventory");

      await orders.insertOne({
        _id: 1,
        item: "almonds",
        price: 12,
        quantity: 2,
      });

      await inventory.insertMany([
        { sku: "almonds", description: "product 1", instock: 120 },
        { sku: "bread", description: "product 2", instock: 80 },
        { sku: "cashews", description: "product 3", instock: 60 },
      ]);

      const results = await orders
        .aggregate([
          {
            $lookup: {
              from: "lookup_pipe_inventory",
              let: { orderItem: "$item" },
              pipeline: [
                {
                  $match: {
                    $expr: { $eq: ["$sku", "$$orderItem"] },
                  },
                },
                { $project: { description: 1, instock: 1, _id: 0 } },
              ],
              as: "fromItems",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.ok(Array.isArray(results[0].fromItems));
      assert.strictEqual((results[0].fromItems as Document[]).length, 1);
      assert.strictEqual(
        (results[0].fromItems as Document[])[0].description,
        "product 1"
      );
      assert.strictEqual((results[0].fromItems as Document[])[0].instock, 120);
    });

    it("should filter and aggregate in pipeline lookup", async () => {
      const customers = client.db(dbName).collection("lookup_pipe_customers");
      const transactions = client.db(dbName).collection("lookup_pipe_txn");

      await customers.insertMany([
        { _id: 1, name: "Alice", minSpend: 100 },
        { _id: 2, name: "Bob", minSpend: 50 },
      ]);

      await transactions.insertMany([
        { customerId: 1, amount: 80 },
        { customerId: 1, amount: 150 },
        { customerId: 1, amount: 50 },
        { customerId: 2, amount: 200 },
      ]);

      const results = await customers
        .aggregate([
          {
            $lookup: {
              from: "lookup_pipe_txn",
              let: { custId: "$_id", minAmt: "$minSpend" },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        { $eq: ["$customerId", "$$custId"] },
                        { $gte: ["$amount", "$$minAmt"] },
                      ],
                    },
                  },
                },
              ],
              as: "qualifyingTransactions",
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      // Alice: minSpend 100, so only transactions >= 100 qualify (150)
      assert.strictEqual(
        (results[0].qualifyingTransactions as Document[]).length,
        1
      );
      // Bob: minSpend 50, so transactions >= 50 qualify (200)
      assert.strictEqual(
        (results[1].qualifyingTransactions as Document[]).length,
        1
      );
    });

    it("should return all foreign docs when pipeline has no $match", async () => {
      const main = client.db(dbName).collection("lookup_pipe_main");
      const items = client.db(dbName).collection("lookup_pipe_items");

      await main.insertOne({ _id: 1, name: "Test" });
      await items.insertMany([
        { value: 10 },
        { value: 20 },
        { value: 30 },
      ]);

      const results = await main
        .aggregate([
          {
            $lookup: {
              from: "lookup_pipe_items",
              let: {},
              pipeline: [{ $sort: { value: 1 } }],
              as: "allItems",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual((results[0].allItems as Document[]).length, 3);
    });

    it("should work without let clause", async () => {
      const primary = client.db(dbName).collection("lookup_pipe_prim");
      const secondary = client.db(dbName).collection("lookup_pipe_sec");

      await primary.insertOne({ _id: 1 });
      await secondary.insertMany([
        { active: true, value: 1 },
        { active: false, value: 2 },
        { active: true, value: 3 },
      ]);

      const results = await primary
        .aggregate([
          {
            $lookup: {
              from: "lookup_pipe_sec",
              pipeline: [{ $match: { active: true } }],
              as: "activeItems",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual((results[0].activeItems as Document[]).length, 2);
    });

    it("should support dotted variable paths in let", async () => {
      const orders = client.db(dbName).collection("lookup_dotted_orders");
      const products = client.db(dbName).collection("lookup_dotted_products");

      await orders.insertOne({
        _id: 1,
        customer: { name: "Alice", tier: "gold" },
        items: ["prod1", "prod2"],
      });

      await products.insertMany([
        { sku: "prod1", name: "Widget", discount: { gold: 20, silver: 10 } },
        { sku: "prod2", name: "Gadget", discount: { gold: 15, silver: 5 } },
        { sku: "prod3", name: "Other", discount: { gold: 5, silver: 0 } },
      ]);

      const results = await orders
        .aggregate([
          {
            $lookup: {
              from: "lookup_dotted_products",
              let: { customerTier: "$customer.tier" },
              pipeline: [
                {
                  $project: {
                    sku: 1,
                    name: 1,
                    applicableDiscount: {
                      $cond: {
                        if: { $eq: ["$$customerTier", "gold"] },
                        then: "$discount.gold",
                        else: "$discount.silver",
                      },
                    },
                    _id: 0,
                  },
                },
              ],
              as: "productInfo",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      const products_result = results[0].productInfo as Document[];
      assert.strictEqual(products_result.length, 3);
      // Check that gold discounts were applied
      const widget = products_result.find((p) => p.sku === "prod1");
      assert.strictEqual(widget?.applicableDiscount, 20);
    });
  });

  // ==================== $out Stage ====================

  describe("$out Stage", () => {
    it("should write results to collection", async () => {
      const source = client.db(dbName).collection("out_source");
      await source.insertMany([
        { status: "active", value: 10 },
        { status: "active", value: 20 },
        { status: "inactive", value: 30 },
      ]);

      await source
        .aggregate([
          { $match: { status: "active" } },
          { $out: "out_target" },
        ])
        .toArray();

      const target = client.db(dbName).collection("out_target");
      const targetDocs = await target.find({}).toArray();

      assert.strictEqual(targetDocs.length, 2);
      assert.ok(targetDocs.every((d) => d.status === "active"));
    });

    it("should replace existing collection", async () => {
      const source = client.db(dbName).collection("out_source2");
      const target = client.db(dbName).collection("out_target2");

      // Pre-populate target
      await target.insertMany([
        { old: "data1" },
        { old: "data2" },
      ]);

      // Source data
      await source.insertMany([
        { new: "data1" },
      ]);

      await source
        .aggregate([{ $out: "out_target2" }])
        .toArray();

      const targetDocs = await target.find({}).toArray();
      assert.strictEqual(targetDocs.length, 1);
      assert.strictEqual(targetDocs[0].new, "data1");
      assert.strictEqual(targetDocs[0].old, undefined);
    });

    it("should return empty array", async () => {
      const source = client.db(dbName).collection("out_source3");
      await source.insertOne({ value: 1 });

      const results = await source
        .aggregate([{ $out: "out_target3" }])
        .toArray();

      // $out returns empty array, results go to collection
      assert.deepStrictEqual(results, []);
    });
  });

  // ==================== Combined Pipeline Tests ====================

  describe("Combined Pipeline Tests", () => {
    it("should execute $match -> $group -> $sort", async () => {
      const collection = client.db(dbName).collection("combo_mgs");
      await collection.insertMany([
        { category: "A", status: "active", value: 10 },
        { category: "B", status: "active", value: 20 },
        { category: "A", status: "active", value: 30 },
        { category: "A", status: "inactive", value: 40 },
      ]);

      const results = await collection
        .aggregate([
          { $match: { status: "active" } },
          { $group: { _id: "$category", total: { $sum: "$value" } } },
          { $sort: { total: -1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0]._id, "A");
      assert.strictEqual(results[0].total, 40);
      assert.strictEqual(results[1]._id, "B");
      assert.strictEqual(results[1].total, 20);
    });

    it("should execute $unwind -> $group (count per array element)", async () => {
      const collection = client.db(dbName).collection("combo_ug");
      await collection.insertMany([
        { name: "Doc1", tags: ["a", "b", "c"] },
        { name: "Doc2", tags: ["a", "b"] },
        { name: "Doc3", tags: ["a"] },
      ]);

      const results = await collection
        .aggregate([
          { $unwind: "$tags" },
          { $group: { _id: "$tags", count: { $sum: 1 } } },
          { $sort: { count: -1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0]._id, "a");
      assert.strictEqual(results[0].count, 3);
      assert.strictEqual(results[1]._id, "b");
      assert.strictEqual(results[1].count, 2);
    });

    it("should execute $lookup -> $unwind (flatten joins)", async () => {
      const posts = client.db(dbName).collection("combo_posts");
      const authors = client.db(dbName).collection("combo_authors");

      await authors.insertMany([
        { _id: "a1" as unknown, name: "Alice" },
        { _id: "a2" as unknown, name: "Bob" },
      ]);

      await posts.insertMany([
        { title: "Post 1", authorId: "a1" },
        { title: "Post 2", authorId: "a1" },
      ]);

      const results = await posts
        .aggregate([
          {
            $lookup: {
              from: "combo_authors",
              localField: "authorId",
              foreignField: "_id",
              as: "author",
            },
          },
          { $unwind: "$author" },
          { $project: { title: 1, authorName: "$author.name", _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.ok(results.every((r) => r.authorName === "Alice"));
    });

    it("should handle complex multi-stage analytics pipeline", async () => {
      const sales = client.db(dbName).collection("combo_sales");
      await sales.insertMany([
        { product: "Widget", region: "North", amount: 100, date: new Date("2024-01-15") },
        { product: "Widget", region: "South", amount: 150, date: new Date("2024-01-20") },
        { product: "Gadget", region: "North", amount: 200, date: new Date("2024-02-10") },
        { product: "Widget", region: "North", amount: 120, date: new Date("2024-02-15") },
      ]);

      const results = await sales
        .aggregate([
          { $match: { product: "Widget" } },
          {
            $group: {
              _id: "$region",
              totalSales: { $sum: "$amount" },
              avgSale: { $avg: "$amount" },
              count: { $sum: 1 },
            },
          },
          { $addFields: { region: "$_id" } },
          { $project: { _id: 0, region: 1, totalSales: 1, avgSale: 1, count: 1 } },
          { $sort: { totalSales: -1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      // North: 100 + 120 = 220
      // South: 150
      assert.strictEqual(results[0].region, "North");
      assert.strictEqual(results[0].totalSales, 220);
      assert.strictEqual(results[0].count, 2);
    });
  });

  // ==================== $merge Stage ====================

  describe("$merge Stage", () => {
    it("should insert documents when no match exists (whenNotMatched: insert)", async () => {
      const source = client.db(dbName).collection("merge_source1");
      const target = client.db(dbName).collection("merge_target1");

      await source.insertMany([
        { _id: 1, name: "Alice", score: 100 },
        { _id: 2, name: "Bob", score: 90 },
      ]);

      await source
        .aggregate([
          {
            $merge: {
              into: "merge_target1",
              whenMatched: "replace",
              whenNotMatched: "insert",
            },
          },
        ])
        .toArray();

      const targetDocs = await target.find({}).toArray();
      targetDocs.sort((a, b) => (a._id as number) - (b._id as number));

      assert.strictEqual(targetDocs.length, 2);
      assert.strictEqual(targetDocs[0].name, "Alice");
      assert.strictEqual(targetDocs[1].name, "Bob");
    });

    it("should replace matching documents (whenMatched: replace)", async () => {
      const source = client.db(dbName).collection("merge_source2");
      const target = client.db(dbName).collection("merge_target2");

      // Pre-populate target with old data
      await target.insertMany([
        { _id: 1, name: "Old Alice", score: 50 },
        { _id: 3, name: "Charlie", score: 80 },
      ]);

      // Source with updated data
      await source.insertMany([
        { _id: 1, name: "New Alice", score: 100 },
        { _id: 2, name: "Bob", score: 90 },
      ]);

      await source
        .aggregate([
          {
            $merge: {
              into: "merge_target2",
              whenMatched: "replace",
              whenNotMatched: "insert",
            },
          },
        ])
        .toArray();

      const targetDocs = await target.find({}).toArray();
      targetDocs.sort((a, b) => (a._id as number) - (b._id as number));

      assert.strictEqual(targetDocs.length, 3);
      assert.strictEqual(targetDocs[0].name, "New Alice"); // Replaced
      assert.strictEqual(targetDocs[1].name, "Bob"); // Inserted
      assert.strictEqual(targetDocs[2].name, "Charlie"); // Unchanged
    });

    it("should keep existing documents (whenMatched: keepExisting)", async () => {
      const source = client.db(dbName).collection("merge_source3");
      const target = client.db(dbName).collection("merge_target3");

      // Pre-populate target
      await target.insertMany([
        { _id: 1, name: "Original Alice", score: 50 },
      ]);

      // Source with different data for same _id
      await source.insertMany([
        { _id: 1, name: "New Alice", score: 100 },
        { _id: 2, name: "Bob", score: 90 },
      ]);

      await source
        .aggregate([
          {
            $merge: {
              into: "merge_target3",
              whenMatched: "keepExisting",
              whenNotMatched: "insert",
            },
          },
        ])
        .toArray();

      const targetDocs = await target.find({}).toArray();
      targetDocs.sort((a, b) => (a._id as number) - (b._id as number));

      assert.strictEqual(targetDocs.length, 2);
      assert.strictEqual(targetDocs[0].name, "Original Alice"); // Kept
      assert.strictEqual(targetDocs[1].name, "Bob"); // Inserted
    });

    it("should merge fields (whenMatched: merge)", async () => {
      const source = client.db(dbName).collection("merge_source4");
      const target = client.db(dbName).collection("merge_target4");

      // Pre-populate target with partial data
      await target.insertMany([
        { _id: 1, name: "Alice", extra: "preserved" },
      ]);

      // Source with additional/updated data
      await source.insertMany([
        { _id: 1, score: 100, status: "active" },
      ]);

      await source
        .aggregate([
          {
            $merge: {
              into: "merge_target4",
              whenMatched: "merge",
              whenNotMatched: "insert",
            },
          },
        ])
        .toArray();

      const doc = await target.findOne({ _id: 1 });
      assert.ok(doc);
      assert.strictEqual(doc.name, "Alice"); // Preserved from target
      assert.strictEqual(doc.extra, "preserved"); // Preserved from target
      assert.strictEqual(doc.score, 100); // Added from source
      assert.strictEqual(doc.status, "active"); // Added from source
    });

    it("should discard non-matching documents (whenNotMatched: discard)", async () => {
      const source = client.db(dbName).collection("merge_source5");
      const target = client.db(dbName).collection("merge_target5");

      // Pre-populate target
      await target.insertMany([
        { _id: 1, name: "Alice" },
      ]);

      // Source with mix of matching and non-matching
      await source.insertMany([
        { _id: 1, name: "Updated Alice", score: 100 },
        { _id: 2, name: "Bob", score: 90 }, // No match - should be discarded
      ]);

      await source
        .aggregate([
          {
            $merge: {
              into: "merge_target5",
              whenMatched: "replace",
              whenNotMatched: "discard",
            },
          },
        ])
        .toArray();

      const targetDocs = await target.find({}).toArray();

      assert.strictEqual(targetDocs.length, 1);
      assert.strictEqual(targetDocs[0].name, "Updated Alice");
      assert.strictEqual(targetDocs[0].score, 100);
    });

    it("should match on custom field (on option)", async () => {
      const source = client.db(dbName).collection("merge_source6");
      const target = client.db(dbName).collection("merge_target6");

      // MongoDB requires a unique index on the 'on' field
      await target.createIndex({ uniqueId: 1 }, { unique: true });

      // Target with unique userId
      await target.insertMany([
        { _id: 100, uniqueId: "user1", name: "Old Name" },
      ]);

      // Source matching on uniqueId - use $project to remove _id before merging
      await source.insertMany([
        { _id: 999, uniqueId: "user1", name: "New Name", score: 100 },
        { _id: 888, uniqueId: "user2", name: "Bob", score: 90 },
      ]);

      await source
        .aggregate([
          { $project: { _id: 0, uniqueId: 1, name: 1, score: 1 } },
          {
            $merge: {
              into: "merge_target6",
              on: "uniqueId",
              whenMatched: "merge",
              whenNotMatched: "insert",
            },
          },
        ])
        .toArray();

      const targetDocs = await target.find({}).toArray();
      targetDocs.sort((a, b) =>
        String(a.uniqueId).localeCompare(String(b.uniqueId))
      );

      assert.strictEqual(targetDocs.length, 2);
      // First doc should be updated (matched on uniqueId), preserving _id
      assert.strictEqual(targetDocs[0].uniqueId, "user1");
      assert.strictEqual(targetDocs[0].name, "New Name");
      assert.strictEqual((targetDocs[0] as { _id: number })._id, 100); // Original _id preserved
      // Second doc should be inserted
      assert.strictEqual(targetDocs[1].uniqueId, "user2");
    });

    it("should return empty array like $out", async () => {
      const source = client.db(dbName).collection("merge_source7");
      await source.insertOne({ value: 1 });

      const results = await source
        .aggregate([
          {
            $merge: {
              into: "merge_target7",
              whenMatched: "replace",
              whenNotMatched: "insert",
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(results, []);
    });

    it("should fail when whenMatched is 'fail' and match exists", async () => {
      const source = client.db(dbName).collection("merge_fail_source");
      const target = client.db(dbName).collection("merge_fail_target");

      await target.insertOne({ _id: 1, name: "Existing" });
      await source.insertOne({ _id: 1, name: "New" });

      await assert.rejects(
        async () => {
          await source
            .aggregate([
              {
                $merge: {
                  into: "merge_fail_target",
                  whenMatched: "fail",
                  whenNotMatched: "insert",
                },
              },
            ])
            .toArray();
        },
        /\$merge.*match/i // Matches both MangoDB and MongoDB error formats
      );
    });

    it("should fail when whenNotMatched is 'fail' and no match exists", async () => {
      const source = client.db(dbName).collection("merge_fail_source2");
      const target = client.db(dbName).collection("merge_fail_target2");

      // Empty target - no matches possible
      await source.insertOne({ _id: 1, name: "New" });

      await assert.rejects(
        async () => {
          await source
            .aggregate([
              {
                $merge: {
                  into: "merge_fail_target2",
                  whenMatched: "replace",
                  whenNotMatched: "fail",
                },
              },
            ])
            .toArray();
        },
        /\$merge.*match/i // Matches both MangoDB and MongoDB error formats
      );
    });

    it("should use $$new in whenMatched pipeline", async () => {
      const source = client.db(dbName).collection("merge_new_source");
      const target = client.db(dbName).collection("merge_new_target");

      await target.insertOne({ _id: 1, value: 10, originalValue: 10 });
      await source.insertOne({ _id: 1, value: 25 });

      await source
        .aggregate([
          {
            $merge: {
              into: "merge_new_target",
              whenMatched: [
                {
                  $set: {
                    value: "$$new.value",
                    updated: true,
                  },
                },
              ],
              whenNotMatched: "insert",
            },
          },
        ])
        .toArray();

      const doc = await target.findOne({ _id: 1 });
      assert.ok(doc);
      assert.strictEqual(doc.value, 25); // Updated from $$new
      assert.strictEqual(doc.originalValue, 10); // Preserved
      assert.strictEqual(doc.updated, true); // Added
    });
  });
});
