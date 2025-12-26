/**
 * Phase C: Positional Update Operators Tests
 *
 * Tests for $, $[], and $[identifier] positional update operators.
 *
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "./test-harness.ts";

describe(`Positional Update Operators (${getTestModeName()})`, () => {
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

  describe("$[] update all elements operator", () => {
    it("should update all array elements with $set", async () => {
      const collection = client.db(dbName).collection("pos_all_set");
      await collection.insertOne({
        _id: 1,
        grades: [85, 90, 78],
      });

      await collection.updateOne({ _id: 1 }, { $set: { "grades.$[]": 100 } });

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      assert.deepStrictEqual(doc.grades, [100, 100, 100]);
    });

    it("should increment all array elements with $inc", async () => {
      const collection = client.db(dbName).collection("pos_all_inc");
      await collection.insertOne({
        _id: 1,
        scores: [10, 20, 30],
      });

      await collection.updateOne({ _id: 1 }, { $inc: { "scores.$[]": 5 } });

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      assert.deepStrictEqual(doc.scores, [15, 25, 35]);
    });

    it("should update nested field in all array elements", async () => {
      const collection = client.db(dbName).collection("pos_all_nested");
      await collection.insertOne({
        _id: 1,
        items: [
          { name: "a", status: "pending" },
          { name: "b", status: "pending" },
          { name: "c", status: "pending" },
        ],
      });

      await collection.updateOne(
        { _id: 1 },
        { $set: { "items.$[].status": "complete" } }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      const items = doc.items as Array<{ name: string; status: string }>;
      assert.strictEqual(items[0].status, "complete");
      assert.strictEqual(items[1].status, "complete");
      assert.strictEqual(items[2].status, "complete");
    });

    it("should multiply all array elements with $mul", async () => {
      const collection = client.db(dbName).collection("pos_all_mul");
      await collection.insertOne({
        _id: 1,
        prices: [10, 20, 30],
      });

      await collection.updateOne({ _id: 1 }, { $mul: { "prices.$[]": 2 } });

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      assert.deepStrictEqual(doc.prices, [20, 40, 60]);
    });
  });

  describe("$[identifier] filtered positional operator", () => {
    it("should update only matching elements with simple filter", async () => {
      const collection = client.db(dbName).collection("pos_filter_simple");
      await collection.insertOne({
        _id: 1,
        grades: [85, 60, 90, 55, 78],
      });

      await collection.updateOne(
        { _id: 1 },
        { $set: { "grades.$[elem]": 100 } },
        { arrayFilters: [{ elem: { $lt: 70 } }] }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      // Only elements < 70 (60, 55) should be updated to 100
      assert.deepStrictEqual(doc.grades, [85, 100, 90, 100, 78]);
    });

    it("should update nested field in matching elements", async () => {
      const collection = client.db(dbName).collection("pos_filter_nested");
      await collection.insertOne({
        _id: 1,
        items: [
          { name: "a", qty: 5, status: "pending" },
          { name: "b", qty: 15, status: "pending" },
          { name: "c", qty: 3, status: "pending" },
        ],
      });

      await collection.updateOne(
        { _id: 1 },
        { $set: { "items.$[item].status": "low_stock" } },
        { arrayFilters: [{ "item.qty": { $lt: 10 } }] }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      const items = doc.items as Array<{
        name: string;
        qty: number;
        status: string;
      }>;
      assert.strictEqual(items[0].status, "low_stock"); // qty 5 < 10
      assert.strictEqual(items[1].status, "pending"); // qty 15 >= 10
      assert.strictEqual(items[2].status, "low_stock"); // qty 3 < 10
    });

    it("should support multiple array filters", async () => {
      const collection = client.db(dbName).collection("pos_filter_multi");
      await collection.insertOne({
        _id: 1,
        matrix: [
          [1, 2, 3],
          [4, 5, 6],
          [7, 8, 9],
        ],
      });

      // Update inner array elements > 5 to 0
      await collection.updateOne(
        { _id: 1 },
        { $set: { "matrix.$[].$[val]": 0 } },
        { arrayFilters: [{ val: { $gt: 5 } }] }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      assert.deepStrictEqual(doc.matrix, [
        [1, 2, 3],
        [4, 5, 0],
        [0, 0, 0],
      ]);
    });

    it("should increment matching elements", async () => {
      const collection = client.db(dbName).collection("pos_filter_inc");
      await collection.insertOne({
        _id: 1,
        numbers: [10, 20, 30, 40],
      });

      await collection.updateOne(
        { _id: 1 },
        { $inc: { "numbers.$[n]": 100 } },
        { arrayFilters: [{ n: { $gte: 25 } }] }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      assert.deepStrictEqual(doc.numbers, [10, 20, 130, 140]);
    });

    it("should handle equality filter", async () => {
      const collection = client.db(dbName).collection("pos_filter_eq");
      await collection.insertOne({
        _id: 1,
        tags: ["red", "blue", "red", "green"],
      });

      await collection.updateOne(
        { _id: 1 },
        { $set: { "tags.$[t]": "RED" } },
        { arrayFilters: [{ t: "red" }] }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      assert.deepStrictEqual(doc.tags, ["RED", "blue", "RED", "green"]);
    });
  });

  describe("$ positional operator (first match)", () => {
    it("should update first matching array element", async () => {
      const collection = client.db(dbName).collection("pos_first_basic");
      await collection.insertOne({
        _id: 1,
        items: [
          { name: "apple", qty: 10 },
          { name: "banana", qty: 5 },
          { name: "apple", qty: 8 },
        ],
      });

      // Query matches items where name is "apple"
      await collection.updateOne(
        { _id: 1, "items.name": "apple" },
        { $set: { "items.$.qty": 50 } }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      const items = doc.items as Array<{ name: string; qty: number }>;
      assert.strictEqual(items[0].qty, 50); // First apple updated
      assert.strictEqual(items[1].qty, 5); // banana unchanged
      assert.strictEqual(items[2].qty, 8); // Second apple unchanged
    });

    it("should update first matching element with $inc", async () => {
      const collection = client.db(dbName).collection("pos_first_inc");
      await collection.insertOne({
        _id: 1,
        scores: [
          { student: "A", score: 80 },
          { student: "B", score: 90 },
          { student: "A", score: 75 },
        ],
      });

      await collection.updateOne(
        { _id: 1, "scores.student": "A" },
        { $inc: { "scores.$.score": 10 } }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      const scores = doc.scores as Array<{ student: string; score: number }>;
      assert.strictEqual(scores[0].score, 90); // First A: 80 + 10
      assert.strictEqual(scores[1].score, 90); // B unchanged
      assert.strictEqual(scores[2].score, 75); // Second A unchanged
    });

    it("should work with comparison operators via $elemMatch in query", async () => {
      const collection = client.db(dbName).collection("pos_first_compare");
      await collection.insertOne({
        _id: 1,
        values: [5, 15, 25, 35],
      });

      // Match first element > 10 using $elemMatch for positional tracking
      await collection.updateOne(
        { _id: 1, values: { $elemMatch: { $gt: 10 } } },
        { $set: { "values.$": 999 } }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      // First element matching > 10 is 15 at index 1
      assert.deepStrictEqual(doc.values, [5, 999, 25, 35]);
    });

    it("should work with $elemMatch in query", async () => {
      const collection = client.db(dbName).collection("pos_first_elemmatch");
      await collection.insertOne({
        _id: 1,
        orders: [
          { product: "A", status: "shipped" },
          { product: "B", status: "pending" },
          { product: "C", status: "pending" },
        ],
      });

      await collection.updateOne(
        { _id: 1, orders: { $elemMatch: { status: "pending" } } },
        { $set: { "orders.$.status": "processing" } }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      const orders = doc.orders as Array<{ product: string; status: string }>;
      assert.strictEqual(orders[0].status, "shipped"); // unchanged
      assert.strictEqual(orders[1].status, "processing"); // first pending updated
      assert.strictEqual(orders[2].status, "pending"); // second pending unchanged
    });

    it("should error when $ used without matching array in query", async () => {
      const collection = client.db(dbName).collection("pos_first_error");
      await collection.insertOne({
        _id: 1,
        items: [1, 2, 3],
      });

      await assert.rejects(
        async () => {
          await collection.updateOne(
            { _id: 1 }, // No array condition in query
            { $set: { "items.$": 100 } }
          );
        },
        /positional operator/i
      );
    });
  });

  describe("Combined positional operators", () => {
    it("should support $[] with nested arrays", async () => {
      const collection = client.db(dbName).collection("pos_combined_nested");
      await collection.insertOne({
        _id: 1,
        groups: [
          { members: [1, 2, 3] },
          { members: [4, 5, 6] },
        ],
      });

      // Double all member values
      await collection.updateOne(
        { _id: 1 },
        { $mul: { "groups.$[].members.$[]": 2 } }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      const groups = doc.groups as Array<{ members: number[] }>;
      assert.deepStrictEqual(groups[0].members, [2, 4, 6]);
      assert.deepStrictEqual(groups[1].members, [8, 10, 12]);
    });

    it("should support unset with $[]", async () => {
      const collection = client.db(dbName).collection("pos_combined_unset");
      await collection.insertOne({
        _id: 1,
        items: [
          { name: "a", temp: "x" },
          { name: "b", temp: "y" },
        ],
      });

      await collection.updateOne(
        { _id: 1 },
        { $unset: { "items.$[].temp": "" } }
      );

      const doc = await collection.findOne({ _id: 1 });
      assert.ok(doc);
      const items = doc.items as Array<{ name: string; temp?: string }>;
      assert.strictEqual(items[0].temp, undefined);
      assert.strictEqual(items[1].temp, undefined);
    });
  });
});
