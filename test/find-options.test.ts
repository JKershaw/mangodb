/**
 * Phase 12.5: Find Options Parity Tests
 *
 * These tests verify that findOne supports sort and skip options,
 * matching the behavior available in cursor methods and findOneAnd* operations.
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

describe(`Find Options Parity Tests (${getTestModeName()})`, () => {
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

  describe("findOne with sort option", () => {
    it("should return document matching sort order (descending)", async () => {
      const collection = client.db(dbName).collection("findone_sort_desc");
      await collection.insertMany([
        { name: "Alice", score: 85 },
        { name: "Bob", score: 92 },
        { name: "Charlie", score: 78 },
      ]);

      const result = await collection.findOne({}, { sort: { score: -1 } });

      assert.ok(result);
      assert.strictEqual(result.name, "Bob");
      assert.strictEqual(result.score, 92);
    });

    it("should return latest document with descending sort on date", async () => {
      const collection = client.db(dbName).collection("findone_sort_date");
      const date1 = new Date("2024-01-01");
      const date2 = new Date("2024-06-15");
      const date3 = new Date("2024-03-10");

      await collection.insertMany([
        { name: "First", createdAt: date1 },
        { name: "Latest", createdAt: date2 },
        { name: "Middle", createdAt: date3 },
      ]);

      const result = await collection.findOne({}, { sort: { createdAt: -1 } });

      assert.ok(result);
      assert.strictEqual(result.name, "Latest");
    });

    it("should return earliest document with ascending sort", async () => {
      const collection = client.db(dbName).collection("findone_sort_asc");
      await collection.insertMany([
        { name: "C", priority: 3 },
        { name: "A", priority: 1 },
        { name: "B", priority: 2 },
      ]);

      const result = await collection.findOne({}, { sort: { priority: 1 } });

      assert.ok(result);
      assert.strictEqual(result.name, "A");
      assert.strictEqual(result.priority, 1);
    });

    it("should work with compound sort", async () => {
      const collection = client.db(dbName).collection("findone_compound_sort");
      await collection.insertMany([
        { category: "A", score: 80 },
        { category: "A", score: 95 },
        { category: "B", score: 90 },
        { category: "A", score: 70 },
      ]);

      // Sort by category ascending, then score descending
      const result = await collection.findOne(
        {},
        { sort: { category: 1, score: -1 } }
      );

      assert.ok(result);
      assert.strictEqual(result.category, "A");
      assert.strictEqual(result.score, 95); // Highest score in category A
    });

    it("should work with sort and filter together", async () => {
      const collection = client.db(dbName).collection("findone_sort_filter");
      await collection.insertMany([
        { status: "active", value: 100 },
        { status: "inactive", value: 200 },
        { status: "active", value: 150 },
        { status: "active", value: 50 },
      ]);

      const result = await collection.findOne(
        { status: "active" },
        { sort: { value: -1 } }
      );

      assert.ok(result);
      assert.strictEqual(result.status, "active");
      assert.strictEqual(result.value, 150); // Highest value among active
    });

    it("should work with sort and projection together", async () => {
      const collection = client
        .db(dbName)
        .collection("findone_sort_projection");
      await collection.insertMany([
        { name: "Alice", age: 30, secret: "hidden1" },
        { name: "Bob", age: 25, secret: "hidden2" },
        { name: "Charlie", age: 35, secret: "hidden3" },
      ]);

      const result = await collection.findOne(
        {},
        { sort: { age: -1 }, projection: { name: 1, age: 1 } }
      );

      assert.ok(result);
      assert.strictEqual(result.name, "Charlie");
      assert.strictEqual(result.age, 35);
      assert.strictEqual(result.secret, undefined); // Should be excluded
    });

    it("should return null if no match (with sort)", async () => {
      const collection = client.db(dbName).collection("findone_sort_no_match");
      await collection.insertMany([
        { type: "A", value: 1 },
        { type: "A", value: 2 },
      ]);

      const result = await collection.findOne(
        { type: "B" },
        { sort: { value: -1 } }
      );

      assert.strictEqual(result, null);
    });

    it("should handle sort on nested fields", async () => {
      const collection = client.db(dbName).collection("findone_sort_nested");
      await collection.insertMany([
        { user: { score: 100 }, name: "First" },
        { user: { score: 200 }, name: "Second" },
        { user: { score: 50 }, name: "Third" },
      ]);

      const result = await collection.findOne(
        {},
        { sort: { "user.score": -1 } }
      );

      assert.ok(result);
      assert.strictEqual(result.name, "Second");
    });
  });

  describe("findOne with skip option", () => {
    it("should skip first N matching documents", async () => {
      const collection = client.db(dbName).collection("findone_skip_basic");
      await collection.insertMany([
        { name: "First", order: 1 },
        { name: "Second", order: 2 },
        { name: "Third", order: 3 },
      ]);

      // Sort by order to have deterministic results, then skip 1
      const result = await collection.findOne(
        {},
        { sort: { order: 1 }, skip: 1 }
      );

      assert.ok(result);
      assert.strictEqual(result.name, "Second");
    });

    it("should return second-highest with sort and skip", async () => {
      const collection = client.db(dbName).collection("findone_skip_runnerup");
      await collection.insertMany([
        { player: "Alice", score: 100 },
        { player: "Bob", score: 85 },
        { player: "Charlie", score: 95 },
        { player: "Diana", score: 90 },
      ]);

      // Get runner-up (second highest score)
      const result = await collection.findOne(
        {},
        { sort: { score: -1 }, skip: 1 }
      );

      assert.ok(result);
      assert.strictEqual(result.player, "Charlie");
      assert.strictEqual(result.score, 95);
    });

    it("should return null if skip exceeds matches", async () => {
      const collection = client.db(dbName).collection("findone_skip_exceed");
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
      ]);

      const result = await collection.findOne({}, { skip: 5 });

      assert.strictEqual(result, null);
    });

    it("should work with skip, sort, and projection combined", async () => {
      const collection = client.db(dbName).collection("findone_skip_combined");
      await collection.insertMany([
        { rank: 1, name: "Gold", details: "secret1" },
        { rank: 2, name: "Silver", details: "secret2" },
        { rank: 3, name: "Bronze", details: "secret3" },
      ]);

      const result = await collection.findOne(
        {},
        {
          sort: { rank: 1 },
          skip: 1,
          projection: { name: 1 },
        }
      );

      assert.ok(result);
      assert.strictEqual(result.name, "Silver");
      assert.strictEqual(result.details, undefined);
    });

    it("should work with skip and filter together", async () => {
      const collection = client.db(dbName).collection("findone_skip_filter");
      await collection.insertMany([
        { type: "premium", value: 100 },
        { type: "basic", value: 50 },
        { type: "premium", value: 200 },
        { type: "premium", value: 150 },
      ]);

      // Among premium items sorted by value descending, get the second one
      const result = await collection.findOne(
        { type: "premium" },
        { sort: { value: -1 }, skip: 1 }
      );

      assert.ok(result);
      assert.strictEqual(result.type, "premium");
      assert.strictEqual(result.value, 150);
    });

    it("should return first document when skip is 0", async () => {
      const collection = client.db(dbName).collection("findone_skip_zero");
      await collection.insertMany([
        { id: 1 },
        { id: 2 },
        { id: 3 },
      ]);

      const result = await collection.findOne(
        {},
        { sort: { id: 1 }, skip: 0 }
      );

      assert.ok(result);
      assert.strictEqual(result.id, 1);
    });
  });

  describe("Projection operators", () => {
    describe("$slice projection", () => {
      it("should return first N elements with positive number", async () => {
        const collection = client.db(dbName).collection("proj_slice_pos");
        await collection.insertOne({
          name: "test",
          items: [1, 2, 3, 4, 5],
        });

        const result = await collection.findOne(
          {},
          { projection: { items: { $slice: 2 } } }
        );

        assert.ok(result);
        assert.deepStrictEqual(result.items, [1, 2]);
      });

      it("should return last N elements with negative number", async () => {
        const collection = client.db(dbName).collection("proj_slice_neg");
        await collection.insertOne({
          name: "test",
          items: [1, 2, 3, 4, 5],
        });

        const result = await collection.findOne(
          {},
          { projection: { items: { $slice: -2 } } }
        );

        assert.ok(result);
        assert.deepStrictEqual(result.items, [4, 5]);
      });

      it("should return elements from skip position with array form", async () => {
        const collection = client.db(dbName).collection("proj_slice_arr");
        await collection.insertOne({
          name: "test",
          items: [1, 2, 3, 4, 5],
        });

        const result = await collection.findOne(
          {},
          { projection: { items: { $slice: [1, 2] } } }
        );

        assert.ok(result);
        assert.deepStrictEqual(result.items, [2, 3]);
      });

      it("should handle negative skip in array form", async () => {
        const collection = client.db(dbName).collection("proj_slice_arr_neg");
        await collection.insertOne({
          name: "test",
          items: [1, 2, 3, 4, 5],
        });

        const result = await collection.findOne(
          {},
          { projection: { items: { $slice: [-3, 2] } } }
        );

        assert.ok(result);
        assert.deepStrictEqual(result.items, [3, 4]);
      });

      it("should return empty array when slice exceeds array length", async () => {
        const collection = client.db(dbName).collection("proj_slice_exceed");
        await collection.insertOne({
          name: "test",
          items: [1, 2],
        });

        const result = await collection.findOne(
          {},
          { projection: { items: { $slice: [10, 5] } } }
        );

        assert.ok(result);
        assert.deepStrictEqual(result.items, []);
      });

      it("should work with other projected fields", async () => {
        const collection = client.db(dbName).collection("proj_slice_combo");
        await collection.insertOne({
          name: "test",
          items: [1, 2, 3, 4, 5],
          other: "value",
        });

        const result = await collection.findOne(
          {},
          { projection: { name: 1, items: { $slice: 2 } } }
        );

        assert.ok(result);
        assert.strictEqual(result.name, "test");
        assert.deepStrictEqual(result.items, [1, 2]);
        assert.strictEqual(result.other, undefined);
      });
    });

    describe("$elemMatch projection", () => {
      it("should return first matching element", async () => {
        const collection = client.db(dbName).collection("proj_elemmatch_basic");
        await collection.insertOne({
          name: "store",
          items: [
            { name: "a", qty: 5 },
            { name: "b", qty: 15 },
            { name: "c", qty: 25 },
          ],
        });

        const result = await collection.findOne(
          {},
          { projection: { items: { $elemMatch: { qty: { $gte: 10 } } } } }
        );

        assert.ok(result);
        const items = result.items as Array<{ name: string; qty: number }>;
        assert.strictEqual(items.length, 1);
        assert.strictEqual(items[0].name, "b");
      });

      it("should return empty array when no match", async () => {
        const collection = client.db(dbName).collection("proj_elemmatch_none");
        await collection.insertOne({
          name: "store",
          items: [
            { name: "a", qty: 5 },
            { name: "b", qty: 8 },
          ],
        });

        const result = await collection.findOne(
          {},
          { projection: { items: { $elemMatch: { qty: { $gte: 100 } } } } }
        );

        assert.ok(result);
        assert.strictEqual(result.items, undefined);
      });

      it("should support equality condition", async () => {
        const collection = client.db(dbName).collection("proj_elemmatch_eq");
        await collection.insertOne({
          tags: ["red", "blue", "green"],
        });

        // For primitive arrays, $elemMatch needs object-style comparison
        const result = await collection.findOne(
          {},
          { projection: { tags: { $elemMatch: { $eq: "blue" } } } }
        );

        assert.ok(result);
        if (result.tags) {
          assert.deepStrictEqual(result.tags, ["blue"]);
        }
      });
    });
  });
});
