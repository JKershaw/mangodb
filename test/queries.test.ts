/**
 * Phase 2: Basic Query Tests
 *
 * These tests run against both real MongoDB and Mongone to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after, beforeEach } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
  type TestCollection,
} from "./test-harness.ts";

describe(`Basic Query Tests (${getTestModeName()})`, () => {
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

  describe("Equality matching", () => {
    it("should match string values", async () => {
      const collection = client.db(dbName).collection("eq_string");
      await collection.insertMany([
        { name: "Alice" },
        { name: "Bob" },
        { name: "Alice" },
      ]);

      const docs = await collection.find({ name: "Alice" }).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => d.name === "Alice"));
    });

    it("should match number values", async () => {
      const collection = client.db(dbName).collection("eq_number");
      await collection.insertMany([{ age: 25 }, { age: 30 }, { age: 25 }]);

      const docs = await collection.find({ age: 25 }).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => d.age === 25));
    });

    it("should match boolean values", async () => {
      const collection = client.db(dbName).collection("eq_boolean");
      await collection.insertMany([
        { active: true },
        { active: false },
        { active: true },
      ]);

      const docs = await collection.find({ active: true }).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should match null values", async () => {
      const collection = client.db(dbName).collection("eq_null");
      await collection.insertMany([
        { value: null },
        { value: "something" },
        { other: "field" }, // value is missing
      ]);

      // MongoDB matches both null values AND missing fields when querying for null
      const docs = await collection.find({ value: null }).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should match multiple fields", async () => {
      const collection = client.db(dbName).collection("eq_multi");
      await collection.insertMany([
        { name: "Alice", age: 30 },
        { name: "Alice", age: 25 },
        { name: "Bob", age: 30 },
      ]);

      const docs = await collection.find({ name: "Alice", age: 30 }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, "Alice");
      assert.strictEqual(docs[0].age, 30);
    });
  });

  describe("Dot notation", () => {
    it("should access nested fields", async () => {
      const collection = client.db(dbName).collection("dot_nested");
      await collection.insertMany([
        { user: { name: "Alice", age: 30 } },
        { user: { name: "Bob", age: 25 } },
      ]);

      const docs = await collection.find({ "user.name": "Alice" }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual((docs[0].user as { name: string }).name, "Alice");
    });

    it("should access deeply nested fields", async () => {
      const collection = client.db(dbName).collection("dot_deep");
      await collection.insertMany([
        { a: { b: { c: { d: 1 } } } },
        { a: { b: { c: { d: 2 } } } },
      ]);

      const docs = await collection.find({ "a.b.c.d": 1 }).toArray();

      assert.strictEqual(docs.length, 1);
    });

    it("should return empty when nested path doesn't exist", async () => {
      const collection = client.db(dbName).collection("dot_missing");
      await collection.insertMany([{ a: { b: 1 } }, { a: { c: 2 } }]);

      const docs = await collection.find({ "a.x.y": 1 }).toArray();

      assert.strictEqual(docs.length, 0);
    });

    it("should access array elements by index", async () => {
      const collection = client.db(dbName).collection("dot_array_index");
      await collection.insertMany([
        { items: ["a", "b", "c"] },
        { items: ["x", "y", "z"] },
      ]);

      const docs = await collection.find({ "items.0": "a" }).toArray();

      assert.strictEqual(docs.length, 1);
    });
  });

  describe("$eq operator", () => {
    it("should match with explicit $eq", async () => {
      const collection = client.db(dbName).collection("op_eq");
      await collection.insertMany([{ value: 10 }, { value: 20 }, { value: 10 }]);

      const docs = await collection.find({ value: { $eq: 10 } }).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should work with $eq on strings", async () => {
      const collection = client.db(dbName).collection("op_eq_string");
      await collection.insertMany([
        { name: "Alice" },
        { name: "Bob" },
      ]);

      const docs = await collection.find({ name: { $eq: "Alice" } }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, "Alice");
    });
  });

  describe("$ne operator", () => {
    it("should match documents where field is not equal", async () => {
      const collection = client.db(dbName).collection("op_ne");
      await collection.insertMany([{ value: 10 }, { value: 20 }, { value: 30 }]);

      const docs = await collection.find({ value: { $ne: 10 } }).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => d.value !== 10));
    });

    it("should include documents where field is missing", async () => {
      const collection = client.db(dbName).collection("op_ne_missing");
      await collection.insertMany([
        { value: 10 },
        { other: "field" }, // value is missing
      ]);

      const docs = await collection.find({ value: { $ne: 10 } }).toArray();

      // Missing field is not equal to 10
      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].other, "field");
    });
  });

  describe("$gt operator", () => {
    it("should match values greater than", async () => {
      const collection = client.db(dbName).collection("op_gt");
      await collection.insertMany([
        { value: 5 },
        { value: 10 },
        { value: 15 },
        { value: 20 },
      ]);

      const docs = await collection.find({ value: { $gt: 10 } }).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => (d.value as number) > 10));
    });

    it("should work with strings (lexicographic)", async () => {
      const collection = client.db(dbName).collection("op_gt_string");
      await collection.insertMany([
        { name: "apple" },
        { name: "banana" },
        { name: "cherry" },
      ]);

      const docs = await collection.find({ name: { $gt: "banana" } }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, "cherry");
    });

    it("should work with dates", async () => {
      const collection = client.db(dbName).collection("op_gt_date");
      const date1 = new Date("2024-01-01");
      const date2 = new Date("2024-06-01");
      const date3 = new Date("2024-12-01");

      await collection.insertMany([
        { date: date1 },
        { date: date2 },
        { date: date3 },
      ]);

      const docs = await collection
        .find({ date: { $gt: new Date("2024-06-01") } })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });
  });

  describe("$gte operator", () => {
    it("should match values greater than or equal", async () => {
      const collection = client.db(dbName).collection("op_gte");
      await collection.insertMany([
        { value: 5 },
        { value: 10 },
        { value: 15 },
        { value: 20 },
      ]);

      const docs = await collection.find({ value: { $gte: 10 } }).toArray();

      assert.strictEqual(docs.length, 3);
      assert.ok(docs.every((d) => (d.value as number) >= 10));
    });
  });

  describe("$lt operator", () => {
    it("should match values less than", async () => {
      const collection = client.db(dbName).collection("op_lt");
      await collection.insertMany([
        { value: 5 },
        { value: 10 },
        { value: 15 },
        { value: 20 },
      ]);

      const docs = await collection.find({ value: { $lt: 15 } }).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => (d.value as number) < 15));
    });
  });

  describe("$lte operator", () => {
    it("should match values less than or equal", async () => {
      const collection = client.db(dbName).collection("op_lte");
      await collection.insertMany([
        { value: 5 },
        { value: 10 },
        { value: 15 },
        { value: 20 },
      ]);

      const docs = await collection.find({ value: { $lte: 15 } }).toArray();

      assert.strictEqual(docs.length, 3);
      assert.ok(docs.every((d) => (d.value as number) <= 15));
    });
  });

  describe("Comparison operator combinations", () => {
    it("should support range queries with $gt and $lt", async () => {
      const collection = client.db(dbName).collection("op_range");
      await collection.insertMany([
        { value: 5 },
        { value: 10 },
        { value: 15 },
        { value: 20 },
        { value: 25 },
      ]);

      const docs = await collection
        .find({ value: { $gt: 5, $lt: 20 } })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.value === 10));
      assert.ok(docs.some((d) => d.value === 15));
    });

    it("should support inclusive range with $gte and $lte", async () => {
      const collection = client.db(dbName).collection("op_range_inclusive");
      await collection.insertMany([
        { value: 5 },
        { value: 10 },
        { value: 15 },
        { value: 20 },
        { value: 25 },
      ]);

      const docs = await collection
        .find({ value: { $gte: 10, $lte: 20 } })
        .toArray();

      assert.strictEqual(docs.length, 3);
    });
  });

  describe("$in operator", () => {
    it("should match values in array", async () => {
      const collection = client.db(dbName).collection("op_in");
      await collection.insertMany([
        { color: "red" },
        { color: "blue" },
        { color: "green" },
        { color: "yellow" },
      ]);

      const docs = await collection
        .find({ color: { $in: ["red", "blue"] } })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.color === "red"));
      assert.ok(docs.some((d) => d.color === "blue"));
    });

    it("should match numbers in array", async () => {
      const collection = client.db(dbName).collection("op_in_number");
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      const docs = await collection
        .find({ value: { $in: [2, 4] } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should return empty when no values match", async () => {
      const collection = client.db(dbName).collection("op_in_empty");
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      const docs = await collection
        .find({ value: { $in: [10, 20, 30] } })
        .toArray();

      assert.strictEqual(docs.length, 0);
    });

    it("should match when document field is array and contains any $in value", async () => {
      const collection = client.db(dbName).collection("op_in_array_field");
      await collection.insertMany([
        { tags: ["a", "b", "c"] },
        { tags: ["d", "e", "f"] },
        { tags: ["a", "x", "y"] },
      ]);

      // Should match if the array field contains any of the $in values
      const docs = await collection
        .find({ tags: { $in: ["a", "d"] } })
        .toArray();

      assert.strictEqual(docs.length, 3);
    });
  });

  describe("$nin operator", () => {
    it("should match values not in array", async () => {
      const collection = client.db(dbName).collection("op_nin");
      await collection.insertMany([
        { color: "red" },
        { color: "blue" },
        { color: "green" },
        { color: "yellow" },
      ]);

      const docs = await collection
        .find({ color: { $nin: ["red", "blue"] } })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => d.color !== "red" && d.color !== "blue"));
    });

    it("should include documents where field is missing", async () => {
      const collection = client.db(dbName).collection("op_nin_missing");
      await collection.insertMany([
        { color: "red" },
        { other: "field" }, // color is missing
      ]);

      const docs = await collection
        .find({ color: { $nin: ["red", "blue"] } })
        .toArray();

      // Missing field is not in the array
      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].other, "field");
    });
  });

  describe("Array field matching", () => {
    it("should match when querying array field with single value", async () => {
      const collection = client.db(dbName).collection("array_single");
      await collection.insertMany([
        { tags: ["red", "blue"] },
        { tags: ["green", "yellow"] },
        { tags: ["red", "green"] },
      ]);

      // MongoDB matches if any array element equals the value
      const docs = await collection.find({ tags: "red" }).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should match exact array", async () => {
      const collection = client.db(dbName).collection("array_exact");
      await collection.insertMany([
        { tags: ["a", "b"] },
        { tags: ["a", "b", "c"] },
        { tags: ["a"] },
      ]);

      // Exact array match
      const docs = await collection.find({ tags: ["a", "b"] }).toArray();

      assert.strictEqual(docs.length, 1);
    });
  });

  describe("Mixed operators with dot notation", () => {
    it("should apply operators to nested fields", async () => {
      const collection = client.db(dbName).collection("mixed_nested");
      await collection.insertMany([
        { user: { score: 50 } },
        { user: { score: 75 } },
        { user: { score: 100 } },
      ]);

      const docs = await collection
        .find({ "user.score": { $gte: 75 } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should combine multiple nested field queries", async () => {
      const collection = client.db(dbName).collection("mixed_multi_nested");
      await collection.insertMany([
        { user: { name: "Alice", age: 30 } },
        { user: { name: "Bob", age: 25 } },
        { user: { name: "Charlie", age: 35 } },
      ]);

      const docs = await collection
        .find({ "user.name": "Alice", "user.age": { $gte: 25 } })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(
        (docs[0].user as { name: string }).name,
        "Alice"
      );
    });
  });

  describe("Array element dot notation (querying into arrays)", () => {
    it("should match nested field in any array element", async () => {
      const collection = client.db(dbName).collection("array_elem_dot");
      await collection.insertMany([
        { items: [{ name: "Alice" }, { name: "Bob" }] },
        { items: [{ name: "Charlie" }, { name: "Dave" }] },
        { items: [{ name: "Eve" }] },
      ]);

      // Should match if ANY array element has name: "Alice"
      const docs = await collection.find({ "items.name": "Alice" }).toArray();

      assert.strictEqual(docs.length, 1);
    });

    it("should match deeply nested field in array elements", async () => {
      const collection = client.db(dbName).collection("array_elem_deep");
      await collection.insertMany([
        { orders: [{ product: { sku: "A1" } }, { product: { sku: "B2" } }] },
        { orders: [{ product: { sku: "C3" } }] },
      ]);

      const docs = await collection
        .find({ "orders.product.sku": "A1" })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });

    it("should work with comparison operators on array element fields", async () => {
      const collection = client.db(dbName).collection("array_elem_cmp");
      await collection.insertMany([
        { scores: [{ value: 50 }, { value: 80 }] },
        { scores: [{ value: 30 }, { value: 40 }] },
        { scores: [{ value: 90 }, { value: 95 }] },
      ]);

      // Should match if ANY score.value >= 80
      const docs = await collection
        .find({ "scores.value": { $gte: 80 } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should handle multiple levels of arrays", async () => {
      const collection = client.db(dbName).collection("array_elem_multi");
      await collection.insertMany([
        { groups: [{ members: [{ name: "Alice" }] }] },
        { groups: [{ members: [{ name: "Bob" }] }] },
      ]);

      const docs = await collection
        .find({ "groups.members.name": "Alice" })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });

    it("should not match when no array element has the nested field value", async () => {
      const collection = client.db(dbName).collection("array_elem_nomatch");
      await collection.insertMany([
        { items: [{ name: "Alice" }, { name: "Bob" }] },
      ]);

      const docs = await collection.find({ "items.name": "Charlie" }).toArray();

      assert.strictEqual(docs.length, 0);
    });
  });

  describe("Boolean comparison operators", () => {
    it("should compare booleans with $gt (false < true)", async () => {
      const collection = client.db(dbName).collection("bool_gt");
      await collection.insertMany([
        { active: false },
        { active: true },
        { active: false },
      ]);

      // In MongoDB, false < true, so $gt: false should match true
      const docs = await collection
        .find({ active: { $gt: false } })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].active, true);
    });

    it("should compare booleans with $lt (false < true)", async () => {
      const collection = client.db(dbName).collection("bool_lt");
      await collection.insertMany([
        { active: false },
        { active: true },
        { active: true },
      ]);

      // $lt: true should match false
      const docs = await collection
        .find({ active: { $lt: true } })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].active, false);
    });

    it("should compare booleans with $gte", async () => {
      const collection = client.db(dbName).collection("bool_gte");
      await collection.insertMany([
        { active: false },
        { active: true },
        { active: true },
      ]);

      // $gte: true should match only true values
      const docs = await collection
        .find({ active: { $gte: true } })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => d.active === true));
    });

    it("should compare booleans with $lte", async () => {
      const collection = client.db(dbName).collection("bool_lte");
      await collection.insertMany([
        { active: false },
        { active: true },
        { active: false },
      ]);

      // $lte: false should match only false values
      const docs = await collection
        .find({ active: { $lte: false } })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => d.active === false));
    });
  });

  describe("Edge cases", () => {
    it("should handle negative array indices (no match)", async () => {
      const collection = client.db(dbName).collection("negative_index");
      await collection.insertMany([
        { items: ["a", "b", "c"] },
      ]);

      // Negative indices should not match anything in MongoDB
      const docs = await collection.find({ "items.-1": "c" }).toArray();

      assert.strictEqual(docs.length, 0);
    });

    it("should handle out-of-bounds array indices (no match)", async () => {
      const collection = client.db(dbName).collection("oob_index");
      await collection.insertMany([
        { items: ["a", "b", "c"] },
      ]);

      const docs = await collection.find({ "items.10": "a" }).toArray();

      assert.strictEqual(docs.length, 0);
    });
  });
});
