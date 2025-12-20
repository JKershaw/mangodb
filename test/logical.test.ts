/**
 * Phase 5: Logical Operator Tests
 *
 * These tests run against both real MongoDB and Mongone to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "./test-harness.ts";

describe(`Logical Operator Tests (${getTestModeName()})`, () => {
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

  describe("$exists operator", () => {
    it("should match documents where field exists with $exists: true", async () => {
      const collection = client.db(dbName).collection("exists_true");
      await collection.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob" }, // age is missing
        { name: "Charlie", age: 25 },
      ]);

      const docs = await collection.find({ age: { $exists: true } }).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => "age" in d));
    });

    it("should match documents where field does not exist with $exists: false", async () => {
      const collection = client.db(dbName).collection("exists_false");
      await collection.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob" }, // age is missing
        { name: "Charlie", age: 25 },
      ]);

      const docs = await collection.find({ age: { $exists: false } }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, "Bob");
    });

    it("should consider null values as existing", async () => {
      const collection = client.db(dbName).collection("exists_null");
      await collection.insertMany([
        { value: null },
        { value: "something" },
        { other: "field" }, // value is missing
      ]);

      // null IS a value, so the field exists
      const docs = await collection
        .find({ value: { $exists: true } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should work with dot notation for nested fields", async () => {
      const collection = client.db(dbName).collection("exists_nested");
      await collection.insertMany([
        { user: { email: "alice@test.com" } },
        { user: { name: "Bob" } }, // email is missing
        { user: { email: "charlie@test.com", name: "Charlie" } },
      ]);

      const docs = await collection
        .find({ "user.email": { $exists: true } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should handle deeply nested field existence", async () => {
      const collection = client.db(dbName).collection("exists_deep");
      await collection.insertMany([
        { a: { b: { c: 1 } } },
        { a: { b: {} } }, // c is missing
        { a: {} }, // b is missing
      ]);

      const docs = await collection
        .find({ "a.b.c": { $exists: true } })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });

    it("should combine $exists with other operators", async () => {
      const collection = client.db(dbName).collection("exists_combined");
      await collection.insertMany([
        { name: "Alice", score: 90 },
        { name: "Bob", score: 60 },
        { name: "Charlie" }, // score is missing
      ]);

      const docs = await collection
        .find({ score: { $exists: true, $gte: 80 } })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, "Alice");
    });
  });

  describe("$and operator", () => {
    it("should match documents satisfying all conditions", async () => {
      const collection = client.db(dbName).collection("and_basic");
      await collection.insertMany([
        { name: "Alice", age: 30, active: true },
        { name: "Bob", age: 25, active: true },
        { name: "Charlie", age: 30, active: false },
      ]);

      const docs = await collection
        .find({ $and: [{ age: 30 }, { active: true }] })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, "Alice");
    });

    it("should work with comparison operators in conditions", async () => {
      const collection = client.db(dbName).collection("and_comparison");
      await collection.insertMany([
        { value: 5 },
        { value: 15 },
        { value: 25 },
        { value: 35 },
      ]);

      const docs = await collection
        .find({ $and: [{ value: { $gte: 10 } }, { value: { $lte: 30 } }] })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.value === 15));
      assert.ok(docs.some((d) => d.value === 25));
    });

    it("should allow same field with different operators", async () => {
      const collection = client.db(dbName).collection("and_same_field");
      await collection.insertMany([
        { score: 50 },
        { score: 75 },
        { score: 100 },
      ]);

      // This is the main use case for explicit $and - same field with different operators
      const docs = await collection
        .find({ $and: [{ score: { $gt: 60 } }, { score: { $lt: 90 } }] })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].score, 75);
    });

    it("should combine with other field conditions", async () => {
      const collection = client.db(dbName).collection("and_with_fields");
      await collection.insertMany([
        { type: "A", value: 10, status: "active" },
        { type: "A", value: 20, status: "inactive" },
        { type: "B", value: 10, status: "active" },
      ]);

      const docs = await collection
        .find({ type: "A", $and: [{ value: { $gte: 10 } }] })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should handle empty $and array (matches all)", async () => {
      const collection = client.db(dbName).collection("and_empty");
      await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);

      const docs = await collection.find({ $and: [] }).toArray();

      // Empty $and is vacuously true - matches all documents
      assert.strictEqual(docs.length, 3);
    });

    it("should support nested $and with $or", async () => {
      const collection = client.db(dbName).collection("and_nested_or");
      await collection.insertMany([
        { a: 1, b: 1 },
        { a: 1, b: 2 },
        { a: 2, b: 1 },
        { a: 2, b: 2 },
      ]);

      const docs = await collection
        .find({ $and: [{ $or: [{ a: 1 }, { b: 1 }] }, { a: { $lte: 2 } }] })
        .toArray();

      // Should match: {a:1,b:1}, {a:1,b:2}, {a:2,b:1}
      assert.strictEqual(docs.length, 3);
    });
  });

  describe("$or operator", () => {
    it("should match documents satisfying any condition", async () => {
      const collection = client.db(dbName).collection("or_basic");
      await collection.insertMany([
        { status: "active", priority: "low" },
        { status: "inactive", priority: "high" },
        { status: "inactive", priority: "low" },
      ]);

      const docs = await collection
        .find({ $or: [{ status: "active" }, { priority: "high" }] })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should work with comparison operators", async () => {
      const collection = client.db(dbName).collection("or_comparison");
      await collection.insertMany([
        { value: 5 },
        { value: 50 },
        { value: 150 },
      ]);

      const docs = await collection
        .find({ $or: [{ value: { $lt: 10 } }, { value: { $gt: 100 } }] })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.value === 5));
      assert.ok(docs.some((d) => d.value === 150));
    });

    it("should combine with other field conditions (implicit AND)", async () => {
      const collection = client.db(dbName).collection("or_with_fields");
      await collection.insertMany([
        { type: "A", status: "active" },
        { type: "A", status: "pending" },
        { type: "B", status: "active" },
        { type: "B", status: "pending" },
      ]);

      // type: "A" AND (status: "active" OR status: "pending")
      const docs = await collection
        .find({ type: "A", $or: [{ status: "active" }, { status: "pending" }] })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => d.type === "A"));
    });

    it("should handle empty $or array (matches nothing)", async () => {
      const collection = client.db(dbName).collection("or_empty");
      await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);

      const docs = await collection.find({ $or: [] }).toArray();

      // Empty $or matches nothing
      assert.strictEqual(docs.length, 0);
    });

    it("should support nested $or", async () => {
      const collection = client.db(dbName).collection("or_nested");
      await collection.insertMany([
        { a: 1, b: 1 },
        { a: 2, b: 2 },
        { a: 3, b: 3 },
      ]);

      const docs = await collection
        .find({ $or: [{ a: 1 }, { $or: [{ b: 2 }, { b: 3 }] }] })
        .toArray();

      assert.strictEqual(docs.length, 3);
    });

    it("should work with $exists in conditions", async () => {
      const collection = client.db(dbName).collection("or_exists");
      await collection.insertMany([
        { name: "Alice", email: "alice@test.com" },
        { name: "Bob", phone: "123-456" },
        { name: "Charlie" }, // no contact info
      ]);

      const docs = await collection
        .find({
          $or: [{ email: { $exists: true } }, { phone: { $exists: true } }],
        })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });
  });

  describe("$not operator", () => {
    it("should invert comparison operator result", async () => {
      const collection = client.db(dbName).collection("not_gt");
      await collection.insertMany([
        { value: 10 },
        { value: 30 },
        { value: 50 },
      ]);

      // $not: { $gt: 25 } should match values <= 25
      const docs = await collection
        .find({ value: { $not: { $gt: 25 } } })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].value, 10);
    });

    it("should work with $in operator", async () => {
      const collection = client.db(dbName).collection("not_in");
      await collection.insertMany([
        { status: "active" },
        { status: "pending" },
        { status: "deleted" },
      ]);

      const docs = await collection
        .find({ status: { $not: { $in: ["deleted", "archived"] } } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should NOT match documents where field is missing", async () => {
      const collection = client.db(dbName).collection("not_missing");
      await collection.insertMany([
        { value: 10 },
        { value: 50 },
        { other: "field" }, // value is missing
      ]);

      // $not does NOT match documents where the field is missing
      // This is different from $ne which DOES match missing fields
      const docs = await collection
        .find({ value: { $not: { $gt: 25 } } })
        .toArray();

      // Only value: 10 should match (not the missing field document)
      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].value, 10);
    });

    it("should work with $lt operator", async () => {
      const collection = client.db(dbName).collection("not_lt");
      await collection.insertMany([
        { age: 15 },
        { age: 25 },
        { age: 35 },
      ]);

      // $not: { $lt: 20 } should match values >= 20
      const docs = await collection
        .find({ age: { $not: { $lt: 20 } } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should work with $eq operator", async () => {
      const collection = client.db(dbName).collection("not_eq");
      await collection.insertMany([
        { color: "red" },
        { color: "blue" },
        { color: "green" },
      ]);

      const docs = await collection
        .find({ color: { $not: { $eq: "red" } } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });
  });

  describe("$nor operator", () => {
    it("should match documents satisfying none of the conditions", async () => {
      const collection = client.db(dbName).collection("nor_basic");
      await collection.insertMany([
        { status: "active", type: "A" },
        { status: "deleted", type: "B" },
        { status: "pending", type: "C" },
      ]);

      const docs = await collection
        .find({ $nor: [{ status: "active" }, { status: "deleted" }] })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].status, "pending");
    });

    it("should work with comparison operators", async () => {
      const collection = client.db(dbName).collection("nor_comparison");
      await collection.insertMany([
        { value: 5 },
        { value: 50 },
        { value: 150 },
      ]);

      // Match documents where value is NOT < 10 AND NOT > 100
      const docs = await collection
        .find({ $nor: [{ value: { $lt: 10 } }, { value: { $gt: 100 } }] })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].value, 50);
    });

    it("should match documents where field is missing", async () => {
      const collection = client.db(dbName).collection("nor_missing");
      await collection.insertMany([
        { status: "active" },
        { status: "deleted" },
        { other: "field" }, // status is missing
      ]);

      // $nor also matches documents where the queried field is missing
      const docs = await collection
        .find({ $nor: [{ status: "active" }, { status: "deleted" }] })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].other, "field");
    });

    it("should combine with other field conditions", async () => {
      const collection = client.db(dbName).collection("nor_with_fields");
      await collection.insertMany([
        { type: "A", error: true },
        { type: "A", suspended: true },
        { type: "A", active: true },
        { type: "B", active: true },
      ]);

      const docs = await collection
        .find({ type: "A", $nor: [{ error: true }, { suspended: true }] })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].active, true);
    });

    it("should handle empty $nor array (matches all)", async () => {
      const collection = client.db(dbName).collection("nor_empty");
      await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);

      const docs = await collection.find({ $nor: [] }).toArray();

      // Empty $nor matches all documents (none of zero conditions are true)
      assert.strictEqual(docs.length, 3);
    });
  });

  describe("Complex combinations", () => {
    it("should handle nested $and inside $or", async () => {
      const collection = client.db(dbName).collection("complex_and_in_or");
      await collection.insertMany([
        { a: 1, b: 1, c: 1 },
        { a: 1, b: 2, c: 1 },
        { a: 2, b: 1, c: 2 },
        { a: 2, b: 2, c: 2 },
      ]);

      const docs = await collection
        .find({
          $or: [{ $and: [{ a: 1 }, { b: 1 }] }, { $and: [{ a: 2 }, { b: 2 }] }],
        })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should handle $not with $exists", async () => {
      const collection = client.db(dbName).collection("complex_not_exists");
      await collection.insertMany([
        { name: "Alice", deleted: true },
        { name: "Bob", deleted: false },
        { name: "Charlie" }, // deleted is missing
      ]);

      // Match where deleted field exists but is not true
      const docs = await collection
        .find({
          deleted: { $exists: true },
          $nor: [{ deleted: true }],
        })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, "Bob");
    });

    it("should handle multiple logical operators at top level", async () => {
      const collection = client.db(dbName).collection("complex_multi_logical");
      await collection.insertMany([
        { a: 1, b: 1, c: 1 },
        { a: 1, b: 2, c: 2 },
        { a: 2, b: 1, c: 1 },
        { a: 2, b: 2, c: 2 },
      ]);

      const docs = await collection
        .find({
          $and: [{ a: { $lte: 2 } }],
          $or: [{ b: 1 }, { c: 2 }],
        })
        .toArray();

      // a <= 2 (all) AND (b = 1 OR c = 2)
      assert.strictEqual(docs.length, 4);
    });

    it("should work with $exists in complex queries", async () => {
      const collection = client.db(dbName).collection("complex_exists");
      await collection.insertMany([
        { name: "Alice", email: "alice@test.com", verified: true },
        { name: "Bob", email: "bob@test.com" }, // verified missing
        { name: "Charlie", verified: false }, // email missing
        { name: "Dave" }, // both missing
      ]);

      // Find users with email who are either verified or verification status is missing
      const docs = await collection
        .find({
          email: { $exists: true },
          $or: [{ verified: true }, { verified: { $exists: false } }],
        })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.name === "Alice"));
      assert.ok(docs.some((d) => d.name === "Bob"));
    });
  });
});
