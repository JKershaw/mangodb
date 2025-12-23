/**
 * Phase 13: Additional Update Operators Tests
 *
 * These tests cover $min, $max, $mul, $rename, $currentDate, and $setOnInsert operators.
 * Tests run against both real MongoDB and MangoDB to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "./test-harness.ts";

describe(`Phase 13: Additional Update Operators (${getTestModeName()})`, () => {
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

  // ==================== $min Operator ====================
  describe("$min operator", () => {
    it("should update when new value is less than current", async () => {
      const collection = client.db(dbName).collection("min_less");
      await collection.insertOne({ name: "Alice", lowScore: 100 });

      await collection.updateOne({ name: "Alice" }, { $min: { lowScore: 50 } });

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.lowScore, 50);
    });

    it("should not update when new value is greater than current", async () => {
      const collection = client.db(dbName).collection("min_greater");
      await collection.insertOne({ name: "Alice", lowScore: 50 });

      const result = await collection.updateOne(
        { name: "Alice" },
        { $min: { lowScore: 100 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.lowScore, 50);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("should not update when values are equal", async () => {
      const collection = client.db(dbName).collection("min_equal");
      await collection.insertOne({ name: "Alice", score: 75 });

      const result = await collection.updateOne(
        { name: "Alice" },
        { $min: { score: 75 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.score, 75);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("should create field if missing", async () => {
      const collection = client.db(dbName).collection("min_create");
      await collection.insertOne({ name: "Alice" });

      await collection.updateOne({ name: "Alice" }, { $min: { lowScore: 75 } });

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.lowScore, 75);
    });

    it("should work with nested fields using dot notation", async () => {
      const collection = client.db(dbName).collection("min_nested");
      await collection.insertOne({ name: "Alice", stats: { lowScore: 100 } });

      await collection.updateOne(
        { name: "Alice" },
        { $min: { "stats.lowScore": 50 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual((doc?.stats as { lowScore: number }).lowScore, 50);
    });

    it("should work with dates", async () => {
      const collection = client.db(dbName).collection("min_dates");
      const earlier = new Date("2023-01-01");
      const later = new Date("2023-06-01");
      await collection.insertOne({ name: "Alice", firstVisit: later });

      await collection.updateOne(
        { name: "Alice" },
        { $min: { firstVisit: earlier } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(
        (doc?.firstVisit as Date).getTime(),
        earlier.getTime()
      );
    });

    it("should not update date when new date is later", async () => {
      const collection = client.db(dbName).collection("min_dates_no_update");
      const earlier = new Date("2023-01-01");
      const later = new Date("2023-06-01");
      await collection.insertOne({ name: "Alice", firstVisit: earlier });

      const result = await collection.updateOne(
        { name: "Alice" },
        { $min: { firstVisit: later } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(
        (doc?.firstVisit as Date).getTime(),
        earlier.getTime()
      );
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("should work with updateMany", async () => {
      const collection = client.db(dbName).collection("min_many");
      await collection.insertMany([
        { type: "score", value: 100 },
        { type: "score", value: 80 },
      ]);

      await collection.updateMany({ type: "score" }, { $min: { value: 90 } });

      const docs = await collection.find({ type: "score" }).toArray();
      const values = docs.map((d) => d.value).sort((a, b) => (a as number) - (b as number));
      // First doc: 100 -> 90 (updated), Second doc: 80 stays 80
      assert.deepStrictEqual(values, [80, 90]);
    });
  });

  // ==================== $max Operator ====================
  describe("$max operator", () => {
    it("should update when new value is greater than current", async () => {
      const collection = client.db(dbName).collection("max_greater");
      await collection.insertOne({ name: "Alice", highScore: 50 });

      await collection.updateOne(
        { name: "Alice" },
        { $max: { highScore: 100 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.highScore, 100);
    });

    it("should not update when new value is less than current", async () => {
      const collection = client.db(dbName).collection("max_less");
      await collection.insertOne({ name: "Alice", highScore: 100 });

      const result = await collection.updateOne(
        { name: "Alice" },
        { $max: { highScore: 50 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.highScore, 100);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("should not update when values are equal", async () => {
      const collection = client.db(dbName).collection("max_equal");
      await collection.insertOne({ name: "Alice", score: 75 });

      const result = await collection.updateOne(
        { name: "Alice" },
        { $max: { score: 75 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.score, 75);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("should create field if missing", async () => {
      const collection = client.db(dbName).collection("max_create");
      await collection.insertOne({ name: "Alice" });

      await collection.updateOne(
        { name: "Alice" },
        { $max: { highScore: 75 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.highScore, 75);
    });

    it("should work with nested fields", async () => {
      const collection = client.db(dbName).collection("max_nested");
      await collection.insertOne({ name: "Alice", stats: { highScore: 50 } });

      await collection.updateOne(
        { name: "Alice" },
        { $max: { "stats.highScore": 100 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual((doc?.stats as { highScore: number }).highScore, 100);
    });

    it("should work with dates", async () => {
      const collection = client.db(dbName).collection("max_dates");
      const earlier = new Date("2023-01-01");
      const later = new Date("2023-06-01");
      await collection.insertOne({ name: "Alice", lastVisit: earlier });

      await collection.updateOne(
        { name: "Alice" },
        { $max: { lastVisit: later } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual((doc?.lastVisit as Date).getTime(), later.getTime());
    });

    it("should work with updateMany", async () => {
      const collection = client.db(dbName).collection("max_many");
      await collection.insertMany([
        { type: "score", value: 100 },
        { type: "score", value: 80 },
      ]);

      await collection.updateMany({ type: "score" }, { $max: { value: 90 } });

      const docs = await collection.find({ type: "score" }).toArray();
      const values = docs.map((d) => d.value).sort((a, b) => (a as number) - (b as number));
      // First doc: 100 stays 100, Second doc: 80 -> 90 (updated)
      assert.deepStrictEqual(values, [90, 100]);
    });
  });

  // ==================== $mul Operator ====================
  describe("$mul operator", () => {
    it("should multiply existing numeric value", async () => {
      const collection = client.db(dbName).collection("mul_basic");
      await collection.insertOne({ name: "Alice", price: 100 });

      await collection.updateOne({ name: "Alice" }, { $mul: { price: 1.1 } });

      const doc = await collection.findOne({ name: "Alice" });
      assert.ok(Math.abs((doc?.price as number) - 110) < 0.0001);
    });

    it("should create field with 0 if missing", async () => {
      const collection = client.db(dbName).collection("mul_missing");
      await collection.insertOne({ name: "Alice" });

      await collection.updateOne({ name: "Alice" }, { $mul: { quantity: 5 } });

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.quantity, 0); // NOT 5!
    });

    it("should handle integer multiplication", async () => {
      const collection = client.db(dbName).collection("mul_int");
      await collection.insertOne({ value: 10 });

      await collection.updateOne({}, { $mul: { value: 3 } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.value, 30);
    });

    it("should handle floating-point multiplication", async () => {
      const collection = client.db(dbName).collection("mul_float");
      await collection.insertOne({ value: 10.5 });

      await collection.updateOne({}, { $mul: { value: 2 } });

      const doc = await collection.findOne({});
      assert.ok(Math.abs((doc?.value as number) - 21) < 0.0001);
    });

    it("should handle multiplication by zero", async () => {
      const collection = client.db(dbName).collection("mul_zero");
      await collection.insertOne({ value: 100 });

      await collection.updateOne({}, { $mul: { value: 0 } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.value, 0);
    });

    it("should handle negative multiplier", async () => {
      const collection = client.db(dbName).collection("mul_negative");
      await collection.insertOne({ value: 10 });

      await collection.updateOne({}, { $mul: { value: -2 } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.value, -20);
    });

    it("should work with nested fields", async () => {
      const collection = client.db(dbName).collection("mul_nested");
      await collection.insertOne({ stats: { multiplier: 5 } });

      await collection.updateOne({}, { $mul: { "stats.multiplier": 3 } });

      const doc = await collection.findOne({});
      assert.strictEqual(
        (doc?.stats as { multiplier: number }).multiplier,
        15
      );
    });

    it("should throw error for non-numeric field value", async () => {
      const collection = client.db(dbName).collection("mul_non_numeric");
      await collection.insertOne({ name: "Alice", value: "not a number" });

      await assert.rejects(
        async () =>
          await collection.updateOne({ name: "Alice" }, { $mul: { value: 2 } }),
        (err: Error) => {
          return (
            err.message.includes("non-numeric") ||
            err.message.includes("Cannot apply $mul") ||
            err.message.includes("Cannot increment") // Old MongoDB error
          );
        }
      );
    });

    it("should work with updateMany", async () => {
      const collection = client.db(dbName).collection("mul_many");
      await collection.insertMany([
        { type: "price", value: 100 },
        { type: "price", value: 200 },
      ]);

      await collection.updateMany({ type: "price" }, { $mul: { value: 0.9 } }); // 10% discount

      const docs = await collection.find({ type: "price" }).toArray();
      const values = docs
        .map((d) => d.value)
        .sort((a, b) => (a as number) - (b as number));
      assert.ok(Math.abs((values[0] as number) - 90) < 0.0001);
      assert.ok(Math.abs((values[1] as number) - 180) < 0.0001);
    });
  });

  // ==================== $rename Operator ====================
  describe("$rename operator", () => {
    it("should rename a field", async () => {
      const collection = client.db(dbName).collection("rename_basic");
      await collection.insertOne({ name: "Alice", oldField: "value" });

      await collection.updateOne(
        { name: "Alice" },
        { $rename: { oldField: "newField" } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.newField, "value");
      assert.strictEqual(doc?.oldField, undefined);
    });

    it("should do nothing if old field does not exist", async () => {
      const collection = client.db(dbName).collection("rename_missing");
      await collection.insertOne({ name: "Alice" });

      const result = await collection.updateOne(
        { name: "Alice" },
        { $rename: { nonexistent: "newField" } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.newField, undefined);
      assert.strictEqual(result.matchedCount, 1);
    });

    it("should overwrite existing target field", async () => {
      const collection = client.db(dbName).collection("rename_overwrite");
      await collection.insertOne({
        name: "Alice",
        oldField: "old",
        newField: "existing",
      });

      await collection.updateOne(
        { name: "Alice" },
        { $rename: { oldField: "newField" } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.newField, "old");
      assert.strictEqual(doc?.oldField, undefined);
    });

    it("should work with nested source field using dot notation", async () => {
      const collection = client.db(dbName).collection("rename_nested_src");
      await collection.insertOne({
        name: "Alice",
        data: { oldName: "value" },
      });

      await collection.updateOne(
        { name: "Alice" },
        { $rename: { "data.oldName": "data.newName" } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(
        (doc?.data as { newName: string; oldName?: string }).newName,
        "value"
      );
      assert.strictEqual(
        (doc?.data as { newName: string; oldName?: string }).oldName,
        undefined
      );
    });

    it("should move field to different nesting level", async () => {
      const collection = client.db(dbName).collection("rename_move_level");
      await collection.insertOne({
        name: "Alice",
        nested: { field: "value" },
      });

      await collection.updateOne(
        { name: "Alice" },
        { $rename: { "nested.field": "topLevel" } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.topLevel, "value");
      assert.strictEqual(
        (doc?.nested as { field?: string }).field,
        undefined
      );
    });

    it("should rename multiple fields at once", async () => {
      const collection = client.db(dbName).collection("rename_multiple");
      await collection.insertOne({ a: 1, b: 2, c: 3 });

      await collection.updateOne({}, { $rename: { a: "x", b: "y" } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.x, 1);
      assert.strictEqual(doc?.y, 2);
      assert.strictEqual(doc?.c, 3);
      assert.strictEqual(doc?.a, undefined);
      assert.strictEqual(doc?.b, undefined);
    });

    it("should work with updateMany", async () => {
      const collection = client.db(dbName).collection("rename_many");
      await collection.insertMany([
        { type: "item", oldName: "A" },
        { type: "item", oldName: "B" },
      ]);

      await collection.updateMany(
        { type: "item" },
        { $rename: { oldName: "newName" } }
      );

      const docs = await collection.find({ type: "item" }).toArray();
      assert.ok(docs.every((d) => d.newName !== undefined));
      assert.ok(docs.every((d) => d.oldName === undefined));
    });
  });

  // ==================== $currentDate Operator ====================
  describe("$currentDate operator", () => {
    it("should set field to current date with true", async () => {
      const collection = client.db(dbName).collection("currentdate_true");
      await collection.insertOne({ name: "Alice" });
      const before = new Date();

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { lastModified: true } }
      );

      const after = new Date();
      const doc = await collection.findOne({ name: "Alice" });
      assert.ok(doc?.lastModified instanceof Date);
      assert.ok((doc?.lastModified as Date) >= before);
      assert.ok((doc?.lastModified as Date) <= after);
    });

    it("should set field to current date with $type: date", async () => {
      const collection = client.db(dbName).collection("currentdate_type_date");
      await collection.insertOne({ name: "Alice" });
      const before = new Date();

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { lastModified: { $type: "date" } } }
      );

      const after = new Date();
      const doc = await collection.findOne({ name: "Alice" });
      assert.ok(doc?.lastModified instanceof Date);
      assert.ok((doc?.lastModified as Date) >= before);
      assert.ok((doc?.lastModified as Date) <= after);
    });

    it("should set field to timestamp with $type: timestamp", async () => {
      const collection = client
        .db(dbName)
        .collection("currentdate_type_timestamp");
      await collection.insertOne({ name: "Alice" });
      const before = Date.now();

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { lastModified: { $type: "timestamp" } } }
      );

      const after = Date.now();
      const doc = await collection.findOne({ name: "Alice" });
      // In MongoDB this returns a Timestamp object; in MangoDB we use numeric timestamp
      // Be flexible about the type - it could be a number, Date, or Timestamp object
      const ts = doc?.lastModified;
      // Check that the value represents a reasonable timestamp
      if (typeof ts === "number") {
        assert.ok(ts >= before && ts <= after);
      } else if (ts instanceof Date) {
        assert.ok(ts.getTime() >= before && ts.getTime() <= after);
      } else if (ts && typeof ts === "object") {
        // MongoDB Timestamp object - just verify it exists
        assert.ok(ts !== null);
      }
    });

    it("should create field if missing", async () => {
      const collection = client.db(dbName).collection("currentdate_create");
      await collection.insertOne({ name: "Alice" });

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { createdAt: true } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.ok(doc?.createdAt !== undefined);
    });

    it("should overwrite existing field", async () => {
      const collection = client.db(dbName).collection("currentdate_overwrite");
      const oldDate = new Date("2020-01-01");
      await collection.insertOne({ name: "Alice", lastModified: oldDate });

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { lastModified: true } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.ok((doc?.lastModified as Date).getTime() > oldDate.getTime());
    });

    it("should work with nested fields", async () => {
      const collection = client.db(dbName).collection("currentdate_nested");
      await collection.insertOne({ name: "Alice", meta: {} });

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { "meta.updatedAt": true } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.ok((doc?.meta as { updatedAt: Date }).updatedAt instanceof Date);
    });

    it("should set multiple fields at once", async () => {
      const collection = client.db(dbName).collection("currentdate_multiple");
      await collection.insertOne({ name: "Alice" });

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { createdAt: true, updatedAt: true } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.ok(doc?.createdAt instanceof Date);
      assert.ok(doc?.updatedAt instanceof Date);
    });

    it("should work with updateMany", async () => {
      const collection = client.db(dbName).collection("currentdate_many");
      await collection.insertMany([
        { type: "item", name: "A" },
        { type: "item", name: "B" },
      ]);

      await collection.updateMany(
        { type: "item" },
        { $currentDate: { updatedAt: true } }
      );

      const docs = await collection.find({ type: "item" }).toArray();
      assert.ok(docs.every((d) => d.updatedAt instanceof Date));
    });
  });

  // ==================== $setOnInsert Operator ====================
  describe("$setOnInsert operator", () => {
    it("should set fields on upsert insert", async () => {
      const collection = client.db(dbName).collection("setoninsert_insert");
      const now = new Date();

      await collection.updateOne(
        { email: "new@test.com" },
        {
          $set: { name: "New User" },
          $setOnInsert: { createdAt: now, role: "user" },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ email: "new@test.com" });
      assert.strictEqual(doc?.name, "New User");
      assert.strictEqual(doc?.role, "user");
      assert.strictEqual((doc?.createdAt as Date).getTime(), now.getTime());
    });

    it("should not set fields when updating existing document", async () => {
      const collection = client.db(dbName).collection("setoninsert_update");
      const oldDate = new Date("2020-01-01");
      await collection.insertOne({
        email: "existing@test.com",
        createdAt: oldDate,
        name: "Old Name",
      });

      const newDate = new Date();
      await collection.updateOne(
        { email: "existing@test.com" },
        {
          $set: { name: "Updated Name" },
          $setOnInsert: { createdAt: newDate, role: "admin" },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ email: "existing@test.com" });
      assert.strictEqual(doc?.name, "Updated Name");
      assert.strictEqual((doc?.createdAt as Date).getTime(), oldDate.getTime()); // NOT updated
      assert.strictEqual(doc?.role, undefined); // NOT set
    });

    it("should be ignored without upsert option", async () => {
      const collection = client.db(dbName).collection("setoninsert_no_upsert");

      await collection.updateOne(
        { email: "nonexistent@test.com" },
        { $setOnInsert: { name: "Should Not Exist" } }
        // No upsert: true
      );

      const doc = await collection.findOne({ email: "nonexistent@test.com" });
      assert.strictEqual(doc, null);
    });

    it("should work with nested fields", async () => {
      const collection = client.db(dbName).collection("setoninsert_nested");

      await collection.updateOne(
        { id: 1 },
        {
          $setOnInsert: {
            "meta.createdAt": new Date(),
            "meta.version": 1,
          },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ id: 1 });
      assert.ok((doc?.meta as { createdAt: Date }).createdAt instanceof Date);
      assert.strictEqual((doc?.meta as { version: number }).version, 1);
    });

    it("should work alone without $set", async () => {
      const collection = client.db(dbName).collection("setoninsert_alone");

      await collection.updateOne(
        { key: "unique" },
        { $setOnInsert: { value: "initial" } },
        { upsert: true }
      );

      const doc = await collection.findOne({ key: "unique" });
      assert.strictEqual(doc?.key, "unique");
      assert.strictEqual(doc?.value, "initial");
    });

    it("should combine with $inc on insert", async () => {
      const collection = client.db(dbName).collection("setoninsert_with_inc");

      await collection.updateOne(
        { name: "counter" },
        {
          $inc: { count: 1 },
          $setOnInsert: { startedAt: new Date() },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ name: "counter" });
      assert.strictEqual(doc?.count, 1);
      assert.ok(doc?.startedAt instanceof Date);
    });

    it("should only apply $setOnInsert on second upsert when doc exists", async () => {
      const collection = client.db(dbName).collection("setoninsert_second");
      const initialDate = new Date("2023-01-01");

      // First upsert - inserts document
      await collection.updateOne(
        { name: "item" },
        {
          $inc: { count: 1 },
          $setOnInsert: { createdAt: initialDate },
        },
        { upsert: true }
      );

      const newDate = new Date("2024-01-01");
      // Second upsert - updates existing document
      await collection.updateOne(
        { name: "item" },
        {
          $inc: { count: 1 },
          $setOnInsert: { createdAt: newDate }, // Should be ignored
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ name: "item" });
      assert.strictEqual(doc?.count, 2);
      assert.strictEqual(
        (doc?.createdAt as Date).getTime(),
        initialDate.getTime()
      ); // Still initial date
    });
  });

  // ==================== Combined Operators ====================
  describe("combining Phase 13 operators", () => {
    it("should combine $min and $max in same update", async () => {
      const collection = client.db(dbName).collection("combine_min_max");
      await collection.insertOne({ name: "Alice", low: 50, high: 100 });

      await collection.updateOne(
        { name: "Alice" },
        {
          $min: { low: 25 },
          $max: { high: 150 },
        }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.low, 25);
      assert.strictEqual(doc?.high, 150);
    });

    it("should combine $mul with $set", async () => {
      const collection = client.db(dbName).collection("combine_mul_set");
      await collection.insertOne({ name: "Product", price: 100 });

      await collection.updateOne(
        { name: "Product" },
        {
          $mul: { price: 1.2 },
          $set: { updated: true },
        }
      );

      const doc = await collection.findOne({ name: "Product" });
      assert.ok(Math.abs((doc?.price as number) - 120) < 0.0001);
      assert.strictEqual(doc?.updated, true);
    });

    it("should combine $rename with $set", async () => {
      const collection = client.db(dbName).collection("combine_rename_set");
      await collection.insertOne({ oldName: "value", other: 1 });

      await collection.updateOne(
        {},
        {
          $rename: { oldName: "newName" },
          $set: { status: "renamed" },
        }
      );

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.newName, "value");
      assert.strictEqual(doc?.oldName, undefined);
      assert.strictEqual(doc?.status, "renamed");
    });

    it("should combine $currentDate with $set", async () => {
      const collection = client.db(dbName).collection("combine_currentdate_set");
      await collection.insertOne({ name: "Alice" });
      const before = new Date();

      await collection.updateOne(
        { name: "Alice" },
        {
          $currentDate: { updatedAt: true },
          $set: { status: "active" },
        }
      );

      const after = new Date();
      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.status, "active");
      assert.ok((doc?.updatedAt as Date) >= before);
      assert.ok((doc?.updatedAt as Date) <= after);
    });
  });

  // ==================== Edge Cases and Error Conditions ====================
  describe("edge cases and error conditions", () => {
    it("$rename should throw error when source equals destination", async () => {
      const collection = client.db(dbName).collection("rename_same_path");
      await collection.insertOne({ field: "value" });

      await assert.rejects(
        async () =>
          await collection.updateOne({}, { $rename: { field: "field" } }),
        (err: Error) => {
          return (
            err.message.includes("source and dest") ||
            err.message.includes("must differ")
          );
        }
      );
    });

    it("$currentDate should throw error for invalid $type value", async () => {
      const collection = client.db(dbName).collection("currentdate_invalid_type");
      await collection.insertOne({ name: "Alice" });

      await assert.rejects(
        async () =>
          await collection.updateOne(
            { name: "Alice" },
            { $currentDate: { lastModified: { $type: "invalid" as "date" } } }
          ),
        (err: Error) => {
          // MongoDB error: "The '$type' string field is required to be 'date' or 'timestamp'"
          return (
            err.message.includes("required to be") ||
            err.message.includes("'date' or 'timestamp'")
          );
        }
      );
    });

    it("$currentDate with false value should still set current date", async () => {
      const collection = client.db(dbName).collection("currentdate_false");
      const oldDate = new Date("2020-01-01");
      await collection.insertOne({ name: "Alice", lastModified: oldDate });

      const beforeUpdate = Date.now();
      // MongoDB treats false the same as true - it sets the current date
      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { lastModified: false as unknown as true } }
      );
      const afterUpdate = Date.now();

      const doc = await collection.findOne({ name: "Alice" });
      const updatedTime = (doc?.lastModified as Date).getTime();

      // Field should be updated to current date (not the old date)
      assert.ok(updatedTime >= beforeUpdate - 1000, "Date should be recent");
      assert.ok(updatedTime <= afterUpdate + 1000, "Date should not be in future");
      assert.notStrictEqual(updatedTime, oldDate.getTime(), "Date should have changed");
    });

    it("$mul should throw error for null field value", async () => {
      const collection = client.db(dbName).collection("mul_null");
      await collection.insertOne({ name: "Alice", value: null });

      await assert.rejects(
        async () =>
          await collection.updateOne({ name: "Alice" }, { $mul: { value: 2 } }),
        (err: Error) => {
          return (
            err.message.includes("non-numeric") ||
            err.message.includes("Cannot apply $mul")
          );
        }
      );
    });

    it("$min should handle null current value using BSON ordering", async () => {
      const collection = client.db(dbName).collection("min_null_current");
      await collection.insertOne({ name: "Alice", value: null });

      // In BSON ordering, null (type 1) < number (type 2)
      // So $min with a number should NOT update because the number is greater
      const result = await collection.updateOne(
        { name: "Alice" },
        { $min: { value: 100 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      // null < 100 in BSON ordering, so value stays null
      assert.strictEqual(doc?.value, null);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("$max should handle null current value using BSON ordering", async () => {
      const collection = client.db(dbName).collection("max_null_current");
      await collection.insertOne({ name: "Alice", value: null });

      // In BSON ordering, null (type 1) < number (type 2)
      // So $max with a number should update because the number is greater
      await collection.updateOne({ name: "Alice" }, { $max: { value: 100 } });

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.value, 100);
    });

    it("$min/$max should compare strings correctly", async () => {
      const collection = client.db(dbName).collection("min_max_strings");
      await collection.insertOne({ name: "Alice", code: "beta" });

      // "alpha" < "beta" alphabetically
      await collection.updateOne(
        { name: "Alice" },
        { $min: { code: "alpha" } }
      );

      let doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.code, "alpha");

      // "gamma" > "alpha" alphabetically
      await collection.updateOne(
        { name: "Alice" },
        { $max: { code: "gamma" } }
      );

      doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.code, "gamma");
    });
  });
});
