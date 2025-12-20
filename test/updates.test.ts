/**
 * Phase 3: Update Operation Tests
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

describe(`Update Operation Tests (${getTestModeName()})`, () => {
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

  describe("updateOne", () => {
    describe("basic update with $set", () => {
      it("should update a single document", async () => {
        const collection = client.db(dbName).collection("update_one_basic");
        await collection.insertMany([
          { name: "Alice", age: 30 },
          { name: "Bob", age: 25 },
        ]);

        const result = await collection.updateOne(
          { name: "Alice" },
          { $set: { age: 31 } }
        );

        assert.strictEqual(result.acknowledged, true);
        assert.strictEqual(result.matchedCount, 1);
        assert.strictEqual(result.modifiedCount, 1);
        assert.strictEqual(result.upsertedId, null);

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.age, 31);
      });

      it("should only update the first matching document", async () => {
        const collection = client.db(dbName).collection("update_one_first");
        await collection.insertMany([
          { type: "fruit", name: "apple" },
          { type: "fruit", name: "banana" },
        ]);

        await collection.updateOne(
          { type: "fruit" },
          { $set: { updated: true } }
        );

        const docs = await collection.find({ updated: true }).toArray();
        assert.strictEqual(docs.length, 1);
      });

      it("should return matchedCount 0 when no match", async () => {
        const collection = client.db(dbName).collection("update_one_nomatch");
        await collection.insertOne({ name: "Alice" });

        const result = await collection.updateOne(
          { name: "Bob" },
          { $set: { age: 25 } }
        );

        assert.strictEqual(result.matchedCount, 0);
        assert.strictEqual(result.modifiedCount, 0);
      });

      it("should set multiple fields at once", async () => {
        const collection = client.db(dbName).collection("update_one_multi");
        await collection.insertOne({ name: "Alice" });

        await collection.updateOne(
          { name: "Alice" },
          { $set: { age: 30, city: "NYC" } }
        );

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.age, 30);
        assert.strictEqual(doc?.city, "NYC");
      });

      it("should create new fields with $set", async () => {
        const collection = client.db(dbName).collection("update_one_newfield");
        await collection.insertOne({ name: "Alice" });

        await collection.updateOne(
          { name: "Alice" },
          { $set: { newField: "value" } }
        );

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.newField, "value");
      });
    });

    describe("$set with dot notation (nested fields)", () => {
      it("should update nested fields", async () => {
        const collection = client.db(dbName).collection("update_set_nested");
        await collection.insertOne({
          name: "Alice",
          address: { city: "NYC", zip: "10001" },
        });

        await collection.updateOne(
          { name: "Alice" },
          { $set: { "address.city": "LA" } }
        );

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual((doc?.address as { city: string }).city, "LA");
        assert.strictEqual((doc?.address as { zip: string }).zip, "10001");
      });

      it("should create nested structure when path doesn't exist", async () => {
        const collection = client
          .db(dbName)
          .collection("update_set_nested_create");
        await collection.insertOne({ name: "Alice" });

        await collection.updateOne(
          { name: "Alice" },
          { $set: { "a.b.c": "deep" } }
        );

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(
          (doc?.a as { b: { c: string } }).b.c,
          "deep"
        );
      });

      it("should update array element by index", async () => {
        const collection = client.db(dbName).collection("update_set_arr_idx");
        await collection.insertOne({ items: ["a", "b", "c"] });

        await collection.updateOne({}, { $set: { "items.1": "X" } });

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, ["a", "X", "c"]);
      });
    });

    describe("$unset operator", () => {
      it("should remove a field", async () => {
        const collection = client.db(dbName).collection("update_unset_basic");
        await collection.insertOne({ name: "Alice", age: 30, city: "NYC" });

        await collection.updateOne({ name: "Alice" }, { $unset: { age: "" } });

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.name, "Alice");
        assert.strictEqual(doc?.age, undefined);
        assert.strictEqual(doc?.city, "NYC");
      });

      it("should remove nested field", async () => {
        const collection = client.db(dbName).collection("update_unset_nested");
        await collection.insertOne({
          name: "Alice",
          address: { city: "NYC", zip: "10001" },
        });

        await collection.updateOne(
          { name: "Alice" },
          { $unset: { "address.zip": "" } }
        );

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual((doc?.address as { city: string }).city, "NYC");
        assert.strictEqual(
          (doc?.address as { zip?: string }).zip,
          undefined
        );
      });

      it("should do nothing when unsetting non-existent field", async () => {
        const collection = client.db(dbName).collection("update_unset_missing");
        await collection.insertOne({ name: "Alice" });

        const result = await collection.updateOne(
          { name: "Alice" },
          { $unset: { nonexistent: "" } }
        );

        assert.strictEqual(result.matchedCount, 1);
        // MongoDB still counts as modified even if field didn't exist
        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.name, "Alice");
      });
    });

    describe("$inc operator", () => {
      it("should increment a numeric field", async () => {
        const collection = client.db(dbName).collection("update_inc_basic");
        await collection.insertOne({ name: "Alice", score: 100 });

        await collection.updateOne({ name: "Alice" }, { $inc: { score: 10 } });

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.score, 110);
      });

      it("should decrement with negative value", async () => {
        const collection = client.db(dbName).collection("update_inc_neg");
        await collection.insertOne({ name: "Alice", score: 100 });

        await collection.updateOne({ name: "Alice" }, { $inc: { score: -30 } });

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.score, 70);
      });

      it("should create field with value if it doesn't exist", async () => {
        const collection = client.db(dbName).collection("update_inc_create");
        await collection.insertOne({ name: "Alice" });

        await collection.updateOne({ name: "Alice" }, { $inc: { score: 50 } });

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.score, 50);
      });

      it("should increment nested field", async () => {
        const collection = client.db(dbName).collection("update_inc_nested");
        await collection.insertOne({ name: "Alice", stats: { score: 100 } });

        await collection.updateOne(
          { name: "Alice" },
          { $inc: { "stats.score": 5 } }
        );

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual((doc?.stats as { score: number }).score, 105);
      });

      it("should work with floating point numbers", async () => {
        const collection = client.db(dbName).collection("update_inc_float");
        await collection.insertOne({ value: 10.5 });

        await collection.updateOne({}, { $inc: { value: 0.3 } });

        const doc = await collection.findOne({});
        // Use approximate comparison for floating point
        assert.ok(Math.abs((doc?.value as number) - 10.8) < 0.0001);
      });
    });

    describe("combining operators", () => {
      it("should apply $set and $inc together", async () => {
        const collection = client.db(dbName).collection("update_combined");
        await collection.insertOne({ name: "Alice", score: 100, level: 1 });

        await collection.updateOne(
          { name: "Alice" },
          { $set: { name: "Alicia" }, $inc: { score: 50, level: 1 } }
        );

        const doc = await collection.findOne({ name: "Alicia" });
        assert.strictEqual(doc?.name, "Alicia");
        assert.strictEqual(doc?.score, 150);
        assert.strictEqual(doc?.level, 2);
      });

      it("should apply $set and $unset together", async () => {
        const collection = client.db(dbName).collection("update_set_unset");
        await collection.insertOne({ name: "Alice", age: 30, oldField: "x" });

        await collection.updateOne(
          { name: "Alice" },
          { $set: { newField: "y" }, $unset: { oldField: "" } }
        );

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.newField, "y");
        assert.strictEqual(doc?.oldField, undefined);
      });
    });
  });

  describe("updateMany", () => {
    it("should update all matching documents", async () => {
      const collection = client.db(dbName).collection("update_many_basic");
      await collection.insertMany([
        { type: "fruit", name: "apple" },
        { type: "fruit", name: "banana" },
        { type: "vegetable", name: "carrot" },
      ]);

      const result = await collection.updateMany(
        { type: "fruit" },
        { $set: { organic: true } }
      );

      assert.strictEqual(result.acknowledged, true);
      assert.strictEqual(result.matchedCount, 2);
      assert.strictEqual(result.modifiedCount, 2);

      const docs = await collection.find({ organic: true }).toArray();
      assert.strictEqual(docs.length, 2);
    });

    it("should update all documents with empty filter", async () => {
      const collection = client.db(dbName).collection("update_many_all");
      await collection.insertMany([{ value: 1 }, { value: 2 }, { value: 3 }]);

      const result = await collection.updateMany(
        {},
        { $set: { updated: true } }
      );

      assert.strictEqual(result.matchedCount, 3);
      assert.strictEqual(result.modifiedCount, 3);
    });

    it("should return zero counts when no match", async () => {
      const collection = client.db(dbName).collection("update_many_nomatch");
      await collection.insertOne({ name: "Alice" });

      const result = await collection.updateMany(
        { name: "Nobody" },
        { $set: { age: 25 } }
      );

      assert.strictEqual(result.matchedCount, 0);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("should work with $inc on multiple documents", async () => {
      const collection = client.db(dbName).collection("update_many_inc");
      await collection.insertMany([
        { type: "counter", value: 10 },
        { type: "counter", value: 20 },
        { type: "other", value: 30 },
      ]);

      await collection.updateMany({ type: "counter" }, { $inc: { value: 5 } });

      const docs = await collection.find({ type: "counter" }).toArray();
      const values = docs.map((d) => d.value).sort();
      assert.deepStrictEqual(values, [15, 25]);
    });
  });

  describe("upsert option", () => {
    it("should insert document when no match and upsert is true", async () => {
      const collection = client.db(dbName).collection("upsert_insert");

      const result = await collection.updateOne(
        { name: "NewUser" },
        { $set: { age: 25 } },
        { upsert: true }
      );

      assert.strictEqual(result.matchedCount, 0);
      assert.strictEqual(result.modifiedCount, 0);
      assert.strictEqual(result.upsertedCount, 1);
      assert.ok(result.upsertedId !== null);

      const doc = await collection.findOne({ name: "NewUser" });
      assert.ok(doc !== null);
      assert.strictEqual(doc?.name, "NewUser");
      assert.strictEqual(doc?.age, 25);
    });

    it("should update existing document when match and upsert is true", async () => {
      const collection = client.db(dbName).collection("upsert_update");
      await collection.insertOne({ name: "Alice", age: 30 });

      const result = await collection.updateOne(
        { name: "Alice" },
        { $set: { age: 31 } },
        { upsert: true }
      );

      assert.strictEqual(result.matchedCount, 1);
      assert.strictEqual(result.modifiedCount, 1);
      assert.strictEqual(result.upsertedId, null);
      assert.strictEqual(result.upsertedCount, 0);

      // Should only have one document
      const docs = await collection.find({ name: "Alice" }).toArray();
      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].age, 31);
    });

    it("should include filter fields in upserted document", async () => {
      const collection = client.db(dbName).collection("upsert_filter_fields");

      await collection.updateOne(
        { type: "fruit", name: "apple" },
        { $set: { color: "red" } },
        { upsert: true }
      );

      const doc = await collection.findOne({ name: "apple" });
      assert.ok(doc !== null);
      assert.strictEqual(doc?.type, "fruit");
      assert.strictEqual(doc?.name, "apple");
      assert.strictEqual(doc?.color, "red");
    });

    it("should work with updateMany and upsert", async () => {
      const collection = client.db(dbName).collection("upsert_many");

      const result = await collection.updateMany(
        { category: "electronics" },
        { $set: { inStock: true } },
        { upsert: true }
      );

      assert.strictEqual(result.upsertedCount, 1);
      assert.ok(result.upsertedId !== null);

      const doc = await collection.findOne({ category: "electronics" });
      assert.ok(doc !== null);
      assert.strictEqual(doc?.inStock, true);
    });

    it("should apply $inc in upserted document", async () => {
      const collection = client.db(dbName).collection("upsert_inc");

      await collection.updateOne(
        { name: "counter" },
        { $inc: { value: 5 } },
        { upsert: true }
      );

      const doc = await collection.findOne({ name: "counter" });
      assert.ok(doc !== null);
      assert.strictEqual(doc?.value, 5);
    });
  });

  describe("modifiedCount behavior", () => {
    it("should not count as modified if values are the same", async () => {
      const collection = client.db(dbName).collection("modified_same");
      await collection.insertOne({ name: "Alice", age: 30 });

      const result = await collection.updateOne(
        { name: "Alice" },
        { $set: { age: 30 } }
      );

      assert.strictEqual(result.matchedCount, 1);
      // MongoDB returns modifiedCount 0 when value doesn't change
      assert.strictEqual(result.modifiedCount, 0);
    });
  });

  describe("edge cases", () => {
    it("should handle updating with ObjectId in filter", async () => {
      const collection = client.db(dbName).collection("update_objectid");
      const insertResult = await collection.insertOne({ name: "Alice" });
      const id = insertResult.insertedId;

      await collection.updateOne({ _id: id }, { $set: { age: 30 } });

      const doc = await collection.findOne({ _id: id });
      assert.strictEqual(doc?.age, 30);
    });

    it("should handle empty $set object", async () => {
      const collection = client.db(dbName).collection("update_empty_set");
      await collection.insertOne({ name: "Alice" });

      const result = await collection.updateOne({ name: "Alice" }, { $set: {} });

      assert.strictEqual(result.matchedCount, 1);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("should handle Date values in $set", async () => {
      const collection = client.db(dbName).collection("update_date");
      await collection.insertOne({ name: "Alice" });

      const now = new Date();
      await collection.updateOne({ name: "Alice" }, { $set: { createdAt: now } });

      const doc = await collection.findOne({ name: "Alice" });
      assert.ok(doc?.createdAt instanceof Date);
      assert.strictEqual(
        (doc?.createdAt as Date).getTime(),
        now.getTime()
      );
    });

    it("should handle array values in $set", async () => {
      const collection = client.db(dbName).collection("update_array");
      await collection.insertOne({ name: "Alice" });

      await collection.updateOne(
        { name: "Alice" },
        { $set: { tags: ["a", "b", "c"] } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.deepStrictEqual(doc?.tags, ["a", "b", "c"]);
    });

    it("should handle nested object values in $set", async () => {
      const collection = client.db(dbName).collection("update_nested_obj");
      await collection.insertOne({ name: "Alice" });

      await collection.updateOne(
        { name: "Alice" },
        { $set: { profile: { bio: "Hello", age: 30 } } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.deepStrictEqual(doc?.profile, { bio: "Hello", age: 30 });
    });

    it("should handle nested arrays in $set", async () => {
      const collection = client.db(dbName).collection("update_nested_arrays");
      await collection.insertOne({ name: "test" });

      await collection.updateOne(
        { name: "test" },
        { $set: { matrix: [[1, 2], [3, 4]] } }
      );

      const doc = await collection.findOne({ name: "test" });
      assert.ok(Array.isArray(doc?.matrix));
      assert.ok(Array.isArray((doc?.matrix as unknown[])[0]));
      assert.deepStrictEqual(doc?.matrix, [[1, 2], [3, 4]]);
    });

    it("should preserve nested arrays when updating other fields", async () => {
      const collection = client.db(dbName).collection("preserve_nested_arr");
      // Document already has nested arrays
      await collection.insertOne({ name: "test", matrix: [[1, 2], [3, 4]] });

      // Update a different field - this triggers cloneDocument on existing doc
      await collection.updateOne(
        { name: "test" },
        { $set: { updated: true } }
      );

      const doc = await collection.findOne({ name: "test" });
      assert.ok(Array.isArray(doc?.matrix), "matrix should be an array");
      assert.ok(
        Array.isArray((doc?.matrix as unknown[])[0]),
        "matrix[0] should be an array, not an object"
      );
      assert.deepStrictEqual(doc?.matrix, [[1, 2], [3, 4]]);
    });

    it("should handle deeply nested arrays with objects", async () => {
      const collection = client.db(dbName).collection("update_deep_nested");
      await collection.insertOne({ name: "test" });

      await collection.updateOne(
        { name: "test" },
        { $set: { data: [{ items: [1, 2] }, { items: [3, 4] }] } }
      );

      const doc = await collection.findOne({ name: "test" });
      assert.deepStrictEqual(doc?.data, [{ items: [1, 2] }, { items: [3, 4] }]);
    });

    it("should include $eq filter fields in upserted document", async () => {
      const collection = client.db(dbName).collection("upsert_eq_filter");

      await collection.updateOne(
        { type: { $eq: "fruit" }, name: "apple" },
        { $set: { color: "red" } },
        { upsert: true }
      );

      const doc = await collection.findOne({ name: "apple" });
      assert.ok(doc !== null);
      assert.strictEqual(doc?.type, "fruit");
      assert.strictEqual(doc?.name, "apple");
      assert.strictEqual(doc?.color, "red");
    });
  });
});
