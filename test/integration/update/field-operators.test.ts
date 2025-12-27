/**
 * Update Field Operators Tests
 *
 * Tests for all update operators: $set, $unset, $inc, $min, $max, $mul,
 * $rename, $currentDate, $setOnInsert.
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "../../test-harness.ts";

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
      await collection.insertOne({ name: "test", matrix: [[1, 2], [3, 4]] });

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

describe(`Additional Update Operators (${getTestModeName()})`, () => {
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
      assert.strictEqual(doc?.quantity, 0);
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
            err.message.includes("Cannot increment")
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

      await collection.updateMany({ type: "price" }, { $mul: { value: 0.9 } });

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
  const CLOCK_DRIFT_MS = 5000;

  describe("$currentDate operator", () => {
    it("should set field to current date with true", async () => {
      const collection = client.db(dbName).collection("currentdate_true");
      await collection.insertOne({ name: "Alice" });
      const before = new Date(Date.now() - CLOCK_DRIFT_MS);

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { lastModified: true } }
      );

      const after = new Date(Date.now() + CLOCK_DRIFT_MS);
      const doc = await collection.findOne({ name: "Alice" });
      assert.ok(doc?.lastModified instanceof Date);
      assert.ok((doc?.lastModified as Date) >= before);
      assert.ok((doc?.lastModified as Date) <= after);
    });

    it("should set field to current date with $type: date", async () => {
      const collection = client.db(dbName).collection("currentdate_type_date");
      await collection.insertOne({ name: "Alice" });
      const before = new Date(Date.now() - CLOCK_DRIFT_MS);

      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { lastModified: { $type: "date" } } }
      );

      const after = new Date(Date.now() + CLOCK_DRIFT_MS);
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
      const ts = doc?.lastModified;
      if (typeof ts === "number") {
        assert.ok(ts >= before && ts <= after);
      } else if (ts instanceof Date) {
        assert.ok(ts.getTime() >= before && ts.getTime() <= after);
      } else if (ts && typeof ts === "object") {
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
      assert.strictEqual((doc?.createdAt as Date).getTime(), oldDate.getTime());
      assert.strictEqual(doc?.role, undefined);
    });

    it("should be ignored without upsert option", async () => {
      const collection = client.db(dbName).collection("setoninsert_no_upsert");

      await collection.updateOne(
        { email: "nonexistent@test.com" },
        { $setOnInsert: { name: "Should Not Exist" } }
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

      await collection.updateOne(
        { name: "item" },
        {
          $inc: { count: 1 },
          $setOnInsert: { createdAt: initialDate },
        },
        { upsert: true }
      );

      const newDate = new Date("2024-01-01");
      await collection.updateOne(
        { name: "item" },
        {
          $inc: { count: 1 },
          $setOnInsert: { createdAt: newDate },
        },
        { upsert: true }
      );

      const doc = await collection.findOne({ name: "item" });
      assert.strictEqual(doc?.count, 2);
      assert.strictEqual(
        (doc?.createdAt as Date).getTime(),
        initialDate.getTime()
      );
    });
  });

  // ==================== Combined Operators ====================
  describe("combining operators", () => {
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
      const before = new Date(Date.now() - CLOCK_DRIFT_MS);

      await collection.updateOne(
        { name: "Alice" },
        {
          $currentDate: { updatedAt: true },
          $set: { status: "active" },
        }
      );

      const after = new Date(Date.now() + CLOCK_DRIFT_MS);
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
      await collection.updateOne(
        { name: "Alice" },
        { $currentDate: { lastModified: false as unknown as true } }
      );
      const afterUpdate = Date.now();

      const doc = await collection.findOne({ name: "Alice" });
      const updatedTime = (doc?.lastModified as Date).getTime();

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

      const result = await collection.updateOne(
        { name: "Alice" },
        { $min: { value: 100 } }
      );

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.value, null);
      assert.strictEqual(result.modifiedCount, 0);
    });

    it("$max should handle null current value using BSON ordering", async () => {
      const collection = client.db(dbName).collection("max_null_current");
      await collection.insertOne({ name: "Alice", value: null });

      await collection.updateOne({ name: "Alice" }, { $max: { value: 100 } });

      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.value, 100);
    });

    it("$min/$max should compare strings correctly", async () => {
      const collection = client.db(dbName).collection("min_max_strings");
      await collection.insertOne({ name: "Alice", code: "beta" });

      await collection.updateOne(
        { name: "Alice" },
        { $min: { code: "alpha" } }
      );

      let doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.code, "alpha");

      await collection.updateOne(
        { name: "Alice" },
        { $max: { code: "gamma" } }
      );

      doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.code, "gamma");
    });
  });

  describe("$pullAll operator", () => {
    it("should remove all matching values from array", async () => {
      const collection = client.db(dbName).collection("pullall_basic");
      await collection.insertOne({ scores: [0, 2, 5, 5, 1, 0] });

      await collection.updateOne({}, { $pullAll: { scores: [0, 5] } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.scores, [2, 1]);
    });

    it("should handle empty array to remove", async () => {
      const collection = client.db(dbName).collection("pullall_empty");
      await collection.insertOne({ items: [1, 2, 3] });

      await collection.updateOne({}, { $pullAll: { items: [] } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.items, [1, 2, 3]);
    });

    it("should handle no matching values", async () => {
      const collection = client.db(dbName).collection("pullall_nomatch");
      await collection.insertOne({ items: [1, 2, 3] });

      await collection.updateOne({}, { $pullAll: { items: [4, 5, 6] } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.items, [1, 2, 3]);
    });

    it("should remove all occurrences of matching values", async () => {
      const collection = client.db(dbName).collection("pullall_multiple");
      await collection.insertOne({ tags: ["a", "b", "a", "c", "a"] });

      await collection.updateOne({}, { $pullAll: { tags: ["a"] } });

      const doc = await collection.findOne({});
      assert.deepStrictEqual(doc?.tags, ["b", "c"]);
    });
  });

  describe("$push array modifiers", () => {
    describe("$position modifier", () => {
      it("should insert at specified position", async () => {
        const collection = client.db(dbName).collection("push_position");
        await collection.insertOne({ items: ["a", "b", "c"] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: ["x", "y"], $position: 1 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, ["a", "x", "y", "b", "c"]);
      });

      it("should insert at beginning with position 0", async () => {
        const collection = client.db(dbName).collection("push_position_zero");
        await collection.insertOne({ items: [1, 2, 3] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: [0], $position: 0 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [0, 1, 2, 3]);
      });

      it("should handle negative position (from end)", async () => {
        const collection = client.db(dbName).collection("push_position_neg");
        await collection.insertOne({ items: ["a", "b", "c"] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: ["x"], $position: -1 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, ["a", "b", "x", "c"]);
      });

      it("should append if position exceeds array length", async () => {
        const collection = client.db(dbName).collection("push_position_exceed");
        await collection.insertOne({ items: [1, 2] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: [3], $position: 100 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [1, 2, 3]);
      });
    });

    describe("$slice modifier", () => {
      it("should keep first N elements with positive slice", async () => {
        const collection = client.db(dbName).collection("push_slice_pos");
        await collection.insertOne({ items: [1, 2, 3] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: [4, 5, 6], $slice: 4 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [1, 2, 3, 4]);
      });

      it("should keep last N elements with negative slice", async () => {
        const collection = client.db(dbName).collection("push_slice_neg");
        await collection.insertOne({ items: [1, 2, 3] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: [4, 5, 6], $slice: -3 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [4, 5, 6]);
      });

      it("should remove all elements with slice 0", async () => {
        const collection = client.db(dbName).collection("push_slice_zero");
        await collection.insertOne({ items: [1, 2, 3] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: [4, 5], $slice: 0 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, []);
      });

      it("should not truncate if slice exceeds array length", async () => {
        const collection = client.db(dbName).collection("push_slice_exceed");
        await collection.insertOne({ items: [1, 2] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: [3], $slice: 10 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [1, 2, 3]);
      });
    });

    describe("$sort modifier", () => {
      it("should sort array ascending with $sort: 1", async () => {
        const collection = client.db(dbName).collection("push_sort_asc");
        await collection.insertOne({ items: [3, 1, 2] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: [5, 4], $sort: 1 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [1, 2, 3, 4, 5]);
      });

      it("should sort array descending with $sort: -1", async () => {
        const collection = client.db(dbName).collection("push_sort_desc");
        await collection.insertOne({ items: [3, 1, 2] });

        await collection.updateOne(
          {},
          { $push: { items: { $each: [5, 4], $sort: -1 } } }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [5, 4, 3, 2, 1]);
      });

      it("should sort objects by field", async () => {
        const collection = client.db(dbName).collection("push_sort_field");
        await collection.insertOne({
          items: [{ score: 80 }, { score: 60 }],
        });

        await collection.updateOne(
          {},
          {
            $push: {
              items: {
                $each: [{ score: 70 }, { score: 90 }],
                $sort: { score: 1 },
              },
            },
          }
        );

        const doc = await collection.findOne({});
        const scores = (doc?.items as { score: number }[]).map((i) => i.score);
        assert.deepStrictEqual(scores, [60, 70, 80, 90]);
      });

      it("should sort objects by field descending", async () => {
        const collection = client.db(dbName).collection("push_sort_field_desc");
        await collection.insertOne({
          items: [{ name: "B" }, { name: "C" }],
        });

        await collection.updateOne(
          {},
          {
            $push: {
              items: {
                $each: [{ name: "A" }, { name: "D" }],
                $sort: { name: -1 },
              },
            },
          }
        );

        const doc = await collection.findOne({});
        const names = (doc?.items as { name: string }[]).map((i) => i.name);
        assert.deepStrictEqual(names, ["D", "C", "B", "A"]);
      });
    });

    describe("Combined modifiers", () => {
      it("should apply $position, $sort, and $slice together", async () => {
        const collection = client.db(dbName).collection("push_combined");
        await collection.insertOne({ items: [5, 3, 7] });

        await collection.updateOne(
          {},
          {
            $push: {
              items: {
                $each: [1, 9, 2],
                $sort: -1,
                $slice: 4,
              },
            },
          }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [9, 7, 5, 3]);
      });

      it("should apply $position before $slice", async () => {
        const collection = client.db(dbName).collection("push_pos_slice");
        await collection.insertOne({ items: ["a", "b", "c"] });

        await collection.updateOne(
          {},
          {
            $push: {
              items: {
                $each: ["x"],
                $position: 0,
                $slice: 3,
              },
            },
          }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, ["x", "a", "b"]);
      });

      it("should handle empty $each with $slice", async () => {
        const collection = client.db(dbName).collection("push_empty_slice");
        await collection.insertOne({ items: [1, 2, 3, 4, 5] });

        await collection.updateOne(
          {},
          {
            $push: {
              items: {
                $each: [],
                $slice: 3,
              },
            },
          }
        );

        const doc = await collection.findOne({});
        assert.deepStrictEqual(doc?.items, [1, 2, 3]);
      });
    });
  });

  describe("$bit operator", () => {
    it("should apply bitwise AND", async () => {
      const collection = client.db(dbName).collection("bit_and");
      await collection.insertOne({ flags: 13 });

      await collection.updateOne({}, { $bit: { flags: { and: 10 } } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.flags, 8);
    });

    it("should apply bitwise OR", async () => {
      const collection = client.db(dbName).collection("bit_or");
      await collection.insertOne({ flags: 5 });

      await collection.updateOne({}, { $bit: { flags: { or: 2 } } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.flags, 7);
    });

    it("should apply bitwise XOR", async () => {
      const collection = client.db(dbName).collection("bit_xor");
      await collection.insertOne({ flags: 5 });

      await collection.updateOne({}, { $bit: { flags: { xor: 3 } } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.flags, 6);
    });

    it("should apply multiple bitwise operations", async () => {
      const collection = client.db(dbName).collection("bit_multiple");
      await collection.insertOne({ flags: 15 });

      await collection.updateOne({}, { $bit: { flags: { and: 12, or: 3 } } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.flags, 15);
    });

    it("should initialize missing field to 0", async () => {
      const collection = client.db(dbName).collection("bit_missing");
      await collection.insertOne({ other: "value" });

      await collection.updateOne({}, { $bit: { flags: { or: 5 } } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.flags, 5);
    });
  });
});
