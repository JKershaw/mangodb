/**
 * Phase 1: Foundation Tests
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
} from "./test-harness.ts";
import { ObjectId } from "bson";

describe(`Foundation Tests (${getTestModeName()})`, () => {
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

  describe("insertOne", () => {
    it("should insert a document and return insertedId", async () => {
      const collection = client.db(dbName).collection("test_insert_one");

      const result = await collection.insertOne({ name: "Alice", age: 30 });

      assert.strictEqual(result.acknowledged, true);
      assert.ok(result.insertedId, "insertedId should be defined");
    });

    it("should auto-generate _id when not provided", async () => {
      const collection = client.db(dbName).collection("test_insert_one_auto_id");

      const result = await collection.insertOne({ name: "Bob" });
      const doc = await collection.findOne({});

      assert.ok(doc, "Document should be found");
      assert.ok(doc._id, "_id should be auto-generated");
      assert.strictEqual(
        (result.insertedId as ObjectId).toString(),
        (doc._id as ObjectId).toString()
      );
    });

    it("should use provided _id", async () => {
      const collection = client.db(dbName).collection("test_insert_one_custom_id");
      const customId = new ObjectId();

      const result = await collection.insertOne({ _id: customId, name: "Charlie" });

      assert.strictEqual((result.insertedId as ObjectId).toString(), customId.toString());

      const doc = await collection.findOne({});
      assert.ok(doc, "Document should be found");
      assert.strictEqual((doc._id as ObjectId).toString(), customId.toString());
    });
  });

  describe("insertMany", () => {
    it("should insert multiple documents", async () => {
      const collection = client.db(dbName).collection("test_insert_many");

      const result = await collection.insertMany([
        { name: "Alice" },
        { name: "Bob" },
        { name: "Charlie" },
      ]);

      assert.strictEqual(result.acknowledged, true);
      assert.ok(result.insertedIds, "insertedIds should be defined");
      assert.ok(result.insertedIds[0], "First insertedId should be defined");
      assert.ok(result.insertedIds[1], "Second insertedId should be defined");
      assert.ok(result.insertedIds[2], "Third insertedId should be defined");
    });

    it("should return insertedIds as object with numeric keys", async () => {
      const collection = client.db(dbName).collection("test_insert_many_keys");

      const result = await collection.insertMany([{ a: 1 }, { b: 2 }]);

      // MongoDB returns insertedIds as { 0: ObjectId, 1: ObjectId }, not an array
      assert.strictEqual(typeof result.insertedIds, "object");
      assert.ok("0" in result.insertedIds || 0 in result.insertedIds);
      assert.ok("1" in result.insertedIds || 1 in result.insertedIds);
    });
  });

  describe("findOne", () => {
    it("should return null when collection is empty", async () => {
      const collection = client.db(dbName).collection("test_find_one_empty");

      const doc = await collection.findOne({});

      assert.strictEqual(doc, null);
    });

    it("should return first document with empty filter", async () => {
      const collection = client.db(dbName).collection("test_find_one_first");
      await collection.insertMany([{ order: 1 }, { order: 2 }, { order: 3 }]);

      const doc = await collection.findOne({});

      assert.ok(doc, "Document should be found");
      assert.strictEqual(doc.order, 1);
    });

    it("should find document by simple equality", async () => {
      const collection = client.db(dbName).collection("test_find_one_equality");
      await collection.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 25 },
      ]);

      const doc = await collection.findOne({ name: "Bob" });

      assert.ok(doc, "Document should be found");
      assert.strictEqual(doc.name, "Bob");
      assert.strictEqual(doc.age, 25);
    });

    it("should return null when no match found", async () => {
      const collection = client.db(dbName).collection("test_find_one_no_match");
      await collection.insertOne({ name: "Alice" });

      const doc = await collection.findOne({ name: "NonExistent" });

      assert.strictEqual(doc, null);
    });
  });

  describe("find", () => {
    it("should return empty array when collection is empty", async () => {
      const collection = client.db(dbName).collection("test_find_empty");

      const docs = await collection.find({}).toArray();

      assert.deepStrictEqual(docs, []);
    });

    it("should return all documents with empty filter", async () => {
      const collection = client.db(dbName).collection("test_find_all");
      await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);

      const docs = await collection.find({}).toArray();

      assert.strictEqual(docs.length, 3);
    });

    it("should filter documents by simple equality", async () => {
      const collection = client.db(dbName).collection("test_find_filter");
      await collection.insertMany([
        { category: "A", value: 1 },
        { category: "B", value: 2 },
        { category: "A", value: 3 },
      ]);

      const docs = await collection.find({ category: "A" }).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((doc) => doc.category === "A"));
    });

    it("should preserve insertion order", async () => {
      const collection = client.db(dbName).collection("test_find_order");
      await collection.insertMany([
        { order: 1 },
        { order: 2 },
        { order: 3 },
      ]);

      const docs = await collection.find({}).toArray();

      assert.strictEqual(docs[0].order, 1);
      assert.strictEqual(docs[1].order, 2);
      assert.strictEqual(docs[2].order, 3);
    });
  });

  describe("deleteOne", () => {
    it("should return deletedCount 0 when no match", async () => {
      const collection = client.db(dbName).collection("test_delete_one_no_match");

      const result = await collection.deleteOne({ name: "NonExistent" });

      assert.strictEqual(result.acknowledged, true);
      assert.strictEqual(result.deletedCount, 0);
    });

    it("should delete single matching document", async () => {
      const collection = client.db(dbName).collection("test_delete_one_single");
      await collection.insertMany([
        { name: "Alice" },
        { name: "Bob" },
      ]);

      const result = await collection.deleteOne({ name: "Alice" });

      assert.strictEqual(result.acknowledged, true);
      assert.strictEqual(result.deletedCount, 1);

      const remaining = await collection.find({}).toArray();
      assert.strictEqual(remaining.length, 1);
      assert.strictEqual(remaining[0].name, "Bob");
    });

    it("should delete only first match when multiple exist", async () => {
      const collection = client.db(dbName).collection("test_delete_one_multiple");
      await collection.insertMany([
        { category: "A", order: 1 },
        { category: "A", order: 2 },
        { category: "A", order: 3 },
      ]);

      const result = await collection.deleteOne({ category: "A" });

      assert.strictEqual(result.deletedCount, 1);

      const remaining = await collection.find({}).toArray();
      assert.strictEqual(remaining.length, 2);
      // First one should be deleted, leaving order 2 and 3
      assert.strictEqual(remaining[0].order, 2);
      assert.strictEqual(remaining[1].order, 3);
    });
  });

  describe("deleteMany", () => {
    it("should return deletedCount 0 when no match", async () => {
      const collection = client.db(dbName).collection("test_delete_many_no_match");
      await collection.insertOne({ name: "Alice" });

      const result = await collection.deleteMany({ name: "NonExistent" });

      assert.strictEqual(result.acknowledged, true);
      assert.strictEqual(result.deletedCount, 0);
    });

    it("should delete all matching documents", async () => {
      const collection = client.db(dbName).collection("test_delete_many_all");
      await collection.insertMany([
        { category: "A", value: 1 },
        { category: "B", value: 2 },
        { category: "A", value: 3 },
        { category: "A", value: 4 },
      ]);

      const result = await collection.deleteMany({ category: "A" });

      assert.strictEqual(result.acknowledged, true);
      assert.strictEqual(result.deletedCount, 3);

      const remaining = await collection.find({}).toArray();
      assert.strictEqual(remaining.length, 1);
      assert.strictEqual(remaining[0].category, "B");
    });

    it("should delete all documents with empty filter", async () => {
      const collection = client.db(dbName).collection("test_delete_many_empty");
      await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);

      const result = await collection.deleteMany({});

      assert.strictEqual(result.deletedCount, 3);

      const remaining = await collection.find({}).toArray();
      assert.strictEqual(remaining.length, 0);
    });
  });

  describe("ObjectId handling", () => {
    it("should find document by _id", async () => {
      const collection = client.db(dbName).collection("test_object_id");
      const insertResult = await collection.insertOne({ name: "Test" });
      const insertedId = insertResult.insertedId;

      const doc = await collection.findOne({ _id: insertedId });

      assert.ok(doc, "Document should be found");
      assert.strictEqual(doc.name, "Test");
    });

    it("should delete document by _id", async () => {
      const collection = client.db(dbName).collection("test_object_id_delete");
      await collection.insertMany([
        { name: "First" },
        { name: "Second" },
      ]);

      const first = await collection.findOne({ name: "First" });
      assert.ok(first, "First document should exist");

      const result = await collection.deleteOne({ _id: first._id });

      assert.strictEqual(result.deletedCount, 1);

      const remaining = await collection.find({}).toArray();
      assert.strictEqual(remaining.length, 1);
      assert.strictEqual(remaining[0].name, "Second");
    });
  });

  describe("Data persistence", () => {
    it("should persist data across operations", async () => {
      const collection = client.db(dbName).collection("test_persistence");

      await collection.insertOne({ step: 1 });
      await collection.insertOne({ step: 2 });

      const docs1 = await collection.find({}).toArray();
      assert.strictEqual(docs1.length, 2);

      await collection.deleteOne({ step: 1 });

      const docs2 = await collection.find({}).toArray();
      assert.strictEqual(docs2.length, 1);
      assert.strictEqual(docs2[0].step, 2);
    });
  });
});
