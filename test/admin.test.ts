/**
 * Phase 15: Administrative Operations Tests
 *
 * These tests verify administrative operations for database and collection management:
 * - db.listCollections() - list all collections
 * - db.stats() - database statistics
 * - collection.drop() - drop collection
 * - collection.rename() - rename collection
 * - collection.stats() - collection statistics
 * - collection.distinct() - get distinct values
 * - collection.estimatedDocumentCount() - fast count without filter
 *
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  isMongoDBMode,
  type TestClient,
} from "./test-harness.ts";

describe(`Administrative Operations Tests (${getTestModeName()})`, () => {
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

  // ==================== estimatedDocumentCount ====================
  describe("estimatedDocumentCount", () => {
    it("should return count of documents", async () => {
      const collection = client.db(dbName).collection("estimated_count_basic");
      await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);

      const count = await collection.estimatedDocumentCount();

      assert.strictEqual(count, 3);
    });

    it("should return 0 for empty collection", async () => {
      const collection = client.db(dbName).collection("estimated_count_empty");

      const count = await collection.estimatedDocumentCount();

      assert.strictEqual(count, 0);
    });

    it("should reflect inserts and deletes", async () => {
      const collection = client.db(dbName).collection("estimated_count_changes");
      await collection.insertMany([{ x: 1 }, { x: 2 }, { x: 3 }]);

      let count = await collection.estimatedDocumentCount();
      assert.strictEqual(count, 3);

      await collection.deleteOne({ x: 2 });
      count = await collection.estimatedDocumentCount();
      assert.strictEqual(count, 2);

      await collection.insertOne({ x: 4 });
      count = await collection.estimatedDocumentCount();
      assert.strictEqual(count, 3);
    });

    it("should match countDocuments for no filter", async () => {
      const collection = client.db(dbName).collection("estimated_count_match");
      await collection.insertMany([{ v: 10 }, { v: 20 }, { v: 30 }, { v: 40 }]);

      const estimated = await collection.estimatedDocumentCount();
      const accurate = await collection.countDocuments({});

      assert.strictEqual(estimated, accurate);
    });
  });

  // ==================== distinct ====================
  describe("distinct", () => {
    it("should return unique values", async () => {
      const collection = client.db(dbName).collection("distinct_basic");
      await collection.insertMany([
        { category: "A" },
        { category: "B" },
        { category: "A" },
        { category: "C" },
        { category: "B" },
      ]);

      const values = await collection.distinct("category");

      assert.strictEqual(values.length, 3);
      assert.ok(values.includes("A"));
      assert.ok(values.includes("B"));
      assert.ok(values.includes("C"));
    });

    it("should return empty array for empty collection", async () => {
      const collection = client.db(dbName).collection("distinct_empty");

      const values = await collection.distinct("field");

      assert.deepStrictEqual(values, []);
    });

    it("should support filter parameter", async () => {
      const collection = client.db(dbName).collection("distinct_filter");
      await collection.insertMany([
        { status: "active", type: "A" },
        { status: "active", type: "B" },
        { status: "inactive", type: "C" },
        { status: "active", type: "A" },
      ]);

      const values = await collection.distinct("type", { status: "active" });

      assert.strictEqual(values.length, 2);
      assert.ok(values.includes("A"));
      assert.ok(values.includes("B"));
      assert.ok(!values.includes("C"));
    });

    it("should treat array elements as separate values", async () => {
      const collection = client.db(dbName).collection("distinct_array");
      await collection.insertMany([
        { tags: ["red", "blue"] },
        { tags: ["green", "red"] },
        { tags: ["blue", "yellow"] },
      ]);

      const values = await collection.distinct("tags");

      assert.strictEqual(values.length, 4);
      assert.ok(values.includes("red"));
      assert.ok(values.includes("blue"));
      assert.ok(values.includes("green"));
      assert.ok(values.includes("yellow"));
    });

    it("should skip missing fields (undefined)", async () => {
      const collection = client.db(dbName).collection("distinct_missing");
      await collection.insertMany([
        { name: "Alice", category: "A" },
        { name: "Bob" }, // No category
        { name: "Charlie", category: "B" },
      ]);

      const values = await collection.distinct("category");

      assert.strictEqual(values.length, 2);
      assert.ok(values.includes("A"));
      assert.ok(values.includes("B"));
    });

    it("should include null as a distinct value", async () => {
      const collection = client.db(dbName).collection("distinct_null");
      await collection.insertMany([
        { status: "active" },
        { status: null },
        { status: "inactive" },
      ]);

      const values = await collection.distinct("status");

      assert.strictEqual(values.length, 3);
      assert.ok(values.includes("active"));
      assert.ok(values.includes("inactive"));
      assert.ok(values.includes(null));
    });

    it("should support nested fields with dot notation", async () => {
      const collection = client.db(dbName).collection("distinct_nested");
      await collection.insertMany([
        { user: { role: "admin" } },
        { user: { role: "user" } },
        { user: { role: "admin" } },
        { user: { role: "moderator" } },
      ]);

      const values = await collection.distinct("user.role");

      assert.strictEqual(values.length, 3);
      assert.ok(values.includes("admin"));
      assert.ok(values.includes("user"));
      assert.ok(values.includes("moderator"));
    });

    it("should handle mixed types", async () => {
      const collection = client.db(dbName).collection("distinct_mixed");
      await collection.insertMany([
        { value: 1 },
        { value: "1" },
        { value: 1 },
        { value: true },
      ]);

      const values = await collection.distinct("value");

      assert.strictEqual(values.length, 3);
      assert.ok(values.includes(1));
      assert.ok(values.includes("1"));
      assert.ok(values.includes(true));
    });
  });

  // ==================== collection.drop ====================
  describe("collection.drop", () => {
    it("should drop existing collection", async () => {
      const collection = client.db(dbName).collection("drop_existing");
      await collection.insertMany([{ a: 1 }, { b: 2 }]);

      const result = await collection.drop();

      assert.strictEqual(result, true);

      // Collection should now be empty
      const docs = await collection.find({}).toArray();
      assert.strictEqual(docs.length, 0);
    });

    it("should return true for non-existent collection", async () => {
      const collection = client.db(dbName).collection("drop_nonexistent_" + Date.now());

      const result = await collection.drop();

      assert.strictEqual(result, true);
    });

    it("should remove indexes when dropping", async () => {
      const collection = client.db(dbName).collection("drop_indexes");
      await collection.insertOne({ email: "test@test.com" });
      await collection.createIndex({ email: 1 }, { unique: true });

      // Verify index exists
      let indexes = await collection.indexes();
      assert.ok(indexes.some((i) => i.name === "email_1"));

      await collection.drop();

      // Re-create collection by inserting a document (needed for MongoDB mode)
      await collection.insertOne({ email: "new@test.com" });

      // After drop and re-create, should only have _id_ index
      indexes = await collection.indexes();
      assert.strictEqual(indexes.length, 1);
      assert.strictEqual(indexes[0].name, "_id_");
    });
  });

  // ==================== collection.stats ====================
  // Note: MongoDB driver doesn't have collection.stats() method directly.
  // These tests only run in MangoDB mode.
  describe("collection.stats", { skip: isMongoDBMode() }, () => {
    it("should return stats object", async () => {
      const collection = client.db(dbName).collection("stats_basic");
      await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);

      const stats = await collection.stats();

      assert.strictEqual(stats.ok, 1);
      assert.strictEqual(typeof stats.count, "number");
      assert.strictEqual(stats.count, 3);
    });

    it("should include namespace", async () => {
      const collection = client.db(dbName).collection("stats_namespace");
      await collection.insertOne({ x: 1 });

      const stats = await collection.stats();

      assert.ok(stats.ns.includes("stats_namespace"));
    });

    it("should include index count", async () => {
      const collection = client.db(dbName).collection("stats_indexes");
      await collection.insertOne({ email: "test@test.com" });
      await collection.createIndex({ email: 1 });

      const stats = await collection.stats();

      // At least _id_ and email_1
      assert.ok(stats.nindexes >= 2);
    });

    it("should include size information", async () => {
      const collection = client.db(dbName).collection("stats_size");
      await collection.insertMany([{ data: "test" }, { data: "more" }]);

      const stats = await collection.stats();

      assert.strictEqual(typeof stats.size, "number");
      assert.strictEqual(typeof stats.storageSize, "number");
      assert.strictEqual(typeof stats.totalSize, "number");
    });

    it("should return ok: 1", async () => {
      const collection = client.db(dbName).collection("stats_ok");

      const stats = await collection.stats();

      assert.strictEqual(stats.ok, 1);
    });
  });

  // ==================== collection.rename ====================
  describe("collection.rename", () => {
    it("should rename collection", async () => {
      const collection = client.db(dbName).collection("rename_source");
      await collection.insertMany([{ x: 1 }, { x: 2 }]);

      const renamed = await collection.rename("rename_target");

      // Verify data is in new collection
      const docs = await renamed.find({}).toArray();
      assert.strictEqual(docs.length, 2);
    });

    it("should preserve documents", async () => {
      const collection = client.db(dbName).collection("rename_preserve_src");
      await collection.insertMany([
        { name: "Alice", score: 100 },
        { name: "Bob", score: 85 },
      ]);

      const renamed = await collection.rename("rename_preserve_dst");
      const docs = await renamed.find({}).toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.name === "Alice"));
      assert.ok(docs.some((d) => d.name === "Bob"));
    });

    it("should preserve indexes", async () => {
      const collection = client.db(dbName).collection("rename_idx_src");
      await collection.insertOne({ email: "test@test.com" });
      await collection.createIndex({ email: 1 }, { unique: true });

      const renamed = await collection.rename("rename_idx_dst");
      const indexes = await renamed.indexes();

      assert.ok(indexes.some((i) => i.name === "email_1"));
    });

    it("should error when renaming to same name", async () => {
      const collection = client.db(dbName).collection("rename_same");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        async () => {
          await collection.rename("rename_same");
        },
        (err: Error) => {
          // MongoDB: "Can't rename a collection to itself"
          // MangoDB: "cannot rename collection to itself"
          return err.message.toLowerCase().includes("rename") &&
                 err.message.toLowerCase().includes("itself");
        }
      );
    });

    it("should error when target exists without dropTarget", async () => {
      const collection1 = client.db(dbName).collection("rename_conflict_src");
      const collection2 = client.db(dbName).collection("rename_conflict_dst");
      await collection1.insertOne({ a: 1 });
      await collection2.insertOne({ b: 2 });

      await assert.rejects(
        async () => {
          await collection1.rename("rename_conflict_dst");
        },
        (err: Error & { code?: number }) => {
          // MongoDB returns code 48 (NamespaceExists)
          return err.code === 48 || err.message.includes("exists");
        }
      );
    });

    it("should succeed with dropTarget: true", async () => {
      const collection1 = client.db(dbName).collection("rename_drop_src");
      const collection2 = client.db(dbName).collection("rename_drop_dst");
      await collection1.insertMany([{ source: true }, { source: true }]);
      await collection2.insertOne({ target: true });

      const renamed = await collection1.rename("rename_drop_dst", { dropTarget: true });
      const docs = await renamed.find({}).toArray();

      // Should only have source docs, target was dropped
      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => d.source === true));
    });

    it("should error for non-existent source (code 26)", async () => {
      const collection = client.db(dbName).collection("rename_nonexistent_" + Date.now());

      await assert.rejects(
        async () => {
          await collection.rename("rename_new_target");
        },
        (err: Error & { code?: number }) => {
          // MongoDB returns code 26 (NamespaceNotFound)
          return err.code === 26 || err.message.includes("not exist") || err.message.includes("not found");
        }
      );
    });

    it("should error for empty name", async () => {
      const collection = client.db(dbName).collection("rename_empty_name");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        async () => {
          await collection.rename("");
        },
        (err: Error) => {
          return err.message.includes("empty") || err.message.includes("Invalid");
        }
      );
    });

    it("should error for name starting with dot", async () => {
      const collection = client.db(dbName).collection("rename_dot_start");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        async () => {
          await collection.rename(".invalid");
        },
        (err: Error) => {
          return err.message.includes("start") || err.message.includes("Invalid") || err.message.includes(".");
        }
      );
    });

    it("should error for name containing $", async () => {
      const collection = client.db(dbName).collection("rename_dollar");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        async () => {
          await collection.rename("invalid$name");
        },
        (err: Error) => {
          return err.message.includes("$") || err.message.includes("Invalid");
        }
      );
    });
  });

  // ==================== db.listCollections ====================
  describe("db.listCollections", () => {
    it("should list all collections", async () => {
      const db = client.db(dbName);
      // Create some collections
      await db.collection("list_coll_a").insertOne({ a: 1 });
      await db.collection("list_coll_b").insertOne({ b: 2 });
      await db.collection("list_coll_c").insertOne({ c: 3 });

      const collections = await db.listCollections().toArray();

      const names = collections.map((c) => c.name);
      assert.ok(names.includes("list_coll_a"));
      assert.ok(names.includes("list_coll_b"));
      assert.ok(names.includes("list_coll_c"));
    });

    it("should return empty for empty database", async () => {
      // Create a fresh client with a new database
      const freshResult = await createTestClient();
      const freshClient = freshResult.client;
      const freshDbName = freshResult.dbName;
      await freshClient.connect();

      try {
        const collections = await freshClient.db(freshDbName).listCollections().toArray();
        assert.strictEqual(collections.length, 0);
      } finally {
        await freshResult.cleanup();
      }
    });

    it("should support filter by name", async () => {
      const db = client.db(dbName);
      await db.collection("filter_test_users").insertOne({ a: 1 });
      await db.collection("filter_test_orders").insertOne({ b: 2 });

      const collections = await db.listCollections({ name: "filter_test_users" }).toArray();

      assert.strictEqual(collections.length, 1);
      assert.strictEqual(collections[0].name, "filter_test_users");
    });

    it("should support regex filter", async () => {
      const db = client.db(dbName);
      await db.collection("regex_prefix_one").insertOne({ a: 1 });
      await db.collection("regex_prefix_two").insertOne({ b: 2 });
      await db.collection("other_collection").insertOne({ c: 3 });

      const collections = await db.listCollections({ name: { $regex: /^regex_prefix/ } }).toArray();

      const names = collections.map((c) => c.name);
      assert.ok(names.includes("regex_prefix_one"));
      assert.ok(names.includes("regex_prefix_two"));
      assert.ok(!names.includes("other_collection"));
    });

    it("should support nameOnly option", async () => {
      const db = client.db(dbName);
      await db.collection("nameonly_test").insertOne({ a: 1 });

      const collections = await db.listCollections({}, { nameOnly: true }).toArray();

      // nameOnly should return simplified objects
      assert.ok(collections.length > 0);
      for (const coll of collections) {
        assert.ok("name" in coll);
        assert.ok("type" in coll);
      }
    });

    it("should return cursor with toArray", async () => {
      const db = client.db(dbName);
      await db.collection("cursor_test").insertOne({ a: 1 });

      const cursor = db.listCollections();
      const collections = await cursor.toArray();

      assert.ok(Array.isArray(collections));
    });
  });

  // ==================== db.stats ====================
  describe("db.stats", () => {
    it("should return stats object", async () => {
      const db = client.db(dbName);
      await db.collection("dbstats_test").insertOne({ a: 1 });

      const stats = await db.stats();

      assert.strictEqual(stats.ok, 1);
      assert.strictEqual(typeof stats.collections, "number");
      assert.strictEqual(typeof stats.objects, "number");
    });

    it("should include database name", async () => {
      const db = client.db(dbName);
      await db.collection("dbstats_name").insertOne({ a: 1 });

      const stats = await db.stats();

      assert.strictEqual(stats.db, dbName);
    });

    it("should include collection count", async () => {
      const db = client.db(dbName);
      await db.collection("dbstats_count_a").insertOne({ a: 1 });
      await db.collection("dbstats_count_b").insertOne({ b: 2 });

      const stats = await db.stats();

      assert.ok(stats.collections >= 2);
    });

    it("should include total document count", async () => {
      const db = client.db(dbName);
      await db.collection("dbstats_docs").insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);

      const stats = await db.stats();

      assert.ok(stats.objects >= 3);
    });

    it("should include size information", async () => {
      const db = client.db(dbName);
      await db.collection("dbstats_size").insertOne({ data: "test" });

      const stats = await db.stats();

      assert.strictEqual(typeof stats.dataSize, "number");
      assert.strictEqual(typeof stats.storageSize, "number");
      assert.strictEqual(typeof stats.indexSize, "number");
    });

    it("should return ok: 1", async () => {
      const db = client.db(dbName);

      const stats = await db.stats();

      assert.strictEqual(stats.ok, 1);
    });
  });
});
