/**
 * Text Search Tests ($text and $meta)
 *
 * Tests for basic full-text search functionality.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
  type Document,
} from "./test-harness.ts";

describe(`Text Search (${getTestModeName()})`, () => {
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

  describe("$text query operator", () => {
    it("should find documents containing search term", async () => {
      const collection = client.db(dbName).collection("text_basic");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "coffee shop in downtown" },
        { _id: 2, content: "tea house on main street" },
        { _id: 3, content: "coffee and tea cafe" },
      ]);

      const results = await collection
        .find({ $text: { $search: "coffee" } })
        .toArray();

      assert.strictEqual(results.length, 2);
      const ids = results.map((r) => r._id).sort();
      assert.deepStrictEqual(ids, [1, 3]);
    });

    it("should find documents matching any word (OR)", async () => {
      const collection = client.db(dbName).collection("text_or");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "the quick brown fox" },
        { _id: 2, content: "lazy dog sleeps" },
        { _id: 3, content: "quick dog runs" },
      ]);

      const results = await collection
        .find({ $text: { $search: "fox dog" } })
        .toArray();

      assert.strictEqual(results.length, 3);
    });

    it("should be case insensitive", async () => {
      const collection = client.db(dbName).collection("text_case");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "UPPERCASE TEXT" },
        { _id: 2, content: "lowercase text" },
        { _id: 3, content: "MiXeD CaSe TeXt" },
      ]);

      const results = await collection
        .find({ $text: { $search: "text" } })
        .toArray();

      assert.strictEqual(results.length, 3);
    });

    it("should support phrase search with quotes", async () => {
      const collection = client.db(dbName).collection("text_phrase");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "new york city is great" },
        { _id: 2, content: "york is in new england" },
        { _id: 3, content: "the new york times" },
      ]);

      const results = await collection
        .find({ $text: { $search: '"new york"' } })
        .toArray();

      // Only docs with exact phrase "new york" adjacent
      assert.strictEqual(results.length, 2);
      const ids = results.map((r) => r._id).sort();
      assert.deepStrictEqual(ids, [1, 3]);
    });

    it("should support negation with minus", async () => {
      const collection = client.db(dbName).collection("text_negation");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "coffee with milk" },
        { _id: 2, content: "black coffee" },
        { _id: 3, content: "coffee and sugar" },
      ]);

      const results = await collection
        .find({ $text: { $search: "coffee -milk" } })
        .toArray();

      assert.strictEqual(results.length, 2);
      const ids = results.map((r) => r._id).sort();
      assert.deepStrictEqual(ids, [2, 3]);
    });

    it("should search across multiple text-indexed fields", async () => {
      const collection = client.db(dbName).collection("text_multi_field");
      await collection.createIndex({ title: "text", description: "text" });

      await collection.insertMany([
        { _id: 1, title: "Coffee Guide", description: "How to brew" },
        { _id: 2, title: "Tea Handbook", description: "All about coffee alternatives" },
        { _id: 3, title: "Water Facts", description: "Pure hydration" },
      ]);

      const results = await collection
        .find({ $text: { $search: "coffee" } })
        .toArray();

      assert.strictEqual(results.length, 2);
      const ids = results.map((r) => r._id).sort();
      assert.deepStrictEqual(ids, [1, 2]);
    });

    it("should return empty array when no matches", async () => {
      const collection = client.db(dbName).collection("text_no_match");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "apple pie" },
        { _id: 2, content: "banana bread" },
      ]);

      const results = await collection
        .find({ $text: { $search: "coffee" } })
        .toArray();

      assert.strictEqual(results.length, 0);
    });

    it("should work with other query conditions", async () => {
      const collection = client.db(dbName).collection("text_combined");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "coffee shop", category: "food" },
        { _id: 2, content: "coffee machine", category: "appliance" },
        { _id: 3, content: "tea shop", category: "food" },
      ]);

      const results = await collection
        .find({
          $text: { $search: "coffee" },
          category: "food",
        })
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0]._id, 1);
    });

    it("should handle $caseSensitive option", async () => {
      const collection = client.db(dbName).collection("text_case_sensitive");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "Coffee Shop" },
        { _id: 2, content: "coffee shop" },
        { _id: 3, content: "COFFEE SHOP" },
      ]);

      const results = await collection
        .find({ $text: { $search: "Coffee", $caseSensitive: true } })
        .toArray();

      // Only exact case match
      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0]._id, 1);
    });
  });

  describe("$meta projection", () => {
    it("should return text score with $meta", async () => {
      const collection = client.db(dbName).collection("meta_score");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "coffee coffee coffee" },
        { _id: 2, content: "coffee tea" },
        { _id: 3, content: "just coffee" },
      ]);

      const results = await collection
        .find(
          { $text: { $search: "coffee" } },
          { projection: { score: { $meta: "textScore" }, content: 1 } }
        )
        .toArray();

      assert.strictEqual(results.length, 3);
      // All results should have a score property
      assert.ok(results.every((r) => typeof (r as Document).score === "number"));
      // Doc with more "coffee" mentions should have higher score
      const doc1 = results.find((r) => r._id === 1) as Document | undefined;
      const doc2 = results.find((r) => r._id === 2) as Document | undefined;
      assert.ok(doc1 && doc2 && (doc1.score as number) > (doc2.score as number));
    });

    it("should allow sorting by text score", async () => {
      const collection = client.db(dbName).collection("meta_sort");
      await collection.createIndex({ content: "text" });

      await collection.insertMany([
        { _id: 1, content: "coffee" },
        { _id: 2, content: "coffee coffee coffee" },
        { _id: 3, content: "coffee coffee" },
      ]);

      const results = await collection
        .find(
          { $text: { $search: "coffee" } },
          { projection: { score: { $meta: "textScore" } } }
        )
        .sort({ score: { $meta: "textScore" } })
        .toArray();

      assert.strictEqual(results.length, 3);
      // Should be sorted by score descending (highest first)
      assert.strictEqual(results[0]._id, 2); // 3x coffee
      assert.strictEqual(results[1]._id, 3); // 2x coffee
      assert.strictEqual(results[2]._id, 1); // 1x coffee
    });
  });

  describe("text index requirements", () => {
    it("should error when no text index exists", async () => {
      const collection = client.db(dbName).collection("text_no_index");

      await collection.insertOne({ content: "some text" });

      await assert.rejects(
        async () => {
          await collection.find({ $text: { $search: "text" } }).toArray();
        },
        /text index/i
      );
    });
  });
});
