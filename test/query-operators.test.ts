/**
 * Phase 12: Query Operators Tests
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
} from "./test-harness.ts";

describe(`Query Operators Tests (${getTestModeName()})`, () => {
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

  describe("$text Operator", () => {
    describe("Text Index Creation", () => {
      it("should create a text index on a single field", async () => {
        const collection = client.db(dbName).collection("text_idx_single");

        const indexName = await collection.createIndex({ title: "text" } as any);

        assert.strictEqual(indexName, "title_text");
      });

      it("should create a compound text index on multiple fields", async () => {
        const collection = client.db(dbName).collection("text_idx_compound");

        const indexName = await collection.createIndex({
          title: "text",
          description: "text"
        } as any);

        assert.strictEqual(indexName, "title_text_description_text");
      });
    });

    describe("Basic Text Search", () => {
      it("should find documents containing a single word", async () => {
        const collection = client.db(dbName).collection("text_basic_single");
        await collection.createIndex({ content: "text" } as any);
        await collection.insertMany([
          { content: "MongoDB is a document database" },
          { content: "PostgreSQL is a relational database" },
          { content: "Redis is an in-memory store" },
        ]);

        const docs = await collection.find({
          $text: { $search: "MongoDB" }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.ok((docs[0].content as string).includes("MongoDB"));
      });

      it("should find documents containing any of multiple words", async () => {
        const collection = client.db(dbName).collection("text_basic_multi");
        await collection.createIndex({ content: "text" } as any);
        await collection.insertMany([
          { content: "MongoDB is a document database" },
          { content: "PostgreSQL is a relational database" },
          { content: "Redis is an in-memory store" },
        ]);

        const docs = await collection.find({
          $text: { $search: "MongoDB Redis" }
        } as any).toArray();

        assert.strictEqual(docs.length, 2);
      });

      it("should be case-insensitive by default", async () => {
        const collection = client.db(dbName).collection("text_case");
        await collection.createIndex({ title: "text" } as any);
        await collection.insertMany([
          { title: "MONGODB Tutorial" },
          { title: "mongodb basics" },
          { title: "MongoDb Advanced" },
        ]);

        const docs = await collection.find({
          $text: { $search: "mongodb" }
        } as any).toArray();

        assert.strictEqual(docs.length, 3);
      });

      it("should search across multiple fields in compound text index", async () => {
        const collection = client.db(dbName).collection("text_compound_search");
        await collection.createIndex({ title: "text", body: "text" } as any);
        await collection.insertMany([
          { title: "MongoDB Guide", body: "Learn about databases" },
          { title: "Python Tutorial", body: "MongoDB integration" },
          { title: "Redis Cache", body: "Fast storage" },
        ]);

        const docs = await collection.find({
          $text: { $search: "MongoDB" }
        } as any).toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe("Edge Cases", () => {
      it("should return empty array for no matches", async () => {
        const collection = client.db(dbName).collection("text_no_match");
        await collection.createIndex({ content: "text" } as any);
        await collection.insertMany([
          { content: "Hello world" },
          { content: "Goodbye world" },
        ]);

        const docs = await collection.find({
          $text: { $search: "nothing" }
        } as any).toArray();

        assert.strictEqual(docs.length, 0);
      });

      it("should not match null field values", async () => {
        const collection = client.db(dbName).collection("text_null");
        await collection.createIndex({ content: "text" } as any);
        await collection.insertMany([
          { content: null },
          { content: "some text" },
          { other: "field" }, // missing content field
        ]);

        const docs = await collection.find({
          $text: { $search: "text" }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].content, "some text");
      });

      it("should handle empty search string", async () => {
        const collection = client.db(dbName).collection("text_empty_search");
        await collection.createIndex({ content: "text" } as any);
        await collection.insertMany([
          { content: "Hello world" },
        ]);

        const docs = await collection.find({
          $text: { $search: "" }
        } as any).toArray();

        // MongoDB returns empty for empty search
        assert.strictEqual(docs.length, 0);
      });

      it("should work with other query conditions", async () => {
        const collection = client.db(dbName).collection("text_with_filter");
        await collection.createIndex({ content: "text" } as any);
        await collection.insertMany([
          { content: "MongoDB tutorial", category: "database" },
          { content: "MongoDB advanced", category: "database" },
          { content: "MongoDB basics", category: "beginner" },
        ]);

        const docs = await collection.find({
          $text: { $search: "MongoDB" },
          category: "database"
        } as any).toArray();

        assert.strictEqual(docs.length, 2);
        assert.ok(docs.every(d => d.category === "database"));
      });
    });

    describe("Error Cases", () => {
      it("should throw error when no text index exists", async () => {
        const collection = client.db(dbName).collection("text_no_index");
        await collection.insertOne({ content: "some text" });

        await assert.rejects(
          async () => {
            await collection.find({
              $text: { $search: "some" }
            } as any).toArray();
          },
          (err: Error) => {
            // MongoDB error: "text index required for $text query"
            return err.message.includes("text index required");
          }
        );
      });
    });
  });
});
