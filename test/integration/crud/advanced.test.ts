/**
 * Phase 8: Advanced Operations Tests
 *
 * Tests for findOneAndDelete, findOneAndReplace, findOneAndUpdate, and bulkWrite.
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "../../test-harness.ts";

describe(`Advanced Operations Tests (${getTestModeName()})`, () => {
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

  describe("findOneAndDelete", () => {
    it("should delete and return the document", async () => {
      const collection = client.db(dbName).collection("foad_basic");
      await collection.insertOne({ name: "Alice", age: 30 });

      const result = await collection.findOneAndDelete({ name: "Alice" });

      // Driver 6.0+: returns document directly, not wrapped in { value, ok }
      assert.strictEqual(result?.name, "Alice");
      assert.strictEqual(result?.age, 30);

      // Verify document is deleted
      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc, null);
    });

    it("should return null when no match", async () => {
      const collection = client.db(dbName).collection("foad_nomatch");
      await collection.insertOne({ name: "Alice" });

      const result = await collection.findOneAndDelete({ name: "Nobody" });

      assert.strictEqual(result, null);
    });

    it("should delete first in sort order when sort specified", async () => {
      const collection = client.db(dbName).collection("foad_sort");
      await collection.insertMany([
        { name: "A", priority: 1 },
        { name: "B", priority: 2 },
        { name: "C", priority: 3 },
      ]);

      // Delete highest priority (descending sort)
      const result = await collection.findOneAndDelete(
        {},
        { sort: { priority: -1 } }
      );

      assert.strictEqual(result?.name, "C");
      assert.strictEqual(result?.priority, 3);

      // Verify correct document deleted
      const remaining = await collection.find({}).toArray();
      assert.strictEqual(remaining.length, 2);
      assert.ok(remaining.every((d) => d.name !== "C"));
    });

    it("should apply projection to returned document", async () => {
      const collection = client.db(dbName).collection("foad_projection");
      await collection.insertOne({ name: "Alice", age: 30, city: "NYC" });

      const result = await collection.findOneAndDelete(
        { name: "Alice" },
        { projection: { name: 1, _id: 0 } }
      );

      assert.strictEqual(result?.name, "Alice");
      assert.strictEqual(result?.age, undefined);
      assert.strictEqual(result?._id, undefined);
    });

    it("should only delete first matching document", async () => {
      const collection = client.db(dbName).collection("foad_first_only");
      await collection.insertMany([
        { type: "fruit", name: "apple" },
        { type: "fruit", name: "banana" },
      ]);

      await collection.findOneAndDelete({ type: "fruit" });

      const remaining = await collection.find({ type: "fruit" }).toArray();
      assert.strictEqual(remaining.length, 1);
    });

    it("should sort by array field using minimum element (ascending)", async () => {
      const collection = client.db(dbName).collection("foad_array_sort");
      await collection.insertMany([
        { name: "A", scores: [5, 10, 15] }, // min: 5
        { name: "B", scores: [1, 20, 30] }, // min: 1
        { name: "C", scores: [8, 9, 10] }, // min: 8
      ]);

      // Ascending sort should use minimum element
      const result = await collection.findOneAndDelete(
        {},
        { sort: { scores: 1 } }
      );

      assert.strictEqual(result?.name, "B"); // min score 1
    });

    it("should sort by array field using maximum element (descending)", async () => {
      const collection = client.db(dbName).collection("foad_array_sort_desc");
      await collection.insertMany([
        { name: "A", scores: [5, 10, 15] }, // max: 15
        { name: "B", scores: [1, 20, 30] }, // max: 30
        { name: "C", scores: [8, 9, 10] }, // max: 10
      ]);

      // Descending sort should use maximum element
      const result = await collection.findOneAndDelete(
        {},
        { sort: { scores: -1 } }
      );

      assert.strictEqual(result?.name, "B"); // max score 30
    });

    it("should sort by boolean field correctly", async () => {
      const collection = client.db(dbName).collection("foad_bool_sort");
      await collection.insertMany([
        { name: "A", active: true },
        { name: "B", active: false },
        { name: "C", active: true },
      ]);

      // Ascending: false < true
      const result = await collection.findOneAndDelete(
        {},
        { sort: { active: 1 } }
      );

      assert.strictEqual(result?.name, "B"); // false comes first
      assert.strictEqual(result?.active, false);
    });
  });

  describe("findOneAndReplace", () => {
    it("should replace and return document before replacement", async () => {
      const collection = client.db(dbName).collection("foar_basic");
      await collection.insertOne({ name: "Alice", age: 30 });

      const result = await collection.findOneAndReplace(
        { name: "Alice" },
        { name: "Alice", age: 31, city: "NYC" }
      );

      assert.strictEqual(result?.age, 30); // Before replacement
      assert.strictEqual(result?.city, undefined);

      // Verify document is replaced
      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.age, 31);
      assert.strictEqual(doc?.city, "NYC");
    });

    it("should return document after replacement with returnDocument: after", async () => {
      const collection = client.db(dbName).collection("foar_after");
      await collection.insertOne({ name: "Bob", score: 100 });

      const result = await collection.findOneAndReplace(
        { name: "Bob" },
        { name: "Bob", score: 200 },
        { returnDocument: "after" }
      );

      assert.strictEqual(result?.score, 200);
    });

    it("should preserve _id when replacing", async () => {
      const collection = client.db(dbName).collection("foar_preserve_id");
      const insertResult = await collection.insertOne({ name: "Alice" });
      const originalId = insertResult.insertedId;

      await collection.findOneAndReplace(
        { name: "Alice" },
        { name: "Bob", newField: true }
      );

      const doc = await collection.findOne({ name: "Bob" });
      assert.strictEqual(
        (doc?._id as { toHexString(): string }).toHexString(),
        (originalId as { toHexString(): string }).toHexString()
      );
    });

    it("should return null when no match", async () => {
      const collection = client.db(dbName).collection("foar_nomatch");
      await collection.insertOne({ name: "Alice" });

      const result = await collection.findOneAndReplace(
        { name: "Nobody" },
        { name: "Bob" }
      );

      assert.strictEqual(result, null);
    });

    it("should upsert when no match and upsert: true", async () => {
      const collection = client.db(dbName).collection("foar_upsert");

      const result = await collection.findOneAndReplace(
        { name: "NewUser" },
        { name: "NewUser", created: true },
        { upsert: true, returnDocument: "after" }
      );

      assert.strictEqual(result?.name, "NewUser");
      assert.strictEqual(result?.created, true);

      // Verify document was inserted
      const doc = await collection.findOne({ name: "NewUser" });
      assert.ok(doc !== null);
    });

    it("should return null for upsert with returnDocument: before", async () => {
      const collection = client.db(dbName).collection("foar_upsert_before");

      const result = await collection.findOneAndReplace(
        { name: "NewUser2" },
        { name: "NewUser2", x: 1 },
        { upsert: true, returnDocument: "before" }
      );

      assert.strictEqual(result, null);

      // But document should exist
      const doc = await collection.findOne({ name: "NewUser2" });
      assert.ok(doc !== null);
    });

    it("should reject replacement containing update operators", async () => {
      const collection = client.db(dbName).collection("foar_reject_ops");
      await collection.insertOne({ name: "Alice" });

      await assert.rejects(
        collection.findOneAndReplace(
          { name: "Alice" },
          { $set: { age: 30 } } as unknown as Record<string, unknown>
        ),
        /update operators|atomic operators|keys starting with '\$'/i
      );
    });

    it("should apply sort when multiple matches", async () => {
      const collection = client.db(dbName).collection("foar_sort");
      await collection.insertMany([
        { type: "task", priority: 1, name: "low" },
        { type: "task", priority: 3, name: "high" },
        { type: "task", priority: 2, name: "medium" },
      ]);

      const result = await collection.findOneAndReplace(
        { type: "task" },
        { type: "task", priority: 99, name: "replaced" },
        { sort: { priority: -1 } }
      );

      assert.strictEqual(result?.name, "high");
      assert.strictEqual(result?.priority, 3);
    });

    it("should apply projection to returned document", async () => {
      const collection = client.db(dbName).collection("foar_projection");
      await collection.insertOne({ name: "Alice", age: 30, secret: "hidden" });

      const result = await collection.findOneAndReplace(
        { name: "Alice" },
        { name: "Alice", age: 31, secret: "stillHidden" },
        { projection: { name: 1, age: 1, _id: 0 }, returnDocument: "after" }
      );

      assert.strictEqual(result?.name, "Alice");
      assert.strictEqual(result?.age, 31);
      assert.strictEqual(result?.secret, undefined);
      assert.strictEqual(result?._id, undefined);
    });
  });

  describe("findOneAndUpdate", () => {
    it("should update and return document before update", async () => {
      const collection = client.db(dbName).collection("foau_basic");
      await collection.insertOne({ name: "Alice", age: 30 });

      const result = await collection.findOneAndUpdate(
        { name: "Alice" },
        { $set: { age: 31 } }
      );

      assert.strictEqual(result?.age, 30); // Before update

      // Verify document is updated
      const doc = await collection.findOne({ name: "Alice" });
      assert.strictEqual(doc?.age, 31);
    });

    it("should return document after update with returnDocument: after", async () => {
      const collection = client.db(dbName).collection("foau_after");
      await collection.insertOne({ name: "Bob", score: 100 });

      const result = await collection.findOneAndUpdate(
        { name: "Bob" },
        { $inc: { score: 10 } },
        { returnDocument: "after" }
      );

      assert.strictEqual(result?.score, 110);
    });

    it("should return null when no match", async () => {
      const collection = client.db(dbName).collection("foau_nomatch");
      await collection.insertOne({ name: "Alice" });

      const result = await collection.findOneAndUpdate(
        { name: "Nobody" },
        { $set: { age: 25 } }
      );

      assert.strictEqual(result, null);
    });

    it("should upsert when no match and upsert: true", async () => {
      const collection = client.db(dbName).collection("foau_upsert");

      const result = await collection.findOneAndUpdate(
        { name: "NewUser" },
        { $set: { score: 0 } },
        { upsert: true, returnDocument: "after" }
      );

      assert.strictEqual(result?.name, "NewUser");
      assert.strictEqual(result?.score, 0);
    });

    it("should apply sort when multiple matches", async () => {
      const collection = client.db(dbName).collection("foau_sort");
      await collection.insertMany([
        { type: "task", priority: 1 },
        { type: "task", priority: 2 },
        { type: "task", priority: 3 },
      ]);

      const result = await collection.findOneAndUpdate(
        { type: "task" },
        { $set: { done: true } },
        { sort: { priority: -1 } }
      );

      assert.strictEqual(result?.priority, 3); // Highest priority
    });

    it("should apply projection to result", async () => {
      const collection = client.db(dbName).collection("foau_projection");
      await collection.insertOne({ name: "Alice", age: 30, secret: "hidden" });

      const result = await collection.findOneAndUpdate(
        { name: "Alice" },
        { $inc: { age: 1 } },
        { projection: { name: 1 }, returnDocument: "after" }
      );

      assert.strictEqual(result?.name, "Alice");
      assert.strictEqual(result?.age, undefined);
      assert.strictEqual(result?.secret, undefined);
    });

    it("should work with $inc operator", async () => {
      const collection = client.db(dbName).collection("foau_inc");
      await collection.insertOne({ counter: 10 });

      const result = await collection.findOneAndUpdate(
        {},
        { $inc: { counter: 5 } },
        { returnDocument: "after" }
      );

      assert.strictEqual(result?.counter, 15);
    });

    it("should work with $unset operator", async () => {
      const collection = client.db(dbName).collection("foau_unset");
      await collection.insertOne({ name: "Alice", toRemove: "value" });

      const result = await collection.findOneAndUpdate(
        { name: "Alice" },
        { $unset: { toRemove: "" } },
        { returnDocument: "after" }
      );

      assert.strictEqual(result?.toRemove, undefined);
    });
  });

  describe("bulkWrite", () => {
    describe("insertOne operations", () => {
      it("should insert multiple documents", async () => {
        const collection = client.db(dbName).collection("bulk_insert");

        const result = await collection.bulkWrite([
          { insertOne: { document: { name: "Alice" } } },
          { insertOne: { document: { name: "Bob" } } },
        ]);

        // Note: acknowledged field may not be present in driver 6.x
        assert.strictEqual(result.insertedCount, 2);
        assert.ok(result.insertedIds[0]);
        assert.ok(result.insertedIds[1]);

        const docs = await collection.find({}).toArray();
        assert.strictEqual(docs.length, 2);
      });
    });

    describe("updateOne operations", () => {
      it("should update documents", async () => {
        const collection = client.db(dbName).collection("bulk_update_one");
        await collection.insertMany([
          { name: "Alice", age: 30 },
          { name: "Bob", age: 25 },
        ]);

        const result = await collection.bulkWrite([
          {
            updateOne: {
              filter: { name: "Alice" },
              update: { $set: { age: 31 } },
            },
          },
        ]);

        assert.strictEqual(result.matchedCount, 1);
        assert.strictEqual(result.modifiedCount, 1);

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.age, 31);
      });

      it("should track upserts", async () => {
        const collection = client.db(dbName).collection("bulk_upsert");

        const result = await collection.bulkWrite([
          {
            updateOne: {
              filter: { name: "NewUser" },
              update: { $set: { score: 100 } },
              upsert: true,
            },
          },
        ]);

        assert.strictEqual(result.upsertedCount, 1);
        assert.ok(result.upsertedIds[0]);
      });
    });

    describe("updateMany operations", () => {
      it("should update multiple documents", async () => {
        const collection = client.db(dbName).collection("bulk_update_many");
        await collection.insertMany([
          { type: "a", value: 1 },
          { type: "a", value: 2 },
          { type: "b", value: 3 },
        ]);

        const result = await collection.bulkWrite([
          {
            updateMany: {
              filter: { type: "a" },
              update: { $inc: { value: 10 } },
            },
          },
        ]);

        assert.strictEqual(result.matchedCount, 2);
        assert.strictEqual(result.modifiedCount, 2);
      });
    });

    describe("deleteOne operations", () => {
      it("should delete a single document", async () => {
        const collection = client.db(dbName).collection("bulk_delete_one");
        await collection.insertMany([{ x: 1 }, { x: 1 }, { x: 2 }]);

        const result = await collection.bulkWrite([
          { deleteOne: { filter: { x: 1 } } },
        ]);

        assert.strictEqual(result.deletedCount, 1);

        const remaining = await collection.find({ x: 1 }).toArray();
        assert.strictEqual(remaining.length, 1);
      });
    });

    describe("deleteMany operations", () => {
      it("should delete multiple documents", async () => {
        const collection = client.db(dbName).collection("bulk_delete_many");
        await collection.insertMany([{ x: 1 }, { x: 1 }, { x: 2 }]);

        const result = await collection.bulkWrite([
          { deleteMany: { filter: { x: 1 } } },
        ]);

        assert.strictEqual(result.deletedCount, 2);
      });
    });

    describe("replaceOne operations", () => {
      it("should replace a document", async () => {
        const collection = client.db(dbName).collection("bulk_replace");
        await collection.insertOne({ name: "Alice", age: 30 });

        const result = await collection.bulkWrite([
          {
            replaceOne: {
              filter: { name: "Alice" },
              replacement: { name: "Alice", age: 31, city: "NYC" },
            },
          },
        ]);

        assert.strictEqual(result.matchedCount, 1);
        assert.strictEqual(result.modifiedCount, 1);

        const doc = await collection.findOne({ name: "Alice" });
        assert.strictEqual(doc?.age, 31);
        assert.strictEqual(doc?.city, "NYC");
      });

      it("should upsert with replaceOne", async () => {
        const collection = client.db(dbName).collection("bulk_replace_upsert");

        const result = await collection.bulkWrite([
          {
            replaceOne: {
              filter: { name: "NewUser" },
              replacement: { name: "NewUser", x: 1 },
              upsert: true,
            },
          },
        ]);

        assert.strictEqual(result.upsertedCount, 1);
        assert.ok(result.upsertedIds[0]);
      });
    });

    describe("mixed operations", () => {
      it("should handle multiple operation types", async () => {
        const collection = client.db(dbName).collection("bulk_mixed");

        const result = await collection.bulkWrite([
          { insertOne: { document: { name: "Alice" } } },
          { insertOne: { document: { name: "Bob" } } },
          {
            updateOne: {
              filter: { name: "Alice" },
              update: { $set: { age: 30 } },
            },
          },
          { deleteOne: { filter: { name: "Bob" } } },
        ]);

        assert.strictEqual(result.insertedCount, 2);
        assert.strictEqual(result.matchedCount, 1);
        assert.strictEqual(result.modifiedCount, 1);
        assert.strictEqual(result.deletedCount, 1);

        const docs = await collection.find({}).toArray();
        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].name, "Alice");
        assert.strictEqual(docs[0].age, 30);
      });
    });

    describe("ordered mode", () => {
      it("should stop on first error in ordered mode (default)", async () => {
        const collection = client.db(dbName).collection("bulk_ordered");
        await collection.createIndex({ email: 1 }, { unique: true });
        await collection.insertOne({ email: "existing@test.com" });

        await assert.rejects(
          collection.bulkWrite([
            { insertOne: { document: { email: "existing@test.com" } } }, // Will fail
            { insertOne: { document: { email: "new@test.com" } } }, // Should not execute
          ])
        );

        // Second insert should not have happened
        const doc = await collection.findOne({ email: "new@test.com" });
        assert.strictEqual(doc, null);
      });
    });

    describe("unordered mode", () => {
      it("should continue on error in unordered mode", async () => {
        const collection = client.db(dbName).collection("bulk_unordered");
        await collection.createIndex({ email: 1 }, { unique: true });
        await collection.insertOne({ email: "existing@test.com" });

        try {
          await collection.bulkWrite(
            [
              { insertOne: { document: { email: "existing@test.com" } } }, // Will fail
              { insertOne: { document: { email: "new@test.com" } } }, // Should execute
            ],
            { ordered: false }
          );
          assert.fail("Should have thrown");
        } catch (error) {
          // Should have partial results
          const _err = error as Error & {
            writeErrors?: unknown[];
            result?: { insertedCount: number };
          };
          // The second insert should have succeeded
          const doc = await collection.findOne({ email: "new@test.com" });
          assert.ok(doc !== null);
        }
      });
    });
  });

  describe("String _id support", () => {
    it("findOneAndDelete should work with string _id", async () => {
      const collection = client.db(dbName).collection("string_id_delete");
      await collection.insertOne({ _id: "my-string-id", name: "Test" } as any);

      const result = await collection.findOneAndDelete({ _id: "my-string-id" } as any);

      assert.strictEqual(result?.name, "Test");
      assert.strictEqual((result as any)?._id, "my-string-id");

      const doc = await collection.findOne({ _id: "my-string-id" } as any);
      assert.strictEqual(doc, null);
    });

    it("findOneAndReplace should work with string _id", async () => {
      const collection = client.db(dbName).collection("string_id_replace");
      await collection.insertOne({ _id: "replace-id", name: "Original" } as any);

      const result = await collection.findOneAndReplace(
        { _id: "replace-id" } as any,
        { name: "Replaced" },
        { returnDocument: "after" }
      );

      assert.strictEqual(result?.name, "Replaced");
      assert.strictEqual((result as any)?._id, "replace-id");
    });

    it("findOneAndUpdate should work with string _id", async () => {
      const collection = client.db(dbName).collection("string_id_update");
      await collection.insertOne({ _id: "update-id", count: 0 } as any);

      const result = await collection.findOneAndUpdate(
        { _id: "update-id" } as any,
        { $inc: { count: 1 } },
        { returnDocument: "after" }
      );

      assert.strictEqual(result?.count, 1);
      assert.strictEqual((result as any)?._id, "update-id");
    });

    it("bulkWrite replaceOne should work with string _id", async () => {
      const collection = client.db(dbName).collection("string_id_bulk");
      await collection.insertOne({ _id: "bulk-id", value: "old" } as any);

      const result = await collection.bulkWrite([
        {
          replaceOne: {
            filter: { _id: "bulk-id" } as any,
            replacement: { value: "new" },
          },
        },
      ]);

      assert.strictEqual(result.matchedCount, 1);
      assert.strictEqual(result.modifiedCount, 1);

      const doc = await collection.findOne({ _id: "bulk-id" } as any);
      assert.strictEqual(doc?.value, "new");
    });

    it("should work with numeric _id", async () => {
      const collection = client.db(dbName).collection("numeric_id");
      await collection.insertOne({ _id: 12345, name: "Numeric" } as any);

      const result = await collection.findOneAndUpdate(
        { _id: 12345 } as any,
        { $set: { name: "Updated" } },
        { returnDocument: "after" }
      );

      assert.strictEqual(result?.name, "Updated");
      assert.strictEqual((result as any)?._id, 12345);

      const deleted = await collection.findOneAndDelete({ _id: 12345 } as any);
      assert.strictEqual(deleted?.name, "Updated");
    });
  });
});
