/**
 * Phase 11: Regular Expression Tests
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

describe(`Regular Expression Tests (${getTestModeName()})`, () => {
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

  describe("$regex Query Operator", () => {
    describe("Basic Pattern Matching", () => {
      it("should match start of string with ^", async () => {
        const collection = client.db(dbName).collection("regex_start");
        await collection.insertMany([
          { name: "Alice" },
          { name: "Bob" },
          { name: "Anna" },
        ]);

        const docs = await collection
          .find({ name: { $regex: "^A" } })
          .toArray();

        assert.strictEqual(docs.length, 2);
        assert.ok(docs.every((d) => (d.name as string).startsWith("A")));
      });

      it("should match end of string with $", async () => {
        const collection = client.db(dbName).collection("regex_end");
        await collection.insertMany([
          { email: "alice@gmail.com" },
          { email: "bob@yahoo.com" },
          { email: "charlie@gmail.com" },
        ]);

        const docs = await collection
          .find({ email: { $regex: "@gmail\\.com$" } })
          .toArray();

        assert.strictEqual(docs.length, 2);
        assert.ok(docs.every((d) => (d.email as string).endsWith("@gmail.com")));
      });

      it("should match pattern anywhere in string", async () => {
        const collection = client.db(dbName).collection("regex_contains");
        await collection.insertMany([
          { description: "This is urgent" },
          { description: "Normal task" },
          { description: "Very urgent task" },
        ]);

        const docs = await collection
          .find({ description: { $regex: "urgent" } })
          .toArray();

        assert.strictEqual(docs.length, 2);
      });

      it("should match with special regex characters escaped", async () => {
        const collection = client.db(dbName).collection("regex_special");
        await collection.insertMany([
          { path: "foo.bar" },
          { path: "fooXbar" },
          { path: "foo/bar" },
        ]);

        const docs = await collection
          .find({ path: { $regex: "foo\\.bar" } })
          .toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].path, "foo.bar");
      });

      it("should not match non-string fields", async () => {
        const collection = client.db(dbName).collection("regex_nonstring");
        await collection.insertMany([
          { value: 25 },
          { value: "25" },
          { value: { num: 25 } },
        ]);

        const docs = await collection
          .find({ value: { $regex: "25" } })
          .toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, "25");
      });

      it("should not match null values", async () => {
        const collection = client.db(dbName).collection("regex_null");
        await collection.insertMany([
          { name: null },
          { name: "Alice" },
          { name: "Bob" },
        ]);

        const docs = await collection
          .find({ name: { $regex: ".*" } })
          .toArray();

        assert.strictEqual(docs.length, 2);
        assert.ok(docs.every((d) => d.name !== null));
      });

      it("should not match undefined/missing fields", async () => {
        const collection = client.db(dbName).collection("regex_missing");
        await collection.insertMany([
          { name: "Alice" },
          { other: "field" },
          { name: "Bob" },
        ]);

        const docs = await collection
          .find({ name: { $regex: ".*" } })
          .toArray();

        assert.strictEqual(docs.length, 2);
        assert.ok(docs.every((d) => "name" in d));
      });

      it("should match empty pattern against any string", async () => {
        const collection = client.db(dbName).collection("regex_empty");
        await collection.insertMany([
          { name: "Alice" },
          { name: "" },
          { name: "Bob" },
        ]);

        const docs = await collection.find({ name: { $regex: "" } }).toArray();

        assert.strictEqual(docs.length, 3);
      });
    });

    describe("Options", () => {
      it("should support case-insensitive (i) option", async () => {
        const collection = client.db(dbName).collection("regex_opt_i");
        await collection.insertMany([
          { name: "Alice" },
          { name: "ALICE" },
          { name: "alice" },
          { name: "Bob" },
        ]);

        const docs = await collection
          .find({ name: { $regex: "alice", $options: "i" } })
          .toArray();

        assert.strictEqual(docs.length, 3);
      });

      it("should support multiline (m) option", async () => {
        const collection = client.db(dbName).collection("regex_opt_m");
        await collection.insertMany([
          { text: "hello\nworld" },
          { text: "world\nhello" },
          { text: "test" },
        ]);

        // Without multiline, ^ only matches start of string
        const docsNoMultiline = await collection
          .find({ text: { $regex: "^world" } })
          .toArray();
        assert.strictEqual(docsNoMultiline.length, 1);

        // With multiline, ^ matches start of any line
        const docsMultiline = await collection
          .find({ text: { $regex: "^world", $options: "m" } })
          .toArray();
        assert.strictEqual(docsMultiline.length, 2);
      });

      it("should support dotAll (s) option", async () => {
        const collection = client.db(dbName).collection("regex_opt_s");
        await collection.insertMany([
          { text: "a\nb" },
          { text: "aXb" },
          { text: "abc" },
        ]);

        // Without dotAll, . doesn't match newline
        // Pattern a.b matches "aXb" but not "a\nb" (newline not matched by .)
        const docsNoDotall = await collection
          .find({ text: { $regex: "a.b" } })
          .toArray();
        assert.strictEqual(docsNoDotall.length, 1); // Only "aXb" matches

        // With dotAll, . matches newline
        // Pattern a.b matches both "aXb" and "a\nb"
        const docsDotall = await collection
          .find({ text: { $regex: "a.b", $options: "s" } })
          .toArray();
        assert.strictEqual(docsDotall.length, 2);
      });

      it("should combine multiple options", async () => {
        const collection = client.db(dbName).collection("regex_opt_multi");
        await collection.insertMany([
          { text: "HELLO\nworld" },
          { text: "hello\nWORLD" },
          { text: "test" },
        ]);

        const docs = await collection
          .find({ text: { $regex: "^world", $options: "im" } })
          .toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe("Error Cases", () => {
      it("should throw for invalid regex pattern", async () => {
        const collection = client.db(dbName).collection("regex_err_invalid");
        await collection.insertOne({ name: "test" });

        await assert.rejects(
          async () => {
            await collection.find({ name: { $regex: "[invalid" } }).toArray();
          },
          (err: Error) => {
            // MongoDB throws error for invalid regex pattern
            return err instanceof Error && (
              err.message.includes("Regular expression") ||
              err.message.includes("Invalid regular expression")
            );
          }
        );
      });

      it("should throw for $options without $regex", async () => {
        const collection = client.db(dbName).collection("regex_err_noopts");
        await collection.insertOne({ name: "test" });

        await assert.rejects(
          async () => {
            await collection.find({ name: { $options: "i" } as any }).toArray();
          },
          (err: Error) => {
            return err.message.includes("$options") || err.message.includes("$regex");
          }
        );
      });

      it("should throw for invalid regex options", async () => {
        const collection = client.db(dbName).collection("regex_err_flags");
        await collection.insertOne({ name: "test" });

        await assert.rejects(
          async () => {
            await collection.find({ name: { $regex: "test", $options: "g" } }).toArray();
          },
          (err: Error) => {
            // MongoDB throws "invalid flag in regex options: g"
            return err instanceof Error && err.message.includes("invalid flag");
          }
        );
      });
    });
  });

  describe("JavaScript RegExp", () => {
    it("should match with RegExp literal", async () => {
      const collection = client.db(dbName).collection("regexp_literal");
      await collection.insertMany([
        { name: "Alice" },
        { name: "Bob" },
        { name: "Anna" },
      ]);

      const docs = await collection.find({ name: /^A/ }).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should match with RegExp object", async () => {
      const collection = client.db(dbName).collection("regexp_object");
      await collection.insertMany([
        { name: "Alice" },
        { name: "ALICE" },
        { name: "Bob" },
      ]);

      const docs = await collection
        .find({ name: new RegExp("^alice", "i") })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should respect RegExp flags", async () => {
      const collection = client.db(dbName).collection("regexp_flags");
      await collection.insertMany([
        { email: "alice@GMAIL.com" },
        { email: "bob@gmail.COM" },
        { email: "charlie@yahoo.com" },
      ]);

      const docs = await collection.find({ email: /gmail\.com$/i }).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should work in findOne", async () => {
      const collection = client.db(dbName).collection("regexp_findone");
      await collection.insertMany([
        { name: "Alice" },
        { name: "Bob" },
        { name: "Anna" },
      ]);

      const doc = await collection.findOne({ name: /^A/ });

      assert.ok(doc);
      assert.ok((doc.name as string).startsWith("A"));
    });

    it("should work combined with other conditions", async () => {
      const collection = client.db(dbName).collection("regexp_combined");
      await collection.insertMany([
        { name: "Alice", age: 25 },
        { name: "Anna", age: 30 },
        { name: "Bob", age: 25 },
      ]);

      const docs = await collection
        .find({ name: /^A/, age: { $gt: 20 } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });
  });

  describe("Array Fields", () => {
    it("should match if any array element matches", async () => {
      const collection = client.db(dbName).collection("regex_array_match");
      await collection.insertMany([
        { tags: ["production", "staging", "dev"] },
        { tags: ["alpha", "beta"] },
        { tags: ["prod-v1", "dev"] },
      ]);

      const docs = await collection
        .find({ tags: { $regex: "^prod" } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should not match if no element matches", async () => {
      const collection = client.db(dbName).collection("regex_array_nomatch");
      await collection.insertMany([
        { tags: ["alpha", "beta"] },
        { tags: ["gamma", "delta"] },
      ]);

      const docs = await collection
        .find({ tags: { $regex: "^prod" } })
        .toArray();

      assert.strictEqual(docs.length, 0);
    });

    it("should skip non-string elements in array", async () => {
      const collection = client.db(dbName).collection("regex_array_mixed");
      await collection.insertMany([
        { values: ["test", 123, "other"] },
        { values: [456, 789] },
        { values: ["production"] },
      ]);

      const docs = await collection
        .find({ values: { $regex: "test" } })
        .toArray();

      assert.strictEqual(docs.length, 1);
    });

    it("should work with RegExp literal on array fields", async () => {
      const collection = client.db(dbName).collection("regex_array_regexp");
      await collection.insertMany([
        { tags: ["URGENT", "important"] },
        { tags: ["normal", "low"] },
        { tags: ["urgent-task"] },
      ]);

      const docs = await collection.find({ tags: /urgent/i }).toArray();

      assert.strictEqual(docs.length, 2);
    });
  });

  describe("With $elemMatch", () => {
    it("should support $regex in $elemMatch", async () => {
      const collection = client.db(dbName).collection("regex_elemmatch");
      await collection.insertMany([
        { items: [{ name: "widget-a", price: 50 }] },
        { items: [{ name: "gadget-b", price: 80 }] },
        { items: [{ name: "widget-c", price: 120 }] },
      ]);

      const docs = await collection
        .find({
          items: { $elemMatch: { name: { $regex: "^widget" }, price: { $lt: 100 } } },
        })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual((docs[0].items as any[])[0].name, "widget-a");
    });

    it("should combine $regex with other conditions in $elemMatch", async () => {
      const collection = client.db(dbName).collection("regex_elemmatch_multi");
      await collection.insertMany([
        {
          logs: [
            { message: "Error occurred", level: "ERROR" },
            { message: "Info message", level: "INFO" },
          ],
        },
        {
          logs: [
            { message: "Warning detected", level: "WARN" },
            { message: "Debug info", level: "DEBUG" },
          ],
        },
        {
          logs: [
            { message: "Fatal error", level: "FATAL" },
          ],
        },
      ]);

      const docs = await collection
        .find({
          logs: {
            $elemMatch: {
              message: { $regex: "error", $options: "i" },
              level: { $regex: "^(ERROR|FATAL)$" },
            },
          },
        })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });
  });

  describe("With $in Operator", () => {
    it("should match RegExp in $in array", async () => {
      const collection = client.db(dbName).collection("regex_in_regexp");
      await collection.insertMany([
        { status: "active-user" },
        { status: "pending-review" },
        { status: "complete" },
        { status: "inactive" },
      ]);

      const docs = await collection
        .find({ status: { $in: [/^active/, /^pending/] } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should mix exact values and RegExp in $in", async () => {
      const collection = client.db(dbName).collection("regex_in_mixed");
      await collection.insertMany([
        { status: "complete" },
        { status: "in_progress_v1" },
        { status: "review_needed" },
        { status: "draft" },
      ]);

      const docs = await collection
        .find({ status: { $in: ["complete", /^in_progress/, /^review/] } })
        .toArray();

      assert.strictEqual(docs.length, 3);
    });

    it("should work with case insensitive regex in $in", async () => {
      const collection = client.db(dbName).collection("regex_in_flags");
      await collection.insertMany([
        { category: "Electronics" },
        { category: "BOOKS" },
        { category: "Games" },
        { category: "Clothing" },
      ]);

      const docs = await collection
        .find({ category: { $in: [/electronics/i, "BOOKS", /games/i] } })
        .toArray();

      assert.strictEqual(docs.length, 3);
    });

    it("should work with array fields and $in containing regex", async () => {
      const collection = client.db(dbName).collection("regex_in_array");
      await collection.insertMany([
        { tags: ["production", "v1"] },
        { tags: ["staging", "v2"] },
        { tags: ["development"] },
      ]);

      const docs = await collection
        .find({ tags: { $in: [/^prod/, /^staging/] } })
        .toArray();

      assert.strictEqual(docs.length, 2);
    });
  });

  describe("With $not Operator", () => {
    it("should negate regex match with $not and $regex", async () => {
      const collection = client.db(dbName).collection("regex_not_regex");
      await collection.insertMany([
        { name: "Admin_user" },
        { name: "Regular_user" },
        { name: "Admin_super" },
        { name: "Guest" },
      ]);

      const docs = await collection
        .find({ name: { $not: { $regex: "^Admin" } } })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.every((d) => !(d.name as string).startsWith("Admin")));
    });

    it("should match missing fields with $not $regex", async () => {
      const collection = client.db(dbName).collection("regex_not_missing");
      await collection.insertMany([
        { name: "Alice" },
        { other: "value" },
        { name: "test_user" },
      ]);

      const docs = await collection
        .find({ name: { $not: { $regex: "test" } } })
        .toArray();

      // Should match "Alice" (doesn't contain "test") and the doc without name field
      assert.strictEqual(docs.length, 2);
    });

    it("should work with $not and $options", async () => {
      const collection = client.db(dbName).collection("regex_not_options");
      await collection.insertMany([
        { email: "alice@test.com" },
        { email: "bob@TEST.COM" },
        { email: "charlie@gmail.com" },
      ]);

      const docs = await collection
        .find({ email: { $not: { $regex: "@test\\.com$", $options: "i" } } })
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].email, "charlie@gmail.com");
    });

    it("should work with $not and RegExp literal", async () => {
      const collection = client.db(dbName).collection("regex_not_literal");
      await collection.insertMany([
        { name: "test_user" },
        { name: "admin" },
        { name: "test_admin" },
      ]);

      const docs = await collection.find({ name: { $not: /^test/ } }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, "admin");
    });
  });

  describe("With $nin Operator", () => {
    it("should not match if any RegExp in $nin matches", async () => {
      const collection = client.db(dbName).collection("regex_nin");
      await collection.insertMany([
        { status: "active" },
        { status: "pending" },
        { status: "complete" },
        { status: "archived" },
      ]);

      const docs = await collection
        .find({ status: { $nin: [/^active/, /^pending/] } })
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.status === "complete"));
      assert.ok(docs.some((d) => d.status === "archived"));
    });

    it("should match if no RegExp in $nin matches", async () => {
      const collection = client.db(dbName).collection("regex_nin_nomatch");
      await collection.insertMany([
        { category: "books" },
        { category: "games" },
        { category: "movies" },
      ]);

      const docs = await collection
        .find({ category: { $nin: [/^electronics/] } })
        .toArray();

      assert.strictEqual(docs.length, 3);
    });
  });

  describe("In Aggregation $match", () => {
    it("should support $regex in $match stage", async () => {
      const collection = client.db(dbName).collection("regex_agg_regex");
      await collection.insertMany([
        { name: "Alice", score: 85 },
        { name: "Bob", score: 90 },
        { name: "Anna", score: 78 },
      ]);

      const docs = await collection
        .aggregate([{ $match: { name: { $regex: "^A" } } }])
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should support RegExp in $match stage", async () => {
      const collection = client.db(dbName).collection("regex_agg_regexp");
      await collection.insertMany([
        { email: "alice@gmail.com" },
        { email: "bob@GMAIL.COM" },
        { email: "charlie@yahoo.com" },
      ]);

      const docs = await collection
        .aggregate([{ $match: { email: /gmail\.com$/i } }])
        .toArray();

      assert.strictEqual(docs.length, 2);
    });

    it("should combine regex with other pipeline stages", async () => {
      const collection = client.db(dbName).collection("regex_agg_combined");
      await collection.insertMany([
        { name: "Alice", score: 85 },
        { name: "Anna", score: 92 },
        { name: "Bob", score: 78 },
        { name: "Amy", score: 88 },
      ]);

      const docs = await collection
        .aggregate([
          { $match: { name: { $regex: "^A" } } },
          { $sort: { score: -1 } },
          { $limit: 2 },
        ])
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.strictEqual(docs[0].name, "Anna"); // Highest score
      assert.strictEqual(docs[1].name, "Amy"); // Second highest
    });
  });
});
