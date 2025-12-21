/**
 * Phase 9: Aggregation Pipeline Tests
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

describe(`Aggregation Pipeline Tests (${getTestModeName()})`, () => {
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

  // ==================== Pipeline Infrastructure ====================

  describe("Pipeline Infrastructure", () => {
    it("should return empty array for empty collection", async () => {
      const collection = client.db(dbName).collection("agg_empty");
      const results = await collection.aggregate([]).toArray();
      assert.deepStrictEqual(results, []);
    });

    it("should return all documents with empty pipeline", async () => {
      const collection = client.db(dbName).collection("agg_no_stages");
      await collection.insertMany([{ a: 1 }, { a: 2 }, { a: 3 }]);

      const results = await collection.aggregate([]).toArray();
      assert.strictEqual(results.length, 3);
    });

    it("should throw for unknown stage", async () => {
      const collection = client.db(dbName).collection("agg_unknown");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $unknownStage: {} } as unknown as Record<string, unknown>])
            .toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("Unrecognized pipeline stage name"));
          return true;
        }
      );
    });

    it("should execute stages in order", async () => {
      const collection = client.db(dbName).collection("agg_order");
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      // Skip 2, then limit 2 - should get [3, 4]
      const results = await collection
        .aggregate([{ $sort: { value: 1 } }, { $skip: 2 }, { $limit: 2 }])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0].value, 3);
      assert.strictEqual(results[1].value, 4);
    });
  });

  // ==================== $match Stage ====================

  describe("$match Stage", () => {
    it("should filter documents by equality", async () => {
      const collection = client.db(dbName).collection("agg_match_eq");
      await collection.insertMany([
        { status: "active", name: "Alice" },
        { status: "inactive", name: "Bob" },
        { status: "active", name: "Charlie" },
      ]);

      const results = await collection
        .aggregate([{ $match: { status: "active" } }])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.ok(results.every((r) => r.status === "active"));
    });

    it("should support comparison operators", async () => {
      const collection = client.db(dbName).collection("agg_match_cmp");
      await collection.insertMany([
        { age: 15 },
        { age: 20 },
        { age: 25 },
        { age: 30 },
      ]);

      const results = await collection
        .aggregate([{ $match: { age: { $gte: 20, $lt: 30 } } }])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.ok(results.every((r) => (r.age as number) >= 20 && (r.age as number) < 30));
    });

    it("should support logical operators", async () => {
      const collection = client.db(dbName).collection("agg_match_logic");
      await collection.insertMany([
        { a: 1, b: 1 },
        { a: 1, b: 2 },
        { a: 2, b: 1 },
        { a: 2, b: 2 },
      ]);

      const results = await collection
        .aggregate([{ $match: { $or: [{ a: 1 }, { b: 1 }] } }])
        .toArray();

      assert.strictEqual(results.length, 3);
    });

    it("should support array operators", async () => {
      const collection = client.db(dbName).collection("agg_match_arr");
      await collection.insertMany([
        { tags: ["js", "ts"] },
        { tags: ["python"] },
        { tags: ["js", "rust"] },
      ]);

      const results = await collection
        .aggregate([{ $match: { tags: "js" } }])
        .toArray();

      assert.strictEqual(results.length, 2);
    });

    it("should return all docs with empty match", async () => {
      const collection = client.db(dbName).collection("agg_match_empty");
      await collection.insertMany([{ a: 1 }, { a: 2 }, { a: 3 }]);

      const results = await collection.aggregate([{ $match: {} }]).toArray();

      assert.strictEqual(results.length, 3);
    });

    it("should support dot notation for nested fields", async () => {
      const collection = client.db(dbName).collection("agg_match_nested");
      await collection.insertMany([
        { user: { name: "Alice", age: 25 } },
        { user: { name: "Bob", age: 30 } },
      ]);

      const results = await collection
        .aggregate([{ $match: { "user.name": "Alice" } }])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual((results[0].user as Record<string, unknown>).name, "Alice");
    });
  });

  // ==================== $project Stage ====================

  describe("$project Stage", () => {
    describe("Inclusion Mode", () => {
      it("should include specified fields only", async () => {
        const collection = client.db(dbName).collection("agg_proj_inc");
        await collection.insertMany([
          { name: "Alice", age: 25, email: "alice@test.com" },
          { name: "Bob", age: 30, email: "bob@test.com" },
        ]);

        const results = await collection
          .aggregate([{ $project: { name: 1, age: 1 } }])
          .toArray();

        assert.strictEqual(results.length, 2);
        assert.ok(results[0].name);
        assert.ok(results[0].age);
        assert.ok(results[0]._id); // _id included by default
        assert.strictEqual(results[0].email, undefined);
      });

      it("should include _id by default", async () => {
        const collection = client.db(dbName).collection("agg_proj_id_default");
        await collection.insertOne({ name: "Test", value: 42 });

        const results = await collection
          .aggregate([{ $project: { name: 1 } }])
          .toArray();

        assert.ok(results[0]._id);
      });

      it("should allow excluding _id with { _id: 0 }", async () => {
        const collection = client.db(dbName).collection("agg_proj_no_id");
        await collection.insertOne({ name: "Test", value: 42 });

        const results = await collection
          .aggregate([{ $project: { name: 1, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0]._id, undefined);
        assert.strictEqual(results[0].name, "Test");
      });

      it("should handle nested fields with dot notation", async () => {
        const collection = client.db(dbName).collection("agg_proj_nested");
        await collection.insertOne({
          user: { name: "Alice", email: "alice@test.com" },
          status: "active",
        });

        const results = await collection
          .aggregate([{ $project: { "user.name": 1, _id: 0 } }])
          .toArray();

        assert.strictEqual(
          (results[0].user as Record<string, unknown>).name,
          "Alice"
        );
        assert.strictEqual(
          (results[0].user as Record<string, unknown>).email,
          undefined
        );
      });

      it("should handle missing fields gracefully", async () => {
        const collection = client.db(dbName).collection("agg_proj_missing");
        await collection.insertOne({ name: "Alice" });

        const results = await collection
          .aggregate([{ $project: { name: 1, age: 1, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].name, "Alice");
        assert.strictEqual(results[0].age, undefined);
      });
    });

    describe("Exclusion Mode", () => {
      it("should exclude specified fields", async () => {
        const collection = client.db(dbName).collection("agg_proj_exc");
        await collection.insertOne({
          name: "Alice",
          password: "secret",
          email: "alice@test.com",
        });

        const results = await collection
          .aggregate([{ $project: { password: 0 } }])
          .toArray();

        assert.strictEqual(results[0].name, "Alice");
        assert.strictEqual(results[0].email, "alice@test.com");
        assert.strictEqual(results[0].password, undefined);
      });

      it("should include all other fields", async () => {
        const collection = client.db(dbName).collection("agg_proj_exc_all");
        await collection.insertOne({ a: 1, b: 2, c: 3, d: 4 });

        const results = await collection
          .aggregate([{ $project: { c: 0 } }])
          .toArray();

        assert.strictEqual(results[0].a, 1);
        assert.strictEqual(results[0].b, 2);
        assert.strictEqual(results[0].c, undefined);
        assert.strictEqual(results[0].d, 4);
      });

      it("should handle { _id: 0 } alone as exclusion mode", async () => {
        const collection = client.db(dbName).collection("agg_proj_id_only");
        await collection.insertOne({ name: "Alice", age: 25 });

        const results = await collection
          .aggregate([{ $project: { _id: 0 } }])
          .toArray();

        // All fields except _id should be included
        assert.strictEqual(results[0]._id, undefined);
        assert.strictEqual(results[0].name, "Alice");
        assert.strictEqual(results[0].age, 25);
      });
    });

    describe("Field Renaming", () => {
      it("should rename fields using $fieldName syntax", async () => {
        const collection = client.db(dbName).collection("agg_proj_rename");
        await collection.insertOne({ name: "Alice", age: 25 });

        const results = await collection
          .aggregate([{ $project: { fullName: "$name", _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].fullName, "Alice");
        assert.strictEqual(results[0].name, undefined);
      });

      it("should handle nested field references", async () => {
        const collection = client.db(dbName).collection("agg_proj_rename_nested");
        await collection.insertOne({ user: { firstName: "Alice", lastName: "Smith" } });

        const results = await collection
          .aggregate([{ $project: { name: "$user.firstName", _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].name, "Alice");
      });

      it("should exclude field when referencing missing field", async () => {
        const collection = client.db(dbName).collection("agg_proj_rename_miss");
        await collection.insertOne({ name: "Alice" });

        const results = await collection
          .aggregate([{ $project: { value: "$nonexistent", _id: 0 } }])
          .toArray();

        // MongoDB excludes the field entirely when the referenced field is missing
        assert.strictEqual(results[0].value, undefined);
        assert.ok(!("value" in results[0]));
      });
    });

    describe("$literal Expression", () => {
      it("should return literal numeric value", async () => {
        const collection = client.db(dbName).collection("agg_proj_literal_num");
        await collection.insertOne({ name: "Test" });

        const results = await collection
          .aggregate([{ $project: { name: 1, staticValue: { $literal: 42 }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].staticValue, 42);
      });

      it("should return literal string value", async () => {
        const collection = client.db(dbName).collection("agg_proj_literal_str");
        await collection.insertOne({ name: "Test" });

        const results = await collection
          .aggregate([{ $project: { label: { $literal: "$notAField" }, _id: 0 } }])
          .toArray();

        // $literal prevents interpretation as field reference
        assert.strictEqual(results[0].label, "$notAField");
      });
    });

    describe("Error Cases", () => {
      it("should throw when mixing inclusion and exclusion", async () => {
        const collection = client.db(dbName).collection("agg_proj_mix_err");
        await collection.insertOne({ a: 1, b: 2 });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { a: 1, b: 0 } }])
              .toArray();
          },
          (err: Error) => {
            // MongoDB error mentions "exclusion" or "inclusion" in various forms
            const msg = err.message.toLowerCase();
            assert.ok(
              msg.includes("exclusion") || msg.includes("inclusion") || msg.includes("mix"),
              `Expected error about mixing inclusion/exclusion, got: ${err.message}`
            );
            return true;
          }
        );
      });
    });
  });

  // ==================== $sort Stage ====================

  describe("$sort Stage", () => {
    it("should sort ascending by numeric field", async () => {
      const collection = client.db(dbName).collection("agg_sort_asc");
      await collection.insertMany([{ value: 30 }, { value: 10 }, { value: 20 }]);

      const results = await collection
        .aggregate([{ $sort: { value: 1 } }])
        .toArray();

      assert.strictEqual(results[0].value, 10);
      assert.strictEqual(results[1].value, 20);
      assert.strictEqual(results[2].value, 30);
    });

    it("should sort descending by numeric field", async () => {
      const collection = client.db(dbName).collection("agg_sort_desc");
      await collection.insertMany([{ value: 10 }, { value: 30 }, { value: 20 }]);

      const results = await collection
        .aggregate([{ $sort: { value: -1 } }])
        .toArray();

      assert.strictEqual(results[0].value, 30);
      assert.strictEqual(results[1].value, 20);
      assert.strictEqual(results[2].value, 10);
    });

    it("should sort by string field (lexicographic)", async () => {
      const collection = client.db(dbName).collection("agg_sort_str");
      await collection.insertMany([
        { name: "Charlie" },
        { name: "Alice" },
        { name: "Bob" },
      ]);

      const results = await collection
        .aggregate([{ $sort: { name: 1 } }])
        .toArray();

      assert.strictEqual(results[0].name, "Alice");
      assert.strictEqual(results[1].name, "Bob");
      assert.strictEqual(results[2].name, "Charlie");
    });

    it("should sort by date field", async () => {
      const collection = client.db(dbName).collection("agg_sort_date");
      const date1 = new Date("2024-01-01");
      const date2 = new Date("2024-06-01");
      const date3 = new Date("2024-12-01");

      await collection.insertMany([
        { createdAt: date2 },
        { createdAt: date3 },
        { createdAt: date1 },
      ]);

      const results = await collection
        .aggregate([{ $sort: { createdAt: 1 } }])
        .toArray();

      assert.strictEqual(
        (results[0].createdAt as Date).getTime(),
        date1.getTime()
      );
      assert.strictEqual(
        (results[2].createdAt as Date).getTime(),
        date3.getTime()
      );
    });

    it("should handle compound sort (multiple fields)", async () => {
      const collection = client.db(dbName).collection("agg_sort_compound");
      await collection.insertMany([
        { category: "A", priority: 2 },
        { category: "B", priority: 1 },
        { category: "A", priority: 1 },
        { category: "B", priority: 2 },
      ]);

      const results = await collection
        .aggregate([{ $sort: { category: 1, priority: 1 } }])
        .toArray();

      assert.strictEqual(results[0].category, "A");
      assert.strictEqual(results[0].priority, 1);
      assert.strictEqual(results[1].category, "A");
      assert.strictEqual(results[1].priority, 2);
      assert.strictEqual(results[2].category, "B");
      assert.strictEqual(results[2].priority, 1);
    });

    it("should handle null/missing values in sort", async () => {
      const collection = client.db(dbName).collection("agg_sort_null");
      await collection.insertMany([
        { value: 3 },
        { value: null },
        { value: 1 },
        { noValue: true },
      ]);

      const results = await collection
        .aggregate([{ $sort: { value: 1 } }])
        .toArray();

      // Null/missing values should come first in ascending order
      assert.strictEqual(results.length, 4);
      assert.ok(
        results[0].value === null || results[0].value === undefined
      );
    });
  });

  // ==================== $limit Stage ====================

  describe("$limit Stage", () => {
    it("should limit results to n documents", async () => {
      const collection = client.db(dbName).collection("agg_limit");
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      const results = await collection.aggregate([{ $limit: 3 }]).toArray();

      assert.strictEqual(results.length, 3);
    });

    it("should return all if limit exceeds document count", async () => {
      const collection = client.db(dbName).collection("agg_limit_exceed");
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      const results = await collection.aggregate([{ $limit: 100 }]).toArray();

      assert.strictEqual(results.length, 2);
    });

    it("should throw for limit 0", async () => {
      const collection = client.db(dbName).collection("agg_limit_zero");
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      await assert.rejects(
        async () => collection.aggregate([{ $limit: 0 }]).toArray(),
        (err: Error) => err.message.includes("the limit must be positive")
      );
    });

    it("should throw for non-integer limit", async () => {
      const collection = client.db(dbName).collection("agg_limit_float");
      await collection.insertOne({ value: 1 });

      await assert.rejects(
        async () => collection.aggregate([{ $limit: 2.5 }]).toArray(),
        (err: Error) => err.message.includes("Expected an integer")
      );
    });

    it("should throw for negative limit", async () => {
      const collection = client.db(dbName).collection("agg_limit_neg");
      await collection.insertOne({ value: 1 });

      await assert.rejects(
        async () => collection.aggregate([{ $limit: -1 }]).toArray(),
        (err: Error) => err.message.includes("Expected a non-negative number")
      );
    });
  });

  // ==================== $skip Stage ====================

  describe("$skip Stage", () => {
    it("should skip first n documents", async () => {
      const collection = client.db(dbName).collection("agg_skip");
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      const results = await collection
        .aggregate([{ $sort: { value: 1 } }, { $skip: 2 }])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].value, 3);
    });

    it("should return empty if skip exceeds document count", async () => {
      const collection = client.db(dbName).collection("agg_skip_exceed");
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      const results = await collection.aggregate([{ $skip: 10 }]).toArray();

      assert.strictEqual(results.length, 0);
    });

    it("should work with limit (pagination)", async () => {
      const collection = client.db(dbName).collection("agg_skip_limit");
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      // Page 2, page size 2
      const results = await collection
        .aggregate([{ $sort: { value: 1 } }, { $skip: 2 }, { $limit: 2 }])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0].value, 3);
      assert.strictEqual(results[1].value, 4);
    });

    it("should handle skip 0 (no-op)", async () => {
      const collection = client.db(dbName).collection("agg_skip_zero");
      await collection.insertMany([{ value: 1 }, { value: 2 }, { value: 3 }]);

      const results = await collection.aggregate([{ $skip: 0 }]).toArray();

      assert.strictEqual(results.length, 3);
    });

    it("should throw for non-integer skip", async () => {
      const collection = client.db(dbName).collection("agg_skip_float");
      await collection.insertOne({ value: 1 });

      await assert.rejects(
        async () => {
          await collection.aggregate([{ $skip: 1.5 }]).toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("integer"));
          return true;
        }
      );
    });

    it("should throw for negative skip", async () => {
      const collection = client.db(dbName).collection("agg_skip_neg");
      await collection.insertOne({ value: 1 });

      await assert.rejects(
        async () => {
          await collection.aggregate([{ $skip: -5 }]).toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("non-negative"));
          return true;
        }
      );
    });
  });

  // ==================== $count Stage ====================

  describe("$count Stage", () => {
    it("should count all documents in collection", async () => {
      const collection = client.db(dbName).collection("agg_count_all");
      await collection.insertMany([{ a: 1 }, { a: 2 }, { a: 3 }, { a: 4 }]);

      const results = await collection
        .aggregate([{ $count: "total" }])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].total, 4);
    });

    it("should count after $match filter", async () => {
      const collection = client.db(dbName).collection("agg_count_match");
      await collection.insertMany([
        { status: "active" },
        { status: "inactive" },
        { status: "active" },
        { status: "active" },
      ]);

      const results = await collection
        .aggregate([{ $match: { status: "active" } }, { $count: "activeCount" }])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].activeCount, 3);
    });

    it("should return empty array for empty input (not { count: 0 })", async () => {
      const collection = client.db(dbName).collection("agg_count_empty");
      // Don't insert anything

      const results = await collection
        .aggregate([{ $count: "total" }])
        .toArray();

      // Critical: returns empty array, NOT [{ total: 0 }]
      assert.deepStrictEqual(results, []);
    });

    it("should return empty array when match filters out all documents", async () => {
      const collection = client.db(dbName).collection("agg_count_no_match");
      await collection.insertMany([{ status: "active" }, { status: "active" }]);

      const results = await collection
        .aggregate([{ $match: { status: "deleted" } }, { $count: "count" }])
        .toArray();

      assert.deepStrictEqual(results, []);
    });

    it("should throw for empty field name", async () => {
      const collection = client.db(dbName).collection("agg_count_err_empty");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        async () => {
          await collection.aggregate([{ $count: "" }]).toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("non-empty"));
          return true;
        }
      );
    });

    it("should throw for field name starting with $", async () => {
      const collection = client.db(dbName).collection("agg_count_err_dollar");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        async () => {
          await collection.aggregate([{ $count: "$invalid" }]).toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("$"));
          return true;
        }
      );
    });

    it("should throw for field name containing .", async () => {
      const collection = client.db(dbName).collection("agg_count_err_dot");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        async () => {
          await collection.aggregate([{ $count: "invalid.name" }]).toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("."));
          return true;
        }
      );
    });
  });

  // ==================== $unwind Stage ====================

  describe("$unwind Stage", () => {
    describe("Basic Unwind", () => {
      it("should unwind array into multiple documents", async () => {
        const collection = client.db(dbName).collection("agg_unwind_basic");
        await collection.insertOne({ name: "Alice", tags: ["a", "b", "c"] });

        const results = await collection
          .aggregate([{ $unwind: "$tags" }])
          .toArray();

        assert.strictEqual(results.length, 3);
        assert.strictEqual(results[0].tags, "a");
        assert.strictEqual(results[1].tags, "b");
        assert.strictEqual(results[2].tags, "c");
        assert.ok(results.every((r) => r.name === "Alice"));
      });

      it("should handle single-element arrays", async () => {
        const collection = client.db(dbName).collection("agg_unwind_single");
        await collection.insertOne({ name: "Bob", items: ["only"] });

        const results = await collection
          .aggregate([{ $unwind: "$items" }])
          .toArray();

        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0].items, "only");
      });

      it("should skip documents with missing field", async () => {
        const collection = client.db(dbName).collection("agg_unwind_missing");
        await collection.insertMany([
          { name: "Alice", tags: ["a", "b"] },
          { name: "Bob" }, // No tags field
          { name: "Charlie", tags: ["c"] },
        ]);

        const results = await collection
          .aggregate([{ $unwind: "$tags" }])
          .toArray();

        assert.strictEqual(results.length, 3);
        assert.ok(!results.some((r) => r.name === "Bob"));
      });

      it("should skip documents with null field", async () => {
        const collection = client.db(dbName).collection("agg_unwind_null");
        await collection.insertMany([
          { name: "Alice", tags: ["a"] },
          { name: "Bob", tags: null },
        ]);

        const results = await collection
          .aggregate([{ $unwind: "$tags" }])
          .toArray();

        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0].name, "Alice");
      });

      it("should skip documents with empty array", async () => {
        const collection = client.db(dbName).collection("agg_unwind_empty_arr");
        await collection.insertMany([
          { name: "Alice", tags: ["a", "b"] },
          { name: "Bob", tags: [] },
        ]);

        const results = await collection
          .aggregate([{ $unwind: "$tags" }])
          .toArray();

        assert.strictEqual(results.length, 2);
        assert.ok(!results.some((r) => r.name === "Bob"));
      });

      it("should treat non-array value as single-element array", async () => {
        const collection = client.db(dbName).collection("agg_unwind_scalar");
        await collection.insertOne({ name: "Alice", value: "scalar" });

        const results = await collection
          .aggregate([{ $unwind: "$value" }])
          .toArray();

        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0].value, "scalar");
      });

      it("should handle nested array fields with dot notation", async () => {
        const collection = client.db(dbName).collection("agg_unwind_nested");
        await collection.insertOne({
          name: "Alice",
          data: { items: [1, 2, 3] },
        });

        const results = await collection
          .aggregate([{ $unwind: "$data.items" }])
          .toArray();

        assert.strictEqual(results.length, 3);
        assert.strictEqual((results[0].data as Record<string, unknown>).items, 1);
        assert.strictEqual((results[1].data as Record<string, unknown>).items, 2);
        assert.strictEqual((results[2].data as Record<string, unknown>).items, 3);
      });
    });

    describe("preserveNullAndEmptyArrays Option", () => {
      it("should preserve documents with missing field", async () => {
        const collection = client.db(dbName).collection("agg_unwind_pres_miss");
        await collection.insertMany([
          { name: "Alice", tags: ["a"] },
          { name: "Bob" }, // Missing tags
        ]);

        const results = await collection
          .aggregate([
            { $unwind: { path: "$tags", preserveNullAndEmptyArrays: true } },
          ])
          .toArray();

        assert.strictEqual(results.length, 2);
        assert.ok(results.some((r) => r.name === "Bob"));
      });

      it("should preserve documents with null field", async () => {
        const collection = client.db(dbName).collection("agg_unwind_pres_null");
        await collection.insertMany([
          { name: "Alice", tags: ["a"] },
          { name: "Bob", tags: null },
        ]);

        const results = await collection
          .aggregate([
            { $unwind: { path: "$tags", preserveNullAndEmptyArrays: true } },
          ])
          .toArray();

        assert.strictEqual(results.length, 2);
        const bobDoc = results.find((r) => r.name === "Bob");
        assert.ok(bobDoc);
      });

      it("should preserve documents with empty array", async () => {
        const collection = client.db(dbName).collection("agg_unwind_pres_empty");
        await collection.insertMany([
          { name: "Alice", tags: ["a"] },
          { name: "Bob", tags: [] },
        ]);

        const results = await collection
          .aggregate([
            { $unwind: { path: "$tags", preserveNullAndEmptyArrays: true } },
          ])
          .toArray();

        assert.strictEqual(results.length, 2);
        const bobDoc = results.find((r) => r.name === "Bob");
        assert.ok(bobDoc);
      });
    });

    describe("includeArrayIndex Option", () => {
      it("should add index field to each unwound document", async () => {
        const collection = client.db(dbName).collection("agg_unwind_idx");
        await collection.insertOne({ name: "Alice", tags: ["a", "b", "c"] });

        const results = await collection
          .aggregate([
            { $unwind: { path: "$tags", includeArrayIndex: "idx" } },
          ])
          .toArray();

        assert.strictEqual(results.length, 3);
        assert.strictEqual(results[0].idx, 0);
        assert.strictEqual(results[1].idx, 1);
        assert.strictEqual(results[2].idx, 2);
      });

      it("should start index at 0", async () => {
        const collection = client.db(dbName).collection("agg_unwind_idx_start");
        await collection.insertOne({ items: ["first"] });

        const results = await collection
          .aggregate([
            { $unwind: { path: "$items", includeArrayIndex: "position" } },
          ])
          .toArray();

        assert.strictEqual(results[0].position, 0);
      });

      it("should work with preserveNullAndEmptyArrays", async () => {
        const collection = client.db(dbName).collection("agg_unwind_idx_pres");
        await collection.insertMany([
          { name: "Alice", tags: ["a", "b"] },
          { name: "Bob", tags: null },
        ]);

        const results = await collection
          .aggregate([
            {
              $unwind: {
                path: "$tags",
                includeArrayIndex: "idx",
                preserveNullAndEmptyArrays: true,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results.length, 3);
        const bobDoc = results.find((r) => r.name === "Bob");
        assert.ok(bobDoc);
        assert.strictEqual(bobDoc.idx, null);
      });
    });

    describe("Error Cases", () => {
      it("should throw for path not starting with $", async () => {
        const collection = client.db(dbName).collection("agg_unwind_err_path");
        await collection.insertOne({ tags: ["a", "b"] });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $unwind: "tags" }]) // Missing $
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$"));
            return true;
          }
        );
      });
    });
  });

  // ==================== Combined Pipeline Tests ====================

  describe("Combined Pipeline Tests", () => {
    it("should execute $match -> $sort -> $limit", async () => {
      const collection = client.db(dbName).collection("agg_combo_msl");
      await collection.insertMany([
        { status: "active", score: 50 },
        { status: "active", score: 80 },
        { status: "inactive", score: 90 },
        { status: "active", score: 70 },
        { status: "active", score: 60 },
      ]);

      const results = await collection
        .aggregate([
          { $match: { status: "active" } },
          { $sort: { score: -1 } },
          { $limit: 2 },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0].score, 80);
      assert.strictEqual(results[1].score, 70);
    });

    it("should execute $match -> $project", async () => {
      const collection = client.db(dbName).collection("agg_combo_mp");
      await collection.insertMany([
        { name: "Alice", age: 25, email: "alice@test.com", status: "active" },
        { name: "Bob", age: 30, email: "bob@test.com", status: "inactive" },
      ]);

      const results = await collection
        .aggregate([
          { $match: { status: "active" } },
          { $project: { name: 1, email: 1, _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].name, "Alice");
      assert.strictEqual(results[0].email, "alice@test.com");
      assert.strictEqual(results[0].age, undefined);
      assert.strictEqual(results[0].status, undefined);
    });

    it("should handle complex multi-stage pipeline", async () => {
      const collection = client.db(dbName).collection("agg_combo_complex");
      await collection.insertMany([
        { category: "A", items: ["x", "y"], value: 10 },
        { category: "B", items: ["z"], value: 20 },
        { category: "A", items: ["w", "v", "u"], value: 30 },
      ]);

      const results = await collection
        .aggregate([
          { $match: { category: "A" } },
          { $unwind: "$items" },
          { $sort: { value: -1 } },
          { $project: { item: "$items", value: 1, _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 5); // 2 + 3 items
      assert.strictEqual(results[0].value, 30);
      assert.ok(["w", "v", "u"].includes(results[0].item as string));
    });

    it("should preserve document order through stages", async () => {
      const collection = client.db(dbName).collection("agg_combo_order");
      await collection.insertMany([
        { order: 1 },
        { order: 2 },
        { order: 3 },
      ]);

      const results = await collection
        .aggregate([
          { $sort: { order: 1 } },
          { $project: { order: 1, _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results[0].order, 1);
      assert.strictEqual(results[1].order, 2);
      assert.strictEqual(results[2].order, 3);
    });
  });
});
