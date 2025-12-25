/**
 * Phase 5: Infrastructure Tests
 *
 * Tests for the preparatory utilities (Tier 0) that support
 * the extended aggregation stages.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "./test-harness.ts";
import {
  partitionDocuments,
  sortPartition,
} from "../src/aggregation/partition.ts";
import { evaluateExpression } from "../src/aggregation/expression.ts";
import { traverseDocument } from "../src/aggregation/traverse.ts";
import {
  applyLocf,
  applyLinearFill,
  isGap,
  getFirstNonNull,
  getLastNonNull,
} from "../src/aggregation/gap-fill.ts";
import {
  addDateStep,
  dateDiff,
  generateDateSequence,
  isValidTimeUnit,
} from "../src/aggregation/date-utils.ts";

describe(`Phase 5 Infrastructure (${getTestModeName()})`, () => {
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

  // ==================== Task 5.0.1: System Variables ====================

  describe("System Variables", () => {
    describe("$$NOW", () => {
      it("should return current date in $project", async () => {
        const collection = client.db(dbName).collection("sysvars_now");
        await collection.insertOne({ name: "test" });

        const before = new Date();
        const results = await collection
          .aggregate([{ $project: { currentTime: "$$NOW", _id: 0 } }])
          .toArray();
        const after = new Date();

        assert.ok(results[0].currentTime instanceof Date);
        assert.ok(results[0].currentTime >= before);
        assert.ok(results[0].currentTime <= after);
      });

      it("should return same date for all documents in pipeline", async () => {
        const collection = client.db(dbName).collection("sysvars_now_same");
        await collection.insertMany([{ n: 1 }, { n: 2 }, { n: 3 }]);

        const results = await collection
          .aggregate([{ $project: { time: "$$NOW", n: 1, _id: 0 } }])
          .toArray();

        // All documents should have the same $$NOW value
        const firstTime = (results[0].time as Date).getTime();
        for (const doc of results) {
          assert.strictEqual((doc.time as Date).getTime(), firstTime);
        }
      });
    });

    describe("$$ROOT", () => {
      it("should return the original document", async () => {
        const collection = client.db(dbName).collection("sysvars_root");
        await collection.insertOne({ name: "Alice", age: 30 });

        const results = await collection
          .aggregate([{ $project: { original: "$$ROOT", _id: 0 } }])
          .toArray();

        const original = results[0].original as { name: string; age: number };
        assert.strictEqual(original.name, "Alice");
        assert.strictEqual(original.age, 30);
      });

      it("should allow accessing $$ROOT fields with dot notation", async () => {
        const collection = client.db(dbName).collection("sysvars_root_dot");
        await collection.insertOne({ user: { name: "Bob" } });

        const results = await collection
          .aggregate([{ $project: { userName: "$$ROOT.user.name", _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].userName, "Bob");
      });

      it("should preserve $$ROOT through $addFields", async () => {
        const collection = client.db(dbName).collection("sysvars_root_addfields");
        await collection.insertOne({ x: 10 });

        const results = await collection
          .aggregate([
            { $addFields: { root: "$$ROOT" } },
            { $project: { originalX: "$root.x", _id: 0 } },
          ])
          .toArray();

        assert.strictEqual(results[0].originalX, 10);
      });
    });

    describe("$$DESCEND, $$PRUNE, $$KEEP (for $redact)", () => {
      it("should have $$DESCEND as string constant", async () => {
        const collection = client.db(dbName).collection("sysvars_descend");
        await collection.insertOne({ a: 1 });

        const results = await collection
          .aggregate([{ $project: { action: "$$DESCEND", _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].action, "descend");
      });

      it("should have $$PRUNE as string constant", async () => {
        const collection = client.db(dbName).collection("sysvars_prune");
        await collection.insertOne({ a: 1 });

        const results = await collection
          .aggregate([{ $project: { action: "$$PRUNE", _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].action, "prune");
      });

      it("should have $$KEEP as string constant", async () => {
        const collection = client.db(dbName).collection("sysvars_keep");
        await collection.insertOne({ a: 1 });

        const results = await collection
          .aggregate([{ $project: { action: "$$KEEP", _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].action, "keep");
      });
    });
  });

  // ==================== Task 5.0.2: Partition Grouping ====================

  describe("Partition Grouping", () => {
    describe("partitionDocuments", () => {
      it("should put all docs in single partition when no options", () => {
        const docs = [{ x: 1 }, { x: 2 }, { x: 3 }];
        const result = partitionDocuments(docs, {}, evaluateExpression);

        assert.strictEqual(result.size, 1);
        assert.strictEqual(result.get("")!.length, 3);
      });

      it("should partition by single field using partitionByFields", () => {
        const docs = [
          { category: "A", value: 1 },
          { category: "B", value: 2 },
          { category: "A", value: 3 },
        ];
        const result = partitionDocuments(
          docs,
          { partitionByFields: ["category"] },
          evaluateExpression
        );

        assert.strictEqual(result.size, 2);
        const groupA = result.get('["A"]')!;
        const groupB = result.get('["B"]')!;
        assert.strictEqual(groupA.length, 2);
        assert.strictEqual(groupB.length, 1);
      });

      it("should partition by multiple fields using partitionByFields", () => {
        const docs = [
          { a: 1, b: "x", val: 10 },
          { a: 1, b: "y", val: 20 },
          { a: 1, b: "x", val: 30 },
          { a: 2, b: "x", val: 40 },
        ];
        const result = partitionDocuments(
          docs,
          { partitionByFields: ["a", "b"] },
          evaluateExpression
        );

        assert.strictEqual(result.size, 3);
        assert.strictEqual(result.get('[1,"x"]')!.length, 2);
        assert.strictEqual(result.get('[1,"y"]')!.length, 1);
        assert.strictEqual(result.get('[2,"x"]')!.length, 1);
      });

      it("should partition using partitionBy expression object", () => {
        const docs = [
          { x: 1, y: 10 },
          { x: 2, y: 20 },
          { x: 1, y: 30 },
        ];
        const result = partitionDocuments(
          docs,
          { partitionBy: { key: "$x" } },
          evaluateExpression
        );

        assert.strictEqual(result.size, 2);
      });

      it("should throw error if partitionBy is a string", () => {
        const docs = [{ x: 1 }];
        assert.throws(
          () =>
            partitionDocuments(
              docs,
              { partitionBy: "$x" as unknown as Record<string, unknown> },
              evaluateExpression
            ),
          /partitionBy must be an object expression/
        );
      });

      it("should handle undefined field values", () => {
        const docs = [{ a: 1, b: 10 }, { a: 1 }, { a: 2, b: 20 }];
        const result = partitionDocuments(
          docs,
          { partitionByFields: ["a", "b"] },
          evaluateExpression
        );

        // [1, undefined] and [1, 10] and [2, 20]
        assert.strictEqual(result.size, 3);
      });
    });

    describe("sortPartition", () => {
      it("should sort by single field ascending", () => {
        const docs = [{ x: 3 }, { x: 1 }, { x: 2 }];
        const result = sortPartition(docs, { x: 1 });

        assert.deepStrictEqual(
          result.map((d) => d.x),
          [1, 2, 3]
        );
      });

      it("should sort by single field descending", () => {
        const docs = [{ x: 1 }, { x: 3 }, { x: 2 }];
        const result = sortPartition(docs, { x: -1 });

        assert.deepStrictEqual(
          result.map((d) => d.x),
          [3, 2, 1]
        );
      });

      it("should sort by multiple fields", () => {
        const docs = [
          { a: 1, b: 2 },
          { a: 2, b: 1 },
          { a: 1, b: 1 },
        ];
        const result = sortPartition(docs, { a: 1, b: 1 });

        assert.deepStrictEqual(
          result.map((d) => [d.a, d.b]),
          [
            [1, 1],
            [1, 2],
            [2, 1],
          ]
        );
      });

      it("should not mutate original array", () => {
        const docs = [{ x: 3 }, { x: 1 }, { x: 2 }];
        const original = [...docs];
        sortPartition(docs, { x: 1 });

        assert.deepStrictEqual(docs, original);
      });
    });
  });

  // ==================== Task 5.0.4: Recursive Document Traversal ====================

  describe("Recursive Document Traversal", () => {
    describe("traverseDocument", () => {
      it("should return document as-is when callback returns keep", () => {
        const doc = { a: 1, b: { c: 2 } };
        const result = traverseDocument(doc, () => "keep");

        assert.deepStrictEqual(result, doc);
      });

      it("should return null when callback returns prune", () => {
        const doc = { a: 1 };
        const result = traverseDocument(doc, () => "prune");

        assert.strictEqual(result, null);
      });

      it("should recurse into nested documents when callback returns descend", () => {
        const doc = {
          level: 1,
          secret: true,
          nested: {
            level: 2,
            secret: false,
          },
        };

        // Prune any sub-document where secret is true
        const result = traverseDocument(doc, (subdoc) => {
          if (subdoc.secret === true) {
            return "prune";
          }
          return "descend";
        });

        // Outer doc has secret=true, so entire doc is pruned
        assert.strictEqual(result, null);
      });

      it("should keep nested doc when callback returns descend and nested passes", () => {
        const doc = {
          level: 1,
          secret: false,
          nested: {
            level: 2,
            secret: false,
            data: "visible",
          },
        };

        const result = traverseDocument(doc, (subdoc) => {
          if (subdoc.secret === true) {
            return "prune";
          }
          return "descend";
        });

        assert.deepStrictEqual(result, doc);
      });

      it("should prune nested documents that fail callback", () => {
        const doc = {
          level: 1,
          secret: false,
          nested: {
            level: 2,
            secret: true,
          },
        };

        const result = traverseDocument(doc, (subdoc) => {
          if (subdoc.secret === true) {
            return "prune";
          }
          return "descend";
        });

        // Nested is pruned, but top level remains without nested field
        assert.deepStrictEqual(result, { level: 1, secret: false });
      });

      it("should handle arrays of documents", () => {
        const doc = {
          items: [
            { value: 1, public: true },
            { value: 2, public: false },
            { value: 3, public: true },
          ],
        };

        const result = traverseDocument(doc, (subdoc) => {
          if (subdoc.public === false) {
            return "prune";
          }
          return "descend";
        });

        assert.deepStrictEqual(result, {
          items: [
            { value: 1, public: true },
            { value: 3, public: true },
          ],
        });
      });

      it("should keep scalar array elements when descending", () => {
        const doc = {
          tags: ["a", "b", "c"],
          nested: { data: 123 },
        };

        const result = traverseDocument(doc, () => "descend");

        assert.deepStrictEqual(result, doc);
      });

      it("should handle deeply nested structures", () => {
        const doc = {
          a: {
            b: {
              c: {
                secret: true,
              },
            },
          },
        };

        const result = traverseDocument(doc, (subdoc) => {
          if (subdoc.secret === true) {
            return "prune";
          }
          return "descend";
        });

        assert.deepStrictEqual(result, { a: { b: {} } });
      });

      it("should preserve Date objects in documents", () => {
        const now = new Date();
        const doc = { created: now, nested: { updated: now } };

        const result = traverseDocument(doc, () => "descend");

        assert.strictEqual(result!.created, now);
        assert.strictEqual((result!.nested as { updated: Date }).updated, now);
      });
    });
  });

  // ==================== Task 5.0.5: Gap Filling Utilities ====================

  describe("Gap Filling Utilities", () => {
    describe("applyLocf", () => {
      it("should fill null values with last non-null value", () => {
        const values = [1, null, null, 4, null];
        const result = applyLocf(values);

        assert.deepStrictEqual(result, [1, 1, 1, 4, 4]);
      });

      it("should leave nulls at start as null", () => {
        const values = [null, null, 3, null, 5];
        const result = applyLocf(values);

        assert.deepStrictEqual(result, [null, null, 3, 3, 5]);
      });

      it("should handle all-null input", () => {
        const values = [null, null, null];
        const result = applyLocf(values);

        assert.deepStrictEqual(result, [null, null, null]);
      });

      it("should handle no nulls", () => {
        const values = [1, 2, 3];
        const result = applyLocf(values);

        assert.deepStrictEqual(result, [1, 2, 3]);
      });

      it("should handle undefined as gap", () => {
        const values = [1, undefined, 3];
        const result = applyLocf(values);

        assert.deepStrictEqual(result, [1, 1, 3]);
      });

      it("should work with non-numeric values", () => {
        const values = ["a", null, null, "b", null];
        const result = applyLocf(values);

        assert.deepStrictEqual(result, ["a", "a", "a", "b", "b"]);
      });
    });

    describe("applyLinearFill", () => {
      it("should linearly interpolate between values", () => {
        const values = [0, null, null, 6];
        const result = applyLinearFill(values);

        assert.deepStrictEqual(result, [0, 2, 4, 6]);
      });

      it("should leave nulls at start as null", () => {
        const values = [null, null, 4, null, 8];
        const result = applyLinearFill(values);

        assert.deepStrictEqual(result, [null, null, 4, 6, 8]);
      });

      it("should leave nulls at end as null", () => {
        const values = [2, null, 6, null, null];
        const result = applyLinearFill(values);

        assert.deepStrictEqual(result, [2, 4, 6, null, null]);
      });

      it("should handle all-null input", () => {
        const values = [null, null, null];
        const result = applyLinearFill(values);

        assert.deepStrictEqual(result, [null, null, null]);
      });

      it("should handle single gap", () => {
        const values = [10, null, 20];
        const result = applyLinearFill(values);

        assert.deepStrictEqual(result, [10, 15, 20]);
      });

      it("should use positions for non-uniform interpolation", () => {
        // Values at positions 0, 2, 4 (but index 1 is at position 2)
        const values = [0, null, 100];
        const positions = [0, 20, 100];
        const result = applyLinearFill(values, positions);

        // At position 20, interpolate between 0 (at 0) and 100 (at 100)
        // fraction = 20/100 = 0.2, value = 0 + 0.2 * 100 = 20
        assert.deepStrictEqual(result, [0, 20, 100]);
      });

      it("should handle multiple separate gaps", () => {
        const values = [0, null, 4, null, 8];
        const result = applyLinearFill(values);

        assert.deepStrictEqual(result, [0, 2, 4, 6, 8]);
      });
    });

    describe("helper functions", () => {
      it("isGap should identify null and undefined", () => {
        assert.strictEqual(isGap(null), true);
        assert.strictEqual(isGap(undefined), true);
        assert.strictEqual(isGap(0), false);
        assert.strictEqual(isGap(""), false);
        assert.strictEqual(isGap(false), false);
      });

      it("getFirstNonNull should return first non-null value", () => {
        assert.strictEqual(getFirstNonNull([null, 1, 2, 3]), 1);
        assert.strictEqual(getFirstNonNull([null, null, null]), null);
        assert.strictEqual(getFirstNonNull([5, null, null]), 5);
      });

      it("getLastNonNull should return last non-null value", () => {
        assert.strictEqual(getLastNonNull([1, 2, 3, null]), 3);
        assert.strictEqual(getLastNonNull([null, null, null]), null);
        assert.strictEqual(getLastNonNull([null, null, 5]), 5);
      });
    });
  });

  // ==================== Task 5.0.6: Date Stepping Utility ====================

  describe("Date Stepping Utility", () => {
    describe("addDateStep", () => {
      it("should add days to a date", () => {
        const date = new Date("2024-01-15T00:00:00Z");
        const result = addDateStep(date, 5, "day");

        assert.strictEqual(result.toISOString(), "2024-01-20T00:00:00.000Z");
      });

      it("should subtract days when step is negative", () => {
        const date = new Date("2024-01-15T00:00:00Z");
        const result = addDateStep(date, -5, "day");

        assert.strictEqual(result.toISOString(), "2024-01-10T00:00:00.000Z");
      });

      it("should add months with calendar awareness", () => {
        const date = new Date("2024-01-31T00:00:00Z");
        const result = addDateStep(date, 1, "month");

        // January 31 + 1 month = February 29 (2024 is leap year)
        assert.strictEqual(result.toISOString(), "2024-03-02T00:00:00.000Z");
      });

      it("should add years correctly", () => {
        const date = new Date("2024-06-15T00:00:00Z");
        const result = addDateStep(date, 2, "year");

        assert.strictEqual(result.toISOString(), "2026-06-15T00:00:00.000Z");
      });

      it("should handle leap year edge case", () => {
        // Feb 29 in leap year + 1 year
        const date = new Date("2024-02-29T00:00:00Z");
        const result = addDateStep(date, 1, "year");

        // 2025 is not a leap year, so Feb 29 becomes March 1
        assert.strictEqual(result.toISOString(), "2025-03-01T00:00:00.000Z");
      });

      it("should add hours correctly", () => {
        const date = new Date("2024-01-15T10:00:00Z");
        const result = addDateStep(date, 14, "hour");

        assert.strictEqual(result.toISOString(), "2024-01-16T00:00:00.000Z");
      });

      it("should add weeks correctly", () => {
        const date = new Date("2024-01-15T00:00:00Z");
        const result = addDateStep(date, 2, "week");

        assert.strictEqual(result.toISOString(), "2024-01-29T00:00:00.000Z");
      });

      it("should add quarters correctly", () => {
        const date = new Date("2024-01-15T00:00:00Z");
        const result = addDateStep(date, 1, "quarter");

        assert.strictEqual(result.toISOString(), "2024-04-15T00:00:00.000Z");
      });

      it("should not mutate original date", () => {
        const date = new Date("2024-01-15T00:00:00Z");
        const original = date.toISOString();
        addDateStep(date, 5, "day");

        assert.strictEqual(date.toISOString(), original);
      });
    });

    describe("dateDiff", () => {
      it("should calculate difference in days", () => {
        const start = new Date("2024-01-15T00:00:00Z");
        const end = new Date("2024-01-20T00:00:00Z");

        assert.strictEqual(dateDiff(start, end, "day"), 5);
      });

      it("should calculate difference in months", () => {
        const start = new Date("2024-01-15T00:00:00Z");
        const end = new Date("2024-04-15T00:00:00Z");

        assert.strictEqual(dateDiff(start, end, "month"), 3);
      });

      it("should calculate difference in years", () => {
        const start = new Date("2022-01-15T00:00:00Z");
        const end = new Date("2024-01-15T00:00:00Z");

        assert.strictEqual(dateDiff(start, end, "year"), 2);
      });

      it("should return negative for reversed dates", () => {
        const start = new Date("2024-01-20T00:00:00Z");
        const end = new Date("2024-01-15T00:00:00Z");

        assert.strictEqual(dateDiff(start, end, "day"), -5);
      });
    });

    describe("generateDateSequence", () => {
      it("should generate sequence of dates", () => {
        const start = new Date("2024-01-01T00:00:00Z");
        const end = new Date("2024-01-05T00:00:00Z");
        const result = generateDateSequence(start, end, 1, "day");

        assert.strictEqual(result.length, 4);
        assert.strictEqual(result[0].toISOString(), "2024-01-01T00:00:00.000Z");
        assert.strictEqual(result[3].toISOString(), "2024-01-04T00:00:00.000Z");
      });

      it("should generate sequence with step > 1", () => {
        const start = new Date("2024-01-01T00:00:00Z");
        const end = new Date("2024-01-10T00:00:00Z");
        const result = generateDateSequence(start, end, 2, "day");

        assert.strictEqual(result.length, 5);
        assert.strictEqual(result[0].toISOString(), "2024-01-01T00:00:00.000Z");
        assert.strictEqual(result[1].toISOString(), "2024-01-03T00:00:00.000Z");
      });

      it("should return empty array when start >= end", () => {
        const start = new Date("2024-01-10T00:00:00Z");
        const end = new Date("2024-01-05T00:00:00Z");
        const result = generateDateSequence(start, end, 1, "day");

        assert.strictEqual(result.length, 0);
      });

      it("should generate hourly sequence", () => {
        const start = new Date("2024-01-01T00:00:00Z");
        const end = new Date("2024-01-01T04:00:00Z");
        const result = generateDateSequence(start, end, 1, "hour");

        assert.strictEqual(result.length, 4);
      });
    });

    describe("isValidTimeUnit", () => {
      it("should return true for valid units", () => {
        assert.strictEqual(isValidTimeUnit("day"), true);
        assert.strictEqual(isValidTimeUnit("month"), true);
        assert.strictEqual(isValidTimeUnit("year"), true);
        assert.strictEqual(isValidTimeUnit("hour"), true);
        assert.strictEqual(isValidTimeUnit("minute"), true);
        assert.strictEqual(isValidTimeUnit("second"), true);
        assert.strictEqual(isValidTimeUnit("millisecond"), true);
        assert.strictEqual(isValidTimeUnit("week"), true);
        assert.strictEqual(isValidTimeUnit("quarter"), true);
      });

      it("should return false for invalid units", () => {
        assert.strictEqual(isValidTimeUnit("days"), false);
        assert.strictEqual(isValidTimeUnit("invalid"), false);
        assert.strictEqual(isValidTimeUnit(""), false);
      });
    });
  });

  // ==================== Tier 1 Stages ====================

  describe("$replaceWith (Task 5.3)", () => {
    it("should replace document with field reference", async () => {
      const collection = client.db(dbName).collection("replaceWith_field");
      await collection.insertOne({ original: 1, nested: { a: 10, b: 20 } });

      const results = await collection
        .aggregate([{ $replaceWith: "$nested" }])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].a, 10);
      assert.strictEqual(results[0].b, 20);
      assert.strictEqual(results[0].original, undefined);
    });

    it("should replace document with literal object", async () => {
      const collection = client.db(dbName).collection("replaceWith_literal");
      await collection.insertOne({ x: 1 });

      const results = await collection
        .aggregate([{ $replaceWith: { $literal: { fixed: "value" } } }])
        .toArray();

      assert.strictEqual(results[0].fixed, "value");
    });

    it("should work with $$ROOT", async () => {
      const collection = client.db(dbName).collection("replaceWith_root");
      await collection.insertOne({ a: 1, b: 2 });

      const results = await collection
        .aggregate([
          { $addFields: { backup: "$$ROOT" } },
          { $replaceWith: "$backup" },
        ])
        .toArray();

      assert.strictEqual(results[0].a, 1);
      assert.strictEqual(results[0].b, 2);
    });

    it("should throw error when result is null", async () => {
      const collection = client.db(dbName).collection("replaceWith_null");
      await collection.insertOne({ x: 1 });

      await assert.rejects(
        () =>
          collection
            .aggregate([{ $replaceWith: "$nonexistent" }])
            .toArray(),
        /newRoot.*must evaluate to an object/
      );
    });

    it("should throw error when result is array", async () => {
      const collection = client.db(dbName).collection("replaceWith_array");
      await collection.insertOne({ items: [1, 2, 3] });

      await assert.rejects(
        () =>
          collection.aggregate([{ $replaceWith: "$items" }]).toArray(),
        /newRoot.*must evaluate to an object/
      );
    });

    it("should throw error when result is scalar", async () => {
      const collection = client.db(dbName).collection("replaceWith_scalar");
      await collection.insertOne({ value: 123 });

      await assert.rejects(
        () =>
          collection.aggregate([{ $replaceWith: "$value" }]).toArray(),
        /newRoot.*must evaluate to an object/
      );
    });
  });

  describe("$unset (Task 5.4)", () => {
    it("should remove single field with string syntax", async () => {
      const collection = client.db(dbName).collection("unset_single");
      await collection.insertOne({ a: 1, b: 2, c: 3 });

      const results = await collection.aggregate([{ $unset: "b" }]).toArray();

      assert.strictEqual(results[0].a, 1);
      assert.strictEqual(results[0].b, undefined);
      assert.strictEqual(results[0].c, 3);
    });

    it("should remove multiple fields with array syntax", async () => {
      const collection = client.db(dbName).collection("unset_array");
      await collection.insertOne({ a: 1, b: 2, c: 3, d: 4 });

      const results = await collection
        .aggregate([{ $unset: ["b", "d"] }])
        .toArray();

      assert.strictEqual(results[0].a, 1);
      assert.strictEqual(results[0].b, undefined);
      assert.strictEqual(results[0].c, 3);
      assert.strictEqual(results[0].d, undefined);
    });

    it("should remove nested field with dot notation", async () => {
      const collection = client.db(dbName).collection("unset_nested");
      await collection.insertOne({ top: { a: 1, b: 2 }, other: 3 });

      const results = await collection
        .aggregate([{ $unset: "top.a" }])
        .toArray();

      assert.strictEqual((results[0].top as { b: number }).b, 2);
      assert.strictEqual(
        (results[0].top as { a?: number }).a,
        undefined
      );
      assert.strictEqual(results[0].other, 3);
    });

    it("should silently ignore non-existent fields", async () => {
      const collection = client.db(dbName).collection("unset_nonexistent");
      await collection.insertOne({ a: 1 });

      const results = await collection
        .aggregate([{ $unset: "nonexistent" }])
        .toArray();

      assert.strictEqual(results[0].a, 1);
    });

    it("should preserve _id when unsetting other fields", async () => {
      const collection = client.db(dbName).collection("unset_id");
      await collection.insertOne({ a: 1, b: 2 });

      const results = await collection.aggregate([{ $unset: "a" }]).toArray();

      assert.ok(results[0]._id);
      assert.strictEqual(results[0].b, 2);
    });

    it("should throw error for non-string in array", async () => {
      const collection = client.db(dbName).collection("unset_invalid");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        () =>
          collection
            .aggregate([{ $unset: ["a", 123 as unknown as string] }])
            .toArray(),
        /\$unset specification must be a string or array of strings/
      );
    });

    it("should throw error for empty string field", async () => {
      const collection = client.db(dbName).collection("unset_empty");
      await collection.insertOne({ a: 1 });

      await assert.rejects(
        () => collection.aggregate([{ $unset: "" }]).toArray(),
        /FieldPath cannot be constructed with empty string/
      );
    });
  });

  describe("$documents (Task 5.5)", () => {
    it("should inject literal documents as first stage", async () => {
      const collection = client.db(dbName).collection("documents_literal");

      const results = await collection
        .aggregate([
          { $documents: [{ a: 1 }, { a: 2 }, { a: 3 }] },
          { $match: { a: { $gt: 1 } } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0].a, 2);
      assert.strictEqual(results[1].a, 3);
    });

    it("should work with empty array", async () => {
      const collection = client.db(dbName).collection("documents_empty");

      const results = await collection
        .aggregate([{ $documents: [] }, { $project: { _id: 0 } }])
        .toArray();

      assert.strictEqual(results.length, 0);
    });

    it("should support $$NOW in documents", async () => {
      const collection = client.db(dbName).collection("documents_now");

      const before = new Date();
      const results = await collection
        .aggregate([
          {
            $documents: [{ timestamp: "$$NOW" }],
          },
        ])
        .toArray();
      const after = new Date();

      assert.ok(results[0].timestamp instanceof Date);
      assert.ok((results[0].timestamp as Date) >= before);
      assert.ok((results[0].timestamp as Date) <= after);
    });

    it("should throw error when not first stage", async () => {
      const collection = client.db(dbName).collection("documents_notfirst");
      await collection.insertOne({ x: 1 });

      await assert.rejects(
        () =>
          collection
            .aggregate([
              { $match: {} },
              { $documents: [{ a: 1 }] },
            ])
            .toArray(),
        /\$documents must be the first stage/
      );
    });

    it("should throw error for non-array", async () => {
      const collection = client.db(dbName).collection("documents_nonarray");

      await assert.rejects(
        () =>
          collection
            .aggregate([{ $documents: { a: 1 } }])
            .toArray(),
        /\$documents requires array of documents/
      );
    });

    it("should throw error for non-object elements", async () => {
      const collection = client.db(dbName).collection("documents_nonobj");

      await assert.rejects(
        () =>
          collection
            .aggregate([{ $documents: [{ a: 1 }, "string"] }])
            .toArray(),
        /\$documents array elements must be objects/
      );
    });

    it("should work with subsequent stages", async () => {
      const collection = client.db(dbName).collection("documents_stages");

      const results = await collection
        .aggregate([
          { $documents: [{ x: 5 }, { x: 10 }, { x: 15 }] },
          { $addFields: { doubled: { $multiply: ["$x", 2] } } },
          { $project: { _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].doubled, 10);
      assert.strictEqual(results[1].doubled, 20);
      assert.strictEqual(results[2].doubled, 30);
    });
  });

  // ==================== Tier 2 Stages ====================

  describe("$redact (Task 5.2)", () => {
    it("should prune documents matching condition", async () => {
      const collection = client.db(dbName).collection("redact_prune");
      await collection.insertMany([
        { level: "public", data: "visible" },
        { level: "secret", data: "hidden" },
      ]);

      const results = await collection
        .aggregate([
          {
            $redact: {
              $cond: {
                if: { $eq: ["$level", "secret"] },
                then: "$$PRUNE",
                else: "$$KEEP",
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].level, "public");
    });

    it("should keep documents matching condition", async () => {
      const collection = client.db(dbName).collection("redact_keep");
      await collection.insertMany([
        { authorized: true, data: "visible" },
        { authorized: false, data: "hidden" },
      ]);

      const results = await collection
        .aggregate([
          {
            $redact: {
              $cond: {
                if: "$authorized",
                then: "$$KEEP",
                else: "$$PRUNE",
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].authorized, true);
    });

    it("should descend into nested documents", async () => {
      const collection = client.db(dbName).collection("redact_descend");
      await collection.insertOne({
        level: "public",
        nested: {
          level: "secret",
          value: 123,
        },
        other: {
          level: "public",
          value: 456,
        },
      });

      const results = await collection
        .aggregate([
          {
            $redact: {
              $cond: {
                if: { $eq: ["$level", "secret"] },
                then: "$$PRUNE",
                else: "$$DESCEND",
              },
            },
          },
          { $project: { _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      // nested should be pruned, other should remain
      assert.strictEqual(results[0].nested, undefined);
      assert.strictEqual((results[0].other as { value: number }).value, 456);
    });

    it("should handle arrays of documents", async () => {
      const collection = client.db(dbName).collection("redact_arrays");
      await collection.insertOne({
        level: "public",
        items: [
          { level: "public", name: "item1" },
          { level: "secret", name: "item2" },
          { level: "public", name: "item3" },
        ],
      });

      const results = await collection
        .aggregate([
          {
            $redact: {
              $cond: {
                if: { $eq: ["$level", "secret"] },
                then: "$$PRUNE",
                else: "$$DESCEND",
              },
            },
          },
          { $project: { _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      const items = results[0].items as { name: string }[];
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].name, "item1");
      assert.strictEqual(items[1].name, "item3");
    });

    it("should throw error for invalid result", async () => {
      const collection = client.db(dbName).collection("redact_invalid");
      await collection.insertOne({ x: 1 });

      await assert.rejects(
        () =>
          collection
            .aggregate([{ $redact: { $literal: "invalid" } }])
            .toArray(),
        /\$redact must resolve to \$\$DESCEND, \$\$PRUNE, or \$\$KEEP/
      );
    });

    it("should use $$DESCEND correctly to stop at field level", async () => {
      const collection = client.db(dbName).collection("redact_descend_stop");
      await collection.insertOne({
        public: true,
        data: {
          public: false,
          secret: "should be pruned",
        },
      });

      const results = await collection
        .aggregate([
          {
            $redact: {
              $cond: {
                if: { $eq: ["$public", true] },
                then: "$$DESCEND",
                else: "$$PRUNE",
              },
            },
          },
          { $project: { _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      // data.public is false, so data should be pruned
      assert.strictEqual(results[0].data, undefined);
    });
  });

  describe("$graphLookup (Task 5.1)", () => {
    it("should traverse simple hierarchy", async () => {
      // Create employees collection with manager hierarchy
      const employees = client.db(dbName).collection("graph_employees");
      await employees.insertMany([
        { _id: "alice", name: "Alice", reportsTo: null },
        { _id: "bob", name: "Bob", reportsTo: "alice" },
        { _id: "charlie", name: "Charlie", reportsTo: "bob" },
        { _id: "dana", name: "Dana", reportsTo: "bob" },
      ]);

      // Find all reports (direct and indirect) for Alice
      const results = await employees
        .aggregate([
          { $match: { _id: "alice" } },
          {
            $graphLookup: {
              from: "graph_employees",
              startWith: "$_id",
              connectFromField: "reportsTo",
              connectToField: "_id",
              as: "reports",
            },
          },
        ])
        .toArray();

      // Note: This finds docs where connectToField (_id) matches startWith
      // So it will find Alice herself since her _id matches
      assert.strictEqual(results.length, 1);
      assert.ok(Array.isArray(results[0].reports));
    });

    it("should limit depth with maxDepth", async () => {
      const nodes = client.db(dbName).collection("graph_nodes");
      await nodes.insertMany([
        { _id: 1, parent: null },
        { _id: 2, parent: 1 },
        { _id: 3, parent: 2 },
        { _id: 4, parent: 3 },
      ]);

      const results = await nodes
        .aggregate([
          { $match: { _id: 1 } },
          {
            $graphLookup: {
              from: "graph_nodes",
              startWith: "$_id",
              connectFromField: "parent",
              connectToField: "_id",
              as: "ancestors",
              maxDepth: 1,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      // maxDepth 1 means depth 0 and depth 1 only
      assert.ok((results[0].ancestors as unknown[]).length <= 2);
    });

    it("should track depth with depthField", async () => {
      const categories = client.db(dbName).collection("graph_categories");
      await categories.insertMany([
        { _id: "root", name: "Root", parentId: null },
        { _id: "electronics", name: "Electronics", parentId: "root" },
        { _id: "phones", name: "Phones", parentId: "electronics" },
      ]);

      const results = await categories
        .aggregate([
          { $match: { _id: "root" } },
          {
            $graphLookup: {
              from: "graph_categories",
              startWith: "$_id",
              connectFromField: "parentId",
              connectToField: "_id",
              as: "descendants",
              depthField: "level",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      // Check that depthField is added
      for (const desc of results[0].descendants as { level: number }[]) {
        assert.ok(typeof desc.level === "number");
      }
    });

    it("should filter with restrictSearchWithMatch", async () => {
      const items = client.db(dbName).collection("graph_items");
      await items.insertMany([
        { _id: "a", nextId: "b", active: true },
        { _id: "b", nextId: "c", active: false },
        { _id: "c", nextId: null, active: true },
      ]);

      const results = await items
        .aggregate([
          { $match: { _id: "a" } },
          {
            $graphLookup: {
              from: "graph_items",
              startWith: "$nextId",
              connectFromField: "nextId",
              connectToField: "_id",
              as: "chain",
              restrictSearchWithMatch: { active: true },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      // Only active items should be in chain
      const chain = results[0].chain as { _id: string; active: boolean }[];
      for (const item of chain) {
        assert.strictEqual(item.active, true);
      }
    });

    it("should return empty array for null startWith", async () => {
      const coll = client.db(dbName).collection("graph_null_start");
      await coll.insertOne({ _id: 1, ref: null });

      const results = await coll
        .aggregate([
          {
            $graphLookup: {
              from: "graph_null_start",
              startWith: "$ref",
              connectFromField: "ref",
              connectToField: "_id",
              as: "found",
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.deepStrictEqual(results[0].found, []);
    });

    it("should throw error for missing required fields", async () => {
      const coll = client.db(dbName).collection("graph_error_test");
      await coll.insertOne({ x: 1 });

      await assert.rejects(
        () =>
          coll
            .aggregate([
              {
                $graphLookup: {
                  from: "test",
                  startWith: "$x",
                  connectFromField: "a",
                  connectToField: "b",
                  // missing "as"
                } as { from: string; startWith: unknown; connectFromField: string; connectToField: string; as: string },
              },
            ])
            .toArray(),
        /\$graphLookup requires 'as'/
      );
    });
  });

  describe("$densify stage", () => {
    it("should fill gaps in numeric sequence", async () => {
      const coll = client.db(dbName).collection("densify_numeric");
      await coll.insertMany([
        { x: 1, label: "a" },
        { x: 3, label: "b" },
        { x: 5, label: "c" },
      ]);

      const results = await coll
        .aggregate([{ $densify: { field: "x", range: { step: 1 } } }])
        .toArray();

      // Should have docs for x=1,2,3,4,5
      assert.strictEqual(results.length, 5);
      const xValues = results.map((d) => d.x as number).sort((a, b) => a - b);
      assert.deepStrictEqual(xValues, [1, 2, 3, 4, 5]);
    });

    it("should preserve original documents in numeric densify", async () => {
      const coll = client.db(dbName).collection("densify_preserve");
      await coll.insertMany([
        { x: 0, data: "first" },
        { x: 2, data: "second" },
      ]);

      const results = await coll
        .aggregate([{ $densify: { field: "x", range: { step: 1 } } }])
        .toArray();

      // Should have x=0,1,2
      assert.strictEqual(results.length, 3);
      // Original docs should have their data fields
      const first = results.find((d) => d.x === 0);
      const second = results.find((d) => d.x === 2);
      assert.strictEqual(first?.data, "first");
      assert.strictEqual(second?.data, "second");
      // Generated doc should only have the densify field
      const generated = results.find((d) => d.x === 1);
      assert.strictEqual(generated?.data, undefined);
    });

    it("should fill gaps in date sequence", async () => {
      const coll = client.db(dbName).collection("densify_date");
      await coll.insertMany([
        { timestamp: new Date("2024-01-01"), value: 10 },
        { timestamp: new Date("2024-01-03"), value: 30 },
      ]);

      const results = await coll
        .aggregate([
          {
            $densify: {
              field: "timestamp",
              range: { step: 1, unit: "day" },
            },
          },
        ])
        .toArray();

      // Should have Jan 1, 2, 3
      assert.strictEqual(results.length, 3);
      const dates = results
        .map((d) => (d.timestamp as Date).toISOString().slice(0, 10))
        .sort();
      assert.deepStrictEqual(dates, ["2024-01-01", "2024-01-02", "2024-01-03"]);
    });

    it("should densify with partitions", async () => {
      const coll = client.db(dbName).collection("densify_partition");
      await coll.insertMany([
        { category: "A", x: 1 },
        { category: "A", x: 3 },
        { category: "B", x: 10 },
        { category: "B", x: 12 },
      ]);

      const results = await coll
        .aggregate([
          {
            $densify: {
              field: "x",
              range: { step: 1 },
              partitionByFields: ["category"],
            },
          },
        ])
        .toArray();

      // Category A: x=1,2,3 (3 docs)
      // Category B: x=10,11,12 (3 docs)
      assert.strictEqual(results.length, 6);

      const catA = results.filter((d) => d.category === "A");
      const catB = results.filter((d) => d.category === "B");
      assert.strictEqual(catA.length, 3);
      assert.strictEqual(catB.length, 3);

      const catAValues = catA.map((d) => d.x as number).sort((a, b) => a - b);
      const catBValues = catB.map((d) => d.x as number).sort((a, b) => a - b);
      assert.deepStrictEqual(catAValues, [1, 2, 3]);
      assert.deepStrictEqual(catBValues, [10, 11, 12]);
    });

    it("should densify with explicit bounds", async () => {
      const coll = client.db(dbName).collection("densify_bounds");
      await coll.insertMany([{ x: 5 }]);

      const results = await coll
        .aggregate([
          {
            $densify: {
              field: "x",
              range: { step: 1, bounds: [3, 7] },
            },
          },
        ])
        .toArray();

      // Should have x=3,4,5,6,7
      assert.strictEqual(results.length, 5);
      const xValues = results.map((d) => d.x as number).sort((a, b) => a - b);
      assert.deepStrictEqual(xValues, [3, 4, 5, 6, 7]);
    });

    it("should throw error for field starting with $", async () => {
      const coll = client.db(dbName).collection("densify_error_field");
      await coll.insertOne({ x: 1 });

      await assert.rejects(
        () =>
          coll
            .aggregate([
              { $densify: { field: "$invalid", range: { step: 1 } } },
            ])
            .toArray(),
        /Cannot densify field starting with '\$'/
      );
    });

    it("should throw error for non-positive step", async () => {
      const coll = client.db(dbName).collection("densify_error_step");
      await coll.insertOne({ x: 1 });

      await assert.rejects(
        () =>
          coll
            .aggregate([{ $densify: { field: "x", range: { step: 0 } } }])
            .toArray(),
        /Step must be positive/
      );
    });

    it("should throw error for unit with numeric field", async () => {
      const coll = client.db(dbName).collection("densify_error_unit");
      await coll.insertOne({ x: 1 });

      await assert.rejects(
        () =>
          coll
            .aggregate([
              { $densify: { field: "x", range: { step: 1, unit: "day" } } },
            ])
            .toArray(),
        /Cannot specify unit for numeric field/
      );
    });

    it("should throw error for date field without unit", async () => {
      const coll = client.db(dbName).collection("densify_error_no_unit");
      await coll.insertOne({ ts: new Date() });

      await assert.rejects(
        () =>
          coll
            .aggregate([{ $densify: { field: "ts", range: { step: 1 } } }])
            .toArray(),
        /Unit required for date field/
      );
    });

    it("should handle empty collection", async () => {
      const coll = client.db(dbName).collection("densify_empty");
      // Empty collection

      const results = await coll
        .aggregate([{ $densify: { field: "x", range: { step: 1 } } }])
        .toArray();

      assert.strictEqual(results.length, 0);
    });
  });

  describe("$fill stage", () => {
    it("should fill nulls with static value", async () => {
      const coll = client.db(dbName).collection("fill_value");
      await coll.insertMany([
        { x: 1, y: 10 },
        { x: 2, y: null },
        { x: 3, y: 30 },
      ]);

      const results = await coll
        .aggregate([
          { $fill: { output: { y: { value: 0 } } } },
          { $sort: { x: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].y, 10);
      assert.strictEqual(results[1].y, 0);
      assert.strictEqual(results[2].y, 30);
    });

    it("should fill nulls with expression value", async () => {
      const coll = client.db(dbName).collection("fill_expr");
      await coll.insertMany([
        { x: 1, y: null },
        { x: 2, y: 20 },
      ]);

      const results = await coll
        .aggregate([
          { $fill: { output: { y: { value: { $multiply: ["$x", 10] } } } } },
          { $sort: { x: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0].y, 10); // 1 * 10
      assert.strictEqual(results[1].y, 20); // unchanged
    });

    it("should fill with locf method", async () => {
      const coll = client.db(dbName).collection("fill_locf");
      await coll.insertMany([
        { x: 1, y: 100 },
        { x: 2, y: null },
        { x: 3, y: null },
        { x: 4, y: 400 },
      ]);

      const results = await coll
        .aggregate([
          { $fill: { sortBy: { x: 1 }, output: { y: { method: "locf" } } } },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      assert.strictEqual(results[0].y, 100);
      assert.strictEqual(results[1].y, 100); // carried from x=1
      assert.strictEqual(results[2].y, 100); // carried from x=1
      assert.strictEqual(results[3].y, 400);
    });

    it("should leave null at start for locf", async () => {
      const coll = client.db(dbName).collection("fill_locf_start");
      await coll.insertMany([
        { x: 1, y: null },
        { x: 2, y: 20 },
        { x: 3, y: null },
      ]);

      const results = await coll
        .aggregate([
          { $fill: { sortBy: { x: 1 }, output: { y: { method: "locf" } } } },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].y, null); // no prior value
      assert.strictEqual(results[1].y, 20);
      assert.strictEqual(results[2].y, 20); // carried from x=2
    });

    it("should fill with linear interpolation", async () => {
      const coll = client.db(dbName).collection("fill_linear");
      await coll.insertMany([
        { x: 1, y: 0 },
        { x: 2, y: null },
        { x: 3, y: null },
        { x: 4, y: 30 },
      ]);

      const results = await coll
        .aggregate([
          { $fill: { sortBy: { x: 1 }, output: { y: { method: "linear" } } } },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      assert.strictEqual(results[0].y, 0);
      assert.strictEqual(results[1].y, 10); // interpolated
      assert.strictEqual(results[2].y, 20); // interpolated
      assert.strictEqual(results[3].y, 30);
    });

    it("should fill with partitions", async () => {
      const coll = client.db(dbName).collection("fill_partition");
      await coll.insertMany([
        { category: "A", x: 1, y: 100 },
        { category: "A", x: 2, y: null },
        { category: "B", x: 1, y: 200 },
        { category: "B", x: 2, y: null },
      ]);

      const results = await coll
        .aggregate([
          {
            $fill: {
              sortBy: { x: 1 },
              partitionByFields: ["category"],
              output: { y: { method: "locf" } },
            },
          },
          { $sort: { category: 1, x: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      // Category A
      assert.strictEqual(results[0].y, 100);
      assert.strictEqual(results[1].y, 100); // carried from A, x=1
      // Category B
      assert.strictEqual(results[2].y, 200);
      assert.strictEqual(results[3].y, 200); // carried from B, x=1
    });

    it("should fill multiple fields", async () => {
      const coll = client.db(dbName).collection("fill_multi");
      await coll.insertMany([
        { x: 1, a: 10, b: 100 },
        { x: 2, a: null, b: null },
        { x: 3, a: 30, b: 300 },
      ]);

      const results = await coll
        .aggregate([
          {
            $fill: {
              sortBy: { x: 1 },
              output: {
                a: { method: "locf" },
                b: { value: 0 },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].a, 10);
      assert.strictEqual(results[0].b, 100);
      assert.strictEqual(results[1].a, 10); // locf
      assert.strictEqual(results[1].b, 0); // value
      assert.strictEqual(results[2].a, 30);
      assert.strictEqual(results[2].b, 300);
    });

    it("should throw error when sortBy missing for locf", async () => {
      const coll = client.db(dbName).collection("fill_error_sort");
      await coll.insertOne({ x: 1, y: null });

      await assert.rejects(
        () =>
          coll
            .aggregate([{ $fill: { output: { y: { method: "locf" } } } }])
            .toArray(),
        /sortBy required for locf\/linear/
      );
    });

    it("should throw error when sortBy missing for linear", async () => {
      const coll = client.db(dbName).collection("fill_error_sort2");
      await coll.insertOne({ x: 1, y: null });

      await assert.rejects(
        () =>
          coll
            .aggregate([{ $fill: { output: { y: { method: "linear" } } } }])
            .toArray(),
        /sortBy required for locf\/linear/
      );
    });

    it("should throw error for both value and method", async () => {
      const coll = client.db(dbName).collection("fill_error_both");
      await coll.insertOne({ x: 1, y: null });

      await assert.rejects(
        () =>
          coll
            .aggregate([
              {
                $fill: {
                  output: { y: { value: 0, method: "locf" } },
                  sortBy: { x: 1 },
                },
              },
            ])
            .toArray(),
        /Cannot specify both 'value' and 'method'/
      );
    });

    it("should throw error for string partitionBy", async () => {
      const coll = client.db(dbName).collection("fill_error_partition");
      await coll.insertOne({ x: 1, y: null });

      await assert.rejects(
        () =>
          coll
            .aggregate([
              {
                $fill: {
                  partitionBy: "category" as unknown as object,
                  output: { y: { value: 0 } },
                },
              },
            ])
            .toArray(),
        /partitionBy must be an object/
      );
    });

    it("should handle empty collection", async () => {
      const coll = client.db(dbName).collection("fill_empty");

      const results = await coll
        .aggregate([{ $fill: { output: { y: { value: 0 } } } }])
        .toArray();

      assert.strictEqual(results.length, 0);
    });
  });

  describe("$setWindowFields stage", () => {
    it("should compute $documentNumber", async () => {
      const coll = client.db(dbName).collection("swf_docnum");
      await coll.insertMany([
        { x: 3 },
        { x: 1 },
        { x: 2 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                docNum: { $documentNumber: {} },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      const nums = results.map((d) => d.docNum as number);
      assert.deepStrictEqual(nums, [1, 2, 3]);
    });

    it("should compute $sum over entire partition", async () => {
      const coll = client.db(dbName).collection("swf_sum");
      await coll.insertMany([
        { x: 1, value: 10 },
        { x: 2, value: 20 },
        { x: 3, value: 30 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                total: { $sum: "$value" },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      // Each doc gets sum of entire partition
      for (const doc of results) {
        assert.strictEqual(doc.total, 60);
      }
    });

    it("should compute $sum with document window", async () => {
      const coll = client.db(dbName).collection("swf_sum_window");
      await coll.insertMany([
        { x: 1, value: 10 },
        { x: 2, value: 20 },
        { x: 3, value: 30 },
        { x: 4, value: 40 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                runningSum: {
                  $sum: "$value",
                  window: { documents: ["unbounded", "current"] },
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      assert.strictEqual(results[0].runningSum, 10);
      assert.strictEqual(results[1].runningSum, 30);
      assert.strictEqual(results[2].runningSum, 60);
      assert.strictEqual(results[3].runningSum, 100);
    });

    it("should compute $avg with sliding window", async () => {
      const coll = client.db(dbName).collection("swf_avg_window");
      await coll.insertMany([
        { x: 1, value: 10 },
        { x: 2, value: 20 },
        { x: 3, value: 30 },
        { x: 4, value: 40 },
        { x: 5, value: 50 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                movingAvg: {
                  $avg: "$value",
                  window: { documents: [-1, 1] },
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 5);
      assert.strictEqual(results[0].movingAvg, 15); // (10+20)/2
      assert.strictEqual(results[1].movingAvg, 20); // (10+20+30)/3
      assert.strictEqual(results[2].movingAvg, 30); // (20+30+40)/3
      assert.strictEqual(results[3].movingAvg, 40); // (30+40+50)/3
      assert.strictEqual(results[4].movingAvg, 45); // (40+50)/2
    });

    it("should partition documents", async () => {
      const coll = client.db(dbName).collection("swf_partition");
      await coll.insertMany([
        { category: "A", x: 1, value: 10 },
        { category: "A", x: 2, value: 20 },
        { category: "B", x: 1, value: 100 },
        { category: "B", x: 2, value: 200 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              partitionBy: { cat: "$category" },
              sortBy: { x: 1 },
              output: {
                total: { $sum: "$value" },
              },
            },
          },
          { $sort: { category: 1, x: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      // Category A total: 30
      assert.strictEqual(results[0].total, 30);
      assert.strictEqual(results[1].total, 30);
      // Category B total: 300
      assert.strictEqual(results[2].total, 300);
      assert.strictEqual(results[3].total, 300);
    });

    it("should compute $first and $last", async () => {
      const coll = client.db(dbName).collection("swf_first_last");
      await coll.insertMany([
        { x: 1, value: "a" },
        { x: 2, value: "b" },
        { x: 3, value: "c" },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                firstVal: { $first: "$value" },
                lastVal: { $last: "$value" },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      for (const doc of results) {
        assert.strictEqual(doc.firstVal, "a");
        assert.strictEqual(doc.lastVal, "c");
      }
    });

    it("should compute $shift", async () => {
      const coll = client.db(dbName).collection("swf_shift");
      await coll.insertMany([
        { x: 1, value: 10 },
        { x: 2, value: 20 },
        { x: 3, value: 30 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                prevValue: {
                  $shift: { output: "$value", by: -1, default: 0 },
                },
                nextValue: {
                  $shift: { output: "$value", by: 1, default: 0 },
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].prevValue, 0); // no previous
      assert.strictEqual(results[0].nextValue, 20);
      assert.strictEqual(results[1].prevValue, 10);
      assert.strictEqual(results[1].nextValue, 30);
      assert.strictEqual(results[2].prevValue, 20);
      assert.strictEqual(results[2].nextValue, 0); // no next
    });

    it("should compute $locf in window", async () => {
      const coll = client.db(dbName).collection("swf_locf");
      await coll.insertMany([
        { x: 1, value: 100 },
        { x: 2, value: null },
        { x: 3, value: null },
        { x: 4, value: 400 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                filled: { $locf: "$value" },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      assert.strictEqual(results[0].filled, 100);
      assert.strictEqual(results[1].filled, 100); // carried
      assert.strictEqual(results[2].filled, 100); // carried
      assert.strictEqual(results[3].filled, 400);
    });

    it("should compute $linearFill in window", async () => {
      const coll = client.db(dbName).collection("swf_linear");
      await coll.insertMany([
        { x: 1, value: 0 },
        { x: 2, value: null },
        { x: 3, value: null },
        { x: 4, value: 30 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                filled: { $linearFill: "$value" },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      assert.strictEqual(results[0].filled, 0);
      assert.strictEqual(results[1].filled, 10); // interpolated
      assert.strictEqual(results[2].filled, 20); // interpolated
      assert.strictEqual(results[3].filled, 30);
    });

    it("should compute $min and $max", async () => {
      const coll = client.db(dbName).collection("swf_minmax");
      await coll.insertMany([
        { x: 1, value: 30 },
        { x: 2, value: 10 },
        { x: 3, value: 50 },
        { x: 4, value: 20 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                minVal: { $min: "$value" },
                maxVal: { $max: "$value" },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      for (const doc of results) {
        assert.strictEqual(doc.minVal, 10);
        assert.strictEqual(doc.maxVal, 50);
      }
    });

    it("should compute $count", async () => {
      const coll = client.db(dbName).collection("swf_count");
      await coll.insertMany([
        { x: 1 },
        { x: 2 },
        { x: 3 },
      ]);

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: {
                totalCount: { $count: {} },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      for (const doc of results) {
        assert.strictEqual(doc.totalCount, 3);
      }
    });

    it("should handle empty collection", async () => {
      const coll = client.db(dbName).collection("swf_empty");

      const results = await coll
        .aggregate([
          {
            $setWindowFields: {
              sortBy: { x: 1 },
              output: { docNum: { $documentNumber: {} } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 0);
    });
  });
});
