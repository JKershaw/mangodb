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
});
