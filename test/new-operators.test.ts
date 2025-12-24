/**
 * Tests for newly implemented query operators.
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

describe(`New Update Operators (${getTestModeName()})`, () => {
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
        // Position -1 means insert before the last element
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

        // Add values, sort, then slice to top 4
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
        // After adding: [5, 3, 7, 1, 9, 2]
        // After sort -1: [9, 7, 5, 3, 2, 1]
        // After slice 4: [9, 7, 5, 3]
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
        // After position 0: ["x", "a", "b", "c"]
        // After slice 3: ["x", "a", "b"]
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
      // 13 = 1101, AND with 10 = 1010, result = 1000 = 8
      await collection.insertOne({ flags: 13 });

      await collection.updateOne({}, { $bit: { flags: { and: 10 } } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.flags, 8);
    });

    it("should apply bitwise OR", async () => {
      const collection = client.db(dbName).collection("bit_or");
      // 5 = 0101, OR with 2 = 0010, result = 0111 = 7
      await collection.insertOne({ flags: 5 });

      await collection.updateOne({}, { $bit: { flags: { or: 2 } } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.flags, 7);
    });

    it("should apply bitwise XOR", async () => {
      const collection = client.db(dbName).collection("bit_xor");
      // 5 = 0101, XOR with 3 = 0011, result = 0110 = 6
      await collection.insertOne({ flags: 5 });

      await collection.updateOne({}, { $bit: { flags: { xor: 3 } } });

      const doc = await collection.findOne({});
      assert.strictEqual(doc?.flags, 6);
    });

    it("should apply multiple bitwise operations", async () => {
      const collection = client.db(dbName).collection("bit_multiple");
      await collection.insertOne({ flags: 15 }); // 1111

      // AND with 12 (1100) = 1100 (12), then OR with 3 (0011) = 1111 (15)
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

describe(`New Aggregation Stages (${getTestModeName()})`, () => {
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

  describe("$sortByCount stage", () => {
    it("should group and count by field value", async () => {
      const collection = client.db(dbName).collection("sortbycount_basic");
      await collection.insertMany([
        { category: "A" },
        { category: "B" },
        { category: "A" },
        { category: "C" },
        { category: "A" },
        { category: "B" },
      ]);

      const docs = await collection
        .aggregate([{ $sortByCount: "$category" }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0]._id, "A");
      assert.strictEqual(docs[0].count, 3);
      assert.strictEqual(docs[1]._id, "B");
      assert.strictEqual(docs[1].count, 2);
      assert.strictEqual(docs[2]._id, "C");
      assert.strictEqual(docs[2].count, 1);
    });

    it("should handle null values", async () => {
      const collection = client.db(dbName).collection("sortbycount_null");
      await collection.insertMany([
        { status: "active" },
        { status: null },
        { status: "active" },
        { status: null },
      ]);

      const docs = await collection
        .aggregate([{ $sortByCount: "$status" }])
        .toArray();

      assert.strictEqual(docs.length, 2);
      // null values are grouped together
      const activeDoc = docs.find((d) => d._id === "active");
      const nullDoc = docs.find((d) => d._id === null);
      assert.strictEqual(activeDoc?.count, 2);
      assert.strictEqual(nullDoc?.count, 2);
    });
  });

  describe("$sample stage", () => {
    it("should return specified number of documents", async () => {
      const collection = client.db(dbName).collection("sample_basic");
      await collection.insertMany(
        Array.from({ length: 10 }, (_, i) => ({ index: i }))
      );

      const docs = await collection
        .aggregate([{ $sample: { size: 3 } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
    });

    it("should return all documents if size exceeds collection", async () => {
      const collection = client.db(dbName).collection("sample_all");
      await collection.insertMany([{ a: 1 }, { a: 2 }, { a: 3 }]);

      const docs = await collection
        .aggregate([{ $sample: { size: 10 } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
    });

    it("should return empty array for size 0", async () => {
      const collection = client.db(dbName).collection("sample_zero");
      await collection.insertMany([{ a: 1 }, { a: 2 }]);

      const docs = await collection
        .aggregate([{ $sample: { size: 0 } }])
        .toArray();

      assert.strictEqual(docs.length, 0);
    });
  });

  describe("$facet stage", () => {
    it("should run multiple pipelines", async () => {
      const collection = client.db(dbName).collection("facet_basic");
      await collection.insertMany([
        { type: "A", value: 10 },
        { type: "B", value: 20 },
        { type: "A", value: 30 },
        { type: "B", value: 40 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $facet: {
              byType: [{ $group: { _id: "$type", total: { $sum: "$value" } } }],
              count: [{ $count: "total" }],
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.ok(Array.isArray(docs[0].byType));
      assert.ok(Array.isArray(docs[0].count));
      assert.strictEqual(docs[0].byType.length, 2);
      assert.strictEqual(docs[0].count[0].total, 4);
    });

    it("should handle empty pipelines", async () => {
      const collection = client.db(dbName).collection("facet_empty");
      await collection.insertMany([{ a: 1 }, { a: 2 }]);

      const docs = await collection
        .aggregate([
          {
            $facet: {
              all: [],
              limited: [{ $limit: 1 }],
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual((docs[0].all as unknown[]).length, 2);
      assert.strictEqual((docs[0].limited as unknown[]).length, 1);
    });
  });

  describe("$bucket stage", () => {
    it("should group into buckets", async () => {
      const collection = client.db(dbName).collection("bucket_basic");
      await collection.insertMany([
        { score: 15 },
        { score: 25 },
        { score: 35 },
        { score: 45 },
        { score: 55 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: "$score",
              boundaries: [0, 20, 40, 60],
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0]._id, 0);
      assert.strictEqual(docs[0].count, 1); // 15
      assert.strictEqual(docs[1]._id, 20);
      assert.strictEqual(docs[1].count, 2); // 25, 35
      assert.strictEqual(docs[2]._id, 40);
      assert.strictEqual(docs[2].count, 2); // 45, 55
    });

    it("should use default bucket for out-of-range values", async () => {
      const collection = client.db(dbName).collection("bucket_default");
      await collection.insertMany([
        { score: 5 },
        { score: 15 },
        { score: 100 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: "$score",
              boundaries: [10, 20],
              default: "other",
            },
          },
        ])
        .toArray();

      const tenBucket = docs.find((d) => d._id === 10);
      const otherBucket = docs.find((d) => d._id === "other");

      assert.strictEqual(tenBucket?.count, 1); // 15
      assert.strictEqual(otherBucket?.count, 2); // 5, 100
    });

    it("should support custom output", async () => {
      const collection = client.db(dbName).collection("bucket_output");
      await collection.insertMany([
        { score: 15, value: 100 },
        { score: 25, value: 200 },
        { score: 35, value: 300 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: "$score",
              boundaries: [0, 20, 40],
              output: {
                count: { $sum: 1 },
                total: { $sum: "$value" },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0]._id, 0);
      assert.strictEqual(docs[0].count, 1);
      assert.strictEqual(docs[0].total, 100);
      assert.strictEqual(docs[1]._id, 20);
      assert.strictEqual(docs[1].count, 2);
      assert.strictEqual(docs[1].total, 500);
    });
  });

  describe("$bucketAuto stage", () => {
    it("should automatically create buckets", async () => {
      const collection = client.db(dbName).collection("bucketauto_basic");
      await collection.insertMany([
        { value: 10 },
        { value: 20 },
        { value: 30 },
        { value: 40 },
        { value: 50 },
        { value: 60 },
      ]);

      const docs = await collection
        .aggregate([
          {
            $bucketAuto: {
              groupBy: "$value",
              buckets: 3,
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 3);
      for (const doc of docs) {
        const id = doc._id as { min: unknown; max: unknown };
        assert.ok(id.min !== undefined);
        assert.ok(id.max !== undefined);
        assert.ok((doc.count as number) >= 1);
      }
    });

    it("should handle fewer docs than buckets", async () => {
      const collection = client.db(dbName).collection("bucketauto_few");
      await collection.insertMany([{ value: 10 }, { value: 20 }]);

      const docs = await collection
        .aggregate([
          {
            $bucketAuto: {
              groupBy: "$value",
              buckets: 5,
            },
          },
        ])
        .toArray();

      // Should create at most 2 buckets since we only have 2 distinct values
      assert.ok(docs.length <= 2);
    });
  });

  describe("$unionWith stage", () => {
    it("should combine documents from two collections", async () => {
      const collection1 = client.db(dbName).collection("union_source");
      const collection2 = client.db(dbName).collection("union_other");

      await collection1.insertMany([{ source: "A", val: 1 }]);
      await collection2.insertMany([{ source: "B", val: 2 }]);

      const docs = await collection1
        .aggregate([{ $unionWith: "union_other" }])
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.source === "A"));
      assert.ok(docs.some((d) => d.source === "B"));
    });

    it("should apply pipeline to unioned collection", async () => {
      const collection1 = client.db(dbName).collection("union_pipe_src");
      const collection2 = client.db(dbName).collection("union_pipe_other");

      await collection1.insertMany([{ val: 1 }]);
      await collection2.insertMany([{ val: 10 }, { val: 20 }, { val: 30 }]);

      const docs = await collection1
        .aggregate([
          {
            $unionWith: {
              coll: "union_pipe_other",
              pipeline: [{ $match: { val: { $gt: 15 } } }],
            },
          },
        ])
        .toArray();

      // 1 from source + 2 from other (20, 30)
      assert.strictEqual(docs.length, 3);
    });
  });
});

describe(`New Operators (${getTestModeName()})`, () => {
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

  describe("$comment operator", () => {
    it("should ignore $comment and return matching documents", async () => {
      const collection = client.db(dbName).collection("comment_basic");
      await collection.insertMany([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 25 },
        { name: "Charlie", age: 35 },
      ]);

      // $comment should not affect query results
      const docs = await collection
        .find({ age: { $gte: 30 }, $comment: "Find users 30 or older" } as any)
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.name === "Alice"));
      assert.ok(docs.some((d) => d.name === "Charlie"));
    });

    it("should work with $comment as the only query parameter alongside field query", async () => {
      const collection = client.db(dbName).collection("comment_with_field");
      await collection.insertMany([
        { status: "active" },
        { status: "inactive" },
      ]);

      const docs = await collection
        .find({ status: "active", $comment: "Get active items" } as any)
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].status, "active");
    });

    it("should work with complex queries and $comment", async () => {
      const collection = client.db(dbName).collection("comment_complex");
      await collection.insertMany([
        { x: 2, y: 10 },
        { x: 3, y: 15 },
        { x: 4, y: 20 },
        { x: 5, y: 25 },
      ]);

      const docs = await collection
        .find({
          $and: [{ x: { $gte: 3 } }, { y: { $lte: 20 } }],
          $comment: "Find records where x >= 3 and y <= 20",
        } as any)
        .toArray();

      assert.strictEqual(docs.length, 2);
    });
  });

  describe("Bitwise query operators", () => {
    describe("$bitsAllSet", () => {
      it("should match when all specified bit positions are set", async () => {
        const collection = client.db(dbName).collection("bits_allset_pos");
        // 54 in binary is 110110 (bits 1, 2, 4, 5 are set)
        // 50 in binary is 110010 (bits 1, 4, 5 are set - bit 2 is NOT set)
        // 38 in binary is 100110 (bits 1, 2, 5 are set - bit 4 is NOT set)
        await collection.insertMany([
          { value: 54 },  // 110110
          { value: 50 },  // 110010
          { value: 38 },  // 100110
        ]);

        // Test bits 1 and 5 (both set in all three numbers)
        const docs = await collection
          .find({ value: { $bitsAllSet: [1, 5] } })
          .toArray();

        assert.strictEqual(docs.length, 3);
      });

      it("should not match when any specified bit is clear", async () => {
        const collection = client.db(dbName).collection("bits_allset_nomatch");
        await collection.insertMany([
          { value: 54 },  // 110110 - bit 0 is clear
        ]);

        // Bit 0 is not set in 54
        const docs = await collection
          .find({ value: { $bitsAllSet: [0, 1] } })
          .toArray();

        assert.strictEqual(docs.length, 0);
      });

      it("should work with numeric bitmask", async () => {
        const collection = client.db(dbName).collection("bits_allset_mask");
        await collection.insertMany([
          { value: 54 },  // 110110
          { value: 50 },  // 110010
        ]);

        // Bitmask 50 = 110010 (bits 1, 4, 5)
        const docs = await collection
          .find({ value: { $bitsAllSet: 50 } })
          .toArray();

        // Both 54 and 50 have bits 1, 4, 5 set
        assert.strictEqual(docs.length, 2);
      });

      it("should not match non-numeric field values", async () => {
        const collection = client.db(dbName).collection("bits_allset_type");
        await collection.insertMany([
          { value: "54" },    // string
          { value: true },    // boolean
          { value: null },    // null
          { value: [54] },    // array
        ]);

        const docs = await collection
          .find({ value: { $bitsAllSet: [1] } })
          .toArray();

        assert.strictEqual(docs.length, 0);
      });
    });

    describe("$bitsAllClear", () => {
      it("should match when all specified bit positions are clear", async () => {
        const collection = client.db(dbName).collection("bits_allclear_pos");
        // 54 in binary is 110110 (bits 0 and 3 are clear)
        await collection.insertMany([
          { value: 54 },  // 110110
          { value: 50 },  // 110010 - bit 0 clear, bit 2 set
        ]);

        // Test bit 0 (clear in both)
        const docs = await collection
          .find({ value: { $bitsAllClear: [0] } })
          .toArray();

        assert.strictEqual(docs.length, 2);
      });

      it("should not match when any specified bit is set", async () => {
        const collection = client.db(dbName).collection("bits_allclear_nomatch");
        await collection.insertMany([
          { value: 54 },  // 110110 - bit 1 is set
        ]);

        // Bit 1 is set in 54
        const docs = await collection
          .find({ value: { $bitsAllClear: [0, 1] } })
          .toArray();

        assert.strictEqual(docs.length, 0);
      });

      it("should work with numeric bitmask", async () => {
        const collection = client.db(dbName).collection("bits_allclear_mask");
        await collection.insertMany([
          { value: 54 },  // 110110
          { value: 50 },  // 110010
        ]);

        // Bitmask 9 = 001001 (bits 0 and 3)
        // Both 54 and 50 have bits 0 and 3 clear
        const docs = await collection
          .find({ value: { $bitsAllClear: 9 } })
          .toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe("$bitsAnySet", () => {
      it("should match when any specified bit position is set", async () => {
        const collection = client.db(dbName).collection("bits_anyset_pos");
        await collection.insertMany([
          { value: 54 },  // 110110
          { value: 1 },   // 000001
          { value: 8 },   // 001000
        ]);

        // Test bits 0 and 3 - value 1 has bit 0, value 8 has bit 3
        const docs = await collection
          .find({ value: { $bitsAnySet: [0, 3] } })
          .toArray();

        assert.strictEqual(docs.length, 2); // 1 and 8 match
      });

      it("should not match when all specified bits are clear", async () => {
        const collection = client.db(dbName).collection("bits_anyset_nomatch");
        await collection.insertMany([
          { value: 54 },  // 110110 - bits 0 and 3 are clear
        ]);

        const docs = await collection
          .find({ value: { $bitsAnySet: [0, 3] } })
          .toArray();

        assert.strictEqual(docs.length, 0);
      });

      it("should work with numeric bitmask", async () => {
        const collection = client.db(dbName).collection("bits_anyset_mask");
        await collection.insertMany([
          { value: 54 },  // 110110
          { value: 1 },   // 000001
        ]);

        // Bitmask 3 = 000011 (bits 0 and 1)
        // 54 has bit 1 set, 1 has bit 0 set
        const docs = await collection
          .find({ value: { $bitsAnySet: 3 } })
          .toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe("$bitsAnyClear", () => {
      it("should match when any specified bit position is clear", async () => {
        const collection = client.db(dbName).collection("bits_anyclear_pos");
        await collection.insertMany([
          { value: 54 },  // 110110 - bit 0 is clear
          { value: 55 },  // 110111 - bit 3 is clear
          { value: 63 },  // 111111 - no bits 0-5 are clear
        ]);

        // Test bits 0 and 3
        const docs = await collection
          .find({ value: { $bitsAnyClear: [0, 3] } })
          .toArray();

        assert.strictEqual(docs.length, 2); // 54 and 55 match
      });

      it("should not match when all specified bits are set", async () => {
        const collection = client.db(dbName).collection("bits_anyclear_nomatch");
        await collection.insertMany([
          { value: 54 },  // 110110 - bits 1, 2, 4, 5 are set
        ]);

        const docs = await collection
          .find({ value: { $bitsAnyClear: [1, 2] } })
          .toArray();

        assert.strictEqual(docs.length, 0);
      });

      it("should work with numeric bitmask", async () => {
        const collection = client.db(dbName).collection("bits_anyclear_mask");
        await collection.insertMany([
          { value: 54 },  // 110110
          { value: 63 },  // 111111
        ]);

        // Bitmask 9 = 001001 (bits 0 and 3)
        // 54 has both bits 0 and 3 clear
        const docs = await collection
          .find({ value: { $bitsAnyClear: 9 } })
          .toArray();

        assert.strictEqual(docs.length, 1); // only 54 matches
      });
    });

    describe("Bitwise operator edge cases", () => {
      it("should handle negative numbers with sign extension", async () => {
        const collection = client.db(dbName).collection("bits_negative");
        // -1 in two's complement has all bits set
        // -5 = ...11111011 (bit 2 is clear)
        // 5 = 00000101 (bit 0 and 2 are set)
        await collection.insertMany([
          { value: -1 },
          { value: -5 },  // ...11111011
          { value: 5 },   // 00000101
        ]);

        // Bit 0 is set in all three: -1 (all bits), -5 (...11111011), 5 (00000101)
        const docs = await collection
          .find({ value: { $bitsAllSet: [0] } })
          .toArray();

        assert.strictEqual(docs.length, 3);
      });

      it("should handle zero value", async () => {
        const collection = client.db(dbName).collection("bits_zero");
        await collection.insertMany([{ value: 0 }]);

        // No bits are set in 0
        const docsSet = await collection
          .find({ value: { $bitsAnySet: [0, 1, 2] } })
          .toArray();
        assert.strictEqual(docsSet.length, 0);

        // All bits are clear in 0
        const docsClear = await collection
          .find({ value: { $bitsAllClear: [0, 1, 2] } })
          .toArray();
        assert.strictEqual(docsClear.length, 1);
      });

      it("should handle empty position array", async () => {
        const collection = client.db(dbName).collection("bits_empty");
        await collection.insertMany([{ value: 54 }]);

        // Empty array - vacuous truth for $bitsAllSet (all of nothing are set)
        const docs = await collection
          .find({ value: { $bitsAllSet: [] } })
          .toArray();

        // MongoDB behavior: empty array matches all numeric values
        assert.strictEqual(docs.length, 1);
      });

      it("should handle missing field", async () => {
        const collection = client.db(dbName).collection("bits_missing");
        await collection.insertMany([
          { other: "field" },
          { value: 54 },
        ]);

        const docs = await collection
          .find({ value: { $bitsAllSet: [1] } })
          .toArray();

        assert.strictEqual(docs.length, 1);
      });
    });
  });

  describe("Arithmetic expression operators", () => {
    it("$exp should calculate e^x", async () => {
      const collection = client.db(dbName).collection("arith_exp");
      await collection.insertMany([{ value: 0 }, { value: 1 }, { value: 2 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $exp: "$value" } } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.ok(Math.abs((docs[0].result as number) - 1) < 0.0001); // e^0 = 1
      assert.ok(Math.abs((docs[1].result as number) - Math.E) < 0.0001); // e^1 = e
      assert.ok(Math.abs((docs[2].result as number) - Math.E * Math.E) < 0.0001); // e^2
    });

    it("$ln should calculate natural logarithm", async () => {
      const collection = client.db(dbName).collection("arith_ln");
      await collection.insertMany([{ value: 1 }, { value: Math.E }, { value: 10 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $ln: "$value" } } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.ok(Math.abs((docs[0].result as number) - 0) < 0.0001); // ln(1) = 0
      assert.ok(Math.abs((docs[1].result as number) - 1) < 0.0001); // ln(e) = 1
    });

    it("$log should calculate logarithm with specified base", async () => {
      const collection = client.db(dbName).collection("arith_log");
      await collection.insertMany([{ value: 8 }, { value: 100 }]);

      const docs = await collection
        .aggregate([
          { $project: { log2: { $log: ["$value", 2] }, log10: { $log: ["$value", 10] } } },
        ])
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(Math.abs((docs[0].log2 as number) - 3) < 0.0001); // log2(8) = 3
      assert.ok(Math.abs((docs[1].log10 as number) - 2) < 0.0001); // log10(100) = 2
    });

    it("$log10 should calculate base-10 logarithm", async () => {
      const collection = client.db(dbName).collection("arith_log10");
      await collection.insertMany([{ value: 1 }, { value: 10 }, { value: 100 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $log10: "$value" } } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.ok(Math.abs((docs[0].result as number) - 0) < 0.0001);
      assert.ok(Math.abs((docs[1].result as number) - 1) < 0.0001);
      assert.ok(Math.abs((docs[2].result as number) - 2) < 0.0001);
    });

    it("$pow should raise to power", async () => {
      const collection = client.db(dbName).collection("arith_pow");
      await collection.insertMany([{ base: 2, exp: 3 }, { base: 10, exp: 2 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $pow: ["$base", "$exp"] } } }])
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.strictEqual(docs[0].result, 8); // 2^3 = 8
      assert.strictEqual(docs[1].result, 100); // 10^2 = 100
    });

    it("$sqrt should calculate square root", async () => {
      const collection = client.db(dbName).collection("arith_sqrt");
      await collection.insertMany([{ value: 0 }, { value: 4 }, { value: 9 }, { value: 2 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $sqrt: "$value" } } }])
        .toArray();

      assert.strictEqual(docs.length, 4);
      assert.strictEqual(docs[0].result, 0);
      assert.strictEqual(docs[1].result, 2);
      assert.strictEqual(docs[2].result, 3);
      assert.ok(Math.abs((docs[3].result as number) - Math.SQRT2) < 0.0001);
    });

    it("$trunc should truncate to integer", async () => {
      const collection = client.db(dbName).collection("arith_trunc");
      await collection.insertMany([
        { value: 3.7 },
        { value: -2.3 },
        { value: 5.9 },
      ]);

      const docs = await collection
        .aggregate([{ $project: { result: { $trunc: "$value" } } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0].result, 3);
      assert.strictEqual(docs[1].result, -2);
      assert.strictEqual(docs[2].result, 5);
    });

    it("$trunc should support decimal places", async () => {
      const collection = client.db(dbName).collection("arith_trunc_places");
      await collection.insertMany([{ value: 3.14159 }]);

      const docs = await collection
        .aggregate([
          { $project: { trunc2: { $trunc: ["$value", 2] }, trunc0: { $trunc: "$value" } } },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].trunc2, 3.14);
      assert.strictEqual(docs[0].trunc0, 3);
    });

    it("should return null for null inputs", async () => {
      const collection = client.db(dbName).collection("arith_null");
      await collection.insertMany([{ value: null }]);

      const docs = await collection
        .aggregate([
          {
            $project: {
              exp: { $exp: "$value" },
              sqrt: { $sqrt: "$value" },
              pow: { $pow: ["$value", 2] },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].exp, null);
      assert.strictEqual(docs[0].sqrt, null);
      assert.strictEqual(docs[0].pow, null);
    });
  });

  describe("$rand expression operator", () => {
    it("should return a number between 0 and 1 in aggregation", async () => {
      const collection = client.db(dbName).collection("rand_agg");
      await collection.insertMany([{ name: "test" }]);

      const docs = await collection
        .aggregate([{ $project: { randomValue: { $rand: {} } } }])
        .toArray();

      assert.strictEqual(docs.length, 1);
      const randomValue = docs[0].randomValue as number;
      assert.strictEqual(typeof randomValue, "number");
      assert.ok(randomValue >= 0, "random value should be >= 0");
      assert.ok(randomValue < 1, "random value should be < 1");
    });

    it("should generate different values for each document", async () => {
      const collection = client.db(dbName).collection("rand_multi");
      await collection.insertMany([
        { name: "doc1" },
        { name: "doc2" },
        { name: "doc3" },
        { name: "doc4" },
        { name: "doc5" },
      ]);

      const docs = await collection
        .aggregate([{ $project: { name: 1, randomValue: { $rand: {} } } }])
        .toArray();

      assert.strictEqual(docs.length, 5);

      // Check all values are numbers in range
      for (const doc of docs) {
        const randomValue = doc.randomValue as number;
        assert.strictEqual(typeof randomValue, "number");
        assert.ok(randomValue >= 0);
        assert.ok(randomValue < 1);
      }

      // Collect unique values - with 5 docs, we should get at least 2 unique values
      // (probability of all 5 being the same is essentially 0)
      const uniqueValues = new Set(docs.map((d) => d.randomValue as number));
      assert.ok(uniqueValues.size >= 2, "should have multiple unique random values");
    });

    it("should work with $expr in find queries for random sampling", async () => {
      const collection = client.db(dbName).collection("rand_expr");
      // Insert many documents so we can test random sampling
      const manyDocs = Array.from({ length: 100 }, (_, i) => ({ index: i }));
      await collection.insertMany(manyDocs);

      // Use $expr with $rand to randomly sample ~50% of documents
      // Since it's random, we can't assert exact count, but should be in a reasonable range
      const docs = await collection
        .find({ $expr: { $lt: [{ $rand: {} }, 0.5] } })
        .toArray();

      // With 100 documents and 50% sampling, we should get roughly 30-70 documents
      // (3 standard deviations from 50)
      assert.ok(docs.length >= 20, `Expected at least 20 documents, got ${docs.length}`);
      assert.ok(docs.length <= 80, `Expected at most 80 documents, got ${docs.length}`);
    });

    it("should work with scaling to generate larger random numbers", async () => {
      const collection = client.db(dbName).collection("rand_scale");
      await collection.insertMany([{ name: "test" }]);

      const docs = await collection
        .aggregate([
          {
            $project: {
              randomInt: { $floor: { $multiply: [{ $rand: {} }, 100] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      const randomInt = docs[0].randomInt as number;
      assert.strictEqual(typeof randomInt, "number");
      assert.ok(Number.isInteger(randomInt), "should be an integer");
      assert.ok(randomInt >= 0, "should be >= 0");
      assert.ok(randomInt < 100, "should be < 100");
    });
  });
});
