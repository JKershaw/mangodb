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
