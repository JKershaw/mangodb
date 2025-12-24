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

describe(`New Query Operators (${getTestModeName()})`, () => {
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
});
