/**
 * Phase 16: Extended Expression Operators Tests
 *
 * These tests verify aggregation expression operators.
 * Tests run against MangoDB (MongoDB not available in this environment).
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
  type TestCollection,
  type Document,
} from "./test-harness.ts";

describe(`Expression Operators (${getTestModeName()})`, () => {
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

  // ==================== Part 1: Arithmetic Operators ====================

  describe("Arithmetic Operators", () => {
    describe("$abs", () => {
      it("should return absolute value of positive number", async () => {
        const collection = client.db(dbName).collection("abs_positive");
        await collection.insertOne({ value: 5 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it("should return absolute value of negative number", async () => {
        const collection = client.db(dbName).collection("abs_negative");
        await collection.insertOne({ value: -5 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it("should return 0 for 0", async () => {
        const collection = client.db(dbName).collection("abs_zero");
        await collection.insertOne({ value: 0 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 0);
      });

      it("should return null for null input", async () => {
        const collection = client.db(dbName).collection("abs_null");
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return null for missing field", async () => {
        const collection = client.db(dbName).collection("abs_missing");
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should throw for non-numeric input", async () => {
        const collection = client.db(dbName).collection("abs_string");
        await collection.insertOne({ value: "hello" });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $abs: "$value" }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$abs only supports numeric types"));
            return true;
          }
        );
      });

      it("should handle floating point numbers", async () => {
        const collection = client.db(dbName).collection("abs_float");
        await collection.insertOne({ value: -3.14 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 3.14);
      });
    });

    describe("$ceil", () => {
      it("should round 2.3 up to 3", async () => {
        const collection = client.db(dbName).collection("ceil_up");
        await collection.insertOne({ value: 2.3 });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 3);
      });

      it("should return integer unchanged", async () => {
        const collection = client.db(dbName).collection("ceil_int");
        await collection.insertOne({ value: 5 });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it("should round -2.3 to -2", async () => {
        const collection = client.db(dbName).collection("ceil_neg");
        await collection.insertOne({ value: -2.3 });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, -2);
      });

      it("should return null for null input", async () => {
        const collection = client.db(dbName).collection("ceil_null");
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return null for missing field", async () => {
        const collection = client.db(dbName).collection("ceil_missing");
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should throw for non-numeric input", async () => {
        const collection = client.db(dbName).collection("ceil_string");
        await collection.insertOne({ value: "hello" });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $ceil: "$value" }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$ceil only supports numeric types"));
            return true;
          }
        );
      });
    });

    describe("$floor", () => {
      it("should round 2.7 down to 2", async () => {
        const collection = client.db(dbName).collection("floor_down");
        await collection.insertOne({ value: 2.7 });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 2);
      });

      it("should return integer unchanged", async () => {
        const collection = client.db(dbName).collection("floor_int");
        await collection.insertOne({ value: 5 });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it("should round -2.3 to -3", async () => {
        const collection = client.db(dbName).collection("floor_neg");
        await collection.insertOne({ value: -2.3 });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, -3);
      });

      it("should return null for null input", async () => {
        const collection = client.db(dbName).collection("floor_null");
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return null for missing field", async () => {
        const collection = client.db(dbName).collection("floor_missing");
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should throw for non-numeric input", async () => {
        const collection = client.db(dbName).collection("floor_string");
        await collection.insertOne({ value: "hello" });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $floor: "$value" }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$floor only supports numeric types"));
            return true;
          }
        );
      });
    });

    describe("$round", () => {
      it("should round to nearest integer by default", async () => {
        const collection = client.db(dbName).collection("round_default");
        await collection.insertOne({ value: 2.5 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: "$value" }, _id: 0 } }])
          .toArray();

        // JavaScript rounds 2.5 to 3 (banker's rounding varies, but Math.round goes to 3)
        assert.strictEqual(results[0].result, 3);
      });

      it("should round to specified decimal places", async () => {
        const collection = client.db(dbName).collection("round_places");
        await collection.insertOne({ value: 2.567 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: ["$value", 2] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 2.57);
      });

      it("should round with negative place value", async () => {
        const collection = client.db(dbName).collection("round_neg_place");
        await collection.insertOne({ value: 1234 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: ["$value", -2] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1200);
      });

      it("should return null for null input", async () => {
        const collection = client.db(dbName).collection("round_null");
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $round: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return null for missing field", async () => {
        const collection = client.db(dbName).collection("round_missing");
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should throw for non-numeric input", async () => {
        const collection = client.db(dbName).collection("round_string");
        await collection.insertOne({ value: "hello" });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $round: "$value" }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$round only supports numeric types"));
            return true;
          }
        );
      });

      it("should handle array form with single element", async () => {
        const collection = client.db(dbName).collection("round_array_single");
        await collection.insertOne({ value: 2.7 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: ["$value"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 3);
      });
    });

    describe("$mod", () => {
      it("should compute modulo of two positive numbers", async () => {
        const collection = client.db(dbName).collection("mod_positive");
        await collection.insertOne({ a: 10, b: 3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1);
      });

      it("should handle negative dividend (remainder follows dividend sign)", async () => {
        const collection = client.db(dbName).collection("mod_neg_dividend");
        await collection.insertOne({ a: -10, b: 3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
          .toArray();

        // JavaScript/MongoDB: -10 % 3 = -1
        assert.strictEqual(results[0].result, -1);
      });

      it("should handle negative divisor", async () => {
        const collection = client.db(dbName).collection("mod_neg_divisor");
        await collection.insertOne({ a: 10, b: -3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
          .toArray();

        // JavaScript/MongoDB: 10 % -3 = 1
        assert.strictEqual(results[0].result, 1);
      });

      it("should return null when divisor is 0", async () => {
        const collection = client.db(dbName).collection("mod_zero");
        await collection.insertOne({ a: 10, b: 0 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return null for null dividend", async () => {
        const collection = client.db(dbName).collection("mod_null_dividend");
        await collection.insertOne({ a: null, b: 3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return null for null divisor", async () => {
        const collection = client.db(dbName).collection("mod_null_divisor");
        await collection.insertOne({ a: 10, b: null });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return null for missing field", async () => {
        const collection = client.db(dbName).collection("mod_missing");
        await collection.insertOne({ a: 10 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should throw for non-numeric inputs", async () => {
        const collection = client.db(dbName).collection("mod_string");
        await collection.insertOne({ a: "hello", b: 3 });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$mod only supports numeric types"));
            return true;
          }
        );
      });

      it("should handle floating point modulo", async () => {
        const collection = client.db(dbName).collection("mod_float");
        await collection.insertOne({ a: 10.5, b: 3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ["$a", "$b"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1.5);
      });
    });
  });
});
