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

  // ==================== Part 2: String Operators ====================

  describe("String Operators", () => {
    describe("$substrCP", () => {
      it("should extract substring from start", async () => {
        const collection = client.db(dbName).collection("substr_basic");
        await collection.insertOne({ value: "hello world" });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ["$value", 0, 5] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "hello");
      });

      it("should extract substring from middle", async () => {
        const collection = client.db(dbName).collection("substr_middle");
        await collection.insertOne({ value: "hello world" });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ["$value", 6, 5] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "world");
      });

      it("should handle out of bounds gracefully", async () => {
        const collection = client.db(dbName).collection("substr_oob");
        await collection.insertOne({ value: "hello" });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ["$value", 3, 100] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "lo");
      });

      it("should return empty string for null input", async () => {
        const collection = client.db(dbName).collection("substr_null");
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ["$value", 0, 5] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "");
      });

      it("should return empty string for missing field", async () => {
        const collection = client.db(dbName).collection("substr_missing");
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ["$value", 0, 5] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "");
      });
    });

    describe("$strLenCP", () => {
      it("should return string length", async () => {
        const collection = client.db(dbName).collection("strlen_basic");
        await collection.insertOne({ value: "hello" });

        const results = await collection
          .aggregate([{ $project: { result: { $strLenCP: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it("should return 0 for empty string", async () => {
        const collection = client.db(dbName).collection("strlen_empty");
        await collection.insertOne({ value: "" });

        const results = await collection
          .aggregate([{ $project: { result: { $strLenCP: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 0);
      });

      it("should throw for null input", async () => {
        const collection = client.db(dbName).collection("strlen_null");
        await collection.insertOne({ value: null });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $strLenCP: "$value" }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$strLenCP requires a string argument"));
            return true;
          }
        );
      });

      it("should throw for non-string input", async () => {
        const collection = client.db(dbName).collection("strlen_number");
        await collection.insertOne({ value: 123 });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $strLenCP: "$value" }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$strLenCP requires a string argument"));
            return true;
          }
        );
      });
    });

    describe("$split", () => {
      it("should split string by delimiter", async () => {
        const collection = client.db(dbName).collection("split_basic");
        await collection.insertOne({ value: "a,b,c" });

        const results = await collection
          .aggregate([{ $project: { result: { $split: ["$value", ","] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, ["a", "b", "c"]);
      });

      it("should return single-element array when delimiter not found", async () => {
        const collection = client.db(dbName).collection("split_notfound");
        await collection.insertOne({ value: "hello" });

        const results = await collection
          .aggregate([{ $project: { result: { $split: ["$value", ","] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, ["hello"]);
      });

      it("should handle empty parts", async () => {
        const collection = client.db(dbName).collection("split_empty");
        await collection.insertOne({ value: ",a,,b," });

        const results = await collection
          .aggregate([{ $project: { result: { $split: ["$value", ","] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, ["", "a", "", "b", ""]);
      });

      it("should throw for null string", async () => {
        const collection = client.db(dbName).collection("split_null");
        await collection.insertOne({ value: null });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $split: ["$value", ","] }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$split"));
            return true;
          }
        );
      });
    });

    describe("$trim", () => {
      it("should trim whitespace by default", async () => {
        const collection = client.db(dbName).collection("trim_basic");
        await collection.insertOne({ value: "  hello  " });

        const results = await collection
          .aggregate([{ $project: { result: { $trim: { input: "$value" } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "hello");
      });

      it("should trim custom characters", async () => {
        const collection = client.db(dbName).collection("trim_custom");
        await collection.insertOne({ value: "xxhelloxx" });

        const results = await collection
          .aggregate([{ $project: { result: { $trim: { input: "$value", chars: "x" } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "hello");
      });

      it("should throw for non-string input", async () => {
        const collection = client.db(dbName).collection("trim_number");
        await collection.insertOne({ value: 123 });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $trim: { input: "$value" } }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes("$trim"));
            return true;
          }
        );
      });
    });

    describe("$ltrim", () => {
      it("should trim left whitespace only", async () => {
        const collection = client.db(dbName).collection("ltrim_basic");
        await collection.insertOne({ value: "  hello  " });

        const results = await collection
          .aggregate([{ $project: { result: { $ltrim: { input: "$value" } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "hello  ");
      });
    });

    describe("$rtrim", () => {
      it("should trim right whitespace only", async () => {
        const collection = client.db(dbName).collection("rtrim_basic");
        await collection.insertOne({ value: "  hello  " });

        const results = await collection
          .aggregate([{ $project: { result: { $rtrim: { input: "$value" } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "  hello");
      });
    });

    describe("$toString", () => {
      it("should convert number to string", async () => {
        const collection = client.db(dbName).collection("tostring_num");
        await collection.insertOne({ value: 123 });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "123");
      });

      it("should convert boolean true to string", async () => {
        const collection = client.db(dbName).collection("tostring_true");
        await collection.insertOne({ value: true });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "true");
      });

      it("should convert boolean false to string", async () => {
        const collection = client.db(dbName).collection("tostring_false");
        await collection.insertOne({ value: false });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "false");
      });

      it("should return null for null input", async () => {
        const collection = client.db(dbName).collection("tostring_null");
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return null for missing field", async () => {
        const collection = client.db(dbName).collection("tostring_missing");
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should keep string unchanged", async () => {
        const collection = client.db(dbName).collection("tostring_string");
        await collection.insertOne({ value: "hello" });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: "$value" }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, "hello");
      });
    });

    describe("$indexOfCP", () => {
      it("should return index of substring", async () => {
        const collection = client.db(dbName).collection("indexof_found");
        await collection.insertOne({ value: "hello world" });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ["$value", "world"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 6);
      });

      it("should return -1 when not found", async () => {
        const collection = client.db(dbName).collection("indexof_notfound");
        await collection.insertOne({ value: "hello" });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ["$value", "xyz"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, -1);
      });

      it("should return null for null string", async () => {
        const collection = client.db(dbName).collection("indexof_null");
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ["$value", "x"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it("should return 0 for match at beginning", async () => {
        const collection = client.db(dbName).collection("indexof_start");
        await collection.insertOne({ value: "hello" });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ["$value", "hel"] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 0);
      });

      it("should support start index", async () => {
        const collection = client.db(dbName).collection("indexof_startidx");
        await collection.insertOne({ value: "hello hello" });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ["$value", "hello", 1] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 6);
      });
    });
  });
});
