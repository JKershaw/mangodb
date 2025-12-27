/**
 * Query Operators Tests
 *
 * Tests for $type, $mod, and $expr operators.
 * Note: $text tests are in text-search.test.ts
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "../../test-harness.ts";

describe(`Query Operators Tests (${getTestModeName()})`, () => {
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

  describe("$type Operator", () => {
    describe("Basic Type Matching", () => {
      it("should match string type", async () => {
        const collection = client.db(dbName).collection("type_string");
        await collection.insertMany([
          { value: "hello" },
          { value: 123 },
          { value: true },
        ]);

        const docs = await collection.find({ value: { $type: "string" } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, "hello");
      });

      it("should match number type alias (any numeric)", async () => {
        const collection = client.db(dbName).collection("type_number");
        await collection.insertMany([
          { value: 42 },
          { value: 3.14 },
          { value: "123" },
          { value: true },
        ]);

        const docs = await collection.find({ value: { $type: "number" } }).toArray();

        assert.strictEqual(docs.length, 2);
        assert.ok(docs.every(d => typeof d.value === "number"));
      });

      it("should match double type", async () => {
        const collection = client.db(dbName).collection("type_double");
        await collection.insertMany([
          { value: 3.14 },
          { value: 42 },
          { value: "3.14" },
        ]);

        const docs = await collection.find({ value: { $type: "double" } }).toArray();

        // Only 3.14 is a double; 42 is an integer (int32)
        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, 3.14);
      });

      it("should match int type (integers only)", async () => {
        const collection = client.db(dbName).collection("type_int");
        await collection.insertMany([
          { value: 42 },
          { value: 3.14 },
          { value: -10 },
        ]);

        const docs = await collection.find({ value: { $type: "int" } }).toArray();

        // Should match 42 and -10 (integers), not 3.14
        assert.strictEqual(docs.length, 2);
        assert.ok(docs.every(d => Number.isInteger(d.value)));
      });

      it("should match array type", async () => {
        const collection = client.db(dbName).collection("type_array");
        await collection.insertMany([
          { value: [1, 2, 3] },
          { value: "not an array" },
          { value: { nested: true } },
        ]);

        const docs = await collection.find({ value: { $type: "array" } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.ok(Array.isArray(docs[0].value));
      });

      it("should match object type (embedded documents)", async () => {
        const collection = client.db(dbName).collection("type_object");
        await collection.insertMany([
          { value: { nested: "object" } },
          { value: [1, 2] },
          { value: "string" },
          { value: null },
        ]);

        const docs = await collection.find({ value: { $type: "object" } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.deepStrictEqual(docs[0].value, { nested: "object" });
      });

      it("should match bool type", async () => {
        const collection = client.db(dbName).collection("type_bool");
        await collection.insertMany([
          { value: true },
          { value: false },
          { value: 1 },
          { value: "true" },
        ]);

        const docs = await collection.find({ value: { $type: "bool" } }).toArray();

        assert.strictEqual(docs.length, 2);
        assert.ok(docs.every(d => typeof d.value === "boolean"));
      });

      it("should match null type", async () => {
        const collection = client.db(dbName).collection("type_null");
        await collection.insertMany([
          { value: null },
          { value: "not null" },
          { other: "missing value field" },
        ]);

        const docs = await collection.find({ value: { $type: "null" } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, null);
      });

      it("should match date type", async () => {
        const collection = client.db(dbName).collection("type_date");
        await collection.insertMany([
          { value: new Date() },
          { value: "2024-01-01" },
          { value: 1704067200000 },
        ]);

        const docs = await collection.find({ value: { $type: "date" } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.ok(docs[0].value instanceof Date);
      });

      it("should match objectId type", async () => {
        const collection = client.db(dbName).collection("type_objectid");
        await collection.insertMany([
          { value: "not an id" },
          { value: 12345 },
        ]);

        // _id is always ObjectId
        const docs = await collection.find({ _id: { $type: "objectId" } }).toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe("Numeric Type Codes", () => {
      it("should match using numeric code 2 (string)", async () => {
        const collection = client.db(dbName).collection("type_code_string");
        await collection.insertMany([
          { value: "hello" },
          { value: 123 },
        ]);

        const docs = await collection.find({ value: { $type: 2 } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, "hello");
      });

      it("should match using numeric code 1 (double)", async () => {
        const collection = client.db(dbName).collection("type_code_double");
        await collection.insertMany([
          { value: 3.14 },
          { value: "string" },
        ]);

        const docs = await collection.find({ value: { $type: 1 } }).toArray();

        assert.strictEqual(docs.length, 1);
      });

      it("should match using numeric code 4 (array)", async () => {
        const collection = client.db(dbName).collection("type_code_array");
        await collection.insertMany([
          { value: [1, 2, 3] },
          { value: "not array" },
        ]);

        const docs = await collection.find({ value: { $type: 4 } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.ok(Array.isArray(docs[0].value));
      });

      it("should match using numeric code 8 (bool)", async () => {
        const collection = client.db(dbName).collection("type_code_bool");
        await collection.insertMany([
          { value: true },
          { value: "true" },
        ]);

        const docs = await collection.find({ value: { $type: 8 } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, true);
      });
    });

    describe("Multiple Types", () => {
      it("should match any type in array", async () => {
        const collection = client.db(dbName).collection("type_multi_any");
        await collection.insertMany([
          { value: "hello" },
          { value: 123 },
          { value: true },
        ]);

        const docs = await collection.find({ value: { $type: ["string", "bool"] } }).toArray();

        assert.strictEqual(docs.length, 2);
      });

      it("should match string or null", async () => {
        const collection = client.db(dbName).collection("type_string_null");
        await collection.insertMany([
          { value: "hello" },
          { value: null },
          { value: 123 },
        ]);

        const docs = await collection.find({ value: { $type: ["string", "null"] } }).toArray();

        assert.strictEqual(docs.length, 2);
      });

      it("should match number or string using mixed codes", async () => {
        const collection = client.db(dbName).collection("type_mixed_codes");
        await collection.insertMany([
          { value: "hello" },
          { value: 42 },
          { value: true },
        ]);

        // Mix string alias and numeric code
        const docs = await collection.find({ value: { $type: ["number", 2] } }).toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe("Edge Cases", () => {
      it("should not match missing fields for any type", async () => {
        const collection = client.db(dbName).collection("type_missing");
        await collection.insertMany([
          { value: "exists" },
          { other: "no value field" },
        ]);

        const docs = await collection.find({ value: { $type: "string" } }).toArray();

        assert.strictEqual(docs.length, 1);
      });

      it("should not match missing fields for null type", async () => {
        const collection = client.db(dbName).collection("type_missing_null");
        await collection.insertMany([
          { value: null },
          { other: "no value field" },
        ]);

        const docs = await collection.find({ value: { $type: "null" } }).toArray();

        // Only matches explicit null, not missing field
        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, null);
      });

      it("should match array elements by type (MongoDB behavior)", async () => {
        const collection = client.db(dbName).collection("type_array_elements");
        await collection.insertMany([
          { tags: ["a", "b", "c"] },
          { values: [1, 2, 3] },
          { mixed: ["a", 1, true] },
        ]);

        // $type: "array" matches fields that ARE arrays
        const arrayDocs = await collection.find({ tags: { $type: "array" } }).toArray();
        assert.strictEqual(arrayDocs.length, 1);

        // $type: "string" matches arrays containing string elements
        const stringDocs = await collection.find({ tags: { $type: "string" } }).toArray();
        assert.strictEqual(stringDocs.length, 1);

        // $type: "number" matches arrays containing number elements
        const numberDocs = await collection.find({ values: { $type: "number" } }).toArray();
        assert.strictEqual(numberDocs.length, 1);

        // Mixed array matches multiple types
        const mixedStringDocs = await collection.find({ mixed: { $type: "string" } }).toArray();
        assert.strictEqual(mixedStringDocs.length, 1);

        const mixedNumberDocs = await collection.find({ mixed: { $type: "number" } }).toArray();
        assert.strictEqual(mixedNumberDocs.length, 1);

        const mixedBoolDocs = await collection.find({ mixed: { $type: "bool" } }).toArray();
        assert.strictEqual(mixedBoolDocs.length, 1);
      });

      it("should work with nested fields", async () => {
        const collection = client.db(dbName).collection("type_nested");
        await collection.insertMany([
          { user: { name: "Alice", age: 30 } },
          { user: { name: "Bob", age: "thirty" } },
        ]);

        const docs = await collection.find({ "user.age": { $type: "number" } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual((docs[0].user as any).name, "Alice");
      });
    });

    describe("Error Cases", () => {
      it("should throw for unknown type alias", async () => {
        const collection = client.db(dbName).collection("type_err_alias");
        await collection.insertOne({ value: "test" });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $type: "unknown" as any } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("Unknown type name alias");
          }
        );
      });

      it("should throw for invalid type alias case", async () => {
        const collection = client.db(dbName).collection("type_err_case");
        await collection.insertOne({ value: "test" });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $type: "String" as any } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("Unknown type name alias");
          }
        );
      });

      it("should throw for invalid numeric type code", async () => {
        const collection = client.db(dbName).collection("type_err_code");
        await collection.insertOne({ value: "test" });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $type: 999 } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("Invalid numerical type code");
          }
        );
      });
    });
  });

  describe("$mod Operator", () => {
    describe("Basic Modulo Matching", () => {
      it("should find even numbers", async () => {
        const collection = client.db(dbName).collection("mod_even");
        await collection.insertMany([
          { value: 2 },
          { value: 3 },
          { value: 4 },
          { value: 5 },
          { value: 6 },
        ]);

        const docs = await collection.find({ value: { $mod: [2, 0] } }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.ok(docs.every(d => (d.value as number) % 2 === 0));
      });

      it("should find odd numbers", async () => {
        const collection = client.db(dbName).collection("mod_odd");
        await collection.insertMany([
          { value: 1 },
          { value: 2 },
          { value: 3 },
          { value: 4 },
          { value: 5 },
        ]);

        const docs = await collection.find({ value: { $mod: [2, 1] } }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.ok(docs.every(d => (d.value as number) % 2 === 1));
      });

      it("should find multiples of 3", async () => {
        const collection = client.db(dbName).collection("mod_mult3");
        await collection.insertMany([
          { value: 3 },
          { value: 6 },
          { value: 7 },
          { value: 9 },
          { value: 10 },
        ]);

        const docs = await collection.find({ value: { $mod: [3, 0] } }).toArray();

        assert.strictEqual(docs.length, 3);
      });

      it("should find values with specific remainder", async () => {
        const collection = client.db(dbName).collection("mod_remainder");
        await collection.insertMany([
          { value: 7 },
          { value: 12 },
          { value: 17 },
          { value: 20 },
        ]);

        // Find values where value % 5 === 2 (7, 12, 17 all have remainder 2)
        const docs = await collection.find({ value: { $mod: [5, 2] } }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.ok(docs.some(d => d.value === 7));
        assert.ok(docs.some(d => d.value === 12));
        assert.ok(docs.some(d => d.value === 17));
      });

      it("should handle negative numbers", async () => {
        const collection = client.db(dbName).collection("mod_negative");
        await collection.insertMany([
          { value: -4 },
          { value: -3 },
          { value: -2 },
          { value: 0 },
          { value: 2 },
        ]);

        // Even numbers (including negative)
        const docs = await collection.find({ value: { $mod: [2, 0] } }).toArray();

        assert.strictEqual(docs.length, 4);
      });
    });

    describe("Edge Cases", () => {
      it("should not match non-numeric fields", async () => {
        const collection = client.db(dbName).collection("mod_nonnumeric");
        await collection.insertMany([
          { value: "4" },
          { value: 4 },
          { value: true },
        ]);

        const docs = await collection.find({ value: { $mod: [2, 0] } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, 4);
      });

      it("should not match null values", async () => {
        const collection = client.db(dbName).collection("mod_null");
        await collection.insertMany([
          { value: null },
          { value: 4 },
        ]);

        const docs = await collection.find({ value: { $mod: [2, 0] } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, 4);
      });

      it("should not match missing fields", async () => {
        const collection = client.db(dbName).collection("mod_missing");
        await collection.insertMany([
          { other: "field" },
          { value: 4 },
        ]);

        const docs = await collection.find({ value: { $mod: [2, 0] } }).toArray();

        assert.strictEqual(docs.length, 1);
      });

      it("should not match NaN document values", async () => {
        const collection = client.db(dbName).collection("mod_nan_doc");
        await collection.insertMany([
          { value: NaN },
          { value: 4 },
        ]);

        const docs = await collection.find({ value: { $mod: [2, 0] } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, 4);
      });

      it("should not match Infinity document values", async () => {
        const collection = client.db(dbName).collection("mod_infinity_doc");
        await collection.insertMany([
          { value: Infinity },
          { value: -Infinity },
          { value: 6 },
        ]);

        const docs = await collection.find({ value: { $mod: [3, 0] } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].value, 6);
      });

      it("should handle floating point divisor (truncation)", async () => {
        const collection = client.db(dbName).collection("mod_float_divisor");
        await collection.insertMany([
          { value: 4 },
          { value: 5 },
          { value: 6 },
        ]);

        // 2.9 is truncated to 2
        const docs = await collection.find({ value: { $mod: [2.9, 0] } }).toArray();

        assert.strictEqual(docs.length, 2);
        assert.ok(docs.some(d => d.value === 4));
        assert.ok(docs.some(d => d.value === 6));
      });

      it("should handle negative divisor", async () => {
        const collection = client.db(dbName).collection("mod_neg_divisor");
        await collection.insertMany([
          { value: 6 },
          { value: 7 },
          { value: 9 },
        ]);

        // -3 as divisor works like 3
        const docs = await collection.find({ value: { $mod: [-3, 0] } }).toArray();

        assert.strictEqual(docs.length, 2);
      });

      it("should work with nested fields", async () => {
        const collection = client.db(dbName).collection("mod_nested");
        await collection.insertMany([
          { data: { count: 10 } },
          { data: { count: 15 } },
          { data: { count: 20 } },
        ]);

        const docs = await collection.find({ "data.count": { $mod: [10, 0] } }).toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe("Error Cases", () => {
      it("should throw for array with one element", async () => {
        const collection = client.db(dbName).collection("mod_err_one");
        await collection.insertOne({ value: 4 });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $mod: [4] as any } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("not enough elements");
          }
        );
      });

      it("should throw for array with more than two elements", async () => {
        const collection = client.db(dbName).collection("mod_err_many");
        await collection.insertOne({ value: 4 });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $mod: [4, 1, 2, 3] as any } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("too many elements");
          }
        );
      });

      it("should throw for empty array", async () => {
        const collection = client.db(dbName).collection("mod_err_empty");
        await collection.insertOne({ value: 4 });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $mod: [] as any } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("not enough elements");
          }
        );
      });

      it("should throw for non-array argument", async () => {
        const collection = client.db(dbName).collection("mod_err_nonarray");
        await collection.insertOne({ value: 4 });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $mod: 2 as any } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("needs to be an array");
          }
        );
      });

      it("should throw for zero divisor", async () => {
        const collection = client.db(dbName).collection("mod_err_zero");
        await collection.insertOne({ value: 4 });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $mod: [0, 0] } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("divisor cannot be 0");
          }
        );
      });

      it("should throw for non-numeric divisor", async () => {
        const collection = client.db(dbName).collection("mod_err_nonnumeric");
        await collection.insertOne({ value: 4 });

        await assert.rejects(
          async () => {
            await collection.find({ value: { $mod: ["two", 0] as any } }).toArray();
          },
          (err: Error) => {
            return err.message.includes("divisor") || err.message.includes("not a number");
          }
        );
      });
    });
  });

  describe("$expr Operator", () => {
    describe("Field Comparisons", () => {
      it("should compare two fields with $gt", async () => {
        const collection = client.db(dbName).collection("expr_gt");
        await collection.insertMany([
          { quantity: 100, threshold: 50 },
          { quantity: 30, threshold: 50 },
          { quantity: 50, threshold: 50 },
        ]);

        const docs = await collection.find({
          $expr: { $gt: ["$quantity", "$threshold"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].quantity, 100);
      });

      it("should compare two fields with $lt", async () => {
        const collection = client.db(dbName).collection("expr_lt");
        await collection.insertMany([
          { sold: 100, target: 150 },
          { sold: 200, target: 150 },
          { sold: 150, target: 150 },
        ]);

        const docs = await collection.find({
          $expr: { $lt: ["$sold", "$target"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].sold, 100);
      });

      it("should compare two fields with $eq", async () => {
        const collection = client.db(dbName).collection("expr_eq");
        await collection.insertMany([
          { a: 10, b: 10 },
          { a: 10, b: 20 },
          { a: 20, b: 10 },
        ]);

        const docs = await collection.find({
          $expr: { $eq: ["$a", "$b"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].a, 10);
        assert.strictEqual(docs[0].b, 10);
      });

      it("should compare two fields with $gte", async () => {
        const collection = client.db(dbName).collection("expr_gte");
        await collection.insertMany([
          { score: 80, passing: 70 },
          { score: 70, passing: 70 },
          { score: 60, passing: 70 },
        ]);

        const docs = await collection.find({
          $expr: { $gte: ["$score", "$passing"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 2);
      });

      it("should compare two fields with $lte", async () => {
        const collection = client.db(dbName).collection("expr_lte");
        await collection.insertMany([
          { price: 50, budget: 100 },
          { price: 100, budget: 100 },
          { price: 150, budget: 100 },
        ]);

        const docs = await collection.find({
          $expr: { $lte: ["$price", "$budget"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 2);
      });

      it("should compare two fields with $ne", async () => {
        const collection = client.db(dbName).collection("expr_ne");
        await collection.insertMany([
          { first: "Alice", preferred: "Alice" },
          { first: "Bob", preferred: "Robert" },
        ]);

        const docs = await collection.find({
          $expr: { $ne: ["$first", "$preferred"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].first, "Bob");
      });

      it("should compare nested fields", async () => {
        const collection = client.db(dbName).collection("expr_nested");
        await collection.insertMany([
          { stats: { current: 100, previous: 80 } },
          { stats: { current: 50, previous: 80 } },
        ]);

        const docs = await collection.find({
          $expr: { $gt: ["$stats.current", "$stats.previous"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual((docs[0].stats as any).current, 100);
      });
    });

    describe("Field to Constant Comparisons", () => {
      it("should compare field to constant number", async () => {
        const collection = client.db(dbName).collection("expr_const_num");
        await collection.insertMany([
          { score: 85 },
          { score: 95 },
          { score: 75 },
        ]);

        const docs = await collection.find({
          $expr: { $gte: ["$score", 90] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].score, 95);
      });

      it("should compare field to constant string", async () => {
        const collection = client.db(dbName).collection("expr_const_str");
        await collection.insertMany([
          { status: "active" },
          { status: "pending" },
          { status: "completed" },
        ]);

        const docs = await collection.find({
          $expr: { $eq: ["$status", "active"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].status, "active");
      });
    });

    describe("Arithmetic Expressions", () => {
      it("should use $add in comparison", async () => {
        const collection = client.db(dbName).collection("expr_add");
        await collection.insertMany([
          { price: 80, tax: 10, budget: 100 },
          { price: 90, tax: 15, budget: 100 },
        ]);

        // Find where price + tax <= budget
        const docs = await collection.find({
          $expr: { $lte: [{ $add: ["$price", "$tax"] }, "$budget"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].price, 80);
      });

      it("should use $subtract in comparison", async () => {
        const collection = client.db(dbName).collection("expr_subtract");
        await collection.insertMany([
          { total: 100, discount: 20 },
          { total: 100, discount: 5 },
        ]);

        // Find where total - discount > 90
        const docs = await collection.find({
          $expr: { $gt: [{ $subtract: ["$total", "$discount"] }, 90] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].discount, 5);
      });

      it("should use $multiply in comparison", async () => {
        const collection = client.db(dbName).collection("expr_multiply");
        await collection.insertMany([
          { quantity: 5, unitPrice: 10 },
          { quantity: 3, unitPrice: 10 },
        ]);

        // Find where quantity * unitPrice >= 50
        const docs = await collection.find({
          $expr: { $gte: [{ $multiply: ["$quantity", "$unitPrice"] }, 50] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].quantity, 5);
      });
    });

    describe("Combined with Regular Query", () => {
      it("should work with field equality", async () => {
        const collection = client.db(dbName).collection("expr_combined");
        await collection.insertMany([
          { status: "active", sold: 100, target: 80 },
          { status: "active", sold: 50, target: 80 },
          { status: "inactive", sold: 100, target: 80 },
        ]);

        const docs = await collection.find({
          status: "active",
          $expr: { $gt: ["$sold", "$target"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].sold, 100);
        assert.strictEqual(docs[0].status, "active");
      });

      it("should work with other query operators", async () => {
        const collection = client.db(dbName).collection("expr_with_ops");
        await collection.insertMany([
          { category: "A", value: 100, limit: 80 },
          { category: "B", value: 100, limit: 80 },
          { category: "A", value: 50, limit: 80 },
        ]);

        const docs = await collection.find({
          category: { $in: ["A", "B"] },
          $expr: { $gt: ["$value", "$limit"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe("Edge Cases", () => {
      it("should handle missing fields (evaluate to null)", async () => {
        const collection = client.db(dbName).collection("expr_missing");
        await collection.insertMany([
          { a: 10, b: 5 },
          { a: 10 }, // b is missing
        ]);

        const docs = await collection.find({
          $expr: { $gt: ["$a", "$b"] }
        } as any).toArray();

        // Both match: 10 > 5 = true, 10 > null = true (numbers > null in BSON ordering)
        assert.strictEqual(docs.length, 2);
      });

      it("should handle boolean result directly", async () => {
        const collection = client.db(dbName).collection("expr_bool");
        await collection.insertMany([
          { active: true },
          { active: false },
          { active: 1 },
        ]);

        const docs = await collection.find({
          $expr: "$active"
        } as any).toArray();

        // Matches truthy values: true and 1
        assert.strictEqual(docs.length, 2);
      });

      it("should work with $size for array comparison", async () => {
        const collection = client.db(dbName).collection("expr_size");
        await collection.insertMany([
          { items: [1, 2, 3], minItems: 2 },
          { items: [1], minItems: 2 },
        ]);

        const docs = await collection.find({
          $expr: { $gte: [{ $size: "$items" }, "$minItems"] }
        } as any).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual((docs[0].items as number[]).length, 3);
      });
    });
  });
});
