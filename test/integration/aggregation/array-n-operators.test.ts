/**
 * Array N operators tests - $firstN, $lastN, $minN, $maxN
 *
 * Note: MongoDB may have different edge case behavior for n=0 or null input.
 * These tests handle both scenarios (return value or error).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "../../test-harness.ts";

describe(`Array N Operators (${getTestModeName()})`, () => {
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

  describe("$firstN", () => {
    it("should return first N elements", async () => {
      const collection = client.db(dbName).collection("firstn_basic");
      await collection.insertOne({ values: [1, 2, 3, 4, 5] });

      const result = await collection
        .aggregate([
          {
            $project: {
              first3: { $firstN: { n: 3, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].first3, [1, 2, 3]);
    });

    it("should return all elements when n > array length", async () => {
      const collection = client.db(dbName).collection("firstn_overflow");
      await collection.insertOne({ values: [1, 2] });

      const result = await collection
        .aggregate([
          {
            $project: {
              first10: { $firstN: { n: 10, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].first10, [1, 2]);
    });

    it("should work with computed n", async () => {
      const collection = client.db(dbName).collection("firstn_computed");
      await collection.insertOne({ values: [1, 2, 3, 4, 5], count: 2 });

      const result = await collection
        .aggregate([
          {
            $project: {
              firstN: { $firstN: { n: "$count", input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].firstN, [1, 2]);
    });
  });

  describe("$lastN", () => {
    it("should return last N elements", async () => {
      const collection = client.db(dbName).collection("lastn_basic");
      await collection.insertOne({ values: [1, 2, 3, 4, 5] });

      const result = await collection
        .aggregate([
          {
            $project: {
              last3: { $lastN: { n: 3, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].last3, [3, 4, 5]);
    });

    it("should return all elements when n > array length", async () => {
      const collection = client.db(dbName).collection("lastn_overflow");
      await collection.insertOne({ values: [1, 2] });

      const result = await collection
        .aggregate([
          {
            $project: {
              last10: { $lastN: { n: 10, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].last10, [1, 2]);
    });
  });

  describe("$minN", () => {
    it("should return N smallest values", async () => {
      const collection = client.db(dbName).collection("minn_basic");
      await collection.insertOne({ values: [3, 1, 4, 1, 5, 9, 2] });

      const result = await collection
        .aggregate([
          {
            $project: {
              min3: { $minN: { n: 3, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].min3, [1, 1, 2]);
    });

    it("should handle strings", async () => {
      const collection = client.db(dbName).collection("minn_strings");
      await collection.insertOne({ values: ["banana", "apple", "cherry", "date"] });

      const result = await collection
        .aggregate([
          {
            $project: {
              min2: { $minN: { n: 2, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].min2, ["apple", "banana"]);
    });

    it("should return all elements sorted when n >= array length", async () => {
      const collection = client.db(dbName).collection("minn_all");
      await collection.insertOne({ values: [3, 1, 2] });

      const result = await collection
        .aggregate([
          {
            $project: {
              min10: { $minN: { n: 10, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].min10, [1, 2, 3]);
    });
  });

  describe("$maxN", () => {
    it("should return N largest values", async () => {
      const collection = client.db(dbName).collection("maxn_basic");
      await collection.insertOne({ values: [3, 1, 4, 1, 5, 9, 2] });

      const result = await collection
        .aggregate([
          {
            $project: {
              max3: { $maxN: { n: 3, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].max3, [9, 5, 4]);
    });

    it("should handle strings", async () => {
      const collection = client.db(dbName).collection("maxn_strings");
      await collection.insertOne({ values: ["banana", "apple", "cherry", "date"] });

      const result = await collection
        .aggregate([
          {
            $project: {
              max2: { $maxN: { n: 2, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].max2, ["date", "cherry"]);
    });

    it("should return all elements sorted when n >= array length", async () => {
      const collection = client.db(dbName).collection("maxn_all");
      await collection.insertOne({ values: [3, 1, 2] });

      const result = await collection
        .aggregate([
          {
            $project: {
              max10: { $maxN: { n: 10, input: "$values" } }
            }
          }
        ])
        .toArray();

      assert.deepStrictEqual(result[0].max10, [3, 2, 1]);
    });
  });
});
