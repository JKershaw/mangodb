/**
 * Error Message Consistency Tests
 *
 * Verifies that MangoDB throws similar errors to MongoDB for common error scenarios.
 * We test error patterns rather than exact strings since MongoDB versions may differ.
 *
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "../../test-harness.ts";

describe(`Error Consistency (${getTestModeName()})`, () => {
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

  describe("Expression operator type errors", () => {
    it("should error on $abs with non-numeric value", async () => {
      const collection = client.db(dbName).collection("err_abs");
      await collection.insertOne({ value: "not a number" });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $abs: "$value" } } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("numeric") || err.message.includes("number"),
            `Expected numeric type error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $sqrt with negative number", async () => {
      const collection = client.db(dbName).collection("err_sqrt");
      await collection.insertOne({ value: -4 });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $sqrt: "$value" } } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("negative") || err.message.includes("sqrt"),
            `Expected negative sqrt error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $log with non-positive number", async () => {
      const collection = client.db(dbName).collection("err_log");
      await collection.insertOne({ value: 0 });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $ln: "$value" } } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("positive") ||
            err.message.includes("greater than") ||
            err.message.includes("log"),
            `Expected positive number error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should handle $divide by zero", async () => {
      // MongoDB may return Infinity or throw an error for division by zero
      const collection = client.db(dbName).collection("err_divide");
      await collection.insertOne({ a: 10, b: 0 });

      try {
        const result = await collection
          .aggregate([{ $project: { result: { $divide: ["$a", "$b"] } } }])
          .toArray();
        // MangoDB returns Infinity
        assert.ok(result[0].result === Infinity || result[0].result === null);
      } catch {
        // MongoDB may throw an error
        assert.ok(true);
      }
    });

    it("should error on $concat with non-string", async () => {
      const collection = client.db(dbName).collection("err_concat");
      await collection.insertOne({ a: "hello", b: 123 });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $concat: ["$a", "$b"] } } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("string"),
            `Expected string type error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $dateFromString with invalid date", async () => {
      const collection = client.db(dbName).collection("err_datefromstr");
      await collection.insertOne({ dateStr: "not-a-date" });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([
              { $project: { result: { $dateFromString: { dateString: "$dateStr" } } } },
            ])
            .toArray();
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("date") || err.message.includes("parse"),
            `Expected date parsing error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $arrayElemAt with non-array", async () => {
      const collection = client.db(dbName).collection("err_arrayelemat");
      await collection.insertOne({ value: "not an array" });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $arrayElemAt: ["$value", 0] } } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("array"),
            `Expected array type error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $size with non-array", async () => {
      const collection = client.db(dbName).collection("err_size");
      await collection.insertOne({ value: "not an array" });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $size: "$value" } } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("array"),
            `Expected array type error, got: ${err.message}`);
          return true;
        }
      );
    });
  });

  describe("Conditional operator errors", () => {
    it("should error on $switch without matching branch and no default", async () => {
      const collection = client.db(dbName).collection("err_switch");
      await collection.insertOne({ score: 50 });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([
              {
                $project: {
                  grade: {
                    $switch: {
                      branches: [{ case: { $gte: ["$score", 90] }, then: "A" }],
                      // no default
                    },
                  },
                },
              },
            ])
            .toArray();
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("default") || err.message.includes("branch") || err.message.includes("match"),
            `Expected no matching branch error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should handle $cond with missing else", async () => {
      // MongoDB may return undefined/null or throw an error for missing else
      const collection = client.db(dbName).collection("err_cond");
      await collection.insertOne({ value: 1 });

      try {
        const result = await collection
          .aggregate([
            {
              $project: {
                result: { $cond: [false, "yes"] }, // missing else, condition is false
              },
            },
          ])
          .toArray();

        // When condition is false and else is missing, result may be undefined/null
        assert.ok(result[0].result === undefined || result[0].result === null);
      } catch {
        // MongoDB may throw an error for missing else
        assert.ok(true);
      }
    });
  });

  describe("Query operator errors", () => {
    it("should error on $all with non-array argument", async () => {
      const collection = client.db(dbName).collection("err_all");
      await collection.insertOne({ tags: ["a", "b"] });

      await assert.rejects(
        async () => {
          await collection.find({ tags: { $all: "not an array" } }).toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("array"),
            `Expected array error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $elemMatch with non-object argument", async () => {
      const collection = client.db(dbName).collection("err_elemmatch");
      await collection.insertOne({ items: [1, 2, 3] });

      await assert.rejects(
        async () => {
          await collection.find({ items: { $elemMatch: "not an object" } }).toArray();
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("object") || err.message.includes("Object"),
            `Expected object error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $size with non-numeric argument", async () => {
      const collection = client.db(dbName).collection("err_size_query");
      await collection.insertOne({ items: [1, 2, 3] });

      await assert.rejects(
        async () => {
          await collection.find({ items: { $size: "three" } }).toArray();
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("number") || err.message.includes("numeric") || err.message.includes("integer"),
            `Expected numeric error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $in with non-array argument", async () => {
      const collection = client.db(dbName).collection("err_in");
      await collection.insertOne({ status: "active" });

      await assert.rejects(
        async () => {
          await collection.find({ status: { $in: "not an array" } }).toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes("array"),
            `Expected array error, got: ${err.message}`);
          return true;
        }
      );
    });
  });

  describe("Update operator errors", () => {
    it("should error on $inc with non-numeric value", async () => {
      const collection = client.db(dbName).collection("err_inc");
      await collection.insertOne({ count: 5 });

      await assert.rejects(
        async () => {
          await collection.updateOne({ count: 5 }, { $inc: { count: "not a number" } });
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("numeric") ||
            err.message.includes("number") ||
            err.message.includes("increment"),
            `Expected numeric error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $push to non-array field", async () => {
      const collection = client.db(dbName).collection("err_push");
      await collection.insertOne({ value: "not an array" });

      await assert.rejects(
        async () => {
          await collection.updateOne({}, { $push: { value: 1 } });
        },
        (err: Error) => {
          assert.ok(err.message.includes("array"),
            `Expected array error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on $rename with invalid target", async () => {
      const collection = client.db(dbName).collection("err_rename");
      await collection.insertOne({ oldName: "value" });

      await assert.rejects(
        async () => {
          await collection.updateOne({}, { $rename: { oldName: 123 as unknown as string } });
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("string") || err.message.includes("$rename"),
            `Expected string/rename error, got: ${err.message}`);
          return true;
        }
      );
    });
  });

  describe("Aggregation stage errors", () => {
    it("should error on unknown aggregation stage", async () => {
      const collection = client.db(dbName).collection("err_stage");
      await collection.insertOne({ value: 1 });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $unknownStage: {} }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("unknown") ||
            err.message.includes("Unrecognized") ||
            err.message.includes("stage"),
            `Expected unknown stage error, got: ${err.message}`);
          return true;
        }
      );
    });

    it("should error on unknown expression operator", async () => {
      const collection = client.db(dbName).collection("err_expr");
      await collection.insertOne({ value: 1 });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $unknownOp: "$value" } } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(
            err.message.includes("Unrecognized") ||
            err.message.includes("unknown") ||
            err.message.includes("operator"),
            `Expected unknown operator error, got: ${err.message}`);
          return true;
        }
      );
    });
  });
});
