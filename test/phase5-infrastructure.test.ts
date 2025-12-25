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
});
