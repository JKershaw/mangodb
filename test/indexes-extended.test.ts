/**
 * Phase 14: Extended Index Features Tests
 *
 * Tests for sparse indexes, TTL indexes, partial indexes, and index hints.
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "./test-harness.ts";

describe(`Sparse Index Tests (${getTestModeName()})`, () => {
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

  describe("Basic Sparse Behavior", () => {
    it("should allow multiple documents with missing field on sparse unique index", async () => {
      const collection = client.db(dbName).collection("sparse_missing");
      await collection.createIndex({ email: 1 }, { unique: true, sparse: true });

      // Both documents are missing the email field - should be allowed with sparse
      await collection.insertOne({ name: "Alice" });
      await collection.insertOne({ name: "Bob" });

      const count = await collection.countDocuments({});
      assert.strictEqual(count, 2);
    });

    it("should NOT allow duplicate null values (null is indexed)", async () => {
      const collection = client.db(dbName).collection("sparse_null");
      await collection.createIndex({ email: 1 }, { unique: true, sparse: true });

      await collection.insertOne({ name: "Alice", email: null });

      await assert.rejects(
        async () => {
          await collection.insertOne({ name: "Bob", email: null });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should enforce uniqueness for present values", async () => {
      const collection = client.db(dbName).collection("sparse_unique");
      await collection.createIndex({ email: 1 }, { unique: true, sparse: true });

      await collection.insertOne({ name: "Alice", email: "alice@test.com" });

      await assert.rejects(
        async () => {
          await collection.insertOne({ name: "Bob", email: "alice@test.com" });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should allow different values with sparse unique index", async () => {
      const collection = client.db(dbName).collection("sparse_different");
      await collection.createIndex({ email: 1 }, { unique: true, sparse: true });

      await collection.insertOne({ name: "Alice", email: "alice@test.com" });
      await collection.insertOne({ name: "Bob", email: "bob@test.com" });
      await collection.insertOne({ name: "Charlie" }); // missing field - OK

      const count = await collection.countDocuments({});
      assert.strictEqual(count, 3);
    });
  });

  describe("Compound Sparse Indexes", () => {
    it("should index document if at least one field is present", async () => {
      const collection = client.db(dbName).collection("sparse_compound_one");
      await collection.createIndex(
        { a: 1, b: 1 },
        { unique: true, sparse: true }
      );

      // Document with only 'a' is indexed
      await collection.insertOne({ a: 1 });

      // Same 'a' value with missing 'b' should fail (both indexed as {a:1, b:null})
      await assert.rejects(
        async () => {
          await collection.insertOne({ a: 1 });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should skip document only if ALL indexed fields are missing", async () => {
      const collection = client.db(dbName).collection("sparse_compound_all");
      await collection.createIndex(
        { a: 1, b: 1 },
        { unique: true, sparse: true }
      );

      // Both fields missing - not indexed, so multiple allowed
      await collection.insertOne({ name: "first" });
      await collection.insertOne({ name: "second" });

      const count = await collection.countDocuments({});
      assert.strictEqual(count, 2);
    });

    it("should treat missing fields as null for uniqueness check when at least one field present", async () => {
      const collection = client.db(dbName).collection("sparse_compound_null");
      await collection.createIndex(
        { a: 1, b: 1 },
        { unique: true, sparse: true }
      );

      // { a: 1 } indexed as { a: 1, b: null }
      await collection.insertOne({ a: 1 });

      // { a: 1, b: null } is the same key
      await assert.rejects(
        async () => {
          await collection.insertOne({ a: 1, b: null });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });
  });

  describe("Sparse Index with Nested Fields", () => {
    it("should allow missing nested field on sparse unique index", async () => {
      const collection = client.db(dbName).collection("sparse_nested");
      await collection.createIndex(
        { "address.email": 1 },
        { unique: true, sparse: true }
      );

      // Documents without address.email are not indexed
      await collection.insertOne({ name: "Alice" });
      await collection.insertOne({ name: "Bob" });
      await collection.insertOne({ address: { city: "NYC" } }); // has address but no email

      const count = await collection.countDocuments({});
      assert.strictEqual(count, 3);
    });

    it("should enforce uniqueness for present nested values", async () => {
      const collection = client.db(dbName).collection("sparse_nested_unique");
      await collection.createIndex(
        { "profile.email": 1 },
        { unique: true, sparse: true }
      );

      await collection.insertOne({ profile: { email: "alice@test.com" } });

      await assert.rejects(
        async () => {
          await collection.insertOne({ profile: { email: "alice@test.com" } });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });
  });

  describe("Sparse Index Metadata", () => {
    it("should list sparse: true in indexes()", async () => {
      const collection = client.db(dbName).collection("sparse_metadata");
      await collection.createIndex({ email: 1 }, { unique: true, sparse: true });

      const indexes = await collection.indexes();
      const emailIndex = indexes.find((i) => i.name === "email_1");

      assert(emailIndex);
      assert.strictEqual(emailIndex.sparse, true);
      assert.strictEqual(emailIndex.unique, true);
    });
  });
});

describe(`TTL Index Tests (${getTestModeName()})`, () => {
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

  describe("TTL Index Creation", () => {
    it("should create TTL index with expireAfterSeconds", async () => {
      const collection = client.db(dbName).collection("ttl_create");

      const name = await collection.createIndex(
        { createdAt: 1 },
        { expireAfterSeconds: 3600 }
      );

      assert.strictEqual(name, "createdAt_1");
    });

    it("should store expireAfterSeconds in index metadata", async () => {
      const collection = client.db(dbName).collection("ttl_metadata");
      await collection.createIndex(
        { expiresAt: 1 },
        { expireAfterSeconds: 7200 }
      );

      const indexes = await collection.indexes();
      const ttlIndex = indexes.find((i) => i.name === "expiresAt_1");

      assert(ttlIndex);
      assert.strictEqual(
        (ttlIndex as { expireAfterSeconds?: number }).expireAfterSeconds,
        7200
      );
    });

    it("should accept expireAfterSeconds: 0", async () => {
      const collection = client.db(dbName).collection("ttl_zero");

      const name = await collection.createIndex(
        { timestamp: 1 },
        { expireAfterSeconds: 0 }
      );

      assert.strictEqual(name, "timestamp_1");

      const indexes = await collection.indexes();
      const ttlIndex = indexes.find((i) => i.name === "timestamp_1");
      assert.strictEqual(
        (ttlIndex as { expireAfterSeconds?: number }).expireAfterSeconds,
        0
      );
    });

    it("should silently ignore TTL on compound indexes", async () => {
      const collection = client.db(dbName).collection("ttl_compound");

      // MongoDB silently ignores expireAfterSeconds on compound indexes
      const name = await collection.createIndex(
        { a: 1, b: 1 },
        { expireAfterSeconds: 3600 }
      );

      assert.strictEqual(name, "a_1_b_1");

      // The index should exist but without TTL
      const indexes = await collection.indexes();
      const compoundIndex = indexes.find((i) => i.name === "a_1_b_1");
      assert(compoundIndex);
      // TTL should not be present on compound index
      assert.strictEqual(
        (compoundIndex as { expireAfterSeconds?: number }).expireAfterSeconds,
        undefined
      );
    });
  });

  describe("TTL Index Validation", () => {
    it("should reject TTL index on _id field", async () => {
      const collection = client.db(dbName).collection("ttl_reject_id");

      await assert.rejects(
        async () => {
          await collection.createIndex({ _id: 1 }, { expireAfterSeconds: 3600 });
        },
        (err: Error) => {
          // MongoDB rejects TTL on _id field
          assert(
            err.message.includes("_id") ||
              err.message.includes("expireAfterSeconds") ||
              (err as { code?: number }).code === 67
          );
          return true;
        }
      );
    });

    it("should reject negative expireAfterSeconds", async () => {
      const collection = client.db(dbName).collection("ttl_reject_negative");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { createdAt: 1 },
            { expireAfterSeconds: -1 }
          );
        },
        (err: Error) => {
          assert(
            err.message.includes("expireAfterSeconds") ||
              (err as { code?: number }).code === 67
          );
          return true;
        }
      );
    });

    it("should reject expireAfterSeconds > 2147483647", async () => {
      const collection = client.db(dbName).collection("ttl_reject_overflow");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { createdAt: 1 },
            { expireAfterSeconds: 2147483648 }
          );
        },
        (err: Error) => {
          assert(
            err.message.includes("expireAfterSeconds") ||
              (err as { code?: number }).code === 67
          );
          return true;
        }
      );
    });

    it("should accept max valid expireAfterSeconds (2147483647)", async () => {
      const collection = client.db(dbName).collection("ttl_max_valid");

      const name = await collection.createIndex(
        { createdAt: 1 },
        { expireAfterSeconds: 2147483647 }
      );

      assert.strictEqual(name, "createdAt_1");

      const indexes = await collection.indexes();
      const ttlIndex = indexes.find((i) => i.name === "createdAt_1");
      assert(ttlIndex);
      assert.strictEqual(
        (ttlIndex as { expireAfterSeconds?: number }).expireAfterSeconds,
        2147483647
      );
    });
  });
});

describe(`Partial Index Tests (${getTestModeName()})`, () => {
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

  describe("Partial Index Creation", () => {
    it("should create partial index with partialFilterExpression", async () => {
      const collection = client.db(dbName).collection("partial_create");

      const name = await collection.createIndex(
        { email: 1 },
        {
          unique: true,
          partialFilterExpression: { status: "active" },
        }
      );

      assert.strictEqual(name, "email_1");
    });

    it("should store partialFilterExpression in index metadata", async () => {
      const collection = client.db(dbName).collection("partial_metadata");
      await collection.createIndex(
        { username: 1 },
        {
          unique: true,
          partialFilterExpression: { verified: true },
        }
      );

      const indexes = await collection.indexes();
      const partialIndex = indexes.find((i) => i.name === "username_1");

      assert(partialIndex);
      assert.deepStrictEqual(
        (partialIndex as { partialFilterExpression?: object })
          .partialFilterExpression,
        { verified: true }
      );
    });
  });

  describe("Partial Index Unique Constraint Scoping", () => {
    it("should enforce uniqueness only for matching documents", async () => {
      const collection = client.db(dbName).collection("partial_unique_match");
      await collection.createIndex(
        { email: 1 },
        {
          unique: true,
          partialFilterExpression: { status: "active" },
        }
      );

      await collection.insertOne({
        email: "test@test.com",
        status: "active",
      });

      // Same email with status: "active" should fail
      await assert.rejects(
        async () => {
          await collection.insertOne({
            email: "test@test.com",
            status: "active",
          });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should allow duplicate values in non-matching documents", async () => {
      const collection = client.db(dbName).collection("partial_unique_nonmatch");
      await collection.createIndex(
        { email: 1 },
        {
          unique: true,
          partialFilterExpression: { status: "active" },
        }
      );

      await collection.insertOne({
        email: "test@test.com",
        status: "active",
      });

      // Same email with different status should be allowed
      await collection.insertOne({
        email: "test@test.com",
        status: "inactive",
      });

      // Same email with missing status should be allowed
      await collection.insertOne({
        email: "test@test.com",
      });

      const count = await collection.countDocuments({ email: "test@test.com" });
      assert.strictEqual(count, 3);
    });

    it("should support $exists: true in filter", async () => {
      const collection = client.db(dbName).collection("partial_exists");
      await collection.createIndex(
        { code: 1 },
        {
          unique: true,
          partialFilterExpression: { code: { $exists: true } },
        }
      );

      await collection.insertOne({ code: "ABC" });

      // Duplicate where code exists should fail
      await assert.rejects(
        async () => {
          await collection.insertOne({ code: "ABC" });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );

      // Documents without code field are allowed (not in index)
      await collection.insertOne({ name: "no code 1" });
      await collection.insertOne({ name: "no code 2" });

      const count = await collection.countDocuments({});
      assert.strictEqual(count, 3);
    });

    it("should support comparison operators in filter", async () => {
      const collection = client.db(dbName).collection("partial_comparison");
      await collection.createIndex(
        { email: 1 },
        {
          unique: true,
          partialFilterExpression: { age: { $gte: 18 } },
        }
      );

      await collection.insertOne({ email: "adult@test.com", age: 25 });

      // Same email for another adult should fail
      await assert.rejects(
        async () => {
          await collection.insertOne({ email: "adult@test.com", age: 30 });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );

      // Same email for minor (age < 18) should be allowed
      await collection.insertOne({ email: "adult@test.com", age: 15 });

      const count = await collection.countDocuments({ email: "adult@test.com" });
      assert.strictEqual(count, 2);
    });
  });

  describe("Partial Index Restrictions", () => {
    it("should reject combining sparse and partialFilterExpression", async () => {
      const collection = client.db(dbName).collection("partial_sparse_reject");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { email: 1 },
            {
              sparse: true,
              partialFilterExpression: { status: "active" },
            }
          );
        },
        (err: Error) => {
          // MongoDB error code 67 - cannot combine sparse and partial
          assert(
            err.message.includes("sparse") ||
              err.message.includes("partial") ||
              (err as { code?: number }).code === 67
          );
          return true;
        }
      );
    });
  });

  describe("Partial Index with Updates", () => {
    it("should allow duplicate after update moves document out of filter scope", async () => {
      const collection = client.db(dbName).collection("partial_update_out");
      await collection.createIndex(
        { email: 1 },
        {
          unique: true,
          partialFilterExpression: { status: "active" },
        }
      );

      // Insert active document
      await collection.insertOne({
        email: "test@test.com",
        status: "active",
      });

      // Update to inactive - document leaves index scope
      await collection.updateOne(
        { email: "test@test.com" },
        { $set: { status: "inactive" } }
      );

      // Now we can insert another active document with same email
      await collection.insertOne({
        email: "test@test.com",
        status: "active",
      });

      const count = await collection.countDocuments({ email: "test@test.com" });
      assert.strictEqual(count, 2);
    });

    it("should reject duplicate when update moves document into filter scope", async () => {
      const collection = client.db(dbName).collection("partial_update_in");
      await collection.createIndex(
        { email: 1 },
        {
          unique: true,
          partialFilterExpression: { status: "active" },
        }
      );

      // Insert inactive document
      await collection.insertOne({
        email: "test@test.com",
        status: "inactive",
      });

      // Insert active document with same email (allowed because first is inactive)
      await collection.insertOne({
        email: "test@test.com",
        status: "active",
      });

      // Try to update the inactive one to active - should fail due to duplicate
      await assert.rejects(
        async () => {
          await collection.updateOne(
            { email: "test@test.com", status: "inactive" },
            { $set: { status: "active" } }
          );
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });
  });
});

describe(`Index Hint Tests (${getTestModeName()})`, () => {
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

  describe("Cursor Hints", () => {
    it("should accept hint by index name", async () => {
      const collection = client.db(dbName).collection("hint_name");
      await collection.createIndex({ email: 1 });
      await collection.insertOne({ email: "test@test.com", name: "Test" });

      // This should not throw - index exists
      const results = await collection
        .find({ email: "test@test.com" })
        .hint("email_1")
        .toArray();

      assert.strictEqual(results.length, 1);
    });

    it("should accept hint by key pattern", async () => {
      const collection = client.db(dbName).collection("hint_pattern");
      await collection.createIndex({ email: 1 });
      await collection.insertOne({ email: "test@test.com", name: "Test" });

      // This should not throw - index exists
      const results = await collection
        .find({ email: "test@test.com" })
        .hint({ email: 1 })
        .toArray();

      assert.strictEqual(results.length, 1);
    });

    it("should throw for non-existent index name", async () => {
      const collection = client.db(dbName).collection("hint_nonexist_name");
      await collection.insertOne({ email: "test@test.com" });

      await assert.rejects(
        async () => {
          await collection
            .find({ email: "test@test.com" })
            .hint("nonexistent_1")
            .toArray();
        },
        (err: Error) => {
          assert(
            err.message.includes("bad hint") ||
              err.message.includes("hint") ||
              err.message.includes("planner")
          );
          return true;
        }
      );
    });

    it("should throw for non-existent key pattern", async () => {
      const collection = client.db(dbName).collection("hint_nonexist_pattern");
      await collection.insertOne({ email: "test@test.com" });

      await assert.rejects(
        async () => {
          await collection
            .find({ email: "test@test.com" })
            .hint({ nonexistent: 1 })
            .toArray();
        },
        (err: Error) => {
          assert(
            err.message.includes("bad hint") ||
              err.message.includes("hint") ||
              err.message.includes("planner")
          );
          return true;
        }
      );
    });
  });

  describe("$natural Hint", () => {
    it("should support $natural: 1 (forward scan)", async () => {
      const collection = client.db(dbName).collection("hint_natural_forward");
      await collection.insertMany([
        { order: 1 },
        { order: 2 },
        { order: 3 },
      ]);

      const results = await collection
        .find({})
        .hint({ $natural: 1 })
        .toArray();

      assert.strictEqual(results.length, 3);
      // Forward scan should return in insertion order
      assert.strictEqual((results[0] as { order: number }).order, 1);
      assert.strictEqual((results[2] as { order: number }).order, 3);
    });

    it("should support $natural: -1 (reverse scan)", async () => {
      const collection = client.db(dbName).collection("hint_natural_reverse");
      await collection.insertMany([
        { order: 1 },
        { order: 2 },
        { order: 3 },
      ]);

      const results = await collection
        .find({})
        .hint({ $natural: -1 })
        .toArray();

      assert.strictEqual(results.length, 3);
      // Reverse scan should return in reverse insertion order
      assert.strictEqual((results[0] as { order: number }).order, 3);
      assert.strictEqual((results[2] as { order: number }).order, 1);
    });
  });

  describe("Hint with Other Cursor Methods", () => {
    it("should work with sort, limit, and skip", async () => {
      const collection = client.db(dbName).collection("hint_combined");
      await collection.createIndex({ value: 1 });
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      const results = await collection
        .find({})
        .hint({ value: 1 })
        .sort({ value: -1 })
        .skip(1)
        .limit(2)
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual((results[0] as { value: number }).value, 4);
      assert.strictEqual((results[1] as { value: number }).value, 3);
    });
  });
});
