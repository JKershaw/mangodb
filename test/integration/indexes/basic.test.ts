/**
 * Basic Index Management Tests
 *
 * Tests for index creation, deletion, listing, and unique constraints.
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "../../test-harness.ts";

describe(`Index Management Tests (${getTestModeName()})`, () => {
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

  describe("createIndex", () => {
    it("should create ascending index and return name", async () => {
      const collection = client.db(dbName).collection("idx_create_asc");

      const name = await collection.createIndex({ email: 1 });

      assert.strictEqual(name, "email_1");
    });

    it("should create descending index and return name", async () => {
      const collection = client.db(dbName).collection("idx_create_desc");

      const name = await collection.createIndex({ createdAt: -1 });

      assert.strictEqual(name, "createdAt_-1");
    });

    it("should create compound index", async () => {
      const collection = client.db(dbName).collection("idx_compound");

      const name = await collection.createIndex({ lastName: 1, firstName: 1 });

      assert.strictEqual(name, "lastName_1_firstName_1");
    });

    it("should create index with custom name", async () => {
      const collection = client.db(dbName).collection("idx_custom_name");

      const name = await collection.createIndex(
        { email: 1 },
        { name: "idx_email" }
      );

      assert.strictEqual(name, "idx_email");
    });

    it("should be idempotent for same spec", async () => {
      const collection = client.db(dbName).collection("idx_idempotent");

      const name1 = await collection.createIndex({ email: 1 });
      const name2 = await collection.createIndex({ email: 1 });

      assert.strictEqual(name1, name2);
      assert.strictEqual(name1, "email_1");

      // Should still only have 2 indexes (default _id + email)
      const indexes = await collection.indexes();
      const emailIndexes = indexes.filter((i) => i.name === "email_1");
      assert.strictEqual(emailIndexes.length, 1);
    });

    it("should create index on nested field", async () => {
      const collection = client.db(dbName).collection("idx_nested");

      const name = await collection.createIndex({ "address.city": 1 });

      assert.strictEqual(name, "address.city_1");
    });
  });

  describe("dropIndex", () => {
    it("should drop index by name", async () => {
      const collection = client.db(dbName).collection("idx_drop_name");
      await collection.createIndex({ email: 1 });

      await collection.dropIndex("email_1");

      const indexes = await collection.indexes();
      const emailIndex = indexes.find((i) => i.name === "email_1");
      assert.strictEqual(emailIndex, undefined);
    });

    it("should drop index by key spec", async () => {
      const collection = client.db(dbName).collection("idx_drop_spec");
      await collection.createIndex({ email: 1 });

      await collection.dropIndex({ email: 1 });

      const indexes = await collection.indexes();
      const emailIndex = indexes.find((i) => i.name === "email_1");
      assert.strictEqual(emailIndex, undefined);
    });

    it("should throw for non-existent index", async () => {
      const collection = client.db(dbName).collection("idx_drop_nonexist");
      // Ensure collection exists first (MongoDB throws "ns not found" otherwise)
      await collection.insertOne({ test: true });
      await collection.deleteMany({});

      await assert.rejects(
        async () => {
          await collection.dropIndex("nonexistent_1");
        },
        (err: Error) => {
          // Both MongoDB and MangoDB use "index not found with name [...]"
          assert(
            err.message.includes("index not found with name"),
            `Expected "index not found with name" error, got: ${err.message}`
          );
          return true;
        }
      );
    });

    it("should throw when dropping _id index by name", async () => {
      const collection = client.db(dbName).collection("idx_drop_id_name");
      // Ensure collection exists first (MongoDB throws "ns not found" otherwise)
      await collection.insertOne({ test: true });
      await collection.deleteMany({});

      await assert.rejects(
        async () => {
          await collection.dropIndex("_id_");
        },
        (err: Error) => {
          // Both MongoDB and MangoDB use "cannot drop _id index"
          assert(
            err.message.includes("cannot drop _id index"),
            `Expected "cannot drop _id index" error, got: ${err.message}`
          );
          return true;
        }
      );
    });

    it("should throw when dropping _id index by spec", async () => {
      const collection = client.db(dbName).collection("idx_drop_id_spec");
      // Ensure collection exists first (MongoDB throws "ns not found" otherwise)
      await collection.insertOne({ test: true });
      await collection.deleteMany({});

      await assert.rejects(
        async () => {
          await collection.dropIndex({ _id: 1 });
        },
        (err: Error) => {
          // Both MongoDB and MangoDB use "cannot drop _id index"
          assert(
            err.message.includes("cannot drop _id index"),
            `Expected "cannot drop _id index" error, got: ${err.message}`
          );
          return true;
        }
      );
    });
  });

  describe("indexes / listIndexes", () => {
    it("should return _id index for empty collection", async () => {
      const collection = client.db(dbName).collection("idx_list_empty");
      // Force collection to exist by doing any operation
      await collection.insertOne({ test: true });
      await collection.deleteMany({});

      const indexes = await collection.indexes();

      assert.strictEqual(indexes.length, 1);
      assert.deepStrictEqual(indexes[0].key, { _id: 1 });
      assert.strictEqual(indexes[0].name, "_id_");
    });

    it("should list all created indexes", async () => {
      const collection = client.db(dbName).collection("idx_list_all");
      await collection.createIndex({ email: 1 });
      await collection.createIndex({ createdAt: -1 });

      const indexes = await collection.indexes();

      assert.strictEqual(indexes.length, 3); // _id + email + createdAt
      const names = indexes.map((i) => i.name).sort();
      assert.deepStrictEqual(names, ["_id_", "createdAt_-1", "email_1"]);
    });

    it("should include unique flag in index info", async () => {
      const collection = client.db(dbName).collection("idx_list_unique");
      await collection.createIndex({ email: 1 }, { unique: true });

      const indexes = await collection.indexes();
      const emailIndex = indexes.find((i) => i.name === "email_1");

      assert(emailIndex);
      assert.strictEqual(emailIndex.unique, true);
    });

    it("listIndexes should return cursor with toArray", async () => {
      const collection = client.db(dbName).collection("idx_cursor");
      await collection.createIndex({ email: 1 });

      const cursor = collection.listIndexes();
      const indexes = await cursor.toArray();

      assert.strictEqual(indexes.length, 2); // _id + email
    });
  });
});

describe(`Unique Constraint Tests (${getTestModeName()})`, () => {
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

  describe("insertOne", () => {
    it("should reject duplicate on unique index", async () => {
      const collection = client.db(dbName).collection("uniq_insert_dup");
      await collection.createIndex({ email: 1 }, { unique: true });
      await collection.insertOne({ email: "alice@test.com" });

      await assert.rejects(
        async () => {
          await collection.insertOne({ email: "alice@test.com" });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          assert(err.message.includes("E11000"));
          assert(err.message.includes("duplicate key"));
          return true;
        }
      );
    });

    it("should allow same value on non-unique index", async () => {
      const collection = client.db(dbName).collection("uniq_insert_nonuniq");
      await collection.createIndex({ category: 1 }); // Not unique
      await collection.insertOne({ category: "books" });

      // Should not throw
      await collection.insertOne({ category: "books" });

      const count = await collection.countDocuments({ category: "books" });
      assert.strictEqual(count, 2);
    });

    it("should include index name in error message", async () => {
      const collection = client.db(dbName).collection("uniq_insert_errmsg");
      await collection.createIndex({ email: 1 }, { unique: true });
      await collection.insertOne({ email: "test@test.com" });

      await assert.rejects(
        async () => {
          await collection.insertOne({ email: "test@test.com" });
        },
        (err: Error) => {
          assert(err.message.includes("email_1"));
          return true;
        }
      );
    });
  });

  describe("insertMany", () => {
    it("should reject duplicate within batch", async () => {
      const collection = client.db(dbName).collection("uniq_many_batch");
      await collection.createIndex({ email: 1 }, { unique: true });

      await assert.rejects(
        async () => {
          await collection.insertMany([
            { email: "a@test.com" },
            { email: "b@test.com" },
            { email: "a@test.com" }, // duplicate within batch
          ]);
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should reject duplicate with existing document", async () => {
      const collection = client.db(dbName).collection("uniq_many_existing");
      await collection.createIndex({ email: 1 }, { unique: true });
      await collection.insertOne({ email: "exists@test.com" });

      await assert.rejects(
        async () => {
          await collection.insertMany([
            { email: "new@test.com" },
            { email: "exists@test.com" }, // duplicate with existing
          ]);
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });
  });

  describe("updateOne", () => {
    it("should reject update creating duplicate", async () => {
      const collection = client.db(dbName).collection("uniq_update_dup");
      await collection.createIndex({ email: 1 }, { unique: true });
      await collection.insertOne({ name: "Alice", email: "alice@test.com" });
      await collection.insertOne({ name: "Bob", email: "bob@test.com" });

      await assert.rejects(
        async () => {
          await collection.updateOne(
            { name: "Bob" },
            { $set: { email: "alice@test.com" } }
          );
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should allow update not affecting unique field", async () => {
      const collection = client.db(dbName).collection("uniq_update_other");
      await collection.createIndex({ email: 1 }, { unique: true });
      await collection.insertOne({ name: "Alice", email: "alice@test.com" });

      // Should not throw - updating non-unique field
      await collection.updateOne(
        { email: "alice@test.com" },
        { $set: { name: "Alicia" } }
      );

      const doc = await collection.findOne({ email: "alice@test.com" });
      assert.strictEqual((doc as { name: string }).name, "Alicia");
    });

    it("should enforce unique on upsert", async () => {
      const collection = client.db(dbName).collection("uniq_update_upsert");
      await collection.createIndex({ email: 1 }, { unique: true });
      await collection.insertOne({ email: "exists@test.com" });

      await assert.rejects(
        async () => {
          await collection.updateOne(
            { name: "New" },
            { $set: { email: "exists@test.com" } },
            { upsert: true }
          );
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should allow updating same document with same unique value", async () => {
      const collection = client.db(dbName).collection("uniq_update_same");
      await collection.createIndex({ email: 1 }, { unique: true });
      await collection.insertOne({
        name: "Alice",
        email: "alice@test.com",
        age: 25,
      });

      // Should not throw - same document, same email
      await collection.updateOne(
        { email: "alice@test.com" },
        { $set: { age: 26 } }
      );

      const doc = await collection.findOne({ email: "alice@test.com" });
      assert.strictEqual((doc as { age: number }).age, 26);
    });
  });

  describe("updateMany", () => {
    it("should reject batch update creating duplicates", async () => {
      const collection = client.db(dbName).collection("uniq_updmany_dup");
      await collection.createIndex({ code: 1 }, { unique: true });
      await collection.insertMany([
        { name: "A", code: 1 },
        { name: "B", code: 2 },
        { name: "C", code: 3 },
      ]);

      await assert.rejects(
        async () => {
          // Try to set all codes to 1, which would create duplicates
          await collection.updateMany(
            { code: { $gt: 1 } },
            { $set: { code: 1 } }
          );
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });
  });

  describe("Edge Cases", () => {
    it("should enforce unique on nested fields", async () => {
      const collection = client.db(dbName).collection("uniq_nested");
      await collection.createIndex({ "user.email": 1 }, { unique: true });
      await collection.insertOne({ user: { email: "a@test.com" } });

      await assert.rejects(
        async () => {
          await collection.insertOne({ user: { email: "a@test.com" } });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should enforce unique on compound indexes", async () => {
      const collection = client.db(dbName).collection("uniq_compound");
      await collection.createIndex(
        { firstName: 1, lastName: 1 },
        { unique: true }
      );
      await collection.insertOne({ firstName: "John", lastName: "Doe" });
      await collection.insertOne({ firstName: "John", lastName: "Smith" }); // OK - different combo

      await assert.rejects(
        async () => {
          await collection.insertOne({ firstName: "John", lastName: "Doe" });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });

    it("should have correct error code (11000)", async () => {
      const collection = client.db(dbName).collection("uniq_errcode");
      await collection.createIndex({ email: 1 }, { unique: true });
      await collection.insertOne({ email: "test@test.com" });

      try {
        await collection.insertOne({ email: "test@test.com" });
        assert.fail("Expected error to be thrown");
      } catch (err) {
        assert.strictEqual((err as { code: number }).code, 11000);
      }
    });
  });
});
