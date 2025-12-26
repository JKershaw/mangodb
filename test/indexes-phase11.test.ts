/**
 * Phase 11: Index Types & Options Tests
 *
 * Tests for hashed indexes, wildcard indexes, collation, hidden option,
 * text index weights, and default_language.
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "./test-harness.ts";

describe(`Hashed Index Tests (${getTestModeName()})`, () => {
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

  describe("Basic Hashed Index Creation", () => {
    it("should create a hashed index", async () => {
      const collection = client.db(dbName).collection("hashed_basic");
      const indexName = await collection.createIndex({ userId: "hashed" });

      assert.strictEqual(indexName, "userId_hashed");

      const indexes = await collection.indexes();
      const hashedIndex = indexes.find((i) => i.name === "userId_hashed");
      assert(hashedIndex);
      assert.deepStrictEqual(hashedIndex.key, { userId: "hashed" });
    });

    it("should generate correct index name for hashed index", async () => {
      const collection = client.db(dbName).collection("hashed_name");
      const indexName = await collection.createIndex({ field: "hashed" });

      assert.strictEqual(indexName, "field_hashed");
    });

    it("should allow custom name for hashed index", async () => {
      const collection = client.db(dbName).collection("hashed_custom_name");
      const indexName = await collection.createIndex(
        { data: "hashed" },
        { name: "my_hash_idx" }
      );

      assert.strictEqual(indexName, "my_hash_idx");
    });
  });

  describe("Hashed Index Restrictions", () => {
    it("should reject unique constraint on hashed index", async () => {
      const collection = client.db(dbName).collection("hashed_no_unique");

      await assert.rejects(
        async () => {
          await collection.createIndex({ field: "hashed" }, { unique: true });
        },
        (err: Error & { code?: number }) => {
          assert(err.message.includes("hashed") || err.message.includes("unique"));
          return true;
        }
      );
    });

    it("should reject multiple hashed fields in one index", async () => {
      const collection = client.db(dbName).collection("hashed_multi");

      await assert.rejects(
        async () => {
          await collection.createIndex({ a: "hashed", b: "hashed" });
        },
        (err: Error) => {
          assert(err.message.includes("hashed") || err.message.includes("one"));
          return true;
        }
      );
    });
  });

  describe("Hashed Index with Data", () => {
    it("should allow inserting documents with hashed index", async () => {
      const collection = client.db(dbName).collection("hashed_insert");
      await collection.createIndex({ key: "hashed" });

      await collection.insertOne({ key: "value1" });
      await collection.insertOne({ key: "value2" });
      await collection.insertOne({ key: 12345 });

      const count = await collection.countDocuments({});
      assert.strictEqual(count, 3);
    });

    it("should handle null values in hashed index", async () => {
      const collection = client.db(dbName).collection("hashed_null");
      await collection.createIndex({ key: "hashed" });

      await collection.insertOne({ key: null });
      await collection.insertOne({ other: "data" }); // missing key

      const count = await collection.countDocuments({});
      assert.strictEqual(count, 2);
    });
  });
});

describe(`Wildcard Index Tests (${getTestModeName()})`, () => {
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

  describe("Basic Wildcard Index Creation", () => {
    it("should create a wildcard index on all fields", async () => {
      const collection = client.db(dbName).collection("wildcard_all");
      const indexName = await collection.createIndex({ "$**": 1 });

      assert.strictEqual(indexName, "$**_1");

      const indexes = await collection.indexes();
      const wildcardIndex = indexes.find((i) => i.name === "$**_1");
      assert(wildcardIndex);
      assert.deepStrictEqual(wildcardIndex.key, { "$**": 1 });
    });

    it("should create wildcard index on path prefix", async () => {
      const collection = client.db(dbName).collection("wildcard_path");
      const indexName = await collection.createIndex({ "data.$**": 1 });

      assert.strictEqual(indexName, "data.$**_1");
    });
  });

  describe("Wildcard Index Restrictions", () => {
    it("should reject unique constraint on wildcard index", async () => {
      const collection = client.db(dbName).collection("wildcard_no_unique");

      await assert.rejects(
        async () => {
          await collection.createIndex({ "$**": 1 }, { unique: true });
        },
        (err: Error) => {
          assert(err.message.includes("wildcard") || err.message.includes("unique"));
          return true;
        }
      );
    });

    it("should reject compound wildcard index", async () => {
      const collection = client.db(dbName).collection("wildcard_no_compound");

      await assert.rejects(
        async () => {
          await collection.createIndex({ "$**": 1, other: 1 });
        },
        (err: Error) => {
          assert(err.message.includes("wildcard") || err.message.includes("compound"));
          return true;
        }
      );
    });
  });

  describe("Wildcard Index with wildcardProjection", () => {
    it("should store wildcardProjection in metadata (inclusion)", async () => {
      const collection = client.db(dbName).collection("wildcard_proj_include");
      await collection.createIndex(
        { "$**": 1 },
        { wildcardProjection: { name: 1, email: 1 } }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.name === "$**_1");
      assert(idx);
      assert.deepStrictEqual(idx.wildcardProjection, { name: 1, email: 1 });
    });

    it("should store wildcardProjection in metadata (exclusion)", async () => {
      const collection = client.db(dbName).collection("wildcard_proj_exclude");
      await collection.createIndex(
        { "$**": 1 },
        { wildcardProjection: { password: 0, secret: 0 } }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.name === "$**_1");
      assert(idx);
      assert.deepStrictEqual(idx.wildcardProjection, { password: 0, secret: 0 });
    });

    it("should reject mixed inclusion/exclusion in wildcardProjection", async () => {
      const collection = client.db(dbName).collection("wildcard_proj_mixed");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { "$**": 1 },
            { wildcardProjection: { name: 1, password: 0 } }
          );
        },
        (err: Error) => {
          assert(err.message.includes("wildcardProjection") || err.message.includes("mix"));
          return true;
        }
      );
    });

    it("should allow _id with different inclusion/exclusion", async () => {
      const collection = client.db(dbName).collection("wildcard_proj_id");
      // Including _id with exclusion of other fields is allowed
      await collection.createIndex(
        { "$**": 1 },
        { wildcardProjection: { _id: 1, password: 0 } }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.name === "$**_1");
      assert(idx);
    });
  });

  describe("Wildcard Index Implicit Sparse", () => {
    it("should set wildcard index as implicitly sparse", async () => {
      const collection = client.db(dbName).collection("wildcard_sparse");
      await collection.createIndex({ "$**": 1 });

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.name === "$**_1");
      assert(idx);
      assert.strictEqual(idx.sparse, true);
    });
  });
});

describe(`Hidden Index Tests (${getTestModeName()})`, () => {
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

  describe("Basic Hidden Index", () => {
    it("should create a hidden index", async () => {
      const collection = client.db(dbName).collection("hidden_basic");
      await collection.createIndex({ field: 1 }, { hidden: true });

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.name === "field_1");
      assert(idx);
      assert.strictEqual(idx.hidden, true);
    });

    it("should create non-hidden index by default", async () => {
      const collection = client.db(dbName).collection("hidden_default");
      await collection.createIndex({ field: 1 });

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.name === "field_1");
      assert(idx);
      // hidden should be undefined or false
      assert(idx.hidden === undefined || idx.hidden === false);
    });
  });

  describe("Hidden Index Restrictions", () => {
    it("should reject hiding _id index", async () => {
      const collection = client.db(dbName).collection("hidden_no_id");

      await assert.rejects(
        async () => {
          await collection.createIndex({ _id: 1 }, { hidden: true });
        },
        (err: Error) => {
          assert(err.message.includes("_id") || err.message.includes("hide"));
          return true;
        }
      );
    });
  });

  describe("Hidden Index with Unique Constraint", () => {
    it("should still enforce unique constraint on hidden index", async () => {
      const collection = client.db(dbName).collection("hidden_unique");
      await collection.createIndex({ email: 1 }, { unique: true, hidden: true });

      await collection.insertOne({ email: "test@example.com" });

      await assert.rejects(
        async () => {
          await collection.insertOne({ email: "test@example.com" });
        },
        (err: Error & { code?: number }) => {
          assert.strictEqual(err.code, 11000);
          return true;
        }
      );
    });
  });
});

describe(`Collation Index Tests (${getTestModeName()})`, () => {
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

  describe("Basic Collation Index", () => {
    it("should create index with collation", async () => {
      const collection = client.db(dbName).collection("collation_basic");
      await collection.createIndex(
        { name: 1 },
        { collation: { locale: "en" } }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.name === "name_1");
      assert(idx);
      assert(idx.collation);
      assert.strictEqual(idx.collation.locale, "en");
    });

    it("should store full collation options", async () => {
      const collection = client.db(dbName).collection("collation_full");
      await collection.createIndex(
        { title: 1 },
        {
          collation: {
            locale: "en_US",
            strength: 2,
            caseLevel: true,
            caseFirst: "upper",
          },
        }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.name === "title_1");
      assert(idx);
      assert(idx.collation);
      assert.strictEqual(idx.collation.locale, "en_US");
      assert.strictEqual(idx.collation.strength, 2);
      assert.strictEqual(idx.collation.caseLevel, true);
      assert.strictEqual(idx.collation.caseFirst, "upper");
    });
  });

  describe("Collation Index Restrictions", () => {
    it("should require locale in collation", async () => {
      const collection = client.db(dbName).collection("collation_no_locale");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { field: 1 },
            { collation: { locale: "" } }
          );
        },
        (err: Error) => {
          assert(err.message.includes("locale") || err.message.includes("required"));
          return true;
        }
      );
    });

    it("should reject collation on text index", async () => {
      const collection = client.db(dbName).collection("collation_no_text");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { content: "text" },
            { collation: { locale: "en" } }
          );
        },
        (err: Error) => {
          assert(err.message.includes("text") || err.message.includes("collation"));
          return true;
        }
      );
    });
  });
});

describe(`Text Index Options Tests (${getTestModeName()})`, () => {
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

  describe("Text Index Weights", () => {
    it("should create text index with weights", async () => {
      const collection = client.db(dbName).collection("text_weights_basic");
      await collection.createIndex(
        { title: "text", body: "text" },
        { weights: { title: 10, body: 1 } }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.key.title === "text");
      assert(idx);
      assert(idx.weights);
      assert.strictEqual(idx.weights.title, 10);
      assert.strictEqual(idx.weights.body, 1);
    });

    it("should store textIndexVersion in metadata", async () => {
      const collection = client.db(dbName).collection("text_version");
      await collection.createIndex(
        { content: "text" },
        { weights: { content: 5 } }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.key.content === "text");
      assert(idx);
      assert.strictEqual(idx.textIndexVersion, 3);
    });
  });

  describe("Text Index Weights Validation", () => {
    it("should reject weights without text index", async () => {
      const collection = client.db(dbName).collection("weights_no_text");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { field: 1 },
            { weights: { field: 5 } }
          );
        },
        (err: Error) => {
          assert(err.message.includes("weights") || err.message.includes("text"));
          return true;
        }
      );
    });

    it("should reject weight less than 1", async () => {
      const collection = client.db(dbName).collection("weights_too_low");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { content: "text" },
            { weights: { content: 0 } }
          );
        },
        (err: Error) => {
          assert(err.message.includes("weight") || err.message.includes("1"));
          return true;
        }
      );
    });

    it("should reject weight greater than 99999", async () => {
      const collection = client.db(dbName).collection("weights_too_high");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { content: "text" },
            { weights: { content: 100000 } }
          );
        },
        (err: Error) => {
          assert(err.message.includes("weight") || err.message.includes("99999"));
          return true;
        }
      );
    });

    it("should reject non-integer weight", async () => {
      const collection = client.db(dbName).collection("weights_float");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { content: "text" },
            { weights: { content: 5.5 } }
          );
        },
        (err: Error) => {
          assert(err.message.includes("weight") || err.message.includes("integer"));
          return true;
        }
      );
    });
  });

  describe("default_language Option", () => {
    it("should create text index with default_language", async () => {
      const collection = client.db(dbName).collection("text_lang_basic");
      await collection.createIndex(
        { content: "text" },
        { default_language: "spanish" }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.key.content === "text");
      assert(idx);
      assert.strictEqual(idx.default_language, "spanish");
    });

    it("should reject default_language without text index", async () => {
      const collection = client.db(dbName).collection("lang_no_text");

      await assert.rejects(
        async () => {
          await collection.createIndex(
            { field: 1 },
            { default_language: "french" }
          );
        },
        (err: Error) => {
          assert(err.message.includes("default_language") || err.message.includes("text"));
          return true;
        }
      );
    });

    it("should combine weights and default_language", async () => {
      const collection = client.db(dbName).collection("text_combined");
      await collection.createIndex(
        { title: "text", body: "text" },
        {
          weights: { title: 10, body: 1 },
          default_language: "german",
        }
      );

      const indexes = await collection.indexes();
      const idx = indexes.find((i) => i.key.title === "text");
      assert(idx);
      assert.strictEqual(idx.weights?.title, 10);
      assert.strictEqual(idx.default_language, "german");
      assert.strictEqual(idx.textIndexVersion, 3);
    });
  });
});
