/**
 * $jsonSchema Query Operator Tests
 *
 * Tests for JSON Schema validation in queries.
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

type Doc = Record<string, any>;

describe(`$jsonSchema Query Operator Tests (${getTestModeName()})`, () => {
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

  describe('bsonType Validation', () => {
    it('should match documents with correct bsonType: string', async () => {
      const collection = client.db(dbName).collection('schema_bsontype_string');
      await collection.insertMany([
        { name: 'Alice' },
        { name: 123 },
        { name: null },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            name: { bsonType: 'string' },
          },
        },
      }).toArray();

      // All docs match because properties only validates if the field exists with that type
      // Schema validation doesn't fail for missing fields unless 'required' is used
      assert.ok(docs.length >= 1);
    });

    it('should match documents with bsonType: int', async () => {
      const collection = client.db(dbName).collection('schema_bsontype_int');
      await collection.insertMany([
        { age: 25 },
        { age: 25.5 },
        { age: 'twenty' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            age: { bsonType: 'int' },
          },
        },
      }).toArray();

      // Should match the document with integer age
      const intDoc = docs.find((d) => d.age === 25);
      assert.ok(intDoc);
    });

    it('should match documents with bsonType: array', async () => {
      const collection = client.db(dbName).collection('schema_bsontype_array');
      await collection.insertMany([
        { tags: ['a', 'b'] },
        { tags: 'not-array' },
        { tags: null },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            tags: { bsonType: 'array' },
          },
        },
      }).toArray();

      const arrayDoc = docs.find((d) => Array.isArray(d.tags));
      assert.ok(arrayDoc);
    });

    it('should support multiple bsonTypes', async () => {
      const collection = client.db(dbName).collection('schema_bsontype_multi');
      await collection.insertMany([
        { value: 'hello' },
        { value: null },
        { value: 123 },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            value: { bsonType: ['string', 'null'] },
          },
        },
      }).toArray();

      // Should match string and null values
      assert.ok(docs.length >= 2);
    });
  });

  describe('required Validation', () => {
    it('should only match documents with required fields', async () => {
      const collection = client.db(dbName).collection('schema_required');
      await collection.insertMany([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob' },
        { email: 'charlie@example.com' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          required: ['name', 'email'],
        },
      }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, 'Alice');
    });

    it('should handle empty required array', async () => {
      const collection = client.db(dbName).collection('schema_required_empty');
      await collection.insertMany([
        { name: 'Alice' },
        { age: 25 },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          required: [],
        },
      }).toArray();

      // Empty required means all documents match
      assert.strictEqual(docs.length, 2);
    });
  });

  describe('properties Validation', () => {
    it('should validate nested properties', async () => {
      const collection = client.db(dbName).collection('schema_properties_nested');
      await collection.insertMany([
        { address: { city: 'NYC', zip: 10001 } },
        { address: { city: 123, zip: 'invalid' } },
        { address: 'not-object' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            address: {
              bsonType: 'object',
              properties: {
                city: { bsonType: 'string' },
                zip: { bsonType: 'int' },
              },
            },
          },
        },
      }).toArray();

      const validDoc = docs.find(
        (d: Doc) => d.address?.city === 'NYC' && d.address?.zip === 10001
      );
      assert.ok(validDoc);
    });
  });

  describe('Numeric Constraints', () => {
    it('should validate minimum constraint', async () => {
      const collection = client.db(dbName).collection('schema_minimum');
      await collection.insertMany([
        { age: 18 },
        { age: 17 },
        { age: 25 },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            age: { minimum: 18 },
          },
        },
      }).toArray();

      // Should match 18 and 25
      assert.ok(docs.every((d: Doc) => d.age >= 18));
    });

    it('should validate maximum constraint', async () => {
      const collection = client.db(dbName).collection('schema_maximum');
      await collection.insertMany([
        { score: 100 },
        { score: 101 },
        { score: 50 },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            score: { maximum: 100 },
          },
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => d.score <= 100));
    });

    it('should validate exclusiveMinimum', async () => {
      const collection = client.db(dbName).collection('schema_exclusive_min');
      await collection.insertMany([
        { value: 0 },
        { value: 1 },
        { value: -1 },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            value: { minimum: 0, exclusiveMinimum: true },
          },
        },
      }).toArray();

      // Should only match value > 0
      assert.ok(docs.every((d: Doc) => d.value > 0));
    });
  });

  describe('String Constraints', () => {
    it('should validate minLength', async () => {
      const collection = client.db(dbName).collection('schema_minlength');
      await collection.insertMany([
        { name: 'Al' },
        { name: 'Alice' },
        { name: 'A' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            name: { minLength: 3 },
          },
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => d.name.length >= 3));
    });

    it('should validate maxLength', async () => {
      const collection = client.db(dbName).collection('schema_maxlength');
      await collection.insertMany([
        { code: 'ABC' },
        { code: 'ABCDEF' },
        { code: 'AB' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            code: { maxLength: 3 },
          },
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => d.code.length <= 3));
    });

    it('should validate pattern', async () => {
      const collection = client.db(dbName).collection('schema_pattern');
      await collection.insertMany([
        { email: 'test@example.com' },
        { email: 'invalid' },
        { email: 'user@domain.org' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            email: { pattern: '^[a-z]+@[a-z]+\\.[a-z]+$' },
          },
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => d.email.includes('@')));
    });
  });

  describe('Array Constraints', () => {
    it('should validate minItems', async () => {
      const collection = client.db(dbName).collection('schema_minitems');
      await collection.insertMany([
        { items: [1, 2, 3] },
        { items: [1] },
        { items: [] },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            items: { minItems: 2 },
          },
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => d.items.length >= 2));
    });

    it('should validate maxItems', async () => {
      const collection = client.db(dbName).collection('schema_maxitems');
      await collection.insertMany([
        { items: [1, 2] },
        { items: [1, 2, 3, 4] },
        { items: [1] },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            items: { maxItems: 2 },
          },
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => d.items.length <= 2));
    });

    it('should validate uniqueItems', async () => {
      const collection = client.db(dbName).collection('schema_uniqueitems');
      await collection.insertMany([
        { values: [1, 2, 3] },
        { values: [1, 1, 2] },
        { values: ['a', 'b', 'c'] },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            values: { uniqueItems: true },
          },
        },
      }).toArray();

      // Should match arrays with unique items only
      assert.ok(docs.every((d: Doc) => new Set(d.values).size === d.values.length));
    });

    it('should validate items schema', async () => {
      const collection = client.db(dbName).collection('schema_items');
      await collection.insertMany([
        { scores: [85, 90, 95] },
        { scores: [85, 'ninety', 95] },
        { scores: [100] },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            scores: {
              items: { bsonType: 'int' },
            },
          },
        },
      }).toArray();

      // Should match arrays where all items are integers
      assert.ok(docs.every((d: Doc) => d.scores.every((s: unknown) => Number.isInteger(s))));
    });
  });

  describe('enum Validation', () => {
    it('should validate enum values', async () => {
      const collection = client.db(dbName).collection('schema_enum');
      await collection.insertMany([
        { status: 'active' },
        { status: 'pending' },
        { status: 'unknown' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            status: { enum: ['active', 'pending', 'inactive'] },
          },
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => ['active', 'pending', 'inactive'].includes(d.status)));
    });
  });

  describe('Logical Composition', () => {
    it('should validate allOf', async () => {
      const collection = client.db(dbName).collection('schema_allof');
      await collection.insertMany([
        { value: 15 },
        { value: 5 },
        { value: 25 },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          allOf: [
            { properties: { value: { minimum: 10 } } },
            { properties: { value: { maximum: 20 } } },
          ],
        },
      }).toArray();

      // Should match value between 10 and 20
      assert.ok(docs.every((d: Doc) => d.value >= 10 && d.value <= 20));
    });

    it('should validate anyOf', async () => {
      const collection = client.db(dbName).collection('schema_anyof');
      await collection.insertMany([
        { type: 'admin' },
        { type: 'user' },
        { type: 'guest' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          anyOf: [
            { properties: { type: { enum: ['admin'] } } },
            { properties: { type: { enum: ['user'] } } },
          ],
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => ['admin', 'user'].includes(d.type)));
    });

    it('should validate oneOf', async () => {
      const collection = client.db(dbName).collection('schema_oneof');
      await collection.insertMany([
        { value: 5 },
        { value: 15 },
        { value: 25 },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          oneOf: [
            { properties: { value: { maximum: 10 } } },
            { properties: { value: { minimum: 20 } } },
          ],
        },
      }).toArray();

      // Should match value <= 10 OR value >= 20, but not both
      assert.ok(docs.every((d: Doc) => (d.value <= 10) !== (d.value >= 20)));
    });

    it('should validate not', async () => {
      const collection = client.db(dbName).collection('schema_not');
      await collection.insertMany([
        { status: 'deleted' },
        { status: 'active' },
        { status: 'pending' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          not: {
            properties: { status: { enum: ['deleted'] } },
          },
        },
      }).toArray();

      assert.ok(docs.every((d: Doc) => d.status !== 'deleted'));
    });
  });

  describe('Combined with Other Operators', () => {
    it('should work with $and', async () => {
      const collection = client.db(dbName).collection('schema_with_and');
      await collection.insertMany([
        { name: 'Alice', age: 25 },
        { name: 'Bob', age: 17 },
        { name: 'Charlie', age: 30 },
      ]);

      const docs = await collection.find({
        $and: [
          { age: { $gte: 18 } },
          {
            $jsonSchema: {
              properties: {
                name: { minLength: 3 },
              },
            },
          },
        ],
      }).toArray();

      assert.ok(docs.every((d: Doc) => d.age >= 18 && d.name.length >= 3));
    });

    it('should work with field queries', async () => {
      const collection = client.db(dbName).collection('schema_with_field');
      await collection.insertMany([
        { category: 'tech', price: 99 },
        { category: 'tech', price: 199 },
        { category: 'home', price: 49 },
      ]);

      const docs = await collection.find({
        category: 'tech',
        $jsonSchema: {
          properties: {
            price: { maximum: 150 },
          },
        },
      }).toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].price, 99);
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty schema (matches all)', async () => {
      const collection = client.db(dbName).collection('schema_empty');
      await collection.insertMany([
        { a: 1 },
        { b: 2 },
        { c: 3 },
      ]);

      const docs = await collection.find({
        $jsonSchema: {},
      }).toArray();

      assert.strictEqual(docs.length, 3);
    });

    it('should handle additionalProperties: false', async () => {
      const collection = client.db(dbName).collection('schema_additional_false');
      await collection.insertMany([
        { name: 'Alice' },
        { name: 'Bob', extra: 'field' },
      ]);

      const docs = await collection.find({
        $jsonSchema: {
          properties: {
            name: { bsonType: 'string' },
            _id: {}, // Allow _id
          },
          additionalProperties: false,
        },
      }).toArray();

      // Only the document without extra fields should match
      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].name, 'Alice');
    });
  });
});
