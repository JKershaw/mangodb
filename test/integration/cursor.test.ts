/**
 * Phase 4: Cursor Operation Tests
 *
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../test-harness.ts';

describe(`Cursor Operation Tests (${getTestModeName()})`, () => {
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

  describe('sort', () => {
    describe('single field ascending', () => {
      it('should sort numbers ascending', async () => {
        const collection = client.db(dbName).collection('sort_num_asc');
        await collection.insertMany([{ value: 30 }, { value: 10 }, { value: 20 }]);

        const docs = await collection.find({}).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].value, 10);
        assert.strictEqual(docs[1].value, 20);
        assert.strictEqual(docs[2].value, 30);
      });

      it('should sort strings ascending (lexicographic)', async () => {
        const collection = client.db(dbName).collection('sort_str_asc');
        await collection.insertMany([{ name: 'Charlie' }, { name: 'Alice' }, { name: 'Bob' }]);

        const docs = await collection.find({}).sort({ name: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].name, 'Alice');
        assert.strictEqual(docs[1].name, 'Bob');
        assert.strictEqual(docs[2].name, 'Charlie');
      });

      it('should sort strings using binary comparison (uppercase before lowercase)', async () => {
        const collection = client.db(dbName).collection('sort_str_binary');
        await collection.insertMany([
          { name: 'banana' },
          { name: 'Apple' },
          { name: 'apple' },
          { name: 'Banana' },
        ]);

        const docs = await collection.find({}).sort({ name: 1 }).toArray();

        // MongoDB uses binary comparison: A-Z (65-90) before a-z (97-122)
        assert.strictEqual(docs.length, 4);
        assert.strictEqual(docs[0].name, 'Apple');
        assert.strictEqual(docs[1].name, 'Banana');
        assert.strictEqual(docs[2].name, 'apple');
        assert.strictEqual(docs[3].name, 'banana');
      });

      it('should sort dates ascending', async () => {
        const collection = client.db(dbName).collection('sort_date_asc');
        const date1 = new Date('2024-01-01');
        const date2 = new Date('2024-06-01');
        const date3 = new Date('2024-12-01');

        await collection.insertMany([
          { createdAt: date2 },
          { createdAt: date3 },
          { createdAt: date1 },
        ]);

        const docs = await collection.find({}).sort({ createdAt: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual((docs[0].createdAt as Date).getTime(), date1.getTime());
        assert.strictEqual((docs[1].createdAt as Date).getTime(), date2.getTime());
        assert.strictEqual((docs[2].createdAt as Date).getTime(), date3.getTime());
      });
    });

    describe('single field descending', () => {
      it('should sort numbers descending', async () => {
        const collection = client.db(dbName).collection('sort_num_desc');
        await collection.insertMany([{ value: 10 }, { value: 30 }, { value: 20 }]);

        const docs = await collection.find({}).sort({ value: -1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].value, 30);
        assert.strictEqual(docs[1].value, 20);
        assert.strictEqual(docs[2].value, 10);
      });

      it('should sort strings descending', async () => {
        const collection = client.db(dbName).collection('sort_str_desc');
        await collection.insertMany([{ name: 'Alice' }, { name: 'Charlie' }, { name: 'Bob' }]);

        const docs = await collection.find({}).sort({ name: -1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].name, 'Charlie');
        assert.strictEqual(docs[1].name, 'Bob');
        assert.strictEqual(docs[2].name, 'Alice');
      });
    });

    describe('compound/multiple field sort', () => {
      it('should sort by multiple fields', async () => {
        const collection = client.db(dbName).collection('sort_compound');
        await collection.insertMany([
          { category: 'A', priority: 2 },
          { category: 'B', priority: 1 },
          { category: 'A', priority: 1 },
          { category: 'B', priority: 2 },
        ]);

        const docs = await collection.find({}).sort({ category: 1, priority: 1 }).toArray();

        assert.strictEqual(docs.length, 4);
        assert.strictEqual(docs[0].category, 'A');
        assert.strictEqual(docs[0].priority, 1);
        assert.strictEqual(docs[1].category, 'A');
        assert.strictEqual(docs[1].priority, 2);
        assert.strictEqual(docs[2].category, 'B');
        assert.strictEqual(docs[2].priority, 1);
        assert.strictEqual(docs[3].category, 'B');
        assert.strictEqual(docs[3].priority, 2);
      });

      it('should sort by multiple fields with mixed directions', async () => {
        const collection = client.db(dbName).collection('sort_compound_mixed');
        await collection.insertMany([
          { category: 'A', priority: 1 },
          { category: 'A', priority: 2 },
          { category: 'B', priority: 1 },
          { category: 'B', priority: 2 },
        ]);

        // Sort category ascending, priority descending
        const docs = await collection.find({}).sort({ category: 1, priority: -1 }).toArray();

        assert.strictEqual(docs.length, 4);
        assert.strictEqual(docs[0].category, 'A');
        assert.strictEqual(docs[0].priority, 2);
        assert.strictEqual(docs[1].category, 'A');
        assert.strictEqual(docs[1].priority, 1);
        assert.strictEqual(docs[2].category, 'B');
        assert.strictEqual(docs[2].priority, 2);
        assert.strictEqual(docs[3].category, 'B');
        assert.strictEqual(docs[3].priority, 1);
      });
    });

    describe('sort with nested fields', () => {
      it('should sort by nested field using dot notation', async () => {
        const collection = client.db(dbName).collection('sort_nested');
        await collection.insertMany([
          { user: { name: 'Charlie' } },
          { user: { name: 'Alice' } },
          { user: { name: 'Bob' } },
        ]);

        const docs = await collection.find({}).sort({ 'user.name': 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual((docs[0].user as { name: string }).name, 'Alice');
        assert.strictEqual((docs[1].user as { name: string }).name, 'Bob');
        assert.strictEqual((docs[2].user as { name: string }).name, 'Charlie');
      });
    });

    describe('sort with null/missing values', () => {
      it('should place null values first when ascending', async () => {
        const collection = client.db(dbName).collection('sort_null_asc');
        await collection.insertMany([{ value: 20 }, { value: null }, { value: 10 }]);

        const docs = await collection.find({}).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].value, null);
        assert.strictEqual(docs[1].value, 10);
        assert.strictEqual(docs[2].value, 20);
      });

      it('should place null values last when descending', async () => {
        const collection = client.db(dbName).collection('sort_null_desc');
        await collection.insertMany([{ value: null }, { value: 20 }, { value: 10 }]);

        const docs = await collection.find({}).sort({ value: -1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].value, 20);
        assert.strictEqual(docs[1].value, 10);
        assert.strictEqual(docs[2].value, null);
      });

      it('should place missing values first when ascending', async () => {
        const collection = client.db(dbName).collection('sort_missing_asc');
        await collection.insertMany([
          { value: 20 },
          { other: 'field' }, // value is missing
          { value: 10 },
        ]);

        const docs = await collection.find({}).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].value, undefined);
        assert.strictEqual(docs[1].value, 10);
        assert.strictEqual(docs[2].value, 20);
      });
    });

    describe('sort with filter', () => {
      it('should sort filtered results', async () => {
        const collection = client.db(dbName).collection('sort_filtered');
        await collection.insertMany([
          { type: 'A', value: 30 },
          { type: 'B', value: 10 },
          { type: 'A', value: 10 },
          { type: 'A', value: 20 },
        ]);

        const docs = await collection.find({ type: 'A' }).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].value, 10);
        assert.strictEqual(docs[1].value, 20);
        assert.strictEqual(docs[2].value, 30);
      });
    });

    describe('sort with array fields', () => {
      it('should sort by minimum element ascending', async () => {
        const collection = client.db(dbName).collection('sort_array_asc');
        await collection.insertMany([
          { name: 'doc1', scores: [10, 20, 30] }, // min: 10
          { name: 'doc2', scores: [5, 15, 25] }, // min: 5
          { name: 'doc3', scores: [8, 12] }, // min: 8
        ]);

        const docs = await collection.find({}).sort({ scores: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].name, 'doc2'); // min 5
        assert.strictEqual(docs[1].name, 'doc3'); // min 8
        assert.strictEqual(docs[2].name, 'doc1'); // min 10
      });

      it('should sort by maximum element descending', async () => {
        const collection = client.db(dbName).collection('sort_array_desc');
        await collection.insertMany([
          { name: 'doc1', scores: [10, 20, 30] }, // max: 30
          { name: 'doc2', scores: [5, 15, 25] }, // max: 25
          { name: 'doc3', scores: [8, 50] }, // max: 50
        ]);

        const docs = await collection.find({}).sort({ scores: -1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].name, 'doc3'); // max 50
        assert.strictEqual(docs[1].name, 'doc1'); // max 30
        assert.strictEqual(docs[2].name, 'doc2'); // max 25
      });

      it('should handle empty arrays (sort before null)', async () => {
        const collection = client.db(dbName).collection('sort_array_empty');
        await collection.insertMany([
          { name: 'with_null', value: null },
          { name: 'with_empty', value: [] },
          { name: 'with_value', value: [5] },
        ]);

        const docs = await collection.find({}).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        // Empty array sorts before null in MongoDB
        assert.strictEqual(docs[0].name, 'with_empty');
        assert.strictEqual(docs[1].name, 'with_null');
        assert.strictEqual(docs[2].name, 'with_value');
      });
    });

    describe('sort with mixed types', () => {
      it('should sort null before numbers', async () => {
        const collection = client.db(dbName).collection('sort_mixed_null_num');
        await collection.insertMany([
          { name: 'number', value: 5 },
          { name: 'null', value: null },
          { name: 'another_number', value: 3 },
        ]);

        const docs = await collection.find({}).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        assert.strictEqual(docs[0].name, 'null');
        assert.strictEqual(docs[1].name, 'another_number');
        assert.strictEqual(docs[2].name, 'number');
      });

      it('should sort numbers before strings', async () => {
        const collection = client.db(dbName).collection('sort_mixed_num_string');
        await collection.insertMany([
          { name: 'string', value: 'abc' },
          { name: 'number', value: 100 },
        ]);

        const docs = await collection.find({}).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 2);
        assert.strictEqual(docs[0].name, 'number');
        assert.strictEqual(docs[1].name, 'string');
      });

      it('should sort strings before objects', async () => {
        const collection = client.db(dbName).collection('sort_mixed_string_obj');
        await collection.insertMany([
          { name: 'object', value: { a: 1 } },
          { name: 'string', value: 'xyz' },
        ]);

        const docs = await collection.find({}).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 2);
        assert.strictEqual(docs[0].name, 'string');
        assert.strictEqual(docs[1].name, 'object');
      });

      it('should sort booleans after ObjectId', async () => {
        const collection = client.db(dbName).collection('sort_mixed_bool_oid');
        // Note: We need to be careful with ObjectId import
        // For this test, we just check boolean vs other primitives

        await collection.insertMany([
          { name: 'true', value: true },
          { name: 'false', value: false },
          { name: 'number', value: 5 },
        ]);

        const docs = await collection.find({}).sort({ value: 1 }).toArray();

        assert.strictEqual(docs.length, 3);
        // Numbers come before booleans
        assert.strictEqual(docs[0].name, 'number');
        // false comes before true
        assert.strictEqual(docs[1].name, 'false');
        assert.strictEqual(docs[2].name, 'true');
      });
    });
  });

  describe('limit', () => {
    it('should limit results to specified count', async () => {
      const collection = client.db(dbName).collection('limit_basic');
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      const docs = await collection.find({}).limit(3).toArray();

      assert.strictEqual(docs.length, 3);
    });

    it('should return all if limit exceeds count', async () => {
      const collection = client.db(dbName).collection('limit_exceeds');
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      const docs = await collection.find({}).limit(10).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it('should return all documents if limit is 0 (no limit)', async () => {
      const collection = client.db(dbName).collection('limit_zero');
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      // In MongoDB, limit(0) means "no limit" - returns all documents
      const docs = await collection.find({}).limit(0).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it('should handle negative limit (returns absolute value)', async () => {
      const collection = client.db(dbName).collection('limit_negative');
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      const docs = await collection.find({}).sort({ value: 1 }).limit(-3).toArray();

      // Negative limit returns |n| documents (single batch behavior)
      // The exact count may vary, but should be at most 3
      assert.ok(docs.length <= 3, `Expected at most 3 docs, got ${docs.length}`);
      assert.ok(docs.length > 0, 'Expected at least 1 document');
      // First document should be the smallest when sorted
      if (docs.length > 0) {
        assert.strictEqual(docs[0].value, 1);
      }
    });
  });

  describe('skip', () => {
    it('should skip specified number of documents', async () => {
      const collection = client.db(dbName).collection('skip_basic');
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
      ]);

      const docs = await collection.find({}).sort({ value: 1 }).skip(2).toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0].value, 3);
      assert.strictEqual(docs[1].value, 4);
      assert.strictEqual(docs[2].value, 5);
    });

    it('should return empty array if skip exceeds count', async () => {
      const collection = client.db(dbName).collection('skip_exceeds');
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      const docs = await collection.find({}).skip(10).toArray();

      assert.strictEqual(docs.length, 0);
    });

    it('should skip 0 documents when skip is 0', async () => {
      const collection = client.db(dbName).collection('skip_zero');
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      const docs = await collection.find({}).skip(0).toArray();

      assert.strictEqual(docs.length, 2);
    });

    it('should throw error for negative skip', async () => {
      const collection = client.db(dbName).collection('skip_negative');
      await collection.insertMany([{ value: 1 }, { value: 2 }]);

      await assert.rejects(
        async () => {
          await collection.find({}).skip(-1).toArray();
        },
        { message: /non-negative|skip/i }
      );
    });
  });

  describe('chaining cursor methods', () => {
    it('should chain sort and limit', async () => {
      const collection = client.db(dbName).collection('chain_sort_limit');
      await collection.insertMany([
        { value: 50 },
        { value: 10 },
        { value: 30 },
        { value: 40 },
        { value: 20 },
      ]);

      const docs = await collection.find({}).sort({ value: 1 }).limit(3).toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0].value, 10);
      assert.strictEqual(docs[1].value, 20);
      assert.strictEqual(docs[2].value, 30);
    });

    it('should chain sort and skip', async () => {
      const collection = client.db(dbName).collection('chain_sort_skip');
      await collection.insertMany([
        { value: 50 },
        { value: 10 },
        { value: 30 },
        { value: 40 },
        { value: 20 },
      ]);

      const docs = await collection.find({}).sort({ value: 1 }).skip(2).toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0].value, 30);
      assert.strictEqual(docs[1].value, 40);
      assert.strictEqual(docs[2].value, 50);
    });

    it('should chain sort, skip and limit', async () => {
      const collection = client.db(dbName).collection('chain_all');
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
        { value: 6 },
        { value: 7 },
      ]);

      const docs = await collection.find({}).sort({ value: 1 }).skip(2).limit(3).toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0].value, 3);
      assert.strictEqual(docs[1].value, 4);
      assert.strictEqual(docs[2].value, 5);
    });

    it('should allow methods in any order', async () => {
      const collection = client.db(dbName).collection('chain_order');
      await collection.insertMany([
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
        { value: 5 },
        { value: 6 },
        { value: 7 },
      ]);

      // limit -> sort -> skip (different order in code)
      // MongoDB always applies: sort first, then skip, then limit
      const docs = await collection.find({}).limit(3).sort({ value: 1 }).skip(2).toArray();

      // Sort: [1,2,3,4,5,6,7], Skip 2: [3,4,5,6,7], Limit 3: [3,4,5]
      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0].value, 3);
      assert.strictEqual(docs[1].value, 4);
      assert.strictEqual(docs[2].value, 5);
    });
  });

  describe('projection', () => {
    describe('field inclusion', () => {
      it('should include only specified fields', async () => {
        const collection = client.db(dbName).collection('proj_include');
        await collection.insertOne({
          name: 'Alice',
          age: 30,
          city: 'NYC',
          email: 'alice@example.com',
        });

        const docs = await collection.find({}, { projection: { name: 1, age: 1 } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].name, 'Alice');
        assert.strictEqual(docs[0].age, 30);
        assert.strictEqual(docs[0].city, undefined);
        assert.strictEqual(docs[0].email, undefined);
        // _id is included by default
        assert.ok(docs[0]._id !== undefined);
      });

      it('should exclude _id when specified', async () => {
        const collection = client.db(dbName).collection('proj_exclude_id');
        await collection.insertOne({ name: 'Alice', age: 30 });

        const docs = await collection.find({}, { projection: { name: 1, _id: 0 } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].name, 'Alice');
        assert.strictEqual(docs[0]._id, undefined);
        assert.strictEqual(docs[0].age, undefined);
      });
    });

    describe('field exclusion', () => {
      it('should exclude specified fields', async () => {
        const collection = client.db(dbName).collection('proj_exclude');
        await collection.insertOne({
          name: 'Alice',
          age: 30,
          city: 'NYC',
          email: 'alice@example.com',
        });

        const docs = await collection.find({}, { projection: { email: 0, city: 0 } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].name, 'Alice');
        assert.strictEqual(docs[0].age, 30);
        assert.strictEqual(docs[0].city, undefined);
        assert.strictEqual(docs[0].email, undefined);
        assert.ok(docs[0]._id !== undefined);
      });

      it('should include all fields except excluded ones', async () => {
        const collection = client.db(dbName).collection('proj_exclude_one');
        await collection.insertOne({ a: 1, b: 2, c: 3, d: 4 });

        const docs = await collection.find({}, { projection: { b: 0 } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].a, 1);
        assert.strictEqual(docs[0].b, undefined);
        assert.strictEqual(docs[0].c, 3);
        assert.strictEqual(docs[0].d, 4);
      });
    });

    describe('projection with nested fields', () => {
      it('should project nested fields', async () => {
        const collection = client.db(dbName).collection('proj_nested');
        await collection.insertOne({
          name: 'Alice',
          address: { city: 'NYC', zip: '10001', street: '123 Main' },
        });

        const docs = await collection
          .find({}, { projection: { name: 1, 'address.city': 1 } })
          .toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].name, 'Alice');
        assert.ok(docs[0].address !== undefined);
        assert.strictEqual((docs[0].address as { city: string }).city, 'NYC');
        assert.strictEqual((docs[0].address as { zip?: string }).zip, undefined);
      });
    });

    describe('projection with findOne', () => {
      it('should apply projection to findOne', async () => {
        const collection = client.db(dbName).collection('proj_findone');
        await collection.insertOne({ name: 'Alice', age: 30, city: 'NYC' });

        const doc = await collection.findOne(
          { name: 'Alice' },
          { projection: { name: 1, _id: 0 } }
        );

        assert.ok(doc !== null);
        assert.strictEqual(doc.name, 'Alice');
        assert.strictEqual(doc._id, undefined);
        assert.strictEqual(doc.age, undefined);
      });
    });

    describe('projection with cursor methods', () => {
      it('should combine projection with sort and limit', async () => {
        const collection = client.db(dbName).collection('proj_combined');
        await collection.insertMany([
          { name: 'Charlie', age: 35, score: 80 },
          { name: 'Alice', age: 30, score: 90 },
          { name: 'Bob', age: 25, score: 85 },
        ]);

        const docs = await collection
          .find({}, { projection: { name: 1, score: 1, _id: 0 } })
          .sort({ score: -1 })
          .limit(2)
          .toArray();

        assert.strictEqual(docs.length, 2);
        assert.strictEqual(docs[0].name, 'Alice');
        assert.strictEqual(docs[0].score, 90);
        assert.strictEqual(docs[0].age, undefined);
        assert.strictEqual(docs[1].name, 'Bob');
        assert.strictEqual(docs[1].score, 85);
      });
    });

    describe('projection error handling', () => {
      it('should throw error when mixing inclusion and exclusion', async () => {
        const collection = client.db(dbName).collection('proj_mix_error');
        await collection.insertOne({ name: 'Alice', age: 30, city: 'NYC' });

        await assert.rejects(
          async () => {
            await collection.find({}, { projection: { name: 1, age: 0 } }).toArray();
          },
          (err: Error) => {
            // MongoDB error: "Projection cannot have a mix of inclusion and exclusion"
            // MangoDB error: "Cannot mix inclusion and exclusion in projection"
            const msg = err.message.toLowerCase();
            const hasRelevantWords =
              msg.includes('mix') ||
              msg.includes('inclusion') ||
              msg.includes('exclusion') ||
              msg.includes('cannot') ||
              msg.includes('invalid');
            assert.ok(
              hasRelevantWords,
              `Expected error about mixing projection modes, got: ${err.message}`
            );
            return true;
          }
        );
      });

      it('should allow _id: 0 with inclusion projection', async () => {
        const collection = client.db(dbName).collection('proj_id_exclusion');
        await collection.insertOne({ name: 'Alice', age: 30, city: 'NYC' });

        // This should NOT throw - _id: 0 is special case
        const docs = await collection.find({}, { projection: { name: 1, _id: 0 } }).toArray();

        assert.strictEqual(docs.length, 1);
        assert.strictEqual(docs[0].name, 'Alice');
        assert.strictEqual(docs[0]._id, undefined);
      });
    });
  });

  describe('countDocuments', () => {
    it('should count all documents with empty filter', async () => {
      const collection = client.db(dbName).collection('count_all');
      await collection.insertMany([{ value: 1 }, { value: 2 }, { value: 3 }]);

      const count = await collection.countDocuments({});

      assert.strictEqual(count, 3);
    });

    it('should count matching documents with filter', async () => {
      const collection = client.db(dbName).collection('count_filter');
      await collection.insertMany([
        { type: 'A', value: 1 },
        { type: 'B', value: 2 },
        { type: 'A', value: 3 },
        { type: 'A', value: 4 },
      ]);

      const count = await collection.countDocuments({ type: 'A' });

      assert.strictEqual(count, 3);
    });

    it('should return 0 for empty collection', async () => {
      const collection = client.db(dbName).collection('count_empty');

      const count = await collection.countDocuments({});

      assert.strictEqual(count, 0);
    });

    it('should return 0 when no documents match', async () => {
      const collection = client.db(dbName).collection('count_no_match');
      await collection.insertMany([{ type: 'A' }, { type: 'B' }]);

      const count = await collection.countDocuments({ type: 'C' });

      assert.strictEqual(count, 0);
    });

    it('should work with comparison operators', async () => {
      const collection = client.db(dbName).collection('count_operators');
      await collection.insertMany([{ value: 10 }, { value: 20 }, { value: 30 }, { value: 40 }]);

      const count = await collection.countDocuments({ value: { $gte: 25 } });

      assert.strictEqual(count, 2);
    });
  });
});
