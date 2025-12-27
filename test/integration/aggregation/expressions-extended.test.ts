/**
 * Extended Expression Operators Tests
 *
 * Tests for expression operators: string expressions, array expressions,
 * bitwise operators, type conversions, and accumulators.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

describe(`String Expression Operators (${getTestModeName()})`, () => {
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

  describe('$regexFind', () => {
    it('should find first regex match', async () => {
      const collection = client.db(dbName).collection('regex_find');
      await collection.insertOne({ text: 'hello world hello' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              result: {
                $regexFind: { input: '$text', regex: 'hello' },
              },
            },
          },
        ])
        .toArray();

      const result = docs[0].result as { match: string; idx: number } | null;
      assert.strictEqual(result?.match, 'hello');
      assert.strictEqual(result?.idx, 0);
    });

    it('should support captures', async () => {
      const collection = client.db(dbName).collection('regex_capture');
      await collection.insertOne({ text: 'abc123def' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              result: {
                $regexFind: { input: '$text', regex: '([a-z]+)(\\d+)' },
              },
            },
          },
        ])
        .toArray();

      const result = docs[0].result as { captures: string[] } | null;
      const captures = result?.captures;
      assert.ok(Array.isArray(captures));
      assert.strictEqual(captures?.[0], 'abc');
      assert.strictEqual(captures?.[1], '123');
    });
  });

  describe('$regexFindAll', () => {
    it('should find all regex matches', async () => {
      const collection = client.db(dbName).collection('regex_all');
      await collection.insertOne({ text: 'a1 b2 c3' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              results: {
                $regexFindAll: { input: '$text', regex: '[a-z]\\d' },
              },
            },
          },
        ])
        .toArray();

      const results = docs[0].results as Array<{ match: string }>;
      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].match, 'a1');
      assert.strictEqual(results[1].match, 'b2');
      assert.strictEqual(results[2].match, 'c3');
    });
  });

  describe('$regexMatch', () => {
    it('should return true when regex matches', async () => {
      const collection = client.db(dbName).collection('regex_match_true');
      await collection.insertOne({ text: 'hello123' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              matches: {
                $regexMatch: { input: '$text', regex: '\\d+' },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].matches, true);
    });

    it('should return false when regex does not match', async () => {
      const collection = client.db(dbName).collection('regex_match_false');
      await collection.insertOne({ text: 'hello' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              matches: {
                $regexMatch: { input: '$text', regex: '\\d+' },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].matches, false);
    });
  });

  describe('$replaceOne and $replaceAll', () => {
    it('should replace first occurrence', async () => {
      const collection = client.db(dbName).collection('replace_one');
      await collection.insertOne({ text: 'foo bar foo' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              replaced: {
                $replaceOne: {
                  input: '$text',
                  find: 'foo',
                  replacement: 'baz',
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].replaced, 'baz bar foo');
    });

    it('should replace all occurrences', async () => {
      const collection = client.db(dbName).collection('replace_all');
      await collection.insertOne({ text: 'foo bar foo' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              replaced: {
                $replaceAll: {
                  input: '$text',
                  find: 'foo',
                  replacement: 'baz',
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].replaced, 'baz bar baz');
    });
  });

  describe('$strcasecmp', () => {
    it('should return 0 for equal strings ignoring case', async () => {
      const collection = client.db(dbName).collection('strcasecmp_eq');
      await collection.insertOne({});

      const docs = await collection
        .aggregate([{ $project: { cmp: { $strcasecmp: ['Hello', 'hello'] } } }])
        .toArray();

      assert.strictEqual(docs[0].cmp, 0);
    });

    it('should return -1 when first string is less', async () => {
      const collection = client.db(dbName).collection('strcasecmp_lt');
      await collection.insertOne({});

      const docs = await collection
        .aggregate([{ $project: { cmp: { $strcasecmp: ['ABC', 'xyz'] } } }])
        .toArray();

      assert.strictEqual(docs[0].cmp, -1);
    });

    it('should return 1 when first string is greater', async () => {
      const collection = client.db(dbName).collection('strcasecmp_gt');
      await collection.insertOne({});

      const docs = await collection
        .aggregate([{ $project: { cmp: { $strcasecmp: ['xyz', 'ABC'] } } }])
        .toArray();

      assert.strictEqual(docs[0].cmp, 1);
    });
  });

  describe('$strLenBytes', () => {
    it('should return byte length for ASCII', async () => {
      const collection = client.db(dbName).collection('strlen_ascii');
      await collection.insertOne({ text: 'hello' });

      const docs = await collection
        .aggregate([{ $project: { len: { $strLenBytes: '$text' } } }])
        .toArray();

      assert.strictEqual(docs[0].len, 5);
    });

    it('should return byte length for UTF-8', async () => {
      const collection = client.db(dbName).collection('strlen_utf8');
      await collection.insertOne({ text: 'こんにちは' }); // 5 chars, 15 bytes in UTF-8

      const docs = await collection
        .aggregate([{ $project: { len: { $strLenBytes: '$text' } } }])
        .toArray();

      assert.strictEqual(docs[0].len, 15);
    });
  });
});

describe(`Array Expression Operators (${getTestModeName()})`, () => {
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

  describe('$first and $last', () => {
    it('should return first element', async () => {
      const collection = client.db(dbName).collection('first_basic');
      await collection.insertOne({ items: [1, 2, 3, 4] });

      const docs = await collection
        .aggregate([{ $project: { first: { $first: '$items' } } }])
        .toArray();

      assert.strictEqual(docs[0].first, 1);
    });

    it('should return last element', async () => {
      const collection = client.db(dbName).collection('last_basic');
      await collection.insertOne({ items: ['a', 'b', 'c'] });

      const docs = await collection
        .aggregate([{ $project: { last: { $last: '$items' } } }])
        .toArray();

      assert.strictEqual(docs[0].last, 'c');
    });

    it('should return null for null array', async () => {
      const collection = client.db(dbName).collection('first_null');
      await collection.insertOne({ items: null });

      const docs = await collection
        .aggregate([{ $project: { first: { $first: '$items' } } }])
        .toArray();

      assert.strictEqual(docs[0].first, null);
    });
  });

  describe('$indexOfArray', () => {
    it('should find index of element', async () => {
      const collection = client.db(dbName).collection('indexof_basic');
      await collection.insertOne({ items: ['a', 'b', 'c', 'd'] });

      const docs = await collection
        .aggregate([{ $project: { idx: { $indexOfArray: ['$items', 'c'] } } }])
        .toArray();

      assert.strictEqual(docs[0].idx, 2);
    });

    it('should return -1 when not found', async () => {
      const collection = client.db(dbName).collection('indexof_notfound');
      await collection.insertOne({ items: [1, 2, 3] });

      const docs = await collection
        .aggregate([{ $project: { idx: { $indexOfArray: ['$items', 5] } } }])
        .toArray();

      assert.strictEqual(docs[0].idx, -1);
    });

    it('should support start and end indices', async () => {
      const collection = client.db(dbName).collection('indexof_range');
      await collection.insertOne({ items: ['a', 'b', 'c', 'b', 'd'] });

      const docs = await collection
        .aggregate([{ $project: { idx: { $indexOfArray: ['$items', 'b', 2] } } }])
        .toArray();

      assert.strictEqual(docs[0].idx, 3);
    });
  });

  describe('$isArray', () => {
    it('should return true for arrays', async () => {
      const collection = client.db(dbName).collection('isarray_true');
      await collection.insertOne({ items: [1, 2, 3] });

      const docs = await collection
        .aggregate([{ $project: { isArr: { $isArray: ['$items'] } } }])
        .toArray();

      assert.strictEqual(docs[0].isArr, true);
    });

    it('should return false for non-arrays', async () => {
      const collection = client.db(dbName).collection('isarray_false');
      await collection.insertOne({ value: 'string' });

      const docs = await collection
        .aggregate([{ $project: { isArr: { $isArray: ['$value'] } } }])
        .toArray();

      assert.strictEqual(docs[0].isArr, false);
    });
  });

  describe('$range', () => {
    it('should generate integer range', async () => {
      const collection = client.db(dbName).collection('range_basic');
      await collection.insertOne({});

      const docs = await collection
        .aggregate([{ $project: { nums: { $range: [0, 5] } } }])
        .toArray();

      assert.deepStrictEqual(docs[0].nums, [0, 1, 2, 3, 4]);
    });

    it('should support step parameter', async () => {
      const collection = client.db(dbName).collection('range_step');
      await collection.insertOne({});

      const docs = await collection
        .aggregate([{ $project: { nums: { $range: [0, 10, 2] } } }])
        .toArray();

      assert.deepStrictEqual(docs[0].nums, [0, 2, 4, 6, 8]);
    });

    it('should support negative step', async () => {
      const collection = client.db(dbName).collection('range_neg');
      await collection.insertOne({});

      const docs = await collection
        .aggregate([{ $project: { nums: { $range: [5, 0, -1] } } }])
        .toArray();

      assert.deepStrictEqual(docs[0].nums, [5, 4, 3, 2, 1]);
    });
  });

  describe('$reverseArray', () => {
    it('should reverse an array', async () => {
      const collection = client.db(dbName).collection('reverse_basic');
      await collection.insertOne({ items: [1, 2, 3, 4] });

      const docs = await collection
        .aggregate([{ $project: { reversed: { $reverseArray: '$items' } } }])
        .toArray();

      assert.deepStrictEqual(docs[0].reversed, [4, 3, 2, 1]);
    });
  });

  describe('$arrayToObject and $objectToArray', () => {
    it('should convert array to object using [k, v] format', async () => {
      const collection = client.db(dbName).collection('a2o_kv');
      await collection.insertOne({
        pairs: [
          ['name', 'John'],
          ['age', 30],
        ],
      });

      const docs = await collection
        .aggregate([{ $project: { obj: { $arrayToObject: '$pairs' } } }])
        .toArray();

      assert.deepStrictEqual(docs[0].obj, { name: 'John', age: 30 });
    });

    it('should convert array to object using {k, v} format', async () => {
      const collection = client.db(dbName).collection('a2o_obj');
      await collection.insertOne({
        pairs: [
          { k: 'x', v: 10 },
          { k: 'y', v: 20 },
        ],
      });

      const docs = await collection
        .aggregate([{ $project: { obj: { $arrayToObject: '$pairs' } } }])
        .toArray();

      assert.deepStrictEqual(docs[0].obj, { x: 10, y: 20 });
    });

    it('should convert object to array', async () => {
      const collection = client.db(dbName).collection('o2a_basic');
      await collection.insertOne({ data: { a: 1, b: 2 } });

      const docs = await collection
        .aggregate([{ $project: { arr: { $objectToArray: '$data' } } }])
        .toArray();

      const arr = docs[0].arr as Array<{ k: string; v: unknown }>;
      assert.ok(arr.some((e) => e.k === 'a' && e.v === 1));
      assert.ok(arr.some((e) => e.k === 'b' && e.v === 2));
    });
  });

  describe('$zip', () => {
    it('should zip arrays together', async () => {
      const collection = client.db(dbName).collection('zip_basic');
      await collection.insertOne({
        a: [1, 2, 3],
        b: ['a', 'b', 'c'],
      });

      const docs = await collection
        .aggregate([
          {
            $project: {
              zipped: { $zip: { inputs: ['$a', '$b'] } },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(docs[0].zipped, [
        [1, 'a'],
        [2, 'b'],
        [3, 'c'],
      ]);
    });

    it('should use shortest array by default', async () => {
      const collection = client.db(dbName).collection('zip_short');
      await collection.insertOne({
        a: [1, 2, 3, 4],
        b: ['x', 'y'],
      });

      const docs = await collection
        .aggregate([{ $project: { zipped: { $zip: { inputs: ['$a', '$b'] } } } }])
        .toArray();

      assert.deepStrictEqual(docs[0].zipped, [
        [1, 'x'],
        [2, 'y'],
      ]);
    });

    it('should pad with defaults when using longest length', async () => {
      const collection = client.db(dbName).collection('zip_long');
      await collection.insertOne({
        a: [1, 2],
        b: ['x', 'y', 'z'],
      });

      const docs = await collection
        .aggregate([
          {
            $project: {
              zipped: {
                $zip: {
                  inputs: ['$a', '$b'],
                  useLongestLength: true,
                  defaults: [0, 'default'],
                },
              },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(docs[0].zipped, [
        [1, 'x'],
        [2, 'y'],
        [0, 'z'],
      ]);
    });
  });

  describe('$sortArray', () => {
    it('should sort primitives ascending', async () => {
      const collection = client.db(dbName).collection('sortarr_asc');
      await collection.insertOne({ nums: [3, 1, 4, 1, 5] });

      const docs = await collection
        .aggregate([
          {
            $project: {
              sorted: { $sortArray: { input: '$nums', sortBy: 1 } },
            },
          },
        ])
        .toArray();

      assert.deepStrictEqual(docs[0].sorted, [1, 1, 3, 4, 5]);
    });

    it('should sort objects by field', async () => {
      const collection = client.db(dbName).collection('sortarr_field');
      await collection.insertOne({
        items: [{ name: 'C' }, { name: 'A' }, { name: 'B' }],
      });

      const docs = await collection
        .aggregate([
          {
            $project: {
              sorted: {
                $sortArray: { input: '$items', sortBy: { name: 1 } },
              },
            },
          },
        ])
        .toArray();

      const names = (docs[0].sorted as Array<{ name: string }>).map((i) => i.name);
      assert.deepStrictEqual(names, ['A', 'B', 'C']);
    });
  });
});

describe(`New Operators (${getTestModeName()})`, () => {
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

  describe('$comment operator', () => {
    it('should ignore $comment and return matching documents', async () => {
      const collection = client.db(dbName).collection('comment_basic');
      await collection.insertMany([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
        { name: 'Charlie', age: 35 },
      ]);

      // $comment should not affect query results
      const docs = await collection
        .find({ age: { $gte: 30 }, $comment: 'Find users 30 or older' } as any)
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(docs.some((d) => d.name === 'Alice'));
      assert.ok(docs.some((d) => d.name === 'Charlie'));
    });

    it('should work with $comment as the only query parameter alongside field query', async () => {
      const collection = client.db(dbName).collection('comment_with_field');
      await collection.insertMany([{ status: 'active' }, { status: 'inactive' }]);

      const docs = await collection
        .find({ status: 'active', $comment: 'Get active items' } as any)
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].status, 'active');
    });

    it('should work with complex queries and $comment', async () => {
      const collection = client.db(dbName).collection('comment_complex');
      await collection.insertMany([
        { x: 2, y: 10 },
        { x: 3, y: 15 },
        { x: 4, y: 20 },
        { x: 5, y: 25 },
      ]);

      const docs = await collection
        .find({
          $and: [{ x: { $gte: 3 } }, { y: { $lte: 20 } }],
          $comment: 'Find records where x >= 3 and y <= 20',
        } as any)
        .toArray();

      assert.strictEqual(docs.length, 2);
    });
  });

  describe('Bitwise query operators', () => {
    describe('$bitsAllSet', () => {
      it('should match when all specified bit positions are set', async () => {
        const collection = client.db(dbName).collection('bits_allset_pos');
        // 54 in binary is 110110 (bits 1, 2, 4, 5 are set)
        // 50 in binary is 110010 (bits 1, 4, 5 are set - bit 2 is NOT set)
        // 38 in binary is 100110 (bits 1, 2, 5 are set - bit 4 is NOT set)
        await collection.insertMany([
          { value: 54 }, // 110110
          { value: 50 }, // 110010
          { value: 38 }, // 100110
        ]);

        // Test bits 1 and 5 (both set in all three numbers)
        const docs = await collection.find({ value: { $bitsAllSet: [1, 5] } }).toArray();

        assert.strictEqual(docs.length, 3);
      });

      it('should not match when any specified bit is clear', async () => {
        const collection = client.db(dbName).collection('bits_allset_nomatch');
        await collection.insertMany([
          { value: 54 }, // 110110 - bit 0 is clear
        ]);

        // Bit 0 is not set in 54
        const docs = await collection.find({ value: { $bitsAllSet: [0, 1] } }).toArray();

        assert.strictEqual(docs.length, 0);
      });

      it('should work with numeric bitmask', async () => {
        const collection = client.db(dbName).collection('bits_allset_mask');
        await collection.insertMany([
          { value: 54 }, // 110110
          { value: 50 }, // 110010
        ]);

        // Bitmask 50 = 110010 (bits 1, 4, 5)
        const docs = await collection.find({ value: { $bitsAllSet: 50 } }).toArray();

        // Both 54 and 50 have bits 1, 4, 5 set
        assert.strictEqual(docs.length, 2);
      });

      it('should not match non-numeric field values', async () => {
        const collection = client.db(dbName).collection('bits_allset_type');
        await collection.insertMany([
          { value: '54' }, // string
          { value: true }, // boolean
          { value: null }, // null
        ]);

        const docs = await collection.find({ value: { $bitsAllSet: [1] } }).toArray();

        assert.strictEqual(docs.length, 0);
      });

      it('should match array elements', async () => {
        const collection = client.db(dbName).collection('bits_allset_array');
        await collection.insertMany([
          { value: [54] }, // array with numeric element - 54 = 110110, bit 1 is set
          { value: [1, 2] }, // array with elements - 2 = 10, bit 1 is set
        ]);

        const docs = await collection.find({ value: { $bitsAllSet: [1] } }).toArray();

        // Both arrays have elements with bit 1 set
        assert.strictEqual(docs.length, 2);
      });
    });

    describe('$bitsAllClear', () => {
      it('should match when all specified bit positions are clear', async () => {
        const collection = client.db(dbName).collection('bits_allclear_pos');
        // 54 in binary is 110110 (bits 0 and 3 are clear)
        await collection.insertMany([
          { value: 54 }, // 110110
          { value: 50 }, // 110010 - bit 0 clear, bit 2 set
        ]);

        // Test bit 0 (clear in both)
        const docs = await collection.find({ value: { $bitsAllClear: [0] } }).toArray();

        assert.strictEqual(docs.length, 2);
      });

      it('should not match when any specified bit is set', async () => {
        const collection = client.db(dbName).collection('bits_allclear_nomatch');
        await collection.insertMany([
          { value: 54 }, // 110110 - bit 1 is set
        ]);

        // Bit 1 is set in 54
        const docs = await collection.find({ value: { $bitsAllClear: [0, 1] } }).toArray();

        assert.strictEqual(docs.length, 0);
      });

      it('should work with numeric bitmask', async () => {
        const collection = client.db(dbName).collection('bits_allclear_mask');
        await collection.insertMany([
          { value: 54 }, // 110110
          { value: 50 }, // 110010
        ]);

        // Bitmask 9 = 001001 (bits 0 and 3)
        // Both 54 and 50 have bits 0 and 3 clear
        const docs = await collection.find({ value: { $bitsAllClear: 9 } }).toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe('$bitsAnySet', () => {
      it('should match when any specified bit position is set', async () => {
        const collection = client.db(dbName).collection('bits_anyset_pos');
        await collection.insertMany([
          { value: 54 }, // 110110
          { value: 1 }, // 000001
          { value: 8 }, // 001000
        ]);

        // Test bits 0 and 3 - value 1 has bit 0, value 8 has bit 3
        const docs = await collection.find({ value: { $bitsAnySet: [0, 3] } }).toArray();

        assert.strictEqual(docs.length, 2); // 1 and 8 match
      });

      it('should not match when all specified bits are clear', async () => {
        const collection = client.db(dbName).collection('bits_anyset_nomatch');
        await collection.insertMany([
          { value: 54 }, // 110110 - bits 0 and 3 are clear
        ]);

        const docs = await collection.find({ value: { $bitsAnySet: [0, 3] } }).toArray();

        assert.strictEqual(docs.length, 0);
      });

      it('should work with numeric bitmask', async () => {
        const collection = client.db(dbName).collection('bits_anyset_mask');
        await collection.insertMany([
          { value: 54 }, // 110110
          { value: 1 }, // 000001
        ]);

        // Bitmask 3 = 000011 (bits 0 and 1)
        // 54 has bit 1 set, 1 has bit 0 set
        const docs = await collection.find({ value: { $bitsAnySet: 3 } }).toArray();

        assert.strictEqual(docs.length, 2);
      });
    });

    describe('$bitsAnyClear', () => {
      it('should match when any specified bit position is clear', async () => {
        const collection = client.db(dbName).collection('bits_anyclear_pos');
        await collection.insertMany([
          { value: 54 }, // 110110 - bit 0 is clear
          { value: 55 }, // 110111 - bit 3 is clear
          { value: 63 }, // 111111 - no bits 0-5 are clear
        ]);

        // Test bits 0 and 3
        const docs = await collection.find({ value: { $bitsAnyClear: [0, 3] } }).toArray();

        assert.strictEqual(docs.length, 2); // 54 and 55 match
      });

      it('should not match when all specified bits are set', async () => {
        const collection = client.db(dbName).collection('bits_anyclear_nomatch');
        await collection.insertMany([
          { value: 54 }, // 110110 - bits 1, 2, 4, 5 are set
        ]);

        const docs = await collection.find({ value: { $bitsAnyClear: [1, 2] } }).toArray();

        assert.strictEqual(docs.length, 0);
      });

      it('should work with numeric bitmask', async () => {
        const collection = client.db(dbName).collection('bits_anyclear_mask');
        await collection.insertMany([
          { value: 54 }, // 110110
          { value: 63 }, // 111111
        ]);

        // Bitmask 9 = 001001 (bits 0 and 3)
        // 54 has both bits 0 and 3 clear
        const docs = await collection.find({ value: { $bitsAnyClear: 9 } }).toArray();

        assert.strictEqual(docs.length, 1); // only 54 matches
      });
    });

    describe('Bitwise operator edge cases', () => {
      it('should handle negative numbers with sign extension', async () => {
        const collection = client.db(dbName).collection('bits_negative');
        // -1 in two's complement has all bits set
        // -5 = ...11111011 (bit 2 is clear)
        // 5 = 00000101 (bit 0 and 2 are set)
        await collection.insertMany([
          { value: -1 },
          { value: -5 }, // ...11111011
          { value: 5 }, // 00000101
        ]);

        // Bit 0 is set in all three: -1 (all bits), -5 (...11111011), 5 (00000101)
        const docs = await collection.find({ value: { $bitsAllSet: [0] } }).toArray();

        assert.strictEqual(docs.length, 3);
      });

      it('should handle zero value', async () => {
        const collection = client.db(dbName).collection('bits_zero');
        await collection.insertMany([{ value: 0 }]);

        // No bits are set in 0
        const docsSet = await collection.find({ value: { $bitsAnySet: [0, 1, 2] } }).toArray();
        assert.strictEqual(docsSet.length, 0);

        // All bits are clear in 0
        const docsClear = await collection.find({ value: { $bitsAllClear: [0, 1, 2] } }).toArray();
        assert.strictEqual(docsClear.length, 1);
      });

      it('should handle empty position array', async () => {
        const collection = client.db(dbName).collection('bits_empty');
        await collection.insertMany([{ value: 54 }]);

        // Empty array - vacuous truth for $bitsAllSet (all of nothing are set)
        const docs = await collection.find({ value: { $bitsAllSet: [] } }).toArray();

        // MongoDB behavior: empty array matches all numeric values
        assert.strictEqual(docs.length, 1);
      });

      it('should handle missing field', async () => {
        const collection = client.db(dbName).collection('bits_missing');
        await collection.insertMany([{ other: 'field' }, { value: 54 }]);

        const docs = await collection.find({ value: { $bitsAllSet: [1] } }).toArray();

        assert.strictEqual(docs.length, 1);
      });
    });
  });

  describe('Arithmetic expression operators', () => {
    it('$exp should calculate e^x', async () => {
      const collection = client.db(dbName).collection('arith_exp');
      await collection.insertMany([{ value: 0 }, { value: 1 }, { value: 2 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $exp: '$value' } } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.ok(Math.abs((docs[0].result as number) - 1) < 0.0001); // e^0 = 1
      assert.ok(Math.abs((docs[1].result as number) - Math.E) < 0.0001); // e^1 = e
      assert.ok(Math.abs((docs[2].result as number) - Math.E * Math.E) < 0.0001); // e^2
    });

    it('$ln should calculate natural logarithm', async () => {
      const collection = client.db(dbName).collection('arith_ln');
      await collection.insertMany([{ value: 1 }, { value: Math.E }, { value: 10 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $ln: '$value' } } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.ok(Math.abs((docs[0].result as number) - 0) < 0.0001); // ln(1) = 0
      assert.ok(Math.abs((docs[1].result as number) - 1) < 0.0001); // ln(e) = 1
    });

    it('$log should calculate logarithm with specified base', async () => {
      const collection = client.db(dbName).collection('arith_log');
      await collection.insertMany([{ value: 8 }, { value: 100 }]);

      const docs = await collection
        .aggregate([
          { $project: { log2: { $log: ['$value', 2] }, log10: { $log: ['$value', 10] } } },
        ])
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.ok(Math.abs((docs[0].log2 as number) - 3) < 0.0001); // log2(8) = 3
      assert.ok(Math.abs((docs[1].log10 as number) - 2) < 0.0001); // log10(100) = 2
    });

    it('$log10 should calculate base-10 logarithm', async () => {
      const collection = client.db(dbName).collection('arith_log10');
      await collection.insertMany([{ value: 1 }, { value: 10 }, { value: 100 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $log10: '$value' } } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.ok(Math.abs((docs[0].result as number) - 0) < 0.0001);
      assert.ok(Math.abs((docs[1].result as number) - 1) < 0.0001);
      assert.ok(Math.abs((docs[2].result as number) - 2) < 0.0001);
    });

    it('$pow should raise to power', async () => {
      const collection = client.db(dbName).collection('arith_pow');
      await collection.insertMany([
        { base: 2, exp: 3 },
        { base: 10, exp: 2 },
      ]);

      const docs = await collection
        .aggregate([{ $project: { result: { $pow: ['$base', '$exp'] } } }])
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.strictEqual(docs[0].result, 8); // 2^3 = 8
      assert.strictEqual(docs[1].result, 100); // 10^2 = 100
    });

    it('$sqrt should calculate square root', async () => {
      const collection = client.db(dbName).collection('arith_sqrt');
      await collection.insertMany([{ value: 0 }, { value: 4 }, { value: 9 }, { value: 2 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $sqrt: '$value' } } }])
        .toArray();

      assert.strictEqual(docs.length, 4);
      assert.strictEqual(docs[0].result, 0);
      assert.strictEqual(docs[1].result, 2);
      assert.strictEqual(docs[2].result, 3);
      assert.ok(Math.abs((docs[3].result as number) - Math.SQRT2) < 0.0001);
    });

    it('$trunc should truncate to integer', async () => {
      const collection = client.db(dbName).collection('arith_trunc');
      await collection.insertMany([{ value: 3.7 }, { value: -2.3 }, { value: 5.9 }]);

      const docs = await collection
        .aggregate([{ $project: { result: { $trunc: '$value' } } }])
        .toArray();

      assert.strictEqual(docs.length, 3);
      assert.strictEqual(docs[0].result, 3);
      assert.strictEqual(docs[1].result, -2);
      assert.strictEqual(docs[2].result, 5);
    });

    it('$trunc should support decimal places', async () => {
      const collection = client.db(dbName).collection('arith_trunc_places');
      await collection.insertMany([{ value: 3.14159 }]);

      const docs = await collection
        .aggregate([
          { $project: { trunc2: { $trunc: ['$value', 2] }, trunc0: { $trunc: '$value' } } },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].trunc2, 3.14);
      assert.strictEqual(docs[0].trunc0, 3);
    });

    it('should return null for null inputs', async () => {
      const collection = client.db(dbName).collection('arith_null');
      await collection.insertMany([{ value: null }]);

      const docs = await collection
        .aggregate([
          {
            $project: {
              exp: { $exp: '$value' },
              sqrt: { $sqrt: '$value' },
              pow: { $pow: ['$value', 2] },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.strictEqual(docs[0].exp, null);
      assert.strictEqual(docs[0].sqrt, null);
      assert.strictEqual(docs[0].pow, null);
    });
  });

  describe('$rand expression operator', () => {
    it('should return a number between 0 and 1 in aggregation', async () => {
      const collection = client.db(dbName).collection('rand_agg');
      await collection.insertMany([{ name: 'test' }]);

      const docs = await collection
        .aggregate([{ $project: { randomValue: { $rand: {} } } }])
        .toArray();

      assert.strictEqual(docs.length, 1);
      const randomValue = docs[0].randomValue as number;
      assert.strictEqual(typeof randomValue, 'number');
      assert.ok(randomValue >= 0, 'random value should be >= 0');
      assert.ok(randomValue < 1, 'random value should be < 1');
    });

    it('should generate different values for each document', async () => {
      const collection = client.db(dbName).collection('rand_multi');
      await collection.insertMany([
        { name: 'doc1' },
        { name: 'doc2' },
        { name: 'doc3' },
        { name: 'doc4' },
        { name: 'doc5' },
      ]);

      const docs = await collection
        .aggregate([{ $project: { name: 1, randomValue: { $rand: {} } } }])
        .toArray();

      assert.strictEqual(docs.length, 5);

      // Check all values are numbers in range
      for (const doc of docs) {
        const randomValue = doc.randomValue as number;
        assert.strictEqual(typeof randomValue, 'number');
        assert.ok(randomValue >= 0);
        assert.ok(randomValue < 1);
      }

      // Collect unique values - with 5 docs, we should get at least 2 unique values
      // (probability of all 5 being the same is essentially 0)
      const uniqueValues = new Set(docs.map((d) => d.randomValue as number));
      assert.ok(uniqueValues.size >= 2, 'should have multiple unique random values');
    });

    it('should work with $expr in find queries for random sampling', async () => {
      const collection = client.db(dbName).collection('rand_expr');
      // Insert many documents so we can test random sampling
      const manyDocs = Array.from({ length: 100 }, (_, i) => ({ index: i }));
      await collection.insertMany(manyDocs);

      // Use $expr with $rand to randomly sample ~50% of documents
      // Since it's random, we can't assert exact count, but should be in a reasonable range
      const docs = await collection.find({ $expr: { $lt: [{ $rand: {} }, 0.5] } }).toArray();

      // With 100 documents and 50% sampling, we should get roughly 30-70 documents
      // (3 standard deviations from 50)
      assert.ok(docs.length >= 20, `Expected at least 20 documents, got ${docs.length}`);
      assert.ok(docs.length <= 80, `Expected at most 80 documents, got ${docs.length}`);
    });

    it('should work with scaling to generate larger random numbers', async () => {
      const collection = client.db(dbName).collection('rand_scale');
      await collection.insertMany([{ name: 'test' }]);

      const docs = await collection
        .aggregate([
          {
            $project: {
              randomInt: { $floor: { $multiply: [{ $rand: {} }, 100] } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs.length, 1);
      const randomInt = docs[0].randomInt as number;
      assert.strictEqual(typeof randomInt, 'number');
      assert.ok(Number.isInteger(randomInt), 'should be an integer');
      assert.ok(randomInt >= 0, 'should be >= 0');
      assert.ok(randomInt < 100, 'should be < 100');
    });
  });

  // Phase 10: Other Expression Operators
  describe('$cmp operator', () => {
    it('should return -1 when first value is less than second', async () => {
      const collection = client.db(dbName).collection('cmp_less');
      await collection.insertOne({ a: 5, b: 10 });

      const docs = await collection
        .aggregate([{ $project: { result: { $cmp: ['$a', '$b'] } } }])
        .toArray();

      assert.strictEqual(docs[0].result, -1);
    });

    it('should return 0 when values are equal', async () => {
      const collection = client.db(dbName).collection('cmp_equal');
      await collection.insertOne({ a: 5, b: 5 });

      const docs = await collection
        .aggregate([{ $project: { result: { $cmp: ['$a', '$b'] } } }])
        .toArray();

      assert.strictEqual(docs[0].result, 0);
    });

    it('should return 1 when first value is greater than second', async () => {
      const collection = client.db(dbName).collection('cmp_greater');
      await collection.insertOne({ a: 10, b: 5 });

      const docs = await collection
        .aggregate([{ $project: { result: { $cmp: ['$a', '$b'] } } }])
        .toArray();

      assert.strictEqual(docs[0].result, 1);
    });

    it('should compare strings', async () => {
      const collection = client.db(dbName).collection('cmp_strings');
      await collection.insertOne({ a: 'apple', b: 'banana' });

      const docs = await collection
        .aggregate([{ $project: { result: { $cmp: ['$a', '$b'] } } }])
        .toArray();

      assert.strictEqual(docs[0].result, -1);
    });
  });

  describe('$switch operator', () => {
    it('should return value from first matching branch', async () => {
      const collection = client.db(dbName).collection('switch_basic');
      await collection.insertOne({ score: 85 });

      const docs = await collection
        .aggregate([
          {
            $project: {
              grade: {
                $switch: {
                  branches: [
                    { case: { $gte: ['$score', 90] }, then: 'A' },
                    { case: { $gte: ['$score', 80] }, then: 'B' },
                    { case: { $gte: ['$score', 70] }, then: 'C' },
                  ],
                  default: 'F',
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].grade, 'B');
    });

    it('should return default when no branch matches', async () => {
      const collection = client.db(dbName).collection('switch_default');
      await collection.insertOne({ score: 50 });

      const docs = await collection
        .aggregate([
          {
            $project: {
              grade: {
                $switch: {
                  branches: [
                    { case: { $gte: ['$score', 90] }, then: 'A' },
                    { case: { $gte: ['$score', 80] }, then: 'B' },
                  ],
                  default: 'F',
                },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].grade, 'F');
    });

    it('should throw when no branch matches and no default', async () => {
      const collection = client.db(dbName).collection('switch_no_default');
      await collection.insertOne({ score: 50 });

      await assert.rejects(async () => {
        await collection
          .aggregate([
            {
              $project: {
                grade: {
                  $switch: {
                    branches: [{ case: { $gte: ['$score', 90] }, then: 'A' }],
                  },
                },
              },
            },
          ])
          .toArray();
      }, /\$switch/);
    });
  });

  describe('$isNumber operator', () => {
    it('should return true for numbers', async () => {
      const collection = client.db(dbName).collection('isNumber_true');
      await collection.insertOne({ value: 42 });

      const docs = await collection
        .aggregate([{ $project: { result: { $isNumber: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].result, true);
    });

    it('should return true for floating point numbers', async () => {
      const collection = client.db(dbName).collection('isNumber_float');
      await collection.insertOne({ value: 3.14 });

      const docs = await collection
        .aggregate([{ $project: { result: { $isNumber: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].result, true);
    });

    it('should return false for strings', async () => {
      const collection = client.db(dbName).collection('isNumber_string');
      await collection.insertOne({ value: '42' });

      const docs = await collection
        .aggregate([{ $project: { result: { $isNumber: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].result, false);
    });

    it('should return false for null', async () => {
      const collection = client.db(dbName).collection('isNumber_null');
      await collection.insertOne({ value: null });

      const docs = await collection
        .aggregate([{ $project: { result: { $isNumber: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].result, false);
    });
  });

  describe('$toLong operator', () => {
    it('should convert number to long (truncated)', async () => {
      const collection = client.db(dbName).collection('toLong_number');
      await collection.insertOne({ value: 3.7 });

      const docs = await collection
        .aggregate([{ $project: { result: { $toLong: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].result, 3);
    });

    it('should convert string to long', async () => {
      const collection = client.db(dbName).collection('toLong_string');
      await collection.insertOne({ value: '123' });

      const docs = await collection
        .aggregate([{ $project: { result: { $toLong: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].result, 123);
    });

    it('should return null for null input', async () => {
      const collection = client.db(dbName).collection('toLong_null');
      await collection.insertOne({ value: null });

      const docs = await collection
        .aggregate([{ $project: { result: { $toLong: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].result, null);
    });
  });

  describe('$toDecimal operator', () => {
    it('should convert string to decimal', async () => {
      const collection = client.db(dbName).collection('toDecimal_string');
      await collection.insertOne({ value: '3.14159' });

      const docs = await collection
        .aggregate([{ $project: { result: { $toDecimal: '$value' } } }])
        .toArray();

      // MongoDB returns Decimal128 object, MangoDB returns number
      // Convert to number for comparison
      const resultValue = Number(docs[0].result);
      assert.ok(Math.abs(resultValue - 3.14159) < 0.0001);
    });
  });

  describe('$convert operator', () => {
    it('should convert string to int', async () => {
      const collection = client.db(dbName).collection('convert_string_int');
      await collection.insertOne({ value: '42' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              result: { $convert: { input: '$value', to: 'int' } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].result, 42);
    });

    it('should use onNull when input is null', async () => {
      const collection = client.db(dbName).collection('convert_onNull');
      await collection.insertOne({ value: null });

      const docs = await collection
        .aggregate([
          {
            $project: {
              result: {
                $convert: { input: '$value', to: 'int', onNull: -1 },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].result, -1);
    });

    it('should use onError when conversion fails', async () => {
      const collection = client.db(dbName).collection('convert_onError');
      await collection.insertOne({ value: 'not a number' });

      const docs = await collection
        .aggregate([
          {
            $project: {
              result: {
                $convert: { input: '$value', to: 'int', onError: 0 },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].result, 0);
    });

    it('should convert to bool', async () => {
      const collection = client.db(dbName).collection('convert_bool');
      await collection.insertOne({ value: 1 });

      const docs = await collection
        .aggregate([
          {
            $project: {
              result: { $convert: { input: '$value', to: 'bool' } },
            },
          },
        ])
        .toArray();

      assert.strictEqual(docs[0].result, true);
    });
  });

  describe('$count accumulator', () => {
    it('should count documents in a group', async () => {
      const collection = client.db(dbName).collection('count_accum');
      await collection.insertMany([
        { category: 'A' },
        { category: 'A' },
        { category: 'B' },
        { category: 'A' },
        { category: 'B' },
      ]);

      const docs = await collection
        .aggregate([{ $group: { _id: '$category', count: { $count: {} } } }, { $sort: { _id: 1 } }])
        .toArray();

      assert.strictEqual(docs.length, 2);
      assert.strictEqual(docs[0]._id, 'A');
      assert.strictEqual(docs[0].count, 3);
      assert.strictEqual(docs[1]._id, 'B');
      assert.strictEqual(docs[1].count, 2);
    });
  });

  describe('$mergeObjects accumulator', () => {
    it('should merge objects in a group', async () => {
      const collection = client.db(dbName).collection('mergeObjects_accum');
      await collection.insertMany([
        { id: 1, data: { a: 1 } },
        { id: 1, data: { b: 2 } },
        { id: 1, data: { c: 3 } },
      ]);

      const docs = await collection
        .aggregate([{ $group: { _id: '$id', merged: { $mergeObjects: '$data' } } }])
        .toArray();

      assert.strictEqual(docs.length, 1);
      assert.deepStrictEqual(docs[0].merged, { a: 1, b: 2, c: 3 });
    });

    it('should override earlier values with later ones', async () => {
      const collection = client.db(dbName).collection('mergeObjects_override');
      await collection.insertMany([
        { id: 1, data: { a: 1 } },
        { id: 1, data: { a: 2 } },
      ]);

      const docs = await collection
        .aggregate([{ $group: { _id: '$id', merged: { $mergeObjects: '$data' } } }])
        .toArray();

      const merged = docs[0].merged as { a: number };
      assert.strictEqual(merged.a, 2);
    });
  });

  describe('$stdDevPop accumulator', () => {
    it('should calculate population standard deviation', async () => {
      const collection = client.db(dbName).collection('stdDevPop_basic');
      await collection.insertMany([
        { group: 'A', value: 2 },
        { group: 'A', value: 4 },
        { group: 'A', value: 4 },
        { group: 'A', value: 4 },
        { group: 'A', value: 5 },
        { group: 'A', value: 5 },
        { group: 'A', value: 7 },
        { group: 'A', value: 9 },
      ]);

      const docs = await collection
        .aggregate([{ $group: { _id: '$group', stdDev: { $stdDevPop: '$value' } } }])
        .toArray();

      // Mean = 5, variance = 4, stdDev = 2
      assert.strictEqual(docs.length, 1);
      assert.ok(Math.abs((docs[0].stdDev as number) - 2) < 0.001);
    });

    it('should return null for empty group', async () => {
      const collection = client.db(dbName).collection('stdDevPop_empty');
      await collection.insertMany([{ group: 'A', value: 'not a number' }]);

      const docs = await collection
        .aggregate([{ $group: { _id: '$group', stdDev: { $stdDevPop: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].stdDev, null);
    });
  });

  describe('$stdDevSamp accumulator', () => {
    it('should calculate sample standard deviation', async () => {
      const collection = client.db(dbName).collection('stdDevSamp_basic');
      await collection.insertMany([
        { group: 'A', value: 2 },
        { group: 'A', value: 4 },
        { group: 'A', value: 4 },
        { group: 'A', value: 4 },
        { group: 'A', value: 5 },
        { group: 'A', value: 5 },
        { group: 'A', value: 7 },
        { group: 'A', value: 9 },
      ]);

      const docs = await collection
        .aggregate([{ $group: { _id: '$group', stdDev: { $stdDevSamp: '$value' } } }])
        .toArray();

      // Sample std dev is sqrt(32/7) ≈ 2.138
      assert.strictEqual(docs.length, 1);
      const expected = Math.sqrt(32 / 7);
      assert.ok(Math.abs((docs[0].stdDev as number) - expected) < 0.001);
    });

    it('should return null for single value', async () => {
      const collection = client.db(dbName).collection('stdDevSamp_single');
      await collection.insertMany([{ group: 'A', value: 5 }]);

      const docs = await collection
        .aggregate([{ $group: { _id: '$group', stdDev: { $stdDevSamp: '$value' } } }])
        .toArray();

      assert.strictEqual(docs[0].stdDev, null);
    });
  });

  // Phase 12: Collection Methods
  describe('replaceOne method', () => {
    it('should replace a document', async () => {
      const collection = client.db(dbName).collection('replaceOne_basic');
      await collection.insertOne({ name: 'John', age: 30 });

      const result = await collection.replaceOne(
        { name: 'John' },
        { name: 'John Doe', age: 31, email: 'john@example.com' }
      );

      assert.strictEqual(result.matchedCount, 1);
      assert.strictEqual(result.modifiedCount, 1);

      const doc = await collection.findOne({ name: 'John Doe' });
      assert.strictEqual(doc?.age, 31);
      assert.strictEqual(doc?.email, 'john@example.com');
    });

    it('should preserve _id when replacing', async () => {
      const collection = client.db(dbName).collection('replaceOne_id');
      const insertResult = await collection.insertOne({ name: 'Jane', age: 25 });
      const originalId = insertResult.insertedId;

      await collection.replaceOne({ name: 'Jane' }, { name: 'Jane Smith', age: 26 });

      const doc = await collection.findOne({ name: 'Jane Smith' });
      const docId = doc?._id as { equals: (id: unknown) => boolean };
      assert.ok(docId.equals(originalId));
    });

    it('should upsert when document not found', async () => {
      const collection = client.db(dbName).collection('replaceOne_upsert');

      const result = await collection.replaceOne(
        { name: 'NotFound' },
        { name: 'NewUser', age: 20 },
        { upsert: true }
      );

      assert.strictEqual(result.matchedCount, 0);
      assert.strictEqual(result.upsertedCount, 1);
      assert.ok(result.upsertedId);

      const doc = await collection.findOne({ name: 'NewUser' });
      assert.strictEqual(doc?.age, 20);
    });

    it('should return matchedCount 0 when no match and no upsert', async () => {
      const collection = client.db(dbName).collection('replaceOne_nomatch');

      const result = await collection.replaceOne(
        { name: 'NonExistent' },
        { name: 'NewDoc', age: 30 }
      );

      assert.strictEqual(result.matchedCount, 0);
      assert.strictEqual(result.modifiedCount, 0);
    });
  });

  describe('createIndexes method', () => {
    it('should create multiple indexes', async () => {
      const collection = client.db(dbName).collection('createIndexes_basic');
      await collection.insertOne({ email: 'test@example.com', name: 'Test' });

      const names = await collection.createIndexes([
        { key: { email: 1 }, unique: true },
        { key: { name: 1 } },
      ]);

      assert.strictEqual(names.length, 2);
      assert.ok(names.includes('email_1'));
      assert.ok(names.includes('name_1'));

      const indexes = await collection.indexes();
      const indexNames = indexes.map((idx) => idx.name);
      assert.ok(indexNames.includes('email_1'));
      assert.ok(indexNames.includes('name_1'));
    });
  });

  describe('dropIndexes method', () => {
    it('should drop all indexes except _id', async () => {
      const collection = client.db(dbName).collection('dropIndexes_all');
      await collection.insertOne({ email: 'test@example.com', name: 'Test' });
      await collection.createIndex({ email: 1 });
      await collection.createIndex({ name: 1 });

      await collection.dropIndexes();

      const indexes = await collection.indexes();
      assert.strictEqual(indexes.length, 1);
      assert.strictEqual(indexes[0].name, '_id_');
    });
  });
});
