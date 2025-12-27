/**
 * Expression Operators Tests
 *
 * Tests for aggregation expression operators: arithmetic, string, array,
 * type conversion, date, set, and boolean/logical operators.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

describe(`Expression Operators (${getTestModeName()})`, () => {
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

  // ==================== Part 1: Arithmetic Operators ====================

  describe('Arithmetic Operators', () => {
    describe('$abs', () => {
      it('should return absolute value of positive number', async () => {
        const collection = client.db(dbName).collection('abs_positive');
        await collection.insertOne({ value: 5 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it('should return absolute value of negative number', async () => {
        const collection = client.db(dbName).collection('abs_negative');
        await collection.insertOne({ value: -5 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it('should return 0 for 0', async () => {
        const collection = client.db(dbName).collection('abs_zero');
        await collection.insertOne({ value: 0 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 0);
      });

      it('should return null for null input', async () => {
        const collection = client.db(dbName).collection('abs_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return null for missing field', async () => {
        const collection = client.db(dbName).collection('abs_missing');
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should throw for non-numeric input', async () => {
        const collection = client.db(dbName).collection('abs_string');
        await collection.insertOne({ value: 'hello' });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $abs: '$value' }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$abs only supports numeric types'));
            return true;
          }
        );
      });

      it('should handle floating point numbers', async () => {
        const collection = client.db(dbName).collection('abs_float');
        await collection.insertOne({ value: -3.14 });

        const results = await collection
          .aggregate([{ $project: { result: { $abs: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 3.14);
      });
    });

    describe('$ceil', () => {
      it('should round 2.3 up to 3', async () => {
        const collection = client.db(dbName).collection('ceil_up');
        await collection.insertOne({ value: 2.3 });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 3);
      });

      it('should return integer unchanged', async () => {
        const collection = client.db(dbName).collection('ceil_int');
        await collection.insertOne({ value: 5 });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it('should round -2.3 to -2', async () => {
        const collection = client.db(dbName).collection('ceil_neg');
        await collection.insertOne({ value: -2.3 });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, -2);
      });

      it('should return null for null input', async () => {
        const collection = client.db(dbName).collection('ceil_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return null for missing field', async () => {
        const collection = client.db(dbName).collection('ceil_missing');
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $ceil: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should throw for non-numeric input', async () => {
        const collection = client.db(dbName).collection('ceil_string');
        await collection.insertOne({ value: 'hello' });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $ceil: '$value' }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$ceil only supports numeric types'));
            return true;
          }
        );
      });
    });

    describe('$floor', () => {
      it('should round 2.7 down to 2', async () => {
        const collection = client.db(dbName).collection('floor_down');
        await collection.insertOne({ value: 2.7 });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 2);
      });

      it('should return integer unchanged', async () => {
        const collection = client.db(dbName).collection('floor_int');
        await collection.insertOne({ value: 5 });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it('should round -2.3 to -3', async () => {
        const collection = client.db(dbName).collection('floor_neg');
        await collection.insertOne({ value: -2.3 });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, -3);
      });

      it('should return null for null input', async () => {
        const collection = client.db(dbName).collection('floor_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return null for missing field', async () => {
        const collection = client.db(dbName).collection('floor_missing');
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $floor: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should throw for non-numeric input', async () => {
        const collection = client.db(dbName).collection('floor_string');
        await collection.insertOne({ value: 'hello' });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $floor: '$value' }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$floor only supports numeric types'));
            return true;
          }
        );
      });
    });

    describe('$round', () => {
      it('should round to nearest integer by default', async () => {
        const collection = client.db(dbName).collection('round_default');
        await collection.insertOne({ value: 2.5 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: '$value' }, _id: 0 } }])
          .toArray();

        // MongoDB uses banker's rounding (round half to even): 2.5 -> 2
        assert.strictEqual(results[0].result, 2);
      });

      it('should round to specified decimal places', async () => {
        const collection = client.db(dbName).collection('round_places');
        await collection.insertOne({ value: 2.567 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: ['$value', 2] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 2.57);
      });

      it('should round with negative place value', async () => {
        const collection = client.db(dbName).collection('round_neg_place');
        await collection.insertOne({ value: 1234 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: ['$value', -2] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1200);
      });

      it('should return null for null input', async () => {
        const collection = client.db(dbName).collection('round_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $round: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return null for missing field', async () => {
        const collection = client.db(dbName).collection('round_missing');
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should throw for non-numeric input', async () => {
        const collection = client.db(dbName).collection('round_string');
        await collection.insertOne({ value: 'hello' });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $round: '$value' }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$round only supports numeric types'));
            return true;
          }
        );
      });

      it('should handle array form with single element', async () => {
        const collection = client.db(dbName).collection('round_array_single');
        await collection.insertOne({ value: 2.7 });

        const results = await collection
          .aggregate([{ $project: { result: { $round: ['$value'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 3);
      });
    });

    describe('$mod', () => {
      it('should compute modulo of two positive numbers', async () => {
        const collection = client.db(dbName).collection('mod_positive');
        await collection.insertOne({ a: 10, b: 3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1);
      });

      it('should handle negative dividend (remainder follows dividend sign)', async () => {
        const collection = client.db(dbName).collection('mod_neg_dividend');
        await collection.insertOne({ a: -10, b: 3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        // JavaScript/MongoDB: -10 % 3 = -1
        assert.strictEqual(results[0].result, -1);
      });

      it('should handle negative divisor', async () => {
        const collection = client.db(dbName).collection('mod_neg_divisor');
        await collection.insertOne({ a: 10, b: -3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        // JavaScript/MongoDB: 10 % -3 = 1
        assert.strictEqual(results[0].result, 1);
      });

      it('should throw error when divisor is 0', async () => {
        const collection = client.db(dbName).collection('mod_zero');
        await collection.insertOne({ a: 10, b: 0 });

        await assert.rejects(
          collection
            .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
            .toArray(),
          (err: Error) => err.message.includes('$mod by zero')
        );
      });

      it('should return null for null dividend', async () => {
        const collection = client.db(dbName).collection('mod_null_dividend');
        await collection.insertOne({ a: null, b: 3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return null for null divisor', async () => {
        const collection = client.db(dbName).collection('mod_null_divisor');
        await collection.insertOne({ a: 10, b: null });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return null for missing field', async () => {
        const collection = client.db(dbName).collection('mod_missing');
        await collection.insertOne({ a: 10 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should throw for non-numeric inputs', async () => {
        const collection = client.db(dbName).collection('mod_string');
        await collection.insertOne({ a: 'hello', b: 3 });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$mod only supports numeric types'));
            return true;
          }
        );
      });

      it('should handle floating point modulo', async () => {
        const collection = client.db(dbName).collection('mod_float');
        await collection.insertOne({ a: 10.5, b: 3 });

        const results = await collection
          .aggregate([{ $project: { result: { $mod: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1.5);
      });
    });
  });

  // ==================== Part 2: String Operators ====================

  describe('String Operators', () => {
    describe('$substrCP', () => {
      it('should extract substring from start', async () => {
        const collection = client.db(dbName).collection('substr_basic');
        await collection.insertOne({ value: 'hello world' });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ['$value', 0, 5] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'hello');
      });

      it('should extract substring from middle', async () => {
        const collection = client.db(dbName).collection('substr_middle');
        await collection.insertOne({ value: 'hello world' });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ['$value', 6, 5] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'world');
      });

      it('should handle out of bounds gracefully', async () => {
        const collection = client.db(dbName).collection('substr_oob');
        await collection.insertOne({ value: 'hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ['$value', 3, 100] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'lo');
      });

      it('should return empty string for null input', async () => {
        const collection = client.db(dbName).collection('substr_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ['$value', 0, 5] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, '');
      });

      it('should return empty string for missing field', async () => {
        const collection = client.db(dbName).collection('substr_missing');
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $substrCP: ['$value', 0, 5] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, '');
      });
    });

    describe('$strLenCP', () => {
      it('should return string length', async () => {
        const collection = client.db(dbName).collection('strlen_basic');
        await collection.insertOne({ value: 'hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $strLenCP: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5);
      });

      it('should return 0 for empty string', async () => {
        const collection = client.db(dbName).collection('strlen_empty');
        await collection.insertOne({ value: '' });

        const results = await collection
          .aggregate([{ $project: { result: { $strLenCP: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 0);
      });

      it('should throw for null input', async () => {
        const collection = client.db(dbName).collection('strlen_null');
        await collection.insertOne({ value: null });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $strLenCP: '$value' }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$strLenCP requires a string argument'));
            return true;
          }
        );
      });

      it('should throw for non-string input', async () => {
        const collection = client.db(dbName).collection('strlen_number');
        await collection.insertOne({ value: 123 });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $strLenCP: '$value' }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$strLenCP requires a string argument'));
            return true;
          }
        );
      });
    });

    describe('$split', () => {
      it('should split string by delimiter', async () => {
        const collection = client.db(dbName).collection('split_basic');
        await collection.insertOne({ value: 'a,b,c' });

        const results = await collection
          .aggregate([{ $project: { result: { $split: ['$value', ','] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, ['a', 'b', 'c']);
      });

      it('should return single-element array when delimiter not found', async () => {
        const collection = client.db(dbName).collection('split_notfound');
        await collection.insertOne({ value: 'hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $split: ['$value', ','] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, ['hello']);
      });

      it('should handle empty parts', async () => {
        const collection = client.db(dbName).collection('split_empty');
        await collection.insertOne({ value: ',a,,b,' });

        const results = await collection
          .aggregate([{ $project: { result: { $split: ['$value', ','] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, ['', 'a', '', 'b', '']);
      });

      it('should return null for null string', async () => {
        const collection = client.db(dbName).collection('split_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $split: ['$value', ','] }, _id: 0 } }])
          .toArray();

        // MongoDB returns null for null input string
        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$trim', () => {
      it('should trim whitespace by default', async () => {
        const collection = client.db(dbName).collection('trim_basic');
        await collection.insertOne({ value: '  hello  ' });

        const results = await collection
          .aggregate([{ $project: { result: { $trim: { input: '$value' } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'hello');
      });

      it('should trim custom characters', async () => {
        const collection = client.db(dbName).collection('trim_custom');
        await collection.insertOne({ value: 'xxhelloxx' });

        const results = await collection
          .aggregate([{ $project: { result: { $trim: { input: '$value', chars: 'x' } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'hello');
      });

      it('should throw for non-string input', async () => {
        const collection = client.db(dbName).collection('trim_number');
        await collection.insertOne({ value: 123 });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $trim: { input: '$value' } }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$trim'));
            return true;
          }
        );
      });
    });

    describe('$ltrim', () => {
      it('should trim left whitespace only', async () => {
        const collection = client.db(dbName).collection('ltrim_basic');
        await collection.insertOne({ value: '  hello  ' });

        const results = await collection
          .aggregate([{ $project: { result: { $ltrim: { input: '$value' } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'hello  ');
      });
    });

    describe('$rtrim', () => {
      it('should trim right whitespace only', async () => {
        const collection = client.db(dbName).collection('rtrim_basic');
        await collection.insertOne({ value: '  hello  ' });

        const results = await collection
          .aggregate([{ $project: { result: { $rtrim: { input: '$value' } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, '  hello');
      });
    });

    describe('$toString', () => {
      it('should convert number to string', async () => {
        const collection = client.db(dbName).collection('tostring_num');
        await collection.insertOne({ value: 123 });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, '123');
      });

      it('should convert boolean true to string', async () => {
        const collection = client.db(dbName).collection('tostring_true');
        await collection.insertOne({ value: true });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'true');
      });

      it('should convert boolean false to string', async () => {
        const collection = client.db(dbName).collection('tostring_false');
        await collection.insertOne({ value: false });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'false');
      });

      it('should return null for null input', async () => {
        const collection = client.db(dbName).collection('tostring_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return null for missing field', async () => {
        const collection = client.db(dbName).collection('tostring_missing');
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should keep string unchanged', async () => {
        const collection = client.db(dbName).collection('tostring_string');
        await collection.insertOne({ value: 'hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $toString: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'hello');
      });
    });

    describe('$indexOfCP', () => {
      it('should return index of substring', async () => {
        const collection = client.db(dbName).collection('indexof_found');
        await collection.insertOne({ value: 'hello world' });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ['$value', 'world'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 6);
      });

      it('should return -1 when not found', async () => {
        const collection = client.db(dbName).collection('indexof_notfound');
        await collection.insertOne({ value: 'hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ['$value', 'xyz'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, -1);
      });

      it('should return null for null string', async () => {
        const collection = client.db(dbName).collection('indexof_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ['$value', 'x'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return 0 for match at beginning', async () => {
        const collection = client.db(dbName).collection('indexof_start');
        await collection.insertOne({ value: 'hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ['$value', 'hel'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 0);
      });

      it('should support start index', async () => {
        const collection = client.db(dbName).collection('indexof_startidx');
        await collection.insertOne({ value: 'hello hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $indexOfCP: ['$value', 'hello', 1] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 6);
      });
    });
  });

  // ==================== Part 3: Array Operators ====================

  describe('Array Operators', () => {
    describe('$arrayElemAt', () => {
      it('should get element at positive index', async () => {
        const collection = client.db(dbName).collection('arrayelemat_pos');
        await collection.insertOne({ arr: ['a', 'b', 'c', 'd'] });

        const results = await collection
          .aggregate([{ $project: { result: { $arrayElemAt: ['$arr', 1] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'b');
      });

      it('should get element at negative index', async () => {
        const collection = client.db(dbName).collection('arrayelemat_neg');
        await collection.insertOne({ arr: ['a', 'b', 'c', 'd'] });

        const results = await collection
          .aggregate([{ $project: { result: { $arrayElemAt: ['$arr', -1] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'd');
      });

      it('should return undefined (missing) for out of bounds', async () => {
        const collection = client.db(dbName).collection('arrayelemat_oob');
        await collection.insertOne({ arr: ['a', 'b'] });

        const results = await collection
          .aggregate([{ $project: { result: { $arrayElemAt: ['$arr', 10] }, _id: 0 } }])
          .toArray();

        // MongoDB omits the field for out-of-bounds access
        assert.strictEqual(results[0].result, undefined);
      });

      it('should return null for null array', async () => {
        const collection = client.db(dbName).collection('arrayelemat_null');
        await collection.insertOne({ arr: null });

        const results = await collection
          .aggregate([{ $project: { result: { $arrayElemAt: ['$arr', 0] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should return null for missing field', async () => {
        const collection = client.db(dbName).collection('arrayelemat_missing');
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $arrayElemAt: ['$arr', 0] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$slice (expression)', () => {
      it('should get first n elements with positive n', async () => {
        const collection = client.db(dbName).collection('slice_first');
        await collection.insertOne({ arr: [1, 2, 3, 4, 5] });

        const results = await collection
          .aggregate([{ $project: { result: { $slice: ['$arr', 2] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, [1, 2]);
      });

      it('should get last n elements with negative n', async () => {
        const collection = client.db(dbName).collection('slice_last');
        await collection.insertOne({ arr: [1, 2, 3, 4, 5] });

        const results = await collection
          .aggregate([{ $project: { result: { $slice: ['$arr', -2] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, [4, 5]);
      });

      it('should slice from position with 3-arg form', async () => {
        const collection = client.db(dbName).collection('slice_pos');
        await collection.insertOne({ arr: [1, 2, 3, 4, 5] });

        const results = await collection
          .aggregate([{ $project: { result: { $slice: ['$arr', 1, 2] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, [2, 3]);
      });

      it('should handle negative position in 3-arg form', async () => {
        const collection = client.db(dbName).collection('slice_negpos');
        await collection.insertOne({ arr: [1, 2, 3, 4, 5] });

        const results = await collection
          .aggregate([{ $project: { result: { $slice: ['$arr', -3, 2] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, [3, 4]);
      });

      it('should return null for null array', async () => {
        const collection = client.db(dbName).collection('slice_null');
        await collection.insertOne({ arr: null });

        const results = await collection
          .aggregate([{ $project: { result: { $slice: ['$arr', 2] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$concatArrays', () => {
      it('should concatenate multiple arrays', async () => {
        const collection = client.db(dbName).collection('concat_basic');
        await collection.insertOne({ a: [1, 2], b: [3, 4], c: [5] });

        const results = await collection
          .aggregate([{ $project: { result: { $concatArrays: ['$a', '$b', '$c'] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, [1, 2, 3, 4, 5]);
      });

      it('should return null if any array is null', async () => {
        const collection = client.db(dbName).collection('concat_null');
        await collection.insertOne({ a: [1, 2], b: null });

        const results = await collection
          .aggregate([{ $project: { result: { $concatArrays: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should throw for non-array input', async () => {
        const collection = client.db(dbName).collection('concat_nonarr');
        await collection.insertOne({ a: [1, 2], b: 'not array' });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $concatArrays: ['$a', '$b'] }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$concatArrays only supports arrays'));
            return true;
          }
        );
      });
    });

    describe('$filter', () => {
      it('should filter array elements by condition', async () => {
        const collection = client.db(dbName).collection('filter_basic');
        await collection.insertOne({ scores: [85, 92, 45, 78, 95] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $filter: {
                    input: '$scores',
                    as: 'score',
                    cond: { $gte: ['$$score', 80] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].result, [85, 92, 95]);
      });

      it("should use default 'this' variable when as not specified", async () => {
        const collection = client.db(dbName).collection('filter_default');
        await collection.insertOne({ nums: [1, 2, 3, 4, 5] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $filter: {
                    input: '$nums',
                    cond: { $gt: ['$$this', 2] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].result, [3, 4, 5]);
      });

      it('should return empty array when no elements match', async () => {
        const collection = client.db(dbName).collection('filter_empty');
        await collection.insertOne({ nums: [1, 2, 3] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $filter: {
                    input: '$nums',
                    cond: { $gt: ['$$this', 10] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].result, []);
      });

      it('should return null for null input', async () => {
        const collection = client.db(dbName).collection('filter_null');
        await collection.insertOne({ nums: null });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $filter: {
                    input: '$nums',
                    cond: { $gt: ['$$this', 0] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$map', () => {
      it('should transform each array element', async () => {
        const collection = client.db(dbName).collection('map_basic');
        await collection.insertOne({ nums: [1, 2, 3] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $map: {
                    input: '$nums',
                    as: 'n',
                    in: { $multiply: ['$$n', 2] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].result, [2, 4, 6]);
      });

      it('should handle complex transformations', async () => {
        const collection = client.db(dbName).collection('map_complex');
        await collection.insertOne({ items: [{ x: 1 }, { x: 2 }, { x: 3 }] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $map: {
                    input: '$items',
                    as: 'item',
                    in: '$$item.x',
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].result, [1, 2, 3]);
      });

      it('should return null for null input', async () => {
        const collection = client.db(dbName).collection('map_null');
        await collection.insertOne({ nums: null });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $map: {
                    input: '$nums',
                    as: 'n',
                    in: '$$n',
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$reduce', () => {
      it('should reduce array to single value', async () => {
        const collection = client.db(dbName).collection('reduce_sum');
        await collection.insertOne({ nums: [1, 2, 3, 4, 5] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $reduce: {
                    input: '$nums',
                    initialValue: 0,
                    in: { $add: ['$$value', '$$this'] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, 15);
      });

      it('should return initialValue for empty array', async () => {
        const collection = client.db(dbName).collection('reduce_empty');
        await collection.insertOne({ nums: [] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $reduce: {
                    input: '$nums',
                    initialValue: 100,
                    in: { $add: ['$$value', '$$this'] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, 100);
      });

      it('should concatenate strings', async () => {
        const collection = client.db(dbName).collection('reduce_concat');
        await collection.insertOne({ words: ['hello', ' ', 'world'] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $reduce: {
                    input: '$words',
                    initialValue: '',
                    in: { $concat: ['$$value', '$$this'] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, 'hello world');
      });
    });

    describe('$in (expression)', () => {
      it('should return true when element in array', async () => {
        const collection = client.db(dbName).collection('in_found');
        await collection.insertOne({ arr: [1, 2, 3, 4, 5] });

        const results = await collection
          .aggregate([{ $project: { result: { $in: [3, '$arr'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return false when element not in array', async () => {
        const collection = client.db(dbName).collection('in_notfound');
        await collection.insertOne({ arr: [1, 2, 3] });

        const results = await collection
          .aggregate([{ $project: { result: { $in: [10, '$arr'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should work with strings', async () => {
        const collection = client.db(dbName).collection('in_strings');
        await collection.insertOne({ tags: ['a', 'b', 'c'] });

        const results = await collection
          .aggregate([{ $project: { result: { $in: ['b', '$tags'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should throw for non-array second argument', async () => {
        const collection = client.db(dbName).collection('in_nonarr');
        await collection.insertOne({ value: 'not array' });

        await assert.rejects(
          async () => {
            await collection
              .aggregate([{ $project: { result: { $in: [1, '$value'] }, _id: 0 } }])
              .toArray();
          },
          (err: Error) => {
            assert.ok(err.message.includes('$in requires an array'));
            return true;
          }
        );
      });
    });
  });

  // ==================== Part 4: Type Conversion Operators ====================

  describe('Type Conversion Operators', () => {
    describe('$toInt', () => {
      it('should convert double to int (truncate)', async () => {
        const collection = client.db(dbName).collection('toint_double');
        await collection.insertOne({ value: 3.9 });

        const results = await collection
          .aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 3);
      });

      it('should truncate negative numbers toward zero', async () => {
        const collection = client.db(dbName).collection('toint_neg');
        await collection.insertOne({ value: -3.9 });

        const results = await collection
          .aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, -3);
      });

      it('should convert string to int', async () => {
        const collection = client.db(dbName).collection('toint_string');
        await collection.insertOne({ value: '123' });

        const results = await collection
          .aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 123);
      });

      it('should convert bool true to 1', async () => {
        const collection = client.db(dbName).collection('toint_true');
        await collection.insertOne({ value: true });

        const results = await collection
          .aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1);
      });

      it('should convert bool false to 0', async () => {
        const collection = client.db(dbName).collection('toint_false');
        await collection.insertOne({ value: false });

        const results = await collection
          .aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 0);
      });

      it('should return null for null', async () => {
        const collection = client.db(dbName).collection('toint_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$toDouble', () => {
      it('should convert int to double', async () => {
        const collection = client.db(dbName).collection('todouble_int');
        await collection.insertOne({ value: 5 });

        const results = await collection
          .aggregate([{ $project: { result: { $toDouble: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 5.0);
      });

      it('should convert string to double', async () => {
        const collection = client.db(dbName).collection('todouble_string');
        await collection.insertOne({ value: '3.14' });

        const results = await collection
          .aggregate([{ $project: { result: { $toDouble: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 3.14);
      });

      it('should convert bool to double', async () => {
        const collection = client.db(dbName).collection('todouble_bool');
        await collection.insertOne({ value: true });

        const results = await collection
          .aggregate([{ $project: { result: { $toDouble: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1.0);
      });

      it('should return null for null', async () => {
        const collection = client.db(dbName).collection('todouble_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $toDouble: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$toBool', () => {
      it('should convert 0 to false', async () => {
        const collection = client.db(dbName).collection('tobool_zero');
        await collection.insertOne({ value: 0 });

        const results = await collection
          .aggregate([{ $project: { result: { $toBool: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should convert non-zero to true', async () => {
        const collection = client.db(dbName).collection('tobool_nonzero');
        await collection.insertOne({ value: 42 });

        const results = await collection
          .aggregate([{ $project: { result: { $toBool: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should convert empty string to true', async () => {
        // Critical: MongoDB treats ALL strings as truthy
        const collection = client.db(dbName).collection('tobool_emptystr');
        await collection.insertOne({ value: '' });

        const results = await collection
          .aggregate([{ $project: { result: { $toBool: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should convert non-empty string to true', async () => {
        const collection = client.db(dbName).collection('tobool_string');
        await collection.insertOne({ value: 'hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $toBool: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return null for null', async () => {
        const collection = client.db(dbName).collection('tobool_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $toBool: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$toDate', () => {
      it('should convert epoch milliseconds to date', async () => {
        const collection = client.db(dbName).collection('todate_epoch');
        // Jan 1, 2020 00:00:00 UTC = 1577836800000
        await collection.insertOne({ value: 1577836800000 });

        const results = await collection
          .aggregate([{ $project: { result: { $toDate: '$value' }, _id: 0 } }])
          .toArray();

        assert.ok(results[0].result instanceof Date);
        assert.strictEqual((results[0].result as Date).getUTCFullYear(), 2020);
      });

      it('should convert ISO string to date', async () => {
        const collection = client.db(dbName).collection('todate_string');
        await collection.insertOne({ value: '2020-01-15T10:30:00Z' });

        const results = await collection
          .aggregate([{ $project: { result: { $toDate: '$value' }, _id: 0 } }])
          .toArray();

        assert.ok(results[0].result instanceof Date);
      });

      it('should return null for null', async () => {
        const collection = client.db(dbName).collection('todate_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $toDate: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$type', () => {
      it("should return 'double' for numbers", async () => {
        const collection = client.db(dbName).collection('type_double');
        await collection.insertOne({ value: 3.14 });

        const results = await collection
          .aggregate([{ $project: { result: { $type: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'double');
      });

      it("should return 'string' for strings", async () => {
        const collection = client.db(dbName).collection('type_string');
        await collection.insertOne({ value: 'hello' });

        const results = await collection
          .aggregate([{ $project: { result: { $type: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'string');
      });

      it("should return 'bool' for booleans", async () => {
        const collection = client.db(dbName).collection('type_bool');
        await collection.insertOne({ value: true });

        const results = await collection
          .aggregate([{ $project: { result: { $type: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'bool');
      });

      it("should return 'array' for arrays", async () => {
        const collection = client.db(dbName).collection('type_array');
        await collection.insertOne({ value: [1, 2, 3] });

        const results = await collection
          .aggregate([{ $project: { result: { $type: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'array');
      });

      it("should return 'object' for objects", async () => {
        const collection = client.db(dbName).collection('type_object');
        await collection.insertOne({ value: { a: 1 } });

        const results = await collection
          .aggregate([{ $project: { result: { $type: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'object');
      });

      it("should return 'null' for null", async () => {
        const collection = client.db(dbName).collection('type_null');
        await collection.insertOne({ value: null });

        const results = await collection
          .aggregate([{ $project: { result: { $type: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'null');
      });

      it("should return 'missing' for missing field", async () => {
        const collection = client.db(dbName).collection('type_missing');
        await collection.insertOne({ other: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $type: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'missing');
      });

      it("should return 'date' for dates", async () => {
        const collection = client.db(dbName).collection('type_date');
        await collection.insertOne({ value: new Date() });

        const results = await collection
          .aggregate([{ $project: { result: { $type: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 'date');
      });
    });
  });

  // ==================== Part 5: Date Operators ====================

  describe('Date Operators', () => {
    describe('$year', () => {
      it('should extract year from date', async () => {
        const collection = client.db(dbName).collection('year_basic');
        await collection.insertOne({ date: new Date('2023-06-15T10:30:00Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $year: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 2023);
      });

      it('should return null for null date', async () => {
        const collection = client.db(dbName).collection('year_null');
        await collection.insertOne({ date: null });

        const results = await collection
          .aggregate([{ $project: { result: { $year: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$month', () => {
      it('should extract month (1-12) from date', async () => {
        const collection = client.db(dbName).collection('month_basic');
        await collection.insertOne({ date: new Date('2023-06-15T10:30:00Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $month: '$date' }, _id: 0 } }])
          .toArray();

        // June = 6
        assert.strictEqual(results[0].result, 6);
      });

      it('should return null for null date', async () => {
        const collection = client.db(dbName).collection('month_null');
        await collection.insertOne({ date: null });

        const results = await collection
          .aggregate([{ $project: { result: { $month: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });
    });

    describe('$dayOfMonth', () => {
      it('should extract day of month from date', async () => {
        const collection = client.db(dbName).collection('dayofmonth_basic');
        await collection.insertOne({ date: new Date('2023-06-15T10:30:00Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $dayOfMonth: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 15);
      });
    });

    describe('$hour', () => {
      it('should extract hour (0-23) from date', async () => {
        const collection = client.db(dbName).collection('hour_basic');
        await collection.insertOne({ date: new Date('2023-06-15T14:30:00Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $hour: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 14);
      });
    });

    describe('$minute', () => {
      it('should extract minute (0-59) from date', async () => {
        const collection = client.db(dbName).collection('minute_basic');
        await collection.insertOne({ date: new Date('2023-06-15T14:45:00Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $minute: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 45);
      });
    });

    describe('$second', () => {
      it('should extract second (0-59) from date', async () => {
        const collection = client.db(dbName).collection('second_basic');
        await collection.insertOne({ date: new Date('2023-06-15T14:45:30Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $second: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 30);
      });
    });

    describe('$dayOfWeek', () => {
      it('should return 1 for Sunday', async () => {
        const collection = client.db(dbName).collection('dow_sunday');
        // June 18, 2023 was a Sunday
        await collection.insertOne({ date: new Date('2023-06-18T10:00:00Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $dayOfWeek: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1);
      });

      it('should return 7 for Saturday', async () => {
        const collection = client.db(dbName).collection('dow_saturday');
        // June 17, 2023 was a Saturday
        await collection.insertOne({ date: new Date('2023-06-17T10:00:00Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $dayOfWeek: '$date' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 7);
      });
    });

    describe('$dateToString', () => {
      it('should format date with default ISO format', async () => {
        const collection = client.db(dbName).collection('datetostring_default');
        await collection.insertOne({ date: new Date('2023-06-15T14:30:45.123Z') });

        const results = await collection
          .aggregate([{ $project: { result: { $dateToString: { date: '$date' } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, '2023-06-15T14:30:45.123Z');
      });

      it('should format date with custom format', async () => {
        const collection = client.db(dbName).collection('datetostring_custom');
        await collection.insertOne({ date: new Date('2023-06-15T14:30:45Z') });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: { $dateToString: { format: '%Y-%m-%d', date: '$date' } },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, '2023-06-15');
      });

      it('should return onNull value for null date', async () => {
        const collection = client.db(dbName).collection('datetostring_onnull');
        await collection.insertOne({ date: null });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $dateToString: { date: '$date', onNull: 'No date' },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, 'No date');
      });

      it('should return null when date is null and no onNull', async () => {
        const collection = client.db(dbName).collection('datetostring_null');
        await collection.insertOne({ date: null });

        const results = await collection
          .aggregate([{ $project: { result: { $dateToString: { date: '$date' } }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should format with time components', async () => {
        const collection = client.db(dbName).collection('datetostring_time');
        await collection.insertOne({ date: new Date('2023-06-15T14:30:45Z') });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $dateToString: { format: '%H:%M:%S', date: '$date' },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, '14:30:45');
      });

      it('should format with day of year (%j)', async () => {
        const collection = client.db(dbName).collection('datetostring_dayofyear');
        // June 15 is day 166 of the year (non-leap year)
        await collection.insertOne({ date: new Date('2023-06-15T00:00:00Z') });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $dateToString: { format: '%j', date: '$date' },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, '166');
      });

      it('should format with day of week (%w and %u)', async () => {
        const collection = client.db(dbName).collection('datetostring_dayofweek');
        // 2023-06-15 is a Thursday
        await collection.insertOne({ date: new Date('2023-06-15T00:00:00Z') });

        const results = await collection
          .aggregate([
            {
              $project: {
                w: { $dateToString: { format: '%w', date: '$date' } },
                u: { $dateToString: { format: '%u', date: '$date' } },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].w, '5'); // Thursday is 5 (Sunday=1)
        assert.strictEqual(results[0].u, '4'); // Thursday is 4 (Monday=1)
      });

      it('should return non-string onNull value as-is', async () => {
        const collection = client.db(dbName).collection('datetostring_onnull_convert');
        await collection.insertOne({ date: null });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: { $dateToString: { date: '$date', onNull: 123 } },
                _id: 0,
              },
            },
          ])
          .toArray();

        // MongoDB returns onNull value as-is (not converted to string)
        assert.strictEqual(results[0].result, 123);
      });
    });
  });

  describe('Edge Cases', () => {
    describe('$toInt edge cases', () => {
      it('should throw for Infinity string', async () => {
        const collection = client.db(dbName).collection('toint_infinity');
        await collection.insertOne({ value: 'Infinity' });

        await assert.rejects(
          collection.aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }]).toArray(),
          (err: Error) => err.message.includes('Failed to parse')
        );
      });

      it('should throw for -Infinity string', async () => {
        const collection = client.db(dbName).collection('toint_neginfinity');
        await collection.insertOne({ value: '-Infinity' });

        await assert.rejects(
          collection.aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }]).toArray(),
          (err: Error) => err.message.includes('Failed to parse')
        );
      });

      it('should throw for NaN string', async () => {
        const collection = client.db(dbName).collection('toint_nan');
        await collection.insertOne({ value: 'NaN' });

        await assert.rejects(
          collection.aggregate([{ $project: { result: { $toInt: '$value' }, _id: 0 } }]).toArray(),
          (err: Error) => err.message.includes('Failed to parse')
        );
      });
    });

    describe('$toDouble edge cases', () => {
      it('should parse Infinity string', async () => {
        const collection = client.db(dbName).collection('todouble_infinity');
        await collection.insertOne({ value: 'Infinity' });

        const results = await collection
          .aggregate([{ $project: { result: { $toDouble: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, Infinity);
      });

      it('should parse -Infinity string', async () => {
        const collection = client.db(dbName).collection('todouble_neginfinity');
        await collection.insertOne({ value: '-Infinity' });

        const results = await collection
          .aggregate([{ $project: { result: { $toDouble: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, -Infinity);
      });

      it('should parse scientific notation correctly', async () => {
        const collection = client.db(dbName).collection('todouble_scientific');
        await collection.insertOne({ value: '1.5e3' });

        const results = await collection
          .aggregate([{ $project: { result: { $toDouble: '$value' }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, 1500);
      });
    });

    describe('$map with optional as parameter', () => {
      it("should use default variable name 'this' when as not specified", async () => {
        const collection = client.db(dbName).collection('map_default_var');
        await collection.insertOne({ values: [1, 2, 3] });

        const results = await collection
          .aggregate([
            {
              $project: {
                doubled: {
                  $map: {
                    input: '$values',
                    in: { $multiply: ['$$this', 2] },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].doubled, [2, 4, 6]);
      });
    });

    describe('Date operator edge cases', () => {
      it('$year should throw for invalid date strings via $toDate', async () => {
        const collection = client.db(dbName).collection('year_invalid');
        await collection.insertOne({ value: 'not-a-date' });

        await assert.rejects(
          collection
            .aggregate([
              {
                $project: {
                  result: { $year: { $toDate: '$value' } },
                  _id: 0,
                },
              },
            ])
            .toArray(),
          (err: Error) => err.message.includes('Error parsing date string')
        );
      });

      it('$dateToString should throw for non-date types', async () => {
        const collection = client.db(dbName).collection('datetostring_err');
        await collection.insertOne({ value: 'not-a-date' });

        await assert.rejects(
          collection
            .aggregate([
              {
                $project: {
                  result: { $dateToString: { date: '$value' } },
                  _id: 0,
                },
              },
            ])
            .toArray(),
          (err: Error) => err.message.includes("can't convert from BSON type string to Date")
        );
      });

      it('should handle epoch milliseconds for dates before 1970', async () => {
        const collection = client.db(dbName).collection('date_before_epoch');
        // December 31, 1969
        await collection.insertOne({ date: new Date('1969-12-31T00:00:00Z') });

        const results = await collection
          .aggregate([
            {
              $project: {
                year: { $year: '$date' },
                month: { $month: '$date' },
                day: { $dayOfMonth: '$date' },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].year, 1969);
        assert.strictEqual(results[0].month, 12);
        assert.strictEqual(results[0].day, 31);
      });
    });

    describe('Variable scoping edge cases', () => {
      it('should handle nested $map with different variable names', async () => {
        const collection = client.db(dbName).collection('nested_map');
        await collection.insertOne({
          matrix: [
            [1, 2],
            [3, 4],
          ],
        });

        const results = await collection
          .aggregate([
            {
              $project: {
                doubled: {
                  $map: {
                    input: '$matrix',
                    as: 'row',
                    in: {
                      $map: {
                        input: '$$row',
                        as: 'cell',
                        in: { $multiply: ['$$cell', 2] },
                      },
                    },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].doubled, [
          [2, 4],
          [6, 8],
        ]);
      });

      it('should allow variable shadowing in nested operators', async () => {
        const collection = client.db(dbName).collection('var_shadowing');
        await collection.insertOne({
          outer: [{ inner: [1, 2] }, { inner: [3, 4] }],
        });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: {
                  $map: {
                    input: '$outer',
                    as: 'item',
                    in: {
                      $filter: {
                        input: '$$item.inner',
                        as: 'item', // Shadows outer 'item'
                        cond: { $gt: ['$$item', 2] },
                      },
                    },
                  },
                },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].result, [[], [3, 4]]);
      });
    });

    describe('Unicode string handling', () => {
      it('should handle emoji in $strLenCP', async () => {
        const collection = client.db(dbName).collection('strlen_emoji');
        await collection.insertOne({ value: 'Hello 👋' });

        const results = await collection
          .aggregate([{ $project: { len: { $strLenCP: '$value' }, _id: 0 } }])
          .toArray();

        // "Hello " is 6 chars, 👋 is 2 code units in JS but may count as 1-2 chars
        // JavaScript string length for "Hello 👋" is 8 (emoji is 2 UTF-16 code units)
        assert.ok((results[0] as { len: number }).len >= 7); // At least 7 characters
      });

      it('should handle CJK characters in $split', async () => {
        const collection = client.db(dbName).collection('split_cjk');
        await collection.insertOne({ value: '你好,世界' });

        const results = await collection
          .aggregate([{ $project: { parts: { $split: ['$value', ','] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].parts, ['你好', '世界']);
      });
    });
  });

  // ==================== Part 5: Set Operators ====================

  describe('Set Operators', () => {
    describe('$setUnion', () => {
      it('should return union of two arrays', async () => {
        const collection = client.db(dbName).collection('setunion_basic');
        await collection.insertOne({ a: [1, 2, 3], b: [3, 4, 5] });

        const results = await collection
          .aggregate([{ $project: { result: { $setUnion: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        const result = results[0].result as number[];
        assert.strictEqual(result.length, 5);
        assert.ok([1, 2, 3, 4, 5].every((v) => result.includes(v)));
      });

      it('should handle duplicates within arrays', async () => {
        const collection = client.db(dbName).collection('setunion_dupes');
        await collection.insertOne({ a: [1, 1, 2], b: [2, 2, 3] });

        const results = await collection
          .aggregate([{ $project: { result: { $setUnion: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        const result = results[0].result as number[];
        assert.strictEqual(result.length, 3);
        assert.ok([1, 2, 3].every((v) => result.includes(v)));
      });

      it('should handle empty arrays', async () => {
        const collection = client.db(dbName).collection('setunion_empty');
        await collection.insertOne({ a: [], b: [1, 2] });

        const results = await collection
          .aggregate([{ $project: { result: { $setUnion: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        const result = results[0].result as number[];
        assert.deepStrictEqual(result.sort(), [1, 2]);
      });

      it('should return null if any array is null', async () => {
        const collection = client.db(dbName).collection('setunion_null');
        await collection.insertOne({ a: [1, 2], b: null });

        const results = await collection
          .aggregate([{ $project: { result: { $setUnion: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should handle more than two arrays', async () => {
        const collection = client.db(dbName).collection('setunion_multi');
        await collection.insertOne({ a: [1], b: [2], c: [3] });

        const results = await collection
          .aggregate([{ $project: { result: { $setUnion: ['$a', '$b', '$c'] }, _id: 0 } }])
          .toArray();

        const result = results[0].result as number[];
        assert.strictEqual(result.length, 3);
      });
    });

    describe('$setIntersection', () => {
      it('should return common elements of two arrays', async () => {
        const collection = client.db(dbName).collection('setintersect_basic');
        await collection.insertOne({ a: [1, 2, 3], b: [2, 3, 4] });

        const results = await collection
          .aggregate([{ $project: { result: { $setIntersection: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        const result = results[0].result as number[];
        assert.strictEqual(result.length, 2);
        assert.ok([2, 3].every((v) => result.includes(v)));
      });

      it('should return empty array when no common elements', async () => {
        const collection = client.db(dbName).collection('setintersect_none');
        await collection.insertOne({ a: [1, 2], b: [3, 4] });

        const results = await collection
          .aggregate([{ $project: { result: { $setIntersection: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, []);
      });

      it('should return null if any array is null', async () => {
        const collection = client.db(dbName).collection('setintersect_null');
        await collection.insertOne({ a: null, b: [1, 2] });

        const results = await collection
          .aggregate([{ $project: { result: { $setIntersection: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should handle multiple arrays', async () => {
        const collection = client.db(dbName).collection('setintersect_multi');
        await collection.insertOne({ a: [1, 2, 3], b: [2, 3, 4], c: [3, 4, 5] });

        const results = await collection
          .aggregate([
            {
              $project: {
                result: { $setIntersection: ['$a', '$b', '$c'] },
                _id: 0,
              },
            },
          ])
          .toArray();

        assert.deepStrictEqual(results[0].result, [3]);
      });
    });

    describe('$setDifference', () => {
      it('should return elements in first but not second array', async () => {
        const collection = client.db(dbName).collection('setdiff_basic');
        await collection.insertOne({ a: [1, 2, 3], b: [2, 3, 4] });

        const results = await collection
          .aggregate([{ $project: { result: { $setDifference: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, [1]);
      });

      it('should return empty array when all elements are in second', async () => {
        const collection = client.db(dbName).collection('setdiff_all');
        await collection.insertOne({ a: [1, 2], b: [1, 2, 3] });

        const results = await collection
          .aggregate([{ $project: { result: { $setDifference: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.deepStrictEqual(results[0].result, []);
      });

      it('should return null if any array is null', async () => {
        const collection = client.db(dbName).collection('setdiff_null');
        await collection.insertOne({ a: [1, 2], b: null });

        const results = await collection
          .aggregate([{ $project: { result: { $setDifference: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, null);
      });

      it('should handle duplicates', async () => {
        const collection = client.db(dbName).collection('setdiff_dupes');
        await collection.insertOne({ a: [1, 1, 2, 2, 3], b: [2] });

        const results = await collection
          .aggregate([{ $project: { result: { $setDifference: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        const result = results[0].result as number[];
        assert.strictEqual(result.length, 2);
        assert.ok([1, 3].every((v) => result.includes(v)));
      });
    });

    describe('$setEquals', () => {
      it('should return true for equal sets', async () => {
        const collection = client.db(dbName).collection('setequals_true');
        await collection.insertOne({ a: [1, 2, 3], b: [3, 2, 1] });

        const results = await collection
          .aggregate([{ $project: { result: { $setEquals: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return false for unequal sets', async () => {
        const collection = client.db(dbName).collection('setequals_false');
        await collection.insertOne({ a: [1, 2], b: [1, 2, 3] });

        const results = await collection
          .aggregate([{ $project: { result: { $setEquals: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should handle duplicates', async () => {
        const collection = client.db(dbName).collection('setequals_dupes');
        await collection.insertOne({ a: [1, 1, 2], b: [1, 2, 2] });

        const results = await collection
          .aggregate([{ $project: { result: { $setEquals: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should throw error if any array is null', async () => {
        const collection = client.db(dbName).collection('setequals_null');
        await collection.insertOne({ a: [1, 2], b: null });

        await assert.rejects(async () => {
          await collection
            .aggregate([{ $project: { result: { $setEquals: ['$a', '$b'] }, _id: 0 } }])
            .toArray();
        }, /All operands of \$setEquals must be arrays/);
      });

      it('should compare multiple arrays', async () => {
        const collection = client.db(dbName).collection('setequals_multi');
        await collection.insertOne({ a: [1, 2], b: [2, 1], c: [1, 2] });

        const results = await collection
          .aggregate([
            {
              $project: { result: { $setEquals: ['$a', '$b', '$c'] }, _id: 0 },
            },
          ])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });
    });

    describe('$setIsSubset', () => {
      it('should return true when first is subset of second', async () => {
        const collection = client.db(dbName).collection('setsubset_true');
        await collection.insertOne({ a: [1, 2], b: [1, 2, 3, 4] });

        const results = await collection
          .aggregate([{ $project: { result: { $setIsSubset: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return false when first is not subset of second', async () => {
        const collection = client.db(dbName).collection('setsubset_false');
        await collection.insertOne({ a: [1, 2, 5], b: [1, 2, 3] });

        const results = await collection
          .aggregate([{ $project: { result: { $setIsSubset: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should return true for empty first array', async () => {
        const collection = client.db(dbName).collection('setsubset_empty');
        await collection.insertOne({ a: [], b: [1, 2] });

        const results = await collection
          .aggregate([{ $project: { result: { $setIsSubset: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should throw error if any array is null', async () => {
        const collection = client.db(dbName).collection('setsubset_null');
        await collection.insertOne({ a: null, b: [1, 2] });

        await assert.rejects(async () => {
          await collection
            .aggregate([{ $project: { result: { $setIsSubset: ['$a', '$b'] }, _id: 0 } }])
            .toArray();
        }, /both operands of \$setIsSubset must be arrays/);
      });

      it('should return true when sets are equal', async () => {
        const collection = client.db(dbName).collection('setsubset_equal');
        await collection.insertOne({ a: [1, 2], b: [1, 2] });

        const results = await collection
          .aggregate([{ $project: { result: { $setIsSubset: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });
    });

    describe('$allElementsTrue', () => {
      it('should return true when all elements are truthy', async () => {
        const collection = client.db(dbName).collection('allelements_true');
        await collection.insertOne({ a: [1, 'hello', true, [1]] });

        const results = await collection
          .aggregate([{ $project: { result: { $allElementsTrue: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return false when any element is falsy', async () => {
        const collection = client.db(dbName).collection('allelements_false');
        await collection.insertOne({ a: [1, 0, true] });

        const results = await collection
          .aggregate([{ $project: { result: { $allElementsTrue: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should return true for empty array', async () => {
        const collection = client.db(dbName).collection('allelements_empty');
        await collection.insertOne({ a: [] });

        const results = await collection
          .aggregate([{ $project: { result: { $allElementsTrue: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should treat null as falsy', async () => {
        const collection = client.db(dbName).collection('allelements_null');
        await collection.insertOne({ a: [1, null, 2] });

        const results = await collection
          .aggregate([{ $project: { result: { $allElementsTrue: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });
    });

    describe('$anyElementTrue', () => {
      it('should return true when any element is truthy', async () => {
        const collection = client.db(dbName).collection('anyelement_true');
        await collection.insertOne({ a: [0, false, 1] });

        const results = await collection
          .aggregate([{ $project: { result: { $anyElementTrue: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return false when all elements are falsy', async () => {
        const collection = client.db(dbName).collection('anyelement_false');
        await collection.insertOne({ a: [0, false, null] });

        const results = await collection
          .aggregate([{ $project: { result: { $anyElementTrue: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should return false for empty array', async () => {
        const collection = client.db(dbName).collection('anyelement_empty');
        await collection.insertOne({ a: [] });

        const results = await collection
          .aggregate([{ $project: { result: { $anyElementTrue: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should treat non-zero numbers as truthy', async () => {
        const collection = client.db(dbName).collection('anyelement_num');
        await collection.insertOne({ a: [0, 0, -1] });

        const results = await collection
          .aggregate([{ $project: { result: { $anyElementTrue: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });
    });
  });

  // ==================== Boolean/Logical Operators ====================

  describe('Boolean/Logical Operators', () => {
    describe('$and', () => {
      it('should return true when all expressions are truthy', async () => {
        const collection = client.db(dbName).collection('and_all_true');
        await collection.insertOne({ a: 1, b: 'hello', c: true });

        const results = await collection
          .aggregate([{ $project: { result: { $and: ['$a', '$b', '$c'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return false when any expression is falsy', async () => {
        const collection = client.db(dbName).collection('and_one_false');
        await collection.insertOne({ a: 1, b: 0, c: true });

        const results = await collection
          .aggregate([{ $project: { result: { $and: ['$a', '$b', '$c'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should return true for empty array', async () => {
        const collection = client.db(dbName).collection('and_empty');
        await collection.insertOne({ a: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $and: [] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should treat null as falsy', async () => {
        const collection = client.db(dbName).collection('and_null');
        await collection.insertOne({ a: 1, b: null });

        const results = await collection
          .aggregate([{ $project: { result: { $and: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should treat 0 as falsy', async () => {
        const collection = client.db(dbName).collection('and_zero');
        await collection.insertOne({ a: 1, b: 0 });

        const results = await collection
          .aggregate([{ $project: { result: { $and: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should treat empty string as truthy', async () => {
        const collection = client.db(dbName).collection('and_empty_str');
        await collection.insertOne({ a: 1, b: '' });

        const results = await collection
          .aggregate([{ $project: { result: { $and: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should treat empty array as truthy', async () => {
        const collection = client.db(dbName).collection('and_empty_arr');
        await collection.insertOne({ a: 1, b: [] });

        const results = await collection
          .aggregate([{ $project: { result: { $and: ['$a', '$b'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });
    });

    describe('$or', () => {
      it('should return true when any expression is truthy', async () => {
        const collection = client.db(dbName).collection('or_one_true');
        await collection.insertOne({ a: 0, b: null, c: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $or: ['$a', '$b', '$c'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return false when all expressions are falsy', async () => {
        const collection = client.db(dbName).collection('or_all_false');
        await collection.insertOne({ a: 0, b: null, c: false });

        const results = await collection
          .aggregate([{ $project: { result: { $or: ['$a', '$b', '$c'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should return false for empty array', async () => {
        const collection = client.db(dbName).collection('or_empty');
        await collection.insertOne({ a: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $or: [] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });
    });

    describe('$not', () => {
      it('should return true for falsy value', async () => {
        const collection = client.db(dbName).collection('not_false');
        await collection.insertOne({ a: false });

        const results = await collection
          .aggregate([{ $project: { result: { $not: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return false for truthy value', async () => {
        const collection = client.db(dbName).collection('not_true');
        await collection.insertOne({ a: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $not: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, false);
      });

      it('should return true for 0', async () => {
        const collection = client.db(dbName).collection('not_zero');
        await collection.insertOne({ a: 0 });

        const results = await collection
          .aggregate([{ $project: { result: { $not: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return true for null', async () => {
        const collection = client.db(dbName).collection('not_null');
        await collection.insertOne({ a: null });

        const results = await collection
          .aggregate([{ $project: { result: { $not: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });

      it('should return true for missing field', async () => {
        const collection = client.db(dbName).collection('not_missing');
        await collection.insertOne({ b: 1 });

        const results = await collection
          .aggregate([{ $project: { result: { $not: ['$a'] }, _id: 0 } }])
          .toArray();

        assert.strictEqual(results[0].result, true);
      });
    });
  });
});
