/**
 * Trigonometry Expression Operators Tests
 *
 * Tests for trigonometric aggregation operators: $sin, $cos, $tan,
 * $asin, $acos, $atan, $atan2, $sinh, $cosh, $tanh, $asinh, $acosh, $atanh,
 * $degreesToRadians, $radiansToDegrees.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../../test-harness.ts';

interface TrigResult {
  result: number | null;
}

describe(`Trigonometry Operators (${getTestModeName()})`, () => {
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

  // ==================== Basic Trigonometric Functions ====================

  describe('$sin', () => {
    it('should return 0 for sin(0)', async () => {
      const collection = client.db(dbName).collection('sin_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $sin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should return 1 for sin(PI/2)', async () => {
      const collection = client.db(dbName).collection('sin_pi2');
      await collection.insertOne({ value: Math.PI / 2 });

      const results = (await collection
        .aggregate([{ $project: { result: { $sin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - 1) < 1e-10);
    });

    it('should return null for null input', async () => {
      const collection = client.db(dbName).collection('sin_null');
      await collection.insertOne({ value: null });

      const results = (await collection
        .aggregate([{ $project: { result: { $sin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });

    it('should return null for missing field', async () => {
      const collection = client.db(dbName).collection('sin_missing');
      await collection.insertOne({ other: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $sin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });

    it('should throw for non-numeric input', async () => {
      const collection = client.db(dbName).collection('sin_string');
      await collection.insertOne({ value: 'hello' });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $sin: '$value' }, _id: 0 } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes('$sin only supports numeric types'));
          return true;
        }
      );
    });

    // Note: MangoDB stores Infinity as null in JSON, so this tests null handling
    // In real MongoDB, this would throw an error for Infinity
    it('should return null when stored Infinity becomes null', async () => {
      const collection = client.db(dbName).collection('sin_infinity');
      await collection.insertOne({ value: Infinity });

      const results = (await collection
        .aggregate([{ $project: { result: { $sin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      // MangoDB stores Infinity as null, so result is null
      assert.strictEqual(results[0].result, null);
    });
  });

  describe('$cos', () => {
    it('should return 1 for cos(0)', async () => {
      const collection = client.db(dbName).collection('cos_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $cos: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 1);
    });

    it('should return 0 for cos(PI/2)', async () => {
      const collection = client.db(dbName).collection('cos_pi2');
      await collection.insertOne({ value: Math.PI / 2 });

      const results = (await collection
        .aggregate([{ $project: { result: { $cos: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result!) < 1e-10);
    });

    it('should return -1 for cos(PI)', async () => {
      const collection = client.db(dbName).collection('cos_pi');
      await collection.insertOne({ value: Math.PI });

      const results = (await collection
        .aggregate([{ $project: { result: { $cos: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - -1) < 1e-10);
    });

    it('should return null for null input', async () => {
      const collection = client.db(dbName).collection('cos_null');
      await collection.insertOne({ value: null });

      const results = (await collection
        .aggregate([{ $project: { result: { $cos: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });
  });

  describe('$tan', () => {
    it('should return 0 for tan(0)', async () => {
      const collection = client.db(dbName).collection('tan_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $tan: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should return 1 for tan(PI/4)', async () => {
      const collection = client.db(dbName).collection('tan_pi4');
      await collection.insertOne({ value: Math.PI / 4 });

      const results = (await collection
        .aggregate([{ $project: { result: { $tan: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - 1) < 1e-10);
    });

    it('should return null for null input', async () => {
      const collection = client.db(dbName).collection('tan_null');
      await collection.insertOne({ value: null });

      const results = (await collection
        .aggregate([{ $project: { result: { $tan: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });
  });

  // ==================== Inverse Trigonometric Functions ====================

  describe('$asin', () => {
    it('should return 0 for asin(0)', async () => {
      const collection = client.db(dbName).collection('asin_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $asin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should return PI/2 for asin(1)', async () => {
      const collection = client.db(dbName).collection('asin_one');
      await collection.insertOne({ value: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $asin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.PI / 2) < 1e-10);
    });

    it('should return NaN for values outside [-1, 1]', async () => {
      const collection = client.db(dbName).collection('asin_outside');
      await collection.insertOne({ value: 2 });

      const results = (await collection
        .aggregate([{ $project: { result: { $asin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Number.isNaN(results[0].result));
    });

    it('should return null for null input', async () => {
      const collection = client.db(dbName).collection('asin_null');
      await collection.insertOne({ value: null });

      const results = (await collection
        .aggregate([{ $project: { result: { $asin: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });
  });

  describe('$acos', () => {
    it('should return PI/2 for acos(0)', async () => {
      const collection = client.db(dbName).collection('acos_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $acos: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.PI / 2) < 1e-10);
    });

    it('should return 0 for acos(1)', async () => {
      const collection = client.db(dbName).collection('acos_one');
      await collection.insertOne({ value: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $acos: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should return NaN for values outside [-1, 1]', async () => {
      const collection = client.db(dbName).collection('acos_outside');
      await collection.insertOne({ value: 2 });

      const results = (await collection
        .aggregate([{ $project: { result: { $acos: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Number.isNaN(results[0].result));
    });
  });

  describe('$atan', () => {
    it('should return 0 for atan(0)', async () => {
      const collection = client.db(dbName).collection('atan_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atan: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should return PI/4 for atan(1)', async () => {
      const collection = client.db(dbName).collection('atan_one');
      await collection.insertOne({ value: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atan: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.PI / 4) < 1e-10);
    });

    // Note: MangoDB stores Infinity as null in JSON
    // In real MongoDB, atan(Infinity) returns PI/2
    it('should return null when stored Infinity becomes null', async () => {
      const collection = client.db(dbName).collection('atan_infinity');
      await collection.insertOne({ value: Infinity });

      const results = (await collection
        .aggregate([{ $project: { result: { $atan: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      // MangoDB stores Infinity as null, so result is null
      assert.strictEqual(results[0].result, null);
    });
  });

  describe('$atan2', () => {
    it('should return 0 for atan2(0, 1)', async () => {
      const collection = client.db(dbName).collection('atan2_zero');
      await collection.insertOne({ y: 0, x: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atan2: ['$y', '$x'] }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should return PI/4 for atan2(1, 1)', async () => {
      const collection = client.db(dbName).collection('atan2_one');
      await collection.insertOne({ y: 1, x: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atan2: ['$y', '$x'] }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.PI / 4) < 1e-10);
    });

    it('should return PI/2 for atan2(1, 0)', async () => {
      const collection = client.db(dbName).collection('atan2_vertical');
      await collection.insertOne({ y: 1, x: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atan2: ['$y', '$x'] }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.PI / 2) < 1e-10);
    });

    it('should return null if either arg is null', async () => {
      const collection = client.db(dbName).collection('atan2_null');
      await collection.insertOne({ y: null, x: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atan2: ['$y', '$x'] }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });
  });

  // ==================== Hyperbolic Functions ====================

  describe('$sinh', () => {
    it('should return 0 for sinh(0)', async () => {
      const collection = client.db(dbName).collection('sinh_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $sinh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should handle positive values', async () => {
      const collection = client.db(dbName).collection('sinh_positive');
      await collection.insertOne({ value: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $sinh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.sinh(1)) < 1e-10);
    });

    it('should return null for null input', async () => {
      const collection = client.db(dbName).collection('sinh_null');
      await collection.insertOne({ value: null });

      const results = (await collection
        .aggregate([{ $project: { result: { $sinh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });
  });

  describe('$cosh', () => {
    it('should return 1 for cosh(0)', async () => {
      const collection = client.db(dbName).collection('cosh_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $cosh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 1);
    });

    it('should handle positive values', async () => {
      const collection = client.db(dbName).collection('cosh_positive');
      await collection.insertOne({ value: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $cosh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.cosh(1)) < 1e-10);
    });
  });

  describe('$tanh', () => {
    it('should return 0 for tanh(0)', async () => {
      const collection = client.db(dbName).collection('tanh_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $tanh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should approach 1 for large positive values', async () => {
      const collection = client.db(dbName).collection('tanh_large');
      await collection.insertOne({ value: 100 });

      const results = (await collection
        .aggregate([{ $project: { result: { $tanh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - 1) < 1e-10);
    });
  });

  // ==================== Inverse Hyperbolic Functions ====================

  describe('$asinh', () => {
    it('should return 0 for asinh(0)', async () => {
      const collection = client.db(dbName).collection('asinh_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $asinh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should handle positive values', async () => {
      const collection = client.db(dbName).collection('asinh_positive');
      await collection.insertOne({ value: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $asinh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.asinh(1)) < 1e-10);
    });
  });

  describe('$acosh', () => {
    it('should return 0 for acosh(1)', async () => {
      const collection = client.db(dbName).collection('acosh_one');
      await collection.insertOne({ value: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $acosh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should handle values > 1', async () => {
      const collection = client.db(dbName).collection('acosh_two');
      await collection.insertOne({ value: 2 });

      const results = (await collection
        .aggregate([{ $project: { result: { $acosh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.acosh(2)) < 1e-10);
    });

    it('should return NaN for values < 1', async () => {
      const collection = client.db(dbName).collection('acosh_invalid');
      await collection.insertOne({ value: 0.5 });

      const results = (await collection
        .aggregate([{ $project: { result: { $acosh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Number.isNaN(results[0].result));
    });
  });

  describe('$atanh', () => {
    it('should return 0 for atanh(0)', async () => {
      const collection = client.db(dbName).collection('atanh_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atanh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should handle values in (-1, 1)', async () => {
      const collection = client.db(dbName).collection('atanh_half');
      await collection.insertOne({ value: 0.5 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atanh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.atanh(0.5)) < 1e-10);
    });

    it('should return Infinity for atanh(1)', async () => {
      const collection = client.db(dbName).collection('atanh_one');
      await collection.insertOne({ value: 1 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atanh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, Infinity);
    });

    it('should return NaN for values outside (-1, 1)', async () => {
      const collection = client.db(dbName).collection('atanh_outside');
      await collection.insertOne({ value: 2 });

      const results = (await collection
        .aggregate([{ $project: { result: { $atanh: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Number.isNaN(results[0].result));
    });
  });

  // ==================== Conversion Functions ====================

  describe('$degreesToRadians', () => {
    it('should convert 0 degrees to 0 radians', async () => {
      const collection = client.db(dbName).collection('deg2rad_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $degreesToRadians: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should convert 180 degrees to PI radians', async () => {
      const collection = client.db(dbName).collection('deg2rad_180');
      await collection.insertOne({ value: 180 });

      const results = (await collection
        .aggregate([{ $project: { result: { $degreesToRadians: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.PI) < 1e-10);
    });

    it('should convert 90 degrees to PI/2 radians', async () => {
      const collection = client.db(dbName).collection('deg2rad_90');
      await collection.insertOne({ value: 90 });

      const results = (await collection
        .aggregate([{ $project: { result: { $degreesToRadians: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - Math.PI / 2) < 1e-10);
    });

    it('should convert 360 degrees to 2*PI radians', async () => {
      const collection = client.db(dbName).collection('deg2rad_360');
      await collection.insertOne({ value: 360 });

      const results = (await collection
        .aggregate([{ $project: { result: { $degreesToRadians: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - 2 * Math.PI) < 1e-10);
    });

    it('should return null for null input', async () => {
      const collection = client.db(dbName).collection('deg2rad_null');
      await collection.insertOne({ value: null });

      const results = (await collection
        .aggregate([{ $project: { result: { $degreesToRadians: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });

    it('should throw for non-numeric input', async () => {
      const collection = client.db(dbName).collection('deg2rad_string');
      await collection.insertOne({ value: 'hello' });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([{ $project: { result: { $degreesToRadians: '$value' }, _id: 0 } }])
            .toArray();
        },
        (err: Error) => {
          assert.ok(err.message.includes('$degreesToRadians only supports numeric types'));
          return true;
        }
      );
    });
  });

  describe('$radiansToDegrees', () => {
    it('should convert 0 radians to 0 degrees', async () => {
      const collection = client.db(dbName).collection('rad2deg_zero');
      await collection.insertOne({ value: 0 });

      const results = (await collection
        .aggregate([{ $project: { result: { $radiansToDegrees: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, 0);
    });

    it('should convert PI radians to 180 degrees', async () => {
      const collection = client.db(dbName).collection('rad2deg_pi');
      await collection.insertOne({ value: Math.PI });

      const results = (await collection
        .aggregate([{ $project: { result: { $radiansToDegrees: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - 180) < 1e-10);
    });

    it('should convert PI/2 radians to 90 degrees', async () => {
      const collection = client.db(dbName).collection('rad2deg_pi2');
      await collection.insertOne({ value: Math.PI / 2 });

      const results = (await collection
        .aggregate([{ $project: { result: { $radiansToDegrees: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - 90) < 1e-10);
    });

    it('should convert 2*PI radians to 360 degrees', async () => {
      const collection = client.db(dbName).collection('rad2deg_2pi');
      await collection.insertOne({ value: 2 * Math.PI });

      const results = (await collection
        .aggregate([{ $project: { result: { $radiansToDegrees: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - 360) < 1e-10);
    });

    it('should return null for null input', async () => {
      const collection = client.db(dbName).collection('rad2deg_null');
      await collection.insertOne({ value: null });

      const results = (await collection
        .aggregate([{ $project: { result: { $radiansToDegrees: '$value' }, _id: 0 } }])
        .toArray()) as unknown as TrigResult[];

      assert.strictEqual(results[0].result, null);
    });
  });

  // ==================== Combined Operations ====================

  describe('Combined operations', () => {
    it('should round-trip degrees through radians and back', async () => {
      const collection = client.db(dbName).collection('roundtrip');
      await collection.insertOne({ degrees: 45 });

      const results = (await collection
        .aggregate([
          {
            $project: {
              original: '$degrees',
              radians: { $degreesToRadians: '$degrees' },
              backToDegrees: { $radiansToDegrees: { $degreesToRadians: '$degrees' } },
              _id: 0,
            },
          },
        ])
        .toArray()) as { original: number; radians: number; backToDegrees: number }[];

      assert.ok(Math.abs(results[0].backToDegrees - 45) < 1e-10);
    });

    it('should calculate sin of 90 degrees using conversion', async () => {
      const collection = client.db(dbName).collection('sin_degrees');
      await collection.insertOne({ degrees: 90 });

      const results = (await collection
        .aggregate([
          {
            $project: {
              result: { $sin: { $degreesToRadians: '$degrees' } },
              _id: 0,
            },
          },
        ])
        .toArray()) as unknown as TrigResult[];

      assert.ok(Math.abs(results[0].result! - 1) < 1e-10);
    });

    it('should verify sin^2 + cos^2 = 1', async () => {
      const collection = client.db(dbName).collection('pythagorean');
      await collection.insertOne({ angle: Math.PI / 3 });

      const results = (await collection
        .aggregate([
          {
            $project: {
              sum: {
                $add: [{ $pow: [{ $sin: '$angle' }, 2] }, { $pow: [{ $cos: '$angle' }, 2] }],
              },
              _id: 0,
            },
          },
        ])
        .toArray()) as { sum: number }[];

      assert.ok(Math.abs(results[0].sum - 1) < 1e-10);
    });
  });
});
