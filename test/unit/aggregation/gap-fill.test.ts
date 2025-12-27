/**
 * Unit tests for gap filling utilities.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  applyLocf,
  applyLinearFill,
  isGap,
  getFirstNonNull,
  getLastNonNull,
} from '../../../src/aggregation/gap-fill.ts';

describe('Gap Filling Utilities', () => {
  describe('applyLocf', () => {
    it('should fill null values with last non-null value', () => {
      const values = [1, null, null, 4, null];
      const result = applyLocf(values);

      assert.deepStrictEqual(result, [1, 1, 1, 4, 4]);
    });

    it('should leave nulls at start as null', () => {
      const values = [null, null, 3, null, 5];
      const result = applyLocf(values);

      assert.deepStrictEqual(result, [null, null, 3, 3, 5]);
    });

    it('should handle all-null input', () => {
      const values = [null, null, null];
      const result = applyLocf(values);

      assert.deepStrictEqual(result, [null, null, null]);
    });

    it('should handle no nulls', () => {
      const values = [1, 2, 3];
      const result = applyLocf(values);

      assert.deepStrictEqual(result, [1, 2, 3]);
    });

    it('should handle undefined as gap', () => {
      const values = [1, undefined, 3];
      const result = applyLocf(values);

      assert.deepStrictEqual(result, [1, 1, 3]);
    });

    it('should work with non-numeric values', () => {
      const values = ['a', null, null, 'b', null];
      const result = applyLocf(values);

      assert.deepStrictEqual(result, ['a', 'a', 'a', 'b', 'b']);
    });
  });

  describe('applyLinearFill', () => {
    it('should linearly interpolate between values', () => {
      const values = [0, null, null, 6];
      const result = applyLinearFill(values);

      assert.deepStrictEqual(result, [0, 2, 4, 6]);
    });

    it('should leave nulls at start as null', () => {
      const values = [null, null, 4, null, 8];
      const result = applyLinearFill(values);

      assert.deepStrictEqual(result, [null, null, 4, 6, 8]);
    });

    it('should leave nulls at end as null', () => {
      const values = [2, null, 6, null, null];
      const result = applyLinearFill(values);

      assert.deepStrictEqual(result, [2, 4, 6, null, null]);
    });

    it('should handle all-null input', () => {
      const values = [null, null, null];
      const result = applyLinearFill(values);

      assert.deepStrictEqual(result, [null, null, null]);
    });

    it('should handle single gap', () => {
      const values = [10, null, 20];
      const result = applyLinearFill(values);

      assert.deepStrictEqual(result, [10, 15, 20]);
    });

    it('should use positions for non-uniform interpolation', () => {
      // Values at positions 0, 2, 4 (but index 1 is at position 2)
      const values = [0, null, 100];
      const positions = [0, 20, 100];
      const result = applyLinearFill(values, positions);

      // At position 20, interpolate between 0 (at 0) and 100 (at 100)
      // fraction = 20/100 = 0.2, value = 0 + 0.2 * 100 = 20
      assert.deepStrictEqual(result, [0, 20, 100]);
    });

    it('should handle multiple separate gaps', () => {
      const values = [0, null, 4, null, 8];
      const result = applyLinearFill(values);

      assert.deepStrictEqual(result, [0, 2, 4, 6, 8]);
    });
  });

  describe('helper functions', () => {
    it('isGap should identify null and undefined', () => {
      assert.strictEqual(isGap(null), true);
      assert.strictEqual(isGap(undefined), true);
      assert.strictEqual(isGap(0), false);
      assert.strictEqual(isGap(''), false);
      assert.strictEqual(isGap(false), false);
    });

    it('getFirstNonNull should return first non-null value', () => {
      assert.strictEqual(getFirstNonNull([null, 1, 2, 3]), 1);
      assert.strictEqual(getFirstNonNull([null, null, null]), null);
      assert.strictEqual(getFirstNonNull([5, null, null]), 5);
    });

    it('getLastNonNull should return last non-null value', () => {
      assert.strictEqual(getLastNonNull([1, 2, 3, null]), 3);
      assert.strictEqual(getLastNonNull([null, null, null]), null);
      assert.strictEqual(getLastNonNull([null, null, 5]), 5);
    });
  });
});
