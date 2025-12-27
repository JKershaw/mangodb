/**
 * Unit tests for date stepping utilities.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  addDateStep,
  dateDiff,
  generateDateSequence,
  isValidTimeUnit,
} from '../../../src/aggregation/date-utils.ts';

describe('Date Utilities', () => {
  describe('addDateStep', () => {
    it('should add days to a date', () => {
      const date = new Date('2024-01-15T00:00:00Z');
      const result = addDateStep(date, 5, 'day');

      assert.strictEqual(result.toISOString(), '2024-01-20T00:00:00.000Z');
    });

    it('should subtract days when step is negative', () => {
      const date = new Date('2024-01-15T00:00:00Z');
      const result = addDateStep(date, -5, 'day');

      assert.strictEqual(result.toISOString(), '2024-01-10T00:00:00.000Z');
    });

    it('should add months with calendar awareness', () => {
      const date = new Date('2024-01-31T00:00:00Z');
      const result = addDateStep(date, 1, 'month');

      // January 31 + 1 month = February 29 (2024 is leap year)
      assert.strictEqual(result.toISOString(), '2024-03-02T00:00:00.000Z');
    });

    it('should add years correctly', () => {
      const date = new Date('2024-06-15T00:00:00Z');
      const result = addDateStep(date, 2, 'year');

      assert.strictEqual(result.toISOString(), '2026-06-15T00:00:00.000Z');
    });

    it('should handle leap year edge case', () => {
      // Feb 29 in leap year + 1 year
      const date = new Date('2024-02-29T00:00:00Z');
      const result = addDateStep(date, 1, 'year');

      // 2025 is not a leap year, so Feb 29 becomes March 1
      assert.strictEqual(result.toISOString(), '2025-03-01T00:00:00.000Z');
    });

    it('should add hours correctly', () => {
      const date = new Date('2024-01-15T10:00:00Z');
      const result = addDateStep(date, 14, 'hour');

      assert.strictEqual(result.toISOString(), '2024-01-16T00:00:00.000Z');
    });

    it('should add weeks correctly', () => {
      const date = new Date('2024-01-15T00:00:00Z');
      const result = addDateStep(date, 2, 'week');

      assert.strictEqual(result.toISOString(), '2024-01-29T00:00:00.000Z');
    });

    it('should add quarters correctly', () => {
      const date = new Date('2024-01-15T00:00:00Z');
      const result = addDateStep(date, 1, 'quarter');

      assert.strictEqual(result.toISOString(), '2024-04-15T00:00:00.000Z');
    });

    it('should not mutate original date', () => {
      const date = new Date('2024-01-15T00:00:00Z');
      const original = date.toISOString();
      addDateStep(date, 5, 'day');

      assert.strictEqual(date.toISOString(), original);
    });
  });

  describe('dateDiff', () => {
    it('should calculate difference in days', () => {
      const start = new Date('2024-01-15T00:00:00Z');
      const end = new Date('2024-01-20T00:00:00Z');

      assert.strictEqual(dateDiff(start, end, 'day'), 5);
    });

    it('should calculate difference in months', () => {
      const start = new Date('2024-01-15T00:00:00Z');
      const end = new Date('2024-04-15T00:00:00Z');

      assert.strictEqual(dateDiff(start, end, 'month'), 3);
    });

    it('should calculate difference in years', () => {
      const start = new Date('2022-01-15T00:00:00Z');
      const end = new Date('2024-01-15T00:00:00Z');

      assert.strictEqual(dateDiff(start, end, 'year'), 2);
    });

    it('should return negative for reversed dates', () => {
      const start = new Date('2024-01-20T00:00:00Z');
      const end = new Date('2024-01-15T00:00:00Z');

      assert.strictEqual(dateDiff(start, end, 'day'), -5);
    });
  });

  describe('generateDateSequence', () => {
    it('should generate sequence of dates', () => {
      const start = new Date('2024-01-01T00:00:00Z');
      const end = new Date('2024-01-05T00:00:00Z');
      const result = generateDateSequence(start, end, 1, 'day');

      assert.strictEqual(result.length, 4);
      assert.strictEqual(result[0].toISOString(), '2024-01-01T00:00:00.000Z');
      assert.strictEqual(result[3].toISOString(), '2024-01-04T00:00:00.000Z');
    });

    it('should generate sequence with step > 1', () => {
      const start = new Date('2024-01-01T00:00:00Z');
      const end = new Date('2024-01-10T00:00:00Z');
      const result = generateDateSequence(start, end, 2, 'day');

      assert.strictEqual(result.length, 5);
      assert.strictEqual(result[0].toISOString(), '2024-01-01T00:00:00.000Z');
      assert.strictEqual(result[1].toISOString(), '2024-01-03T00:00:00.000Z');
    });

    it('should return empty array when start >= end', () => {
      const start = new Date('2024-01-10T00:00:00Z');
      const end = new Date('2024-01-05T00:00:00Z');
      const result = generateDateSequence(start, end, 1, 'day');

      assert.strictEqual(result.length, 0);
    });

    it('should generate hourly sequence', () => {
      const start = new Date('2024-01-01T00:00:00Z');
      const end = new Date('2024-01-01T04:00:00Z');
      const result = generateDateSequence(start, end, 1, 'hour');

      assert.strictEqual(result.length, 4);
    });
  });

  describe('isValidTimeUnit', () => {
    it('should return true for valid units', () => {
      assert.strictEqual(isValidTimeUnit('day'), true);
      assert.strictEqual(isValidTimeUnit('month'), true);
      assert.strictEqual(isValidTimeUnit('year'), true);
      assert.strictEqual(isValidTimeUnit('hour'), true);
      assert.strictEqual(isValidTimeUnit('minute'), true);
      assert.strictEqual(isValidTimeUnit('second'), true);
      assert.strictEqual(isValidTimeUnit('millisecond'), true);
      assert.strictEqual(isValidTimeUnit('week'), true);
      assert.strictEqual(isValidTimeUnit('quarter'), true);
    });

    it('should return false for invalid units', () => {
      assert.strictEqual(isValidTimeUnit('days'), false);
      assert.strictEqual(isValidTimeUnit('invalid'), false);
      assert.strictEqual(isValidTimeUnit(''), false);
    });
  });
});
