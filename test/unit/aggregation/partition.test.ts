/**
 * Unit tests for partition utilities.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { partitionDocuments, sortPartition } from '../../../src/aggregation/partition.ts';
import { evaluateExpression } from '../../../src/aggregation/expression.ts';

describe('Partition Utilities', () => {
  describe('partitionDocuments', () => {
    it('should put all docs in single partition when no options', () => {
      const docs = [{ x: 1 }, { x: 2 }, { x: 3 }];
      const result = partitionDocuments(docs, {}, evaluateExpression);

      assert.strictEqual(result.size, 1);
      assert.strictEqual(result.get('')!.length, 3);
    });

    it('should partition by single field using partitionByFields', () => {
      const docs = [
        { category: 'A', value: 1 },
        { category: 'B', value: 2 },
        { category: 'A', value: 3 },
      ];
      const result = partitionDocuments(
        docs,
        { partitionByFields: ['category'] },
        evaluateExpression
      );

      assert.strictEqual(result.size, 2);
      const groupA = result.get('["A"]')!;
      const groupB = result.get('["B"]')!;
      assert.strictEqual(groupA.length, 2);
      assert.strictEqual(groupB.length, 1);
    });

    it('should partition by multiple fields using partitionByFields', () => {
      const docs = [
        { a: 1, b: 'x', val: 10 },
        { a: 1, b: 'y', val: 20 },
        { a: 1, b: 'x', val: 30 },
        { a: 2, b: 'x', val: 40 },
      ];
      const result = partitionDocuments(
        docs,
        { partitionByFields: ['a', 'b'] },
        evaluateExpression
      );

      assert.strictEqual(result.size, 3);
      assert.strictEqual(result.get('[1,"x"]')!.length, 2);
      assert.strictEqual(result.get('[1,"y"]')!.length, 1);
      assert.strictEqual(result.get('[2,"x"]')!.length, 1);
    });

    it('should partition using partitionBy expression object', () => {
      const docs = [
        { x: 1, y: 10 },
        { x: 2, y: 20 },
        { x: 1, y: 30 },
      ];
      const result = partitionDocuments(docs, { partitionBy: { key: '$x' } }, evaluateExpression);

      assert.strictEqual(result.size, 2);
    });

    it('should throw error if partitionBy is a string', () => {
      const docs = [{ x: 1 }];
      assert.throws(
        () =>
          partitionDocuments(
            docs,
            { partitionBy: '$x' as unknown as Record<string, unknown> },
            evaluateExpression
          ),
        /partitionBy must be an object expression/
      );
    });

    it('should handle undefined field values', () => {
      const docs = [{ a: 1, b: 10 }, { a: 1 }, { a: 2, b: 20 }];
      const result = partitionDocuments(
        docs,
        { partitionByFields: ['a', 'b'] },
        evaluateExpression
      );

      // [1, undefined] and [1, 10] and [2, 20]
      assert.strictEqual(result.size, 3);
    });
  });

  describe('sortPartition', () => {
    it('should sort by single field ascending', () => {
      const docs = [{ x: 3 }, { x: 1 }, { x: 2 }];
      const result = sortPartition(docs, { x: 1 });

      assert.deepStrictEqual(
        result.map((d) => d.x),
        [1, 2, 3]
      );
    });

    it('should sort by single field descending', () => {
      const docs = [{ x: 1 }, { x: 3 }, { x: 2 }];
      const result = sortPartition(docs, { x: -1 });

      assert.deepStrictEqual(
        result.map((d) => d.x),
        [3, 2, 1]
      );
    });

    it('should sort by multiple fields', () => {
      const docs = [
        { a: 1, b: 2 },
        { a: 2, b: 1 },
        { a: 1, b: 1 },
      ];
      const result = sortPartition(docs, { a: 1, b: 1 });

      assert.deepStrictEqual(
        result.map((d) => [d.a, d.b]),
        [
          [1, 1],
          [1, 2],
          [2, 1],
        ]
      );
    });

    it('should not mutate original array', () => {
      const docs = [{ x: 3 }, { x: 1 }, { x: 2 }];
      const original = [...docs];
      sortPartition(docs, { x: 1 });

      assert.deepStrictEqual(docs, original);
    });
  });
});
