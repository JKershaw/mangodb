/**
 * Fuzz tests for query operators.
 *
 * These tests follow the same pattern as regular integration tests:
 * - Without MONGODB_URI: Tests run against MangoDB
 * - With MONGODB_URI: Tests run against MongoDB
 *
 * Additionally, dual-target comparison tests run when MONGODB_URI is set
 * to find behavioral differences between MangoDB and MongoDB.
 */

import { describe, it } from 'node:test';
import * as fc from 'fast-check';
import {
  runFuzz,
  runDualTargetFuzz,
  testQuery,
  compareQueryResults,
  getTestModeName,
} from '../fuzz-harness.ts';
import {
  simpleDocument,
  nullTestDocument,
  documentWithArrays,
  edgeCaseDocument,
} from '../arbitraries/documents.ts';
import { bsonNumber, bsonString, jsonSafeBsonValue } from '../arbitraries/values.ts';

describe(`Query Operator Fuzz Tests (${getTestModeName()})`, () => {
  // ============================================================================
  // Single-target tests (run against MangoDB or MongoDB based on MONGODB_URI)
  // These verify the implementation doesn't crash with random inputs
  // ============================================================================

  describe('Single-target: Robustness Tests', () => {
    it('should handle $eq with various value types without crashing', async () => {
      await runFuzz(
        '$eq-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          queryValue: jsonSafeBsonValue,
        }),
        async ({ docs, queryValue }, ctx) => {
          return testQuery(ctx, 'fuzz_eq', docs, { value: queryValue });
        }
      );
    });

    it('should handle $gt/$lt with numbers without crashing', async () => {
      await runFuzz(
        '$gt-$lt-robustness',
        fc.record({
          docs: fc.array(fc.record({ value: bsonNumber }), { minLength: 1, maxLength: 10 }),
          threshold: bsonNumber,
        }),
        async ({ docs, threshold }, ctx) => {
          return testQuery(ctx, 'fuzz_gtlt', docs, { value: { $gt: threshold } });
        }
      );
    });

    it('should handle $in with mixed types without crashing', async () => {
      await runFuzz(
        '$in-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          values: fc.array(jsonSafeBsonValue, { minLength: 1, maxLength: 5 }),
        }),
        async ({ docs, values }, ctx) => {
          return testQuery(ctx, 'fuzz_in', docs, { value: { $in: values } });
        }
      );
    });

    it('should handle null queries without crashing', async () => {
      await runFuzz(
        'null-query-robustness',
        fc.record({
          docs: fc.array(nullTestDocument, { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return testQuery(ctx, 'fuzz_null', docs, { field: null });
        }
      );
    });

    it('should handle $exists without crashing', async () => {
      await runFuzz(
        '$exists-robustness',
        fc.record({
          docs: fc.array(nullTestDocument, { minLength: 1, maxLength: 10 }),
          exists: fc.boolean(),
        }),
        async ({ docs, exists }, ctx) => {
          return testQuery(ctx, 'fuzz_exists', docs, { field: { $exists: exists } });
        }
      );
    });

    it('should handle $type without crashing', async () => {
      await runFuzz(
        '$type-robustness',
        fc.record({
          docs: fc.array(edgeCaseDocument, { minLength: 1, maxLength: 10 }),
          typeName: fc.constantFrom('string', 'number', 'bool', 'array', 'object', 'null'),
        }),
        async ({ docs, typeName }, ctx) => {
          return testQuery(ctx, 'fuzz_type', docs, { mixedArray: { $type: typeName } });
        }
      );
    });

    it('should handle $size without crashing', async () => {
      await runFuzz(
        '$size-robustness',
        fc.record({
          docs: fc.array(documentWithArrays, { minLength: 1, maxLength: 10 }),
          size: fc.integer({ min: 0, max: 10 }),
        }),
        async ({ docs, size }, ctx) => {
          return testQuery(ctx, 'fuzz_size', docs, { tags: { $size: size } });
        }
      );
    });

    it('should handle $and without crashing', async () => {
      await runFuzz(
        '$and-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          conditions: fc.tuple(
            fc.record({ name: bsonString }),
            fc.record({ active: fc.boolean() })
          ),
        }),
        async ({ docs, conditions }, ctx) => {
          return testQuery(ctx, 'fuzz_and', docs, { $and: conditions });
        }
      );
    });

    it('should handle $or without crashing', async () => {
      await runFuzz(
        '$or-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          conditions: fc.tuple(
            fc.record({ name: bsonString }),
            fc.record({ active: fc.boolean() })
          ),
        }),
        async ({ docs, conditions }, ctx) => {
          return testQuery(ctx, 'fuzz_or', docs, { $or: conditions });
        }
      );
    });

    it('should handle $not without crashing', async () => {
      await runFuzz(
        '$not-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          threshold: bsonNumber,
        }),
        async ({ docs, threshold }, ctx) => {
          return testQuery(ctx, 'fuzz_not', docs, { value: { $not: { $gt: threshold } } });
        }
      );
    });

    it('should handle empty query without crashing', async () => {
      await runFuzz(
        'empty-query-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return testQuery(ctx, 'fuzz_empty', docs, {});
        }
      );
    });
  });

  // ============================================================================
  // Dual-target comparison tests (only run when MONGODB_URI is set)
  // These compare MangoDB and MongoDB behavior to find differences
  // ============================================================================

  describe('Dual-target: Comparison Tests', () => {
    it('should match MongoDB behavior for $eq with various types', async () => {
      await runDualTargetFuzz(
        '$eq-comparison',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          queryValue: jsonSafeBsonValue,
        }),
        async ({ docs, queryValue }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_eq', docs, { value: queryValue });
        }
      );
    });

    it('should match MongoDB behavior for null matching', async () => {
      await runDualTargetFuzz(
        'null-matching-comparison',
        fc.record({
          docs: fc.array(nullTestDocument, { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_null', docs, { field: null });
        }
      );
    });

    it('should match MongoDB behavior for $ne: null', async () => {
      await runDualTargetFuzz(
        '$ne-null-comparison',
        fc.record({
          docs: fc.array(nullTestDocument, { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_ne_null', docs, { field: { $ne: null } });
        }
      );
    });

    it('should match MongoDB behavior for $exists', async () => {
      await runDualTargetFuzz(
        '$exists-comparison',
        fc.record({
          docs: fc.array(nullTestDocument, { minLength: 1, maxLength: 10 }),
          exists: fc.boolean(),
        }),
        async ({ docs, exists }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_exists', docs, { field: { $exists: exists } });
        }
      );
    });

    it('should match MongoDB behavior for $size', async () => {
      await runDualTargetFuzz(
        '$size-comparison',
        fc.record({
          docs: fc.array(documentWithArrays, { minLength: 1, maxLength: 10 }),
          size: fc.integer({ min: 0, max: 10 }),
        }),
        async ({ docs, size }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_size', docs, { tags: { $size: size } });
        }
      );
    });

    it('should match MongoDB behavior for $all', async () => {
      await runDualTargetFuzz(
        '$all-comparison',
        fc.record({
          docs: fc.array(documentWithArrays, { minLength: 1, maxLength: 10 }),
          values: fc.array(bsonNumber, { minLength: 1, maxLength: 3 }),
        }),
        async ({ docs, values }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_all', docs, { scores: { $all: values } });
        }
      );
    });

    it('should match MongoDB behavior for scalar query on array', async () => {
      await runDualTargetFuzz(
        'scalar-on-array-comparison',
        fc.record({
          docs: fc.array(documentWithArrays, { minLength: 1, maxLength: 10 }),
          value: bsonNumber,
        }),
        async ({ docs, value }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_scalar_array', docs, { scores: value });
        }
      );
    });

    it('should match MongoDB behavior for $in with null', async () => {
      await runDualTargetFuzz(
        '$in-with-null-comparison',
        fc.record({
          // Include documents with field present (null or value) AND missing field
          docs: fc.array(
            fc.oneof(
              fc.record({ field: fc.oneof(fc.constant(null), bsonNumber, bsonString) }),
              fc.record({ other: bsonString }) // field is missing
            ),
            { minLength: 1, maxLength: 10 }
          ),
          otherValues: fc.array(jsonSafeBsonValue, { minLength: 0, maxLength: 3 }),
        }),
        async ({ docs, otherValues }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_in_null', docs, {
            field: { $in: [null, ...otherValues] },
          });
        }
      );
    });

    it('should match MongoDB behavior for empty array query', async () => {
      await runDualTargetFuzz(
        'empty-array-comparison',
        fc.record({
          docs: fc.array(
            fc.oneof(
              fc.record({ arr: fc.constant([]) }),
              fc.record({ arr: fc.array(bsonNumber, { minLength: 1, maxLength: 5 }) }),
              fc.record({ other: bsonString })
            ),
            { minLength: 1, maxLength: 10 }
          ),
        }),
        async ({ docs }, ctx) => {
          return compareQueryResults(ctx, 'fuzz_empty_arr', docs, { arr: [] });
        }
      );
    });
  });
});
