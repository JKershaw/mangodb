/**
 * Fuzz tests for update operators.
 *
 * These tests follow the same pattern as regular integration tests:
 * - Without MONGODB_URI: Tests run against MangoDB
 * - With MONGODB_URI: Tests run against MongoDB
 *
 * Additionally, dual-target comparison tests run when MONGODB_URI is set.
 */

import { describe, it } from 'node:test';
import * as fc from 'fast-check';
import {
  runFuzz,
  runDualTargetFuzz,
  testUpdate,
  compareUpdateResults,
  getTestModeName,
} from '../fuzz-harness.ts';
import { simpleDocument } from '../arbitraries/documents.ts';
import { bsonNumber, bsonString, jsonSafeBsonValue, numberArray } from '../arbitraries/values.ts';

describe(`Update Operator Fuzz Tests (${getTestModeName()})`, () => {
  // ============================================================================
  // Single-target robustness tests
  // ============================================================================

  describe('Single-target: Robustness Tests', () => {
    it('should handle $set with various values without crashing', async () => {
      await runFuzz(
        '$set-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          newValue: jsonSafeBsonValue,
        }),
        async ({ docs, newValue }, ctx) => {
          return testUpdate(ctx, 'fuzz_set', docs, {}, { $set: { value: newValue } });
        }
      );
    });

    it('should handle $inc without crashing', async () => {
      await runFuzz(
        '$inc-robustness',
        fc.record({
          docs: fc.array(
            fc.record({ count: fc.integer({ min: 0, max: 100 }) }),
            { minLength: 1, maxLength: 10 }
          ),
          increment: fc.integer({ min: -50, max: 50 }),
        }),
        async ({ docs, increment }, ctx) => {
          return testUpdate(ctx, 'fuzz_inc', docs, {}, { $inc: { count: increment } });
        }
      );
    });

    it('should handle $push without crashing', async () => {
      await runFuzz(
        '$push-robustness',
        fc.record({
          docs: fc.array(
            fc.record({ items: numberArray }),
            { minLength: 1, maxLength: 10 }
          ),
          newItem: bsonNumber,
        }),
        async ({ docs, newItem }, ctx) => {
          return testUpdate(ctx, 'fuzz_push', docs, {}, { $push: { items: newItem } });
        }
      );
    });

    it('should handle $addToSet without crashing', async () => {
      await runFuzz(
        '$addToSet-robustness',
        fc.record({
          docs: fc.array(
            fc.record({ items: numberArray }),
            { minLength: 1, maxLength: 10 }
          ),
          newItem: bsonNumber,
        }),
        async ({ docs, newItem }, ctx) => {
          return testUpdate(ctx, 'fuzz_addtoset', docs, {}, { $addToSet: { items: newItem } });
        }
      );
    });

    it('should handle $pull without crashing', async () => {
      await runFuzz(
        '$pull-robustness',
        fc.record({
          docs: fc.array(
            fc.record({ items: numberArray }),
            { minLength: 1, maxLength: 10 }
          ),
          pullValue: bsonNumber,
        }),
        async ({ docs, pullValue }, ctx) => {
          return testUpdate(ctx, 'fuzz_pull', docs, {}, { $pull: { items: pullValue } });
        }
      );
    });

    it('should handle $unset without crashing', async () => {
      await runFuzz(
        '$unset-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return testUpdate(ctx, 'fuzz_unset', docs, {}, { $unset: { value: '' } });
        }
      );
    });

    it('should handle multi-operator updates without crashing', async () => {
      await runFuzz(
        'multi-op-robustness',
        fc.record({
          docs: fc.array(
            fc.record({ name: bsonString, count: fc.integer({ min: 0, max: 100 }) }),
            { minLength: 1, maxLength: 10 }
          ),
          newName: bsonString,
          increment: fc.integer({ min: 1, max: 10 }),
        }),
        async ({ docs, newName, increment }, ctx) => {
          return testUpdate(ctx, 'fuzz_multi', docs, {}, {
            $set: { name: newName },
            $inc: { count: increment },
          });
        }
      );
    });
  });

  // ============================================================================
  // Dual-target comparison tests
  // ============================================================================

  describe('Dual-target: Comparison Tests', () => {
    it('should match MongoDB behavior for $set', async () => {
      await runDualTargetFuzz(
        '$set-comparison',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          newValue: jsonSafeBsonValue,
        }),
        async ({ docs, newValue }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_set', docs, {}, { $set: { value: newValue } });
        }
      );
    });

    it('should match MongoDB behavior for $inc on existing field', async () => {
      await runDualTargetFuzz(
        '$inc-existing-comparison',
        fc.record({
          docs: fc.array(
            fc.record({ count: fc.integer({ min: 0, max: 100 }) }),
            { minLength: 1, maxLength: 10 }
          ),
          increment: fc.integer({ min: -50, max: 50 }),
        }),
        async ({ docs, increment }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_inc', docs, {}, { $inc: { count: increment } });
        }
      );
    });

    it('should match MongoDB behavior for $inc on missing field', async () => {
      await runDualTargetFuzz(
        '$inc-missing-comparison',
        fc.record({
          docs: fc.array(
            fc.record({ other: bsonString }),
            { minLength: 1, maxLength: 10 }
          ),
          increment: fc.integer({ min: 1, max: 100 }),
        }),
        async ({ docs, increment }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_inc_missing', docs, {}, {
            $inc: { newCount: increment },
          });
        }
      );
    });

    it('should match MongoDB behavior for $push', async () => {
      await runDualTargetFuzz(
        '$push-comparison',
        fc.record({
          docs: fc.array(
            fc.record({ items: numberArray }),
            { minLength: 1, maxLength: 10 }
          ),
          newItem: bsonNumber,
        }),
        async ({ docs, newItem }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_push', docs, {}, { $push: { items: newItem } });
        }
      );
    });

    it('should match MongoDB behavior for $push with $each and $slice', async () => {
      await runDualTargetFuzz(
        '$push-$each-$slice-comparison',
        fc.record({
          docs: fc.array(
            fc.record({ items: numberArray }),
            { minLength: 1, maxLength: 10 }
          ),
          newItems: fc.array(bsonNumber, { minLength: 1, maxLength: 3 }),
          slice: fc.integer({ min: -5, max: 5 }),
        }),
        async ({ docs, newItems, slice }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_push_slice', docs, {}, {
            $push: { items: { $each: newItems, $slice: slice } },
          });
        }
      );
    });

    it('should match MongoDB behavior for $pop', async () => {
      await runDualTargetFuzz(
        '$pop-comparison',
        fc.record({
          docs: fc.array(
            fc.record({ items: fc.array(bsonNumber, { minLength: 1, maxLength: 10 }) }),
            { minLength: 1, maxLength: 10 }
          ),
          direction: fc.constantFrom(-1, 1),
        }),
        async ({ docs, direction }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_pop', docs, {}, { $pop: { items: direction } });
        }
      );
    });

    it('should match MongoDB behavior for $pull with condition', async () => {
      await runDualTargetFuzz(
        '$pull-condition-comparison',
        fc.record({
          docs: fc.array(
            fc.record({
              items: fc.array(fc.integer({ min: 0, max: 100 }), { minLength: 1, maxLength: 10 }),
            }),
            { minLength: 1, maxLength: 10 }
          ),
          threshold: fc.integer({ min: 0, max: 100 }),
        }),
        async ({ docs, threshold }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_pull_cond', docs, {}, {
            $pull: { items: { $gt: threshold } },
          });
        }
      );
    });

    it('should match MongoDB behavior for $set to null', async () => {
      await runDualTargetFuzz(
        '$set-null-comparison',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_set_null', docs, {}, { $set: { value: null } });
        }
      );
    });

    it('should match MongoDB behavior for $set to empty array', async () => {
      await runDualTargetFuzz(
        '$set-empty-array-comparison',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return compareUpdateResults(ctx, 'fuzz_set_empty', docs, {}, { $set: { value: [] } });
        }
      );
    });
  });
});
