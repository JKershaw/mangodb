/**
 * Fuzz tests for aggregation operators.
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
  testAggregation,
  createDualTargetContext,
  compareResults,
  getTestModeName,
  isMongoDBAvailable,
  type DualTargetContext,
  type ComparisonResult,
} from '../fuzz-harness.ts';
import { simpleDocument, documentWithArrays } from '../arbitraries/documents.ts';
import { bsonNumber, bsonString } from '../arbitraries/values.ts';

/**
 * Compare aggregation results for dual-target tests.
 */
async function compareAggregationResults(
  ctx: DualTargetContext,
  collectionName: string,
  documents: Record<string, unknown>[],
  pipeline: Record<string, unknown>[]
): Promise<ComparisonResult> {
  const mangoCollection = ctx.mangoClient.db(ctx.dbName).collection(collectionName);
  const mongoCollection = ctx.mongoClient.db(ctx.dbName).collection(collectionName);

  if (documents.length > 0) {
    await Promise.all([
      mangoCollection.insertMany(documents),
      mongoCollection.insertMany(documents),
    ]);
  }

  let mangoResult: unknown;
  let mongoResult: unknown;
  let mangoError: Error | null = null;
  let mongoError: Error | null = null;

  try {
    mangoResult = await mangoCollection.aggregate(pipeline).toArray();
  } catch (e) {
    mangoError = e as Error;
  }

  try {
    mongoResult = await mongoCollection.aggregate(pipeline).toArray();
  } catch (e) {
    mongoError = e as Error;
  }

  if (mangoError || mongoError) {
    const bothThrew = mangoError !== null && mongoError !== null;
    return {
      equal: bothThrew,
      differences: bothThrew
        ? []
        : [
            `MangoDB ${mangoError ? 'threw' : 'succeeded'}, MongoDB ${mongoError ? 'threw' : 'succeeded'}`,
          ],
      mangoResult: mangoError?.message ?? mangoResult,
      mongoResult: mongoError?.message ?? mongoResult,
    };
  }

  return compareResults(mangoResult, mongoResult, 'aggregation');
}

describe(`Aggregation Fuzz Tests (${getTestModeName()})`, () => {
  // ============================================================================
  // Single-target robustness tests
  // ============================================================================

  describe('Single-target: Robustness Tests', () => {
    it('should handle $match without crashing', async () => {
      await runFuzz(
        '$match-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          matchValue: bsonNumber,
        }),
        async ({ docs, matchValue }, ctx) => {
          return testAggregation(ctx, 'fuzz_match', docs, [
            { $match: { value: { $gt: matchValue } } },
          ]);
        }
      );
    });

    it('should handle $project without crashing', async () => {
      await runFuzz(
        '$project-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return testAggregation(ctx, 'fuzz_project', docs, [
            { $project: { name: 1, value: 1 } },
          ]);
        }
      );
    });

    it('should handle $group with $sum without crashing', async () => {
      await runFuzz(
        '$group-sum-robustness',
        fc.record({
          docs: fc.array(
            fc.record({
              category: fc.constantFrom('A', 'B', 'C'),
              amount: fc.integer({ min: 0, max: 100 }),
            }),
            { minLength: 1, maxLength: 20 }
          ),
        }),
        async ({ docs }, ctx) => {
          return testAggregation(ctx, 'fuzz_group', docs, [
            { $group: { _id: '$category', total: { $sum: '$amount' } } },
          ]);
        }
      );
    });

    it('should handle $sort without crashing', async () => {
      await runFuzz(
        '$sort-robustness',
        fc.record({
          docs: fc.array(fc.record({ value: bsonNumber }), { minLength: 1, maxLength: 10 }),
        }),
        async ({ docs }, ctx) => {
          return testAggregation(ctx, 'fuzz_sort', docs, [{ $sort: { value: 1 } }]);
        }
      );
    });

    it('should handle $limit and $skip without crashing', async () => {
      await runFuzz(
        '$limit-$skip-robustness',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 20 }),
          limit: fc.integer({ min: 1, max: 10 }),
          skip: fc.integer({ min: 0, max: 10 }),
        }),
        async ({ docs, limit, skip }, ctx) => {
          return testAggregation(ctx, 'fuzz_limit_skip', docs, [
            { $sort: { value: 1 } },
            { $skip: skip },
            { $limit: limit },
          ]);
        }
      );
    });

    it('should handle $unwind without crashing', async () => {
      await runFuzz(
        '$unwind-robustness',
        fc.record({
          docs: fc.array(
            fc.record({
              name: bsonString,
              tags: fc.array(bsonString, { minLength: 0, maxLength: 5 }),
            }),
            { minLength: 1, maxLength: 10 }
          ),
        }),
        async ({ docs }, ctx) => {
          return testAggregation(ctx, 'fuzz_unwind', docs, [{ $unwind: '$tags' }]);
        }
      );
    });

    it('should handle $addFields without crashing', async () => {
      await runFuzz(
        '$addFields-robustness',
        fc.record({
          docs: fc.array(
            fc.record({ a: bsonNumber, b: bsonNumber }),
            { minLength: 1, maxLength: 10 }
          ),
        }),
        async ({ docs }, ctx) => {
          return testAggregation(ctx, 'fuzz_addfields', docs, [
            { $addFields: { sum: { $add: ['$a', '$b'] } } },
          ]);
        }
      );
    });

    it('should handle arithmetic operators without crashing', async () => {
      await runFuzz(
        'arithmetic-robustness',
        fc.record({
          docs: fc.array(
            fc.record({
              a: fc.integer({ min: 1, max: 100 }),
              b: fc.integer({ min: 1, max: 100 }),
            }),
            { minLength: 1, maxLength: 10 }
          ),
        }),
        async ({ docs }, ctx) => {
          return testAggregation(ctx, 'fuzz_arith', docs, [
            {
              $project: {
                add: { $add: ['$a', '$b'] },
                sub: { $subtract: ['$a', '$b'] },
                mul: { $multiply: ['$a', '$b'] },
                div: { $divide: ['$a', '$b'] },
              },
            },
          ]);
        }
      );
    });

    it('should handle $cond without crashing', async () => {
      await runFuzz(
        '$cond-robustness',
        fc.record({
          docs: fc.array(fc.record({ value: bsonNumber }), { minLength: 1, maxLength: 10 }),
          threshold: bsonNumber,
        }),
        async ({ docs, threshold }, ctx) => {
          return testAggregation(ctx, 'fuzz_cond', docs, [
            {
              $project: {
                result: {
                  $cond: { if: { $gt: ['$value', threshold] }, then: 'high', else: 'low' },
                },
              },
            },
          ]);
        }
      );
    });
  });

  // ============================================================================
  // Dual-target comparison tests
  // ============================================================================

  describe('Dual-target: Comparison Tests', () => {
    it('should match MongoDB behavior for $match', async () => {
      await runDualTargetFuzz(
        '$match-comparison',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 10 }),
          matchValue: bsonNumber,
        }),
        async ({ docs, matchValue }, ctx) => {
          return compareAggregationResults(ctx, 'fuzz_match', docs, [
            { $match: { value: { $gt: matchValue } } },
          ]);
        }
      );
    });

    it('should match MongoDB behavior for $group with $sum', async () => {
      await runDualTargetFuzz(
        '$group-sum-comparison',
        fc.record({
          docs: fc.array(
            fc.record({
              category: fc.constantFrom('A', 'B', 'C'),
              amount: fc.integer({ min: 0, max: 100 }),
            }),
            { minLength: 1, maxLength: 20 }
          ),
        }),
        async ({ docs }, ctx) => {
          return compareAggregationResults(ctx, 'fuzz_group', docs, [
            { $group: { _id: '$category', total: { $sum: '$amount' } } },
            { $sort: { _id: 1 } },
          ]);
        }
      );
    });

    it('should match MongoDB behavior for $group with $avg', async () => {
      await runDualTargetFuzz(
        '$group-avg-comparison',
        fc.record({
          docs: fc.array(
            fc.record({
              category: fc.constantFrom('X', 'Y', 'Z'),
              value: fc.integer({ min: 0, max: 100 }),
            }),
            { minLength: 1, maxLength: 20 }
          ),
        }),
        async ({ docs }, ctx) => {
          return compareAggregationResults(ctx, 'fuzz_group_avg', docs, [
            { $group: { _id: '$category', avg: { $avg: '$value' } } },
            { $sort: { _id: 1 } },
          ]);
        }
      );
    });

    it('should match MongoDB behavior for $sort', async () => {
      await runDualTargetFuzz(
        '$sort-comparison',
        fc.record({
          docs: fc.array(
            fc.record({ value: bsonNumber, name: bsonString }),
            { minLength: 1, maxLength: 10 }
          ),
        }),
        async ({ docs }, ctx) => {
          // Use name as secondary sort key for stable ordering
          return compareAggregationResults(ctx, 'fuzz_sort', docs, [
            { $sort: { value: 1, name: 1 } },
          ]);
        }
      );
    });

    it('should match MongoDB behavior for $limit', async () => {
      await runDualTargetFuzz(
        '$limit-comparison',
        fc.record({
          docs: fc.array(simpleDocument, { minLength: 1, maxLength: 20 }),
          limit: fc.integer({ min: 1, max: 10 }),
        }),
        async ({ docs, limit }, ctx) => {
          // Use name as secondary sort key for stable ordering
          // (sort on value alone is unstable when values are equal)
          return compareAggregationResults(ctx, 'fuzz_limit', docs, [
            { $sort: { value: 1, name: 1 } },
            { $limit: limit },
          ]);
        }
      );
    });

    it('should match MongoDB behavior for $ifNull', async () => {
      await runDualTargetFuzz(
        '$ifNull-comparison',
        fc.record({
          docs: fc.array(
            fc.oneof(
              fc.record({ value: bsonNumber }),
              fc.record({ value: fc.constant(null) }),
              fc.record({ other: bsonString })
            ),
            { minLength: 1, maxLength: 10 }
          ),
          defaultValue: bsonNumber,
        }),
        async ({ docs, defaultValue }, ctx) => {
          return compareAggregationResults(ctx, 'fuzz_ifnull', docs, [
            { $project: { result: { $ifNull: ['$value', defaultValue] } } },
          ]);
        }
      );
    });

    it('should match MongoDB behavior for comparison operators', async () => {
      await runDualTargetFuzz(
        'comparison-ops-comparison',
        fc.record({
          docs: fc.array(
            fc.record({ a: bsonNumber, b: bsonNumber }),
            { minLength: 1, maxLength: 10 }
          ),
        }),
        async ({ docs }, ctx) => {
          return compareAggregationResults(ctx, 'fuzz_comp', docs, [
            {
              $project: {
                eq: { $eq: ['$a', '$b'] },
                ne: { $ne: ['$a', '$b'] },
                gt: { $gt: ['$a', '$b'] },
                lt: { $lt: ['$a', '$b'] },
              },
            },
          ]);
        }
      );
    });

    it('should match MongoDB behavior for $unwind with preserveNullAndEmptyArrays', async () => {
      await runDualTargetFuzz(
        '$unwind-preserve-comparison',
        fc.record({
          docs: fc.array(
            fc.record({
              name: bsonString,
              tags: fc.oneof(
                fc.array(bsonString, { minLength: 0, maxLength: 5 }),
                fc.constant(null)
              ),
            }),
            { minLength: 1, maxLength: 10 }
          ),
        }),
        async ({ docs }, ctx) => {
          return compareAggregationResults(ctx, 'fuzz_unwind_preserve', docs, [
            { $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } },
            { $sort: { name: 1 } },
          ]);
        }
      );
    });
  });
});
