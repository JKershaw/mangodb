/**
 * fast-check arbitraries for generating MongoDB query filters.
 *
 * Generates queries that exercise various operators and edge cases.
 */

import * as fc from 'fast-check';
import { bsonNumber, bsonString, bsonBoolean, jsonSafeBsonValue } from './values.ts';

// ============================================================================
// Comparison Operators
// ============================================================================

/**
 * Generate a comparison operator query for a field.
 */
export function comparisonQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.oneof(
    // Implicit $eq
    fc.record({ [field]: jsonSafeBsonValue }),
    // Explicit operators
    fc.record({ [field]: fc.record({ $eq: jsonSafeBsonValue }) }),
    fc.record({ [field]: fc.record({ $ne: jsonSafeBsonValue }) }),
    fc.record({ [field]: fc.record({ $gt: bsonNumber }) }),
    fc.record({ [field]: fc.record({ $gte: bsonNumber }) }),
    fc.record({ [field]: fc.record({ $lt: bsonNumber }) }),
    fc.record({ [field]: fc.record({ $lte: bsonNumber }) }),
    fc.record({
      [field]: fc.record({ $in: fc.array(jsonSafeBsonValue, { minLength: 1, maxLength: 5 }) }),
    }),
    fc.record({
      [field]: fc.record({ $nin: fc.array(jsonSafeBsonValue, { minLength: 1, maxLength: 5 }) }),
    })
  );
}

/**
 * Comparison query specifically for number fields.
 */
export function numericComparisonQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.oneof(
    fc.record({ [field]: bsonNumber }),
    fc.record({ [field]: fc.record({ $gt: bsonNumber }) }),
    fc.record({ [field]: fc.record({ $gte: bsonNumber }) }),
    fc.record({ [field]: fc.record({ $lt: bsonNumber }) }),
    fc.record({ [field]: fc.record({ $lte: bsonNumber }) }),
    fc.record({
      [field]: fc.record({
        $gt: bsonNumber,
        $lt: fc.integer({ min: 0, max: 1000 }),
      }),
    })
  );
}

// ============================================================================
// Null/Missing Field Queries (High Priority Edge Case)
// ============================================================================

/**
 * Query that tests null matching behavior.
 * { field: null } matches BOTH null values AND missing fields.
 */
export function nullQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.oneof(
    fc.record({ [field]: fc.constant(null) }),
    fc.record({ [field]: fc.record({ $eq: fc.constant(null) }) }),
    fc.record({ [field]: fc.record({ $ne: fc.constant(null) }) })
  );
}

// ============================================================================
// Existence Operators
// ============================================================================

/**
 * $exists operator query.
 */
export function existsQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    [field]: fc.record({ $exists: bsonBoolean }),
  });
}

/**
 * $type operator query with various type specifications.
 */
export function typeQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    [field]: fc.record({
      $type: fc.oneof(
        // String type names
        fc.constant('string'),
        fc.constant('number'),
        fc.constant('int'),
        fc.constant('double'),
        fc.constant('bool'),
        fc.constant('array'),
        fc.constant('object'),
        fc.constant('null'),
        fc.constant('date'),
        // Numeric type codes
        fc.constant(1), // double
        fc.constant(2), // string
        fc.constant(3), // object
        fc.constant(4), // array
        fc.constant(8), // bool
        fc.constant(10), // null
        fc.constant(16), // int
        fc.constant(18) // long
      ),
    }),
  });
}

// ============================================================================
// Array Operators (High Priority Edge Case)
// ============================================================================

/**
 * $all operator query.
 */
export function allQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    [field]: fc.record({
      $all: fc.array(jsonSafeBsonValue, { minLength: 1, maxLength: 3 }),
    }),
  });
}

/**
 * $size operator query.
 */
export function sizeQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    [field]: fc.record({
      $size: fc.integer({ min: 0, max: 10 }),
    }),
  });
}

/**
 * $elemMatch operator query.
 */
export function elemMatchQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    [field]: fc.record({
      $elemMatch: fc.oneof(
        // Match array of objects
        fc.record({
          qty: fc.record({ $gt: fc.integer({ min: 0, max: 50 }) }),
        }),
        // Match array of primitives with comparison
        fc.record({
          $gt: bsonNumber,
        }),
        // Match with multiple conditions
        fc.record({
          $gte: fc.integer({ min: 0, max: 50 }),
          $lt: fc.integer({ min: 50, max: 100 }),
        })
      ),
    }),
  });
}

// ============================================================================
// Logical Operators
// ============================================================================

/**
 * $and operator query.
 */
export function andQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $and: fc.array(comparisonQuery(field), { minLength: 1, maxLength: 3 }),
  });
}

/**
 * $or operator query.
 */
export function orQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $or: fc.array(comparisonQuery(field), { minLength: 1, maxLength: 3 }),
  });
}

/**
 * $not operator query.
 */
export function notQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    [field]: fc.record({
      $not: fc.oneof(
        fc.record({ $gt: bsonNumber }),
        fc.record({ $lt: bsonNumber }),
        fc.record({ $eq: jsonSafeBsonValue })
      ),
    }),
  });
}

/**
 * $nor operator query.
 */
export function norQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $nor: fc.array(comparisonQuery(field), { minLength: 1, maxLength: 3 }),
  });
}

// ============================================================================
// Combined Queries
// ============================================================================

/**
 * Any query operator for a given field.
 */
export function anyQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.oneof(
    { weight: 3, arbitrary: comparisonQuery(field) },
    { weight: 2, arbitrary: nullQuery(field) },
    { weight: 1, arbitrary: existsQuery(field) },
    { weight: 1, arbitrary: typeQuery(field) },
    { weight: 1, arbitrary: notQuery(field) }
  );
}

/**
 * Query for array fields.
 */
export function arrayFieldQuery(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.oneof(
    allQuery(field),
    sizeQuery(field),
    elemMatchQuery(field),
    // Scalar query on array field (matches if ANY element matches)
    fc.record({ [field]: bsonNumber }),
    fc.record({ [field]: bsonString })
  );
}

/**
 * Generate a complex multi-field query.
 */
export const multiFieldQuery: fc.Arbitrary<Record<string, unknown>> = fc.oneof(
  fc.record({
    name: fc.oneof(bsonString, fc.record({ $regex: fc.constant('.*') })),
    value: fc.record({ $gt: bsonNumber }),
  }),
  fc.record({
    $and: fc.tuple(
      fc.record({ name: bsonString }),
      fc.record({ value: fc.record({ $gte: bsonNumber }) })
    ),
  }),
  fc.record({
    $or: fc.tuple(
      fc.record({ name: bsonString }),
      fc.record({ active: bsonBoolean })
    ),
  })
);

// ============================================================================
// Edge Case Queries
// ============================================================================

/**
 * Queries specifically designed to test edge cases.
 */
export const edgeCaseQuery: fc.Arbitrary<Record<string, unknown>> = fc.oneof(
  // Empty query (matches all)
  fc.constant({}),
  // Query for null (matches null AND missing)
  fc.record({ field: fc.constant(null) }),
  // Query for empty array
  fc.record({ field: fc.constant([]) }),
  // Query for empty object
  fc.record({ field: fc.constant({}) }),
  // Query with $ne: null (matches non-null AND existing)
  fc.record({ field: fc.record({ $ne: fc.constant(null) }) }),
  // $in with null
  fc.record({ field: fc.record({ $in: fc.constant([null, 1, 'a']) }) }),
  // $nin with null
  fc.record({ field: fc.record({ $nin: fc.constant([null]) }) })
);
