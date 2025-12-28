/**
 * fast-check arbitraries for generating MongoDB update operations.
 *
 * Generates updates that exercise various operators and edge cases.
 */

import * as fc from 'fast-check';
import { bsonNumber, bsonString, bsonBoolean, jsonSafeBsonValue, mixedArray } from './values.ts';

// ============================================================================
// Field Update Operators
// ============================================================================

/**
 * $set operator update.
 */
export function setUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $set: fc.record({ [field]: jsonSafeBsonValue }),
  });
}

/**
 * $unset operator update.
 */
export function unsetUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $unset: fc.record({ [field]: fc.constant('') }),
  });
}

/**
 * $inc operator update.
 */
export function incUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $inc: fc.record({ [field]: bsonNumber }),
  });
}

/**
 * $mul operator update.
 */
export function mulUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $mul: fc.record({ [field]: bsonNumber }),
  });
}

/**
 * $min operator update.
 */
export function minUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $min: fc.record({ [field]: bsonNumber }),
  });
}

/**
 * $max operator update.
 */
export function maxUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $max: fc.record({ [field]: bsonNumber }),
  });
}

/**
 * $rename operator update.
 */
export function renameUpdate(
  oldField: string,
  newField: string
): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $rename: fc.record({ [oldField]: fc.constant(newField) }),
  });
}

// ============================================================================
// Array Update Operators
// ============================================================================

/**
 * $push operator update (simple).
 */
export function pushUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $push: fc.record({ [field]: jsonSafeBsonValue }),
  });
}

/**
 * $push with $each modifier.
 */
export function pushEachUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $push: fc.record({
      [field]: fc.record({
        $each: fc.array(jsonSafeBsonValue, { minLength: 1, maxLength: 5 }),
      }),
    }),
  });
}

/**
 * $push with $each, $slice, and $sort modifiers.
 */
export function pushWithModifiersUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $push: fc.record({
      [field]: fc.record({
        $each: fc.array(bsonNumber, { minLength: 1, maxLength: 5 }),
        $slice: fc.integer({ min: -10, max: 10 }),
        $sort: fc.constant(1),
      }),
    }),
  });
}

/**
 * $addToSet operator update.
 */
export function addToSetUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $addToSet: fc.record({ [field]: jsonSafeBsonValue }),
  });
}

/**
 * $addToSet with $each.
 */
export function addToSetEachUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $addToSet: fc.record({
      [field]: fc.record({
        $each: fc.array(jsonSafeBsonValue, { minLength: 1, maxLength: 5 }),
      }),
    }),
  });
}

/**
 * $pop operator update.
 */
export function popUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $pop: fc.record({
      [field]: fc.constantFrom(-1, 1), // -1 = first, 1 = last
    }),
  });
}

/**
 * $pull operator update.
 */
export function pullUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $pull: fc.record({
      [field]: fc.oneof(
        // Pull specific value
        jsonSafeBsonValue,
        // Pull with condition
        fc.record({ $gt: bsonNumber }),
        fc.record({ $lt: bsonNumber })
      ),
    }),
  });
}

/**
 * $pullAll operator update.
 */
export function pullAllUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $pullAll: fc.record({
      [field]: fc.array(jsonSafeBsonValue, { minLength: 1, maxLength: 5 }),
    }),
  });
}

// ============================================================================
// Positional Operators
// ============================================================================

/**
 * Update with positional $ operator.
 */
export function positionalUpdate(arrayField: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $set: fc.record({ [`${arrayField}.$`]: jsonSafeBsonValue }),
  });
}

/**
 * Update with $[] (all positional) operator.
 */
export function allPositionalUpdate(arrayField: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $set: fc.record({ [`${arrayField}.$[]`]: jsonSafeBsonValue }),
  });
}

/**
 * Update with $[identifier] (filtered positional) operator.
 */
export function filteredPositionalUpdate(
  arrayField: string
): fc.Arbitrary<Record<string, unknown>> {
  return fc.record({
    $set: fc.record({ [`${arrayField}.$[elem]`]: bsonNumber }),
  });
}

// ============================================================================
// Combined Updates
// ============================================================================

/**
 * Any field update operator.
 */
export function anyFieldUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.oneof(
    { weight: 3, arbitrary: setUpdate(field) },
    { weight: 2, arbitrary: incUpdate(field) },
    { weight: 1, arbitrary: mulUpdate(field) },
    { weight: 1, arbitrary: minUpdate(field) },
    { weight: 1, arbitrary: maxUpdate(field) },
    { weight: 1, arbitrary: unsetUpdate(field) }
  );
}

/**
 * Any array update operator.
 */
export function anyArrayUpdate(field: string): fc.Arbitrary<Record<string, unknown>> {
  return fc.oneof(
    { weight: 3, arbitrary: pushUpdate(field) },
    { weight: 2, arbitrary: pushEachUpdate(field) },
    { weight: 2, arbitrary: addToSetUpdate(field) },
    { weight: 1, arbitrary: popUpdate(field) },
    { weight: 1, arbitrary: pullUpdate(field) }
  );
}

/**
 * Multi-field update.
 */
export const multiFieldUpdate: fc.Arbitrary<Record<string, unknown>> = fc.record({
  $set: fc.record({
    name: bsonString,
    value: bsonNumber,
  }),
});

/**
 * Update with multiple operators.
 */
export const multiOperatorUpdate: fc.Arbitrary<Record<string, unknown>> = fc.record({
  $set: fc.record({ name: bsonString }),
  $inc: fc.record({ count: fc.integer({ min: 1, max: 10 }) }),
});

// ============================================================================
// Edge Case Updates
// ============================================================================

/**
 * Updates specifically designed to test edge cases.
 */
export const edgeCaseUpdate: fc.Arbitrary<Record<string, unknown>> = fc.oneof(
  // $set to null
  fc.record({ $set: fc.record({ field: fc.constant(null) }) }),
  // $set to empty array
  fc.record({ $set: fc.record({ field: fc.constant([]) }) }),
  // $set to empty object
  fc.record({ $set: fc.record({ field: fc.constant({}) }) }),
  // $inc on missing field (should create it)
  fc.record({ $inc: fc.record({ newField: fc.constant(1) }) }),
  // $push to missing field (should create array)
  fc.record({ $push: fc.record({ newArray: bsonNumber }) }),
  // $addToSet with object (key order matters)
  fc.record({ $addToSet: fc.record({ items: fc.constant({ a: 1, b: 2 }) }) })
);
