/**
 * fast-check arbitraries for generating MongoDB documents.
 *
 * Documents are designed to exercise edge cases in query matching,
 * updates, and aggregation.
 */

import * as fc from 'fast-check';
import {
  bsonNull,
  bsonBoolean,
  bsonNumber,
  bsonString,
  mixedArray,
  numberArray,
  stringArray,
  jsonSafeBsonValue,
  edgeCaseValue,
} from './values.ts';

// ============================================================================
// Simple Documents
// ============================================================================

/**
 * Basic document with common field types.
 */
export const simpleDocument = fc.record({
  name: bsonString,
  value: bsonNumber,
  active: bsonBoolean,
});

/**
 * Document with optional fields (some may be missing).
 */
export const sparseDocument = fc.record(
  {
    name: bsonString,
    value: bsonNumber,
    count: bsonNumber,
    status: bsonString,
  },
  { requiredKeys: [] }
);

// ============================================================================
// Documents with Arrays
// ============================================================================

/**
 * Document containing array fields.
 */
export const documentWithArrays = fc.record({
  tags: stringArray,
  scores: numberArray,
  mixed: mixedArray,
});

/**
 * Document with nested array structure (for $elemMatch testing).
 */
export const documentWithNestedArrays = fc.record({
  items: fc.array(
    fc.record({
      name: bsonString,
      qty: fc.integer({ min: 0, max: 100 }),
      price: fc.double({ min: 0, max: 1000, noNaN: true }),
    }),
    { minLength: 0, maxLength: 5 }
  ),
});

// ============================================================================
// Documents with Null/Missing Values
// ============================================================================

/**
 * Document specifically for testing null matching.
 * { field: null } should match both null values AND missing fields.
 */
export const nullTestDocument = fc.oneof(
  fc.record({ field: bsonNull, other: bsonString }),
  fc.record({ other: bsonString }), // field is missing
  fc.record({ field: bsonNumber, other: bsonString }),
  fc.record({ field: bsonString, other: bsonString })
);

/**
 * Document with explicit null values in various positions.
 */
export const documentWithNulls = fc.record({
  nullField: fc.constant(null),
  maybeNull: fc.oneof(bsonNull, bsonNumber),
  arrayWithNulls: fc.array(fc.oneof(bsonNull, bsonNumber), { maxLength: 5 }),
});

// ============================================================================
// Nested Documents
// ============================================================================

/**
 * Document with nested structure for dot notation queries.
 */
export const nestedDocument = fc.record({
  top: bsonString,
  nested: fc.record({
    middle: bsonNumber,
    deep: fc.record({
      value: bsonString,
    }),
  }),
});

/**
 * Document with variable nesting depth.
 */
export const variableDepthDocument = fc.record({
  level1: fc.oneof(
    bsonNumber,
    fc.record({
      level2: fc.oneof(
        bsonNumber,
        fc.record({
          level3: bsonNumber,
        })
      ),
    })
  ),
});

// ============================================================================
// Edge Case Documents
// ============================================================================

/**
 * Document designed to trigger edge cases.
 */
export const edgeCaseDocument = fc.record({
  nullField: fc.constant(null),
  emptyString: fc.constant(''),
  emptyArray: fc.constant([]),
  emptyObject: fc.constant({}),
  zero: fc.constant(0),
  negativeZero: fc.constant(-0),
  mixedArray: mixedArray,
});

/**
 * Document with various value types for $type operator testing.
 */
export const typeTestDocument = fc.record({
  stringVal: bsonString,
  numberVal: bsonNumber,
  boolVal: bsonBoolean,
  nullVal: fc.constant(null),
  arrayVal: mixedArray,
  objectVal: fc.record({ inner: bsonNumber }),
});

// ============================================================================
// Document Collections (for batch operations)
// ============================================================================

/**
 * Generate a collection of documents for query testing.
 */
export const documentCollection = fc.array(simpleDocument, { minLength: 1, maxLength: 20 });

/**
 * Collection of documents with various structures.
 */
export const mixedDocumentCollection = fc.array(
  fc.oneof(
    simpleDocument,
    sparseDocument,
    documentWithArrays,
    nestedDocument,
    edgeCaseDocument
  ),
  { minLength: 1, maxLength: 15 }
);

/**
 * Collection of documents specifically for null/missing field testing.
 */
export const nullTestCollection = fc.array(nullTestDocument, { minLength: 1, maxLength: 10 });

// ============================================================================
// Generic Document Builder
// ============================================================================

/**
 * Generate a document with the given field names and random values.
 */
export function documentWithFields(fields: string[]): fc.Arbitrary<Record<string, unknown>> {
  const recordSpec: Record<string, fc.Arbitrary<unknown>> = {};
  for (const field of fields) {
    recordSpec[field] = jsonSafeBsonValue;
  }
  return fc.record(recordSpec) as fc.Arbitrary<Record<string, unknown>>;
}

/**
 * Generate a document with at least one of the specified fields.
 */
export function documentWithAtLeastOne(fields: string[]): fc.Arbitrary<Record<string, unknown>> {
  return fc
    .record(
      Object.fromEntries(fields.map((f) => [f, fc.option(jsonSafeBsonValue, { nil: undefined })])),
      { requiredKeys: [] }
    )
    .filter((doc) => Object.values(doc).some((v) => v !== undefined)) as fc.Arbitrary<
    Record<string, unknown>
  >;
}
