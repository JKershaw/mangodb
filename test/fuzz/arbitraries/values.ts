/**
 * fast-check arbitraries for generating BSON-compatible values.
 *
 * Focuses on edge cases that commonly cause behavioral differences
 * between MangoDB and MongoDB.
 */

import * as fc from 'fast-check';
import { ObjectId } from 'bson';

// ============================================================================
// Primitive Values
// ============================================================================

/** Null value */
export const bsonNull = fc.constant(null);

/** Boolean value */
export const bsonBoolean = fc.boolean();

// ============================================================================
// Numbers (with edge cases)
// ============================================================================

/** Integer values including edge cases */
export const bsonInteger = fc.oneof(
  fc.integer({ min: -1000000, max: 1000000 }),
  fc.constant(0),
  fc.constant(-0),
  fc.constant(1),
  fc.constant(-1),
  fc.constant(Number.MAX_SAFE_INTEGER),
  fc.constant(Number.MIN_SAFE_INTEGER)
);

/** Double values (excluding NaN and Infinity which don't survive JSON) */
export const bsonDouble = fc.oneof(
  fc.double({ noNaN: true, noDefaultInfinity: true, min: -1e10, max: 1e10 }),
  fc.constant(0.0),
  fc.constant(-0.0),
  fc.constant(0.1),
  fc.constant(0.2),
  fc.constant(0.1 + 0.2) // Famous floating point edge case
);

/** Any number (integer or double) */
export const bsonNumber = fc.oneof(bsonInteger, bsonDouble);

// ============================================================================
// Strings (with edge cases)
// ============================================================================

/** Simple ASCII string */
export const simpleString = fc.string({ minLength: 0, maxLength: 50 });

/** String with special characters */
export const specialString = fc.oneof(
  fc.constant(''),
  fc.constant(' '),
  fc.constant('\t'),
  fc.constant('\n'),
  fc.constant('$'),
  fc.constant('$field'),
  fc.constant('field.with.dots'),
  fc.constant('field$with$dollars'),
  fc.constant('null'),
  fc.constant('undefined'),
  fc.constant('true'),
  fc.constant('false'),
  fc.constant('0'),
  fc.constant('-1'),
  fc.string({ minLength: 0, maxLength: 20, unit: 'grapheme' })
);

/** Any string value */
export const bsonString = fc.oneof(
  { weight: 3, arbitrary: simpleString },
  { weight: 1, arbitrary: specialString }
);

// ============================================================================
// Dates
// ============================================================================

/** Date value */
export const bsonDate = fc.date({
  min: new Date('1970-01-01'),
  max: new Date('2100-01-01'),
});

// ============================================================================
// ObjectId
// ============================================================================

/** Valid ObjectId */
export const bsonObjectId = fc
  .array(fc.integer({ min: 0, max: 255 }), { minLength: 12, maxLength: 12 })
  .map((bytes) => new ObjectId(Buffer.from(bytes)));

// ============================================================================
// Arrays (with edge cases)
// ============================================================================

/** Empty array */
export const emptyArray = fc.constant([]);

/** Array of numbers */
export const numberArray = fc.array(bsonNumber, { minLength: 0, maxLength: 10 });

/** Array of strings */
export const stringArray = fc.array(bsonString, { minLength: 0, maxLength: 10 });

/** Array with mixed types (common edge case) */
export const mixedArray = fc.array(
  fc.oneof(
    bsonNull,
    bsonBoolean,
    bsonNumber,
    bsonString,
    // Nested arrays
    fc.array(bsonNumber, { maxLength: 3 })
  ),
  { minLength: 0, maxLength: 10 }
);

/** Array that might contain null values */
export const arrayWithNulls = fc.array(
  fc.oneof(bsonNull, bsonNumber, bsonString),
  { minLength: 0, maxLength: 10 }
);

// ============================================================================
// Objects (nested structures)
// ============================================================================

/** Simple object with string keys */
export const simpleObject = fc.dictionary(
  fc.string({ minLength: 1, maxLength: 20 }).filter((s) => !s.includes('.') && !s.startsWith('$')),
  fc.oneof(bsonNull, bsonBoolean, bsonNumber, bsonString)
);

/** Empty object */
export const emptyObject = fc.constant({});

// ============================================================================
// Composite Values
// ============================================================================

/**
 * Any JSON-safe BSON value (values that survive JSON round-trip).
 * Excludes: undefined, NaN, Infinity, functions, symbols
 */
export const jsonSafeBsonValue: fc.Arbitrary<unknown> = fc.oneof(
  { weight: 2, arbitrary: bsonNull },
  { weight: 2, arbitrary: bsonBoolean },
  { weight: 3, arbitrary: bsonNumber },
  { weight: 3, arbitrary: bsonString },
  { weight: 2, arbitrary: mixedArray },
  { weight: 1, arbitrary: simpleObject }
);

/**
 * Value specifically designed to test edge cases.
 */
export const edgeCaseValue = fc.oneof(
  bsonNull,
  fc.constant(0),
  fc.constant(-0),
  fc.constant(''),
  fc.constant([]),
  fc.constant({}),
  fc.constant([null]),
  fc.constant([[]]),
  fc.constant({ a: null }),
  fc.constant([1, 'two', null, true])
);

// ============================================================================
// Field names (for use in documents)
// ============================================================================

/** Valid MongoDB field name */
export const validFieldName = fc
  .string({ minLength: 1, maxLength: 20 })
  .filter((s) => !s.includes('.') && !s.startsWith('$') && s !== '_id')
  .map((s) => s || 'field'); // Ensure non-empty

/** Field name that might cause issues */
export const edgeCaseFieldName = fc.oneof(
  validFieldName,
  fc.constant('a'),
  fc.constant('value'),
  fc.constant('name'),
  fc.constant('items'),
  fc.constant('nested')
);
