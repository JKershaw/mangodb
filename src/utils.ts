import { ObjectId } from "bson";
import {
  getValueByPath,
  setValueByPath,
  cloneValue,
} from "./document-utils.ts";

type Document = Record<string, unknown>;

/**
 * Projection specification type for MongoDB-style field projections.
 *
 * @description
 * Defines which fields to include (1) or exclude (0) from documents.
 * Supports dot notation for nested fields (e.g., "address.city").
 * Cannot mix inclusion and exclusion modes (except for _id field).
 *
 * @example
 * // Inclusion projection - only include specified fields
 * const proj1: ProjectionSpec = { name: 1, age: 1 };
 *
 * @example
 * // Exclusion projection - exclude specified fields
 * const proj2: ProjectionSpec = { password: 0, ssn: 0 };
 *
 * @example
 * // _id can be excluded in inclusion mode
 * const proj3: ProjectionSpec = { _id: 0, name: 1, age: 1 };
 *
 * @example
 * // Nested field projection with dot notation
 * const proj4: ProjectionSpec = { "address.city": 1, "address.country": 1 };
 */
export type ProjectionSpec = Record<string, 0 | 1>;

/**
 * Get a value from a document using dot notation path.
 * Re-exported from document-utils for backward compatibility.
 *
 * @param doc - The document to retrieve the value from
 * @param path - The dot-notation path to the field (e.g., "user.address.city")
 * @returns The value at the specified path, or undefined if not found
 */
export { getValueByPath };

/**
 * Set a value in a document using dot notation path.
 * Re-exported from document-utils for backward compatibility.
 *
 * @param doc - The document to modify
 * @param path - The dot-notation path to the field (e.g., "user.address.city")
 * @param value - The value to set at the specified path
 */
export { setValueByPath };

/**
 * Create a deep clone of a value.
 * Re-exported from document-utils for backward compatibility.
 *
 * @param value - The value to clone (can be any type)
 * @returns A deep copy of the value
 */
export { cloneValue };

/**
 * Special marker symbol used to represent empty arrays in sorting operations.
 *
 * @description
 * In MongoDB's sort order, empty arrays are sorted before null values.
 * This symbol is used internally to represent empty arrays during comparisons
 * to ensure they sort correctly according to MongoDB's BSON type ordering.
 *
 * @example
 * // Empty arrays are converted to EMPTY_ARRAY_MARKER for sorting
 * const arr = [];
 * const sortKey = getArraySortKey(arr, 1); // Returns EMPTY_ARRAY_MARKER
 */
export const EMPTY_ARRAY_MARKER = Symbol("emptyArray");

/**
 * Get the MongoDB BSON type ordering number for a value.
 *
 * @description
 * Returns a numeric value representing the sort order of different BSON types
 * according to MongoDB's comparison rules. Lower numbers sort before higher numbers
 * in ascending order. This implements MongoDB's type precedence for comparisons.
 *
 * Type order (ascending):
 * - MinKey (1)
 * - Empty Array (1.5)
 * - Null/Undefined (2)
 * - Numbers (3)
 * - String (4)
 * - Object (5)
 * - Array (6)
 * - BinData (7)
 * - ObjectId (8)
 * - Boolean (9)
 * - Date (10)
 * - Timestamp (11)
 * - RegExp (12)
 * - MaxKey (13)
 *
 * @param value - The value to get the type order for
 * @returns A number representing the type's position in MongoDB's sort order
 *
 * @see {@link https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/}
 *
 * @example
 * getTypeOrder(null);           // Returns 2
 * getTypeOrder(42);              // Returns 3
 * getTypeOrder("hello");         // Returns 4
 * getTypeOrder({ name: "x" });   // Returns 5
 * getTypeOrder([1, 2, 3]);       // Returns 6
 * getTypeOrder(new ObjectId());  // Returns 8
 * getTypeOrder(true);            // Returns 9
 * getTypeOrder(new Date());      // Returns 10
 * getTypeOrder(EMPTY_ARRAY_MARKER); // Returns 1.5
 */
export function getTypeOrder(value: unknown): number {
  if (value === EMPTY_ARRAY_MARKER) return 1.5; // Empty array sorts before null
  if (value === undefined || value === null) return 2; // Null (and missing)
  if (typeof value === "number") return 3; // Numbers
  if (typeof value === "string") return 4; // String
  if (typeof value === "object" && !Array.isArray(value)) {
    if (value instanceof ObjectId) return 8; // ObjectId
    if (value instanceof Date) return 10; // Date
    return 5; // Plain object
  }
  if (Array.isArray(value)) return 6; // Array
  if (typeof value === "boolean") return 9; // Boolean
  return 13; // Other types (MaxKey equivalent)
}

/**
 * Compare two scalar (non-array) values for sorting using MongoDB's comparison rules.
 *
 * @description
 * Performs a comparison between two values following MongoDB's BSON type ordering
 * and value comparison semantics. First compares by type order, then by value
 * within the same type. Does not handle arrays (use compareValuesForSort for that).
 *
 * Comparison rules:
 * - Different types: compare by BSON type order
 * - Same type: compare by value using type-specific comparison
 * - Numbers: numeric comparison
 * - Strings: lexicographic comparison using localeCompare
 * - Booleans: false < true
 * - Dates: chronological order
 * - ObjectIds: hex string comparison
 * - Objects: JSON string comparison (fallback)
 *
 * @param a - The first value to compare
 * @param b - The second value to compare
 * @returns Negative number if a < b, positive if a > b, 0 if equal
 *
 * @example
 * compareScalarValues(1, 2);           // Returns negative (1 < 2)
 * compareScalarValues(5, 3);           // Returns positive (5 > 3)
 * compareScalarValues("apple", "banana"); // Returns negative (a < b)
 * compareScalarValues(null, 42);       // Returns negative (null < number)
 * compareScalarValues(true, false);    // Returns positive (true > false)
 *
 * @example
 * // Type ordering takes precedence
 * compareScalarValues(999, "1");       // Returns negative (number < string)
 * compareScalarValues(null, 0);        // Returns negative (null < number)
 */
export function compareScalarValues(a: unknown, b: unknown): number {
  const typeOrderA = getTypeOrder(a);
  const typeOrderB = getTypeOrder(b);

  // Different types: sort by type order
  if (typeOrderA !== typeOrderB) {
    return typeOrderA - typeOrderB;
  }

  // Same type: compare values
  if (a === null || a === undefined) return 0;

  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }

  if (typeof a === "string" && typeof b === "string") {
    return a.localeCompare(b);
  }

  if (typeof a === "boolean" && typeof b === "boolean") {
    // false < true
    if (a === b) return 0;
    return a ? 1 : -1;
  }

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  if (a instanceof ObjectId && b instanceof ObjectId) {
    return a.toHexString().localeCompare(b.toHexString());
  }

  // For objects, use JSON string comparison as fallback
  if (typeof a === "object" && typeof b === "object") {
    return JSON.stringify(a).localeCompare(JSON.stringify(b));
  }

  return 0;
}

/**
 * Extract the sort key from an array based on sort direction.
 *
 * @description
 * In MongoDB, when sorting by a field that contains arrays, the array's sort key
 * depends on the sort direction:
 * - Ascending (1): use the minimum (smallest) element in the array
 * - Descending (-1): use the maximum (largest) element in the array
 * Empty arrays return a special EMPTY_ARRAY_MARKER that sorts before null.
 *
 * @param arr - The array to extract the sort key from
 * @param direction - Sort direction: 1 for ascending, -1 for descending
 * @returns The minimum element (ascending) or maximum element (descending),
 *          or EMPTY_ARRAY_MARKER if the array is empty
 *
 * @example
 * // Ascending: get minimum element
 * getArraySortKey([5, 2, 8, 1], 1);  // Returns 1
 *
 * @example
 * // Descending: get maximum element
 * getArraySortKey([5, 2, 8, 1], -1); // Returns 8
 *
 * @example
 * // Empty arrays return special marker
 * getArraySortKey([], 1);  // Returns EMPTY_ARRAY_MARKER
 *
 * @example
 * // Works with mixed types (uses BSON type ordering)
 * getArraySortKey([null, 42, "text"], 1);  // Returns null (lowest type order)
 * getArraySortKey([null, 42, "text"], -1); // Returns "text" (highest type order)
 */
export function getArraySortKey(arr: unknown[], direction: 1 | -1): unknown {
  if (arr.length === 0) {
    // Empty array sorts before null in MongoDB
    return EMPTY_ARRAY_MARKER;
  }

  // Find min (ascending) or max (descending) element
  let result = arr[0];
  for (let i = 1; i < arr.length; i++) {
    const cmp = compareScalarValues(arr[i], result);
    if (direction === 1 && cmp < 0) {
      result = arr[i]; // Found smaller element for ascending
    } else if (direction === -1 && cmp > 0) {
      result = arr[i]; // Found larger element for descending
    }
  }
  return result;
}

/**
 * Compare two values for sorting with proper MongoDB array handling.
 *
 * @description
 * Compares two values according to MongoDB sort semantics, including special
 * handling for arrays. Arrays are compared using their minimum element (for
 * ascending sorts) or maximum element (for descending sorts). Non-array values
 * are compared directly using BSON type ordering rules.
 *
 * This is the main comparison function used for sorting documents by field values.
 *
 * @param a - The first value to compare
 * @param b - The second value to compare
 * @param direction - Sort direction: 1 for ascending, -1 for descending
 * @returns Negative number if a < b, positive if a > b, 0 if equal
 *
 * @example
 * // Scalar values work like normal comparison
 * compareValuesForSort(1, 2, 1);        // Returns negative (1 < 2)
 * compareValuesForSort("b", "a", 1);    // Returns positive (b > a)
 *
 * @example
 * // Ascending: arrays use minimum element
 * compareValuesForSort([5, 2, 8], 3, 1);  // Compares 2 (min) vs 3, returns negative
 * compareValuesForSort([5, 2, 8], 1, 1);  // Compares 2 (min) vs 1, returns positive
 *
 * @example
 * // Descending: arrays use maximum element
 * compareValuesForSort([5, 2, 8], 7, -1); // Compares 8 (max) vs 7, returns positive
 * compareValuesForSort([5, 2, 8], 9, -1); // Compares 8 (max) vs 9, returns negative
 *
 * @example
 * // Empty arrays sort before null
 * compareValuesForSort([], null, 1);      // Returns negative
 * compareValuesForSort([], 0, 1);         // Returns negative (empty array < number)
 */
export function compareValuesForSort(
  a: unknown,
  b: unknown,
  direction: 1 | -1
): number {
  // Handle arrays specially - extract sort key based on direction
  const aKey = Array.isArray(a) ? getArraySortKey(a, direction) : a;
  const bKey = Array.isArray(b) ? getArraySortKey(b, direction) : b;

  return compareScalarValues(aKey, bKey);
}

/**
 * Apply a MongoDB-style projection to a document, selecting or excluding fields.
 *
 * @description
 * Applies a projection specification to filter which fields are included in the
 * returned document. Supports two modes:
 *
 * 1. Inclusion mode (field: 1): Only specified fields are included
 *    - The _id field is included by default unless explicitly excluded
 *    - Example: { name: 1, age: 1 } returns only name and age (plus _id)
 *
 * 2. Exclusion mode (field: 0): All fields except specified ones are included
 *    - All fields are included except those marked with 0
 *    - Example: { password: 0 } returns all fields except password
 *
 * Supports dot notation for nested fields (e.g., "address.city").
 * Cannot mix inclusion and exclusion modes (except _id can be excluded in inclusion mode).
 *
 * @template T - The document type
 * @param doc - The document to apply projection to
 * @param projection - The projection specification (field names to 1 or 0)
 * @returns A new document with only the projected fields (does not modify original)
 *
 * @throws {Error} If projection mixes inclusion and exclusion modes (except for _id)
 *
 * @example
 * // Inclusion projection - only include specified fields
 * const user = { _id: 1, name: "Alice", age: 30, password: "secret" };
 * applyProjection(user, { name: 1, age: 1 });
 * // Returns: { _id: 1, name: "Alice", age: 30 }
 *
 * @example
 * // Exclude _id in inclusion mode
 * applyProjection(user, { _id: 0, name: 1 });
 * // Returns: { name: "Alice" }
 *
 * @example
 * // Exclusion projection - exclude specified fields
 * applyProjection(user, { password: 0 });
 * // Returns: { _id: 1, name: "Alice", age: 30 }
 *
 * @example
 * // Nested field projection with dot notation
 * const doc = { name: "Bob", address: { city: "NYC", zip: "10001" } };
 * applyProjection(doc, { "address.city": 1 });
 * // Returns: { _id: undefined, address: { city: "NYC" } }
 *
 * @example
 * // Empty projection returns original document
 * applyProjection(user, {});
 * // Returns: { _id: 1, name: "Alice", age: 30, password: "secret" }
 *
 * @example
 * // Mixing inclusion and exclusion throws error
 * applyProjection(user, { name: 1, password: 0 });
 * // Throws: Error("Cannot mix inclusion and exclusion in projection")
 */
export function applyProjection<T extends Document>(
  doc: T,
  projection: ProjectionSpec
): T {
  const keys = Object.keys(projection);
  if (keys.length === 0) return doc;

  // Determine if this is inclusion or exclusion mode
  // _id: 0 doesn't count for determining mode
  const nonIdKeys = keys.filter((k) => k !== "_id");
  const hasInclusion = nonIdKeys.some((k) => projection[k] === 1);
  const hasExclusion = nonIdKeys.some((k) => projection[k] === 0);

  // Can't mix inclusion and exclusion (except for _id)
  if (hasInclusion && hasExclusion) {
    throw new Error("Cannot mix inclusion and exclusion in projection");
  }

  const result: Record<string, unknown> = {};

  if (hasInclusion) {
    // Inclusion mode: only include specified fields
    // Always include _id unless explicitly excluded
    if (projection._id !== 0) {
      result._id = (doc as Record<string, unknown>)._id;
    }

    for (const key of nonIdKeys) {
      if (projection[key] === 1) {
        const value = getValueByPath(doc, key);
        if (value !== undefined) {
          if (key.includes(".")) {
            setValueByPath(result, key, value);
          } else {
            result[key] = value;
          }
        }
      }
    }
  } else {
    // Exclusion mode: include all fields except specified
    // Deep copy all fields first to avoid mutating original
    for (const [key, value] of Object.entries(doc)) {
      result[key] = cloneValue(value);
    }

    // Remove excluded fields
    for (const key of keys) {
      if (projection[key] === 0) {
        if (key.includes(".")) {
          // Handle nested field exclusion
          const parts = key.split(".");
          let current: Record<string, unknown> = result;
          for (let i = 0; i < parts.length - 1; i++) {
            if (current[parts[i]] && typeof current[parts[i]] === "object") {
              current = current[parts[i]] as Record<string, unknown>;
            } else {
              break;
            }
          }
          delete current[parts[parts.length - 1]];
        } else {
          delete result[key];
        }
      }
    }
  }

  return result as T;
}
