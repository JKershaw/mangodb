import { ObjectId } from "mongodb";

type Document = Record<string, unknown>;

/**
 * Projection specification type.
 * Keys are field names (can use dot notation).
 * Values are 1 for inclusion, 0 for exclusion.
 */
export type ProjectionSpec = Record<string, 0 | 1>;

/**
 * Symbol to represent an empty array in sorting.
 * Empty arrays sort before null in MongoDB.
 */
export const EMPTY_ARRAY_MARKER = Symbol("emptyArray");

/**
 * MongoDB BSON type ordering for sorting.
 * Based on https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
 * Lower numbers come first when sorting ascending.
 *
 * Order: MinKey(1) < EmptyArray(1.5) < Null(2) < Numbers(3) < String(4) < Object(5) < Array(6) <
 *        BinData(7) < ObjectId(8) < Boolean(9) < Date(10) < Timestamp(11) < RegExp(12) < MaxKey(13)
 *
 * Note: undefined/missing fields are treated as null.
 * Note: Empty arrays sort before null.
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
 * Compare two scalar (non-array) values for sorting.
 * Returns negative if a < b, positive if a > b, 0 if equal.
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
 * Get the sort key for an array value.
 * For ascending: returns minimum element
 * For descending: returns maximum element
 * Empty arrays are treated as less than null.
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
 * Compare two values for sorting, with array handling.
 * Arrays use their min element (ascending) or max element (descending) as sort key.
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
 * Deep clone a value, handling ObjectId and Date properly.
 */
export function cloneValue(value: unknown): unknown {
  if (value instanceof ObjectId) {
    return new ObjectId(value.toHexString());
  }
  if (value instanceof Date) {
    return new Date(value.getTime());
  }
  if (Array.isArray(value)) {
    return value.map((item) => cloneValue(item));
  }
  if (value !== null && typeof value === "object") {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) {
      result[k] = cloneValue(v);
    }
    return result;
  }
  return value;
}

/**
 * Get a value from a document using dot notation.
 */
export function getValueByPath(doc: unknown, path: string): unknown {
  const parts = path.split(".");
  let current: unknown = doc;

  for (const part of parts) {
    if (current === null || current === undefined) {
      return undefined;
    }

    if (Array.isArray(current)) {
      const index = parseInt(part, 10);
      if (!isNaN(index)) {
        current = current[index];
      } else {
        return undefined;
      }
    } else if (typeof current === "object") {
      current = (current as Record<string, unknown>)[part];
    } else {
      return undefined;
    }
  }

  return current;
}

/**
 * Set a value in a document using dot notation.
 */
export function setValueByPath(
  doc: Record<string, unknown>,
  path: string,
  value: unknown
): void {
  const parts = path.split(".");
  let current: Record<string, unknown> = doc;

  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i];
    if (current[part] === undefined) {
      current[part] = {};
    }
    current = current[part] as Record<string, unknown>;
  }

  current[parts[parts.length - 1]] = value;
}

/**
 * Apply projection to a document.
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
