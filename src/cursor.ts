import { ObjectId } from "mongodb";
import {
  applyProjection,
  getValueByPath,
  type ProjectionSpec,
} from "./utils.ts";

type Document = Record<string, unknown>;

/**
 * Sort specification type.
 * Keys are field names (can use dot notation).
 * Values are 1 for ascending, -1 for descending.
 */
type SortSpec = Record<string, 1 | -1>;

/**
 * Symbol to represent an empty array in sorting.
 * Empty arrays sort before null in MongoDB.
 */
const EMPTY_ARRAY_MARKER = Symbol("emptyArray");

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
function getTypeOrder(value: unknown): number {
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
 * Get the sort key for an array value.
 * For ascending: returns minimum element
 * For descending: returns maximum element
 * Empty arrays are treated as less than null.
 */
function getArraySortKey(arr: unknown[], direction: 1 | -1): unknown {
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
 * Compare two scalar (non-array) values for sorting.
 * Returns negative if a < b, positive if a > b, 0 if equal.
 */
function compareScalarValues(a: unknown, b: unknown): number {
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
 * Compare two values for sorting, with array handling.
 * Arrays use their min element (ascending) or max element (descending) as sort key.
 */
function compareValuesForSort(
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
 * MongoneCursor represents a cursor over query results.
 * It mirrors the Cursor API from the official MongoDB driver.
 */
export class MongoneCursor<T extends Document = Document> {
  private readonly fetchDocuments: () => Promise<T[]>;
  private sortSpec: SortSpec | null = null;
  private limitValue: number | null = null;
  private skipValue: number | null = null;
  private projectionSpec: ProjectionSpec | null = null;

  constructor(
    fetchDocuments: () => Promise<T[]>,
    projection?: ProjectionSpec | null
  ) {
    this.fetchDocuments = fetchDocuments;
    this.projectionSpec = projection || null;
  }

  /**
   * Sort the results by the specified fields.
   * Returns this cursor for chaining.
   */
  sort(spec: SortSpec): MongoneCursor<T> {
    this.sortSpec = spec;
    return this;
  }

  /**
   * Limit the number of results returned.
   * Returns this cursor for chaining.
   * Negative values are treated as positive (MongoDB 3.2+ behavior).
   * limit(0) means no limit (returns all documents).
   */
  limit(n: number): MongoneCursor<T> {
    const absN = Math.abs(n);
    // limit(0) means no limit in MongoDB
    this.limitValue = absN === 0 ? null : absN;
    return this;
  }

  /**
   * Skip the first n results.
   * Returns this cursor for chaining.
   * @throws Error if n is negative (MongoDB behavior).
   */
  skip(n: number): MongoneCursor<T> {
    if (n < 0) {
      throw new Error("Skip value must be non-negative");
    }
    this.skipValue = n;
    return this;
  }

  /**
   * Return all documents as an array.
   * Applies sort, skip, and limit in that order.
   */
  async toArray(): Promise<T[]> {
    let docs = await this.fetchDocuments();

    // Apply sort
    if (this.sortSpec) {
      const sortFields = Object.entries(this.sortSpec) as [
        string,
        1 | -1,
      ][];
      docs = [...docs].sort((a, b) => {
        for (const [field, direction] of sortFields) {
          const aValue = getValueByPath(a, field);
          const bValue = getValueByPath(b, field);
          // Use direction-aware comparison for proper array handling
          const comparison = compareValuesForSort(aValue, bValue, direction);
          if (comparison !== 0) {
            return direction === 1 ? comparison : -comparison;
          }
        }
        return 0;
      });
    }

    // Apply skip
    if (this.skipValue !== null && this.skipValue > 0) {
      docs = docs.slice(this.skipValue);
    }

    // Apply limit
    if (this.limitValue !== null) {
      docs = docs.slice(0, this.limitValue);
    }

    // Apply projection
    if (this.projectionSpec) {
      docs = docs.map((doc) => applyProjection(doc, this.projectionSpec!));
    }

    return docs;
  }
}
