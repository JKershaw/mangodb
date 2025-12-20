import { ObjectId } from "mongodb";

type Document = Record<string, unknown>;

/**
 * Sort specification type.
 * Keys are field names (can use dot notation).
 * Values are 1 for ascending, -1 for descending.
 */
type SortSpec = Record<string, 1 | -1>;

/**
 * Projection specification type.
 * Keys are field names (can use dot notation).
 * Values are 1 for inclusion, 0 for exclusion.
 */
type ProjectionSpec = Record<string, 0 | 1>;

/**
 * MongoDB type ordering for sorting.
 * Lower numbers come first when sorting ascending.
 */
function getTypeOrder(value: unknown): number {
  if (value === undefined) return 0; // Missing fields treated like null
  if (value === null) return 1;
  if (typeof value === "number") return 2;
  if (typeof value === "string") return 3;
  if (typeof value === "object" && !Array.isArray(value)) {
    if (value instanceof ObjectId) return 7;
    if (value instanceof Date) return 9;
    return 4; // Plain object
  }
  if (Array.isArray(value)) return 5;
  if (typeof value === "boolean") return 8;
  return 10; // Other types
}

/**
 * Compare two values for sorting.
 * Returns negative if a < b, positive if a > b, 0 if equal.
 */
function compareValues(a: unknown, b: unknown): number {
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

  // For arrays and objects, use JSON string comparison as fallback
  if (typeof a === "object" && typeof b === "object") {
    return JSON.stringify(a).localeCompare(JSON.stringify(b));
  }

  return 0;
}

/**
 * Get a value from a document using dot notation.
 */
function getValueByPath(doc: unknown, path: string): unknown {
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
 * Set a value in a document using dot notation (for projection).
 */
function setValueByPath(
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
function applyProjection<T extends Document>(
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
      result._id = doc._id;
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
    // Copy all fields first
    for (const [key, value] of Object.entries(doc)) {
      result[key] = value;
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
   */
  limit(n: number): MongoneCursor<T> {
    this.limitValue = n;
    return this;
  }

  /**
   * Skip the first n results.
   * Returns this cursor for chaining.
   */
  skip(n: number): MongoneCursor<T> {
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
      const sortFields = Object.entries(this.sortSpec);
      docs = [...docs].sort((a, b) => {
        for (const [field, direction] of sortFields) {
          const aValue = getValueByPath(a, field);
          const bValue = getValueByPath(b, field);
          const comparison = compareValues(aValue, bValue);
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
