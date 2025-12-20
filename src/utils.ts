import { ObjectId } from "mongodb";

type Document = Record<string, unknown>;

/**
 * Projection specification type.
 * Keys are field names (can use dot notation).
 * Values are 1 for inclusion, 0 for exclusion.
 */
export type ProjectionSpec = Record<string, 0 | 1>;

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
