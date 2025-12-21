/**
 * Document utilities for serialization, path access, comparison, and cloning.
 */
import { ObjectId } from "mongodb";
import type { Document } from "./types.ts";

/**
 * Type guard for serialized ObjectId format.
 */
export function isSerializedObjectId(value: unknown): value is { $oid: string } {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "$oid" in value &&
    typeof (value as { $oid: unknown }).$oid === "string"
  );
}

/**
 * Type guard for serialized Date format.
 */
export function isSerializedDate(value: unknown): value is { $date: string } {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "$date" in value &&
    typeof (value as { $date: unknown }).$date === "string"
  );
}

/**
 * Check if a value is a plain object (not null, not array, not special type).
 */
export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    !(value instanceof ObjectId) &&
    !(value instanceof Date)
  );
}

/**
 * Transform a single value for serialization or deserialization.
 */
export function transformValue(
  value: unknown,
  mode: "serialize" | "deserialize"
): unknown {
  if (mode === "serialize") {
    if (value instanceof ObjectId) {
      return { $oid: value.toHexString() };
    }
    if (value instanceof Date) {
      return { $date: value.toISOString() };
    }
  } else {
    if (isSerializedObjectId(value)) {
      return new ObjectId(value.$oid);
    }
    if (isSerializedDate(value)) {
      return new Date(value.$date);
    }
  }

  if (Array.isArray(value)) {
    return value.map((item) => transformValue(item, mode));
  }

  if (isPlainObject(value)) {
    return transformDocument(value, mode);
  }

  return value;
}

/**
 * Transform all values in a document for serialization or deserialization.
 */
export function transformDocument(
  doc: Record<string, unknown>,
  mode: "serialize" | "deserialize"
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(doc)) {
    result[key] = transformValue(value, mode);
  }
  return result;
}

/**
 * Serialize a document for JSON storage.
 */
export function serializeDocument<T extends Document>(
  doc: T
): Record<string, unknown> {
  return transformDocument(doc, "serialize");
}

/**
 * Deserialize a document from JSON storage.
 */
export function deserializeDocument<T extends Document>(
  doc: Record<string, unknown>
): T {
  return transformDocument(doc, "deserialize") as T;
}

/**
 * Get all possible values from a document using dot notation.
 * When traversing arrays with non-numeric path segments, returns values from all elements.
 */
export function getValuesByPath(doc: unknown, path: string): unknown[] {
  const parts = path.split(".");
  let currentValues: unknown[] = [doc];

  for (const part of parts) {
    const nextValues: unknown[] = [];

    for (const current of currentValues) {
      if (current === null || current === undefined) {
        nextValues.push(undefined);
        continue;
      }

      if (Array.isArray(current)) {
        const index = parseInt(part, 10);
        if (!isNaN(index) && index >= 0) {
          nextValues.push(current[index]);
        } else {
          for (const elem of current) {
            if (elem !== null && typeof elem === "object" && !Array.isArray(elem)) {
              nextValues.push((elem as Record<string, unknown>)[part]);
            } else if (Array.isArray(elem)) {
              for (const nested of elem) {
                if (nested !== null && typeof nested === "object") {
                  nextValues.push((nested as Record<string, unknown>)[part]);
                }
              }
            }
          }
        }
      } else if (typeof current === "object") {
        nextValues.push((current as Record<string, unknown>)[part]);
      } else {
        nextValues.push(undefined);
      }
    }

    currentValues = nextValues;
  }

  return currentValues;
}

/**
 * Get a single value from a document using dot notation (first non-undefined).
 */
export function getValueByPath(doc: unknown, path: string): unknown {
  const values = getValuesByPath(doc, path);
  return values.find((v) => v !== undefined) ?? values[0];
}

/**
 * Set a value at a given path using dot notation.
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
    const nextPart = parts[i + 1];
    const isNextNumeric = /^\d+$/.test(nextPart);

    if (current[part] === undefined || current[part] === null) {
      current[part] = isNextNumeric ? [] : {};
    }

    current = current[part] as Record<string, unknown>;
  }

  const lastPart = parts[parts.length - 1];
  current[lastPart] = value;
}

/**
 * Delete a value at a given path using dot notation.
 */
export function deleteValueByPath(doc: Record<string, unknown>, path: string): void {
  const parts = path.split(".");
  let current: Record<string, unknown> = doc;

  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i];
    if (
      current[part] === undefined ||
      current[part] === null ||
      typeof current[part] !== "object"
    ) {
      return;
    }
    current = current[part] as Record<string, unknown>;
  }

  const lastPart = parts[parts.length - 1];
  delete current[lastPart];
}

/**
 * Get a value at a given path for updates (direct access, no array traversal).
 */
export function getValueAtPath(doc: Record<string, unknown>, path: string): unknown {
  const parts = path.split(".");
  let current: unknown = doc;

  for (const part of parts) {
    if (current === undefined || current === null) {
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
 * Compare two values according to MongoDB comparison rules.
 */
export function compareValues(a: unknown, b: unknown): number {
  if (a instanceof ObjectId && b instanceof ObjectId) {
    return a.toHexString().localeCompare(b.toHexString());
  }

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  if (typeof a === typeof b) {
    if (typeof a === "number" && typeof b === "number") {
      return a - b;
    }
    if (typeof a === "string" && typeof b === "string") {
      return a.localeCompare(b);
    }
    if (typeof a === "boolean" && typeof b === "boolean") {
      if (a === b) return 0;
      return a ? 1 : -1;
    }
  }

  return NaN;
}

/**
 * Check if two values are equal according to MongoDB rules.
 */
export function valuesEqual(a: unknown, b: unknown, strictKeyOrder = false): boolean {
  if (a instanceof ObjectId && b instanceof ObjectId) {
    return a.equals(b);
  }

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((val, idx) => valuesEqual(val, b[idx], strictKeyOrder));
  }

  if (a === null && b === null) {
    return true;
  }

  if (
    a !== null &&
    b !== null &&
    typeof a === "object" &&
    typeof b === "object" &&
    !Array.isArray(a) &&
    !Array.isArray(b)
  ) {
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b);
    if (aKeys.length !== bKeys.length) return false;

    if (strictKeyOrder) {
      for (let i = 0; i < aKeys.length; i++) {
        if (aKeys[i] !== bKeys[i]) return false;
        if (
          !valuesEqual(
            (a as Record<string, unknown>)[aKeys[i]],
            (b as Record<string, unknown>)[bKeys[i]],
            strictKeyOrder
          )
        ) {
          return false;
        }
      }
      return true;
    } else {
      return aKeys.every((key) =>
        valuesEqual(
          (a as Record<string, unknown>)[key],
          (b as Record<string, unknown>)[key],
          strictKeyOrder
        )
      );
    }
  }

  return a === b;
}

/**
 * Deep clone a value.
 */
export function cloneValue(value: unknown): unknown {
  if (value instanceof ObjectId) {
    return new ObjectId(value.toHexString());
  } else if (value instanceof Date) {
    return new Date(value.getTime());
  } else if (Array.isArray(value)) {
    return value.map((item) => cloneValue(item));
  } else if (value && typeof value === "object") {
    return cloneDocument(value as Document);
  }
  return value;
}

/**
 * Deep clone a document.
 */
export function cloneDocument<T extends Document>(doc: T): T {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(doc)) {
    result[key] = cloneValue(value);
  }
  return result as T;
}

/**
 * Check if two documents are deeply equal.
 */
export function documentsEqual<T extends Document>(a: T, b: T): boolean {
  const aKeys = Object.keys(a);
  const bKeys = Object.keys(b);

  if (aKeys.length !== bKeys.length) return false;

  for (const key of aKeys) {
    const aVal = (a as Record<string, unknown>)[key];
    const bVal = (b as Record<string, unknown>)[key];

    if (!valuesEqual(aVal, bVal)) {
      return false;
    }
  }

  return true;
}
