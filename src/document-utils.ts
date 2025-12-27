/**
 * Document utilities for serialization, path access, comparison, and cloning.
 */
import { ObjectId } from "bson";
import type { Document } from "./types.ts";

/**
 * Type guard for serialized ObjectId format.
 * @description Checks if a value matches the serialized ObjectId format ({ $oid: string }).
 * This format is used when storing MongoDB ObjectIds as JSON.
 * @param value - The value to check
 * @returns True if the value is a serialized ObjectId, false otherwise
 * @example
 * isSerializedObjectId({ $oid: "507f1f77bcf86cd799439011" }) // true
 * isSerializedObjectId({ id: "123" }) // false
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
 * @description Checks if a value matches the serialized Date format ({ $date: string }).
 * This format is used when storing JavaScript Date objects as JSON.
 * @param value - The value to check
 * @returns True if the value is a serialized Date, false otherwise
 * @example
 * isSerializedDate({ $date: "2023-01-01T00:00:00.000Z" }) // true
 * isSerializedDate({ timestamp: "2023-01-01" }) // false
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
 * @description Determines if a value is a plain JavaScript object, excluding null, arrays,
 * ObjectId instances, and Date instances.
 * @param value - The value to check
 * @returns True if the value is a plain object, false otherwise
 * @example
 * isPlainObject({ name: "John" }) // true
 * isPlainObject([1, 2, 3]) // false
 * isPlainObject(new Date()) // false
 * isPlainObject(null) // false
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
 * @description Recursively transforms a value between MongoDB types (ObjectId, Date) and their
 * JSON-serializable representations. Handles nested objects and arrays.
 * @param value - The value to transform
 * @param mode - Either "serialize" (to JSON format) or "deserialize" (from JSON format)
 * @returns The transformed value
 * @example
 * // Serialize ObjectId to JSON
 * transformValue(new ObjectId("507f1f77bcf86cd799439011"), "serialize")
 * // { $oid: "507f1f77bcf86cd799439011" }
 *
 * // Deserialize from JSON to ObjectId
 * transformValue({ $oid: "507f1f77bcf86cd799439011" }, "deserialize")
 * // ObjectId("507f1f77bcf86cd799439011")
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
 * @description Transforms all fields in a document by applying transformValue to each field.
 * Used internally by serializeDocument and deserializeDocument.
 * @param doc - The document to transform
 * @param mode - Either "serialize" (to JSON format) or "deserialize" (from JSON format)
 * @returns A new document with all values transformed
 * @example
 * transformDocument({ _id: new ObjectId("..."), date: new Date() }, "serialize")
 * // { _id: { $oid: "..." }, date: { $date: "2023-01-01T00:00:00.000Z" } }
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
 * @description Converts a MongoDB document to a JSON-serializable format by transforming
 * ObjectIds to { $oid: string } and Dates to { $date: string }.
 * @param doc - The document to serialize
 * @returns The serialized document with all special types converted to JSON-safe format
 * @example
 * const doc = { _id: new ObjectId("507f1f77bcf86cd799439011"), createdAt: new Date() };
 * serializeDocument(doc)
 * // { _id: { $oid: "507f1f77bcf86cd799439011" }, createdAt: { $date: "..." } }
 */
export function serializeDocument<T extends Document>(
  doc: T
): Record<string, unknown> {
  return transformDocument(doc, "serialize");
}

/**
 * Deserialize a document from JSON storage.
 * @description Converts a JSON-serialized document back to its original MongoDB format by
 * transforming { $oid: string } to ObjectIds and { $date: string } to Dates.
 * @param doc - The serialized document to deserialize
 * @returns The deserialized document with all special types restored
 * @example
 * const serialized = { _id: { $oid: "507f1f77bcf86cd799439011" }, createdAt: { $date: "..." } };
 * deserializeDocument(serialized)
 * // { _id: ObjectId("507f1f77bcf86cd799439011"), createdAt: Date(...) }
 */
export function deserializeDocument<T extends Document>(
  doc: Record<string, unknown>
): T {
  return transformDocument(doc, "deserialize") as T;
}

/**
 * Get all possible values from a document using dot notation.
 * @description Retrieves all values matching a dot-notation path. When traversing arrays with
 * non-numeric path segments, returns values from all matching elements. This matches MongoDB's
 * query behavior for nested arrays.
 * @param doc - The document to query
 * @param path - Dot-notation path (e.g., "user.address.city", "items.0.name")
 * @returns Array of all values found at the path (may include undefined)
 * @example
 * const doc = { items: [{ name: "A" }, { name: "B" }] };
 * getValuesByPath(doc, "items.name") // ["A", "B"]
 * getValuesByPath(doc, "items.0.name") // ["A"]
 * getValuesByPath(doc, "user.email") // [undefined]
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
 * @description Retrieves the first non-undefined value at the specified path. If all values
 * are undefined, returns the first value (which will be undefined).
 * @param doc - The document to query
 * @param path - Dot-notation path (e.g., "user.address.city")
 * @returns The first non-undefined value at the path, or undefined if not found
 * @example
 * const doc = { user: { name: "John", address: { city: "NYC" } } };
 * getValueByPath(doc, "user.name") // "John"
 * getValueByPath(doc, "user.address.city") // "NYC"
 * getValueByPath(doc, "user.email") // undefined
 */
export function getValueByPath(doc: unknown, path: string): unknown {
  const values = getValuesByPath(doc, path);
  return values.find((v) => v !== undefined) ?? values[0];
}

/**
 * Set a value at a given path using dot notation.
 * @description Sets a value at the specified path, creating intermediate objects or arrays
 * as needed. If a numeric segment follows, creates an array; otherwise creates an object.
 * Mutates the document in place.
 * @param doc - The document to modify
 * @param path - Dot-notation path (e.g., "user.address.city")
 * @param value - The value to set at the path
 * @example
 * const doc = {};
 * setValueByPath(doc, "user.name", "John");
 * // doc is now { user: { name: "John" } }
 *
 * setValueByPath(doc, "items.0.price", 10);
 * // doc is now { user: { name: "John" }, items: [{ price: 10 }] }
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
 * @description Removes the property at the specified path from the document. If any intermediate
 * path segment doesn't exist or isn't an object, the operation does nothing. Mutates the document
 * in place.
 * @param doc - The document to modify
 * @param path - Dot-notation path (e.g., "user.address.city")
 * @example
 * const doc = { user: { name: "John", email: "john@example.com" } };
 * deleteValueByPath(doc, "user.email");
 * // doc is now { user: { name: "John" } }
 *
 * deleteValueByPath(doc, "user.nonexistent");
 * // doc unchanged, no error thrown
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
 * @description Retrieves a value using direct path traversal without expanding arrays.
 * Unlike getValueByPath, this function only follows numeric indices for arrays and returns
 * undefined for non-numeric access to arrays. Used for update operations.
 * @param doc - The document to query
 * @param path - Dot-notation path (e.g., "items.0.price")
 * @returns The value at the path, or undefined if not found or path is invalid
 * @example
 * const doc = { items: [{ price: 10 }, { price: 20 }] };
 * getValueAtPath(doc, "items.0.price") // 10
 * getValueAtPath(doc, "items.price") // undefined (no array expansion)
 * getValueAtPath(doc, "items.5") // undefined (index out of bounds)
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
 * @description Compares two values following MongoDB's ordering rules. Supports ObjectIds,
 * Dates, numbers, strings, and booleans. Returns a number indicating sort order.
 * @param a - The first value to compare
 * @param b - The second value to compare
 * @returns Negative if a < b, positive if a > b, 0 if equal, NaN if incomparable
 * @example
 * compareValues(5, 10) // -5 (a < b)
 * compareValues("apple", "banana") // negative (a < b)
 * compareValues(new Date("2023-01-01"), new Date("2023-01-02")) // negative
 * compareValues(5, "10") // NaN (different types)
 */
/**
 * Get the BSON type order for cross-type comparisons.
 * MongoDB compares different types using this ordering.
 */
function _getBSONTypeOrder(value: unknown): number {
  if (value === null || value === undefined) return 1; // Null/undefined
  if (typeof value === "number") return 2; // Numbers
  if (typeof value === "string") return 3; // String
  if (typeof value === "object" && !Array.isArray(value) && !(value instanceof Date) && !(value instanceof RegExp) && !(value instanceof ObjectId)) return 4; // Object
  if (Array.isArray(value)) return 5; // Array
  if (value instanceof ObjectId) return 7; // ObjectId
  if (typeof value === "boolean") return 8; // Boolean
  if (value instanceof Date) return 9; // Date
  if (value instanceof RegExp) return 11; // RegExp
  return 100; // Unknown types go last
}

export function compareValues(a: unknown, b: unknown): number {
  // Handle null/undefined specially
  const aIsNullish = a === null || a === undefined;
  const bIsNullish = b === null || b === undefined;

  if (aIsNullish && bIsNullish) return 0;
  if (aIsNullish) return -1; // null/undefined is less than everything
  if (bIsNullish) return 1;  // everything is greater than null/undefined

  if (a instanceof ObjectId && b instanceof ObjectId) {
    const aHex = a.toHexString();
    const bHex = b.toHexString();
    if (aHex < bHex) return -1;
    if (aHex > bHex) return 1;
    return 0;
  }

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  if (typeof a === typeof b) {
    if (typeof a === "number" && typeof b === "number") {
      return a - b;
    }
    if (typeof a === "string" && typeof b === "string") {
      // MongoDB uses binary comparison by default (not locale-aware)
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    }
    if (typeof a === "boolean" && typeof b === "boolean") {
      if (a === b) return 0;
      return a ? 1 : -1;
    }
  }

  // Different types are incomparable in MongoDB query operators
  // Return NaN to indicate no match for $gt, $lt, $gte, $lte
  return NaN;
}

/**
 * Check if two values are equal according to MongoDB rules.
 * @description Performs deep equality comparison following MongoDB's equality rules. Handles
 * ObjectIds, Dates, arrays, nested objects, and primitives. Optionally checks key order for objects.
 * @param a - The first value to compare
 * @param b - The second value to compare
 * @param strictKeyOrder - If true, objects must have keys in the same order (default: false)
 * @returns True if values are equal, false otherwise
 * @example
 * valuesEqual({ name: "John" }, { name: "John" }) // true
 * valuesEqual([1, 2, 3], [1, 2, 3]) // true
 * valuesEqual(new ObjectId("..."), new ObjectId("...")) // true if same ID
 * valuesEqual({ a: 1, b: 2 }, { b: 2, a: 1 }) // true (key order ignored by default)
 * valuesEqual({ a: 1, b: 2 }, { b: 2, a: 1 }, true) // false (key order matters)
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
 * @description Creates a deep copy of any value, including ObjectIds, Dates, arrays, and
 * nested objects. Preserves the type and structure of the original value.
 * @param value - The value to clone
 * @returns A deep clone of the value
 * @example
 * const obj = { name: "John", tags: ["a", "b"] };
 * const clone = cloneValue(obj);
 * clone.tags.push("c");
 * // obj.tags is still ["a", "b"], clone.tags is ["a", "b", "c"]
 *
 * const id = new ObjectId();
 * const clonedId = cloneValue(id);
 * // clonedId is a new ObjectId instance with the same value
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
 * @description Creates a deep copy of a document, recursively cloning all nested values
 * including ObjectIds, Dates, arrays, and nested objects.
 * @param doc - The document to clone
 * @returns A deep clone of the document
 * @example
 * const doc = {
 *   _id: new ObjectId(),
 *   user: { name: "John" },
 *   tags: ["a", "b"]
 * };
 * const clone = cloneDocument(doc);
 * clone.user.name = "Jane";
 * // doc.user.name is still "John"
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
 * @description Performs deep equality comparison of two documents, comparing all fields and
 * nested values according to MongoDB equality rules.
 * @param a - The first document to compare
 * @param b - The second document to compare
 * @returns True if documents are equal, false otherwise
 * @example
 * const doc1 = { _id: new ObjectId("..."), name: "John" };
 * const doc2 = { _id: new ObjectId("..."), name: "John" };
 * documentsEqual(doc1, doc2) // true if ObjectIds match
 *
 * const doc3 = { name: "John", age: 30 };
 * const doc4 = { age: 30, name: "John" };
 * documentsEqual(doc3, doc4) // true (key order doesn't matter)
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
