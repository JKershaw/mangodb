/**
 * Update operators for MongoDB-compatible document modifications.
 */
import type { Document, UpdateOperators, Filter, QueryOperators } from "./types.ts";
import {
  setValueByPath,
  deleteValueByPath,
  getValueAtPath,
  cloneDocument,
  valuesEqual,
} from "./document-utils.ts";
import { isOperatorObject, matchesOperators, matchesPullCondition } from "./query-matcher.ts";

/**
 * Compare two values using MongoDB's BSON comparison order.
 * Returns: negative if a < b, 0 if equal, positive if a > b
 *
 * @param a - First value to compare
 * @param b - Second value to compare
 * @returns Negative number if a < b, 0 if equal, positive if a > b
 */
function compareValues(a: unknown, b: unknown): number {
  // Handle same type comparisons first
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }
  if (typeof a === "string" && typeof b === "string") {
    return a.localeCompare(b);
  }
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  // For different types, use simplified BSON type ordering
  const typeOrderA = getBsonTypeOrder(a);
  const typeOrderB = getBsonTypeOrder(b);

  if (typeOrderA !== typeOrderB) {
    return typeOrderA - typeOrderB;
  }

  // Same type but not handled above - try generic comparison
  if (a === b) return 0;
  if (a === null) return -1;
  if (b === null) return 1;
  return String(a).localeCompare(String(b));
}

/**
 * Get the BSON type order for a value (simplified).
 * MongoDB uses a specific ordering for type comparison.
 * Order: MinKey < Null < Numbers < String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < RegExp < MaxKey
 */
function getBsonTypeOrder(value: unknown): number {
  if (value === undefined) return 0;
  if (value === null) return 1;
  if (typeof value === "number") return 2;
  if (typeof value === "string") return 3;
  if (typeof value === "object") {
    if (Array.isArray(value)) return 5;
    if (value instanceof Date) return 8; // Date comes AFTER Boolean in MongoDB
    return 4; // Regular object (including ObjectId)
  }
  if (typeof value === "boolean") return 7; // Boolean comes BEFORE Date
  return 10;
}

/**
 * Check if a value is a $push/$addToSet $each modifier.
 *
 * The $each modifier is used with $push and $addToSet to append multiple values
 * to an array in a single operation.
 *
 * @param value - The value to check
 * @returns True if the value is an object containing a $each property with an array value
 * @example
 * isPushEachModifier({ $each: [1, 2, 3] }) // true
 * isPushEachModifier([1, 2, 3]) // false
 * isPushEachModifier({ $each: "not an array" }) // false
 */
export function isPushEachModifier(value: unknown): value is { $each: unknown[] } {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "$each" in value &&
    Array.isArray((value as { $each: unknown }).$each)
  );
}

/**
 * Helper for $push and $addToSet operators.
 *
 * Adds values to an array field. If the field doesn't exist, it creates a new array.
 * Supports the $each modifier to add multiple values at once.
 *
 * @param doc - The document to modify
 * @param path - The dot-notation path to the array field
 * @param value - The value to push (can be a single value or a $each modifier object)
 * @param unique - If true, only adds values that don't already exist in the array ($addToSet behavior)
 * @throws Error if the target field exists but is not an array
 */
function applyArrayPush(
  doc: Record<string, unknown>,
  path: string,
  value: unknown,
  unique: boolean
): void {
  const currentValue = getValueAtPath(doc, path);
  const valuesToAdd = isPushEachModifier(value)
    ? (value as { $each: unknown[] }).$each
    : [value];

  if (currentValue === undefined) {
    if (unique) {
      const uniqueValues: unknown[] = [];
      for (const v of valuesToAdd) {
        if (!uniqueValues.some((existing) => valuesEqual(existing, v, true))) {
          uniqueValues.push(v);
        }
      }
      setValueByPath(doc, path, uniqueValues);
    } else {
      setValueByPath(doc, path, [...valuesToAdd]);
    }
  } else if (Array.isArray(currentValue)) {
    for (const v of valuesToAdd) {
      if (unique) {
        if (!currentValue.some((existing) => valuesEqual(existing, v, true))) {
          currentValue.push(v);
        }
      } else {
        currentValue.push(v);
      }
    }
  } else {
    const fieldType = currentValue === null ? "null" : typeof currentValue;
    throw new Error(
      `The field '${path}' must be an array but is of type ${fieldType}`
    );
  }
}

/**
 * Apply MongoDB update operators to a document.
 *
 * Creates a new document with the specified update operators applied. Supports
 * $set, $unset, $inc, $push, $addToSet, $pop, $pull, $min, $max, $mul, $rename,
 * and $currentDate operators. The original document is not modified.
 *
 * @param doc - The original document to update
 * @param update - Update operators object containing one or more of: $set, $unset, $inc, $push, $addToSet, $pop, $pull, $min, $max, $mul, $rename, $currentDate
 * @returns A new document with all updates applied
 * @throws Error if $pop receives a value other than 1 or -1
 * @throws Error if $push, $addToSet, $pop, or $pull target a field that is not an array
 * @throws Error if $mul targets a non-numeric field
 * @throws Error if $rename source and destination are the same
 * @throws Error if $currentDate has an invalid type specification
 * @example
 * // Increment a counter
 * applyUpdateOperators({ count: 1 }, { $inc: { count: 1 } })
 * // Returns: { count: 2 }
 *
 * @example
 * // Set and push values
 * applyUpdateOperators(
 *   { name: "Alice", tags: ["user"] },
 *   { $set: { name: "Bob" }, $push: { tags: "admin" } }
 * )
 * // Returns: { name: "Bob", tags: ["user", "admin"] }
 *
 * @example
 * // Use $each modifier to push multiple values
 * applyUpdateOperators(
 *   { items: [1] },
 *   { $push: { items: { $each: [2, 3, 4] } } }
 * )
 * // Returns: { items: [1, 2, 3, 4] }
 */
export function applyUpdateOperators<T extends Document>(
  doc: T,
  update: UpdateOperators
): T {
  const result = cloneDocument(doc);

  // Apply $set
  if (update.$set) {
    for (const [path, value] of Object.entries(update.$set)) {
      setValueByPath(result as Record<string, unknown>, path, value);
    }
  }

  // Apply $unset
  if (update.$unset) {
    for (const path of Object.keys(update.$unset)) {
      deleteValueByPath(result as Record<string, unknown>, path);
    }
  }

  // Apply $inc
  if (update.$inc) {
    for (const [path, increment] of Object.entries(update.$inc)) {
      const currentValue = getValueAtPath(result as Record<string, unknown>, path);
      const numericCurrent = typeof currentValue === "number" ? currentValue : 0;
      setValueByPath(result as Record<string, unknown>, path, numericCurrent + increment);
    }
  }

  // Apply $push
  if (update.$push) {
    for (const [path, value] of Object.entries(update.$push)) {
      applyArrayPush(result as Record<string, unknown>, path, value, false);
    }
  }

  // Apply $addToSet
  if (update.$addToSet) {
    for (const [path, value] of Object.entries(update.$addToSet)) {
      applyArrayPush(result as Record<string, unknown>, path, value, true);
    }
  }

  // Apply $pop
  if (update.$pop) {
    for (const [path, direction] of Object.entries(update.$pop)) {
      if (direction !== 1 && direction !== -1) {
        throw new Error("$pop expects 1 or -1");
      }

      const currentValue = getValueAtPath(result as Record<string, unknown>, path);

      if (currentValue === undefined) {
        continue;
      }

      if (!Array.isArray(currentValue)) {
        throw new Error(`Cannot apply $pop to a non-array value`);
      }

      if (currentValue.length > 0) {
        if (direction === 1) {
          currentValue.pop();
        } else {
          currentValue.shift();
        }
      }
    }
  }

  // Apply $pull
  if (update.$pull) {
    for (const [path, condition] of Object.entries(update.$pull)) {
      const currentValue = getValueAtPath(result as Record<string, unknown>, path);

      if (currentValue === undefined) {
        continue;
      }

      if (!Array.isArray(currentValue)) {
        throw new Error(`Cannot apply $pull to a non-array value`);
      }

      const filteredArray = currentValue.filter((elem) => {
        if (isOperatorObject(condition)) {
          return !matchesOperators(elem, condition as QueryOperators);
        } else if (
          condition !== null &&
          typeof condition === "object" &&
          !Array.isArray(condition)
        ) {
          return !matchesPullCondition(elem, condition as Record<string, unknown>);
        } else {
          return !valuesEqual(elem, condition);
        }
      });

      currentValue.length = 0;
      currentValue.push(...filteredArray);
    }
  }

  // Apply $min - only update if new value is less than current
  if (update.$min) {
    for (const [path, minValue] of Object.entries(update.$min)) {
      const currentValue = getValueAtPath(result as Record<string, unknown>, path);
      if (currentValue === undefined) {
        // Field doesn't exist, create it with the specified value
        setValueByPath(result as Record<string, unknown>, path, minValue);
      } else if (compareValues(minValue, currentValue) < 0) {
        // New value is less than current, update it
        setValueByPath(result as Record<string, unknown>, path, minValue);
      }
    }
  }

  // Apply $max - only update if new value is greater than current
  if (update.$max) {
    for (const [path, maxValue] of Object.entries(update.$max)) {
      const currentValue = getValueAtPath(result as Record<string, unknown>, path);
      if (currentValue === undefined) {
        // Field doesn't exist, create it with the specified value
        setValueByPath(result as Record<string, unknown>, path, maxValue);
      } else if (compareValues(maxValue, currentValue) > 0) {
        // New value is greater than current, update it
        setValueByPath(result as Record<string, unknown>, path, maxValue);
      }
    }
  }

  // Apply $mul - multiply field by a number
  if (update.$mul) {
    for (const [path, multiplier] of Object.entries(update.$mul)) {
      const currentValue = getValueAtPath(result as Record<string, unknown>, path);
      if (currentValue === undefined) {
        // Field doesn't exist, create it with value 0 (not the multiplied value!)
        setValueByPath(result as Record<string, unknown>, path, 0);
      } else if (typeof currentValue !== "number") {
        // Non-numeric field value - throw error
        throw new Error(
          `Cannot apply $mul to a value of non-numeric type. Field '${path}' has non-numeric type ${typeof currentValue}`
        );
      } else {
        setValueByPath(result as Record<string, unknown>, path, currentValue * multiplier);
      }
    }
  }

  // Apply $rename - rename fields
  if (update.$rename) {
    for (const [oldPath, newPath] of Object.entries(update.$rename)) {
      // MongoDB errors if source and destination are the same
      if (oldPath === newPath) {
        throw new Error(
          `The source and destination field for $rename must differ: ${oldPath}`
        );
      }
      const value = getValueAtPath(result as Record<string, unknown>, oldPath);
      if (value !== undefined) {
        // Only rename if the old field exists
        deleteValueByPath(result as Record<string, unknown>, oldPath);
        setValueByPath(result as Record<string, unknown>, newPath, value);
      }
    }
  }

  // Apply $currentDate - set field to current date
  if (update.$currentDate) {
    for (const [path, spec] of Object.entries(update.$currentDate)) {
      const now = new Date();
      if (spec === true) {
        // Simple boolean true means Date type
        setValueByPath(result as Record<string, unknown>, path, now);
      } else if (typeof spec === "object" && spec !== null && "$type" in spec) {
        const typeSpec = spec as { $type: string };
        if (typeSpec.$type === "date") {
          setValueByPath(result as Record<string, unknown>, path, now);
        } else if (typeSpec.$type === "timestamp") {
          // For timestamp, use numeric milliseconds
          // (MongoDB uses a Timestamp object, but we use numeric for simplicity)
          setValueByPath(result as Record<string, unknown>, path, now.getTime());
        } else {
          // Invalid $type value - MongoDB errors on this
          throw new Error(
            `$currentDate: unrecognized type specification '${typeSpec.$type}', expected 'date' or 'timestamp'`
          );
        }
      } else {
        // Invalid spec format (not true, not an object with $type)
        throw new Error(
          `$currentDate: expected boolean true or { $type: 'date' | 'timestamp' }`
        );
      }
    }
  }

  return result;
}

/**
 * Create a base document from a filter for upsert operations.
 *
 * Extracts simple equality conditions and $eq operators from the filter to create
 * a starting document. This is used in upsert operations to initialize a new document
 * with the values from the query filter. Only extracts safe, deterministic values
 * (direct equality and $eq operators). Complex operators like $gt, $in, etc. are ignored.
 *
 * @param filter - The query filter to extract values from
 * @returns A new document containing the extracted equality values
 * @example
 * // Simple equality
 * createDocumentFromFilter({ name: "Alice", age: 30 })
 * // Returns: { name: "Alice", age: 30 }
 *
 * @example
 * // With $eq operator
 * createDocumentFromFilter({ name: { $eq: "Bob" }, status: "active" })
 * // Returns: { name: "Bob", status: "active" }
 *
 * @example
 * // Dot notation paths
 * createDocumentFromFilter({ "address.city": "NYC" })
 * // Returns: { address: { city: "NYC" } }
 *
 * @example
 * // Complex operators are ignored
 * createDocumentFromFilter({ age: { $gt: 18 }, name: "Alice" })
 * // Returns: { name: "Alice" }
 */
export function createDocumentFromFilter<T extends Document>(
  filter: Filter<T>
): T {
  const doc: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(filter)) {
    if (!isOperatorObject(value)) {
      if (key.includes(".")) {
        setValueByPath(doc, key, value);
      } else {
        doc[key] = value;
      }
    } else if (
      value &&
      typeof value === "object" &&
      "$eq" in value &&
      Object.keys(value).length === 1
    ) {
      const eqValue = (value as { $eq: unknown }).$eq;
      if (key.includes(".")) {
        setValueByPath(doc, key, eqValue);
      } else {
        doc[key] = eqValue;
      }
    }
  }

  return doc as T;
}

/**
 * Validate that a replacement document doesn't contain update operators.
 *
 * Ensures that a document intended for replacement operations (like replaceOne)
 * doesn't contain any update operator fields (keys starting with '$'). In MongoDB,
 * replacement documents must be plain documents without operators, whereas update
 * documents use operators like $set, $inc, etc.
 *
 * @param replacement - The replacement document to validate
 * @throws Error if the document contains any keys starting with '$'
 * @example
 * // Valid replacement document
 * validateReplacement({ name: "Alice", age: 30 })
 * // No error thrown
 *
 * @example
 * // Invalid - contains update operator
 * validateReplacement({ $set: { name: "Alice" } })
 * // Throws: "Replacement document must not contain update operators (keys starting with '$')"
 *
 * @example
 * // Invalid - mixed content
 * validateReplacement({ name: "Alice", $inc: { count: 1 } })
 * // Throws: "Replacement document must not contain update operators (keys starting with '$')"
 */
export function validateReplacement<T extends Document>(replacement: T): void {
  for (const key of Object.keys(replacement)) {
    if (key.startsWith("$")) {
      throw new Error(
        "Replacement document must not contain update operators (keys starting with '$')"
      );
    }
  }
}
