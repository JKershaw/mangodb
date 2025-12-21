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
 * $set, $unset, $inc, $push, $addToSet, $pop, and $pull operators. The original
 * document is not modified.
 *
 * @param doc - The original document to update
 * @param update - Update operators object containing one or more of: $set, $unset, $inc, $push, $addToSet, $pop, $pull
 * @returns A new document with all updates applied
 * @throws Error if $pop receives a value other than 1 or -1
 * @throws Error if $push, $addToSet, $pop, or $pull target a field that is not an array
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
