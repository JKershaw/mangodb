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
  compareValues,
} from "./document-utils.ts";
import { isOperatorObject, matchesOperators, matchesPullCondition } from "./query-matcher.ts";

/**
 * Push modifier object structure.
 */
interface PushModifiers {
  $each: unknown[];
  $position?: number;
  $slice?: number;
  $sort?: number | Record<string, 1 | -1>;
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
export function isPushEachModifier(value: unknown): value is PushModifiers {
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
 * Supports $position, $slice, and $sort modifiers.
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
  const isModifier = isPushEachModifier(value);
  const valuesToAdd = isModifier ? value.$each : [value];
  const position = isModifier ? value.$position : undefined;
  const sliceValue = isModifier ? value.$slice : undefined;
  const sortSpec = isModifier ? value.$sort : undefined;

  // Get or create the array
  let arr: unknown[];
  if (currentValue === undefined) {
    arr = [];
    setValueByPath(doc, path, arr);
  } else if (Array.isArray(currentValue)) {
    arr = currentValue;
  } else {
    const fieldType = currentValue === null ? "null" : typeof currentValue;
    throw new Error(
      `The field '${path}' must be an array but is of type ${fieldType}`
    );
  }

  // Add values (with uniqueness check if needed)
  const newValues: unknown[] = [];
  for (const v of valuesToAdd) {
    if (unique) {
      if (!arr.some((existing) => valuesEqual(existing, v, true)) &&
          !newValues.some((existing) => valuesEqual(existing, v, true))) {
        newValues.push(v);
      }
    } else {
      newValues.push(v);
    }
  }

  // Apply $position - insert at specific index
  if (position !== undefined) {
    if (!Number.isInteger(position)) {
      throw new Error("$position must be an integer");
    }
    // Negative positions count from end
    const insertAt = position < 0 ? Math.max(0, arr.length + position) : Math.min(position, arr.length);
    arr.splice(insertAt, 0, ...newValues);
  } else {
    // Default: append to end
    arr.push(...newValues);
  }

  // Apply $sort - sort the array after adding
  if (sortSpec !== undefined) {
    if (typeof sortSpec === "number") {
      // Sort array of primitives
      if (sortSpec !== 1 && sortSpec !== -1) {
        throw new Error("$sort must be 1 or -1 for primitive arrays");
      }
      arr.sort((a, b) => {
        const cmp = compareValues(a, b);
        return sortSpec === 1 ? cmp : -cmp;
      });
    } else if (typeof sortSpec === "object" && sortSpec !== null) {
      // Sort array of objects by fields
      const sortFields = Object.entries(sortSpec);
      arr.sort((a, b) => {
        for (const [field, dir] of sortFields) {
          const aVal = getValueAtPath(a as Record<string, unknown>, field);
          const bVal = getValueAtPath(b as Record<string, unknown>, field);
          const cmp = compareValues(aVal, bVal);
          if (cmp !== 0) {
            return dir === 1 ? cmp : -cmp;
          }
        }
        return 0;
      });
    }
  }

  // Apply $slice - limit array size
  if (sliceValue !== undefined) {
    if (!Number.isInteger(sliceValue)) {
      throw new Error("$slice must be an integer");
    }
    if (sliceValue === 0) {
      // Remove all elements
      arr.length = 0;
    } else if (sliceValue > 0) {
      // Keep first N elements
      if (arr.length > sliceValue) {
        arr.length = sliceValue;
      }
    } else {
      // Keep last N elements (negative slice)
      const keep = -sliceValue;
      if (arr.length > keep) {
        arr.splice(0, arr.length - keep);
      }
    }
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

  // Apply $pullAll - remove all matching values from array
  if (update.$pullAll) {
    for (const [path, valuesToRemove] of Object.entries(update.$pullAll)) {
      if (!Array.isArray(valuesToRemove)) {
        throw new Error(`$pullAll requires an array argument`);
      }

      const currentValue = getValueAtPath(result as Record<string, unknown>, path);

      if (currentValue === undefined) {
        continue;
      }

      if (!Array.isArray(currentValue)) {
        throw new Error(`Cannot apply $pullAll to a non-array value`);
      }

      const filteredArray = currentValue.filter(
        (elem) => !valuesToRemove.some((v) => valuesEqual(elem, v))
      );

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
      if (spec === true || spec === false) {
        // MongoDB treats both true and false as "set to current date"
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
            `The '$type' string field is required to be 'date' or 'timestamp': {$currentDate: {${path}: {$type: '${typeSpec.$type}'}}}`
          );
        }
      } else {
        // Invalid spec format (not boolean, not an object with $type)
        throw new Error(
          `$currentDate: expected boolean or { $type: 'date' | 'timestamp' }`
        );
      }
    }
  }

  // Apply $bit - bitwise operations (and, or, xor)
  if (update.$bit) {
    for (const [path, operations] of Object.entries(update.$bit)) {
      if (typeof operations !== "object" || operations === null) {
        throw new Error(`$bit requires an object with and/or/xor operations`);
      }

      let currentValue = getValueAtPath(result as Record<string, unknown>, path);

      // If field doesn't exist, initialize to 0
      if (currentValue === undefined) {
        currentValue = 0;
      }

      if (typeof currentValue !== "number" || !Number.isInteger(currentValue)) {
        throw new Error(`Cannot apply $bit to a non-integer value`);
      }

      let intValue = currentValue;

      const ops = operations as Record<string, unknown>;
      if ("and" in ops) {
        const andValue = ops.and;
        if (typeof andValue !== "number" || !Number.isInteger(andValue)) {
          throw new Error(`$bit and value must be an integer`);
        }
        intValue = intValue & andValue;
      }
      if ("or" in ops) {
        const orValue = ops.or;
        if (typeof orValue !== "number" || !Number.isInteger(orValue)) {
          throw new Error(`$bit or value must be an integer`);
        }
        intValue = intValue | orValue;
      }
      if ("xor" in ops) {
        const xorValue = ops.xor;
        if (typeof xorValue !== "number" || !Number.isInteger(xorValue)) {
          throw new Error(`$bit xor value must be an integer`);
        }
        intValue = intValue ^ xorValue;
      }

      setValueByPath(result as Record<string, unknown>, path, intValue);
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
