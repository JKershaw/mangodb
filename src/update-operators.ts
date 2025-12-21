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
 * Apply update operators to a document.
 * Returns a new document with the updates applied.
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
 * Create a document from filter for upsert.
 * Extracts simple equality conditions and $eq values from the filter.
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
