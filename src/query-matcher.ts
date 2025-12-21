/**
 * Query matching logic for MongoDB-compatible filters.
 */
import { ObjectId } from "mongodb";
import type { Document, Filter, QueryOperators } from "./types.ts";
import {
  getValuesByPath,
  getValueByPath,
  compareValues,
  valuesEqual,
} from "./document-utils.ts";

/**
 * Check if an object contains query operators (keys starting with $).
 */
export function isOperatorObject(value: unknown): value is QueryOperators {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const keys = Object.keys(value as object);
  return keys.length > 0 && keys.every((k) => k.startsWith("$"));
}

/**
 * Check if a document value matches any value in the $in array.
 */
function matchesIn(docValue: unknown, inValues: unknown[]): boolean {
  if (Array.isArray(docValue)) {
    return docValue.some((dv) => inValues.some((iv) => valuesEqual(dv, iv)));
  }
  return inValues.some((iv) => valuesEqual(docValue, iv));
}

/**
 * Check if a document value matches a single filter value.
 * Handles array field matching (any element match).
 */
function matchesSingleValue(docValue: unknown, filterValue: unknown): boolean {
  if (filterValue === null) {
    return docValue === null || docValue === undefined;
  }

  if (valuesEqual(docValue, filterValue)) {
    return true;
  }

  if (Array.isArray(docValue) && !Array.isArray(filterValue)) {
    return docValue.some((elem) => valuesEqual(elem, filterValue));
  }

  return false;
}

/**
 * Check if an array element matches an $elemMatch condition.
 * The condition can contain field queries (for objects) or operators (for primitives).
 */
function matchesElemMatchCondition(
  element: unknown,
  condition: Record<string, unknown>
): boolean {
  for (const [key, value] of Object.entries(condition)) {
    if (key.startsWith("$")) {
      const operators: QueryOperators = { [key]: value };
      if (!matchesOperators(element, operators)) {
        return false;
      }
    } else {
      if (element === null || typeof element !== "object" || Array.isArray(element)) {
        return false;
      }
      const elemObj = element as Record<string, unknown>;
      const fieldValue = getValueByPath(elemObj, key);

      if (isOperatorObject(value)) {
        if (!matchesOperators(fieldValue, value as QueryOperators)) {
          return false;
        }
      } else {
        if (!matchesSingleValue(fieldValue, value)) {
          return false;
        }
      }
    }
  }
  return true;
}

/**
 * Check if a value matches query operators.
 */
export function matchesOperators(
  docValue: unknown,
  operators: QueryOperators
): boolean {
  for (const [op, opValue] of Object.entries(operators)) {
    switch (op) {
      case "$eq":
        if (!matchesSingleValue(docValue, opValue)) return false;
        break;

      case "$ne":
        if (matchesSingleValue(docValue, opValue)) return false;
        break;

      case "$gt": {
        const cmp = compareValues(docValue, opValue);
        if (isNaN(cmp) || cmp <= 0) return false;
        break;
      }

      case "$gte": {
        const cmp = compareValues(docValue, opValue);
        if (isNaN(cmp) || cmp < 0) return false;
        break;
      }

      case "$lt": {
        const cmp = compareValues(docValue, opValue);
        if (isNaN(cmp) || cmp >= 0) return false;
        break;
      }

      case "$lte": {
        const cmp = compareValues(docValue, opValue);
        if (isNaN(cmp) || cmp > 0) return false;
        break;
      }

      case "$in": {
        const arr = opValue as unknown[];
        if (!matchesIn(docValue, arr)) return false;
        break;
      }

      case "$nin": {
        const arr = opValue as unknown[];
        if (matchesIn(docValue, arr)) return false;
        break;
      }

      case "$exists": {
        const shouldExist = opValue as boolean;
        const exists = docValue !== undefined;
        if (shouldExist !== exists) return false;
        break;
      }

      case "$not": {
        if (
          opValue === null ||
          typeof opValue !== "object" ||
          Array.isArray(opValue)
        ) {
          throw new Error("$not argument must be a regex or an object");
        }
        const notOps = opValue as QueryOperators;
        const notKeys = Object.keys(notOps);
        if (notKeys.length === 0 || !notKeys.every((k) => k.startsWith("$"))) {
          throw new Error("$not argument must be a regex or an object");
        }
        if (docValue === undefined) break;
        if (matchesOperators(docValue, notOps)) {
          return false;
        }
        break;
      }

      case "$size": {
        const size = opValue as number;
        if (!Number.isInteger(size)) {
          throw new Error("$size must be a whole number");
        }
        if (size < 0) {
          throw new Error("$size may not be negative");
        }
        if (!Array.isArray(docValue)) return false;
        if (docValue.length !== size) return false;
        break;
      }

      case "$all": {
        if (!Array.isArray(opValue)) {
          throw new Error("$all needs an array");
        }
        const allValues = opValue as unknown[];
        if (allValues.length === 0) return false;
        const valueArray = Array.isArray(docValue) ? docValue : [docValue];
        for (const val of allValues) {
          if (
            val &&
            typeof val === "object" &&
            !Array.isArray(val) &&
            "$elemMatch" in val
          ) {
            const elemMatchCond = (val as { $elemMatch: Record<string, unknown> })
              .$elemMatch;
            const hasMatch = valueArray.some((elem) =>
              matchesElemMatchCondition(elem, elemMatchCond)
            );
            if (!hasMatch) return false;
          } else {
            const found = valueArray.some((elem) => valuesEqual(elem, val));
            if (!found) return false;
          }
        }
        break;
      }

      case "$elemMatch": {
        if (opValue === null || typeof opValue !== "object" || Array.isArray(opValue)) {
          throw new Error("$elemMatch needs an Object");
        }
        if (!Array.isArray(docValue)) return false;
        if (docValue.length === 0) return false;
        const elemMatchCond = opValue as Record<string, unknown>;
        if (Object.keys(elemMatchCond).length === 0) return false;
        const hasMatchingElement = docValue.some((elem) =>
          matchesElemMatchCondition(elem, elemMatchCond)
        );
        if (!hasMatchingElement) return false;
        break;
      }

      case "$and":
        throw new Error("unknown operator: $and");

      case "$or":
        throw new Error("unknown operator: $or");

      case "$nor":
        throw new Error("unknown operator: $nor");

      default:
        break;
    }
  }
  return true;
}

/**
 * Check if ANY of the values matches a filter condition.
 * Special case: $exists: false requires ALL values to be undefined.
 */
function anyValueMatches(
  values: unknown[],
  filterValue: unknown,
  isOperator: boolean
): boolean {
  if (isOperator) {
    const ops = filterValue as QueryOperators;

    if (ops.$exists === false && Object.keys(ops).length === 1) {
      return values.every((v) => v === undefined);
    }

    return values.some((v) => matchesOperators(v, filterValue as QueryOperators));
  } else {
    return values.some((v) => matchesSingleValue(v, filterValue));
  }
}

/**
 * Evaluate a logical operator ($and, $or, $nor) against a document.
 */
function evaluateLogicalOperator<T extends Document>(
  doc: T,
  operator: string,
  conditions: unknown
): boolean {
  if (!Array.isArray(conditions)) {
    throw new Error(`${operator} argument must be an array`);
  }
  if (conditions.length === 0) {
    throw new Error(`${operator} argument must be a non-empty array`);
  }
  const filters = conditions as Filter<T>[];

  switch (operator) {
    case "$and":
      return filters.every((cond) => matchesFilter(doc, cond));
    case "$or":
      return filters.some((cond) => matchesFilter(doc, cond));
    case "$nor":
      return !filters.some((cond) => matchesFilter(doc, cond));
    default:
      return true;
  }
}

/**
 * Check if a document matches a filter.
 * Supports:
 * - Empty filter (matches all)
 * - Simple equality
 * - Dot notation for nested fields (including array element traversal)
 * - Query operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $not)
 * - Logical operators ($and, $or, $nor)
 * - Array field matching
 */
export function matchesFilter<T extends Document>(
  doc: T,
  filter: Filter<T>
): boolean {
  for (const [key, filterValue] of Object.entries(filter)) {
    if (key === "$and" || key === "$or" || key === "$nor") {
      if (!evaluateLogicalOperator(doc, key, filterValue)) {
        return false;
      }
      continue;
    }

    const docValues = getValuesByPath(doc, key);
    const isOperator = isOperatorObject(filterValue);

    if (!anyValueMatches(docValues, filterValue, isOperator)) {
      return false;
    }
  }
  return true;
}

/**
 * Check if an array element matches a $pull object condition.
 * Reuses matchesElemMatchCondition since the logic is equivalent.
 */
export function matchesPullCondition(
  element: unknown,
  condition: Record<string, unknown>
): boolean {
  return matchesElemMatchCondition(element, condition);
}
