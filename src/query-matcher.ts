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
import { evaluateExpression } from "./aggregation.ts";

// BSON type name to numeric code mapping
const BSON_TYPE_ALIASES: Record<string, number> = {
  double: 1,
  string: 2,
  object: 3,
  array: 4,
  binData: 5,
  undefined: 6,
  objectId: 7,
  bool: 8,
  date: 9,
  null: 10,
  regex: 11,
  javascript: 13,
  int: 16,
  timestamp: 17,
  long: 18,
  decimal: 19,
  minKey: -1,
  maxKey: 127,
};

// Special "number" alias matches these types
const NUMBER_TYPE_CODES = new Set([1, 16, 18, 19]); // double, int, long, decimal

// Valid numeric type codes
const VALID_TYPE_CODES = new Set(Object.values(BSON_TYPE_ALIASES));

/**
 * Get the BSON type code for a JavaScript value.
 */
function getBSONTypeCode(value: unknown): number {
  if (value === undefined) return 6; // undefined
  if (value === null) return 10; // null
  if (typeof value === "string") return 2; // string
  if (typeof value === "boolean") return 8; // bool
  if (typeof value === "number") {
    // In JavaScript, we can distinguish integers from floats
    return Number.isInteger(value) ? 16 : 1; // int or double
  }
  if (Array.isArray(value)) return 4; // array
  if (value instanceof Date) return 9; // date
  if (value instanceof RegExp) return 11; // regex
  if (value instanceof ObjectId || (value && typeof (value as { toHexString?: unknown }).toHexString === "function")) {
    return 7; // objectId
  }
  if (typeof value === "object") return 3; // object
  return -999; // unknown type (should not happen)
}

/**
 * Check if a value matches a BSON type specification.
 */
function matchesBSONType(value: unknown, typeSpec: string | number): boolean {
  // Handle "number" alias specially
  if (typeSpec === "number") {
    const valueType = getBSONTypeCode(value);
    return NUMBER_TYPE_CODES.has(valueType);
  }

  // Convert string alias to numeric code
  let targetCode: number;
  if (typeof typeSpec === "string") {
    if (!(typeSpec in BSON_TYPE_ALIASES)) {
      throw new Error(`Unknown type name alias: ${typeSpec}`);
    }
    targetCode = BSON_TYPE_ALIASES[typeSpec];
  } else {
    if (!VALID_TYPE_CODES.has(typeSpec)) {
      throw new Error(`Invalid numerical type code: ${typeSpec}`);
    }
    targetCode = typeSpec;
  }

  const valueType = getBSONTypeCode(value);

  // Special case: "double" (1) should also match integers in JavaScript
  // since MongoDB's double encompasses all numeric values
  if (targetCode === 1 && typeof value === "number") {
    return true;
  }

  return valueType === targetCode;
}

/**
 * Check if an object contains query operators (keys starting with $).
 *
 * @description Determines whether a value is an object where all keys are MongoDB query operators.
 * Query operators are identified by keys that start with a dollar sign ($).
 * Returns false for null, arrays, or non-object values.
 *
 * @param value - The value to check
 * @returns True if the value is an object with all keys starting with $, false otherwise
 *
 * @example
 * isOperatorObject({ $gt: 5, $lt: 10 }) // true
 * isOperatorObject({ age: 25 }) // false
 * isOperatorObject(null) // false
 * isOperatorObject([1, 2, 3]) // false
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
 * Supports RegExp objects in the $in array for pattern matching.
 */
function matchesIn(docValue: unknown, inValues: unknown[]): boolean {
  const testValue = (dv: unknown, iv: unknown): boolean => {
    // Handle RegExp in $in array
    if (iv instanceof RegExp) {
      return typeof dv === "string" && iv.test(dv);
    }
    return valuesEqual(dv, iv);
  };

  if (Array.isArray(docValue)) {
    return docValue.some((dv) => inValues.some((iv) => testValue(dv, iv)));
  }
  return inValues.some((iv) => testValue(docValue, iv));
}

/**
 * Check if a document value matches a single filter value.
 * Handles array field matching (any element match).
 * Handles RegExp filter values for pattern matching.
 */
function matchesSingleValue(docValue: unknown, filterValue: unknown): boolean {
  if (filterValue === null) {
    return docValue === null || docValue === undefined;
  }

  // Handle RegExp filter value
  if (filterValue instanceof RegExp) {
    if (typeof docValue === "string") {
      return filterValue.test(docValue);
    }
    // For arrays, match if any string element matches
    if (Array.isArray(docValue)) {
      return docValue.some(
        (elem) => typeof elem === "string" && filterValue.test(elem)
      );
    }
    // Non-string, non-array values don't match regex
    return false;
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
 *
 * @description Evaluates whether a document value satisfies all specified MongoDB query operators.
 * Supports comparison operators ($eq, $ne, $gt, $gte, $lt, $lte), array operators
 * ($in, $nin, $all, $elemMatch), existence checks ($exists), size checks ($size),
 * and negation ($not). Logical operators ($and, $or, $nor) are not supported at this level
 * and will throw an error.
 *
 * @param docValue - The value from the document to test
 * @param operators - An object containing MongoDB query operators
 * @returns True if the value matches all operators, false otherwise
 *
 * @throws {Error} If an invalid operator argument is provided (e.g., $size with non-integer)
 * @throws {Error} If logical operators ($and, $or, $nor) are used at this level
 *
 * @example
 * matchesOperators(25, { $gte: 18, $lt: 65 }) // true
 * matchesOperators(15, { $gte: 18 }) // false
 * matchesOperators([1, 2, 3], { $size: 3 }) // true
 * matchesOperators('hello', { $in: ['hello', 'world'] }) // true
 * matchesOperators(10, { $not: { $lt: 5 } }) // true
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
        // Handle RegExp directly in $not
        if (opValue instanceof RegExp) {
          if (docValue === undefined) break;
          if (typeof docValue === "string" && opValue.test(docValue)) {
            return false;
          }
          // For arrays, check if any string element matches
          if (Array.isArray(docValue)) {
            const anyMatch = docValue.some(
              (elem) => typeof elem === "string" && opValue.test(elem)
            );
            if (anyMatch) return false;
          }
          break;
        }

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

      case "$regex": {
        const pattern = opValue;
        const options = (operators as QueryOperators).$options || "";

        // Handle RegExp object directly
        if (pattern instanceof RegExp) {
          // For string values
          if (typeof docValue === "string") {
            if (!pattern.test(docValue)) return false;
          } else if (Array.isArray(docValue)) {
            // For arrays, match if any string element matches
            const anyMatch = docValue.some(
              (elem) => typeof elem === "string" && pattern.test(elem)
            );
            if (!anyMatch) return false;
          } else {
            // Non-string, non-array values don't match
            return false;
          }
          break;
        }

        // Handle string pattern
        if (typeof pattern !== "string") {
          throw new Error("$regex has to be a string");
        }

        // Validate options - MongoDB only supports i, m, s, x (not g, u, y)
        // Note: x (extended) is not supported in JavaScript but is in MongoDB
        const validMongoOptions = new Set(["i", "m", "s"]);
        for (const char of options) {
          if (!validMongoOptions.has(char)) {
            throw new Error(`invalid flag in regex options: ${char}`);
          }
        }

        let regex: RegExp;
        try {
          regex = new RegExp(pattern, options);
        } catch (e) {
          // For invalid pattern or other errors
          throw new Error(`Regular expression is invalid: ${(e as Error).message}`);
        }

        // For string values
        if (typeof docValue === "string") {
          if (!regex.test(docValue)) return false;
        } else if (Array.isArray(docValue)) {
          // For arrays, match if any string element matches
          const anyMatch = docValue.some(
            (elem) => typeof elem === "string" && regex.test(elem)
          );
          if (!anyMatch) return false;
        } else {
          // Non-string, non-array values don't match
          return false;
        }
        break;
      }

      case "$options": {
        // $options is handled in $regex case
        // If we get here without $regex, it's an error
        if (!("$regex" in operators)) {
          throw new Error("$options needs a $regex");
        }
        // Otherwise, it was already handled in $regex case
        break;
      }

      case "$type": {
        // Missing fields (undefined) don't match any type
        if (docValue === undefined) return false;

        const typeSpec = opValue as string | number | (string | number)[];

        // Handle array of types (match if any type matches)
        if (Array.isArray(typeSpec)) {
          const matches = typeSpec.some((t) => matchesBSONType(docValue, t));
          if (!matches) return false;
        } else {
          if (!matchesBSONType(docValue, typeSpec)) return false;
        }
        break;
      }

      case "$mod": {
        // Validate $mod argument
        if (!Array.isArray(opValue)) {
          throw new Error("malformed mod, needs to be an array");
        }
        if (opValue.length < 2) {
          throw new Error("malformed mod, not enough elements");
        }
        if (opValue.length > 2) {
          throw new Error("malformed mod, too many elements");
        }

        const [divisor, remainder] = opValue as [unknown, unknown];

        // Validate divisor
        if (typeof divisor !== "number") {
          throw new Error("malformed mod, divisor not a number");
        }
        if (!Number.isFinite(divisor)) {
          throw new Error(
            `malformed mod, divisor value is invalid :: caused by :: ${
              Number.isNaN(divisor) ? "NaN" : "Infinity"
            } is an invalid argument`
          );
        }
        if (Math.trunc(divisor) === 0) {
          throw new Error("divisor cannot be 0");
        }

        // Validate remainder
        if (typeof remainder !== "number") {
          throw new Error("malformed mod, remainder not a number");
        }
        if (!Number.isFinite(remainder)) {
          throw new Error(
            `malformed mod, remainder value is invalid :: caused by :: ${
              Number.isNaN(remainder) ? "NaN" : "Infinity"
            } is an invalid argument`
          );
        }

        // Non-numeric document values don't match
        if (typeof docValue !== "number" || !Number.isFinite(docValue)) {
          return false;
        }

        // Perform modulo with truncation toward zero
        const truncDivisor = Math.trunc(divisor);
        const truncRemainder = Math.trunc(remainder);
        const truncDocValue = Math.trunc(docValue);

        if (truncDocValue % truncDivisor !== truncRemainder) {
          return false;
        }
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
 * Check if a document matches a MongoDB-style filter.
 *
 * @description The main query matching function that evaluates whether a document satisfies
 * a MongoDB-style filter. This is the primary entry point for document matching and supports
 * all standard MongoDB query features including equality, operators, logical operators, and
 * dot notation for nested field access.
 *
 * Supports:
 * - Empty filter (matches all documents)
 * - Simple equality ({ field: value })
 * - Dot notation for nested fields ({ "address.city": "NYC" })
 * - Array element traversal ({ "items.0.name": "apple" })
 * - Query operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $not, $size, $all, $elemMatch)
 * - Logical operators ($and, $or, $nor)
 * - Array field matching (any element match)
 *
 * @param doc - The document to test against the filter
 * @param filter - MongoDB-style filter object with field conditions and/or logical operators
 * @returns True if the document matches the filter, false otherwise
 *
 * @example
 * // Simple equality
 * matchesFilter({ age: 25 }, { age: 25 }) // true
 *
 * @example
 * // Query operators
 * matchesFilter({ age: 25 }, { age: { $gte: 18, $lt: 65 } }) // true
 *
 * @example
 * // Dot notation
 * matchesFilter({ user: { name: 'John' } }, { 'user.name': 'John' }) // true
 *
 * @example
 * // Logical operators
 * matchesFilter(
 *   { age: 25, status: 'active' },
 *   { $and: [{ age: { $gte: 18 } }, { status: 'active' }] }
 * ) // true
 *
 * @example
 * // Array matching
 * matchesFilter({ tags: ['js', 'ts'] }, { tags: 'js' }) // true
 * matchesFilter({ scores: [85, 90, 95] }, { scores: { $gt: 80 } }) // true
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

    // Handle $expr - evaluates aggregation expression against the document
    if (key === "$expr") {
      const result = evaluateExpression(filterValue, doc);
      // Truthy result means match, falsy means no match
      if (!result) {
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
 *
 * @description Determines whether an array element satisfies a condition used in MongoDB's
 * $pull update operator. This is used to identify which array elements should be removed
 * during a pull operation. The condition can specify field-level queries for object elements
 * or direct operator queries for primitive elements.
 *
 * The logic is equivalent to $elemMatch - if an element matches the condition, it will be
 * pulled (removed) from the array.
 *
 * @param element - The array element to test (can be primitive or object)
 * @param condition - An object specifying the match condition with field names and/or operators
 * @returns True if the element matches the pull condition (should be removed), false otherwise
 *
 * @example
 * // Pull objects with score less than 10
 * matchesPullCondition({ score: 5, name: 'test' }, { score: { $lt: 10 } }) // true
 * matchesPullCondition({ score: 15, name: 'test' }, { score: { $lt: 10 } }) // false
 *
 * @example
 * // Pull objects with exact match
 * matchesPullCondition({ status: 'inactive' }, { status: 'inactive' }) // true
 *
 * @example
 * // Pull primitives with operators
 * matchesPullCondition(5, { $lt: 10 }) // true
 * matchesPullCondition(15, { $lt: 10 }) // false
 */
export function matchesPullCondition(
  element: unknown,
  condition: Record<string, unknown>
): boolean {
  return matchesElemMatchCondition(element, condition);
}
