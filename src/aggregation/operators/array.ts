/**
 * Array expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";
import { compareValues } from "../../document-utils.ts";
import { getBSONTypeName } from "../helpers.ts";

export function evalSize(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number {
  const value = evaluate(args, doc);

  if (!Array.isArray(value)) {
    const typeName = getBSONTypeName(value);
    throw new Error(`The argument to $size must be an array, but was of type: ${typeName}`);
  }

  return value.length;
}

export function evalArrayElemAt(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  const [arrExpr, idxExpr] = args;
  const arr = evaluate(arrExpr, doc, vars);
  const idx = evaluate(idxExpr, doc, vars) as number;

  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$arrayElemAt requires an array, not ${typeName}`);
  }

  let actualIdx = idx;
  if (idx < 0) {
    actualIdx = arr.length + idx;
  }

  if (actualIdx < 0 || actualIdx >= arr.length) {
    return undefined;
  }

  return arr[actualIdx];
}

export function evalSlice(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  const arr = evaluate(args[0], doc, vars);

  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$slice requires an array, not ${typeName}`);
  }

  if (args.length === 2) {
    const n = evaluate(args[1], doc, vars) as number;
    if (n >= 0) {
      return arr.slice(0, n);
    } else {
      return arr.slice(n);
    }
  } else {
    const position = evaluate(args[1], doc, vars) as number;
    const n = evaluate(args[2], doc, vars) as number;

    let startIdx = position;
    if (position < 0) {
      startIdx = Math.max(0, arr.length + position);
    }

    return arr.slice(startIdx, startIdx + n);
  }
}

export function evalConcatArrays(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  const arrays = args.map((a) => evaluate(a, doc, vars));

  for (const arr of arrays) {
    if (arr === null || arr === undefined) {
      return null;
    }
    if (!Array.isArray(arr)) {
      const typeName = getBSONTypeName(arr);
      throw new Error(`$concatArrays only supports arrays, not ${typeName}`);
    }
  }

  return (arrays as unknown[][]).flat();
}

export function evalFilter(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  const spec = args as { input: unknown; as?: string; cond: unknown };
  const input = evaluate(spec.input, doc, vars);
  const varName = spec.as || "this";
  const condExpr = spec.cond;

  if (input === null || input === undefined) {
    return null;
  }

  if (!Array.isArray(input)) {
    const typeName = getBSONTypeName(input);
    throw new Error(`input to $filter must be an array, not ${typeName}`);
  }

  const result: unknown[] = [];
  for (const item of input) {
    const newVars = { ...vars, [varName]: item };
    const condResult = evaluate(condExpr, doc, newVars);
    if (condResult) {
      result.push(item);
    }
  }

  return result;
}

export function evalMap(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  const spec = args as { input: unknown; as?: string; in: unknown };
  const input = evaluate(spec.input, doc, vars);
  const varName = spec.as || "this";
  const inExpr = spec.in;

  if (input === null || input === undefined) {
    return null;
  }

  if (!Array.isArray(input)) {
    const typeName = getBSONTypeName(input);
    throw new Error(`input to $map must be an array, not ${typeName}`);
  }

  const result: unknown[] = [];
  for (const item of input) {
    const newVars = { ...vars, [varName]: item };
    const mappedValue = evaluate(inExpr, doc, newVars);
    result.push(mappedValue);
  }

  return result;
}

export function evalReduce(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  const spec = args as { input: unknown; initialValue: unknown; in: unknown };
  const input = evaluate(spec.input, doc, vars);
  const initialValue = evaluate(spec.initialValue, doc, vars);
  const inExpr = spec.in;

  if (input === null || input === undefined) {
    return null;
  }

  if (!Array.isArray(input)) {
    const typeName = getBSONTypeName(input);
    throw new Error(`input to $reduce must be an array, not ${typeName}`);
  }

  if (input.length === 0) {
    return initialValue;
  }

  let value = initialValue;
  for (const item of input) {
    const newVars = { ...vars, value, this: item };
    value = evaluate(inExpr, doc, newVars);
  }

  return value;
}

export function evalIn(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [elementExpr, arrExpr] = args;
  const element = evaluate(elementExpr, doc, vars);
  const arr = evaluate(arrExpr, doc, vars);

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$in requires an array as the second argument, found: ${typeName}`);
  }

  for (const item of arr) {
    if (compareValues(element, item) === 0) {
      return true;
    }
  }

  return false;
}

/**
 * $first - Returns the first element of an array.
 */
export function evalFirst(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  const arr = evaluate(args, doc, vars);

  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$first requires an array argument, not ${typeName}`);
  }

  if (arr.length === 0) {
    return undefined;
  }

  return arr[0];
}

/**
 * $last - Returns the last element of an array.
 */
export function evalLast(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  const arr = evaluate(args, doc, vars);

  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$last requires an array argument, not ${typeName}`);
  }

  if (arr.length === 0) {
    return undefined;
  }

  return arr[arr.length - 1];
}

/**
 * $indexOfArray - Returns the index of first occurrence of a value in an array.
 */
export function evalIndexOfArray(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const [arrExpr, searchExpr, startExpr, endExpr] = args;
  const arr = evaluate(arrExpr, doc, vars);
  const search = evaluate(searchExpr, doc, vars);

  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$indexOfArray requires an array, not ${typeName}`);
  }

  let start = 0;
  let end = arr.length;

  if (startExpr !== undefined) {
    start = evaluate(startExpr, doc, vars) as number;
    if (start < 0) {
      throw new Error("$indexOfArray start index cannot be negative");
    }
  }

  if (endExpr !== undefined) {
    end = evaluate(endExpr, doc, vars) as number;
    if (end < 0) {
      throw new Error("$indexOfArray end index cannot be negative");
    }
  }

  for (let i = start; i < Math.min(end, arr.length); i++) {
    if (compareValues(arr[i], search) === 0) {
      return i;
    }
  }

  return -1;
}

/**
 * $isArray - Determines if the operand is an array.
 */
export function evalIsArray(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  // $isArray takes an array with one element in aggregation context
  let value: unknown;
  if (Array.isArray(args) && args.length === 1) {
    value = evaluate(args[0], doc, vars);
  } else {
    value = evaluate(args, doc, vars);
  }

  return Array.isArray(value);
}

/**
 * $range - Returns an array of integers in a sequence.
 */
export function evalRange(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number[] {
  const [startExpr, endExpr, stepExpr] = args;
  const start = evaluate(startExpr, doc, vars) as number;
  const end = evaluate(endExpr, doc, vars) as number;
  const step = stepExpr !== undefined ? (evaluate(stepExpr, doc, vars) as number) : 1;

  if (!Number.isInteger(start) || !Number.isInteger(end) || !Number.isInteger(step)) {
    throw new Error("$range requires integer arguments");
  }

  if (step === 0) {
    throw new Error("$range step cannot be zero");
  }

  const result: number[] = [];

  if (step > 0) {
    for (let i = start; i < end; i += step) {
      result.push(i);
    }
  } else {
    for (let i = start; i > end; i += step) {
      result.push(i);
    }
  }

  return result;
}

/**
 * $reverseArray - Returns an array with the elements in reverse order.
 */
export function evalReverseArray(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  const arr = evaluate(args, doc, vars);

  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$reverseArray requires an array argument, not ${typeName}`);
  }

  return [...arr].reverse();
}

/**
 * $arrayToObject - Converts an array of key-value pairs to an object.
 */
export function evalArrayToObject(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Record<string, unknown> | null {
  const arr = evaluate(args, doc, vars);

  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$arrayToObject requires an array argument, not ${typeName}`);
  }

  const result: Record<string, unknown> = {};

  for (const item of arr) {
    if (Array.isArray(item)) {
      // Format: [[k1, v1], [k2, v2]]
      if (item.length !== 2) {
        throw new Error("$arrayToObject requires array elements with exactly 2 elements");
      }
      const [k, v] = item;
      if (typeof k !== "string") {
        throw new Error("$arrayToObject requires string keys");
      }
      result[k] = v;
    } else if (item && typeof item === "object") {
      // Format: [{k: "key1", v: "value1"}, {k: "key2", v: "value2"}]
      const obj = item as Record<string, unknown>;
      if ("k" in obj && "v" in obj) {
        if (typeof obj.k !== "string") {
          throw new Error("$arrayToObject requires string keys");
        }
        result[obj.k] = obj.v;
      } else {
        throw new Error("$arrayToObject requires objects with 'k' and 'v' fields");
      }
    } else {
      throw new Error("$arrayToObject requires array elements to be arrays or objects");
    }
  }

  return result;
}

/**
 * $objectToArray - Converts an object to an array of key-value pairs.
 */
export function evalObjectToArray(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Array<{ k: string; v: unknown }> | null {
  const obj = evaluate(args, doc, vars);

  if (obj === null || obj === undefined) {
    return null;
  }

  if (typeof obj !== "object" || Array.isArray(obj)) {
    const typeName = getBSONTypeName(obj);
    throw new Error(`$objectToArray requires an object, not ${typeName}`);
  }

  return Object.entries(obj as Record<string, unknown>).map(([k, v]) => ({ k, v }));
}

/**
 * $zip - Merge two arrays element-wise.
 */
export function evalZip(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[][] | null {
  const spec = args as {
    inputs: unknown[];
    useLongestLength?: boolean;
    defaults?: unknown[];
  };

  const inputs = spec.inputs.map((input) => evaluate(input, doc, vars));

  // Check for null/undefined
  for (const arr of inputs) {
    if (arr === null || arr === undefined) {
      return null;
    }
    if (!Array.isArray(arr)) {
      const typeName = getBSONTypeName(arr);
      throw new Error(`$zip requires array inputs, not ${typeName}`);
    }
  }

  const arrays = inputs as unknown[][];
  const useLongestLength = spec.useLongestLength === true;
  const defaults = spec.defaults
    ? spec.defaults.map((d) => evaluate(d, doc, vars))
    : undefined;

  if (arrays.length === 0) {
    return [];
  }

  const lengths = arrays.map((arr) => arr.length);
  const outputLength = useLongestLength ? Math.max(...lengths) : Math.min(...lengths);

  const result: unknown[][] = [];
  for (let i = 0; i < outputLength; i++) {
    const row: unknown[] = [];
    for (let j = 0; j < arrays.length; j++) {
      if (i < arrays[j].length) {
        row.push(arrays[j][i]);
      } else if (defaults && j < defaults.length) {
        row.push(defaults[j]);
      } else {
        row.push(null);
      }
    }
    result.push(row);
  }

  return result;
}

/**
 * $sortArray - Sort an array by specified criteria.
 */
export function evalSortArray(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  const spec = args as { input: unknown; sortBy: Record<string, 1 | -1> | 1 | -1 };
  const input = evaluate(spec.input, doc, vars);

  if (input === null || input === undefined) {
    return null;
  }

  if (!Array.isArray(input)) {
    const typeName = getBSONTypeName(input);
    throw new Error(`$sortArray requires an array input, not ${typeName}`);
  }

  const sorted = [...input];
  const sortBy = spec.sortBy;

  if (typeof sortBy === "number") {
    // Sort primitives
    sorted.sort((a, b) => {
      const cmp = compareValues(a, b);
      return sortBy === 1 ? cmp : -cmp;
    });
  } else {
    // Sort by fields
    const sortFields = Object.entries(sortBy);
    sorted.sort((a, b) => {
      for (const [field, dir] of sortFields) {
        const aObj = a as Record<string, unknown>;
        const bObj = b as Record<string, unknown>;
        const aVal = field.includes(".")
          ? field.split(".").reduce((obj: unknown, key) => (obj as Record<string, unknown>)?.[key], aObj)
          : aObj[field];
        const bVal = field.includes(".")
          ? field.split(".").reduce((obj: unknown, key) => (obj as Record<string, unknown>)?.[key], bObj)
          : bObj[field];
        const cmp = compareValues(aVal, bVal);
        if (cmp !== 0) {
          return dir === 1 ? cmp : -cmp;
        }
      }
      return 0;
    });
  }

  return sorted;
}

/**
 * Helper function to check if two values are equal for set operations.
 * Uses compareValues for deep equality.
 */
function setContains(set: unknown[], value: unknown): boolean {
  for (const item of set) {
    if (compareValues(item, value) === 0) {
      return true;
    }
  }
  return false;
}

/**
 * $setUnion - Returns the union of all input arrays (unique values).
 */
export function evalSetUnion(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  const arrays = args.map((a) => evaluate(a, doc, vars));

  // Check for null/undefined
  for (const arr of arrays) {
    if (arr === null || arr === undefined) {
      return null;
    }
    if (!Array.isArray(arr)) {
      const typeName = getBSONTypeName(arr);
      throw new Error(`All operands of $setUnion must be arrays, not ${typeName}`);
    }
  }

  const result: unknown[] = [];
  for (const arr of arrays as unknown[][]) {
    for (const item of arr) {
      if (!setContains(result, item)) {
        result.push(item);
      }
    }
  }

  return result;
}

/**
 * $setIntersection - Returns elements common to all input arrays.
 */
export function evalSetIntersection(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  const arrays = args.map((a) => evaluate(a, doc, vars));

  // Check for null/undefined
  for (const arr of arrays) {
    if (arr === null || arr === undefined) {
      return null;
    }
    if (!Array.isArray(arr)) {
      const typeName = getBSONTypeName(arr);
      throw new Error(`All operands of $setIntersection must be arrays, not ${typeName}`);
    }
  }

  if (arrays.length === 0) {
    return [];
  }

  // Start with unique elements from first array
  const firstArray = arrays[0] as unknown[];
  const uniqueFirst: unknown[] = [];
  for (const item of firstArray) {
    if (!setContains(uniqueFirst, item)) {
      uniqueFirst.push(item);
    }
  }

  // Filter to only elements that exist in ALL arrays
  const result = uniqueFirst.filter((item) => {
    for (let i = 1; i < arrays.length; i++) {
      if (!setContains(arrays[i] as unknown[], item)) {
        return false;
      }
    }
    return true;
  });

  return result;
}

/**
 * $setDifference - Returns elements in first array but not in second.
 */
export function evalSetDifference(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown[] | null {
  if (args.length !== 2) {
    throw new Error("$setDifference requires exactly 2 arguments");
  }

  const [firstExpr, secondExpr] = args;
  const first = evaluate(firstExpr, doc, vars);
  const second = evaluate(secondExpr, doc, vars);

  if (first === null || first === undefined || second === null || second === undefined) {
    return null;
  }

  if (!Array.isArray(first)) {
    const typeName = getBSONTypeName(first);
    throw new Error(`$setDifference requires arrays, not ${typeName}`);
  }
  if (!Array.isArray(second)) {
    const typeName = getBSONTypeName(second);
    throw new Error(`$setDifference requires arrays, not ${typeName}`);
  }

  // Get unique elements from first that are not in second
  const result: unknown[] = [];
  for (const item of first) {
    if (!setContains(second, item) && !setContains(result, item)) {
      result.push(item);
    }
  }

  return result;
}

/**
 * $setEquals - Returns true if all input arrays have the same distinct elements.
 */
export function evalSetEquals(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const arrays = args.map((a) => evaluate(a, doc, vars));

  if (arrays.length < 2) {
    throw new Error("$setEquals requires at least 2 arguments");
  }

  // Check for null/undefined - MongoDB throws error for these
  for (let i = 0; i < arrays.length; i++) {
    const arr = arrays[i];
    if (arr === null || arr === undefined) {
      const typeName = getBSONTypeName(arr);
      throw new Error(`All operands of $setEquals must be arrays. ${i + 1}-th argument is of type: ${typeName}`);
    }
    if (!Array.isArray(arr)) {
      const typeName = getBSONTypeName(arr);
      throw new Error(`All operands of $setEquals must be arrays. ${i + 1}-th argument is of type: ${typeName}`);
    }
  }

  // Get unique elements from first array
  const firstSet: unknown[] = [];
  for (const item of arrays[0] as unknown[]) {
    if (!setContains(firstSet, item)) {
      firstSet.push(item);
    }
  }

  // Compare each array's unique elements with the first
  for (let i = 1; i < arrays.length; i++) {
    const currentSet: unknown[] = [];
    for (const item of arrays[i] as unknown[]) {
      if (!setContains(currentSet, item)) {
        currentSet.push(item);
      }
    }

    // Check same size
    if (firstSet.length !== currentSet.length) {
      return false;
    }

    // Check all elements in firstSet exist in currentSet
    for (const item of firstSet) {
      if (!setContains(currentSet, item)) {
        return false;
      }
    }
  }

  return true;
}

/**
 * $setIsSubset - Returns true if first array is a subset of second.
 */
export function evalSetIsSubset(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  if (args.length !== 2) {
    throw new Error("$setIsSubset requires exactly 2 arguments");
  }

  const [firstExpr, secondExpr] = args;
  const first = evaluate(firstExpr, doc, vars);
  const second = evaluate(secondExpr, doc, vars);

  // MongoDB throws errors for null/non-array inputs
  if (first === null || first === undefined || !Array.isArray(first)) {
    const typeName = getBSONTypeName(first);
    throw new Error(`both operands of $setIsSubset must be arrays. First argument is of type: ${typeName}`);
  }
  if (second === null || second === undefined || !Array.isArray(second)) {
    const typeName = getBSONTypeName(second);
    throw new Error(`both operands of $setIsSubset must be arrays. Second argument is of type: ${typeName}`);
  }

  // Check if every element in first exists in second
  for (const item of first) {
    if (!setContains(second, item)) {
      return false;
    }
  }

  return true;
}

/**
 * $allElementsTrue - Returns true if all elements in the array are truthy.
 */
export function evalAllElementsTrue(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  // $allElementsTrue takes an array with a single element that is the array to check
  if (args.length !== 1) {
    throw new Error("$allElementsTrue requires exactly 1 argument");
  }

  const arr = evaluate(args[0], doc, vars);

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$allElementsTrue requires an array, not ${typeName}`);
  }

  // Empty array returns true (vacuous truth)
  if (arr.length === 0) {
    return true;
  }

  for (const item of arr) {
    // Falsy values in MongoDB: false, 0, null, undefined
    if (item === false || item === 0 || item === null || item === undefined) {
      return false;
    }
  }

  return true;
}

/**
 * $anyElementTrue - Returns true if any element in the array is truthy.
 */
export function evalAnyElementTrue(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  // $anyElementTrue takes an array with a single element that is the array to check
  if (args.length !== 1) {
    throw new Error("$anyElementTrue requires exactly 1 argument");
  }

  const arr = evaluate(args[0], doc, vars);

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$anyElementTrue requires an array, not ${typeName}`);
  }

  // Empty array returns false
  if (arr.length === 0) {
    return false;
  }

  for (const item of arr) {
    // Truthy if not: false, 0, null, undefined
    if (item !== false && item !== 0 && item !== null && item !== undefined) {
      return true;
    }
  }

  return false;
}
