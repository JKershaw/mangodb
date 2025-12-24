/**
 * Aggregation Pipeline for MangoDB.
 *
 * This module provides MongoDB-compatible aggregation pipeline functionality.
 * Supports stages: $match, $project, $sort, $limit, $skip, $count, $unwind,
 * $group, $lookup, $addFields, $set, $replaceRoot, $out.
 */
import { matchesFilter } from "./query-matcher.ts";
import {
  getValueByPath,
  setValueByPath,
  compareValuesForSort,
} from "./utils.ts";
import { cloneDocument, compareValues } from "./document-utils.ts";
import type {
  Document,
  Filter,
  PipelineStage,
  SortSpec,
  UnwindOptions,
  ProjectExpression,
} from "./types.ts";

// ==================== Helper Functions ====================

/**
 * Get BSON type name for a value (used in error messages).
 */
function getBSONTypeName(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "missing";
  if (Array.isArray(value)) return "array";
  if (value instanceof Date) return "date";
  if (typeof value === "number") {
    return Number.isInteger(value) ? "int" : "double";
  }
  if (typeof value === "boolean") return "bool";
  if (typeof value === "string") return "string";
  if (typeof value === "object") {
    // Check for ObjectId
    if (value && typeof (value as { toHexString?: unknown }).toHexString === "function") {
      return "objectId";
    }
    return "object";
  }
  return typeof value;
}

/**
 * Deep equality check that handles primitives and objects.
 */
function deepEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null) return a === b;
  if (typeof a !== typeof b) return false;

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((val, i) => deepEquals(val, b[i]));
  }

  if (typeof a === "object" && typeof b === "object") {
    const aKeys = Object.keys(a as object);
    const bKeys = Object.keys(b as object);
    if (aKeys.length !== bKeys.length) return false;
    return aKeys.every(key =>
      deepEquals((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key])
    );
  }

  return false;
}

// ==================== Expression Evaluation ====================

/**
 * Variable context for $filter, $map, $reduce expressions.
 */
type VariableContext = Record<string, unknown>;

/**
 * Evaluate an aggregation expression against a document.
 *
 * Expressions can be:
 * - Field references: "$fieldName" or "$nested.field"
 * - Variable references: "$$varName" or "$$varName.field"
 * - Literal values: numbers, strings, booleans, null
 * - Operator expressions: { $add: [...] }, { $concat: [...] }, etc.
 *
 * @param expr - The expression to evaluate
 * @param doc - The document context
 * @param vars - Optional variable context for scoped variables
 * @returns The evaluated value
 */
export function evaluateExpression(expr: unknown, doc: Document, vars?: VariableContext): unknown {
  // String starting with $$ is a variable reference
  if (typeof expr === "string" && expr.startsWith("$$")) {
    const varPath = expr.slice(2);
    const dotIndex = varPath.indexOf(".");
    if (dotIndex === -1) {
      // Simple variable reference: $$varName
      return vars?.[varPath];
    } else {
      // Nested variable reference: $$varName.field
      const varName = varPath.slice(0, dotIndex);
      const fieldPath = varPath.slice(dotIndex + 1);
      const varValue = vars?.[varName];
      if (varValue && typeof varValue === "object") {
        return getValueByPath(varValue as Document, fieldPath);
      }
      return undefined;
    }
  }

  // String starting with $ is a field reference
  if (typeof expr === "string" && expr.startsWith("$")) {
    const fieldPath = expr.slice(1);
    return getValueByPath(doc, fieldPath);
  }

  // Primitive values returned as-is
  if (expr === null || typeof expr !== "object") {
    return expr;
  }

  // Arrays - evaluate each element
  if (Array.isArray(expr)) {
    return expr.map((item) => evaluateExpression(item, doc, vars));
  }

  // Object with operator key
  const exprObj = expr as Record<string, unknown>;
  const keys = Object.keys(exprObj);

  if (keys.length === 1 && keys[0].startsWith("$")) {
    return evaluateOperator(keys[0], exprObj[keys[0]], doc, vars);
  }

  // Object literal - evaluate each field
  const result: Document = {};
  for (const [key, value] of Object.entries(exprObj)) {
    result[key] = evaluateExpression(value, doc, vars);
  }
  return result;
}

/**
 * Evaluate a specific operator expression.
 */
function evaluateOperator(op: string, args: unknown, doc: Document, vars?: VariableContext): unknown {
  switch (op) {
    case "$literal":
      return args; // Return as-is without evaluation

    // Arithmetic operators
    case "$add":
      return evalAdd(args as unknown[], doc, vars);
    case "$subtract":
      return evalSubtract(args as unknown[], doc, vars);
    case "$multiply":
      return evalMultiply(args as unknown[], doc, vars);
    case "$divide":
      return evalDivide(args as unknown[], doc, vars);
    case "$abs":
      return evalAbs(args, doc);
    case "$ceil":
      return evalCeil(args, doc);
    case "$floor":
      return evalFloor(args, doc);
    case "$round":
      return evalRound(args, doc);
    case "$mod":
      return evalMod(args as unknown[], doc);

    // String operators
    case "$concat":
      return evalConcat(args as unknown[], doc, vars);
    case "$toUpper":
      return evalToUpper(args, doc);
    case "$toLower":
      return evalToLower(args, doc);
    case "$substrCP":
      return evalSubstrCP(args as unknown[], doc);
    case "$strLenCP":
      return evalStrLenCP(args, doc);
    case "$split":
      return evalSplit(args as unknown[], doc);
    case "$trim":
      return evalTrim(args, doc);
    case "$ltrim":
      return evalLTrim(args, doc);
    case "$rtrim":
      return evalRTrim(args, doc);
    case "$toString":
      return evalToString(args, doc);
    case "$indexOfCP":
      return evalIndexOfCP(args as unknown[], doc);

    // Conditional operators
    case "$cond":
      return evalCond(args, doc);
    case "$ifNull":
      return evalIfNull(args as unknown[], doc);

    // Comparison operators (for $cond conditions)
    // Uses BSON type ordering for cross-type comparisons
    case "$gt":
      return evalComparison(args as unknown[], doc, vars, (a, b) => compareValues(a, b) > 0);
    case "$gte":
      return evalComparison(args as unknown[], doc, vars, (a, b) => compareValues(a, b) >= 0);
    case "$lt":
      return evalComparison(args as unknown[], doc, vars, (a, b) => compareValues(a, b) < 0);
    case "$lte":
      return evalComparison(args as unknown[], doc, vars, (a, b) => compareValues(a, b) <= 0);
    case "$eq":
      return evalComparison(args as unknown[], doc, vars, (a, b) => compareValues(a, b) === 0);
    case "$ne":
      return evalComparison(args as unknown[], doc, vars, (a, b) => compareValues(a, b) !== 0);

    // Array operators
    case "$size":
      return evalSize(args, doc);
    case "$arrayElemAt":
      return evalArrayElemAt(args as unknown[], doc, vars);
    case "$slice":
      return evalSlice(args as unknown[], doc, vars);
    case "$concatArrays":
      return evalConcatArrays(args as unknown[], doc, vars);
    case "$filter":
      return evalFilter(args, doc, vars);
    case "$map":
      return evalMap(args, doc, vars);
    case "$reduce":
      return evalReduce(args, doc, vars);
    case "$in":
      return evalIn(args as unknown[], doc, vars);

    default:
      throw new Error(`Unrecognized expression operator: '${op}'`);
  }
}

// ==================== Arithmetic Operators ====================

function evalAdd(args: unknown[], doc: Document, vars?: VariableContext): number | null {
  const values = args.map((a) => evaluateExpression(a, doc, vars));

  // null/undefined propagates
  if (values.some((v) => v === null || v === undefined)) {
    return null;
  }

  let sum = 0;
  for (const v of values) {
    if (typeof v !== "number") {
      throw new Error("$add only supports numeric types");
    }
    sum += v;
  }
  return sum;
}

function evalSubtract(args: unknown[], doc: Document, vars?: VariableContext): number | null {
  const [arg1, arg2] = args.map((a) => evaluateExpression(a, doc, vars));

  if (arg1 === null || arg1 === undefined || arg2 === null || arg2 === undefined) {
    return null;
  }

  if (typeof arg1 !== "number" || typeof arg2 !== "number") {
    throw new Error("$subtract only supports numeric types");
  }

  return arg1 - arg2;
}

function evalMultiply(args: unknown[], doc: Document, vars?: VariableContext): number | null {
  const values = args.map((a) => evaluateExpression(a, doc, vars));

  if (values.some((v) => v === null || v === undefined)) {
    return null;
  }

  let product = 1;
  for (const v of values) {
    if (typeof v !== "number") {
      throw new Error("$multiply only supports numeric types");
    }
    product *= v;
  }
  return product;
}

function evalDivide(args: unknown[], doc: Document, vars?: VariableContext): number | null {
  const [dividend, divisor] = args.map((a) => evaluateExpression(a, doc, vars));

  if (dividend === null || dividend === undefined || divisor === null || divisor === undefined) {
    return null;
  }

  if (typeof dividend !== "number" || typeof divisor !== "number") {
    throw new Error("$divide only supports numeric types");
  }

  // MongoDB returns null for divide by zero (doesn't throw)
  if (divisor === 0) {
    return null;
  }

  return dividend / divisor;
}

function evalAbs(args: unknown, doc: Document): number | null {
  const value = evaluateExpression(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "number") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$abs only supports numeric types, not ${typeName}`);
  }

  return Math.abs(value);
}

function evalCeil(args: unknown, doc: Document): number | null {
  const value = evaluateExpression(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "number") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$ceil only supports numeric types, not ${typeName}`);
  }

  return Math.ceil(value);
}

function evalFloor(args: unknown, doc: Document): number | null {
  const value = evaluateExpression(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "number") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$floor only supports numeric types, not ${typeName}`);
  }

  return Math.floor(value);
}

function evalRound(args: unknown, doc: Document): number | null {
  // Handle both single value and array form
  let numberExpr: unknown;
  let placeExpr: unknown = 0;

  if (Array.isArray(args)) {
    [numberExpr, placeExpr = 0] = args;
  } else {
    numberExpr = args;
  }

  const value = evaluateExpression(numberExpr, doc);
  const place = evaluateExpression(placeExpr, doc) as number;

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "number") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$round only supports numeric types, not ${typeName}`);
  }

  if (place === 0) {
    return Math.round(value);
  }

  // Round to specified decimal places
  const multiplier = Math.pow(10, place);
  return Math.round(value * multiplier) / multiplier;
}

function evalMod(args: unknown[], doc: Document): number | null {
  const [dividendExpr, divisorExpr] = args;
  const dividend = evaluateExpression(dividendExpr, doc);
  const divisor = evaluateExpression(divisorExpr, doc);

  if (dividend === null || dividend === undefined || divisor === null || divisor === undefined) {
    return null;
  }

  if (typeof dividend !== "number" || typeof divisor !== "number") {
    const dividendType = getBSONTypeName(dividend);
    const divisorType = getBSONTypeName(divisor);
    throw new Error(`$mod only supports numeric types, not ${dividendType} and ${divisorType}`);
  }

  // MongoDB returns null for mod by zero
  if (divisor === 0) {
    return null;
  }

  // JavaScript % operator follows dividend sign (truncated division)
  return dividend % divisor;
}

// ==================== String Operators ====================

function evalConcat(args: unknown[], doc: Document, vars?: VariableContext): string | null {
  const values = args.map((a) => evaluateExpression(a, doc, vars));

  // null propagates
  if (values.some((v) => v === null || v === undefined)) {
    return null;
  }

  // All values must be strings
  for (const v of values) {
    if (typeof v !== "string") {
      // MongoDB uses BSON type names
      const typeName = getBSONTypeName(v);
      throw new Error(`$concat only supports strings, not ${typeName}`);
    }
  }

  return values.join("");
}

function evalToUpper(args: unknown, doc: Document): string {
  const value = evaluateExpression(args, doc);

  // MongoDB returns empty string for null/undefined
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value !== "string") {
    throw new Error("$toUpper requires a string argument");
  }

  return value.toUpperCase();
}

function evalToLower(args: unknown, doc: Document): string {
  const value = evaluateExpression(args, doc);

  // MongoDB returns empty string for null/undefined
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value !== "string") {
    throw new Error("$toLower requires a string argument");
  }

  return value.toLowerCase();
}

function evalSubstrCP(args: unknown[], doc: Document): string {
  const [strExpr, startExpr, countExpr] = args;
  const str = evaluateExpression(strExpr, doc);
  const start = evaluateExpression(startExpr, doc) as number;
  const count = evaluateExpression(countExpr, doc) as number;

  // MongoDB returns empty string for null/undefined
  if (str === null || str === undefined) {
    return "";
  }

  if (typeof str !== "string") {
    const typeName = getBSONTypeName(str);
    throw new Error(`$substrCP requires a string argument, found: ${typeName}`);
  }

  // Handle out of bounds gracefully
  return str.substring(start, start + count);
}

function evalStrLenCP(args: unknown, doc: Document): number {
  const value = evaluateExpression(args, doc);

  // $strLenCP throws error for null/missing (unlike other string operators)
  if (value === null || value === undefined) {
    const typeName = value === null ? "null" : "missing";
    throw new Error(`$strLenCP requires a string argument, found: ${typeName}`);
  }

  if (typeof value !== "string") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$strLenCP requires a string argument, found: ${typeName}`);
  }

  return value.length;
}

function evalSplit(args: unknown[], doc: Document): string[] {
  const [strExpr, delimExpr] = args;
  const str = evaluateExpression(strExpr, doc);
  const delim = evaluateExpression(delimExpr, doc);

  if (str === null || str === undefined) {
    throw new Error("$split requires a string as the first argument, found: null");
  }

  if (typeof str !== "string") {
    const typeName = getBSONTypeName(str);
    throw new Error(`$split requires a string as the first argument, found: ${typeName}`);
  }

  if (delim === null || delim === undefined) {
    throw new Error("$split requires a string as the second argument, found: null");
  }

  if (typeof delim !== "string") {
    const typeName = getBSONTypeName(delim);
    throw new Error(`$split requires a string as the second argument, found: ${typeName}`);
  }

  return str.split(delim);
}

function evalTrim(args: unknown, doc: Document): string {
  const spec = args as { input: unknown; chars?: unknown };
  const input = evaluateExpression(spec.input, doc);
  const chars = spec.chars ? evaluateExpression(spec.chars, doc) as string : undefined;

  if (typeof input !== "string") {
    const typeName = getBSONTypeName(input);
    throw new Error(`$trim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    // Trim custom characters
    const charSet = new Set(chars.split(""));
    let start = 0;
    let end = input.length;
    while (start < end && charSet.has(input[start])) start++;
    while (end > start && charSet.has(input[end - 1])) end--;
    return input.substring(start, end);
  }

  return input.trim();
}

function evalLTrim(args: unknown, doc: Document): string {
  const spec = args as { input: unknown; chars?: unknown };
  const input = evaluateExpression(spec.input, doc);
  const chars = spec.chars ? evaluateExpression(spec.chars, doc) as string : undefined;

  if (typeof input !== "string") {
    const typeName = getBSONTypeName(input);
    throw new Error(`$ltrim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    const charSet = new Set(chars.split(""));
    let start = 0;
    while (start < input.length && charSet.has(input[start])) start++;
    return input.substring(start);
  }

  return input.trimStart();
}

function evalRTrim(args: unknown, doc: Document): string {
  const spec = args as { input: unknown; chars?: unknown };
  const input = evaluateExpression(spec.input, doc);
  const chars = spec.chars ? evaluateExpression(spec.chars, doc) as string : undefined;

  if (typeof input !== "string") {
    const typeName = getBSONTypeName(input);
    throw new Error(`$rtrim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    const charSet = new Set(chars.split(""));
    let end = input.length;
    while (end > 0 && charSet.has(input[end - 1])) end--;
    return input.substring(0, end);
  }

  return input.trimEnd();
}

function evalToString(args: unknown, doc: Document): string | null {
  const value = evaluateExpression(args, doc);

  // Null/missing returns null
  if (value === null || value === undefined) {
    return null;
  }

  // String unchanged
  if (typeof value === "string") {
    return value;
  }

  // Boolean
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  // Number
  if (typeof value === "number") {
    return String(value);
  }

  // Date
  if (value instanceof Date) {
    return value.toISOString();
  }

  // ObjectId (has toHexString method)
  if (value && typeof (value as { toHexString?: unknown }).toHexString === "function") {
    return (value as { toHexString: () => string }).toHexString();
  }

  // Array and object not supported
  const typeName = getBSONTypeName(value);
  throw new Error(`Unsupported conversion from ${typeName} to string`);
}

function evalIndexOfCP(args: unknown[], doc: Document): number | null {
  const [strExpr, substrExpr, startExpr, endExpr] = args;
  const str = evaluateExpression(strExpr, doc);
  const substr = evaluateExpression(substrExpr, doc);
  const start = startExpr !== undefined ? evaluateExpression(startExpr, doc) as number : 0;
  const end = endExpr !== undefined ? evaluateExpression(endExpr, doc) as number : undefined;

  // Null string returns null
  if (str === null || str === undefined) {
    return null;
  }

  // Null substring is an error
  if (substr === null || substr === undefined) {
    throw new Error("$indexOfCP requires a string as the second argument, found: null");
  }

  if (typeof str !== "string") {
    const typeName = getBSONTypeName(str);
    throw new Error(`$indexOfCP requires a string as the first argument, found: ${typeName}`);
  }

  if (typeof substr !== "string") {
    const typeName = getBSONTypeName(substr);
    throw new Error(`$indexOfCP requires a string as the second argument, found: ${typeName}`);
  }

  // Search within bounds
  const searchStr = end !== undefined ? str.substring(0, end) : str;
  const index = searchStr.indexOf(substr, start);

  return index;
}

// ==================== Conditional Operators ====================

function evalCond(args: unknown, doc: Document): unknown {
  let condition: unknown;
  let thenValue: unknown;
  let elseValue: unknown;

  if (Array.isArray(args)) {
    // Array syntax: [$cond: [condition, thenValue, elseValue]]
    [condition, thenValue, elseValue] = args;
  } else if (typeof args === "object" && args !== null) {
    // Object syntax: { $cond: { if: condition, then: thenValue, else: elseValue } }
    const obj = args as { if: unknown; then: unknown; else: unknown };
    condition = obj.if;
    thenValue = obj.then;
    elseValue = obj.else;
  } else {
    throw new Error("$cond requires an array or object argument");
  }

  const evalCondition = evaluateExpression(condition, doc);

  // Truthy check
  if (evalCondition) {
    return evaluateExpression(thenValue, doc);
  } else {
    return evaluateExpression(elseValue, doc);
  }
}

function evalIfNull(args: unknown[], doc: Document): unknown {
  for (const arg of args) {
    const value = evaluateExpression(arg, doc);
    if (value !== null && value !== undefined) {
      return value;
    }
  }
  return null;
}

function evalComparison(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  compareFn: (a: unknown, b: unknown) => boolean
): boolean {
  const [left, right] = args.map((a) => evaluateExpression(a, doc, vars));
  return compareFn(left, right);
}

// ==================== Array Operators ====================

function evalSize(args: unknown, doc: Document): number {
  const value = evaluateExpression(args, doc);

  if (!Array.isArray(value)) {
    const typeName = getBSONTypeName(value);
    throw new Error(`The argument to $size must be an array, but was of type: ${typeName}`);
  }

  return value.length;
}

function evalArrayElemAt(args: unknown[], doc: Document, vars?: VariableContext): unknown {
  const [arrExpr, idxExpr] = args;
  const arr = evaluateExpression(arrExpr, doc, vars);
  const idx = evaluateExpression(idxExpr, doc, vars) as number;

  // Null/missing returns null
  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$arrayElemAt requires an array, not ${typeName}`);
  }

  // Handle negative index
  let actualIdx = idx;
  if (idx < 0) {
    actualIdx = arr.length + idx;
  }

  // Out of bounds returns null (not undefined)
  if (actualIdx < 0 || actualIdx >= arr.length) {
    return null;
  }

  return arr[actualIdx];
}

function evalSlice(args: unknown[], doc: Document, vars?: VariableContext): unknown[] | null {
  const arr = evaluateExpression(args[0], doc, vars);

  // Null/missing returns null
  if (arr === null || arr === undefined) {
    return null;
  }

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$slice requires an array, not ${typeName}`);
  }

  if (args.length === 2) {
    // 2-arg form: [array, n]
    const n = evaluateExpression(args[1], doc, vars) as number;
    if (n >= 0) {
      // First n elements
      return arr.slice(0, n);
    } else {
      // Last -n elements
      return arr.slice(n);
    }
  } else {
    // 3-arg form: [array, position, n]
    const position = evaluateExpression(args[1], doc, vars) as number;
    const n = evaluateExpression(args[2], doc, vars) as number;

    let startIdx = position;
    if (position < 0) {
      startIdx = Math.max(0, arr.length + position);
    }

    return arr.slice(startIdx, startIdx + n);
  }
}

function evalConcatArrays(args: unknown[], doc: Document, vars?: VariableContext): unknown[] | null {
  const arrays = args.map((a) => evaluateExpression(a, doc, vars));

  // If any is null/undefined, return null
  for (const arr of arrays) {
    if (arr === null || arr === undefined) {
      return null;
    }
    if (!Array.isArray(arr)) {
      const typeName = getBSONTypeName(arr);
      throw new Error(`$concatArrays only supports arrays, not ${typeName}`);
    }
  }

  // Concatenate all arrays
  return (arrays as unknown[][]).flat();
}

function evalFilter(args: unknown, doc: Document, vars?: VariableContext): unknown[] | null {
  const spec = args as { input: unknown; as?: string; cond: unknown };
  const input = evaluateExpression(spec.input, doc, vars);
  const varName = spec.as || "this";
  const condExpr = spec.cond;

  // Null/missing returns null
  if (input === null || input === undefined) {
    return null;
  }

  if (!Array.isArray(input)) {
    const typeName = getBSONTypeName(input);
    throw new Error(`input to $filter must be an array, not ${typeName}`);
  }

  const result: unknown[] = [];
  for (const item of input) {
    // Create new variable context with the current item
    const newVars = { ...vars, [varName]: item };
    const condResult = evaluateExpression(condExpr, doc, newVars);
    if (condResult) {
      result.push(item);
    }
  }

  return result;
}

function evalMap(args: unknown, doc: Document, vars?: VariableContext): unknown[] | null {
  const spec = args as { input: unknown; as: string; in: unknown };
  const input = evaluateExpression(spec.input, doc, vars);
  const varName = spec.as || "this";
  const inExpr = spec.in;

  // Null/missing returns null
  if (input === null || input === undefined) {
    return null;
  }

  if (!Array.isArray(input)) {
    const typeName = getBSONTypeName(input);
    throw new Error(`input to $map must be an array, not ${typeName}`);
  }

  const result: unknown[] = [];
  for (const item of input) {
    // Create new variable context with the current item
    const newVars = { ...vars, [varName]: item };
    const mappedValue = evaluateExpression(inExpr, doc, newVars);
    result.push(mappedValue);
  }

  return result;
}

function evalReduce(args: unknown, doc: Document, vars?: VariableContext): unknown {
  const spec = args as { input: unknown; initialValue: unknown; in: unknown };
  const input = evaluateExpression(spec.input, doc, vars);
  const initialValue = evaluateExpression(spec.initialValue, doc, vars);
  const inExpr = spec.in;

  // Null/missing returns null
  if (input === null || input === undefined) {
    return null;
  }

  if (!Array.isArray(input)) {
    const typeName = getBSONTypeName(input);
    throw new Error(`input to $reduce must be an array, not ${typeName}`);
  }

  // Empty array returns initialValue
  if (input.length === 0) {
    return initialValue;
  }

  let value = initialValue;
  for (const item of input) {
    // Create new variable context with $$value and $$this
    const newVars = { ...vars, value, this: item };
    value = evaluateExpression(inExpr, doc, newVars);
  }

  return value;
}

function evalIn(args: unknown[], doc: Document, vars?: VariableContext): boolean {
  const [elementExpr, arrExpr] = args;
  const element = evaluateExpression(elementExpr, doc, vars);
  const arr = evaluateExpression(arrExpr, doc, vars);

  if (!Array.isArray(arr)) {
    const typeName = getBSONTypeName(arr);
    throw new Error(`$in requires an array as the second argument, found: ${typeName}`);
  }

  // Use compareValues for proper equality checking
  for (const item of arr) {
    if (compareValues(element, item) === 0) {
      return true;
    }
  }

  return false;
}

// ==================== Accumulator Classes ====================

interface Accumulator {
  accumulate(doc: Document): void;
  getResult(): unknown;
}

class SumAccumulator implements Accumulator {
  private sum = 0;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (typeof value === "number") {
      this.sum += value;
    }
    // Non-numbers ignored
  }

  getResult(): number {
    return this.sum;
  }
}

class AvgAccumulator implements Accumulator {
  private sum = 0;
  private count = 0;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (typeof value === "number") {
      this.sum += value;
      this.count++;
    }
  }

  getResult(): number | null {
    return this.count > 0 ? this.sum / this.count : null;
  }
}

class MinAccumulator implements Accumulator {
  private min: unknown = undefined;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (value !== null && value !== undefined) {
      if (this.min === undefined || compareValuesForSort(value, this.min, 1) < 0) {
        this.min = value;
      }
    }
  }

  getResult(): unknown {
    return this.min === undefined ? null : this.min;
  }
}

class MaxAccumulator implements Accumulator {
  private max: unknown = undefined;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (value !== null && value !== undefined) {
      if (this.max === undefined || compareValuesForSort(value, this.max, 1) > 0) {
        this.max = value;
      }
    }
  }

  getResult(): unknown {
    return this.max === undefined ? null : this.max;
  }
}

class FirstAccumulator implements Accumulator {
  private first: unknown = undefined;
  private hasValue = false;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    if (!this.hasValue) {
      this.first = evaluateExpression(this.expr, doc);
      this.hasValue = true;
    }
  }

  getResult(): unknown {
    return this.first;
  }
}

class LastAccumulator implements Accumulator {
  private last: unknown = undefined;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    this.last = evaluateExpression(this.expr, doc);
  }

  getResult(): unknown {
    return this.last;
  }
}

class PushAccumulator implements Accumulator {
  private values: unknown[] = [];
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    this.values.push(value);
  }

  getResult(): unknown[] {
    return this.values;
  }
}

class AddToSetAccumulator implements Accumulator {
  private values: unknown[] = [];
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    // Check if already exists (using deep equality that handles primitives)
    if (!this.values.some((v) => deepEquals(v, value))) {
      this.values.push(value);
    }
  }

  getResult(): unknown[] {
    return this.values;
  }
}

function createAccumulator(op: string, expr: unknown): Accumulator {
  switch (op) {
    case "$sum":
      return new SumAccumulator(expr);
    case "$avg":
      return new AvgAccumulator(expr);
    case "$min":
      return new MinAccumulator(expr);
    case "$max":
      return new MaxAccumulator(expr);
    case "$first":
      return new FirstAccumulator(expr);
    case "$last":
      return new LastAccumulator(expr);
    case "$push":
      return new PushAccumulator(expr);
    case "$addToSet":
      return new AddToSetAccumulator(expr);
    default:
      throw new Error(`unknown group operator '${op}'`);
  }
}

// ==================== Database Context Interface ====================

/**
 * Interface for database context needed by $lookup and $out stages.
 */
export interface AggregationDbContext {
  getCollection(name: string): {
    find(filter: Document): { toArray(): Promise<Document[]> };
    deleteMany(filter: Document): Promise<unknown>;
    insertMany(docs: Document[]): Promise<unknown>;
  };
}

// ==================== AggregationCursor Class ====================

/**
 * AggregationCursor represents a cursor over aggregation pipeline results.
 *
 * It executes a series of pipeline stages against a collection of documents,
 * transforming them according to each stage's operation.
 *
 * @template T - The input document type
 *
 * @example
 * ```typescript
 * const cursor = collection.aggregate([
 *   { $match: { status: "active" } },
 *   { $sort: { createdAt: -1 } },
 *   { $limit: 10 }
 * ]);
 * const results = await cursor.toArray();
 * ```
 */
export class AggregationCursor<T extends Document = Document> {
  private readonly source: () => Promise<T[]>;
  private readonly pipeline: PipelineStage[];
  private readonly dbContext?: AggregationDbContext;

  /**
   * Creates a new AggregationCursor instance.
   *
   * @param source - Function that returns a promise resolving to source documents
   * @param pipeline - Array of pipeline stages to execute
   * @param dbContext - Optional database context for $lookup and $out stages
   */
  constructor(
    source: () => Promise<T[]>,
    pipeline: PipelineStage[],
    dbContext?: AggregationDbContext
  ) {
    this.source = source;
    this.pipeline = pipeline;
    this.dbContext = dbContext;
  }

  /**
   * Executes the aggregation pipeline and returns all results as an array.
   *
   * Stages are executed sequentially, with each stage transforming the
   * document stream for the next stage.
   *
   * @returns Promise resolving to an array of result documents
   * @throws Error if an unknown pipeline stage is encountered
   */
  async toArray(): Promise<Document[]> {
    // Validate $out is last stage if present
    for (let i = 0; i < this.pipeline.length; i++) {
      const stage = this.pipeline[i] as unknown as Record<string, unknown>;
      if ("$out" in stage && i !== this.pipeline.length - 1) {
        throw new Error("$out can only be the final stage in the pipeline");
      }
    }

    let documents: Document[] = await this.source();

    for (const stage of this.pipeline) {
      documents = await this.executeStage(stage, documents);
    }

    return documents;
  }

  /**
   * Execute a single pipeline stage on the document stream.
   */
  private async executeStage(
    stage: PipelineStage,
    docs: Document[]
  ): Promise<Document[]> {
    const stageKeys = Object.keys(stage);
    if (stageKeys.length !== 1) {
      throw new Error("Pipeline stage must have exactly one field");
    }

    const stageKey = stageKeys[0];
    const stageValue = (stage as unknown as Record<string, unknown>)[stageKey];

    switch (stageKey) {
      case "$match":
        return this.execMatch(stageValue as Filter<Document>, docs);
      case "$project":
        return this.execProject(
          stageValue as Record<string, 0 | 1 | string | ProjectExpression>,
          docs
        );
      case "$sort":
        return this.execSort(stageValue as SortSpec, docs);
      case "$limit":
        return this.execLimit(stageValue as number, docs);
      case "$skip":
        return this.execSkip(stageValue as number, docs);
      case "$count":
        return this.execCount(stageValue as string, docs);
      case "$unwind":
        return this.execUnwind(stageValue as string | UnwindOptions, docs);
      case "$group":
        return this.execGroup(stageValue as { _id: unknown; [key: string]: unknown }, docs);
      case "$lookup":
        return this.execLookup(
          stageValue as { from: string; localField: string; foreignField: string; as: string },
          docs
        );
      case "$addFields":
        return this.execAddFields(stageValue as Record<string, unknown>, docs);
      case "$set":
        return this.execAddFields(stageValue as Record<string, unknown>, docs);
      case "$replaceRoot":
        return this.execReplaceRoot(stageValue as { newRoot: unknown }, docs);
      case "$out":
        return this.execOut(stageValue as string, docs);
      default:
        throw new Error(`Unrecognized pipeline stage name: '${stageKey}'`);
    }
  }

  // ==================== Basic Stage Implementations ====================

  private execMatch(filter: Filter<Document>, docs: Document[]): Document[] {
    return docs.filter((doc) => matchesFilter(doc, filter));
  }

  private execProject(
    projection: Record<string, 0 | 1 | string | ProjectExpression | unknown>,
    docs: Document[]
  ): Document[] {
    const keys = Object.keys(projection);
    if (keys.length === 0) {
      throw new Error("$project requires at least one field");
    }

    // Determine projection mode
    const nonIdKeys = keys.filter((k) => k !== "_id");
    let hasInclusion = false;
    let hasExclusion = false;
    let hasFieldRef = false;
    let hasExpression = false;

    for (const key of nonIdKeys) {
      const value = projection[key];
      if (value === 1) {
        hasInclusion = true;
      } else if (value === 0) {
        hasExclusion = true;
      } else if (typeof value === "string" && value.startsWith("$")) {
        hasFieldRef = true;
      } else if (typeof value === "object" && value !== null) {
        hasExpression = true;
      }
    }

    // Field refs and expressions count as inclusion mode
    if (hasFieldRef || hasExpression) {
      hasInclusion = true;
    }

    // Cannot mix inclusion and exclusion (except _id)
    if (hasInclusion && hasExclusion) {
      throw new Error("Cannot mix inclusion and exclusion in projection");
    }

    const isExclusionMode =
      hasExclusion || (nonIdKeys.length === 0 && projection._id === 0);

    return docs.map((doc) =>
      this.projectDocument(doc, projection, isExclusionMode)
    );
  }

  private isLiteralExpression(value: unknown): value is ProjectExpression {
    return (
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      "$literal" in value
    );
  }

  private projectDocument(
    doc: Document,
    projection: Record<string, unknown>,
    isExclusionMode: boolean
  ): Document {
    const keys = Object.keys(projection);

    if (isExclusionMode) {
      const result = cloneDocument(doc);
      for (const key of keys) {
        if (projection[key] === 0) {
          if (key.includes(".")) {
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
      return result;
    }

    // Inclusion mode
    const result: Document = {};

    // Handle _id (included by default unless explicitly excluded)
    if (projection._id !== 0) {
      result._id = doc._id;
    }

    // Process other fields
    const nonIdKeys = keys.filter((k) => k !== "_id");
    for (const key of nonIdKeys) {
      const value = projection[key];

      if (value === 1) {
        const fieldValue = getValueByPath(doc, key);
        if (fieldValue !== undefined) {
          if (key.includes(".")) {
            setValueByPath(result, key, fieldValue);
          } else {
            result[key] = fieldValue;
          }
        }
      } else if (typeof value === "string" && value.startsWith("$")) {
        const refPath = value.slice(1);
        const refValue = getValueByPath(doc, refPath);
        if (refValue !== undefined) {
          result[key] = refValue;
        }
      } else if (this.isLiteralExpression(value)) {
        result[key] = value.$literal;
      } else if (typeof value === "object" && value !== null) {
        // Expression - evaluate it
        const evaluated = evaluateExpression(value, doc);
        result[key] = evaluated;
      }
    }

    return result;
  }

  private execSort(sortSpec: SortSpec, docs: Document[]): Document[] {
    const sortFields = Object.entries(sortSpec) as [string, 1 | -1][];

    return [...docs].sort((a, b) => {
      for (const [field, direction] of sortFields) {
        const aValue = getValueByPath(a, field);
        const bValue = getValueByPath(b, field);
        const cmp = compareValuesForSort(aValue, bValue, direction);
        if (cmp !== 0) {
          return direction === 1 ? cmp : -cmp;
        }
      }
      return 0;
    });
  }

  private execLimit(limit: number, docs: Document[]): Document[] {
    if (typeof limit !== "number" || !Number.isFinite(limit)) {
      throw new Error(`Expected an integer: $limit: ${limit}`);
    }
    if (!Number.isInteger(limit)) {
      throw new Error(`Expected an integer: $limit: ${limit}`);
    }
    if (limit < 0) {
      throw new Error(`Expected a non-negative number in: $limit: ${limit}`);
    }
    if (limit === 0) {
      throw new Error("the limit must be positive");
    }
    return docs.slice(0, limit);
  }

  private execSkip(skip: number, docs: Document[]): Document[] {
    if (typeof skip !== "number" || !Number.isFinite(skip) || skip < 0) {
      throw new Error("$skip must be a non-negative integer");
    }
    if (!Number.isInteger(skip)) {
      throw new Error("$skip must be a non-negative integer");
    }
    return docs.slice(skip);
  }

  private execCount(fieldName: string, docs: Document[]): Document[] {
    if (!fieldName || typeof fieldName !== "string" || fieldName.length === 0) {
      throw new Error("$count field name must be a non-empty string");
    }
    if (fieldName.startsWith("$")) {
      throw new Error("$count field name cannot start with '$'");
    }
    if (fieldName.includes(".")) {
      throw new Error("$count field name cannot contain '.'");
    }

    if (docs.length === 0) {
      return [];
    }

    return [{ [fieldName]: docs.length }];
  }

  private execUnwind(
    unwind: string | UnwindOptions,
    docs: Document[]
  ): Document[] {
    const path = typeof unwind === "string" ? unwind : unwind.path;
    const preserveNullAndEmpty =
      typeof unwind === "object" && unwind.preserveNullAndEmptyArrays === true;
    const includeArrayIndex =
      typeof unwind === "object" ? unwind.includeArrayIndex : undefined;

    if (!path.startsWith("$")) {
      throw new Error(
        "$unwind requires a path starting with '$', found: " + path
      );
    }

    const fieldPath = path.slice(1);
    const result: Document[] = [];

    for (const doc of docs) {
      const value = getValueByPath(doc, fieldPath);

      if (value === undefined || value === null) {
        if (preserveNullAndEmpty) {
          const newDoc = cloneDocument(doc);
          if (includeArrayIndex) {
            newDoc[includeArrayIndex] = null;
          }
          result.push(newDoc);
        }
        continue;
      }

      if (!Array.isArray(value)) {
        const newDoc = cloneDocument(doc);
        if (includeArrayIndex) {
          newDoc[includeArrayIndex] = 0;
        }
        result.push(newDoc);
        continue;
      }

      if (value.length === 0) {
        if (preserveNullAndEmpty) {
          const newDoc = cloneDocument(doc);
          if (fieldPath.includes(".")) {
            setValueByPath(newDoc, fieldPath, null);
          } else {
            delete newDoc[fieldPath];
          }
          if (includeArrayIndex) {
            newDoc[includeArrayIndex] = null;
          }
          result.push(newDoc);
        }
        continue;
      }

      for (let i = 0; i < value.length; i++) {
        const newDoc = cloneDocument(doc);
        if (fieldPath.includes(".")) {
          setValueByPath(newDoc, fieldPath, value[i]);
        } else {
          newDoc[fieldPath] = value[i];
        }
        if (includeArrayIndex) {
          newDoc[includeArrayIndex] = i;
        }
        result.push(newDoc);
      }
    }

    return result;
  }

  // ==================== Phase 10 Stage Implementations ====================

  private execGroup(
    groupSpec: { _id: unknown; [key: string]: unknown },
    docs: Document[]
  ): Document[] {
    // Validate _id field is present (MongoDB requires it)
    if (!("_id" in groupSpec)) {
      throw new Error("a group specification must include an _id");
    }

    const groups = new Map<
      string,
      { _id: unknown; accumulators: Map<string, Accumulator> }
    >();

    // Get accumulator fields (all fields except _id)
    const accumulatorFields = Object.entries(groupSpec).filter(
      ([key]) => key !== "_id"
    );

    for (const doc of docs) {
      // Evaluate grouping _id
      const groupId = evaluateExpression(groupSpec._id, doc);
      const groupKey = JSON.stringify(groupId);

      if (!groups.has(groupKey)) {
        // Initialize accumulators for this group
        const accumulators = new Map<string, Accumulator>();
        for (const [field, expr] of accumulatorFields) {
          const exprObj = expr as Record<string, unknown>;
          const opKeys = Object.keys(exprObj);
          if (opKeys.length === 1 && opKeys[0].startsWith("$")) {
            accumulators.set(field, createAccumulator(opKeys[0], exprObj[opKeys[0]]));
          }
        }
        groups.set(groupKey, { _id: groupId, accumulators });
      }

      const group = groups.get(groupKey)!;

      // Apply each accumulator
      for (const [, accumulator] of group.accumulators) {
        accumulator.accumulate(doc);
      }
    }

    // Build result documents
    return Array.from(groups.values()).map((group) => {
      const result: Document = { _id: group._id };
      for (const [field, accumulator] of group.accumulators) {
        result[field] = accumulator.getResult();
      }
      return result;
    });
  }

  private async execLookup(
    lookupSpec: { from: string; localField: string; foreignField: string; as: string },
    docs: Document[]
  ): Promise<Document[]> {
    // Validate required fields
    if (!lookupSpec.from) {
      throw new Error("$lookup requires 'from' field");
    }
    if (!lookupSpec.localField) {
      throw new Error("$lookup requires 'localField' field");
    }
    if (!lookupSpec.foreignField) {
      throw new Error("$lookup requires 'foreignField' field");
    }
    if (!lookupSpec.as) {
      throw new Error("$lookup requires 'as' field");
    }

    if (!this.dbContext) {
      throw new Error("$lookup requires database context");
    }

    const foreignCollection = this.dbContext.getCollection(lookupSpec.from);
    const foreignDocs = await foreignCollection.find({}).toArray();

    return docs.map((doc) => {
      const localValue = getValueByPath(doc, lookupSpec.localField);

      // Find matching foreign documents
      const matches = foreignDocs.filter((foreignDoc) => {
        const foreignValue = getValueByPath(foreignDoc, lookupSpec.foreignField);
        return deepEquals(localValue, foreignValue);
      });

      return {
        ...doc,
        [lookupSpec.as]: matches,
      };
    });
  }

  private execAddFields(
    addFieldsSpec: Record<string, unknown>,
    docs: Document[]
  ): Document[] {
    return docs.map((doc) => {
      const result = cloneDocument(doc);

      for (const [field, expr] of Object.entries(addFieldsSpec)) {
        const value = evaluateExpression(expr, doc);
        if (field.includes(".")) {
          setValueByPath(result, field, value);
        } else {
          result[field] = value;
        }
      }

      return result;
    });
  }

  private execReplaceRoot(
    spec: { newRoot: unknown },
    docs: Document[]
  ): Document[] {
    return docs.map((doc) => {
      const newRoot = evaluateExpression(spec.newRoot, doc);

      if (newRoot === null || newRoot === undefined) {
        const typeName = newRoot === null ? "null" : "missing";
        throw new Error(
          `'newRoot' expression must evaluate to an object, but resulting value was: ${typeName === "missing" ? "MISSING" : typeName}. Type of resulting value: '${typeName}'.`
        );
      }

      if (typeof newRoot !== "object" || Array.isArray(newRoot)) {
        const typeName = getBSONTypeName(newRoot);
        throw new Error(
          `'newRoot' expression must evaluate to an object, but resulting value was of type: ${typeName}`
        );
      }

      return newRoot as Document;
    });
  }

  private async execOut(
    collectionName: string,
    docs: Document[]
  ): Promise<Document[]> {
    if (!this.dbContext) {
      throw new Error("$out requires database context");
    }

    const targetCollection = this.dbContext.getCollection(collectionName);

    // Drop existing collection and replace with results
    await targetCollection.deleteMany({});

    if (docs.length > 0) {
      await targetCollection.insertMany(docs);
    }

    // $out returns empty array (results written to collection)
    return [];
  }
}
