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
