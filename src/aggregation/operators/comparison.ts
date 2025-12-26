/**
 * Comparison expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";
import { compareValues } from "../../document-utils.ts";

export function evalGt(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) > 0;
}

export function evalGte(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) >= 0;
}

export function evalLt(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) < 0;
}

export function evalLte(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) <= 0;
}

export function evalEq(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) === 0;
}

export function evalNe(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) !== 0;
}

/**
 * $cmp - Compares two values and returns -1, 0, or 1.
 * Returns -1 if first < second, 0 if equal, 1 if first > second.
 */
export function evalCmp(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  const cmp = compareValues(left, right);
  if (cmp < 0) return -1;
  if (cmp > 0) return 1;
  return 0;
}

/**
 * Helper to determine truthiness for boolean operators.
 * In MongoDB, all values are truthy except: false, null, undefined, 0.
 */
function isTruthy(value: unknown): boolean {
  if (value === false || value === null || value === undefined || value === 0) {
    return false;
  }
  return true;
}

/**
 * $and - Returns true if all expressions evaluate to true.
 */
export function evalAnd(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  if (!Array.isArray(args)) {
    throw new Error("$and requires an array argument");
  }
  for (const expr of args) {
    const val = evaluate(expr, doc, vars);
    if (!isTruthy(val)) {
      return false;
    }
  }
  return true;
}

/**
 * $or - Returns true if any expression evaluates to true.
 */
export function evalOr(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  if (!Array.isArray(args)) {
    throw new Error("$or requires an array argument");
  }
  for (const expr of args) {
    const val = evaluate(expr, doc, vars);
    if (isTruthy(val)) {
      return true;
    }
  }
  return false;
}

/**
 * $not - Returns the boolean opposite of its argument expression.
 */
export function evalNot(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  // $not takes a single expression, but might come in as array with one element
  const expr = Array.isArray(args) ? args[0] : args;
  const val = evaluate(expr, doc, vars);
  return !isTruthy(val);
}
