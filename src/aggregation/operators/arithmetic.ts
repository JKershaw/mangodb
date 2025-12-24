/**
 * Arithmetic expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";
import { getBSONTypeName } from "../helpers.ts";

export function evalAdd(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const values = args.map((a) => evaluate(a, doc, vars));

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

export function evalSubtract(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const [arg1, arg2] = args.map((a) => evaluate(a, doc, vars));

  if (arg1 === null || arg1 === undefined || arg2 === null || arg2 === undefined) {
    return null;
  }

  if (typeof arg1 !== "number" || typeof arg2 !== "number") {
    throw new Error("$subtract only supports numeric types");
  }

  return arg1 - arg2;
}

export function evalMultiply(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const values = args.map((a) => evaluate(a, doc, vars));

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

export function evalDivide(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const [dividend, divisor] = args.map((a) => evaluate(a, doc, vars));

  if (dividend === null || dividend === undefined || divisor === null || divisor === undefined) {
    return null;
  }

  if (typeof dividend !== "number" || typeof divisor !== "number") {
    throw new Error("$divide only supports numeric types");
  }

  if (divisor === 0) {
    return null;
  }

  return dividend / divisor;
}

export function evalAbs(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "number") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$abs only supports numeric types, not ${typeName}`);
  }

  return Math.abs(value);
}

export function evalCeil(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "number") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$ceil only supports numeric types, not ${typeName}`);
  }

  return Math.ceil(value);
}

export function evalFloor(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "number") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$floor only supports numeric types, not ${typeName}`);
  }

  return Math.floor(value);
}

/**
 * Banker's rounding (round half to even).
 */
function bankersRound(value: number, decimalPlaces: number): number {
  const multiplier = Math.pow(10, decimalPlaces);
  const shifted = value * multiplier;
  const floor = Math.floor(shifted);
  const decimal = shifted - floor;

  if (Math.abs(decimal - 0.5) < 1e-10) {
    if (floor % 2 === 0) {
      return floor / multiplier;
    } else {
      return (floor + 1) / multiplier;
    }
  }

  return Math.round(shifted) / multiplier;
}

export function evalRound(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  let numberExpr: unknown;
  let placeExpr: unknown = 0;

  if (Array.isArray(args)) {
    [numberExpr, placeExpr = 0] = args;
  } else {
    numberExpr = args;
  }

  const value = evaluate(numberExpr, doc);
  const place = evaluate(placeExpr, doc) as number;

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "number") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$round only supports numeric types, not ${typeName}`);
  }

  return bankersRound(value, place);
}

export function evalMod(
  args: unknown[],
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const [dividendExpr, divisorExpr] = args;
  const dividend = evaluate(dividendExpr, doc);
  const divisor = evaluate(divisorExpr, doc);

  if (dividend === null || dividend === undefined || divisor === null || divisor === undefined) {
    return null;
  }

  if (typeof dividend !== "number" || typeof divisor !== "number") {
    const dividendType = getBSONTypeName(dividend);
    const divisorType = getBSONTypeName(divisor);
    throw new Error(`$mod only supports numeric types, not ${dividendType} and ${divisorType}`);
  }

  if (divisor === 0) {
    throw new Error("can't $mod by zero");
  }

  return dividend % divisor;
}

/**
 * $rand - Returns a random float between 0 (inclusive) and 1 (exclusive).
 * Syntax: { $rand: {} }
 *
 * Each invocation generates a new random value.
 */
export function evalRand(
  _args: unknown,
  _doc: Document,
  _vars: VariableContext | undefined,
  _evaluate: EvaluateExpressionFn
): number {
  // $rand takes no arguments (empty object) and returns a random float in [0, 1)
  return Math.random();
}
