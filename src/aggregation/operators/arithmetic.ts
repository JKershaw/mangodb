/**
 * Arithmetic expression operators.
 */
import type { Document } from '../../types.ts';
import type { VariableContext, EvaluateExpressionFn } from '../types.ts';
import { getBSONTypeName } from '../helpers.ts';

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
    if (typeof v !== 'number') {
      throw new Error('$add only supports numeric types');
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

  if (typeof arg1 !== 'number' || typeof arg2 !== 'number') {
    throw new Error('$subtract only supports numeric types');
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
    if (typeof v !== 'number') {
      throw new Error('$multiply only supports numeric types');
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

  if (typeof dividend !== 'number' || typeof divisor !== 'number') {
    throw new Error('$divide only supports numeric types');
  }

  // MongoDB returns Infinity for division by zero
  if (divisor === 0) {
    return dividend >= 0 ? Infinity : -Infinity;
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

  if (typeof value !== 'number') {
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

  if (typeof value !== 'number') {
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

  if (typeof value !== 'number') {
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

  if (typeof value !== 'number') {
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

  if (typeof dividend !== 'number' || typeof divisor !== 'number') {
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

/**
 * $exp - Raises Euler's number (e) to the specified exponent.
 */
export function evalExp(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== 'number') {
    const typeName = getBSONTypeName(value);
    throw new Error(`$exp only supports numeric types, not ${typeName}`);
  }

  return Math.exp(value);
}

/**
 * $ln - Calculates the natural logarithm (ln) of a number.
 */
export function evalLn(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== 'number') {
    const typeName = getBSONTypeName(value);
    throw new Error(`$ln only supports numeric types, not ${typeName}`);
  }

  if (value <= 0) {
    throw new Error("$ln's argument must be a positive number");
  }

  return Math.log(value);
}

/**
 * $log - Calculates the log of a number in the specified base.
 */
export function evalLog(
  args: unknown[],
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const [numberExpr, baseExpr] = args;
  const number = evaluate(numberExpr, doc);
  const base = evaluate(baseExpr, doc);

  if (number === null || number === undefined || base === null || base === undefined) {
    return null;
  }

  if (typeof number !== 'number' || typeof base !== 'number') {
    throw new Error("$log's arguments must be numbers");
  }

  if (number <= 0) {
    throw new Error("$log's argument must be a positive number");
  }

  if (base <= 0 || base === 1) {
    throw new Error("$log's base must be a positive number not equal to 1");
  }

  return Math.log(number) / Math.log(base);
}

/**
 * $log10 - Calculates the base-10 logarithm of a number.
 */
export function evalLog10(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== 'number') {
    const typeName = getBSONTypeName(value);
    throw new Error(`$log10 only supports numeric types, not ${typeName}`);
  }

  if (value <= 0) {
    throw new Error("$log10's argument must be a positive number");
  }

  return Math.log10(value);
}

/**
 * $pow - Raises a number to the specified exponent.
 */
export function evalPow(
  args: unknown[],
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const [baseExpr, exponentExpr] = args;
  const base = evaluate(baseExpr, doc);
  const exponent = evaluate(exponentExpr, doc);

  if (base === null || base === undefined || exponent === null || exponent === undefined) {
    return null;
  }

  if (typeof base !== 'number' || typeof exponent !== 'number') {
    throw new Error("$pow's arguments must be numbers");
  }

  return Math.pow(base, exponent);
}

/**
 * $sqrt - Calculates the square root of a number.
 */
export function evalSqrt(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== 'number') {
    const typeName = getBSONTypeName(value);
    throw new Error(`$sqrt only supports numeric types, not ${typeName}`);
  }

  if (value < 0) {
    throw new Error("$sqrt's argument must be greater than or equal to 0");
  }

  return Math.sqrt(value);
}

/**
 * $trunc - Truncates a number to a whole integer or to a specified decimal place.
 */
export function evalTrunc(
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

  if (typeof value !== 'number') {
    const typeName = getBSONTypeName(value);
    throw new Error(`$trunc only supports numeric types, not ${typeName}`);
  }

  if (place === 0) {
    return Math.trunc(value);
  }

  // Truncate to specified decimal places
  const multiplier = Math.pow(10, place);
  return Math.trunc(value * multiplier) / multiplier;
}
