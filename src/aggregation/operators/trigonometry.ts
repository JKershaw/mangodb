/**
 * Trigonometry expression operators.
 */
import type { Document } from '../../types.ts';
import type { VariableContext, EvaluateExpressionFn } from '../types.ts';
import { getBSONTypeName } from '../helpers.ts';

/**
 * Helper to validate a single numeric argument.
 * Returns the evaluated value or null if null/undefined.
 * Throws if not a number.
 */
function validateNumericArg(
  args: unknown,
  doc: Document,
  evaluate: EvaluateExpressionFn,
  operatorName: string
): number | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== 'number') {
    const typeName = getBSONTypeName(value);
    throw new Error(`${operatorName} only supports numeric types, not ${typeName}`);
  }

  return value;
}

/**
 * $sin - Returns the sine of a value in radians.
 */
export function evalSin(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$sin');

  if (value === null) {
    return null;
  }

  // NaN returns NaN
  if (Number.isNaN(value)) {
    return NaN;
  }

  // Infinity throws error in MongoDB
  if (!Number.isFinite(value)) {
    throw new Error("cannot apply $sin to -inf, value must be in (-inf,inf)");
  }

  return Math.sin(value);
}

/**
 * $cos - Returns the cosine of a value in radians.
 */
export function evalCos(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$cos');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  if (!Number.isFinite(value)) {
    throw new Error("cannot apply $cos to -inf, value must be in (-inf,inf)");
  }

  return Math.cos(value);
}

/**
 * $tan - Returns the tangent of a value in radians.
 */
export function evalTan(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$tan');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  if (!Number.isFinite(value)) {
    throw new Error("cannot apply $tan to -inf, value must be in (-inf,inf)");
  }

  return Math.tan(value);
}

/**
 * $asin - Returns the arc sine (inverse sine) in radians.
 * Input must be in range [-1, 1].
 */
export function evalAsin(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$asin');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  // MongoDB throws for values outside [-1, 1]
  if (value < -1 || value > 1) {
    throw new Error(`cannot apply $asin to ${value}, value must be in [-1,1]`);
  }

  return Math.asin(value);
}

/**
 * $acos - Returns the arc cosine (inverse cosine) in radians.
 * Input must be in range [-1, 1].
 */
export function evalAcos(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$acos');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  // MongoDB throws for values outside [-1, 1]
  if (value < -1 || value > 1) {
    throw new Error(`cannot apply $acos to ${value}, value must be in [-1,1]`);
  }

  return Math.acos(value);
}

/**
 * $atan - Returns the arc tangent (inverse tangent) in radians.
 */
export function evalAtan(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$atan');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  return Math.atan(value);
}

/**
 * $atan2 - Returns the arc tangent of y/x in radians.
 * Takes two arguments: [y, x]
 */
export function evalAtan2(
  args: unknown[],
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const [yExpr, xExpr] = args;
  const y = evaluate(yExpr, doc);
  const x = evaluate(xExpr, doc);

  if (y === null || y === undefined || x === null || x === undefined) {
    return null;
  }

  if (typeof y !== 'number') {
    const typeName = getBSONTypeName(y);
    throw new Error(`$atan2 only supports numeric types, not ${typeName}`);
  }

  if (typeof x !== 'number') {
    const typeName = getBSONTypeName(x);
    throw new Error(`$atan2 only supports numeric types, not ${typeName}`);
  }

  if (Number.isNaN(y) || Number.isNaN(x)) {
    return NaN;
  }

  return Math.atan2(y, x);
}

/**
 * $sinh - Returns the hyperbolic sine.
 */
export function evalSinh(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$sinh');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  return Math.sinh(value);
}

/**
 * $cosh - Returns the hyperbolic cosine.
 */
export function evalCosh(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$cosh');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  return Math.cosh(value);
}

/**
 * $tanh - Returns the hyperbolic tangent.
 */
export function evalTanh(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$tanh');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  return Math.tanh(value);
}

/**
 * $asinh - Returns the inverse hyperbolic sine.
 */
export function evalAsinh(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$asinh');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  return Math.asinh(value);
}

/**
 * $acosh - Returns the inverse hyperbolic cosine.
 * Input must be >= 1.
 */
export function evalAcosh(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$acosh');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  // MongoDB throws for values < 1
  if (value < 1) {
    throw new Error(`cannot apply $acosh to ${value}, value must be in [1,inf]`);
  }

  return Math.acosh(value);
}

/**
 * $atanh - Returns the inverse hyperbolic tangent.
 * Input must be in range (-1, 1).
 */
export function evalAtanh(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$atanh');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  // Math.atanh returns NaN for |value| > 1, Infinity for |value| === 1
  return Math.atanh(value);
}

/**
 * $degreesToRadians - Converts degrees to radians.
 */
export function evalDegreesToRadians(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$degreesToRadians');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  if (!Number.isFinite(value)) {
    // Return Infinity with same sign
    return value;
  }

  return value * (Math.PI / 180);
}

/**
 * $radiansToDegrees - Converts radians to degrees.
 */
export function evalRadiansToDegrees(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = validateNumericArg(args, doc, evaluate, '$radiansToDegrees');

  if (value === null) {
    return null;
  }

  if (Number.isNaN(value)) {
    return NaN;
  }

  if (!Number.isFinite(value)) {
    // Return Infinity with same sign
    return value;
  }

  return value * (180 / Math.PI);
}
