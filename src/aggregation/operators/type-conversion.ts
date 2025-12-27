/**
 * Type conversion expression operators.
 */
import { ObjectId } from 'bson';
import type { Document } from '../../types.ts';
import type { VariableContext, EvaluateExpressionFn } from '../types.ts';
import { getValueByPath } from '../../utils.ts';
import { getBSONTypeName } from '../helpers.ts';

export function evalToInt(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc, vars);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'number') {
    return Math.trunc(value);
  }

  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === 'Infinity' || trimmed === '-Infinity' || trimmed === 'NaN') {
      throw new Error(`Failed to parse number '${value}' in $convert`);
    }
    const parsed = parseInt(value, 10);
    if (isNaN(parsed) || !Number.isFinite(parsed)) {
      throw new Error(`Failed to parse number '${value}' in $convert`);
    }
    return parsed;
  }

  if (value instanceof Date) {
    return Math.trunc(value.getTime());
  }

  const typeName = getBSONTypeName(value);
  throw new Error(`Unsupported conversion from ${typeName} to int`);
}

export function evalToDouble(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc, vars);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'number') {
    return value;
  }

  if (typeof value === 'boolean') {
    return value ? 1.0 : 0.0;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === 'Infinity') {
      return Infinity;
    }
    if (trimmed === '-Infinity') {
      return -Infinity;
    }
    if (trimmed === 'NaN') {
      throw new Error(`Failed to parse number '${value}' in $convert`);
    }
    const parsed = parseFloat(value);
    if (isNaN(parsed)) {
      throw new Error(`Failed to parse number '${value}' in $convert`);
    }
    return parsed;
  }

  if (value instanceof Date) {
    return value.getTime();
  }

  const typeName = getBSONTypeName(value);
  throw new Error(`Unsupported conversion from ${typeName} to double`);
}

export function evalToBool(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean | null {
  const value = evaluate(args, doc, vars);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'number') {
    return value !== 0;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'string') {
    return true;
  }

  if (value instanceof Date) {
    return true;
  }

  if (Array.isArray(value) || typeof value === 'object') {
    return true;
  }

  return true;
}

export function evalToDate(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Date | null {
  const value = evaluate(args, doc, vars);

  if (value === null || value === undefined) {
    return null;
  }

  if (value instanceof Date) {
    return value;
  }

  if (typeof value === 'number') {
    return new Date(value);
  }

  if (typeof value === 'string') {
    const parsed = new Date(value);
    if (isNaN(parsed.getTime())) {
      throw new Error(`Error parsing date string '${value}'`);
    }
    return parsed;
  }

  const typeName = getBSONTypeName(value);
  throw new Error(`can't convert from BSON type ${typeName} to Date`);
}

export function evalType(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string {
  // Special handling for $type - detect "missing" before evaluating
  if (typeof args === 'string' && args.startsWith('$') && !args.startsWith('$$')) {
    const fieldPath = args.slice(1);
    const value = getValueByPath(doc, fieldPath);
    if (value === undefined) {
      return 'missing';
    }
  }

  const value = evaluate(args, doc, vars);

  if (value === undefined) {
    return 'missing';
  }

  if (value === null) {
    return 'null';
  }

  if (typeof value === 'boolean') {
    return 'bool';
  }

  if (typeof value === 'number') {
    return 'double';
  }

  if (typeof value === 'string') {
    return 'string';
  }

  if (value instanceof Date) {
    return 'date';
  }

  if (Array.isArray(value)) {
    return 'array';
  }

  if (value && typeof (value as { toHexString?: unknown }).toHexString === 'function') {
    return 'objectId';
  }

  if (typeof value === 'object') {
    return 'object';
  }

  return 'unknown';
}

/**
 * $isNumber - Returns true if the expression resolves to a number (int, long, double, decimal).
 */
export function evalIsNumber(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const value = evaluate(args, doc, vars);
  return typeof value === 'number' && !isNaN(value);
}

/**
 * $toLong - Converts a value to a long (64-bit integer).
 * In JavaScript, we use Number and truncate to integer.
 */
export function evalToLong(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = evaluate(args, doc, vars);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'number') {
    return Math.trunc(value);
  }

  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === 'Infinity' || trimmed === '-Infinity' || trimmed === 'NaN') {
      throw new Error(`Failed to parse number '${value}' in $convert`);
    }
    const parsed = parseInt(value, 10);
    if (isNaN(parsed) || !Number.isFinite(parsed)) {
      throw new Error(`Failed to parse number '${value}' in $convert`);
    }
    return parsed;
  }

  if (value instanceof Date) {
    return value.getTime();
  }

  const typeName = getBSONTypeName(value);
  throw new Error(`Unsupported conversion from ${typeName} to long`);
}

/**
 * $toDecimal - Converts a value to a decimal (in JS, same as double).
 */
export function evalToDecimal(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  // In JavaScript, we don't have a separate decimal type, so this is equivalent to toDouble
  return evalToDouble(args, doc, vars, evaluate);
}

/**
 * $toObjectId - Converts a string to an ObjectId.
 */
export function evalToObjectId(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): ObjectId | null {
  const value = evaluate(args, doc, vars);

  if (value === null || value === undefined) {
    return null;
  }

  // Already an ObjectId
  if (value && typeof (value as { toHexString?: unknown }).toHexString === 'function') {
    return value as ObjectId;
  }

  if (typeof value === 'string') {
    // Validate 24-character hex string
    if (!/^[0-9a-fA-F]{24}$/.test(value)) {
      throw new Error(
        `Invalid string length for parsing to ObjectId, string length: ${value.length} but must be 24`
      );
    }
    return new ObjectId(value);
  }

  const typeName = getBSONTypeName(value);
  throw new Error(`Unsupported conversion from ${typeName} to objectId`);
}

/**
 * $convert - Generic type conversion operator.
 * Supports: string, bool, int, long, double, decimal, date, objectId
 */
export function evalConvert(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  if (typeof args !== 'object' || args === null) {
    throw new Error('$convert requires an object as argument');
  }

  const spec = args as { input: unknown; to: unknown; onError?: unknown; onNull?: unknown };

  if (spec.input === undefined) {
    throw new Error("$convert requires 'input' field");
  }

  if (spec.to === undefined) {
    throw new Error("$convert requires 'to' field");
  }

  const toType = evaluate(spec.to, doc, vars);
  let targetType: string;

  if (typeof toType === 'string') {
    targetType = toType.toLowerCase();
  } else if (typeof toType === 'number') {
    // MongoDB type codes
    const typeCodes: Record<number, string> = {
      1: 'double',
      2: 'string',
      7: 'objectId',
      8: 'bool',
      9: 'date',
      16: 'int',
      18: 'long',
      19: 'decimal',
    };
    targetType = typeCodes[toType] || 'unknown';
  } else {
    throw new Error(`Invalid 'to' type in $convert`);
  }

  try {
    const inputValue = evaluate(spec.input, doc, vars);

    // Handle null/undefined
    if (inputValue === null || inputValue === undefined) {
      if (spec.onNull !== undefined) {
        return evaluate(spec.onNull, doc, vars);
      }
      return null;
    }

    switch (targetType) {
      case 'string':
        return evalToString(spec.input, doc, vars, evaluate);
      case 'bool':
      case 'boolean':
        return evalToBool(spec.input, doc, vars, evaluate);
      case 'int':
        return evalToInt(spec.input, doc, vars, evaluate);
      case 'long':
        return evalToLong(spec.input, doc, vars, evaluate);
      case 'double':
        return evalToDouble(spec.input, doc, vars, evaluate);
      case 'decimal':
        return evalToDecimal(spec.input, doc, vars, evaluate);
      case 'date':
        return evalToDate(spec.input, doc, vars, evaluate);
      case 'objectid':
        return evalToObjectId(spec.input, doc, vars, evaluate);
      default:
        throw new Error(`Unsupported target type '${targetType}' in $convert`);
    }
  } catch (error) {
    if (spec.onError !== undefined) {
      return evaluate(spec.onError, doc, vars);
    }
    throw error;
  }
}

// Import evalToString locally to avoid circular dependency issues
function evalToString(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string | null {
  const value = evaluate(args, doc, vars);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'string') {
    return value;
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value && typeof (value as { toHexString?: () => string }).toHexString === 'function') {
    return (value as { toHexString: () => string }).toHexString();
  }

  return JSON.stringify(value);
}
