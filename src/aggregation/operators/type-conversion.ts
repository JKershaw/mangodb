/**
 * Type conversion expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";
import { getValueByPath } from "../../utils.ts";
import { getBSONTypeName } from "../helpers.ts";

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

  if (typeof value === "number") {
    return Math.trunc(value);
  }

  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "Infinity" || trimmed === "-Infinity" || trimmed === "NaN") {
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

  if (typeof value === "number") {
    return value;
  }

  if (typeof value === "boolean") {
    return value ? 1.0 : 0.0;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "Infinity") {
      return Infinity;
    }
    if (trimmed === "-Infinity") {
      return -Infinity;
    }
    if (trimmed === "NaN") {
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

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    return true;
  }

  if (value instanceof Date) {
    return true;
  }

  if (Array.isArray(value) || typeof value === "object") {
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

  if (typeof value === "number") {
    return new Date(value);
  }

  if (typeof value === "string") {
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
  if (typeof args === "string" && args.startsWith("$") && !args.startsWith("$$")) {
    const fieldPath = args.slice(1);
    const value = getValueByPath(doc, fieldPath);
    if (value === undefined) {
      return "missing";
    }
  }

  const value = evaluate(args, doc, vars);

  if (value === undefined) {
    return "missing";
  }

  if (value === null) {
    return "null";
  }

  if (typeof value === "boolean") {
    return "bool";
  }

  if (typeof value === "number") {
    return "double";
  }

  if (typeof value === "string") {
    return "string";
  }

  if (value instanceof Date) {
    return "date";
  }

  if (Array.isArray(value)) {
    return "array";
  }

  if (value && typeof (value as { toHexString?: unknown }).toHexString === "function") {
    return "objectId";
  }

  if (typeof value === "object") {
    return "object";
  }

  return "unknown";
}
