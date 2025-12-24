/**
 * Date expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";
import { getBSONTypeName } from "../helpers.ts";

/**
 * Helper to extract and validate a date value for date operators.
 */
function extractDateValue(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  operatorName: string,
  evaluate: EvaluateExpressionFn
): Date | null {
  const value = evaluate(args, doc, vars);

  if (value === null || value === undefined) {
    return null;
  }

  if (!(value instanceof Date)) {
    const typeName = getBSONTypeName(value);
    throw new Error(`${operatorName} requires a date, found: ${typeName}`);
  }

  if (isNaN(value.getTime())) {
    throw new Error(`${operatorName} requires a valid date`);
  }

  return value;
}

export function evalYear(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, "$year", evaluate);
  if (value === null) return null;
  return value.getUTCFullYear();
}

export function evalMonth(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, "$month", evaluate);
  if (value === null) return null;
  return value.getUTCMonth() + 1;
}

export function evalDayOfMonth(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, "$dayOfMonth", evaluate);
  if (value === null) return null;
  return value.getUTCDate();
}

export function evalHour(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, "$hour", evaluate);
  if (value === null) return null;
  return value.getUTCHours();
}

export function evalMinute(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, "$minute", evaluate);
  if (value === null) return null;
  return value.getUTCMinutes();
}

export function evalSecond(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, "$second", evaluate);
  if (value === null) return null;
  return value.getUTCSeconds();
}

export function evalDayOfWeek(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, "$dayOfWeek", evaluate);
  if (value === null) return null;
  return value.getUTCDay() + 1;
}

/**
 * Format a date according to MongoDB's format specifiers.
 */
function formatDate(date: Date, format: string): string {
  const pad2 = (n: number) => n.toString().padStart(2, "0");
  const pad3 = (n: number) => n.toString().padStart(3, "0");
  const pad4 = (n: number) => n.toString().padStart(4, "0");

  const startOfYear = Date.UTC(date.getUTCFullYear(), 0, 1);
  const dayOfYear = Math.floor((date.getTime() - startOfYear) / (24 * 60 * 60 * 1000)) + 1;

  const startOfYearDate = new Date(startOfYear);
  const startDayOfWeek = startOfYearDate.getUTCDay();
  const daysSinceStart = dayOfYear - 1;
  const weekOfYear = Math.floor((daysSinceStart + startDayOfWeek) / 7);

  const jan4 = new Date(Date.UTC(date.getUTCFullYear(), 0, 4));
  const jan4DayOfWeek = jan4.getUTCDay() || 7;
  const startOfISOWeek1 = new Date(jan4.getTime() - (jan4DayOfWeek - 1) * 24 * 60 * 60 * 1000);
  const daysSinceISOWeek1 = Math.floor((date.getTime() - startOfISOWeek1.getTime()) / (24 * 60 * 60 * 1000));
  let isoWeek = Math.floor(daysSinceISOWeek1 / 7) + 1;
  if (isoWeek < 1) isoWeek = 52;
  if (isoWeek > 52) isoWeek = 1;

  const isoDayOfWeek = date.getUTCDay() === 0 ? 7 : date.getUTCDay();

  return format
    .replace(/%Y/g, pad4(date.getUTCFullYear()))
    .replace(/%m/g, pad2(date.getUTCMonth() + 1))
    .replace(/%d/g, pad2(date.getUTCDate()))
    .replace(/%H/g, pad2(date.getUTCHours()))
    .replace(/%M/g, pad2(date.getUTCMinutes()))
    .replace(/%S/g, pad2(date.getUTCSeconds()))
    .replace(/%L/g, pad3(date.getUTCMilliseconds()))
    .replace(/%j/g, pad3(dayOfYear))
    .replace(/%w/g, (date.getUTCDay() + 1).toString())
    .replace(/%u/g, isoDayOfWeek.toString())
    .replace(/%U/g, pad2(weekOfYear))
    .replace(/%V/g, pad2(isoWeek));
}

export function evalDateToString(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string | null {
  const spec = args as { date: unknown; format?: string; onNull?: unknown };
  const dateValue = evaluate(spec.date, doc, vars);

  if (dateValue === null || dateValue === undefined) {
    if (spec.onNull !== undefined) {
      const onNullValue = evaluate(spec.onNull, doc, vars);
      return onNullValue as string | null;
    }
    return null;
  }

  if (!(dateValue instanceof Date)) {
    const typeName = getBSONTypeName(dateValue);
    throw new Error(`can't convert from BSON type ${typeName} to Date`);
  }

  if (isNaN(dateValue.getTime())) {
    throw new Error(`$dateToString requires a valid date`);
  }

  const format = spec.format || "%Y-%m-%dT%H:%M:%S.%LZ";
  return formatDate(dateValue, format);
}
