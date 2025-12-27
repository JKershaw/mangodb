/**
 * Date expression operators.
 */
import type { Document } from '../../types.ts';
import type { VariableContext, EvaluateExpressionFn } from '../types.ts';
import { getBSONTypeName } from '../helpers.ts';

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
  const value = extractDateValue(args, doc, vars, '$year', evaluate);
  if (value === null) return null;
  return value.getUTCFullYear();
}

export function evalMonth(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$month', evaluate);
  if (value === null) return null;
  return value.getUTCMonth() + 1;
}

export function evalDayOfMonth(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$dayOfMonth', evaluate);
  if (value === null) return null;
  return value.getUTCDate();
}

export function evalHour(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$hour', evaluate);
  if (value === null) return null;
  return value.getUTCHours();
}

export function evalMinute(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$minute', evaluate);
  if (value === null) return null;
  return value.getUTCMinutes();
}

export function evalSecond(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$second', evaluate);
  if (value === null) return null;
  return value.getUTCSeconds();
}

export function evalDayOfWeek(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$dayOfWeek', evaluate);
  if (value === null) return null;
  return value.getUTCDay() + 1;
}

/**
 * Format a date according to MongoDB's format specifiers.
 */
function formatDate(date: Date, format: string): string {
  const pad2 = (n: number) => n.toString().padStart(2, '0');
  const pad3 = (n: number) => n.toString().padStart(3, '0');
  const pad4 = (n: number) => n.toString().padStart(4, '0');

  const startOfYear = Date.UTC(date.getUTCFullYear(), 0, 1);
  const dayOfYear = Math.floor((date.getTime() - startOfYear) / (24 * 60 * 60 * 1000)) + 1;

  const startOfYearDate = new Date(startOfYear);
  const startDayOfWeek = startOfYearDate.getUTCDay();
  const daysSinceStart = dayOfYear - 1;
  const weekOfYear = Math.floor((daysSinceStart + startDayOfWeek) / 7);

  const jan4 = new Date(Date.UTC(date.getUTCFullYear(), 0, 4));
  const jan4DayOfWeek = jan4.getUTCDay() || 7;
  const startOfISOWeek1 = new Date(jan4.getTime() - (jan4DayOfWeek - 1) * 24 * 60 * 60 * 1000);
  const daysSinceISOWeek1 = Math.floor(
    (date.getTime() - startOfISOWeek1.getTime()) / (24 * 60 * 60 * 1000)
  );
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

  const format = spec.format || '%Y-%m-%dT%H:%M:%S.%LZ';
  return formatDate(dateValue, format);
}

/**
 * $millisecond - Returns the millisecond portion of a date.
 */
export function evalMillisecond(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$millisecond', evaluate);
  if (value === null) return null;
  return value.getUTCMilliseconds();
}

/**
 * $dayOfYear - Returns the day of the year for a date (1-366).
 */
export function evalDayOfYear(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$dayOfYear', evaluate);
  if (value === null) return null;
  const startOfYear = new Date(Date.UTC(value.getUTCFullYear(), 0, 1));
  return Math.floor((value.getTime() - startOfYear.getTime()) / (24 * 60 * 60 * 1000)) + 1;
}

/**
 * $week - Returns the week number for a date (0-53).
 */
export function evalWeek(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$week', evaluate);
  if (value === null) return null;

  const startOfYear = new Date(Date.UTC(value.getUTCFullYear(), 0, 1));
  const startDayOfWeek = startOfYear.getUTCDay();
  const dayOfYear =
    Math.floor((value.getTime() - startOfYear.getTime()) / (24 * 60 * 60 * 1000)) + 1;
  return Math.floor((dayOfYear - 1 + startDayOfWeek) / 7);
}

/**
 * $isoWeek - Returns the ISO week number for a date (1-53).
 */
export function evalIsoWeek(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$isoWeek', evaluate);
  if (value === null) return null;

  const jan4 = new Date(Date.UTC(value.getUTCFullYear(), 0, 4));
  const jan4DayOfWeek = jan4.getUTCDay() || 7;
  const startOfISOWeek1 = new Date(jan4.getTime() - (jan4DayOfWeek - 1) * 24 * 60 * 60 * 1000);
  const daysSinceISOWeek1 = Math.floor(
    (value.getTime() - startOfISOWeek1.getTime()) / (24 * 60 * 60 * 1000)
  );
  let isoWeek = Math.floor(daysSinceISOWeek1 / 7) + 1;

  if (isoWeek < 1) isoWeek = 52;
  if (isoWeek > 53) isoWeek = 1;

  return isoWeek;
}

/**
 * $isoWeekYear - Returns the ISO week year for a date.
 */
export function evalIsoWeekYear(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$isoWeekYear', evaluate);
  if (value === null) return null;

  const jan4 = new Date(Date.UTC(value.getUTCFullYear(), 0, 4));
  const jan4DayOfWeek = jan4.getUTCDay() || 7;
  const startOfISOWeek1 = new Date(jan4.getTime() - (jan4DayOfWeek - 1) * 24 * 60 * 60 * 1000);

  if (value.getTime() < startOfISOWeek1.getTime()) {
    return value.getUTCFullYear() - 1;
  }

  // Check if we're in the last week of the year
  const nextJan4 = new Date(Date.UTC(value.getUTCFullYear() + 1, 0, 4));
  const nextJan4DayOfWeek = nextJan4.getUTCDay() || 7;
  const startOfNextISOWeek1 = new Date(
    nextJan4.getTime() - (nextJan4DayOfWeek - 1) * 24 * 60 * 60 * 1000
  );

  if (value.getTime() >= startOfNextISOWeek1.getTime()) {
    return value.getUTCFullYear() + 1;
  }

  return value.getUTCFullYear();
}

/**
 * $isoDayOfWeek - Returns the ISO day of the week (1-7, Monday = 1).
 */
export function evalIsoDayOfWeek(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const value = extractDateValue(args, doc, vars, '$isoDayOfWeek', evaluate);
  if (value === null) return null;
  const day = value.getUTCDay();
  return day === 0 ? 7 : day;
}

/**
 * Helper to get time unit in milliseconds.
 */
function _getTimeUnitMs(unit: string): number {
  switch (unit) {
    case 'year':
      return 365 * 24 * 60 * 60 * 1000; // Approximate
    case 'quarter':
      return 91 * 24 * 60 * 60 * 1000; // Approximate
    case 'month':
      return 30 * 24 * 60 * 60 * 1000; // Approximate
    case 'week':
      return 7 * 24 * 60 * 60 * 1000;
    case 'day':
      return 24 * 60 * 60 * 1000;
    case 'hour':
      return 60 * 60 * 1000;
    case 'minute':
      return 60 * 1000;
    case 'second':
      return 1000;
    case 'millisecond':
      return 1;
    default:
      throw new Error(`Invalid time unit: ${unit}`);
  }
}

/**
 * $dateAdd - Adds a specified amount to a date.
 */
export function evalDateAdd(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Date | null {
  const spec = args as { startDate: unknown; unit: string; amount: unknown };
  const startDate = evaluate(spec.startDate, doc, vars);
  const amount = evaluate(spec.amount, doc, vars) as number;

  if (startDate === null || startDate === undefined) {
    return null;
  }

  if (!(startDate instanceof Date)) {
    const typeName = getBSONTypeName(startDate);
    throw new Error(`$dateAdd requires a date, found: ${typeName}`);
  }

  const unit = spec.unit.toLowerCase();
  const date = new Date(startDate.getTime());

  switch (unit) {
    case 'year':
      date.setUTCFullYear(date.getUTCFullYear() + amount);
      break;
    case 'quarter':
      date.setUTCMonth(date.getUTCMonth() + amount * 3);
      break;
    case 'month':
      date.setUTCMonth(date.getUTCMonth() + amount);
      break;
    case 'week':
      date.setUTCDate(date.getUTCDate() + amount * 7);
      break;
    case 'day':
      date.setUTCDate(date.getUTCDate() + amount);
      break;
    case 'hour':
      date.setUTCHours(date.getUTCHours() + amount);
      break;
    case 'minute':
      date.setUTCMinutes(date.getUTCMinutes() + amount);
      break;
    case 'second':
      date.setUTCSeconds(date.getUTCSeconds() + amount);
      break;
    case 'millisecond':
      date.setUTCMilliseconds(date.getUTCMilliseconds() + amount);
      break;
    default:
      throw new Error(`Invalid time unit: ${unit}`);
  }

  return date;
}

/**
 * $dateSubtract - Subtracts a specified amount from a date.
 */
export function evalDateSubtract(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Date | null {
  const spec = args as { startDate: unknown; unit: string; amount: unknown };
  const negatedSpec = {
    startDate: spec.startDate,
    unit: spec.unit,
    amount: { $multiply: [spec.amount, -1] },
  };
  return evalDateAdd(negatedSpec, doc, vars, evaluate);
}

/**
 * $dateDiff - Returns the difference between two dates.
 */
export function evalDateDiff(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const spec = args as { startDate: unknown; endDate: unknown; unit: string };
  const startDate = evaluate(spec.startDate, doc, vars);
  const endDate = evaluate(spec.endDate, doc, vars);

  if (startDate === null || endDate === null) {
    return null;
  }

  if (!(startDate instanceof Date) || !(endDate instanceof Date)) {
    throw new Error('$dateDiff requires date arguments');
  }

  const diffMs = endDate.getTime() - startDate.getTime();
  const unit = spec.unit.toLowerCase();

  switch (unit) {
    case 'year':
      return endDate.getUTCFullYear() - startDate.getUTCFullYear();
    case 'quarter': {
      const startQ = startDate.getUTCFullYear() * 4 + Math.floor(startDate.getUTCMonth() / 3);
      const endQ = endDate.getUTCFullYear() * 4 + Math.floor(endDate.getUTCMonth() / 3);
      return endQ - startQ;
    }
    case 'month':
      return (
        (endDate.getUTCFullYear() - startDate.getUTCFullYear()) * 12 +
        (endDate.getUTCMonth() - startDate.getUTCMonth())
      );
    case 'week':
      return Math.floor(diffMs / (7 * 24 * 60 * 60 * 1000));
    case 'day':
      return Math.floor(diffMs / (24 * 60 * 60 * 1000));
    case 'hour':
      return Math.floor(diffMs / (60 * 60 * 1000));
    case 'minute':
      return Math.floor(diffMs / (60 * 1000));
    case 'second':
      return Math.floor(diffMs / 1000);
    case 'millisecond':
      return diffMs;
    default:
      throw new Error(`Invalid time unit: ${unit}`);
  }
}

/**
 * $dateFromParts - Constructs a date from its constituent parts.
 */
export function evalDateFromParts(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Date | null {
  const spec = args as {
    year: unknown;
    month?: unknown;
    day?: unknown;
    hour?: unknown;
    minute?: unknown;
    second?: unknown;
    millisecond?: unknown;
    isoWeekYear?: unknown;
    isoWeek?: unknown;
    isoDayOfWeek?: unknown;
  };

  if ('isoWeekYear' in spec) {
    // ISO week date format
    const isoWeekYear = evaluate(spec.isoWeekYear, doc, vars) as number;
    const isoWeek = spec.isoWeek !== undefined ? (evaluate(spec.isoWeek, doc, vars) as number) : 1;
    const isoDayOfWeek =
      spec.isoDayOfWeek !== undefined ? (evaluate(spec.isoDayOfWeek, doc, vars) as number) : 1;

    if (isoWeekYear === null) return null;

    // Find Jan 4 of the ISO week year (always in week 1)
    const jan4 = new Date(Date.UTC(isoWeekYear, 0, 4));
    const jan4DayOfWeek = jan4.getUTCDay() || 7;
    const startOfISOWeek1 = new Date(jan4.getTime() - (jan4DayOfWeek - 1) * 24 * 60 * 60 * 1000);

    // Add weeks and days
    const targetDate = new Date(
      startOfISOWeek1.getTime() +
        (isoWeek - 1) * 7 * 24 * 60 * 60 * 1000 +
        (isoDayOfWeek - 1) * 24 * 60 * 60 * 1000
    );

    return targetDate;
  } else {
    // Regular date format
    const year = evaluate(spec.year, doc, vars) as number;
    const month = spec.month !== undefined ? (evaluate(spec.month, doc, vars) as number) : 1;
    const day = spec.day !== undefined ? (evaluate(spec.day, doc, vars) as number) : 1;
    const hour = spec.hour !== undefined ? (evaluate(spec.hour, doc, vars) as number) : 0;
    const minute = spec.minute !== undefined ? (evaluate(spec.minute, doc, vars) as number) : 0;
    const second = spec.second !== undefined ? (evaluate(spec.second, doc, vars) as number) : 0;
    const millisecond =
      spec.millisecond !== undefined ? (evaluate(spec.millisecond, doc, vars) as number) : 0;

    if (year === null) return null;

    return new Date(Date.UTC(year, month - 1, day, hour, minute, second, millisecond));
  }
}

/**
 * $dateToParts - Returns a document with the constituent parts of a date.
 */
export function evalDateToParts(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Record<string, number> | null {
  const spec = args as { date: unknown; iso8601?: boolean };
  const dateValue = evaluate(spec.date, doc, vars);

  if (dateValue === null || dateValue === undefined) {
    return null;
  }

  if (!(dateValue instanceof Date)) {
    const typeName = getBSONTypeName(dateValue);
    throw new Error(`$dateToParts requires a date, found: ${typeName}`);
  }

  if (spec.iso8601) {
    const day = dateValue.getUTCDay();
    const isoDayOfWeek = day === 0 ? 7 : day;

    // Calculate ISO week
    const jan4 = new Date(Date.UTC(dateValue.getUTCFullYear(), 0, 4));
    const jan4DayOfWeek = jan4.getUTCDay() || 7;
    const startOfISOWeek1 = new Date(jan4.getTime() - (jan4DayOfWeek - 1) * 24 * 60 * 60 * 1000);
    const daysSinceISOWeek1 = Math.floor(
      (dateValue.getTime() - startOfISOWeek1.getTime()) / (24 * 60 * 60 * 1000)
    );
    let isoWeek = Math.floor(daysSinceISOWeek1 / 7) + 1;
    let isoWeekYear = dateValue.getUTCFullYear();

    if (isoWeek < 1) {
      isoWeek = 52;
      isoWeekYear -= 1;
    }
    if (isoWeek > 52) {
      isoWeek = 1;
      isoWeekYear += 1;
    }

    return {
      isoWeekYear,
      isoWeek,
      isoDayOfWeek,
      hour: dateValue.getUTCHours(),
      minute: dateValue.getUTCMinutes(),
      second: dateValue.getUTCSeconds(),
      millisecond: dateValue.getUTCMilliseconds(),
    };
  } else {
    return {
      year: dateValue.getUTCFullYear(),
      month: dateValue.getUTCMonth() + 1,
      day: dateValue.getUTCDate(),
      hour: dateValue.getUTCHours(),
      minute: dateValue.getUTCMinutes(),
      second: dateValue.getUTCSeconds(),
      millisecond: dateValue.getUTCMilliseconds(),
    };
  }
}

/**
 * $dateFromString - Converts a date/time string to a date object.
 */
export function evalDateFromString(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Date | null {
  const spec = args as {
    dateString: unknown;
    format?: string;
    onNull?: unknown;
    onError?: unknown;
  };
  const dateString = evaluate(spec.dateString, doc, vars);

  if (dateString === null || dateString === undefined) {
    if (spec.onNull !== undefined) {
      return evaluate(spec.onNull, doc, vars) as Date | null;
    }
    return null;
  }

  if (typeof dateString !== 'string') {
    if (spec.onError !== undefined) {
      return evaluate(spec.onError, doc, vars) as Date | null;
    }
    throw new Error('$dateFromString requires a string');
  }

  // Try to parse ISO format by default
  const date = new Date(dateString);

  if (isNaN(date.getTime())) {
    if (spec.onError !== undefined) {
      return evaluate(spec.onError, doc, vars) as Date | null;
    }
    throw new Error(`Cannot parse date from string: ${dateString}`);
  }

  return date;
}
