/**
 * Date utilities for aggregation stages.
 *
 * Used by $densify for stepping through date ranges, and can be used
 * by other stages that need date arithmetic without expression evaluation.
 */

/**
 * Valid time units for date operations.
 */
export type TimeUnit =
  | 'year'
  | 'quarter'
  | 'month'
  | 'week'
  | 'day'
  | 'hour'
  | 'minute'
  | 'second'
  | 'millisecond';

/**
 * Add a step amount to a date by a given time unit.
 * Uses calendar-aware arithmetic for month/year units.
 *
 * @param date - The starting date
 * @param step - The amount to add (can be negative)
 * @param unit - The time unit
 * @returns A new Date with the step added
 */
export function addDateStep(date: Date, step: number, unit: TimeUnit): Date {
  const result = new Date(date.getTime());

  switch (unit) {
    case 'year':
      result.setUTCFullYear(result.getUTCFullYear() + step);
      break;
    case 'quarter':
      result.setUTCMonth(result.getUTCMonth() + step * 3);
      break;
    case 'month':
      result.setUTCMonth(result.getUTCMonth() + step);
      break;
    case 'week':
      result.setUTCDate(result.getUTCDate() + step * 7);
      break;
    case 'day':
      result.setUTCDate(result.getUTCDate() + step);
      break;
    case 'hour':
      result.setUTCHours(result.getUTCHours() + step);
      break;
    case 'minute':
      result.setUTCMinutes(result.getUTCMinutes() + step);
      break;
    case 'second':
      result.setUTCSeconds(result.getUTCSeconds() + step);
      break;
    case 'millisecond':
      result.setUTCMilliseconds(result.getUTCMilliseconds() + step);
      break;
    default:
      throw new Error(`Invalid time unit: ${unit}`);
  }

  return result;
}

/**
 * Calculate the difference between two dates in the given unit.
 *
 * @param startDate - The start date
 * @param endDate - The end date
 * @param unit - The time unit
 * @returns The difference in the specified unit
 */
export function dateDiff(startDate: Date, endDate: Date, unit: TimeUnit): number {
  const diffMs = endDate.getTime() - startDate.getTime();

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
 * Check if a string is a valid time unit.
 */
export function isValidTimeUnit(unit: string): unit is TimeUnit {
  return [
    'year',
    'quarter',
    'month',
    'week',
    'day',
    'hour',
    'minute',
    'second',
    'millisecond',
  ].includes(unit.toLowerCase());
}

/**
 * Normalize a time unit string to lowercase.
 */
export function normalizeTimeUnit(unit: string): TimeUnit {
  const normalized = unit.toLowerCase();
  if (!isValidTimeUnit(normalized)) {
    throw new Error(`Invalid time unit: ${unit}`);
  }
  return normalized;
}

/**
 * Generate a sequence of dates from start to end with given step.
 *
 * @param start - Start date (inclusive)
 * @param end - End date (exclusive)
 * @param step - Step size
 * @param unit - Time unit for stepping
 * @returns Array of dates from start to just before end
 */
export function generateDateSequence(start: Date, end: Date, step: number, unit: TimeUnit): Date[] {
  const result: Date[] = [];
  let current = new Date(start.getTime());

  while (current.getTime() < end.getTime()) {
    result.push(new Date(current.getTime()));
    current = addDateStep(current, step, unit);
  }

  return result;
}
