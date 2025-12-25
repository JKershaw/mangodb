/**
 * Gap filling utilities for aggregation stages.
 *
 * Used by $fill stage and window functions like $locf and $linearFill.
 */

/**
 * Apply Last Observation Carried Forward (LOCF) to fill gaps.
 *
 * Replaces null/undefined values with the last non-null value encountered.
 * Values at the start before any non-null value remain null.
 *
 * @param values - Array of values, with nulls representing gaps
 * @returns New array with gaps filled using LOCF
 */
export function applyLocf<T>(values: (T | null | undefined)[]): (T | null)[] {
  const result: (T | null)[] = [];
  let lastValue: T | null = null;

  for (const value of values) {
    if (value !== null && value !== undefined) {
      lastValue = value;
      result.push(value);
    } else {
      result.push(lastValue);
    }
  }

  return result;
}

/**
 * Apply linear interpolation to fill numeric gaps.
 *
 * Fills null values by linearly interpolating between surrounding non-null values.
 * Values at the start (before first non-null) remain null.
 * Values at the end (after last non-null) remain null.
 *
 * @param values - Array of numeric values, with nulls representing gaps
 * @param positions - Optional array of positions for range-based interpolation.
 *                    If not provided, uses index-based interpolation.
 * @returns New array with gaps filled using linear interpolation
 */
export function applyLinearFill(
  values: (number | null | undefined)[],
  positions?: number[]
): (number | null)[] {
  const result: (number | null)[] = [...values].map((v) =>
    v === undefined ? null : v
  );

  // Use positions if provided, otherwise use indices
  const pos = positions ?? values.map((_, i) => i);

  // Find gaps and fill them
  let i = 0;
  while (i < result.length) {
    if (result[i] === null) {
      // Find the start of this gap
      const gapStart = i;

      // Find the left boundary (last non-null before gap)
      let leftIdx = gapStart - 1;
      while (leftIdx >= 0 && result[leftIdx] === null) {
        leftIdx--;
      }

      // Find the right boundary (first non-null after gap)
      let rightIdx = gapStart;
      while (rightIdx < result.length && result[rightIdx] === null) {
        rightIdx++;
      }

      // If we have both boundaries, interpolate
      if (leftIdx >= 0 && rightIdx < result.length) {
        const leftVal = result[leftIdx] as number;
        const rightVal = result[rightIdx] as number;
        const leftPos = pos[leftIdx];
        const rightPos = pos[rightIdx];
        const posRange = rightPos - leftPos;

        // Fill each gap position
        for (let j = gapStart; j < rightIdx; j++) {
          const currentPos = pos[j];
          const fraction = (currentPos - leftPos) / posRange;
          result[j] = leftVal + fraction * (rightVal - leftVal);
        }
      }
      // If no left or no right boundary, leave as null

      i = rightIdx + 1;
    } else {
      i++;
    }
  }

  return result;
}

/**
 * Check if a value represents a gap (null or undefined).
 */
export function isGap(value: unknown): value is null | undefined {
  return value === null || value === undefined;
}

/**
 * Get the first non-null value from an array.
 */
export function getFirstNonNull<T>(
  values: (T | null | undefined)[]
): T | null {
  for (const value of values) {
    if (value !== null && value !== undefined) {
      return value;
    }
  }
  return null;
}

/**
 * Get the last non-null value from an array.
 */
export function getLastNonNull<T>(
  values: (T | null | undefined)[]
): T | null {
  for (let i = values.length - 1; i >= 0; i--) {
    if (values[i] !== null && values[i] !== undefined) {
      return values[i] as T;
    }
  }
  return null;
}
