/**
 * Shared helper functions for aggregation.
 */

/**
 * Get BSON type name for a value (used in error messages).
 */
export function getBSONTypeName(value: unknown): string {
  if (value === null) return 'null';
  if (value === undefined) return 'missing';
  if (Array.isArray(value)) return 'array';
  if (value instanceof Date) return 'date';
  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'int' : 'double';
  }
  if (typeof value === 'boolean') return 'bool';
  if (typeof value === 'string') return 'string';
  if (typeof value === 'object') {
    if (value && typeof (value as { toHexString?: unknown }).toHexString === 'function') {
      return 'objectId';
    }
    return 'object';
  }
  return typeof value;
}
