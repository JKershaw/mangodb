/**
 * Query Analyzer for MangoDB index utilization.
 *
 * Analyzes query filters to determine if they can use an index,
 * and generates execution plans for index-based queries.
 */

import type { Filter, IndexInfo, Document } from './types.ts';

/**
 * Bounds for a range query on a single field.
 */
export interface RangeBounds {
  lower?: { value: unknown; inclusive: boolean };
  upper?: { value: unknown; inclusive: boolean };
}

/**
 * Plan for executing a query using an index.
 */
export interface IndexScanPlan {
  /** Name of the index to use */
  indexName: string;

  /** Type of index scan */
  type: 'equality' | 'range';

  /** For equality lookups: the key fields and their values */
  equalityFields?: Record<string, unknown>;

  /** For range queries: the field and bounds */
  rangeField?: string;
  rangeBounds?: RangeBounds;

  /** Filter conditions not covered by the index (need post-filtering) */
  remainingFilter: Filter<Document> | null;
}

/**
 * Non-indexable operators that force a full collection scan.
 */
const NON_INDEXABLE_OPERATORS = new Set([
  '$or',
  '$nor',
  '$not',
  '$text',
  '$where',
  '$expr',
  '$jsonSchema',
]);

/**
 * Range operators that can use index range scans.
 */
const RANGE_OPERATORS = new Set(['$gt', '$gte', '$lt', '$lte']);

/**
 * Operators that cannot use indexes efficiently.
 */
const NON_INDEXABLE_FIELD_OPERATORS = new Set([
  '$ne',
  '$nin',
  '$regex',
  '$not',
  '$elemMatch', // Complex to optimize
  '$all', // Complex to optimize
  '$size', // Cannot use index
  '$type', // Cannot use index efficiently
  '$mod', // Cannot use index
  '$geoWithin',
  '$geoIntersects',
  '$near',
  '$nearSphere',
]);

/**
 * Check if a filter value is a simple equality (not an operator object).
 */
function isSimpleEquality(value: unknown): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value !== 'object') return true;
  if (Array.isArray(value)) return true;

  // Check if it's an operator object
  const keys = Object.keys(value as object);
  return !keys.some((k) => k.startsWith('$'));
}

/**
 * Check if a filter value uses only $eq operator.
 */
function isEqOperator(value: unknown): value is { $eq: unknown } {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj);
  return keys.length === 1 && keys[0] === '$eq';
}

/**
 * Check if a filter value uses $in operator with values.
 */
function isInOperator(value: unknown): value is { $in: unknown[] } {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj);
  return keys.length === 1 && keys[0] === '$in' && Array.isArray(obj.$in);
}

/**
 * Extract range bounds from a filter value.
 * Returns null if the value is not a simple range query.
 */
function extractRangeBounds(value: unknown): RangeBounds | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return null;
  }

  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj);

  // Check all keys are range operators
  if (!keys.every((k) => RANGE_OPERATORS.has(k))) {
    return null;
  }

  const bounds: RangeBounds = {};

  if ('$gt' in obj) {
    bounds.lower = { value: obj.$gt, inclusive: false };
  } else if ('$gte' in obj) {
    bounds.lower = { value: obj.$gte, inclusive: true };
  }

  if ('$lt' in obj) {
    bounds.upper = { value: obj.$lt, inclusive: false };
  } else if ('$lte' in obj) {
    bounds.upper = { value: obj.$lte, inclusive: true };
  }

  return Object.keys(bounds).length > 0 ? bounds : null;
}

/**
 * Check if a field condition can use an index.
 * Returns the type of usage: 'equality', 'range', or null if not indexable.
 */
function getFieldIndexUsage(
  value: unknown
): { type: 'equality' | 'range'; equalityValue?: unknown; rangeBounds?: RangeBounds } | null {
  // Simple equality
  if (isSimpleEquality(value)) {
    return { type: 'equality', equalityValue: value };
  }

  // $eq operator
  if (isEqOperator(value)) {
    return { type: 'equality', equalityValue: value.$eq };
  }

  // $in with single value is effectively equality
  if (isInOperator(value) && value.$in.length === 1) {
    return { type: 'equality', equalityValue: value.$in[0] };
  }

  // Range operators
  const rangeBounds = extractRangeBounds(value);
  if (rangeBounds) {
    return { type: 'range', rangeBounds };
  }

  return null;
}

/**
 * Calculate how well an index matches a query.
 * Higher score = better match.
 *
 * @param indexInfo - The index to evaluate
 * @param fieldConditions - Map of field -> condition from the query
 * @returns Score and match details, or null if index cannot be used
 */
function scoreIndexMatch(
  indexInfo: IndexInfo,
  fieldConditions: Map<string, unknown>
): {
  score: number;
  equalityFields: Record<string, unknown>;
  rangeField?: string;
  rangeBounds?: RangeBounds;
  usedFields: Set<string>;
  isFullKeyMatch: boolean;
} | null {
  const indexFields = Object.keys(indexInfo.key);
  const equalityFields: Record<string, unknown> = {};
  const usedFields = new Set<string>();
  let score = 0;
  let rangeField: string | undefined;
  let rangeBounds: RangeBounds | undefined;

  // Check index fields in order (prefix matching)
  for (const field of indexFields) {
    const condition = fieldConditions.get(field);

    if (condition === undefined) {
      // Field not in query - can't use remaining index fields
      break;
    }

    const usage = getFieldIndexUsage(condition);

    if (!usage) {
      // Field has non-indexable operator - can't use this or remaining fields
      break;
    }

    if (usage.type === 'equality') {
      equalityFields[field] = usage.equalityValue;
      usedFields.add(field);
      score += 10; // Equality is highly selective
    } else if (usage.type === 'range') {
      // Range query - can only be on the last used field
      rangeField = field;
      rangeBounds = usage.rangeBounds;
      usedFields.add(field);
      score += 5; // Range is less selective than equality
      break; // Can't use more index fields after range
    }
  }

  // Must use at least one index field
  if (usedFields.size === 0) {
    return null;
  }

  // Check if all index fields are covered by equality conditions
  const isFullKeyMatch =
    !rangeField && Object.keys(equalityFields).length === indexFields.length;

  // Bonus for using _id index (always exists, always unique)
  if (indexInfo.name === '_id_') {
    score += 2;
  }

  return { score, equalityFields, rangeField, rangeBounds, usedFields, isFullKeyMatch };
}

/**
 * Analyze a query filter and determine the best index to use.
 *
 * @param filter - The query filter
 * @param indexes - Available indexes on the collection
 * @returns An execution plan if an index can be used, null for full scan
 */
export function analyzeQuery(
  filter: Filter<Document>,
  indexes: IndexInfo[]
): IndexScanPlan | null {
  // Empty filter = full scan (need all documents anyway)
  if (!filter || Object.keys(filter).length === 0) {
    return null;
  }

  // Check for non-indexable operators at top level
  for (const key of Object.keys(filter)) {
    if (NON_INDEXABLE_OPERATORS.has(key)) {
      return null;
    }
  }

  // Extract field conditions (skip operators)
  const fieldConditions = new Map<string, unknown>();
  for (const [key, value] of Object.entries(filter)) {
    if (!key.startsWith('$')) {
      // Check if this field has non-indexable operators
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const ops = Object.keys(value as object);
        if (ops.some((op) => NON_INDEXABLE_FIELD_OPERATORS.has(op))) {
          // This field can't use index, but might still be able to use other fields
          continue;
        }
      }
      fieldConditions.set(key, value);
    }
  }

  if (fieldConditions.size === 0) {
    return null;
  }

  // Score each index and find the best match
  let bestPlan: IndexScanPlan | null = null;
  let bestScore = 0;

  for (const indexInfo of indexes) {
    // Skip special index types for now
    const indexValues = Object.values(indexInfo.key);
    if (indexValues.some((v) => v === 'text' || v === '2d' || v === '2dsphere' || v === 'hashed')) {
      continue;
    }

    // Skip hidden indexes
    if (indexInfo.hidden) {
      continue;
    }

    const match = scoreIndexMatch(indexInfo, fieldConditions);
    if (!match) continue;

    // For equality queries on compound indexes, only use if it's a full key match
    // Partial key (prefix) queries require range scan or full scan
    const canUseEquality = match.isFullKeyMatch;
    const canUseRange = !!match.rangeField;

    // Skip if we can't use either equality or range lookup
    if (!canUseEquality && !canUseRange) {
      continue;
    }

    if (match.score > bestScore) {
      bestScore = match.score;

      // Calculate remaining filter (fields not covered by index)
      const remainingFilter: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(filter)) {
        if (!match.usedFields.has(key) && !key.startsWith('$')) {
          remainingFilter[key] = value;
        }
      }

      bestPlan = {
        indexName: indexInfo.name,
        type: canUseRange ? 'range' : 'equality',
        equalityFields: canUseEquality ? match.equalityFields : undefined,
        rangeField: match.rangeField,
        rangeBounds: match.rangeBounds,
        remainingFilter:
          Object.keys(remainingFilter).length > 0 ? (remainingFilter as Filter<Document>) : null,
      };
    }
  }

  return bestPlan;
}
