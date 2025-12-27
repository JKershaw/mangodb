/**
 * Partition utilities for aggregation stages.
 *
 * Used by $densify, $fill, $setWindowFields, $bucket, $bucketAuto
 * to group documents into partitions.
 */
import type { Document, SortSpec } from '../types.ts';
import type { EvaluateExpressionFn, VariableContext } from './types.ts';
import { getValueByPath, compareValuesForSort } from '../utils.ts';

/**
 * Options for partitioning documents.
 */
export interface PartitionOptions {
  /** Expression form: { field1: "$field1", field2: "$field2" } */
  partitionBy?: unknown;
  /** Array form: ["field1", "field2"] */
  partitionByFields?: string[];
}

/**
 * Generate a stable partition key from partition values.
 * Uses JSON.stringify for consistent key generation.
 */
function getPartitionKey(values: unknown[]): string {
  return JSON.stringify(values);
}

/**
 * Partition documents into groups based on field values.
 *
 * @param docs - Documents to partition
 * @param options - Partitioning options
 * @param evaluate - Expression evaluation function
 * @param vars - Optional variable context for expression evaluation
 * @returns Map from partition key to array of documents
 */
export function partitionDocuments(
  docs: Document[],
  options: PartitionOptions,
  evaluate: EvaluateExpressionFn,
  vars?: VariableContext
): Map<string, Document[]> {
  const result = new Map<string, Document[]>();

  // No partitioning - all docs in single group
  if (!options.partitionBy && !options.partitionByFields) {
    result.set('', docs);
    return result;
  }

  for (const doc of docs) {
    let partitionValues: unknown[];

    if (options.partitionByFields) {
      // Array form: extract values from specified fields
      partitionValues = options.partitionByFields.map((field) => getValueByPath(doc, field));
    } else if (options.partitionBy) {
      // Expression form: must be an object
      if (
        typeof options.partitionBy !== 'object' ||
        options.partitionBy === null ||
        Array.isArray(options.partitionBy)
      ) {
        throw new Error('partitionBy must be an object expression, not a string or array');
      }

      const partitionByObj = options.partitionBy as Record<string, unknown>;
      partitionValues = Object.keys(partitionByObj).map((key) =>
        evaluate(partitionByObj[key], doc, vars)
      );
    } else {
      partitionValues = [];
    }

    const key = getPartitionKey(partitionValues);
    if (!result.has(key)) {
      result.set(key, []);
    }
    result.get(key)!.push(doc);
  }

  return result;
}

/**
 * Sort documents within a partition.
 *
 * @param docs - Documents to sort (not mutated)
 * @param sortBy - Sort specification
 * @returns New sorted array
 */
export function sortPartition(docs: Document[], sortBy: SortSpec): Document[] {
  const sortFields = Object.entries(sortBy) as [string, 1 | -1][];

  return [...docs].sort((a, b) => {
    for (const [field, direction] of sortFields) {
      const aValue = getValueByPath(a, field);
      const bValue = getValueByPath(b, field);
      const cmp = compareValuesForSort(aValue, bValue, direction);
      if (cmp !== 0) {
        return direction === 1 ? cmp : -cmp;
      }
    }
    return 0;
  });
}
