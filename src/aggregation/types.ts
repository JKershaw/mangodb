/**
 * Shared types for aggregation pipeline.
 */
import type { Document } from '../types.ts';

/**
 * Variable context for $filter, $map, $reduce expressions.
 */
export type VariableContext = Record<string, unknown>;

/**
 * Geo index information for aggregation context.
 */
export interface GeoIndexInfo {
  field: string;
  type: '2d' | '2dsphere';
  indexName: string;
}

/**
 * Interface for database context needed by $lookup, $out, and $merge stages.
 */
export interface AggregationDbContext {
  getCollection(name: string): {
    find(filter: Document): { toArray(): Promise<Document[]> };
    findOne(filter: Document): Promise<Document | null>;
    deleteMany(filter: Document): Promise<unknown>;
    insertOne(doc: Document): Promise<unknown>;
    insertMany(docs: Document[]): Promise<unknown>;
    replaceOne(filter: Document, replacement: Document): Promise<unknown>;
    updateOne(filter: Document, update: Document): Promise<unknown>;
  };
  /** Get geo indexes for the current collection (used by $geoNear) */
  getGeoIndexes?(): Promise<GeoIndexInfo[]>;
}

/**
 * Operator evaluation function signature.
 */
export type EvaluateExpressionFn = (
  expr: unknown,
  doc: Document,
  vars?: VariableContext
) => unknown;
