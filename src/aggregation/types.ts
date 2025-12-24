/**
 * Shared types for aggregation pipeline.
 */
import type { Document } from "../types.ts";

/**
 * Variable context for $filter, $map, $reduce expressions.
 */
export type VariableContext = Record<string, unknown>;

/**
 * Interface for database context needed by $lookup and $out stages.
 */
export interface AggregationDbContext {
  getCollection(name: string): {
    find(filter: Document): { toArray(): Promise<Document[]> };
    deleteMany(filter: Document): Promise<unknown>;
    insertMany(docs: Document[]): Promise<unknown>;
  };
}

/**
 * Operator evaluation function signature.
 */
export type EvaluateExpressionFn = (
  expr: unknown,
  doc: Document,
  vars?: VariableContext
) => unknown;
