/**
 * Aggregation Pipeline for MangoDB.
 *
 * This module provides MongoDB-compatible aggregation pipeline functionality.
 */
export { evaluateExpression } from './expression.ts';
export { AggregationCursor } from './cursor.ts';
export type { AggregationDbContext, VariableContext } from './types.ts';
