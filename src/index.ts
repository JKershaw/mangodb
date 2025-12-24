export { MangoClient } from "./client.ts";
export { MangoDb } from "./db.ts";
export { MangoCollection } from "./collection.ts";
export type { IndexKeySpec, CreateIndexOptions, IndexInfo } from "./collection.ts";
export { MangoCursor, IndexCursor } from "./cursor.ts";
export { AggregationCursor } from "./aggregation/index.ts";
export {
  DuplicateKeyError,
  IndexNotFoundError,
  CannotDropIdIndexError,
  TextIndexRequiredError,
  InvalidIndexOptionsError,
  BadHintError,
} from "./errors.ts";

// Aggregation pipeline types
export type {
  PipelineStage,
  MatchStage,
  ProjectStage,
  SortStage,
  LimitStage,
  SkipStage,
  CountStage,
  UnwindStage,
  UnwindOptions,
  AggregateOptions,
  ProjectExpression,
} from "./types.ts";
