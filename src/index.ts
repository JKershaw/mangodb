export { MangoDBClient } from "./client.ts";
export { MangoDBDb } from "./db.ts";
export { MangoDBCollection } from "./collection.ts";
export type { IndexKeySpec, CreateIndexOptions, IndexInfo } from "./collection.ts";
export { MangoDBCursor, IndexCursor } from "./cursor.ts";
export { AggregationCursor } from "./aggregation/index.ts";
export {
  MongoDuplicateKeyError,
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
