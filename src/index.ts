export { MongoneClient } from "./client.ts";
export { MongoneDb } from "./db.ts";
export { MongoneCollection } from "./collection.ts";
export type { IndexKeySpec, CreateIndexOptions, IndexInfo } from "./collection.ts";
export { MongoneCursor, IndexCursor } from "./cursor.ts";
export { AggregationCursor } from "./aggregation.ts";
export {
  MongoDuplicateKeyError,
  IndexNotFoundError,
  CannotDropIdIndexError,
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
