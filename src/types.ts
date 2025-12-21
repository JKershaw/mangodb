/**
 * Common types and interfaces for Mongone.
 */
import type { ObjectId } from "mongodb";

/**
 * Base document type used throughout Mongone.
 */
export type Document = Record<string, unknown>;

/**
 * Query operators supported by Mongone.
 */
export interface QueryOperators {
  $eq?: unknown;
  $ne?: unknown;
  $gt?: unknown;
  $gte?: unknown;
  $lt?: unknown;
  $lte?: unknown;
  $in?: unknown[];
  $nin?: unknown[];
  $exists?: boolean;
  $not?: QueryOperators;
  $size?: number;
  $all?: unknown[];
  $elemMatch?: Record<string, unknown>;
}

/**
 * A filter value can be a direct value or an object with query operators.
 */
export type FilterValue = unknown | QueryOperators;

/**
 * Filter type for queries.
 * Supports field conditions and logical operators ($and, $or, $nor).
 */
export type Filter<T> = {
  [P in keyof T]?: FilterValue;
} & {
  [key: string]: FilterValue;
} & {
  $and?: Filter<T>[];
  $or?: Filter<T>[];
  $nor?: Filter<T>[];
};

/**
 * Result of insertOne operation.
 */
export interface InsertOneResult {
  acknowledged: boolean;
  insertedId: ObjectId;
}

/**
 * Result of insertMany operation.
 */
export interface InsertManyResult {
  acknowledged: boolean;
  insertedIds: Record<number, ObjectId>;
}

/**
 * Result of delete operations.
 */
export interface DeleteResult {
  acknowledged: boolean;
  deletedCount: number;
}

/**
 * Result of update operations.
 */
export interface UpdateResult {
  acknowledged: boolean;
  matchedCount: number;
  modifiedCount: number;
  upsertedCount: number;
  upsertedId: ObjectId | null;
}

/**
 * Options for update operations.
 */
export interface UpdateOptions {
  upsert?: boolean;
}

/**
 * Options for find operations.
 */
export interface FindOptions {
  projection?: ProjectionSpec;
}

/**
 * Projection specification type.
 * Keys are field names (can use dot notation).
 * Values are 1 for inclusion, 0 for exclusion.
 */
export type ProjectionSpec = Record<string, 0 | 1>;

/**
 * Update operators supported by Mongone.
 */
export interface UpdateOperators {
  $set?: Record<string, unknown>;
  $unset?: Record<string, unknown>;
  $inc?: Record<string, number>;
  $push?: Record<string, unknown>;
  $pull?: Record<string, unknown>;
  $addToSet?: Record<string, unknown>;
  $pop?: Record<string, number>;
}

/**
 * Options for findOneAndDelete operation.
 */
export interface FindOneAndDeleteOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
}

/**
 * Options for findOneAndReplace operation.
 */
export interface FindOneAndReplaceOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

/**
 * Options for findOneAndUpdate operation.
 */
export interface FindOneAndUpdateOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

/**
 * A single operation for bulkWrite.
 */
export interface BulkWriteOperation<T> {
  insertOne?: { document: T };
  updateOne?: { filter: Filter<T>; update: UpdateOperators; upsert?: boolean };
  updateMany?: { filter: Filter<T>; update: UpdateOperators; upsert?: boolean };
  deleteOne?: { filter: Filter<T> };
  deleteMany?: { filter: Filter<T> };
  replaceOne?: { filter: Filter<T>; replacement: T; upsert?: boolean };
}

/**
 * Options for bulkWrite operation.
 */
export interface BulkWriteOptions {
  ordered?: boolean;
}

/**
 * Result of a bulkWrite operation.
 */
export interface BulkWriteResult {
  acknowledged: boolean;
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: Record<number, ObjectId>;
  upsertedIds: Record<number, ObjectId>;
}

/**
 * Index key specification.
 * Keys are field names (can use dot notation).
 * Values are 1 for ascending, -1 for descending.
 */
export type IndexKeySpec = Record<string, 1 | -1>;

/**
 * Options for creating an index.
 */
export interface CreateIndexOptions {
  unique?: boolean;
  name?: string;
  sparse?: boolean;
}

/**
 * Information about an index.
 */
export interface IndexInfo {
  v: number;
  key: IndexKeySpec;
  name: string;
  unique?: boolean;
  sparse?: boolean;
}

/**
 * Sort specification for queries.
 */
export type SortSpec = Record<string, 1 | -1>;
