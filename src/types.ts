/**
 * Common types and interfaces for MangoDB.
 */
import type { ObjectId } from "mongodb";

/**
 * Base document type used throughout MangoDB.
 * Represents a MongoDB document as a flexible key-value record.
 */
export type Document = Record<string, unknown>;

/**
 * Query operators supported by MangoDB for filtering documents.
 * These operators enable complex query conditions beyond simple equality checks.
 * @property $eq - Matches values that are equal to a specified value
 * @property $ne - Matches values that are not equal to a specified value
 * @property $gt - Matches values that are greater than a specified value
 * @property $gte - Matches values that are greater than or equal to a specified value
 * @property $lt - Matches values that are less than a specified value
 * @property $lte - Matches values that are less than or equal to a specified value
 * @property $in - Matches any of the values specified in an array
 * @property $nin - Matches none of the values specified in an array
 * @property $exists - Matches documents that have the specified field
 * @property $not - Inverts the effect of a query expression (can also accept RegExp)
 * @property $size - Matches arrays with a specified number of elements
 * @property $all - Matches arrays that contain all elements specified in the query
 * @property $elemMatch - Matches documents that contain an array field with at least one element matching all specified query criteria
 * @property $regex - Matches strings using a regular expression pattern
 * @property $options - Options for $regex (i=case-insensitive, m=multiline, s=dotall)
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
  $not?: QueryOperators | RegExp;
  $size?: number;
  $all?: unknown[];
  $elemMatch?: Record<string, unknown>;
  $regex?: string | RegExp;
  $options?: string;
}

/**
 * A filter value can be a direct value or an object with query operators.
 * Used in filter expressions to specify either an exact match or a complex query condition.
 */
export type FilterValue = unknown | QueryOperators;

/**
 * Filter type for queries.
 * Supports field conditions and logical operators ($and, $or, $nor).
 * Allows querying documents by field values with support for MongoDB query operators.
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
 * Result of an insertOne operation.
 * Contains information about the inserted document.
 * @property acknowledged - Whether the write operation was acknowledged by the server
 * @property insertedId - The ObjectId of the newly inserted document
 */
export interface InsertOneResult {
  acknowledged: boolean;
  insertedId: ObjectId;
}

/**
 * Result of an insertMany operation.
 * Contains information about the batch insert operation.
 * @property acknowledged - Whether the write operation was acknowledged by the server
 * @property insertedIds - A map of the index position to the ObjectId of each inserted document
 */
export interface InsertManyResult {
  acknowledged: boolean;
  insertedIds: Record<number, ObjectId>;
}

/**
 * Result of delete operations (deleteOne and deleteMany).
 * Contains information about the number of documents removed.
 * @property acknowledged - Whether the write operation was acknowledged by the server
 * @property deletedCount - The number of documents that were deleted
 */
export interface DeleteResult {
  acknowledged: boolean;
  deletedCount: number;
}

/**
 * Result of update operations (updateOne and updateMany).
 * Contains detailed information about the update operation's effects.
 * @property acknowledged - Whether the write operation was acknowledged by the server
 * @property matchedCount - The number of documents that matched the filter
 * @property modifiedCount - The number of documents that were actually modified
 * @property upsertedCount - The number of documents that were upserted
 * @property upsertedId - The ObjectId of the upserted document, or null if no upsert occurred
 */
export interface UpdateResult {
  acknowledged: boolean;
  matchedCount: number;
  modifiedCount: number;
  upsertedCount: number;
  upsertedId: ObjectId | null;
}

/**
 * Options for update operations (updateOne and updateMany).
 * @property upsert - When true, creates a new document if no document matches the filter
 */
export interface UpdateOptions {
  upsert?: boolean;
}

/**
 * Options for find operations (find and findOne).
 * @property projection - Specifies which fields to include or exclude in the returned documents
 */
export interface FindOptions {
  projection?: ProjectionSpec;
}

/**
 * Projection specification type for controlling which fields are returned.
 * Keys are field names (can use dot notation for nested fields).
 * Values are 1 for inclusion or 0 for exclusion.
 * Note: Cannot mix inclusion and exclusion except for _id field.
 */
export type ProjectionSpec = Record<string, 0 | 1>;

/**
 * Update operators supported by MangoDB for modifying documents.
 * These operators enable targeted modifications to specific fields within documents.
 * @property $set - Sets the value of a field in a document
 * @property $unset - Removes the specified field from a document
 * @property $inc - Increments the value of a numeric field by the specified amount
 * @property $push - Appends a specified value to an array
 * @property $pull - Removes all array elements that match a specified query
 * @property $addToSet - Adds elements to an array only if they do not already exist in the set
 * @property $pop - Removes the first or last element of an array (1 for last, -1 for first)
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
 * Configures the behavior of finding and deleting a single document.
 * @property projection - Specifies which fields to include or exclude in the returned document
 * @property sort - Determines which document to delete when multiple documents match (1 for ascending, -1 for descending)
 */
export interface FindOneAndDeleteOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
}

/**
 * Options for findOneAndReplace operation.
 * Configures the behavior of finding and replacing a single document.
 * @property projection - Specifies which fields to include or exclude in the returned document
 * @property sort - Determines which document to replace when multiple documents match (1 for ascending, -1 for descending)
 * @property upsert - When true, creates a new document if no document matches the filter
 * @property returnDocument - Specifies whether to return the document before ("before") or after ("after") the replacement
 */
export interface FindOneAndReplaceOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

/**
 * Options for findOneAndUpdate operation.
 * Configures the behavior of finding and updating a single document.
 * @property projection - Specifies which fields to include or exclude in the returned document
 * @property sort - Determines which document to update when multiple documents match (1 for ascending, -1 for descending)
 * @property upsert - When true, creates a new document if no document matches the filter
 * @property returnDocument - Specifies whether to return the document before ("before") or after ("after") the update
 */
export interface FindOneAndUpdateOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

/**
 * A single operation for bulkWrite.
 * Represents one of several possible write operations that can be executed in a batch.
 * Only one property should be set per operation object.
 * @property insertOne - Inserts a single document
 * @property updateOne - Updates a single document matching the filter
 * @property updateMany - Updates all documents matching the filter
 * @property deleteOne - Deletes a single document matching the filter
 * @property deleteMany - Deletes all documents matching the filter
 * @property replaceOne - Replaces a single document matching the filter
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
 * Controls the execution behavior of batch write operations.
 * @property ordered - When true, operations are executed in order and stop on first error. When false, all operations are attempted regardless of errors (default: true)
 */
export interface BulkWriteOptions {
  ordered?: boolean;
}

/**
 * Result of a bulkWrite operation.
 * Contains comprehensive statistics about all operations performed in the batch.
 * @property acknowledged - Whether the write operation was acknowledged by the server
 * @property insertedCount - The number of documents inserted
 * @property matchedCount - The number of documents matched for update operations
 * @property modifiedCount - The number of documents modified
 * @property deletedCount - The number of documents deleted
 * @property upsertedCount - The number of documents upserted
 * @property insertedIds - A map of the operation index to the ObjectId of each inserted document
 * @property upsertedIds - A map of the operation index to the ObjectId of each upserted document
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
 * Index key specification for creating indexes.
 * Keys are field names (can use dot notation for nested fields).
 * Values are 1 for ascending index order or -1 for descending index order.
 */
export type IndexKeySpec = Record<string, 1 | -1>;

/**
 * Options for creating an index.
 * Configures the properties and behavior of a new index.
 * @property unique - When true, creates a unique index that rejects duplicate values for the indexed field(s)
 * @property name - Optional custom name for the index (MongoDB will generate one if not provided)
 * @property sparse - When true, the index only includes documents that contain the indexed field(s)
 */
export interface CreateIndexOptions {
  unique?: boolean;
  name?: string;
  sparse?: boolean;
}

/**
 * Information about an existing index.
 * Describes the configuration and properties of an index in the collection.
 * @property v - The index version number
 * @property key - The key specification showing which field(s) are indexed and their sort order
 * @property name - The name of the index
 * @property unique - Whether this is a unique index that enforces unique values
 * @property sparse - Whether this is a sparse index that only indexes documents containing the field(s)
 */
export interface IndexInfo {
  v: number;
  key: IndexKeySpec;
  name: string;
  unique?: boolean;
  sparse?: boolean;
}

/**
 * Sort specification for ordering query results.
 * Keys are field names (can use dot notation for nested fields).
 * Values are 1 for ascending sort order or -1 for descending sort order.
 */
export type SortSpec = Record<string, 1 | -1>;

// ==================== Aggregation Pipeline Types ====================

/**
 * $match stage - filters documents using query syntax.
 * Reuses the same filter syntax as find().
 */
export interface MatchStage {
  $match: Filter<Document>;
}

/**
 * $project stage - reshapes documents by including, excluding, or renaming fields.
 * Values can be:
 * - 1: include field
 * - 0: exclude field
 * - "$fieldName": reference another field (rename/copy)
 * - { $literal: value }: literal value
 */
export interface ProjectStage {
  $project: Record<string, 0 | 1 | string | ProjectExpression>;
}

/**
 * Expression for $project stage computed fields.
 */
export interface ProjectExpression {
  $literal?: unknown;
}

/**
 * $sort stage - orders documents by specified fields.
 */
export interface SortStage {
  $sort: SortSpec;
}

/**
 * $limit stage - limits the number of documents passed to the next stage.
 */
export interface LimitStage {
  $limit: number;
}

/**
 * $skip stage - skips the first n documents.
 */
export interface SkipStage {
  $skip: number;
}

/**
 * $count stage - counts documents and outputs a single document with the count.
 * The string value specifies the output field name for the count.
 */
export interface CountStage {
  $count: string;
}

/**
 * Options for the $unwind stage when using the object syntax.
 */
export interface UnwindOptions {
  /** The path to the array field to unwind (must start with $) */
  path: string;
  /** If true, documents with null, missing, or empty arrays are preserved */
  preserveNullAndEmptyArrays?: boolean;
  /** Field name to add containing the array index */
  includeArrayIndex?: string;
}

/**
 * $unwind stage - deconstructs an array field into multiple documents.
 * Can be a string (short syntax) or an object with options.
 */
export interface UnwindStage {
  $unwind: string | UnwindOptions;
}

/**
 * $group stage - groups documents by _id and applies accumulators.
 */
export interface GroupStage {
  $group: {
    _id: unknown;
    [field: string]: unknown;
  };
}

/**
 * $lookup stage - performs a left outer join with another collection.
 */
export interface LookupStage {
  $lookup: {
    from: string;
    localField: string;
    foreignField: string;
    as: string;
  };
}

/**
 * $addFields stage - adds new fields to documents.
 */
export interface AddFieldsStage {
  $addFields: Record<string, unknown>;
}

/**
 * $set stage - alias for $addFields.
 */
export interface SetStage {
  $set: Record<string, unknown>;
}

/**
 * $replaceRoot stage - replaces document with embedded document.
 */
export interface ReplaceRootStage {
  $replaceRoot: {
    newRoot: unknown;
  };
}

/**
 * $out stage - writes pipeline results to a collection.
 */
export interface OutStage {
  $out: string;
}

/**
 * Union type for all supported pipeline stages.
 */
export type PipelineStage =
  | MatchStage
  | ProjectStage
  | SortStage
  | LimitStage
  | SkipStage
  | CountStage
  | UnwindStage
  | GroupStage
  | LookupStage
  | AddFieldsStage
  | SetStage
  | ReplaceRootStage
  | OutStage;

/**
 * Options for the aggregate() method.
 */
export interface AggregateOptions {
  // Reserved for future options like allowDiskUse, batchSize, etc.
}
