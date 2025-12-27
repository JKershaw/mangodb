/**
 * Common types and interfaces for MangoDB.
 */
import type { ObjectId } from 'bson';

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
  $type?: string | number | (string | number)[];
  $mod?: [number, number];
  // Geospatial query operators
  $geoWithin?: GeoWithinSpec;
  $geoIntersects?: GeoIntersectsSpec;
  $near?: GeoNearQuerySpec;
  $nearSphere?: GeoNearQuerySpec;
}

/**
 * GeoJSON geometry types for geospatial queries.
 */
export interface GeoJSONPoint {
  type: 'Point';
  coordinates: [number, number];
}

export interface GeoJSONPolygon {
  type: 'Polygon';
  coordinates: [number, number][][];
}

export interface GeoJSONLineString {
  type: 'LineString';
  coordinates: [number, number][];
}

export type GeoJSONGeometry =
  | GeoJSONPoint
  | GeoJSONPolygon
  | GeoJSONLineString
  | { type: 'MultiPoint'; coordinates: [number, number][] }
  | { type: 'MultiPolygon'; coordinates: [number, number][][][] }
  | { type: 'MultiLineString'; coordinates: [number, number][][] }
  | { type: 'GeometryCollection'; geometries: GeoJSONGeometry[] };

/**
 * Specification for $geoWithin query operator.
 * Supports various shape specifiers.
 */
export interface GeoWithinSpec {
  $geometry?: GeoJSONGeometry;
  $box?: [[number, number], [number, number]];
  $center?: [[number, number], number];
  $centerSphere?: [[number, number], number];
  $polygon?: [number, number][];
}

/**
 * Specification for $geoIntersects query operator.
 * Only supports $geometry.
 */
export interface GeoIntersectsSpec {
  $geometry: GeoJSONGeometry;
}

/**
 * Specification for $near and $nearSphere query operators.
 */
export type GeoNearQuerySpec =
  | GeoJSONPoint
  | [number, number]
  | {
      $geometry?: GeoJSONPoint;
      $maxDistance?: number;
      $minDistance?: number;
    };

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
  $expr?: unknown;
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
 * Array filter for positional update operators.
 * Used with $[identifier] to specify which array elements to update.
 */
export type ArrayFilter = Record<string, unknown>;

/**
 * Options for update operations (updateOne and updateMany).
 * @property upsert - When true, creates a new document if no document matches the filter
 * @property arrayFilters - Array of filter documents for positional update operators ($[identifier])
 */
export interface UpdateOptions {
  upsert?: boolean;
  arrayFilters?: ArrayFilter[];
}

/**
 * Options for find operations (find and findOne).
 * @property projection - Specifies which fields to include or exclude in the returned documents
 * @property sort - Specifies the sort order for matching documents (findOne only)
 * @property skip - Number of documents to skip before returning the first match (findOne only)
 */
export interface FindOptions {
  projection?: ProjectionSpec;
  sort?: SortSpec;
  skip?: number;
}

/**
 * Projection operator for $slice - array slicing in projection.
 */
export interface ProjectionSlice {
  $slice: number | [number, number];
}

/**
 * Projection operator for $elemMatch - project first matching array element.
 */
export interface ProjectionElemMatch {
  $elemMatch: Record<string, unknown>;
}

/**
 * Projection operator for $meta - metadata projection (text score, etc).
 */
export interface ProjectionMeta {
  $meta: 'textScore' | 'indexKey';
}

/**
 * Projection specification type for controlling which fields are returned.
 * Keys are field names (can use dot notation for nested fields).
 * Values can be:
 * - 1 for inclusion
 * - 0 for exclusion
 * - { $slice: n } for array slicing
 * - { $elemMatch: query } for projecting first matching array element
 * - { $meta: "textScore" } for text search score
 * Note: Cannot mix inclusion and exclusion except for _id field.
 */
export type ProjectionSpec = Record<
  string,
  0 | 1 | ProjectionSlice | ProjectionElemMatch | ProjectionMeta
>;

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
 * @property $min - Updates field only if specified value is less than current value
 * @property $max - Updates field only if specified value is greater than current value
 * @property $mul - Multiplies the value of a field by a number
 * @property $rename - Renames a field
 * @property $currentDate - Sets the field to the current date
 * @property $setOnInsert - Sets fields only during upsert insert operations
 */
export interface UpdateOperators {
  $set?: Record<string, unknown>;
  $unset?: Record<string, unknown>;
  $inc?: Record<string, number>;
  $push?: Record<string, unknown>;
  $pull?: Record<string, unknown>;
  $pullAll?: Record<string, unknown[]>;
  $addToSet?: Record<string, unknown>;
  $pop?: Record<string, number>;
  // Phase 13 operators
  $min?: Record<string, unknown>;
  $max?: Record<string, unknown>;
  $mul?: Record<string, number>;
  $rename?: Record<string, string>;
  $currentDate?: Record<string, boolean | { $type: 'date' | 'timestamp' }>;
  $setOnInsert?: Record<string, unknown>;
  $bit?: Record<string, { and?: number; or?: number; xor?: number }>;
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
  returnDocument?: 'before' | 'after';
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
  returnDocument?: 'before' | 'after';
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
 * Values are:
 * - 1 for ascending
 * - -1 for descending
 * - "text" for text indexes
 * - "2d" for 2d planar geospatial indexes
 * - "2dsphere" for 2dsphere geospatial indexes
 * - "hashed" for hashed indexes
 */
export type IndexKeySpec = Record<string, 1 | -1 | 'text' | '2d' | '2dsphere' | 'hashed'>;

/**
 * Collation options for locale-aware string comparison.
 * Controls how string comparison is performed for index operations.
 * @property locale - ICU locale identifier (required)
 * @property strength - Comparison level: 1 (base), 2 (+accents), 3 (+case), 4 (+width), 5 (identical)
 * @property caseLevel - Whether to consider case as a separate level
 * @property caseFirst - Sort order for uppercase vs lowercase: "upper", "lower", or "off"
 * @property numericOrdering - Whether to compare numeric strings as numbers
 * @property alternate - How to handle whitespace/punctuation: "non-ignorable" or "shifted"
 * @property maxVariable - Which characters are affected by alternate: "punct" or "space"
 * @property backwards - Whether to reverse secondary difference sorting (e.g., French accents)
 */
export interface CollationOptions {
  locale: string;
  strength?: 1 | 2 | 3 | 4 | 5;
  caseLevel?: boolean;
  caseFirst?: 'upper' | 'lower' | 'off';
  numericOrdering?: boolean;
  alternate?: 'non-ignorable' | 'shifted';
  maxVariable?: 'punct' | 'space';
  backwards?: boolean;
}

/**
 * Options for creating an index.
 * Configures the properties and behavior of a new index.
 * @property unique - When true, creates a unique index that rejects duplicate values for the indexed field(s)
 * @property name - Optional custom name for the index (MongoDB will generate one if not provided)
 * @property sparse - When true, the index only includes documents that contain the indexed field(s)
 * @property expireAfterSeconds - TTL index: documents expire after this many seconds from the date field value
 * @property partialFilterExpression - Only index documents matching this filter expression
 * @property min - Minimum bound for 2d index coordinates (default: -180)
 * @property max - Maximum bound for 2d index coordinates (default: 180)
 * @property hidden - When true, the index is hidden from the query planner
 * @property collation - Locale-aware string comparison options
 * @property weights - Field weights for text indexes (1-99999)
 * @property default_language - Default language for text indexes
 * @property wildcardProjection - Include/exclude fields for wildcard indexes
 */
export interface CreateIndexOptions {
  unique?: boolean;
  name?: string;
  sparse?: boolean;
  expireAfterSeconds?: number;
  partialFilterExpression?: Record<string, unknown>;
  // Geospatial index options (for 2d indexes)
  min?: number;
  max?: number;
  // Phase 11: New index options
  hidden?: boolean;
  collation?: CollationOptions;
  // Text index options
  weights?: Record<string, number>;
  default_language?: string;
  // Wildcard index options
  wildcardProjection?: Record<string, 0 | 1>;
}

/**
 * Information about an existing index.
 * Describes the configuration and properties of an index in the collection.
 * @property v - The index version number
 * @property key - The key specification showing which field(s) are indexed and their sort order
 * @property name - The name of the index
 * @property unique - Whether this is a unique index that enforces unique values
 * @property sparse - Whether this is a sparse index that only indexes documents containing the field(s)
 * @property expireAfterSeconds - TTL index expiration time in seconds
 * @property partialFilterExpression - Filter expression for partial indexes
 * @property min - Minimum coordinate bound for 2d indexes
 * @property max - Maximum coordinate bound for 2d indexes
 * @property 2dsphereIndexVersion - Version of 2dsphere index (typically 3)
 * @property hidden - Whether this index is hidden from the query planner
 * @property collation - Locale-aware string comparison options
 * @property weights - Field weights for text indexes
 * @property default_language - Default language for text indexes
 * @property textIndexVersion - Version of text index (typically 3)
 * @property wildcardProjection - Include/exclude fields for wildcard indexes
 */
export interface IndexInfo {
  v: number;
  key: IndexKeySpec;
  name: string;
  unique?: boolean;
  sparse?: boolean;
  expireAfterSeconds?: number;
  partialFilterExpression?: Record<string, unknown>;
  // Geospatial index metadata
  min?: number;
  max?: number;
  '2dsphereIndexVersion'?: number;
  // Phase 11: New index metadata
  hidden?: boolean;
  collation?: CollationOptions;
  // Text index metadata
  weights?: Record<string, number>;
  default_language?: string;
  textIndexVersion?: number;
  // Wildcard index metadata
  wildcardProjection?: Record<string, 0 | 1>;
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
 * $geoNear stage - returns documents sorted by proximity to a geospatial point.
 * Must be the first stage in the pipeline.
 */
export interface GeoNearStage {
  $geoNear: {
    /** The point for which to find the closest documents. */
    near: GeoJSONPoint | [number, number];
    /** The output field that contains the calculated distance. */
    distanceField: string;
    /** The maximum distance from the center point (in meters for 2dsphere, coordinate units for 2d). */
    maxDistance?: number;
    /** The minimum distance from the center point. */
    minDistance?: number;
    /** If true, calculate distances using spherical geometry. */
    spherical?: boolean;
    /** Limits results to documents matching the query. */
    query?: Filter<Document>;
    /** The factor to multiply all distances by. */
    distanceMultiplier?: number;
    /** Output field to store the location used to calculate the distance. */
    includeLocs?: string;
    /** Specify which geospatial index to use. */
    key?: string;
  };
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
  | OutStage
  | GeoNearStage;

/**
 * Options for the aggregate() method.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface AggregateOptions {
  // Reserved for future options like allowDiskUse, batchSize, etc.
}

// ==================== Administrative Operation Types ====================

/**
 * Options for listCollections operation.
 * @property nameOnly - If true, returns only collection names (simplified output)
 * @property batchSize - Cursor batch size (ignored in MangoDB)
 */
export interface ListCollectionsOptions {
  nameOnly?: boolean;
  batchSize?: number;
}

/**
 * Information about a collection returned by listCollections.
 * @property name - The name of the collection
 * @property type - The type ('collection' or 'view')
 * @property options - Collection options
 * @property info - Additional info including readOnly status
 */
export interface CollectionInfo {
  name: string;
  type: 'collection' | 'view';
  options?: Record<string, unknown>;
  info?: {
    readOnly: boolean;
  };
}

/**
 * Options for collection.rename operation.
 * @property dropTarget - If true, drop target collection if it exists
 */
export interface RenameOptions {
  dropTarget?: boolean;
}

/**
 * Statistics about a database returned by db.stats().
 * @property db - Database name
 * @property collections - Number of collections
 * @property views - Number of views (always 0 for MangoDB)
 * @property objects - Total number of documents
 * @property dataSize - Total size of data files in bytes
 * @property storageSize - Same as dataSize for MangoDB
 * @property indexes - Total number of indexes
 * @property indexSize - Total size of index files in bytes
 * @property ok - Status indicator (1 for success)
 */
export interface DbStats {
  db: string;
  collections: number;
  views: number;
  objects: number;
  dataSize: number;
  storageSize: number;
  indexes: number;
  indexSize: number;
  ok: 1;
}

/**
 * Statistics about a collection returned by collection.stats().
 * @property ns - Namespace (db.collection)
 * @property count - Number of documents
 * @property size - Total size of documents in bytes
 * @property storageSize - Same as size for MangoDB
 * @property totalIndexSize - Size of indexes in bytes
 * @property indexSizes - Size of each index by name
 * @property totalSize - size + totalIndexSize
 * @property nindexes - Number of indexes
 * @property ok - Status indicator (1 for success)
 */
export interface CollectionStats {
  ns: string;
  count: number;
  size: number;
  storageSize: number;
  totalIndexSize: number;
  indexSizes: Record<string, number>;
  totalSize: number;
  nindexes: number;
  ok: 1;
}
