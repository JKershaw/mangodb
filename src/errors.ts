/**
 * MongoDB-compatible error classes for MangoDB.
 */

import type { IndexKeySpec } from "./types.ts";

/**
 * Error thrown when a duplicate key violation occurs on a unique index.
 * Matches MongoDB's E11000 error format.
 *
 * @example
 * ```typescript
 * try {
 *   await collection.insertOne({ email: 'user@example.com' });
 *   await collection.insertOne({ email: 'user@example.com' }); // Duplicate!
 * } catch (err) {
 *   if (err instanceof MongoDuplicateKeyError) {
 *     console.log('Duplicate key error:', err.keyValue);
 *     console.log('Index pattern:', err.keyPattern);
 *   }
 * }
 * ```
 */
export class MongoDuplicateKeyError extends Error {
  /** MongoDB error code for duplicate key */
  readonly code = 11000;

  /** The index key pattern that was violated */
  readonly keyPattern: IndexKeySpec;

  /** The duplicate key value that caused the error */
  readonly keyValue: Record<string, unknown>;

  /**
   * Create a new MongoDuplicateKeyError.
   *
   * @param db - Database name where the error occurred
   * @param collection - Collection name where the error occurred
   * @param indexName - Name of the unique index that was violated
   * @param keyPattern - The index key pattern (e.g., { email: 1 })
   * @param keyValue - The duplicate key value that caused the violation
   */
  constructor(
    db: string,
    collection: string,
    indexName: string,
    keyPattern: IndexKeySpec,
    keyValue: Record<string, unknown>
  ) {
    const keyStr = Object.entries(keyValue)
      .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
      .join(", ");
    super(
      `E11000 duplicate key error collection: ${db}.${collection} index: ${indexName} dup key: { ${keyStr} }`
    );
    this.name = "MongoDuplicateKeyError";
    this.keyPattern = keyPattern;
    this.keyValue = keyValue;
  }
}

/**
 * Error thrown when trying to drop an index that doesn't exist.
 * Matches MongoDB's IndexNotFound error (code 27).
 *
 * @example
 * ```typescript
 * try {
 *   await collection.dropIndex('nonexistent_index');
 * } catch (err) {
 *   if (err instanceof IndexNotFoundError) {
 *     console.log('Index not found:', err.message);
 *   }
 * }
 * ```
 */
export class IndexNotFoundError extends Error {
  /** MongoDB error code for IndexNotFound */
  readonly code = 27;
  readonly codeName = "IndexNotFound";

  /**
   * Create a new IndexNotFoundError.
   *
   * @param indexName - Name of the index that was not found
   */
  constructor(indexName: string) {
    // MongoDB uses "index not found with name [indexName]"
    super(`index not found with name [${indexName}]`);
    this.name = "IndexNotFoundError";
  }
}

/**
 * Error thrown when trying to drop the _id index.
 * Matches MongoDB's InvalidOptions error (code 72).
 * The _id index is required and cannot be dropped.
 *
 * @example
 * ```typescript
 * try {
 *   await collection.dropIndex('_id_');
 * } catch (err) {
 *   if (err instanceof CannotDropIdIndexError) {
 *     console.log('Cannot drop _id index');
 *   }
 * }
 * ```
 */
export class CannotDropIdIndexError extends Error {
  /** MongoDB error code for InvalidOptions */
  readonly code = 72;
  readonly codeName = "InvalidOptions";

  /**
   * Create a new CannotDropIdIndexError.
   */
  constructor() {
    super("cannot drop _id index");
    this.name = "CannotDropIdIndexError";
  }
}

/**
 * Error thrown when a $text query is executed without a text index.
 * Matches MongoDB's IndexNotFound error (code 27).
 *
 * @example
 * ```typescript
 * try {
 *   await collection.find({ $text: { $search: "hello" } }).toArray();
 * } catch (err) {
 *   if (err instanceof TextIndexRequiredError) {
 *     console.log('Need to create a text index first');
 *   }
 * }
 * ```
 */
export class TextIndexRequiredError extends Error {
  /** MongoDB error code for IndexNotFound */
  readonly code = 27;
  readonly codeName = "IndexNotFound";

  /**
   * Create a new TextIndexRequiredError.
   */
  constructor() {
    super("text index required for $text query");
    this.name = "TextIndexRequiredError";
  }
}
