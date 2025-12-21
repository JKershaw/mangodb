/**
 * MongoDB-compatible error classes for Mongone.
 */

/**
 * Error thrown when a duplicate key violation occurs on a unique index.
 * Matches MongoDB's E11000 error format.
 */
export class MongoDuplicateKeyError extends Error {
  /** MongoDB error code for duplicate key */
  readonly code = 11000;

  /** The index key pattern that was violated */
  readonly keyPattern: Record<string, 1 | -1>;

  /** The duplicate key value that caused the error */
  readonly keyValue: Record<string, unknown>;

  constructor(
    db: string,
    collection: string,
    indexName: string,
    keyPattern: Record<string, 1 | -1>,
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
 */
export class IndexNotFoundError extends Error {
  constructor(indexName: string) {
    super(`index not found with name [${indexName}]`);
    this.name = "IndexNotFoundError";
  }
}

/**
 * Error thrown when trying to drop the _id index.
 */
export class CannotDropIdIndexError extends Error {
  constructor() {
    super("cannot drop _id index");
    this.name = "CannotDropIdIndexError";
  }
}
