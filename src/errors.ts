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

/**
 * Error thrown when invalid index options are specified.
 * For example, combining sparse and partialFilterExpression.
 * Matches MongoDB's InvalidOptions error (code 67).
 *
 * @example
 * ```typescript
 * try {
 *   await collection.createIndex({ email: 1 }, {
 *     sparse: true,
 *     partialFilterExpression: { status: "active" }
 *   });
 * } catch (err) {
 *   if (err instanceof InvalidIndexOptionsError) {
 *     console.log('Cannot combine these options');
 *   }
 * }
 * ```
 */
export class InvalidIndexOptionsError extends Error {
  /** MongoDB error code for InvalidOptions */
  readonly code = 67;
  readonly codeName = "InvalidOptions";

  /**
   * Create a new InvalidIndexOptionsError.
   *
   * @param message - Description of the invalid options
   */
  constructor(message: string) {
    super(message);
    this.name = "InvalidIndexOptionsError";
  }
}

/**
 * Error thrown when using hint() with a non-existent index.
 * Matches MongoDB's planner error format.
 *
 * @example
 * ```typescript
 * try {
 *   await collection.find({}).hint("nonexistent_index").toArray();
 * } catch (err) {
 *   if (err instanceof BadHintError) {
 *     console.log('Index not found');
 *   }
 * }
 * ```
 */
export class BadHintError extends Error {
  /** MongoDB error code for bad hint */
  readonly code = 17007;
  readonly codeName = "BadValue";

  /**
   * Create a new BadHintError.
   *
   * @param hint - The hint that was specified (index name or key pattern)
   */
  constructor(hint: string | Record<string, unknown>) {
    const hintStr = typeof hint === "string" ? hint : JSON.stringify(hint);
    super(`planner returned error: bad hint - ${hintStr}`);
    this.name = "BadHintError";
  }
}

/**
 * Error thrown when a namespace (collection) is not found.
 * For example, when trying to rename a non-existent collection.
 * Matches MongoDB's NamespaceNotFound error (code 26).
 *
 * @example
 * ```typescript
 * try {
 *   const nonExistent = db.collection('doesNotExist');
 *   await nonExistent.rename('newName');
 * } catch (err) {
 *   if (err instanceof NamespaceNotFoundError) {
 *     console.log('Collection does not exist');
 *   }
 * }
 * ```
 */
export class NamespaceNotFoundError extends Error {
  /** MongoDB error code for NamespaceNotFound */
  readonly code = 26;
  readonly codeName = "NamespaceNotFound";

  /**
   * Create a new NamespaceNotFoundError.
   *
   * @param message - Description of the namespace issue (default: "source namespace does not exist")
   */
  constructor(message = "source namespace does not exist") {
    super(message);
    this.name = "NamespaceNotFoundError";
  }
}

/**
 * Error thrown when a target namespace already exists.
 * For example, when trying to rename a collection to a name that already exists
 * without specifying dropTarget: true.
 * Matches MongoDB's NamespaceExists error (code 48).
 *
 * @example
 * ```typescript
 * try {
 *   await collection.rename('existingCollection');
 * } catch (err) {
 *   if (err instanceof TargetNamespaceExistsError) {
 *     console.log('Target collection already exists');
 *   }
 * }
 * ```
 */
export class TargetNamespaceExistsError extends Error {
  /** MongoDB error code for NamespaceExists */
  readonly code = 48;
  readonly codeName = "NamespaceExists";

  /**
   * Create a new TargetNamespaceExistsError.
   *
   * @param message - Description of the namespace issue (default: "target namespace exists")
   */
  constructor(message = "target namespace exists") {
    super(message);
    this.name = "TargetNamespaceExistsError";
  }
}

/**
 * Error thrown when an illegal operation is attempted.
 * For example, when trying to rename a collection to itself.
 * Matches MongoDB's IllegalOperation error (code 20).
 *
 * @example
 * ```typescript
 * try {
 *   await collection.rename('sameCollection'); // same name as current
 * } catch (err) {
 *   if (err instanceof IllegalOperationError) {
 *     console.log('Cannot rename collection to itself');
 *   }
 * }
 * ```
 */
export class IllegalOperationError extends Error {
  /** MongoDB error code for IllegalOperation */
  readonly code = 20;
  readonly codeName = "IllegalOperation";

  /**
   * Create a new IllegalOperationError.
   *
   * @param message - Description of the illegal operation
   */
  constructor(message: string) {
    super(message);
    this.name = "IllegalOperationError";
  }
}

/**
 * Error thrown when an invalid collection name is used.
 * Matches MongoDB's InvalidNamespace error (code 73).
 *
 * @example
 * ```typescript
 * try {
 *   await collection.rename('.invalid');
 * } catch (err) {
 *   if (err instanceof InvalidNamespaceError) {
 *     console.log('Invalid collection name');
 *   }
 * }
 * ```
 */
export class InvalidNamespaceError extends Error {
  /** MongoDB error code for InvalidNamespace */
  readonly code = 73;
  readonly codeName = "InvalidNamespace";

  /**
   * Create a new InvalidNamespaceError.
   *
   * @param message - Description of the naming issue
   */
  constructor(message: string) {
    super(message);
    this.name = "InvalidNamespaceError";
  }
}
