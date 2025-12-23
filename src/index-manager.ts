/**
 * Index management for MangoDB collections.
 */
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { Document, IndexKeySpec, IndexInfo, CreateIndexOptions } from "./types.ts";
import { getValueByPath } from "./document-utils.ts";
import { MongoDuplicateKeyError, IndexNotFoundError, CannotDropIdIndexError, InvalidIndexOptionsError } from "./errors.ts";
import { matchesFilter } from "./query-matcher.ts";

/**
 * Default _id index that exists on all collections.
 */
const DEFAULT_ID_INDEX: IndexInfo = { v: 2, key: { _id: 1 }, name: "_id_" };

/**
 * IndexManager handles index operations for a MangoDB collection.
 * Stores index metadata in a separate JSON file and manages index creation,
 * deletion, and unique constraint validation.
 */
export class IndexManager {
  private readonly indexFilePath: string;
  private readonly dbName: string;
  private readonly collectionName: string;

  /**
   * Create an IndexManager for a collection.
   * @param indexFilePath - Path to the index metadata file
   * @param dbName - Database name (for error messages)
   * @param collectionName - Collection name (for error messages)
   */
  constructor(indexFilePath: string, dbName: string, collectionName: string) {
    this.indexFilePath = indexFilePath;
    this.dbName = dbName;
    this.collectionName = collectionName;
  }

  /**
   * Read index metadata from the index file.
   * If the file doesn't exist, returns the default _id index.
   * @returns Array of index metadata objects
   * @throws Error if file read or JSON parsing fails (except ENOENT)
   */
  async loadIndexes(): Promise<IndexInfo[]> {
    try {
      const content = await readFile(this.indexFilePath, "utf-8");
      const parsed = JSON.parse(content);
      return parsed.indexes || [];
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return [DEFAULT_ID_INDEX];
      }
      throw error;
    }
  }

  /**
   * Write index metadata to the index file.
   * Creates the parent directory if it doesn't exist.
   * @param indexes - Array of index metadata objects to save
   * @throws Error if directory creation or file write fails
   */
  async saveIndexes(indexes: IndexInfo[]): Promise<void> {
    await mkdir(dirname(this.indexFilePath), { recursive: true });
    await writeFile(this.indexFilePath, JSON.stringify({ indexes }, null, 2));
  }

  /**
   * Generate an index name from the key specification.
   * Concatenates field names and directions with underscores.
   * @param keySpec - Fields to index (e.g., { email: 1, age: -1 })
   * @returns Generated index name (e.g., "email_1_age_-1")
   */
  generateIndexName(keySpec: IndexKeySpec): string {
    return Object.entries(keySpec)
      .map(([field, direction]) => `${field}_${direction}`)
      .join("_");
  }

  /**
   * Check if two key specifications are equivalent.
   * Compares both field names and their sort directions in order.
   * @param a - First index key specification
   * @param b - Second index key specification
   * @returns True if the specifications are identical, false otherwise
   */
  keySpecsEqual(a: IndexKeySpec, b: IndexKeySpec): boolean {
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b);
    if (aKeys.length !== bKeys.length) return false;
    for (let i = 0; i < aKeys.length; i++) {
      if (aKeys[i] !== bKeys[i]) return false;
      if (a[aKeys[i]] !== b[bKeys[i]]) return false;
    }
    return true;
  }

  /**
   * Create an index on the collection.
   * If an index with the same key specification already exists, returns its name.
   * @param keySpec - Fields to index (e.g., { email: 1 })
   * @param options - Index options (unique, name, sparse, expireAfterSeconds, partialFilterExpression)
   * @returns The name of the created or existing index
   * @throws InvalidIndexOptionsError if sparse and partialFilterExpression are both specified
   */
  async createIndex(
    keySpec: IndexKeySpec,
    options: CreateIndexOptions = {}
  ): Promise<string> {
    // Validate: cannot combine sparse and partialFilterExpression
    if (options.sparse && options.partialFilterExpression) {
      throw new InvalidIndexOptionsError(
        "cannot mix 'partialFilterExpression' with 'sparse'"
      );
    }

    const indexes = await this.loadIndexes();
    const indexName = options.name || this.generateIndexName(keySpec);

    const existingBySpec = indexes.find((idx) => this.keySpecsEqual(idx.key, keySpec));
    if (existingBySpec) {
      return existingBySpec.name;
    }

    const newIndex: IndexInfo = {
      v: 2,
      key: keySpec,
      name: indexName,
    };

    if (options.unique) {
      newIndex.unique = true;
    }

    if (options.sparse) {
      newIndex.sparse = true;
    }

    // TTL indexes only work on single-field indexes
    // Compound indexes silently ignore expireAfterSeconds (MongoDB behavior)
    const isCompoundIndex = Object.keys(keySpec).length > 1;
    if (options.expireAfterSeconds !== undefined && !isCompoundIndex) {
      newIndex.expireAfterSeconds = options.expireAfterSeconds;
    }

    if (options.partialFilterExpression) {
      newIndex.partialFilterExpression = options.partialFilterExpression;
    }

    indexes.push(newIndex);
    await this.saveIndexes(indexes);

    return indexName;
  }

  /**
   * Drop an index from the collection.
   * The _id index cannot be dropped.
   * @param indexNameOrSpec - Index name or key specification to drop
   * @throws CannotDropIdIndexError if attempting to drop the _id index
   * @throws IndexNotFoundError if the index does not exist
   */
  async dropIndex(indexNameOrSpec: string | IndexKeySpec): Promise<void> {
    const indexes = await this.loadIndexes();

    let indexName: string;
    if (typeof indexNameOrSpec === "string") {
      indexName = indexNameOrSpec;
    } else {
      indexName = this.generateIndexName(indexNameOrSpec);
    }

    if (indexName === "_id_") {
      throw new CannotDropIdIndexError();
    }

    if (
      typeof indexNameOrSpec === "object" &&
      this.keySpecsEqual(indexNameOrSpec, { _id: 1 })
    ) {
      throw new CannotDropIdIndexError();
    }

    const indexIdx = indexes.findIndex((idx) => idx.name === indexName);
    if (indexIdx === -1) {
      throw new IndexNotFoundError(indexName);
    }

    indexes.splice(indexIdx, 1);
    await this.saveIndexes(indexes);
  }

  /**
   * List all indexes on the collection.
   * @returns Array of all index metadata objects
   */
  async indexes(): Promise<IndexInfo[]> {
    return this.loadIndexes();
  }

  /**
   * Get the fields that are part of a text index.
   * Returns an empty array if no text index exists.
   * @returns Array of field names that are text-indexed
   */
  async getTextIndexFields(): Promise<string[]> {
    const indexes = await this.loadIndexes();
    for (const idx of indexes) {
      const fields: string[] = [];
      for (const [field, direction] of Object.entries(idx.key)) {
        if (direction === "text") {
          fields.push(field);
        }
      }
      if (fields.length > 0) {
        return fields;
      }
    }
    return [];
  }

  /**
   * Extract the key value from a document for a given index key specification.
   * Supports nested field paths using dot notation.
   * Missing fields are treated as null (matching MongoDB behavior).
   * @param doc - Document to extract values from
   * @param keySpec - Index key specification
   * @returns Object mapping field names to their values in the document
   */
  extractKeyValue<T extends Document>(
    doc: T,
    keySpec: IndexKeySpec
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const field of Object.keys(keySpec)) {
      const value = getValueByPath(doc, field);
      // Missing fields (undefined) are treated as null for uniqueness checks
      // This matches MongoDB behavior where { a: 1 } and { a: 1, b: null }
      // have the same index key for index { a: 1, b: 1 }
      result[field] = value === undefined ? null : value;
    }
    return result;
  }

  /**
   * Check if a document should be included in a sparse index.
   * For sparse indexes, documents are only indexed if at least one indexed field exists.
   * @param doc - Document to check
   * @param indexFields - Array of field names in the index
   * @returns true if document should be indexed, false if it should be skipped
   */
  private shouldIncludeInSparseIndex<T extends Document>(
    doc: T,
    indexFields: string[]
  ): boolean {
    // Include if at least one indexed field exists (is not undefined)
    return indexFields.some((field) => {
      const value = getValueByPath(doc, field);
      return value !== undefined;
    });
  }

  /**
   * Check if a document matches a partial index filter.
   * @param doc - Document to check
   * @param filter - Partial filter expression
   * @returns true if document matches the filter
   */
  private matchesPartialFilter<T extends Document>(
    doc: T,
    filter: Record<string, unknown>
  ): boolean {
    return matchesFilter(doc, filter);
  }

  /**
   * Check unique constraints for documents being inserted or updated.
   * Validates that no unique index constraints would be violated by the operation.
   * Handles sparse indexes (skip if all indexed fields missing) and partial indexes
   * (only enforce uniqueness for documents matching the filter).
   * @param docs - Documents to be inserted or updated
   * @param existingDocs - All existing documents in the collection
   * @param excludeIds - Document IDs to exclude from constraint checking (for updates)
   * @throws MongoDuplicateKeyError if a unique constraint would be violated
   */
  async checkUniqueConstraints<T extends Document>(
    docs: T[],
    existingDocs: T[],
    excludeIds: Set<string> = new Set()
  ): Promise<void> {
    const indexes = await this.loadIndexes();
    const uniqueIndexes = indexes.filter((idx) => idx.unique);

    if (uniqueIndexes.length === 0) {
      return;
    }

    const existingValues = new Map<string, Map<string, T>>();
    for (const idx of uniqueIndexes) {
      const valueMap = new Map<string, T>();
      const indexFields = Object.keys(idx.key);
      const isSparse = idx.sparse === true;
      const partialFilter = idx.partialFilterExpression;

      for (const doc of existingDocs) {
        const docId = (doc as { _id?: { toHexString(): string } })._id;
        if (docId && excludeIds.has(docId.toHexString())) {
          continue;
        }

        // For sparse indexes: skip if ALL indexed fields are missing
        if (isSparse && !this.shouldIncludeInSparseIndex(doc, indexFields)) {
          continue;
        }

        // For partial indexes: skip if document doesn't match the filter
        if (partialFilter && !this.matchesPartialFilter(doc, partialFilter)) {
          continue;
        }

        const keyValue = this.extractKeyValue(doc, idx.key);
        const keyStr = JSON.stringify(keyValue);
        valueMap.set(keyStr, doc);
      }
      existingValues.set(idx.name, valueMap);
    }

    for (const doc of docs) {
      for (const idx of uniqueIndexes) {
        const indexFields = Object.keys(idx.key);
        const isSparse = idx.sparse === true;
        const partialFilter = idx.partialFilterExpression;

        // For sparse indexes: skip if ALL indexed fields are missing
        if (isSparse && !this.shouldIncludeInSparseIndex(doc, indexFields)) {
          continue;
        }

        // For partial indexes: skip if document doesn't match the filter
        if (partialFilter && !this.matchesPartialFilter(doc, partialFilter)) {
          continue;
        }

        const keyValue = this.extractKeyValue(doc, idx.key);
        const keyStr = JSON.stringify(keyValue);
        const valueMap = existingValues.get(idx.name)!;

        if (valueMap.has(keyStr)) {
          throw new MongoDuplicateKeyError(
            this.dbName,
            this.collectionName,
            idx.name,
            idx.key,
            keyValue
          );
        }

        valueMap.set(keyStr, doc);
      }
    }
  }
}
