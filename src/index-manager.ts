/**
 * Index management for MangoDB collections.
 */
import { readFile, writeFile, mkdir, rename as renameFile, unlink } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import type { Document, IndexKeySpec, IndexInfo, CreateIndexOptions } from './types.ts';
import { getValueByPath } from './document-utils.ts';
import {
  DuplicateKeyError,
  IndexNotFoundError,
  CannotDropIdIndexError,
  InvalidIndexOptionsError,
} from './errors.ts';

import { matchesFilter } from './query-matcher.ts';
import { compareScalarValues } from './utils.ts';
import type { RangeBounds } from './query-analyzer.ts';

/**
 * Result of getGeoIndexInfo() describing a geo index.
 */
export interface GeoIndexInfo {
  field: string;
  type: '2d' | '2dsphere';
  indexName: string;
}

/**
 * In-memory index data structure for query optimization.
 * Stores document references by their _id for efficient lookup.
 */
export interface IndexData {
  /** For equality lookups: Map<serializedKeyValue, Set<documentIdString>> */
  equalityMap: Map<string, Set<string>>;

  /** For range queries: sorted array of { value, docIds } for single-field indexes */
  sortedEntries: Array<{ value: unknown; docIds: Set<string> }>;

  /** The first field in the index (used for range queries) */
  firstField: string;

  /** Reference to the index metadata */
  indexInfo: IndexInfo;
}

/**
 * Default _id index that exists on all collections.
 */
const DEFAULT_ID_INDEX: IndexInfo = { v: 2, key: { _id: 1 }, name: '_id_' };

/**
 * IndexManager handles index operations for a MangoDB collection.
 * Stores index metadata in a separate JSON file and manages index creation,
 * deletion, and unique constraint validation.
 */
export class IndexManager {
  private readonly indexFilePath: string;
  private readonly dbName: string;
  private readonly collectionName: string;

  /** In-memory index data structures for query optimization */
  private indexData: Map<string, IndexData> = new Map();

  /** Whether indexes have been built from documents */
  private indexesBuilt = false;

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

  // ==================== Index Data Methods ====================

  /**
   * Check if index data structures have been built.
   */
  isIndexDataBuilt(): boolean {
    return this.indexesBuilt;
  }

  /**
   * Get the document ID as a string for index storage.
   */
  private getDocIdString(doc: Document): string {
    const id = (doc as { _id?: unknown })._id;
    if (id === undefined || id === null) {
      return '';
    }
    if (typeof (id as { toHexString?: () => string }).toHexString === 'function') {
      return (id as { toHexString(): string }).toHexString();
    }
    return String(id);
  }

  /**
   * Serialize a key value for use as a map key.
   * Handles ObjectId, Date, and other special types.
   */
  private serializeKeyValue(value: unknown): string {
    if (value === undefined) {
      return JSON.stringify(null);
    }
    if (value !== null && typeof (value as { toHexString?: () => string }).toHexString === 'function') {
      return JSON.stringify({ $oid: (value as { toHexString(): string }).toHexString() });
    }
    if (value instanceof Date) {
      return JSON.stringify({ $date: value.toISOString() });
    }
    return JSON.stringify(value);
  }

  /**
   * Serialize a compound key (multiple fields) for equality map.
   */
  private serializeCompoundKey(keyValues: Record<string, unknown>): string {
    const serialized: Record<string, string> = {};
    for (const [field, value] of Object.entries(keyValues)) {
      serialized[field] = this.serializeKeyValue(value);
    }
    return JSON.stringify(serialized);
  }

  /**
   * Build all index data structures from documents.
   * Called lazily on first query after documents are loaded.
   *
   * @param documents - All documents in the collection
   */
  async buildIndexes<T extends Document>(documents: T[]): Promise<void> {
    const indexes = await this.loadIndexes();
    this.indexData.clear();

    for (const indexInfo of indexes) {
      // Skip special index types (text, geo, hashed) for now
      const indexValues = Object.values(indexInfo.key);
      if (indexValues.some((v) => v === 'text' || v === '2d' || v === '2dsphere' || v === 'hashed')) {
        continue;
      }

      // Skip hidden indexes
      if (indexInfo.hidden) {
        continue;
      }

      const indexFields = Object.keys(indexInfo.key);
      const firstField = indexFields[0];
      const isSparse = indexInfo.sparse === true;
      const partialFilter = indexInfo.partialFilterExpression;

      const data: IndexData = {
        equalityMap: new Map(),
        sortedEntries: [],
        firstField,
        indexInfo,
      };

      // Build a temporary map for sorted entries (value -> Set<docId>)
      const sortedMap = new Map<string, { value: unknown; docIds: Set<string> }>();

      for (const doc of documents) {
        const docId = this.getDocIdString(doc);
        if (!docId) continue;

        // Check sparse index condition
        if (isSparse && !this.shouldIncludeInSparseIndex(doc, indexFields)) {
          continue;
        }

        // Check partial filter condition
        if (partialFilter && !this.matchesPartialFilter(doc, partialFilter)) {
          continue;
        }

        // Extract key values for this document
        const keyValues = this.extractKeyValue(doc, indexInfo.key);

        // Add to equality map (compound key)
        const compoundKey = this.serializeCompoundKey(keyValues);
        if (!data.equalityMap.has(compoundKey)) {
          data.equalityMap.set(compoundKey, new Set());
        }
        data.equalityMap.get(compoundKey)!.add(docId);

        // Add to sorted entries (first field only, for range queries)
        const firstValue = keyValues[firstField];
        const serializedFirst = this.serializeKeyValue(firstValue);
        if (!sortedMap.has(serializedFirst)) {
          sortedMap.set(serializedFirst, { value: firstValue, docIds: new Set() });
        }
        sortedMap.get(serializedFirst)!.docIds.add(docId);
      }

      // Convert sorted map to sorted array
      data.sortedEntries = Array.from(sortedMap.values());
      data.sortedEntries.sort((a, b) => compareScalarValues(a.value, b.value));

      this.indexData.set(indexInfo.name, data);
    }

    this.indexesBuilt = true;
  }

  /**
   * Add a document to all indexes.
   * Called after insert operations.
   *
   * @param doc - The document that was inserted
   */
  addToIndexes<T extends Document>(doc: T): void {
    if (!this.indexesBuilt) return;

    const docId = this.getDocIdString(doc);
    if (!docId) return;

    for (const [, data] of this.indexData) {
      const indexInfo = data.indexInfo;
      const indexFields = Object.keys(indexInfo.key);
      const isSparse = indexInfo.sparse === true;
      const partialFilter = indexInfo.partialFilterExpression;

      // Check sparse index condition
      if (isSparse && !this.shouldIncludeInSparseIndex(doc, indexFields)) {
        continue;
      }

      // Check partial filter condition
      if (partialFilter && !this.matchesPartialFilter(doc, partialFilter)) {
        continue;
      }

      // Extract key values
      const keyValues = this.extractKeyValue(doc, indexInfo.key);

      // Add to equality map
      const compoundKey = this.serializeCompoundKey(keyValues);
      if (!data.equalityMap.has(compoundKey)) {
        data.equalityMap.set(compoundKey, new Set());
      }
      data.equalityMap.get(compoundKey)!.add(docId);

      // Add to sorted entries using binary search
      const firstValue = keyValues[data.firstField];
      const insertIdx = this.findInsertPosition(data.sortedEntries, firstValue);

      // Check if there's an existing entry at or before this position with the same value
      if (
        insertIdx < data.sortedEntries.length &&
        compareScalarValues(data.sortedEntries[insertIdx].value, firstValue) === 0
      ) {
        // Add to existing entry
        data.sortedEntries[insertIdx].docIds.add(docId);
      } else {
        // Insert new entry at sorted position
        const newEntry = { value: firstValue, docIds: new Set([docId]) };
        data.sortedEntries.splice(insertIdx, 0, newEntry);
      }
    }
  }

  /**
   * Remove a document from all indexes.
   * Called after delete operations.
   *
   * @param doc - The document that was deleted
   */
  removeFromIndexes<T extends Document>(doc: T): void {
    if (!this.indexesBuilt) return;

    const docId = this.getDocIdString(doc);
    if (!docId) return;

    for (const [, data] of this.indexData) {
      const indexInfo = data.indexInfo;

      // Extract key values
      const keyValues = this.extractKeyValue(doc, indexInfo.key);

      // Remove from equality map
      const compoundKey = this.serializeCompoundKey(keyValues);
      const docIds = data.equalityMap.get(compoundKey);
      if (docIds) {
        docIds.delete(docId);
        if (docIds.size === 0) {
          data.equalityMap.delete(compoundKey);
        }
      }

      // Remove from sorted entries using binary search
      const firstValue = keyValues[data.firstField];
      const idx = this.findInsertPosition(data.sortedEntries, firstValue);

      // Check if entry exists at this position
      if (
        idx < data.sortedEntries.length &&
        compareScalarValues(data.sortedEntries[idx].value, firstValue) === 0
      ) {
        data.sortedEntries[idx].docIds.delete(docId);
        if (data.sortedEntries[idx].docIds.size === 0) {
          data.sortedEntries.splice(idx, 1);
        }
      }
    }
  }

  /**
   * Update a document in all indexes.
   * Called after update operations.
   *
   * @param oldDoc - The document before update
   * @param newDoc - The document after update
   */
  updateInIndexes<T extends Document>(oldDoc: T, newDoc: T): void {
    if (!this.indexesBuilt) return;

    // Simple implementation: remove old, add new
    this.removeFromIndexes(oldDoc);
    this.addToIndexes(newDoc);
  }

  /**
   * Clear all index data structures.
   * Called when collection is dropped.
   */
  clearIndexData(): void {
    this.indexData.clear();
    this.indexesBuilt = false;
  }

  /**
   * Find the insert position for a value in a sorted array using binary search.
   */
  private findInsertPosition(
    entries: Array<{ value: unknown; docIds: Set<string> }>,
    value: unknown
  ): number {
    let low = 0;
    let high = entries.length;

    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      if (compareScalarValues(entries[mid].value, value) < 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return low;
  }

  /**
   * Look up documents by equality on indexed fields.
   *
   * @param indexName - Name of the index to use
   * @param keyValues - Field values to match
   * @returns Set of document IDs matching the query, or null if index not found
   */
  lookupEquality(indexName: string, keyValues: Record<string, unknown>): Set<string> | null {
    const data = this.indexData.get(indexName);
    if (!data) return null;

    const compoundKey = this.serializeCompoundKey(keyValues);
    const docIds = data.equalityMap.get(compoundKey);

    return docIds ? new Set(docIds) : new Set();
  }

  /**
   * Look up documents by range on the first field of an index.
   *
   * @param indexName - Name of the index to use
   * @param bounds - Range bounds for the query
   * @returns Set of document IDs matching the range, or null if index not found
   */
  lookupRange(indexName: string, bounds: RangeBounds): Set<string> | null {
    const data = this.indexData.get(indexName);
    if (!data) return null;

    const entries = data.sortedEntries;
    const result = new Set<string>();

    // Find start position using binary search
    let startIdx = 0;
    if (bounds.lower !== undefined) {
      startIdx = this.findLowerBound(entries, bounds.lower.value, bounds.lower.inclusive);
    }

    // Find end position
    let endIdx = entries.length;
    if (bounds.upper !== undefined) {
      endIdx = this.findUpperBound(entries, bounds.upper.value, bounds.upper.inclusive);
    }

    // Collect all document IDs in the range
    for (let i = startIdx; i < endIdx; i++) {
      for (const docId of entries[i].docIds) {
        result.add(docId);
      }
    }

    return result;
  }

  /**
   * Find the lower bound index for a range query.
   */
  private findLowerBound(
    entries: Array<{ value: unknown; docIds: Set<string> }>,
    value: unknown,
    inclusive: boolean
  ): number {
    let low = 0;
    let high = entries.length;

    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      const cmp = compareScalarValues(entries[mid].value, value);
      if (cmp < 0 || (!inclusive && cmp === 0)) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return low;
  }

  /**
   * Find the upper bound index for a range query.
   */
  private findUpperBound(
    entries: Array<{ value: unknown; docIds: Set<string> }>,
    value: unknown,
    inclusive: boolean
  ): number {
    let low = 0;
    let high = entries.length;

    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      const cmp = compareScalarValues(entries[mid].value, value);
      if (cmp < 0 || (inclusive && cmp === 0)) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return low;
  }

  /**
   * Read index metadata from the index file.
   * If the file doesn't exist, returns the default _id index.
   * @returns Array of index metadata objects
   * @throws Error if file read or JSON parsing fails (except ENOENT)
   */
  async loadIndexes(): Promise<IndexInfo[]> {
    try {
      const content = await readFile(this.indexFilePath, 'utf-8');
      const parsed = JSON.parse(content);
      return parsed.indexes || [];
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
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
    const content = JSON.stringify({ indexes }, null, 2);

    // Atomic write: write to temp file, then rename
    const tempPath = `${this.indexFilePath}.tmp-${randomUUID()}`;
    try {
      await writeFile(tempPath, content);
      await renameFile(tempPath, this.indexFilePath);
    } finally {
      // Clean up temp file if rename failed
      try {
        await unlink(tempPath);
      } catch {
        // Ignore - file was renamed or never created
      }
    }
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
      .join('_');
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
   * @throws InvalidIndexOptionsError if expireAfterSeconds is used on _id field
   * @throws InvalidIndexOptionsError if expireAfterSeconds is out of valid range
   */
  async createIndex(keySpec: IndexKeySpec, options: CreateIndexOptions = {}): Promise<string> {
    // Validate: cannot combine sparse and partialFilterExpression
    if (options.sparse && options.partialFilterExpression) {
      throw new InvalidIndexOptionsError("cannot mix 'partialFilterExpression' with 'sparse'");
    }

    // Validate TTL options
    if (options.expireAfterSeconds !== undefined) {
      // Check if it's a valid number
      if (typeof options.expireAfterSeconds !== 'number' || isNaN(options.expireAfterSeconds)) {
        throw new InvalidIndexOptionsError('expireAfterSeconds must be a number');
      }

      // Check range: must be between 0 and 2147483647 (max 32-bit signed integer)
      if (options.expireAfterSeconds < 0 || options.expireAfterSeconds > 2147483647) {
        throw new InvalidIndexOptionsError('expireAfterSeconds must be between 0 and 2147483647');
      }

      // TTL cannot be used on _id field
      const indexFields = Object.keys(keySpec);
      if (indexFields.length === 1 && indexFields[0] === '_id') {
        throw new InvalidIndexOptionsError(
          "The field 'expireAfterSeconds' is not valid for an _id index"
        );
      }

      // TTL indexes must be single-field indexes
      if (indexFields.length > 1) {
        throw new InvalidIndexOptionsError(
          'TTL indexes are single-field indexes, compound indexes do not support TTL'
        );
      }
    }

    // Validate hashed index restrictions
    const hashedFields = Object.entries(keySpec).filter(([, v]) => v === 'hashed');
    if (hashedFields.length > 0) {
      // Cannot be unique
      if (options.unique) {
        throw new InvalidIndexOptionsError('hashed indexes cannot be unique');
      }
      // Cannot have multiple hashed fields in one index
      if (hashedFields.length > 1) {
        throw new InvalidIndexOptionsError('can only have one hashed index field');
      }
    }

    // Validate wildcard index restrictions
    const wildcardFields = Object.keys(keySpec).filter((k) => k.includes('$**'));
    if (wildcardFields.length > 0) {
      // Cannot be unique
      if (options.unique) {
        throw new InvalidIndexOptionsError('wildcard indexes cannot be unique');
      }
      // Cannot be compound
      if (Object.keys(keySpec).length > 1) {
        throw new InvalidIndexOptionsError('wildcard indexes cannot be compound');
      }
      // Validate wildcardProjection if provided
      if (options.wildcardProjection) {
        const nonIdKeys = Object.keys(options.wildcardProjection).filter((k) => k !== '_id');
        const nonIdValues = nonIdKeys.map((k) => options.wildcardProjection![k]);
        const hasInclusion = nonIdValues.some((v) => v === 1);
        const hasExclusion = nonIdValues.some((v) => v === 0);
        if (hasInclusion && hasExclusion) {
          throw new InvalidIndexOptionsError(
            'wildcardProjection cannot mix inclusion and exclusion'
          );
        }
      }
    }

    // Validate hidden option
    if (options.hidden === true) {
      const isIdIndex = Object.keys(keySpec).length === 1 && '_id' in keySpec;
      if (isIdIndex) {
        throw new InvalidIndexOptionsError('cannot hide _id index');
      }
    }

    // Validate text index options
    const hasTextIndex = Object.values(keySpec).includes('text');

    if (options.weights) {
      if (!hasTextIndex) {
        throw new InvalidIndexOptionsError('weights option requires a text index');
      }
      // Validate weight values
      for (const [field, weight] of Object.entries(options.weights)) {
        if (
          typeof weight !== 'number' ||
          weight < 1 ||
          weight > 99999 ||
          !Number.isInteger(weight)
        ) {
          throw new InvalidIndexOptionsError(
            `weight for field '${field}' must be an integer between 1 and 99999`
          );
        }
      }
    }

    if (options.default_language !== undefined) {
      if (!hasTextIndex) {
        throw new InvalidIndexOptionsError('default_language option requires a text index');
      }
    }

    // Validate collation option
    if (options.collation) {
      if (hasTextIndex) {
        throw new InvalidIndexOptionsError('text indexes do not support collation');
      }
      if (!options.collation.locale || options.collation.locale.trim() === '') {
        throw new InvalidIndexOptionsError('collation locale is required');
      }
    }

    const indexes = await this.loadIndexes();
    const indexName = options.name || this.generateIndexName(keySpec);

    const existingBySpec = indexes.find((idx) => this.keySpecsEqual(idx.key, keySpec));
    if (existingBySpec) {
      return existingBySpec.name;
    }

    // Check for geo index types and validate
    const geoFields = this.extractGeoFields(keySpec);
    if (geoFields.length > 0) {
      // Validate: cannot have multiple geo fields in one index
      if (geoFields.length > 1) {
        throw new InvalidIndexOptionsError('only one geo index type allowed per index');
      }

      // Validate: cannot have both 2d and 2dsphere on same field across indexes
      const existingGeoIndexes = await this.getGeoIndexes();
      for (const geoField of geoFields) {
        const existingOnField = existingGeoIndexes.find((g) => g.field === geoField.field);
        if (existingOnField && existingOnField.type !== geoField.type) {
          throw new InvalidIndexOptionsError(
            `can't have 2 geo indexes on the same field: already have ${existingOnField.type} index on '${geoField.field}'`
          );
        }
      }
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

    // TTL index expireAfterSeconds (already validated above - single-field only)
    if (options.expireAfterSeconds !== undefined) {
      newIndex.expireAfterSeconds = options.expireAfterSeconds;
    }

    if (options.partialFilterExpression) {
      newIndex.partialFilterExpression = options.partialFilterExpression;
    }

    // Add geo-specific metadata
    if (geoFields.length > 0) {
      const geoField = geoFields[0];
      if (geoField.type === '2d') {
        // 2d index bounds (default -180 to 180 for lat/lng)
        newIndex.min = options.min ?? -180;
        newIndex.max = options.max ?? 180;
      } else if (geoField.type === '2dsphere') {
        newIndex['2dsphereIndexVersion'] = 3;
      }
    }

    // Phase 11: Add new index metadata

    // Hidden option
    if (options.hidden !== undefined) {
      newIndex.hidden = options.hidden;
    }

    // Collation option
    if (options.collation) {
      newIndex.collation = options.collation;
    }

    // Text index options
    if (options.weights) {
      newIndex.weights = options.weights;
      newIndex.textIndexVersion = 3;
    }
    if (options.default_language !== undefined) {
      newIndex.default_language = options.default_language;
      newIndex.textIndexVersion = 3;
    }

    // Wildcard index metadata (implicitly sparse)
    if (wildcardFields.length > 0) {
      newIndex.sparse = true;
      if (options.wildcardProjection) {
        newIndex.wildcardProjection = options.wildcardProjection;
      }
    }

    indexes.push(newIndex);
    await this.saveIndexes(indexes);

    // Clear index data so it will be rebuilt on next query
    this.clearIndexData();

    return indexName;
  }

  /**
   * Extract geo fields from a key specification.
   * @param keySpec - Index key specification
   * @returns Array of { field, type } for each geo field
   */
  private extractGeoFields(
    keySpec: IndexKeySpec
  ): Array<{ field: string; type: '2d' | '2dsphere' }> {
    const result: Array<{ field: string; type: '2d' | '2dsphere' }> = [];
    for (const [field, direction] of Object.entries(keySpec)) {
      if (direction === '2d' || direction === '2dsphere') {
        result.push({ field, type: direction });
      }
    }
    return result;
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
    if (typeof indexNameOrSpec === 'string') {
      indexName = indexNameOrSpec;
    } else {
      indexName = this.generateIndexName(indexNameOrSpec);
    }

    if (indexName === '_id_') {
      throw new CannotDropIdIndexError();
    }

    if (typeof indexNameOrSpec === 'object' && this.keySpecsEqual(indexNameOrSpec, { _id: 1 })) {
      throw new CannotDropIdIndexError();
    }

    const indexIdx = indexes.findIndex((idx) => idx.name === indexName);
    if (indexIdx === -1) {
      throw new IndexNotFoundError(indexName);
    }

    indexes.splice(indexIdx, 1);
    await this.saveIndexes(indexes);

    // Clear index data so it will be rebuilt without the dropped index
    this.clearIndexData();
  }

  /**
   * List all indexes on the collection.
   * @returns Array of all index metadata objects
   */
  async indexes(): Promise<IndexInfo[]> {
    return this.loadIndexes();
  }

  /**
   * Reset the index manager to the default state.
   * Used when a collection is dropped to clear any cached indexes.
   * After reset, the collection will only have the default _id index.
   */
  async reset(): Promise<void> {
    // Clear in-memory index data structures
    this.clearIndexData();
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
        if (direction === 'text') {
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
   * Hash a value for hashed index comparison.
   * Floating-point numbers are truncated to 64-bit integer before hashing.
   * Arrays cannot be hashed (multikey not supported for hashed indexes).
   * @param value - Value to hash
   * @returns A string representation of the hashed value
   * @throws Error if value is an array (hashed indexes don't support arrays)
   */
  private hashValue(value: unknown): string {
    if (Array.isArray(value)) {
      throw new Error('hashed indexes do not support array values');
    }

    // Truncate floating-point to integer (MongoDB behavior)
    let normalizedValue = value;
    if (typeof value === 'number' && !Number.isInteger(value)) {
      normalizedValue = Math.trunc(value);
    }

    // Use consistent JSON representation for hashing
    return `hashed:${JSON.stringify(normalizedValue)}`;
  }

  /**
   * Extract the key value from a document for a given index key specification.
   * Supports nested field paths using dot notation.
   * Missing fields are treated as null (matching MongoDB behavior).
   * For hashed indexes, values are hashed and arrays are rejected.
   * @param doc - Document to extract values from
   * @param keySpec - Index key specification
   * @returns Object mapping field names to their values in the document
   * @throws Error if a hashed field contains an array value
   */
  extractKeyValue<T extends Document>(doc: T, keySpec: IndexKeySpec): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const [field, direction] of Object.entries(keySpec)) {
      const value = getValueByPath(doc, field);
      // Missing fields (undefined) are treated as null for uniqueness checks
      // This matches MongoDB behavior where { a: 1 } and { a: 1, b: null }
      // have the same index key for index { a: 1, b: 1 }
      const normalizedValue = value === undefined ? null : value;

      if (direction === 'hashed') {
        // For hashed indexes, hash the value (arrays will throw)
        result[field] = this.hashValue(normalizedValue);
      } else {
        result[field] = normalizedValue;
      }
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
  private shouldIncludeInSparseIndex<T extends Document>(doc: T, indexFields: string[]): boolean {
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
   * @throws DuplicateKeyError if a unique constraint would be violated
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
        const docId = (doc as { _id?: unknown })._id;
        if (docId !== undefined) {
          const idStr =
            typeof (docId as { toHexString?: () => string }).toHexString === 'function'
              ? (docId as { toHexString(): string }).toHexString()
              : String(docId);
          if (excludeIds.has(idStr)) {
            continue;
          }
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
          throw new DuplicateKeyError(
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

  /**
   * Get all geo indexes on this collection.
   * @returns Array of geo index info objects
   */
  async getGeoIndexes(): Promise<GeoIndexInfo[]> {
    const indexes = await this.loadIndexes();
    const result: GeoIndexInfo[] = [];

    for (const idx of indexes) {
      for (const [field, direction] of Object.entries(idx.key)) {
        if (direction === '2d' || direction === '2dsphere') {
          result.push({
            field,
            type: direction,
            indexName: idx.name,
          });
        }
      }
    }

    return result;
  }

  /**
   * Check if a field has a geo index.
   * @param field - Field name to check
   * @returns The geo index type ("2d" or "2dsphere") or null if no geo index exists
   */
  async hasGeoIndex(field: string): Promise<'2d' | '2dsphere' | null> {
    const geoIndexes = await this.getGeoIndexes();
    const geoIndex = geoIndexes.find((g) => g.field === field);
    return geoIndex?.type ?? null;
  }

  /**
   * Get the first geo-indexed field in the collection.
   * Used by $geoNear when 'key' is not specified.
   * @returns The field name and type, or null if no geo index exists
   */
  async getDefaultGeoField(): Promise<GeoIndexInfo | null> {
    const geoIndexes = await this.getGeoIndexes();
    return geoIndexes.length > 0 ? geoIndexes[0] : null;
  }

  /**
   * Validate that a geo index exists for a given field.
   * Throws an error if no geo index is found.
   * @param field - Field name to validate
   * @param operator - Operator name for error message (e.g., "$near", "$geoNear")
   * @throws Error if no geo index exists on the field
   */
  async requireGeoIndex(field: string, operator: string): Promise<'2d' | '2dsphere'> {
    const indexType = await this.hasGeoIndex(field);
    if (!indexType) {
      throw new Error(
        `error processing query: ns=${this.dbName}.${this.collectionName}: ` +
          `${operator} requires a 2d or 2dsphere index on field '${field}'`
      );
    }
    return indexType;
  }
}
