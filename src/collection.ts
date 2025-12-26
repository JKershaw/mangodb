/**
 * MangoCollection - File-based MongoDB-compatible collection.
 *
 * This module provides the main collection class that uses extracted modules:
 * - types.ts: Type definitions
 * - document-utils.ts: Serialization, path access, cloning
 * - query-matcher.ts: Query matching logic
 * - update-operators.ts: Update operations
 * - index-manager.ts: Index management
 */
import { ObjectId } from "bson";
import { MangoCursor, IndexCursor } from "./cursor.ts";
import { AggregationCursor, type AggregationDbContext } from "./aggregation/index.ts";
import { applyProjection, compareValuesForSort } from "./utils.ts";
import { readFile, writeFile, mkdir, unlink, rename as renameFile, access, stat } from "node:fs/promises";
import { join, dirname } from "node:path";

import type {
  Document,
  Filter,
  InsertOneResult,
  InsertManyResult,
  DeleteResult,
  UpdateResult,
  UpdateOptions,
  FindOptions,
  UpdateOperators,
  FindOneAndDeleteOptions,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions,
  BulkWriteOperation,
  BulkWriteOptions,
  BulkWriteResult,
  IndexKeySpec,
  CreateIndexOptions,
  IndexInfo,
  PipelineStage,
  AggregateOptions,
  RenameOptions,
  CollectionStats,
} from "./types.ts";

import {
  serializeDocument,
  deserializeDocument,
  getValueByPath,
  setValueByPath,
  documentsEqual,
} from "./document-utils.ts";

import { matchesFilter } from "./query-matcher.ts";

import {
  applyUpdateOperators,
  createDocumentFromFilter,
  validateReplacement,
  type PositionalContext,
} from "./update-operators.ts";

import { IndexManager } from "./index-manager.ts";

import {
  TextIndexRequiredError,
  NamespaceNotFoundError,
  TargetNamespaceExistsError,
  IllegalOperationError,
  InvalidNamespaceError,
} from "./errors.ts";

import {
  hasNearQuery,
  extractNearQuery,
  evaluateNear,
  extractPointFromDocument,
  GeoIndexRequiredError,
} from "./geo/index.ts";

// Re-export types for backward compatibility
export type { IndexKeySpec, CreateIndexOptions, IndexInfo };

/**
 * MangoCollection - A file-based MongoDB-compatible collection.
 *
 * This class provides MongoDB Collection API methods backed by JSON file storage.
 * It supports standard CRUD operations, indexing, bulk writes, and advanced queries
 * with full compatibility with the official MongoDB driver API.
 *
 * @template T - The document type for this collection, must extend Document
 *
 * @example
 * ```typescript
 * const collection = new MangoCollection<User>('./data', 'mydb', 'users');
 * await collection.insertOne({ name: 'John', age: 30 });
 * const user = await collection.findOne({ name: 'John' });
 * ```
 */
export class MangoCollection<T extends Document = Document> {
  private readonly filePath: string;
  private readonly indexManager: IndexManager;
  private readonly dataDir: string;
  private readonly dbName: string;

  /**
   * Create a new MangoCollection instance.
   *
   * @param dataDir - Base directory for data storage
   * @param dbName - Database name
   * @param collectionName - Collection name
   *
   * @example
   * ```typescript
   * const collection = new MangoCollection('./data', 'mydb', 'users');
   * ```
   */
  constructor(dataDir: string, dbName: string, collectionName: string) {
    this.dataDir = dataDir;
    this.dbName = dbName;
    this.filePath = join(dataDir, dbName, `${collectionName}.json`);
    const indexFilePath = join(dataDir, dbName, `${collectionName}.indexes.json`);
    this.indexManager = new IndexManager(indexFilePath, dbName, collectionName);
  }

  // ==================== Private Helpers ====================

  private async readDocuments(): Promise<T[]> {
    try {
      const content = await readFile(this.filePath, "utf-8");
      const parsed = JSON.parse(content);
      return parsed.map((doc: Record<string, unknown>) => deserializeDocument<T>(doc));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return [];
      }
      throw error;
    }
  }

  private async writeDocuments(documents: T[]): Promise<void> {
    await mkdir(dirname(this.filePath), { recursive: true });
    const serialized = documents.map((doc) => serializeDocument(doc));
    await writeFile(this.filePath, JSON.stringify(serialized, null, 2));
  }

  private sortDocuments(docs: T[], sortSpec: Record<string, 1 | -1>): T[] {
    const sortFields = Object.entries(sortSpec) as [string, 1 | -1][];
    return [...docs].sort((a, b) => {
      for (const [field, direction] of sortFields) {
        const aValue = getValueByPath(a, field);
        const bValue = getValueByPath(b, field);
        const comparison = compareValuesForSort(aValue, bValue, direction);
        if (comparison !== 0) {
          return direction === 1 ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  /**
   * Parse a text search string into include terms, exclude terms, and phrases.
   */
  private parseTextSearch(searchString: string): {
    includeTerms: string[];
    excludeTerms: string[];
    phrases: string[];
  } {
    const includeTerms: string[] = [];
    const excludeTerms: string[] = [];
    const phrases: string[] = [];

    // Extract phrases in quotes first
    const phraseRegex = /"([^"]*)"/g;
    let match;
    let remaining = searchString;

    while ((match = phraseRegex.exec(searchString)) !== null) {
      // Only add non-empty phrases
      if (match[1].trim().length > 0) {
        phrases.push(match[1]);
      }
      remaining = remaining.replace(match[0], " ");
    }

    // Process remaining terms
    const tokens = remaining.split(/\s+/).filter((t) => t.length > 0);
    for (const token of tokens) {
      if (token.startsWith("-") && token.length > 1) {
        excludeTerms.push(token.slice(1));
      } else if (!token.startsWith("-")) {
        includeTerms.push(token);
      }
    }

    return { includeTerms, excludeTerms, phrases };
  }

  /**
   * Tokenize text into words for matching.
   */
  private tokenizeText(text: string): string[] {
    return text.split(/[\s\-_.,;:!?'"()\[\]{}]+/).filter((t) => t.length > 0);
  }

  /**
   * Check if a document matches a text search query and calculate score.
   * Supports:
   * - Multiple terms (OR matching)
   * - Phrases in quotes (exact phrase matching)
   * - Negation with minus prefix
   * - Case sensitivity option
   *
   * @param doc - Document to check
   * @param searchString - Text to search for
   * @param textFields - Fields that are text-indexed
   * @param caseSensitive - Whether to match case-sensitively
   * @returns Score (0 if no match, positive if match)
   */
  private matchesTextSearch(
    doc: T,
    searchString: string,
    textFields: string[],
    caseSensitive: boolean = false
  ): number {
    // Empty search string matches nothing
    if (!searchString || searchString.trim() === "") {
      return 0;
    }

    const { includeTerms, excludeTerms, phrases } = this.parseTextSearch(searchString);

    // If nothing to search for, no match
    if (includeTerms.length === 0 && phrases.length === 0) {
      return 0;
    }

    // Collect all text content from indexed fields
    const allTextContent: string[] = [];
    for (const field of textFields) {
      const value = getValueByPath(doc, field);
      if (typeof value === "string") {
        allTextContent.push(value);
      }
    }

    if (allTextContent.length === 0) {
      return 0;
    }

    const fullText = allTextContent.join(" ");
    const normalizedFullText = caseSensitive ? fullText : fullText.toLowerCase();
    const textTokens = this.tokenizeText(normalizedFullText);

    // Check exclusions first - if any excluded term is found, no match
    for (const term of excludeTerms) {
      const normalizedTerm = caseSensitive ? term : term.toLowerCase();
      if (textTokens.some((t) => t === normalizedTerm || t.includes(normalizedTerm))) {
        return 0;
      }
    }

    // Check phrases - all phrases must match
    for (const phrase of phrases) {
      const normalizedPhrase = caseSensitive ? phrase : phrase.toLowerCase();
      if (!normalizedFullText.includes(normalizedPhrase)) {
        return 0;
      }
    }

    // Check include terms - at least one must match (OR logic)
    let score = 0;
    const totalTerms = includeTerms.length + phrases.length;

    for (const term of includeTerms) {
      const normalizedTerm = caseSensitive ? term : term.toLowerCase();
      // Count occurrences for scoring
      const matches = textTokens.filter(
        (t) => t === normalizedTerm || t.includes(normalizedTerm)
      ).length;
      if (matches > 0) {
        score += matches;
      }
    }

    // Add phrase matches to score
    for (const phrase of phrases) {
      const normalizedPhrase = caseSensitive ? phrase : phrase.toLowerCase();
      // Use 'g' flag only; case sensitivity is already handled by normalizing text
      const regexFlags = caseSensitive ? "g" : "gi";
      const regex = new RegExp(normalizedPhrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), regexFlags);
      const matches = (normalizedFullText.match(regex) || []).length;
      score += matches * 2; // Weight phrases higher
    }

    // If we have include terms but none matched, no match
    if (includeTerms.length > 0 && score === 0) {
      return 0;
    }

    // Normalize score by total text length for fairer comparison
    return score / Math.max(1, Math.sqrt(textTokens.length));
  }

  /**
   * Symbol for storing text scores on documents (internal use).
   * Shared with cursor.ts via Symbol.for() for cross-module access.
   */
  private static readonly TEXT_SCORE_KEY = Symbol.for("mangodb.textScore");

  /**
   * Filter documents with $text query support.
   * Handles $text operator and delegates to matchesFilter for other conditions.
   * Attaches text scores to documents for $meta projection.
   *
   * @param documents - Documents to filter
   * @param filter - Query filter that may contain $text
   * @returns Filtered documents with text scores attached
   * @throws TextIndexRequiredError if $text is used without a text index
   */
  private async filterWithTextSupport(documents: T[], filter: Filter<T>): Promise<T[]> {
    const textQuery = (filter as Record<string, unknown>).$text as
      | { $search?: string; $caseSensitive?: boolean }
      | undefined;

    if (textQuery) {
      // Get text index fields
      const textFields = await this.indexManager.getTextIndexFields();
      if (textFields.length === 0) {
        throw new TextIndexRequiredError();
      }

      const searchString = textQuery.$search || "";
      const caseSensitive = textQuery.$caseSensitive || false;

      // Create a filter without $text for additional conditions
      const remainingFilter = { ...filter } as Record<string, unknown>;
      delete remainingFilter.$text;

      // Apply both text search and regular filter, storing scores
      const results: T[] = [];
      for (const doc of documents) {
        const score = this.matchesTextSearch(doc, searchString, textFields, caseSensitive);
        if (score > 0) {
          const matchesOther =
            Object.keys(remainingFilter).length === 0 ||
            matchesFilter(doc, remainingFilter as Filter<T>);
          if (matchesOther) {
            // Create a shallow copy to avoid mutating the original document
            // and attach score to the copy using Object.defineProperty
            const docWithScore = { ...doc };
            Object.defineProperty(docWithScore, MangoCollection.TEXT_SCORE_KEY, {
              value: score,
              enumerable: false,
              writable: false,
            });
            results.push(docWithScore);
          }
        }
      }
      return results;
    }

    return documents.filter((doc) => matchesFilter(doc, filter));
  }

  // ==================== Index Operations ====================

  /**
   * Create an index on the collection.
   *
   * Indexes can improve query performance and enforce unique constraints.
   * Multiple indexes can be created on a single collection.
   *
   * @param keySpec - The fields to index, e.g., { name: 1 } for ascending or { age: -1 } for descending
   * @param options - Index creation options, including unique constraint
   * @returns The name of the created index
   * @throws DuplicateKeyError if unique constraint is violated by existing documents
   *
   * @example
   * ```typescript
   * // Create a simple index
   * await collection.createIndex({ name: 1 });
   *
   * // Create a unique index
   * await collection.createIndex({ email: 1 }, { unique: true });
   *
   * // Create a compound index
   * await collection.createIndex({ lastName: 1, firstName: 1 });
   * ```
   */
  async createIndex(
    keySpec: IndexKeySpec,
    options: CreateIndexOptions = {}
  ): Promise<string> {
    return this.indexManager.createIndex(keySpec, options);
  }

  /**
   * Drop an index from the collection.
   *
   * @param indexNameOrSpec - Either the index name (string) or the key specification object
   * @throws Error if the index does not exist
   *
   * @example
   * ```typescript
   * // Drop by name
   * await collection.dropIndex('name_1');
   *
   * // Drop by specification
   * await collection.dropIndex({ name: 1 });
   * ```
   */
  async dropIndex(indexNameOrSpec: string | IndexKeySpec): Promise<void> {
    return this.indexManager.dropIndex(indexNameOrSpec);
  }

  /**
   * Create multiple indexes on the collection.
   *
   * @param indexSpecs - Array of index specifications, each with key and optional options
   * @returns Array of created index names
   *
   * @example
   * ```typescript
   * const names = await collection.createIndexes([
   *   { key: { email: 1 }, unique: true },
   *   { key: { lastName: 1, firstName: 1 } },
   *   { key: { createdAt: 1 }, expireAfterSeconds: 3600 }
   * ]);
   * ```
   */
  async createIndexes(
    indexSpecs: Array<{ key: IndexKeySpec } & CreateIndexOptions>
  ): Promise<string[]> {
    const names: string[] = [];
    for (const spec of indexSpecs) {
      const { key, ...options } = spec;
      const name = await this.indexManager.createIndex(key, options);
      names.push(name);
    }
    return names;
  }

  /**
   * Drop all indexes on the collection except _id.
   *
   * When called with no arguments, drops all non-_id indexes.
   * When called with "*", drops all non-_id indexes.
   * When called with an array of names, drops those specific indexes.
   *
   * @param indexNames - Optional: "*" to drop all, or array of index names
   *
   * @example
   * ```typescript
   * // Drop all indexes except _id
   * await collection.dropIndexes();
   *
   * // Drop all indexes except _id (explicit)
   * await collection.dropIndexes("*");
   *
   * // Drop specific indexes
   * await collection.dropIndexes(["email_1", "name_1"]);
   * ```
   */
  async dropIndexes(indexNames?: "*" | string[]): Promise<void> {
    const indexes = await this.indexManager.indexes();

    if (indexNames === "*" || indexNames === undefined) {
      // Drop all non-_id indexes
      for (const idx of indexes) {
        if (idx.name !== "_id_") {
          await this.indexManager.dropIndex(idx.name);
        }
      }
    } else {
      // Drop specific indexes
      for (const name of indexNames) {
        await this.indexManager.dropIndex(name);
      }
    }
  }

  /**
   * Get all indexes on the collection.
   *
   * @returns Array of index information objects
   *
   * @example
   * ```typescript
   * const indexes = await collection.indexes();
   * console.log(indexes); // [{ name: 'name_1', key: { name: 1 }, unique: false }, ...]
   * ```
   */
  async indexes(): Promise<IndexInfo[]> {
    return this.indexManager.indexes();
  }

  /**
   * List all indexes on the collection using a cursor.
   *
   * @returns An IndexCursor for iterating through indexes
   *
   * @example
   * ```typescript
   * const cursor = collection.listIndexes();
   * const indexes = await cursor.toArray();
   * ```
   */
  listIndexes(): IndexCursor {
    return new IndexCursor(() => this.indexManager.indexes());
  }

  // ==================== Insert Operations ====================

  /**
   * Insert a single document into the collection.
   *
   * If the document does not have an _id field, one will be automatically generated.
   * The operation will fail if a unique index constraint is violated.
   *
   * @param doc - The document to insert
   * @returns Result containing the inserted document's ObjectId
   * @throws DuplicateKeyError if a unique constraint is violated
   *
   * @example
   * ```typescript
   * const result = await collection.insertOne({ name: 'John', age: 30 });
   * console.log(result.insertedId); // ObjectId('...')
   *
   * // Insert with custom _id
   * await collection.insertOne({ _id: new ObjectId(), name: 'Jane' });
   * ```
   */
  async insertOne(doc: T): Promise<InsertOneResult> {
    const documents = await this.readDocuments();

    const docWithId = { ...doc };
    if (!("_id" in docWithId)) {
      (docWithId as Record<string, unknown>)._id = new ObjectId();
    }

    await this.indexManager.checkUniqueConstraints([docWithId], documents);

    documents.push(docWithId);
    await this.writeDocuments(documents);

    return {
      acknowledged: true,
      insertedId: (docWithId as unknown as { _id: ObjectId })._id,
    };
  }

  /**
   * Insert multiple documents into the collection.
   *
   * Documents without an _id field will have one automatically generated.
   * All documents are validated against unique constraints before insertion.
   *
   * @param docs - Array of documents to insert
   * @returns Result containing a map of inserted document ObjectIds
   * @throws DuplicateKeyError if any unique constraint is violated
   *
   * @example
   * ```typescript
   * const result = await collection.insertMany([
   *   { name: 'John', age: 30 },
   *   { name: 'Jane', age: 25 }
   * ]);
   * console.log(result.insertedIds); // { 0: ObjectId('...'), 1: ObjectId('...') }
   * console.log(result.insertedCount); // 2
   * ```
   */
  async insertMany(docs: T[]): Promise<InsertManyResult> {
    const documents = await this.readDocuments();
    const insertedIds: Record<number, ObjectId> = {};
    const docsWithIds: T[] = [];

    for (let i = 0; i < docs.length; i++) {
      const docWithId = { ...docs[i] };
      if (!("_id" in docWithId)) {
        (docWithId as Record<string, unknown>)._id = new ObjectId();
      }
      docsWithIds.push(docWithId);
      insertedIds[i] = (docWithId as unknown as { _id: ObjectId })._id;
    }

    await this.indexManager.checkUniqueConstraints(docsWithIds, documents);

    documents.push(...docsWithIds);
    await this.writeDocuments(documents);

    return {
      acknowledged: true,
      insertedIds,
    };
  }

  // ==================== Query Operations ====================

  /**
   * Find a single document matching the filter.
   *
   * Returns the first document that matches the filter criteria.
   * If no document matches, returns null. Supports sorting to control
   * which document is returned when multiple match, and skip to return
   * the Nth matching document.
   *
   * @param filter - Query filter to match documents (default: empty object matches all)
   * @param options - Query options including projection, sort, and skip
   * @returns The matching document or null if not found
   *
   * @example
   * ```typescript
   * // Find by field value
   * const user = await collection.findOne({ name: 'John' });
   *
   * // Find with projection
   * const user = await collection.findOne(
   *   { age: { $gte: 18 } },
   *   { projection: { name: 1, age: 1 } }
   * );
   *
   * // Find by _id
   * const user = await collection.findOne({ _id: new ObjectId('...') });
   *
   * // Get the most recent active order (sort)
   * const latest = await collection.findOne(
   *   { status: 'active' },
   *   { sort: { createdAt: -1 } }
   * );
   *
   * // Get the second-highest scorer (sort + skip)
   * const runnerUp = await collection.findOne(
   *   { tournament: 'finals' },
   *   { sort: { score: -1 }, skip: 1 }
   * );
   * ```
   */
  async findOne(filter: Filter<T> = {}, options: FindOptions = {}): Promise<T | null> {
    const documents = await this.readDocuments();
    let filtered = await this.filterWithTextSupport(documents, filter);

    if (filtered.length === 0) {
      return null;
    }

    // Apply sort if specified
    if (options.sort) {
      filtered = this.sortDocuments(filtered, options.sort);
    }

    // Apply skip if specified
    if (options.skip && options.skip > 0) {
      filtered = filtered.slice(options.skip);
    }

    if (filtered.length === 0) {
      return null;
    }

    const doc = filtered[0];
    if (options.projection) {
      return applyProjection(doc, options.projection);
    }
    return doc;
  }

  /**
   * Find all documents matching the filter.
   *
   * Returns a cursor that can be used to iterate through matching documents.
   * The cursor supports methods like toArray(), sort(), limit(), skip(), and hint().
   *
   * @param filter - Query filter to match documents (default: empty object matches all)
   * @param options - Query options including projection
   * @returns A MangoCursor for iterating through matching documents
   *
   * @example
   * ```typescript
   * // Find all documents
   * const cursor = collection.find();
   * const allDocs = await cursor.toArray();
   *
   * // Find with filter
   * const adults = await collection.find({ age: { $gte: 18 } }).toArray();
   *
   * // Find with sorting and limiting
   * const topUsers = await collection.find({ active: true })
   *   .sort({ score: -1 })
   *   .limit(10)
   *   .toArray();
   *
   * // Find with projection
   * const names = await collection.find({}, { projection: { name: 1 } }).toArray();
   *
   * // Find with index hint
   * const results = await collection.find({ email: 'test@test.com' })
   *   .hint('email_1')
   *   .toArray();
   * ```
   */
  find(filter: Filter<T> = {}, options: FindOptions = {}): MangoCursor<T> {
    // Create hint validator that checks if the specified index exists
    const hintValidator = async (hint: string | Record<string, unknown>): Promise<boolean> => {
      const indexes = await this.indexManager.indexes();

      if (typeof hint === "string") {
        // Hint by index name
        return indexes.some((idx) => idx.name === hint);
      } else {
        // Hint by key pattern - generate name and check
        const hintName = this.indexManager.generateIndexName(
          hint as Record<string, 1 | -1 | "text">
        );
        return indexes.some((idx) => idx.name === hintName);
      }
    };

    // Check if this is a $near or $nearSphere query
    const nearQuery = extractNearQuery(filter as Record<string, unknown>);

    if (nearQuery) {
      // Handle $near/$nearSphere query with special geo processing
      return new MangoCursor<T>(
        async () => {
          return this.executeNearQuery(
            nearQuery.geoField,
            nearQuery.nearSpec,
            nearQuery.spherical,
            nearQuery.remainingFilter as Filter<T>
          );
        },
        options.projection || null,
        hintValidator,
        { geoSorted: true }
      );
    }

    return new MangoCursor<T>(
      async () => {
        const documents = await this.readDocuments();
        return this.filterWithTextSupport(documents, filter);
      },
      options.projection || null,
      hintValidator
    );
  }

  /**
   * Execute a $near or $nearSphere query.
   * Requires a geo index on the field and returns documents sorted by distance.
   */
  private async executeNearQuery(
    geoField: string,
    nearSpec: unknown,
    spherical: boolean,
    remainingFilter: Filter<T>
  ): Promise<T[]> {
    // Verify geo index exists on the field
    const indexType = await this.indexManager.hasGeoIndex(geoField);
    if (!indexType) {
      throw new GeoIndexRequiredError(spherical ? "$nearSphere" : "$near");
    }

    // Use spherical calculations for 2dsphere indexes regardless of operator
    const useSpherical = indexType === "2dsphere" || spherical;

    // Read all documents
    const documents = await this.readDocuments();

    // Apply remaining filter first
    const filtered = Object.keys(remainingFilter).length > 0
      ? await this.filterWithTextSupport(documents, remainingFilter)
      : documents;

    // Calculate distances and filter
    const withDistances: Array<{ doc: T; distance: number }> = [];

    for (const doc of filtered) {
      const docValue = getValueByPath(doc, geoField);
      if (docValue === undefined) continue;

      const result = evaluateNear(docValue, nearSpec, useSpherical);
      if (result.matches) {
        withDistances.push({ doc, distance: result.distance });
      }
    }

    // Sort by distance ascending
    withDistances.sort((a, b) => a.distance - b.distance);

    return withDistances.map((item) => item.doc);
  }

  /**
   * Execute an aggregation pipeline on the collection.
   *
   * The aggregation pipeline processes documents through a sequence of stages,
   * each transforming the document stream. Stages are executed in order.
   *
   * Supported stages:
   * - $match: Filter documents using query syntax
   * - $project: Include, exclude, or rename fields
   * - $sort: Order documents by field values
   * - $limit: Limit the number of documents
   * - $skip: Skip the first n documents
   * - $count: Count documents and output a single document
   * - $unwind: Deconstruct array fields into multiple documents
   *
   * @param pipeline - Array of pipeline stages to execute
   * @param options - Aggregation options (reserved for future use)
   * @returns An AggregationCursor for iterating through results
   *
   * @example
   * ```typescript
   * // Filter and sort
   * const results = await collection.aggregate([
   *   { $match: { status: "active" } },
   *   { $sort: { createdAt: -1 } },
   *   { $limit: 10 }
   * ]).toArray();
   *
   * // Project specific fields
   * const names = await collection.aggregate([
   *   { $project: { name: 1, email: 1, _id: 0 } }
   * ]).toArray();
   *
   * // Count documents
   * const countResult = await collection.aggregate([
   *   { $match: { age: { $gte: 18 } } },
   *   { $count: "adultCount" }
   * ]).toArray();
   *
   * // Unwind arrays
   * const unwound = await collection.aggregate([
   *   { $unwind: "$tags" }
   * ]).toArray();
   * ```
   */
  aggregate(
    pipeline: PipelineStage[],
    _options?: AggregateOptions
  ): AggregationCursor<T> {
    // Create database context for $lookup, $out, and $geoNear stages
    const dbContext: AggregationDbContext = {
      getCollection: (name: string) => {
        return new MangoCollection(this.dataDir, this.dbName, name);
      },
      getGeoIndexes: async () => {
        return this.indexManager.getGeoIndexes();
      },
    };

    return new AggregationCursor<T>(
      () => this.readDocuments(),
      pipeline,
      dbContext
    );
  }

  /**
   * Count the number of documents matching the filter.
   *
   * @param filter - Query filter to match documents (default: empty object matches all)
   * @returns The number of matching documents
   *
   * @example
   * ```typescript
   * // Count all documents
   * const total = await collection.countDocuments();
   *
   * // Count with filter
   * const adultCount = await collection.countDocuments({ age: { $gte: 18 } });
   * ```
   */
  async countDocuments(filter: Filter<T> = {}): Promise<number> {
    const documents = await this.readDocuments();
    const filtered = await this.filterWithTextSupport(documents, filter);
    return filtered.length;
  }

  // ==================== Delete Operations ====================

  /**
   * Delete a single document matching the filter.
   *
   * Deletes the first document that matches the filter criteria.
   * If no document matches, no operation is performed.
   *
   * @param filter - Query filter to match the document to delete
   * @returns Result containing the count of deleted documents (0 or 1)
   *
   * @example
   * ```typescript
   * // Delete by field value
   * const result = await collection.deleteOne({ name: 'John' });
   * console.log(result.deletedCount); // 1 if deleted, 0 if not found
   *
   * // Delete by _id
   * await collection.deleteOne({ _id: new ObjectId('...') });
   * ```
   */
  async deleteOne(filter: Filter<T>): Promise<DeleteResult> {
    const documents = await this.readDocuments();
    let deletedCount = 0;

    const remaining: T[] = [];
    let deleted = false;

    for (const doc of documents) {
      if (!deleted && matchesFilter(doc, filter)) {
        deleted = true;
        deletedCount = 1;
      } else {
        remaining.push(doc);
      }
    }

    await this.writeDocuments(remaining);

    return {
      acknowledged: true,
      deletedCount,
    };
  }

  /**
   * Delete all documents matching the filter.
   *
   * Deletes all documents that match the filter criteria.
   * If no documents match, no operation is performed.
   *
   * @param filter - Query filter to match documents to delete
   * @returns Result containing the count of deleted documents
   *
   * @example
   * ```typescript
   * // Delete all matching documents
   * const result = await collection.deleteMany({ age: { $lt: 18 } });
   * console.log(result.deletedCount); // Number of deleted documents
   *
   * // Delete all documents
   * await collection.deleteMany({});
   * ```
   */
  async deleteMany(filter: Filter<T>): Promise<DeleteResult> {
    const documents = await this.readDocuments();
    const remaining = documents.filter((doc) => !matchesFilter(doc, filter));
    const deletedCount = documents.length - remaining.length;

    await this.writeDocuments(remaining);

    return {
      acknowledged: true,
      deletedCount,
    };
  }

  // ==================== Update Operations ====================

  /**
   * Find matched array indices for positional $ operator.
   * Analyzes the filter to determine which array element indices matched.
   */
  private findMatchedArrayIndices(
    doc: T,
    filter: Filter<T>
  ): Map<string, number> | undefined {
    const matchedIndices = new Map<string, number>();

    for (const [key, condition] of Object.entries(filter)) {
      // Skip _id and logical operators
      if (key === "_id" || key.startsWith("$")) continue;

      // Check for dot notation indicating array element query (e.g., "items.status")
      if (key.includes(".")) {
        const parts = key.split(".");
        let current: unknown = doc;
        let arrayPath = "";

        for (let i = 0; i < parts.length; i++) {
          const part = parts[i];

          if (current === null || current === undefined) break;

          if (Array.isArray(current)) {
            // Found array - check for matching index
            const remainingPath = parts.slice(i).join(".");
            for (let idx = 0; idx < current.length; idx++) {
              const element = current[idx];
              const valueToCheck =
                remainingPath && typeof element === "object" && element !== null
                  ? getValueByPath(element as Record<string, unknown>, remainingPath)
                  : element;

              if (this.valueMatchesCondition(valueToCheck, condition)) {
                // Store the path to the array (without the remaining path)
                matchedIndices.set(arrayPath, idx);
                break;
              }
            }
            break;
          } else if (typeof current === "object") {
            arrayPath = arrayPath ? `${arrayPath}.${part}` : part;
            current = (current as Record<string, unknown>)[part];
          } else {
            break;
          }
        }
      } else {
        // Non-dotted path - check if value is an array with conditions
        const value = getValueByPath(doc as Record<string, unknown>, key);

        if (Array.isArray(value)) {
          // Check for $elemMatch
          if (
            condition &&
            typeof condition === "object" &&
            "$elemMatch" in condition
          ) {
            const elemMatchCond = (condition as { $elemMatch: Record<string, unknown> }).$elemMatch;
            for (let idx = 0; idx < value.length; idx++) {
              const elem = value[idx];
              // For primitive arrays, wrap element for matching
              if (typeof elem !== "object" || elem === null) {
                // Check if elemMatchCond is an operator object like { $gt: 10 }
                if (this.valueMatchesCondition(elem, elemMatchCond)) {
                  matchedIndices.set(key, idx);
                  break;
                }
              } else if (matchesFilter(elem as Document, elemMatchCond)) {
                matchedIndices.set(key, idx);
                break;
              }
            }
          } else {
            // Direct array comparison with operators like $gt, $in, etc.
            for (let idx = 0; idx < value.length; idx++) {
              if (this.valueMatchesCondition(value[idx], condition)) {
                matchedIndices.set(key, idx);
                break;
              }
            }
          }
        }
      }
    }

    return matchedIndices.size > 0 ? matchedIndices : undefined;
  }

  /**
   * Check if a single value matches a filter condition.
   */
  private valueMatchesCondition(value: unknown, condition: unknown): boolean {
    if (condition === null || condition === undefined) {
      return value === condition;
    }

    if (typeof condition === "object" && !Array.isArray(condition)) {
      // Operator-based condition
      const condObj = condition as Record<string, unknown>;
      const keys = Object.keys(condObj);

      // Check if it's an operator object
      if (keys.some((k) => k.startsWith("$"))) {
        return matchesFilter({ value } as Document, { value: condObj });
      }
    }

    // Direct equality
    if (value === condition) return true;

    // Deep equality for objects/arrays
    if (
      typeof value === "object" &&
      typeof condition === "object" &&
      value !== null &&
      condition !== null
    ) {
      return JSON.stringify(value) === JSON.stringify(condition);
    }

    return false;
  }

  private async performUpdate(
    filter: Filter<T>,
    update: UpdateOperators,
    options: UpdateOptions,
    limitOne: boolean
  ): Promise<UpdateResult> {
    const documents = await this.readDocuments();
    let matchedCount = 0;
    let modifiedCount = 0;
    let upsertedId: ObjectId | null = null;
    let upsertedCount = 0;

    const updatedDocuments: T[] = [];
    const modifiedDocs: T[] = [];
    const unchangedDocs: T[] = [];

    // Create context for positional update operators
    const baseContext: PositionalContext = {
      arrayFilters: options.arrayFilters,
    };

    for (const doc of documents) {
      const shouldMatch = limitOne ? matchedCount === 0 : true;

      if (shouldMatch && matchesFilter(doc, filter)) {
        matchedCount++;

        // Track matched array indices for $ positional operator
        const matchedArrayIndex = this.findMatchedArrayIndices(doc, filter);
        const context: PositionalContext = {
          ...baseContext,
          matchedArrayIndex,
        };

        const updatedDoc = applyUpdateOperators(doc, update, context);

        if (!documentsEqual(doc, updatedDoc)) {
          modifiedCount++;
          modifiedDocs.push(updatedDoc);
          updatedDocuments.push(updatedDoc);
        } else {
          unchangedDocs.push(doc);
          updatedDocuments.push(doc);
        }
      } else {
        unchangedDocs.push(doc);
        updatedDocuments.push(doc);
      }
    }

    if (matchedCount === 0 && options.upsert) {
      const baseDoc = createDocumentFromFilter(filter);

      // Apply $setOnInsert fields - only during upsert insert
      if (update.$setOnInsert) {
        for (const [path, value] of Object.entries(update.$setOnInsert)) {
          setValueByPath(baseDoc as Record<string, unknown>, path, value);
        }
      }

      const newDoc = applyUpdateOperators(baseDoc, update, baseContext);

      if (!("_id" in newDoc)) {
        (newDoc as Record<string, unknown>)._id = new ObjectId();
      }

      upsertedId = (newDoc as unknown as { _id: ObjectId })._id;
      upsertedCount = 1;
      modifiedDocs.push(newDoc);
      updatedDocuments.push(newDoc);
    }

    if (modifiedDocs.length > 0) {
      await this.indexManager.checkUniqueConstraints(modifiedDocs, unchangedDocs);
    }

    await this.writeDocuments(updatedDocuments);

    return {
      acknowledged: true,
      matchedCount,
      modifiedCount,
      upsertedCount,
      upsertedId,
    };
  }

  /**
   * Update a single document matching the filter.
   *
   * Updates the first document that matches the filter using update operators.
   * Supports MongoDB update operators like $set, $inc, $push, etc.
   *
   * @param filter - Query filter to match the document to update
   * @param update - Update operations to apply (must use update operators)
   * @param options - Update options including upsert
   * @returns Result containing matched, modified, and upserted counts
   * @throws DuplicateKeyError if upsert violates a unique constraint
   *
   * @example
   * ```typescript
   * // Update with $set
   * const result = await collection.updateOne(
   *   { name: 'John' },
   *   { $set: { age: 31 } }
   * );
   * console.log(result.modifiedCount); // 1 if modified
   *
   * // Update with upsert
   * await collection.updateOne(
   *   { email: 'john@example.com' },
   *   { $set: { name: 'John', age: 30 } },
   *   { upsert: true }
   * );
   *
   * // Multiple operators
   * await collection.updateOne(
   *   { _id: userId },
   *   { $set: { lastLogin: new Date() }, $inc: { loginCount: 1 } }
   * );
   * ```
   */
  async updateOne(
    filter: Filter<T>,
    update: UpdateOperators,
    options: UpdateOptions = {}
  ): Promise<UpdateResult> {
    return this.performUpdate(filter, update, options, true);
  }

  /**
   * Update all documents matching the filter.
   *
   * Updates all documents that match the filter using update operators.
   * Supports MongoDB update operators like $set, $inc, $push, etc.
   *
   * @param filter - Query filter to match documents to update
   * @param update - Update operations to apply (must use update operators)
   * @param options - Update options including upsert
   * @returns Result containing matched, modified, and upserted counts
   * @throws DuplicateKeyError if upsert violates a unique constraint
   *
   * @example
   * ```typescript
   * // Update multiple documents
   * const result = await collection.updateMany(
   *   { status: 'pending' },
   *   { $set: { status: 'active' } }
   * );
   * console.log(result.modifiedCount); // Number of modified documents
   *
   * // Increment all matching documents
   * await collection.updateMany(
   *   { active: true },
   *   { $inc: { points: 10 } }
   * );
   * ```
   */
  async updateMany(
    filter: Filter<T>,
    update: UpdateOperators,
    options: UpdateOptions = {}
  ): Promise<UpdateResult> {
    return this.performUpdate(filter, update, options, false);
  }

  /**
   * Replace a single document matching the filter.
   *
   * Finds the first document matching the filter and replaces it entirely with
   * the replacement document. The _id field is preserved from the original document.
   *
   * @param filter - Query filter to match the document
   * @param replacement - Replacement document (must not contain update operators)
   * @param options - Options including upsert
   * @returns Result containing matchedCount, modifiedCount, and optionally upsertedId
   * @throws Error if replacement contains update operators
   * @throws DuplicateKeyError if replacement or upsert violates a unique constraint
   *
   * @example
   * ```typescript
   * // Replace a document
   * const result = await collection.replaceOne(
   *   { name: 'John' },
   *   { name: 'John Doe', age: 31, email: 'john@example.com' }
   * );
   *
   * // Replace with upsert
   * const result = await collection.replaceOne(
   *   { name: 'New User' },
   *   { name: 'New User', age: 25 },
   *   { upsert: true }
   * );
   * ```
   */
  async replaceOne(
    filter: Filter<T>,
    replacement: T,
    options: { upsert?: boolean } = {}
  ): Promise<UpdateResult> {
    validateReplacement(replacement);

    const documents = await this.readDocuments();
    const matches = documents.filter((doc) => matchesFilter(doc, filter));

    if (matches.length === 0) {
      if (options.upsert) {
        const newDoc = { ...replacement } as T;
        if (!("_id" in newDoc)) {
          (newDoc as Record<string, unknown>)._id = new ObjectId();
        }
        await this.indexManager.checkUniqueConstraints([newDoc], documents);
        documents.push(newDoc);
        await this.writeDocuments(documents);

        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 1,
          upsertedId: (newDoc as unknown as { _id: ObjectId })._id,
        };
      }
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
        upsertedId: null,
      };
    }

    const docToReplace = matches[0];
    const originalId = (docToReplace as { _id?: unknown })._id;
    const newDoc = { ...replacement } as T;
    if (originalId !== undefined) {
      (newDoc as Record<string, unknown>)._id = originalId;
    }

    // Convert _id to string for unique constraint check
    // Use explicit undefined check to handle falsy _id values like 0 or ""
    const getIdString = (id: unknown): string => {
      if (id === undefined) return "";
      if (typeof (id as ObjectId).toHexString === "function") {
        return (id as ObjectId).toHexString();
      }
      return String(id);
    };
    const idString = getIdString(originalId);

    await this.indexManager.checkUniqueConstraints(
      [newDoc],
      documents,
      originalId !== undefined ? new Set([idString]) : new Set()
    );

    const idx = documents.findIndex((doc) => {
      const id = (doc as { _id?: unknown })._id;
      if (id === undefined || originalId === undefined) return false;
      // Handle both ObjectId and primitive _id values
      if (typeof (id as ObjectId).equals === "function" && typeof (originalId as ObjectId).equals === "function") {
        return (id as ObjectId).equals(originalId as ObjectId);
      }
      return id === originalId;
    });

    if (idx !== -1) {
      documents[idx] = newDoc;
    }

    await this.writeDocuments(documents);

    return {
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1,
      upsertedCount: 0,
      upsertedId: null,
    };
  }

  // ==================== FindOneAnd* Operations ====================

  /**
   * Find a single document and delete it.
   *
   * Finds the first document matching the filter, deletes it, and returns it.
   * Returns null if no document matches. Supports sorting to control which
   * document is selected when multiple match.
   *
   * @param filter - Query filter to match the document
   * @param options - Options including sort and projection
   * @returns The deleted document or null if not found
   * @throws Error if the document doesn't have an _id field
   *
   * @example
   * ```typescript
   * // Delete and return document
   * const deletedUser = await collection.findOneAndDelete({ name: 'John' });
   *
   * // Delete with sorting (deletes the oldest)
   * const oldest = await collection.findOneAndDelete(
   *   { status: 'inactive' },
   *   { sort: { createdAt: 1 } }
   * );
   *
   * // Delete with projection
   * const user = await collection.findOneAndDelete(
   *   { _id: userId },
   *   { projection: { name: 1, email: 1 } }
   * );
   * ```
   */
  async findOneAndDelete(
    filter: Filter<T>,
    options: FindOneAndDeleteOptions = {}
  ): Promise<T | null> {
    const documents = await this.readDocuments();

    let matches = documents.filter((doc) => matchesFilter(doc, filter));

    if (matches.length === 0) {
      return null;
    }

    if (options.sort) {
      matches = this.sortDocuments(matches, options.sort);
    }

    const docToDelete = matches[0];
    const docId = (docToDelete as { _id?: ObjectId })._id;

    if (!docId) {
      throw new Error("Cannot delete document without _id");
    }

    const remaining = documents.filter((doc) => {
      const id = (doc as { _id?: ObjectId })._id;
      return !id || !id.equals(docId);
    });

    await this.writeDocuments(remaining);

    if (options.projection) {
      return applyProjection(docToDelete, options.projection);
    }

    return docToDelete;
  }

  /**
   * Find a single document and replace it.
   *
   * Finds the first document matching the filter and replaces it entirely with
   * the replacement document. The _id field is preserved from the original document.
   * By default, returns the original document; use returnDocument: 'after' to return
   * the replacement document.
   *
   * @param filter - Query filter to match the document
   * @param replacement - Replacement document (must not contain update operators)
   * @param options - Options including upsert, sort, projection, and returnDocument
   * @returns The original or replaced document, or null if not found and no upsert
   * @throws Error if replacement contains update operators
   * @throws DuplicateKeyError if replacement or upsert violates a unique constraint
   *
   * @example
   * ```typescript
   * // Replace and return original
   * const original = await collection.findOneAndReplace(
   *   { name: 'John' },
   *   { name: 'John Doe', age: 31, email: 'john@example.com' }
   * );
   *
   * // Replace and return new document
   * const updated = await collection.findOneAndReplace(
   *   { name: 'John' },
   *   { name: 'John Doe', age: 31 },
   *   { returnDocument: 'after' }
   * );
   *
   * // Replace with upsert
   * const result = await collection.findOneAndReplace(
   *   { email: 'jane@example.com' },
   *   { name: 'Jane', email: 'jane@example.com' },
   *   { upsert: true, returnDocument: 'after' }
   * );
   * ```
   */
  async findOneAndReplace(
    filter: Filter<T>,
    replacement: T,
    options: FindOneAndReplaceOptions = {}
  ): Promise<T | null> {
    validateReplacement(replacement);

    const documents = await this.readDocuments();
    const returnAfter = options.returnDocument === "after";

    let matches = documents.filter((doc) => matchesFilter(doc, filter));

    if (matches.length === 0) {
      if (options.upsert) {
        const newDoc = { ...replacement } as T;
        if (!("_id" in newDoc)) {
          (newDoc as Record<string, unknown>)._id = new ObjectId();
        }

        await this.indexManager.checkUniqueConstraints([newDoc], documents);

        documents.push(newDoc);
        await this.writeDocuments(documents);

        if (returnAfter) {
          return options.projection
            ? applyProjection(newDoc, options.projection)
            : newDoc;
        }
        return null;
      }
      return null;
    }

    if (options.sort) {
      matches = this.sortDocuments(matches, options.sort);
    }

    const docToReplace = matches[0];
    const originalId = (docToReplace as { _id?: ObjectId })._id;

    const newDoc = { ...replacement } as T;
    if (originalId) {
      (newDoc as Record<string, unknown>)._id = originalId;
    }

    const updatedDocuments: T[] = [];
    const unchangedDocs: T[] = [];
    let replaced = false;

    for (const doc of documents) {
      const id = (doc as { _id?: ObjectId })._id;
      if (!replaced && id && originalId && id.equals(originalId)) {
        updatedDocuments.push(newDoc);
        replaced = true;
      } else {
        updatedDocuments.push(doc);
        unchangedDocs.push(doc);
      }
    }

    await this.indexManager.checkUniqueConstraints([newDoc], unchangedDocs);

    await this.writeDocuments(updatedDocuments);

    const resultDoc = returnAfter ? newDoc : docToReplace;
    if (options.projection) {
      return applyProjection(resultDoc, options.projection);
    }

    return resultDoc;
  }

  /**
   * Find a single document and update it.
   *
   * Finds the first document matching the filter and updates it using update operators.
   * By default, returns the original document; use returnDocument: 'after' to return
   * the updated document. Supports sorting to control which document is selected.
   *
   * @param filter - Query filter to match the document
   * @param update - Update operations to apply (must use update operators)
   * @param options - Options including upsert, sort, projection, and returnDocument
   * @returns The original or updated document, or null if not found and no upsert
   * @throws DuplicateKeyError if update or upsert violates a unique constraint
   *
   * @example
   * ```typescript
   * // Update and return original
   * const original = await collection.findOneAndUpdate(
   *   { name: 'John' },
   *   { $set: { age: 31 } }
   * );
   *
   * // Update and return new document
   * const updated = await collection.findOneAndUpdate(
   *   { name: 'John' },
   *   { $inc: { loginCount: 1 }, $set: { lastLogin: new Date() } },
   *   { returnDocument: 'after' }
   * );
   *
   * // Update with upsert
   * const result = await collection.findOneAndUpdate(
   *   { email: 'john@example.com' },
   *   { $set: { name: 'John', lastLogin: new Date() } },
   *   { upsert: true, returnDocument: 'after' }
   * );
   *
   * // Update with sorting (updates the newest)
   * const newest = await collection.findOneAndUpdate(
   *   { status: 'pending' },
   *   { $set: { status: 'processing' } },
   *   { sort: { createdAt: -1 }, returnDocument: 'after' }
   * );
   * ```
   */
  async findOneAndUpdate(
    filter: Filter<T>,
    update: UpdateOperators,
    options: FindOneAndUpdateOptions = {}
  ): Promise<T | null> {
    const documents = await this.readDocuments();
    const returnAfter = options.returnDocument === "after";

    let matches = documents.filter((doc) => matchesFilter(doc, filter));

    if (matches.length === 0) {
      if (options.upsert) {
        const baseDoc = createDocumentFromFilter(filter);

        // Apply $setOnInsert fields - only during upsert insert
        if (update.$setOnInsert) {
          for (const [path, value] of Object.entries(update.$setOnInsert)) {
            setValueByPath(baseDoc as Record<string, unknown>, path, value);
          }
        }

        const newDoc = applyUpdateOperators(baseDoc, update);

        if (!("_id" in newDoc)) {
          (newDoc as Record<string, unknown>)._id = new ObjectId();
        }

        await this.indexManager.checkUniqueConstraints([newDoc], documents);

        documents.push(newDoc);
        await this.writeDocuments(documents);

        if (returnAfter) {
          return options.projection
            ? applyProjection(newDoc, options.projection)
            : newDoc;
        }
        return null;
      }
      return null;
    }

    if (options.sort) {
      matches = this.sortDocuments(matches, options.sort);
    }

    const docToUpdate = matches[0];
    const originalId = (docToUpdate as { _id?: ObjectId })._id;

    const updatedDoc = applyUpdateOperators(docToUpdate, update);

    const updatedDocuments: T[] = [];
    const unchangedDocs: T[] = [];
    let updated = false;

    for (const doc of documents) {
      const id = (doc as { _id?: ObjectId })._id;
      if (!updated && id && originalId && id.equals(originalId)) {
        updatedDocuments.push(updatedDoc);
        updated = true;
      } else {
        updatedDocuments.push(doc);
        unchangedDocs.push(doc);
      }
    }

    await this.indexManager.checkUniqueConstraints([updatedDoc], unchangedDocs);

    await this.writeDocuments(updatedDocuments);

    const resultDoc = returnAfter ? updatedDoc : docToUpdate;
    if (options.projection) {
      return applyProjection(resultDoc, options.projection);
    }

    return resultDoc;
  }

  // ==================== Bulk Write ====================

  /**
   * Execute multiple write operations in bulk.
   *
   * Performs multiple insert, update, replace, and delete operations in a single call.
   * By default, operations are executed in order and stop on the first error (ordered: true).
   * Set ordered: false to execute all operations regardless of errors.
   *
   * Supported operations:
   * - insertOne: Insert a single document
   * - updateOne: Update a single document
   * - updateMany: Update multiple documents
   * - replaceOne: Replace a single document
   * - deleteOne: Delete a single document
   * - deleteMany: Delete multiple documents
   *
   * @param operations - Array of write operations to execute
   * @param options - Bulk write options, including ordered flag (default: true)
   * @returns Result containing counts for all operations and any upserted IDs
   * @throws Error if ordered is true and any operation fails
   * @throws Error with writeErrors property if ordered is false and any operations fail
   * @throws DuplicateKeyError if any operation violates a unique constraint
   *
   * @example
   * ```typescript
   * // Ordered bulk write (stops on first error)
   * const result = await collection.bulkWrite([
   *   { insertOne: { document: { name: 'John', age: 30 } } },
   *   { updateOne: { filter: { name: 'Jane' }, update: { $set: { age: 26 } } } },
   *   { deleteOne: { filter: { status: 'inactive' } } }
   * ]);
   * console.log(result.insertedCount); // 1
   * console.log(result.modifiedCount); // 1
   * console.log(result.deletedCount); // 1
   *
   * // Unordered bulk write (attempts all operations)
   * const result = await collection.bulkWrite([
   *   { insertOne: { document: { name: 'Alice' } } },
   *   { insertOne: { document: { name: 'Bob' } } },
   *   { updateMany: { filter: { active: false }, update: { $set: { archived: true } } } }
   * ], { ordered: false });
   *
   * // With upserts
   * const result = await collection.bulkWrite([
   *   { updateOne: {
   *       filter: { email: 'john@example.com' },
   *       update: { $set: { name: 'John', lastSeen: new Date() } },
   *       upsert: true
   *     }
   *   }
   * ]);
   * if (result.upsertedCount > 0) {
   *   console.log('Upserted IDs:', result.upsertedIds);
   * }
   * ```
   */
  async bulkWrite(
    operations: BulkWriteOperation<T>[],
    options: BulkWriteOptions = {}
  ): Promise<BulkWriteResult> {
    const ordered = options.ordered !== false;

    const result: BulkWriteResult = {
      acknowledged: true,
      insertedCount: 0,
      matchedCount: 0,
      modifiedCount: 0,
      deletedCount: 0,
      upsertedCount: 0,
      insertedIds: {},
      upsertedIds: {},
    };

    const errors: Array<{ index: number; error: Error }> = [];

    for (let i = 0; i < operations.length; i++) {
      const op = operations[i];

      try {
        if (op.insertOne) {
          const insertResult = await this.insertOne(op.insertOne.document);
          result.insertedCount++;
          result.insertedIds[i] = insertResult.insertedId;
        } else if (op.updateOne) {
          const updateResult = await this.updateOne(
            op.updateOne.filter,
            op.updateOne.update,
            { upsert: op.updateOne.upsert }
          );
          result.matchedCount += updateResult.matchedCount;
          result.modifiedCount += updateResult.modifiedCount;
          if (updateResult.upsertedId) {
            result.upsertedCount++;
            result.upsertedIds[i] = updateResult.upsertedId;
          }
        } else if (op.updateMany) {
          const updateResult = await this.updateMany(
            op.updateMany.filter,
            op.updateMany.update,
            { upsert: op.updateMany.upsert }
          );
          result.matchedCount += updateResult.matchedCount;
          result.modifiedCount += updateResult.modifiedCount;
          if (updateResult.upsertedId) {
            result.upsertedCount++;
            result.upsertedIds[i] = updateResult.upsertedId;
          }
        } else if (op.deleteOne) {
          const deleteResult = await this.deleteOne(op.deleteOne.filter);
          result.deletedCount += deleteResult.deletedCount;
        } else if (op.deleteMany) {
          const deleteResult = await this.deleteMany(op.deleteMany.filter);
          result.deletedCount += deleteResult.deletedCount;
        } else if (op.replaceOne) {
          validateReplacement(op.replaceOne.replacement);

          const documents = await this.readDocuments();
          const matches = documents.filter((doc) =>
            matchesFilter(doc, op.replaceOne!.filter)
          );

          if (matches.length === 0 && op.replaceOne.upsert) {
            const newDoc = { ...op.replaceOne.replacement } as T;
            if (!("_id" in newDoc)) {
              (newDoc as Record<string, unknown>)._id = new ObjectId();
            }
            await this.indexManager.checkUniqueConstraints([newDoc], documents);
            documents.push(newDoc);
            await this.writeDocuments(documents);
            result.upsertedCount++;
            result.upsertedIds[i] = (newDoc as Record<string, unknown>)._id as ObjectId;
          } else if (matches.length > 0) {
            const docToReplace = matches[0];
            const originalId = (docToReplace as { _id?: ObjectId })._id;
            const newDoc = { ...op.replaceOne.replacement } as T;
            if (originalId) {
              (newDoc as Record<string, unknown>)._id = originalId;
            }

            const updatedDocuments: T[] = [];
            const unchangedDocs: T[] = [];
            let replaced = false;

            for (const doc of documents) {
              const id = (doc as { _id?: ObjectId })._id;
              if (!replaced && id && originalId && id.equals(originalId)) {
                updatedDocuments.push(newDoc);
                replaced = true;
              } else {
                updatedDocuments.push(doc);
                unchangedDocs.push(doc);
              }
            }

            await this.indexManager.checkUniqueConstraints([newDoc], unchangedDocs);
            await this.writeDocuments(updatedDocuments);
            result.matchedCount++;
            result.modifiedCount++;
          }
        }
      } catch (error) {
        if (ordered) {
          throw error;
        } else {
          errors.push({ index: i, error: error as Error });
        }
      }
    }

    if (!ordered && errors.length > 0) {
      const bulkError = new Error(
        `BulkWrite had ${errors.length} error(s)`
      ) as Error & { writeErrors: typeof errors; result: BulkWriteResult };
      bulkError.writeErrors = errors;
      bulkError.result = result;
      throw bulkError;
    }

    return result;
  }

  // ==================== Administrative Operations ====================

  /**
   * Get an estimated count of documents in the collection.
   *
   * This method returns the count of documents without taking a query filter.
   * It's faster than countDocuments() because it can use collection metadata.
   * For MangoDB, it reads the document count from the file.
   *
   * @returns The estimated number of documents in the collection
   *
   * @example
   * ```typescript
   * const count = await collection.estimatedDocumentCount();
   * console.log(`Approximately ${count} documents`);
   * ```
   */
  async estimatedDocumentCount(): Promise<number> {
    const documents = await this.readDocuments();
    return documents.length;
  }

  /**
   * Get distinct values for a specified field.
   *
   * Returns an array of distinct values for the given field across all documents
   * that match the optional filter. If a field value is an array, each element
   * is treated as a separate value.
   *
   * @param field - The field name to get distinct values for (supports dot notation)
   * @param filter - Optional query filter to limit the documents considered
   * @returns An array of distinct values
   *
   * @example
   * ```typescript
   * // Get all distinct categories
   * const categories = await collection.distinct('category');
   *
   * // Get distinct categories for active items only
   * const activeCategories = await collection.distinct('category', { active: true });
   *
   * // Array field - each element is distinct
   * // Given: [{ tags: ['a', 'b'] }, { tags: ['b', 'c'] }]
   * const tags = await collection.distinct('tags');
   * // Returns: ['a', 'b', 'c']
   * ```
   */
  async distinct(field: string, filter: Filter<T> = {}): Promise<unknown[]> {
    const documents = await this.readDocuments();
    const filtered = await this.filterWithTextSupport(documents, filter);

    const seen = new Set<string>();
    const values: unknown[] = [];

    for (const doc of filtered) {
      const value = getValueByPath(doc, field);

      // Skip undefined (missing field)
      if (value === undefined) {
        continue;
      }

      // If value is an array, add each element separately
      if (Array.isArray(value)) {
        for (const elem of value) {
          const key = JSON.stringify(elem);
          if (!seen.has(key)) {
            seen.add(key);
            values.push(elem);
          }
        }
      } else {
        const key = JSON.stringify(value);
        if (!seen.has(key)) {
          seen.add(key);
          values.push(value);
        }
      }
    }

    return values;
  }

  /**
   * Drop (delete) the collection.
   *
   * Permanently removes the collection and all its indexes from the database.
   * Returns true regardless of whether the collection existed.
   *
   * @returns true (always)
   *
   * @example
   * ```typescript
   * // Drop the collection
   * const dropped = await collection.drop();
   * console.log(dropped); // true
   *
   * // Collection is now empty
   * const docs = await collection.find({}).toArray();
   * console.log(docs); // []
   * ```
   */
  async drop(): Promise<boolean> {
    // Delete data file
    try {
      await unlink(this.filePath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw error;
      }
    }

    // Delete index file
    const indexFilePath = this.filePath.replace(".json", ".indexes.json");
    try {
      await unlink(indexFilePath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw error;
      }
    }

    // Reset index manager
    await this.indexManager.reset();

    return true;
  }

  /**
   * Get statistics about the collection.
   *
   * Returns information about document count, size, and indexes.
   *
   * @returns Collection statistics object
   *
   * @example
   * ```typescript
   * const stats = await collection.stats();
   * console.log(`Documents: ${stats.count}`);
   * console.log(`Size: ${stats.size} bytes`);
   * console.log(`Indexes: ${stats.nindexes}`);
   * ```
   */
  async stats(): Promise<CollectionStats> {
    const docs = await this.readDocuments();
    const indexes = await this.indexManager.indexes();

    let dataSize = 0;
    let indexSize = 0;

    try {
      const fileStat = await stat(this.filePath);
      dataSize = fileStat.size;
    } catch {
      // File doesn't exist, size is 0
    }

    const indexFilePath = this.filePath.replace(".json", ".indexes.json");
    try {
      const indexStat = await stat(indexFilePath);
      indexSize = indexStat.size;
    } catch {
      // Index file doesn't exist, size is 0
    }

    // Distribute index size roughly among indexes
    const indexSizes: Record<string, number> = {};
    const perIndexSize = indexes.length > 0 ? Math.floor(indexSize / indexes.length) : 0;
    for (const idx of indexes) {
      indexSizes[idx.name] = perIndexSize;
    }

    // Extract collection name from file path
    const collectionName = this.filePath.split("/").pop()?.replace(".json", "") || "";

    return {
      ns: `${this.dbName}.${collectionName}`,
      count: docs.length,
      size: dataSize,
      storageSize: dataSize,
      totalIndexSize: indexSize,
      indexSizes,
      totalSize: dataSize + indexSize,
      nindexes: indexes.length,
      ok: 1,
    };
  }

  /**
   * Rename the collection.
   *
   * Renames this collection to a new name. By default, fails if the target
   * collection already exists. Use dropTarget: true to overwrite an existing
   * collection.
   *
   * @param newName - The new name for the collection
   * @param options - Options including dropTarget
   * @returns A new Collection instance pointing to the renamed collection
   * @throws NamespaceNotFoundError if this collection doesn't exist (code 26)
   * @throws TargetNamespaceExistsError if target exists and dropTarget is false (code 48)
   * @throws IllegalOperationError if newName is the same as current name
   * @throws InvalidNamespaceError if newName is invalid
   *
   * @example
   * ```typescript
   * // Basic rename
   * const newCollection = await collection.rename('newName');
   *
   * // Rename with overwrite
   * const newCollection = await collection.rename('existingName', { dropTarget: true });
   * ```
   */
  async rename(newName: string, options: RenameOptions = {}): Promise<MangoCollection<T>> {
    // Validate new name
    if (!newName || newName.length === 0) {
      throw new InvalidNamespaceError("collection names cannot be empty");
    }
    if (newName.startsWith(".") || newName.endsWith(".")) {
      throw new InvalidNamespaceError("collection names must not start or end with '.'");
    }
    if (newName.includes("$")) {
      throw new InvalidNamespaceError("collection names cannot contain '$'");
    }

    // Get current collection name
    const currentName = this.filePath.split("/").pop()?.replace(".json", "") || "";
    if (currentName === newName) {
      throw new IllegalOperationError("cannot rename collection to itself");
    }

    // Check source exists
    try {
      await access(this.filePath);
    } catch {
      throw new NamespaceNotFoundError();
    }

    const dbDir = dirname(this.filePath);
    const newFilePath = join(dbDir, `${newName}.json`);
    const newIndexPath = join(dbDir, `${newName}.indexes.json`);
    const currentIndexPath = this.filePath.replace(".json", ".indexes.json");

    // Check if target exists
    let targetExists = false;
    try {
      await access(newFilePath);
      targetExists = true;
    } catch {
      // Target doesn't exist, which is fine
    }

    if (targetExists) {
      if (!options.dropTarget) {
        throw new TargetNamespaceExistsError();
      }
      // Drop target
      try {
        await unlink(newFilePath);
      } catch {
        // Ignore errors
      }
      try {
        await unlink(newIndexPath);
      } catch {
        // Ignore errors
      }
    }

    // Rename data file
    await renameFile(this.filePath, newFilePath);

    // Rename index file if it exists
    try {
      await access(currentIndexPath);
      await renameFile(currentIndexPath, newIndexPath);
    } catch {
      // Index file doesn't exist, that's okay
    }

    // Return new collection instance
    return new MangoCollection(this.dataDir, this.dbName, newName);
  }
}
