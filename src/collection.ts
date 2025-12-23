/**
 * MangoDBCollection - File-based MongoDB-compatible collection.
 *
 * This module provides the main collection class that uses extracted modules:
 * - types.ts: Type definitions
 * - document-utils.ts: Serialization, path access, cloning
 * - query-matcher.ts: Query matching logic
 * - update-operators.ts: Update operations
 * - index-manager.ts: Index management
 */
import { ObjectId } from "mongodb";
import { MangoDBCursor, IndexCursor } from "./cursor.ts";
import { AggregationCursor, type AggregationDbContext } from "./aggregation.ts";
import { applyProjection, compareValuesForSort } from "./utils.ts";
import { readFile, writeFile, mkdir } from "node:fs/promises";
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
} from "./types.ts";

import {
  serializeDocument,
  deserializeDocument,
  getValueByPath,
  documentsEqual,
} from "./document-utils.ts";

import { matchesFilter } from "./query-matcher.ts";

import {
  applyUpdateOperators,
  createDocumentFromFilter,
  validateReplacement,
} from "./update-operators.ts";

import { IndexManager } from "./index-manager.ts";

import { TextIndexRequiredError } from "./errors.ts";

// Re-export types for backward compatibility
export type { IndexKeySpec, CreateIndexOptions, IndexInfo };

/**
 * MangoDBCollection - A file-based MongoDB-compatible collection.
 *
 * This class provides MongoDB Collection API methods backed by JSON file storage.
 * It supports standard CRUD operations, indexing, bulk writes, and advanced queries
 * with full compatibility with the official MongoDB driver API.
 *
 * @template T - The document type for this collection, must extend Document
 *
 * @example
 * ```typescript
 * const collection = new MangoDBCollection<User>('./data', 'mydb', 'users');
 * await collection.insertOne({ name: 'John', age: 30 });
 * const user = await collection.findOne({ name: 'John' });
 * ```
 */
export class MangoDBCollection<T extends Document = Document> {
  private readonly filePath: string;
  private readonly indexManager: IndexManager;
  private readonly dataDir: string;
  private readonly dbName: string;

  /**
   * Create a new MangoDBCollection instance.
   *
   * @param dataDir - Base directory for data storage
   * @param dbName - Database name
   * @param collectionName - Collection name
   *
   * @example
   * ```typescript
   * const collection = new MangoDBCollection('./data', 'mydb', 'users');
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
   * Check if a document matches a text search query.
   * Tokenizes the search string and matches if ANY token appears in ANY text field.
   * Case-insensitive matching.
   *
   * @param doc - Document to check
   * @param searchString - Text to search for (space-separated tokens)
   * @param textFields - Fields that are text-indexed
   * @returns True if any token matches any text field
   */
  private matchesTextSearch(doc: T, searchString: string, textFields: string[]): boolean {
    // Empty search string matches nothing
    if (!searchString || searchString.trim() === "") {
      return false;
    }

    // Tokenize by whitespace
    const tokens = searchString.toLowerCase().split(/\s+/).filter(t => t.length > 0);
    if (tokens.length === 0) {
      return false;
    }

    // Check each text field
    for (const field of textFields) {
      const value = getValueByPath(doc, field);
      if (typeof value === "string") {
        const lowerValue = value.toLowerCase();
        // Match if ANY token is found as substring
        if (tokens.some(token => lowerValue.includes(token))) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Filter documents with $text query support.
   * Handles $text operator and delegates to matchesFilter for other conditions.
   *
   * @param documents - Documents to filter
   * @param filter - Query filter that may contain $text
   * @returns Filtered documents
   * @throws TextIndexRequiredError if $text is used without a text index
   */
  private async filterWithTextSupport(documents: T[], filter: Filter<T>): Promise<T[]> {
    const textQuery = (filter as Record<string, unknown>).$text as
      | { $search?: string }
      | undefined;

    if (textQuery) {
      // Get text index fields
      const textFields = await this.indexManager.getTextIndexFields();
      if (textFields.length === 0) {
        throw new TextIndexRequiredError();
      }

      const searchString = textQuery.$search || "";

      // Create a filter without $text for additional conditions
      const remainingFilter = { ...filter } as Record<string, unknown>;
      delete remainingFilter.$text;

      // Apply both text search and regular filter
      return documents.filter((doc) => {
        const matchesText = this.matchesTextSearch(doc, searchString, textFields);
        const matchesOther =
          Object.keys(remainingFilter).length === 0 ||
          matchesFilter(doc, remainingFilter as Filter<T>);
        return matchesText && matchesOther;
      });
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
   * @throws MongoDuplicateKeyError if unique constraint is violated by existing documents
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
   * @throws MongoDuplicateKeyError if a unique constraint is violated
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
   * @throws MongoDuplicateKeyError if any unique constraint is violated
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
   * If no document matches, returns null.
   *
   * @param filter - Query filter to match documents (default: empty object matches all)
   * @param options - Query options including projection
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
   * ```
   */
  async findOne(filter: Filter<T> = {}, options: FindOptions = {}): Promise<T | null> {
    const documents = await this.readDocuments();
    const filtered = await this.filterWithTextSupport(documents, filter);

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
   * The cursor supports methods like toArray(), sort(), limit(), and skip().
   *
   * @param filter - Query filter to match documents (default: empty object matches all)
   * @param options - Query options including projection
   * @returns A MangoDBCursor for iterating through matching documents
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
   * ```
   */
  find(filter: Filter<T> = {}, options: FindOptions = {}): MangoDBCursor<T> {
    return new MangoDBCursor<T>(
      async () => {
        const documents = await this.readDocuments();
        return this.filterWithTextSupport(documents, filter);
      },
      options.projection || null
    );
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
    // Create database context for $lookup and $out stages
    const dbContext: AggregationDbContext = {
      getCollection: (name: string) => {
        return new MangoDBCollection(this.dataDir, this.dbName, name);
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

    for (const doc of documents) {
      const shouldMatch = limitOne ? matchedCount === 0 : true;

      if (shouldMatch && matchesFilter(doc, filter)) {
        matchedCount++;
        const updatedDoc = applyUpdateOperators(doc, update);

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
      const newDoc = applyUpdateOperators(baseDoc, update);

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
   * @throws MongoDuplicateKeyError if upsert violates a unique constraint
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
   * @throws MongoDuplicateKeyError if upsert violates a unique constraint
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
   * @throws MongoDuplicateKeyError if replacement or upsert violates a unique constraint
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
   * @throws MongoDuplicateKeyError if update or upsert violates a unique constraint
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
   * @throws MongoDuplicateKeyError if any operation violates a unique constraint
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
}
