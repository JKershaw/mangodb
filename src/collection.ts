/**
 * MongoneCollection - File-based MongoDB-compatible collection.
 *
 * This module provides the main collection class that uses extracted modules:
 * - types.ts: Type definitions
 * - document-utils.ts: Serialization, path access, cloning
 * - query-matcher.ts: Query matching logic
 * - update-operators.ts: Update operations
 * - index-manager.ts: Index management
 */
import { ObjectId } from "mongodb";
import { MongoneCursor, IndexCursor } from "./cursor.ts";
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

// Re-export types for backward compatibility
export type { IndexKeySpec, CreateIndexOptions, IndexInfo };

/**
 * MongoneCollection represents a collection in Mongone.
 * It mirrors the Collection API from the official MongoDB driver.
 */
export class MongoneCollection<T extends Document = Document> {
  private readonly filePath: string;
  private readonly indexManager: IndexManager;

  constructor(dataDir: string, dbName: string, collectionName: string) {
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

  // ==================== Index Operations ====================

  async createIndex(
    keySpec: IndexKeySpec,
    options: CreateIndexOptions = {}
  ): Promise<string> {
    return this.indexManager.createIndex(keySpec, options);
  }

  async dropIndex(indexNameOrSpec: string | IndexKeySpec): Promise<void> {
    return this.indexManager.dropIndex(indexNameOrSpec);
  }

  async indexes(): Promise<IndexInfo[]> {
    return this.indexManager.indexes();
  }

  listIndexes(): IndexCursor {
    return new IndexCursor(() => this.indexManager.indexes());
  }

  // ==================== Insert Operations ====================

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

  async findOne(filter: Filter<T> = {}, options: FindOptions = {}): Promise<T | null> {
    const documents = await this.readDocuments();

    for (const doc of documents) {
      if (matchesFilter(doc, filter)) {
        if (options.projection) {
          return applyProjection(doc, options.projection);
        }
        return doc;
      }
    }

    return null;
  }

  find(filter: Filter<T> = {}, options: FindOptions = {}): MongoneCursor<T> {
    return new MongoneCursor<T>(
      async () => {
        const documents = await this.readDocuments();
        return documents.filter((doc) => matchesFilter(doc, filter));
      },
      options.projection || null
    );
  }

  async countDocuments(filter: Filter<T> = {}): Promise<number> {
    const documents = await this.readDocuments();
    return documents.filter((doc) => matchesFilter(doc, filter)).length;
  }

  // ==================== Delete Operations ====================

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

  async updateOne(
    filter: Filter<T>,
    update: UpdateOperators,
    options: UpdateOptions = {}
  ): Promise<UpdateResult> {
    return this.performUpdate(filter, update, options, true);
  }

  async updateMany(
    filter: Filter<T>,
    update: UpdateOperators,
    options: UpdateOptions = {}
  ): Promise<UpdateResult> {
    return this.performUpdate(filter, update, options, false);
  }

  // ==================== FindOneAnd* Operations ====================

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
