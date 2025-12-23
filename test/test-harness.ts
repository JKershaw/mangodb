/**
 * Dual-target test harness for MangoDB.
 *
 * This module provides a unified interface for tests that can run against
 * either real MongoDB or MangoDB, controlled by the MONGODB_URI environment variable.
 *
 * - If MONGODB_URI is set: Tests run against real MongoDB
 * - If MONGODB_URI is not set: Tests run against MangoDB
 */

import { MongoClient } from "mongodb";
import { MangoDBClient } from "../src/index.ts";
import { randomUUID } from "node:crypto";
import { rm } from "node:fs/promises";

export interface TestClient {
  connect(): Promise<void>;
  close(): Promise<void>;
  db(name: string): TestDb;
}

export interface TestDb {
  collection<T extends Document = Document>(name: string): TestCollection<T>;
  dropDatabase(): Promise<void>;
}

export interface UpdateResult {
  acknowledged: boolean;
  matchedCount: number;
  modifiedCount: number;
  upsertedCount: number;
  upsertedId: unknown | null;
}

export interface UpdateOptions {
  upsert?: boolean;
}

export interface FindOptions {
  projection?: Document;
  sort?: Document;
  skip?: number;
}

export interface IndexInfo {
  v: number;
  key: Record<string, 1 | -1 | "text">;
  name: string;
  unique?: boolean;
  sparse?: boolean;
  expireAfterSeconds?: number;
  partialFilterExpression?: Record<string, unknown>;
}

export interface CreateIndexOptions {
  unique?: boolean;
  name?: string;
  sparse?: boolean;
  expireAfterSeconds?: number;
  partialFilterExpression?: Record<string, unknown>;
}

export interface IndexCursor {
  toArray(): Promise<IndexInfo[]>;
}

// Phase 8: FindOneAnd* types
// Note: Driver 6.0+ returns document directly, not wrapped in ModifyResult

export interface FindOneAndDeleteOptions {
  projection?: Document;
  sort?: Document;
}

export interface FindOneAndReplaceOptions {
  projection?: Document;
  sort?: Document;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

export interface FindOneAndUpdateOptions {
  projection?: Document;
  sort?: Document;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

export interface BulkWriteOperation {
  insertOne?: { document: Document };
  updateOne?: { filter: Document; update: Document; upsert?: boolean };
  updateMany?: { filter: Document; update: Document; upsert?: boolean };
  deleteOne?: { filter: Document };
  deleteMany?: { filter: Document };
  replaceOne?: { filter: Document; replacement: Document; upsert?: boolean };
}

export interface BulkWriteResult {
  acknowledged?: boolean; // May not be present in driver 6.x
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: Record<number, unknown>;
  upsertedIds: Record<number, unknown>;
}

export interface TestCollection<T extends Document = Document> {
  insertOne(
    doc: T
  ): Promise<{ acknowledged: boolean; insertedId: unknown }>;
  insertMany(
    docs: T[]
  ): Promise<{ acknowledged: boolean; insertedIds: Record<number, unknown> }>;
  findOne(filter?: Partial<T>, options?: FindOptions): Promise<T | null>;
  find(filter?: Partial<T>, options?: FindOptions): TestCursor<T>;
  deleteOne(
    filter: Partial<T>
  ): Promise<{ acknowledged: boolean; deletedCount: number }>;
  deleteMany(
    filter: Partial<T>
  ): Promise<{ acknowledged: boolean; deletedCount: number }>;
  updateOne(
    filter: Partial<T>,
    update: Document,
    options?: UpdateOptions
  ): Promise<UpdateResult>;
  updateMany(
    filter: Partial<T>,
    update: Document,
    options?: UpdateOptions
  ): Promise<UpdateResult>;
  countDocuments(filter?: Partial<T>): Promise<number>;
  createIndex(
    keySpec: Record<string, 1 | -1>,
    options?: CreateIndexOptions
  ): Promise<string>;
  dropIndex(indexNameOrSpec: string | Record<string, 1 | -1>): Promise<void>;
  indexes(): Promise<IndexInfo[]>;
  listIndexes(): IndexCursor;
  // Phase 8: FindOneAnd* and bulkWrite
  // Driver 6.0+ returns document directly (not wrapped in { value, ok })
  findOneAndDelete(
    filter: Partial<T>,
    options?: FindOneAndDeleteOptions
  ): Promise<T | null>;
  findOneAndReplace(
    filter: Partial<T>,
    replacement: T,
    options?: FindOneAndReplaceOptions
  ): Promise<T | null>;
  findOneAndUpdate(
    filter: Partial<T>,
    update: Document,
    options?: FindOneAndUpdateOptions
  ): Promise<T | null>;
  bulkWrite(
    operations: BulkWriteOperation[],
    options?: { ordered?: boolean }
  ): Promise<BulkWriteResult>;
  aggregate(pipeline: Document[]): AggregationCursor<T>;
}

export interface TestCursor<T> {
  sort(spec: Document): TestCursor<T>;
  limit(n: number): TestCursor<T>;
  skip(n: number): TestCursor<T>;
  hint(indexHint: string | Document): TestCursor<T>;
  toArray(): Promise<T[]>;
}

export interface AggregationCursor<T> {
  toArray(): Promise<T[]>;
}

export type Document = Record<string, unknown>;

const MONGODB_URI = process.env.MONGODB_URI;

export function isMongoDBMode(): boolean {
  return !!MONGODB_URI;
}

export function getTestModeName(): string {
  return isMongoDBMode() ? "MongoDB" : "MangoDB";
}

/**
 * Creates a test client. Each call creates a unique database name to avoid
 * test interference.
 */
export async function createTestClient(): Promise<{
  client: TestClient;
  dbName: string;
  cleanup: () => Promise<void>;
}> {
  const dbName = `mangodb_test_${randomUUID().replace(/-/g, "")}`;

  if (isMongoDBMode()) {
    const client = new MongoClient(MONGODB_URI!);

    const cleanup = async () => {
      try {
        await client.db(dbName).dropDatabase();
      } catch {
        // Ignore cleanup errors
      }
      await client.close();
    };

    return {
      client: client as unknown as TestClient,
      dbName,
      cleanup,
    };
  } else {
    const dataDir = `/tmp/mangodb_test_${randomUUID()}`;
    const client = new MangoDBClient(dataDir);

    const cleanup = async () => {
      await client.close();
      try {
        await rm(dataDir, { recursive: true, force: true });
      } catch {
        // Ignore cleanup errors
      }
    };

    return {
      client: client as unknown as TestClient,
      dbName,
      cleanup,
    };
  }
}
