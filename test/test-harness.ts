/**
 * Dual-target test harness for Mongone.
 *
 * This module provides a unified interface for tests that can run against
 * either real MongoDB or Mongone, controlled by the MONGODB_URI environment variable.
 *
 * - If MONGODB_URI is set: Tests run against real MongoDB
 * - If MONGODB_URI is not set: Tests run against Mongone
 */

import { MongoClient } from "mongodb";
import { MongoneClient } from "../src/index.ts";
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
}

export interface TestCursor<T> {
  sort(spec: Document): TestCursor<T>;
  limit(n: number): TestCursor<T>;
  skip(n: number): TestCursor<T>;
  toArray(): Promise<T[]>;
}

export type Document = Record<string, unknown>;

const MONGODB_URI = process.env.MONGODB_URI;

export function isMongoDBMode(): boolean {
  return !!MONGODB_URI;
}

export function getTestModeName(): string {
  return isMongoDBMode() ? "MongoDB" : "Mongone";
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
  const dbName = `mongone_test_${randomUUID().replace(/-/g, "")}`;

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
    const dataDir = `/tmp/mongone_test_${randomUUID()}`;
    const client = new MongoneClient(dataDir);

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
