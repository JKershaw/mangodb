/**
 * Fuzz testing harness for MangoDB.
 *
 * Follows the same pattern as test-harness.ts:
 * - Without MONGODB_URI: Tests run against MangoDB
 * - With MONGODB_URI: Tests run against MongoDB
 *
 * Additionally supports dual-target comparison mode for finding behavioral differences.
 */

import * as fc from 'fast-check';
import { MongoClient } from 'mongodb';
import { MangoClient } from '../../src/index.ts';
import { randomUUID } from 'node:crypto';
import { rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { FUZZ_CONFIG, getNumRuns } from './config.ts';
import {
  createTestClient,
  isMongoDBMode,
  getTestModeName,
  type TestClient,
  type Document,
} from '../test-harness.ts';

/**
 * Context for fuzz tests - follows the same pattern as regular tests.
 * Uses createTestClient() from the main test harness.
 */
export interface FuzzContext {
  client: TestClient;
  dbName: string;
  cleanup: () => Promise<void>;
}

/**
 * Context for dual-target comparison (requires both MangoDB and MongoDB).
 */
export interface DualTargetContext {
  mangoClient: TestClient;
  mongoClient: TestClient;
  dbName: string;
  cleanup: () => Promise<void>;
}

/**
 * Result of comparing MangoDB and MongoDB behavior.
 */
export interface ComparisonResult {
  equal: boolean;
  differences: string[];
  mangoResult: unknown;
  mongoResult: unknown;
}

const MONGODB_URI = process.env.MONGODB_URI;

// Re-export for convenience
export { isMongoDBMode, getTestModeName };

/**
 * Check if MongoDB is available for dual-target comparison.
 */
export function isMongoDBAvailable(): boolean {
  return !!MONGODB_URI;
}

/**
 * Create a fuzz context using the standard test harness.
 * Works exactly like regular tests - uses MangoDB or MongoDB based on MONGODB_URI.
 */
export async function createFuzzContext(): Promise<FuzzContext> {
  return createTestClient();
}

/**
 * Create a dual-target context with both MangoDB and MongoDB clients.
 * Only available when MONGODB_URI is set.
 */
export async function createDualTargetContext(): Promise<DualTargetContext> {
  if (!MONGODB_URI) {
    throw new Error('MONGODB_URI is required for dual-target comparison. Set it in .env.mongodb');
  }

  const dbName = `_fuzz_${randomUUID().replace(/-/g, '').slice(0, 24)}`;
  const mangoDataDir = `${tmpdir()}/mangodb_fuzz_${randomUUID()}`;

  const mongoClient = new MongoClient(MONGODB_URI);
  const mangoClient = new MangoClient(mangoDataDir);

  await Promise.all([mongoClient.connect(), mangoClient.connect()]);

  const cleanup = async () => {
    try {
      await mongoClient.db(dbName).dropDatabase();
    } catch {
      // Ignore cleanup errors
    }
    await mongoClient.close();

    await mangoClient.close();
    try {
      await rm(mangoDataDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  };

  return {
    mangoClient: mangoClient as unknown as TestClient,
    mongoClient: mongoClient as unknown as TestClient,
    dbName,
    cleanup,
  };
}

/**
 * Deep equality check for BSON values, handling ObjectId, Date, etc.
 */
export function deepBsonEqual(a: unknown, b: unknown): boolean {
  // Handle null/undefined
  if (a === null && b === null) return true;
  if (a === undefined && b === undefined) return true;
  if (a === null || b === null || a === undefined || b === undefined) return false;

  // Handle primitives
  if (typeof a !== typeof b) return false;
  if (typeof a !== 'object') return a === b;

  // Handle Date
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }

  // Handle ObjectId (check for $oid property or toHexString method)
  if (isObjectId(a) && isObjectId(b)) {
    return getObjectIdString(a) === getObjectIdString(b);
  }

  // Handle arrays
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((item, index) => deepBsonEqual(item, b[index]));
  }

  // Handle objects
  if (Array.isArray(a) || Array.isArray(b)) return false;

  const objA = a as Record<string, unknown>;
  const objB = b as Record<string, unknown>;
  const keysA = Object.keys(objA).sort();
  const keysB = Object.keys(objB).sort();

  if (keysA.length !== keysB.length) return false;
  if (!keysA.every((key, index) => key === keysB[index])) return false;

  return keysA.every((key) => deepBsonEqual(objA[key], objB[key]));
}

function isObjectId(value: unknown): boolean {
  if (value === null || typeof value !== 'object') return false;
  const obj = value as Record<string, unknown>;
  return (
    '$oid' in obj ||
    (typeof (obj as { toHexString?: unknown }).toHexString === 'function' &&
      typeof (obj as { toString?: unknown }).toString === 'function')
  );
}

function getObjectIdString(value: unknown): string {
  const obj = value as Record<string, unknown>;
  if ('$oid' in obj) return String(obj.$oid);
  if (typeof (obj as { toHexString?: () => string }).toHexString === 'function') {
    return (obj as { toHexString: () => string }).toHexString();
  }
  return String(value);
}

/**
 * Compare results from MangoDB and MongoDB.
 */
export function compareResults(
  mangoResult: unknown,
  mongoResult: unknown,
  context: string
): ComparisonResult {
  const differences: string[] = [];

  // Normalize results - strip _id for comparison if needed
  const normalizedMango = normalizeResult(mangoResult);
  const normalizedMongo = normalizeResult(mongoResult);

  const equal = deepBsonEqual(normalizedMango, normalizedMongo);

  if (!equal) {
    differences.push(`Results differ at ${context}`);

    // Add more specific difference info for debugging
    if (Array.isArray(normalizedMango) && Array.isArray(normalizedMongo)) {
      if (normalizedMango.length !== normalizedMongo.length) {
        differences.push(
          `Array length: MangoDB=${normalizedMango.length}, MongoDB=${normalizedMongo.length}`
        );
      }
    }
  }

  return {
    equal,
    differences,
    mangoResult: normalizedMango,
    mongoResult: normalizedMongo,
  };
}

/**
 * Normalize a result for comparison.
 * Removes _id fields since they differ between databases.
 */
function normalizeResult(result: unknown): unknown {
  if (result === null || result === undefined) return result;
  if (typeof result !== 'object') return result;

  if (Array.isArray(result)) {
    return result.map(normalizeResult);
  }

  const obj = result as Record<string, unknown>;
  const normalized: Record<string, unknown> = {};

  for (const key of Object.keys(obj)) {
    // Skip _id for comparison since they differ
    if (key === '_id') continue;
    normalized[key] = normalizeResult(obj[key]);
  }

  return normalized;
}

/**
 * Compare errors from MangoDB and MongoDB.
 */
export function compareErrors(
  mangoError: Error | null,
  mongoError: Error | null,
  context: string
): ComparisonResult {
  const differences: string[] = [];

  // Both should either throw or not throw
  const mangoThrew = mangoError !== null;
  const mongoThrew = mongoError !== null;

  if (mangoThrew !== mongoThrew) {
    differences.push(`Error behavior differs at ${context}`);
    differences.push(`MangoDB ${mangoThrew ? 'threw' : 'succeeded'}`);
    differences.push(`MongoDB ${mongoThrew ? 'threw' : 'succeeded'}`);
    return {
      equal: false,
      differences,
      mangoResult: mangoError?.message ?? 'no error',
      mongoResult: mongoError?.message ?? 'no error',
    };
  }

  // If both threw, check error codes match (not messages, which can vary)
  if (mangoThrew && mongoThrew) {
    const mangoCode = (mangoError as { code?: number }).code;
    const mongoCode = (mongoError as { code?: number }).code;

    if (mangoCode !== mongoCode) {
      differences.push(`Error codes differ: MangoDB=${mangoCode}, MongoDB=${mongoCode}`);
      return {
        equal: false,
        differences,
        mangoResult: mangoError.message,
        mongoResult: mongoError.message,
      };
    }
  }

  return {
    equal: true,
    differences: [],
    mangoResult: mangoError?.message ?? 'no error',
    mongoResult: mongoError?.message ?? 'no error',
  };
}

/**
 * Report a difference found during fuzz testing.
 */
export function reportDifference(
  testName: string,
  input: unknown,
  result: ComparisonResult
): void {
  console.error('\n========================================');
  console.error('FUZZ DIFFERENCE DETECTED');
  console.error('========================================');
  console.error(`Test: ${testName}`);
  console.error(`Timestamp: ${new Date().toISOString()}`);
  console.error('\nInput:');
  console.error(JSON.stringify(input, null, 2));
  console.error('\nMangoDB result:');
  console.error(JSON.stringify(result.mangoResult, null, 2));
  console.error('\nMongoDB result:');
  console.error(JSON.stringify(result.mongoResult, null, 2));
  console.error('\nDifferences:');
  result.differences.forEach((d) => console.error(`  - ${d}`));
  console.error('========================================\n');
}

/**
 * Run a fuzz test against the current target (MangoDB or MongoDB based on MONGODB_URI).
 * Follows the same pattern as regular integration tests.
 *
 * @param name - Name of the test (for reporting)
 * @param arb - fast-check arbitrary to generate test inputs
 * @param testFn - Test function that runs and returns true if passed
 */
export async function runFuzz<T>(
  name: string,
  arb: fc.Arbitrary<T>,
  testFn: (input: T, ctx: FuzzContext) => Promise<boolean>
): Promise<void> {
  await fc.assert(
    fc.asyncProperty(arb, async (input) => {
      const ctx = await createFuzzContext();
      try {
        return await testFn(input, ctx);
      } finally {
        await ctx.cleanup();
      }
    }),
    {
      numRuns: getNumRuns(),
      seed: FUZZ_CONFIG.seed,
      verbose: FUZZ_CONFIG.verbose,
    }
  );
}

/**
 * Run a dual-target fuzz test that compares MangoDB and MongoDB behavior.
 * Requires MONGODB_URI to be set.
 *
 * @param name - Name of the test (for reporting)
 * @param arb - fast-check arbitrary to generate test inputs
 * @param testFn - Test function that runs on both targets and returns comparison
 */
export async function runDualTargetFuzz<T>(
  name: string,
  arb: fc.Arbitrary<T>,
  testFn: (input: T, ctx: DualTargetContext) => Promise<ComparisonResult>
): Promise<void> {
  if (!isMongoDBAvailable()) {
    console.log(`Skipping dual-target fuzz test "${name}" - MONGODB_URI not set`);
    return;
  }

  await fc.assert(
    fc.asyncProperty(arb, async (input) => {
      const ctx = await createDualTargetContext();
      try {
        const result = await testFn(input, ctx);
        if (!result.equal) {
          reportDifference(name, input, result);
          return false;
        }
        return true;
      } finally {
        await ctx.cleanup();
      }
    }),
    {
      numRuns: getNumRuns(),
      seed: FUZZ_CONFIG.seed,
      verbose: FUZZ_CONFIG.verbose,
    }
  );
}

/**
 * Helper to run a query on both targets and compare results.
 */
export async function compareQueryResults(
  ctx: DualTargetContext,
  collectionName: string,
  documents: Document[],
  query: Document
): Promise<ComparisonResult> {
  const mangoCollection = ctx.mangoClient.db(ctx.dbName).collection(collectionName);
  const mongoCollection = ctx.mongoClient.db(ctx.dbName).collection(collectionName);

  // Insert same documents into both
  if (documents.length > 0) {
    await Promise.all([mangoCollection.insertMany(documents), mongoCollection.insertMany(documents)]);
  }

  // Run query on both
  let mangoResult: unknown;
  let mongoResult: unknown;
  let mangoError: Error | null = null;
  let mongoError: Error | null = null;

  try {
    mangoResult = await mangoCollection.find(query).toArray();
  } catch (e) {
    mangoError = e as Error;
  }

  try {
    mongoResult = await mongoCollection.find(query).toArray();
  } catch (e) {
    mongoError = e as Error;
  }

  // If either threw, compare error behavior
  if (mangoError || mongoError) {
    return compareErrors(mangoError, mongoError, `query: ${JSON.stringify(query)}`);
  }

  return compareResults(mangoResult, mongoResult, `query: ${JSON.stringify(query)}`);
}

/**
 * Helper to run an update on both targets and compare results.
 */
export async function compareUpdateResults(
  ctx: DualTargetContext,
  collectionName: string,
  documents: Document[],
  filter: Document,
  update: Document
): Promise<ComparisonResult> {
  const mangoCollection = ctx.mangoClient.db(ctx.dbName).collection(collectionName);
  const mongoCollection = ctx.mongoClient.db(ctx.dbName).collection(collectionName);

  // Insert same documents into both
  if (documents.length > 0) {
    await Promise.all([mangoCollection.insertMany(documents), mongoCollection.insertMany(documents)]);
  }

  // Run update on both
  let mangoError: Error | null = null;
  let mongoError: Error | null = null;

  try {
    await mangoCollection.updateMany(filter, update);
  } catch (e) {
    mangoError = e as Error;
  }

  try {
    await mongoCollection.updateMany(filter, update);
  } catch (e) {
    mongoError = e as Error;
  }

  // If either threw, compare error behavior
  if (mangoError || mongoError) {
    return compareErrors(mangoError, mongoError, `update: ${JSON.stringify(update)}`);
  }

  // Compare resulting documents
  const mangoResult = await mangoCollection.find({}).toArray();
  const mongoResult = await mongoCollection.find({}).toArray();

  return compareResults(mangoResult, mongoResult, `update result`);
}

// ============================================================================
// Single-target helpers (for use with runFuzz)
// ============================================================================

/**
 * Run a query and verify it doesn't throw.
 * Returns true if successful, false if an unexpected error occurred.
 */
export async function testQuery(
  ctx: FuzzContext,
  collectionName: string,
  documents: Document[],
  query: Document
): Promise<boolean> {
  const collection = ctx.client.db(ctx.dbName).collection(collectionName);

  if (documents.length > 0) {
    await collection.insertMany(documents);
  }

  try {
    await collection.find(query).toArray();
    return true;
  } catch (e) {
    // Log the error for debugging but return true if it's an expected MongoDB error
    // (the fuzz test is checking that we don't crash unexpectedly)
    const error = e as Error & { code?: number };
    if (error.code) {
      // Known MongoDB error codes are expected behavior
      return true;
    }
    console.error(`Unexpected error in query fuzz test:`, e);
    return false;
  }
}

/**
 * Run an update and verify it doesn't throw unexpectedly.
 */
export async function testUpdate(
  ctx: FuzzContext,
  collectionName: string,
  documents: Document[],
  filter: Document,
  update: Document
): Promise<boolean> {
  const collection = ctx.client.db(ctx.dbName).collection(collectionName);

  if (documents.length > 0) {
    await collection.insertMany(documents);
  }

  try {
    await collection.updateMany(filter, update);
    return true;
  } catch (e) {
    const error = e as Error & { code?: number };
    if (error.code) {
      return true;
    }
    console.error(`Unexpected error in update fuzz test:`, e);
    return false;
  }
}

/**
 * Run an aggregation and verify it doesn't throw unexpectedly.
 */
export async function testAggregation(
  ctx: FuzzContext,
  collectionName: string,
  documents: Document[],
  pipeline: Document[]
): Promise<boolean> {
  const collection = ctx.client.db(ctx.dbName).collection(collectionName);

  if (documents.length > 0) {
    await collection.insertMany(documents);
  }

  try {
    await collection.aggregate(pipeline).toArray();
    return true;
  } catch (e) {
    const error = e as Error & { code?: number };
    if (error.code) {
      return true;
    }
    console.error(`Unexpected error in aggregation fuzz test:`, e);
    return false;
  }
}
