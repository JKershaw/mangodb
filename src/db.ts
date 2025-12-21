import { MangoDBCollection } from "./collection.ts";
import { rm } from "node:fs/promises";
import { join } from "node:path";

/**
 * MangoDBDb represents a database in MangoDB.
 * It mirrors the Db API from the official MongoDB driver,
 * providing methods to work with collections and manage the database.
 *
 * @example
 * ```typescript
 * const client = new MangoDBClient('./data');
 * await client.connect();
 * const db = client.db('myDatabase');
 * const collection = db.collection('users');
 * ```
 */
export class MangoDBDb {
  private readonly dataDir: string;
  private readonly name: string;
  private collections = new Map<string, MangoDBCollection<Document>>();

  /**
   * Create a new MangoDBDb instance.
   * Note: Typically you don't create this directly; use MangoDBClient.db() instead.
   *
   * @param dataDir - Base directory for storing database files
   * @param name - Database name
   *
   * @example
   * ```typescript
   * // Usually obtained via client.db()
   * const db = client.db('myDatabase');
   * ```
   */
  constructor(dataDir: string, name: string) {
    this.dataDir = dataDir;
    this.name = name;
  }

  /**
   * Get a collection instance.
   * Collection instances are cached and reused for the same name.
   * The generic type parameter T allows for typed document operations.
   *
   * @param name - Collection name
   * @returns A MangoDBCollection instance typed to T
   *
   * @example
   * ```typescript
   * // Untyped collection
   * const users = db.collection('users');
   *
   * // Typed collection
   * interface User {
   *   _id?: ObjectId;
   *   name: string;
   *   email: string;
   * }
   * const typedUsers = db.collection<User>('users');
   * ```
   */
  collection<T extends Document = Document>(name: string): MangoDBCollection<T> {
    if (!this.collections.has(name)) {
      this.collections.set(
        name,
        new MangoDBCollection<Document>(this.dataDir, this.name, name)
      );
    }
    return this.collections.get(name)! as MangoDBCollection<T>;
  }

  /**
   * Drop the database.
   * Permanently deletes the database directory and all its collections.
   * Clears all cached collection instances.
   *
   * @example
   * ```typescript
   * await db.dropDatabase();
   * ```
   */
  async dropDatabase(): Promise<void> {
    const dbPath = join(this.dataDir, this.name);
    await rm(dbPath, { recursive: true, force: true });
    this.collections.clear();
  }
}

type Document = Record<string, unknown>;
