import { MangoDBDb } from "./db.ts";
import { mkdir } from "node:fs/promises";

/**
 * MangoDBClient is the entry point for using MangoDB.
 * It mirrors the MongoClient API from the official MongoDB driver,
 * providing a MongoDB-compatible client interface backed by file storage.
 *
 * @example
 * ```typescript
 * const client = new MangoDBClient('./data');
 * await client.connect();
 * const db = client.db('myDatabase');
 * await client.close();
 * ```
 */
export class MangoDBClient {
  private readonly dataDir: string;
  private connected = false;
  private databases = new Map<string, MangoDBDb>();

  /**
   * Create a new MangoDBClient.
   * @param dataDir - Directory where data will be stored
   *
   * @example
   * ```typescript
   * const client = new MangoDBClient('./data');
   * ```
   */
  constructor(dataDir: string) {
    this.dataDir = dataDir;
  }

  /**
   * Connect to the MangoDB instance.
   * Creates the data directory if it doesn't exist.
   *
   * @returns The client instance for method chaining
   *
   * @example
   * ```typescript
   * const client = new MangoDBClient('./data');
   * await client.connect();
   * ```
   */
  async connect(): Promise<this> {
    await mkdir(this.dataDir, { recursive: true });
    this.connected = true;
    return this;
  }

  /**
   * Close the MangoDB client connection.
   * Clears all cached database instances.
   *
   * @example
   * ```typescript
   * await client.close();
   * ```
   */
  async close(): Promise<void> {
    this.connected = false;
    this.databases.clear();
  }

  /**
   * Get a database instance.
   * Database instances are cached and reused for the same name.
   *
   * @param name - Database name
   * @returns A MangoDBDb instance
   *
   * @example
   * ```typescript
   * const db = client.db('myDatabase');
   * const collection = db.collection('users');
   * ```
   */
  db(name: string): MangoDBDb {
    if (!this.databases.has(name)) {
      this.databases.set(name, new MangoDBDb(this.dataDir, name));
    }
    return this.databases.get(name)!;
  }
}
