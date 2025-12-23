import { MangoDBCollection } from "./collection.ts";
import { rm, readdir, readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { matchesFilter } from "./query-matcher.ts";
import type {
  Document,
  Filter,
  ListCollectionsOptions,
  CollectionInfo,
  DbStats,
} from "./types.ts";

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

  /**
   * List collections in this database.
   *
   * Returns a cursor that can iterate over collection information documents.
   * Optionally filter collections by name or other properties.
   *
   * @param filter - Filter to match collections (e.g., { name: 'users' })
   * @param options - Options including nameOnly
   * @returns A ListCollectionsCursor for iterating through collections
   *
   * @example
   * ```typescript
   * // List all collections
   * const collections = await db.listCollections().toArray();
   *
   * // Filter by name
   * const filtered = await db.listCollections({ name: 'users' }).toArray();
   *
   * // nameOnly option
   * const names = await db.listCollections({}, { nameOnly: true }).toArray();
   * ```
   */
  listCollections(
    filter: Filter<CollectionInfo> = {},
    options: ListCollectionsOptions = {}
  ): ListCollectionsCursor {
    return new ListCollectionsCursor(
      async () => {
        const dbPath = join(this.dataDir, this.name);
        try {
          const files = await readdir(dbPath);
          const collectionNames = files
            .filter((f) => f.endsWith(".json") && !f.endsWith(".indexes.json"))
            .map((f) => f.replace(".json", ""));

          return collectionNames.map((name) => ({
            name,
            type: "collection" as const,
            options: {},
            info: { readOnly: false },
          }));
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === "ENOENT") {
            return [];
          }
          throw error;
        }
      },
      filter,
      options
    );
  }

  /**
   * Get statistics about this database.
   *
   * Returns information about collections, documents, sizes, and indexes.
   *
   * @returns Database statistics object
   *
   * @example
   * ```typescript
   * const stats = await db.stats();
   * console.log(`Collections: ${stats.collections}`);
   * console.log(`Documents: ${stats.objects}`);
   * console.log(`Size: ${stats.dataSize} bytes`);
   * ```
   */
  async stats(): Promise<DbStats> {
    const dbPath = join(this.dataDir, this.name);

    let collections = 0;
    let objects = 0;
    let dataSize = 0;
    let indexes = 0;
    let indexSize = 0;

    try {
      const files = await readdir(dbPath);

      for (const file of files) {
        const filePath = join(dbPath, file);
        const fileStat = await stat(filePath);

        if (file.endsWith(".indexes.json")) {
          // Read index count from file
          try {
            const content = JSON.parse(await readFile(filePath, "utf-8"));
            indexes += content.indexes?.length || 0;
          } catch {
            // If file can't be parsed, assume 0 indexes
          }
          indexSize += fileStat.size;
        } else if (file.endsWith(".json")) {
          collections++;
          dataSize += fileStat.size;
          // Read document count
          try {
            const content = JSON.parse(await readFile(filePath, "utf-8"));
            objects += Array.isArray(content) ? content.length : 0;
          } catch {
            // If file can't be parsed, assume 0 documents
          }
        }
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw error;
      }
      // Database doesn't exist, all values stay at 0
    }

    return {
      db: this.name,
      collections,
      views: 0,
      objects,
      dataSize,
      storageSize: dataSize,
      indexes,
      indexSize,
      totalSize: dataSize + indexSize,
      ok: 1,
    };
  }
}

/**
 * Cursor for iterating through collection info from listCollections.
 */
class ListCollectionsCursor {
  private results: CollectionInfo[] | null = null;
  private loader: () => Promise<CollectionInfo[]>;
  private filter: Filter<CollectionInfo>;
  private options: ListCollectionsOptions;

  constructor(
    loader: () => Promise<CollectionInfo[]>,
    filter: Filter<CollectionInfo> = {},
    options: ListCollectionsOptions = {}
  ) {
    this.loader = loader;
    this.filter = filter;
    this.options = options;
  }

  /**
   * Execute the query and return all matching collections as an array.
   * @returns Array of collection information documents
   */
  async toArray(): Promise<CollectionInfo[]> {
    if (this.results === null) {
      this.results = await this.loader();

      // Apply filter if provided
      if (Object.keys(this.filter).length > 0) {
        this.results = this.results.filter((doc) =>
          matchesFilter(doc as unknown as Document, this.filter as Filter<Document>)
        );
      }
    }

    if (this.options.nameOnly) {
      return this.results.map((c) => ({
        name: c.name,
        type: c.type,
      }));
    }
    return this.results;
  }

  /**
   * Alias for toArray() - makes cursor compatible with for-await.
   */
  async *[Symbol.asyncIterator](): AsyncIterator<CollectionInfo> {
    const results = await this.toArray();
    for (const doc of results) {
      yield doc;
    }
  }
}
