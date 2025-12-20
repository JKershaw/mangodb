import { MongoneDb } from "./db.ts";
import { mkdir } from "node:fs/promises";

/**
 * MongoneClient is the entry point for using Mongone.
 * It mirrors the MongoClient API from the official MongoDB driver.
 */
export class MongoneClient {
  private readonly dataDir: string;
  private connected = false;
  private databases = new Map<string, MongoneDb>();

  /**
   * Create a new MongoneClient.
   * @param dataDir - Directory where data will be stored
   */
  constructor(dataDir: string) {
    this.dataDir = dataDir;
  }

  /**
   * Connect to the Mongone instance.
   * Creates the data directory if it doesn't exist.
   */
  async connect(): Promise<this> {
    await mkdir(this.dataDir, { recursive: true });
    this.connected = true;
    return this;
  }

  /**
   * Close the Mongone client connection.
   */
  async close(): Promise<void> {
    this.connected = false;
    this.databases.clear();
  }

  /**
   * Get a database instance.
   * @param name - Database name
   */
  db(name: string): MongoneDb {
    if (!this.databases.has(name)) {
      this.databases.set(name, new MongoneDb(this.dataDir, name));
    }
    return this.databases.get(name)!;
  }
}
