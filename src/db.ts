import { MongoneCollection } from "./collection.ts";
import { rm } from "node:fs/promises";
import { join } from "node:path";

/**
 * MongoneDb represents a database in Mongone.
 * It mirrors the Db API from the official MongoDB driver.
 */
export class MongoneDb {
  private readonly dataDir: string;
  private readonly name: string;
  private collections = new Map<string, MongoneCollection<Document>>();

  constructor(dataDir: string, name: string) {
    this.dataDir = dataDir;
    this.name = name;
  }

  /**
   * Get a collection instance.
   * @param name - Collection name
   */
  collection<T extends Document = Document>(name: string): MongoneCollection<T> {
    if (!this.collections.has(name)) {
      this.collections.set(
        name,
        new MongoneCollection<Document>(this.dataDir, this.name, name)
      );
    }
    return this.collections.get(name)! as MongoneCollection<T>;
  }

  /**
   * Drop the database.
   */
  async dropDatabase(): Promise<void> {
    const dbPath = join(this.dataDir, this.name);
    await rm(dbPath, { recursive: true, force: true });
    this.collections.clear();
  }
}

type Document = Record<string, unknown>;
