import { ObjectId } from "mongodb";
import { MongoneCursor } from "./cursor.ts";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join, dirname } from "node:path";

type Document = Record<string, unknown>;

interface InsertOneResult {
  acknowledged: boolean;
  insertedId: ObjectId;
}

interface InsertManyResult {
  acknowledged: boolean;
  insertedIds: Record<number, ObjectId>;
}

interface DeleteResult {
  acknowledged: boolean;
  deletedCount: number;
}

/**
 * MongoneCollection represents a collection in Mongone.
 * It mirrors the Collection API from the official MongoDB driver.
 */
export class MongoneCollection<T extends Document = Document> {
  private readonly filePath: string;

  constructor(dataDir: string, dbName: string, collectionName: string) {
    this.filePath = join(dataDir, dbName, `${collectionName}.json`);
  }

  /**
   * Read all documents from the collection file.
   */
  private async readDocuments(): Promise<T[]> {
    try {
      const content = await readFile(this.filePath, "utf-8");
      const parsed = JSON.parse(content);
      // Restore ObjectId instances from serialized format
      return parsed.map((doc: T) => this.deserializeDocument(doc));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return [];
      }
      throw error;
    }
  }

  /**
   * Write all documents to the collection file.
   */
  private async writeDocuments(documents: T[]): Promise<void> {
    await mkdir(dirname(this.filePath), { recursive: true });
    const serialized = documents.map((doc) => this.serializeDocument(doc));
    await writeFile(this.filePath, JSON.stringify(serialized, null, 2));
  }

  /**
   * Serialize a document for JSON storage.
   * Converts ObjectId to a special format that can be restored.
   */
  private serializeDocument(doc: T): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(doc)) {
      if (value instanceof ObjectId) {
        result[key] = { $oid: value.toHexString() };
      } else if (value && typeof value === "object" && !Array.isArray(value)) {
        result[key] = this.serializeDocument(value as T);
      } else if (Array.isArray(value)) {
        result[key] = value.map((item) =>
          item && typeof item === "object" && !Array.isArray(item)
            ? this.serializeDocument(item as T)
            : item instanceof ObjectId
              ? { $oid: item.toHexString() }
              : item
        );
      } else {
        result[key] = value;
      }
    }
    return result;
  }

  /**
   * Deserialize a document from JSON storage.
   * Restores ObjectId from the special format.
   */
  private deserializeDocument(doc: Record<string, unknown>): T {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(doc)) {
      if (
        value &&
        typeof value === "object" &&
        !Array.isArray(value) &&
        "$oid" in value &&
        typeof (value as { $oid: unknown }).$oid === "string"
      ) {
        result[key] = new ObjectId((value as { $oid: string }).$oid);
      } else if (value && typeof value === "object" && !Array.isArray(value)) {
        result[key] = this.deserializeDocument(value as Record<string, unknown>);
      } else if (Array.isArray(value)) {
        result[key] = value.map((item) => {
          if (
            item &&
            typeof item === "object" &&
            !Array.isArray(item) &&
            "$oid" in item &&
            typeof (item as { $oid: unknown }).$oid === "string"
          ) {
            return new ObjectId((item as { $oid: string }).$oid);
          } else if (item && typeof item === "object" && !Array.isArray(item)) {
            return this.deserializeDocument(item as Record<string, unknown>);
          }
          return item;
        });
      } else {
        result[key] = value;
      }
    }
    return result as T;
  }

  /**
   * Check if a document matches a filter.
   * Currently supports empty filter and simple equality.
   */
  private matchesFilter(doc: T, filter: Partial<T>): boolean {
    for (const [key, value] of Object.entries(filter)) {
      const docValue = doc[key];

      // Handle ObjectId comparison
      if (value instanceof ObjectId) {
        if (!(docValue instanceof ObjectId)) {
          return false;
        }
        if (!value.equals(docValue)) {
          return false;
        }
      } else if (docValue !== value) {
        return false;
      }
    }
    return true;
  }

  /**
   * Insert a single document into the collection.
   */
  async insertOne(doc: T): Promise<InsertOneResult> {
    const documents = await this.readDocuments();

    // Generate _id if not present
    const docWithId = { ...doc };
    if (!("_id" in docWithId)) {
      (docWithId as Record<string, unknown>)._id = new ObjectId();
    }

    documents.push(docWithId);
    await this.writeDocuments(documents);

    return {
      acknowledged: true,
      insertedId: (docWithId as unknown as { _id: ObjectId })._id,
    };
  }

  /**
   * Insert multiple documents into the collection.
   */
  async insertMany(docs: T[]): Promise<InsertManyResult> {
    const documents = await this.readDocuments();
    const insertedIds: Record<number, ObjectId> = {};

    for (let i = 0; i < docs.length; i++) {
      const docWithId = { ...docs[i] };
      if (!("_id" in docWithId)) {
        (docWithId as Record<string, unknown>)._id = new ObjectId();
      }
      documents.push(docWithId);
      insertedIds[i] = (docWithId as unknown as { _id: ObjectId })._id;
    }

    await this.writeDocuments(documents);

    return {
      acknowledged: true,
      insertedIds,
    };
  }

  /**
   * Find a single document matching the filter.
   */
  async findOne(filter: Partial<T> = {}): Promise<T | null> {
    const documents = await this.readDocuments();

    for (const doc of documents) {
      if (this.matchesFilter(doc, filter)) {
        return doc;
      }
    }

    return null;
  }

  /**
   * Find documents matching the filter.
   * Returns a cursor for further operations.
   */
  find(filter: Partial<T> = {}): MongoneCursor<T> {
    return new MongoneCursor<T>(async () => {
      const documents = await this.readDocuments();
      return documents.filter((doc) => this.matchesFilter(doc, filter));
    });
  }

  /**
   * Delete a single document matching the filter.
   */
  async deleteOne(filter: Partial<T>): Promise<DeleteResult> {
    const documents = await this.readDocuments();
    let deletedCount = 0;

    const remaining: T[] = [];
    let deleted = false;

    for (const doc of documents) {
      if (!deleted && this.matchesFilter(doc, filter)) {
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
   */
  async deleteMany(filter: Partial<T>): Promise<DeleteResult> {
    const documents = await this.readDocuments();
    const remaining = documents.filter((doc) => !this.matchesFilter(doc, filter));
    const deletedCount = documents.length - remaining.length;

    await this.writeDocuments(remaining);

    return {
      acknowledged: true,
      deletedCount,
    };
  }
}
