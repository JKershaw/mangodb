/**
 * Index management for Mongone collections.
 */
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { Document, IndexKeySpec, IndexInfo, CreateIndexOptions } from "./types.ts";
import { getValueByPath } from "./document-utils.ts";
import { MongoDuplicateKeyError, IndexNotFoundError, CannotDropIdIndexError } from "./errors.ts";

/**
 * Default _id index that exists on all collections.
 */
const DEFAULT_ID_INDEX: IndexInfo = { v: 2, key: { _id: 1 }, name: "_id_" };

/**
 * IndexManager handles index operations for a collection.
 */
export class IndexManager {
  private readonly indexFilePath: string;
  private readonly dbName: string;
  private readonly collectionName: string;

  constructor(indexFilePath: string, dbName: string, collectionName: string) {
    this.indexFilePath = indexFilePath;
    this.dbName = dbName;
    this.collectionName = collectionName;
  }

  /**
   * Read index metadata from the index file.
   */
  async loadIndexes(): Promise<IndexInfo[]> {
    try {
      const content = await readFile(this.indexFilePath, "utf-8");
      const parsed = JSON.parse(content);
      return parsed.indexes || [];
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return [DEFAULT_ID_INDEX];
      }
      throw error;
    }
  }

  /**
   * Write index metadata to the index file.
   */
  async saveIndexes(indexes: IndexInfo[]): Promise<void> {
    await mkdir(dirname(this.indexFilePath), { recursive: true });
    await writeFile(this.indexFilePath, JSON.stringify({ indexes }, null, 2));
  }

  /**
   * Generate an index name from the key specification.
   */
  generateIndexName(keySpec: IndexKeySpec): string {
    return Object.entries(keySpec)
      .map(([field, direction]) => `${field}_${direction}`)
      .join("_");
  }

  /**
   * Check if two key specifications are equivalent.
   */
  keySpecsEqual(a: IndexKeySpec, b: IndexKeySpec): boolean {
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b);
    if (aKeys.length !== bKeys.length) return false;
    for (let i = 0; i < aKeys.length; i++) {
      if (aKeys[i] !== bKeys[i]) return false;
      if (a[aKeys[i]] !== b[bKeys[i]]) return false;
    }
    return true;
  }

  /**
   * Create an index on the collection.
   */
  async createIndex(
    keySpec: IndexKeySpec,
    options: CreateIndexOptions = {}
  ): Promise<string> {
    const indexes = await this.loadIndexes();
    const indexName = options.name || this.generateIndexName(keySpec);

    const existingBySpec = indexes.find((idx) => this.keySpecsEqual(idx.key, keySpec));
    if (existingBySpec) {
      return existingBySpec.name;
    }

    const newIndex: IndexInfo = {
      v: 2,
      key: keySpec,
      name: indexName,
    };

    if (options.unique) {
      newIndex.unique = true;
    }

    if (options.sparse) {
      newIndex.sparse = true;
    }

    indexes.push(newIndex);
    await this.saveIndexes(indexes);

    return indexName;
  }

  /**
   * Drop an index from the collection.
   */
  async dropIndex(indexNameOrSpec: string | IndexKeySpec): Promise<void> {
    const indexes = await this.loadIndexes();

    let indexName: string;
    if (typeof indexNameOrSpec === "string") {
      indexName = indexNameOrSpec;
    } else {
      indexName = this.generateIndexName(indexNameOrSpec);
    }

    if (indexName === "_id_") {
      throw new CannotDropIdIndexError();
    }

    if (
      typeof indexNameOrSpec === "object" &&
      this.keySpecsEqual(indexNameOrSpec, { _id: 1 })
    ) {
      throw new CannotDropIdIndexError();
    }

    const indexIdx = indexes.findIndex((idx) => idx.name === indexName);
    if (indexIdx === -1) {
      throw new IndexNotFoundError(indexName);
    }

    indexes.splice(indexIdx, 1);
    await this.saveIndexes(indexes);
  }

  /**
   * List all indexes on the collection.
   */
  async indexes(): Promise<IndexInfo[]> {
    return this.loadIndexes();
  }

  /**
   * Extract the key value from a document for a given index key specification.
   */
  extractKeyValue<T extends Document>(
    doc: T,
    keySpec: IndexKeySpec
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const field of Object.keys(keySpec)) {
      result[field] = getValueByPath(doc, field);
    }
    return result;
  }

  /**
   * Check unique constraints for documents being inserted or updated.
   */
  async checkUniqueConstraints<T extends Document>(
    docs: T[],
    existingDocs: T[],
    excludeIds: Set<string> = new Set()
  ): Promise<void> {
    const indexes = await this.loadIndexes();
    const uniqueIndexes = indexes.filter((idx) => idx.unique);

    if (uniqueIndexes.length === 0) {
      return;
    }

    const existingValues = new Map<string, Map<string, T>>();
    for (const idx of uniqueIndexes) {
      const valueMap = new Map<string, T>();
      for (const doc of existingDocs) {
        const docId = (doc as { _id?: { toHexString(): string } })._id;
        if (docId && excludeIds.has(docId.toHexString())) {
          continue;
        }
        const keyValue = this.extractKeyValue(doc, idx.key);
        const keyStr = JSON.stringify(keyValue);
        valueMap.set(keyStr, doc);
      }
      existingValues.set(idx.name, valueMap);
    }

    for (const doc of docs) {
      for (const idx of uniqueIndexes) {
        const keyValue = this.extractKeyValue(doc, idx.key);
        const keyStr = JSON.stringify(keyValue);
        const valueMap = existingValues.get(idx.name)!;

        if (valueMap.has(keyStr)) {
          throw new MongoDuplicateKeyError(
            this.dbName,
            this.collectionName,
            idx.name,
            idx.key,
            keyValue
          );
        }

        valueMap.set(keyStr, doc);
      }
    }
  }
}
