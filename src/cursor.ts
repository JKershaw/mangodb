import {
  applyProjection,
  getValueByPath,
  compareValuesForSort,
  type ProjectionSpec,
} from "./utils.ts";

type Document = Record<string, unknown>;

/**
 * Information about an index.
 * Duplicated here to avoid circular imports.
 */
interface IndexInfo {
  v: number;
  key: Record<string, 1 | -1>;
  name: string;
  unique?: boolean;
  sparse?: boolean;
}

/**
 * IndexCursor represents a cursor over index information.
 * Provides a minimal cursor API for listIndexes() compatibility.
 */
export class IndexCursor {
  private readonly fetchIndexes: () => Promise<IndexInfo[]>;

  constructor(fetchIndexes: () => Promise<IndexInfo[]>) {
    this.fetchIndexes = fetchIndexes;
  }

  /**
   * Return all indexes as an array.
   */
  async toArray(): Promise<IndexInfo[]> {
    return this.fetchIndexes();
  }
}

/**
 * MongoneCursor represents a cursor over query results.
 * It mirrors the Cursor API from the official MongoDB driver.
 */
export class MongoneCursor<T extends Document = Document> {
  private readonly fetchDocuments: () => Promise<T[]>;
  private sortSpec: SortSpec | null = null;
  private limitValue: number | null = null;
  private skipValue: number | null = null;
  private projectionSpec: ProjectionSpec | null = null;

  constructor(
    fetchDocuments: () => Promise<T[]>,
    projection?: ProjectionSpec | null
  ) {
    this.fetchDocuments = fetchDocuments;
    this.projectionSpec = projection || null;
  }

  /**
   * Sort the results by the specified fields.
   * Returns this cursor for chaining.
   */
  sort(spec: SortSpec): MongoneCursor<T> {
    this.sortSpec = spec;
    return this;
  }

  /**
   * Limit the number of results returned.
   * Returns this cursor for chaining.
   * Negative values are treated as positive (MongoDB 3.2+ behavior).
   * limit(0) means no limit (returns all documents).
   */
  limit(n: number): MongoneCursor<T> {
    const absN = Math.abs(n);
    // limit(0) means no limit in MongoDB
    this.limitValue = absN === 0 ? null : absN;
    return this;
  }

  /**
   * Skip the first n results.
   * Returns this cursor for chaining.
   * @throws Error if n is negative (MongoDB behavior).
   */
  skip(n: number): MongoneCursor<T> {
    if (n < 0) {
      throw new Error("Skip value must be non-negative");
    }
    this.skipValue = n;
    return this;
  }

  /**
   * Return all documents as an array.
   * Applies sort, skip, and limit in that order.
   */
  async toArray(): Promise<T[]> {
    let docs = await this.fetchDocuments();

    // Apply sort
    if (this.sortSpec) {
      const sortFields = Object.entries(this.sortSpec) as [
        string,
        1 | -1,
      ][];
      docs = [...docs].sort((a, b) => {
        for (const [field, direction] of sortFields) {
          const aValue = getValueByPath(a, field);
          const bValue = getValueByPath(b, field);
          // Use direction-aware comparison for proper array handling
          const comparison = compareValuesForSort(aValue, bValue, direction);
          if (comparison !== 0) {
            return direction === 1 ? comparison : -comparison;
          }
        }
        return 0;
      });
    }

    // Apply skip
    if (this.skipValue !== null && this.skipValue > 0) {
      docs = docs.slice(this.skipValue);
    }

    // Apply limit
    if (this.limitValue !== null) {
      docs = docs.slice(0, this.limitValue);
    }

    // Apply projection
    if (this.projectionSpec) {
      docs = docs.map((doc) => applyProjection(doc, this.projectionSpec!));
    }

    return docs;
  }
}
