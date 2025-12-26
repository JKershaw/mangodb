import {
  applyProjection,
  getValueByPath,
  compareValuesForSort,
  type ProjectionSpec,
} from "./utils.ts";
import { BadHintError } from "./errors.ts";
import type { IndexInfo } from "./types.ts";

type Document = Record<string, unknown>;
type SortSpec = Record<string, 1 | -1>;
type HintSpec = string | Record<string, unknown>;

/**
 * Function to validate an index hint.
 * Returns true if the hint is valid, false otherwise.
 */
export type HintValidator = (hint: HintSpec) => Promise<boolean>;

/**
 * IndexCursor represents a cursor over index information.
 * Provides a minimal cursor API for listIndexes() compatibility with the MongoDB driver.
 * Used to iterate over database indexes returned by collection.listIndexes().
 *
 * @example
 * ```typescript
 * const indexCursor = collection.listIndexes();
 * const indexes = await indexCursor.toArray();
 * console.log(indexes); // [{ v: 2, key: { _id: 1 }, name: '_id_' }, ...]
 * ```
 */
export class IndexCursor {
  private readonly fetchIndexes: () => Promise<IndexInfo[]>;

  /**
   * Creates a new IndexCursor instance.
   *
   * @param fetchIndexes - Function that returns a promise resolving to an array of index information
   */
  constructor(fetchIndexes: () => Promise<IndexInfo[]>) {
    this.fetchIndexes = fetchIndexes;
  }

  /**
   * Returns all indexes as an array.
   * Executes the fetch operation and returns the complete list of indexes.
   *
   * @returns Promise resolving to an array of index information objects
   *
   * @example
   * ```typescript
   * const indexes = await indexCursor.toArray();
   * indexes.forEach(index => {
   *   console.log(`Index: ${index.name}, Keys: ${JSON.stringify(index.key)}`);
   * });
   * ```
   */
  async toArray(): Promise<IndexInfo[]> {
    return this.fetchIndexes();
  }
}

/**
 * MangoCursor represents a cursor over query results.
 * It mirrors the Cursor API from the official MongoDB driver, providing chainable methods
 * for sorting, limiting, skipping, and projecting query results.
 *
 * @template T - The document type (defaults to generic Document)
 *
 * @example
 * ```typescript
 * const cursor = collection.find({ status: 'active' });
 * const results = await cursor
 *   .sort({ createdAt: -1 })
 *   .skip(10)
 *   .limit(5)
 *   .toArray();
 * ```
 */
/**
 * Options for MangoCursor constructor.
 */
export interface CursorOptions {
  /** If true, documents are pre-sorted by geo distance and should not be re-sorted */
  geoSorted?: boolean;
}

export class MangoCursor<T extends Document = Document> {
  private readonly fetchDocuments: () => Promise<T[]>;
  private sortSpec: SortSpec | null = null;
  private limitValue: number | null = null;
  private skipValue: number | null = null;
  private projectionSpec: ProjectionSpec | null = null;
  private hintSpec: HintSpec | null = null;
  private readonly hintValidator: HintValidator | null = null;
  private readonly geoSorted: boolean = false;

  /**
   * Creates a new MangoCursor instance.
   *
   * @param fetchDocuments - Function that returns a promise resolving to an array of documents
   * @param projection - Optional projection specification to apply to all results
   * @param hintValidator - Optional function to validate index hints
   * @param options - Optional cursor options
   */
  constructor(
    fetchDocuments: () => Promise<T[]>,
    projection?: ProjectionSpec | null,
    hintValidator?: HintValidator | null,
    options?: CursorOptions
  ) {
    this.fetchDocuments = fetchDocuments;
    this.projectionSpec = projection || null;
    this.hintValidator = hintValidator || null;
    this.geoSorted = options?.geoSorted ?? false;
  }

  /**
   * Sorts the query results by the specified fields.
   * Multiple fields can be specified for multi-level sorting.
   * Returns this cursor for chaining.
   *
   * @param spec - Sort specification where keys are field names and values are 1 (ascending) or -1 (descending)
   * @returns This cursor instance for method chaining
   *
   * @example
   * ```typescript
   * // Sort by age descending, then name ascending
   * cursor.sort({ age: -1, name: 1 });
   *
   * // Sort by single field
   * cursor.sort({ createdAt: -1 });
   * ```
   */
  sort(spec: SortSpec): MangoCursor<T> {
    this.sortSpec = spec;
    return this;
  }

  /**
   * Limits the number of documents returned by the query.
   * Negative values are treated as positive (MongoDB 3.2+ behavior).
   * A limit of 0 means no limit and returns all documents.
   * Returns this cursor for chaining.
   *
   * @param n - Maximum number of documents to return (0 for no limit)
   * @returns This cursor instance for method chaining
   *
   * @example
   * ```typescript
   * // Return at most 10 documents
   * cursor.limit(10);
   *
   * // Remove any previous limit (return all)
   * cursor.limit(0);
   *
   * // Negative values are treated as positive
   * cursor.limit(-5); // Same as limit(5)
   * ```
   */
  limit(n: number): MangoCursor<T> {
    const absN = Math.abs(n);
    // limit(0) means no limit in MongoDB
    this.limitValue = absN === 0 ? null : absN;
    return this;
  }

  /**
   * Skips the first n documents in the query results.
   * Useful for pagination when combined with limit().
   * Returns this cursor for chaining.
   *
   * @param n - Number of documents to skip (must be non-negative)
   * @returns This cursor instance for method chaining
   * @throws Error if n is negative (matches MongoDB behavior)
   *
   * @example
   * ```typescript
   * // Skip the first 20 documents (e.g., for pagination)
   * cursor.skip(20).limit(10);
   *
   * // Page 3 with 10 items per page
   * const page = 3;
   * const pageSize = 10;
   * cursor.skip((page - 1) * pageSize).limit(pageSize);
   * ```
   */
  skip(n: number): MangoCursor<T> {
    if (n < 0) {
      throw new Error("Skip value must be non-negative");
    }
    this.skipValue = n;
    return this;
  }

  /**
   * Forces the query to use a specific index.
   * The hint can be an index name (string) or a key pattern (object).
   * Returns this cursor for chaining.
   *
   * @param indexHint - Index name or key pattern to use
   * @returns This cursor instance for method chaining
   *
   * @example
   * ```typescript
   * // Hint by index name
   * cursor.hint("email_1");
   *
   * // Hint by key pattern
   * cursor.hint({ email: 1 });
   *
   * // $natural hint for collection scan order
   * cursor.hint({ $natural: 1 });  // Forward scan
   * cursor.hint({ $natural: -1 }); // Reverse scan
   * ```
   */
  hint(indexHint: string | Record<string, unknown>): MangoCursor<T> {
    this.hintSpec = indexHint;
    return this;
  }

  /**
   * Executes the query and returns all matching documents as an array.
   * Applies all cursor modifiers in the following order:
   * 1. Hint validation - Validates the specified index exists
   * 2. $natural hint - Applies reverse scan if $natural: -1
   * 3. Sort - Orders documents by specified fields
   * 4. Skip - Skips the first n documents
   * 5. Limit - Restricts the number of documents returned
   * 6. Projection - Shapes the documents by including/excluding fields
   *
   * @returns Promise resolving to an array of documents matching the query and cursor options
   * @throws BadHintError if hint specifies a non-existent index
   *
   * @example
   * ```typescript
   * // Simple query
   * const users = await collection.find({ active: true }).toArray();
   *
   * // With cursor modifiers
   * const recentPosts = await collection
   *   .find({ published: true })
   *   .sort({ createdAt: -1 })
   *   .limit(10)
   *   .toArray();
   *
   * // Pagination example
   * const page2 = await collection
   *   .find({})
   *   .sort({ _id: 1 })
   *   .skip(20)
   *   .limit(20)
   *   .toArray();
   * ```
   */
  async toArray(): Promise<T[]> {
    // Validate hint if specified (but not for $natural which is always valid)
    if (this.hintSpec) {
      const isNaturalHint =
        typeof this.hintSpec === "object" && "$natural" in this.hintSpec;

      if (!isNaturalHint && this.hintValidator) {
        const isValid = await this.hintValidator(this.hintSpec);
        if (!isValid) {
          throw new BadHintError(this.hintSpec);
        }
      }
    }

    let docs = await this.fetchDocuments();

    // Handle $natural hint for scan direction
    if (
      this.hintSpec &&
      typeof this.hintSpec === "object" &&
      "$natural" in this.hintSpec
    ) {
      const direction = this.hintSpec.$natural;
      if (direction === -1) {
        docs = [...docs].reverse();
      }
    }

    // Apply sort (skip if geoSorted and no explicit sort was requested)
    // For geo queries, documents are pre-sorted by distance
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
    // Note: if geoSorted is true and no sortSpec, docs are already sorted by distance

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
