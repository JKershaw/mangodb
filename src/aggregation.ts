/**
 * Aggregation Pipeline for Mongone.
 *
 * This module provides MongoDB-compatible aggregation pipeline functionality.
 * Supports basic stages: $match, $project, $sort, $limit, $skip, $count, $unwind.
 */
import { matchesFilter } from "./query-matcher.ts";
import {
  getValueByPath,
  setValueByPath,
  compareValuesForSort,
} from "./utils.ts";
import { cloneDocument } from "./document-utils.ts";
import type {
  Document,
  Filter,
  PipelineStage,
  SortSpec,
  UnwindOptions,
  ProjectExpression,
} from "./types.ts";

/**
 * AggregationCursor represents a cursor over aggregation pipeline results.
 *
 * It executes a series of pipeline stages against a collection of documents,
 * transforming them according to each stage's operation.
 *
 * @template T - The input document type
 *
 * @example
 * ```typescript
 * const cursor = collection.aggregate([
 *   { $match: { status: "active" } },
 *   { $sort: { createdAt: -1 } },
 *   { $limit: 10 }
 * ]);
 * const results = await cursor.toArray();
 * ```
 */
export class AggregationCursor<T extends Document = Document> {
  private readonly source: () => Promise<T[]>;
  private readonly pipeline: PipelineStage[];

  /**
   * Creates a new AggregationCursor instance.
   *
   * @param source - Function that returns a promise resolving to source documents
   * @param pipeline - Array of pipeline stages to execute
   */
  constructor(source: () => Promise<T[]>, pipeline: PipelineStage[]) {
    this.source = source;
    this.pipeline = pipeline;
  }

  /**
   * Executes the aggregation pipeline and returns all results as an array.
   *
   * Stages are executed sequentially, with each stage transforming the
   * document stream for the next stage.
   *
   * @returns Promise resolving to an array of result documents
   * @throws Error if an unknown pipeline stage is encountered
   *
   * @example
   * ```typescript
   * const results = await collection.aggregate([
   *   { $match: { age: { $gte: 18 } } },
   *   { $project: { name: 1, age: 1 } }
   * ]).toArray();
   * ```
   */
  async toArray(): Promise<Document[]> {
    let documents: Document[] = await this.source();

    for (const stage of this.pipeline) {
      documents = this.executeStage(stage, documents);
    }

    return documents;
  }

  /**
   * Execute a single pipeline stage on the document stream.
   */
  private executeStage(stage: PipelineStage, docs: Document[]): Document[] {
    const stageKeys = Object.keys(stage);
    if (stageKeys.length !== 1) {
      throw new Error("Pipeline stage must have exactly one field");
    }

    const stageKey = stageKeys[0];
    const stageValue = (stage as Record<string, unknown>)[stageKey];

    switch (stageKey) {
      case "$match":
        return this.execMatch(stageValue as Filter<Document>, docs);
      case "$project":
        return this.execProject(
          stageValue as Record<string, 0 | 1 | string | ProjectExpression>,
          docs
        );
      case "$sort":
        return this.execSort(stageValue as SortSpec, docs);
      case "$limit":
        return this.execLimit(stageValue as number, docs);
      case "$skip":
        return this.execSkip(stageValue as number, docs);
      case "$count":
        return this.execCount(stageValue as string, docs);
      case "$unwind":
        return this.execUnwind(stageValue as string | UnwindOptions, docs);
      default:
        throw new Error(`Unrecognized pipeline stage name: '${stageKey}'`);
    }
  }

  // ==================== Stage Implementations ====================

  /**
   * $match stage - Filter documents using query syntax.
   * Reuses the existing matchesFilter logic from query-matcher.
   */
  private execMatch(filter: Filter<Document>, docs: Document[]): Document[] {
    return docs.filter((doc) => matchesFilter(doc, filter));
  }

  /**
   * $project stage - Reshape documents by including, excluding, or renaming fields.
   *
   * Rules:
   * - Cannot mix inclusion (1) and exclusion (0) except for _id
   * - _id is included by default unless explicitly set to 0
   * - Field references use $ prefix: { newName: "$existingField" }
   * - $literal can be used for literal values: { value: { $literal: 1 } }
   */
  private execProject(
    projection: Record<string, 0 | 1 | string | ProjectExpression>,
    docs: Document[]
  ): Document[] {
    const keys = Object.keys(projection);
    if (keys.length === 0) {
      throw new Error("$project requires at least one field");
    }

    // Determine projection mode
    const nonIdKeys = keys.filter((k) => k !== "_id");
    let hasInclusion = false;
    let hasExclusion = false;
    let hasFieldRef = false;
    let hasLiteral = false;

    for (const key of nonIdKeys) {
      const value = projection[key];
      if (value === 1) {
        hasInclusion = true;
      } else if (value === 0) {
        hasExclusion = true;
      } else if (typeof value === "string" && value.startsWith("$")) {
        hasFieldRef = true;
      } else if (this.isLiteralExpression(value)) {
        hasLiteral = true;
      }
    }

    // Field refs and literals count as inclusion mode
    if (hasFieldRef || hasLiteral) {
      hasInclusion = true;
    }

    // Cannot mix inclusion and exclusion (except _id)
    if (hasInclusion && hasExclusion) {
      throw new Error("Cannot mix inclusion and exclusion in projection");
    }

    return docs.map((doc) => this.projectDocument(doc, projection, hasExclusion));
  }

  /**
   * Check if a value is a $literal expression.
   */
  private isLiteralExpression(value: unknown): value is ProjectExpression {
    return (
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      "$literal" in value
    );
  }

  /**
   * Apply projection to a single document.
   */
  private projectDocument(
    doc: Document,
    projection: Record<string, 0 | 1 | string | ProjectExpression>,
    isExclusionMode: boolean
  ): Document {
    const keys = Object.keys(projection);

    if (isExclusionMode) {
      // Exclusion mode: start with all fields, remove specified
      const result = cloneDocument(doc);
      for (const key of keys) {
        if (projection[key] === 0) {
          if (key.includes(".")) {
            // Handle nested field exclusion
            const parts = key.split(".");
            let current: Record<string, unknown> = result;
            for (let i = 0; i < parts.length - 1; i++) {
              if (current[parts[i]] && typeof current[parts[i]] === "object") {
                current = current[parts[i]] as Record<string, unknown>;
              } else {
                break;
              }
            }
            delete current[parts[parts.length - 1]];
          } else {
            delete result[key];
          }
        }
      }
      return result;
    }

    // Inclusion mode: start empty, add specified fields
    const result: Document = {};

    // Handle _id (included by default unless explicitly excluded)
    if (projection._id !== 0) {
      result._id = doc._id;
    }

    // Process other fields
    const nonIdKeys = keys.filter((k) => k !== "_id");
    for (const key of nonIdKeys) {
      const value = projection[key];

      if (value === 1) {
        // Include field
        const fieldValue = getValueByPath(doc, key);
        if (fieldValue !== undefined) {
          if (key.includes(".")) {
            setValueByPath(result, key, fieldValue);
          } else {
            result[key] = fieldValue;
          }
        }
      } else if (typeof value === "string" && value.startsWith("$")) {
        // Field reference - get value from referenced field
        const refPath = value.slice(1); // Remove $ prefix
        const refValue = getValueByPath(doc, refPath);
        // MongoDB returns null for missing referenced fields (not undefined)
        result[key] = refValue !== undefined ? refValue : null;
      } else if (this.isLiteralExpression(value)) {
        // Literal value
        result[key] = value.$literal;
      }
    }

    return result;
  }

  /**
   * $sort stage - Order documents by specified fields.
   * Reuses the existing sort comparison logic.
   */
  private execSort(sortSpec: SortSpec, docs: Document[]): Document[] {
    const sortFields = Object.entries(sortSpec) as [string, 1 | -1][];

    return [...docs].sort((a, b) => {
      for (const [field, direction] of sortFields) {
        const aValue = getValueByPath(a, field);
        const bValue = getValueByPath(b, field);
        const cmp = compareValuesForSort(aValue, bValue, direction);
        if (cmp !== 0) {
          return direction === 1 ? cmp : -cmp;
        }
      }
      return 0;
    });
  }

  /**
   * $limit stage - Limit output to first n documents.
   */
  private execLimit(limit: number, docs: Document[]): Document[] {
    if (typeof limit !== "number" || limit < 0) {
      throw new Error("$limit must be a non-negative number");
    }
    return docs.slice(0, limit);
  }

  /**
   * $skip stage - Skip first n documents.
   */
  private execSkip(skip: number, docs: Document[]): Document[] {
    if (typeof skip !== "number" || skip < 0) {
      throw new Error("$skip must be a non-negative number");
    }
    return docs.slice(skip);
  }

  /**
   * $count stage - Count documents and return single document with count.
   *
   * Critical behavior: Returns empty array if no documents (not { count: 0 }).
   */
  private execCount(fieldName: string, docs: Document[]): Document[] {
    // Validate field name
    if (!fieldName || typeof fieldName !== "string" || fieldName.length === 0) {
      throw new Error("$count field name must be a non-empty string");
    }
    if (fieldName.startsWith("$")) {
      throw new Error("$count field name cannot start with '$'");
    }
    if (fieldName.includes(".")) {
      throw new Error("$count field name cannot contain '.'");
    }

    // Empty input returns NO document (not { count: 0 })
    if (docs.length === 0) {
      return [];
    }

    return [{ [fieldName]: docs.length }];
  }

  /**
   * $unwind stage - Deconstruct array field into multiple documents.
   *
   * Behavior by input type:
   * - Array with items: One document per element
   * - Non-array value: Treated as single-element array
   * - null: No output (or preserved with option)
   * - Missing field: No output (or preserved with option)
   * - Empty array: No output (or preserved with option)
   */
  private execUnwind(
    unwind: string | UnwindOptions,
    docs: Document[]
  ): Document[] {
    // Parse options
    const path = typeof unwind === "string" ? unwind : unwind.path;
    const preserveNullAndEmpty =
      typeof unwind === "object" && unwind.preserveNullAndEmptyArrays === true;
    const includeArrayIndex =
      typeof unwind === "object" ? unwind.includeArrayIndex : undefined;

    // Validate path
    if (!path.startsWith("$")) {
      throw new Error(
        "$unwind requires a path starting with '$', found: " + path
      );
    }

    const fieldPath = path.slice(1); // Remove $ prefix
    const result: Document[] = [];

    for (const doc of docs) {
      const value = getValueByPath(doc, fieldPath);

      // Handle null/undefined
      if (value === undefined || value === null) {
        if (preserveNullAndEmpty) {
          const newDoc = cloneDocument(doc);
          if (includeArrayIndex) {
            newDoc[includeArrayIndex] = null;
          }
          result.push(newDoc);
        }
        continue;
      }

      // Handle non-array: treat as single-element array
      if (!Array.isArray(value)) {
        const newDoc = cloneDocument(doc);
        if (includeArrayIndex) {
          newDoc[includeArrayIndex] = 0;
        }
        result.push(newDoc);
        continue;
      }

      // Handle empty array
      if (value.length === 0) {
        if (preserveNullAndEmpty) {
          const newDoc = cloneDocument(doc);
          // Remove the empty array field when preserving
          if (fieldPath.includes(".")) {
            setValueByPath(newDoc, fieldPath, null);
          } else {
            delete newDoc[fieldPath];
          }
          if (includeArrayIndex) {
            newDoc[includeArrayIndex] = null;
          }
          result.push(newDoc);
        }
        continue;
      }

      // Unwind array into multiple documents
      for (let i = 0; i < value.length; i++) {
        const newDoc = cloneDocument(doc);
        // Set the field to the individual element
        if (fieldPath.includes(".")) {
          setValueByPath(newDoc, fieldPath, value[i]);
        } else {
          newDoc[fieldPath] = value[i];
        }
        if (includeArrayIndex) {
          newDoc[includeArrayIndex] = i;
        }
        result.push(newDoc);
      }
    }

    return result;
  }
}
