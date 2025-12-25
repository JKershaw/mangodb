/**
 * AggregationCursor and pipeline stage implementations.
 */
import { matchesFilter } from "../query-matcher.ts";
import {
  getValueByPath,
  setValueByPath,
  compareValuesForSort,
} from "../utils.ts";
import { cloneDocument, valuesEqual } from "../document-utils.ts";
import type {
  Document,
  Filter,
  PipelineStage,
  SortSpec,
  UnwindOptions,
  ProjectExpression,
} from "../types.ts";
import type { AggregationDbContext } from "./types.ts";
import { evaluateExpression } from "./expression.ts";
import { createAccumulator } from "./accumulators.ts";
import { getBSONTypeName } from "./helpers.ts";
import { createSystemVars, mergeVars, REDACT_DESCEND, REDACT_PRUNE, REDACT_KEEP } from "./system-vars.ts";
import { traverseDocument, type TraversalAction } from "./traverse.ts";
import { partitionDocuments } from "./partition.ts";
import { addDateStep, type TimeUnit, isValidTimeUnit } from "./date-utils.ts";

/**
 * AggregationCursor represents a cursor over aggregation pipeline results.
 */
export class AggregationCursor<T extends Document = Document> {
  private readonly source: () => Promise<T[]>;
  private readonly pipeline: PipelineStage[];
  private readonly dbContext?: AggregationDbContext;
  private pipelineNow?: Date;

  constructor(
    source: () => Promise<T[]>,
    pipeline: PipelineStage[],
    dbContext?: AggregationDbContext
  ) {
    this.source = source;
    this.pipeline = pipeline;
    this.dbContext = dbContext;
  }

  /**
   * Get system variables for a document.
   * Uses shared $$NOW date for consistency across pipeline.
   */
  private getSystemVars(doc: Document): ReturnType<typeof createSystemVars> {
    return createSystemVars(doc, this.pipelineNow);
  }

  async toArray(): Promise<Document[]> {
    // Set shared $$NOW for this pipeline execution
    this.pipelineNow = new Date();

    // Validate stage positions
    for (let i = 0; i < this.pipeline.length; i++) {
      const stage = this.pipeline[i] as unknown as Record<string, unknown>;
      if ("$out" in stage && i !== this.pipeline.length - 1) {
        throw new Error("$out can only be the final stage in the pipeline");
      }
      if ("$documents" in stage && i !== 0) {
        throw new Error("$documents must be the first stage in the pipeline");
      }
    }

    let documents: Document[];

    // Handle $documents as first stage specially
    if (this.pipeline.length > 0) {
      const firstStage = this.pipeline[0] as unknown as Record<string, unknown>;
      if ("$documents" in firstStage) {
        documents = this.execDocuments(firstStage.$documents);
        // Process remaining stages
        for (let i = 1; i < this.pipeline.length; i++) {
          documents = await this.executeStage(this.pipeline[i], documents);
        }
        return documents;
      }
    }

    documents = await this.source();

    for (const stage of this.pipeline) {
      documents = await this.executeStage(stage, documents);
    }

    return documents;
  }

  private async executeStage(
    stage: PipelineStage,
    docs: Document[]
  ): Promise<Document[]> {
    const stageKeys = Object.keys(stage);
    if (stageKeys.length !== 1) {
      throw new Error("Pipeline stage must have exactly one field");
    }

    const stageKey = stageKeys[0];
    const stageValue = (stage as unknown as Record<string, unknown>)[stageKey];

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
      case "$group":
        return this.execGroup(stageValue as { _id: unknown; [key: string]: unknown }, docs);
      case "$lookup":
        return this.execLookup(
          stageValue as { from: string; localField: string; foreignField: string; as: string },
          docs
        );
      case "$addFields":
        return this.execAddFields(stageValue as Record<string, unknown>, docs);
      case "$set":
        return this.execAddFields(stageValue as Record<string, unknown>, docs);
      case "$replaceRoot":
        return this.execReplaceRoot(stageValue as { newRoot: unknown }, docs);
      case "$replaceWith":
        // $replaceWith is an alias for $replaceRoot with simpler syntax
        return this.execReplaceRoot({ newRoot: stageValue }, docs);
      case "$unset":
        return this.execUnset(stageValue, docs);
      case "$redact":
        return this.execRedact(stageValue, docs);
      case "$graphLookup":
        return this.execGraphLookup(
          stageValue as {
            from: string;
            startWith: unknown;
            connectFromField: string;
            connectToField: string;
            as: string;
            maxDepth?: number;
            depthField?: string;
            restrictSearchWithMatch?: Document;
          },
          docs
        );
      case "$densify":
        return this.execDensify(
          stageValue as {
            field: string;
            range: {
              step: number;
              unit?: string;
              bounds?: [unknown, unknown] | "full" | "partition";
            };
            partitionByFields?: string[];
          },
          docs
        );
      case "$out":
        return this.execOut(stageValue as string, docs);
      case "$sortByCount":
        return this.execSortByCount(stageValue, docs);
      case "$sample":
        return this.execSample(stageValue as { size: number }, docs);
      case "$facet":
        return this.execFacet(stageValue as Record<string, PipelineStage[]>, docs);
      case "$bucket":
        return this.execBucket(
          stageValue as {
            groupBy: unknown;
            boundaries: unknown[];
            default?: unknown;
            output?: Record<string, unknown>;
          },
          docs
        );
      case "$bucketAuto":
        return this.execBucketAuto(
          stageValue as {
            groupBy: unknown;
            buckets: number;
            output?: Record<string, unknown>;
            granularity?: string;
          },
          docs
        );
      case "$unionWith":
        return this.execUnionWith(
          stageValue as string | { coll: string; pipeline?: PipelineStage[] },
          docs
        );
      default:
        throw new Error(`Unrecognized pipeline stage name: '${stageKey}'`);
    }
  }

  private execMatch(filter: Filter<Document>, docs: Document[]): Document[] {
    return docs.filter((doc) => matchesFilter(doc, filter));
  }

  private execProject(
    projection: Record<string, 0 | 1 | string | ProjectExpression | unknown>,
    docs: Document[]
  ): Document[] {
    const keys = Object.keys(projection);
    if (keys.length === 0) {
      throw new Error("$project requires at least one field");
    }

    const nonIdKeys = keys.filter((k) => k !== "_id");
    let hasInclusion = false;
    let hasExclusion = false;
    let hasFieldRef = false;
    let hasExpression = false;

    for (const key of nonIdKeys) {
      const value = projection[key];
      if (value === 1) {
        hasInclusion = true;
      } else if (value === 0) {
        hasExclusion = true;
      } else if (typeof value === "string" && value.startsWith("$")) {
        hasFieldRef = true;
      } else if (typeof value === "object" && value !== null) {
        hasExpression = true;
      }
    }

    if (hasFieldRef || hasExpression) {
      hasInclusion = true;
    }

    if (hasInclusion && hasExclusion) {
      throw new Error("Cannot mix inclusion and exclusion in projection");
    }

    const isExclusionMode =
      hasExclusion || (nonIdKeys.length === 0 && projection._id === 0);

    return docs.map((doc) =>
      this.projectDocument(doc, projection, isExclusionMode)
    );
  }

  /**
   * $redact - field-level access control with recursive traversal.
   */
  private execRedact(expr: unknown, docs: Document[]): Document[] {
    const results: Document[] = [];

    for (const doc of docs) {
      const result = traverseDocument(doc, (subdoc) => {
        // Evaluate expression at this document level
        const systemVars = this.getSystemVars(subdoc);
        const action = evaluateExpression(expr, subdoc, systemVars);

        // Validate result
        if (action === REDACT_DESCEND) {
          return "descend";
        } else if (action === REDACT_PRUNE) {
          return "prune";
        } else if (action === REDACT_KEEP) {
          return "keep";
        } else {
          throw new Error(
            "$redact must resolve to $$DESCEND, $$PRUNE, or $$KEEP"
          );
        }
      });

      if (result !== null) {
        results.push(result);
      }
    }

    return results;
  }

  /**
   * $documents - injects literal documents into the pipeline.
   * Must be the first stage.
   */
  private execDocuments(spec: unknown): Document[] {
    // Evaluate expression to get array of documents
    // Use empty doc and system vars since there's no input document
    const systemVars = createSystemVars({}, this.pipelineNow);
    const result = evaluateExpression(spec, {}, systemVars);

    if (!Array.isArray(result)) {
      throw new Error("$documents requires array of documents");
    }

    // Validate all elements are objects
    for (let i = 0; i < result.length; i++) {
      const item = result[i];
      if (item === null || typeof item !== "object" || Array.isArray(item)) {
        throw new Error("$documents array elements must be objects");
      }
    }

    return result as Document[];
  }

  /**
   * $unset - removes specified fields from documents.
   * Alias for $project with field exclusion.
   */
  private execUnset(spec: unknown, docs: Document[]): Document[] {
    // Normalize to array of field paths
    let fields: string[];

    if (typeof spec === "string") {
      fields = [spec];
    } else if (Array.isArray(spec)) {
      fields = spec.map((f, i) => {
        if (typeof f !== "string") {
          throw new Error(
            "$unset specification must be a string or array of strings"
          );
        }
        if (f === "") {
          throw new Error("FieldPath cannot be constructed with empty string");
        }
        return f;
      });
    } else {
      throw new Error(
        "$unset specification must be a string or array of strings"
      );
    }

    // Validate fields
    for (const field of fields) {
      if (field === "") {
        throw new Error("FieldPath cannot be constructed with empty string");
      }
    }

    // Convert to $project exclusion format
    const projection: Record<string, 0> = {};
    for (const field of fields) {
      projection[field] = 0;
    }

    // Delegate to project
    return this.execProject(projection, docs);
  }

  private isLiteralExpression(value: unknown): value is ProjectExpression {
    return (
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      "$literal" in value
    );
  }

  private projectDocument(
    doc: Document,
    projection: Record<string, unknown>,
    isExclusionMode: boolean
  ): Document {
    const keys = Object.keys(projection);

    if (isExclusionMode) {
      const result = cloneDocument(doc);
      for (const key of keys) {
        if (projection[key] === 0) {
          if (key.includes(".")) {
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

    const result: Document = {};

    if (projection._id !== 0) {
      result._id = doc._id;
    }

    const nonIdKeys = keys.filter((k) => k !== "_id");
    for (const key of nonIdKeys) {
      const value = projection[key];

      if (value === 1) {
        const fieldValue = getValueByPath(doc, key);
        if (fieldValue !== undefined) {
          if (key.includes(".")) {
            setValueByPath(result, key, fieldValue);
          } else {
            result[key] = fieldValue;
          }
        }
      } else if (typeof value === "string" && value.startsWith("$$")) {
        // System variable reference (e.g., $$NOW, $$ROOT)
        const evaluated = evaluateExpression(value, doc, this.getSystemVars(doc));
        if (evaluated !== undefined) {
          result[key] = evaluated;
        }
      } else if (typeof value === "string" && value.startsWith("$")) {
        // Field reference (e.g., $fieldName)
        const refPath = value.slice(1);
        const refValue = getValueByPath(doc, refPath);
        if (refValue !== undefined) {
          result[key] = refValue;
        }
      } else if (this.isLiteralExpression(value)) {
        result[key] = value.$literal;
      } else if (typeof value === "object" && value !== null) {
        const evaluated = evaluateExpression(value, doc, this.getSystemVars(doc));
        result[key] = evaluated;
      }
    }

    return result;
  }

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

  private execLimit(limit: number, docs: Document[]): Document[] {
    if (typeof limit !== "number" || !Number.isFinite(limit)) {
      throw new Error(`Expected an integer: $limit: ${limit}`);
    }
    if (!Number.isInteger(limit)) {
      throw new Error(`Expected an integer: $limit: ${limit}`);
    }
    if (limit < 0) {
      throw new Error(`Expected a non-negative number in: $limit: ${limit}`);
    }
    if (limit === 0) {
      throw new Error("the limit must be positive");
    }
    return docs.slice(0, limit);
  }

  private execSkip(skip: number, docs: Document[]): Document[] {
    if (typeof skip !== "number" || !Number.isFinite(skip) || skip < 0) {
      throw new Error("$skip must be a non-negative integer");
    }
    if (!Number.isInteger(skip)) {
      throw new Error("$skip must be a non-negative integer");
    }
    return docs.slice(skip);
  }

  private execCount(fieldName: string, docs: Document[]): Document[] {
    if (!fieldName || typeof fieldName !== "string" || fieldName.length === 0) {
      throw new Error("$count field name must be a non-empty string");
    }
    if (fieldName.startsWith("$")) {
      throw new Error("$count field name cannot start with '$'");
    }
    if (fieldName.includes(".")) {
      throw new Error("$count field name cannot contain '.'");
    }

    if (docs.length === 0) {
      return [];
    }

    return [{ [fieldName]: docs.length }];
  }

  private execUnwind(
    unwind: string | UnwindOptions,
    docs: Document[]
  ): Document[] {
    const path = typeof unwind === "string" ? unwind : unwind.path;
    const preserveNullAndEmpty =
      typeof unwind === "object" && unwind.preserveNullAndEmptyArrays === true;
    const includeArrayIndex =
      typeof unwind === "object" ? unwind.includeArrayIndex : undefined;

    if (!path.startsWith("$")) {
      throw new Error(
        "$unwind requires a path starting with '$', found: " + path
      );
    }

    const fieldPath = path.slice(1);
    const result: Document[] = [];

    for (const doc of docs) {
      const value = getValueByPath(doc, fieldPath);

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

      if (!Array.isArray(value)) {
        const newDoc = cloneDocument(doc);
        if (includeArrayIndex) {
          newDoc[includeArrayIndex] = 0;
        }
        result.push(newDoc);
        continue;
      }

      if (value.length === 0) {
        if (preserveNullAndEmpty) {
          const newDoc = cloneDocument(doc);
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

      for (let i = 0; i < value.length; i++) {
        const newDoc = cloneDocument(doc);
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

  private execGroup(
    groupSpec: { _id: unknown; [key: string]: unknown },
    docs: Document[]
  ): Document[] {
    if (!("_id" in groupSpec)) {
      throw new Error("a group specification must include an _id");
    }

    const groups = new Map<
      string,
      { _id: unknown; accumulators: Map<string, ReturnType<typeof createAccumulator>> }
    >();

    const accumulatorFields = Object.entries(groupSpec).filter(
      ([key]) => key !== "_id"
    );

    for (const doc of docs) {
      const groupId = evaluateExpression(groupSpec._id, doc, this.getSystemVars(doc));
      const groupKey = JSON.stringify(groupId);

      if (!groups.has(groupKey)) {
        const accumulators = new Map<string, ReturnType<typeof createAccumulator>>();
        for (const [field, expr] of accumulatorFields) {
          const exprObj = expr as Record<string, unknown>;
          const opKeys = Object.keys(exprObj);
          if (opKeys.length === 1 && opKeys[0].startsWith("$")) {
            accumulators.set(field, createAccumulator(opKeys[0], exprObj[opKeys[0]]));
          }
        }
        groups.set(groupKey, { _id: groupId, accumulators });
      }

      const group = groups.get(groupKey)!;

      for (const [, accumulator] of group.accumulators) {
        accumulator.accumulate(doc);
      }
    }

    return Array.from(groups.values()).map((group) => {
      const result: Document = { _id: group._id };
      for (const [field, accumulator] of group.accumulators) {
        result[field] = accumulator.getResult();
      }
      return result;
    });
  }

  private async execLookup(
    lookupSpec: { from: string; localField: string; foreignField: string; as: string },
    docs: Document[]
  ): Promise<Document[]> {
    if (!lookupSpec.from) {
      throw new Error("$lookup requires 'from' field");
    }
    if (!lookupSpec.localField) {
      throw new Error("$lookup requires 'localField' field");
    }
    if (!lookupSpec.foreignField) {
      throw new Error("$lookup requires 'foreignField' field");
    }
    if (!lookupSpec.as) {
      throw new Error("$lookup requires 'as' field");
    }

    if (!this.dbContext) {
      throw new Error("$lookup requires database context");
    }

    const foreignCollection = this.dbContext.getCollection(lookupSpec.from);
    const foreignDocs = await foreignCollection.find({}).toArray();

    return docs.map((doc) => {
      const localValue = getValueByPath(doc, lookupSpec.localField);

      const matches = foreignDocs.filter((foreignDoc) => {
        const foreignValue = getValueByPath(foreignDoc, lookupSpec.foreignField);
        return valuesEqual(localValue, foreignValue);
      });

      return {
        ...doc,
        [lookupSpec.as]: matches,
      };
    });
  }

  /**
   * $graphLookup - recursive lookup for graph traversal.
   */
  private async execGraphLookup(
    spec: {
      from: string;
      startWith: unknown;
      connectFromField: string;
      connectToField: string;
      as: string;
      maxDepth?: number;
      depthField?: string;
      restrictSearchWithMatch?: Document;
    },
    docs: Document[]
  ): Promise<Document[]> {
    // Validate required fields
    if (!spec.from) {
      throw new Error("$graphLookup requires 'from'");
    }
    if (spec.startWith === undefined) {
      throw new Error("$graphLookup requires 'startWith'");
    }
    if (!spec.connectFromField) {
      throw new Error("$graphLookup requires 'connectFromField'");
    }
    if (!spec.connectToField) {
      throw new Error("$graphLookup requires 'connectToField'");
    }
    if (!spec.as) {
      throw new Error("$graphLookup requires 'as'");
    }

    // Validate optional fields
    if (spec.maxDepth !== undefined && spec.maxDepth < 0) {
      throw new Error("maxDepth must be non-negative");
    }
    if (
      spec.restrictSearchWithMatch !== undefined &&
      (typeof spec.restrictSearchWithMatch !== "object" ||
        spec.restrictSearchWithMatch === null ||
        Array.isArray(spec.restrictSearchWithMatch))
    ) {
      throw new Error("restrictSearchWithMatch must be object");
    }

    if (!this.dbContext) {
      throw new Error("$graphLookup requires database context");
    }

    // Get all documents from the target collection
    const foreignCollection = this.dbContext.getCollection(spec.from);
    let foreignDocs = await foreignCollection.find({}).toArray();

    // Apply restrictSearchWithMatch filter if specified
    if (spec.restrictSearchWithMatch) {
      foreignDocs = foreignDocs.filter((doc) =>
        matchesFilter(doc, spec.restrictSearchWithMatch as Filter<Document>)
      );
    }

    const results: Document[] = [];

    for (const doc of docs) {
      // Evaluate startWith expression
      const startValue = evaluateExpression(
        spec.startWith,
        doc,
        this.getSystemVars(doc)
      );

      // Handle null/missing startWith - return empty array
      if (startValue === null || startValue === undefined) {
        results.push({ ...doc, [spec.as]: [] });
        continue;
      }

      // Initialize search values (handle array startWith)
      const initialValues = Array.isArray(startValue) ? startValue : [startValue];

      // BFS traversal
      const found: Array<Document & { __depth?: number }> = [];
      let currentValues = initialValues;
      let depth = 0;

      while (currentValues.length > 0) {
        // Check max depth
        if (spec.maxDepth !== undefined && depth > spec.maxDepth) {
          break;
        }

        const nextValues: unknown[] = [];

        for (const searchValue of currentValues) {
          // Find matching documents
          const matches = foreignDocs.filter((foreignDoc) => {
            const connectToValue = getValueByPath(
              foreignDoc,
              spec.connectToField
            );
            if (Array.isArray(connectToValue)) {
              return connectToValue.some((v) => valuesEqual(v, searchValue));
            }
            return valuesEqual(connectToValue, searchValue);
          });

          for (const match of matches) {
            // Add to found results
            const resultDoc: Document = { ...match };
            if (spec.depthField) {
              resultDoc[spec.depthField] = depth;
            }
            found.push(resultDoc);

            // Extract values for next iteration
            const fromValue = getValueByPath(match, spec.connectFromField);
            if (fromValue !== null && fromValue !== undefined) {
              if (Array.isArray(fromValue)) {
                nextValues.push(...fromValue);
              } else {
                nextValues.push(fromValue);
              }
            }
          }
        }

        currentValues = nextValues;
        depth++;
      }

      results.push({ ...doc, [spec.as]: found });
    }

    return results;
  }

  /**
   * $densify - fills gaps in numeric or date sequences.
   */
  private execDensify(
    spec: {
      field: string;
      range: {
        step: number;
        unit?: string;
        bounds?: [unknown, unknown] | "full" | "partition";
      };
      partitionByFields?: string[];
    },
    docs: Document[]
  ): Document[] {
    // Validate field name
    if (!spec.field || spec.field.startsWith("$")) {
      throw new Error("Cannot densify field starting with '$'");
    }

    // Validate step
    if (spec.range.step <= 0) {
      throw new Error("Step must be positive");
    }

    // Validate unit if provided
    if (spec.range.unit && !isValidTimeUnit(spec.range.unit)) {
      throw new Error(`Invalid time unit: ${spec.range.unit}`);
    }

    // Partition documents
    const partitions = partitionDocuments(
      docs,
      { partitionByFields: spec.partitionByFields },
      evaluateExpression
    );

    const results: Document[] = [];

    for (const [, partitionDocs] of partitions) {
      // Get all field values in this partition
      const values: { value: number | Date; doc: Document }[] = [];
      for (const doc of partitionDocs) {
        const val = getValueByPath(doc, spec.field);
        if (val !== null && val !== undefined) {
          if (typeof val === "number" || val instanceof Date) {
            values.push({ value: val, doc });
          }
        }
      }

      // Sort by field value
      values.sort((a, b) => {
        const aVal = a.value instanceof Date ? a.value.getTime() : a.value;
        const bVal = b.value instanceof Date ? b.value.getTime() : b.value;
        return aVal - bVal;
      });

      if (values.length === 0) {
        // No values to densify, return original docs
        results.push(...partitionDocs);
        continue;
      }

      // Determine bounds
      let lowerBound: number | Date = values[0].value;
      let upperBound: number | Date = values[values.length - 1].value;

      if (spec.range.bounds && spec.range.bounds !== "partition") {
        if (spec.range.bounds === "full") {
          // Use global min/max across all partitions
          // For simplicity, use partition bounds (same for this implementation)
        } else if (Array.isArray(spec.range.bounds)) {
          lowerBound = spec.range.bounds[0] as number | Date;
          upperBound = spec.range.bounds[1] as number | Date;
        }
      }

      // Check type consistency
      const isDate = values[0].value instanceof Date;
      if (isDate && !spec.range.unit) {
        throw new Error("Unit required for date field");
      }
      if (!isDate && spec.range.unit) {
        throw new Error("Cannot specify unit for numeric field");
      }

      // Generate sequence and merge with originals
      const outputDocs: Document[] = [];
      const existingValues = new Set(
        values.map((v) =>
          v.value instanceof Date ? v.value.getTime() : v.value
        )
      );

      // Create a map of existing docs by their field value
      const existingDocsByValue = new Map<number, Document>();
      for (const { value, doc } of values) {
        const key = value instanceof Date ? value.getTime() : value;
        existingDocsByValue.set(key, doc);
      }

      // Generate all values from lower to upper bound
      let current = lowerBound;
      while (true) {
        const currentKey =
          current instanceof Date ? current.getTime() : current;
        const upperKey =
          upperBound instanceof Date ? upperBound.getTime() : upperBound;

        if (currentKey > upperKey) break;

        if (existingDocsByValue.has(currentKey)) {
          // Use existing document
          outputDocs.push(existingDocsByValue.get(currentKey)!);
        } else {
          // Generate new document with just the field
          const newDoc: Document = {};
          setValueByPath(newDoc, spec.field, current);
          // Copy partition fields from first doc in partition
          if (spec.partitionByFields && values.length > 0) {
            for (const pField of spec.partitionByFields) {
              const pVal = getValueByPath(values[0].doc, pField);
              if (pVal !== undefined) {
                setValueByPath(newDoc, pField, pVal);
              }
            }
          }
          outputDocs.push(newDoc);
        }

        // Step to next value
        if (isDate) {
          current = addDateStep(
            current as Date,
            spec.range.step,
            spec.range.unit as TimeUnit
          );
        } else {
          current = (current as number) + spec.range.step;
        }
      }

      results.push(...outputDocs);
    }

    return results;
  }

  private execAddFields(
    addFieldsSpec: Record<string, unknown>,
    docs: Document[]
  ): Document[] {
    return docs.map((doc) => {
      const result = cloneDocument(doc);

      for (const [field, expr] of Object.entries(addFieldsSpec)) {
        const value = evaluateExpression(expr, doc, this.getSystemVars(doc));
        if (field.includes(".")) {
          setValueByPath(result, field, value);
        } else {
          result[field] = value;
        }
      }

      return result;
    });
  }

  private execReplaceRoot(
    spec: { newRoot: unknown },
    docs: Document[]
  ): Document[] {
    return docs.map((doc) => {
      const newRoot = evaluateExpression(spec.newRoot, doc, this.getSystemVars(doc));

      if (newRoot === null || newRoot === undefined) {
        const typeName = newRoot === null ? "null" : "missing";
        throw new Error(
          `'newRoot' expression must evaluate to an object, but resulting value was: ${typeName === "missing" ? "MISSING" : typeName}. Type of resulting value: '${typeName}'.`
        );
      }

      if (typeof newRoot !== "object" || Array.isArray(newRoot)) {
        const typeName = getBSONTypeName(newRoot);
        throw new Error(
          `'newRoot' expression must evaluate to an object, but resulting value was of type: ${typeName}`
        );
      }

      return newRoot as Document;
    });
  }

  private async execOut(
    collectionName: string,
    docs: Document[]
  ): Promise<Document[]> {
    if (!this.dbContext) {
      throw new Error("$out requires database context");
    }

    const targetCollection = this.dbContext.getCollection(collectionName);

    await targetCollection.deleteMany({});

    if (docs.length > 0) {
      await targetCollection.insertMany(docs);
    }

    return [];
  }

  /**
   * $sortByCount - Groups by expression and counts, sorted by count descending.
   * Equivalent to: { $group: { _id: <expression>, count: { $sum: 1 } } }, { $sort: { count: -1 } }
   */
  private execSortByCount(expression: unknown, docs: Document[]): Document[] {
    // Group by the expression value
    const counts = new Map<string, { _id: unknown; count: number }>();

    for (const doc of docs) {
      const groupValue = evaluateExpression(expression, doc, this.getSystemVars(doc));
      const key = JSON.stringify(groupValue);

      if (!counts.has(key)) {
        counts.set(key, { _id: groupValue, count: 0 });
      }
      counts.get(key)!.count++;
    }

    // Convert to array and sort by count descending
    const result = Array.from(counts.values());
    result.sort((a, b) => b.count - a.count);

    return result;
  }

  /**
   * $sample - Randomly selects the specified number of documents.
   */
  private execSample(spec: { size: number }, docs: Document[]): Document[] {
    if (spec.size === undefined || spec.size === null) {
      throw new Error("$sample requires a 'size' field");
    }
    if (typeof spec.size !== "number" || !Number.isInteger(spec.size)) {
      throw new Error("size argument to $sample must be a positive integer");
    }
    if (spec.size <= 0) {
      throw new Error("size argument to $sample must be a positive integer");
    }

    if (docs.length === 0) {
      return [];
    }

    if (spec.size >= docs.length) {
      // Return all documents in random order
      const shuffled = [...docs];
      for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
      }
      return shuffled;
    }

    // Random sampling using Fisher-Yates partial shuffle
    const result: Document[] = [];
    const indices = new Set<number>();

    while (indices.size < spec.size) {
      const idx = Math.floor(Math.random() * docs.length);
      if (!indices.has(idx)) {
        indices.add(idx);
        result.push(docs[idx]);
      }
    }

    return result;
  }

  /**
   * $facet - Processes multiple aggregation pipelines within a single stage.
   * Returns a single document where each field contains the array of results from its pipeline.
   */
  private async execFacet(
    facetSpec: Record<string, PipelineStage[]>,
    docs: Document[]
  ): Promise<Document[]> {
    const result: Document = {};

    // Process each facet pipeline independently
    for (const [field, pipeline] of Object.entries(facetSpec)) {
      if (!Array.isArray(pipeline)) {
        throw new Error(`$facet field '${field}' must be an array of pipeline stages`);
      }

      // Validate that facet pipelines don't contain $out or $merge
      for (const stage of pipeline) {
        const stageKeys = Object.keys(stage);
        if (stageKeys.includes("$out") || stageKeys.includes("$merge")) {
          throw new Error("$out and $merge stages are not allowed within $facet");
        }
        if (stageKeys.includes("$facet")) {
          throw new Error("$facet is not allowed within $facet");
        }
      }

      // Create a new cursor for this sub-pipeline
      const subCursor = new AggregationCursor(
        async () => [...docs],
        pipeline,
        this.dbContext
      );

      result[field] = await subCursor.toArray();
    }

    return [result];
  }

  /**
   * $bucket - Categorizes documents into groups (buckets) based on boundaries.
   */
  private execBucket(
    spec: {
      groupBy: unknown;
      boundaries: unknown[];
      default?: unknown;
      output?: Record<string, unknown>;
    },
    docs: Document[]
  ): Document[] {
    if (!spec.groupBy) {
      throw new Error("$bucket requires a 'groupBy' field");
    }
    if (!spec.boundaries || !Array.isArray(spec.boundaries)) {
      throw new Error("$bucket requires a 'boundaries' array");
    }
    if (spec.boundaries.length < 2) {
      throw new Error("$bucket 'boundaries' must have at least 2 elements");
    }

    // Verify boundaries are in ascending order
    for (let i = 1; i < spec.boundaries.length; i++) {
      const prev = spec.boundaries[i - 1];
      const curr = spec.boundaries[i];
      if (typeof prev === "number" && typeof curr === "number") {
        if (prev >= curr) {
          throw new Error("$bucket 'boundaries' must be in ascending order");
        }
      }
    }

    const hasDefault = spec.default !== undefined;
    const buckets = new Map<
      string,
      { _id: unknown; docs: Document[]; accumulators: Map<string, ReturnType<typeof createAccumulator>> }
    >();

    // Initialize buckets for each boundary range
    for (let i = 0; i < spec.boundaries.length - 1; i++) {
      const key = JSON.stringify(spec.boundaries[i]);
      const accumulators = new Map<string, ReturnType<typeof createAccumulator>>();

      if (spec.output) {
        for (const [field, expr] of Object.entries(spec.output)) {
          const exprObj = expr as Record<string, unknown>;
          const opKeys = Object.keys(exprObj);
          if (opKeys.length === 1 && opKeys[0].startsWith("$")) {
            accumulators.set(field, createAccumulator(opKeys[0], exprObj[opKeys[0]]));
          }
        }
      } else {
        // Default output: just count
        accumulators.set("count", createAccumulator("$sum", 1));
      }

      buckets.set(key, {
        _id: spec.boundaries[i],
        docs: [],
        accumulators,
      });
    }

    // Initialize default bucket if specified
    if (hasDefault) {
      const defaultKey = JSON.stringify(spec.default);
      const accumulators = new Map<string, ReturnType<typeof createAccumulator>>();

      if (spec.output) {
        for (const [field, expr] of Object.entries(spec.output)) {
          const exprObj = expr as Record<string, unknown>;
          const opKeys = Object.keys(exprObj);
          if (opKeys.length === 1 && opKeys[0].startsWith("$")) {
            accumulators.set(field, createAccumulator(opKeys[0], exprObj[opKeys[0]]));
          }
        }
      } else {
        accumulators.set("count", createAccumulator("$sum", 1));
      }

      buckets.set(defaultKey, {
        _id: spec.default,
        docs: [],
        accumulators,
      });
    }

    // Categorize each document
    for (const doc of docs) {
      const value = evaluateExpression(spec.groupBy, doc, this.getSystemVars(doc));
      let bucketKey: string | null = null;

      // Find which bucket this value belongs to
      for (let i = 0; i < spec.boundaries.length - 1; i++) {
        const lower = spec.boundaries[i];
        const upper = spec.boundaries[i + 1];

        // Value is in bucket if lower <= value < upper
        if (
          typeof value === "number" &&
          typeof lower === "number" &&
          typeof upper === "number"
        ) {
          if (value >= lower && value < upper) {
            bucketKey = JSON.stringify(lower);
            break;
          }
        }
      }

      if (bucketKey === null) {
        if (hasDefault) {
          bucketKey = JSON.stringify(spec.default);
        } else {
          throw new Error(
            `$bucket could not find a matching bucket for value: ${JSON.stringify(value)}`
          );
        }
      }

      const bucket = buckets.get(bucketKey)!;
      bucket.docs.push(doc);

      for (const [, accumulator] of bucket.accumulators) {
        accumulator.accumulate(doc);
      }
    }

    // Build results, only include non-empty buckets
    const result: Document[] = [];
    for (const bucket of buckets.values()) {
      if (bucket.docs.length > 0) {
        const doc: Document = { _id: bucket._id };
        for (const [field, accumulator] of bucket.accumulators) {
          doc[field] = accumulator.getResult();
        }
        result.push(doc);
      }
    }

    // Sort by _id for consistent output
    result.sort((a, b) => {
      const aId = a._id as number;
      const bId = b._id as number;
      if (typeof aId === "number" && typeof bId === "number") {
        return aId - bId;
      }
      return 0;
    });

    return result;
  }

  /**
   * $bucketAuto - Automatically creates bucket boundaries.
   */
  private execBucketAuto(
    spec: {
      groupBy: unknown;
      buckets: number;
      output?: Record<string, unknown>;
      granularity?: string;
    },
    docs: Document[]
  ): Document[] {
    if (!spec.groupBy) {
      throw new Error("$bucketAuto requires a 'groupBy' field");
    }
    if (spec.buckets === undefined || typeof spec.buckets !== "number") {
      throw new Error("$bucketAuto requires a 'buckets' field as a positive integer");
    }
    if (!Number.isInteger(spec.buckets) || spec.buckets < 1) {
      throw new Error("$bucketAuto 'buckets' must be a positive integer");
    }

    if (docs.length === 0) {
      return [];
    }

    // Extract and sort values
    const valuesWithDocs: { value: unknown; doc: Document }[] = [];
    for (const doc of docs) {
      const value = evaluateExpression(spec.groupBy, doc, this.getSystemVars(doc));
      if (value !== null && value !== undefined) {
        valuesWithDocs.push({ value, doc });
      }
    }

    if (valuesWithDocs.length === 0) {
      return [];
    }

    // Sort by value
    valuesWithDocs.sort((a, b) => {
      const aVal = a.value as number;
      const bVal = b.value as number;
      if (typeof aVal === "number" && typeof bVal === "number") {
        return aVal - bVal;
      }
      return 0;
    });

    // Calculate items per bucket
    const numBuckets = Math.min(spec.buckets, valuesWithDocs.length);
    const itemsPerBucket = Math.ceil(valuesWithDocs.length / numBuckets);

    // Create buckets
    const bucketResults: Array<{
      min: unknown;
      max: unknown;
      docs: Document[];
      accumulators: Map<string, ReturnType<typeof createAccumulator>>;
    }> = [];

    for (let i = 0; i < numBuckets; i++) {
      const start = i * itemsPerBucket;
      const end = Math.min(start + itemsPerBucket, valuesWithDocs.length);
      const bucketDocs = valuesWithDocs.slice(start, end);

      if (bucketDocs.length === 0) continue;

      const accumulators = new Map<string, ReturnType<typeof createAccumulator>>();
      if (spec.output) {
        for (const [field, expr] of Object.entries(spec.output)) {
          const exprObj = expr as Record<string, unknown>;
          const opKeys = Object.keys(exprObj);
          if (opKeys.length === 1 && opKeys[0].startsWith("$")) {
            accumulators.set(field, createAccumulator(opKeys[0], exprObj[opKeys[0]]));
          }
        }
      } else {
        accumulators.set("count", createAccumulator("$sum", 1));
      }

      // Accumulate for each doc in bucket
      for (const { doc } of bucketDocs) {
        for (const [, accumulator] of accumulators) {
          accumulator.accumulate(doc);
        }
      }

      bucketResults.push({
        min: bucketDocs[0].value,
        max: bucketDocs[bucketDocs.length - 1].value,
        docs: bucketDocs.map(({ doc }) => doc),
        accumulators,
      });
    }

    // Build output documents
    const result: Document[] = [];
    for (let i = 0; i < bucketResults.length; i++) {
      const bucket = bucketResults[i];
      const nextBucket = bucketResults[i + 1];

      const doc: Document = {
        _id: {
          min: bucket.min,
          max: nextBucket ? nextBucket.min : bucket.max,
        },
      };

      for (const [field, accumulator] of bucket.accumulators) {
        doc[field] = accumulator.getResult();
      }

      result.push(doc);
    }

    return result;
  }

  /**
   * $unionWith - Combines documents from two collections.
   */
  private async execUnionWith(
    spec: string | { coll: string; pipeline?: PipelineStage[] },
    docs: Document[]
  ): Promise<Document[]> {
    if (!this.dbContext) {
      throw new Error("$unionWith requires database context");
    }

    const collName = typeof spec === "string" ? spec : spec.coll;
    const pipeline = typeof spec === "object" ? spec.pipeline : undefined;

    if (!collName) {
      throw new Error("$unionWith requires a collection name");
    }

    const foreignCollection = this.dbContext.getCollection(collName);
    let foreignDocs = await foreignCollection.find({}).toArray();

    // Apply pipeline to foreign docs if specified
    if (pipeline && pipeline.length > 0) {
      const subCursor = new AggregationCursor(
        async () => foreignDocs,
        pipeline,
        this.dbContext
      );
      foreignDocs = await subCursor.toArray();
    }

    // Combine documents
    return [...docs, ...foreignDocs];
  }
}
