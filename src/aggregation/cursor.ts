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

/**
 * AggregationCursor represents a cursor over aggregation pipeline results.
 */
export class AggregationCursor<T extends Document = Document> {
  private readonly source: () => Promise<T[]>;
  private readonly pipeline: PipelineStage[];
  private readonly dbContext?: AggregationDbContext;

  constructor(
    source: () => Promise<T[]>,
    pipeline: PipelineStage[],
    dbContext?: AggregationDbContext
  ) {
    this.source = source;
    this.pipeline = pipeline;
    this.dbContext = dbContext;
  }

  async toArray(): Promise<Document[]> {
    // Validate $out is last stage if present
    for (let i = 0; i < this.pipeline.length; i++) {
      const stage = this.pipeline[i] as unknown as Record<string, unknown>;
      if ("$out" in stage && i !== this.pipeline.length - 1) {
        throw new Error("$out can only be the final stage in the pipeline");
      }
    }

    let documents: Document[] = await this.source();

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
      } else if (typeof value === "string" && value.startsWith("$")) {
        const refPath = value.slice(1);
        const refValue = getValueByPath(doc, refPath);
        if (refValue !== undefined) {
          result[key] = refValue;
        }
      } else if (this.isLiteralExpression(value)) {
        result[key] = value.$literal;
      } else if (typeof value === "object" && value !== null) {
        const evaluated = evaluateExpression(value, doc);
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
      const groupId = evaluateExpression(groupSpec._id, doc);
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

  private execAddFields(
    addFieldsSpec: Record<string, unknown>,
    docs: Document[]
  ): Document[] {
    return docs.map((doc) => {
      const result = cloneDocument(doc);

      for (const [field, expr] of Object.entries(addFieldsSpec)) {
        const value = evaluateExpression(expr, doc);
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
      const newRoot = evaluateExpression(spec.newRoot, doc);

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
      const groupValue = evaluateExpression(expression, doc);
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
      const value = evaluateExpression(spec.groupBy, doc);
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
      const value = evaluateExpression(spec.groupBy, doc);
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
