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
}
