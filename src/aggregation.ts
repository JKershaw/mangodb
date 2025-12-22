/**
 * Aggregation Pipeline for MangoDB.
 *
 * This module provides MongoDB-compatible aggregation pipeline functionality.
 * Supports stages: $match, $project, $sort, $limit, $skip, $count, $unwind,
 * $group, $lookup, $addFields, $set, $replaceRoot, $out.
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

// ==================== Helper Functions ====================

/**
 * Get BSON type name for a value (used in error messages).
 */
function getBSONTypeName(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "missing";
  if (Array.isArray(value)) return "array";
  if (value instanceof Date) return "date";
  if (typeof value === "number") {
    return Number.isInteger(value) ? "int" : "double";
  }
  if (typeof value === "boolean") return "bool";
  if (typeof value === "string") return "string";
  if (typeof value === "object") {
    // Check for ObjectId
    if (value && typeof (value as { toHexString?: unknown }).toHexString === "function") {
      return "objectId";
    }
    return "object";
  }
  return typeof value;
}

/**
 * Deep equality check that handles primitives and objects.
 */
function deepEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null) return a === b;
  if (typeof a !== typeof b) return false;

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((val, i) => deepEquals(val, b[i]));
  }

  if (typeof a === "object" && typeof b === "object") {
    const aKeys = Object.keys(a as object);
    const bKeys = Object.keys(b as object);
    if (aKeys.length !== bKeys.length) return false;
    return aKeys.every(key =>
      deepEquals((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key])
    );
  }

  return false;
}

// ==================== Expression Evaluation ====================

/**
 * Evaluate an aggregation expression against a document.
 *
 * Expressions can be:
 * - Field references: "$fieldName" or "$nested.field"
 * - Literal values: numbers, strings, booleans, null
 * - Operator expressions: { $add: [...] }, { $concat: [...] }, etc.
 *
 * @param expr - The expression to evaluate
 * @param doc - The document context
 * @returns The evaluated value
 */
function evaluateExpression(expr: unknown, doc: Document): unknown {
  // String starting with $ is a field reference
  if (typeof expr === "string" && expr.startsWith("$")) {
    const fieldPath = expr.slice(1);
    return getValueByPath(doc, fieldPath);
  }

  // Primitive values returned as-is
  if (expr === null || typeof expr !== "object") {
    return expr;
  }

  // Arrays - evaluate each element
  if (Array.isArray(expr)) {
    return expr.map((item) => evaluateExpression(item, doc));
  }

  // Object with operator key
  const exprObj = expr as Record<string, unknown>;
  const keys = Object.keys(exprObj);

  if (keys.length === 1 && keys[0].startsWith("$")) {
    return evaluateOperator(keys[0], exprObj[keys[0]], doc);
  }

  // Object literal - evaluate each field
  const result: Document = {};
  for (const [key, value] of Object.entries(exprObj)) {
    result[key] = evaluateExpression(value, doc);
  }
  return result;
}

/**
 * Evaluate a specific operator expression.
 */
function evaluateOperator(op: string, args: unknown, doc: Document): unknown {
  switch (op) {
    case "$literal":
      return args; // Return as-is without evaluation

    // Arithmetic operators
    case "$add":
      return evalAdd(args as unknown[], doc);
    case "$subtract":
      return evalSubtract(args as unknown[], doc);
    case "$multiply":
      return evalMultiply(args as unknown[], doc);
    case "$divide":
      return evalDivide(args as unknown[], doc);

    // String operators
    case "$concat":
      return evalConcat(args as unknown[], doc);
    case "$toUpper":
      return evalToUpper(args, doc);
    case "$toLower":
      return evalToLower(args, doc);

    // Conditional operators
    case "$cond":
      return evalCond(args, doc);
    case "$ifNull":
      return evalIfNull(args as unknown[], doc);

    // Comparison operators (for $cond conditions)
    case "$gt":
      return evalComparison(args as unknown[], doc, (a, b) => a > b);
    case "$gte":
      return evalComparison(args as unknown[], doc, (a, b) => a >= b);
    case "$lt":
      return evalComparison(args as unknown[], doc, (a, b) => a < b);
    case "$lte":
      return evalComparison(args as unknown[], doc, (a, b) => a <= b);
    case "$eq":
      return evalComparison(args as unknown[], doc, (a, b) => a === b);
    case "$ne":
      return evalComparison(args as unknown[], doc, (a, b) => a !== b);

    // Array operators
    case "$size":
      return evalSize(args, doc);

    default:
      throw new Error(`Unrecognized expression operator: '${op}'`);
  }
}

// ==================== Arithmetic Operators ====================

function evalAdd(args: unknown[], doc: Document): number | null {
  const values = args.map((a) => evaluateExpression(a, doc));

  // null/undefined propagates
  if (values.some((v) => v === null || v === undefined)) {
    return null;
  }

  let sum = 0;
  for (const v of values) {
    if (typeof v !== "number") {
      throw new Error("$add only supports numeric types");
    }
    sum += v;
  }
  return sum;
}

function evalSubtract(args: unknown[], doc: Document): number | null {
  const [arg1, arg2] = args.map((a) => evaluateExpression(a, doc));

  if (arg1 === null || arg1 === undefined || arg2 === null || arg2 === undefined) {
    return null;
  }

  if (typeof arg1 !== "number" || typeof arg2 !== "number") {
    throw new Error("$subtract only supports numeric types");
  }

  return arg1 - arg2;
}

function evalMultiply(args: unknown[], doc: Document): number | null {
  const values = args.map((a) => evaluateExpression(a, doc));

  if (values.some((v) => v === null || v === undefined)) {
    return null;
  }

  let product = 1;
  for (const v of values) {
    if (typeof v !== "number") {
      throw new Error("$multiply only supports numeric types");
    }
    product *= v;
  }
  return product;
}

function evalDivide(args: unknown[], doc: Document): number | null {
  const [dividend, divisor] = args.map((a) => evaluateExpression(a, doc));

  if (dividend === null || dividend === undefined || divisor === null || divisor === undefined) {
    return null;
  }

  if (typeof dividend !== "number" || typeof divisor !== "number") {
    throw new Error("$divide only supports numeric types");
  }

  // MongoDB returns null for divide by zero (doesn't throw)
  if (divisor === 0) {
    return null;
  }

  return dividend / divisor;
}

// ==================== String Operators ====================

function evalConcat(args: unknown[], doc: Document): string | null {
  const values = args.map((a) => evaluateExpression(a, doc));

  // null propagates
  if (values.some((v) => v === null || v === undefined)) {
    return null;
  }

  // All values must be strings
  for (const v of values) {
    if (typeof v !== "string") {
      // MongoDB uses BSON type names
      const typeName = getBSONTypeName(v);
      throw new Error(`$concat only supports strings, not ${typeName}`);
    }
  }

  return values.join("");
}

function evalToUpper(args: unknown, doc: Document): string | null {
  const value = evaluateExpression(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "string") {
    throw new Error("$toUpper requires a string argument");
  }

  return value.toUpperCase();
}

function evalToLower(args: unknown, doc: Document): string | null {
  const value = evaluateExpression(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "string") {
    throw new Error("$toLower requires a string argument");
  }

  return value.toLowerCase();
}

// ==================== Conditional Operators ====================

function evalCond(args: unknown, doc: Document): unknown {
  let condition: unknown;
  let thenValue: unknown;
  let elseValue: unknown;

  if (Array.isArray(args)) {
    // Array syntax: [$cond: [condition, thenValue, elseValue]]
    [condition, thenValue, elseValue] = args;
  } else if (typeof args === "object" && args !== null) {
    // Object syntax: { $cond: { if: condition, then: thenValue, else: elseValue } }
    const obj = args as { if: unknown; then: unknown; else: unknown };
    condition = obj.if;
    thenValue = obj.then;
    elseValue = obj.else;
  } else {
    throw new Error("$cond requires an array or object argument");
  }

  const evalCondition = evaluateExpression(condition, doc);

  // Truthy check
  if (evalCondition) {
    return evaluateExpression(thenValue, doc);
  } else {
    return evaluateExpression(elseValue, doc);
  }
}

function evalIfNull(args: unknown[], doc: Document): unknown {
  for (const arg of args) {
    const value = evaluateExpression(arg, doc);
    if (value !== null && value !== undefined) {
      return value;
    }
  }
  return null;
}

function evalComparison(
  args: unknown[],
  doc: Document,
  compareFn: (a: unknown, b: unknown) => boolean
): boolean {
  const [left, right] = args.map((a) => evaluateExpression(a, doc));
  return compareFn(left, right);
}

// ==================== Array Operators ====================

function evalSize(args: unknown, doc: Document): number {
  const value = evaluateExpression(args, doc);

  if (!Array.isArray(value)) {
    const typeName = getBSONTypeName(value);
    throw new Error(`The argument to $size must be an array, but was of type: ${typeName}`);
  }

  return value.length;
}

// ==================== Accumulator Classes ====================

interface Accumulator {
  accumulate(doc: Document): void;
  getResult(): unknown;
}

class SumAccumulator implements Accumulator {
  private sum = 0;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (typeof value === "number") {
      this.sum += value;
    }
    // Non-numbers ignored
  }

  getResult(): number {
    return this.sum;
  }
}

class AvgAccumulator implements Accumulator {
  private sum = 0;
  private count = 0;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (typeof value === "number") {
      this.sum += value;
      this.count++;
    }
  }

  getResult(): number | null {
    return this.count > 0 ? this.sum / this.count : null;
  }
}

class MinAccumulator implements Accumulator {
  private min: unknown = undefined;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (value !== null && value !== undefined) {
      if (this.min === undefined || compareValuesForSort(value, this.min, 1) < 0) {
        this.min = value;
      }
    }
  }

  getResult(): unknown {
    return this.min === undefined ? null : this.min;
  }
}

class MaxAccumulator implements Accumulator {
  private max: unknown = undefined;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (value !== null && value !== undefined) {
      if (this.max === undefined || compareValuesForSort(value, this.max, 1) > 0) {
        this.max = value;
      }
    }
  }

  getResult(): unknown {
    return this.max === undefined ? null : this.max;
  }
}

class FirstAccumulator implements Accumulator {
  private first: unknown = undefined;
  private hasValue = false;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    if (!this.hasValue) {
      this.first = evaluateExpression(this.expr, doc);
      this.hasValue = true;
    }
  }

  getResult(): unknown {
    return this.first;
  }
}

class LastAccumulator implements Accumulator {
  private last: unknown = undefined;
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    this.last = evaluateExpression(this.expr, doc);
  }

  getResult(): unknown {
    return this.last;
  }
}

class PushAccumulator implements Accumulator {
  private values: unknown[] = [];
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    this.values.push(value);
  }

  getResult(): unknown[] {
    return this.values;
  }
}

class AddToSetAccumulator implements Accumulator {
  private values: unknown[] = [];
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    // Check if already exists (using deep equality that handles primitives)
    if (!this.values.some((v) => deepEquals(v, value))) {
      this.values.push(value);
    }
  }

  getResult(): unknown[] {
    return this.values;
  }
}

function createAccumulator(op: string, expr: unknown): Accumulator {
  switch (op) {
    case "$sum":
      return new SumAccumulator(expr);
    case "$avg":
      return new AvgAccumulator(expr);
    case "$min":
      return new MinAccumulator(expr);
    case "$max":
      return new MaxAccumulator(expr);
    case "$first":
      return new FirstAccumulator(expr);
    case "$last":
      return new LastAccumulator(expr);
    case "$push":
      return new PushAccumulator(expr);
    case "$addToSet":
      return new AddToSetAccumulator(expr);
    default:
      throw new Error(`unknown group operator '${op}'`);
  }
}

// ==================== Database Context Interface ====================

/**
 * Interface for database context needed by $lookup and $out stages.
 */
export interface AggregationDbContext {
  getCollection(name: string): {
    find(filter: Document): { toArray(): Promise<Document[]> };
    deleteMany(filter: Document): Promise<unknown>;
    insertMany(docs: Document[]): Promise<unknown>;
  };
}

// ==================== AggregationCursor Class ====================

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
  private readonly dbContext?: AggregationDbContext;

  /**
   * Creates a new AggregationCursor instance.
   *
   * @param source - Function that returns a promise resolving to source documents
   * @param pipeline - Array of pipeline stages to execute
   * @param dbContext - Optional database context for $lookup and $out stages
   */
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
   * Executes the aggregation pipeline and returns all results as an array.
   *
   * Stages are executed sequentially, with each stage transforming the
   * document stream for the next stage.
   *
   * @returns Promise resolving to an array of result documents
   * @throws Error if an unknown pipeline stage is encountered
   */
  async toArray(): Promise<Document[]> {
    // Validate $out is last stage if present
    for (let i = 0; i < this.pipeline.length; i++) {
      const stage = this.pipeline[i] as Record<string, unknown>;
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

  /**
   * Execute a single pipeline stage on the document stream.
   */
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

  // ==================== Basic Stage Implementations ====================

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

    // Determine projection mode
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

    // Field refs and expressions count as inclusion mode
    if (hasFieldRef || hasExpression) {
      hasInclusion = true;
    }

    // Cannot mix inclusion and exclusion (except _id)
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

    // Inclusion mode
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
        // Expression - evaluate it
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

  // ==================== Phase 10 Stage Implementations ====================

  private execGroup(
    groupSpec: { _id: unknown; [key: string]: unknown },
    docs: Document[]
  ): Document[] {
    // Validate _id field is present (MongoDB requires it)
    if (!("_id" in groupSpec)) {
      throw new Error("a group specification must include an _id");
    }

    const groups = new Map<
      string,
      { _id: unknown; accumulators: Map<string, Accumulator> }
    >();

    // Get accumulator fields (all fields except _id)
    const accumulatorFields = Object.entries(groupSpec).filter(
      ([key]) => key !== "_id"
    );

    for (const doc of docs) {
      // Evaluate grouping _id
      const groupId = evaluateExpression(groupSpec._id, doc);
      const groupKey = JSON.stringify(groupId);

      if (!groups.has(groupKey)) {
        // Initialize accumulators for this group
        const accumulators = new Map<string, Accumulator>();
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

      // Apply each accumulator
      for (const [, accumulator] of group.accumulators) {
        accumulator.accumulate(doc);
      }
    }

    // Build result documents
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
    // Validate required fields
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

      // Find matching foreign documents
      const matches = foreignDocs.filter((foreignDoc) => {
        const foreignValue = getValueByPath(foreignDoc, lookupSpec.foreignField);
        return deepEquals(localValue, foreignValue);
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

    // Drop existing collection and replace with results
    await targetCollection.deleteMany({});

    if (docs.length > 0) {
      await targetCollection.insertMany(docs);
    }

    // $out returns empty array (results written to collection)
    return [];
  }
}
