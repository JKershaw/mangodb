import { ObjectId } from "mongodb";
import { MongoneCursor } from "./cursor.ts";
import { applyProjection, type ProjectionSpec } from "./utils.ts";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join, dirname } from "node:path";

type Document = Record<string, unknown>;

/**
 * Query operators supported by Mongone.
 */
interface QueryOperators {
  $eq?: unknown;
  $ne?: unknown;
  $gt?: unknown;
  $gte?: unknown;
  $lt?: unknown;
  $lte?: unknown;
  $in?: unknown[];
  $nin?: unknown[];
  $exists?: boolean;
  $not?: QueryOperators;
  // Phase 6: Array query operators
  $size?: number;
  $all?: unknown[];
  $elemMatch?: Record<string, unknown>;
}

/**
 * A filter value can be a direct value or an object with query operators.
 */
type FilterValue = unknown | QueryOperators;

/**
 * Filter type for queries.
 * Supports field conditions and logical operators ($and, $or, $nor).
 */
type Filter<T> = {
  [P in keyof T]?: FilterValue;
} & {
  [key: string]: FilterValue;
} & {
  $and?: Filter<T>[];
  $or?: Filter<T>[];
  $nor?: Filter<T>[];
};

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

interface UpdateResult {
  acknowledged: boolean;
  matchedCount: number;
  modifiedCount: number;
  upsertedCount: number;
  upsertedId: ObjectId | null;
}

interface UpdateOptions {
  upsert?: boolean;
}

interface FindOptions {
  projection?: ProjectionSpec;
}

/**
 * Update operators supported by Mongone.
 */
interface UpdateOperators {
  $set?: Record<string, unknown>;
  $unset?: Record<string, unknown>;
  $inc?: Record<string, number>;
  // Phase 6: Array update operators
  $push?: Record<string, unknown>;
  $pull?: Record<string, unknown>;
  $addToSet?: Record<string, unknown>;
  $pop?: Record<string, number>;
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
   * Converts ObjectId and Date to special formats that can be restored.
   */
  private serializeDocument(doc: T): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(doc)) {
      if (value instanceof ObjectId) {
        result[key] = { $oid: value.toHexString() };
      } else if (value instanceof Date) {
        result[key] = { $date: value.toISOString() };
      } else if (value && typeof value === "object" && !Array.isArray(value)) {
        result[key] = this.serializeDocument(value as T);
      } else if (Array.isArray(value)) {
        result[key] = value.map((item) =>
          item instanceof ObjectId
            ? { $oid: item.toHexString() }
            : item instanceof Date
              ? { $date: item.toISOString() }
              : item && typeof item === "object" && !Array.isArray(item)
                ? this.serializeDocument(item as T)
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
   * Restores ObjectId and Date from the special formats.
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
      } else if (
        value &&
        typeof value === "object" &&
        !Array.isArray(value) &&
        "$date" in value &&
        typeof (value as { $date: unknown }).$date === "string"
      ) {
        result[key] = new Date((value as { $date: string }).$date);
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
          } else if (
            item &&
            typeof item === "object" &&
            !Array.isArray(item) &&
            "$date" in item &&
            typeof (item as { $date: unknown }).$date === "string"
          ) {
            return new Date((item as { $date: string }).$date);
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
   * Get all possible values from a document using dot notation.
   * When traversing arrays with non-numeric path segments, returns values from all elements.
   * This handles MongoDB's array element querying behavior.
   */
  private getValuesByPath(doc: unknown, path: string): unknown[] {
    const parts = path.split(".");
    let currentValues: unknown[] = [doc];

    for (const part of parts) {
      const nextValues: unknown[] = [];

      for (const current of currentValues) {
        if (current === null || current === undefined) {
          // Keep undefined to signal missing path
          nextValues.push(undefined);
          continue;
        }

        if (Array.isArray(current)) {
          // Try to parse as array index
          const index = parseInt(part, 10);
          if (!isNaN(index) && index >= 0) {
            // Numeric index - access specific element
            nextValues.push(current[index]);
          } else {
            // Non-numeric - traverse into each array element
            for (const elem of current) {
              if (elem !== null && typeof elem === "object" && !Array.isArray(elem)) {
                nextValues.push((elem as Record<string, unknown>)[part]);
              } else if (Array.isArray(elem)) {
                // Handle nested arrays - recurse into each element
                for (const nested of elem) {
                  if (nested !== null && typeof nested === "object") {
                    nextValues.push((nested as Record<string, unknown>)[part]);
                  }
                }
              }
            }
          }
        } else if (typeof current === "object") {
          nextValues.push((current as Record<string, unknown>)[part]);
        } else {
          nextValues.push(undefined);
        }
      }

      currentValues = nextValues;
    }

    return currentValues;
  }

  /**
   * Get a single value from a document using dot notation.
   * For simple cases where array traversal isn't needed.
   */
  private getValueByPath(doc: unknown, path: string): unknown {
    const values = this.getValuesByPath(doc, path);
    // Return first non-undefined value, or undefined if all are undefined
    return values.find((v) => v !== undefined) ?? values[0];
  }

  /**
   * Check if an operator object contains query operators.
   */
  private isOperatorObject(value: unknown): value is QueryOperators {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      return false;
    }
    const keys = Object.keys(value as object);
    return keys.length > 0 && keys.every((k) => k.startsWith("$"));
  }

  /**
   * Compare two values according to MongoDB comparison rules.
   * Returns negative if a < b, positive if a > b, 0 if equal.
   */
  private compareValues(a: unknown, b: unknown): number {
    // Handle ObjectId
    if (a instanceof ObjectId && b instanceof ObjectId) {
      return a.toHexString().localeCompare(b.toHexString());
    }

    // Handle Date
    if (a instanceof Date && b instanceof Date) {
      return a.getTime() - b.getTime();
    }

    // Handle same types
    if (typeof a === typeof b) {
      if (typeof a === "number" && typeof b === "number") {
        return a - b;
      }
      if (typeof a === "string" && typeof b === "string") {
        return a.localeCompare(b);
      }
      // Handle boolean comparison: false < true
      if (typeof a === "boolean" && typeof b === "boolean") {
        if (a === b) return 0;
        return a ? 1 : -1; // false < true, so false returns -1, true returns 1
      }
    }

    // Handle mixed types - return NaN to indicate incomparable
    return NaN;
  }

  /**
   * Check if two values are equal according to MongoDB rules.
   * By default, uses order-insensitive object comparison (for queries).
   * Set strictKeyOrder=true for BSON-style comparison where key order matters.
   */
  private valuesEqual(a: unknown, b: unknown, strictKeyOrder = false): boolean {
    // Handle ObjectId
    if (a instanceof ObjectId && b instanceof ObjectId) {
      return a.equals(b);
    }

    // Handle Date
    if (a instanceof Date && b instanceof Date) {
      return a.getTime() === b.getTime();
    }

    // Handle arrays - must be exact match
    if (Array.isArray(a) && Array.isArray(b)) {
      if (a.length !== b.length) return false;
      return a.every((val, idx) => this.valuesEqual(val, b[idx], strictKeyOrder));
    }

    // Handle null
    if (a === null && b === null) {
      return true;
    }

    // Handle objects
    if (
      a !== null &&
      b !== null &&
      typeof a === "object" &&
      typeof b === "object" &&
      !Array.isArray(a) &&
      !Array.isArray(b)
    ) {
      const aKeys = Object.keys(a);
      const bKeys = Object.keys(b);
      if (aKeys.length !== bKeys.length) return false;

      if (strictKeyOrder) {
        // BSON-style: key order matters
        for (let i = 0; i < aKeys.length; i++) {
          if (aKeys[i] !== bKeys[i]) return false;
          if (
            !this.valuesEqual(
              (a as Record<string, unknown>)[aKeys[i]],
              (b as Record<string, unknown>)[bKeys[i]],
              strictKeyOrder
            )
          ) {
            return false;
          }
        }
        return true;
      } else {
        // Order-insensitive for queries
        return aKeys.every((key) =>
          this.valuesEqual(
            (a as Record<string, unknown>)[key],
            (b as Record<string, unknown>)[key],
            strictKeyOrder
          )
        );
      }
    }

    return a === b;
  }

  /**
   * Check if a value matches query operators.
   */
  private matchesOperators(
    docValue: unknown,
    operators: QueryOperators
  ): boolean {
    for (const [op, opValue] of Object.entries(operators)) {
      switch (op) {
        case "$eq":
          if (!this.matchesSingleValue(docValue, opValue)) return false;
          break;

        case "$ne":
          if (this.matchesSingleValue(docValue, opValue)) return false;
          break;

        case "$gt": {
          const cmp = this.compareValues(docValue, opValue);
          if (isNaN(cmp) || cmp <= 0) return false;
          break;
        }

        case "$gte": {
          const cmp = this.compareValues(docValue, opValue);
          if (isNaN(cmp) || cmp < 0) return false;
          break;
        }

        case "$lt": {
          const cmp = this.compareValues(docValue, opValue);
          if (isNaN(cmp) || cmp >= 0) return false;
          break;
        }

        case "$lte": {
          const cmp = this.compareValues(docValue, opValue);
          if (isNaN(cmp) || cmp > 0) return false;
          break;
        }

        case "$in": {
          const arr = opValue as unknown[];
          if (!this.matchesIn(docValue, arr)) return false;
          break;
        }

        case "$nin": {
          const arr = opValue as unknown[];
          if (this.matchesIn(docValue, arr)) return false;
          break;
        }

        case "$exists": {
          const shouldExist = opValue as boolean;
          const exists = docValue !== undefined;
          if (shouldExist !== exists) return false;
          break;
        }

        case "$not": {
          // $not requires an operator expression (or regex), not a plain value
          if (
            opValue === null ||
            typeof opValue !== "object" ||
            Array.isArray(opValue)
          ) {
            throw new Error("$not argument must be a regex or an object");
          }
          const notOps = opValue as QueryOperators;
          const notKeys = Object.keys(notOps);
          if (notKeys.length === 0 || !notKeys.every((k) => k.startsWith("$"))) {
            throw new Error("$not argument must be a regex or an object");
          }
          // $not DOES match documents where the field is missing
          // (the inner condition can't be true if field doesn't exist)
          if (docValue === undefined) break;
          // Invert the result of the nested operators
          if (this.matchesOperators(docValue, notOps)) {
            return false;
          }
          break;
        }

        case "$size": {
          // $size matches arrays with exactly the specified number of elements
          const size = opValue as number;
          if (!Number.isInteger(size)) {
            throw new Error("$size must be a whole number");
          }
          if (size < 0) {
            throw new Error("$size may not be negative");
          }
          if (!Array.isArray(docValue)) return false;
          if (docValue.length !== size) return false;
          break;
        }

        case "$all": {
          // $all matches arrays containing all specified elements
          if (!Array.isArray(opValue)) {
            throw new Error("$all needs an array");
          }
          if (!Array.isArray(docValue)) return false;
          const allValues = opValue as unknown[];
          // Empty $all matches any array
          if (allValues.length === 0) break;
          // Check if every value in $all is in the document array
          for (const val of allValues) {
            // Handle $elemMatch inside $all
            if (
              val &&
              typeof val === "object" &&
              !Array.isArray(val) &&
              "$elemMatch" in val
            ) {
              const elemMatchCond = (val as { $elemMatch: Record<string, unknown> })
                .$elemMatch;
              const hasMatch = docValue.some((elem) =>
                this.matchesElemMatchCondition(elem, elemMatchCond)
              );
              if (!hasMatch) return false;
            } else {
              // Regular value - check if any element equals it
              const found = docValue.some((elem) => this.valuesEqual(elem, val));
              if (!found) return false;
            }
          }
          break;
        }

        case "$elemMatch": {
          // $elemMatch matches arrays where at least one element satisfies all conditions
          if (opValue === null || typeof opValue !== "object" || Array.isArray(opValue)) {
            throw new Error("$elemMatch needs an Object");
          }
          if (!Array.isArray(docValue)) return false;
          if (docValue.length === 0) return false;
          const elemMatchCond = opValue as Record<string, unknown>;
          // Empty condition matches any non-empty array
          if (Object.keys(elemMatchCond).length === 0) break;
          // Check if any element matches all conditions
          const hasMatchingElement = docValue.some((elem) =>
            this.matchesElemMatchCondition(elem, elemMatchCond)
          );
          if (!hasMatchingElement) return false;
          break;
        }

        case "$and":
          throw new Error("unknown operator: $and");

        case "$or":
          throw new Error("unknown operator: $or");

        case "$nor":
          throw new Error("unknown operator: $nor");

        default:
          // Unknown operator - ignore for forward compatibility
          break;
      }
    }
    return true;
  }

  /**
   * Check if a document value matches any value in the $in array.
   */
  private matchesIn(docValue: unknown, inValues: unknown[]): boolean {
    // If docValue is an array, check if any element matches any in value
    if (Array.isArray(docValue)) {
      return docValue.some((dv) =>
        inValues.some((iv) => this.valuesEqual(dv, iv))
      );
    }
    // Otherwise check if docValue matches any in value
    return inValues.some((iv) => this.valuesEqual(docValue, iv));
  }

  /**
   * Check if an array element matches an $elemMatch condition.
   * The condition can contain field queries (for objects) or operators (for primitives).
   */
  private matchesElemMatchCondition(
    element: unknown,
    condition: Record<string, unknown>
  ): boolean {
    for (const [key, value] of Object.entries(condition)) {
      if (key.startsWith("$")) {
        // Operator applied directly to the element (e.g., { $gte: 10, $lt: 20 })
        const operators: QueryOperators = { [key]: value };
        if (!this.matchesOperators(element, operators)) {
          return false;
        }
      } else {
        // Field condition (e.g., { score: 80, passed: true })
        // Element must be an object with this field
        if (element === null || typeof element !== "object" || Array.isArray(element)) {
          return false;
        }
        const elemObj = element as Record<string, unknown>;
        const fieldValue = this.getValueByPath(elemObj, key);

        if (this.isOperatorObject(value)) {
          if (!this.matchesOperators(fieldValue, value as QueryOperators)) {
            return false;
          }
        } else {
          if (!this.matchesSingleValue(fieldValue, value)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Check if a document value matches a single filter value.
   * Handles array field matching (any element match).
   */
  private matchesSingleValue(docValue: unknown, filterValue: unknown): boolean {
    // Handle null matching: matches null values and missing fields
    if (filterValue === null) {
      return docValue === null || docValue === undefined;
    }

    // Direct equality check
    if (this.valuesEqual(docValue, filterValue)) {
      return true;
    }

    // Array field matching: if doc field is array and filter is single value,
    // match if any array element equals the filter value
    if (Array.isArray(docValue) && !Array.isArray(filterValue)) {
      return docValue.some((elem) => this.valuesEqual(elem, filterValue));
    }

    return false;
  }

  /**
   * Check if ANY of the values matches a filter condition.
   * Special case: $exists: false requires ALL values to be undefined.
   */
  private anyValueMatches(
    values: unknown[],
    filterValue: unknown,
    isOperator: boolean
  ): boolean {
    if (isOperator) {
      const ops = filterValue as QueryOperators;

      // Special case: $exists: false requires ALL paths to be undefined
      // This differs from other operators where ANY match is sufficient
      if (ops.$exists === false && Object.keys(ops).length === 1) {
        return values.every((v) => v === undefined);
      }

      // For other operators, check if ANY value matches
      return values.some((v) =>
        this.matchesOperators(v, filterValue as QueryOperators)
      );
    } else {
      // For direct values, check if ANY value matches
      return values.some((v) => this.matchesSingleValue(v, filterValue));
    }
  }

  /**
   * Check if a document matches a filter.
   * Supports:
   * - Empty filter (matches all)
   * - Simple equality
   * - Dot notation for nested fields (including array element traversal)
   * - Query operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $not)
   * - Logical operators ($and, $or, $nor)
   * - Array field matching
   */
  private matchesFilter(doc: T, filter: Filter<T>): boolean {
    for (const [key, filterValue] of Object.entries(filter)) {
      // Handle top-level logical operators
      if (key === "$and") {
        if (!Array.isArray(filterValue)) {
          throw new Error("$and argument must be an array");
        }
        if (filterValue.length === 0) {
          throw new Error("$and argument must be a non-empty array");
        }
        const conditions = filterValue as Filter<T>[];
        // All conditions must match
        if (!conditions.every((cond) => this.matchesFilter(doc, cond))) {
          return false;
        }
        continue;
      }

      if (key === "$or") {
        if (!Array.isArray(filterValue)) {
          throw new Error("$or argument must be an array");
        }
        if (filterValue.length === 0) {
          throw new Error("$or argument must be a non-empty array");
        }
        const conditions = filterValue as Filter<T>[];
        // At least one condition must match
        if (!conditions.some((cond) => this.matchesFilter(doc, cond))) {
          return false;
        }
        continue;
      }

      if (key === "$nor") {
        if (!Array.isArray(filterValue)) {
          throw new Error("$nor argument must be an array");
        }
        if (filterValue.length === 0) {
          throw new Error("$nor argument must be a non-empty array");
        }
        const conditions = filterValue as Filter<T>[];
        // No condition may match
        if (conditions.some((cond) => this.matchesFilter(doc, cond))) {
          return false;
        }
        continue;
      }

      // Regular field condition
      const docValues = this.getValuesByPath(doc, key);
      const isOperator = this.isOperatorObject(filterValue);

      if (!this.anyValueMatches(docValues, filterValue, isOperator)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Set a value at a given path using dot notation.
   * Creates intermediate objects/arrays as needed.
   */
  private setValueByPath(
    doc: Record<string, unknown>,
    path: string,
    value: unknown
  ): void {
    const parts = path.split(".");
    let current: Record<string, unknown> = doc;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      const nextPart = parts[i + 1];
      const isNextNumeric = /^\d+$/.test(nextPart);

      if (current[part] === undefined || current[part] === null) {
        // Create nested structure: array if next part is numeric, object otherwise
        current[part] = isNextNumeric ? [] : {};
      }

      current = current[part] as Record<string, unknown>;
    }

    const lastPart = parts[parts.length - 1];
    current[lastPart] = value;
  }

  /**
   * Delete a value at a given path using dot notation.
   */
  private deleteValueByPath(doc: Record<string, unknown>, path: string): void {
    const parts = path.split(".");
    let current: Record<string, unknown> = doc;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      if (
        current[part] === undefined ||
        current[part] === null ||
        typeof current[part] !== "object"
      ) {
        return; // Path doesn't exist, nothing to delete
      }
      current = current[part] as Record<string, unknown>;
    }

    const lastPart = parts[parts.length - 1];
    delete current[lastPart];
  }

  /**
   * Get a value at a given path for updates (direct access, no array traversal).
   */
  private getValueAtPath(
    doc: Record<string, unknown>,
    path: string
  ): unknown {
    const parts = path.split(".");
    let current: unknown = doc;

    for (const part of parts) {
      if (current === undefined || current === null) {
        return undefined;
      }
      if (Array.isArray(current)) {
        const index = parseInt(part, 10);
        if (!isNaN(index)) {
          current = current[index];
        } else {
          return undefined;
        }
      } else if (typeof current === "object") {
        current = (current as Record<string, unknown>)[part];
      } else {
        return undefined;
      }
    }

    return current;
  }

  /**
   * Deep clone a value (handles primitives, arrays, objects, ObjectId, Date).
   */
  private cloneValue(value: unknown): unknown {
    if (value instanceof ObjectId) {
      return new ObjectId(value.toHexString());
    } else if (value instanceof Date) {
      return new Date(value.getTime());
    } else if (Array.isArray(value)) {
      return value.map((item) => this.cloneValue(item));
    } else if (value && typeof value === "object") {
      return this.cloneDocument(value as T);
    }
    return value;
  }

  /**
   * Deep clone a document.
   */
  private cloneDocument(doc: T): T {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(doc)) {
      result[key] = this.cloneValue(value);
    }
    return result as T;
  }

  /**
   * Check if two documents are deeply equal.
   */
  private documentsEqual(a: T, b: T): boolean {
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b);

    if (aKeys.length !== bKeys.length) return false;

    for (const key of aKeys) {
      const aVal = (a as Record<string, unknown>)[key];
      const bVal = (b as Record<string, unknown>)[key];

      if (!this.valuesEqual(aVal, bVal)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Apply update operators to a document.
   * Returns a new document with the updates applied.
   */
  private applyUpdateOperators(doc: T, update: UpdateOperators): T {
    const result = this.cloneDocument(doc);

    // Apply $set
    if (update.$set) {
      for (const [path, value] of Object.entries(update.$set)) {
        this.setValueByPath(result as Record<string, unknown>, path, value);
      }
    }

    // Apply $unset
    if (update.$unset) {
      for (const path of Object.keys(update.$unset)) {
        this.deleteValueByPath(result as Record<string, unknown>, path);
      }
    }

    // Apply $inc
    if (update.$inc) {
      for (const [path, increment] of Object.entries(update.$inc)) {
        const currentValue = this.getValueAtPath(
          result as Record<string, unknown>,
          path
        );
        const numericCurrent =
          typeof currentValue === "number" ? currentValue : 0;
        this.setValueByPath(
          result as Record<string, unknown>,
          path,
          numericCurrent + increment
        );
      }
    }

    // Apply $push
    if (update.$push) {
      for (const [path, value] of Object.entries(update.$push)) {
        const currentValue = this.getValueAtPath(
          result as Record<string, unknown>,
          path
        );

        if (currentValue === undefined) {
          // Field doesn't exist - create new array
          if (this.isPushEachModifier(value)) {
            this.setValueByPath(
              result as Record<string, unknown>,
              path,
              [...(value as { $each: unknown[] }).$each]
            );
          } else {
            this.setValueByPath(result as Record<string, unknown>, path, [value]);
          }
        } else if (Array.isArray(currentValue)) {
          // Field is an array - push to it
          if (this.isPushEachModifier(value)) {
            currentValue.push(...(value as { $each: unknown[] }).$each);
          } else {
            currentValue.push(value);
          }
        } else {
          // Field exists but is not an array
          const fieldType = currentValue === null ? "null" : typeof currentValue;
          throw new Error(
            `The field '${path}' must be an array but is of type ${fieldType}`
          );
        }
      }
    }

    // Apply $addToSet
    if (update.$addToSet) {
      for (const [path, value] of Object.entries(update.$addToSet)) {
        const currentValue = this.getValueAtPath(
          result as Record<string, unknown>,
          path
        );

        if (currentValue === undefined) {
          // Field doesn't exist - create new array
          if (this.isPushEachModifier(value)) {
            this.setValueByPath(
              result as Record<string, unknown>,
              path,
              [...(value as { $each: unknown[] }).$each]
            );
          } else {
            this.setValueByPath(result as Record<string, unknown>, path, [value]);
          }
        } else if (Array.isArray(currentValue)) {
          // Field is an array - add unique values
          // Use strictKeyOrder=true for BSON-style object comparison (key order matters)
          if (this.isPushEachModifier(value)) {
            for (const v of (value as { $each: unknown[] }).$each) {
              if (!currentValue.some((existing) => this.valuesEqual(existing, v, true))) {
                currentValue.push(v);
              }
            }
          } else {
            if (!currentValue.some((existing) => this.valuesEqual(existing, value, true))) {
              currentValue.push(value);
            }
          }
        } else {
          // Field exists but is not an array
          const fieldType = currentValue === null ? "null" : typeof currentValue;
          throw new Error(
            `The field '${path}' must be an array but is of type ${fieldType}`
          );
        }
      }
    }

    // Apply $pop
    if (update.$pop) {
      for (const [path, direction] of Object.entries(update.$pop)) {
        if (direction !== 1 && direction !== -1) {
          throw new Error("$pop expects 1 or -1");
        }

        const currentValue = this.getValueAtPath(
          result as Record<string, unknown>,
          path
        );

        if (currentValue === undefined) {
          // Field doesn't exist - no-op
          continue;
        }

        if (!Array.isArray(currentValue)) {
          throw new Error(
            `Cannot apply $pop to a non-array value`
          );
        }

        if (currentValue.length > 0) {
          if (direction === 1) {
            currentValue.pop();
          } else {
            currentValue.shift();
          }
        }
      }
    }

    // Apply $pull
    if (update.$pull) {
      for (const [path, condition] of Object.entries(update.$pull)) {
        const currentValue = this.getValueAtPath(
          result as Record<string, unknown>,
          path
        );

        if (currentValue === undefined) {
          // Field doesn't exist - no-op
          continue;
        }

        if (!Array.isArray(currentValue)) {
          throw new Error(
            `Cannot apply $pull to a non-array value`
          );
        }

        // Filter out matching elements
        const filteredArray = currentValue.filter((elem) => {
          if (this.isOperatorObject(condition)) {
            // Condition is an operator expression
            return !this.matchesOperators(elem, condition as QueryOperators);
          } else if (
            condition !== null &&
            typeof condition === "object" &&
            !Array.isArray(condition)
          ) {
            // Condition is an object - match fields
            return !this.matchesPullCondition(elem, condition as Record<string, unknown>);
          } else {
            // Condition is a direct value - exact match
            return !this.valuesEqual(elem, condition);
          }
        });

        // Update the array in place
        currentValue.length = 0;
        currentValue.push(...filteredArray);
      }
    }

    return result;
  }

  /**
   * Check if a value is a $push/$addToSet $each modifier.
   */
  private isPushEachModifier(value: unknown): value is { $each: unknown[] } {
    return (
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      "$each" in value &&
      Array.isArray((value as { $each: unknown }).$each)
    );
  }

  /**
   * Check if an array element matches a $pull object condition.
   */
  private matchesPullCondition(
    element: unknown,
    condition: Record<string, unknown>
  ): boolean {
    // Element must be an object
    if (element === null || typeof element !== "object" || Array.isArray(element)) {
      return false;
    }

    const elemObj = element as Record<string, unknown>;

    for (const [key, value] of Object.entries(condition)) {
      const fieldValue = this.getValueByPath(elemObj, key);

      if (this.isOperatorObject(value)) {
        if (!this.matchesOperators(fieldValue, value as QueryOperators)) {
          return false;
        }
      } else {
        if (!this.valuesEqual(fieldValue, value)) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Create a document from filter for upsert.
   * Extracts simple equality conditions and $eq values from the filter.
   */
  private createDocumentFromFilter(filter: Filter<T>): T {
    const doc: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(filter)) {
      if (!this.isOperatorObject(value)) {
        // Simple equality value
        if (key.includes(".")) {
          this.setValueByPath(doc, key, value);
        } else {
          doc[key] = value;
        }
      } else if (
        value &&
        typeof value === "object" &&
        "$eq" in value &&
        Object.keys(value).length === 1
      ) {
        // Extract $eq value (only if $eq is the only operator)
        const eqValue = (value as { $eq: unknown }).$eq;
        if (key.includes(".")) {
          this.setValueByPath(doc, key, eqValue);
        } else {
          doc[key] = eqValue;
        }
      }
    }

    return doc as T;
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
  async findOne(
    filter: Filter<T> = {},
    options: FindOptions = {}
  ): Promise<T | null> {
    const documents = await this.readDocuments();

    for (const doc of documents) {
      if (this.matchesFilter(doc, filter)) {
        if (options.projection) {
          return applyProjection(doc, options.projection);
        }
        return doc;
      }
    }

    return null;
  }

  /**
   * Find documents matching the filter.
   * Returns a cursor for further operations.
   */
  find(filter: Filter<T> = {}, options: FindOptions = {}): MongoneCursor<T> {
    return new MongoneCursor<T>(
      async () => {
        const documents = await this.readDocuments();
        return documents.filter((doc) => this.matchesFilter(doc, filter));
      },
      options.projection || null
    );
  }

  /**
   * Count documents matching the filter.
   */
  async countDocuments(filter: Filter<T> = {}): Promise<number> {
    const documents = await this.readDocuments();
    return documents.filter((doc) => this.matchesFilter(doc, filter)).length;
  }

  /**
   * Delete a single document matching the filter.
   */
  async deleteOne(filter: Filter<T>): Promise<DeleteResult> {
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
  async deleteMany(filter: Filter<T>): Promise<DeleteResult> {
    const documents = await this.readDocuments();
    const remaining = documents.filter((doc) => !this.matchesFilter(doc, filter));
    const deletedCount = documents.length - remaining.length;

    await this.writeDocuments(remaining);

    return {
      acknowledged: true,
      deletedCount,
    };
  }

  /**
   * Update a single document matching the filter.
   */
  async updateOne(
    filter: Filter<T>,
    update: UpdateOperators,
    options: UpdateOptions = {}
  ): Promise<UpdateResult> {
    const documents = await this.readDocuments();
    let matchedCount = 0;
    let modifiedCount = 0;
    let upsertedId: ObjectId | null = null;
    let upsertedCount = 0;

    let matchFound = false;
    const updatedDocuments: T[] = [];

    for (const doc of documents) {
      if (!matchFound && this.matchesFilter(doc, filter)) {
        matchFound = true;
        matchedCount = 1;
        const updatedDoc = this.applyUpdateOperators(doc, update);

        // Check if document actually changed
        if (!this.documentsEqual(doc, updatedDoc)) {
          modifiedCount = 1;
          updatedDocuments.push(updatedDoc);
        } else {
          updatedDocuments.push(doc);
        }
      } else {
        updatedDocuments.push(doc);
      }
    }

    // Handle upsert
    if (!matchFound && options.upsert) {
      const baseDoc = this.createDocumentFromFilter(filter);
      const newDoc = this.applyUpdateOperators(baseDoc, update);

      // Add _id if not present
      if (!("_id" in newDoc)) {
        (newDoc as Record<string, unknown>)._id = new ObjectId();
      }

      upsertedId = (newDoc as unknown as { _id: ObjectId })._id;
      upsertedCount = 1;
      updatedDocuments.push(newDoc);
    }

    await this.writeDocuments(updatedDocuments);

    return {
      acknowledged: true,
      matchedCount,
      modifiedCount,
      upsertedCount,
      upsertedId,
    };
  }

  /**
   * Update all documents matching the filter.
   */
  async updateMany(
    filter: Filter<T>,
    update: UpdateOperators,
    options: UpdateOptions = {}
  ): Promise<UpdateResult> {
    const documents = await this.readDocuments();
    let matchedCount = 0;
    let modifiedCount = 0;
    let upsertedId: ObjectId | null = null;
    let upsertedCount = 0;

    const updatedDocuments: T[] = [];

    for (const doc of documents) {
      if (this.matchesFilter(doc, filter)) {
        matchedCount++;
        const updatedDoc = this.applyUpdateOperators(doc, update);

        // Check if document actually changed
        if (!this.documentsEqual(doc, updatedDoc)) {
          modifiedCount++;
          updatedDocuments.push(updatedDoc);
        } else {
          updatedDocuments.push(doc);
        }
      } else {
        updatedDocuments.push(doc);
      }
    }

    // Handle upsert - only if no matches found
    if (matchedCount === 0 && options.upsert) {
      const baseDoc = this.createDocumentFromFilter(filter);
      const newDoc = this.applyUpdateOperators(baseDoc, update);

      // Add _id if not present
      if (!("_id" in newDoc)) {
        (newDoc as Record<string, unknown>)._id = new ObjectId();
      }

      upsertedId = (newDoc as unknown as { _id: ObjectId })._id;
      upsertedCount = 1;
      updatedDocuments.push(newDoc);
    }

    await this.writeDocuments(updatedDocuments);

    return {
      acknowledged: true,
      matchedCount,
      modifiedCount,
      upsertedCount,
      upsertedId,
    };
  }
}
