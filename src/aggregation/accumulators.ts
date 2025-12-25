/**
 * Accumulator classes for $group stage.
 */
import type { Document } from "../types.ts";
import { compareValuesForSort } from "../utils.ts";
import { valuesEqual } from "../document-utils.ts";
import { evaluateExpression } from "./expression.ts";

export interface Accumulator {
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
    if (!this.values.some((v) => valuesEqual(v, value))) {
      this.values.push(value);
    }
  }

  getResult(): unknown[] {
    return this.values;
  }
}

/**
 * $count accumulator - counts the number of documents in a group.
 * Note: This is the accumulator form, not the $count stage.
 */
class CountAccumulator implements Accumulator {
  private count = 0;

  accumulate(_doc: Document): void {
    this.count++;
  }

  getResult(): number {
    return this.count;
  }
}

/**
 * $mergeObjects accumulator - merges documents into a single document.
 */
class MergeObjectsAccumulator implements Accumulator {
  private result: Record<string, unknown> = {};
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (value !== null && value !== undefined && typeof value === "object" && !Array.isArray(value)) {
      Object.assign(this.result, value);
    }
  }

  getResult(): Record<string, unknown> {
    return this.result;
  }
}

/**
 * $stdDevPop accumulator - calculates the population standard deviation.
 */
class StdDevPopAccumulator implements Accumulator {
  private values: number[] = [];
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (typeof value === "number" && !isNaN(value)) {
      this.values.push(value);
    }
  }

  getResult(): number | null {
    if (this.values.length === 0) {
      return null;
    }

    const n = this.values.length;
    const mean = this.values.reduce((a, b) => a + b, 0) / n;
    const squaredDiffs = this.values.map((v) => Math.pow(v - mean, 2));
    const variance = squaredDiffs.reduce((a, b) => a + b, 0) / n;
    return Math.sqrt(variance);
  }
}

/**
 * $stdDevSamp accumulator - calculates the sample standard deviation.
 */
class StdDevSampAccumulator implements Accumulator {
  private values: number[] = [];
  private expr: unknown;

  constructor(expr: unknown) {
    this.expr = expr;
  }

  accumulate(doc: Document): void {
    const value = evaluateExpression(this.expr, doc);
    if (typeof value === "number" && !isNaN(value)) {
      this.values.push(value);
    }
  }

  getResult(): number | null {
    if (this.values.length < 2) {
      return null;
    }

    const n = this.values.length;
    const mean = this.values.reduce((a, b) => a + b, 0) / n;
    const squaredDiffs = this.values.map((v) => Math.pow(v - mean, 2));
    const variance = squaredDiffs.reduce((a, b) => a + b, 0) / (n - 1);
    return Math.sqrt(variance);
  }
}

export function createAccumulator(op: string, expr: unknown): Accumulator {
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
    case "$count":
      return new CountAccumulator();
    case "$mergeObjects":
      return new MergeObjectsAccumulator(expr);
    case "$stdDevPop":
      return new StdDevPopAccumulator(expr);
    case "$stdDevSamp":
      return new StdDevSampAccumulator(expr);
    default:
      throw new Error(`unknown group operator '${op}'`);
  }
}
