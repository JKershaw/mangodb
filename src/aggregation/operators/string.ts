/**
 * String expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";
import { getBSONTypeName } from "../helpers.ts";

export function evalConcat(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string | null {
  const values = args.map((a) => evaluate(a, doc, vars));

  if (values.some((v) => v === null || v === undefined)) {
    return null;
  }

  for (const v of values) {
    if (typeof v !== "string") {
      const typeName = getBSONTypeName(v);
      throw new Error(`$concat only supports strings, not ${typeName}`);
    }
  }

  return values.join("");
}

export function evalToUpper(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value !== "string") {
    throw new Error("$toUpper requires a string argument");
  }

  return value.toUpperCase();
}

export function evalToLower(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value !== "string") {
    throw new Error("$toLower requires a string argument");
  }

  return value.toLowerCase();
}

export function evalSubstrCP(
  args: unknown[],
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string {
  const [strExpr, startExpr, countExpr] = args;
  const str = evaluate(strExpr, doc);
  const start = evaluate(startExpr, doc) as number;
  const count = evaluate(countExpr, doc) as number;

  if (str === null || str === undefined) {
    return "";
  }

  if (typeof str !== "string") {
    const typeName = getBSONTypeName(str);
    throw new Error(`$substrCP requires a string argument, found: ${typeName}`);
  }

  return str.substring(start, start + count);
}

export function evalStrLenCP(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    const typeName = value === null ? "null" : "missing";
    throw new Error(`$strLenCP requires a string argument, found: ${typeName}`);
  }

  if (typeof value !== "string") {
    const typeName = getBSONTypeName(value);
    throw new Error(`$strLenCP requires a string argument, found: ${typeName}`);
  }

  return value.length;
}

export function evalSplit(
  args: unknown[],
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string[] | null {
  const [strExpr, delimExpr] = args;
  const str = evaluate(strExpr, doc);
  const delim = evaluate(delimExpr, doc);

  if (str === null || str === undefined) {
    return null;
  }

  if (typeof str !== "string") {
    const typeName = getBSONTypeName(str);
    throw new Error(`$split requires a string as the first argument, found: ${typeName}`);
  }

  if (delim === null || delim === undefined) {
    throw new Error("$split requires a string as the second argument, found: null");
  }

  if (typeof delim !== "string") {
    const typeName = getBSONTypeName(delim);
    throw new Error(`$split requires a string as the second argument, found: ${typeName}`);
  }

  return str.split(delim);
}

export function evalTrim(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string {
  const spec = args as { input: unknown; chars?: unknown };
  const input = evaluate(spec.input, doc);
  const chars = spec.chars ? (evaluate(spec.chars, doc) as string) : undefined;

  if (typeof input !== "string") {
    const typeName = getBSONTypeName(input);
    throw new Error(`$trim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    const charSet = new Set(chars.split(""));
    let start = 0;
    let end = input.length;
    while (start < end && charSet.has(input[start])) start++;
    while (end > start && charSet.has(input[end - 1])) end--;
    return input.substring(start, end);
  }

  return input.trim();
}

export function evalLTrim(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string {
  const spec = args as { input: unknown; chars?: unknown };
  const input = evaluate(spec.input, doc);
  const chars = spec.chars ? (evaluate(spec.chars, doc) as string) : undefined;

  if (typeof input !== "string") {
    const typeName = getBSONTypeName(input);
    throw new Error(`$ltrim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    const charSet = new Set(chars.split(""));
    let start = 0;
    while (start < input.length && charSet.has(input[start])) start++;
    return input.substring(start);
  }

  return input.trimStart();
}

export function evalRTrim(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string {
  const spec = args as { input: unknown; chars?: unknown };
  const input = evaluate(spec.input, doc);
  const chars = spec.chars ? (evaluate(spec.chars, doc) as string) : undefined;

  if (typeof input !== "string") {
    const typeName = getBSONTypeName(input);
    throw new Error(`$rtrim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    const charSet = new Set(chars.split(""));
    let end = input.length;
    while (end > 0 && charSet.has(input[end - 1])) end--;
    return input.substring(0, end);
  }

  return input.trimEnd();
}

export function evalToString(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string | null {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "number") {
    return String(value);
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value && typeof (value as { toHexString?: unknown }).toHexString === "function") {
    return (value as { toHexString: () => string }).toHexString();
  }

  const typeName = getBSONTypeName(value);
  throw new Error(`Unsupported conversion from ${typeName} to string`);
}

export function evalIndexOfCP(
  args: unknown[],
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number | null {
  const [strExpr, substrExpr, startExpr, endExpr] = args;
  const str = evaluate(strExpr, doc);
  const substr = evaluate(substrExpr, doc);
  const start = startExpr !== undefined ? (evaluate(startExpr, doc) as number) : 0;
  const end = endExpr !== undefined ? (evaluate(endExpr, doc) as number) : undefined;

  if (str === null || str === undefined) {
    return null;
  }

  if (substr === null || substr === undefined) {
    throw new Error("$indexOfCP requires a string as the second argument, found: null");
  }

  if (typeof str !== "string") {
    const typeName = getBSONTypeName(str);
    throw new Error(`$indexOfCP requires a string as the first argument, found: ${typeName}`);
  }

  if (typeof substr !== "string") {
    const typeName = getBSONTypeName(substr);
    throw new Error(`$indexOfCP requires a string as the second argument, found: ${typeName}`);
  }

  const searchStr = end !== undefined ? str.substring(0, end) : str;
  return searchStr.indexOf(substr, start);
}
