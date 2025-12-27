/**
 * String expression operators.
 */
import type { Document } from '../../types.ts';
import type { VariableContext, EvaluateExpressionFn } from '../types.ts';
import { getBSONTypeName } from '../helpers.ts';

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
    if (typeof v !== 'string') {
      const typeName = getBSONTypeName(v);
      throw new Error(`$concat only supports strings, not ${typeName}`);
    }
  }

  return values.join('');
}

export function evalToUpper(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    return '';
  }

  if (typeof value !== 'string') {
    throw new Error('$toUpper requires a string argument');
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
    return '';
  }

  if (typeof value !== 'string') {
    throw new Error('$toLower requires a string argument');
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
    return '';
  }

  if (typeof str !== 'string') {
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
    const typeName = value === null ? 'null' : 'missing';
    throw new Error(`$strLenCP requires a string argument, found: ${typeName}`);
  }

  if (typeof value !== 'string') {
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

  if (typeof str !== 'string') {
    const typeName = getBSONTypeName(str);
    throw new Error(`$split requires a string as the first argument, found: ${typeName}`);
  }

  if (delim === null || delim === undefined) {
    throw new Error('$split requires a string as the second argument, found: null');
  }

  if (typeof delim !== 'string') {
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

  if (typeof input !== 'string') {
    const typeName = getBSONTypeName(input);
    throw new Error(`$trim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    const charSet = new Set(chars.split(''));
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

  if (typeof input !== 'string') {
    const typeName = getBSONTypeName(input);
    throw new Error(`$ltrim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    const charSet = new Set(chars.split(''));
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

  if (typeof input !== 'string') {
    const typeName = getBSONTypeName(input);
    throw new Error(`$rtrim requires its input to be a string, got ${typeName}`);
  }

  if (chars) {
    const charSet = new Set(chars.split(''));
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

  if (typeof value === 'string') {
    return value;
  }

  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }

  if (typeof value === 'number') {
    return String(value);
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value && typeof (value as { toHexString?: unknown }).toHexString === 'function') {
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
    throw new Error('$indexOfCP requires a string as the second argument, found: null');
  }

  if (typeof str !== 'string') {
    const typeName = getBSONTypeName(str);
    throw new Error(`$indexOfCP requires a string as the first argument, found: ${typeName}`);
  }

  if (typeof substr !== 'string') {
    const typeName = getBSONTypeName(substr);
    throw new Error(`$indexOfCP requires a string as the second argument, found: ${typeName}`);
  }

  const searchStr = end !== undefined ? str.substring(0, end) : str;
  return searchStr.indexOf(substr, start);
}

/**
 * $regexFind - Returns info about first regex match.
 */
export function evalRegexFind(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): { match: string; idx: number; captures: string[] } | null {
  const spec = args as { input: unknown; regex: unknown; options?: unknown };
  const input = evaluate(spec.input, doc, vars);
  const regex = evaluate(spec.regex, doc, vars);
  const options = spec.options ? (evaluate(spec.options, doc, vars) as string) : '';

  if (input === null || input === undefined) {
    return null;
  }

  if (typeof input !== 'string') {
    const typeName = getBSONTypeName(input);
    throw new Error(`$regexFind requires a string input, not ${typeName}`);
  }

  let regexStr: string;
  let regexOptions: string = options;

  if (regex instanceof RegExp) {
    regexStr = regex.source;
    regexOptions = regex.flags + options;
  } else if (typeof regex === 'string') {
    regexStr = regex;
  } else {
    throw new Error('$regexFind requires regex as string or RegExp');
  }

  const re = new RegExp(regexStr, regexOptions.replace('g', ''));
  const match = re.exec(input);

  if (!match) {
    return null;
  }

  return {
    match: match[0],
    idx: match.index,
    captures: match.slice(1),
  };
}

/**
 * $regexFindAll - Returns array of all regex matches.
 */
export function evalRegexFindAll(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): Array<{ match: string; idx: number; captures: string[] }> {
  const spec = args as { input: unknown; regex: unknown; options?: unknown };
  const input = evaluate(spec.input, doc, vars);
  const regex = evaluate(spec.regex, doc, vars);
  const options = spec.options ? (evaluate(spec.options, doc, vars) as string) : '';

  if (input === null || input === undefined) {
    return [];
  }

  if (typeof input !== 'string') {
    const typeName = getBSONTypeName(input);
    throw new Error(`$regexFindAll requires a string input, not ${typeName}`);
  }

  let regexStr: string;
  let regexOptions: string = options;

  if (regex instanceof RegExp) {
    regexStr = regex.source;
    regexOptions = regex.flags + options;
  } else if (typeof regex === 'string') {
    regexStr = regex;
  } else {
    throw new Error('$regexFindAll requires regex as string or RegExp');
  }

  // Always use global flag for findAll
  if (!regexOptions.includes('g')) {
    regexOptions += 'g';
  }

  const re = new RegExp(regexStr, regexOptions);
  const results: Array<{ match: string; idx: number; captures: string[] }> = [];
  let match: RegExpExecArray | null;

  while ((match = re.exec(input)) !== null) {
    results.push({
      match: match[0],
      idx: match.index,
      captures: match.slice(1),
    });
  }

  return results;
}

/**
 * $regexMatch - Returns true if string matches regex.
 */
export function evalRegexMatch(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const spec = args as { input: unknown; regex: unknown; options?: unknown };
  const input = evaluate(spec.input, doc, vars);
  const regex = evaluate(spec.regex, doc, vars);
  const options = spec.options ? (evaluate(spec.options, doc, vars) as string) : '';

  if (input === null || input === undefined) {
    return false;
  }

  if (typeof input !== 'string') {
    const typeName = getBSONTypeName(input);
    throw new Error(`$regexMatch requires a string input, not ${typeName}`);
  }

  let regexStr: string;
  let regexOptions: string = options;

  if (regex instanceof RegExp) {
    regexStr = regex.source;
    regexOptions = regex.flags + options;
  } else if (typeof regex === 'string') {
    regexStr = regex;
  } else {
    throw new Error('$regexMatch requires regex as string or RegExp');
  }

  const re = new RegExp(regexStr, regexOptions);
  return re.test(input);
}

/**
 * $replaceOne - Replaces first occurrence of search string.
 */
export function evalReplaceOne(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string | null {
  const spec = args as { input: unknown; find: unknown; replacement: unknown };
  const input = evaluate(spec.input, doc, vars);
  const find = evaluate(spec.find, doc, vars);
  const replacement = evaluate(spec.replacement, doc, vars);

  if (input === null || input === undefined) {
    return null;
  }

  if (typeof input !== 'string') {
    const typeName = getBSONTypeName(input);
    throw new Error(`$replaceOne requires a string input, not ${typeName}`);
  }

  if (find === null || find === undefined) {
    throw new Error("$replaceOne requires a 'find' string");
  }

  if (typeof find !== 'string') {
    const typeName = getBSONTypeName(find);
    throw new Error(`$replaceOne requires a string for 'find', not ${typeName}`);
  }

  if (replacement === null || replacement === undefined) {
    throw new Error("$replaceOne requires a 'replacement' string");
  }

  if (typeof replacement !== 'string') {
    const typeName = getBSONTypeName(replacement);
    throw new Error(`$replaceOne requires a string for 'replacement', not ${typeName}`);
  }

  return input.replace(find, replacement);
}

/**
 * $replaceAll - Replaces all occurrences of search string.
 */
export function evalReplaceAll(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): string | null {
  const spec = args as { input: unknown; find: unknown; replacement: unknown };
  const input = evaluate(spec.input, doc, vars);
  const find = evaluate(spec.find, doc, vars);
  const replacement = evaluate(spec.replacement, doc, vars);

  if (input === null || input === undefined) {
    return null;
  }

  if (typeof input !== 'string') {
    const typeName = getBSONTypeName(input);
    throw new Error(`$replaceAll requires a string input, not ${typeName}`);
  }

  if (find === null || find === undefined) {
    throw new Error("$replaceAll requires a 'find' string");
  }

  if (typeof find !== 'string') {
    const typeName = getBSONTypeName(find);
    throw new Error(`$replaceAll requires a string for 'find', not ${typeName}`);
  }

  if (replacement === null || replacement === undefined) {
    throw new Error("$replaceAll requires a 'replacement' string");
  }

  if (typeof replacement !== 'string') {
    const typeName = getBSONTypeName(replacement);
    throw new Error(`$replaceAll requires a string for 'replacement', not ${typeName}`);
  }

  return input.split(find).join(replacement);
}

/**
 * $strcasecmp - Case-insensitive string comparison.
 * Returns: -1 if str1 < str2, 0 if equal, 1 if str1 > str2
 */
export function evalStrcasecmp(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number {
  const [str1Expr, str2Expr] = args;
  const str1 = evaluate(str1Expr, doc, vars);
  const str2 = evaluate(str2Expr, doc, vars);

  const s1 = str1 === null || str1 === undefined ? '' : String(str1);
  const s2 = str2 === null || str2 === undefined ? '' : String(str2);

  const lower1 = s1.toLowerCase();
  const lower2 = s2.toLowerCase();

  if (lower1 < lower2) return -1;
  if (lower1 > lower2) return 1;
  return 0;
}

/**
 * $strLenBytes - Returns the number of UTF-8 encoded bytes in the string.
 */
export function evalStrLenBytes(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): number {
  const value = evaluate(args, doc);

  if (value === null || value === undefined) {
    const typeName = value === null ? 'null' : 'missing';
    throw new Error(`$strLenBytes requires a string argument, found: ${typeName}`);
  }

  if (typeof value !== 'string') {
    const typeName = getBSONTypeName(value);
    throw new Error(`$strLenBytes requires a string argument, found: ${typeName}`);
  }

  // Calculate UTF-8 byte length
  return new TextEncoder().encode(value).length;
}

/**
 * $indexOfBytes - Returns the byte index of first occurrence.
 */
export function evalIndexOfBytes(
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

  if (typeof str !== 'string' || typeof substr !== 'string') {
    throw new Error('$indexOfBytes requires string arguments');
  }

  // Convert to bytes
  const strBytes = new TextEncoder().encode(str);
  const substrBytes = new TextEncoder().encode(substr);

  const searchEnd = end !== undefined ? Math.min(end, strBytes.length) : strBytes.length;

  for (let i = start; i <= searchEnd - substrBytes.length; i++) {
    let found = true;
    for (let j = 0; j < substrBytes.length; j++) {
      if (strBytes[i + j] !== substrBytes[j]) {
        found = false;
        break;
      }
    }
    if (found) {
      return i;
    }
  }

  return -1;
}

/**
 * $substrBytes - Returns substring by byte positions.
 */
export function evalSubstrBytes(
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
    return '';
  }

  if (typeof str !== 'string') {
    const typeName = getBSONTypeName(str);
    throw new Error(`$substrBytes requires a string argument, found: ${typeName}`);
  }

  const strBytes = new TextEncoder().encode(str);
  const subBytes = strBytes.slice(start, start + count);
  return new TextDecoder().decode(subBytes);
}
