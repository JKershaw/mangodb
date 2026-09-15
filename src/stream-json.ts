/**
 * Streaming reader for the JSON-array files a collection is stored in.
 *
 * The write path (`MangoCollection.writeDocuments`) already streams one
 * document at a time, so a large collection never has to exist as a single JS
 * string on the way out. The read path had no equivalent: `readFile(path,
 * 'utf-8')` builds the whole file as one string, and past V8's maximum string
 * length (`buffer.constants.MAX_STRING_LENGTH`, ~512MB) that throws
 * `RangeError: Invalid string length` before `JSON.parse` ever runs. A
 * collection that crossed the ceiling therefore became permanently unreadable
 * — and unshrinkable, because `deleteMany` has to read the current documents
 * before it can write fewer of them.
 *
 * This scanner walks the file in chunks, tracking string/escape state and
 * nesting depth, and hands back one complete top-level value at a time. Only a
 * single document is ever held as a string, with an explicit per-value string
 * length limit. It deliberately does not change the
 * whole-collection-in-memory design of `readDocuments()`, which still returns a
 * full array; lifting that needs the NDJSON rearchitecture and is out of scope.
 *
 * The format written by `writeDocuments` is a pretty-printed array of objects,
 * but the scanner is a general JSON-array reader (objects, arrays, strings,
 * numbers, booleans, null) so files written by older versions — or by hand —
 * parse identically.
 */
import { createReadStream } from 'node:fs';
import { constants } from 'node:buffer';

/** Maximum serialized value length in UTF-16 code units, not UTF-8 bytes. */
export const MAX_VALUE_LENGTH = constants.MAX_STRING_LENGTH;

export class OversizedJsonValueError extends RangeError {
  readonly filePath: string;
  readonly maxValueLength: number;
  readonly byteOffset?: number;

  constructor(
    filePath: string,
    maxValueLength: number,
    context: { byteOffset?: number; documentId?: string; cause?: unknown } = {}
  ) {
    const location =
      context.byteOffset !== undefined
        ? ` at byte offset ${context.byteOffset}`
        : ` for document _id ${context.documentId ?? '(unknown)'}`;
    super(
      `JSON value in ${filePath}${location} exceeds the serialized value limit of ${maxValueLength} UTF-16 code units; split the document or store large payloads separately`,
      { cause: context.cause }
    );
    this.name = 'OversizedJsonValueError';
    this.filePath = filePath;
    this.maxValueLength = maxValueLength;
    this.byteOffset = context.byteOffset;
  }
}

/** Characters JSON treats as insignificant whitespace between tokens. */
function isWhitespace(ch: string): boolean {
  return ch === ' ' || ch === '\n' || ch === '\r' || ch === '\t';
}

/** True when `ch` terminates an unquoted scalar (number/true/false/null). */
function endsScalar(ch: string): boolean {
  return isWhitespace(ch) || ch === ',' || ch === ']';
}

/**
 * Yield the raw JSON text of each element of the top-level array in `filePath`,
 * one element at a time, without ever materialising the whole file as a string.
 *
 * @param filePath - Path to a file whose top-level value is a JSON array.
 * @param chunkSize - Read buffer size in bytes; exposed for tests.
 * @param maxValueLength - Per-value UTF-16 code-unit limit; exposed for tests.
 * @throws {SyntaxError} If the file is not a well-formed JSON array.
 */
export async function* streamJsonArrayEntries(
  filePath: string,
  chunkSize = 1 << 16,
  maxValueLength = MAX_VALUE_LENGTH
): AsyncGenerator<string> {
  if (
    !Number.isInteger(maxValueLength) ||
    maxValueLength < 1 ||
    maxValueLength > MAX_VALUE_LENGTH
  ) {
    throw new RangeError(`maxValueLength must be between 1 and ${MAX_VALUE_LENGTH}`);
  }
  const stream = createReadStream(filePath, {
    encoding: 'utf-8',
    highWaterMark: chunkSize,
  });

  let value = '';
  let start = -1; // where the value currently being scanned began, or -1
  let byteOffset = 0;
  let bytesRead = 0;
  let depth = 0; // nesting depth inside the current value
  let inString = false;
  let escaped = false;
  let sawOpen = false; // consumed the array's opening '['
  let sawClose = false; // consumed the array's closing ']'

  for await (const chunk of stream) {
    const buf = chunk as string;
    let pos = 0;
    let bytePos = 0;

    while (pos < buf.length) {
      const ch = buf[pos]!;

      if (
        start !== -1 &&
        (inString || depth > 0 || !endsScalar(ch)) &&
        value.length + pos - start >= maxValueLength
      ) {
        throw new OversizedJsonValueError(filePath, maxValueLength, { byteOffset });
      }

      // Inside a quoted string: only escape handling and the closing quote matter.
      if (inString) {
        if (escaped) escaped = false;
        else if (ch === '\\') escaped = true;
        else if (ch === '"') {
          inString = false;
          // A bare top-level string is a complete value the moment it closes.
          if (start !== -1 && depth === 0) {
            pos++;
            yield value + buf.slice(start, pos);
            value = '';
            start = -1;
            continue;
          }
        }
        pos++;
        continue;
      }

      if (sawClose) {
        if (!isWhitespace(ch)) {
          throw new SyntaxError(`Unexpected trailing content in ${filePath}`);
        }
        pos++;
        continue;
      }

      if (!sawOpen) {
        if (ch === '[') sawOpen = true;
        else if (!isWhitespace(ch)) {
          throw new SyntaxError(`Expected a JSON array in ${filePath}`);
        }
        pos++;
        continue;
      }

      // Between values: skip separators, detect the end of the array, or open
      // a new value.
      if (start === -1) {
        if (isWhitespace(ch) || ch === ',') {
          pos++;
          continue;
        }
        if (ch === ']') {
          sawClose = true;
          pos++;
          continue;
        }
        start = pos;
        bytesRead += Buffer.byteLength(buf.slice(bytePos, pos), 'utf8');
        bytePos = pos;
        byteOffset = bytesRead;
        if (ch === '{' || ch === '[') {
          depth = 1;
        } else if (ch === '"') {
          inString = true;
        }
        pos++;
        continue;
      }

      // Inside a structured value: track nesting until it closes.
      if (depth > 0) {
        if (ch === '"') inString = true;
        else if (ch === '{' || ch === '[') depth++;
        else if (ch === '}' || ch === ']') {
          depth--;
          if (depth === 0) {
            pos++;
            yield value + buf.slice(start, pos);
            value = '';
            start = -1;
            continue;
          }
        }
        pos++;
        continue;
      }

      // Inside an unquoted scalar: it ends at whitespace, a comma or the
      // array's closing bracket, none of which are consumed here.
      if (endsScalar(ch)) {
        yield value + buf.slice(start, pos);
        value = '';
        start = -1;
        continue;
      }
      pos++;
    }

    // Append only checked value text, never the remainder of its final chunk.
    // This removes the read-buffer overshoot without reserving a chunk on writes.
    if (start !== -1) {
      value += buf.slice(start, pos);
      start = 0;
    }
    bytesRead += Buffer.byteLength(buf.slice(bytePos), 'utf8');
  }

  // A trailing unquoted scalar with no delimiter before EOF (e.g. `[1`) is
  // unterminated, as is any value or string still open.
  if (inString || depth > 0 || start !== -1 || !sawOpen || !sawClose) {
    throw new SyntaxError(`Unexpected end of JSON input in ${filePath}`);
  }
}

/**
 * Read a JSON array file into memory one element at a time.
 *
 * Equivalent in result to `JSON.parse(await readFile(path, 'utf-8'))` for an
 * array file, but never builds the whole file as a single string.
 */
export async function readJsonArray<T = unknown>(
  filePath: string,
  maxValueLength = MAX_VALUE_LENGTH
): Promise<T[]> {
  const out: T[] = [];
  for await (const entry of streamJsonArrayEntries(filePath, undefined, maxValueLength)) {
    out.push(JSON.parse(entry) as T);
  }
  return out;
}

/**
 * Count the elements of a JSON array file without parsing or retaining them.
 * Used by `db.stats()`, which only ever needed the length.
 */
export async function countJsonArray(
  filePath: string,
  maxValueLength = MAX_VALUE_LENGTH
): Promise<number> {
  let n = 0;
  for await (const _entry of streamJsonArrayEntries(filePath, undefined, maxValueLength)) n++;
  return n;
}
