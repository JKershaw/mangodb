/**
 * Streaming reader for JSON array files.
 *
 * Collection files are a single top-level JSON array of documents. Reading one
 * with `readFile(path, 'utf-8')` materializes the whole file as one JavaScript
 * string, and V8 caps string length at roughly 536MB - so a large collection
 * throws `RangeError: Invalid string length` before `JSON.parse` ever runs, and
 * the collection becomes permanently unreadable (not even `deleteMany` can
 * shrink it, because it has to read the current document set first).
 *
 * This module reads the file as a stream and hands back one element at a time,
 * so no single string ever holds more than one document. The scanner is
 * depth/quote/escape aware rather than whitespace dependent, so it reads any
 * valid JSON array layout - the pretty-printed output `writeDocuments()` emits
 * today, the `JSON.stringify(docs, null, 2)` output written before streaming
 * writes landed, and compact output alike.
 *
 * Backpressure is inherent: the underlying stream is only pulled when the
 * consumer asks for the next element, so a slow consumer does not buffer the
 * file. Note that this bounds *string* size, not total memory - a caller that
 * collects every element into an array still holds the whole collection.
 *
 * Scanning in JavaScript is several times slower than V8's native `JSON.parse`
 * over a whole file, so {@link readJsonArray} - the entry point for callers that
 * are going to materialize every element anyway - only streams once the file is
 * large enough for the ceiling to be a concern. Callers that do *not* need every
 * element in memory (counting, iterating with early termination) should use the
 * streaming entry points directly, where the scan pays for itself.
 */
import { createReadStream } from 'node:fs';
import { readFile, stat } from 'node:fs/promises';

/** Default read buffer size, in bytes. */
const DEFAULT_CHUNK_SIZE = 64 * 1024;

/**
 * Largest file {@link readJsonArray} will read whole, in bytes.
 *
 * A UTF-8 file never decodes to more UTF-16 code units than it has bytes, so a
 * file at or below this size is comfortably inside V8's ~536MB string limit -
 * roughly an eightfold margin, and far above the collection sizes MangoDB is
 * meant for.
 */
export const MAX_WHOLE_FILE_READ_BYTES = 64 * 1024 * 1024;

// Character codes, compared numerically because the scanner visits every byte
// of the file and `charCodeAt` avoids allocating a string per character.
const CHAR_TAB = 9;
const CHAR_LF = 10;
const CHAR_CR = 13;
const CHAR_SPACE = 32;
const CHAR_QUOTE = 34;
const CHAR_COMMA = 44;
const CHAR_OPEN_BRACKET = 91;
const CHAR_BACKSLASH = 92;
const CHAR_CLOSE_BRACKET = 93;
const CHAR_OPEN_BRACE = 123;
const CHAR_CLOSE_BRACE = 125;

/** Anything JSON does not accept as insignificant whitespace. */
const NON_WHITESPACE = /[^ \t\n\r]/;

export interface StreamJsonArrayOptions {
  /**
   * Read buffer size in bytes. Exposed for testing chunk-boundary handling and
   * for tuning; the default suits normal collection files.
   */
  chunkSize?: number;
}

export interface ReadJsonArrayOptions extends StreamJsonArrayOptions {
  /**
   * Files larger than this are streamed instead of read whole. Defaults to
   * {@link MAX_WHOLE_FILE_READ_BYTES}; exposed so tests can exercise the
   * streaming branch without building a 64MB fixture.
   */
  wholeFileLimit?: number;
}

function isWhitespace(code: number): boolean {
  return code === CHAR_SPACE || code === CHAR_LF || code === CHAR_TAB || code === CHAR_CR;
}

function corrupt(filePath: string, detail: string): SyntaxError {
  return new SyntaxError(`Invalid JSON array in ${filePath}: ${detail}`);
}

/**
 * Stream the raw text of each top-level element of a JSON array file.
 *
 * @description Yields the (whitespace-trimmed) source text of each element
 * without parsing it. The scanner validates the array's structure - balanced
 * braces/brackets, terminated strings, no missing or doubled separators - but
 * not the contents of an individual element, so a caller that needs valid
 * documents must parse what it receives. Throws `SyntaxError` on a corrupt or
 * truncated file, and propagates filesystem errors (notably `ENOENT`) from the
 * underlying stream.
 *
 * @param filePath - Path to a file containing a single top-level JSON array
 * @param options - Reader options
 * @yields The source text of each array element, in file order
 *
 * @example
 * ```typescript
 * for await (const text of streamJsonArrayText('users.json')) {
 *   console.log(text); // '{"_id": "...", "name": "Alice"}'
 * }
 * ```
 */
export async function* streamJsonArrayText(
  filePath: string,
  options: StreamJsonArrayOptions = {}
): AsyncGenerator<string> {
  const stream = createReadStream(filePath, {
    encoding: 'utf-8',
    highWaterMark: options.chunkSize ?? DEFAULT_CHUNK_SIZE,
  });

  // Scanner state, carried across chunk boundaries.
  let sawOpenBracket = false;
  let sawCloseBracket = false;
  let depth = 0;
  let inString = false;
  let escaped = false;
  let yieldedAny = false;
  // Text of the element currently being scanned, from earlier chunks only.
  let pending = '';

  // Scanning regexes. Scoped to this call rather than the module: they carry
  // `lastIndex` state, and generators interleave.
  const structural = /["[\]{},]/g; // state-changing characters outside a string
  const stringDelimiter = /[\\"]/g; // ... and inside one

  try {
    for await (const chunk of stream as AsyncIterable<string>) {
      const length = chunk.length;
      // Where the current element's text starts within this chunk. Text before
      // this point has already been yielded or belongs to `pending`.
      let elementStart = 0;
      let i = 0;

      // The scan never walks character by character over ordinary content: it
      // jumps straight to the next character that can change the parse state.
      // That matters because this runs over every byte of every read.
      while (i < length) {
        if (sawCloseBracket) {
          if (NON_WHITESPACE.test(chunk.slice(i))) {
            throw corrupt(filePath, 'unexpected content after the closing "]"');
          }
          break;
        }

        if (!sawOpenBracket) {
          const code = chunk.charCodeAt(i);
          if (isWhitespace(code)) {
            i++;
            continue;
          }
          if (code !== CHAR_OPEN_BRACKET) {
            throw corrupt(filePath, `expected "[" but found ${JSON.stringify(chunk[i])}`);
          }
          sawOpenBracket = true;
          i++;
          elementStart = i;
          continue;
        }

        if (inString) {
          if (escaped) {
            // The escaping backslash ended the previous chunk.
            escaped = false;
            i++;
            continue;
          }
          // One forward scan for whichever comes first, a backslash or the
          // closing quote - searching for each separately would rescan the
          // string once per escape it contains.
          stringDelimiter.lastIndex = i;
          const delimiter = stringDelimiter.exec(chunk);
          if (delimiter === null) {
            // The string runs past the end of this chunk.
            i = length;
            continue;
          }
          if (chunk.charCodeAt(delimiter.index) === CHAR_BACKSLASH) {
            if (delimiter.index + 1 < length) {
              i = delimiter.index + 2;
            } else {
              escaped = true;
              i = length;
            }
            continue;
          }
          inString = false;
          i = delimiter.index + 1;
          continue;
        }

        structural.lastIndex = i;
        const match = structural.exec(chunk);
        if (match === null) break;
        const at = match.index;
        const code = chunk.charCodeAt(at);
        i = at + 1;

        switch (code) {
          case CHAR_QUOTE:
            inString = true;
            break;

          case CHAR_OPEN_BRACE:
          case CHAR_OPEN_BRACKET:
            depth++;
            break;

          case CHAR_CLOSE_BRACE:
            depth--;
            if (depth < 0) {
              throw corrupt(filePath, 'unbalanced "}"');
            }
            break;

          case CHAR_CLOSE_BRACKET: {
            if (depth > 0) {
              depth--;
              break;
            }
            // Depth 0: this closes the top-level array.
            const text = (pending + chunk.slice(elementStart, at)).trim();
            pending = '';
            sawCloseBracket = true;
            if (text.length > 0) {
              yieldedAny = true;
              yield text;
            } else if (yieldedAny) {
              throw corrupt(filePath, 'trailing comma before the closing "]"');
            }
            break;
          }

          case CHAR_COMMA: {
            if (depth > 0) break;
            const text = (pending + chunk.slice(elementStart, at)).trim();
            pending = '';
            elementStart = i;
            if (text.length === 0) {
              throw corrupt(filePath, 'missing element before ","');
            }
            yieldedAny = true;
            yield text;
            break;
          }
        }
      }

      if (sawOpenBracket && !sawCloseBracket) {
        pending += chunk.slice(elementStart);
      }
    }
  } finally {
    stream.destroy();
  }

  if (!sawOpenBracket) {
    throw corrupt(filePath, 'expected "[" but the file is empty');
  }
  if (!sawCloseBracket) {
    if (inString) {
      throw corrupt(filePath, 'unexpected end of file inside a string');
    }
    throw corrupt(filePath, 'unexpected end of file before the closing "]"');
  }
}

/**
 * Stream and parse each element of a JSON array file.
 *
 * @description Parses one element at a time, so no single string holds more than
 * one document. Throws `SyntaxError` if the file is corrupt or an element is not
 * valid JSON, and propagates filesystem errors (notably `ENOENT`).
 *
 * @param filePath - Path to a file containing a single top-level JSON array
 * @param options - Reader options
 * @yields Each parsed array element, in file order
 *
 * @example
 * ```typescript
 * for await (const doc of streamJsonArray('users.json')) {
 *   console.log(doc);
 * }
 * ```
 */
export async function* streamJsonArray(
  filePath: string,
  options: StreamJsonArrayOptions = {}
): AsyncGenerator<unknown> {
  for await (const text of streamJsonArrayText(filePath, options)) {
    yield JSON.parse(text);
  }
}

/**
 * Read every element of a JSON array file into an array.
 *
 * @description The entry point for callers that need the whole array in memory.
 * Small files are read whole and parsed natively, which is several times faster
 * than scanning them in JavaScript; files large enough to approach V8's string
 * length limit are streamed instead, so they stay readable rather than throwing
 * `RangeError: Invalid string length`. Both paths return the same values and
 * raise `SyntaxError` (naming the file) on corrupt content, and propagate
 * filesystem errors such as `ENOENT`.
 *
 * Streaming buys nothing here beyond staying under the ceiling - the result
 * holds every element either way - so it is not used until it has to be.
 *
 * @param filePath - Path to a file containing a single top-level JSON array
 * @param options - Reader options
 * @returns Every element of the array, in file order
 *
 * @example
 * ```typescript
 * const documents = await readJsonArray('users.json');
 * ```
 */
export async function readJsonArray(
  filePath: string,
  options: ReadJsonArrayOptions = {}
): Promise<unknown[]> {
  const { size } = await stat(filePath);

  if (size > (options.wholeFileLimit ?? MAX_WHOLE_FILE_READ_BYTES)) {
    const elements: unknown[] = [];
    for await (const element of streamJsonArray(filePath, options)) {
      elements.push(element);
    }
    return elements;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(await readFile(filePath, 'utf-8'));
  } catch (error) {
    if (error instanceof SyntaxError) {
      // Match the streaming path, which names the offending file.
      throw corrupt(filePath, error.message);
    }
    throw error;
  }
  if (!Array.isArray(parsed)) {
    throw corrupt(filePath, 'expected a top-level JSON array');
  }
  return parsed;
}

/**
 * Count the elements of a JSON array file without materializing them.
 *
 * @description Scans the file's structure only - elements are never parsed, so a
 * structurally sound file containing an invalid document still counts. Throws
 * `SyntaxError` if the array's structure is corrupt or truncated, and propagates
 * filesystem errors (notably `ENOENT`).
 *
 * @param filePath - Path to a file containing a single top-level JSON array
 * @returns The number of elements in the array
 *
 * @example
 * ```typescript
 * const count = await countJsonArrayElements('users.json');
 * ```
 */
export async function countJsonArrayElements(
  filePath: string,
  options: StreamJsonArrayOptions = {}
): Promise<number> {
  let count = 0;
  for await (const _text of streamJsonArrayText(filePath, options)) {
    count++;
  }
  return count;
}
