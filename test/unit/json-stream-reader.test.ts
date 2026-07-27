/**
 * Unit tests for the streaming JSON array reader.
 *
 * These tests exercise the scanner directly against fixture files. The scanner
 * carries the correctness burden for the read path (MAN-21 / issue #52): it
 * replaces `readFile(utf-8) + JSON.parse`, which throws `RangeError: Invalid
 * string length` once a collection file crosses V8's max string length (~536MB)
 * and permanently bricks the collection.
 *
 * The 536MB regression itself is not asserted here - it needs more time and disk
 * than CI has. Instead the scanner is tested exhaustively against adversarial
 * content, both on-disk layouts, and corrupt/truncated files.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  streamJsonArray,
  streamJsonArrayText,
  countJsonArrayElements,
  readJsonArray,
  MAX_WHOLE_FILE_READ_BYTES,
} from '../../src/json-stream-reader.ts';

describe('json-stream-reader', () => {
  let dir: string;
  let fileCounter = 0;

  before(async () => {
    dir = await mkdtemp(join(tmpdir(), 'mangodb_jsr_'));
  });

  after(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  /** Write `content` to a fresh file and return its path. */
  async function fixture(content: string): Promise<string> {
    const path = join(dir, `fixture_${fileCounter++}.json`);
    await writeFile(path, content, 'utf-8');
    return path;
  }

  /** Drain the reader into an array. */
  async function readAll(path: string, chunkSize?: number): Promise<unknown[]> {
    const out: unknown[] = [];
    for await (const doc of streamJsonArray(path, { chunkSize })) {
      out.push(doc);
    }
    return out;
  }

  /**
   * The layout `writeDocuments()` emits since PR #51 (streaming write):
   * `[\n` + per-document `JSON.stringify(doc, null, 2)` indented 2 spaces,
   * joined by `,\n`, + `\n]`.
   */
  function postStreamingLayout(docs: unknown[]): string {
    const body = docs
      .map((doc) =>
        JSON.stringify(doc, null, 2)
          .split('\n')
          .map((line) => '  ' + line)
          .join('\n')
      )
      .join(',\n');
    return `[\n${body}\n]`;
  }

  /** The layout written before PR #51: `JSON.stringify(docs, null, 2)`. */
  function preStreamingLayout(docs: unknown[]): string {
    return JSON.stringify(docs, null, 2);
  }

  // ==================== Layout compatibility ====================

  describe('on-disk layout compatibility', () => {
    const docs = [
      { _id: 'a', name: 'Alice', tags: ['x', 'y'], nested: { deep: { n: 1 } } },
      { _id: 'b', name: 'Bob', tags: [], nested: { deep: { n: 2 } } },
    ];

    it('should read the post-streaming-write layout', async () => {
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should read the pre-streaming-write layout', async () => {
      const path = await fixture(preStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should read a compact (no whitespace) layout', async () => {
      const path = await fixture(JSON.stringify(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should produce identical results across all three layouts', async () => {
      const [post, pre, compact] = await Promise.all([
        readAll(await fixture(postStreamingLayout(docs))),
        readAll(await fixture(preStreamingLayout(docs))),
        readAll(await fixture(JSON.stringify(docs))),
      ]);

      assert.deepStrictEqual(post, pre);
      assert.deepStrictEqual(pre, compact);
    });

    it('should tolerate a trailing newline', async () => {
      const path = await fixture(postStreamingLayout(docs) + '\n');

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should tolerate leading and trailing whitespace', async () => {
      const path = await fixture(`\n\t  ${JSON.stringify(docs)}  \r\n `);

      assert.deepStrictEqual(await readAll(path), docs);
    });
  });

  // ==================== Shapes ====================

  describe('array shapes', () => {
    it('should yield nothing for an empty array', async () => {
      assert.deepStrictEqual(await readAll(await fixture('[]')), []);
    });

    it('should yield nothing for an empty array with whitespace', async () => {
      assert.deepStrictEqual(await readAll(await fixture('[\n  \n]\n')), []);
    });

    it('should read a single document', async () => {
      const path = await fixture('[\n  {\n    "a": 1\n  }\n]');

      assert.deepStrictEqual(await readAll(path), [{ a: 1 }]);
    });

    it('should read scalar elements', async () => {
      const path = await fixture('[1, "two", true, false, null, 3.5, -1e3]');

      assert.deepStrictEqual(await readAll(path), [1, 'two', true, false, null, 3.5, -1000]);
    });

    it('should read nested arrays as elements', async () => {
      const path = await fixture('[[1, 2], [], [[3]], {"a": [4, {"b": 5}]}]');

      assert.deepStrictEqual(await readAll(path), [[1, 2], [], [[3]], { a: [4, { b: 5 }] }]);
    });

    it('should read deeply nested documents', async () => {
      const deep = { l1: { l2: { l3: { l4: { l5: [{ l6: 'bottom' }] } } } } };
      const path = await fixture(postStreamingLayout([deep]));

      assert.deepStrictEqual(await readAll(path), [deep]);
    });

    it('should preserve document order', async () => {
      const many = Array.from({ length: 200 }, (_, i) => ({ i }));
      const path = await fixture(postStreamingLayout(many));

      assert.deepStrictEqual(await readAll(path), many);
    });
  });

  // ==================== Adversarial string content ====================

  describe('adversarial string content', () => {
    it('should not treat braces inside string values as structure', async () => {
      const docs = [{ s: '{{{' }, { s: '}}}' }, { s: '{"fake": "doc"}' }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should not treat brackets inside string values as structure', async () => {
      const docs = [{ s: '[[[' }, { s: ']]]' }, { s: '[1, 2, 3]' }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should not treat commas inside string values as separators', async () => {
      const docs = [{ s: 'a,b,c' }, { s: ',,,' }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should handle escaped quotes inside string values', async () => {
      const docs = [{ s: 'he said "hi"' }, { s: '"' }, { s: '\\"' }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should handle trailing backslashes before a closing quote', async () => {
      const docs = [{ s: 'ends with backslash \\' }, { s: '\\\\' }, { s: '\\\\"' }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should handle escape sequences and control characters', async () => {
      const docs = [{ s: 'line1\nline2\ttabbed' }, { s: '\u0000\u001f' }, { s: '\r\n' }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should handle a value that looks like the whole file', async () => {
      const docs = [{ s: '[{"a": 1}, {"b": 2}]' }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });

    it('should handle keys containing structural characters', async () => {
      const docs = [{ '{weird[key],': 1, 'quote"key': 2 }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path), docs);
    });
  });

  // ==================== Chunk boundaries ====================

  describe('chunk boundaries', () => {
    const docs = [
      { _id: 1, s: 'a,b {} [] "q"', emoji: '🥭🥭🥭', accented: 'café naïve' },
      { _id: 2, s: 'x'.repeat(300), emoji: '日本語テキスト' },
      { _id: 3, nested: { a: [1, 2, { b: 'c' }] } },
    ];

    for (const chunkSize of [1, 2, 3, 5, 7, 16, 64, 1024]) {
      it(`should produce the same documents with chunkSize=${chunkSize}`, async () => {
        const path = await fixture(postStreamingLayout(docs));

        assert.deepStrictEqual(await readAll(path, chunkSize), docs);
      });
    }

    it('should not split multi-byte characters across chunks', async () => {
      // Emoji are 4 UTF-8 bytes; a 3-byte chunk guarantees mid-character splits.
      const docs = [{ s: '🥭'.repeat(50) }, { s: '→'.repeat(50) }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path, 3), docs);
    });

    it('should handle an element larger than one chunk', async () => {
      const docs = [{ blob: 'y'.repeat(5000) }];
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readAll(path, 64), docs);
    });
  });

  // ==================== Corrupt and truncated files ====================

  describe('corrupt and truncated files', () => {
    const cases: Array<[string, string]> = [
      ['an empty file', ''],
      ['a whitespace-only file', '  \n\t '],
      ['a file truncated before the closing bracket', '[{"a": 1}, {"b": 2}'],
      ['a file truncated mid-document', '[{"a": 1}, {"b":'],
      ['a file truncated mid-string', '[{"a": "unterminated'],
      ['a top-level object instead of an array', '{"a": 1}'],
      ['a top-level scalar', '42'],
      ['a trailing comma', '[{"a": 1},]'],
      ['a doubled comma', '[{"a": 1},,{"b": 2}]'],
      ['a leading comma', '[,{"a": 1}]'],
      ['unbalanced closing braces', '[{"a": 1}}]'],
      ['content after the closing bracket', '[{"a": 1}] trailing'],
      ['a second array after the closing bracket', '[{"a": 1}][{"b": 2}]'],
    ];

    for (const [name, content] of cases) {
      it(`should throw a SyntaxError for ${name}`, async () => {
        const path = await fixture(content);

        await assert.rejects(() => readAll(path), SyntaxError);
      });
    }

    it('should throw a SyntaxError for a structurally sound but invalid element', async () => {
      // The scanner sees balanced braces; JSON.parse rejects the unquoted key.
      const path = await fixture('[{a: 1}]');

      await assert.rejects(() => readAll(path), SyntaxError);
    });

    it('should include the file path in the error message', async () => {
      const path = await fixture('[{"a": 1}');

      await assert.rejects(
        () => readAll(path),
        (error: Error) => error.message.includes(path)
      );
    });

    it('should yield the documents preceding the corruption before throwing', async () => {
      const path = await fixture('[{"a": 1}, {"b": 2}, {"c":');
      const seen: unknown[] = [];

      await assert.rejects(async () => {
        for await (const doc of streamJsonArray(path)) {
          seen.push(doc);
        }
      }, SyntaxError);

      assert.deepStrictEqual(seen, [{ a: 1 }, { b: 2 }]);
    });
  });

  // ==================== Missing files ====================

  describe('missing files', () => {
    it('should propagate ENOENT', async () => {
      const path = join(dir, 'does_not_exist.json');

      await assert.rejects(
        () => readAll(path),
        (error: NodeJS.ErrnoException) => error.code === 'ENOENT'
      );
    });
  });

  // ==================== Early termination ====================

  describe('early termination', () => {
    it('should stop reading when the consumer breaks out', async () => {
      const docs = Array.from({ length: 500 }, (_, i) => ({ i, pad: 'z'.repeat(200) }));
      const path = await fixture(postStreamingLayout(docs));

      const seen: unknown[] = [];
      for await (const doc of streamJsonArray(path, { chunkSize: 64 })) {
        seen.push(doc);
        if (seen.length === 3) break;
      }

      assert.deepStrictEqual(seen, docs.slice(0, 3));
    });

    it('should not throw on a corrupt tail if the consumer stops first', async () => {
      const path = await fixture('[{"a": 1}, {"b": 2}, {"c":');

      const seen: unknown[] = [];
      for await (const doc of streamJsonArray(path)) {
        seen.push(doc);
        break;
      }

      assert.deepStrictEqual(seen, [{ a: 1 }]);
    });
  });

  // ==================== streamJsonArrayText ====================

  describe('streamJsonArrayText', () => {
    it('should yield trimmed raw element text', async () => {
      const path = await fixture('[\n  {"a": 1},\n  {"b": 2}\n]');

      const texts: string[] = [];
      for await (const text of streamJsonArrayText(path)) {
        texts.push(text);
      }

      assert.deepStrictEqual(texts, ['{"a": 1}', '{"b": 2}']);
    });
  });

  // ==================== readJsonArray ====================

  describe('readJsonArray', () => {
    /**
     * `readJsonArray` reads small files whole (native `JSON.parse`, several
     * times faster) and streams large ones. Both branches must be
     * indistinguishable to callers, so every case here is asserted twice:
     * once at the default limit, and once with the limit forced to 0 so the
     * streaming branch runs on the same fixture.
     */
    async function bothBranches(path: string): Promise<[unknown[], unknown[]]> {
      return [await readJsonArray(path), await readJsonArray(path, { wholeFileLimit: 0 })];
    }

    it('should default to a limit well below V8 max string length', () => {
      assert.ok(MAX_WHOLE_FILE_READ_BYTES > 0);
      // V8's cap is ~536MB; a UTF-8 file never decodes to more UTF-16 units
      // than it has bytes, so the limit in bytes must stay clear of it.
      assert.ok(MAX_WHOLE_FILE_READ_BYTES < 500 * 1024 * 1024);
    });

    it('should read documents identically on both branches', async () => {
      const docs = [
        { _id: 'a', s: '}, {"injected": true}, {', tags: ['x'] },
        { _id: 'b', s: 'quote " backslash \\ comma ,', nested: { deep: [1, 2] } },
        { _id: 'c', s: '🥭 日本語' },
      ];
      const path = await fixture(postStreamingLayout(docs));

      const [whole, streamed] = await bothBranches(path);

      assert.deepStrictEqual(whole, docs);
      assert.deepStrictEqual(streamed, docs);
    });

    it('should read the pre-streaming-write layout on both branches', async () => {
      const docs = [{ a: 1 }, { b: 2 }];
      const path = await fixture(preStreamingLayout(docs));

      const [whole, streamed] = await bothBranches(path);

      assert.deepStrictEqual(whole, docs);
      assert.deepStrictEqual(streamed, docs);
    });

    it('should return an empty array for an empty collection on both branches', async () => {
      const path = await fixture('[]');

      const [whole, streamed] = await bothBranches(path);

      assert.deepStrictEqual(whole, []);
      assert.deepStrictEqual(streamed, []);
    });

    it('should stream when the file exceeds the limit', async () => {
      // 1-byte limit forces the streaming branch on any non-empty file.
      const docs = Array.from({ length: 100 }, (_, i) => ({ i }));
      const path = await fixture(postStreamingLayout(docs));

      assert.deepStrictEqual(await readJsonArray(path, { wholeFileLimit: 1 }), docs);
    });

    it('should throw a SyntaxError naming the file on both branches', async () => {
      const path = await fixture('[{"a": 1}, {"b":');

      for (const options of [{}, { wholeFileLimit: 0 }]) {
        await assert.rejects(
          () => readJsonArray(path, options),
          (error: Error) => error instanceof SyntaxError && error.message.includes(path)
        );
      }
    });

    it('should reject a top-level object on both branches', async () => {
      const path = await fixture('{"a": 1}');

      for (const options of [{}, { wholeFileLimit: 0 }]) {
        await assert.rejects(() => readJsonArray(path, options), SyntaxError);
      }
    });

    it('should propagate ENOENT on both branches', async () => {
      const path = join(dir, 'read_missing.json');

      for (const options of [{}, { wholeFileLimit: 0 }]) {
        await assert.rejects(
          () => readJsonArray(path, options),
          (error: NodeJS.ErrnoException) => error.code === 'ENOENT'
        );
      }
    });
  });

  // ==================== countJsonArrayElements ====================

  describe('countJsonArrayElements', () => {
    it('should count documents without materializing them', async () => {
      const docs = Array.from({ length: 137 }, (_, i) => ({ i }));
      const path = await fixture(postStreamingLayout(docs));

      assert.strictEqual(await countJsonArrayElements(path), 137);
    });

    it('should return 0 for an empty array', async () => {
      assert.strictEqual(await countJsonArrayElements(await fixture('[]')), 0);
    });

    it('should count elements in the pre-streaming-write layout', async () => {
      const docs = Array.from({ length: 5 }, (_, i) => ({ i }));
      const path = await fixture(preStreamingLayout(docs));

      assert.strictEqual(await countJsonArrayElements(path), 5);
    });

    it('should not be confused by structural characters inside strings', async () => {
      const path = await fixture('[{"s": "},{"}, {"s": "],["}]');

      assert.strictEqual(await countJsonArrayElements(path), 2);
    });

    it('should throw a SyntaxError on a truncated file', async () => {
      const path = await fixture('[{"a": 1}');

      await assert.rejects(() => countJsonArrayElements(path), SyntaxError);
    });

    it('should propagate ENOENT', async () => {
      await assert.rejects(
        () => countJsonArrayElements(join(dir, 'nope.json')),
        (error: NodeJS.ErrnoException) => error.code === 'ENOENT'
      );
    });
  });
});
