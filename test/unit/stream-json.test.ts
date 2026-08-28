/**
 * Unit tests for the streaming JSON-array reader (src/stream-json.ts).
 *
 * The point of this module is that it never builds the whole file as a single
 * string, so the tests deliberately drive it at a 1-byte chunk size as well as
 * the default: every state transition (inside a string, mid-escape, mid-number,
 * between nesting levels) is then forced to straddle a chunk boundary.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { streamJsonArrayEntries, readJsonArray, countJsonArray } from '../../src/stream-json.ts';

let dir: string;
let file: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'mango-stream-'));
  file = join(dir, 'docs.json');
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

/** Write `text` and read it back at both the default and a 1-byte chunk size. */
async function parseBothChunkSizes(text: string): Promise<unknown[]> {
  await writeFile(file, text);
  const viaDefault = await readJsonArray(file);

  const viaTiny: unknown[] = [];
  for await (const entry of streamJsonArrayEntries(file, 1)) {
    viaTiny.push(JSON.parse(entry));
  }

  assert.deepStrictEqual(
    viaTiny,
    viaDefault,
    'a 1-byte chunk size must parse identically to the default'
  );
  return viaDefault;
}

/** Serialize exactly the way MangoCollection.writeDocuments does. */
function asWritten(docs: unknown[]): string {
  const body = docs
    .map((d) =>
      JSON.stringify(d, null, 2)
        .split('\n')
        .map((line) => '  ' + line)
        .join('\n')
    )
    .join(',\n');
  return '[\n' + body + '\n]';
}

describe('streamJsonArrayEntries', () => {
  it('reads an empty array', async () => {
    assert.deepStrictEqual(await parseBothChunkSizes('[]'), []);
  });

  it('reads the empty array shape writeDocuments actually emits', async () => {
    // writeDocuments writes '[\n' then '\n]' with nothing between.
    assert.deepStrictEqual(await parseBothChunkSizes('[\n\n]'), []);
  });

  it('reads a single document', async () => {
    const docs = [{ _id: 'a', n: 1 }];
    assert.deepStrictEqual(await parseBothChunkSizes(asWritten(docs)), docs);
  });

  it('reads many documents in the written format', async () => {
    const docs = Array.from({ length: 50 }, (_, i) => ({ _id: `id-${i}`, i }));
    assert.deepStrictEqual(await parseBothChunkSizes(asWritten(docs)), docs);
  });

  it('handles nested objects and arrays', async () => {
    const docs = [
      { _id: 1, nested: { a: [1, 2, { b: 'c' }], d: {} }, list: [[], [{}]] },
      { _id: 2, empty: {}, arr: [] },
    ];
    assert.deepStrictEqual(await parseBothChunkSizes(asWritten(docs)), docs);
  });

  it('does not mistake JSON punctuation inside strings for structure', async () => {
    const docs = [
      { s: '{"not":"json"} , ] [ }' },
      { s: 'comma, brace }, bracket ]' },
      { s: '[[[' },
    ];
    assert.deepStrictEqual(await parseBothChunkSizes(asWritten(docs)), docs);
  });

  it('handles escaped quotes and backslashes', async () => {
    const docs = [
      { s: 'he said "hi"' },
      { s: 'trailing backslash \\' },
      { s: '\\"' },
      { s: 'newline\nand\ttab' },
    ];
    assert.deepStrictEqual(await parseBothChunkSizes(asWritten(docs)), docs);
  });

  it('handles unicode and multi-byte characters', async () => {
    const docs = [{ s: '日本語 — emoji 🥭 ok' }, { s: 'é́' }];
    assert.deepStrictEqual(await parseBothChunkSizes(asWritten(docs)), docs);
  });

  it('reads scalar array elements', async () => {
    assert.deepStrictEqual(await parseBothChunkSizes('[1, -2.5, 1e3, true, false, null, "s"]'), [
      1,
      -2.5,
      1e3,
      true,
      false,
      null,
      's',
    ]);
  });

  it('tolerates unusual but valid whitespace', async () => {
    assert.deepStrictEqual(
      await parseBothChunkSizes('\n\t [\r\n {"a":1}\t,\n\n {"b":2} \r\n]\n '),
      [{ a: 1 }, { b: 2 }]
    );
  });

  it('matches JSON.parse on a file larger than one chunk', async () => {
    const docs = Array.from({ length: 400 }, (_, i) => ({
      _id: i,
      blob: 'x'.repeat(500),
    }));
    const text = asWritten(docs);
    assert.ok(text.length > 1 << 16, 'fixture must exceed the default chunk size');
    await writeFile(file, text);
    assert.deepStrictEqual(await readJsonArray(file), JSON.parse(text));
  });

  it('throws on a non-array top-level value', async () => {
    await writeFile(file, '{"a":1}');
    await assert.rejects(() => readJsonArray(file), SyntaxError);
  });

  it('throws on an unterminated array', async () => {
    await writeFile(file, '[{"a":1}');
    await assert.rejects(() => readJsonArray(file), SyntaxError);
  });

  it('throws on an unterminated string', async () => {
    await writeFile(file, '[{"a":"oops}');
    await assert.rejects(() => readJsonArray(file), SyntaxError);
  });

  it('throws on trailing content after the array', async () => {
    await writeFile(file, '[] junk');
    await assert.rejects(() => readJsonArray(file), SyntaxError);
  });

  it('propagates ENOENT for a missing file', async () => {
    await assert.rejects(() => readJsonArray(join(dir, 'nope.json')), {
      code: 'ENOENT',
    });
  });
});

describe('countJsonArray', () => {
  it('counts without retaining documents', async () => {
    const docs = Array.from({ length: 123 }, (_, i) => ({ i }));
    await writeFile(file, asWritten(docs));
    assert.strictEqual(await countJsonArray(file), 123);
  });

  it('counts an empty collection as zero', async () => {
    await writeFile(file, '[\n\n]');
    assert.strictEqual(await countJsonArray(file), 0);
  });
});
