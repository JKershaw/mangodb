import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import fs, { type WriteStream } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { mkdtemp, rm, readFile, readdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { MangoCollection } from '../../../src/collection.ts';
import { MangoDb } from '../../../src/db.ts';
import {
  MAX_VALUE_LENGTH,
  OversizedJsonValueError,
  readJsonArray,
  countJsonArray,
  streamJsonArrayEntries,
} from '../../../src/stream-json.ts';
import { isMongoDBMode } from '../../test-harness.ts';

describe('MangoDB document storage limits', { skip: isMongoDBMode() }, () => {
  let dir: string;
  let file: string;
  let collection: MangoCollection;
  let streams: WriteStream[];

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'mango-size-'));
    file = join(dir, 'db', 'docs.json');
    collection = new MangoCollection(dir, 'db', 'docs');
    streams = [];
    const createWriteStream = fs.createWriteStream;
    mock.method(fs, 'createWriteStream', (...args: Parameters<typeof createWriteStream>) => {
      const stream = createWriteStream(...args);
      streams.push(stream);
      return stream;
    });
    syncBuiltinESMExports();
    await collection.insertOne({ _id: 'existing', n: 1 });
  });

  afterEach(async () => {
    mock.restoreAll();
    syncBuiltinESMExports();
    await rm(dir, { recursive: true, force: true });
  });

  async function writeWithLimit(documents: Record<string, unknown>[], limit: number) {
    // Inject a small limit only at the private storage boundary, not in the public API.
    await (
      collection as unknown as {
        writeDocuments(docs: Record<string, unknown>[], maxValueLength: number): Promise<void>;
      }
    ).writeDocuments(documents, limit);
  }

  async function assertPreserved(original: string) {
    assert.equal(await readFile(file, 'utf8'), original);
    assert.deepEqual(
      (await readdir(join(dir, 'db'))).filter((name) => name.includes('.tmp-')),
      []
    );
    assert.ok(streams.every((stream) => stream.destroyed && stream.closed));
    assert.equal((await collection.findOne({ _id: 'existing' }))?.n, 1);
  }

  it('round-trips the exact write boundary, including internal pretty-print indentation', async () => {
    const doc = { _id: 'edge', s: 'é🥭'.repeat(25), nested: { n: 1 } };
    const expected = JSON.stringify(doc, null, 2).replace(/\n/g, '\n  ');
    const limit = expected.length;
    assert.ok(limit < MAX_VALUE_LENGTH);
    await writeWithLimit([doc, { _id: 'next' }], limit);
    assert.deepEqual(await readJsonArray(file, limit), [doc, { _id: 'next' }]);
    assert.deepEqual(await collection.findOne({ _id: 'edge' }), doc);
    for (const chunkSize of [1, 7, limit, 65536]) {
      const entries: string[] = [];
      for await (const entry of streamJsonArrayEntries(file, chunkSize, limit)) entries.push(entry);
      assert.equal(entries[0], expected);
    }
    assert.equal(
      await readFile(file, 'utf8'),
      '[\n  ' + expected + ',\n  {\n    "_id": "next"\n  }\n]'
    );
  });

  it('rejects one unit over the write boundary and closes before removing the temp file', async () => {
    const original = await readFile(file, 'utf8');
    const doc = { _id: 'too-large', s: 'x'.repeat(100) };
    const length = JSON.stringify(doc, null, 2).replace(/\n/g, '\n  ').length;
    await assert.rejects(writeWithLimit([{ _id: 'first' }, doc], length - 1), (error: unknown) => {
      assert.ok(error instanceof OversizedJsonValueError);
      assert.ok(error instanceof RangeError);
      assert.equal(error.filePath, file);
      assert.match(error.message, /too-large/);
      return true;
    });
    await assertPreserved(original);
    await collection.insertOne({ _id: 'after-failure' });
  });

  for (const message of ['Invalid string length', 'Maximum call stack size exceeded']) {
    it(`safely handles serialization failure: ${message}`, async () => {
      const original = await readFile(file, 'utf8');
      const failure = new RangeError(message);
      const stringify = JSON.stringify;
      mock.method(JSON, 'stringify', (...args: Parameters<typeof JSON.stringify>) => {
        if (args[0]?._id === 'broken') throw failure;
        return stringify(...args);
      });
      await assert.rejects(collection.insertOne({ _id: 'broken' }), (error: unknown) => {
        if (message === 'Invalid string length') {
          assert.ok(error instanceof OversizedJsonValueError);
          assert.equal(error.cause, failure);
          assert.match(error.message, /broken/);
        } else {
          assert.equal(error, failure);
        }
        return true;
      });
      await assertPreserved(original);
    });
  }

  it('propagates oversized-value diagnostics from counting through db.stats()', async () => {
    let error: OversizedJsonValueError;
    await assert.rejects(countJsonArray(file, 10), (actual: unknown) => {
      assert.ok(actual instanceof OversizedJsonValueError);
      error = actual;
      return true;
    });
    // Inject the real counter diagnostic at the I/O seam: stats has no public
    // limit override, and allocating a runtime-limit document is not viable.
    mock.method(fs, 'createReadStream', () => {
      throw error;
    });
    syncBuiltinESMExports();
    await assert.rejects(new MangoDb(dir, 'db').stats(), (actual) => actual === error);
  });

  it('closes the reader when an externally authored oversized value is rejected', async () => {
    await writeFile(file, '[{"s":"é🥭oversized"}]');
    const createReadStream = fs.createReadStream;
    const readers: fs.ReadStream[] = [];
    mock.method(fs, 'createReadStream', (...args: Parameters<typeof createReadStream>) => {
      const stream = createReadStream(...args);
      readers.push(stream);
      return stream;
    });
    syncBuiltinESMExports();
    await assert.rejects(readJsonArray(file, 7), OversizedJsonValueError);
    for (const stream of readers) {
      if (!stream.closed) await new Promise<void>((resolve) => stream.once('close', resolve));
      assert.ok(stream.destroyed && stream.closed);
    }
    assert.equal(readers.length, 1);
  });

  it('cleans up a rejected first write even when the stream has not opened yet', async () => {
    const fresh = new MangoCollection(dir, 'db', 'fresh');
    await assert.rejects(
      (
        fresh as unknown as {
          writeDocuments(docs: Record<string, unknown>[], maxValueLength: number): Promise<void>;
        }
      ).writeDocuments([{ _id: 'large', s: 'x'.repeat(100) }], 20),
      OversizedJsonValueError
    );
    assert.ok(streams.every((stream) => stream.destroyed && stream.closed));
    assert.ok(!(await readdir(join(dir, 'db'))).some((name) => name.startsWith('fresh')));
  });

  it('preserves stats handling of unrelated malformed collection and index files', async () => {
    await writeFile(join(dir, 'db', 'broken.json'), '[{"unterminated":');
    await writeFile(join(dir, 'db', 'broken.indexes.json'), 'not JSON');
    const stats = await new MangoDb(dir, 'db').stats();
    assert.equal(stats.objects, 1);
    assert.equal(stats.collections, 2);
  });
});
