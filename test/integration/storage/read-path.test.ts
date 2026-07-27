/**
 * Read-path streaming tests (MAN-21 / issue #52).
 *
 * `readDocuments()` used to read a collection file with `readFile(utf-8)` and
 * `JSON.parse` the result, at any size. Past V8's ~536MB max string length that
 * throws `RangeError: Invalid string length` before parsing even starts, which
 * made the collection permanently unreadable - `deleteMany` could not shrink it,
 * because shrinking requires reading the current document set first. Large files
 * are now streamed instead, so no single string holds the whole collection.
 *
 * The behavioural half of these tests runs against both targets: documents whose
 * contents are adversarial for a hand-rolled scanner must round-trip the same way
 * on MangoDB and MongoDB. The storage-internals half is MangoDB-only, because it
 * asserts on the JSON file layout MongoDB doesn't have.
 *
 * Set MONGODB_URI environment variable to run against MongoDB.
 * Set MANGODB_TEST_OVERSIZED=1 to additionally run the >536MB end-to-end test
 * (slow, and needs ~1.5GB of free disk).
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ObjectId } from 'bson';
import { MangoClient } from '../../../src/index.ts';
import {
  createTestClient,
  getTestModeName,
  isMongoDBMode,
  type TestClient,
} from '../../test-harness.ts';

// ==================== Dual-target behaviour ====================

describe(`Read Path Streaming Tests (${getTestModeName()})`, () => {
  let client: TestClient;
  let cleanup: () => Promise<void>;
  let dbName: string;

  before(async () => {
    const result = await createTestClient();
    client = result.client;
    cleanup = result.cleanup;
    dbName = result.dbName;
    await client.connect();
  });

  after(async () => {
    await cleanup();
  });

  it('should round-trip strings that look like JSON structure', async () => {
    const collection = client.db(dbName).collection('readpath_structure');
    const docs = [
      { k: 1, s: '}, {"_id": "injected"}, {' },
      { k: 2, s: '[{"a": 1}, {"b": 2}]' },
      { k: 3, s: '{{{ }}} [[[ ]]]' },
      { k: 4, s: 'a,b,c' },
    ];
    await collection.insertMany(docs.map((d) => ({ ...d })));

    const found = await collection.find({}).sort({ k: 1 }).toArray();

    assert.strictEqual(found.length, 4);
    assert.deepStrictEqual(
      found.map((d) => d.s),
      docs.map((d) => d.s)
    );
  });

  it('should round-trip escaped quotes and backslashes', async () => {
    const collection = client.db(dbName).collection('readpath_escapes');
    const docs = [
      { k: 1, s: 'he said "hello"' },
      { k: 2, s: '"' },
      { k: 3, s: '\\' },
      { k: 4, s: '\\"' },
      { k: 5, s: 'C:\\path\\to\\"file"' },
    ];
    await collection.insertMany(docs.map((d) => ({ ...d })));

    const found = await collection.find({}).sort({ k: 1 }).toArray();

    assert.deepStrictEqual(
      found.map((d) => d.s),
      docs.map((d) => d.s)
    );
  });

  it('should round-trip control characters and multi-byte text', async () => {
    const collection = client.db(dbName).collection('readpath_unicode');
    const docs = [
      { k: 1, s: 'line1\nline2\ttabbed\r\n' },
      { k: 2, s: '🥭 mango 日本語 café naïve' },
      { k: 3, s: '\u0000\u001f' },
    ];
    await collection.insertMany(docs.map((d) => ({ ...d })));

    const found = await collection.find({}).sort({ k: 1 }).toArray();

    assert.deepStrictEqual(
      found.map((d) => d.s),
      docs.map((d) => d.s)
    );
  });

  it('should find a document by a filter on an adversarial string', async () => {
    const collection = client.db(dbName).collection('readpath_filter');
    await collection.insertMany([{ s: '}, {' }, { s: 'normal' }]);

    const found = await collection.findOne({ s: '}, {' });

    assert.ok(found);
    assert.strictEqual(found.s, '}, {');
  });

  it('should round-trip a large batch of documents', async () => {
    const collection = client.db(dbName).collection('readpath_large_batch');
    const docs = Array.from({ length: 2000 }, (_, i) => ({
      i,
      pad: 'x'.repeat(200),
      s: `doc ${i} }, {`,
    }));
    await collection.insertMany(docs);

    const count = await collection.countDocuments({});
    const first = await collection.findOne({ i: 0 });
    const last = await collection.findOne({ i: 1999 });

    assert.strictEqual(count, 2000);
    assert.strictEqual(first?.s, 'doc 0 }, {');
    assert.strictEqual(last?.s, 'doc 1999 }, {');
  });

  it('should report an accurate document count from db.stats()', async () => {
    const db = client.db(dbName);
    const collection = db.collection('readpath_stats_count');
    await collection.insertMany(Array.from({ length: 50 }, (_, i) => ({ i, s: '}, {' })));

    const stats = await db.stats();

    assert.ok(
      stats.objects >= 50,
      `expected db.stats().objects to include the 50 inserted documents, got ${stats.objects}`
    );
  });

  it('should reflect deletions in db.stats()', async () => {
    const db = client.db(dbName);
    const collection = db.collection('readpath_stats_delete');
    await collection.insertMany(Array.from({ length: 20 }, (_, i) => ({ i })));

    const before = await db.stats();
    await collection.deleteMany({ i: { $lt: 15 } });
    const after = await db.stats();

    assert.strictEqual(before.objects - after.objects, 15);
  });
});

// ==================== MangoDB storage internals ====================

describe('Read Path Streaming (MangoDB storage internals)', { skip: isMongoDBMode() }, () => {
  let dataDir: string;
  let client: MangoClient;
  const dbName = 'readpath_internals';

  before(async () => {
    dataDir = await mkdtemp(join(tmpdir(), 'mangodb_readpath_'));
    client = new MangoClient(dataDir);
    await client.connect();
    await mkdir(join(dataDir, dbName), { recursive: true });
  });

  after(async () => {
    await client.close();
    await rm(dataDir, { recursive: true, force: true });
  });

  function collectionPath(name: string): string {
    return join(dataDir, dbName, `${name}.json`);
  }

  /** Write a collection file directly, bypassing MangoDB's writer. */
  async function seedFile(name: string, content: string): Promise<void> {
    await writeFile(collectionPath(name), content, 'utf-8');
  }

  it('should read a collection file in the pre-streaming-write layout', async () => {
    // How writeDocuments() serialized before streaming writes (PR #51) landed.
    const docs = [
      { _id: 'a', name: 'Alice', tags: ['x'] },
      { _id: 'b', name: 'Bob', tags: [] },
    ];
    await seedFile('legacy_layout', JSON.stringify(docs, null, 2));

    const found = await client.db(dbName).collection('legacy_layout').find({}).toArray();

    assert.deepStrictEqual(found, docs);
  });

  it('should read a collection file in a compact layout', async () => {
    const docs = [
      { _id: 'a', n: 1 },
      { _id: 'b', n: 2 },
    ];
    await seedFile('compact_layout', JSON.stringify(docs));

    const found = await client.db(dbName).collection('compact_layout').find({}).toArray();

    assert.deepStrictEqual(found, docs);
  });

  it('should read an empty collection file', async () => {
    await seedFile('empty_file', '[]');

    const found = await client.db(dbName).collection('empty_file').find({}).toArray();

    assert.deepStrictEqual(found, []);
  });

  it('should treat a missing collection file as an empty collection', async () => {
    const collection = client.db(dbName).collection('never_written');

    assert.deepStrictEqual(await collection.find({}).toArray(), []);
    assert.strictEqual(await collection.countDocuments({}), 0);
    assert.strictEqual((await collection.stats()).count, 0);
  });

  it('should write a layout it can read back', async () => {
    const collection = client.db(dbName).collection('round_trip');
    await collection.insertMany([{ s: '}, {' }, { s: '"quoted"' }, { s: '🥭' }]);

    const raw = await readFile(collectionPath('round_trip'), 'utf-8');
    const found = await collection.find({}).toArray();

    // The written file is still a plain JSON array, readable by JSON.parse.
    assert.deepStrictEqual(
      JSON.parse(raw).map((d: Record<string, unknown>) => d.s),
      ['}, {', '"quoted"', '🥭']
    );
    assert.deepStrictEqual(
      found.map((d) => d.s),
      ['}, {', '"quoted"', '🥭']
    );
  });

  it('should restore BSON types through the streaming reader', async () => {
    const collection = client.db(dbName).collection('bson_types');
    const id = new ObjectId();
    const when = new Date('2024-06-01T12:34:56.789Z');
    await collection.insertOne({ _id: id, when, nested: { when } });

    const found = await collection.findOne({});

    assert.ok(found);
    assert.ok(found._id instanceof ObjectId);
    assert.strictEqual(found._id.toHexString(), id.toHexString());
    assert.ok(found.when instanceof Date);
    assert.strictEqual((found.when as Date).getTime(), when.getTime());
    assert.ok((found.nested as { when: Date }).when instanceof Date);
  });

  it('should surface a corrupt collection file instead of reporting it empty', async () => {
    await seedFile('corrupt_find', '[{"a": 1}, {"b": 2}');

    await assert.rejects(
      () => client.db(dbName).collection('corrupt_find').find({}).toArray(),
      SyntaxError
    );
  });

  it('should surface a corrupt collection file from db.stats()', async () => {
    // Previously a bare `catch {}` swallowed the parse failure, so a corrupt or
    // oversized collection silently reported 0 documents.
    const isolatedDir = await mkdtemp(join(tmpdir(), 'mangodb_readpath_stats_'));
    const isolatedClient = new MangoClient(isolatedDir);
    await isolatedClient.connect();
    await mkdir(join(isolatedDir, dbName), { recursive: true });
    await writeFile(join(isolatedDir, dbName, 'broken.json'), '[{"a": 1}, {"b":', 'utf-8');

    try {
      await assert.rejects(() => isolatedClient.db(dbName).stats(), SyntaxError);
    } finally {
      await isolatedClient.close();
      await rm(isolatedDir, { recursive: true, force: true });
    }
  });

  it('should count without materializing in collection.stats()', async () => {
    const collection = client.db(dbName).collection('stats_streaming_count');
    await collection.insertMany(Array.from({ length: 250 }, (_, i) => ({ i, s: '}, {' })));

    const stats = await collection.stats();

    assert.strictEqual(stats.count, 250);
    assert.strictEqual(stats.count, await collection.countDocuments({}));
  });

  it('should count documents whose strings contain structural characters', async () => {
    await seedFile('stats_adversarial', '[{"s": "},{"}, {"s": "],["}, {"s": "\\""}]');

    const stats = await client.db(dbName).collection('stats_adversarial').stats();

    assert.strictEqual(stats.count, 3);
  });
});

// ==================== Oversized collection (opt-in) ====================

/**
 * The actual issue #52 regression: a collection file past V8's max string length.
 *
 * Opt in with MANGODB_TEST_OVERSIZED=1. It writes roughly 600MB to a temp dir and
 * takes a couple of minutes, so it stays out of the default suite.
 */
describe(
  'Read Path Streaming (oversized collection)',
  { skip: isMongoDBMode() || process.env.MANGODB_TEST_OVERSIZED !== '1' },
  () => {
    let dataDir: string;
    const dbName = 'oversized';

    before(async () => {
      dataDir = await mkdtemp(join(tmpdir(), 'mangodb_oversized_'));
      await mkdir(join(dataDir, dbName), { recursive: true });
    });

    after(async () => {
      await rm(dataDir, { recursive: true, force: true });
    });

    it('should read and shrink a collection larger than V8 max string length', async () => {
      const filePath = join(dataDir, dbName, 'huge.json');
      const blob = 'x'.repeat(1_000_000);
      const documentCount = 600; // ~600MB, comfortably past the ~536MB ceiling

      await new Promise<void>((resolve, reject) => {
        const stream = createWriteStream(filePath);
        stream.on('error', reject);
        stream.on('finish', resolve);
        stream.write('[\n');
        for (let i = 0; i < documentCount; i++) {
          if (i > 0) stream.write(',\n');
          stream.write(JSON.stringify({ _id: `doc_${i}`, keep: i < 3, blob }));
        }
        stream.write('\n]');
        stream.end();
      });

      // A single string of this file cannot exist, so the old readFile path
      // threw RangeError here and the collection could never recover.
      await assert.rejects(() => readFile(filePath, 'utf-8'), RangeError);

      const client = new MangoClient(dataDir);
      await client.connect();
      try {
        const collection = client.db(dbName).collection('huge');

        assert.strictEqual(await collection.countDocuments({}), documentCount);

        const one = await collection.findOne({ _id: 'doc_1' });
        assert.strictEqual(one?.blob, blob);

        // The recovery path: shrinking the collection back down.
        const result = await collection.deleteMany({ keep: false });
        assert.strictEqual(result.deletedCount, documentCount - 3);
        assert.strictEqual(await collection.countDocuments({}), 3);
      } finally {
        await client.close();
      }
    });
  }
);
