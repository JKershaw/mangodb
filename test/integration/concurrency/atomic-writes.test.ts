import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import {
  createTestClient,
  getTestModeName,
  isMongoDBMode,
  type TestClient,
} from '../../test-harness.ts';

describe(`Atomic Write Tests (${getTestModeName()})`, () => {
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

  it('should not corrupt data with concurrent writes and reads', async () => {
    const collection = client.db(dbName).collection('atomic_stress');

    await collection.insertOne({ counter: 0 });

    const writeCount = 50;
    const readCount = 100;
    const errors: Error[] = [];

    const writes = Array.from({ length: writeCount }, () =>
      collection.updateOne({}, { $inc: { counter: 1 } }).catch((e) => errors.push(e))
    );

    const reads = Array.from({ length: readCount }, () =>
      collection.findOne({}).catch((e) => errors.push(e))
    );

    await Promise.all([...writes, ...reads]);

    // No JSON parse errors should occur
    const jsonErrors = errors.filter(
      (e) => e.message?.includes('JSON') || e.message?.includes('parse')
    );
    assert.strictEqual(jsonErrors.length, 0, `JSON errors: ${jsonErrors.map((e) => e.message)}`);

    // Final state should be consistent
    const doc = await collection.findOne({});
    assert.strictEqual(doc?.counter, writeCount);
  });

  it('should handle aggressive concurrent stress', async function () {
    if (isMongoDBMode()) return; // MangoDB-specific file atomicity test

    const collection = client.db(dbName).collection('atomic_aggressive');

    await collection.insertMany(Array.from({ length: 10 }, (_, i) => ({ idx: i, count: 0 })));

    const promises: Promise<unknown>[] = [];
    for (let i = 0; i < 200; i++) {
      const idx = i % 10;
      promises.push(
        collection.updateOne({ idx }, { $inc: { count: 1 } }),
        collection.findOne({ idx }),
        collection.countDocuments({ idx })
      );
    }

    const results = await Promise.allSettled(promises);
    const jsonFailures = results.filter(
      (r) =>
        r.status === 'rejected' &&
        ((r as PromiseRejectedResult).reason?.message?.includes('JSON') ||
          (r as PromiseRejectedResult).reason?.message?.includes('parse'))
    );

    assert.strictEqual(jsonFailures.length, 0);

    const docs = await collection.find({}).toArray();
    for (const doc of docs) {
      assert.strictEqual(doc.count, 20); // 200 updates / 10 docs
    }
  });
});
