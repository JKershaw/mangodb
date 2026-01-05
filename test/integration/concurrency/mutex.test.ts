import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from '../../test-harness.ts';

describe(`Concurrency Tests (${getTestModeName()})`, () => {
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

  it('should handle concurrent insertOne without data loss', async () => {
    const collection = client.db(dbName).collection('concurrent_insert');
    const n = 100;

    const promises = Array.from({ length: n }, (_, i) =>
      collection.insertOne({ index: i })
    );

    await Promise.all(promises);

    const count = await collection.countDocuments();
    assert.strictEqual(count, n, `Expected ${n} documents, got ${count}`);
  });

  it('should handle concurrent $inc updates correctly', async () => {
    const collection = client.db(dbName).collection('concurrent_update');
    await collection.insertOne({ counter: 0 });

    const n = 50;
    const promises = Array.from({ length: n }, () =>
      collection.updateOne({}, { $inc: { counter: 1 } })
    );

    await Promise.all(promises);

    const doc = await collection.findOne({});
    assert.strictEqual(doc?.counter, n);
  });

  it('should handle mixed concurrent operations', async () => {
    const collection = client.db(dbName).collection('concurrent_mixed');
    await collection.insertMany([{ type: 'a' }, { type: 'b' }]);

    const promises = [
      collection.insertOne({ type: 'c' }),
      collection.updateOne({ type: 'a' }, { $set: { updated: true } }),
      collection.deleteOne({ type: 'b' }),
      collection.insertOne({ type: 'd' }),
    ];

    await Promise.all(promises);

    const docs = await collection.find({}).toArray();
    assert.strictEqual(docs.length, 3); // 2 + 2 inserts - 1 delete
  });
});
