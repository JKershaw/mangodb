# Testing Patterns

MangoDB is designed for testing. This guide covers patterns for effective test setup, isolation, and best practices.

## Test Setup

### Basic Setup with Node Test Runner

```typescript
import { describe, it, before, after, beforeEach } from 'node:test';
import assert from 'node:assert';
import { MangoClient } from '@jkershaw/mangodb';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

describe('User tests', () => {
  let client: MangoClient;
  let db;
  let dataDir: string;

  before(async () => {
    // Create isolated temp directory for this test suite
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'test-'));
    client = new MangoClient(dataDir);
    await client.connect();
    db = client.db('testdb');
  });

  after(async () => {
    await client.close();
    // Clean up temp directory
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  beforeEach(async () => {
    // Clear collection before each test
    await db.collection('users').deleteMany({});
  });

  it('should create a user', async () => {
    const users = db.collection('users');
    const result = await users.insertOne({ name: 'Alice', email: 'alice@test.com' });
    assert.ok(result.insertedId);
  });
});
```

### Setup with Jest

```typescript
import { MangoClient } from '@jkershaw/mangodb';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

describe('User tests', () => {
  let client: MangoClient;
  let db;
  let dataDir: string;

  beforeAll(async () => {
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'test-'));
    client = new MangoClient(dataDir);
    await client.connect();
    db = client.db('testdb');
  });

  afterAll(async () => {
    await client.close();
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  beforeEach(async () => {
    await db.collection('users').deleteMany({});
  });

  it('should create a user', async () => {
    const users = db.collection('users');
    const result = await users.insertOne({ name: 'Alice' });
    expect(result.insertedId).toBeDefined();
  });
});
```

### Setup with Vitest

```typescript
import { describe, it, beforeAll, afterAll, beforeEach, expect } from 'vitest';
import { MangoClient } from '@jkershaw/mangodb';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

describe('User tests', () => {
  let client: MangoClient;
  let db;
  let dataDir: string;

  beforeAll(async () => {
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'test-'));
    client = new MangoClient(dataDir);
    await client.connect();
    db = client.db('testdb');
  });

  afterAll(async () => {
    await client.close();
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  beforeEach(async () => {
    await db.collection('users').deleteMany({});
  });

  it('should create a user', async () => {
    const result = await db.collection('users').insertOne({ name: 'Alice' });
    expect(result.insertedId).toBeDefined();
  });
});
```

## Test Isolation

### Per-Test Isolation

Each test gets a clean state:

```typescript
beforeEach(async () => {
  // Option 1: Delete all documents
  await db.collection('users').deleteMany({});

  // Option 2: Drop and recreate collection (also removes indexes)
  await db.collection('users').drop().catch(() => {});
});
```

### Per-Suite Isolation

Each test file gets its own database:

```typescript
// In a shared test helper
export function createTestDb() {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'test-'));
  const client = new MangoClient(dataDir);

  return {
    client,
    async setup() {
      await client.connect();
      return client.db('test');
    },
    async teardown() {
      await client.close();
      fs.rmSync(dataDir, { recursive: true, force: true });
    }
  };
}

// In test file
const testDb = createTestDb();
let db;

before(async () => { db = await testDb.setup(); });
after(async () => { await testDb.teardown(); });
```

### Parallel Test Isolation

For parallel test execution, use unique directories:

```typescript
import { randomUUID } from 'node:crypto';

function createIsolatedClient() {
  const uniqueDir = path.join(os.tmpdir(), `test-${randomUUID()}`);
  fs.mkdirSync(uniqueDir, { recursive: true });
  return {
    client: new MangoClient(uniqueDir),
    cleanup: () => fs.rmSync(uniqueDir, { recursive: true, force: true })
  };
}
```

## Seeding Test Data

### Fixture Files

```typescript
// fixtures/users.json
[
  { "name": "Alice", "role": "admin" },
  { "name": "Bob", "role": "user" },
  { "name": "Charlie", "role": "user" }
]
```

```typescript
// test-helper.ts
import fixtures from './fixtures/users.json';

async function seedUsers(db) {
  await db.collection('users').insertMany(fixtures);
}
```

### Factory Functions

```typescript
function createUser(overrides = {}) {
  return {
    name: 'Test User',
    email: `test-${Date.now()}@example.com`,
    role: 'user',
    createdAt: new Date(),
    ...overrides
  };
}

// Usage
it('should find admin users', async () => {
  await users.insertMany([
    createUser({ role: 'admin' }),
    createUser({ role: 'user' }),
    createUser({ role: 'admin' })
  ]);

  const admins = await users.find({ role: 'admin' }).toArray();
  assert.strictEqual(admins.length, 2);
});
```

### Builder Pattern

```typescript
class UserBuilder {
  private data = {
    name: 'Test User',
    email: 'test@example.com',
    role: 'user'
  };

  withName(name: string) {
    this.data.name = name;
    return this;
  }

  withRole(role: string) {
    this.data.role = role;
    return this;
  }

  build() {
    return { ...this.data, email: `${this.data.name.toLowerCase()}@example.com` };
  }
}

// Usage
const admin = new UserBuilder().withName('Alice').withRole('admin').build();
```

## Dual-Target Testing

Test against both MangoDB and real MongoDB to ensure compatibility:

```typescript
// test-harness.ts
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

export type TestClient = MongoClient | MangoClient;

export async function createTestClient(): Promise<{
  client: TestClient;
  cleanup: () => Promise<void>;
}> {
  if (process.env.MONGODB_URI) {
    // Test against real MongoDB
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    return {
      client,
      cleanup: async () => {
        // Clean up test database
        await client.db('test').dropDatabase();
        await client.close();
      }
    };
  }

  // Test against MangoDB
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'test-'));
  const client = new MangoClient(dataDir);
  await client.connect();
  return {
    client,
    cleanup: async () => {
      await client.close();
      fs.rmSync(dataDir, { recursive: true, force: true });
    }
  };
}

export function getTestModeName(): string {
  return process.env.MONGODB_URI ? 'MongoDB' : 'MangoDB';
}
```

```typescript
// users.test.ts
import { createTestClient, getTestModeName } from './test-harness';

describe(`Users (${getTestModeName()})`, () => {
  let client;
  let cleanup;
  let db;

  before(async () => {
    ({ client, cleanup } = await createTestClient());
    db = client.db('test');
  });

  after(async () => {
    await cleanup();
  });

  it('should work on both implementations', async () => {
    // This test runs against both MangoDB and MongoDB
    const result = await db.collection('users').insertOne({ name: 'Test' });
    assert.ok(result.insertedId);
  });
});
```

Run tests:
```bash
# Test with MangoDB
npm test

# Test with MongoDB
MONGODB_URI=mongodb://localhost:27017 npm test
```

## Testing Aggregation Pipelines

```typescript
describe('Aggregation', () => {
  beforeEach(async () => {
    await db.collection('orders').insertMany([
      { product: 'A', quantity: 10, price: 5 },
      { product: 'B', quantity: 5, price: 10 },
      { product: 'A', quantity: 3, price: 5 },
    ]);
  });

  it('should calculate totals by product', async () => {
    const results = await db.collection('orders').aggregate([
      { $group: {
        _id: '$product',
        totalQuantity: { $sum: '$quantity' },
        totalRevenue: { $sum: { $multiply: ['$quantity', '$price'] } }
      }},
      { $sort: { _id: 1 } }
    ]).toArray();

    assert.deepStrictEqual(results, [
      { _id: 'A', totalQuantity: 13, totalRevenue: 65 },
      { _id: 'B', totalQuantity: 5, totalRevenue: 50 }
    ]);
  });
});
```

## Testing Error Conditions

```typescript
describe('Error handling', () => {
  it('should reject duplicate keys', async () => {
    await db.collection('users').createIndex({ email: 1 }, { unique: true });
    await db.collection('users').insertOne({ email: 'test@example.com' });

    await assert.rejects(
      db.collection('users').insertOne({ email: 'test@example.com' }),
      (err) => {
        assert.strictEqual(err.code, 11000);
        return true;
      }
    );
  });

  it('should handle missing documents', async () => {
    const result = await db.collection('users').findOne({ _id: 'nonexistent' });
    assert.strictEqual(result, null);
  });
});
```

## Performance Considerations

MangoDB loads all documents into memory for each operation. Keep test datasets small:

```typescript
// Good - small focused dataset
beforeEach(async () => {
  await collection.insertMany(
    Array.from({ length: 10 }, (_, i) => ({ index: i }))
  );
});

// Avoid - large datasets slow tests
beforeEach(async () => {
  await collection.insertMany(
    Array.from({ length: 10000 }, (_, i) => ({ index: i }))
  );
});
```

## CI Configuration

### GitHub Actions

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - run: npm ci
      - run: npm test  # Runs against MangoDB, no MongoDB needed
```

### GitLab CI

```yaml
test:
  image: node:22
  script:
    - npm ci
    - npm test  # No MongoDB service needed
```

### CircleCI

```yaml
version: 2.1
jobs:
  test:
    docker:
      - image: cimg/node:22.0
    steps:
      - checkout
      - run: npm ci
      - run: npm test  # No MongoDB needed
```

## Debugging Tests

### Inspect Data Directory

```typescript
it('debug: inspect stored data', async () => {
  await collection.insertOne({ name: 'Debug' });

  // Read the raw JSON file
  const dataPath = path.join(dataDir, 'testdb', 'collection.json');
  const raw = fs.readFileSync(dataPath, 'utf-8');
  console.log('Stored data:', raw);
});
```

### Log Queries

```typescript
// Wrap collection for debugging
function debugCollection(collection) {
  return new Proxy(collection, {
    get(target, prop) {
      const original = target[prop];
      if (typeof original === 'function') {
        return (...args) => {
          console.log(`${prop}(${JSON.stringify(args)})`);
          return original.apply(target, args);
        };
      }
      return original;
    }
  });
}

const users = debugCollection(db.collection('users'));
```
