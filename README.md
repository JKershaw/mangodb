# MangoDB

File-based MongoDB drop-in replacement for TypeScript/Node.js.

**SQLite is to SQL as MangoDB is to MongoDB.**

## Why MangoDB?

Developing with MongoDB typically requires:
- Running a local MongoDB instance
- Setting up Docker containers or cloud databases for CI
- Managing connection strings and database state between test runs

MangoDB lets you develop and test using only the filesystem, then deploy to real MongoDB without code changes. Just swap the connection string.

## Installation

```bash
npm install @jkershaw/mangodb
```

Requires Node.js >= 22.0.0

## Quick Start

```typescript
import { MangoClient } from '@jkershaw/mangodb';

const client = new MangoClient('./data');
await client.connect();

const db = client.db('myapp');
const users = db.collection('users');

// Same API as MongoDB driver
await users.insertOne({ name: 'Alice', email: 'alice@example.com' });
const user = await users.findOne({ name: 'Alice' });

await client.close();
```

## Switching to MongoDB

The API is identical. Only initialization differs:

```typescript
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

// Environment-based switching
const client = process.env.MONGODB_URI
  ? new MongoClient(process.env.MONGODB_URI)
  : new MangoClient('./data');

await client.connect();
// ... rest of your code works unchanged
```

## Documentation

| Document | Description |
|----------|-------------|
| [Compatibility](./docs/COMPATIBILITY.md) | What's supported, what's not |
| [Migration](./docs/MIGRATION.md) | Switching between MangoDB and MongoDB |
| [Testing](./docs/TESTING.md) | Test patterns and best practices |
| [Edge Cases](./docs/EDGE-CASES.md) | Behavioral quirks and gotchas |
| [Examples](./docs/EXAMPLES.md) | Framework integration examples |
| [API Reference](./docs/API.md) | Quick reference for all operations |
| [Troubleshooting](./docs/TROUBLESHOOTING.md) | Common issues and solutions |
| [Initial Prompt](./docs/INITIAL-PROMPT.md) | The original AI prompt that started this project |

## Feature Coverage

| Category | Coverage |
|----------|----------|
| Query Operators | 30/32 (94%) |
| Update Operators | 20/20 (100%) |
| Aggregation Stages | 28/34 (82%) |
| Expression Operators | 106/112 (95%) |
| Index Types | 9/9 (100%) |

See [COMPATIBILITY.md](./docs/COMPATIBILITY.md) for details.

## When to Use MangoDB

**Ideal for:**
- Local development without MongoDB setup
- Unit and integration testing
- CI/CD pipelines (no database service needed)
- Prototyping and learning
- Small datasets (< 10,000 documents)

**Not recommended for:**
- Production deployments
- Large datasets
- Multi-process access
- Applications requiring transactions

## Running Tests

```bash
# Run against MangoDB
npm test

# Run against MongoDB (requires MongoDB instance)
MONGODB_URI=mongodb://localhost:27017 npm test
```

Both modes should pass for full compatibility.

## License

MIT
