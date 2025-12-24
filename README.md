# MangoDB 🥭 

File-based MongoDB drop-in replacement for TypeScript/Node.js. **SQLite is to SQL as MangoDB is to MongoDB.**

## What Problem Does This Solve?

Developing with MongoDB often means:
- Running a local MongoDB instance during development
- Setting up Docker containers or cloud databases for CI
- Managing connection strings and database state between test runs

MangoDB lets you develop and test locally using only the filesystem, then deploy to a real MongoDB server without changing your code. Just swap the connection string.

## Status

**Active Development** - Core MongoDB API compatibility is well implemented with 993+ passing tests.

See [ROADMAP.md](./docs/ROADMAP.md) for implementation phases and [LIMITATIONS.md](./docs/LIMITATIONS.md) for current feature coverage.

## Documentation

- [LIMITATIONS.md](./docs/LIMITATIONS.md) - What MongoDB features are not supported
- [COMPATIBILITY.md](./COMPATIBILITY.md) - MongoDB behavior documentation
- [FUTURE_WORK.md](./docs/FUTURE_WORK.md) - Planned features

## Installation

```bash
npm install @jkershaw/mangodb
```

## Usage

### With MangoDB (local development/testing)

```typescript
import { MangoClient } from '@jkershaw/mangodb';

const client = new MangoClient('./data');
await client.connect();

const db = client.db('myapp');
const users = db.collection('users');

await users.insertOne({ name: 'Alice', email: 'alice@example.com' });
const user = await users.findOne({ name: 'Alice' });

await client.close();
```

### With MongoDB (production)

```typescript
import { MongoClient } from 'mongodb';

const client = new MongoClient(process.env.MONGODB_URI);
await client.connect();

const db = client.db('myapp');
const users = db.collection('users');

await users.insertOne({ name: 'Alice', email: 'alice@example.com' });
const user = await users.findOne({ name: 'Alice' });

await client.close();
```

### Environment-Based Switching

```typescript
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

const client = process.env.MONGODB_URI
  ? new MongoClient(process.env.MONGODB_URI)
  : new MangoClient('./data');

await client.connect();
// ... rest of your code works identically
```

## Running Tests

Tests run against both real MongoDB and MangoDB to ensure compatibility.

```bash
# Run tests against MangoDB only
npm test

# Run tests against MongoDB (requires MongoDB instance)
MONGODB_URI=mongodb://localhost:27017 npm test
```

For CI, both modes must pass.

## Requirements

- Node.js >= 22.0.0
- TypeScript >= 5.0
- MongoDB instance for running compatibility tests

## Contributing

1. Write failing tests first using real MongoDB
2. Implement minimum code to pass tests in MangoDB
3. Run tests against both targets
4. Document any discovered MongoDB behaviors in [COMPATIBILITY.md](./COMPATIBILITY.md)

## License

MIT
