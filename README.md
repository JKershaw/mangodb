# Mongone

File-based MongoDB drop-in replacement for TypeScript/Node.js. **SQLite is to SQL as Mongone is to MongoDB.**

## What Problem Does This Solve?

Developing with MongoDB often means:
- Running a local MongoDB instance during development
- Setting up Docker containers or cloud databases for CI
- Managing connection strings and database state between test runs

Mongone lets you develop and test locally using only the filesystem, then deploy to a real MongoDB server without changing your code. Just swap the connection string.

## Status

**Early Development** - This project is in active development. Not yet suitable for production use.

See [ROADMAP.md](./ROADMAP.md) for implementation phases and [PROGRESS.md](./PROGRESS.md) for current status.

## Usage

### With Mongone (local development/testing)

```typescript
import { MongoneClient } from 'mongone';

const client = new MongoneClient('./data');
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
import { MongoneClient } from 'mongone';

const client = process.env.MONGODB_URI
  ? new MongoClient(process.env.MONGODB_URI)
  : new MongoneClient('./data');

await client.connect();
// ... rest of your code works identically
```

## What's Implemented

See [PROGRESS.md](./PROGRESS.md) for detailed status.

### Phase 1: Foundation (Current)
- [x] Client and database abstractions
- [x] Collection access
- [x] `insertOne`, `insertMany`
- [x] `findOne`, `find` with empty filter
- [x] `deleteOne`, `deleteMany` with simple equality

### Planned
- Basic queries ($eq, $gt, $lt, $in, etc.)
- Update operations ($set, $unset, $inc)
- Cursor operations (sort, limit, skip)
- Logical operators ($and, $or, $not)
- Array handling ($elemMatch, $push, $pull)
- Indexes

## Running Tests

Tests run against both real MongoDB and Mongone to ensure compatibility.

```bash
# Run tests against Mongone only
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
2. Implement minimum code to pass tests in Mongone
3. Run tests against both targets
4. Document any discovered MongoDB behaviors in [COMPATIBILITY.md](./COMPATIBILITY.md)

## License

MIT
