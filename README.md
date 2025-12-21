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

### Phase 1: Foundation (Complete)
- [x] Client and database abstractions
- [x] Collection access
- [x] `insertOne`, `insertMany`
- [x] `findOne`, `find` with empty filter
- [x] `deleteOne`, `deleteMany` with simple equality

### Phase 2: Basic Queries (Complete)
- [x] Equality matching and dot notation for nested fields
- [x] Comparison operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
- [x] Array operators: `$in`, `$nin`
- [x] Array field matching (any element match)
- [x] Date serialization and comparison

### Phase 3: Updates (Complete)
- [x] `updateOne` and `updateMany` operations
- [x] `$set` - Set field values (with dot notation for nested fields)
- [x] `$unset` - Remove fields
- [x] `$inc` - Increment numeric values
- [x] `upsert` option - Insert if not found

### Phase 4: Cursor Operations (Complete)
- [x] `cursor.sort(spec)` - Single and compound field sorting
- [x] `cursor.limit(n)` - Limit result count
- [x] `cursor.skip(n)` - Skip first n results
- [x] Projection (field inclusion and exclusion)
- [x] `collection.countDocuments(filter)` - Count matching documents

### Phase 5: Logical Operators (Complete)
- [x] `$exists` - Field existence check
- [x] `$and` - Explicit logical AND
- [x] `$or` - Logical OR
- [x] `$not` - Operator negation
- [x] `$nor` - Logical NOR

### Phase 6: Array Handling (Complete)
- [x] `$size` - Match arrays by exact length
- [x] `$all` - Match arrays containing all specified elements
- [x] `$elemMatch` - Match array elements satisfying conditions
- [x] `$push` - Append element(s) to array (with `$each`)
- [x] `$pull` - Remove elements matching condition
- [x] `$addToSet` - Add unique elements (with `$each`)
- [x] `$pop` - Remove first or last element

### Phase 7: Indexes (Complete)
- [x] `collection.createIndex(keySpec, options)` - Create index
- [x] `collection.dropIndex(nameOrSpec)` - Drop index
- [x] `collection.indexes()` / `listIndexes()` - List indexes
- [x] Unique constraint enforcement on insert/update
- [x] E11000 duplicate key errors matching MongoDB format

### Phase 8: Advanced Operations (Complete)
- [x] `collection.findOneAndDelete(filter, options)` - Find and delete
- [x] `collection.findOneAndReplace(filter, replacement, options)` - Find and replace
- [x] `collection.findOneAndUpdate(filter, update, options)` - Find and update
- [x] `collection.bulkWrite(operations, options)` - Bulk write operations

### Planned
- Basic aggregation pipeline

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
