# Mongone Progress

This document tracks implementation progress and notable discoveries.

## Current Status

**Phase**: 8 - Advanced
**Status**: Complete

---

## Changelog

### 2025-12-21 - Phase 8: Advanced Operations

#### Added
- FindOneAnd* methods for atomic find-and-modify operations:
  - `collection.findOneAndDelete(filter, options)` - Find and delete a document
  - `collection.findOneAndReplace(filter, replacement, options)` - Find and replace a document
  - `collection.findOneAndUpdate(filter, update, options)` - Find and update a document
  - `collection.bulkWrite(operations, options)` - Execute multiple write operations

#### Behaviors Implemented
- `findOneAndDelete` returns the deleted document
- `findOneAndReplace` replaces the entire document (preserving `_id`)
- `findOneAndUpdate` applies update operators to the document
- All findOneAnd* methods support:
  - `sort` option - Determines which document to modify when multiple match
  - `projection` option - Controls which fields to return
- `findOneAndReplace` and `findOneAndUpdate` support:
  - `upsert` option - Insert if no match found
  - `returnDocument: "before" | "after"` - Return pre or post modification (default: "before")
- `bulkWrite` supports:
  - `insertOne`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`, `replaceOne` operations
  - `ordered` mode (default: true) - Stop on first error
  - Unordered mode - Continue on errors and collect all failures
  - Aggregated result with counts for all operation types

#### Validation
- `findOneAndReplace` and `replaceOne` reject documents containing update operators (`$` keys)

#### Examples
```typescript
// findOneAndDelete
const result = await collection.findOneAndDelete(
  { status: "pending" },
  { sort: { priority: -1 } }
);
// result.value contains the deleted document

// findOneAndReplace
const result = await collection.findOneAndReplace(
  { name: "Alice" },
  { name: "Alice", age: 31, city: "NYC" },
  { returnDocument: "after" }
);

// findOneAndUpdate
const result = await collection.findOneAndUpdate(
  { name: "Alice" },
  { $inc: { score: 10 } },
  { returnDocument: "after", upsert: true }
);

// bulkWrite
const result = await collection.bulkWrite([
  { insertOne: { document: { name: "Alice" } } },
  { updateOne: { filter: { name: "Alice" }, update: { $set: { age: 30 } } } },
  { deleteOne: { filter: { name: "Bob" } } }
]);
// result.insertedCount, result.modifiedCount, result.deletedCount, etc.
```

---

### 2025-12-21 - Phase 7: Indexes

#### Added
- Index management methods:
  - `collection.createIndex(keySpec, options)` - Create an index
  - `collection.dropIndex(indexNameOrSpec)` - Drop an index by name or key spec
  - `collection.indexes()` - List all indexes (returns array)
  - `collection.listIndexes()` - List all indexes (returns cursor)

- Unique constraint enforcement:
  - Unique indexes prevent duplicate values on `insertOne`/`insertMany`
  - Unique indexes prevent duplicates on `updateOne`/`updateMany`
  - E11000 duplicate key error with MongoDB-compatible format

- Error classes:
  - `MongoDuplicateKeyError` - Error code 11000 for duplicate keys
  - `IndexNotFoundError` - When dropping non-existent index
  - `CannotDropIdIndexError` - When trying to drop `_id` index

#### Behaviors Implemented
- Default `_id_` index always exists (cannot be dropped)
- Index names auto-generated from key spec: `field1_1_field2_-1`
- `createIndex` is idempotent (same spec returns existing name)
- Unique constraint checks on nested fields with dot notation
- Compound unique indexes enforce uniqueness across field combination
- Error messages match MongoDB format: `E11000 duplicate key error collection: db.coll index: name dup key: { field: "value" }`

#### Storage Format
- Index metadata stored in `{collection}.indexes.json` alongside data
- Format: `{ "indexes": [{ v: 2, key: {...}, name: "...", unique?: true }] }`

#### Design Decision
- Indexes are NOT used for query optimization (full scans remain)
- Only API surface and unique constraint enforcement implemented
- This keeps Mongone lightweight for dev/test use cases

#### Examples
```typescript
// Create indexes
await collection.createIndex({ email: 1 }, { unique: true });
await collection.createIndex({ lastName: 1, firstName: 1 });
await collection.createIndex({ createdAt: -1 }, { name: "idx_created" });

// List indexes
const indexes = await collection.indexes();
// [{ v: 2, key: { _id: 1 }, name: "_id_" }, { v: 2, key: { email: 1 }, name: "email_1", unique: true }, ...]

// Drop index
await collection.dropIndex("email_1");
await collection.dropIndex({ lastName: 1, firstName: 1 });

// Unique constraint enforcement
await collection.createIndex({ email: 1 }, { unique: true });
await collection.insertOne({ email: "alice@test.com" });
await collection.insertOne({ email: "alice@test.com" }); // Throws E11000
```

---

### 2025-12-21 - Phase 6: Array Handling

#### Added
- Array query operators:
  - `$size` - Match arrays by exact length
  - `$all` - Match arrays containing all specified elements
  - `$elemMatch` - Match array elements satisfying multiple conditions

- Array update operators:
  - `$push` - Append element(s) to array (supports `$each` modifier)
  - `$pull` - Remove elements matching condition
  - `$addToSet` - Add element if not already present (supports `$each`)
  - `$pop` - Remove first (-1) or last (1) element

#### Behaviors Implemented
- `$size` only matches arrays (not strings or objects)
- `$all` with empty array matches any array (including empty)
- `$elemMatch` requires ALL conditions to be satisfied by the SAME element
- `$push`/`$addToSet` create array if field doesn't exist
- `$push`/`$addToSet` throw error if field exists but is not an array
- `$addToSet` uses BSON-style comparison (key order matters for objects)
- `$pull` supports both exact values and query conditions
- `$pop` is no-op on empty arrays or missing fields

#### Examples
```typescript
// Array query operators
await collection.find({ tags: { $size: 3 } }).toArray();
await collection.find({ tags: { $all: ["red", "blue"] } }).toArray();
await collection.find({
  results: { $elemMatch: { score: { $gte: 80 }, passed: true } }
}).toArray();

// Array update operators
await collection.updateOne({}, { $push: { tags: "new" } });
await collection.updateOne({}, { $push: { tags: { $each: ["a", "b", "c"] } } });
await collection.updateOne({}, { $addToSet: { tags: "unique" } });
await collection.updateOne({}, { $pull: { scores: { $lt: 50 } } });
await collection.updateOne({}, { $pop: { items: 1 } });  // Remove last
await collection.updateOne({}, { $pop: { items: -1 } }); // Remove first
```

---

### 2025-12-20 - Phase 5: Logical Operators

#### Added
- Logical query operators for complex filtering:
  - `$exists` - Check if field exists or not
  - `$and` - Explicit logical AND (all conditions must match)
  - `$or` - Logical OR (any condition must match)
  - `$not` - Negate operator expression
  - `$nor` - Logical NOR (no condition may match)

#### Behaviors Implemented
- `$exists: true` matches documents where field exists (including null values)
- `$exists: false` matches documents where field does not exist
- `$and`, `$or`, `$nor` require nonempty arrays (throws error otherwise)
- `$not` DOES match documents where field is missing (inner condition can't be true)
- Logical operators can be nested and combined
- Multiple logical operators can be used at top level with field conditions
- Field-level use of `$and`/`$or`/`$nor` throws "unknown operator" error

#### Examples
```typescript
// Field existence
await collection.find({ deleted: { $exists: false } }).toArray();

// Explicit AND for same field with different operators
await collection.find({ $and: [{ score: { $gt: 50 } }, { score: { $lt: 100 } }] }).toArray();

// OR with field condition (implicit AND)
await collection.find({ type: "A", $or: [{ status: "active" }, { priority: "high" }] }).toArray();

// NOT operator
await collection.find({ age: { $not: { $gt: 30 } } }).toArray();

// NOR operator
await collection.find({ $nor: [{ deleted: true }, { archived: true }] }).toArray();
```

---

### 2025-12-20 - Phase 4: Cursor Operations

#### Added
- Cursor operations for result manipulation:
  - `cursor.sort(spec)` - Sort by single or multiple fields (compound sort)
  - `cursor.limit(n)` - Limit number of results returned
  - `cursor.skip(n)` - Skip first n results
- Projection support:
  - Field inclusion: `find({}, { projection: { name: 1, age: 1 } })`
  - Field exclusion: `find({}, { projection: { password: 0 } })`
  - `_id` can be excluded with inclusion: `{ name: 1, _id: 0 }`
  - Nested field projection with dot notation
- `countDocuments(filter)` - Count matching documents

#### Behaviors Implemented
- Cursor methods can be chained in any order
- Execution order is always: sort → skip → limit
- Sort handles multiple types with MongoDB's type ordering (null/undefined first)
- Null and missing values sort first when ascending, last when descending
- Projection cannot mix inclusion and exclusion (except _id)
- Nested field projection preserves parent structure

#### Return Values
```typescript
// cursor.toArray() returns projected/sorted/limited documents
// countDocuments() returns number
```

---

### 2025-12-20 - Phase 3: Updates

#### Added
- Update operations for modifying documents:
  - `updateOne(filter, update, options)` - Update a single matching document
  - `updateMany(filter, update, options)` - Update all matching documents
- Update operators:
  - `$set` - Set field values (supports dot notation for nested fields)
  - `$unset` - Remove fields from documents
  - `$inc` - Increment/decrement numeric values
- `upsert` option - Insert document if no match found
- Combining multiple update operators in single update

#### Behaviors Implemented
- Dot notation in `$set` creates nested structures when path doesn't exist
- `$inc` creates field with increment value if field doesn't exist
- `$unset` on non-existent field is a no-op
- `modifiedCount` is 0 when values don't actually change
- Upsert includes filter equality fields in the new document
- Array element updates via index using dot notation (e.g., `"items.0"`)

#### Return Values
```typescript
{
  acknowledged: true,
  matchedCount: number,   // Documents matching filter
  modifiedCount: number,  // Documents actually modified
  upsertedCount: number,  // 0 or 1
  upsertedId: ObjectId | null
}
```

---

### 2024-12-20 - Phase 2: Basic Queries

#### Added
- Query operators for filtering documents:
  - `$eq` - Explicit equality matching
  - `$ne` - Not equal matching
  - `$gt` - Greater than comparison
  - `$gte` - Greater than or equal comparison
  - `$lt` - Less than comparison
  - `$lte` - Less than or equal comparison
  - `$in` - Match any value in array
  - `$nin` - Match none of values in array
- Dot notation for nested field access (`{"a.b.c": value}`)
- Array field matching (any element in array matches query value)
- Date serialization and deserialization for JSON storage
- Range queries by combining operators (`{ value: { $gte: 10, $lte: 20 } }`)

#### Behaviors Implemented
- null matching: `{ field: null }` matches both null values and missing fields
- Array index access via dot notation: `{ "items.0": "value" }`
- Lexicographic string comparison for $gt/$lt operators
- Date comparison using timestamp values

---

### 2024-12-20 - Initial Implementation

#### Added
- Project structure with TypeScript configuration
- `MongoneClient` class with `connect()` and `close()` methods
- `MongoneDb` class with `collection()` method
- `MongoneCollection` class with basic CRUD operations:
  - `insertOne(doc)` - Insert single document
  - `insertMany(docs)` - Insert multiple documents
  - `findOne(filter)` - Find single document
  - `find(filter)` - Find documents, returns cursor
  - `deleteOne(filter)` - Delete single document
  - `deleteMany(filter)` - Delete multiple documents
- `MongoneCursor` class with `toArray()` method
- File-based storage (JSON per collection)
- ObjectId generation using MongoDB's BSON library
- Dual-target test infrastructure
- GitHub Actions CI workflow

#### Storage Format
- Data stored in `{dataDir}/{dbName}/{collectionName}.json`
- Each collection file contains array of documents
- Documents serialized as JSON with ObjectId handling

---

## MongoDB Behaviors Discovered

See [COMPATIBILITY.md](./COMPATIBILITY.md) for detailed documentation of MongoDB behaviors discovered through testing.

### Summary
- ObjectId must be handled specially for JSON serialization
- `insertOne` returns `{ acknowledged: true, insertedId: ObjectId }`
- `insertMany` returns `{ acknowledged: true, insertedIds: { 0: ObjectId, 1: ObjectId, ... } }`
- `deleteOne` returns `{ acknowledged: true, deletedCount: 0 | 1 }`
- `deleteMany` returns `{ acknowledged: true, deletedCount: number }`

---

## Known Limitations

Current implementation has these intentional limitations:

1. **No query optimization** - Indexes are for unique constraints only, queries still scan full collection
2. **Single-threaded** - No concurrent write protection

These will be addressed in future phases as documented in [ROADMAP.md](./ROADMAP.md).
