# MangoDB Progress

This document tracks implementation progress and notable discoveries.

## Current Status

**Phase**: 11 - Regular Expressions
**Status**: Complete

---

## Changelog

### 2025-12-22 - Phase 11: Regular Expressions

#### Added
- `$regex` query operator for pattern matching on string fields
- `$options` modifier for regex flags (i, m, s)
- JavaScript RegExp support as filter values
- RegExp support in `$in` operator array
- RegExp support in `$not` operator
- RegExp support in `$nin` operator

#### Behaviors Implemented
- `$regex` accepts string patterns or RegExp objects
- `$options` supports `i` (case-insensitive), `m` (multiline), `s` (dotall)
- Non-string fields silently don't match regex (no error)
- `null` and `undefined` values don't match any regex
- Array fields match if ANY string element matches the pattern
- RegExp in `$in` uses JavaScript RegExp objects (not `{ $regex: ... }` syntax)
- `$not` with regex matches documents where field doesn't match pattern
- Missing fields match `$not: { $regex: ... }` (can't match pattern if field doesn't exist)
- `$options` without `$regex` throws error
- Invalid regex patterns throw error

#### Examples
```typescript
// Basic $regex
await collection.find({ name: { $regex: "^A" } }).toArray();

// Case insensitive
await collection.find({ email: { $regex: "gmail\\.com$", $options: "i" } }).toArray();

// JavaScript RegExp literal
await collection.find({ name: /^Alice/i }).toArray();

// Array field matching
await collection.find({ tags: { $regex: "^prod" } }).toArray();

// RegExp in $in
await collection.find({ status: { $in: [/^active/, "pending"] } }).toArray();

// $not with regex
await collection.find({ name: { $not: { $regex: "^Admin" } } }).toArray();

// In aggregation $match
await collection.aggregate([
  { $match: { email: /gmail\.com$/i } }
]).toArray();
```

---

### 2025-12-22 - Phase 10: Aggregation Pipeline Advanced

#### Added
- Expression evaluation framework for computed fields
- Expression operators:
  - Arithmetic: `$add`, `$subtract`, `$multiply`, `$divide`
  - String: `$concat`, `$toUpper`, `$toLower`
  - Conditional: `$cond`, `$ifNull`
  - Comparison: `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`
  - Array: `$size` (expression version)
- New pipeline stages:
  - `$group` - Group documents by key with accumulators
  - `$lookup` - Left outer join with another collection
  - `$addFields` - Add new fields to documents
  - `$set` - Alias for `$addFields`
  - `$replaceRoot` - Replace document with embedded document
  - `$out` - Write pipeline results to a collection
- Accumulator operators for `$group`:
  - `$sum` - Sum numeric values (or count with `$sum: 1`)
  - `$avg` - Calculate average
  - `$min` - Find minimum value
  - `$max` - Find maximum value
  - `$first` - Get first value in group
  - `$last` - Get last value in group
  - `$push` - Collect all values into array
  - `$addToSet` - Collect unique values into array

#### Behaviors Implemented
- Arithmetic operators return `null` if any operand is `null` or missing
- `$concat` returns `null` if any operand is `null` (throws for non-strings)
- `$cond` supports both array syntax `[condition, then, else]` and object syntax `{ if, then, else }`
- `$ifNull` returns first non-null value in array
- `$group` with `_id: null` groups ALL documents into single group
- `$lookup` returns empty array for no matches (left outer join behavior)
- `$replaceRoot` throws error if `newRoot` evaluates to `null`, `undefined`, or non-object
- `$out` must be the final stage in the pipeline (throws otherwise)
- `$out` replaces the entire target collection
- `$out` returns empty array (results are in target collection)
- `$avg` returns `null` when there are no numeric values (not `0`)
- Non-numeric values are ignored by `$sum` and `$avg` accumulators

#### Examples
```typescript
// $group with multiple accumulators
await collection.aggregate([
  { $group: {
    _id: "$category",
    count: { $sum: 1 },
    total: { $sum: "$amount" },
    avgAmount: { $avg: "$amount" },
    items: { $push: "$name" }
  }},
  { $sort: { total: -1 } }
]).toArray();

// $lookup for joining collections
await orders.aggregate([
  { $lookup: {
    from: "products",
    localField: "productId",
    foreignField: "_id",
    as: "product"
  }},
  { $unwind: "$product" }
]).toArray();

// $addFields with expressions
await collection.aggregate([
  { $addFields: {
    total: { $add: ["$price", "$tax"] },
    fullName: { $concat: ["$firstName", " ", "$lastName"] },
    status: { $cond: [{ $gte: ["$score", 60] }, "pass", "fail"] }
  }}
]).toArray();

// $replaceRoot
await collection.aggregate([
  { $replaceRoot: { newRoot: "$address" } }
]).toArray();

// $out to write results
await collection.aggregate([
  { $match: { status: "active" } },
  { $out: "active_users" }
]).toArray();
```

---

### 2025-12-22 - Phase 9: Aggregation Pipeline Basic

#### Added
- Aggregation pipeline framework:
  - `collection.aggregate(pipeline)` - Execute aggregation pipeline
  - `AggregationCursor` class with `toArray()` method
  - Pipeline stage execution framework with sequential processing

- Pipeline stages:
  - `$match` - Filter documents using query syntax (reuses existing query matcher)
  - `$project` - Reshape documents with inclusion, exclusion, field renaming, and `$literal`
  - `$sort` - Order documents by specified fields
  - `$limit` - Limit output to first n documents
  - `$skip` - Skip first n documents
  - `$count` - Count documents and return single document with count
  - `$unwind` - Deconstruct array field into multiple documents

#### Behaviors Implemented
- `$project` cannot mix inclusion (1) and exclusion (0) except for `_id`
- `$project` includes `_id` by default unless explicitly set to 0
- `$project` supports field references with `$fieldName` syntax for renaming
- `$project` supports `$literal` for literal values (prevents `$` interpretation)
- `$count` returns empty array for empty input (not `{ count: 0 }`)
- `$count` field name cannot be empty, start with `$`, or contain `.`
- `$limit` requires positive integer (throws for 0, negative, or non-integer)
- `$skip` requires non-negative integer
- `$unwind` treats non-array values as single-element arrays
- `$unwind` skips documents with null, missing, or empty array fields by default
- `$unwind` supports `preserveNullAndEmptyArrays` option to preserve such documents
- `$unwind` supports `includeArrayIndex` option to add index field
- Unknown pipeline stages throw `Unrecognized pipeline stage name` error

#### Examples
```typescript
// Basic aggregation pipeline
const results = await collection.aggregate([
  { $match: { status: "active" } },
  { $sort: { createdAt: -1 } },
  { $limit: 10 },
  { $project: { name: 1, email: 1, _id: 0 } }
]).toArray();

// $unwind with options
await collection.aggregate([
  { $unwind: { path: "$items", includeArrayIndex: "idx", preserveNullAndEmptyArrays: true } }
]).toArray();

// $count after filtering
await collection.aggregate([
  { $match: { category: "books" } },
  { $count: "totalBooks" }
]).toArray();
// Returns: [{ totalBooks: 42 }] or [] if no matches
```

---

### 2025-12-21 - Phase 8: Advanced Operations

#### Added
- FindOneAnd* methods for atomic find-and-modify operations:
  - `collection.findOneAndDelete(filter, options)` - Find and delete a document
  - `collection.findOneAndReplace(filter, replacement, options)` - Find and replace a document
  - `collection.findOneAndUpdate(filter, update, options)` - Find and update a document
  - `collection.bulkWrite(operations, options)` - Execute multiple write operations

#### Behaviors Implemented
- **Driver 6.0+ API**: All findOneAnd* methods return the document directly (not wrapped in `{ value, ok }`)
- `findOneAndDelete` returns the deleted document or `null`
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
// findOneAndDelete - returns document directly (Driver 6.0+ behavior)
const deleted = await collection.findOneAndDelete(
  { status: "pending" },
  { sort: { priority: -1 } }
);
// deleted is the document or null

// findOneAndReplace
const doc = await collection.findOneAndReplace(
  { name: "Alice" },
  { name: "Alice", age: 31, city: "NYC" },
  { returnDocument: "after" }
);

// findOneAndUpdate
const updated = await collection.findOneAndUpdate(
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
- This keeps MangoDB lightweight for dev/test use cases

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
- `MangoDBClient` class with `connect()` and `close()` methods
- `MangoDBDb` class with `collection()` method
- `MangoDBCollection` class with basic CRUD operations:
  - `insertOne(doc)` - Insert single document
  - `insertMany(docs)` - Insert multiple documents
  - `findOne(filter)` - Find single document
  - `find(filter)` - Find documents, returns cursor
  - `deleteOne(filter)` - Delete single document
  - `deleteMany(filter)` - Delete multiple documents
- `MangoDBCursor` class with `toArray()` method
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
