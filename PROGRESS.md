# Mongone Progress

This document tracks implementation progress and notable discoveries.

## Current Status

**Phase**: 5 - Logical Operators
**Status**: Complete

---

## Changelog

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

1. **No indexing** - All queries scan full collection
2. **No array operators** - No $elemMatch, $size, $all, $push, $pull (Phase 6)
3. **Single-threaded** - No concurrent write protection

These will be addressed in future phases as documented in [ROADMAP.md](./ROADMAP.md).
