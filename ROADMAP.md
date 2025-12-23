# MangoDB Roadmap

This document outlines the implementation phases for MangoDB. Each phase builds on the previous and includes specific MongoDB operations to implement.

## Current Phase: Phase 13 - Additional Update Operators (Next)

See [ROADMAP_REMAINING.md](./ROADMAP_REMAINING.md) for detailed plans on remaining phases (13-16).

---

## Phase 1: Foundation (Complete)

**Goal**: Establish core abstractions and basic CRUD operations.

### Operations
- [x] `MangoDBClient` - Client abstraction matching MongoClient interface
- [x] `client.connect()` / `client.close()`
- [x] `client.db(name)` - Database access
- [x] `db.collection(name)` - Collection access
- [x] `collection.insertOne(doc)`
- [x] `collection.insertMany(docs)`
- [x] `collection.findOne(filter)` - with empty filter
- [x] `collection.find(filter)` - with empty filter, returns cursor
- [x] `cursor.toArray()`
- [x] `collection.deleteOne(filter)` - simple equality
- [x] `collection.deleteMany(filter)` - simple equality

### Storage
- JSON file per collection
- Simple file-based persistence
- Data directory structure: `{dataDir}/{dbName}/{collectionName}.json`

### Infrastructure
- [x] Dual-target test harness
- [x] GitHub Actions CI

---

## Phase 2: Basic Queries (Complete)

**Goal**: Support common query operators for filtering documents.

### Operations
- [x] Equality matching (`{field: value}`)
- [x] Dot notation for nested fields (`{"a.b.c": value}`)
- [x] `$eq` - Explicit equality
- [x] `$ne` - Not equal
- [x] `$gt` - Greater than
- [x] `$gte` - Greater than or equal
- [x] `$lt` - Less than
- [x] `$lte` - Less than or equal
- [x] `$in` - Match any value in array
- [x] `$nin` - Match none of values in array
- [x] Array field matching (any element match)
- [x] Date serialization and comparison

### Considerations (Resolved)
- No type coercion - exact type matching
- null matches both null values and missing fields
- Date comparison uses getTime() for accurate comparisons

---

## Phase 3: Updates (Complete)

**Goal**: Support document updates with common operators.

### Operations
- [x] `collection.updateOne(filter, update)`
- [x] `collection.updateMany(filter, update)`
- [x] `$set` - Set field values
- [x] `$unset` - Remove fields
- [x] `$inc` - Increment numeric values
- [x] `upsert` option - Insert if not found

### Considerations (Resolved)
- Dot notation in updates creates nested structure
- Update operators can be combined
- Return values (matchedCount, modifiedCount, upsertedId)

---

## Phase 4: Cursor Operations (Complete)

**Goal**: Support result manipulation and projection.

### Operations
- [x] `cursor.sort(spec)` - Single field
- [x] `cursor.sort(spec)` - Compound/multiple fields
- [x] `cursor.limit(n)`
- [x] `cursor.skip(n)`
- [x] Projection (field inclusion)
- [x] Projection (field exclusion)
- [x] `collection.countDocuments(filter)`

### Considerations (Resolved)
- Sort order for mixed types follows MongoDB type ordering
- Chaining cursor methods (order doesn't matter - always sort, skip, limit)
- Projection cannot mix inclusion/exclusion (except _id)

---

## Phase 5: Logical Operators (Complete)

**Goal**: Support complex query logic.

### Operations
- [x] `$exists` - Field existence check
- [x] `$and` - Logical AND (explicit)
- [x] `$or` - Logical OR
- [x] `$not` - Logical NOT (operator negation)
- [x] `$nor` - Logical NOR

### Implementation Plan

#### Step 1: `$exists` Operator
Add to `QueryOperators` interface and `matchesOperators()` method.

**Syntax**: `{ field: { $exists: boolean } }`

**Behavior**:
- `{ $exists: true }` - matches documents where field exists (including null values)
- `{ $exists: false }` - matches documents where field does not exist
- Works with dot notation paths

**Test Cases**:
```typescript
// Field exists
await collection.find({ name: { $exists: true } }).toArray();
// Field does not exist
await collection.find({ deleted: { $exists: false } }).toArray();
// Nested field with dot notation
await collection.find({ "user.email": { $exists: true } }).toArray();
// Exists with null value (should match $exists: true)
await collection.find({ value: { $exists: true } }).toArray(); // matches { value: null }
```

#### Step 2: `$and` Operator
Add top-level logical operator handling to `matchesFilter()`.

**Syntax**: `{ $and: [ { condition1 }, { condition2 }, ... ] }`

**Behavior**:
- All conditions in the array must match
- Can combine with field conditions: `{ name: "Alice", $and: [...] }`
- Explicit $and useful when same field appears multiple times

**Test Cases**:
```typescript
// Basic $and
await collection.find({ $and: [{ age: { $gte: 18 } }, { age: { $lte: 65 } }] }).toArray();
// $and with other field conditions
await collection.find({ status: "active", $and: [{ score: { $gt: 50 } }] }).toArray();
// Nested $and with $or
await collection.find({ $and: [{ $or: [{ a: 1 }, { b: 2 }] }, { c: 3 }] }).toArray();
```

#### Step 3: `$or` Operator
Add top-level logical operator handling to `matchesFilter()`.

**Syntax**: `{ $or: [ { condition1 }, { condition2 }, ... ] }`

**Behavior**:
- At least one condition in the array must match
- Can combine with field conditions (implicit AND with other fields)
- Empty array matches nothing

**Test Cases**:
```typescript
// Basic $or
await collection.find({ $or: [{ status: "active" }, { priority: "high" }] }).toArray();
// $or with other field conditions
await collection.find({ type: "task", $or: [{ urgent: true }, { dueDate: { $lt: tomorrow } }] }).toArray();
// $or with comparison operators
await collection.find({ $or: [{ value: { $lt: 10 } }, { value: { $gt: 100 } }] }).toArray();
```

#### Step 4: `$not` Operator
Add to `QueryOperators` interface - wraps another operator expression.

**Syntax**: `{ field: { $not: { operator: value } } }`

**Behavior**:
- Inverts the result of the wrapped operator expression
- Does NOT match if field is missing (differs from $ne)
- Can wrap any comparison operator

**Test Cases**:
```typescript
// $not with $gt
await collection.find({ age: { $not: { $gt: 30 } } }).toArray();
// $not with $in
await collection.find({ status: { $not: { $in: ["deleted", "archived"] } } }).toArray();
// $not with regex (if regex supported)
// Note: Missing fields do NOT match $not conditions
```

#### Step 5: `$nor` Operator
Add top-level logical operator handling to `matchesFilter()`.

**Syntax**: `{ $nor: [ { condition1 }, { condition2 }, ... ] }`

**Behavior**:
- No condition in the array may match (opposite of $or)
- Equivalent to `{ $not: { $or: [...] } }` but at top level
- Also matches documents missing the queried fields

**Test Cases**:
```typescript
// Basic $nor
await collection.find({ $nor: [{ status: "deleted" }, { status: "archived" }] }).toArray();
// $nor with other field conditions
await collection.find({ active: true, $nor: [{ error: true }, { suspended: true }] }).toArray();
```

### Code Changes Required

1. **`src/collection.ts`**:
   - Extend `QueryOperators` interface to add `$exists` and `$not`
   - Create `LogicalOperators` type for `$and`, `$or`, `$nor`
   - Modify `Filter` type to include logical operators
   - Update `matchesFilter()` to handle top-level logical operators first
   - Update `matchesOperators()` to handle `$exists` and `$not`

2. **`test/logical.test.ts`** (new file):
   - Test suite for all logical operators
   - Edge cases: empty arrays, missing fields, nested operators
   - Combination tests with existing comparison operators

### Considerations
- **Implicit AND**: Already implemented - `{ a: 1, b: 2 }` requires both to match
- **Operator precedence**: Logical operators evaluated within their scope
- **Recursion**: `$and`, `$or`, `$nor` recursively call `matchesFilter()`
- **$not vs $ne**: `$not` doesn't match missing fields; `$ne` does
- **Empty logical arrays**:
  - `$and: []` - matches all documents (vacuous truth)
  - `$or: []` - matches no documents
  - `$nor: []` - matches all documents

---

## Phase 6: Array Handling (Complete)

**Goal**: Support querying and modifying array fields.

### Query Operations
- [x] Array element matching (`{tags: "red"}` matches `{tags: ["red", "blue"]}`) - Already working from Phase 2
- [x] `$elemMatch` - Match array element with multiple conditions
- [x] `$size` - Match array by length
- [x] `$all` - Match arrays containing all specified elements

### Update Operations
- [x] `$push` - Add element to array (with `$each` modifier)
- [x] `$pull` - Remove elements matching condition
- [x] `$addToSet` - Add element if not present (with `$each` modifier)
- [x] `$pop` - Remove first or last element

### Considerations (Resolved)
- `$elemMatch` ensures all conditions met by SAME array element
- `$size` only works with exact numbers (no range queries)
- `$addToSet` uses BSON-style object comparison (key order matters)
- Array update operators throw errors for non-array fields

---

## Phase 7: Indexes (Complete)

**Goal**: Support index management and unique constraints.

### Operations
- [x] `collection.createIndex(keySpec, options)` - Create index with optional unique/name
- [x] `collection.dropIndex(nameOrSpec)` - Drop index by name or key spec
- [x] `collection.indexes()` - List all indexes (returns array)
- [x] `collection.listIndexes()` - List all indexes (returns cursor)
- [x] Unique index constraint enforcement

### Implementation
- Index metadata stored in `{collection}.indexes.json`
- Default `_id_` index always exists
- Unique constraints enforced on insert/update operations
- E11000 duplicate key errors match MongoDB format

### Design Decision
- Indexes are NOT used for query optimization (full scans remain)
- Only API surface and unique constraint enforcement implemented
- This keeps MangoDB lightweight for dev/test use cases

---

## Phase 8: Advanced (Complete)

**Goal**: Support additional commonly-used operations.

### Operations
- [x] `collection.findOneAndUpdate(filter, update, options)`
- [x] `collection.findOneAndDelete(filter, options)`
- [x] `collection.findOneAndReplace(filter, replacement, options)`
- [x] `collection.bulkWrite(operations)`

### Basic Aggregation Pipeline
See Phase 9 below for complete aggregation pipeline implementation.

---

## Phase 9: Aggregation Pipeline Basic (Complete)

**Goal**: Implement the aggregation framework with commonly-used stages.

### Operations
- [x] `collection.aggregate(pipeline)` - Execute aggregation pipeline
- [x] `AggregationCursor` with `toArray()` method
- [x] `$match` stage - Filter documents (reuses query matcher)
- [x] `$project` stage - Reshape documents (inclusion/exclusion/renaming)
- [x] `$sort` stage - Order documents
- [x] `$limit` stage - Limit output documents
- [x] `$skip` stage - Skip first n documents
- [x] `$count` stage - Count documents and return single result
- [x] `$unwind` stage - Deconstruct arrays into multiple documents

### Implementation Details
- `$project` supports field inclusion, exclusion, renaming (`$field`), and `$literal`
- `$count` returns empty array for empty input (not `{ count: 0 }`)
- `$unwind` supports `preserveNullAndEmptyArrays` and `includeArrayIndex` options
- Non-array values are treated as single-element arrays in `$unwind`
- Pipeline stages execute sequentially, each transforming the document stream

---

## Design Principles

1. **Test-Driven**: Write tests against real MongoDB first, then implement in MangoDB
2. **Correctness Over Performance**: Get it right before making it fast
3. **Minimal API Surface**: Only implement what's tested and needed
4. **Document Discoveries**: Note unexpected MongoDB behaviors in COMPATIBILITY.md
