# Phase 7: Indexes - Implementation Plan

This document outlines the detailed implementation plan for Phase 7 of Mongone, following TDD principles. All tests will run against both MongoDB and Mongone to ensure behavioral compatibility.

## Overview

Phase 7 adds index management and unique constraint enforcement to Mongone. Per design decision, we implement the **API surface and unique constraints only** - indexes are not used for query optimization (full scans remain acceptable for dev/test datasets).

### Operations
1. `collection.createIndex(keySpec, options)` - Create an index
2. `collection.dropIndex(indexNameOrSpec)` - Drop an index
3. `collection.indexes()` - List all indexes (alias for `listIndexes().toArray()`)
4. `collection.listIndexes()` - Return cursor over indexes
5. Unique constraint enforcement on insert/update

### Out of Scope (Intentionally)
- Using indexes for query optimization
- Compound index key ordering logic
- Sparse indexes (we store metadata but don't enforce sparse semantics)
- TTL indexes
- Text/geospatial indexes

---

## Storage Format

### Index Metadata File
```
{dataDir}/{db}/{collection}.indexes.json
```

### Format
```json
{
  "indexes": [
    {
      "v": 2,
      "key": { "_id": 1 },
      "name": "_id_"
    },
    {
      "v": 2,
      "key": { "email": 1 },
      "name": "email_1",
      "unique": true
    }
  ]
}
```

### Default `_id` Index
- Every collection implicitly has `{ key: { _id: 1 }, name: "_id_" }` index
- This index is always unique (enforced by ObjectId generation)
- Cannot be dropped

---

## Step 1: `createIndex()` Method

### Syntax
```typescript
collection.createIndex(
  keySpec: { [field: string]: 1 | -1 },
  options?: { unique?: boolean; name?: string; sparse?: boolean }
): Promise<string>  // Returns index name
```

### Behavior (from MongoDB docs)
- Returns the index name as a string
- If no `name` option provided, generates name from key spec: `field1_1_field2_-1`
- If index with same key spec already exists, returns existing name (no-op)
- If index with same name but different spec exists, throws error

### Test Cases

#### Basic Tests
```typescript
// Test 1: Create simple ascending index
const name = await collection.createIndex({ email: 1 });
// Returns: "email_1"

// Test 2: Create descending index
const name = await collection.createIndex({ createdAt: -1 });
// Returns: "createdAt_-1"

// Test 3: Create compound index
const name = await collection.createIndex({ lastName: 1, firstName: 1 });
// Returns: "lastName_1_firstName_1"

// Test 4: Create index with custom name
const name = await collection.createIndex({ email: 1 }, { name: "idx_email" });
// Returns: "idx_email"

// Test 5: Create unique index
const name = await collection.createIndex({ email: 1 }, { unique: true });
// Returns: "email_1"
// Index metadata includes unique: true
```

#### Idempotency Tests
```typescript
// Test 6: Creating same index twice returns same name (no-op)
await collection.createIndex({ email: 1 });
const name = await collection.createIndex({ email: 1 });
// Returns: "email_1" (no error, no duplicate)

// Test 7: Same key spec with same options is idempotent
await collection.createIndex({ email: 1 }, { unique: true });
await collection.createIndex({ email: 1 }, { unique: true });
// No error

// Test 8: Same key spec with different unique option
await collection.createIndex({ email: 1 }, { unique: true });
await collection.createIndex({ email: 1 }, { unique: false });
// MongoDB: No error, keeps original (does not change options)
// We match this behavior
```

#### Edge Cases
```typescript
// Test 9: Create index on nested field
const name = await collection.createIndex({ "address.city": 1 });
// Returns: "address.city_1"

// Test 10: Index with dots and underscores in field name
const name = await collection.createIndex({ "user_name": 1, "data.value": -1 });
// Returns: "user_name_1_data.value_-1"
```

### Implementation Notes
- Store index metadata in `{collection}.indexes.json`
- Generate index name: join `${field}_${direction}` with underscores
- Check for existing index with same key spec before creating
- Write updated indexes array to file

---

## Step 2: `dropIndex()` Method

### Syntax
```typescript
collection.dropIndex(indexNameOrSpec: string | { [field: string]: 1 | -1 }): Promise<void>
```

### Behavior (from MongoDB docs)
- Accepts index name (string) or key specification (object)
- Throws error if index not found
- Cannot drop the `_id_` index

### Test Cases

#### Basic Tests
```typescript
// Test 1: Drop index by name
await collection.createIndex({ email: 1 });
await collection.dropIndex("email_1");
// Index removed

// Test 2: Drop index by key spec
await collection.createIndex({ email: 1 });
await collection.dropIndex({ email: 1 });
// Index removed

// Test 3: Drop compound index
await collection.createIndex({ a: 1, b: -1 });
await collection.dropIndex("a_1_b_-1");
// Index removed
```

#### Error Cases
```typescript
// Test 4: Drop non-existent index (by name)
await collection.dropIndex("nonexistent_1");
// Error: "index not found with name [nonexistent_1]"

// Test 5: Drop non-existent index (by spec)
await collection.dropIndex({ nonexistent: 1 });
// Error: "index not found with name [nonexistent_1]"

// Test 6: Cannot drop _id index
await collection.dropIndex("_id_");
// Error: "cannot drop _id index"

// Test 7: Cannot drop _id index by spec
await collection.dropIndex({ _id: 1 });
// Error: "cannot drop _id index"
```

### Implementation Notes
- If spec provided, convert to name for lookup
- Find index in array, remove if found
- Throw descriptive error if not found
- Special case: reject dropping `_id_` or `{ _id: 1 }`

---

## Step 3: `indexes()` and `listIndexes()` Methods

### Syntax
```typescript
collection.indexes(): Promise<IndexInfo[]>
collection.listIndexes(): IndexCursor
```

### Behavior (from MongoDB docs)
- `indexes()` is convenience method that returns array directly
- `listIndexes()` returns cursor (for consistency with MongoDB API)
- Always includes the default `_id_` index
- Returns index info with: `v`, `key`, `name`, and optional `unique`

### Test Cases

```typescript
// Test 1: Empty collection has _id index
const indexes = await collection.indexes();
assert.strictEqual(indexes.length, 1);
assert.deepStrictEqual(indexes[0].key, { _id: 1 });
assert.strictEqual(indexes[0].name, "_id_");

// Test 2: List multiple indexes
await collection.createIndex({ email: 1 }, { unique: true });
await collection.createIndex({ createdAt: -1 });
const indexes = await collection.indexes();
assert.strictEqual(indexes.length, 3);
// Indexes: _id_, email_1, createdAt_-1

// Test 3: Index info includes unique flag
await collection.createIndex({ email: 1 }, { unique: true });
const indexes = await collection.indexes();
const emailIdx = indexes.find(i => i.name === "email_1");
assert.strictEqual(emailIdx.unique, true);

// Test 4: listIndexes returns cursor
const cursor = collection.listIndexes();
const indexes = await cursor.toArray();
// Same result as indexes()

// Test 5: Index info structure
const indexes = await collection.indexes();
// Each index has: { v: 2, key: {...}, name: "...", unique?: boolean }
```

### Return Value Format
```typescript
interface IndexInfo {
  v: number;           // Index version (always 2)
  key: { [field: string]: 1 | -1 };
  name: string;
  unique?: boolean;    // Only present if true
}
```

---

## Step 4: Unique Constraint Enforcement

### Behavior (from MongoDB docs)
- Unique index prevents duplicate values for indexed field(s)
- Applies to `insertOne`, `insertMany`, `updateOne`, `updateMany` (when modifying indexed field)
- Error code: 11000
- Error format: `E11000 duplicate key error collection: <db>.<collection> index: <index_name> dup key: { <field>: "<value>" }`

### Test Cases - Insert Operations

```typescript
// Test 1: insertOne with duplicate value on unique index
await collection.createIndex({ email: 1 }, { unique: true });
await collection.insertOne({ email: "alice@test.com" });
await collection.insertOne({ email: "alice@test.com" });
// Error: E11000 duplicate key error collection: db.coll index: email_1 dup key: { email: "alice@test.com" }

// Test 2: insertMany with duplicate in batch
await collection.createIndex({ email: 1 }, { unique: true });
await collection.insertMany([
  { email: "a@test.com" },
  { email: "b@test.com" },
  { email: "a@test.com" }  // duplicate
]);
// Error on third document

// Test 3: insertMany with duplicate against existing
await collection.createIndex({ email: 1 }, { unique: true });
await collection.insertOne({ email: "exists@test.com" });
await collection.insertMany([
  { email: "new@test.com" },
  { email: "exists@test.com" }  // duplicate with existing
]);
// Error on second document

// Test 4: Unique constraint on compound index
await collection.createIndex({ firstName: 1, lastName: 1 }, { unique: true });
await collection.insertOne({ firstName: "John", lastName: "Doe" });
await collection.insertOne({ firstName: "John", lastName: "Smith" });  // OK
await collection.insertOne({ firstName: "John", lastName: "Doe" });    // Error
```

### Test Cases - Update Operations

```typescript
// Test 5: updateOne creating duplicate
await collection.createIndex({ email: 1 }, { unique: true });
await collection.insertOne({ name: "Alice", email: "alice@test.com" });
await collection.insertOne({ name: "Bob", email: "bob@test.com" });
await collection.updateOne({ name: "Bob" }, { $set: { email: "alice@test.com" } });
// Error: duplicate key

// Test 6: updateMany creating duplicate
await collection.createIndex({ code: 1 }, { unique: true });
await collection.insertMany([
  { name: "A", code: 1 },
  { name: "B", code: 2 },
  { name: "C", code: 3 }
]);
await collection.updateMany({ code: { $gt: 1 } }, { $set: { code: 1 } });
// Error: would create duplicates

// Test 7: upsert with duplicate
await collection.createIndex({ email: 1 }, { unique: true });
await collection.insertOne({ email: "exists@test.com" });
await collection.updateOne(
  { name: "New" },
  { $set: { email: "exists@test.com" } },
  { upsert: true }
);
// Error: duplicate key on upsert
```

### Test Cases - Edge Cases

```typescript
// Test 8: null values - multiple nulls allowed? (MongoDB allows multiple nulls)
await collection.createIndex({ optional: 1 }, { unique: true });
await collection.insertOne({ name: "A" });  // optional is missing (treated as null)
await collection.insertOne({ name: "B" });  // optional is missing
// This actually errors in MongoDB! Null/missing counts as a value.
// Error: duplicate key

// Test 9: Unique on nested field
await collection.createIndex({ "user.email": 1 }, { unique: true });
await collection.insertOne({ user: { email: "a@test.com" } });
await collection.insertOne({ user: { email: "a@test.com" } });
// Error: duplicate key

// Test 10: Error includes correct metadata
try {
  await collection.insertOne({ email: "duplicate@test.com" });
} catch (e) {
  assert(e.code === 11000);
  assert(e.message.includes("E11000"));
  assert(e.message.includes("email_1"));
  assert(e.message.includes("duplicate@test.com"));
}
```

### Implementation Notes

1. **On insert**: Before inserting document(s), check all unique indexes
2. **On update**: If update modifies a unique-indexed field, check for conflicts
3. **Check algorithm** (for each unique index):
   - Extract value(s) at indexed field path(s) from new/updated document
   - Query collection for existing document with same value
   - If found (and different `_id`), throw duplicate key error
4. **Error class**: Create `MongoDuplicateKeyError` extending Error with `code: 11000`

---

## Step 5: Error Classes and Messages

### Error Definitions

```typescript
class MongoDuplicateKeyError extends Error {
  code = 11000;
  keyPattern: { [field: string]: 1 | -1 };
  keyValue: { [field: string]: unknown };

  constructor(db: string, collection: string, indexName: string, keyValue: Record<string, unknown>) {
    const keyStr = Object.entries(keyValue)
      .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
      .join(", ");
    super(`E11000 duplicate key error collection: ${db}.${collection} index: ${indexName} dup key: { ${keyStr} }`);
    this.keyValue = keyValue;
  }
}
```

### Error Messages (to match MongoDB)

| Condition | Error Message |
|-----------|---------------|
| Duplicate key on insert/update | `E11000 duplicate key error collection: <db>.<coll> index: <name> dup key: { <field>: <value> }` |
| Drop non-existent index | `index not found with name [<indexName>]` |
| Drop _id index | `cannot drop _id index` |

---

## Implementation Order

### Recommended Order (by dependency)

1. **Index storage layer** - Read/write `.indexes.json` files
2. **`createIndex()`** - Create and persist indexes
3. **`indexes()` / `listIndexes()`** - List indexes
4. **`dropIndex()`** - Remove indexes
5. **Unique enforcement on `insertOne`** - Check before insert
6. **Unique enforcement on `insertMany`** - Check batch + existing
7. **Unique enforcement on `updateOne/Many`** - Check when modifying indexed fields
8. **Error class** - `MongoDuplicateKeyError` with correct format

### File Changes Required

#### `src/collection.ts`
```typescript
// New methods
async createIndex(keySpec: IndexKeySpec, options?: CreateIndexOptions): Promise<string>
async dropIndex(indexNameOrSpec: string | IndexKeySpec): Promise<void>
async indexes(): Promise<IndexInfo[]>
listIndexes(): IndexCursor

// Modified methods (add unique constraint checks)
async insertOne(doc: Document): Promise<InsertOneResult>
async insertMany(docs: Document[]): Promise<InsertManyResult>
async updateOne(filter, update, options): Promise<UpdateResult>
async updateMany(filter, update, options): Promise<UpdateResult>

// New private methods
private async loadIndexes(): Promise<IndexInfo[]>
private async saveIndexes(indexes: IndexInfo[]): Promise<void>
private generateIndexName(keySpec: IndexKeySpec): string
private async checkUniqueConstraints(docs: Document[], excludeId?: ObjectId): Promise<void>
```

#### `src/errors.ts` (new file)
```typescript
export class MongoDuplicateKeyError extends Error {
  code = 11000;
  // ...
}
```

#### `src/cursor.ts`
```typescript
// New class for listIndexes cursor
export class IndexCursor {
  async toArray(): Promise<IndexInfo[]>
}
```

#### `test/indexes.test.ts` (new file)
- All index creation/deletion tests
- All unique constraint tests
- Following existing test patterns

---

## Test File Structure

```
test/indexes.test.ts
├── Index Management Tests (${getTestModeName()})
│   ├── createIndex
│   │   ├── should create ascending index and return name
│   │   ├── should create descending index and return name
│   │   ├── should create compound index
│   │   ├── should create index with custom name
│   │   ├── should create unique index
│   │   ├── should be idempotent for same spec
│   │   ├── should handle nested field paths
│   │   └── should handle existing index with different options
│   │
│   ├── dropIndex
│   │   ├── should drop index by name
│   │   ├── should drop index by key spec
│   │   ├── should throw for non-existent index
│   │   ├── should throw when dropping _id index
│   │   └── should handle compound index
│   │
│   └── indexes / listIndexes
│       ├── should return _id index for empty collection
│       ├── should list all created indexes
│       ├── should include unique flag in index info
│       └── listIndexes should return cursor
│
├── Unique Constraint Tests (${getTestModeName()})
│   ├── insertOne
│   │   ├── should reject duplicate on unique index
│   │   ├── should allow same value on non-unique index
│   │   └── should include error details
│   │
│   ├── insertMany
│   │   ├── should reject duplicate within batch
│   │   ├── should reject duplicate with existing document
│   │   └── should insert all before first duplicate
│   │
│   ├── updateOne
│   │   ├── should reject update creating duplicate
│   │   ├── should allow update not affecting unique field
│   │   └── should enforce unique on upsert
│   │
│   ├── updateMany
│   │   ├── should reject batch update creating duplicates
│   │   └── should allow updates not creating conflicts
│   │
│   └── Edge Cases
│       ├── should enforce unique on nested fields
│       ├── should enforce unique on compound indexes
│       ├── should handle null/missing values
│       └── should have correct error code (11000)
```

---

## Documentation Updates

After implementation, update:

1. **PROGRESS.md** - Add Phase 7 changelog entry
2. **ROADMAP.md** - Mark Phase 7 as complete, update current phase to 8
3. **COMPATIBILITY.md** - Add index behaviors section
4. **README.md** - Update "What's Implemented" section

---

## Sources

- [MongoDB createIndex documentation](https://www.mongodb.com/docs/manual/reference/method/db.collection.createindex/)
- [MongoDB dropIndex documentation](https://www.mongodb.com/docs/manual/reference/method/db.collection.dropindex/)
- [MongoDB listIndexes command](https://www.mongodb.com/docs/manual/reference/command/listindexes/)
- [MongoDB Unique Indexes](https://www.mongodb.com/docs/manual/core/index-unique/)
- [MongoDB E11000 Error Format](https://www.mongodb.com/community/forums/t/mongoservererror-e11000-duplicate-key-error-collection/184202)
- [MongoDB Node.js Driver - Indexes](https://www.mongodb.com/docs/drivers/node/current/fundamentals/indexes/)
