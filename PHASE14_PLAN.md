# Phase 14: Extended Index Features - Implementation Plan

## Overview

This phase enhances index functionality with sparse indexes, TTL indexes, partial indexes, and index hints. These features are lower priority but provide important edge-case handling for advanced use cases.

**Priority**: Low
**Effort**: Medium
**Estimated Tests**: 25-30

---

## Current State Analysis

The existing `IndexManager` class (`src/index-manager.ts`) already has:
- Basic `sparse` option support in `CreateIndexOptions` (stored but not enforced)
- Unique constraint checking via `checkUniqueConstraints()`
- Index metadata storage in `{collection}.indexes.json`

**What needs to be implemented:**
1. Sparse index unique constraint behavior (skip missing fields)
2. TTL index creation and document expiration
3. Partial index creation and unique constraint scoping
4. Index hints on cursors and aggregation

---

## Step 1: Sparse Indexes (Behavioral Fix)

### Current Problem

The `sparse` option is accepted but not enforced. With `sparse: true`, unique indexes should allow multiple documents missing the indexed field.

### MongoDB Behavior (from [MongoDB Sparse Index Docs](https://www.mongodb.com/docs/manual/core/index-sparse/))

- Sparse indexes only contain entries for documents that **have the indexed field**
- Even `null` values are indexed (sparse means "skip missing", not "skip null")
- An index that is both sparse and unique prevents duplicate values but **allows multiple documents that omit the key**

### Syntax

```typescript
await collection.createIndex({ email: 1 }, { unique: true, sparse: true });

// These should all succeed:
await collection.insertOne({ name: "A" });                    // missing email - OK
await collection.insertOne({ name: "B" });                    // missing email - OK (sparse allows this)
await collection.insertOne({ name: "C", email: null });       // null email - OK
await collection.insertOne({ name: "D", email: null });       // null email - ERROR (duplicate null)
await collection.insertOne({ name: "E", email: "a@test.com" }); // OK
await collection.insertOne({ name: "F", email: "a@test.com" }); // ERROR (duplicate)
```

### Compound Index Sparse Behavior

For compound sparse indexes, a document is included **if at least one of the indexed fields is set**. Any missing fields are indexed as `null`.

```typescript
await collection.createIndex({ a: 1, b: 1 }, { unique: true, sparse: true });

// Document { a: 1 } is indexed as { a: 1, b: null }
// Document { b: 2 } is indexed as { a: null, b: 2 }
// Document {} is NOT indexed (all fields missing)
```

### Test Cases

```typescript
describe("Sparse Indexes", () => {
  // Basic sparse behavior
  it("should allow multiple missing values with sparse unique index");
  it("should NOT allow duplicate null values (null is indexed)");
  it("should enforce uniqueness for present values");

  // Compound sparse indexes
  it("should index document if at least one field is present");
  it("should skip document only if ALL indexed fields are missing");
  it("should treat missing fields as null for uniqueness check");

  // Metadata
  it("should list sparse: true in indexes()");
  it("should return sparse index name on creation");

  // Edge cases
  it("should handle sparse index on nested field");
  it("should work with updateOne that removes the indexed field");
});
```

### Implementation Notes

Modify `IndexManager.checkUniqueConstraints()`:

```typescript
async checkUniqueConstraints<T extends Document>(
  docs: T[],
  existingDocs: T[],
  excludeIds: Set<string> = new Set()
): Promise<void> {
  const indexes = await this.loadIndexes();
  const uniqueIndexes = indexes.filter((idx) => idx.unique);

  for (const idx of uniqueIndexes) {
    const isSparse = idx.sparse === true;
    const indexFields = Object.keys(idx.key);

    // For sparse indexes, filter out documents missing ALL indexed fields
    const filterForSparse = (doc: T): boolean => {
      if (!isSparse) return true;
      // For sparse: include if at least one indexed field exists
      return indexFields.some(field => {
        const value = getValueByPath(doc, field);
        return value !== undefined;
      });
    };

    // Build existing values map (filtering for sparse)
    const valueMap = new Map<string, T>();
    for (const doc of existingDocs) {
      if (!filterForSparse(doc)) continue;
      // ... rest of existing logic
    }

    // Check new documents (also filter for sparse)
    for (const doc of docs) {
      if (!filterForSparse(doc)) continue;
      // ... rest of existing logic
    }
  }
}
```

---

## Step 2: TTL Indexes

### MongoDB Behavior (from [MongoDB TTL Index Docs](https://www.mongodb.com/docs/manual/core/index-ttl/))

- TTL indexes are **single-field indexes only**
- Compound indexes do not support TTL (the option is ignored)
- The `_id` field does not support TTL indexes
- The indexed field must contain **Date values**
- Documents expire after `expireAfterSeconds` from the date field value
- The background deletion task runs approximately every **60 seconds**
- `expireAfterSeconds` must be between 0 and 2147483647 inclusive

### Syntax

```typescript
await collection.createIndex(
  { createdAt: 1 },
  { expireAfterSeconds: 3600 }  // Expire 1 hour after createdAt
);
```

### Error Messages

| Condition | Error Message |
|-----------|---------------|
| Compound index with TTL | No error, but TTL is silently ignored |
| TTL on `_id` field | `The field 'expireAfterSeconds' is not valid for an _id index` |
| Non-numeric expireAfterSeconds | `expireAfterSeconds must be a number` |
| Value out of range | `expireAfterSeconds must be between 0 and 2147483647` |
| NaN value | MongoDB 5.0.14+: index is not used for TTL |

### Design Decision for MangoDB

Since MangoDB is file-based and typically used for dev/testing:
- **Option A**: Store TTL metadata but don't auto-delete (simpler, still useful for API testing)
- **Option B**: Implement lazy deletion on read operations (moderate complexity)
- **Option C**: Background deletion with timer (complex, may not suit all use cases)

**Recommended: Option A** - Store TTL metadata, provide manual `expireDocuments()` method for testing.

### Test Cases

```typescript
describe("TTL Indexes", () => {
  // Creation
  it("should create TTL index with expireAfterSeconds");
  it("should store expireAfterSeconds in index metadata");
  it("should silently ignore TTL on compound indexes");
  it("should reject TTL on _id field");

  // Validation
  it("should reject non-numeric expireAfterSeconds");
  it("should reject expireAfterSeconds < 0");
  it("should reject expireAfterSeconds > 2147483647");
  it("should accept expireAfterSeconds: 0");

  // Metadata
  it("should list expireAfterSeconds in indexes()");

  // Optional: Expiration behavior
  it("should identify expired documents via helper method");
});
```

### Types to Add

```typescript
// In types.ts
export interface CreateIndexOptions {
  unique?: boolean;
  name?: string;
  sparse?: boolean;
  expireAfterSeconds?: number;  // NEW
}

export interface IndexInfo {
  v: number;
  key: IndexKeySpec;
  name: string;
  unique?: boolean;
  sparse?: boolean;
  expireAfterSeconds?: number;  // NEW
}
```

---

## Step 3: Partial Indexes

### MongoDB Behavior (from [MongoDB Partial Index Docs](https://www.mongodb.com/docs/manual/core/index-partial/))

- `partialFilterExpression` specifies which documents are indexed
- Unique constraints **only apply to documents matching the filter**
- Cannot combine `sparse` and `partialFilterExpression` options

### Allowed Operators in partialFilterExpression

- Equality expressions (`field: value` or `$eq`)
- `$exists: true`
- `$gt`, `$gte`, `$lt`, `$lte`
- `$type`
- `$and` (top-level only)

**NOT allowed**: `$or`, `$in`, `$regex`, `$exists: false`, nested `$and`

### Syntax

```typescript
await collection.createIndex(
  { email: 1 },
  {
    unique: true,
    partialFilterExpression: { status: "active" }
  }
);

// Unique constraint only applies when status === "active"
await collection.insertOne({ email: "a@test.com", status: "active" });
await collection.insertOne({ email: "a@test.com", status: "inactive" }); // OK - not in index
await collection.insertOne({ email: "a@test.com", status: "active" }); // ERROR - duplicate
```

### Error Messages

| Condition | Error Message |
|-----------|---------------|
| `sparse` + `partialFilterExpression` | `cannot mix 'partialFilterExpression' with 'sparse'` (error code 67) |
| Unsupported operator | `unsupported expression in partial index: $or` |
| Invalid expression | `partialFilterExpression must be an object` |

### Test Cases

```typescript
describe("Partial Indexes", () => {
  // Creation
  it("should create partial index with partialFilterExpression");
  it("should store partialFilterExpression in index metadata");
  it("should reject combining sparse and partialFilterExpression");

  // Unique constraint scoping
  it("should enforce uniqueness only for matching documents");
  it("should allow duplicate values in non-matching documents");
  it("should support $exists: true in filter");
  it("should support comparison operators in filter");
  it("should support $type in filter");
  it("should support top-level $and in filter");

  // Edge cases
  it("should handle document updates that move in/out of filter");
  it("should work with compound indexes");

  // Metadata
  it("should list partialFilterExpression in indexes()");
});
```

### Types to Add

```typescript
// In types.ts
export interface CreateIndexOptions {
  unique?: boolean;
  name?: string;
  sparse?: boolean;
  expireAfterSeconds?: number;
  partialFilterExpression?: Record<string, unknown>;  // NEW
}

export interface IndexInfo {
  v: number;
  key: IndexKeySpec;
  name: string;
  unique?: boolean;
  sparse?: boolean;
  expireAfterSeconds?: number;
  partialFilterExpression?: Record<string, unknown>;  // NEW
}
```

### Implementation Notes

Modify `IndexManager.checkUniqueConstraints()`:

```typescript
// Before checking uniqueness for a partial index, filter documents
const matchesPartialFilter = (doc: T, filter: Record<string, unknown>): boolean => {
  // Reuse matchesFilter from query-matcher.ts
  return matchesFilter(doc, filter);
};

for (const idx of uniqueIndexes) {
  const partialFilter = idx.partialFilterExpression;

  // Filter existing docs to only those matching partial filter
  const relevantExisting = partialFilter
    ? existingDocs.filter(doc => matchesPartialFilter(doc, partialFilter))
    : existingDocs;

  // Filter new docs similarly
  const relevantNew = partialFilter
    ? docs.filter(doc => matchesPartialFilter(doc, partialFilter))
    : docs;

  // Check uniqueness only among relevant documents
  // ...
}
```

---

## Step 4: Index Hints

### MongoDB Behavior (from [MongoDB cursor.hint() Docs](https://www.mongodb.com/docs/manual/reference/method/cursor.hint/))

- Forces the query planner to use a specific index
- Can specify by index name (string) or key pattern (object)
- Returns error if the specified index doesn't exist

### Syntax

```typescript
// By index name
await collection.find({ email: "a@test.com" }).hint("email_1").toArray();

// By key pattern
await collection.find({ email: "a@test.com" }).hint({ email: 1 }).toArray();

// $natural hint (scan order)
await collection.find({}).hint({ $natural: 1 }).toArray();   // Forward scan
await collection.find({}).hint({ $natural: -1 }).toArray();  // Reverse scan
```

### Error Message

```
error processing query: ns=test.collection limit=0 skip=0
Tree: ...
Sort: {}
Proj: {}
 planner returned error: bad hint
```

Error code: `17007` (or similar depending on version)

### Design Decision for MangoDB

Since MangoDB doesn't use indexes for query optimization (full scans always), hints are **API compatibility only**:
- Validate that the hinted index exists (throw if not)
- For `$natural`, apply scan direction (reverse array if -1)
- Otherwise, execute query as normal

### Test Cases

```typescript
describe("Index Hints", () => {
  // Cursor hints
  it("should accept hint by index name");
  it("should accept hint by key pattern");
  it("should throw for non-existent index name");
  it("should throw for non-existent key pattern");

  // $natural hints
  it("should support $natural: 1 (forward scan)");
  it("should support $natural: -1 (reverse scan)");
  it("should throw for invalid $natural value (MongoDB 7.0+)");

  // Aggregation hints
  it("should accept hint option in aggregate()");

  // Edge cases
  it("should ignore hint if index filter exists (API compat)");
  it("should work with other cursor methods (sort, limit, skip)");
});
```

### Implementation Notes

Add `hint()` method to `MangoDBCursor`:

```typescript
export class MangoDBCursor<T extends Document = Document> {
  private hintSpec: string | Record<string, unknown> | null = null;

  hint(indexHint: string | Record<string, unknown>): MangoDBCursor<T> {
    this.hintSpec = indexHint;
    return this;
  }

  async toArray(): Promise<T[]> {
    // Validate hint if provided
    if (this.hintSpec) {
      await this.validateHint();
    }

    let docs = await this.fetchDocuments();

    // Handle $natural hint for scan direction
    if (this.hintSpec && typeof this.hintSpec === 'object' && '$natural' in this.hintSpec) {
      const direction = this.hintSpec.$natural;
      if (direction === -1) {
        docs = [...docs].reverse();
      }
    }

    // ... rest of existing logic
  }

  private async validateHint(): Promise<void> {
    // Check if index exists, throw BadHintError if not
  }
}
```

Add new error class:

```typescript
// In errors.ts
export class BadHintError extends Error {
  readonly code = 17007;

  constructor(hint: string | Record<string, unknown>) {
    const hintStr = typeof hint === 'string' ? hint : JSON.stringify(hint);
    super(`planner returned error: bad hint - ${hintStr}`);
    this.name = "BadHintError";
  }
}
```

---

## Implementation Order

1. **Step 1: Sparse Indexes** (highest value, fixes existing partial implementation)
2. **Step 3: Partial Indexes** (builds on sparse, uses existing query matcher)
3. **Step 2: TTL Indexes** (independent, mainly metadata)
4. **Step 4: Index Hints** (independent, cursor modification)

---

## File Changes Required

| File | Changes |
|------|---------|
| `src/types.ts` | Add `expireAfterSeconds`, `partialFilterExpression` to options/info |
| `src/index-manager.ts` | Update `createIndex()`, `checkUniqueConstraints()` |
| `src/cursor.ts` | Add `hint()` method, `$natural` handling |
| `src/errors.ts` | Add `BadHintError`, `InvalidIndexOptionsError` |
| `src/collection.ts` | Pass index manager to cursor for hint validation |

---

## Test File Structure

```
test/indexes-extended.test.ts
├── Sparse Indexes (${getTestModeName()})
│   ├── should allow multiple missing values with sparse unique index
│   ├── should NOT allow duplicate null values
│   ├── should enforce uniqueness for present values
│   ├── should index document if at least one compound field is present
│   ├── should skip document only if ALL indexed fields are missing
│   ├── should list sparse: true in indexes()
│   └── should handle sparse index on nested field
│
├── TTL Indexes (${getTestModeName()})
│   ├── should create TTL index with expireAfterSeconds
│   ├── should store expireAfterSeconds in index metadata
│   ├── should silently ignore TTL on compound indexes
│   ├── should reject TTL on _id field
│   ├── should reject non-numeric expireAfterSeconds
│   ├── should reject expireAfterSeconds out of range
│   └── should accept expireAfterSeconds: 0
│
├── Partial Indexes (${getTestModeName()})
│   ├── should create partial index with partialFilterExpression
│   ├── should enforce uniqueness only for matching documents
│   ├── should allow duplicate values in non-matching documents
│   ├── should reject combining sparse and partialFilterExpression
│   ├── should support comparison operators in filter
│   ├── should support $exists: true in filter
│   └── should list partialFilterExpression in indexes()
│
└── Index Hints (${getTestModeName()})
    ├── should accept hint by index name
    ├── should accept hint by key pattern
    ├── should throw for non-existent index
    ├── should support $natural: 1 (forward scan)
    ├── should support $natural: -1 (reverse scan)
    └── should work with other cursor methods
```

---

## Documentation Updates

After implementation:
1. Update `PROGRESS.md` with Phase 14 changelog
2. Update `ROADMAP.md` current phase to Phase 15
3. Add new behaviors to `COMPATIBILITY.md`
4. Update test count in `ROADMAP_REMAINING.md`

---

## Edge Cases and Gotchas

### Sparse Indexes
- `null` values ARE indexed (sparse only skips `undefined`/missing)
- Compound sparse indexes include doc if ANY field exists

### TTL Indexes
- Compound indexes silently ignore `expireAfterSeconds` (no error!)
- NaN handling varies by MongoDB version

### Partial Indexes
- Cannot use `$or` in `partialFilterExpression`
- Cannot combine with `sparse: true`
- Query must include filter expression to use the index

### Index Hints
- Key order in hint must match index definition exactly
- `$natural` only accepts 1 or -1 (MongoDB 7.0+)
- Hint on hidden index returns error

---

## Summary

| Step | Feature | Tests | Complexity |
|------|---------|-------|------------|
| 1 | Sparse Indexes | ~8 | Medium |
| 2 | TTL Indexes | ~7 | Low |
| 3 | Partial Indexes | ~8 | Medium |
| 4 | Index Hints | ~7 | Low |
| **Total** | | **~30** | |

**Sources consulted:**
- [MongoDB Sparse Index Docs](https://www.mongodb.com/docs/manual/core/index-sparse/)
- [MongoDB TTL Index Docs](https://www.mongodb.com/docs/manual/core/index-ttl/)
- [MongoDB Partial Index Docs](https://www.mongodb.com/docs/manual/core/index-partial/)
- [MongoDB cursor.hint() Docs](https://www.mongodb.com/docs/manual/reference/method/cursor.hint/)
- [MongoDB Unique Index Docs](https://www.mongodb.com/docs/manual/core/index-unique/)
