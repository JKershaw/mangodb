# Phase 8: Operations Implementation Plan

**Goal**: Support additional commonly-used operations: `findOneAndUpdate`, `findOneAndDelete`, `findOneAndReplace`, and `bulkWrite`.

---

## Overview

Phase 8 adds four operations that are commonly used for atomic find-and-modify patterns and bulk operations. These methods combine find and write operations atomically and return the affected document.

---

## Step 1: `findOneAndDelete`

The simplest of the findOneAnd* methods - finds a document and deletes it.

### Interface

```typescript
interface FindOneAndDeleteOptions {
  /** Which fields to return in the result */
  projection?: ProjectionSpec;
  /** Sort order to determine which document to delete (if multiple match) */
  sort?: Record<string, 1 | -1>;
}

interface ModifyResult<T> {
  /** The document before deletion (null if not found) */
  value: T | null;
  /** Whether the operation was acknowledged */
  ok: 1 | 0;
}
```

### Method Signature

```typescript
async findOneAndDelete(
  filter: Filter<T>,
  options?: FindOneAndDeleteOptions
): Promise<ModifyResult<T>>
```

### Behavior

1. Find document(s) matching `filter`
2. If `sort` option provided, sort matches and select first
3. Delete the selected document
4. Return `{ value: deletedDoc, ok: 1 }` or `{ value: null, ok: 1 }` if no match

### Test Cases

```typescript
// Basic deletion - returns deleted document
await collection.insertOne({ name: "Alice", age: 30 });
const result = await collection.findOneAndDelete({ name: "Alice" });
assert.strictEqual(result.value?.name, "Alice");
assert.strictEqual(result.ok, 1);

// No match returns null
const result = await collection.findOneAndDelete({ name: "Nobody" });
assert.strictEqual(result.value, null);

// With sort - deletes first in sort order
await collection.insertMany([
  { name: "A", priority: 1 },
  { name: "B", priority: 2 },
  { name: "C", priority: 3 }
]);
const result = await collection.findOneAndDelete({}, { sort: { priority: -1 } });
assert.strictEqual(result.value?.name, "C"); // Highest priority deleted

// With projection
const result = await collection.findOneAndDelete(
  { name: "Alice" },
  { projection: { name: 1, _id: 0 } }
);
assert.deepStrictEqual(result.value, { name: "Alice" });
```

### Implementation Notes

- Reuse `matchesFilter()` for filtering
- Reuse cursor sort logic for ordering
- Apply projection to returned document using `applyProjection()`
- Check unique constraints NOT needed (we're deleting)

---

## Step 2: `findOneAndReplace`

Finds a document and replaces it entirely with a new document.

### Interface

```typescript
interface FindOneAndReplaceOptions {
  /** Which fields to return in the result */
  projection?: ProjectionSpec;
  /** Sort order to determine which document to replace */
  sort?: Record<string, 1 | -1>;
  /** Insert document if no match found */
  upsert?: boolean;
  /** Return document 'before' or 'after' replacement */
  returnDocument?: "before" | "after";
}
```

### Method Signature

```typescript
async findOneAndReplace(
  filter: Filter<T>,
  replacement: T,
  options?: FindOneAndReplaceOptions
): Promise<ModifyResult<T>>
```

### Behavior

1. Find document(s) matching `filter`
2. If `sort` provided, sort matches and select first
3. Replace the document (preserving `_id`)
4. If no match and `upsert: true`, insert replacement with new `_id`
5. Return document based on `returnDocument` option (default: "before")

### Validation

- `replacement` must NOT contain update operators (`$set`, `$inc`, etc.)
- Throw error if replacement contains keys starting with `$`

### Test Cases

```typescript
// Basic replace - returns document before replacement
await collection.insertOne({ name: "Alice", age: 30 });
const result = await collection.findOneAndReplace(
  { name: "Alice" },
  { name: "Alice", age: 31, city: "NYC" }
);
assert.strictEqual(result.value?.age, 30); // Before
const doc = await collection.findOne({ name: "Alice" });
assert.strictEqual(doc?.age, 31); // After
assert.strictEqual(doc?.city, "NYC");

// returnDocument: "after"
const result = await collection.findOneAndReplace(
  { name: "Alice" },
  { name: "Alice", age: 32 },
  { returnDocument: "after" }
);
assert.strictEqual(result.value?.age, 32);

// Preserves _id
const insertResult = await collection.insertOne({ name: "Alice" });
const originalId = insertResult.insertedId;
await collection.findOneAndReplace({ name: "Alice" }, { name: "Bob" });
const doc = await collection.findOne({ name: "Bob" });
assert.strictEqual(doc?._id.toHexString(), originalId.toHexString());

// Upsert when no match
const result = await collection.findOneAndReplace(
  { name: "NewUser" },
  { name: "NewUser", created: true },
  { upsert: true, returnDocument: "after" }
);
assert.strictEqual(result.value?.created, true);

// Error on update operators in replacement
await assert.rejects(
  collection.findOneAndReplace({ name: "Alice" }, { $set: { age: 30 } }),
  /replacement.*update operators/i
);
```

### Implementation Notes

- Must validate replacement doesn't contain `$` operators
- Preserve `_id` from original document when replacing
- For upsert, generate new `_id` if not in replacement
- Apply unique constraint checking after replacement

---

## Step 3: `findOneAndUpdate`

Finds a document and applies update operators to it.

### Interface

```typescript
interface FindOneAndUpdateOptions {
  /** Which fields to return in the result */
  projection?: ProjectionSpec;
  /** Sort order to determine which document to update */
  sort?: Record<string, 1 | -1>;
  /** Insert document if no match found */
  upsert?: boolean;
  /** Return document 'before' or 'after' update */
  returnDocument?: "before" | "after";
}
```

### Method Signature

```typescript
async findOneAndUpdate(
  filter: Filter<T>,
  update: UpdateOperators,
  options?: FindOneAndUpdateOptions
): Promise<ModifyResult<T>>
```

### Behavior

1. Find document(s) matching `filter`
2. If `sort` provided, sort matches and select first
3. Apply update operators to the document
4. If no match and `upsert: true`, create document from filter + update
5. Return document based on `returnDocument` option (default: "before")

### Test Cases

```typescript
// Basic update - returns document before update
await collection.insertOne({ name: "Alice", age: 30 });
const result = await collection.findOneAndUpdate(
  { name: "Alice" },
  { $set: { age: 31 } }
);
assert.strictEqual(result.value?.age, 30); // Before
const doc = await collection.findOne({ name: "Alice" });
assert.strictEqual(doc?.age, 31); // After

// returnDocument: "after"
await collection.insertOne({ name: "Bob", score: 100 });
const result = await collection.findOneAndUpdate(
  { name: "Bob" },
  { $inc: { score: 10 } },
  { returnDocument: "after" }
);
assert.strictEqual(result.value?.score, 110);

// With sort
await collection.insertMany([
  { type: "task", priority: 1 },
  { type: "task", priority: 2 }
]);
const result = await collection.findOneAndUpdate(
  { type: "task" },
  { $set: { done: true } },
  { sort: { priority: -1 } }
);
assert.strictEqual(result.value?.priority, 2); // Highest priority updated

// Upsert when no match
const result = await collection.findOneAndUpdate(
  { name: "NewUser" },
  { $set: { score: 0 } },
  { upsert: true, returnDocument: "after" }
);
assert.strictEqual(result.value?.name, "NewUser");
assert.strictEqual(result.value?.score, 0);

// With projection
const result = await collection.findOneAndUpdate(
  { name: "Alice" },
  { $inc: { age: 1 } },
  { projection: { name: 1 }, returnDocument: "after" }
);
assert.strictEqual(result.value?.name, "Alice");
assert.strictEqual(result.value?.age, undefined); // Not projected
```

### Implementation Notes

- Reuse `applyUpdateOperators()` from existing update code
- Reuse `createDocumentFromFilter()` for upsert
- Apply unique constraint checking after update

---

## Step 4: `bulkWrite`

Executes multiple write operations in bulk with ordered/unordered modes.

### Interface

```typescript
interface BulkWriteOperation<T> {
  insertOne?: { document: T };
  updateOne?: { filter: Filter<T>; update: UpdateOperators; upsert?: boolean };
  updateMany?: { filter: Filter<T>; update: UpdateOperators; upsert?: boolean };
  deleteOne?: { filter: Filter<T> };
  deleteMany?: { filter: Filter<T> };
  replaceOne?: { filter: Filter<T>; replacement: T; upsert?: boolean };
}

interface BulkWriteOptions {
  /** If true (default), stop on first error. If false, continue on errors. */
  ordered?: boolean;
}

interface BulkWriteResult {
  acknowledged: boolean;
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: Record<number, ObjectId>;
  upsertedIds: Record<number, ObjectId>;
}
```

### Method Signature

```typescript
async bulkWrite(
  operations: BulkWriteOperation<T>[],
  options?: BulkWriteOptions
): Promise<BulkWriteResult>
```

### Behavior

1. Execute operations in order (or unordered if specified)
2. For `ordered: true` (default), stop on first error
3. For `ordered: false`, continue and collect all errors
4. Aggregate results from all operations
5. Return combined result object

### Test Cases

```typescript
// Mixed operations
const result = await collection.bulkWrite([
  { insertOne: { document: { name: "Alice" } } },
  { insertOne: { document: { name: "Bob" } } },
  { updateOne: { filter: { name: "Alice" }, update: { $set: { age: 30 } } } },
  { deleteOne: { filter: { name: "Bob" } } }
]);
assert.strictEqual(result.insertedCount, 2);
assert.strictEqual(result.matchedCount, 1);
assert.strictEqual(result.modifiedCount, 1);
assert.strictEqual(result.deletedCount, 1);

// insertedIds tracking
const result = await collection.bulkWrite([
  { insertOne: { document: { x: 1 } } },
  { insertOne: { document: { x: 2 } } }
]);
assert.strictEqual(Object.keys(result.insertedIds).length, 2);
assert.ok(result.insertedIds[0]);
assert.ok(result.insertedIds[1]);

// updateMany in bulk
const result = await collection.bulkWrite([
  { insertOne: { document: { type: "a", v: 1 } } },
  { insertOne: { document: { type: "a", v: 2 } } },
  { updateMany: { filter: { type: "a" }, update: { $inc: { v: 10 } } } }
]);
assert.strictEqual(result.matchedCount, 2);
assert.strictEqual(result.modifiedCount, 2);

// deleteMany in bulk
await collection.insertMany([{ x: 1 }, { x: 1 }, { x: 2 }]);
const result = await collection.bulkWrite([
  { deleteMany: { filter: { x: 1 } } }
]);
assert.strictEqual(result.deletedCount, 2);

// replaceOne in bulk
const result = await collection.bulkWrite([
  { insertOne: { document: { name: "Alice", age: 30 } } },
  { replaceOne: { filter: { name: "Alice" }, replacement: { name: "Alice", age: 31, city: "NYC" } } }
]);
assert.strictEqual(result.matchedCount, 1);
assert.strictEqual(result.modifiedCount, 1);

// upsert tracking
const result = await collection.bulkWrite([
  { updateOne: { filter: { name: "New" }, update: { $set: { x: 1 } }, upsert: true } }
]);
assert.strictEqual(result.upsertedCount, 1);
assert.ok(result.upsertedIds[0]);

// ordered: true (default) - stops on error
await collection.createIndex({ email: 1 }, { unique: true });
await collection.insertOne({ email: "a@test.com" });
await assert.rejects(
  collection.bulkWrite([
    { insertOne: { document: { email: "a@test.com" } } }, // Duplicate - fails
    { insertOne: { document: { email: "b@test.com" } } }  // Never executed
  ])
);

// ordered: false - continues on error
// (Returns BulkWriteError with partial results)
```

### Implementation Notes

- Each operation type calls the corresponding existing method internally
- Track operation index for insertedIds/upsertedIds
- For ordered mode, stop execution on first error
- For unordered mode, collect errors and throw BulkWriteError at end
- Consider adding `BulkWriteError` class to errors.ts

---

## Code Changes Required

### 1. `src/errors.ts`

Add new error class:

```typescript
/**
 * Error thrown when a bulk write operation fails.
 */
export class BulkWriteError extends Error {
  readonly code = 65; // WriteConflict
  readonly writeErrors: Array<{ index: number; code: number; errmsg: string }>;
  readonly result: BulkWriteResult;

  constructor(
    message: string,
    writeErrors: Array<{ index: number; code: number; errmsg: string }>,
    result: BulkWriteResult
  ) {
    super(message);
    this.name = "BulkWriteError";
    this.writeErrors = writeErrors;
    this.result = result;
  }
}
```

### 2. `src/collection.ts`

Add interfaces:

```typescript
interface FindOneAndDeleteOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
}

interface FindOneAndReplaceOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

interface FindOneAndUpdateOptions {
  projection?: ProjectionSpec;
  sort?: Record<string, 1 | -1>;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

interface ModifyResult<T> {
  value: T | null;
  ok: 1 | 0;
}

interface BulkWriteOperation<T> {
  insertOne?: { document: T };
  updateOne?: { filter: Filter<T>; update: UpdateOperators; upsert?: boolean };
  updateMany?: { filter: Filter<T>; update: UpdateOperators; upsert?: boolean };
  deleteOne?: { filter: Filter<T> };
  deleteMany?: { filter: Filter<T> };
  replaceOne?: { filter: Filter<T>; replacement: T; upsert?: boolean };
}

interface BulkWriteOptions {
  ordered?: boolean;
}

interface BulkWriteResult {
  acknowledged: boolean;
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: Record<number, ObjectId>;
  upsertedIds: Record<number, ObjectId>;
}
```

Add methods to `MongoneCollection`:

1. `findOneAndDelete(filter, options?)` - ~30 lines
2. `findOneAndReplace(filter, replacement, options?)` - ~50 lines
3. `findOneAndUpdate(filter, update, options?)` - ~50 lines
4. `bulkWrite(operations, options?)` - ~80 lines

Add helper method:

```typescript
/**
 * Sort documents and return the first one.
 * Used by findOneAnd* methods.
 */
private sortAndSelectFirst(docs: T[], sortSpec: Record<string, 1 | -1>): T | null
```

### 3. `test/test-harness.ts`

Add interfaces:

```typescript
interface ModifyResult<T> {
  value: T | null;
  ok: number;
}

interface FindOneAndDeleteOptions {
  projection?: Document;
  sort?: Document;
}

interface FindOneAndReplaceOptions {
  projection?: Document;
  sort?: Document;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

interface FindOneAndUpdateOptions {
  projection?: Document;
  sort?: Document;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

interface BulkWriteOperation {
  insertOne?: { document: Document };
  updateOne?: { filter: Document; update: Document; upsert?: boolean };
  updateMany?: { filter: Document; update: Document; upsert?: boolean };
  deleteOne?: { filter: Document };
  deleteMany?: { filter: Document };
  replaceOne?: { filter: Document; replacement: Document; upsert?: boolean };
}

interface BulkWriteResult {
  acknowledged: boolean;
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: Record<number, unknown>;
  upsertedIds: Record<number, unknown>;
}
```

Update `TestCollection` interface:

```typescript
findOneAndDelete(filter: Partial<T>, options?: FindOneAndDeleteOptions): Promise<ModifyResult<T>>;
findOneAndReplace(filter: Partial<T>, replacement: T, options?: FindOneAndReplaceOptions): Promise<ModifyResult<T>>;
findOneAndUpdate(filter: Partial<T>, update: Document, options?: FindOneAndUpdateOptions): Promise<ModifyResult<T>>;
bulkWrite(operations: BulkWriteOperation[], options?: { ordered?: boolean }): Promise<BulkWriteResult>;
```

### 4. `test/advanced.test.ts` (new file)

Create test file with sections:

1. `findOneAndDelete` tests
2. `findOneAndReplace` tests
3. `findOneAndUpdate` tests
4. `bulkWrite` tests

---

## Implementation Order

1. **findOneAndDelete** - Simplest, establishes pattern for findOneAnd* methods
2. **findOneAndReplace** - Builds on delete, adds replacement logic
3. **findOneAndUpdate** - Builds on replace, uses existing update operators
4. **bulkWrite** - Uses all existing operations, adds bulk coordination

---

## Design Decisions

1. **Return type**: Use `ModifyResult<T>` with `value` and `ok` fields to match MongoDB driver
2. **Default returnDocument**: "before" (MongoDB default) - returns document before modification
3. **Sort reuse**: Create a shared helper for sorting in findOneAnd* methods
4. **bulkWrite atomicity**: Operations are NOT atomic across the batch (matches MongoDB for non-transactional writes)
5. **Error handling**: Ordered mode stops on first error; unordered collects all errors

---

## Considerations

1. **Unique constraints**: Must check after modifications, not before
2. **Sort complexity**: Reuse cursor sorting logic via helper function
3. **Projection timing**: Apply projection AFTER determining returnDocument value
4. **bulkWrite efficiency**: Read documents once, apply all operations, write once (optimization for later)
5. **replaceOne validation**: Must reject documents containing `$` operator keys
