# Phase 15: Administrative Operations - Implementation Plan

## Overview

This phase implements database-level and collection-level administrative operations. These methods are useful for testing, management, and introspection of the database.

**Priority**: Low
**Estimated Tests**: 25-35
**Estimated Code Lines**: 200-300

---

## Operations to Implement

| Method | Location | Description |
|--------|----------|-------------|
| `db.listCollections()` | `db.ts` | List all collections in database |
| `db.stats()` | `db.ts` | Database statistics |
| `collection.drop()` | `collection.ts` | Drop the collection |
| `collection.rename()` | `collection.ts` | Rename collection |
| `collection.stats()` | `collection.ts` | Collection statistics |
| `collection.distinct()` | `collection.ts` | Get distinct values for a field |
| `collection.estimatedDocumentCount()` | `collection.ts` | Fast count without filter |

---

## Step 1: `db.listCollections()`

### Syntax

```typescript
// Returns a cursor that iterates over collection info documents
db.listCollections(filter?: Document, options?: ListCollectionsOptions): ListCollectionsCursor

interface ListCollectionsOptions {
  nameOnly?: boolean;  // If true, returns only collection names
  batchSize?: number;  // Cursor batch size (ignored in MangoDB)
}

// Each document in cursor has format:
interface CollectionInfo {
  name: string;
  type: 'collection' | 'view';
  options: Record<string, unknown>;
  info: {
    readOnly: boolean;
  };
}
```

### Behavior

- Returns a cursor that can be iterated with `toArray()` or `next()`
- `filter` can be used to match collections: `{ name: "users" }` or `{ name: { $regex: /^test/ } }`
- `nameOnly: true` returns simplified documents: `{ name: string, type: string }`
- Does NOT include system collections (if any)
- Empty database returns empty cursor
- **MangoDB**: Scans the database directory for `.json` files (excluding `.indexes.json`)

### Test Cases

```typescript
// List all collections
const cursor = db.listCollections();
const collections = await cursor.toArray();
// Returns: [{ name: 'users', type: 'collection', ... }, ...]

// List with filter
const filtered = await db.listCollections({ name: 'users' }).toArray();

// List with regex filter
const matching = await db.listCollections({ name: { $regex: /^test/ } }).toArray();

// nameOnly option
const names = await db.listCollections({}, { nameOnly: true }).toArray();
// Returns: [{ name: 'users', type: 'collection' }, ...]

// Empty database
const empty = await emptyDb.listCollections().toArray();
// Returns: []
```

### Implementation Notes

**File**: `src/db.ts`

```typescript
import { readdir } from "node:fs/promises";
import { join } from "node:path";

class ListCollectionsCursor {
  private results: CollectionInfo[] | null = null;

  constructor(
    private loader: () => Promise<CollectionInfo[]>,
    private filter: Document = {},
    private options: ListCollectionsOptions = {}
  ) {}

  async toArray(): Promise<CollectionInfo[]> {
    if (this.results === null) {
      this.results = await this.loader();
      // Apply filter if provided
      if (Object.keys(this.filter).length > 0) {
        this.results = this.results.filter(doc => matchesFilter(doc, this.filter));
      }
    }

    if (this.options.nameOnly) {
      return this.results.map(c => ({ name: c.name, type: c.type }));
    }
    return this.results;
  }
}

// In MangoDBDb class:
listCollections(filter: Document = {}, options: ListCollectionsOptions = {}): ListCollectionsCursor {
  return new ListCollectionsCursor(
    async () => {
      const dbPath = join(this.dataDir, this.name);
      try {
        const files = await readdir(dbPath);
        const collections = files
          .filter(f => f.endsWith('.json') && !f.endsWith('.indexes.json'))
          .map(f => f.replace('.json', ''));

        return collections.map(name => ({
          name,
          type: 'collection' as const,
          options: {},
          info: { readOnly: false }
        }));
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
          return [];
        }
        throw error;
      }
    },
    filter,
    options
  );
}
```

---

## Step 2: `db.stats()`

### Syntax

```typescript
interface DbStats {
  db: string;           // Database name
  collections: number;  // Number of collections
  views: number;        // Number of views (always 0 for MangoDB)
  objects: number;      // Total number of documents across all collections
  dataSize: number;     // Total size of data files in bytes
  storageSize: number;  // Same as dataSize for MangoDB
  indexes: number;      // Total number of indexes
  indexSize: number;    // Total size of index files in bytes
  totalSize: number;    // dataSize + indexSize
  ok: 1;                // Status indicator
}

db.stats(): Promise<DbStats>
```

### Behavior

- Returns statistics about the database
- Reads all collection files to calculate sizes
- Does not require reading document contents (uses file stats)
- Empty database returns all zeros with `ok: 1`

### Test Cases

```typescript
// Basic stats
const stats = await db.stats();
expect(stats.db).toBe('testdb');
expect(stats.ok).toBe(1);
expect(stats.collections).toBeGreaterThanOrEqual(0);

// Stats after inserting documents
await collection.insertMany([{ a: 1 }, { b: 2 }]);
const stats = await db.stats();
expect(stats.objects).toBeGreaterThanOrEqual(2);
expect(stats.dataSize).toBeGreaterThan(0);

// Empty database
const emptyStats = await emptyDb.stats();
expect(emptyStats.collections).toBe(0);
expect(emptyStats.objects).toBe(0);
```

### Implementation Notes

**File**: `src/db.ts`

```typescript
import { readdir, stat } from "node:fs/promises";

async stats(): Promise<DbStats> {
  const dbPath = join(this.dataDir, this.name);

  let collections = 0;
  let objects = 0;
  let dataSize = 0;
  let indexes = 0;
  let indexSize = 0;

  try {
    const files = await readdir(dbPath);

    for (const file of files) {
      const filePath = join(dbPath, file);
      const fileStat = await stat(filePath);

      if (file.endsWith('.indexes.json')) {
        // Read index count from file
        const content = JSON.parse(await readFile(filePath, 'utf-8'));
        indexes += content.indexes?.length || 0;
        indexSize += fileStat.size;
      } else if (file.endsWith('.json')) {
        collections++;
        dataSize += fileStat.size;
        // Read document count
        const content = JSON.parse(await readFile(filePath, 'utf-8'));
        objects += Array.isArray(content) ? content.length : 0;
      }
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error;
    }
  }

  return {
    db: this.name,
    collections,
    views: 0,
    objects,
    dataSize,
    storageSize: dataSize,
    indexes,
    indexSize,
    totalSize: dataSize + indexSize,
    ok: 1
  };
}
```

---

## Step 3: `collection.drop()`

### Syntax

```typescript
collection.drop(): Promise<boolean>
```

### Behavior

- Permanently removes the collection from the database
- Deletes the data file (`.json`) and index file (`.indexes.json`)
- Returns `true` if the collection existed and was dropped
- Returns `true` even if the collection did not exist (MongoDB behavior as of recent versions)
- Clears any cached collection reference in the parent database
- **Note**: The MongoDB documentation states it returns `true` even if collection doesn't exist

### Test Cases

```typescript
// Drop existing collection
await collection.insertOne({ name: 'test' });
const result = await collection.drop();
expect(result).toBe(true);

// Verify collection is gone
const docs = await collection.find({}).toArray();
expect(docs).toEqual([]);

// Drop non-existent collection (returns true per MongoDB docs)
const newCollection = db.collection('nonexistent');
const result = await newCollection.drop();
expect(result).toBe(true);

// Drop removes indexes too
await collection.createIndex({ email: 1 }, { unique: true });
await collection.drop();
const indexes = await collection.indexes();
expect(indexes).toHaveLength(1); // Only _id_ index on fresh collection
```

### Implementation Notes

**File**: `src/collection.ts`

```typescript
import { unlink } from "node:fs/promises";

async drop(): Promise<boolean> {
  try {
    // Delete data file
    await unlink(this.filePath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error;
    }
  }

  try {
    // Delete index file
    const indexFilePath = this.filePath.replace('.json', '.indexes.json');
    await unlink(indexFilePath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error;
    }
  }

  // Reset index manager
  await this.indexManager.reset();

  return true;
}
```

---

## Step 4: `collection.rename()`

### Syntax

```typescript
interface RenameOptions {
  dropTarget?: boolean;  // If true, drop target collection if it exists
}

collection.rename(newName: string, options?: RenameOptions): Promise<Collection<T>>
```

### Behavior

- Renames the collection to `newName`
- Returns a new Collection instance pointing to the renamed collection
- **Error Cases**:
  - If `newName` is the same as current name: `IllegalOperation` error
  - If target collection exists and `dropTarget` is not `true`: Error
  - If source collection doesn't exist: `NamespaceNotFound` error (code 26)
  - Invalid collection names (empty, starts/ends with `.`, contains `$`)

### Error Messages (from MongoDB)

| Condition | Error Message | Code |
|-----------|---------------|------|
| Same name | `cannot rename collection to itself` | (varies) |
| Target exists | `target namespace exists` | 48 |
| Source doesn't exist | `source namespace does not exist` | 26 |
| Empty name | `collection names cannot be empty` | - |
| Starts with `.` | `collection names must not start or end with '.'` | - |
| Contains `$` | `collection names cannot contain '$'` | - |

### Test Cases

```typescript
// Basic rename
await collection.insertOne({ name: 'test' });
const renamed = await collection.rename('newName');
const docs = await renamed.find({}).toArray();
expect(docs).toHaveLength(1);

// Old collection should be empty
const oldDocs = await collection.find({}).toArray();
expect(oldDocs).toEqual([]);

// Rename preserves indexes
await collection.createIndex({ email: 1 }, { unique: true });
const renamed = await collection.rename('renamedCollection');
const indexes = await renamed.indexes();
expect(indexes.some(i => i.name === 'email_1')).toBe(true);

// Rename to existing collection (error without dropTarget)
await expect(collection.rename('existingCollection')).rejects.toThrow();

// Rename with dropTarget: true
await collection.rename('existingCollection', { dropTarget: true });

// Rename to same name (error)
await expect(collection.rename('sameCollection')).rejects.toThrow(/cannot rename collection to itself/);

// Rename non-existent collection (error code 26)
const nonExistent = db.collection('doesNotExist');
await expect(nonExistent.rename('newName')).rejects.toThrow(/source namespace does not exist/);

// Invalid names
await expect(collection.rename('')).rejects.toThrow(/cannot be empty/);
await expect(collection.rename('.invalid')).rejects.toThrow(/must not start or end/);
await expect(collection.rename('invalid$name')).rejects.toThrow(/cannot contain/);
```

### Implementation Notes

**File**: `src/collection.ts`

```typescript
import { rename as renameFile, access } from "node:fs/promises";

async rename(newName: string, options: RenameOptions = {}): Promise<MangoDBCollection<T>> {
  // Validate new name
  if (!newName || newName.length === 0) {
    throw new Error('collection names cannot be empty');
  }
  if (newName.startsWith('.') || newName.endsWith('.')) {
    throw new Error("collection names must not start or end with '.'");
  }
  if (newName.includes('$')) {
    throw new Error("collection names cannot contain '$'");
  }

  const currentName = this.filePath.split('/').pop()?.replace('.json', '');
  if (currentName === newName) {
    throw new Error('cannot rename collection to itself');
  }

  // Check source exists
  try {
    await access(this.filePath);
  } catch {
    const error = new Error('source namespace does not exist') as Error & { code: number };
    error.code = 26;
    throw error;
  }

  const newFilePath = this.filePath.replace(`${currentName}.json`, `${newName}.json`);
  const newIndexPath = this.filePath.replace(`${currentName}.json`, `${newName}.indexes.json`);
  const currentIndexPath = this.filePath.replace('.json', '.indexes.json');

  // Check if target exists
  try {
    await access(newFilePath);
    if (!options.dropTarget) {
      const error = new Error('target namespace exists') as Error & { code: number };
      error.code = 48;
      throw error;
    }
    // Drop target
    await unlink(newFilePath);
    try { await unlink(newIndexPath); } catch {}
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error;
    }
  }

  // Rename files
  await renameFile(this.filePath, newFilePath);
  try {
    await renameFile(currentIndexPath, newIndexPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      throw error;
    }
  }

  // Return new collection instance
  return new MangoDBCollection(this.dataDir, this.dbName, newName);
}
```

---

## Step 5: `collection.stats()`

### Syntax

```typescript
interface CollectionStats {
  ns: string;           // Namespace: "db.collection"
  count: number;        // Number of documents
  size: number;         // Total size of documents in bytes
  storageSize: number;  // Same as size for MangoDB
  totalIndexSize: number;  // Size of indexes
  indexSizes: Record<string, number>;  // Size of each index
  totalSize: number;    // size + totalIndexSize
  nindexes: number;     // Number of indexes
  ok: 1;
}

collection.stats(): Promise<CollectionStats>
```

### Behavior

- Returns statistics about the collection
- `count` is the number of documents
- `size` is calculated from the data file size
- `indexSizes` shows each index by name with its size
- For MangoDB, all indexes share the same file, so sizes are approximate

### Test Cases

```typescript
// Basic stats
const stats = await collection.stats();
expect(stats.ns).toBe('testdb.testcollection');
expect(stats.ok).toBe(1);
expect(stats.nindexes).toBeGreaterThanOrEqual(1); // At least _id_

// Stats reflect document count
await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);
const stats = await collection.stats();
expect(stats.count).toBe(3);

// Stats reflect indexes
await collection.createIndex({ email: 1 });
const stats = await collection.stats();
expect(stats.nindexes).toBeGreaterThanOrEqual(2);
expect(stats.indexSizes).toHaveProperty('email_1');

// Empty collection
const emptyStats = await emptyCollection.stats();
expect(emptyStats.count).toBe(0);
```

### Implementation Notes

**File**: `src/collection.ts`

```typescript
async stats(): Promise<CollectionStats> {
  const docs = await this.readDocuments();
  const indexes = await this.indexManager.indexes();

  let dataSize = 0;
  let indexSize = 0;

  try {
    const fileStat = await stat(this.filePath);
    dataSize = fileStat.size;
  } catch {}

  const indexFilePath = this.filePath.replace('.json', '.indexes.json');
  try {
    const indexStat = await stat(indexFilePath);
    indexSize = indexStat.size;
  } catch {}

  // Distribute index size roughly among indexes
  const indexSizes: Record<string, number> = {};
  const perIndexSize = indexes.length > 0 ? Math.floor(indexSize / indexes.length) : 0;
  for (const idx of indexes) {
    indexSizes[idx.name] = perIndexSize;
  }

  return {
    ns: `${this.dbName}.${this.filePath.split('/').pop()?.replace('.json', '')}`,
    count: docs.length,
    size: dataSize,
    storageSize: dataSize,
    totalIndexSize: indexSize,
    indexSizes,
    totalSize: dataSize + indexSize,
    nindexes: indexes.length,
    ok: 1
  };
}
```

---

## Step 6: `collection.distinct()`

### Syntax

```typescript
collection.distinct<K extends keyof T>(
  field: string,
  filter?: Filter<T>
): Promise<unknown[]>
```

### Behavior

- Returns an array of distinct values for the specified field
- If `filter` is provided, only considers matching documents
- **Array field behavior**: If the field value is an array, each element is treated as a separate value
  - Example: `[1, [1], 1]` → distinct values are `1`, `[1]`
- Returns values in arbitrary order (no guaranteed sort)
- Empty collection returns empty array `[]`
- Missing field values are NOT included (undefined is skipped)
- `null` values ARE included as a distinct value

### Test Cases

```typescript
// Basic distinct
await collection.insertMany([
  { category: 'A' },
  { category: 'B' },
  { category: 'A' },
  { category: 'C' }
]);
const values = await collection.distinct('category');
expect(values.sort()).toEqual(['A', 'B', 'C']);

// Distinct with filter
const filtered = await collection.distinct('category', { active: true });

// Array field - each element is separate
await collection.insertMany([
  { tags: ['red', 'blue'] },
  { tags: ['green', 'red'] }
]);
const tags = await collection.distinct('tags');
expect(tags.sort()).toEqual(['blue', 'green', 'red']);

// Nested array elements
await collection.insertOne({ values: [1, [1], 1] });
const vals = await collection.distinct('values');
// Expect: [1, [1]] (duplicates removed, nested array is distinct)

// Missing field skipped
await collection.insertMany([
  { name: 'Alice', category: 'A' },
  { name: 'Bob' }  // No category
]);
const categories = await collection.distinct('category');
expect(categories).toEqual(['A']); // Bob not included

// Null is included
await collection.insertMany([
  { status: 'active' },
  { status: null }
]);
const statuses = await collection.distinct('status');
expect(statuses).toContain(null);
expect(statuses).toContain('active');

// Empty collection
const empty = await emptyCollection.distinct('field');
expect(empty).toEqual([]);

// Nested field with dot notation
await collection.insertMany([
  { user: { role: 'admin' } },
  { user: { role: 'user' } },
  { user: { role: 'admin' } }
]);
const roles = await collection.distinct('user.role');
expect(roles.sort()).toEqual(['admin', 'user']);
```

### Implementation Notes

**File**: `src/collection.ts`

```typescript
async distinct(field: string, filter: Filter<T> = {}): Promise<unknown[]> {
  const documents = await this.readDocuments();
  const filtered = await this.filterWithTextSupport(documents, filter);

  const seen = new Set<string>();
  const values: unknown[] = [];

  for (const doc of filtered) {
    const value = getValueByPath(doc, field);

    // Skip undefined (missing field)
    if (value === undefined) {
      continue;
    }

    // If value is an array, add each element separately
    if (Array.isArray(value)) {
      for (const elem of value) {
        const key = JSON.stringify(elem);
        if (!seen.has(key)) {
          seen.add(key);
          values.push(elem);
        }
      }
    } else {
      const key = JSON.stringify(value);
      if (!seen.has(key)) {
        seen.add(key);
        values.push(value);
      }
    }
  }

  return values;
}
```

---

## Step 7: `collection.estimatedDocumentCount()`

### Syntax

```typescript
collection.estimatedDocumentCount(): Promise<number>
```

### Behavior

- Returns an estimated count of documents in the collection
- Does NOT take a query filter (use `countDocuments()` for filtered counts)
- Faster than `countDocuments()` as it can use metadata
- **MangoDB**: Reads document count from file (same as `countDocuments({})`)
- May be inaccurate after unclean shutdown (MongoDB behavior - not applicable to MangoDB)

### Test Cases

```typescript
// Basic count
await collection.insertMany([{ a: 1 }, { b: 2 }, { c: 3 }]);
const count = await collection.estimatedDocumentCount();
expect(count).toBe(3);

// Empty collection
const emptyCount = await emptyCollection.estimatedDocumentCount();
expect(emptyCount).toBe(0);

// Count after deletes
await collection.deleteOne({ a: 1 });
const newCount = await collection.estimatedDocumentCount();
expect(newCount).toBe(2);

// Compare with countDocuments
const estimated = await collection.estimatedDocumentCount();
const accurate = await collection.countDocuments({});
expect(estimated).toBe(accurate);
```

### Implementation Notes

**File**: `src/collection.ts`

```typescript
async estimatedDocumentCount(): Promise<number> {
  const documents = await this.readDocuments();
  return documents.length;
}
```

---

## Implementation Order

1. **`estimatedDocumentCount()`** - Simplest, foundation for stats
2. **`distinct()`** - Self-contained, uses existing query matching
3. **`collection.drop()`** - File operations only
4. **`collection.stats()`** - Uses existing methods
5. **`collection.rename()`** - More complex, error handling
6. **`db.listCollections()`** - Requires cursor class
7. **`db.stats()`** - Uses multiple collection stats

---

## File Changes Required

| File | Changes |
|------|---------|
| `src/types.ts` | Add `ListCollectionsOptions`, `RenameOptions`, `DbStats`, `CollectionStats` interfaces |
| `src/collection.ts` | Add `drop()`, `rename()`, `stats()`, `distinct()`, `estimatedDocumentCount()` methods |
| `src/db.ts` | Add `listCollections()`, `stats()` methods; add `ListCollectionsCursor` class |
| `src/errors.ts` | Add `NamespaceNotFoundError` (code 26), `TargetNamespaceExistsError` (code 48) |
| `test/admin.test.ts` | New test file |

---

## Test File Structure

```
test/admin.test.ts
├── Administrative Operations Tests (${getTestModeName()})
│   │
│   ├── estimatedDocumentCount
│   │   ├── should return count of documents
│   │   ├── should return 0 for empty collection
│   │   ├── should reflect inserts and deletes
│   │   └── should match countDocuments for no filter
│   │
│   ├── distinct
│   │   ├── should return unique values
│   │   ├── should return empty array for empty collection
│   │   ├── should support filter parameter
│   │   ├── should treat array elements as separate values
│   │   ├── should handle nested arrays as distinct values
│   │   ├── should skip missing fields (undefined)
│   │   ├── should include null as a distinct value
│   │   ├── should support nested fields with dot notation
│   │   └── should handle mixed types
│   │
│   ├── collection.drop
│   │   ├── should drop existing collection
│   │   ├── should return true for non-existent collection
│   │   ├── should remove data file
│   │   ├── should remove index file
│   │   └── should reset indexes (fresh _id_ only)
│   │
│   ├── collection.rename
│   │   ├── should rename collection
│   │   ├── should preserve documents
│   │   ├── should preserve indexes
│   │   ├── should return new collection instance
│   │   ├── should error when renaming to same name
│   │   ├── should error when target exists (no dropTarget)
│   │   ├── should succeed with dropTarget: true
│   │   ├── should error for non-existent source (code 26)
│   │   ├── should error for empty name
│   │   ├── should error for name starting with dot
│   │   └── should error for name containing $
│   │
│   ├── collection.stats
│   │   ├── should return stats object
│   │   ├── should include namespace
│   │   ├── should include document count
│   │   ├── should include index count
│   │   ├── should include size information
│   │   └── should return ok: 1
│   │
│   ├── db.listCollections
│   │   ├── should list all collections
│   │   ├── should return empty for empty database
│   │   ├── should support filter by name
│   │   ├── should support regex filter
│   │   ├── should support nameOnly option
│   │   └── should return cursor with toArray
│   │
│   └── db.stats
│       ├── should return stats object
│       ├── should include database name
│       ├── should include collection count
│       ├── should include total document count
│       ├── should include size information
│       └── should return ok: 1
```

---

## Error Classes to Add

**File**: `src/errors.ts`

```typescript
export class NamespaceNotFoundError extends Error {
  code = 26;
  codeName = 'NamespaceNotFound';

  constructor(message = 'source namespace does not exist') {
    super(message);
    this.name = 'NamespaceNotFoundError';
  }
}

export class TargetNamespaceExistsError extends Error {
  code = 48;
  codeName = 'NamespaceExists';

  constructor(message = 'target namespace exists') {
    super(message);
    this.name = 'TargetNamespaceExistsError';
  }
}
```

---

## Documentation Updates

After implementation:
1. Update `PROGRESS.md` with Phase 15 details
2. Update `ROADMAP_REMAINING.md` to mark Phase 15 complete
3. Add any new behaviors discovered to `COMPATIBILITY.md`
4. Update test count in documentation

---

## Edge Cases and MongoDB Behaviors

### distinct()

| Behavior | Details |
|----------|---------|
| Array field | Each element treated as separate value |
| Nested array `[1, [1]]` | `1` and `[1]` are distinct values |
| Missing field | Skipped (undefined not included) |
| `null` field | Included as distinct value |
| Empty collection | Returns `[]` |
| Result size limit | Must fit in BSON max size (not applicable to MangoDB) |

### drop()

| Behavior | Details |
|----------|---------|
| Non-existent collection | Returns `true` (MongoDB 5.0+ behavior) |
| Drops indexes | All indexes removed with collection |
| Admin/config db restriction | MongoDB 5.0+ restricts on mongos (N/A for MangoDB) |

### rename()

| Behavior | Details |
|----------|---------|
| Same name | Error: "cannot rename collection to itself" |
| Target exists | Error unless `dropTarget: true` |
| Source missing | Error code 26: NamespaceNotFound |
| Preserves data | All documents moved to new name |
| Preserves indexes | All indexes preserved |

### listCollections()

| Behavior | Details |
|----------|---------|
| Returns cursor | Must call `.toArray()` or iterate |
| filter support | Can filter by name or regex |
| nameOnly | Returns simplified documents |
| Empty db | Returns empty cursor |

---

## Sources

- [MongoDB distinct() documentation](https://www.mongodb.com/docs/manual/reference/method/db.collection.distinct/)
- [MongoDB drop() documentation](https://www.mongodb.com/docs/manual/reference/method/db.collection.drop/)
- [MongoDB renameCollection() documentation](https://www.mongodb.com/docs/manual/reference/method/db.collection.renamecollection/)
- [MongoDB listCollections documentation](https://www.mongodb.com/docs/manual/reference/command/listcollections/)
- [MongoDB estimatedDocumentCount() documentation](https://www.mongodb.com/docs/manual/reference/method/db.collection.estimateddocumentcount/)
- [MongoDB collection.stats() documentation](https://www.mongodb.com/docs/manual/reference/method/db.collection.stats/)
- [MongoDB db.stats() documentation](https://www.mongodb.com/docs/manual/reference/method/db.stats/)
- [MongoDB Node.js Driver documentation](https://www.mongodb.com/docs/drivers/node/current/databases-collections/)
