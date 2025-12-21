# Phase 9: Aggregation Pipeline (Basic) - Implementation Plan

## Overview

Implement the MongoDB aggregation framework with commonly-used stages. This is the highest priority remaining feature for MongoDB compatibility.

**Estimated Test Cases**: 80-100

---

## Implementation Steps

### Step 1: Create Types for Aggregation Pipeline

**File**: `src/types.ts`

Add the following types:

```typescript
// Pipeline stage types
export interface MatchStage {
  $match: Filter<Document>;
}

export interface ProjectStage {
  $project: Record<string, 0 | 1 | string | ProjectExpression>;
}

export interface SortStage {
  $sort: Record<string, 1 | -1>;
}

export interface LimitStage {
  $limit: number;
}

export interface SkipStage {
  $skip: number;
}

export interface CountStage {
  $count: string;
}

export interface UnwindStage {
  $unwind: string | UnwindOptions;
}

export interface UnwindOptions {
  path: string;
  preserveNullAndEmptyArrays?: boolean;
  includeArrayIndex?: string;
}

// Union type for all pipeline stages
export type PipelineStage =
  | MatchStage
  | ProjectStage
  | SortStage
  | LimitStage
  | SkipStage
  | CountStage
  | UnwindStage;

// Aggregation options
export interface AggregateOptions {
  // Future: allowDiskUse, batchSize, etc.
}

// Project expression (for computed fields)
export interface ProjectExpression {
  $literal?: unknown;
}
```

---

### Step 2: Create Aggregation Module

**File**: `src/aggregation.ts` (NEW)

Create a new module containing:

1. **`AggregationCursor<T>` class** - Similar to MangoDBCursor but for pipelines
   - Constructor takes pipeline stages and a document source function
   - `toArray()` - Execute pipeline and return results
   - `next()` - Get next document (optional, for future)
   - `forEach(callback)` - Iterate with callback (optional)

2. **Pipeline execution logic**:
   - Execute stages sequentially
   - Each stage transforms the document stream
   - Validate stage names (throw for unknown stages)

```typescript
// Skeleton structure
import { matchesFilter } from "./query-matcher.ts";
import { applyProjection, compareValuesForSort, getValueByPath } from "./utils.ts";
import type { Document, Filter, PipelineStage } from "./types.ts";

export class AggregationCursor<T extends Document = Document> {
  private pipeline: PipelineStage[];
  private source: () => Promise<T[]>;

  constructor(source: () => Promise<T[]>, pipeline: PipelineStage[]) {
    this.source = source;
    this.pipeline = pipeline;
  }

  async toArray(): Promise<Document[]> {
    let documents: Document[] = await this.source();

    for (const stage of this.pipeline) {
      documents = await this.executeStage(stage, documents);
    }

    return documents;
  }

  private async executeStage(stage: PipelineStage, docs: Document[]): Promise<Document[]> {
    const stageKey = Object.keys(stage)[0];

    switch (stageKey) {
      case '$match':
        return this.execMatch(stage.$match, docs);
      case '$project':
        return this.execProject(stage.$project, docs);
      case '$sort':
        return this.execSort(stage.$sort, docs);
      case '$limit':
        return this.execLimit(stage.$limit, docs);
      case '$skip':
        return this.execSkip(stage.$skip, docs);
      case '$count':
        return this.execCount(stage.$count, docs);
      case '$unwind':
        return this.execUnwind(stage.$unwind, docs);
      default:
        throw new Error(`Unrecognized pipeline stage name: '${stageKey}'`);
    }
  }

  // Stage implementations...
}
```

---

### Step 3: Implement Individual Stages

#### 3.1: `$match` Stage
- Reuse existing `matchesFilter()` from query-matcher.ts
- Filter documents that match the query
- Supports all existing query operators

```typescript
private execMatch(filter: Filter<Document>, docs: Document[]): Document[] {
  return docs.filter(doc => matchesFilter(doc, filter));
}
```

#### 3.2: `$project` Stage
- Include fields: `{ field: 1 }`
- Exclude fields: `{ field: 0 }`
- Rename/computed fields: `{ newName: "$existingField" }`
- `_id` included by default unless explicitly excluded
- **Cannot mix inclusion and exclusion** (except `_id`)
- Handle `$literal` for numeric/boolean literals

```typescript
private execProject(projection: Record<string, unknown>, docs: Document[]): Document[] {
  // Validate: cannot mix 1 and 0 except for _id
  // Handle field references ($fieldName)
  // Handle $literal expressions
  // Reuse applyProjection for simple cases
}
```

**Edge Cases**:
- Empty `$project: {}` should throw an error
- `{ name: 1, age: 0 }` is invalid (mixing modes)
- `{ name: 1, _id: 0 }` is valid (special case)
- `{ value: { $literal: 1 } }` returns literal value 1, not inclusion

#### 3.3: `$sort` Stage
- Reuse existing sort logic from cursor/utils
- Ascending (1) and descending (-1)
- Compound sort support

```typescript
private execSort(sortSpec: Record<string, 1 | -1>, docs: Document[]): Document[] {
  const sortFields = Object.entries(sortSpec);
  return [...docs].sort((a, b) => {
    for (const [field, direction] of sortFields) {
      const aValue = getValueByPath(a, field);
      const bValue = getValueByPath(b, field);
      const cmp = compareValuesForSort(aValue, bValue, direction);
      if (cmp !== 0) return direction === 1 ? cmp : -cmp;
    }
    return 0;
  });
}
```

#### 3.4: `$limit` Stage
- Limit output to first n documents
- Simple slice operation

```typescript
private execLimit(limit: number, docs: Document[]): Document[] {
  if (limit < 0) throw new Error("$limit must be non-negative");
  return docs.slice(0, limit);
}
```

#### 3.5: `$skip` Stage
- Skip first n documents
- Pipeline position matters (unlike cursor where order doesn't matter)

```typescript
private execSkip(skip: number, docs: Document[]): Document[] {
  if (skip < 0) throw new Error("$skip must be non-negative");
  return docs.slice(skip);
}
```

#### 3.6: `$count` Stage
- Returns count with specified field name
- **Critical**: Returns empty array if no documents (not `{ count: 0 }`)
- Field name validation:
  - Must be non-empty string
  - Cannot start with `$`
  - Cannot contain `.`

```typescript
private execCount(fieldName: string, docs: Document[]): Document[] {
  if (!fieldName || fieldName.length === 0) {
    throw new Error("$count field name must be non-empty");
  }
  if (fieldName.startsWith('$')) {
    throw new Error("$count field name cannot start with '$'");
  }
  if (fieldName.includes('.')) {
    throw new Error("$count field name cannot contain '.'");
  }

  // Empty input returns NO document
  if (docs.length === 0) {
    return [];
  }

  return [{ [fieldName]: docs.length }];
}
```

#### 3.7: `$unwind` Stage
- Deconstruct array field into multiple documents
- Two syntaxes:
  - Short: `{ $unwind: "$arrayField" }`
  - Long: `{ $unwind: { path: "$arrayField", ... } }`

**Behavior by input type**:
| Input Type | Default | With preserveNullAndEmptyArrays |
|------------|---------|--------------------------------|
| Array with items | One doc per element | Same |
| Non-array value | Treated as single-element array | Same |
| null | No output | Doc with null field |
| Missing field | No output | Doc with missing field |
| Empty array [] | No output | Doc included |

```typescript
private execUnwind(unwind: string | UnwindOptions, docs: Document[]): Document[] {
  const path = typeof unwind === 'string' ? unwind : unwind.path;
  const preserveNullAndEmpty = typeof unwind === 'object' && unwind.preserveNullAndEmptyArrays;
  const includeArrayIndex = typeof unwind === 'object' ? unwind.includeArrayIndex : undefined;

  // Path must start with $
  if (!path.startsWith('$')) {
    throw new Error("$unwind path must start with '$'");
  }

  const fieldPath = path.slice(1); // Remove $ prefix
  const result: Document[] = [];

  for (const doc of docs) {
    const value = getValueByPath(doc, fieldPath);

    if (value === undefined || value === null) {
      if (preserveNullAndEmpty) {
        result.push({ ...doc }); // Keep original doc
      }
      // Otherwise skip
      continue;
    }

    if (!Array.isArray(value)) {
      // Non-array treated as single-element array
      const newDoc = { ...doc };
      if (includeArrayIndex) {
        newDoc[includeArrayIndex] = 0;
      }
      result.push(newDoc);
      continue;
    }

    if (value.length === 0) {
      if (preserveNullAndEmpty) {
        result.push({ ...doc });
      }
      continue;
    }

    // Unwind array
    for (let i = 0; i < value.length; i++) {
      const newDoc = { ...doc };
      setValueByPath(newDoc, fieldPath, value[i]);
      if (includeArrayIndex) {
        newDoc[includeArrayIndex] = i;
      }
      result.push(newDoc);
    }
  }

  return result;
}
```

---

### Step 4: Add aggregate() Method to Collection

**File**: `src/collection.ts`

Add the aggregate method:

```typescript
import { AggregationCursor } from "./aggregation.ts";
import type { PipelineStage, AggregateOptions } from "./types.ts";

// In MangoDBCollection class:

/**
 * Execute an aggregation pipeline on the collection.
 */
aggregate(pipeline: PipelineStage[], options?: AggregateOptions): AggregationCursor<T> {
  return new AggregationCursor<T>(
    () => this.readDocuments(),
    pipeline
  );
}
```

---

### Step 5: Update Exports

**File**: `src/index.ts`

Add exports for aggregation:

```typescript
export { AggregationCursor } from "./aggregation.ts";
export type {
  PipelineStage,
  MatchStage,
  ProjectStage,
  SortStage,
  LimitStage,
  SkipStage,
  CountStage,
  UnwindStage,
  UnwindOptions,
  AggregateOptions
} from "./types.ts";
```

---

### Step 6: Update Test Harness

**File**: `test/test-harness.ts`

Add aggregation support to test interfaces:

```typescript
export interface AggregationCursor<T> {
  toArray(): Promise<T[]>;
}

export interface TestCollection<T extends Document = Document> {
  // ... existing methods ...
  aggregate(pipeline: Document[]): AggregationCursor<T>;
}
```

---

### Step 7: Create Test File

**File**: `test/aggregation.test.ts` (NEW)

Structure:

```
test/aggregation.test.ts
├── Aggregation Pipeline Tests (${getTestModeName()})
│   ├── Pipeline Infrastructure
│   │   ├── should return empty array for empty collection
│   │   ├── should return all documents with empty pipeline
│   │   ├── should throw for invalid/unknown stage
│   │   ├── should execute stages in order
│   │   └── should handle multiple stages
│   │
│   ├── $match Stage
│   │   ├── should filter documents by equality
│   │   ├── should support comparison operators ($gt, $lt, etc.)
│   │   ├── should support logical operators ($and, $or)
│   │   ├── should support array operators ($in, $all, $elemMatch)
│   │   ├── should return all docs with empty match {}
│   │   ├── should support dot notation for nested fields
│   │   └── should handle null/undefined values
│   │
│   ├── $project Stage
│   │   ├── Inclusion Mode
│   │   │   ├── should include specified fields only
│   │   │   ├── should include _id by default
│   │   │   ├── should allow excluding _id with { _id: 0 }
│   │   │   ├── should handle nested fields with dot notation
│   │   │   └── should handle missing fields gracefully
│   │   │
│   │   ├── Exclusion Mode
│   │   │   ├── should exclude specified fields
│   │   │   ├── should include all other fields
│   │   │   └── should handle nested field exclusion
│   │   │
│   │   ├── Field Renaming
│   │   │   ├── should rename fields using $fieldName syntax
│   │   │   ├── should handle nested field references
│   │   │   └── should return null for missing referenced field
│   │   │
│   │   ├── $literal Expression
│   │   │   ├── should return literal numeric value
│   │   │   └── should return literal boolean value
│   │   │
│   │   └── Error Cases
│   │       └── should throw when mixing inclusion and exclusion
│   │
│   ├── $sort Stage
│   │   ├── should sort ascending by numeric field
│   │   ├── should sort descending by numeric field
│   │   ├── should sort by string field (lexicographic)
│   │   ├── should sort by date field
│   │   ├── should handle compound sort (multiple fields)
│   │   ├── should handle null/missing values in sort
│   │   └── should handle array fields in sort (min/max element)
│   │
│   ├── $limit Stage
│   │   ├── should limit results to n documents
│   │   ├── should return all if limit exceeds document count
│   │   ├── should return empty array for limit 0
│   │   └── should work in middle of pipeline
│   │
│   ├── $skip Stage
│   │   ├── should skip first n documents
│   │   ├── should return empty if skip exceeds document count
│   │   ├── should work with limit (pagination)
│   │   └── should handle skip 0 (no-op)
│   │
│   ├── $count Stage
│   │   ├── should count all documents in collection
│   │   ├── should count after $match filter
│   │   ├── should return single document with count field
│   │   ├── should return empty array for empty input (not { count: 0 })
│   │   ├── should throw for empty field name
│   │   ├── should throw for field name starting with $
│   │   └── should throw for field name containing .
│   │
│   ├── $unwind Stage
│   │   ├── Basic Unwind
│   │   │   ├── should unwind array into multiple documents
│   │   │   ├── should handle single-element arrays
│   │   │   ├── should skip documents with missing field
│   │   │   ├── should skip documents with null field
│   │   │   ├── should skip documents with empty array
│   │   │   ├── should treat non-array value as single-element array
│   │   │   └── should handle nested array fields with dot notation
│   │   │
│   │   ├── preserveNullAndEmptyArrays Option
│   │   │   ├── should preserve documents with missing field
│   │   │   ├── should preserve documents with null field
│   │   │   └── should preserve documents with empty array
│   │   │
│   │   ├── includeArrayIndex Option
│   │   │   ├── should add index field to each unwound document
│   │   │   ├── should start index at 0
│   │   │   └── should work with preserveNullAndEmptyArrays
│   │   │
│   │   └── Error Cases
│   │       └── should throw for path not starting with $
│   │
│   └── Combined Pipeline Tests
│       ├── should execute $match -> $sort -> $limit
│       ├── should execute $match -> $project
│       ├── should execute $unwind -> $group (prep for Phase 10)
│       ├── should handle complex multi-stage pipeline
│       └── should preserve document order through stages
```

---

## Implementation Order

1. **Types** (`src/types.ts`) - Add all pipeline stage types
2. **Aggregation module** (`src/aggregation.ts`) - Create AggregationCursor class with stage framework
3. **$match stage** - Simplest, reuses existing code
4. **$sort stage** - Reuses existing sort logic
5. **$limit stage** - Simple slice
6. **$skip stage** - Simple slice
7. **$count stage** - Simple but with edge cases
8. **$project stage** - More complex, field renaming
9. **$unwind stage** - Most complex, array handling
10. **Collection method** (`src/collection.ts`) - Add aggregate()
11. **Exports** (`src/index.ts`) - Export new types and classes
12. **Test harness** (`test/test-harness.ts`) - Add aggregate interface
13. **Tests** (`test/aggregation.test.ts`) - Comprehensive test suite

---

## File Changes Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `src/types.ts` | Modify | Add PipelineStage types |
| `src/aggregation.ts` | **New** | AggregationCursor class |
| `src/collection.ts` | Modify | Add aggregate() method |
| `src/index.ts` | Modify | Export aggregation types/classes |
| `test/test-harness.ts` | Modify | Add aggregate to TestCollection |
| `test/aggregation.test.ts` | **New** | Aggregation tests |

---

## Error Messages Reference

| Condition | Error Message |
|-----------|---------------|
| Unknown stage | `Unrecognized pipeline stage name: '$unknown'` |
| $count empty name | `$count field name must be non-empty` |
| $count starts with $ | `$count field name cannot start with '$'` |
| $count contains . | `$count field name cannot contain '.'` |
| $unwind path no $ | `$unwind path must start with '$'` |
| $project mixing | `Cannot mix inclusion and exclusion in projection` |
| $limit negative | `$limit must be non-negative` |
| $skip negative | `$skip must be non-negative` |

---

## Critical Behaviors to Verify

1. **$count on empty input** returns `[]` not `[{ count: 0 }]`
2. **$unwind on non-array** treats value as single-element array
3. **$project cannot mix** inclusion (1) and exclusion (0) except for `_id`
4. **$unwind with preserveNullAndEmptyArrays** keeps docs with null/missing/empty
5. **Pipeline order matters** - stages execute sequentially

---

## Documentation Updates After Implementation

1. Update PROGRESS.md with Phase 9 completion
2. Update README.md "What's Implemented" section
3. Add aggregation behaviors to COMPATIBILITY.md
4. Update test count in documentation

---

## Test Execution

```bash
# Run only aggregation tests (MangoDB)
npm test -- --test-name-pattern "Aggregation"

# Run against real MongoDB
MONGODB_URI="mongodb://localhost:27017" npm test -- --test-name-pattern "Aggregation"
```
