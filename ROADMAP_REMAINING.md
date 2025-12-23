# MangoDB: Remaining Work Roadmap

This document outlines the remaining implementation phases for MangoDB, following the established project methodology.

---

## Implementation Methodology Summary

The project follows a rigorous development process:

1. **Phase-Based Development**: Work organized into focused phases with clear goals
2. **Test-Driven Development (TDD)**: Write tests against real MongoDB first, then implement in MangoDB
3. **Dual-Target Testing**: Tests run against both MongoDB and MangoDB via the test harness
4. **Detailed Planning**: Each phase has comprehensive planning with test cases, edge cases, and error formats
5. **Behavior Documentation**: Discovered behaviors recorded in COMPATIBILITY.md
6. **Incremental Progress**: Each phase builds on previous work; complexity increases gradually

---

## Current Codebase Structure (Post-Refactoring)

The codebase has been refactored into focused modules:

```
src/
├── index.ts              # Public API exports
├── client.ts             # MangoDBClient entry point
├── db.ts                 # Database abstraction
├── collection.ts         # MangoDBCollection (uses modules below)
├── cursor.ts             # Query cursor with sort/limit/skip/projection
├── types.ts              # All type definitions (QueryOperators, Filter, etc.)
├── query-matcher.ts      # Query matching logic (matchesFilter, matchesOperators)
├── update-operators.ts   # Update operator application (applyUpdateOperators)
├── index-manager.ts      # IndexManager class (createIndex, uniqueness checks)
├── document-utils.ts     # Document utilities (getValueByPath, cloneDocument, etc.)
├── errors.ts             # Error classes (MongoDuplicateKeyError, etc.)
└── utils.ts              # Sorting and projection utilities
```

**Where to implement new features:**

| Feature Type | Target Module |
|--------------|---------------|
| Query operators ($regex, $type, $mod) | `query-matcher.ts` + `types.ts` |
| Update operators ($min, $max, $mul) | `update-operators.ts` + `types.ts` |
| Aggregation pipeline | New `aggregation.ts` module |
| Error classes | `errors.ts` |
| Index features (sparse, TTL, partial) | `index-manager.ts` + `types.ts` |
| Admin operations (distinct, drop) | `collection.ts` / `db.ts` |

---

## Current State (Phases 1-12 Complete)

| Phase | Feature | Status | Test Cases |
|-------|---------|--------|------------|
| 1 | Foundation (CRUD) | ✅ Complete | 32 |
| 2 | Basic Queries | ✅ Complete | 60 |
| 3 | Updates | ✅ Complete | 49 |
| 4 | Cursor Operations | ✅ Complete | 76 |
| 5 | Logical Operators | ✅ Complete | 50 |
| 6 | Array Handling | ✅ Complete | 86 |
| 7 | Indexes | ✅ Complete | 39 |
| 8 | Advanced Operations | ✅ Complete | 51 |
| 9 | Aggregation Pipeline (Basic) | ✅ Complete | 70 |
| 10 | Aggregation Pipeline (Advanced) | ✅ Complete | 57 |
| 11 | Regular Expressions | ✅ Complete | 38 |
| 12 | Additional Query Operators | ✅ Complete | 67 |
| **Total** | | | **675** |

**Approximate MongoDB Coverage**: 90%+ of common operations

---

## Remaining Phases Overview

| Phase | Feature | Priority | Effort | Est. Tests |
|-------|---------|----------|--------|------------|
| 12.5 | Find Options Parity | **High** | Small | 8-12 |
| 13 | Additional Update Operators | Medium | Small | 30-40 |
| 14 | Extended Index Features | Low | Medium | 25-30 |
| 15 | Administrative Operations | Low | Small | 15-20 |
| 16 | Extended Expression Operators | Low | Medium | 50-70 |

**Total Remaining**: ~130-170 additional test cases

---

## Phase 9: Aggregation Pipeline (Basic) ✅ COMPLETE

**Goal**: Implement the aggregation framework with commonly-used stages.

**Status**: COMPLETE — All basic pipeline stages implemented and tested.

### Overview

The aggregation pipeline processes documents through a sequence of stages, each transforming the document stream.

```typescript
const results = await collection.aggregate([
  { $match: { status: "active" } },
  { $sort: { createdAt: -1 } },
  { $limit: 10 }
]).toArray();
```

### Operations

#### Step 1: Pipeline Infrastructure
- [x] `collection.aggregate(pipeline)` method
- [x] `AggregationCursor` class with `toArray()`, `next()`, `forEach()`
- [x] Pipeline stage execution framework
- [x] Stage validation

#### Step 2: `$match` Stage
- [x] Filter documents using query syntax (reuse existing `matchesFilter` from `query-matcher.ts`)
- [x] Support all existing query operators
- [x] Works identically to `find()` filter

**Restrictions (from MongoDB docs)**:
- Cannot use raw aggregation expressions - must wrap in `$expr`
- Cannot use `$where` operator in `$match`
- `$text` operator must be first stage if used

**Test Cases**:
```typescript
// Basic match
await collection.aggregate([{ $match: { status: "active" } }]).toArray();

// With query operators
await collection.aggregate([{ $match: { age: { $gte: 18 } } }]).toArray();

// With logical operators
await collection.aggregate([{ $match: { $or: [{ a: 1 }, { b: 2 }] } }]).toArray();
```

#### Step 3: `$project` Stage
- [x] Include fields: `{ $project: { name: 1, email: 1 } }`
- [x] Exclude fields: `{ $project: { password: 0 } }`
- [x] Rename fields: `{ $project: { userName: "$name" } }`
- [x] Computed fields with expressions (basic)
- [x] Nested field projection

**Critical Restrictions (from MongoDB docs)**:
- **Cannot mix inclusion (1) and exclusion (0)** except for `_id`
- `_id` is ALWAYS included by default unless explicitly set to 0
- Use `$literal` for numeric/boolean literals: `{ value: { $literal: 1 } }`
- Empty `$project: {}` returns an error

**Error Cases**:
```typescript
// ERROR - mixing include and exclude
{ $project: { name: 1, age: 0 } }  // Invalid!

// OK - _id exception
{ $project: { name: 1, _id: 0 } }  // Valid
```

**Test Cases**:
```typescript
// Include fields
await collection.aggregate([{ $project: { name: 1, age: 1 } }]).toArray();

// Exclude _id
await collection.aggregate([{ $project: { _id: 0, name: 1 } }]).toArray();

// Rename field using $ prefix
await collection.aggregate([{ $project: { fullName: "$name" } }]).toArray();

// Nested fields
await collection.aggregate([{ $project: { "user.name": 1 } }]).toArray();
```

#### Step 4: `$sort` Stage
- [x] Ascending/descending sort
- [x] Compound sort
- [x] Reuse existing sort logic from cursors

**Test Cases**:
```typescript
await collection.aggregate([{ $sort: { createdAt: -1 } }]).toArray();
await collection.aggregate([{ $sort: { category: 1, name: -1 } }]).toArray();
```

#### Step 5: `$limit` and `$skip` Stages
- [x] `$limit: n` — Limit output documents
- [x] `$skip: n` — Skip first n documents
- [x] Position in pipeline matters (unlike cursor chaining)

**Test Cases**:
```typescript
await collection.aggregate([{ $limit: 5 }]).toArray();
await collection.aggregate([{ $skip: 10 }, { $limit: 5 }]).toArray();
```

#### Step 6: `$count` Stage
- [x] Returns count as specified field name
- [x] `{ $count: "totalDocs" }` → `{ totalDocs: 123 }`

**Critical Behavior (from MongoDB docs)**:
- Field name must be **non-empty string**
- Field name must **NOT start with `$`**
- Field name must **NOT contain `.` (dot)**
- **If input is empty, $count returns NO document** (not `{ count: 0 }`)

**Test Cases**:
```typescript
await collection.aggregate([
  { $match: { status: "active" } },
  { $count: "activeCount" }
]).toArray();
// Returns: [{ activeCount: 42 }]

// Empty collection - returns empty array, NOT [{ count: 0 }]
await emptyCollection.aggregate([{ $count: "total" }]).toArray();
// Returns: []
```

#### Step 7: `$unwind` Stage
- [x] Deconstruct array field into multiple documents
- [x] `preserveNullAndEmptyArrays` option
- [x] `includeArrayIndex` option

**Behavior by Input Type (from MongoDB docs)**:

| Input Type | Default Behavior | With `preserveNullAndEmptyArrays: true` |
|------------|------------------|----------------------------------------|
| Populated array | One doc per element | Same |
| **Non-array value** | **Treated as single-element array** | Same |
| null | No output (doc excluded) | Doc included (field is null) |
| Missing field | No output (doc excluded) | Doc included (field missing) |
| Empty array `[]` | No output (doc excluded) | Doc included |

**Important**: Non-array operands are treated as single-element arrays (MongoDB 3.2+)

**Test Cases**:
```typescript
// Given: { name: "Alice", tags: ["a", "b", "c"] }
await collection.aggregate([{ $unwind: "$tags" }]).toArray();
// Returns 3 documents, each with one tag

// With options
await collection.aggregate([{
  $unwind: {
    path: "$items",
    preserveNullAndEmptyArrays: true,
    includeArrayIndex: "idx"
  }
}]).toArray();

// Non-array treated as single-element array
// Given: { value: "scalar" }
await collection.aggregate([{ $unwind: "$value" }]).toArray();
// Returns: [{ value: "scalar" }]
```

### Implementation Notes

**New file: `src/aggregation.ts`**

```typescript
// src/aggregation.ts - New module for aggregation pipeline
import { matchesFilter } from "./query-matcher.ts";
import { sortDocuments, applyProjection } from "./utils.ts";
import type { Document, Filter } from "./types.ts";

export interface PipelineStage {
  $match?: Filter<Document>;
  $project?: Record<string, 0 | 1 | string>;
  $sort?: Record<string, 1 | -1>;
  $limit?: number;
  $skip?: number;
  $count?: string;
  $unwind?: string | { path: string; preserveNullAndEmptyArrays?: boolean; includeArrayIndex?: string };
}

export class AggregationCursor<T extends Document> {
  private pipeline: PipelineStage[];
  private collection: MangoDBCollection<T>;

  async toArray(): Promise<T[]> {
    let documents = await this.collection.find({}).toArray();
    for (const stage of this.pipeline) {
      documents = await this.executeStage(stage, documents);
    }
    return documents;
  }

  private async executeStage(stage: PipelineStage, docs: T[]): Promise<T[]> {
    if ('$match' in stage) {
      // Reuse existing matchesFilter from query-matcher.ts
      return docs.filter(doc => matchesFilter(doc, stage.$match!));
    }
    if ('$project' in stage) return this.execProject(stage.$project!, docs);
    if ('$sort' in stage) {
      // Reuse existing sortDocuments from utils.ts
      return sortDocuments(docs, stage.$sort!);
    }
    if ('$limit' in stage) return docs.slice(0, stage.$limit);
    if ('$skip' in stage) return docs.slice(stage.$skip);
    if ('$count' in stage) return this.execCount(stage.$count!, docs);
    if ('$unwind' in stage) return this.execUnwind(stage.$unwind!, docs);
    throw new Error(`Unrecognized pipeline stage name: '${Object.keys(stage)[0]}'`);
  }
}
```

**Types to add to `src/types.ts`**:
- `PipelineStage` interface
- `AggregationOptions` interface

### Test File Structure

```
test/aggregation-basic.test.ts
├── Aggregation Pipeline Tests (${getTestModeName()})
│   ├── Pipeline Infrastructure
│   │   ├── should return empty array for empty collection
│   │   ├── should return all documents with empty pipeline
│   │   ├── should throw for invalid stage
│   │   └── should execute stages in order
│   │
│   ├── $match Stage
│   │   ├── should filter documents by equality
│   │   ├── should support comparison operators
│   │   ├── should support logical operators
│   │   ├── should support array operators
│   │   └── should return all docs with empty match
│   │
│   ├── $project Stage
│   │   ├── should include specified fields
│   │   ├── should exclude specified fields
│   │   ├── should include _id by default
│   │   ├── should allow excluding _id
│   │   ├── should rename fields using $field syntax
│   │   ├── should handle nested fields
│   │   └── should handle missing fields
│   │
│   ├── $sort Stage
│   │   ├── should sort ascending
│   │   ├── should sort descending
│   │   ├── should handle compound sort
│   │   └── should handle null/missing values
│   │
│   ├── $limit Stage
│   │   ├── should limit results
│   │   ├── should return all if limit exceeds count
│   │   └── should return empty for limit 0
│   │
│   ├── $skip Stage
│   │   ├── should skip documents
│   │   ├── should return empty if skip exceeds count
│   │   └── should work with limit
│   │
│   ├── $count Stage
│   │   ├── should count all documents
│   │   ├── should count after match
│   │   └── should return single document with count
│   │
│   └── $unwind Stage
│       ├── should unwind array into multiple docs
│       ├── should skip docs with missing field
│       ├── should skip docs with empty array
│       ├── should preserve with preserveNullAndEmptyArrays
│       └── should include index with includeArrayIndex
```

### Documentation Updates

After implementation:
1. Add Phase 9 to PROGRESS.md
2. Update ROADMAP.md current phase
3. Add aggregation behaviors to COMPATIBILITY.md
4. Update README.md "What's Implemented"

---

## Phase 10: Aggregation Pipeline (Advanced) ✅ COMPLETE

**Goal**: Add grouping, lookup, and expression operators to the aggregation framework.

**Status**: COMPLETE — All advanced pipeline stages and expression operators implemented.

### Operations

#### Step 1: `$group` Stage
- [x] Group documents by `_id` expression
- [x] Accumulator operators: `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`
- [x] `$push` and `$addToSet` accumulators
- [x] `$count` accumulator (via `$sum: 1`)

**Test Cases**:
```typescript
// Group and count
await collection.aggregate([
  { $group: { _id: "$category", count: { $sum: 1 } } }
]).toArray();

// Group with multiple accumulators
await collection.aggregate([
  { $group: {
    _id: "$department",
    totalSalary: { $sum: "$salary" },
    avgSalary: { $avg: "$salary" },
    employees: { $push: "$name" }
  }}
]).toArray();

// Group all documents (null _id)
await collection.aggregate([
  { $group: { _id: null, total: { $sum: 1 } } }
]).toArray();
```

#### Step 2: `$lookup` Stage (Basic)
- [x] Left outer join with another collection
- [x] `from`, `localField`, `foreignField`, `as` fields
- [x] Returns array of matching documents

**Test Cases**:
```typescript
// Basic lookup
await orders.aggregate([
  { $lookup: {
    from: "products",
    localField: "productId",
    foreignField: "_id",
    as: "product"
  }}
]).toArray();

// Lookup with no matches (empty array)
// Lookup with multiple matches
```

#### Step 3: `$addFields` Stage
- [x] Add new fields to documents
- [x] Does not remove existing fields
- [x] Can reference existing fields with `$fieldName`

**Test Cases**:
```typescript
await collection.aggregate([
  { $addFields: { fullName: { $concat: ["$firstName", " ", "$lastName"] } } }
]).toArray();
```

#### Step 4: `$set` Stage (alias for $addFields)
- [x] Alias for `$addFields`
- [x] Same functionality

#### Step 5: `$replaceRoot` Stage
- [x] Replace document with specified embedded document
- [x] `newRoot` expression required

**Test Cases**:
```typescript
await collection.aggregate([
  { $replaceRoot: { newRoot: "$address" } }
]).toArray();
```

#### Step 6: `$out` Stage
- [x] Write results to a collection
- [x] Must be last stage in pipeline
- [x] Replaces collection if exists

**Test Cases**:
```typescript
await collection.aggregate([
  { $match: { active: true } },
  { $out: "activeUsers" }
]).toArray();
```

#### Step 7: Expression Operators (Basic Set)
- [x] `$concat` — Concatenate strings
- [x] `$add`, `$subtract`, `$multiply`, `$divide` — Arithmetic
- [x] `$cond` — Conditional expression
- [x] `$ifNull` — Null coalescing
- [x] `$toUpper`, `$toLower` — String case
- [x] `$size` — Array size (expression version)

### Test File Structure

```
test/aggregation-advanced.test.ts
├── $group Stage
│   ├── should group by single field
│   ├── should group by compound _id
│   ├── should handle $sum accumulator
│   ├── should handle $avg accumulator
│   ├── should handle $min/$max accumulators
│   ├── should handle $push accumulator
│   ├── should handle $addToSet accumulator
│   ├── should group all with null _id
│   └── should handle empty groups
│
├── $lookup Stage
│   ├── should join collections
│   ├── should return empty array for no match
│   ├── should return multiple matches in array
│   └── should handle missing foreign collection
│
├── $addFields / $set Stage
│   ├── should add new field
│   ├── should reference existing fields
│   ├── should preserve existing fields
│   └── should overwrite existing field
│
├── $replaceRoot Stage
│   ├── should replace with embedded document
│   ├── should throw for missing newRoot field
│   └── should handle null/missing embedded doc
│
├── $out Stage
│   ├── should write results to collection
│   ├── should replace existing collection
│   └── should be last stage only
│
└── Expression Operators
    ├── $concat tests
    ├── Arithmetic operator tests
    ├── $cond tests
    └── String operator tests
```

---

## Phase 11: Regular Expressions ✅ COMPLETE

**Goal**: Support regex matching in queries.

**Status**: COMPLETE — All regex matching features implemented and tested.

### Operations

#### Step 1: `$regex` Query Operator
- [x] Basic pattern matching: `{ field: { $regex: "pattern" } }`
- [x] With options: `{ field: { $regex: "pattern", $options: "i" } }`
- [x] JavaScript RegExp support: `{ field: /pattern/i }`
- [x] Options: `i` (case-insensitive), `m` (multiline), `s` (dotAll)

**Test Cases**:
```typescript
// Basic regex
await collection.find({ name: { $regex: "^A" } }).toArray();

// Case insensitive
await collection.find({ email: { $regex: "@gmail\\.com$", $options: "i" } }).toArray();

// JavaScript RegExp
await collection.find({ name: /alice/i }).toArray();

// In aggregation $match
await collection.aggregate([
  { $match: { description: { $regex: "urgent", $options: "i" } } }
]).toArray();
```

#### Step 2: Regex in Array Fields
- [x] Match array elements containing pattern
- [x] Works with `$elemMatch`

**Test Cases**:
```typescript
// Array element matching
await collection.find({ tags: { $regex: "^prod" } }).toArray();

// With $elemMatch
await collection.find({
  items: { $elemMatch: { name: { $regex: "widget" } } }
}).toArray();
```

#### Step 3: Regex in `$in` Operator
- [x] Support regex patterns in `$in` array
- [x] Mix of exact values and patterns

**Test Cases**:
```typescript
await collection.find({
  status: { $in: [/^active/, "pending", /^review/] }
}).toArray();
```

### Implementation Notes

```typescript
// Extend QueryOperators interface
interface QueryOperators {
  // ... existing
  $regex?: string | RegExp;
  $options?: string;  // Only valid with $regex
}

// In matchesOperators():
if (ops.$regex !== undefined) {
  const pattern = ops.$regex;
  const options = ops.$options || '';
  const regex = new RegExp(pattern, options);
  return typeof fieldValue === 'string' && regex.test(fieldValue);
}
```

### Test File Structure

```
test/regex.test.ts
├── $regex Query Operator
│   ├── Basic Pattern Matching
│   │   ├── should match start of string with ^
│   │   ├── should match end of string with $
│   │   ├── should match anywhere in string
│   │   ├── should handle special characters
│   │   └── should not match non-string fields
│   │
│   ├── Options
│   │   ├── should support case-insensitive (i)
│   │   ├── should support multiline (m)
│   │   ├── should support dotAll (s)
│   │   └── should combine multiple options
│   │
│   ├── JavaScript RegExp
│   │   ├── should accept RegExp object
│   │   ├── should respect RegExp flags
│   │   └── should work in filter
│   │
│   ├── Array Fields
│   │   ├── should match array element
│   │   ├── should not match if no element matches
│   │   └── should work with $elemMatch
│   │
│   ├── With $in Operator
│   │   ├── should match regex in $in array
│   │   ├── should mix regex and exact values
│   │   └── should match if any regex matches
│   │
│   └── In Aggregation
│       ├── should work in $match stage
│       └── should work with other operators
```

### Error Messages

| Condition | Error Message |
|-----------|---------------|
| Invalid regex pattern | `$regex has to be a string` |
| $options without $regex | `$options needs a $regex` |
| Invalid options | `invalid flag in regex options: x` |

---

## Phase 12: Additional Query Operators

**Goal**: Implement remaining useful query operators.

**Priority**: MEDIUM — Enhances query expressiveness.

### Operations

#### Step 1: `$type` Operator
- [ ] Match by BSON type
- [ ] Support type aliases: `"string"`, `"number"`, `"object"`, `"array"`, `"bool"`, `"null"`
- [ ] Support numeric type codes

**Test Cases**:
```typescript
await collection.find({ value: { $type: "string" } }).toArray();
await collection.find({ data: { $type: "array" } }).toArray();
await collection.find({ flag: { $type: "bool" } }).toArray();
```

#### Step 2: `$mod` Operator
- [ ] Modulo operation: `{ field: { $mod: [divisor, remainder] } }`
- [ ] Match documents where `field % divisor === remainder`

**Test Cases**:
```typescript
// Even numbers
await collection.find({ value: { $mod: [2, 0] } }).toArray();

// Every third item
await collection.find({ index: { $mod: [3, 0] } }).toArray();
```

#### Step 3: `$expr` Operator
- [ ] Allow aggregation expressions in queries
- [ ] Compare fields within same document
- [ ] Use with aggregation operators

**Test Cases**:
```typescript
// Compare two fields
await collection.find({
  $expr: { $gt: ["$quantity", "$threshold"] }
}).toArray();

// With arithmetic
await collection.find({
  $expr: { $lt: [{ $add: ["$a", "$b"] }, 100] }
}).toArray();
```

#### Step 4: `$text` Operator (Simplified)
- [ ] Basic text search (without full-text index)
- [ ] `$search` for keywords
- [ ] Case-insensitive by default
- [ ] Note: This is a simplified version; full text search requires text indexes

**Test Cases**:
```typescript
// Requires text index on field
await collection.createIndex({ description: "text" });
await collection.find({ $text: { $search: "mongodb database" } }).toArray();
```

### Test File Structure

```
test/query-operators.test.ts
├── $type Operator
│   ├── should match string type
│   ├── should match number type
│   ├── should match array type
│   ├── should match object type
│   ├── should match boolean type
│   ├── should match null type
│   ├── should not match missing fields
│   └── should support numeric type codes
│
├── $mod Operator
│   ├── should match modulo remainder
│   ├── should find even numbers
│   ├── should find odd numbers
│   ├── should handle negative numbers
│   └── should not match non-numeric fields
│
├── $expr Operator
│   ├── should compare two fields
│   ├── should use arithmetic operators
│   ├── should use comparison operators
│   └── should work with nested fields
│
└── $text Operator (Simplified)
    ├── should search single keyword
    ├── should search multiple keywords
    ├── should be case-insensitive
    └── should require text index
```

---

## Phase 12.5: Find Options Parity

**Goal**: Ensure `findOne` supports the same options available on cursor methods.

**Priority**: HIGH — Enables common "get latest" query pattern.

**Background**: The `findOneAnd*` methods support `sort`, but plain `findOne` only accepts `projection`. This is a gap discovered during real-world usage testing.

### Operations

#### Step 1: Add `sort` option to `findOne`
- [ ] Extend `FindOptions` interface to include `sort`
- [ ] Apply sort before selecting first document
- [ ] Reuse existing `sortDocuments` from utils

**Use Case**:
```typescript
// Get the most recent active order
const latest = await collection.findOne(
  { status: "active" },
  { sort: { createdAt: -1 } }
);
```

#### Step 2: Add `skip` option to `findOne` (optional)
- [ ] Extend `FindOptions` interface to include `skip`
- [ ] Apply skip after sort, before selecting first document
- [ ] Useful for "get Nth matching document" pattern

**Use Case**:
```typescript
// Get the second-highest scorer
const runnerUp = await collection.findOne(
  { tournament: "finals" },
  { sort: { score: -1 }, skip: 1 }
);
```

### Implementation Notes

**Files to modify:**
1. `src/types.ts` — Extend `FindOptions` interface
2. `src/collection.ts` — Update `findOne` method to apply sort/skip

**Implementation approach:**
```typescript
// In types.ts
export interface FindOptions {
  projection?: ProjectionSpec;
  sort?: SortSpec;      // NEW
  skip?: number;        // NEW
}

// In collection.ts findOne method
async findOne(filter: Filter<T>, options?: FindOptions): Promise<T | null> {
  let docs = await this.loadDocuments();
  docs = docs.filter(doc => matchesFilter(doc, filter));

  if (options?.sort) {
    docs = sortDocuments(docs, options.sort);
  }
  if (options?.skip) {
    docs = docs.slice(options.skip);
  }

  const doc = docs[0] ?? null;
  // Apply projection...
}
```

### Test File Structure

```
test/find-options.test.ts
├── findOne with sort
│   ├── should return document matching sort order
│   ├── should return latest with descending sort
│   ├── should return earliest with ascending sort
│   ├── should work with compound sort
│   ├── should work with sort and projection
│   └── should return null if no match (with sort)
│
└── findOne with skip
    ├── should skip first N matching documents
    ├── should work with sort and skip together
    ├── should return null if skip exceeds matches
    └── should work with skip, sort, and projection
```

### Estimated Tests: 8-12

---

## Phase 13: Additional Update Operators

**Goal**: Implement remaining update operators.

**Priority**: MEDIUM — Completes update functionality.

### Operations

#### Step 1: `$min` and `$max` Update Operators
- [ ] `$min` — Only update if new value is less than current
- [ ] `$max` — Only update if new value is greater than current
- [ ] Works with numbers, dates, strings

**Test Cases**:
```typescript
// $min - update only if smaller
await collection.updateOne({ name: "Alice" }, { $min: { lowScore: 50 } });

// $max - update only if larger
await collection.updateOne({ name: "Alice" }, { $max: { highScore: 100 } });

// With dates
await collection.updateOne({}, { $min: { firstVisit: new Date() } });
```

#### Step 2: `$mul` Update Operator
- [ ] Multiply field by value
- [ ] Creates field with 0 if doesn't exist

**Test Cases**:
```typescript
await collection.updateOne({ name: "Alice" }, { $mul: { price: 1.1 } }); // 10% increase
await collection.updateOne({ name: "Alice" }, { $mul: { quantity: 2 } }); // Double
```

#### Step 3: `$rename` Update Operator
- [ ] Rename a field
- [ ] Works with dot notation for nested fields
- [ ] Removes old field, creates new field

**Test Cases**:
```typescript
await collection.updateOne({}, { $rename: { "oldName": "newName" } });
await collection.updateOne({}, { $rename: { "user.old": "user.new" } });
```

#### Step 4: `$currentDate` Update Operator
- [ ] Set field to current date
- [ ] Supports `$type: "date"` or `$type: "timestamp"`

**Test Cases**:
```typescript
await collection.updateOne({}, { $currentDate: { lastModified: true } });
await collection.updateOne({}, { $currentDate: { lastModified: { $type: "date" } } });
```

#### Step 5: `$setOnInsert` Update Operator
- [ ] Only set during upsert insert (not update)
- [ ] Ignored when document already exists

**Test Cases**:
```typescript
await collection.updateOne(
  { email: "new@test.com" },
  { $set: { name: "New" }, $setOnInsert: { createdAt: new Date() } },
  { upsert: true }
);
```

#### Step 6: Array Position Operators
- [ ] `$` positional operator — Update first matching array element
- [ ] `$[]` all positional operator — Update all array elements
- [ ] `$[<identifier>]` filtered positional operator with `arrayFilters`

**Test Cases**:
```typescript
// Update first matching element
await collection.updateOne(
  { "grades.score": { $lt: 60 } },
  { $set: { "grades.$.score": 60 } }
);

// Update all elements
await collection.updateOne({}, { $inc: { "scores.$[]": 5 } });

// With arrayFilters
await collection.updateOne(
  {},
  { $set: { "grades.$[elem].passed": true } },
  { arrayFilters: [{ "elem.score": { $gte: 60 } }] }
);
```

### Test File Structure

```
test/update-operators.test.ts
├── $min Operator
│   ├── should update when new value is less
│   ├── should not update when new value is greater
│   ├── should create field if missing
│   └── should work with dates
│
├── $max Operator
│   ├── should update when new value is greater
│   ├── should not update when new value is less
│   └── should work with dates
│
├── $mul Operator
│   ├── should multiply existing value
│   ├── should create field with 0 if missing
│   └── should handle floating point
│
├── $rename Operator
│   ├── should rename field
│   ├── should work with nested fields
│   ├── should remove old field
│   └── should no-op if old field missing
│
├── $currentDate Operator
│   ├── should set to current date
│   ├── should support date type
│   └── should support timestamp type
│
├── $setOnInsert Operator
│   ├── should set on insert (upsert)
│   ├── should not set on update
│   └── should work with other operators
│
└── Positional Operators
    ├── $ Positional
    │   ├── should update first matching element
    │   ├── should require matching query
    │   └── should work with nested arrays
    │
    ├── $[] All Positional
    │   ├── should update all elements
    │   └── should work with nested fields
    │
    └── $[identifier] Filtered Positional
        ├── should update matching elements
        ├── should respect arrayFilters
        └── should handle multiple filters
```

---

## Phase 14: Extended Index Features

**Goal**: Enhance index functionality for edge cases.

**Priority**: LOW — Nice-to-have for advanced use cases.

### Operations

#### Step 1: Sparse Indexes
- [ ] `sparse: true` option on createIndex
- [ ] Only index documents containing the field
- [ ] Allow multiple null/missing values with unique sparse index

**Test Cases**:
```typescript
await collection.createIndex({ optionalField: 1 }, { unique: true, sparse: true });
await collection.insertOne({ name: "A" });  // optionalField missing - OK
await collection.insertOne({ name: "B" });  // optionalField missing - Also OK (sparse)
```

#### Step 2: TTL Indexes
- [ ] `expireAfterSeconds` option
- [ ] Automatic document expiration (background process)
- [ ] Only works on date fields

**Test Cases**:
```typescript
await collection.createIndex({ createdAt: 1 }, { expireAfterSeconds: 3600 });
// Documents expire 1 hour after createdAt
```

#### Step 3: Partial Indexes
- [ ] `partialFilterExpression` option
- [ ] Only index documents matching the filter
- [ ] Reduces index size

**Test Cases**:
```typescript
await collection.createIndex(
  { email: 1 },
  { unique: true, partialFilterExpression: { email: { $exists: true } } }
);
```

#### Step 4: Index Hints
- [ ] `hint` option on find/aggregate
- [ ] Force use of specific index
- [ ] Note: With our full-scan design, this is mostly for API compatibility

**Test Cases**:
```typescript
await collection.find({ email: "a@test.com" }).hint("email_1").toArray();
await collection.find({ email: "a@test.com" }).hint({ email: 1 }).toArray();
```

### Test File Structure

```
test/indexes-extended.test.ts
├── Sparse Indexes
│   ├── should allow multiple missing values
│   ├── should enforce uniqueness for present values
│   └── should list as sparse in indexes()
│
├── TTL Indexes
│   ├── should create with expireAfterSeconds
│   ├── should expire documents (mock time)
│   └── should only work on date fields
│
├── Partial Indexes
│   ├── should create with partialFilterExpression
│   ├── should only enforce for matching docs
│   └── should list filter in indexes()
│
└── Index Hints
    ├── should accept hint on find
    ├── should accept hint on aggregate
    └── should accept hint as string or object
```

---

## Phase 15: Administrative Operations

**Goal**: Implement database-level and collection-level admin operations.

**Priority**: LOW — Useful for testing and management.

### Operations

#### Step 1: Database Operations
- [ ] `db.listCollections()` — List all collections
- [ ] `db.dropDatabase()` — Drop entire database (already exists but enhance)
- [ ] `db.stats()` — Database statistics

#### Step 2: Collection Operations
- [ ] `collection.drop()` — Drop the collection
- [ ] `collection.rename(newName)` — Rename collection
- [ ] `collection.stats()` — Collection statistics

#### Step 3: `distinct()` Method
- [ ] Get distinct values for a field
- [ ] Support query filter

**Test Cases**:
```typescript
await collection.distinct("category");
await collection.distinct("tags", { active: true });
```

#### Step 4: `estimatedDocumentCount()`
- [ ] Fast count without filter
- [ ] Returns total document count

**Test Cases**:
```typescript
const count = await collection.estimatedDocumentCount();
```

### Test File Structure

```
test/admin.test.ts
├── Database Operations
│   ├── listCollections
│   │   ├── should list all collections
│   │   └── should return empty for new db
│   │
│   ├── dropDatabase
│   │   └── should remove all collections
│   │
│   └── stats
│       └── should return database statistics
│
├── Collection Operations
│   ├── drop
│   │   ├── should remove collection
│   │   └── should remove data file
│   │
│   ├── rename
│   │   ├── should rename collection
│   │   └── should preserve data
│   │
│   └── stats
│       └── should return collection statistics
│
├── distinct
│   ├── should return unique values
│   ├── should support filter
│   ├── should handle array fields
│   └── should handle nested fields
│
└── estimatedDocumentCount
    ├── should return count
    └── should be faster than countDocuments
```

---

## Phase 16: Extended Expression Operators

**Goal**: Expand the expression operator library for more powerful aggregation pipelines.

**Priority**: LOW — Enhances aggregation expressiveness for advanced use cases.

**Background**: The current expression evaluator (`evaluateExpression` in `aggregation.ts`) supports a core set of operators. Real-world usage revealed gaps when building complex aggregations with null handling, type conversion, and array manipulation.

### Current Expression Operators (Implemented)

| Category | Operators |
|----------|-----------|
| Arithmetic | `$add`, `$subtract`, `$multiply`, `$divide` |
| String | `$concat`, `$toUpper`, `$toLower` |
| Conditional | `$cond`, `$ifNull` |
| Comparison | `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne` |
| Array | `$size` |
| Special | `$literal` |

### Operations

#### Step 1: Additional Arithmetic Operators
- [ ] `$abs` — Absolute value
- [ ] `$ceil` — Ceiling (round up)
- [ ] `$floor` — Floor (round down)
- [ ] `$round` — Round to specified decimal places
- [ ] `$mod` — Modulo (expression version)

**Test Cases**:
```typescript
await collection.aggregate([
  { $project: {
    absValue: { $abs: "$balance" },
    rounded: { $round: ["$price", 2] },
    remainder: { $mod: ["$quantity", 12] }
  }}
]).toArray();
```

#### Step 2: Additional String Operators
- [ ] `$substr` / `$substrCP` — Substring extraction
- [ ] `$strLenCP` — String length
- [ ] `$split` — Split string into array
- [ ] `$trim` / `$ltrim` / `$rtrim` — Whitespace trimming
- [ ] `$toString` — Convert to string
- [ ] `$indexOfCP` — Find substring position

**Test Cases**:
```typescript
await collection.aggregate([
  { $project: {
    firstThree: { $substr: ["$code", 0, 3] },
    words: { $split: ["$sentence", " "] },
    length: { $strLenCP: "$name" }
  }}
]).toArray();
```

#### Step 3: Array Manipulation Operators
- [ ] `$arrayElemAt` — Get element at index
- [ ] `$slice` — Extract portion of array (expression version)
- [ ] `$concatArrays` — Concatenate arrays
- [ ] `$filter` — Filter array elements
- [ ] `$map` — Transform array elements
- [ ] `$reduce` — Reduce array to single value
- [ ] `$in` — Check if value in array (expression version)

**Test Cases**:
```typescript
await collection.aggregate([
  { $project: {
    firstItem: { $arrayElemAt: ["$items", 0] },
    topThree: { $slice: ["$scores", 3] },
    allTags: { $concatArrays: ["$tags", "$categories"] },
    activeTasks: { $filter: {
      input: "$tasks",
      as: "task",
      cond: { $eq: ["$$task.status", "active"] }
    }}
  }}
]).toArray();
```

#### Step 4: Type Conversion Operators
- [ ] `$toInt` — Convert to integer
- [ ] `$toDouble` — Convert to double
- [ ] `$toBool` — Convert to boolean
- [ ] `$toDate` — Convert to date
- [ ] `$toObjectId` — Convert to ObjectId
- [ ] `$type` — Get BSON type name (expression version)

**Test Cases**:
```typescript
await collection.aggregate([
  { $project: {
    numericId: { $toInt: "$stringId" },
    timestamp: { $toDate: "$dateString" },
    fieldType: { $type: "$unknownField" }
  }}
]).toArray();
```

#### Step 5: Date Operators
- [ ] `$year` — Extract year
- [ ] `$month` — Extract month (1-12)
- [ ] `$dayOfMonth` — Extract day of month
- [ ] `$hour` — Extract hour
- [ ] `$minute` — Extract minute
- [ ] `$second` — Extract second
- [ ] `$dayOfWeek` — Extract day of week (1=Sunday)
- [ ] `$dateToString` — Format date as string

**Test Cases**:
```typescript
await collection.aggregate([
  { $project: {
    year: { $year: "$createdAt" },
    month: { $month: "$createdAt" },
    formatted: { $dateToString: {
      format: "%Y-%m-%d",
      date: "$createdAt"
    }}
  }}
]).toArray();
```

### Implementation Notes

**Files to modify:**
1. `src/aggregation.ts` — Add cases to `evaluateOperator` function

**Implementation approach:**
```typescript
// In aggregation.ts evaluateOperator function
case "$abs":
  return Math.abs(evaluateExpression(args, doc) as number);

case "$substr": {
  const [str, start, length] = args as [unknown, unknown, unknown];
  const strVal = evaluateExpression(str, doc) as string;
  const startVal = evaluateExpression(start, doc) as number;
  const lengthVal = evaluateExpression(length, doc) as number;
  return strVal?.substring(startVal, startVal + lengthVal) ?? null;
}

case "$arrayElemAt": {
  const [arr, idx] = args as [unknown, unknown];
  const arrVal = evaluateExpression(arr, doc) as unknown[];
  const idxVal = evaluateExpression(idx, doc) as number;
  return arrVal?.[idxVal < 0 ? arrVal.length + idxVal : idxVal] ?? null;
}
```

### Test File Structure

```
test/expression-operators.test.ts
├── Arithmetic Operators
│   ├── $abs
│   │   ├── should return absolute value of positive
│   │   ├── should return absolute value of negative
│   │   └── should return null for null input
│   ├── $ceil / $floor / $round
│   └── $mod (expression)
│
├── String Operators
│   ├── $substr
│   │   ├── should extract substring
│   │   ├── should handle start beyond length
│   │   └── should return null for null input
│   ├── $split
│   ├── $strLenCP
│   ├── $trim / $ltrim / $rtrim
│   └── $toString
│
├── Array Operators
│   ├── $arrayElemAt
│   │   ├── should get element at positive index
│   │   ├── should get element at negative index
│   │   └── should return null for out of bounds
│   ├── $slice
│   ├── $concatArrays
│   ├── $filter
│   └── $map
│
├── Type Conversion
│   ├── $toInt / $toDouble
│   ├── $toBool
│   ├── $toDate
│   └── $type (expression)
│
└── Date Operators
    ├── $year / $month / $dayOfMonth
    ├── $hour / $minute / $second
    └── $dateToString
```

### Estimated Tests: 50-70

---

## Appendix A: Error Messages Reference

All error messages should match MongoDB's format exactly. These have been verified against official MongoDB documentation.

### Query Errors

| Operator | Condition | Error Message |
|----------|-----------|---------------|
| `$regex` | Invalid pattern | `$regex has to be a string` |
| `$options` | Without $regex | `$options needs a $regex` |
| `$options` | Invalid flag | `invalid flag in regex options: <flag>` |
| `$type` | Unknown type | `unknown type code: X` |
| `$mod` | Wrong array length | Array must have exactly 2 elements (error varies) |
| `$mod` | NaN/Infinity value | Error in MongoDB 5.1+ |
| `$regexMatch` | Non-string input | `$regexMatch needs 'input' to be of type string` |

### Aggregation Errors

| Stage | Condition | Error Message |
|-------|-----------|---------------|
| `$group` | Missing _id | `a group specification must include an _id` |
| `$lookup` | Missing from | `$lookup requires 'from' field` |
| `$out` | Not last stage | `$out can only be the final stage in the pipeline` |
| `$replaceRoot` | Missing/null newRoot | `$replaceRoot` errors and fails |
| `$concat` | Non-string arg | `$concat only supports strings, not <type>` |
| Pipeline | Unknown stage | `Unrecognized pipeline stage name: '$unknown'` |
| `$count` | Empty field name | Field name must be non-empty |
| `$count` | Field starts with $ | Field name cannot start with $ |
| `$count` | Field contains . | Field name cannot contain . |

### Update Errors

| Operator | Condition | Error Message |
|----------|-----------|---------------|
| `$rename` | Same field | `$rename source and dest can't be the same` |
| `$mul` | Non-numeric field | `Cannot apply $mul to a non-numeric value` |
| `$mul` | Missing field | Creates field with value 0 (not an error!) |
| `$` | No array in query | `The positional operator did not find the match needed` |
| `$[identifier]` | Spaces in identifier | Operation fails |
| `$position` | Without $each | Error - must use with $each |
| `$slice` | Without $each | Error - must use with $each |
| `$sort` | Without $each | Error - must use with $each |

### Index Errors

| Operation | Condition | Error Message |
|-----------|-----------|---------------|
| `dropIndex` | _id index | `cannot drop _id index` |
| `dropIndex` | Not found | `index not found with name [indexName]` |
| `hint()` | Invalid index | `planner returned error: bad hint` |
| `rename` | Source not found | `source namespace does not exist` (Code 26) |
| Sparse + Partial | Both specified | Cannot use both options |

---

## Appendix B: Critical Edge Cases and Surprising Behaviors

These behaviors were verified from official MongoDB documentation and must be implemented correctly.

### Aggregation Pipeline

| Behavior | Details |
|----------|---------|
| `$count` on empty input | Returns **NO document** (empty array), not `{ count: 0 }` |
| `$unwind` on non-array | Treats as single-element array (MongoDB 3.2+) |
| `$project` mixing | Cannot mix 1 and 0 except for `_id` exclusion |
| `$concat` with null | Returns **null** (null propagates) |
| `$concat` with non-string | **Throws error** |
| `$replaceRoot` missing field | **Errors and fails** (use `$mergeObjects` or `$ifNull` to handle) |
| `$lookup` no match | Returns **empty array** in output field (left outer join) |
| `$group` with `_id: null` | Groups ALL documents into single group |
| `$out` position | Must be **last stage** (throws error otherwise) |
| `$merge` failures | Does **NOT** rollback previous writes (unlike `$out`) |

### Regular Expressions

| Behavior | Details |
|----------|---------|
| Array field matching | Matches if **ANY** element matches (not all) |
| Non-string fields | **Silently skipped** (no match, no error) |
| `$regex` in `$in` | Only JavaScript regex objects allowed (`/pattern/`), not `{ $regex: ... }` |
| Options `x` and `s` | Require `$regex` operator syntax, not inline `/pattern/` |
| `$regexMatch` null input | **Throws error** (use `$ifNull` to handle) |

### Query Operators

| Behavior | Details |
|----------|---------|
| `$type: "number"` | Matches `double`, `int`, `long`, AND `decimal` |
| `$type` on arrays | Checks **element types** (not array type itself pre-3.6) |
| `$mod` with decimals | **Rounds** towards zero |
| `$mod` negative dividend | Returns **negative** remainder (e.g., -5 % 4 = -1) |
| `$expr` index usage | Only for field-to-**constant** comparisons |

### Update Operators

| Behavior | Details |
|----------|---------|
| `$mul` missing field | Creates field with **0** (not the multiplied value!) |
| `$min`/`$max` missing field | Creates field with specified value |
| `$rename` missing old field | **No-op** (no error) |
| `$rename` existing new field | **Removes** existing field first |
| `$setOnInsert` without upsert | **Ignored** |
| `$` positional | Requires array field **in query** |
| `$[identifier]` format | Must be lowercase letter + alphanumeric, no spaces |
| `$push` modifiers order | Always: position → sort → slice (regardless of spec order) |

### Indexes

| Behavior | Details |
|----------|---------|
| Sparse indexes | DO index `null` values, only skip **missing** fields |
| TTL deletion | Asynchronous, ~60 second intervals (not immediate) |
| Partial + Sparse | **Cannot combine** both options |
| `estimatedDocumentCount()` | May be **inaccurate** on sharded clusters |
| `distinct()` on arrays | Treats **each element** as separate value |

---

## Appendix C: Feature Comparison (After All Phases)

### MongoDB Feature Coverage

| MongoDB Feature | MangoDB Status | Notes |
|-----------------|----------------|-------|
| CRUD Operations | ✅ Full | Phases 1-3 |
| Query Operators | ✅ Full | Phases 2, 5, 11, 12 |
| Update Operators | ✅ Full | Phases 3, 6, 13 |
| Aggregation Pipeline | ✅ Full | Phases 9, 10 |
| Indexes | ✅ Full | Phases 7, 14 |
| Regex Search | ✅ Full | Phase 11 |
| Transactions | ❌ None | Out of scope (file-based) |
| Change Streams | ❌ None | Out of scope |
| Replication | ❌ None | Single-instance only |
| Sharding | ❌ None | Single-instance only |
| Geospatial | ❌ None | Complex, low demand |

---

## Appendix D: Implementation Checklist Template

For each new phase, create a PHASE*_PLAN.md with:

```markdown
# Phase X: [Feature Name] - Implementation Plan

## Overview
Brief description of the feature set.

## Operations
List of operations to implement.

## Step 1: [First Operation]

### Syntax
```typescript
// Method signature
```

### Behavior
- Bullet points describing behavior

### Test Cases
```typescript
// Example tests
```

### Edge Cases
```typescript
// Edge case tests
```

### Error Cases
```typescript
// Error case tests
```

### Implementation Notes
- Implementation details

## Implementation Order
Recommended order by dependency.

## File Changes Required
- List of files to modify

## Test File Structure
```
test/[feature].test.ts
├── Section 1
│   ├── test 1
│   └── test 2
```

## Documentation Updates
1. PROGRESS.md
2. ROADMAP.md
3. COMPATIBILITY.md
4. README.md
```

---

## Summary

### Current Status

Phases 1-12 are now complete. MangoDB has approximately **90%+ coverage** of common MongoDB usage with **675 tests**.

### High Priority (Phase 12.5)

Consistency fix discovered during real-world usage testing:
- **Phase 12.5**: Find options parity (`findOne` + sort/skip)

### Medium Priority (Phase 13)

Additional operators to complete the update API:
- **Phase 13**: $min, $max, $mul, $rename, positional operators

### Low Priority (Phases 14-16)

Extended features for completeness:
- **Phase 14**: Sparse, TTL, partial indexes
- **Phase 15**: Admin operations, distinct()
- **Phase 16**: Extended expression operators (arithmetic, string, array, date)

### Estimated Total Work

| Metric | Value |
|--------|-------|
| Remaining Phases | 5 |
| Estimated New Tests | 130-170 |
| Estimated Code Lines | 800-1200 |
| Estimated Time | Varies based on scope per phase |

After completing all phases, MangoDB will be a comprehensive file-based MongoDB replacement suitable for:
- Local development
- Integration testing
- CI pipelines without MongoDB
- Educational purposes
- Prototyping
