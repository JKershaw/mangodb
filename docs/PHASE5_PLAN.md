# Phase 5: Aggregation Stages (Extended) - Implementation Plan

This document provides a detailed implementation plan for Phase 5, following MangoDB's TDD methodology and research-first approach.

---

## Overview

**Goal**: Implement remaining useful aggregation stages to increase pipeline coverage from 56% to ~70%.

**Estimated Complexity**: High (especially `$setWindowFields`)

**Files to modify**:
- `src/aggregation/cursor.ts` - Add stage handlers
- `src/aggregation/index.ts` - Export new types if needed
- `src/aggregation/types.ts` - Add stage type definitions
- `test/aggregation-advanced.test.ts` - Add comprehensive tests

---

## Task Breakdown by Complexity

### Tier 0: Preparatory Infrastructure (Do First!)

These utilities reduce duplication and make Tier 2-3 tasks significantly easier.

---

#### Task 5.0.1: System Variables Support
**Description**: Add built-in system variables to expression evaluation.

**Current State**: `evaluateExpression()` handles `$$varName` lookup in `vars` context, but doesn't provide built-in system variables.

**Variables Needed**:
| Variable | Used By | Value |
|----------|---------|-------|
| `$$NOW` | `$documents` | Current datetime |
| `$$ROOT` | General | Original root document |
| `$$DESCEND` | `$redact` | String literal `"descend"` |
| `$$PRUNE` | `$redact` | String literal `"prune"` |
| `$$KEEP` | `$redact` | String literal `"keep"` |
| `$$REMOVE` | `$project` | Special marker for field removal |

**Implementation**:
1. Create `createSystemVars(doc: Document): VariableContext` function
2. Call it at stage entry points, merge with user vars
3. Special-case `$$NOW` to generate fresh Date each evaluation

**Sub-tasks**:
- [ ] 5.0.1.1 Create `src/aggregation/system-vars.ts` with `createSystemVars()`
- [ ] 5.0.1.2 Add `$$NOW` returning `new Date()`
- [ ] 5.0.1.3 Add `$$ROOT` returning the original document
- [ ] 5.0.1.4 Add `$$DESCEND`, `$$PRUNE`, `$$KEEP` as string constants
- [ ] 5.0.1.5 Update `evaluateExpression()` to merge system vars
- [ ] 5.0.1.6 Write tests for each system variable

**Estimated effort**: Small (1-2 hours)

---

#### Task 5.0.2: Partition Grouping Utility
**Description**: Shared utility for grouping documents by field(s).

**Used By**: `$densify`, `$fill`, `$setWindowFields`, `$bucket`, `$bucketAuto`

**Current State**: `$bucket` and `$bucketAuto` each implement their own grouping logic.

**API Design**:
```typescript
interface PartitionOptions {
  partitionBy?: unknown;           // Expression form: { field: "$field" }
  partitionByFields?: string[];    // Array form: ["field1", "field2"]
}

function partitionDocuments(
  docs: Document[],
  options: PartitionOptions,
  evaluate: EvaluateExpressionFn
): Map<string, Document[]>
```

**Implementation**:
1. Handle both `partitionBy` (expression) and `partitionByFields` (array)
2. Generate stable partition keys via JSON.stringify
3. Return Map with partition key → documents array

**Sub-tasks**:
- [ ] 5.0.2.1 Create `src/aggregation/partition.ts` with `partitionDocuments()`
- [ ] 5.0.2.2 Handle `partitionByFields` array form
- [ ] 5.0.2.3 Handle `partitionBy` expression form
- [ ] 5.0.2.4 Handle no partitioning (all docs in single group)
- [ ] 5.0.2.5 Validate `partitionBy` is object, not string
- [ ] 5.0.2.6 Write tests for all partitioning modes

**Estimated effort**: Small (1-2 hours)

---

#### Task 5.0.3: Sort Within Partitions Utility
**Description**: Sort documents within each partition.

**Used By**: `$fill`, `$setWindowFields`

**API Design**:
```typescript
function sortPartition(
  docs: Document[],
  sortBy: SortSpec
): Document[]
```

**Implementation**:
1. Reuse existing `compareValuesForSort()` from utils
2. Support multi-field sort specs
3. Return sorted copy (don't mutate)

**Sub-tasks**:
- [ ] 5.0.3.1 Add `sortPartition()` to `src/aggregation/partition.ts`
- [ ] 5.0.3.2 Handle ascending/descending for each field
- [ ] 5.0.3.3 Write tests for single and multi-field sorts

**Estimated effort**: Small (30 mins - 1 hour)

---

#### Task 5.0.4: Recursive Document Traversal Utility
**Description**: Walk through nested documents/arrays applying a callback.

**Used By**: `$redact`

**API Design**:
```typescript
type TraversalAction = "descend" | "prune" | "keep";

function traverseDocument(
  doc: Document,
  callback: (subdoc: Document) => TraversalAction
): Document | null
```

**Implementation**:
1. Call callback on current document
2. If "prune": return null (exclude)
3. If "keep": return doc as-is (no recursion)
4. If "descend": recurse into embedded docs and arrays

**Sub-tasks**:
- [ ] 5.0.4.1 Create `src/aggregation/traverse.ts` with `traverseDocument()`
- [ ] 5.0.4.2 Handle embedded documents (object fields)
- [ ] 5.0.4.3 Handle arrays of documents
- [ ] 5.0.4.4 Handle arrays of scalars (keep if parent descends)
- [ ] 5.0.4.5 Write tests: nested docs, arrays, mixed content

**Estimated effort**: Small-Medium (2-3 hours)

---

#### Task 5.0.5: Gap Filling Utilities (LOCF & Linear)
**Description**: Shared gap-filling logic for `$fill` and `$setWindowFields`.

**Used By**: `$fill` stage, `$locf` operator, `$linearFill` operator

**API Design**:
```typescript
// Last Observation Carried Forward
function applyLocf(
  values: (number | null)[],
): (number | null)[]

// Linear Interpolation
function applyLinearFill(
  values: (number | null)[],
  sortValues?: number[]  // for range-based interpolation
): (number | null)[]
```

**Implementation**:
1. `applyLocf`: Scan forward, replace nulls with last non-null
2. `applyLinearFill`: Find surrounding non-nulls, interpolate proportionally
3. Handle edge cases: all null, null at start, single value

**Sub-tasks**:
- [ ] 5.0.5.1 Create `src/aggregation/gap-fill.ts`
- [ ] 5.0.5.2 Implement `applyLocf()` with forward scan
- [ ] 5.0.5.3 Implement `applyLinearFill()` with interpolation
- [ ] 5.0.5.4 Handle edge case: nulls at start remain null
- [ ] 5.0.5.5 Handle edge case: all-null input
- [ ] 5.0.5.6 Write tests for various gap patterns

**Estimated effort**: Small-Medium (2-3 hours)

---

#### Task 5.0.6: Date Stepping Utility
**Description**: Step through dates by time unit (for `$densify`).

**Used By**: `$densify` with date fields

**Current State**: `evalDateAdd()` exists but takes expression args. Need pure utility.

**API Design**:
```typescript
function addDateStep(
  date: Date,
  step: number,
  unit: string  // "hour", "day", "week", "month", "year"
): Date
```

**Implementation**:
1. Extract logic from `evalDateAdd()` into pure function
2. Handle all time units: millisecond, second, minute, hour, day, week, month, year
3. Handle calendar-aware stepping for month/year

**Sub-tasks**:
- [ ] 5.0.6.1 Create `src/aggregation/date-utils.ts` with `addDateStep()`
- [ ] 5.0.6.2 Extract logic from existing `evalDateAdd()`
- [ ] 5.0.6.3 Refactor `evalDateAdd()` to use the new utility
- [ ] 5.0.6.4 Write tests for all time units
- [ ] 5.0.6.5 Write tests for calendar edge cases (month boundaries, leap years)

**Estimated effort**: Small (1-2 hours)

---

### Tier 0 Summary

| Task | Description | Enables | Effort |
|------|-------------|---------|--------|
| 5.0.1 | System Variables | `$documents`, `$redact` | 1-2h |
| 5.0.2 | Partition Grouping | `$densify`, `$fill`, `$setWindowFields` | 1-2h |
| 5.0.3 | Sort Within Partitions | `$fill`, `$setWindowFields` | 0.5-1h |
| 5.0.4 | Recursive Traversal | `$redact` | 2-3h |
| 5.0.5 | Gap Filling (LOCF/Linear) | `$fill`, `$setWindowFields` | 2-3h |
| 5.0.6 | Date Stepping | `$densify` | 1-2h |

**Total Tier 0 Effort**: 8-13 hours

**After Tier 0, the Tier 2-3 stages become much simpler**:
- `$redact` = system vars + traversal utility + expression eval
- `$densify` = partition utility + date stepping + gap detection
- `$fill` = partition utility + sort utility + gap filling utilities
- `$setWindowFields` = partition utility + sort utility + window bounds + gap filling

---

### Tier 1: Simple Aliases (Low Complexity)

#### Task 5.3: `$replaceWith`
**Description**: Alias for `$replaceRoot` with simpler syntax.

**Syntax**:
```javascript
{ $replaceWith: <expression> }
// Equivalent to: { $replaceRoot: { newRoot: <expression> } }
```

**Implementation**:
1. Add case in stage switch
2. Delegate to existing `$replaceRoot` logic
3. Validate expression evaluates to document

**Error Handling**:
| Condition | Error Code | Message |
|-----------|------------|---------|
| Non-object result | 40228 | `"'newRoot' expression must evaluate to an object"` |
| Null result | - | Same as non-object |
| Missing field | - | Same as non-object |

**Sub-tasks**:
- [ ] 5.3.1 Add `$replaceWith` case that delegates to `$replaceRoot`
- [ ] 5.3.2 Write tests for expression types (field ref, literal, $mergeObjects)
- [ ] 5.3.3 Write error case tests (null, array, scalar results)

**Estimated effort**: Small (1-2 hours)

---

#### Task 5.4: `$unset`
**Description**: Alias for `$project` with field exclusion.

**Syntax**:
```javascript
{ $unset: "<field>" }
{ $unset: ["<field1>", "<field2>"] }
```

**Implementation**:
1. Accept string or array of strings
2. Convert to `$project` with `{ field: 0 }` format
3. Delegate to existing `$project` exclusion logic

**Error Handling**:
| Condition | Error Code | Message |
|-----------|------------|---------|
| Non-string in array | 31120 | `"$unset specification must be a string or array of strings"` |
| Empty string field | 40352 | `"FieldPath cannot be constructed with empty string"` |

**Behavior Notes**:
- `_id` field cannot be unset (silently ignored)
- Non-existent fields are silently ignored
- Array index notation (e.g., `"items.0"`) replaces element with `null`
- Nested paths supported via dot notation

**Sub-tasks**:
- [ ] 5.4.1 Add `$unset` case with string/array normalization
- [ ] 5.4.2 Convert to `$project` exclusion format and delegate
- [ ] 5.4.3 Write tests: single field, array, nested paths
- [ ] 5.4.4 Write tests: edge cases (_id, non-existent, array indices)

**Estimated effort**: Small (1-2 hours)

---

#### Task 5.5: `$documents`
**Description**: Inject literal documents into pipeline (must be first stage).

**Syntax**:
```javascript
db.aggregate([
  { $documents: [ { field: "value" }, ... ] },
  // subsequent stages
])
```

**Implementation**:
1. Validate stage is first in pipeline
2. Accept array of document literals
3. Support expressions that resolve to arrays
4. Support system variables (`$$NOW`)

**Error Handling**:
| Condition | Message |
|-----------|---------|
| Not first stage | `"$documents must be the first stage in pipeline"` |
| Non-array expression | `"$documents requires array of documents"` |
| Non-object in array | `"$documents array elements must be objects"` |

**Behavior Notes**:
- Cannot reference document fields (`$field`)
- Cannot use `$$ROOT`
- Empty array produces no output documents
- Supports `$let` expressions

**Sub-tasks**:
- [ ] 5.5.1 Add `$documents` case with first-stage validation
- [ ] 5.5.2 Evaluate expression to get document array
- [ ] 5.5.3 Write tests: literal arrays, empty arrays
- [ ] 5.5.4 Write tests: with expressions (`$$NOW`, `$let`)
- [ ] 5.5.5 Write error tests: not first stage, non-array

**Estimated effort**: Small-Medium (2-3 hours)

---

### Tier 2: Moderate Complexity

#### Task 5.2: `$redact`
**Description**: Field-level access control with recursive document traversal.

**Syntax**:
```javascript
{
  $redact: {
    $cond: {
      if: <condition>,
      then: "$$DESCEND" | "$$PRUNE" | "$$KEEP",
      else: "$$DESCEND" | "$$PRUNE" | "$$KEEP"
    }
  }
}
```

**System Variables**:
| Variable | Behavior |
|----------|----------|
| `$$DESCEND` | Keep current level, evaluate nested levels |
| `$$PRUNE` | Remove entire level and all nested content |
| `$$KEEP` | Keep entire level and all nested content (no recursion) |

**Implementation**:
1. Evaluate expression at document root
2. If `$$DESCEND`: recursively evaluate embedded documents
3. If `$$PRUNE`: exclude document/field entirely
4. If `$$KEEP`: include document/field and stop recursion
5. Process arrays by evaluating each element document

**Error Handling**:
| Condition | Message |
|-----------|---------|
| Invalid result | `"$redact must resolve to $$DESCEND, $$PRUNE, or $$KEEP"` |

**Edge Cases**:
- Null/missing fields in expressions (use `$ifNull`)
- Arrays of scalars (included if parent returns `$$DESCEND`)
- Empty embedded documents (still evaluated)

**Sub-tasks**:
- [ ] 5.2.1 Implement `redactDocument()` recursive function
- [ ] 5.2.2 Handle system variable evaluation (`$$DESCEND`, `$$PRUNE`, `$$KEEP`)
- [ ] 5.2.3 Implement array element traversal for embedded documents
- [ ] 5.2.4 Write tests: basic ACL pattern with `$cond`
- [ ] 5.2.5 Write tests: nested documents with different ACLs
- [ ] 5.2.6 Write tests: arrays with embedded documents
- [ ] 5.2.7 Write error tests: invalid expression results

**Estimated effort**: Medium (4-6 hours)

---

#### Task 5.6: `$densify`
**Description**: Fill gaps in numeric or date sequences.

**Syntax**:
```javascript
{
  $densify: {
    field: "<field>",
    range: {
      step: <number>,
      unit: "<time-unit>",        // for dates only
      bounds: [<lower>, <upper>]  // optional
    },
    partitionByFields: ["<field1>", ...]  // optional
  }
}
```

**Time Units**: `"millisecond"`, `"second"`, `"minute"`, `"hour"`, `"day"`, `"week"`, `"month"`, `"year"`

**Implementation**:
1. Validate field name (cannot start with `$`)
2. Group by partition fields if specified
3. For each partition:
   - Collect all field values
   - Determine bounds (explicit or min/max of values)
   - Generate missing documents at step intervals
4. Merge generated documents with originals

**Error Handling**:
| Condition | Message |
|-----------|---------|
| Field starts with `$` | `"Cannot densify field starting with '$'"` |
| Negative/zero step | `"Step must be positive"` |
| Unit with numeric field | `"Cannot specify unit for numeric field"` |
| No unit with date field | `"Unit required for date field"` |
| Bounds type mismatch | `"Bounds must match field type"` |
| Non-ascending bounds | `"Lower bound cannot exceed upper bound"` |

**Edge Cases**:
- Empty collections: no output
- Single document: pass through unchanged
- Null values: excluded from densification
- Very large gaps: generates many documents (performance concern)

**Sub-tasks**:
- [x] 5.6.1 Add validation for field name, step, bounds
- [x] 5.6.2 Implement partition grouping logic
- [x] 5.6.3 Implement numeric gap detection and filling
- [x] 5.6.4 Implement date gap detection with time units
- [x] 5.6.5 Handle bounds (explicit vs inferred from data)
- [x] 5.6.6 Write tests: numeric field without partitions
- [x] 5.6.7 Write tests: date field with time units
- [x] 5.6.8 Write tests: partitioned densification
- [x] 5.6.9 Write tests: explicit bounds
- [x] 5.6.10 Write error tests: validation failures

**Estimated effort**: Medium-High (6-8 hours)

---

#### Task 5.7: `$fill`
**Description**: Fill null/missing values with various strategies.

**Syntax**:
```javascript
{
  $fill: {
    sortBy: { <field>: 1 },           // required for locf/linear
    partitionBy: { <expr> },          // optional (object form)
    partitionByFields: ["<field>"],   // optional (array form)
    output: {
      <field1>: { value: <expression> },
      <field2>: { method: "locf" },
      <field3>: { method: "linear" }
    }
  }
}
```

**Fill Methods**:
| Method | Description | sortBy Required |
|--------|-------------|-----------------|
| `value` | Static value or expression | No |
| `locf` | Last observation carried forward | Yes |
| `linear` | Linear interpolation | Yes |

**Implementation**:
1. Validate `sortBy` presence for locf/linear
2. Group documents by partition
3. Sort each partition by `sortBy`
4. For each output field:
   - `value`: Replace all nulls with expression result
   - `locf`: Scan forward, carry last non-null value
   - `linear`: Find surrounding non-nulls, interpolate

**Error Handling**:
| Condition | Message |
|-----------|---------|
| Missing sortBy with locf | `"sortBy required for locf method"` |
| Missing sortBy with linear | `"sortBy required for linear method"` |
| partitionBy as string | `"partitionBy must be object, not string"` |
| Duplicate sortBy values (linear) | `"Duplicate sortBy values in partition"` |

**Edge Cases**:
- All null partition: remains null for locf/linear
- Null at start: remains null for locf
- Single document: no interpolation possible
- Both value and method: error

**Sub-tasks**:
- [x] 5.7.1 Add validation for sortBy, partitionBy, output
- [x] 5.7.2 Implement partition grouping (both forms)
- [x] 5.7.3 Implement `value` fill method
- [x] 5.7.4 Implement `locf` fill method with forward scan
- [x] 5.7.5 Implement `linear` interpolation with duplicate check
- [x] 5.7.6 Write tests: value fill with expressions
- [x] 5.7.7 Write tests: locf with various null patterns
- [x] 5.7.8 Write tests: linear interpolation
- [x] 5.7.9 Write tests: partitioned filling
- [x] 5.7.10 Write error tests: missing sortBy, duplicates

**Estimated effort**: Medium-High (6-8 hours)

---

### Tier 3: High Complexity

#### Task 5.1: `$graphLookup`
**Description**: Recursive lookup for graph traversal.

**Syntax**:
```javascript
{
  $graphLookup: {
    from: "<collection>",
    startWith: <expression>,
    connectFromField: "<field>",
    connectToField: "<field>",
    as: "<output-array>",
    maxDepth: <number>,              // optional
    depthField: "<field>",           // optional
    restrictSearchWithMatch: <query> // optional
  }
}
```

**Implementation**:
1. For each input document:
   - Evaluate `startWith` expression
   - Initialize result set and visited tracking
   - BFS/DFS traversal:
     - Match `startWith` value against `connectToField` in `from` collection
     - Apply `restrictSearchWithMatch` filter
     - Extract `connectFromField` values for next iteration
     - Track depth, add `depthField` if specified
     - Continue until no matches or `maxDepth` reached
   - Add results to `as` array

**Error Handling**:
| Condition | Error Code | Message |
|-----------|------------|---------|
| Missing required field | - | `"$graphLookup requires '<field>'"` |
| Invalid restrictSearchWithMatch | 40185 | `"restrictSearchWithMatch must be object"` |
| Negative maxDepth | - | `"maxDepth must be non-negative"` |

**Behavior Notes**:
- Cycles are NOT prevented (use `maxDepth` to limit)
- Same document can appear multiple times at different depths
- Array `startWith` initiates parallel searches
- Null/missing `startWith`: empty result array
- `depthField` values are `NumberLong` starting at 0

**Sub-tasks**:
- [ ] 5.1.1 Add validation for all required and optional fields
- [ ] 5.1.2 Implement `startWith` expression evaluation
- [ ] 5.1.3 Implement BFS traversal with collection access
- [ ] 5.1.4 Implement `maxDepth` limiting
- [ ] 5.1.5 Implement `depthField` tracking
- [ ] 5.1.6 Implement `restrictSearchWithMatch` filtering
- [ ] 5.1.7 Handle array `startWith` values
- [ ] 5.1.8 Write tests: simple hierarchy traversal
- [ ] 5.1.9 Write tests: with maxDepth limits
- [ ] 5.1.10 Write tests: with depthField tracking
- [ ] 5.1.11 Write tests: with restrictSearchWithMatch
- [ ] 5.1.12 Write tests: cyclic graphs
- [ ] 5.1.13 Write tests: empty results, null startWith
- [ ] 5.1.14 Write error tests: validation failures

**Estimated effort**: High (8-12 hours)

---

#### Task 5.8: `$setWindowFields`
**Description**: Window functions for analytics (most complex stage).

**This task is broken into sub-phases due to complexity.**

**Syntax**:
```javascript
{
  $setWindowFields: {
    partitionBy: <expression>,        // optional
    sortBy: { <field>: 1|-1 },        // required for some operators
    output: {
      <field>: {
        <operator>: <expression>,
        window: {
          documents: [<lower>, <upper>],  // OR
          range: [<lower>, <upper>],
          unit: "<time-unit>"             // for time ranges
        }
      }
    }
  }
}
```

---

##### Sub-phase 5.8.A: Core Infrastructure

**Sub-tasks**:
- [x] 5.8.A.1 Add `$setWindowFields` stage handler structure
- [x] 5.8.A.2 Implement `partitionBy` grouping
- [x] 5.8.A.3 Implement `sortBy` ordering within partitions
- [x] 5.8.A.4 Implement window bounds parsing (`documents`, `range`)
- [x] 5.8.A.5 Implement special bounds: `"unbounded"`, `"current"`, integers
- [x] 5.8.A.6 Write infrastructure tests

**Estimated effort**: 4-6 hours

---

##### Sub-phase 5.8.B: Rank Operators

Operators with implicit windows (no window specification allowed).

| Operator | Behavior |
|----------|----------|
| `$rank` | Rank with gaps (1, 2, 2, 4) |
| `$denseRank` | Rank without gaps (1, 2, 2, 3) |
| `$documentNumber` | Sequential position (1, 2, 3, 4) |

**Sub-tasks**:
- [x] 5.8.B.1 Implement `$rank` with gap handling
- [x] 5.8.B.2 Implement `$denseRank` without gaps
- [x] 5.8.B.3 Implement `$documentNumber` sequential
- [ ] 5.8.B.4 Validate no window spec for rank operators
- [x] 5.8.B.5 Write tests for all rank operators
- [ ] 5.8.B.6 Write tests for tie handling

**Estimated effort**: 3-4 hours

---

##### Sub-phase 5.8.C: Accumulator Operators in Windows

Reuse existing accumulator logic with window bounds.

| Operator | Empty Window Result |
|----------|-------------------|
| `$sum`, `$count` | 0 |
| `$avg`, `$min`, `$max` | null |
| `$push`, `$addToSet` | [] |
| `$first`, `$last` | null |

**Sub-tasks**:
- [x] 5.8.C.1 Implement window document selection for `documents` bounds
- [x] 5.8.C.2 Implement window document selection for `range` bounds
- [ ] 5.8.C.3 Implement time range with `unit` parameter
- [x] 5.8.C.4 Wire up existing accumulators: `$sum`, `$avg`, `$min`, `$max`, `$count`
- [x] 5.8.C.5 Wire up array accumulators: `$push`, `$addToSet`
- [x] 5.8.C.6 Implement `$first`, `$last` for windows
- [x] 5.8.C.7 Write tests for each accumulator with various windows
- [ ] 5.8.C.8 Write tests for empty window results

**Estimated effort**: 6-8 hours

---

##### Sub-phase 5.8.D: Order & Shift Operators

| Operator | Description |
|----------|-------------|
| `$shift` | Access value at relative offset with default |

**Syntax**:
```javascript
{ $shift: { output: "$field", by: -1, default: null } }
```

**Sub-tasks**:
- [x] 5.8.D.1 Implement `$shift` with positive/negative offset
- [x] 5.8.D.2 Implement default value for out-of-bounds
- [x] 5.8.D.3 Write tests for various offsets
- [ ] 5.8.D.4 Write tests for partition boundaries

**Estimated effort**: 2-3 hours

---

##### Sub-phase 5.8.E: Derivative & Integration Operators

| Operator | Description |
|----------|-------------|
| `$derivative` | Rate of change |
| `$integral` | Area under curve |
| `$expMovingAvg` | Exponential moving average |

**Sub-tasks**:
- [ ] 5.8.E.1 Implement `$derivative` with unit support
- [ ] 5.8.E.2 Implement `$integral` with unit support
- [ ] 5.8.E.3 Implement `$expMovingAvg` with alpha parameter
- [ ] 5.8.E.4 Write tests for each operator
- [ ] 5.8.E.5 Write tests for time-based calculations

**Estimated effort**: 4-6 hours

---

##### Sub-phase 5.8.F: Gap Filling Operators

| Operator | Description |
|----------|-------------|
| `$linearFill` | Linear interpolation for nulls |
| `$locf` | Last observation carried forward |

**Sub-tasks**:
- [x] 5.8.F.1 Implement `$linearFill` expression operator
- [x] 5.8.F.2 Implement `$locf` expression operator
- [x] 5.8.F.3 Write tests for gap patterns
- [ ] 5.8.F.4 Write tests for all-null partitions

**Estimated effort**: 3-4 hours

---

##### Sub-phase 5.8.G: Statistical Operators

| Operator | Description |
|----------|-------------|
| `$covariancePop` | Population covariance |
| `$covarianceSamp` | Sample covariance |
| `$stdDevPop` | Population standard deviation (may already exist) |
| `$stdDevSamp` | Sample standard deviation (may already exist) |

**Sub-tasks**:
- [ ] 5.8.G.1 Implement `$covariancePop` for window
- [ ] 5.8.G.2 Implement `$covarianceSamp` for window
- [ ] 5.8.G.3 Wire up `$stdDevPop`, `$stdDevSamp` if not already
- [ ] 5.8.G.4 Write tests for all statistical operators

**Estimated effort**: 3-4 hours

---

## Error Handling Summary

All error messages should match MongoDB exactly. Key error patterns:

```typescript
// Stage validation errors
throw new Error("$<stage> requires '<field>' to be specified");

// Type errors
throw new Error("$<stage> requires '<field>' to be a <type>");

// Invalid value errors
throw new Error("$<stage> '<field>' must be <constraint>");
```

---

## Testing Strategy

### Dual-Target Testing
All tests should run against both MangoDB and real MongoDB:

```typescript
const testCases = [
  { name: "basic case", input: [...], expected: [...] },
  { name: "edge case", input: [...], expected: [...] },
];

for (const { name, input, expected } of testCases) {
  test(`$stage - ${name}`, async () => {
    // Test runs against both backends
    const result = await collection.aggregate(pipeline).toArray();
    expect(result).toEqual(expected);
  });
}
```

### Test Categories
1. **Happy path**: Basic functionality works
2. **Edge cases**: Empty inputs, nulls, single documents
3. **Error cases**: Invalid inputs throw correct errors
4. **Complex scenarios**: Nested documents, multiple stages

---

## Implementation Order

Based on dependencies and complexity:

```
┌─────────────────────────────────────────────────────┐
│                    TIER 0                           │
│            (Preparatory Utilities)                  │
├─────────────────────────────────────────────────────┤
│ 5.0.1 System Variables ──┬── 5.0.4 Traversal       │
│ 5.0.2 Partition Grouping │                          │
│ 5.0.3 Sort Partitions ───┼── 5.0.5 Gap Filling     │
│ 5.0.6 Date Stepping ─────┘                          │
└─────────────────────────────────────────────────────┘
                    │
                    v
┌─────────────────────────────────────────────────────┐
│                    TIER 1                           │
│              (Simple Aliases)                       │
├─────────────────────────────────────────────────────┤
│ 5.3 $replaceWith ─┬─ 5.4 $unset ─┬─ 5.5 $documents │
│     (parallel)    │   (parallel)  │    (parallel)   │
└─────────────────────────────────────────────────────┘
                    │
                    v
┌─────────────────────────────────────────────────────┐
│                    TIER 2                           │
│            (Moderate Complexity)                    │
├─────────────────────────────────────────────────────┤
│ 5.2 $redact ─────> 5.6 $densify ─────> 5.7 $fill   │
│  (uses 5.0.1,       (uses 5.0.2,        (uses 5.0.2│
│   5.0.4)             5.0.6)              5.0.3,    │
│                                          5.0.5)    │
└─────────────────────────────────────────────────────┘
                    │
                    v
┌─────────────────────────────────────────────────────┐
│                    TIER 3                           │
│              (High Complexity)                      │
├─────────────────────────────────────────────────────┤
│ 5.1 $graphLookup ──────> 5.8 $setWindowFields      │
│   (independent)          (uses 5.0.2, 5.0.3, 5.0.5)│
│                                                     │
│   5.8.A Core ──> 5.8.B Rank ──> 5.8.C Accumulators │
│        │                                            │
│        └──> 5.8.D Shift ──> 5.8.E Derivatives      │
│                    │                                │
│                    └──> 5.8.F Gap Fill ──> 5.8.G   │
└─────────────────────────────────────────────────────┘
                    │
                    v
              Phase 5 Complete
```

**Recommended execution order**:

1. **Tier 0 first** - Build utilities (can parallelize independent ones)
   - 5.0.1 + 5.0.4 in parallel (both independent)
   - 5.0.2 + 5.0.3 together (sort depends on partition)
   - 5.0.5 + 5.0.6 in parallel (both independent)

2. **Tier 1 parallel** - All three aliases can be done simultaneously

3. **Tier 2 sequential** - Each builds on prior work
   - `$redact` first (uses traversal utility)
   - `$densify` second (uses partition + date stepping)
   - `$fill` third (uses partition + sort + gap filling)

4. **Tier 3 parallel start**
   - `$graphLookup` is independent, can start anytime after Tier 0
   - `$setWindowFields` sub-phases are sequential

**Estimated total effort**:
- Tier 0: 8-13 hours
- Tier 1: 4-7 hours
- Tier 2: 16-22 hours
- Tier 3: 30-45 hours
- **Total: 58-87 hours** (reduced from original due to code reuse)

---

## Checklist for Phase 5 Completion

- [ ] All tests written and passing
- [ ] Dual-target tests verified against MongoDB
- [ ] Error messages match MongoDB exactly
- [ ] Edge cases covered (nulls, empty, single doc)
- [ ] Complex scenarios tested
- [ ] Code follows existing patterns in `cursor.ts`
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] ROADMAP.md Phase 5 marked complete
- [ ] Changes committed and pushed

---

## Appendix: MongoDB Error Codes Reference

| Code | Name | When Used |
|------|------|-----------|
| 40228 | - | `$replaceWith`/`$replaceRoot` non-object result |
| 40185 | - | `$graphLookup` invalid restrictSearchWithMatch |
| 31120 | - | `$unset` non-string field specification |
| 40352 | - | Empty field path |

---

*Document created following MangoDB development methodology. Refer to ROADMAP.md for overall project context.*
