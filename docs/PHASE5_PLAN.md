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
- [ ] 5.6.1 Add validation for field name, step, bounds
- [ ] 5.6.2 Implement partition grouping logic
- [ ] 5.6.3 Implement numeric gap detection and filling
- [ ] 5.6.4 Implement date gap detection with time units
- [ ] 5.6.5 Handle bounds (explicit vs inferred from data)
- [ ] 5.6.6 Write tests: numeric field without partitions
- [ ] 5.6.7 Write tests: date field with time units
- [ ] 5.6.8 Write tests: partitioned densification
- [ ] 5.6.9 Write tests: explicit bounds
- [ ] 5.6.10 Write error tests: validation failures

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
- [ ] 5.7.1 Add validation for sortBy, partitionBy, output
- [ ] 5.7.2 Implement partition grouping (both forms)
- [ ] 5.7.3 Implement `value` fill method
- [ ] 5.7.4 Implement `locf` fill method with forward scan
- [ ] 5.7.5 Implement `linear` interpolation with duplicate check
- [ ] 5.7.6 Write tests: value fill with expressions
- [ ] 5.7.7 Write tests: locf with various null patterns
- [ ] 5.7.8 Write tests: linear interpolation
- [ ] 5.7.9 Write tests: partitioned filling
- [ ] 5.7.10 Write error tests: missing sortBy, duplicates

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
- [ ] 5.8.A.1 Add `$setWindowFields` stage handler structure
- [ ] 5.8.A.2 Implement `partitionBy` grouping
- [ ] 5.8.A.3 Implement `sortBy` ordering within partitions
- [ ] 5.8.A.4 Implement window bounds parsing (`documents`, `range`)
- [ ] 5.8.A.5 Implement special bounds: `"unbounded"`, `"current"`, integers
- [ ] 5.8.A.6 Write infrastructure tests

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
- [ ] 5.8.B.1 Implement `$rank` with gap handling
- [ ] 5.8.B.2 Implement `$denseRank` without gaps
- [ ] 5.8.B.3 Implement `$documentNumber` sequential
- [ ] 5.8.B.4 Validate no window spec for rank operators
- [ ] 5.8.B.5 Write tests for all rank operators
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
- [ ] 5.8.C.1 Implement window document selection for `documents` bounds
- [ ] 5.8.C.2 Implement window document selection for `range` bounds
- [ ] 5.8.C.3 Implement time range with `unit` parameter
- [ ] 5.8.C.4 Wire up existing accumulators: `$sum`, `$avg`, `$min`, `$max`, `$count`
- [ ] 5.8.C.5 Wire up array accumulators: `$push`, `$addToSet`
- [ ] 5.8.C.6 Implement `$first`, `$last` for windows
- [ ] 5.8.C.7 Write tests for each accumulator with various windows
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
- [ ] 5.8.D.1 Implement `$shift` with positive/negative offset
- [ ] 5.8.D.2 Implement default value for out-of-bounds
- [ ] 5.8.D.3 Write tests for various offsets
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
- [ ] 5.8.F.1 Implement `$linearFill` expression operator
- [ ] 5.8.F.2 Implement `$locf` expression operator
- [ ] 5.8.F.3 Write tests for gap patterns
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
Phase 5.3 ($replaceWith) ─┐
Phase 5.4 ($unset) ───────┼─> Tier 1 Complete
Phase 5.5 ($documents) ───┘
         │
         v
Phase 5.2 ($redact) ──────┐
Phase 5.6 ($densify) ─────┼─> Tier 2 Complete
Phase 5.7 ($fill) ────────┘
         │
         v
Phase 5.1 ($graphLookup) ─┐
Phase 5.8.A-G ────────────┼─> Tier 3 Complete
($setWindowFields)        ┘
         │
         v
     Phase 5 Complete
```

**Recommended order**:
1. Start with Tier 1 (simple aliases) for quick wins
2. Move to Tier 2 (moderate complexity)
3. Complete `$graphLookup`
4. Tackle `$setWindowFields` sub-phases incrementally

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
