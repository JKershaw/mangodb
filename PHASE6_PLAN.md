# Phase 6: Array Handling - Implementation Plan

This document outlines the detailed implementation plan for Phase 6 of Mongone, following TDD principles. All tests will run against both MongoDB and Mongone to ensure behavioral compatibility.

## Overview

Phase 6 adds array-specific query operators and array update operators to Mongone.

### Query Operators
1. `$elemMatch` - Match array element with multiple conditions
2. `$size` - Match array by length
3. `$all` - Match arrays containing all specified elements

### Update Operators
4. `$push` - Add element(s) to array
5. `$pull` - Remove elements matching condition
6. `$addToSet` - Add element if not present
7. `$pop` - Remove first or last element

---

## Step 1: `$elemMatch` Query Operator

### Behavior (from MongoDB docs)
- Matches documents that contain an array field with at least one element that matches ALL specified query criteria
- Key difference from implicit matching: with `$elemMatch`, all conditions must be satisfied by the SAME array element
- Works with embedded documents in arrays
- Can be combined with comparison operators

### Syntax
```typescript
{ field: { $elemMatch: { <query1>, <query2>, ... } } }
```

### Test Cases

#### Basic Tests
```typescript
// Test 1: Single condition (equivalent to direct match)
// Given: [{ scores: [10, 20, 30] }, { scores: [5, 15, 25] }]
{ scores: { $elemMatch: { $gte: 25 } } }
// Matches both documents

// Test 2: Multiple conditions on same element
// Given: [{ results: [{ score: 80, passed: true }, { score: 60, passed: false }] }]
{ results: { $elemMatch: { score: { $gte: 70 }, passed: true } } }
// Matches because first element satisfies BOTH conditions

// Test 3: Difference from implicit AND
// Given: [{ results: [{ score: 80, passed: false }, { score: 60, passed: true }] }]
// WITHOUT $elemMatch: { "results.score": { $gte: 70 }, "results.passed": true }
// Matches! (score>=70 on elem 0, passed=true on elem 1)
// WITH $elemMatch: { results: { $elemMatch: { score: { $gte: 70 }, passed: true } } }
// Does NOT match (no single element has both)
```

#### Edge Cases
```typescript
// Test 4: Empty array - should not match
{ scores: { $elemMatch: { $gt: 0 } } }
// on { scores: [] } -> no match

// Test 5: Non-array field - should not match
{ value: { $elemMatch: { $gt: 5 } } }
// on { value: 10 } -> no match (value is not an array)

// Test 6: Missing field - should not match
{ scores: { $elemMatch: { $gt: 0 } } }
// on { other: "field" } -> no match

// Test 7: Nested $elemMatch
{ matrix: { $elemMatch: { $elemMatch: { $gt: 5 } } } }
// on { matrix: [[1, 2], [6, 7]] } -> matches

// Test 8: $elemMatch with primitives
{ tags: { $elemMatch: { $eq: "red" } } }
// Equivalent to { tags: "red" } for primitives
```

#### Error Cases
```typescript
// Test 9: Invalid argument (not an object)
{ field: { $elemMatch: "invalid" } }
// Should throw error: "$elemMatch needs an Object"

// Test 10: Empty $elemMatch object
{ field: { $elemMatch: {} } }
// Matches any document where field is a non-empty array
```

### Implementation Notes
- Add `$elemMatch` to `QueryOperators` interface
- In `matchesOperators`, check if field value is an array
- For each array element, check if it matches ALL conditions in the $elemMatch expression
- Return true if ANY element matches all conditions

---

## Step 2: `$size` Query Operator

### Behavior (from MongoDB docs)
- Matches any array with exactly the specified number of elements
- Does NOT accept ranges or comparison operators
- Only works with arrays, not other types

### Syntax
```typescript
{ field: { $size: <number> } }
```

### Test Cases

#### Basic Tests
```typescript
// Test 1: Basic size match
// Given: [{ tags: ["a", "b"] }, { tags: ["a", "b", "c"] }]
{ tags: { $size: 2 } }
// Matches first document only

// Test 2: Size 0 matches empty arrays
{ items: { $size: 0 } }
// on { items: [] } -> matches

// Test 3: Size 1
{ items: { $size: 1 } }
// on { items: ["only"] } -> matches
```

#### Edge Cases
```typescript
// Test 4: Non-array field - no match
{ value: { $size: 1 } }
// on { value: "string" } -> no match

// Test 5: Missing field - no match
{ items: { $size: 0 } }
// on { other: "field" } -> no match (missing is not same as empty)

// Test 6: Null field - no match
{ items: { $size: 0 } }
// on { items: null } -> no match
```

#### Error Cases
```typescript
// Test 7: Non-integer size (MongoDB behavior)
{ tags: { $size: 2.5 } }
// MongoDB throws: "$size must be a whole number"

// Test 8: Negative size
{ tags: { $size: -1 } }
// MongoDB throws: "$size may not be negative"

// Test 9: $size does not accept comparison operators
{ tags: { $size: { $gt: 2 } } }
// This is a valid query but matches literally nothing
// (no array has length equal to the object { $gt: 2 })
```

### Implementation Notes
- Add `$size` to `QueryOperators` interface
- Validate that the value is a non-negative integer
- Check if field value is an array and has exactly that length

---

## Step 3: `$all` Query Operator

### Behavior (from MongoDB docs)
- Matches arrays that contain ALL elements specified in the query array
- Order does not matter
- The document array can contain additional elements
- Equivalent to `$and` with multiple element matches

### Syntax
```typescript
{ field: { $all: [ <value1>, <value2>, ... ] } }
```

### Test Cases

#### Basic Tests
```typescript
// Test 1: Basic all match
// Given: [{ tags: ["a", "b", "c"] }, { tags: ["a", "d"] }]
{ tags: { $all: ["a", "b"] } }
// Matches first document only

// Test 2: Single element (equivalent to direct match)
{ tags: { $all: ["a"] } }
// Matches both documents

// Test 3: Order doesn't matter
{ tags: { $all: ["c", "a"] } }
// on { tags: ["a", "b", "c"] } -> matches

// Test 4: Exact match subset
{ tags: { $all: ["a", "b", "c"] } }
// on { tags: ["a", "b", "c"] } -> matches
// on { tags: ["a", "b", "c", "d"] } -> also matches
```

#### Edge Cases
```typescript
// Test 5: Empty $all array - matches all documents with array field
{ tags: { $all: [] } }
// MongoDB: matches any document where tags is an array (including empty)

// Test 6: Non-array field - no match
{ value: { $all: [1] } }
// on { value: 1 } -> no match

// Test 7: Missing field - no match
{ tags: { $all: ["a"] } }
// on { other: "field" } -> no match

// Test 8: $all with nested arrays
{ matrix: { $all: [[1, 2]] } }
// on { matrix: [[1, 2], [3, 4]] } -> matches

// Test 9: Duplicate values in $all
{ tags: { $all: ["a", "a", "b"] } }
// Equivalent to { tags: { $all: ["a", "b"] } }
```

#### Combined with $elemMatch
```typescript
// Test 10: $all with $elemMatch for complex conditions
{ results: { $all: [{ $elemMatch: { score: { $gte: 80 } } }] } }
// Matches if any element has score >= 80
```

#### Error Cases
```typescript
// Test 11: $all with non-array value
{ tags: { $all: "invalid" } }
// MongoDB throws: "$all needs an array"
```

### Implementation Notes
- Add `$all` to `QueryOperators` interface
- Validate that the value is an array
- Check if the document field is an array
- Check if every element in $all array is contained in the document array
- Handle `$elemMatch` inside `$all` specially

---

## Step 4: `$push` Update Operator

### Behavior (from MongoDB docs)
- Appends a specified value to an array
- If the field is absent, creates a new array with the value
- If the field is not an array, the operation fails
- With `$each` modifier, can push multiple values

### Syntax
```typescript
{ $push: { <field>: <value> } }
{ $push: { <field>: { $each: [ <value1>, <value2>, ... ] } } }
```

### Test Cases

#### Basic Tests
```typescript
// Test 1: Push single value
// updateOne({ name: "Alice" }, { $push: { tags: "new" } })
// on { name: "Alice", tags: ["a", "b"] }
// result: { name: "Alice", tags: ["a", "b", "new"] }

// Test 2: Push creates array if missing
// updateOne({ name: "Alice" }, { $push: { tags: "first" } })
// on { name: "Alice" }
// result: { name: "Alice", tags: ["first"] }

// Test 3: Push to nested array (dot notation)
// updateOne({}, { $push: { "user.tags": "tag" } })
// on { user: { tags: ["a"] } }
// result: { user: { tags: ["a", "tag"] } }

// Test 4: Push object to array
// updateOne({}, { $push: { items: { name: "new", value: 1 } } })
// result: items array has new object appended

// Test 5: Push with $each modifier
// updateOne({}, { $push: { tags: { $each: ["x", "y", "z"] } } })
// on { tags: ["a"] }
// result: { tags: ["a", "x", "y", "z"] }
```

#### Edge Cases
```typescript
// Test 6: Push array as single element (without $each)
// updateOne({}, { $push: { matrix: [1, 2, 3] } })
// on { matrix: [[0, 0]] }
// result: { matrix: [[0, 0], [1, 2, 3]] }

// Test 7: Push to empty array
// on { tags: [] }
// $push: { tags: "first" }
// result: { tags: ["first"] }

// Test 8: Push with $each and empty array
// $push: { tags: { $each: [] } }
// No change to the array
```

#### Error Cases
```typescript
// Test 9: Push to non-array field
// updateOne({}, { $push: { name: "value" } })
// on { name: "Alice" }
// Error: "The field 'name' must be an array but is of type string"

// Test 10: Push to null field
// on { tags: null }
// Error: "The field 'tags' must be an array but is of type null"
```

### Implementation Notes
- Add `$push` to `UpdateOperators` interface
- Check if field exists and is an array
- If field doesn't exist, create new array
- If field exists but isn't array, throw error
- Handle `$each` modifier

---

## Step 5: `$pull` Update Operator

### Behavior (from MongoDB docs)
- Removes all instances of a value or values that match a specified condition
- Can use query operators for complex matching
- If the field is not an array, the operation fails

### Syntax
```typescript
{ $pull: { <field>: <value or condition> } }
```

### Test Cases

#### Basic Tests
```typescript
// Test 1: Pull exact value
// updateOne({}, { $pull: { tags: "remove" } })
// on { tags: ["keep", "remove", "keep2", "remove"] }
// result: { tags: ["keep", "keep2"] }

// Test 2: Pull with query condition
// updateOne({}, { $pull: { scores: { $lt: 50 } } })
// on { scores: [20, 60, 40, 80] }
// result: { scores: [60, 80] }

// Test 3: Pull object matching condition
// updateOne({}, { $pull: { items: { status: "deleted" } } })
// on { items: [{ name: "a", status: "active" }, { name: "b", status: "deleted" }] }
// result: { items: [{ name: "a", status: "active" }] }

// Test 4: Pull nested field (dot notation)
// updateOne({}, { $pull: { "user.tags": "old" } })
```

#### Edge Cases
```typescript
// Test 5: Pull from empty array (no-op)
// on { tags: [] }
// $pull: { tags: "x" }
// result: { tags: [] }

// Test 6: Pull non-existent value (no-op)
// on { tags: ["a", "b"] }
// $pull: { tags: "x" }
// result: { tags: ["a", "b"] }

// Test 7: Pull from missing field (no-op)
// on { name: "Alice" }
// $pull: { tags: "x" }
// result: no change (field stays missing)

// Test 8: Pull all elements
// on { scores: [1, 2, 3] }
// $pull: { scores: { $gte: 0 } }
// result: { scores: [] }
```

#### Error Cases
```typescript
// Test 9: Pull from non-array field
// on { name: "Alice" }
// $pull: { name: "x" }
// Error: "Cannot apply $pull to a non-array value"

// Test 10: Pull from null field
// on { tags: null }
// $pull: { tags: "x" }
// Error: "Cannot apply $pull to a non-array value"
```

### Implementation Notes
- Add `$pull` to `UpdateOperators` interface
- If field doesn't exist, no-op
- If field exists but isn't array, throw error
- Filter array to remove matching elements
- Support both exact value matching and query operators

---

## Step 6: `$addToSet` Update Operator

### Behavior (from MongoDB docs)
- Adds a value to an array unless the value is already present
- Does not affect existing duplicate elements (only prevents new duplicates)
- Uses deep equality for objects
- With `$each` modifier, can add multiple values

### Syntax
```typescript
{ $addToSet: { <field>: <value> } }
{ $addToSet: { <field>: { $each: [ <value1>, <value2>, ... ] } } }
```

### Test Cases

#### Basic Tests
```typescript
// Test 1: Add new value
// updateOne({}, { $addToSet: { tags: "new" } })
// on { tags: ["a", "b"] }
// result: { tags: ["a", "b", "new"] }

// Test 2: Value already exists (no-op for that value)
// updateOne({}, { $addToSet: { tags: "a" } })
// on { tags: ["a", "b"] }
// result: { tags: ["a", "b"] } (unchanged)

// Test 3: Creates array if missing
// updateOne({}, { $addToSet: { tags: "first" } })
// on { name: "Alice" }
// result: { name: "Alice", tags: ["first"] }

// Test 4: $each modifier
// updateOne({}, { $addToSet: { tags: { $each: ["x", "y", "a"] } } })
// on { tags: ["a", "b"] }
// result: { tags: ["a", "b", "x", "y"] } ("a" already exists)
```

#### Edge Cases
```typescript
// Test 5: Object equality (deep comparison)
// updateOne({}, { $addToSet: { items: { id: 1, name: "x" } } })
// on { items: [{ id: 1, name: "x" }] }
// result: unchanged (object already exists)

// Test 6: Object with different key order
// updateOne({}, { $addToSet: { items: { name: "x", id: 1 } } })
// on { items: [{ id: 1, name: "x" }] }
// MongoDB: Objects with same keys but different order are DIFFERENT
// result: { items: [{ id: 1, name: "x" }, { name: "x", id: 1 }] }

// Test 7: Add to empty array
// on { tags: [] }
// $addToSet: { tags: "first" }
// result: { tags: ["first"] }

// Test 8: Add array as element (without $each)
// $addToSet: { matrix: [1, 2] }
// on { matrix: [[0, 0]] }
// result: { matrix: [[0, 0], [1, 2]] }
```

#### Error Cases
```typescript
// Test 9: addToSet on non-array field
// on { name: "Alice" }
// $addToSet: { name: "x" }
// Error: "The field 'name' must be an array but is of type string"

// Test 10: addToSet on null field
// on { tags: null }
// Error: "The field 'tags' must be an array but is of type null"
```

### Implementation Notes
- Add `$addToSet` to `UpdateOperators` interface
- Similar to `$push` but check for existing values first
- Use deep equality for comparison
- Handle `$each` modifier

---

## Step 7: `$pop` Update Operator

### Behavior (from MongoDB docs)
- Removes the first or last element of an array
- `-1` removes the first element
- `1` removes the last element
- If the array is empty, no-op

### Syntax
```typescript
{ $pop: { <field>: -1 } }  // Remove first
{ $pop: { <field>: 1 } }   // Remove last
```

### Test Cases

#### Basic Tests
```typescript
// Test 1: Pop last element
// updateOne({}, { $pop: { items: 1 } })
// on { items: ["a", "b", "c"] }
// result: { items: ["a", "b"] }

// Test 2: Pop first element
// updateOne({}, { $pop: { items: -1 } })
// on { items: ["a", "b", "c"] }
// result: { items: ["b", "c"] }

// Test 3: Pop with dot notation
// updateOne({}, { $pop: { "user.tags": 1 } })
// on { user: { tags: ["a", "b"] } }
// result: { user: { tags: ["a"] } }
```

#### Edge Cases
```typescript
// Test 4: Pop from empty array (no-op)
// on { items: [] }
// $pop: { items: 1 }
// result: { items: [] }

// Test 5: Pop from single-element array
// on { items: ["only"] }
// $pop: { items: 1 }
// result: { items: [] }

// Test 6: Pop from missing field (no-op)
// on { name: "Alice" }
// $pop: { items: 1 }
// result: no change
```

#### Error Cases
```typescript
// Test 7: Pop from non-array field
// on { name: "Alice" }
// $pop: { name: 1 }
// Error: "Cannot apply $pop to a non-array value"

// Test 8: Pop from null field
// on { items: null }
// $pop: { items: 1 }
// Error: "Cannot apply $pop to a non-array value"

// Test 9: Invalid pop value (not 1 or -1)
// $pop: { items: 0 }
// Error: "$pop expects 1 or -1"

// Test 10: Invalid pop value (not a number)
// $pop: { items: "last" }
// Error: "Expected a number in: items: \"last\""
```

### Implementation Notes
- Add `$pop` to `UpdateOperators` interface
- Validate value is 1 or -1
- If field doesn't exist, no-op
- If field exists but isn't array, throw error
- Remove first or last element based on value

---

## Implementation Order

### Recommended Order (by complexity and dependency)

1. **`$size`** - Simplest, no dependencies
2. **`$all`** - Simple, may use existing equality checks
3. **`$elemMatch`** - More complex, recursive matching
4. **`$pop`** - Simplest update operator
5. **`$push`** - Foundation for other update operators
6. **`$addToSet`** - Similar to `$push`, adds uniqueness check
7. **`$pull`** - Most complex, requires query matching

### File Changes Required

#### `src/collection.ts`
```typescript
// Extend QueryOperators interface
interface QueryOperators {
  // ... existing operators ...
  $elemMatch?: Record<string, unknown>;
  $size?: number;
  $all?: unknown[];
}

// Extend UpdateOperators interface
interface UpdateOperators {
  // ... existing operators ...
  $push?: Record<string, unknown>;
  $pull?: Record<string, unknown>;
  $addToSet?: Record<string, unknown>;
  $pop?: Record<string, number>;
}
```

#### `test/arrays.test.ts` (new file)
- All array query operator tests
- All array update operator tests
- Following existing test patterns from `logical.test.ts`

---

## Error Messages (to match MongoDB)

| Operator | Condition | Error Message |
|----------|-----------|---------------|
| `$elemMatch` | Non-object argument | `$elemMatch needs an Object` |
| `$size` | Negative number | `$size may not be negative` |
| `$size` | Non-integer | `$size must be a whole number` |
| `$all` | Non-array argument | `$all needs an array` |
| `$push` | Field is not array | `The field '<field>' must be an array but is of type <type>` |
| `$pull` | Field is not array | `Cannot apply $pull to a non-array value` |
| `$addToSet` | Field is not array | `The field '<field>' must be an array but is of type <type>` |
| `$pop` | Field is not array | `Cannot apply $pop to a non-array value` |
| `$pop` | Value not 1 or -1 | `$pop expects 1 or -1` |
| `$pop` | Non-numeric value | `Expected a number in: <field>: <value>` |

---

## Test File Structure

```
test/arrays.test.ts
├── Array Query Tests (${getTestModeName()})
│   ├── $size operator
│   │   ├── should match arrays with exact size
│   │   ├── should match empty arrays with size 0
│   │   ├── should not match non-array fields
│   │   ├── should not match missing fields
│   │   ├── should throw error for negative size
│   │   └── should throw error for non-integer size
│   ├── $all operator
│   │   ├── should match arrays containing all specified elements
│   │   ├── should not match when missing any element
│   │   ├── should ignore element order
│   │   ├── should match when array has extra elements
│   │   ├── should handle empty $all array
│   │   ├── should not match non-array fields
│   │   └── should throw error for non-array argument
│   └── $elemMatch operator
│       ├── should match when single element satisfies all conditions
│       ├── should not match when conditions satisfied by different elements
│       ├── should work with comparison operators
│       ├── should work with embedded documents
│       ├── should not match empty arrays
│       ├── should not match non-array fields
│       └── should throw error for non-object argument
│
├── Array Update Tests (${getTestModeName()})
│   ├── $push operator
│   │   ├── should append value to array
│   │   ├── should create array if field missing
│   │   ├── should work with dot notation
│   │   ├── should handle $each modifier
│   │   ├── should throw error for non-array field
│   │   └── should throw error for null field
│   ├── $pull operator
│   │   ├── should remove matching values
│   │   ├── should remove values matching condition
│   │   ├── should handle missing field
│   │   ├── should handle empty array
│   │   └── should throw error for non-array field
│   ├── $addToSet operator
│   │   ├── should add value if not present
│   │   ├── should not add duplicate value
│   │   ├── should create array if field missing
│   │   ├── should handle $each modifier
│   │   ├── should use deep equality for objects
│   │   └── should throw error for non-array field
│   └── $pop operator
│       ├── should remove last element with 1
│       ├── should remove first element with -1
│       ├── should handle empty array
│       ├── should handle missing field
│       ├── should throw error for non-array field
│       └── should throw error for invalid value
```

---

## Documentation Updates

After implementation, update:

1. **PROGRESS.md** - Add Phase 6 changelog entry
2. **ROADMAP.md** - Mark Phase 6 as complete, update current phase to 7
3. **COMPATIBILITY.md** - Add array operator behaviors section
4. **README.md** - Update "What's Implemented" section

---

## Sources

- [$elemMatch (query) - MongoDB Docs](https://www.mongodb.com/docs/manual/reference/operator/query/elemmatch/)
- [$size - MongoDB Docs](https://www.mongodb.com/docs/manual/reference/operator/query/size/)
- [$all - MongoDB Docs](https://www.mongodb.com/docs/manual/reference/operator/query/all/)
- [Array Update Operators - MongoDB Docs](https://www.mongodb.com/docs/manual/reference/operator/update-array/)
- [$push - MongoDB Docs](https://www.mongodb.com/docs/manual/reference/operator/update/push/)
- [$pull - MongoDB Docs](https://www.mongodb.com/docs/manual/reference/operator/update/pull/)
- [$addToSet - MongoDB Docs](https://www.mongodb.com/docs/manual/reference/operator/update/addtoset/)
