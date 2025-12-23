# Phase 12: Additional Query Operators - Implementation Plan

## Overview

Add remaining useful query operators to MangoDB: `$type`, `$mod`, `$expr`, and `$text`. These operators enable type checking, modular arithmetic queries, comparing fields within the same document, and simple text search.

**Priority**: MEDIUM — Enhances query expressiveness
**Estimated Tests**: 45-55

---

## Status

| Step | Operator | Status | Notes |
|------|----------|--------|-------|
| 1 | `$type` | ⬜ TODO | Type checking with BSON aliases |
| 2 | `$mod` | ⬜ TODO | Modulo arithmetic queries |
| 3 | `$expr` | ⬜ TODO | Compare fields using aggregation expressions |
| 4 | `$text` | ✅ COMPLETE | Implemented in collection.ts with 18 tests |

---

## Operations

### Step 1: `$type` Query Operator

**Syntax**:
```typescript
// String alias
{ field: { $type: "string" } }

// Numeric BSON type code
{ field: { $type: 2 } }

// Array of types (match any)
{ field: { $type: ["string", "null"] } }
```

**BSON Types and Aliases**:

| Type | Number | Alias | Notes |
|------|--------|-------|-------|
| Double | 1 | `"double"` | Floating-point numbers |
| String | 2 | `"string"` | |
| Object | 3 | `"object"` | Embedded documents |
| Array | 4 | `"array"` | |
| Binary data | 5 | `"binData"` | |
| Undefined | 6 | `"undefined"` | Deprecated |
| ObjectId | 7 | `"objectId"` | |
| Boolean | 8 | `"bool"` | |
| Date | 9 | `"date"` | |
| Null | 10 | `"null"` | |
| Regular Expression | 11 | `"regex"` | |
| JavaScript | 13 | `"javascript"` | |
| 32-bit integer | 16 | `"int"` | |
| Timestamp | 17 | `"timestamp"` | |
| 64-bit integer | 18 | `"long"` | |
| Decimal128 | 19 | `"decimal"` | |
| Min key | -1 | `"minKey"` | |
| Max key | 127 | `"maxKey"` | |

**Special alias**:
- `"number"` — Matches `int`, `long`, `double`, and `decimal` (any numeric type)

**Behavior**:
- Match documents where field is the specified BSON type
- Missing fields do NOT match any type (including `"null"`)
- For array fields: matches if the field itself is an array (type 4)
- In JavaScript context: use JavaScript types for detection since we don't have full BSON

**JavaScript Type Mapping** (for MangoDB implementation):
| BSON Alias | JavaScript Detection |
|------------|---------------------|
| `"string"` | `typeof value === "string"` |
| `"number"` / `"double"` | `typeof value === "number"` |
| `"int"` | `typeof value === "number" && Number.isInteger(value)` |
| `"bool"` | `typeof value === "boolean"` |
| `"object"` | `typeof value === "object" && !Array.isArray(value) && value !== null && !(value instanceof Date) && !(value instanceof RegExp)` |
| `"array"` | `Array.isArray(value)` |
| `"null"` | `value === null` |
| `"date"` | `value instanceof Date` |
| `"regex"` | `value instanceof RegExp` |
| `"objectId"` | `value instanceof ObjectId` |
| `"undefined"` | `value === undefined` (but missing fields return false) |

**Test Cases**:
```typescript
// Match string type
await collection.find({ value: { $type: "string" } }).toArray();

// Match number type (any numeric)
await collection.find({ value: { $type: "number" } }).toArray();

// Match array type
await collection.find({ data: { $type: "array" } }).toArray();

// Match object type (embedded document)
await collection.find({ address: { $type: "object" } }).toArray();

// Match boolean type
await collection.find({ active: { $type: "bool" } }).toArray();

// Match null type
await collection.find({ deleted: { $type: "null" } }).toArray();

// Match date type
await collection.find({ createdAt: { $type: "date" } }).toArray();

// Match objectId type
await collection.find({ _id: { $type: "objectId" } }).toArray();

// Match using numeric type code
await collection.find({ value: { $type: 2 } }).toArray(); // string

// Match multiple types (array syntax)
await collection.find({ value: { $type: ["string", "null"] } }).toArray();
```

**Edge Cases**:
```typescript
// Missing field does NOT match null type
// Given: { name: "Alice" } (no "deleted" field)
await collection.find({ deleted: { $type: "null" } }).toArray(); // No match

// Missing field does NOT match any type
await collection.find({ missing: { $type: "string" } }).toArray(); // No match

// Integer detection
// Given: { value: 42 }
await collection.find({ value: { $type: "int" } }).toArray(); // Match (42 is integer)
// Given: { value: 42.5 }
await collection.find({ value: { $type: "int" } }).toArray(); // No match

// Nested field type check
await collection.find({ "address.city": { $type: "string" } }).toArray();

// Array field - checks the array itself, not elements
// Given: { tags: ["a", "b"] }
await collection.find({ tags: { $type: "array" } }).toArray(); // Matches
await collection.find({ tags: { $type: "string" } }).toArray(); // No match (tags is array, not string)
```

**Error Cases**:
```typescript
// Invalid type alias (case-sensitive, must be lowercase)
await collection.find({ value: { $type: "String" } }).toArray();
// Error: "Unknown type name alias: String"

// Unknown type alias
await collection.find({ value: { $type: "unknown" } }).toArray();
// Error: "Unknown type name alias: unknown"

// Invalid numeric type code
await collection.find({ value: { $type: 999 } }).toArray();
// Error: "Invalid numerical type code: 999"
```

---

### Step 2: `$mod` Query Operator

**Syntax**:
```typescript
{ field: { $mod: [divisor, remainder] } }
```

**Behavior**:
- Select documents where `field % divisor === remainder`
- Requires exactly 2 elements in the array
- Works with numeric values only
- Non-numeric fields do not match (no error)
- Uses truncation towards zero for decimals

**Test Cases**:
```typescript
// Find even numbers (divisible by 2, remainder 0)
await collection.find({ value: { $mod: [2, 0] } }).toArray();

// Find odd numbers
await collection.find({ value: { $mod: [2, 1] } }).toArray();

// Every 3rd item (index 0, 3, 6, ...)
await collection.find({ index: { $mod: [3, 0] } }).toArray();

// Items with remainder 2 when divided by 5
await collection.find({ score: { $mod: [5, 2] } }).toArray();

// Floating point divisor (truncated toward zero)
await collection.find({ value: { $mod: [2.5, 0] } }).toArray(); // Uses 2 as divisor

// Negative numbers
await collection.find({ value: { $mod: [4, -1] } }).toArray();
```

**Edge Cases**:
```typescript
// Non-numeric fields - silently skip (no match, no error)
await collection.find({ name: { $mod: [2, 0] } }).toArray(); // Never matches strings

// Null/undefined values - no match
await collection.find({ value: { $mod: [2, 0] } }).toArray(); // Won't match null values

// Document field is NaN or Infinity - excluded from results
// Given: { value: NaN } or { value: Infinity }
await collection.find({ value: { $mod: [2, 0] } }).toArray(); // No match

// Negative divisor
await collection.find({ value: { $mod: [-3, 0] } }).toArray(); // Valid

// Zero remainder
await collection.find({ value: { $mod: [10, 0] } }).toArray(); // Multiples of 10

// Large numbers
await collection.find({ value: { $mod: [1000000, 0] } }).toArray();
```

**Error Cases**:
```typescript
// Not enough elements (1 element)
await collection.find({ qty: { $mod: [4] } }).toArray();
// Error: "malformed mod, not enough elements" (code 16810)

// Too many elements (4 elements)
await collection.find({ qty: { $mod: [4, 1, 2, 3] } }).toArray();
// Error: "malformed mod, too many elements" (code 16810)

// Empty array
await collection.find({ qty: { $mod: [] } }).toArray();
// Error: "malformed mod, not enough elements"

// NaN as divisor (MongoDB 5.1+)
await collection.find({ qty: { $mod: [NaN, 0] } }).toArray();
// Error: "malformed mod, divisor value is invalid :: caused by :: NaN is an invalid argument"

// Infinity as divisor (MongoDB 5.1+)
await collection.find({ qty: { $mod: [Infinity, 0] } }).toArray();
// Error: "malformed mod, divisor value is invalid :: caused by :: Infinity is an invalid argument"

// Zero as divisor
await collection.find({ qty: { $mod: [0, 0] } }).toArray();
// Error: "divisor cannot be 0"

// Non-array argument
await collection.find({ qty: { $mod: 2 } }).toArray();
// Error: "malformed mod, needs to be an array"

// Non-numeric elements
await collection.find({ qty: { $mod: ["two", "zero"] } }).toArray();
// Error: "malformed mod, divisor not a number"
```

---

### Step 3: `$expr` Query Operator

**Syntax**:
```typescript
{ $expr: <aggregation expression> }
```

**Behavior**:
- Allow aggregation expressions in query predicates
- Compare fields within the same document
- Uses expression operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
- Supports arithmetic operators: `$add`, `$subtract`, `$multiply`, `$divide`
- Field references use `$fieldName` syntax
- Reuses existing expression evaluation from aggregation module

**Test Cases**:
```typescript
// Compare two fields in same document
await collection.find({
  $expr: { $gt: ["$quantity", "$threshold"] }
}).toArray();

// Compare field to constant
await collection.find({
  $expr: { $gte: ["$score", 100] }
}).toArray();

// With arithmetic - find where sum exceeds limit
await collection.find({
  $expr: { $lt: [{ $add: ["$price", "$tax"] }, "$budget"] }
}).toArray();

// Equality comparison between fields
await collection.find({
  $expr: { $eq: ["$firstName", "$preferredName"] }
}).toArray();

// Nested field comparison
await collection.find({
  $expr: { $gt: ["$stats.current", "$stats.previous"] }
}).toArray();

// Multiple conditions combined with $and
await collection.find({
  $expr: {
    $and: [
      { $gt: ["$quantity", 0] },
      { $lt: ["$quantity", "$maxStock"] }
    ]
  }
}).toArray();

// With $or
await collection.find({
  $expr: {
    $or: [
      { $gt: ["$score", 90] },
      { $eq: ["$bonus", true] }
    ]
  }
}).toArray();

// Combined with regular query operators
await collection.find({
  status: "active",
  $expr: { $gt: ["$sold", "$target"] }
}).toArray();
```

**Edge Cases**:
```typescript
// Missing field - evaluates to null in expression
// Given: { a: 10 } (no field "b")
await collection.find({ $expr: { $gt: ["$a", "$b"] } }).toArray();
// $b evaluates to null, comparison with null returns false

// Array field reference
// Given: { items: [1, 2, 3] }
await collection.find({ $expr: { $gt: [{ $size: "$items" }, 2] } }).toArray();

// Boolean result used directly
await collection.find({ $expr: "$active" }).toArray(); // Matches where active is truthy

// Literal values
await collection.find({
  $expr: { $eq: [{ $literal: "$notAField" }, "$fieldName"] }
}).toArray();
```

**Error Cases**:
```typescript
// Invalid expression operator
await collection.find({
  $expr: { $unknown: ["$a", "$b"] }
}).toArray();
// Error: "Unrecognized expression '$unknown'"

// Field path must start with $
// (This is handled by expression evaluation - invalid paths are treated as literals)
```

**Implementation Notes**:
- Reuse `evaluateExpression` from `aggregation.ts`
- `$expr` should be handled at the top level of filter matching
- Field references resolve against the current document being matched

---

### Step 4: `$text` Query Operator (Simplified) ✅ COMPLETE

**Syntax**:
```typescript
// Create text index first
await collection.createIndex({ title: "text", description: "text" });

// Then use $text query
{ $text: { $search: "mongodb database" } }
```

**Behavior**:
- Requires a text index to be created on the collection first
- Text index records which fields to search (no actual indexing for performance)
- `$search` tokenizes by whitespace and matches if ANY token is found
- Case-insensitive matching by default
- Searches all text-indexed fields

**Simplified Implementation** (no performance optimization needed):
1. `createIndex({ field: "text" })` stores field name as a text field
2. `$text` query does full scan, checking text fields for token matches
3. Match if any search token appears as substring in any text field

**Test Cases**:
```typescript
// Create text index
await collection.createIndex({ title: "text" });

// Basic text search - finds documents containing "mongodb"
await collection.find({ $text: { $search: "mongodb" } }).toArray();

// Multiple words - matches if ANY word is found
await collection.find({ $text: { $search: "mongodb database" } }).toArray();

// Case insensitive
await collection.find({ $text: { $search: "MONGODB" } }).toArray();

// Compound text index (multiple fields)
await collection.createIndex({ title: "text", body: "text" });
await collection.find({ $text: { $search: "urgent" } }).toArray();
// Searches both title and body fields
```

**Edge Cases**:
```typescript
// No text index - should throw error
await collection.find({ $text: { $search: "test" } }).toArray();
// Error: "text index required for $text query" (code 27)

// Empty search string
await collection.find({ $text: { $search: "" } }).toArray();
// Returns no documents (no tokens to match)

// Field value is null or missing - no match
// Given: { title: null }
await collection.find({ $text: { $search: "anything" } }).toArray();
// No match

// Non-string field values - silently skip
// Given: { title: 123 }
await collection.find({ $text: { $search: "123" } }).toArray();
// No match (we only search strings)

// Partial word match (substring)
// Given: { title: "introduction to mongodb" }
await collection.find({ $text: { $search: "mongo" } }).toArray();
// Matches (substring match)
```

**Error Cases**:
```typescript
// No text index exists
await collection.find({ $text: { $search: "test" } }).toArray();
// Error: "text index required for $text query" (IndexNotFound, code 27)

// $text without $search
await collection.find({ $text: {} }).toArray();
// Error: "$text requires $search"

// $search is not a string
await collection.find({ $text: { $search: 123 } }).toArray();
// Error: "$search must be a string"
```

**What We Skip** (intentionally simplified):
| MongoDB Feature | Our Approach |
|-----------------|--------------|
| Inverted index | Full scan |
| Stemming | Exact substring match |
| Stop words | Include all words |
| Text score | No scoring |
| Phrase matching (`"exact phrase"`) | Not implemented |
| Negation (`-word`) | Not implemented |
| Language options | Not implemented |

---

## Implementation Order

1. **$type operator** (Step 1)
   - Add to `QueryOperators` interface in `types.ts`
   - Implement type matching in `query-matcher.ts`
   - Create BSON type mapping

2. **$mod operator** (Step 2)
   - Add to `QueryOperators` interface in `types.ts`
   - Implement modulo matching in `query-matcher.ts`
   - Handle error cases with proper messages

3. **$expr operator** (Step 3)
   - Add `$expr` support to `Filter` type in `types.ts`
   - Handle `$expr` in `matchesFilter` function
   - Reuse expression evaluation from aggregation module

4. **$text operator** (Step 4)
   - Add text index support in `index-manager.ts`
   - Add `$text` handling in `collection.ts`
   - Store text field names in index metadata

---

## File Changes Required

### `src/types.ts`
```typescript
// Add to QueryOperators interface
export interface QueryOperators {
  // ... existing
  $type?: string | number | (string | number)[];
  $mod?: [number, number];
}

// Add to Filter type
export type Filter<T> = {
  // ... existing
  $expr?: unknown;  // Aggregation expression
};
```

### `src/query-matcher.ts`
- Add `$type` case in `matchesOperators` function
- Add `$mod` case in `matchesOperators` function
- Add `$expr` handling in `matchesFilter` function
- Import expression evaluation from aggregation module

### `src/aggregation.ts` (if needed)
- Export `evaluateExpression` function for use by `$expr`

---

## Test File Structure

```
test/query-operators.test.ts
├── Query Operators Tests (${getTestModeName()})
│   │
│   ├── $type Operator
│   │   ├── Basic Type Matching
│   │   │   ├── should match string type
│   │   │   ├── should match number type (alias)
│   │   │   ├── should match double type
│   │   │   ├── should match int type (integer detection)
│   │   │   ├── should match array type
│   │   │   ├── should match object type
│   │   │   ├── should match bool type
│   │   │   ├── should match null type
│   │   │   ├── should match date type
│   │   │   ├── should match objectId type
│   │   │   └── should match regex type
│   │   │
│   │   ├── Numeric Type Codes
│   │   │   ├── should match using numeric code 2 (string)
│   │   │   ├── should match using numeric code 1 (double)
│   │   │   ├── should match using numeric code 4 (array)
│   │   │   └── should match using numeric code 8 (bool)
│   │   │
│   │   ├── Multiple Types
│   │   │   ├── should match any type in array
│   │   │   ├── should match string or null
│   │   │   └── should match number or string
│   │   │
│   │   ├── Edge Cases
│   │   │   ├── should not match missing fields for any type
│   │   │   ├── should not match missing fields for null type
│   │   │   ├── should check array field type not element types
│   │   │   └── should work with nested fields
│   │   │
│   │   └── Error Cases
│   │       ├── should throw for unknown type alias
│   │       ├── should throw for invalid type alias case
│   │       └── should throw for invalid numeric type code
│   │
│   ├── $mod Operator
│   │   ├── Basic Modulo Matching
│   │   │   ├── should find even numbers
│   │   │   ├── should find odd numbers
│   │   │   ├── should find multiples of 3
│   │   │   ├── should find values with specific remainder
│   │   │   └── should handle negative numbers
│   │   │
│   │   ├── Edge Cases
│   │   │   ├── should not match non-numeric fields
│   │   │   ├── should not match null values
│   │   │   ├── should not match NaN document values
│   │   │   ├── should not match Infinity document values
│   │   │   ├── should handle floating point divisor (truncation)
│   │   │   └── should handle negative divisor
│   │   │
│   │   └── Error Cases
│   │       ├── should throw for array with one element
│   │       ├── should throw for array with more than two elements
│   │       ├── should throw for empty array
│   │       ├── should throw for non-array argument
│   │       ├── should throw for zero divisor
│   │       ├── should throw for NaN divisor
│   │       ├── should throw for Infinity divisor
│   │       └── should throw for non-numeric divisor
│   │
│   └── $expr Operator
│       ├── Field Comparisons
│       │   ├── should compare two fields with $gt
│       │   ├── should compare two fields with $lt
│       │   ├── should compare two fields with $eq
│       │   ├── should compare two fields with $gte
│       │   ├── should compare two fields with $lte
│       │   ├── should compare two fields with $ne
│       │   └── should compare nested fields
│       │
│       ├── Field to Constant Comparisons
│       │   ├── should compare field to constant number
│       │   ├── should compare field to constant string
│       │   └── should compare field to constant boolean
│       │
│       ├── Arithmetic Expressions
│       │   ├── should use $add in comparison
│       │   ├── should use $subtract in comparison
│       │   ├── should use $multiply in comparison
│       │   └── should use $divide in comparison
│       │
│       ├── Logical Operators
│       │   ├── should support $and in expression
│       │   ├── should support $or in expression
│       │   └── should support $not in expression
│       │
│       ├── Combined with Regular Query
│       │   ├── should work with field equality
│       │   ├── should work with other query operators
│       │   └── should work with logical operators at top level
│       │
│       └── Edge Cases
│           ├── should handle missing fields (evaluate to null)
│           ├── should work with array size comparison
│           └── should handle boolean result directly
```

---

## Error Messages Reference

### $type Errors

| Condition | Error Message |
|-----------|---------------|
| Unknown string alias | `Unknown type name alias: <alias>` |
| Invalid case (e.g., "String") | `Unknown type name alias: String` |
| Invalid numeric code | `Invalid numerical type code: <code>` |

### $mod Errors

| Condition | Error Message | Code |
|-----------|---------------|------|
| Array with < 2 elements | `malformed mod, not enough elements` | 16810 |
| Array with > 2 elements | `malformed mod, too many elements` | 16810 |
| Not an array | `malformed mod, needs to be an array` | 16810 |
| NaN as divisor | `malformed mod, divisor value is invalid :: caused by :: NaN is an invalid argument` | |
| Infinity as divisor | `malformed mod, divisor value is invalid :: caused by :: Infinity is an invalid argument` | |
| Zero as divisor | `divisor cannot be 0` | |
| Non-numeric divisor | `malformed mod, divisor not a number` | |
| Non-numeric remainder | `malformed mod, remainder not a number` | |

### $expr Errors

| Condition | Error Message |
|-----------|---------------|
| Unknown expression operator | `Unrecognized expression '<operator>'` |

---

## Documentation Updates

After implementation:
1. Add Phase 12 to PROGRESS.md
2. Update ROADMAP_REMAINING.md to mark Phase 12 complete
3. Add operator behaviors to COMPATIBILITY.md
4. Update README.md "What's Implemented"

---

## Summary

Phase 12 adds four query operators:
- **$text**: ✅ COMPLETE - Simplified text search with text indexes
- **$type**: ⬜ TODO - Type checking with BSON type aliases and numeric codes
- **$mod**: ⬜ TODO - Modular arithmetic for finding documents with specific remainders
- **$expr**: ⬜ TODO - Compare fields within same document using aggregation expressions

These operators complete the core query operator set and enable more expressive queries.

**Remaining Work**:
- Implementation: ~200-250 lines of code
- Tests: 35-40 new test cases
- Files modified: 3 (types.ts, query-matcher.ts, aggregation.ts)

**Sources**:
- [MongoDB $type documentation](https://www.mongodb.com/docs/manual/reference/operator/query/type/)
- [MongoDB $mod documentation](https://www.mongodb.com/docs/manual/reference/operator/query/mod/)
- [MongoDB $expr documentation](https://www.mongodb.com/docs/manual/reference/operator/query/expr/)
- [MongoDB BSON Types](https://www.mongodb.com/docs/manual/reference/bson-types/)

---

## Remaining Implementation Plan

### Step 1: Implement `$type` Operator

**Files to modify:**
1. `src/types.ts` - Add `$type` to QueryOperators interface
2. `src/query-matcher.ts` - Add `$type` case in `matchesOperators`

**Implementation details:**
```typescript
// In types.ts - add to QueryOperators:
$type?: string | number | (string | number)[];

// In query-matcher.ts - add BSON type mapping and matching logic
const BSON_TYPE_MAP: Record<string, number> = {
  double: 1, string: 2, object: 3, array: 4, binData: 5,
  undefined: 6, objectId: 7, bool: 8, date: 9, null: 10,
  regex: 11, javascript: 13, int: 16, timestamp: 17, long: 18,
  decimal: 19, minKey: -1, maxKey: 127
};

// Special "number" alias matches: double, int, long, decimal
const NUMBER_TYPES = [1, 16, 18, 19];
```

**Key behaviors:**
- Missing fields do NOT match any type (including "null")
- "number" alias matches int, long, double, decimal
- Array syntax `["string", "null"]` matches if field is ANY of those types
- Throws for unknown type alias or invalid numeric code

**Test cases to add:** ~15 tests

---

### Step 2: Implement `$mod` Operator

**Files to modify:**
1. `src/types.ts` - Add `$mod` to QueryOperators interface
2. `src/query-matcher.ts` - Add `$mod` case in `matchesOperators`

**Implementation details:**
```typescript
// In types.ts:
$mod?: [number, number];

// In query-matcher.ts:
case "$mod": {
  const modArr = opValue as unknown[];
  // Validate array length
  if (!Array.isArray(modArr)) throw new Error("malformed mod, needs to be an array");
  if (modArr.length < 2) throw new Error("malformed mod, not enough elements");
  if (modArr.length > 2) throw new Error("malformed mod, too many elements");

  const [divisor, remainder] = modArr;
  // Validate divisor/remainder are numbers
  // Validate no NaN, Infinity, or zero divisor
  // Return false for non-numeric docValue
  // Use Math.trunc for decimal truncation
  return Math.trunc(docValue) % Math.trunc(divisor) === Math.trunc(remainder);
}
```

**Key behaviors:**
- Non-numeric fields silently don't match (no error)
- NaN/Infinity in document value → no match
- Decimal values truncated toward zero
- Negative dividend produces negative remainder

**Test cases to add:** ~12 tests

---

### Step 3: Implement `$expr` Operator

**Files to modify:**
1. `src/types.ts` - Add `$expr` to Filter type
2. `src/query-matcher.ts` - Handle `$expr` in `matchesFilter`
3. `src/aggregation.ts` - Export `evaluateExpression` function

**Implementation details:**
```typescript
// In types.ts - add to Filter type:
$expr?: unknown;

// In aggregation.ts - export the function:
export function evaluateExpression(expr: unknown, doc: Document): unknown { ... }

// In query-matcher.ts - add to matchesFilter:
import { evaluateExpression } from "./aggregation.ts";

// Handle $expr at top level
if (key === "$expr") {
  const result = evaluateExpression(filterValue, doc);
  if (!result) return false;  // Falsy result = no match
  continue;
}
```

**Key behaviors:**
- Reuses existing `evaluateExpression` from aggregation.ts
- Supports: `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne` comparisons
- Supports: `$add`, `$subtract`, `$multiply`, `$divide` arithmetic
- Supports: `$and`, `$or` logical operators
- Field references: `"$fieldName"` resolves to field value
- Missing fields evaluate to `null`
- Result is truthy/falsy check

**Test cases to add:** ~15 tests

---

### Step 4: Extend Test File

**Current state:** `test/query-operators.test.ts` has only $text tests (18 tests)

**Tests to add:**
- `$type Operator` section (~15 tests)
- `$mod Operator` section (~12 tests)
- `$expr Operator` section (~15 tests)

**Test patterns to follow:** (from regex.test.ts)
- Group by functionality (Basic, Edge Cases, Error Cases)
- Use `assert.rejects` for error cases
- Create unique collection names per test
- Clean up after tests with `cleanup()` function
