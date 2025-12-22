# Phase 11: Regular Expressions - Implementation Plan

## Overview

Add regex matching support to MangoDB queries. This enables pattern matching on string fields, which is a common MongoDB feature for text search and validation.

**Priority**: MEDIUM — Common use case for text search
**Estimated Tests**: 40-50

---

## Operations

### Step 1: `$regex` Query Operator (Basic)

**Syntax**:
```typescript
// String pattern
{ field: { $regex: "pattern" } }

// With options
{ field: { $regex: "pattern", $options: "i" } }

// JavaScript RegExp (implicit)
{ field: /pattern/i }
```

**Behavior**:
- Match documents where string field matches the regex pattern
- Non-string fields silently don't match (no error)
- `null` and `undefined` values don't match
- Pattern is a string that will be compiled to RegExp

**Options** (`$options`):
- `i` — Case-insensitive matching
- `m` — Multiline mode (^ and $ match line boundaries)
- `s` — Dotall mode (. matches newlines)

**Test Cases**:
```typescript
// Basic pattern matching
await collection.find({ name: { $regex: "^A" } }).toArray();        // Starts with A
await collection.find({ email: { $regex: "@gmail\\.com$" } }).toArray(); // Ends with @gmail.com
await collection.find({ desc: { $regex: "urgent" } }).toArray();    // Contains "urgent"

// With options
await collection.find({ name: { $regex: "alice", $options: "i" } }).toArray(); // Case insensitive
await collection.find({ text: { $regex: "^hello", $options: "m" } }).toArray(); // Multiline
await collection.find({ text: { $regex: "a.b", $options: "s" } }).toArray();    // Dotall

// Multiple options
await collection.find({ name: { $regex: "^alice", $options: "im" } }).toArray();
```

**Edge Cases**:
```typescript
// Non-string fields - silently skip (no match, no error)
await collection.find({ age: { $regex: "^25" } }).toArray(); // Never matches numbers
await collection.find({ data: { $regex: "test" } }).toArray(); // Never matches objects/arrays

// null/undefined - don't match
await collection.find({ name: { $regex: ".*" } }).toArray(); // Won't match null names

// Empty pattern
await collection.find({ name: { $regex: "" } }).toArray(); // Matches any string

// Special regex characters
await collection.find({ path: { $regex: "foo\\.bar" } }).toArray(); // Escape dot
await collection.find({ expr: { $regex: "\\$price" } }).toArray();  // Escape dollar
```

**Error Cases**:
```typescript
// Invalid regex pattern
await collection.find({ name: { $regex: "[invalid" } }).toArray(); // Should throw

// $options without $regex
await collection.find({ name: { $options: "i" } }).toArray(); // Error: $options needs a $regex

// Invalid options
await collection.find({ name: { $regex: "test", $options: "xyz" } }).toArray(); // Error: invalid flag
```

---

### Step 2: JavaScript RegExp Support

**Syntax**:
```typescript
// RegExp literal
{ field: /pattern/i }

// RegExp object
{ field: new RegExp("pattern", "i") }
```

**Behavior**:
- Accept JavaScript RegExp objects directly as filter values
- Flags on RegExp are used directly (no separate $options)
- Works anywhere a filter value is expected

**Test Cases**:
```typescript
// RegExp literal
await collection.find({ name: /^Alice/ }).toArray();
await collection.find({ email: /gmail\.com$/i }).toArray();

// RegExp object
await collection.find({ name: new RegExp("^A", "i") }).toArray();

// In findOne
await collection.findOne({ name: /Alice/ });

// Combined with other operators (implicit AND)
await collection.find({ name: /^A/, age: { $gt: 20 } }).toArray();
```

---

### Step 3: Regex in Array Fields

**Behavior**:
- When field is an array, regex matches if ANY element matches
- Only string elements in the array are tested
- Non-string elements are silently skipped

**Test Cases**:
```typescript
// Array element matching
// Given: { tags: ["production", "staging", "dev"] }
await collection.find({ tags: { $regex: "^prod" } }).toArray(); // Matches

// No match if no element matches
// Given: { tags: ["alpha", "beta"] }
await collection.find({ tags: { $regex: "^prod" } }).toArray(); // No match

// Mixed array (strings and non-strings)
// Given: { values: ["test", 123, "other"] }
await collection.find({ values: { $regex: "test" } }).toArray(); // Matches "test"

// With RegExp literal
await collection.find({ tags: /urgent/i }).toArray();
```

---

### Step 4: Regex with `$elemMatch`

**Behavior**:
- `$elemMatch` can contain `$regex` conditions
- Useful for matching array elements with multiple conditions

**Test Cases**:
```typescript
// $elemMatch with regex
await collection.find({
  items: { $elemMatch: { name: { $regex: "^widget" }, price: { $lt: 100 } } }
}).toArray();

// Multiple regex in $elemMatch
await collection.find({
  logs: { $elemMatch: {
    message: { $regex: "error", $options: "i" },
    level: { $regex: "^(ERROR|FATAL)$" }
  }}
}).toArray();
```

---

### Step 5: Regex in `$in` Operator

**Behavior**:
- `$in` array can contain RegExp objects (not `{ $regex: ... }` syntax)
- Mix of exact values and regex patterns allowed
- Document matches if field matches ANY value or regex

**Important**: Only JavaScript RegExp objects (`/pattern/` or `new RegExp()`) are allowed in `$in`, NOT the `{ $regex: ... }` syntax.

**Test Cases**:
```typescript
// Regex in $in
await collection.find({ status: { $in: [/^active/, /^pending/] } }).toArray();

// Mix of exact values and regex
await collection.find({ status: { $in: ["complete", /^in_progress/, /^review/] } }).toArray();

// Case insensitive regex in $in
await collection.find({ category: { $in: [/electronics/i, "Books", /games/i] } }).toArray();

// Array field with $in containing regex
await collection.find({ tags: { $in: [/^prod/, /^staging/] } }).toArray();
```

**Error Cases**:
```typescript
// $regex syntax in $in is NOT supported (only RegExp objects)
// This should be tested to document behavior - MongoDB may error or ignore
await collection.find({ status: { $in: [{ $regex: "active" }] } }).toArray();
```

---

### Step 6: Regex in `$not` Operator

**Behavior**:
- `$not` can wrap `$regex` to match documents that DON'T match the pattern
- Missing fields match `$not: { $regex: ... }` (field can't match pattern if it doesn't exist)

**Test Cases**:
```typescript
// $not with $regex
await collection.find({ name: { $not: { $regex: "^Admin" } } }).toArray();

// $not with $regex and options
await collection.find({ email: { $not: { $regex: "@test\\.com$", $options: "i" } } }).toArray();

// Missing field matches $not: $regex (it doesn't match the pattern)
// Given: { other: "value" } (no name field)
await collection.find({ name: { $not: { $regex: "test" } } }).toArray(); // Matches

// $not with RegExp literal
await collection.find({ name: { $not: /^test/ } }).toArray();
```

---

### Step 7: Regex in Aggregation `$match`

**Behavior**:
- `$match` stage supports all the same regex syntax as `find()`
- Reuses existing `matchesFilter` function

**Test Cases**:
```typescript
// $regex in $match
await collection.aggregate([
  { $match: { name: { $regex: "^A" } } }
]).toArray();

// RegExp in $match
await collection.aggregate([
  { $match: { email: /gmail\.com$/i } }
]).toArray();

// Combined with other conditions
await collection.aggregate([
  { $match: { name: { $regex: "^A" }, status: "active" } },
  { $sort: { name: 1 } }
]).toArray();
```

---

## Implementation Notes

### Files to Modify

1. **`src/types.ts`** — Add `$regex` and `$options` to `QueryOperators` interface
2. **`src/query-matcher.ts`** — Add regex matching logic
3. **`src/document-utils.ts`** — May need to update `valuesEqual` for RegExp comparison

### Type Changes (`src/types.ts`)

```typescript
export interface QueryOperators {
  // ... existing operators
  $regex?: string | RegExp;
  $options?: string;
}
```

### Implementation in `query-matcher.ts`

```typescript
// Add to matchesOperators switch statement:

case "$regex": {
  const pattern = opValue;
  const options = (operators as QueryOperators).$options || '';

  // Handle RegExp object directly
  if (pattern instanceof RegExp) {
    if (typeof docValue !== 'string') return false;
    return pattern.test(docValue);
  }

  // Handle string pattern
  if (typeof pattern !== 'string') {
    throw new Error('$regex has to be a string');
  }

  try {
    const regex = new RegExp(pattern, options);
    if (typeof docValue !== 'string') return false;
    return regex.test(docValue);
  } catch (e) {
    throw new Error(`Invalid regular expression: ${pattern}`);
  }
}

case "$options": {
  // $options is handled in $regex case
  // If we get here, $regex wasn't present
  if (!('$regex' in operators)) {
    throw new Error('$options needs a $regex');
  }
  break;
}
```

### RegExp in Filter Values

Update `matchesSingleValue` to handle RegExp:

```typescript
function matchesSingleValue(docValue: unknown, filterValue: unknown): boolean {
  // Handle RegExp filter value
  if (filterValue instanceof RegExp) {
    if (typeof docValue !== 'string') return false;
    return filterValue.test(docValue);
  }

  // ... existing logic
}
```

### RegExp in `$in` Operator

Update `matchesIn` to handle RegExp:

```typescript
function matchesIn(docValue: unknown, inValues: unknown[]): boolean {
  const testValue = (dv: unknown, iv: unknown): boolean => {
    if (iv instanceof RegExp) {
      return typeof dv === 'string' && iv.test(dv);
    }
    return valuesEqual(dv, iv);
  };

  if (Array.isArray(docValue)) {
    return docValue.some((dv) => inValues.some((iv) => testValue(dv, iv)));
  }
  return inValues.some((iv) => testValue(docValue, iv));
}
```

### Validation

```typescript
// Validate $options contains only valid flags
function validateRegexOptions(options: string): void {
  const validFlags = ['i', 'm', 's', 'u', 'x'];
  for (const char of options) {
    if (!validFlags.includes(char)) {
      throw new Error(`invalid flag in regex options: ${char}`);
    }
  }
}
```

**Note**: MongoDB supports `x` (extended) but JavaScript doesn't. We should validate against JavaScript-supported flags: `i`, `m`, `s`, `u`, `g` (though `g` doesn't affect `test()`).

---

## Error Messages (Match MongoDB)

| Condition | Error Message |
|-----------|---------------|
| Invalid regex pattern | `Invalid regular expression: [pattern]` (varies) |
| `$options` without `$regex` | `$options needs a $regex` |
| Invalid option flag | `invalid flag in regex options: [flag]` |
| Non-string `$regex` value | `$regex has to be a string` |

---

## Test File Structure

```
test/regex.test.ts
├── $regex Query Operator
│   ├── Basic Pattern Matching
│   │   ├── should match start of string with ^
│   │   ├── should match end of string with $
│   │   ├── should match pattern anywhere in string
│   │   ├── should match with special regex characters escaped
│   │   ├── should not match non-string fields
│   │   ├── should not match null values
│   │   ├── should not match undefined/missing fields
│   │   └── should match empty pattern against any string
│   │
│   ├── Options
│   │   ├── should support case-insensitive (i) option
│   │   ├── should support multiline (m) option
│   │   ├── should support dotAll (s) option
│   │   ├── should combine multiple options
│   │   └── should throw for invalid options
│   │
│   └── Error Cases
│       ├── should throw for invalid regex pattern
│       ├── should throw for $options without $regex
│       └── should throw for non-string $regex value
│
├── JavaScript RegExp
│   ├── should match with RegExp literal
│   ├── should match with RegExp object
│   ├── should respect RegExp flags
│   ├── should work in findOne
│   └── should work combined with other conditions
│
├── Array Fields
│   ├── should match if any array element matches
│   ├── should not match if no element matches
│   ├── should skip non-string elements in array
│   └── should work with RegExp literal
│
├── With $elemMatch
│   ├── should support $regex in $elemMatch
│   ├── should combine $regex with other conditions
│   └── should support RegExp in $elemMatch
│
├── With $in Operator
│   ├── should match RegExp in $in array
│   ├── should mix exact values and RegExp
│   ├── should match if any pattern matches
│   └── should work with array fields
│
├── With $not Operator
│   ├── should negate regex match
│   ├── should match missing fields
│   ├── should work with $options
│   └── should work with RegExp literal
│
├── With $nin Operator
│   ├── should not match if any RegExp in $nin matches
│   └── should match if no RegExp matches
│
└── In Aggregation $match
    ├── should support $regex in $match stage
    ├── should support RegExp in $match stage
    └── should combine with other pipeline stages
```

---

## Implementation Order

1. **Add types** (`src/types.ts`) — Add `$regex` and `$options` to `QueryOperators`
2. **Basic `$regex`** (`src/query-matcher.ts`) — Implement in `matchesOperators`
3. **RegExp in filter values** — Update `matchesSingleValue` for RegExp objects
4. **RegExp in `$in`** — Update `matchesIn` function
5. **Write tests** — Create `test/regex.test.ts` with full test coverage
6. **Verify against MongoDB** — Run tests with `MONGODB_URI` set
7. **Documentation** — Update PROGRESS.md, COMPATIBILITY.md

---

## Documentation Updates

After implementation:
1. Add Phase 11 section to PROGRESS.md
2. Update ROADMAP_REMAINING.md (mark Phase 11 complete)
3. Add regex behaviors to COMPATIBILITY.md
4. Update README.md "What's Implemented" section

---

## Edge Cases and Gotchas

| Behavior | Details |
|----------|---------|
| Array field matching | Matches if ANY string element matches |
| Non-string fields | Silently skipped (no match, no error) |
| `$regex` in `$in` | Only RegExp objects allowed, not `{ $regex: ... }` |
| Missing fields | Don't match `$regex`, but DO match `$not: { $regex: ... }` |
| `null` values | Don't match any regex pattern |
| Empty string pattern | Matches any string (equivalent to `/.*/`) |
| Options `x` | MongoDB supports extended syntax, but JS doesn't — test behavior |
