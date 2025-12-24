# Phase 16: Extended Expression Operators - Implementation Plan

## Overview

This phase adds 30+ expression operators to the aggregation pipeline, organized into 5 sub-phases. Each sub-phase follows TDD methodology: write tests first, then implement, run tests to verify.

**Environment Constraint**: MongoDB is not available in this environment. Tests will run against MangoDB only. Behavior specifications are derived from official MongoDB documentation research.

**Estimated Tests**: 60-80 test cases
**Primary File**: `src/aggregation.ts` (add cases to `evaluateOperator` function)
**Test File**: `test/expression-operators.test.ts`

---

## Implementation Strategy

### TDD Cycle (Per Operator)
1. Write test cases for the operator
2. Run tests (expect failures)
3. Implement the operator in `evaluateOperator()`
4. Run tests (expect passes)
5. Move to next operator

### Error Message Patterns (From MongoDB Documentation)

| Category | Error Pattern |
|----------|---------------|
| Arithmetic | `"$<op> only supports numeric types, not <type>"` |
| String | `"$<op> requires a string argument, found: <type>"` |
| Array | `"$<op> requires an array, not <type>"` |
| Type conversion | `"Unsupported conversion from <type> to <target>"` |
| Date | `"can't convert from BSON type <type> to Date"` |

---

## Part 1: Arithmetic Operators (Est. 15 tests)

### Operators
1. `$abs` - Absolute value
2. `$ceil` - Round up to integer
3. `$floor` - Round down to integer
4. `$round` - Round to specified decimal places
5. `$mod` - Modulo (expression version)

### Behaviors

#### $abs
```typescript
{ $abs: <number> }
```
- Null/undefined/missing → `null`
- Non-number → Error: `"$abs only supports numeric types, not <type>"`
- NaN → NaN, Infinity → Infinity, -Infinity → Infinity
- Uses `Math.abs()`

#### $ceil
```typescript
{ $ceil: <number> }
```
- Null/undefined/missing → `null`
- Non-number → Error: `"$ceil only supports numeric types, not <type>"`
- Uses `Math.ceil()`

#### $floor
```typescript
{ $floor: <number> }
```
- Null/undefined/missing → `null`
- Non-number → Error: `"$floor only supports numeric types, not <type>"`
- Uses `Math.floor()`

#### $round
```typescript
{ $round: [ <number>, <place> ] }
// or
{ $round: <number> }  // defaults to place=0
```
- Null/undefined/missing → `null`
- Non-number → Error
- `<place>` must be integer between -20 and 100 (exclusive)
- Default place: 0
- Negative place: rounds left of decimal (e.g., -2 rounds to hundreds)

#### $mod
```typescript
{ $mod: [ <dividend>, <divisor> ] }
```
- Either null/undefined/missing → `null`
- Non-numbers → Error: `"$mod only supports numeric types, not <type1> and <type2>"`
- Divisor 0 → `null` (MongoDB returns null for mod by zero)
- **Negative handling**: Remainder follows dividend sign (JavaScript `%` behavior)
  - `-10 % 3 = -1` (NOT 2)

### Test Cases - Part 1

```typescript
describe("Arithmetic Operators", () => {
  describe("$abs", () => {
    it("should return absolute value of positive number");
    it("should return absolute value of negative number");
    it("should return 0 for 0");
    it("should return null for null input");
    it("should return null for missing field");
    it("should throw for non-numeric input");
  });

  describe("$ceil", () => {
    it("should round 2.3 up to 3");
    it("should return integer unchanged");
    it("should round -2.3 to -2");
    it("should return null for null input");
    it("should throw for non-numeric input");
  });

  describe("$floor", () => {
    it("should round 2.7 down to 2");
    it("should return integer unchanged");
    it("should round -2.3 to -3");
    it("should return null for null input");
    it("should throw for non-numeric input");
  });

  describe("$round", () => {
    it("should round to nearest integer by default");
    it("should round to specified decimal places");
    it("should round with negative place value");
    it("should return null for null input");
    it("should throw for non-numeric input");
  });

  describe("$mod", () => {
    it("should compute modulo of two positive numbers");
    it("should handle negative dividend");
    it("should handle negative divisor");
    it("should return null when divisor is 0");
    it("should return null for null input");
    it("should throw for non-numeric inputs");
  });
});
```

---

## Part 2: String Operators (Est. 18 tests)

### Operators
1. `$substr` / `$substrCP` - Substring extraction
2. `$strLenCP` - String length (code points)
3. `$split` - Split string into array
4. `$trim` / `$ltrim` / `$rtrim` - Whitespace trimming
5. `$toString` - Convert to string
6. `$indexOfCP` - Find substring position

### Behaviors

#### $substrCP
```typescript
{ $substrCP: [ <string>, <start>, <count> ] }
```
- All 3 args required
- `start` and `count` must be non-negative integers
- Out of bounds → returns remaining characters (no error)
- Null string → returns `""`
- Non-string → Error (code 40093)

#### $strLenCP
```typescript
{ $strLenCP: <string> }
```
- Returns length in UTF-8 code points
- Null/missing → **Error** (code 34471): `"$strLenCP requires a string argument, found: <type>"`
- Non-string → Error

#### $split
```typescript
{ $split: [ <string>, <delimiter> ] }
```
- Both must be strings
- Null → Error (code 40085/40086)
- Delimiter not found → `["original string"]`
- Returns array of substrings

#### $trim / $ltrim / $rtrim
```typescript
{ $trim: { input: <string>, chars: <string> } }
{ $ltrim: { input: <string>, chars: <string> } }
{ $rtrim: { input: <string>, chars: <string> } }
```
- `chars` is optional (defaults to whitespace)
- Non-string input → Error (code 50699)

#### $toString
```typescript
{ $toString: <expression> }
```
- Supports: double, string, bool, date, int, ObjectId
- Does NOT support: array, object
- Null/missing → `null`
- bool true → `"true"`, false → `"false"`
- date → ISO string

#### $indexOfCP
```typescript
{ $indexOfCP: [ <string>, <substring>, <start>, <end> ] }
```
- Returns 0-based index of first occurrence, or -1 if not found
- Null string → `null`
- Null substring → Error (code 40094)

### Test Cases - Part 2

```typescript
describe("String Operators", () => {
  describe("$substrCP", () => {
    it("should extract substring from start");
    it("should handle out of bounds gracefully");
    it("should return empty string for null input");
    it("should throw for non-string input");
  });

  describe("$strLenCP", () => {
    it("should return string length");
    it("should handle multi-byte characters");
    it("should throw for null input");
    it("should throw for non-string input");
  });

  describe("$split", () => {
    it("should split string by delimiter");
    it("should return single-element array when delimiter not found");
    it("should throw for null string");
    it("should throw for null delimiter");
  });

  describe("$trim", () => {
    it("should trim whitespace by default");
    it("should trim custom characters");
    it("should throw for non-string input");
  });

  describe("$toString", () => {
    it("should convert number to string");
    it("should convert boolean to string");
    it("should return null for null input");
    it("should throw for array input");
  });

  describe("$indexOfCP", () => {
    it("should return index of substring");
    it("should return -1 when not found");
    it("should return null for null string");
    it("should throw for null substring");
  });
});
```

---

## Part 3: Array Operators (Est. 20 tests)

### Operators
1. `$arrayElemAt` - Get element at index
2. `$slice` - Extract portion of array
3. `$concatArrays` - Concatenate arrays
4. `$filter` - Filter array elements
5. `$map` - Transform array elements
6. `$reduce` - Reduce to single value
7. `$in` - Check membership (expression version)

### Behaviors

#### $arrayElemAt
```typescript
{ $arrayElemAt: [ <array>, <idx> ] }
```
- Positive index: from start (0-based)
- Negative index: from end (-1 = last)
- Out of bounds → `null`
- Missing/null array → `null`

#### $slice (expression)
```typescript
{ $slice: [ <array>, <n> ] }           // 2-arg form
{ $slice: [ <array>, <position>, <n> ] } // 3-arg form
```
- 2-arg: positive n = first n, negative n = last n
- 3-arg: position can be negative, n must be positive
- Error if 3rd arg ≤ 0: `"Third argument to $slice must be positive"`

#### $concatArrays
```typescript
{ $concatArrays: [ <array1>, <array2>, ... ] }
```
- Any null → returns `null`
- Non-array → Error (code 28664): `"$concatArrays only supports arrays, not <type>"`

#### $filter
```typescript
{ $filter: { input: <array>, as: <string>, cond: <expression> } }
```
- `as` is optional, defaults to `"this"`
- Variable reference: `$$varName` (e.g., `$$this`, `$$item`)
- Missing input → `null`

#### $map
```typescript
{ $map: { input: <array>, as: <string>, in: <expression> } }
```
- All fields required (as, in)
- Variable reference: `$$varName`
- Non-array input → Error (code 16883): `"input to $map must be an array not <type>"`

#### $reduce
```typescript
{ $reduce: { input: <array>, initialValue: <expr>, in: <expr> } }
```
- All fields required
- Variables: `$$value` (accumulator), `$$this` (current element)
- Empty array → returns `initialValue`

#### $in (expression)
```typescript
{ $in: [ <element>, <array> ] }
```
- **Order matters**: element first, array second
- Returns `true`/`false`
- Non-array second arg → Error (code 40081)
- Does NOT support regex (unlike query $in)

### Test Cases - Part 3

```typescript
describe("Array Operators", () => {
  describe("$arrayElemAt", () => {
    it("should get element at positive index");
    it("should get element at negative index");
    it("should return null for out of bounds");
    it("should return null for null array");
  });

  describe("$slice", () => {
    it("should get first n elements with positive n");
    it("should get last n elements with negative n");
    it("should slice from position with 3-arg form");
    it("should throw when third arg is not positive");
  });

  describe("$concatArrays", () => {
    it("should concatenate multiple arrays");
    it("should return null if any array is null");
    it("should throw for non-array input");
  });

  describe("$filter", () => {
    it("should filter array elements by condition");
    it("should use default 'this' variable when as not specified");
    it("should return null for missing input");
  });

  describe("$map", () => {
    it("should transform each array element");
    it("should throw for non-array input");
  });

  describe("$reduce", () => {
    it("should reduce array to single value");
    it("should return initialValue for empty array");
  });

  describe("$in (expression)", () => {
    it("should return true when element in array");
    it("should return false when element not in array");
    it("should throw for non-array second argument");
  });
});
```

---

## Part 4: Type Conversion Operators (Est. 15 tests)

### Operators
1. `$toInt` - Convert to integer
2. `$toDouble` - Convert to double
3. `$toBool` - Convert to boolean
4. `$toDate` - Convert to date
5. `$toObjectId` - Convert to ObjectId
6. `$type` - Get BSON type name

### Behaviors

#### $toInt
- Supports: int, long, double (truncates), decimal, bool (0/1), string (base-10)
- Null/missing → `null`
- Unsupported type → Error (code 241)

#### $toDouble
- Supports: double, int, long, decimal, bool (0.0/1.0), string, date (epoch ms)
- Null/missing → `null`

#### $toBool
- **CRITICAL**: ALL strings are true (including `""`, `"false"`, `"0"`)
- Numeric 0 → false
- All non-zero numbers → true
- Null/missing → `null`

#### $toDate
- Supports: date, string (ISO 8601), numbers (epoch milliseconds), ObjectId
- Null/missing → `null`
- Unsupported type → Error (code 16006)

#### $toObjectId
- Only accepts: 24-character hex string, ObjectId
- Null/missing → `null`
- Invalid format → Error

#### $type
- Returns string type name: `"double"`, `"string"`, `"object"`, `"array"`, `"bool"`, `"date"`, `"null"`, `"int"`, `"objectId"`, `"missing"`
- Missing field → `"missing"`

### Test Cases - Part 4

```typescript
describe("Type Conversion Operators", () => {
  describe("$toInt", () => {
    it("should convert double to int (truncate)");
    it("should convert string to int");
    it("should convert bool to int");
    it("should return null for null");
    it("should throw for invalid string");
  });

  describe("$toDouble", () => {
    it("should convert int to double");
    it("should convert string to double");
    it("should return null for null");
  });

  describe("$toBool", () => {
    it("should convert 0 to false");
    it("should convert non-zero to true");
    it("should convert empty string to true"); // Critical!
    it("should return null for null");
  });

  describe("$toDate", () => {
    it("should convert epoch milliseconds to date");
    it("should convert ISO string to date");
    it("should return null for null");
  });

  describe("$type", () => {
    it("should return 'double' for numbers");
    it("should return 'string' for strings");
    it("should return 'array' for arrays");
    it("should return 'null' for null");
    it("should return 'missing' for missing field");
  });
});
```

---

## Part 5: Date Operators (Est. 12 tests)

### Operators
1. `$year` - Extract year
2. `$month` - Extract month (1-12)
3. `$dayOfMonth` - Extract day (1-31)
4. `$hour` - Extract hour (0-23)
5. `$minute` - Extract minute (0-59)
6. `$second` - Extract second (0-60)
7. `$dayOfWeek` - Extract day of week (1=Sunday, 7=Saturday)
8. `$dateToString` - Format date as string

### Behaviors

#### Date Extraction Operators ($year, $month, etc.)
```typescript
{ $year: <dateExpression> }
{ $month: <dateExpression> }
// etc.
```
- Null/missing → `null`
- Non-date → Error (code 16006): `"can't convert from BSON type <type> to Date"`
- `$month`: 1-12 (January=1)
- `$dayOfWeek`: 1-7 (Sunday=1)
- `$second`: 0-60 (60 for leap seconds)

#### $dateToString
```typescript
{ $dateToString: { format: "<format>", date: <dateExpr>, timezone: "<tz>", onNull: <expr> } }
```
- `format` optional (defaults to ISO: `%Y-%m-%dT%H:%M:%S.%LZ`)
- Format specifiers: `%Y`, `%m`, `%d`, `%H`, `%M`, `%S`, `%L`, `%j`, `%w`, `%u`, `%%`
- `onNull` optional (what to return if date is null)
- Without `onNull`, null date → `null`

### Test Cases - Part 5

```typescript
describe("Date Operators", () => {
  describe("$year", () => {
    it("should extract year from date");
    it("should return null for null date");
    it("should throw for non-date input");
  });

  describe("$month", () => {
    it("should extract month (1-12) from date");
    it("should return null for null date");
  });

  describe("$dayOfMonth", () => {
    it("should extract day of month from date");
  });

  describe("$hour / $minute / $second", () => {
    it("should extract time components");
  });

  describe("$dayOfWeek", () => {
    it("should return 1 for Sunday");
    it("should return 7 for Saturday");
  });

  describe("$dateToString", () => {
    it("should format date with default ISO format");
    it("should format date with custom format");
    it("should return onNull value for null date");
    it("should return null when date is null and no onNull");
  });
});
```

---

## Implementation Order

Execute in this order (each part builds on previous):

### Part 1: Arithmetic (Day 1)
1. Create `test/expression-operators.test.ts` with Part 1 tests
2. Run tests → all fail
3. Implement `$abs` → run tests
4. Implement `$ceil` → run tests
5. Implement `$floor` → run tests
6. Implement `$round` → run tests
7. Implement `$mod` → run tests

### Part 2: String (Day 2)
1. Add Part 2 tests to test file
2. Implement `$substrCP` → run tests
3. Implement `$strLenCP` → run tests
4. Implement `$split` → run tests
5. Implement `$trim` / `$ltrim` / `$rtrim` → run tests
6. Implement `$toString` → run tests
7. Implement `$indexOfCP` → run tests

### Part 3: Array (Day 3)
1. Add Part 3 tests to test file
2. Add variable scoping support for `$$varName` syntax
3. Implement `$arrayElemAt` → run tests
4. Implement `$slice` → run tests
5. Implement `$concatArrays` → run tests
6. Implement `$filter` → run tests
7. Implement `$map` → run tests
8. Implement `$reduce` → run tests
9. Implement `$in` → run tests

### Part 4: Type Conversion (Day 4)
1. Add Part 4 tests to test file
2. Implement `$toInt` → run tests
3. Implement `$toDouble` → run tests
4. Implement `$toBool` → run tests
5. Implement `$toDate` → run tests
6. Implement `$toObjectId` → run tests
7. Implement `$type` → run tests

### Part 5: Date (Day 5)
1. Add Part 5 tests to test file
2. Implement `$year`, `$month`, `$dayOfMonth` → run tests
3. Implement `$hour`, `$minute`, `$second` → run tests
4. Implement `$dayOfWeek` → run tests
5. Implement `$dateToString` → run tests

---

## File Changes Required

1. **`src/aggregation.ts`**
   - Add cases to `evaluateOperator()` switch statement
   - Add helper functions for complex operators
   - Add variable scope handling for `$filter`, `$map`, `$reduce`

2. **`test/expression-operators.test.ts`** (NEW)
   - All test cases organized by operator category

3. **`PROGRESS.md`** - Update after completion
4. **`ROADMAP.md`** - Update current phase
5. **`COMPATIBILITY.md`** - Document any behavior notes

---

## Variable Scoping Implementation Notes

For `$filter`, `$map`, and `$reduce`, need to support `$$varName` syntax:

```typescript
// In evaluateExpression, modify string handling:
if (typeof expr === "string" && expr.startsWith("$$")) {
  // Look up in variable context
  const varName = expr.slice(2);
  return variableContext[varName];
}

// evaluateOperator needs to pass variable context
function evaluateOperator(op: string, args: unknown, doc: Document, vars?: Record<string, unknown>): unknown
```

---

## Success Criteria

1. All ~80 tests pass
2. Error messages match MongoDB format
3. Null propagation follows MongoDB behavior
4. Variable scoping works for $filter, $map, $reduce
5. No regressions in existing 827 tests

---

## Commands Reference

```bash
# Run only the new expression operator tests
npm test -- --test-name-pattern="Expression Operators"

# Run all tests
npm test

# Run with verbose output
npm test -- --test-reporter=spec
```
