# Phase 10: Aggregation Pipeline (Advanced) - Implementation Plan

## Overview

Extend the aggregation framework with grouping, lookups, and expression operators. This enables analytics-style queries and relational joins between collections.

**Priority**: HIGH — Required for analytics and relational-style queries
**Estimated Test Cases**: 60-80

---

## Implementation Strategy Summary

The project follows a rigorous TDD methodology:

1. **Test-Driven Development**: Write tests against real MongoDB first, verify behavior, then implement in MangoDB
2. **Dual-Target Testing**: All tests run against both MongoDB and MangoDB via the test harness
3. **Incremental Progress**: Implement one step at a time, verify tests pass before moving on
4. **Behavior Documentation**: Record any surprising MongoDB behaviors in COMPATIBILITY.md
5. **Error Message Matching**: Match MongoDB's exact error messages where possible

---

## Implementation Steps

### Step 1: Expression Evaluation Framework

**File**: `src/aggregation.ts`

Before implementing `$group` and `$addFields`, we need an expression evaluator that can:
- Resolve field references (`$fieldName`)
- Evaluate arithmetic operators (`$add`, `$subtract`, `$multiply`, `$divide`)
- Evaluate string operators (`$concat`, `$toUpper`, `$toLower`)
- Evaluate conditional operators (`$cond`, `$ifNull`)
- Handle nested expressions

```typescript
/**
 * Evaluate an aggregation expression against a document.
 *
 * @param expr - The expression (field reference, literal, or operator)
 * @param doc - The document context
 * @returns The evaluated value
 */
function evaluateExpression(expr: unknown, doc: Document): unknown {
  // String starting with $ is a field reference
  if (typeof expr === 'string' && expr.startsWith('$')) {
    const fieldPath = expr.slice(1);
    return getValueByPath(doc, fieldPath);
  }

  // Primitive values returned as-is
  if (expr === null || typeof expr !== 'object') {
    return expr;
  }

  // Object with operator key
  if (!Array.isArray(expr)) {
    const keys = Object.keys(expr);
    if (keys.length === 1 && keys[0].startsWith('$')) {
      return evaluateOperator(keys[0], expr[keys[0]], doc);
    }
    // Object literal - evaluate each field
    const result: Document = {};
    for (const [key, value] of Object.entries(expr)) {
      result[key] = evaluateExpression(value, doc);
    }
    return result;
  }

  // Array - evaluate each element
  return expr.map(item => evaluateExpression(item, doc));
}

function evaluateOperator(op: string, args: unknown, doc: Document): unknown {
  switch (op) {
    case '$literal':
      return args; // Return as-is without evaluation
    case '$add':
      return evalAdd(args, doc);
    case '$subtract':
      return evalSubtract(args, doc);
    case '$multiply':
      return evalMultiply(args, doc);
    case '$divide':
      return evalDivide(args, doc);
    case '$concat':
      return evalConcat(args, doc);
    case '$toUpper':
      return evalToUpper(args, doc);
    case '$toLower':
      return evalToLower(args, doc);
    case '$cond':
      return evalCond(args, doc);
    case '$ifNull':
      return evalIfNull(args, doc);
    case '$size':
      return evalSize(args, doc);
    default:
      throw new Error(`Unrecognized expression operator: '${op}'`);
  }
}
```

---

### Step 2: Implement Expression Operators

#### 2.1: Arithmetic Operators

```typescript
// $add - Add numbers or dates
function evalAdd(args: unknown, doc: Document): number | Date | null {
  const values = (args as unknown[]).map(a => evaluateExpression(a, doc));

  // Check for null - null propagates
  if (values.some(v => v === null || v === undefined)) {
    return null;
  }

  // Date + number = Date (add milliseconds)
  const dateIndex = values.findIndex(v => v instanceof Date);
  if (dateIndex !== -1) {
    const date = values[dateIndex] as Date;
    const sum = values.reduce((acc, v, i) => {
      if (i === dateIndex) return acc;
      if (typeof v !== 'number') throw new Error('$add only supports numeric or date types');
      return acc + v;
    }, 0);
    return new Date(date.getTime() + sum);
  }

  // All numbers
  return values.reduce((acc: number, v) => {
    if (typeof v !== 'number') throw new Error('$add only supports numeric or date types');
    return acc + v;
  }, 0);
}

// $subtract - Subtract numbers or dates
function evalSubtract(args: unknown, doc: Document): number | null {
  const [arg1, arg2] = (args as unknown[]).map(a => evaluateExpression(a, doc));

  if (arg1 === null || arg1 === undefined || arg2 === null || arg2 === undefined) {
    return null;
  }

  // Date - Date = milliseconds difference
  if (arg1 instanceof Date && arg2 instanceof Date) {
    return arg1.getTime() - arg2.getTime();
  }

  // Date - number = Date
  if (arg1 instanceof Date && typeof arg2 === 'number') {
    return new Date(arg1.getTime() - arg2);
  }

  if (typeof arg1 !== 'number' || typeof arg2 !== 'number') {
    throw new Error('$subtract only supports numeric or date types');
  }

  return arg1 - arg2;
}

// $multiply - Multiply numbers
function evalMultiply(args: unknown, doc: Document): number | null {
  const values = (args as unknown[]).map(a => evaluateExpression(a, doc));

  if (values.some(v => v === null || v === undefined)) {
    return null;
  }

  return values.reduce((acc: number, v) => {
    if (typeof v !== 'number') throw new Error('$multiply only supports numeric types');
    return acc * v;
  }, 1);
}

// $divide - Divide numbers
function evalDivide(args: unknown, doc: Document): number | null {
  const [dividend, divisor] = (args as unknown[]).map(a => evaluateExpression(a, doc));

  if (dividend === null || dividend === undefined || divisor === null || divisor === undefined) {
    return null;
  }

  if (typeof dividend !== 'number' || typeof divisor !== 'number') {
    throw new Error('$divide only supports numeric types');
  }

  if (divisor === 0) {
    throw new Error("can't $divide by zero");
  }

  return dividend / divisor;
}
```

#### 2.2: String Operators

```typescript
// $concat - Concatenate strings
function evalConcat(args: unknown, doc: Document): string | null {
  const values = (args as unknown[]).map(a => evaluateExpression(a, doc));

  // null propagates
  if (values.some(v => v === null || v === undefined)) {
    return null;
  }

  // All values must be strings
  for (const v of values) {
    if (typeof v !== 'string') {
      throw new Error(`$concat only supports strings, not ${typeof v}`);
    }
  }

  return values.join('');
}

// $toUpper - Convert to uppercase
function evalToUpper(args: unknown, doc: Document): string | null {
  const value = evaluateExpression(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== 'string') {
    throw new Error('$toUpper requires a string argument');
  }

  return value.toUpperCase();
}

// $toLower - Convert to lowercase
function evalToLower(args: unknown, doc: Document): string | null {
  const value = evaluateExpression(args, doc);

  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== 'string') {
    throw new Error('$toLower requires a string argument');
  }

  return value.toLowerCase();
}
```

#### 2.3: Conditional Operators

```typescript
// $cond - Conditional expression (if-then-else)
// Supports both array syntax and object syntax
function evalCond(args: unknown, doc: Document): unknown {
  let condition: unknown, thenValue: unknown, elseValue: unknown;

  if (Array.isArray(args)) {
    // Array syntax: [$cond: [condition, thenValue, elseValue]]
    [condition, thenValue, elseValue] = args;
  } else if (typeof args === 'object' && args !== null) {
    // Object syntax: { $cond: { if: condition, then: thenValue, else: elseValue } }
    const obj = args as { if: unknown; then: unknown; else: unknown };
    condition = obj.if;
    thenValue = obj.then;
    elseValue = obj.else;
  } else {
    throw new Error('$cond requires an array or object argument');
  }

  const evalCondition = evaluateExpression(condition, doc);

  // Truthy check (MongoDB-style: null, undefined, 0, false are falsy)
  if (evalCondition) {
    return evaluateExpression(thenValue, doc);
  } else {
    return evaluateExpression(elseValue, doc);
  }
}

// $ifNull - Null coalescing
function evalIfNull(args: unknown, doc: Document): unknown {
  const values = args as unknown[];

  for (const arg of values) {
    const value = evaluateExpression(arg, doc);
    if (value !== null && value !== undefined) {
      return value;
    }
  }

  return null;
}
```

#### 2.4: Array Size Operator

```typescript
// $size - Get array size (expression version)
function evalSize(args: unknown, doc: Document): number {
  const value = evaluateExpression(args, doc);

  if (!Array.isArray(value)) {
    throw new Error('$size requires an array argument');
  }

  return value.length;
}
```

---

### Step 3: `$group` Stage

**The most complex stage** - groups documents by `_id` and applies accumulators.

```typescript
interface GroupStage {
  $group: {
    _id: unknown;  // Grouping expression (field ref, literal, or null)
    [field: string]: unknown;  // Accumulator expressions
  };
}
```

#### Implementation

```typescript
private execGroup(groupSpec: GroupSpec, docs: Document[]): Document[] {
  const groups = new Map<string, { _id: unknown; accumulators: Map<string, Accumulator> }>();

  for (const doc of docs) {
    // Evaluate grouping _id
    const groupId = evaluateExpression(groupSpec._id, doc);
    const groupKey = JSON.stringify(groupId);  // Serialize for Map key

    if (!groups.has(groupKey)) {
      groups.set(groupKey, {
        _id: groupId,
        accumulators: this.initializeAccumulators(groupSpec)
      });
    }

    const group = groups.get(groupKey)!;

    // Apply each accumulator
    for (const [field, accumulator] of group.accumulators) {
      const expr = groupSpec[field];
      accumulator.accumulate(doc, expr);
    }
  }

  // Build result documents
  return Array.from(groups.values()).map(group => {
    const result: Document = { _id: group._id };
    for (const [field, accumulator] of group.accumulators) {
      result[field] = accumulator.getResult();
    }
    return result;
  });
}
```

#### Accumulators

```typescript
interface Accumulator {
  accumulate(doc: Document, expr: unknown): void;
  getResult(): unknown;
}

class SumAccumulator implements Accumulator {
  private sum = 0;

  accumulate(doc: Document, expr: { $sum: unknown }): void {
    const value = evaluateExpression(expr.$sum, doc);
    if (typeof value === 'number') {
      this.sum += value;
    }
    // Non-numbers ignored (like MongoDB)
  }

  getResult(): number {
    return this.sum;
  }
}

class AvgAccumulator implements Accumulator {
  private sum = 0;
  private count = 0;

  accumulate(doc: Document, expr: { $avg: unknown }): void {
    const value = evaluateExpression(expr.$avg, doc);
    if (typeof value === 'number') {
      this.sum += value;
      this.count++;
    }
  }

  getResult(): number | null {
    return this.count > 0 ? this.sum / this.count : null;
  }
}

class MinAccumulator implements Accumulator {
  private min: unknown = undefined;

  accumulate(doc: Document, expr: { $min: unknown }): void {
    const value = evaluateExpression(expr.$min, doc);
    if (value !== null && value !== undefined) {
      if (this.min === undefined || compareValues(value, this.min) < 0) {
        this.min = value;
      }
    }
  }

  getResult(): unknown {
    return this.min === undefined ? null : this.min;
  }
}

class MaxAccumulator implements Accumulator {
  private max: unknown = undefined;

  accumulate(doc: Document, expr: { $max: unknown }): void {
    const value = evaluateExpression(expr.$max, doc);
    if (value !== null && value !== undefined) {
      if (this.max === undefined || compareValues(value, this.max) > 0) {
        this.max = value;
      }
    }
  }

  getResult(): unknown {
    return this.max === undefined ? null : this.max;
  }
}

class FirstAccumulator implements Accumulator {
  private first: unknown = undefined;
  private hasValue = false;

  accumulate(doc: Document, expr: { $first: unknown }): void {
    if (!this.hasValue) {
      this.first = evaluateExpression(expr.$first, doc);
      this.hasValue = true;
    }
  }

  getResult(): unknown {
    return this.first;
  }
}

class LastAccumulator implements Accumulator {
  private last: unknown = undefined;

  accumulate(doc: Document, expr: { $last: unknown }): void {
    this.last = evaluateExpression(expr.$last, doc);
  }

  getResult(): unknown {
    return this.last;
  }
}

class PushAccumulator implements Accumulator {
  private values: unknown[] = [];

  accumulate(doc: Document, expr: { $push: unknown }): void {
    const value = evaluateExpression(expr.$push, doc);
    this.values.push(value);
  }

  getResult(): unknown[] {
    return this.values;
  }
}

class AddToSetAccumulator implements Accumulator {
  private values: unknown[] = [];

  accumulate(doc: Document, expr: { $addToSet: unknown }): void {
    const value = evaluateExpression(expr.$addToSet, doc);
    // Check if already exists (using deep equality)
    if (!this.values.some(v => deepEquals(v, value))) {
      this.values.push(value);
    }
  }

  getResult(): unknown[] {
    return this.values;
  }
}

class CountAccumulator implements Accumulator {
  private count = 0;

  accumulate(): void {
    this.count++;
  }

  getResult(): number {
    return this.count;
  }
}
```

#### Test Cases for $group

```typescript
// Group by single field
await collection.aggregate([
  { $group: { _id: "$category", count: { $sum: 1 } } }
]).toArray();
// Returns: [{ _id: "A", count: 5 }, { _id: "B", count: 3 }]

// Group with multiple accumulators
await collection.aggregate([
  { $group: {
    _id: "$department",
    totalSalary: { $sum: "$salary" },
    avgSalary: { $avg: "$salary" },
    minSalary: { $min: "$salary" },
    maxSalary: { $max: "$salary" },
    employees: { $push: "$name" }
  }}
]).toArray();

// Group all documents (null _id)
await collection.aggregate([
  { $group: { _id: null, total: { $sum: 1 }, allNames: { $push: "$name" } } }
]).toArray();
// Returns: [{ _id: null, total: 10, allNames: ["Alice", "Bob", ...] }]

// Group by compound key
await collection.aggregate([
  { $group: { _id: { year: "$year", month: "$month" }, count: { $sum: 1 } } }
]).toArray();

// $addToSet accumulator
await collection.aggregate([
  { $group: { _id: "$category", uniqueTags: { $addToSet: "$tag" } } }
]).toArray();
```

---

### Step 4: `$lookup` Stage

Left outer join with another collection.

```typescript
interface LookupStage {
  $lookup: {
    from: string;           // Foreign collection name
    localField: string;     // Field from input documents
    foreignField: string;   // Field from foreign collection
    as: string;            // Output array field name
  };
}
```

#### Implementation

```typescript
private async execLookup(lookupSpec: LookupSpec, docs: Document[]): Promise<Document[]> {
  // Get foreign collection from database
  const foreignCollection = this.db.collection(lookupSpec.from);
  const foreignDocs = await foreignCollection.find({}).toArray();

  return docs.map(doc => {
    const localValue = getValueByPath(doc, lookupSpec.localField);

    // Find matching foreign documents
    const matches = foreignDocs.filter(foreignDoc => {
      const foreignValue = getValueByPath(foreignDoc, lookupSpec.foreignField);
      return deepEquals(localValue, foreignValue);
    });

    // Add matches as array field
    return {
      ...doc,
      [lookupSpec.as]: matches
    };
  });
}
```

**Critical Behaviors**:
- Returns **empty array** for no matches (left outer join)
- Returns array even for single match
- `as` field overwrites existing field if present

#### Test Cases for $lookup

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
// Lookup with multiple matches (array with multiple items)
// Lookup with null values
```

---

### Step 5: `$addFields` Stage

Add new fields while preserving existing ones.

```typescript
interface AddFieldsStage {
  $addFields: Record<string, unknown>;  // Field expressions
}
```

#### Implementation

```typescript
private execAddFields(addFieldsSpec: Record<string, unknown>, docs: Document[]): Document[] {
  return docs.map(doc => {
    const result = { ...doc };

    for (const [field, expr] of Object.entries(addFieldsSpec)) {
      const value = evaluateExpression(expr, doc);
      setValueByPath(result, field, value);
    }

    return result;
  });
}
```

#### Test Cases for $addFields

```typescript
// Add computed field
await collection.aggregate([
  { $addFields: { fullName: { $concat: ["$firstName", " ", "$lastName"] } } }
]).toArray();

// Add multiple fields
await collection.aggregate([
  { $addFields: {
    total: { $add: ["$price", "$tax"] },
    discounted: { $multiply: ["$price", 0.9] }
  }}
]).toArray();

// Overwrite existing field
await collection.aggregate([
  { $addFields: { name: { $toUpper: "$name" } } }
]).toArray();

// Add nested field
await collection.aggregate([
  { $addFields: { "metadata.processed": true } }
]).toArray();
```

---

### Step 6: `$set` Stage

Alias for `$addFields` - identical behavior.

```typescript
interface SetStage {
  $set: Record<string, unknown>;
}

// In executeStage:
case '$set':
  return this.execAddFields(stage.$set, docs);  // Reuse $addFields logic
```

---

### Step 7: `$replaceRoot` Stage

Replace document with embedded document.

```typescript
interface ReplaceRootStage {
  $replaceRoot: {
    newRoot: unknown;  // Expression resolving to document
  };
}
```

#### Implementation

```typescript
private execReplaceRoot(spec: { newRoot: unknown }, docs: Document[]): Document[] {
  return docs.map(doc => {
    const newRoot = evaluateExpression(spec.newRoot, doc);

    if (newRoot === null || newRoot === undefined) {
      throw new Error("$replaceRoot newRoot expression must evaluate to an object");
    }

    if (typeof newRoot !== 'object' || Array.isArray(newRoot)) {
      throw new Error("$replaceRoot newRoot expression must evaluate to an object");
    }

    return newRoot as Document;
  });
}
```

**Critical Behavior**: Errors if `newRoot` evaluates to null, undefined, or non-object.

#### Test Cases for $replaceRoot

```typescript
// Replace with embedded document
await collection.aggregate([
  { $replaceRoot: { newRoot: "$address" } }
]).toArray();
// { name: "Alice", address: { city: "NYC", zip: "10001" } }
// Becomes: { city: "NYC", zip: "10001" }

// With $mergeObjects for partial replacement
await collection.aggregate([
  { $replaceRoot: { newRoot: { $mergeObjects: ["$defaults", "$$ROOT"] } } }
]).toArray();

// Error case: missing embedded document
// Throws error when address is null/undefined
```

---

### Step 8: `$out` Stage

Write results to a collection.

```typescript
interface OutStage {
  $out: string;  // Target collection name
}
```

#### Implementation

```typescript
private async execOut(collectionName: string, docs: Document[]): Promise<Document[]> {
  // $out must be last stage - validate in pipeline execution

  const targetCollection = this.db.collection(collectionName);

  // Drop existing collection and replace with results
  await targetCollection.deleteMany({});

  if (docs.length > 0) {
    await targetCollection.insertMany(docs);
  }

  // $out returns empty array (results written to collection)
  return [];
}
```

**Critical Behaviors**:
- Must be **last stage** in pipeline (throw error otherwise)
- **Replaces** entire collection
- Returns empty result (documents are in target collection)

#### Validation in Pipeline

```typescript
async toArray(): Promise<Document[]> {
  // Validate $out is last stage if present
  for (let i = 0; i < this.pipeline.length; i++) {
    if ('$out' in this.pipeline[i] && i !== this.pipeline.length - 1) {
      throw new Error("$out can only be the final stage in the pipeline");
    }
  }

  // Execute pipeline...
}
```

---

### Step 9: Update Types

**File**: `src/types.ts`

```typescript
// Add new stage types
export interface GroupStage {
  $group: {
    _id: unknown;
    [field: string]: unknown;
  };
}

export interface LookupStage {
  $lookup: {
    from: string;
    localField: string;
    foreignField: string;
    as: string;
  };
}

export interface AddFieldsStage {
  $addFields: Record<string, unknown>;
}

export interface SetStage {
  $set: Record<string, unknown>;
}

export interface ReplaceRootStage {
  $replaceRoot: {
    newRoot: unknown;
  };
}

export interface OutStage {
  $out: string;
}

// Update PipelineStage union
export type PipelineStage =
  | MatchStage
  | ProjectStage
  | SortStage
  | LimitStage
  | SkipStage
  | CountStage
  | UnwindStage
  | GroupStage      // New
  | LookupStage     // New
  | AddFieldsStage  // New
  | SetStage        // New
  | ReplaceRootStage // New
  | OutStage;       // New
```

---

## Implementation Order

1. **Expression evaluator** - Foundation for all computed fields
2. **Expression operators** - `$add`, `$concat`, `$cond`, etc.
3. **$addFields / $set** - Uses expression evaluator, simpler than $group
4. **$group with $sum only** - Core grouping logic
5. **Other accumulators** - `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`
6. **$lookup** - Requires collection access pattern
7. **$replaceRoot** - Simple once expressions work
8. **$out** - Write operation with validation
9. **Tests** - Comprehensive test suite

---

## File Changes Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `src/types.ts` | Modify | Add new stage types and expression types |
| `src/aggregation.ts` | Modify | Add expression evaluator, new stages |
| `src/collection.ts` | Modify | Pass db reference to AggregationCursor for $lookup |
| `test/aggregation-advanced.test.ts` | **New** | Tests for Phase 10 stages |

---

## Test File Structure

```
test/aggregation-advanced.test.ts
├── Expression Operators
│   ├── $add
│   │   ├── should add numbers
│   │   ├── should add date and number (milliseconds)
│   │   ├── should return null if any operand is null
│   │   └── should throw for non-numeric types
│   │
│   ├── $subtract
│   │   ├── should subtract numbers
│   │   ├── should subtract dates (return milliseconds)
│   │   └── should return null if any operand is null
│   │
│   ├── $multiply
│   │   ├── should multiply numbers
│   │   ├── should return null if any operand is null
│   │   └── should throw for non-numeric types
│   │
│   ├── $divide
│   │   ├── should divide numbers
│   │   ├── should throw for divide by zero
│   │   └── should return null if any operand is null
│   │
│   ├── $concat
│   │   ├── should concatenate strings
│   │   ├── should return null if any operand is null
│   │   └── should throw for non-string types
│   │
│   ├── $toUpper / $toLower
│   │   ├── should convert string case
│   │   └── should return null for null input
│   │
│   ├── $cond
│   │   ├── should return then value for truthy condition
│   │   ├── should return else value for falsy condition
│   │   ├── should support array syntax
│   │   └── should support object syntax (if/then/else)
│   │
│   ├── $ifNull
│   │   ├── should return first non-null value
│   │   ├── should return null if all values are null
│   │   └── should handle undefined as null
│   │
│   └── $size (expression)
│       ├── should return array length
│       └── should throw for non-array
│
├── $group Stage
│   ├── Basic Grouping
│   │   ├── should group by single field
│   │   ├── should group by compound _id
│   │   ├── should group all with null _id
│   │   └── should return empty for empty input
│   │
│   ├── $sum Accumulator
│   │   ├── should sum numeric values
│   │   ├── should count with $sum: 1
│   │   └── should ignore non-numeric values
│   │
│   ├── $avg Accumulator
│   │   ├── should calculate average
│   │   ├── should return null for no values
│   │   └── should ignore non-numeric values
│   │
│   ├── $min / $max Accumulators
│   │   ├── should find minimum value
│   │   ├── should find maximum value
│   │   ├── should handle dates
│   │   └── should return null for empty group
│   │
│   ├── $first / $last Accumulators
│   │   ├── should return first value in group
│   │   ├── should return last value in group
│   │   └── should handle null values
│   │
│   ├── $push Accumulator
│   │   ├── should collect all values into array
│   │   └── should include null values
│   │
│   ├── $addToSet Accumulator
│   │   ├── should collect unique values
│   │   ├── should handle object equality
│   │   └── should not include duplicates
│   │
│   └── Error Cases
│       └── should throw for missing _id
│
├── $lookup Stage
│   ├── should join collections on matching field
│   ├── should return empty array for no matches
│   ├── should return multiple matches in array
│   ├── should handle null local field value
│   ├── should handle missing foreign collection (empty)
│   └── should overwrite existing field with same name as 'as'
│
├── $addFields / $set Stage
│   ├── should add new field with literal value
│   ├── should add field with field reference
│   ├── should add field with expression
│   ├── should preserve existing fields
│   ├── should overwrite existing field
│   ├── should add nested field with dot notation
│   └── $set should behave identically to $addFields
│
├── $replaceRoot Stage
│   ├── should replace document with embedded document
│   ├── should throw for missing newRoot field
│   ├── should throw for null newRoot value
│   └── should throw for non-object newRoot value
│
├── $out Stage
│   ├── should write results to collection
│   ├── should replace existing collection
│   ├── should return empty array
│   ├── should throw if not last stage
│   └── should handle empty result set
│
└── Combined Pipeline Tests
    ├── $match -> $group -> $sort
    ├── $unwind -> $group (count per array element)
    ├── $lookup -> $unwind (flatten joins)
    ├── $addFields -> $group (computed grouping)
    └── Complex multi-stage analytics pipeline
```

---

## Error Messages Reference

| Stage/Operator | Condition | Error Message |
|----------------|-----------|---------------|
| `$group` | Missing _id | `a group specification must include an _id` |
| `$lookup` | Missing from | `$lookup requires 'from' field` |
| `$lookup` | Missing localField | `$lookup requires 'localField' field` |
| `$lookup` | Missing foreignField | `$lookup requires 'foreignField' field` |
| `$lookup` | Missing as | `$lookup requires 'as' field` |
| `$out` | Not last stage | `$out can only be the final stage in the pipeline` |
| `$replaceRoot` | Missing newRoot | `$replaceRoot requires 'newRoot' field` |
| `$replaceRoot` | null/undefined newRoot | `$replaceRoot newRoot expression must evaluate to an object` |
| `$concat` | Non-string arg | `$concat only supports strings, not <type>` |
| `$divide` | Divide by zero | `can't $divide by zero` |
| Expression | Unknown operator | `Unrecognized expression operator: '$unknown'` |

---

## Critical Behaviors to Verify

| Behavior | Details |
|----------|---------|
| `$group` with `_id: null` | Groups ALL documents into single group |
| `$lookup` no match | Returns empty array in output field (left outer join) |
| `$concat` with null | Returns **null** (null propagates) |
| `$concat` with non-string | **Throws error** |
| `$avg` with no values | Returns **null**, not 0 |
| `$replaceRoot` missing field | **Errors and fails** |
| `$out` position | Must be **last stage** |
| `$addFields` | Does NOT remove existing fields |
| `$set` | Identical to `$addFields` (alias) |
| Accumulator non-numeric | $sum/$avg ignore non-numeric values |

---

## Documentation Updates After Implementation

1. Update PROGRESS.md with Phase 10 completion
2. Update ROADMAP.md current phase marker
3. Add aggregation behaviors to COMPATIBILITY.md
4. Update README.md "What's Implemented" section
5. Update test count in all documentation

---

## Test Execution

```bash
# Run only advanced aggregation tests (MangoDB)
npm test -- --test-name-pattern "Aggregation.*Advanced"

# Run against real MongoDB
MONGODB_URI="mongodb://localhost:27017" npm test -- --test-name-pattern "Aggregation"
```

---

## Completion Criteria

Phase 10 is complete when:

1. All expression operators implemented and tested
2. `$group` with all accumulators working
3. `$lookup` basic join working
4. `$addFields` / `$set` working
5. `$replaceRoot` working with error handling
6. `$out` working with position validation
7. All tests pass against both MangoDB and MongoDB
8. Documentation updated
9. Error messages match MongoDB format
