# Phase 13: Additional Update Operators - Implementation Plan

## Overview

**Goal**: Complete the update operator functionality with remaining commonly-used operators.

**Priority**: Medium
**Estimated Tests**: 30-40
**Status**: Planning

This phase follows Test-Driven Development (TDD):
1. Write tests against real MongoDB first
2. Verify tests pass against MongoDB
3. Implement in MangoDB to pass the same tests
4. Document any discovered behaviors in COMPATIBILITY.md

---

## Current State

### Existing Update Operators (Phases 3 & 6)

| Operator | Type | Location | Description |
|----------|------|----------|-------------|
| `$set` | Field | `update-operators.ts:130-134` | Sets field values |
| `$unset` | Field | `update-operators.ts:136-140` | Removes fields |
| `$inc` | Field | `update-operators.ts:143-150` | Increments numeric values |
| `$push` | Array | `update-operators.ts:152-157` | Appends to arrays |
| `$addToSet` | Array | `update-operators.ts:159-164` | Adds unique values |
| `$pop` | Array | `update-operators.ts:166-191` | Removes first/last element |
| `$pull` | Array | `update-operators.ts:193-223` | Removes matching elements |

### Helper Functions Available

From `document-utils.ts`:
- `cloneDocument()` - Deep copy documents
- `setValueByPath()` - Set values using dot notation
- `getValueAtPath()` - Get values using dot notation
- `deleteValueByPath()` - Delete fields using dot notation
- `valuesEqual()` - Deep equality comparison

From `query-matcher.ts`:
- `isOperatorObject()` - Detect `$-prefixed` operators
- `matchesOperators()` - Evaluate query conditions

---

## Operations to Implement

### Step 1: `$min` Update Operator

**Syntax**: `{ $min: { <field>: <value> } }`

**Behavior**:
- Only updates if the specified value is **less than** the current field value
- If the field does not exist, creates the field with the specified value
- Uses BSON type comparison order for mixed types
- Works with numbers, dates, and strings

**Edge Cases**:
- Missing field → creates field with specified value
- `null` field value → uses BSON comparison (null < numbers)
- Different types → uses MongoDB type ordering

**MongoDB Error Messages**:
- Starting in MongoDB 5.0, empty operand `{ $min: {} }` is a no-op (no error)

**Test Cases**:
```typescript
describe("$min operator", () => {
  it("should update when new value is less than current", async () => {
    await collection.insertOne({ name: "Alice", lowScore: 100 });
    await collection.updateOne({ name: "Alice" }, { $min: { lowScore: 50 } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.lowScore, 50);
  });

  it("should not update when new value is greater than current", async () => {
    await collection.insertOne({ name: "Alice", lowScore: 50 });
    const result = await collection.updateOne(
      { name: "Alice" },
      { $min: { lowScore: 100 } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.lowScore, 50);
    assert.strictEqual(result.modifiedCount, 0);
  });

  it("should create field if missing", async () => {
    await collection.insertOne({ name: "Alice" });
    await collection.updateOne({ name: "Alice" }, { $min: { lowScore: 75 } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.lowScore, 75);
  });

  it("should work with nested fields using dot notation", async () => {
    await collection.insertOne({ name: "Alice", stats: { lowScore: 100 } });
    await collection.updateOne(
      { name: "Alice" },
      { $min: { "stats.lowScore": 50 } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.stats?.lowScore, 50);
  });

  it("should work with dates", async () => {
    const earlier = new Date("2023-01-01");
    const later = new Date("2023-06-01");
    await collection.insertOne({ name: "Alice", firstVisit: later });
    await collection.updateOne({ name: "Alice" }, { $min: { firstVisit: earlier } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.firstVisit.getTime(), earlier.getTime());
  });

  it("should not update date when new date is later", async () => {
    const earlier = new Date("2023-01-01");
    const later = new Date("2023-06-01");
    await collection.insertOne({ name: "Alice", firstVisit: earlier });
    const result = await collection.updateOne(
      { name: "Alice" },
      { $min: { firstVisit: later } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.firstVisit.getTime(), earlier.getTime());
    assert.strictEqual(result.modifiedCount, 0);
  });

  it("should work with updateMany", async () => {
    await collection.insertMany([
      { type: "score", value: 100 },
      { type: "score", value: 80 },
    ]);
    await collection.updateMany({ type: "score" }, { $min: { value: 90 } });
    const docs = await collection.find({ type: "score" }).toArray();
    // First doc: 100 -> 90 (updated), Second doc: 80 stays 80
    assert.ok(docs.some(d => d.value === 90));
    assert.ok(docs.some(d => d.value === 80));
  });
});
```

---

### Step 2: `$max` Update Operator

**Syntax**: `{ $max: { <field>: <value> } }`

**Behavior**:
- Only updates if the specified value is **greater than** the current field value
- If the field does not exist, creates the field with the specified value
- Uses BSON type comparison order for mixed types
- Works with numbers, dates, and strings

**Edge Cases**:
- Missing field → creates field with specified value
- `null` field value → uses BSON comparison (null < numbers)

**MongoDB Error Messages**:
- Starting in MongoDB 5.0, empty operand `{ $max: {} }` is a no-op (no error)

**Test Cases**:
```typescript
describe("$max operator", () => {
  it("should update when new value is greater than current", async () => {
    await collection.insertOne({ name: "Alice", highScore: 50 });
    await collection.updateOne({ name: "Alice" }, { $max: { highScore: 100 } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.highScore, 100);
  });

  it("should not update when new value is less than current", async () => {
    await collection.insertOne({ name: "Alice", highScore: 100 });
    const result = await collection.updateOne(
      { name: "Alice" },
      { $max: { highScore: 50 } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.highScore, 100);
    assert.strictEqual(result.modifiedCount, 0);
  });

  it("should create field if missing", async () => {
    await collection.insertOne({ name: "Alice" });
    await collection.updateOne({ name: "Alice" }, { $max: { highScore: 75 } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.highScore, 75);
  });

  it("should work with nested fields", async () => {
    await collection.insertOne({ name: "Alice", stats: { highScore: 50 } });
    await collection.updateOne(
      { name: "Alice" },
      { $max: { "stats.highScore": 100 } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.stats?.highScore, 100);
  });

  it("should work with dates", async () => {
    const earlier = new Date("2023-01-01");
    const later = new Date("2023-06-01");
    await collection.insertOne({ name: "Alice", lastVisit: earlier });
    await collection.updateOne({ name: "Alice" }, { $max: { lastVisit: later } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.lastVisit.getTime(), later.getTime());
  });

  it("should work with updateMany", async () => {
    await collection.insertMany([
      { type: "score", value: 100 },
      { type: "score", value: 80 },
    ]);
    await collection.updateMany({ type: "score" }, { $max: { value: 90 } });
    const docs = await collection.find({ type: "score" }).toArray();
    // First doc: 100 stays 100, Second doc: 80 -> 90 (updated)
    assert.ok(docs.some(d => d.value === 100));
    assert.ok(docs.some(d => d.value === 90));
  });
});
```

---

### Step 3: `$mul` Update Operator

**Syntax**: `{ $mul: { <field>: <number> } }`

**Behavior**:
- Multiplies the value of a field by the specified number
- If the field does not exist, creates the field with value **0** (not the multiplied value!)
- Only works with numeric fields
- Supports integer and floating-point numbers

**Edge Cases**:
- Missing field → creates field with value **0** (important!)
- Non-numeric existing value → throws error
- Non-numeric multiplier → throws error

**MongoDB Error Messages**:
```
Cannot apply $mul to a value of non-numeric type. {_id: ObjectId('...')} has the field '<field>' of non-numeric type <type>
```

Note: MongoDB historically had a bug where error said "Cannot increment" instead of "Cannot apply $mul" (SERVER-12992). Modern versions should say "$mul".

**Test Cases**:
```typescript
describe("$mul operator", () => {
  it("should multiply existing numeric value", async () => {
    await collection.insertOne({ name: "Alice", price: 100 });
    await collection.updateOne({ name: "Alice" }, { $mul: { price: 1.1 } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.ok(Math.abs((doc?.price as number) - 110) < 0.0001);
  });

  it("should create field with 0 if missing", async () => {
    await collection.insertOne({ name: "Alice" });
    await collection.updateOne({ name: "Alice" }, { $mul: { quantity: 5 } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.quantity, 0); // NOT 5!
  });

  it("should handle integer multiplication", async () => {
    await collection.insertOne({ value: 10 });
    await collection.updateOne({}, { $mul: { value: 3 } });
    const doc = await collection.findOne({});
    assert.strictEqual(doc?.value, 30);
  });

  it("should handle floating-point multiplication", async () => {
    await collection.insertOne({ value: 10.5 });
    await collection.updateOne({}, { $mul: { value: 2 } });
    const doc = await collection.findOne({});
    assert.ok(Math.abs((doc?.value as number) - 21) < 0.0001);
  });

  it("should handle multiplication by zero", async () => {
    await collection.insertOne({ value: 100 });
    await collection.updateOne({}, { $mul: { value: 0 } });
    const doc = await collection.findOne({});
    assert.strictEqual(doc?.value, 0);
  });

  it("should handle negative multiplier", async () => {
    await collection.insertOne({ value: 10 });
    await collection.updateOne({}, { $mul: { value: -2 } });
    const doc = await collection.findOne({});
    assert.strictEqual(doc?.value, -20);
  });

  it("should work with nested fields", async () => {
    await collection.insertOne({ stats: { multiplier: 5 } });
    await collection.updateOne({}, { $mul: { "stats.multiplier": 3 } });
    const doc = await collection.findOne({});
    assert.strictEqual(doc?.stats?.multiplier, 15);
  });

  it("should throw error for non-numeric field value", async () => {
    await collection.insertOne({ name: "Alice", value: "not a number" });
    await assert.rejects(
      async () => await collection.updateOne({ name: "Alice" }, { $mul: { value: 2 } }),
      (err: Error) => {
        return (
          err.message.includes("non-numeric") ||
          err.message.includes("Cannot apply $mul") ||
          err.message.includes("Cannot increment") // Old MongoDB error
        );
      }
    );
  });

  it("should work with updateMany", async () => {
    await collection.insertMany([
      { type: "price", value: 100 },
      { type: "price", value: 200 },
    ]);
    await collection.updateMany({ type: "price" }, { $mul: { value: 0.9 } }); // 10% discount
    const docs = await collection.find({ type: "price" }).toArray();
    const values = docs.map(d => d.value).sort((a, b) => (a as number) - (b as number));
    assert.ok(Math.abs((values[0] as number) - 90) < 0.0001);
    assert.ok(Math.abs((values[1] as number) - 180) < 0.0001);
  });
});
```

---

### Step 4: `$rename` Update Operator

**Syntax**: `{ $rename: { <oldField>: <newField> } }`

**Behavior**:
- Renames a field from old name to new name
- Internally performs: `$unset` old field + `$set` new field with value
- If old field doesn't exist, does nothing (no error)
- If new field already exists, it gets overwritten
- Works with dot notation for nested fields
- **Cannot rename fields within arrays** (limitation)

**Edge Cases**:
- Missing old field → no-op (no error)
- New field already exists → overwrites new field
- Same source and dest → error in some versions

**MongoDB Error Messages**:
```
The source and destination field for $rename must differ
```
(When renaming a field to itself)

**Test Cases**:
```typescript
describe("$rename operator", () => {
  it("should rename a field", async () => {
    await collection.insertOne({ name: "Alice", oldField: "value" });
    await collection.updateOne({ name: "Alice" }, { $rename: { oldField: "newField" } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.newField, "value");
    assert.strictEqual(doc?.oldField, undefined);
  });

  it("should do nothing if old field does not exist", async () => {
    await collection.insertOne({ name: "Alice" });
    const result = await collection.updateOne(
      { name: "Alice" },
      { $rename: { nonexistent: "newField" } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.newField, undefined);
    // Note: matchedCount is 1, but modifiedCount may be 0 or 1 depending on MongoDB version
    assert.strictEqual(result.matchedCount, 1);
  });

  it("should overwrite existing target field", async () => {
    await collection.insertOne({ name: "Alice", oldField: "old", newField: "existing" });
    await collection.updateOne({ name: "Alice" }, { $rename: { oldField: "newField" } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.newField, "old");
    assert.strictEqual(doc?.oldField, undefined);
  });

  it("should work with nested source field using dot notation", async () => {
    await collection.insertOne({ name: "Alice", data: { oldName: "value" } });
    await collection.updateOne(
      { name: "Alice" },
      { $rename: { "data.oldName": "data.newName" } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.data?.newName, "value");
    assert.strictEqual(doc?.data?.oldName, undefined);
  });

  it("should move field to different nesting level", async () => {
    await collection.insertOne({ name: "Alice", nested: { field: "value" } });
    await collection.updateOne(
      { name: "Alice" },
      { $rename: { "nested.field": "topLevel" } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.strictEqual(doc?.topLevel, "value");
    assert.strictEqual(doc?.nested?.field, undefined);
  });

  it("should rename multiple fields at once", async () => {
    await collection.insertOne({ a: 1, b: 2, c: 3 });
    await collection.updateOne({}, { $rename: { a: "x", b: "y" } });
    const doc = await collection.findOne({});
    assert.strictEqual(doc?.x, 1);
    assert.strictEqual(doc?.y, 2);
    assert.strictEqual(doc?.c, 3);
    assert.strictEqual(doc?.a, undefined);
    assert.strictEqual(doc?.b, undefined);
  });

  it("should work with updateMany", async () => {
    await collection.insertMany([
      { type: "item", oldName: "A" },
      { type: "item", oldName: "B" },
    ]);
    await collection.updateMany({ type: "item" }, { $rename: { oldName: "newName" } });
    const docs = await collection.find({ type: "item" }).toArray();
    assert.ok(docs.every(d => d.newName !== undefined));
    assert.ok(docs.every(d => d.oldName === undefined));
  });
});
```

---

### Step 5: `$currentDate` Update Operator

**Syntax**:
```javascript
{ $currentDate: { <field>: true } }
{ $currentDate: { <field>: { $type: "date" } } }
{ $currentDate: { <field>: { $type: "timestamp" } } }
```

**Behavior**:
- Sets the field to the current date
- `true` or `{ $type: "date" }` → JavaScript Date object
- `{ $type: "timestamp" }` → MongoDB Timestamp (for MangoDB, we'll use numeric timestamp)
- Case-sensitive: only "date" and "timestamp" (lowercase)
- Creates field if it doesn't exist

**Edge Cases**:
- Invalid $type value → error
- Case sensitivity on type values

**MongoDB Error Messages**:
```
$currentDate: unrecognized type specification
```

**Test Cases**:
```typescript
describe("$currentDate operator", () => {
  it("should set field to current date with true", async () => {
    await collection.insertOne({ name: "Alice" });
    const before = new Date();
    await collection.updateOne({ name: "Alice" }, { $currentDate: { lastModified: true } });
    const after = new Date();
    const doc = await collection.findOne({ name: "Alice" });
    assert.ok(doc?.lastModified instanceof Date);
    assert.ok(doc?.lastModified >= before);
    assert.ok(doc?.lastModified <= after);
  });

  it("should set field to current date with $type: date", async () => {
    await collection.insertOne({ name: "Alice" });
    const before = new Date();
    await collection.updateOne(
      { name: "Alice" },
      { $currentDate: { lastModified: { $type: "date" } } }
    );
    const after = new Date();
    const doc = await collection.findOne({ name: "Alice" });
    assert.ok(doc?.lastModified instanceof Date);
    assert.ok(doc?.lastModified >= before);
    assert.ok(doc?.lastModified <= after);
  });

  it("should set field to timestamp with $type: timestamp", async () => {
    await collection.insertOne({ name: "Alice" });
    const before = Date.now();
    await collection.updateOne(
      { name: "Alice" },
      { $currentDate: { lastModified: { $type: "timestamp" } } }
    );
    const after = Date.now();
    const doc = await collection.findOne({ name: "Alice" });
    // In MongoDB this returns a Timestamp object; in MangoDB we'll use a numeric timestamp
    // The test should be flexible about the type
    const ts = doc?.lastModified;
    assert.ok(typeof ts === "number" || ts instanceof Date || (ts && typeof ts === "object"));
  });

  it("should create field if missing", async () => {
    await collection.insertOne({ name: "Alice" });
    await collection.updateOne({ name: "Alice" }, { $currentDate: { createdAt: true } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.ok(doc?.createdAt !== undefined);
  });

  it("should overwrite existing field", async () => {
    const oldDate = new Date("2020-01-01");
    await collection.insertOne({ name: "Alice", lastModified: oldDate });
    await collection.updateOne({ name: "Alice" }, { $currentDate: { lastModified: true } });
    const doc = await collection.findOne({ name: "Alice" });
    assert.ok((doc?.lastModified as Date).getTime() > oldDate.getTime());
  });

  it("should work with nested fields", async () => {
    await collection.insertOne({ name: "Alice", meta: {} });
    await collection.updateOne(
      { name: "Alice" },
      { $currentDate: { "meta.updatedAt": true } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.ok(doc?.meta?.updatedAt instanceof Date);
  });

  it("should set multiple fields at once", async () => {
    await collection.insertOne({ name: "Alice" });
    await collection.updateOne(
      { name: "Alice" },
      { $currentDate: { createdAt: true, updatedAt: true } }
    );
    const doc = await collection.findOne({ name: "Alice" });
    assert.ok(doc?.createdAt instanceof Date);
    assert.ok(doc?.updatedAt instanceof Date);
  });

  it("should work with updateMany", async () => {
    await collection.insertMany([
      { type: "item", name: "A" },
      { type: "item", name: "B" },
    ]);
    await collection.updateMany({ type: "item" }, { $currentDate: { updatedAt: true } });
    const docs = await collection.find({ type: "item" }).toArray();
    assert.ok(docs.every(d => d.updatedAt instanceof Date));
  });
});
```

---

### Step 6: `$setOnInsert` Update Operator

**Syntax**: `{ $setOnInsert: { <field>: <value> } }`

**Behavior**:
- Only applies when an upsert operation results in an **insert**
- Completely ignored when the operation updates an existing document
- Typically used with `$set` to set creation-only fields
- Creates fields with specified values only during insert

**Edge Cases**:
- Without `upsert: true` → operator is completely ignored
- With matching document → operator is ignored (only `$set` etc. apply)
- With no match and `upsert: true` → fields are set

**Test Cases**:
```typescript
describe("$setOnInsert operator", () => {
  it("should set fields on upsert insert", async () => {
    const now = new Date();
    await collection.updateOne(
      { email: "new@test.com" },
      {
        $set: { name: "New User" },
        $setOnInsert: { createdAt: now, role: "user" }
      },
      { upsert: true }
    );
    const doc = await collection.findOne({ email: "new@test.com" });
    assert.strictEqual(doc?.name, "New User");
    assert.strictEqual(doc?.role, "user");
    assert.strictEqual((doc?.createdAt as Date).getTime(), now.getTime());
  });

  it("should not set fields when updating existing document", async () => {
    const oldDate = new Date("2020-01-01");
    await collection.insertOne({ email: "existing@test.com", createdAt: oldDate, name: "Old Name" });

    const newDate = new Date();
    await collection.updateOne(
      { email: "existing@test.com" },
      {
        $set: { name: "Updated Name" },
        $setOnInsert: { createdAt: newDate, role: "admin" }
      },
      { upsert: true }
    );

    const doc = await collection.findOne({ email: "existing@test.com" });
    assert.strictEqual(doc?.name, "Updated Name");
    assert.strictEqual((doc?.createdAt as Date).getTime(), oldDate.getTime()); // NOT updated
    assert.strictEqual(doc?.role, undefined); // NOT set
  });

  it("should be ignored without upsert option", async () => {
    await collection.updateOne(
      { email: "nonexistent@test.com" },
      { $setOnInsert: { name: "Should Not Exist" } }
      // No upsert: true
    );
    const doc = await collection.findOne({ email: "nonexistent@test.com" });
    assert.strictEqual(doc, null);
  });

  it("should work with nested fields", async () => {
    await collection.updateOne(
      { id: 1 },
      { $setOnInsert: { "meta.createdAt": new Date(), "meta.version": 1 } },
      { upsert: true }
    );
    const doc = await collection.findOne({ id: 1 });
    assert.ok(doc?.meta?.createdAt instanceof Date);
    assert.strictEqual(doc?.meta?.version, 1);
  });

  it("should work alone without $set", async () => {
    await collection.updateOne(
      { key: "unique" },
      { $setOnInsert: { value: "initial" } },
      { upsert: true }
    );
    const doc = await collection.findOne({ key: "unique" });
    assert.strictEqual(doc?.key, "unique");
    assert.strictEqual(doc?.value, "initial");
  });

  it("should combine with other operators on insert", async () => {
    await collection.updateOne(
      { name: "counter" },
      {
        $inc: { count: 1 },
        $setOnInsert: { startedAt: new Date() }
      },
      { upsert: true }
    );
    const doc = await collection.findOne({ name: "counter" });
    assert.strictEqual(doc?.count, 1);
    assert.ok(doc?.startedAt instanceof Date);
  });
});
```

---

### Step 7: Positional Operators (Stretch Goal)

These operators are more complex and may be deferred to a future phase.

#### 7a: `$` Positional Operator

**Syntax**: `{ $set: { "array.$.field": value } }`

**Behavior**:
- Updates the **first** array element that matches the query condition
- Requires the array field to be part of the query
- Acts as a placeholder for the matched element's index

**MongoDB Error Messages**:
```
The positional operator did not find the match needed from the query.
```

#### 7b: `$[]` All Positional Operator

**Syntax**: `{ $set: { "array.$[].field": value } }`

**Behavior**:
- Updates **all** elements in the array
- Does not require matching query

#### 7c: `$[<identifier>]` Filtered Positional Operator

**Syntax**:
```javascript
updateOne(
  { _id: id },
  { $set: { "items.$[elem].status": "done" } },
  { arrayFilters: [{ "elem.priority": { $gte: 5 } }] }
)
```

**Behavior**:
- Updates array elements that match the arrayFilters condition
- Identifier must be lowercase letter followed by alphanumerics
- No spaces allowed in identifier

**MongoDB Error Messages**:
```
No array filter found for identifier '<identifier>' in path '<path>'
```

**Note**: Positional operators are complex and require query context. They may be implemented in a later phase or simplified version.

---

## Implementation Order

Based on complexity and dependencies:

1. **$min** and **$max** - Simple comparison logic
2. **$mul** - Simple arithmetic with error handling
3. **$rename** - Field manipulation using existing helpers
4. **$currentDate** - Date generation
5. **$setOnInsert** - Requires coordination with upsert logic in `collection.ts`
6. **Positional operators** (stretch goal) - Complex, may defer

---

## File Changes Required

### 1. `src/types.ts`

Extend `UpdateOperators` interface:

```typescript
export interface UpdateOperators {
  // Existing operators
  $set?: Record<string, unknown>;
  $unset?: Record<string, unknown>;
  $inc?: Record<string, number>;
  $push?: Record<string, unknown>;
  $pull?: Record<string, unknown>;
  $addToSet?: Record<string, unknown>;
  $pop?: Record<string, number>;

  // NEW Phase 13 operators
  $min?: Record<string, unknown>;
  $max?: Record<string, unknown>;
  $mul?: Record<string, number>;
  $rename?: Record<string, string>;
  $currentDate?: Record<string, boolean | { $type: "date" | "timestamp" }>;
  $setOnInsert?: Record<string, unknown>;
}
```

### 2. `src/update-operators.ts`

Add implementation for each operator in `applyUpdateOperators()`:

```typescript
// After existing operators...

// Apply $min
if (update.$min) {
  for (const [path, minValue] of Object.entries(update.$min)) {
    const currentValue = getValueAtPath(result as Record<string, unknown>, path);
    if (currentValue === undefined) {
      setValueByPath(result as Record<string, unknown>, path, minValue);
    } else if (compareValues(minValue, currentValue) < 0) {
      setValueByPath(result as Record<string, unknown>, path, minValue);
    }
  }
}

// Apply $max
if (update.$max) {
  for (const [path, maxValue] of Object.entries(update.$max)) {
    const currentValue = getValueAtPath(result as Record<string, unknown>, path);
    if (currentValue === undefined) {
      setValueByPath(result as Record<string, unknown>, path, maxValue);
    } else if (compareValues(maxValue, currentValue) > 0) {
      setValueByPath(result as Record<string, unknown>, path, maxValue);
    }
  }
}

// Apply $mul
if (update.$mul) {
  for (const [path, multiplier] of Object.entries(update.$mul)) {
    const currentValue = getValueAtPath(result as Record<string, unknown>, path);
    if (currentValue === undefined) {
      setValueByPath(result as Record<string, unknown>, path, 0);
    } else if (typeof currentValue !== "number") {
      throw new Error(
        `Cannot apply $mul to a value of non-numeric type. Field '${path}' has non-numeric type ${typeof currentValue}`
      );
    } else {
      setValueByPath(result as Record<string, unknown>, path, currentValue * multiplier);
    }
  }
}

// Apply $rename
if (update.$rename) {
  for (const [oldPath, newPath] of Object.entries(update.$rename)) {
    const value = getValueAtPath(result as Record<string, unknown>, oldPath);
    if (value !== undefined) {
      deleteValueByPath(result as Record<string, unknown>, oldPath);
      setValueByPath(result as Record<string, unknown>, newPath, value);
    }
  }
}

// Apply $currentDate
if (update.$currentDate) {
  for (const [path, spec] of Object.entries(update.$currentDate)) {
    const now = new Date();
    if (spec === true || (typeof spec === "object" && spec.$type === "date")) {
      setValueByPath(result as Record<string, unknown>, path, now);
    } else if (typeof spec === "object" && spec.$type === "timestamp") {
      // For timestamp, use numeric milliseconds (MongoDB uses Timestamp object)
      setValueByPath(result as Record<string, unknown>, path, now.getTime());
    }
  }
}
```

### 3. `src/collection.ts`

Modify `updateOne` and `updateMany` to handle `$setOnInsert`:

```typescript
// In the upsert logic where new document is created:
if (options?.upsert && matchedCount === 0) {
  const newDoc = createDocumentFromFilter(filter);

  // Apply $setOnInsert ONLY during insert
  if (update.$setOnInsert) {
    for (const [path, value] of Object.entries(update.$setOnInsert)) {
      setValueByPath(newDoc as Record<string, unknown>, path, value);
    }
  }

  // Apply other update operators
  const finalDoc = applyUpdateOperators(newDoc, update);
  // ... rest of insert logic
}
```

### 4. `src/utils.ts` or `src/document-utils.ts`

Add a `compareValues` helper for BSON comparison order:

```typescript
/**
 * Compare two values using MongoDB's BSON comparison order.
 * Returns: negative if a < b, 0 if equal, positive if a > b
 */
export function compareValues(a: unknown, b: unknown): number {
  // Handle same type comparisons
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }
  if (typeof a === "string" && typeof b === "string") {
    return a.localeCompare(b);
  }
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }
  // For different types, use BSON type ordering
  // (simplified - full BSON ordering is complex)
  return getBsonTypeOrder(a) - getBsonTypeOrder(b);
}

function getBsonTypeOrder(value: unknown): number {
  if (value === undefined) return 0;
  if (value === null) return 1;
  if (typeof value === "number") return 2;
  if (typeof value === "string") return 3;
  if (typeof value === "object") return 4;
  if (typeof value === "boolean") return 5;
  if (value instanceof Date) return 6;
  return 10;
}
```

---

## Test File Structure

```
test/update-operators-phase13.test.ts
├── Update Operators Phase 13 Tests (${getTestModeName()})
│   │
│   ├── $min Operator
│   │   ├── should update when new value is less than current
│   │   ├── should not update when new value is greater than current
│   │   ├── should create field if missing
│   │   ├── should work with nested fields using dot notation
│   │   ├── should work with dates
│   │   ├── should not update date when new date is later
│   │   └── should work with updateMany
│   │
│   ├── $max Operator
│   │   ├── should update when new value is greater than current
│   │   ├── should not update when new value is less than current
│   │   ├── should create field if missing
│   │   ├── should work with nested fields
│   │   ├── should work with dates
│   │   └── should work with updateMany
│   │
│   ├── $mul Operator
│   │   ├── should multiply existing numeric value
│   │   ├── should create field with 0 if missing
│   │   ├── should handle integer multiplication
│   │   ├── should handle floating-point multiplication
│   │   ├── should handle multiplication by zero
│   │   ├── should handle negative multiplier
│   │   ├── should work with nested fields
│   │   ├── should throw error for non-numeric field value
│   │   └── should work with updateMany
│   │
│   ├── $rename Operator
│   │   ├── should rename a field
│   │   ├── should do nothing if old field does not exist
│   │   ├── should overwrite existing target field
│   │   ├── should work with nested source field using dot notation
│   │   ├── should move field to different nesting level
│   │   ├── should rename multiple fields at once
│   │   └── should work with updateMany
│   │
│   ├── $currentDate Operator
│   │   ├── should set field to current date with true
│   │   ├── should set field to current date with $type: date
│   │   ├── should set field to timestamp with $type: timestamp
│   │   ├── should create field if missing
│   │   ├── should overwrite existing field
│   │   ├── should work with nested fields
│   │   ├── should set multiple fields at once
│   │   └── should work with updateMany
│   │
│   └── $setOnInsert Operator
│       ├── should set fields on upsert insert
│       ├── should not set fields when updating existing document
│       ├── should be ignored without upsert option
│       ├── should work with nested fields
│       ├── should work alone without $set
│       └── should combine with other operators on insert
```

---

## Documentation Updates

After implementation:

1. **PROGRESS.md** - Add Phase 13 completion entry
2. **ROADMAP.md** - Update current phase marker
3. **COMPATIBILITY.md** - Document any discovered behaviors
4. **README.md** - Update "What's Implemented" if needed

---

## Error Messages Reference

| Operator | Condition | Error Message |
|----------|-----------|---------------|
| `$mul` | Non-numeric field | `Cannot apply $mul to a value of non-numeric type. Field '<field>' has non-numeric type <type>` |
| `$mul` | Non-numeric multiplier | `Cannot apply $mul with non-numeric argument` |
| `$currentDate` | Invalid $type | `$currentDate: unrecognized type specification` |
| `$` positional | No array match | `The positional operator did not find the match needed from the query` |
| `$[id]` filtered | Missing filter | `No array filter found for identifier '<id>' in path '<path>'` |

---

## Sources

- [MongoDB $min Documentation](https://www.mongodb.com/docs/manual/reference/operator/update/min/)
- [MongoDB $max Documentation](https://www.mongodb.com/docs/manual/reference/operator/update/max/)
- [MongoDB $mul Documentation](https://www.mongodb.com/docs/manual/reference/operator/update/mul/)
- [MongoDB $rename Documentation](https://www.mongodb.com/docs/manual/reference/operator/update/rename/)
- [MongoDB $currentDate Documentation](https://www.mongodb.com/docs/manual/reference/operator/update/currentdate/)
- [MongoDB $setOnInsert Documentation](https://www.mongodb.com/docs/manual/reference/operator/update/setoninsert/)
- [MongoDB Positional Operators](https://www.mongodb.com/docs/manual/reference/operator/update/positional-filtered/)
- [SERVER-12992 - $mul error message issue](https://jira.mongodb.org/browse/SERVER-12992)
