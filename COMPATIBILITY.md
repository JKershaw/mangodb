# MongoDB Compatibility Notes

This document records MongoDB behaviors discovered through dual-target testing. These are especially useful for understanding implicit/undocumented behaviors that must be replicated for drop-in compatibility.

---

## ObjectId

### Structure
- 12-byte identifier
- Contains embedded timestamp (first 4 bytes)
- Includes machine identifier, process ID, and counter

### JSON Serialization
- MongoDB driver's ObjectId has custom `toJSON()` method
- Returns hex string representation
- Must handle both ObjectId objects and string representations

### Equality
- Two ObjectId instances with same hex value are equal
- String comparison of `.toHexString()` is reliable

---

## Insert Operations

### insertOne

**Return Value**:
```typescript
{
  acknowledged: true,
  insertedId: ObjectId("...")
}
```

**Behaviors**:
- If document has no `_id`, one is generated automatically
- If document has `_id`, that value is used
- Duplicate `_id` throws error (when unique index exists)

### insertMany

**Return Value**:
```typescript
{
  acknowledged: true,
  insertedIds: {
    0: ObjectId("..."),
    1: ObjectId("..."),
    // ... indexed by position
  }
}
```

**Behaviors**:
- `insertedIds` is object with numeric string keys, not array
- Documents processed in order
- All documents receive `_id` if not present

---

## Delete Operations

### deleteOne

**Return Value**:
```typescript
{
  acknowledged: true,
  deletedCount: 0 | 1
}
```

**Behaviors**:
- Deletes first matching document
- Returns `deletedCount: 0` if no match found
- Empty filter `{}` deletes first document in collection

### deleteMany

**Return Value**:
```typescript
{
  acknowledged: true,
  deletedCount: number
}
```

**Behaviors**:
- Deletes all matching documents
- Returns actual count deleted
- Empty filter `{}` deletes all documents

---

## Find Operations

### findOne

**Return Value**:
- Returns document if found
- Returns `null` if not found (not `undefined`)

**Behaviors**:
- Empty filter `{}` returns first document
- With filter, returns first matching document

### find (Cursor)

**Return Value**:
- Returns cursor object, not documents directly
- Call `.toArray()` to get documents

**Behaviors**:
- Empty filter `{}` returns all documents
- Order is insertion order (when no sort specified)

---

## Query Matching

### Empty Filter
- `{}` matches all documents

### Simple Equality
- `{ field: value }` matches documents where `field === value`
- Exact match required (no type coercion)

### Null Matching (Tested)
- `{ field: null }` matches:
  - Documents where `field` is `null`
  - Documents where `field` does not exist
- This is different from `{ field: { $exists: false } }`

**Example**:
```typescript
// Given documents:
// { value: null }
// { value: "something" }
// { other: "field" }  // value is missing

await collection.find({ value: null }).toArray();
// Returns 2 documents: the one with null and the one with missing field
```

### Array Field Matching (Tested)
- `{ tags: "red" }` matches `{ tags: ["red", "blue"] }`
- Any array element matching the value is a match
- Exact array matching: `{ tags: ["a", "b"] }` only matches arrays with exact same elements in order

**Example**:
```typescript
// Given: { tags: ["red", "blue"] }
await collection.find({ tags: "red" }).toArray();  // Matches
await collection.find({ tags: ["red", "blue"] }).toArray();  // Matches (exact)
await collection.find({ tags: ["blue", "red"] }).toArray();  // Does NOT match (order matters)
```

### Nested Field Matching (Tested)
- `{ "a.b.c": 1 }` accesses nested field
- Works with both objects and array indices

**Example**:
```typescript
// Given: { user: { name: "Alice", scores: [10, 20, 30] } }
await collection.find({ "user.name": "Alice" }).toArray();  // Matches
await collection.find({ "user.scores.0": 10 }).toArray();   // Matches (array index)
```

### Array Element Dot Notation (Tested)
- `{ "items.name": "value" }` matches if ANY array element's nested field equals the value
- Works with comparison operators
- Supports multiple levels of nesting

**Example**:
```typescript
// Given: { items: [{ name: "Alice" }, { name: "Bob" }] }
await collection.find({ "items.name": "Alice" }).toArray();  // Matches
await collection.find({ "items.name": "Charlie" }).toArray();  // No match

// With comparison operators:
// Given: { scores: [{ value: 50 }, { value: 80 }] }
await collection.find({ "scores.value": { $gte: 80 } }).toArray();  // Matches
```

---

## Comparison Operators

### $eq - Equality
- Explicit equality: `{ field: { $eq: value } }` same as `{ field: value }`
- Uses same matching logic as implicit equality

### $ne - Not Equal
- `{ field: { $ne: value } }` matches documents where field does not equal value
- Also matches documents where the field is missing

**Example**:
```typescript
// Given: [{ value: 10 }, { other: "field" }]
await collection.find({ value: { $ne: 10 } }).toArray();
// Returns the document with missing field
```

### $gt, $gte, $lt, $lte - Comparison
- Work with numbers, strings (lexicographic), dates, and booleans
- No type coercion - types must be comparable
- Date comparison uses timestamp values
- Boolean comparison: `false < true`

**Example**:
```typescript
await collection.find({ age: { $gte: 18, $lt: 65 } }).toArray();
await collection.find({ name: { $gt: "M" } }).toArray();  // Names after "M"
await collection.find({ createdAt: { $gte: new Date("2024-01-01") } }).toArray();

// Boolean comparison:
// Given: [{ active: false }, { active: true }]
await collection.find({ active: { $gt: false } }).toArray();  // Matches only true
await collection.find({ active: { $lte: false } }).toArray();  // Matches only false
```

### $in - Match Any
- `{ field: { $in: [value1, value2] } }` matches if field equals any value
- If field is an array, matches if any array element is in the $in array

**Example**:
```typescript
await collection.find({ color: { $in: ["red", "blue"] } }).toArray();
// Given: { tags: ["a", "b"] }
await collection.find({ tags: { $in: ["a", "x"] } }).toArray();  // Matches (has "a")
```

### $nin - Match None
- `{ field: { $nin: [value1, value2] } }` matches if field does not equal any value
- Also matches documents where field is missing

---

## Update Operations

### updateOne / updateMany

**Return Value**:
```typescript
{
  acknowledged: true,
  matchedCount: number,   // Number of documents matching filter
  modifiedCount: number,  // Number of documents actually modified
  upsertedCount: number,  // 0 or 1
  upsertedId: ObjectId | null  // ID of upserted document, or null
}
```

**Behaviors**:
- `matchedCount` counts documents matching filter
- `modifiedCount` only counts documents where values actually changed
- Setting same value results in `modifiedCount: 0`

### $set Operator

- Sets field values on matched documents
- Creates fields if they don't exist
- Supports dot notation for nested fields
- Dot notation creates nested structure if path doesn't exist

**Example**:
```typescript
// Creates nested structure: { a: { b: { c: "value" } } }
await collection.updateOne({ name: "Alice" }, { $set: { "a.b.c": "value" } });

// Update array element by index
await collection.updateOne({}, { $set: { "items.0": "newValue" } });
```

### $unset Operator

- Removes fields from documents
- Value doesn't matter (commonly "" or 1)
- No-op if field doesn't exist (still counted as matched)

**Example**:
```typescript
await collection.updateOne({ name: "Alice" }, { $unset: { age: "" } });
```

### $inc Operator

- Increments numeric fields
- Creates field with increment value if doesn't exist
- Negative values decrement
- Works with floating point numbers

**Example**:
```typescript
// Increment by 10
await collection.updateOne({ name: "Alice" }, { $inc: { score: 10 } });

// Decrement by 5
await collection.updateOne({ name: "Alice" }, { $inc: { score: -5 } });

// Creates field with value 50 if doesn't exist
await collection.updateOne({ name: "Alice" }, { $inc: { newField: 50 } });
```

### Upsert Option

- When `upsert: true` and no match found, inserts new document
- New document includes filter equality conditions
- `$set` and `$inc` values applied to new document
- Returns `upsertedId` with the new document's ObjectId

**Example**:
```typescript
// If no document with name: "Bob", creates { name: "Bob", age: 25 }
await collection.updateOne(
  { name: "Bob" },
  { $set: { age: 25 } },
  { upsert: true }
);
```

### Combining Operators

- Multiple operators can be used in single update
- Applied in order: $set, $unset, $inc

**Example**:
```typescript
await collection.updateOne(
  { name: "Alice" },
  {
    $set: { status: "active" },
    $inc: { loginCount: 1 },
    $unset: { tempField: "" }
  }
);
```

---

## Cursor Operations

### sort

**Behaviors**:
- Cursor methods can be chained in any order in code
- Execution order is always: sort → skip → limit (regardless of calling order)
- Supports single field sort: `cursor.sort({ name: 1 })`
- Supports compound sort: `cursor.sort({ category: 1, name: -1 })`
- `1` = ascending, `-1` = descending
- Supports dot notation for nested fields: `cursor.sort({ "user.name": 1 })`

**Type Ordering in Sort (Implemented)**:
Based on [MongoDB BSON Type Comparison Order](https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/). For ascending sort:
1. Empty arrays
2. Null / Undefined / Missing fields
3. Numbers
4. Strings
5. Objects
6. Arrays (non-empty)
7. ObjectId
8. Boolean
9. Date

**Array Field Sorting**:
When sorting by a field that contains an array:
- **Ascending sort**: Uses the minimum element of the array as the sort key
- **Descending sort**: Uses the maximum element of the array as the sort key
- Empty arrays sort before null

```typescript
// Example: sorting by array field
await collection.insertMany([
  { name: "doc1", scores: [10, 20, 30] },  // min: 10, max: 30
  { name: "doc2", scores: [5, 15, 25] },   // min: 5, max: 25
  { name: "doc3", scores: [8, 50] },       // min: 8, max: 50
]);

// Ascending: uses minimum element
await collection.find({}).sort({ scores: 1 }).toArray();
// Returns: doc2 (min 5), doc3 (min 8), doc1 (min 10)

// Descending: uses maximum element
await collection.find({}).sort({ scores: -1 }).toArray();
// Returns: doc3 (max 50), doc1 (max 30), doc2 (max 25)
```

**Null/Missing Behavior**:
```typescript
// Given: [{ value: 20 }, { value: null }, { value: 10 }]
await collection.find({}).sort({ value: 1 }).toArray();
// Returns: [{ value: null }, { value: 10 }, { value: 20 }]

// Missing fields behave like null
// Given: [{ value: 20 }, { other: "field" }, { value: 10 }]
await collection.find({}).sort({ value: 1 }).toArray();
// Returns: [{ other: "field" }, { value: 10 }, { value: 20 }]
```

### limit

**Behaviors**:
- Returns at most n documents
- If fewer documents exist, returns all available
- `limit(0)` means no limit (returns all documents)
- Negative values are treated as their absolute value

### skip

**Behaviors**:
- Skips first n documents
- Applied after sort but before limit
- If skip exceeds document count, returns empty array

### Chaining Example

```typescript
// These all produce the same result:
await collection.find({}).sort({ value: 1 }).skip(2).limit(3).toArray();
await collection.find({}).limit(3).sort({ value: 1 }).skip(2).toArray();
await collection.find({}).skip(2).limit(3).sort({ value: 1 }).toArray();
// Order is always: sort, then skip, then limit
```

---

## Projection

### Inclusion Mode

```typescript
await collection.find({}, { projection: { name: 1, age: 1 } }).toArray();
// Returns only _id, name, age (\_id included by default)

await collection.find({}, { projection: { name: 1, _id: 0 } }).toArray();
// Returns only name (\_id explicitly excluded)
```

### Exclusion Mode

```typescript
await collection.find({}, { projection: { password: 0, secret: 0 } }).toArray();
// Returns all fields except password and secret
```

### Rules

- Cannot mix inclusion (1) and exclusion (0) in the same projection
- Exception: `_id: 0` can be combined with inclusion
- Nested fields supported with dot notation: `{ "address.city": 1 }`
- Works with both `find()` and `findOne()`

---

## countDocuments

```typescript
await collection.countDocuments({});  // Count all documents
await collection.countDocuments({ status: "active" });  // Count matching
await collection.countDocuments({ value: { $gte: 10 } });  // Works with operators
```

**Behaviors**:
- Returns number (not an object)
- Empty collection returns 0
- No matches returns 0

---

## Logical Operators

### $exists

```typescript
await collection.find({ age: { $exists: true } }).toArray();
await collection.find({ deleted: { $exists: false } }).toArray();
```

**Behaviors**:
- `$exists: true` matches documents where the field exists (including null values)
- `$exists: false` matches documents where the field does not exist
- Works with dot notation for nested fields: `{ "user.email": { $exists: true } }`
- With array traversal (`"items.field"`):
  - `$exists: true` matches if ANY array element has the field
  - `$exists: false` matches only if NO array element has the field

### $and

```typescript
// Explicit AND - useful when same field has multiple conditions
await collection.find({
  $and: [{ score: { $gte: 50 } }, { score: { $lte: 100 } }]
}).toArray();

// Can combine with field conditions
await collection.find({
  type: "A",
  $and: [{ status: "active" }]
}).toArray();
```

**Behaviors**:
- All conditions in the array must match
- Can be nested with other logical operators
- Throws error if value is not a nonempty array

### $or

```typescript
await collection.find({
  $or: [{ status: "active" }, { priority: "high" }]
}).toArray();

// Combines with field conditions via implicit AND
await collection.find({
  type: "A",
  $or: [{ status: "active" }, { status: "pending" }]
}).toArray();
```

**Behaviors**:
- At least one condition must match
- Field conditions are AND'd with the $or result
- Throws error if value is not a nonempty array

### $not

```typescript
// Invert operator result
await collection.find({ age: { $not: { $gt: 30 } } }).toArray();
await collection.find({ status: { $not: { $in: ["deleted", "archived"] } } }).toArray();
```

**Behaviors**:
- Wraps another operator expression and inverts its result
- **IMPORTANT**: `$not` does NOT match documents where the field is missing
- This differs from `$ne`, which does match missing fields
- Example: `{ value: { $not: { $gt: 25 } } }` on `{ other: "field" }` does NOT match
- Throws error "$not needs a regex or a document" if value is not an operator expression

### $nor

```typescript
await collection.find({
  $nor: [{ status: "deleted" }, { status: "archived" }]
}).toArray();
```

**Behaviors**:
- No condition in the array may match (opposite of $or)
- Matches documents where the queried field is missing
- Throws error if value is not a nonempty array

### Field-Level Logical Operator Errors

Logical operators (`$and`, `$or`, `$nor`) are only valid at the top level of a filter, not inside field conditions:

```typescript
// VALID - top level
await collection.find({ $and: [{ a: 1 }, { b: 2 }] }).toArray();

// INVALID - throws error
await collection.find({ field: { $and: [{ a: 1 }] } }).toArray();
// Error: "unknown operator: $and"
```

---

## Notes

This document will be updated as more behaviors are discovered through testing. Each entry should include:

1. The specific behavior observed
2. Example code demonstrating it
3. Any edge cases or surprising results

When in doubt, write a test and run it against real MongoDB to verify behavior.
