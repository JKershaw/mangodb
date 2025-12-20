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
- Work with numbers, strings (lexicographic), and dates
- No type coercion - types must be comparable
- Date comparison uses timestamp values

**Example**:
```typescript
await collection.find({ age: { $gte: 18, $lt: 65 } }).toArray();
await collection.find({ name: { $gt: "M" } }).toArray();  // Names after "M"
await collection.find({ createdAt: { $gte: new Date("2024-01-01") } }).toArray();
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

## Type Ordering in Sort (To Be Tested)

MongoDB has specific ordering for mixed types:
1. MinKey
2. Null
3. Numbers (int, long, double, decimal)
4. Symbol, String
5. Object
6. Array
7. BinData
8. ObjectId
9. Boolean
10. Date
11. Timestamp
12. Regular Expression
13. MaxKey

---

## Notes

This document will be updated as more behaviors are discovered through testing. Each entry should include:

1. The specific behavior observed
2. Example code demonstrating it
3. Any edge cases or surprising results

When in doubt, write a test and run it against real MongoDB to verify behavior.
