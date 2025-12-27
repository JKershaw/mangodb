# Edge Cases and Gotchas

This document covers MongoDB behaviors that MangoDB replicates, plus known differences. Understanding these helps avoid surprises.

## Query Behavior

### Null Matching

`{ field: null }` matches **both** null values AND missing fields:

```javascript
// Documents:
// { value: null }
// { value: "something" }
// { other: "field" }  <- value is missing

await collection.find({ value: null }).toArray();
// Returns 2 documents: null value AND missing field

// To match only null (not missing):
await collection.find({ value: { $eq: null, $exists: true } }).toArray();
// Returns 1 document: only the null value
```

### Array Field Matching

A scalar query on an array field matches if **any** element matches:

```javascript
// Document: { tags: ["red", "blue", "green"] }

await collection.find({ tags: "red" }).toArray();     // Matches!
await collection.find({ tags: "yellow" }).toArray();  // No match
```

Exact array matching requires the exact array:

```javascript
await collection.find({ tags: ["red", "blue"] }).toArray();  // Exact match only
await collection.find({ tags: ["blue", "red"] }).toArray();  // Order matters - different array
```

### $elemMatch vs Implicit AND

Without `$elemMatch`, conditions can match different array elements:

```javascript
// Document: { results: [{ score: 80, passed: false }, { score: 60, passed: true }] }

// WITHOUT $elemMatch - matches! (score>=80 on elem 0, passed=true on elem 1)
await collection.find({
  "results.score": { $gte: 80 },
  "results.passed": true
}).toArray();

// WITH $elemMatch - no match (no single element has both)
await collection.find({
  results: { $elemMatch: { score: { $gte: 80 }, passed: true } }
}).toArray();
```

### $not Matches Missing Fields

`$not` inverts the condition, so missing fields often match:

```javascript
// Documents:
// { value: 30 }
// { other: "field" }  <- value is missing

await collection.find({ value: { $not: { $gt: 25 } } }).toArray();
// Returns the missing-field document! (can't be > 25 if it doesn't exist)
```

### $ne Matches Missing Fields

Similar to `$not`:

```javascript
// Documents:
// { status: "active" }
// { status: "inactive" }
// { name: "orphan" }  <- status is missing

await collection.find({ status: { $ne: "active" } }).toArray();
// Returns "inactive" AND the document with missing status
```

---

## Update Behavior

### $set Creates Nested Paths

Dot notation in `$set` creates the full path if it doesn't exist:

```javascript
// Document: { name: "Alice" }

await collection.updateOne(
  { name: "Alice" },
  { $set: { "address.city.name": "NYC" } }
);

// Result: { name: "Alice", address: { city: { name: "NYC" } } }
```

### $inc Creates Fields

`$inc` on a missing field creates it with the increment value:

```javascript
// Document: { name: "Alice" }

await collection.updateOne({ name: "Alice" }, { $inc: { score: 10 } });
// Result: { name: "Alice", score: 10 }
```

### $push on Non-Array Throws

```javascript
// Document: { name: "Alice" }

await collection.updateOne({ name: "Alice" }, { $push: { name: "Bob" } });
// Throws: "The field 'name' must be an array but is of type string"
```

### $addToSet Object Key Order

Objects with different key order are considered different:

```javascript
// Document: { items: [{ id: 1, name: "a" }] }

await collection.updateOne({}, { $addToSet: { items: { id: 1, name: "a" } } });
// No change - exact match exists

await collection.updateOne({}, { $addToSet: { items: { name: "a", id: 1 } } });
// ADDS the object! Different key order = different value
```

### Positional $ Requires Query Match

The `$` positional operator only works when the query matches an array element:

```javascript
// Document: { scores: [10, 20, 30] }

// Works - query matches an element
await collection.updateOne(
  { scores: 20 },
  { $set: { "scores.$": 25 } }
);

// Fails - query doesn't match array element
await collection.updateOne(
  { _id: someId },
  { $set: { "scores.$": 25 } }  // Error: no matching element
);
```

---

## Sort Behavior

### BSON Type Ordering

MongoDB sorts by type first, then value. MangoDB follows this order:

1. Empty arrays
2. Null / Undefined / Missing fields
3. Numbers
4. Strings
5. Objects
6. Non-empty arrays
7. ObjectId
8. Boolean
9. Date

```javascript
// Documents with mixed types in 'value' field
await collection.find({}).sort({ value: 1 }).toArray();
// Order: null/missing, numbers, strings, objects, arrays, booleans, dates
```

### Array Field Sorting

When sorting by an array field:
- **Ascending**: Uses minimum element
- **Descending**: Uses maximum element

```javascript
// Documents:
// { name: "A", scores: [10, 20, 30] }  <- min: 10, max: 30
// { name: "B", scores: [5, 15, 25] }   <- min: 5, max: 25

await collection.find({}).sort({ scores: 1 }).toArray();
// Order: B (min 5), A (min 10)

await collection.find({}).sort({ scores: -1 }).toArray();
// Order: A (max 30), B (max 25)
```

---

## Aggregation Behavior

### $toBool - Strings Are Always Truthy

All strings are truthy, including empty string:

```javascript
{ $toBool: "" }       // true (!)
{ $toBool: "false" }  // true (!)
{ $toBool: "0" }      // true (!)
{ $toBool: 0 }        // false
{ $toBool: null }     // null
```

### $toInt Truncates Toward Zero

Not the same as floor:

```javascript
{ $toInt: 3.9 }   // 3
{ $toInt: -3.9 }  // -3 (not -4)
```

### Date Operators Use UTC

All date operators return UTC values:

```javascript
// Date: 2023-06-15T14:30:00Z (UTC)

{ $hour: "$date" }       // 14 (UTC hour)
{ $dayOfWeek: "$date" }  // 5 (1=Sunday, 7=Saturday)
{ $month: "$date" }      // 6 (1-12, not 0-11)
```

### Variable Scoping in Expressions

`$map`, `$filter`, `$reduce` use `$$varName` syntax:

```javascript
{
  $filter: {
    input: "$scores",
    as: "s",
    cond: { $gte: ["$$s", 60] }  // Note: $$ prefix
  }
}
```

Default variable names:
- `$filter`, `$map`: `$$this`
- `$reduce`: `$$value` (accumulator), `$$this` (current)

---

## Index Behavior

### Indexes Don't Optimize Queries

MangoDB indexes enforce constraints but don't speed up queries. All queries perform full collection scans.

### Unique Index Allows One Null

A unique index allows one document with a null/missing value:

```javascript
await collection.createIndex({ email: 1 }, { unique: true });

await collection.insertOne({ name: "A" });  // OK - email missing
await collection.insertOne({ name: "B" });  // Error - duplicate null
```

### Cannot Drop _id Index

```javascript
await collection.dropIndex("_id_");
// Throws: "cannot drop _id index"
```

---

## JSON Storage Differences

MangoDB stores data as JSON, not BSON. This affects some types:

| Type | MongoDB | MangoDB |
|------|---------|---------|
| `undefined` | Stored explicitly | Stripped (treated as missing) |
| `NaN` | Stored as NaN | Stored as `null` |
| `Infinity` | Stored as Infinity | Stored as `null` |
| `-Infinity` | Stored as -Infinity | Stored as `null` |
| Binary data | Native BinData | Base64 string |
| Decimal128 | Native decimal | JavaScript number |

### ObjectId Serialization

ObjectIds are serialized as extended JSON:

```javascript
// In file:
{ "_id": { "$oid": "507f1f77bcf86cd799439011" } }

// When queried, returns proper ObjectId instance
```

### Date Serialization

Dates are serialized as ISO strings:

```javascript
// In file:
{ "createdAt": { "$date": "2023-06-15T14:30:00.000Z" } }
```

---

## Geospatial Gotchas

### Coordinate Order

GeoJSON uses `[longitude, latitude]`, not `[latitude, longitude]`:

```javascript
// Correct - New York City
{ type: "Point", coordinates: [-74.006, 40.7128] }

// Wrong - swapped coordinates give wrong results
{ type: "Point", coordinates: [40.7128, -74.006] }
```

### $near Requires Index

```javascript
// Throws without geo index
await collection.find({
  location: { $near: { $geometry: { type: "Point", coordinates: [0, 0] } } }
}).toArray();
// Error: "$near requires a 2d or 2dsphere index"
```

### $geoNear Must Be First Stage

```javascript
// Throws - not first stage
await collection.aggregate([
  { $match: { active: true } },
  { $geoNear: { near: { type: "Point", coordinates: [0, 0] }, distanceField: "dist" } }
]).toArray();
// Error: "$geoNear is only valid as the first stage"
```

---

## Error Code Compatibility

MangoDB uses MongoDB error codes for common errors:

| Error | Code | Message Pattern |
|-------|------|-----------------|
| Duplicate key | 11000 | `E11000 duplicate key error collection: ...` |
| Index not found | - | `index not found with name [name]` |
| Cannot drop _id | - | `cannot drop _id index` |
| Text index required | - | `text index required for $text query` |

---

## Concurrency Limitations

MangoDB is **not safe for concurrent writes** from multiple processes:

```javascript
// DON'T: Multiple processes writing to same data directory
// Process 1:
const client1 = new MangoClient('./data');

// Process 2:
const client2 = new MangoClient('./data');  // May cause data corruption
```

For testing with parallel test runners, use isolated data directories per test file or process.
