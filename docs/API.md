# API Quick Reference

Scannable reference for MangoDB's MongoDB-compatible API.

## Client

```typescript
import { MangoClient } from '@jkershaw/mangodb';

const client = new MangoClient('./data');  // Data directory path
await client.connect();
const db = client.db('databaseName');
await client.close();
```

## Collection Methods

### Insert

```typescript
// Insert one document
await collection.insertOne({ name: 'Alice' });
// Returns: { acknowledged: true, insertedId: ObjectId }

// Insert multiple documents
await collection.insertMany([{ name: 'Bob' }, { name: 'Charlie' }]);
// Returns: { acknowledged: true, insertedIds: { 0: ObjectId, 1: ObjectId } }
```

### Find

```typescript
// Find one document
await collection.findOne({ name: 'Alice' });
// Returns: Document | null

// Find multiple documents
await collection.find({ status: 'active' }).toArray();
// Returns: Document[]

// With options
await collection.find({ status: 'active' })
  .sort({ createdAt: -1 })
  .skip(10)
  .limit(5)
  .project({ password: 0 })
  .toArray();
```

### Update

```typescript
// Update one
await collection.updateOne(
  { name: 'Alice' },
  { $set: { status: 'active' } }
);
// Returns: { acknowledged, matchedCount, modifiedCount, upsertedCount, upsertedId }

// Update many
await collection.updateMany(
  { status: 'pending' },
  { $set: { status: 'processed' } }
);

// With upsert
await collection.updateOne(
  { email: 'new@example.com' },
  { $set: { name: 'New User' } },
  { upsert: true }
);

// Replace entire document
await collection.replaceOne(
  { _id: someId },
  { name: 'Replaced', status: 'new' }
);
```

### Delete

```typescript
// Delete one
await collection.deleteOne({ name: 'Alice' });
// Returns: { acknowledged: true, deletedCount: 0 | 1 }

// Delete many
await collection.deleteMany({ status: 'expired' });
// Returns: { acknowledged: true, deletedCount: number }
```

### FindOneAnd*

```typescript
// Find, update, and return
await collection.findOneAndUpdate(
  { name: 'Alice' },
  { $inc: { score: 1 } },
  { returnDocument: 'after' }  // 'before' or 'after'
);

// Find and delete
await collection.findOneAndDelete({ status: 'expired' });

// Find and replace
await collection.findOneAndReplace(
  { _id: someId },
  { name: 'Replaced' },
  { returnDocument: 'after' }
);
```

### Bulk Write

```typescript
await collection.bulkWrite([
  { insertOne: { document: { name: 'New' } } },
  { updateOne: { filter: { name: 'Old' }, update: { $set: { name: 'Updated' } } } },
  { deleteOne: { filter: { status: 'expired' } } }
], { ordered: false });  // ordered: true (default) stops on first error
```

### Count & Distinct

```typescript
await collection.countDocuments({ status: 'active' });
// Returns: number

await collection.estimatedDocumentCount();
// Returns: number (faster, less accurate)

await collection.distinct('status');
// Returns: string[]
```

### Indexes

```typescript
// Create index
await collection.createIndex({ email: 1 });
await collection.createIndex({ email: 1 }, { unique: true, sparse: true });
await collection.createIndex({ name: 1, age: -1 });  // Compound
await collection.createIndex({ location: '2dsphere' });  // Geospatial
await collection.createIndex({ content: 'text' });  // Text search

// Create multiple indexes
await collection.createIndexes([
  { key: { email: 1 }, unique: true },
  { key: { createdAt: -1 } }
]);

// List indexes
await collection.indexes();
// Returns: IndexInfo[]

// Drop index
await collection.dropIndex('email_1');
await collection.dropIndex({ email: 1 });
await collection.dropIndexes();  // Drop all except _id
```

### Other

```typescript
await collection.drop();           // Delete collection
await collection.rename('newName');
await collection.stats();          // Collection statistics
```

---

## Query Operators

### Comparison

```typescript
{ field: value }                    // Implicit $eq
{ field: { $eq: value } }
{ field: { $ne: value } }
{ field: { $gt: value } }
{ field: { $gte: value } }
{ field: { $lt: value } }
{ field: { $lte: value } }
{ field: { $in: [v1, v2] } }
{ field: { $nin: [v1, v2] } }
```

### Logical

```typescript
{ $and: [{ a: 1 }, { b: 2 }] }
{ $or: [{ a: 1 }, { b: 2 }] }
{ $nor: [{ a: 1 }, { b: 2 }] }
{ field: { $not: { $gt: 5 } } }
```

### Element

```typescript
{ field: { $exists: true } }
{ field: { $type: 'string' } }      // 'number', 'bool', 'array', 'object', etc.
```

### Array

```typescript
{ tags: { $all: ['a', 'b'] } }      // Contains all
{ tags: { $size: 3 } }              // Exact size
{ results: { $elemMatch: { score: { $gte: 80 }, passed: true } } }
```

### Evaluation

```typescript
{ field: { $regex: /pattern/i } }
{ field: { $regex: 'pattern', $options: 'i' } }
{ value: { $mod: [divisor, remainder] } }
{ $text: { $search: 'keywords' } }
{ $expr: { $gt: ['$qty', '$minQty'] } }
{ $jsonSchema: { required: ['name'], properties: { name: { bsonType: 'string' } } } }
```

### Geospatial

```typescript
{ location: { $geoWithin: { $geometry: { type: 'Polygon', coordinates: [...] } } } }
{ location: { $geoIntersects: { $geometry: { type: 'LineString', coordinates: [...] } } } }
{ location: { $near: { $geometry: { type: 'Point', coordinates: [lng, lat] }, $maxDistance: 1000 } } }
{ location: { $nearSphere: { $geometry: { type: 'Point', coordinates: [lng, lat] } } } }
```

### Bitwise

```typescript
{ flags: { $bitsAllSet: [0, 2] } }
{ flags: { $bitsAllClear: 5 } }      // Bitmask
{ flags: { $bitsAnySet: [1, 3] } }
{ flags: { $bitsAnyClear: [0, 1] } }
```

---

## Update Operators

### Field

```typescript
{ $set: { field: value } }
{ $unset: { field: '' } }
{ $inc: { counter: 1 } }
{ $mul: { price: 1.1 } }
{ $min: { low: 5 } }
{ $max: { high: 100 } }
{ $rename: { oldName: 'newName' } }
{ $currentDate: { lastModified: true } }
{ $setOnInsert: { createdAt: new Date() } }  // Only on upsert
```

### Array

```typescript
{ $push: { tags: 'new' } }
{ $push: { tags: { $each: ['a', 'b'], $position: 0, $slice: 5, $sort: 1 } } }
{ $addToSet: { tags: 'unique' } }
{ $addToSet: { tags: { $each: ['a', 'b'] } } }
{ $pull: { tags: 'remove' } }
{ $pull: { items: { status: 'expired' } } }
{ $pullAll: { tags: ['a', 'b'] } }
{ $pop: { tags: 1 } }               // Remove last
{ $pop: { tags: -1 } }              // Remove first
```

### Positional

```typescript
{ $set: { 'items.$.price': 10 } }           // First matching
{ $inc: { 'items.$[].count': 1 } }          // All elements
{ $set: { 'items.$[elem].status': 'done' } } // With arrayFilters

// With arrayFilters option
await collection.updateOne(
  { 'items.status': 'pending' },
  { $set: { 'items.$[elem].status': 'done' } },
  { arrayFilters: [{ 'elem.status': 'pending' }] }
);
```

### Bitwise

```typescript
{ $bit: { flags: { and: 5 } } }
{ $bit: { flags: { or: 3 } } }
{ $bit: { flags: { xor: 7 } } }
```

---

## Aggregation Stages

```typescript
await collection.aggregate([
  { $match: { status: 'active' } },
  { $project: { name: 1, total: { $multiply: ['$price', '$qty'] } } },
  { $group: { _id: '$category', total: { $sum: '$amount' } } },
  { $sort: { total: -1 } },
  { $limit: 10 },
  { $skip: 5 },
  { $count: 'totalDocs' },
  { $unwind: '$items' },
  { $lookup: { from: 'other', localField: 'id', foreignField: '_id', as: 'joined' } },
  { $addFields: { newField: { $concat: ['$a', '$b'] } } },
  { $set: { field: 'value' } },
  { $unset: ['field1', 'field2'] },
  { $replaceRoot: { newRoot: '$nested' } },
  { $facet: { pipeline1: [...], pipeline2: [...] } },
  { $bucket: { groupBy: '$price', boundaries: [0, 100, 500], default: 'Other' } },
  { $bucketAuto: { groupBy: '$price', buckets: 4 } },
  { $sortByCount: '$category' },
  { $sample: { size: 5 } },
  { $unionWith: 'otherCollection' },
  { $graphLookup: { from: 'employees', startWith: '$reportsTo', connectFromField: 'reportsTo', connectToField: '_id', as: 'hierarchy' } },
  { $redact: { $cond: { if: { $eq: ['$level', 'public'] }, then: '$$DESCEND', else: '$$PRUNE' } } },
  { $merge: { into: 'targetCollection', whenMatched: 'merge', whenNotMatched: 'insert' } },
  { $out: 'outputCollection' },
  { $geoNear: { near: { type: 'Point', coordinates: [lng, lat] }, distanceField: 'distance' } }
]).toArray();
```

---

## Expression Operators

### Arithmetic

```typescript
{ $add: [1, 2] }
{ $subtract: [10, 3] }
{ $multiply: [2, 3] }
{ $divide: [10, 2] }
{ $mod: [10, 3] }
{ $abs: -5 }
{ $ceil: 3.2 }
{ $floor: 3.8 }
{ $round: [3.456, 2] }
{ $trunc: 3.9 }
{ $sqrt: 16 }
{ $pow: [2, 3] }
{ $exp: 1 }
{ $ln: 10 }
{ $log: [100, 10] }
{ $log10: 100 }
```

### String

```typescript
{ $concat: ['Hello', ' ', 'World'] }
{ $toLower: '$name' }
{ $toUpper: '$name' }
{ $trim: { input: '$text' } }
{ $ltrim: { input: '$text' } }
{ $rtrim: { input: '$text' } }
{ $split: ['$csv', ','] }
{ $substrCP: ['$text', 0, 5] }
{ $strLenCP: '$text' }
{ $indexOfCP: ['$text', 'search'] }
{ $regexMatch: { input: '$text', regex: /pattern/ } }
{ $regexFind: { input: '$text', regex: /pattern/ } }
{ $replaceOne: { input: '$text', find: 'old', replacement: 'new' } }
{ $replaceAll: { input: '$text', find: 'old', replacement: 'new' } }
```

### Array

```typescript
{ $arrayElemAt: ['$arr', 0] }
{ $first: '$arr' }
{ $last: '$arr' }
{ $size: '$arr' }
{ $slice: ['$arr', 2] }
{ $slice: ['$arr', 1, 3] }
{ $concatArrays: ['$arr1', '$arr2'] }
{ $in: ['value', '$arr'] }
{ $indexOfArray: ['$arr', 'value'] }
{ $isArray: '$field' }
{ $range: [0, 10] }
{ $reverseArray: '$arr' }
{ $sortArray: { input: '$arr', sortBy: 1 } }
{ $filter: { input: '$arr', as: 'x', cond: { $gte: ['$$x', 5] } } }
{ $map: { input: '$arr', as: 'x', in: { $multiply: ['$$x', 2] } } }
{ $reduce: { input: '$arr', initialValue: 0, in: { $add: ['$$value', '$$this'] } } }
```

### Date

```typescript
{ $year: '$date' }
{ $month: '$date' }
{ $dayOfMonth: '$date' }
{ $hour: '$date' }
{ $minute: '$date' }
{ $second: '$date' }
{ $millisecond: '$date' }
{ $dayOfWeek: '$date' }
{ $dayOfYear: '$date' }
{ $week: '$date' }
{ $isoWeek: '$date' }
{ $isoWeekYear: '$date' }
{ $isoDayOfWeek: '$date' }
{ $dateAdd: { startDate: '$date', unit: 'day', amount: 7 } }
{ $dateSubtract: { startDate: '$date', unit: 'month', amount: 1 } }
{ $dateDiff: { startDate: '$start', endDate: '$end', unit: 'day' } }
{ $dateToString: { date: '$date', format: '%Y-%m-%d' } }
{ $dateFromString: { dateString: '2023-01-15' } }
```

### Conditional

```typescript
{ $cond: { if: { $gte: ['$qty', 10] }, then: 'bulk', else: 'single' } }
{ $cond: [{ $gte: ['$qty', 10] }, 'bulk', 'single'] }  // Array form
{ $ifNull: ['$field', 'default'] }
{ $switch: { branches: [{ case: { $eq: ['$x', 1] }, then: 'one' }], default: 'other' } }
```

### Type

```typescript
{ $type: '$field' }
{ $toBool: '$field' }
{ $toDate: '$field' }
{ $toDouble: '$field' }
{ $toInt: '$field' }
{ $toLong: '$field' }
{ $toObjectId: '$field' }
{ $toString: '$field' }
{ $convert: { input: '$field', to: 'int', onError: 0 } }
{ $isNumber: '$field' }
```

### Comparison

```typescript
{ $eq: ['$a', '$b'] }
{ $ne: ['$a', '$b'] }
{ $gt: ['$a', '$b'] }
{ $gte: ['$a', '$b'] }
{ $lt: ['$a', '$b'] }
{ $lte: ['$a', '$b'] }
{ $cmp: ['$a', '$b'] }  // Returns -1, 0, or 1
```

### Trigonometry

```typescript
{ $sin: '$radians' }           // Sine (radians input)
{ $cos: '$radians' }           // Cosine (radians input)
{ $tan: '$radians' }           // Tangent (radians input)
{ $asin: '$value' }            // Arc sine, returns radians
{ $acos: '$value' }            // Arc cosine, returns radians
{ $atan: '$value' }            // Arc tangent, returns radians
{ $atan2: ['$y', '$x'] }       // Arc tangent of y/x
{ $sinh: '$value' }            // Hyperbolic sine
{ $cosh: '$value' }            // Hyperbolic cosine
{ $tanh: '$value' }            // Hyperbolic tangent
{ $asinh: '$value' }           // Inverse hyperbolic sine
{ $acosh: '$value' }           // Inverse hyperbolic cosine (input >= 1)
{ $atanh: '$value' }           // Inverse hyperbolic tangent (input in -1..1)
{ $degreesToRadians: '$deg' }  // Convert degrees to radians
{ $radiansToDegrees: '$rad' }  // Convert radians to degrees
```

### Accumulators (in $group)

```typescript
{ $sum: '$amount' }
{ $avg: '$score' }
{ $min: '$value' }
{ $max: '$value' }
{ $first: '$field' }
{ $last: '$field' }
{ $push: '$field' }
{ $addToSet: '$field' }
{ $count: {} }
{ $mergeObjects: '$obj' }
{ $stdDevPop: '$values' }
{ $stdDevSamp: '$values' }
```

---

## Index Options

```typescript
{
  unique: true,            // Reject duplicate values
  sparse: true,            // Only index documents with the field
  name: 'custom_name',     // Custom index name
  hidden: true,            // Hide from query planner
  expireAfterSeconds: 3600, // TTL index
  partialFilterExpression: { status: 'active' },
  weights: { title: 10, content: 1 },  // Text index weights
  default_language: 'english',  // Text index language
  wildcardProjection: { secret: 0 }  // Wildcard index
}
```
