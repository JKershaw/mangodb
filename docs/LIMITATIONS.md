# MangoDB Limitations

This document provides a comprehensive reference of MongoDB features that MangoDB does not currently support. MangoDB is designed as a lightweight, file-based MongoDB drop-in replacement for development and testing - not as a full MongoDB implementation.

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Fully implemented |
| ⚠️ | Partially implemented |
| ❌ | Not implemented (could be added) |
| ⛔ | Not applicable (by design) |

---

## Quick Summary

| Category | Coverage | Notes |
|----------|----------|-------|
| Query Operators | 23/39 (59%) | Missing geospatial, bitwise |
| Update Operators | 13/20 (65%) | Missing positional operators |
| Aggregation Stages | 13/34 (38%) | Core stages implemented |
| Expression Operators | 57/112 (51%) | Missing advanced math, regex |
| Index Types | 5/8 (63%) | Missing geospatial, hashed |
| Core Features | Limited | No transactions, sessions, streams |

---

## Query Operators

### Comparison Operators ✅ Complete

All comparison operators are fully implemented:
- `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`

### Logical Operators ✅ Complete

All logical operators are fully implemented:
- `$and`, `$or`, `$nor`, `$not`

### Element Operators ✅ Complete

All element operators are fully implemented:
- `$exists`, `$type` (with full BSON type support)

### Array Operators ✅ Complete

All array query operators are fully implemented:
- `$all`, `$elemMatch`, `$size`

### Evaluation Operators ⚠️ Partial

| Operator | Status | Notes |
|----------|--------|-------|
| `$expr` | ✅ | Supports aggregation expressions |
| `$mod` | ✅ | Full implementation |
| `$regex` | ✅ | With `$options` support (i, m, s flags) |
| `$text` | ✅ | Requires text index |
| `$where` | ⛔ | JavaScript evaluation - security concern |
| `$jsonSchema` | ❌ | Schema validation not implemented |

### Geospatial Operators ❌ Not Implemented

None of these operators are supported:
- `$geoIntersects`
- `$geoWithin`
- `$near`
- `$nearSphere`
- `$box`, `$center`, `$centerSphere`, `$geometry`, `$polygon`

**Reason**: Would require GIS library integration and geospatial index support.

### Bitwise Operators ❌ Not Implemented

None of these operators are supported:
- `$bitsAllClear`
- `$bitsAllSet`
- `$bitsAnyClear`
- `$bitsAnySet`

### Projection Operators ❌ Not Implemented

These projection-specific operators are not supported:
- `$` (positional projection)
- `$elemMatch` (in projection context)
- `$meta` (text search metadata)
- `$slice` (array slicing in projection)

**Note**: Basic field projection (inclusion/exclusion) is fully supported.

### Other Query Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$comment` | ❌ | Query comments not implemented |
| `$rand` | ❌ | Random value generation |

---

## Update Operators

### Field Update Operators ✅ Complete

All field update operators are fully implemented:
- `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`
- `$rename`, `$currentDate`, `$setOnInsert`

### Array Update Operators ⚠️ Partial

| Operator | Status | Notes |
|----------|--------|-------|
| `$push` | ✅ | Supports `$each` modifier |
| `$pull` | ✅ | Supports query conditions |
| `$addToSet` | ✅ | Supports `$each` modifier |
| `$pop` | ✅ | Supports 1 (last) and -1 (first) |
| `$pullAll` | ❌ | Use `$pull` with `$in` as workaround |

### Positional Update Operators ❌ Not Implemented

**These are documented in [FUTURE_WORK.md](./FUTURE_WORK.md):**

| Operator | Description |
|----------|-------------|
| `$` | Update first matching array element |
| `$[]` | Update all array elements |
| `$[<identifier>]` | Update elements matching `arrayFilters` |

**Workaround**: Read the document, modify the array in application code, then replace.

### Array Update Modifiers ⚠️ Partial

| Modifier | Status | Notes |
|----------|--------|-------|
| `$each` | ✅ | Works with `$push` and `$addToSet` |
| `$position` | ❌ | Insert at specific array index |
| `$slice` | ❌ | Limit array size after push |
| `$sort` | ❌ | Sort array after push |

### Bitwise Update Operator ❌ Not Implemented

| Operator | Status |
|----------|--------|
| `$bit` | ❌ |

---

## Aggregation Pipeline

### Implemented Stages ✅

| Stage | Notes |
|-------|-------|
| `$match` | Full query operator support |
| `$project` | Inclusion/exclusion, computed fields, `$literal` |
| `$sort` | Multi-field, ascending/descending |
| `$limit` | Positive integer validation |
| `$skip` | Non-negative integer validation |
| `$count` | Returns single document with count |
| `$unwind` | Supports `preserveNullAndEmptyArrays`, `includeArrayIndex` |
| `$group` | 8 accumulator operators |
| `$lookup` | Basic form (localField/foreignField) |
| `$addFields` | Add/modify fields |
| `$set` | Alias for `$addFields` |
| `$replaceRoot` | Replace document root |
| `$out` | Write to collection (must be final) |

### Not Implemented Stages ❌

| Stage | Description | Reason |
|-------|-------------|--------|
| `$bucket` | Group into buckets | Not implemented |
| `$bucketAuto` | Auto-create buckets | Not implemented |
| `$changeStream` | Real-time changes | Requires change streams |
| `$collStats` | Collection statistics | Use `collection.stats()` instead |
| `$densify` | Fill gaps in data | Not implemented |
| `$documents` | Inject literal documents | Not implemented |
| `$facet` | Multiple sub-pipelines | Not implemented |
| `$fill` | Fill missing values | Not implemented |
| `$geoNear` | Geospatial query | Requires geo indexes |
| `$graphLookup` | Recursive lookup | Not implemented |
| `$indexStats` | Index usage stats | Not implemented |
| `$listSessions` | Active sessions | No session support |
| `$merge` | Merge into collection | Use `$out` instead |
| `$planCacheStats` | Query plan stats | No query planner |
| `$redact` | Field-level access control | Not implemented |
| `$replaceWith` | Replace document (4.4+) | Use `$replaceRoot` |
| `$sample` | Random sampling | Not implemented |
| `$search` | Atlas full-text search | Atlas-only feature |
| `$searchMeta` | Atlas search metadata | Atlas-only feature |
| `$setWindowFields` | Window functions | Not implemented |
| `$sortByCount` | Group and count | Use `$group` + `$sort` |
| `$unionWith` | Union collections | Not implemented |
| `$unset` | Remove fields | Use `$project` with exclusion |

### $lookup Limitations

Only the basic form is supported:
```javascript
// ✅ Supported
{ $lookup: { from: "collection", localField: "field", foreignField: "_id", as: "results" } }

// ❌ Not supported - pipeline form
{ $lookup: { from: "collection", let: { localVar: "$field" }, pipeline: [...], as: "results" } }
```

---

## Aggregation Expression Operators

### Arithmetic Operators ⚠️ Partial (9/16)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$abs`, `$add`, `$ceil`, `$divide`, `$floor`, `$mod`, `$multiply`, `$round`, `$subtract` | `$exp`, `$ln`, `$log`, `$log10`, `$pow`, `$sqrt`, `$trunc` |

### Array Operators ⚠️ Partial (8/24)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$arrayElemAt`, `$concatArrays`, `$filter`, `$in`, `$map`, `$reduce`, `$size`, `$slice` | `$arrayToObject`, `$first`, `$indexOfArray`, `$isArray`, `$last`, `$objectToArray`, `$range`, `$reverseArray`, `$zip`, and others |

### String Operators ⚠️ Partial (11/20)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$concat`, `$indexOfCP`, `$ltrim`, `$rtrim`, `$split`, `$strLenCP`, `$substrCP`, `$toLower`, `$toString`, `$toUpper`, `$trim` | `$indexOfBytes`, `$regexFind`, `$regexFindAll`, `$regexMatch`, `$replaceAll`, `$replaceOne`, `$strcasecmp`, `$strLenBytes`, `$substrBytes` |

### Date Operators ⚠️ Partial (8/18)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$dateToString`, `$dayOfMonth`, `$dayOfWeek`, `$hour`, `$minute`, `$month`, `$second`, `$year` | `$dateAdd`, `$dateDiff`, `$dateFromParts`, `$dateFromString`, `$dateToParts`, `$dateSubtract`, `$dayOfYear`, `$isoDayOfWeek`, `$isoWeek`, `$isoWeekYear`, `$millisecond`, `$week` |

### Comparison Operators ✅ Nearly Complete (6/7)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne` | `$cmp` |

### Conditional Operators ⚠️ Partial (2/3)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$cond`, `$ifNull` | `$switch` |

### Type Operators ⚠️ Partial (5/11)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$toBool`, `$toDate`, `$toDouble`, `$toInt`, `$type` | `$convert`, `$isNumber`, `$toDecimal`, `$toLong`, `$toObjectId` |

### Accumulator Operators ⚠️ Partial (8/13)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$addToSet`, `$avg`, `$first`, `$last`, `$max`, `$min`, `$push`, `$sum` | `$accumulator`, `$count`, `$mergeObjects`, `$stdDevPop`, `$stdDevSamp` |

---

## Index Types

### Implemented Index Types ✅

| Type | Options Supported |
|------|-------------------|
| Single Field | `unique`, `sparse`, `name` |
| Compound | `unique`, `sparse`, `name` |
| Text | Basic tokenized search |
| TTL | `expireAfterSeconds` (single field only) |
| Partial | `partialFilterExpression` |

### Not Implemented Index Types ❌

| Type | Description |
|------|-------------|
| 2d | Flat geospatial index |
| 2dsphere | Spherical geospatial index |
| Hashed | Hash-based sharding index |
| Wildcard | Dynamic field indexing |

### Not Implemented Index Options ❌

| Option | Description |
|--------|-------------|
| `collation` | Locale-aware string comparison |
| `hidden` | Hide index from query planner |
| `weights` | Text index field weights |
| `default_language` | Text index language |

---

## Core MongoDB Features

### Not Applicable (By Design) ⛔

These features don't apply to a file-based implementation:

| Feature | Reason |
|---------|--------|
| **Transactions** | File-based storage has no ACID guarantees |
| **Sessions** | No multi-operation session tracking |
| **Connection Pooling** | Single synchronous file access |
| **Authentication** | Rely on filesystem permissions |
| **Read/Write Concerns** | Single-node, synchronous writes |
| **Replica Sets** | Single-node only |
| **Sharding** | Single-node only |

### Not Implemented ❌

| Feature | Description | Workaround |
|---------|-------------|------------|
| **Change Streams** | Real-time document watching | Poll with `find()` |
| **GridFS** | Large file storage | Use filesystem directly |
| **Capped Collections** | Fixed-size, auto-rotating | Manually manage size |
| **Schema Validation** | Server-side validation | Validate in application |
| **Collation** | Locale-aware operations | Handle in application |

---

## Collection Methods

### Implemented ✅

All core CRUD methods:
- `insertOne`, `insertMany`
- `find`, `findOne`
- `updateOne`, `updateMany`
- `deleteOne`, `deleteMany`
- `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`
- `bulkWrite` (ordered and unordered)
- `aggregate`
- `countDocuments`, `estimatedDocumentCount`, `distinct`
- `createIndex`, `dropIndex`, `indexes`, `listIndexes`
- `drop`, `rename`, `stats`

### Not Implemented ❌

| Method | Workaround |
|--------|------------|
| `replaceOne` | Use `bulkWrite` with `replaceOne` operation |
| `watch` | Not available - poll with `find()` |
| `initializeOrderedBulkOp` | Use `bulkWrite()` with `ordered: true` |
| `initializeUnorderedBulkOp` | Use `bulkWrite()` with `ordered: false` |
| `createIndexes` | Call `createIndex` in a loop |
| `dropIndexes` | Call `dropIndex` in a loop |

### Method Options Not Supported ❌

These options are not supported on any method:

| Option | Description |
|--------|-------------|
| `arrayFilters` | Positional array element updates |
| `collation` | Locale-aware string operations |
| `session` | Transaction session |
| `hint` | Index hint (supported only on `find()` cursor) |
| `maxTimeMS` | Operation timeout |
| `allowDiskUse` | Large result handling |

---

## Architectural Limitations

### Storage Model

| Aspect | Limitation |
|--------|------------|
| **File Format** | JSON files - no binary BSON optimization |
| **Memory Usage** | All documents loaded into memory per operation |
| **Document Size** | Limited by available memory and JSON parsing |
| **Concurrency** | Not safe for multiple processes writing simultaneously |

### Performance Characteristics

| Aspect | Limitation |
|--------|------------|
| **Query Optimization** | No query planner - full collection scans |
| **Index Usage** | Indexes enforce constraints but don't optimize queries |
| **Explain Plans** | Not available |
| **Cursor Streaming** | All results materialized immediately |

### Behavioral Differences

| Aspect | MangoDB Behavior |
|--------|------------------|
| **Write Durability** | Synchronous file writes (no journaling) |
| **Atomicity** | Single-document only, no multi-document atomicity |
| **ObjectId Generation** | Uses `bson` library (compatible) |
| **Error Codes** | MongoDB-compatible error codes for common errors |

---

## Recommended Usage

MangoDB is ideal for:
- ✅ Local development without MongoDB setup
- ✅ Unit and integration testing
- ✅ CI/CD pipelines
- ✅ Prototyping and learning
- ✅ Small datasets (< 10,000 documents per collection)

MangoDB is NOT suitable for:
- ❌ Production deployments
- ❌ Large datasets
- ❌ Multi-process access
- ❌ High-throughput workloads
- ❌ Applications requiring transactions
- ❌ Geospatial queries

---

## Feature Requests

If you need a feature that's not implemented, please:
1. Check if there's a workaround listed above
2. Open an issue at the project repository
3. Consider contributing a PR

For features marked as ⛔ (Not Applicable), these are architectural decisions and unlikely to change.
