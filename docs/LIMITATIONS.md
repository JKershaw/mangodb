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
| Query Operators | 32/39 (82%) | Missing projection operators |
| Update Operators | 20/20 (100%) | All operators including positional |
| Aggregation Stages | 28/34 (82%) | Core stages + window functions + geospatial |
| Expression Operators | 106/112 (95%) | Nearly complete coverage |
| Index Types | 9/9 (100%) | All types including hashed, wildcard |
| Index Options | 10/10 (100%) | All options including collation, hidden, weights |
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

### Geospatial Operators ✅ Complete

All geospatial query operators are fully implemented:
- `$geoWithin` - Find documents within a shape
- `$geoIntersects` - Find geometries that intersect
- `$near` - Find and sort by distance (requires geo index)
- `$nearSphere` - Find and sort by spherical distance (requires geo index)

Shape specifiers supported:
- `$geometry` - GeoJSON geometry
- `$box` - Rectangular box (2d)
- `$center` - Circle with flat distance (2d)
- `$centerSphere` - Circle with spherical distance
- `$polygon` - Legacy polygon (2d)

**Note**: All geometry calculations implemented from scratch (no external libraries).

### Bitwise Operators ✅ Complete

All bitwise query operators are fully implemented:
- `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`

Supports position arrays and numeric bitmasks. Handles negative numbers with two's complement.

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
| `$comment` | ✅ | Query comments (no-op, for logging/profiling) |
| `$rand` | ✅ | Available as aggregation expression in `$expr` |

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
| `$pullAll` | ✅ | Remove all matching values |

### Positional Update Operators ✅ Complete

All positional update operators are fully implemented:

| Operator | Description |
|----------|-------------|
| `$` | Update first matching array element |
| `$[]` | Update all array elements |
| `$[<identifier>]` | Update elements matching `arrayFilters` |

Supports all update operators (`$set`, `$inc`, `$mul`, `$unset`, `$min`, `$max`) with positional paths.

### Array Update Modifiers ✅ Complete

| Modifier | Status | Notes |
|----------|--------|-------|
| `$each` | ✅ | Works with `$push` and `$addToSet` |
| `$position` | ✅ | Insert at specific array index (negative counts from end) |
| `$slice` | ✅ | Limit array size after push (positive/negative/zero) |
| `$sort` | ✅ | Sort array after push (ascending/descending, by field) |

### Bitwise Update Operator ✅ Implemented

| Operator | Status | Notes |
|----------|--------|-------|
| `$bit` | ✅ | Supports `and`, `or`, `xor` operations |

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
| `$group` | 12 accumulator operators |
| `$lookup` | Basic form (localField/foreignField) |
| `$addFields` | Add/modify fields |
| `$set` | Alias for `$addFields` |
| `$replaceRoot` | Replace document root |
| `$replaceWith` | Alias for `$replaceRoot` |
| `$out` | Write to collection (must be final) |
| `$sortByCount` | Group and count by expression |
| `$sample` | Random sampling |
| `$facet` | Multiple sub-pipelines |
| `$bucket` | Group into buckets |
| `$bucketAuto` | Auto-create buckets |
| `$unionWith` | Union collections |
| `$redact` | Field-level access control with `$$DESCEND`, `$$PRUNE`, `$$KEEP` |
| `$graphLookup` | Recursive graph traversal with `maxDepth`, `depthField`, `restrictSearchWithMatch` |
| `$documents` | Inject literal documents (must be first stage) |
| `$unset` | Remove fields from documents |
| `$densify` | Fill gaps in numeric/date sequences with partitioning |
| `$fill` | Fill null values with `value`, `locf`, or `linear` methods |
| `$setWindowFields` | Window functions (see below) |
| `$geoNear` | Geospatial distance query (must be first stage) |

### $setWindowFields Operators

The `$setWindowFields` stage supports the following window operators:

| Operator | Status | Notes |
|----------|--------|-------|
| `$documentNumber` | ✅ | Sequential position |
| `$rank` | ✅ | Rank with gaps for ties |
| `$denseRank` | ✅ | Rank without gaps |
| `$sum` | ✅ | Over window |
| `$avg` | ✅ | Over window |
| `$min` | ✅ | Over window (numbers, strings, dates) |
| `$max` | ✅ | Over window (numbers, strings, dates) |
| `$count` | ✅ | Over window |
| `$first` | ✅ | First value in window |
| `$last` | ✅ | Last value in window |
| `$push` | ✅ | Collect values in window |
| `$addToSet` | ✅ | Collect unique values in window |
| `$locf` | ✅ | Last observation carried forward |
| `$linearFill` | ✅ | Linear interpolation |
| `$shift` | ✅ | Access value at relative offset |
| `$derivative` | ✅ | Rate of change |
| `$integral` | ✅ | Area under curve (trapezoidal) |
| `$expMovingAvg` | ✅ | Exponential moving average |
| `$covariancePop` | ✅ | Population covariance |
| `$covarianceSamp` | ✅ | Sample covariance |
| `$stdDevPop` | ✅ | Population standard deviation |
| `$stdDevSamp` | ✅ | Sample standard deviation |

**Window bounds**: Both `documents` and `range` bounds are supported with `"unbounded"`, `"current"`, and integer offsets.

**Known limitation**: `"full"` bounds option in `$densify` uses partition bounds instead of global bounds.

### Not Implemented Stages ❌

| Stage | Description | Reason |
|-------|-------------|--------|
| `$changeStream` | Real-time changes | Requires change streams |
| `$collStats` | Collection statistics | Use `collection.stats()` instead |
| `$indexStats` | Index usage stats | Not implemented |
| `$listSessions` | Active sessions | No session support |
| `$merge` | Merge into collection | Not implemented |
| `$planCacheStats` | Query plan stats | No query planner |
| `$search` | Atlas full-text search | Atlas-only feature |
| `$searchMeta` | Atlas search metadata | Atlas-only feature |

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

### Arithmetic Operators ✅ Complete (17/17)

All arithmetic expression operators are implemented:
- `$abs`, `$add`, `$ceil`, `$divide`, `$exp`, `$floor`, `$ln`, `$log`, `$log10`
- `$mod`, `$multiply`, `$pow`, `$rand`, `$round`, `$sqrt`, `$subtract`, `$trunc`

### Array Operators ✅ Nearly Complete (18/24)

All commonly used array operators are now implemented:
- `$arrayElemAt`, `$arrayToObject`, `$concatArrays`, `$filter`, `$first`, `$in`, `$indexOfArray`
- `$isArray`, `$last`, `$map`, `$objectToArray`, `$range`, `$reduce`, `$reverseArray`
- `$size`, `$slice`, `$sortArray`, `$zip`

### String Operators ✅ Complete (20/20)

All string operators are now implemented:
- `$concat`, `$indexOfBytes`, `$indexOfCP`, `$ltrim`, `$regexFind`, `$regexFindAll`, `$regexMatch`
- `$replaceAll`, `$replaceOne`, `$rtrim`, `$split`, `$strcasecmp`, `$strLenBytes`, `$strLenCP`
- `$substrBytes`, `$substrCP`, `$toLower`, `$toString`, `$toUpper`, `$trim`

### Date Operators ✅ Complete (20/20)

All date operators are now implemented:
- `$dateAdd`, `$dateDiff`, `$dateFromParts`, `$dateFromString`, `$dateToParts`, `$dateSubtract`
- `$dateToString`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$hour`, `$isoDayOfWeek`
- `$isoWeek`, `$isoWeekYear`, `$millisecond`, `$minute`, `$month`, `$second`, `$week`, `$year`

### Comparison Operators ✅ Complete (7/7)

All comparison expression operators are implemented:
- `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$cmp`

### Conditional Operators ✅ Complete (3/3)

All conditional expression operators are implemented:
- `$cond`, `$ifNull`, `$switch`

### Type Operators ✅ Nearly Complete (10/11)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$toBool`, `$toDate`, `$toDouble`, `$toInt`, `$type`, `$convert`, `$isNumber`, `$toDecimal`, `$toLong`, `$toObjectId` | `$accumulator` (custom JS) |

### Accumulator Operators ✅ Nearly Complete (12/13)

| Implemented | Not Implemented |
|-------------|-----------------|
| `$addToSet`, `$avg`, `$first`, `$last`, `$max`, `$min`, `$push`, `$sum`, `$count`, `$mergeObjects`, `$stdDevPop`, `$stdDevSamp` | `$accumulator` (custom JS) |

---

## Index Types

### Implemented Index Types ✅

| Type | Options Supported |
|------|-------------------|
| Single Field | `unique`, `sparse`, `name`, `hidden`, `collation` |
| Compound | `unique`, `sparse`, `name`, `hidden`, `collation` |
| Text | `weights`, `default_language` (basic tokenized search) |
| TTL | `expireAfterSeconds` (single field only) |
| Partial | `partialFilterExpression` |
| 2d | Flat/planar geospatial index |
| 2dsphere | Spherical geospatial index |
| Hashed | Hash-based index (cannot be unique, no array values) |
| Wildcard | Dynamic field indexing (`$**`, `wildcardProjection`) |

### Index Options ✅

| Option | Status | Notes |
|--------|--------|-------|
| `unique` | ✅ | Enforces unique values (not on hashed/wildcard) |
| `sparse` | ✅ | Only index documents with the field |
| `name` | ✅ | Custom index name |
| `hidden` | ✅ | Hide from query planner (constraints still enforced) |
| `collation` | ✅ | Locale-aware comparison (metadata stored) |
| `weights` | ✅ | Text index field weights (1-99999) |
| `default_language` | ✅ | Text index language (metadata stored) |
| `wildcardProjection` | ✅ | Include/exclude fields for wildcard indexes |
| `expireAfterSeconds` | ✅ | TTL index expiration |
| `partialFilterExpression` | ✅ | Partial index filter |

**Note**: MangoDB does not use indexes for query optimization - they are for constraint enforcement and API compatibility.

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
| **Collation** | Locale-aware query operations | Index collation metadata stored |

---

## Collection Methods

### Implemented ✅

All core CRUD methods:
- `insertOne`, `insertMany`
- `find`, `findOne`
- `updateOne`, `updateMany`, `replaceOne`
- `deleteOne`, `deleteMany`
- `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`
- `bulkWrite` (ordered and unordered)
- `aggregate`
- `countDocuments`, `estimatedDocumentCount`, `distinct`
- `createIndex`, `createIndexes`, `dropIndex`, `dropIndexes`, `indexes`, `listIndexes`
- `drop`, `rename`, `stats`

### Not Implemented ❌

| Method | Workaround |
|--------|------------|
| `watch` | Not available - poll with `find()` |
| `initializeOrderedBulkOp` | Use `bulkWrite()` with `ordered: true` |
| `initializeUnorderedBulkOp` | Use `bulkWrite()` with `ordered: false` |

### Method Options Not Supported ❌

These options are not supported on any method:

| Option | Description |
|--------|-------------|
| ~~`arrayFilters`~~ | ✅ Now supported for positional updates |
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

### JSON Storage Limitations

These differences arise from using JSON instead of BSON for storage:

| Aspect | MongoDB | MangoDB |
|--------|---------|---------|
| **`undefined` values** | Stored as explicit undefined | Stripped during serialization (treated as missing) |
| **`NaN`** | Stored as NaN | Stored as `null` |
| **`Infinity`** | Stored as Infinity | Stored as `null` |
| **`-Infinity`** | Stored as -Infinity | Stored as `null` |
| **Binary data** | Native BinData type | Base64 encoded strings |
| **Decimal128** | Native decimal type | Converted to JavaScript number |

**Impact**: Code relying on `undefined` field existence, NaN/Infinity comparisons, or high-precision decimals may behave differently.

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

---

## Feature Requests

If you need a feature that's not implemented, please:
1. Check if there's a workaround listed above
2. Open an issue at the project repository
3. Consider contributing a PR

For features marked as ⛔ (Not Applicable), these are architectural decisions and unlikely to change.
