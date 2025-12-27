# Compatibility Matrix

MangoDB aims to be a drop-in replacement for MongoDB during development and testing. This document details what's supported, what's not, and known behavioral differences.

## Quick Reference

| Category | Coverage | Notes |
|----------|----------|-------|
| Query Operators | 31/32 | Missing `$where` |
| Update Operators | 20/20 | Full coverage including positional |
| Aggregation Stages | 28/34 | Core stages + window functions |
| Expression Operators | 121/127 | Nearly complete |
| Index Types | 9/9 | All types supported |
| Collection Methods | 24/27 | Missing `watch`, bulk op builders |

---

## Query Operators

### Comparison

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$eq` | Yes | |
| `$ne` | Yes | |
| `$gt` | Yes | |
| `$gte` | Yes | |
| `$lt` | Yes | |
| `$lte` | Yes | |
| `$in` | Yes | |
| `$nin` | Yes | |

### Logical

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$and` | Yes | |
| `$or` | Yes | |
| `$not` | Yes | |
| `$nor` | Yes | |

### Element

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$exists` | Yes | |
| `$type` | Yes | Full BSON type support |

### Array

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$all` | Yes | |
| `$elemMatch` | Yes | |
| `$size` | Yes | |

### Evaluation

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$expr` | Yes | Aggregation expressions in queries |
| `$mod` | Yes | |
| `$regex` | Yes | With `$options` support |
| `$text` | Yes | Requires text index |
| `$comment` | Yes | No-op, for logging/profiling |
| `$jsonSchema` | Yes | JSON Schema draft 4 with BSON extensions |
| `$where` | No | JavaScript execution not supported |

### Geospatial

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$geoWithin` | Yes | All shape specifiers |
| `$geoIntersects` | Yes | |
| `$near` | Yes | Requires geo index |
| `$nearSphere` | Yes | Requires geo index |

### Bitwise

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$bitsAllClear` | Yes | |
| `$bitsAllSet` | Yes | |
| `$bitsAnyClear` | Yes | |
| `$bitsAnySet` | Yes | |

---

## Update Operators

### Field Updates

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$set` | Yes | |
| `$unset` | Yes | |
| `$inc` | Yes | |
| `$mul` | Yes | |
| `$min` | Yes | |
| `$max` | Yes | |
| `$rename` | Yes | |
| `$currentDate` | Yes | |
| `$setOnInsert` | Yes | |

### Array Updates

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$push` | Yes | With `$each`, `$position`, `$slice`, `$sort` |
| `$pull` | Yes | With query conditions |
| `$addToSet` | Yes | With `$each` |
| `$pop` | Yes | |
| `$pullAll` | Yes | |

### Positional Updates

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$` | Yes | First matching element |
| `$[]` | Yes | All elements |
| `$[<identifier>]` | Yes | With `arrayFilters` |

### Bitwise Updates

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$bit` | Yes | `and`, `or`, `xor` |

---

## Aggregation Stages

### Supported Stages

| Stage | Notes |
|-------|-------|
| `$addFields` | |
| `$bucket` | |
| `$bucketAuto` | |
| `$count` | |
| `$densify` | Fill gaps in sequences |
| `$documents` | Must be first stage |
| `$facet` | Multiple sub-pipelines |
| `$fill` | `value`, `locf`, `linear` methods |
| `$geoNear` | Must be first stage |
| `$graphLookup` | Recursive traversal |
| `$group` | 12 accumulator operators |
| `$limit` | |
| `$lookup` | Basic form only |
| `$match` | |
| `$out` | Must be final stage |
| `$project` | |
| `$redact` | `$$DESCEND`, `$$PRUNE`, `$$KEEP` |
| `$replaceRoot` | |
| `$replaceWith` | |
| `$sample` | |
| `$set` | Alias for `$addFields` |
| `$setWindowFields` | 22 window operators |
| `$skip` | |
| `$sort` | |
| `$sortByCount` | |
| `$unionWith` | |
| `$unset` | |
| `$unwind` | With `preserveNullAndEmptyArrays` |

### Not Supported Stages

| Stage | Reason |
|-------|--------|
| `$changeStream` | Requires change streams |
| `$collStats` | Use `collection.stats()` |
| `$indexStats` | No query planner |
| `$listSessions` | No session support |
| `$merge` | Not implemented |
| `$planCacheStats` | No query planner |
| `$search` | Atlas-only feature |
| `$searchMeta` | Atlas-only feature |

### $lookup Limitation

Only basic form is supported:

```javascript
// Supported
{ $lookup: { from: "coll", localField: "a", foreignField: "b", as: "results" } }

// NOT supported - pipeline form
{ $lookup: { from: "coll", let: { x: "$a" }, pipeline: [...], as: "results" } }
```

---

## Expression Operators

Expression operators are used in aggregation pipelines. See [API.md](./API.md) for syntax examples.

### Categories

| Category | Count | Notes |
|----------|-------|-------|
| Arithmetic | 17 | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`, `$trunc`, `$sqrt`, `$pow`, `$exp`, `$ln`, `$log`, `$log10`, `$rand` |
| String | 19 | `$concat`, `$toLower`, `$toUpper`, `$trim`, `$split`, `$substrCP`, `$regexMatch`, etc. |
| Array | 22 | `$arrayElemAt`, `$first`, `$last`, `$size`, `$filter`, `$map`, `$reduce`, etc. |
| Set | 7 | `$setUnion`, `$setIntersection`, `$setDifference`, `$setEquals`, `$setIsSubset`, `$allElementsTrue`, `$anyElementTrue` |
| Date | 19 | `$year`, `$month`, `$dayOfMonth`, `$dateAdd`, `$dateDiff`, `$dateToString`, etc. |
| Comparison | 7 | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$cmp` |
| Conditional | 4 | `$cond`, `$ifNull`, `$switch`, `$let` |
| Type | 10 | `$type`, `$toBool`, `$toDate`, `$toDouble`, `$toInt`, `$convert`, etc. |
| Boolean | 3 | `$and`, `$or`, `$not` |
| Object | 3 | `$getField`, `$setField`, `$mergeObjects` |
| Trigonometry | 15 | See below |

### Trigonometry Operators

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$sin` | Yes | Sine (radians) |
| `$cos` | Yes | Cosine (radians) |
| `$tan` | Yes | Tangent (radians) |
| `$asin` | Yes | Arc sine |
| `$acos` | Yes | Arc cosine |
| `$atan` | Yes | Arc tangent |
| `$atan2` | Yes | Arc tangent of y/x (2 args) |
| `$sinh` | Yes | Hyperbolic sine |
| `$cosh` | Yes | Hyperbolic cosine |
| `$tanh` | Yes | Hyperbolic tangent |
| `$asinh` | Yes | Inverse hyperbolic sine |
| `$acosh` | Yes | Inverse hyperbolic cosine |
| `$atanh` | Yes | Inverse hyperbolic tangent |
| `$degreesToRadians` | Yes | Degrees to radians |
| `$radiansToDegrees` | Yes | Radians to degrees |

### Not Supported

| Operator | Reason |
|----------|--------|
| `$accumulator` | JavaScript execution |
| `$function` | JavaScript execution |
| `$getField` (with $$) | Limited variable support |
| `$literal` | Not implemented |
| `$meta` | No text search scoring |
| `$sampleRate` | Not implemented |

---

## Index Types

| Type | Supported | Options |
|------|-----------|---------|
| Single Field | Yes | `unique`, `sparse`, `name`, `hidden` |
| Compound | Yes | Same as single field |
| Text | Yes | `weights`, `default_language` |
| TTL | Yes | `expireAfterSeconds` |
| Partial | Yes | `partialFilterExpression` |
| 2d | Yes | Flat/planar geometry |
| 2dsphere | Yes | Spherical geometry |
| Hashed | Yes | Cannot be unique |
| Wildcard | Yes | `wildcardProjection` |

**Note**: Indexes enforce constraints but don't optimize queries. MangoDB performs full collection scans.

---

## Collection Methods

### Fully Supported

- `insertOne`, `insertMany`
- `find`, `findOne`
- `updateOne`, `updateMany`, `replaceOne`
- `deleteOne`, `deleteMany`
- `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`
- `bulkWrite` (ordered and unordered)
- `aggregate`
- `countDocuments`, `estimatedDocumentCount`, `distinct`
- `createIndex`, `createIndexes`, `dropIndex`, `dropIndexes`
- `indexes`, `listIndexes`
- `drop`, `rename`, `stats`

### Not Supported

| Method | Alternative |
|--------|-------------|
| `watch` | Poll with `find()` |
| `initializeOrderedBulkOp` | Use `bulkWrite({ ordered: true })` |
| `initializeUnorderedBulkOp` | Use `bulkWrite({ ordered: false })` |

---

## Architectural Differences

These are fundamental differences from MongoDB's architecture:

| Aspect | MongoDB | MangoDB |
|--------|---------|---------|
| Storage | BSON binary format | JSON text files |
| Query execution | Index-optimized | Full collection scan |
| Concurrency | Multi-process safe | Single process only |
| Atomicity | Multi-document transactions | Single-document only |
| Memory | Streams large results | Loads all documents |
| Authentication | Built-in auth | Filesystem permissions |

---

## Not Applicable Features

These features don't apply to file-based storage:

- **Transactions** - No ACID guarantees
- **Sessions** - No multi-operation tracking
- **Connection pooling** - Synchronous file access
- **Read/write concerns** - Single-node, synchronous
- **Replica sets** - Single node only
- **Sharding** - Single node only
- **Change streams** - No real-time notifications
- **GridFS** - Use filesystem directly

---

## Recommended Use Cases

**Good fit:**
- Local development without MongoDB setup
- Unit and integration testing
- CI/CD pipelines
- Prototyping and learning
- Small datasets (< 10,000 documents per collection)

**Not recommended:**
- Production deployments
- Large datasets
- Multi-process access
- High-throughput workloads
- Applications requiring transactions
