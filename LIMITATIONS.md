# MangoDB Limitations & Coverage

This document outlines the MongoDB API coverage and known limitations of MangoDB.

## Coverage Summary

| Category | Implemented | Coverage |
|----------|-------------|----------|
| Query Operators | 37 | ~95% |
| Update Operators | 20 | 100% |
| Aggregation Stages | 30 | ~88% |
| Expression Operators | 90+ | ~95% |

## Query Operators

### Comparison Operators (All Implemented)
- `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`

### Logical Operators (All Implemented)
- `$and`, `$or`, `$not`, `$nor`

### Element Operators (All Implemented)
- `$exists`, `$type`

### Array Operators (All Implemented)
- `$elemMatch`, `$size`, `$all`

### Evaluation Operators (Partial)
- Implemented: `$regex`, `$expr`, `$mod`, `$text` (basic full-text search)
- Not implemented: `$jsonSchema`, `$where`

### Bitwise Operators (All Implemented)
- `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`

### Geospatial Operators (All Implemented)
- `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`

### Projection Operators (All Implemented)
- `$slice`, `$elemMatch`, `$` (positional), `$meta` (textScore)

## Update Operators

### Field Update Operators (All Implemented)
- `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$setOnInsert`, `$currentDate`

### Array Update Operators (All Implemented)
- `$push`, `$pull`, `$pop`, `$addToSet`, `$pullAll`
- Array modifiers: `$each`, `$slice`, `$sort`, `$position`

### Positional Update Operators (All Implemented)
- `$` (first match), `$[]` (all elements), `$[<identifier>]` (filtered)
- `arrayFilters` option supported

## Aggregation Stages

### Implemented Stages (30)
- `$match`, `$project`, `$group`, `$sort`, `$limit`, `$skip`
- `$unwind`, `$lookup`, `$graphLookup`
- `$addFields`, `$set`, `$unset`
- `$replaceRoot`, `$replaceWith`
- `$count`, `$sortByCount`
- `$facet`, `$bucket`, `$bucketAuto`
- `$out`, `$merge`
- `$sample`, `$redact`
- `$geoNear`
- `$setWindowFields` (with window functions)
- `$fill`, `$densify`
- `$unionWith`
- `$documents`

### Pipeline $lookup
- Simple form: `localField`/`foreignField`
- Pipeline form: `let`/`pipeline` with `$$variable` substitution
- Supports dotted variable paths (`$$varName.field`)

### $merge Options
- `into`: collection name (cross-database not supported)
- `on`: custom match field(s)
- `whenMatched`: `replace`, `keepExisting`, `merge`, `fail`, or pipeline with `$$new`
- `whenNotMatched`: `insert`, `discard`, `fail`

### Not Implemented Stages
- `$search`, `$searchMeta` (Atlas Search)
- `$listSessions`, `$listLocalSessions`
- `$currentOp`, `$collStats`, `$indexStats`

## Expression Operators

### Arithmetic (All Implemented)
- `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`
- `$abs`, `$ceil`, `$floor`, `$round`, `$trunc`
- `$exp`, `$ln`, `$log`, `$log10`, `$pow`, `$sqrt`

### String (All Implemented)
- `$concat`, `$substr`, `$toLower`, `$toUpper`
- `$trim`, `$ltrim`, `$rtrim`, `$split`
- `$strLenCP`, `$strLenBytes`, `$indexOfCP`, `$indexOfBytes`
- `$regexFind`, `$regexFindAll`, `$regexMatch`
- `$replaceOne`, `$replaceAll`

### Array (All Implemented)
- `$arrayElemAt`, `$first`, `$last`, `$slice`
- `$size`, `$isArray`, `$in`
- `$map`, `$filter`, `$reduce`
- `$concatArrays`, `$reverseArray`, `$sortArray`
- `$range`, `$zip`, `$indexOfArray`
- `$arrayToObject`, `$objectToArray`

### Set Operators (All Implemented)
- `$setUnion`, `$setIntersection`, `$setDifference`
- `$setEquals`, `$setIsSubset`
- `$allElementsTrue`, `$anyElementTrue`

### Comparison (All Implemented)
- `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$cmp`

### Boolean/Logical (All Implemented)
- `$and`, `$or`, `$not`

### Conditional (All Implemented)
- `$cond`, `$ifNull`, `$switch`

### Date (All Implemented)
- `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$millisecond`
- `$dayOfWeek`, `$dayOfYear`, `$week`, `$isoWeek`, `$isoWeekYear`, `$isoDayOfWeek`
- `$dateToString`, `$dateFromString`, `$dateFromParts`, `$dateToParts`
- `$dateAdd`, `$dateSubtract`, `$dateDiff`

### Type Conversion (All Implemented)
- `$toString`, `$toInt`, `$toLong`, `$toDouble`, `$toDecimal`
- `$toBool`, `$toDate`, `$toObjectId`
- `$convert`, `$type`, `$isNumber`

### Window Functions (All Implemented)
- `$sum`, `$avg`, `$min`, `$max`, `$count`
- `$first`, `$last`, `$push`
- `$stdDevPop`, `$stdDevSamp`
- `$derivative`, `$integral`
- `$rank`, `$denseRank`, `$documentNumber`
- `$shift`, `$expMovingAvg`
- `$covariancePop`, `$covarianceSamp`
- `$linearFill`, `$locf`

## Index Support

### Index Types
- Single field indexes
- Compound indexes
- Unique indexes
- Sparse indexes
- Partial indexes
- TTL indexes
- Text indexes (basic)
- 2d indexes (flat geometry)
- 2dsphere indexes (spherical geometry)
- Hashed indexes
- Wildcard indexes

### Index Options
- `unique`, `sparse`, `expireAfterSeconds`
- `partialFilterExpression`
- `hidden`
- `collation` (basic support)
- `weights` (for text indexes)
- `wildcardProjection`

## Administrative Operations

### Collection Operations
- `createCollection`, `drop`, `rename`
- `createIndex`, `createIndexes`, `dropIndex`, `dropIndexes`
- `listIndexes`, `indexes`
- `stats`, `estimatedDocumentCount`, `countDocuments`
- `distinct`

### Database Operations
- `listCollections`
- `stats`
- `dropDatabase`

## Known Limitations

### General
- File-based storage (not suitable for high-concurrency production use)
- No replication or sharding
- No transactions (single-document atomicity only)
- No change streams
- No authentication/authorization

### Query
- `$where` JavaScript evaluation not implemented
- `$jsonSchema` validation not implemented

### Aggregation
- `$merge` cross-database writes not supported
- Atlas Search stages not implemented
- Some administrative stages not implemented

### Indexes
- Collation support is basic (locale-aware sorting not fully implemented)

### Text Search
- `$text` search is simplified (no stemming, no language-specific stop words)
- Supports: phrase search (`"exact phrase"`), negation (`-term`), `$caseSensitive`
- `$meta: "textScore"` supported for projection and sorting

### Performance
- All documents loaded into memory for operations
- No query planning or optimization
- No connection pooling

## Test Coverage

MangoDB is tested against both its own implementation and real MongoDB to ensure compatibility:

- **MangoDB tests**: 1283 passing
- **MongoDB tests**: 1276 passing

The test suite covers all implemented features with edge cases.
