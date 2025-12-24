# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-12-24

### Added

- Initial release of MangoDB
- `MangoDBClient` for file-based MongoDB-compatible storage
- `MangoDBDb` for database operations
- `MangoDBCollection` with CRUD operations:
  - `insertOne`, `insertMany`
  - `findOne`, `find` with cursor support
  - `updateOne`, `updateMany`
  - `deleteOne`, `deleteMany`
  - `replaceOne`
  - `countDocuments`, `estimatedDocumentCount`
  - `distinct`
- Query operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$and`, `$or`, `$not`, `$nor`, `$exists`, `$type`, `$regex`, `$elemMatch`, `$all`, `$size`, `$mod`, `$text`
- Update operators: `$set`, `$unset`, `$inc`, `$push`, `$pull`, `$addToSet`, `$pop`, `$rename`, `$min`, `$max`, `$mul`, `$currentDate`
- Aggregation pipeline support with stages: `$match`, `$project`, `$sort`, `$limit`, `$skip`, `$count`, `$unwind`, `$group`, `$lookup`, `$addFields`, `$set`, `$unset`, `$replaceRoot`, `$replaceWith`, `$sample`, `$sortByCount`, `$facet`
- Index support: single-field, compound, unique, sparse, text indexes
- Cursor operations: `toArray`, `forEach`, `map`, `limit`, `skip`, `sort`, `project`
- Full TypeScript support with type definitions
