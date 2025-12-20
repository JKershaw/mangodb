# Mongone Progress

This document tracks implementation progress and notable discoveries.

## Current Status

**Phase**: 1 - Foundation
**Status**: Complete

---

## Changelog

### 2024-12-20 - Initial Implementation

#### Added
- Project structure with TypeScript configuration
- `MongoneClient` class with `connect()` and `close()` methods
- `MongoneDb` class with `collection()` method
- `MongoneCollection` class with basic CRUD operations:
  - `insertOne(doc)` - Insert single document
  - `insertMany(docs)` - Insert multiple documents
  - `findOne(filter)` - Find single document
  - `find(filter)` - Find documents, returns cursor
  - `deleteOne(filter)` - Delete single document
  - `deleteMany(filter)` - Delete multiple documents
- `MongoneCursor` class with `toArray()` method
- File-based storage (JSON per collection)
- ObjectId generation using MongoDB's BSON library
- Dual-target test infrastructure
- GitHub Actions CI workflow

#### Storage Format
- Data stored in `{dataDir}/{dbName}/{collectionName}.json`
- Each collection file contains array of documents
- Documents serialized as JSON with ObjectId handling

---

## MongoDB Behaviors Discovered

See [COMPATIBILITY.md](./COMPATIBILITY.md) for detailed documentation of MongoDB behaviors discovered through testing.

### Summary
- ObjectId must be handled specially for JSON serialization
- `insertOne` returns `{ acknowledged: true, insertedId: ObjectId }`
- `insertMany` returns `{ acknowledged: true, insertedIds: { 0: ObjectId, 1: ObjectId, ... } }`
- `deleteOne` returns `{ acknowledged: true, deletedCount: 0 | 1 }`
- `deleteMany` returns `{ acknowledged: true, deletedCount: number }`

---

## Known Limitations

Current implementation has these intentional limitations:

1. **No indexing** - All queries scan full collection
2. **No query operators** - Only empty filter and exact equality supported
3. **No update operations** - Coming in Phase 3
4. **No cursor operations** - No sort/limit/skip, coming in Phase 4
5. **No projection** - Returns full documents only
6. **Single-threaded** - No concurrent write protection

These will be addressed in future phases as documented in [ROADMAP.md](./ROADMAP.md).
