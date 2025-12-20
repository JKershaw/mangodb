# Mongone Roadmap

This document outlines the implementation phases for Mongone. Each phase builds on the previous and includes specific MongoDB operations to implement.

## Current Phase: Phase 1 - Foundation

---

## Phase 1: Foundation (Current)

**Goal**: Establish core abstractions and basic CRUD operations.

### Operations
- [x] `MongoneClient` - Client abstraction matching MongoClient interface
- [x] `client.connect()` / `client.close()`
- [x] `client.db(name)` - Database access
- [x] `db.collection(name)` - Collection access
- [x] `collection.insertOne(doc)`
- [x] `collection.insertMany(docs)`
- [x] `collection.findOne(filter)` - with empty filter
- [x] `collection.find(filter)` - with empty filter, returns cursor
- [x] `cursor.toArray()`
- [x] `collection.deleteOne(filter)` - simple equality
- [x] `collection.deleteMany(filter)` - simple equality

### Storage
- JSON file per collection
- Simple file-based persistence
- Data directory structure: `{dataDir}/{dbName}/{collectionName}.json`

### Infrastructure
- [x] Dual-target test harness
- [x] GitHub Actions CI

---

## Phase 2: Basic Queries

**Goal**: Support common query operators for filtering documents.

### Operations
- [ ] Equality matching (`{field: value}`)
- [ ] Dot notation for nested fields (`{"a.b.c": value}`)
- [ ] `$eq` - Explicit equality
- [ ] `$ne` - Not equal
- [ ] `$gt` - Greater than
- [ ] `$gte` - Greater than or equal
- [ ] `$lt` - Less than
- [ ] `$lte` - Less than or equal
- [ ] `$in` - Match any value in array
- [ ] `$nin` - Match none of values in array

### Considerations
- Type coercion behavior (or lack thereof)
- null vs undefined vs missing field handling
- Date comparison

---

## Phase 3: Updates

**Goal**: Support document updates with common operators.

### Operations
- [ ] `collection.updateOne(filter, update)`
- [ ] `collection.updateMany(filter, update)`
- [ ] `$set` - Set field values
- [ ] `$unset` - Remove fields
- [ ] `$inc` - Increment numeric values
- [ ] `upsert` option - Insert if not found

### Considerations
- Dot notation in updates creates nested structure
- Update operators can be combined
- Return values (matchedCount, modifiedCount, upsertedId)

---

## Phase 4: Cursor Operations

**Goal**: Support result manipulation and projection.

### Operations
- [ ] `cursor.sort(spec)` - Single field
- [ ] `cursor.sort(spec)` - Compound/multiple fields
- [ ] `cursor.limit(n)`
- [ ] `cursor.skip(n)`
- [ ] Projection (field inclusion)
- [ ] Projection (field exclusion)
- [ ] `collection.countDocuments(filter)`

### Considerations
- Sort order for mixed types (MongoDB has specific rules)
- Chaining cursor methods
- Projection cannot mix inclusion/exclusion (except _id)

---

## Phase 5: Logical Operators

**Goal**: Support complex query logic.

### Operations
- [ ] `$and` - Logical AND
- [ ] `$or` - Logical OR
- [ ] `$not` - Logical NOT
- [ ] `$nor` - Logical NOR
- [ ] `$exists` - Field existence

### Considerations
- Implicit $and in query objects
- Operator precedence
- Combining with comparison operators

---

## Phase 6: Array Handling

**Goal**: Support querying and modifying array fields.

### Query Operations
- [ ] Array element matching (`{tags: "red"}` matches `{tags: ["red", "blue"]}`)
- [ ] `$elemMatch` - Match array element with multiple conditions
- [ ] `$size` - Match array by length
- [ ] `$all` - Match arrays containing all specified elements

### Update Operations
- [ ] `$push` - Add element to array
- [ ] `$pull` - Remove elements matching condition
- [ ] `$addToSet` - Add element if not present
- [ ] `$pop` - Remove first or last element

---

## Phase 7: Indexes

**Goal**: Support index creation and optimization.

### Operations
- [ ] `collection.createIndex(spec)`
- [ ] `collection.dropIndex(name)`
- [ ] `collection.indexes()`
- [ ] Unique index constraint

### Implementation
- Index metadata stored alongside collection data
- Use indexes to speed up queries on indexed fields
- Enforce uniqueness on write operations

---

## Phase 8: Advanced (As Needed)

**Goal**: Support additional commonly-used operations.

### Operations
- [ ] `collection.findOneAndUpdate(filter, update, options)`
- [ ] `collection.findOneAndDelete(filter, options)`
- [ ] `collection.findOneAndReplace(filter, replacement, options)`
- [ ] `collection.bulkWrite(operations)`

### Basic Aggregation Pipeline
- [ ] `collection.aggregate(pipeline)`
- [ ] `$match` stage
- [ ] `$project` stage
- [ ] `$sort` stage
- [ ] `$limit` stage
- [ ] `$skip` stage
- [ ] `$count` stage

---

## Design Principles

1. **Test-Driven**: Write tests against real MongoDB first, then implement in Mongone
2. **Correctness Over Performance**: Get it right before making it fast
3. **Minimal API Surface**: Only implement what's tested and needed
4. **Document Discoveries**: Note unexpected MongoDB behaviors in COMPATIBILITY.md
