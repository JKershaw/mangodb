# MangoDB: Project Initialization Prompt

> This document is the original prompt used to bootstrap the MangoDB project with AI assistance. It's preserved as part of our "open source with AI" philosophy, demonstrating how the project was conceived and initially structured.

## Project Overview

MangoDB is a file-based MongoDB drop-in replacement for TypeScript/Node.js. Think "SQLite is to SQL as MangoDB is to MongoDB." It allows applications to use MongoDB's API in environments with only local file access, then deploy to a real MongoDB server without code changes.

The core value proposition: develop and test locally without running MongoDB, then swap in the real database for production via a connection string environment variable.

## Technical Approach

### Dual-Target Testing Strategy

This is the most critical architectural decision. Create a single test suite that runs against both real MongoDB and MangoDB, controlled by environment variable (presence or absence of a MongoDB connection string). The tests should not know which implementation they're testing.

This approach:
- Catches subtle behavioral differences that spec-based testing would miss
- Validates MongoDB's implicit/undocumented behaviors automatically
- Ensures true drop-in compatibility
- Provides confidence when adding features

### Storage Design

- Store data as JSON files on disk
- One file per collection is fine to start
- Use simple in-memory indexes loaded on startup
- Prioritize correctness over performance initially

### API Compatibility Target

Implement a commonly-used subset of the MongoDB Node.js driver API. Users should be able to import MangoDB and use it with the same syntax as the official driver. The client should be instantiated differently (pointing to a directory instead of a connection string), but collection operations should be identical.

## Development Philosophy

### Lean/Agile TDD

- Write failing tests first using real MongoDB
- Implement minimum code to pass tests in MangoDB
- Only add functionality when tests demand it
- Resist adding features "just in case"
- Each commit should ideally be a small, working increment

### Avoid Over-Engineering

- No premature abstractions
- No configuration options until proven necessary
- No performance optimizations until correctness is complete
- Simple code that's easy to read and modify
- If debating between clever and obvious, choose obvious

### Minimal Dependencies

- Use Node.js built-in test runner (node:test)
- Use Node.js built-in filesystem APIs
- The only acceptable external dependency is the MongoDB driver (for testing against real MongoDB and potentially for BSON/ObjectId utilities)
- Avoid test frameworks, assertion libraries, build tools beyond TypeScript itself

## Implementation Order

### Phase 1: Foundation
- Client and database abstractions
- Collection access
- insertOne, insertMany
- findOne, find with empty filter
- ObjectId generation (or simple string IDs initially)
- deleteOne, deleteMany with simple equality

### Phase 2: Basic Queries
- Equality matching
- Dot notation for nested fields
- Comparison operators: $eq, $ne, $gt, $gte, $lt, $lte
- $in, $nin

### Phase 3: Updates
- updateOne, updateMany with $set
- $unset, $inc
- Upsert flag

### Phase 4: Cursor Operations
- sort (single field, then compound)
- limit, skip
- Projection (field inclusion/exclusion)
- countDocuments

### Phase 5: Logical Operators
- $and, $or, $not, $nor
- $exists

### Phase 6: Array Handling
- Array field queries (any element matching)
- $elemMatch, $size
- $push, $pull, $addToSet, $pop

### Phase 7: Indexes
- createIndex, dropIndex
- Unique constraints
- Using indexes for query optimization

### Phase 8: Advanced (as needed)
- Basic aggregation pipeline ($match, $project, $sort, $limit)
- findOneAndUpdate, findOneAndDelete
- Bulk operations

## Known Gotchas to Watch For

These are MongoDB behaviors that will likely surface through testing:

- Type ordering in sorts: MongoDB has specific ordering for mixed types
- Array query semantics: {tags: "red"} matches {tags: ["red", "blue"]}
- Null vs missing vs undefined: {field: null} matches both null values AND missing fields
- Dot notation in updates: {"a.b.c": 1} must create nested structure
- Implicit $and: {a: 1, b: 2} is implicitly {$and: [{a: 1}, {b: 2}]}
- ObjectId: 12-byte identifiers with embedded timestamp, need to handle JSON serialization

## CI/CD Setup

Configure GitHub Actions to:
- Run TypeScript compilation
- Run tests against a MongoDB service container
- Run tests in MangoDB mode (no MongoDB connection string)
- Both test runs must pass for CI to pass

## First Session Goals

1. Initialize the repository with TypeScript configuration
2. Set up the documentation files listed above with initial content
3. Create the test infrastructure with the dual-target pattern
4. Implement Phase 1 (foundation) with corresponding tests
5. Set up GitHub Actions workflow

Begin by creating the documentation and test infrastructure before writing implementation code. The test harness design is the foundation everything else builds on.
