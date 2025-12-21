# MangoDB Rename Plan

## Overview
Rename the project from "Mongone" to "MangoDB" across the entire codebase.

## Naming Conventions
- Package name: `mongone` → `mangodb`
- Class prefix: `Mongone*` → `MangoDB*`
  - `MongoneClient` → `MangoDBClient`
  - `MongoneDb` → `MangoDBDb`
  - `MongoneCollection` → `MangoDBCollection`
  - `MongoneCursor` → `MangoDBCursor`
- Directory references: `.mongone-data` → `.mangodb-data`
- Temp directories: `/tmp/mongone_test` → `/tmp/mangodb_test`
- Text/branding: "Mongone" → "MangoDB"

## DO NOT CHANGE
- MongoDB driver imports (`import { MongoClient } from 'mongodb'`)
- MongoDB package dependency in package.json
- Technical references to MongoDB behavior/compatibility
- References to the actual MongoDB database service

---

## Phase 1: Configuration Files

### 1.1 package.json
- Change `"name": "mongone"` → `"name": "mangodb"`
- Update description: "SQLite is to SQL as Mongone is to MongoDB" → "SQLite is to SQL as MangoDB is to MongoDB"

### 1.2 package-lock.json
- Update all `"name": "mongone"` references to `"name": "mangodb"`

### 1.3 .gitignore
- Change `.mongone-data/` → `.mangodb-data/`

### 1.4 .github/workflows/ci.yml
- Change job name `test-mongone:` → `test-mangodb:`
- Update step names with "Mongone" → "MangoDB"

---

## Phase 2: Source Code (src/)

### 2.1 src/index.ts
- Update all export class names

### 2.2 src/client.ts
- Rename class `MongoneClient` → `MangoDBClient`
- Update imports and comments

### 2.3 src/db.ts
- Rename class `MongoneDb` → `MangoDBDb`
- Update imports and comments

### 2.4 src/collection.ts
- Rename class `MongoneCollection` → `MangoDBCollection`
- Update imports and comments

### 2.5 src/cursor.ts
- Rename class `MongoneCursor` → `MangoDBCursor`
- Update comments

### 2.6 src/types.ts
- Update comments referencing "Mongone"

### 2.7 src/errors.ts
- Update comments referencing "Mongone"

### 2.8 src/index-manager.ts
- Update comments referencing "Mongone"

### 2.9 src/aggregation.ts
- Update comments referencing "Mongone"

### 2.10 Other src files
- Check and update any remaining "Mongone" references in:
  - document-utils.ts
  - query-matcher.ts
  - update-operators.ts
  - utils.ts

---

## Phase 3: Test Files (test/)

### 3.1 test/test-harness.ts
- Update imports: `MongoneClient` → `MangoDBClient`
- Change db name prefix: `mongone_test_` → `mangodb_test_`
- Change data dir: `/tmp/mongone_test_` → `/tmp/mangodb_test_`
- Update mode label: `"Mongone"` → `"MangoDB"`

### 3.2 All test files
Update imports and comments in:
- foundation.test.ts
- queries.test.ts
- logical.test.ts
- updates.test.ts
- arrays.test.ts
- indexes.test.ts
- advanced.test.ts
- aggregation.test.ts
- cursor.test.ts

---

## Phase 4: Documentation

### 4.1 README.md
- Update title and tagline
- Update all code examples
- Update all "Mongone" text references

### 4.2 PROGRESS.md
- Update title and all "Mongone" references
- Update class name references

### 4.3 ROADMAP.md
- Update title and all "Mongone" references

### 4.4 ROADMAP_REMAINING.md
- Update all "Mongone" references

### 4.5 prompt.md
- Update project description and all references

### 4.6 PHASE*.md files
- Update all "Mongone" references in PHASE6_PLAN.md, PHASE7_PLAN.md, PHASE8_PLAN.md, PHASE9_PLAN.md

---

## Phase 5: Verification

1. Run `npm run build` to verify TypeScript compilation
2. Run `npm test` to verify tests pass
3. Search for any remaining "Mongone" references (case-insensitive)
4. Commit and push changes

---

## Estimated Changes
- **32 files** total
- Configuration: 4 files
- Source code: 13 files
- Tests: 10 files
- Documentation: 8+ files
