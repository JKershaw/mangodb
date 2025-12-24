# MangoDB Implementation Roadmap

This document outlines the phased implementation plan to achieve full MongoDB API compatibility. It is designed to guide autonomous development with multiple subagents while accounting for context compaction.

---

## Strategy Overview

### Development Principles

1. **Test-Driven Development (TDD)**
   - Write failing tests first
   - Implement minimum code to pass
   - Refactor while tests pass
   - Run dual-target tests against real MongoDB to verify behavior

2. **Research-First Approach**
   - Review existing codebase patterns before implementing
   - Search online for official MongoDB documentation
   - Find official error messages and error codes
   - Identify edge cases from MongoDB docs and Stack Overflow

3. **Incremental Commits**
   - Commit after each task completion
   - Use conventional commit format: `feat:`, `fix:`, `test:`
   - Push after each phase completion

4. **Code Review Checkpoints**
   - After each phase: review all changes
   - Check for consistency with existing patterns
   - Verify error handling matches MongoDB
   - Ensure tests cover edge cases

### Subagent Strategy

Each phase can utilize parallel subagents for:
- **Research Agent**: Look up MongoDB docs, error messages, edge cases
- **Implementation Agent**: Write the actual code
- **Test Agent**: Write comprehensive tests
- **Review Agent**: Check code quality and consistency

Tasks within a phase that don't depend on each other can run in parallel.

### Context Compaction Protocol

Before starting any task:
1. Re-read this ROADMAP.md file
2. Read the relevant source file(s)
3. Read the relevant test file(s)
4. Check LIMITATIONS.md for current status

After completing each phase:
1. Update LIMITATIONS.md with new coverage
2. Update this ROADMAP.md to mark phase complete
3. Commit all changes

---

## Phase Status Tracker

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Query Operators Completion | 🔄 In Progress (bitwise ✅, $comment ✅, $rand ✅) |
| 2 | Update Operators Completion | ⬜ Not Started |
| 3 | Array Update Modifiers | ⬜ Not Started |
| 4 | Aggregation Stages (Priority) | ⬜ Not Started |
| 5 | Aggregation Stages (Extended) | ⬜ Not Started |
| 6 | Expression Operators (Arithmetic) | 🔄 In Progress ($rand ✅) |
| 7 | Expression Operators (Array) | ⬜ Not Started |
| 8 | Expression Operators (String) | ⬜ Not Started |
| 9 | Expression Operators (Date) | ⬜ Not Started |
| 10 | Expression Operators (Other) | ⬜ Not Started |
| 11 | Index Types & Options | ⬜ Not Started |
| 12 | Collection Methods | ⬜ Not Started |
| 13 | Final Polish | ⬜ Not Started |

---

## Phase 1: Query Operators Completion

**Goal**: Complete all missing query operators except geospatial (out of scope).

**Files to review**: `src/query-matcher.ts`, `test/queries.test.ts`

### Tasks

#### 1.1 Bitwise Query Operators
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 1.1.1 | `$bitsAllClear` | All bit positions are clear | A |
| 1.1.2 | `$bitsAllSet` | All bit positions are set | A |
| 1.1.3 | `$bitsAnyClear` | Any bit position is clear | A |
| 1.1.4 | `$bitsAnySet` | Any bit position is set | A |

**Research needed**: MongoDB bitwise operator behavior with negative numbers, binary data, position arrays vs bitmasks.

#### 1.2 Projection Operators
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 1.2.1 | `$` | Positional projection | B |
| 1.2.2 | `$elemMatch` | Projection context | B |
| 1.2.3 | `$slice` | Array slicing in projection | B |
| 1.2.4 | `$meta` | Text search score | C |

**Research needed**: Projection operator interaction with queries, nested array behavior.

#### 1.3 Other Query Operators
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 1.3.1 | `$jsonSchema` | Schema validation in queries | D |
| 1.3.2 | `$comment` | Query comments (no-op) | D |
| 1.3.3 | `$rand` | Random value comparison | D |

### Phase 1 Checklist
- [ ] All tests written and passing
- [ ] Dual-target tests verified against MongoDB
- [ ] Error messages match MongoDB
- [ ] Edge cases covered
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 2: Update Operators Completion

**Goal**: Complete all missing update operators.

**Files to review**: `src/update-operators.ts`, `test/updates.test.ts`

### Tasks

#### 2.1 Missing Array Operators
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 2.1.1 | `$pullAll` | Remove all matching values | A |

#### 2.2 Positional Update Operators
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 2.2.1 | `$` | Update first matching element | B |
| 2.2.2 | `$[]` | Update all elements | B |
| 2.2.3 | `$[<identifier>]` | Update with arrayFilters | C |

**Research needed**: How positional operators interact with nested arrays, arrayFilters syntax and matching.

#### 2.3 Bitwise Update Operator
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 2.3.1 | `$bit` | Bitwise AND, OR, XOR | D |

### Phase 2 Checklist
- [ ] All tests written and passing
- [ ] Dual-target tests verified against MongoDB
- [ ] arrayFilters option implemented
- [ ] Error messages match MongoDB
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 3: Array Update Modifiers

**Goal**: Complete $push modifiers.

**Files to review**: `src/update-operators.ts`, `test/arrays.test.ts`

### Tasks

| Task | Modifier | Description | Parallel Group |
|------|----------|-------------|----------------|
| 3.1 | `$position` | Insert at specific index | A |
| 3.2 | `$slice` | Limit array size | A |
| 3.3 | `$sort` | Sort array after modification | A |

**Research needed**: Modifier interaction order, behavior with negative positions.

### Phase 3 Checklist
- [ ] All tests written and passing
- [ ] Modifier combinations tested
- [ ] Edge cases (empty arrays, out of bounds) covered
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 4: Aggregation Stages (Priority)

**Goal**: Implement high-value aggregation stages.

**Files to review**: `src/aggregation/`, `test/aggregation.test.ts`

### Tasks

| Task | Stage | Description | Parallel Group |
|------|-------|-------------|----------------|
| 4.1 | `$facet` | Multiple sub-pipelines | A |
| 4.2 | `$bucket` | Group into buckets | B |
| 4.3 | `$bucketAuto` | Auto-create buckets | B |
| 4.4 | `$sortByCount` | Group and count | C |
| 4.5 | `$sample` | Random sampling | C |
| 4.6 | `$unionWith` | Union collections | D |
| 4.7 | `$merge` | Merge into collection | D |

**Research needed**: $facet sub-pipeline isolation, $bucket boundary handling, $merge whenMatched/whenNotMatched options.

### Phase 4 Checklist
- [ ] All tests written and passing
- [ ] Complex pipeline combinations tested
- [ ] Error messages match MongoDB
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 5: Aggregation Stages (Extended)

**Goal**: Implement remaining useful aggregation stages.

### Tasks

| Task | Stage | Description | Parallel Group |
|------|-------|-------------|----------------|
| 5.1 | `$graphLookup` | Recursive lookup | A |
| 5.2 | `$redact` | Field-level access control | B |
| 5.3 | `$replaceWith` | Replace document (alias) | C |
| 5.4 | `$unset` | Remove fields (alias) | C |
| 5.5 | `$documents` | Inject literal documents | D |
| 5.6 | `$densify` | Fill gaps in data | E |
| 5.7 | `$fill` | Fill missing values | E |
| 5.8 | `$setWindowFields` | Window functions | F |

**Note**: `$setWindowFields` is complex - may need dedicated sub-phase.

### Phase 5 Checklist
- [ ] All tests written and passing
- [ ] $graphLookup depth limits tested
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 6: Expression Operators (Arithmetic)

**Goal**: Complete arithmetic expression operators.

**Files to review**: `src/aggregation/operators/arithmetic.ts`

### Tasks

| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 6.1 | `$exp` | Euler's number to power | A |
| 6.2 | `$ln` | Natural logarithm | A |
| 6.3 | `$log` | Log with base | A |
| 6.4 | `$log10` | Base 10 logarithm | A |
| 6.5 | `$pow` | Power | B |
| 6.6 | `$sqrt` | Square root | B |
| 6.7 | `$trunc` | Truncate to integer | B |

### Phase 6 Checklist
- [ ] All tests written and passing
- [ ] Edge cases (negative, zero, infinity) covered
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 7: Expression Operators (Array)

**Goal**: Complete array expression operators.

**Files to review**: `src/aggregation/operators/array.ts`

### Tasks

| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 7.1 | `$arrayToObject` | Array to object | A |
| 7.2 | `$objectToArray` | Object to array | A |
| 7.3 | `$first` | First array element | B |
| 7.4 | `$last` | Last array element | B |
| 7.5 | `$indexOfArray` | Find index of element | B |
| 7.6 | `$isArray` | Check if array | C |
| 7.7 | `$range` | Generate number range | C |
| 7.8 | `$reverseArray` | Reverse array | C |
| 7.9 | `$zip` | Zip arrays together | D |
| 7.10 | `$sortArray` | Sort array | D |

### Phase 7 Checklist
- [ ] All tests written and passing
- [ ] Null/undefined handling tested
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 8: Expression Operators (String)

**Goal**: Complete string expression operators.

**Files to review**: `src/aggregation/operators/string.ts`

### Tasks

| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 8.1 | `$regexFind` | First regex match | A |
| 8.2 | `$regexFindAll` | All regex matches | A |
| 8.3 | `$regexMatch` | Test regex match | A |
| 8.4 | `$replaceOne` | Replace first occurrence | B |
| 8.5 | `$replaceAll` | Replace all occurrences | B |
| 8.6 | `$strcasecmp` | Case-insensitive compare | C |
| 8.7 | `$indexOfBytes` | Byte index | C |
| 8.8 | `$strLenBytes` | Byte length | C |
| 8.9 | `$substrBytes` | Byte substring | C |

### Phase 8 Checklist
- [ ] All tests written and passing
- [ ] Unicode handling tested
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 9: Expression Operators (Date)

**Goal**: Complete date expression operators.

**Files to review**: `src/aggregation/operators/date.ts`

### Tasks

| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 9.1 | `$dateAdd` | Add to date | A |
| 9.2 | `$dateSubtract` | Subtract from date | A |
| 9.3 | `$dateDiff` | Difference between dates | A |
| 9.4 | `$dateFromParts` | Construct date | B |
| 9.5 | `$dateToParts` | Decompose date | B |
| 9.6 | `$dateFromString` | Parse date string | C |
| 9.7 | `$dayOfYear` | Day of year (1-366) | D |
| 9.8 | `$week` | Week of year | D |
| 9.9 | `$isoWeek` | ISO week | D |
| 9.10 | `$isoWeekYear` | ISO week year | D |
| 9.11 | `$isoDayOfWeek` | ISO day of week | D |
| 9.12 | `$millisecond` | Millisecond component | D |

### Phase 9 Checklist
- [ ] All tests written and passing
- [ ] Timezone handling tested
- [ ] Leap year edge cases covered
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 10: Expression Operators (Other)

**Goal**: Complete remaining expression operators.

### Tasks

#### 10.1 Comparison
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 10.1.1 | `$cmp` | Compare two values | A |

#### 10.2 Conditional
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 10.2.1 | `$switch` | Switch statement | A |

#### 10.3 Type Operators
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 10.3.1 | `$convert` | Generic type conversion | B |
| 10.3.2 | `$isNumber` | Check if number | B |
| 10.3.3 | `$toDecimal` | Convert to decimal | B |
| 10.3.4 | `$toLong` | Convert to long | B |
| 10.3.5 | `$toObjectId` | Convert to ObjectId | B |

#### 10.4 Accumulators
| Task | Operator | Description | Parallel Group |
|------|----------|-------------|----------------|
| 10.4.1 | `$count` | Count accumulator | C |
| 10.4.2 | `$mergeObjects` | Merge objects | C |
| 10.4.3 | `$stdDevPop` | Population std dev | D |
| 10.4.4 | `$stdDevSamp` | Sample std dev | D |

### Phase 10 Checklist
- [ ] All tests written and passing
- [ ] Type coercion edge cases covered
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 11: Index Types & Options

**Goal**: Complete index functionality.

**Files to review**: `src/index-manager.ts`, `test/indexes.test.ts`

### Tasks

| Task | Feature | Description | Parallel Group |
|------|---------|-------------|----------------|
| 11.1 | Hashed Index | Hash-based indexing | A |
| 11.2 | Wildcard Index | Dynamic field indexing | B |
| 11.3 | `collation` option | Locale-aware comparison | C |
| 11.4 | `hidden` option | Hide from query planner | D |
| 11.5 | `weights` option | Text index weights | E |
| 11.6 | `default_language` | Text index language | E |

**Note**: Geospatial indexes (2d, 2dsphere) are out of scope.

### Phase 11 Checklist
- [ ] All tests written and passing
- [ ] Index enforcement tested
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 12: Collection Methods

**Goal**: Complete collection method coverage.

**Files to review**: `src/collection.ts`, `test/foundation.test.ts`

### Tasks

| Task | Method | Description | Parallel Group |
|------|--------|-------------|----------------|
| 12.1 | `replaceOne` | Top-level method | A |
| 12.2 | `createIndexes` | Batch index creation | A |
| 12.3 | `dropIndexes` | Batch index removal | A |
| 12.4 | `hint` option | Index hints | B |
| 12.5 | `maxTimeMS` option | Operation timeout | C |

### Phase 12 Checklist
- [ ] All tests written and passing
- [ ] Error handling matches MongoDB
- [ ] Code review completed
- [ ] LIMITATIONS.md updated
- [ ] Changes committed and pushed

---

## Phase 13: Final Polish

**Goal**: Final cleanup and documentation.

### Tasks

| Task | Description | Parallel Group |
|------|-------------|----------------|
| 13.1 | Full test suite review | A |
| 13.2 | Documentation update | A |
| 13.3 | Error message audit | B |
| 13.4 | Performance review | B |
| 13.5 | Update LIMITATIONS.md to final state | C |
| 13.6 | Update README.md | C |
| 13.7 | Version bump to 0.2.0 | D |

### Phase 13 Checklist
- [ ] All tests passing
- [ ] Documentation complete
- [ ] CHANGELOG updated
- [ ] Version bumped
- [ ] Final commit and push

---

## Execution Protocol

### Starting a Phase

1. Read this ROADMAP.md
2. Read LIMITATIONS.md for current status
3. Read relevant source files
4. Read relevant test files
5. Create todo list for phase tasks

### Executing a Task

1. **Research** (can use subagent):
   - Search MongoDB documentation for operator/feature
   - Find official error messages
   - Identify edge cases
   - Note any special behaviors

2. **Test First**:
   - Write failing test for basic functionality
   - Write edge case tests
   - Run tests to confirm they fail

3. **Implement**:
   - Review existing code patterns
   - Implement minimum to pass tests
   - Match MongoDB error messages

4. **Verify**:
   - Run full test suite
   - Run against real MongoDB if available
   - Check for regressions

5. **Commit**:
   - `git add` changed files
   - `git commit -m "feat(module): add $operator support"`

### Completing a Phase

1. Run full test suite
2. Perform code review:
   - Check consistency with existing patterns
   - Verify error handling
   - Review test coverage
3. Update LIMITATIONS.md
4. Update phase status in this ROADMAP.md
5. Commit and push all changes

---

## Out of Scope

The following are explicitly not planned for implementation:

- **Geospatial** (`$geoNear`, `$geoWithin`, `$near`, 2d/2dsphere indexes)
- **Atlas-specific** (`$search`, `$searchMeta`)
- **Real-time** (`$changeStream`, `watch()`)
- **Server features** (transactions, sessions, auth, replication, sharding)
- **JavaScript execution** (`$where`, `$accumulator` with custom JS)
- **GridFS** (use filesystem directly)

---

## Notes for Subagents

When spawning subagents for this work:

1. **Always include context**: Tell them which phase/task, point to this file
2. **Be specific**: "Implement $operator in file X following patterns in Y"
3. **Research tasks**: "Search MongoDB docs for $operator error messages and edge cases"
4. **Review tasks**: "Review changes in src/X.ts for consistency with existing patterns"

Remember: Each subagent is stateless. Give complete context in the prompt.

---

*Last updated: Phase 0 - Initial roadmap creation*
