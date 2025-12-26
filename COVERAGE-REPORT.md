# Test Coverage Analysis Report

**Generated:** 2025-12-26
**Overall Coverage:** 88.50% line | 84.13% branch | 87.31% function
**Test Results:** 1,289 tests passing across 400 suites

---

## Executive Summary

MangoDB has solid overall test coverage at 88.5%, but several modules have significant gaps that could hide bugs. This report identifies **15 files with coverage below 90%** and provides specific recommendations for improvement.

### Priority Levels
- **CRITICAL** (< 60%): Immediate attention required
- **HIGH** (60-75%): Should be addressed soon
- **MEDIUM** (75-90%): Plan for upcoming sprints
- **LOW** (> 90%): Minor gaps, address opportunistically

---

## Critical Coverage Gaps (< 60%)

### 1. `src/aggregation/operators/date.ts` - 38.49% line coverage

**Status:** CRITICAL - Most date operators are untested

**Untested Functions (lines 177-621):**
| Operator | Lines | Status |
|----------|-------|--------|
| `$millisecond` | 187-196 | Untested |
| `$dayOfYear` | 201-211 | Untested |
| `$week` | 216-229 | Untested |
| `$isoWeek` | 234-253 | Untested |
| `$isoWeekYear` | 258-285 | Untested |
| `$isoDayOfWeek` | 290-300 | Untested |
| `$dateAdd` | 333-388 | Untested |
| `$dateSubtract` | 393-406 | Untested |
| `$dateDiff` | 411-459 | Untested |
| `$dateFromParts` | 464-518 | Untested |
| `$dateToParts` | 523-582 | Untested |
| `$dateFromString` | 587-621 | Untested |

**Recommended Tests:**
```typescript
// test/date-operators.test.ts
describe('$millisecond', () => {
  it('should return milliseconds from date');
  it('should return null for null input');
  it('should throw for non-date input');
});

describe('$dateAdd', () => {
  it('should add years to date');
  it('should add months to date');
  it('should add days to date');
  it('should handle negative amounts');
  it('should return null for null startDate');
});

describe('$dateDiff', () => {
  it('should calculate year difference');
  it('should calculate month difference');
  it('should calculate day difference');
  it('should handle all time units');
});

describe('$dateFromParts', () => {
  it('should construct date from year/month/day');
  it('should construct date from ISO week format');
  it('should use defaults for missing parts');
});
```

---

### 2. `src/geo/calculations.ts` - 59.33% line coverage

**Status:** CRITICAL - Core geospatial calculations untested

**Untested Functions:**
| Function | Lines | Purpose |
|----------|-------|---------|
| `pointToPolygonDistance` | 181-200 | Distance from point to polygon |
| `segmentIntersectsPolygon` | 251-271 | Line-polygon intersection |
| `polygonsIntersect` | 276-309 | Polygon-polygon intersection |
| `bboxIntersects` | 315-325 | Bounding box intersection |
| `geometryContainsPoint` (MultiPoint, MultiLineString, MultiPolygon) | 398-425 | Multi-geometry containment |
| `geometriesIntersect` (various combos) | 431-535 | Geometry intersection tests |

**Recommended Tests:**
```typescript
// test/geo-calculations.test.ts
describe('pointToPolygonDistance', () => {
  it('should return 0 when point is inside polygon');
  it('should return distance to nearest edge when outside');
});

describe('polygonsIntersect', () => {
  it('should detect overlapping polygons');
  it('should detect vertex inside other polygon');
  it('should detect edge intersections');
  it('should return false for non-overlapping polygons');
});

describe('geometriesIntersect', () => {
  it('should handle LineString vs Polygon');
  it('should handle MultiPolygon vs Polygon');
  it('should handle GeometryCollection');
});
```

---

### 3. `src/geo/geometry.ts` - 61.25% line coverage

**Status:** CRITICAL - GeoJSON validation gaps

**Untested Functions:**
| Function | Lines | Purpose |
|----------|-------|---------|
| `isValidGeoJSONMultiPoint` | 133-140 | MultiPoint validation |
| `isValidGeoJSONMultiLineString` | 145-157 | MultiLineString validation |
| `isValidGeoJSONMultiPolygon` | 162-176 | MultiPolygon validation |
| `isValidGeoJSONGeometryCollection` | 181-188 | GeometryCollection validation |
| `extractCoordinates` | 249-282 | Coordinate extraction |
| `validateSphericalCoordinates` | 291-298 | Bounds validation |
| `normalizePoint` | 313-322 | Point normalization |
| `getBoundingBox` | 328-347 | Bounding box calculation |
| `getAllPositions` | 352-369 | Position extraction |

**Recommended Tests:**
```typescript
// test/geo-geometry.test.ts
describe('GeoJSON Validation', () => {
  describe('isValidGeoJSONMultiPolygon', () => {
    it('should validate correct MultiPolygon');
    it('should reject invalid ring structure');
    it('should reject non-closed rings');
  });

  describe('extractCoordinates', () => {
    it('should extract from GeoJSON Point');
    it('should extract from legacy {x, y} format');
    it('should extract from legacy {lng, lat} format');
    it('should extract from array format');
    it('should return null for invalid input');
  });
});
```

---

## High Priority Gaps (60-75%)

### 4. `src/aggregation/operators/type-conversion.ts` - 67.51% line coverage

**Untested Areas:**
- `$toLong` (lines 236-274) - Long integer conversion
- `$toDecimal` (lines 279-287) - Decimal conversion
- `$toObjectId` (lines 292-319) - ObjectId conversion
- `$convert` various type codes (lines 351-365)

**Recommended Tests:**
```typescript
describe('$toLong', () => {
  it('should convert number to long');
  it('should convert boolean to long');
  it('should convert string to long');
  it('should convert Date to long (timestamp)');
  it('should throw for invalid string');
});

describe('$toObjectId', () => {
  it('should convert valid 24-char hex string');
  it('should return null for null input');
  it('should throw for invalid string length');
  it('should pass through existing ObjectId');
});

describe('$convert', () => {
  it('should support type code 1 (double)');
  it('should support type code 7 (objectId)');
  it('should use onError handler');
  it('should use onNull handler');
});
```

---

### 5. `src/aggregation/operators/string.ts` - 71.65% line coverage

**Untested Functions:**
| Function | Lines | Status |
|----------|-------|--------|
| `$replaceOne` | 436-475 | Untested |
| `$replaceAll` | 480-519 | Untested |
| `$strcasecmp` | 525-544 | Untested |
| `$strLenBytes` | 549-569 | Untested |
| `$indexOfBytes` | 574-614 | Untested |
| `$substrBytes` | 619-642 | Untested |

**Recommended Tests:**
```typescript
describe('$replaceOne', () => {
  it('should replace first occurrence');
  it('should return original if not found');
  it('should return null for null input');
  it('should throw for non-string arguments');
});

describe('$strLenBytes', () => {
  it('should return byte length for ASCII');
  it('should return byte length for UTF-8');
  it('should throw for null input');
});
```

---

### 6. `src/aggregation/helpers.ts` - 72.00% line coverage

**Untested Area:** Lines 18-24

---

## Medium Priority Gaps (75-90%)

### 7. `src/geo/errors.ts` - 81.54% line coverage

Multiple error class constructors untested (lines 46-129).

### 8. `src/aggregation/date-utils.ts` - 84.62% line coverage

Date arithmetic edge cases (lines 54-143).

### 9. `src/aggregation/operators/array.ts` - 85.90% line coverage

**Untested Edge Cases:**
- `$setDifference` edge cases (lines 702-738)
- `$setEquals` error handling (lines 743-799)
- `$setIsSubset` error handling (lines 804-836)
- `$allElementsTrue` / `$anyElementTrue` (lines 841-908)

### 10. `src/geo/operators.ts` - 86.81% line coverage

- `$near` spherical mode (lines 48-58)
- `$geoWithin` legacy shapes (lines 266-288)

### 11. `src/aggregation/operators/conditional.ts` - 86.73% line coverage

- `$switch` edge cases (lines 63-85)

### 12. `src/aggregation/operators/arithmetic.ts` - 88.96% line coverage

- `$exp`, `$ln`, `$log`, `$log10` edge cases
- `$pow` edge cases
- `$sqrt` with negative numbers
- `$trunc` with decimal places

### 13. `src/aggregation/system-vars.ts` - 88.73% line coverage

- `$$CLUSTER_TIME`, `$$JS_SCOPE` (lines 64-71)

### 14. `src/aggregation/cursor.ts` - 89.16% line coverage

**Untested Pipeline Stages:**
- `$graphLookup` complex cases (lines 1383-1424)
- `$setWindowFields` partitioning (lines 1889-1920)
- `$densify` step handling (lines 2309-2315)
- Error handling in various stages

---

## Test Quality Recommendations

### 1. Edge Cases to Add

**Null/Undefined Handling:**
- Every operator should test null input behavior
- Test undefined vs null distinction
- Test missing field behavior

**Type Coercion:**
- Test operators with wrong types (expect errors)
- Test type boundaries (MAX_SAFE_INTEGER, etc.)

**Array Edge Cases:**
- Empty arrays
- Single-element arrays
- Nested arrays
- Arrays with null elements

**String Edge Cases:**
- Empty strings
- Unicode/UTF-8 multi-byte characters
- Very long strings

### 2. Integration Test Gaps

**Missing Pipeline Combinations:**
```typescript
// Complex pipeline not tested
[
  { $match: { status: 'active' } },
  { $lookup: { ... } },
  { $unwind: '$joined' },
  { $group: { _id: '$category', items: { $push: '$$ROOT' } } },
  { $setWindowFields: { ... } }
]
```

**Missing Operator Combinations:**
- `$expr` with complex expressions in queries
- Nested `$cond` / `$switch`
- `$reduce` with complex accumulators

### 3. Error Message Verification

Current tests often just check that errors are thrown. Add assertions for:
- Error message content
- Error codes (where applicable)
- Error types

### 4. MongoDB Parity Tests

For all untested operators, run against real MongoDB to verify:
- Exact return values
- Error conditions
- Edge case behavior

---

## Action Plan

### Phase 1: Critical (This Week)
1. Add date operator tests (38% -> 90%+)
2. Add geo calculation tests (59% -> 85%+)
3. Add geo geometry validation tests (61% -> 85%+)

### Phase 2: High Priority (Next Week)
4. Add type conversion tests (67% -> 90%+)
5. Add string operator tests (71% -> 90%+)

### Phase 3: Medium Priority (Following Weeks)
6. Address array operator edge cases
7. Add aggregation cursor edge cases
8. Improve error handling coverage

---

## Coverage Commands

```bash
# Run full coverage
npm run test:coverage

# Run specific test file
npm test -- test/date-operators.test.ts

# Run against real MongoDB
MONGODB_URI=mongodb://localhost:27017 npm test
```

---

## Files by Coverage (Sorted)

| File | Line % | Branch % | Priority |
|------|--------|----------|----------|
| date.ts | 38.49 | 72.73 | CRITICAL |
| geo/calculations.ts | 59.33 | 51.25 | CRITICAL |
| geo/geometry.ts | 61.25 | 46.34 | CRITICAL |
| type-conversion.ts | 67.51 | 73.15 | HIGH |
| string.ts | 71.65 | 58.14 | HIGH |
| helpers.ts | 72.00 | 70.59 | HIGH |
| geo/errors.ts | 81.54 | 41.67 | MEDIUM |
| date-utils.ts | 84.62 | 57.69 | MEDIUM |
| array.ts | 85.90 | 76.47 | MEDIUM |
| conditional.ts | 86.73 | 68.18 | MEDIUM |
| geo/operators.ts | 86.81 | 75.68 | MEDIUM |
| system-vars.ts | 88.73 | 50.00 | MEDIUM |
| arithmetic.ts | 88.96 | 81.20 | MEDIUM |
| cursor.ts (agg) | 89.16 | 82.24 | MEDIUM |
