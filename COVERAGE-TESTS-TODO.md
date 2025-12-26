# Test Coverage TODO - Specific Tests to Add

This file contains concrete test cases to add, organized by priority.

---

## CRITICAL: Date Operators (38% coverage)

Create `test/date-operators-extended.test.ts`:

```typescript
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { getTestClient, getTestDb, getTestCollection, cleanup } from './test-harness.ts';

describe('Extended Date Operators (MangoDB)', () => {
  let client: TestClient;
  let db: TestDb;
  let collection: TestCollection;

  beforeEach(async () => {
    client = getTestClient();
    await client.connect();
    db = client.db('test_date_ops');
    collection = db.collection('dates');
    await collection.insertMany([
      { _id: 1, date: new Date('2024-03-15T10:30:45.123Z') },
      { _id: 2, date: new Date('2024-12-31T23:59:59.999Z') },
      { _id: 3, date: null },
    ]);
  });

  afterEach(async () => {
    await cleanup(client, db);
  });

  describe('$millisecond', () => {
    it('should return millisecond component of date', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { ms: { $millisecond: '$date' } } }
      ]).toArray();
      assert.strictEqual(result[0].ms, 123);
    });

    it('should return null for null date', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 3 } },
        { $project: { ms: { $millisecond: '$date' } } }
      ]).toArray();
      assert.strictEqual(result[0].ms, null);
    });
  });

  describe('$dayOfYear', () => {
    it('should return day of year (1-366)', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { doy: { $dayOfYear: '$date' } } }
      ]).toArray();
      assert.strictEqual(result[0].doy, 75); // March 15 = 75th day
    });

    it('should return 366 for Dec 31 leap year', async () => {
      await collection.insertOne({ _id: 4, date: new Date('2024-12-31T00:00:00Z') });
      const result = await collection.aggregate([
        { $match: { _id: 4 } },
        { $project: { doy: { $dayOfYear: '$date' } } }
      ]).toArray();
      assert.strictEqual(result[0].doy, 366);
    });
  });

  describe('$week', () => {
    it('should return week number (0-53)', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { week: { $week: '$date' } } }
      ]).toArray();
      assert.strictEqual(typeof result[0].week, 'number');
      assert(result[0].week >= 0 && result[0].week <= 53);
    });
  });

  describe('$isoWeek', () => {
    it('should return ISO week number (1-53)', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { isoWeek: { $isoWeek: '$date' } } }
      ]).toArray();
      assert(result[0].isoWeek >= 1 && result[0].isoWeek <= 53);
    });
  });

  describe('$isoWeekYear', () => {
    it('should return ISO week year', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { year: { $isoWeekYear: '$date' } } }
      ]).toArray();
      assert.strictEqual(result[0].year, 2024);
    });
  });

  describe('$isoDayOfWeek', () => {
    it('should return ISO day of week (1=Monday, 7=Sunday)', async () => {
      // March 15, 2024 is a Friday = 5
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { dow: { $isoDayOfWeek: '$date' } } }
      ]).toArray();
      assert.strictEqual(result[0].dow, 5);
    });
  });

  describe('$dateAdd', () => {
    it('should add years to date', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { newDate: { $dateAdd: { startDate: '$date', unit: 'year', amount: 1 } } } }
      ]).toArray();
      assert.strictEqual(result[0].newDate.getUTCFullYear(), 2025);
    });

    it('should add months to date', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { newDate: { $dateAdd: { startDate: '$date', unit: 'month', amount: 2 } } } }
      ]).toArray();
      assert.strictEqual(result[0].newDate.getUTCMonth(), 4); // May (0-indexed)
    });

    it('should add days to date', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { newDate: { $dateAdd: { startDate: '$date', unit: 'day', amount: 10 } } } }
      ]).toArray();
      assert.strictEqual(result[0].newDate.getUTCDate(), 25);
    });

    it('should handle negative amounts (subtract)', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { newDate: { $dateAdd: { startDate: '$date', unit: 'day', amount: -5 } } } }
      ]).toArray();
      assert.strictEqual(result[0].newDate.getUTCDate(), 10);
    });

    it('should return null for null startDate', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 3 } },
        { $project: { newDate: { $dateAdd: { startDate: '$date', unit: 'day', amount: 1 } } } }
      ]).toArray();
      assert.strictEqual(result[0].newDate, null);
    });
  });

  describe('$dateSubtract', () => {
    it('should subtract days from date', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { newDate: { $dateSubtract: { startDate: '$date', unit: 'day', amount: 5 } } } }
      ]).toArray();
      assert.strictEqual(result[0].newDate.getUTCDate(), 10);
    });
  });

  describe('$dateDiff', () => {
    it('should calculate year difference', async () => {
      await collection.insertOne({ _id: 5, start: new Date('2020-01-01'), end: new Date('2024-01-01') });
      const result = await collection.aggregate([
        { $match: { _id: 5 } },
        { $project: { diff: { $dateDiff: { startDate: '$start', endDate: '$end', unit: 'year' } } } }
      ]).toArray();
      assert.strictEqual(result[0].diff, 4);
    });

    it('should calculate month difference', async () => {
      await collection.insertOne({ _id: 6, start: new Date('2024-01-15'), end: new Date('2024-04-15') });
      const result = await collection.aggregate([
        { $match: { _id: 6 } },
        { $project: { diff: { $dateDiff: { startDate: '$start', endDate: '$end', unit: 'month' } } } }
      ]).toArray();
      assert.strictEqual(result[0].diff, 3);
    });

    it('should calculate day difference', async () => {
      await collection.insertOne({ _id: 7, start: new Date('2024-01-01'), end: new Date('2024-01-11') });
      const result = await collection.aggregate([
        { $match: { _id: 7 } },
        { $project: { diff: { $dateDiff: { startDate: '$start', endDate: '$end', unit: 'day' } } } }
      ]).toArray();
      assert.strictEqual(result[0].diff, 10);
    });
  });

  describe('$dateFromParts', () => {
    it('should construct date from year/month/day', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { date: { $dateFromParts: { year: 2024, month: 6, day: 15 } } } }
      ]).toArray();
      const d = result[0].date;
      assert.strictEqual(d.getUTCFullYear(), 2024);
      assert.strictEqual(d.getUTCMonth(), 5); // June = 5
      assert.strictEqual(d.getUTCDate(), 15);
    });

    it('should construct date with time components', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { date: { $dateFromParts: { year: 2024, month: 1, day: 1, hour: 12, minute: 30, second: 45 } } } }
      ]).toArray();
      const d = result[0].date;
      assert.strictEqual(d.getUTCHours(), 12);
      assert.strictEqual(d.getUTCMinutes(), 30);
      assert.strictEqual(d.getUTCSeconds(), 45);
    });

    it('should construct date from ISO week format', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { date: { $dateFromParts: { isoWeekYear: 2024, isoWeek: 1, isoDayOfWeek: 1 } } } }
      ]).toArray();
      assert(result[0].date instanceof Date);
    });
  });

  describe('$dateToParts', () => {
    it('should return date parts object', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { parts: { $dateToParts: { date: '$date' } } } }
      ]).toArray();
      const parts = result[0].parts;
      assert.strictEqual(parts.year, 2024);
      assert.strictEqual(parts.month, 3);
      assert.strictEqual(parts.day, 15);
      assert.strictEqual(parts.hour, 10);
      assert.strictEqual(parts.minute, 30);
      assert.strictEqual(parts.second, 45);
      assert.strictEqual(parts.millisecond, 123);
    });

    it('should return ISO parts with iso8601: true', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { parts: { $dateToParts: { date: '$date', iso8601: true } } } }
      ]).toArray();
      const parts = result[0].parts;
      assert('isoWeekYear' in parts);
      assert('isoWeek' in parts);
      assert('isoDayOfWeek' in parts);
    });
  });

  describe('$dateFromString', () => {
    it('should parse ISO date string', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { date: { $dateFromString: { dateString: '2024-06-15T12:00:00Z' } } } }
      ]).toArray();
      assert(result[0].date instanceof Date);
      assert.strictEqual(result[0].date.getUTCMonth(), 5);
    });

    it('should return null for null with onNull', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { date: { $dateFromString: { dateString: null, onNull: new Date('2000-01-01') } } } }
      ]).toArray();
      assert.strictEqual(result[0].date.getUTCFullYear(), 2000);
    });

    it('should use onError for invalid strings', async () => {
      const result = await collection.aggregate([
        { $match: { _id: 1 } },
        { $project: { date: { $dateFromString: { dateString: 'invalid', onError: new Date('1999-01-01') } } } }
      ]).toArray();
      assert.strictEqual(result[0].date.getUTCFullYear(), 1999);
    });
  });
});
```

---

## CRITICAL: Geo Calculations (59% coverage)

Create `test/geo-calculations.test.ts`:

```typescript
import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  pointInPolygon,
  pointToPolygonDistance,
  segmentIntersectsPolygon,
  polygonsIntersect,
  bboxIntersects,
  geometryContainsPoint,
  geometriesIntersect,
} from '../src/geo/calculations.ts';

describe('Geo Calculations', () => {
  const simplePolygon = {
    type: 'Polygon' as const,
    coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
  };

  describe('pointToPolygonDistance', () => {
    it('should return 0 when point is inside polygon', () => {
      const dist = pointToPolygonDistance([5, 5], simplePolygon);
      assert.strictEqual(dist, 0);
    });

    it('should return distance to nearest edge when outside', () => {
      const dist = pointToPolygonDistance([15, 5], simplePolygon);
      assert.strictEqual(dist, 5);
    });

    it('should handle point on edge', () => {
      const dist = pointToPolygonDistance([5, 0], simplePolygon);
      assert.strictEqual(dist, 0);
    });
  });

  describe('segmentIntersectsPolygon', () => {
    it('should detect segment crossing polygon', () => {
      const result = segmentIntersectsPolygon([-5, 5], [15, 5], simplePolygon);
      assert.strictEqual(result, true);
    });

    it('should detect segment starting inside polygon', () => {
      const result = segmentIntersectsPolygon([5, 5], [15, 5], simplePolygon);
      assert.strictEqual(result, true);
    });

    it('should return false for segment outside polygon', () => {
      const result = segmentIntersectsPolygon([15, 5], [20, 5], simplePolygon);
      assert.strictEqual(result, false);
    });
  });

  describe('polygonsIntersect', () => {
    it('should detect overlapping polygons', () => {
      const poly2 = {
        type: 'Polygon' as const,
        coordinates: [[[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]]]
      };
      assert.strictEqual(polygonsIntersect(simplePolygon, poly2), true);
    });

    it('should detect one polygon inside another', () => {
      const innerPoly = {
        type: 'Polygon' as const,
        coordinates: [[[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]]]
      };
      assert.strictEqual(polygonsIntersect(simplePolygon, innerPoly), true);
    });

    it('should return false for non-overlapping polygons', () => {
      const farPoly = {
        type: 'Polygon' as const,
        coordinates: [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]]
      };
      assert.strictEqual(polygonsIntersect(simplePolygon, farPoly), false);
    });
  });

  describe('bboxIntersects', () => {
    it('should detect overlapping bounding boxes', () => {
      assert.strictEqual(bboxIntersects([0, 0, 10, 10], [5, 5, 15, 15]), true);
    });

    it('should return false for non-overlapping bboxes', () => {
      assert.strictEqual(bboxIntersects([0, 0, 10, 10], [20, 20, 30, 30]), false);
    });

    it('should detect edge-touching bboxes', () => {
      assert.strictEqual(bboxIntersects([0, 0, 10, 10], [10, 0, 20, 10]), true);
    });
  });

  describe('geometryContainsPoint', () => {
    it('should check MultiPoint contains point', () => {
      const multiPoint = { type: 'MultiPoint' as const, coordinates: [[0, 0], [5, 5], [10, 10]] };
      assert.strictEqual(geometryContainsPoint(multiPoint, [5, 5]), true);
      assert.strictEqual(geometryContainsPoint(multiPoint, [3, 3]), false);
    });

    it('should check MultiLineString contains point', () => {
      const multiLine = {
        type: 'MultiLineString' as const,
        coordinates: [[[0, 0], [10, 0]], [[0, 5], [10, 5]]]
      };
      assert.strictEqual(geometryContainsPoint(multiLine, [5, 0]), true);
    });

    it('should check MultiPolygon contains point', () => {
      const multiPoly = {
        type: 'MultiPolygon' as const,
        coordinates: [
          [[[0, 0], [5, 0], [5, 5], [0, 5], [0, 0]]],
          [[[10, 10], [15, 10], [15, 15], [10, 15], [10, 10]]]
        ]
      };
      assert.strictEqual(geometryContainsPoint(multiPoly, [2, 2]), true);
      assert.strictEqual(geometryContainsPoint(multiPoly, [12, 12]), true);
      assert.strictEqual(geometryContainsPoint(multiPoly, [7, 7]), false);
    });

    it('should check GeometryCollection contains point', () => {
      const collection = {
        type: 'GeometryCollection' as const,
        geometries: [
          { type: 'Point' as const, coordinates: [5, 5] },
          simplePolygon
        ]
      };
      assert.strictEqual(geometryContainsPoint(collection, [5, 5]), true);
      assert.strictEqual(geometryContainsPoint(collection, [2, 2]), true);
    });
  });

  describe('geometriesIntersect', () => {
    it('should check LineString vs Polygon intersection', () => {
      const line = { type: 'LineString' as const, coordinates: [[-5, 5], [15, 5]] };
      assert.strictEqual(geometriesIntersect(line, simplePolygon), true);
    });

    it('should check LineString vs LineString intersection', () => {
      const line1 = { type: 'LineString' as const, coordinates: [[0, 0], [10, 10]] };
      const line2 = { type: 'LineString' as const, coordinates: [[0, 10], [10, 0]] };
      assert.strictEqual(geometriesIntersect(line1, line2), true);
    });

    it('should check MultiPolygon vs Polygon intersection', () => {
      const multiPoly = {
        type: 'MultiPolygon' as const,
        coordinates: [[[[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]]]]
      };
      assert.strictEqual(geometriesIntersect(multiPoly, simplePolygon), true);
    });

    it('should handle GeometryCollection', () => {
      const collection = {
        type: 'GeometryCollection' as const,
        geometries: [{ type: 'Point' as const, coordinates: [5, 5] }]
      };
      assert.strictEqual(geometriesIntersect(collection, simplePolygon), true);
    });
  });
});
```

---

## HIGH: Type Conversion Operators (67% coverage)

Add to `test/expression-operators.test.ts`:

```typescript
describe('$toLong', () => {
  it('should convert number to long', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $toLong: 42.9 } } }
    ]).toArray();
    assert.strictEqual(result[0].val, 42);
  });

  it('should convert boolean to long', async () => {
    const result = await collection.aggregate([
      { $project: { t: { $toLong: true }, f: { $toLong: false } } }
    ]).toArray();
    assert.strictEqual(result[0].t, 1);
    assert.strictEqual(result[0].f, 0);
  });

  it('should convert string to long', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $toLong: '123' } } }
    ]).toArray();
    assert.strictEqual(result[0].val, 123);
  });

  it('should convert Date to timestamp', async () => {
    const date = new Date('2024-01-01T00:00:00Z');
    await collection.insertOne({ _id: 'x', d: date });
    const result = await collection.aggregate([
      { $match: { _id: 'x' } },
      { $project: { val: { $toLong: '$d' } } }
    ]).toArray();
    assert.strictEqual(result[0].val, date.getTime());
  });
});

describe('$toObjectId', () => {
  it('should convert valid 24-char hex string', async () => {
    const result = await collection.aggregate([
      { $project: { oid: { $toObjectId: '507f1f77bcf86cd799439011' } } }
    ]).toArray();
    assert.strictEqual(result[0].oid.toHexString(), '507f1f77bcf86cd799439011');
  });

  it('should throw for invalid string length', async () => {
    await assert.rejects(async () => {
      await collection.aggregate([
        { $project: { oid: { $toObjectId: 'invalid' } } }
      ]).toArray();
    });
  });
});

describe('$convert', () => {
  it('should convert using type code 1 (double)', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $convert: { input: '42.5', to: 1 } } } }
    ]).toArray();
    assert.strictEqual(result[0].val, 42.5);
  });

  it('should use onNull handler', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $convert: { input: null, to: 'int', onNull: -1 } } } }
    ]).toArray();
    assert.strictEqual(result[0].val, -1);
  });

  it('should use onError handler', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $convert: { input: 'abc', to: 'int', onError: 0 } } } }
    ]).toArray();
    assert.strictEqual(result[0].val, 0);
  });
});
```

---

## HIGH: String Operators (71% coverage)

Add to `test/expression-operators.test.ts`:

```typescript
describe('$replaceOne', () => {
  it('should replace first occurrence', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $replaceOne: { input: 'hello world world', find: 'world', replacement: 'there' } } } }
    ]).toArray();
    assert.strictEqual(result[0].val, 'hello there world');
  });

  it('should return original if not found', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $replaceOne: { input: 'hello', find: 'xyz', replacement: 'abc' } } } }
    ]).toArray();
    assert.strictEqual(result[0].val, 'hello');
  });

  it('should return null for null input', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $replaceOne: { input: null, find: 'a', replacement: 'b' } } } }
    ]).toArray();
    assert.strictEqual(result[0].val, null);
  });
});

describe('$replaceAll', () => {
  it('should replace all occurrences', async () => {
    const result = await collection.aggregate([
      { $project: { val: { $replaceAll: { input: 'aaa', find: 'a', replacement: 'b' } } } }
    ]).toArray();
    assert.strictEqual(result[0].val, 'bbb');
  });
});

describe('$strcasecmp', () => {
  it('should return -1 when first string is less', async () => {
    const result = await collection.aggregate([
      { $project: { cmp: { $strcasecmp: ['abc', 'XYZ'] } } }
    ]).toArray();
    assert.strictEqual(result[0].cmp, -1);
  });

  it('should return 0 for case-insensitive equal strings', async () => {
    const result = await collection.aggregate([
      { $project: { cmp: { $strcasecmp: ['Hello', 'HELLO'] } } }
    ]).toArray();
    assert.strictEqual(result[0].cmp, 0);
  });

  it('should return 1 when first string is greater', async () => {
    const result = await collection.aggregate([
      { $project: { cmp: { $strcasecmp: ['xyz', 'ABC'] } } }
    ]).toArray();
    assert.strictEqual(result[0].cmp, 1);
  });
});

describe('$strLenBytes', () => {
  it('should return byte length for ASCII', async () => {
    const result = await collection.aggregate([
      { $project: { len: { $strLenBytes: 'hello' } } }
    ]).toArray();
    assert.strictEqual(result[0].len, 5);
  });

  it('should return byte length for UTF-8 multibyte', async () => {
    const result = await collection.aggregate([
      { $project: { len: { $strLenBytes: 'cafe' } } }  // 'cafe' with accent would be more
    ]).toArray();
    assert.strictEqual(result[0].len, 4);
  });
});

describe('$indexOfBytes', () => {
  it('should return byte index of substring', async () => {
    const result = await collection.aggregate([
      { $project: { idx: { $indexOfBytes: ['hello', 'lo'] } } }
    ]).toArray();
    assert.strictEqual(result[0].idx, 3);
  });

  it('should return -1 if not found', async () => {
    const result = await collection.aggregate([
      { $project: { idx: { $indexOfBytes: ['hello', 'xyz'] } } }
    ]).toArray();
    assert.strictEqual(result[0].idx, -1);
  });
});

describe('$substrBytes', () => {
  it('should extract substring by byte position', async () => {
    const result = await collection.aggregate([
      { $project: { sub: { $substrBytes: ['hello', 1, 3] } } }
    ]).toArray();
    assert.strictEqual(result[0].sub, 'ell');
  });
});
```

---

## Files to Create

1. `test/date-operators-extended.test.ts` - Date operator tests
2. `test/geo-calculations.test.ts` - Geo calculation unit tests
3. `test/geo-geometry.test.ts` - GeoJSON validation tests
4. `test/type-conversion-extended.test.ts` - Type conversion tests

---

## Quick Wins (Easy additions to existing tests)

### In `test/expression-operators.test.ts`:
- Add null handling tests to every operator
- Add error case tests for type mismatches
- Add edge case tests (empty strings, empty arrays)

### In `test/geo.test.ts`:
- Add MultiPolygon tests
- Add GeometryCollection tests
- Add coordinate boundary tests

### In `test/aggregation.test.ts`:
- Add complex multi-stage pipeline tests
- Add error propagation tests
