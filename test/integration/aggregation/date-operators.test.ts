/**
 * Date Operators Extended Tests
 *
 * Tests for date operators that need additional coverage:
 * $millisecond, $dayOfYear, $week, $isoWeek, $isoWeekYear, $isoDayOfWeek,
 * $dateAdd, $dateSubtract, $dateDiff, $dateFromParts, $dateToParts, $dateFromString
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
} from "../../test-harness.ts";

describe(`Date Operators Extended (${getTestModeName()})`, () => {
  let client: TestClient;
  let cleanup: () => Promise<void>;
  let dbName: string;

  before(async () => {
    const result = await createTestClient();
    client = result.client;
    cleanup = result.cleanup;
    dbName = result.dbName;
    await client.connect();
  });

  after(async () => {
    await cleanup();
  });

  describe("$millisecond", () => {
    it("should extract milliseconds from date", async () => {
      const collection = client.db(dbName).collection("ms_basic");
      await collection.insertOne({ date: new Date("2023-06-15T14:30:45.123Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $millisecond: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 123);
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("ms_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([{ $project: { result: { $millisecond: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });

    it("should return null for missing field", async () => {
      const collection = client.db(dbName).collection("ms_missing");
      await collection.insertOne({ other: 1 });

      const results = await collection
        .aggregate([{ $project: { result: { $millisecond: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });

    it("should handle zero milliseconds", async () => {
      const collection = client.db(dbName).collection("ms_zero");
      await collection.insertOne({ date: new Date("2023-06-15T14:30:45.000Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $millisecond: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 0);
    });
  });

  describe("$dayOfYear", () => {
    it("should return 1 for January 1st", async () => {
      const collection = client.db(dbName).collection("doy_jan1");
      await collection.insertOne({ date: new Date("2023-01-01T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $dayOfYear: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 1);
    });

    it("should return 365 for December 31st (non-leap year)", async () => {
      const collection = client.db(dbName).collection("doy_dec31");
      await collection.insertOne({ date: new Date("2023-12-31T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $dayOfYear: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 365);
    });

    it("should return 366 for December 31st (leap year)", async () => {
      const collection = client.db(dbName).collection("doy_leap");
      await collection.insertOne({ date: new Date("2024-12-31T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $dayOfYear: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 366);
    });

    it("should handle February 29th in leap year", async () => {
      const collection = client.db(dbName).collection("doy_feb29");
      await collection.insertOne({ date: new Date("2024-02-29T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $dayOfYear: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 60);
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("doy_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([{ $project: { result: { $dayOfYear: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$week", () => {
    it("should return week number for first week of year", async () => {
      const collection = client.db(dbName).collection("week_first");
      // Jan 1, 2023 was a Sunday
      await collection.insertOne({ date: new Date("2023-01-01T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $week: "$date" }, _id: 0 } }])
        .toArray();

      // Week number should be 0 or 1 depending on implementation
      const r = results[0].result as number;
      assert.ok(r >= 0 && r <= 1);
    });

    it("should return correct week number mid-year", async () => {
      const collection = client.db(dbName).collection("week_mid");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $week: "$date" }, _id: 0 } }])
        .toArray();

      // Week number for June 15, 2023
      const r = results[0].result as number;
      assert.ok(r >= 23 && r <= 25);
    });

    it("should handle year boundary", async () => {
      const collection = client.db(dbName).collection("week_boundary");
      await collection.insertOne({ date: new Date("2023-12-31T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $week: "$date" }, _id: 0 } }])
        .toArray();

      const r = results[0].result as number;
      assert.ok(r >= 52 && r <= 53);
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("week_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([{ $project: { result: { $week: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$isoWeek", () => {
    it("should return ISO week number", async () => {
      const collection = client.db(dbName).collection("isoweek_basic");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $isoWeek: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 24);
    });

    it("should handle year boundary (week 1 may be in previous year)", async () => {
      const collection = client.db(dbName).collection("isoweek_boundary");
      // Dec 31, 2020 was week 53 of 2020
      await collection.insertOne({ date: new Date("2020-12-31T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $isoWeek: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 53);
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("isoweek_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([{ $project: { result: { $isoWeek: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$isoWeekYear", () => {
    it("should return ISO week year", async () => {
      const collection = client.db(dbName).collection("isoweekyear_basic");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $isoWeekYear: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 2023);
    });

    it("should handle year boundary (Jan 1 may belong to previous ISO year)", async () => {
      const collection = client.db(dbName).collection("isoweekyear_boundary");
      // Jan 1, 2021 was Friday, part of ISO week 53 of 2020
      await collection.insertOne({ date: new Date("2021-01-01T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $isoWeekYear: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 2020);
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("isoweekyear_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([{ $project: { result: { $isoWeekYear: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$isoDayOfWeek", () => {
    it("should return 1 for Monday", async () => {
      const collection = client.db(dbName).collection("isodow_monday");
      // June 19, 2023 was a Monday
      await collection.insertOne({ date: new Date("2023-06-19T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $isoDayOfWeek: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 1);
    });

    it("should return 7 for Sunday", async () => {
      const collection = client.db(dbName).collection("isodow_sunday");
      // June 18, 2023 was a Sunday
      await collection.insertOne({ date: new Date("2023-06-18T00:00:00Z") });

      const results = await collection
        .aggregate([{ $project: { result: { $isoDayOfWeek: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, 7);
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("isodow_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([{ $project: { result: { $isoDayOfWeek: "$date" }, _id: 0 } }])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$dateAdd", () => {
    it("should add days to date", async () => {
      const collection = client.db(dbName).collection("dateadd_days");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateAdd: { startDate: "$date", unit: "day", amount: 5 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-06-20T00:00:00.000Z"
      );
    });

    it("should add months to date", async () => {
      const collection = client.db(dbName).collection("dateadd_months");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateAdd: { startDate: "$date", unit: "month", amount: 2 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-08-15T00:00:00.000Z"
      );
    });

    it("should add years to date", async () => {
      const collection = client.db(dbName).collection("dateadd_years");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateAdd: { startDate: "$date", unit: "year", amount: 1 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2024-06-15T00:00:00.000Z"
      );
    });

    it("should add hours to date", async () => {
      const collection = client.db(dbName).collection("dateadd_hours");
      await collection.insertOne({ date: new Date("2023-06-15T10:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateAdd: { startDate: "$date", unit: "hour", amount: 3 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-06-15T13:00:00.000Z"
      );
    });

    it("should add minutes to date", async () => {
      const collection = client.db(dbName).collection("dateadd_minutes");
      await collection.insertOne({ date: new Date("2023-06-15T10:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateAdd: { startDate: "$date", unit: "minute", amount: 45 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-06-15T10:45:00.000Z"
      );
    });

    it("should handle negative amounts (subtract)", async () => {
      const collection = client.db(dbName).collection("dateadd_negative");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateAdd: { startDate: "$date", unit: "day", amount: -5 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-06-10T00:00:00.000Z"
      );
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("dateadd_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateAdd: { startDate: "$date", unit: "day", amount: 5 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$dateSubtract", () => {
    it("should subtract days from date", async () => {
      const collection = client.db(dbName).collection("datesub_days");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateSubtract: { startDate: "$date", unit: "day", amount: 5 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-06-10T00:00:00.000Z"
      );
    });

    it("should subtract months from date", async () => {
      const collection = client.db(dbName).collection("datesub_months");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateSubtract: { startDate: "$date", unit: "month", amount: 2 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-04-15T00:00:00.000Z"
      );
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("datesub_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateSubtract: { startDate: "$date", unit: "day", amount: 5 } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$dateDiff", () => {
    it("should calculate difference in days", async () => {
      const collection = client.db(dbName).collection("datediff_days");
      await collection.insertOne({
        start: new Date("2023-06-10T00:00:00Z"),
        end: new Date("2023-06-15T00:00:00Z"),
      });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateDiff: { startDate: "$start", endDate: "$end", unit: "day" },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, 5);
    });

    it("should calculate difference in months", async () => {
      const collection = client.db(dbName).collection("datediff_months");
      await collection.insertOne({
        start: new Date("2023-01-15T00:00:00Z"),
        end: new Date("2023-06-15T00:00:00Z"),
      });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateDiff: { startDate: "$start", endDate: "$end", unit: "month" },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, 5);
    });

    it("should calculate difference in years", async () => {
      const collection = client.db(dbName).collection("datediff_years");
      await collection.insertOne({
        start: new Date("2020-06-15T00:00:00Z"),
        end: new Date("2023-06-15T00:00:00Z"),
      });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateDiff: { startDate: "$start", endDate: "$end", unit: "year" },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, 3);
    });

    it("should return negative for reversed dates", async () => {
      const collection = client.db(dbName).collection("datediff_negative");
      await collection.insertOne({
        start: new Date("2023-06-15T00:00:00Z"),
        end: new Date("2023-06-10T00:00:00Z"),
      });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateDiff: { startDate: "$start", endDate: "$end", unit: "day" },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, -5);
    });

    it("should calculate difference in hours", async () => {
      const collection = client.db(dbName).collection("datediff_hours");
      await collection.insertOne({
        start: new Date("2023-06-15T10:00:00Z"),
        end: new Date("2023-06-15T15:00:00Z"),
      });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateDiff: { startDate: "$start", endDate: "$end", unit: "hour" },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, 5);
    });

    it("should return null if either date is null", async () => {
      const collection = client.db(dbName).collection("datediff_null");
      await collection.insertOne({
        start: new Date("2023-06-15T00:00:00Z"),
        end: null,
      });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateDiff: { startDate: "$start", endDate: "$end", unit: "day" },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$dateFromParts", () => {
    it("should construct date from parts", async () => {
      const collection = client.db(dbName).collection("datefromparts_basic");
      await collection.insertOne({});

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateFromParts: {
                  year: 2023,
                  month: 6,
                  day: 15,
                  hour: 14,
                  minute: 30,
                  second: 45,
                },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-06-15T14:30:45.000Z"
      );
    });

    it("should use default values for optional parts", async () => {
      const collection = client.db(dbName).collection("datefromparts_defaults");
      await collection.insertOne({});

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateFromParts: { year: 2023, month: 6, day: 15 },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-06-15T00:00:00.000Z"
      );
    });

    it("should handle ISO week format", async () => {
      const collection = client.db(dbName).collection("datefromparts_iso");
      await collection.insertOne({});

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateFromParts: { isoWeekYear: 2023, isoWeek: 24, isoDayOfWeek: 4 },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      // ISO week 24 of 2023, Thursday (day 4)
      assert.ok(results[0].result instanceof Date);
    });

    it("should return null for null year", async () => {
      const collection = client.db(dbName).collection("datefromparts_null");
      await collection.insertOne({ y: null });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateFromParts: { year: "$y", month: 6, day: 15 },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });

  describe("$dateToParts", () => {
    it("should extract parts from date", async () => {
      const collection = client.db(dbName).collection("datetoparts_basic");
      await collection.insertOne({ date: new Date("2023-06-15T14:30:45.123Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              parts: { $dateToParts: { date: "$date" } },
              _id: 0,
            },
          },
        ])
        .toArray();

      const parts = results[0].parts as Record<string, number>;
      assert.strictEqual(parts.year, 2023);
      assert.strictEqual(parts.month, 6);
      assert.strictEqual(parts.day, 15);
      assert.strictEqual(parts.hour, 14);
      assert.strictEqual(parts.minute, 30);
      assert.strictEqual(parts.second, 45);
      assert.strictEqual(parts.millisecond, 123);
    });

    it("should extract ISO parts when iso8601 is true", async () => {
      const collection = client.db(dbName).collection("datetoparts_iso");
      await collection.insertOne({ date: new Date("2023-06-15T00:00:00Z") });

      const results = await collection
        .aggregate([
          {
            $project: {
              parts: { $dateToParts: { date: "$date", iso8601: true } },
              _id: 0,
            },
          },
        ])
        .toArray();

      const parts = results[0].parts as Record<string, number>;
      assert.ok(parts.isoWeekYear !== undefined);
      assert.ok(parts.isoWeek !== undefined);
      assert.ok(parts.isoDayOfWeek !== undefined);
    });

    it("should return null for null date", async () => {
      const collection = client.db(dbName).collection("datetoparts_null");
      await collection.insertOne({ date: null });

      const results = await collection
        .aggregate([
          {
            $project: {
              parts: { $dateToParts: { date: "$date" } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].parts, null);
    });
  });

  describe("$dateFromString", () => {
    it("should parse ISO date string", async () => {
      const collection = client.db(dbName).collection("datefromstring_iso");
      await collection.insertOne({ dateStr: "2023-06-15T14:30:45Z" });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateFromString: { dateString: "$dateStr" } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.ok(results[0].result instanceof Date);
      assert.strictEqual(
        (results[0].result as Date).toISOString(),
        "2023-06-15T14:30:45.000Z"
      );
    });

    it("should parse date-only ISO format", async () => {
      const collection = client.db(dbName).collection("datefromstring_dateonly");
      await collection.insertOne({ dateStr: "2023-06-15" });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateFromString: { dateString: "$dateStr" } },
              _id: 0,
            },
          },
        ])
        .toArray();

      const date = results[0].result as Date;
      assert.ok(date instanceof Date);
      assert.strictEqual(date.getUTCFullYear(), 2023);
      assert.strictEqual(date.getUTCMonth(), 5); // June = 5 (0-indexed)
      assert.strictEqual(date.getUTCDate(), 15);
    });

    it("should return onNull value for null string", async () => {
      const collection = client.db(dbName).collection("datefromstring_onnull");
      await collection.insertOne({ dateStr: null });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateFromString: {
                  dateString: "$dateStr",
                  onNull: "default-date",
                },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, "default-date");
    });

    it("should return onError value for invalid string", async () => {
      const collection = client.db(dbName).collection("datefromstring_onerror");
      await collection.insertOne({ dateStr: "not-a-date" });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: {
                $dateFromString: {
                  dateString: "$dateStr",
                  onError: "error-fallback",
                },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, "error-fallback");
    });

    it("should throw error for invalid string without onError", async () => {
      const collection = client.db(dbName).collection("datefromstring_error");
      await collection.insertOne({ dateStr: "invalid" });

      await assert.rejects(
        collection
          .aggregate([
            {
              $project: {
                result: { $dateFromString: { dateString: "$dateStr" } },
                _id: 0,
              },
            },
          ])
          .toArray(),
        (err: Error) => {
          // MangoDB uses "Cannot parse date from string", MongoDB uses "Error parsing date string"
          return err.message.includes("parse date") || err.message.includes("parsing date");
        }
      );
    });

    it("should return null for null string without onNull", async () => {
      const collection = client.db(dbName).collection("datefromstring_null");
      await collection.insertOne({ dateStr: null });

      const results = await collection
        .aggregate([
          {
            $project: {
              result: { $dateFromString: { dateString: "$dateStr" } },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results[0].result, null);
    });
  });
});
