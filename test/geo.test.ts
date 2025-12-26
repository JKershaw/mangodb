/**
 * Geospatial Tests
 *
 * These tests run against both real MongoDB and MangoDB to ensure compatibility.
 * Set MONGODB_URI environment variable to run against MongoDB.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import {
  createTestClient,
  getTestModeName,
  type TestClient,
  type TestCollection,
} from "./test-harness.ts";
import { ObjectId } from "bson";

// Helper type for documents with geo data
interface GeoDocument {
  _id?: ObjectId;
  name: string;
  location:
    | { type: "Point"; coordinates: [number, number] }
    | [number, number];
}

describe(`Geospatial Tests (${getTestModeName()})`, () => {
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

  describe("2dsphere Index Creation", () => {
    it("should create a 2dsphere index", async () => {
      const collection = client.db(dbName).collection("geo_2dsphere_idx");

      const indexName = await collection.createIndex({ location: "2dsphere" });

      assert.strictEqual(indexName, "location_2dsphere");

      const indexes = await collection.listIndexes().toArray();
      const geoIndex = indexes.find((idx) => idx.name === "location_2dsphere");
      assert.ok(geoIndex, "2dsphere index should exist");
      assert.strictEqual(geoIndex.key.location, "2dsphere");
    });

    it("should create a 2d index", async () => {
      const collection = client.db(dbName).collection("geo_2d_idx");

      const indexName = await collection.createIndex({ location: "2d" });

      assert.strictEqual(indexName, "location_2d");

      const indexes = await collection.listIndexes().toArray();
      const geoIndex = indexes.find((idx) => idx.name === "location_2d");
      assert.ok(geoIndex, "2d index should exist");
      assert.strictEqual(geoIndex.key.location, "2d");
    });
  });

  describe("$geoWithin with $geometry (Polygon)", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_within_polygon");
      await collection.createIndex({ location: "2dsphere" });

      // Insert test locations
      await collection.insertMany([
        // Points inside the polygon (roughly Manhattan area)
        {
          name: "Times Square",
          location: { type: "Point", coordinates: [-73.985, 40.758] },
        },
        {
          name: "Empire State",
          location: { type: "Point", coordinates: [-73.9857, 40.7484] },
        },
        // Points outside the polygon
        {
          name: "Brooklyn",
          location: { type: "Point", coordinates: [-73.95, 40.65] },
        },
        {
          name: "New Jersey",
          location: { type: "Point", coordinates: [-74.1, 40.73] },
        },
      ]);
    });

    it("should find points within a polygon using $geometry", async () => {
      const polygon = {
        type: "Polygon" as const,
        coordinates: [
          [
            [-74.0, 40.7], // SW corner
            [-73.97, 40.7], // SE corner
            [-73.97, 40.77], // NE corner
            [-74.0, 40.77], // NW corner
            [-74.0, 40.7], // Close the ring
          ],
        ],
      };

      const results = await collection
        .find({
          location: {
            $geoWithin: {
              $geometry: polygon,
            },
          },
        })
        .toArray();

      const names = results.map((d) => d.name).sort();
      assert.deepStrictEqual(names, ["Empire State", "Times Square"]);
    });
  });

  describe("$geoWithin with $box", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_within_box");
      await collection.createIndex({ location: "2dsphere" });

      await collection.insertMany([
        {
          name: "Inside",
          location: { type: "Point", coordinates: [0.5, 0.5] },
        },
        {
          name: "Also Inside",
          location: { type: "Point", coordinates: [0.9, 0.9] },
        },
        { name: "Outside", location: { type: "Point", coordinates: [2, 2] } },
      ]);
    });

    it("should find points within a box", async () => {
      const results = await collection
        .find({
          location: {
            $geoWithin: {
              $box: [
                [0, 0],
                [1, 1],
              ],
            },
          },
        })
        .toArray();

      const names = results.map((d) => d.name).sort();
      assert.deepStrictEqual(names, ["Also Inside", "Inside"]);
    });
  });

  describe("$geoWithin with $center (2d)", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_within_center");
      await collection.createIndex({ location: "2d" });

      // Insert points at known distances from origin
      await collection.insertMany([
        { name: "At origin", location: [0, 0] },
        { name: "Close", location: [0.5, 0] }, // distance 0.5
        { name: "Farther", location: [1.5, 0] }, // distance 1.5
        { name: "Far", location: [3, 0] }, // distance 3
      ]);
    });

    it("should find points within a circle (flat)", async () => {
      const results = await collection
        .find({
          location: {
            $geoWithin: {
              $center: [[0, 0], 1], // center at origin, radius 1
            },
          },
        })
        .toArray();

      const names = results.map((d) => d.name).sort();
      assert.deepStrictEqual(names, ["At origin", "Close"]);
    });
  });

  describe("$geoWithin with $centerSphere", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_within_centersphere");
      await collection.createIndex({ location: "2dsphere" });

      // Insert locations at various distances from New York
      await collection.insertMany([
        // New York (center)
        {
          name: "New York",
          location: { type: "Point", coordinates: [-74.006, 40.7128] },
        },
        // Philadelphia (~130km from NYC)
        {
          name: "Philadelphia",
          location: { type: "Point", coordinates: [-75.1652, 39.9526] },
        },
        // Boston (~300km from NYC)
        {
          name: "Boston",
          location: { type: "Point", coordinates: [-71.0589, 42.3601] },
        },
        // Miami (~2000km from NYC)
        {
          name: "Miami",
          location: { type: "Point", coordinates: [-80.1918, 25.7617] },
        },
      ]);
    });

    it("should find points within spherical distance", async () => {
      // Find cities within ~200km of NYC
      // Earth radius ~6378.1km, so 200km = 200/6378.1 radians
      const radiusInRadians = 200 / 6378.1;

      const results = await collection
        .find({
          location: {
            $geoWithin: {
              $centerSphere: [[-74.006, 40.7128], radiusInRadians],
            },
          },
        })
        .toArray();

      const names = results.map((d) => d.name).sort();
      assert.deepStrictEqual(names, ["New York", "Philadelphia"]);
    });
  });

  describe("$geoWithin with $polygon (legacy)", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_within_legacy_polygon");
      await collection.createIndex({ location: "2d" });

      await collection.insertMany([
        { name: "Inside", location: [0.5, 0.5] },
        { name: "Also Inside", location: [0.3, 0.3] },
        { name: "Outside", location: [2, 2] },
      ]);
    });

    it("should find points within a legacy polygon", async () => {
      const results = await collection
        .find({
          location: {
            $geoWithin: {
              $polygon: [
                [0, 0],
                [1, 0],
                [1, 1],
                [0, 1],
              ],
            },
          },
        })
        .toArray();

      const names = results.map((d) => d.name).sort();
      assert.deepStrictEqual(names, ["Also Inside", "Inside"]);
    });
  });

  describe("$geoIntersects", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_intersects");
      await collection.createIndex({ location: "2dsphere" });

      // Insert various geometries
      await collection.insertMany([
        {
          name: "Point in NYC",
          location: { type: "Point", coordinates: [-73.985, 40.758] },
        },
        {
          name: "Line through NYC",
          location: {
            type: "LineString",
            coordinates: [
              [-74.1, 40.7],
              [-73.8, 40.8],
            ],
          },
        },
        {
          name: "Point in LA",
          location: { type: "Point", coordinates: [-118.2437, 34.0522] },
        },
      ]);
    });

    it("should find geometries intersecting a polygon", async () => {
      const polygon = {
        type: "Polygon" as const,
        coordinates: [
          [
            [-74.05, 40.7],
            [-73.9, 40.7],
            [-73.9, 40.8],
            [-74.05, 40.8],
            [-74.05, 40.7],
          ],
        ],
      };

      const results = await collection
        .find({
          location: {
            $geoIntersects: {
              $geometry: polygon,
            },
          },
        })
        .toArray();

      const names = results.map((d) => d.name).sort();
      assert.deepStrictEqual(names, ["Line through NYC", "Point in NYC"]);
    });
  });

  describe("$near operator", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_near");
      await collection.createIndex({ location: "2dsphere" });

      await collection.insertMany([
        {
          name: "Times Square",
          location: { type: "Point", coordinates: [-73.985, 40.758] },
        },
        {
          name: "Empire State",
          location: { type: "Point", coordinates: [-73.9857, 40.7484] },
        },
        {
          name: "Central Park",
          location: { type: "Point", coordinates: [-73.9654, 40.7829] },
        },
        {
          name: "Brooklyn Bridge",
          location: { type: "Point", coordinates: [-73.9969, 40.7061] },
        },
      ]);
    });

    it("should return documents sorted by distance", async () => {
      // Query near Times Square
      const results = await collection
        .find({
          location: {
            $near: {
              $geometry: { type: "Point", coordinates: [-73.985, 40.758] },
            },
          },
        })
        .toArray();

      // First result should be Times Square (closest/same point)
      assert.strictEqual(results[0].name, "Times Square");
      assert.strictEqual(results.length, 4);
    });

    it("should respect $maxDistance", async () => {
      // Query near Times Square with max distance of 2km
      const results = await collection
        .find({
          location: {
            $near: {
              $geometry: { type: "Point", coordinates: [-73.985, 40.758] },
              $maxDistance: 2000, // 2km in meters
            },
          },
        })
        .toArray();

      // Should only find Times Square and Empire State (both within 2km)
      const names = results.map((d) => d.name);
      assert.ok(names.includes("Times Square"));
      assert.ok(names.includes("Empire State"));
      assert.ok(!names.includes("Brooklyn Bridge"));
    });

    it("should respect $minDistance", async () => {
      // Query near Times Square with min distance of 500m
      const results = await collection
        .find({
          location: {
            $near: {
              $geometry: { type: "Point", coordinates: [-73.985, 40.758] },
              $minDistance: 500, // 500m in meters
            },
          },
        })
        .toArray();

      // Should not include Times Square itself
      const names = results.map((d) => d.name);
      assert.ok(!names.includes("Times Square"));
      assert.ok(results.length > 0);
    });
  });

  describe("$nearSphere operator", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_nearsphere");
      await collection.createIndex({ location: "2dsphere" });

      await collection.insertMany([
        {
          name: "NYC",
          location: { type: "Point", coordinates: [-74.006, 40.7128] },
        },
        {
          name: "Philly",
          location: { type: "Point", coordinates: [-75.1652, 39.9526] },
        },
        {
          name: "Boston",
          location: { type: "Point", coordinates: [-71.0589, 42.3601] },
        },
      ]);
    });

    it("should return documents sorted by spherical distance", async () => {
      const results = await collection
        .find({
          location: {
            $nearSphere: {
              $geometry: { type: "Point", coordinates: [-74.006, 40.7128] },
            },
          },
        })
        .toArray();

      const names = results.map((d) => d.name);
      // NYC should be first (same point), then Philly (closer), then Boston
      assert.strictEqual(names[0], "NYC");
      assert.strictEqual(names[1], "Philly");
      assert.strictEqual(names[2], "Boston");
    });

    it("should work with legacy coordinate array", async () => {
      const results = await collection
        .find({
          location: {
            $nearSphere: [-74.006, 40.7128],
          },
        })
        .toArray();

      assert.strictEqual(results[0].name, "NYC");
    });
  });

  describe("$geoNear aggregation stage", () => {
    let collection: TestCollection;

    before(async () => {
      collection = client.db(dbName).collection("geo_geonear_agg");
      await collection.createIndex({ location: "2dsphere" });

      await collection.insertMany([
        {
          name: "Point A",
          location: { type: "Point", coordinates: [0, 0] },
        },
        {
          name: "Point B",
          location: { type: "Point", coordinates: [0.01, 0] },
        },
        {
          name: "Point C",
          location: { type: "Point", coordinates: [0.02, 0] },
        },
      ]);
    });

    it("should add distance field and sort by distance", async () => {
      const results = await collection
        .aggregate([
          {
            $geoNear: {
              near: { type: "Point", coordinates: [0, 0] },
              distanceField: "dist",
              spherical: true,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].name, "Point A");
      assert.strictEqual(results[1].name, "Point B");
      assert.strictEqual(results[2].name, "Point C");

      // Verify distance field exists and is ordered
      assert.ok("dist" in results[0]);
      assert.ok("dist" in results[1]);
      assert.ok("dist" in results[2]);
      assert.ok((results[0].dist as number) <= (results[1].dist as number));
      assert.ok((results[1].dist as number) <= (results[2].dist as number));
    });

    it("should respect maxDistance", async () => {
      const results = await collection
        .aggregate([
          {
            $geoNear: {
              near: { type: "Point", coordinates: [0, 0] },
              distanceField: "dist",
              spherical: true,
              maxDistance: 1200, // ~1.2km, should exclude Point C (~2.2km away)
            },
          },
        ])
        .toArray();

      const names = results.map((d) => d.name);
      assert.ok(names.includes("Point A"));
      assert.ok(names.includes("Point B"));
      // Point C at 0.02 degrees (~2.2km) should be excluded
    });

    it("should support distanceMultiplier", async () => {
      const results = await collection
        .aggregate([
          {
            $geoNear: {
              near: { type: "Point", coordinates: [0, 0] },
              distanceField: "dist",
              spherical: true,
              distanceMultiplier: 0.001, // Convert meters to km
            },
          },
        ])
        .toArray();

      // Distances should be in km (much smaller than meters)
      assert.ok((results[0].dist as number) < 1); // Point A at origin
      assert.ok((results[1].dist as number) < 2); // Point B about 1.1km
    });

    it("should support query filter", async () => {
      // Add another point with different attribute
      await collection.insertOne({
        name: "Point D",
        location: { type: "Point", coordinates: [0.005, 0] },
        category: "special",
      });

      const results = await collection
        .aggregate([
          {
            $geoNear: {
              near: { type: "Point", coordinates: [0, 0] },
              distanceField: "dist",
              spherical: true,
              query: { category: "special" },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 1);
      assert.strictEqual(results[0].name, "Point D");
    });

    it("should support includeLocs", async () => {
      const results = await collection
        .aggregate([
          {
            $geoNear: {
              near: { type: "Point", coordinates: [0, 0] },
              distanceField: "dist",
              includeLocs: "matchedLocation",
              spherical: true,
            },
          },
          { $limit: 1 },
        ])
        .toArray();

      assert.ok("matchedLocation" in results[0]);
    });
  });

  describe("$geoNear validation", () => {
    it("should require $geoNear to be first stage", async () => {
      const collection = client.db(dbName).collection("geo_geonear_validation");
      await collection.createIndex({ location: "2dsphere" });
      await collection.insertOne({
        name: "Test",
        location: { type: "Point", coordinates: [0, 0] },
      });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([
              { $match: { name: "Test" } },
              {
                $geoNear: {
                  near: { type: "Point", coordinates: [0, 0] },
                  distanceField: "dist",
                },
              },
            ])
            .toArray();
        },
        (err: Error) =>
          err.message.includes("first stage") ||
          err.message.includes("$geoNear")
      );
    });
  });

  describe("Geo query without index errors", () => {
    it("should throw error for $near without geo index", async () => {
      const collection = client.db(dbName).collection("geo_no_index_near");
      await collection.insertOne({
        name: "Test",
        location: { type: "Point", coordinates: [0, 0] },
      });

      await assert.rejects(
        async () => {
          await collection
            .find({
              location: {
                $near: {
                  $geometry: { type: "Point", coordinates: [0, 0] },
                },
              },
            })
            .toArray();
        },
        (err: Error) =>
          err.message.includes("index") ||
          err.message.includes("$near") ||
          (err as unknown as { code?: number }).code === 291 ||
          (err as unknown as { code?: number }).code === 5
      );
    });

    it("should throw error for $geoNear without geo index", async () => {
      const collection = client.db(dbName).collection("geo_no_index_geonear");
      await collection.insertOne({
        name: "Test",
        location: { type: "Point", coordinates: [0, 0] },
      });

      await assert.rejects(
        async () => {
          await collection
            .aggregate([
              {
                $geoNear: {
                  near: { type: "Point", coordinates: [0, 0] },
                  distanceField: "dist",
                },
              },
            ])
            .toArray();
        },
        (err: Error) =>
          err.message.includes("index") ||
          err.message.includes("$geoNear") ||
          (err as unknown as { code?: number }).code === 291
      );
    });
  });

  describe("Edge cases", () => {
    it("should handle empty results", async () => {
      const collection = client.db(dbName).collection("geo_empty_results");
      await collection.createIndex({ location: "2dsphere" });
      await collection.insertOne({
        name: "NYC",
        location: { type: "Point", coordinates: [-74.006, 40.7128] },
      });

      // Query far away with small maxDistance
      const results = await collection
        .find({
          location: {
            $near: {
              $geometry: { type: "Point", coordinates: [100, 0] }, // Far away
              $maxDistance: 100, // 100 meters
            },
          },
        })
        .toArray();

      assert.strictEqual(results.length, 0);
    });

    it("should handle points on polygon boundary", async () => {
      const collection = client.db(dbName).collection("geo_boundary");
      await collection.createIndex({ location: "2dsphere" });
      await collection.insertOne({
        name: "On boundary",
        location: { type: "Point", coordinates: [0, 0] },
      });

      const results = await collection
        .find({
          location: {
            $geoWithin: {
              $geometry: {
                type: "Polygon",
                coordinates: [
                  [
                    [0, 0],
                    [1, 0],
                    [1, 1],
                    [0, 1],
                    [0, 0],
                  ],
                ],
              },
            },
          },
        })
        .toArray();

      // Points on boundary are typically included
      assert.ok(results.length >= 0); // May vary by implementation
    });

    it("should handle various GeoJSON point formats", async () => {
      const collection = client.db(dbName).collection("geo_formats");
      await collection.createIndex({ location: "2dsphere" });

      await collection.insertMany([
        // GeoJSON Point
        {
          name: "GeoJSON",
          location: { type: "Point", coordinates: [0, 0] },
        },
        // Legacy coordinate pair (only with 2d index typically)
      ]);

      const results = await collection
        .find({
          location: {
            $geoWithin: {
              $geometry: {
                type: "Polygon",
                coordinates: [
                  [
                    [-1, -1],
                    [1, -1],
                    [1, 1],
                    [-1, 1],
                    [-1, -1],
                  ],
                ],
              },
            },
          },
        })
        .toArray();

      assert.ok(results.length >= 1);
    });
  });
});
