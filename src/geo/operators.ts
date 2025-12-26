/**
 * Geospatial operator implementations for MangoDB.
 * These functions are used by query-matcher.ts and collection.ts.
 */

import type { Position, GeoJSONGeometry } from "./geometry.ts";
import { extractCoordinates, isValidGeoJSON, normalizePoint } from "./geometry.ts";
import {
  haversineDistance,
  euclideanDistance,
  geometryContainsPoint,
  geometriesIntersect,
} from "./calculations.ts";
import {
  parseGeoShape,
  parseNearQuery,
  pointWithinShape,
  validateGeoWithinShape,
  validateGeoIntersectsShape,
  type GeoShape,
  type NearQuery,
} from "./shapes.ts";
import { InvalidGeoJSONError } from "./errors.ts";

/**
 * Extract a point from a document field value.
 * Supports GeoJSON Point and legacy coordinate formats.
 *
 * @param value The document field value
 * @returns Position [lng, lat] or null if not a valid point
 */
export function extractPointFromDocument(value: unknown): Position | null {
  return extractCoordinates(value);
}

/**
 * Extract a full geometry from a document field value.
 * Returns GeoJSON geometry or converts legacy point to GeoJSON Point.
 *
 * @param value The document field value
 * @returns GeoJSONGeometry or null if not valid
 */
export function extractGeometryFromDocument(value: unknown): GeoJSONGeometry | null {
  // If it's already valid GeoJSON, return it
  if (isValidGeoJSON(value)) {
    return value;
  }

  // Try to extract as a point and convert to GeoJSON Point
  const point = extractCoordinates(value);
  if (point) {
    return {
      type: "Point",
      coordinates: point,
    };
  }

  return null;
}

/**
 * Evaluate a $geoWithin query operator.
 *
 * @param docValue The document field value to test
 * @param opValue The $geoWithin operator value (shape specifier)
 * @returns true if the document point is within the shape
 */
export function evaluateGeoWithin(docValue: unknown, opValue: unknown): boolean {
  // Validate and parse the shape specifier
  const shape = validateGeoWithinShape(opValue);

  // Extract the point from the document
  const point = extractPointFromDocument(docValue);
  if (!point) {
    return false;
  }

  return pointWithinShape(point, shape);
}

/**
 * Evaluate a $geoIntersects query operator.
 *
 * @param docValue The document field value to test
 * @param opValue The $geoIntersects operator value (must have $geometry)
 * @returns true if the geometries intersect
 */
export function evaluateGeoIntersects(docValue: unknown, opValue: unknown): boolean {
  // Validate $geoIntersects - must have $geometry
  const shape = validateGeoIntersectsShape(opValue);

  // Extract geometry from document
  const docGeometry = extractGeometryFromDocument(docValue);
  if (!docGeometry) {
    return false;
  }

  return geometriesIntersect(docGeometry, shape.$geometry);
}

/**
 * Result of a $near/$nearSphere evaluation including distance.
 */
export interface NearResult {
  matches: boolean;
  distance: number;
}

/**
 * Evaluate a $near query operator.
 * Note: This is used by collection.ts, not query-matcher.ts,
 * because $near requires sorting results by distance.
 *
 * @param docValue The document field value to test
 * @param nearSpec The $near operator specification
 * @param spherical Whether to use spherical (true) or planar (false) geometry
 * @returns NearResult with match status and distance
 */
export function evaluateNear(
  docValue: unknown,
  nearSpec: unknown,
  spherical: boolean
): NearResult {
  // Parse the near query
  const nearQuery = parseNearQuery(nearSpec);
  if (!nearQuery) {
    throw new InvalidGeoJSONError("$near requires a valid point");
  }

  // Extract point from document
  const docPoint = extractPointFromDocument(docValue);
  if (!docPoint) {
    return { matches: false, distance: Infinity };
  }

  // Calculate distance
  const distance = spherical
    ? haversineDistance(nearQuery.point, docPoint)
    : euclideanDistance(nearQuery.point, docPoint);

  // Check distance constraints
  if (nearQuery.maxDistance !== undefined && distance > nearQuery.maxDistance) {
    return { matches: false, distance };
  }
  if (nearQuery.minDistance !== undefined && distance < nearQuery.minDistance) {
    return { matches: false, distance };
  }

  return { matches: true, distance };
}

/**
 * Extract $near/$nearSphere query from a filter object.
 * Returns the geo field name, query spec, and remaining filter.
 */
export interface ExtractedNearQuery {
  geoField: string;
  nearSpec: unknown;
  spherical: boolean;
  remainingFilter: Record<string, unknown>;
}

export function extractNearQuery(
  filter: Record<string, unknown>
): ExtractedNearQuery | null {
  for (const [field, value] of Object.entries(filter)) {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      continue;
    }

    const fieldOps = value as Record<string, unknown>;

    // Check for $near
    if ("$near" in fieldOps) {
      const remaining = { ...filter };
      delete remaining[field];

      // Check for $maxDistance/$minDistance at field level
      const nearSpec: Record<string, unknown> = {};
      if (isPlainGeoJSONOrArray(fieldOps.$near)) {
        nearSpec.$geometry = fieldOps.$near;
      } else {
        Object.assign(nearSpec, fieldOps.$near);
      }

      if ("$maxDistance" in fieldOps && !("$maxDistance" in nearSpec)) {
        nearSpec.$maxDistance = fieldOps.$maxDistance;
      }
      if ("$minDistance" in fieldOps && !("$minDistance" in nearSpec)) {
        nearSpec.$minDistance = fieldOps.$minDistance;
      }

      return {
        geoField: field,
        nearSpec,
        spherical: false,
        remainingFilter: remaining,
      };
    }

    // Check for $nearSphere
    if ("$nearSphere" in fieldOps) {
      const remaining = { ...filter };
      delete remaining[field];

      const nearSpec: Record<string, unknown> = {};
      if (isPlainGeoJSONOrArray(fieldOps.$nearSphere)) {
        nearSpec.$geometry = fieldOps.$nearSphere;
      } else {
        Object.assign(nearSpec, fieldOps.$nearSphere);
      }

      if ("$maxDistance" in fieldOps && !("$maxDistance" in nearSpec)) {
        nearSpec.$maxDistance = fieldOps.$maxDistance;
      }
      if ("$minDistance" in fieldOps && !("$minDistance" in nearSpec)) {
        nearSpec.$minDistance = fieldOps.$minDistance;
      }

      return {
        geoField: field,
        nearSpec,
        spherical: true,
        remainingFilter: remaining,
      };
    }
  }

  return null;
}

/**
 * Check if a filter contains a $near or $nearSphere operator.
 */
export function hasNearQuery(filter: Record<string, unknown>): boolean {
  return extractNearQuery(filter) !== null;
}

/**
 * Check if value is a plain GeoJSON point or coordinate array (not wrapped in $geometry).
 */
function isPlainGeoJSONOrArray(value: unknown): boolean {
  if (Array.isArray(value)) return true;
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;
  return obj.type === "Point" && !("$geometry" in obj);
}

/**
 * Calculate distance between two points.
 *
 * @param p1 First point [lng, lat]
 * @param p2 Second point [lng, lat]
 * @param spherical Use spherical (true) or planar (false) geometry
 * @returns Distance (meters for spherical, coordinate units for planar)
 */
export function calculateDistance(p1: Position, p2: Position, spherical: boolean): number {
  return spherical ? haversineDistance(p1, p2) : euclideanDistance(p1, p2);
}

/**
 * Validate that a value is a valid point for $near/$geoNear.
 * Throws an error if invalid.
 */
export function validateNearPoint(value: unknown): Position {
  const point = extractCoordinates(value);
  if (!point) {
    throw new InvalidGeoJSONError("$near requires a valid point");
  }
  return point;
}

/**
 * Get the default geo index field from a collection's indexes.
 * Used by $geoNear when 'key' is not specified.
 */
export function getGeoFieldFromIndexes(
  indexes: Array<{ key: Record<string, unknown> }>
): string | null {
  for (const index of indexes) {
    for (const [field, type] of Object.entries(index.key)) {
      if (type === "2d" || type === "2dsphere") {
        return field;
      }
    }
  }
  return null;
}
