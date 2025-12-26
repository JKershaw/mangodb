/**
 * MongoDB shape specifiers for geospatial queries.
 * Handles $box, $center, $centerSphere, $polygon, $geometry.
 */

import type { Position, GeoJSONGeometry, GeoJSONPolygon, LinearRing } from "./geometry.ts";
import { isValidGeoJSON, extractCoordinates } from "./geometry.ts";
import {
  pointInCircle,
  pointInSphericalCircle,
  pointInPolygon,
  pointInBbox,
  geometryContainsPoint,
} from "./calculations.ts";

// Shape specifier types
export interface BoxShape {
  $box: [Position, Position]; // [bottomLeft, topRight]
}

export interface CenterShape {
  $center: [Position, number]; // [center, radius]
}

export interface CenterSphereShape {
  $centerSphere: [Position, number]; // [center, radiusInRadians]
}

export interface LegacyPolygonShape {
  $polygon: Position[]; // Array of points (not closed)
}

export interface GeometryShape {
  $geometry: GeoJSONGeometry;
}

export type GeoShape = BoxShape | CenterShape | CenterSphereShape | LegacyPolygonShape | GeometryShape;

/**
 * Check if value is a $box shape specifier.
 */
export function isBoxShape(value: unknown): value is BoxShape {
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;

  if (!Array.isArray(obj.$box) || obj.$box.length !== 2) return false;

  const [bottomLeft, topRight] = obj.$box;
  return (
    Array.isArray(bottomLeft) &&
    bottomLeft.length >= 2 &&
    typeof bottomLeft[0] === "number" &&
    typeof bottomLeft[1] === "number" &&
    Array.isArray(topRight) &&
    topRight.length >= 2 &&
    typeof topRight[0] === "number" &&
    typeof topRight[1] === "number"
  );
}

/**
 * Check if value is a $center shape specifier.
 */
export function isCenterShape(value: unknown): value is CenterShape {
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;

  if (!Array.isArray(obj.$center) || obj.$center.length !== 2) return false;

  const [center, radius] = obj.$center;
  return (
    Array.isArray(center) &&
    center.length >= 2 &&
    typeof center[0] === "number" &&
    typeof center[1] === "number" &&
    typeof radius === "number" &&
    radius >= 0
  );
}

/**
 * Check if value is a $centerSphere shape specifier.
 */
export function isCenterSphereShape(value: unknown): value is CenterSphereShape {
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;

  if (!Array.isArray(obj.$centerSphere) || obj.$centerSphere.length !== 2) return false;

  const [center, radius] = obj.$centerSphere;
  return (
    Array.isArray(center) &&
    center.length >= 2 &&
    typeof center[0] === "number" &&
    typeof center[1] === "number" &&
    typeof radius === "number" &&
    radius >= 0
  );
}

/**
 * Check if value is a $polygon shape specifier.
 */
export function isLegacyPolygonShape(value: unknown): value is LegacyPolygonShape {
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;

  if (!Array.isArray(obj.$polygon) || obj.$polygon.length < 3) return false;

  return obj.$polygon.every(
    (p) =>
      Array.isArray(p) &&
      p.length >= 2 &&
      typeof p[0] === "number" &&
      typeof p[1] === "number"
  );
}

/**
 * Check if value is a $geometry shape specifier.
 */
export function isGeometryShape(value: unknown): value is GeometryShape {
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;

  return isValidGeoJSON(obj.$geometry);
}

/**
 * Parse a geo shape specifier and return a normalized form.
 */
export function parseGeoShape(spec: unknown): GeoShape | null {
  if (!spec || typeof spec !== "object") return null;

  if (isGeometryShape(spec)) return spec;
  if (isBoxShape(spec)) return spec;
  if (isCenterShape(spec)) return spec;
  if (isCenterSphereShape(spec)) return spec;
  if (isLegacyPolygonShape(spec)) return spec;

  return null;
}

/**
 * Check if a point is within a $box.
 * $box: [[x1, y1], [x2, y2]] where [x1, y1] is bottom-left and [x2, y2] is top-right.
 */
export function pointInBox(point: Position, box: [Position, Position]): boolean {
  const [bottomLeft, topRight] = box;

  // Normalize box coordinates (handle inverted boxes)
  const minX = Math.min(bottomLeft[0], topRight[0]);
  const maxX = Math.max(bottomLeft[0], topRight[0]);
  const minY = Math.min(bottomLeft[1], topRight[1]);
  const maxY = Math.max(bottomLeft[1], topRight[1]);

  return pointInBbox(point, [minX, minY, maxX, maxY]);
}

/**
 * Check if a point is within a $center (planar circle).
 * $center: [[x, y], radius]
 */
export function pointInCenterCircle(point: Position, center: Position, radius: number): boolean {
  return pointInCircle(point, center, radius);
}

/**
 * Check if a point is within a $centerSphere (spherical circle).
 * $centerSphere: [[lng, lat], radiusInRadians]
 */
export function pointInCenterSphere(point: Position, center: Position, radiusRadians: number): boolean {
  return pointInSphericalCircle(point, center, radiusRadians);
}

/**
 * Check if a point is within a $polygon (legacy polygon format).
 * $polygon: [[x1, y1], [x2, y2], [x3, y3], ...]
 *
 * The legacy polygon format doesn't require the polygon to be closed.
 */
export function pointInLegacyPolygon(point: Position, vertices: Position[]): boolean {
  // Close the polygon if needed
  const ring: LinearRing =
    vertices[0][0] === vertices[vertices.length - 1][0] &&
    vertices[0][1] === vertices[vertices.length - 1][1]
      ? (vertices as LinearRing)
      : ([...vertices, vertices[0]] as LinearRing);

  // Convert to GeoJSON Polygon format
  const polygon: GeoJSONPolygon = {
    type: "Polygon",
    coordinates: [ring],
  };

  return pointInPolygon(point, polygon);
}

/**
 * Check if a point is within a $geometry (GeoJSON).
 */
export function pointInGeometry(point: Position, geometry: GeoJSONGeometry): boolean {
  return geometryContainsPoint(geometry, point);
}

/**
 * Check if a point is within any of the supported shape specifiers.
 */
export function pointWithinShape(point: Position, shape: GeoShape): boolean {
  if ("$geometry" in shape) {
    return pointInGeometry(point, shape.$geometry);
  }

  if ("$box" in shape) {
    return pointInBox(point, shape.$box);
  }

  if ("$center" in shape) {
    const [center, radius] = shape.$center;
    return pointInCenterCircle(point, center as Position, radius);
  }

  if ("$centerSphere" in shape) {
    const [center, radiusRadians] = shape.$centerSphere;
    return pointInCenterSphere(point, center as Position, radiusRadians);
  }

  if ("$polygon" in shape) {
    return pointInLegacyPolygon(point, shape.$polygon as Position[]);
  }

  return false;
}

/**
 * Get the shape specifier type for error messages.
 */
export function getShapeType(spec: unknown): string | null {
  if (!spec || typeof spec !== "object") return null;
  const obj = spec as Record<string, unknown>;

  if ("$geometry" in obj) return "$geometry";
  if ("$box" in obj) return "$box";
  if ("$center" in obj) return "$center";
  if ("$centerSphere" in obj) return "$centerSphere";
  if ("$polygon" in obj) return "$polygon";

  return null;
}

/**
 * Validate a $geoWithin shape specifier.
 * Throws an error if invalid.
 */
export function validateGeoWithinShape(spec: unknown): GeoShape {
  const shape = parseGeoShape(spec);

  if (!shape) {
    throw new Error(
      "$geoWithin not supported with provided geometry: requires a $geometry, $box, $polygon, $center, or $centerSphere"
    );
  }

  return shape;
}

/**
 * Validate a $geoIntersects shape specifier.
 * $geoIntersects only supports $geometry.
 * Throws an error if invalid.
 */
export function validateGeoIntersectsShape(spec: unknown): GeometryShape {
  if (!spec || typeof spec !== "object") {
    throw new Error("$geoIntersects requires a $geometry argument");
  }

  const obj = spec as Record<string, unknown>;

  if (!("$geometry" in obj)) {
    throw new Error("$geoIntersects requires a $geometry argument");
  }

  if (!isGeometryShape(spec)) {
    throw new Error("$geoIntersects requires a valid GeoJSON geometry");
  }

  return spec;
}

/**
 * Parse a $near or $nearSphere query specification.
 */
export interface NearQuery {
  point: Position;
  maxDistance?: number;
  minDistance?: number;
}

export function parseNearQuery(spec: unknown): NearQuery | null {
  if (!spec) return null;

  // Direct GeoJSON Point
  if (
    typeof spec === "object" &&
    !Array.isArray(spec) &&
    (spec as Record<string, unknown>).type === "Point"
  ) {
    const coords = extractCoordinates(spec);
    if (coords) {
      return { point: coords };
    }
  }

  // Legacy [lng, lat] array
  if (Array.isArray(spec) && spec.length >= 2) {
    const coords = extractCoordinates(spec);
    if (coords) {
      return { point: coords };
    }
  }

  // Object with $geometry and optional $maxDistance/$minDistance
  if (typeof spec === "object" && !Array.isArray(spec)) {
    const obj = spec as Record<string, unknown>;

    if (obj.$geometry) {
      const coords = extractCoordinates(obj.$geometry);
      if (coords) {
        return {
          point: coords,
          maxDistance: typeof obj.$maxDistance === "number" ? obj.$maxDistance : undefined,
          minDistance: typeof obj.$minDistance === "number" ? obj.$minDistance : undefined,
        };
      }
    }
  }

  return null;
}
