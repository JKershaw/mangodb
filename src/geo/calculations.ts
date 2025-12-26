/**
 * Geospatial calculation functions for MangoDB.
 * All calculations implemented from scratch without external libraries.
 */

import type { Position, GeoJSONGeometry, GeoJSONPolygon, LinearRing } from "./geometry.ts";

// Earth radius in meters (WGS84 mean radius)
export const EARTH_RADIUS_METERS = 6378100;

// Conversion constants
export const DEGREES_TO_RADIANS = Math.PI / 180;
export const RADIANS_TO_DEGREES = 180 / Math.PI;

/**
 * Calculate the Haversine distance between two points on a sphere.
 * Used for 2dsphere indexes (Earth-like geometry).
 *
 * @param p1 First point [lng, lat] in degrees
 * @param p2 Second point [lng, lat] in degrees
 * @returns Distance in meters
 */
export function haversineDistance(p1: Position, p2: Position): number {
  const lat1Rad = p1[1] * DEGREES_TO_RADIANS;
  const lat2Rad = p2[1] * DEGREES_TO_RADIANS;
  const deltaLat = (p2[1] - p1[1]) * DEGREES_TO_RADIANS;
  const deltaLng = (p2[0] - p1[0]) * DEGREES_TO_RADIANS;

  const a =
    Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.sin(deltaLng / 2) * Math.sin(deltaLng / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return EARTH_RADIUS_METERS * c;
}

/**
 * Calculate Euclidean (planar) distance between two points.
 * Used for 2d indexes (flat geometry).
 *
 * @param p1 First point [x, y]
 * @param p2 Second point [x, y]
 * @returns Distance (same units as input)
 */
export function euclideanDistance(p1: Position, p2: Position): number {
  const dx = p2[0] - p1[0];
  const dy = p2[1] - p1[1];
  return Math.sqrt(dx * dx + dy * dy);
}

/**
 * Check if a point is inside a polygon using ray casting algorithm.
 * Works for simple polygons (no self-intersections).
 *
 * @param point The point to test [x, y]
 * @param ring The polygon ring (array of points, should be closed)
 * @returns true if point is inside the polygon
 */
export function pointInRing(point: Position, ring: LinearRing): boolean {
  const x = point[0];
  const y = point[1];
  let inside = false;
  const n = ring.length;

  for (let i = 0, j = n - 1; i < n; j = i++) {
    const xi = ring[i][0];
    const yi = ring[i][1];
    const xj = ring[j][0];
    const yj = ring[j][1];

    // Check if the ray from point crosses this edge
    const intersect = yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi;

    if (intersect) {
      inside = !inside;
    }
  }

  return inside;
}

/**
 * Check if a point is inside a polygon (with holes).
 *
 * @param point The point to test
 * @param polygon GeoJSON Polygon (exterior ring + optional holes)
 * @returns true if point is inside the polygon and not in any holes
 */
export function pointInPolygon(point: Position, polygon: GeoJSONPolygon): boolean {
  const rings = polygon.coordinates;

  // Must be inside exterior ring
  if (!pointInRing(point, rings[0])) {
    return false;
  }

  // Must not be inside any hole
  for (let i = 1; i < rings.length; i++) {
    if (pointInRing(point, rings[i])) {
      return false;
    }
  }

  return true;
}

/**
 * Check if a point is on or very close to a line segment.
 *
 * @param point The point to test
 * @param lineStart Start of line segment
 * @param lineEnd End of line segment
 * @param tolerance Distance tolerance
 * @returns true if point is within tolerance of the line segment
 */
export function pointOnSegment(
  point: Position,
  lineStart: Position,
  lineEnd: Position,
  tolerance: number = 1e-10
): boolean {
  return pointToSegmentDistance(point, lineStart, lineEnd) <= tolerance;
}

/**
 * Calculate the minimum distance from a point to a line segment.
 *
 * @param point The point
 * @param lineStart Start of line segment
 * @param lineEnd End of line segment
 * @returns Minimum distance to the line segment
 */
export function pointToSegmentDistance(
  point: Position,
  lineStart: Position,
  lineEnd: Position
): number {
  const px = point[0];
  const py = point[1];
  const ax = lineStart[0];
  const ay = lineStart[1];
  const bx = lineEnd[0];
  const by = lineEnd[1];

  // Vector from A to B
  const abx = bx - ax;
  const aby = by - ay;

  // Vector from A to P
  const apx = px - ax;
  const apy = py - ay;

  // Dot products
  const abab = abx * abx + aby * aby;
  const apab = apx * abx + apy * aby;

  // Handle degenerate case where segment is a point
  if (abab === 0) {
    return euclideanDistance(point, lineStart);
  }

  // Projection parameter (clamped to [0, 1])
  const t = Math.max(0, Math.min(1, apab / abab));

  // Closest point on segment
  const closestX = ax + t * abx;
  const closestY = ay + t * aby;

  return euclideanDistance(point, [closestX, closestY]);
}

/**
 * Calculate the minimum distance from a point to a polygon.
 * Returns 0 if the point is inside the polygon.
 *
 * @param point The point
 * @param polygon The polygon
 * @returns Minimum distance to the polygon
 */
export function pointToPolygonDistance(point: Position, polygon: GeoJSONPolygon): number {
  // If point is inside, distance is 0
  if (pointInPolygon(point, polygon)) {
    return 0;
  }

  // Find minimum distance to any edge
  let minDistance = Infinity;

  for (const ring of polygon.coordinates) {
    for (let i = 0; i < ring.length - 1; i++) {
      const dist = pointToSegmentDistance(point, ring[i], ring[i + 1]);
      if (dist < minDistance) {
        minDistance = dist;
      }
    }
  }

  return minDistance;
}

/**
 * Check if two line segments intersect.
 *
 * @param p1 Start of first segment
 * @param p2 End of first segment
 * @param p3 Start of second segment
 * @param p4 End of second segment
 * @returns true if segments intersect
 */
export function segmentsIntersect(p1: Position, p2: Position, p3: Position, p4: Position): boolean {
  const d1 = direction(p3, p4, p1);
  const d2 = direction(p3, p4, p2);
  const d3 = direction(p1, p2, p3);
  const d4 = direction(p1, p2, p4);

  if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
    return true;
  }

  if (d1 === 0 && onSegment(p3, p4, p1)) return true;
  if (d2 === 0 && onSegment(p3, p4, p2)) return true;
  if (d3 === 0 && onSegment(p1, p2, p3)) return true;
  if (d4 === 0 && onSegment(p1, p2, p4)) return true;

  return false;
}

/**
 * Calculate the cross product direction.
 */
function direction(pi: Position, pj: Position, pk: Position): number {
  return (pk[0] - pi[0]) * (pj[1] - pi[1]) - (pj[0] - pi[0]) * (pk[1] - pi[1]);
}

/**
 * Check if point pk is on segment pi-pj.
 */
function onSegment(pi: Position, pj: Position, pk: Position): boolean {
  return (
    Math.min(pi[0], pj[0]) <= pk[0] &&
    pk[0] <= Math.max(pi[0], pj[0]) &&
    Math.min(pi[1], pj[1]) <= pk[1] &&
    pk[1] <= Math.max(pi[1], pj[1])
  );
}

/**
 * Check if a line segment intersects a polygon.
 */
export function segmentIntersectsPolygon(
  segStart: Position,
  segEnd: Position,
  polygon: GeoJSONPolygon
): boolean {
  // Check if either endpoint is inside the polygon
  if (pointInPolygon(segStart, polygon) || pointInPolygon(segEnd, polygon)) {
    return true;
  }

  // Check if segment intersects any polygon edge
  for (const ring of polygon.coordinates) {
    for (let i = 0; i < ring.length - 1; i++) {
      if (segmentsIntersect(segStart, segEnd, ring[i], ring[i + 1])) {
        return true;
      }
    }
  }

  return false;
}

/**
 * Check if two polygons intersect.
 */
export function polygonsIntersect(poly1: GeoJSONPolygon, poly2: GeoJSONPolygon): boolean {
  // Check if any vertex of poly1 is inside poly2
  for (const ring of poly1.coordinates) {
    for (const point of ring) {
      if (pointInPolygon(point, poly2)) {
        return true;
      }
    }
  }

  // Check if any vertex of poly2 is inside poly1
  for (const ring of poly2.coordinates) {
    for (const point of ring) {
      if (pointInPolygon(point, poly1)) {
        return true;
      }
    }
  }

  // Check if any edges intersect
  for (const ring1 of poly1.coordinates) {
    for (let i = 0; i < ring1.length - 1; i++) {
      for (const ring2 of poly2.coordinates) {
        for (let j = 0; j < ring2.length - 1; j++) {
          if (segmentsIntersect(ring1[i], ring1[i + 1], ring2[j], ring2[j + 1])) {
            return true;
          }
        }
      }
    }
  }

  return false;
}

/**
 * Check if two bounding boxes intersect.
 * Each bbox is [minLng, minLat, maxLng, maxLat].
 */
export function bboxIntersects(
  bbox1: [number, number, number, number],
  bbox2: [number, number, number, number]
): boolean {
  return !(
    bbox1[2] < bbox2[0] || // bbox1 is left of bbox2
    bbox1[0] > bbox2[2] || // bbox1 is right of bbox2
    bbox1[3] < bbox2[1] || // bbox1 is below bbox2
    bbox1[1] > bbox2[3]
  ); // bbox1 is above bbox2
}

/**
 * Check if a point is within a bounding box.
 */
export function pointInBbox(point: Position, bbox: [number, number, number, number]): boolean {
  return point[0] >= bbox[0] && point[0] <= bbox[2] && point[1] >= bbox[1] && point[1] <= bbox[3];
}

/**
 * Check if a point is within a circle (planar geometry).
 *
 * @param point The point to test
 * @param center Center of the circle
 * @param radius Radius of the circle (same units as coordinates)
 * @returns true if point is within the circle
 */
export function pointInCircle(point: Position, center: Position, radius: number): boolean {
  return euclideanDistance(point, center) <= radius;
}

/**
 * Check if a point is within a spherical cap (circle on Earth's surface).
 *
 * @param point The point to test [lng, lat]
 * @param center Center of the circle [lng, lat]
 * @param radiusRadians Radius in radians
 * @returns true if point is within the spherical cap
 */
export function pointInSphericalCircle(
  point: Position,
  center: Position,
  radiusRadians: number
): boolean {
  const radiusMeters = radiusRadians * EARTH_RADIUS_METERS;
  return haversineDistance(point, center) <= radiusMeters;
}

/**
 * Convert radians to meters on Earth's surface.
 */
export function radiansToMeters(radians: number): number {
  return radians * EARTH_RADIUS_METERS;
}

/**
 * Convert meters to radians on Earth's surface.
 */
export function metersToRadians(meters: number): number {
  return meters / EARTH_RADIUS_METERS;
}

/**
 * Check if a geometry contains a point.
 */
export function geometryContainsPoint(geometry: GeoJSONGeometry, point: Position): boolean {
  switch (geometry.type) {
    case "Point":
      // Point contains point only if they're the same
      return geometry.coordinates[0] === point[0] && geometry.coordinates[1] === point[1];

    case "LineString":
      // Check if point is on any segment of the line
      for (let i = 0; i < geometry.coordinates.length - 1; i++) {
        if (pointOnSegment(point, geometry.coordinates[i], geometry.coordinates[i + 1])) {
          return true;
        }
      }
      return false;

    case "Polygon":
      return pointInPolygon(point, geometry);

    case "MultiPoint":
      return geometry.coordinates.some((p) => p[0] === point[0] && p[1] === point[1]);

    case "MultiLineString":
      for (const line of geometry.coordinates) {
        for (let i = 0; i < line.length - 1; i++) {
          if (pointOnSegment(point, line[i], line[i + 1])) {
            return true;
          }
        }
      }
      return false;

    case "MultiPolygon":
      for (const polyCoords of geometry.coordinates) {
        const poly: GeoJSONPolygon = { type: "Polygon", coordinates: polyCoords };
        if (pointInPolygon(point, poly)) {
          return true;
        }
      }
      return false;

    case "GeometryCollection":
      return geometry.geometries.some((g) => geometryContainsPoint(g, point));

    default:
      return false;
  }
}

/**
 * Check if two geometries intersect.
 */
export function geometriesIntersect(geo1: GeoJSONGeometry, geo2: GeoJSONGeometry): boolean {
  // Handle GeometryCollection by checking all sub-geometries
  if (geo1.type === "GeometryCollection") {
    return geo1.geometries.some((g) => geometriesIntersect(g, geo2));
  }
  if (geo2.type === "GeometryCollection") {
    return geo2.geometries.some((g) => geometriesIntersect(geo1, g));
  }

  // Point vs anything
  if (geo1.type === "Point") {
    return geometryContainsPoint(geo2, geo1.coordinates);
  }
  if (geo2.type === "Point") {
    return geometryContainsPoint(geo1, geo2.coordinates);
  }

  // MultiPoint vs anything
  if (geo1.type === "MultiPoint") {
    return geo1.coordinates.some((p) => geometryContainsPoint(geo2, p));
  }
  if (geo2.type === "MultiPoint") {
    return geo2.coordinates.some((p) => geometryContainsPoint(geo1, p));
  }

  // Polygon vs Polygon
  if (geo1.type === "Polygon" && geo2.type === "Polygon") {
    return polygonsIntersect(geo1, geo2);
  }

  // MultiPolygon vs Polygon
  if (geo1.type === "MultiPolygon" && geo2.type === "Polygon") {
    return geo1.coordinates.some((polyCoords) =>
      polygonsIntersect({ type: "Polygon", coordinates: polyCoords }, geo2)
    );
  }
  if (geo1.type === "Polygon" && geo2.type === "MultiPolygon") {
    return geo2.coordinates.some((polyCoords) =>
      polygonsIntersect(geo1, { type: "Polygon", coordinates: polyCoords })
    );
  }

  // MultiPolygon vs MultiPolygon
  if (geo1.type === "MultiPolygon" && geo2.type === "MultiPolygon") {
    for (const coords1 of geo1.coordinates) {
      for (const coords2 of geo2.coordinates) {
        if (
          polygonsIntersect(
            { type: "Polygon", coordinates: coords1 },
            { type: "Polygon", coordinates: coords2 }
          )
        ) {
          return true;
        }
      }
    }
    return false;
  }

  // LineString vs Polygon
  if (geo1.type === "LineString" && geo2.type === "Polygon") {
    for (let i = 0; i < geo1.coordinates.length - 1; i++) {
      if (segmentIntersectsPolygon(geo1.coordinates[i], geo1.coordinates[i + 1], geo2)) {
        return true;
      }
    }
    return false;
  }
  if (geo1.type === "Polygon" && geo2.type === "LineString") {
    return geometriesIntersect(geo2, geo1);
  }

  // LineString vs LineString
  if (geo1.type === "LineString" && geo2.type === "LineString") {
    for (let i = 0; i < geo1.coordinates.length - 1; i++) {
      for (let j = 0; j < geo2.coordinates.length - 1; j++) {
        if (
          segmentsIntersect(
            geo1.coordinates[i],
            geo1.coordinates[i + 1],
            geo2.coordinates[j],
            geo2.coordinates[j + 1]
          )
        ) {
          return true;
        }
      }
    }
    return false;
  }

  // Handle MultiLineString
  if (geo1.type === "MultiLineString") {
    return geo1.coordinates.some((line) =>
      geometriesIntersect({ type: "LineString", coordinates: line }, geo2)
    );
  }
  if (geo2.type === "MultiLineString") {
    return geo2.coordinates.some((line) =>
      geometriesIntersect(geo1, { type: "LineString", coordinates: line })
    );
  }

  // Default: no intersection
  return false;
}
