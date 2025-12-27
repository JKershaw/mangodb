/**
 * GeoJSON types and validation functions for MangoDB geospatial support.
 */

// GeoJSON coordinate types
export type Position = [number, number]; // [longitude, latitude]
export type LinearRing = Position[]; // Closed ring (first === last point)

// GeoJSON geometry types
export interface GeoJSONPoint {
  type: 'Point';
  coordinates: Position;
}

export interface GeoJSONLineString {
  type: 'LineString';
  coordinates: Position[];
}

export interface GeoJSONPolygon {
  type: 'Polygon';
  coordinates: LinearRing[]; // First is exterior, rest are holes
}

export interface GeoJSONMultiPoint {
  type: 'MultiPoint';
  coordinates: Position[];
}

export interface GeoJSONMultiLineString {
  type: 'MultiLineString';
  coordinates: Position[][];
}

export interface GeoJSONMultiPolygon {
  type: 'MultiPolygon';
  coordinates: LinearRing[][];
}

export interface GeoJSONGeometryCollection {
  type: 'GeometryCollection';
  geometries: GeoJSONGeometry[];
}

export type GeoJSONGeometry =
  | GeoJSONPoint
  | GeoJSONLineString
  | GeoJSONPolygon
  | GeoJSONMultiPoint
  | GeoJSONMultiLineString
  | GeoJSONMultiPolygon
  | GeoJSONGeometryCollection;

// Legacy coordinate formats
export type LegacyPoint = Position | { x: number; y: number } | { lng: number; lat: number };

/**
 * Check if a value is a valid GeoJSON geometry.
 */
export function isValidGeoJSON(value: unknown): value is GeoJSONGeometry {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }

  const obj = value as Record<string, unknown>;
  const type = obj.type;

  if (typeof type !== 'string') {
    return false;
  }

  switch (type) {
    case 'Point':
      return isValidGeoJSONPoint(obj);
    case 'LineString':
      return isValidGeoJSONLineString(obj);
    case 'Polygon':
      return isValidGeoJSONPolygon(obj);
    case 'MultiPoint':
      return isValidGeoJSONMultiPoint(obj);
    case 'MultiLineString':
      return isValidGeoJSONMultiLineString(obj);
    case 'MultiPolygon':
      return isValidGeoJSONMultiPolygon(obj);
    case 'GeometryCollection':
      return isValidGeoJSONGeometryCollection(obj);
    default:
      return false;
  }
}

/**
 * Validate a GeoJSON Point.
 */
export function isValidGeoJSONPoint(obj: unknown): boolean {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  const o = obj as Record<string, unknown>;
  if (o.type !== 'Point') return false;
  return isValidPosition(o.coordinates);
}

/**
 * Validate a GeoJSON LineString.
 */
export function isValidGeoJSONLineString(obj: unknown): boolean {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  const o = obj as Record<string, unknown>;
  if (o.type !== 'LineString') return false;
  const coords = o.coordinates;
  if (!Array.isArray(coords) || coords.length < 2) return false;
  return coords.every(isValidPosition);
}

/**
 * Validate a GeoJSON Polygon.
 */
export function isValidGeoJSONPolygon(obj: unknown): boolean {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  const o = obj as Record<string, unknown>;
  if (o.type !== 'Polygon') return false;
  const coords = o.coordinates;
  if (!Array.isArray(coords) || coords.length === 0) return false;

  for (const ring of coords) {
    if (!isValidLinearRing(ring)) return false;
  }
  return true;
}

/**
 * Validate a GeoJSON MultiPoint.
 */
export function isValidGeoJSONMultiPoint(obj: unknown): boolean {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  const o = obj as Record<string, unknown>;
  if (o.type !== 'MultiPoint') return false;
  const coords = o.coordinates;
  if (!Array.isArray(coords)) return false;
  return coords.every(isValidPosition);
}

/**
 * Validate a GeoJSON MultiLineString.
 */
export function isValidGeoJSONMultiLineString(obj: unknown): boolean {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  const o = obj as Record<string, unknown>;
  if (o.type !== 'MultiLineString') return false;
  const coords = o.coordinates;
  if (!Array.isArray(coords)) return false;

  for (const line of coords) {
    if (!Array.isArray(line) || line.length < 2) return false;
    if (!line.every(isValidPosition)) return false;
  }
  return true;
}

/**
 * Validate a GeoJSON MultiPolygon.
 */
export function isValidGeoJSONMultiPolygon(obj: unknown): boolean {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  const o = obj as Record<string, unknown>;
  if (o.type !== 'MultiPolygon') return false;
  const coords = o.coordinates;
  if (!Array.isArray(coords)) return false;

  for (const polygon of coords) {
    if (!Array.isArray(polygon) || polygon.length === 0) return false;
    for (const ring of polygon) {
      if (!isValidLinearRing(ring)) return false;
    }
  }
  return true;
}

/**
 * Validate a GeoJSON GeometryCollection.
 */
export function isValidGeoJSONGeometryCollection(obj: unknown): boolean {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  const o = obj as Record<string, unknown>;
  if (o.type !== 'GeometryCollection') return false;
  const geometries = o.geometries;
  if (!Array.isArray(geometries)) return false;
  return geometries.every(isValidGeoJSON);
}

/**
 * Check if a value is a valid Position [lng, lat].
 */
export function isValidPosition(value: unknown): value is Position {
  if (!Array.isArray(value) || value.length < 2) return false;
  const [lng, lat] = value;
  return typeof lng === 'number' && typeof lat === 'number' && isFinite(lng) && isFinite(lat);
}

/**
 * Check if a value is a valid LinearRing (closed polygon ring).
 */
export function isValidLinearRing(value: unknown): value is LinearRing {
  if (!Array.isArray(value) || value.length < 4) return false;
  if (!value.every(isValidPosition)) return false;

  // Ring must be closed (first point === last point)
  const first = value[0] as Position;
  const last = value[value.length - 1] as Position;
  return first[0] === last[0] && first[1] === last[1];
}

/**
 * Check if a value is a valid legacy point format.
 */
export function isValidLegacyPoint(value: unknown): value is LegacyPoint {
  // Array format: [x, y]
  if (Array.isArray(value)) {
    if (value.length < 2) return false;
    return (
      typeof value[0] === 'number' &&
      typeof value[1] === 'number' &&
      isFinite(value[0]) &&
      isFinite(value[1])
    );
  }

  // Object format: { x, y } or { lng, lat }
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>;

    // Check for { x, y }
    if (typeof obj.x === 'number' && typeof obj.y === 'number') {
      return isFinite(obj.x) && isFinite(obj.y);
    }

    // Check for { lng, lat }
    if (typeof obj.lng === 'number' && typeof obj.lat === 'number') {
      return isFinite(obj.lng) && isFinite(obj.lat);
    }
  }

  return false;
}

/**
 * Extract coordinates [lng, lat] from various point formats.
 * Returns null if the value is not a valid point.
 */
export function extractCoordinates(value: unknown): Position | null {
  // GeoJSON Point
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>;

    if (obj.type === 'Point' && isValidGeoJSONPoint(obj)) {
      return obj.coordinates as Position;
    }

    // Legacy { x, y } format
    if (typeof obj.x === 'number' && typeof obj.y === 'number') {
      if (isFinite(obj.x) && isFinite(obj.y)) {
        return [obj.x, obj.y];
      }
    }

    // Legacy { lng, lat } format
    if (typeof obj.lng === 'number' && typeof obj.lat === 'number') {
      if (isFinite(obj.lng) && isFinite(obj.lat)) {
        return [obj.lng, obj.lat];
      }
    }
  }

  // Array format: [lng, lat]
  if (Array.isArray(value) && value.length >= 2) {
    const [lng, lat] = value;
    if (typeof lng === 'number' && typeof lat === 'number' && isFinite(lng) && isFinite(lat)) {
      return [lng, lat];
    }
  }

  return null;
}

/**
 * Validate that coordinates are within valid bounds for spherical geometry.
 * Longitude: -180 to 180
 * Latitude: -90 to 90
 *
 * @throws Error if coordinates are out of bounds
 */
export function validateSphericalCoordinates(lng: number, lat: number): void {
  if (lng < -180 || lng > 180) {
    throw new Error(`Can't extract geo keys: longitude/latitude is out of bounds, lng: ${lng}`);
  }
  if (lat < -90 || lat > 90) {
    throw new Error(`Can't extract geo keys: longitude/latitude is out of bounds, lat: ${lat}`);
  }
}

/**
 * Validate that a position has valid spherical coordinates.
 *
 * @throws Error if coordinates are out of bounds
 */
export function validateSphericalPosition(pos: Position): void {
  validateSphericalCoordinates(pos[0], pos[1]);
}

/**
 * Normalize a point value to a Position [lng, lat].
 * Throws an error if the value is not a valid point.
 */
export function normalizePoint(value: unknown): Position {
  const coords = extractCoordinates(value);
  if (!coords) {
    if (!value || typeof value !== 'object') {
      throw new Error("Can't extract geo keys: Point must be an array or object");
    }
    throw new Error("Can't extract geo keys: Point must only contain numeric elements");
  }
  return coords;
}

/**
 * Get the bounding box of a geometry.
 * Returns [minLng, minLat, maxLng, maxLat].
 */
export function getBoundingBox(geometry: GeoJSONGeometry): [number, number, number, number] {
  const positions = getAllPositions(geometry);
  if (positions.length === 0) {
    return [0, 0, 0, 0];
  }

  let minLng = Infinity;
  let minLat = Infinity;
  let maxLng = -Infinity;
  let maxLat = -Infinity;

  for (const pos of positions) {
    if (pos[0] < minLng) minLng = pos[0];
    if (pos[0] > maxLng) maxLng = pos[0];
    if (pos[1] < minLat) minLat = pos[1];
    if (pos[1] > maxLat) maxLat = pos[1];
  }

  return [minLng, minLat, maxLng, maxLat];
}

/**
 * Get all positions from a geometry.
 */
export function getAllPositions(geometry: GeoJSONGeometry): Position[] {
  switch (geometry.type) {
    case 'Point':
      return [geometry.coordinates];
    case 'LineString':
    case 'MultiPoint':
      return geometry.coordinates;
    case 'Polygon':
    case 'MultiLineString':
      return geometry.coordinates.flat();
    case 'MultiPolygon':
      return geometry.coordinates.flat(2);
    case 'GeometryCollection':
      return geometry.geometries.flatMap(getAllPositions);
    default:
      return [];
  }
}
