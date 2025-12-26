/**
 * MangoDB Geospatial Module
 *
 * Provides MongoDB-compatible geospatial support including:
 * - Query operators: $geoWithin, $geoIntersects, $near, $nearSphere
 * - Aggregation stage: $geoNear
 * - Index types: 2d, 2dsphere
 */

// Geometry types and validation
export {
  type Position,
  type LinearRing,
  type GeoJSONPoint,
  type GeoJSONLineString,
  type GeoJSONPolygon,
  type GeoJSONMultiPoint,
  type GeoJSONMultiLineString,
  type GeoJSONMultiPolygon,
  type GeoJSONGeometryCollection,
  type GeoJSONGeometry,
  type LegacyPoint,
  isValidGeoJSON,
  isValidGeoJSONPoint,
  isValidGeoJSONLineString,
  isValidGeoJSONPolygon,
  isValidGeoJSONMultiPoint,
  isValidGeoJSONMultiLineString,
  isValidGeoJSONMultiPolygon,
  isValidGeoJSONGeometryCollection,
  isValidPosition,
  isValidLinearRing,
  isValidLegacyPoint,
  extractCoordinates,
  validateSphericalCoordinates,
  validateSphericalPosition,
  normalizePoint,
  getBoundingBox,
  getAllPositions,
} from "./geometry.ts";

// Distance and intersection calculations
export {
  EARTH_RADIUS_METERS,
  DEGREES_TO_RADIANS,
  RADIANS_TO_DEGREES,
  haversineDistance,
  euclideanDistance,
  pointInRing,
  pointInPolygon,
  pointOnSegment,
  pointToSegmentDistance,
  pointToPolygonDistance,
  segmentsIntersect,
  segmentIntersectsPolygon,
  polygonsIntersect,
  bboxIntersects,
  pointInBbox,
  pointInCircle,
  pointInSphericalCircle,
  radiansToMeters,
  metersToRadians,
  geometryContainsPoint,
  geometriesIntersect,
} from "./calculations.ts";

// Shape specifiers
export {
  type BoxShape,
  type CenterShape,
  type CenterSphereShape,
  type LegacyPolygonShape,
  type GeometryShape,
  type GeoShape,
  type NearQuery,
  isBoxShape,
  isCenterShape,
  isCenterSphereShape,
  isLegacyPolygonShape,
  isGeometryShape,
  parseGeoShape,
  pointInBox,
  pointInCenterCircle,
  pointInCenterSphere,
  pointInLegacyPolygon,
  pointInGeometry,
  pointWithinShape,
  getShapeType,
  validateGeoWithinShape,
  validateGeoIntersectsShape,
  parseNearQuery,
} from "./shapes.ts";

// Operator implementations
export {
  type NearResult,
  type ExtractedNearQuery,
  extractPointFromDocument,
  extractGeometryFromDocument,
  evaluateGeoWithin,
  evaluateGeoIntersects,
  evaluateNear,
  extractNearQuery,
  hasNearQuery,
  calculateDistance,
  validateNearPoint,
  getGeoFieldFromIndexes,
} from "./operators.ts";

// Error classes
export {
  GeoIndexRequiredError,
  GeoNearIndexRequiredError,
  InvalidGeoJSONError,
  GeoNearNotFirstError,
  GeoNearMissingFieldError,
  InvalidGeoWithinError,
  InvalidGeoIntersectsError,
  DuplicateGeoIndexError,
  CoordinateOutOfBoundsError,
} from "./errors.ts";
