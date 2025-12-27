/**
 * Geospatial-specific error classes for MangoDB.
 * Error codes and messages match MongoDB behavior.
 */

/**
 * Error thrown when a geo query is executed without a required geo index.
 * Matches MongoDB's error format.
 */
export class GeoIndexRequiredError extends Error {
  readonly code = 291;
  readonly codeName = 'IndexNotFound';

  constructor(operator: string) {
    super(`error processing query: ns=unknown: ` + `${operator} requires a 2d or 2dsphere index`);
    this.name = 'GeoIndexRequiredError';
  }
}

/**
 * Error thrown when a $geoNear query doesn't have a geospatial index.
 */
export class GeoNearIndexRequiredError extends Error {
  readonly code = 291;
  readonly codeName = 'IndexNotFound';

  constructor() {
    super('unable to find index for $geoNear query');
    this.name = 'GeoNearIndexRequiredError';
  }
}

/**
 * Error thrown when invalid GeoJSON is encountered.
 */
export class InvalidGeoJSONError extends Error {
  readonly code = 16755;
  readonly codeName = 'Location16755';

  constructor(message: string) {
    super(`Can't extract geo keys: ${message}`);
    this.name = 'InvalidGeoJSONError';
  }
}

/**
 * Error thrown when $geoNear is not the first pipeline stage.
 */
export class GeoNearNotFirstError extends Error {
  readonly code = 40602;
  readonly codeName = 'Location40602';

  constructor() {
    super('$geoNear is only valid as the first stage in an aggregation pipeline');
    this.name = 'GeoNearNotFirstError';
  }
}

/**
 * Error thrown when $geoNear is missing required fields.
 */
export class GeoNearMissingFieldError extends Error {
  readonly code = 40412;
  readonly codeName = 'Location40412';

  constructor(field: string) {
    super(`$geoNear requires a '${field}' option`);
    this.name = 'GeoNearMissingFieldError';
  }
}

/**
 * Error thrown when a $geoWithin shape specifier is invalid.
 */
export class InvalidGeoWithinError extends Error {
  readonly code = 2;
  readonly codeName = 'BadValue';

  constructor() {
    super(
      '$geoWithin not supported with provided geometry: ' +
        'requires a $geometry, $box, $polygon, $center, or $centerSphere'
    );
    this.name = 'InvalidGeoWithinError';
  }
}

/**
 * Error thrown when $geoIntersects is missing $geometry.
 */
export class InvalidGeoIntersectsError extends Error {
  readonly code = 2;
  readonly codeName = 'BadValue';

  constructor() {
    super('$geoIntersects requires a $geometry argument');
    this.name = 'InvalidGeoIntersectsError';
  }
}

/**
 * Error thrown when attempting to create multiple geo indexes on the same field.
 */
export class DuplicateGeoIndexError extends Error {
  readonly code = 16800;
  readonly codeName = 'Location16800';

  constructor(field: string) {
    super(`can't have 2 geo indexes on a single collection: already have ${field}`);
    this.name = 'DuplicateGeoIndexError';
  }
}

/**
 * Error thrown when coordinates are out of valid bounds.
 */
export class CoordinateOutOfBoundsError extends Error {
  readonly code = 16755;
  readonly codeName = 'Location16755';

  constructor(type: 'longitude' | 'latitude', value: number) {
    super(`Can't extract geo keys: ${type}/latitude is out of bounds, ${type}: ${value}`);
    this.name = 'CoordinateOutOfBoundsError';
  }
}
