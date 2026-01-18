# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-01-18

### Added
- In-process mutex for concurrent write operations
- Concurrency tests for mutex validation

### Fixed
- Race condition causing data loss and performance degradation with concurrent writes to the same collection ([#46](https://github.com/JKershaw/mangodb/issues/46))

## [0.1.0] - 2024-12-27

### Added
- Initial release
- MongoDB-compatible API for file-based storage
- Query operators: 31/32 supported (all except `$where`)
- Update operators: 20/20 (100% coverage)
- Aggregation stages: 29/34 supported
- Expression operators: 121/127 supported
- Index types: single, compound, text, TTL, partial, 2d, 2dsphere, hashed, wildcard
- Geospatial queries with GeoJSON support
- Text search with weighted indexes
- Trigonometry expression operators (15 operators)
- `$jsonSchema` query operator
- `$merge` aggregation stage
- Comprehensive documentation
