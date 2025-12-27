# MangoDB

A file-based MongoDB drop-in replacement. See @README.md for overview.

## Commands

- `npm test` - Run tests against MangoDB
- `npm run test:mongodb` - Run tests against real MongoDB (reads from `.env.mongodb`)
- `npm run typecheck` - TypeScript type checking
- `npm run lint` - ESLint
- `npm run format` - Prettier formatting

## Environment

- `.env.mongodb` - Contains `MONGODB_URI` for running tests against real MongoDB (not committed)

## Development Workflow

- Create feature branches from `main` - never commit directly to main
- Run `npm test` before committing
- Run `npm run typecheck` and `npm run lint` to verify code quality
- Push branch and create PR for review

## Testing Requirements

- **Dual-target testing**: All tests must pass against both MangoDB and real MongoDB
- Write failing tests first (TDD)
- Test edge cases discovered in MongoDB behavior
- Use the test harness in `test/test-harness.ts` for client abstraction

## Code Style

- TypeScript strict mode
- Single quotes, trailing commas (Prettier enforced)
- Match MongoDB error messages and codes exactly
- No premature abstractions - keep it simple

## Architecture

- `src/collection.ts` - Main CRUD operations
- `src/query-matcher.ts` - Query filter matching
- `src/update-operators.ts` - Update operations
- `src/aggregation/` - Aggregation pipeline
- `src/geo/` - Geospatial operations
- Data stored as JSON: `dataDir/dbName/collection.json`

## MongoDB Compatibility

- Replicate MongoDB behavior exactly, including edge cases
- When unsure, write a test and run against real MongoDB
- Document any intentional deviations (there should be almost none)
