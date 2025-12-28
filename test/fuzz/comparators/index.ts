/**
 * Re-export comparison utilities from fuzz-harness.
 *
 * The main comparison logic lives in fuzz-harness.ts.
 * This module exists for organizational consistency and to allow
 * future expansion of comparison utilities.
 */

export {
  compareResults,
  compareErrors,
  deepBsonEqual,
  reportDifference,
  type ComparisonResult,
} from '../fuzz-harness.ts';
