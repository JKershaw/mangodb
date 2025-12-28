/**
 * Fuzz testing configuration.
 */

export const FUZZ_CONFIG = {
  // Number of test runs per property (default)
  numRuns: process.env.FUZZ_RUNS ? parseInt(process.env.FUZZ_RUNS, 10) : 100,

  // CI mode runs fewer iterations for faster feedback
  ciNumRuns: 25,

  // Timeout per test case in milliseconds
  timeout: 5000,

  // Enable verbose output for debugging
  verbose: process.env.FUZZ_VERBOSE === 'true',

  // Seed for reproducibility (optional - fast-check generates one if not set)
  seed: process.env.FUZZ_SEED ? parseInt(process.env.FUZZ_SEED, 10) : undefined,
};

/**
 * Check if running in CI environment.
 */
export function isCIMode(): boolean {
  return process.env.CI === 'true';
}

/**
 * Get the number of runs to use based on environment.
 */
export function getNumRuns(): number {
  return isCIMode() ? FUZZ_CONFIG.ciNumRuns : FUZZ_CONFIG.numRuns;
}
