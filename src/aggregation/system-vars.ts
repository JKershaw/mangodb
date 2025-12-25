/**
 * System variables for aggregation expressions.
 *
 * MongoDB provides built-in system variables that can be referenced
 * using the $$ prefix (e.g., $$NOW, $$ROOT, $$DESCEND).
 */
import type { Document } from "../types.ts";
import type { VariableContext } from "./types.ts";

/**
 * Special marker object for $$REMOVE in projections.
 * When a field evaluates to this, the field is excluded from output.
 */
export const REMOVE_MARKER = Symbol("$$REMOVE");

/**
 * System variable string constants for $redact stage.
 */
export const REDACT_DESCEND = "descend";
export const REDACT_PRUNE = "prune";
export const REDACT_KEEP = "keep";

/**
 * Create system variables context for expression evaluation.
 *
 * @param doc - The current document being processed (for $$ROOT)
 * @param now - Optional fixed Date for $$NOW (defaults to new Date())
 * @returns Variable context with system variables
 */
export function createSystemVars(
  doc?: Document,
  now?: Date
): VariableContext {
  return {
    // $$NOW - Current datetime (fixed per aggregation pipeline)
    NOW: now ?? new Date(),

    // $$ROOT - The original root document
    ROOT: doc ?? {},

    // $$DESCEND - String constant for $redact
    DESCEND: REDACT_DESCEND,

    // $$PRUNE - String constant for $redact
    PRUNE: REDACT_PRUNE,

    // $$KEEP - String constant for $redact
    KEEP: REDACT_KEEP,

    // $$REMOVE - Special marker for field removal in projections
    REMOVE: REMOVE_MARKER,
  };
}

/**
 * Merge user-provided variables with system variables.
 * User variables take precedence over system variables.
 *
 * @param systemVars - System variables context
 * @param userVars - User-provided variables (from $let, etc.)
 * @returns Merged variable context
 */
export function mergeVars(
  systemVars: VariableContext,
  userVars?: VariableContext
): VariableContext {
  if (!userVars) {
    return systemVars;
  }
  return { ...systemVars, ...userVars };
}
