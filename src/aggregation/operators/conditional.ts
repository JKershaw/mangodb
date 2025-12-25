/**
 * Conditional expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";

export function evalCond(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  let condition: unknown;
  let thenValue: unknown;
  let elseValue: unknown;

  if (Array.isArray(args)) {
    [condition, thenValue, elseValue] = args;
  } else if (typeof args === "object" && args !== null) {
    const obj = args as { if: unknown; then: unknown; else: unknown };
    condition = obj.if;
    thenValue = obj.then;
    elseValue = obj.else;
  } else {
    throw new Error("$cond requires an array or object argument");
  }

  const evalCondition = evaluate(condition, doc);

  if (evalCondition) {
    return evaluate(thenValue, doc);
  } else {
    return evaluate(elseValue, doc);
  }
}

export function evalIfNull(
  args: unknown[],
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  for (const arg of args) {
    const value = evaluate(arg, doc);
    if (value !== null && value !== undefined) {
      return value;
    }
  }
  return null;
}

/**
 * $switch - Evaluates a series of case expressions and returns the value of
 * the first expression that evaluates to true.
 */
export function evalSwitch(
  args: unknown,
  doc: Document,
  _vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  if (typeof args !== "object" || args === null) {
    throw new Error("$switch requires an object as an argument");
  }

  const spec = args as { branches?: unknown[]; default?: unknown };

  if (!spec.branches || !Array.isArray(spec.branches)) {
    throw new Error("$switch requires 'branches' to be an array");
  }

  for (const branch of spec.branches) {
    if (typeof branch !== "object" || branch === null) {
      throw new Error("$switch requires each branch to be an object");
    }

    const branchObj = branch as { case?: unknown; then?: unknown };

    if (branchObj.case === undefined) {
      throw new Error("$switch found a branch without a 'case' expression");
    }

    if (branchObj.then === undefined) {
      throw new Error("$switch found a branch without a 'then' expression");
    }

    const caseResult = evaluate(branchObj.case, doc);
    if (caseResult) {
      return evaluate(branchObj.then, doc);
    }
  }

  if (spec.default !== undefined) {
    return evaluate(spec.default, doc);
  }

  throw new Error("$switch could not find a matching branch, and no default was specified");
}
