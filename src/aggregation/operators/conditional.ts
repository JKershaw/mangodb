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
