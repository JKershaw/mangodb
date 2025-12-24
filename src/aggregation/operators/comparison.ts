/**
 * Comparison expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";
import { compareValues } from "../../document-utils.ts";

export function evalGt(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) > 0;
}

export function evalGte(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) >= 0;
}

export function evalLt(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) < 0;
}

export function evalLte(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) <= 0;
}

export function evalEq(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) === 0;
}

export function evalNe(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): boolean {
  const [left, right] = args.map((a) => evaluate(a, doc, vars));
  return compareValues(left, right) !== 0;
}
