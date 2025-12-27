/**
 * Conditional expression operators.
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";

export function evalCond(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
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

  const evalCondition = evaluate(condition, doc, vars);

  if (evalCondition) {
    return evaluate(thenValue, doc, vars);
  } else {
    return evaluate(elseValue, doc, vars);
  }
}

export function evalIfNull(
  args: unknown[],
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  for (const arg of args) {
    const value = evaluate(arg, doc, vars);
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
  vars: VariableContext | undefined,
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

    const caseResult = evaluate(branchObj.case, doc, vars);
    if (caseResult) {
      return evaluate(branchObj.then, doc, vars);
    }
  }

  if (spec.default !== undefined) {
    return evaluate(spec.default, doc, vars);
  }

  throw new Error("$switch could not find a matching branch, and no default was specified");
}

/**
 * $let - Binds variables for use within a specified expression.
 *
 * Syntax: { $let: { vars: { <var1>: <expr1>, ... }, in: <expression> } }
 *
 * The vars field defines variables. The in expression uses these variables
 * with $$varName syntax. Variables defined in $let can shadow outer variables.
 */
export function evalLet(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  if (typeof args !== "object" || args === null) {
    throw new Error("$let requires an object as an argument");
  }

  const spec = args as { vars?: unknown; in?: unknown };

  if (spec.vars === undefined) {
    throw new Error("Missing 'vars' parameter to $let");
  }

  if (spec.in === undefined) {
    throw new Error("Missing 'in' parameter to $let");
  }

  if (typeof spec.vars !== "object" || spec.vars === null || Array.isArray(spec.vars)) {
    throw new Error("'vars' argument to $let must be an object");
  }

  // Evaluate each variable definition and merge with existing vars
  const newVars: VariableContext = { ...vars };
  for (const [varName, varExpr] of Object.entries(spec.vars as Record<string, unknown>)) {
    // Variable names should not start with $ in the definition
    if (varName.startsWith("$")) {
      throw new Error(`Variable names cannot start with '$': ${varName}`);
    }
    newVars[varName] = evaluate(varExpr, doc, vars);
  }

  // Evaluate the 'in' expression with the new variables
  return evaluate(spec.in, doc, newVars);
}
