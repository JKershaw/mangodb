/**
 * Object operators - $getField, $setField, $mergeObjects
 */
import type { Document } from "../../types.ts";
import type { VariableContext, EvaluateExpressionFn } from "../types.ts";

/**
 * $getField - Returns the value of a specified field from a document.
 *
 * Syntax:
 *   { $getField: { field: <fieldName>, input: <document> } }
 *   { $getField: <fieldName> } // uses $$CURRENT as input
 */
export function evalGetField(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  let fieldName: string;
  let inputExpr: unknown = "$$CURRENT";

  if (typeof args === "string") {
    // Short form: { $getField: "fieldName" }
    // The string is the literal field name, not an expression
    fieldName = args;
  } else if (typeof args === "object" && args !== null && !Array.isArray(args)) {
    const spec = args as { field?: unknown; input?: unknown };
    if (spec.field === undefined) {
      throw new Error("$getField requires 'field' to be specified");
    }

    // Long form: field can be a literal string or an expression
    if (typeof spec.field === "string" && !spec.field.startsWith("$")) {
      fieldName = spec.field;
    } else {
      // Evaluate field expression (e.g., { $concat: [...] } or "$someField")
      const evaluatedField = evaluate(spec.field, doc, vars);
      if (typeof evaluatedField !== "string") {
        throw new Error("$getField 'field' must evaluate to a string");
      }
      fieldName = evaluatedField;
    }

    if (spec.input !== undefined) {
      inputExpr = spec.input;
    }
  } else {
    throw new Error("$getField requires a string or object argument");
  }

  // Evaluate input document
  const inputDoc = evaluate(inputExpr, doc, vars);
  if (inputDoc === null || inputDoc === undefined) {
    return null;
  }
  if (typeof inputDoc !== "object" || Array.isArray(inputDoc)) {
    throw new Error("$getField 'input' must be a document");
  }

  // Get the field value (supports fields with dots or $ in names)
  return (inputDoc as Record<string, unknown>)[fieldName];
}

/**
 * $setField - Adds, updates, or removes a specified field in a document.
 *
 * Syntax:
 *   { $setField: { field: <fieldName>, input: <document>, value: <value> } }
 *
 * To remove a field, set value to $$REMOVE.
 */
export function evalSetField(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  if (typeof args !== "object" || args === null || Array.isArray(args)) {
    throw new Error("$setField requires an object argument");
  }

  const spec = args as { field?: unknown; input?: unknown; value?: unknown };

  if (spec.field === undefined) {
    throw new Error("$setField requires 'field' to be specified");
  }
  if (spec.input === undefined) {
    throw new Error("$setField requires 'input' to be specified");
  }
  if (!("value" in spec)) {
    throw new Error("$setField requires 'value' to be specified");
  }

  // Evaluate field name
  const fieldName = evaluate(spec.field, doc, vars);
  if (typeof fieldName !== "string") {
    throw new Error("$setField 'field' must evaluate to a string");
  }

  // Evaluate input document
  const inputDoc = evaluate(spec.input, doc, vars);
  if (inputDoc === null || inputDoc === undefined) {
    return null;
  }
  if (typeof inputDoc !== "object" || Array.isArray(inputDoc)) {
    throw new Error("$setField 'input' must be a document");
  }

  // Evaluate value
  const value = evaluate(spec.value, doc, vars);

  // Create a shallow copy of the input document
  const result = { ...(inputDoc as Record<string, unknown>) };

  // Check for $$REMOVE
  if (value === vars?.["REMOVE"] || (typeof spec.value === "string" && spec.value === "$$REMOVE")) {
    delete result[fieldName];
  } else {
    result[fieldName] = value;
  }

  return result;
}

/**
 * $mergeObjects - Combines multiple documents into a single document.
 *
 * Syntax:
 *   { $mergeObjects: [<doc1>, <doc2>, ...] }
 *   { $mergeObjects: <doc> } // single document (often with $ifNull)
 */
export function evalMergeObjects(
  args: unknown,
  doc: Document,
  vars: VariableContext | undefined,
  evaluate: EvaluateExpressionFn
): unknown {
  // Handle single document argument
  let documents: unknown[];
  if (Array.isArray(args)) {
    documents = args;
  } else {
    documents = [args];
  }

  const result: Record<string, unknown> = {};

  for (const docExpr of documents) {
    const evaluated = evaluate(docExpr, doc, vars);

    // Null/undefined values are ignored
    if (evaluated === null || evaluated === undefined) {
      continue;
    }

    if (typeof evaluated !== "object" || Array.isArray(evaluated)) {
      throw new Error("$mergeObjects requires object operands");
    }

    // Merge fields from this document
    Object.assign(result, evaluated);
  }

  return result;
}
