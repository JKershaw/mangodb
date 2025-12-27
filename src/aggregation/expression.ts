/**
 * Expression evaluation for aggregation pipeline.
 */
import type { Document } from '../types.ts';
import type { VariableContext } from './types.ts';
import { getValueByPath } from '../utils.ts';
import { operators } from './operators/index.ts';

/**
 * Evaluate an aggregation expression against a document.
 *
 * Expressions can be:
 * - Field references: "$fieldName" or "$nested.field"
 * - Variable references: "$$varName" or "$$varName.field"
 * - Literal values: numbers, strings, booleans, null
 * - Operator expressions: { $add: [...] }, { $concat: [...] }, etc.
 */
export function evaluateExpression(expr: unknown, doc: Document, vars?: VariableContext): unknown {
  // String starting with $$ is a variable reference
  if (typeof expr === 'string' && expr.startsWith('$$')) {
    const varPath = expr.slice(2);
    const dotIndex = varPath.indexOf('.');
    if (dotIndex === -1) {
      return vars?.[varPath];
    } else {
      const varName = varPath.slice(0, dotIndex);
      const fieldPath = varPath.slice(dotIndex + 1);
      const varValue = vars?.[varName];
      if (varValue && typeof varValue === 'object') {
        return getValueByPath(varValue as Document, fieldPath);
      }
      return undefined;
    }
  }

  // String starting with $ is a field reference
  if (typeof expr === 'string' && expr.startsWith('$')) {
    const fieldPath = expr.slice(1);
    return getValueByPath(doc, fieldPath);
  }

  // Primitive values returned as-is
  if (expr === null || typeof expr !== 'object') {
    return expr;
  }

  // Arrays - evaluate each element
  if (Array.isArray(expr)) {
    return expr.map((item) => evaluateExpression(item, doc, vars));
  }

  // Object with operator key
  const exprObj = expr as Record<string, unknown>;
  const keys = Object.keys(exprObj);

  if (keys.length === 1 && keys[0].startsWith('$')) {
    const op = keys[0];
    const args = exprObj[op];

    // Handle $literal specially - return as-is without evaluation
    if (op === '$literal') {
      return args;
    }

    if (!(op in operators)) {
      throw new Error(`Unrecognized expression operator: '${op}'`);
    }

    const handler = operators[op as keyof typeof operators];
    return handler(args as never, doc, vars, evaluateExpression);
  }

  // Object literal - evaluate each field
  const result: Document = {};
  for (const [key, value] of Object.entries(exprObj)) {
    result[key] = evaluateExpression(value, doc, vars);
  }
  return result;
}
