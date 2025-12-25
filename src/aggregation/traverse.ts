/**
 * Recursive document traversal utilities for aggregation stages.
 *
 * Used by $redact to walk through nested documents and arrays,
 * applying access control logic at each level.
 */
import type { Document } from "../types.ts";

/**
 * Actions that can be taken during document traversal.
 */
export type TraversalAction = "descend" | "prune" | "keep";

/**
 * Recursively traverse a document, applying a callback at each level.
 *
 * @param doc - The document to traverse
 * @param callback - Function called for each document/sub-document
 *                   Returns "prune" to exclude, "keep" to include as-is,
 *                   or "descend" to recurse into nested content
 * @returns The processed document, or null if pruned
 */
export function traverseDocument(
  doc: Document,
  callback: (subdoc: Document) => TraversalAction
): Document | null {
  // Get the action for this document
  const action = callback(doc);

  if (action === "prune") {
    return null;
  }

  if (action === "keep") {
    return doc;
  }

  // action === "descend": recurse into embedded documents and arrays
  const result: Document = {};

  for (const [key, value] of Object.entries(doc)) {
    if (value === null || value === undefined) {
      result[key] = value;
    } else if (Array.isArray(value)) {
      // Process arrays: apply traversal to document elements, keep scalars
      const processedArray: unknown[] = [];
      for (const item of value) {
        if (
          item !== null &&
          typeof item === "object" &&
          !Array.isArray(item) &&
          !(item instanceof Date)
        ) {
          // Recurse into embedded documents in arrays
          const processed = traverseDocument(item as Document, callback);
          if (processed !== null) {
            processedArray.push(processed);
          }
          // If null, the item is pruned from the array
        } else {
          // Scalars and other non-document values are kept
          processedArray.push(item);
        }
      }
      result[key] = processedArray;
    } else if (
      typeof value === "object" &&
      !(value instanceof Date)
    ) {
      // Recurse into embedded documents
      const processed = traverseDocument(value as Document, callback);
      if (processed !== null) {
        result[key] = processed;
      }
      // If null, the field is omitted
    } else {
      // Scalars (numbers, strings, booleans, dates) are kept
      result[key] = value;
    }
  }

  return result;
}
