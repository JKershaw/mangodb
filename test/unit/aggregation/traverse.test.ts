/**
 * Unit tests for document traversal utilities.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { traverseDocument } from '../../../src/aggregation/traverse.ts';

describe('Document Traversal Utilities', () => {
  describe('traverseDocument', () => {
    it('should return document as-is when callback returns keep', () => {
      const doc = { a: 1, b: { c: 2 } };
      const result = traverseDocument(doc, () => 'keep');

      assert.deepStrictEqual(result, doc);
    });

    it('should return null when callback returns prune', () => {
      const doc = { a: 1 };
      const result = traverseDocument(doc, () => 'prune');

      assert.strictEqual(result, null);
    });

    it('should recurse into nested documents when callback returns descend', () => {
      const doc = {
        level: 1,
        secret: true,
        nested: {
          level: 2,
          secret: false,
        },
      };

      // Prune any sub-document where secret is true
      const result = traverseDocument(doc, (subdoc) => {
        if (subdoc.secret === true) {
          return 'prune';
        }
        return 'descend';
      });

      // Outer doc has secret=true, so entire doc is pruned
      assert.strictEqual(result, null);
    });

    it('should keep nested doc when callback returns descend and nested passes', () => {
      const doc = {
        level: 1,
        secret: false,
        nested: {
          level: 2,
          secret: false,
          data: 'visible',
        },
      };

      const result = traverseDocument(doc, (subdoc) => {
        if (subdoc.secret === true) {
          return 'prune';
        }
        return 'descend';
      });

      assert.deepStrictEqual(result, doc);
    });

    it('should prune nested documents that fail callback', () => {
      const doc = {
        level: 1,
        secret: false,
        nested: {
          level: 2,
          secret: true,
        },
      };

      const result = traverseDocument(doc, (subdoc) => {
        if (subdoc.secret === true) {
          return 'prune';
        }
        return 'descend';
      });

      // Nested is pruned, but top level remains without nested field
      assert.deepStrictEqual(result, { level: 1, secret: false });
    });

    it('should handle arrays of documents', () => {
      const doc = {
        items: [
          { value: 1, public: true },
          { value: 2, public: false },
          { value: 3, public: true },
        ],
      };

      const result = traverseDocument(doc, (subdoc) => {
        if (subdoc.public === false) {
          return 'prune';
        }
        return 'descend';
      });

      assert.deepStrictEqual(result, {
        items: [
          { value: 1, public: true },
          { value: 3, public: true },
        ],
      });
    });

    it('should keep scalar array elements when descending', () => {
      const doc = {
        tags: ['a', 'b', 'c'],
        nested: { data: 123 },
      };

      const result = traverseDocument(doc, () => 'descend');

      assert.deepStrictEqual(result, doc);
    });

    it('should handle deeply nested structures', () => {
      const doc = {
        a: {
          b: {
            c: {
              secret: true,
            },
          },
        },
      };

      const result = traverseDocument(doc, (subdoc) => {
        if (subdoc.secret === true) {
          return 'prune';
        }
        return 'descend';
      });

      assert.deepStrictEqual(result, { a: { b: {} } });
    });

    it('should preserve Date objects in documents', () => {
      const now = new Date();
      const doc = { created: now, nested: { updated: now } };

      const result = traverseDocument(doc, () => 'descend');

      assert.strictEqual(result!.created, now);
      assert.strictEqual((result!.nested as { updated: Date }).updated, now);
    });
  });
});
