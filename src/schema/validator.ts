/**
 * JSON Schema validator for MongoDB's $jsonSchema query operator.
 *
 * Implements JSON Schema draft 4 validation with MongoDB's BSON extensions.
 */

import type { MongoJSONSchema } from './types.ts';
import {
  BSON_TYPE_ALIASES,
  NUMBER_TYPE_CODES,
  getBSONTypeCode,
  matchesBSONType,
} from '../query-matcher.ts';
import { valuesEqual } from '../document-utils.ts';

/**
 * Map JSON Schema types to BSON types.
 */
const JSON_TO_BSON_TYPE: Record<string, string> = {
  string: 'string',
  number: 'number', // matches int, long, double, decimal
  integer: 'int',
  boolean: 'bool',
  object: 'object',
  array: 'array',
  null: 'null',
};

/**
 * Validate a document against a MongoDB JSON Schema.
 *
 * @param doc - The document to validate
 * @param schema - The JSON Schema to validate against
 * @returns true if the document matches the schema
 */
export function validateDocument(doc: unknown, schema: MongoJSONSchema): boolean {
  // Metadata fields are ignored (title, description)

  // Type validation (bsonType or type)
  if (schema.bsonType !== undefined) {
    if (!validateBsonType(doc, schema.bsonType)) {
      return false;
    }
  }

  if (schema.type !== undefined) {
    if (!validateJsonSchemaType(doc, schema.type)) {
      return false;
    }
  }

  // Enum validation
  if (schema.enum !== undefined) {
    if (!validateEnum(doc, schema.enum)) {
      return false;
    }
  }

  // Logical composition
  if (schema.allOf !== undefined) {
    if (!schema.allOf.every((subSchema) => validateDocument(doc, subSchema))) {
      return false;
    }
  }

  if (schema.anyOf !== undefined) {
    if (!schema.anyOf.some((subSchema) => validateDocument(doc, subSchema))) {
      return false;
    }
  }

  if (schema.oneOf !== undefined) {
    const matchCount = schema.oneOf.filter((subSchema) =>
      validateDocument(doc, subSchema)
    ).length;
    if (matchCount !== 1) {
      return false;
    }
  }

  if (schema.not !== undefined) {
    if (validateDocument(doc, schema.not)) {
      return false;
    }
  }

  // Object-specific validation
  if (isPlainObject(doc)) {
    if (!validateObjectConstraints(doc, schema)) {
      return false;
    }
  }

  // Array-specific validation
  if (Array.isArray(doc)) {
    if (!validateArrayConstraints(doc, schema)) {
      return false;
    }
  }

  // String-specific validation
  if (typeof doc === 'string') {
    if (!validateStringConstraints(doc, schema)) {
      return false;
    }
  }

  // Numeric-specific validation
  if (typeof doc === 'number') {
    if (!validateNumericConstraints(doc, schema)) {
      return false;
    }
  }

  return true;
}

/**
 * Check if value is a plain object (not array, not null, not special type).
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    !(value instanceof Date) &&
    !(value instanceof RegExp)
  );
}

/**
 * Validate bsonType constraint.
 */
function validateBsonType(value: unknown, bsonType: string | string[]): boolean {
  const types = Array.isArray(bsonType) ? bsonType : [bsonType];

  // Value must match at least one of the specified types
  return types.some((type) => {
    // Handle "number" specially - it matches int, long, double, decimal
    if (type === 'number') {
      const valueType = getBSONTypeCode(value);
      return NUMBER_TYPE_CODES.has(valueType);
    }

    // Check if type is valid
    if (!(type in BSON_TYPE_ALIASES)) {
      throw new Error(`Unknown bsonType: ${type}`);
    }

    return matchesBSONType(value, type);
  });
}

/**
 * Validate JSON Schema type constraint.
 */
function validateJsonSchemaType(value: unknown, type: string | string[]): boolean {
  const types = Array.isArray(type) ? type : [type];

  return types.some((t) => {
    const bsonType = JSON_TO_BSON_TYPE[t];
    if (!bsonType) {
      throw new Error(`Unknown JSON Schema type: ${t}`);
    }

    // "number" in JSON Schema matches any numeric type
    if (t === 'number') {
      const valueType = getBSONTypeCode(value);
      return NUMBER_TYPE_CODES.has(valueType);
    }

    // "integer" specifically checks for int type
    if (t === 'integer') {
      return typeof value === 'number' && Number.isInteger(value);
    }

    return matchesBSONType(value, bsonType);
  });
}

/**
 * Validate enum constraint.
 */
function validateEnum(value: unknown, enumValues: unknown[]): boolean {
  return enumValues.some((enumVal) => valuesEqual(value, enumVal));
}

/**
 * Validate object-specific constraints.
 */
function validateObjectConstraints(
  obj: Record<string, unknown>,
  schema: MongoJSONSchema
): boolean {
  const objKeys = Object.keys(obj);

  // required validation
  if (schema.required !== undefined) {
    // MongoDB rejects empty required arrays
    if (schema.required.length === 0) {
      throw new Error("$jsonSchema keyword 'required' cannot be an empty array");
    }
    for (const field of schema.required) {
      if (!(field in obj)) {
        return false;
      }
    }
  }

  // minProperties / maxProperties
  if (schema.minProperties !== undefined && objKeys.length < schema.minProperties) {
    return false;
  }
  if (schema.maxProperties !== undefined && objKeys.length > schema.maxProperties) {
    return false;
  }

  // properties validation
  if (schema.properties !== undefined) {
    for (const [propName, propSchema] of Object.entries(schema.properties)) {
      if (propName in obj) {
        if (!validateDocument(obj[propName], propSchema)) {
          return false;
        }
      }
    }
  }

  // patternProperties validation
  if (schema.patternProperties !== undefined) {
    for (const [pattern, propSchema] of Object.entries(schema.patternProperties)) {
      const regex = new RegExp(pattern);
      for (const key of objKeys) {
        if (regex.test(key)) {
          if (!validateDocument(obj[key], propSchema)) {
            return false;
          }
        }
      }
    }
  }

  // additionalProperties validation
  if (schema.additionalProperties !== undefined) {
    const declaredProps = new Set(Object.keys(schema.properties || {}));
    const patternRegexes = Object.keys(schema.patternProperties || {}).map(
      (p) => new RegExp(p)
    );

    for (const key of objKeys) {
      // Skip if property is declared in properties
      if (declaredProps.has(key)) continue;

      // Skip if property matches any patternProperties
      if (patternRegexes.some((regex) => regex.test(key))) continue;

      // This is an additional property
      if (schema.additionalProperties === false) {
        return false;
      } else if (typeof schema.additionalProperties === 'object') {
        if (!validateDocument(obj[key], schema.additionalProperties)) {
          return false;
        }
      }
      // additionalProperties: true allows any additional properties
    }
  }

  // dependencies validation
  if (schema.dependencies !== undefined) {
    for (const [prop, dep] of Object.entries(schema.dependencies)) {
      if (prop in obj) {
        if (Array.isArray(dep)) {
          // Property dependency: if prop exists, all deps must exist
          for (const depProp of dep) {
            if (!(depProp in obj)) {
              return false;
            }
          }
        } else {
          // Schema dependency: if prop exists, object must match schema
          if (!validateDocument(obj, dep)) {
            return false;
          }
        }
      }
    }
  }

  return true;
}

/**
 * Validate array-specific constraints.
 */
function validateArrayConstraints(arr: unknown[], schema: MongoJSONSchema): boolean {
  // minItems / maxItems
  if (schema.minItems !== undefined && arr.length < schema.minItems) {
    return false;
  }
  if (schema.maxItems !== undefined && arr.length > schema.maxItems) {
    return false;
  }

  // uniqueItems
  if (schema.uniqueItems === true) {
    for (let i = 0; i < arr.length; i++) {
      for (let j = i + 1; j < arr.length; j++) {
        if (valuesEqual(arr[i], arr[j])) {
          return false;
        }
      }
    }
  }

  // items validation
  if (schema.items !== undefined) {
    if (Array.isArray(schema.items)) {
      // Tuple validation: each position has its own schema
      for (let i = 0; i < schema.items.length && i < arr.length; i++) {
        if (!validateDocument(arr[i], schema.items[i])) {
          return false;
        }
      }

      // additionalItems validation for items beyond the tuple
      if (schema.additionalItems !== undefined && arr.length > schema.items.length) {
        for (let i = schema.items.length; i < arr.length; i++) {
          if (schema.additionalItems === false) {
            return false;
          } else if (typeof schema.additionalItems === 'object') {
            if (!validateDocument(arr[i], schema.additionalItems)) {
              return false;
            }
          }
        }
      }
    } else {
      // All items must match the schema
      for (const item of arr) {
        if (!validateDocument(item, schema.items)) {
          return false;
        }
      }
    }
  }

  return true;
}

/**
 * Validate string-specific constraints.
 */
function validateStringConstraints(str: string, schema: MongoJSONSchema): boolean {
  // minLength / maxLength (character count, not bytes)
  if (schema.minLength !== undefined && str.length < schema.minLength) {
    return false;
  }
  if (schema.maxLength !== undefined && str.length > schema.maxLength) {
    return false;
  }

  // pattern validation
  if (schema.pattern !== undefined) {
    const regex = new RegExp(schema.pattern);
    if (!regex.test(str)) {
      return false;
    }
  }

  return true;
}

/**
 * Validate numeric-specific constraints.
 */
function validateNumericConstraints(num: number, schema: MongoJSONSchema): boolean {
  // Handle NaN - typically fails numeric constraints
  if (Number.isNaN(num)) {
    return false;
  }

  // minimum / maximum with exclusive variants
  if (schema.minimum !== undefined) {
    if (schema.exclusiveMinimum === true) {
      if (num <= schema.minimum) return false;
    } else {
      if (num < schema.minimum) return false;
    }
  }

  if (schema.maximum !== undefined) {
    if (schema.exclusiveMaximum === true) {
      if (num >= schema.maximum) return false;
    } else {
      if (num > schema.maximum) return false;
    }
  }

  // multipleOf validation
  if (schema.multipleOf !== undefined) {
    // Use modulo with tolerance for floating point precision
    const remainder = num % schema.multipleOf;
    const tolerance = 1e-10;
    if (Math.abs(remainder) > tolerance && Math.abs(remainder - schema.multipleOf) > tolerance) {
      return false;
    }
  }

  return true;
}
