/**
 * MongoDB JSON Schema types (JSON Schema draft 4 with BSON extensions).
 *
 * MongoDB's $jsonSchema operator uses a subset of JSON Schema draft 4,
 * extended with BSON-specific keywords like `bsonType`.
 */

/**
 * MongoDB JSON Schema definition.
 */
export interface MongoJSONSchema {
  // Type validation
  /** BSON type(s) the value must match */
  bsonType?: BSONTypeName | BSONTypeName[];
  /** JSON Schema type(s) - mapped to BSON types */
  type?: JSONSchemaType | JSONSchemaType[];

  // Object validation
  /** Schema for each property */
  properties?: Record<string, MongoJSONSchema>;
  /** Control validation of properties not in `properties` */
  additionalProperties?: boolean | MongoJSONSchema;
  /** Required property names */
  required?: string[];
  /** Minimum number of properties */
  minProperties?: number;
  /** Maximum number of properties */
  maxProperties?: number;
  /** Pattern-based property schemas */
  patternProperties?: Record<string, MongoJSONSchema>;
  /** Property dependencies */
  dependencies?: Record<string, string[] | MongoJSONSchema>;

  // Numeric validation
  /** Minimum value (inclusive by default) */
  minimum?: number;
  /** Maximum value (inclusive by default) */
  maximum?: number;
  /** Whether minimum is exclusive */
  exclusiveMinimum?: boolean;
  /** Whether maximum is exclusive */
  exclusiveMaximum?: boolean;
  /** Value must be a multiple of this */
  multipleOf?: number;

  // String validation
  /** Minimum string length */
  minLength?: number;
  /** Maximum string length */
  maxLength?: number;
  /** Regex pattern the string must match */
  pattern?: string;

  // Array validation
  /** Schema for array items */
  items?: MongoJSONSchema | MongoJSONSchema[];
  /** Schema for additional items when items is an array (tuple validation) */
  additionalItems?: boolean | MongoJSONSchema;
  /** Minimum array length */
  minItems?: number;
  /** Maximum array length */
  maxItems?: number;
  /** Whether array items must be unique */
  uniqueItems?: boolean;

  // Enum validation
  /** Allowed values */
  enum?: unknown[];

  // Logical composition
  /** Must match ALL schemas */
  allOf?: MongoJSONSchema[];
  /** Must match ANY schema */
  anyOf?: MongoJSONSchema[];
  /** Must match exactly ONE schema */
  oneOf?: MongoJSONSchema[];
  /** Must NOT match the schema */
  not?: MongoJSONSchema;

  // Metadata (ignored for validation)
  /** Schema title */
  title?: string;
  /** Schema description */
  description?: string;
}

/**
 * BSON type names supported by MongoDB's $jsonSchema.
 */
export type BSONTypeName =
  | 'double'
  | 'string'
  | 'object'
  | 'array'
  | 'binData'
  | 'undefined'
  | 'objectId'
  | 'bool'
  | 'date'
  | 'null'
  | 'regex'
  | 'javascript'
  | 'int'
  | 'timestamp'
  | 'long'
  | 'decimal'
  | 'minKey'
  | 'maxKey'
  | 'number'; // Alias for int, long, double, decimal

/**
 * Standard JSON Schema type names.
 */
export type JSONSchemaType =
  | 'string'
  | 'number'
  | 'integer'
  | 'boolean'
  | 'object'
  | 'array'
  | 'null';
