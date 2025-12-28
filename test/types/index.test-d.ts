/**
 * Type-level tests for MangoDB
 *
 * These tests verify that MangoDB's TypeScript types are correct and
 * compatible with the MongoDB driver types.
 *
 * Run with: npm run test:types
 */

import { expectType, expectAssignable, expectError } from 'tsd';
import {
  MangoClient,
  MangoDb,
  MangoCollection,
  MangoCursor,
  AggregationCursor,
} from '@jkershaw/mangodb';
import type { ObjectId } from 'bson';

// =============================================================================
// Test Document Types
// =============================================================================

interface User {
  _id?: ObjectId;
  name: string;
  email: string;
  age: number;
  tags?: string[];
  [key: string]: unknown;
}

interface Post {
  _id?: ObjectId;
  title: string;
  content: string;
  authorId: ObjectId;
  views: number;
  [key: string]: unknown;
}

// =============================================================================
// Basic Setup Tests
// =============================================================================

declare const client: MangoClient;
declare const db: MangoDb;
declare const users: MangoCollection<User>;
declare const posts: MangoCollection<Post>;

// Test that main classes are exported and have correct types
expectType<MangoDb>(client.db('test'));
expectType<MangoCollection<User>>(db.collection<User>('users'));

// =============================================================================
// MAN-28: Collection CRUD Type Tests
// =============================================================================

// -----------------------------------------------------------------------------
// insertOne
// -----------------------------------------------------------------------------

// Basic insert - should accept document matching collection type
async function testInsertOne() {
  const result = await users.insertOne({ name: 'Alice', email: 'alice@test.com', age: 30 });

  // Result should have acknowledged and insertedId
  expectType<boolean>(result.acknowledged);
  expectType<ObjectId>(result.insertedId);
}

// Insert with optional fields
async function testInsertOneWithOptionalFields() {
  const result = await users.insertOne({
    name: 'Bob',
    email: 'bob@test.com',
    age: 25,
    tags: ['developer', 'tester'],
  });
  expectType<ObjectId>(result.insertedId);
}

// -----------------------------------------------------------------------------
// insertMany
// -----------------------------------------------------------------------------

async function testInsertMany() {
  const result = await users.insertMany([
    { name: 'Alice', email: 'alice@test.com', age: 30 },
    { name: 'Bob', email: 'bob@test.com', age: 25 },
  ]);

  // Result should have acknowledged and insertedIds map
  expectType<boolean>(result.acknowledged);
  expectType<Record<number, ObjectId>>(result.insertedIds);
}

// -----------------------------------------------------------------------------
// findOne
// -----------------------------------------------------------------------------

async function testFindOne() {
  // Basic findOne returns T | null
  const user = await users.findOne({ name: 'Alice' });
  expectType<User | null>(user);

  // With empty filter
  const anyUser = await users.findOne({});
  expectType<User | null>(anyUser);

  // With projection option
  const withProjection = await users.findOne({ name: 'Alice' }, { projection: { name: 1 } });
  expectType<User | null>(withProjection);

  // With sort option
  const sorted = await users.findOne({ age: { $gte: 18 } }, { sort: { age: -1 } });
  expectType<User | null>(sorted);

  // With skip option
  const skipped = await users.findOne({}, { skip: 5 });
  expectType<User | null>(skipped);
}

// -----------------------------------------------------------------------------
// find
// -----------------------------------------------------------------------------

async function testFind() {
  // find returns a cursor
  const cursor = users.find({ age: { $gte: 18 } });
  expectType<MangoCursor<User>>(cursor);

  // Cursor toArray returns Promise<T[]>
  const results = await cursor.toArray();
  expectType<User[]>(results);

  // Empty filter
  const allUsers = users.find({});
  expectType<MangoCursor<User>>(allUsers);

  // With projection
  const projected = users.find({}, { projection: { name: 1, email: 1 } });
  expectType<MangoCursor<User>>(projected);
}

// -----------------------------------------------------------------------------
// updateOne
// -----------------------------------------------------------------------------

async function testUpdateOne() {
  const result = await users.updateOne({ name: 'Alice' }, { $set: { age: 31 } });

  expectType<boolean>(result.acknowledged);
  expectType<number>(result.matchedCount);
  expectType<number>(result.modifiedCount);
  expectType<number>(result.upsertedCount);
  expectType<ObjectId | null>(result.upsertedId);
}

// With upsert option
async function testUpdateOneWithUpsert() {
  const result = await users.updateOne(
    { email: 'new@test.com' },
    { $set: { name: 'New User', age: 20 } },
    { upsert: true }
  );
  expectType<ObjectId | null>(result.upsertedId);
}

// -----------------------------------------------------------------------------
// updateMany
// -----------------------------------------------------------------------------

async function testUpdateMany() {
  const result = await users.updateMany({ age: { $lt: 18 } }, { $set: { tags: ['minor'] } });

  expectType<boolean>(result.acknowledged);
  expectType<number>(result.matchedCount);
  expectType<number>(result.modifiedCount);
  expectType<number>(result.upsertedCount);
  expectType<ObjectId | null>(result.upsertedId);
}

// -----------------------------------------------------------------------------
// replaceOne
// -----------------------------------------------------------------------------

async function testReplaceOne() {
  const result = await users.replaceOne(
    { name: 'Alice' },
    { name: 'Alice Updated', email: 'alice.updated@test.com', age: 32 }
  );

  expectType<boolean>(result.acknowledged);
  expectType<number>(result.matchedCount);
  expectType<number>(result.modifiedCount);
  expectType<number>(result.upsertedCount);
  expectType<ObjectId | null>(result.upsertedId);
}

// With upsert
async function testReplaceOneWithUpsert() {
  const result = await users.replaceOne(
    { email: 'nonexistent@test.com' },
    { name: 'Created', email: 'nonexistent@test.com', age: 25 },
    { upsert: true }
  );
  expectType<ObjectId | null>(result.upsertedId);
}

// -----------------------------------------------------------------------------
// deleteOne
// -----------------------------------------------------------------------------

async function testDeleteOne() {
  const result = await users.deleteOne({ name: 'Alice' });

  expectType<boolean>(result.acknowledged);
  expectType<number>(result.deletedCount);
}

// -----------------------------------------------------------------------------
// deleteMany
// -----------------------------------------------------------------------------

async function testDeleteMany() {
  const result = await users.deleteMany({ age: { $lt: 18 } });

  expectType<boolean>(result.acknowledged);
  expectType<number>(result.deletedCount);
}

// -----------------------------------------------------------------------------
// findOneAndDelete
// -----------------------------------------------------------------------------

async function testFindOneAndDelete() {
  // Returns the deleted document or null
  const deleted = await users.findOneAndDelete({ name: 'Alice' });
  expectType<User | null>(deleted);

  // With sort option
  const oldestDeleted = await users.findOneAndDelete(
    { status: 'inactive' } as any,
    { sort: { createdAt: 1 } }
  );
  expectType<User | null>(oldestDeleted);

  // With projection
  const projected = await users.findOneAndDelete({ name: 'Bob' }, { projection: { name: 1 } });
  expectType<User | null>(projected);
}

// -----------------------------------------------------------------------------
// findOneAndReplace
// -----------------------------------------------------------------------------

async function testFindOneAndReplace() {
  const replaced = await users.findOneAndReplace(
    { name: 'Alice' },
    { name: 'Alice Replaced', email: 'alice.replaced@test.com', age: 33 }
  );
  expectType<User | null>(replaced);

  // With returnDocument: 'after'
  const afterReplace = await users.findOneAndReplace(
    { name: 'Bob' },
    { name: 'Bob Replaced', email: 'bob.replaced@test.com', age: 26 },
    { returnDocument: 'after' }
  );
  expectType<User | null>(afterReplace);

  // With upsert
  const upserted = await users.findOneAndReplace(
    { email: 'new@test.com' },
    { name: 'New', email: 'new@test.com', age: 20 },
    { upsert: true, returnDocument: 'after' }
  );
  expectType<User | null>(upserted);
}

// -----------------------------------------------------------------------------
// findOneAndUpdate
// -----------------------------------------------------------------------------

async function testFindOneAndUpdate() {
  // Default returns original document
  const original = await users.findOneAndUpdate({ name: 'Alice' }, { $set: { age: 31 } });
  expectType<User | null>(original);

  // With returnDocument: 'after'
  const updated = await users.findOneAndUpdate(
    { name: 'Bob' },
    { $inc: { age: 1 } },
    { returnDocument: 'after' }
  );
  expectType<User | null>(updated);

  // With upsert
  const upserted = await users.findOneAndUpdate(
    { email: 'new@test.com' },
    { $set: { name: 'New User', age: 20 } },
    { upsert: true, returnDocument: 'after' }
  );
  expectType<User | null>(upserted);

  // With sort
  const sorted = await users.findOneAndUpdate(
    { status: 'pending' } as any,
    { $set: { status: 'processing' } },
    { sort: { createdAt: -1 } }
  );
  expectType<User | null>(sorted);
}

// -----------------------------------------------------------------------------
// bulkWrite
// -----------------------------------------------------------------------------

async function testBulkWrite() {
  const result = await users.bulkWrite([
    { insertOne: { document: { name: 'New User', email: 'new@test.com', age: 25 } } },
    { updateOne: { filter: { name: 'Alice' }, update: { $set: { age: 31 } } } },
    { updateMany: { filter: { age: { $lt: 18 } }, update: { $set: { tags: ['minor'] } } } },
    { deleteOne: { filter: { name: 'ToDelete' } } },
    { deleteMany: { filter: { status: 'inactive' } as any } },
    {
      replaceOne: {
        filter: { name: 'ToReplace' },
        replacement: { name: 'Replaced', email: 'replaced@test.com', age: 30 },
      },
    },
  ]);

  expectType<boolean>(result.acknowledged);
  expectType<number>(result.insertedCount);
  expectType<number>(result.matchedCount);
  expectType<number>(result.modifiedCount);
  expectType<number>(result.deletedCount);
  expectType<number>(result.upsertedCount);
  expectType<Record<number, ObjectId>>(result.insertedIds);
  expectType<Record<number, ObjectId>>(result.upsertedIds);
}

// With ordered option
async function testBulkWriteUnordered() {
  const result = await users.bulkWrite(
    [{ insertOne: { document: { name: 'User', email: 'user@test.com', age: 30 } } }],
    { ordered: false }
  );
  expectType<boolean>(result.acknowledged);
}

// -----------------------------------------------------------------------------
// Generic type parameter flows correctly
// -----------------------------------------------------------------------------

async function testGenericTypeFlow() {
  // Creating a collection with a specific type
  const typedCollection = db.collection<Post>('posts');

  // insertOne should require Post-shaped document
  await typedCollection.insertOne({
    title: 'Hello',
    content: 'World',
    authorId: {} as ObjectId,
    views: 0,
  });

  // findOne should return Post | null
  const post = await typedCollection.findOne({ title: 'Hello' });
  expectType<Post | null>(post);

  // find should return MangoCursor<Post>
  const postCursor = typedCollection.find({});
  expectType<MangoCursor<Post>>(postCursor);

  // toArray should return Post[]
  const postArray = await postCursor.toArray();
  expectType<Post[]>(postArray);
}

// =============================================================================
// MAN-29: Filter and QueryOperator Type Tests
// =============================================================================

// -----------------------------------------------------------------------------
// Comparison Operators: $eq, $ne, $gt, $gte, $lt, $lte
// -----------------------------------------------------------------------------

async function testComparisonOperators() {
  // $eq - equality
  await users.findOne({ age: { $eq: 30 } });
  await users.findOne({ name: { $eq: 'Alice' } });

  // $ne - not equal
  await users.findOne({ age: { $ne: 30 } });

  // $gt, $gte, $lt, $lte - comparison
  await users.findOne({ age: { $gt: 18 } });
  await users.findOne({ age: { $gte: 18 } });
  await users.findOne({ age: { $lt: 65 } });
  await users.findOne({ age: { $lte: 65 } });

  // Combining multiple operators on same field
  await users.findOne({ age: { $gte: 18, $lte: 65 } });
}

// -----------------------------------------------------------------------------
// Array Operators: $in, $nin
// -----------------------------------------------------------------------------

async function testArrayInOperators() {
  // $in - match any value in array
  await users.findOne({ age: { $in: [25, 30, 35] } });
  await users.findOne({ name: { $in: ['Alice', 'Bob', 'Charlie'] } });

  // $nin - match none of the values
  await users.findOne({ age: { $nin: [0, 999] } });
  await users.findOne({ name: { $nin: ['Admin', 'System'] } });
}

// -----------------------------------------------------------------------------
// Existence Operators: $exists, $type
// -----------------------------------------------------------------------------

async function testExistenceOperators() {
  // $exists - field existence check
  await users.findOne({ tags: { $exists: true } });
  await users.findOne({ tags: { $exists: false } });

  // $type - BSON type check (can be string or number or array of both)
  await users.findOne({ age: { $type: 'number' } });
  await users.findOne({ name: { $type: 'string' } });
  await users.findOne({ age: { $type: 16 } }); // int32
  await users.findOne({ name: { $type: ['string', 'null'] } });
}

// -----------------------------------------------------------------------------
// Logical Operators: $and, $or, $nor, $not
// -----------------------------------------------------------------------------

async function testLogicalOperators() {
  // $and - all conditions must match
  await users.findOne({
    $and: [{ age: { $gte: 18 } }, { age: { $lte: 65 } }],
  });

  // $or - any condition must match
  await users.findOne({
    $or: [{ name: 'Alice' }, { name: 'Bob' }],
  });

  // $nor - none of the conditions should match
  await users.findOne({
    $nor: [{ age: { $lt: 18 } }, { age: { $gt: 65 } }],
  });

  // $not - inverts the condition
  await users.findOne({
    age: { $not: { $gt: 65 } },
  });

  // $not with RegExp
  await users.findOne({
    name: { $not: /^Admin/i },
  });

  // Nested logical operators
  await users.findOne({
    $and: [
      { $or: [{ name: 'Alice' }, { name: 'Bob' }] },
      { age: { $gte: 18 } },
    ],
  });
}

// -----------------------------------------------------------------------------
// Array Query Operators: $elemMatch, $size, $all
// -----------------------------------------------------------------------------

async function testArrayQueryOperators() {
  // $size - match arrays of specific length
  await users.findOne({ tags: { $size: 3 } });

  // $all - match arrays containing all specified elements
  await users.findOne({ tags: { $all: ['developer', 'tester'] } });

  // $elemMatch - match array elements that satisfy all conditions
  await users.findOne({
    tags: { $elemMatch: { $eq: 'developer' } },
  });
}

// Test with nested document arrays
interface Order {
  _id?: ObjectId;
  items: Array<{ name: string; qty: number; price: number }>;
  [key: string]: unknown;
}
declare const orders: MangoCollection<Order>;

async function testElemMatchWithObjects() {
  // $elemMatch with object conditions
  await orders.findOne({
    items: { $elemMatch: { qty: { $gt: 5 }, price: { $lt: 100 } } },
  });
}

// -----------------------------------------------------------------------------
// Regex Operators: $regex, $options
// -----------------------------------------------------------------------------

async function testRegexOperators() {
  // $regex with string pattern
  await users.findOne({ name: { $regex: '^A' } });

  // $regex with options
  await users.findOne({ name: { $regex: 'alice', $options: 'i' } });

  // $regex with RegExp object
  await users.findOne({ name: { $regex: /^Alice/i } });

  // Direct RegExp in filter (MongoDB allows this)
  await users.findOne({ email: /.*@example\.com$/ });
}

// -----------------------------------------------------------------------------
// $mod Operator
// -----------------------------------------------------------------------------

async function testModOperator() {
  // $mod - modulo operation [divisor, remainder]
  await users.findOne({ age: { $mod: [2, 0] } }); // even ages
  await users.findOne({ age: { $mod: [5, 0] } }); // divisible by 5
}

// -----------------------------------------------------------------------------
// Geospatial Operators
// -----------------------------------------------------------------------------

interface Location {
  _id?: ObjectId;
  name: string;
  location: { type: 'Point'; coordinates: [number, number] };
  [key: string]: unknown;
}
declare const locations: MangoCollection<Location>;

async function testGeoOperators() {
  // $geoWithin with $geometry
  await locations.findOne({
    location: {
      $geoWithin: {
        $geometry: {
          type: 'Polygon',
          coordinates: [
            [
              [0, 0],
              [10, 0],
              [10, 10],
              [0, 10],
              [0, 0],
            ],
          ],
        },
      },
    },
  });

  // $geoWithin with $box
  await locations.findOne({
    location: {
      $geoWithin: {
        $box: [
          [0, 0],
          [10, 10],
        ],
      },
    },
  });

  // $geoWithin with $center
  await locations.findOne({
    location: {
      $geoWithin: {
        $center: [[0, 0], 5],
      },
    },
  });

  // $geoIntersects
  await locations.findOne({
    location: {
      $geoIntersects: {
        $geometry: { type: 'Point', coordinates: [5, 5] },
      },
    },
  });

  // $near with $geometry
  await locations.findOne({
    location: {
      $near: {
        $geometry: { type: 'Point', coordinates: [0, 0] },
        $maxDistance: 1000,
      },
    },
  });

  // $nearSphere
  await locations.findOne({
    location: {
      $nearSphere: {
        $geometry: { type: 'Point', coordinates: [-73.9667, 40.78] },
        $maxDistance: 5000,
        $minDistance: 100,
      },
    },
  });
}

// -----------------------------------------------------------------------------
// $expr and $jsonSchema
// -----------------------------------------------------------------------------

async function testExprOperator() {
  // $expr allows aggregation expressions in queries
  await users.findOne({
    $expr: { $gt: ['$age', 30] },
  });

  // More complex $expr
  await users.findOne({
    $expr: {
      $and: [{ $gte: ['$age', 18] }, { $lte: ['$age', 65] }],
    },
  });
}

async function testJsonSchemaOperator() {
  // $jsonSchema for schema validation
  await users.findOne({
    $jsonSchema: {
      bsonType: 'object',
      required: ['name', 'email'],
      properties: {
        name: { bsonType: 'string' },
        age: { bsonType: 'int', minimum: 0 },
      },
    },
  });
}

// -----------------------------------------------------------------------------
// Direct field value matching (implicit $eq)
// -----------------------------------------------------------------------------

async function testDirectFieldMatching() {
  // Direct value matching (implicit $eq)
  await users.findOne({ name: 'Alice' });
  await users.findOne({ age: 30 });

  // Matching with null
  await users.findOne({ tags: null });

  // Matching multiple fields
  await users.findOne({ name: 'Alice', age: 30 });
}

// -----------------------------------------------------------------------------
// Dot notation for nested fields
// -----------------------------------------------------------------------------

interface UserWithAddress {
  _id?: ObjectId;
  name: string;
  address: {
    city: string;
    zip: string;
    country: string;
  };
  [key: string]: unknown;
}
declare const usersWithAddress: MangoCollection<UserWithAddress>;

async function testDotNotation() {
  // Dot notation for nested field access
  await usersWithAddress.findOne({ 'address.city': 'New York' });
  await usersWithAddress.findOne({ 'address.zip': { $regex: '^10' } });

  // Dot notation with operators
  await usersWithAddress.findOne({
    'address.city': { $in: ['New York', 'Los Angeles', 'Chicago'] },
  });
}

// =============================================================================
// MAN-30: UpdateOperator Type Tests
// =============================================================================

// -----------------------------------------------------------------------------
// Field Operators: $set, $unset, $setOnInsert, $rename
// -----------------------------------------------------------------------------

async function testSetOperator() {
  // $set - set field values
  await users.updateOne({ name: 'Alice' }, { $set: { age: 31 } });

  // $set multiple fields
  await users.updateOne(
    { name: 'Alice' },
    { $set: { age: 31, email: 'newemail@test.com', tags: ['updated'] } }
  );

  // $set with dot notation (nested fields)
  await usersWithAddress.updateOne(
    { name: 'Alice' },
    { $set: { 'address.city': 'Los Angeles', 'address.zip': '90210' } }
  );
}

async function testUnsetOperator() {
  // $unset - remove fields (value is ignored, typically empty string or 1)
  await users.updateOne({ name: 'Alice' }, { $unset: { tags: '' } });

  // $unset multiple fields
  await users.updateOne({ name: 'Alice' }, { $unset: { tags: 1, email: 1 } });
}

async function testSetOnInsertOperator() {
  // $setOnInsert - only set on insert during upsert
  await users.updateOne(
    { email: 'new@test.com' },
    {
      $set: { name: 'Updated' },
      $setOnInsert: { age: 25, tags: ['new'] },
    },
    { upsert: true }
  );
}

async function testRenameOperator() {
  // $rename - rename field (value is new field name)
  await users.updateOne({ name: 'Alice' }, { $rename: { tags: 'labels' } });

  // $rename with dot notation
  await usersWithAddress.updateOne(
    { name: 'Alice' },
    { $rename: { 'address.zip': 'address.postalCode' } }
  );
}

// -----------------------------------------------------------------------------
// Numeric Operators: $inc, $mul, $min, $max
// -----------------------------------------------------------------------------

async function testIncOperator() {
  // $inc - increment by value (number only)
  await users.updateOne({ name: 'Alice' }, { $inc: { age: 1 } });

  // $inc with negative value (decrement)
  await users.updateOne({ name: 'Alice' }, { $inc: { age: -1 } });

  // $inc multiple fields
  await posts.updateOne({ title: 'Hello' }, { $inc: { views: 1 } });
}

async function testMulOperator() {
  // $mul - multiply by value (number only)
  await users.updateOne({ name: 'Alice' }, { $mul: { age: 2 } });

  // $mul with decimal
  await posts.updateOne({ title: 'Hello' }, { $mul: { views: 1.5 } });
}

async function testMinMaxOperators() {
  // $min - update only if new value is less than current
  await users.updateOne({ name: 'Alice' }, { $min: { age: 18 } });

  // $max - update only if new value is greater than current
  await users.updateOne({ name: 'Alice' }, { $max: { age: 65 } });

  // $min/$max can work with dates and other comparable types
  await posts.updateOne({ title: 'Hello' }, { $min: { views: 0 } });
}

// -----------------------------------------------------------------------------
// Array Operators: $push, $pull, $addToSet, $pop, $pullAll
// -----------------------------------------------------------------------------

async function testPushOperator() {
  // $push - add to array
  await users.updateOne({ name: 'Alice' }, { $push: { tags: 'new-tag' } });

  // $push with $each - add multiple items
  await users.updateOne(
    { name: 'Alice' },
    { $push: { tags: { $each: ['tag1', 'tag2', 'tag3'] } } }
  );

  // $push with $each, $sort, $slice
  await users.updateOne(
    { name: 'Alice' },
    {
      $push: {
        tags: {
          $each: ['new-tag'],
          $sort: 1,
          $slice: 10,
        },
      },
    }
  );

  // $push with $position
  await users.updateOne(
    { name: 'Alice' },
    {
      $push: {
        tags: {
          $each: ['first-tag'],
          $position: 0,
        },
      },
    }
  );
}

async function testPullOperator() {
  // $pull - remove matching elements
  await users.updateOne({ name: 'Alice' }, { $pull: { tags: 'old-tag' } });

  // $pull with query condition
  await orders.updateOne(
    { _id: {} as ObjectId },
    { $pull: { items: { qty: { $lt: 1 } } } }
  );
}

async function testPullAllOperator() {
  // $pullAll - remove all matching values
  await users.updateOne({ name: 'Alice' }, { $pullAll: { tags: ['tag1', 'tag2'] } });
}

async function testAddToSetOperator() {
  // $addToSet - add only if not already present
  await users.updateOne({ name: 'Alice' }, { $addToSet: { tags: 'unique-tag' } });

  // $addToSet with $each
  await users.updateOne(
    { name: 'Alice' },
    { $addToSet: { tags: { $each: ['tag1', 'tag2'] } } }
  );
}

async function testPopOperator() {
  // $pop - remove first (-1) or last (1) element
  await users.updateOne({ name: 'Alice' }, { $pop: { tags: 1 } }); // remove last
  await users.updateOne({ name: 'Alice' }, { $pop: { tags: -1 } }); // remove first
}

// -----------------------------------------------------------------------------
// Bitwise Operator: $bit
// -----------------------------------------------------------------------------

async function testBitOperator() {
  // $bit with and
  await users.updateOne({ name: 'Alice' }, { $bit: { age: { and: 0b1111 } } });

  // $bit with or
  await users.updateOne({ name: 'Alice' }, { $bit: { age: { or: 0b0001 } } });

  // $bit with xor
  await users.updateOne({ name: 'Alice' }, { $bit: { age: { xor: 0b0101 } } });
}

// -----------------------------------------------------------------------------
// Date Operator: $currentDate
// -----------------------------------------------------------------------------

interface Document_WithDates {
  _id?: ObjectId;
  name: string;
  lastModified?: Date;
  createdAt?: Date;
  [key: string]: unknown;
}
declare const docsWithDates: MangoCollection<Document_WithDates>;

async function testCurrentDateOperator() {
  // $currentDate with boolean (uses Date type)
  await docsWithDates.updateOne({ name: 'test' }, { $currentDate: { lastModified: true } });

  // $currentDate with $type: 'date'
  await docsWithDates.updateOne(
    { name: 'test' },
    { $currentDate: { lastModified: { $type: 'date' } } }
  );

  // $currentDate with $type: 'timestamp'
  await docsWithDates.updateOne(
    { name: 'test' },
    { $currentDate: { lastModified: { $type: 'timestamp' } } }
  );
}

// -----------------------------------------------------------------------------
// Combining Multiple Update Operators
// -----------------------------------------------------------------------------

async function testCombinedUpdateOperators() {
  // Multiple operators in single update
  await users.updateOne(
    { name: 'Alice' },
    {
      $set: { email: 'alice@newdomain.com' },
      $inc: { age: 1 },
      $push: { tags: 'birthday' },
      $currentDate: { lastModified: true } as any,
    }
  );

  // Complex update with multiple array operations
  await users.updateOne(
    { name: 'Bob' },
    {
      $set: { name: 'Robert' },
      $addToSet: { tags: { $each: ['promoted', 'senior'] } },
      $unset: { temporaryField: '' } as any,
    }
  );
}

// =============================================================================
// MAN-31: Cursor Type Tests
// =============================================================================

// -----------------------------------------------------------------------------
// MangoCursor - Method Chaining
// -----------------------------------------------------------------------------

async function testCursorMethodChaining() {
  // find() returns MangoCursor<T>
  const cursor = users.find({ age: { $gte: 18 } });
  expectType<MangoCursor<User>>(cursor);

  // sort() returns same cursor type for chaining
  const sortedCursor = cursor.sort({ age: -1 });
  expectType<MangoCursor<User>>(sortedCursor);

  // limit() returns same cursor type for chaining
  const limitedCursor = cursor.limit(10);
  expectType<MangoCursor<User>>(limitedCursor);

  // skip() returns same cursor type for chaining
  const skippedCursor = cursor.skip(5);
  expectType<MangoCursor<User>>(skippedCursor);

  // hint() returns same cursor type for chaining
  const hintedCursor = cursor.hint('email_1');
  expectType<MangoCursor<User>>(hintedCursor);

  // Full chain maintains type
  const fullChain = users
    .find({ age: { $gte: 18 } })
    .sort({ age: -1, name: 1 })
    .skip(10)
    .limit(5)
    .hint({ age: 1 });
  expectType<MangoCursor<User>>(fullChain);
}

// -----------------------------------------------------------------------------
// MangoCursor - sort() options
// -----------------------------------------------------------------------------

async function testCursorSortOptions() {
  // Sort ascending
  users.find({}).sort({ name: 1 });

  // Sort descending
  users.find({}).sort({ age: -1 });

  // Multi-field sort
  users.find({}).sort({ age: -1, name: 1 });

  // Sort with $meta for text score
  users.find({}).sort({ score: { $meta: 'textScore' } });
}

// -----------------------------------------------------------------------------
// MangoCursor - hint() options
// -----------------------------------------------------------------------------

async function testCursorHintOptions() {
  // Hint by index name (string)
  users.find({}).hint('email_1');

  // Hint by key pattern (object)
  users.find({}).hint({ email: 1 });

  // $natural hint for collection scan order
  users.find({}).hint({ $natural: 1 });
  users.find({}).hint({ $natural: -1 });
}

// -----------------------------------------------------------------------------
// MangoCursor - toArray()
// -----------------------------------------------------------------------------

async function testCursorToArray() {
  // toArray() returns Promise<T[]>
  const results = await users.find({ age: { $gte: 18 } }).toArray();
  expectType<User[]>(results);

  // With chaining
  const sortedResults = await users.find({}).sort({ age: -1 }).limit(10).toArray();
  expectType<User[]>(sortedResults);

  // Different document type
  const postResults = await posts.find({}).toArray();
  expectType<Post[]>(postResults);
}

// -----------------------------------------------------------------------------
// MangoCursor - Generic Type Preservation
// -----------------------------------------------------------------------------

async function testCursorGenericTypePreservation() {
  // Generic type is preserved through all operations
  const cursor: MangoCursor<User> = users.find({});

  // After sort
  const afterSort: MangoCursor<User> = cursor.sort({ age: 1 });

  // After limit
  const afterLimit: MangoCursor<User> = afterSort.limit(10);

  // After skip
  const afterSkip: MangoCursor<User> = afterLimit.skip(5);

  // toArray returns correct type
  const array: User[] = await afterSkip.toArray();
  expectType<User[]>(array);
}

// -----------------------------------------------------------------------------
// IndexCursor
// -----------------------------------------------------------------------------

async function testIndexCursor() {
  // listIndexes returns IndexCursor
  const indexCursor = users.listIndexes();

  // Note: IndexCursor is not exported from main package, but we can test via listIndexes
  // toArray returns IndexInfo[]
  const indexes = await indexCursor.toArray();

  // Each index has expected properties
  if (indexes.length > 0) {
    expectType<string>(indexes[0].name);
    expectType<Record<string, 1 | -1 | 'text' | '2d' | '2dsphere' | 'hashed'>>(indexes[0].key);
  }
}

// -----------------------------------------------------------------------------
// Cursor with different collection types
// -----------------------------------------------------------------------------

async function testCursorWithDifferentTypes() {
  // User collection cursor
  const userCursor = users.find({ name: 'Alice' });
  const userArray = await userCursor.toArray();
  expectType<User[]>(userArray);

  // Post collection cursor
  const postCursor = posts.find({ views: { $gt: 100 } });
  const postArray = await postCursor.toArray();
  expectType<Post[]>(postArray);

  // Location collection cursor
  const locationCursor = locations.find({});
  const locationArray = await locationCursor.toArray();
  expectType<Location[]>(locationArray);

  // Order collection cursor
  const orderCursor = orders.find({});
  const orderArray = await orderCursor.toArray();
  expectType<Order[]>(orderArray);
}

// =============================================================================
// MAN-32: Aggregation Type Tests
// =============================================================================

// -----------------------------------------------------------------------------
// AggregationCursor
// -----------------------------------------------------------------------------

async function testAggregationCursor() {
  // aggregate() returns AggregationCursor
  const cursor = users.aggregate([{ $match: { age: { $gte: 18 } } }]);
  expectType<AggregationCursor<User>>(cursor);

  // toArray() returns Promise<Document[]> (aggregation can change document shape)
  const results = await cursor.toArray();
  // Note: Aggregation returns Document[] since pipeline can reshape documents
}

// -----------------------------------------------------------------------------
// $match stage
// -----------------------------------------------------------------------------

async function testMatchStage() {
  // Basic $match
  await users.aggregate([{ $match: { name: 'Alice' } }]).toArray();

  // $match with operators
  await users.aggregate([{ $match: { age: { $gte: 18, $lte: 65 } } }]).toArray();

  // $match with logical operators
  await users
    .aggregate([{ $match: { $or: [{ name: 'Alice' }, { name: 'Bob' }] } }])
    .toArray();
}

// -----------------------------------------------------------------------------
// $project stage
// -----------------------------------------------------------------------------

async function testProjectStage() {
  // Include fields
  await users.aggregate([{ $project: { name: 1, email: 1 } }]).toArray();

  // Exclude fields
  await users.aggregate([{ $project: { tags: 0 } }]).toArray();

  // Rename/reference fields
  await users.aggregate([{ $project: { userName: '$name', userAge: '$age' } }]).toArray();

  // With $literal
  await users.aggregate([{ $project: { name: 1, type: { $literal: 'user' } } }]).toArray();
}

// -----------------------------------------------------------------------------
// $sort stage
// -----------------------------------------------------------------------------

async function testSortStage() {
  // Sort ascending
  await users.aggregate([{ $sort: { name: 1 } }]).toArray();

  // Sort descending
  await users.aggregate([{ $sort: { age: -1 } }]).toArray();

  // Multi-field sort
  await users.aggregate([{ $sort: { age: -1, name: 1 } }]).toArray();
}

// -----------------------------------------------------------------------------
// $limit and $skip stages
// -----------------------------------------------------------------------------

async function testLimitSkipStages() {
  // $limit
  await users.aggregate([{ $limit: 10 }]).toArray();

  // $skip
  await users.aggregate([{ $skip: 5 }]).toArray();

  // Combined pagination
  await users.aggregate([{ $skip: 20 }, { $limit: 10 }]).toArray();
}

// -----------------------------------------------------------------------------
// $count stage
// -----------------------------------------------------------------------------

async function testCountStage() {
  // $count outputs a single document with the specified field name
  const result = await users.aggregate([{ $match: { age: { $gte: 18 } } }, { $count: 'adultCount' }]).toArray();
  // Result is [{ adultCount: number }]
}

// -----------------------------------------------------------------------------
// $unwind stage
// -----------------------------------------------------------------------------

async function testUnwindStage() {
  // Short syntax (string)
  await users.aggregate([{ $unwind: '$tags' }]).toArray();

  // Object syntax with options
  await users
    .aggregate([
      {
        $unwind: {
          path: '$tags',
          preserveNullAndEmptyArrays: true,
          includeArrayIndex: 'tagIndex',
        },
      },
    ])
    .toArray();
}

// -----------------------------------------------------------------------------
// $group stage
// -----------------------------------------------------------------------------

async function testGroupStage() {
  // Group by field value
  await users
    .aggregate([
      {
        $group: {
          _id: '$age',
          count: { $sum: 1 },
        },
      },
    ])
    .toArray();

  // Group with multiple accumulators
  await posts
    .aggregate([
      {
        $group: {
          _id: '$authorId',
          totalViews: { $sum: '$views' },
          avgViews: { $avg: '$views' },
          postCount: { $sum: 1 },
          titles: { $push: '$title' },
        },
      },
    ])
    .toArray();

  // Group by null (all documents)
  await users
    .aggregate([
      {
        $group: {
          _id: null,
          totalUsers: { $sum: 1 },
          avgAge: { $avg: '$age' },
          minAge: { $min: '$age' },
          maxAge: { $max: '$age' },
        },
      },
    ])
    .toArray();

  // Group with $first and $last
  await users
    .aggregate([
      { $sort: { age: 1 } },
      {
        $group: {
          _id: null,
          youngest: { $first: '$name' },
          oldest: { $last: '$name' },
        },
      },
    ])
    .toArray();

  // Group with $addToSet
  await users
    .aggregate([
      {
        $group: {
          _id: null,
          allTags: { $addToSet: '$tags' },
        },
      },
    ])
    .toArray();
}

// -----------------------------------------------------------------------------
// $lookup stage
// -----------------------------------------------------------------------------

async function testLookupStage() {
  // Basic $lookup (left outer join)
  await posts
    .aggregate([
      {
        $lookup: {
          from: 'users',
          localField: 'authorId',
          foreignField: '_id',
          as: 'author',
        },
      },
    ])
    .toArray();
}

// -----------------------------------------------------------------------------
// $addFields and $set stages
// -----------------------------------------------------------------------------

async function testAddFieldsAndSetStages() {
  // $addFields
  await users
    .aggregate([
      {
        $addFields: {
          isAdult: { $gte: ['$age', 18] },
          fullName: { $concat: ['$name', ' (', '$email', ')'] },
        },
      },
    ])
    .toArray();

  // $set (alias for $addFields)
  await users
    .aggregate([
      {
        $set: {
          ageInMonths: { $multiply: ['$age', 12] },
        },
      },
    ])
    .toArray();
}

// -----------------------------------------------------------------------------
// $replaceRoot stage
// -----------------------------------------------------------------------------

async function testReplaceRootStage() {
  // Replace document with embedded document
  await usersWithAddress
    .aggregate([
      {
        $replaceRoot: {
          newRoot: '$address',
        },
      },
    ])
    .toArray();

  // Replace with $mergeObjects
  await users
    .aggregate([
      {
        $replaceRoot: {
          newRoot: { $mergeObjects: [{ type: 'user' }, '$$ROOT'] },
        },
      },
    ])
    .toArray();
}

// -----------------------------------------------------------------------------
// $out stage
// -----------------------------------------------------------------------------

async function testOutStage() {
  // Write results to another collection
  await users
    .aggregate([{ $match: { age: { $gte: 18 } } }, { $out: 'adults' }])
    .toArray();
}

// -----------------------------------------------------------------------------
// $geoNear stage
// -----------------------------------------------------------------------------

async function testGeoNearStage() {
  // $geoNear with GeoJSON point
  await locations
    .aggregate([
      {
        $geoNear: {
          near: { type: 'Point', coordinates: [-73.9667, 40.78] },
          distanceField: 'distance',
          maxDistance: 5000,
          spherical: true,
        },
      },
    ])
    .toArray();

  // $geoNear with legacy coordinates
  await locations
    .aggregate([
      {
        $geoNear: {
          near: [-73.9667, 40.78],
          distanceField: 'dist.calculated',
          minDistance: 100,
          maxDistance: 10000,
          query: { name: { $regex: '^A' } },
          includeLocs: 'dist.location',
          distanceMultiplier: 0.001, // Convert to km
        },
      },
    ])
    .toArray();
}

// -----------------------------------------------------------------------------
// Complex multi-stage pipelines
// -----------------------------------------------------------------------------

async function testComplexPipeline() {
  // Full pipeline with multiple stages
  await users
    .aggregate([
      // Filter
      { $match: { age: { $gte: 18 } } },
      // Add computed field
      { $addFields: { isAdult: true } },
      // Unwind tags array
      { $unwind: '$tags' },
      // Group by tag
      {
        $group: {
          _id: '$tags',
          users: { $push: '$name' },
          count: { $sum: 1 },
        },
      },
      // Sort by count
      { $sort: { count: -1 } },
      // Limit results
      { $limit: 10 },
      // Reshape output
      { $project: { tag: '$_id', users: 1, count: 1, _id: 0 } },
    ])
    .toArray();
}

// Pipeline for analytics
async function testAnalyticsPipeline() {
  await posts
    .aggregate([
      // Group by author
      {
        $group: {
          _id: '$authorId',
          totalPosts: { $sum: 1 },
          totalViews: { $sum: '$views' },
          avgViews: { $avg: '$views' },
        },
      },
      // Join with users
      {
        $lookup: {
          from: 'users',
          localField: '_id',
          foreignField: '_id',
          as: 'author',
        },
      },
      // Unwind author array (should be single element)
      { $unwind: '$author' },
      // Sort by total views
      { $sort: { totalViews: -1 } },
      // Project final shape
      {
        $project: {
          authorName: '$author.name',
          totalPosts: 1,
          totalViews: 1,
          avgViews: 1,
        },
      },
    ])
    .toArray();
}

// =============================================================================
// MAN-33: MongoDB Driver Compatibility Tests
// =============================================================================

// Import MongoDB driver types for comparison
import type {
  Collection as MongoCollection,
  Db as MongoDb,
  MongoClient as MongoClientType,
  InsertOneResult as MongoInsertOneResult,
  InsertManyResult as MongoInsertManyResult,
  UpdateResult as MongoUpdateResult,
  DeleteResult as MongoDeleteResult,
  FindCursor as MongoFindCursor,
  WithId,
  OptionalUnlessRequiredId,
} from 'mongodb';

// Test document type for compatibility tests
interface CompatTestDoc {
  _id?: ObjectId;
  name: string;
  value: number;
}

// -----------------------------------------------------------------------------
// Return Type Compatibility - Insert Operations
// -----------------------------------------------------------------------------

// Note: These tests verify that MangoDB return types have the same shape as
// MongoDB driver return types. The types may not be directly assignable due to
// additional properties in MongoDB types, but the essential properties match.

async function testInsertOneReturnTypeCompatibility() {
  // MangoDB insertOne result
  const mangoResult = await users.insertOne({ name: 'Test', email: 'test@test.com', age: 25 });

  // Verify essential properties exist and have compatible types
  expectType<boolean>(mangoResult.acknowledged);
  expectType<ObjectId>(mangoResult.insertedId);

  // These are the same properties MongoDB's InsertOneResult has
  const acknowledged: boolean = mangoResult.acknowledged;
  const insertedId: ObjectId = mangoResult.insertedId;
}

async function testInsertManyReturnTypeCompatibility() {
  const mangoResult = await users.insertMany([{ name: 'Test', email: 'test@test.com', age: 25 }]);

  // Verify essential properties
  expectType<boolean>(mangoResult.acknowledged);
  expectType<Record<number, ObjectId>>(mangoResult.insertedIds);

  // MongoDB uses insertedCount and a { [key: number]: ObjectId } for insertedIds
  const acknowledged: boolean = mangoResult.acknowledged;
}

// -----------------------------------------------------------------------------
// Return Type Compatibility - Update Operations
// -----------------------------------------------------------------------------

async function testUpdateReturnTypeCompatibility() {
  const mangoResult = await users.updateOne({ name: 'Test' }, { $set: { age: 26 } });

  // Essential properties that match MongoDB
  expectType<boolean>(mangoResult.acknowledged);
  expectType<number>(mangoResult.matchedCount);
  expectType<number>(mangoResult.modifiedCount);
  expectType<number>(mangoResult.upsertedCount);
  expectType<ObjectId | null>(mangoResult.upsertedId);
}

async function testReplaceReturnTypeCompatibility() {
  const mangoResult = await users.replaceOne(
    { name: 'Test' },
    { name: 'Replaced', email: 'replaced@test.com', age: 30 }
  );

  expectType<boolean>(mangoResult.acknowledged);
  expectType<number>(mangoResult.matchedCount);
  expectType<number>(mangoResult.modifiedCount);
}

// -----------------------------------------------------------------------------
// Return Type Compatibility - Delete Operations
// -----------------------------------------------------------------------------

async function testDeleteReturnTypeCompatibility() {
  const mangoResult = await users.deleteOne({ name: 'Test' });

  // MongoDB DeleteResult has acknowledged and deletedCount
  expectType<boolean>(mangoResult.acknowledged);
  expectType<number>(mangoResult.deletedCount);
}

// -----------------------------------------------------------------------------
// Return Type Compatibility - Find Operations
// -----------------------------------------------------------------------------

async function testFindOneReturnTypeCompatibility() {
  // findOne returns T | null in both MongoDB and MangoDB
  const mangoResult = await users.findOne({ name: 'Test' });
  expectType<User | null>(mangoResult);
}

async function testFindReturnTypeCompatibility() {
  // find returns a cursor in both
  const mangoCursor = users.find({ name: 'Test' });

  // toArray returns T[] in both
  const results = await mangoCursor.toArray();
  expectType<User[]>(results);
}

// -----------------------------------------------------------------------------
// Return Type Compatibility - FindOneAnd* Operations
// -----------------------------------------------------------------------------

async function testFindOneAndDeleteReturnTypeCompatibility() {
  // Returns T | null
  const result = await users.findOneAndDelete({ name: 'Test' });
  expectType<User | null>(result);
}

async function testFindOneAndUpdateReturnTypeCompatibility() {
  const result = await users.findOneAndUpdate({ name: 'Test' }, { $set: { age: 30 } });
  expectType<User | null>(result);
}

async function testFindOneAndReplaceReturnTypeCompatibility() {
  const result = await users.findOneAndReplace(
    { name: 'Test' },
    { name: 'Replaced', email: 'r@test.com', age: 25 }
  );
  expectType<User | null>(result);
}

// -----------------------------------------------------------------------------
// API Signature Compatibility
// -----------------------------------------------------------------------------

// These tests verify that MangoDB methods accept the same parameter patterns
// as MongoDB driver methods.

async function testFilterParameterCompatibility() {
  // Both should accept:
  // - Empty object
  await users.findOne({});

  // - Field equality
  await users.findOne({ name: 'Alice' });

  // - Comparison operators
  await users.findOne({ age: { $gt: 18 } });

  // - Logical operators
  await users.findOne({ $and: [{ age: { $gte: 18 } }, { age: { $lte: 65 } }] });

  // - Array operators
  await users.findOne({ tags: { $in: ['dev', 'test'] } });
}

async function testUpdateParameterCompatibility() {
  // Both should accept update operators:
  await users.updateOne({ name: 'Alice' }, { $set: { age: 30 } });
  await users.updateOne({ name: 'Alice' }, { $inc: { age: 1 } });
  await users.updateOne({ name: 'Alice' }, { $push: { tags: 'new' } });
  await users.updateOne({ name: 'Alice' }, { $unset: { tags: '' } });
}

async function testOptionsCompatibility() {
  // Both should accept common options

  // Update with upsert
  await users.updateOne({ name: 'New' }, { $set: { age: 25 } }, { upsert: true });

  // Find with projection
  await users.findOne({ name: 'Alice' }, { projection: { name: 1, email: 1 } });

  // FindOneAndUpdate with returnDocument
  await users.findOneAndUpdate(
    { name: 'Alice' },
    { $set: { age: 30 } },
    { returnDocument: 'after' }
  );

  // FindOneAndReplace with upsert and returnDocument
  await users.findOneAndReplace(
    { name: 'New' },
    { name: 'New', email: 'new@test.com', age: 25 },
    { upsert: true, returnDocument: 'after' }
  );
}

// -----------------------------------------------------------------------------
// Cursor Method Compatibility
// -----------------------------------------------------------------------------

async function testCursorMethodCompatibility() {
  // Both cursors should support:
  const cursor = users.find({});

  // sort
  cursor.sort({ age: -1 });

  // limit
  cursor.limit(10);

  // skip
  cursor.skip(5);

  // toArray
  const results = await users.find({}).sort({ age: -1 }).limit(10).skip(5).toArray();
  expectType<User[]>(results);
}

// -----------------------------------------------------------------------------
// Common Usage Patterns
// -----------------------------------------------------------------------------

// These patterns are common in real applications and must work with both
// MongoDB and MangoDB.

async function testCommonCRUDPattern() {
  // Create
  const createResult = await users.insertOne({
    name: 'John Doe',
    email: 'john@example.com',
    age: 30,
  });
  const newUserId = createResult.insertedId;

  // Read
  const user = await users.findOne({ _id: newUserId });
  if (user) {
    expectType<string>(user.name);
    expectType<number>(user.age);
  }

  // Update
  const updateResult = await users.updateOne(
    { _id: newUserId },
    { $set: { age: 31 }, $push: { tags: 'birthday' } }
  );
  if (updateResult.modifiedCount > 0) {
    // User was updated
  }

  // Delete
  const deleteResult = await users.deleteOne({ _id: newUserId });
  if (deleteResult.deletedCount > 0) {
    // User was deleted
  }
}

async function testPaginationPattern() {
  const page = 2;
  const pageSize = 10;

  const results = await users
    .find({ age: { $gte: 18 } })
    .sort({ name: 1 })
    .skip((page - 1) * pageSize)
    .limit(pageSize)
    .toArray();

  expectType<User[]>(results);
}

async function testBulkOperationPattern() {
  const result = await users.bulkWrite([
    { insertOne: { document: { name: 'User1', email: 'u1@test.com', age: 20 } } },
    { updateOne: { filter: { name: 'User2' }, update: { $inc: { age: 1 } } } },
    { deleteOne: { filter: { name: 'User3' } } },
  ]);

  expectType<number>(result.insertedCount);
  expectType<number>(result.modifiedCount);
  expectType<number>(result.deletedCount);
}

async function testAggregationPattern() {
  // Common aggregation pattern: group + sort + limit
  const topAuthors = await posts
    .aggregate([
      {
        $group: {
          _id: '$authorId',
          postCount: { $sum: 1 },
          totalViews: { $sum: '$views' },
        },
      },
      { $sort: { totalViews: -1 } },
      { $limit: 10 },
    ])
    .toArray();

  // Result is Document[] since aggregation reshapes data
}

// =============================================================================
// MAN-34: Gap Analysis - Additional Type Tests
// =============================================================================

// -----------------------------------------------------------------------------
// Index Operations
// -----------------------------------------------------------------------------

async function testCreateIndex() {
  // Create simple ascending index
  const indexName = await users.createIndex({ email: 1 });
  expectType<string>(indexName);

  // Create descending index
  await users.createIndex({ age: -1 });

  // Create compound index
  await users.createIndex({ name: 1, age: -1 });

  // Create unique index
  await users.createIndex({ email: 1 }, { unique: true });

  // Create index with name
  await users.createIndex({ name: 1 }, { name: 'name_index' });

  // Create sparse index
  await users.createIndex({ tags: 1 }, { sparse: true });

  // Create text index
  await users.createIndex({ name: 'text', email: 'text' });

  // Create 2dsphere index
  await locations.createIndex({ location: '2dsphere' });

  // Create hashed index
  await users.createIndex({ email: 'hashed' });

  // Create index with partial filter expression
  await users.createIndex({ age: 1 }, { partialFilterExpression: { age: { $gte: 18 } } });

  // Create TTL index
  await docsWithDates.createIndex({ createdAt: 1 }, { expireAfterSeconds: 3600 });

  // Create hidden index
  await users.createIndex({ name: 1 }, { hidden: true });
}

async function testCreateIndexes() {
  // Create multiple indexes at once
  const indexNames = await users.createIndexes([
    { key: { email: 1 }, unique: true },
    { key: { name: 1 } },
    { key: { age: -1, name: 1 } },
  ]);
  expectType<string[]>(indexNames);
}

async function testDropIndex() {
  // Drop by name
  await users.dropIndex('email_1');

  // Drop by key spec
  await users.dropIndex({ email: 1 });
}

async function testDropIndexes() {
  // Drop all indexes (except _id)
  await users.dropIndexes('*');

  // Drop specific indexes by name
  await users.dropIndexes(['email_1', 'name_1']);
}

async function testIndexes() {
  // Get all indexes
  const indexes = await users.indexes();

  // indexes() returns IndexInfo[]
  if (indexes.length > 0) {
    expectType<string>(indexes[0].name);
    expectType<Record<string, 1 | -1 | 'text' | '2d' | '2dsphere' | 'hashed'>>(indexes[0].key);
    expectType<number>(indexes[0].v);
  }
}

async function testListIndexes() {
  // listIndexes returns IndexCursor
  const indexCursor = users.listIndexes();

  // toArray returns IndexInfo[]
  const indexes = await indexCursor.toArray();

  if (indexes.length > 0) {
    // Check optional properties
    const idx = indexes[0];
    if (idx.unique !== undefined) expectType<boolean>(idx.unique);
    if (idx.sparse !== undefined) expectType<boolean>(idx.sparse);
    if (idx.expireAfterSeconds !== undefined) expectType<number>(idx.expireAfterSeconds);
    if (idx.hidden !== undefined) expectType<boolean>(idx.hidden);
  }
}

// -----------------------------------------------------------------------------
// Collection Admin Methods
// -----------------------------------------------------------------------------

async function testCountDocuments() {
  // Count all documents
  const count = await users.countDocuments();
  expectType<number>(count);

  // Count with filter
  const adultCount = await users.countDocuments({ age: { $gte: 18 } });
  expectType<number>(adultCount);
}

async function testEstimatedDocumentCount() {
  // Estimated count (faster, no filter)
  const count = await users.estimatedDocumentCount();
  expectType<number>(count);
}

async function testDistinct() {
  // Get distinct values for a field
  const names = await users.distinct('name');
  expectType<unknown[]>(names);

  // Distinct with filter
  const adultNames = await users.distinct('name', { age: { $gte: 18 } });
  expectType<unknown[]>(adultNames);

  // Distinct on nested field
  const cities = await usersWithAddress.distinct('address.city');
  expectType<unknown[]>(cities);
}

async function testCollectionDrop() {
  // Drop collection
  const dropped = await users.drop();
  expectType<boolean>(dropped);
}

async function testCollectionStats() {
  // Get collection statistics
  const stats = await users.stats();

  expectType<string>(stats.ns);
  expectType<number>(stats.count);
  expectType<number>(stats.size);
  expectType<number>(stats.storageSize);
  expectType<number>(stats.totalIndexSize);
  expectType<Record<string, number>>(stats.indexSizes);
  expectType<number>(stats.totalSize);
  expectType<number>(stats.nindexes);
  expectType<1>(stats.ok);
}

async function testCollectionRename() {
  // Rename collection
  const renamedCollection = await users.rename('users_renamed');
  expectType<MangoCollection<User>>(renamedCollection);

  // Rename with dropTarget option
  const renamedWithDrop = await users.rename('users_new', { dropTarget: true });
  expectType<MangoCollection<User>>(renamedWithDrop);
}

// -----------------------------------------------------------------------------
// Database Methods
// -----------------------------------------------------------------------------

async function testDbDropDatabase() {
  // Drop entire database
  await db.dropDatabase();
}

async function testDbListCollections() {
  // List all collections
  const cursor = db.listCollections();
  const collections = await cursor.toArray();

  if (collections.length > 0) {
    expectType<string>(collections[0].name);
    expectType<'collection' | 'view'>(collections[0].type);
  }

  // List with filter
  const filtered = await db.listCollections({ name: 'users' }).toArray();

  // List with nameOnly option
  const namesOnly = await db.listCollections({}, { nameOnly: true }).toArray();
}

async function testDbStats() {
  // Get database statistics
  const stats = await db.stats();

  expectType<string>(stats.db);
  expectType<number>(stats.collections);
  expectType<number>(stats.views);
  expectType<number>(stats.objects);
  expectType<number>(stats.dataSize);
  expectType<number>(stats.storageSize);
  expectType<number>(stats.indexes);
  expectType<number>(stats.indexSize);
  expectType<1>(stats.ok);
}

async function testDbAggregate() {
  // Database-level aggregation (e.g., with $documents stage)
  const cursor = db.aggregate([{ $match: {} }]);
  const results = await cursor.toArray();
}

// -----------------------------------------------------------------------------
// Client Methods
// -----------------------------------------------------------------------------

async function testClientConnect() {
  // connect() returns the client for chaining
  const connectedClient = await client.connect();
  expectType<MangoClient>(connectedClient);
}

async function testClientClose() {
  // close() returns void
  await client.close();
}

async function testClientDb() {
  // db() returns MangoDb
  const database = client.db('testdb');
  expectType<MangoDb>(database);
}

// -----------------------------------------------------------------------------
// Error Types
// -----------------------------------------------------------------------------

import {
  DuplicateKeyError,
  IndexNotFoundError,
  CannotDropIdIndexError,
  TextIndexRequiredError,
  InvalidIndexOptionsError,
  BadHintError,
} from '@jkershaw/mangodb';

function testErrorTypes() {
  // DuplicateKeyError - takes (db, collection, indexName, keyPattern, keyValue)
  const dupError = new DuplicateKeyError('testdb', 'users', '_id_', { _id: 1 }, { _id: 'test' });
  expectType<DuplicateKeyError>(dupError);
  expectType<string>(dupError.message);
  expectType<string>(dupError.name);
  // code is a specific literal type 11000
  expectType<11000>(dupError.code);

  // IndexNotFoundError - takes (indexName)
  const indexError = new IndexNotFoundError('nonexistent_index');
  expectType<IndexNotFoundError>(indexError);
  expectType<27>(indexError.code);

  // CannotDropIdIndexError - takes no arguments
  const idError = new CannotDropIdIndexError();
  expectType<CannotDropIdIndexError>(idError);
  expectType<72>(idError.code);

  // TextIndexRequiredError - takes no arguments
  const textError = new TextIndexRequiredError();
  expectType<TextIndexRequiredError>(textError);
  expectType<27>(textError.code);

  // InvalidIndexOptionsError - takes (message)
  const optionsError = new InvalidIndexOptionsError('Invalid option');
  expectType<InvalidIndexOptionsError>(optionsError);
  expectType<67>(optionsError.code);

  // BadHintError - takes (hint)
  const hintError = new BadHintError('bad_hint');
  expectType<BadHintError>(hintError);
  expectType<17007>(hintError.code);
}

// -----------------------------------------------------------------------------
// Type Exports Verification
// -----------------------------------------------------------------------------

import type {
  IndexKeySpec,
  CreateIndexOptions,
  IndexInfo,
  PipelineStage,
  MatchStage,
  ProjectStage,
  SortStage,
  LimitStage,
  SkipStage,
  CountStage,
  UnwindStage,
  UnwindOptions,
  AggregateOptions,
  ProjectExpression,
} from '@jkershaw/mangodb';

function testTypeExports() {
  // Verify exported types are usable
  const indexKey: IndexKeySpec = { name: 1, email: -1 };
  const indexOptions: CreateIndexOptions = { unique: true, sparse: true };

  const matchStage: MatchStage = { $match: { age: { $gte: 18 } } };
  const projectStage: ProjectStage = { $project: { name: 1 } };
  const sortStage: SortStage = { $sort: { age: -1 } };
  const limitStage: LimitStage = { $limit: 10 };
  const skipStage: SkipStage = { $skip: 5 };
  const countStage: CountStage = { $count: 'total' };

  const unwindOptions: UnwindOptions = {
    path: '$tags',
    preserveNullAndEmptyArrays: true,
    includeArrayIndex: 'idx',
  };
  const unwindStage: UnwindStage = { $unwind: unwindOptions };

  // PipelineStage union type accepts all stage types
  const pipeline: PipelineStage[] = [matchStage, projectStage, sortStage, limitStage];
}
