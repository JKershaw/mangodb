/**
 * Complex Pipeline E2E Tests
 *
 * Tests for multi-stage aggregation pipelines that combine multiple operators
 * to simulate real-world data processing scenarios.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createTestClient, getTestModeName, type TestClient } from '../test-harness.ts';

describe(`Complex Pipeline Scenarios (${getTestModeName()})`, () => {
  let client: TestClient;
  let cleanup: () => Promise<void>;
  let dbName: string;

  before(async () => {
    const result = await createTestClient();
    client = result.client;
    cleanup = result.cleanup;
    dbName = result.dbName;
    await client.connect();
  });

  after(async () => {
    await cleanup();
  });

  describe('E-commerce Analytics', () => {
    it('should calculate sales summary by category with filtering', async () => {
      const collection = client.db(dbName).collection('ecom_sales');
      await collection.insertMany([
        {
          product: 'Laptop',
          category: 'Electronics',
          price: 1000,
          qty: 2,
          date: new Date('2023-06-15'),
        },
        {
          product: 'Phone',
          category: 'Electronics',
          price: 500,
          qty: 5,
          date: new Date('2023-06-16'),
        },
        {
          product: 'Shirt',
          category: 'Clothing',
          price: 30,
          qty: 10,
          date: new Date('2023-06-15'),
        },
        { product: 'Pants', category: 'Clothing', price: 50, qty: 8, date: new Date('2023-06-16') },
        {
          product: 'Tablet',
          category: 'Electronics',
          price: 300,
          qty: 3,
          date: new Date('2023-06-17'),
        },
      ]);

      const results = await collection
        .aggregate([
          { $match: { price: { $gt: 40 } } },
          {
            $group: {
              _id: '$category',
              totalRevenue: { $sum: { $multiply: ['$price', '$qty'] } },
              avgPrice: { $avg: '$price' },
              count: { $sum: 1 },
            },
          },
          { $sort: { totalRevenue: -1 } },
          {
            $project: {
              category: '$_id',
              totalRevenue: 1,
              avgPrice: { $round: ['$avgPrice', 2] },
              count: 1,
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0].category, 'Electronics');
      assert.strictEqual(results[0].totalRevenue, 5400); // 2000 + 2500 + 900
      assert.strictEqual(results[1].category, 'Clothing');
      assert.strictEqual(results[1].totalRevenue, 400); // 400 (pants only, shirt filtered out)
    });

    it('should create product ranking with window functions', async () => {
      const collection = client.db(dbName).collection('ecom_ranking');
      await collection.insertMany([
        { product: 'A', category: 'X', sales: 100 },
        { product: 'B', category: 'X', sales: 200 },
        { product: 'C', category: 'X', sales: 150 },
        { product: 'D', category: 'Y', sales: 300 },
        { product: 'E', category: 'Y', sales: 250 },
      ]);

      const results = await collection
        .aggregate([
          { $sort: { category: 1, sales: -1 } },
          {
            $setWindowFields: {
              partitionBy: { category: '$category' },
              sortBy: { sales: -1 },
              output: { rank: { $rank: {} } },
            },
          },
          { $project: { _id: 0, product: 1, category: 1, sales: 1, rank: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 5);
      const catX = results.filter((r) => r.category === 'X');
      const catY = results.filter((r) => r.category === 'Y');
      assert.strictEqual(catX.find((r) => r.product === 'B')?.rank, 1);
      assert.strictEqual(catY.find((r) => r.product === 'D')?.rank, 1);
    });
  });

  describe('Time Series Analysis', () => {
    it('should calculate daily aggregates with date grouping', async () => {
      const collection = client.db(dbName).collection('timeseries_daily');
      await collection.insertMany([
        { timestamp: new Date('2023-06-15T10:00:00Z'), value: 100 },
        { timestamp: new Date('2023-06-15T14:00:00Z'), value: 150 },
        { timestamp: new Date('2023-06-16T09:00:00Z'), value: 200 },
        { timestamp: new Date('2023-06-16T15:00:00Z'), value: 180 },
        { timestamp: new Date('2023-06-16T20:00:00Z'), value: 220 },
      ]);

      const results = await collection
        .aggregate([
          {
            $group: {
              _id: { $dateToString: { format: '%Y-%m-%d', date: '$timestamp' } },
              total: { $sum: '$value' },
              avg: { $avg: '$value' },
              count: { $sum: 1 },
              min: { $min: '$value' },
              max: { $max: '$value' },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0]._id, '2023-06-15');
      assert.strictEqual(results[0].total, 250);
      assert.strictEqual(results[0].count, 2);
      assert.strictEqual(results[1]._id, '2023-06-16');
      assert.strictEqual(results[1].total, 600);
      assert.strictEqual(results[1].count, 3);
    });

    it('should compute running totals with setWindowFields', async () => {
      const collection = client.db(dbName).collection('timeseries_running');
      await collection.insertMany([
        { date: new Date('2023-01-01'), value: 10 },
        { date: new Date('2023-01-02'), value: 20 },
        { date: new Date('2023-01-03'), value: 15 },
        { date: new Date('2023-01-04'), value: 25 },
      ]);

      const results = await collection
        .aggregate([
          { $sort: { date: 1 } },
          {
            $setWindowFields: {
              sortBy: { date: 1 },
              output: {
                runningTotal: {
                  $sum: '$value',
                  window: { documents: ['unbounded', 'current'] },
                },
              },
            },
          },
          { $project: { _id: 0, date: 1, value: 1, runningTotal: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      assert.strictEqual(results[0].runningTotal, 10);
      assert.strictEqual(results[1].runningTotal, 30);
      assert.strictEqual(results[2].runningTotal, 45);
      assert.strictEqual(results[3].runningTotal, 70);
    });
  });

  describe('Hierarchical Data', () => {
    it('should unwind nested arrays and group results', async () => {
      const collection = client.db(dbName).collection('hier_orders');
      await collection.insertMany([
        {
          orderId: 1,
          items: [
            { product: 'A', price: 10, qty: 2 },
            { product: 'B', price: 20, qty: 1 },
          ],
        },
        {
          orderId: 2,
          items: [
            { product: 'A', price: 10, qty: 3 },
            { product: 'C', price: 15, qty: 2 },
          ],
        },
      ]);

      const results = await collection
        .aggregate([
          { $unwind: '$items' },
          {
            $group: {
              _id: '$items.product',
              totalQty: { $sum: '$items.qty' },
              totalRevenue: { $sum: { $multiply: ['$items.price', '$items.qty'] } },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0]._id, 'A');
      assert.strictEqual(results[0].totalQty, 5);
      assert.strictEqual(results[0].totalRevenue, 50);
      assert.strictEqual(results[1]._id, 'B');
      assert.strictEqual(results[1].totalQty, 1);
    });

    it('should reconstruct hierarchical data with $push', async () => {
      const collection = client.db(dbName).collection('hier_reconstruct');
      await collection.insertMany([
        { category: 'Electronics', product: 'Laptop', price: 1000 },
        { category: 'Electronics', product: 'Phone', price: 500 },
        { category: 'Clothing', product: 'Shirt', price: 30 },
        { category: 'Clothing', product: 'Pants', price: 50 },
      ]);

      const results = await collection
        .aggregate([
          {
            $group: {
              _id: '$category',
              products: { $push: { name: '$product', price: '$price' } },
              totalProducts: { $sum: 1 },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0]._id, 'Clothing');
      assert.strictEqual(results[0].totalProducts, 2);
      assert.strictEqual((results[0].products as unknown[]).length, 2);
      assert.strictEqual(results[1]._id, 'Electronics');
      assert.strictEqual(results[1].totalProducts, 2);
    });
  });

  describe('Multi-Collection Operations', () => {
    it('should perform lookup join between collections', async () => {
      const orders = client.db(dbName).collection('lookup_orders');
      const customers = client.db(dbName).collection('lookup_customers');

      await customers.insertMany([
        { _id: 'c1', name: 'Alice', tier: 'gold' },
        { _id: 'c2', name: 'Bob', tier: 'silver' },
      ]);

      await orders.insertMany([
        { orderId: 1, customerId: 'c1', total: 100 },
        { orderId: 2, customerId: 'c1', total: 200 },
        { orderId: 3, customerId: 'c2', total: 50 },
      ]);

      const results = await orders
        .aggregate([
          {
            $lookup: {
              from: 'lookup_customers',
              localField: 'customerId',
              foreignField: '_id',
              as: 'customer',
            },
          },
          { $unwind: '$customer' },
          {
            $group: {
              _id: '$customer.name',
              orderCount: { $sum: 1 },
              totalSpent: { $sum: '$total' },
              tier: { $first: '$customer.tier' },
            },
          },
          { $sort: { totalSpent: -1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0]._id, 'Alice');
      assert.strictEqual(results[0].orderCount, 2);
      assert.strictEqual(results[0].totalSpent, 300);
      assert.strictEqual(results[0].tier, 'gold');
    });

    it('should combine collections with unionWith', async () => {
      const sales2022 = client.db(dbName).collection('union_sales_2022');
      const sales2023 = client.db(dbName).collection('union_sales_2023');

      await sales2022.insertMany([
        { product: 'A', amount: 100, year: 2022 },
        { product: 'B', amount: 200, year: 2022 },
      ]);

      await sales2023.insertMany([
        { product: 'A', amount: 150, year: 2023 },
        { product: 'C', amount: 300, year: 2023 },
      ]);

      const results = await sales2022
        .aggregate([
          { $unionWith: { coll: 'union_sales_2023' } },
          {
            $group: {
              _id: '$product',
              totalAmount: { $sum: '$amount' },
              years: { $addToSet: '$year' },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0]._id, 'A');
      assert.strictEqual(results[0].totalAmount, 250);
      assert.ok((results[0].years as number[]).includes(2022));
      assert.ok((results[0].years as number[]).includes(2023));
    });
  });

  describe('Data Transformation', () => {
    it('should transform document structure with $project and $addFields', async () => {
      const collection = client.db(dbName).collection('transform_user');
      await collection.insertMany([
        { firstName: 'John', lastName: 'Doe', email: 'john@example.com', age: 30 },
        { firstName: 'Jane', lastName: 'Smith', email: 'jane@example.com', age: 25 },
      ]);

      const results = await collection
        .aggregate([
          {
            $addFields: {
              fullName: { $concat: ['$firstName', ' ', '$lastName'] },
              ageGroup: {
                $switch: {
                  branches: [
                    { case: { $lt: ['$age', 25] }, then: 'young' },
                    { case: { $lt: ['$age', 35] }, then: 'adult' },
                  ],
                  default: 'senior',
                },
              },
            },
          },
          {
            $project: {
              fullName: 1,
              email: 1,
              ageGroup: 1,
              _id: 0,
            },
          },
          { $sort: { fullName: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0].fullName, 'Jane Smith');
      assert.strictEqual(results[0].ageGroup, 'adult');
      assert.strictEqual(results[1].fullName, 'John Doe');
      assert.strictEqual(results[1].ageGroup, 'adult');
    });

    it('should reshape array data with $map and $filter', async () => {
      const collection = client.db(dbName).collection('transform_scores');
      await collection.insertMany([
        { student: 'Alice', scores: [85, 90, 78, 92, 65] },
        { student: 'Bob', scores: [70, 75, 80, 85, 90] },
      ]);

      const results = await collection
        .aggregate([
          {
            $project: {
              student: 1,
              passingScores: {
                $filter: {
                  input: '$scores',
                  as: 'score',
                  cond: { $gte: ['$$score', 70] },
                },
              },
              scaledScores: {
                $map: {
                  input: '$scores',
                  as: 'score',
                  in: { $multiply: ['$$score', 1.1] },
                },
              },
              _id: 0,
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual((results[0].passingScores as unknown[]).length, 4); // Alice: 85, 90, 78, 92
      assert.strictEqual((results[1].passingScores as unknown[]).length, 5); // Bob: all >= 70
    });
  });

  describe('Conditional Aggregation', () => {
    it('should apply conditional logic with $cond in group', async () => {
      const collection = client.db(dbName).collection('cond_orders');
      await collection.insertMany([
        { type: 'online', amount: 100 },
        { type: 'store', amount: 200 },
        { type: 'online', amount: 150 },
        { type: 'store', amount: 50 },
        { type: 'online', amount: 300 },
      ]);

      const results = await collection
        .aggregate([
          {
            $group: {
              _id: null,
              totalOnline: {
                $sum: { $cond: [{ $eq: ['$type', 'online'] }, '$amount', 0] },
              },
              totalStore: {
                $sum: { $cond: [{ $eq: ['$type', 'store'] }, '$amount', 0] },
              },
              onlineCount: {
                $sum: { $cond: [{ $eq: ['$type', 'online'] }, 1, 0] },
              },
              storeCount: {
                $sum: { $cond: [{ $eq: ['$type', 'store'] }, 1, 0] },
              },
            },
          },
          { $project: { _id: 0 } },
        ])
        .toArray();

      assert.strictEqual(results[0].totalOnline, 550);
      assert.strictEqual(results[0].totalStore, 250);
      assert.strictEqual(results[0].onlineCount, 3);
      assert.strictEqual(results[0].storeCount, 2);
    });

    it('should use $ifNull for default values', async () => {
      const collection = client.db(dbName).collection('cond_ifnull');
      await collection.insertMany([
        { name: 'A', value: 10 },
        { name: 'B', value: null },
        { name: 'C' }, // missing value field
      ]);

      const results = await collection
        .aggregate([
          {
            $project: {
              name: 1,
              safeValue: { $ifNull: ['$value', 0] },
              _id: 0,
            },
          },
          { $sort: { name: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 3);
      assert.strictEqual(results[0].safeValue, 10);
      assert.strictEqual(results[1].safeValue, 0);
      assert.strictEqual(results[2].safeValue, 0);
    });
  });

  describe('Statistical Analysis', () => {
    it('should calculate comprehensive statistics with multiple accumulators', async () => {
      const collection = client.db(dbName).collection('stats_data');
      await collection.insertMany([
        { group: 'A', value: 10 },
        { group: 'A', value: 20 },
        { group: 'A', value: 30 },
        { group: 'B', value: 100 },
        { group: 'B', value: 200 },
      ]);

      const results = await collection
        .aggregate([
          {
            $group: {
              _id: '$group',
              count: { $sum: 1 },
              sum: { $sum: '$value' },
              avg: { $avg: '$value' },
              min: { $min: '$value' },
              max: { $max: '$value' },
              stdDevPop: { $stdDevPop: '$value' },
            },
          },
          { $sort: { _id: 1 } },
        ])
        .toArray();

      assert.strictEqual(results.length, 2);
      assert.strictEqual(results[0]._id, 'A');
      assert.strictEqual(results[0].count, 3);
      assert.strictEqual(results[0].sum, 60);
      assert.strictEqual(results[0].avg, 20);
      assert.strictEqual(results[0].min, 10);
      assert.strictEqual(results[0].max, 30);
    });

    it('should bucket data into ranges', async () => {
      const collection = client.db(dbName).collection('stats_bucket');
      await collection.insertMany([
        { score: 15 },
        { score: 25 },
        { score: 35 },
        { score: 45 },
        { score: 55 },
        { score: 65 },
        { score: 75 },
        { score: 85 },
        { score: 95 },
      ]);

      const results = await collection
        .aggregate([
          {
            $bucket: {
              groupBy: '$score',
              boundaries: [0, 30, 60, 90, 100],
              default: 'other',
              output: {
                count: { $sum: 1 },
                scores: { $push: '$score' },
              },
            },
          },
        ])
        .toArray();

      assert.strictEqual(results.length, 4);
      assert.strictEqual(results.find((r) => r._id === 0)?.count, 2);
      assert.strictEqual(results.find((r) => r._id === 30)?.count, 3);
      assert.strictEqual(results.find((r) => r._id === 60)?.count, 3);
      assert.strictEqual(results.find((r) => r._id === 90)?.count, 1);
    });
  });
});
