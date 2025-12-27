# Integration Examples

Real-world examples showing MangoDB integration with popular frameworks and tools.

## Express.js REST API

```typescript
// app.ts
import express from 'express';
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';
import { ObjectId } from 'bson';

const app = express();
app.use(express.json());

// Database setup
const client = process.env.MONGODB_URI
  ? new MongoClient(process.env.MONGODB_URI)
  : new MangoClient('./data');

let db: ReturnType<typeof client.db>;

// Initialize database connection
async function initDb() {
  await client.connect();
  db = client.db('myapp');

  // Create indexes (idempotent)
  await db.collection('users').createIndex({ email: 1 }, { unique: true });
  await db.collection('posts').createIndex({ authorId: 1 });
  await db.collection('posts').createIndex({ createdAt: -1 });
}

// Routes
app.get('/users', async (req, res) => {
  const users = await db.collection('users')
    .find({})
    .project({ password: 0 })
    .toArray();
  res.json(users);
});

app.get('/users/:id', async (req, res) => {
  const user = await db.collection('users').findOne(
    { _id: new ObjectId(req.params.id) },
    { projection: { password: 0 } }
  );
  if (!user) return res.status(404).json({ error: 'User not found' });
  res.json(user);
});

app.post('/users', async (req, res) => {
  try {
    const result = await db.collection('users').insertOne({
      ...req.body,
      createdAt: new Date()
    });
    res.status(201).json({ id: result.insertedId });
  } catch (err: any) {
    if (err.code === 11000) {
      return res.status(409).json({ error: 'Email already exists' });
    }
    throw err;
  }
});

app.put('/users/:id', async (req, res) => {
  const result = await db.collection('users').updateOne(
    { _id: new ObjectId(req.params.id) },
    { $set: { ...req.body, updatedAt: new Date() } }
  );
  if (result.matchedCount === 0) {
    return res.status(404).json({ error: 'User not found' });
  }
  res.json({ updated: result.modifiedCount });
});

app.delete('/users/:id', async (req, res) => {
  const result = await db.collection('users').deleteOne({
    _id: new ObjectId(req.params.id)
  });
  if (result.deletedCount === 0) {
    return res.status(404).json({ error: 'User not found' });
  }
  res.status(204).send();
});

// Start server
initDb().then(() => {
  app.listen(3000, () => console.log('Server running on port 3000'));
});
```

## Fastify with TypeScript

```typescript
// server.ts
import Fastify from 'fastify';
import { MongoClient, ObjectId } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

interface User {
  _id?: ObjectId;
  name: string;
  email: string;
  createdAt: Date;
}

const fastify = Fastify({ logger: true });

const client = process.env.MONGODB_URI
  ? new MongoClient(process.env.MONGODB_URI)
  : new MangoClient('./data');

fastify.addHook('onReady', async () => {
  await client.connect();
  fastify.decorate('db', client.db('myapp'));
});

fastify.addHook('onClose', async () => {
  await client.close();
});

// Declare type augmentation
declare module 'fastify' {
  interface FastifyInstance {
    db: ReturnType<typeof client.db>;
  }
}

// Routes
fastify.get<{ Params: { id: string } }>('/users/:id', async (request, reply) => {
  const user = await fastify.db.collection<User>('users').findOne({
    _id: new ObjectId(request.params.id)
  });
  if (!user) {
    return reply.status(404).send({ error: 'Not found' });
  }
  return user;
});

fastify.post<{ Body: Omit<User, '_id' | 'createdAt'> }>('/users', async (request, reply) => {
  const result = await fastify.db.collection<User>('users').insertOne({
    ...request.body,
    createdAt: new Date()
  });
  return reply.status(201).send({ id: result.insertedId });
});

fastify.listen({ port: 3000 });
```

## NestJS Module

```typescript
// database.module.ts
import { Module, Global, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

export const DATABASE_CONNECTION = 'DATABASE_CONNECTION';

const dbProvider = {
  provide: DATABASE_CONNECTION,
  useFactory: async () => {
    const client = process.env.MONGODB_URI
      ? new MongoClient(process.env.MONGODB_URI)
      : new MangoClient('./data');

    await client.connect();
    return client.db('myapp');
  },
};

@Global()
@Module({
  providers: [dbProvider],
  exports: [dbProvider],
})
export class DatabaseModule {}

// users.service.ts
import { Injectable, Inject } from '@nestjs/common';
import { Db, ObjectId } from 'mongodb';
import { DATABASE_CONNECTION } from './database.module';

@Injectable()
export class UsersService {
  constructor(@Inject(DATABASE_CONNECTION) private db: Db) {}

  async findAll() {
    return this.db.collection('users').find({}).toArray();
  }

  async findOne(id: string) {
    return this.db.collection('users').findOne({ _id: new ObjectId(id) });
  }

  async create(data: { name: string; email: string }) {
    const result = await this.db.collection('users').insertOne({
      ...data,
      createdAt: new Date(),
    });
    return result.insertedId;
  }
}
```

## Repository Pattern

```typescript
// base.repository.ts
import { Collection, Document, Filter, OptionalUnlessRequiredId, ObjectId } from 'mongodb';

export abstract class BaseRepository<T extends Document> {
  constructor(protected collection: Collection<T>) {}

  async findById(id: string | ObjectId): Promise<T | null> {
    return this.collection.findOne({ _id: new ObjectId(id) } as Filter<T>);
  }

  async findAll(filter: Filter<T> = {}): Promise<T[]> {
    return this.collection.find(filter).toArray();
  }

  async create(data: OptionalUnlessRequiredId<T>): Promise<ObjectId> {
    const result = await this.collection.insertOne(data);
    return result.insertedId;
  }

  async update(id: string | ObjectId, data: Partial<T>): Promise<boolean> {
    const result = await this.collection.updateOne(
      { _id: new ObjectId(id) } as Filter<T>,
      { $set: data }
    );
    return result.modifiedCount > 0;
  }

  async delete(id: string | ObjectId): Promise<boolean> {
    const result = await this.collection.deleteOne({
      _id: new ObjectId(id)
    } as Filter<T>);
    return result.deletedCount > 0;
  }
}

// user.repository.ts
interface User {
  _id?: ObjectId;
  name: string;
  email: string;
  role: 'admin' | 'user';
  createdAt: Date;
}

export class UserRepository extends BaseRepository<User> {
  async findByEmail(email: string): Promise<User | null> {
    return this.collection.findOne({ email });
  }

  async findAdmins(): Promise<User[]> {
    return this.collection.find({ role: 'admin' }).toArray();
  }

  async updateLastLogin(id: string | ObjectId): Promise<void> {
    await this.collection.updateOne(
      { _id: new ObjectId(id) },
      { $set: { lastLoginAt: new Date() } }
    );
  }
}

// Usage
const db = client.db('myapp');
const userRepo = new UserRepository(db.collection('users'));

const user = await userRepo.findByEmail('alice@example.com');
const admins = await userRepo.findAdmins();
```

## CLI Tool with Commander

```typescript
// cli.ts
import { Command } from 'commander';
import { MangoClient } from '@jkershaw/mangodb';

const program = new Command();
const client = new MangoClient('./data');

program
  .name('myapp')
  .description('CLI tool with MangoDB');

program
  .command('list-users')
  .description('List all users')
  .action(async () => {
    await client.connect();
    const users = await client.db('myapp').collection('users').find({}).toArray();
    console.table(users.map(u => ({ id: u._id.toString(), name: u.name, email: u.email })));
    await client.close();
  });

program
  .command('add-user')
  .description('Add a new user')
  .requiredOption('-n, --name <name>', 'User name')
  .requiredOption('-e, --email <email>', 'User email')
  .action(async (options) => {
    await client.connect();
    const result = await client.db('myapp').collection('users').insertOne({
      name: options.name,
      email: options.email,
      createdAt: new Date()
    });
    console.log(`Created user with ID: ${result.insertedId}`);
    await client.close();
  });

program
  .command('stats')
  .description('Show database statistics')
  .action(async () => {
    await client.connect();
    const db = client.db('myapp');
    const collections = await db.listCollections().toArray();

    for (const coll of collections) {
      const stats = await db.collection(coll.name).stats();
      console.log(`${coll.name}: ${stats.count} documents`);
    }
    await client.close();
  });

program.parse();
```

## Aggregation Examples

### Sales Report

```typescript
async function getSalesReport(db: Db, startDate: Date, endDate: Date) {
  return db.collection('orders').aggregate([
    {
      $match: {
        createdAt: { $gte: startDate, $lte: endDate },
        status: 'completed'
      }
    },
    {
      $group: {
        _id: {
          year: { $year: '$createdAt' },
          month: { $month: '$createdAt' }
        },
        totalOrders: { $sum: 1 },
        totalRevenue: { $sum: '$total' },
        averageOrder: { $avg: '$total' }
      }
    },
    {
      $sort: { '_id.year': 1, '_id.month': 1 }
    },
    {
      $project: {
        _id: 0,
        period: {
          $concat: [
            { $toString: '$_id.year' },
            '-',
            { $toString: '$_id.month' }
          ]
        },
        totalOrders: 1,
        totalRevenue: { $round: ['$totalRevenue', 2] },
        averageOrder: { $round: ['$averageOrder', 2] }
      }
    }
  ]).toArray();
}
```

### User Activity Dashboard

```typescript
async function getUserActivity(db: Db, userId: ObjectId) {
  const [result] = await db.collection('events').aggregate([
    { $match: { userId } },
    {
      $facet: {
        recentEvents: [
          { $sort: { timestamp: -1 } },
          { $limit: 10 },
          { $project: { type: 1, timestamp: 1 } }
        ],
        eventCounts: [
          { $group: { _id: '$type', count: { $sum: 1 } } }
        ],
        dailyActivity: [
          {
            $group: {
              _id: { $dateToString: { format: '%Y-%m-%d', date: '$timestamp' } },
              count: { $sum: 1 }
            }
          },
          { $sort: { _id: -1 } },
          { $limit: 30 }
        ]
      }
    }
  ]).toArray();

  return result;
}
```

### Leaderboard with Ranking

```typescript
async function getLeaderboard(db: Db, limit = 10) {
  return db.collection('scores').aggregate([
    { $sort: { score: -1 } },
    { $limit: limit },
    {
      $setWindowFields: {
        sortBy: { score: -1 },
        output: {
          rank: { $rank: {} }
        }
      }
    },
    {
      $lookup: {
        from: 'users',
        localField: 'userId',
        foreignField: '_id',
        as: 'user'
      }
    },
    { $unwind: '$user' },
    {
      $project: {
        rank: 1,
        score: 1,
        playerName: '$user.name',
        achievedAt: '$createdAt'
      }
    }
  ]).toArray();
}
```

## Geospatial Example

```typescript
// Store locations with coordinates
async function setupLocations(db: Db) {
  const locations = db.collection('locations');

  await locations.createIndex({ coordinates: '2dsphere' });

  await locations.insertMany([
    {
      name: 'Central Park',
      type: 'park',
      coordinates: { type: 'Point', coordinates: [-73.965355, 40.782865] }
    },
    {
      name: 'Empire State Building',
      type: 'landmark',
      coordinates: { type: 'Point', coordinates: [-73.985428, 40.748817] }
    }
  ]);
}

// Find nearby locations
async function findNearby(db: Db, longitude: number, latitude: number, maxDistanceMeters: number) {
  return db.collection('locations').find({
    coordinates: {
      $near: {
        $geometry: { type: 'Point', coordinates: [longitude, latitude] },
        $maxDistance: maxDistanceMeters
      }
    }
  }).toArray();
}

// Find locations within a polygon
async function findInArea(db: Db, polygon: number[][]) {
  return db.collection('locations').find({
    coordinates: {
      $geoWithin: {
        $geometry: {
          type: 'Polygon',
          coordinates: [polygon]
        }
      }
    }
  }).toArray();
}
```

## Text Search Example

```typescript
async function setupSearch(db: Db) {
  const articles = db.collection('articles');

  // Create text index with weights
  await articles.createIndex(
    { title: 'text', content: 'text', tags: 'text' },
    { weights: { title: 10, tags: 5, content: 1 } }
  );
}

async function searchArticles(db: Db, query: string) {
  return db.collection('articles').find({
    $text: { $search: query }
  }).toArray();
}
```
