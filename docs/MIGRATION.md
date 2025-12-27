# Migration Guide

This guide explains how to switch between MangoDB and MongoDB. The goal is zero code changes - only configuration differs.

## Basic Setup

### MangoDB (Development/Testing)

```typescript
import { MangoClient } from '@jkershaw/mangodb';

const client = new MangoClient('./data');
await client.connect();

const db = client.db('myapp');
const users = db.collection('users');
```

### MongoDB (Production)

```typescript
import { MongoClient } from 'mongodb';

const client = new MongoClient('mongodb://localhost:27017');
await client.connect();

const db = client.db('myapp');
const users = db.collection('users');
```

## Environment-Based Switching

The recommended pattern uses an environment variable to switch implementations:

```typescript
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

function createClient() {
  if (process.env.MONGODB_URI) {
    return new MongoClient(process.env.MONGODB_URI);
  }
  return new MangoClient(process.env.DATA_DIR || './data');
}

const client = createClient();
await client.connect();
```

### Environment Files

**.env.development**
```bash
# No MONGODB_URI - uses MangoDB
DATA_DIR=./dev-data
```

**.env.production**
```bash
MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/myapp
```

**.env.test**
```bash
# No MONGODB_URI - uses MangoDB
DATA_DIR=./test-data
```

## TypeScript Types

Both clients expose compatible interfaces. For type safety across both:

```typescript
import type { MongoClient, Db, Collection, Document } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

// The MangoClient interface matches MongoClient for common operations
type AppClient = MongoClient | MangoClient;

async function getUsers(client: AppClient): Promise<Document[]> {
  const db = client.db('myapp');
  const users = db.collection('users');
  return users.find({}).toArray();
}
```

## Factory Pattern

For dependency injection or more complex setups:

```typescript
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

interface DatabaseConfig {
  type: 'mongodb' | 'mangodb';
  uri?: string;
  dataDir?: string;
}

class DatabaseFactory {
  static create(config: DatabaseConfig) {
    if (config.type === 'mongodb' && config.uri) {
      return new MongoClient(config.uri);
    }
    return new MangoClient(config.dataDir || './data');
  }
}

// Usage
const config: DatabaseConfig = {
  type: process.env.MONGODB_URI ? 'mongodb' : 'mangodb',
  uri: process.env.MONGODB_URI,
  dataDir: './data'
};

const client = DatabaseFactory.create(config);
```

## Express/Fastify Integration

### Express Example

```typescript
import express from 'express';
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

const app = express();

const client = process.env.MONGODB_URI
  ? new MongoClient(process.env.MONGODB_URI)
  : new MangoClient('./data');

app.locals.db = null;

app.use(async (req, res, next) => {
  if (!app.locals.db) {
    await client.connect();
    app.locals.db = client.db('myapp');
  }
  req.db = app.locals.db;
  next();
});

app.get('/users', async (req, res) => {
  const users = await req.db.collection('users').find({}).toArray();
  res.json(users);
});
```

### Fastify Example

```typescript
import Fastify from 'fastify';
import { MongoClient } from 'mongodb';
import { MangoClient } from '@jkershaw/mangodb';

const fastify = Fastify();

const client = process.env.MONGODB_URI
  ? new MongoClient(process.env.MONGODB_URI)
  : new MangoClient('./data');

fastify.decorate('db', null);

fastify.addHook('onReady', async () => {
  await client.connect();
  fastify.db = client.db('myapp');
});

fastify.get('/users', async () => {
  return fastify.db.collection('users').find({}).toArray();
});
```

## CI/CD Configuration

### GitHub Actions

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test-mangodb:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - run: npm ci
      - run: npm test
        # No MONGODB_URI - runs against MangoDB

  test-mongodb:
    runs-on: ubuntu-latest
    services:
      mongodb:
        image: mongo:7
        ports:
          - 27017:27017
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - run: npm ci
      - run: npm test
        env:
          MONGODB_URI: mongodb://localhost:27017
```

### Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  app:
    build: .
    environment:
      - MONGODB_URI=mongodb://mongo:27017/myapp
    depends_on:
      - mongo

  mongo:
    image: mongo:7
    volumes:
      - mongo-data:/data/db

volumes:
  mongo-data:
```

```yaml
# docker-compose.dev.yml - No MongoDB needed
version: '3.8'

services:
  app:
    build: .
    volumes:
      - ./data:/app/data
    # No MONGODB_URI - uses MangoDB with ./data directory
```

## Data Directory Structure

When using MangoDB, data is stored as JSON files:

```
./data/
  myapp/                    # Database name
    users.json              # Collection data
    users.indexes.json      # Index metadata
    orders.json
    orders.indexes.json
```

Each collection file contains an array of documents. Index files contain index definitions.

## Migrating Existing Data

### Export from MongoDB

```bash
# Export a collection to JSON
mongoexport --uri="mongodb://localhost:27017/myapp" \
  --collection=users \
  --out=users.json \
  --jsonArray
```

### Import to MangoDB

Place the exported JSON file in the data directory:

```bash
mkdir -p ./data/myapp
mv users.json ./data/myapp/users.json
```

**Note**: Ensure ObjectIds are serialized as `{ "$oid": "..." }` format, which is the default mongoexport format.

### Export from MangoDB to MongoDB

```bash
# Import MangoDB JSON to MongoDB
mongoimport --uri="mongodb://localhost:27017/myapp" \
  --collection=users \
  --file=./data/myapp/users.json \
  --jsonArray
```

## Checklist Before Switching to Production

Before deploying with real MongoDB:

1. **Run tests against MongoDB** - Set `MONGODB_URI` and run your test suite
2. **Check feature usage** - Review [COMPATIBILITY.md](./COMPATIBILITY.md) for unsupported features
3. **Verify indexes** - Ensure required indexes exist in production MongoDB
4. **Test with real data volume** - MangoDB is optimized for small datasets
5. **Remove MangoDB from production dependencies** - It's only needed for dev/test
