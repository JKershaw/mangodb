# Troubleshooting Guide

Common issues and their solutions when using MangoDB.

## Installation Issues

### "Cannot find module '@jkershaw/mangodb'"

**Cause**: Package not installed or wrong import path.

**Solution**:
```bash
npm install @jkershaw/mangodb
```

Verify installation:
```bash
npm ls @jkershaw/mangodb
```

### "This package requires Node.js >= 22.0.0"

**Cause**: Node.js version too old.

**Solution**:
```bash
# Check version
node --version

# Update Node.js
nvm install 22
nvm use 22
```

---

## Runtime Errors

### "ENOENT: no such file or directory"

**Cause**: Data directory doesn't exist or isn't writable.

**Solution**:
```typescript
import fs from 'node:fs';

const dataDir = './data';

// Ensure directory exists
fs.mkdirSync(dataDir, { recursive: true });

const client = new MangoClient(dataDir);
```

### "EACCES: permission denied"

**Cause**: No write permission to data directory.

**Solution**:
```bash
# Check permissions
ls -la ./data

# Fix permissions
chmod 755 ./data
```

### "E11000 duplicate key error"

**Cause**: Inserting a document that violates a unique index.

**Solution**:
```typescript
try {
  await collection.insertOne({ email: 'existing@example.com' });
} catch (err) {
  if (err.code === 11000) {
    // Handle duplicate - update instead or use different value
    console.log('Email already exists');
  }
}
```

Or use upsert:
```typescript
await collection.updateOne(
  { email: 'user@example.com' },
  { $set: { name: 'Updated Name' } },
  { upsert: true }
);
```

### "index not found with name"

**Cause**: Trying to drop an index that doesn't exist.

**Solution**:
```typescript
// Check existing indexes first
const indexes = await collection.indexes();
console.log(indexes);

// Or use try/catch
try {
  await collection.dropIndex('nonexistent_1');
} catch (err) {
  // Index doesn't exist, that's ok
}
```

### "cannot drop _id index"

**Cause**: Attempting to drop the default `_id_` index.

**Solution**: The `_id` index cannot be dropped. It's required for document identification.

### "$near requires a 2d or 2dsphere index"

**Cause**: Using `$near` or `$nearSphere` without a geospatial index.

**Solution**:
```typescript
// Create the index first
await collection.createIndex({ location: '2dsphere' });

// Then query
await collection.find({
  location: {
    $near: {
      $geometry: { type: 'Point', coordinates: [lng, lat] },
      $maxDistance: 1000
    }
  }
}).toArray();
```

### "text index required for $text query"

**Cause**: Using `$text` search without a text index.

**Solution**:
```typescript
// Create text index
await collection.createIndex({ content: 'text' });

// Then search
await collection.find({ $text: { $search: 'keywords' } }).toArray();
```

### "The field 'X' must be an array"

**Cause**: Using array operators (`$push`, `$pull`, etc.) on non-array fields.

**Solution**: Ensure the field is an array, or initialize it:
```typescript
// Initialize as array if needed
await collection.updateOne(
  { _id: docId },
  { $set: { tags: [] } }
);

// Then push
await collection.updateOne(
  { _id: docId },
  { $push: { tags: 'new-tag' } }
);
```

---

## Query Issues

### Query returns nothing but data exists

**Possible causes**:

1. **Type mismatch**: Query value type differs from stored value.
   ```typescript
   // Stored: { count: 5 } (number)
   await collection.find({ count: '5' });  // No match - string vs number
   await collection.find({ count: 5 });    // Match
   ```

2. **ObjectId not constructed**:
   ```typescript
   // Wrong - string comparison
   await collection.find({ _id: '507f1f77bcf86cd799439011' });

   // Correct - ObjectId comparison
   import { ObjectId } from 'bson';
   await collection.find({ _id: new ObjectId('507f1f77bcf86cd799439011') });
   ```

3. **Case sensitivity**:
   ```typescript
   // Stored: { name: 'Alice' }
   await collection.find({ name: 'alice' });  // No match

   // Use regex for case-insensitive
   await collection.find({ name: { $regex: /^alice$/i } });
   ```

### Query returns too many results

**Cause**: Overly broad filter or missing conditions.

**Solution**: Add more specific conditions:
```typescript
// Too broad
await collection.find({}).toArray();

// More specific
await collection.find({
  status: 'active',
  createdAt: { $gte: new Date('2024-01-01') }
}).toArray();
```

### Sorting doesn't work as expected

**Possible causes**:

1. **Mixed types in field**: See [Edge Cases - BSON Type Ordering](./EDGE-CASES.md#bson-type-ordering).

2. **Null/missing values**: Nulls and missing sort before all other values in ascending order.
   ```typescript
   // Filter out nulls if needed
   await collection.find({ field: { $ne: null } })
     .sort({ field: 1 })
     .toArray();
   ```

3. **Array fields**: Arrays use min element for ascending, max for descending.

---

## Performance Issues

### Operations are slow

**Cause**: Large dataset loaded into memory.

**Solution**: MangoDB is designed for small datasets. For large data:

1. **Reduce dataset size**:
   ```typescript
   // Add filters to reduce documents processed
   await collection.find({ status: 'active' })
     .limit(100)
     .toArray();
   ```

2. **Use MongoDB for large datasets**: Switch to real MongoDB for production workloads.

3. **Split data**: Use separate collections or databases for different data types.

### Test suite is slow

**Solution**:

1. **Use smaller test fixtures**:
   ```typescript
   // Instead of 1000 documents
   await collection.insertMany(Array(10).fill({ test: true }));
   ```

2. **Isolate tests with temp directories**:
   ```typescript
   const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'test-'));
   // Each test file gets fresh, empty database
   ```

3. **Parallel test isolation**: Ensure parallel tests use different data directories.

---

## Data Issues

### Data not persisting

**Cause**: Client not connected or closed prematurely.

**Solution**:
```typescript
const client = new MangoClient('./data');
await client.connect();  // Don't forget this!

// ... operations ...

// Don't close until done
await client.close();
```

### Data looks corrupted in files

**Cause**: Manual editing broke JSON format, or concurrent writes.

**Solution**:

1. **Validate JSON**:
   ```bash
   cat ./data/mydb/collection.json | jq .
   ```

2. **Restore from backup** or recreate data.

3. **Avoid concurrent writes**: MangoDB isn't safe for multiple processes writing simultaneously.

### ObjectIds look wrong in files

**Expected**: ObjectIds are stored as extended JSON:
```json
{ "_id": { "$oid": "507f1f77bcf86cd799439011" } }
```

This is correct and will be parsed back to ObjectId instances.

---

## Feature Not Working

### Operation X doesn't work

**Check compatibility**:

1. Review [COMPATIBILITY.md](./COMPATIBILITY.md) for feature support.
2. Check if the feature is a MongoDB-only feature (transactions, change streams, etc.).

### $lookup returns empty results

**Cause**: `$lookup` only supports basic form.

**Not supported**:
```javascript
// Pipeline form - NOT supported
{ $lookup: { from: 'coll', let: { x: '$a' }, pipeline: [...], as: 'results' } }
```

**Supported**:
```javascript
// Basic form - supported
{ $lookup: { from: 'coll', localField: 'a', foreignField: 'b', as: 'results' } }
```

### Aggregation expression returns null

**Common causes**:

1. **Field path typo**: Check `$fieldName` spelling.
2. **Missing field**: Use `$ifNull` for defaults:
   ```javascript
   { $ifNull: ['$maybeNull', 0] }
   ```
3. **Null propagation**: Many operators return null for null input.

---

## Debugging Tips

### Inspect stored data

```typescript
import fs from 'node:fs';
import path from 'node:path';

function inspectCollection(dataDir: string, dbName: string, collName: string) {
  const filePath = path.join(dataDir, dbName, `${collName}.json`);
  const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
  console.log(JSON.stringify(data, null, 2));
}

inspectCollection('./data', 'myapp', 'users');
```

### Log query results

```typescript
const results = await collection.find({ status: 'active' }).toArray();
console.log('Found:', results.length, 'documents');
console.log('First result:', JSON.stringify(results[0], null, 2));
```

### Verify index exists

```typescript
const indexes = await collection.indexes();
console.log('Indexes:', indexes.map(i => i.name));
```

### Compare with MongoDB

Run the same query against real MongoDB to verify expected behavior:

```bash
# Set MongoDB URI and run tests
MONGODB_URI=mongodb://localhost:27017 npm test
```

---

## Getting Help

If your issue isn't covered here:

1. Check the [Edge Cases](./EDGE-CASES.md) documentation
2. Review MongoDB documentation for expected behavior
3. Open an issue at https://github.com/JKershaw/mangodb/issues
