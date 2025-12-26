/**
 * Cleanup script for Atlas - drops any leftover _mtest_* test databases
 * Safe to run because _mtest_ prefix is exclusively for tests
 */
import { MongoClient } from "mongodb";

const uri = process.env.MONGODB_URI;
if (!uri) {
  process.exit(0);
}

const client = new MongoClient(uri);

try {
  await client.connect();
  const { databases } = await client.db().admin().listDatabases();
  const testDbs = databases.filter((d) => d.name.startsWith("_mtest_"));

  if (testDbs.length > 0) {
    console.log(`Cleaning ${testDbs.length} leftover test db(s)...`);
    for (const db of testDbs) {
      await client.db(db.name).dropDatabase();
    }
  }
} catch {
  // Ignore errors - cleanup is best-effort
} finally {
  await client.close();
}
