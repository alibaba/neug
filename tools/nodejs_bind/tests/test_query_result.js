/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const { test, after } = require('./test-shim');
const assert = require('assert').strict;
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('neug');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let _tmpCounter = 0;
const _tmpDirs = [];
function makeTmpDir(prefix = 'neug_qr_test_') {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix + _tmpCounter++ + '_'));
  _tmpDirs.push(dir);
  return dir;
}

after(() => {
  for (const dir of _tmpDirs) {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch (_) {}
  }
});

/**
 * Create a database with sample data for query result tests.
 */
function setupTestDb() {
  const dbDir = makeTmpDir('qr_');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, score DOUBLE, active BOOL, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30, score: 95.5, active: true});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25, score: 88.0, active: false});");
  conn.execute("CREATE (p:person {id: 3, name: 'Charlie', age: 35, score: 72.3, active: true});");
  return { db, conn };
}

// ---------------------------------------------------------------------------
// QueryResult.length()
// ---------------------------------------------------------------------------

test('test_query_result_length', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.id;');
  assert.equal(result.length(), 3);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// QueryResult.columnNames()
// ---------------------------------------------------------------------------

test('test_query_result_column_names', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.id AS id, p.name AS name, p.age AS age;');
  const cols = result.columnNames();
  assert.deepEqual(cols, ['id', 'name', 'age']);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// QueryResult.hasNext() / getNext() — sequential iteration
// ---------------------------------------------------------------------------

test('test_query_result_sequential_iteration', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.id ORDER BY p.id;');
  const ids = [];
  while (result.hasNext()) {
    const row = result.getNext();
    ids.push(row[0]);
  }
  assert.deepEqual(ids, [1n, 2n, 3n]);
  // getNext() after exhaustion should throw
  assert.throws(() => result.getNext());
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// QueryResult iterator protocol (for...of)
// ---------------------------------------------------------------------------

test('test_query_result_iterator_protocol', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.name ORDER BY p.id;');
  const names = [];
  for (const row of result) {
    names.push(row[0]);
  }
  assert.deepEqual(names, ['Alice', 'Bob', 'Charlie']);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// QueryResult.getAt(index) — random access
// ---------------------------------------------------------------------------

test('test_query_result_get_at', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.name ORDER BY p.id;');
  assert.equal(result.getAt(0)[0], 'Alice');
  assert.equal(result.getAt(2)[0], 'Charlie');
  assert.equal(result.getAt(1)[0], 'Bob');
  conn.close();
  db.close();
});

test('test_query_result_get_at_negative_index', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.name ORDER BY p.id;');
  // Python-style negative indexing
  assert.equal(result.getAt(-1)[0], 'Charlie');
  assert.equal(result.getAt(-3)[0], 'Alice');
  conn.close();
  db.close();
});

test('test_query_result_get_at_out_of_range', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.name;');
  assert.throws(
    () => result.getAt(100),
    (err) => err instanceof RangeError || err.message.includes('out of range')
  );
  assert.throws(
    () => result.getAt(-100),
    (err) => err instanceof RangeError || err.message.includes('out of range')
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// QueryResult.statusCode() / statusMessage()
// ---------------------------------------------------------------------------

test('test_query_result_status_success', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.id;');
  assert.equal(result.statusCode(), 0);
  assert.equal(typeof result.statusMessage(), 'string');
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// QueryResult.close()
// ---------------------------------------------------------------------------

test('test_query_result_close', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) RETURN p.id;');
  // close should not throw
  result.close();
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Parameterized queries
// ---------------------------------------------------------------------------

test('test_parameterized_query', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute(
    'MATCH (p:person) WHERE p.id = $target_id RETURN p.name;',
    'read',
    { target_id: 2 }
  );
  assert.equal(result.length(), 1);
  assert.equal(result.getAt(0)[0], 'Bob');
  conn.close();
  db.close();
});

test('test_parameterized_query_string_param', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute(
    'MATCH (p:person) WHERE p.name = $name RETURN p.id;',
    'read',
    { name: 'Alice' }
  );
  assert.equal(result.length(), 1);
  assert.equal(result.getAt(0)[0], 1n);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Multiple data types through query results
// ---------------------------------------------------------------------------

test('test_query_result_data_types', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute(
    'MATCH (p:person) WHERE p.id = 1 RETURN p.id, p.name, p.age, p.score, p.active;'
  );
  assert.equal(result.length(), 1);
  const row = result.getAt(0);
  // INT64 (returned as BigInt)
  assert.equal(row[0], 1n);
  // STRING
  assert.equal(row[1], 'Alice');
  // INT32
  assert.equal(row[2], 30);
  // DOUBLE
  assert.ok(Math.abs(row[3] - 95.5) < 0.001);
  // BOOL
  assert.equal(row[4], true);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Multiple queries in single execute (semicolon-separated)
// ---------------------------------------------------------------------------

test('test_multiple_queries_in_single_execute',
  { skip: 'multiple queries in single execute not yet supported' }, () => {
  const dbDir = makeTmpDir('multi_query_');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE animal(id INT64, species STRING, PRIMARY KEY(id));' +
    'CREATE NODE TABLE food(id INT64, name STRING, PRIMARY KEY(id));'
  );
  // Verify both tables exist
  const schema = conn.getSchema();
  assert.ok(schema.includes('animal'));
  assert.ok(schema.includes('food'));
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Empty result set
// ---------------------------------------------------------------------------

test('test_empty_result_set', () => {
  const { db, conn } = setupTestDb();
  const result = conn.execute('MATCH (p:person) WHERE p.id > 1000 RETURN p.name;');
  assert.equal(result.length(), 0);
  assert.equal(result.hasNext(), false);
  // for...of should produce nothing
  const rows = [];
  for (const row of result) {
    rows.push(row);
  }
  assert.equal(rows.length, 0);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Connection.getSchema()
// ---------------------------------------------------------------------------

test('test_connection_get_schema', () => {
  const dbDir = makeTmpDir('schema_');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE city(id INT64, name STRING, population INT64, PRIMARY KEY(id));');
  const schema = conn.getSchema();
  assert.ok(typeof schema === 'string');
  assert.ok(schema.includes('city'));
  assert.ok(schema.includes('name'));
  assert.ok(schema.includes('population'));
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Connection.isOpen
// ---------------------------------------------------------------------------

test('test_connection_is_open', () => {
  const dbDir = makeTmpDir('is_open_');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  assert.equal(conn.isOpen, true);
  conn.close();
  assert.equal(conn.isOpen, false);
  db.close();
});

// ---------------------------------------------------------------------------
// Database.version
// ---------------------------------------------------------------------------

test('test_database_version', () => {
  const dbDir = makeTmpDir('version_');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const ver = db.version;
  assert.ok(typeof ver === 'string');
  assert.ok(ver.length > 0);
  // version should look like semver (e.g. "0.1.3")
  assert.ok(/^\d+\.\d+/.test(ver), `Version "${ver}" doesn't match expected format`);
  db.close();
});

// ---------------------------------------------------------------------------
// Error handling — invalid Cypher
// ---------------------------------------------------------------------------

test('test_invalid_cypher_throws', () => {
  const { db, conn } = setupTestDb();
  assert.throws(
    () => conn.execute('THIS IS NOT VALID CYPHER;'),
    (err) => err instanceof Error && err.message.length > 0
  );
  conn.close();
  db.close();
});

test('test_query_nonexistent_table', () => {
  const { db, conn } = setupTestDb();
  assert.throws(
    () => conn.execute('MATCH (x:nonexistent_table) RETURN x;'),
    (err) => err instanceof Error
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DDL result — should return empty QueryResult with statusCode 0
// ---------------------------------------------------------------------------

test('test_ddl_result', () => {
  const dbDir = makeTmpDir('ddl_result_');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  const result = conn.execute('CREATE NODE TABLE t1(id INT64, PRIMARY KEY(id));');
  assert.equal(result.statusCode(), 0);
  assert.equal(result.length(), 0);
  conn.close();
  db.close();
});
