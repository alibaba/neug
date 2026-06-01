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

const { test, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { Database, Session } = require('../lib');
const {
  ERR_COMPILATION,
  ERR_INVALID_ARGUMENT,
  ERR_QUERY_SYNTAX,
  ERR_SCHEMA_MISMATCH,
  ERR_TYPE_CONVERSION,
  ERR_TYPE_OVERFLOW,
} = require('../lib/error-codes');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let _tmpCounter = 0;
const _tmpDirs = [];
function makeTmpDir(prefix = 'neug_test_') {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix + _tmpCounter++ + '_'));
  _tmpDirs.push(dir);
  return dir;
}

// Clean up all temporary directories after all tests finish
after(() => {
  for (const dir of _tmpDirs) {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch (_) {
      // ignore cleanup errors
    }
  }
});

// Python conftest helpers (inlined):
function ensureResultCntGtZero(result) {
  const rows = [...result];
  assert.ok(rows.length > 0, 'Expected result count > 0');
}
function ensureResultCntEq(result, n) {
  const rows = [...result];
  assert.equal(rows.length, n);
}

const SKIP_SERVER = 'Requires running server (Session test)';
const SKIP_BUG = 'Skipped in Python (known bug)';

const SESSION_PORT = 10010;

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ===========================================================================
// DB-003-01  test_create_schema_basic_types
// ===========================================================================

test('test_create_schema_basic_types', () => {
  const dbDir = makeTmpDir('schema_basic');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE PERSON(int32_prop INT32, uint32_prop UINT32, ' +
    'int64_prop INT64, uint64_prop UINT64, string_prop STRING, ' +
    'bool_prop BOOL, float_prop FLOAT, double_prop DOUBLE, ' +
    'PRIMARY KEY(int32_prop));'
  );
  conn.execute(
    "CREATE (n:PERSON {int32_prop: 1, uint32_prop: 2, " +
    "int64_prop: 3, uint64_prop: 4, string_prop: 'test', " +
    "bool_prop: true, float_prop: 1.23, double_prop: 2.34});"
  );
  const result = conn.execute(
    'MATCH (n:PERSON) RETURN n.int32_prop, n.uint32_prop, ' +
    'n.int64_prop, n.uint64_prop, n.string_prop, ' +
    'n.bool_prop, n.float_prop, n.double_prop;'
  );
  const record = [...result][0];
  assert.equal(record[0], 1);
  assert.equal(record[1], 2);
  assert.equal(record[2], 3);
  assert.equal(record[3], 4);
  assert.equal(record[4], 'test');
  assert.equal(record[5], true);
  assert.ok(Math.abs(record[6] - 1.23) < 1e-6);
  assert.ok(Math.abs(record[7] - 2.34) < 1e-6);
  conn.close();
  db.close();
});

test('test_session_create_schema_basic_types', async () => {
  const dbDir = makeTmpDir('session_schema');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const endpoint = db.serve({ host: '127.0.0.1', port: SESSION_PORT, blocking: false });
  const sess = await Session.open({ endpoint, timeout: '30s' });
  try {
    await sess.execute(
      'CREATE NODE TABLE PERSON(int32_prop INT32, uint32_prop UINT32, ' +
      'int64_prop INT64, uint64_prop UINT64, string_prop STRING, ' +
      'bool_prop BOOL, float_prop FLOAT, double_prop DOUBLE, ' +
      'PRIMARY KEY(int32_prop));'
    );

    await sess.execute(
      "CREATE (n:PERSON {int32_prop: 1, uint32_prop: 2, " +
      "int64_prop: 3, uint64_prop: 4, string_prop: 'test', " +
      "bool_prop: true, float_prop: 1.23, double_prop: 2.34});"
    );

    const result = await sess.execute(
      'MATCH (n:PERSON) RETURN n.int32_prop, n.uint32_prop, ' +
      'n.int64_prop, n.uint64_prop, n.string_prop, ' +
      'n.bool_prop, n.float_prop, n.double_prop;'
    );
    const record = [...result][0];
    assert.equal(record[0], 1);
    assert.equal(record[1], 2);
    assert.equal(record[2], 3);
    assert.equal(record[3], 4);
    assert.equal(record[4], 'test');
    assert.equal(record[5], true);
    assert.ok(Math.abs(record[6] - 1.23) < 1e-6);
    assert.ok(Math.abs(record[7] - 2.34) < 1e-6);
  } finally {
    sess.close();
    db.stopServing();
    db.close();
  }
});

test('test_create_schema_float_types', () => {
  const dbDir = makeTmpDir('schema_float');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE PERSON(int32_prop INT32, float_prop FLOAT, PRIMARY KEY(int32_prop));'
  );
  conn.execute('CREATE (n:PERSON {int32_prop: 1, float_prop: 2.3});');
  const result = conn.execute('MATCH (n:PERSON) RETURN *;');
  assert.ok(result !== null && result.length() === 1);
  conn.execute(
    'MATCH (n:PERSON) WHERE n.float_prop > 4.5 RETURN n.float_prop;'
  );
  conn.close();
  db.close();
});

test('test_create_schema_complex_types', () => {
  const dbDir = makeTmpDir('schema_complex');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE Type (p1 INT32, p8 Date, p9 Timestamp, p10 Interval, PRIMARY KEY (p1));'
  );
  conn.close();
  db.close();
});

// DB-003-02

test('test_insert_basic_type_check', () => {
  const dbDir = makeTmpDir('insert_basic');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT32, i64 INT64, u32 UINT32, u64 UINT64, ' +
    'f FLOAT, d DOUBLE, s STRING, PRIMARY KEY (id));'
  );
  conn.execute(
    "CREATE (t:person {id: 1, i64: 1234567890123, u32: 123, u64: 456, f: 1.23, d: 4.56, s: 'abc'});"
  );
  assert.throws(() => conn.execute("CREATE (t:person {id: 'abc'})"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  assert.throws(() => conn.execute("CREATE (t:person {id: 2, i64: 'bad'})"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  assert.throws(() => conn.execute('CREATE (t:person {id: 3, u32: -1})'),
    new RegExp(String(ERR_TYPE_OVERFLOW)));
  assert.throws(() => conn.execute("CREATE (t:person {id: 4, f: 'bad'})"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  conn.close();
  db.close();
});

test('test_insert_type_check', () => {
  const dbDir = makeTmpDir('insert_type');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE T (' +
    'id INT32, i64 INT64, u32 UINT32, u64 UINT64, ' +
    'f FLOAT, d DOUBLE, s STRING, dt Date, tms Timestamp, ivl Interval, ' +
    'PRIMARY KEY(id));'
  );
  conn.execute(
    "CREATE (t:T {id: 1, i64: 1234567890123, u32: 123, u64: 456, f: 1.23, d: 4.56, " +
    "s: 'abc', dt: date('2023-01-01'), tms: Timestamp('2023-01-01 12:00:00'), " +
    "ivl: interval('1 year')});"
  );
  assert.throws(() => conn.execute("CREATE (t:T {id: 'abc'})"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  assert.throws(() => conn.execute("CREATE (t:T {id: 1, i64: 'bad'})"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  assert.throws(() => conn.execute('CREATE (t:T {id: 2, u32: -1})'),
    new RegExp(String(ERR_TYPE_OVERFLOW)));
  assert.throws(() => conn.execute("CREATE (t:T {id: 3, f: 'bad'})"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  assert.throws(() => conn.execute("CREATE (t:T {id: 4, dt: 'notadate'})"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  assert.throws(() => conn.execute("CREATE (t:T {id: 5, dttm: 'notadatetime'})"),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  assert.throws(() => conn.execute("CREATE (t:T {id: 6, ivl: 'notaninterval'})"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  conn.close();
  db.close();
});

// DB-003-03

test('test_return_expression', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      "Match (n) RETURN 1+2, date('2023-01-01'), interval('1 year 2 days') limit 1;"
    );
    assert.ok(result !== null);
    assert.equal(result.length(), 1);
    const row = [...result][0];
    assert.equal(row[0], 3);
    assert.equal(String(row[1]), '2023-01-01');
    assert.equal(row[2], '1 year 2 days');
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-04

test('test_create_node_table', () => {
  const dbDir = makeTmpDir('create_node');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY (name));');
  conn.close();
  db.close();
});

test('test_create_node_table_with_default_value', () => {
  const dbDir = makeTmpDir('create_node_def');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person (name STRING, age INT64 DEFAULT 0, PRIMARY KEY (name));'
  );
  conn.close();
  db.close();
});

test('test_create_node_table_errors', () => {
  const dbDir = makeTmpDir('create_node_err');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY (name));');
  assert.throws(
    () => conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY (name));'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  assert.throws(
    () => conn.execute('CREATE NODE TABLE person1(name STRING, age INT64);'),
    new RegExp(String(ERR_QUERY_SYNTAX)));
  assert.throws(
    () => conn.execute("CREATE NODE TABLE person2(name STRING, age INT64 DEFAULT 'abc', PRIMARY KEY (name));"),
    new RegExp(String(ERR_TYPE_CONVERSION)));
  conn.close();
  db.close();
});

// DB-003-05

test('test_create_rel_table', () => {
  const dbDir = makeTmpDir('create_rel');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);');
  conn.close();
  db.close();
});

test('test_create_rel_table_with_multiple_src_dst', () => {
  const dbDir = makeTmpDir('rel_multi_src');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE NODE TABLE comment(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE NODE TABLE post(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE likes(FROM person TO comment, FROM person TO post);');
  conn.close();
  db.close();
});

test('test_create_rel_table_with_multiple_relationships', () => {
  const dbDir = makeTmpDir('rel_multi_rel');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE NODE TABLE city(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE worksAt(FROM person TO city, FROM person TO person);');
  conn.close();
  db.close();
});

test('test_create_rel_table_errors', () => {
  const dbDir = makeTmpDir('rel_errors');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);');
  assert.throws(
    () => conn.execute('CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  assert.throws(
    () => conn.execute('CREATE REL TABLE NewFollows(FROM person TO user, MANY_TO_MANY);'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.close();
  db.close();
});

test('test_create_duplicated_rel_table_between_same_vertex_tables', () => {
  const dbDir = makeTmpDir('dup_rel');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);');
  conn.execute('CREATE REL TABLE knows(FROM person TO person, MANY_TO_MANY);');
  conn.close();
  db.close();
});

// DB-003-06

test('test_alter_vertex_table', () => {
  const dbDir = makeTmpDir('alter_vtx');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));');
  conn.execute('ALTER TABLE person ADD grade INT64;');
  assert.throws(() => conn.execute('ALTER TABLE person ADD age INT64;'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.execute('ALTER TABLE person RENAME age TO newAge;');
  assert.throws(() => conn.execute('ALTER TABLE person RENAME age1 TO newAge1;'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.execute('ALTER TABLE person DROP newAge;');
  assert.throws(() => conn.execute('ALTER TABLE person DROP age1;'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.close();
  db.close();
});

test('test_session_alter_vertex_table', async () => {
  const dbDir = makeTmpDir('session_alter');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const endpoint = db.serve({ host: '127.0.0.1', port: SESSION_PORT, blocking: false });
  const sess = await Session.open({ endpoint, timeout: '30s' });
  try {
    await sess.execute('CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));');
    await sess.execute('ALTER TABLE person ADD grade INT64;');
    await assert.rejects(
      () => sess.execute('ALTER TABLE person ADD age INT64;'),
      new RegExp(String(ERR_SCHEMA_MISMATCH)));
    await sess.execute('ALTER TABLE person RENAME age TO newAge;');
    await assert.rejects(
      () => sess.execute('ALTER TABLE person RENAME age1 TO newAge1;'),
      new RegExp(String(ERR_SCHEMA_MISMATCH)));
    await sess.execute('ALTER TABLE person DROP newAge;');
    await assert.rejects(
      () => sess.execute('ALTER TABLE person DROP age1;'),
      new RegExp(String(ERR_INVALID_ARGUMENT)));
  } finally {
    sess.close();
    db.stopServing();
    db.close();
  }
});

test('test_alter_edge_table', () => {
  const dbDir = makeTmpDir('alter_edge');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);');
  conn.execute('ALTER TABLE knows ADD since INT64;');
  assert.throws(() => conn.execute('ALTER TABLE knows ADD weight DOUBLE;'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.execute('ALTER TABLE knows RENAME weight TO newWeight;');
  assert.throws(() => conn.execute('ALTER TABLE knows RENAME weight1 TO newWeight1;'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.close();
  db.close();
});

test('test_alter_edge_table_drop_property', () => {
  const dbDir = makeTmpDir('alter_edge_drop');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);');
  conn.execute('ALTER TABLE knows DROP weight;');
  assert.throws(() => conn.execute('ALTER TABLE knows DROP weight1;'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.close();
  db.close();
});

// DB-003-07

test('test_drop_table', () => {
  const dbDir = makeTmpDir('drop_table');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);');
  conn.execute('DROP TABLE knows;');
  conn.execute('DROP TABLE person;');
  conn.close();
  db.close();
});

test('test_drop_table_errors', () => {
  const dbDir = makeTmpDir('drop_table_err');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);');
  conn.execute('DROP TABLE person;');
  assert.throws(() => conn.execute('DROP TABLE knows;'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  assert.throws(() => conn.execute('DROP TABLE person;'),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.close();
  db.close();
});

// DB-003-08

test('test_insert_node', () => {
  const dbDir = makeTmpDir('insert_node');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));');
  conn.execute("CREATE (u:person{name:'Alice',age:35});");
  assert.throws(() => conn.execute('CREATE (u:person{age:36});'),
    new RegExp(String(ERR_COMPILATION)));
  assert.throws(() => conn.execute("CREATE (u:person{name:'Alice', age:26});"),
    new RegExp(String(ERR_INVALID_ARGUMENT)));
  assert.throws(() => conn.execute("CREATE (u:person{name:'Alice', age:26, addr:'aa'});"),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.close();
  db.close();
});

// DB-003-09

test('test_insert_edge', () => {
  const dbDir = makeTmpDir('insert_edge');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_TO_MANY);');
  conn.execute("CREATE (u:person{name:'Alice'});");
  conn.execute("CREATE (u:person{name:'Josh'});");
  conn.execute(
    "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' " +
    "AND u2.name = 'Josh' CREATE (u1)-[:follows {since: 2011}]->(u2);");
  conn.execute(
    "MATCH (a:person), (b:person) WHERE a.name = 'Alice' CREATE (a)-[:follows {since:2022}]->(b);");
  conn.execute(
    "MATCH (a:person), (b:person) WHERE a.name = 'nobody' CREATE (a)-[:follows {since:2022}]->(b);");
  conn.execute(
    "CREATE (u:person {name: 'Alice1'})-[:follows {since:2022}]->(b:person {name: 'Josh1'});");
  assert.throws(
    () => conn.execute(
      "CREATE (u:person {name: 'Alice'})-[:follows {since:2022}]->(b:person {name: 'Josh2'});"),
    new RegExp(String(ERR_INVALID_ARGUMENT)));
  assert.throws(
    () => conn.execute(
      "CREATE (u:person {name: 'Alice2'})-[:follows {nonprop:2022}]->(b:person {name: 'Josh2'});"),
    new RegExp(String(ERR_SCHEMA_MISMATCH)));
  conn.close();
  db.close();
});

test('test_create_edge_return_edge_property', () => {
  const dbDir = makeTmpDir('edge_return');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE knows(FROM person TO person, since INT64, MANY_TO_MANY);');
  conn.execute('CREATE (a:person {id: 1});');
  conn.execute('CREATE (b:person {id: 2});');
  const result = conn.execute(
    'MATCH (a:person {id: 1}), (b:person {id: 2}) ' +
    'CREATE (a)-[e:knows {since: 2024}]->(b) RETURN e.since;');
  const records = [...result];
  assert.deepEqual(records, [[2024]]);
  conn.close();
  db.close();
});

// DB-003-10

test('test_set_node_property', () => {
  const dbDir = makeTmpDir('set_node');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));');
  conn.execute("CREATE (u:person{name:'Alice',age:35});");
  let result = conn.execute("MATCH (u:person) WHERE u.name = 'Alice' RETURN u.age;");
  assert.equal([...result][0][0], 35);
  result = conn.execute(
    "MATCH (u:person) WHERE u.name = 'Alice' SET u.age = 50 RETURN u.age;");
  assert.equal([...result][0][0], 50);
  conn.execute('ALTER TABLE person ADD population INT64;');
  result = conn.execute('MATCH (u) SET u.population = 0 RETURN u.population;');
  assert.equal([...result][0][0], 0);
  result = conn.execute(
    "MATCH (u:person) WHERE u.name = 'nobody' SET u.age = 50 RETURN u.name;");
  assert.equal(result.length(), 0);
  assert.throws(
    () => conn.execute(
      "MATCH (u:person) WHERE u.name = 'Alice' SET u.addr = '' RETURN u.*;"),
    /Cannot find property/);
  conn.close();
  db.close();
});

test('test_set_multi_edge_property', () => {
  const dbDir = makeTmpDir('set_multi_edge');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE NODE TABLE software(name STRING, PRIMARY KEY(name));');
  conn.execute(
    'CREATE REL TABLE create_software(FROM person TO software, since INT64, weight DOUBLE, MANY_TO_MANY);');
  conn.execute("CREATE (u:person{name:'Alice'});");
  conn.execute("CREATE (u:person{name:'Bob'});");
  conn.execute("CREATE (s:software{name:'Neug'});");
  conn.execute(
    "MATCH (u1:person), (s1:software) WHERE u1.name = 'Alice' AND s1.name = 'Neug' " +
    "CREATE (u1)-[:create_software {since: 2022, weight: 1.0}]->(s1);");
  conn.execute(
    "MATCH (u1:person), (s1:software) WHERE u1.name = 'Bob' AND s1.name = 'Neug' " +
    "CREATE (u1)-[:create_software {since: 2023, weight: 2.0}]->(s1);");
  let result = conn.execute(
    'MATCH (u0:person)-[c:create_software]->(s1:software) return c.since, c.weight;');
  let rows = [...result];
  assert.deepEqual(rows[0], [2022, 1.0]);
  assert.deepEqual(rows[1], [2023, 2.0]);
  result = conn.execute(
    "MATCH (u0:person)-[c:create_software]->(s1:software) " +
    "WHERE u0.name = 'Alice' AND s1.name = 'Neug' " +
    "SET c.since = 1999, c.weight = 3.0 RETURN c.since;");
  assert.deepEqual([...result][0], [1999]);
  result = conn.execute(
    "MATCH (u0:person)-[c:create_software]->(s1:software) WHERE u0.name = 'Alice' " +
    "AND s1.name = 'Neug' RETURN c.since, c.weight;");
  assert.deepEqual([...result][0], [1999, 3.0]);
  conn.close();
  db.close();
});

// DB-003-11

test('test_set_edge_property', () => {
  const dbDir = makeTmpDir('set_edge');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));');
  conn.execute('CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_TO_MANY);');
  conn.execute('CREATE REL TABLE likes(FROM person TO person, since INT64);');
  conn.execute("CREATE (u:person{name:'Alice'});");
  conn.execute("CREATE (u:person{name:'Josh'});");
  conn.execute("CREATE (u:person{name:'Bob'});");
  conn.execute(
    "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' AND" +
    " u2.name = 'Josh' CREATE (u1)-[:follows {since: 2012}]->(u2);");
  conn.execute(
    "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' AND" +
    " u2.name = 'Bob' CREATE (u1)-[:follows {since: 2009}]->(u2);");
  let result = conn.execute(
    "MATCH (u0:person)-[f:follows]->(u1:person)" +
    " WHERE u0.name = 'Alice' AND u1.name = 'Josh' SET f.since = 1999 RETURN f.since;");
  assert.equal([...result][0][0], 1999);
  result = conn.execute(
    "MATCH (u0)-[f]->() WHERE u0.name = 'Alice' SET f.since = 1999 RETURN f.since;");
  const rows = [...result];
  assert.equal(rows[0][0], 1999);
  assert.equal(rows[1][0], 1999);
  assert.throws(
    () => conn.execute(
      "MATCH (u0)-[f]->() WHERE u0.name = 'Alice' SET f.noprop = 1999 RETURN f.noprop;"),
    /Cannot find property noprop/);
  conn.close();
  db.close();
});

// DB-003-12

test('test_query_sync', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute('MATCH (n) RETURN n;');
    assert.equal(result.length(), 6);
  } finally {
    conn.close();
    db.close();
  }
});

test('test_query_async', async () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = await db.asyncConnect();
  try {
    const result = await conn.execute('MATCH (n) RETURN n;');
    assert.equal(result.length(), 6);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-20

test('test_query_syntax_error', () => {
  const dbDir = makeTmpDir('syntax_err');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  assert.throws(() => conn.execute('MATCH (n RETURN n;'),
    new RegExp(String(ERR_QUERY_SYNTAX)));
  conn.close();
  db.close();
});

// DB-003-22

test('test_insert_vertex_edge', () => {
  const dbDir = makeTmpDir('insert_ve');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);');
  conn.execute("CREATE (p:person {id: 1, name: 'Alice'});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob'});");
  conn.execute("CREATE (p:person {id: 3, name: 'Charlie'});");
  conn.execute(
    "MATCH (a:person), (b:person) WHERE a.name = 'Alice' AND b.name = 'Bob' " +
    "CREATE (a)-[:knows {weight: 1.0}]->(b);");
  conn.execute(
    "MATCH (a:person), (b:person) WHERE a.name = 'Alice' AND b.name = 'Charlie' " +
    "CREATE (a)-[:knows {weight: 2.0}]->(b);");
  let result = conn.execute('MATCH (n) RETURN n;');
  assert.equal(result.length(), 3);
  result = conn.execute('MATCH (a:person)-[r:knows]->(b:person) RETURN a, r, b;');
  assert.equal(result.length(), 2);
  result = conn.execute(
    "MATCH (a:person)-[b:knows]->(c:person) WHERE c.name = 'Charlie' RETURN b.weight;");
  assert.equal(result.length(), 1);
  assert.equal([...result][0][0], 2.0);
  conn.close();
  db.close();
});

// DB-003-23
test('test_complex_example', async () => {
  const dbDir = makeTmpDir('complex_example');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  conn.execute(`
    CREATE NODE TABLE Person(
        id INT64 PRIMARY KEY,
        name STRING,
        age INT32,
        email STRING
    )
  `);

  conn.execute(`
    CREATE NODE TABLE Company(
        id INT64 PRIMARY KEY,
        name STRING,
        industry STRING,
        founded_year INT32
    )
  `);

  conn.execute(`
    CREATE REL TABLE WORKS_FOR(
        FROM Person TO Company,
        position STRING,
        start_date DATE,
        salary DOUBLE
    )
  `);

  conn.execute(`
    CREATE REL TABLE KNOWS(
        FROM Person TO Person,
        since_year INT32,
        relationship_type STRING
    )
  `);

  conn.execute("CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})");
  conn.execute("CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})");
  conn.execute("CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})");

  conn.execute("MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1 CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)");
  conn.execute("MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)");

  conn.close();

  const endpoint = db.serve({ host: '127.0.0.1', port: SESSION_PORT, blocking: false });
  const session = await Session.open({ endpoint, timeout: '30s' });

  await session.execute('CREATE NODE TABLE User(id INT64 PRIMARY KEY, username STRING, created_at TIMESTAMP)');
  await session.execute("CREATE (u:User {id: 1, username: 'user1', created_at: timestamp('2024-01-01 10:00:00')})");
  const result = await session.execute('MATCH (u:User) RETURN u.username, u.created_at');
  const records = [...result];
  assert.equal(records.length, 1);
  assert.equal(records[0][0], 'user1');

  session.close();
  db.stopServing();
  db.close();
});

// DB-003-24

test('test_query_on_empty_graph', () => {
  const db = new Database();
  const conn = db.connect();
  const res = conn.execute('MATCH (n) RETURN n;');
  assert.ok(res !== null);
  assert.equal(res.length(), 0);
  conn.close();
  db.close();
});

test.skip('test_join_queries', SKIP_BUG);

// DB-003-26

test('test_path_expand', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute('MATCH (p:person)-[k*1..2]->(f) RETURN k;');
    assert.ok(result !== null);
    const expected = [
      [{ nodes: [{ _ID: 0, _LABEL: 'person', id: 1, name: 'marko', age: 29 }, { _ID: 1, _LABEL: 'person', id: 2, name: 'vadas', age: 27 }], rels: [{ _ID: 1, _LABEL: 'knows', _SRC_ID: 0, _DST_ID: 1, weight: 0.5 }], length: 1 }],
      [{ nodes: [{ _ID: 0, _LABEL: 'person', id: 1, name: 'marko', age: 29 }, { _ID: 2, _LABEL: 'person', id: 4, name: 'josh', age: 32 }], rels: [{ _ID: 2, _LABEL: 'knows', _SRC_ID: 0, _DST_ID: 2, weight: 1.0 }], length: 1 }],
      [{ nodes: [{ _ID: 0, _LABEL: 'person', id: 1, name: 'marko', age: 29 }, { _ID: 72057594037927936, _LABEL: 'software', id: 3, name: 'lop', lang: 'java' }], rels: [{ _ID: 1103806595072, _LABEL: 'created', _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4 }], length: 1 }],
      [{ nodes: [{ _ID: 2, _LABEL: 'person', id: 4, name: 'josh', age: 32 }, { _ID: 72057594037927936, _LABEL: 'software', id: 3, name: 'lop', lang: 'java' }], rels: [{ _ID: 1103808692224, _LABEL: 'created', _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4 }], length: 1 }],
      [{ nodes: [{ _ID: 2, _LABEL: 'person', id: 4, name: 'josh', age: 32 }, { _ID: 72057594037927937, _LABEL: 'software', id: 5, name: 'ripple', lang: 'java' }], rels: [{ _ID: 1103808692225, _LABEL: 'created', _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0 }], length: 1 }],
      [{ nodes: [{ _ID: 3, _LABEL: 'person', id: 6, name: 'peter', age: 35 }, { _ID: 72057594037927936, _LABEL: 'software', id: 3, name: 'lop', lang: 'java' }], rels: [{ _ID: 1103809740800, _LABEL: 'created', _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2 }], length: 1 }],
      [{ nodes: [{ _ID: 0, _LABEL: 'person', id: 1, name: 'marko', age: 29 }, { _ID: 2, _LABEL: 'person', id: 4, name: 'josh', age: 32 }, { _ID: 72057594037927936, _LABEL: 'software', id: 3, name: 'lop', lang: 'java' }], rels: [{ _ID: 2, _LABEL: 'knows', _SRC_ID: 0, _DST_ID: 2, weight: 1.0 }, { _ID: 1103808692224, _LABEL: 'created', _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4 }], length: 2 }],
      [{ nodes: [{ _ID: 0, _LABEL: 'person', id: 1, name: 'marko', age: 29 }, { _ID: 2, _LABEL: 'person', id: 4, name: 'josh', age: 32 }, { _ID: 72057594037927937, _LABEL: 'software', id: 5, name: 'ripple', lang: 'java' }], rels: [{ _ID: 2, _LABEL: 'knows', _SRC_ID: 0, _DST_ID: 2, weight: 1.0 }, { _ID: 1103808692225, _LABEL: 'created', _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0 }], length: 2 }],
    ];
    let i = 0;
    for (const record of result) {
      assert.deepEqual(record, expected[i], `Record ${i} does not match expected result`);
      i++;
    }
    const result2 = conn.execute(
      "MATCH (p:person {name: 'marko'})-[k:knows*1..2 (r, _ | WHERE r.weight <= 1.0)]->(f:person) Return k;");
    assert.ok(result2 !== null);
    const expected2 = [
      [{ nodes: [{ _ID: 0, _LABEL: 'person', id: 1, name: 'marko', age: 29 }, { _ID: 1, _LABEL: 'person', id: 2, name: 'vadas', age: 27 }], rels: [{ _ID: 1, _LABEL: 'knows', _SRC_ID: 0, _DST_ID: 1, weight: 0.5 }], length: 1 }],
      [{ nodes: [{ _ID: 0, _LABEL: 'person', id: 1, name: 'marko', age: 29 }, { _ID: 2, _LABEL: 'person', id: 4, name: 'josh', age: 32 }], rels: [{ _ID: 2, _LABEL: 'knows', _SRC_ID: 0, _DST_ID: 2, weight: 1.0 }], length: 1 }],
    ];
    let j = 0;
    for (const record of result2) {
      assert.deepEqual(record, expected2[j], `Record ${j} does not match expected result`);
      j++;
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-28

test('test_query_cyclic', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  try {
    const res = conn.execute(
      "Match (a:person)-[:created]->(b:software), (c:person)-[:created]->(b:software)," +
      "(a:person)-[:knows]->(c:person) Where a.name <> b.name AND b.name <> c.name" +
      " Return count(*);");
    assert.equal([...res][0][0], 1);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-29

test('test_no_existing_property', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const res = conn.execute(
      'MATCH (a:person)-[e1:knows|:studyAt|:workAt]->(b:person:organisation) WHERE a.age > 35 RETURN b.fName, b.name;'
    );
    for (const record of res) {
      // iterate to verify query works without errors
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-30

test('test_result_getitem', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const res = conn.execute('MATCH (n) RETURN count(n);');
    assert.ok(res !== null);
    assert.equal(res.length(), 1);
    const rows = [...res];
    assert.equal(rows[0][0], 6);
    assert.equal(rows[rows.length - 1][0], 6);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-31

test('test_return_literal', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const res = conn.execute('MATCH (a:person) RETURN 1 + 1, label(a) LIMIT 2');
    assert.ok(res !== null);
    assert.equal(res.length(), 2);
    const rows = [...res];
    assert.deepEqual(rows[0], [2, 'person']);
    assert.deepEqual(rows[1], [2, 'person']);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-32

test('test_count', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    let res = conn.execute('MATCH (n) RETURN count(n)');
    assert.ok(res !== null);
    assert.equal(res.length(), 1);
    assert.equal([...res][0][0], 14);

    res = conn.execute('MATCH ()-[e]->() RETURN count(e)');
    assert.ok(res !== null);
    assert.equal(res.length(), 1);
    assert.equal([...res][0][0], 30);

    res = conn.execute('MATCH ()-[e]-() RETURN count(e)');
    assert.ok(res !== null);
    assert.equal(res.length(), 1);
    assert.equal([...res][0][0], 60);

    res = conn.execute('MATCH ()-[e]-()-[]-()-[]-() RETURN COUNT(*)');
    assert.ok(res !== null);
    assert.equal(res.length(), 1);
    assert.equal([...res][0][0], 4120);

    res = conn.execute('MATCH (a)-[]->(b) return count(*)');
    assert.ok(res !== null);
    assert.equal(res.length(), 1);
    assert.equal([...res][0][0], 30);

    res = conn.execute('MATCH (a)<-[]-(b)-[]->() return count(*)');
    assert.ok(res !== null);
    assert.equal(res.length(), 1);
    assert.equal([...res][0][0], 144);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-33

test('test_list_return_basic', () => {
  const dbDir = makeTmpDir('list_ret');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person (id INT32 PRIMARY KEY, name STRING, value FLOAT);');
  conn.execute("CREATE (p:Person {id: 1, name: 'Alice', value: 1.11});");
  conn.execute("CREATE (p:Person {id: 2, name: 'Bob', value: 2.22});");
  conn.execute("CREATE (p:Person {id: 3, name: 'Charlie', value: 3.33});");
  const result = conn.execute('MATCH (p:Person) RETURN [p.name, p.value] ORDER BY p.id;');
  const records = [...result];
  assert.equal(records.length, 3);
  assert.equal(records[0][0][0], 'Alice');
  assert.equal(records[1][0][0], 'Bob');
  assert.equal(records[2][0][0], 'Charlie');
  assert.ok(Math.abs(records[0][0][1] - 1.11) < 1e-5);
  assert.ok(Math.abs(records[1][0][1] - 2.22) < 1e-5);
  assert.ok(Math.abs(records[2][0][1] - 3.33) < 1e-5);
  conn.close();
  db.close();
});

// DB-003-34

test.skip('test_optional_match', SKIP_BUG);

// DB-003-35

test('test_create_edge_with_prop_on_both_end', () => {
  const dbDir = makeTmpDir('edge_both');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person, id INT64);');
  conn.execute('CREATE (p: Person {id: 111});');
  conn.execute('CREATE (p: Person {id: 222});');
  conn.execute(
    'MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 333}]->(p2);');
  conn.execute(
    'MATCH (p1: Person {id: 111})-[k: Knows]-(p2:Person {id: 222}) RETURN k.id');
  conn.close();
  db.close();
});

// DB-003-36

test('test_copy_from', () => {
  const dbDir = makeTmpDir('copy_from');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  const file = path.join(dbDir, 'test_data.csv');
  fs.writeFileSync(file,
    '"id","entity","entity_type"\n' +
    '1,"-1-10000","属性"\n' +
    '2,"-180°-180°","场景"\n' +
    '3,"-180°-180°","属性"\n' +
    '4,"0","属性"\n' +
    '5,"0-1","场景"\n' +
    '6,"0-1","属性"\n' +
    '7,"0-10","属性"\n' +
    '8,"0-100","属性"\n' +
    '9,"0-1000","属性"\n' +
    '10,"0-1000000","属性"\n');
  conn.execute(
    'CREATE NODE TABLE Entity(id STRING, entity STRING, entity_type STRING, PRIMARY KEY(id))');
  conn.execute(`COPY Entity FROM '${file}' (HEADER TRUE, DELIMITER=',')`);
  conn.close();
  db.close();
});

// DB-003-37

test('test_tinysnb_path_expand', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute('MATCH (n:Person)-[:Meets*1..2]->(m:Person) return count(*);');
    const records = [...result];
    assert.equal(records.length, 1);
    assert.equal(records[0][0], 13);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-38

test('test_path_expand_count_on_typed_rel_table', () => {
  const dbDir = makeTmpDir('path_typed');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  const setup = [
    ['CREATE NODE TABLE A(id STRING, p INT32, PRIMARY KEY(id));', 'schema'],
    ['CREATE NODE TABLE B(id STRING, q INT32, PRIMARY KEY(id));', 'schema'],
    ['CREATE REL TABLE R(FROM A TO B, w INT32);', 'schema'],
    ["CREATE (a:A {id:'a1', p:1});", 'update'],
    ["CREATE (a:A {id:'a2', p:2});", 'update'],
    ["CREATE (b:B {id:'b1', q:1});", 'update'],
    ["MATCH (a:A {id:'a1'}), (b:B {id:'b1'}) CREATE (a)-[:R {w:1}]->(b);", 'update'],
    ["MATCH (a:A {id:'a2'}), (b:B {id:'b1'}) CREATE (a)-[:R {w:2}]->(b);", 'update'],
  ];
  for (const [q, mode] of setup) conn.execute(q, mode);
  const result = conn.execute('MATCH (a:A)-[:R*1..2]->(b:B) RETURN count(*) AS c', 'read');
  const records = [...result];
  assert.equal(records.length, 1);
  assert.equal(records[0][0], 2);
  conn.close();
  db.close();
});

// DB-003-39

test('test_path_expand_with_filter', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    let result = conn.execute(
      'MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE (a.ID = 0) RETURN a.ID, b.ID');
    assert.deepEqual([...result], [[0, 1], [0, 5], [0, 1], [0, 5]]);
    result = conn.execute(
      'MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE ((b.ID < 5)) AND (a.ID = 0) RETURN a.ID, b.ID');
    assert.deepEqual([...result], [[0, 1], [0, 1]]);
    result = conn.execute(
      'MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE (b.ID < 5) RETURN a.ID, b.ID');
    assert.deepEqual([...result], [[3, 3], [7, 3], [0, 1], [10, 1], [0, 1], [7, 1]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-40

test('test_edge_expand_with_filter', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    let result = conn.execute(
      'MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE (a.ID = 0) RETURN a.ID, b.ID,label(e)');
    let rows = [...result];
    const sorted = rows.sort((a, b) => a[1] - b[1] || a[2].localeCompare(b[2]));
    assert.deepEqual(sorted, [[0, 1, 'studyAt'], [0, 2, 'marries'], [0, 2, 'meets']]);
    result = conn.execute(
      'MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE ((b.ID > 1)) AND (a.ID = 0) RETURN a.ID, b.ID,label(e);');
    assert.deepEqual([...result], [[0, 2, 'meets'], [0, 2, 'marries']]);
    result = conn.execute(
      'MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE (b.ID > 5) RETURN a.ID, b.ID');
    assert.deepEqual([...result], [[3, 7], [7, 8]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-41

test('test_upper', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute('MATCH (n:person) RETURN UPPER(n.name)');
    const expected = new Set(['MARKO', 'VADAS', 'JOSH', 'PETER']);
    const actual = new Set([...result].map(r => r[0]));
    assert.deepEqual(actual, expected);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-42

test('test_lower', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      "RETURN LOWER('MARKO'), LOWER('VaDaS'), LOWER('Josh'), LOWER('PETER')");
    const row = [...result][0];
    assert.deepEqual(row, ['marko', 'vadas', 'josh', 'peter']);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-43

test('test_reverse', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute('MATCH (n:person) RETURN n.name, REVERSE(n.name)');
    const expectedMap = { marko: 'okram', vadas: 'sadav', josh: 'hsoj', peter: 'retep' };
    for (const record of result) {
      const [original, reversed] = record;
      assert.equal(reversed, expectedMap[original]);
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-44

test('test_distinct', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'MATCH (a:person)-[:knows]->(c:person) Return distinct a.fName;');
    const records = [...result];
    assert.deepEqual(records, [['Alice'], ['Bob'], ['Carol'], ['Dan'], ['Elizabeth']]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-45

test('test_length', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    let result = conn.execute(
      'Match (n:PERSON {id: 933})-[k:KNOWS*1..3]->(m) Return LENGTH(k) as len Order by len Limit 1');
    for (const record of result) {
      assert.equal(record[0], 1);
    }
    result = conn.execute(
      'MATCH (:TAGCLASS {name: "OfficeHolder"})<-[:HASTYPE]-(:TAG)<-[:HASTAG]-(message)-[:REPLYOF*0..30]->(p:POST)' +
      ' RETURN count(p) AS numPosts');
    for (const record of result) {
      assert.equal(record[0], 19519);
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-46

test('test_nodes_rels', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'Match (n:PERSON {id: 933})-[k:KNOWS*1..3]-(m:PERSON {id: 2199023256668})' +
      ' Return nodes(k) as n1, rels(k) as n2 LIMIT 1;');
    ensureResultCntGtZero(result);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-47

test('test_case_expression', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'Match (n:PERSON {id: 933}) Return CASE WHEN n.id > 0 THEN n.id ELSE 0 END');
    for (const record of result) {
      assert.equal(record[0], 933);
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-48

test('test_to_tuple', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'Match (n:PERSON {id: 933})' +
      ' Return [n.firstName, n.gender, n.birthday] as n2 LIMIT 1;');
    ensureResultCntGtZero(result);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-49

test('test_dummy_scan', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute('Return 1002');
    for (const record of result) {
      assert.equal(record[0], 1002);
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-50

test('test_nested_tuple', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute("Match (n {name: 'marko'}) Return [[n.name, n.age], n.id]");
    for (const record of result) {
      assert.deepEqual(record[0], [['marko', 29], 1]);
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-51

test('test_null_value_tuple', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute("Match (n {name: 'lop'}) Return [n.name, n.age]");
    for (const record of result) {
      assert.deepEqual(record[0], ['lop', null]);
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-52

test('test_starts_with', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute("Match (n) Where n.name starts with 'mar' Return n.name");
    assert.equal(result.length(), 1);
    assert.equal([...result][0][0], 'marko');
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-53

test('test_ends_with', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute("Match (n) Where n.name ends with 'rko' Return n.name");
    assert.equal(result.length(), 1);
    assert.equal([...result][0][0], 'marko');
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-54

test('test_contains', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute("Match (n) Where n.name contains 'ark' Return n.name");
    assert.equal(result.length(), 1);
    assert.equal([...result][0][0], 'marko');
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-55

test('test_ends_with_and_contains_with_slash_in_string', () => {
  const dbDir = makeTmpDir('slash');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE path_node(path STRING, PRIMARY KEY(path));');
  conn.execute("CREATE (n:path_node {path: 'path/to/file'});");
  conn.execute("CREATE (n:path_node {path: 'a/b/c'});");
  conn.execute("CREATE (n:path_node {path: 'no_slash_here'});");
  conn.execute("CREATE (n:path_node {path: 'trailing/'});");
  let result = conn.execute(
    "MATCH (n:path_node) WHERE n.path ends with '/file' RETURN n.path ORDER BY n.path");
  let rows = [...result];
  assert.equal(rows.length, 1);
  assert.equal(rows[0][0], 'path/to/file');
  result = conn.execute(
    "MATCH (n:path_node) WHERE n.path ends with '/' RETURN n.path ORDER BY n.path");
  rows = [...result];
  assert.equal(rows.length, 1);
  assert.equal(rows[0][0], 'trailing/');
  result = conn.execute(
    "MATCH (n:path_node) WHERE n.path contains '/' RETURN n.path ORDER BY n.path");
  rows = [...result];
  assert.equal(rows.length, 3);
  assert.equal(rows[0][0], 'a/b/c');
  assert.equal(rows[1][0], 'path/to/file');
  assert.equal(rows[2][0], 'trailing/');
  result = conn.execute(
    "MATCH (n:path_node) WHERE n.path contains '/to/' RETURN n.path");
  rows = [...result];
  assert.equal(rows.length, 1);
  assert.equal(rows[0][0], 'path/to/file');
  conn.close();
  db.close();
});

// DB-003-56

test('test_date_time_to_string', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'MATCH (m:POST:COMMENT {id: 1030792332314})' +
      ' RETURN CASE WHEN m.content = "" THEN m.imageFile ELSE m.content END as messageContent,' +
      ' m.creationDate as messageCreationDate');
    const rows = [...result];
    assert.equal(rows[0][0], 'photo1030792332314.jpg');
    // creationDate comparison: just verify it's not null
    assert.ok(rows[0][1] != null);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-57

test('test_create_interval', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const res = conn.execute("RETURN INTERVAL('5 DAY')");
    for (const record of res) {
      assert.equal(record[0], '5 days');
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-58

test('test_intersect_predicate', () => {
  const dbDir = makeTmpDir('intersect');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE address(id INT32, name STRING, PRIMARY KEY(id))');
  conn.execute('CREATE REL TABLE structure(FROM address TO address, weight DOUBLE)');
  conn.execute('CREATE REL TABLE belong(FROM address TO address, weight DOUBLE)');
  conn.execute("CREATE (u: address {id: 1, name: 'address1' } )");
  conn.execute("CREATE (v: address {id: 2, name: 'address2' } )");
  conn.execute("CREATE (w: address {id: 3, name: 'address3' } )");
  conn.execute("CREATE (x: address {id: 4, name: 'address4' } )");
  conn.execute("CREATE (y: address {id: 5, name: 'address5' } )");
  conn.execute("CREATE (z: address {id: 6, name: 'address6' } )");
  conn.execute('MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:structure {weight: 1.0}]->(b)');
  conn.execute('MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:structure {weight: 2.2}]->(b)');
  conn.execute('MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 6 CREATE (a)-[:structure {weight: 2.3}]->(b)');
  conn.execute('MATCH (a: address), (b: address) WHERE a.id = 2 AND b.id = 4 CREATE (a)-[:structure {weight: 1.3}]->(b)');
  conn.execute('MATCH (a: address), (b: address) WHERE a.id = 2 AND b.id = 5 CREATE (a)-[:structure {weight: 1.4}]->(b)');
  conn.execute('MATCH (a: address), (b: address) WHERE a.id = 3 AND b.id = 6 CREATE (a)-[:structure {weight: 1.5}]->(b)');
  conn.execute('MATCH (a: address), (b: address) WHERE a.id = 3 AND b.id = 6 CREATE (a)-[:belong {weight: 2.0}]->(b)');
  conn.execute('MATCH (a: address), (b: address) WHERE a.id = 4 AND b.id = 5 CREATE (a)-[:belong {weight: 2.1}]->(b)');
  const res = conn.execute(
    'MATCH(n1: address)-[e1: structure]->(m1: address),' +
    '      (n1: address)-[e2: structure]->(m2: address),' +
    '      (m1)-[e3: belong]->(m2)' +
    ' WHERE n1.id = 1 AND e1.weight > 2.0 AND e2.weight > 2.0 AND e3.weight > 1.9' +
    ' RETURN e1.weight, e2.weight, e3.weight');
  assert.deepEqual([...res][0], [2.2, 2.3, 2.0]);
  conn.close();
  db.close();
});

// DB-003-59

test('test_intersect_predicate_ml', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir });
  const conn = db.connect();
  try {
    const res = conn.execute(
      'MATCH(p1)<-[e1:studyAt]-(t2), (p1)<-[e2:studyAt]-(t1), (t1)-[e3]-(t2)' +
      ' WHERE e1.year > 2020 RETURN e1.year,e2.year');
    assert.deepEqual([...res], [[2021, 2020], [2021, 2020], [2021, 2020], [2021, 2020]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-60

test('test_where_not_subquery', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir });
  const conn = db.connect();
  try {
    const res = conn.execute(
      'Match (a:person)-[:created]->(b)<-[:created]-(c:person)' +
      ' Where NOT (a)-[:knows]->(c) AND a <> c Return count(a);');
    assert.deepEqual([...res], [[5]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-61

test('test_where_subquery', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir });
  const conn = db.connect();
  try {
    const res = conn.execute(
      'Match (a:person)-[:created]->(b)<-[:created]-(c:person)' +
      ' Where (a)-[:knows]->(c) AND a <> c Return count(a);');
    assert.deepEqual([...res], [[1]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-62

test('test_exists_correlated_pattern_order', () => {
  const dbDir = makeTmpDir('exists_pat');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  conn.execute('CREATE NODE TABLE L0 (id STRING PRIMARY KEY);');
  conn.execute('CREATE NODE TABLE L2 (id STRING PRIMARY KEY);');
  conn.execute('CREATE REL TABLE T2 (FROM L2 TO L0);');
  conn.execute('CREATE REL TABLE T0 (FROM L0 TO L2);');

  conn.execute("CREATE (n:L2 {id: 'a'});");
  conn.execute("CREATE (n:L0 {id: 'b'});");
  conn.execute("CREATE (n:L2 {id: 'c'});");
  conn.execute("MATCH (n1:L2), (n2:L0) WHERE n1.id = 'a' AND n2.id = 'b' CREATE (n1)-[:T2]->(n2);");
  conn.execute("MATCH (n2:L0), (n3:L2) WHERE n2.id = 'b' AND n3.id = 'c' CREATE (n2)-[:T0]->(n3);");

  const q10 = 'MATCH (n1) WHERE EXISTS { MATCH (n1:L2)-[r1:T2]->(n2:L0), ' +
    '(n2:L0)-[r2:T0]->(n3:L2) } RETURN n1.id AS start_id';
  const q11 = 'MATCH (n1) WHERE EXISTS { MATCH (n2)-[r2:T0]->(n3:L2), ' +
    '(n1:L2)-[r1:T2]->(n2:L0) } RETURN n1.id AS start_id';

  const rows10 = [...conn.execute(q10)];
  const rows11 = [...conn.execute(q11)];
  assert.deepEqual(rows10, [['a']]);
  assert.deepEqual(rows11, [['a']]);

  conn.close();
  db.close();
});

// DB-003-63

test('test_checkpoint', () => {
  const dbDir = makeTmpDir('checkpoint');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT32, name STRING, PRIMARY KEY(id))');
  conn.execute("CREATE (p:Person {id: 1, name: 'Alice'});");
  conn.execute("CREATE (p:Person {id: 2, name: 'Bob'});");
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person)');
  conn.execute('CREATE REL TABLE Likes(FROM Person TO Person, weight DOUBLE)');
  conn.execute('CREATE REL TABLE Visits(FROM Person TO Person, time STRING, location STRING)');
  conn.execute('MATCH (p1:Person), (p2:Person) WHERE p1.id = 1 AND p2.id = 2 CREATE (p1)-[:Knows]->(p2);');
  conn.execute('MATCH (p1:Person), (p2:Person) WHERE p1.id = 1 AND p2.id = 2 CREATE (p1)-[:Likes {weight: 0.5}]->(p2);');
  conn.execute("MATCH (p1:Person), (p2:Person) WHERE p2.id = 1 AND p1.id = 2 CREATE (p1)-[:Visits {time: '2024-01-01', location: 'NYC'}]->(p2);");

  let res = conn.execute('MATCH (p1:Person)-[k:Knows]->(p2:Person) RETURN p1.id, p2.id;');
  assert.deepEqual([...res], [[1, 2]]);

  res = conn.execute('MATCH (p1:Person)-[k:Likes]->(p2:Person) RETURN p1.id, p2.id, k.weight;');
  assert.deepEqual([...res], [[1, 2, 0.5]]);

  res = conn.execute('MATCH (p1:Person)-[k:Visits]->(p2:Person) RETURN p1.id, p2.id, k.time, k.location;');
  assert.deepEqual([...res], [[2, 1, '2024-01-01', 'NYC']]);

  conn.execute('CHECKPOINT;');

  res = conn.execute('MATCH (p:Person) RETURN p.id, p.name;');
  assert.deepEqual([...res], [[1, 'Alice'], [2, 'Bob']]);

  res = conn.execute('MATCH (p1:Person)-[k:Knows]->(p2:Person) RETURN p1.id, p2.id;');
  assert.deepEqual([...res], [[1, 2]]);

  conn.close();
  db.close();
});

// DB-003-64

test('test_return_date', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute('MATCH (n) return n.birthdate limit 1');
    const records = [...result];
    assert.equal(records.length, 1);
    assert.equal(String(records[0][0]), '1900-01-01');
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-65

test('test_start_end_node', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'Match (n:PERSON {id: 933})-[k:KNOWS]->(m:PERSON {id: 2199023256077})' +
      ' Return START_NODE(k) as n1, END_NODE(k) as n2;');
    ensureResultCntGtZero(result);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-66

test('test_shortest_path', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'Match (n:PERSON {id: 933})-[k:KNOWS* SHORTEST  1..3]-(m:PERSON {id: 2199023256668}) Return LENGTH(k) Limit 1;');
    for (const record of result) {
      assert.equal(record[0], 3);
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-67

test('test_properties', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      "Match (n:PERSON {id: 933})-[k:KNOWS*1..3]-(m:PERSON {id: 2199023256668})" +
      " Return properties(nodes(k), 'firstName') as n1, properties(rels(k),'creationDate') as n2 LIMIT 1;");
    ensureResultCntGtZero(result);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-68

test('test_delete_edges', () => {
  const dbDir = makeTmpDir('delete_edges');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person, id INT64);');
  conn.execute('CREATE (p: Person {id: 111});');
  conn.execute('CREATE (p: Person {id: 222});');
  conn.execute('CREATE (p: Person {id: 333});');
  conn.execute('MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 333}]->(p2);');
  conn.execute('MATCH (p1: Person {id: 111}), (p2: Person {id: 333}) CREATE (p1)-[k:Knows {id: 444}]->(p2);');
  conn.execute('MATCH (p1: Person)-[k: Knows]->(p2:Person) WHERE k.id = 333 DELETE k');
  const res = conn.execute('MATCH (p1: Person)-[k: Knows]->(p2:Person) RETURN count(k)');
  assert.deepEqual([...res], [[1]]);
  conn.close();
  db.close();
});

// DB-003-69

test('test_internal_id_filter', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'Match (n:PERSON {id: 933}) Where id(n) = 72057594037927936 Return n.id;');
    for (const record of result) {
      assert.equal(record[0], 933);
    }
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-70

test('test_drop_person_if_exists', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  try {
    const result = conn.execute('drop table if exists person2;');
    assert.equal(result.length(), 0);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-71

test('test_drop_knows_if_exists', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  try {
    const result = conn.execute('drop table if exists knows2;');
    assert.equal(result.length(), 0);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-72

test('test_create_person_if_not_exists', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  try {
    conn.execute('create node table if not exists person(name STRING, PRIMARY KEY(name));');
    const res = conn.execute('match (p:person) return count(p.age);');
    assert.deepEqual([...res], [[4]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-73

test('test_create_knows_if_not_exists', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  try {
    conn.execute('create rel table if not exists knows(FROM person TO person, name STRING);');
    const res = conn.execute('match (p:person)-[r:knows]->(q:person) return count(r.weight);');
    assert.deepEqual([...res], [[2]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-74

test('test_undir_multi_label', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'MATCH (a:person:organisation)-[:meets|:marries|:workAt]-(b:person:organisation) RETURN COUNT(*);');
    assert.deepEqual([...result], [[26]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-75

test('test_mixed_match', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) MATCH (b)-[:knows]->(c:person) RETURN a.id,b.id,c.id;');
    const records = [...result];
    assert.equal(records.length, 36);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-76

test('test_mullti_label2', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'MATCH (a:person:organisation) OPTIONAL MATCH (a)-[:studyAt|:workAt]->(b:person:organisation) RETURN a.id,b.id;');
    const records = [...result];
    assert.equal(records.length, 11);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-77

test('test_recreate_vertex', () => {
  const dbDir = makeTmpDir('recreate_vtx');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE (p: Person {id: 111});');
  conn.execute('CREATE (p: Person {id: 222});');
  conn.execute('DROP TABLE IF EXISTS Person;');
  conn.execute('CREATE NODE TABLE Person(id INT64, age INT32, PRIMARY KEY(id));');
  let val = conn.execute('MATCH (n) return count(n);');
  assert.deepEqual([...val], [[0]]);
  conn.execute('CREATE (p: Person {id: 111, age: 20});');
  let res = conn.execute('MATCH (p: Person) RETURN p.id, p.age;');
  assert.deepEqual([...res], [[111, 20]]);
  conn.execute('DROP TABLE IF EXISTS Person;');
  conn.execute('CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));');
  conn.execute("CREATE (p: Person {id: 111, name: 'Alice'});");
  res = conn.execute('MATCH (p: Person) RETURN p.id, p.name;');
  assert.deepEqual([...res], [[111, 'Alice']]);
  conn.close();
  db.close();
});

// DB-003-78

test('test_recreate_edge', () => {
  const dbDir = makeTmpDir('recreate_edge');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person, id INT64);');
  conn.execute('CREATE (p: Person {id: 111});');
  conn.execute('CREATE (p: Person {id: 222});');
  conn.execute('MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 333}]->(p2);');
  conn.execute('DROP TABLE IF EXISTS Knows;');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person, weight DOUBLE);');
  let val = conn.execute('MATCH ()-[e:Knows]->() return count(e);');
  assert.deepEqual([...val], [[0]]);
  conn.execute('MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {weight: 1.5}]->(p2);');
  let res = conn.execute('MATCH (p1: Person)-[k: Knows]->(p2:Person) RETURN k.weight;');
  assert.deepEqual([...res], [[1.5]]);
  conn.execute('DROP TABLE IF EXISTS Knows;');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person, id INT64);');
  conn.execute('MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 444}]->(p2);');
  res = conn.execute('MATCH (p1: Person)-[k: Knows]->(p2:Person) RETURN k.id;');
  assert.deepEqual([...res], [[444]]);
  conn.close();
  db.close();
});

// DB-003-79

test('test_delete_vertex_detach_edge', () => {
  const dbDir = makeTmpDir('detach_edge');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person, id INT64);');
  conn.execute('CREATE (p: Person {id: 111});');
  conn.execute('CREATE (p: Person {id: 222});');
  conn.execute('MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 333}]->(p2);');
  conn.execute('MATCH (p1: Person)-[k: Knows]->(p2:Person) WHERE p1.id = 111 DETACH DELETE p1');
  let res = conn.execute('MATCH (p: Person) RETURN p.id;');
  assert.deepEqual([...res], [[222]]);
  conn.execute('DROP TABLE IF EXISTS Person;');
  res = conn.execute('MATCH ()-[e]->() RETURN count(e);');
  assert.deepEqual([...res], []);
  assert.throws(() => conn.execute('MATCH (p: Person) RETURN count(p);'));
  assert.throws(() => conn.execute('MATCH ()-[e: Knows]->() RETURN count(e);'));
  conn.close();
  db.close();
});

// DB-003-80

test('test_default_value', () => {
  const dbDir = makeTmpDir('default_val');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute("CREATE NODE TABLE Person(id INT64 PRIMARY KEY, age INT32 DEFAULT 18, name STRING DEFAULT 'unknown');");
  conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, since INT32 DEFAULT 2020, NOTE STRING DEFAULT 'none');");
  conn.execute('CREATE (p: Person {id: 111});');
  let res = conn.execute('MATCH (p: Person) RETURN p.id, p.age, p.name;');
  assert.deepEqual([...res], [[111, 18, 'unknown']]);
  conn.execute('CREATE (p: Person {id: 222, age: 25});');
  res = conn.execute('MATCH (p: Person {id: 222}) RETURN p.id, p.age, p.name;');
  assert.deepEqual([...res], [[222, 25, 'unknown']]);
  conn.execute('MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows]->(p2);');
  conn.execute("MATCH (p1: Person {id: 222}), (p2: Person {id: 111}) CREATE (p1)-[k:Knows {since: 2022, NOTE: 'updated'}]->(p2);");
  res = conn.execute('MATCH ()-[e: Knows]->() RETURN e.since, e.NOTE;');
  assert.deepEqual([...res], [[2020, 'none'], [2022, 'updated']]);
  conn.close();
  db.close();
});

// DB-003-81

test('test_delete_vertex_detach_edge2', () => {
  const dbDir = makeTmpDir('detach_edge2');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE NODE TABLE City(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE LivesIn(FROM Person TO City, id INT64);');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person, id INT64);');
  conn.execute('CREATE (p: Person {id: 1});');
  conn.execute('CREATE (p: Person {id: 2});');
  conn.execute('CREATE (p: Person {id: 3});');
  conn.execute('CREATE (c: City {id: 100});');
  conn.execute('CREATE (c: City {id: 200});');
  conn.execute('MATCH (p1: Person {id: 1}), (p2: Person {id: 2}) CREATE (p1)-[k:Knows {id: 10}]->(p2);');
  conn.execute('MATCH (p1: Person {id: 2}), (p2: Person {id: 3}) CREATE (p1)-[k:Knows {id: 20}]->(p2);');
  conn.execute('MATCH (p: Person {id: 3}), (p2: Person {id: 1}) CREATE (p)-[k:Knows {id: 30}]->(p2);');
  conn.execute('MATCH (p: Person {id: 1}), (c: City {id: 100}) CREATE (p)-[k:LivesIn {id: 1000}]->(c);');
  conn.execute('MATCH (p: Person {id: 2}), (c: City {id: 200}) CREATE (p)-[k:LivesIn {id: 2000}]->(c);');
  conn.execute('MATCH (p: Person {id: 3}), (c: City {id: 100}) CREATE (p)-[k:LivesIn {id: 3000}]->(c);');
  conn.execute('MATCH (p: Person {id: 3}), (c: City {id: 200}) CREATE (p)-[k:LivesIn {id: 4000}]->(c);');
  // NOTE: Python source has "DELETE p1" but variable is "p" — kept as-is
  conn.execute('MATCH (p1: Person {id: 3}) DELETE p1;');
  let res = conn.execute('MATCH (p: Person) RETURN count(p);');
  assert.deepEqual([...res], [[2]]);
  res = conn.execute('MATCH ()-[e: Knows]->() RETURN count(e);');
  assert.deepEqual([...res], [[1]]);
  res = conn.execute('MATCH ()-[e: LivesIn]->() RETURN count(e);');
  assert.deepEqual([...res], [[2]]);
  conn.close();
  db.close();
});

// DB-003-82

test('test_list_extract_function', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  try {
    const res = conn.execute(
      'Match (a) WITH a ORDER BY a.name RETURN labels(a) as label, collect(a.name)[0];');
    assert.deepEqual([...res], [['person', 'josh'], ['software', 'lop']]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-83

test('test_weight_shortest_path', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const res = conn.execute(
      "Match (a:person {name : 'marko'})-[k * WSHORTEST(weight)]-(b:person {name: 'josh'})" +
      " Return a.name, b.name, cost(k);");
    assert.deepEqual([...res], [['marko', 'josh', 0.8]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-84

test('test_optional_match_person_software', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const res = conn.execute(
      'MATCH (p: PERSON) WHERE p.id=1' +
      ' OPTIONAL MATCH (p)-[]-(other:PERSON:SOFTWARE) WHERE other.id>0 RETURN other;');
    const records = [...res];
    assert.deepEqual(records, [
      [{ _ID: 1, id: 2, name: 'vadas', age: 27, _LABEL: 'person' }],
      [{ _ID: 2, id: 4, name: 'josh', age: 32, _LABEL: 'person' }],
      [{ _ID: 72057594037927936, id: 3, name: 'lop', lang: 'java', _LABEL: 'software' }],
    ]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-85

test('test_optional_match_person_software_with_edge_weight', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const res = conn.execute(
      'MATCH (p: PERSON) WHERE p.id=1' +
      ' OPTIONAL MATCH (p)-[e]->(other:PERSON:Software)' +
      ' WHERE e.weight>10 and other.id>10 RETURN other;');
    assert.deepEqual([...res], [[null]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-86

test('test_multi_ddl_queries', () => {
  const dbDir = makeTmpDir('multi_ddl');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  assert.throws(
    () => conn.execute('CREATE NODE TABLE N (id SERIAL, PRIMARY KEY(id));'),
    /Unsupported basic type for conversion: SERIAL/);
  conn.close();
  db.close();
});

// DB-003-87

test('test_parameterized_query', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const res = conn.execute(
      'MATCH (n:PERSON {id: $person_id})-[:KNOWS]->(m:PERSON) RETURN m.name;',
      '', { person_id: 1 });
    assert.deepEqual([...res], [['vadas'], ['josh']]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-88

test('test_alter_table_add_property_with_default_tinysnb', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'w', checkpointOnClose: false });
  const conn = db.connect();
  try {
    conn.execute('ALTER TABLE person ADD propy INT64 DEFAULT 10;');
    const result = conn.execute('MATCH (c:person) RETURN c.propy LIMIT 1;');
    assert.deepEqual([...result], [[10]]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-89

test('test_filtering', () => {
  const dbDir = '/tmp/tinysnb';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute(
      'MATCH (a:person)-[e1:knows]->(b:person) WHERE a.age > 35 RETURN b.fName');
    assert.deepEqual([...result], [['Alice'], ['Bob'], ['Dan']]);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-90

test('test_result', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conn = db.connect();
  try {
    const result = conn.execute('Match (n: person) return n');
    [...result]; // consume
    // Just verify it doesn't throw
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-91

test('test_parameterized_where_on_edge_string_property', () => {
  const db = new Database({ databasePath: ':memory', mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE IF NOT EXISTS A(id STRING PRIMARY KEY)');
  conn.execute('CREATE REL TABLE IF NOT EXISTS R(FROM A TO A, tag STRING)');
  conn.execute("CREATE (a:A {id: 'n1'})");
  conn.execute("CREATE (a:A {id: 'n2'})");
  conn.execute("MATCH (a:A), (b:A) WHERE a.id = 'n1' AND b.id = 'n2' CREATE (a)-[:R {tag: 'hello'}]->(b)");
  const resLiteral = conn.execute("MATCH (a:A)-[e:R]->(b:A) WHERE e.tag = 'hello' RETURN e.tag");
  assert.deepEqual([...resLiteral], [['hello']]);
  const resParam = conn.execute(
    'MATCH (a:A)-[e:R]->(b:A) WHERE e.tag = $t RETURN e.tag', '', { t: 'hello' });
  assert.deepEqual([...resParam], [['hello']]);
  conn.close();
  db.close();
});

// DB-003-92

test('test_insert_many_vertices', () => {
  const dbDir = makeTmpDir('many_vtx');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));');
  for (let i = 0; i < 10000; i++) conn.execute(`CREATE (p: Person {id: ${i}});`);
  assert.deepEqual([...conn.execute('MATCH (p: Person) RETURN count(p);')], [[10000]]);
  conn.close();
  db.close();
});

// DB-003-93

test('test_insert_many_edges', () => {
  const dbDir = makeTmpDir('many_edges');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person);');
  for (let i = 0; i < 100; i++) conn.execute(`CREATE (p: Person {id: ${i}});`);
  for (let i = 0; i < 100; i++)
    for (let j = i + 1; j < 100; j++)
      conn.execute(`MATCH (p1: Person {id: ${i}}), (p2: Person {id: ${j}}) CREATE (p1)-[:Knows]->(p2);`);
  assert.deepEqual([...conn.execute('MATCH ()-[e: Knows]->() RETURN count(e);')], [[4950]]);
  conn.close();
  db.close();
});

// DB-003-94

test('test_insert_string_column_exhaustion', () => {
  const dbDir = makeTmpDir('str_exhaust');
  let db = new Database({ databasePath: dbDir, mode: 'w' });
  let conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));');
  conn.execute("CREATE (p: Person {id: 1, name: 'a'});");
  conn.execute("CREATE (p: Person {id: 2, name: 'b'});");
  conn.execute('CHECKPOINT;');
  conn.close();
  db.close();
  let db2 = new Database({ databasePath: dbDir, mode: 'w' });
  let conn2 = db2.connect();
  const strProp = 'a'.repeat(255);
  for (let i = 0; i < 10000; i++) conn2.execute(`CREATE (p: Person {id: ${i + 3}, name: '${strProp}'});`);
  assert.deepEqual([...conn2.execute('MATCH (p: Person) RETURN count(p);')], [[10002]]);
  conn2.close();
  db2.close();
  let db3 = new Database({ databasePath: dbDir, mode: 'w' });
  let conn3 = db3.connect();
  conn3.execute('CREATE REL TABLE Knows(FROM Person TO Person, note STRING);');
  conn3.execute("MATCH (a: Person), (b: Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:Knows {note: '12'}]->(b);");
  conn3.execute("MATCH (a: Person), (b: Person) WHERE a.id = 3 AND b.id = 4 CREATE (a)-[:Knows {note: '34'}]->(b);");
  conn3.execute('CHECKPOINT;');
  db3.close();
  let db4 = new Database({ databasePath: dbDir, mode: 'w' });
  let conn4 = db4.connect();
  let res4 = conn4.execute('MATCH (a: Person)-[k: Knows]->(b: Person) RETURN k.note ORDER BY k.note;');
  assert.deepEqual([...res4], [['12'], ['34']]);
  for (let i = 0; i < 100; i++)
    conn4.execute(`MATCH (a: Person {id: 1}), (b: Person {id: 2}) CREATE (a)-[:Knows {note: '${strProp}'}]->(b);`);
  conn4.close();
  db4.close();
});

// DB-003-95

test('test_edge_default_value', () => {
  const dbDir = makeTmpDir('edge_default');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE IF NOT EXISTS TestNode(id INT64 PRIMARY KEY, thread_id INT64)');
  conn.execute('CREATE REL TABLE IF NOT EXISTS TestEdge(FROM TestNode TO TestNode)');
  conn.execute("ALTER TABLE TestEdge ADD description STRING DEFAULT 'unknown'");
  conn.execute('CREATE (n1: TestNode {id: 1, thread_id: 1}), (n2: TestNode {id: 2, thread_id: 1}) CREATE (n1)-[:TestEdge]->(n2);');
  assert.deepEqual([...conn.execute('MATCH ()-[e: TestEdge]->() RETURN e.description;')], [['unknown']]);
  conn.close();
  db.close();
});

// DB-003-96

test('test_optional_match_on_edge', () => {
  const dbDir = makeTmpDir('opt_match_edge');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE SRC_INFRA(id STRING PRIMARY KEY, finder STRING);');
  conn.execute('CREATE NODE TABLE SRC_LOGGING(id STRING PRIMARY KEY, finder STRING);');
  conn.execute('CREATE REL TABLE CALLS_NEW (FROM SRC_INFRA TO SRC_INFRA);');
  conn.execute("CREATE (u: SRC_INFRA {id: '1', finder: 'finder'});");
  conn.execute("CREATE (u: SRC_INFRA {id: '2', finder: 'finder'});");
  conn.execute("CREATE (u: SRC_LOGGING {id: '1', finder: 'finder'});");
  const result = conn.execute("MATCH (u) WHERE u.finder = 'finder' OPTIONAL MATCH (u)-[e:CALLS_NEW]-(v) RETURN u, e, v;");
  assert.equal([...result].length, 3);
  conn.close();
  db.close();
});

// DB-003-97

test('test_is_not_null_on_node_variable', () => {
  const dbDir = makeTmpDir('is_not_null');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Node(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE (a:Node {id: 1});');
  let result = conn.execute('MATCH (a:Node) WHERE a IS NOT NULL RETURN 1;', 'read');
  let records = [...result];
  assert.equal(records.length, 1);
  assert.equal(records[0][0], 1);
  conn.execute('CREATE NODE TABLE person(id INT64 PRIMARY KEY);');
  conn.execute('CREATE REL TABLE knows(FROM person TO person);');
  conn.execute('CREATE (a:person {id: 1});');
  conn.execute('CREATE (a:person {id: 2});');
  conn.execute('MATCH (a:person {id: 1}), (b:person {id: 2}) CREATE (a)-[:knows]->(b);');
  result = conn.execute('MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) WHERE b IS NULL RETURN a, b;', 'read');
  records = [...result];
  const ids = records.map(row => row[0].id).sort();
  assert.deepEqual(ids, [1, 2]);
  conn.close();
  db.close();
});

// DB-003-98

test('test_drop_and_recreate_table_same_name', () => {
  const dbDir = makeTmpDir('drop_recreate');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  const queries = [
    'CREATE NODE TABLE Y0(id STRING, p0 INT32, PRIMARY KEY(id));',
    'CREATE NODE TABLE Y1(id STRING, p1 STRING, PRIMARY KEY(id));',
    'CREATE REL TABLE YR0(FROM Y0 TO Y1, rp0 DOUBLE);',
    'CREATE (a:Y0 {id: "a", p0: 1});',
    'CREATE (b:Y1 {id: "b", p1: "x"});',
    'MATCH (a:Y0 {id: "a"}), (b:Y1 {id: "b"}) CREATE (a)-[:YR0 {rp0: 1.5}]->(b);',
    'DROP TABLE IF EXISTS Y1;',
    'DROP TABLE IF EXISTS Y0;',
    'CREATE NODE TABLE Y0(id STRING, q DOUBLE, PRIMARY KEY(id));',
  ];
  for (const q of queries) conn.execute(q);
  conn.execute('CREATE (c:Y0 {id: "c", q: 3.14});');
  const result = conn.execute('MATCH (n:Y0) RETURN n.id, n.q;');
  const rows = [...result];
  assert.equal(rows.length, 1);
  assert.equal(rows[0][0], 'c');
  assert.ok(Math.abs(rows[0][1] - 3.14) < 1e-6);
  conn.close();
  db.close();
});

// DB-003-99

test('test_unwind_t1_l3_l4_read_from_explicit_schema', () => {
  const cols = 'id INT64, k19 BOOL, k18 BOOL, k20 INT64, k22 BOOL, k21 INT64, k24 STRING, k23 BOOL, ' +
    'k30 BOOL, k25 STRING, k26 BOOL, k28 INT64, k27 INT64, k29 INT64, PRIMARY KEY (id)';
  const ddl = [
    `CREATE NODE TABLE L3 (${cols});`,
    `CREATE NODE TABLE L4 (${cols});`,
    'CREATE REL TABLE T1 (FROM L3 TO L3, k39 STRING, k38 BOOL, k40 BOOL, k42 INT64, k41 STRING, id INT64, k43 BOOL);',
  ];
  const dml = [
    'CREATE (n0 :L3 {k19 : false, k18 : true, k20 : 1400705806, k22 : true, id : 44, k21 : 685854768, k23 : false});',
    'CREATE (n0 :L3 {k20 : 512128668, k30 : true, k22 : true, k21 : -1607710882, k24 : "ct", k23 : false, k26 : false, k25 : "0", k28 : -1022812775, k27 : 1963567328, k19 : false, k29 : 787123989, k18 : true, id : 120});',
    'CREATE (n0 :L4 {k20 : 512128668, k30 : true, k22 : true, k21 : -1607710882, k24 : "ct", k23 : false, k26 : false, k25 : "0", k28 : -1022812775, k27 : 1963567328, k19 : false, k29 : 787123989, k18 : true, id : 120});',
    'MATCH (n0 :L3 {id : 120}), (n1 :L3 {id : 44}) CREATE (n0)-[r :T1{k39 : "Q", k38 : false, k40 : false, k42 : 1062135372, k41 : "g", id : 131, k43 : true}]->(n1);',
  ];
  const readQ = 'MATCH (n1 :L3)<-[r1 :T1]-(n2 :L3 :L4) WHERE ((r1.id) > -1) ' +
    'UNWIND [(n2.k28), -1206557154, (n2.k28)] AS a0 UNWIND [(n2.k20), -986093799, (n1.k20)] AS a1 ' +
    'RETURN DISTINCT a1, (r1.k43) AS a2;';
  const dbDir = makeTmpDir('unwind_t1');
  const db = new Database({ databasePath: dbDir, mode: 'w', checkpointOnClose: false });
  const conn = db.connect();
  for (const s of ddl) conn.execute(s, 'schema');
  for (const s of dml) conn.execute(s, 'update');
  const res = conn.execute(readQ, 'read');
  const rows = [...res];
  const expectedA1 = new Set([-986093799, 512128668, 1400705806]);
  assert.equal(rows.length, 3);
  assert.deepEqual(new Set(rows.map(r => r[0])), expectedA1);
  for (const r of rows) assert.ok(r[1] === true || r[1] === 1);
  conn.close();
  db.close();
});

// DB-003-100

test('test_duplicate_project_column', () => {
  const dbDir = makeTmpDir('dup_proj');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE L0(id INT64, p0_2 INT64, PRIMARY KEY(id))');
  conn.execute('CREATE (:L0 {id: 1, p0_2: 643})');
  const result = conn.execute(
    'MATCH (n:L0) WHERE n.p0_2 = $v RETURN n.id AS node_id, n.id AS selected_id ORDER BY node_id LIMIT 100',
    '', { v: 643 });
  assert.deepEqual([...result], [[1, 1]]);
  conn.close();
  db.close();
});

// DB-003-101

test('test_not_starts_with', () => {
  const dbDir = makeTmpDir('not_starts');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id STRING, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE Knows(FROM Person TO Person, id STRING);');
  conn.execute("CREATE (:Person {id: 'n4'});");
  conn.execute("CREATE (:Person {id: 'n8'});");
  conn.execute("MATCH (a:Person {id: 'n4'}), (b:Person {id: 'n8'}) CREATE (a)-[:Knows {id: 'e19'}]->(b);");
  const result = conn.execute(
    "MATCH (a:Person {id: 'n4'})-[r0:Knows {id: 'e19'}]->(b:Person {id: 'n8'})" +
    " WHERE NOT ('a' STARTS WITH 'a') OR (r0.id IN [a.id]) RETURN a.id AS source_id, b.id AS target_id;");
  assert.deepEqual([...result], []);
  conn.close();
  db.close();
});

// DB-003-102

test('test_drop_and_recreate_node_table_no_stale_data', () => {
  const dbDir = makeTmpDir('drop_no_stale');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE IF NOT EXISTS Person(id STRING PRIMARY KEY);');
  conn.execute("CREATE (p:Person {id: 'alice'});");
  conn.execute('CHECKPOINT');
  assert.deepEqual([...conn.execute('MATCH (p:Person) RETURN p.id;')], [['alice']]);
  conn.execute('DROP TABLE IF EXISTS Person;');
  conn.execute('CREATE NODE TABLE IF NOT EXISTS Person(id STRING PRIMARY KEY);');
  assert.deepEqual([...conn.execute('MATCH (p:Person) RETURN p.id;')], []);
  conn.execute("CREATE (p:Person {id: 'bob'});");
  assert.deepEqual([...conn.execute('MATCH (p:Person) RETURN p.id;')], [['bob']]);
  conn.close();
  db.close();
});

// DB-003-103

test('test_not_list_contains', () => {
  const dbDir = makeTmpDir('not_list');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE L1(id STRING, p0 STRING, PRIMARY KEY(id));');
  conn.execute("CREATE (:L1 {id: 'n1', p0: 's3836'});");
  conn.execute("CREATE (:L1 {id: 'n2', p0: 'x'});");
  conn.execute("CREATE (:L1 {id: 'n3', p0: 'y'});");
  assert.deepEqual([...conn.execute('MATCH (n:L1) RETURN count(n) AS pair_count;')], [[3]]);
  assert.deepEqual([...conn.execute("MATCH (n:L1) WHERE (n.p0 IN ['s3836', 'L1']) RETURN count(n) AS pair_count;")], [[1]]);
  assert.deepEqual([...conn.execute("MATCH (n:L1) WHERE NOT (n.p0 IN ['s3836', 'L1']) RETURN count(n) AS pair_count;")], [[2]]);
  assert.deepEqual([...conn.execute("MATCH (n:L1) WHERE ((n.p0 IN ['s3836', 'L1'])) IS NULL RETURN count(n) AS pair_count;")], [[0]]);
  conn.close();
  db.close();
});

// DB-003-104

test('test_sort_csr_compact', async () => {
  const dbDir = makeTmpDir('sort_csr');
  let db = new Database({ databasePath: dbDir, mode: 'rw' });
  let conn = db.connect();
  conn.execute('CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))');
  conn.execute(
    "CREATE REL TABLE Knows(FROM Person TO Person, since INT64) WITH (sort_key_for_nbr='since')"
  );
  for (let i = 0; i < 100; i++) {
    conn.execute(`CREATE (:Person {id: ${i}});`);
  }
  for (let i = 0; i < 100; i++) {
    if (i % 2 === 0) {
      conn.execute(
        `MATCH (a:Person {id: 1}), (b:Person {id: ${i}}) CREATE (a)-[:Knows {since: ${i}}]->(b:Person);`
      );
    } else {
      conn.execute(
        `MATCH (a:Person {id: 0}), (b:Person {id: ${i}}) CREATE (a)-[:Knows {since: ${i}}]->(b);`
      );
    }
  }
  conn.close();
  db.close();

  db = new Database({ databasePath: dbDir, mode: 'rw' });
  const endpoint = db.serve({ host: '127.0.0.1', port: SESSION_PORT, blocking: false });
  const sess = await Session.open({ endpoint, timeout: '30s' });
  try {
    await sess.execute(
      "MATCH (a:Person {id: 1}), (b:Person {id: 98}) CREATE (a)-[:Knows {since: 1}]->(b);"
    );
    await sess.execute(
      "MATCH (a:Person {id: 0}), (b:Person {id: 1}) CREATE (a)-[:Knows {since: 100}]->(b);"
    );

    let res = await sess.execute(
      'MATCH (a: Person {id: 1})-[r:Knows]-> (b: Person) WHERE r.since < 2 RETURN b.id, r.since'
    );
    assert.deepEqual([...res], [[0, 0], [98, 1]]);

    res = await sess.execute(
      'MATCH (a: Person {id: 0})-[r:Knows]-> (b: Person) WHERE r.since > 99 RETURN b.id, r.since'
    );
    assert.deepEqual([...res], [[1, 100]]);
  } finally {
    sess.close();
    db.stopServing();
    db.close();
  }
});

// DB-003-105

test('test_unsupported_operator_error_message', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'rw' });
  const conn = db.connect();
  try {
    assert.throws(() => conn.execute('CREATE MACRO f(x) AS x + 1'),
      /Unsupported operator type: CREATE_MACRO/);
  } finally {
    conn.close();
    db.close();
  }
});

// DB-003-106

test('test_delete_all_rows_then_reinsert_visible', () => {
  const dbDir = makeTmpDir('del_reinsert');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE IF NOT EXISTS Person(id STRING PRIMARY KEY);');
  conn.execute("CREATE (p:Person {id: 'alice'});");
  conn.execute("CREATE (p:Person {id: 'bob'});");
  conn.execute('CHECKPOINT');
  const sorted = [...conn.execute('MATCH (p:Person) RETURN p.id;')].sort();
  assert.deepEqual(sorted, [['alice'], ['bob']]);
  conn.execute('MATCH (a:Person) DELETE a;');
  assert.deepEqual([...conn.execute('MATCH (p:Person) RETURN p.id;')], []);
  conn.execute("CREATE (p:Person {id: 'charlie'});");
  assert.deepEqual([...conn.execute('MATCH (p:Person) RETURN p.id;')], [['charlie']]);
  conn.close();
  db.close();
});
