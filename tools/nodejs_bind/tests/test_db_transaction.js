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
const {
  Database,
  ERR_DATABASE_LOCKED,
  ERR_TX_STATE_CONFLICT,
  ERR_TYPE_CONVERSION,
  ERR_SCHEMA_MISMATCH,
  ERR_TX_TIMEOUT,
} = require('neug');

// ---------------------------------------------------------------------------
// Helpers (mirrors Python tmp_path fixture)
// ---------------------------------------------------------------------------

let _tmpCounter = 0;
const _tmpDirs = [];
function makeTmpDir(prefix = 'neug_tx_test_') {
  const dir = fs.mkdtempSync(
    path.join(os.tmpdir(), prefix + _tmpCounter++ + '_')
  );
  _tmpDirs.push(dir);
  return dir;
}

after(() => {
  for (const dir of _tmpDirs) {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch (_) {
      // ignore cleanup errors
    }
  }
});

// ---------------------------------------------------------------------------
// DB-004-01
// ---------------------------------------------------------------------------

test('test_ap_read_concurrent', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const conns = [];
  for (let i = 0; i < 4; i++) {
    conns.push(db.connect());
  }
  for (const conn of conns) {
    const result = conn.execute('MATCH (n) RETURN n');
    assert.equal(result.length(), 6);
  }
  for (const conn of conns) {
    conn.close();
  }
  db.close();
});

// ---------------------------------------------------------------------------
// DB-004-02
// ---------------------------------------------------------------------------

test('test_ap_write_concurrent', () => {
  const dbDir = makeTmpDir('ap_write_concurrent');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  assert.throws(
    () => db.connect(),
    (err) => err.message.includes(String(ERR_TX_STATE_CONFLICT))
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-004-03
// ---------------------------------------------------------------------------

test('test_ap_read_write_concurrent', () => {
  const dbDir = '/tmp/modern_graph';
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  assert.throws(
    () => db.connect(),
    (err) => err.message.includes(String(ERR_TX_STATE_CONFLICT))
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-004-07
// ---------------------------------------------------------------------------

test('test_auto_transaction_management', () => {
  const dbDir = makeTmpDir('auto_tx_mgmt');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  // create success, commit automatically
  conn.execute('CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));');
  conn.execute('CREATE (n:T {id: 1});');
  let r = conn.execute('MATCH (n:T) RETURN n;');
  assert.equal(r.length(), 1);

  // create with errors, rollback automatically
  assert.throws(
    () => conn.execute("CREATE (n:T {id: 'bad_type'});"),
    (err) => err.message.includes(String(ERR_TYPE_CONVERSION))
  );
  r = conn.execute('MATCH (n:T) RETURN n;');
  assert.equal(r.length(), 1);

  assert.throws(
    () => conn.execute('CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));'),
    (err) => err.message.includes(String(ERR_SCHEMA_MISMATCH))
  );
  r = conn.execute('MATCH (n:T) RETURN n;');
  assert.equal(r.length(), 1);

  assert.throws(
    () => conn.execute('ALTER TABLE T DROP not_exist;'),
    (err) => err.message.includes(String(ERR_SCHEMA_MISMATCH))
  );
  r = conn.execute('MATCH (n:T) RETURN n;');
  assert.equal(r.length(), 1);

  assert.throws(
    () => conn.execute('DROP TABLE not_exist;'),
    (err) => err.message.includes(String(ERR_SCHEMA_MISMATCH))
  );
  r = conn.execute('MATCH (n:T) RETURN n;');
  assert.equal(r.length(), 1);

  assert.throws(
    () => conn.execute('MATCH (n:T) WHERE n.id = 1 SET n.not_exist = 1;'),
    (err) => err.message.includes(String(ERR_SCHEMA_MISMATCH))
  );
  r = conn.execute('MATCH (n:T) RETURN n;');
  assert.equal(r.length(), 1);

  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-004-08
// ---------------------------------------------------------------------------

test('test_manual_transaction_management', { skip: 'BEGIN TRANSACTION is not planned yet' }, () => {
  // Not implemented
});

// ---------------------------------------------------------------------------
// DB-004-09
// ---------------------------------------------------------------------------

test('test_readonly_transaction_write', { skip: 'BEGIN TRANSACTION is not planned yet' }, () => {
  // Not implemented
});

// ---------------------------------------------------------------------------
// DB-004-11
// ---------------------------------------------------------------------------

test('test_nested_transaction', { skip: 'BEGIN TRANSACTION is not planned yet' }, () => {
  // Not implemented
});

// ---------------------------------------------------------------------------
// DB-004-12
// ---------------------------------------------------------------------------

test('test_transaction_timeout', { skip: 'BEGIN TRANSACTION is not planned yet' }, () => {
  // Not implemented
});

// ---------------------------------------------------------------------------
// DB-004-13
// ---------------------------------------------------------------------------

test('test_commit_after_rollback', { skip: 'BEGIN TRANSACTION is not planned yet' }, () => {
  // Not implemented
});

// ---------------------------------------------------------------------------
// DB-004-14
// ---------------------------------------------------------------------------

test('test_crash_recovery', { skip: 'BEGIN TRANSACTION is not planned yet' }, () => {
  // Not implemented
});

// ---------------------------------------------------------------------------
// DB-004-15
// ---------------------------------------------------------------------------

test('test_auto_enable_checkpoint', () => {
  const dbDir = makeTmpDir('test_checkpoint_auto');

  // 1. open database and create some data
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();
  db.close();

  // 2. reopen database with checkpoint
  const db2 = new Database({ databasePath: dbDir, mode: 'w' });
  const conn2 = db2.connect();
  const result = conn2.execute(
    'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
  );
  const rows = [...result];
  assert.deepEqual(rows, [
    [1n, 'Alice', 30],
    [2n, 'Bob', 25],
  ]);
  conn2.close();
  db2.close();
});

// ---------------------------------------------------------------------------
// DB-004-16
// ---------------------------------------------------------------------------

test('test_manual_enable_checkpoint', () => {
  const dbDir = makeTmpDir('test_checkpoint_manual');

  // 1. open database with checkpointOnClose=true
  const db = new Database({
    databasePath: dbDir,
    mode: 'w',
    checkpointOnClose: true,
  });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();
  db.close();

  // 2. reopen database with checkpoint
  const db2 = new Database({ databasePath: dbDir, mode: 'w' });
  const conn2 = db2.connect();
  const result = conn2.execute(
    'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
  );
  const rows = [...result];
  assert.deepEqual(rows, [
    [1n, 'Alice', 30],
    [2n, 'Bob', 25],
  ]);
  conn2.close();
  db2.close();
});

// ---------------------------------------------------------------------------
// DB-004-17
// ---------------------------------------------------------------------------

test('test_manual_disable_checkpoint', () => {
  const dbDir = makeTmpDir('test_checkpoint_disable');

  // 1. open database with checkpointOnClose=false
  const db = new Database({
    databasePath: dbDir,
    mode: 'w',
    checkpointOnClose: false,
  });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();
  db.close();

  // 2. reopen database with no checkpoint
  const db2 = new Database({ databasePath: dbDir, mode: 'w' });
  const conn2 = db2.connect();
  const result = conn2.execute('MATCH (p) RETURN p;');
  const rows = [...result];
  assert.deepEqual(rows, []);
  conn2.close();
  db2.close();
});

// ---------------------------------------------------------------------------
// DB-004-18
// ---------------------------------------------------------------------------

test('test_manual_checkpoint_command', () => {
  const dbDir = makeTmpDir('test_checkpoint_cmd');

  // 1. open database with checkpointOnClose=false, then CHECKPOINT manually
  const db = new Database({
    databasePath: dbDir,
    mode: 'w',
    checkpointOnClose: false,
  });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.execute('CHECKPOINT;');
  conn.close();
  db.close();

  // 2. reopen database with checkpoint
  const db2 = new Database({ databasePath: dbDir, mode: 'w' });
  const conn2 = db2.connect();
  const result = conn2.execute(
    'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
  );
  const rows = [...result];
  assert.deepEqual(rows, [
    [1n, 'Alice', 30],
    [2n, 'Bob', 25],
  ]);
  conn2.close();
  db2.close();
});

// ---------------------------------------------------------------------------
// DB-004-19
// ---------------------------------------------------------------------------

test('test_pure_memory_without_parameter', () => {
  // 1. open database with pure_memory model (empty string path)
  let db = new Database({ databasePath: '', mode: 'w' });
  let conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();

  // 2. reopen database with pure_memory model, data is lost
  db = new Database({ databasePath: '', mode: 'w' });
  conn = db.connect();
  let result = conn.execute('MATCH (p) RETURN p;');
  assert.deepEqual([...result], []);
  conn.close();

  // 3. open a pure_memory database using :memory
  db = new Database({ databasePath: ':memory', mode: 'w' });
  conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();
});

test('test_pure_memory_with_true_parameter', () => {
  // 1. open database with pure_memory model and checkpointOnClose=true
  let db = new Database({
    databasePath: '',
    mode: 'w',
    checkpointOnClose: true,
  });
  let conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();

  // 2. reopen database with pure_memory model, data is lost
  db = new Database({ databasePath: '', mode: 'w' });
  conn = db.connect();
  const result = conn.execute('MATCH (p) RETURN p;');
  assert.deepEqual([...result], []);
  conn.close();
});

test('test_pure_memory_with_false_parameter', () => {
  // 1. open database with pure_memory model and checkpointOnClose=false
  let db = new Database({
    databasePath: '',
    mode: 'w',
    checkpointOnClose: false,
  });
  let conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();

  // 2. reopen database with pure_memory model, data is lost
  db = new Database({ databasePath: '', mode: 'w' });
  conn = db.connect();
  const result = conn.execute('MATCH (p) RETURN p;');
  assert.deepEqual([...result], []);
  conn.close();
});

// ---------------------------------------------------------------------------
// DB-004-20
// ---------------------------------------------------------------------------

test('test_database_concurrent_read', () => {
  const dbDir = makeTmpDir('db_concurrent_read');

  // 1. open database and create some data
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();
  db.close();

  // 2. read data concurrently from two separate Database instances
  const db1 = new Database({ databasePath: dbDir, mode: 'r' });
  const conn1 = db1.connect();
  const result1 = conn1.execute(
    'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
  );
  const rows1 = [...result1];

  const db2 = new Database({ databasePath: dbDir, mode: 'r' });
  const conn2 = db2.connect();
  const result2 = conn2.execute(
    'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
  );
  const rows2 = [...result2];

  assert.deepEqual(rows1, [
    [1n, 'Alice', 30],
    [2n, 'Bob', 25],
  ]);
  assert.deepEqual(rows1, rows2);
  conn1.close();
  db1.close();
  conn2.close();
  db2.close();
});

// ---------------------------------------------------------------------------
// DB-004-21
// ---------------------------------------------------------------------------

test('test_database_concurrent_lock', () => {
  const dbDir = makeTmpDir('db_concurrent_lock');

  // 1. open database and create some data
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.close();
  db.close();

  // 2. read-lock: hold a read connection, write should fail
  const db1 = new Database({ databasePath: dbDir, mode: 'r' });
  const conn1 = db1.connect();
  conn1.execute('MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;');

  assert.throws(() => {
    let db2 = null;
    try {
      db2 = new Database({ databasePath: dbDir, mode: 'w' });
      const conn2 = db2.connect();
      conn2.execute(
        'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
      );
      conn2.close();
    } finally {
      if (db2) db2.close();
    }
  }, (err) => err.message.includes(String(ERR_DATABASE_LOCKED)));

  conn1.close();
  db1.close();

  // 3. write-lock: hold a write connection, read and write should both fail
  const db1w = new Database({ databasePath: dbDir, mode: 'w' });
  const conn1w = db1w.connect();
  conn1w.execute(
    'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
  );

  assert.throws(() => {
    let db2 = null;
    try {
      db2 = new Database({ databasePath: dbDir, mode: 'r' });
      const conn2 = db2.connect();
      conn2.execute(
        'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
      );
      conn2.close();
    } finally {
      if (db2) db2.close();
    }
  }, (err) => err.message.includes(String(ERR_DATABASE_LOCKED)));

  assert.throws(() => {
    let db3 = null;
    try {
      db3 = new Database({ databasePath: dbDir, mode: 'w' });
      const conn3 = db3.connect();
      conn3.execute(
        'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
      );
      conn3.close();
    } finally {
      if (db3) db3.close();
    }
  }, (err) => err.message.includes(String(ERR_DATABASE_LOCKED)));

  conn1w.close();
  db1w.close();
});

// ---------------------------------------------------------------------------
// DB-004-22
// ---------------------------------------------------------------------------

test('test_checkpoint_alter', () => {
  const dbDir = makeTmpDir('checkpoint_alter');

  const db = new Database({
    databasePath: dbDir,
    mode: 'w',
    checkpointOnClose: true,
  });
  const conn = db.connect();
  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));'
  );
  conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");
  conn.execute('ALTER TABLE person ADD creation INT64;');
  conn.close();
  db.close();

  const db2 = new Database({ databasePath: dbDir, mode: 'w' });
  const conn2 = db2.connect();
  const result = conn2.execute('MATCH (p:person) RETURN p.creation;');
  const rows = [...result];
  assert.deepEqual(rows, [[0n], [0n]]);
  conn2.execute('ALTER TABLE person DROP creation;');
  const result2 = conn2.execute(
    'MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;'
  );
  const rows2 = [...result2];
  assert.deepEqual(rows2, [
    [1n, 'Alice', 30],
    [2n, 'Bob', 25],
  ]);
  conn2.close();
  db2.close();
});
