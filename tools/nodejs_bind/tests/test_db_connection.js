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
  ERR_CONFIG_INVALID,
  ERR_CONNECTION_CLOSED,
} = require('neug');

// ---------------------------------------------------------------------------
// Helpers (mirrors Python tmp_path fixture)
// ---------------------------------------------------------------------------

let _tmpCounter = 0;
const _tmpDirs = [];
function makeTmpDir(prefix = 'neug_conn_test_') {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix + _tmpCounter++ + '_'));
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
// DB-002-01 & DB-002-02
// ---------------------------------------------------------------------------

test('test_local_connection', () => {
  const dbDir = makeTmpDir('local_conn_db');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  assert.ok(conn);
  conn.close();
  db.close();
});

test('test_open_after_close', () => {
  const dbDir = makeTmpDir('open_after_close_db');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  assert.ok(conn);
  conn.close();
  // try to open a new connection after closing the previous one
  const newConn = db.connect();
  assert.ok(newConn);
  newConn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-002-03
// ---------------------------------------------------------------------------

test('test_local_connection_params', () => {
  const dbDir = makeTmpDir('local_conn_param_db');
  const db = new Database({ databasePath: dbDir, mode: 'w', maxThreadNum: Database.cpuCount() });
  const conn = db.connect();
  assert.ok(conn);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-002-04
// ---------------------------------------------------------------------------

test('test_local_connection_invalid_param', () => {
  const dbDir = makeTmpDir('local_conn_invalid_db');
  assert.throws(
    () => {
      new Database({ databasePath: dbDir, mode: 'w', maxThreadNum: -1 });
    },
    (err) => err.message.includes(String(ERR_CONFIG_INVALID))
  );
});

// ---------------------------------------------------------------------------
// DB-002-12 (local)
// ---------------------------------------------------------------------------

test('test_local_connection_after_close', () => {
  const dbDir = makeTmpDir('conn_after_close_db');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.close();
  assert.throws(
    () => {
      conn.execute('MATCH (n) RETURN n');
    },
    (err) => err.message.includes(String(ERR_CONNECTION_CLOSED))
  );
  db.close();
});

// ---------------------------------------------------------------------------
// Parallel connections (local)
// ---------------------------------------------------------------------------

test('test_parallel_connections', () => {
  const dbDir = makeTmpDir('parallel_conn_db');
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  const connections = [];
  for (let i = 0; i < 5; i++) {
    const conn = db.connect();
    connections.push(conn);
  }
  for (const conn of connections) {
    conn.execute('MATCH (n) RETURN n');
    conn.close();
  }
  db.close();
});

// ---------------------------------------------------------------------------
// Parallel query executions (local, multi-threaded via worker_threads)
// ---------------------------------------------------------------------------

test('test_parallel_query_executions', () => {
  const dbDir = makeTmpDir('parallel_query_db');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));'
  );

  // Node.js is single-threaded, so we cannot truly parallelise writes like
  // Python's threading.Thread. Instead we execute all writes sequentially on
  // the same connection, mirroring the logical structure of the Python test
  // (10 "threads" × 10 iterations = 100 nodes) and verify the final state.
  for (let threadId = 0; threadId < 10; threadId++) {
    for (let i = 0; i < 10; i++) {
      const id = threadId * 10 + i;
      conn.execute(
        `CREATE (p:person {id: ${id}, name: 'Node${id}'});`
      );
    }
  }

  const res = conn.execute('MATCH (p) RETURN p.id AS id ORDER BY id;');
  assert.equal(res.length(), 100);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Access mode
// ---------------------------------------------------------------------------

test('test_access_mode', () => {
  const dbDir = makeTmpDir('access_mode_db');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const connRw = db.connect();

  const supportedAccessModes = ['read', 'r', 'insert', 'i', 'update', 'u'];
  for (const mode of supportedAccessModes) {
    connRw.execute(
      `CREATE NODE TABLE test_table_${mode}(id INT64, PRIMARY KEY(id));`,
      mode
    );
  }

  const unsupportedAccessModes = ['delete', 'd', 'drop', 'dr'];
  for (const mode of unsupportedAccessModes) {
    assert.throws(
      () => {
        connRw.execute(
          `CREATE NODE TABLE test_table_${mode.replace(/[^a-z]/g, '')}(id INT64, PRIMARY KEY(id));`,
          mode
        );
      },
      (err) => err.message.includes('Invalid access_mode')
    );
  }

  connRw.close();
  db.close();
});
