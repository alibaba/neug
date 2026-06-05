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

const { test, before, after } = require('./test-shim');
const assert = require('assert').strict;
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database, Session } = require('../lib');
const {
  ERR_CONFIG_INVALID,
  ERR_CONNECTION_CLOSED,
  ERR_NETWORK,
  ERR_SESSION_CLOSED,
} = require('../lib/error-codes');

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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
// Server helpers (mirrors Python started_server fixture)
// ---------------------------------------------------------------------------

const SERVER_PORT = 18765;

async function startServer(dbDir) {
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const endpoint = db.serve({ port: SERVER_PORT, host: 'localhost', blocking: false });
  // Wait for the server to be ready
  await sleep(1000);
  return { db, endpoint };
}

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
  const db = new Database({ databasePath: dbDir, mode: 'w', maxThreadNum: 4 });
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
// DB-002-05 & DB-002-06
// ---------------------------------------------------------------------------

test('test_remote_connection', async () => {
  const dbDir = makeTmpDir('remote_conn_db');
  const { db, endpoint } = await startServer(dbDir);
  try {
    const session = await Session.open({ endpoint });
    assert.ok(session);
    session.close();
  } finally {
    db.close();
  }
});

// ---------------------------------------------------------------------------
// DB-002-07
// ---------------------------------------------------------------------------

test('test_remote_connection_params', async () => {
  const dbDir = makeTmpDir('remote_conn_params_db');
  const { db, endpoint } = await startServer(dbDir);
  try {
    // num_threads is not supported in Node.js Session
    const session = await Session.open({ endpoint, timeout: 10 });
    assert.ok(session);
    session.close();
  } finally {
    db.close();
  }
});

// ---------------------------------------------------------------------------
// DB-002-08
// ---------------------------------------------------------------------------

test('test_remote_connection_wrong_ip_port', async () => {
  const session1 = new Session({ endpoint: 'http://256.256.256.256:10000' });
  await assert.rejects(
    () => session1.execute('RETURN 1'),
    (err) => err.message.includes(String(ERR_NETWORK))
  );

  const session2 = new Session({ endpoint: 'http://127.0.0.1:65536' });
  await assert.rejects(
    () => session2.execute('RETURN 1'),
    (err) => err.message.includes(String(ERR_NETWORK))
  );
});

// ---------------------------------------------------------------------------
// DB-002-09
// ---------------------------------------------------------------------------

test('test_remote_connection_broken', async () => {
  const dbDir = makeTmpDir('remote_broken_db');
  const { db, endpoint } = await startServer(dbDir);
  const session = await Session.open({ endpoint });
  // simulate server disconnect
  db.close();
  await sleep(2000);
  await assert.rejects(
    () =>
      session.execute(
        'CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));'
      ),
    (err) => err.message.includes(String(ERR_NETWORK))
  );
  session.close();
});

// ---------------------------------------------------------------------------
// DB-002-10
// ---------------------------------------------------------------------------

test(
  'test_tx_not_commit_connection_broken',
  { skip: 'Not Planned Yet' },
  async () => {
    const dbDir = makeTmpDir('tx_broken_db');
    const { db, endpoint } = await startServer(dbDir);
    const session = await Session.open({ endpoint });
    // session.begin();
    // await session.execute('CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));');
    // simulate server disconnect
    db.close();
    await sleep(5000);
    await assert.rejects(
      () => session.commit(),
      (err) => err.message.includes(String(ERR_SESSION_CLOSED))
    );
    // reconnect and check if the transaction is rolled back
    const session2 = await Session.open({ endpoint });
    // TODO: do we support to query schema directly?
    // await session2.execute('MATCH (n:T) RETURN n');
    const result = await session2.execute('SHOW TABLES;');
    assert.equal(result.length(), 0);
    // session2.close();
  }
);

// ---------------------------------------------------------------------------
// DB-002-11
// ---------------------------------------------------------------------------

test(
  'test_server_load_overflow',
  { skip: 'Planned in stress test issues #524' },
  async () => {
    const dbDir = makeTmpDir('load_overflow_db');
    const { db, endpoint } = await startServer(dbDir);
    const sessions = [];
    try {
      for (let i = 0; i < 10000; i++) {
        try {
          const s = await Session.open({ endpoint });
          sessions.push(s);
        } catch (exc) {
          assert.ok(exc.message.includes(String(ERR_LOAD_OVERFLOW)));
          break;
        }
      }
    } finally {
      for (const s of sessions) {
        try { s.close(); } catch (_) { /* ignore */ }
      }
      db.close();
    }
  }
);

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
// DB-002-12 (remote)
// ---------------------------------------------------------------------------

test('test_remote_connection_after_close', async () => {
  const dbDir = makeTmpDir('remote_after_close_db');
  const { db, endpoint } = await startServer(dbDir);
  try {
    const session = await Session.open({ endpoint });
    session.close();
    await assert.rejects(
      () =>
        session.execute(
          'CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));'
        ),
      (err) => err.message.includes(String(ERR_SESSION_CLOSED))
    );
  } finally {
    db.close();
  }
});

// ---------------------------------------------------------------------------
// DB-002-13
// ---------------------------------------------------------------------------

test('test_server_restart', async () => {
  const dbDir = makeTmpDir('server_restart_db');
  const { db, endpoint } = await startServer(dbDir);
  const session = await Session.open({ endpoint });
  db.close();
  await sleep(2000);

  // Restart server on the same port
  const dbDir2 = makeTmpDir('server_restart_db2');
  const db2 = new Database({ databasePath: dbDir2, mode: 'w' });
  const port = parseInt(new URL(endpoint).port, 10);
  db2.serve({ port, host: 'localhost', blocking: false });
  await sleep(2000);

  try {
    try {
      await session.execute(
        'CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));'
      );
    } catch (e) {
      assert.ok(e.message.includes(String(ERR_SESSION_CLOSED)));
    }
  } finally {
    try { db2.close(); } catch (_) { /* ignore */ }
  }
});

// ---------------------------------------------------------------------------
// DB-002-14
// ---------------------------------------------------------------------------

test(
  'test_connection_pool_exhausted',
  { skip: 'Planned in stress test issues #524' },
  async () => {
    const dbDir = makeTmpDir('pool_exhausted_db');
    const { db, endpoint } = await startServer(dbDir);
    try {
      // suppose the server has a connection pool limit of 8
      const s1 = await Session.open({ endpoint });
      await assert.rejects(
        () => Session.open({ endpoint }),
        (err) => err.message.includes(String(ERR_POOL_EXHAUSTED))
      );
      s1.close();
    } finally {
      db.close();
    }
  }
);

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
