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

const assert = require('assert').strict;
const { once } = require('node:events');
const {
  Worker, isMainThread, parentPort, workerData,
} = require('node:worker_threads');

function queryOne(conn) {
  const result = conn.execute('RETURN 1 AS value;');
  try {
    return result.getNext()[0];
  } finally {
    result.close();
  }
}

function runWorker() {
  const { Database } = require('neug');
  if (workerData.action === 'load') {
    parentPort.postMessage({ loaded: true });
    parentPort.close();
    return;
  }

  let db = workerData.action === 'own'
    ? new Database({ databasePath: '', mode: 'r' })
    : Database.attach(workerData.handle);
  let conn = workerData.connect === false ? null : db.connect();

  parentPort.on('message', ({ command, count = 1 }) => {
    try {
      switch (command) {
        case 'connect':
          conn = db.connect();
          parentPort.postMessage({ connected: true });
          break;
        case 'query': {
          let value;
          for (let i = 0; i < count; i++) {
            value = queryOne(conn);
          }
          parentPort.postMessage({ value });
          break;
        }
        case 'close':
          db.close();
          db = null;
          conn = null;
          parentPort.postMessage({ closed: true });
          break;
        case 'exit':
          // Leave native wrappers open to exercise environment teardown.
          parentPort.close();
          break;
        default:
          throw new Error(`Unknown worker command: ${command}`);
      }
    } catch (error) {
      parentPort.postMessage({ error: error.stack || error.message });
    }
  });

  parentPort.postMessage({
    handle: workerData.action === 'own' ? db.share() : workerData.handle,
    mode: db.mode,
  });
}

function waitForMessage(worker) {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timer);
      worker.removeListener('message', onMessage);
      worker.removeListener('error', onError);
      worker.removeListener('exit', onExit);
    };
    const onMessage = (message) => {
      cleanup();
      if (message.error) {
        reject(new Error(message.error));
      } else {
        resolve(message);
      }
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const onExit = (code) => {
      cleanup();
      reject(new Error(`Worker exited before replying (code ${code})`));
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error('Worker did not reply within 30 seconds'));
    }, 30000);
    worker.once('message', onMessage);
    worker.once('error', onError);
    worker.once('exit', onExit);
  });
}

function spawnWorker(data) {
  const worker = new Worker(__filename, { workerData: data });
  return { worker, ready: waitForMessage(worker) };
}

function request(worker, command, options = {}) {
  const response = waitForMessage(worker);
  worker.postMessage({ command, ...options });
  return response;
}

async function exitWorker(worker) {
  const exited = once(worker, 'exit', { signal: AbortSignal.timeout(30000) });
  worker.postMessage({ command: 'exit' });
  const [code] = await exited;
  assert.equal(code, 0);
}

if (!isMainThread) {
  runWorker();
} else {
  const { test } = require('./test-shim');

  // The test shim runs this file in a fresh process. Keep its first NeuG load
  // inside a worker, then use all constructors in main after that worker exits.
  test('test_worker_load_before_main_and_worker_exit', async () => {
    const { worker, ready } = spawnWorker({ action: 'load' });
    const exited = once(worker, 'exit', { signal: AbortSignal.timeout(30000) });
    try {
      assert.deepEqual(await ready, { loaded: true });
      const [code] = await exited;
      assert.equal(code, 0);
      const { Database } = require('neug');
      const db = new Database({ databasePath: '', mode: 'r' });
      try {
        const conn = db.connect();
        assert.equal(queryOne(conn), 1n);
        conn.close();
      } finally {
        db.close();
      }
    } finally {
      await worker.terminate();
    }
  });

  test('test_shared_database_concurrent_workers_and_local_close', async () => {
    const { Database } = require('neug');
    const db = new Database({ databasePath: '', mode: 'r' });
    const workers = [];
    try {
      const handle = db.share();
      assert.equal(typeof handle, 'string');
      for (let i = 0; i < 3; i++) {
        workers.push(spawnWorker({ handle }));
      }
      const ready = await Promise.all(workers.map((entry) => entry.ready));
      for (const info of ready) {
        assert.equal(info.mode, db.mode);
      }

      // All workers hold a separate Connection to the same already-open DB.
      const replies = await Promise.all(workers.map(({ worker }) =>
        request(worker, 'query', { count: 20 })));
      assert.deepEqual(replies.map((reply) => reply.value), [1n, 1n, 1n]);

      await request(workers[0].worker, 'close');
      await exitWorker(workers[0].worker);
      await exitWorker(workers[1].worker);
      await workers[2].worker.terminate();

      // Explicit detach, natural exit, and forced termination must each leave
      // the owner's DB and constructor references usable.
      const conn = db.connect();
      assert.equal(queryOne(conn), 1n);
      conn.close();
      const attached = Database.attach(handle);
      attached.close();
    } finally {
      await Promise.all(workers.map(({ worker }) => worker.terminate()));
      db.close();
    }
  });

  test('test_shared_database_owner_close_invalidates_handles_and_connections', async () => {
    const { Database } = require('neug');
    const db = new Database({ databasePath: '', mode: 'r' });
    const handle = db.share();
    const { worker, ready } = spawnWorker({ handle });
    try {
      await ready;
      db.close();
      assert.throws(() => Database.attach(handle));
      assert.throws(() => db.share());
      await assert.rejects(request(worker, 'query'));
      await assert.rejects(request(worker, 'connect'));
      await request(worker, 'close');
      await exitWorker(worker);
    } finally {
      await worker.terminate();
      db.close();
    }
  });

  test('test_shared_database_handle_validation', () => {
    const { Database } = require('neug');
    for (const handle of [undefined, null, 1, {}, [], '', 'unknown-handle']) {
      assert.throws(() => Database.attach(handle));
    }
  });

  test('test_shared_database_preserves_single_read_write_connection_limit', async () => {
    const { Database } = require('neug');
    const db = new Database({ databasePath: '', mode: 'w' });
    let conn = db.connect();
    const { worker, ready } = spawnWorker({
      handle: db.share(), connect: false,
    });
    try {
      await ready;
      // Reopening another in-memory DB would incorrectly allow this connect.
      await assert.rejects(request(worker, 'connect'));
      conn.close();
      await request(worker, 'connect');
      assert.equal((await request(worker, 'query')).value, 1n);
      assert.throws(() => db.connect());
      // Environment teardown must unregister the unique writable connection,
      // even without an explicit close on the attachment or connection.
      await exitWorker(worker);
      conn = db.connect();
      assert.equal(queryOne(conn), 1n);
      conn.close();
    } finally {
      await worker.terminate();
      db.close();
    }
  });

  test('test_shared_database_survives_owner_environment_exit', async () => {
    const { Database } = require('neug');
    const { worker, ready } = spawnWorker({ action: 'own' });
    let attached;
    try {
      const { handle } = await ready;
      attached = Database.attach(handle);
      const conn = attached.connect();
      await exitWorker(worker);
      assert.equal(queryOne(conn), 1n);
      conn.close();
      attached.close();
      assert.throws(() => Database.attach(handle));
    } finally {
      await worker.terminate();
      if (attached) {
        attached.close();
      }
    }
  });
}
