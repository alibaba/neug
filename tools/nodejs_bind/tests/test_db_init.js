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
const { Database } = require('../lib');
const {
  ERR_CONFIG_INVALID,
  ERR_CORRUPTION_DETECTED,
  ERR_DATABASE_LOCKED,
  ERR_DISK_SPACE_EXHAUSTED,
  ERR_INVALID_ARGUMENT,
  ERR_INVALID_PATH,
  ERR_PERMISSION,
  ERR_VERSION_MISMATCHED,
} = require('../lib/error-codes');

// ---------------------------------------------------------------------------
// Helpers (mirrors Python tmp_path fixture)
// ---------------------------------------------------------------------------

let _tmpCounter = 0;
const _tmpDirs = [];
function makeTmpDir(prefix = 'neug_init_test_') {
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


// DB-001-01 & DB-001-02
test('test_memory_mode_open_and_close', () => {
  const db = new Database({ databasePath: '', mode: 'r' });
  assert.ok(db);
  db.close();
  const db2 = new Database({ databasePath: '', mode: 'w' });
  assert.ok(db2);
  db2.close();
});

test('test_memory_mode_open_and_close_none', () => {
  assert.throws(() => {
    // In memory database should not be read-only
    new Database({ databasePath: null, mode: 'r' });
  }, (err) => {
    return err.message.includes(String(ERR_INVALID_ARGUMENT));
  });
  const db2 = new Database({ databasePath: null, mode: 'w' });
  assert.ok(db2);
  db2.close();
});


// DB-001-03
test('test_local_db_open_exists_and_close', () => {
  const dbDir = makeTmpDir('existdb');
  if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir);
  }
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  assert.ok(db);
  db.close();
  const db2 = new Database({ databasePath: dbDir, mode: 'rw' });
  assert.ok(db2);
  db2.close();
});

test('test_local_ldbc_open_and_close', () => {
  const dbDir = '/tmp/ldbc';
  const db = new Database({ databasePath: String(dbDir), mode: 'r' });
  assert.ok(db);
  db.close();
  const db2 = new Database({ databasePath: String(dbDir), mode: 'rw' });
  assert.ok(db2);
  db2.close();
});


// DB-001-04
test('test_local_db_open_not_exists_and_close', () => {
  const dbDir = makeTmpDir('not_existdb');
  if (fs.existsSync(dbDir)) {
    fs.rmSync(dbDir, { recursive: true, force: true });
  }
  assert.ok(!fs.existsSync(dbDir));
  const db = new Database({ databasePath: dbDir, mode: 'r' });
  assert.ok(db);
  db.close();
  if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir);
  }
  const db2 = new Database({ databasePath: dbDir, mode: 'w' });
  assert.ok(db2);
  db2.close();
});


// DB-001-05
test('test_local_db_close', () => {
  const dbDir = makeTmpDir('closedb');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  db.close();
});


// DB-001-06
test('test_readonly_mode_multi_instance', () => {
  const dbDir = makeTmpDir('multi_db');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  db.close();
  const db1 = new Database({ databasePath: dbDir, mode: 'r' });
  const db2 = new Database({ databasePath: dbDir, mode: 'r' });
  assert.ok(db1 && db2);
  db1.close();
  db2.close();
});


// DB-001-07
test('test_rw_mode_exclusive', () => {
  const dbDir = makeTmpDir('exclusive_db');
  const db1 = new Database({ databasePath: dbDir, mode: 'w' });
  try {
    assert.throws(() => {
      new Database({ databasePath: dbDir, mode: 'w' });
    }, (err) => {
      return err.message.includes(String(ERR_DATABASE_LOCKED));
    });
  } finally {
    db1.close();
  }
});


// DB-001-08
test('test_rw_ro_conflict', () => {
  const dbDir = makeTmpDir('conflict_db');
  const db1 = new Database({ databasePath: dbDir, mode: 'w' });
  try {
    assert.throws(() => {
      new Database({ databasePath: dbDir, mode: 'r' });
    }, (err) => {
      return err.message.includes(String(ERR_DATABASE_LOCKED));
    });
  } finally {
    db1.close();
  }
});


// DB-001-09
test('test_readonly_write_operation', () => {
  const dbDir = makeTmpDir('readonly_db');
  const dbRo = new Database({ databasePath: dbDir, mode: 'r' });
  assert.throws(() => {
    const conn = dbRo.connect();
    conn.execute('CREATE NODE TABLE person(id INT32, PRIMARY KEY(id));');
    conn.close();
  }, (err) => {
    return err.message.includes(String(ERR_INVALID_ARGUMENT));
  });
  dbRo.close();
});


// DB-001-10
test('test_invalid_path', () => {
  assert.throws(() => {
    new Database({ databasePath: '??/illegal', mode: 'r' });
  }, (err) => {
    return err.message.includes(String(ERR_INVALID_PATH));
  });
  // remove the invalid path after the test
  if (fs.existsSync('??/illegal')) {
    fs.rmSync('??', { recursive: true, force: true });
  }
});


// DB-001-11
test('test_config_param', () => {
  const dbDir = makeTmpDir('config_db');
  // mode: 'r', 'read', 'readwrite', 'w', 'rw', 'write'
  const db1 = new Database({ databasePath: dbDir, mode: 'r', maxThreadNum: 0 });
  assert.ok(db1);
  db1.close();
  const db2 = new Database({ databasePath: dbDir, mode: 'read', maxThreadNum: 0 });
  assert.ok(db2);
  db2.close();
  const db3 = new Database({ databasePath: dbDir, mode: 'readwrite', maxThreadNum: 0 });
  assert.ok(db3);
  db3.close();
  const db4 = new Database({ databasePath: dbDir, mode: 'w', maxThreadNum: 0 });
  assert.ok(db4);
  db4.close();
  const db5 = new Database({ databasePath: dbDir, mode: 'rw', maxThreadNum: 0 });
  assert.ok(db5);
  db5.close();
  const db6 = new Database({ databasePath: dbDir, mode: 'write', maxThreadNum: 0 });
  assert.ok(db6);
  db6.close();
  // maxThreadNum: 0 means auto-select from hardware concurrency
  const db7 = new Database({ databasePath: dbDir, mode: 'r', maxThreadNum: 0 });
  assert.ok(db7);
  db7.close();
  const maxThreadNum = Database.cpuCount() || 1;
  const db8 = new Database({ databasePath: dbDir, mode: 'r', maxThreadNum });
  assert.ok(db8);
  db8.close();
  const db9 = new Database({ databasePath: dbDir, mode: 'r', maxThreadNum: 0 });
  assert.ok(db9);
  db9.close();
});

test('test_config_param_exception', () => {
  const dbDir = makeTmpDir('config_db_exception');
  assert.throws(() => {
    new Database({ databasePath: dbDir, mode: 'rw', maxThreadNum: -1 });
  }, (err) => {
    return err.message.includes(String(ERR_CONFIG_INVALID));
  });
  assert.throws(() => {
    new Database({ databasePath: dbDir, mode: 'red' });
  }, (err) => {
    return err.message.includes(String(ERR_INVALID_ARGUMENT));
  });
  // Node.js silently ignores unknown options (no TypeError like Python),
  // so passing planner="gopt123" should succeed without error.
  const db3 = new Database({ databasePath: dbDir, mode: 'write', planner: 'gopt123' });
  assert.ok(db3);
  db3.close();
});

test('test_config_param_boundary', () => {
  const dbDir = makeTmpDir('conn_param_boundary_db');
  // test with more than maximum cores
  assert.throws(() => {
    const maxCores = Database.cpuCount() || 1;
    // maxThreadNum should not exceed the number of cores
    new Database({ databasePath: dbDir, mode: 'w', maxThreadNum: maxCores + 1 });
  }, (err) => {
    return err.message.includes(String(ERR_INVALID_ARGUMENT));
  });
});


// DB-001-12
test('test_open_no_permission', () => {
  const dbDir = makeTmpDir('no_permission_db');
  if (fs.existsSync(dbDir)) {
    fs.rmSync(dbDir, { recursive: true, force: true });
  }
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  db.close();
  fs.chmodSync(dbDir, 0o400);
  try {
    assert.throws(() => {
      new Database({ databasePath: dbDir, mode: 'w' });
    }, (err) => {
      return err.message.includes(String(ERR_PERMISSION));
    });
  } finally {
    fs.chmodSync(dbDir, 0o700);
  }
});


// DB-001-13
test('test_open_version_mismatch', { skip: 'https://github.com/GraphScope/neug/issues/788' }, () => {
  const dbDir = makeTmpDir('ver_db');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  db.close();

  // Simulate version mismatch by modifying the version metadata file
  // Assuming version metadata is stored in version.txt
  const versionFile = path.join(dbDir, 'version.txt');
  fs.writeFileSync(versionFile, 'mismatched_version');

  // Attempt to open the database
  assert.throws(() => {
    new Database({ databasePath: dbDir, mode: 'r' });
  }, (err) => {
    return err.message.includes(String(ERR_VERSION_MISMATCHED));
  });
});


// DB-001-14
test('test_open_dir_not_exist', () => {
  const dbDir = makeTmpDir('not_exist_dir');
  if (fs.existsSync(dbDir)) {
    fs.rmSync(dbDir, { recursive: true, force: true });
  }
  // mock the os.chmod to simulate no permission
  fs.mkdirSync(dbDir, { recursive: true });
  fs.chmodSync(dbDir, 0o400);
  try {
    assert.throws(() => {
      new Database({ databasePath: dbDir, mode: 'w' });
    }, (err) => {
      return err.message.includes(String(ERR_PERMISSION));
    });
  } finally {
    fs.chmodSync(dbDir, 0o700);
  }
});


test('test_open_with_multiple_process', { skip: 'https://github.com/alibaba/neug/issues/233' }, () => {
  const dbDir = makeTmpDir('multi_process_db');
  fs.rmSync(dbDir, { recursive: true, force: true });  // Ensure clean state
  fs.mkdirSync(dbDir);
  const db1 = new Database({ databasePath: String(dbDir), mode: 'r' });
  db1.close();
  const db2 = new Database({ databasePath: String(dbDir), mode: 'r' });
  db2.close();
});


// DB-001-15
test('test_disk_space_exhausted', { skip: 'Planned in stress test issues #524' }, () => {
  const dbDir = makeTmpDir('no_space_db');
  // NOTE: Cannot monkey-patch fs.open the same way Python's monkeypatch works.
  assert.throws(() => {
    new Database({ databasePath: dbDir, mode: 'w' });
  }, (err) => {
    return err.message.includes(String(ERR_DISK_SPACE_EXHAUSTED));
  });
});


// DB-001-16
test('test_file_header_corruption', { skip: 'https://github.com/GraphScope/neug/issues/794' }, () => {
  const dbDir = makeTmpDir('corrupt_db');
  new Database({ databasePath: dbDir, mode: 'w' });
  // db_file such as "wal/thread_0_0.wal" should exist after db creation
  const dbFile = path.join(dbDir, 'wal', 'thread_0_0.wal');
  assert.ok(fs.existsSync(dbFile), 'Database file should exist after creation');
  // simulate file corruption by writing a corrupt header
  fs.writeFileSync(dbFile, Buffer.from('corrupt-header'));
  try {
    new Database({ databasePath: dbDir, mode: 'w' });
  } catch (err) {
    assert.ok(
      err.message.includes(String(ERR_CORRUPTION_DETECTED)),
    );
    return;
  }
  assert.fail('Expected ERR_CORRUPTION_DETECTED but no exception was raised');
});


// DB-001-17
test('test_db_default_mode', () => {
  const dbDir = makeTmpDir('default_mode_db');
  const db = new Database({ databasePath: dbDir });
  assert.ok(db);
  assert.equal(db.mode, 'read-write');
  db.close();
});


// DB-001-18
test('test_memory_level_default', () => {
  /**
   * Verify that the default memory_level ('InMemory') is accepted and
   * the database opens successfully.
   */
  const dbDir = makeTmpDir('default_memory_level_db');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  assert.ok(db);
  db.close();
});


// DB-001-19
test('test_memory_level_in_memory', () => {
  /** Verify that all aliases for 'InMemory' memory level are accepted. */
  const dbDir = makeTmpDir('in_memory_level_db');
  // canonical form
  let db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'InMemory' });
  assert.ok(db);
  db.close();
  // lowercase alias
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'inmemory' });
  assert.ok(db);
  db.close();
  // underscore alias
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'in_memory' });
  assert.ok(db);
  db.close();
  // short literal
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'M_FULL' });
  assert.ok(db);
  db.close();
});


// DB-001-20
test('test_memory_level_sync_to_file', () => {
  /** Verify that all aliases for 'SyncToFile' memory level are accepted. */
  const dbDir = makeTmpDir('sync_to_file_level_db');
  // canonical form
  let db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'SyncToFile' });
  assert.ok(db);
  db.close();
  // lowercase alias
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'synctofile' });
  assert.ok(db);
  db.close();
  // underscore alias
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'sync_to_file' });
  assert.ok(db);
  db.close();
  // short literal
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'M_LAZY' });
  assert.ok(db);
  db.close();
});


// DB-001-21
test('test_memory_level_huge_page_preferred', () => {
  /** Verify that all aliases for 'HugePagePreferred' memory level are accepted. */
  const dbDir = makeTmpDir('huge_page_preferred_level_db');
  // canonical form
  let db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'HugePagePreferred' });
  assert.ok(db);
  db.close();
  // lowercase alias
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'hugepagepreferred' });
  assert.ok(db);
  db.close();
  // underscore alias
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'huge_page_preferred' });
  assert.ok(db);
  db.close();
  // short literal
  db = new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'M_HUGE' });
  assert.ok(db);
  db.close();
});


// DB-001-22
test('test_memory_level_invalid', () => {
  /** Verify that an invalid memory_level raises ERR_INVALID_ARGUMENT. */
  const dbDir = makeTmpDir('invalid_memory_level_db');
  assert.throws(() => {
    new Database({ databasePath: dbDir, mode: 'w', bufferStrategy: 'invalid_level' });
  }, (err) => {
    return err.message.includes(String(ERR_INVALID_ARGUMENT));
  });
});
