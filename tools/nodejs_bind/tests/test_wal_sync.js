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

const { test } = require('./test-shim');
const assert = require('assert').strict;
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Database } = require('neug');
const { NodeDatabase } = require('../lib/binding');

for (const mode of ['STANDARD', 'standard', 'sTaNdArD', 'FULL', 'full', 'fUlL']) {
  test(`wal_sync_recovery_${mode}`, () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'neug_wal_sync_'));
    const options = { databasePath: dir, mode: 'w', walSyncMode: mode, checkpointOnClose: false };
    let db;
    let conn;
    try {
      db = new Database(options);
      assert.equal(db._database.getOptions().walSyncMode, mode.toUpperCase());
      const attached = Database.attach(db.share());
      assert.equal(attached._database.getOptions().walSyncMode, mode.toUpperCase());
      attached.close();
      conn = db.connect();
      conn.execute('CREATE NODE TABLE item(id INT64, PRIMARY KEY(id))');
      conn.execute('CREATE (:item {id: 1})');
      conn.execute('CHECKPOINT');
      conn.execute('CREATE (:item {id: 2})');
      conn.close();
      db.close();
      db = new Database(options);
      conn = db.connect();
      assert.deepEqual([...conn.execute('MATCH (n:item) RETURN n.id ORDER BY n.id')], [[1n], [2n]]);
    } finally {
      if (conn) conn.close();
      if (db) db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
}

test('wal_sync_defaults_and_native_parsing', () => {
  for (const mode of [undefined, 'standard', 'sTaNdArD', 'full', 'fUlL']) {
    const options = { databasePath: '', mode: 'w' };
    if (mode !== undefined) options.walSyncMode = mode;
    const db = new NodeDatabase(options);
    try {
      assert.equal(db.getOptions().walSyncMode, (mode || 'STANDARD').toUpperCase());
    } finally {
      db.close();
    }
  }
});

test('wal_sync_rejects_invalid_values_and_types', () => {
  for (const Constructor of [Database, NodeDatabase]) {
    for (const mode of ['', 'NONE', 'unknown', ' full', 'full ', 'ſtandard']) {
      assert.throws(() => new Constructor({ databasePath: '', mode: 'w', walSyncMode: mode }),
        /walSyncMode must be STANDARD or FULL|wal_sync_mode must be STANDARD or FULL/);
    }
    for (const mode of [null, 42, true]) {
      assert.throws(() => new Constructor({ databasePath: '', mode: 'w', walSyncMode: mode }),
        /walSyncMode must be a string/);
    }
  }
});
