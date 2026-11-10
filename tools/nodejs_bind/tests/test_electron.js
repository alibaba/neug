/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const assert = require('assert').strict;
const path = require('path');
const { spawnSync } = require('child_process');

const successMarker = 'NEUG_ELECTRON_SMOKE_OK';

function smoke(packageDir) {
  assert.ok(process.versions.electron, 'Test must run inside Electron');
  console.log(`Electron ${process.versions.electron}, Node ${process.versions.node}`);
  console.log('Loading NeuG');
  const { Database } = require(packageDir);
  const db = new Database({ databasePath: '', mode: 'w' });
  let conn;
  try {
    conn = db.connect();
    const result = conn.execute('RETURN 1');
    try {
      assert.equal(Number(result.getNext()[0]), 1);
    } finally {
      result.close();
    }
  } finally {
    if (conn) conn.close();
    db.close();
  }
  console.log(successMarker);
}

if (process.argv[2] === '--child') {
  smoke(process.argv[3]);
} else if (process.argv[2] === '--help') {
  console.log(
    'Usage: node tests/test_electron.js [electron-executable] [package-directory]\n' +
      'Or set NEUG_ELECTRON_PATH for the default test suite.\n' +
      'Defaults to the local Node.js binding package. Electron must be installed separately.'
  );
} else {
  const { test } = require('./test-shim');
  const electronPath = process.argv[2] || process.env.NEUG_ELECTRON_PATH;
  const packageDir = process.argv[3]
    ? path.resolve(process.argv[3])
    : path.resolve(__dirname, '..');
  test(
    'electron_load_and_query',
    { skip: electronPath ? false : 'Set NEUG_ELECTRON_PATH to run the Electron smoke test.' },
    () => {
      const child = spawnSync(path.resolve(electronPath), [__filename, '--child', packageDir], {
        env: { ...process.env, ELECTRON_RUN_AS_NODE: '1' },
        encoding: 'utf8',
        timeout: 60000,
      });
      if (child.stdout) process.stdout.write(child.stdout);
      if (child.stderr) process.stderr.write(child.stderr);
      if (child.error) throw child.error;
      assert.equal(child.status, 0, `Electron exited with status ${child.status}, signal ${child.signal}`);
      assert.ok(child.stdout.includes(successMarker), 'Electron did not complete the smoke test');
    }
  );
}
