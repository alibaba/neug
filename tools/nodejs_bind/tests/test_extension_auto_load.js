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

const homes = [];
function makeTmpDir() {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'neug_auto_load_'));
  homes.push(home);
  return home;
}
after(() => {
  for (const home of homes) fs.rmSync(home, { recursive: true, force: true });
});

for (const autoLoadExt of [null, 0, 1, 'true', []]) {
  test(`autoLoadExt rejects ${JSON.stringify(autoLoadExt)}`, () => {
    assert.throws(() => new Database({ autoLoadExt }), TypeError);
    const native = require('../lib/binding');
    assert.throws(() => new native.NodeDatabase({ autoLoadExt }), /autoLoadExt must be a boolean/);
  });
}

for (const autoLoadExt of [undefined, false, true]) {
  test(`autoLoadExt opens with ${autoLoadExt}`, () => {
    const previous = process.env.NEUG_EXTENSION_HOME_PYENV;
    process.env.NEUG_EXTENSION_HOME_PYENV = makeTmpDir();
    let db;
    try {
      db = new Database({ mode: 'w', maxThreadNum: 1, autoLoadExt });
      const conn = db.connect();
      assert.deepEqual(Array.from(conn.execute('RETURN 1')), [[1n]]);
      conn.close();
    } finally {
      if (db) db.close();
      if (previous === undefined) delete process.env.NEUG_EXTENSION_HOME_PYENV;
      else process.env.NEUG_EXTENSION_HOME_PYENV = previous;
    }
  });
}

for (const autoLoadExt of [false, true]) {
  test(`autoLoadExt=${autoLoadExt} reaches the native loader`, () => {
    const home = makeTmpDir();
    const extension = path.join(home, 'extension', 'broken');
    fs.mkdirSync(extension, { recursive: true });
    fs.writeFileSync(path.join(extension, 'libbroken.neug_extension'), 'invalid library');
    const { spawnSync } = require('child_process');
    const result = spawnSync(process.execPath, ['-e', `
      const { Database } = require('./lib');
      const db = new Database({ autoLoadExt: ${autoLoadExt}, maxThreadNum: 1 });
      const conn = db.connect();
      require('assert').deepStrictEqual(Array.from(conn.execute('RETURN 1')), [[1n]]);
      conn.close();
      db.close();
    `], {
      cwd: path.resolve(__dirname, '..'),
      env: { ...process.env, NEUG_EXTENSION_HOME_PYENV: home },
      encoding: 'utf8',
      timeout: 30000,
    });
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.equal(result.stderr.includes('[AutoLoadExtension] stage=load extension=broken'), autoLoadExt);
  });
}
