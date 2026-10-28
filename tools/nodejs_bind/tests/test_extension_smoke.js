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
const { test } = require('./test-shim');

const enabled = /^(1|true|yes|on)$/i.test(process.env.NEUG_RUN_EXTENSION_TESTS || '');
const extensions = ['parquet', 'httpfs', 'pattern_matching', 'vector_search', 'fts', 'gds'];

test('load_all_extensions', {
  skip: enabled ? false : 'Set NEUG_RUN_EXTENSION_TESTS=1 to run extension tests.',
}, () => {
  const { Database } = require('neug');
  const db = new Database({ databasePath: '', mode: 'w' });
  let conn;
  try {
    conn = db.connect();
    for (const name of extensions) {
      conn.execute(`LOAD ${name}`).close();
    }
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
});
