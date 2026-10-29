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
const fs = require('fs');
const path = require('path');
const { test } = require('./test-shim');

const enabled = /^(1|true|yes|on)$/i.test(
  process.env.NEUG_RUN_EXTENSION_INSTALL_TESTS || ''
);

// Extensions that release-extension.yml publishes to OSS for Windows
// (vector_search is excluded until the ZVec MSVC port lands). INSTALL pulls
// these prebuilt artifacts -- built WITH_MIMALLOC=ON independently of the core
// under test -- which is what makes the subsequent LOAD a faithful
// cross-pipeline ABI check. Building the extensions in the same tree as the
// core would NOT reproduce #1160: they would inherit the core's WITH_MIMALLOC
// and always agree, so LOAD would pass even with the bug present.
const extensions = ['parquet', 'httpfs', 'pattern_matching', 'fts', 'gds'];

test(
  'install_and_load_prebuilt_extensions',
  {
    skip: enabled
      ? false
      : 'Set NEUG_RUN_EXTENSION_INSTALL_TESTS=1 to run the INSTALL+LOAD smoke.',
  },
  () => {
    const { Database } = require('neug');
    // lib/binding.js points NEUG_EXTENSION_HOME_PYENV at the native build dir
    // at require time. Read it back so we can prove INSTALL relocated there
    // (root cause a) instead of silently falling back to /tmp.
    const nativeExtHome = process.env.NEUG_EXTENSION_HOME_PYENV;
    const db = new Database({ databasePath: '', mode: 'w' });
    let conn;
    try {
      conn = db.connect();
      for (const name of extensions) {
        // INSTALL downloads the OSS-prebuilt extension into the runtime
        // extension home. On Windows this requires the native side to observe
        // binding.js's process.env write via GetEnvironmentVariableW.
        conn.execute(`INSTALL ${name}`).close();

        if (nativeExtHome) {
          const dir = path.join(nativeExtHome, 'extension', name);
          assert.ok(
            fs.existsSync(dir),
            `INSTALL ${name} did not land under the native extension home ` +
              `(${dir}); it likely fell back to /tmp -- see #1160 root cause (a).`
          );
        }

        // LOAD links the prebuilt (mimalloc ON) extension against the freshly
        // built core. A WITH_MIMALLOC ON/OFF mismatch throws here with Windows
        // error 127 (ERROR_PROC_NOT_FOUND) -- see #1160 root cause (b).
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
  }
);
