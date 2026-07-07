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

const path = require('path');

/**
 * Minimal test shim compatible with Node.js 16 (no node:test built-in).
 * Provides test(), before(), and after() with basic TAP-style output.
 */

const _beforeHooks = [];
const _afterHooks = [];
const _tests = [];
const _failures = [];
let _running = false;

function before(fn) {
  _beforeHooks.push(fn);
}

function after(fn) {
  _afterHooks.push(fn);
}

function test(name, optsOrFn, maybeFn) {
  let fn;
  let opts = {};

  if (typeof optsOrFn === 'function') {
    fn = optsOrFn;
  } else {
    opts = optsOrFn || {};
    fn = maybeFn;
  }

  // Capture caller file from stack trace
  const stack = new Error().stack;
  const callerLine = stack.split('\n').find((line, i) => i > 0 && !line.includes('test-shim.js'));
  const fileMatch = callerLine && callerLine.match(/\((.+?):\d+:\d+\)|at (.+?):\d+:\d+/);
  const file = fileMatch ? (fileMatch[1] || fileMatch[2]) : '<unknown>';

  _tests.push({ name, fn, opts, file });

  // Auto-schedule the runner on next tick if not already scheduled
  if (!_running) {
    _running = true;
    process.nextTick(_run);
  }
}

function prepareModernGraphDataset(Database, dbDir = '/tmp/modern_graph') {
  const dataDir = process.env.FLEX_DATA_DIR;
  if (!dataDir) {
    throw new Error('FLEX_DATA_DIR is not set');
  }

  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));'
  );
  conn.execute(
    'CREATE NODE TABLE software(id INT64, name STRING, lang STRING, PRIMARY KEY(id));'
  );
  conn.execute('CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);');
  conn.execute(
    'CREATE REL TABLE created(FROM person TO software, weight DOUBLE, since INT64);'
  );

  conn.execute(`COPY person from "${path.join(dataDir, 'person.csv')}"`);
  conn.execute(`COPY software from "${path.join(dataDir, 'software.csv')}"`);
  conn.execute(
    `COPY knows from "${path.join(dataDir, 'person_knows_person.csv')}" (from="person", to="person")`
  );
  conn.execute(
    `COPY created from "${path.join(dataDir, 'person_created_software.csv')}" (from="person", to="software")`
  );

  conn.close();
  db.close();
}

async function _run() {
  // Run before hooks
  for (const hook of _beforeHooks) {
    try {
      await hook();
    } catch (err) {
      console.error('before hook failed:', err);
      process.exitCode = 1;
    }
  }

  let passed = 0;
  let failed = 0;
  let skipped = 0;

  for (const { name, fn, opts, file } of _tests) {
    if (opts.skip) {
      console.log(`# skip - ${name}: ${opts.skip}`);
      skipped++;
      continue;
    }

    try {
      await fn();
      console.log(`ok - ${name}`);
      passed++;
    } catch (err) {
      console.error(`not ok - ${name}`);
      console.error(`  ${err.stack || err.message || err}`);
      _failures.push({ name, file, error: err.stack || err.message || String(err) });
      failed++;
      process.exitCode = 1;
    }
  }

  // Run after hooks
  for (const hook of _afterHooks) {
    try {
      await hook();
    } catch (err) {
      console.error('after hook failed:', err);
      process.exitCode = 1;
    }
  }

  console.log('');
  console.log(`# tests ${passed + failed + skipped}`);
  console.log(`# pass  ${passed}`);
  console.log(`# fail  ${failed}`);
  console.log(`# skip  ${skipped}`);

  if (_failures.length > 0) {
    console.log('');
    console.log('# --- Failed Tests Summary ---');
    for (const { name, file, error } of _failures) {
      console.log(`# FAIL: ${name}`);
      console.log(`#   file:  ${file}`);
      const errMsg = error.split('\n')[0];
      console.log(`#   error: ${errMsg}`);
      console.log('#');
    }
  }
}

module.exports = { test, before, after, prepareModernGraphDataset };
