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
  ERR_IO_ERROR,
  ERR_PERMISSION,
  ERR_SCHEMA_MISMATCH,
} = require('../lib/error-codes');

// ---------------------------------------------------------------------------
// Helpers (mirrors Python tmp_path fixture)
// ---------------------------------------------------------------------------

let _tmpCounter = 0;
const _tmpDirs = [];
function makeTmpDir(prefix = 'neug_ie_test_') {
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

/**
 * Return <repo>/example_dataset path (tests/ -> nodejs_bind/ -> tools/ -> repo root).
 */
function neugRepoRoot() {
  return path.resolve(__dirname, '..', '..', '..');
}

const EXTENSION_TESTS_ENABLED = ['1', 'true', 'yes', 'on'].includes(
  (process.env.NEUG_RUN_EXTENSION_TESTS || '').toLowerCase()
);

// ---------------------------------------------------------------------------
// DB-005-01
// ---------------------------------------------------------------------------

test('test_import_default', () => {
  const dbDir = makeTmpDir('import_default');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'person.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|Alice\n2|Bob\n');
  conn.execute(`COPY person FROM "${csvPath}";`);
  const res = conn.execute('MATCH (n:person) RETURN n;');
  assert.equal(res.length(), 2);
  conn.close();
  db.close();
});

test('test_import_config', () => {
  const dbDir = makeTmpDir('import_config');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'person.csv');
  fs.writeFileSync(csvPath, '1,Alice\n2,Bob\n3,Charlie\n');
  conn.execute(`COPY person FROM "${csvPath}" (HEADER FALSE, DELIMITER=",");`);
  const res = conn.execute('MATCH (n:person) RETURN n;');
  assert.equal(res.length(), 3);
  conn.close();
  db.close();
});

test('test_double_quote', () => {
  const dbDir = makeTmpDir('double_quote');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'person.csv');
  fs.writeFileSync(csvPath, '"1","["Alice"]"\n"2","["Bob"]"\n"3","["Charlie"]"\n');
  conn.execute(
    `COPY person FROM "${csvPath}" (HEADER FALSE, DELIMITER=",", DOUBLE_QUOTE=true);`
  );
  const res = conn.execute('MATCH (n:person) RETURN n;');
  assert.equal(res.length(), 3);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-02
// ---------------------------------------------------------------------------

test('test_import_bad_csv', () => {
  const dbDir = makeTmpDir('bad_csv');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'bad.csv');
  fs.writeFileSync(csvPath, Buffer.from('id\n1\n\xff\n2\n', 'binary'));
  assert.throws(
    () => {
      conn.execute(`COPY person FROM "${csvPath}";`);
    },
    (err) => err.message.includes(String(ERR_IO_ERROR))
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-03
// ---------------------------------------------------------------------------

test('test_import_null', () => {
  const dbDir = makeTmpDir('import_null');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'null.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|NULL\n2|NaN\n');
  conn.execute(`COPY person FROM "${csvPath}";`);
  const res = conn.execute('MATCH (n:person) RETURN n');
  assert.equal(res.length(), 2);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-04
// ---------------------------------------------------------------------------

test('test_import_type_conversion1', () => {
  const dbDir = makeTmpDir('import_type_conversion1');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'type.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|111\n2|222\n');
  conn.execute(`COPY person FROM "${csvPath}"`);
  conn.close();
  db.close();
});

test('test_import_type_conversion2', () => {
  const dbDir = makeTmpDir('import_type_conversion2');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person2(id INT64, age INT32, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'type2.csv');
  fs.writeFileSync(csvPath, 'id|age\n1|30\n2|40\n');
  conn.execute(`COPY person2 FROM "${csvPath}";`);
  conn.close();
  db.close();
});

test('test_import_type_conversion_overflow', () => {
  const dbDir = makeTmpDir('import_type_conversion_overflow');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'type2.csv');
  fs.writeFileSync(csvPath, 'id\n12345678901234567890\n'); // INT64 overflow
  assert.throws(
    () => {
      conn.execute(`COPY person FROM "${csvPath}";`);
    },
    (err) => err.message.includes(String(ERR_IO_ERROR))
  );
  conn.close();
  db.close();
});

test('test_import_string_pk', () => {
  const dbDir = makeTmpDir('import_type');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'type.csv');
  fs.writeFileSync(csvPath, 'id\nAlice\n');
  conn.execute(`COPY person FROM "${csvPath}"`);
  conn.close();
  db.close();
});

test('test_import_int32_pk', () => {
  const dbDir = makeTmpDir('import_primary_key');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT32, name STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'person.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|Alice\n2|Bob\n');
  conn.execute(`COPY person FROM "${csvPath}";`);
  const res = conn.execute('MATCH (n:person) RETURN n;');
  assert.equal(res.length(), 2);
  conn.close();
  db.close();
});

test('test_import_uint32_pk', () => {
  const dbDir = makeTmpDir('import_uint32_pk');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id UINT32, name STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'person.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|Alice\n2|Bob\n');
  conn.execute(`COPY person FROM "${csvPath}";`);
  const res = conn.execute('MATCH (n:person) RETURN n;');
  assert.equal(res.length(), 2);
  conn.close();
  db.close();
});

test('test_import_uint64_pk', () => {
  const dbDir = makeTmpDir('import_uint64_pk');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id UINT64, name STRING, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'person.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|Alice\n2|Bob\n');
  conn.execute(`COPY person FROM "${csvPath}";`);
  const res = conn.execute('MATCH (n:person) RETURN n;');
  assert.equal(res.length(), 2);
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-05
// ---------------------------------------------------------------------------

test('test_export_config', () => {
  const dbDir = makeTmpDir('export_config');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));');
  conn.execute('CREATE (u:person {id: 1}), (u2:person {id: 2});');
  const outPath = path.join(dbDir, 'out.csv');
  conn.execute(
    `COPY (MATCH (p:person) RETURN *) TO "${outPath}" (DELIMITER=",", HEADER=TRUE)`
  );
  assert.ok(fs.existsSync(outPath));
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-07
// ---------------------------------------------------------------------------

test('test_import_file_not_found', () => {
  const dbDir = makeTmpDir('import_file_not_found');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));');
  assert.throws(
    () => {
      conn.execute('COPY person FROM "/not/exist.csv";');
    },
    (err) => err.message.includes(String(ERR_IO_ERROR))
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-08
// ---------------------------------------------------------------------------

test('test_export_no_permission', () => {
  const dbDir = makeTmpDir('export_no_permission');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));');
  const outDir = path.join(dbDir, 'no_perm');
  fs.mkdirSync(outDir);
  fs.chmodSync(outDir, 0o400);
  const outPath = path.join(outDir, 'out.csv');
  try {
    assert.throws(
      () => {
        conn.execute(`COPY (MATCH (v:person) RETURN v) to "${outPath}";`);
      },
      (err) =>
        err.message.includes(String(ERR_PERMISSION)) ||
        err.message.includes(String(ERR_IO_ERROR))
    );
  } finally {
    fs.chmodSync(outDir, 0o700);
  }
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-09
// ---------------------------------------------------------------------------

test('test_import_schema_mismatch', () => {
  const dbDir = makeTmpDir('import_schema_mismatch');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'mismatch.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|Alice\n');
  assert.throws(
    () => {
      conn.execute(`COPY person FROM "${csvPath}";`);
    },
    (err) => err.message.includes(String(ERR_SCHEMA_MISMATCH))
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-10
// ---------------------------------------------------------------------------

test('test_import_bad_encoding', () => {
  const dbDir = makeTmpDir('import_bad_encoding');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));');
  const csvPath = path.join(dbDir, 'badenc.csv');
  fs.writeFileSync(csvPath, Buffer.from('id\n1\n\xff\n', 'binary'));
  assert.throws(
    () => {
      conn.execute(`COPY person FROM "${csvPath}";`);
    },
    (err) => err.message.includes(String(ERR_IO_ERROR))
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// DB-005-11
// ---------------------------------------------------------------------------

test('test_export_vertex_edge', () => {
  const dbDir = makeTmpDir('syntax_error');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  assert.throws(
    () => {
      conn.execute("COPY (MATCH (v:person) RETURN v) to 'person.csv';");
    },
    (err) => err.message.includes(String(ERR_SCHEMA_MISMATCH))
  );
  assert.throws(
    () => {
      conn.execute(
        "COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) to 'person_knows_person.csv' (HEADER = true);"
      );
    },
    (err) => err.message.includes(String(ERR_SCHEMA_MISMATCH))
  );
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// COPY (LOAD FROM ...) TO — read external file and export to another file
// ---------------------------------------------------------------------------

test('test_copy_load_from_to_csv', () => {
  const dbDir = makeTmpDir('copy_load_from_to');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const inputCsv = path.join(dbDir, 'input.csv');
  fs.writeFileSync(inputCsv, 'id|name|age\n1|Alice|30\n2|Bob|25\n3|Charlie|28\n');

  const outputCsv = path.join(dbDir, 'output.csv');
  conn.execute(
    `COPY (LOAD FROM "${inputCsv}" RETURN id, name, age) ` +
    `TO "${outputCsv}" (HEADER=TRUE, DELIMITER=",");`
  );

  assert.ok(fs.existsSync(outputCsv));
  const lines = fs.readFileSync(outputCsv, 'utf-8').trim().split('\n');
  assert.equal(lines.length, 4); // 1 header + 3 data rows
  assert.ok(lines[0].includes('id') && lines[0].includes('name') && lines[0].includes('age'));
  const content = fs.readFileSync(outputCsv, 'utf-8');
  assert.ok(content.includes('Alice'));
  assert.ok(content.includes('Bob'));

  conn.close();
  db.close();
});

test('test_copy_load_from_to_with_filter', () => {
  const dbDir = makeTmpDir('copy_load_from_to_filter');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const inputCsv = path.join(dbDir, 'people.csv');
  fs.writeFileSync(inputCsv, 'id|name|age\n1|Alice|30\n2|Bob|17\n3|Charlie|28\n4|Dave|15\n');

  const outputCsv = path.join(dbDir, 'adults.csv');
  conn.execute(
    `COPY (LOAD FROM "${inputCsv}" WHERE age >= 18 RETURN id, name, age) ` +
    `TO "${outputCsv}" (HEADER=TRUE);`
  );

  assert.ok(fs.existsSync(outputCsv));
  const lines = fs.readFileSync(outputCsv, 'utf-8').trim().split('\n');
  assert.equal(lines.length, 3); // header + 2 adult rows
  const content = fs.readFileSync(outputCsv, 'utf-8');
  assert.ok(content.includes('Alice'));
  assert.ok(content.includes('Charlie'));
  assert.ok(!content.includes('Bob'));
  assert.ok(!content.includes('Dave'));

  conn.close();
  db.close();
});

test('test_copy_load_from_to_with_column_reorder', () => {
  const dbDir = makeTmpDir('copy_load_from_to_reorder');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const inputCsv = path.join(dbDir, 'source.csv');
  fs.writeFileSync(inputCsv, 'name|id|score\nAlice|1|95.5\nBob|2|88.0\n');

  const outputCsv = path.join(dbDir, 'reordered.csv');
  conn.execute(
    `COPY (LOAD FROM "${inputCsv}" RETURN id, name, score) ` +
    `TO "${outputCsv}" (HEADER=TRUE, DELIMITER="|");`
  );

  assert.ok(fs.existsSync(outputCsv));
  const lines = fs.readFileSync(outputCsv, 'utf-8').trim().split('\n');
  assert.equal(lines.length, 3); // header + 2 rows
  const headerCols = lines[0].split('|');
  assert.equal(headerCols[0].trim(), 'id');
  assert.equal(headerCols[1].trim(), 'name');
  assert.equal(headerCols[2].trim(), 'score');

  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Module 5: Copy From With No Schema (auto-detect / schema-less COPY FROM)
// ---------------------------------------------------------------------------

test('test_copy_from_no_schema_node_basic', () => {
  const dbDir = makeTmpDir('no_schema_node_basic');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'people.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|Alice\n2|Bob\n3|Charlie\n');

  conn.execute(`COPY ns_person FROM "${csvPath}";`);

  const res = [...conn.execute('MATCH (n:ns_person) RETURN n.id, n.name ORDER BY n.id;')];
  assert.equal(res.length, 3);
  assert.equal(res[0][0], 1);
  assert.equal(res[0][1], 'Alice');
  assert.equal(res[2][0], 3);
  assert.equal(res[2][1], 'Charlie');

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_node_subquery', () => {
  const dbDir = makeTmpDir('no_schema_node_subquery');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'users.csv');
  fs.writeFileSync(csvPath, 'name|user_id\nAlice|1\nBob|2\nCharlie|3\n');

  conn.execute(`COPY ns_user FROM (LOAD FROM "${csvPath}" RETURN user_id, name);`);

  const res = [...conn.execute('MATCH (u:ns_user) RETURN u.user_id, u.name ORDER BY u.user_id;')];
  assert.equal(res.length, 3);
  assert.equal(res[0][0], 1);
  assert.equal(res[0][1], 'Alice');

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_edge_from_file', () => {
  const dbDir = makeTmpDir('no_schema_edge_file');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const personCsv = path.join(dbDir, 'person.csv');
  fs.writeFileSync(personCsv, 'id|name\n1|Alice\n2|Bob\n3|Charlie\n');
  conn.execute('CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));');
  conn.execute(`COPY person FROM "${personCsv}";`);

  const edgeCsv = path.join(dbDir, 'knows.csv');
  fs.writeFileSync(edgeCsv, 'from|to|weight\n1|2|0.5\n2|3|0.8\n');

  conn.execute(`COPY ns_knows FROM "${edgeCsv}" (from="person", to="person");`);

  const res = [...conn.execute(
    'MATCH (a:person)-[k:ns_knows]->(b:person) ' +
    'RETURN a.id, b.id, k.weight ORDER BY a.id;'
  )];
  assert.equal(res.length, 2);
  assert.equal(res[0][0], 1);
  assert.equal(res[0][1], 2);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_edge_subquery', () => {
  const dbDir = makeTmpDir('no_schema_edge_subquery');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const personCsv = path.join(dbDir, 'person.csv');
  fs.writeFileSync(personCsv, 'id|name\n1|Alice\n2|Bob\n3|Charlie\n');
  conn.execute('CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));');
  conn.execute(`COPY person FROM "${personCsv}";`);

  const edgeCsv = path.join(dbDir, 'follows.csv');
  fs.writeFileSync(edgeCsv, 'since|dst|src\n2020|2|1\n2021|3|2\n');

  conn.execute(
    `COPY ns_follows FROM (LOAD FROM "${edgeCsv}" RETURN src, dst, since) ` +
    `(from="person", to="person");`
  );

  const res = [...conn.execute(
    'MATCH (a:person)-[f:ns_follows]->(b:person) ' +
    'RETURN a.id, b.id, f.since ORDER BY a.id;'
  )];
  assert.equal(res.length, 2);
  assert.equal(res[0][0], 1);
  assert.equal(res[0][1], 2);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_vertex_then_edge_no_ddl', () => {
  const dbDir = makeTmpDir('no_schema_v_then_e');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const vCsv = path.join(dbDir, 'ns_vertices.csv');
  fs.writeFileSync(vCsv, 'id|name\n1|Alice\n2|Bob\n3|Charlie\n');
  conn.execute(`COPY ns5_person FROM "${vCsv}";`);

  const eCsv = path.join(dbDir, 'ns_edges.csv');
  fs.writeFileSync(eCsv, 'from|to|weight\n1|2|0.5\n2|3|0.8\n');
  conn.execute(`COPY ns5_knows FROM "${eCsv}" (from="ns5_person", to="ns5_person");`);

  const vrows = [...conn.execute('MATCH (n:ns5_person) RETURN n.id ORDER BY n.id;')];
  assert.equal(vrows.length, 3);
  const erows = [...conn.execute(
    'MATCH (a:ns5_person)-[k:ns5_knows]->(b:ns5_person) ' +
    'RETURN a.id, b.id, k.weight ORDER BY a.id;'
  )];
  assert.equal(erows.length, 2);
  assert.equal(erows[0][0], 1);
  assert.equal(erows[0][1], 2);
  assert.ok(Math.abs(erows[0][2] - 0.5) < 0.01);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_header_false_f_column_names', () => {
  const dbDir = makeTmpDir('no_schema_f_cols');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const vCsv = path.join(dbDir, 'vnh.csv');
  fs.writeFileSync(vCsv, '1|Alice\n2|Bob\n3|Charlie\n');
  conn.execute(`COPY ns_fvert FROM "${vCsv}" (HEADER FALSE, DELIMITER="|");`);

  const vrows = [...conn.execute('MATCH (n:ns_fvert) RETURN n.f0, n.f1 ORDER BY n.f0;')];
  assert.deepEqual(vrows, [[1, 'Alice'], [2, 'Bob'], [3, 'Charlie']]);

  const eCsv = path.join(dbDir, 'enh.csv');
  fs.writeFileSync(eCsv, '1|2|0.5\n2|3|0.8\n');
  conn.execute(
    `COPY ns_fedge FROM "${eCsv}" ` +
    `(from="ns_fvert", to="ns_fvert", HEADER FALSE, DELIMITER="|");`
  );

  const erows = [...conn.execute(
    'MATCH (a:ns_fvert)-[e:ns_fedge]->(b:ns_fvert) ' +
    'RETURN a.f0, b.f0, e.f2 ORDER BY a.f0;'
  )];
  assert.equal(erows.length, 2);
  assert.equal(erows[0][0], 1);
  assert.equal(erows[0][1], 2);
  assert.ok(Math.abs(erows[0][2] - 0.5) < 0.01);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_multiple_types', () => {
  const dbDir = makeTmpDir('no_schema_multi_types');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'products.csv');
  fs.writeFileSync(
    csvPath,
    'product_id|product_name|price\n1|Widget|9.99\n2|Gadget|19.50\n3|Doohickey|4.25\n'
  );

  conn.execute(`COPY ns_product FROM "${csvPath}";`);

  const res = [...conn.execute(
    'MATCH (p:ns_product) RETURN p.product_id, p.product_name, p.price ' +
    'ORDER BY p.product_id;'
  )];
  assert.equal(res.length, 3);
  assert.equal(res[0][1], 'Widget');
  assert.equal(typeof res[0][2], 'number');
  assert.ok(Math.abs(res[0][2] - 9.99) < 0.01);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_pk_is_first_column', () => {
  const dbDir = makeTmpDir('no_schema_pk_first_col');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'items.csv');
  fs.writeFileSync(csvPath, 'item_id|description\n10|Pen\n20|Pencil\n30|Eraser\n');

  conn.execute(`COPY ns_item FROM "${csvPath}";`);

  const res = [...conn.execute('MATCH (i:ns_item) RETURN i.item_id ORDER BY i.item_id;')];
  assert.equal(res.length, 3);
  assert.equal(res[0][0], 10);
  assert.equal(res[1][0], 20);
  assert.equal(res[2][0], 30);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_node_wide_row', () => {
  const src = path.join(neugRepoRoot(), 'example_dataset', 'comprehensive_graph', 'node_a.csv');
  if (!fs.existsSync(src)) {
    return; // skip: missing comprehensive_graph dataset
  }
  const lines = fs.readFileSync(src, 'utf-8').split('\n');
  if (lines.length < 4) {
    return; // skip: node_a.csv too short
  }

  const dbDir = makeTmpDir('no_schema_wide');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'comp_node_slice.csv');
  // Header + id=1 + id=2 rows (skip id=0 to avoid large u64 sniff edge cases)
  fs.writeFileSync(csvPath, [lines[0], lines[2], lines[3]].join('\n') + '\n');

  conn.execute(`COPY ns_comp FROM "${csvPath}";`);

  const idRes = [...conn.execute('MATCH (n:ns_comp) RETURN n.id ORDER BY n.id;')];
  assert.equal(idRes.length, 2);

  const r1 = [...conn.execute(
    'MATCH (n:ns_comp) WHERE n.id = 1 RETURN n.i32_property, n.i64_property, ' +
    'n.f32_property, n.f64_property, n.str_property, n.interval_property;'
  )][0];
  assert.equal(r1[0], 987654321);
  // native binding casts INT64 to double (JS Number), so check approximately
  assert.ok(r1[1] < -9e18, `i64_property should be a very large negative number, got ${r1[1]}`);
  assert.ok(Math.abs(r1[2] - -1.4142135) < 1e-5);
  assert.ok(Math.abs(r1[3] - 1.4142135623730951) < 1e-12);
  assert.equal(r1[4], 'test_string_1');
  assert.equal(r1[5], '3years');

  const dtRow = [...conn.execute(
    'MATCH (n:ns_comp) WHERE n.id = 1 RETURN n.datetime_property;'
  )][0];
  // datetime may be returned as a Date object or string depending on binding
  if (dtRow[0] instanceof Date) {
    assert.equal(dtRow[0].getFullYear(), 2023);
    assert.equal(dtRow[0].getMonth() + 1, 6);
    assert.equal(dtRow[0].getDate(), 22);
  } else {
    assert.ok(String(dtRow[0]).includes('2023-06-22'));
  }

  const dateRow = [...conn.execute(
    'MATCH (n:ns_comp) WHERE n.id = 1 RETURN n.date_property;'
  )][0];
  assert.ok(String(dateRow[0]).includes('2023-06-22'));

  const u32u64 = [...conn.execute(
    'MATCH (n:ns_comp) WHERE n.id = 1 RETURN n.u32_property, n.u64_property;'
  )][0];
  assert.equal(u32u64[0], 0);
  assert.ok(u32u64[1] == 0);

  const r2 = [...conn.execute(
    'MATCH (n:ns_comp) WHERE n.id = 2 RETURN n.str_property;'
  )][0];
  assert.equal(r2[0], 'test_string_2');

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_node_string_pk', () => {
  const dbDir = makeTmpDir('no_schema_str_pk');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'skus.csv');
  fs.writeFileSync(csvPath, 'sku|qty\nABC-001|10\nXYZ-9|20\n');

  conn.execute(`COPY ns_sku FROM "${csvPath}";`);

  const res = [...conn.execute('MATCH (s:ns_sku) RETURN s.sku, s.qty ORDER BY s.sku;')];
  assert.equal(res.length, 2);
  assert.equal(res[0][0], 'ABC-001');
  assert.equal(res[0][1], 10);
  assert.equal(res[1][0], 'XYZ-9');

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_node_duplicate_pk_first_row_wins', () => {
  const dbDir = makeTmpDir('no_schema_dup_pk');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'dup.csv');
  fs.writeFileSync(csvPath, 'id|name\n1|First\n1|Second\n');

  conn.execute(`COPY ns_dup_pk FROM "${csvPath}";`);

  const res = [...conn.execute('MATCH (n:ns_dup_pk) RETURN n.id, n.name;')];
  assert.equal(res.length, 1);
  assert.equal(res[0][0], 1);
  assert.equal(res[0][1], 'First');

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_node_reopen_database', () => {
  const dbDir = makeTmpDir('no_schema_reopen');
  const csvPath = path.join(dbDir, 'persist.csv');
  fs.writeFileSync(csvPath, 'id|name\n7|Reopen\n');

  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute(`COPY ns_persist FROM "${csvPath}";`);
  conn.close();
  db.close();

  const db2 = new Database({ databasePath: dbDir, mode: 'r' });
  const conn2 = db2.connect();
  const rows = [...conn2.execute('MATCH (n:ns_persist) RETURN n.id, n.name ORDER BY n.id;')];
  assert.equal(rows.length, 1);
  assert.equal(rows[0][0], 7);
  assert.equal(rows[0][1], 'Reopen');
  conn2.close();
  db2.close();
});

test('test_copy_from_no_schema_node_second_copy_appends', () => {
  const dbDir = makeTmpDir('no_schema_second_copy');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const a = path.join(dbDir, 'batch_a.csv');
  const b = path.join(dbDir, 'batch_b.csv');
  fs.writeFileSync(a, 'id|v\n1|a\n');
  fs.writeFileSync(b, 'id|v\n2|b\n');

  conn.execute(`COPY ns_twice FROM "${a}";`);
  conn.execute(`COPY ns_twice FROM "${b}";`);

  const rows = [...conn.execute('MATCH (n:ns_twice) RETURN n.id, n.v ORDER BY n.id;')];
  assert.equal(rows.length, 2);
  assert.deepEqual(rows[0], [1, 'a']);
  assert.deepEqual(rows[1], [2, 'b']);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_node_subquery_where_filter', () => {
  const dbDir = makeTmpDir('no_schema_where');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'filtered.csv');
  fs.writeFileSync(csvPath, 'id|name|tag\n1|A|x\n2|B|y\n3|C|z\n');

  conn.execute(
    `COPY ns_filt FROM (LOAD FROM "${csvPath}" WHERE id > 1 RETURN id, name);`
  );

  const rows = [...conn.execute('MATCH (n:ns_filt) RETURN n.id ORDER BY n.id;')];
  assert.equal(rows.length, 2);
  assert.deepEqual(rows.map((r) => r[0]), [2, 3]);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_node_header_only_csv', () => {
  const dbDir = makeTmpDir('no_schema_header_only');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'empty_body.csv');
  fs.writeFileSync(csvPath, 'id|name\n');

  conn.execute(`COPY ns_empty FROM "${csvPath}";`);

  const rows = [...conn.execute('MATCH (n:ns_empty) RETURN n.id;')];
  assert.equal(rows.length, 0);

  conn.close();
  db.close();
});

test('test_copy_from_no_schema_node_unicode_string', () => {
  const dbDir = makeTmpDir('no_schema_unicode');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const csvPath = path.join(dbDir, 'uni.csv');
  fs.writeFileSync(csvPath, 'id|label\n1|你好\n2|English\n', 'utf-8');

  conn.execute(`COPY ns_uni FROM "${csvPath}";`);

  const rows = [...conn.execute('MATCH (u:ns_uni) RETURN u.id, u.label ORDER BY u.id;')];
  assert.equal(rows.length, 2);
  assert.equal(rows[0][1], '你好');
  assert.equal(rows[1][1], 'English');

  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Extension Tests: COPY FROM JSON / JSONL / Parquet
// ---------------------------------------------------------------------------

function tinysnbPath() {
  return path.join(neugRepoRoot(), 'example_dataset', 'tinysnb');
}

test('test_copy_from_json_node_no_schema', () => {
  const dbDir = makeTmpDir('copy_json_no_schema');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const jsonPath = path.join(dbDir, 'employees.json');
  const data = [
    { emp_id: 100, name: 'Alice', salary: 55000.0 },
    { emp_id: 200, name: 'Bob', salary: 62000.5 },
  ];
  fs.writeFileSync(jsonPath, JSON.stringify(data));

  conn.execute(`COPY ns_employee FROM "${jsonPath}";`);

  const res = [...conn.execute(
    'MATCH (e:ns_employee) RETURN e.emp_id, e.name, e.salary ORDER BY e.emp_id;'
  )];
  assert.equal(res.length, 2);
  assert.equal(res[0][0], 100);
  assert.equal(res[0][1], 'Alice');
  assert.ok(Math.abs(res[0][2] - 55000.0) < 0.01);

  conn.close();
  db.close();
});

test('test_copy_from_jsonl_node_no_schema', () => {
  const dbDir = makeTmpDir('copy_jsonl_no_schema');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  const jsonlPath = path.join(dbDir, 'items.jsonl');
  const records = [
    { item_id: 1, description: 'Pen', price: 1.5 },
    { item_id: 2, description: 'Pencil', price: 0.75 },
    { item_id: 3, description: 'Eraser', price: 0.5 },
  ];
  fs.writeFileSync(jsonlPath, records.map((r) => JSON.stringify(r)).join('\n') + '\n');

  conn.execute(`COPY ns_item FROM "${jsonlPath}";`);

  const res = [...conn.execute(
    'MATCH (i:ns_item) RETURN i.item_id, i.description, i.price ORDER BY i.item_id;'
  )];
  assert.equal(res.length, 3);
  assert.deepEqual(res[0], [1, 'Pen', 1.5]);

  conn.close();
  db.close();
});

test('test_copy_from_json_with_subquery', () => {
  const dbDir = makeTmpDir('copy_json_subquery');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();

  conn.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));'
  );

  const jsonPath = path.join(dbDir, 'unordered.json');
  const data = [
    { name: 'Alice', age: 30, user_id: 1 },
    { name: 'Bob', age: 25, user_id: 2 },
  ];
  fs.writeFileSync(jsonPath, JSON.stringify(data));

  conn.execute(
    `COPY person FROM (LOAD FROM "${jsonPath}" RETURN user_id AS id, name, age);`
  );

  const res = [...conn.execute(
    'MATCH (n:person) RETURN n.id, n.name, n.age ORDER BY n.id;'
  )];
  assert.equal(res.length, 2);
  assert.deepEqual(res[0], [1, 'Alice', 30]);
  assert.deepEqual(res[1], [2, 'Bob', 25]);

  conn.close();
  db.close();
});

test('test_copy_from_parquet_node_no_schema', () => {
  if (!EXTENSION_TESTS_ENABLED) {
    return; // skip: extension tests disabled
  }
  const parquetPath = path.join(tinysnbPath(), 'parquet', 'vPerson.parquet');
  if (!fs.existsSync(parquetPath)) {
    return; // skip: parquet file not found
  }

  const dbDir = makeTmpDir('copy_parquet_no_schema');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('LOAD PARQUET');

  conn.execute(`COPY ns_person FROM "${parquetPath}";`);

  const res = [...conn.execute(
    'MATCH (n:ns_person) RETURN n.ID, n.fName, n.age ORDER BY n.ID;'
  )];
  assert.equal(res.length, 8);
  assert.equal(res[0][1], 'Alice');
  assert.equal(res[0][2], 35);
  assert.equal(res[res.length - 1][1], 'Hubert Blaine Wolfeschlegelsteinhausenbergerdorff');

  conn.close();
  db.close();
});

test('test_copy_from_parquet_edge_no_schema', () => {
  if (!EXTENSION_TESTS_ENABLED) {
    return; // skip: extension tests disabled
  }
  const personParquet = path.join(tinysnbPath(), 'parquet', 'vPerson.parquet');
  const meetsParquet = path.join(tinysnbPath(), 'parquet', 'eMeets.parquet');
  if (!fs.existsSync(personParquet) || !fs.existsSync(meetsParquet)) {
    return; // skip: parquet test data not found
  }

  const dbDir = makeTmpDir('copy_parquet_edge');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  conn.execute('LOAD PARQUET');

  conn.execute(`COPY ns_person FROM "${personParquet}";`);
  conn.execute(
    `COPY ns_meets FROM "${meetsParquet}" (from="ns_person", to="ns_person");`
  );

  const res = [...conn.execute(
    'MATCH (a:ns_person)-[m:ns_meets]->(b:ns_person) ' +
    'RETURN a.fName, b.fName ORDER BY a.fName;'
  )];
  assert.equal(res.length, 7);

  conn.close();
  db.close();
});
