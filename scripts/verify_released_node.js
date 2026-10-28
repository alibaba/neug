'use strict';
// Cross-platform smoke test for the RELEASED @neug-ai/neug Node.js SDK.
//
// The CI workflow runs `npm install @neug-ai/neug@<version>` at the repo root,
// so require() from this script (under scripts/) resolves the package via the
// root node_modules. Verifies three things and exits non-zero if any fails:
//   1) version match EXPECT_VERSION (default 0.3.0)
//   2) basic create/insert/query on an in-memory database
//   3) INSTALL + LOAD each of the 6 official extensions (from the release OSS
//      bucket for the current platform)
//
// Set NEUG_NODE_MODULE to override the module id (e.g. the legacy
// '@graphscope-neug/neug'). Prints BASE_RESULT / EXT_MATRIX / EXT_ALL_OK / FINAL.

const assert = require('assert');

const EXPECTED = process.env.EXPECT_VERSION || '0.3.0';
const MODULE_ID = process.env.NEUG_NODE_MODULE || '@neug-ai/neug';
const EXTENSIONS = ['parquet', 'httpfs', 'pattern_matching', 'vector_search', 'fts', 'gds'];

let baseFail = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`  [PASS] ${name}`);
  } catch (e) {
    console.log(`  [FAIL] ${name}: ${e && e.message ? e.message : e}`);
    baseFail++;
  }
}
function rows(conn, cypher, mode, params) {
  const res = params ? conn.execute(cypher, mode, params) : conn.execute(cypher);
  const out = [];
  for (const r of res) out.push(Array.from(r));
  return out;
}
const val = (x) => (typeof x === 'bigint' ? Number(x) : x);

console.log('=== 0. environment ===');
console.log(`  node     : ${process.version}`);
console.log(`  platform : ${process.platform} ${process.arch}`);

let neug;
try {
  neug = require(MODULE_ID);
} catch (e) {
  console.log(`  [FAIL] require ${MODULE_ID}: ${e && e.message ? e.message : e}`);
  console.log('BASE_RESULT: FAIL');
  console.log('FINAL: FAIL');
  process.exit(2);
}
const { Database } = neug;

console.log(`  module   : ${MODULE_ID}`);
console.log(`  neug.version = ${neug.version}`);
check(`version === ${EXPECTED}`, () => assert.strictEqual(neug.version, EXPECTED));

console.log('\n=== 1. basic insert / query (in-memory) ===');
const db = new Database({ databasePath: '', mode: 'w' });
const conn = db.connect();
check('create schema', () => {
  conn.execute('CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id));');
  conn.execute('CREATE NODE TABLE Company(id INT64, name STRING, PRIMARY KEY(id));');
  conn.execute('CREATE REL TABLE WORKS_FOR(FROM Person TO Company, since INT64);');
  conn.execute('CREATE REL TABLE KNOWS(FROM Person TO Person);');
});
check('insert rows', () => {
  conn.execute("CREATE (:Person {id: 1, name: 'Alice', age: 30});");
  conn.execute("CREATE (:Person {id: 2, name: 'Bob', age: 25});");
  conn.execute("CREATE (:Company {id: 1, name: 'Acme'});");
  conn.execute('MATCH (a:Person),(b:Company) WHERE a.id=1 AND b.id=1 CREATE (a)-[:WORKS_FOR {since: 2020}]->(b);');
  conn.execute('MATCH (a:Person),(b:Person) WHERE a.id=1 AND b.id=2 CREATE (a)-[:KNOWS]->(b);');
});
check('count Person == 2', () => assert.strictEqual(val(rows(conn, 'MATCH (p:Person) RETURN count(p);')[0][0]), 2));
check('WORKS_FOR.since == 2020', () => assert.strictEqual(val(rows(conn, 'MATCH (a:Person)-[w:WORKS_FOR]->(c:Company) RETURN w.since;')[0][0]), 2020));
check('parameterized query', () => {
  const r = rows(conn, 'MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name=$name RETURN b.name;', 'read', { name: 'Alice' });
  assert.strictEqual(r[0][0], 'Bob');
});

console.log('\n=== 2. extensions INSTALL / LOAD (from OSS) ===');
const matrix = {};
for (const ext of EXTENSIONS) {
  let ins = false, ld = false, insErr = '', ldErr = '';
  try {
    conn.execute(`INSTALL ${ext};`);
    ins = true;
  } catch (e) {
    insErr = String(e && e.message ? e.message : e).split('\n').join(' | ').slice(0, 400);
  }
  if (ins) {
    try {
      conn.execute(`LOAD ${ext};`);
      ld = true;
    } catch (e) {
      ldErr = String(e && e.message ? e.message : e).split('\n').join(' | ').slice(0, 400);
    }
  }
  matrix[ext] = { ins, ld };
  console.log(`  [${ins && ld ? 'OK' : 'XX'}] ${ext.padEnd(18)} INSTALL=${ins ? 'Y' : 'N'} LOAD=${ld ? 'Y' : 'N'}`);
  if (insErr) console.log(`        install-error: ${insErr}`);
  if (ldErr) console.log(`        load-error   : ${ldErr}`);
}
try {
  const r = rows(conn, 'CALL SHOW_LOADED_EXTENSIONS() RETURN *;');
  console.log(`  SHOW_LOADED_EXTENSIONS -> ${r.length} loaded: ${JSON.stringify(r.map((x) => String(x[0])))}`);
} catch (e) {
  console.log(`  SHOW_LOADED_EXTENSIONS unavailable: ${e.message || e}`);
}
conn.close();
db.close();

const extAllOk = EXTENSIONS.every((e) => matrix[e].ins && matrix[e].ld);
console.log('\n=== SUMMARY ===');
console.log(`BASE_RESULT: ${baseFail === 0 ? 'PASS' : 'FAIL'}`);
console.log('EXT_MATRIX:');
for (const e of EXTENSIONS) console.log(`  ${e.padEnd(18)} INSTALL=${matrix[e].ins ? 'Y' : 'N'} LOAD=${matrix[e].ld ? 'Y' : 'N'}`);
console.log(`EXT_ALL_OK: ${extAllOk}`);
const finalOk = baseFail === 0 && extAllOk;
console.log(`FINAL: ${finalOk ? 'PASS' : 'FAIL'}`);
process.exit(finalOk ? 0 : 1);
