/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * End-to-end tests for PROFILE and EXPLAIN modes in Node.js binding (AP mode only).
 *
 * NOTE: Node.js binding only supports AP (embedded) mode.
 *
 * This test suite covers:
 * - AP mode (local embedded): QueryProcessor::execute_internal via C++ binding
 * - PROFILE mode: Executes query and collects per-operator metrics
 * - EXPLAIN mode: Builds operator tree without executing, returns 0 rows
 * - Dict interface: getProfileMetrics() returns structured metrics
 */

'use strict';

const { test, after } = require('./test-shim');
const assert = require('assert').strict;
const fs = require('fs');
const path = require('path');
const os = require('os');
const { Database } = require('neug');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let _tmpCounter = 0;
const _tmpDirs = [];

function makeTmpDir(prefix = 'neug_explain_profile_test_') {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix + _tmpCounter++ + '_'));
  _tmpDirs.push(dir);
  return dir;
}

after(() => {
  for (const dir of _tmpDirs) {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch (_) {}
  }
});

/**
 * Load sample graph data for testing
 */
function loadSampleData(connection) {
  // Create schema
  connection.execute(
    'CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));'
  );
  connection.execute(
    'CREATE NODE TABLE software(id INT64, name STRING, lang STRING, PRIMARY KEY(id));'
  );
  connection.execute('CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);');
  connection.execute(
    'CREATE REL TABLE created(FROM person TO software, weight DOUBLE);'
  );

  // Insert person data
  connection.execute("CREATE (p:person {id: 1, name: 'marko', age: 29});");
  connection.execute("CREATE (p:person {id: 2, name: 'vadas', age: 27});");
  connection.execute("CREATE (p:person {id: 3, name: 'josh', age: 32});");
  connection.execute("CREATE (p:person {id: 4, name: 'peter', age: 35});");

  // Insert software data
  connection.execute("CREATE (s:software {id: 10, name: 'lop', lang: 'java'});");
  connection.execute("CREATE (s:software {id: 11, name: 'ripple', lang: 'java'});");

  // Insert knows relationships
  connection.execute(
    'MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 2 ' +
      'CREATE (a)-[:knows {weight: 0.5}]->(b);'
  );
  connection.execute(
    'MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 3 ' +
      'CREATE (a)-[:knows {weight: 1.0}]->(b);'
  );
  connection.execute(
    'MATCH (a:person), (b:person) WHERE a.id = 2 AND b.id = 3 ' +
      'CREATE (a)-[:knows {weight: 0.7}]->(b);'
  );
  connection.execute(
    'MATCH (a:person), (b:person) WHERE a.id = 3 AND b.id = 4 ' +
      'CREATE (a)-[:knows {weight: 0.4}]->(b);'
  );

  // Insert created relationships
  connection.execute(
    'MATCH (a:person), (s:software) WHERE a.id = 1 AND s.id = 10 ' +
      'CREATE (a)-[:created {weight: 0.4}]->(s);'
  );
  connection.execute(
    'MATCH (a:person), (s:software) WHERE a.id = 4 AND s.id = 10 ' +
      'CREATE (a)-[:created {weight: 0.6}]->(s);'
  );
  connection.execute(
    'MATCH (a:person), (s:software) WHERE a.id = 1 AND s.id = 11 ' +
      'CREATE (a)-[:created {weight: 1.0}]->(s);'
  );
}

function setupTestDb() {
  const dbDir = makeTmpDir('explain_profile_');
  const db = new Database({ databasePath: dbDir, mode: 'w' });
  const conn = db.connect();
  loadSampleData(conn);
  return { db, conn };
}

// ---------------------------------------------------------------------------
// PROFILE Mode Tests - Basic Operations
// ---------------------------------------------------------------------------

test('test_profile_single_table_scan', () => {
  const { db, conn } = setupTestDb();
  const query = 'PROFILE MATCH (p:person) RETURN p.name, p.age';
  const result = conn.execute(query);

  // Verify query results
  const rows = [];
  while (result.hasNext()) {
    rows.push(result.getNext());
  }
  assert.equal(rows.length, 4);

  const colNames = result.columnNames();
  assert.equal(colNames.length, 2);

  // Verify metrics object
  const metrics = result.getProfileMetrics();
  assert.ok(metrics.hasOwnProperty('total_elapsed_ms'));
  assert.ok(metrics.hasOwnProperty('total_output_rows'));
  assert.ok(metrics.hasOwnProperty('operators'));
  assert.equal(metrics.total_output_rows, 4);
  assert.ok(metrics.operators.length > 0);

  result.close();
  conn.close();
  db.close();
});

test('test_profile_with_join', () => {
  const { db, conn } = setupTestDb();
  const query =
    'PROFILE MATCH (p1:person), (p2:person) WHERE p1.id < p2.id RETURN p1.name, p2.name';
  const result = conn.execute(query);

  const rows = [];
  while (result.hasNext()) {
    rows.push(result.getNext());
  }
  assert.equal(rows.length, 6); // 4 choose 2

  const metrics = result.getProfileMetrics();
  assert.equal(metrics.total_output_rows, 6);

  result.close();
  conn.close();
  db.close();
});

test('test_profile_with_aggregation', () => {
  const { db, conn } = setupTestDb();
  const query = 'PROFILE MATCH (p:person) RETURN COUNT(*) as person_count';
  const result = conn.execute(query);

  const rows = [];
  while (result.hasNext()) {
    rows.push(result.getNext());
  }
  assert.equal(rows.length, 1);
  // COUNT(*) returns a number (double conversion from int64)
  assert.ok(typeof rows[0][0] === 'number' || typeof rows[0][0] === 'bigint');
  assert.equal(Number(rows[0][0]), 4);

  const metrics = result.getProfileMetrics();
  assert.equal(metrics.total_output_rows, 1);

  result.close();
  conn.close();
  db.close();
});

test('test_profile_with_edge_traversal', () => {
  const { db, conn } = setupTestDb();
  const query =
    'PROFILE MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name';
  const result = conn.execute(query);

  const rows = [];
  while (result.hasNext()) {
    rows.push(result.getNext());
  }
  assert.equal(rows.length, 4); // 4 knows relationships

  const metrics = result.getProfileMetrics();
  assert.equal(metrics.total_output_rows, 4);

  result.close();
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// PROFILE Mode Tests - Metrics Structure
// ---------------------------------------------------------------------------

test('test_profile_metrics_structure', () => {
  const { db, conn } = setupTestDb();
  const query = 'PROFILE MATCH (p:person) RETURN p.id LIMIT 2';
  const result = conn.execute(query);

  // Consume result
  while (result.hasNext()) {
    result.getNext();
  }

  const metrics = result.getProfileMetrics();

  // Check top-level keys
  assert.ok(metrics.hasOwnProperty('total_elapsed_ms'));
  assert.ok(metrics.hasOwnProperty('total_output_rows'));
  assert.ok(metrics.hasOwnProperty('operators'));

  // Check operator structure
  assert.ok(metrics.operators.length > 0);
  for (const op of metrics.operators) {
    assert.ok(op.hasOwnProperty('operator_id'));
    assert.ok(op.hasOwnProperty('operator_name'));
    assert.ok(op.hasOwnProperty('elapsed_ms'));
    assert.ok(op.hasOwnProperty('output_rows'));
    assert.ok(op.hasOwnProperty('parent_id'));
    assert.ok(op.hasOwnProperty('child_ids'));

    // Verify types
    assert.equal(typeof op.operator_id, 'number');
    assert.equal(typeof op.operator_name, 'string');
    assert.equal(typeof op.elapsed_ms, 'number');
    assert.equal(typeof op.output_rows, 'number');
    assert.equal(typeof op.parent_id, 'number');
    assert.ok(Array.isArray(op.child_ids));
  }

  result.close();
  conn.close();
  db.close();
});

test('test_profile_parent_child_relationship', () => {
  const { db, conn } = setupTestDb();
  const query =
    'PROFILE MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name';
  const result = conn.execute(query);

  while (result.hasNext()) {
    result.getNext();
  }

  const metrics = result.getProfileMetrics();
  const operators = metrics.operators;

  // Build a map of operator IDs for validation
  const opMap = new Map();
  for (const op of operators) {
    opMap.set(op.operator_id, op);
  }

  // Verify parent-child relationships
  for (const op of operators) {
    if (op.parent_id !== -1) {
      assert.ok(opMap.has(op.parent_id));
    }
    for (const childId of op.child_ids) {
      assert.ok(opMap.has(childId));
    }
  }

  result.close();
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// EXPLAIN Mode Tests - Plan Visualization
// ---------------------------------------------------------------------------

test('test_explain_basic_query', () => {
  const { db, conn } = setupTestDb();
  const query =
    'EXPLAIN MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name';
  const result = conn.execute(query);

  // EXPLAIN should return 0 rows
  const rows = [];
  while (result.hasNext()) {
    rows.push(result.getNext());
  }
  assert.equal(rows.length, 0);

  // But should have profile result (the plan)
  const metrics = result.getProfileMetrics();
  assert.equal(metrics.total_output_rows, 0);
  assert.ok(metrics.operators.length > 0);

  result.close();
  conn.close();
  db.close();
});

test('test_explain_complex_query', () => {
  const { db, conn } = setupTestDb();
  const query =
    'EXPLAIN MATCH (p:person)-[e1:knows]->(q:person), ' +
    '(q)-[e2:created]->(s:software) ' +
    'RETURN p.name, s.name';
  const result = conn.execute(query);

  const rows = [];
  while (result.hasNext()) {
    rows.push(result.getNext());
  }
  assert.equal(rows.length, 0);

  const metrics = result.getProfileMetrics();
  assert.ok(metrics.operators.length > 0);

  result.close();
  conn.close();
  db.close();
});

test('test_explain_text_output_format', () => {
  const { db, conn } = setupTestDb();
  const query = 'EXPLAIN MATCH (p:person) RETURN p.id';
  const result = conn.execute(query);

  while (result.hasNext()) {
    result.getNext();
  }

  // Verify metrics object has expected structure
  const metrics = result.getProfileMetrics();
  assert.ok(metrics !== undefined);
  assert.ok(Array.isArray(metrics.operators));

  result.close();
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Normal Execution Tests (without PROFILE/EXPLAIN)
// ---------------------------------------------------------------------------

test('test_normal_execution_no_profile', () => {
  const { db, conn } = setupTestDb();
  const query = 'MATCH (p:person) RETURN p.name';
  const result = conn.execute(query);

  const rows = [];
  while (result.hasNext()) {
    rows.push(result.getNext());
  }
  assert.equal(rows.length, 4);

  // Normal execution should not have profile result (metrics will be empty)
  const metrics = result.getProfileMetrics();
  assert.ok(metrics.operators.length === 0);

  result.close();
  conn.close();
  db.close();
});

test('test_normal_execution_empty_metrics', () => {
  const { db, conn } = setupTestDb();
  const query = 'MATCH (p:person) RETURN p.id';
  const result = conn.execute(query);

  while (result.hasNext()) {
    result.getNext();
  }

  // When no profile result, metrics should be empty or minimal
  const metrics = result.getProfileMetrics();
  assert.equal(metrics.operators.length, 0);

  result.close();
  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Error Handling Tests
// ---------------------------------------------------------------------------

test('test_invalid_query_graceful_handling', () => {
  const { db, conn } = setupTestDb();
  const query = 'INVALID QUERY SYNTAX';

  // execute() throws on invalid query
  assert.throws(() => conn.execute(query), /Error/);

  conn.close();
  db.close();
});

test('test_profile_invalid_query_fails', () => {
  const { db, conn } = setupTestDb();
  const query = 'PROFILE INVALID QUERY';

  // PROFILE on invalid query should also throw
  assert.throws(() => conn.execute(query), /Error/);

  conn.close();
  db.close();
});

// ---------------------------------------------------------------------------
// Performance Comparison Tests
// ---------------------------------------------------------------------------

test('test_explain_vs_profile_execution', () => {
  const { db, conn } = setupTestDb();
  const query = 'MATCH (p:person) RETURN p.name';

  // Run EXPLAIN (should be fast)
  const explainResult = conn.execute(`EXPLAIN ${query}`);
  while (explainResult.hasNext()) {
    explainResult.getNext();
  }
  const explainMetrics = explainResult.getProfileMetrics();

  // Run PROFILE (should be slower due to execution)
  const profileResult = conn.execute(`PROFILE ${query}`);
  while (profileResult.hasNext()) {
    profileResult.getNext();
  }
  const profileMetrics = profileResult.getProfileMetrics();

  // EXPLAIN should have 0 output rows, PROFILE should have 4
  assert.equal(explainMetrics.total_output_rows, 0);
  assert.equal(profileMetrics.total_output_rows, 4);

  explainResult.close();
  profileResult.close();
  conn.close();
  db.close();
});

test('test_execution_time_progression', () => {
  const { db, conn } = setupTestDb();
  const query = 'PROFILE MATCH (p:person) RETURN p.name';
  const result = conn.execute(query);

  while (result.hasNext()) {
    result.getNext();
  }

  const metrics = result.getProfileMetrics();

  // Total time should be >= 0
  assert.ok(metrics.total_elapsed_ms >= 0);

  // Verify operator times are non-negative
  for (const op of metrics.operators) {
    assert.ok(op.elapsed_ms >= 0);
  }

  result.close();
  conn.close();
  db.close();
});
