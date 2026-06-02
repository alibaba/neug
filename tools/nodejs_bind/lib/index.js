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

/**
 * NeuG Node.js Bindings
 *
 * A high-performance embedded graph database with Cypher query support.
 *
 * @example
 * const { Database } = require('neug');
 *
 * // Open an in-memory database
 * const db = new Database({ mode: 'w' });
 * const conn = db.connect();
 *
 * // Create schema
 * conn.execute('CREATE (n:person {name: "Alice", age: 30})');
 *
 * // Query
 * const result = conn.execute('MATCH (n) RETURN n');
 * for (const row of result) {
 *   console.log(row);
 * }
 *
 * conn.close();
 * db.close();
 *
 * @module neug
 */

'use strict';

const { Database } = require('./database');
const { Connection } = require('./connection');
const { AsyncConnection } = require('./async-connection');
const { QueryResult } = require('./query-result');
const { Session } = require('./session');
const { parseAndFormatResults, printResultsAsTable, parseEntry } = require('./format');
const { version } = require('./version');
const errorCodes = require('./error-codes');

module.exports = {
  Database,
  Connection,
  AsyncConnection,
  QueryResult,
  Session,
  parseAndFormatResults,
  printResultsAsTable,
  parseEntry,
  version,
  // Error codes (mirrors protobuf Code enum)
  ...errorCodes,
};
