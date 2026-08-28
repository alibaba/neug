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

const { QueryResult } = require('./query-result');
const { validAccessModes, isAccessModeValid } = require('./utils');
const { OK, ERR_CONNECTION_CLOSED, codeName } = require('./error-codes');

/**
 * Connection represents a logical connection to a NeuG database.
 * Use this class to interact with the database, executing queries and managing transactions.
 *
 * @example
 * const db = new Database({ databasePath: '/tmp/test.db', mode: 'w' });
 * const conn = db.connect();
 * const result = conn.execute('MATCH (n) RETURN n');
 * for (const row of result) {
 *   console.log(row);
 * }
 * conn.close();
 */
class Connection {
  /**
   * @param {object} nativeConnection - The native NodeConnection object from the C++ binding.
   */
  constructor(nativeConnection) {
    this._conn = nativeConnection;
    this._isOpen = true;
  }

  /**
   * Check if the connection is open.
   * @returns {boolean} True if the connection is open.
   */
  get isOpen() {
    return this._isOpen;
  }

  /**
   * Close the connection to the database.
   */
  close() {
    if (this._isOpen) {
      this._conn.close();
      this._isOpen = false;
    }
  }

  /**
   * Whether this connection has an unfinished explicit transaction.
   *
   * The property remains true while a failed transaction is rollback-only.
   * Call {@link rollback} before issuing another query.
   *
   * @returns {boolean}
   */
  get hasActiveTransaction() {
    return this._isOpen && this._conn.hasActiveTransaction();
  }

  /**
   * Begin an explicit embedded AP transaction.
   *
   * @param {{readOnly?: boolean}} [options={}] Transaction options.
   * @param {boolean} [options.readOnly=false] Pin one read view and reject
   *   writes. By default, the transaction uses a private COW write view.
   * @throws {Error} If the connection is closed or already has an active
   *   transaction.
   */
  beginTransaction(options = {}) {
    this._ensureOpen('beginning a transaction');
    if (
      typeof options !== 'object' ||
      options === null ||
      Array.isArray(options)
    ) {
      throw new TypeError('Transaction options must be an object.');
    }
    const { readOnly = false } = options;
    if (typeof readOnly !== 'boolean') {
      throw new TypeError('options.readOnly must be a boolean.');
    }
    this._checkTransactionStatus(
      'begin transaction',
      this._conn.beginTransaction(readOnly)
    );
  }

  /**
   * Commit the active explicit transaction.
   *
   * A rollback-only transaction must be rolled back instead.
   */
  commit() {
    this._ensureOpen('committing a transaction');
    this._checkTransactionStatus('commit transaction', this._conn.commit());
  }

  /**
   * Roll back the active explicit transaction and return to auto-commit mode.
   */
  rollback() {
    this._ensureOpen('rolling back a transaction');
    this._checkTransactionStatus('roll back transaction', this._conn.rollback());
  }

  /**
   * Execute a cypher query on the database.
   *
   * @param {string} query - The cypher query to execute.
   * @param {string} [accessMode=''] - The access mode of the query.
   *   Supported modes: 'read'/'r', 'insert'/'i', 'update'/'u', 'schema'/'s'.
   * @param {Object} [parameters=null] - Query parameters as key-value pairs.
   * @returns {QueryResult} The result of the query execution.
   * @throws {Error} If the connection is closed or the query fails.
   *
   * @example
   * // Simple query
   * const result = conn.execute('MATCH (n) RETURN n.id');
   *
   * // Query with parameters
   * const result = conn.execute(
   *   'MATCH (n:person) WHERE n.id = $id RETURN n.name',
   *   'read',
   *   { id: 12345 }
   * );
   */
  execute(query, accessMode = '', parameters = null) {
    this._ensureOpen('executing queries');

    if (accessMode !== '' && !isAccessModeValid(accessMode.toLowerCase())) {
      throw new Error(
        `Invalid access_mode: ${accessMode}. Supported access modes are ` +
        `[${validAccessModes.join(', ')}].`
      );
    }

    const nativeResult = this._conn.execute(
      query,
      accessMode,
      parameters || {}
    );

    const statusCode = nativeResult.statusCode();
    const statusMessage = nativeResult.statusMessage();

    if (statusCode === OK) {
      return new QueryResult(nativeResult);
    }

    throw new Error(
      `Failed to execute query: ${query}. ` +
      `Error code: ${statusCode}, Error Message: ` +
      `${codeName(statusCode)}: ${statusMessage}`
    );
  }

  /**
   * Get the schema of the NeuG database.
   * @returns {string} The schema as a string.
   */
  getSchema() {
    this._ensureOpen('getting the schema');
    return this._conn.getSchema();
  }

  _ensureOpen(action) {
    if (!this._isOpen) {
      throw new Error(
        `Connection is closed. Please open the connection before ${action}. ` +
        `Error code: ${ERR_CONNECTION_CLOSED}`
      );
    }
  }

  _checkTransactionStatus(action, status) {
    if (status.code !== OK) {
      throw new Error(
        `Failed to ${action}. Error code: ${status.code}, Error Message: ` +
        `${codeName(status.code)}: ${status.message}`
      );
    }
  }
}

module.exports = { Connection };
