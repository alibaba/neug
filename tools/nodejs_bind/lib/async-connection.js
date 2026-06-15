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

/**
 * AsyncConnection provides an asynchronous interface to interact with the NeuG database.
 *
 * The underlying implementation uses Node.js worker_threads (or the synchronous Connection
 * wrapped in a Promise) to execute queries asynchronously.
 *
 * @example
 * const db = new Database({ databasePath: '/tmp/test.db', mode: 'w' });
 * const conn = db.asyncConnect();
 * const result = await conn.execute('MATCH (n) RETURN n');
 * for (const row of result) {
 *   console.log(row);
 * }
 * conn.close();
 */
class AsyncConnection {
  /**
   * @param {object} nativeConnection - The native NodeConnection object.
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
   * Close the async connection.
   */
  close() {
    if (this._isOpen) {
      this._conn.close();
      this._isOpen = false;
    }
  }

  /**
   * Execute a cypher query asynchronously.
   *
   * @param {string} query - The cypher query to execute.
   * @param {string} [accessMode=''] - The access mode of the query.
   * @param {Object} [parameters=null] - Query parameters as key-value pairs.
   * @returns {Promise<QueryResult>} A promise that resolves to the query result.
   */
  async execute(query, accessMode = '', parameters = null) {
    if (!this._isOpen) {
      throw new Error('Connection is closed.');
    }
    // Wrap synchronous native call in a Promise for async interface
    return new Promise((resolve, reject) => {
      try {
        const nativeResult = this._conn.execute(
          query,
          accessMode,
          parameters || {}
        );
        resolve(new QueryResult(nativeResult));
      } catch (e) {
        reject(e);
      }
    });
  }
}

module.exports = { AsyncConnection };
