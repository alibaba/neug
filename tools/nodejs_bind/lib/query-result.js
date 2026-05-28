/**
 * Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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

/**
 * QueryResult represents the result of a cypher query.
 * It provides an iterator-like interface to access the results.
 *
 * @example
 * const result = conn.execute('MATCH (n) RETURN n');
 * for (const row of result) {
 *   console.log(row);
 * }
 */
class QueryResult {
  /**
   * @param {object} nativeResult - The native NodeQueryResult object from the C++ binding.
   */
  constructor(nativeResult) {
    this._result = nativeResult;
  }

  /**
   * Check if there are more results available.
   * @returns {boolean} True if there are more results.
   */
  hasNext() {
    return this._result.hasNext();
  }

  /**
   * Get the next row of results.
   * @returns {Array} The next row as an array of values.
   */
  getNext() {
    return this._result.getNext();
  }

  /**
   * Get the result at the specified index.
   * @param {number} index - The index of the result to retrieve.
   * @returns {Array} The row at the specified index.
   * @throws {RangeError} If the index is out of range.
   */
  getAt(index) {
    return this._result.getAt(index);
  }

  /**
   * Get the total number of results.
   * @returns {number} The number of results.
   */
  length() {
    return this._result.length();
  }

  /**
   * Get the projected column names.
   * @returns {string[]} Column names in projection order.
   */
  columnNames() {
    return this._result.columnNames();
  }

  /**
   * Get the status code of the query result.
   * @returns {number} 0 for success, non-zero for error.
   */
  statusCode() {
    return this._result.statusCode();
  }

  /**
   * Get the status message of the query result.
   * @returns {string} The status message.
   */
  statusMessage() {
    return this._result.statusMessage();
  }

  /**
   * Get the result in Bolt response format.
   * @returns {string} The result in Bolt response format.
   */
  getBoltResponse() {
    return this._result.getBoltResponse();
  }

  /**
   * Close the query result and release resources.
   */
  close() {
    this._result.close();
  }

  /**
   * Make QueryResult iterable with for...of loops.
   * @returns {Iterator} An iterator over the result rows.
   */
  [Symbol.iterator]() {
    const self = this;
    return {
      next() {
        if (self.hasNext()) {
          return { value: self.getNext(), done: false };
        }
        return { value: undefined, done: true };
      },
    };
  }

  /**
   * Get the string representation.
   * @returns {string} String representation.
   */
  toString() {
    return `QueryResult(size ${this.length()})`;
  }
}

module.exports = { QueryResult };
