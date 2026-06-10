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

const { Connection } = require('./connection');
const { AsyncConnection } = require('./async-connection');
const { readable } = require('./utils');
const { version } = require('./version');
const {
  ERR_CONFIG_INVALID,
  ERR_INVALID_ARGUMENT,
  ERR_INVALID_PATH,
} = require('./error-codes');

// Load the native binding (unified loader handles prebuilds/ and build/)
const nativeBinding = require('./binding');

const ILLEGAL_CHARS = ['?', '*', '"', '<', '>', '|', ':', '\\'];
const PURE_MEMORY_PATHS = [':memory', ':memory:'];
const VALID_MODES = [
  'r', 'read', 'w', 'rw', 'write',
  'readwrite', 'read-write', 'read_write',
  'read-only', 'read_only',
];

/**
 * Database is the main entry point for the NeuG graph database.
 *
 * Use this class to open a database, create connections, and manage the database lifecycle.
 *
 * @example
 * const { Database } = require('neug');
 *
 * const db = new Database({ databasePath: '/tmp/test.db', mode: 'w' });
 * const conn = db.connect();
 *
 * conn.execute('CREATE (n:person {name: "Alice"})');
 * const result = conn.execute('MATCH (n) RETURN n');
 * for (const row of result) {
 *   console.log(row);
 * }
 *
 * conn.close();
 * db.close();
 */
class Database {
  /**
   * Return the number of online CPUs, using the same system call as
   * Python's os.cpu_count() (sysconf(_SC_NPROCESSORS_ONLN)).
   * This is more reliable than os.cpus().length on arm64 Linux.
   * @returns {number}
   */
  static cpuCount() {
    return nativeBinding.NodeDatabase.cpuCount();
  }

  /**
   * Open a NeuG database.
   *
   * @param {Object} options - Database configuration options.
   * @param {string|null} [options.databasePath=null] - Path to the database directory.
   *   Empty string or null for in-memory mode.
   * @param {string} [options.mode='read-write'] - Database access mode.
   *   Supported: 'r', 'read', 'w', 'rw', 'write', 'readwrite', 'read-write', 'read-only'.
   * @param {number} [options.maxThreadNum=0] - Maximum number of threads (0 = no limit).
   * @param {boolean} [options.checkpointOnClose=true] - Whether to checkpoint on close.
   * @param {string} [options.bufferStrategy='M_FULL'] - Buffer strategy:
   *   'InMemory'/'M_FULL', 'SyncToFile'/'M_LAZY', 'HugePagePreferred'/'M_HUGE'.
   */
  constructor(options = {}) {
    const {
      databasePath = null,
      mode = 'read-write',
      maxThreadNum = 0,
      checkpointOnClose = true,
      bufferStrategy = 'M_FULL',
    } = options;

    this._connections = [];
    this._asyncConnections = [];

    // Validate path
    const dbPath = databasePath !== null ? databasePath : '';
    if (
      typeof dbPath === 'string' &&
      ILLEGAL_CHARS.some((ch) => dbPath.includes(ch)) &&
      !PURE_MEMORY_PATHS.includes(dbPath)
    ) {
      throw new Error(
        `Invalid path: database path '${dbPath}' contains illegal characters: ` +
        `[${ILLEGAL_CHARS.join(', ')}]. Error code: ${ERR_INVALID_PATH}.`
      );
    }

    // Validate mode
    if (!VALID_MODES.includes(mode)) {
      throw new Error(
        `Invalid mode: ${mode}. Must be one of: ${VALID_MODES.join(', ')}. ` +
        `Error code: ${ERR_INVALID_ARGUMENT}.`
      );
    }

    // Validate maxThreadNum
    if (maxThreadNum < 0) {
      throw new Error(
        `Invalid config: maxThreadNum: ${maxThreadNum}. Must be a non-negative integer. ` +
        `Error code: ${ERR_CONFIG_INVALID}.`
      );
    }

    // Use sysconf(_SC_NPROCESSORS_ONLN) via native binding to match
    // Python's os.cpu_count() behaviour (especially on arm64 Linux).
    const cpuCount = Database.cpuCount();
    if (maxThreadNum > cpuCount) {
      throw new Error(
        `Invalid argument: maxThreadNum: ${maxThreadNum}. ` +
        `Must be less than or equal to CPU cores: ${cpuCount}. ` +
        `Error code: ${ERR_INVALID_ARGUMENT}.`
      );
    }

    // In-memory database cannot be opened in read-only mode
    // NOTE: only null (i.e. databasePath not provided) is blocked;
    // empty string '' is allowed, matching the Python binding behaviour.
    if (
      databasePath === null &&
      ['r', 'read', 'read-only', 'read_only'].includes(mode)
    ) {
      throw new Error(
        `Invalid mode: ${mode}. In-memory database cannot be opened in read-only mode. ` +
        `Error code: ${ERR_INVALID_ARGUMENT}.`
      );
    }

    this._dbPath = dbPath;
    this._mode = mode;
    this._maxThreadNum = maxThreadNum;

    // Create the native database
    this._database = new nativeBinding.NodeDatabase({
      databasePath: dbPath,
      maxThreadNum,
      mode: readable(mode),
      planner: 'gopt',
      checkpointOnClose,
      bufferStrategy,
    });

    if (!dbPath || dbPath.trim() === '') {
      console.log(`[neug] Open in-memory database in ${readable(mode)} mode`);
    } else {
      console.log(`[neug] Open database ${dbPath} in ${mode} mode`);
    }
  }

  /**
   * Get the version of the database.
   * @returns {string} The version string.
   */
  get version() {
    return version;
  }

  /**
   * Get the database mode.
   * @returns {string} The mode string.
   */
  get mode() {
    return this._mode;
  }

  /**
   * Connect to the database and return a Connection object.
   *
   * @returns {Connection} A connection to the database.
   * @throws {Error} If the database is closed.
   */
  connect() {
    if (!this._database) {
      throw new Error('Database is closed.');
    }
    const nativeConn = this._database.connect();
    const conn = new Connection(nativeConn);
    this._connections.push(conn);
    return conn;
  }

  /**
   * Connect to the database asynchronously.
   *
   * @returns {AsyncConnection} An async connection to the database.
   * @throws {Error} If the database is closed.
   */
  asyncConnect() {
    if (!this._database) {
      throw new Error('Database is closed.');
    }
    const nativeConn = this._database.connect();
    const asyncConn = new AsyncConnection(nativeConn);
    this._asyncConnections.push(asyncConn);
    return asyncConn;
  }

  /**
   * Close the database connection and release all resources.
   */
  close() {
    if (this._dbPath && this._dbPath.trim() !== '') {
      console.log(`[neug] Closing database ${this._dbPath}.`);
    }

    // Close all connections
    if (this._connections) {
      for (const conn of this._connections) {
        try {
          conn.close();
        } catch (e) {
          console.warn(`[neug] Failed to close connection: ${e.message}`);
        }
      }
    }

    // Close all async connections
    if (this._asyncConnections) {
      for (const asyncConn of this._asyncConnections) {
        try {
          asyncConn.close();
        } catch (e) {
          console.warn(`[neug] Failed to close async connection: ${e.message}`);
        }
      }
    }

    if (this._database) {
      this._database.close();
      this._database = null;
    }
  }
}

module.exports = { Database };
