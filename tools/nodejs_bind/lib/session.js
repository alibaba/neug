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

const http = require('http');
const https = require('https');
const { URL } = require('url');
const { QueryResult } = require('./query-result');
const { isAccessModeValid, validAccessModes } = require('./utils');
const { ERR_NETWORK, ERR_SESSION_CLOSED } = require('./error-codes');

// Load the native binding (unified loader handles prebuilds/ and build/)
const nativeBinding = require('./binding');

/**
 * Parse a timeout string to milliseconds.
 * @param {string|number} timeout - Timeout value (e.g., '10s', '500ms', or number of seconds).
 * @returns {number} Timeout in milliseconds.
 */
function parseTimeout(timeout) {
  if (typeof timeout === 'number') {
    return timeout * 1000;
  }
  if (typeof timeout === 'string') {
    if (timeout.endsWith('ms')) {
      return parseInt(timeout.slice(0, -2), 10);
    } else if (timeout.endsWith('s')) {
      return parseInt(timeout.slice(0, -1), 10) * 1000;
    }
  }
  return 10000; // default 10 seconds
}

/**
 * Make an HTTP request using Node.js built-in http/https modules.
 * @param {string} url - The URL to request.
 * @param {Object} options - Request options.
 * @param {string} [options.method='GET'] - HTTP method.
 * @param {string} [options.body] - Request body.
 * @param {number} [options.timeout=10000] - Timeout in milliseconds.
 * @returns {Promise<{statusCode: number, body: Buffer}>}
 */
function httpRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const isHttps = parsedUrl.protocol === 'https:';
    const lib = isHttps ? https : http;

    const reqOptions = {
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || (isHttps ? 443 : 80),
      path: parsedUrl.pathname + parsedUrl.search,
      method: options.method || 'GET',
      timeout: options.timeout || 10000,
      headers: options.headers || {},
    };

    const req = lib.request(reqOptions, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          body: Buffer.concat(chunks),
        });
      });
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timed out'));
    });

    if (options.body) {
      req.write(options.body);
    }
    req.end();
  });
}

/**
 * Session connects to a NeuG server and provides the same query interface as Connection.
 *
 * @example
 * // Start the server
 * const db = new Database({ databasePath: '/tmp/test.db', mode: 'w' });
 * db.serve({ port: 10000, host: 'localhost' });
 *
 * // In another process, connect via Session
 * const session = new Session({ endpoint: 'http://localhost:10000' });
 * const result = session.execute('MATCH (n) RETURN count(n)');
 * for (const row of result) {
 *   console.log(row);
 * }
 * session.close();
 */
class Session {
  /**
   * @param {Object} [options] - Session options.
   * @param {string} [options.endpoint='http://localhost:10000'] - The server endpoint URL.
   * @param {string|number} [options.timeout='10s'] - Request timeout.
   */
  constructor(options = {}) {
    const {
      endpoint = 'http://localhost:10000',
      timeout = '10s',
    } = options;

    this._endpoint = endpoint.replace(/\/+$/, '');
    this._queryEndpoint = `${this._endpoint}/cypher`;
    this._statusEndpoint = `${this._endpoint}/service_status`;
    this._schemaEndpoint = `${this._endpoint}/schema`;
    this._timeout = parseTimeout(timeout);
    this._closed = false;

    // Quick connectivity check
    // (We do this synchronously-ish via the constructor, but it's fire-and-forget)
    // For a proper async init, use Session.open() instead.
  }

  /**
   * Open a session with connectivity check.
   *
   * @param {Object} [options] - Session options (same as constructor).
   * @returns {Promise<Session>} A connected Session instance.
   */
  static async open(options = {}) {
    const session = new Session(options);
    // Wait for the server to be ready, same purpose as Python's
    // implicit readiness via slower HTTP client startup.
    const maxRetries = 5;
    const retryDelayMs = 200;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        await session._checkConnection();
        return session;
      } catch (_) {
        if (attempt < maxRetries - 1) {
          await new Promise((r) => setTimeout(r, retryDelayMs));
        }
      }
    }
    // Return anyway — let actual queries surface the real error
    return session;
  }

  /**
   * Check connectivity to the server.
   * @private
   */
  async _checkConnection() {
    await httpRequest(this._statusEndpoint, {
      method: 'GET',
      timeout: this._timeout,
    });
  }

  /**
   * Close the session.
   */
  close() {
    if (this._closed) {
      console.warn('[neug] Session is already closed.');
      return;
    }
    this._closed = true;
  }

  /**
   * Execute a query on the NeuG server.
   *
   * @param {string} query - The cypher query string.
   * @param {string} [accessMode=''] - The access mode.
   * @param {Object} [parameters=null] - Query parameters.
   * @returns {Promise<QueryResult>} The query result.
   * @throws {Error} If the session is closed or the request fails.
   */
  async execute(query, accessMode = '', parameters = null) {
    if (this._closed) {
      throw new Error(
        `Session is closed. Cannot execute query. Error code: ${ERR_SESSION_CLOSED}`
      );
    }

    if (accessMode !== '' && !isAccessModeValid(accessMode.toLowerCase())) {
      throw new Error(
        `Invalid access_mode: ${accessMode}. Supported access modes are ` +
        `[${validAccessModes.join(', ')}].`
      );
    }

    // Build payload
    const payload = JSON.stringify({
      query,
      access_mode: accessMode || 'update',
      parameters: parameters || {},
    });

    let response;
    try {
      response = await httpRequest(this._queryEndpoint, {
        method: 'POST',
        body: payload,
        timeout: this._timeout,
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
      });
    } catch (e) {
      throw new Error(
        `Could not execute query: ${query}. Error code: ${ERR_NETWORK}. ` +
        `Details: ${e.message}`
      );
    }

    if (response.statusCode !== 200) {
      throw new Error(
        `Failed to execute query: ${query}. Http code: ${response.statusCode}, ` +
        `Response: ${response.body.toString()}`
      );
    }

    // The response body is a protobuf-serialized QueryResponse.
    // Use the native binding to deserialize it.
    if (!nativeBinding || !nativeBinding.NodeQueryResult) {
      throw new Error(
        'Native binding is required for Session but was not loaded. ' +
        'Please ensure the neug native addon is built and available.'
      );
    }
    const nativeResult = nativeBinding.NodeQueryResult.fromString(response.body);
    return new QueryResult(nativeResult);
  }

  /**
   * Get the service status of the NeuG server.
   * @returns {Promise<Object>} The server status as a JSON object.
   */
  async serviceStatus() {
    const response = await httpRequest(this._statusEndpoint, {
      method: 'GET',
      timeout: this._timeout,
    });
    return JSON.parse(response.body.toString());
  }

  /**
   * Get the schema of the NeuG database from the server.
   * @returns {Promise<Object>} The schema as a JSON object.
   */
  async getSchema() {
    const response = await httpRequest(this._schemaEndpoint, {
      method: 'GET',
      timeout: this._timeout,
    });
    return JSON.parse(response.body.toString());
  }
}

module.exports = { Session };
