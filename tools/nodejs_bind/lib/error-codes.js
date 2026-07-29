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
 * Error codes generated from `proto/error.proto`.
 *
 * The `Code` enum is loaded from the pre-generated `lib/proto/error_pb.js`
 * (checked into the repository, so end users never need to run protoc).
 * To regenerate after modifying `proto/error.proto`, see the header
 * comment in `lib/proto/error_pb.js` or run `make proto`.
 *
 * @module error-codes
 */

'use strict';

const { Code } = require('./proto/error_pb');

// ---- Numeric error code constants (from error.proto Code enum) ----
// General errors (1001-1018)
const OK                    = Code.OK;
const ERR_PERMISSION        = Code.ERR_PERMISSION;
const ERR_VERSION_MISMATCHED = Code.ERR_VERSION_MISMATCHED;
const ERR_DIRECTORY_NOT_EXIST = Code.ERR_DIRECTORY_NOT_EXIST;
const ERR_DATABASE_LOCKED   = Code.ERR_DATABASE_LOCKED;
const ERR_DISK_SPACE_EXHAUSTED = Code.ERR_DISK_SPACE_EXHAUSTED;
const ERR_CORRUPTION_DETECTED = Code.ERR_CORRUPTION_DETECTED;
const ERR_INVALID_PATH      = Code.ERR_INVALID_PATH;
const ERR_CONFIG_INVALID    = Code.ERR_CONFIG_INVALID;
const ERR_INVALID_ARGUMENT  = Code.ERR_INVALID_ARGUMENT;
const ERR_NOT_FOUND         = Code.ERR_NOT_FOUND;
const ERR_NOT_SUPPORTED     = Code.ERR_NOT_SUPPORTED;
const ERR_INTERNAL_ERROR    = Code.ERR_INTERNAL_ERROR;
const ERR_ILLEGAL_OPERATION = Code.ERR_ILLEGAL_OPERATION;
const ERR_IO_ERROR          = Code.ERR_IO_ERROR;
const ERR_BAD_ENCODING      = Code.ERR_BAD_ENCODING;
const ERR_INVALID_FILE      = Code.ERR_INVALID_FILE;
const ERR_EXTENSION         = Code.ERR_EXTENSION;
const ERR_NO_CHECKPOINT     = Code.ERR_NO_CHECKPOINT;

// Network errors (2001-2007)
const ERR_NETWORK           = Code.ERR_NETWORK;
const ERR_SESSION_CLOSED    = Code.ERR_SESSION_CLOSED;
const ERR_CONNECTION_CLOSED = Code.ERR_CONNECTION_CLOSED;
const ERR_POOL_EXHAUSTED    = Code.ERR_POOL_EXHAUSTED;
const ERR_SERVICE_UNAVAILABLE = Code.ERR_SERVICE_UNAVAILABLE;
const ERR_LOAD_OVERFLOW     = Code.ERR_LOAD_OVERFLOW;
const ERR_CONNECTION_ERROR  = Code.ERR_CONNECTION_ERROR;

// Query errors (3000-3007)
const ERR_COMPILATION       = Code.ERR_COMPILATION;
const ERR_QUERY_EXECUTION   = Code.ERR_QUERY_EXECUTION;
const ERR_QUERY_SYNTAX      = Code.ERR_QUERY_SYNTAX;
const ERR_QUERY_TIMEOUT     = Code.ERR_QUERY_TIMEOUT;
const ERR_CONCURRENT_WRITE  = Code.ERR_CONCURRENT_WRITE;
const ERR_CODEGEN_ERROR     = Code.ERR_CODEGEN_ERROR;
const ERR_EMPTY_RESULT      = Code.ERR_EMPTY_RESULT;
const ERR_NOT_INITIALIZED   = Code.ERR_NOT_INITIALIZED;

// Transaction errors (4001-4003)
const ERR_TX_STATE_CONFLICT = Code.ERR_TX_STATE_CONFLICT;
const ERR_WAL_WRITE_FAIL    = Code.ERR_WAL_WRITE_FAIL;
const ERR_TX_TIMEOUT        = Code.ERR_TX_TIMEOUT;

// Schema errors (5001-5005)
const ERR_SCHEMA_MISMATCH   = Code.ERR_SCHEMA_MISMATCH;
const ERR_INVALID_SCHEMA    = Code.ERR_INVALID_SCHEMA;
const ERR_TYPE_CONVERSION   = Code.ERR_TYPE_CONVERSION;
const ERR_TYPE_OVERFLOW     = Code.ERR_TYPE_OVERFLOW;
const ERR_INDEX_ERROR       = Code.ERR_INDEX_ERROR;

// Deployment errors (6001-6004)
const ERR_PLATFORM_ABI      = Code.ERR_PLATFORM_ABI;
const ERR_PY_BIND_INIT      = Code.ERR_PY_BIND_INIT;
const ERR_ARCH_MISMATCH     = Code.ERR_ARCH_MISMATCH;
const ERR_DEPLOY_DEPENDENCY = Code.ERR_DEPLOY_DEPENDENCY;

// Not implemented (7001)
const ERR_NOT_IMPLEMENTED   = Code.ERR_NOT_IMPLEMENTED;

// Unknown (9999)
const ERR_UNKNOWN           = Code.ERR_UNKNOWN;

/**
 * Get the name of an error code.
 * @param {number} code - The numeric error code.
 * @returns {string} The error code name, or 'ERR_UNKNOWN' if not found.
 */
function codeName(code) {
  // Code enum provides reverse mapping: number → name via prototype chain
  return Code[code] || 'ERR_UNKNOWN';
}

module.exports = {
  // General errors
  OK,
  ERR_PERMISSION,
  ERR_VERSION_MISMATCHED,
  ERR_DIRECTORY_NOT_EXIST,
  ERR_DATABASE_LOCKED,
  ERR_DISK_SPACE_EXHAUSTED,
  ERR_CORRUPTION_DETECTED,
  ERR_INVALID_PATH,
  ERR_CONFIG_INVALID,
  ERR_INVALID_ARGUMENT,
  ERR_NOT_FOUND,
  ERR_NOT_SUPPORTED,
  ERR_INTERNAL_ERROR,
  ERR_ILLEGAL_OPERATION,
  ERR_IO_ERROR,
  ERR_BAD_ENCODING,
  ERR_INVALID_FILE,
  ERR_EXTENSION,
  ERR_NO_CHECKPOINT,
  // Network errors
  ERR_NETWORK,
  ERR_SESSION_CLOSED,
  ERR_CONNECTION_CLOSED,
  ERR_POOL_EXHAUSTED,
  ERR_SERVICE_UNAVAILABLE,
  ERR_LOAD_OVERFLOW,
  ERR_CONNECTION_ERROR,
  // Query errors
  ERR_COMPILATION,
  ERR_QUERY_EXECUTION,
  ERR_QUERY_SYNTAX,
  ERR_QUERY_TIMEOUT,
  ERR_CONCURRENT_WRITE,
  ERR_CODEGEN_ERROR,
  ERR_EMPTY_RESULT,
  ERR_NOT_INITIALIZED,
  // Transaction errors
  ERR_TX_STATE_CONFLICT,
  ERR_WAL_WRITE_FAIL,
  ERR_TX_TIMEOUT,
  // Schema errors
  ERR_SCHEMA_MISMATCH,
  ERR_INVALID_SCHEMA,
  ERR_TYPE_CONVERSION,
  ERR_TYPE_OVERFLOW,
  ERR_INDEX_ERROR,
  // Deployment errors
  ERR_PLATFORM_ABI,
  ERR_PY_BIND_INIT,
  ERR_ARCH_MISMATCH,
  ERR_DEPLOY_DEPENDENCY,
  // Not implemented
  ERR_NOT_IMPLEMENTED,
  // Unknown
  ERR_UNKNOWN,
  // Code enum (name → number, with reverse mapping number → name via prototype)
  Code,
  codeName,
};
