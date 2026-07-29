/**
 * Generated from `proto/error.proto` — DO NOT EDIT MANUALLY.
 *
 * This file is checked into the repository so end users never need to run
 * protoc or install any extra tooling.
 *
 * ── Regeneration (developer only) ────────────────────────────────────────
 *
 *   make proto          # from tools/nodejs_bind/
 *   # or equivalently:
 *   node scripts/generate_proto.js
 *
 * ──────────────────────────────────────────────────────────────────────────
 */

'use strict';

// Code enum — parsed from proto/error.proto.
// Forward lookup:  Code.ERR_INVALID_ARGUMENT === 1009
// Reverse lookup:  Code[1009] === "ERR_INVALID_ARGUMENT"  (via prototype chain)
const Code = Object.create(
  // reverse map: number → name (on prototype)
  Object.fromEntries([
    [   0, 'OK'],
    [1001, 'ERR_PERMISSION'],
    [1002, 'ERR_VERSION_MISMATCHED'],
    [1003, 'ERR_DIRECTORY_NOT_EXIST'],
    [1004, 'ERR_DATABASE_LOCKED'],
    [1005, 'ERR_DISK_SPACE_EXHAUSTED'],
    [1006, 'ERR_CORRUPTION_DETECTED'],
    [1007, 'ERR_INVALID_PATH'],
    [1008, 'ERR_CONFIG_INVALID'],
    [1009, 'ERR_INVALID_ARGUMENT'],
    [1010, 'ERR_NOT_FOUND'],
    [1011, 'ERR_NOT_SUPPORTED'],
    [1012, 'ERR_INTERNAL_ERROR'],
    [1013, 'ERR_ILLEGAL_OPERATION'],
    [1014, 'ERR_IO_ERROR'],
    [1015, 'ERR_BAD_ENCODING'],
    [1016, 'ERR_INVALID_FILE'],
    [1017, 'ERR_EXTENSION'],
    [1018, 'ERR_NO_CHECKPOINT'],
    [2001, 'ERR_NETWORK'],
    [2002, 'ERR_SESSION_CLOSED'],
    [2003, 'ERR_CONNECTION_CLOSED'],
    [2004, 'ERR_POOL_EXHAUSTED'],
    [2005, 'ERR_SERVICE_UNAVAILABLE'],
    [2006, 'ERR_LOAD_OVERFLOW'],
    [2007, 'ERR_CONNECTION_ERROR'],
    [3000, 'ERR_COMPILATION'],
    [3001, 'ERR_QUERY_EXECUTION'],
    [3002, 'ERR_QUERY_SYNTAX'],
    [3003, 'ERR_QUERY_TIMEOUT'],
    [3004, 'ERR_CONCURRENT_WRITE'],
    [3005, 'ERR_CODEGEN_ERROR'],
    [3006, 'ERR_EMPTY_RESULT'],
    [3007, 'ERR_NOT_INITIALIZED'],
    [4001, 'ERR_TX_STATE_CONFLICT'],
    [4002, 'ERR_WAL_WRITE_FAIL'],
    [4003, 'ERR_TX_TIMEOUT'],
    [5001, 'ERR_SCHEMA_MISMATCH'],
    [5002, 'ERR_INVALID_SCHEMA'],
    [5003, 'ERR_TYPE_CONVERSION'],
    [5004, 'ERR_TYPE_OVERFLOW'],
    [5005, 'ERR_INDEX_ERROR'],
    [6001, 'ERR_PLATFORM_ABI'],
    [6002, 'ERR_PY_BIND_INIT'],
    [6003, 'ERR_ARCH_MISMATCH'],
    [6004, 'ERR_DEPLOY_DEPENDENCY'],
    [7001, 'ERR_NOT_IMPLEMENTED'],
    [9999, 'ERR_UNKNOWN'],
  ])
);

// Forward map: name → number (own properties)
Code.OK                       = 0;
Code.ERR_PERMISSION           = 1001;
Code.ERR_VERSION_MISMATCHED   = 1002;
Code.ERR_DIRECTORY_NOT_EXIST  = 1003;
Code.ERR_DATABASE_LOCKED      = 1004;
Code.ERR_DISK_SPACE_EXHAUSTED = 1005;
Code.ERR_CORRUPTION_DETECTED  = 1006;
Code.ERR_INVALID_PATH         = 1007;
Code.ERR_CONFIG_INVALID       = 1008;
Code.ERR_INVALID_ARGUMENT     = 1009;
Code.ERR_NOT_FOUND            = 1010;
Code.ERR_NOT_SUPPORTED        = 1011;
Code.ERR_INTERNAL_ERROR       = 1012;
Code.ERR_ILLEGAL_OPERATION    = 1013;
Code.ERR_IO_ERROR             = 1014;
Code.ERR_BAD_ENCODING         = 1015;
Code.ERR_INVALID_FILE         = 1016;
Code.ERR_EXTENSION            = 1017;
Code.ERR_NO_CHECKPOINT        = 1018;
Code.ERR_NETWORK              = 2001;
Code.ERR_SESSION_CLOSED       = 2002;
Code.ERR_CONNECTION_CLOSED    = 2003;
Code.ERR_POOL_EXHAUSTED       = 2004;
Code.ERR_SERVICE_UNAVAILABLE  = 2005;
Code.ERR_LOAD_OVERFLOW        = 2006;
Code.ERR_CONNECTION_ERROR     = 2007;
Code.ERR_COMPILATION          = 3000;
Code.ERR_QUERY_EXECUTION      = 3001;
Code.ERR_QUERY_SYNTAX         = 3002;
Code.ERR_QUERY_TIMEOUT        = 3003;
Code.ERR_CONCURRENT_WRITE     = 3004;
Code.ERR_CODEGEN_ERROR        = 3005;
Code.ERR_EMPTY_RESULT         = 3006;
Code.ERR_NOT_INITIALIZED      = 3007;
Code.ERR_TX_STATE_CONFLICT    = 4001;
Code.ERR_WAL_WRITE_FAIL       = 4002;
Code.ERR_TX_TIMEOUT           = 4003;
Code.ERR_SCHEMA_MISMATCH      = 5001;
Code.ERR_INVALID_SCHEMA       = 5002;
Code.ERR_TYPE_CONVERSION      = 5003;
Code.ERR_TYPE_OVERFLOW        = 5004;
Code.ERR_INDEX_ERROR          = 5005;
Code.ERR_PLATFORM_ABI         = 6001;
Code.ERR_PY_BIND_INIT         = 6002;
Code.ERR_ARCH_MISMATCH        = 6003;
Code.ERR_DEPLOY_DEPENDENCY    = 6004;
Code.ERR_NOT_IMPLEMENTED      = 7001;
Code.ERR_UNKNOWN              = 9999;

module.exports = { Code };
