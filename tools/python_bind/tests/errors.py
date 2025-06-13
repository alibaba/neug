# NeuG v0.1 Error Code 定义

from nexg.proto.error_pb2 import *


ERROR_STRINGS = {
    ERR_PERMISSION: "Permission denied",
    ERR_VERSION_MISMATCHED: "version mismatch",
    ERR_DIRECTORY_NOT_EXIST: "No such file or directory",
    ERR_DATABASE_LOCKED: "Failed to lock",
    ERR_DISK_SPACE_EXHAUSTED: "No space left",
    ERR_CORRUPTION_DETECTED: "corruption detected",
    ERR_INVALID_PATH: "invalid path",
    ERR_CONFIG_INVALID: "invalid config",
    ERR_INVALID_ARGUMENT: "invalid argument",
    ERR_NOT_FOUND: "not found",
    ERR_NOT_SUPPORTED: "not supported",
    ERR_INTERNAL_ERROR: "internal error",
    ERR_ILLEGAL_OPERATION: "illegal operation",
    ERR_IO_ERROR: "io error",
    ERR_BAD_ENCODING: "bad encoding",

    ERR_NETWORK: "network error",
    ERR_CONNECTION_BROKEN: "connection broken",
    ERR_POOL_EXHAUSTED: "connection pool exhausted",
    ERR_SERVICE_UNAVAILABLE: "service unavailable",
    ERR_LOAD_OVERFLOW: "load overflow",

    ERR_COMPILATION: "compilation error",
    ERR_QUERY_EXECUTION: "execution error",
    ERR_QUERY_SYNTAX: "syntax error",
    ERR_QUERY_TIMEOUT: "query timeout",
    ERR_CONCURRENT_WRITE: "concurrent write conflict",
    ERR_CODEGEN_ERROR: "codegen error",

    ERR_TX_STATE_CONFLICT: "a read-write connection constructed",
    ERR_WAL_WRITE_FAIL: "wal write fail",
    ERR_TX_TIMEOUT: "transaction timeout",

    ERR_SCHEMA_MISMATCH: "schema mismatch",
    ERR_INVALID_SCHEMA: "invalid schema",
    ERR_TYPE_CONVERSION: "type conversion error",

    ERR_PLATFORM_ABI: "platform abi error",
    ERR_PY_BIND_INIT: "python binding init error",
    ERR_ARCH_MISMATCH: "arch mismatch",
    ERR_DEPLOY_DEPENDENCY: "deploy dependency error",

    ERR_NOT_IMPLEMENTED: "not implemented",
    ERR_UNKNOWN: "unknown error",
}
