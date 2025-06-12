# NeuG v0.1 Error Code 定义

# Permission denied, e.g., trying to access a file without read permission.
ERR_PERMISSION = 1001
# database version is not compatible with the data directory.
ERR_VERSION_MISMATCHED = 1002
# The directory does not exist.
ERR_DIRECTORY_NOT_EXIST = 1003
# The database is locked, e.g., another process is writing to the database.
ERR_DATABASE_LOCKED = 1004
# Disk space is exhausted, e.g., no more space to write to the database.
ERR_DISK_SPACE_EXHAUSTED = 1005
# The database file is corrupted, e.g., the data file is not in the expected format.
ERR_CORRUPTION_DETECTED = 1006
# The path is invalid, e.g., the path is not a valid file or directory.
ERR_INVALID_PATH = 1007
# Configuration is invalid, e.g., the configuration file is not in the expected format.
ERR_CONFIG_INVALID = 1008
# Arguments of operations is invalid / in a wrong format.
ERR_INVALID_ARGUMENT = 1009
# Required resources cannot be found.
ERR_NOT_FOUND = 1010
# The operation is not supported.
ERR_NOT_SUPPORTED = 1011
# Internal error, e.g., an unexpected error occurred.
ERR_INTERNAL_ERROR = 1012
# Illegal operation, e.g., trying to perform an operation that is not allowed.
ERR_ILLEGAL_OPERATION = 1013
# I/O error, e.g., an error occurred while reading or writing to a file.
ERR_IO_ERROR = 1014
# Bad encoding, e.g., the data is not in the expected encoding format.
ERR_BAD_ENCODING = 1015

# Network related errors
ERR_NETWORK = 2001
# Connection broken, e.g., the connection to the server is lost, or connection to the database is lost(database closed).
ERR_CONNECTION_BROKEN = 2002
# Pool exhausted, e.g., the connection pool is exhausted and no more connections can be created.
ERR_POOL_EXHAUSTED = 2003
# Service unavailable, e.g., the service is not available currently, or the service is not running.
ERR_SERVICE_UNAVAILABLE = 2004
# Load overflow, e.g., the system is overloaded and cannot handle more requests.
ERR_LOAD_OVERFLOW = 2005

# Error during query compilation
ERR_COMPILATION = 3000
# Unknow error during query execution
ERR_QUERY_EXECUTION = 3001
# Error during query syntax parsing
ERR_QUERY_SYNTAX = 3002
# Query execution timeout, e.g., the query takes too long to execute.
ERR_QUERY_TIMEOUT = 3003
# Error during concurrent write, e.g., multiple queries trying to write to the same data at the same time.
ERR_CONCURRENT_WRITE = 3004
# Error during query code generation
ERR_CODEGEN_ERROR = 3005

# Error during transaction management
ERR_TX_STATE_CONFLICT = 4001
# Error during write-ahead log (WAL) writing, e.g., the WAL file cannot be written to.
ERR_WAL_WRITE_FAIL = 4002
# Transaction timeout, e.g., the transaction takes too long to commit.
ERR_TX_TIMEOUT = 4003

# Error during schema management
ERR_SCHEMA_MISMATCH = 5001
# Error during schema validation, e.g., the schema is not valid or does not match the data.
ERR_INVALID_SCHEMA = 5002
# Error during type conversion, e.g., trying to convert a value to a type that is not compatible.
ERR_TYPE_CONVERSION = 5003

# Deployment related errors
ERR_PLATFORM_ABI = 6001
ERR_PY_BIND_INIT = 6002
ERR_ARCH_MISMATCH = 6003
ERR_DEPLOY_DEPENDENCY = 6004

# Functionality not implemented yet
ERR_NOT_IMPLEMENTED = 7001

ERR_UNKNOWN = 9999

# 兼容字符串映射（可根据实际报错内容调整）
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
    # ... 其他可补充 ...
}
