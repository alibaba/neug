# NeuG Error Codes

NeuG defines all runtime/service error codes in the protobuf file [`error.proto`](https://github.com/alibaba/neug/blob/main/proto/error.proto). The enums in that file are compiled into every component, so developers should treat the definitions there as the single source of truth. The table below summarizes each code and its meaning for quick reference while debugging or surfacing errors.

| Category | Code | Numeric Value | Meaning |
| --- | --- | --- | --- |
| General | `OK` | 0 | Successful operation; no error. |
| General | `ERR_PERMISSION` | 1001 | Operation blocked due to missing permissions. |
| General | `ERR_VERSION_MISMATCHED` | 1002 | Binary/database versions are incompatible with the data directory. |
| General | `ERR_DIRECTORY_NOT_EXIST` | 1003 | Target directory path does not exist. |
| General | `ERR_DATABASE_LOCKED` | 1004 | Data directory is locked by another process. |
| General | `ERR_DISK_SPACE_EXHAUSTED` | 1005 | Insufficient disk space to continue. |
| General | `ERR_CORRUPTION_DETECTED` | 1006 | Files appear corrupted or not in the expected format. |
| General | `ERR_INVALID_PATH` | 1007 | Provided filesystem path is invalid. |
| General | `ERR_CONFIG_INVALID` | 1008 | Configuration file or values are malformed. |
| General | `ERR_INVALID_ARGUMENT` | 1009 | API/input arguments are missing or malformed. |
| General | `ERR_NOT_FOUND` | 1010 | Requested resource (vertex, file, etc.) cannot be found. |
| General | `ERR_NOT_SUPPORTED` | 1011 | Feature or operation is not implemented for the current context. |
| General | `ERR_INTERNAL_ERROR` | 1012 | Unexpected internal failure; inspect logs for stacktrace. |
| General | `ERR_ILLEGAL_OPERATION` | 1013 | Operation is not allowed in the current state/configuration. |
| General | `ERR_IO_ERROR` | 1014 | Underlying filesystem or device I/O failure. |
| General | `ERR_BAD_ENCODING` | 1015 | Unsupported encoding/decoding encountered. |
| General | `ERR_INVALID_FILE` | 1016 | Referenced file does not exist or is unreadable. |
| General | `ERR_EXTENSION` | 1017 | Failure while loading or executing an extension. |
| Networking / Sessions | `ERR_NETWORK` | 2001 | Generic network transport error. |
| Networking / Sessions | `ERR_SESSION_CLOSED` | 2002 | Session handle is closed and no longer usable. |
| Networking / Sessions | `ERR_CONNECTION_CLOSED` | 2003 | Connection dropped (server shutdown or DB closed). |
| Networking / Sessions | `ERR_POOL_EXHAUSTED` | 2004 | Connection/session pool ran out of available entries. |
| Networking / Sessions | `ERR_SERVICE_UNAVAILABLE` | 2005 | Service is offline or not yet ready. |
| Networking / Sessions | `ERR_LOAD_OVERFLOW` | 2006 | Service is overloaded; clients should retry/back off. |
| Networking / Sessions | `ERR_CONNECTION_ERROR` | 2007 | Failure establishing or maintaining a connection. |
| Query Compilation & Execution | `ERR_COMPILATION` | 3000 | Failure during query compilation stage. |
| Query Compilation & Execution | `ERR_QUERY_EXECUTION` | 3001 | Generic runtime failure while executing query plan. |
| Query Compilation & Execution | `ERR_QUERY_SYNTAX` | 3002 | Query syntax or semantics invalid. |
| Query Compilation & Execution | `ERR_QUERY_TIMEOUT` | 3003 | Query exceeded configured execution time limit. |
| Query Compilation & Execution | `ERR_CONCURRENT_WRITE` | 3004 | Conflicting concurrent write detected. |
| Query Compilation & Execution | `ERR_CODEGEN_ERROR` | 3005 | Failure during query code generation. |
| Query Compilation & Execution | `ERR_EMPTY_RESULT` | 3006 | Planner inferred empty result set. |
| Query Compilation & Execution | `ERR_NOT_INITIALIZED` | 3007 | Database/session not initialized before query execution. |
| Transactions & WAL | `ERR_TX_STATE_CONFLICT` | 4001 | Transaction state conflict (e.g., invalid transitions). |
| Transactions & WAL | `ERR_WAL_WRITE_FAIL` | 4002 | Failed to append to the write-ahead log. |
| Transactions & WAL | `ERR_TX_TIMEOUT` | 4003 | Transaction exceeded the timeout limit. |
| Schema & Types | `ERR_SCHEMA_MISMATCH` | 5001 | Schema mismatch between operation and stored data. |
| Schema & Types | `ERR_INVALID_SCHEMA` | 5002 | Schema definition is invalid. |
| Schema & Types | `ERR_TYPE_CONVERSION` | 5003 | Illegal type conversion requested. |
| Schema & Types | `ERR_TYPE_OVERFLOW` | 5004 | Value cannot fit into the target data type. |
| Schema & Types | `ERR_INDEX_ERROR` | 5005 | Index/offset out of bounds. |
| Schema & Types | `ERR_PROPERTY_NOT_FOUND` | 5006 | Requested property missing in schema. |
| Deployment / Platform | `ERR_PLATFORM_ABI` | 6001 | ABI mismatch between binaries and host platform. |
| Deployment / Platform | `ERR_PY_BIND_INIT` | 6002 | Python binding initialization failed. |
| Deployment / Platform | `ERR_ARCH_MISMATCH` | 6003 | Binary architecture does not match runtime environment. |
| Deployment / Platform | `ERR_DEPLOY_DEPENDENCY` | 6004 | Missing runtime dependency during deployment. |
| Feature Gaps | `ERR_NOT_IMPLEMENTED` | 7001 | Feature placeholder; not yet implemented. |
| General | `ERR_UNKNOWN` | 9999 | Unclassified error; inspect logs for context. |

**Tip:** When adding new error codes, update `proto/error.proto` first, regenerate the protobuf outputs if necessary, and extend this document so other developers can discover the new code quickly.
