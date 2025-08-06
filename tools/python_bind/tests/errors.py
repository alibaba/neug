#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# NeuG v0.1 Error Code Definitions

from neug.proto.error_pb2 import ERR_ARCH_MISMATCH
from neug.proto.error_pb2 import ERR_BAD_ENCODING
from neug.proto.error_pb2 import ERR_CODEGEN_ERROR
from neug.proto.error_pb2 import ERR_COMPILATION
from neug.proto.error_pb2 import ERR_CONCURRENT_WRITE
from neug.proto.error_pb2 import ERR_CONFIG_INVALID
from neug.proto.error_pb2 import ERR_CONNECTION_CLOSED
from neug.proto.error_pb2 import ERR_CORRUPTION_DETECTED
from neug.proto.error_pb2 import ERR_DATABASE_LOCKED
from neug.proto.error_pb2 import ERR_DEPLOY_DEPENDENCY
from neug.proto.error_pb2 import ERR_DIRECTORY_NOT_EXIST
from neug.proto.error_pb2 import ERR_DISK_SPACE_EXHAUSTED
from neug.proto.error_pb2 import ERR_ILLEGAL_OPERATION
from neug.proto.error_pb2 import ERR_INTERNAL_ERROR
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_INVALID_FILE
from neug.proto.error_pb2 import ERR_INVALID_PATH
from neug.proto.error_pb2 import ERR_INVALID_SCHEMA
from neug.proto.error_pb2 import ERR_IO_ERROR
from neug.proto.error_pb2 import ERR_LOAD_OVERFLOW
from neug.proto.error_pb2 import ERR_NETWORK
from neug.proto.error_pb2 import ERR_NOT_FOUND
from neug.proto.error_pb2 import ERR_NOT_IMPLEMENTED
from neug.proto.error_pb2 import ERR_NOT_SUPPORTED
from neug.proto.error_pb2 import ERR_PERMISSION
from neug.proto.error_pb2 import ERR_PLATFORM_ABI
from neug.proto.error_pb2 import ERR_POOL_EXHAUSTED
from neug.proto.error_pb2 import ERR_PY_BIND_INIT
from neug.proto.error_pb2 import ERR_QUERY_EXECUTION
from neug.proto.error_pb2 import ERR_QUERY_SYNTAX
from neug.proto.error_pb2 import ERR_QUERY_TIMEOUT
from neug.proto.error_pb2 import ERR_SCHEMA_MISMATCH
from neug.proto.error_pb2 import ERR_SERVICE_UNAVAILABLE
from neug.proto.error_pb2 import ERR_SESSION_CLOSED
from neug.proto.error_pb2 import ERR_TX_STATE_CONFLICT
from neug.proto.error_pb2 import ERR_TX_TIMEOUT
from neug.proto.error_pb2 import ERR_TYPE_CONVERSION
from neug.proto.error_pb2 import ERR_TYPE_OVERFLOW
from neug.proto.error_pb2 import ERR_UNKNOWN
from neug.proto.error_pb2 import ERR_VERSION_MISMATCHED
from neug.proto.error_pb2 import ERR_WAL_WRITE_FAIL

# Error strings mapping for human-readable error messages
ERROR_STRINGS = {
    ERR_PERMISSION: "Permission denied",
    ERR_VERSION_MISMATCHED: "version mismatch",
    ERR_DIRECTORY_NOT_EXIST: "No such file or directory",
    ERR_DATABASE_LOCKED: "Failed to lock",
    ERR_DISK_SPACE_EXHAUSTED: "No space left",
    ERR_CORRUPTION_DETECTED: "corruption detected",
    ERR_INVALID_PATH: "invalid path",
    ERR_CONFIG_INVALID: "Invalid config",
    ERR_INVALID_ARGUMENT: "Invalid argument",
    ERR_NOT_FOUND: "not found",
    ERR_NOT_SUPPORTED: "not supported",
    ERR_INTERNAL_ERROR: "internal error",
    ERR_ILLEGAL_OPERATION: "illegal operation",
    ERR_IO_ERROR: "io error",
    ERR_BAD_ENCODING: "bad encoding",
    ERR_INVALID_FILE: "Provided path is not a file",
    ERR_NETWORK: "Could not connect",
    ERR_SESSION_CLOSED: "Session is closed",
    ERR_CONNECTION_CLOSED: "Connection is closed",
    ERR_POOL_EXHAUSTED: "connection pool exhausted",
    ERR_SERVICE_UNAVAILABLE: "service unavailable",
    ERR_LOAD_OVERFLOW: "load overflow",
    ERR_COMPILATION: "compilation error",
    ERR_QUERY_EXECUTION: "execution error",
    ERR_QUERY_SYNTAX: "3002",
    ERR_QUERY_TIMEOUT: "query timeout",
    ERR_CONCURRENT_WRITE: "concurrent write conflict",
    ERR_CODEGEN_ERROR: "codegen error",
    ERR_TX_STATE_CONFLICT: "a read-write connection constructed",
    ERR_WAL_WRITE_FAIL: "wal write fail",
    ERR_TX_TIMEOUT: "transaction timeout",
    ERR_SCHEMA_MISMATCH: "Schema mismatch",
    ERR_INVALID_SCHEMA: "5002",
    ERR_TYPE_CONVERSION: "Implicit cast is not supported.",
    ERR_TYPE_OVERFLOW: "Overflow exception",
    ERR_PLATFORM_ABI: "platform abi error",
    ERR_PY_BIND_INIT: "python binding init error",
    ERR_ARCH_MISMATCH: "arch mismatch",
    ERR_DEPLOY_DEPENDENCY: "deploy dependency error",
    ERR_NOT_IMPLEMENTED: "not implemented",
    ERR_UNKNOWN: "unknown error",
}
