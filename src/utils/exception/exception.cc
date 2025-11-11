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

#include "neug/utils/exception/exception.h"
#include <utility>
#include "neug/generated/proto/plan/error.pb.h"

#ifdef NEUG_BACKTRACE
#include <cpptrace/cpptrace.hpp>
#endif

namespace gs {
namespace exception {

Exception::Exception(std::string msg, gs::StatusCode error_code)
    : exception(), exception_message_(std::move(msg)) {
#ifdef NEUG_BACKTRACE
  cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
  exception_message_ += ", Error Code: " + std::to_string(error_code);
}

Exception::Exception(std::string msg, std::string file_line)
    : exception(), exception_message_(msg + " at " + file_line) {
#ifdef NEUG_BACKTRACE
  cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
  exception_message_ +=
      ", Error Code: " + std::to_string(gs::StatusCode::ERR_UNKNOWN);
}

Exception::Exception(std::string msg, std::string file_line,
                     gs::StatusCode error_code)
    : exception(), exception_message_(msg + " at " + file_line) {
#ifdef NEUG_BACKTRACE
  cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
  exception_message_ += ", Error Code: " + std::to_string(error_code);
}

PermissionDeniedException::PermissionDeniedException(const std::string& msg)
    : Exception("Permission denied: " + msg, gs::StatusCode::ERR_PERMISSION) {}

PermissionDeniedException::PermissionDeniedException(
    const std::string& msg, const std::string& file_line)
    : Exception("Permission denied: " + msg, file_line,
                gs::StatusCode::ERR_PERMISSION) {}

DatabaseLockedException::DatabaseLockedException(const std::string& msg)
    : Exception("Database locked: " + msg,
                gs::StatusCode::ERR_DATABASE_LOCKED) {}

DatabaseLockedException::DatabaseLockedException(const std::string& msg,
                                                 const std::string& file_line)
    : Exception("Database locked: " + msg, file_line,
                gs::StatusCode::ERR_DATABASE_LOCKED) {}

InvalidArgumentException::InvalidArgumentException(const std::string& msg)
    : Exception("Invalid argument: " + msg,
                gs::StatusCode::ERR_INVALID_ARGUMENT) {}
InvalidArgumentException::InvalidArgumentException(const std::string& msg,
                                                   const std::string& file_line)
    : Exception("Invalid argument: " + msg, file_line,
                gs::StatusCode::ERR_INVALID_ARGUMENT) {}

BinderException::BinderException(const std::string& msg)
    : Exception("Binder exception: " + msg, gs::StatusCode::ERR_COMPILATION) {}

BinderException::BinderException(const std::string& msg,
                                 const std::string& file_line)
    : Exception("Binder exception: " + msg, file_line,
                gs::StatusCode::ERR_COMPILATION) {}

CatalogException::CatalogException(const std::string& msg)
    : Exception("Catalog exception: " + msg, gs::StatusCode::ERR_COMPILATION) {}
CatalogException::CatalogException(const std::string& msg,
                                   const std::string& file_line)
    : Exception("Catalog exception: " + msg, file_line,
                gs::StatusCode::ERR_COMPILATION) {}
CheckpointException::CheckpointException(const std::exception& e)
    : Exception(e.what(), gs::StatusCode::ERR_INTERNAL_ERROR) {}

CheckpointException::CheckpointException(const std::string& msg)
    : Exception("Checkpoint exception: " + msg,
                gs::StatusCode::ERR_INTERNAL_ERROR) {}

ConnectionException::ConnectionException(const std::string& msg)
    : Exception("Connection exception: " + msg,
                gs::StatusCode::ERR_CONNECTION_ERROR) {}
ConnectionException::ConnectionException(const std::string& msg,
                                         const std::string& file_line)
    : Exception("Connection exception: " + msg, file_line,
                gs::StatusCode::ERR_CONNECTION_ERROR) {}

ConversionException::ConversionException(const std::string& msg)
    : Exception("Conversion exception: " + msg,
                gs::StatusCode::ERR_TYPE_CONVERSION) {}
ConversionException::ConversionException(const std::string& msg,
                                         const std::string& file_line)
    : Exception("Conversion exception: " + msg, file_line,
                gs::StatusCode::ERR_TYPE_CONVERSION) {}

QueryExecutionError::QueryExecutionError(const std::string& msg)
    : Exception("Query execution error: " + msg,
                gs::StatusCode::ERR_QUERY_EXECUTION) {}
QueryExecutionError::QueryExecutionError(const std::string& msg,
                                         const std::string& file_line)
    : Exception("Query execution error: " + msg, file_line,
                gs::StatusCode::ERR_QUERY_EXECUTION) {}

CopyException::CopyException(const std::string& msg)
    : Exception("Copy exception: " + msg, gs::StatusCode::ERR_INTERNAL_ERROR) {}
CopyException::CopyException(const std::string& msg,
                             const std::string& file_line)
    : Exception("Copy exception: " + msg, file_line,
                gs::StatusCode::ERR_INTERNAL_ERROR) {}

IndexException::IndexException(const std::string& msg)
    : Exception("Index exception: " + msg, gs::StatusCode::ERR_INDEX_ERROR) {}
IndexException::IndexException(const std::string& msg,
                               const std::string& file_line)
    : Exception("Index exception: " + msg, file_line,
                gs::StatusCode::ERR_INDEX_ERROR) {}

ExtensionException::ExtensionException(const std::string& msg)
    : Exception("Extension exception: " + msg, gs::StatusCode::ERR_EXTENSION) {}
ExtensionException::ExtensionException(const std::string& msg,
                                       const std::string& file_line)
    : Exception("Extension exception: " + msg, file_line,
                gs::StatusCode::ERR_EXTENSION) {}
InternalException::InternalException(const std::string& msg)
    : Exception(msg, gs::StatusCode::ERR_INTERNAL_ERROR) {}
InternalException::InternalException(const std::string& msg,
                                     const std::string& file_line)
    : Exception(msg, file_line, gs::StatusCode::ERR_INTERNAL_ERROR) {}
InterruptException::InterruptException()
    : Exception("Interrupted.", gs::StatusCode::ERR_INTERNAL_ERROR) {}
InterruptException::InterruptException(const std::string& msg,
                                       const std::string& file_line)
    : Exception(msg, file_line, gs::StatusCode::ERR_INTERNAL_ERROR) {}
IOException::IOException(const std::string& msg)
    : Exception("IO exception: " + msg, gs::StatusCode::ERR_IO_ERROR) {}
IOException::IOException(const std::string& msg, const std::string& file_line)
    : Exception("IO exception: " + msg, file_line,
                gs::StatusCode::ERR_IO_ERROR) {}

NotImplementedException::NotImplementedException(const std::string& msg)
    : Exception(msg, gs::StatusCode::ERR_NOT_IMPLEMENTED) {}
NotImplementedException::NotImplementedException(const std::string& msg,
                                                 const std::string& file_line)
    : Exception(msg, file_line, gs::StatusCode::ERR_NOT_IMPLEMENTED) {}
NotFoundException::NotFoundException(const std::string& msg)
    : Exception("Not found exception: " + msg, gs::StatusCode::ERR_NOT_FOUND) {}
NotFoundException::NotFoundException(const std::string& msg,
                                     const std::string& file_line)
    : Exception("Not found exception: " + msg, file_line,
                gs::StatusCode::ERR_NOT_FOUND) {}

NotSupportedException::NotSupportedException(const std::string& msg)
    : Exception("Not supported: " + msg, gs::StatusCode::ERR_NOT_SUPPORTED) {}
NotSupportedException::NotSupportedException(const std::string& msg,
                                             const std::string& file_line)
    : Exception("Not supported: " + msg, file_line,
                gs::StatusCode::ERR_NOT_SUPPORTED) {}
OverflowException::OverflowException(const std::string& msg)
    : Exception("Overflow exception: " + msg,
                gs::StatusCode::ERR_TYPE_OVERFLOW) {}
OverflowException::OverflowException(const std::string& msg,
                                     const std::string& file_line)
    : Exception("Overflow exception: " + msg, file_line,
                gs::StatusCode::ERR_TYPE_OVERFLOW) {}

SchemaMismatchException::SchemaMismatchException(const std::string& msg)
    : Exception("Schema mismatch: " + msg,
                gs::StatusCode::ERR_SCHEMA_MISMATCH) {}
SchemaMismatchException::SchemaMismatchException(const std::string& msg,
                                                 const std::string& file_line)
    : Exception("Schema mismatch: " + msg, file_line,
                gs::StatusCode::ERR_SCHEMA_MISMATCH) {}

ParserException::ParserException(const std::string& msg)
    : Exception("Parser exception: " + msg, gs::StatusCode::ERR_QUERY_SYNTAX) {}
ParserException::ParserException(const std::string& msg,
                                 const std::string& file_line)
    : Exception("Parser exception: " + msg, file_line,
                gs::StatusCode::ERR_QUERY_SYNTAX) {}

RuntimeError::RuntimeError(const std::string& msg)
    : Exception("Runtime exception: " + msg,
                gs::StatusCode::ERR_INTERNAL_ERROR) {}
RuntimeError::RuntimeError(const std::string& msg, const std::string& file_line)
    : Exception("Runtime exception: " + msg, file_line,
                gs::StatusCode::ERR_INTERNAL_ERROR) {}

StorageException::StorageException(const std::string& msg)
    : Exception("Storage exception: " + msg,
                gs::StatusCode::ERR_INTERNAL_ERROR) {}
StorageException::StorageException(const std::string& msg,
                                   const std::string& file_line)
    : Exception("Storage exception: " + msg, file_line,
                gs::StatusCode::ERR_INTERNAL_ERROR) {}
TestException::TestException(const std::string& msg)
    : Exception("Test exception: " + msg, gs::StatusCode::ERR_INTERNAL_ERROR) {}
TestException::TestException(const std::string& msg,
                             const std::string& file_line)
    : Exception("Test exception: " + msg, file_line,
                gs::StatusCode::ERR_INTERNAL_ERROR) {}
TransactionManagerException::TransactionManagerException(const std::string& msg)
    : Exception(msg, gs::StatusCode::ERR_INTERNAL_ERROR) {}
TransactionManagerException::TransactionManagerException(
    const std::string& msg, const std::string& file_line)
    : Exception(msg, file_line, gs::StatusCode::ERR_INTERNAL_ERROR) {}

TxStateConflictException::TxStateConflictException(const std::string& msg)
    : Exception("Transaction state conflict: " + msg,
                gs::StatusCode::ERR_TX_STATE_CONFLICT) {}
TxStateConflictException::TxStateConflictException(const std::string& msg,
                                                   const std::string& file_line)
    : Exception("Transaction state conflict: " + msg, file_line,
                gs::StatusCode::ERR_TX_STATE_CONFLICT) {}

}  // namespace exception
}  // namespace gs
