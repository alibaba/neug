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

#pragma once

#include <exception>
#include <string>

#include "neug/utils/api.h"

namespace neug {
namespace interactive {
enum Code : int;
}  // namespace interactive

using StatusCode = neug::interactive::Code;
namespace exception {

class NEUG_API Exception : public std::exception {
 public:
  explicit Exception(std::string msg, neug::StatusCode error_code);
  Exception(std::string msg, std::string file_line);
  Exception(std::string msg, std::string file_line,
            neug::StatusCode error_code);

 public:
  const char* what() const noexcept override {
    return exception_message_.c_str();
  }

 protected:
  std::string exception_message_;
};

class NEUG_API PermissionDeniedException : public Exception {
 public:
  explicit PermissionDeniedException(const std::string& msg);

  PermissionDeniedException(const std::string& msg,
                            const std::string& file_line);
};

class NEUG_API DatabaseLockedException : public Exception {
 public:
  explicit DatabaseLockedException(const std::string& msg);

  DatabaseLockedException(const std::string& msg, const std::string& file_line);
};

class NEUG_API InvalidArgumentException : public Exception {
 public:
  explicit InvalidArgumentException(const std::string& msg);

  InvalidArgumentException(const std::string& msg,
                           const std::string& file_line);
};

class NEUG_API BinderException : public Exception {
 public:
  explicit BinderException(const std::string& msg);

  BinderException(const std::string& msg, const std::string& file_line);
};

class NEUG_API CatalogException : public Exception {
 public:
  explicit CatalogException(const std::string& msg);

  CatalogException(const std::string& msg, const std::string& file_line);
};

class NEUG_API CheckpointException : public Exception {
 public:
  explicit CheckpointException(const std::exception& e);

  explicit CheckpointException(const std::string& msg);
};

class NEUG_API ConnectionException : public Exception {
 public:
  explicit ConnectionException(const std::string& msg);

  ConnectionException(const std::string& msg, const std::string& file_line);
};

class NEUG_API ConversionException : public Exception {
 public:
  explicit ConversionException(const std::string& msg);

  ConversionException(const std::string& msg, const std::string& file_line);
};

class NEUG_API QueryExecutionError : public Exception {
 public:
  explicit QueryExecutionError(const std::string& msg);

  QueryExecutionError(const std::string& msg, const std::string& file_line);
};

class NEUG_API CopyException : public Exception {
 public:
  explicit CopyException(const std::string& msg);

  CopyException(const std::string& msg, const std::string& file_line);
};

class NEUG_API IndexException : public Exception {
 public:
  explicit IndexException(const std::string& msg);

  IndexException(const std::string& msg, const std::string& file_line);
};

class NEUG_API ExtensionException : public Exception {
 public:
  explicit ExtensionException(const std::string& msg);

  ExtensionException(const std::string& msg, const std::string& file_line);
};

class NEUG_API InternalException : public Exception {
 public:
  explicit InternalException(const std::string& msg);

  InternalException(const std::string& msg, const std::string& file_line);
};

class NEUG_API InterruptException : public Exception {
 public:
  explicit InterruptException();
  InterruptException(const std::string& msg, const std::string& file_line);
};

class NEUG_API IOException : public Exception {
 public:
  explicit IOException(const std::string& msg);

  IOException(const std::string& msg, const std::string& file_line);
};

class NEUG_API NotImplementedException : public Exception {
 public:
  explicit NotImplementedException(const std::string& msg);

  NotImplementedException(const std::string& msg, const std::string& file_line);
};

class NEUG_API NotFoundException : public Exception {
 public:
  explicit NotFoundException(const std::string& msg);

  NotFoundException(const std::string& msg, const std::string& file_line);
};

class NEUG_API NotSupportedException : public Exception {
 public:
  explicit NotSupportedException(const std::string& msg);

  NotSupportedException(const std::string& msg, const std::string& file_line);
};

class NEUG_API OverflowException : public Exception {
 public:
  explicit OverflowException(const std::string& msg);

  OverflowException(const std::string& msg, const std::string& file_line);
};

class NEUG_API SchemaMismatchException : public Exception {
 public:
  explicit SchemaMismatchException(const std::string& msg);

  SchemaMismatchException(const std::string& msg, const std::string& file_line);
};

class NEUG_API ParserException : public Exception {
 public:
  static constexpr const char* ERROR_PREFIX = "Parser exception: ";

  explicit ParserException(const std::string& msg);

  ParserException(const std::string& msg, const std::string& file_line);
};

class NEUG_API RuntimeError : public Exception {
 public:
  explicit RuntimeError(const std::string& msg);

  RuntimeError(const std::string& msg, const std::string& file_line);
};

class NEUG_API StorageException : public Exception {
 public:
  explicit StorageException(const std::string& msg);

  StorageException(const std::string& msg, const std::string& file_line);
};

class TestException : public Exception {
 public:
  explicit TestException(const std::string& msg);

  TestException(const std::string& msg, const std::string& file_line);
};

class NEUG_API TransactionManagerException : public Exception {
 public:
  explicit TransactionManagerException(const std::string& msg);

  TransactionManagerException(const std::string& msg,
                              const std::string& file_line);
};

class NEUG_API TxStateConflictException : public Exception {
 public:
  explicit TxStateConflictException(const std::string& msg);

  TxStateConflictException(const std::string& msg,
                           const std::string& file_line);
};

class NEUG_API PropertyNotFoundException : public Exception {
 public:
  explicit PropertyNotFoundException(const std::string& msg);

  PropertyNotFoundException(const std::string& msg,
                            const std::string& file_line);
};

}  // namespace exception

}  // namespace neug

// Define guard to get the file and line number where the exception is thrown
#define THROW_EXCEPTION_WITH_FILE_LINE(msg)                         \
  throw neug::exception::Exception(                                 \
      msg, std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
               " func: " + std::string(__FUNCTION__))

// Define a template guard with exception name and msg given
#define THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(exception_type, msg) \
  throw neug::exception::exception_type(                             \
      msg, std::string(__FILE__) + ":" + std::to_string(__LINE__) +  \
               " func: " + std::string(__FUNCTION__))

#define THROW_INVALID_ARGUMENT_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(InvalidArgumentException, msg)

#define THROW_PERMISSION_DENIED(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(PermissionDeniedException, msg)

#define THROW_DATABASE_LOCKED_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(DatabaseLockedException, msg)

#define THROW_BINDER_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(BinderException, msg)

#define THROW_CATALOG_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(CatalogException, msg)

#define THROW_CHECKPOINT_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(CheckpointException, msg)

#define THROW_CONNECTION_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(ConnectionException, msg)

#define THROW_CONVERSION_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(ConversionException, msg)

#define THROW_QUERY_EXECUTION_ERROR(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(QueryExecutionError, msg)

#define THROW_COPY_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(CopyException, msg)

#define THROW_INDEX_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(IndexException, msg)

#define THROW_EXTENSION_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(ExtensionException, msg)

#define THROW_INTERNAL_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(InternalException, msg)

#define THROW_INTERRUPT_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(InterruptException, msg)

#define THROW_IO_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(IOException, msg)

#define THROW_NOT_IMPLEMENTED_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(NotImplementedException, msg)

#define THROW_NOT_FOUND_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(NotFoundException, msg)

#define THROW_NOT_SUPPORTED_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(NotSupportedException, msg)

#define THROW_OVERFLOW_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(OverflowException, msg)

#define THROW_PARSER_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(ParserException, msg)

#define THROW_RUNTIME_ERROR(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(RuntimeError, msg)

#define THROW_STORAGE_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(StorageException, msg)

#define THROW_TEST_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(TestException, msg)

#define THROW_TRANSACTION_MANAGER_EXCEPTION(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(TransactionManagerException, msg)

#define THROW_SCHEMA_MISMATCH(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(SchemaMismatchException, msg)

#define THROW_TX_STATE_CONFLICT(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(TxStateConflictException, msg)

#define THROW_PROPERTY_NOT_FOUND(msg) \
  THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(PropertyNotFoundException, msg)

#define THROW_IF_ARROW_NOT_OK(expr)                             \
  do {                                                          \
    auto status = (expr);                                       \
    if (!status.ok()) {                                         \
      THROW_RUNTIME_ERROR("Arrow error: " + status.ToString()); \
    }                                                           \
  } while (0)
