#pragma once

#include <exception>
#include <string>

#include "neug/compiler/common/api.h"

#include "neug/proto_generated_gie/error.pb.h"

namespace gs {

using StatusCode = gs::neug::interactive::Code;
namespace exception {

class KUZU_API Exception : public std::exception {
 public:
  explicit Exception(std::string msg,
                     gs::StatusCode error_code = StatusCode::ERR_UNKNOWN);
  Exception(std::string msg, std::string file_line,
            gs::StatusCode error_code = StatusCode::ERR_UNKNOWN);

 public:
  const char* what() const noexcept override {
    return exception_message_.c_str();
  }

 protected:
  std::string exception_message_;
};

class KUZU_API PermissionDeniedException : public Exception {
 public:
  explicit PermissionDeniedException(const std::string& msg)
      : Exception("Permission denied: " + msg, StatusCode::ERR_PERMISSION){};

  PermissionDeniedException(const std::string& msg,
                            const std::string& file_line)
      : Exception("Permission denied: " + msg, file_line,
                  gs::StatusCode::ERR_PERMISSION){};
};

class KUZU_API DatabaseLockedException : public Exception {
 public:
  explicit DatabaseLockedException(const std::string& msg)
      : Exception("Database locked: " + msg, StatusCode::ERR_DATABASE_LOCKED){};

  DatabaseLockedException(const std::string& msg, const std::string& file_line)
      : Exception("Database locked: " + msg, file_line,
                  gs::StatusCode::ERR_DATABASE_LOCKED){};
};

class KUZU_API InvalidArgumentException : public Exception {
 public:
  explicit InvalidArgumentException(const std::string& msg)
      : Exception("Invalid argument: " + msg,
                  gs::StatusCode::ERR_INVALID_ARGUMENT){};

  InvalidArgumentException(const std::string& msg, const std::string& file_line)
      : Exception("Invalid argument: " + msg, file_line,
                  gs::StatusCode::ERR_INVALID_ARGUMENT){};
};

class KUZU_API BinderException : public Exception {
 public:
  explicit BinderException(const std::string& msg)
      : Exception("Binder exception: " + msg,
                  gs::StatusCode::ERR_COMPILATION){};

  BinderException(const std::string& msg, const std::string& file_line)
      : Exception("Binder exception: " + msg, file_line,
                  gs::StatusCode::ERR_COMPILATION){};
};

class KUZU_API CatalogException : public Exception {
 public:
  explicit CatalogException(const std::string& msg)
      : Exception("Catalog exception: " + msg,
                  gs::StatusCode::ERR_COMPILATION){};

  CatalogException(const std::string& msg, const std::string& file_line)
      : Exception("Catalog exception: " + msg, file_line,
                  gs::StatusCode::ERR_COMPILATION){};
};

class KUZU_API CheckpointException : public Exception {
 public:
  explicit CheckpointException(const std::exception& e)
      : Exception(e.what(), gs::StatusCode::ERR_INTERNAL_ERROR){};

  explicit CheckpointException(const std::string& msg)
      : Exception("Checkpoint exception: " + msg,
                  gs::StatusCode::ERR_INTERNAL_ERROR){};
};

class KUZU_API ConnectionException : public Exception {
 public:
  explicit ConnectionException(const std::string& msg)
      : Exception("Connection exception: " + msg,
                  gs::StatusCode::ERR_CONNECTION_ERROR){};

  ConnectionException(const std::string& msg, const std::string& file_line)
      : Exception("Connection exception: " + msg, file_line,
                  gs::StatusCode::ERR_CONNECTION_ERROR){};
};

class KUZU_API ConversionException : public Exception {
 public:
  explicit ConversionException(const std::string& msg)
      : Exception("Conversion exception: " + msg,
                  gs::StatusCode::ERR_TYPE_CONVERSION) {}

  ConversionException(const std::string& msg, const std::string& file_line)
      : Exception("Conversion exception: " + msg, file_line,
                  gs::StatusCode::ERR_TYPE_CONVERSION){};
};

class KUZU_API QueryExecutionError : public Exception {
 public:
  explicit QueryExecutionError(const std::string& msg)
      : Exception("Query execution error: " + msg,
                  gs::StatusCode::ERR_QUERY_EXECUTION){};

  QueryExecutionError(const std::string& msg, const std::string& file_line)
      : Exception("Query execution error: " + msg, file_line,
                  gs::StatusCode::ERR_QUERY_EXECUTION){};
};

class KUZU_API CopyException : public Exception {
 public:
  explicit CopyException(const std::string& msg)
      : Exception("Copy exception: " + msg,
                  gs::StatusCode::ERR_INTERNAL_ERROR){};

  CopyException(const std::string& msg, const std::string& file_line)
      : Exception("Copy exception: " + msg, file_line,
                  gs::StatusCode::ERR_INTERNAL_ERROR){};
};

class KUZU_API IndexException : public Exception {
 public:
  explicit IndexException(const std::string& msg)
      : Exception("Index exception: " + msg, gs::StatusCode::ERR_INDEX_ERROR){};

  IndexException(const std::string& msg, const std::string& file_line)
      : Exception("Index exception: " + msg, file_line,
                  gs::StatusCode::ERR_INDEX_ERROR){};
};

class KUZU_API ExtensionException : public Exception {
 public:
  explicit ExtensionException(const std::string& msg)
      : Exception("Extension exception: " + msg,
                  gs::StatusCode::ERR_EXTENSION) {}

  ExtensionException(const std::string& msg, const std::string& file_line)
      : Exception("Extension exception: " + msg, file_line,
                  gs::StatusCode::ERR_EXTENSION){};
};

class KUZU_API InternalException : public Exception {
 public:
  explicit InternalException(const std::string& msg)
      : Exception(msg, gs::StatusCode::ERR_INTERNAL_ERROR){};

  InternalException(const std::string& msg, const std::string& file_line)
      : Exception(msg, file_line, gs::StatusCode::ERR_INTERNAL_ERROR){};
};

class KUZU_API InterruptException : public Exception {
 public:
  explicit InterruptException()
      : Exception("Interrupted.", gs::StatusCode::ERR_INTERNAL_ERROR){};

  InterruptException(const std::string& msg, const std::string& file_line)
      : Exception(msg, file_line, gs::StatusCode::ERR_INTERNAL_ERROR){};
};

class KUZU_API IOException : public Exception {
 public:
  explicit IOException(const std::string& msg)
      : Exception("IO exception: " + msg, gs::StatusCode::ERR_IO_ERROR) {}

  IOException(const std::string& msg, const std::string& file_line)
      : Exception("IO exception: " + msg, file_line,
                  gs::StatusCode::ERR_IO_ERROR){};
};

class KUZU_API NotImplementedException : public Exception {
 public:
  explicit NotImplementedException(const std::string& msg)
      : Exception(msg, gs::StatusCode::ERR_NOT_IMPLEMENTED){};

  NotImplementedException(const std::string& msg, const std::string& file_line)
      : Exception(msg, file_line, gs::StatusCode::ERR_NOT_IMPLEMENTED){};
};

class KUZU_API NotFoundException : public Exception {
 public:
  explicit NotFoundException(const std::string& msg)
      : Exception("Not found: " + msg, gs::StatusCode::ERR_NOT_FOUND){};

  NotFoundException(const std::string& msg, const std::string& file_line)
      : Exception("Not found: " + msg, file_line,
                  gs::StatusCode::ERR_NOT_FOUND){};
};

class KUZU_API NotSupportedException : public Exception {
 public:
  explicit NotSupportedException(const std::string& msg)
      : Exception("Not supported: " + msg, gs::StatusCode::ERR_NOT_SUPPORTED){};

  NotSupportedException(const std::string& msg, const std::string& file_line)
      : Exception("Not supported: " + msg, file_line,
                  gs::StatusCode::ERR_NOT_SUPPORTED){};
};

class KUZU_API OverflowException : public Exception {
 public:
  explicit OverflowException(const std::string& msg)
      : Exception("Overflow exception: " + msg,
                  gs::StatusCode::ERR_TYPE_OVERFLOW) {}

  OverflowException(const std::string& msg, const std::string& file_line)
      : Exception("Overflow exception: " + msg, file_line,
                  gs::StatusCode::ERR_TYPE_OVERFLOW){};
};

class KUZU_API SchemaMismatchException : public Exception {
 public:
  explicit SchemaMismatchException(const std::string& msg)
      : Exception("Schema mismatch: " + msg,
                  gs::StatusCode::ERR_SCHEMA_MISMATCH) {}

  SchemaMismatchException(const std::string& msg, const std::string& file_line)
      : Exception("Schema mismatch: " + msg, file_line,
                  gs::StatusCode::ERR_SCHEMA_MISMATCH){};
};

class KUZU_API ParserException : public Exception {
 public:
  static constexpr const char* ERROR_PREFIX = "Parser exception: ";

  explicit ParserException(const std::string& msg)
      : Exception(ERROR_PREFIX + msg, gs::StatusCode::ERR_QUERY_SYNTAX){};

  ParserException(const std::string& msg, const std::string& file_line)
      : Exception(ERROR_PREFIX + msg, file_line,
                  gs::StatusCode::ERR_QUERY_SYNTAX){};
};

class KUZU_API RuntimeError : public Exception {
 public:
  explicit RuntimeError(const std::string& msg)
      : Exception("Runtime exception: " + msg,
                  gs::StatusCode::ERR_INTERNAL_ERROR){};

  RuntimeError(const std::string& msg, const std::string& file_line)
      : Exception("Runtime exception: " + msg, file_line,
                  gs::StatusCode::ERR_INTERNAL_ERROR){};
};

class KUZU_API StorageException : public Exception {
 public:
  explicit StorageException(const std::string& msg)
      : Exception("Storage exception: " + msg,
                  gs::StatusCode::ERR_INTERNAL_ERROR){};

  StorageException(const std::string& msg, const std::string& file_line)
      : Exception("Storage exception: " + msg, file_line,
                  gs::StatusCode::ERR_INTERNAL_ERROR){};
};

class TestException : public Exception {
 public:
  explicit TestException(const std::string& msg)
      : Exception("Test exception: " + msg){};

  TestException(const std::string& msg, const std::string& file_line)
      : Exception("Test exception: " + msg, file_line){};
};

class KUZU_API TransactionManagerException : public Exception {
 public:
  explicit TransactionManagerException(const std::string& msg)
      : Exception(msg){};

  TransactionManagerException(const std::string& msg,
                              const std::string& file_line)
      : Exception(msg, file_line){};
};

class KUZU_API TxStateConflictException : public Exception {
 public:
  explicit TxStateConflictException(const std::string& msg)
      : Exception("Transaction state conflict: " + msg,
                  gs::StatusCode::ERR_TX_STATE_CONFLICT){};

  TxStateConflictException(const std::string& msg, const std::string& file_line)
      : Exception("Transaction state conflict: " + msg, file_line,
                  gs::StatusCode::ERR_TX_STATE_CONFLICT){};
};

}  // namespace exception

}  // namespace gs

// Define guard to get the file and line number where the exception is thrown
#define THROW_EXCEPTION_WITH_FILE_LINE(msg)                         \
  throw gs::exception::Exception(                                   \
      msg, std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
               " func: " + std::string(__FUNCTION__))

// Define a template guard with exception name and msg given
#define THROW_EXCEPTION_WITH_FILE_LINE_AND_TYPE(exception_type, msg) \
  throw gs::exception::exception_type(                               \
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
