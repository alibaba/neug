#pragma once

#include <exception>
#include <string>

#include "neug/compiler/common/api.h"

namespace gs {
namespace exception {

class KUZU_API Exception : public std::exception {
 public:
  explicit Exception(std::string msg);

 public:
  const char* what() const noexcept override {
    return exception_message_.c_str();
  }

 private:
  std::string exception_message_;
};

class KUZU_API InvalidArgumentException : public Exception {
 public:
  explicit InvalidArgumentException(const std::string& msg)
      : Exception("Invalid argument: " + msg){};
};

class KUZU_API BinderException : public Exception {
 public:
  explicit BinderException(const std::string& msg)
      : Exception("Binder exception: " + msg){};
};

class KUZU_API BufferManagerException : public Exception {
 public:
  explicit BufferManagerException(const std::string& msg)
      : Exception("Buffer manager exception: " + msg){};
};

class KUZU_API CatalogException : public Exception {
 public:
  explicit CatalogException(const std::string& msg)
      : Exception("Catalog exception: " + msg){};
};

class KUZU_API CheckpointException : public Exception {
 public:
  explicit CheckpointException(const std::exception& e) : Exception(e.what()){};
};

class KUZU_API ConnectionException : public Exception {
 public:
  explicit ConnectionException(const std::string& msg)
      : Exception("Connection exception: " + msg){};
};

class KUZU_API ConversionException : public Exception {
 public:
  explicit ConversionException(const std::string& msg)
      : Exception("Conversion exception: " + msg) {}
};

class KUZU_API QueryExecutionError : public Exception {
 public:
  explicit QueryExecutionError(const std::string& msg)
      : Exception("Query execution error: " + msg){};
};

class KUZU_API CopyException : public Exception {
 public:
  explicit CopyException(const std::string& msg)
      : Exception("Copy exception: " + msg){};
};

class KUZU_API IndexException : public Exception {
 public:
  explicit IndexException(const std::string& msg)
      : Exception("Index exception: " + msg){};
};

class KUZU_API ExtensionException : public Exception {
 public:
  explicit ExtensionException(const std::string& msg)
      : Exception("Extension exception: " + msg) {}
};

class KUZU_API InternalException : public Exception {
 public:
  explicit InternalException(const std::string& msg) : Exception(msg){};
};

class KUZU_API InterruptException : public Exception {
 public:
  explicit InterruptException() : Exception("Interrupted."){};
};

class KUZU_API IOException : public Exception {
 public:
  explicit IOException(const std::string& msg)
      : Exception("IO exception: " + msg) {}
};

class KUZU_API NotImplementedException : public Exception {
 public:
  explicit NotImplementedException(const std::string& msg) : Exception(msg){};
};

class KUZU_API NotSupportedException : public Exception {
 public:
  explicit NotSupportedException(const std::string& msg)
      : Exception("Not supported: " + msg){};
};

class KUZU_API OverflowException : public Exception {
 public:
  explicit OverflowException(const std::string& msg)
      : Exception("Overflow exception: " + msg) {}
};

class KUZU_API ParserException : public Exception {
 public:
  static constexpr const char* ERROR_PREFIX = "Parser exception: ";

  explicit ParserException(const std::string& msg)
      : Exception(ERROR_PREFIX + msg){};
};

class KUZU_API RuntimeError : public Exception {
 public:
  explicit RuntimeError(const std::string& msg)
      : Exception("Runtime exception: " + msg){};
};

class KUZU_API StorageException : public Exception {
 public:
  explicit StorageException(const std::string& msg)
      : Exception("Storage exception: " + msg){};
};

class TestException : public Exception {
 public:
  explicit TestException(const std::string& msg)
      : Exception("Test exception: " + msg){};
};

class KUZU_API TransactionManagerException : public Exception {
 public:
  explicit TransactionManagerException(const std::string& msg)
      : Exception(msg){};
};

}  // namespace exception

}  // namespace gs
