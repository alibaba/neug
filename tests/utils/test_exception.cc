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

#include <gtest/gtest.h>
#include <regex>

#include "neug/utils/exception/exception.h"
#include "neug/utils/exception/message.h"
#include "neug/utils/result.h"

int ExtractErrorCode(const std::string& msg) {
  std::regex pattern(R"(Error Code: (\d+))");
  std::smatch matches;
  if (std::regex_search(msg, matches, pattern)) {
    return std::stoi(matches[1].str());
  }
  return -1;
}

struct ExceptionTestParam {
  std::function<neug::exception::Exception*(const std::string&)> ctor_no_file;
  std::function<neug::exception::Exception*(const std::string&,
                                            const std::string&)>
      ctor_with_file;
  std::string expected_prefix;
  neug::StatusCode expected_code;
};

class ExceptionTest : public ::testing::TestWithParam<ExceptionTestParam> {};

TEST_P(ExceptionTest, ConstructorWithoutFileLine) {
  auto param = GetParam();
  std::unique_ptr<neug::exception::Exception> e(
      param.ctor_no_file("test message"));

  std::string msg = e->what();
  EXPECT_NE(msg.find(param.expected_prefix + "test message"),
            std::string::npos);
  EXPECT_EQ(ExtractErrorCode(msg), static_cast<int>(param.expected_code));
}

TEST_P(ExceptionTest, ConstructorWithFileLine) {
  auto param = GetParam();
  std::unique_ptr<neug::exception::Exception> e(
      param.ctor_with_file("test message", "fake.cc:100 func: Foo"));

  std::string msg = e->what();
  EXPECT_NE(msg.find(param.expected_prefix + "test message"),
            std::string::npos);
  EXPECT_NE(msg.find("at fake.cc:100 func: Foo"), std::string::npos);
  EXPECT_EQ(ExtractErrorCode(msg), static_cast<int>(param.expected_code));
}

// Instantiate test cases for all exceptions
INSTANTIATE_TEST_SUITE_P(
    AllExceptions, ExceptionTest,
    ::testing::Values(
        // (ctor_no_file, ctor_with_file, prefix, error_code)
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::PermissionDeniedException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::PermissionDeniedException(m, f);
            },
            "Permission denied: ", neug::StatusCode::ERR_PERMISSION},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::DatabaseLockedException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::DatabaseLockedException(m, f);
            },
            "Database locked: ", neug::StatusCode::ERR_DATABASE_LOCKED},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::InvalidArgumentException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::InvalidArgumentException(m, f);
            },
            "Invalid argument: ", neug::StatusCode::ERR_INVALID_ARGUMENT},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::BinderException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::BinderException(m, f);
            },
            "Binder exception: ", neug::StatusCode::ERR_COMPILATION},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::CatalogException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::CatalogException(m, f);
            },
            "Catalog exception: ", neug::StatusCode::ERR_COMPILATION},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::ConnectionException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::ConnectionException(m, f);
            },
            "Connection exception: ", neug::StatusCode::ERR_CONNECTION_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::ConversionException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::ConversionException(m, f);
            },
            "Conversion exception: ", neug::StatusCode::ERR_TYPE_CONVERSION},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::QueryExecutionError(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::QueryExecutionError(m, f);
            },
            "Query execution error: ", neug::StatusCode::ERR_QUERY_EXECUTION},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::CopyException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::CopyException(m, f);
            },
            "Copy exception: ", neug::StatusCode::ERR_INTERNAL_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::IndexException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::IndexException(m, f);
            },
            "Index exception: ", neug::StatusCode::ERR_INDEX_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::ExtensionException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::ExtensionException(m, f);
            },
            "Extension exception: ", neug::StatusCode::ERR_EXTENSION},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::InternalException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::InternalException(m, f);
            },
            "",  // No prefix
            neug::StatusCode::ERR_INTERNAL_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::IOException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::IOException(m, f);
            },
            "IO exception: ", neug::StatusCode::ERR_IO_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::NotImplementedException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::NotImplementedException(m, f);
            },
            "",  // No prefix
            neug::StatusCode::ERR_NOT_IMPLEMENTED},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::NotFoundException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::NotFoundException(m, f);
            },
            "Not found exception: ", neug::StatusCode::ERR_NOT_FOUND},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::NotSupportedException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::NotSupportedException(m, f);
            },
            "Not supported: ", neug::StatusCode::ERR_NOT_SUPPORTED},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::OverflowException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::OverflowException(m, f);
            },
            "Overflow exception: ", neug::StatusCode::ERR_TYPE_OVERFLOW},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::SchemaMismatchException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::SchemaMismatchException(m, f);
            },
            "Schema mismatch: ", neug::StatusCode::ERR_SCHEMA_MISMATCH},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::ParserException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::ParserException(m, f);
            },
            "Parser exception: ", neug::StatusCode::ERR_QUERY_SYNTAX},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::RuntimeError(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::RuntimeError(m, f);
            },
            "Runtime exception: ", neug::StatusCode::ERR_INTERNAL_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::StorageException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::StorageException(m, f);
            },
            "Storage exception: ", neug::StatusCode::ERR_INTERNAL_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::TestException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::TestException(m, f);
            },
            "Test exception: ", neug::StatusCode::ERR_INTERNAL_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::TransactionManagerException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::TransactionManagerException(m, f);
            },
            "",  // No prefix
            neug::StatusCode::ERR_INTERNAL_ERROR},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::TxStateConflictException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::TxStateConflictException(m, f);
            },
            "Transaction state conflict: ",
            neug::StatusCode::ERR_TX_STATE_CONFLICT},
        ExceptionTestParam{
            [](const std::string& m) -> neug::exception::Exception* {
              return new neug::exception::PropertyNotFoundException(m);
            },
            [](const std::string& m,
               const std::string& f) -> neug::exception::Exception* {
              return new neug::exception::PropertyNotFoundException(m, f);
            },
            "Property not found: ", neug::StatusCode::ERR_PROPERTY_NOT_FOUND}));

TEST(CheckpointExceptionTest, FromStdException) {
  std::runtime_error std_e("disk full");
  neug::exception::CheckpointException e(std_e);
  std::string msg = e.what();
  EXPECT_NE(msg.find("disk full"), std::string::npos);
  EXPECT_EQ(ExtractErrorCode(msg),
            static_cast<int>(neug::StatusCode::ERR_INTERNAL_ERROR));
}

TEST(CheckpointExceptionTest, FromMessage) {
  neug::exception::CheckpointException e("manual checkpoint error");
  std::string msg = e.what();
  EXPECT_NE(msg.find("Checkpoint exception: manual checkpoint error"),
            std::string::npos);
  EXPECT_EQ(ExtractErrorCode(msg),
            static_cast<int>(neug::StatusCode::ERR_INTERNAL_ERROR));
}

TEST(InterruptExceptionTest, DefaultConstructor) {
  neug::exception::InterruptException e;
  std::string msg = e.what();
  EXPECT_NE(msg.find("Interrupted."), std::string::npos);
  EXPECT_EQ(ExtractErrorCode(msg),
            static_cast<int>(neug::StatusCode::ERR_INTERNAL_ERROR));
}

#define TEST_THROW_MACRO(Macro, ExceptionType, ExpectedPrefix) \
  TEST(ExceptionMacroTest, Macro) {                            \
    try {                                                      \
      Macro("macro test message");                             \
      FAIL() << "Expected " #ExceptionType;                    \
    } catch (const neug::exception::ExceptionType& e) {        \
      std::string msg = e.what();                              \
      EXPECT_NE(msg.find(ExpectedPrefix "macro test message"), \
                std::string::npos);                            \
      EXPECT_NE(msg.find(__FILE__), std::string::npos);        \
      EXPECT_NE(msg.find(ExpectedPrefix), std::string::npos);  \
    } catch (...) { FAIL() << "Unexpected exception type"; }   \
  }

// Generate tests for all THROW_XXX macros
TEST_THROW_MACRO(THROW_INVALID_ARGUMENT_EXCEPTION, InvalidArgumentException,
                 "Invalid argument: ")
TEST_THROW_MACRO(THROW_PERMISSION_DENIED, PermissionDeniedException,
                 "Permission denied: ")
TEST_THROW_MACRO(THROW_DATABASE_LOCKED_EXCEPTION, DatabaseLockedException,
                 "Database locked: ")
TEST_THROW_MACRO(THROW_BINDER_EXCEPTION, BinderException, "Binder exception: ")
TEST_THROW_MACRO(THROW_CATALOG_EXCEPTION, CatalogException,
                 "Catalog exception: ")
TEST_THROW_MACRO(THROW_CONNECTION_EXCEPTION, ConnectionException,
                 "Connection exception: ")
TEST_THROW_MACRO(THROW_CONVERSION_EXCEPTION, ConversionException,
                 "Conversion exception: ")
TEST_THROW_MACRO(THROW_QUERY_EXECUTION_ERROR, QueryExecutionError,
                 "Query execution error: ")
TEST_THROW_MACRO(THROW_COPY_EXCEPTION, CopyException, "Copy exception: ")
TEST_THROW_MACRO(THROW_INDEX_EXCEPTION, IndexException, "Index exception: ")
TEST_THROW_MACRO(THROW_EXTENSION_EXCEPTION, ExtensionException,
                 "Extension exception: ")
TEST_THROW_MACRO(THROW_INTERNAL_EXCEPTION, InternalException, "")
TEST_THROW_MACRO(THROW_INTERRUPT_EXCEPTION, InterruptException, "")
TEST_THROW_MACRO(THROW_IO_EXCEPTION, IOException, "IO exception: ")
TEST_THROW_MACRO(THROW_NOT_IMPLEMENTED_EXCEPTION, NotImplementedException, "")
TEST_THROW_MACRO(THROW_NOT_FOUND_EXCEPTION, NotFoundException,
                 "Not found exception: ")
TEST_THROW_MACRO(THROW_NOT_SUPPORTED_EXCEPTION, NotSupportedException,
                 "Not supported: ")
TEST_THROW_MACRO(THROW_OVERFLOW_EXCEPTION, OverflowException,
                 "Overflow exception: ")
TEST_THROW_MACRO(THROW_PARSER_EXCEPTION, ParserException, "Parser exception: ")
TEST_THROW_MACRO(THROW_RUNTIME_ERROR, RuntimeError, "Runtime exception: ")
TEST_THROW_MACRO(THROW_STORAGE_EXCEPTION, StorageException,
                 "Storage exception: ")
TEST_THROW_MACRO(THROW_TEST_EXCEPTION, TestException, "Test exception: ")
TEST_THROW_MACRO(THROW_TRANSACTION_MANAGER_EXCEPTION,
                 TransactionManagerException, "")
TEST_THROW_MACRO(THROW_SCHEMA_MISMATCH, SchemaMismatchException,
                 "Schema mismatch: ")
TEST_THROW_MACRO(THROW_TX_STATE_CONFLICT, TxStateConflictException,
                 "Transaction state conflict: ")
TEST_THROW_MACRO(THROW_PROPERTY_NOT_FOUND, PropertyNotFoundException,
                 "Property not found: ")

TEST(BaseExceptionTest, WithErrorCode) {
  neug::exception::Exception e("base error",
                               neug::StatusCode::ERR_INTERNAL_ERROR);
  std::string msg = e.what();
  EXPECT_NE(msg.find("base error"), std::string::npos);
  EXPECT_EQ(ExtractErrorCode(msg),
            static_cast<int>(neug::StatusCode::ERR_INTERNAL_ERROR));
}

TEST(BaseExceptionTest, WithFileLineAndUnknownCode) {
  neug::exception::Exception e("base error", "test.cc:42 func: Foo");
  std::string msg = e.what();
  EXPECT_NE(msg.find("base error at test.cc:42 func: Foo"), std::string::npos);
  EXPECT_EQ(ExtractErrorCode(msg),
            static_cast<int>(neug::StatusCode::ERR_UNKNOWN));
}

void ExpectMessage(const std::string& actual,
                   const std::vector<std::string>& required_substrings) {
  for (const auto& substr : required_substrings) {
    EXPECT_NE(actual.find(substr), std::string::npos)
        << "Message: [" << actual << "] does not contain: [" << substr << "]";
  }
}

TEST(ExceptionMessageTest, DuplicatePKException) {
  std::string msg =
      neug::common::ExceptionMessage::duplicatePKException("user123");
  ExpectMessage(msg, {"Found duplicated primary key value user123",
                      "uniqueness constraint"});
}

TEST(ExceptionMessageTest, NonExistentPKException) {
  std::string msg =
      neug::common::ExceptionMessage::nonExistentPKException("order_999");
  ExpectMessage(msg, {"Unable to find primary key value order_999"});
}

TEST(ExceptionMessageTest, InvalidPKType) {
  std::string msg = neug::common::ExceptionMessage::invalidPKType("BLOB");
  ExpectMessage(msg, {"Invalid primary key column type BLOB",
                      "STRING or a numeric type"});
}

TEST(ExceptionMessageTest, NullPKException) {
  std::string msg = neug::common::ExceptionMessage::nullPKException();
  ExpectMessage(msg,
                {"Found NULL", "non-null constraint", "primary key column"});
}

TEST(ExceptionMessageTest, OverLargeStringPKValueException) {
  uint64_t len = 300000;
  std::string msg =
      neug::common::ExceptionMessage::overLargeStringPKValueException(len);
  ExpectMessage(msg, {"maximum length of primary key strings is 262144 bytes",
                      "length was 300000"});
}

TEST(ExceptionMessageTest, OverLargeStringValueException) {
  uint64_t len = 500000;
  std::string msg =
      neug::common::ExceptionMessage::overLargeStringValueException(len);
  ExpectMessage(
      msg, {"maximum length of strings is 262144 bytes", "length was 500000"});
}

TEST(ExceptionMessageTest, ViolateDeleteNodeWithConnectedEdgesConstraint) {
  std::string msg = neug::common::ExceptionMessage::
      violateDeleteNodeWithConnectedEdgesConstraint("FRIENDS", "12345",
                                                    "OUTGOING");
  ExpectMessage(msg,
                {"Node(nodeOffset: 12345)", "connected edges in table FRIENDS",
                 "OUTGOING direction", "cannot be deleted", "DETACH DELETE"});
}

TEST(ExceptionMessageTest, ViolateRelMultiplicityConstraint) {
  std::string msg =
      neug::common::ExceptionMessage::violateRelMultiplicityConstraint(
          "MARRIED_TO", "67890", "INCOMING");
  ExpectMessage(
      msg,
      {"Node(nodeOffset: 67890)", "more than one neighbour in table MARRIED_TO",
       "INCOMING direction", "violates the rel multiplicity constraint"});
}

TEST(ExceptionMessageTest, VariableNotInScope) {
  std::string msg = neug::common::ExceptionMessage::variableNotInScope("x");
  ExpectMessage(msg, {"Variable x is not in scope"});
}

TEST(ExceptionMessageTest, ListFunctionIncompatibleChildrenType) {
  std::string msg =
      neug::common::ExceptionMessage::listFunctionIncompatibleChildrenType(
          "LIST_CONCAT", "INT64", "STRING");
  ExpectMessage(
      msg, {"Cannot bind LIST_CONCAT with parameter type INT64 and STRING"});
}