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

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "neug/generated/proto/plan/error.pb.h"
#include "tl/expected.hpp"

namespace neug {

using StatusCode = neug::interactive::Code;

class Status {
 public:
  Status() noexcept;
  explicit Status(StatusCode error_code) noexcept;
  Status(StatusCode error_code, std::string&& error_msg) noexcept;
  Status(StatusCode error_code, const std::string& error_msg) noexcept;
  bool ok() const;
  const std::string& error_message() const;
  StatusCode error_code() const;

  static Status OK();
  static Status RuntimeError(const std::string& error_msg);
  static Status InternalError(const std::string& error_msg);
  static Status Unknown(const std::string& error_msg);
  inline operator bool() const { return ok(); }

  std::string ToString() const;

 private:
  StatusCode error_code_;
  std::string error_msg_;
};

// define a macro, which checks the return status of a function, if ok, continue
// to execute, otherwise, return the status.
// the macro accept the calling code of a function, and the function name.
#define RETURN_IF_NOT_OK(expr) \
  do {                         \
    auto status = (expr);      \
    if (!status.ok()) {        \
      return status;           \
    }                          \
  } while (0)

#define RETURN_IF_NOT_OK_CONFLICT(expr, error_on_conflict)     \
  do {                                                         \
    auto status = (expr);                                      \
    if (!status.ok()) {                                        \
      if (error_on_conflict) {                                 \
        return status;                                         \
      } else {                                                 \
        return Status(StatusCode::OK, status.error_message()); \
      }                                                        \
    }                                                          \
  } while (0)

template <typename ReturnType>
using result = tl::expected<ReturnType, neug::Status>;

}  // namespace neug

namespace std {
inline std::string to_string(const neug::interactive::Code& status) {
  // format the code into 0x-xxxx, where multiple zeros are prepend to the code
  std::stringstream ss;
  ss << std::setw(4) << std::setfill('0') << static_cast<int32_t>(status);
  return ss.str();
}
}  // namespace std

#define RETURN_ERROR(err) return tl::unexpected(err)

#define RETURN_STATUS_ERROR(code, msg) \
  return tl::unexpected(neug::Status(code, msg))

#define RETURN_STATUS_ERROR_IF_NOT_OK(expr) \
  do {                                      \
    auto status = (expr);                   \
    if (!status.ok()) {                     \
      return tl::unexpected(status);        \
    }                                       \
  } while (0)

#define GS_RESULT_CHECK(r)               \
  ({                                     \
    auto&& _r = (r);                     \
    if (!_r)                             \
      return tl::unexpected(_r.error()); \
    std::move(_r);                       \
  }).value()

#define GS_AUTO(var, expr)                                         \
  auto var = ({                                                    \
               auto&& _r = (expr);                                 \
               if (!_r) {                                          \
                 LOG(ERROR) << "Error: " << _r.error().ToString(); \
                 return tl::unexpected(_r.error());                \
               }                                                   \
               std::move(_r);                                      \
             }).value();
#define GS_ASSIGN(var, expr)                            \
  do {                                                  \
    auto&& _r = (expr);                                 \
    if (!_r) {                                          \
      LOG(ERROR) << "Error: " << _r.error().ToString(); \
      return tl::unexpected(_r.error());                \
    }                                                   \
    var = std::move(_r).value();                        \
  } while (0)

// Concatenate the current function name and line number to form the error
// message
#define PREPEND_LINE_INFO(msg)                             \
  std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
      " func: " + std::string(__FUNCTION__) + ", " + msg

#define RETURN_UNSUPPORTED_ERROR(msg)                                         \
  return tl::unexpected(::neug::Status(::neug::StatusCode::ERR_NOT_SUPPORTED, \
                                       PREPEND_LINE_INFO(msg)))
#define RETURN_INVALID_ARGUMENT_ERROR(msg) \
  return tl::unexpected(::neug::Status(    \
      ::neug::StatusCode::ERR_INVALID_ARGUMENT, PREPEND_LINE_INFO(msg)))
#define RETURN_NOT_IMPLEMENTED_ERROR(msg) \
  return tl::unexpected(::neug::Status(   \
      ::neug::StatusCode::ERR_NOT_IMPLEMENTED, PREPEND_LINE_INFO(msg)))

#define RETURN_CALL_PROCEDURE_ERROR(msg) \
  return tl::unexpected(::neug::Status(  \
      ::neug::StatusCode::ERR_QUERY_EXECUTION, PREPEND_LINE_INFO(msg)))

// Define a macro that run a function and catch all exceptions
#define TRY_HANDLE_ALL_WITH_EXCEPTION(ret_t, func, error_handling,             \
                                      normal_handling)                         \
  try {                                                                        \
    auto _ret = func();                                                        \
    if (!_ret) {                                                               \
      error_handling(_ret.error());                                            \
    } else {                                                                   \
      normal_handling(std::move(_ret));                                        \
    }                                                                          \
  } catch (const neug::exception::PermissionDeniedException& err) {            \
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_PERMISSION, err.what()));  \
  } catch (const neug::exception::DatabaseLockedException& err) {              \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_DATABASE_LOCKED, err.what()));      \
  } catch (const neug::exception::InvalidArgumentException& err) {             \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT, err.what()));     \
  } catch (const neug::exception::BinderException& err) {                      \
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_COMPILATION, err.what())); \
  } catch (const neug::exception::CatalogException& err) {                     \
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_COMPILATION, err.what())); \
  } catch (const neug::exception::CheckpointException& err) {                  \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, err.what()));       \
  } catch (const neug::exception::ConnectionException& err) {                  \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_CONNECTION_ERROR, err.what()));     \
  } catch (const neug::exception::ConversionException& err) {                  \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_TYPE_CONVERSION, err.what()));      \
  } catch (const neug::exception::QueryExecutionError& err) {                  \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_QUERY_EXECUTION, err.what()));      \
  } catch (const neug::exception::CopyException& err) {                        \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, err.what()));       \
  } catch (const neug::exception::RuntimeError& err) {                         \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, err.what()));       \
  } catch (const neug::exception::IndexException& err) {                       \
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_INDEX_ERROR, err.what())); \
  } catch (const neug::exception::ExtensionException& err) {                   \
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_EXTENSION, err.what()));   \
  } catch (const neug::exception::InternalException& err) {                    \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, err.what()));       \
  } catch (const neug::exception::InterruptException& err) {                   \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, err.what()));       \
  } catch (const neug::exception::IOException& err) {                          \
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_IO_ERROR, err.what()));    \
  } catch (const neug::exception::NotImplementedException& err) {              \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_NOT_IMPLEMENTED, err.what()));      \
  } catch (const neug::exception::NotFoundException& err) {                    \
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_NOT_FOUND, err.what()));   \
  } catch (const neug::exception::NotSupportedException& err) {                \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_NOT_SUPPORTED, err.what()));        \
  } catch (const neug::exception::OverflowException& err) {                    \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_TYPE_OVERFLOW, err.what()));        \
  } catch (const neug::exception::SchemaMismatchException& err) {              \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_SCHEMA_MISMATCH, err.what()));      \
  } catch (const neug::exception::ParserException& err) {                      \
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_COMPILATION, err.what())); \
  } catch (const neug::exception::StorageException& err) {                     \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, err.what()));       \
  } catch (const neug::exception::PropertyNotFoundException& err) {            \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_PROPERTY_NOT_FOUND, err.what()));   \
  } catch (const neug::exception::Exception& err) {                            \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, err.what()));       \
  } catch (const std::exception& err) {                                        \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_INTERNAL_ERROR, err.what()));       \
  } catch (...) {                                                              \
    RETURN_ERROR(                                                              \
        neug::Status(neug::StatusCode::ERR_UNKNOWN, "Unknown error"));         \
  }
