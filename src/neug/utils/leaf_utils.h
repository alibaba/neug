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

#ifndef RUNTIME_COMMON_LEAF_UTILS_H_
#define RUNTIME_COMMON_LEAF_UTILS_H_

#include <boost/leaf.hpp>
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"

namespace bl = boost::leaf;

// Concatenate the current function name and line number to form the error
// message
#define PREPEND_LINE_INFO(msg)                             \
  std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
      " func: " + std::string(__FUNCTION__) + ", " + msg

#define RETURN_UNSUPPORTED_ERROR(msg)           \
  return ::boost::leaf::new_error(::gs::Status( \
      ::gs::StatusCode::ERR_NOT_SUPPORTED, PREPEND_LINE_INFO(msg)))

#define RETURN_INVALID_ARGUMENT_ERROR(msg)      \
  return ::boost::leaf::new_error(::gs::Status( \
      ::gs::StatusCode::ERR_INVALID_ARGUMENT, PREPEND_LINE_INFO(msg)))

#define RETURN_NOT_IMPLEMENTED_ERROR(msg)       \
  return ::boost::leaf::new_error(::gs::Status( \
      ::gs::StatusCode::ERR_NOT_IMPLEMENTED, PREPEND_LINE_INFO(msg)))

#define RETURN_CALL_PROCEDURE_ERROR(msg)        \
  return ::boost::leaf::new_error(::gs::Status( \
      ::gs::StatusCode::ERR_QUERY_EXECUTION, PREPEND_LINE_INFO(msg)))

/* Define a macro which generate wrap bl::try_handle_all with exception catching
    auto ret = bl::try_handle_all(
        [&]() -> bl::result<Context> {
          return opr->Eval(graph, params, std::move(ctx), timer);
        },
        [&](const gs::Status& err) {
          status = err;
          return ctx;
        },
        [&](const gs::exception::InvalidArgument& err) {
          status = gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return ctx;
        },
        [&](const gs::exception::InternalError& err) {
          status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return ctx;
        },
        [&](const bl::error_info& err) {
          status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return ctx;
        },
        [&]() {
          status = gs::Status(gs::StatusCode::ERR_UNKNOWN, "Unknown error");
          return ctx;
        });
*/
#define TRY_HANDLE_ALL_WITH_EXCEPTION_CATCHING(res, func, ret_t,               \
                                               status_handling, ret_value)     \
  auto res = bl::try_handle_all(                                               \
      [&]() -> ret_t { return func(); },                                       \
      [&](const gs::Status& err) {                                             \
        status_handling(err);                                                  \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::InvalidArgumentException& err) {                \
        status = gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT, err.what()); \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::PermissionDeniedException& err) {               \
        status = gs::Status(gs::StatusCode::ERR_PERMISSION, err.what());       \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::DatabaseLockedException& err) {                 \
        status = gs::Status(gs::StatusCode::ERR_DATABASE_LOCKED, err.what());  \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::BinderException& err) {                         \
        status = gs::Status(gs::StatusCode::ERR_COMPILATION, err.what());      \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::CatalogException& err) {                        \
        status = gs::Status(gs::StatusCode::ERR_COMPILATION, err.what());      \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::CheckpointException& err) {                     \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::InternalException& err) {                       \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::ConnectionException& err) {                     \
        status = gs::Status(gs::StatusCode::ERR_CONNECTION_ERROR, err.what()); \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::ConversionException& err) {                     \
        status = gs::Status(gs::StatusCode::ERR_TYPE_CONVERSION, err.what());  \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::QueryExecutionError& err) {                     \
        status = gs::Status(gs::StatusCode::ERR_QUERY_EXECUTION, err.what());  \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::RuntimeError& err) {                            \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::CopyException& err) {                           \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::IndexException& err) {                          \
        status = gs::Status(gs::StatusCode::ERR_INDEX_ERROR, err.what());      \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::ExtensionException& err) {                      \
        status = gs::Status(gs::StatusCode::ERR_EXTENSION, err.what());        \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::IOException& err) {                             \
        status = gs::Status(gs::StatusCode::ERR_IO_ERROR, err.what());         \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::NotImplementedException& err) {                 \
        status = gs::Status(gs::StatusCode::ERR_NOT_IMPLEMENTED, err.what());  \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::NotFoundException& err) {                       \
        status = gs::Status(gs::StatusCode::ERR_NOT_FOUND, err.what());        \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::NotSupportedException& err) {                   \
        status = gs::Status(gs::StatusCode::ERR_NOT_SUPPORTED, err.what());    \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::OverflowException& err) {                       \
        status = gs::Status(gs::StatusCode::ERR_TYPE_OVERFLOW, err.what());    \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::ParserException& err) {                         \
        status = gs::Status(gs::StatusCode::ERR_COMPILATION, err.what());      \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::StorageException& err) {                        \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::RuntimeError& err) {                            \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::TestException& err) {                           \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::TransactionManagerException& err) {             \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::SchemaMismatchException& err) {                 \
        status = gs::Status(gs::StatusCode::ERR_SCHEMA_MISMATCH, err.what());  \
        return ret_value;                                                      \
      },                                                                       \
      [&](const gs::exception::Exception& err) {                               \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what());   \
        return ret_value;                                                      \
      },                                                                       \
      [&](const bl::error_info& err) {                                         \
        status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR,                \
                            "Error: " + std::to_string(err.error().value()) +  \
                                ", Exception: " + err.exception()->what());    \
        return ret_value;                                                      \
      },                                                                       \
      [&]() {                                                                  \
        status = gs::Status(gs::StatusCode::ERR_UNKNOWN, "Unknown error");     \
        return ret_value;                                                      \
      });

inline std::string build_error_message(gs::StatusCode code,
                                       const bl::error_info& e) {
  /* Build a json error message from the error info:
    {
      "error_code": <error_code>,
      "error_message": <error_message>
    }
  */
  return std::string("{\"error_code\":") + std::to_string(code) +
         ",\"error_message\":\"" + std::to_string(e.error().value()) + "," +
         (e.exception() ? e.exception()->what() : "no exception") + "\"}";
}

inline std::string build_error_message(gs::StatusCode code,
                                       const std::string& msg) {
  /* Build a json error message from the error info:
    { "error_code": <error_code>,
      "error_message": <error_message>
    }
  */
  return std::string("{\"error_code\":") + std::to_string(code) +
         ",\"error_message\":\"" + msg + "\"}";
}

#endif  // RUNTIME_COMMON_LEAF_UTILS_H_