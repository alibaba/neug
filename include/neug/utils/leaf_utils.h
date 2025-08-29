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

#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"
#include "tl/expected.hpp"

namespace gs {
template <typename ReturnType>
using result = tl::expected<ReturnType, gs::Status>;
}

#define RETURN_ERROR(err) return tl::unexpected(err)

#define RETURN_STATUS_ERROR(code, msg) \
  return tl::unexpected(gs::Status(code, msg))

#define GS_RESULT_CHECK(r)               \
  ({                                     \
    auto&& _r = (r);                     \
    if (!_r)                             \
      return tl::unexpected(_r.error()); \
    std::move(_r);                       \
  }).value()

// Concatenate the current function name and line number to form the error
// message
#define PREPEND_LINE_INFO(msg)                             \
  std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
      " func: " + std::string(__FUNCTION__) + ", " + msg

#define RETURN_UNSUPPORTED_ERROR(msg)                                     \
  return tl::unexpected(::gs::Status(::gs::StatusCode::ERR_NOT_SUPPORTED, \
                                     PREPEND_LINE_INFO(msg)))
#define RETURN_INVALID_ARGUMENT_ERROR(msg)                                   \
  return tl::unexpected(::gs::Status(::gs::StatusCode::ERR_INVALID_ARGUMENT, \
                                     PREPEND_LINE_INFO(msg)))
#define RETURN_NOT_IMPLEMENTED_ERROR(msg)                                   \
  return tl::unexpected(::gs::Status(::gs::StatusCode::ERR_NOT_IMPLEMENTED, \
                                     PREPEND_LINE_INFO(msg)))

#define RETURN_CALL_PROCEDURE_ERROR(msg)                                    \
  return tl::unexpected(::gs::Status(::gs::StatusCode::ERR_QUERY_EXECUTION, \
                                     PREPEND_LINE_INFO(msg)))

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
  } catch (const gs::exception::PermissionDeniedException& err) {              \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_PERMISSION, err.what()));      \
  } catch (const gs::exception::DatabaseLockedException& err) {                \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_DATABASE_LOCKED, err.what())); \
  } catch (const gs::exception::InvalidArgumentException& err) {               \
    RETURN_ERROR(                                                              \
        gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT, err.what()));         \
  } catch (const gs::exception::BinderException& err) {                        \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_COMPILATION, err.what()));     \
  } catch (const gs::exception::CatalogException& err) {                       \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_COMPILATION, err.what()));     \
  } catch (const gs::exception::CheckpointException& err) {                    \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what()));  \
  } catch (const gs::exception::ConnectionException& err) {                    \
    RETURN_ERROR(                                                              \
        gs::Status(gs::StatusCode::ERR_CONNECTION_ERROR, err.what()));         \
  } catch (const gs::exception::ConversionException& err) {                    \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_TYPE_CONVERSION, err.what())); \
  } catch (const gs::exception::QueryExecutionError& err) {                    \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_QUERY_EXECUTION, err.what())); \
  } catch (const gs::exception::CopyException& err) {                          \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what()));  \
  } catch (const gs::exception::RuntimeError& err) {                           \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what()));  \
  } catch (const gs::exception::IndexException& err) {                         \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INDEX_ERROR, err.what()));     \
  } catch (const gs::exception::ExtensionException& err) {                     \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_EXTENSION, err.what()));       \
  } catch (const gs::exception::InternalException& err) {                      \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what()));  \
  } catch (const gs::exception::InterruptException& err) {                     \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what()));  \
  } catch (const gs::exception::IOException& err) {                            \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_IO_ERROR, err.what()));        \
  } catch (const gs::exception::NotImplementedException& err) {                \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_NOT_IMPLEMENTED, err.what())); \
  } catch (const gs::exception::NotFoundException& err) {                      \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_NOT_FOUND, err.what()));       \
  } catch (const gs::exception::NotSupportedException& err) {                  \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_NOT_SUPPORTED, err.what()));   \
  } catch (const gs::exception::OverflowException& err) {                      \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_TYPE_OVERFLOW, err.what()));   \
  } catch (const gs::exception::SchemaMismatchException& err) {                \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_SCHEMA_MISMATCH, err.what())); \
  } catch (const gs::exception::ParserException& err) {                        \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_COMPILATION, err.what()));     \
  } catch (const gs::exception::StorageException& err) {                       \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what()));  \
  } catch (const gs::exception::Exception& err) {                              \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what()));  \
  } catch (const std::exception& err) {                                        \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR, err.what()));  \
  } catch (...) {                                                              \
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_UNKNOWN, "Unknown error"));    \
  }

#endif  // RUNTIME_COMMON_LEAF_UTILS_H_