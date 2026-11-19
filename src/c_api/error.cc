/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#include "c_api_internal.h"

#include <new>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace capi {

neug_status Report(neug_status code, const char* message,
                   neug_error** out_error) noexcept {
  if (out_error) {
    try {
      *out_error = new neug_error{message};
    } catch (...) { *out_error = nullptr; }
  }
  return code;
}

neug_status CurrentException(neug_error** out_error) noexcept {
  try {
    throw;
  } catch (const Error& e) {
    return Report(e.code, e.what(), out_error);
  } catch (const std::bad_alloc&) {
    return Report(NEUG_ERROR_OUT_OF_MEMORY, "Out of memory", out_error);
  } catch (const exception::PermissionDeniedException& e) {
    return Report(NEUG_ERROR_PERMISSION, e.what(), out_error);
  } catch (const exception::DatabaseLockedException& e) {
    return Report(NEUG_ERROR_DATABASE_LOCKED, e.what(), out_error);
  } catch (const exception::NoCheckpointException& e) {
    return Report(NEUG_ERROR_NO_CHECKPOINT, e.what(), out_error);
  } catch (const exception::InvalidArgumentException& e) {
    return Report(NEUG_ERROR_INVALID_ARGUMENT, e.what(), out_error);
  } catch (const exception::ConversionException& e) {
    return Report(NEUG_ERROR_TYPE, e.what(), out_error);
  } catch (const exception::BinderException& e) {
    return Report(NEUG_ERROR_COMPILATION, e.what(), out_error);
  } catch (const exception::CatalogException& e) {
    return Report(NEUG_ERROR_COMPILATION, e.what(), out_error);
  } catch (const exception::ParserException& e) {
    return Report(NEUG_ERROR_COMPILATION, e.what(), out_error);
  } catch (const exception::QueryExecutionError& e) {
    return Report(NEUG_ERROR_QUERY, e.what(), out_error);
  } catch (const exception::NotFoundException& e) {
    return Report(NEUG_ERROR_NOT_FOUND, e.what(), out_error);
  } catch (const exception::IndexException& e) {
    return Report(NEUG_ERROR_INDEX, e.what(), out_error);
  } catch (const exception::SchemaMismatchException& e) {
    return Report(static_cast<neug_status>(StatusCode::ERR_SCHEMA_MISMATCH),
                  e.what(), out_error);
  } catch (const exception::ExtensionException& e) {
    return Report(static_cast<neug_status>(StatusCode::ERR_EXTENSION), e.what(),
                  out_error);
  } catch (const exception::ConnectionException& e) {
    return Report(static_cast<neug_status>(StatusCode::ERR_CONNECTION_ERROR),
                  e.what(), out_error);
  } catch (const exception::OverflowException& e) {
    return Report(NEUG_ERROR_OVERFLOW, e.what(), out_error);
  } catch (const exception::TxStateConflictException& e) {
    return Report(NEUG_ERROR_TRANSACTION, e.what(), out_error);
  } catch (const exception::IOException& e) {
    return Report(NEUG_ERROR_IO, e.what(), out_error);
  } catch (const exception::NotSupportedException& e) {
    return Report(NEUG_ERROR_NOT_SUPPORTED, e.what(), out_error);
  } catch (const exception::NotImplementedException& e) {
    return Report(static_cast<neug_status>(StatusCode::ERR_NOT_IMPLEMENTED),
                  e.what(), out_error);
  } catch (const std::invalid_argument& e) {
    return Report(NEUG_ERROR_INVALID_ARGUMENT, e.what(), out_error);
  } catch (const std::out_of_range& e) {
    return Report(NEUG_ERROR_INDEX, e.what(), out_error);
  } catch (const std::exception& e) {
    return Report(NEUG_ERROR_INTERNAL, e.what(), out_error);
  } catch (...) {
    return Report(NEUG_ERROR_INTERNAL, "Unknown C++ exception", out_error);
  }
}

}  // namespace capi
}  // namespace neug

neug_string_view neug_error_message(const neug_error* error) {
  return error ? neug::capi::View(error->message) : neug_string_view{};
}
void neug_error_destroy(neug_error** error) {
  if (error) {
    delete *error;
    *error = nullptr;
  }
}
