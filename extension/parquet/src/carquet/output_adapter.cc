/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "output_adapter.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <utility>

namespace neug::parquet {
namespace {

struct OutputContext {
  explicit OutputContext(std::unique_ptr<io::OutputStream> value)
      : output(std::move(value)) {}

  std::unique_ptr<io::OutputStream> output;
};

carquet_status_t write(void* opaque, const void* data, size_t size) noexcept {
  if (!opaque || (size > 0 && !data) ||
      size > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
    return CARQUET_ERROR_INVALID_ARGUMENT;
  }
  try {
    auto status = static_cast<OutputContext*>(opaque)->output->Write(
        static_cast<const uint8_t*>(data), static_cast<int64_t>(size));
    return status.ok() ? CARQUET_OK : CARQUET_ERROR_FILE_WRITE;
  } catch (...) { return CARQUET_ERROR_FILE_WRITE; }
}

carquet_status_t closeOutput(void* opaque) noexcept {
  if (!opaque) {
    return CARQUET_ERROR_INVALID_ARGUMENT;
  }
  auto* context = static_cast<OutputContext*>(opaque);
  try {
    auto status = context->output->Close();
    if (!status.ok()) {
      return CARQUET_ERROR_FILE_WRITE;
    }
    delete context;
    return CARQUET_OK;
  } catch (...) { return CARQUET_ERROR_FILE_WRITE; }
}

void abortOutput(void* opaque) noexcept {
  std::unique_ptr<OutputContext> context(static_cast<OutputContext*>(opaque));
  if (!context || !context->output) {
    return;
  }
  try {
    context->output->Abort();
  } catch (...) {}
}

void setError(carquet_error_t* error, carquet_status_t status,
              const char* message) {
  carquet_error_set(error, status, __FILE__, __LINE__, __func__, "%s", message);
}

void abortBestEffort(std::unique_ptr<io::OutputStream>& output) noexcept {
  if (!output) {
    return;
  }
  try {
    output->Abort();
  } catch (...) {}
}

}  // namespace

carquet_writer_t* createCarquetWriter(std::unique_ptr<io::OutputStream> output,
                                      const carquet_schema_t* schema,
                                      const carquet_writer_options_t* options,
                                      carquet_error_t* error) {
  if (!output) {
    setError(error, CARQUET_ERROR_INVALID_ARGUMENT,
             "NeuG output stream is null");
    return nullptr;
  }
  if (!schema) {
    abortBestEffort(output);
    setError(error, CARQUET_ERROR_INVALID_ARGUMENT,
             "Carquet output schema is null");
    return nullptr;
  }

  auto* context = new (std::nothrow) OutputContext(nullptr);
  if (!context) {
    abortBestEffort(output);
    setError(error, CARQUET_ERROR_OUT_OF_MEMORY,
             "Failed to allocate NeuG output context");
    return nullptr;
  }
  context->output = std::move(output);

  const carquet_output_stream_t stream{
      .context = context,
      .write = write,
      .close = closeOutput,
      .abort = abortOutput,
  };
  return carquet_writer_create_output(&stream, schema, options, error);
}

}  // namespace neug::parquet
