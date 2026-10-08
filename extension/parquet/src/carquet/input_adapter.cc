/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "carquet/input_adapter.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <utility>
#include <vector>

namespace neug::parquet {
namespace {

struct InputContext {
  InputContext(std::unique_ptr<io::InputStream> value, size_t bufferSize)
      : input(std::move(value)), bufferSize(bufferSize) {}

  std::unique_ptr<io::InputStream> input;
  size_t bufferSize;
  std::vector<uint8_t> buffer;
  int64_t bufferOffset = 0;
  size_t bufferedBytes = 0;
  std::mutex mutex;
};

carquet_status_t getSize(void* opaque, int64_t* size) noexcept {
  if (!opaque || !size) {
    return CARQUET_ERROR_INVALID_ARGUMENT;
  }
  try {
    auto value = static_cast<InputContext*>(opaque)->input->GetSize();
    if (!value || *value < 0) {
      return CARQUET_ERROR_FILE_READ;
    }
    *size = *value;
    return CARQUET_OK;
  } catch (...) { return CARQUET_ERROR_FILE_READ; }
}

carquet_status_t readAt(void* opaque, int64_t offset, void* buffer, size_t size,
                        size_t* bytesRead) noexcept {
  if (!opaque || !bytesRead || offset < 0 || (size > 0 && !buffer) ||
      size > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
    return CARQUET_ERROR_INVALID_ARGUMENT;
  }
  *bytesRead = 0;
  try {
    auto& context = *static_cast<InputContext*>(opaque);
    // InputStream does not guarantee concurrent ReadAt calls: local streams
    // seek/read a shared std::ifstream. Protect both the stream and the cache;
    // parallel row-group tasks each open an independent stream.
    std::lock_guard<std::mutex> guard(context.mutex);
    if (size == 0) {
      return CARQUET_OK;
    }
    if (context.buffer.empty() && context.bufferSize > 0) {
      context.buffer.resize(context.bufferSize);
    }
    if (!context.buffer.empty() && offset >= context.bufferOffset) {
      const uint64_t relative =
          static_cast<uint64_t>(offset - context.bufferOffset);
      if (relative <= context.bufferedBytes &&
          size <= context.bufferedBytes - relative) {
        std::memcpy(buffer, context.buffer.data() + relative, size);
        *bytesRead = size;
        return CARQUET_OK;
      }
    }

    if (context.buffer.empty() || size >= context.buffer.size()) {
      auto value =
          context.input->ReadAt(offset, static_cast<int64_t>(size), buffer);
      if (!value || *value < 0 || *value > static_cast<int64_t>(size)) {
        return CARQUET_ERROR_FILE_READ;
      }
      *bytesRead = static_cast<size_t>(*value);
      return CARQUET_OK;
    }

    auto value = context.input->ReadAt(
        offset, static_cast<int64_t>(context.buffer.size()),
        context.buffer.data());
    if (!value || *value < 0 ||
        *value > static_cast<int64_t>(context.buffer.size())) {
      return CARQUET_ERROR_FILE_READ;
    }
    context.bufferOffset = offset;
    context.bufferedBytes = static_cast<size_t>(*value);
    *bytesRead = std::min(size, context.bufferedBytes);
    if (*bytesRead > 0) {
      std::memcpy(buffer, context.buffer.data(), *bytesRead);
    }
    return CARQUET_OK;
  } catch (...) { return CARQUET_ERROR_FILE_READ; }
}

void closeInput(void* opaque) noexcept {
  std::unique_ptr<InputContext> context(static_cast<InputContext*>(opaque));
  if (!context || !context->input) {
    return;
  }
  try {
    context->input->Close();
  } catch (...) {}
}

void setInvalidArgument(carquet_error_t* error, const char* message) {
  carquet_error_set(error, CARQUET_ERROR_INVALID_ARGUMENT, __FILE__, __LINE__,
                    __func__, "%s", message);
}

}  // namespace

carquet_reader_t* openCarquetReader(std::unique_ptr<io::InputStream> input,
                                    const carquet_reader_options_t* options,
                                    carquet_error_t* error) {
  if (!input) {
    setInvalidArgument(error, "NeuG input stream is null");
    return nullptr;
  }

  const size_t bufferSize = options ? options->buffer_size : 0;
  auto* context = new (std::nothrow) InputContext(nullptr, bufferSize);
  if (!context) {
    try {
      input->Close();
    } catch (...) {}
    carquet_error_set(error, CARQUET_ERROR_OUT_OF_MEMORY, __FILE__, __LINE__,
                      __func__, "Failed to allocate NeuG input context");
    return nullptr;
  }
  context->input = std::move(input);

  const carquet_input_stream_t stream{
      .context = context,
      .get_size = getSize,
      .read_at = readAt,
      .close = closeInput,
  };
  return carquet_reader_open_input(&stream, options, error);
}

}  // namespace neug::parquet
