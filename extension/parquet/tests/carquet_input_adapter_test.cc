/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <carquet/carquet.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "carquet/input_adapter.h"
#include "neug/utils/io/stream/input_stream.h"
#include "neug/utils/result.h"

namespace neug::parquet {
namespace {

struct InputState {
  int readCalls = 0;
  int closeCalls = 0;
  int destructCalls = 0;
  int64_t requestedBytes = 0;
};

enum class FailureMode {
  NONE,
  SHORT_READ,
  EOF_READ,
  READ_ERROR,
  SIZE_ERROR,
  NEGATIVE_SIZE,
  OVERSIZED_READ,
  THROW_READ,
  THROW_SIZE,
  THROW_CLOSE,
};

class MemoryInput final : public io::InputStream {
 public:
  MemoryInput(std::shared_ptr<std::vector<uint8_t>> bytes,
              std::shared_ptr<InputState> state, FailureMode failure = {})
      : bytes_(std::move(bytes)), state_(std::move(state)), failure_(failure) {
    reportedSize_ = static_cast<int64_t>(bytes_->size());
    if (failure_ == FailureMode::EOF_READ) {
      reportedSize_ += 128;
    } else if (failure_ == FailureMode::NEGATIVE_SIZE) {
      reportedSize_ = -1;
    }
  }

  ~MemoryInput() override { ++state_->destructCalls; }

  result<int64_t> Read(void* out, int64_t nbytes) override {
    auto result = ReadAt(position_, nbytes, out);
    if (result) {
      position_ += *result;
    }
    return result;
  }

  result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    ++state_->readCalls;
    state_->requestedBytes += nbytes;
    if (failure_ == FailureMode::THROW_READ) {
      throw std::runtime_error("injected read exception");
    }
    if (failure_ == FailureMode::READ_ERROR) {
      return tl::unexpected(
          Status(StatusCode::ERR_IO_ERROR, "injected read failure"));
    }
    if (failure_ == FailureMode::OVERSIZED_READ) {
      return nbytes + 1;
    }
    if (position < 0 || nbytes <= 0 ||
        position >= static_cast<int64_t>(bytes_->size())) {
      return int64_t{0};
    }
    int64_t count = std::min<int64_t>(
        nbytes, static_cast<int64_t>(bytes_->size()) - position);
    if (failure_ == FailureMode::SHORT_READ && count > 0) {
      --count;
    }
    if (count > 0) {
      std::memcpy(out, bytes_->data() + position, static_cast<size_t>(count));
    }
    return count;
  }

  result<int64_t> GetSize() override {
    if (failure_ == FailureMode::THROW_SIZE) {
      throw std::runtime_error("injected size exception");
    }
    if (failure_ == FailureMode::SIZE_ERROR) {
      return tl::unexpected(
          Status(StatusCode::ERR_IO_ERROR, "injected size failure"));
    }
    return reportedSize_;
  }

  void Close() override {
    ++state_->closeCalls;
    if (failure_ == FailureMode::THROW_CLOSE) {
      throw std::runtime_error("injected close exception");
    }
  }

 private:
  std::shared_ptr<std::vector<uint8_t>> bytes_;
  std::shared_ptr<InputState> state_;
  FailureMode failure_;
  int64_t reportedSize_ = 0;
  int64_t position_ = 0;
};

carquet_schema_t* makeSchema(carquet_error_t* error) {
  carquet_schema_t* schema = carquet_schema_create(error);
  if (!schema) {
    return nullptr;
  }
  for (const char* name : {"selected", "unselected"}) {
    if (carquet_schema_add_column(schema, name, CARQUET_PHYSICAL_INT64, nullptr,
                                  CARQUET_REPETITION_REQUIRED, 0,
                                  0) != CARQUET_OK) {
      carquet_schema_free(schema);
      return nullptr;
    }
  }
  return schema;
}

std::shared_ptr<std::vector<uint8_t>> makeLargeParquet() {
  constexpr int64_t kRowsPerGroup = 300000;
  carquet_error_t error = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = makeSchema(&error);
  EXPECT_NE(schema, nullptr) << error.message;
  if (!schema) {
    return nullptr;
  }

  carquet_writer_options_t options;
  carquet_writer_options_init(&options);
  options.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
  options.dictionary_encoding = CARQUET_ENCODING_PLAIN;
  options.page_size = 64 * 1024;

  carquet_writer_t* writer =
      carquet_writer_create_buffer(schema, &options, &error);
  EXPECT_NE(writer, nullptr) << error.message;
  if (!writer) {
    carquet_schema_free(schema);
    return nullptr;
  }

  std::vector<int64_t> values(static_cast<size_t>(kRowsPerGroup));
  for (int64_t i = 0; i < kRowsPerGroup; ++i) {
    values[static_cast<size_t>(i)] = i * 17 - 9;
  }
  for (int group = 0; group < 2; ++group) {
    for (int column = 0; column < 2; ++column) {
      if (carquet_writer_write_batch(writer, column, values.data(),
                                     kRowsPerGroup, nullptr,
                                     nullptr) != CARQUET_OK) {
        ADD_FAILURE() << "failed to write Carquet test batch";
        carquet_writer_abort(writer);
        carquet_schema_free(schema);
        return nullptr;
      }
    }
    if (group == 0 && carquet_writer_new_row_group(writer) != CARQUET_OK) {
      ADD_FAILURE() << "failed to start Carquet test row group";
      carquet_writer_abort(writer);
      carquet_schema_free(schema);
      return nullptr;
    }
  }

  if (carquet_writer_close(writer) != CARQUET_OK) {
    ADD_FAILURE() << "failed to close Carquet test writer";
    carquet_writer_abort(writer);
    carquet_schema_free(schema);
    return nullptr;
  }
  void* output = nullptr;
  size_t outputSize = 0;
  if (carquet_writer_get_buffer(writer, &output, &outputSize) != CARQUET_OK) {
    ADD_FAILURE() << "failed to retrieve Carquet test buffer";
    carquet_writer_abort(writer);
    carquet_schema_free(schema);
    return nullptr;
  }
  carquet_schema_free(schema);

  auto bytes = std::make_shared<std::vector<uint8_t>>(
      static_cast<uint8_t*>(output),
      static_cast<uint8_t*>(output) + outputSize);
  std::free(output);
  EXPECT_GT(bytes->size(), 8U * 1024U * 1024U);
  return bytes;
}

void verifyFirstColumn(carquet_reader_t* reader, carquet_error_t* error) {
  constexpr int64_t kRows = 300000;
  carquet_column_reader_t* column =
      carquet_reader_get_column(reader, 0, 0, error);
  ASSERT_NE(column, nullptr) << error->message;
  std::vector<int64_t> values(static_cast<size_t>(kRows));
  ASSERT_EQ(
      carquet_column_read_batch(column, values.data(), kRows, nullptr, nullptr),
      kRows);
  EXPECT_EQ(values.front(), -9);
  EXPECT_EQ(values.back(), (kRows - 1) * 17 - 9);
  carquet_column_reader_free(column);
}

TEST(CarquetInputAdapterTest, ReadsRangesAndOwnsTheNeuGStream) {
  auto bytes = makeLargeParquet();
  ASSERT_NE(bytes, nullptr);
  auto state = std::make_shared<InputState>();
  carquet_error_t error = CARQUET_ERROR_INIT;
  carquet_reader_t* reader = openCarquetReader(
      std::make_unique<MemoryInput>(bytes, state), nullptr, &error);
  ASSERT_NE(reader, nullptr) << error.message;

  verifyFirstColumn(reader, &error);
  carquet_reader_close(reader);

  EXPECT_GT(state->readCalls, 1);
  EXPECT_LT(state->requestedBytes, static_cast<int64_t>(bytes->size()));
  EXPECT_EQ(state->closeCalls, 1);
  EXPECT_EQ(state->destructCalls, 1);
}

TEST(CarquetInputAdapterTest, PropagatesShortReadsInvalidResultsAndExceptions) {
  auto bytes = makeLargeParquet();
  ASSERT_NE(bytes, nullptr);
  for (const auto failure :
       {FailureMode::SHORT_READ, FailureMode::EOF_READ, FailureMode::READ_ERROR,
        FailureMode::SIZE_ERROR, FailureMode::NEGATIVE_SIZE,
        FailureMode::OVERSIZED_READ, FailureMode::THROW_READ,
        FailureMode::THROW_SIZE}) {
    auto state = std::make_shared<InputState>();
    carquet_error_t error = CARQUET_ERROR_INIT;
    carquet_reader_t* reader = openCarquetReader(
        std::make_unique<MemoryInput>(bytes, state, failure), nullptr, &error);
    EXPECT_EQ(reader, nullptr);
    EXPECT_NE(error.code, CARQUET_OK);
    EXPECT_EQ(state->closeCalls, 1);
    EXPECT_EQ(state->destructCalls, 1);
  }
}

TEST(CarquetInputAdapterTest, SwallowsCloseExceptionsAtTheCAbiBoundary) {
  auto bytes = makeLargeParquet();
  ASSERT_NE(bytes, nullptr);
  auto state = std::make_shared<InputState>();
  carquet_error_t error = CARQUET_ERROR_INIT;
  carquet_reader_t* reader = openCarquetReader(
      std::make_unique<MemoryInput>(bytes, state, FailureMode::THROW_CLOSE),
      nullptr, &error);
  ASSERT_NE(reader, nullptr) << error.message;
  EXPECT_NO_THROW(carquet_reader_close(reader));
  EXPECT_EQ(state->closeCalls, 1);
  EXPECT_EQ(state->destructCalls, 1);
}

TEST(CarquetInputAdapterTest, ReadsLocalPathsAndFileUrisThroughExistingOpener) {
  auto bytes = makeLargeParquet();
  ASSERT_NE(bytes, nullptr);
  const auto path = std::filesystem::path(::testing::TempDir()) /
                    "neug-carquet-input-adapter.parquet";
  {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output.write(reinterpret_cast<const char*>(bytes->data()),
                 static_cast<std::streamsize>(bytes->size()));
  }

  for (const auto& source :
       {path.string(), std::string("file://") + path.string()}) {
    carquet_error_t error = CARQUET_ERROR_INIT;
    carquet_reader_t* reader =
        openCarquetReader(io::openLocalInputStream(source), nullptr, &error);
    ASSERT_NE(reader, nullptr) << error.message;
    verifyFirstColumn(reader, &error);
    carquet_reader_close(reader);
  }
  std::filesystem::remove(path);
}

TEST(CarquetInputAdapterTest, RejectsNullInput) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  EXPECT_EQ(openCarquetReader(nullptr, nullptr, &error), nullptr);
  EXPECT_EQ(error.code, CARQUET_ERROR_INVALID_ARGUMENT);
}

}  // namespace
}  // namespace neug::parquet
