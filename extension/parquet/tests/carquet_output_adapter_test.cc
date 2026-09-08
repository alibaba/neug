/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <carquet/carquet.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "carquet/input_adapter.h"
#include "carquet/output_adapter.h"
#include "neug/utils/io/stream/input_stream.h"
#include "neug/utils/io/stream/output_stream.h"
#include "neug/utils/result.h"

namespace neug::parquet {
namespace {

enum class OutputFailure {
  NONE,
  WRITE,
  CLOSE,
  THROW_WRITE,
  THROW_CLOSE,
  THROW_ABORT
};

struct OutputState {
  std::vector<uint8_t> bytes;
  int writeCalls = 0;
  int closeCalls = 0;
  int abortCalls = 0;
  int destructCalls = 0;
};

class MemoryOutput final : public io::OutputStream {
 public:
  MemoryOutput(std::shared_ptr<OutputState> state, OutputFailure failure = {})
      : state_(std::move(state)), failure_(failure) {}

  ~MemoryOutput() override { ++state_->destructCalls; }

  Status Write(const uint8_t* data, int64_t nbytes) override {
    ++state_->writeCalls;
    if (failure_ == OutputFailure::THROW_WRITE) {
      throw std::runtime_error("injected write exception");
    }
    if (failure_ == OutputFailure::WRITE) {
      return Status(StatusCode::ERR_IO_ERROR, "injected write failure");
    }
    state_->bytes.insert(state_->bytes.end(), data, data + nbytes);
    return Status::OK();
  }

  Status Close() override {
    ++state_->closeCalls;
    if (failure_ == OutputFailure::THROW_CLOSE) {
      throw std::runtime_error("injected close exception");
    }
    if (failure_ == OutputFailure::CLOSE) {
      return Status(StatusCode::ERR_IO_ERROR, "injected close failure");
    }
    return Status::OK();
  }

  void Abort() override {
    ++state_->abortCalls;
    state_->bytes.clear();
    if (failure_ == OutputFailure::THROW_ABORT) {
      throw std::runtime_error("injected abort exception");
    }
  }

 private:
  std::shared_ptr<OutputState> state_;
  OutputFailure failure_;
};

carquet_schema_t* makeOutputSchema(carquet_error_t* error) {
  carquet_schema_t* schema = carquet_schema_create(error);
  if (!schema) {
    return nullptr;
  }
  if (carquet_schema_add_column(schema, "value", CARQUET_PHYSICAL_INT64,
                                nullptr, CARQUET_REPETITION_REQUIRED, 0,
                                0) != CARQUET_OK) {
    carquet_schema_free(schema);
    return nullptr;
  }
  return schema;
}

carquet_status_t writeValues(carquet_writer_t* writer) {
  const int64_t values[] = {11, 22, 33};
  return carquet_writer_write_batch(writer, 0, values, 3, nullptr, nullptr);
}

TEST(CarquetOutputAdapterTest, ClosesOrAbortsAndReleasesTheNeuGStreamOnce) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = makeOutputSchema(&error);
  ASSERT_NE(schema, nullptr) << error.message;

  auto closeState = std::make_shared<OutputState>();
  auto* writer = createCarquetWriter(std::make_unique<MemoryOutput>(closeState),
                                     schema, nullptr, &error);
  ASSERT_NE(writer, nullptr) << error.message;
  ASSERT_EQ(writeValues(writer), CARQUET_OK);
  EXPECT_EQ(carquet_writer_close(writer), CARQUET_OK);
  EXPECT_EQ(closeState->closeCalls, 1);
  EXPECT_EQ(closeState->abortCalls, 0);
  EXPECT_EQ(closeState->destructCalls, 1);
  EXPECT_GT(closeState->writeCalls, 0);

  auto abortState = std::make_shared<OutputState>();
  writer = createCarquetWriter(std::make_unique<MemoryOutput>(abortState),
                               schema, nullptr, &error);
  ASSERT_NE(writer, nullptr) << error.message;
  carquet_writer_abort(writer);
  EXPECT_EQ(abortState->closeCalls, 0);
  EXPECT_EQ(abortState->abortCalls, 1);
  EXPECT_EQ(abortState->destructCalls, 1);

  carquet_schema_free(schema);
}

TEST(CarquetOutputAdapterTest, WriteAndCloseFailuresDiscardPartialOutput) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = makeOutputSchema(&error);
  ASSERT_NE(schema, nullptr) << error.message;

  for (const auto failure :
       {OutputFailure::WRITE, OutputFailure::CLOSE, OutputFailure::THROW_WRITE,
        OutputFailure::THROW_CLOSE}) {
    auto state = std::make_shared<OutputState>();
    auto* writer =
        createCarquetWriter(std::make_unique<MemoryOutput>(state, failure),
                            schema, nullptr, &error);
    ASSERT_NE(writer, nullptr) << error.message;
    const auto writeStatus = writeValues(writer);
    if (failure == OutputFailure::WRITE ||
        failure == OutputFailure::THROW_WRITE) {
      EXPECT_EQ(writeStatus, CARQUET_ERROR_FILE_WRITE);
    } else {
      EXPECT_EQ(writeStatus, CARQUET_OK);
    }
    EXPECT_EQ(carquet_writer_close(writer), CARQUET_ERROR_FILE_WRITE);
    EXPECT_EQ(state->abortCalls, 1);
    EXPECT_EQ(state->destructCalls, 1);
    EXPECT_TRUE(state->bytes.empty());
    if (failure == OutputFailure::CLOSE ||
        failure == OutputFailure::THROW_CLOSE) {
      EXPECT_EQ(state->closeCalls, 1);
    } else {
      EXPECT_EQ(state->closeCalls, 0);
    }
  }

  carquet_schema_free(schema);
}

TEST(CarquetOutputAdapterTest, ExceptionsCannotCrossTheCAbiBoundary) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = makeOutputSchema(&error);
  ASSERT_NE(schema, nullptr) << error.message;
  auto state = std::make_shared<OutputState>();
  auto* writer = createCarquetWriter(
      std::make_unique<MemoryOutput>(state, OutputFailure::THROW_ABORT), schema,
      nullptr, &error);
  ASSERT_NE(writer, nullptr) << error.message;
  EXPECT_NO_THROW(carquet_writer_abort(writer));
  EXPECT_EQ(state->abortCalls, 1);
  EXPECT_EQ(state->destructCalls, 1);
  carquet_schema_free(schema);
}

TEST(CarquetOutputAdapterTest, WritesReadableFilesThroughExistingLocalOutput) {
  const auto path = std::filesystem::path(::testing::TempDir()) /
                    "neug-carquet-output-adapter.parquet";

  for (const auto& destination :
       {path.string(), std::string("file://") + path.string()}) {
    carquet_error_t error = CARQUET_ERROR_INIT;
    carquet_schema_t* schema = makeOutputSchema(&error);
    ASSERT_NE(schema, nullptr) << error.message;
    auto* writer = createCarquetWriter(io::openLocalOutputStream(destination),
                                       schema, nullptr, &error);
    ASSERT_NE(writer, nullptr) << error.message;
    ASSERT_EQ(writeValues(writer), CARQUET_OK);
    ASSERT_EQ(carquet_writer_close(writer), CARQUET_OK);
    carquet_schema_free(schema);

    auto* reader = openCarquetReader(io::openLocalInputStream(path.string()),
                                     nullptr, &error);
    ASSERT_NE(reader, nullptr) << error.message;
    auto* column = carquet_reader_get_column(reader, 0, 0, &error);
    ASSERT_NE(column, nullptr) << error.message;
    int64_t values[3] = {};
    EXPECT_EQ(carquet_column_read_batch(column, values, 3, nullptr, nullptr),
              3);
    EXPECT_EQ(std::vector<int64_t>(values, values + 3),
              (std::vector<int64_t>{11, 22, 33}));
    carquet_column_reader_free(column);
    carquet_reader_close(reader);
    std::filesystem::remove(path);
  }
}

TEST(CarquetOutputAdapterTest, RejectsInvalidArgumentsAndAbortsOwnedOutput) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  EXPECT_EQ(createCarquetWriter(nullptr, nullptr, nullptr, &error), nullptr);
  EXPECT_EQ(error.code, CARQUET_ERROR_INVALID_ARGUMENT);

  auto state = std::make_shared<OutputState>();
  error = CARQUET_ERROR_INIT;
  EXPECT_EQ(createCarquetWriter(std::make_unique<MemoryOutput>(state), nullptr,
                                nullptr, &error),
            nullptr);
  EXPECT_EQ(error.code, CARQUET_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(state->abortCalls, 1);
  EXPECT_EQ(state->destructCalls, 1);
}

}  // namespace
}  // namespace neug::parquet
