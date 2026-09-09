/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <carquet/carquet.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "carquet/chunk_supplier.h"
#include "carquet/column_converter.h"
#include "carquet/schema_converter.h"
#include "carquet/sniffer.h"

namespace neug::parquet {
namespace {

struct InputState {
  std::shared_ptr<std::vector<uint8_t>> bytes;
  bool failReads = false;
  int readCalls = 0;
  int closeCalls = 0;
  int destructCalls = 0;
};

class MemoryInput final : public io::InputStream {
 public:
  explicit MemoryInput(std::shared_ptr<InputState> state)
      : state_(std::move(state)) {}
  ~MemoryInput() override { ++state_->destructCalls; }

  result<int64_t> Read(void* out, int64_t nbytes) override {
    auto read = ReadAt(position_, nbytes, out);
    if (read) {
      position_ += *read;
    }
    return read;
  }

  result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    ++state_->readCalls;
    if (state_->failReads) {
      RETURN_STATUS_ERROR(StatusCode::ERR_IO_ERROR, "injected read failure");
    }
    if (position < 0 || nbytes < 0 || (nbytes > 0 && !out)) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "invalid memory read");
    }
    if (position >= static_cast<int64_t>(state_->bytes->size())) {
      return int64_t{0};
    }
    const int64_t count = std::min(
        nbytes, static_cast<int64_t>(state_->bytes->size()) - position);
    if (count > 0) {
      std::memcpy(out, state_->bytes->data() + position,
                  static_cast<size_t>(count));
    }
    return count;
  }

  result<int64_t> GetSize() override {
    return static_cast<int64_t>(state_->bytes->size());
  }

  void Close() override { ++state_->closeCalls; }

 private:
  std::shared_ptr<InputState> state_;
  int64_t position_ = 0;
};

std::shared_ptr<InputState> fixtureState(const std::string& name) {
  const auto path = std::filesystem::path(NEUG_PARQUET_DATASET_DIR) / name;
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Cannot open existing Parquet dataset: " +
                             path.string());
  }
  auto bytes = std::make_shared<std::vector<uint8_t>>(
      std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
  return std::make_shared<InputState>(InputState{.bytes = std::move(bytes)});
}

void checkCarquet(carquet_status_t status) {
  if (status != CARQUET_OK) {
    throw std::runtime_error(carquet_status_string(status));
  }
}

// Generate only the row-group/null cases missing from the existing datasets.
std::shared_ptr<InputState> scalarFileState(
    int groups,
    decltype(CARQUET_COMPRESSION_ZSTD) codec = CARQUET_COMPRESSION_ZSTD) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_schema_t, decltype(&carquet_schema_free)> schema(
      carquet_schema_create(&error), carquet_schema_free);
  if (!schema) {
    throw std::runtime_error(error.message);
  }
  checkCarquet(carquet_schema_add_column(schema.get(), "id",
                                         CARQUET_PHYSICAL_INT64, nullptr,
                                         CARQUET_REPETITION_REQUIRED, 0, 0));
  checkCarquet(carquet_schema_add_column(schema.get(), "flag",
                                         CARQUET_PHYSICAL_BOOLEAN, nullptr,
                                         CARQUET_REPETITION_OPTIONAL, 0, 0));
  checkCarquet(carquet_schema_add_column(schema.get(), "measurement",
                                         CARQUET_PHYSICAL_FLOAT, nullptr,
                                         CARQUET_REPETITION_OPTIONAL, 0, 0));
  carquet_logical_type_t stringType{};
  stringType.id = CARQUET_LOGICAL_STRING;
  checkCarquet(carquet_schema_add_column(
      schema.get(), "category", CARQUET_PHYSICAL_BYTE_ARRAY, &stringType,
      CARQUET_REPETITION_OPTIONAL, 0, 0));
  carquet_writer_options_t options;
  carquet_writer_options_init(&options);
  options.compression = codec;
  std::unique_ptr<carquet_writer_t, decltype(&carquet_writer_abort)> writer(
      carquet_writer_create_buffer(schema.get(), &options, &error),
      carquet_writer_abort);
  if (!writer) {
    throw std::runtime_error(error.message);
  }
  const int16_t defined[] = {1, 1, 0, 1, 1, 1};
  const uint8_t flags[] = {1, 0, 0, 1, 0};
  const float measurements[] = {0.5f, -1.25f, 3.5f, 4.5f, 5.5f};
  std::string labels[] = {"", "中文", "b", "a", "c"};
  carquet_byte_array_t strings[5];
  for (size_t i = 0; i < 5; ++i) {
    strings[i] = {reinterpret_cast<uint8_t*>(labels[i].data()),
                  static_cast<int32_t>(labels[i].size())};
  }
  for (int group = 0; group < groups; ++group) {
    const int64_t ids[] = {group * 6 + 1, group * 6 + 2, group * 6 + 3,
                           group * 6 + 4, group * 6 + 5, group * 6 + 6};
    checkCarquet(
        carquet_writer_write_batch(writer.get(), 0, ids, 6, nullptr, nullptr));
    checkCarquet(carquet_writer_write_batch(writer.get(), 1, flags, 6, defined,
                                            nullptr));
    checkCarquet(carquet_writer_write_batch(writer.get(), 2, measurements, 6,
                                            defined, nullptr));
    checkCarquet(carquet_writer_write_batch(writer.get(), 3, strings, 6,
                                            defined, nullptr));
    if (group + 1 < groups) {
      checkCarquet(carquet_writer_new_row_group(writer.get()));
    }
  }
  checkCarquet(carquet_writer_close(writer.get()));
  void* data = nullptr;
  size_t size = 0;
  checkCarquet(carquet_writer_get_buffer(writer.get(), &data, &size));
  writer.release();  // get_buffer consumes the closed buffer writer.
  std::unique_ptr<void, decltype(&std::free)> buffer(data, std::free);
  auto bytes = std::make_shared<std::vector<uint8_t>>(
      static_cast<uint8_t*>(data), static_cast<uint8_t*>(data) + size);
  return std::make_shared<InputState>(InputState{.bytes = std::move(bytes)});
}

template <typename T>
T valueAt(const std::shared_ptr<DataChunk>& chunk, int column, size_t row) {
  return chunk->get(column)->get_elem(row).GetValue<T>();
}

void expectPrimitive(const std::shared_ptr<::common::DataType>& type,
                     ::common::PrimitiveType expected) {
  ASSERT_NE(type, nullptr);
  EXPECT_EQ(type->item_case(), ::common::DataType::kPrimitiveType);
  EXPECT_EQ(type->primitive_type(), expected);
}

TEST(CarquetChunkSupplierTest, StreamsOwnedScalarChunksAcrossRowGroups) {
  for (auto codec :
       {CARQUET_COMPRESSION_UNCOMPRESSED, CARQUET_COMPRESSION_SNAPPY,
        CARQUET_COMPRESSION_GZIP, CARQUET_COMPRESSION_ZSTD}) {
    SCOPED_TRACE(static_cast<int>(codec));
    auto state = scalarFileState(2, codec);
    std::shared_ptr<DataChunk> retained;
    std::vector<int64_t> ids;
    std::vector<std::string> categories;
    {
      auto supplier =
          CarquetChunkSupplier::create(std::make_unique<MemoryInput>(state),
                                       {.batchSize = 3, .numThreads = 1});
      ASSERT_TRUE(supplier) << supplier.error().ToString();
      EXPECT_EQ((*supplier)->RowNum(), 12);
      ASSERT_EQ(
          (*supplier)->schema()->columnNames,
          (std::vector<std::string>{"id", "flag", "measurement", "category"}));
      expectPrimitive((*supplier)->schema()->columnTypes[0],
                      ::common::DT_SIGNED_INT64);
      expectPrimitive((*supplier)->schema()->columnTypes[1], ::common::DT_BOOL);
      expectPrimitive((*supplier)->schema()->columnTypes[2],
                      ::common::DT_FLOAT);
      EXPECT_EQ((*supplier)->schema()->columnTypes[3]->item_case(),
                ::common::DataType::kString);
      while (auto chunk = (*supplier)->GetNextChunk()) {
        EXPECT_GT(chunk->row_num(), 0u);
        EXPECT_LE(chunk->row_num(), 3u);
        ASSERT_EQ(chunk->col_num(), 4u);
        if (!retained) {
          retained = chunk;
        }
        for (size_t row = 0; row < chunk->row_num(); ++row) {
          const int64_t id = valueAt<int64_t>(chunk, 0, row);
          ids.push_back(id);
          for (int col = 1; col < 4; ++col) {
            EXPECT_EQ(chunk->get(col)->get_elem(row).IsNull(), id % 6 == 3);
          }
          if (id % 6 != 3) {
            categories.push_back(valueAt<std::string>(chunk, 3, row));
          }
        }
      }
      EXPECT_EQ((*supplier)->GetNextChunk(), nullptr);
      EXPECT_EQ((*supplier)->GetNextChunk(), nullptr);
    }
    EXPECT_EQ(ids,
              (std::vector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}));
    EXPECT_EQ(categories,
              (std::vector<std::string>{"", "中文", "b", "a", "c", "", "中文",
                                        "b", "a", "c"}));
    ASSERT_NE(retained, nullptr);
    EXPECT_EQ(valueAt<int64_t>(retained, 0, 0), 1);
    EXPECT_TRUE(valueAt<bool>(retained, 1, 0));
    EXPECT_FALSE(valueAt<bool>(retained, 1, 1));
    EXPECT_FLOAT_EQ(valueAt<float>(retained, 2, 1), -1.25f);
    EXPECT_EQ(valueAt<std::string>(retained, 3, 1), "中文");
    EXPECT_TRUE(retained->get(3)->get_elem(2).IsNull());
    EXPECT_EQ(state->closeCalls, 1);
    EXPECT_EQ(state->destructCalls, 1);
  }
}

TEST(CarquetChunkSupplierTest, ReadsExistingIndependentScalarDataset) {
  auto state = fixtureState("comprehensive_graph/parquet/node_a.parquet");
  auto supplier = CarquetChunkSupplier::create(
      std::make_unique<MemoryInput>(state), {.batchSize = 3});
  ASSERT_TRUE(supplier) << supplier.error().ToString();
  ASSERT_EQ((*supplier)->RowNum(), 10);
  const auto& types = (*supplier)->schema()->columnTypes;
  ASSERT_EQ(types.size(), 11u);
  expectPrimitive(types[1], ::common::DT_SIGNED_INT32);
  expectPrimitive(types[3], ::common::DT_UNSIGNED_INT32);
  expectPrimitive(types[4], ::common::DT_UNSIGNED_INT64);
  EXPECT_TRUE(types[8]->temporal().has_date());
  EXPECT_TRUE(types[9]->temporal().has_timestamp());
  auto chunk = (*supplier)->GetNextChunk();
  ASSERT_NE(chunk, nullptr);
  ASSERT_EQ(chunk->col_num(), 11u);
  ASSERT_EQ(chunk->row_num(), 3u);
  EXPECT_EQ(valueAt<int32_t>(chunk, 1, 0), -123456789);
  EXPECT_EQ(valueAt<int64_t>(chunk, 2, 0), std::numeric_limits<int64_t>::max());
  EXPECT_EQ(valueAt<int64_t>(chunk, 2, 1), std::numeric_limits<int64_t>::min());
  EXPECT_EQ(valueAt<uint32_t>(chunk, 3, 0),
            std::numeric_limits<uint32_t>::max());
  EXPECT_EQ(valueAt<uint64_t>(chunk, 4, 0),
            std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(valueAt<uint64_t>(chunk, 4, 2), uint64_t{1} << 63);
  EXPECT_FLOAT_EQ(valueAt<float>(chunk, 5, 0), 3.1415927f);
  EXPECT_DOUBLE_EQ(valueAt<double>(chunk, 6, 0), 2.718281828459045);
  EXPECT_EQ(valueAt<std::string>(chunk, 7, 0), "test_string_0");
  EXPECT_EQ(valueAt<Date>(chunk, 8, 0).to_string(), "2023-01-15");
  EXPECT_EQ(valueAt<DateTime>(chunk, 9, 0).milli_second, 1673740800000LL);
  size_t rows = chunk->row_num();
  while (auto next = (*supplier)->GetNextChunk()) {
    rows += next->row_num();
  }
  EXPECT_EQ(rows, 10u);
  supplier->reset();
  EXPECT_EQ(valueAt<std::string>(chunk, 7, 0), "test_string_0");
  EXPECT_EQ(state->closeCalls, 1);
}

TEST(CarquetChunkSupplierTest, SniffsMetadataThroughFreshExistingStreams) {
  auto bytes = fixtureState("tinysnb/parquet/eMeets.parquet")->bytes;
  std::vector<std::shared_ptr<InputState>> states;
  CarquetSniffer sniffer([&]() {
    auto state = std::make_shared<InputState>(InputState{.bytes = bytes});
    states.push_back(state);
    return std::make_unique<MemoryInput>(state);
  });
  for (int attempt = 0; attempt < 2; ++attempt) {
    auto schema = sniffer.sniff();
    ASSERT_TRUE(schema) << schema.error().ToString();
    EXPECT_EQ(
        (*schema)->columnNames,
        (std::vector<std::string>{"from", "to", "location", "times", "data"}));
  }
  ASSERT_EQ(states.size(), 2u);
  for (const auto& state : states) {
    EXPECT_GT(state->readCalls, 0);
    EXPECT_EQ(state->closeCalls, 1);
    EXPECT_EQ(state->destructCalls, 1);
  }
  auto nested =
      fixtureState("comprehensive_graph/parquet/parquet_list.parquet");
  CarquetSniffer unsupported(
      [nested]() { return std::make_unique<MemoryInput>(nested); });
  auto schema = unsupported.sniff();
  ASSERT_FALSE(schema);
  EXPECT_EQ(schema.error().error_code(), StatusCode::ERR_TYPE_CONVERSION);
  EXPECT_EQ(nested->closeCalls, 1);
}

TEST(CarquetChunkSupplierTest, HandlesEmptyInvalidAndMalformedInputs) {
  auto empty = scalarFileState(0);
  {
    auto supplier =
        CarquetChunkSupplier::create(std::make_unique<MemoryInput>(empty));
    ASSERT_TRUE(supplier) << supplier.error().ToString();
    EXPECT_EQ((*supplier)->RowNum(), 0);
    EXPECT_EQ((*supplier)->GetNextChunk(), nullptr);
    EXPECT_EQ((*supplier)->GetNextChunk(), nullptr);
  }
  EXPECT_EQ(empty->closeCalls, 1);
  for (auto options : {CarquetReaderOptions{.batchSize = 0},
                       CarquetReaderOptions{.numThreads = -1}}) {
    auto rejected = scalarFileState(0);
    auto bad = CarquetChunkSupplier::create(
        std::make_unique<MemoryInput>(rejected), options);
    ASSERT_FALSE(bad);
    EXPECT_EQ(bad.error().error_code(), StatusCode::ERR_INVALID_ARGUMENT);
    EXPECT_EQ(rejected->closeCalls, 1);
    EXPECT_EQ(rejected->destructCalls, 1);
  }
  auto broken = std::make_shared<InputState>(
      InputState{.bytes = std::make_shared<std::vector<uint8_t>>(
                     std::initializer_list<uint8_t>{'N', 'O', 'P', 'E'})});
  auto invalid =
      CarquetChunkSupplier::create(std::make_unique<MemoryInput>(broken));
  ASSERT_FALSE(invalid);
  EXPECT_EQ(invalid.error().error_code(), StatusCode::ERR_IO_ERROR);
  EXPECT_EQ(broken->closeCalls, 1);
  EXPECT_EQ(broken->destructCalls, 1);
  auto nested =
      fixtureState("comprehensive_graph/parquet/parquet_list.parquet");
  auto unsupported =
      CarquetChunkSupplier::create(std::make_unique<MemoryInput>(nested));
  ASSERT_FALSE(unsupported);
  EXPECT_EQ(unsupported.error().error_code(), StatusCode::ERR_TYPE_CONVERSION);
  EXPECT_EQ(nested->closeCalls, 1);
  EXPECT_EQ(nested->destructCalls, 1);
  EXPECT_FALSE(CarquetChunkSupplier::create(nullptr));
  CarquetSniffer nullFactory(nullptr);
  EXPECT_FALSE(nullFactory.sniff());
  CarquetSniffer nullStream(
      []() { return std::unique_ptr<io::InputStream>{}; });
  EXPECT_FALSE(nullStream.sniff());
}

TEST(CarquetChunkSupplierTest, ReleasesInputAfterMidReadFailure) {
  auto state = scalarFileState(2);
  {
    auto supplier =
        CarquetChunkSupplier::create(std::make_unique<MemoryInput>(state));
    ASSERT_TRUE(supplier) << supplier.error().ToString();
    state->failReads = true;
    EXPECT_THROW((*supplier)->GetNextChunk(), exception::IOException);
  }
  EXPECT_EQ(state->closeCalls, 1);
  EXPECT_EQ(state->destructCalls, 1);
}

template <typename Source, typename Dest>
void expectSlicedPrimitive(const char* format) {
  SCOPED_TRACE(format);
  const Source data[] = {0, std::numeric_limits<Source>::lowest(), 0,
                         std::numeric_limits<Source>::max()};
  const uint8_t validity = 0x0a;  // Indices 1 and 3 are valid.
  const void* buffers[] = {&validity, data};
  ArrowSchema schema{.format = format};
  ArrowArray array{.length = 3,
                   .null_count = -1,
                   .offset = 1,
                   .n_buffers = 2,
                   .buffers = buffers};
  auto converted = convertArrowCScalarColumn(schema, array);
  ASSERT_TRUE(converted) << converted.error().ToString();
  EXPECT_EQ((*converted)->get_elem(0).GetValue<Dest>(),
            static_cast<Dest>(data[1]));
  EXPECT_TRUE((*converted)->get_elem(1).IsNull());
  EXPECT_EQ((*converted)->get_elem(2).GetValue<Dest>(),
            static_cast<Dest>(data[3]));
}

TEST(CarquetScalarConverterTest, PreservesSlicedNumericValuesAndNulls) {
  expectSlicedPrimitive<int8_t, int32_t>("c");
  expectSlicedPrimitive<int16_t, int32_t>("s");
  expectSlicedPrimitive<int32_t, int32_t>("i");
  expectSlicedPrimitive<int64_t, int64_t>("l");
  expectSlicedPrimitive<uint8_t, uint32_t>("C");
  expectSlicedPrimitive<uint16_t, uint32_t>("S");
  expectSlicedPrimitive<uint32_t, uint32_t>("I");
  expectSlicedPrimitive<uint64_t, uint64_t>("L");
  expectSlicedPrimitive<float, float>("f");
  expectSlicedPrimitive<double, double>("g");
}

TEST(CarquetScalarConverterTest, PreservesFloatingSpecialValues) {
  const double values[] = {std::numeric_limits<double>::quiet_NaN(),
                           std::numeric_limits<double>::infinity(),
                           -std::numeric_limits<double>::infinity()};
  const void* buffers[] = {nullptr, values};
  ArrowSchema schema{.format = "g"};
  ArrowArray array{.length = 3, .n_buffers = 2, .buffers = buffers};
  auto converted = convertArrowCScalarColumn(schema, array);
  ASSERT_TRUE(converted) << converted.error().ToString();
  EXPECT_FALSE((*converted)->get_elem(0).IsNull());
  EXPECT_TRUE(std::isnan((*converted)->get_elem(0).GetValue<double>()));
  EXPECT_EQ((*converted)->get_elem(1).GetValue<double>(), values[1]);
  EXPECT_EQ((*converted)->get_elem(2).GetValue<double>(), values[2]);
}

TEST(CarquetScalarConverterTest, ConvertsTemporalUnitsAndRejectsOverflow) {
  const int64_t ticks[] = {0, -1000000, 123000000, 0};
  const uint8_t validity = 0x07;
  const void* buffers[] = {&validity, ticks};
  ArrowArray array{
      .length = 4, .null_count = 1, .n_buffers = 2, .buffers = buffers};
  struct TimestampCase {
    const char* format;
    int64_t negativeMillis;
    int64_t positiveMillis;
  };
  for (const auto& test : {TimestampCase{"tss:", -1000000000, 123000000000},
                           {"tsm:", -1000000, 123000000},
                           {"tsu:", -1000, 123000},
                           {"tsn:", -1, 123}}) {
    ArrowSchema schema{.format = test.format};
    auto converted = convertArrowCScalarColumn(schema, array);
    ASSERT_TRUE(converted) << converted.error().ToString();
    EXPECT_EQ((*converted)->get_elem(0).GetValue<DateTime>().milli_second, 0);
    EXPECT_EQ((*converted)->get_elem(1).GetValue<DateTime>().milli_second,
              test.negativeMillis);
    EXPECT_EQ((*converted)->get_elem(2).GetValue<DateTime>().milli_second,
              test.positiveMillis);
    EXPECT_TRUE((*converted)->get_elem(3).IsNull());
  }
  const int64_t dateMillis[] = {0, -86400000, 1709164800000, 0};
  buffers[1] = dateMillis;
  ArrowSchema dateSchema{.format = "tdm"};
  auto dates = convertArrowCScalarColumn(dateSchema, array);
  ASSERT_TRUE(dates) << dates.error().ToString();
  EXPECT_EQ((*dates)->get_elem(1).GetValue<Date>().to_num_days(), -1);
  EXPECT_EQ((*dates)->get_elem(2).GetValue<Date>().to_string(), "2024-02-29");
  EXPECT_TRUE((*dates)->get_elem(3).IsNull());
  const int64_t overflow[] = {std::numeric_limits<int64_t>::min(),
                              std::numeric_limits<int64_t>::max()};
  array.length = 1;
  array.null_count = 0;
  for (const auto& value : overflow) {
    buffers[1] = &value;
    for (const char* format : {"tss:", "tdm"}) {
      ArrowSchema schema{.format = format};
      EXPECT_FALSE(convertArrowCScalarColumn(schema, array));
    }
  }
}

TEST(CarquetScalarConverterTest, ValidatesStringsAndOwnsLargeStringSlices) {
  const int32_t badOffsets[] = {0, 4, 2};
  const void* buffers[] = {nullptr, badOffsets, "abcd"};
  ArrowSchema schema{.format = "u", .name = "label"};
  ArrowArray array{
      .length = 2, .null_count = 0, .n_buffers = 3, .buffers = buffers};
  auto malformed = convertArrowCScalarColumn(schema, array);
  ASSERT_FALSE(malformed);
  EXPECT_NE(malformed.error().error_message().find("decreasing"),
            std::string::npos);
  int64_t offsets[] = {0, 4, 4, 10, 10};
  std::string text = "skip中文";
  const uint8_t validity = 0x07;
  buffers[0] = &validity;
  buffers[1] = offsets;
  buffers[2] = text.data();
  schema.format = "U";
  array.length = 3;
  array.offset = 1;
  array.null_count = 1;
  auto converted = convertArrowCScalarColumn(schema, array);
  ASSERT_TRUE(converted) << converted.error().ToString();
  text.assign(text.size(), 'x');
  EXPECT_EQ((*converted)->get_elem(0).GetValue<std::string>(), "");
  EXPECT_EQ((*converted)->get_elem(1).GetValue<std::string>(), "中文");
  EXPECT_TRUE((*converted)->get_elem(2).IsNull());
}

TEST(CarquetScalarConverterTest, RejectsInvalidSchemasAndStructLayouts) {
  ArrowSchema field{.format = "l", .name = "id"};
  ArrowSchema* children[] = {&field, &field};
  ArrowSchema root{.format = "+s", .n_children = 2, .children = children};
  EXPECT_FALSE(convertArrowCStructSchema(root));  // Duplicate field names.
  field.name = nullptr;
  root.n_children = 1;
  EXPECT_FALSE(convertArrowCStructSchema(root));
  field.name = "id";
  field.format = "z";
  EXPECT_FALSE(convertArrowCStructSchema(root));
  field.format = "l";
  field.dictionary = &field;
  EXPECT_FALSE(convertArrowCStructSchema(root));
  field.dictionary = nullptr;
  const int64_t value = 1;
  const void* scalarBuffers[] = {nullptr, &value};
  ArrowArray child{.length = 1, .n_buffers = 2, .buffers = scalarBuffers};
  ArrowArray* arrays[] = {&child};
  const void* rootBuffers[] = {nullptr};
  ArrowArray array{.length = 2,
                   .n_buffers = 1,
                   .n_children = 1,
                   .buffers = rootBuffers,
                   .children = arrays};
  EXPECT_FALSE(
      convertArrowCFlatStruct(root, array));  // Child row count mismatch.
  array.length = 1;
  EXPECT_TRUE(convertArrowCFlatStruct(root, array));
  child.offset = std::numeric_limits<int64_t>::max();
  EXPECT_FALSE(convertArrowCScalarColumn(field, child));
  child.offset = 0;
  child.null_count = 1;
  EXPECT_FALSE(convertArrowCScalarColumn(field, child));  // Missing validity.
}

}  // namespace
}  // namespace neug::parquet
