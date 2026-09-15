/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "carquet/c_data_writer.h"
#include "carquet/export_writer.h"
#include "carquet/output_adapter.h"
#include "neug/utils/property/types.h"

namespace neug::parquet {
namespace {

struct OutputState {
  std::vector<uint8_t> bytes;
  int writes = 0;
  int closes = 0;
  int aborts = 0;
  int destroys = 0;
  int failOnWrite = 0;
  bool failWrite = false;
  bool failClose = false;
};

class MemoryOutput final : public io::OutputStream {
 public:
  explicit MemoryOutput(std::shared_ptr<OutputState> state)
      : state_(std::move(state)) {}
  ~MemoryOutput() override { ++state_->destroys; }

  Status Write(const uint8_t* data, int64_t nbytes) override {
    ++state_->writes;
    if (state_->failWrite || state_->writes == state_->failOnWrite) {
      return Status(StatusCode::ERR_IO_ERROR, "injected write failure");
    }
    state_->bytes.insert(state_->bytes.end(), data, data + nbytes);
    return Status::OK();
  }

  Status Close() override {
    ++state_->closes;
    if (state_->failClose) {
      return Status(StatusCode::ERR_IO_ERROR, "injected close failure");
    }
    return Status::OK();
  }

  void Abort() override {
    ++state_->aborts;
    state_->bytes.clear();
  }

 private:
  std::shared_ptr<OutputState> state_;
};

reader::FileSchema fileSchema(
    std::string path,
    common::case_insensitive_map_t<std::string> options = {}) {
  reader::FileSchema schema;
  schema.paths = {std::move(path)};
  schema.format = "parquet";
  schema.options = std::move(options);
  return schema;
}

Status writeResponse(const reader::FileSchema& schema,
                     const QueryResponse* response,
                     const std::shared_ptr<OutputState>& state,
                     int* openCalls = nullptr) {
  CarquetExportWriter writer(schema);
  writer.setStreamOpener([state, openCalls]() {
    if (openCalls) {
      ++*openCalls;
    }
    return std::make_unique<MemoryOutput>(state);
  });
  return writer.writeTable(response);
}

std::string validityExceptRowTwo() {
  return std::string(1, static_cast<char>(0b00011011));
}

QueryResponse makeFlatResponse() {
  QueryResponse response;
  response.set_row_count(5);
  for (const char* name : {"i32", "u32", "i64", "u64", "f32", "f64", "flag",
                           "text", "day", "instant"}) {
    response.mutable_schema()->add_name(name);
  }
  const auto validity = validityExceptRowTwo();

  auto* i32 = response.add_arrays()->mutable_int32_array();
  for (const int32_t value : {-2, -1, 0, 1, 2}) {
    i32->add_values(value);
  }
  i32->set_validity(validity);

  auto* u32 = response.add_arrays()->mutable_uint32_array();
  for (const uint32_t value : {0U, 1U, 2U, 3U, UINT32_MAX}) {
    u32->add_values(value);
  }
  u32->set_validity(validity);

  auto* i64 = response.add_arrays()->mutable_int64_array();
  for (const int64_t value : {-9, -8, 0, 8, 9}) {
    i64->add_values(value);
  }
  i64->set_validity(validity);

  auto* u64 = response.add_arrays()->mutable_uint64_array();
  for (const uint64_t value : {uint64_t{0}, uint64_t{1}, uint64_t{2},
                               uint64_t{3}, uint64_t{UINT64_MAX}}) {
    u64->add_values(value);
  }
  u64->set_validity(validity);

  auto* f32 = response.add_arrays()->mutable_float_array();
  for (const float value : {-1.5F, 0.0F, 1.0F, 2.5F, 4.0F}) {
    f32->add_values(value);
  }
  f32->set_validity(validity);

  auto* f64 = response.add_arrays()->mutable_double_array();
  for (const double value : {-4.5, -0.0, 1.0, 3.5, 9.0}) {
    f64->add_values(value);
  }
  f64->set_validity(validity);

  auto* flag = response.add_arrays()->mutable_bool_array();
  for (const bool value : {true, false, true, false, true}) {
    flag->add_values(value);
  }
  flag->set_validity(validity);

  auto* text = response.add_arrays()->mutable_string_array();
  for (const char* value : {"", "hello", "ignored", "中文", "tail"}) {
    text->add_values(value);
  }
  text->set_validity(validity);

  auto* day = response.add_arrays()->mutable_date_array();
  // QueryExportWriter stores Date::to_timestamp(). A Date produced from a
  // DateTime can retain its hour, so valid payloads need not be at midnight.
  for (const int64_t value : {-43200000, 43200000, 1, 129600000, 216000000}) {
    day->add_values(value);
  }
  day->set_validity(validity);

  auto* instant = response.add_arrays()->mutable_timestamp_array();
  for (const int64_t value : {-1000001, -1, 0, 1, 1000001}) {
    instant->add_values(value);
  }
  instant->set_validity(validity);
  return response;
}

::common::DataType fixedType(const ::common::DataType& component,
                             uint32_t length) {
  ::common::DataType type;
  *type.mutable_array()->mutable_component_type() = component;
  type.mutable_array()->set_fixed_length(length);
  return type;
}

std::shared_ptr<reader::TableEntrySchema> nestedEntrySchema() {
  auto schema = std::make_shared<reader::TableEntrySchema>();
  schema->columnNames = {"numbers", "matrix", "record", "duration",
                         "vertex",  "edge",   "path"};

  auto numbers = std::make_shared<::common::DataType>();
  numbers->mutable_list()->mutable_component_type()->set_primitive_type(
      ::common::DT_SIGNED_INT64);
  schema->columnTypes.push_back(numbers);

  ::common::DataType element;
  element.set_primitive_type(::common::DT_SIGNED_INT32);
  auto inner = fixedType(element, 2);
  schema->columnTypes.push_back(
      std::make_shared<::common::DataType>(fixedType(inner, 2)));

  auto record = std::make_shared<::common::DataType>();
  record->mutable_tuple()->add_component_types()->set_primitive_type(
      ::common::DT_SIGNED_INT32);
  record->mutable_tuple()->add_component_types()->mutable_string();
  schema->columnTypes.push_back(record);

  for (int column = 0; column < 4; ++column) {
    auto string = std::make_shared<::common::DataType>();
    string->mutable_string()->mutable_long_text();
    schema->columnTypes.push_back(string);
  }
  return schema;
}

QueryResponse makeNestedResponse() {
  QueryResponse response;
  response.set_row_count(4);
  for (const char* name :
       {"numbers", "matrix", "record", "duration", "vertex", "edge", "path"}) {
    response.mutable_schema()->add_name(name);
  }

  auto* numbers = response.add_arrays()->mutable_list_array();
  for (const uint32_t offset : {0U, 3U, 3U, 3U, 5U}) {
    numbers->add_offsets(offset);
  }
  numbers->set_validity(std::string(1, static_cast<char>(0b00001011)));
  auto* numberValues = numbers->mutable_elements()->mutable_int64_array();
  for (const int64_t value : {1, 2, 3, 4, 5}) {
    numberValues->add_values(value);
  }
  numberValues->set_validity(std::string(1, static_cast<char>(0b00011101)));

  // QueryResponse serializes fixed ARRAY values through list_array. The entry
  // schema below validates both dimensions, while the exported Parquet schema
  // intentionally keeps the baseline LIST<LIST<INT32>> representation.
  auto* matrix = response.add_arrays()->mutable_list_array();
  for (const uint32_t offset : {0U, 2U, 4U, 6U, 8U}) {
    matrix->add_offsets(offset);
  }
  matrix->set_validity(std::string(1, static_cast<char>(0b00001101)));
  auto* rows = matrix->mutable_elements()->mutable_list_array();
  for (uint32_t offset = 0; offset <= 16; offset += 2) {
    rows->add_offsets(offset);
  }
  rows->set_validity(std::string(1, static_cast<char>(0b11011111)));
  auto* cells = rows->mutable_elements()->mutable_int32_array();
  for (int32_t value = 0; value < 16; ++value) {
    cells->add_values(100 + value);
  }
  cells->set_validity(std::string(2, static_cast<char>(0xff)));
  (*cells->mutable_validity())[0] &= static_cast<char>(~(1U << 2));

  auto* record = response.add_arrays()->mutable_struct_array();
  record->set_validity(std::string(1, static_cast<char>(0b00001101)));
  auto* count = record->add_fields()->mutable_int32_array();
  for (const int32_t value : {7, 8, 9, 10}) {
    count->add_values(value);
  }
  count->set_validity(std::string(1, static_cast<char>(0b00001011)));
  auto* label = record->add_fields()->mutable_string_array();
  for (const char* value : {"a", "b", "c", "d"}) {
    label->add_values(value);
  }
  label->set_validity(std::string(1, static_cast<char>(0b00000111)));

  const auto specialValidity = std::string(1, static_cast<char>(0b00001011));
  auto* duration = response.add_arrays()->mutable_interval_array();
  auto* vertex = response.add_arrays()->mutable_vertex_array();
  auto* edge = response.add_arrays()->mutable_edge_array();
  auto* path = response.add_arrays()->mutable_path_array();
  for (int row = 0; row < 4; ++row) {
    duration->add_values("P" + std::to_string(row) + "D");
    vertex->add_values("{\"vertex\":" + std::to_string(row) + "}");
    edge->add_values("{\"edge\":" + std::to_string(row) + "}");
    path->add_values("{\"path\":" + std::to_string(row) + "}");
  }
  duration->set_validity(specialValidity);
  vertex->set_validity(specialValidity);
  edge->set_validity(specialValidity);
  path->set_validity(specialValidity);
  return response;
}

std::shared_ptr<arrow::Buffer> wrap(const std::vector<uint8_t>& bytes) {
  return std::make_shared<arrow::Buffer>(bytes.data(),
                                         static_cast<int64_t>(bytes.size()));
}

std::shared_ptr<arrow::Table> readWithArrow(const std::vector<uint8_t>& bytes) {
  auto input = std::make_shared<arrow::io::BufferReader>(wrap(bytes));
  std::unique_ptr<::parquet::arrow::FileReader> reader;
  auto status =
      ::parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader);
  if (!status.ok()) {
    ADD_FAILURE() << status.ToString();
    return nullptr;
  }
  std::shared_ptr<arrow::Table> table;
  status = reader->ReadTable(&table);
  if (!status.ok()) {
    ADD_FAILURE() << status.ToString();
    return nullptr;
  }
  auto combined = table->CombineChunks();
  if (!combined.ok()) {
    ADD_FAILURE() << combined.status().ToString();
    return nullptr;
  }
  return *combined;
}

std::unique_ptr<::parquet::ParquetFileReader> openMetadata(
    const std::vector<uint8_t>& bytes) {
  return ::parquet::ParquetFileReader::Open(
      std::make_shared<arrow::io::BufferReader>(wrap(bytes)));
}

template <typename ArrayType, typename Value>
void expectScalar(const std::shared_ptr<arrow::Table>& table, int column,
                  Value first, Value last) {
  auto array =
      std::static_pointer_cast<ArrayType>(table->column(column)->chunk(0));
  ASSERT_EQ(array->length(), 5);
  EXPECT_EQ(array->Value(0), first);
  EXPECT_TRUE(array->IsNull(2));
  EXPECT_EQ(array->Value(4), last);
}

TEST(CarquetExportWriterTest, WritesFlatValuesForIndependentArrowReader) {
  auto state = std::make_shared<OutputState>();
  auto schema = fileSchema("flat", {{"compression", "zstd"},
                                    {"row_group_size", "2"},
                                    {"dictionary_encoding", "true"}});
  auto response = makeFlatResponse();
  auto status = writeResponse(schema, &response, state);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_GT(state->writes, 1);
  EXPECT_EQ(state->closes, 1);
  EXPECT_EQ(state->aborts, 0);
  EXPECT_EQ(state->destroys, 1);

  auto raw = openMetadata(state->bytes);
  ASSERT_NE(raw, nullptr);
  ASSERT_EQ(raw->metadata()->num_rows(), 5);
  ASSERT_EQ(raw->metadata()->num_row_groups(), 3);
  for (int group = 0; group < 3; ++group) {
    EXPECT_EQ(raw->metadata()->RowGroup(group)->ColumnChunk(0)->compression(),
              ::parquet::Compression::ZSTD);
  }

  auto table = readWithArrow(state->bytes);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->num_columns(), 10);
  expectScalar<arrow::Int32Array>(table, 0, int32_t{-2}, int32_t{2});
  expectScalar<arrow::UInt32Array>(table, 1, uint32_t{0}, UINT32_MAX);
  expectScalar<arrow::Int64Array>(table, 2, int64_t{-9}, int64_t{9});
  expectScalar<arrow::UInt64Array>(table, 3, uint64_t{0}, UINT64_MAX);
  expectScalar<arrow::FloatArray>(table, 4, -1.5F, 4.0F);
  expectScalar<arrow::DoubleArray>(table, 5, -4.5, 9.0);
  expectScalar<arrow::BooleanArray>(table, 6, true, true);
  auto text =
      std::static_pointer_cast<arrow::StringArray>(table->column(7)->chunk(0));
  EXPECT_EQ(text->GetString(0), "");
  EXPECT_TRUE(text->IsNull(2));
  EXPECT_EQ(text->GetString(3), "中文");
  EXPECT_EQ(text->GetString(4), "tail");
  expectScalar<arrow::Date32Array>(table, 8, int32_t{-1}, int32_t{2});
  expectScalar<arrow::TimestampArray>(table, 9, int64_t{-1000001},
                                      int64_t{1000001});
  EXPECT_EQ(table->schema()->field(8)->type(), arrow::date32());
  EXPECT_TRUE(table->schema()->field(9)->type()->Equals(
      arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")));
}

TEST(CarquetExportWriterTest, HonorsCompressionDictionaryAndRowGroupOptions) {
  struct Codec {
    const char* option;
    ::parquet::Compression::type expected;
  };
  for (const auto& codec : {Codec{"none", ::parquet::Compression::UNCOMPRESSED},
                            Codec{"snappy", ::parquet::Compression::SNAPPY},
                            Codec{"gzip", ::parquet::Compression::GZIP},
                            Codec{"zstd", ::parquet::Compression::ZSTD}}) {
    for (const bool dictionary : {false, true}) {
      auto state = std::make_shared<OutputState>();
      auto schema = fileSchema(
          std::string(codec.option) + (dictionary ? "-dict" : "-plain"),
          {{"compression", codec.option},
           {"row_group_size", "32"},
           {"dictionary_encoding", dictionary ? "true" : "false"}});
      QueryResponse response;
      response.set_row_count(96);
      response.mutable_schema()->add_name("category");
      auto* values = response.add_arrays()->mutable_string_array();
      for (int row = 0; row < response.row_count(); ++row) {
        values->add_values("repeated-value-" + std::to_string(row % 3));
      }

      auto status = writeResponse(schema, &response, state);
      ASSERT_TRUE(status.ok()) << codec.option << ": " << status.ToString();
      auto raw = openMetadata(state->bytes);
      ASSERT_NE(raw, nullptr);
      ASSERT_EQ(raw->metadata()->num_row_groups(), 3);
      auto chunk = raw->metadata()->RowGroup(0)->ColumnChunk(0);
      EXPECT_EQ(chunk->compression(), codec.expected);
      const auto encodings = chunk->encodings();
      const bool hasDictionary =
          std::find(encodings.begin(), encodings.end(),
                    ::parquet::Encoding::RLE_DICTIONARY) != encodings.end() ||
          std::find(encodings.begin(), encodings.end(),
                    ::parquet::Encoding::PLAIN_DICTIONARY) != encodings.end();
      EXPECT_EQ(hasDictionary, dictionary) << codec.option;
      auto table = readWithArrow(state->bytes);
      ASSERT_NE(table, nullptr);
      auto actual = std::static_pointer_cast<arrow::StringArray>(
          table->column(0)->chunk(0));
      ASSERT_EQ(actual->length(), response.row_count());
      for (int row = 0; row < response.row_count(); ++row) {
        EXPECT_EQ(actual->GetString(row), values->values(row));
      }
    }
  }
}

TEST(CarquetExportWriterTest,
     PreservesTimestampMillisecondsAndNormalizesDatePayloads) {
  QueryResponse response;
  response.set_row_count(5);
  auto* timestamps = response.add_arrays()->mutable_timestamp_array();
  // Use the same DateTime representation that QueryExportWriter sinks.
  const DateTime millennium("2000-01-01");
  ASSERT_EQ(millennium.milli_second, 946684800000LL);
  for (const int64_t millis :
       {-1001LL, -1LL, 0LL, 946684800000LL, 946684800123LL}) {
    timestamps->add_values(millis);
  }
  timestamps->set_values(3, millennium.milli_second);
  auto* dates = response.add_arrays()->mutable_date_array();
  constexpr int64_t kDayMillis = 86400000;
  const std::vector<std::pair<int64_t, int32_t>> dateCases = {
      {static_cast<int64_t>(INT32_MIN) * kDayMillis + kDayMillis / 2,
       INT32_MIN},
      {-kDayMillis / 2, -1},
      {kDayMillis / 2, 0},
      {millennium.milli_second + 7 * 60 * 60 * 1000,
       static_cast<int32_t>(millennium.milli_second / kDayMillis)},
      {static_cast<int64_t>(INT32_MAX) * kDayMillis + kDayMillis - 1,
       INT32_MAX}};
  for (const auto& dateCase : dateCases) {
    dates->add_values(dateCase.first);
  }
  auto state = std::make_shared<OutputState>();
  ASSERT_TRUE(writeResponse(fileSchema("temporal"), &response, state).ok());
  auto table = readWithArrow(state->bytes);
  ASSERT_NE(table, nullptr);
  auto actual = std::static_pointer_cast<arrow::TimestampArray>(
      table->column(0)->chunk(0));
  EXPECT_TRUE(
      actual->type()->Equals(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")));
  for (int row = 0; row < response.row_count(); ++row) {
    EXPECT_EQ(actual->Value(row), timestamps->values(row));
  }
  auto actualDates =
      std::static_pointer_cast<arrow::Date32Array>(table->column(1)->chunk(0));
  for (int row = 0; row < response.row_count(); ++row) {
    EXPECT_EQ(actualDates->Value(row),
              dateCases[static_cast<size_t>(row)].second);
  }
}

TEST(CarquetExportWriterTest, WritesAllNullScalarRowGroups) {
  for (const char* dictionary : {"false", "true"}) {
    auto response = makeFlatResponse();
    const std::string nulls(1, '\0');
    response.mutable_arrays(0)->mutable_int32_array()->set_validity(nulls);
    response.mutable_arrays(1)->mutable_uint32_array()->set_validity(nulls);
    response.mutable_arrays(2)->mutable_int64_array()->set_validity(nulls);
    response.mutable_arrays(3)->mutable_uint64_array()->set_validity(nulls);
    response.mutable_arrays(4)->mutable_float_array()->set_validity(nulls);
    response.mutable_arrays(5)->mutable_double_array()->set_validity(nulls);
    response.mutable_arrays(6)->mutable_bool_array()->set_validity(nulls);
    response.mutable_arrays(7)->mutable_string_array()->set_validity(nulls);
    auto* dates = response.mutable_arrays(8)->mutable_date_array();
    dates->set_validity(nulls);
    dates->set_values(0, 1);  // Null payloads need not be valid dates.
    response.mutable_arrays(9)->mutable_timestamp_array()->set_validity(nulls);
    auto state = std::make_shared<OutputState>();
    auto schema = fileSchema("nulls", {{"row_group_size", "2"},
                                       {"dictionary_encoding", dictionary}});
    auto status = writeResponse(schema, &response, state);
    ASSERT_TRUE(status.ok()) << status.ToString();
    auto table = readWithArrow(state->bytes);
    ASSERT_NE(table, nullptr);
    ASSERT_EQ(table->num_columns(), response.arrays_size());
    ASSERT_EQ(table->num_rows(), response.row_count());
    for (const auto& column : table->columns()) {
      EXPECT_EQ(column->null_count(), response.row_count());
    }
  }
}

TEST(CarquetExportWriterTest, ResolvesResponseEntryAndFallbackColumnNames) {
  auto response = makeFlatResponse();
  response.mutable_schema()->clear_name();
  response.mutable_schema()->add_name("response_name");
  auto entrySchema = std::make_shared<reader::TableEntrySchema>();
  entrySchema->columnNames = {"ignored", "entry_name"};
  auto schema = fileSchema("names");
  CarquetExportWriter writer(schema, entrySchema);
  auto state = std::make_shared<OutputState>();
  writer.setStreamOpener(
      [state]() { return std::make_unique<MemoryOutput>(state); });
  ASSERT_TRUE(writer.writeTable(&response).ok());
  auto table = readWithArrow(state->bytes);
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->field(0)->name(), "response_name");
  EXPECT_EQ(table->field(1)->name(), "entry_name");
  EXPECT_EQ(table->field(2)->name(), "col_2");
}

TEST(CarquetExportWriterTest,
     PreservesValidityAcrossByteAndRowGroupBoundaries) {
  QueryResponse response;
  response.set_row_count(19);
  auto* integers = response.add_arrays()->mutable_int64_array();
  auto* booleans = response.add_arrays()->mutable_bool_array();
  auto* strings = response.add_arrays()->mutable_string_array();
  std::string validity(3, '\0');
  for (int row = 0; row < response.row_count(); ++row) {
    integers->add_values(row * 17);
    booleans->add_values(row % 2 == 0);
    strings->add_values("value-" + std::to_string(row));
    if (row % 3 != 0) {
      validity[row / 8] = static_cast<char>(
          static_cast<uint8_t>(validity[row / 8]) | (1U << (row % 8)));
    }
  }
  integers->set_validity(validity);
  booleans->set_validity(validity);
  strings->set_validity(validity);
  for (const char* dictionary : {"false", "true"}) {
    auto schema = fileSchema("validity", {{"row_group_size", "5"},
                                          {"dictionary_encoding", dictionary}});
    auto state = std::make_shared<OutputState>();
    auto status = writeResponse(schema, &response, state);
    ASSERT_TRUE(status.ok()) << status.ToString();
    auto table = readWithArrow(state->bytes);
    ASSERT_NE(table, nullptr);
    auto actualIntegers =
        std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
    auto actualBooleans = std::static_pointer_cast<arrow::BooleanArray>(
        table->column(1)->chunk(0));
    auto actualStrings = std::static_pointer_cast<arrow::StringArray>(
        table->column(2)->chunk(0));
    ASSERT_EQ(table->num_rows(), response.row_count());
    for (int row = 0; row < response.row_count(); ++row) {
      const bool null = row % 3 == 0;
      EXPECT_EQ(actualIntegers->IsNull(row), null);
      EXPECT_EQ(actualBooleans->IsNull(row), null);
      EXPECT_EQ(actualStrings->IsNull(row), null);
      if (!null) {
        EXPECT_EQ(actualIntegers->Value(row), integers->values(row));
        EXPECT_EQ(actualBooleans->Value(row), booleans->values(row));
        EXPECT_EQ(actualStrings->GetString(row), strings->values(row));
      }
    }
  }
}

TEST(CarquetExportWriterTest, WritesMultiplePagesAcrossExplicitRowGroups) {
  constexpr int32_t kRows = 600001;
  constexpr int32_t kRowsPerGroup = 300000;
  auto state = std::make_shared<OutputState>();
  auto schema =
      fileSchema("large", {{"compression", "none"},
                           {"row_group_size", std::to_string(kRowsPerGroup)},
                           {"dictionary_encoding", "false"}});
  QueryResponse response;
  response.set_row_count(kRows);
  response.mutable_schema()->add_name("sequence");
  auto* values = response.add_arrays()->mutable_int64_array();
  for (int32_t row = 0; row < kRows; ++row) {
    values->add_values(static_cast<int64_t>(row) * 17 - 5);
  }

  auto status = writeResponse(schema, &response, state);
  ASSERT_TRUE(status.ok()) << status.ToString();
  auto raw = openMetadata(state->bytes);
  ASSERT_NE(raw, nullptr);
  ASSERT_EQ(raw->metadata()->num_row_groups(), 3);
  EXPECT_EQ(raw->metadata()->RowGroup(0)->num_rows(), kRowsPerGroup);
  EXPECT_EQ(raw->metadata()->RowGroup(1)->num_rows(), kRowsPerGroup);
  EXPECT_EQ(raw->metadata()->RowGroup(2)->num_rows(), 1);
  EXPECT_GT(state->bytes.size(), 4U * 1024U * 1024U);

  auto table = readWithArrow(state->bytes);
  ASSERT_NE(table, nullptr);
  auto sequence =
      std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
  ASSERT_EQ(sequence->length(), kRows);
  EXPECT_EQ(sequence->Value(0), -5);
  EXPECT_EQ(sequence->Value(kRowsPerGroup),
            static_cast<int64_t>(kRowsPerGroup) * 17 - 5);
  EXPECT_EQ(sequence->Value(kRows - 1),
            static_cast<int64_t>(kRows - 1) * 17 - 5);
}

TEST(CarquetExportWriterTest, RejectsOutOfRangeDatesAndNamesBeforeOpening) {
  auto response = makeFlatResponse();
  int openCalls = 0;
  for (const int64_t millis :
       {(static_cast<int64_t>(INT32_MAX) + 1) * 86400000,
        static_cast<int64_t>(INT32_MIN) * 86400000 - 1}) {
    auto invalid = response;
    invalid.mutable_arrays(8)->mutable_date_array()->set_values(0, millis);
    auto status = writeResponse(fileSchema("bad-date"), &invalid,
                                std::make_shared<OutputState>(), &openCalls);
    EXPECT_EQ(status.error_code(), StatusCode::ERR_INVALID_ARGUMENT);
  }
  for (const auto& name : {std::string(), std::string("a\0b", 3)}) {
    auto invalid = response;
    invalid.mutable_schema()->set_name(0, name);
    auto status = writeResponse(fileSchema("bad-name"), &invalid,
                                std::make_shared<OutputState>(), &openCalls);
    EXPECT_EQ(status.error_code(), StatusCode::ERR_INVALID_ARGUMENT);
  }
  EXPECT_EQ(openCalls, 0);
}

TEST(CarquetExportWriterTest, AbortsAndDestroysAfterPartialWrites) {
  auto response = makeFlatResponse();
  auto schema = fileSchema("failure", {{"row_group_size", "2"}});
  auto success = std::make_shared<OutputState>();
  ASSERT_TRUE(writeResponse(schema, &response, success).ok());
  ASSERT_GT(success->writes, 2);
  // Fail during row-group output and at the final footer write. Both cases
  // have already emitted bytes and must abort instead of leaving a partial
  // file.
  for (const int failOnWrite : {2, success->writes}) {
    auto state = std::make_shared<OutputState>();
    state->failOnWrite = failOnWrite;
    auto status = writeResponse(schema, &response, state);
    EXPECT_EQ(status.error_code(), StatusCode::ERR_IO_ERROR);
    EXPECT_EQ(state->writes, failOnWrite);
    EXPECT_EQ(state->closes, 0);
    EXPECT_EQ(state->aborts, 1);
    EXPECT_EQ(state->destroys, 1);
    EXPECT_TRUE(state->bytes.empty());
  }
}

TEST(CarquetExportWriterTest,
     WritesNestedAndSpecialValuesForIndependentArrowReader) {
  auto state = std::make_shared<OutputState>();
  auto response = makeNestedResponse();
  auto schema = fileSchema("nested", {{"compression", "zstd"},
                                      {"row_group_size", "2"},
                                      {"dictionary_encoding", "true"}});
  CarquetExportWriter writer(schema, nestedEntrySchema());
  writer.setStreamOpener(
      [state]() { return std::make_unique<MemoryOutput>(state); });
  auto status = writer.writeTable(&response);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(state->closes, 1);
  EXPECT_EQ(state->aborts, 0);

  auto metadata = openMetadata(state->bytes);
  ASSERT_NE(metadata, nullptr);
  ASSERT_EQ(metadata->metadata()->num_row_groups(), 2);
  ASSERT_EQ(metadata->metadata()->num_columns(), 8);

  auto table = readWithArrow(state->bytes);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->num_rows(), 4);
  ASSERT_EQ(table->num_columns(), 7);

  auto numbers =
      std::static_pointer_cast<arrow::ListArray>(table->column(0)->chunk(0));
  ASSERT_EQ(numbers->length(), 4);
  EXPECT_EQ(numbers->value_length(0), 3);
  EXPECT_EQ(numbers->value_length(1), 0);
  EXPECT_TRUE(numbers->IsNull(2));
  EXPECT_EQ(numbers->value_length(3), 2);
  auto numberValues =
      std::static_pointer_cast<arrow::Int64Array>(numbers->values());
  EXPECT_EQ(numberValues->Value(numbers->value_offset(0)), 1);
  EXPECT_TRUE(numberValues->IsNull(numbers->value_offset(0) + 1));
  EXPECT_EQ(numberValues->Value(numbers->value_offset(3) + 1), 5);

  ASSERT_EQ(table->schema()->field(1)->type()->id(), arrow::Type::LIST);
  auto matrix =
      std::static_pointer_cast<arrow::ListArray>(table->column(1)->chunk(0));
  EXPECT_EQ(matrix->value_length(0), 2);
  EXPECT_TRUE(matrix->IsNull(1));
  auto matrixRows =
      std::static_pointer_cast<arrow::ListArray>(matrix->values());
  EXPECT_EQ(matrixRows->value_length(0), 2);
  EXPECT_TRUE(matrixRows->IsNull(matrix->value_offset(2) + 1));
  auto matrixCells =
      std::static_pointer_cast<arrow::Int32Array>(matrixRows->values());
  EXPECT_EQ(matrixCells->Value(0), 100);
  EXPECT_EQ(matrixCells->Value(1), 101);
  EXPECT_TRUE(matrixCells->IsNull(2));
  EXPECT_EQ(matrixCells->Value(matrixCells->length() - 1), 115);

  auto record =
      std::static_pointer_cast<arrow::StructArray>(table->column(2)->chunk(0));
  EXPECT_TRUE(record->IsNull(1));
  auto counts = std::static_pointer_cast<arrow::Int32Array>(record->field(0));
  auto labels = std::static_pointer_cast<arrow::StringArray>(record->field(1));
  EXPECT_EQ(counts->Value(0), 7);
  EXPECT_TRUE(counts->IsNull(2));
  EXPECT_EQ(labels->GetString(2), "c");
  EXPECT_TRUE(labels->IsNull(3));

  for (int column = 3; column < 7; ++column) {
    auto values = std::static_pointer_cast<arrow::StringArray>(
        table->column(column)->chunk(0));
    EXPECT_TRUE(values->IsNull(2));
  }
  auto durations =
      std::static_pointer_cast<arrow::StringArray>(table->column(3)->chunk(0));
  EXPECT_EQ(durations->GetString(0), "P0D");
  auto paths =
      std::static_pointer_cast<arrow::StringArray>(table->column(6)->chunk(0));
  EXPECT_EQ(paths->GetString(3), "{\"path\":3}");
}

TEST(CarquetExportWriterTest, PreservesAllScalarTypesInMixedNestedBatches) {
  auto flat = makeFlatResponse();
  flat.mutable_arrays(7)->mutable_string_array()->set_values(
      1, std::string("a\0b", 3));
  auto flatState = std::make_shared<OutputState>();
  ASSERT_TRUE(writeResponse(fileSchema("flat"), &flat, flatState).ok());
  auto expected = readWithArrow(flatState->bytes);
  ASSERT_NE(expected, nullptr);

  auto response = flat;
  response.mutable_schema()->add_name("record");
  auto* record = response.add_arrays()->mutable_struct_array();
  record->set_validity(std::string(1, static_cast<char>(0b00011101)));
  for (const auto& column : flat.arrays()) {
    *record->add_fields() = column;
  }
  response.mutable_schema()->add_name("records");
  auto* records = response.add_arrays()->mutable_list_array();
  for (const uint32_t offset : {0U, 0U, 2U, 2U, 3U, 5U}) {
    records->add_offsets(offset);
  }
  records->set_validity(validityExceptRowTwo());
  *records->mutable_elements()->mutable_struct_array() = *record;

  // Exercise both nesting orders. The repeated field contains every scalar
  // type, while the outer struct adds an independent parent validity bitmap.
  auto list = response.arrays(11);
  response.mutable_schema()->add_name("record_with_list");
  auto* outer = response.add_arrays()->mutable_struct_array();
  outer->set_validity(std::string(1, static_cast<char>(0b00011101)));
  *outer->add_fields() = list;

  for (const char* codec : {"none", "snappy", "gzip", "zstd"}) {
    for (const char* dictionary : {"false", "true"}) {
      SCOPED_TRACE(std::string(codec) + "/" + dictionary);
      auto state = std::make_shared<OutputState>();
      auto status = writeResponse(
          fileSchema("mixed", {{"compression", codec},
                               {"dictionary_encoding", dictionary},
                               {"row_group_size", "2"}}),
          &response, state);
      ASSERT_TRUE(status.ok()) << status.ToString();
      auto actual = readWithArrow(state->bytes);
      ASSERT_NE(actual, nullptr);
      ASSERT_EQ(actual->num_rows(), 5);
      ASSERT_EQ(actual->num_columns(), 13);
      auto actualRecord = std::static_pointer_cast<arrow::StructArray>(
          actual->column(10)->chunk(0));
      auto actualList = std::static_pointer_cast<arrow::ListArray>(
          actual->column(11)->chunk(0));
      auto listRecords =
          std::static_pointer_cast<arrow::StructArray>(actualList->values());
      auto actualOuter = std::static_pointer_cast<arrow::StructArray>(
          actual->column(12)->chunk(0));
      auto innerList =
          std::static_pointer_cast<arrow::ListArray>(actualOuter->field(0));
      EXPECT_TRUE(actualRecord->IsNull(1));
      EXPECT_TRUE(actualOuter->IsNull(1));
      EXPECT_TRUE(innerList->IsNull(2));
      EXPECT_EQ(innerList->value_length(0), 0);
      EXPECT_EQ(innerList->value_length(4), 2);
      EXPECT_TRUE(actualList->IsNull(2));
      EXPECT_EQ(actualList->value_length(0), 0);
      EXPECT_EQ(actualList->value_length(4), 2);
      EXPECT_TRUE(listRecords->IsNull(1));
      for (int column = 0; column < flat.arrays_size(); ++column) {
        EXPECT_TRUE(actual->column(column)->Equals(expected->column(column)));
        EXPECT_TRUE(actualRecord->field(column)->type()->Equals(
            expected->column(column)->type()));
        EXPECT_TRUE(listRecords->field(column)->type()->Equals(
            expected->column(column)->type()));
        for (int row : {0, 2, 3, 4}) {
          auto value = expected->column(column)->GetScalar(row);
          auto inRecord = actualRecord->field(column)->GetScalar(row);
          auto inList = listRecords->field(column)->GetScalar(row);
          ASSERT_TRUE(value.ok());
          ASSERT_TRUE(inRecord.ok());
          ASSERT_TRUE(inList.ok());
          EXPECT_TRUE((*value)->Equals(**inRecord));
          EXPECT_TRUE((*value)->Equals(**inList));
        }
      }
    }
  }
}

TEST(CarquetExportWriterTest, PreservesNestedValidityAcrossBytesAndRowGroups) {
  QueryResponse response;
  constexpr int kRows = 19;
  response.set_row_count(kRows);
  auto* list = response.add_arrays()->mutable_list_array();
  auto* flags = list->mutable_elements()->mutable_bool_array();
  list->set_validity(std::string((kRows + 7) / 8, '\0'));
  flags->set_validity(std::string((kRows * 3 + 7) / 8, '\0'));
  for (int row = 0; row <= kRows; ++row) {
    list->add_offsets(row * 3);
  }
  for (int row = 0; row < kRows; ++row) {
    // Row group 1 is entirely NULL. Every other row group is partial.
    if (row % 4 != 0 && (row < 3 || row >= 6)) {
      (*list->mutable_validity())[row >> 3] |= 1U << (row & 7);
    }
    for (int child = 0; child < 3; ++child) {
      const int index = row * 3 + child;
      flags->add_values(index % 2 == 0);
      if (index % 5 != 0) {
        (*flags->mutable_validity())[index >> 3] |= 1U << (index & 7);
      }
    }
  }
  auto state = std::make_shared<OutputState>();
  auto status = writeResponse(fileSchema("validity", {{"row_group_size", "3"}}),
                              &response, state);
  ASSERT_TRUE(status.ok()) << status.ToString();
  auto metadata = openMetadata(state->bytes);
  ASSERT_EQ(metadata->metadata()->num_row_groups(), 7);
  auto table = readWithArrow(state->bytes);
  ASSERT_NE(table, nullptr);
  auto actual =
      std::static_pointer_cast<arrow::ListArray>(table->column(0)->chunk(0));
  auto values = std::static_pointer_cast<arrow::BooleanArray>(actual->values());
  for (int row = 0; row < kRows; ++row) {
    const bool valid = row % 4 != 0 && (row < 3 || row >= 6);
    ASSERT_EQ(actual->IsValid(row), valid) << row;
    if (!valid) {
      continue;
    }
    ASSERT_EQ(actual->value_length(row), 3);
    for (int child = 0; child < 3; ++child) {
      const int source = row * 3 + child;
      const int64_t target = actual->value_offset(row) + child;
      ASSERT_EQ(values->IsNull(target), source % 5 == 0);
      if (values->IsValid(target)) {
        EXPECT_EQ(values->Value(target), source % 2 == 0);
      }
    }
  }
}

TEST(CarquetExportWriterTest, AbortsNestedOutputOnWriteAndCloseFailures) {
  auto response = makeNestedResponse();
  auto schema = fileSchema("failure", {{"row_group_size", "2"}});
  auto success = std::make_shared<OutputState>();
  ASSERT_TRUE(writeResponse(schema, &response, success).ok());
  ASSERT_GT(success->writes, 2);
  for (int failOnWrite : {2, success->writes, 0}) {
    auto state = std::make_shared<OutputState>();
    state->failOnWrite = failOnWrite;
    state->failClose = failOnWrite == 0;
    auto status = writeResponse(schema, &response, state);
    EXPECT_EQ(status.error_code(), StatusCode::ERR_IO_ERROR);
    EXPECT_EQ(state->closes, failOnWrite == 0 ? 1 : 0);
    EXPECT_EQ(state->aborts, 1);
    EXPECT_EQ(state->destroys, 1);
    EXPECT_TRUE(state->bytes.empty());
  }
}

TEST(CarquetExportWriterTest, WritesSpecialStringsWithoutNestedColumns) {
  QueryResponse response;
  response.set_row_count(3);
  for (const char* name : {"duration", "vertex", "edge", "path"}) {
    response.mutable_schema()->add_name(name);
  }
  auto* duration = response.add_arrays()->mutable_interval_array();
  auto* vertex = response.add_arrays()->mutable_vertex_array();
  auto* edge = response.add_arrays()->mutable_edge_array();
  auto* path = response.add_arrays()->mutable_path_array();
  for (int row = 0; row < 3; ++row) {
    duration->add_values("PT" + std::to_string(row) + "S");
    vertex->add_values("v" + std::to_string(row));
    edge->add_values("e" + std::to_string(row));
    path->add_values("p" + std::to_string(row));
  }
  const auto validity = std::string(1, static_cast<char>(0b00000101));
  duration->set_validity(validity);
  vertex->set_validity(validity);
  edge->set_validity(validity);
  path->set_validity(validity);

  auto state = std::make_shared<OutputState>();
  auto status = writeResponse(fileSchema("special"), &response, state, nullptr);
  ASSERT_TRUE(status.ok()) << status.ToString();
  auto table = readWithArrow(state->bytes);
  ASSERT_NE(table, nullptr);
  for (int column = 0; column < 4; ++column) {
    auto values = std::static_pointer_cast<arrow::StringArray>(
        table->column(column)->chunk(0));
    EXPECT_TRUE(values->IsNull(1));
  }
  EXPECT_EQ(
      std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0))
          ->GetString(2),
      "PT2S");
  EXPECT_EQ(
      std::static_pointer_cast<arrow::StringArray>(table->column(3)->chunk(0))
          ->GetString(2),
      "p2");
}

TEST(CarquetExportWriterTest, ReleasesPartiallyBuiltCDataTreesOnFailure) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_schema_t, decltype(&carquet_schema_free)> schema(
      carquet_schema_create(&error), carquet_schema_free);
  ASSERT_NE(schema, nullptr);
  for (const char* name : {"first", "unsupported"}) {
    ASSERT_EQ(
        carquet_schema_add_column(schema.get(), name, CARQUET_PHYSICAL_INT32,
                                  nullptr, CARQUET_REPETITION_OPTIONAL, 0, 0),
        CARQUET_OK);
  }
  carquet_writer_options_t options;
  carquet_writer_options_init(&options);
  auto state = std::make_shared<OutputState>();
  auto* writer = createCarquetWriter(std::make_unique<MemoryOutput>(state),
                                     schema.get(), &options, &error);
  ASSERT_NE(writer, nullptr) << error.message;

  QueryResponse response;
  response.set_row_count(1);
  response.add_arrays()->mutable_int32_array()->add_values(42);
  response.add_arrays();
  auto status =
      writeCarquetCDataBatch(writer, response, 0, 1, {"first", "unsupported"});
  EXPECT_FALSE(status.ok());
  carquet_writer_abort(writer);
  EXPECT_EQ(state->aborts, 1);
}

TEST(CarquetExportWriterTest, RejectsDeclaredShapeMismatchesBeforeOpening) {
  ::common::DataType scalarType;
  scalarType.set_primitive_type(::common::DT_SIGNED_INT32);
  ::common::DataType listType;
  *listType.mutable_list()->mutable_component_type() = scalarType;
  ::common::DataType structType;
  *structType.mutable_tuple()->add_component_types() = scalarType;

  Array scalar;
  scalar.mutable_int32_array()->add_values(7);
  Array list;
  list.mutable_list_array()->add_offsets(0);
  list.mutable_list_array()->add_offsets(1);
  *list.mutable_list_array()->mutable_elements() = scalar;
  Array structure;
  *structure.mutable_struct_array()->add_fields() = scalar;
  Array nullScalar = scalar;
  nullScalar.mutable_int32_array()->set_validity(std::string(1, '\0'));

  // LIST and fixed ARRAY share the list_array wire representation. All other
  // shape pairs must fail, even when the supplied scalar is entirely NULL.
  enum Shape { Scalar, List, Struct };
  const std::vector<std::pair<::common::DataType, Shape>> declarations = {
      {scalarType, Scalar},
      {listType, List},
      {fixedType(scalarType, 1), List},
      {structType, Struct}};
  const std::vector<std::pair<Array, Shape>> responses = {{scalar, Scalar},
                                                          {list, List},
                                                          {structure, Struct},
                                                          {nullScalar, Scalar}};
  for (const auto& [declared, expectedShape] : declarations) {
    for (const auto& [actual, actualShape] : responses) {
      if (expectedShape == actualShape) {
        continue;
      }
      for (int wrapper = 0; wrapper < 3; ++wrapper) {
        SCOPED_TRACE(static_cast<int>(declared.item_case()));
        SCOPED_TRACE(static_cast<int>(actual.typed_array_case()));
        SCOPED_TRACE(wrapper);
        auto type = declared;
        auto column = actual;
        std::string path = "value";
        // Exercise the same mismatch at the root, in a LIST element and in a
        // STRUCT field, including a declared fixed dimension below the root.
        if (wrapper == 1) {
          type.Clear();
          *type.mutable_list()->mutable_component_type() = declared;
          column.Clear();
          auto* parent = column.mutable_list_array();
          parent->add_offsets(0);
          parent->add_offsets(1);
          *parent->mutable_elements() = actual;
          path += ".element";
        } else if (wrapper == 2) {
          type.Clear();
          *type.mutable_tuple()->add_component_types() = declared;
          column.Clear();
          *column.mutable_struct_array()->add_fields() = actual;
          path += ".field_0";
        }
        QueryResponse response;
        response.set_row_count(1);
        response.mutable_schema()->add_name("value");
        *response.add_arrays() = column;
        auto entry = std::make_shared<reader::TableEntrySchema>();
        entry->columnTypes.push_back(
            std::make_shared<::common::DataType>(std::move(type)));
        const auto schema = fileSchema("shape-mismatch");
        CarquetExportWriter writer(schema, entry);
        auto output = std::make_shared<OutputState>();
        int opens = 0;
        writer.setStreamOpener([&]() {
          ++opens;
          return std::make_unique<MemoryOutput>(output);
        });
        const auto status = writer.writeTable(&response);
        EXPECT_EQ(status.error_code(), StatusCode::ERR_INVALID_ARGUMENT);
        EXPECT_NE(
            status.ToString().find(
                "Parquet column shape does not match declared type: " + path),
            std::string::npos);
        EXPECT_EQ(opens, 0);
        EXPECT_TRUE(output->bytes.empty());
      }
    }
  }
}

TEST(CarquetExportWriterTest, ValidatesBeforeOpenAndAbortsPartialOutput) {
  auto response = makeFlatResponse();
  int openCalls = 0;

  auto badCodec = fileSchema("bad", {{"compression", "brotli"}});
  EXPECT_FALSE(writeResponse(badCodec, &response,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());
  auto badGroup = fileSchema("bad", {{"row_group_size", "0"}});
  EXPECT_FALSE(writeResponse(badGroup, &response,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());
  auto badDictionary =
      fileSchema("bad", {{"dictionary_encoding", "sometimes"}});
  EXPECT_FALSE(writeResponse(badDictionary, &response,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());

  auto duplicateNames = response;
  duplicateNames.mutable_schema()->set_name(1, response.schema().name(0));
  EXPECT_FALSE(writeResponse(fileSchema("bad"), &duplicateNames,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());

  QueryResponse badShape;
  badShape.set_row_count(9);
  auto* values = badShape.add_arrays()->mutable_int64_array();
  values->add_values(1);
  EXPECT_FALSE(writeResponse(fileSchema("bad"), &badShape,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());

  values->clear_values();
  for (int row = 0; row < badShape.row_count(); ++row) {
    values->add_values(row);
  }
  values->set_validity("\1");
  EXPECT_FALSE(writeResponse(fileSchema("bad"), &badShape,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());

  QueryResponse nested;
  nested.set_row_count(1);
  nested.add_arrays()->mutable_list_array()->add_offsets(0);
  EXPECT_FALSE(writeResponse(fileSchema("bad"), &nested,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());

  QueryResponse badOffsets;
  badOffsets.set_row_count(2);
  auto* badList = badOffsets.add_arrays()->mutable_list_array();
  for (const uint32_t offset : {0U, 2U, 1U}) {
    badList->add_offsets(offset);
  }
  badList->mutable_elements()->mutable_int64_array()->add_values(1);
  EXPECT_FALSE(writeResponse(fileSchema("bad"), &badOffsets,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());

  QueryResponse badStruct;
  badStruct.set_row_count(2);
  badStruct.add_arrays()
      ->mutable_struct_array()
      ->add_fields()
      ->mutable_int32_array()
      ->add_values(1);
  EXPECT_FALSE(writeResponse(fileSchema("bad"), &badStruct,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());

  // A monotonic offset array must still match the actual child length.
  badList->set_offsets(2, 3);
  EXPECT_EQ(writeResponse(fileSchema("bad"), &badOffsets,
                          std::make_shared<OutputState>(), &openCalls)
                .error_code(),
            StatusCode::ERR_INVALID_ARGUMENT);

  auto shortChildValidity = makeNestedResponse();
  shortChildValidity.mutable_arrays(1)
      ->mutable_list_array()
      ->mutable_elements()
      ->mutable_list_array()
      ->mutable_elements()
      ->mutable_int32_array()
      ->set_validity(std::string(1, static_cast<char>(0xff)));
  EXPECT_EQ(writeResponse(fileSchema("bad"), &shortChildValidity,
                          std::make_shared<OutputState>(), &openCalls)
                .error_code(),
            StatusCode::ERR_INVALID_ARGUMENT);

  QueryResponse badFixed;
  badFixed.set_row_count(1);
  auto* fixed = badFixed.add_arrays()->mutable_list_array();
  fixed->add_offsets(0);
  fixed->add_offsets(1);
  fixed->mutable_elements()->mutable_int32_array()->add_values(1);
  auto fixedSchema = std::make_shared<reader::TableEntrySchema>();
  ::common::DataType element;
  element.set_primitive_type(::common::DT_SIGNED_INT32);
  fixedSchema->columnTypes.push_back(
      std::make_shared<::common::DataType>(fixedType(element, 2)));
  auto badFixedFile = fileSchema("bad");
  CarquetExportWriter badFixedWriter(badFixedFile, fixedSchema);
  badFixedWriter.setStreamOpener([&openCalls]() {
    ++openCalls;
    return std::make_unique<MemoryOutput>(std::make_shared<OutputState>());
  });
  EXPECT_FALSE(badFixedWriter.writeTable(&badFixed).ok());

  QueryResponse tooDeep;
  tooDeep.set_row_count(1);
  auto* nestedNode = tooDeep.add_arrays();
  for (int depth = 0; depth < 65; ++depth) {
    auto* list = nestedNode->mutable_list_array();
    list->add_offsets(0);
    list->add_offsets(1);
    nestedNode = list->mutable_elements();
  }
  nestedNode->mutable_int32_array()->add_values(1);
  EXPECT_FALSE(writeResponse(fileSchema("bad"), &tooDeep,
                             std::make_shared<OutputState>(), &openCalls)
                   .ok());
  EXPECT_EQ(openCalls, 0);

  auto nullOutputSchema = fileSchema("null-output");
  CarquetExportWriter nullOutput(nullOutputSchema);
  nullOutput.setStreamOpener([&openCalls]() {
    ++openCalls;
    return std::unique_ptr<io::OutputStream>();
  });
  EXPECT_FALSE(nullOutput.writeTable(&response).ok());

  auto throwingOutputSchema = fileSchema("throwing-output");
  CarquetExportWriter throwingOutput(throwingOutputSchema);
  throwingOutput.setStreamOpener(
      [&openCalls]() -> std::unique_ptr<io::OutputStream> {
        ++openCalls;
        throw std::runtime_error("injected open failure");
      });
  EXPECT_FALSE(throwingOutput.writeTable(&response).ok());

  auto writeFailure = std::make_shared<OutputState>();
  writeFailure->failWrite = true;
  auto status = writeResponse(fileSchema("write-failure"), &response,
                              writeFailure, &openCalls);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(writeFailure->closes, 0);
  EXPECT_EQ(writeFailure->aborts, 1);
  EXPECT_EQ(writeFailure->destroys, 1);

  auto closeFailure = std::make_shared<OutputState>();
  closeFailure->failClose = true;
  status = writeResponse(fileSchema("close-failure"), &response, closeFailure,
                         &openCalls);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(closeFailure->closes, 1);
  EXPECT_EQ(closeFailure->aborts, 1);
  EXPECT_EQ(closeFailure->destroys, 1);

  QueryResponse empty;
  EXPECT_TRUE(writeResponse(fileSchema("empty"), &empty,
                            std::make_shared<OutputState>(), &openCalls)
                  .ok());
  EXPECT_TRUE(writeResponse(fileSchema("empty"), nullptr,
                            std::make_shared<OutputState>(), &openCalls)
                  .ok());
  EXPECT_EQ(openCalls, 4);
}

}  // namespace
}  // namespace neug::parquet
