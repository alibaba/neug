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

#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/main/neug_db.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/encoder.h"
#include "unittest/utils.h"

namespace neug {
namespace execution {
namespace test {
namespace {

class ExecutionBenchmarkTestBase : public ::testing::Test {
 protected:
  static void SetUpTestSuite();
  static void TearDownTestSuite();

  static NeugDB& Db();

  static std::unique_ptr<NeugDB> db_;
  static constexpr const char kDbDir[] = "/tmp/execution_benchmark_test";
};

std::unique_ptr<NeugDB> ExecutionBenchmarkTestBase::db_;

NeugDB& ExecutionBenchmarkTestBase::Db() {
  assert(db_ != nullptr);
  return *db_;
}

void ExecutionBenchmarkTestBase::SetUpTestSuite() {
  if (std::filesystem::exists(kDbDir)) {
    std::filesystem::remove_all(kDbDir);
  }
  std::filesystem::create_directories(kDbDir);

  db_ = std::make_unique<NeugDB>();
  NeugDBConfig config;
  config.data_dir = kDbDir;
  config.checkpoint_on_close = true;
  config.compact_on_close = true;
  config.compact_csr = true;
  config.enable_auto_compaction = false;
  EXPECT_TRUE(db_->Open(config));

  auto conn = db_->Connect();
  EXPECT_NE(conn, nullptr);
  neug::test::load_modern_graph(conn);
  conn->Close();
}

void ExecutionBenchmarkTestBase::TearDownTestSuite() {
  if (db_) {
    db_->Close();
    db_.reset();
  }
  if (std::filesystem::exists(kDbDir)) {
    std::filesystem::remove_all(kDbDir);
  }
}

template <typename ColumnList>
class ExecutionBenchmarkTest : public ExecutionBenchmarkTestBase {};

template <typename... ColumnTypes>
struct ColumnTypeList {
  using TypeTuple = std::tuple<ColumnTypes...>;
  using VectorTuple = std::tuple<std::vector<ColumnTypes>...>;
  static constexpr size_t kSize = sizeof...(ColumnTypes);

  template <size_t Index>
  using TypeAt = typename std::tuple_element<Index, TypeTuple>::type;
};

template <typename ColumnList>
struct TypedDataset;

template <typename... ColumnTypes>
struct TypedDataset<ColumnTypeList<ColumnTypes...>> {
  using VectorTuple = std::tuple<std::vector<ColumnTypes>...>;
  Context ctx;
  VectorTuple expected_columns;
  size_t row_count = 0;
};

template <typename T>
struct ColumnTypeTraits;

template <>
struct ColumnTypeTraits<int32_t> {
  using ArrowArrayType = arrow::Int32Array;

  static int32_t GenerateValue(size_t row, size_t column) {
    return static_cast<int32_t>((row + 1) * static_cast<size_t>(column + 1));
  }

  static int32_t GetValue(const std::shared_ptr<ArrowArrayType>& array,
                          int64_t index) {
    return array->Value(index);
  }

  static int32_t Decode(Decoder& decoder) { return decoder.get_int(); }
};

template <>
struct ColumnTypeTraits<int64_t> {
  using ArrowArrayType = arrow::Int64Array;

  static int64_t GenerateValue(size_t row, size_t column) {
    return static_cast<int64_t>(row * 11 + column * 13);
  }

  static int64_t GetValue(const std::shared_ptr<ArrowArrayType>& array,
                          int64_t index) {
    return array->Value(index);
  }

  static int64_t Decode(Decoder& decoder) { return decoder.get_long(); }
};

template <>
struct ColumnTypeTraits<double> {
  using ArrowArrayType = arrow::DoubleArray;

  static double GenerateValue(size_t row, size_t column) {
    return static_cast<double>(row) * 0.25 + static_cast<double>(column);
  }

  static double GetValue(const std::shared_ptr<ArrowArrayType>& array,
                         int64_t index) {
    return array->Value(index);
  }

  static double Decode(Decoder& decoder) {
    int64_t bits = decoder.get_long();
    double value = 0.0;
    std::memcpy(&value, &bits, sizeof(double));
    return value;
  }
};

template <>
struct ColumnTypeTraits<std::string> {
  using ArrowArrayType = arrow::StringArray;
  static std::string GenerateValue(size_t row, size_t column) {
    return "col" + std::to_string(column) + "_row_" + std::to_string(row);
  }

  static std::string GetValue(
      const std::shared_ptr<arrow::LargeStringArray>& array, int64_t index) {
    auto view = array->GetView(index);
    return std::string(view.data(), view.size());
  }

  static std::string GetValue(const std::shared_ptr<arrow::StringArray>& array,
                              int64_t index) {
    auto view = array->GetView(index);
    return std::string(view.data(), view.size());
  }

  static std::string Decode(Decoder& decoder) {
    auto view = decoder.get_string();
    return std::string(view.data(), view.size());
  }
};

template <>
struct ColumnTypeTraits<Date> {
  using ArrowArrayType = arrow::Date64Array;
  static Date GenerateValue(size_t row, size_t column) {
    int64_t ts = 1622505600000;
    return Date(ts);
  }

  static Date GetValue(const std::shared_ptr<ArrowArrayType>& array,
                       int64_t index) {
    int64_t ts = array->Value(index);
    return Date(ts);
  }

  static Date Decode(Decoder& decoder) {
    int64_t ts = decoder.get_long();
    return Date(ts);
  }
};

template <>
struct ColumnTypeTraits<DateTime> {
  using ArrowArrayType = arrow::TimestampArray;
  static DateTime GenerateValue(size_t row, size_t column) {
    int64_t ts = 1622505600000;
    return DateTime(ts);
  }

  static DateTime GetValue(const std::shared_ptr<ArrowArrayType>& array,
                           int64_t index) {
    int64_t ts = array->Value(index);
    return DateTime(ts);
  }

  static DateTime Decode(Decoder& decoder) {
    int64_t ts = decoder.get_long();
    return DateTime(ts);
  }
};

template <typename ColumnList>
struct ColumnOperations;

template <typename... ColumnTypes>
struct ColumnOperations<ColumnTypeList<ColumnTypes...>> {
  using Dataset = TypedDataset<ColumnTypeList<ColumnTypes...>>;
  using VectorTuple = typename Dataset::VectorTuple;
  using BuildersTuple = std::tuple<ValueColumnBuilder<ColumnTypes>...>;
  using IndexSeq = std::index_sequence_for<ColumnTypes...>;

  static_assert(sizeof...(ColumnTypes) > 0,
                "ColumnTypeList must contain at least one column type");

  static Dataset MakeDataset(size_t row_count) {
    Dataset dataset;
    dataset.row_count = row_count;
    dataset.ctx.tag_ids.resize(sizeof...(ColumnTypes));
    for (size_t i = 0; i < sizeof...(ColumnTypes); ++i) {
      dataset.ctx.tag_ids[i] = static_cast<int>(i);
    }

    BuildersTuple builders;
    ReserveBuilders(row_count, builders, IndexSeq{});
    ReserveVectors(row_count, dataset.expected_columns, IndexSeq{});

    for (size_t row = 0; row < row_count; ++row) {
      AppendRow(row, builders, dataset.expected_columns, IndexSeq{});
    }

    FinishBuilders(dataset.ctx, builders, IndexSeq{});
    return dataset;
  }

  static VectorTuple DecodeArrow(const std::shared_ptr<arrow::Table>& table) {
    VectorTuple vectors;
    size_t row_count = static_cast<size_t>(table->num_rows());
    ReserveVectors(row_count, vectors, IndexSeq{});
    ExtractColumns(table, vectors, IndexSeq{});
    return vectors;
  }

  static VectorTuple DecodeEncoder(const std::vector<char>& buffer,
                                   size_t row_count) {
    VectorTuple vectors;
    ReserveVectors(row_count, vectors, IndexSeq{});
    Decoder decoder(buffer.data(), buffer.size());
    for (size_t row = 0; row < row_count; ++row) {
      DecodeRow(decoder, vectors, IndexSeq{});
    }
    return vectors;
  }

  static void ExpectEqual(const VectorTuple& actual,
                          const VectorTuple& expected) {
    ExpectEqualImpl(actual, expected, IndexSeq{});
  }

 private:
  template <size_t... Is>
  static void ReserveBuilders(size_t row_count, BuildersTuple& builders,
                              std::index_sequence<Is...>) {
    (std::get<Is>(builders).reserve(row_count), ...);
  }

  template <size_t... Is>
  static void ReserveVectors(size_t row_count, VectorTuple& vectors,
                             std::index_sequence<Is...>) {
    (std::get<Is>(vectors).reserve(row_count), ...);
  }

  template <size_t... Is>
  static void AppendRow(size_t row, BuildersTuple& builders,
                        VectorTuple& vectors, std::index_sequence<Is...>) {
    (AppendValue<Is>(row, builders, vectors), ...);
  }

  template <size_t Index>
  static void AppendValue(size_t row, BuildersTuple& builders,
                          VectorTuple& vectors) {
    using ColumnType =
        typename std::tuple_element<Index, std::tuple<ColumnTypes...>>::type;
    auto value = ColumnTypeTraits<ColumnType>::GenerateValue(row, Index);
    std::get<Index>(builders).push_back_opt(value);
    std::get<Index>(vectors).push_back(value);
  }

  template <size_t... Is>
  static void FinishBuilders(Context& ctx, BuildersTuple& builders,
                             std::index_sequence<Is...>) {
    (ctx.set(static_cast<int>(Is), std::get<Is>(builders).finish()), ...);
  }

  template <size_t... Is>
  static void ExtractColumns(const std::shared_ptr<arrow::Table>& table,
                             VectorTuple& vectors, std::index_sequence<Is...>) {
    (ExtractColumn<Is>(table, vectors), ...);
  }

  template <size_t Index>
  static void ExtractColumn(const std::shared_ptr<arrow::Table>& table,
                            VectorTuple& vectors) {
    using ColumnType =
        typename std::tuple_element<Index, std::tuple<ColumnTypes...>>::type;
    using ArrowArrayType =
        typename ColumnTypeTraits<ColumnType>::ArrowArrayType;
    auto chunked = table->column(static_cast<int>(Index));
    for (const auto& chunk : chunked->chunks()) {
      std::get<Index>(vectors).reserve(std::get<Index>(vectors).size() +
                                       static_cast<size_t>(chunk->length()));
      auto typed_chunk = std::static_pointer_cast<ArrowArrayType>(chunk);
      for (int64_t i = 0; i < typed_chunk->length(); ++i) {
        auto value = ColumnTypeTraits<ColumnType>::GetValue(typed_chunk, i);
        std::get<Index>(vectors).push_back(value);
      }
    }
  }

  template <size_t... Is>
  static void DecodeRow(Decoder& decoder, VectorTuple& vectors,
                        std::index_sequence<Is...>) {
    (DecodeValue<Is>(decoder, vectors), ...);
  }

  template <size_t Index>
  static void DecodeValue(Decoder& decoder, VectorTuple& vectors) {
    using ColumnType =
        typename std::tuple_element<Index, std::tuple<ColumnTypes...>>::type;
    auto value = ColumnTypeTraits<ColumnType>::Decode(decoder);
    std::get<Index>(vectors).push_back(value);
  }

  template <size_t... Is>
  static void ExpectEqualImpl(const VectorTuple& actual,
                              const VectorTuple& expected,
                              std::index_sequence<Is...>) {
    (ExpectColumnEqual<Is>(actual, expected), ...);
  }

  template <size_t Index>
  static void ExpectColumnEqual(const VectorTuple& actual,
                                const VectorTuple& expected) {
    EXPECT_EQ(std::get<Index>(actual), std::get<Index>(expected));
  }
};

template <typename ColumnList>
TypedDataset<ColumnList> MakeValueColumnDataset(size_t row_count) {
  return ColumnOperations<ColumnList>::MakeDataset(row_count);
}

template <typename ColumnList>
auto MockArrowDeserialization(const std::shared_ptr<arrow::Table>& table) ->
    typename ColumnList::VectorTuple {
  return ColumnOperations<ColumnList>::DecodeArrow(table);
}

template <typename ColumnList>
auto MockEncoderDeserialization(const std::vector<char>& buffer,
                                size_t row_count) ->
    typename ColumnList::VectorTuple {
  return ColumnOperations<ColumnList>::DecodeEncoder(buffer, row_count);
}

struct MockHttpBinaryResponse {
  std::vector<uint8_t> body;
};

MockHttpBinaryResponse MockServerSendArrowTable(const QueryResult& result) {
  MockHttpBinaryResponse response;
  auto res = result.Serialize();
  response.body.resize(res.size());
  std::memcpy(response.body.data(), res.data(), res.size());
  return response;
}

std::shared_ptr<arrow::Table> MockClientReceiveArrowTable(
    const MockHttpBinaryResponse& response) {
  CHECK(response.body.size() >= 1);
  auto reader = std::make_shared<arrow::io::BufferReader>(
      response.body.data() + 1, static_cast<int64_t>(response.body.size() - 1));
  auto stream_result = arrow::ipc::RecordBatchStreamReader::Open(reader);
  EXPECT_TRUE(stream_result.ok()) << stream_result.status().ToString();
  auto table_result = stream_result.MoveValueUnsafe()->ToTable();
  EXPECT_TRUE(table_result.ok());
  return table_result.MoveValueUnsafe();
}

MockHttpBinaryResponse MockServerSendEncoderPayload(
    const std::vector<char>& payload) {
  MockHttpBinaryResponse response;
  response.body.resize(payload.size());
  std::memcpy(response.body.data(), payload.data(), payload.size());
  return response;
}

std::vector<char> MockClientReceiveEncoderPayload(
    const MockHttpBinaryResponse& response) {
  std::vector<char> payload(response.body.size());
  std::memcpy(payload.data(), response.body.data(), response.body.size());
  return payload;
}

using BenchmarkColumnSets =
    ::testing::Types<ColumnTypeList<std::string, std::string, std::string>,
                     ColumnTypeList<int32_t, double, Date, DateTime>>;

TYPED_TEST_SUITE(ExecutionBenchmarkTest, BenchmarkColumnSets);

}  // namespace

TYPED_TEST(ExecutionBenchmarkTest, SinkArrowAndEncoderValueColumnBenchmark) {
  using ColumnList = TypeParam;
  auto& db = ExecutionBenchmarkTestBase::Db();
  const auto& property_graph = db.graph();
  StorageReadInterface graph(property_graph, 0);

  constexpr size_t kRowCount = 1;
  auto dataset = MakeValueColumnDataset<ColumnList>(kRowCount);
  const size_t row_count = dataset.row_count;

  double total_arrow_pack_ms = 0.0;
  double total_arrow_unpack_ms = 0.0;
  double total_encoder_pack_ms = 0.0;
  double total_encoder_unpack_ms = 0.0;
  const int iterations = 50;
  size_t arrow_http_response_size = 0;
  size_t encoder_http_response_size = 0;
  for (int iter = 0; iter < iterations; ++iter) {
    auto arrow_pack_start = std::chrono::steady_clock::now();
    auto arrow_result = Sink::sink_neug_serial(dataset.ctx, graph);
    auto arrow_http_response = MockServerSendArrowTable(arrow_result);
    auto arrow_pack_end = std::chrono::steady_clock::now();

    ASSERT_NE(arrow_result.table(), nullptr);
    auto arrow_unpack_start = std::chrono::steady_clock::now();
    auto arrow_table = MockClientReceiveArrowTable(arrow_http_response);
    auto arrow_vectors = MockArrowDeserialization<ColumnList>(arrow_table);
    auto arrow_unpack_end = std::chrono::steady_clock::now();

    std::vector<char> buffer;
    Encoder encoder(buffer);
    auto encoder_pack_start = std::chrono::steady_clock::now();
    Sink::sink_encoder(dataset.ctx, graph, encoder);
    auto encoder_pack_end = std::chrono::steady_clock::now();

    ASSERT_FALSE(buffer.empty());
    auto encoder_unpack_start = std::chrono::steady_clock::now();
    auto encoder_http_response = MockServerSendEncoderPayload(buffer);
    auto encoder_http_payload =
        MockClientReceiveEncoderPayload(encoder_http_response);
    auto encoder_vectors =
        MockEncoderDeserialization<ColumnList>(encoder_http_payload, row_count);
    auto encoder_unpack_end = std::chrono::steady_clock::now();

    ColumnOperations<ColumnList>::ExpectEqual(arrow_vectors,
                                              dataset.expected_columns);
    ColumnOperations<ColumnList>::ExpectEqual(encoder_vectors,
                                              dataset.expected_columns);

    auto to_millis = [](auto duration) {
      return std::chrono::duration_cast<
                 std::chrono::duration<double, std::milli>>(duration)
          .count();
    };

    total_arrow_pack_ms += to_millis(arrow_pack_end - arrow_pack_start);
    total_arrow_unpack_ms += to_millis(arrow_unpack_end - arrow_unpack_start);
    total_encoder_pack_ms += to_millis(encoder_pack_end - encoder_pack_start);
    total_encoder_unpack_ms +=
        to_millis(encoder_unpack_end - encoder_unpack_start);
    arrow_http_response_size += arrow_http_response.body.size();
    encoder_http_response_size += encoder_http_response.body.size();
  }

  std::ostringstream timing_report;
  timing_report << "arrow_pack_ms=" << total_arrow_pack_ms
                << ", arrow_unpack_ms=" << total_arrow_unpack_ms
                << ", encoder_pack_ms=" << total_encoder_pack_ms
                << ", encoder_unpack_ms=" << total_encoder_unpack_ms
                << ", arrow_http_reponse_size_bytes="
                << arrow_http_response_size
                << ", encoder_http_reponse_size_bytes="
                << encoder_http_response_size;
  GTEST_LOG_(INFO) << timing_report.str();
}

template <typename T>
struct GetTypeName {
  static std::string name() {
    if constexpr (std::is_same<T, int32_t>::value) {
      return "int32_t";
    } else if constexpr (std::is_same<T, double>::value) {
      return "double";
    } else if constexpr (std::is_same<T, std::string>::value) {
      return "std::string";
    } else if constexpr (std::is_same<T, Date>::value) {
      return "Date";
    } else if constexpr (std::is_same<T, DateTime>::value) {
      return "DateTime";
    } else {
      return "unknown";
    }
  }
};

template <typename T>
void constructAndPrintRecordBatchSizes(size_t num_rows, T sample_value) {
  using builder_type = typename TypeConverter<T>::ArrowBuilderType;
  std::shared_ptr<builder_type> builder = TypeConverter<T>::CreateBuilder();
  for (size_t i = 0; i < num_rows; ++i) {
    THROW_IF_ARROW_NOT_OK(
        builder->Append(TypeConverter<T>::ToArrowCType(sample_value)));
  }
  std::shared_ptr<arrow::Array> array;
  THROW_IF_ARROW_NOT_OK(builder->Finish(&array));
  // construct record batch
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("column_1", TypeConverter<T>::ArrowTypeValue()),
  };
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::vector<std::shared_ptr<arrow::Array>> arrays = {array};
  auto record_batch =
      arrow::RecordBatch::Make(schema, static_cast<int64_t>(num_rows), arrays);
  // serialize to measure size
  std::shared_ptr<arrow::io::BufferOutputStream> output_stream;
  THROW_IF_ARROW_NOT_OK(
      arrow::io::BufferOutputStream::Create().Value(&output_stream));
  auto writer_result = arrow::ipc::MakeStreamWriter(output_stream, schema);
  auto writer = writer_result.ValueOrDie();
  THROW_IF_ARROW_NOT_OK(writer->WriteRecordBatch(*record_batch));
  auto status = writer->stats();
  static_cast<void>(status);
  // LOG(INFO) << "status: " << status.num_record_batches << " record batches, "
  //           << status.total_serialized_body_size
  //           << " bytes serialized body size," << status.total_raw_body_size
  //           << " bytes raw body size, " << status.num_messages << "
  //           messages.";
  THROW_IF_ARROW_NOT_OK(writer->Close());
  std::shared_ptr<arrow::Buffer> buffer = output_stream->Finish().ValueOrDie();
  // get the actual size of the sample_value
  size_t value_size = 0;
  if constexpr (std::is_same<T, std::string>::value) {
    value_size = sample_value.size();
  } else {
    value_size = sizeof(T);
  }
  LOG(INFO) << "RecordBatch with " << num_rows << " rows of type <"
            << GetTypeName<T>::name()
            << "> has serialized size: " << buffer->size()
            << " bytes, each value size: " << value_size << " bytes.";
}

TEST(ArraySizeTest, RecordBatchBasedTest) {
  constructAndPrintRecordBatchSizes<int32_t>(1, 1);
  constructAndPrintRecordBatchSizes<double>(1, 1.0);
  constructAndPrintRecordBatchSizes<std::string>(1, "test_string");
  constructAndPrintRecordBatchSizes<Date>(1, Date{"2021-06-01"});
  constructAndPrintRecordBatchSizes<DateTime>(1,
                                              DateTime{"2021-06-01 00:00:00"});
}

}  // namespace test
}  // namespace execution
}  // namespace neug
