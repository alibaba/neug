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

#include "neug/common/columns/array_columns.h"
#include "neug/common/columns/list_columns.h"
#include "neug/common/columns/value_columns.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/common/stream.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/execution/execute/ops/retrieve/sink.h"
#include "neug/execution/execute/pipeline.h"
#include "neug/storages/graph/property_graph.h"

namespace neug::execution {
namespace {
using ChunkStream = Stream<ContextChunk>;

DataChunk chunk(int64_t value, int alias = 0) {
  ValueColumnBuilder<int64_t> builder;
  builder.push_back_opt(value);
  DataChunk out;
  out.set(alias, builder.finish());
  return out;
}

TEST(StreamTest, NestedColumnsMergeAcrossBatchesAndSerialize) {
  std::function<void(const Value&, const Value&)> expect_value;
  expect_value = [&](const Value& actual, const Value& expected) {
    ASSERT_EQ(actual.type(), expected.type());
    ASSERT_EQ(actual.IsNull(), expected.IsNull());
    if (expected.IsNull()) {
      return;
    }
    if (expected.type().id() == DataTypeId::kList ||
        expected.type().id() == DataTypeId::kArray) {
      const auto& actual_children = expected.type().id() == DataTypeId::kList
                                        ? ListValue::GetChildren(actual)
                                        : ArrayValue::GetChildren(actual);
      const auto& expected_children = expected.type().id() == DataTypeId::kList
                                          ? ListValue::GetChildren(expected)
                                          : ArrayValue::GetChildren(expected);
      ASSERT_EQ(actual_children.size(), expected_children.size());
      for (size_t i = 0; i < actual_children.size(); ++i) {
        expect_value(actual_children[i], expected_children[i]);
      }
    } else {
      EXPECT_EQ(actual, expected);
    }
  };
  const auto list_type = DataType::List(DataType::INT32);
  const auto array_type = DataType::Array(DataType::INT32, 2);
  const auto nested_type = DataType::Array(array_type, 2);
  const auto pair =
      Value::ARRAY(array_type, {Value::INT32(1), Value(DataType::INT32)});
  for (const auto& values : std::vector<std::vector<Value>>{
           {Value::LIST(DataType::INT32,
                        {Value::INT32(1), Value(DataType::INT32)}),
            Value(list_type), Value::LIST(DataType::INT32, {}),
            Value::LIST(DataType::INT32, {Value::INT32(4)})},
           {pair, Value(array_type), pair, Value(array_type)},
           {Value::ARRAY(nested_type, {pair, pair}), Value(nested_type),
            Value::ARRAY(nested_type, {pair, pair}), Value(nested_type)}}) {
    std::vector<ContextChunk> batches;
    for (size_t begin = 0; begin < values.size(); begin += 2) {
      auto builder = ColumnsUtils::create_builder(values.front().type());
      builder->push_back_elem(values[begin]);
      builder->push_back_elem(values[begin + 1]);
      ContextChunk batch;
      batch.set(0, builder->finish());
      batches.push_back(std::move(batch));
    }
    auto merged = collect_chunk(stream_from_batches(batches));
    ASSERT_TRUE(merged);
    ASSERT_EQ(merged->row_num(), values.size());
    for (size_t row = 0; row < values.size(); ++row) {
      expect_value(merged->get(0)->get_elem(row), values[row]);
    }
    EXPECT_EQ(merged->head(), merged->get(0));
    auto result = materialize(
        stream_from_batches(std::move(batches), StreamMetadata{{0}}));
    ASSERT_TRUE(result);
    PropertyGraph graph;
    GraphView view(graph);
    StorageReadInterface storage(view, 0);
    QueryResponse response;
    Sink::sink_results(*result, storage, &response);
    EXPECT_EQ(response.row_count(), values.size());
    EXPECT_EQ(response.arrays_size(), 1);
  }
}

TEST(StreamTest, IsLazyAndReleasesCursorOnCancellation) {
  int pulls = 0;
  std::weak_ptr<int> weak;
  {
    auto lifetime = std::make_shared<int>(0);
    weak = lifetime;
    ChunkStream stream([&, lifetime]() -> ChunkStream::NextResult {
      ++pulls;
      return std::optional<ContextChunk>(std::in_place, chunk(pulls));
    });
    EXPECT_EQ(pulls, 0);
    ASSERT_TRUE(stream.Next());
    EXPECT_EQ(pulls, 1);
  }
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(pulls, 1);
}

TEST(StreamTest, ErrorIsTerminalAndIsNotEndOfStream) {
  int pulls = 0;
  ChunkStream stream([&]() -> ChunkStream::NextResult {
    ++pulls;
    if (pulls == 1) {
      return std::optional<ContextChunk>(std::in_place, chunk(1));
    }
    THROW_IO_EXCEPTION("late read failure");
  });
  ASSERT_TRUE(stream.Next());
  auto failed = stream.Next();
  ASSERT_FALSE(failed);
  EXPECT_NE(failed.error().error_message().find("late read failure"),
            std::string::npos);
  EXPECT_FALSE(stream.Next());
  EXPECT_EQ(pulls, 2);
}

TEST(StreamTest, PreservesHeadsTagsSparseAliasesAndEmptyBatches) {
  auto first = chunk(7, 3);
  auto head = first.get(3);
  Context original;
  original.tag_ids = {-1, 3};
  original.append_chunk(std::move(first), head);
  ValueColumnBuilder<int64_t> empty_builder;
  DataChunk empty;
  empty.set(3, empty_builder.finish());
  original.append_chunk(std::move(empty));
  original.append_chunk(DataChunk(), head);
  auto restored = materialize(stream_from_context(std::move(original)));
  ASSERT_TRUE(restored);
  EXPECT_EQ(restored->tag_ids, (std::vector<int>{-1, 3}));
  ASSERT_EQ(restored->chunk_num(), 3);
  EXPECT_EQ(restored->chunk(0).get(-1), restored->chunk(0).get(3));
  EXPECT_EQ(restored->chunk(1).row_num(), 0);
  EXPECT_EQ(restored->chunk(1).get(3)->elem_type(), DataType::INT64);
  EXPECT_EQ(restored->chunk(2).row_num(), 1);
}

TEST(StreamTest, BatchTransformPreservesColumnIdentityAndDoesNotReadAhead) {
  int pulls = 0;
  auto data = chunk(42, 3);
  auto column = data.get(3);
  ChunkStream source(
      [&]() -> ChunkStream::NextResult {
        if (++pulls > 1) {
          THROW_IO_EXCEPTION("must not read ahead");
        }
        return std::optional<ContextChunk>(std::in_place, std::move(data),
                                           column);
      },
      StreamMetadata{{3, -1}});
  auto mapped = map_chunks(std::move(source),
                           [](ContextChunk&& batch) -> result<ContextChunk> {
                             return std::move(batch);
                           });
  EXPECT_EQ(pulls, 0);
  auto first = mapped.Next();
  ASSERT_TRUE(first);
  ASSERT_TRUE(*first);
  EXPECT_EQ((**first).get(3), column);
  EXPECT_EQ((**first).head(), column);
  EXPECT_EQ(mapped.metadata().output_columns, (std::vector<int>{3, -1}));
  EXPECT_EQ(pulls, 1);
}

TEST(StreamTest, StorageBridgePreservesMappingAndLateError) {
  int pulls = 0;
  ChunkStream stream([&]() -> ChunkStream::NextResult {
    if (++pulls == 2) {
      return tl::unexpected(
          Status(StatusCode::ERR_IO_ERROR, "bad second batch"));
    }
    auto input = chunk(5, 2);
    input.set(0, chunk(9).get(0));
    return std::optional<ContextChunk>(std::in_place, std::move(input));
  });
  ops::StreamChunkSupplier supplier(std::move(stream), {{2, "a"}, {0, "b"}});
  EXPECT_EQ(pulls, 0);
  EXPECT_EQ(supplier.RowNum(), -1);
  auto first = supplier.GetNextChunk();
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->get(0)->get_elem(0).GetValue<int64_t>(), 5);
  EXPECT_EQ(first->get(1)->get_elem(0).GetValue<int64_t>(), 9);
  EXPECT_EQ(supplier.GetNextChunk(), nullptr);
  EXPECT_EQ(supplier.status().error_code(), StatusCode::ERR_IO_ERROR);
}

TEST(StreamTest, CopyResultPreservesCardinalityWithoutInputColumns) {
  auto output = materialize(ops::batch_insert_result(1000000));
  ASSERT_TRUE(output);
  EXPECT_EQ(output->row_num(), 1000000);
  EXPECT_EQ(output->col_num(), 0);
  EXPECT_TRUE(output->tag_ids.empty());
}

struct Counts {
  int produced = 0;
  int consumed = 0;
};
class CountingSource final : public IOperator {
 public:
  explicit CountingSource(Counts& counts) : counts_(counts) {}
  std::string get_operator_name() const override { return "CountingSource"; }
  ChunkStream Eval(IStorageInterface&, const ParamsMap&, ChunkStream&&,
                   OprTimer*) override {
    return ChunkStream([this]() -> ChunkStream::NextResult {
      EXPECT_EQ(counts_.produced, counts_.consumed);
      if (counts_.produced == 3) {
        return std::optional<ContextChunk>{};
      }
      return std::optional<ContextChunk>(std::in_place,
                                         chunk(++counts_.produced));
    });
  }

 private:
  Counts& counts_;
};
class CountingProject final : public IOperator {
 public:
  explicit CountingProject(Counts& counts) : counts_(counts) {}
  std::string get_operator_name() const override { return "CountingProject"; }
  Stream<ContextChunk> Eval(IStorageInterface&, const ParamsMap&,
                            Stream<ContextChunk>&& input, OprTimer*) override {
    return map_chunks(std::move(input),
                      [this](ContextChunk&& chunk) -> result<ContextChunk> {
                        ++counts_.consumed;
                        EXPECT_EQ(chunk.row_num(), 1);
                        return std::move(chunk);
                      });
  }

 private:
  Counts& counts_;
};

TEST(StreamTest, DeferredInitializationRunsOnceAndReportsErrorsOnNext) {
  int initialized = 0;
  auto stream = defer_stream(
      Stream<ContextChunk>(),
      [&](Stream<ContextChunk> &&) -> Stream<ContextChunk> {
        ++initialized;
        return error_stream<ContextChunk>(Status::InternalError("init failed"));
      });
  EXPECT_EQ(initialized, 0);
  auto first = stream.Next();
  ASSERT_FALSE(first);
  EXPECT_NE(first.error().ToString().find("init failed"), std::string::npos);
  EXPECT_EQ(initialized, 1);
  auto second = stream.Next();
  ASSERT_FALSE(second);
  EXPECT_EQ(second.error().ToString(), first.error().ToString());
  EXPECT_EQ(initialized, 1);

  auto throwing =
      defer_stream(Stream<ContextChunk>(),
                   [](Stream<ContextChunk> &&) -> Stream<ContextChunk> {
                     THROW_IO_EXCEPTION("opening source failed");
                   });
  auto error = throwing.Next();
  ASSERT_FALSE(error);
  EXPECT_NE(error.error().ToString().find("opening source failed"),
            std::string::npos);
}

TEST(StreamTest, SinkMetadataPreservesOutputOrderAndEmptyResults) {
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  physical::PhysicalPlan plan;
  auto* sink = plan.add_plan()->mutable_opr()->mutable_sink();
  for (int alias : {3, 0, 3}) {
    sink->add_tags()->mutable_tag()->set_value(alias);
  }
  ops::SinkOprBuilder builder;
  auto built = builder.Build(Schema(), ContextMeta(), plan, 0);
  ASSERT_TRUE(built);
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::move(built->first));
  Pipeline pipeline(std::move(operators));
  for (bool empty : {false, true}) {
    Context input;
    if (!empty) {
      auto data = chunk(7, 3);
      data.set(0, chunk(9).get(0));
      input.append_chunk(std::move(data));
    }
    auto result = pipeline.Execute(storage, std::move(input), {}, nullptr);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->tag_ids, (std::vector<int>{3, 0, 3}));
    EXPECT_EQ(result->row_num(), empty ? 0 : 1);
  }
}

TEST(StreamTest, PipelineReportsInitializationFailureOnlyWhenPulled) {
  class FailingSource final : public IOperator {
   public:
    explicit FailingSource(int& calls) : calls_(calls) {}
    std::string get_operator_name() const override { return "FailingSource"; }
    Stream<ContextChunk> Eval(IStorageInterface&, const ParamsMap&,
                              Stream<ContextChunk>&& input,
                              OprTimer*) override {
      return defer_stream(
          std::move(input),
          [this](Stream<ContextChunk> &&) -> Stream<ContextChunk> {
            ++calls_;
            THROW_IO_EXCEPTION("source initialization failed");
          });
    }

   private:
    int& calls_;
  };
  int calls = 0;
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<FailingSource>(calls));
  Pipeline pipeline(std::move(operators));
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  auto output =
      pipeline.ExecuteStream(storage, Stream<ContextChunk>(), {}, nullptr);
  EXPECT_EQ(calls, 0);
  auto error = output.Next();
  ASSERT_FALSE(error);
  EXPECT_NE(error.error().ToString().find("FailingSource"), std::string::npos);
  EXPECT_EQ(calls, 1);
  EXPECT_FALSE(output.Next());
  EXPECT_EQ(calls, 1);
}

TEST(StreamTest, PipelinePullsThroughChunkwiseOperatorsAndProfilesRows) {
  Counts counts;
  std::vector<std::unique_ptr<IOperator>> operators;
  operators.push_back(std::make_unique<CountingSource>(counts));
  operators.push_back(std::make_unique<CountingProject>(counts));
  Pipeline pipeline(std::move(operators));
  PropertyGraph graph;
  GraphView view(graph);
  StorageReadInterface storage(view, 0);
  OprTimer timer;
  auto result = pipeline.Execute(storage, Context(), {}, &timer);
  ASSERT_TRUE(result) << result.error().ToString();
  EXPECT_EQ(result->row_num(), 3);
  EXPECT_EQ(counts.produced, 3);
  EXPECT_EQ(counts.consumed, 3);
  auto profile = OprTimer::ToProfileResult(&timer);
  ASSERT_EQ(profile.operators_size(), 2);
  EXPECT_EQ(profile.operators(0).output_rows(), 3);
  EXPECT_EQ(profile.operators(1).output_rows(), 3);
}
}  // namespace
}  // namespace neug::execution
