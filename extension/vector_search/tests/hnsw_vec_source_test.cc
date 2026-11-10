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

#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>

#include <zvec/core/interface/index_factory.h>
#include "algorithm/hnsw/hnsw_context.h"
#include "hnsw_index.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/vertex_table.h"

namespace neug::vector_search_ext {
namespace {
class TrackingSource final : public zvec::core::VectorSource {
 public:
  explicit TrackingSource(const VecColumn* column) : source_(column) {}
  const void* get_vector(uint32_t id) const override {
    auto* result = source_.get_vector(id);
    if (!result)
      ++unavailable_reads;
    return result;
  }
  mutable size_t unavailable_reads = 0;

 private:
  HNSWVecSource source_;
};

class HNSWVecSourceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = std::filesystem::temp_directory_path() /
            ("hnsw_source_" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    manager_.Open(path_.string());
    auto staging = manager_.CreateStaging();
    checkpoint_ = staging.checkpoint();
    staging.Discard();
    auto accessor = std::make_unique<DefaultIndexIDAccessor>();
    accessor->Open(*checkpoint_, ModuleDescriptor{}, MemoryLevel::kInMemory);
    column_ = std::make_unique<VecColumn>(
        checkpoint_->CreateRuntimeContainer(4 * 2 * sizeof(float),
                                            MemoryLevel::kInMemory),
        std::move(accessor), type_, 4, MakeVector(0), *checkpoint_,
        MemoryLevel::kInMemory);
    column_->set_any(0, MakeVector(0), true);
  }
  void TearDown() override {
    column_.reset();
    checkpoint_.reset();
    manager_.Close();
    std::filesystem::remove_all(path_);
  }
  Value MakeVector(float value) {
    return Value::ARRAY(type_, {Value::FLOAT(value), Value::FLOAT(0)});
  }
  void CheckSnapshotSearch(uint32_t capacity, uint32_t count);

  std::unique_ptr<HNSWIndex> MakeIndex(size_t workers = 4) {
    IndexMeta meta;
    meta.name = "parallel_test";
    meta.type = "hnsw";
    meta.schema.columns.push_back({"v", type_});
    meta.options["m"] = "16";
    meta.options["ef_construction"] = "100";
    meta.options["concurrency"] = std::to_string(workers);
    ModuleDescriptor descriptor;
    descriptor.set("index_meta", meta.ToJsonString());
    descriptor.set("flat_index_offset", "0");
    auto index = std::make_unique<HNSWIndex>();
    index->Open(*checkpoint_, descriptor, MemoryLevel::kInMemory);
    EXPECT_TRUE(index->Rebind({{column_.get()}}).ok());
    return index;
  }

  void ExpectNearest(HNSWIndex& index, vid_t id) {
    HNSWIndexQueryParams query;
    query.target_value = MakeVector(static_cast<float>(id));
    query.topk = 1;
    query.ef_search = 100;
    auto result = index.Search(query);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(result->front().vid, id);
    EXPECT_FLOAT_EQ(result->front().score, 0);
  }

  DataType type_ = DataType::Array(DataType::FLOAT, 2);
  std::filesystem::path path_;
  CheckpointManager manager_;
  std::shared_ptr<Checkpoint> checkpoint_;
  std::unique_ptr<VecColumn> column_;
};

TEST_F(HNSWVecSourceTest, NormalizationOptInFollowsColumnRepresentation) {
  column_->set_any(1, MakeVector(7), true);
  HNSWVecSource source(column_.get());
  EXPECT_FALSE(source.is_l2_normalized());
  ASSERT_TRUE(column_->EnsureL2Normalized().ok());
  EXPECT_TRUE(source.is_l2_normalized());
  EXPECT_FLOAT_EQ(static_cast<const float*>(source.get_vector(1))[0], 1);
  EXPECT_FLOAT_EQ(static_cast<const float*>(source.get_vector(0))[0], 0);
  TrackingSource raw_source(column_.get());
  EXPECT_FALSE(raw_source.is_l2_normalized());
}

TEST_F(HNSWVecSourceTest, ParallelBuildAndOptimizePreserveSnapshots) {
  constexpr vid_t count = 256;
  for (vid_t i = 1; i < count; ++i)
    column_->set_any(i, MakeVector(i), true);
  auto index = MakeIndex();
  VertexTimestamp timestamps;
  timestamps.Init(count, count);
  ASSERT_TRUE(index->BulkBuild(VertexSet(count, timestamps, 0)).ok());
  for (vid_t id : {0u, 63u, 127u, 255u})
    ExpectNearest(*index, id);
  auto old_column = column_->Clone();
  auto old_index = index->Clone();
  auto* snapshot = static_cast<HNSWIndex*>(old_index.get());
  ASSERT_TRUE(
      snapshot->Rebind({{static_cast<VecColumn*>(old_column.get())}}).ok());
  for (vid_t i = count; i < 2 * count; ++i) {
    column_->set_any(i, MakeVector(i), true);
    ASSERT_TRUE(index->Upsert(i, {0, MakeVector(i)}).ok());
  }
  // A retry snapshot retains the old logical offset but shares physical HNSW.
  auto retry_module = index->Clone();
  auto* retry = static_cast<HNSWIndex*>(retry_module.get());
  auto optimized = index->Optimize();
  ASSERT_TRUE(optimized.has_value());
  EXPECT_EQ(optimized->indexed_count, count);
  auto duplicate_retry = retry->Optimize();
  ASSERT_TRUE(duplicate_retry.has_value());
  EXPECT_EQ(duplicate_retry->indexed_count, 0);
  EXPECT_EQ(duplicate_retry->skipped_count, count);
  EXPECT_TRUE(optimized->changed);
  EXPECT_EQ(index->Show()->buffered_count.value(), 0);
  for (vid_t id : {0u, 127u, 255u, 256u, 383u, 511u})
    ExpectNearest(*index, id);
  ExpectNearest(*snapshot, 255);
  EXPECT_FALSE(index->Optimize()->changed);
}

TEST_F(HNSWVecSourceTest, ExtremeConcurrencyHandlesEmptyAndChunkBoundaries) {
  for (size_t workers : {size_t{0}, size_t{2147483647}}) {
    for (vid_t count : {0u, 1u, 64u, 65u, 129u}) {
      SCOPED_TRACE(::testing::Message()
                   << "workers=" << workers << ", count=" << count);
      for (vid_t i = 0; i < count; ++i)
        column_->set_any(i, MakeVector(i), true);
      auto index = MakeIndex(workers);
      VertexTimestamp timestamps;
      timestamps.Init(count, count);
      ASSERT_TRUE(index->BulkBuild(VertexSet(count, timestamps, 0)).ok());
      EXPECT_EQ(index->Show()->buffered_count.value(), 0);
      EXPECT_FALSE(index->Optimize()->changed);
      if (count != 0)
        ExpectNearest(*index, count - 1);
      // Grow by each boundary size to exercise Optimize's offset-based range.
      const auto first = column_->get_offset_accessor()->GetVisibleLimit();
      for (vid_t i = first; i < first + count; ++i) {
        column_->set_any(i, MakeVector(i), true);
        ASSERT_TRUE(index->Upsert(i, {0, MakeVector(i)}).ok());
      }
      auto optimized = index->Optimize();
      ASSERT_TRUE(optimized.has_value());
      EXPECT_EQ(optimized->indexed_count, count);
      EXPECT_EQ(optimized->skipped_count, 0);
      EXPECT_EQ(index->Show()->buffered_count.value(), 0);
      if (count != 0)
        ExpectNearest(*index, first + count - 1);
    }
  }
}

TEST_F(HNSWVecSourceTest, SerialAndParallelBuildPreserveIDsAndRecall) {
  // Exceed ZVec's default brute-force threshold to exercise HNSW traversal.
  constexpr vid_t count = 2048;
  for (vid_t i = 1; i < count; ++i)
    column_->set_any(i, MakeVector(i), true);
  auto serial = MakeIndex(1);
  auto parallel = MakeIndex(4);
  VertexTimestamp timestamps;
  timestamps.Init(count, count);
  VertexSet vertices(count, timestamps, 0);
  ASSERT_TRUE(serial->BulkBuild(vertices).ok());
  ASSERT_TRUE(parallel->BulkBuild(vertices).ok());
  for (vid_t id = 0; id < count; id += 31) {
    ExpectNearest(*serial, id);
    ExpectNearest(*parallel, id);
  }
  HNSWIndexQueryParams query;
  query.target_value = MakeVector(0);
  query.topk = count;
  query.ef_search = count;
  auto serial_results = serial->Search(query);
  auto parallel_results = parallel->Search(query);
  ASSERT_TRUE(serial_results);
  ASSERT_TRUE(parallel_results);
  ASSERT_EQ(serial_results->size(), count);
  ASSERT_EQ(parallel_results->size(), count);
  for (vid_t id = 0; id < count; ++id) {
    EXPECT_EQ((*serial_results)[id].vid, id);
    EXPECT_EQ((*parallel_results)[id].vid, id);
  }
  // Retrying all existing IDs must still publish the complete visible bitmap.
  ASSERT_TRUE(parallel->BulkBuild(vertices).ok());
  EXPECT_EQ(parallel->Show()->buffered_count.value(), 0);
  ExpectNearest(*parallel, count - 1);
}

TEST_F(HNSWVecSourceTest, FailedParallelOperationsLeaveOriginalIndexRetryable) {
  constexpr vid_t count = 256;
  for (vid_t i = 1; i < count; ++i)
    column_->set_any(i, MakeVector(i), true);
  auto index = MakeIndex();
  VertexTimestamp timestamps;
  timestamps.Init(2 * count + 1, 2 * count + 1);
  ASSERT_TRUE(index->BulkBuild(VertexSet(count, timestamps, 0)).ok());
  for (vid_t i = count; i < 2 * count; ++i) {
    column_->set_any(i, MakeVector(i), true);
    ASSERT_TRUE(index->Upsert(i, {0, MakeVector(i)}).ok());
  }
  // Fault injection: publish a mapping without allocating its vector payload.
  // Valid tasks precede this ID, so failure can occur after partial insertion.
  auto* accessor = column_->get_offset_accessor();
  while (accessor->UpsertVID(2 * count) < column_->buffer_size()) {}
  const auto buffered = index->Show()->buffered_count;
  EXPECT_FALSE(index->Optimize().has_value());
  EXPECT_EQ(index->Show()->buffered_count, buffered);
  EXPECT_FALSE(index->BulkBuild(VertexSet(2 * count + 1, timestamps, 0)).ok());
  EXPECT_EQ(index->Show()->buffered_count, buffered);
  // Repair the deliberately inconsistent source before testing normal reads.
  column_->set_any(2 * count, MakeVector(2 * count), true);
  ASSERT_TRUE(accessor->DeleteVID(2 * count).ok());
  ExpectNearest(*index, 127);
  const auto pending = index->Show()->buffered_count.value();
  auto retried = index->Optimize();
  ASSERT_TRUE(retried.has_value());
  // Already inserted IDs survive failure and are skipped on retry. The number
  // inserted before cancellation depends on worker scheduling.
  EXPECT_LE(retried->indexed_count, count);
  EXPECT_EQ(retried->indexed_count + retried->skipped_count, pending);
  EXPECT_EQ(index->Show()->buffered_count.value(), 0);
  ExpectNearest(*index, 511);
  ASSERT_TRUE(index->BulkBuild(VertexSet(2 * count, timestamps, 0)).ok());
  ExpectNearest(*index, 127);
  ExpectNearest(*index, 511);
}

TEST_F(HNSWVecSourceTest, ReadsNewIDWithinSnapshotCapacity) {
  auto snapshot_module = column_->Clone();
  auto* snapshot = static_cast<VecColumn*>(snapshot_module.get());
  HNSWVecSource source(snapshot);
  const auto visible_count = snapshot->get_offset_accessor()->GetNextIndexID();
  std::thread writer([&] { column_->set_any(1, MakeVector(7), true); });
  writer.join();
  ASSERT_EQ(visible_count, 1);
  // The accessor's allocation counter is shared, but the old vector source
  // captured visible_count at construction and incorrectly rejected this ID.
  ASSERT_GT(snapshot->get_offset_accessor()->GetNextIndexID(), visible_count);
  ASSERT_EQ(column_->get_buffer_ptr(), snapshot->get_buffer_ptr());
  const auto* vector = static_cast<const float*>(source.get_vector(1));
  ASSERT_NE(vector, nullptr);
  EXPECT_FLOAT_EQ(vector[0], 7);
  EXPECT_FLOAT_EQ(vector[1], 0);
}

TEST_F(HNSWVecSourceTest, OldSnapshotSkipsIDsAfterResize) {
  auto snapshot_module = column_->Clone();
  auto* snapshot = static_cast<VecColumn*>(snapshot_module.get());
  HNSWVecSource old_source(snapshot);
  const auto capacity = snapshot->buffer_size();
  std::thread writer([&] {
    for (uint32_t id = 1; id <= capacity; ++id)
      column_->set_any(id, MakeVector(static_cast<float>(id)), true);
  });
  writer.join();
  ASSERT_NE(column_->get_buffer_ptr(), snapshot->get_buffer_ptr());
  ASSERT_EQ(snapshot->buffer_size(), capacity);
  EXPECT_NE(old_source.get_vector(capacity - 1), nullptr);
  EXPECT_EQ(old_source.get_vector(capacity), nullptr);
  EXPECT_EQ(old_source.get_vector(UINT32_MAX), nullptr);
  HNSWVecSource new_source(column_.get());
  ASSERT_NE(new_source.get_vector(capacity), nullptr);
  EXPECT_FLOAT_EQ(static_cast<const float*>(new_source.get_vector(capacity))[0],
                  static_cast<float>(capacity));
}

TEST_F(HNSWVecSourceTest, FlatSourcePinsOnlyRequestedRange) {
  column_->set_any(1, MakeVector(7), true);
  FlatVecSource source(column_.get(), 2, 1);
  auto view = source.get_contiguous_view();
  EXPECT_EQ(view.first_id, 1);
  EXPECT_EQ(view.count, 1);
  EXPECT_EQ(source.get_vector(0), nullptr);
  EXPECT_EQ(source.get_vector(2), nullptr);
  ASSERT_NE(source.get_vector(1), nullptr);
  const auto capacity = column_->buffer_size();
  for (uint32_t id = 2; id <= capacity; ++id)
    column_->set_any(id, MakeVector(static_cast<float>(id)), true);
  EXPECT_FLOAT_EQ(static_cast<const float*>(source.get_vector(1))[0], 7);
}

TEST_F(HNSWVecSourceTest, DuplicateErrorSurvivesCheckpointReopen) {
  namespace zv = zvec::core_interface;
  zv::HNSWIndexParam param(zv::MetricType::kL2sq, 2, 16, 100);
  param.data_type = zv::DataType::DT_FP32;
  param.use_external_vector = true;
  param.params.set("proxima.hnsw.streamer.filter_same_key", true);
  HNSWVecSource source(column_.get());
  zv::VectorData vector{zv::DenseVector{source.get_vector(0)}};
  auto index = zv::IndexFactory::CreateAndInitIndex(param);
  ASSERT_NE(index, nullptr);
  const auto path = (path_ / "duplicate-index").string();
  ASSERT_EQ(index->open(path, {zv::StorageOptions::StorageType::kMMAP, true}),
            0);
  ASSERT_EQ(index->add_with_source(vector, 0, source), 0);
  EXPECT_EQ(index->add_with_source(vector, 0, source),
            zvec::core::IndexError_Duplicate);
  ASSERT_EQ(index->flush(), 0);
  ASSERT_EQ(index->close(), 0);
  index = zv::IndexFactory::CreateAndInitIndex(param);
  ASSERT_EQ(index->open(path, {zv::StorageOptions::StorageType::kMMAP, false}),
            0);
  EXPECT_EQ(index->add_with_source(vector, 0, source),
            zvec::core::IndexError_Duplicate);
  EXPECT_EQ(index->close(), 0);
}

void HNSWVecSourceTest::CheckSnapshotSearch(uint32_t capacity, uint32_t count) {
  namespace zv = zvec::core_interface;
  column_->resize(capacity);
  for (uint32_t id = 1; id < capacity; ++id)
    column_->set_any(id, MakeVector(static_cast<float>(id)), true);
  auto snapshot_module = column_->Clone();
  auto* snapshot = static_cast<VecColumn*>(snapshot_module.get());
  TrackingSource old_source(snapshot);
  // Publish additional graph nodes whose payload exists only in the new buffer.
  for (uint32_t id = capacity; id < count; ++id)
    column_->set_any(id, MakeVector(static_cast<float>(id)), true);
  ASSERT_EQ(snapshot->buffer_size(), capacity);
  ASSERT_NE(snapshot->get_buffer_ptr(), column_->get_buffer_ptr());
  HNSWVecSource new_source(column_.get());
  zv::HNSWIndexParam param(zv::MetricType::kL2sq, 2, 16, 100);
  param.data_type = zv::DataType::DT_FP32;
  param.use_external_vector = true;
  auto index = zv::IndexFactory::CreateAndInitIndex(param);
  ASSERT_NE(index, nullptr);
  ASSERT_EQ(index->open((path_ / "index").string(),
                        {zv::StorageOptions::StorageType::kMMAP, true}),
            0);
  for (uint32_t id = 0; id < count; ++id) {
    zv::VectorData vector{zv::DenseVector{new_source.get_vector(id)}};
    ASSERT_EQ(index->add_with_source(vector, id, new_source), 0);
  }
  ASSERT_EQ(index->get_doc_count(), count);
  auto context = index->index_searcher()->create_context();
  auto* hnsw_context = dynamic_cast<zvec::core::HnswContext*>(context.get());
  ASSERT_NE(hnsw_context, nullptr);
  ASSERT_EQ(hnsw_context->get_bruteforce_threshold(), 1000);
  if (count > 1000) {
    ASSERT_GT(index->get_doc_count(), hnsw_context->get_bruteforce_threshold());
  } else {
    ASSERT_LE(index->get_doc_count(), hnsw_context->get_bruteforce_threshold());
  }
  // The nearest nodes in the full index are unavailable to the old snapshot.
  // A correct search must skip them and find the nearest available nodes.
  float target[] = {static_cast<float>(count - 1), 0};
  zv::VectorData query{zv::DenseVector{target}};
  auto options = std::make_shared<zv::HNSWQueryParam>();
  options->topk = 2;
  options->ef_search = 100;
  for (bool fetch : {false, true}) {
    SCOPED_TRACE(fetch ? "fetch_vector=true" : "fetch_vector=false");
    options->fetch_vector = fetch;
    zv::SearchResult full_result;
    ASSERT_EQ(
        index->search_with_source(query, options, new_source, &full_result), 0);
    ASSERT_EQ(full_result.doc_list_.size(), 2);
    EXPECT_EQ(full_result.doc_list_[0].key(), count - 1);
    EXPECT_EQ(full_result.doc_list_[1].key(), count - 2);
    EXPECT_FLOAT_EQ(full_result.doc_list_[0].score(), 0);
    EXPECT_FLOAT_EQ(full_result.doc_list_[1].score(), 1);

    old_source.unavailable_reads = 0;
    zv::SearchResult result;
    ASSERT_EQ(index->search_with_source(query, options, old_source, &result),
              0);
    ASSERT_EQ(result.doc_list_.size(), 2);
    EXPECT_EQ(result.doc_list_[0].key(), capacity - 1);
    EXPECT_EQ(result.doc_list_[1].key(), capacity - 2);
    const float first_distance = static_cast<float>(count - capacity);
    const float second_distance = first_distance + 1;
    EXPECT_FLOAT_EQ(result.doc_list_[0].score(),
                    first_distance * first_distance);
    EXPECT_FLOAT_EQ(result.doc_list_[1].score(),
                    second_distance * second_distance);
    if (fetch) {
      ASSERT_NE(result.doc_list_[0].vector(), nullptr);
      ASSERT_NE(result.doc_list_[1].vector(), nullptr);
      EXPECT_FLOAT_EQ(
          static_cast<const float*>(result.doc_list_[0].vector())[0],
          static_cast<float>(capacity - 1));
      EXPECT_FLOAT_EQ(
          static_cast<const float*>(result.doc_list_[1].vector())[0],
          static_cast<float>(capacity - 2));
    }
    EXPECT_GT(old_source.unavailable_reads, 0);
  }
  EXPECT_EQ(index->close(), 0);
}
TEST_F(HNSWVecSourceTest, ZVecSearchSkipsUnavailableSnapshotVectors) {
  CheckSnapshotSearch(4, 16);
}

TEST_F(HNSWVecSourceTest, ZVecHNSWSearchSkipsUnavailableSnapshotVectors) {
  // 1001 nodes exceed the default flat-scan threshold. Appending the final
  // node resizes the payload; it is the exact query match but is unavailable
  // to the old snapshot. HNSW must handle nullptr and recover valid neighbors.
  CheckSnapshotSearch(1000, 1001);
}
}  // namespace
}  // namespace neug::vector_search_ext
