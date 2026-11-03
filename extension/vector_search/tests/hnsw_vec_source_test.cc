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

  DataType type_ = DataType::Array(DataType::FLOAT, 2);
  std::filesystem::path path_;
  CheckpointManager manager_;
  std::shared_ptr<Checkpoint> checkpoint_;
  std::unique_ptr<VecColumn> column_;
};

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
