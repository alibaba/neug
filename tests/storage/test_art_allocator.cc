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

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <memory>
#include <set>
#include <vector>
#include "neug/storages/index/art_tree/fixed_size_allocator.h"
#include "unittest/utils.h"

namespace neug::art {
namespace {
class ARTAllocatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const auto* test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("neug_art_allocator_" + std::string(test_info->name()) + "_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(temp_dir_);
    checkpoint_mgr_.Open(temp_dir_.string());
    checkpoint_ = make_checkpoint(checkpoint_mgr_);
  }

  void TearDown() override {
    checkpoint_.reset();
    checkpoint_mgr_.Close();
    std::filesystem::remove_all(temp_dir_);
  }

  std::filesystem::path temp_dir_;
  CheckpointManager checkpoint_mgr_;
  std::shared_ptr<Checkpoint> checkpoint_;
};
}  // namespace

TEST_F(ARTAllocatorTest, AllocatorCapacityAndReset) {
  FixedSizeAllocator allocator(8, *checkpoint_);
  EXPECT_EQ(allocator.GetCapacity(), 0);
  auto node = allocator.New();
  auto used = allocator.GetMemoryUsage();
  EXPECT_EQ(used.node_buffer_bytes + used.occupancy_bytes,
            FixedSizeBuffer::BUFFER_DATA_SIZE);
  EXPECT_EQ(used.occupied_node_bytes, 8);
  allocator.Free(node);
  EXPECT_EQ(allocator.GetMemoryUsage().occupied_node_bytes, 0);
  EXPECT_EQ(allocator.GetMemoryUsage().TotalBytes(), used.TotalBytes());
  allocator.Reset();
  EXPECT_EQ(allocator.GetCapacity(), 0);
  EXPECT_EQ(allocator.GetMemoryUsage().node_buffer_bytes, 0);
}

TEST_F(ARTAllocatorTest, UpstreamSlotLayout) {
  const idx_t sizes[] = {24, 40, 152, 648, 2056, 8, 16};
  const idx_t capacities[] = {10865, 6532, 1723, 404, 127, 32262, 16256};
  const idx_t bitmap_bytes[] = {1360, 824, 216, 56, 16, 4040, 2040};
  for (size_t i = 0; i < 7; ++i) {
    FixedSizeAllocator allocator(sizes[i], *checkpoint_);
    EXPECT_EQ(allocator.GetBufferCapacity(), capacities[i]);
    EXPECT_EQ(allocator.GetCapacity(), 0);
    auto node = allocator.New();
    EXPECT_EQ(allocator.GetCapacity(node), capacities[i]);
    auto usage = allocator.GetMemoryUsage();
    EXPECT_EQ(usage.occupancy_bytes, bitmap_bytes[i]);
    EXPECT_EQ(usage.node_buffer_bytes + usage.occupancy_bytes,
              FixedSizeBuffer::BUFFER_DATA_SIZE);
  }
}

TEST_F(ARTAllocatorTest, AppendPreservesLiveAddresses) {
  FixedSizeAllocator allocator(8, *checkpoint_);
  const auto slots = allocator.GetBufferCapacity();
  auto first = allocator.New();
  auto handle = allocator.GetHandle(first);
  auto* address = handle.GetPtr<uint64_t>();
  *address = 1234;
  for (idx_t i = 1; i < slots; ++i) {
    auto node = allocator.New();
    *allocator.Get<uint64_t>(node) = i;
  }
  EXPECT_EQ(allocator.GetCapacity(), slots);
  auto second = allocator.New();
  EXPECT_EQ(second.GetBufferId(), 1);
  EXPECT_EQ(allocator.GetCapacity(second), slots);
  EXPECT_EQ(allocator.GetCapacity(), slots * 2);
  for (idx_t i = 1; i < slots; ++i)
    allocator.New();
  auto third = allocator.New();
  EXPECT_EQ(third.GetBufferId(), 2);
  EXPECT_EQ(allocator.GetCapacity(third), slots);
  EXPECT_EQ(allocator.GetCapacity(), slots * 3);
  EXPECT_EQ(allocator.Get<uint64_t>(first).get(), address);
  EXPECT_EQ(handle.GetRef<uint64_t>(), 1234);
  for (idx_t i = 1; i < slots; ++i)
    EXPECT_EQ(*allocator.Get<uint64_t>(IndexPointer(0, i)), i);
}

TEST_F(ARTAllocatorTest, FreedSlotReuseAndEmptyRemoval) {
  FixedSizeAllocator allocator(2056, *checkpoint_);
  const auto slots = allocator.GetBufferCapacity();
  std::vector<IndexPointer> nodes;
  for (idx_t i = 0; i < slots; ++i)
    nodes.push_back(allocator.New());
  allocator.Free(nodes[0]);
  EXPECT_EQ(allocator.New(), nodes[0]);
  auto second = allocator.New();
  EXPECT_EQ(allocator.GetCapacity(), slots * 2);
  allocator.Free(second);
  allocator.RemoveEmptyBuffers();
  EXPECT_EQ(allocator.GetCapacity(), slots);
  EXPECT_EQ(allocator.GetSegmentCount(), slots);
  for (auto node : nodes)
    allocator.Free(node);
  allocator.RemoveEmptyBuffers();
  EXPECT_EQ(allocator.GetCapacity(), 0);
  EXPECT_EQ(allocator.GetMemoryUsage().node_buffer_bytes, 0);
  EXPECT_EQ(allocator.GetCapacity(allocator.New()), slots);
}

TEST_F(ARTAllocatorTest, FixedBuffersRoundTripAndAppend) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(8, checkpoint);
  const auto slots = allocator.GetBufferCapacity();
  std::vector<IndexPointer> nodes;
  for (idx_t i = 0; i < slots + 10; ++i) {
    auto node = allocator.New();
    *allocator.Get<uint64_t>(node) = i + 100;
    nodes.push_back(node);
  }
  allocator.Free(nodes[3]);
  auto buffer_metadata = allocator.Save(checkpoint);
  FixedSizeAllocator loaded(8, checkpoint);
  loaded.Load(checkpoint, buffer_metadata, allocator.GetSegmentCount());
  EXPECT_EQ(loaded.GetCapacity(), slots * 2);
  EXPECT_EQ(loaded.GetMemoryUsage().node_buffer_bytes,
            allocator.GetMemoryUsage().node_buffer_bytes);
  for (idx_t i = 0; i < nodes.size(); ++i) {
    if (i != 3)
      EXPECT_EQ(*loaded.Get<uint64_t>(nodes[i]), i + 100);
  }
  auto* old_address = loaded.Get<uint64_t>(nodes[0]).get();
  auto capacity = loaded.GetCapacity();
  while (loaded.GetSegmentCount() < capacity)
    loaded.New();
  auto appended = loaded.New();
  EXPECT_EQ(loaded.GetCapacity(appended), slots);
  EXPECT_EQ(loaded.GetCapacity(), slots * 3);
  EXPECT_EQ(loaded.Get<uint64_t>(nodes[0]).get(), old_address);
}

TEST_F(ARTAllocatorTest, EmptyRoundTrip) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(8, checkpoint);
  auto buffer_metadata = allocator.Save(checkpoint);
  FixedSizeAllocator loaded(8, checkpoint);
  loaded.Load(checkpoint, buffer_metadata, allocator.GetSegmentCount());
  EXPECT_EQ(loaded.GetCapacity(), 0);

  auto node = allocator.New();
  allocator.Free(node);
  buffer_metadata = allocator.Save(checkpoint);
  loaded.Load(checkpoint, buffer_metadata, allocator.GetSegmentCount());
  EXPECT_EQ(loaded.GetCapacity(), 0);
  EXPECT_EQ(loaded.GetCapacity(loaded.New()), loaded.GetBufferCapacity());
}

TEST_F(ARTAllocatorTest, InvalidSegmentSize) {
  EXPECT_THROW(FixedSizeAllocator(0, *checkpoint_), InternalException);
  EXPECT_THROW(
      (FixedSizeAllocator(FixedSizeBuffer::BUFFER_DATA_SIZE, *checkpoint_)),
      InternalException);
}
}  // namespace neug::art

namespace neug::art {
namespace {
size_t RuntimeFileCount(const std::string& directory) {
  size_t count = 0;
  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(directory))
    count += entry.is_regular_file();
  return count;
}
size_t RuntimeFileCount(Checkpoint& checkpoint) {
  return RuntimeFileCount(checkpoint.runtime_dir());
}
}  // namespace

TEST_F(ARTAllocatorTest, IndependentBuffersRestoreDetachAndCleanup) {
  auto& checkpoint = *checkpoint_;
  const auto baseline = RuntimeFileCount(checkpoint);
  FixedSizeAllocator allocator(2056, checkpoint, MemoryLevel::kSyncToFile);
  const auto slots = allocator.GetBufferCapacity();
  auto first = allocator.New();
  auto* first_address = allocator.Get<uint64_t>(first).get();
  *first_address = 123456;
  for (idx_t i = 1; i < slots * 2 + 1; ++i)
    allocator.New();
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 3);
  EXPECT_EQ(allocator.Get<uint64_t>(first).get(), first_address);
  EXPECT_EQ(*first_address, 123456);

  auto buffer_metadata = allocator.Save(checkpoint);
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 3);
  EXPECT_EQ(allocator.Get<uint64_t>(first).get(), first_address);
  *first_address = 999;
  FixedSizeAllocator restored(2056, checkpoint, MemoryLevel::kSyncToFile);
  restored.Load(checkpoint, buffer_metadata, allocator.GetSegmentCount());
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 6);
  EXPECT_EQ(*restored.Get<uint64_t>(first), 123456);
  EXPECT_EQ(*first_address, 999);
  restored.New();
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 6);

  CheckpointManager target_manager;
  target_manager.Open((temp_dir_ / "target").string());
  auto target_checkpoint = make_checkpoint(target_manager);
  auto& target = *target_checkpoint;
  const auto target_baseline = RuntimeFileCount(target);
  restored.Detach(target, MemoryLevel::kInMemory);
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 3);
  EXPECT_EQ(RuntimeFileCount(target), target_baseline);
  EXPECT_EQ(*restored.Get<uint64_t>(first), 123456);
  restored.Detach(target, MemoryLevel::kSyncToFile);
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 3);
  EXPECT_EQ(RuntimeFileCount(target), target_baseline + 3);
  EXPECT_EQ(*restored.Get<uint64_t>(first), 123456);
  const auto capacity = restored.GetCapacity();
  while (restored.GetSegmentCount() <= capacity)
    restored.New();
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 3);
  EXPECT_EQ(RuntimeFileCount(target), target_baseline + 4);
  allocator.Reset();
  restored.Reset();
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline);
  EXPECT_EQ(RuntimeFileCount(target), target_baseline);
}

TEST_F(ARTAllocatorTest, KeepsRuntimeAllocatorAliveForLaterBuffers) {
  auto path = temp_dir_ / "lifetime";
  std::weak_ptr<Checkpoint> checkpoint_lifetime;
  std::weak_ptr<CheckpointFileManager> allocator_lifetime;
  std::string runtime_dir;
  std::unique_ptr<FixedSizeAllocator> allocator;
  {
    CheckpointManager manager;
    manager.Open(path.string());
    auto checkpoint = make_checkpoint(manager);
    checkpoint_lifetime = checkpoint;
    allocator_lifetime = checkpoint->runtime_allocator();
    runtime_dir = checkpoint->runtime_dir();
    allocator = std::make_unique<FixedSizeAllocator>(2056, *checkpoint,
                                                     MemoryLevel::kSyncToFile);
  }
  EXPECT_TRUE(checkpoint_lifetime.expired());
  EXPECT_FALSE(allocator_lifetime.expired());
  for (idx_t i = 0; i <= allocator->GetBufferCapacity(); ++i)
    allocator->New();
  EXPECT_EQ(allocator->GetCapacity(), 2 * allocator->GetBufferCapacity());
  EXPECT_EQ(RuntimeFileCount(runtime_dir), 2);
  allocator->Reset();
  EXPECT_EQ(RuntimeFileCount(runtime_dir), 0);
  EXPECT_FALSE(allocator_lifetime.expired());
  allocator.reset();
  EXPECT_TRUE(allocator_lifetime.expired());
  EXPECT_FALSE(std::filesystem::exists(runtime_dir));
  std::filesystem::remove_all(path);
}
}  // namespace neug::art

namespace neug::art {
TEST_F(ARTAllocatorTest,
       AllocatorRecordsIndependentAddressesAndPreservesHandles) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(2056, checkpoint);
  const auto capacity = allocator.GetBufferCapacity();
  auto first = allocator.New();
  auto handle = allocator.GetHandle(first);
  auto* address = handle.GetPtr<uint64_t>();
  *address = 123456;
  for (idx_t i = 1; i <= 2 * capacity; ++i)
    allocator.New();
  auto buffer_metadata = allocator.Save(checkpoint);
  ASSERT_EQ(buffer_metadata.size(), 3);
  std::set<string> addresses;
  for (const auto& metadata : buffer_metadata) {
    EXPECT_TRUE(addresses.insert(metadata.address).second);
    auto container =
        checkpoint.OpenFile(metadata.address, MemoryLevel::kInMemory);
    EXPECT_EQ(container->GetDataSize(), FixedSizeBuffer::BUFFER_ALLOC_SIZE);
    EXPECT_EQ(metadata.segment_count, metadata.buffer_id == 2 ? 1 : capacity);
  }
  EXPECT_EQ(allocator.Get<uint64_t>(first).get(), address);
  EXPECT_EQ(handle.GetRef<uint64_t>(), 123456);
  *address = 999;
  FixedSizeAllocator restored(2056, checkpoint);
  restored.Load(checkpoint, buffer_metadata, allocator.GetSegmentCount());
  EXPECT_EQ(*restored.Get<uint64_t>(first), 123456);
  EXPECT_EQ(*allocator.Get<uint64_t>(first), 999);
  auto appended = restored.New();
  EXPECT_EQ(appended.GetBufferId(), 2);
  EXPECT_EQ(appended.GetOffset(), 1);
  auto wrong_count = buffer_metadata;
  ++wrong_count[0].segment_count;
  EXPECT_THROW(
      restored.Load(checkpoint, wrong_count, allocator.GetSegmentCount()),
      SerializationException);
}
}  // namespace neug::art
