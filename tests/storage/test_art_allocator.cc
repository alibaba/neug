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
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
#include <vector>
#include "neug/storages/container/file_header.h"
#include "neug/storages/index/art_tree/fixed_size_allocator.h"
#include "neug/storages/index/art_tree/storage_manager.h"
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
  ModuleDescriptor descriptor_;
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

TEST_F(ARTAllocatorTest, SegmentSizeBoundariesPreserveBufferBounds) {
  const idx_t sizes[] = {
      65531, 131063, 262127,
      FixedSizeBuffer::BUFFER_DATA_SIZE - sizeof(validity_t)};
  const idx_t capacities[] = {4, 2, 1, 1};
  for (size_t i = 0; i < 4; ++i) {
    SCOPED_TRACE(sizes[i]);
    FixedSizeAllocator allocator(sizes[i], *checkpoint_);
    ASSERT_EQ(allocator.GetBufferCapacity(), capacities[i]);
    std::vector<IndexPointer> nodes;
    // Fill the first buffer and cross into the next one. Write every byte
    // so the last slot must fit entirely within its buffer.
    for (idx_t slot = 0; slot <= capacities[i]; ++slot) {
      auto node = allocator.New();
      EXPECT_EQ(node.GetBufferId(), slot == capacities[i] ? 1 : 0);
      EXPECT_EQ(node.GetOffset(), slot == capacities[i] ? 0 : slot);
      std::memset(allocator.Get(node), static_cast<int>(slot + 1), sizes[i]);
      nodes.push_back(node);
    }
    EXPECT_EQ(allocator.GetCapacity(), 2 * capacities[i]);
    EXPECT_EQ(allocator.GetSegmentCount(), nodes.size());
    for (size_t slot = 0; slot < nodes.size(); ++slot) {
      std::vector<data_t> expected(sizes[i], static_cast<data_t>(slot + 1));
      EXPECT_EQ(
          std::memcmp(allocator.Get(nodes[slot]), expected.data(), sizes[i]),
          0);
    }
  }
}

TEST_F(ARTAllocatorTest, SingleSlotBuffersFreeAndReuse) {
  FixedSizeAllocator allocator(131072, *checkpoint_);
  ASSERT_EQ(allocator.GetBufferCapacity(), 1);
  auto first = allocator.New();
  allocator.Free(first);
  EXPECT_TRUE(allocator.Empty());
  EXPECT_EQ(allocator.GetSegmentCount(), 0);
  EXPECT_EQ(allocator.GetCapacity(), 1);
  EXPECT_EQ(allocator.New(), first);

  auto second = allocator.New();
  ASSERT_NE(second.GetBufferId(), first.GetBufferId());
  *allocator.Get<uint64_t>(second) = 123456;
  EXPECT_EQ(allocator.GetCapacity(), 2);
  allocator.Free(first);
  EXPECT_EQ(allocator.GetSegmentCount(), 1);
  EXPECT_EQ(allocator.New(), first);
  EXPECT_EQ(*allocator.Get<uint64_t>(second), 123456);

  // Once both buffers are empty, retain only one cached buffer for reuse.
  allocator.Free(first);
  allocator.Free(second);
  EXPECT_TRUE(allocator.Empty());
  EXPECT_EQ(allocator.GetCapacity(), 1);
  EXPECT_EQ(allocator.New(), first);
  allocator.Free(first);
  allocator.RemoveEmptyBuffers();
  EXPECT_EQ(allocator.GetCapacity(), 0);
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
  auto buffer_metadata = allocator.Save(checkpoint, descriptor_);
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
  auto buffer_metadata = allocator.Save(checkpoint, descriptor_);
  FixedSizeAllocator loaded(8, checkpoint);
  loaded.Load(checkpoint, buffer_metadata, allocator.GetSegmentCount());
  EXPECT_EQ(loaded.GetCapacity(), 0);

  auto node = allocator.New();
  allocator.Free(node);
  buffer_metadata = allocator.Save(checkpoint, descriptor_);
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

TEST_F(ARTAllocatorTest, StorageManagerSharesOneFileAcrossBatches) {
  auto& checkpoint = *checkpoint_;
  const auto baseline = RuntimeFileCount(checkpoint);
  const uint64_t first[] = {123, 456};
  const uint64_t second[] = {789, 1000, 2000};
  ARTStorageManager storage(checkpoint);
  auto first_slice = storage.Append(first, sizeof(first));
  auto second_slice = storage.Append(second, sizeof(second));
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 1);
  auto address = storage.Commit();
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline);
  auto object = checkpoint.OpenFile(address, MemoryLevel::kInMemory);
  EXPECT_EQ(object->GetDataSize(), sizeof(first) + sizeof(second));
  EXPECT_FALSE(object->IsDirty());
  EXPECT_EQ(first_slice.offset, 0);
  EXPECT_EQ(second_slice.offset, sizeof(first));
  const auto* data = static_cast<const char*>(object->GetData());
  EXPECT_EQ(std::memcmp(data + first_slice.offset, first, first_slice.length),
            0);
  EXPECT_EQ(
      std::memcmp(data + second_slice.offset, second, second_slice.length), 0);
  EXPECT_EQ(first_slice.checksum, ComputeMD5(first, sizeof(first)));
  EXPECT_EQ(second_slice.checksum, ComputeMD5(second, sizeof(second)));
}

TEST_F(ARTAllocatorTest, StorageManagerRemovesUncommittedFile) {
  auto& checkpoint = *checkpoint_;
  const auto baseline = RuntimeFileCount(checkpoint);
  const auto objects = (temp_dir_ / "checkpoint" / "objects").string();
  const auto objects_baseline = RuntimeFileCount(objects);
  {
    ARTStorageManager storage(checkpoint);
    const uint64_t data = 123456;
    storage.Append(&data, sizeof(data));
    EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 1);
  }
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline);
  EXPECT_EQ(RuntimeFileCount(objects), objects_baseline);
}

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

  auto buffer_metadata = allocator.Save(checkpoint, descriptor_);
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

TEST_F(ARTAllocatorTest, LoadRebindsRuntimeAllocatorForLaterBuffers) {
  auto& source = *checkpoint_;
  FixedSizeAllocator original(131072, source, MemoryLevel::kSyncToFile);
  ASSERT_EQ(original.GetBufferCapacity(), 1);
  auto first = original.New();
  *original.Get<uint64_t>(first) = 123456;
  auto metadata = original.Save(source, descriptor_);

  CheckpointManager target_manager;
  target_manager.Open((temp_dir_ / "target").string());
  auto target_checkpoint = make_checkpoint(target_manager);
  auto& target = *target_checkpoint;
  ASSERT_NE(source.runtime_dir(), target.runtime_dir());
  const auto source_baseline = RuntimeFileCount(source);
  const auto target_baseline = RuntimeFileCount(target);

  FixedSizeAllocator restored(131072, source, MemoryLevel::kSyncToFile);
  restored.Load(target, metadata, 1);
  EXPECT_EQ(RuntimeFileCount(source), source_baseline);
  EXPECT_EQ(RuntimeFileCount(target), target_baseline + 1);
  auto appended = restored.New();
  EXPECT_NE(appended.GetBufferId(), first.GetBufferId());
  EXPECT_EQ(RuntimeFileCount(source), source_baseline);
  EXPECT_EQ(RuntimeFileCount(target), target_baseline + 2);
  EXPECT_EQ(*restored.Get<uint64_t>(first), 123456);
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
TEST_F(ARTAllocatorTest, AllocatorPacksBuffersAndPreservesHandles) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(2056, checkpoint);
  const auto capacity = allocator.GetBufferCapacity();
  auto first = allocator.New();
  auto handle = allocator.GetHandle(first);
  auto* address = handle.GetPtr<uint64_t>();
  *address = 123456;
  for (idx_t i = 1; i <= 2 * capacity; ++i)
    allocator.New();
  auto buffer_metadata = allocator.Save(checkpoint, descriptor_);
  ASSERT_EQ(buffer_metadata.size(), 3);
  std::set<string> addresses;
  std::set<idx_t> offsets;
  for (const auto& [id, metadata] : buffer_metadata) {
    addresses.insert(metadata.address);
    EXPECT_TRUE(offsets.insert(metadata.offset).second);
    EXPECT_EQ(metadata.length, FixedSizeBuffer::BUFFER_ALLOC_SIZE);
    auto container =
        checkpoint.OpenFile(metadata.address, MemoryLevel::kInMemory);
    EXPECT_EQ(container->GetDataSize(), 3 * FixedSizeBuffer::BUFFER_ALLOC_SIZE);
    EXPECT_FALSE(container->IsDirty());
    EXPECT_EQ(metadata.segment_count, metadata.buffer_id == 2 ? 1 : capacity);
  }
  EXPECT_EQ(addresses.size(), 1);
  EXPECT_EQ(descriptor_.paths().size(), 3);
  EXPECT_EQ(allocator.Get<uint64_t>(first).get(), address);
  EXPECT_EQ(handle.GetRef<uint64_t>(), 123456);
  handle.MarkModified();
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

TEST_F(ARTAllocatorTest, PackedSaveReusesCleanBuffersAndPersistsMarkedWrites) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(131072, checkpoint);
  std::vector<IndexPointer> nodes;
  for (idx_t i = 0; i < 3; ++i) {
    auto node = allocator.New();
    *allocator.Get<uint64_t>(node) = i + 100;
    nodes.push_back(node);
  }
  auto retained_handle = allocator.GetHandle(nodes[0]);
  auto* retained_pointer = retained_handle.GetPtr<uint64_t>();
  const auto objects = (temp_dir_ / "checkpoint" / "objects").string();
  const auto& directory = allocator.Save(checkpoint, descriptor_);
  auto first = directory;
  const auto* clean_record = &directory.at(nodes[1].GetBufferId());
  ASSERT_EQ(RuntimeFileCount(objects), 1);
  auto unchanged = allocator.Save(checkpoint, descriptor_);
  EXPECT_EQ(&directory.at(nodes[1].GetBufferId()), clean_record);
  EXPECT_EQ(RuntimeFileCount(objects), 1);
  for (size_t i = 0; i < first.size(); ++i) {
    EXPECT_EQ(unchanged[i].address, first[i].address);
    EXPECT_EQ(unchanged[i].offset, first[i].offset);
  }

  // A retained handle must explicitly mark writes after the previous save.
  retained_handle.MarkModified();
  *retained_pointer = 999;
  auto changed = allocator.Save(checkpoint, descriptor_);
  EXPECT_EQ(RuntimeFileCount(objects), 2);
  EXPECT_EQ(&directory.at(nodes[1].GetBufferId()), clean_record);
  for (const auto& [id, metadata] : changed) {
    if (metadata.buffer_id == nodes[0].GetBufferId()) {
      EXPECT_NE(metadata.address, first[0].address);
      EXPECT_EQ(std::filesystem::file_size(metadata.address),
                sizeof(FileHeader) + FixedSizeBuffer::BUFFER_ALLOC_SIZE);
    } else {
      EXPECT_EQ(metadata.address, first[0].address);
    }
  }
  FixedSizeAllocator restored(131072, checkpoint);
  restored.Load(checkpoint, changed, 3);
  EXPECT_EQ(*restored.Get<const uint64_t>(nodes[0], false), 999);
  EXPECT_EQ(*restored.Get<const uint64_t>(nodes[1], false), 101);
  auto restored_metadata = restored.Save(checkpoint, descriptor_);
  EXPECT_EQ(RuntimeFileCount(objects), 2);
  EXPECT_EQ(restored_metadata.size(), changed.size());
  for (const auto& [id, metadata] : changed) {
    EXPECT_EQ(restored_metadata.at(id).address, metadata.address);
    EXPECT_EQ(restored_metadata.at(id).offset, metadata.offset);
    EXPECT_EQ(restored_metadata.at(id).checksum, metadata.checksum);
  }
  // Save returns the owned directory; Load must also accept that same
  // directory.
  restored.Load(checkpoint, restored.Save(checkpoint, descriptor_), 3);

  // Legacy writable access marks the buffer without a separate MarkModified.
  *restored.Get<uint64_t>(nodes[1]) = 888;
  auto legacy_write = restored.Save(checkpoint, descriptor_);
  EXPECT_EQ(RuntimeFileCount(objects), 3);
  FixedSizeAllocator reloaded(131072, checkpoint);
  reloaded.Load(checkpoint, legacy_write, 3);
  EXPECT_EQ(*reloaded.Get<const uint64_t>(nodes[1], false), 888);

  allocator.Free(nodes[0]);
  auto after_free = allocator.Save(checkpoint, descriptor_);
  EXPECT_EQ(after_free.size(), 2);
  EXPECT_EQ(descriptor_.paths().size(), 2);
  for (const auto& [key, path] : descriptor_.paths())
    EXPECT_EQ(path, first[0].address);
}

TEST_F(ARTAllocatorTest, PackedLoadRejectsInvalidSlices) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(131072, checkpoint);
  allocator.New();
  allocator.New();
  auto metadata = allocator.Save(checkpoint, descriptor_);
  FixedSizeAllocator restored(131072, checkpoint);
  auto invalid = metadata;
  invalid[0].offset = 2 * FixedSizeBuffer::BUFFER_ALLOC_SIZE;
  EXPECT_THROW(restored.Load(checkpoint, invalid, 2), SerializationException);
  invalid = metadata;
  --invalid[0].length;
  EXPECT_THROW(restored.Load(checkpoint, invalid, 2), SerializationException);
  invalid = metadata;
  invalid[0].checksum[0] ^= 1;
  EXPECT_THROW(restored.Load(checkpoint, invalid, 2), SerializationException);
}

TEST_F(ARTAllocatorTest, PackedSavePersistsAllocationAndFreeChanges) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(131063, checkpoint);
  ASSERT_EQ(allocator.GetBufferCapacity(), 2);
  auto first = allocator.New();
  auto second = allocator.New();
  *allocator.Get<uint64_t>(second) = 123456;
  auto original = allocator.Save(checkpoint, descriptor_);

  allocator.Free(first);
  auto freed = allocator.Save(checkpoint, descriptor_);
  EXPECT_NE(freed[0].address, original[0].address);
  FixedSizeAllocator restored(131063, checkpoint);
  restored.Load(checkpoint, freed, 1);
  EXPECT_EQ(*restored.Get<const uint64_t>(second, false), 123456);
  EXPECT_EQ(restored.New(), first);
  auto allocated = restored.Save(checkpoint, descriptor_);
  EXPECT_NE(allocated[0].address, freed[0].address);
  allocator.Load(checkpoint, allocated, 2);
  EXPECT_EQ(*allocator.Get<const uint64_t>(second, false), 123456);
}

TEST_F(ARTAllocatorTest, PackedObjectRootsRetainAndCollectOldSnapshots) {
  FixedSizeAllocator allocator(131072, *checkpoint_);
  auto first = allocator.New();
  auto second = allocator.New();
  auto* first_value = allocator.Get<uint64_t>(first).get();
  auto* second_value = allocator.Get<uint64_t>(second).get();
  *first_value = 100;
  *second_value = 200;
  ARTBufferMetadataMap metadata;
  auto publish = [&]() {
    auto staging = checkpoint_mgr_.CreateStaging();
    auto checkpoint = staging.checkpoint();
    metadata = allocator.Save(*checkpoint, descriptor_);
    CheckpointManifest manifest;
    manifest.SetSchema(Schema());
    manifest.SetModule("art_allocator", descriptor_);
    checkpoint->SetManifest(std::move(manifest));
    return staging.Publish();
  };
  auto oldest = publish();
  const auto old_object = metadata[0].address;
  allocator.GetHandle(first).MarkModified();
  *first_value = 101;
  auto middle = publish();
  oldest.reset();
  checkpoint_mgr_.CollectGarbage();
  EXPECT_TRUE(std::filesystem::exists(old_object));

  // Keep the middle snapshot alive after its last buffer moves to a new object.
  allocator.GetHandle(second).MarkModified();
  *second_value = 201;
  auto latest = publish();
  for (const auto& [key, path] : descriptor_.paths())
    EXPECT_NE(path, old_object);
  checkpoint_mgr_.CollectGarbage();
  EXPECT_TRUE(std::filesystem::exists(old_object));
  middle.reset();
  checkpoint_mgr_.CollectGarbage();
  EXPECT_FALSE(std::filesystem::exists(old_object));

  // Restore paths from the published manifest, just as a reopened checkpoint
  // would, and check that all current objects survived GC.
  const auto* descriptor = latest->manifest().FindModule("art_allocator");
  ASSERT_NE(descriptor, nullptr);
  for (auto& [id, buffer] : metadata)
    buffer.address =
        *descriptor->get_path("art_buffer_" + std::to_string(buffer.buffer_id));
  FixedSizeAllocator restored(131072, *latest);
  restored.Load(*latest, metadata, 2);
  EXPECT_EQ(*restored.Get<uint64_t>(first), 101);
  EXPECT_EQ(*restored.Get<uint64_t>(second), 201);
}

}  // namespace neug::art
