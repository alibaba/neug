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
#include <fstream>
#include <memory>
#include <set>
#include <vector>
#include "neug/storages/container/file_header.h"
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

  const ARTBufferMetadataMap& SaveBuffers(FixedSizeAllocator& allocator,
                                          Checkpoint& checkpoint,
                                          ModuleDescriptor& descriptor) {
    auto writer = checkpoint.CreateObjectWriter();
    allocator.Save(writer);
    writer.Commit();
    return allocator.SaveMetadata(descriptor, "art_buffer_");
  }

  std::shared_ptr<Checkpoint> PublishART(
      CheckpointManager::StagingCheckpoint& staging,
      const ModuleDescriptor& descriptor) {
    CheckpointManifest manifest;
    manifest.SetSchema(Schema());
    manifest.SetModule("art_allocator", descriptor);
    staging.checkpoint()->SetManifest(std::move(manifest));
    return staging.Publish();
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
  SaveBuffers(allocator, checkpoint, descriptor_);
  FixedSizeAllocator loaded(8, checkpoint);
  auto reader = checkpoint.CreateObjectReader();
  loaded.Load(checkpoint, reader, descriptor_, "art_buffer_");
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
  SaveBuffers(allocator, checkpoint, descriptor_);
  FixedSizeAllocator loaded(8, checkpoint);
  auto reader = checkpoint.CreateObjectReader();
  loaded.Load(checkpoint, reader, descriptor_, "art_buffer_");
  EXPECT_EQ(loaded.GetCapacity(), 0);

  auto node = allocator.New();
  allocator.Free(node);
  SaveBuffers(allocator, checkpoint, descriptor_);
  loaded.Load(checkpoint, reader, descriptor_, "art_buffer_");
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

TEST_F(ARTAllocatorTest, ObjectWriterPublishesFragmentsInAppendOrder) {
  auto& checkpoint = *checkpoint_;
  const auto baseline = RuntimeFileCount(checkpoint);
  const uint64_t first[] = {123, 456};
  const uint64_t second[] = {789, 1000, 2000};
  auto writer = checkpoint.CreateObjectWriter();
  std::vector<CheckpointObjectSlice> slices;
  std::vector<std::string> addresses;
  auto record = [&](const std::string& address,
                    const CheckpointObjectSlice& slice) {
    EXPECT_TRUE(std::filesystem::exists(address));
    addresses.push_back(address);
    slices.push_back(slice);
  };
  writer.Append(first, sizeof(first), record);
  writer.Append(second, sizeof(second), record);
  EXPECT_TRUE(slices.empty());
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 1);
  auto address = writer.Commit();
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline);
  ASSERT_EQ(slices.size(), 2);
  EXPECT_EQ(addresses, (std::vector<std::string>{address, address}));
  const auto& first_slice = slices[0];
  const auto& second_slice = slices[1];
  EXPECT_EQ(std::filesystem::file_size(address),
            sizeof(FileHeader) + sizeof(first) + sizeof(second));
  EXPECT_EQ(first_slice.offset, 0);
  EXPECT_EQ(second_slice.offset, sizeof(first));
  ASSERT_EQ(first_slice.length, sizeof(first));
  ASSERT_EQ(second_slice.length, sizeof(second));
  ModuleDescriptor descriptor;
  descriptor.set_path("first", address);
  descriptor.set_path("second", address);
  descriptor.set_slice("first", first_slice);
  descriptor.set_slice("second", second_slice);
  rapidjson::Document json;
  const auto module = descriptor.ToJson(json.GetAllocator());
  ASSERT_TRUE(module.HasMember("slices"));
  EXPECT_EQ(module["slices"]["first"]["length"].GetUint64(), sizeof(first));
  EXPECT_EQ(module["slices"]["second"]["length"].GetUint64(), sizeof(second));
  const auto reopened = ModuleDescriptor::FromJson(module);
  auto reader = checkpoint.CreateObjectReader();
  const auto first_view = reader.Get(reopened, "first");
  const auto second_view = reader.Get(reopened, "second");
  EXPECT_EQ(second_view.data,
            static_cast<const char*>(first_view.data) + sizeof(first));
  ASSERT_EQ(first_view.length, sizeof(first));
  ASSERT_EQ(second_view.length, sizeof(second));
  EXPECT_EQ(std::memcmp(first_view.data, first, first_view.length), 0);
  EXPECT_EQ(std::memcmp(second_view.data, second, second_view.length), 0);
}

TEST_F(ARTAllocatorTest, ObjectWriterRemovesUncommittedFile) {
  auto& checkpoint = *checkpoint_;
  const auto baseline = RuntimeFileCount(checkpoint);
  const auto objects = (temp_dir_ / "checkpoint" / "objects").string();
  const auto objects_baseline = RuntimeFileCount(objects);
  {
    auto writer = checkpoint.CreateObjectWriter();
    const uint64_t data = 123456;
    writer.Append(&data, sizeof(data),
                  [](const std::string&, const CheckpointObjectSlice&) {
                    ADD_FAILURE() << "Uncommitted fragment callback ran";
                  });
    EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 1);
  }
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline);
  EXPECT_EQ(RuntimeFileCount(objects), objects_baseline);
}

TEST_F(ARTAllocatorTest, SharedWriterDefersAddressesUntilCommitAndAllowsRetry) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator first(8, checkpoint), second(8, checkpoint);
  const auto first_node = first.New();
  const auto second_node = second.New();
  *first.Get<uint64_t>(first_node) = 123;
  *second.Get<uint64_t>(second_node) = 456;
  {
    auto abandoned = checkpoint.CreateObjectWriter();
    first.Save(abandoned);
    second.Save(abandoned);
    EXPECT_TRUE(first.SaveMetadata(descriptor_, "first_").empty());
    EXPECT_TRUE(second.SaveMetadata(descriptor_, "second_").empty());
  }

  auto staging = checkpoint_mgr_.CreateStaging();
  auto writer = staging.checkpoint()->CreateObjectWriter();
  first.Save(writer);
  second.Save(writer);
  const auto address = writer.Commit();
  const auto& first_saved = first.SaveMetadata(descriptor_, "first_");
  const auto& second_saved = second.SaveMetadata(descriptor_, "second_");
  ASSERT_EQ(first_saved.size(), 1);
  ASSERT_EQ(second_saved.size(), 1);
  EXPECT_EQ(first_saved.at(0).address, address);
  EXPECT_EQ(second_saved.at(0).address, address);
  EXPECT_NE(first_saved.at(0).offset, second_saved.at(0).offset);
  auto published = PublishART(staging, descriptor_);
  const auto* descriptor = published->manifest().FindModule("art_allocator");
  ASSERT_NE(descriptor, nullptr);
  auto reader = published->CreateObjectReader();
  FixedSizeAllocator restored_first(8, *published);
  FixedSizeAllocator restored_second(8, *published);
  restored_first.Load(*published, reader, *descriptor, "first_");
  restored_second.Load(*published, reader, *descriptor, "second_");
  EXPECT_EQ(*restored_first.Get<const uint64_t>(first_node, false), 123);
  EXPECT_EQ(*restored_second.Get<const uint64_t>(second_node, false), 456);

  auto unchanged = published->CreateObjectWriter();
  restored_first.Save(unchanged);
  restored_second.Save(unchanged);
  EXPECT_TRUE(unchanged.Commit().empty());
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

  SaveBuffers(allocator, checkpoint, descriptor_);
  EXPECT_EQ(RuntimeFileCount(checkpoint), baseline + 3);
  EXPECT_EQ(allocator.Get<uint64_t>(first).get(), first_address);
  *first_address = 999;
  FixedSizeAllocator restored(2056, checkpoint, MemoryLevel::kSyncToFile);
  auto reader = checkpoint.CreateObjectReader();
  restored.Load(checkpoint, reader, descriptor_, "art_buffer_");
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
  SaveBuffers(original, source, descriptor_);

  CheckpointManager target_manager;
  target_manager.Open((temp_dir_ / "target").string());
  auto target_checkpoint = make_checkpoint(target_manager);
  auto& target = *target_checkpoint;
  ASSERT_NE(source.runtime_dir(), target.runtime_dir());
  const auto source_baseline = RuntimeFileCount(source);
  const auto target_baseline = RuntimeFileCount(target);

  FixedSizeAllocator restored(131072, source, MemoryLevel::kSyncToFile);
  auto reader = source.CreateObjectReader();
  restored.Load(target, reader, descriptor_, "art_buffer_");
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
  auto buffer_metadata = SaveBuffers(allocator, checkpoint, descriptor_);
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
  }
  EXPECT_EQ(addresses.size(), 1);
  EXPECT_EQ(descriptor_.paths().size(), 3);
  EXPECT_EQ(allocator.Get<uint64_t>(first).get(), address);
  EXPECT_EQ(handle.GetRef<uint64_t>(), 123456);
  handle.MarkModified();
  *address = 999;
  FixedSizeAllocator restored(2056, checkpoint);
  auto reader = checkpoint.CreateObjectReader();
  restored.Load(checkpoint, reader, descriptor_, "art_buffer_");
  EXPECT_EQ(restored.GetSegmentCount(), allocator.GetSegmentCount());
  EXPECT_EQ(*restored.Get<uint64_t>(first), 123456);
  EXPECT_EQ(*allocator.Get<uint64_t>(first), 999);
  auto appended = restored.New();
  EXPECT_EQ(appended.GetBufferId(), 2);
  EXPECT_EQ(appended.GetOffset(), 1);
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
  const auto objects = (temp_dir_ / "checkpoint" / "objects").string();
  auto save_and_reopen = [&]() {
    auto staging = checkpoint_mgr_.CreateStaging();
    descriptor_ = ModuleDescriptor{};
    auto saved = SaveBuffers(allocator, *staging.checkpoint(), descriptor_);
    auto published = PublishART(staging, descriptor_);
    auto reader = published->CreateObjectReader();
    allocator.Load(*published, reader,
                   *published->manifest().FindModule("art_allocator"),
                   "art_buffer_");
    return saved;
  };
  auto first = save_and_reopen();
  ASSERT_EQ(RuntimeFileCount(objects), 1);
  auto unchanged = save_and_reopen();
  EXPECT_EQ(RuntimeFileCount(objects), 1);
  for (size_t i = 0; i < first.size(); ++i) {
    EXPECT_EQ(unchanged[i].address, first[i].address);
    EXPECT_EQ(unchanged[i].offset, first[i].offset);
  }

  // A retained handle must explicitly mark writes after the previous save.
  auto retained_handle = allocator.GetHandle(nodes[0]);
  auto* retained_pointer = retained_handle.GetPtr<uint64_t>();
  retained_handle.MarkModified();
  *retained_pointer = 999;
  auto changed = save_and_reopen();
  EXPECT_EQ(RuntimeFileCount(objects), 2);
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
  auto reader = checkpoint.CreateObjectReader();
  restored.Load(checkpoint, reader, descriptor_, "art_buffer_");
  EXPECT_EQ(*restored.Get<const uint64_t>(nodes[0], false), 999);
  EXPECT_EQ(*restored.Get<const uint64_t>(nodes[1], false), 101);
  auto restored_metadata = SaveBuffers(restored, checkpoint, descriptor_);
  EXPECT_EQ(RuntimeFileCount(objects), 2);
  EXPECT_EQ(restored_metadata.size(), changed.size());
  for (const auto& [id, metadata] : changed) {
    EXPECT_EQ(restored_metadata.at(id).address, metadata.address);
    EXPECT_EQ(restored_metadata.at(id).offset, metadata.offset);
  }
  // Legacy writable access marks the buffer without a separate MarkModified.
  *restored.Get<uint64_t>(nodes[1]) = 888;
  SaveBuffers(restored, checkpoint, descriptor_);
  EXPECT_EQ(RuntimeFileCount(objects), 3);
  FixedSizeAllocator reloaded(131072, checkpoint);
  reloaded.Load(checkpoint, reader, descriptor_, "art_buffer_");
  EXPECT_EQ(*reloaded.Get<const uint64_t>(nodes[1], false), 888);

  allocator.Free(nodes[0]);
  ModuleDescriptor next_descriptor;
  auto after_free = SaveBuffers(allocator, checkpoint, next_descriptor);
  EXPECT_EQ(after_free.size(), 2);
  EXPECT_EQ(next_descriptor.paths().size(), 2);
  EXPECT_FALSE(next_descriptor.has_path("art_buffer_0"));
  EXPECT_FALSE(next_descriptor.get_slice("art_buffer_0").has_value());
  EXPECT_TRUE(descriptor_.has_path("art_buffer_0"));
  for (const auto& [key, path] : next_descriptor.paths())
    EXPECT_EQ(path, first[0].address);
}

TEST_F(ARTAllocatorTest, PackedLoadRejectsInvalidSlicesAndCorruptedObjects) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(131072, checkpoint);
  allocator.New();
  allocator.New();
  auto metadata = SaveBuffers(allocator, checkpoint, descriptor_);
  FixedSizeAllocator restored(131072, checkpoint);
  {
    auto reader = checkpoint.CreateObjectReader();
    auto invalid = descriptor_;
    const auto& slice = metadata.at(0);
    invalid.set_slice("art_buffer_0",
                      {2 * FixedSizeBuffer::BUFFER_ALLOC_SIZE, slice.length});
    EXPECT_THROW(restored.Load(checkpoint, reader, invalid, "art_buffer_"),
                 SerializationException);
    invalid.set_slice("art_buffer_0", {slice.offset, slice.length - 1});
    EXPECT_THROW(restored.Load(checkpoint, reader, invalid, "art_buffer_"),
                 SerializationException);
    ModuleDescriptor missing_slice;
    missing_slice.set_path("art_buffer_0", slice.address);
    EXPECT_THROW(
        restored.Load(checkpoint, reader, missing_slice, "art_buffer_"),
        SerializationException);
  }
  const auto& slice = metadata.at(0);
  std::fstream object(slice.address,
                      std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(object);
  const auto payload_tail =
      sizeof(FileHeader) + slice.offset + slice.length - 1;
  char byte;
  object.seekg(payload_tail);
  ASSERT_TRUE(object.get(byte));
  object.seekp(payload_tail);
  object.put(static_cast<char>(byte ^ 1));
  object.close();
  ASSERT_TRUE(object);
  auto reader = checkpoint.CreateObjectReader();
  EXPECT_THROW(restored.Load(checkpoint, reader, descriptor_, "art_buffer_"),
               SerializationException);
}

TEST_F(ARTAllocatorTest, PackedSavePersistsAllocationAndFreeChanges) {
  auto& checkpoint = *checkpoint_;
  FixedSizeAllocator allocator(131063, checkpoint);
  ASSERT_EQ(allocator.GetBufferCapacity(), 2);
  auto first = allocator.New();
  auto second = allocator.New();
  *allocator.Get<uint64_t>(second) = 123456;
  auto original = SaveBuffers(allocator, checkpoint, descriptor_);

  allocator.Free(first);
  auto freed = SaveBuffers(allocator, checkpoint, descriptor_);
  EXPECT_NE(freed[0].address, original[0].address);
  FixedSizeAllocator restored(131063, checkpoint);
  auto reader = checkpoint.CreateObjectReader();
  restored.Load(checkpoint, reader, descriptor_, "art_buffer_");
  EXPECT_EQ(*restored.Get<const uint64_t>(second, false), 123456);
  EXPECT_EQ(restored.New(), first);
  auto allocated = SaveBuffers(restored, checkpoint, descriptor_);
  EXPECT_NE(allocated[0].address, freed[0].address);
  allocator.Load(checkpoint, reader, descriptor_, "art_buffer_");
  EXPECT_EQ(*allocator.Get<const uint64_t>(second, false), 123456);
}

TEST_F(ARTAllocatorTest, PackedObjectRootsRetainAndCollectOldSnapshots) {
  FixedSizeAllocator allocator(131072, *checkpoint_);
  auto first = allocator.New();
  auto second = allocator.New();
  *allocator.Get<uint64_t>(first) = 100;
  *allocator.Get<uint64_t>(second) = 200;
  ARTBufferMetadataMap metadata;
  auto publish = [&]() {
    auto staging = checkpoint_mgr_.CreateStaging();
    auto checkpoint = staging.checkpoint();
    descriptor_ = ModuleDescriptor{};
    metadata = SaveBuffers(allocator, *checkpoint, descriptor_);
    auto published = PublishART(staging, descriptor_);
    auto reader = published->CreateObjectReader();
    allocator.Load(*published, reader,
                   *published->manifest().FindModule("art_allocator"),
                   "art_buffer_");
    return published;
  };
  auto oldest = publish();
  const auto old_object = metadata[0].address;
  *allocator.Get<uint64_t>(first) = 101;
  auto middle = publish();
  oldest.reset();
  checkpoint_mgr_.CollectGarbage();
  EXPECT_TRUE(std::filesystem::exists(old_object));

  // Keep the middle snapshot alive after its last buffer moves to a new object.
  *allocator.Get<uint64_t>(second) = 201;
  auto latest = publish();
  for (const auto& [key, path] : descriptor_.paths())
    EXPECT_NE(path, old_object);
  checkpoint_mgr_.CollectGarbage();
  EXPECT_TRUE(std::filesystem::exists(old_object));
  middle.reset();
  checkpoint_mgr_.CollectGarbage();
  EXPECT_FALSE(std::filesystem::exists(old_object));

  // Check that all objects referenced by the current manifest survived GC.
  const auto* descriptor = latest->manifest().FindModule("art_allocator");
  ASSERT_NE(descriptor, nullptr);
  FixedSizeAllocator restored(131072, *latest);
  auto reader = latest->CreateObjectReader();
  restored.Load(*latest, reader, *descriptor, "art_buffer_");
  EXPECT_EQ(*restored.Get<uint64_t>(first), 101);
  EXPECT_EQ(*restored.Get<uint64_t>(second), 201);
}

TEST_F(ARTAllocatorTest, RewritesObjectsCollectedAfterDiscard) {
  FixedSizeAllocator allocator(131072, *checkpoint_);
  const auto node = allocator.New();
  *allocator.Get<uint64_t>(node) = 123;
  auto abandoned = checkpoint_mgr_.CreateStaging();
  ModuleDescriptor abandoned_descriptor;
  const auto abandoned_object =
      SaveBuffers(allocator, *abandoned.checkpoint(), abandoned_descriptor)
          .at(node.GetBufferId())
          .address;
  ASSERT_TRUE(std::filesystem::exists(abandoned_object));
  abandoned.Discard();
  checkpoint_mgr_.CollectGarbage();
  ASSERT_FALSE(std::filesystem::exists(abandoned_object));

  auto retry = checkpoint_mgr_.CreateStaging();
  ModuleDescriptor descriptor;
  const auto& saved = SaveBuffers(allocator, *retry.checkpoint(), descriptor);
  EXPECT_NE(saved.at(node.GetBufferId()).address, abandoned_object);
  auto published = PublishART(retry, descriptor);
  auto reader = published->CreateObjectReader();
  FixedSizeAllocator restored(131072, *published);
  restored.Load(*published, reader,
                *published->manifest().FindModule("art_allocator"),
                "art_buffer_");
  EXPECT_EQ(*restored.Get<const uint64_t>(node, false), 123);
}

TEST_F(ARTAllocatorTest, ReopensPackedAndIncrementalBuffersFromManifest) {
  const auto path = (temp_dir_ / "reopen").string();
  const IndexPointer first(0, 0), second(1, 0), third(2, 0);
  {
    CheckpointManager manager;
    manager.Open(path);
    auto checkpoint = make_checkpoint(manager);
    FixedSizeAllocator allocator(131072, *checkpoint, MemoryLevel::kSyncToFile);
    ASSERT_EQ(allocator.GetBufferCapacity(), 1);
    ASSERT_EQ(allocator.New(), first);
    ASSERT_EQ(allocator.New(), second);
    ASSERT_EQ(allocator.New(), third);
    *allocator.Get<uint64_t>(first) = 100;
    *allocator.Get<uint64_t>(second) = 200;
    *allocator.Get<uint64_t>(third) = 300;

    auto publish = [&]() {
      auto staging = manager.CreateStaging();
      ModuleDescriptor descriptor;
      SaveBuffers(allocator, *staging.checkpoint(), descriptor);
      descriptor.set_path("whole_object",
                          descriptor.paths().at("art_buffer_0"));
      return PublishART(staging, descriptor);
    };

    auto initial = publish();
    const auto* initial_descriptor =
        initial->manifest().FindModule("art_allocator");
    ASSERT_NE(initial_descriptor, nullptr);
    const auto shared_object = initial_descriptor->get_path("art_buffer_0");
    ASSERT_TRUE(shared_object);
    EXPECT_EQ(initial_descriptor->get_path("art_buffer_1"), shared_object);
    EXPECT_EQ(initial_descriptor->get_path("art_buffer_2"), shared_object);

    auto reader = initial->CreateObjectReader();
    allocator.Load(*initial, reader, *initial_descriptor, "art_buffer_");
    *allocator.Get<uint64_t>(first) = 101;
    auto incremental = publish();
    const auto* descriptor =
        incremental->manifest().FindModule("art_allocator");
    ASSERT_NE(descriptor, nullptr);
    ASSERT_TRUE(descriptor->has_path("art_buffer_0"));
    EXPECT_NE(descriptor->get_path("art_buffer_0"), shared_object);
    EXPECT_EQ(descriptor->get_path("art_buffer_1"), shared_object);
    EXPECT_EQ(descriptor->get_path("art_buffer_2"), shared_object);
  }

  CheckpointManager reopened;
  reopened.Open(path, false);
  auto checkpoint = reopened.Current();
  ASSERT_NE(checkpoint, nullptr);
  const auto* descriptor = checkpoint->manifest().FindModule("art_allocator");
  ASSERT_NE(descriptor, nullptr);
  EXPECT_FALSE(descriptor->has("art_buffer_segment_count"));
  rapidjson::Document json;
  const auto module = descriptor->ToJson(json.GetAllocator());
  ASSERT_TRUE(module.HasMember("slices"));
  EXPECT_TRUE(module["objects"]["art_buffer_1"].IsString());
  const auto& slices = module["slices"];
  EXPECT_FALSE(slices.HasMember("whole_object"));
  EXPECT_EQ(slices["art_buffer_1"]["length"].GetUint64(),
            FixedSizeBuffer::BUFFER_ALLOC_SIZE);
  EXPECT_EQ(descriptor->get_path("whole_object"),
            descriptor->get_path("art_buffer_0"));
  auto reader = checkpoint->CreateObjectReader();
  const auto whole_object = reader.Get(*descriptor, "whole_object");
  EXPECT_EQ(whole_object.offset, 0);
  EXPECT_EQ(whole_object.length, FixedSizeBuffer::BUFFER_ALLOC_SIZE);
  const auto second_slice = reader.Get(*descriptor, "art_buffer_1");
  const auto third_slice = reader.Get(*descriptor, "art_buffer_2");
  EXPECT_NE(second_slice.offset, third_slice.offset);
  EXPECT_EQ(second_slice.length, FixedSizeBuffer::BUFFER_ALLOC_SIZE);
  EXPECT_EQ(third_slice.length, FixedSizeBuffer::BUFFER_ALLOC_SIZE);
  FixedSizeAllocator restored(131072, *checkpoint, MemoryLevel::kSyncToFile);
  restored.Load(*checkpoint, reader, *descriptor, "art_buffer_");
  EXPECT_EQ(restored.GetSegmentCount(), 3);
  EXPECT_EQ(*restored.Get<const uint64_t>(first, false), 101);
  EXPECT_EQ(*restored.Get<const uint64_t>(second, false), 200);
  EXPECT_EQ(*restored.Get<const uint64_t>(third, false), 300);
}

}  // namespace neug::art
