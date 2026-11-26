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
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include "neug/storages/index/art_tree/art.h"
#include "unittest/utils.h"
namespace neug::art {
namespace {
class ARTTreeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const auto* test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    temp_dir_ =
        std::filesystem::temp_directory_path() /
        ("neug_art_tree_" + std::string(test_info->name()) + "_" +
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

TEST_F(ARTTreeTest, GrowthNestedLeavesAndRoundTrip) {
  ArtTree tree(*checkpoint_);
  // Force all fanout transitions, nested 7/15/256 leaves and prefix chains.
  for (unsigned i = 0; i < 256; ++i) {
    EXPECT_TRUE(tree.Insert(Value::STRING(std::string(1, char(i))), i));
    EXPECT_TRUE(tree.Insert(Value::STRING("duplicate"), i));
    EXPECT_FALSE(tree.Insert(Value::STRING("duplicate"), i));
  }
  EXPECT_TRUE(tree.Insert(Value::STRING(""), 1000));
  EXPECT_TRUE(tree.Insert(Value::STRING("a"), 1001));
  EXPECT_TRUE(tree.Insert(Value::STRING("aa"), 1002));
  EXPECT_TRUE(tree.Insert(Value::STRING(std::string(600, 'x')), 1003));
  EXPECT_TRUE(tree.Insert(Value::STRING("large-id"), UINT32_MAX - 1));
  tree.Verify();
  auto all = tree.Range({}, {});
  EXPECT_EQ(all.size(), 261);
  auto duplicates = tree.Lookup(Value::STRING("duplicate"));
  ASSERT_EQ(duplicates.size(), 256);
  for (unsigned i = 0; i < 256; ++i) {
    EXPECT_TRUE(tree.Contains(Value::STRING("duplicate"), i));
    EXPECT_TRUE(tree.Contains(Value::STRING(std::string(1, char(i))), i));
  }
  EXPECT_EQ(tree.Lookup(Value::STRING("missing")).size(), 0);
  ModuleDescriptor image;
  tree.Save(*checkpoint_, image);
  auto copy = ArtTree::Load(image, *checkpoint_);
  EXPECT_EQ(copy->Range({}, {}), all);
  EXPECT_TRUE(copy->Contains(Value::STRING(std::string(600, 'x')), 1003));
  EXPECT_TRUE(copy->Contains(Value::STRING("large-id"), UINT32_MAX - 1));
  EXPECT_TRUE(copy->Insert(Value::STRING("after-load"), 2000));
  EXPECT_TRUE(tree.Insert(Value::STRING("after-save"), 2001));
  copy->Verify();
  tree.Verify();
  EXPECT_FALSE(copy->Contains(Value::STRING("after-save"), 2001));
}
TEST_F(ARTTreeTest, RandomizedRangeAgainstOrderedMap) {
  ArtTree tree(*checkpoint_);
  std::map<std::string, std::set<index_id_t>> oracle;
  std::mt19937 rng(72);
  for (unsigned i = 0; i < 4000; ++i) {
    std::string key;
    for (unsigned n = rng() % 40; n; --n)
      key += char(rng() % 256);
    auto id = rng() % 10000;
    EXPECT_EQ(tree.Insert(Value::STRING(key), id),
              oracle[key].insert(id).second);
  }
  tree.Verify();
  for (unsigned trial = 0; trial < 100; ++trial) {
    std::string lo(1, char(rng() % 256)), hi(1, char(rng() % 256));
    if (hi < lo)
      std::swap(lo, hi);
    bool li = rng() % 2, ui = rng() % 2;
    std::set<index_id_t> expected;
    for (auto& [key, ids] : oracle)
      if ((key > lo || (li && key == lo)) && (key < hi || (ui && key == hi)))
        expected.insert(ids.begin(), ids.end());
    EXPECT_EQ(tree.Range(Value::STRING(lo), Value::STRING(hi), li, ui),
              (std::vector<index_id_t>(expected.begin(), expected.end())));
  }
}
TEST_F(ARTTreeTest, ConcurrentReadersAndWriter) {
  ArtTree tree(*checkpoint_);
  tree.Insert(Value::STRING("key"), 0);
  std::thread writer([&] {
    for (unsigned i = 1; i < 2000; ++i)
      tree.Insert(Value::STRING("key"), i);
  });
  std::thread reader([&] {
    for (unsigned i = 0; i < 200; ++i) {
      EXPECT_TRUE(tree.Contains(Value::STRING("key"), 0));
      auto ids = tree.Lookup(Value::STRING("key"));
      EXPECT_FALSE(ids.empty());
    }
  });
  writer.join();
  reader.join();
  EXPECT_EQ(tree.Lookup(Value::STRING("key")).size(), 2000);
  tree.Verify();
}
TEST_F(ARTTreeTest, RejectTruncatedAndMissingBuffer) {
  auto& checkpoint = *checkpoint_;
  ArtTree tree(checkpoint);
  tree.Insert(Value::STRING("key"), 1);
  ModuleDescriptor saved;
  tree.Save(checkpoint, saved);
  ASSERT_FALSE(saved.paths().empty());
  const auto& [key, address] = *saved.paths().begin();
  auto truncated = checkpoint.CreateRuntimeContainer(
      FixedSizeBuffer::BUFFER_ALLOC_SIZE - 1, MemoryLevel::kInMemory);
  auto invalid = saved;
  invalid.set_path(key, checkpoint.Commit(*truncated));
  EXPECT_THROW(ArtTree::Load(invalid, checkpoint), std::exception);
  invalid = saved;
  invalid.mutable_paths().erase(key);
  EXPECT_THROW(ArtTree::Load(invalid, checkpoint), std::exception);
}

TEST_F(ARTTreeTest, RejectInvalidRootReference) {
  auto& checkpoint = *checkpoint_;
  ArtTree tree(checkpoint);
  ASSERT_TRUE(tree.Insert(Value::STRING("key"), 1));
  ModuleDescriptor saved;
  tree.Save(checkpoint, saved);

  ASSERT_EQ(saved.get("art_allocator_0_segment_count"), "1");
  const auto encoded_root = std::stoull(saved.get("art_root").value());
  NodePtr root;
  root.Set(encoded_root);
  ASSERT_EQ(root.GetType(), NType::PREFIX);
  ASSERT_EQ(root.GetOffset(), 0);

  const auto type = encoded_root & IndexPointer::AND_METADATA;
  const auto buffer = encoded_root & IndexPointer::AND_BUFFER_ID;
  const std::pair<const char*, idx_t> invalid_roots[] = {
      {"invalid type", (idx_t{0x7f} << IndexPointer::SHIFT_METADATA) | buffer},
      {"missing buffer", type | IndexPointer::AND_BUFFER_ID},
      {"out of bounds slot",
       type | buffer |
           (IndexPointer::AND_OFFSET << IndexPointer::SHIFT_OFFSET)},
      {"unallocated slot",
       type | buffer | (idx_t{1} << IndexPointer::SHIFT_OFFSET)},
  };
  for (const auto& [reason, invalid_root] : invalid_roots) {
    SCOPED_TRACE(reason);
    auto invalid = saved;
    invalid.set("art_root", std::to_string(invalid_root));
    EXPECT_THROW(ArtTree::Load(invalid, checkpoint), SerializationException);
  }
}

TEST_F(ARTTreeTest, RejectInvalidPrefixChildReference) {
  auto& checkpoint = *checkpoint_;
  ArtTree tree(checkpoint);
  ASSERT_TRUE(tree.Insert(Value::STRING("key"), 1));
  ModuleDescriptor saved;
  tree.Save(checkpoint, saved);

  NodePtr root;
  root.Set(std::stoull(saved.get("art_root").value()));
  ASSERT_EQ(root.GetType(), NType::PREFIX);
  const auto segment_size =
      std::stoull(saved.get("art_allocator_0_segment_size").value());
  FixedSizeAllocator allocator(segment_size, checkpoint);
  auto reader = checkpoint.CreateObjectReader();
  const std::string prefix = "art_allocator_0_buffer_";
  allocator.Load(checkpoint, reader, saved, prefix);
  const idx_t invalid_child =
      (root.Get() & IndexPointer::AND_METADATA) | IndexPointer::AND_BUFFER_ID;
  ARTStore(invalid_child, allocator.Get(root) + tree.PrefixCount() + 1);

  // Submit valid object bytes so the reader reaches reference validation.
  auto writer = checkpoint.CreateObjectWriter();
  allocator.Save(writer);
  writer.Commit();
  auto invalid = saved;
  allocator.SaveMetadata(invalid, prefix);
  EXPECT_THROW(ArtTree::Load(invalid, checkpoint), SerializationException);
}

TEST_F(ARTTreeTest, MultipleBuffersAndStableCheckpoint) {
  ArtTree tree(*checkpoint_, MemoryLevel::kInMemory, 239);
  for (unsigned i = 0; i < 1500; ++i) {
    std::string key;
    key += char(i >> 8);
    key += char(i);
    key += std::string(260, 'q');
    tree.Insert(Value::STRING(key), i);
  }
  tree.Verify();
  {
    CheckpointManager manager;
    manager.Open((temp_dir_ / "target").string());
    auto target_checkpoint = make_checkpoint(manager);
    ModuleDescriptor saved;
    tree.Save(*target_checkpoint, saved);
    auto copy = ArtTree::Load(saved, *checkpoint_);
    EXPECT_EQ(copy->Range({}, {}).size(), 1500);
    EXPECT_FALSE(copy->Contains(Value::STRING("later"), 1500));
    ASSERT_TRUE(copy->Insert(Value::STRING("later"), 1500));
    ModuleDescriptor next;
    copy->Save(*target_checkpoint, next);
    ASSERT_FALSE(saved.paths().empty());
    const auto& first_object = saved.paths().begin()->second;
    std::set<std::string> next_objects;
    for (const auto& [key, address] : next.paths())
      next_objects.insert(address);
    EXPECT_TRUE(next_objects.contains(first_object));
    EXPECT_EQ(next_objects.size(), 2);
    auto updated = ArtTree::Load(next, *checkpoint_);
    EXPECT_TRUE(updated->Contains(Value::STRING("later"), 1500));
    EXPECT_FALSE(tree.Contains(Value::STRING("later"), 1500));
    tree.Verify();
    updated->Verify();
  }
}

TEST_F(ARTTreeTest, PublishedCheckpointPreservesInPlaceNodeUpdate) {
  ArtTree tree(*checkpoint_);
  const auto key = Value::STRING("duplicate");
  ASSERT_TRUE(tree.Insert(key, 10));
  ASSERT_TRUE(tree.Insert(key, 20));
  auto publish = [&] {
    auto staging = checkpoint_mgr_.CreateStaging();
    auto checkpoint = staging.checkpoint();
    ModuleDescriptor descriptor;
    tree.Save(*checkpoint, descriptor);
    CheckpointManifest manifest;
    manifest.SetSchema(Schema());
    manifest.SetModule("art", std::move(descriptor));
    checkpoint->SetManifest(std::move(manifest));
    return staging.Publish();
  };

  auto first = publish();
  auto object_count = [&] {
    return std::distance(std::filesystem::directory_iterator(
                             temp_dir_ / "checkpoint" / "objects"),
                         std::filesystem::directory_iterator{});
  };
  EXPECT_EQ(object_count(), 1);
  const auto* first_image = first->manifest().FindModule("art");
  ASSERT_NE(first_image, nullptr);
  const std::string prefix_buffer = "art_allocator_0_buffer_0";
  const std::string leaf_buffer = "art_allocator_5_buffer_0";
  ASSERT_TRUE(first_image->has_path(prefix_buffer));
  ASSERT_TRUE(first_image->has_path(leaf_buffer));
  EXPECT_EQ(first_image->get_path(prefix_buffer),
            first_image->get_path(leaf_buffer));
  const auto prefix_slice = first_image->get_slice(prefix_buffer);
  const auto leaf_slice = first_image->get_slice(leaf_buffer);
  ASSERT_TRUE(prefix_slice);
  ASSERT_TRUE(leaf_slice);
  EXPECT_NE(prefix_slice->offset, leaf_slice->offset);

  // Adding the third ID updates the existing Node7Leaf in place.
  ASSERT_TRUE(tree.Insert(key, 30));
  auto second = publish();
  EXPECT_EQ(object_count(), 2);
  const auto* second_image = second->manifest().FindModule("art");
  ASSERT_NE(second_image, nullptr);
  EXPECT_NE(second_image->get_path(leaf_buffer),
            first_image->get_path(leaf_buffer));
  auto old_tree = ArtTree::Load(*first_image, *first);
  EXPECT_FALSE(old_tree->Contains(key, 30));

  CheckpointManager reopened;
  reopened.Open(temp_dir_.string(), false);
  auto current = reopened.Current();
  ASSERT_NE(current, nullptr);
  const auto* reopened_image = current->manifest().FindModule("art");
  ASSERT_NE(reopened_image, nullptr);
  auto restored = ArtTree::Load(*reopened_image, *current);
  EXPECT_TRUE(restored->Contains(key, 10));
  EXPECT_TRUE(restored->Contains(key, 20));
  EXPECT_TRUE(restored->Contains(key, 30));
  auto unchanged = *reopened_image;
  const std::string stale_buffer = "art_allocator_0_buffer_99";
  unchanged.set_path(stale_buffer, *reopened_image->get_path(prefix_buffer));
  unchanged.set_slice(stale_buffer, *prefix_slice);
  restored->Save(*current, unchanged);
  EXPECT_EQ(object_count(), 2);
  EXPECT_EQ(unchanged.paths(), reopened_image->paths());
  EXPECT_FALSE(unchanged.get_slice(stale_buffer));
}

TEST_F(ARTTreeTest, RestoreBeforeGrowthAndNestedFanout) {
  auto tree = std::make_unique<ArtTree>(*checkpoint_);
  for (unsigned i = 0; i < 256; ++i) {
    tree->Insert(Value::STRING(std::string(1, char(i))), i);
    tree->Insert(Value::STRING("nested"), (i << 24) | i);
    if (i == 2 || i == 3 || i == 15 || i == 47) {
      ModuleDescriptor image;
      tree->Save(*checkpoint_, image);
      tree = ArtTree::Load(image, *checkpoint_);
    }
  }
  tree->Verify();
  EXPECT_EQ(tree->Lookup(Value::STRING("nested")).size(), 256);
  for (unsigned i = 0; i < 256; ++i) {
    EXPECT_TRUE(tree->Contains(Value::STRING(std::string(1, char(i))), i));
    EXPECT_TRUE(tree->Contains(Value::STRING("nested"), (i << 24) | i));
  }
  EXPECT_THROW(tree->Insert(Value::STRING("bad"), INVALID_INDEX_ID),
               std::exception);
  ModuleDescriptor image;
  tree->Save(*checkpoint_, image);
  auto loaded = ArtTree::Load(image, *checkpoint_);
  EXPECT_EQ(loaded->Range({}, {}), tree->Range({}, {}));
}
TEST_F(ARTTreeTest, RejectInvalidPrefixAndAllocatorCount) {
  ArtTree tree(*checkpoint_);
  tree.Insert(Value::STRING("key"), 1);
  ModuleDescriptor saved;
  tree.Save(*checkpoint_, saved);
  EXPECT_EQ(saved.get("art_allocator_count"), "8");
  for (const auto& [key, value] :
       std::vector<std::pair<std::string, std::string>>{
           {"art_prefix_count", "0"},
           {"art_prefix_count", "240"},
           {"art_allocator_count", "9"}}) {
    auto invalid = saved;
    invalid.set(key, value);
    EXPECT_THROW(ArtTree::Load(invalid, *checkpoint_), std::exception);
  }
}
TEST_F(ARTTreeTest, NeuGDispatchAndOrdering) {
  ARTArenaAllocator arena;
  auto key = [&](const Value& value) {
    return ARTKey::CreateKey(arena, value);
  };
  EXPECT_TRUE(key(Value()).Empty());
  EXPECT_TRUE(key(Value(DataType::INT32)).Empty());
  EXPECT_TRUE(key(Value::BOOLEAN(true)) > key(Value::BOOLEAN(false)));
  for (const auto& values : std::vector<std::vector<Value>>{
           {Value::INT32(INT32_MIN), Value::INT32(-1), Value::INT32(0),
            Value::INT32(INT32_MAX)},
           {Value::INT64(INT64_MIN), Value::INT64(-1), Value::INT64(0),
            Value::INT64(INT64_MAX)},
           {Value::UINT32(0), Value::UINT32(256), Value::UINT32(UINT32_MAX)},
           {Value::UINT64(0), Value::UINT64(256), Value::UINT64(UINT64_MAX)},
           {Value::FLOAT(-std::numeric_limits<float>::infinity()),
            Value::FLOAT(-1), Value::FLOAT(0), Value::FLOAT(1),
            Value::FLOAT(std::numeric_limits<float>::infinity())},
           {Value::DOUBLE(-std::numeric_limits<double>::infinity()),
            Value::DOUBLE(-1), Value::DOUBLE(0), Value::DOUBLE(1),
            Value::DOUBLE(std::numeric_limits<double>::infinity())},
           {Value::STRING(""), Value::STRING(std::string(1, '\0')),
            Value::STRING(std::string(1, '\1')), Value::STRING("a"),
            Value::STRING("aa"), Value::STRING("b")}}) {
    ArtTree tree(*checkpoint_);
    for (size_t i = 0; i < values.size(); ++i) {
      EXPECT_TRUE(tree.Insert(values[i], i));
      EXPECT_TRUE(tree.Contains(values[i], i));
      EXPECT_EQ(tree.Lookup(values[i]),
                (std::vector<index_id_t>{index_id_t(i)}));
    }
    for (size_t i = 1; i < values.size(); ++i) {
      EXPECT_TRUE(values[i - 1] < values[i]);
      EXPECT_TRUE(key(values[i]) > key(values[i - 1]));
      EXPECT_EQ(tree.Range(values[i - 1], values[i]),
                (std::vector<index_id_t>{index_id_t(i - 1), index_id_t(i)}));
    }
    ModuleDescriptor image;
    tree.Save(*checkpoint_, image);
    auto loaded = ArtTree::Load(image, *checkpoint_);
    EXPECT_EQ(loaded->Lookup(values.front()), (std::vector<index_id_t>{0}));
  }
  auto text = key(Value::VARCHAR(std::string("\0\1", 2), 10));
  EXPECT_EQ((std::vector<uint8_t>(text.data, text.data + text.len)),
            (std::vector<uint8_t>{1, 0, 1, 1, 0}));
  EXPECT_TRUE(key(Value::FLOAT(-0.0f)) == key(Value::FLOAT(0.0f)));
  EXPECT_TRUE(key(Value::DOUBLE(-0.0)) == key(Value::DOUBLE(0.0)));
  EXPECT_TRUE(key(Value::DOUBLE(std::numeric_limits<double>::quiet_NaN())) ==
              key(Value::DOUBLE(-std::numeric_limits<double>::quiet_NaN())));
  EXPECT_TRUE(key(Value::DOUBLE(std::numeric_limits<double>::quiet_NaN())) >
              key(Value::DOUBLE(std::numeric_limits<double>::infinity())));
  ARTKey target;
  ARTKey::CreateARTKey<int32_t>(arena, target, Value::INT32(-1));
  EXPECT_TRUE(target == key(Value::INT32(-1)));
}
TEST_F(ARTTreeTest, RejectTemporalAndCompositeValues) {
  ARTArenaAllocator arena;
  for (const auto& value : std::vector<Value>{
           Value::DATE(Date("2026-10-09")), Value::TIMESTAMPMS(DateTime(0)),
           Value::INTERVAL(Interval(std::string("1 day"))),
           Value::LIST(DataType::INT32, {}),
           Value::STRUCT(std::vector<Value>{Value::INT32(1)})}) {
    EXPECT_THROW(ARTKey::CreateKey(arena, value), std::exception);
  }
}
TEST_F(ARTTreeTest, ScalarKeysWorkWithTreeAndPersistence) {
  ArtTree tree(*checkpoint_);
  for (int32_t i = -50; i <= 50; ++i)
    tree.Insert(Value::INT32(i), i + 50);
  EXPECT_EQ(tree.Range(Value::INT32(-2), Value::INT32(2)),
            (std::vector<index_id_t>{48, 49, 50, 51, 52}));
  ModuleDescriptor image;
  tree.Save(*checkpoint_, image);
  auto loaded = ArtTree::Load(image, *checkpoint_);
  EXPECT_EQ(loaded->Lookup(Value::INT32(-2)), (std::vector<index_id_t>{48}));
}

TEST_F(ARTTreeTest, TreeNullAndUnsupportedInputs) {
  ArtTree tree(*checkpoint_);
  EXPECT_FALSE(tree.Insert(Value(), 1));
  EXPECT_TRUE(tree.Insert(Value::STRING(""), 2));
  EXPECT_FALSE(tree.Contains(Value(), 2));
  EXPECT_TRUE(tree.Lookup(Value()).empty());
  EXPECT_EQ(tree.Lookup(Value::STRING("")), (std::vector<index_id_t>{2}));
  EXPECT_TRUE(tree.Range(Value(), std::nullopt).empty());
  EXPECT_TRUE(tree.Range(std::nullopt, Value()).empty());
  EXPECT_EQ(tree.Range(std::nullopt, std::nullopt),
            (std::vector<index_id_t>{2}));
  ArtTree empty(*checkpoint_);
  auto date = Value::DATE(Date("2026-10-09"));
  EXPECT_THROW(empty.Insert(date, 0), std::exception);
  EXPECT_THROW(empty.Contains(date, 0), std::exception);
  EXPECT_THROW(empty.Lookup(date), std::exception);
  EXPECT_THROW(empty.Range(date, std::nullopt), std::exception);
}

TEST_F(ARTTreeTest, BatchMatchesSingleInsertAndPersistence) {
  ArtTree batch(*checkpoint_), single(*checkpoint_);
  std::vector<Value> values;
  std::vector<index_id_t> ids;
  for (unsigned i = 0; i < 256; ++i) {
    values.push_back(Value::STRING(std::string(1, char(i))));
    ids.push_back(i);
    values.push_back(Value::STRING("nested"));
    ids.push_back((i << 24) | i);
  }
  values.push_back(Value());
  ids.push_back(1000);
  values.push_back(Value::STRING(""));
  ids.push_back(1001);
  values.push_back(Value::STRING(std::string(600, 'x')));
  ids.push_back(1002);
  values.push_back(values.front());
  ids.push_back(ids.front());
  size_t inserted = 0;
  for (size_t i = 0; i < values.size(); ++i)
    inserted += single.Insert(values[i], ids[i]);
  EXPECT_EQ(batch.Insert(values, ids), inserted);
  EXPECT_EQ(batch.Insert(values, ids), 0);
  EXPECT_EQ(
      batch.Insert(std::span<const Value>{}, std::span<const index_id_t>{}), 0);
  EXPECT_EQ(batch.Range({}, {}), single.Range({}, {}));
  EXPECT_EQ(batch.Lookup(Value::STRING("nested")),
            single.Lookup(Value::STRING("nested")));
  batch.Verify();
  ModuleDescriptor image;
  batch.Save(*checkpoint_, image);
  auto loaded = ArtTree::Load(image, *checkpoint_);
  EXPECT_EQ(loaded->Lookup(Value::STRING("nested")),
            batch.Lookup(Value::STRING("nested")));
  std::vector<Value> next{Value::STRING("after-load")};
  std::vector<index_id_t> next_ids{2000};
  EXPECT_EQ(loaded->Insert(next, next_ids), 1);
  EXPECT_TRUE(loaded->Contains(next.front(), next_ids.front()));
}

TEST_F(ARTTreeTest, BatchValidatesInputsBeforeInsertion) {
  ArtTree tree(*checkpoint_);
  std::vector<Value> values{Value::INT32(1), Value::INT32(2)};
  std::vector<index_id_t> ids{1};
  EXPECT_THROW(tree.Insert(values, ids), std::exception);
  EXPECT_TRUE(tree.Range({}, {}).empty());
  ids.push_back(INVALID_INDEX_ID);
  EXPECT_THROW(tree.Insert(values, ids), std::exception);
  EXPECT_TRUE(tree.Range({}, {}).empty());
  ids.back() = 2;
  values.back() = Value::DATE(Date("2026-10-09"));
  EXPECT_THROW(tree.Insert(values, ids), std::exception);
  EXPECT_TRUE(tree.Range({}, {}).empty());
  values.back() = Value::INT32(2);
  EXPECT_EQ(tree.Insert(values, ids), 2);
  EXPECT_EQ(tree.Range(Value::INT32(1), Value::INT32(2)),
            (std::vector<index_id_t>{1, 2}));
}

TEST_F(ARTTreeTest, UpstreamSortedBuilderMatchesInsertion) {
  ArtTree built(*checkpoint_), inserted(*checkpoint_);
  std::vector<Value> keys;
  std::vector<index_id_t> ids;
  for (index_id_t i = 0; i < 32768; ++i) {
    keys.push_back(Value::INT64(i / 32));
    ids.push_back(i);
  }
  EXPECT_EQ(built.Build(keys, ids), ARTConflictType::NO_CONFLICT);
  EXPECT_EQ(inserted.Insert(keys, ids), ids.size());
  EXPECT_EQ(built.Range({}, {}), inserted.Range({}, {}));
  for (int64_t key : {0, 255, 256, 1023})
    EXPECT_EQ(built.Lookup(Value::INT64(key)),
              inserted.Lookup(Value::INT64(key)));
  EXPECT_EQ(built.Range(Value::INT64(255), Value::INT64(512), false, true),
            inserted.Range(Value::INT64(255), Value::INT64(512), false, true));
  built.Verify();
  built.VerifyAllocations();
  built.VerifyBuffers();
  ModuleDescriptor image;
  built.Save(*checkpoint_, image);
  auto loaded = ArtTree::Load(image, *checkpoint_);
  EXPECT_EQ(loaded->Lookup(Value::INT64(256)), built.Lookup(Value::INT64(256)));
  EXPECT_TRUE(loaded->Insert(Value::INT64(256), 40000));
  EXPECT_TRUE(loaded->Contains(Value::INT64(256), 40000));
}

TEST_F(ARTTreeTest, TreeBreakdownAndRoundTrip) {
  ArtTree tree(*checkpoint_);
  auto empty = tree.GetMemoryUsage();
  EXPECT_EQ(empty.node_buffer_bytes, 0);
  EXPECT_GT(empty.directory_bytes, 0);
  std::vector<Value> values;
  std::vector<index_id_t> ids;
  for (index_id_t i = 0; i < 1000; ++i) {
    values.push_back(Value::INT64(i % 10));
    ids.push_back(i);
  }
  EXPECT_EQ(tree.Insert(values, ids), ids.size());
  auto before = tree.GetMemoryUsage();
  EXPECT_GT(before.occupied_node_bytes, 0);
  EXPECT_LE(before.occupied_node_bytes, before.node_buffer_bytes);
  EXPECT_GT(before.occupancy_bytes, 0);
  EXPECT_EQ(before.free_slots_bytes, 0);
  EXPECT_EQ(before.TotalBytes(),
            before.node_buffer_bytes + before.occupancy_bytes +
                before.free_slots_bytes + before.directory_bytes);
  EXPECT_EQ(tree.Insert(values, ids), 0);
  EXPECT_EQ(tree.GetMemoryUsage().TotalBytes(), before.TotalBytes());
  ModuleDescriptor image;
  tree.Save(*checkpoint_, image);
  auto saved = tree.GetMemoryUsage();
  EXPECT_LE(saved.node_buffer_bytes, before.node_buffer_bytes);
  EXPECT_EQ(saved.occupied_node_bytes, before.occupied_node_bytes);
  auto loaded = ArtTree::Load(image, *checkpoint_);
  auto after = loaded->GetMemoryUsage();
  EXPECT_EQ(after.node_buffer_bytes, saved.node_buffer_bytes);
  EXPECT_EQ(after.occupied_node_bytes, before.occupied_node_bytes);
  EXPECT_EQ(after.occupancy_bytes, saved.occupancy_bytes);
  EXPECT_EQ(loaded->Lookup(Value::INT64(3)), tree.Lookup(Value::INT64(3)));
}

TEST_F(ARTTreeTest, TreeBatchAndFixedBufferRoundTrip) {
  ArtTree batch(*checkpoint_), single(*checkpoint_);
  std::vector<Value> values;
  std::vector<index_id_t> ids;
  for (index_id_t i = 0; i < 20000; ++i) {
    values.push_back(Value::INT64((i * 37) % 1000));
    ids.push_back(i);
    EXPECT_TRUE(single.Insert(values.back(), i));
  }
  EXPECT_EQ(batch.Insert(values, ids), ids.size());
  for (int64_t key = 0; key < 1000; ++key)
    EXPECT_EQ(batch.Lookup(Value::INT64(key)),
              single.Lookup(Value::INT64(key)));
  EXPECT_EQ(batch.GetMemoryUsage().node_buffer_bytes,
            single.GetMemoryUsage().node_buffer_bytes);
  batch.VerifyAllocations();
  ModuleDescriptor image;
  batch.Save(*checkpoint_, image);
  auto loaded = ArtTree::Load(image, *checkpoint_);
  EXPECT_EQ(loaded->GetMemoryUsage().node_buffer_bytes,
            batch.GetMemoryUsage().node_buffer_bytes);
  for (index_id_t i = 20000; i < 40000; ++i)
    EXPECT_TRUE(loaded->Insert(Value::INT64((i * 37) % 1000), i));
  for (int64_t key = 0; key < 1000; ++key)
    EXPECT_EQ(loaded->Lookup(Value::INT64(key)).size(), 40);
  loaded->Verify();
  loaded->VerifyAllocations();
}

TEST_F(ARTTreeTest, BulkCleanupAndSaveCleanup) {
  ArtTree tree(*checkpoint_);
  for (index_id_t i = 0; i < 10000; ++i)
    ASSERT_TRUE(tree.Insert(Value::INT64((i * 37) % 100), i));
  const auto before = tree.GetMemoryUsage();
  EXPECT_EQ(before.node_buffer_bytes + before.occupancy_bytes,
            6 * FixedSizeBuffer::BUFFER_DATA_SIZE);
  const auto matches = tree.Range({}, {});
  tree.RemoveEmptyBuffers();
  const auto cleaned = tree.GetMemoryUsage();
  EXPECT_EQ(cleaned.node_buffer_bytes + cleaned.occupancy_bytes,
            4 * FixedSizeBuffer::BUFFER_DATA_SIZE);
  EXPECT_EQ(cleaned.occupied_node_bytes, before.occupied_node_bytes);
  EXPECT_EQ(tree.Range({}, {}), matches);
  tree.RemoveEmptyBuffers();
  EXPECT_EQ(tree.GetMemoryUsage().TotalBytes(), cleaned.TotalBytes());
  // Further ordinary inserts may recreate temporary node types and empty
  // blocks.
  for (index_id_t i = 10000; i < 20000; ++i)
    ASSERT_TRUE(tree.Insert(Value::INT64((i * 37) % 100), i));
  ModuleDescriptor image;
  tree.Save(*checkpoint_, image);
  const auto saved = tree.GetMemoryUsage();
  tree.RemoveEmptyBuffers();
  EXPECT_EQ(tree.GetMemoryUsage().TotalBytes(), saved.TotalBytes());
  auto loaded = ArtTree::Load(image, *checkpoint_);
  EXPECT_EQ(loaded->Range({}, {}), tree.Range({}, {}));
  EXPECT_EQ(loaded->GetMemoryUsage().node_buffer_bytes,
            saved.node_buffer_bytes);
  EXPECT_TRUE(loaded->Insert(Value::INT64(10), 20000));
  EXPECT_TRUE(tree.Insert(Value::INT64(10), 20001));
  EXPECT_TRUE(loaded->Contains(Value::INT64(10), 20000));
  EXPECT_TRUE(tree.Contains(Value::INT64(10), 20001));
  tree.Verify();
  loaded->Verify();
  tree.VerifyAllocations();
  loaded->VerifyAllocations();
}

TEST_F(ARTTreeTest, PrefixCursorPreservesKeysValuesAndReset) {
  ArtTree tree(*checkpoint_);
  ARTArenaAllocator arena;
  auto term = ARTKey::CreateKey(arena, Value::STRING(std::string("a\0\1b", 4)));
  ARTKey prefix(arena, term.len + 1);
  std::memcpy(prefix.data, term.data, term.len);
  prefix.data[term.len] = 0;
  std::vector<uint32_t> ids{0, 1, 255, 256, 0x01000100, UINT32_MAX - 1};
  std::vector<std::vector<uint8_t>> keys;
  for (auto id : ids) {
    ARTKey key(arena, prefix.len + 4);
    std::memcpy(key.data, prefix.data, prefix.len);
    for (size_t i = 0; i < 4; ++i)
      key.data[prefix.len + i] = id >> (24 - i * 8);
    keys.emplace_back(key.data, key.data + key.len);
    EXPECT_TRUE(tree.Insert(key, 2));
    EXPECT_FALSE(tree.Insert(key, 2));
    EXPECT_EQ(tree.Lookup(key), (std::vector<index_id_t>{2}));
  }
  auto other = keys[0];
  other[term.len] = 1;
  EXPECT_TRUE(tree.Insert(ARTKey(other.data(), other.size()), 9));
  auto read = tree.OpenRead();
  auto cursor = read.ScanPrefix(prefix);
  for (int pass = 0; pass < 2; ++pass) {
    for (const auto& key : keys) {
      auto row = cursor.Next();
      ASSERT_TRUE(row);
      EXPECT_EQ(row->key, key);
      EXPECT_EQ(row->value, 2);
    }
    EXPECT_FALSE(cursor.Next());
    cursor.Reset();
  }
  auto exact = read.ScanPrefix(ARTKey(other.data(), other.size()));
  ASSERT_TRUE(exact.Next());
  EXPECT_FALSE(exact.Next());
  EXPECT_EQ(read.Lookup(ARTKey(other.data(), other.size() - 1)).size(), 0);
}

TEST_F(ARTTreeTest, PrefixInsideCompressedNodesAndDuplicateValues) {
  ArtTree tree(*checkpoint_);
  ARTArenaAllocator arena;
  auto key = ARTKey::CreateKey(arena, Value::STRING(std::string(300, 'x')));
  EXPECT_TRUE(tree.Insert(key, 0));
  EXPECT_TRUE(tree.Insert(key, 256));
  EXPECT_TRUE(tree.Insert(key, 257));
  for (size_t length : {size_t(0), size_t(1), size_t(100), size_t(key.len)}) {
    auto cursor = [&] {
      auto read = tree.OpenRead();
      return read.ScanPrefix(ARTKey(key.data, length));
    }();
    for (auto value : {0, 256, 257}) {
      auto row = cursor.Next();
      ASSERT_TRUE(row);
      EXPECT_EQ(row->key, (std::vector<uint8_t>(key.data, key.data + key.len)));
      EXPECT_EQ(row->value, value);
    }
    EXPECT_FALSE(cursor.Next());
  }
  EXPECT_THROW(tree.Insert(ARTKey(key.data, key.len - 1), 1),
               InternalException);
  std::vector<uint8_t> longer(key.data, key.data + key.len);
  longer.push_back(0);
  EXPECT_THROW(tree.Insert(ARTKey(longer.data(), longer.size()), 1),
               InternalException);
  std::vector<uint8_t> missing{'y'};
  auto read = tree.OpenRead();
  auto cursor = read.ScanPrefix(ARTKey(missing.data(), missing.size()));
  EXPECT_FALSE(cursor.Next());
  cursor.Reset();
  EXPECT_FALSE(cursor.Next());
}

TEST_F(ARTTreeTest, BulkBuildMatchesInsertsAndOwnsInputBytes) {
  ArtTree built(*checkpoint_), inserted(*checkpoint_);
  std::vector<std::vector<uint8_t>> expected;
  std::vector<index_id_t> values;
  {
    ARTArenaAllocator arena;
    auto term = ARTKey::CreateKey(
        arena, Value::STRING(std::string(70, 'x') + std::string("\0\1", 2)));
    std::vector<ARTKey> keys;
    for (index_id_t i = 0; i < 4096; ++i) {
      ARTKey key(arena, term.len + 5);
      std::memcpy(key.data, term.data, term.len);
      key.data[term.len] = 0;
      for (size_t byte = 0; byte < 4; ++byte)
        key.data[term.len + 1 + byte] = i >> (24 - byte * 8);
      keys.push_back(key);
      values.push_back(i % 7);  // Repeated payloads, including zero.
      expected.emplace_back(key.data, key.data + key.len);
      ASSERT_TRUE(inserted.Insert(key, values.back()));
    }
    EXPECT_EQ(built.BuildEncoded(keys, values), ARTConflictType::NO_CONFLICT);
  }
  auto check = [&](ArtTree& tree) {
    tree.Verify();
    tree.VerifyAllocations();
    auto read = tree.OpenRead();
    auto cursor = read.ScanPrefix(ARTKey());
    for (size_t i = 0; i < expected.size(); ++i) {
      auto row = cursor.Next();
      ASSERT_TRUE(row);
      EXPECT_EQ(row->key, expected[i]);
      EXPECT_EQ(row->value, values[i]);
    }
    EXPECT_FALSE(cursor.Next());
  };
  // Input arena is gone; the tree must own all stored bytes.
  check(built);
  check(inserted);
  ModuleDescriptor image;
  built.Save(*checkpoint_, image);
  auto restored = ArtTree::Load(image, *checkpoint_);
  check(*restored);
  auto last = expected.back();
  last.back() = 0;
  last[last.size() - 2] = 16;  // Next key: doc ID 4096.
  EXPECT_TRUE(restored->Insert(ARTKey(last.data(), last.size()), 42));
  EXPECT_EQ(restored->Lookup(ARTKey(last.data(), last.size())),
            (std::vector<index_id_t>{42}));
}

TEST_F(ARTTreeTest, BulkBuildRejectsMalformedInputBeforeChangingTree) {
  ArtTree tree(*checkpoint_);
  std::vector<uint8_t> a{0}, b{0, 1}, c{1};
  auto key = [](auto& bytes) { return ARTKey(bytes.data(), bytes.size()); };
  std::vector<ARTKey> keys{key(a), key(b)};
  std::vector<index_id_t> values{0, 1};
  EXPECT_THROW(tree.BuildEncoded(keys, values), InternalException);  // Prefix.
  keys = {key(a), key(a)};
  EXPECT_THROW(tree.BuildEncoded(keys, values),
               InternalException);  // Duplicate.
  keys = {key(c), key(a)};
  EXPECT_THROW(tree.BuildEncoded(keys, values),
               InternalException);  // Unsorted.
  keys = {key(a), ARTKey()};
  EXPECT_THROW(tree.BuildEncoded(keys, values), InternalException);
  keys = {key(a), key(c)};
  values = {0, INVALID_INDEX_ID};
  EXPECT_THROW(tree.BuildEncoded(keys, values), InternalException);
  values = {0};
  EXPECT_THROW(tree.BuildEncoded(keys, values), InternalException);
  values = {0, 1};
  EXPECT_EQ(tree.BuildEncoded({}, {}), ARTConflictType::NO_CONFLICT);
  EXPECT_EQ(tree.BuildEncoded(keys, values), ARTConflictType::NO_CONFLICT);
  EXPECT_EQ(tree.Lookup(key(a)), (std::vector<index_id_t>{0}));
  EXPECT_THROW(tree.BuildEncoded(keys, values), InternalException);
}
}  // namespace neug::art
