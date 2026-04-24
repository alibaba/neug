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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/workspace.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/property.h"

namespace neug {

class LFIndexerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ =
        "/tmp/lf_indexer_test_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  template <typename INDEX_T>
  static void ExpectInt64Values(const LFIndexer<INDEX_T>& indexer,
                                const std::vector<int64_t>& values) {
    ASSERT_EQ(indexer.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      INDEX_T lid;
      ASSERT_TRUE(indexer.get_index(Property::from_int64(values[i]), lid));
      EXPECT_EQ(lid, static_cast<INDEX_T>(i));

      auto key = indexer.get_key(static_cast<INDEX_T>(i));
      EXPECT_EQ(key.type(), DataTypeId::kInt64);
      EXPECT_EQ(key.as_int64(), values[i])
          << " at index " << i << " with lid " << static_cast<INDEX_T>(i)
          << " and key " << key.as_int64();
    }
  }

  template <typename INDEX_T>
  static void ExpectStringValues(const LFIndexer<INDEX_T>& indexer,
                                 const std::vector<std::string>& values) {
    ASSERT_EQ(indexer.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      INDEX_T lid;
      ASSERT_TRUE(
          indexer.get_index(Property::from_string_view(values[i]), lid));
      EXPECT_EQ(lid, static_cast<INDEX_T>(i));

      auto key = indexer.get_key(static_cast<INDEX_T>(i));
      EXPECT_EQ(key.type(), DataTypeId::kVarchar);
      EXPECT_EQ(key.as_string_view(), values[i]);
    }
  }

  std::string test_dir_;
};

TEST_F(LFIndexerTest, SupportsCoreMutableInterfacesInMemory) {
  EXPECT_EQ(LFIndexer<uint32_t>::prefix(), "indexer");

  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  LFIndexer<uint32_t> indexer;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kInt64)));
    indexer.init(neug::DataType::INT64);
    indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }
  EXPECT_EQ(indexer.get_type(), DataTypeId::kInt64);
  EXPECT_EQ(indexer.get_keys().type(), DataTypeId::kInt64);
  EXPECT_EQ(indexer.size(), 0U);
  EXPECT_EQ(indexer.capacity(), 0U);

  indexer.reserve(8);
  EXPECT_GE(indexer.capacity(), 8U);

  std::vector<int64_t> values = {7, 11, 13, 17, 19, 23, 29, 31, 37, 41};
  EXPECT_EQ(indexer.insert(Property::from_int64(values[0]), false), 0U);
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_EQ(indexer.insert(Property::from_int64(values[i]), true),
              static_cast<uint32_t>(i));
  }

  EXPECT_EQ(indexer.size(), values.size());
  EXPECT_GE(indexer.capacity(), values.size());
  ExpectInt64Values(indexer, values);

  uint32_t lid = std::numeric_limits<uint32_t>::max();
  EXPECT_TRUE(indexer.get_index(Property::from_int64(23), lid));
  EXPECT_EQ(lid, 5U);
  EXPECT_FALSE(indexer.get_index(Property::from_int32(23), lid));
  EXPECT_TRUE(indexer.contains(Property::from_int64(37)));
  EXPECT_FALSE(indexer.contains(Property::from_int64(1001)));
  EXPECT_EQ(indexer.get_index(Property::from_int64(1001)),
            std::numeric_limits<uint32_t>::max());

  indexer.rehash(64);
  EXPECT_GE(indexer.capacity(), 64U);
  ExpectInt64Values(indexer, values);

  indexer.Close();
}

TEST_F(LFIndexerTest, DumpsAndOpensAcrossBackends) {
  const std::string name = "persisted_index";

  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  ModuleDescriptor desc;
  {
    LFIndexer<uint32_t> writable;
    {
      ModuleDescriptor fresh_desc;
      fresh_desc.module_type = "lf_indexer";
      fresh_desc.set("type",
                     std::to_string(static_cast<uint8_t>(DataTypeId::kInt64)));
      writable.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
    }

    std::vector<int64_t> values = {5, 10, 15, 20};
    for (const auto& value : values) {
      writable.insert(Property::from_int64(value), true);
    }

    desc = writable.Dump(ckp);
  }
  std::vector<int64_t> values = {5, 10, 15, 20};
  {
    LFIndexer<uint32_t> readonly;
    readonly.Open(ckp, desc, MemoryLevel::kInMemory);
    ExpectInt64Values(readonly, values);
    EXPECT_EQ(readonly.get_keys().type(), DataTypeId::kInt64);
    readonly.Close();

    // Verify that sub-module paths were recorded in the descriptor
    EXPECT_FALSE(desc.get_sub_module_or_default("keys").path.empty());
    EXPECT_TRUE(
        std::filesystem::exists(desc.get_sub_module_or_default("keys").path) ||
        std::filesystem::exists(desc.get_sub_module_or_default("keys").path +
                                ".items"));
    EXPECT_FALSE(desc.get_sub_module_or_default("indices").path.empty());
    EXPECT_TRUE(std::filesystem::exists(
        desc.get_sub_module_or_default("indices").path));
  }

  // Verify the data can also be opened using hugepages and sync-to-file modes
  {
    LFIndexer<uint32_t> hugepage_idx;
    hugepage_idx.Open(ckp, desc, MemoryLevel::kHugePagePreferred);
    ExpectInt64Values(hugepage_idx, values);
    hugepage_idx.Close();
  }
  {
    LFIndexer<uint32_t> sync_idx;
    sync_idx.Open(ckp, desc, MemoryLevel::kSyncToFile);
    ExpectInt64Values(sync_idx, values);
    sync_idx.Close();
  }
}

TEST_F(LFIndexerTest, SupportsBuildEmptySwapAndVarcharKeys) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  // Verify that an empty indexer can be initialised and dumped to disk.
  LFIndexer<uint32_t> empty_indexer;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kInt64)));
    empty_indexer.init(neug::DataType::INT64);
    empty_indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }
  ModuleDescriptor empty_dump = empty_indexer.Dump(ckp);
  EXPECT_FALSE(empty_dump.get_sub_module_or_default("indices").path.empty());
  EXPECT_TRUE(std::filesystem::exists(
      empty_dump.get_sub_module_or_default("indices").path));
  empty_indexer.Close();

  const std::string lhs_base = test_dir_ + "/lhs_varchar";
  const std::string rhs_base = test_dir_ + "/rhs_varchar";

  LFIndexer<uint32_t> lhs;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    lhs.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }
  lhs.reserve(4);

  LFIndexer<uint32_t> rhs;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    rhs.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }
  rhs.reserve(4);

  std::vector<std::string> lhs_values = {"alice", "bob"};
  std::vector<std::string> rhs_values = {"carol", "dave", "erin"};
  for (const auto& value : lhs_values) {
    lhs.insert(Property::from_string_view(value), true);
  }
  for (const auto& value : rhs_values) {
    rhs.insert(Property::from_string_view(value), true);
  }

  EXPECT_EQ(lhs.get_type(), DataTypeId::kVarchar);
  EXPECT_EQ(lhs.get_keys().type(), DataTypeId::kVarchar);
  ExpectStringValues(lhs, lhs_values);
  ExpectStringValues(rhs, rhs_values);

  lhs.swap(rhs);
  ExpectStringValues(lhs, rhs_values);
  ExpectStringValues(rhs, lhs_values);

  rhs.drop();
  lhs.Close();
}

// Corner case 1: reserve(N) then insert() (insert_safe=false) for exactly N
// varchar strings — the primary bug trigger.
TEST_F(LFIndexerTest, VarcharReserveEnablesNonSafeInsert) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  LFIndexer<uint32_t> indexer;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    indexer.init(DataTypeId::kVarchar);
    indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }

  constexpr size_t N = 8;
  indexer.reserve(N);
  EXPECT_GE(indexer.capacity(), N);

  // insert() with insert_safe=false requires capacity and data buffer space
  // to already be available — exactly what the bug fix guarantees.
  std::vector<std::string> values = {"alpha",   "beta", "gamma", "delta",
                                     "epsilon", "zeta", "eta",   "theta"};
  for (const auto& v : values) {
    indexer.insert(Property::from_string_view(v), false);
  }
  ExpectStringValues(indexer, values);
  indexer.Close();
}

// Corner case 2: reserve(N) then insert N strings each of length == max width.
// Exercises the tightest possible data buffer requirement: N * width_ bytes.
TEST_F(LFIndexerTest, VarcharReserveMaxWidthStrings) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  constexpr uint16_t kMaxWidth = 16;
  auto type_info = std::make_shared<StringTypeInfo>(kMaxWidth);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    indexer.init(string_type);
    indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }

  constexpr size_t N = 6;
  indexer.reserve(N);
  EXPECT_GE(indexer.capacity(), N);

  // Each string fills the entire declared max width (minus 1 for safety with
  // truncation — width_ is the exclusive upper bound in set_value).
  std::vector<std::string> values;
  for (size_t i = 0; i < N; ++i) {
    values.push_back(std::string(kMaxWidth - 1, static_cast<char>('a' + i)));
  }
  for (const auto& v : values) {
    indexer.insert(Property::from_string_view(v), false);
  }
  ExpectStringValues(indexer, values);
  indexer.Close();
}

// Corner case 3: two successive reserve() calls; the second must not shrink
// the data buffer (already-committed bytes at pos_ must be preserved).
TEST_F(LFIndexerTest, VarcharMultipleReservesAccumulateDataSpace) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  auto type_info = std::make_shared<StringTypeInfo>(32);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    indexer.init(string_type);
    indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }

  // First batch: reserve 4, insert 4 via insert() (non-safe).
  indexer.reserve(4);
  std::vector<std::string> batch1 = {"alice", "bob", "carol", "dave"};
  for (const auto& v : batch1) {
    indexer.insert(Property::from_string_view(v), false);
  }
  ExpectStringValues(indexer, batch1);

  // Second batch: reserve 8 more slots. At this point pos_ > 0 bytes are
  // committed; resize() must honour std::max(needed, current) so the already-
  // written string bytes are not lost.
  indexer.reserve(8);
  std::vector<std::string> batch2 = {"erin", "frank", "grace", "heidi"};
  for (const auto& v : batch2) {
    indexer.insert(Property::from_string_view(v), false);
  }

  std::vector<std::string> all = {"alice", "bob",   "carol", "dave",
                                  "erin",  "frank", "grace", "heidi"};
  ExpectStringValues(indexer, all);
  indexer.Close();
}

// Corner case 4: reserve() with a count smaller than current size must be
// a no-op (items_ and data_ must not shrink, existing data must be readable).
TEST_F(LFIndexerTest, VarcharReserveSmallerThanCapacityIsNoop) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  auto type_info = std::make_shared<StringTypeInfo>(32);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    indexer.init(DataTypeId::kVarchar);
    indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }

  indexer.reserve(16);
  EXPECT_GE(indexer.capacity(), 16U);
  size_t size_before = indexer.size();
  indexer.insert(Property::from_string_view("foo"), false);
  indexer.insert(Property::from_string_view("bar"), false);
  indexer.insert(Property::from_string_view("baz"), false);
  indexer.insert(Property::from_string_view("qux"), false);

  // Shrinking reserve must not corrupt state.
  indexer.reserve(4);
  EXPECT_GE(indexer.capacity(), size_before);
  indexer.Close();
}

// Corner case 5: rehash() after inserting varchar strings calls
// keys_->resize() internally; verify all lookups remain correct.
TEST_F(LFIndexerTest, VarcharRehashPreservesData) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  {
    ModuleDescriptor fresh_desc;
    fresh_desc.module_type = "lf_indexer";
    fresh_desc.set("type",
                   std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
    indexer.init(string_type);
    indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  }

  std::vector<std::string> values = {"foo",  "bar",   "baz",   "qux",
                                     "quux", "corge", "grault"};
  for (const auto& v : values) {
    indexer.insert(Property::from_string_view(v), true);
  }
  ExpectStringValues(indexer, values);

  // rehash() calls keys_->resize() again internally; data buffer must stay
  // valid.
  indexer.rehash(64);
  EXPECT_GE(indexer.capacity(), 64U);
  ExpectStringValues(indexer, values);
  indexer.Close();
}

// Corner case 6: dump and reload a varchar LFIndexer populated via
// reserve()+insert() (non-safe) to confirm persistence is not affected.
TEST_F(LFIndexerTest, VarcharReserveInsertDumpReload) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  DataType string_type(DataTypeId::kVarchar, type_info);

  LFIndexer<uint32_t> writable;
  ModuleDescriptor fresh_desc;
  fresh_desc.module_type = "lf_indexer";
  fresh_desc.set("type",
                 std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
  writable.init(string_type);
  writable.Open(ckp, fresh_desc, MemoryLevel::kInMemory);

  constexpr size_t N = 5;
  writable.reserve(N);

  std::vector<std::string> values = {"one", "two", "three", "four", "five"};
  for (const auto& v : values) {
    writable.insert(Property::from_string_view(v), true);
  }
  ExpectStringValues(writable, values);

  ModuleDescriptor dump_desc = writable.Dump(ckp);

  // Reload from descriptor and verify all entries survive the round-trip.
  LFIndexer<uint32_t> reader;
  reader.init(string_type);
  reader.Open(ckp, dump_desc, MemoryLevel::kInMemory);
  ExpectStringValues(reader, values);
  reader.Close();
}

// ---- Dump-with-short-strings → reopen → insert-long-strings ----
//
// After a dump+reopen the .data file contains only the compacted actual bytes
// (pos_ bytes, which is small when all inserted strings are short).
// data_buffer_->GetDataSize() therefore equals pos_ — a tight allocation.
//
// Without the fix, any subsequent resize() for new slots computed:
//   needed = pos_ + new_items * width_
// but never called data_buffer_->Resize(), leaving no room for long values.

// Path A: Open + explicit reserve() + insert() (non-safe)
TEST_F(LFIndexerTest, VarcharShortDumpReopenReserveThenInsertLong_InMemory) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  constexpr uint16_t kWidth = 64;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);
  ModuleDescriptor dump_desc;

  // Phase 1: populate with short strings (avg 3 chars << kWidth), then dump.
  std::vector<std::string> short_values = {"a", "bb", "ccc"};
  {
    LFIndexer<uint32_t> writer;
    {
      ModuleDescriptor fresh_desc;
      fresh_desc.module_type = "lf_indexer";
      fresh_desc.set(
          "type", std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
      writer.init(string_type);
      writer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
    }
    for (const auto& v : short_values) {
      writer.insert(Property::from_string_view(v), true);
    }
    dump_desc = writer.Dump(ckp);
  }

  // Phase 2: reopen — data_buffer_ is now tight (= sum of short string bytes).
  LFIndexer<uint32_t> indexer;
  indexer.Open(ckp, dump_desc, MemoryLevel::kInMemory);
  ExpectStringValues(indexer, short_values);

  // reserve() must expand data_buffer_ to accommodate new_items * kWidth bytes
  // on top of the existing pos_ — exercising the exact bug-fix path.
  constexpr size_t kExtra = 4;
  indexer.reserve(short_values.size() + kExtra);
  EXPECT_GE(indexer.capacity(), short_values.size() + kExtra);

  // 60-char strings — well above the 3-char average of the dump phase.
  std::vector<std::string> long_values;
  for (size_t i = 0; i < kExtra; ++i) {
    long_values.push_back(std::string(60, static_cast<char>('d' + i)));
  }
  for (const auto& v : long_values) {
    indexer.insert(Property::from_string_view(v), true);
  }

  std::vector<std::string> all = short_values;
  all.insert(all.end(), long_values.begin(), long_values.end());
  ExpectStringValues(indexer, all);
  indexer.Close();
}

// Path B: Open + insert(true) triggers auto-resize internally.
// No explicit reserve — capacity is exhausted and auto-grows via
// reserve(cap + cap/4) inside insert(..., true).
TEST_F(LFIndexerTest, VarcharShortDumpReopenInsertSafeLong_InMemory) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  constexpr uint16_t kWidth = 48;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);
  ModuleDescriptor dump_desc;

  // Phase 1: fill to capacity with 1-char strings, then dump.
  std::vector<std::string> short_values = {"x", "y", "z", "w"};
  {
    LFIndexer<uint32_t> writer;
    {
      ModuleDescriptor fresh_desc;
      fresh_desc.module_type = "lf_indexer";
      fresh_desc.set(
          "type", std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
      writer.init(string_type);
      writer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
    }
    writer.reserve(short_values.size());
    for (const auto& v : short_values) {
      writer.insert(Property::from_string_view(v), false);
    }
    dump_desc = writer.Dump(ckp);
    writer.Close();
  }

  // Phase 2: reopen — capacity == short_values.size(), data_buffer_ tight.
  LFIndexer<uint32_t> indexer;
  indexer.init(string_type);
  indexer.Open(ckp, dump_desc, MemoryLevel::kInMemory);
  EXPECT_EQ(indexer.size(), short_values.size());

  // insert(true) with long strings: the first call hits ind >= capacity(),
  // triggering reserve(cap + cap/4) which calls resize() on the tight buffer.
  std::vector<std::string> long_values;
  for (size_t i = 0; i < 5; ++i) {
    long_values.push_back(std::string(45, static_cast<char>('A' + i)));
  }
  for (const auto& v : long_values) {
    indexer.insert(Property::from_string_view(v), true);
  }

  std::vector<std::string> all = short_values;
  all.insert(all.end(), long_values.begin(), long_values.end());
  ExpectStringValues(indexer, all);
  indexer.Close();
}

// Path C: Open (SyncToFile backend) + explicit reserve() + insert() long.
// Validates that the SyncToFile container also gets its data_buffer_ extended
// correctly after a short-string dump.
TEST_F(LFIndexerTest, VarcharShortDumpReopenReserveThenInsertLong_SyncToFile) {
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  constexpr uint16_t kWidth = 32;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);
  ModuleDescriptor dump_desc;

  // Phase 1: insert 2-char strings to keep data compact, then dump.
  std::vector<std::string> short_values = {"hi", "yo", "ok"};
  {
    LFIndexer<uint32_t> writer;
    {
      ModuleDescriptor fresh_desc;
      fresh_desc.module_type = "lf_indexer";
      fresh_desc.set(
          "type", std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
      writer.init(string_type);
      writer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
    }
    for (const auto& v : short_values) {
      writer.insert(Property::from_string_view(v), true);
    }
    dump_desc = writer.Dump(ckp);
    writer.Close();
  }

  // Phase 2: reopen via SyncToFile — data_buffer_ memory-maps the small dump.
  LFIndexer<uint32_t> indexer;
  indexer.init(string_type);
  indexer.Open(ckp, dump_desc, MemoryLevel::kSyncToFile);
  ExpectStringValues(indexer, short_values);

  constexpr size_t kExtra = 3;
  indexer.reserve(short_values.size() + kExtra);
  EXPECT_GE(indexer.capacity(), short_values.size() + kExtra);

  // 30-char strings (close to kWidth), much longer than the original 2-char
  // average.
  std::vector<std::string> long_values;
  for (size_t i = 0; i < kExtra; ++i) {
    long_values.push_back(std::string(30, static_cast<char>('p' + i)));
  }
  for (const auto& v : long_values) {
    indexer.insert(Property::from_string_view(v), true);
  }

  std::vector<std::string> all = short_values;
  all.insert(all.end(), long_values.begin(), long_values.end());
  ExpectStringValues(indexer, all);
  indexer.Close();
}

// Test: String overflow handling when inserting strings exceeding max length.
TEST_F(LFIndexerTest, VarcharStringOverflow) {
  const std::string base = test_dir_ + "/varchar_string_overflow";
  Workspace ws;
  ws.Open(test_dir_);
  auto ckp_id = ws.CreateCheckpoint();
  auto& ckp = ws.GetCheckpoint(ckp_id);

  // Create indexer with max string length of 32
  auto type_info = std::make_shared<StringTypeInfo>(32);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  ModuleDescriptor fresh_desc;
  fresh_desc.module_type = "lf_indexer";
  fresh_desc.set("type",
                 std::to_string(static_cast<uint8_t>(DataTypeId::kVarchar)));
  indexer.init(string_type);
  indexer.Open(ckp, fresh_desc, MemoryLevel::kInMemory);
  indexer.reserve(4);
  // Insert valid strings within the limit
  std::vector<std::string> valid_strings = {
      "short",                 // 5 chars
      "medium_length_string",  // 21 chars
      std::string(32, 'a'),    // exactly 32 chars (max length)
      "boundary"               // 8 chars, to test boundary condition
  };

  for (const auto& v : valid_strings) {
    indexer.insert(Property::from_string_view(v), false);
  }
  ExpectStringValues(indexer, valid_strings);
  indexer.reserve(8);

  // Test: Insert additional strings of 32 characters
  std::string overflow_string = std::string(31, 'a');  // 31 chars
  for (size_t i = 0; i < 2; ++i) {
    std::string test_string =
        overflow_string + std::to_string(i);  // 31 chars + 1 char = 32 chars
    indexer.insert(Property::from_string_view(test_string), false);
    valid_strings.push_back(test_string);
  }
  ExpectStringValues(indexer, valid_strings);

  EXPECT_THROW(
      indexer.insert(Property::from_string_view(overflow_string), false),
      neug::exception::StorageException);

  indexer.Close();
}

}  // namespace neug
