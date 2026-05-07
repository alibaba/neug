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
#include "neug/storages/file_names.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/property.h"

namespace neug {
namespace {

class LFIndexerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ =
        "/tmp/lf_indexer_test_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    snapshot_dir_ = test_dir_ + "/snapshot";
    checkpoint_dir_ = test_dir_ + "/checkpoint";
    work_dir_ = test_dir_ + "/work";
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::create_directories(snapshot_dir_);
    std::filesystem::create_directories(checkpoint_dir_);
    std::filesystem::create_directories(tmp_dir(work_dir_));
  }

  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  static void CreateEmptyIndicesFile(const std::string& base_path) {
    std::filesystem::create_directories(
        std::filesystem::path(base_path).parent_path());
    FileHeader header{};
    std::ofstream fout(base_path + ".indices", std::ios::binary);
    fout.write(reinterpret_cast<const char*>(&header), sizeof(header));
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
  std::string snapshot_dir_;
  std::string checkpoint_dir_;
  std::string work_dir_;
};

TEST_F(LFIndexerTest, SupportsCoreMutableInterfacesInMemory) {
  const std::string base_path = test_dir_ + "/core_index";
  CreateEmptyIndicesFile(base_path);

  LFIndexer<uint32_t> indexer;
  EXPECT_EQ(LFIndexer<uint32_t>::prefix(), "indexer");

  indexer.init(DataTypeId::kInt64);
  EXPECT_EQ(indexer.get_type(), DataTypeId::kInt64);
  EXPECT_EQ(indexer.get_keys().type(), DataTypeId::kInt64);

  indexer.open_in_memory(base_path);
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

  indexer.close();
}

TEST_F(LFIndexerTest, DumpsAndOpensAcrossBackends) {
  const std::string name = "persisted_index";
  const std::string in_memory_base = test_dir_ + "/persisted_seed";
  CreateEmptyIndicesFile(in_memory_base);

  LFIndexer<uint32_t> writable;
  writable.init(DataTypeId::kInt64);
  writable.open_in_memory(in_memory_base);

  std::vector<int64_t> values = {5, 10, 15, 20};
  for (const auto& value : values) {
    writable.insert(Property::from_int64(value), true);
  }

  writable.dump(name, snapshot_dir_);
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".meta"));
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".keys"));
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".indices"));

  LFIndexer<uint32_t> copied_to_workdir;
  copied_to_workdir.init(DataTypeId::kInt64);
  copied_to_workdir.open(name, snapshot_dir_, work_dir_);
  ExpectInt64Values(copied_to_workdir, values);
  EXPECT_TRUE(
      std::filesystem::exists(tmp_dir(work_dir_) + "/" + name + ".keys"));
  EXPECT_TRUE(
      std::filesystem::exists(tmp_dir(work_dir_) + "/" + name + ".indices"));
  copied_to_workdir.drop();

  LFIndexer<uint32_t> in_memory;
  in_memory.init(DataTypeId::kInt64);
  in_memory.open_in_memory(snapshot_dir_ + "/" + name);
  ExpectInt64Values(in_memory, values);
  in_memory.close();

  LFIndexer<uint32_t> hugepage_indexer;
  hugepage_indexer.init(DataTypeId::kInt64);
  hugepage_indexer.open_with_hugepages(snapshot_dir_ + "/" + name);
  ExpectInt64Values(hugepage_indexer, values);
  hugepage_indexer.close();
}

TEST_F(LFIndexerTest, SupportsBuildEmptySwapAndVarcharKeys) {
  const std::string empty_name = "empty_index";
  const std::string empty_dir = test_dir_ + "/empty_dir";
  std::filesystem::create_directories(empty_dir);
  CreateEmptyIndicesFile(empty_dir + "/" + empty_name);

  LFIndexer<uint32_t> empty_indexer;
  empty_indexer.init(DataTypeId::kInt64);
  empty_indexer.build_empty_LFIndexer(empty_name, "", empty_dir);
  EXPECT_TRUE(std::filesystem::exists(empty_dir + "/" + empty_name + ".meta"));
  EXPECT_TRUE(
      std::filesystem::exists(empty_dir + "/" + empty_name + ".indices"));

  const std::string lhs_base = test_dir_ + "/lhs_varchar";
  const std::string rhs_base = test_dir_ + "/rhs_varchar";
  CreateEmptyIndicesFile(lhs_base);
  CreateEmptyIndicesFile(rhs_base);

  auto string_type_info = std::make_shared<StringTypeInfo>(64);
  LFIndexer<uint32_t> lhs;
  DataType string_type(DataTypeId::kVarchar, string_type_info);
  lhs.init(string_type);
  lhs.open_in_memory(lhs_base);
  lhs.reserve(4);

  LFIndexer<uint32_t> rhs;
  rhs.init(string_type);
  rhs.open_in_memory(rhs_base);
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
  lhs.close();
}

// ---- Tests for the reserve()-then-insert() bug fix on varchar keys ----
//
// Bug: StringColumn::resize() allocated items_buffer_ (the offset/length
//      array) but left data_buffer_ (the raw string bytes) at size 0.
//      After reserve(N), calling insert() with insert_safe=false invokes
//      set_value(), which checks:
//        pos_.load() + len <= data_buffer_->GetDataSize()
//      and threw "not enough space in buffer" because GetDataSize() was 0.
//
// Fix: resize() now pre-allocates data_buffer_ for new_items * width_ bytes,
//      using std::max(needed, current) so previously committed bytes are
//      never discarded.

// Corner case 1: reserve(N) then insert() (insert_safe=false) for exactly N
// varchar strings — the primary bug trigger.
TEST_F(LFIndexerTest, VarcharReserveEnablesNonSafeInsert) {
  const std::string base = test_dir_ + "/varchar_reserve_non_safe";
  CreateEmptyIndicesFile(base);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  indexer.init(string_type);
  indexer.open_in_memory(base);

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
  indexer.close();
}

// Corner case 2: reserve(N) then insert N strings each of length == max width.
// Exercises the tightest possible data buffer requirement: N * width_ bytes.
TEST_F(LFIndexerTest, VarcharReserveMaxWidthStrings) {
  const std::string base = test_dir_ + "/varchar_max_width";
  CreateEmptyIndicesFile(base);

  constexpr uint16_t kMaxWidth = 16;
  auto type_info = std::make_shared<StringTypeInfo>(kMaxWidth);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  indexer.init(string_type);
  indexer.open_in_memory(base);

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
  indexer.close();
}

// Corner case 3: two successive reserve() calls; the second must not shrink
// the data buffer (already-committed bytes at pos_ must be preserved).
TEST_F(LFIndexerTest, VarcharMultipleReservesAccumulateDataSpace) {
  const std::string base = test_dir_ + "/varchar_multi_reserve";
  CreateEmptyIndicesFile(base);

  auto type_info = std::make_shared<StringTypeInfo>(32);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  indexer.init(string_type);
  indexer.open_in_memory(base);

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
  indexer.close();
}

// Corner case 4: reserve() with a count smaller than current size must be
// a no-op (items_ and data_ must not shrink, existing data must be readable).
TEST_F(LFIndexerTest, VarcharReserveSmallerThanCapacityIsNoop) {
  const std::string base = test_dir_ + "/varchar_reserve_noop";
  CreateEmptyIndicesFile(base);

  auto type_info = std::make_shared<StringTypeInfo>(32);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  indexer.init(string_type);
  indexer.open_in_memory(base);

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
  indexer.close();
}

// Corner case 5: rehash() after inserting varchar strings calls
// keys_->resize() internally; verify all lookups remain correct.
TEST_F(LFIndexerTest, VarcharRehashPreservesData) {
  const std::string base = test_dir_ + "/varchar_rehash";
  CreateEmptyIndicesFile(base);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  indexer.init(string_type);
  indexer.open_in_memory(base);

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
  indexer.close();
}

// Corner case 6: dump and reload a varchar LFIndexer populated via
// reserve()+insert() (non-safe) to confirm persistence is not affected.
TEST_F(LFIndexerTest, VarcharReserveInsertDumpReload) {
  const std::string name = "varchar_persisted";
  const std::string base = test_dir_ + "/varchar_persisted_seed";
  CreateEmptyIndicesFile(base);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> writable;
  writable.init(string_type);
  writable.open_in_memory(base);

  constexpr size_t N = 5;
  writable.reserve(N);

  std::vector<std::string> values = {"one", "two", "three", "four", "five"};
  for (const auto& v : values) {
    writable.insert(Property::from_string_view(v), false);
  }
  ExpectStringValues(writable, values);

  writable.dump(name, snapshot_dir_);
  EXPECT_TRUE(
      std::filesystem::exists(snapshot_dir_ + "/" + name + ".keys.items"));
  EXPECT_TRUE(
      std::filesystem::exists(snapshot_dir_ + "/" + name + ".keys.data"));

  // Reload from snapshot and verify all entries survive the round-trip.
  LFIndexer<uint32_t> reader;
  reader.init(string_type);
  reader.open_in_memory(snapshot_dir_ + "/" + name);
  ExpectStringValues(reader, values);
  reader.close();
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

// Path A: open_in_memory + explicit reserve() + insert() (non-safe)
TEST_F(LFIndexerTest, VarcharShortDumpReopenReserveThenInsertLong_InMemory) {
  const std::string name = "short_to_long_inmem";
  const std::string base = test_dir_ + "/short_to_long_seed_inmem";
  CreateEmptyIndicesFile(base);

  constexpr uint16_t kWidth = 64;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);

  // Phase 1: populate with short strings (avg 3 chars << kWidth), then dump.
  std::vector<std::string> short_values = {"a", "bb", "ccc"};
  {
    LFIndexer<uint32_t> writer;
    writer.init(string_type);
    writer.open_in_memory(base);
    for (const auto& v : short_values) {
      writer.insert(Property::from_string_view(v), true);
    }
    writer.dump(name, snapshot_dir_);
  }

  // Phase 2: reopen — data_buffer_ is now tight (= sum of short string bytes).
  LFIndexer<uint32_t> indexer;
  indexer.init(string_type);
  indexer.open_in_memory(snapshot_dir_ + "/" + name);
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
  indexer.close();
}

// Path B: open_in_memory + insert(true) triggers auto-resize internally.
// No explicit reserve — capacity is exhausted and auto-grows via
// reserve(cap + cap/4) inside insert(..., true).
TEST_F(LFIndexerTest, VarcharShortDumpReopenInsertSafeLong_InMemory) {
  const std::string name = "short_to_long_safe_inmem";
  const std::string base = test_dir_ + "/short_to_long_seed_safe_inmem";
  CreateEmptyIndicesFile(base);

  constexpr uint16_t kWidth = 48;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);

  // Phase 1: fill to capacity with 1-char strings, then dump.
  std::vector<std::string> short_values = {"x", "y", "z", "w"};
  {
    LFIndexer<uint32_t> writer;
    writer.init(string_type);
    writer.open_in_memory(base);
    writer.reserve(short_values.size());
    for (const auto& v : short_values) {
      writer.insert(Property::from_string_view(v), false);
    }
    writer.dump(name, snapshot_dir_);
  }

  // Phase 2: reopen — capacity == short_values.size(), data_buffer_ tight.
  LFIndexer<uint32_t> indexer;
  indexer.init(string_type);
  indexer.open_in_memory(snapshot_dir_ + "/" + name);
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
  indexer.close();
}

// Path C: open() (SyncToFile backend) + explicit reserve() + insert() long.
// Validates that the SyncToFile container also gets its data_buffer_ extended
// correctly after a short-string dump.
TEST_F(LFIndexerTest, VarcharShortDumpReopenReserveThenInsertLong_SyncToFile) {
  const std::string name = "short_to_long_sync";
  const std::string base = test_dir_ + "/short_to_long_seed_sync";
  CreateEmptyIndicesFile(base);

  constexpr uint16_t kWidth = 32;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);

  // Phase 1: insert 2-char strings to keep .data file tiny, then dump.
  std::vector<std::string> short_values = {"hi", "yo", "ok"};
  {
    LFIndexer<uint32_t> writer;
    writer.init(string_type);
    writer.open_in_memory(base);
    for (const auto& v : short_values) {
      writer.insert(Property::from_string_view(v), true);
    }
    writer.dump(name, snapshot_dir_);
  }

  // Phase 2: reopen via SyncToFile — data_buffer_ memory-maps the small file.
  LFIndexer<uint32_t> indexer;
  indexer.init(string_type);
  indexer.open(name, snapshot_dir_, work_dir_);
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
  indexer.drop();
}

// Test: String overflow handling when inserting strings exceeding max length.
TEST_F(LFIndexerTest, VarcharStringOverflow) {
  const std::string base = test_dir_ + "/varchar_string_overflow";
  CreateEmptyIndicesFile(base);

  // Create indexer with max string length of 32
  auto type_info = std::make_shared<StringTypeInfo>(32);
  LFIndexer<uint32_t> indexer;
  DataType string_type(DataTypeId::kVarchar, type_info);
  indexer.init(string_type);
  indexer.open_in_memory(base);
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

  indexer.close();
}

}  // namespace
}  // namespace neug
