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

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/common/types/value.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/module/module_factory.h"
#include "neug/storages/module/type_name.h"
#include "neug/utils/id_indexer.h"
#include "unittest/utils.h"

namespace neug {

// OpenIndexerLegacy / DumpIndexerLegacy live in unittest/utils.h so multiple
// test binaries (test_indexer, test_edge_table, ...) can share them.

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
      ASSERT_TRUE(indexer.get_index(neug::Value::INT64(values[i]), lid));
      EXPECT_EQ(lid, static_cast<INDEX_T>(i));

      const auto& key = indexer.get_key(static_cast<INDEX_T>(i));
      EXPECT_EQ(key.type().id(), DataTypeId::kInt64);
      EXPECT_EQ(key.template GetValue<int64_t>(), values[i])
          << " at index " << i << " with lid " << static_cast<INDEX_T>(i)
          << " and key " << key.template GetValue<int64_t>();
    }
  }

  template <typename INDEX_T>
  static void ExpectStringValues(const LFIndexer<INDEX_T>& indexer,
                                 const std::vector<std::string>& values) {
    ASSERT_EQ(indexer.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      INDEX_T lid;
      ASSERT_TRUE(indexer.get_index(neug::Value::STRING(values[i]), lid));
      EXPECT_EQ(lid, static_cast<INDEX_T>(i));

      const auto& key = indexer.get_key(static_cast<INDEX_T>(i));
      EXPECT_EQ(key.type().id(), DataTypeId::kVarchar);
      EXPECT_EQ(key.template GetValue<std::string>(), values[i]);
    }
  }

  std::string test_dir_;
};

TEST_F(LFIndexerTest, SupportsCoreMutableInterfacesInMemory) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, DataType(DataTypeId::kInt64),
                    CheckpointManifest(), MemoryLevel::kInMemory);

  EXPECT_EQ(indexer.get_type(), DataTypeId::kInt64);
  EXPECT_EQ(indexer.get_keys().type(), DataTypeId::kInt64);
  EXPECT_EQ(indexer.size(), 0U);
  EXPECT_EQ(indexer.capacity(), 0U);

  indexer.reserve(8);
  EXPECT_GE(indexer.capacity(), 8U);

  std::vector<int64_t> values = {7, 11, 13, 17, 19, 23, 29, 31, 37, 41};
  EXPECT_EQ(indexer.insert(neug::Value::INT64(values[0]), false), 0U);
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_EQ(indexer.insert(neug::Value::INT64(values[i]), true),
              static_cast<uint32_t>(i));
  }

  EXPECT_EQ(indexer.size(), values.size());
  EXPECT_GE(indexer.capacity(), values.size());
  ExpectInt64Values(indexer, values);

  uint32_t lid = std::numeric_limits<uint32_t>::max();
  EXPECT_TRUE(indexer.get_index(neug::Value::INT64(23), lid));
  EXPECT_EQ(lid, 5U);
  EXPECT_FALSE(indexer.get_index(neug::Value::INT32(23), lid));
  EXPECT_TRUE(indexer.contains(neug::Value::INT64(37)));
  EXPECT_FALSE(indexer.contains(neug::Value::INT64(1001)));
  EXPECT_EQ(indexer.get_index(neug::Value::INT64(1001)),
            std::numeric_limits<uint32_t>::max());

  indexer.rehash(64);
  EXPECT_GE(indexer.capacity(), 64U);
  ExpectInt64Values(indexer, values);

  indexer.Close();
}

TEST_F(LFIndexerTest, RejectsMismatchedPrimaryKeyTypeOnInsert) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, DataType(DataTypeId::kInt64),
                    CheckpointManifest(), MemoryLevel::kInMemory);
  indexer.reserve(1);

  EXPECT_THROW(indexer.insert(neug::Value::INT32(7), false),
               neug::exception::StorageException);
  indexer.Close();
}

TEST_F(LFIndexerTest, BatchInt64LookupAndInsertHandleNullsAndDuplicates) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, DataType(DataTypeId::kInt64),
                    CheckpointManifest(), MemoryLevel::kInMemory);
  ValueColumnBuilder<int64_t> insert_builder;
  insert_builder.push_back_opt(7);
  insert_builder.push_back_opt(11);
  insert_builder.push_back_opt(7);
  auto insert_keys = insert_builder.finish();

  std::vector<uint32_t> vids;
  std::vector<uint8_t> inserted;
  indexer.get_or_insert(*insert_keys, vids, inserted);
  EXPECT_EQ(vids, (std::vector<uint32_t>{0, 1, 0}));
  EXPECT_EQ(inserted, (std::vector<uint8_t>{1, 1, 0}));

  ValueColumnBuilder<int64_t> lookup_builder;
  lookup_builder.push_back_opt(11);
  lookup_builder.push_back_opt(99);
  auto lookup_keys = lookup_builder.finish();
  vids.clear();
  indexer.get_index(*lookup_keys, vids);
  EXPECT_EQ(vids,
            (std::vector<uint32_t>{1, std::numeric_limits<uint32_t>::max()}));

  ValueColumnBuilder<int64_t> optional_builder(true);
  optional_builder.push_back_opt(7);
  optional_builder.push_back_null();
  auto optional_keys = optional_builder.finish();
  vids.clear();
  EXPECT_THROW(indexer.get_index(*optional_keys, vids),
               neug::exception::NotSupportedException);

  indexer.Close();
}

TEST_F(LFIndexerTest, BatchVarcharLookupAndInsertUseStringColumn) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);
  ValueColumnBuilder<std::string> builder;
  builder.push_back_opt("alice");
  builder.push_back_opt("bob");
  builder.push_back_opt("alice");
  auto keys = builder.finish();

  std::vector<uint32_t> vids;
  std::vector<uint8_t> inserted;
  indexer.get_or_insert(*keys, vids, inserted);
  EXPECT_EQ(vids, (std::vector<uint32_t>{0, 1, 0}));
  EXPECT_EQ(inserted, (std::vector<uint8_t>{1, 1, 0}));

  vids.clear();
  indexer.get_index(*keys, vids);
  EXPECT_EQ(vids, (std::vector<uint32_t>{0, 1, 0}));

  // Varchar batches can also arrive as string_view columns. Keep the backing
  // strings alive while the context column is consumed by the indexer.
  std::vector<std::string> backing = {"alice", "carol", "alice"};
  ValueColumnBuilder<std::string> view_builder;
  for (const auto& value : backing) {
    view_builder.push_back_opt(value);
  }
  auto view_keys = view_builder.finish();
  vids.clear();
  inserted.clear();
  indexer.get_or_insert(*view_keys, vids, inserted);
  EXPECT_EQ(vids, (std::vector<uint32_t>{0, 2, 0}));
  EXPECT_EQ(inserted, (std::vector<uint8_t>{0, 1, 0}));

  indexer.Close();
}

TEST_F(LFIndexerTest, DumpsAndOpensAcrossBackends) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  std::vector<int64_t> values = {5, 10, 15, 20};
  DataType int64_type(DataTypeId::kInt64);
  CheckpointManifest desc;
  {
    LFIndexer<uint32_t> writable;
    OpenIndexerLegacy(writable, *ckp, int64_type, CheckpointManifest(),
                      MemoryLevel::kInMemory);
    for (const auto& value : values) {
      writable.insert(neug::Value::INT64(value), true);
    }
    desc = DumpIndexerLegacy(writable, *ckp);
  }
  {
    LFIndexer<uint32_t> readonly;
    OpenIndexerLegacy(readonly, *ckp, int64_type, desc, MemoryLevel::kInMemory);
    ExpectInt64Values(readonly, values);
    EXPECT_EQ(readonly.get_keys().type(), DataTypeId::kInt64);
    readonly.Close();

    // The dumped meta should advertise the two indexer leaves.
    EXPECT_TRUE(desc.has_module(kIndexerKeys));
    EXPECT_TRUE(desc.has_module(kIndexerIndices));
  }

  {
    LFIndexer<uint32_t> hugepage_idx;
    OpenIndexerLegacy(hugepage_idx, *ckp, int64_type, desc,
                      MemoryLevel::kHugePagePreferred);
    ExpectInt64Values(hugepage_idx, values);
    hugepage_idx.Close();
  }
  {
    LFIndexer<uint32_t> sync_idx;
    OpenIndexerLegacy(sync_idx, *ckp, int64_type, desc,
                      MemoryLevel::kSyncToFile);
    ExpectInt64Values(sync_idx, values);
    sync_idx.Close();
  }
}

TEST_F(LFIndexerTest, SupportsBuildEmptySwapAndVarcharKeys) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  DataType int64_type(DataTypeId::kInt64);
  LFIndexer<uint32_t> empty_indexer;
  OpenIndexerLegacy(empty_indexer, *ckp, int64_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);
  EXPECT_EQ(empty_indexer.get_index(neug::Value::INT64(42)),
            std::numeric_limits<uint32_t>::max());
  CheckpointManifest empty_dump = DumpIndexerLegacy(empty_indexer, *ckp);
  EXPECT_TRUE(empty_dump.has_module(kIndexerIndices));
  empty_indexer.Close();

  DataType varchar_type(DataTypeId::kVarchar);

  LFIndexer<uint32_t> lhs;
  OpenIndexerLegacy(lhs, *ckp, varchar_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);
  lhs.reserve(4);

  LFIndexer<uint32_t> rhs;
  OpenIndexerLegacy(rhs, *ckp, varchar_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);
  rhs.reserve(4);

  std::vector<std::string> lhs_values = {"alice", "bob"};
  std::vector<std::string> rhs_values = {"carol", "dave", "erin"};
  for (const auto& value : lhs_values) {
    lhs.insert(neug::Value::STRING(value), true);
  }
  for (const auto& value : rhs_values) {
    rhs.insert(neug::Value::STRING(value), true);
  }

  EXPECT_EQ(lhs.get_type(), DataTypeId::kVarchar);
  EXPECT_EQ(lhs.get_keys().type(), DataTypeId::kVarchar);
  ExpectStringValues(lhs, lhs_values);
  ExpectStringValues(rhs, rhs_values);

  lhs.swap(rhs);
  ExpectStringValues(lhs, rhs_values);
  ExpectStringValues(rhs, lhs_values);

  rhs.Close();
  lhs.Close();
}

TEST_F(LFIndexerTest, VarcharReserveEnablesNonSafeInsert) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);

  constexpr size_t N = 8;
  indexer.reserve(N);
  EXPECT_GE(indexer.capacity(), N);

  std::vector<std::string> values = {"alpha",   "beta", "gamma", "delta",
                                     "epsilon", "zeta", "eta",   "theta"};
  for (const auto& v : values) {
    indexer.insert(neug::Value::STRING(v), false);
  }
  ExpectStringValues(indexer, values);
  indexer.Close();
}

TEST_F(LFIndexerTest, VarcharReserveMaxWidthStrings) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  constexpr uint16_t kMaxWidth = 16;
  auto type_info = std::make_shared<StringTypeInfo>(kMaxWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);

  constexpr size_t N = 6;
  indexer.reserve(N);
  EXPECT_GE(indexer.capacity(), N);

  std::vector<std::string> values;
  for (size_t i = 0; i < N; ++i) {
    values.push_back(std::string(kMaxWidth - 1, static_cast<char>('a' + i)));
  }
  for (const auto& v : values) {
    indexer.insert(neug::Value::STRING(v), false);
  }
  ExpectStringValues(indexer, values);
  indexer.Close();
}

TEST_F(LFIndexerTest, VarcharMultipleReservesAccumulateDataSpace) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  auto type_info = std::make_shared<StringTypeInfo>(32);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);

  indexer.reserve(4);
  std::vector<std::string> batch1 = {"alice", "bob", "carol", "dave"};
  for (const auto& v : batch1) {
    indexer.insert(neug::Value::STRING(v), false);
  }
  ExpectStringValues(indexer, batch1);

  indexer.reserve(8);
  std::vector<std::string> batch2 = {"erin", "frank", "grace", "heidi"};
  for (const auto& v : batch2) {
    indexer.insert(neug::Value::STRING(v), false);
  }

  std::vector<std::string> all = {"alice", "bob",   "carol", "dave",
                                  "erin",  "frank", "grace", "heidi"};
  ExpectStringValues(indexer, all);
  indexer.Close();
}

TEST_F(LFIndexerTest, VarcharReserveSmallerThanCapacityIsNoop) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  auto type_info = std::make_shared<StringTypeInfo>(32);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);

  indexer.reserve(16);
  EXPECT_GE(indexer.capacity(), 16U);
  size_t size_before = indexer.size();
  indexer.insert(neug::Value::STRING(std::string("foo")), false);
  indexer.insert(neug::Value::STRING(std::string("bar")), false);
  indexer.insert(neug::Value::STRING(std::string("baz")), false);
  indexer.insert(neug::Value::STRING(std::string("qux")), false);

  indexer.reserve(4);
  EXPECT_GE(indexer.capacity(), size_before);
  indexer.Close();
}

TEST_F(LFIndexerTest, VarcharRehashPreservesData) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);

  std::vector<std::string> values = {"foo",  "bar",   "baz",   "qux",
                                     "quux", "corge", "grault"};
  for (const auto& v : values) {
    indexer.insert(neug::Value::STRING(v), true);
  }
  ExpectStringValues(indexer, values);

  indexer.rehash(64);
  EXPECT_GE(indexer.capacity(), 64U);
  ExpectStringValues(indexer, values);
  indexer.Close();
}

TEST_F(LFIndexerTest, VarcharReserveInsertDumpReload) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  auto type_info = std::make_shared<StringTypeInfo>(64);
  DataType string_type(DataTypeId::kVarchar, type_info);

  LFIndexer<uint32_t> writable;
  OpenIndexerLegacy(writable, *ckp, string_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);

  constexpr size_t N = 5;
  writable.reserve(N);

  std::vector<std::string> values = {"one", "two", "three", "four", "five"};
  for (const auto& v : values) {
    writable.insert(neug::Value::STRING(v), false);
  }
  ExpectStringValues(writable, values);

  CheckpointManifest dump_desc = DumpIndexerLegacy(writable, *ckp);

  LFIndexer<uint32_t> reader;
  OpenIndexerLegacy(reader, *ckp, string_type, dump_desc,
                    MemoryLevel::kInMemory);
  ExpectStringValues(reader, values);
  reader.Close();
}

TEST_F(LFIndexerTest, VarcharShortDumpReopenReserveThenInsertLong_InMemory) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  constexpr uint16_t kWidth = 64;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);
  CheckpointManifest dump_desc;

  std::vector<std::string> short_values = {"a", "bb", "ccc"};
  {
    LFIndexer<uint32_t> writer;
    OpenIndexerLegacy(writer, *ckp, string_type, CheckpointManifest(),
                      MemoryLevel::kInMemory);
    for (const auto& v : short_values) {
      writer.insert(neug::Value::STRING(v), true);
    }
    dump_desc = DumpIndexerLegacy(writer, *ckp);
  }

  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, dump_desc,
                    MemoryLevel::kInMemory);
  ExpectStringValues(indexer, short_values);

  constexpr size_t kExtra = 4;
  indexer.reserve(short_values.size() + kExtra);
  EXPECT_GE(indexer.capacity(), short_values.size() + kExtra);

  std::vector<std::string> long_values;
  for (size_t i = 0; i < kExtra; ++i) {
    long_values.push_back(std::string(60, static_cast<char>('d' + i)));
  }
  for (const auto& v : long_values) {
    indexer.insert(neug::Value::STRING(v), true);
  }

  std::vector<std::string> all = short_values;
  all.insert(all.end(), long_values.begin(), long_values.end());
  ExpectStringValues(indexer, all);
  indexer.Close();
}

TEST_F(LFIndexerTest, VarcharShortDumpReopenInsertSafeLong_InMemory) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  constexpr uint16_t kWidth = 48;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);
  CheckpointManifest dump_desc;

  std::vector<std::string> short_values = {"x", "y", "z", "w"};
  {
    LFIndexer<uint32_t> writer;
    OpenIndexerLegacy(writer, *ckp, string_type, CheckpointManifest(),
                      MemoryLevel::kInMemory);
    writer.reserve(short_values.size());
    for (const auto& v : short_values) {
      writer.insert(neug::Value::STRING(v), false);
    }
    dump_desc = DumpIndexerLegacy(writer, *ckp);
    writer.Close();
  }

  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, dump_desc,
                    MemoryLevel::kInMemory);
  EXPECT_EQ(indexer.size(), short_values.size());

  std::vector<std::string> long_values;
  for (size_t i = 0; i < 5; ++i) {
    long_values.push_back(std::string(45, static_cast<char>('A' + i)));
  }
  for (const auto& v : long_values) {
    indexer.insert(neug::Value::STRING(v), true);
  }

  std::vector<std::string> all = short_values;
  all.insert(all.end(), long_values.begin(), long_values.end());
  ExpectStringValues(indexer, all);
  indexer.Close();
}

TEST_F(LFIndexerTest, VarcharShortDumpReopenReserveThenInsertLong_SyncToFile) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  constexpr uint16_t kWidth = 32;
  auto type_info = std::make_shared<StringTypeInfo>(kWidth);
  DataType string_type(DataTypeId::kVarchar, type_info);
  CheckpointManifest dump_desc;

  std::vector<std::string> short_values = {"hi", "yo", "ok"};
  {
    LFIndexer<uint32_t> writer;
    OpenIndexerLegacy(writer, *ckp, string_type, CheckpointManifest(),
                      MemoryLevel::kInMemory);
    for (const auto& v : short_values) {
      writer.insert(neug::Value::STRING(v), true);
    }
    dump_desc = DumpIndexerLegacy(writer, *ckp);
    writer.Close();
  }

  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, dump_desc,
                    MemoryLevel::kSyncToFile);
  ExpectStringValues(indexer, short_values);

  constexpr size_t kExtra = 3;
  indexer.reserve(short_values.size() + kExtra);
  EXPECT_GE(indexer.capacity(), short_values.size() + kExtra);

  std::vector<std::string> long_values;
  for (size_t i = 0; i < kExtra; ++i) {
    long_values.push_back(std::string(30, static_cast<char>('p' + i)));
  }
  for (const auto& v : long_values) {
    indexer.insert(neug::Value::STRING(v), true);
  }

  std::vector<std::string> all = short_values;
  all.insert(all.end(), long_values.begin(), long_values.end());
  ExpectStringValues(indexer, all);
  indexer.Close();
}

TEST_F(LFIndexerTest, VarcharStringOverflow) {
  CheckpointManager ws;
  ws.Open(test_dir_);
  auto ckp = make_checkpoint(ws);

  auto type_info = std::make_shared<StringTypeInfo>(32);
  DataType string_type(DataTypeId::kVarchar, type_info);
  LFIndexer<uint32_t> indexer;
  OpenIndexerLegacy(indexer, *ckp, string_type, CheckpointManifest(),
                    MemoryLevel::kInMemory);
  indexer.reserve(4);

  std::vector<std::string> valid_strings = {
      "short",                 // 5 chars
      "medium_length_string",  // 21 chars
      std::string(32, 'a'),    // exactly 32 chars (max length)
      "boundary"               // 8 chars, to test boundary condition
  };

  for (const auto& v : valid_strings) {
    indexer.insert(neug::Value::STRING(v), false);
  }
  ExpectStringValues(indexer, valid_strings);
  indexer.reserve(8);

  std::string overflow_string = std::string(31, 'a');
  for (size_t i = 0; i < 2; ++i) {
    std::string test_string =
        overflow_string + std::to_string(i);  // 31 chars + 1 char = 32 chars
    indexer.insert(neug::Value::STRING(test_string), false);
    valid_strings.push_back(test_string);
  }
  ExpectStringValues(indexer, valid_strings);

  EXPECT_THROW(indexer.insert(neug::Value::STRING(overflow_string), false),
               neug::exception::StorageException);

  indexer.Close();
}

TEST_F(LFIndexerTest, RejectsUnsupportedPrimaryKeyType) {
  EXPECT_THROW((LFIndexer<uint32_t>(DataType(DataTypeId::kDouble))),
               neug::exception::NotSupportedException);
}

}  // namespace neug
