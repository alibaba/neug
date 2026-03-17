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
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "neug/common/extra_type_info.h"
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

  std::string test_dir_;
  std::string snapshot_dir_;
  std::string checkpoint_dir_;
  std::string work_dir_;
};

TEST_F(LFIndexerTest, SupportsCoreMutableInterfacesInMemory) {
  const std::string base_path = test_dir_ + "/core_index";

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
  EXPECT_EQ(indexer.insert(Property::from_int64(values[0])), 0U);
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_EQ(indexer.insert_safe(Property::from_int64(values[i])),
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

  indexer.ensure_writable(work_dir_);
  indexer.close();
}

TEST_F(LFIndexerTest, DumpsAndOpensAcrossBackends) {
  const std::string name = "persisted_index";
  const std::string in_memory_base = test_dir_ + "/persisted_seed";

  LFIndexer<uint32_t> writable;
  writable.init(DataTypeId::kInt64);
  writable.open_in_memory(in_memory_base);

  std::vector<int64_t> values = {5, 10, 15, 20};
  for (const auto& value : values) {
    writable.insert_safe(Property::from_int64(value));
  }

  writable.dump(name, snapshot_dir_);
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".meta"));
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".keys"));
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".indices"));

  LFIndexer<uint32_t> readonly;
  readonly.init(DataTypeId::kInt64);
  readonly.open(name, snapshot_dir_);
  ExpectInt64Values(readonly, values);
  EXPECT_EQ(readonly.get_keys().type(), DataTypeId::kInt64);
  readonly.close();

  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".meta"));
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".keys"));
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/" + name + ".indices"));
}

}  // namespace
}  // namespace neug
