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
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/property/types.h"

#include <glog/logging.h>

class VertexTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = "/tmp/test_vertex_table";
    tmp_dir_ = dir_ + "/tmp";
    memory_level_ = 1;
    // remove the directory if it exists
    if (std::filesystem::exists(dir_)) {
      std::filesystem::remove_all(dir_);
    }
    // create the directory
    std::filesystem::create_directories(dir_);

    v_label_name_ = "person";
    pk_type_ = gs::PropertyType::kInt64;
    property_names_ = {"name", "age", "score"};
    property_types_ = {gs::PropertyType::kStringView, gs::PropertyType::kInt32,
                       gs::PropertyType::kDouble};
    storage_strategies_ = {gs::StorageStrategy::kMem, gs::StorageStrategy::kMem,
                           gs::StorageStrategy::kMem};
    vertex_count_ = 1000000;
  }
  void TearDown() override {
    // remove the directory if it exists
    if (std::filesystem::exists(dir_)) {
      std::filesystem::remove_all(dir_);
    }
  }

  std::string dir_, tmp_dir_;
  int32_t memory_level_;
  std::string v_label_name_;
  gs::PropertyType pk_type_;
  std::vector<std::string> property_names_;
  std::vector<gs::PropertyType> property_types_;
  std::vector<gs::StorageStrategy> storage_strategies_;
  std::mt19937 generator_;

  size_t vertex_count_;
};

TEST_F(VertexTableTest, VertexTableBasicOps) {
  gs::VertexTable table(v_label_name_, pk_type_, property_names_,
                        property_types_, storage_strategies_);
  table.Open(dir_, tmp_dir_, memory_level_, true);
  table.Reserve(vertex_count_);

  gs::vid_t lid1, lid2, lid3;
  gs::Any oid1, oid2, oid3;
  oid1.set_i64(1);
  oid2.set_i64(2);
  oid3.set_i64(3);
  lid1 = table.add_vertex(oid1, 1);
  lid2 = table.add_vertex(oid2, 2);
  lid3 = table.add_vertex(oid3, 3);

  EXPECT_EQ(table.vertex_num(), 3);
  EXPECT_EQ(table.lid_num(), 3);
  EXPECT_EQ(table.vertex_num(2), 2);
  EXPECT_EQ(table.vertex_num(1), 1);

  EXPECT_TRUE(table.vertex_table_modified());
  EXPECT_TRUE(table.is_valid_lid(lid1));
  EXPECT_TRUE(table.is_valid_lid(lid2));
  EXPECT_TRUE(table.is_valid_lid(lid3));
  EXPECT_FALSE(table.is_valid_lid(4));
  EXPECT_TRUE(table.is_valid_lid(lid3, 3));

  EXPECT_EQ(oid1, table.get_oid(lid1));
  EXPECT_EQ(oid2, table.get_oid(lid2));
  EXPECT_EQ(oid3, table.get_oid(lid3));

  try {
    auto ret = table.get_oid(3, 2);
    FAIL() << "Expected exception not thrown";
  } catch (gs::exception::Exception& e) {}

  gs::vid_t tmp_vid;
  EXPECT_FALSE(table.get_index(oid1, tmp_vid, 0));
  EXPECT_TRUE(table.get_index(oid1, tmp_vid, 1));
}