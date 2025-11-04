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

  std::string dir_;
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
  table.Open(dir_, memory_level_, true);
  table.Reserve(vertex_count_);

  gs::vid_t lid1, lid2, lid3;
  gs::Property oid1, oid2, oid3;
  oid1.set_int64(1);
  oid2.set_int64(2);
  oid3.set_int64(3);
  lid1 = table.AddVertex(oid1, 1);
  lid2 = table.AddVertex(oid2, 2);
  lid3 = table.AddVertex(oid3, 3);
  LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
            << lid3;
  LOG(INFO) << "and oids: " << oid1.as_int64() << ", " << oid2.as_int64()
            << ", " << oid3.as_int64();

  EXPECT_EQ(table.VertexNum(), 3);
  EXPECT_EQ(table.LidNum(), 3);
  EXPECT_EQ(table.VertexNum(2), 2);
  EXPECT_EQ(table.VertexNum(1), 1);

  EXPECT_TRUE(table.vertex_table_modified());
  EXPECT_TRUE(table.IsValidLid(lid1));
  EXPECT_TRUE(table.IsValidLid(lid2));
  EXPECT_TRUE(table.IsValidLid(lid3));
  EXPECT_FALSE(table.IsValidLid(4));
  EXPECT_TRUE(table.IsValidLid(lid3, 3));

  EXPECT_EQ(oid1, table.GetOid(lid1));
  EXPECT_EQ(oid2, table.GetOid(lid2));
  EXPECT_EQ(oid3, table.GetOid(lid3));

  try {
    auto ret = table.GetOid(3, 2);
    FAIL() << "Expected exception not thrown";
  } catch (gs::exception::Exception& e) {}

  gs::vid_t tmp_vid;
  EXPECT_FALSE(table.get_index(oid1, tmp_vid, 0));
  EXPECT_TRUE(table.get_index(oid1, tmp_vid, 1));
}

TEST_F(VertexTableTest, VertexTableDumpAndReload) {
  std::string dump_dir = "/tmp/test_vertex_table_dump";
  if (std::filesystem::exists(dump_dir)) {
    std::filesystem::remove_all(dump_dir);
  }
  std::filesystem::create_directories(dump_dir);
  std::filesystem::create_directories(gs::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(gs::temp_checkpoint_dir(dump_dir));
  {
    gs::VertexTable table(v_label_name_, pk_type_, property_names_,
                          property_types_, storage_strategies_);
    table.Open(dump_dir, memory_level_, true);
    table.Reserve(vertex_count_);

    gs::vid_t lid1, lid2, lid3;
    gs::Property oid1, oid2, oid3;
    oid1.set_int64(1);
    oid2.set_int64(2);
    oid3.set_int64(3);
    lid1 = table.AddVertex(oid1, 1);
    lid2 = table.AddVertex(oid2, 2);
    lid3 = table.AddVertex(oid3, 3);
    LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
              << lid3;
    table.Dump(gs::temp_checkpoint_dir(dump_dir));
  }

  std::filesystem::remove_all(gs::checkpoint_dir(dump_dir));
  std::filesystem::rename(gs::temp_checkpoint_dir(dump_dir),
                          gs::checkpoint_dir(dump_dir));

  {
    gs::VertexTable new_table(v_label_name_, pk_type_, property_names_,
                              property_types_, storage_strategies_);
    new_table.Open(dump_dir, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 3);
    EXPECT_EQ(new_table.LidNum(), 3);
    EXPECT_EQ(new_table.VertexNum(2), 3);
    EXPECT_EQ(new_table.VertexNum(1), 3);
    EXPECT_FALSE(new_table.vertex_table_modified());
  }
}

TEST_F(VertexTableTest, VertexTableAddAndDeleteAndReload) {
  std::string dump_dir = "/tmp/test_vertex_table_add_delete";
  if (std::filesystem::exists(dump_dir)) {
    std::filesystem::remove_all(dump_dir);
  }
  std::filesystem::create_directories(dump_dir);
  std::filesystem::create_directories(gs::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(gs::temp_checkpoint_dir(dump_dir));
  gs::vid_t lid1, lid2, lid3;
  gs::Property oid1, oid2, oid3;
  {
    gs::VertexTable table(v_label_name_, pk_type_, property_names_,
                          property_types_, storage_strategies_);
    table.Open(dump_dir, memory_level_, true);
    table.Reserve(vertex_count_);

    oid1.set_int64(1);
    oid2.set_int64(2);
    oid3.set_int64(3);
    lid1 = table.AddVertex(oid1, 1);
    lid2 = table.AddVertex(oid2, 2);
    lid3 = table.AddVertex(oid3, 3);
    LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
              << lid3;

    EXPECT_EQ(table.VertexNum(), 3);
    EXPECT_EQ(table.LidNum(), 3);

    table.Dump(gs::temp_checkpoint_dir(dump_dir));
  }

  std::filesystem::remove_all(gs::checkpoint_dir(dump_dir));
  std::filesystem::rename(gs::temp_checkpoint_dir(dump_dir),
                          gs::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(gs::temp_checkpoint_dir(dump_dir));

  {
    gs::VertexTable new_table(v_label_name_, pk_type_, property_names_,
                              property_types_, storage_strategies_);
    new_table.Open(dump_dir, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 3);
    EXPECT_EQ(new_table.LidNum(), 3);

    new_table.BatchDeleteVertices({lid1, lid2});
    EXPECT_EQ(new_table.VertexNum(), 1);
    EXPECT_EQ(new_table.LidNum(), 3);

    gs::vid_t tmp_vid;
    EXPECT_FALSE(new_table.get_index(oid1, tmp_vid));
    EXPECT_FALSE(new_table.get_index(oid2, tmp_vid));
    EXPECT_TRUE(new_table.get_index(oid3, tmp_vid));

    new_table.Dump(gs::temp_checkpoint_dir(dump_dir));
  }

  std::filesystem::remove_all(gs::checkpoint_dir(dump_dir));
  std::filesystem::rename(gs::temp_checkpoint_dir(dump_dir),
                          gs::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(gs::temp_checkpoint_dir(dump_dir));

  {
    gs::VertexTable new_table(v_label_name_, pk_type_, property_names_,
                              property_types_, storage_strategies_);
    new_table.Open(dump_dir, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 1);
    EXPECT_EQ(new_table.LidNum(), 3);
    EXPECT_TRUE(new_table.vertex_table_modified());

    gs::vid_t tmp_vid;
    EXPECT_FALSE(new_table.get_index(oid1, tmp_vid));
    EXPECT_FALSE(new_table.get_index(oid2, tmp_vid));
    EXPECT_TRUE(new_table.get_index(oid3, tmp_vid));
  }
}