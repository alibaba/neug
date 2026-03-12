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
#include "unittest/utils.h"

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
    pk_type_ = neug::DataTypeId::kInt64;
    property_names_ = {"name", "age", "score"};
    property_types_ = {neug::DataTypeId::kVarchar, neug::DataTypeId::kInt32,
                       neug::DataTypeId::kDouble};
    property_values_ = {neug::Property::from_string_view("Alice"),
                        neug::Property::from_int32(30),
                        neug::Property::from_double(88.5)};
    storage_strategies_ = {neug::StorageStrategy::kMem,
                           neug::StorageStrategy::kMem,
                           neug::StorageStrategy::kMem};
    default_prop_values_ = {neug::Property::from_string_view(""),
                            neug::Property::from_int32(0),
                            neug::Property::from_double(0.0)};
    vertex_count_ = 1000000;
    schema_.AddVertexLabel(v_label_name_, property_types_, property_names_,
                           {std::make_tuple(pk_type_, "id", 0)},
                           storage_strategies_, 4096, "", default_prop_values_);
    v_label_id_ = schema_.get_vertex_label_id(v_label_name_);
  }
  void TearDown() override {
    // remove the directory if it exists
    if (std::filesystem::exists(dir_)) {
      std::filesystem::remove_all(dir_);
    }
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> generate_record_batches(
      size_t num_vertices) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    std::vector<int64_t> oid_values;
    std::vector<std::string> name_values;
    std::vector<int32_t> age_values;
    std::vector<double> score_values;
    for (int64_t i = 0; i < num_vertices; ++i) {
      oid_values.push_back(i);
      name_values.push_back("name_" + std::to_string(i));
      age_values.push_back(static_cast<int32_t>(20 + (i % 30)));
      score_values.push_back(50.0 + (i % 50));
    }
    auto oid_array = convert_to_arrow_arrays(oid_values, 10);
    auto name_array = convert_to_arrow_arrays(name_values, 10);
    auto age_array = convert_to_arrow_arrays(age_values, 10);
    auto score_array = convert_to_arrow_arrays(score_values, 10);

    auto record_batches = convert_to_record_batches(
        {"id", "name", "age", "score"},
        {oid_array, name_array, age_array, score_array});
    return record_batches;
  }

  std::string dir_;
  int32_t memory_level_;
  std::string v_label_name_;
  neug::DataTypeId pk_type_;
  std::vector<std::string> property_names_;
  std::vector<neug::DataType> property_types_;
  std::vector<neug::Property> property_values_;
  std::vector<neug::StorageStrategy> storage_strategies_;
  std::vector<neug::Property> default_prop_values_;
  std::mt19937 generator_;
  neug::Schema schema_;
  neug::label_t v_label_id_ = 0;

  size_t vertex_count_;
};

TEST_F(VertexTableTest, VertexTableBasicOps) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(vertex_count_);

  neug::vid_t lid1, lid2, lid3;
  neug::Property oid1, oid2, oid3;
  oid1.set_int64(1);
  oid2.set_int64(2);
  oid3.set_int64(3);
  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1));
  EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2));
  EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 3));
  LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
            << lid3;
  LOG(INFO) << "and oids: " << oid1.as_int64() << ", " << oid2.as_int64()
            << ", " << oid3.as_int64();

  EXPECT_EQ(table.VertexNum(), 3);
  EXPECT_EQ(table.LidNum(), 3);
  EXPECT_EQ(table.VertexNum(2), 2);
  EXPECT_EQ(table.VertexNum(1), 1);

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
  } catch (neug::exception::Exception& e) {}

  neug::vid_t tmp_vid;
  EXPECT_FALSE(table.get_index(oid1, tmp_vid, 0));
  EXPECT_TRUE(table.get_index(oid1, tmp_vid, 1));

  table.Drop();
}

TEST_F(VertexTableTest, VertexTableDumpAndReload) {
  std::string dump_dir = "/tmp/test_vertex_table_dump";
  if (std::filesystem::exists(dump_dir)) {
    std::filesystem::remove_all(dump_dir);
  }
  std::filesystem::create_directories(dump_dir);
  std::filesystem::create_directories(neug::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(neug::temp_checkpoint_dir(dump_dir));
  {
    neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
    table.Open(dump_dir, memory_level_, true);
    table.Reserve(vertex_count_);

    neug::vid_t lid1, lid2, lid3;
    neug::Property oid1, oid2, oid3;
    oid1.set_int64(1);
    oid2.set_int64(2);
    oid3.set_int64(3);
    EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1));
    EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2));
    EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 3));
    LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
              << lid3;
    table.Dump(neug::temp_checkpoint_dir(dump_dir));
  }

  std::filesystem::remove_all(neug::checkpoint_dir(dump_dir));
  std::filesystem::rename(neug::temp_checkpoint_dir(dump_dir),
                          neug::checkpoint_dir(dump_dir));

  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    new_table.Open(dump_dir, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 3);
    EXPECT_EQ(new_table.LidNum(), 3);
    EXPECT_EQ(new_table.VertexNum(2), 3);
    EXPECT_EQ(new_table.VertexNum(1), 3);
    new_table.Close();
  }
}

TEST_F(VertexTableTest, VertexTableAddAndDeleteAndReload) {
  std::string dump_dir = "/tmp/test_vertex_table_add_delete";
  if (std::filesystem::exists(dump_dir)) {
    std::filesystem::remove_all(dump_dir);
  }
  std::filesystem::create_directories(dump_dir);
  std::filesystem::create_directories(neug::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(neug::temp_checkpoint_dir(dump_dir));
  neug::vid_t lid1, lid2, lid3;
  neug::Property oid1, oid2, oid3;
  {
    neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
    table.Open(dump_dir, memory_level_, true);
    table.Reserve(vertex_count_);

    oid1.set_int64(1);
    oid2.set_int64(2);
    oid3.set_int64(3);
    EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1));
    EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2));
    EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 3));
    LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
              << lid3;

    EXPECT_EQ(table.VertexNum(), 3);
    EXPECT_EQ(table.LidNum(), 3);

    table.Dump(neug::temp_checkpoint_dir(dump_dir));
  }

  std::filesystem::remove_all(neug::checkpoint_dir(dump_dir));
  std::filesystem::rename(neug::temp_checkpoint_dir(dump_dir),
                          neug::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(neug::temp_checkpoint_dir(dump_dir));

  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    new_table.Open(dump_dir, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 3);
    EXPECT_EQ(new_table.LidNum(), 3);

    new_table.BatchDeleteVertices({lid1, lid2});
    EXPECT_EQ(new_table.VertexNum(), 1);
    EXPECT_EQ(new_table.LidNum(), 3);

    neug::vid_t tmp_vid;
    EXPECT_FALSE(new_table.get_index(oid1, tmp_vid));
    EXPECT_FALSE(new_table.get_index(oid2, tmp_vid));
    EXPECT_TRUE(new_table.get_index(oid3, tmp_vid));

    new_table.Dump(neug::temp_checkpoint_dir(dump_dir));
  }

  std::filesystem::remove_all(neug::checkpoint_dir(dump_dir));
  std::filesystem::rename(neug::temp_checkpoint_dir(dump_dir),
                          neug::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(neug::temp_checkpoint_dir(dump_dir));

  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    new_table.Open(dump_dir, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 1);
    EXPECT_EQ(new_table.LidNum(), 3);

    neug::vid_t tmp_vid;
    EXPECT_FALSE(new_table.get_index(oid1, tmp_vid));
    EXPECT_FALSE(new_table.get_index(oid2, tmp_vid));
    EXPECT_TRUE(new_table.get_index(oid3, tmp_vid));
  }
}

TEST_F(VertexTableTest, AddVertexBasic) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  neug::Property oid1, oid2, oid3;
  oid1.set_int64(100);
  oid2.set_int64(200);
  oid3.set_int64(300);
  neug::vid_t lid1, lid2, lid3;
  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 0));
  EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 1));
  EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 2));

  EXPECT_EQ(table.VertexNum(), 3);
  EXPECT_EQ(table.LidNum(), 3);

  EXPECT_TRUE(table.IsValidLid(lid1, 0));
  EXPECT_TRUE(table.IsValidLid(lid2, 1));
  EXPECT_TRUE(table.IsValidLid(lid3, 2));

  EXPECT_EQ(table.VertexNum(0), 1);  // Only lid1 visible at ts=0
  EXPECT_EQ(table.VertexNum(1), 2);  // lid1 and lid2 visible at ts=1
  EXPECT_EQ(table.VertexNum(2), 3);  // All visible at ts=2

  EXPECT_EQ(table.GetOid(lid1), oid1);
  EXPECT_EQ(table.GetOid(lid2), oid2);
  EXPECT_EQ(table.GetOid(lid3), oid3);

  neug::vid_t tmp_vid;
  EXPECT_TRUE(table.get_index(oid1, tmp_vid));
  EXPECT_EQ(tmp_vid, lid1);
  EXPECT_TRUE(table.get_index(oid2, tmp_vid));
  EXPECT_EQ(tmp_vid, lid2);
  EXPECT_TRUE(table.get_index(oid3, tmp_vid));
  EXPECT_EQ(tmp_vid, lid3);
}

// Test AddVertex for concurrent scenarios
TEST_F(VertexTableTest, AddVertex) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  neug::vid_t tmp_vid;
  EXPECT_FALSE(table.AddVertex(neug::Property::from_int64(1), property_values_,
                               tmp_vid));

  std::vector<neug::Property> oids;
  std::vector<neug::vid_t> lids;
  table.Reserve(100);
  lids.resize(100);

  for (int64_t i = 0; i < 100; ++i) {
    neug::Property oid;
    oid.set_int64(i);
    oids.push_back(oid);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i % 10));
  }

  EXPECT_EQ(table.VertexNum(), 100);
  EXPECT_EQ(table.LidNum(), 100);

  for (size_t i = 0; i < oids.size(); ++i) {
    neug::vid_t tmp_vid;
    EXPECT_TRUE(table.get_index(oids[i], tmp_vid));
    EXPECT_EQ(tmp_vid, lids[i]);
    EXPECT_TRUE(table.IsValidLid(lids[i], i % 10));
  }
}

// Test DeleteVertex basic functionality
TEST_F(VertexTableTest, DeleteVertexBasic) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  neug::Property oid1, oid2, oid3;
  oid1.set_int64(1);
  oid2.set_int64(2);
  oid3.set_int64(3);
  neug::vid_t lid1, lid2, lid3;

  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1));
  EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2));
  EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 3));

  EXPECT_EQ(table.VertexNum(), 3);

  table.DeleteVertex(lid1, 4);
  EXPECT_EQ(table.VertexNum(), 2);
  EXPECT_FALSE(table.IsValidLid(lid1, 4));

  table.DeleteVertex(oid2, 5);
  EXPECT_EQ(table.VertexNum(), 1);
  EXPECT_FALSE(table.IsValidLid(lid2, 5));

  EXPECT_TRUE(table.IsValidLid(lid3, 5));
  neug::vid_t tmp_vid;
  EXPECT_FALSE(table.get_index(oid1, tmp_vid));
  EXPECT_FALSE(table.get_index(oid2, tmp_vid));
  EXPECT_TRUE(table.get_index(oid3, tmp_vid));
}

// Test RevertDeleteVertex basic functionality
TEST_F(VertexTableTest, RevertDeleteVertexBasic) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  neug::Property oid1;
  oid1.set_int64(1);
  neug::vid_t lid1;
  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1));

  EXPECT_EQ(table.VertexNum(), 1);
  EXPECT_TRUE(table.IsValidLid(lid1, 1));

  // Delete vertex
  table.DeleteVertex(lid1, 2);
  EXPECT_EQ(table.VertexNum(), 0);
  EXPECT_FALSE(table.IsValidLid(lid1, 2));

  // Revert deletion
  table.RevertDeleteVertex(lid1, 3);
  EXPECT_EQ(table.VertexNum(), 1);
  EXPECT_TRUE(table.IsValidLid(lid1, 3));

  neug::vid_t tmp_vid;
  EXPECT_TRUE(table.get_index(oid1, tmp_vid));
  EXPECT_EQ(tmp_vid, lid1);
  EXPECT_EQ(table.GetOid(lid1), oid1);
}

// Test complex combination: Add -> Delete -> Revert
TEST_F(VertexTableTest, AddDeleteRevertCombination) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  std::vector<neug::Property> oids;
  std::vector<neug::vid_t> lids;
  lids.resize(10);

  for (int64_t i = 0; i < 10; ++i) {
    neug::Property oid;
    oid.set_int64(i);
    oids.push_back(oid);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i));
  }

  EXPECT_EQ(table.VertexNum(), 10);

  for (size_t i = 0; i < lids.size(); i += 2) {
    table.DeleteVertex(lids[i], 20);
  }

  EXPECT_EQ(table.VertexNum(), 5);

  for (size_t i = 1; i < lids.size(); i += 2) {
    EXPECT_TRUE(table.IsValidLid(lids[i], 20));
    neug::vid_t tmp_vid;
    EXPECT_TRUE(table.get_index(oids[i], tmp_vid));
  }

  for (size_t i = 0; i < lids.size(); i += 4) {  // Revert 0, 4, 8
    table.RevertDeleteVertex(lids[i], 30);
  }

  EXPECT_EQ(table.VertexNum(), 8);  // 5 odd + 3 reverted

  // Verify reverted vertices
  for (size_t i = 0; i < lids.size(); i += 4) {
    EXPECT_TRUE(table.IsValidLid(lids[i], 30));
    neug::vid_t tmp_vid;
    EXPECT_TRUE(table.get_index(oids[i], tmp_vid));
  }
}

// Test complex combination: Multiple deletes and reverts
TEST_F(VertexTableTest, MultipleDeletesAndReverts) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  neug::Property oid;
  oid.set_int64(42);
  neug::vid_t lid;
  EXPECT_TRUE(table.AddVertex(oid, property_values_, lid, 1));

  EXPECT_EQ(table.VertexNum(), 1);

  // Delete and revert multiple times
  table.DeleteVertex(lid, 2);
  EXPECT_EQ(table.VertexNum(), 0);
  EXPECT_FALSE(table.IsValidLid(lid, 2));

  table.RevertDeleteVertex(lid, 3);
  EXPECT_EQ(table.VertexNum(), 1);
  EXPECT_TRUE(table.IsValidLid(lid, 3));

  table.DeleteVertex(lid, 4);
  EXPECT_EQ(table.VertexNum(), 0);
  EXPECT_FALSE(table.IsValidLid(lid, 4));

  table.RevertDeleteVertex(lid, 5);
  EXPECT_EQ(table.VertexNum(), 1);
  EXPECT_TRUE(table.IsValidLid(lid, 5));

  neug::vid_t tmp_vid;
  EXPECT_TRUE(table.get_index(oid, tmp_vid));
  EXPECT_EQ(tmp_vid, lid);
}

// Test AddVertex and AddVertexSafe mixed usage
TEST_F(VertexTableTest, MixedAddVertexAndAddVertexSafe) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(50);

  std::vector<neug::vid_t> lids;
  lids.resize(20);

  // Add using both methods alternately
  for (int64_t i = 0; i < 20; ++i) {
    neug::Property oid;
    oid.set_int64(i);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i));
  }

  EXPECT_EQ(table.VertexNum(), 20);
  EXPECT_EQ(table.LidNum(), 20);

  for (size_t i = 0; i < lids.size(); ++i) {
    EXPECT_TRUE(table.IsValidLid(lids[i], i));
  }
}

// Test temporal visibility with add/delete/revert
TEST_F(VertexTableTest, TemporalVisibilityComplex) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  neug::Property oid1, oid2, oid3;
  oid1.set_int64(1);
  oid2.set_int64(2);
  oid3.set_int64(3);

  EXPECT_EQ(table.VertexNum(0), 0);
  neug::vid_t lid1;
  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1));
  EXPECT_TRUE(table.IsValidLid(lid1, 1));
  EXPECT_EQ(table.VertexNum(1), 1);

  neug::vid_t lid2;
  EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2));
  EXPECT_TRUE(table.IsValidLid(lid1, 2));
  EXPECT_TRUE(table.IsValidLid(lid2, 2));
  EXPECT_EQ(table.VertexNum(2), 2);  // oid2,

  table.DeleteVertex(lid1, 3);
  EXPECT_EQ(table.VertexNum(3), 1);  // oid2,
  EXPECT_FALSE(table.IsValidLid(lid1, 3));
  neug::vid_t lid3;
  EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 4));
  EXPECT_FALSE(table.IsValidLid(lid1, 4));
  EXPECT_TRUE(table.IsValidLid(lid3, 4));
  EXPECT_EQ(table.VertexNum(4), 2);  // oid2, oid3

  table.RevertDeleteVertex(lid1, 5);
  EXPECT_TRUE(table.IsValidLid(lid1, 5));
  EXPECT_TRUE(table.IsValidLid(lid2, 5));
  EXPECT_EQ(table.VertexNum(5), 3);  // oid1 (reverted), oid2, oid3

  table.DeleteVertex(lid2, 6);
  EXPECT_FALSE(table.IsValidLid(lid2, 6));
  EXPECT_TRUE(table.IsValidLid(lid3, 6));
  EXPECT_EQ(table.VertexNum(6), 2);  // oid1, oid3 (oid2 deleted)
}

// Test edge cases: Delete already deleted vertex
TEST_F(VertexTableTest, DeleteAlreadyDeletedVertex) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  neug::Property oid;
  oid.set_int64(1);
  neug::vid_t lid;
  EXPECT_TRUE(table.AddVertex(oid, property_values_, lid, 1));

  table.DeleteVertex(lid, 2);
  EXPECT_EQ(table.VertexNum(), 0);

  // Try to delete again - should log warning but not crash
  table.DeleteVertex(lid, 3);
  EXPECT_EQ(table.VertexNum(), 0);

  // Also try deleting by oid
  table.DeleteVertex(oid, 4);
  EXPECT_EQ(table.VertexNum(), 0);
}

// Test edge cases: Revert non-deleted vertex
TEST_F(VertexTableTest, RevertNonDeletedVertex) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  neug::Property oid;
  oid.set_int64(1);
  neug::vid_t lid;
  EXPECT_TRUE(table.AddVertex(oid, property_values_, lid, 1));

  EXPECT_EQ(table.VertexNum(), 1);

  // Try to revert a non-deleted vertex - should log warning
  table.RevertDeleteVertex(lid, 2);
  EXPECT_EQ(table.VertexNum(), 1);
  EXPECT_TRUE(table.IsValidLid(lid, 2));
}

// Test complex combination with dump and reload
TEST_F(VertexTableTest, ComplexAddDeleteRevertDumpReload) {
  std::string dump_dir = "/tmp/test_vertex_table_complex";
  if (std::filesystem::exists(dump_dir)) {
    std::filesystem::remove_all(dump_dir);
  }
  std::filesystem::create_directories(dump_dir);
  std::filesystem::create_directories(neug::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(neug::temp_checkpoint_dir(dump_dir));

  std::vector<neug::Property> oids;
  std::vector<neug::vid_t> lids;
  lids.resize(20);

  // Create complex state
  {
    neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
    table.Open(dump_dir, memory_level_, true);
    table.Reserve(100);

    for (int64_t i = 0; i < 20; ++i) {
      neug::Property oid;
      oid.set_int64(i);
      oids.push_back(oid);
      EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i));
    }

    for (size_t i = 5; i < 15; ++i) {
      table.DeleteVertex(lids[i], 50);
    }

    for (size_t i = 5; i < 10; ++i) {
      table.RevertDeleteVertex(lids[i], 60);
    }

    EXPECT_EQ(table.VertexNum(), 15);  // 5 + 5 + 5 (0-4, 5-9 reverted, 15-19)

    table.Dump(neug::temp_checkpoint_dir(dump_dir));
  }

  std::filesystem::remove_all(neug::checkpoint_dir(dump_dir));
  std::filesystem::rename(neug::temp_checkpoint_dir(dump_dir),
                          neug::checkpoint_dir(dump_dir));

  // Reload and verify
  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    new_table.Open(dump_dir, memory_level_);

    EXPECT_EQ(new_table.VertexNum(), 15);
    EXPECT_EQ(new_table.LidNum(), 20);

    for (size_t i = 0; i < 5; ++i) {
      neug::vid_t tmp_vid;
      EXPECT_TRUE(new_table.get_index(oids[i], tmp_vid));
    }
    for (size_t i = 5; i < 10; ++i) {
      neug::vid_t tmp_vid;
      EXPECT_TRUE(new_table.get_index(oids[i], tmp_vid));
    }
    for (size_t i = 10; i < 15; ++i) {
      neug::vid_t tmp_vid;
      EXPECT_FALSE(new_table.get_index(oids[i], tmp_vid));
    }
    for (size_t i = 15; i < 20; ++i) {
      neug::vid_t tmp_vid;
      EXPECT_TRUE(new_table.get_index(oids[i], tmp_vid));
    }
  }

  if (std::filesystem::exists(dump_dir)) {
    std::filesystem::remove_all(dump_dir);
  }
}

// Test stress: Many add/delete/revert operations
TEST_F(VertexTableTest, StressAddDeleteRevert) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(1000);

  std::vector<neug::Property> oids;
  std::vector<neug::vid_t> lids;
  lids.resize(100);

  for (int64_t i = 0; i < 100; ++i) {
    neug::Property oid;
    oid.set_int64(i);
    oids.push_back(oid);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], 1));
  }

  EXPECT_EQ(table.VertexNum(), 100);

  for (size_t i = 1; i < lids.size(); i += 2) {
    table.DeleteVertex(lids[i], 2);
  }

  EXPECT_EQ(table.VertexNum(), 50);

  for (size_t i = 1; i < lids.size(); i += 4) {
    table.RevertDeleteVertex(lids[i], 3);
  }

  size_t expected_count = 50 + 25;  // 50 even + 25 reverted
  EXPECT_EQ(table.VertexNum(), expected_count);

  for (size_t i = 0; i < lids.size(); i += 5) {
    if (table.IsValidLid(lids[i])) {
      table.DeleteVertex(lids[i], 4);
    }
  }

  size_t manual_count = 0;
  for (size_t i = 0; i < lids.size(); ++i) {
    if (table.IsValidLid(lids[i])) {
      manual_count++;
    }
  }
  size_t cnt = 0;
  table.get_vertex_timestamp().foreach_vertex(
      [&](neug::vid_t vid) { cnt++; }, table.LidNum() + 10000,
      std::numeric_limits<neug::timestamp_t>::max());
  EXPECT_EQ(cnt, manual_count);

  EXPECT_EQ(table.VertexNum(), manual_count);
}

TEST_F(VertexTableTest, VertexTableResizeTest) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  auto record_batches = generate_record_batches(10000);
  std::shared_ptr<neug::IRecordBatchSupplier> batch_supplier =
      std::make_shared<GeneratedRecordBatchSupplier>(std::move(record_batches));
  table.insert_vertices(batch_supplier);

  EXPECT_EQ(table.VertexNum(), 10000);
  EXPECT_EQ(table.LidNum(), 10000);
  table.Compact(true);
  EXPECT_EQ(table.get_vertex_timestamp().InitVertexNum(), 10000);

  std::string dump_dir = "/tmp/test_vertex_table_resize_dump";
  if (std::filesystem::exists(dump_dir)) {
    std::filesystem::remove_all(dump_dir);
  }
  std::filesystem::create_directories(dump_dir);
  std::filesystem::create_directories(neug::checkpoint_dir(dump_dir));
  std::filesystem::create_directories(neug::temp_checkpoint_dir(dump_dir));
  table.Dump(neug::temp_checkpoint_dir(dump_dir));
  table.Close();

  std::filesystem::remove_all(neug::checkpoint_dir(dump_dir));
  std::filesystem::rename(neug::temp_checkpoint_dir(dump_dir),
                          neug::checkpoint_dir(dump_dir));
  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    new_table.Open(dump_dir, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 10000);
    EXPECT_EQ(new_table.LidNum(), 10000);
    EXPECT_EQ(new_table.get_vertex_timestamp().InitVertexNum(), 10000);
  }
}

TEST_F(VertexTableTest, VertexTimestampValidVertexNum) {
  std::string dump_dir = "/tmp/test_vertex_timestamp_valid_num";
  if (std::filesystem::exists(dump_dir)) {
    std::filesystem::remove_all(dump_dir);
  }
  std::filesystem::create_directories(dump_dir);
  neug::VertexTimestamp vts;
  vts.Open(dump_dir);
  vts.Init(100, 1000);
  vts.RemoveVertex(12);
  vts.RemoveVertex(0);
  vts.RemoveVertex(99);
  EXPECT_EQ(vts.ValidVertexNum(0, 100), 97);
  EXPECT_THROW(vts.RemoveVertex(101),
               neug::exception::InvalidArgumentException);
  EXPECT_THROW(vts.RemoveVertex(999),
               neug::exception::InvalidArgumentException);
  EXPECT_THROW(vts.RemoveVertex(1000),
               neug::exception::InvalidArgumentException);
  EXPECT_EQ(vts.ValidVertexNum(0, 1000), 97);
  for (neug::vid_t vid = 100; vid < 1000; ++vid) {
    vts.InsertVertex(vid, 1);
  }
  EXPECT_EQ(vts.ValidVertexNum(1, 1000), 997);
  for (neug::vid_t vid = 200; vid < 300; ++vid) {
    vts.RemoveVertex(vid);
  }
  EXPECT_EQ(vts.ValidVertexNum(1, 1000), 897);
  EXPECT_EQ(vts.ValidVertexNum(0, 100), 97);
  vts.RemoveVertex(50);
  EXPECT_EQ(vts.ValidVertexNum(0, 100), 96);
  EXPECT_EQ(vts.ValidVertexNum(1, 0), 0);
  EXPECT_EQ(vts.ValidVertexNum(1, 50), 48);
}

TEST_F(VertexTableTest, VertexSetForeachVertex) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  table.Open(dir_, memory_level_, true);
  table.Reserve(100);

  std::vector<neug::Property> oids;
  std::vector<neug::vid_t> lids;
  lids.resize(10);

  for (int64_t i = 0; i < 10; ++i) {
    neug::Property oid;
    oid.set_int64(i);
    oids.push_back(oid);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i));
  }

  for (size_t i = 0; i < lids.size(); i += 2) {
    table.DeleteVertex(lids[i], 20);
  }

  neug::VertexSet vset = table.GetVertexSet(20);
  size_t count = 0;
  vset.foreach_vertex([&](neug::vid_t vid) { count++; });
  EXPECT_EQ(count, 5);  // Only odd lids are valid at ts=20
}