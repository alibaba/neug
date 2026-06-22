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

#include "neug/execution/common/types/value.h"
#include "neug/main/neug_db.h"
#include "neug/storages/checkpoint_manager.h"
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
    memory_level_ = neug::MemoryLevel::kInMemory;
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
    property_values_ = {neug::execution::Value::STRING("Alice"),
                        neug::execution::Value::INT32(30),
                        neug::execution::Value::DOUBLE(88.5)};
    default_prop_values_ = {neug::execution::Value::STRING(""),
                            neug::execution::Value::INT32(0),
                            neug::execution::Value::DOUBLE(0.0)};
    vertex_count_ = 1000000;
    schema_.AddVertexLabel(v_label_name_, property_types_, property_names_,
                           {std::make_tuple(pk_type_, "id", 0)}, 4096, "",
                           default_prop_values_);
    v_label_id_ = schema_.get_vertex_label_id(v_label_name_);
    ws.Open(dir_);
  }

  neug::CheckpointManager& Workspace() { return ws; }

  void TearDown() override {
    // remove the directory if it exists
    if (std::filesystem::exists(dir_)) {
      std::filesystem::remove_all(dir_);
    }
  }

  std::vector<std::shared_ptr<neug::execution::DataChunk>> generate_data_chunks(
      size_t num_vertices) {
    std::vector<int64_t> oid_values;
    std::vector<std::string> name_values;
    std::vector<int32_t> age_values;
    std::vector<double> score_values;
    for (int64_t i = 0; i < static_cast<int64_t>(num_vertices); ++i) {
      oid_values.push_back(i);
      name_values.push_back("name_" + std::to_string(i));
      age_values.push_back(static_cast<int32_t>(20 + (i % 30)));
      score_values.push_back(50.0 + (i % 50));
    }
    auto oid_chunks = split_column_to_chunks(oid_values, 10);
    auto name_chunks = split_column_to_chunks(name_values, 10);
    auto age_chunks = split_column_to_chunks(age_values, 10);
    auto score_chunks = split_column_to_chunks(score_values, 10);
    return convert_to_data_chunks(
        {oid_chunks, name_chunks, age_chunks, score_chunks});
  }

  std::string dir_;
  neug::MemoryLevel memory_level_;
  std::string v_label_name_;
  neug::DataTypeId pk_type_;
  std::vector<std::string> property_names_;
  std::vector<neug::DataType> property_types_;
  std::vector<neug::execution::Value> property_values_;
  std::vector<neug::execution::Value> default_prop_values_;
  std::mt19937 generator_;
  neug::Schema schema_;
  neug::label_t v_label_id_ = 0;
  neug::CheckpointManager ws;

  size_t vertex_count_;
};

TEST_F(VertexTableTest, VertexTableBasicOps) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(vertex_count_);

  neug::vid_t lid1, lid2, lid3;
  auto oid1 = neug::execution::Value::INT64(1);
  auto oid2 = neug::execution::Value::INT64(2);
  auto oid3 = neug::execution::Value::INT64(3);
  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1, false));
  EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2, false));
  EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 3, false));
  LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
            << lid3;
  LOG(INFO) << "and oids: " << oid1.GetValue<int64_t>() << ", "
            << oid2.GetValue<int64_t>() << ", " << oid3.GetValue<int64_t>();

  EXPECT_EQ(table.VertexNum(), 3);
  EXPECT_EQ(table.LidNum(), 3);
  EXPECT_EQ(table.VertexNum(2), 2);
  EXPECT_EQ(table.VertexNum(1), 1);

  EXPECT_TRUE(table.IsValidLid(lid1));
  EXPECT_TRUE(table.IsValidLid(lid2));
  EXPECT_TRUE(table.IsValidLid(lid3));
  EXPECT_FALSE(table.IsValidLid(4));
  EXPECT_TRUE(table.IsValidLid(lid3, 3));

  EXPECT_EQ(oid1, table.GetOid(lid1, 1));
  EXPECT_EQ(oid2, table.GetOid(lid2, 2));
  EXPECT_EQ(oid3, table.GetOid(lid3, 3));

  try {
    auto ret = table.GetOid(3, 2);
    FAIL() << "Expected exception not thrown";
  } catch (neug::exception::Exception& e) {}

  neug::vid_t tmp_vid;
  EXPECT_FALSE(table.get_index(oid1, tmp_vid, 0));
  EXPECT_TRUE(table.get_index(oid1, tmp_vid, 1));

  table.Close();
}

TEST_F(VertexTableTest, VertexTableDumpAndReload) {
  auto ckp = make_checkpoint(Workspace());
  neug::CheckpointManifest desc;
  {
    neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
    OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(),
                          memory_level_);
    table.EnsureCapacity(vertex_count_);

    neug::vid_t lid1, lid2, lid3;
    auto oid1 = neug::execution::Value::INT64(1);
    auto oid2 = neug::execution::Value::INT64(2);
    auto oid3 = neug::execution::Value::INT64(3);
    EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1, false));
    EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2, false));
    EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 3, false));
    LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
              << lid3;
    desc = DumpVertexTableLegacy(table, *ckp);
  }

  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    OpenVertexTableLegacy(new_table, ckp, desc, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 3);
    EXPECT_EQ(new_table.LidNum(), 3);
    EXPECT_EQ(new_table.VertexNum(2), 3);
    EXPECT_EQ(new_table.VertexNum(1), 3);
    new_table.Close();
  }
}

TEST_F(VertexTableTest, VertexTableAddAndDeleteAndReload) {
  auto ckp = make_checkpoint(Workspace());

  neug::vid_t lid1, lid2, lid3;
  neug::execution::Value oid1, oid2, oid3;
  neug::CheckpointManifest desc;
  {
    neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
    OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(),
                          memory_level_);
    table.EnsureCapacity(vertex_count_);

    oid1 = neug::execution::Value::INT64(1);
    oid2 = neug::execution::Value::INT64(2);
    oid3 = neug::execution::Value::INT64(3);
    EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1, false));
    EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2, false));
    EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 3, false));
    LOG(INFO) << "Added vertices with lids: " << lid1 << ", " << lid2 << ", "
              << lid3;

    EXPECT_EQ(table.VertexNum(), 3);
    EXPECT_EQ(table.LidNum(), 3);

    desc = DumpVertexTableLegacy(table, *ckp);
  }

  neug::CheckpointManifest desc2;
  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    OpenVertexTableLegacy(new_table, ckp, desc, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 3);
    EXPECT_EQ(new_table.LidNum(), 3);

    new_table.BatchDeleteVertices({lid1, lid2});
    EXPECT_EQ(new_table.VertexNum(), 1);
    EXPECT_EQ(new_table.LidNum(), 3);

    neug::vid_t tmp_vid;
    EXPECT_FALSE(new_table.get_index(oid1, tmp_vid));
    EXPECT_FALSE(new_table.get_index(oid2, tmp_vid));
    EXPECT_TRUE(new_table.get_index(oid3, tmp_vid));

    desc2 = DumpVertexTableLegacy(new_table, *ckp);
  }

  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    OpenVertexTableLegacy(new_table, ckp, desc2, memory_level_);
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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  auto oid1 = neug::execution::Value::INT64(100);
  auto oid2 = neug::execution::Value::INT64(200);
  auto oid3 = neug::execution::Value::INT64(300);
  neug::vid_t lid1, lid2, lid3;
  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 0, false));
  EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 1, false));
  EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 2, false));

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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);

  // AddVertex must return false on an opened table whose capacity is still 0
  // (no EnsureCapacity call yet).
  neug::vid_t tmp_vid;
  EXPECT_FALSE(table.AddVertex(neug::execution::Value::INT64(1),
                               property_values_, tmp_vid, 0, false));

  std::vector<neug::execution::Value> oids;
  std::vector<neug::vid_t> lids;
  table.EnsureCapacity(100);
  lids.resize(100);

  for (int64_t i = 0; i < 100; ++i) {
    auto oid = neug::execution::Value::INT64(i);
    oids.push_back(oid);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i % 10, false));
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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  auto oid1 = neug::execution::Value::INT64(1);
  auto oid2 = neug::execution::Value::INT64(2);
  auto oid3 = neug::execution::Value::INT64(3);
  neug::vid_t lid1, lid2, lid3;

  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1, false));
  EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2, false));
  EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 3, false));

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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  auto oid1 = neug::execution::Value::INT64(1);
  neug::vid_t lid1;
  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1, false));

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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  std::vector<neug::execution::Value> oids;
  std::vector<neug::vid_t> lids;
  lids.resize(10);

  for (int64_t i = 0; i < 10; ++i) {
    auto oid = neug::execution::Value::INT64(i);
    oids.push_back(oid);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i, false));
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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  auto oid = neug::execution::Value::INT64(42);
  neug::vid_t lid;
  EXPECT_TRUE(table.AddVertex(oid, property_values_, lid, 1, false));

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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(50);

  std::vector<neug::vid_t> lids;
  lids.resize(20);

  // Add using both methods alternately
  for (int64_t i = 0; i < 20; ++i) {
    auto oid = neug::execution::Value::INT64(i);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i, false));
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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  auto oid1 = neug::execution::Value::INT64(1);
  auto oid2 = neug::execution::Value::INT64(2);
  auto oid3 = neug::execution::Value::INT64(3);

  EXPECT_EQ(table.VertexNum(0), 0);
  neug::vid_t lid1;
  EXPECT_TRUE(table.AddVertex(oid1, property_values_, lid1, 1, false));
  EXPECT_TRUE(table.IsValidLid(lid1, 1));
  EXPECT_EQ(table.VertexNum(1), 1);

  neug::vid_t lid2;
  EXPECT_TRUE(table.AddVertex(oid2, property_values_, lid2, 2, false));
  EXPECT_TRUE(table.IsValidLid(lid1, 2));
  EXPECT_TRUE(table.IsValidLid(lid2, 2));
  EXPECT_EQ(table.VertexNum(2), 2);  // oid2,

  table.DeleteVertex(lid1, 3);
  EXPECT_EQ(table.VertexNum(3), 1);  // oid2,
  EXPECT_FALSE(table.IsValidLid(lid1, 3));
  neug::vid_t lid3;
  EXPECT_TRUE(table.AddVertex(oid3, property_values_, lid3, 4, false));
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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  auto oid = neug::execution::Value::INT64(1);
  neug::vid_t lid;
  EXPECT_TRUE(table.AddVertex(oid, property_values_, lid, 1, false));

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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  auto oid = neug::execution::Value::INT64(1);
  neug::vid_t lid;
  EXPECT_TRUE(table.AddVertex(oid, property_values_, lid, 1, false));

  EXPECT_EQ(table.VertexNum(), 1);

  // Try to revert a non-deleted vertex - should log warning
  table.RevertDeleteVertex(lid, 2);
  EXPECT_EQ(table.VertexNum(), 1);
  EXPECT_TRUE(table.IsValidLid(lid, 2));
}

// Test complex combination with dump and reload
TEST_F(VertexTableTest, ComplexAddDeleteRevertDumpReload) {
  std::vector<neug::execution::Value> oids;
  std::vector<neug::vid_t> lids;
  lids.resize(20);

  auto ckp = make_checkpoint(Workspace());
  // Create complex state
  neug::CheckpointManifest desc;
  {
    neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
    OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(),
                          memory_level_);
    table.EnsureCapacity(100);

    for (int64_t i = 0; i < 20; ++i) {
      auto oid = neug::execution::Value::INT64(i);
      oids.push_back(oid);
      EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i, false));
    }

    for (size_t i = 5; i < 15; ++i) {
      table.DeleteVertex(lids[i], 50);
    }

    for (size_t i = 5; i < 10; ++i) {
      table.RevertDeleteVertex(lids[i], 60);
    }

    EXPECT_EQ(table.VertexNum(), 15);  // 5 + 5 + 5 (0-4, 5-9 reverted, 15-19)

    desc = DumpVertexTableLegacy(table, *ckp);
  }

  // Reload and verify
  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    OpenVertexTableLegacy(new_table, ckp, desc, memory_level_);

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
}

// Test stress: Many add/delete/revert operations
TEST_F(VertexTableTest, StressAddDeleteRevert) {
  neug::VertexTable table(schema_.get_vertex_schema(v_label_id_));
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(1000);

  std::vector<neug::execution::Value> oids;
  std::vector<neug::vid_t> lids;
  lids.resize(100);

  for (int64_t i = 0; i < 100; ++i) {
    auto oid = neug::execution::Value::INT64(i);
    oids.push_back(oid);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], 1, false));
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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  auto data_chunks = generate_data_chunks(10000);
  std::shared_ptr<neug::IDataChunkSupplier> batch_supplier =
      std::make_shared<GeneratedChunkSupplier>(std::move(data_chunks));
  table.insert_vertices(batch_supplier);

  EXPECT_EQ(table.VertexNum(), 10000);
  EXPECT_EQ(table.LidNum(), 10000);
  table.Compact(true);
  EXPECT_EQ(table.get_vertex_timestamp().InitVertexNum(), 10000);

  auto desc = DumpVertexTableLegacy(table, *ckp);
  table.Close();

  {
    neug::VertexTable new_table(schema_.get_vertex_schema(v_label_id_));
    OpenVertexTableLegacy(new_table, ckp, desc, memory_level_);
    EXPECT_EQ(new_table.VertexNum(), 10000);
    EXPECT_EQ(new_table.LidNum(), 10000);
    EXPECT_EQ(new_table.get_vertex_timestamp().InitVertexNum(), 10000);
  }
}

TEST_F(VertexTableTest, VertexTimestampValidVertexNum) {
  auto ckp = make_checkpoint(Workspace());
  neug::VertexTimestamp vts;
  vts.Open(*ckp, neug::ModuleDescriptor(), memory_level_);
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
  auto ckp = make_checkpoint(Workspace());
  OpenVertexTableLegacy(table, ckp, neug::CheckpointManifest(), memory_level_);
  table.EnsureCapacity(100);

  std::vector<neug::execution::Value> oids;
  std::vector<neug::vid_t> lids;
  lids.resize(10);

  for (int64_t i = 0; i < 10; ++i) {
    auto oid = neug::execution::Value::INT64(i);
    oids.push_back(oid);
    EXPECT_TRUE(table.AddVertex(oid, property_values_, lids[i], i, false));
  }

  for (size_t i = 0; i < lids.size(); i += 2) {
    table.DeleteVertex(lids[i], 20);
  }

  neug::VertexSet vset = table.GetVertexSet(20);
  size_t count = 0;
  vset.foreach_vertex([&](neug::vid_t vid) { count++; });
  EXPECT_EQ(count, 5);  // Only odd lids are valid at ts=20
}
