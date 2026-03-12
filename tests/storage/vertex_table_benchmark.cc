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

#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/property/types.h"
#include "unittest/utils.h"

class VertexTableBenchmark : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/vertex_table_benchmark_test";

    // Clean up and create test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);

    // Setup vertex table with three properties
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
    pk_types_ = {{neug::DataTypeId::kVarchar, "name", 0}};
    description = "Person vertex label";
    v_schema_ = std::make_shared<neug::VertexSchema>(
        v_label_name_, property_types_, property_names_, pk_types_,
        storage_strategies_, default_prop_values_, description);
    // Initialize random number generator
    generator_.seed(42);  // Fixed seed for reproducible results
  }

  void TearDown() override {
    // Clean up test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  void CreateAndOpenVertexTable(neug::VertexTable& table) {
    // Open the vertex table
    table.Open(test_dir_, 1, true);  // memory_level=1 for in-memory
  }

  void AddVerticesWithProperties(neug::VertexTable& table, size_t count) {
    table.Reserve(count);
    std::uniform_int_distribution<int> age_dist(18, 80);
    std::uniform_real_distribution<double> score_dist(0.0, 100.0);

    for (size_t i = 0; i < count; ++i) {
      // Add vertex with integer ID
      neug::Property vertex_id;
      vertex_id.set_int64(static_cast<int64_t>(i));

      neug::vid_t vid;
      EXPECT_TRUE(table.AddVertex(vertex_id, property_values_, vid, i));
      EXPECT_EQ(vid, i);
      if (i % (count / 100) == 0) {
        LOG(INFO) << "Added " << i << " vertices so far...";
      }
    }
  }

  void BulkLoadVertices(
      neug::VertexTable& table,
      std::shared_ptr<neug::IRecordBatchSupplier> batch_supplier) {
    table.insert_vertices(batch_supplier);
  }

  void BatchDeleteVertices(neug::VertexTable& table, size_t delete_count) {
    size_t current_vertex_num = table.VertexNum();
    if (delete_count > current_vertex_num) {
      delete_count = current_vertex_num;
    }

    std::vector<neug::vid_t> vids_to_delete;
    std::uniform_int_distribution<neug::vid_t> vid_dist(0,
                                                        current_vertex_num - 1);
    std::unordered_set<neug::vid_t> unique_vids;

    while (unique_vids.size() < delete_count) {
      neug::vid_t vid = vid_dist(generator_);
      if (table.IsValidLid(vid, neug::MAX_TIMESTAMP)) {
        unique_vids.insert(vid);
      }
    }

    vids_to_delete.assign(unique_vids.begin(), unique_vids.end());
    table.BatchDeleteVertices(vids_to_delete);
  }

  std::vector<neug::vid_t> GenerateRandomVertexIds(
      const neug::VertexTable& vertex_table, size_t count, size_t max_id) {
    std::vector<neug::vid_t> ids;
    std::uniform_int_distribution<neug::vid_t> id_dist(0, max_id - 1);

    while (ids.size() < count) {
      auto vid = id_dist(generator_);
      if (vertex_table.IsValidLid(vid)) {
        ids.push_back(vid);
      }
    }
    return ids;
  }

  std::vector<neug::Property> GenerateRandomOids(
      const neug::VertexTable& vertex_table, size_t count, size_t max_id) {
    std::vector<neug::Property> oids;
    std::uniform_int_distribution<int64_t> oid_dist(0, max_id - 1);

    neug::vid_t lid;
    while (oids.size() < count) {
      neug::Property oid;
      auto oid_value = oid_dist(generator_);
      oid.set_int64(oid_value);
      if (vertex_table.get_index(oid, lid)) {
        oids.push_back(oid);
        (void) lid;  // Avoid unused variable warning
      }
      if (oids.size() % (count / 10) == 0) {
        LOG(INFO) << "Generated " << oids.size() << " OIDs so far...";
      }
    }

    return oids;
  }

 protected:
  std::string test_dir_;
  std::string v_label_name_;
  neug::DataTypeId pk_type_;
  std::vector<std::string> property_names_;
  std::vector<neug::DataType> property_types_;
  std::vector<neug::Property> property_values_;
  std::vector<neug::StorageStrategy> storage_strategies_;
  std::vector<neug::Property> default_prop_values_;
  std::shared_ptr<neug::VertexSchema> v_schema_;
  std::vector<std::tuple<neug::DataType, std::string, size_t>> pk_types_;
  std::vector<std::shared_ptr<neug::ExtraTypeInfo>> property_extra_infos_;
  std::string description;
  std::mt19937 generator_;
};

TEST_F(VertexTableBenchmark, AddVertexPerformance) {
  const size_t vertex_count = 1000000;

  neug::VertexTable table(v_schema_);
  CreateAndOpenVertexTable(table);
  table.Reserve(vertex_count);

  auto start = std::chrono::high_resolution_clock::now();

  // Add vertices
  for (size_t i = 0; i < vertex_count; ++i) {
    neug::Property vertex_id;
    vertex_id.set_int64(static_cast<int64_t>(i));
    neug::vid_t vid;
    EXPECT_TRUE(table.AddVertex(vertex_id, property_values_, vid, i));
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  LOG(INFO) << "Added " << vertex_count << " vertices in " << duration.count()
            << " microseconds";
  LOG(INFO) << "Average time per vertex: "
            << (double) duration.count() / vertex_count << " microseconds";

  EXPECT_EQ(table.VertexNum(), vertex_count);

  table.Close();
}

TEST_F(VertexTableBenchmark, GetOidPerformance) {
  const size_t vertex_count = 100000000;
  const size_t lookup_count = 25000000;

  neug::VertexTable table(v_schema_);

  CreateAndOpenVertexTable(table);
  LOG(INFO) << "Finish Open table";
  auto load_start = std::chrono::high_resolution_clock::now();
  AddVerticesWithProperties(table, vertex_count);
  auto load_end = std::chrono::high_resolution_clock::now();
  LOG(INFO) << "Finish add vertices with properties: cost "
            << std::chrono::duration_cast<std::chrono::seconds>(load_end -
                                                                load_start)
                   .count()
            << " seconds";

  // Generate random vertex IDs for lookup
  auto random_vids = GenerateRandomVertexIds(table, lookup_count, vertex_count);
  LOG(INFO) << "Generated " << random_vids.size() << " random vertex IDs";

  // Warm up
  for (auto vid : random_vids) {
    neug::Property oid = table.GetOid(vid, neug::MAX_TIMESTAMP);
    (void) oid;  // Avoid unused variable warning
  }
  LOG(INFO) << "Warm-up completed";

  auto start = std::chrono::high_resolution_clock::now();

  // Perform GetOid operations
  for (auto vid : random_vids) {
    neug::Property oid = table.GetOid(vid);
    (void) oid;  // Avoid unused variable warning
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  // Delete vertices
  BatchDeleteVertices(table, 25000);
  LOG(INFO) << "Deleted 25 thousand vertices";
  auto random_vids_after_delete =
      GenerateRandomVertexIds(table, lookup_count, vertex_count);

  auto start2 = std::chrono::high_resolution_clock::now();

  for (auto vid : random_vids_after_delete) {
    neug::Property oid = table.GetOid(vid);
    (void) oid;  // Avoid unused variable warning
  }
  auto end2 = std::chrono::high_resolution_clock::now();
  auto duration2 =
      std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2);

  LOG(INFO) << "Performed " << lookup_count << " GetOid operations in "
            << duration.count() << " microseconds";
  LOG(INFO) << "Average time per GetOid: "
            << (double) duration.count() / lookup_count << " microseconds";
  LOG(INFO) << "After deletion, performed " << lookup_count
            << " GetOid operations in " << duration2.count() << " microseconds";
  LOG(INFO) << "Average time per GetOid after deletion: "
            << (double) duration2.count() / lookup_count << " microseconds";

  table.Close();
}

TEST_F(VertexTableBenchmark, GetIndexPerformance) {
  const size_t vertex_count = 100000000;
  const size_t lookup_count = 25000000;

  neug::VertexTable table(v_schema_);

  CreateAndOpenVertexTable(table);
  AddVerticesWithProperties(table, vertex_count);

  // Generate random OIDs for lookup
  auto random_oids = GenerateRandomOids(table, lookup_count, vertex_count);
  LOG(INFO) << "Generated " << random_oids.size() << " random OIDs";

  // Warm up
  for (const auto& oid : random_oids) {
    neug::vid_t lid;
    bool found = table.get_index(oid, lid, neug::MAX_TIMESTAMP);
    (void) found;  // Avoid unused variable warning
    (void) lid;    // Avoid unused variable warning
  }
  LOG(INFO) << "Warm-up completed";

  auto start = std::chrono::high_resolution_clock::now();

  // Perform get_index operations
  for (const auto& oid : random_oids) {
    neug::vid_t lid;
    bool found = table.get_index(oid, lid);
    (void) found;  // Avoid unused variable warning
    (void) lid;    // Avoid unused variable warning
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  // Delete vertices
  BatchDeleteVertices(table, 25000);  // Delete 25 thousand vertices
  LOG(INFO) << "Deleted 25 thousand vertices";

  auto start2 = std::chrono::high_resolution_clock::now();

  for (const auto& oid : random_oids) {
    neug::vid_t lid;
    bool found = table.get_index(oid, lid);
    (void) found;  // Avoid unused variable warning
    (void) lid;    // Avoid unused variable warning
  }

  auto end2 = std::chrono::high_resolution_clock::now();
  auto duration2 =
      std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2);

  LOG(INFO) << "Performed " << lookup_count << " get_index operations in "
            << duration.count() << " microseconds";
  LOG(INFO) << "Average time per get_index: "
            << (double) duration.count() / lookup_count << " microseconds";
  LOG(INFO) << "After deletion, performed " << lookup_count
            << " get_index operations in " << duration2.count()
            << " microseconds";
  LOG(INFO) << "Average time per get_index after deletion: "
            << (double) duration2.count() / lookup_count << " microseconds";

  table.Close();
}

// Test the performance of VertexSet from readTransaction
TEST_F(VertexTableBenchmark, VertexSetPerformance) {
  const size_t vertex_count = 100000000;

  neug::VertexTable table(v_schema_);

  CreateAndOpenVertexTable(table);
  AddVerticesWithProperties(table, vertex_count);

  {
    auto vertex_set = table.GetVertexSet(neug::MAX_TIMESTAMP);

    auto start = std::chrono::high_resolution_clock::now();

    neug::vid_t res = 0;
    for (auto vid : vertex_set) {
      res = res | vid;
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    LOG(INFO) << "Iterated over " << vertex_count << " vertices in "
              << duration.count() << " microseconds";
    LOG(INFO) << "Average time per vertex iteration: "
              << (double) duration.count() / vertex_count << " microseconds";
    LOG(INFO) << "Dummy result to prevent optimization: " << res;
  }
  BatchDeleteVertices(table, 25000);
  {
    auto vertex_set = table.GetVertexSet(neug::MAX_TIMESTAMP);

    auto start = std::chrono::high_resolution_clock::now();

    neug::vid_t res = 0;
    for (auto vid : vertex_set) {
      res = res | vid;
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    LOG(INFO) << "After deletion, iterated over " << (vertex_count - 25000000)
              << " vertices in " << duration.count() << " microseconds";
    LOG(INFO) << "Average time per vertex iteration after deletion: "
              << (double) duration.count() / (vertex_count - 25000000)
              << " microseconds";
    LOG(INFO) << "Dummy result to prevent optimization: " << res;
  }

  table.Close();
}

TEST_F(VertexTableBenchmark, MixedOperationsPerformance) {
  const size_t vertex_count = 100000000;
  const size_t operation_count = 25000000;

  neug::VertexTable table(v_schema_);

  CreateAndOpenVertexTable(table);
  AddVerticesWithProperties(table, vertex_count);

  // Generate random data for mixed operations
  auto random_vids =
      GenerateRandomVertexIds(table, operation_count, vertex_count);
  auto random_oids = GenerateRandomOids(table, operation_count, vertex_count);

  auto start = std::chrono::high_resolution_clock::now();

  // Mixed operations: GetOid and get_index in random order
  std::uniform_int_distribution<int> op_dist(0, 1);

  for (size_t i = 0; i < operation_count; ++i) {
    int op = op_dist(generator_);

    switch (op) {
    case 0: {
      // GetOid operation
      neug::Property oid = table.GetOid(random_vids[i]);
      (void) oid;
      break;
    }
    case 1: {
      // get_index operation
      neug::vid_t lid;
      bool found = table.get_index(random_oids[i], lid, neug::MAX_TIMESTAMP);
      (void) found;
      (void) lid;
      break;
    }
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  LOG(INFO) << "Performed " << operation_count << " mixed operations in "
            << duration.count() << " microseconds";
  LOG(INFO) << "Average time per mixed operation: "
            << (double) duration.count() / operation_count << " microseconds";

  table.Close();
}

TEST_F(VertexTableBenchmark, BulkLoadTest) {
  const size_t vertex_count = 100000000;

  neug::VertexTable table(v_schema_);

  CreateAndOpenVertexTable(table);

  // Bulk load vertices
  std::vector<int64_t> oid_values;
  std::vector<std::string> name_values;
  std::vector<int32_t> age_values;
  std::vector<double> score_values;
  for (int64_t i = 0; i < vertex_count; ++i) {
    oid_values.push_back(i);
    name_values.push_back("name_" + std::to_string(i));
    age_values.push_back(static_cast<int32_t>(20 + (i % 30)));
    score_values.push_back(50.0 + (i % 50));
  }
  auto oid_array = convert_to_arrow_arrays(oid_values, 100);
  auto name_array = convert_to_arrow_arrays(name_values, 100);
  auto age_array = convert_to_arrow_arrays(age_values, 100);
  auto score_array = convert_to_arrow_arrays(score_values, 100);
  auto record_batches = convert_to_record_batches(
      {"id", "name", "age", "score"},
      {oid_array, name_array, age_array, score_array});
  std::shared_ptr<neug::IRecordBatchSupplier> batch_supplier =
      std::make_shared<GeneratedRecordBatchSupplier>(std::move(record_batches));

  auto start = std::chrono::high_resolution_clock::now();
  BulkLoadVertices(table, batch_supplier);
  auto end = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  LOG(INFO) << "Bulk loaded " << vertex_count << " vertices in "
            << duration.count() << " seconds";

  EXPECT_EQ(table.VertexNum(), vertex_count);

  table.Close();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  std::cout << "=== VertexTable Performance Benchmark ===" << std::endl;
  std::cout << "Testing VertexTable with three properties (name, age, score)"
            << std::endl;
  std::cout
      << "All operations use random access patterns for realistic benchmarking"
      << std::endl;
  std::cout << std::endl;

  return RUN_ALL_TESTS();
}