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
#include "neug/storages/graph/vertex_table.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/property/types.h"

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
    pk_type_ = gs::PropertyType::kInt64;

    property_names_ = {"name", "age", "score"};
    property_types_ = {gs::PropertyType::kStringView, gs::PropertyType::kInt32,
                       gs::PropertyType::kDouble};
    storage_strategies_ = {gs::StorageStrategy::kMem, gs::StorageStrategy::kMem,
                           gs::StorageStrategy::kMem};

    // Initialize random number generator
    generator_.seed(42);  // Fixed seed for reproducible results
  }

  void TearDown() override {
    // Clean up test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  void CreateAndOpenVertexTable(gs::VertexTable& table) {
    // Open the vertex table
    table.Open(test_dir_, 1, true);  // memory_level=1 for in-memory
  }

  void AddVerticesWithProperties(gs::VertexTable& table, size_t count) {
    table.Reserve(count);
    std::uniform_int_distribution<int> age_dist(18, 80);
    std::uniform_real_distribution<double> score_dist(0.0, 100.0);

    for (size_t i = 0; i < count; ++i) {
      // Add vertex with integer ID
      gs::Property vertex_id;
      vertex_id.set_int64(static_cast<int64_t>(i));

      gs::vid_t vid = table.add_vertex(vertex_id, i);
      EXPECT_EQ(vid, i);

      // Set properties
      auto& props_table = table.get_properties_table();

      // Set name property
      gs::Property name_value;
      name_value.set_string_view("person_" + std::to_string(i));
      props_table.get_column_by_id(0)->set_any(vid, name_value);

      // Set age property
      gs::Property age_value;
      age_value.set_int32(age_dist(generator_));
      props_table.get_column_by_id(1)->set_any(vid, age_value);

      // Set score property
      gs::Property score_value;
      score_value.set_double(score_dist(generator_));
      props_table.get_column_by_id(2)->set_any(vid, score_value);
    }
  }

  void BatchDeleteVertices(gs::VertexTable& table, size_t delete_count) {
    size_t current_vertex_num = table.vertex_num();
    if (delete_count > current_vertex_num) {
      delete_count = current_vertex_num;
    }

    std::vector<gs::vid_t> vids_to_delete;
    std::uniform_int_distribution<gs::vid_t> vid_dist(0,
                                                      current_vertex_num - 1);
    std::unordered_set<gs::vid_t> unique_vids;

    while (unique_vids.size() < delete_count) {
      gs::vid_t vid = vid_dist(generator_);
      if (table.is_valid_lid(vid, gs::MAX_TIMESTAMP)) {
        unique_vids.insert(vid);
      }
    }

    vids_to_delete.assign(unique_vids.begin(), unique_vids.end());
    table.BatchDeleteVertices(vids_to_delete);
  }

  std::vector<gs::vid_t> GenerateRandomVertexIds(
      const gs::VertexTable& vertex_table, size_t count, size_t max_id) {
    std::vector<gs::vid_t> ids;
    std::uniform_int_distribution<gs::vid_t> id_dist(0, max_id - 1);

    while (ids.size() < count) {
      auto vid = id_dist(generator_);
      if (vertex_table.is_valid_lid(vid)) {
        ids.push_back(vid);
      }
    }
    return ids;
  }

  std::vector<gs::Property> GenerateRandomOids(
      const gs::VertexTable& vertex_table, size_t count, size_t max_id) {
    std::vector<gs::Property> oids;
    std::uniform_int_distribution<int64_t> oid_dist(0, max_id - 1);

    gs::vid_t lid;
    while (oids.size() < count) {
      gs::Property oid;
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
  gs::PropertyType pk_type_;
  std::vector<std::string> property_names_;
  std::vector<gs::PropertyType> property_types_;
  std::vector<gs::StorageStrategy> storage_strategies_;
  std::mt19937 generator_;
};

TEST_F(VertexTableBenchmark, AddVertexPerformance) {
  const size_t vertex_count = 1000000;

  gs::VertexTable table(v_label_name_, pk_type_, property_names_,
                        property_types_, storage_strategies_);
  CreateAndOpenVertexTable(table);
  table.Reserve(vertex_count);

  auto start = std::chrono::high_resolution_clock::now();

  // Add vertices
  for (size_t i = 0; i < vertex_count; ++i) {
    gs::Property vertex_id;
    vertex_id.set_int64(static_cast<int64_t>(i));
    table.add_vertex(vertex_id);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  LOG(INFO) << "Added " << vertex_count << " vertices in " << duration.count()
            << " microseconds";
  LOG(INFO) << "Average time per vertex: "
            << (double) duration.count() / vertex_count << " microseconds";

  EXPECT_EQ(table.vertex_num(), vertex_count);

  table.Close();
}

TEST_F(VertexTableBenchmark, GetOidPerformance) {
  const size_t vertex_count = 100000000;
  const size_t lookup_count = 25000000;

  gs::VertexTable table(v_label_name_, pk_type_, property_names_,
                        property_types_, storage_strategies_);

  CreateAndOpenVertexTable(table);
  AddVerticesWithProperties(table, vertex_count);

  // Generate random vertex IDs for lookup
  auto random_vids = GenerateRandomVertexIds(table, lookup_count, vertex_count);

  // Warm up
  for (auto vid : random_vids) {
    gs::Property oid = table.get_oid(vid, gs::MAX_TIMESTAMP);
    (void) oid;  // Avoid unused variable warning
  }

  auto start = std::chrono::high_resolution_clock::now();

  // Perform get_oid operations
  for (auto vid : random_vids) {
    gs::Property oid = table.get_oid(vid);
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
    gs::Property oid = table.get_oid(vid);
    (void) oid;  // Avoid unused variable warning
  }
  auto end2 = std::chrono::high_resolution_clock::now();
  auto duration2 =
      std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2);

  LOG(INFO) << "Performed " << lookup_count << " get_oid operations in "
            << duration.count() << " microseconds";
  LOG(INFO) << "Average time per get_oid: "
            << (double) duration.count() / lookup_count << " microseconds";
  LOG(INFO) << "After deletion, performed " << lookup_count
            << " get_oid operations in " << duration2.count()
            << " microseconds";
  LOG(INFO) << "Average time per get_oid after deletion: "
            << (double) duration2.count() / lookup_count << " microseconds";

  table.Close();
}

TEST_F(VertexTableBenchmark, GetIndexPerformance) {
  const size_t vertex_count = 100000000;
  const size_t lookup_count = 25000000;

  gs::VertexTable table(v_label_name_, pk_type_, property_names_,
                        property_types_, storage_strategies_);

  CreateAndOpenVertexTable(table);
  AddVerticesWithProperties(table, vertex_count);

  // Generate random OIDs for lookup
  auto random_oids = GenerateRandomOids(table, lookup_count, vertex_count);
  LOG(INFO) << "Generated " << random_oids.size() << " random OIDs";

  // Warm up
  for (const auto& oid : random_oids) {
    gs::vid_t lid;
    bool found = table.get_index(oid, lid, gs::MAX_TIMESTAMP);
    (void) found;  // Avoid unused variable warning
    (void) lid;    // Avoid unused variable warning
  }
  LOG(INFO) << "Warm-up completed";

  auto start = std::chrono::high_resolution_clock::now();

  // Perform get_index operations
  for (const auto& oid : random_oids) {
    gs::vid_t lid;
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
    gs::vid_t lid;
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

  gs::VertexTable table(v_label_name_, pk_type_, property_names_,
                        property_types_, storage_strategies_);

  CreateAndOpenVertexTable(table);
  AddVerticesWithProperties(table, vertex_count);

  {
    auto vertex_set =
        gs::VertexSet(table.lid_num(), table.get_vertex_timestamps(),
                      gs::MAX_TIMESTAMP, table.vertex_table_modified());

    auto start = std::chrono::high_resolution_clock::now();

    gs::vid_t res = 0;
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
    auto vertex_set =
        gs::VertexSet(table.lid_num(), table.get_vertex_timestamps(),
                      gs::MAX_TIMESTAMP, table.vertex_table_modified());

    auto start = std::chrono::high_resolution_clock::now();

    gs::vid_t res = 0;
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

  gs::VertexTable table(v_label_name_, pk_type_, property_names_,
                        property_types_, storage_strategies_);

  CreateAndOpenVertexTable(table);
  AddVerticesWithProperties(table, vertex_count);

  // Generate random data for mixed operations
  auto random_vids =
      GenerateRandomVertexIds(table, operation_count, vertex_count);
  auto random_oids = GenerateRandomOids(table, operation_count, vertex_count);

  auto start = std::chrono::high_resolution_clock::now();

  // Mixed operations: get_oid and get_index in random order
  std::uniform_int_distribution<int> op_dist(0, 1);

  for (size_t i = 0; i < operation_count; ++i) {
    int op = op_dist(generator_);

    switch (op) {
    case 0: {
      // get_oid operation
      gs::Property oid = table.get_oid(random_vids[i]);
      (void) oid;
      break;
    }
    case 1: {
      // get_index operation
      gs::vid_t lid;
      bool found = table.get_index(random_oids[i], lid, gs::MAX_TIMESTAMP);
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