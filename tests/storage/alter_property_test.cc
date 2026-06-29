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
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stddef.h>
#include <stdint.h>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/io/read/csv/csv_read_config.h"
#include "neug/utils/property/types.h"
#include "unittest/utils.h"

namespace neug {

void testLoadVertexBatch(PropertyGraph& graph, std::string vertex_type_name,
                         std::string& v_file, char delimiter, bool skip_head,
                         int32_t batch_size,
                         std::vector<std::string>& null_values) {
  label_t v_label = graph.schema().get_vertex_label_id(vertex_type_name);
  CsvReadConfig config;
  put_boolean_option(config);
  config.delimiter = delimiter;
  config.skip_rows = skip_head ? 1 : 0;
  config.escaping = false;
  config.quoting = false;
  config.chunk_size = batch_size;
  config.null_values = null_values;

  auto property_size = graph.schema().get_vertex_properties(v_label).size();
  config.column_names.resize(property_size + 1);
  for (size_t i = 0; i < config.column_names.size(); ++i) {
    config.column_names[i] = std::string("f") + std::to_string(i);
  }

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;
  auto primary_keys = graph.schema().get_vertex_primary_key(v_label);
  auto primary_key = primary_keys[0];
  auto primary_key_name = std::get<1>(primary_key);
  auto primary_key_ind = std::get<2>(primary_key);
  auto property_names = graph.schema().get_vertex_property_names(v_label);
  property_names.insert(property_names.begin() + primary_key_ind,
                        primary_key_name);
  for (size_t i = 0; i < config.column_names.size(); ++i) {
    included_col_names.emplace_back(config.column_names[i]);
    LOG(INFO) << "Emplace property name " << property_names[i] << "\n";
    mapped_property_names.emplace_back(property_names[i]);
  }
  LOG(INFO) << "property_names size is " << property_names.size();
  config.include_columns = included_col_names;

  {
    auto property_types = graph.schema().get_vertex_properties(v_label);
    auto property_names = graph.schema().get_vertex_property_names(v_label);
    EXPECT_TRUE(property_types.size() == property_names.size());

    for (size_t i = 0; i < property_types.size(); ++i) {
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (size_t j = 0; j < mapped_property_names.size(); ++j) {
        if (mapped_property_names[j] == property_name) {
          ind = j;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << property_name
                   << " does not exist in the vertex column mapping for "
                      "vertex label: "
                   << graph.schema().get_vertex_label_name(v_label)
                   << " please "
                      "check your configuration";
      }
      config.column_types.insert({included_col_names[ind], property_type});
    }
    {
      auto primary_key_name = std::get<1>(primary_key);
      auto primary_key_type = std::get<0>(primary_key);
      size_t ind = mapped_property_names.size();
      for (size_t i = 0; i < mapped_property_names.size(); ++i) {
        LOG(INFO) << "Find mapped property name " << mapped_property_names[i];
        if (mapped_property_names[i] == primary_key_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << primary_key_name
                   << " does not exist in the vertex column mapping, please "
                      "check your configuration";
      }
      config.column_types.insert({included_col_names[ind], primary_key_type});
    }
  }
  auto supplier = std::make_shared<CSVChunkSupplier>(v_file, std::move(config));
  CHECK(graph.BatchAddVertices(v_label, supplier).ok());
}

void testLoadEdgeBatch(PropertyGraph& graph, std::string src_vertex_type,
                       std::string dst_vertex_type, std::string edge_type,
                       std::string& e_file, char delimiter, bool skip_head,
                       int32_t batch_size,
                       std::vector<std::string>& null_values) {
  label_t src_label_id = graph.schema().get_vertex_label_id(src_vertex_type);
  label_t dst_label_id = graph.schema().get_vertex_label_id(dst_vertex_type);
  label_t e_label_id = graph.schema().get_edge_label_id(edge_type);
  CsvReadConfig config;
  put_boolean_option(config);
  config.delimiter = delimiter;
  config.skip_rows = skip_head ? 1 : 0;
  config.escaping = false;
  config.quoting = false;
  config.chunk_size = batch_size;
  config.null_values = null_values;

  auto edge_prop_names = graph.schema().get_edge_property_names(
      src_label_id, dst_label_id, e_label_id);

  config.column_names.resize(edge_prop_names.size() + 2);
  for (size_t i = 0; i < config.column_names.size(); ++i) {
    config.column_names[i] = std::string("f") + std::to_string(i);
  }

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;
  included_col_names.emplace_back(config.column_names[0]);
  included_col_names.emplace_back(config.column_names[1]);
  for (size_t i = 0; i < edge_prop_names.size(); ++i) {
    auto property_name = edge_prop_names[i];
    included_col_names.emplace_back(config.column_names[i + 2]);
    mapped_property_names.emplace_back(property_name);
  }
  config.include_columns = included_col_names;

  {
    auto property_types = graph.schema().get_edge_properties(
        src_label_id, dst_label_id, e_label_id);
    auto property_names = graph.schema().get_edge_property_names(
        src_label_id, dst_label_id, e_label_id);
    EXPECT_TRUE(property_types.size() == property_names.size());

    for (size_t i = 0; i < property_types.size(); ++i) {
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (size_t j = 0; j < mapped_property_names.size(); ++j) {
        if (mapped_property_names[j] == property_name) {
          ind = j;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << property_name
                   << " does not exist in the vertex column mapping, please "
                      "check your configuration";
      }
      config.column_types.insert({included_col_names[ind + 2], property_type});
    }
    {
      auto src_primary_keys =
          graph.schema().get_vertex_primary_key(src_label_id);
      EXPECT_TRUE(src_primary_keys.size() == 1);
      auto src_col_type = std::get<0>(src_primary_keys[0]);
      config.column_types.insert({config.column_names[0], src_col_type});

      auto dst_primary_keys =
          graph.schema().get_vertex_primary_key(dst_label_id);
      EXPECT_TRUE(dst_primary_keys.size() == 1);
      auto dst_col_type = std::get<0>(dst_primary_keys[0]);
      config.column_types.insert({config.column_names[1], dst_col_type});
    }
  }
  auto supplier = std::make_shared<CSVChunkSupplier>(e_file, std::move(config));
  CHECK(graph.BatchAddEdges(src_label_id, dst_label_id, e_label_id, supplier)
            .ok());
}

void testOpenEmptyGraph(std::shared_ptr<neug::Checkpoint> ckp,
                        const std::string& data_dir) {
  PropertyGraph graph;
  graph.Open(ckp, MemoryLevel::kSyncToFile);

  // Create vertex type PERSON
  {
    LOG(INFO) << "Create vertex type PERSON";
    std::string vertex_label_name = "PERSON";
    std::vector<std::pair<std::string, execution::Value>> properties;
    std::vector<std::string> primary_keys;
    primary_keys.emplace_back("id");
    properties.emplace_back(
        std::make_pair(std::string("id"), execution::Value::INT32(0)));
    properties.emplace_back(
        std::make_pair(std::string("name"), execution::Value::STRING("")));
    properties.emplace_back(
        std::make_pair(std::string("age"), execution::Value::INT32(0)));
    // testCreateVertexType(graph, vertex_label_name, properties, primary_keys);
    CreateVertexTypeParamBuilder builder;
    auto status = graph.CreateVertexType(builder.VertexLabel(vertex_label_name)
                                             .Properties(properties)
                                             .PrimaryKeyNames(primary_keys)
                                             .Build());
    EXPECT_TRUE(status.ok());
    std::cout << "Get vertex label num: "
              << static_cast<size_t>(graph.schema().vertex_label_num()) << "\n";
  }

  // Create edge type PERSON-KNOWS->PERSON
  {
    LOG(INFO) << "Create edge type PERSON-KNOWS->PERSON";
    std::string src_vertex_label = "PERSON";
    std::string edge_label_name = "KNOWS";
    std::string dst_vertex_label = "PERSON";
    std::vector<std::pair<std::string, execution::Value>> edge_properties;
    edge_properties.emplace_back(
        std::make_pair(std::string("weight"), execution::Value::FLOAT(0.0)));
    CreateEdgeTypeParamBuilder builder;
    auto status = graph.CreateEdgeType(builder.SrcLabel(src_vertex_label)
                                           .DstLabel(dst_vertex_label)
                                           .EdgeLabel(edge_label_name)
                                           .Properties(edge_properties)
                                           .Build());
    EXPECT_TRUE(status.ok());
    auto edge_label_num = graph.schema().edge_label_num();
    std::cout << "Get edge label num: " << static_cast<size_t>(edge_label_num)
              << "\n";
  }

  // Insert vertices for PERSON
  {
    LOG(INFO) << "Insert vertices for PERSON";
    std::string vertex_label_name = "PERSON";
    std::string vfile = data_dir + "/person.csv";
    std::vector<std::string> vertex_null_values;
    testLoadVertexBatch(graph, vertex_label_name, vfile, '|', true, 1024,
                        vertex_null_values);
    LOG(INFO) << "Vertices num after load " << graph.VertexNum(0);
  }

  // Batch load edges for PERSON-KNOWS->PERSON
  {
    LOG(INFO) << "Batch load edge type PERSON-KNOWS->PERSON";
    std::string src_vertex_type = "PERSON";
    std::string dst_vertex_type = "PERSON";
    std::string edge_type_name = "KNOWS";
    std::string efile = data_dir + "/person_knows_person.csv";
    std::vector<std::string> null_values;
    testLoadEdgeBatch(graph, src_vertex_type, dst_vertex_type, edge_type_name,
                      efile, '|', true, 1024, null_values);
    LOG(INFO) << "Edges num after load " << graph.EdgeNum(0, 0, 0);
  }

  // Traverse edge PERSON-KNOWS->PERSON
  {
    LOG(INFO) << "Start to traverse edge 1";
    auto person_num = graph.VertexNum(0, MAX_TIMESTAMP);
    auto generic_view = graph.GetGenericOutgoingGraphView(0, 0, 0);
    auto ed_accessor = graph.GetEdgeDataAccessor(0, 0, 0, 0);
    for (vid_t i = 0; i < person_num; i++) {
      auto adj_list = generic_view.get_edges(i);
      for (auto nbr = adj_list.begin(); nbr != adj_list.end(); ++nbr) {
        LOG(INFO) << "edge " << i << " " << nbr.get_vertex()
                  << ", data: " << ed_accessor.get_data(nbr).to_string();
      }
    }
  }

  // Add property for PERSON-KNOWS->PERSON
  {
    LOG(INFO) << "Start to add property for edge";
    std::string src_vertex_type = "PERSON";
    std::string dst_vertex_type = "PERSON";
    std::string edge_type_name = "KNOWS";
    std::vector<std::pair<std::string, execution::Value>> add_properties;
    add_properties.emplace_back(
        std::make_pair(std::string("creationDate"),
                       execution::Value::TIMESTAMPMS(DateTime(0))));
    AddEdgePropertiesParamBuilder builder;
    graph.AddEdgeProperties(builder.SrcLabel(src_vertex_type)
                                .DstLabel(dst_vertex_type)
                                .EdgeLabel(edge_type_name)
                                .Properties(add_properties)
                                .Build());
  }

  // Traverse edge PERSON-KNOWS->PERSON
  {
    LOG(INFO) << "Start to traverse edge 2";
    auto person_num = graph.VertexNum(0);
    auto generic_view = graph.GetGenericOutgoingGraphView(0, 0, 0);
    auto ed_accessor = graph.GetEdgeDataAccessor(0, 0, 0, 0);
    for (vid_t i = 0; i < person_num; i++) {
      auto adj_list = generic_view.get_edges(i);
      for (auto nbr = adj_list.begin(); nbr != adj_list.end(); ++nbr) {
        LOG(INFO) << "edge " << i << " " << nbr.get_vertex()
                  << ", data: " << ed_accessor.get_data(nbr).to_string();
      }
    }
  }
}

}  // namespace neug

TEST(DatabaseTest, TestAlterProperty) {
  std::string data_path = "/tmp/alter_property_test";
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);
  const char* data_dir = std::getenv("MODERN_GRAPH_DATA_DIR");
  if (data_dir == nullptr) {
    throw std::runtime_error(
        "MODERN_GRAPH_DATA_DIR environment variable is not set");
  }
  LOG(INFO) << "Data directory: " << data_dir;
  neug::CheckpointManager ws;
  ws.Open(data_path);
  auto ckp = make_checkpoint(ws);
  neug::testOpenEmptyGraph(ckp, data_dir);
}
