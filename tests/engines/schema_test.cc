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
#include <iostream>
#include <string>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include "arrow/util/value_parsing.h"

#include "src/storages/rt_mutable_graph/loader/csv_fragment_loader.h"
#include "src/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

void testCreateVertexType(
    MutablePropertyFragment& graph, std::string& vertex_type_name,
    std::vector<std::tuple<PropertyType, std::string, Any>>& properties,
    std::vector<std::string>& primary_keys) {
  graph.create_vertex_type(vertex_type_name, properties, primary_keys);
}

void testCreateEdgeType(
    MutablePropertyFragment& graph, std::string& src_vertex_label,
    std::string& dst_vertex_label, std::string& edge_type_name,
    std::vector<std::tuple<PropertyType, std::string, Any>>& properties) {
  graph.create_edge_type(src_vertex_label, dst_vertex_label, edge_type_name,
                         properties);
}

void testOpenEmptyGraph(const std::string& graph_dir) {
  MutablePropertyFragment graph;
  graph.Open(graph_dir, 0);

  // Create vertex type PERSON
  {
    std::string vertex_label_name = "PERSON";
    std::vector<std::tuple<PropertyType, std::string, Any>> properties;
    std::vector<std::string> primary_keys;
    primary_keys.emplace_back("id");
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Int32(), std::string("id"), std::string("")));
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::String(), std::string("name"), std::string("")));
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Int32(), std::string("age"), std::string("")));
    testCreateVertexType(graph, vertex_label_name, properties, primary_keys);
    std::cout << "Get vertex label num: "
              << static_cast<size_t>(graph.schema().vertex_label_num()) << "\n";
  }

  // Create vertex type SOFTWARE
  {
    std::string vertex_label_name = "SOFTWARE";
    std::vector<std::tuple<PropertyType, std::string, Any>> properties;
    std::vector<std::string> primary_keys;
    primary_keys.emplace_back("id");
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Int32(), std::string("id"), std::string("")));
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::String(), std::string("name"), std::string("")));
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::String(), std::string("lang"), std::string("")));
    testCreateVertexType(graph, vertex_label_name, properties, primary_keys);
    std::cout << "Get vertex label num: "
              << static_cast<size_t>(graph.schema().vertex_label_num()) << "\n";
  }

  // Create edge type PERSON-KNOWS->PERSON
  {
    std::string src_vertex_label = "PERSON";
    std::string edge_label_name = "KNOWS";
    std::string dst_vertex_label = "PERSON";
    std::vector<std::tuple<PropertyType, std::string, Any>> edge_properties;
    edge_properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Float(), std::string("weight"), std::string("")));
    testCreateEdgeType(graph, src_vertex_label, dst_vertex_label,
                       edge_label_name, edge_properties);
    auto edge_label_num = graph.schema().edge_label_num();
    std::cout << "Get edge label num: " << static_cast<size_t>(edge_label_num)
              << "\n";
  }

  // Create edge type PERSON-CREATED->SOFTWARE
  {
    std::string src_vertex_label = "PERSON";
    std::string edge_label_name = "CREATED";
    std::string dst_vertex_label = "SOFTWARE";
    std::vector<std::tuple<PropertyType, std::string, Any>> edge_properties;
    edge_properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Float(), std::string("weight"), std::string("")));
    edge_properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Int32(), std::string("since"), std::string("")));
    testCreateEdgeType(graph, src_vertex_label, dst_vertex_label,
                       edge_label_name, edge_properties);
    auto edge_label_num = graph.schema().edge_label_num();
    std::cout << "Get edge label num: " << static_cast<size_t>(edge_label_num)
              << "\n";
  }

  // Update property for PERSON
  {
    std::string vertex_label_name = "PERSON";
    std::vector<std::tuple<PropertyType, std::string, Any>> add_properties;
    std::vector<std::tuple<std::string, std::string>> update_properties;
    std::vector<std::string> delete_properties;
    add_properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::String(), std::string("lastName"), std::string("")));
    update_properties.emplace_back(std::make_tuple<std::string, std::string>(
        std::string("name"), std::string("firstName")));
    delete_properties.emplace_back("age");
    graph.add_vertex_properties(vertex_label_name, add_properties);
    graph.rename_vertex_properties(vertex_label_name, update_properties);
    graph.delete_vertex_properties(vertex_label_name, delete_properties);
  }

  {
    auto vertex_label_num = graph.schema().vertex_label_num();
    auto edge_label_num = graph.schema().edge_label_num();
    CHECK_EQ(static_cast<size_t>(vertex_label_num), 2);
    CHECK_EQ(static_cast<size_t>(edge_label_num), 2);

    auto properties = graph.schema().get_vertex_property_names("PERSON");
    CHECK_EQ(properties.size(), 2);
    CHECK_EQ(properties[0], "firstName");
    CHECK_EQ(properties[1], "lastName");
  }

  // Delete vertex SOFTWARE
  {
    std::string vertex_label_name = "SOFTWARE";
    graph.delete_vertex_type(vertex_label_name, false, false);
    auto vertex_label_num = graph.schema().vertex_label_num();
    CHECK_EQ(static_cast<size_t>(vertex_label_num), 1);
  }
}
}  // namespace gs

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: bulk_load_test <graph_dir>" << std::endl;
    return -1;
  }
  std::string work_dir = argv[1];
  gs::testOpenEmptyGraph(work_dir);
  return 0;
}