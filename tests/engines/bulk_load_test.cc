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
#include <arrow/csv/options.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "arrow/util/value_parsing.h"
#include "src/storages/rt_mutable_graph/loader/loader_utils.h"
#include "src/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "src/storages/rt_mutable_graph/schema.h"
#include "src/storages/rt_mutable_graph/types.h"
#include "src/utils/arrow_utils.h"
#include "src/utils/property/types.h"

namespace arrow {
class DataType;
}  // namespace arrow

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

void testLoadVertexBatch(MutablePropertyFragment& graph,
                         std::string vertex_type_name, std::string& v_file,
                         char delimiter, bool skip_head, int32_t batch_size,
                         std::vector<std::string>& null_values) {
  label_t v_label = graph.schema().get_vertex_label_id(vertex_type_name);
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  put_boolean_option(convert_options);
  parse_options.delimiter = delimiter;
  if (skip_head) {
    read_options.skip_rows = 1;
  } else {
    read_options.skip_rows = 0;
  }
  parse_options.escaping = false;
  parse_options.quoting = false;
  read_options.block_size = batch_size;
  for (auto& null_value : null_values) {
    convert_options.null_values.emplace_back(null_value);
  }

  auto property_size = graph.schema().get_vertex_properties(v_label).size();
  std::vector<std::string> all_column_names;
  all_column_names.resize(property_size + 1);
  for (size_t i = 0; i < all_column_names.size(); ++i) {
    all_column_names[i] = std::string("f") + std::to_string(i);
  }
  read_options.column_names = all_column_names;

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;
  auto primary_keys = graph.schema().get_vertex_primary_key(v_label);
  auto primary_key = primary_keys[0];
  auto primary_key_name = std::get<1>(primary_key);
  auto primary_key_ind = std::get<2>(primary_key);
  auto property_names = graph.schema().get_vertex_property_names(v_label);
  property_names.insert(property_names.begin() + primary_key_ind,
                        primary_key_name);
  for (size_t i = 0; i < read_options.column_names.size(); ++i) {
    included_col_names.emplace_back(read_options.column_names[i]);
    // We assume the order of the columns in the file is the same as the
    // order of the properties in the schema, except for primary key.
    LOG(INFO) << "Emplace property name " << property_names[i] << "\n";
    mapped_property_names.emplace_back(property_names[i]);
  }
  LOG(INFO) << "property_names size is " << property_names.size();
  convert_options.include_columns = included_col_names;
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> arrow_types;
  {
    auto property_types = graph.schema().get_vertex_properties(v_label);
    auto property_names = graph.schema().get_vertex_property_names(v_label);
    CHECK(property_types.size() == property_names.size());

    for (size_t i = 0; i < property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (size_t i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == property_name) {
          ind = i;
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
      VLOG(10) << "vertex_label: "
               << graph.schema().get_vertex_label_name(v_label)
               << " property_name: " << property_name
               << " property_type: " << property_type << " ind: " << ind;
      arrow_types.insert(
          {included_col_names[ind], PropertyTypeToArrowType(property_type)});
    }
    {
      // add primary key types;
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
      arrow_types.insert(
          {included_col_names[ind], PropertyTypeToArrowType(primary_key_type)});
    }
    convert_options.column_types = arrow_types;
  }
  auto supplier = std::make_shared<CSVStreamRecordBatchSupplier>(
      v_file, convert_options, read_options, parse_options);
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  suppliers.emplace_back(
      std::dynamic_pointer_cast<IRecordBatchSupplier>(supplier));
  graph.batch_load_vertices<int32_t>(v_label, suppliers);
}

void testLoadEdgeBatch(MutablePropertyFragment& graph,
                       std::string src_vertex_type, std::string dst_vertex_type,
                       std::string edge_type, std::string& e_file,
                       char delimiter, bool skip_head, int32_t batch_size,
                       std::vector<std::string>& null_values) {
  label_t src_label_id = graph.schema().get_vertex_label_id(src_vertex_type);
  label_t dst_label_id = graph.schema().get_vertex_label_id(dst_vertex_type);
  label_t e_label_id = graph.schema().get_edge_label_id(edge_type);
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  put_boolean_option(convert_options);
  parse_options.delimiter = delimiter;
  if (skip_head) {
    read_options.skip_rows = 1;
  } else {
    read_options.skip_rows = 0;
  }
  parse_options.escaping = false;
  parse_options.quoting = false;
  read_options.block_size = batch_size;
  for (auto& null_value : null_values) {
    convert_options.null_values.emplace_back(null_value);
  }

  auto edge_prop_names = graph.schema().get_edge_property_names(
      src_label_id, dst_label_id, e_label_id);

  std::vector<std::string> all_column_names;
  all_column_names.resize(edge_prop_names.size() + 2);
  for (size_t i = 0; i < all_column_names.size(); ++i) {
    all_column_names[i] = std::string("f") + std::to_string(i);
  }
  read_options.column_names = all_column_names;

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;
  included_col_names.emplace_back(read_options.column_names[0]);
  included_col_names.emplace_back(read_options.column_names[1]);
  for (size_t i = 0; i < edge_prop_names.size(); ++i) {
    auto property_name = edge_prop_names[i];
    included_col_names.emplace_back(read_options.column_names[i + 2]);
    mapped_property_names.emplace_back(property_name);
  }
  convert_options.include_columns = included_col_names;
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> arrow_types;
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
        arrow_types;

    auto property_types = graph.schema().get_edge_properties(
        src_label_id, dst_label_id, e_label_id);
    auto property_names = graph.schema().get_edge_property_names(
        src_label_id, dst_label_id, e_label_id);
    CHECK(property_types.size() == property_names.size());

    for (size_t i = 0; i < property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (size_t i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == property_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << property_name
                   << " does not exist in the vertex column mapping, please "
                      "check your configuration";
      }
      arrow_types.insert({included_col_names[ind + 2],
                          PropertyTypeToArrowType(property_type)});
    }
    {
      // add primary key types;
      PropertyType src_col_type, dst_col_type;
      {
        auto src_primary_keys =
            graph.schema().get_vertex_primary_key(src_label_id);
        CHECK(src_primary_keys.size() == 1);
        src_col_type = std::get<0>(src_primary_keys[0]);
        arrow_types.insert({read_options.column_names[0],
                            PropertyTypeToArrowType(src_col_type)});
      }
      {
        auto dst_primary_keys =
            graph.schema().get_vertex_primary_key(dst_label_id);
        CHECK(dst_primary_keys.size() == 1);
        dst_col_type = std::get<0>(dst_primary_keys[0]);
        arrow_types.insert({read_options.column_names[1],
                            PropertyTypeToArrowType(dst_col_type)});
      }
    }
    convert_options.column_types = arrow_types;
  }
  auto supplier = std::make_shared<CSVStreamRecordBatchSupplier>(
      e_file, convert_options, read_options, parse_options);
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  suppliers.emplace_back(
      std::dynamic_pointer_cast<IRecordBatchSupplier>(supplier));
  graph.batch_load_edges<int32_t, int32_t, float,
                         std::vector<std::tuple<vid_t, vid_t, float>>>(
      src_label_id, dst_label_id, e_label_id, suppliers);
}

void testOpenEmptyGraph(const std::string& graph_dir,
                        const std::string& data_dir) {
  MutablePropertyFragment graph;
  graph.Open(graph_dir, 0);

  // Create vertex type PERSON
  {
    LOG(INFO) << "Create vertex type PERSON";
    std::string vertex_label_name = "PERSON";
    std::vector<std::tuple<PropertyType, std::string, Any>> properties;
    std::vector<std::string> primary_keys;
    primary_keys.emplace_back("id");
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Int32(), std::string("id"), std::string("")));
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::StringView(), std::string("name"), std::string("")));
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Int32(), std::string("age"), std::string("")));
    testCreateVertexType(graph, vertex_label_name, properties, primary_keys);
    std::cout << "Get vertex label num: "
              << static_cast<size_t>(graph.schema().vertex_label_num()) << "\n";
  }

  // Create vertex type SOFTWARE
  {
    LOG(INFO) << "Create vertex type SOFTWARE";
    std::string vertex_label_name = "SOFTWARE";
    std::vector<std::tuple<PropertyType, std::string, Any>> properties;
    std::vector<std::string> primary_keys;
    primary_keys.emplace_back("id");
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::Int32(), std::string("id"), std::string("")));
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::StringView(), std::string("name"), std::string("")));
    properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::StringView(), std::string("lang"), std::string("")));
    testCreateVertexType(graph, vertex_label_name, properties, primary_keys);
    std::cout << "Get vertex label num: "
              << static_cast<size_t>(graph.schema().vertex_label_num()) << "\n";
  }

  // Create edge type PERSON-KNOWS->PERSON
  {
    LOG(INFO) << "Create edge type PERSON-KNOWS->PERSON";
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
    LOG(INFO) << "Create edge type PERSON-CREATED->SOFTWARE";
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

  // Load vertices for PERSON
  {
    LOG(INFO) << "Load vertices for PERSON";
    std::string vertex_label_name = "PERSON";
    std::string vfile = data_dir + "/person.csv.part1";
    std::vector<std::string> vertex_null_values;
    testLoadVertexBatch(graph, vertex_label_name, vfile, '|', true, 1024,
                        vertex_null_values);
    LOG(INFO) << "Vertices num after load " << graph.vertex_num(0);
  }

  // Insert vertices for PERSON
  {
    LOG(INFO) << "Insert vertices for PERSON";
    std::string vertex_label_name = "PERSON";
    std::string vfile = data_dir + "/person.csv.part2";
    std::vector<std::string> vertex_null_values;
    testLoadVertexBatch(graph, vertex_label_name, vfile, '|', true, 1024,
                        vertex_null_values);
    LOG(INFO) << "Vertices num after load " << graph.vertex_num(0);
  }

  // Update property for PERSON
  {
    LOG(INFO) << "Update property for PERSON";
    std::string vertex_label_name = "PERSON";
    std::vector<std::tuple<PropertyType, std::string, Any>> add_properties;
    std::vector<std::tuple<std::string, std::string>> update_properties;
    std::vector<std::string> delete_properties;
    add_properties.emplace_back(
        std::make_tuple<PropertyType, std::string, std::string>(
            PropertyType::StringView(), std::string("lastName"),
            std::string("")));
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

  // Insert edges for PERSON-KNOWS->PERSON
  {
    LOG(INFO) << "Insert edges for PERSON-KNOWS->PERSON";
    std::string src_vertex_type = "PERSON";
    std::string dst_vertex_type = "PERSON";
    std::string edge_type_name = "KNOWS";
    std::string efile = data_dir + "/person_knows_person.csv.part1";
    std::vector<std::string> null_values;
    testLoadEdgeBatch(graph, src_vertex_type, dst_vertex_type, edge_type_name,
                      efile, '|', true, 1024, null_values);
    LOG(INFO) << "Edges num after load " << graph.edge_num(0, 0, 0);
  }

  {
    LOG(INFO) << "Insert edges for PERSON-KNOWS->PERSON";
    std::string src_vertex_type = "PERSON";
    std::string dst_vertex_type = "PERSON";
    std::string edge_type_name = "KNOWS";
    std::string efile = data_dir + "/person_knows_person.csv.part2";
    std::vector<std::string> null_values;
    testLoadEdgeBatch(graph, src_vertex_type, dst_vertex_type, edge_type_name,
                      efile, '|', true, 1024, null_values);
    LOG(INFO) << "Edges num after load " << graph.edge_num(0, 0, 0);
  }

  // Delete vertex SOFTWARE
  {
    LOG(INFO) << "Delete vertices SOFTWARE";
    std::string vertex_label_name = "SOFTWARE";
    graph.delete_vertex_type(vertex_label_name, false, false);
    auto vertex_label_num = graph.schema().vertex_label_num();
    CHECK_EQ(static_cast<size_t>(vertex_label_num), 1);
  }
}
}  // namespace gs

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: bulk_load_test <graph_dir> <data_dir>" << std::endl;
    return -1;
  }
  std::string work_dir = argv[1];
  std::string data_dir = argv[2];
  gs::testOpenEmptyGraph(work_dir, data_dir);
  return 0;
}