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

#include "src/storages/rt_mutable_graph/loader/mutable_fragment_loader.h"
#include "src/utils/string_utils.h"

namespace gs {

static std::string process_header_row_token(const std::string& token,
                                            const LoadingConfig& config) {
  std::string new_token = token;
  // trim the quote char at the beginning and end of the token
  if (config.GetIsQuoting()) {
    auto quote_char = config.GetQuotingChar();
    if (token.size() >= 2 && token[0] == quote_char &&
        token[token.size() - 1] == quote_char) {
      new_token = token.substr(1, token.size() - 2);
    }
  }
  // unescape the token
  if (config.GetIsEscaping()) {
    auto escape_char = config.GetEscapeChar();
    std::string res;
    for (size_t i = 0; i < new_token.size(); ++i) {
      if (new_token[i] == escape_char) {
        if (i + 1 < new_token.size()) {
          res.push_back(new_token[i + 1]);
          i++;
        }
      } else {
        res.push_back(new_token[i]);
      }
    }
    new_token = res;
  }
  return new_token;
}

static std::vector<std::string> read_header(
    const std::string& file_name, char delimiter,
    const LoadingConfig& loading_config) {
  // read the header line of the file, and split into vector to string by
  // delimiter. If quote_char is not empty, then use it to parse the header
  // line.
  std::vector<std::string> res_vec;
  std::ifstream file(file_name);
  std::string line;
  if (file.is_open()) {
    if (std::getline(file, line)) {
      std::stringstream ss(line);
      std::string token;
      while (std::getline(ss, token, delimiter)) {
        // trim the token
        token.erase(token.find_last_not_of(" \n\r\t") + 1);
        token = process_header_row_token(token, loading_config);
        res_vec.push_back(token);
      }
    } else {
      LOG(FATAL) << "Fail to read header line of file: " << file_name;
    }
    file.close();
  } else {
    LOG(FATAL) << "Fail to open file: " << file_name;
  }
  return res_vec;
}

static void put_column_names_option(const LoadingConfig& loading_config,
                                    bool header_row,
                                    const std::string& file_path,
                                    char delimiter,
                                    arrow::csv::ReadOptions& read_options,
                                    size_t len) {
  std::vector<std::string> all_column_names;
  if (header_row) {
    all_column_names = read_header(file_path, delimiter, loading_config);
    // It is possible that there exists duplicate column names in the header,
    // transform them to unique names
    std::unordered_map<std::string, int> name_count;
    for (auto& name : all_column_names) {
      if (name_count.find(name) == name_count.end()) {
        name_count[name] = 1;
      } else {
        name_count[name]++;
      }
    }
    VLOG(10) << "before Got all column names: " << all_column_names.size()
             << gs::to_string(all_column_names);
    for (size_t i = 0; i < all_column_names.size(); ++i) {
      auto& name = all_column_names[i];
      if (name_count[name] > 1) {
        auto cur_cnt = name_count[name];
        name_count[name] -= 1;
        all_column_names[i] = name + "_" + std::to_string(cur_cnt);
      }
    }
    VLOG(10) << "Got all column names: " << all_column_names.size()
             << gs::to_string(all_column_names);
  } else {
    // just get the number of columns.
    all_column_names.resize(len);
    for (size_t i = 0; i < all_column_names.size(); ++i) {
      all_column_names[i] = std::string("f") + std::to_string(i);
    }
  }
  read_options.column_names = all_column_names;
  VLOG(10) << "Got all column names: " << all_column_names.size()
           << gs::to_string(all_column_names);
}

MutableFragmentLoader::MutableFragmentLoader(
    const std::string& work_dir, const Schema& schema,
    const LoadingConfig& loading_config)
    : schema_(schema), loading_config_(loading_config) {
  graph_.Open(work_dir, 0);
}

std::shared_ptr<IFragmentLoader> MutableFragmentLoader::Make(
    const std::string& work_dir, const Schema& schema,
    const LoadingConfig& loading_config) {
  return std::shared_ptr<IFragmentLoader>(
      new MutableFragmentLoader(work_dir, schema, loading_config));
}

Result<bool> MutableFragmentLoader::LoadFragment() {
  LOG(INFO) << "Start to create vertex type";
  createVertices();
  LOG(INFO) << "Start to create edge type";
  createEdges();
  LOG(INFO) << "Start to batch load vertices";
  loadVertices();
  LOG(INFO) << "Start to batch load edges";
  loadEdges();
  return Result<bool>(true);
};

void MutableFragmentLoader::createVertices() {
  label_t vertex_label_num = schema_.vertex_label_num();
  for (label_t v_label_id = 0; v_label_id < vertex_label_num; v_label_id++) {
    std::string v_type_name = schema_.get_vertex_label_name(v_label_id);
    std::vector<std::string> v_property_names =
        schema_.get_vertex_property_names(v_label_id);
    std::vector<PropertyType> v_property_types =
        schema_.get_vertex_properties(v_label_id);
    std::vector<std::tuple<PropertyType, std::string, size_t>> v_primary_keys =
        schema_.get_vertex_primary_key(v_label_id);
    size_t primary_index = std::get<2>(v_primary_keys[0]);
    v_property_names.insert(v_property_names.begin() + primary_index,
                            std::get<1>(v_primary_keys[0]));
    v_property_types.insert(v_property_types.begin() + primary_index,
                            std::get<0>(v_primary_keys[0]));
    std::vector<std::tuple<PropertyType, std::string, Any>> properties;
    std::vector<std::string> primary_key_names;
    primary_key_names.emplace_back(std::get<1>(v_primary_keys[0]));
    for (size_t i = 0; i < v_property_names.size(); i++) {
      properties.emplace_back(std::make_tuple(
          v_property_types[i], v_property_names[i], std::string("")));
    }
    graph_.create_vertex_type(v_type_name, properties, primary_key_names);
  }
}

void MutableFragmentLoader::createEdges() {
  label_t vertex_label_num = schema_.vertex_label_num();
  label_t edge_label_num = schema_.edge_label_num();
  for (label_t e_label_id = 0; e_label_id < edge_label_num; e_label_id++) {
    for (label_t src_label_id = 0; src_label_id < vertex_label_num;
         src_label_id++) {
      for (label_t dst_label_id = 0; dst_label_id < vertex_label_num;
           dst_label_id++) {
        if (schema_.has_edge_label(src_label_id, dst_label_id, e_label_id)) {
          std::string src_type_name =
              schema_.get_vertex_label_name(src_label_id);
          std::string dst_type_name =
              schema_.get_vertex_label_name(dst_label_id);
          std::string edge_type_name = schema_.get_edge_label_name(e_label_id);
          auto edge_property_names = schema_.get_edge_property_names(
              src_label_id, dst_label_id, e_label_id);
          std::vector<PropertyType> edge_property_types =
              schema_.get_edge_properties(src_label_id, dst_label_id,
                                          e_label_id);
          std::vector<std::tuple<PropertyType, std::string, Any>> properties;
          for (size_t i = 0; i < edge_property_names.size(); i++) {
            properties.emplace_back(std::make_tuple(edge_property_types[i],
                                                    edge_property_names[i],
                                                    std::string("")));
          }
          graph_.create_edge_type(src_type_name, dst_type_name, edge_type_name,
                                  properties);
        }
      }
    }
  }
}

void MutableFragmentLoader::loadVertices() {
  label_t vertex_label_num = schema_.vertex_label_num();
  for (label_t v_label_id = 0; v_label_id < vertex_label_num; v_label_id++) {
    std::vector<std::string> v_files =
        loading_config_.GetVertexLoadingMeta().at(v_label_id);
    for (auto v_file : v_files) {
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
      parse_options.delimiter = loading_config_.GetDelimiter()[0];
      bool header_row = loading_config_.GetHasHeaderRow();
      if (header_row) {
        read_options.skip_rows = 1;
      } else {
        read_options.skip_rows = 0;
      }
      if (loading_config_.GetIsEscaping()) {
        parse_options.escaping = true;
        parse_options.escape_char = loading_config_.GetEscapeChar();
      } else {
        parse_options.escaping = false;
      }
      if (loading_config_.GetIsQuoting()) {
        parse_options.quoting = true;
        parse_options.quote_char = loading_config_.GetQuotingChar();
      } else {
        parse_options.quoting = false;
      }
      read_options.block_size = loading_config_.GetBatchSize();
      for (auto& null_value : loading_config_.GetNullValues()) {
        convert_options.null_values.emplace_back(null_value);
      }

      std::vector<std::string> all_column_names;
      auto property_names = schema_.get_vertex_property_names(v_label_id);
      put_column_names_option(loading_config_, header_row, v_file,
                              parse_options.delimiter, read_options,
                              property_names.size() + 1);

      std::vector<std::string> included_col_names;
      std::vector<std::string> mapped_property_names;
      auto cur_label_col_mapping =
          loading_config_.GetVertexColumnMappings(v_label_id);
      auto primary_keys = schema_.get_vertex_primary_key(v_label_id);
      CHECK(primary_keys.size() == 1);
      auto primary_key = primary_keys[0];
      if (cur_label_col_mapping.size() == 0) {
        // use default mapping, we assume the order of the columns in the
        // file is the same as the order of the properties in the schema,
        // except for primary key.
        auto primary_key_name = std::get<1>(primary_key);
        auto primary_key_ind = std::get<2>(primary_key);
        auto property_names = schema_.get_vertex_property_names(v_label_id);
        // for example, schema is : (name,age)
        // file header is (id,name,age), the primary key is id.
        // so, the mapped_property_names are: (id,name,age)
        CHECK(property_names.size() + 1 == read_options.column_names.size())
            << " size in schema: " << property_names.size()
            << ", size in file: " << read_options.column_names.size() << ","
            << gs::to_string(property_names)
            << ", read options: " << gs::to_string(read_options.column_names);
        // insert primary_key to property_names
        property_names.insert(property_names.begin() + primary_key_ind,
                              primary_key_name);

        for (size_t i = 0; i < read_options.column_names.size(); ++i) {
          included_col_names.emplace_back(read_options.column_names[i]);
          // We assume the order of the columns in the file is the same as
          // the order of the properties in the schema, except for primary
          // key.
          mapped_property_names.emplace_back(property_names[i]);
        }
      } else {
        for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
          auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
          if (col_name.empty()) {
            if (col_id >= read_options.column_names.size() || col_id < 0) {
              LOG(FATAL) << "The specified column index: " << col_id
                         << " is out of range, please check your configuration";
            }
            col_name = read_options.column_names[col_id];
          }
          // check whether index match to the name if col_id is valid
          if (col_id >= 0 && col_id < read_options.column_names.size()) {
            if (col_name != read_options.column_names[col_id]) {
              LOG(FATAL) << "The specified column name: " << col_name
                         << " does not match the column name in the file: "
                         << read_options.column_names[col_id];
            }
          }
          included_col_names.emplace_back(col_name);
          mapped_property_names.emplace_back(property_name);
        }
      }

      VLOG(10) << "Include columns: " << included_col_names.size();
      // if empty, then means need all columns
      convert_options.include_columns = included_col_names;

      // put column_types, col_name : col_type
      std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
          arrow_types;
      {
        auto property_types = schema_.get_vertex_properties(v_label_id);
        auto property_names = schema_.get_vertex_property_names(v_label_id);
        CHECK(property_types.size() == property_names.size());

        for (size_t i = 0; i < property_types.size(); ++i) {
          // for each schema' property name, get the index of the column
          // in vertex_column mapping, and bind the type with the column
          // name
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
                       << schema_.get_vertex_label_name(v_label_id)
                       << " please "
                          "check your configuration";
          }
          VLOG(10) << "vertex_label: "
                   << schema_.get_vertex_label_name(v_label_id)
                   << " property_name: " << property_name
                   << " property_type: " << property_type << " ind: " << ind;
          arrow_types.insert({included_col_names[ind],
                              PropertyTypeToArrowType(property_type)});
        }
        {
          // add primary key types;
          auto primary_key_name = std::get<1>(primary_key);
          auto primary_key_type = std::get<0>(primary_key);
          size_t ind = mapped_property_names.size();
          for (size_t i = 0; i < mapped_property_names.size(); ++i) {
            if (mapped_property_names[i] == primary_key_name) {
              ind = i;
              break;
            }
          }
          if (ind == mapped_property_names.size()) {
            LOG(FATAL)
                << "The specified property name: " << primary_key_name
                << " does not exist in the vertex column mapping, please "
                   "check your configuration";
          }
          arrow_types.insert({included_col_names[ind],
                              PropertyTypeToArrowType(primary_key_type)});
        }

        convert_options.column_types = arrow_types;

        std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
        if (loading_config_.GetIsBatchReader()) {
          auto res = std::make_shared<CSVStreamRecordBatchSupplier>(
              v_file, convert_options, read_options, parse_options);
          suppliers.emplace_back(
              std::dynamic_pointer_cast<IRecordBatchSupplier>(res));
        } else {
          auto res = std::make_shared<CSVTableRecordBatchSupplier>(
              v_file, convert_options, read_options, parse_options);
          suppliers.emplace_back(
              std::dynamic_pointer_cast<IRecordBatchSupplier>(res));
        }
        auto primary_type =
            std::get<0>(schema_.get_vertex_primary_key(v_label_id)[0]);
        if (primary_type == PropertyType::kInt32) {
          graph_.batch_load_vertices<int32_t>(v_label_id, suppliers);
        } else if (primary_type == PropertyType::kInt64) {
          graph_.batch_load_vertices<int64_t>(v_label_id, suppliers);
        } else if (primary_type == PropertyType::kUInt32) {
          graph_.batch_load_vertices<uint32_t>(v_label_id, suppliers);
        } else if (primary_type == PropertyType::kUInt64) {
          graph_.batch_load_vertices<uint64_t>(v_label_id, suppliers);
        } else if (primary_type.type_enum == impl::PropertyTypeImpl::kVarChar ||
                   primary_type.type_enum ==
                       impl::PropertyTypeImpl::kStringView) {
          graph_.batch_load_vertices<std::string_view>(v_label_id, suppliers);
        } else {
          LOG(FATAL) << "Unsupported primary type";
        }
      }
    }
  }
}

void MutableFragmentLoader::loadEdges() {
  auto& edge_sources = loading_config_.GetEdgeLoadingMeta();
  if (edge_sources.empty()) {
    return;
  }
  std::vector<std::pair<typename LoadingConfig::edge_triplet_type,
                        std::vector<std::string>>>
      edge_files;
  for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
    edge_files.emplace_back(iter->first, iter->second);
  }
  for (auto& edge_file : edge_files) {
    auto src_label_id = std::get<0>(edge_file.first);
    auto dst_label_id = std::get<1>(edge_file.first);
    auto e_label_id = std::get<2>(edge_file.first);
    auto e_files = edge_file.second;
    std::string src_type_name = schema_.get_vertex_label_name(src_label_id);
    std::string dst_type_name = schema_.get_vertex_label_name(dst_label_id);
    std::string edge_type_name = schema_.get_edge_label_name(e_label_id);
    for (auto e_file : e_files) {
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
      put_delimiter_option(loading_config_.GetDelimiter(), parse_options);
      bool header_row = loading_config_.GetHasHeaderRow();
      if (header_row) {
        read_options.skip_rows = 1;
      } else {
        read_options.skip_rows = 0;
      }
      auto edge_prop_names = schema_.get_edge_property_names(
          src_label_id, dst_label_id, e_label_id);

      put_column_names_option(loading_config_, header_row, e_file,
                              parse_options.delimiter, read_options,
                              edge_prop_names.size() + 2);
      if (loading_config_.GetIsEscaping()) {
        parse_options.escaping = true;
        parse_options.escape_char = loading_config_.GetEscapeChar();
      } else {
        parse_options.escaping = false;
      }
      if (loading_config_.GetIsQuoting()) {
        parse_options.quoting = true;
        parse_options.quote_char = loading_config_.GetQuotingChar();
      } else {
        parse_options.quoting = false;
      }
      read_options.block_size = loading_config_.GetBatchSize();
      for (auto& null_value : loading_config_.GetNullValues()) {
        convert_options.null_values.emplace_back(null_value);
      }

      auto src_dst_cols = loading_config_.GetEdgeSrcDstCol(
          src_label_id, dst_label_id, e_label_id);

      // parse all column_names
      // Get all column names(header, and always skip the first row)
      std::vector<std::string> included_col_names;
      std::vector<std::string> mapped_property_names;

      {
        CHECK(src_dst_cols.first.size() == 1 &&
              src_dst_cols.second.size() == 1);
        auto src_col_ind = src_dst_cols.first[0].second;
        auto dst_col_ind = src_dst_cols.second[0].second;
        CHECK(src_col_ind >= 0 &&
              src_col_ind < read_options.column_names.size())
            << " src_col_ind: " << src_col_ind
            << ", read_options.column_names.size(): "
            << read_options.column_names.size();
        CHECK(dst_col_ind >= 0 &&
              dst_col_ind < read_options.column_names.size())
            << " dst_col_ind: " << dst_col_ind
            << ", read_options.column_names.size(): "
            << read_options.column_names.size();

        included_col_names.emplace_back(read_options.column_names[src_col_ind]);
        included_col_names.emplace_back(read_options.column_names[dst_col_ind]);
      }
      auto cur_label_col_mapping = loading_config_.GetEdgeColumnMappings(
          src_label_id, dst_label_id, e_label_id);
      if (cur_label_col_mapping.empty()) {
        // use default mapping, we assume the order of the columns in the file
        // is the same as the order of the properties in the schema,
        auto edge_prop_names = schema_.get_edge_property_names(
            src_label_id, dst_label_id, e_label_id);
        for (size_t i = 0; i < edge_prop_names.size(); ++i) {
          auto property_name = edge_prop_names[i];
          if (loading_config_.GetHasHeaderRow()) {
            included_col_names.emplace_back(property_name);
          } else {
            included_col_names.emplace_back(read_options.column_names[i + 2]);
          }
          mapped_property_names.emplace_back(property_name);
        }
      } else {
        // add the property columns into the included columns
        for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
          // TODO: make the property column's names are in same order with
          // schema.
          auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
          if (col_name.empty()) {
            if (col_id >= read_options.column_names.size() || col_id < 0) {
              LOG(FATAL) << "The specified column index: " << col_id
                         << " is out of range, please check your configuration";
            }
            col_name = read_options.column_names[col_id];
          }
          // check whether index match to the name if col_id is valid
          if (col_id >= 0 && col_id < read_options.column_names.size()) {
            if (col_name != read_options.column_names[col_id]) {
              LOG(FATAL) << "The specified column name: " << col_name
                         << " does not match the column name in the file: "
                         << read_options.column_names[col_id];
            }
          }
          if (loading_config_.GetHasHeaderRow()) {
            included_col_names.emplace_back(col_name);
          } else {
            included_col_names.emplace_back(read_options.column_names[col_id]);
          }
          mapped_property_names.emplace_back(property_name);
        }
      }

      VLOG(10) << "Include Edge columns: " << gs::to_string(included_col_names);
      // if empty, then means need all columns
      convert_options.include_columns = included_col_names;

      // put column_types, col_name : col_type
      std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
          arrow_types;
      auto property_types =
          schema_.get_edge_properties(src_label_id, dst_label_id, e_label_id);
      {
        auto property_names = schema_.get_edge_property_names(
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
            LOG(FATAL)
                << "The specified property name: " << property_name
                << " does not exist in the vertex column mapping, please "
                   "check your configuration";
          }
          VLOG(10) << "edge_label: " << schema_.get_edge_label_name(e_label_id)
                   << " property_name: " << property_name
                   << " property_type: " << property_type << " ind: " << ind;
          arrow_types.insert({included_col_names[ind + 2],
                              PropertyTypeToArrowType(property_type)});
        }
        {
          // add src and dst primary col, to included_columns and column types.
          auto src_dst_cols = loading_config_.GetEdgeSrcDstCol(
              src_label_id, dst_label_id, e_label_id);
          CHECK(src_dst_cols.first.size() == 1 &&
                src_dst_cols.second.size() == 1);
          auto src_col_ind = src_dst_cols.first[0].second;
          auto dst_col_ind = src_dst_cols.second[0].second;
          CHECK(src_col_ind >= 0 &&
                src_col_ind < read_options.column_names.size());
          CHECK(dst_col_ind >= 0 &&
                dst_col_ind < read_options.column_names.size());
          PropertyType src_col_type, dst_col_type;
          {
            auto src_primary_keys =
                schema_.get_vertex_primary_key(src_label_id);
            CHECK(src_primary_keys.size() == 1);
            src_col_type = std::get<0>(src_primary_keys[0]);
            arrow_types.insert({read_options.column_names[src_col_ind],
                                PropertyTypeToArrowType(src_col_type)});
          }
          {
            auto dst_primary_keys =
                schema_.get_vertex_primary_key(dst_label_id);
            CHECK(dst_primary_keys.size() == 1);
            dst_col_type = std::get<0>(dst_primary_keys[0]);
            arrow_types.insert({read_options.column_names[dst_col_ind],
                                PropertyTypeToArrowType(dst_col_type)});
          }
        }

        convert_options.column_types = arrow_types;

        VLOG(10) << "Column types: ";
        for (auto iter : arrow_types) {
          VLOG(10) << iter.first << " : " << iter.second->ToString();
        }
      }
      std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
      if (loading_config_.GetIsBatchReader()) {
        auto res = std::make_shared<CSVStreamRecordBatchSupplier>(
            e_file, convert_options, read_options, parse_options);
        suppliers.emplace_back(
            std::dynamic_pointer_cast<IRecordBatchSupplier>(res));
      } else {
        auto res = std::make_shared<CSVTableRecordBatchSupplier>(
            e_file, convert_options, read_options, parse_options);
        suppliers.emplace_back(
            std::dynamic_pointer_cast<IRecordBatchSupplier>(res));
      }
      auto col_num = property_types.size();
      if (col_num == 0) {
        addEdges<grape::EmptyType>(src_label_id, dst_label_id, e_label_id,
                                   suppliers);
      } else if (col_num == 1) {
        auto property_type = property_types[0];
        if (property_type == PropertyType::kInt32) {
          addEdges<int32_t>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else if (property_type == PropertyType::kInt64) {
          addEdges<int64_t>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else if (property_type == PropertyType::kUInt32) {
          addEdges<uint32_t>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else if (property_type == PropertyType::kUInt64) {
          addEdges<uint64_t>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else if (property_type.type_enum ==
                       impl::PropertyTypeImpl::kVarChar ||
                   property_type.type_enum ==
                       impl::PropertyTypeImpl::kStringView) {
          addEdges<std::string_view>(src_label_id, dst_label_id, e_label_id,
                                     suppliers);
        } else if (property_type == PropertyType::kDate) {
          addEdges<Date>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else if (property_type == PropertyType::kBool) {
          addEdges<bool>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else if (property_type == PropertyType::kFloat) {
          addEdges<float>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else if (property_type == PropertyType::kDouble) {
          addEdges<double>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else if (property_type == PropertyType::kInterval) {
          addEdges<Interval>(src_label_id, dst_label_id, e_label_id, suppliers);
        } else {
          LOG(FATAL) << "Unsupported edge property type";
        }
      } else {
        addEdges<RecordView>(src_label_id, dst_label_id, e_label_id, suppliers);
      }
    }
  }
}

const bool MutableFragmentLoader::registered_ =
    LoaderFactory::Register("mutable", "csv",
                            static_cast<LoaderFactory::loader_initializer_t>(
                                &MutableFragmentLoader::Make));
}  // namespace gs