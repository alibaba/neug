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

#include "neug/storages/loader/loader_utils.h"

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/csv/options.h>
#include <arrow/csv/reader.h>
#include <arrow/io/file.h>
#include <arrow/io/type_fwd.h>
#include <glog/logging.h>
#include <stdint.h>
#include <sys/statvfs.h>
#include <memory>
#include <ostream>

#include "neug/utils/arrow_utils.h"
#include "neug/utils/string_utils.h"

namespace neug {

static bool put_skip_rows_option(const LoadingConfig& loading_config,
                                 arrow::csv::ReadOptions& read_options) {
  bool header_row = loading_config.GetHasHeaderRow();
  if (header_row) {
    read_options.skip_rows = 1;
  } else {
    read_options.skip_rows = 0;
  }
  return header_row;
}

static void put_escape_char_option(const LoadingConfig& loading_config,
                                   arrow::csv::ParseOptions& parse_options) {
  parse_options.escaping = loading_config.GetIsEscaping();
  if (parse_options.escaping) {
    parse_options.escape_char = loading_config.GetEscapeChar();
  }
}
static void put_block_size_option(const LoadingConfig& loading_config,
                                  arrow::csv::ReadOptions& read_options) {
  auto batch_size = loading_config.GetBatchSize();
  if (batch_size <= 0) {
    LOG(FATAL) << "Block size should be positive";
  }
  read_options.block_size = batch_size;
}

static void put_quote_char_option(const LoadingConfig& loading_config,
                                  arrow::csv::ParseOptions& parse_options) {
  parse_options.quoting = loading_config.GetIsQuoting();
  if (parse_options.quoting) {
    parse_options.quote_char = loading_config.GetQuotingChar();
  }
  parse_options.double_quote = loading_config.GetIsDoubleQuoting();
}

static void put_null_values(const LoadingConfig& loading_config,
                            arrow::csv::ConvertOptions& convert_options) {
  auto null_values = loading_config.GetNullValues();
  for (auto& null_value : null_values) {
    convert_options.null_values.emplace_back(null_value);
  }
}

void printDiskRemaining(const std::string& path) {
  struct statvfs buf;
  if (statvfs(path.c_str(), &buf) == 0) {
    LOG(INFO) << "Disk remaining: " << buf.f_bsize * buf.f_bavail / 1024 / 1024
              << "MB";
  }
}

void put_delimiter_option(const std::string& delimiter_str,
                          arrow::csv::ParseOptions& parse_options) {
  if (delimiter_str.size() != 1 && delimiter_str[0] != '\\') {
    LOG(FATAL) << "Delimiter should be a single character, or a escape "
                  "character, like '\\t'";
  }
  if (delimiter_str[0] == '\\') {
    if (delimiter_str.size() != 2) {
      LOG(FATAL) << "Delimiter should be a single character";
    }
    // escape the special character
    switch (delimiter_str[1]) {
    case 't':
      parse_options.delimiter = '\t';
      break;
    default:
      LOG(FATAL) << "Unsupported escape character: " << delimiter_str[1];
    }
  } else {
    parse_options.delimiter = delimiter_str[0];
  }
}

void put_boolean_option(arrow::csv::ConvertOptions& convert_options) {
  convert_options.true_values.emplace_back("True");
  convert_options.true_values.emplace_back("true");
  convert_options.true_values.emplace_back("TRUE");
  convert_options.false_values.emplace_back("False");
  convert_options.false_values.emplace_back("false");
  convert_options.false_values.emplace_back("FALSE");
}

std::string process_header_row_token(const std::string& token, bool is_quoting,
                                     char quote_char, bool is_escaping,
                                     char escape_char) {
  std::string new_token = token;
  // trim the quote char at the beginning and end of the token
  if (is_quoting) {
    if (token.size() >= 2 && token[0] == quote_char &&
        token[token.size() - 1] == quote_char) {
      new_token = token.substr(1, token.size() - 2);
    }
  }
  // unescape the token
  if (is_escaping) {
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

std::vector<std::string> read_header(const std::string& file_name,
                                     char delimiter, bool is_quoting,
                                     char quote_char, bool is_escaping,
                                     char escape_char) {
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
        size_t endpos = token.find_last_not_of(" \n\r\t");
        if (endpos == std::string::npos) {
          token.clear();
        } else {
          token.erase(endpos + 1);
        }
        token = process_header_row_token(token, is_quoting, quote_char,
                                         is_escaping, escape_char);
        res_vec.push_back(token);
      }
    } else {
      file.close();
      THROW_IO_EXCEPTION("Fail to read header line of file: " + file_name);
    }
    file.close();
  } else {
    THROW_IO_EXCEPTION("Fail to open file: " + file_name);
  }
  return res_vec;
}

void put_column_names_option(bool header_row, const std::string& file_path,
                             char delimiter, bool is_quoting, char quote_char,
                             bool is_escaping, char escape_char,
                             arrow::csv::ReadOptions& read_options,
                             size_t len) {
  std::vector<std::string> all_column_names;
  if (header_row) {
    all_column_names = read_header(file_path, delimiter, is_quoting, quote_char,
                                   is_escaping, escape_char);
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
             << neug::to_string(all_column_names);
    for (size_t i = 0; i < all_column_names.size(); ++i) {
      auto& name = all_column_names[i];
      if (name_count[name] > 1) {
        auto cur_cnt = name_count[name];
        name_count[name] -= 1;
        all_column_names[i] = name + "_" + std::to_string(cur_cnt);
      }
    }
    VLOG(10) << "Got all column names: " << all_column_names.size()
             << neug::to_string(all_column_names);
  } else {
    // just get the number of columns.
    all_column_names.resize(len);
    for (size_t i = 0; i < all_column_names.size(); ++i) {
      all_column_names[i] = std::string("f") + std::to_string(i);
    }
  }
  read_options.column_names = all_column_names;
  VLOG(10) << "Got all column names: " << all_column_names.size()
           << neug::to_string(all_column_names);
}

std::vector<std::string> columnMappingsToSelectedCols(
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings) {
  std::vector<std::string> selected_cols;
  for (auto& column_mapping : column_mappings) {
    selected_cols.push_back(std::get<1>(column_mapping));
  }
  return selected_cols;
}

CSVStreamRecordBatchSupplier::CSVStreamRecordBatchSupplier(
    const std::string& file_path, arrow::csv::ConvertOptions convert_options,
    arrow::csv::ReadOptions read_options,
    arrow::csv::ParseOptions parse_options)
    : file_path_(file_path) {
  auto read_result = arrow::io::ReadableFile::Open(file_path);
  if (!read_result.ok()) {
    LOG(ERROR) << "Failed to open file: " << file_path
               << " error: " << read_result.status().message();
    THROW_IO_EXCEPTION("Failed to open file: " + file_path +
                       " error: " + read_result.status().message());
  }
  auto file = read_result.ValueOrDie();
  auto count_file_result = arrow::io::ReadableFile::Open(file_path);
  if (count_file_result.ok()) {
    auto count_file = count_file_result.ValueOrDie();
    auto future = arrow::csv::CountRowsAsync(
        arrow::io::default_io_context(), count_file,
        arrow::internal::GetCpuThreadPool(), read_options, parse_options);
    future.Wait();

    auto count_result = future.result();
    if (count_result.ok()) {
      row_num_ = count_result.ValueUnsafe();
    } else {
      LOG(WARNING) << "Failed to count rows for " << file_path << ": "
                   << count_result.status().message();
      THROW_IO_EXCEPTION("Failed to count rows for " + file_path + ": " +
                         count_result.status().message());
    }
  } else {
    LOG(WARNING) << "Failed to reopen file for counting: "
                 << count_file_result.status().message();
    THROW_IO_EXCEPTION("Failed to reopen file for counting: " +
                       count_file_result.status().message());
  }
  auto res = arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
                                               file, read_options,
                                               parse_options, convert_options);
  if (!res.ok()) {
    THROW_IO_EXCEPTION("Failed to create streaming reader for file: " +
                       file_path + " error: " + res.status().message());
  }
  reader_ = res.ValueOrDie();
  VLOG(10) << "Finish init CSVRecordBatchSupplier for file: " << file_path;
}

std::shared_ptr<arrow::RecordBatch>
CSVStreamRecordBatchSupplier::GetNextBatch() {
  auto res = reader_->Next();
  if (res.ok()) {
    return res.ValueOrDie();
  } else {
    LOG(ERROR) << "Failed to read next batch from file: " << file_path_
               << " error: " << res.status().message();
    return nullptr;
  }
}

CSVTableRecordBatchSupplier::CSVTableRecordBatchSupplier(
    const std::string& path, arrow::csv::ConvertOptions convert_options,
    arrow::csv::ReadOptions read_options,
    arrow::csv::ParseOptions parse_options)
    : file_path_(path) {
  auto read_result = arrow::io::ReadableFile::Open(path);
  if (!read_result.ok()) {
    LOG(FATAL) << "Failed to open file: " << path
               << " error: " << read_result.status().message();
  }
  std::shared_ptr<arrow::io::ReadableFile> file = read_result.ValueOrDie();
  auto res = arrow::csv::TableReader::Make(arrow::io::default_io_context(),
                                           file, read_options, parse_options,
                                           convert_options);

  if (!res.ok()) {
    LOG(FATAL) << "Failed to create table reader for file: " << path
               << " error: " << res.status().message();
  }
  auto reader = res.ValueOrDie();

  auto result = reader->Read();
  auto status = result.status();
  if (!status.ok()) {
    LOG(FATAL) << "Failed to read table from file: " << path
               << " error: " << status.message();
  }
  table_ = result.ValueOrDie();
  reader_ = std::make_shared<arrow::TableBatchReader>(*table_);
}

std::shared_ptr<arrow::RecordBatch>
CSVTableRecordBatchSupplier::GetNextBatch() {
  std::shared_ptr<arrow::RecordBatch> batch;
  auto status = reader_->ReadNext(&batch);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to read batch from file: " << file_path_
               << " error: " << status.message();
  }
  return batch;
}

std::shared_ptr<arrow::RecordBatch>
ArrowRecordBatchArraySupplier::GetNextBatch() {
  if (current_batch_index_ >= batch_num_) {
    return nullptr;
  }
  std::vector<std::shared_ptr<arrow::Array>> columns;
  int64_t num_rows = 0;
  for (size_t i = 0; i < arrays_.size(); ++i) {
    columns.push_back(arrays_[i][current_batch_index_]);
    if (i == 0) {
      num_rows = arrays_[i][current_batch_index_]->length();
    } else {
      if (num_rows != arrays_[i][current_batch_index_]->length()) {
        LOG(FATAL) << "The length of columns is not equal";
      }
    }
  }
  auto batch = arrow::RecordBatch::Make(schema_, num_rows, columns);
  current_batch_index_++;
  return batch;
}

std::shared_ptr<arrow::RecordBatch>
ArrowRecordBatchStreamSupplier::GetNextBatch() {
  if (!reader_) {
    LOG(ERROR) << "Reader is null";
    return nullptr;
  }
  auto result = reader_->Next();
  if (result.ok()) {
    return result.ValueOrDie();
  } else {
    LOG(ERROR) << "Failed to get next batch: " << result.status().message();
    return nullptr;  // Handle error appropriately in production code
  }
}

void fillVertexReaderMeta(
    label_t v_label, const std::string& v_label_name, const std::string& v_file,
    const LoadingConfig& loading_config,
    const std::vector<std::string>& vertex_property_names,
    const std::vector<DataTypeId>& vertex_edge_property_types,
    DataTypeId pk_type, const std::string& pk_name, size_t pk_ind,
    arrow::csv::ReadOptions& read_options,
    arrow::csv::ParseOptions& parse_options,
    arrow::csv::ConvertOptions& convert_options) {
  CHECK(vertex_edge_property_types.size() == vertex_property_names.size());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  // BOOLEAN parser
  put_boolean_option(convert_options);

  put_delimiter_option(loading_config.GetDelimiter(), parse_options);
  bool header_row = put_skip_rows_option(loading_config, read_options);
  put_column_names_option(
      header_row, v_file, parse_options.delimiter,
      loading_config.GetIsQuoting(), loading_config.GetQuotingChar(),
      loading_config.GetIsEscaping(), loading_config.GetEscapeChar(),
      read_options, vertex_property_names.size() + 1);
  put_escape_char_option(loading_config, parse_options);
  put_quote_char_option(loading_config, parse_options);
  put_block_size_option(loading_config, read_options);
  put_null_values(loading_config, convert_options);

  // parse all column_names

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;

  auto cur_label_col_mapping = loading_config.GetVertexColumnMappings(v_label);

  if (cur_label_col_mapping.size() == 0) {
    // use default mapping, we assume the order of the columns in the file is
    // the same as the order of the properties in the schema, except for
    // primary key.
    // for example, schema is : (name,age)
    // file header is (id,name,age), the primary key is id.
    // so, the mapped_property_names are: (id,name,age)
    std::vector<std::string> vertex_property_names_copy =
        vertex_property_names;  // make a copy
    CHECK(vertex_property_names.size() + 1 == read_options.column_names.size())
        << " size in schema: " << vertex_property_names.size()
        << ", size in file: " << read_options.column_names.size() << ","
        << neug::to_string(vertex_property_names)
        << ", read options: " << neug::to_string(read_options.column_names);
    vertex_property_names_copy.insert(
        vertex_property_names_copy.begin() + pk_ind, pk_name);

    for (size_t i = 0; i < read_options.column_names.size(); ++i) {
      included_col_names.emplace_back(read_options.column_names[i]);
      // We assume the order of the columns in the file is the same as the
      // order of the properties in the schema, except for primary key.
      mapped_property_names.emplace_back(vertex_property_names_copy[i]);
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
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> arrow_types;
  {
    for (size_t i = 0; i < vertex_edge_property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = vertex_edge_property_types[i];
      auto property_name = vertex_property_names[i];
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
                   << v_label_name
                   << " please "
                      "check your configuration";
      }
      auto arrow_type = PropertyTypeToArrowType(property_type);
      VLOG(10) << "vertex_label: " << v_label_name
               << " property_name: " << property_name
               << " property_type: " << property_type << " ind: " << ind << " "
               << arrow_type->ToString();
      arrow_types.insert({included_col_names[ind], arrow_type});
    }
    {
      // add primary key types;
      size_t ind = mapped_property_names.size();
      for (size_t i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == pk_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << pk_name
                   << " does not exist in the vertex column mapping, please "
                      "check your configuration";
      }
      arrow_types.insert(
          {included_col_names[ind], PropertyTypeToArrowType(pk_type)});
    }

    convert_options.column_types = arrow_types;
  }
}

void fillEdgeReaderMeta(label_t src_label_id, label_t dst_label_id,
                        label_t label_id, const std::string& edge_label_name,
                        const std::string& e_file,
                        const LoadingConfig& loading_config,
                        const std::vector<std::string>& edge_property_names,
                        const std::vector<DataTypeId>& edge_property_types,
                        DataTypeId src_pk_type, DataTypeId dst_pk_type,
                        arrow::csv::ReadOptions& read_options,
                        arrow::csv::ParseOptions& parse_options,
                        arrow::csv::ConvertOptions& convert_options) {
  CHECK(edge_property_types.size() == edge_property_names.size());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  put_boolean_option(convert_options);

  put_delimiter_option(loading_config.GetDelimiter(), parse_options);
  bool header_row = put_skip_rows_option(loading_config, read_options);

  put_column_names_option(
      header_row, e_file, parse_options.delimiter,
      loading_config.GetIsQuoting(), loading_config.GetQuotingChar(),
      loading_config.GetIsEscaping(), loading_config.GetEscapeChar(),
      read_options, edge_property_names.size() + 2);
  put_escape_char_option(loading_config, parse_options);
  put_quote_char_option(loading_config, parse_options);
  put_block_size_option(loading_config, read_options);
  put_null_values(loading_config, convert_options);

  auto src_dst_cols =
      loading_config.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);

  // parse all column_names
  // Get all column names(header, and always skip the first row)
  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;

  {
    // add src and dst primary col, to included_columns, put src_col and
    // dst_col at the first of included_columns.
    CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
    auto src_col_ind = src_dst_cols.first[0].second;
    auto dst_col_ind = src_dst_cols.second[0].second;
    CHECK(src_col_ind >= 0 && src_col_ind < read_options.column_names.size())
        << " src_col_ind: " << src_col_ind
        << ", read_options.column_names.size(): "
        << read_options.column_names.size();
    CHECK(dst_col_ind >= 0 && dst_col_ind < read_options.column_names.size())
        << " dst_col_ind: " << dst_col_ind
        << ", read_options.column_names.size(): "
        << read_options.column_names.size();

    included_col_names.emplace_back(read_options.column_names[src_col_ind]);
    included_col_names.emplace_back(read_options.column_names[dst_col_ind]);
  }

  auto cur_label_col_mapping = loading_config.GetEdgeColumnMappings(
      src_label_id, dst_label_id, label_id);
  if (cur_label_col_mapping.empty()) {
    // use default mapping, we assume the order of the columns in the file is
    // the same as the order of the properties in the schema,
    for (size_t i = 0; i < edge_property_names.size(); ++i) {
      auto property_name = edge_property_names[i];
      if (loading_config.GetHasHeaderRow()) {
        included_col_names.emplace_back(property_name);
      } else {
        included_col_names.emplace_back(read_options.column_names[i + 2]);
      }
      mapped_property_names.emplace_back(property_name);
    }
  } else {
    // add the property columns into the included columns
    for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
      // TODO: make the property column's names are in same order with schema.
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
      if (loading_config.GetHasHeaderRow()) {
        included_col_names.emplace_back(col_name);
      } else {
        included_col_names.emplace_back(read_options.column_names[col_id]);
      }
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include Edge columns: " << neug::to_string(included_col_names);
  // if empty, then means need all columns
  convert_options.include_columns = included_col_names;

  // put column_types, col_name : col_type
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> arrow_types;
  {
    for (size_t i = 0; i < edge_property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = edge_property_types[i];
      auto property_name = edge_property_names[i];
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
      VLOG(10) << "edge_label: " << edge_label_name
               << " property_name: " << property_name
               << " property_type: " << property_type << " ind: " << ind;
      arrow_types.insert({included_col_names[ind + 2],
                          PropertyTypeToArrowType(property_type)});
    }
    {
      // add src and dst primary col, to included_columns and column types.
      auto src_dst_cols =
          loading_config.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);
      CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
      auto src_col_ind = src_dst_cols.first[0].second;
      auto dst_col_ind = src_dst_cols.second[0].second;
      CHECK(src_col_ind >= 0 && src_col_ind < read_options.column_names.size());
      CHECK(dst_col_ind >= 0 && dst_col_ind < read_options.column_names.size());
      arrow_types.insert({read_options.column_names[src_col_ind],
                          PropertyTypeToArrowType(src_pk_type)});
      arrow_types.insert({read_options.column_names[dst_col_ind],
                          PropertyTypeToArrowType(dst_pk_type)});
    }

    convert_options.column_types = arrow_types;

    VLOG(10) << "Column types: ";
    for (auto iter : arrow_types) {
      VLOG(10) << iter.first << " : " << iter.second->ToString();
    }
  }
}

template <typename COL_T>
void set_column(std::shared_ptr<neug::ColumnBase> col,
                std::shared_ptr<arrow::ChunkedArray> array,
                const std::vector<vid_t>& vids) {
  using arrow_array_type = typename neug::TypeConverter<COL_T>::ArrowArrayType;
  auto array_type = array->type();
  auto arrow_type = neug::TypeConverter<COL_T>::ArrowTypeValue();
  CHECK(array_type->Equals(arrow_type))
      << "Inconsistent data type, expect " << arrow_type->ToString()
      << ", but got " << array_type->ToString();
  for (auto j = 0; j < array->num_chunks(); ++j) {
    auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(j));
    for (auto k = 0; k < casted->length(); ++k) {
      if (vids[k] >= std::numeric_limits<vid_t>::max()) {
        continue;
      }
      col->set_any(vids[k],
                   std::move(PropUtils<COL_T>::to_prop(casted->Value(k))),
                   false);
    }
  }
}

void set_column_from_date_array(std::shared_ptr<neug::ColumnBase> col,
                                std::shared_ptr<arrow::ChunkedArray> array,
                                const std::vector<vid_t>& vids) {
  auto type = array->type();
  auto col_type = col->type();
  if (type->Equals(arrow::date32())) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Date32Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (vids[k] >= std::numeric_limits<vid_t>::max()) {
          continue;
        }
        col->set_any(
            vids[k],
            std::move(PropUtils<Date>::to_prop(Date(casted->Value(k)))), false);
      }
    }
  } else if (type->Equals(arrow::date64())) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Date64Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (vids[k] >= std::numeric_limits<vid_t>::max()) {
          continue;
        }
        col->set_any(
            vids[k],
            std::move(PropUtils<Date>::to_prop(Date(casted->Value(k)))), false);
      }
    }
  } else {
    LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
               << col_type;
  }
}

template <typename COL_T>  // COL_T = DateTime or Timestamp
void set_column_from_timestamp_array(std::shared_ptr<neug::ColumnBase> col,
                                     std::shared_ptr<arrow::ChunkedArray> array,
                                     const std::vector<vid_t>& vids) {
  auto type = array->type();
  auto col_type = col->type();
  if (type->Equals(arrow::timestamp(arrow::TimeUnit::type::MILLI))) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (vids[k] >= std::numeric_limits<vid_t>::max()) {
          continue;
        }
        col->set_any(
            vids[k],
            std::move(PropUtils<COL_T>::to_prop(COL_T(casted->Value(k)))),
            false);
      }
    }
  } else {
    LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
               << col_type;
  }
}

void set_interval_column_from_string_array(
    std::shared_ptr<neug::ColumnBase> col,
    std::shared_ptr<arrow::ChunkedArray> array,
    const std::vector<vid_t>& vids) {
  auto type = array->type();
  auto col_type = col->type();
  switch (type->id()) {
#define SET_ANY_FOR_INTERVAL_FROM_STRING_ARRAY(ARROW_TYPE, ARROW_ARRAY_TYPE) \
  case ARROW_TYPE:                                                           \
    for (auto j = 0; j < array->num_chunks(); ++j) {                         \
      auto casted =                                                          \
          std::static_pointer_cast<ARROW_ARRAY_TYPE>(array->chunk(j));       \
      for (auto k = 0; k < casted->length(); ++k) {                          \
        if (vids[k] >= std::numeric_limits<vid_t>::max()) {                  \
          continue;                                                          \
        }                                                                    \
        col->set_any(vids[k],                                                \
                     std::move(PropUtils<Interval>::to_prop(                 \
                         Interval(casted->GetView(k)))),                     \
                     false);                                                 \
      }                                                                      \
    }                                                                        \
    break;
    SET_ANY_FOR_INTERVAL_FROM_STRING_ARRAY(arrow::Type::LARGE_STRING,
                                           arrow::LargeStringArray)
    SET_ANY_FOR_INTERVAL_FROM_STRING_ARRAY(arrow::Type::STRING,
                                           arrow::StringArray)
  default:
    LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
               << col_type;
#undef SET_ANY_FOR_INTERVAL_FROM_STRING_ARRAY
  }
}

void set_column_from_string_array(std::shared_ptr<neug::ColumnBase> col,
                                  std::shared_ptr<arrow::ChunkedArray> array,
                                  const std::vector<vid_t>& vids,
                                  std::shared_mutex& rw_mutex,
                                  bool enable_resize = false) {
  auto type = array->type();
  auto typed_col =
      dynamic_cast<neug::TypedColumn<std::string_view>*>(col.get());
  if (enable_resize) {
    CHECK(typed_col != nullptr) << "Only support TypedColumn<std::string_view>";
  }
  CHECK(type->Equals(arrow::large_utf8()) || type->Equals(arrow::utf8()))
      << "Inconsistent data type, expect string, but got " << type->ToString();
  if (type->Equals(arrow::large_utf8())) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::LargeStringArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (vids[k] >= std::numeric_limits<vid_t>::max()) {
          continue;
        }
        auto str = casted->GetView(k);
        std::string_view sw;
        if (casted->IsNull(k)) {
          VLOG(1) << "Found null string in vertex property.";
          sw = "";
        } else {
          sw = std::string_view(str.data(), str.size());
        }
        if (!enable_resize) {
          Property any_val = Property::From(sw);
          col->set_any(vids[k], any_val, false);
        } else {
          std::shared_lock<std::shared_mutex> lock(rw_mutex);
          if (typed_col->available_space() <= sw.size()) {
            lock.unlock();
            std::unique_lock<std::shared_mutex> w_lock(rw_mutex);
            typed_col->resize(typed_col->size());
            w_lock.unlock();
            lock.lock();
          }
          typed_col->set_value(vids[k], std::move(sw));
        }
      }
    }
  } else {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::StringArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (vids[k] >= std::numeric_limits<vid_t>::max()) {
          continue;
        }
        auto str = casted->GetView(k);
        std::string_view sw(str.data(), str.size());

        if (!enable_resize) {
          Property any_val = Property::From(sw);
          col->set_any(vids[k], std::move(any_val), false);
        } else {
          std::shared_lock<std::shared_mutex> lock(rw_mutex);
          if (typed_col->available_space() <= sw.size()) {
            lock.unlock();
            std::unique_lock<std::shared_mutex> w_lock(rw_mutex);
            typed_col->resize(typed_col->size());
            w_lock.unlock();
            lock.lock();
          }
          typed_col->set_value(vids[k], std::move(sw));
        }
      }
    }
  }
}

void set_properties_column(std::shared_ptr<neug::ColumnBase> col,
                           std::shared_ptr<arrow::ChunkedArray> array,
                           const std::vector<vid_t>& vids,
                           std::shared_mutex& mutex) {
  auto type = array->type();
  auto col_type = col->type();

  // TODO(zhanglei): reduce the dummy code here with a template function.
  switch (col_type) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    set_column<type>(col, array, vids); \
    break;
    FOR_EACH_DATA_TYPE_PRIMITIVE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kTimestampMs:
    set_column_from_timestamp_array<DateTime>(col, array, vids);
    break;
  case DataTypeId::kDate:
    set_column_from_date_array(col, array, vids);
    break;
  case DataTypeId::kInterval:
    set_interval_column_from_string_array(col, array, vids);
    break;
  case DataTypeId::kVarchar:
    set_column_from_string_array(col, array, vids, mutex, true);
    break;
  default:
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}

}  // namespace neug