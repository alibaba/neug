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

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/csv/options.h>

#include <iostream>

#include "src/engines/graph_db/runtime/execute/ops/batch/batch_update_utils.h"
#include "src/engines/graph_db/runtime/execute/writer/csv_export_writer.h"

namespace gs {
namespace runtime {

void put_delimiter_option(const std::string& delimiter_str, char& delimeter) {
  LOG(INFO) << "Put delimiter " << delimiter_str;
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
      delimeter = '\t';
      break;
    default:
      LOG(FATAL) << "Unsupported escape character: " << delimiter_str[1];
    }
  } else {
    delimeter = delimiter_str[0];
  }
}

void CsvExportWriter::parse_csv_options(
    const std::unordered_map<std::string, std::string>& csv_options) {
  if (csv_options.find(ops::CSV_DELIM_KEY) != csv_options.end()) {
    put_delimiter_option(csv_options.at(ops::CSV_DELIM_KEY), delimeter_);
  } else if (csv_options.find(ops::CSV_DELIMITER_KEY) != csv_options.end()) {
    put_delimiter_option(csv_options.at(ops::CSV_DELIMITER_KEY), delimeter_);
  } else {
    put_delimiter_option(ops::DEFAULT_CSV_DELIMITER, delimeter_);
  }
  if (csv_options.find(ops::CSV_HEADER_KEY) != csv_options.end()) {
    LOG(INFO) << "Find csv option " << ops::CSV_HEADER_KEY << " "
              << csv_options.at(ops::CSV_HEADER_KEY);
    if (csv_options.at(ops::CSV_HEADER_KEY) == "True" ||
        csv_options.at(ops::CSV_HEADER_KEY) == "true") {
      write_header_ = true;
    } else {
      write_header_ = false;
    }
  } else {
    write_header_ = false;
  }
  if (csv_options.find(ops::CSV_QUOTE_KEY) != csv_options.end()) {
    LOG(WARNING) << "The parameter \"" << ops::CSV_QUOTE_KEY
                 << "\" is currently not supported.";
  }
  if (csv_options.find(ops::CSV_ESCAPE_KEY) != csv_options.end()) {
    LOG(WARNING) << "The parameter \"" << ops::CSV_ESCAPE_KEY
                 << "\" is currently not supported.";
  }
}

CsvExportWriter::CsvExportWriter(
    const std::string& file_path,
    const std::vector<std::pair<int, std::string>>& header,
    const std::unordered_map<std::string, std::string>& write_config)
    : file_path_(file_path), header_(std::move(header)) {
  if (!ops::check_csv_import_options(write_config)) {
    LOG(ERROR) << "Parameters of CSV export are invalid";
  }
  parse_csv_options(write_config);
}

std::shared_ptr<IExportWriter> CsvExportWriter::Make(
    const std::string& file_path,
    const std::vector<std::pair<int, std::string>>& header,
    const std::unordered_map<std::string, std::string>& write_config) {
  return std::make_shared<CsvExportWriter>(file_path, header, write_config);
}

void CsvExportWriter::Write(
    const std::vector<std::shared_ptr<IContextColumn>>& columns_map,
    const gs::runtime::GraphReadInterface& graph) {
  std::string dir_path;
  if (file_path_.find_last_of("/\\") != std::string::npos) {
    dir_path = file_path_.substr(0, file_path_.find_last_of("/\\"));
  }
  if (!dir_path.empty() && (!std::filesystem::exists(dir_path) ||
                            !std::filesystem::is_directory(dir_path))) {
    LOG(ERROR) << "Directory \"" << dir_path << "\" does not exist";
    return;
  }
  if (columns_map.size() == 0) {
    LOG(ERROR) << "The number of export columns is 0";
    return;
  }
  size_t row_num = columns_map[0]->size();
  for (size_t i = 1; i < columns_map.size(); i++) {
    if (columns_map[i]->size() != row_num) {
      LOG(ERROR) << "The row counts between columns are inconsistent";
      return;
    }
  }
  std::ofstream ofs(file_path_);
  if (!ofs.is_open()) {
    LOG(ERROR) << "Failed to open file \"" << file_path_ << "\"";
    return;
  }
  if (write_header_) {
    for (size_t i = 0; i < header_.size(); i++) {
      if (i > 0) {
        ofs << delimeter_;
      }
      ofs << header_[i].second;
    }
    ofs << "\n";
  }
  for (size_t i = 0; i < row_num; i++) {
    for (size_t j = 0; j < columns_map.size(); j++) {
      if (j > 0) {
        ofs << delimeter_;
      }
      if (columns_map[j]->column_type() == ContextColumnType::kVertex) {
        auto vertex = columns_map[j]->get_elem(i).as_vertex();
        ofs << ops::vertex_to_json_string(vertex.label_, vertex.vid_, graph);
      } else if (columns_map[j]->column_type() == ContextColumnType::kEdge) {
        auto edge = columns_map[j]->get_elem(i).as_edge();
        ofs << ops::edge_to_json_string(edge, graph);
      } else if (columns_map[j]->column_type() == ContextColumnType::kPath) {
        auto path = columns_map[j]->get_elem(i).as_path();
        ofs << ops::path_to_json_string(path, graph);
      } else {
        ofs << columns_map[j]->get_elem(i).to_string();
      }
    }
    ofs << "\n";
  }
  ofs.close();
  return;
}

const bool CsvExportWriter::registered_ = ExportWriterFactory::Register(
    "csv", static_cast<ExportWriterFactory::writer_initializer_t>(
               &CsvExportWriter::Make));

}  // namespace runtime
}  // namespace gs