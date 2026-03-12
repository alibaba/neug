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

#include <cerrno>
#include <fstream>

#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/execution/execute/ops/batch/writer/csv_export_writer.h"

#include "neug/execution/common/columns/i_context_column.h"

namespace neug {
namespace execution {

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
    if (csv_options.at(ops::CSV_HEADER_KEY) == "True" ||
        csv_options.at(ops::CSV_HEADER_KEY) == "true" ||
        csv_options.at(ops::CSV_HEADER_KEY) == "TRUE" ||
        csv_options.at(ops::CSV_HEADER_KEY) == "1") {
      write_header_ = true;
    } else if (csv_options.at(ops::CSV_HEADER_KEY) == "False" ||
               csv_options.at(ops::CSV_HEADER_KEY) == "false" ||
               csv_options.at(ops::CSV_HEADER_KEY) == "FALSE" ||
               csv_options.at(ops::CSV_HEADER_KEY) == "0") {
      write_header_ = false;
    } else {
      LOG(FATAL) << "Invalid parameter, key: \"" << ops::CSV_HEADER_KEY
                 << "\", value: \"" << csv_options.at(ops::CSV_HEADER_KEY)
                 << "\"";
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

Status CsvExportWriter::Write(
    const std::vector<std::shared_ptr<IContextColumn>>& columns_map,
    const StorageReadInterface& graph) {
  std::string dir_path;
  if (file_path_.find_last_of("/\\") != std::string::npos) {
    dir_path = file_path_.substr(0, file_path_.find_last_of("/\\"));
  }
  if (!dir_path.empty() && (!std::filesystem::exists(dir_path) ||
                            !std::filesystem::is_directory(dir_path))) {
    LOG(ERROR) << "Directory \"" << dir_path << "\" does not exist";
    return Status(StatusCode::ERR_DIRECTORY_NOT_EXIST,
                  "Directory \"" + dir_path + "\" does not exist");
  }
  CHECK_GT(columns_map.size(), 0);
  size_t row_num = columns_map[0]->size();
  for (size_t i = 1; i < columns_map.size(); i++) {
    CHECK_EQ(columns_map[i]->size(), row_num);
  }
  std::ofstream ofs;
  ofs.open(file_path_, std::ios::out);
  if (!ofs) {
    int error_code = errno;
    std::string error = strerror(error_code);
    switch (error_code) {
    case EACCES:
      return Status(StatusCode::ERR_PERMISSION,
                    "Failed to open file " + file_path_ + ";" + error);
    case EISDIR:
      return Status(StatusCode::ERR_INVALID_PATH,
                    "Failed to open file " + file_path_ + ";" + error);
    case EMFILE:
      return Status(StatusCode::ERR_INTERNAL_ERROR,
                    "Failed to open file " + file_path_ + ";" + error);
    case ENOSPC:
      return Status(StatusCode::ERR_DISK_SPACE_EXHAUSTED,
                    "Failed to open file " + file_path_ + ";" + error);
    default:
      return Status(StatusCode::ERR_UNKNOWN,
                    "Failed to open file " + file_path_ + ";" + error);
    }
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
        auto vertex = columns_map[j]->get_elem(i).GetValue<vertex_t>();
        ofs << ops::vertex_to_json_string(vertex.label_, vertex.vid_, graph);
      } else if (columns_map[j]->column_type() == ContextColumnType::kEdge) {
        auto edge = columns_map[j]->get_elem(i).GetValue<edge_t>();
        ofs << ops::edge_to_json_string(edge, graph);
      } else if (columns_map[j]->column_type() == ContextColumnType::kPath) {
        auto path = PathValue::Get(columns_map[j]->get_elem(i));
        ofs << ops::path_to_json_string(path, graph);
      } else {
        ofs << columns_map[j]->get_elem(i).to_string();
      }
    }
    ofs << "\n";
  }
  ofs.close();
  return Status::OK();
}

const bool CsvExportWriter::registered_ = ExportWriterFactory::Register(
    "csv", static_cast<ExportWriterFactory::writer_initializer_t>(
               &CsvExportWriter::Make));

}  // namespace execution

}  // namespace neug
