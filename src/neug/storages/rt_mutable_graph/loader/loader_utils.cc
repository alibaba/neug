

#include "neug/storages/rt_mutable_graph/loader/loader_utils.h"

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/csv/options.h>
#include <arrow/csv/reader.h>
#include <arrow/io/file.h>
#include <arrow/io/type_fwd.h>
#include <glog/logging.h>
#include <stdint.h>
#include <ostream>

namespace gs {

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

CSVStreamRecordBatchSupplier::CSVStreamRecordBatchSupplier(
    const std::string& file_path, arrow::csv::ConvertOptions convert_options,
    arrow::csv::ReadOptions read_options,
    arrow::csv::ParseOptions parse_options)
    : file_path_(file_path) {
  auto read_result = arrow::io::ReadableFile::Open(file_path);
  if (!read_result.ok()) {
    LOG(FATAL) << "Failed to open file: " << file_path
               << " error: " << read_result.status().message();
  }
  auto file = read_result.ValueOrDie();
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
    return nullptr;
  }
  auto result = reader_->ReadNext();
  if (result.ok()) {
    auto batch = result.ValueOrDie().batch;
    auto metadata = result.ValueOrDie().custom_metadata;
    if (metadata) {
      // Handle metadata if needed
      LOG(INFO) << "Batch metadata: " << metadata->ToString();
    }
    return batch;
  } else {
    LOG(INFO) << "No more batches";
    return nullptr;  // Handle error appropriately in production code
  }
}
}  // namespace gs