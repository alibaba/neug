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
#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <glog/logging.h>
#include <stddef.h>

#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/storages/loader/loading_config.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/string_utils.h"

namespace arrow {
class Array;
class RecordBatch;
class Schema;
class Table;
class TableBatchReader;

namespace csv {
class StreamingReader;
struct ConvertOptions;
struct ParseOptions;
struct ReadOptions;
}  // namespace csv
}  // namespace arrow

namespace neug {

void printDiskRemaining(const std::string& path);

void put_boolean_option(arrow::csv::ConvertOptions& convert_options);

void put_delimiter_option(const std::string& delimiter_str,
                          arrow::csv::ParseOptions& parse_options);

std::string process_header_row_token(const std::string& token, bool is_quoting,
                                     char quote_char, bool is_escaping,
                                     char escape_char);

std::vector<std::string> read_header(const std::string& file_name,
                                     char delimiter, bool is_quoting,
                                     char quote_char, bool is_escaping,
                                     char escape_char);

std::vector<std::string> columnMappingsToSelectedCols(
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings);

void put_column_names_option(bool header_row, const std::string& file_path,
                             char delimiter, bool is_quoting, char quote_char,
                             bool is_escaping, char escape_char,
                             arrow::csv::ReadOptions& read_options, size_t len);

class IRecordBatchSupplier {
 public:
  virtual ~IRecordBatchSupplier() = default;
  virtual std::shared_ptr<arrow::RecordBatch> GetNextBatch() = 0;
};

class SupplierWrapperWithFirstBatch : public IRecordBatchSupplier {
 public:
  explicit SupplierWrapperWithFirstBatch(
      const std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers,
      std::shared_ptr<arrow::RecordBatch> first_batch)
      : suppliers_(suppliers),
        first_batch_(std::move(first_batch)),
        has_first_batch_(true) {}
  SupplierWrapperWithFirstBatch(
      const std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers)
      : suppliers_(suppliers), has_first_batch_(false) {}

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override {
    if (has_first_batch_) {
      has_first_batch_ = false;
      return first_batch_;
    }
    if (suppliers_.empty()) {
      return nullptr;  // No more batches to supply
    }
    if (current_supplier_index_ >= suppliers_.size()) {
      return nullptr;  // No more suppliers left
    }
    auto batch = suppliers_[current_supplier_index_]->GetNextBatch();
    if (!batch) {
      current_supplier_index_++;
      return GetNextBatch();  // Try the next supplier
    }
    return batch;  // Return the batch from the current supplier
  }

 private:
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers_;
  std::shared_ptr<arrow::RecordBatch> first_batch_;
  bool has_first_batch_;
  size_t current_supplier_index_ = 0;
};

class CSVStreamRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  CSVStreamRecordBatchSupplier(const std::string& file_path,
                               arrow::csv::ConvertOptions convert_options,
                               arrow::csv::ReadOptions read_options,
                               arrow::csv::ParseOptions parse_options);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  std::string file_path_;
  std::shared_ptr<arrow::csv::StreamingReader> reader_;
};

class CSVTableRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  CSVTableRecordBatchSupplier(const std::string& file_path,
                              arrow::csv::ConvertOptions convert_options,
                              arrow::csv::ReadOptions read_options,
                              arrow::csv::ParseOptions parse_options);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  std::string file_path_;
  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<arrow::TableBatchReader> reader_;
};

/**
 * @brief A record batch supplier that provides all record batches from a
 * vector of arrays. Already in memory.
 */
class ArrowRecordBatchArraySupplier : public IRecordBatchSupplier {
 public:
  ArrowRecordBatchArraySupplier(
      const std::vector<std::vector<std::shared_ptr<arrow::Array>>>& arrays,
      const std::shared_ptr<arrow::Schema>& schema)
      : arrays_(arrays), schema_(schema), current_batch_index_(0) {
    if (arrays_.empty()) {
      batch_num_ = 0;
    } else {
      batch_num_ = arrays_[0].size();
    }
  }

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  // NUM_COLUMNS * NUM_BATCHES
  std::vector<std::vector<std::shared_ptr<arrow::Array>>> arrays_;
  std::shared_ptr<arrow::Schema> schema_;
  size_t current_batch_index_;
  size_t batch_num_;
};

/**
 * @brief A record batch supplier that provides all record batches from a arrow
 * reader, which is a streaming reader or a table reader, producing record
 * batches from a CSV file.
 */
class ArrowRecordBatchStreamSupplier : public IRecordBatchSupplier {
 public:
  ArrowRecordBatchStreamSupplier(
      const std::shared_ptr<arrow::RecordBatchReader>& reader)
      : reader_(reader) {}

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  std::shared_ptr<arrow::RecordBatchReader> reader_;
};

void fillVertexReaderMeta(label_t v_label, const std::string& v_label_name,
                          const std::string& v_file,
                          const LoadingConfig& loading_config,
                          const std::vector<std::string>& vertex_property_names,
                          const std::vector<DataTypeId>& vertex_property_types,
                          DataTypeId pk_type, const std::string& pk_name,
                          size_t pk_ind, arrow::csv::ReadOptions& read_options,
                          arrow::csv::ParseOptions& parse_options,
                          arrow::csv::ConvertOptions& convert_options);

void fillEdgeReaderMeta(label_t src_label_id, label_t dst_label_id,
                        label_t label_id, const std::string& edge_label_name,
                        const std::string& e_file,
                        const LoadingConfig& loading_config,
                        const std::vector<std::string>& edge_property_names,
                        const std::vector<DataTypeId>& edge_property_types,
                        DataTypeId src_pk_type, DataTypeId dst_pk_type,
                        arrow::csv::ReadOptions& read_options,
                        arrow::csv::ParseOptions& parse_options,
                        arrow::csv::ConvertOptions& convert_options);

void set_properties_column(std::shared_ptr<neug::ColumnBase> col,
                           std::shared_ptr<arrow::ChunkedArray> array,
                           const std::vector<vid_t>& vids);

}  // namespace neug
