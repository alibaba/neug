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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <string>

namespace gs {

void put_boolean_option(arrow::csv::ConvertOptions& convert_options);

void put_delimiter_option(const std::string& delimiter_str,
                          arrow::csv::ParseOptions& parse_options);

class IRecordBatchSupplier {
 public:
  virtual ~IRecordBatchSupplier() = default;
  virtual std::shared_ptr<arrow::RecordBatch> GetNextBatch() = 0;
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

class ArrayRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  ArrayRecordBatchSupplier(
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

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_