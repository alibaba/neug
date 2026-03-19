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

#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
namespace arrow {
class DataType;
class RecordBatch;
}  // namespace arrow

namespace neug {
class IRecordBatchSupplier;

namespace execution {

std::pair<size_t, size_t> locate_array_and_offset(
    const std::vector<std::shared_ptr<arrow::Array>>& columns, size_t size,
    size_t idx);

DataType arrow_type_to_rt_type(const std::shared_ptr<arrow::DataType>& type);

/**
 * @brief ArrowArrayContextColumn is a context column that holds multiple
 * arrow::Array objects. It is used to represent a column in the context
 * that can contain multiple arrays, such as when dealing with a record batch
 * or a table in Apache Arrow format.
 */
class ArrowArrayContextColumn : public IContextColumn {
 public:
  ArrowArrayContextColumn(
      const std::vector<std::shared_ptr<arrow::Array>>& columns)
      : columns_(columns), size_(0) {
    for (const auto& column : columns_) {
      size_ += column->length();
    }
    type_ = DataType(DataTypeId::kUnknown);
    if (columns_.size() > 0) {
      type_ = arrow_type_to_rt_type(columns_[0]->type());
    }
  }

  ~ArrowArrayContextColumn() = default;

  std::string column_info() const override { return "ArrowArrayContextColumn"; }

  size_t size() const override { return size_; }

  const DataType& elem_type() const override { return type_; }

  ContextColumnType column_type() const override {
    return ContextColumnType::kArrowArray;
  }

  bool is_optional() const override { return false; }

  const std::vector<std::shared_ptr<arrow::Array>>& GetColumns() const {
    return columns_;
  }

  std::shared_ptr<arrow::DataType> GetArrowType() const {
    if (columns_.size() > 0) {
      return columns_[0]->type();
    }
    return arrow::null();
  }
  Value get_elem(size_t idx) const override;

  bool has_value(size_t idx) const override { return idx >= 0 && idx < size_; }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> cast_to_value_column() const;

 private:
  std::vector<std::shared_ptr<arrow::Array>> columns_;
  size_t size_;
  DataType type_;
};

class ArrowArrayContextColumnBuilder : public IContextColumnBuilder {
 public:
  ArrowArrayContextColumnBuilder() = default;
  ~ArrowArrayContextColumnBuilder() = default;

  void reserve(size_t size) override {
    LOG(FATAL) << "not implemented for arrow column";
  }
  void push_back_elem(const Value& val) override {
    LOG(FATAL) << "not implemented for arrow column";
  }

  std::shared_ptr<IContextColumn> finish() override;

  void push_back(const std::shared_ptr<arrow::Array>& column);

 private:
  std::vector<std::shared_ptr<arrow::Array>> columns_;
};

/**
 * @brief There are num_cols ArrowStreamContextColumn objects for a record
 * batch. Currently it is basiclly not well-implemented, and only workable with
 * BatchInsertVertex/Edge operator followed by.
 */
class ArrowStreamContextColumn : public IContextColumn {
 public:
  ArrowStreamContextColumn(
      const std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers)
      : suppliers_(suppliers) {}

  ~ArrowStreamContextColumn() = default;

  std::string column_info() const override {
    return "ArrowStreamContextColumn";
  }

  size_t size() const override { return suppliers_.size(); }

  const DataType& elem_type() const override { return type_; }

  ContextColumnType column_type() const override {
    return ContextColumnType::kArrowStream;
  }

  std::vector<std::shared_ptr<IRecordBatchSupplier>> GetSuppliers() const {
    return suppliers_;
  }

  Value get_elem(size_t idx) const override {
    LOG(FATAL) << "get_elem not implemented for arrow stream column";
    return Value(DataType::SQLNULL);
  }

 private:
  std::shared_ptr<arrow::RecordBatch> first_batch_;
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers_;
  DataType type_;
};

/**
 * @brief ArrowStreamContextColumnBuilder is a context column builder
 * that is used to build a context column for streaming data in Apache Arrow
 * format. Each column take data from the streamReader's one column.
 */
class ArrowStreamContextColumnBuilder : public IContextColumnBuilder {
 public:
  ArrowStreamContextColumnBuilder(
      const std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers)
      : suppliers_(suppliers) {}
  ~ArrowStreamContextColumnBuilder() = default;

  void reserve(size_t size) override {
    LOG(FATAL) << "not implemented for arrow stream column";
  }
  void push_back_elem(const execution::Value& val) override {
    LOG(FATAL) << "not implemented for arrow stream column";
  }

  std::shared_ptr<IContextColumn> finish() override {
    return std::make_shared<ArrowStreamContextColumn>(suppliers_);
  }

 private:
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers_;
};

}  // namespace execution
}  // namespace neug
