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

#ifndef RUNTIME_COMMON_COLUMNS_ARROW_COLUMN_H_
#define RUNTIME_COMMON_COLUMNS_ARROW_COLUMN_H_

#include "src/engines/graph_db/runtime/common/columns/i_context_column.h"

namespace gs {
namespace runtime {
class ArrowContextColumn : public IContextColumn {
 public:
  ArrowContextColumn(const std::vector<std::shared_ptr<arrow::Array>>& columns)
      : columns_(columns), size_(0) {
    for (const auto& column : columns_) {
      size_ += column->length();
    }
    type_ = RTAnyType::kEmpty;
    if (columns_.size() > 0) {
      type_ = arrow_type_to_rt_type(columns_[0]->type());
    }
  }

  ~ArrowContextColumn() = default;

  std::string column_info() const override { return "ArrowContextColumn"; }

  size_t size() const override { return size_; }

  RTAnyType elem_type() const override { return type_; }

  ContextColumnType column_type() const override {
    return ContextColumnType::kArrow;
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

 private:
  std::vector<std::shared_ptr<arrow::Array>> columns_;
  size_t size_;
  RTAnyType type_;
};

class ArrowContextColumnBuilder : public IContextColumnBuilder {
 public:
  ArrowContextColumnBuilder() = default;
  ~ArrowContextColumnBuilder() = default;

  void reserve(size_t size) override {
    LOG(FATAL) << "not implemented for arrow column";
  }
  void push_back_elem(const RTAny& val) {
    LOG(FATAL) << "not implemented for arrow column";
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>& arena) override;

  void push_back(const std::shared_ptr<arrow::Array>& column);

 private:
  std::vector<std::shared_ptr<arrow::Array>> columns_;
};

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_COLUMNS_ARROW_COLUMN_H_