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

#include "neug/execution/common/columns/i_context_column.h"

namespace neug {
namespace execution {
class StructColumnBuilder;

class StructColumn : public IContextColumn {
 public:
  StructColumn() = default;
  ~StructColumn() = default;

  size_t size() const override {
    if (children_.empty()) {
      return 0;
    }
    return children_[0]->size();
  }

  std::string column_info() const override {
    return "StructColumn[" + std::to_string(size()) + "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  const DataType& elem_type() const override { return type_; }
  Value get_elem(size_t idx) const override;

  bool is_optional() const override { return is_optional_; }

  bool has_value(size_t idx) const override {
    if (!is_optional_) {
      return true;
    }
    return valids_[idx];
  }

  const std::vector<std::shared_ptr<IContextColumn>>& children() const {
    return children_;
  }

  const std::vector<bool>& validity_bitmap() const { return valids_; }
  friend class StructColumnBuilder;

 private:
  DataType type_;
  bool is_optional_;
  std::vector<bool> valids_;
  std::vector<std::shared_ptr<IContextColumn>> children_;
};

class StructColumnBuilder : public IContextColumnBuilder {
 public:
  StructColumnBuilder(DataType type);
  ~StructColumnBuilder() = default;

  void reserve(size_t size) override {
    for (auto& child_builder : child_builders_) {
      child_builder->reserve(size);
    }
  }

  void push_back_elem(const Value& val) override;
  void push_back_null() override;

  std::shared_ptr<IContextColumn> finish() override;

 private:
  size_t current_size_ = 0;
  DataType type_;
  bool is_optional_;
  std::vector<bool> valids_;
  std::vector<std::shared_ptr<IContextColumnBuilder>> child_builders_;
};

}  // namespace execution
}  // namespace neug