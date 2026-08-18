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

#include "neug/common/columns/columns_utils.h"
#include "neug/common/extra_type_info.h"
#include "neug/common/types/i_context_column.h"
#include "neug/common/types/value.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/default_value.h"

namespace neug {

class ContextArrayColumnBuilder;

/**
 * @brief Fixed-length array column in the execution layer.
 *
 * Unlike ListColumn which stores variable-length item offsets,
 * ContextArrayColumn stores elements flat: row i element j is at datas_[i *
 * array_size_ + j]. No items_ overhead — simpler and more cache-friendly.
 */
class ContextArrayColumn : public IContextColumn {
 public:
  explicit ContextArrayColumn(const DataType& array_type)
      : elem_type_(ArrayType::GetChildType(array_type)),
        type_(array_type),
        array_size_(ArrayType::GetNumElements(array_type)) {}
  ~ContextArrayColumn() = default;

  size_t size() const override {
    if (!datas_) {
      return 0;
    }
    return datas_->size() / array_size_;
  }

  std::string column_info() const override {
    return "ContextArrayColumn[" + std::to_string(size()) + "][" +
           std::to_string(array_size_) + "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  const DataType& elem_type() const override { return type_; }

  std::shared_ptr<IContextColumn> shuffle(
      const sel_vec_t& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const sel_vec_t& offsets) const override;

  Value get_elem(size_t idx) const override {
    if (!has_value(idx)) {
      return Value(type_);
    }
    std::vector<Value> values;
    values.reserve(array_size_);
    size_t base = idx * array_size_;
    for (uint64_t j = 0; j < array_size_; ++j) {
      values.emplace_back(datas_->get_elem(base + j));
    }
    return Value::ARRAY(type_, std::move(values));
  }

  bool is_optional() const override { return is_optional_; }

  bool has_value(size_t idx) const override {
    return !is_optional_ || valids_[idx];
  }

  std::pair<std::shared_ptr<IContextColumn>, sel_vec_t> unfold() const;

  std::shared_ptr<IContextColumn> data_column() const { return datas_; }
  uint64_t array_size() const { return array_size_; }
  const vector_t<bool>& validity_bitmap() const { return valids_; }

 private:
  friend class ContextArrayColumnBuilder;
  DataType elem_type_;
  DataType type_;
  uint64_t array_size_;
  std::shared_ptr<IContextColumn> datas_;
  bool is_optional_ = false;
  vector_t<bool> valids_;
};

/**
 * @brief Builder for ContextArrayColumn.
 */
class ContextArrayColumnBuilder : public IContextColumnBuilder {
 public:
  explicit ContextArrayColumnBuilder(const DataType& array_type)
      : elem_type_(ArrayType::GetChildType(array_type)),
        array_type_(array_type),
        array_size_(ArrayType::GetNumElements(array_type)) {
    child_builder_ = ColumnsUtils::create_builder(elem_type_);
  }

  ~ContextArrayColumnBuilder() = default;

  void reserve(size_t size) override {
    if (child_builder_) {
      child_builder_->reserve(size * array_size_);
    }
  }

  void push_back_elem(const Value& val) override {
    if (val.IsNull()) {
      push_back_null();
      return;
    }
    if (val.type().id() != DataTypeId::kArray) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "ContextArrayColumnBuilder: expected ARRAY value, got " +
          val.type().ToString());
    }
    const auto& children = ArrayValue::GetChildren(val);
    if (children.size() != array_size_) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "ContextArrayColumnBuilder: expected " + std::to_string(array_size_) +
          " elements, got " + std::to_string(children.size()));
    }
    for (const auto& v : children) {
      child_builder_->push_back_elem(v);
    }
    ++current_size_;
  }

  void push_back_null() override {
    if (!is_optional_) {
      is_optional_ = true;
    }
    valids_.resize(current_size_, true);
    const auto child_default = get_default_value(elem_type_);
    for (uint64_t i = 0; i < array_size_; ++i) {
      child_builder_->push_back_elem(child_default);
    }
    valids_.push_back(false);
    ++current_size_;
  }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<ContextArrayColumn>(array_type_);
    ret->datas_ = child_builder_->finish();
    if (is_optional_) {
      valids_.resize(current_size_, true);
      ret->valids_.swap(valids_);
      ret->is_optional_ = true;
    }
    return ret;
  }

 private:
  DataType elem_type_;
  DataType array_type_;
  uint64_t array_size_;
  std::shared_ptr<IContextColumnBuilder> child_builder_;
  size_t current_size_ = 0;
  bool is_optional_ = false;
  vector_t<bool> valids_;
};

}  // namespace neug
