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

#include "neug/execution/common/columns/value_columns.h"

namespace neug {
namespace execution {

class ListColumnBase : public IContextColumn {
 public:
  virtual std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
  unfold() const = 0;
};

struct list_item {
  uint64_t offset;
  uint64_t length;
};

class ListColumnBuilder;

class ListColumn : public ListColumnBase {
 public:
  explicit ListColumn(DataType type) : elem_type_(type) {
    std::shared_ptr<ExtraTypeInfo> elem_type_info =
        std::make_shared<ListTypeInfo>(elem_type_);
    type_ = DataType(DataTypeId::kList, elem_type_info);
  }
  ~ListColumn() = default;

  size_t size() const override { return items_.size(); }

  std::string column_info() const override {
    return "ListColumn[" + std::to_string(size()) + "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  const DataType& elem_type() const override { return type_; }
  Value get_elem(size_t idx) const override {
    std::vector<Value> list_values;
    for (size_t i = items_[idx].offset;
         i < items_[idx].offset + items_[idx].length; ++i) {
      list_values.emplace_back(datas_->get_elem(i));
    }
    return Value::LIST(elem_type_, std::move(list_values));
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    // ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>> unfold()
      const override;

  std::shared_ptr<IContextColumn> data_column() const { return datas_; }

  const std::vector<list_item>& items() const { return items_; }

  std::shared_ptr<IContextColumn> reorder() const {
    auto ptr = std::make_shared<ListColumn>(elem_type_);
    std::vector<list_item> new_items(items_.size());
    size_t cur_offset = 0;
    std::vector<size_t> indices;
    indices.reserve(datas_->size());
    for (size_t i = 0; i < items_.size(); ++i) {
      new_items[i].offset = cur_offset;
      new_items[i].length = items_[i].length;
      cur_offset += items_[i].length;
      for (size_t j = items_[i].offset; j < items_[i].offset + items_[i].length;
           ++j) {
        indices.push_back(j);
      }
    }
    ptr->items_.swap(new_items);

    ptr->datas_ = datas_->shuffle(indices);
    return ptr;
  }

 private:
  template <typename T>
  std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>> unfold_impl()
      const {
    std::vector<size_t> offsets;
    auto builder = std::make_shared<ValueColumnBuilder<T>>();
    size_t i = 0;
    for (const auto& list : items_) {
      for (size_t j = list.offset; j < list.offset + list.length; ++j) {
        builder->push_back_elem(datas_->get_elem(j));
        offsets.push_back(i);
      }
      ++i;
    }

    return {builder->finish(), offsets};
  }
  friend class ListColumnBuilder;
  DataType elem_type_;
  DataType type_;
  std::vector<list_item> items_;
  std::shared_ptr<IContextColumn> datas_;
};

class ListColumnBuilder : public IContextColumnBuilder {
 public:
  explicit ListColumnBuilder(DataType type) : type_(type), cur_offset_(0) {
    child_builder_ = ColumnsUtils::create_builder(type_);
  }

  ~ListColumnBuilder() = default;

  void reserve(size_t size) override { items_.reserve(size); }
  void push_back_elem(const Value& val) override {
    assert(val.type().id() == DataTypeId::kList);
    const auto& values = ListValue::GetChildren(val);
    for (const auto& v : values) {
      child_builder_->push_back_elem(v);
    }
    list_item item = {cur_offset_, values.size()};
    items_.push_back(item);
    cur_offset_ += values.size();
  }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<ListColumn>(type_);
    ret->datas_ = child_builder_->finish();
    ret->items_.swap(items_);
    return ret;
  }

 private:
  DataType type_;
  size_t cur_offset_;

  std::vector<list_item> items_;
  std::shared_ptr<IContextColumnBuilder> child_builder_;
};

}  // namespace execution
}  // namespace neug