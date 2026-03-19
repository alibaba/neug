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

#include "neug/execution/common/columns/struct_columns.h"
#include "neug/execution/common/columns/columns_utils.h"

namespace neug {
namespace execution {
std::shared_ptr<IContextColumn> StructColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  std::vector<std::shared_ptr<IContextColumn>> shuffled_children;
  for (const auto& child : children_) {
    shuffled_children.emplace_back(child->shuffle(offsets));
  }
  auto shuffled_col = std::make_shared<StructColumn>();
  shuffled_col->type_ = type_;
  shuffled_col->children_ = std::move(shuffled_children);
  shuffled_col->is_optional_ = is_optional_;
  if (is_optional_) {
    shuffled_col->valids_.reserve(offsets.size());
    for (auto offset : offsets) {
      shuffled_col->valids_.push_back(valids_[offset]);
    }
  }
  return shuffled_col;
}

std::shared_ptr<IContextColumn> StructColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  std::vector<std::shared_ptr<IContextColumn>> shuffled_children;
  for (const auto& child : children_) {
    shuffled_children.emplace_back(child->optional_shuffle(offsets));
  }
  auto shuffled_col = std::make_shared<StructColumn>();
  shuffled_col->type_ = type_;
  shuffled_col->children_ = std::move(shuffled_children);
  shuffled_col->is_optional_ = true;

  shuffled_col->valids_.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      shuffled_col->valids_.push_back(false);
    } else {
      shuffled_col->valids_.push_back(valids_[offset]);
    }
  }

  return shuffled_col;
}

Value StructColumn::get_elem(size_t idx) const {
  if (is_optional_ && (!valids_[idx])) {
    return Value(type_);
  }
  std::vector<Value> struct_values;
  for (const auto& child : children_) {
    struct_values.emplace_back(child->get_elem(idx));
  }
  return Value::STRUCT(type_, std::move(struct_values));
}

void StructColumn::generate_dedup_offset(std::vector<size_t>& offsets) const {
  LOG(FATAL) << "not implemented for " << this->column_info();
}

StructColumnBuilder::StructColumnBuilder(DataType type) : type_(type) {
  const auto& child_types = StructType::GetChildTypes(type);
  for (const auto& child_type : child_types) {
    child_builders_.emplace_back(ColumnsUtils::create_builder(child_type));
  }
  is_optional_ = false;
  current_size_ = 0;
}

void StructColumnBuilder::push_back_elem(const Value& val) {
  const auto& struct_values = StructValue::GetChildren(val);
  for (size_t i = 0; i < child_builders_.size(); ++i) {
    if (struct_values[i].IsNull()) {
      child_builders_[i]->push_back_null();
    } else {
      child_builders_[i]->push_back_elem(struct_values[i]);
    }
  }
  ++current_size_;
}

void StructColumnBuilder::push_back_null() {
  is_optional_ = true;
  valids_.resize(current_size_ + 1, true);
  valids_[current_size_] = false;
  for (auto& child_builder : child_builders_) {
    child_builder->push_back_null();
  }
  ++current_size_;
}

std::shared_ptr<IContextColumn> StructColumnBuilder::finish() {
  auto struct_col = std::make_shared<StructColumn>();
  struct_col->type_ = type_;
  for (const auto& child_builder : child_builders_) {
    struct_col->children_.emplace_back(child_builder->finish());
  }
  struct_col->is_optional_ = is_optional_;
  valids_.resize(current_size_, true);
  struct_col->valids_.swap(valids_);
  return struct_col;
}

}  // namespace execution
}  // namespace neug