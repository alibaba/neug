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

#include "neug/execution/common/columns/columns_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/top_n_generator.h"

namespace neug {

namespace execution {

template <typename T>
class ValueColumnBuilder;

template <typename T>
class ValueColumn : public IContextColumn {
 public:
  ValueColumn() : is_optional_(false), type_(ValueConverter<T>::type()) {}
  ~ValueColumn() = default;

  inline size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "ValueColumn<" + ValueConverter<T>::name() + ">[" +
           std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline const DataType& elem_type() const override { return type_; }
  inline Value get_elem(size_t idx) const override {
    if (is_optional_ && !valid_[idx]) {
      return Value(type_);
    }
    return Value::CreateValue<T>(data_[idx]);
  }

  inline T get_value(size_t idx) const { return data_[idx]; }

  const std::vector<T>& data() const { return data_; }
  const std::vector<bool>& validity_bitmap() const { return valid_; }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    if (!is_optional_) {
      return ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
    }
    std::set<T> st;
    size_t null_index = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < data_.size(); ++i) {
      if (valid_[i]) {
        if (st.find(data_[i]) == st.end()) {
          st.insert(data_[i]);
          offsets.push_back(i);
        }
      } else {
        null_index = i;
      }
    }
    if (null_index != std::numeric_limits<size_t>::max()) {
      offsets.push_back(null_index);
    }
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override;

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override;

  bool has_value(size_t idx) const override {
    if (!is_optional_) {
      return true;
    }
    return valid_[idx];
  }

  bool is_optional() const override { return is_optional_; }

 private:
  template <typename _T>
  friend class ValueColumnBuilder;
  std::vector<T> data_;
  std::vector<bool> valid_;
  bool is_optional_;
  DataType type_;
};

template <typename T>
class ValueColumnBuilder : public IContextColumnBuilder {
 public:
  ValueColumnBuilder(bool is_optional = false) : is_optional_(is_optional) {}
  ~ValueColumnBuilder() = default;

  void reserve(size_t size) override {
    data_.reserve(size);
    if (is_optional_) {
      valid_.reserve(size);
    }
  }
  inline void push_back_elem(const Value& val) override {
    data_.push_back(val.template GetValue<T>());
  }

  inline void push_back_opt(const T& val) { data_.push_back(val); }
  inline void push_back_null() override {
    if (valid_.empty()) {
      valid_.reserve(data_.capacity());
      is_optional_ = true;
    }
    valid_.resize(data_.size(), true);
    valid_.push_back(false);
    data_.emplace_back(T());
  }

  std::shared_ptr<IContextColumn> finish() override {
    if (is_optional_) {
      auto ret = std::make_shared<ValueColumn<T>>();
      valid_.resize(data_.size(), true);
      ret->data_.swap(data_);
      ret->valid_.swap(valid_);
      ret->is_optional_ = true;
      return ret;
    } else {
      auto ret = std::make_shared<ValueColumn<T>>();
      ret->data_.swap(data_);
      ret->is_optional_ = false;
      ret->valid_.clear();
      return ret;
    }
  }

 private:
  bool is_optional_;
  std::vector<bool> valid_;
  std::vector<T> data_;
};

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::shuffle(
    const std::vector<size_t>& offsets) const {
  ValueColumnBuilder<T> builder;
  builder.reserve(offsets.size());
  if (!is_optional_) {
    for (auto offset : offsets) {
      builder.push_back_opt(data_[offset]);
    }
  } else {
    for (auto offset : offsets) {
      if (!valid_[offset]) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(data_[offset]);
      }
    }
  }
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  ValueColumnBuilder<T> builder(true);
  builder.reserve(offsets.size());
  if (!is_optional_) {
    for (auto offset : offsets) {
      if (offset == std::numeric_limits<size_t>::max()) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(data_[offset]);
      }
    }
  } else {
    for (auto offset : offsets) {
      if (offset == std::numeric_limits<size_t>::max() || !valid_[offset]) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(data_[offset]);
      }
    }
  }
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::union_col(
    std::shared_ptr<IContextColumn> other) const {
  ValueColumnBuilder<T> builder;
  if (is_optional_) {
    for (size_t i = 0; i < data_.size(); ++i) {
      if (!valid_[i]) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(data_[i]);
      }
    }
  } else {
    for (auto v : data_) {
      builder.push_back_opt(v);
    }
  }
  const ValueColumn<T>& rhs = *std::dynamic_pointer_cast<ValueColumn<T>>(other);
  if (rhs.is_optional_) {
    for (size_t i = 0; i < rhs.data_.size(); ++i) {
      if (!rhs.valid_[i]) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(rhs.data_[i]);
      }
    }
  } else {
    for (auto v : rhs.data_) {
      builder.push_back_opt(v);
    }
  }
  return builder.finish();
}

template <typename T>
bool ValueColumn<T>::order_by_limit(bool asc, size_t limit,
                                    std::vector<size_t>& offsets) const {
  if (is_optional_) {
    return false;
  }
  size_t size = data_.size();
  if (size == 0) {
    return false;
  }
  if (asc) {
    TopNGenerator<T, TopNAscCmp<T>> generator(limit);
    for (size_t i = 0; i < size; ++i) {
      generator.push(data_[i], i);
    }
    generator.generate_indices(offsets);
  } else {
    TopNGenerator<T, TopNDescCmp<T>> generator(limit);
    for (size_t i = 0; i < size; ++i) {
      generator.push(data_[i], i);
    }
    generator.generate_indices(offsets);
  }
  return true;
}

}  // namespace execution

}  // namespace neug
