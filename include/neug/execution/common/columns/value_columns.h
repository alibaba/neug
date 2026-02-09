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
class IValueColumn : public IContextColumn {
 public:
  IValueColumn() = default;
  virtual ~IValueColumn() = default;
  virtual T get_value(size_t idx) const = 0;
};
template <typename T>
class ValueColumn : public IValueColumn<T> {
 public:
  ValueColumn() : type_(ValueConverter<T>::type()) {}
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
    return Value::CreateValue<T>(data_[idx]);
  }

  inline T get_value(size_t idx) const override { return data_[idx]; }

  inline const std::vector<T>& data() const { return data_; }

  ISigColumn* generate_signature() const override {
    if constexpr (std::is_same_v<T, std::string>) {
      return new SigColumn<std::string>(data_);
    } else if constexpr (std::is_same_v<T, bool>) {
      LOG(FATAL) << "not implemented for " << this->column_info();
      return nullptr;
    } else if constexpr (std::is_arithmetic_v<T>) {
      return new SigColumn<T>(data_);
    }
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override;

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override;

 private:
  template <typename _T>
  friend class ValueColumnBuilder;
  std::vector<T> data_;
  DataType type_;
};

template <typename T>
class OptionalValueColumn : public IValueColumn<T> {
 public:
  OptionalValueColumn() : type_(ValueConverter<T>::type()) {}
  ~OptionalValueColumn() = default;

  inline size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "OptionalValueColumn<" + ValueConverter<T>::name() + ">[" +
           std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    ValueColumnBuilder<T> builder(true);
    builder.reserve(offsets.size());
    for (auto offset : offsets) {
      if (!valid_[offset]) {
        builder.push_back_null();
      } else {
        builder.push_back_opt(data_[offset]);
      }
    }
    return builder.finish();
  }

  inline const DataType& elem_type() const override { return type_; }
  inline Value get_elem(size_t idx) const override {
    if (!valid_[idx]) {
      return Value(type_);
    }
    return Value::CreateValue<T>(data_[idx]);
  }

  inline T get_value(size_t idx) const override { return data_[idx]; }

  ISigColumn* generate_signature() const override {
    if constexpr (std::is_same_v<T, std::string>) {
      return new SigColumn<std::string>(data_);
    } else if constexpr (std::is_same_v<T, bool>) {
      LOG(FATAL) << "not implemented for " << this->column_info();
      return nullptr;
    } else if constexpr (std::is_arithmetic_v<T>) {
      return new SigColumn<T>(data_);
    }
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
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

  bool has_value(size_t idx) const override { return valid_[idx]; }
  bool is_optional() const override { return true; }

 private:
  template <typename _T>
  friend class ValueColumnBuilder;
  std::vector<T> data_;
  std::vector<bool> valid_;
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
      auto ret = std::make_shared<OptionalValueColumn<T>>();
      valid_.resize(data_.size(), true);
      ret->data_.swap(data_);
      ret->valid_.swap(valid_);
      return ret;
    } else {
      auto ret = std::make_shared<ValueColumn<T>>();
      ret->data_.swap(data_);
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
  for (auto offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  ValueColumnBuilder<T> builder(true);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(data_[offset]);
    }
  }
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::union_col(
    std::shared_ptr<IContextColumn> other) const {
  ValueColumnBuilder<T> builder;
  for (auto v : data_) {
    builder.push_back_opt(v);
  }
  const ValueColumn<T>& rhs = *std::dynamic_pointer_cast<ValueColumn<T>>(other);
  for (auto v : rhs.data_) {
    builder.push_back_opt(v);
  }
  return builder.finish();
}

template <typename T>
bool ValueColumn<T>::order_by_limit(bool asc, size_t limit,
                                    std::vector<size_t>& offsets) const {
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
