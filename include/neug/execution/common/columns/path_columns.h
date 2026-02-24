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
#include "neug/execution/common/columns/i_context_column.h"

namespace neug {
namespace runtime {

class IPathColumn : public IContextColumn {
 public:
  IPathColumn() = default;
  virtual ~IPathColumn() = default;
  virtual const Path& get_path(size_t idx) const = 0;
  virtual int path_length(size_t idx) const {
    return get_path(idx).length() - 1;
  }
};

class PathColumnBuilder;

class PathColumn : public IPathColumn {
 public:
  PathColumn() : type_(DataType(DataTypeId::kPath)) {}
  ~PathColumn() {}
  inline size_t size() const override { return data_.size(); }
  std::string column_info() const override {
    return "PathColumn[" + std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kPath;
  }
  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;
  inline const DataType& elem_type() const override { return type_; }
  inline Value get_elem(size_t idx) const override {
    return Value::PATH(data_[idx]);
  }
  inline const Path& get_path(size_t idx) const override { return data_[idx]; }
  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  template <typename FUNC>
  void foreach_path(FUNC func) const {
    for (size_t i = 0; i < data_.size(); ++i) {
      const auto& path = data_[i];
      func(i, path);
    }
  }

 private:
  friend class PathColumnBuilder;
  std::vector<Path> data_;
  DataType type_;
};

class OptionalPathColumn : public IPathColumn {
 public:
  OptionalPathColumn() : type_(DataType(DataTypeId::kPath)) {}
  ~OptionalPathColumn() {}
  inline size_t size() const override { return data_.size(); }
  std::string column_info() const override {
    return "OptionalPathColumn[" + std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kPath;
  }
  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  inline bool is_optional() const override { return true; }
  inline const DataType& elem_type() const override { return type_; }
  inline bool has_value(size_t idx) const override { return valids_[idx]; }
  inline Value get_elem(size_t idx) const override {
    if (!valids_[idx]) {
      return Value(DataType(DataTypeId::kNull));
    }
    return Value::PATH(data_[idx]);
  }
  inline const Path& get_path(size_t idx) const override { return data_[idx]; }
  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  template <typename FUNC>
  void foreach_path(FUNC func) const {
    for (size_t i = 0; i < data_.size(); ++i) {
      const auto& path = data_[i];
      func(i, path);
    }
  }

 private:
  friend class PathColumnBuilder;
  std::vector<Path> data_;
  std::vector<bool> valids_;
  DataType type_;
};

class PathColumnBuilder : public IContextColumnBuilder {
 public:
  PathColumnBuilder(bool is_optional = false) : is_optional_(is_optional) {}
  ~PathColumnBuilder() = default;
  inline void push_back_opt(const Path& p) { data_.push_back(p); }
  inline void push_back_elem(const Value& val) override {
    data_.push_back(PathValue::Get(val));
  }
  void push_back_null() override {
    if (!is_optional_) {
      is_optional_ = true;
      valids_.reserve(data_.capacity());
    }
    valids_.resize(data_.size(), true);
    valids_.push_back(false);
    data_.emplace_back();
  }
  void reserve(size_t size) override {
    data_.reserve(size);
    if (is_optional_) {
      valids_.reserve(size);
    }
  }

  std::shared_ptr<IContextColumn> finish() override {
    if (is_optional_) {
      auto col = std::make_shared<OptionalPathColumn>();
      valids_.resize(data_.size(), true);
      col->data_.swap(data_);
      col->valids_.swap(valids_);
      return col;
    } else {
      auto col = std::make_shared<PathColumn>();
      col->data_.swap(data_);
      return col;
    }
  }

 private:
  bool is_optional_ = false;
  std::vector<bool> valids_;
  std::vector<Path> data_;
};

}  // namespace runtime
}  // namespace neug
