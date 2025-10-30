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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_COLUMNS_VALUE_COLUMNS_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_COLUMNS_VALUE_COLUMNS_H_

#include <assert.h>
#include <glog/logging.h>
#include <stddef.h>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/columns_utils.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/utils/property/types.h"
#include "neug/utils/runtime/rt_any.h"
#include "neug/utils/top_n_generator.h"

namespace gs {

namespace runtime {

template <typename T>
class ValueColumnBuilder;
template <typename T>
class OptionalValueColumnBuilder;

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
  ValueColumn() {}
  ~ValueColumn() = default;

  inline size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "ValueColumn<" + TypedConverter<T>::name() + ">[" +
           std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline RTAnyType elem_type() const override {
    return TypedConverter<T>::type();
  }
  inline RTAny get_elem(size_t idx) const override {
    return TypedConverter<T>::from_typed(data_[idx]);
  }

  inline T get_value(size_t idx) const override { return data_[idx]; }

  inline const std::vector<T>& data() const { return data_; }

  ISigColumn* generate_signature() const override {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return new SigColumn<std::string_view>(data_);
    } else if constexpr (std::is_same_v<T, bool> ||
                         gs::runtime::is_view_type<T>::value) {
      LOG(FATAL) << "not implemented for " << this->column_info();
      return nullptr;
    } else {
      return new SigColumn<T>(data_);
    }
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override;

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override;

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }

  std::shared_ptr<Arena> get_arena() const override { return arena_; }

 private:
  template <typename _T>
  friend class ValueColumnBuilder;
  std::vector<T> data_;
  std::shared_ptr<Arena> arena_;
};

template <typename T>
class ValueColumnBuilder : public IContextColumnBuilder {
 public:
  ValueColumnBuilder() = default;
  ~ValueColumnBuilder() = default;

  void reserve(size_t size) override { data_.reserve(size); }
  inline void push_back_elem(const RTAny& val) override {
    data_.push_back(TypedConverter<T>::to_typed(val));
  }

  inline void push_back_opt(const T& val) { data_.push_back(val); }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<ValueColumn<T>>();
    ret->set_arena(arena_);
    ret->data_.swap(data_);
    return ret;
  }

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }

 private:
  std::vector<T> data_;
  std::shared_ptr<Arena> arena_;
};

class ListValueColumnBuilder;

class ListValueColumnBase : public IValueColumn<List> {
 public:
  virtual std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
  unfold() const = 0;
};

class ListValueColumn : public ListValueColumnBase {
 public:
  explicit ListValueColumn(RTAnyType type) : elem_type_(type) {}
  ~ListValueColumn() = default;

  size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "ListValueColumn[" + std::to_string(size()) + "]";
  }
  ContextColumnType column_type() const override {
    return ContextColumnType::kValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  RTAnyType elem_type() const override {
    auto type = RTAnyType::kList;
    return type;
  }
  RTAny get_elem(size_t idx) const override {
    return RTAny::from_list(data_[idx]);
  }

  List get_value(size_t idx) const override { return data_[idx]; }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  template <typename T>
  std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>> unfold_impl()
      const {
    std::vector<size_t> offsets;
    auto builder = std::make_shared<ValueColumnBuilder<T>>();
    size_t i = 0;
    for (const auto& list : data_) {
      for (size_t j = 0; j < list.size(); ++j) {
        auto elem = list.get(j);
        builder->push_back_elem(elem);
        offsets.push_back(i);
      }
      ++i;
    }

    if constexpr (gs::runtime::is_view_type<T>::value) {
      // TODO(liulexiao): we shouldn't use the same arena as the original
      // column. The ownership of list elements should be released.
      builder->set_arena(this->get_arena());
      return {builder->finish(), offsets};
    } else {
      return {builder->finish(), offsets};
    }
  }

  std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>> unfold()
      const override {
    if (elem_type_ == RTAnyType::kBoolValue) {
      return unfold_impl<bool>();
    } else if (elem_type_ == RTAnyType::kI64Value) {
      return unfold_impl<int64_t>();
    } else if (elem_type_ == RTAnyType::kI32Value) {
      return unfold_impl<int32_t>();
    } else if (elem_type_ == RTAnyType::kU32Value) {
      return unfold_impl<uint32_t>();
    } else if (elem_type_ == RTAnyType::kU64Value) {
      return unfold_impl<uint64_t>();
    } else if (elem_type_ == RTAnyType::kDate) {
      return unfold_impl<Date>();
    } else if (elem_type_ == RTAnyType::kDateTime) {
      return unfold_impl<DateTime>();
    } else if (elem_type_ == RTAnyType::kTimestamp) {
      return unfold_impl<TimeStamp>();
    } else if (elem_type_ == RTAnyType::kInterval) {
      return unfold_impl<Interval>();
    } else if (elem_type_ == RTAnyType::kF32Value) {
      return unfold_impl<float>();
    } else if (elem_type_ == RTAnyType::kF64Value) {
      return unfold_impl<double>();
    } else if (elem_type_ == RTAnyType::kStringValue) {
      return unfold_impl<std::string_view>();
    } else if (elem_type_ == RTAnyType::kTuple) {
      return unfold_impl<Tuple>();
    } else if (elem_type_ == RTAnyType::kEdge) {
      std::vector<size_t> offsets;
      std::vector<LabelTriplet> labels;
      BDMLEdgeColumnBuilder builder(labels);
      size_t i = 0;
      for (const auto& list : data_) {
        for (size_t j = 0; j < list.size(); ++j) {
          auto elem = list.get(j);
          auto edge = elem.as_edge();
          builder.insert_label(edge.label);
          builder.push_back_opt(edge.label, edge.src, edge.dst, edge.prop,
                                edge.dir);
          offsets.push_back(i);
        }
        ++i;
      }
      return {builder.finish(), offsets};
    } else if (elem_type_ == RTAnyType::kMap) {
      return unfold_impl<Map>();
    } else if (elem_type_ == RTAnyType::kVertex) {
      std::vector<size_t> offsets;
      MLVertexColumnBuilder builder;
      size_t i = 0;
      for (const auto& list : data_) {
        for (size_t j = 0; j < list.size(); ++j) {
          auto elem = list.get(j);
          builder.push_back_elem(elem);
          offsets.push_back(i);
        }
        ++i;
      }
      return {builder.finish(), offsets};
    } else {
      LOG(FATAL) << "not implemented for " << this->column_info() << " "
                 << static_cast<int>(elem_type_);
      return {nullptr, std::vector<size_t>()};
    }
  }

  std::shared_ptr<Arena> get_arena() const override { return arena_; }

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }

 private:
  friend class ListValueColumnBuilder;
  RTAnyType elem_type_;
  std::vector<List> data_;

  std::shared_ptr<Arena> arena_;
};

class ListValueColumnBuilder : public IContextColumnBuilder {
 public:
  explicit ListValueColumnBuilder(RTAnyType type) : type_(type) {}
  ~ListValueColumnBuilder() = default;

  void reserve(size_t size) override { data_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    assert(val.type() == RTAnyType::kList);
    data_.emplace_back(val.as_list());
  }

  void push_back_opt(const List& val) { data_.emplace_back(val); }

  void set_arena(const std::shared_ptr<Arena>& ptr) override { arena_ = ptr; }

  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<ListValueColumn>(type_);
    ret->data_.swap(data_);
    ret->set_arena(arena_);
    return ret;
  }

 private:
  RTAnyType type_;
  std::vector<List> data_;
  std::shared_ptr<Arena> arena_;
};

template <typename T>
class OptionalValueColumn : public IValueColumn<T> {
 public:
  OptionalValueColumn() = default;
  ~OptionalValueColumn() = default;

  inline size_t size() const override { return data_.size(); }

  std::string column_info() const override {
    return "OptionalValueColumn<" + TypedConverter<T>::name() + ">[" +
           std::to_string(size()) + "]";
  }
  inline ContextColumnType column_type() const override {
    return ContextColumnType::kOptionalValue;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    OptionalValueColumnBuilder<T> builder;
    builder.reserve(offsets.size());
    for (auto offset : offsets) {
      builder.push_back_opt(data_[offset], valid_[offset]);
    }
    builder.set_arena(this->get_arena());
    return builder.finish();
  }

  inline RTAnyType elem_type() const override {
    auto type = TypedConverter<T>::type();
    return type;
  }
  inline RTAny get_elem(size_t idx) const override {
    if (!valid_[idx]) {
      return RTAny(RTAnyType::kNull);
    }
    return TypedConverter<T>::from_typed(data_[idx]);
  }

  inline T get_value(size_t idx) const override { return data_[idx]; }

  ISigColumn* generate_signature() const override {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return new SigColumn<std::string_view>(data_);
    } else if constexpr (std::is_same_v<T, bool> ||
                         gs::runtime::is_view_type<T>::value) {
      LOG(FATAL) << "not implemented for " << this->column_info();
      return nullptr;
    } else {
      return new SigColumn<T>(data_);
    }
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    ColumnsUtils::generate_dedup_offset(data_, data_.size(), offsets);
  }

  bool has_value(size_t idx) const override { return valid_[idx]; }
  bool is_optional() const override { return true; }

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }
  std::shared_ptr<Arena> get_arena() const override { return arena_; }

 private:
  template <typename _T>
  friend class OptionalValueColumnBuilder;
  std::vector<T> data_;
  std::vector<bool> valid_;
  std::shared_ptr<Arena> arena_;
};

template <typename T>
class OptionalValueColumnBuilder : public IOptionalContextColumnBuilder {
 public:
  OptionalValueColumnBuilder() = default;
  ~OptionalValueColumnBuilder() = default;

  void reserve(size_t size) override {
    data_.reserve(size);
    valid_.reserve(size);
  }

  inline void push_back_elem(const RTAny& val) override {
    data_.push_back(TypedConverter<T>::to_typed(val));
    valid_.push_back(true);
  }

  inline void push_back_opt(const T& val, bool valid) {
    data_.push_back(val);
    valid_.push_back(valid);
  }

  inline void push_back_null() override {
    data_.emplace_back(T());
    valid_.push_back(false);
  }

  void set_arena(const std::shared_ptr<Arena>& arena) override {
    arena_ = arena;
  }
  std::shared_ptr<IContextColumn> finish() override {
    auto ret = std::make_shared<OptionalValueColumn<T>>();
    ret->data_.swap(data_);
    ret->valid_.swap(valid_);
    ret->set_arena(arena_);
    return std::dynamic_pointer_cast<IContextColumn>(ret);
  }

 private:
  std::vector<T> data_;
  std::vector<bool> valid_;
  std::shared_ptr<Arena> arena_;
};

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::shuffle(
    const std::vector<size_t>& offsets) const {
  ValueColumnBuilder<T> builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  builder.set_arena(this->get_arena());
  return builder.finish();
}

template <typename T>
std::shared_ptr<IContextColumn> ValueColumn<T>::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalValueColumnBuilder<T> builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(data_[offset], true);
    }
  }
  builder.set_arena(this->get_arena());
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
  auto arena1 = this->get_arena();
  auto arena2 = other->get_arena();
  auto arena = std::make_shared<Arena>();
  if (arena1 != nullptr) {
    arena->emplace_back(std::make_unique<ArenaRef>(arena1));
  }
  if (arena2 != nullptr) {
    arena->emplace_back(std::make_unique<ArenaRef>(arena2));
  }
  builder.set_arena(arena);
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

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_COLUMNS_VALUE_COLUMNS_H_
