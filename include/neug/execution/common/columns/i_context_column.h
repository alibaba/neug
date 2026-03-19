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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/execution/common/types/value.h"

#include "glog/logging.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace execution {

enum class ContextColumnType {
  kVertex,
  kEdge,
  kValue,
  kPath,
  kArrowArray,
  kArrowStream,
  kNone,
};

class IContextColumnBuilder;

class IContextColumn {
 public:
  IContextColumn() = default;
  virtual ~IContextColumn() = default;

  virtual size_t size() const = 0;

  virtual std::string column_info() const = 0;
  virtual ContextColumnType column_type() const = 0;

  virtual const DataType& elem_type() const = 0;

  virtual std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const {
    LOG(FATAL) << "shuffle not implemented for " << this->column_info();
    return nullptr;
  }

  virtual std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const {
    LOG(FATAL) << "optional_shuffle not implemented for "
               << this->column_info();
    return nullptr;
  }

  virtual std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const {
    LOG(FATAL) << "union_col not implemented for " << this->column_info();
    return nullptr;
  }

  virtual Value get_elem(size_t idx) const = 0;
  virtual bool has_value(size_t idx) const { return true; }

  virtual bool is_optional() const { return false; }

  virtual bool generate_dedup_offset(std::vector<size_t>& offsets) const {
    LOG(ERROR) << "generate_dedup_offset not implemented for "
               << this->column_info() << ", return false by default";
    return false;
  }

  virtual std::pair<std::shared_ptr<IContextColumn>,
                    std::vector<std::vector<size_t>>>
  generate_aggregate_offset() const {
    LOG(INFO) << "generate_aggregate_offset not implemented for "
              << this->column_info() << ", return empty by default";
    std::shared_ptr<IContextColumn> col(nullptr);
    return std::make_pair(col, std::vector<std::vector<size_t>>());
  }

  virtual bool order_by_limit(bool asc, size_t limit,
                              std::vector<size_t>& offsets) const {
    LOG(ERROR) << "order by limit not implemented for " << this->column_info()
               << ", return false by default";
    return false;
  }
};

class IContextColumnBuilder {
 public:
  IContextColumnBuilder() = default;
  virtual ~IContextColumnBuilder() = default;

  virtual void reserve(size_t size) = 0;
  virtual void push_back_elem(const Value& val) = 0;
  virtual void push_back_null() {
    LOG(FATAL) << "push_back_null not implemented";
  }

  virtual std::shared_ptr<IContextColumn> finish() = 0;
};

}  // namespace execution

}  // namespace neug
