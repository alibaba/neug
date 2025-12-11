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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/read_transaction.h"
#include "neug/utils/property/types.h"

namespace gs {

namespace runtime {
class IContextColumn;
template <typename T>
class ValueColumn;

class Context {
 public:
  Context();

  ~Context() = default;

  void clear();

  void set(int alias, std::shared_ptr<IContextColumn> col);

  void set_with_reshuffle(int alias, std::shared_ptr<IContextColumn> col,
                          const std::vector<size_t>& offsets);

  void reshuffle(const std::vector<size_t>& offsets);
  void optional_reshuffle(const std::vector<size_t>& offsets);

  std::shared_ptr<IContextColumn> get(int alias);

  const std::shared_ptr<IContextColumn> get(int alias) const;

  void remove(int alias);

  size_t row_num() const;

  bool exist(int alias) const;

  void desc(const std::string& info = "") const;

  void show(const StorageReadInterface& graph) const;

  size_t col_num() const;

  Context union_ctx(const Context& ctx) const;

  std::vector<std::shared_ptr<IContextColumn>> columns;
  std::shared_ptr<IContextColumn> head;

  std::vector<int> tag_ids;
};

class ContextMeta {
 public:
  ContextMeta() = default;
  ~ContextMeta() = default;

  bool exist(int alias) const {
    if (alias == -1) {
      return head_exists_;
    }
    return alias_set_.find(alias) != alias_set_.end();
  }

  void set(int alias) {
    if (alias >= 0) {
      head_ = alias;
      head_exists_ = true;
      alias_set_.insert(alias);
    }
  }

  const std::set<int>& columns() const { return alias_set_; }

  void desc() const {
    std::cout << "===============================================" << std::endl;
    for (auto col : alias_set_) {
      std::cout << "col - " << col << std::endl;
    }
    if (head_exists_) {
      std::cout << "head - " << head_ << std::endl;
    }
  }

 private:
  std::set<int> alias_set_;
  int head_ = -1;
  bool head_exists_ = false;
};

}  // namespace runtime

}  // namespace gs
