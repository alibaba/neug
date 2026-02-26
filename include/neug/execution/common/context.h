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
#include <unordered_map>
#include "neug/common/types.h"

namespace neug {
class StorageReadInterface;

namespace execution {
class IContextColumn;

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
    return alias_set_.find(alias) != alias_set_.end();
  }

  void set(int32_t alias, const DataType& type) {
    if (alias >= 0) {
      alias_set_.emplace(alias, type);
    }
  }

  DataType get(int32_t alias) const { return alias_set_.at(alias); }

  const std::unordered_map<int32_t, DataType>& columns() const {
    return alias_set_;
  }

  void desc() const;

 private:
  std::unordered_map<int32_t, DataType> alias_set_;
};

}  // namespace execution

}  // namespace neug
