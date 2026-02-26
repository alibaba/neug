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

#include <utility>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/utils/top_n_generator.h"

namespace neug {
class StorageReadInterface;
namespace execution {
class IVertexColumn;
namespace ops {
class GeneralComparer {
 public:
  GeneralComparer() : keys_num_(0) {}
  ~GeneralComparer() {}

  void add_keys(const std::shared_ptr<IContextColumn>& key, bool asc) {
    keys_.emplace_back(key);
    order_.push_back(asc);
    ++keys_num_;
  }

  bool operator()(size_t lhs, size_t rhs) const {
    for (size_t k = 0; k < keys_num_; ++k) {
      auto& v = keys_[k];
      auto asc = order_[k];
      Value lhs_val = v->get_elem(lhs);
      Value rhs_val = v->get_elem(rhs);
      if (lhs_val < rhs_val) {
        return asc;
      } else if (rhs_val < lhs_val) {
        return !asc;
      }
    }

    return lhs < rhs;
  }

 private:
  std::vector<std::shared_ptr<IContextColumn>> keys_;
  std::vector<bool> order_;
  size_t keys_num_;
};

bool vertex_property_topN(bool asc, size_t limit,
                          const std::shared_ptr<IVertexColumn>& col,
                          const StorageReadInterface& graph,
                          const std::string& prop_name,
                          std::vector<size_t>& offsets);
}  // namespace ops
}  // namespace execution
}  // namespace neug
