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

#include "neug/execution/execute/ops/retrieve/group_by_utils.h"

namespace neug {
namespace runtime {
namespace ops {
bool BuildGroupByUtils(const physical::GroupBy& group_by,
                       std::vector<common::Variable>& key_vars,
                       std::vector<std::pair<int, int>>& mappings,
                       std::vector<physical::GroupBy_AggFunc>& reduce_funcs) {
  int mappings_num = group_by.mappings_size();
  int func_num = group_by.functions_size();
  for (int i = 0; i < mappings_num; ++i) {
    auto& key = group_by.mappings(i);
    if (!key.has_key() || !key.has_alias()) {
      LOG(ERROR) << "key should have key and alias";
      return false;
    }
    int tag = key.key().has_tag() ? key.key().tag().id() : -1;
    int alias = key.has_alias() ? key.alias().value() : -1;
    if (key.key().has_property()) {
      mappings.emplace_back(alias, alias);
    } else {
      mappings.emplace_back(tag, alias);
    }
    key_vars.emplace_back(key.key());
  }
  for (int i = 0; i < func_num; ++i) {
    reduce_funcs.emplace_back(group_by.functions(i));
  }

  return true;
}

}  // namespace ops

}  // namespace runtime
}  // namespace neug
