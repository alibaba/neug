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

namespace gs {
namespace runtime {
namespace ops {
AggrKind parse_aggregate(physical::GroupBy_AggFunc::Aggregate v) {
  if (v == physical::GroupBy_AggFunc::SUM) {
    return AggrKind::kSum;
  } else if (v == physical::GroupBy_AggFunc::MIN) {
    return AggrKind::kMin;
  } else if (v == physical::GroupBy_AggFunc::MAX) {
    return AggrKind::kMax;
  } else if (v == physical::GroupBy_AggFunc::COUNT) {
    return AggrKind::kCount;
  } else if (v == physical::GroupBy_AggFunc::COUNT_DISTINCT) {
    return AggrKind::kCountDistinct;
  } else if (v == physical::GroupBy_AggFunc::TO_SET) {
    return AggrKind::kToSet;
  } else if (v == physical::GroupBy_AggFunc::FIRST) {
    return AggrKind::kFirst;
  } else if (v == physical::GroupBy_AggFunc::TO_LIST) {
    return AggrKind::kToList;
  } else if (v == physical::GroupBy_AggFunc::AVG) {
    return AggrKind::kAvg;
  } else {
    LOG(FATAL) << "unsupport" << static_cast<int>(v);
    return AggrKind::kSum;
  }
}

bool BuildGroupByUtils(
    const physical::GroupBy& group_by,
    std::vector<std::pair<common::Variable, int>>& project_vars,
    std::vector<common::Variable>& key_vars,
    std::vector<std::pair<int, int>>& mappings,
    std::vector<physical::GroupBy_AggFunc>& reduce_funcs,
    std::vector<std::pair<int, int>>& dependencies) {
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
  for (size_t i = 0; i < key_vars.size(); ++i) {
    if (key_vars[i].has_property()) {
      project_vars.emplace_back(key_vars[i], mappings[i].second);
    }
  }
  for (int i = 0; i < func_num; ++i) {
    reduce_funcs.emplace_back(group_by.functions(i));
    const auto& func = group_by.functions(i);
    if (func.vars_size() == 0) {
      continue;
    }
    auto& var = func.vars(0);
    int alias = func.has_alias() ? func.alias().value() : -1;
    auto aggr_kind = parse_aggregate(func.aggregate());
    if (aggr_kind == AggrKind::kToList || aggr_kind == AggrKind::kToSet ||
        aggr_kind == AggrKind::kFirst || aggr_kind == AggrKind::kMin ||
        aggr_kind == AggrKind::kMax) {
      if (!var.has_property()) {
        int tag = var.has_tag() ? var.tag().id() : -1;
        dependencies.emplace_back(alias, tag);
      }
    }
  }
  return true;
}

}  // namespace ops

}  // namespace runtime
}  // namespace gs
