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
#include "neug/execution/common/operators/retrieve/group_by.h"
#include "neug/execution/common/operators/retrieve/project.h"
#include "neug/execution/utils/pb_parse_utils.h"

#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {
namespace ops {

bool BuildGroupByUtils(const physical::GroupBy& group_by,
                       std::vector<std::pair<int, int>>& mappings,
                       std::vector<physical::GroupBy_AggFunc>& reduce_funcs);

std::unique_ptr<KeyBase> create_key_func(
    const std::vector<std::pair<int, int>>& mappings,
    const IStorageInterface& graph, const Context& ctx);

ReduceOp create_reduce_op(const physical::GroupBy_AggFunc& func,
                          const IStorageInterface& graph, const Context& ctx);

}  // namespace ops

}  // namespace execution
}  // namespace neug
