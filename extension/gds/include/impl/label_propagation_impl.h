/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/predicates.h"

namespace neug {
namespace gds {

struct LabelPropagationRunArgs {
  label_t vertex_label;
  execution::LabelTriplet edge_triplet;
  int max_iterations;
  int concurrency;
  int32_t node_alias;
  int32_t label_alias;
  execution::ExprBase* vertex_pred;
  execution::ExprBase* edge_pred;
};

execution::Context RunLabelPropagation(const LabelPropagationRunArgs& args,
                                       const StorageReadInterface& graph,
                                       execution::Context& ctx);

}  // namespace gds
}  // namespace neug
