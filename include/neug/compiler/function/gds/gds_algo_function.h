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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/graph/graph_entry.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace main {
class ClientContext;
}

namespace function {

using options_t = common::case_insensitive_map_t<std::string>;

// Execution hook for GDS algorithms; distinct from [`call_exec_func_t`] until
// the runtime passes subgraph/options from the physical operator.
using algo_exec_func_t = std::function<execution::Context(
    execution::Context& ctx, const ::physical::Subgraph& subgraph,
    const options_t& options, IStorageInterface& graph)>;

struct NEUG_API GDSFuncBindData : TableFuncBindData {
  graph::GraphEntry graphEntry;
  common::case_insensitive_map_t<std::string> options;

  GDSFuncBindData(binder::expression_vector columns, common::row_idx_t numRows,
                  binder::expression_vector params,
                  graph::GraphEntry graphEntryIn,
                  common::case_insensitive_map_t<std::string> optionsIn);

  GDSFuncBindData(const GDSFuncBindData& other);

  std::unique_ptr<TableFuncBindData> copy() const override;
};

// Shared binder for all GDS [`CALL`] algorithms: resolves projected graph
// name, binds subgraph, parses options, and builds YIELD columns from
// `yieldSchema`.
NEUG_API std::unique_ptr<TableFuncBindData> bindGDSFunction(
    main::ClientContext* clientContext, const TableFuncBindInput* input,
    call_output_columns outputColumns);

// [`NeugCallFunction`] specialization for graph algorithms: wires
// [`TableFunction::bindFunc`] to
// [`bindGDSFunction`] and exposes [`algo_exec_func_t`] for engine integration.
struct NEUG_API GDSAlgoFunction : public NeugCallFunction {
  algo_exec_func_t algoExec;

  GDSAlgoFunction(std::string name,
                  std::vector<common::LogicalTypeID> inputTypes,
                  call_output_columns outputColumns);
};
}  // namespace function
}  // namespace neug
