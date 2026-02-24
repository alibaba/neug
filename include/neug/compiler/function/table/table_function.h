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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <mutex>

#include "neug/compiler/common/data_chunk/data_chunk.h"
#include "neug/compiler/common/mask.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/processor/operator/physical_operator.h"

namespace neug {
namespace binder {
class BoundReadingClause;
}
namespace parser {
struct YieldVariable;
class ParsedExpression;
}  // namespace parser

namespace planner {
class LogicalOperator;
class LogicalPlan;
class Planner;
}  // namespace planner

namespace processor {
struct ExecutionContext;
class PlanMapper;
}  // namespace processor

namespace function {

struct TableFuncBindInput;
struct TableFuncBindData;

struct NEUG_API TableFuncSharedState {
  common::row_idx_t numRows = 0;
  common::NodeOffsetMaskMap semiMasks;
  std::mutex mtx;

  explicit TableFuncSharedState() = default;
  explicit TableFuncSharedState(common::row_idx_t numRows) : numRows{numRows} {}
  virtual ~TableFuncSharedState() = default;
  virtual uint64_t getNumRows() const { return numRows; }

  common::table_id_map_t<common::SemiMask*> getSemiMasks() const {
    return semiMasks.getMasks();
  }

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }
};

struct TableFuncLocalState {
  virtual ~TableFuncLocalState() = default;

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }
};

struct TableFuncInput {
  TableFuncBindData* bindData;
  TableFuncLocalState* localState;
  TableFuncSharedState* sharedState;
  processor::ExecutionContext* context;

  TableFuncInput() = default;
  TableFuncInput(TableFuncBindData* bindData, TableFuncLocalState* localState,
                 TableFuncSharedState* sharedState,
                 processor::ExecutionContext* context)
      : bindData{bindData},
        localState{localState},
        sharedState{sharedState},
        context{context} {}
  DELETE_COPY_DEFAULT_MOVE(TableFuncInput);
};

struct TableFuncOutput {
  common::DataChunk dataChunk;

  explicit TableFuncOutput(common::DataChunk dataChunk)
      : dataChunk{std::move(dataChunk)} {}
  virtual ~TableFuncOutput() = default;

  void resetState();
  void setOutputSize(common::offset_t size) const;
};

struct NEUG_API TableFuncInitSharedStateInput final {
  TableFuncBindData* bindData;
  processor::ExecutionContext* context;

  TableFuncInitSharedStateInput(TableFuncBindData* bindData,
                                processor::ExecutionContext* context)
      : bindData{bindData}, context{context} {}
};

struct TableFuncInitLocalStateInput {
  TableFuncSharedState& sharedState;
  TableFuncBindData& bindData;
  main::ClientContext* clientContext;

  TableFuncInitLocalStateInput(TableFuncSharedState& sharedState,
                               TableFuncBindData& bindData,
                               main::ClientContext* clientContext)
      : sharedState{sharedState},
        bindData{bindData},
        clientContext{clientContext} {}
};

struct TableFuncInitOutputInput {
  std::vector<processor::DataPos> outColumnPositions;
  processor::ResultSet& resultSet;

  TableFuncInitOutputInput(std::vector<processor::DataPos> outColumnPositions,
                           processor::ResultSet& resultSet)
      : outColumnPositions{std::move(outColumnPositions)},
        resultSet{resultSet} {}
};

using table_func_bind_t = std::function<std::unique_ptr<TableFuncBindData>(
    main::ClientContext*, const TableFuncBindInput*)>;
using table_func_t = std::function<common::offset_t(const TableFuncInput&,
                                                    TableFuncOutput& output)>;
using table_func_init_shared_t =
    std::function<std::shared_ptr<TableFuncSharedState>(
        const TableFuncInitSharedStateInput&)>;
using table_func_init_local_t =
    std::function<std::unique_ptr<TableFuncLocalState>(
        const TableFuncInitLocalStateInput&)>;
using table_func_init_output_t = std::function<std::unique_ptr<TableFuncOutput>(
    const TableFuncInitOutputInput&)>;
using table_func_can_parallel_t = std::function<bool()>;
using table_func_progress_t =
    std::function<double(TableFuncSharedState* sharedState)>;
using table_func_finalize_t = std::function<void(
    const processor::ExecutionContext*, TableFuncSharedState*)>;
using table_func_rewrite_t = std::function<std::string(
    main::ClientContext&, const TableFuncBindData& bindData)>;
using table_func_get_logical_plan_t =
    std::function<void(planner::Planner*, const binder::BoundReadingClause&,
                       std::vector<std::shared_ptr<binder::Expression>>,
                       std::vector<std::unique_ptr<planner::LogicalPlan>>&)>;
using table_func_infer_input_types =
    std::function<std::vector<common::LogicalType>(
        const binder::expression_vector&)>;

struct NEUG_API TableFunction : Function {
  table_func_t tableFunc = nullptr;
  table_func_bind_t bindFunc = nullptr;
  table_func_init_shared_t initSharedStateFunc = nullptr;
  table_func_init_local_t initLocalStateFunc = nullptr;
  table_func_init_output_t initOutputFunc = nullptr;
  table_func_can_parallel_t canParallelFunc = [] { return true; };
  table_func_progress_t progressFunc = [](TableFuncSharedState*) {
    return 0.0;
  };
  table_func_finalize_t finalizeFunc = [](auto, auto) {};
  table_func_rewrite_t rewriteFunc = nullptr;
  table_func_get_logical_plan_t getLogicalPlanFunc = getLogicalPlan;
  table_func_infer_input_types inferInputTypes = nullptr;

  TableFunction() {}
  TableFunction(std::string name, std::vector<common::LogicalTypeID> inputTypes)
      : Function{std::move(name), std::move(inputTypes)} {}
  ~TableFunction() override;
  TableFunction(const TableFunction&) = default;
  TableFunction& operator=(const TableFunction& other) = default;
  DEFAULT_BOTH_MOVE(TableFunction);

  std::string signatureToString() const override {
    return common::LogicalTypeUtils::toString(parameterTypeIDs);
  }

  std::unique_ptr<TableFunction> copy() const {
    return std::make_unique<TableFunction>(*this);
  }

  static std::unique_ptr<TableFuncLocalState> initEmptyLocalState(
      const TableFuncInitLocalStateInput& input);
  static std::unique_ptr<TableFuncSharedState> initEmptySharedState(
      const TableFuncInitSharedStateInput& input);
  static std::unique_ptr<TableFuncOutput> initSingleDataChunkScanOutput(
      const TableFuncInitOutputInput& input);
  static std::vector<std::string> extractYieldVariables(
      const std::vector<std::string>& names,
      const std::vector<parser::YieldVariable>& yieldVariables);
  static void getLogicalPlan(
      planner::Planner* planner,
      const binder::BoundReadingClause& boundReadingClause,
      binder::expression_vector predicates,
      std::vector<std::unique_ptr<planner::LogicalPlan>>& plans);
  static common::offset_t emptyTableFunc(const TableFuncInput& input,
                                         TableFuncOutput& output);
};

}  // namespace function
}  // namespace neug
