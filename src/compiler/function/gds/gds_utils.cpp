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

#include "neug/compiler/function/gds/gds_utils.h"

#include <re2.h>
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/task_system/task_scheduler.h"
#include "neug/compiler/function/gds/gds_task.h"
#include "neug/compiler/graph/graph.h"
#include "neug/compiler/graph/graph_entry.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;
using namespace gs::catalog;
using namespace gs::function;
using namespace gs::processor;
using namespace gs::graph;

namespace gs {
namespace function {

static std::shared_ptr<FrontierTask> getFrontierTask(
    const main::ClientContext* context, TableCatalogEntry* fromEntry,
    TableCatalogEntry* toEntry, TableCatalogEntry* relEntry, Graph* graph,
    ExtendDirection extendDirection, const GDSComputeState& computeState,
    std::vector<std::string> propertiesToScan) {
  computeState.beginFrontierCompute(fromEntry->getTableID(),
                                    toEntry->getTableID());
  auto info =
      FrontierTaskInfo(fromEntry, toEntry, relEntry, graph, extendDirection,
                       *computeState.edgeCompute, std::move(propertiesToScan));
  auto numThreads = 1;
  auto sharedState = std::make_shared<FrontierTaskSharedState>(
      numThreads, *computeState.frontierPair);
  auto maxOffset =
      graph->getMaxOffset(context->getTransaction(), fromEntry->getTableID());
  sharedState->morselDispatcher.init(maxOffset);
  return std::make_shared<FrontierTask>(numThreads, info, sharedState);
}

static void scheduleFrontierTask(ExecutionContext* context,
                                 TableCatalogEntry* fromEntry,
                                 TableCatalogEntry* toEntry,
                                 TableCatalogEntry* relEntry, Graph* graph,
                                 ExtendDirection extendDirection,
                                 const GDSComputeState& computeState,
                                 std::vector<std::string> propertiesToScan) {
  THROW_EXCEPTION_WITH_FILE_LINE("scheduleFrontierTask is not implemented");
}

static void runOneIteration(ExecutionContext* context, Graph* graph,
                            ExtendDirection extendDirection,
                            const GDSComputeState& compState,
                            const std::vector<std::string>& propertiesToScan) {
  for (auto info : graph->getGraphEntry()->nodeInfos) {
    auto fromEntry = info.entry;
    for (const auto& nbrInfo :
         graph->getForwardNbrTableInfos(fromEntry->getTableID())) {
      auto toEntry = nbrInfo.nodeEntry;
      auto relEntry = nbrInfo.relEntry;
      switch (extendDirection) {
      case ExtendDirection::FWD: {
        scheduleFrontierTask(context, fromEntry, toEntry, relEntry, graph,
                             ExtendDirection::FWD, compState, propertiesToScan);
      } break;
      case ExtendDirection::BWD: {
        scheduleFrontierTask(context, toEntry, fromEntry, relEntry, graph,
                             ExtendDirection::BWD, compState, propertiesToScan);
      } break;
      case ExtendDirection::BOTH: {
        scheduleFrontierTask(context, fromEntry, toEntry, relEntry, graph,
                             ExtendDirection::FWD, compState, propertiesToScan);
        scheduleFrontierTask(context, toEntry, fromEntry, relEntry, graph,
                             ExtendDirection::BWD, compState, propertiesToScan);
      } break;
      default:
        KU_UNREACHABLE;
      }
    }
  }
}

void GDSUtils::runAlgorithmEdgeCompute(ExecutionContext* context,
                                       GDSComputeState& compState, Graph* graph,
                                       ExtendDirection extendDirection,
                                       uint64_t maxIteration) {
  auto frontierPair = compState.frontierPair.get();
  while (frontierPair->continueNextIter(maxIteration)) {
    frontierPair->beginNewIteration();
    runOneIteration(context, graph, extendDirection, compState, {});
  }
}

void GDSUtils::runFTSEdgeCompute(
    ExecutionContext* context, GDSComputeState& compState, Graph* graph,
    ExtendDirection extendDirection,
    const std::vector<std::string>& propertiesToScan) {
  compState.frontierPair->beginNewIteration();
  runOneIteration(context, graph, extendDirection, compState, propertiesToScan);
}

void GDSUtils::runRecursiveJoinEdgeCompute(
    ExecutionContext* context, GDSComputeState& compState, Graph* graph,
    ExtendDirection extendDirection, uint64_t maxIteration,
    NodeOffsetMaskMap* outputNodeMask,
    const std::vector<std::string>& propertiesToScan) {
  auto frontierPair = compState.frontierPair.get();
  compState.edgeCompute->resetSingleThreadState();
  while (frontierPair->continueNextIter(maxIteration)) {
    frontierPair->beginNewIteration();
    if (outputNodeMask != nullptr &&
        compState.edgeCompute->terminate(*outputNodeMask)) {
      break;
    }
    runOneIteration(context, graph, extendDirection, compState,
                    propertiesToScan);
    if (frontierPair->needSwitchToDense(
            context->clientContext->getClientConfig()
                ->sparseFrontierThreshold)) {
      compState.switchToDense(context, graph);
    }
  }
}

static void runVertexComputeInternal(const TableCatalogEntry* currentEntry,
                                     GDSDensityState densityState,
                                     const Graph* graph,
                                     std::shared_ptr<VertexComputeTask> task,
                                     ExecutionContext* context) {
  THROW_EXCEPTION_WITH_FILE_LINE("runVertexComputeInternal is not implemented");
}

void GDSUtils::runVertexCompute(
    ExecutionContext* context, GDSDensityState densityState, Graph* graph,
    VertexCompute& vc, const std::vector<std::string>& propertiesToScan) {
  auto maxThreads = 1;
  auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
  for (const auto& nodeInfo : graph->getGraphEntry()->nodeInfos) {
    auto entry = nodeInfo.entry;
    if (!vc.beginOnTable(entry->getTableID())) {
      continue;
    }
    auto info = VertexComputeTaskInfo(vc, graph, entry, propertiesToScan);
    auto task =
        std::make_shared<VertexComputeTask>(maxThreads, info, sharedState);
    runVertexComputeInternal(entry, densityState, graph, task, context);
  }
}

void GDSUtils::runVertexCompute(ExecutionContext* context,
                                GDSDensityState densityState, Graph* graph,
                                VertexCompute& vc) {
  runVertexCompute(context, densityState, graph, vc,
                   std::vector<std::string>{});
}

void GDSUtils::runVertexCompute(
    ExecutionContext* context, GDSDensityState densityState, Graph* graph,
    VertexCompute& vc, TableCatalogEntry* entry,
    const std::vector<std::string>& propertiesToScan) {
  auto maxThreads = 1;
  auto info = VertexComputeTaskInfo(vc, graph, entry, propertiesToScan);
  auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
  if (!vc.beginOnTable(entry->getTableID())) {
    return;
  }
  auto task =
      std::make_shared<VertexComputeTask>(maxThreads, info, sharedState);
  runVertexComputeInternal(entry, densityState, graph, task, context);
}

}  // namespace function
}  // namespace gs
