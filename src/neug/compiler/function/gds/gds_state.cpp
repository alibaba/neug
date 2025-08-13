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

#include "neug/compiler/function/gds/gds_state.h"

namespace gs {
namespace function {

void GDSComputeState::initSource(common::nodeID_t sourceNodeID) const {
  frontierPair->pinNextFrontier(sourceNodeID.tableID);
  frontierPair->addNodeToNextFrontier(sourceNodeID);
  frontierPair->setActiveNodesForNextIter();
  auxiliaryState->initSource(sourceNodeID);
}

void GDSComputeState::beginFrontierCompute(
    common::table_id_t currTableID, common::table_id_t nextTableID) const {
  frontierPair->beginFrontierComputeBetweenTables(currTableID, nextTableID);
  auxiliaryState->beginFrontierCompute(currTableID, nextTableID);
}

void GDSComputeState::switchToDense(processor::ExecutionContext* context,
                                    graph::Graph* graph) const {
  frontierPair->switchToDense(context, graph);
  auxiliaryState->switchToDense(context, graph);
}

}  // namespace function
}  // namespace gs
