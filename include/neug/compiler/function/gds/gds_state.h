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

#include "auxiliary_state/gds_auxilary_state.h"
#include "gds_frontier.h"

namespace gs {
namespace function {

struct GDSComputeState {
  std::shared_ptr<FrontierPair> frontierPair = nullptr;
  std::unique_ptr<EdgeCompute> edgeCompute = nullptr;
  std::unique_ptr<GDSAuxiliaryState> auxiliaryState = nullptr;

  GDSComputeState(std::shared_ptr<FrontierPair> frontierPair,
                  std::unique_ptr<EdgeCompute> edgeCompute,
                  std::unique_ptr<GDSAuxiliaryState> auxiliaryState)
      : frontierPair{std::move(frontierPair)},
        edgeCompute{std::move(edgeCompute)},
        auxiliaryState{std::move(auxiliaryState)} {}

  void initSource(common::nodeID_t sourceNodeID) const;
  // When performing computations on multi-label graphs, it is beneficial to fix
  // a single node table of nodes in the current frontier and a single node
  // table of nodes for the next frontier. That is because algorithms will
  // perform extensions using a single relationship table at a time, and each
  // relationship table R is between a single source node table S and a single
  // destination node table T. Therefore, during execution the algorithm will
  // need to check only the active S nodes in current frontier and update the
  // active statuses of only the T nodes in the next frontier. The information
  // that the algorithm is beginning and S-to-T extensions are be given to the
  // data structures of the computation, e.g., FrontierPairs and RJOutputs, to
  // possibly avoid them doing lookups of S and T-related data structures, e.g.,
  // maps, internally.
  void beginFrontierCompute(common::table_id_t currTableID,
                            common::table_id_t nextTableID) const;

  // Switch all data structures (frontierPair & auxiliaryState) to dense
  // version.
  void switchToDense(processor::ExecutionContext* context,
                     graph::Graph* graph) const;
};

}  // namespace function
}  // namespace gs
