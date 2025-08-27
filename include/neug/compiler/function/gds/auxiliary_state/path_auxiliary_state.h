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

#include "gds_auxilary_state.h"
#include "neug/compiler/function/gds/bfs_graph.h"

namespace gs {
namespace function {

class PathAuxiliaryState : public GDSAuxiliaryState {
 public:
  explicit PathAuxiliaryState(std::unique_ptr<BFSGraphManager> bfsGraphManager)
      : bfsGraphManager{std::move(bfsGraphManager)} {}

  BFSGraphManager* getBFSGraphManager() { return bfsGraphManager.get(); }

  void beginFrontierCompute(common::table_id_t,
                            common::table_id_t toTableID) override {
    bfsGraphManager->getCurrentGraph()->pinTableID(toTableID);
  }

  void switchToDense(processor::ExecutionContext* context,
                     graph::Graph* graph) override {
    bfsGraphManager->switchToDense(context, graph);
  }

 private:
  std::unique_ptr<BFSGraphManager> bfsGraphManager;
};

class WSPPathsAuxiliaryState : public GDSAuxiliaryState {
 public:
  explicit WSPPathsAuxiliaryState(
      std::unique_ptr<BFSGraphManager> bfsGraphManager)
      : bfsGraphManager{std::move(bfsGraphManager)} {}

  BFSGraphManager* getBFSGraphManager() { return bfsGraphManager.get(); }

  void initSource(common::nodeID_t sourceNodeID) override {
    sourceParent.setCost(0);
    bfsGraphManager->getCurrentGraph()->pinTableID(sourceNodeID.tableID);
    bfsGraphManager->getCurrentGraph()->setParentList(sourceNodeID.offset,
                                                      &sourceParent);
  }

  void beginFrontierCompute(common::table_id_t,
                            common::table_id_t toTableID) override {
    bfsGraphManager->getCurrentGraph()->pinTableID(toTableID);
  }

  void switchToDense(processor::ExecutionContext* context,
                     graph::Graph* graph) override {
    bfsGraphManager->switchToDense(context, graph);
  }

 private:
  std::unique_ptr<BFSGraphManager> bfsGraphManager;
  ParentList sourceParent;
};

}  // namespace function
}  // namespace gs
