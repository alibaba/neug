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

#include <utility>

#include "frontier_morsel.h"
#include "neug/compiler/common/enums/extend_direction.h"
#include "neug/compiler/common/task_system/task.h"
#include "neug/compiler/function/gds/gds_frontier.h"
#include "neug/compiler/graph/graph.h"

namespace gs {
namespace function {

struct FrontierTaskInfo {
  catalog::TableCatalogEntry* boundEntry = nullptr;
  catalog::TableCatalogEntry* nbrEntry = nullptr;
  catalog::TableCatalogEntry* relEntry = nullptr;
  graph::Graph* graph;
  common::ExtendDirection direction;
  EdgeCompute& edgeCompute;
  std::vector<std::string> propertiesToScan;

  FrontierTaskInfo(catalog::TableCatalogEntry* boundEntry,
                   catalog::TableCatalogEntry* nbrEntry,
                   catalog::TableCatalogEntry* relEntry, graph::Graph* graph,
                   common::ExtendDirection direction, EdgeCompute& edgeCompute,
                   std::vector<std::string> propertiesToScan)
      : boundEntry{boundEntry},
        nbrEntry{nbrEntry},
        relEntry{relEntry},
        graph{graph},
        direction{direction},
        edgeCompute{edgeCompute},
        propertiesToScan{std::move(propertiesToScan)} {}
  FrontierTaskInfo(const FrontierTaskInfo& other)
      : boundEntry{other.boundEntry},
        nbrEntry{other.nbrEntry},
        relEntry{other.relEntry},
        graph{other.graph},
        direction{other.direction},
        edgeCompute{other.edgeCompute},
        propertiesToScan{other.propertiesToScan} {}
};

struct FrontierTaskSharedState {
  FrontierMorselDispatcher morselDispatcher;
  FrontierPair& frontierPair;

  FrontierTaskSharedState(uint64_t maxNumThreads, FrontierPair& frontierPair)
      : morselDispatcher{maxNumThreads}, frontierPair{frontierPair} {}
  DELETE_COPY_AND_MOVE(FrontierTaskSharedState);
};

class FrontierTask : public common::Task {
 public:
  FrontierTask(uint64_t maxNumThreads, const FrontierTaskInfo& info,
               std::shared_ptr<FrontierTaskSharedState> sharedState)
      : Task{maxNumThreads}, info{info}, sharedState{std::move(sharedState)} {}

  void run() override;

  void runSparse();

 private:
  FrontierTaskInfo info;
  std::shared_ptr<FrontierTaskSharedState> sharedState;
};

struct VertexComputeTaskSharedState {
  FrontierMorselDispatcher morselDispatcher;

  explicit VertexComputeTaskSharedState(uint64_t maxNumThreads)
      : morselDispatcher{maxNumThreads} {}
};

struct VertexComputeTaskInfo {
  VertexCompute& vc;
  graph::Graph* graph;
  catalog::TableCatalogEntry* tableEntry;
  std::vector<std::string> propertiesToScan;

  VertexComputeTaskInfo(VertexCompute& vc, graph::Graph* graph,
                        catalog::TableCatalogEntry* tableEntry,
                        std::vector<std::string> propertiesToScan)
      : vc{vc},
        graph{graph},
        tableEntry{tableEntry},
        propertiesToScan{std::move(propertiesToScan)} {}
  VertexComputeTaskInfo(const VertexComputeTaskInfo& other)
      : vc{other.vc},
        graph{other.graph},
        tableEntry{other.tableEntry},
        propertiesToScan{other.propertiesToScan} {}

  bool hasPropertiesToScan() const { return !propertiesToScan.empty(); }
};

class VertexComputeTask : public common::Task {
 public:
  VertexComputeTask(uint64_t maxNumThreads, const VertexComputeTaskInfo& info,
                    std::shared_ptr<VertexComputeTaskSharedState> sharedState)
      : common::Task{maxNumThreads},
        info{info},
        sharedState{std::move(sharedState)} {};

  VertexComputeTaskSharedState* getSharedState() const {
    return sharedState.get();
  }

  void run() override;

  void runSparse();

 private:
  VertexComputeTaskInfo info;
  std::shared_ptr<VertexComputeTaskSharedState> sharedState;
};

}  // namespace function
}  // namespace gs
