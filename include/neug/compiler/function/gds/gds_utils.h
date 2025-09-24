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

#include "gds_state.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/enums/extend_direction.h"

namespace gs {
namespace function {

class NEUG_API GDSUtils {
 public:
  // Run edge compute for graph algorithms
  static void runAlgorithmEdgeCompute(processor::ExecutionContext* context,
                                      GDSComputeState& compState,
                                      graph::Graph* graph,
                                      common::ExtendDirection extendDirection,
                                      uint64_t maxIteration);
  // Run edge compute for full text search
  static void runFTSEdgeCompute(
      processor::ExecutionContext* context, GDSComputeState& compState,
      graph::Graph* graph, common::ExtendDirection extendDirection,
      const std::vector<std::string>& propertiesToScan);
  // Run edge compute for recursive join.
  static void runRecursiveJoinEdgeCompute(
      processor::ExecutionContext* context, GDSComputeState& compState,
      graph::Graph* graph, common::ExtendDirection extendDirection,
      uint64_t maxIteration, common::NodeOffsetMaskMap* outputNodeMask,
      const std::vector<std::string>& propertiesToScan);

  // Run vertex compute without property scan
  static void runVertexCompute(processor::ExecutionContext* context,
                               GDSDensityState densityState,
                               graph::Graph* graph, VertexCompute& vc);
  // Run vertex compute with property scan
  static void runVertexCompute(
      processor::ExecutionContext* context, GDSDensityState densityState,
      graph::Graph* graph, VertexCompute& vc,
      const std::vector<std::string>& propertiesToScan);
  // Run vertex compute on specific table with property scan
  static void runVertexCompute(
      processor::ExecutionContext* context, GDSDensityState densityState,
      graph::Graph* graph, VertexCompute& vc, catalog::TableCatalogEntry* entry,
      const std::vector<std::string>& propertiesToScan);
};

}  // namespace function
}  // namespace gs
