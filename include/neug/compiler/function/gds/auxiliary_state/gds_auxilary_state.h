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

#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/types/types.h"

namespace gs {
namespace graph {
class Graph;
}

namespace processor {
struct ExecutionContext;
}

namespace function {

// Maintain algorithm specific data structures
class GDSAuxiliaryState {
 public:
  GDSAuxiliaryState() = default;
  virtual ~GDSAuxiliaryState() = default;

  // Initialize state for source node.
  virtual void initSource(common::nodeID_t) {}
  // Initialize state before extending from `fromTable` to `toTable`.
  // Normally you want to pin data structures on `toTableID`.
  virtual void beginFrontierCompute(common::table_id_t fromTableID,
                                    common::table_id_t toTableID) = 0;

  virtual void switchToDense(processor::ExecutionContext* context,
                             graph::Graph* graph) = 0;

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }
};

class EmptyGDSAuxiliaryState : public GDSAuxiliaryState {
 public:
  EmptyGDSAuxiliaryState() = default;

  void beginFrontierCompute(common::table_id_t, common::table_id_t) override {}

  void switchToDense(processor::ExecutionContext*, graph::Graph*) override {}
};

}  // namespace function
}  // namespace gs
