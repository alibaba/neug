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

#include "neug/compiler/function/gds/compute.h"
#include "neug/compiler/function/gds/gds.h"

namespace gs {
namespace function {

class GDSVertexCompute : public VertexCompute {
 public:
  explicit GDSVertexCompute(common::NodeOffsetMaskMap* nodeMask)
      : nodeMask{nodeMask} {}

  bool beginOnTable(common::table_id_t tableID) override {
    if (nodeMask != nullptr) {
      nodeMask->pin(tableID);
    }
    beginOnTableInternal(tableID);
    return true;
  }

 protected:
  bool skip(common::offset_t offset) {
    if (nodeMask != nullptr && nodeMask->hasPinnedMask()) {
      return !nodeMask->valid(offset);
    }
    return false;
  }

  virtual void beginOnTableInternal(common::table_id_t tableID) = 0;

 protected:
  common::NodeOffsetMaskMap* nodeMask;
};

class GDSResultVertexCompute : public GDSVertexCompute {
 public:
  GDSResultVertexCompute(storage::MemoryManager* mm,
                         GDSFuncSharedState* sharedState)
      : GDSVertexCompute{sharedState->getGraphNodeMaskMap()},
        sharedState{sharedState},
        mm{mm} {
    localFT = sharedState->factorizedTablePool.claimLocalTable(mm);
  }
  ~GDSResultVertexCompute() override {
    sharedState->factorizedTablePool.returnLocalTable(localFT);
  }

 protected:
  std::unique_ptr<common::ValueVector> createVector(
      const common::LogicalType& type) {
    auto vector = std::make_unique<common::ValueVector>(type.copy(), mm);
    vector->state = common::DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(vector.get());
    return vector;
  }

 protected:
  GDSFuncSharedState* sharedState;
  storage::MemoryManager* mm;
  processor::FactorizedTable* localFT = nullptr;
  std::vector<common::ValueVector*> vectors;
};

}  // namespace function
}  // namespace gs
