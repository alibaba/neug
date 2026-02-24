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

#include "neug/compiler/function/gds/rec_joins.h"

namespace neug {
namespace function {

RJBindData::RJBindData(const RJBindData& other) {
  graphEntry = other.graphEntry.copy();
  nodeInput = other.nodeInput;
  nodeOutput = other.nodeOutput;
  lowerBound = other.lowerBound;
  upperBound = other.upperBound;
  semantic = other.semantic;
  extendDirection = other.extendDirection;
  flipPath = other.flipPath;
  writePath = other.writePath;
  directionExpr = other.directionExpr;
  lengthExpr = other.lengthExpr;
  pathNodeIDsExpr = other.pathNodeIDsExpr;
  pathEdgeIDsExpr = other.pathEdgeIDsExpr;
  weightPropertyExpr = other.weightPropertyExpr;
  weightOutputExpr = other.weightOutputExpr;
}

}  // namespace function
}  // namespace neug
