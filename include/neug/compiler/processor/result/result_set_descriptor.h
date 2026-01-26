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

#include "neug/compiler/common/types/types.h"

namespace neug {
namespace planner {
class Schema;
}  // namespace planner

namespace processor {

struct DataChunkDescriptor {
  bool isSingleState;
  std::vector<common::LogicalType> logicalTypes;

  explicit DataChunkDescriptor(bool isSingleState)
      : isSingleState{isSingleState} {}
  DataChunkDescriptor(const DataChunkDescriptor& other)
      : isSingleState{other.isSingleState},
        logicalTypes(common::LogicalType::copy(other.logicalTypes)) {}

  inline std::unique_ptr<DataChunkDescriptor> copy() const {
    return std::make_unique<DataChunkDescriptor>(*this);
  }
};

struct NEUG_API ResultSetDescriptor {
  std::vector<std::unique_ptr<DataChunkDescriptor>> dataChunkDescriptors;

  ResultSetDescriptor() = default;
  explicit ResultSetDescriptor(
      std::vector<std::unique_ptr<DataChunkDescriptor>> dataChunkDescriptors)
      : dataChunkDescriptors{std::move(dataChunkDescriptors)} {}
  explicit ResultSetDescriptor(planner::Schema* schema);
  DELETE_BOTH_COPY(ResultSetDescriptor);

  std::unique_ptr<ResultSetDescriptor> copy() const;
};

}  // namespace processor
}  // namespace neug
