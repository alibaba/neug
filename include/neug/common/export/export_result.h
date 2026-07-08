/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <vector>

#include "neug/common/types.h"
#include "neug/common/types/data_chunk.h"

namespace neug {

class StorageReadInterface;

namespace execution {
class Context;
}

struct ExportResult {
  DataChunk chunk;
  std::vector<DataType> source_types;
};

/// Materializes graph values for file export and merges Context chunks into a
/// single DataChunk so writers can emit one complete output artifact.
ExportResult materialize_result_for_export(const execution::Context& ctx,
                                           const StorageReadInterface& graph);

}  // namespace neug
