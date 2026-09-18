/** Copyright 2020 Alibaba Group Holding Limited.
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

#pragma once

#include <memory>
#include <string>

#include <carquet/carquet.h>

#include "neug/common/types/data_chunk.h"
#include "neug/utils/result.h"

namespace neug {
namespace parquet {

result<void> validateArrowCScalarSchema(const ArrowSchema& schema,
                                        const std::string& path = "column");

// Converts one scalar Arrow C Data column into an owned NeuG value column.
// The returned column never retains pointers into the ArrowArray.
result<std::shared_ptr<IContextColumn>> convertArrowCScalarColumn(
    const ArrowSchema& schema, const ArrowArray& array,
    const std::string& path = "column");

// Converts a flat top-level Arrow struct into an owned NeuG DataChunk.
// The root must be non-null and all children must have the same length.
result<std::shared_ptr<DataChunk>> convertArrowCFlatStruct(
    const ArrowSchema& schema, const ArrowArray& array);

}  // namespace parquet
}  // namespace neug
