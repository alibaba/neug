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

#include <memory>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/data_chunk.h"

namespace neug {
namespace execution {

/// Convert a single Arrow array to a ValueColumn.
std::shared_ptr<IContextColumn> arrow_array_to_value_column(
    const std::shared_ptr<arrow::Array>& array);

/// Convert multiple Arrow array chunks (from a ChunkedArray) to a ValueColumn.
std::shared_ptr<IContextColumn> arrow_arrays_to_value_column(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays);

/// Convert an Arrow RecordBatch to a DataChunk with ValueColumns.
std::shared_ptr<DataChunk> recordbatch_to_value_datachunk(
    const std::shared_ptr<arrow::RecordBatch>& batch);

}  // namespace execution
}  // namespace neug
