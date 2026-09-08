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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/common/types/data_chunk.h"
#include "neug/execution/common/params_map.h"
#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
class IDataChunkSupplier;
namespace reader {

/// Evaluates a file predicate using the same expressions as query execution.
/// Readers may use this after decoding when native pushdown is unavailable.
/// Retains a shallow copy of input, keeping its column layout and storage
/// alive. Shared column values must not be mutated while the filter is in use.
class RowExpressionFilter {
 public:
  RowExpressionFilter(const ::common::Expression& expr,
                      const std::unordered_map<std::string, int>& column_index,
                      const DataChunk& input,
                      const execution::ParamsMap& parameters = {});

  // row must be a valid row index in the input bound at construction.
  bool eval(size_t row) const;

 private:
  std::function<bool(size_t)> evaluator_;
};

DataChunk filter_chunk(const DataChunk& input,
                       const std::shared_ptr<::common::Expression>& filter_expr,
                       const std::vector<std::string>& column_names,
                       const execution::ParamsMap& parameters = {});

DataChunk project_chunk(const DataChunk& input,
                        const std::vector<std::string>& column_names,
                        const std::vector<std::string>& project_columns);

DataChunk read_all_chunks(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers);

// Merge dense reader chunks in order, preserving column types and NULLs.
// Ignore null/columnless chunks, retain empty schemas, and reject mismatched
// column counts, types, or lengths before allocating output columns.
DataChunk merge_chunks(std::vector<std::shared_ptr<DataChunk>> chunks);

}  // namespace reader
}  // namespace neug
