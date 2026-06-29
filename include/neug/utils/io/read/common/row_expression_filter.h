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

#include "neug/execution/common/data_chunk.h"
#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
class IDataChunkSupplier;
namespace reader {

/// Evaluates a common::Expression row-by-row against a DataChunk.
class RowExpressionFilter {
 public:
  RowExpressionFilter(const ::common::Expression& expr,
                      const std::unordered_map<std::string, int>& column_index);

  bool eval(const execution::DataChunk& chunk, size_t row) const;

 private:
  std::function<bool(const execution::DataChunk&, size_t)> evaluator_;
};

execution::DataChunk filter_chunk(
    const execution::DataChunk& input,
    const std::shared_ptr<::common::Expression>& filter_expr,
    const std::vector<std::string>& column_names);

execution::DataChunk project_chunk(
    const execution::DataChunk& input,
    const std::vector<std::string>& column_names,
    const std::vector<std::string>& project_columns);

execution::DataChunk read_all_chunks(
    const std::vector<std::shared_ptr<IDataChunkSupplier>>& suppliers);

}  // namespace reader
}  // namespace neug
