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

#ifndef RUNTIME_EXECUTE_OPS_INSERT_DATA_SOURCE_H_
#define RUNTIME_EXECUTE_OPS_INSERT_DATA_SOURCE_H_

#include "src/engines/graph_db/runtime/execute/operator.h"
#include "src/engines/graph_db/runtime/execute/ops/insert/batch_insert_utils.h"

namespace gs {
namespace runtime {
namespace ops {

/**
 * @brief DataSourceOpr is used to load data from a CSV file.
 */
class CSVDataSourceOpr : public IUpdateOperator {
 public:
  CSVDataSourceOpr(
      const std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers)
      : suppliers_(suppliers) {}

  ~CSVDataSourceOpr() = default;

  std::string get_operator_name() const override { return "CSVDataSourceOpr"; }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override;

 private:
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers_;
};

class DataSourceOprBuilder : public IUpdateOperatorBuilder {
 public:
  DataSourceOprBuilder() = default;
  ~DataSourceOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kSource;
  }
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_INSERT_DATA_SOURCE_H_