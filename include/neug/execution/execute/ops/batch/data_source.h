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

#ifndef EXECUTION_EXECUTE_OPS_BATCH_DATA_SOURCE_H_
#define EXECUTION_EXECUTE_OPS_BATCH_DATA_SOURCE_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/execute/operator.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/cypher_dml.pb.h"
#include "neug/utils/proto/plan/physical.pb.h"
#endif

namespace gs {
class IRecordBatchSupplier;
class Schema;

namespace runtime {
class Context;
class GraphUpdateInterface;
class OprTimer;

namespace ops {

/**
 * @brief DataSourceOpr is used to load data from a CSV file.
 */
class CSVDataSourceOpr : public IUpdateOperator {
 public:
  static constexpr bool batch_reader_default = true;
  CSVDataSourceOpr(
      const std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers,
      bool batch_reader)
      : suppliers_(suppliers), batch_reader_(batch_reader) {}

  ~CSVDataSourceOpr() = default;

  std::string get_operator_name() const override { return "CSVDataSourceOpr"; }

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  gs::result<Context> eval_table_reader(Context&& ctx);
  gs::result<Context> eval_batch_reader(Context&& ctx);

  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers_;
  bool batch_reader_;  // With batch reader, we will read the file in batches.
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

#endif  // EXECUTION_EXECUTE_OPS_BATCH_DATA_SOURCE_H_