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

#include "neug/execution/execute/operator.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/utils/reader/reader.h"

namespace neug {
using namespace reader;
class IRecordBatchSupplier;
namespace execution {

namespace ops {

// Build ReadSharedState from DataSource PB.
class ReadStateBuilder {
 public:
  std::shared_ptr<ReadSharedState> build(
      const ::physical::DataSource& data_source);
  static std::shared_ptr<EntrySchema> buildEntrySchema(
      const ::physical::EntrySchema& entry_schema);
  static FileSchema buildFileSchema(const ::physical::FileSchema& file_schema);
};

class DataSourceOprBuilder : public IOperatorBuilder {
 public:
  DataSourceOprBuilder() = default;
  ~DataSourceOprBuilder() = default;

  neug::result<OpBuildResultT> Build(const Schema& schema,
                                     const ContextMeta& ctx_meta,
                                     const physical::PhysicalPlan& plan,
                                     int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kSource};
  }
};

}  // namespace ops
}  // namespace execution
}  // namespace neug
