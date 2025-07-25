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

#ifndef RUNTIME_EXECUTE_RETRIEVE_OPS_BATCH_DATA_EXPORT_H_
#define RUNTIME_EXECUTE_RETRIEVE_OPS_BATCH_DATA_EXPORT_H_

#include "neug/engines/graph_db/runtime/execute/operator.h"
#include "neug/engines/graph_db/runtime/execute/writer/export_writer_factory.h"
#include "neug/proto_generated_gie/physical.pb.h"

namespace gs {
namespace runtime {
namespace ops {

class DataExportOpr : public IReadOperator {
 public:
  DataExportOpr(const std::string& extension_name, const std::string& file_path,
                const std::unordered_map<std::string, std::string>& options,
                const std::vector<std::pair<int, std::string>>& headers)
      : extension_name_(extension_name),
        file_path_(file_path),
        options_(options),
        headers_(headers) {}

  std::string get_operator_name() const override { return "DataExportOpr"; }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override;

  std::shared_ptr<IExportWriter> writer_;
  std::string extension_name_;
  std::string file_path_;
  std::unordered_map<std::string, std::string> options_;
  std::vector<std::pair<int, std::string>> headers_;
};

class DataExportOprBuilder : public IReadOperatorBuilder {
 public:
  DataExportOprBuilder() = default;
  ~DataExportOprBuilder() = default;

  bl::result<ReadOpBuildResultT> Build(const gs::Schema& schema,
                                       const ContextMeta& ctx_meta,
                                       const physical::PhysicalPlan& plan,
                                       int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kDataExport};
  }
};
}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_EXECUTE_RETRIEVE_OPS_BATCH_DATA_EXPORT_H_