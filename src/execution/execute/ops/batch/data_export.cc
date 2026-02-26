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

#include "neug/execution/execute/ops/batch/data_export.h"
#include "neug/execution/execute/ops/batch/writer/export_writer_factory.h"

namespace neug {
namespace execution {
namespace ops {

class DataExportOpr : public IOperator {
 public:
  DataExportOpr(const std::string& extension_name, const std::string& file_path,
                const std::unordered_map<std::string, std::string>& options,
                const std::vector<std::pair<int, std::string>>& headers)
      : extension_name_(extension_name),
        file_path_(file_path),
        options_(options),
        headers_(headers) {}

  std::string get_operator_name() const override { return "DataExportOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override;

  std::shared_ptr<IExportWriter> writer_;
  std::string extension_name_;
  std::string file_path_;
  std::unordered_map<std::string, std::string> options_;
  std::vector<std::pair<int, std::string>> headers_;
};

neug::result<neug::execution::Context> DataExportOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params,
    neug::execution::Context&& ctx, neug::execution::OprTimer* timer) {
  const auto& graph =
      dynamic_cast<const StorageReadInterface&>(graph_interface);
  writer_ = neug::execution::ExportWriterFactory::CreateExportWriter(
      extension_name_, file_path_, headers_, options_);
  std::vector<std::shared_ptr<IContextColumn>> columns;
  for (size_t i = 0; i < headers_.size(); i++) {
    columns.emplace_back(ctx.get(headers_[i].first));
  }
  Status status = writer_->Write(columns, graph);
  if (!status.ok()) {
    RETURN_ERROR(status);
  }
  for (size_t i = 0; i < headers_.size(); i++) {
    ctx.remove(headers_[i].first);
  }
  return neug::result<Context>(std::move(ctx));
}

neug::result<OpBuildResultT> DataExportOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& data_export_opr = plan.plan(op_idx).opr().data_export();
  std::string extension_name = data_export_opr.extension_name();
  std::string file_path = data_export_opr.file_path();
  std::unordered_map<std::string, std::string> options;
  for (const auto& entry : data_export_opr.options()) {
    std::string upper_str;
    std::transform(entry.first.begin(), entry.first.end(),
                   std::back_inserter(upper_str), ::toupper);
    options.insert({upper_str, entry.second});
  }
  std::vector<std::pair<int, std::string>> headers;
  for (const auto& property_mapping : data_export_opr.property_mappings()) {
    std::string column_name = property_mapping.property().key().name();
    int alias = property_mapping.data().operators(0).var().tag().id();
    headers.emplace_back(std::make_pair(alias, column_name));
  }
  return std::make_pair(std::make_unique<DataExportOpr>(
                            extension_name, file_path, options, headers),
                        ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug