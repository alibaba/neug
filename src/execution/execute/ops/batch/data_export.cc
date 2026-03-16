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
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/gopt/g_catalog_holder.h"
#include "neug/execution/execute/ops/batch/data_source.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace execution {
namespace ops {

class DataExportOpr : public IOperator {
 public:
  DataExportOpr(const reader::FileSchema& schema,
                std::shared_ptr<reader::EntrySchema> entry_schema,
                function::ExportFunction* exportFunction)
      : schema_(schema),
        entry_schema_(std::move(entry_schema)),
        exportFunction_(exportFunction) {}

  std::string get_operator_name() const override { return "DataExportOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override;

 private:
  reader::FileSchema schema_;
  std::shared_ptr<reader::EntrySchema> entry_schema_;
  function::ExportFunction* exportFunction_;
};

neug::result<neug::execution::Context> DataExportOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params,
    neug::execution::Context&& ctx, neug::execution::OprTimer* timer) {
  const auto& graph =
      dynamic_cast<const StorageReadInterface&>(graph_interface);
  if (!exportFunction_) {
    THROW_IO_EXCEPTION("DataExportOpr: export function is nullptr");
  }
  if (!exportFunction_->execFunc) {
    THROW_IO_EXCEPTION(
        "DataExportOpr: write function in export function is nullptr");
  }
  return exportFunction_->execFunc(ctx, schema_, entry_schema_, graph);
}

neug::result<OpBuildResultT> DataExportOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& data_export_opr = plan.plan(op_idx).opr().data_export();
  std::string extension_name = data_export_opr.extension_name();
  auto signatureName = data_export_opr.extension_name();
  auto gCatalog = catalog::GCatalogHolder::getGCatalog();
  auto func = gCatalog->getFunctionWithSignature(signatureName);
  auto writeFunc = func->ptrCast<function::ExportFunction>();
  auto fileSchema =
      ops::ReadStateBuilder::buildFileSchema(data_export_opr.file_schema());
  auto entrySchema =
      ops::ReadStateBuilder::buildEntrySchema(data_export_opr.entry_schema());
  return std::make_pair(std::make_unique<DataExportOpr>(
                            fileSchema, std::move(entrySchema), writeFunc),
                        ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug