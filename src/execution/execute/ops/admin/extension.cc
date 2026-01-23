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

#include "neug/execution/execute/ops/admin/extension.h"
#include "glog/logging.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace runtime {
namespace ops {
class ExtensionInstallOpr : public IOperator {
 public:
  explicit ExtensionInstallOpr(std::string extension_name)
      : extension_name_(std::move(extension_name)) {}
  ~ExtensionInstallOpr() override = default;
  std::string get_operator_name() const override {
    return "ExtensionInstallOpr";
  }
  gs::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::string extension_name_;
};

class ExtensionLoadOpr : public IOperator {
 public:
  explicit ExtensionLoadOpr(std::string extension_name)
      : extension_name_(std::move(extension_name)) {}
  ~ExtensionLoadOpr() override = default;
  std::string get_operator_name() const override { return "ExtensionLoadOpr"; }
  gs::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::string extension_name_;
};

class ExtensionUninstallOpr : public IOperator {
 public:
  explicit ExtensionUninstallOpr(std::string extension_name)
      : extension_name_(std::move(extension_name)) {}
  ~ExtensionUninstallOpr() override = default;
  std::string get_operator_name() const override {
    return "ExtensionUninstallOpr";
  }
  gs::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::string extension_name_;
};

gs::result<Context> ExtensionInstallOpr::Eval(IStorageInterface& graph,
                                              const ParamsMap& params,
                                              Context&& ctx, OprTimer* timer) {
  LOG(INFO) << "[Admin Pipeline] Executing ExtensionInstall for: "
            << extension_name_;

  auto status = gs::extension::install_extension(extension_name_);
  if (!status.ok()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Install failed: " + status.ToString() +
                                   "; ");
  }
  return gs::result<Context>(std::move(ctx));
}

gs::result<Context> ExtensionLoadOpr::Eval(IStorageInterface& graph,
                                           const ParamsMap& params,
                                           Context&& ctx, OprTimer* timer) {
  LOG(INFO) << "[Admin Pipeline] Executing ExtensionLoad for: "
            << extension_name_;

  auto status = gs::extension::load_extension(extension_name_);
  if (!status.ok()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Load failed: " + status.ToString() + "; ");
  }
  return gs::result<Context>(std::move(ctx));
}

gs::result<Context> ExtensionUninstallOpr::Eval(IStorageInterface& graph,
                                                const ParamsMap& params,
                                                Context&& ctx,
                                                OprTimer* timer) {
  LOG(INFO) << "[Admin Pipeline] Executing ExtensionUninstall for: "
            << extension_name_;

  auto status = gs::extension::uninstall_extension(extension_name_);
  if (!status.ok()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Uninstall failed: " + status.ToString() +
                                   "; ");
  }
  return gs::result<Context>(std::move(ctx));
}

// Builders
gs::result<OpBuildResultT> ExtensionInstallOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& op = plan.plan(op_idx).opr();
  return std::make_pair(
      std::make_unique<ExtensionInstallOpr>(op.ext_install().extension_name()),
      ctx_meta);
}

gs::result<OpBuildResultT> ExtensionLoadOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& op = plan.plan(op_idx).opr();
  return std::make_pair(
      std::make_unique<ExtensionLoadOpr>(op.ext_load().extension_name()),
      ctx_meta);
}

gs::result<OpBuildResultT> ExtensionUninstallOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& op = plan.plan(op_idx).opr();
  return std::make_pair(std::make_unique<ExtensionUninstallOpr>(
                            op.ext_uninstall().extension_name()),
                        ctx_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
