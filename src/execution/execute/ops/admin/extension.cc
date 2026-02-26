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

namespace neug {
namespace execution {
namespace ops {
class ExtensionInstallOpr : public IOperator {
 public:
  explicit ExtensionInstallOpr(std::string extension_name)
      : extension_name_(std::move(extension_name)) {}
  ~ExtensionInstallOpr() override = default;
  std::string get_operator_name() const override {
    return "ExtensionInstallOpr";
  }
  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
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
  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
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
  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  std::string extension_name_;
};

neug::result<Context> ExtensionInstallOpr::Eval(IStorageInterface& graph,
                                                const ParamsMap& params,
                                                Context&& ctx,
                                                OprTimer* timer) {
  LOG(INFO) << "[Admin Pipeline] Executing ExtensionInstall for: "
            << extension_name_;

  auto status = neug::extension::install_extension(extension_name_);
  if (!status.ok()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Install failed: " + status.ToString() +
                                   "; ");
  }
  return neug::result<Context>(std::move(ctx));
}

neug::result<Context> ExtensionLoadOpr::Eval(IStorageInterface& graph,
                                             const ParamsMap& params,
                                             Context&& ctx, OprTimer* timer) {
  LOG(INFO) << "[Admin Pipeline] Executing ExtensionLoad for: "
            << extension_name_;

  auto status = neug::extension::load_extension(extension_name_);
  if (!status.ok()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Load failed: " + status.ToString() + "; ");
  }
  return neug::result<Context>(std::move(ctx));
}

neug::result<Context> ExtensionUninstallOpr::Eval(IStorageInterface& graph,
                                                  const ParamsMap& params,
                                                  Context&& ctx,
                                                  OprTimer* timer) {
  LOG(INFO) << "[Admin Pipeline] Executing ExtensionUninstall for: "
            << extension_name_;

  auto status = neug::extension::uninstall_extension(extension_name_);
  if (!status.ok()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Uninstall failed: " + status.ToString() +
                                   "; ");
  }
  return neug::result<Context>(std::move(ctx));
}

// Builders
neug::result<OpBuildResultT> ExtensionInstallOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& op = plan.plan(op_idx).opr();
  return std::make_pair(
      std::make_unique<ExtensionInstallOpr>(op.ext_install().extension_name()),
      ctx_meta);
}

neug::result<OpBuildResultT> ExtensionLoadOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& op = plan.plan(op_idx).opr();
  return std::make_pair(
      std::make_unique<ExtensionLoadOpr>(op.ext_load().extension_name()),
      ctx_meta);
}

neug::result<OpBuildResultT> ExtensionUninstallOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& op = plan.plan(op_idx).opr();
  return std::make_pair(std::make_unique<ExtensionUninstallOpr>(
                            op.ext_uninstall().extension_name()),
                        ctx_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug