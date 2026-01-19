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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "neug/execution/execute/operator.h"
#include "neug/execution/extension/extension.h"

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
  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
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
  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
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
  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::string extension_name_;
};

// Builders
class ExtensionInstallOprBuilder : public IOperatorBuilder {
 public:
  ExtensionInstallOprBuilder() = default;
  ~ExtensionInstallOprBuilder() override = default;

  gs::result<OpBuildResultT> Build(const Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::PhysicalPlan& plan,
                                   int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kExtInstall};
  }
};

class ExtensionLoadOprBuilder : public IOperatorBuilder {
 public:
  ExtensionLoadOprBuilder() = default;
  ~ExtensionLoadOprBuilder() override = default;

  gs::result<OpBuildResultT> Build(const Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::PhysicalPlan& plan,
                                   int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kExtLoad};
  }
};

class ExtensionUninstallOprBuilder : public IOperatorBuilder {
 public:
  ExtensionUninstallOprBuilder() = default;
  ~ExtensionUninstallOprBuilder() override = default;

  gs::result<OpBuildResultT> Build(const Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::PhysicalPlan& plan,
                                   int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kExtUninstall};
  }
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs