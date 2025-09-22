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
#include "neug/execution/extension/extension.h"

namespace gs {
namespace runtime {
namespace ops {

class ExtensionInstallOpr : public IAdminOperator {
 public:
  ExtensionInstallOpr(std::string extension_name) 
      : extension_name_(std::move(extension_name)) {}
  ~ExtensionInstallOpr() override = default;
  std::string get_operator_name() const override { return "ExtensionInstallOpr"; }
  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;
 private:
  std::string extension_name_;
};

class ExtensionLoadOpr : public IAdminOperator {
 public:
  ExtensionLoadOpr(std::string extension_name) 
      : extension_name_(std::move(extension_name)) {}
  ~ExtensionLoadOpr() override = default;
  std::string get_operator_name() const override { return "ExtensionLoadOpr"; }
  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;
 private:
  std::string extension_name_;
};

class ExtensionUninstallOpr : public IAdminOperator {
 public:
  ExtensionUninstallOpr(std::string extension_name) 
      : extension_name_(std::move(extension_name)) {}
  ~ExtensionUninstallOpr() override = default;
  std::string get_operator_name() const override { return "ExtensionUninstallOpr"; }
  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;
 private:
  std::string extension_name_;
};

//Builders
class ExtensionInstallOprBuilder : public IAdminOperatorBuilder {
 public:
  ExtensionInstallOprBuilder() = default;
  ~ExtensionInstallOprBuilder() override = default;

  std::unique_ptr<IAdminOperator> Build(const Schema& schema,
                                        const physical::AdminPlan& plan,
                                        int op_idx) override;

  physical::AdminPlan_Operator::KindCase GetOpKind() const override {
    return physical::AdminPlan_Operator::KindCase::kExtInstall;
  }
};

class ExtensionLoadOprBuilder : public IAdminOperatorBuilder {
 public:
  ExtensionLoadOprBuilder() = default;
  ~ExtensionLoadOprBuilder() override = default;

  std::unique_ptr<IAdminOperator> Build(const Schema& schema,
                                        const physical::AdminPlan& plan,
                                        int op_idx) override;

  physical::AdminPlan_Operator::KindCase GetOpKind() const override {
    return physical::AdminPlan_Operator::KindCase::kExtLoad;
  }
};

class ExtensionUninstallOprBuilder : public IAdminOperatorBuilder {
 public:
  ExtensionUninstallOprBuilder() = default;
  ~ExtensionUninstallOprBuilder() override = default;

  std::unique_ptr<IAdminOperator> Build(const Schema& schema,
                                        const physical::AdminPlan& plan,
                                        int op_idx) override;

  physical::AdminPlan_Operator::KindCase GetOpKind() const override {
    return physical::AdminPlan_Operator::KindCase::kExtUninstall;
  }
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs