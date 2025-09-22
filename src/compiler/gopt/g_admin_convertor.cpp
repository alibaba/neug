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

#include "neug/compiler/gopt/g_admin_convertor.h"
#include <string>
#include "neug/compiler/planner/operator/logical_transaction.h"

namespace gs {
namespace gopt {

std::unique_ptr<::physical::AdminPlan> GAdminConvertor::convert(
    const planner::LogicalPlan& plan) {
  auto adminPB = std::make_unique<::physical::AdminPlan>();
  convertOperator(adminPB.get(), plan.getLastOperatorRef());
  return adminPB;
}

void GAdminConvertor::convertOperator(::physical::AdminPlan* plan,
                                      const planner::LogicalOperator& op) {
  for (auto child : op.getChildren()) {
    convertOperator(plan, *child);
  }

  switch (op.getOperatorType()) {
  case planner::LogicalOperatorType::TRANSACTION: {
    plan->mutable_plan()->AddAllocated(convertCheckpoint(op).release());
    break;
  }
  case planner::LogicalOperatorType::EXTENSION: {
    plan->mutable_plan()->AddAllocated(convertExtension(op).release());
    break;
  }
  default:
    break;
  }
}

std::unique_ptr<::physical::AdminPlan::Operator>
GAdminConvertor::convertCheckpoint(const planner::LogicalOperator& op) {
  auto transaction = op.constPtrCast<planner::LogicalTransaction>();
  auto action = transaction->getTransactionAction();
  if (action != transaction::TransactionAction::CHECKPOINT) {
    THROW_EXCEPTION_WITH_FILE_LINE("Cannot convert transaction action " +
                                   std::to_string(static_cast<int>(action)) +
                                   " to admin operator");
  }
  auto checkpointPB = std::make_unique<::physical::AdminPlan::Operator>();
  checkpointPB->set_allocated_checkpoint(new ::physical::Checkpoint());
  return checkpointPB;
}

std::unique_ptr<::physical::AdminPlan::Operator>
GAdminConvertor::convertExtension(const planner::LogicalOperator& op) {
  auto& ext = op.constCast<planner::LogicalExtension>();
  const auto& aux = ext.getAuxInfo();
  
  auto adminOpr = std::make_unique<::physical::AdminPlan::Operator>();
  
  switch (aux.action) {
  case planner::ExtensionAction::INSTALL: {
    auto* install = new ::physical::ExtensionInstall();
    install->set_extension_name(aux.path);
    adminOpr->set_allocated_ext_install(install);
    break;
  }
  case planner::ExtensionAction::LOAD: {
    auto* load = new ::physical::ExtensionLoad();
    load->set_extension_name(aux.path);
    adminOpr->set_allocated_ext_load(load);
    break;
  }
  case planner::ExtensionAction::UNINSTALL: {
    auto* uninst = new ::physical::ExtensionUninstall();
    uninst->set_extension_name(aux.path);
    adminOpr->set_allocated_ext_uninstall(uninst);
    break;
  }
  default:
    THROW_EXCEPTION_WITH_FILE_LINE("Unknown extension action");
  }
  
  return adminOpr;
}
}  // namespace gopt
}  // namespace gs