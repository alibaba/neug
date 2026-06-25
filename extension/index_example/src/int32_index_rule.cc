/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "int32_index_rule.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "neug/compiler/binder/expression/scalar_function_expression.h"

#include "int32_index.h"
#include "int32_index_scan.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression_evaluator_utils.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/function_catalog_entry.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/storages/index/index.h"
#include "neug/storages/index/index_manager.h"

namespace neug::extension::index_example {
namespace {

struct IndexPredicate {
  std::shared_ptr<binder::PropertyExpression> property;
  std::shared_ptr<binder::LiteralExpression> literal;
};

bool isCastFunction(const binder::Expression& expr) {
  if (expr.expressionType != common::ExpressionType::FUNCTION) {
    return false;
  }
  auto& funcExpr = expr.constCast<binder::ScalarFunctionExpression>();
  return funcExpr.getFunction().name == "CAST";
}

bool isFoldableCast(const binder::Expression& expr) {
  if (!isCastFunction(expr) || expr.getNumChildren() < 1) {
    return false;
  }
  auto& child = *expr.getChild(0);
  return child.expressionType == common::ExpressionType::LITERAL ||
         isFoldableCast(child);
}

std::shared_ptr<binder::LiteralExpression> tryFoldToLiteral(
    const std::shared_ptr<binder::Expression>& expr, main::ClientContext* ctx) {
  if (expr->expressionType == common::ExpressionType::LITERAL) {
    return std::dynamic_pointer_cast<binder::LiteralExpression>(expr);
  }
  if (!isFoldableCast(*expr) || !ctx) {
    return nullptr;
  }
  auto value = evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(
      expr, ctx);
  return std::make_shared<binder::LiteralExpression>(std::move(value),
                                                     expr->getUniqueName());
}

std::optional<IndexPredicate> MatchIndexPredicate(
    const std::shared_ptr<binder::Expression>& predicate,
    const planner::LogicalScanNodeTable& scan, main::ClientContext* ctx) {
  if (!predicate ||
      predicate->expressionType != common::ExpressionType::EQUALS ||
      predicate->getNumChildren() != 2) {
    return std::nullopt;
  }

  auto left = predicate->getChild(0);
  auto right = predicate->getChild(1);
  if (left->expressionType != common::ExpressionType::PROPERTY) {
    std::swap(left, right);
  }
  if (left->expressionType != common::ExpressionType::PROPERTY ||
      left->getDataType().id() != common::DataTypeId::kInt32) {
    return std::nullopt;
  }

  auto literal = tryFoldToLiteral(right, ctx);
  if (!literal || literal->isNull() ||
      literal->getDataType().id() != common::DataTypeId::kInt32) {
    return std::nullopt;
  }

  auto property = std::dynamic_pointer_cast<binder::PropertyExpression>(left);
  if (!property) {
    return std::nullopt;
  }
  return IndexPredicate{std::move(property), std::move(literal)};
}

Index* FindMatchingIndex(const IndexManager& indexManager, label_t labelId,
                         const std::string& propertyName) {
  Index* result = nullptr;
  for (auto& [name, indexPtr] : indexManager.GetAllIndexEntries()) {
    if (!indexPtr || indexPtr->ModuleTypeName() != Int32Index::type_name()) {
      continue;
    }
    const auto& meta = indexPtr->GetMeta();
    if (meta.schema.label.type != EntryType::VERTEX ||
        meta.schema.label.label_id != labelId ||
        meta.schema.property_names.size() != 1 ||
        meta.schema.property_names[0] != propertyName ||
        meta.schema.property_types.size() != 1 ||
        meta.schema.property_types[0].id() != common::DataTypeId::kInt32) {
      continue;
    }
    if (!result || meta.name < result->GetMeta().name) {
      result = indexPtr.get();
    }
  }
  return result;
}

function::TableFunction* GetIndexScanFunction(catalog::Catalog& catalog) {
  auto* transaction = &transaction::DUMMY_TRANSACTION;
  if (!catalog.containsFunction(transaction, Int32IndexScanFunction::name)) {
    return nullptr;
  }
  auto* entry =
      catalog.getFunctionEntry(transaction, Int32IndexScanFunction::name);
  if (!entry ||
      entry->getType() != catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY) {
    return nullptr;
  }
  auto* function = function::BuiltInFunctionsUtils::matchFunction(
      Int32IndexScanFunction::name, {},
      entry->ptrCast<catalog::FunctionCatalogEntry>());
  return dynamic_cast<function::TableFunction*>(function);
}

}  // namespace

std::shared_ptr<planner::LogicalOperator>
Int32IndexRule::visitScanNodeTableReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  auto* metadata = main::MetadataRegistry::getMetadata();
  auto* index_manager = metadata->getIndexManager();
  if (!index_manager) {
    return op;
  }

  auto* catalog = metadata->getCatalog();
  if (!catalog) {
    return op;
  }

  auto& scan = op->cast<planner::LogicalScanNodeTable>();
  if (scan.getTableIDs().size() != 1 || scan.getNumChildren() != 0 ||
      !scan.getNodeID()) {
    return op;
  }

  auto predicate =
      MatchIndexPredicate(scan.getPredicates(), scan, clientContext_);
  if (!predicate) {
    return op;
  }

  const auto labelId = scan.getTableIDs()[0];
  auto* matchedIndex = FindMatchingIndex(
      *index_manager, labelId, predicate->property->getPropertyName());
  if (!matchedIndex) {
    return op;
  }

  auto* function = GetIndexScanFunction(*catalog);
  if (!function) {
    return op;
  }

  auto nodeID = scan.getNodeID();
  auto& nodeIDProp = nodeID->constCast<binder::PropertyExpression>();
  auto aliasExpr = std::shared_ptr<binder::Expression>(nodeID->copy());
  aliasExpr->setUniqueName(nodeIDProp.getVariableName());
  aliasExpr->setAlias(nodeIDProp.getRawVariableName());
  binder::expression_vector columns{aliasExpr};
  auto bindData = std::make_unique<function::IndexScanBindData>(
      std::move(columns), matchedIndex->GetMeta().name, predicate->literal);
  bindData->numRows = scan.getCardinality();
  auto result = std::make_shared<planner::LogicalTableFunctionCall>(
      *function, std::move(bindData));
  result->computeFlatSchema();
  return result;
}

}  // namespace neug::extension::index_example
