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

#include "hnsw_index_scan.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "hnsw_index.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/catalog/catalog_entry/function_catalog_entry.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/planner/operator/logical_order_by.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/index/index_utils.h"
#include "neug/utils/exception/exception.h"

namespace neug::vector_search_ext {
namespace {

template <typename T>
T ParseUnsignedOption(const std::string& value, const std::string& name) {
  size_t parsed_length = 0;
  unsigned long long parsed_value = 0;
  if (!value.empty() && value.front() == '-') {
    THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN option '" + name +
                        "' must be an unsigned integer: " + value);
  }
  try {
    parsed_value = std::stoull(value, &parsed_length);
  } catch (const std::invalid_argument&) {
    THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN option '" + name +
                        "' has an invalid value: " + value);
  } catch (const std::out_of_range&) {
    THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN option '" + name +
                        "' is out of range: " + value);
  }
  if (parsed_length != value.size() ||
      parsed_value > std::numeric_limits<T>::max()) {
    THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN option '" + name +
                        "' is out of range: " + value);
  }
  return static_cast<T>(parsed_value);
}

bool IsVectorDistanceFunction(const function::ScalarFunction& function) {
  return function.name == "VECTOR_DISTANCE_L2" ||
         function.name == "VECTOR_DISTANCE_COSINE" ||
         function.name == "VECTOR_DISTANCE_IP";
}

std::string DistanceMetric(const function::ScalarFunction& function) {
  if (function.name == "VECTOR_DISTANCE_L2") {
    return "l2";
  }
  if (function.name == "VECTOR_DISTANCE_COSINE") {
    return "cosine";
  }
  if (function.name == "VECTOR_DISTANCE_IP") {
    return "ip";
  }
  return {};
}

std::string IndexMetric(const IndexMeta& meta) {
  auto metric = meta.options.find("metric");
  if (metric == meta.options.end() || metric->second == "l2" ||
      metric->second == "l2sq") {
    return "l2";
  }
  if (metric->second == "inner_product" || metric->second == "ip") {
    return "ip";
  }
  return metric->second;
}

bool IsCompatibleHNSWIndex(const StorageIndex& index,
                           const binder::ScalarFunctionExpression& distance,
                           const binder::Expression& target) {
  const auto& meta = index.GetMeta();
  if (!IsHNSWIndex(meta) ||
      IndexMetric(meta) != DistanceMetric(distance.getFunction())) {
    return false;
  }
  if (meta.schema.columns.size() != 1) {
    return false;
  }
  const auto& property_type = meta.schema.columns[0].property_type;
  const auto& target_type = target.getDataType();
  if (property_type.id() != DataTypeId::kArray ||
      target_type.id() != DataTypeId::kArray) {
    return false;
  }
  return ArrayType::GetChildType(property_type) ==
             ArrayType::GetChildType(target_type) &&
         ArrayType::GetNumElements(property_type) ==
             ArrayType::GetNumElements(target_type);
}

std::shared_ptr<binder::ScalarFunctionExpression> FindDistanceExpression(
    const planner::LogicalOrderBy& order_by,
    const planner::LogicalProjection& projection) {
  const auto order_expressions = order_by.getExpressionsToOrderBy();
  if (order_expressions.size() != 1) {
    return nullptr;
  }

  const auto& order_expression = order_expressions[0];
  const auto matches_order = [&](const function::ScalarFunction& function) {
    if (!IsVectorDistanceFunction(function)) {
      return false;
    }
    const bool is_inner_product = function.name == "VECTOR_DISTANCE_IP";
    return order_by.getIsAscOrders()[0] != is_inner_product;
  };
  if (order_expression->expressionType == common::ExpressionType::FUNCTION) {
    auto function =
        order_expression->ptrCast<binder::ScalarFunctionExpression>();
    if (matches_order(function->getFunction())) {
      return std::static_pointer_cast<binder::ScalarFunctionExpression>(
          order_expression);
    }
  }

  for (const auto& expression : projection.getExpressionsToProject()) {
    if (expression->expressionType != common::ExpressionType::FUNCTION ||
        expression->getUniqueName() != order_expression->getUniqueName()) {
      continue;
    }
    auto function = expression->ptrCast<binder::ScalarFunctionExpression>();
    if (matches_order(function->getFunction())) {
      return std::static_pointer_cast<binder::ScalarFunctionExpression>(
          expression);
    }
  }
  return nullptr;
}

bool ExtractDistanceArguments(const binder::ScalarFunctionExpression& distance,
                              const binder::PropertyExpression*& property,
                              std::shared_ptr<binder::Expression>& target) {
  const auto children = distance.getChildren();
  if (children.size() != 2) {
    return false;
  }
  for (const auto& child : children) {
    if (child->expressionType == common::ExpressionType::PROPERTY) {
      property = child->ptrCast<binder::PropertyExpression>();
    } else {
      target = child;
    }
  }
  return property != nullptr && target != nullptr;
}

bool ContainsPrimaryKeyEqualityPredicate(
    const std::shared_ptr<binder::Expression>& expression,
    common::table_id_t table_id) {
  if (!expression) {
    return false;
  }
  if (expression->expressionType == common::ExpressionType::EQUALS &&
      expression->getNumChildren() == 2) {
    for (const auto& child : expression->getChildren()) {
      if (child->expressionType == common::ExpressionType::PROPERTY &&
          child->constCast<binder::PropertyExpression>().isPrimaryKey(
              table_id)) {
        return true;
      }
    }
  }
  if (expression->expressionType == common::ExpressionType::AND) {
    for (const auto& child : expression->getChildren()) {
      if (ContainsPrimaryKeyEqualityPredicate(child, table_id)) {
        return true;
      }
    }
  }
  return false;
}

std::shared_ptr<binder::Expression> MakeScanColumn(
    const binder::PropertyExpression& property) {
  auto output = std::shared_ptr<binder::Expression>(property.copy());
  output->setUniqueName(property.getVariableName());
  output->setAlias(property.getRawVariableName());
  return output;
}

std::shared_ptr<binder::Expression> MakeScoreColumn(
    const binder::ScalarFunctionExpression& score) {
  auto output = std::make_shared<binder::VariableExpression>(
      DataType::DOUBLE, score.Expression::getUniqueName(),
      score.Expression::getUniqueName());
  if (score.hasAlias()) {
    output->setAlias(score.getAlias());
  }
  return output;
}

const binder::PropertyExpression* GetVertexOnlyOutput(
    const planner::LogicalOperator& input,
    const binder::PropertyExpression& distance_property) {
  const auto* schema = input.getSchema();
  if (schema == nullptr) {
    return nullptr;
  }
  const binder::PropertyExpression* vertex = nullptr;
  for (const auto& expression : schema->getExpressionsInScope()) {
    const binder::PropertyExpression* property = nullptr;
    if (expression->expressionType == common::ExpressionType::PATTERN) {
      const auto* node = expression->constPtrCast<binder::NodeExpression>();
      if (node == nullptr || node->getTableIDs().size() != 1 ||
          node->getTableIDs()[0] != distance_property.getSingleTableID()) {
        return nullptr;
      }
      property =
          node->getInternalIDRef()->constPtrCast<binder::PropertyExpression>();
    } else if (expression->expressionType == common::ExpressionType::PROPERTY) {
      property = expression->constPtrCast<binder::PropertyExpression>();
    } else {
      return nullptr;
    }
    if (property == nullptr || !property->isInternalID() ||
        !property->isSingleLabel() ||
        property->getSingleTableID() != distance_property.getSingleTableID()) {
      return nullptr;
    }
    if (property->getVariableName() == distance_property.getVariableName()) {
      vertex = property;
    }
  }
  return vertex;
}

std::unique_ptr<function::CallFuncInputBase> BindHNSWIndexScan(
    const Schema&, const execution::ContextMeta& context_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& op = plan.plan(op_idx);
  const auto& scan = op.opr().index_scan();
  auto input = std::make_unique<HNSWIndexScanFuncInput>();
  std::string label;
  std::string topk;
  for (const auto& option : scan.options()) {
    if (option.first == "label_id") {
      label = option.second;
    } else if (option.first == "topk") {
      topk = option.second;
    }
  }
  if (label.empty() || scan.unique_index_name().empty() || topk.empty()) {
    THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN is missing required options");
  }
  input->label_id = ParseUnsignedOption<label_t>(label, "label_id");
  input->unique_index_name = scan.unique_index_name();
  input->topk = ParseUnsignedOption<uint32_t>(topk, "topk");
  input->target_value = execution::parse_expression(
      scan.target_value(), context_meta, execution::VarType::kRecord);
  if (op.meta_data_size() != 2) {
    THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN must have vertex and score outputs");
  }
  input->vertex_alias = op.meta_data(0).alias();
  input->score_alias = op.meta_data(1).alias();
  return input;
}

execution::Context ExecuteHNSWIndexScan(
    const function::CallFuncInputBase& base_input, IStorageInterface& graph) {
  const auto& input = dynamic_cast<const HNSWIndexScanFuncInput&>(base_input);
  auto* reader = dynamic_cast<StorageReadInterface*>(&graph);
  if (reader == nullptr) {
    THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN requires a readable graph");
  }

  HNSWIndexQueryParams params;
  params.target_value = input.bound_target_value;
  params.topk = input.topk;
  constexpr uint32_t kMinEfSearch = 100;
  const auto doubled_topk =
      input.topk > std::numeric_limits<uint32_t>::max() / 2
          ? std::numeric_limits<uint32_t>::max()
          : input.topk * 2;
  params.ef_search = std::max(doubled_topk, kMinEfSearch);

  std::vector<std::shared_ptr<IVertexColumn>> filter_inputs;
  size_t scalar_filter_size = 0;
  for (const auto& context_chunk : input.context.chunks()) {
    if (!context_chunk.exist(input.vertex_alias)) {
      THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN filter input alias not found");
    }
    auto vertices = std::dynamic_pointer_cast<IVertexColumn>(
        context_chunk.get(input.vertex_alias));
    if (!vertices) {
      THROW_RUNTIME_ERROR(
          "HNSW_INDEX_SCAN filter input must be a vertex column");
    }
    scalar_filter_size += vertices->size();
    filter_inputs.push_back(std::move(vertices));
  }

  if (!filter_inputs.empty()) {
    params.use_scalar_filter = true;
    params.scalar_filter.reserve(scalar_filter_size);
  }
  for (const auto& vertices : filter_inputs) {
    foreach_vertex(*vertices, [&](size_t, label_t label, vid_t vid) {
      if (label != input.label_id) {
        THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN filter input label mismatch");
      }
      if (vid != INVALID_VID) {
        params.scalar_filter.push_back(vid);
      }
    });
  }
  auto result = reader->IndexSearch(input.unique_index_name, params);
  if (!result.has_value()) {
    THROW_RUNTIME_ERROR(result.error().ToString());
  }

  MSVertexColumnBuilder builder(input.label_id);
  ValueColumnBuilder<double> score_builder;
  builder.reserve(result->size());
  score_builder.reserve(result->size());
  for (const auto& item : result.value()) {
    builder.push_back_opt(item.vid);
    score_builder.push_back_opt(item.score);
  }
  execution::Context context;
  execution::ContextChunk chunk;
  chunk.set(input.vertex_alias, builder.finish());
  chunk.set(input.score_alias, score_builder.finish());
  context.append_chunk(std::move(chunk));
  return context;
}

}  // namespace

std::unique_ptr<function::CallFuncInputBase> HNSWIndexScanFuncInput::bindParams(
    const execution::ParamsMap& params) const {
  if (target_value == nullptr) {
    THROW_RUNTIME_ERROR("HNSW_INDEX_SCAN target expression is not initialized");
  }
  auto bound_expression = target_value->bind(nullptr, params);
  if (bound_expression == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "HNSW_INDEX_SCAN target expression contains an unbound parameter");
  }

  auto bound = std::make_unique<HNSWIndexScanFuncInput>();
  bound->label_id = label_id;
  bound->unique_index_name = unique_index_name;
  bound->topk = topk;
  bound->vertex_alias = vertex_alias;
  bound->score_alias = score_alias;
  bound->bound_target_value =
      bound_expression->Cast<execution::RecordExprBase>().eval_record(
          DataChunk(), 0);
  return bound;
}

function::function_set HNSWIndexScanFunction::getFunctionSet() {
  auto function = std::make_unique<function::NeugCallFunction>(
      name, function::call_input_types{}, function::call_output_columns{});
  function->bindFunc = BindHNSWIndexScan;
  function->execFunc = ExecuteHNSWIndexScan;
  function::function_set result;
  result.push_back(std::move(function));
  return result;
}

void HNSWIndexScanOptimizer::rewrite(main::ClientContext* context,
                                     planner::LogicalPlan* plan) {
  context_ = context;
  optimizer::LogicalRule::rewrite(context, plan);
  context_ = nullptr;
}

std::shared_ptr<planner::LogicalOperator>
HNSWIndexScanOptimizer::visitOrderByReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  if (context_ == nullptr) {
    return op;
  }
  auto order_by = op->ptrCast<planner::LogicalOrderBy>();
  if (!order_by->isTopK() ||
      (order_by->getSkipNum() != 0 && order_by->getSkipNum() != UINT64_MAX) ||
      order_by->getLimitNum() == 0 || order_by->getNumChildren() != 1) {
    return op;
  }
  auto child = order_by->getChild(0);
  if (child->getOperatorType() != planner::LogicalOperatorType::PROJECTION) {
    return op;
  }
  auto projection = child->ptrCast<planner::LogicalProjection>();
  if (projection->getNumChildren() != 1) {
    return op;
  }
  auto input_op = projection->getChild(0);

  auto distance = FindDistanceExpression(*order_by, *projection);
  if (distance == nullptr) {
    return op;
  }
  const binder::PropertyExpression* property = nullptr;
  std::shared_ptr<binder::Expression> target;
  if (!ExtractDistanceArguments(*distance, property, target) ||
      !property->isSingleLabel()) {
    return op;
  }

  auto* metadata_manager = context_->getMetadataManager();
  if (metadata_manager == nullptr) {
    return op;
  }
  auto graph_stats = metadata_manager->getGraphStats();
  if (graph_stats == nullptr) {
    return op;
  }
  auto indexes = graph_stats->GetIndex(property->getSingleTableID(),
                                       {property->getPropertyName()});
  if (!indexes.has_value()) {
    return op;
  }
  const StorageIndex* hnsw_index = nullptr;
  for (const auto* index : indexes.value()) {
    if (index != nullptr && IsCompatibleHNSWIndex(*index, *distance, *target)) {
      hnsw_index = index;
      break;
    }
  }
  if (hnsw_index == nullptr) {
    return op;
  }

  const binder::PropertyExpression* vertex_output = nullptr;
  bool attach_input = true;
  if (input_op->getOperatorType() ==
      planner::LogicalOperatorType::SCAN_NODE_TABLE) {
    auto scan = input_op->ptrCast<planner::LogicalScanNodeTable>();
    if (scan->getTableIDs().size() != 1 ||
        scan->getScanType() != planner::LogicalScanNodeTableType::SCAN ||
        ContainsPrimaryKeyEqualityPredicate(scan->getPredicates(),
                                            scan->getTableIDs()[0]) ||
        property->getVariableName() != scan->getAliasName() ||
        property->getSingleTableID() != scan->getTableIDs()[0]) {
      return op;
    }
    vertex_output = &scan->getNodeID()->constCast<binder::PropertyExpression>();
    attach_input = scan->getPredicates() != nullptr ||
                   !scan->getPropertyPredicates().empty();
  } else {
    vertex_output = GetVertexOnlyOutput(*input_op, *property);
    if (vertex_output == nullptr) {
      return op;
    }
  }

  auto* function = GetIndexScanFunction(*context_->getCatalog());
  if (function == nullptr) {
    return op;
  }
  binder::expression_vector columns{MakeScanColumn(*vertex_output),
                                    MakeScoreColumn(*distance)};
  auto bind_data = std::make_unique<function::IndexScanBindData>(
      columns, hnsw_index->GetMeta().name, target);
  bind_data->options["label_id"] = std::to_string(property->getSingleTableID());
  bind_data->options["topk"] = std::to_string(order_by->getLimitNum());

  auto table_call = std::make_shared<planner::LogicalTableFunctionCall>(
      *function, std::move(bind_data));
  if (attach_input) {
    table_call->addChild(std::move(input_op));
  }
  table_call->computeFlatSchema();
  projection->setChild(0, std::move(table_call));
  return op;
}

function::TableFunction* HNSWIndexScanOptimizer::GetIndexScanFunction(
    catalog::Catalog& catalog) const {
  auto* transaction = &transaction::DUMMY_TRANSACTION;
  if (!catalog.containsFunction(transaction, HNSWIndexScanFunction::name)) {
    return nullptr;
  }
  auto* entry =
      catalog.getFunctionEntry(transaction, HNSWIndexScanFunction::name);
  if (entry == nullptr ||
      entry->getType() != catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY) {
    return nullptr;
  }
  auto* function = function::BuiltInFunctionsUtils::matchFunction(
      HNSWIndexScanFunction::name, {},
      entry->ptrCast<catalog::FunctionCatalogEntry>());
  return dynamic_cast<function::TableFunction*>(function);
}

}  // namespace neug::vector_search_ext
