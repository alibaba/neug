/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include "fts_index_scan.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fts_function.h"
#include "fts_index.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/catalog/catalog_entry/function_catalog_entry.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/planner/operator/logical_order_by.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/execution/common/context.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/utils/exception/exception.h"

namespace neug::fts_ext {

std::unique_ptr<function::CallFuncInputBase> FTSIndexScanFuncInput::bindParams(
    const execution::ParamsMap& params) const {
  auto bound = std::make_unique<FTSIndexScanFuncInput>();
  bound->label_id = label_id;
  bound->unique_index_name = unique_index_name;
  if (query_literal) {
    bound->query_string = *query_literal;
  } else if (query_parameter) {
    auto parameter = params.find(*query_parameter);
    if (parameter == params.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("FTS_INDEX_SCAN query parameter $" +
                                       *query_parameter + " is missing");
    }
    if (parameter->second.IsNull()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "FTS_INDEX_SCAN query parameter must not be NULL");
    }
    if (parameter->second.type().id() != DataTypeId::kVarchar) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "FTS_INDEX_SCAN query parameter must be STRING");
    }
    bound->query_string = parameter->second.GetValue<std::string>();
  } else {
    THROW_RUNTIME_ERROR("FTS_INDEX_SCAN query is not initialized");
  }
  bound->topk = topk;
  bound->node_alias = node_alias;
  bound->score_alias = score_alias;
  return bound;
}

namespace {

std::shared_ptr<binder::ScalarFunctionExpression> FindBM25Expression(
    const planner::LogicalOrderBy& order_by,
    const planner::LogicalProjection& projection) {
  const auto order_expressions = order_by.getExpressionsToOrderBy();
  if (order_expressions.size() != 1 || order_by.getIsAscOrders().size() != 1 ||
      !order_by.getIsAscOrders()[0]) {
    return nullptr;
  }

  const auto& order_expression = order_expressions[0];
  if (order_expression->expressionType == common::ExpressionType::FUNCTION) {
    auto* function =
        order_expression->ptrCast<binder::ScalarFunctionExpression>();
    if (function->getFunction().name == FTSBM25Function::name) {
      return std::static_pointer_cast<binder::ScalarFunctionExpression>(
          order_expression);
    }
  }
  for (const auto& expression : projection.getExpressionsToProject()) {
    if (expression->expressionType != common::ExpressionType::FUNCTION ||
        expression->getUniqueName() != order_expression->getUniqueName()) {
      continue;
    }
    auto* function = expression->ptrCast<binder::ScalarFunctionExpression>();
    if (function->getFunction().name == FTSBM25Function::name) {
      return std::static_pointer_cast<binder::ScalarFunctionExpression>(
          expression);
    }
  }
  return nullptr;
}

bool ExtractBM25Arguments(const binder::ScalarFunctionExpression& expression,
                          const binder::PropertyExpression*& property,
                          std::shared_ptr<binder::Expression>& query) {
  auto children = expression.getChildren();
  if (children.size() != 2 ||
      children[0]->expressionType != common::ExpressionType::PROPERTY ||
      (children[1]->expressionType != common::ExpressionType::LITERAL &&
       children[1]->expressionType != common::ExpressionType::PARAMETER)) {
    return false;
  }
  property = children[0]->ptrCast<binder::PropertyExpression>();
  query = children[1];
  return property != nullptr && query != nullptr &&
         query->getDataType().id() == DataTypeId::kVarchar;
}

std::shared_ptr<binder::Expression> MakeScanColumn(
    const binder::PropertyExpression& property, DataType node_type) {
  auto output = std::make_shared<binder::VariableExpression>(
      std::move(node_type), property.getVariableName(),
      property.getVariableName());
  output->setAlias(property.getRawVariableName());
  return output;
}

DataType MakeScanNodeType(main::ClientContext& context,
                          common::table_id_t table_id) {
  DataType result{DataTypeId::kVertex};
  const auto* entry = context.getCatalog()->getTableCatalogEntry(
      context.getTransaction(), table_id);
  const auto* vertex_schema = dynamic_cast<const VertexSchema*>(entry);
  if (!vertex_schema) {
    return result;
  }
  auto graph_type = std::make_shared<gopt::GNodeType>(
      std::vector<const VertexSchema*>{vertex_schema});
  result.setExtraTypeInfo(std::make_shared<common::GNodeTypeInfo>(
      std::vector<std::string>{}, std::vector<DataType>{},
      std::move(graph_type)));
  return result;
}

const binder::PropertyExpression* FindVertexOutput(
    const planner::LogicalOperator& input,
    const binder::PropertyExpression& indexed_property) {
  const auto* schema = input.getSchema();
  if (!schema) {
    return nullptr;
  }
  for (const auto& expression : schema->getExpressionsInScope()) {
    const binder::PropertyExpression* property = nullptr;
    if (expression->expressionType == common::ExpressionType::PATTERN) {
      const auto* node = expression->constPtrCast<binder::NodeExpression>();
      if (node) {
        property = node->getInternalIDRef()
                       ->constPtrCast<binder::PropertyExpression>();
      }
    } else if (expression->expressionType == common::ExpressionType::PROPERTY) {
      property = expression->constPtrCast<binder::PropertyExpression>();
    }
    if (property && property->isInternalID() && property->isSingleLabel() &&
        property->getSingleTableID() == indexed_property.getSingleTableID() &&
        property->getVariableName() == indexed_property.getVariableName()) {
      return property;
    }
  }
  return nullptr;
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

std::unique_ptr<function::CallFuncInputBase> BindFTSIndexScan(
    const Schema&, const execution::ContextMeta&,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& op = plan.plan(op_idx);
  const auto& scan = op.opr().index_scan();
  auto input = std::make_unique<FTSIndexScanFuncInput>();
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
    THROW_RUNTIME_ERROR("FTS_INDEX_SCAN is missing required options");
  }
  if (op.meta_data_size() != 2) {
    THROW_RUNTIME_ERROR("FTS_INDEX_SCAN must produce node and score columns");
  }
  input->label_id = static_cast<label_t>(std::stoul(label));
  input->unique_index_name = scan.unique_index_name();
  const auto& target = scan.target_value();
  if (target.operators_size() != 1) {
    THROW_RUNTIME_ERROR(
        "FTS_INDEX_SCAN query must be a STRING literal or parameter");
  }
  const auto& query = target.operators(0);
  if (query.has_const_() && query.const_().has_str()) {
    input->query_literal = query.const_().str();
  } else if (query.has_param()) {
    input->query_parameter = query.param().name();
  } else {
    THROW_RUNTIME_ERROR(
        "FTS_INDEX_SCAN query must be a STRING literal or parameter");
  }
  input->topk = static_cast<uint32_t>(std::stoul(topk));
  input->node_alias = op.meta_data(0).alias();
  input->score_alias = op.meta_data(1).alias();
  return input;
}

execution::Context ExecuteFTSIndexScan(
    const function::CallFuncInputBase& base_input, IStorageInterface& graph) {
  const auto& input = dynamic_cast<const FTSIndexScanFuncInput&>(base_input);
  auto* reader = dynamic_cast<StorageReadInterface*>(&graph);
  if (!reader) {
    THROW_RUNTIME_ERROR("FTS_INDEX_SCAN requires a readable graph");
  }

  FTSQueryParams params;
  params.query_string = input.query_string;
  params.topk = input.topk;
  for (const auto& context_chunk : input.context.chunks()) {
    if (!context_chunk.exist(input.node_alias)) {
      THROW_RUNTIME_ERROR("FTS_INDEX_SCAN filter input alias not found");
    }
    params.use_scalar_filter = true;
    auto vertices = std::dynamic_pointer_cast<IVertexColumn>(
        context_chunk.get(input.node_alias));
    if (!vertices) {
      THROW_RUNTIME_ERROR(
          "FTS_INDEX_SCAN filter input must be a vertex column");
    }
    params.scalar_filter.reserve(params.scalar_filter.size() +
                                 vertices->size());
    foreach_vertex(*vertices, [&](size_t, label_t label, vid_t vid) {
      if (label != input.label_id) {
        THROW_RUNTIME_ERROR("FTS_INDEX_SCAN filter input label mismatch");
      }
      if (vid != INVALID_VID) {
        params.scalar_filter.push_back(vid);
      }
    });
  }
  auto results = reader->IndexSearch(input.unique_index_name, params);
  if (!results) {
    THROW_RUNTIME_ERROR(results.error().ToString());
  }

  bool has_filter_input = false;
  for (const auto& context_chunk : input.context.chunks()) {
    has_filter_input =
        has_filter_input || context_chunk.exist(input.node_alias);
  }
  if (has_filter_input) {
    execution::Context context = input.context;
    context.flatten();
    auto& chunk = context.chunk(0);
    auto vertices =
        std::dynamic_pointer_cast<IVertexColumn>(chunk.get(input.node_alias));
    if (!vertices) {
      THROW_RUNTIME_ERROR(
          "FTS_INDEX_SCAN filter input must be a vertex column");
    }

    std::unordered_map<vid_t, std::vector<size_t>> rows_by_vid;
    foreach_vertex(*vertices, [&](size_t row, label_t label, vid_t vid) {
      if (label == input.label_id && vid != INVALID_VID) {
        rows_by_vid[vid].push_back(row);
      }
    });

    sel_vec_t offsets;
    ValueColumnBuilder<double> score_builder;
    offsets.reserve(input.topk);
    score_builder.reserve(input.topk);
    for (const auto& result : results.value()) {
      auto rows = rows_by_vid.find(result.vid);
      if (rows == rows_by_vid.end()) {
        continue;
      }
      for (auto row : rows->second) {
        if (offsets.size() == input.topk) {
          break;
        }
        offsets.push_back(row);
        score_builder.push_back_opt(result.score);
      }
      if (offsets.size() == input.topk) {
        break;
      }
    }
    chunk.reshuffle(offsets);
    chunk.set(input.score_alias, score_builder.finish());
    return context;
  }

  MSVertexColumnBuilder node_builder(input.label_id);
  ValueColumnBuilder<double> score_builder;
  node_builder.reserve(results->size());
  score_builder.reserve(results->size());
  for (const auto& result : results.value()) {
    node_builder.push_back_opt(result.vid);
    score_builder.push_back_opt(result.score);
  }
  execution::ContextChunk chunk;
  chunk.set(input.node_alias, node_builder.finish());
  chunk.set(input.score_alias, score_builder.finish());
  execution::Context context;
  context.append_chunk(std::move(chunk));
  return context;
}

}  // namespace

function::function_set FTSIndexScanFunction::getFunctionSet() {
  auto function = std::make_unique<function::NeugCallFunction>(
      name, function::call_input_types{}, function::call_output_columns{});
  function->bindFunc = BindFTSIndexScan;
  function->execFunc = ExecuteFTSIndexScan;
  function::function_set result;
  result.push_back(std::move(function));
  return result;
}

void FTSIndexScanOptimizer::rewrite(main::ClientContext* context,
                                    planner::LogicalPlan* plan) {
  context_ = context;
  optimizer::LogicalRule::rewrite(context, plan);
  context_ = nullptr;
}

std::shared_ptr<planner::LogicalOperator>
FTSIndexScanOptimizer::visitOrderByReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  if (!context_) {
    return op;
  }
  auto order_by = op->ptrCast<planner::LogicalOrderBy>();
  if (!order_by->isTopK() || order_by->getSkipNum() != 0 ||
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

  auto bm25 = FindBM25Expression(*order_by, *projection);
  if (!bm25) {
    return op;
  }
  const binder::PropertyExpression* property = nullptr;
  std::shared_ptr<binder::Expression> query;
  if (!ExtractBM25Arguments(*bm25, property, query) ||
      !property->isSingleLabel()) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "BM25 on the current storage index API requires one node STRING "
        "property and a STRING query");
  }

  auto* metadata_manager = context_->getMetadataManager();
  if (!metadata_manager) {
    return op;
  }
  auto graph_stats = metadata_manager->getGraphStats();
  if (!graph_stats) {
    return op;
  }
  auto indexes = graph_stats->GetIndex(property->getSingleTableID(),
                                       property->getPropertyName());
  if (!indexes.has_value()) {
    THROW_RUNTIME_ERROR("FTS index not found for the requested label/property");
  }
  const StorageIndex* fts_index = nullptr;
  for (const auto* index : indexes.value()) {
    if (index && dynamic_cast<const FTSIndex*>(index)) {
      fts_index = index;
      break;
    }
  }
  if (!fts_index) {
    THROW_RUNTIME_ERROR("FTS index not found for the requested label/property");
  }

  const binder::PropertyExpression* vertex_output = nullptr;
  bool attach_input = true;
  if (input_op->getOperatorType() ==
      planner::LogicalOperatorType::SCAN_NODE_TABLE) {
    auto scan = input_op->ptrCast<planner::LogicalScanNodeTable>();
    if (scan->getTableIDs().size() != 1 ||
        property->getVariableName() != scan->getAliasName() ||
        property->getSingleTableID() != scan->getTableIDs()[0]) {
      return op;
    }
    vertex_output = &scan->getNodeID()->constCast<binder::PropertyExpression>();
    attach_input = scan->getScanType() ==
                       planner::LogicalScanNodeTableType::PRIMARY_KEY_SCAN ||
                   scan->getPredicates() != nullptr ||
                   !scan->getPropertyPredicates().empty();
  } else {
    vertex_output = FindVertexOutput(*input_op, *property);
    if (!vertex_output) {
      return op;
    }
  }

  auto* function = GetIndexScanFunction(*context_->getCatalog());
  if (!function) {
    return op;
  }
  auto node_column =
      MakeScanColumn(*vertex_output,
                     MakeScanNodeType(*context_, property->getSingleTableID()));
  auto score_column = MakeScoreColumn(*bm25);
  binder::expression_vector columns{node_column, score_column};
  auto bind_data = std::make_unique<function::IndexScanBindData>(
      columns, fts_index->GetMeta().name, query);
  bind_data->options["label_id"] = std::to_string(property->getSingleTableID());
  bind_data->options["topk"] = std::to_string(order_by->getLimitNum());

  auto table_call = std::make_shared<planner::LogicalTableFunctionCall>(
      *function, std::move(bind_data));
  if (attach_input) {
    table_call->addChild(std::move(input_op));
  }
  table_call->computeFlatSchema();
  projection->setChild(0, std::move(table_call));
  // FTSIndex::SearchImpl guarantees ordered results, so remove OrderBy.
  return child;
}

function::TableFunction* FTSIndexScanOptimizer::GetIndexScanFunction(
    catalog::Catalog& catalog) const {
  auto* transaction = &transaction::DUMMY_TRANSACTION;
  if (!catalog.containsFunction(transaction, FTSIndexScanFunction::name)) {
    return nullptr;
  }
  auto* entry =
      catalog.getFunctionEntry(transaction, FTSIndexScanFunction::name);
  if (!entry ||
      entry->getType() != catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY) {
    return nullptr;
  }
  auto* function = function::BuiltInFunctionsUtils::matchFunction(
      FTSIndexScanFunction::name, {},
      entry->ptrCast<catalog::FunctionCatalogEntry>());
  return dynamic_cast<function::TableFunction*>(function);
}

}  // namespace neug::fts_ext
