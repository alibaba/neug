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

#include "neug/compiler/function/gds/gds_algo_function.h"

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/path_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/nested.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/compiler/graph/graph_entry.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

namespace {

static std::string extractOptionValue(const common::Value& val) {
  if (val.isNull()) {
    return "";
  }
  switch (val.getDataType().id()) {
  case common::DataTypeId::kVarchar:
    return val.getValue<std::string>();
  case common::DataTypeId::kBoolean:
    return val.getValue<bool>() ? "true" : "false";
  case common::DataTypeId::kInt64:
    return std::to_string(val.getValue<int64_t>());
  case common::DataTypeId::kInt32:
    return std::to_string(val.getValue<int32_t>());
  case common::DataTypeId::kDouble:
    return std::to_string(val.getValue<double>());
  case common::DataTypeId::kFloat:
    return std::to_string(val.getValue<float>());
  case common::DataTypeId::kUInt64:
    return std::to_string(val.getValue<uint64_t>());
  case common::DataTypeId::kUInt32:
    return std::to_string(val.getValue<uint32_t>());
  default:
    THROW_BINDER_EXCEPTION("Unsupported option value type: " +
                           val.getDataType().ToString());
  }
}

static common::case_insensitive_map_t<std::string> extractStringOptions(
    const common::Value& value) {
  common::case_insensitive_map_t<std::string> out;
  const auto typeId = value.getDataType().id();
  if (typeId == common::DataTypeId::kStruct) {
    for (auto i = 0u; i < common::StructType::GetNumFields(value.getDataType());
         ++i) {
      auto& fieldName =
          common::StructType::GetChildName(value.getDataType(), i);
      const auto* child = common::NestedVal::getChildVal(&value, i);
      out.emplace(fieldName, extractOptionValue(*child));
    }
    return out;
  }
  if (typeId == common::DataTypeId::kMap) {
    for (auto i = 0u; i < common::NestedVal::getChildrenSize(&value); ++i) {
      const auto& entry = *common::NestedVal::getChildVal(&value, i);
      const auto& entryType = entry.getDataType();
      if (entryType.id() != common::DataTypeId::kStruct) {
        THROW_BINDER_EXCEPTION(
            "GDS options map entries must be structs (key/value pairs).");
      }
      if (common::StructType::GetNumFields(entryType) != 2) {
        THROW_BINDER_EXCEPTION(
            "GDS options map entry must have exactly two fields.");
      }
      const auto& keyVal = *common::NestedVal::getChildVal(&entry, 0);
      const auto& valVal = *common::NestedVal::getChildVal(&entry, 1);
      keyVal.validateType(common::DataTypeId::kVarchar);
      out.emplace(keyVal.getValue<std::string>(), extractOptionValue(valVal));
    }
    return out;
  }
  THROW_BINDER_EXCEPTION(
      "Second argument to GDS CALL must be a map literal or struct literal, "
      "got " +
      value.getDataType().ToString() + ".");
}

static std::shared_ptr<binder::Expression> bindGDSOutputColumn(
    const TableFuncBindInput& input, binder::Binder& binder,
    const graph::GraphEntry& graphEntry, const std::string& name,
    common::DataTypeId typeID) {
  switch (typeID) {
  case common::DataTypeId::kVertex:
    return graph::GDSFunction::bindNodeOutput(
        input, graphEntry.getNodeEntries(), name);
  case common::DataTypeId::kEdge: {
    auto srcNode =
        binder.createQueryNode(name + "_src", graphEntry.getNodeEntries());
    auto dstNode =
        binder.createQueryNode(name + "_dst", graphEntry.getNodeEntries());
    return graph::GDSFunction::bindRelOutput(input, graphEntry.getRelEntries(),
                                             srcNode, dstNode, name);
  }
  case common::DataTypeId::kPath: {
    auto srcNode =
        binder.createQueryNode(name + "_src", graphEntry.getNodeEntries());
    auto dstNode =
        binder.createQueryNode(name + "_dst", graphEntry.getNodeEntries());

    auto rel = binder.createNonRecursiveQueryRel(
        name + "_rel", graphEntry.getRelEntries(), srcNode, dstNode,
        binder::RelDirectionType::SINGLE);
    auto nodeType = srcNode->getDataType();
    auto relType = rel->getDataType();
    binder::expression_vector children;
    auto pathExpr = std::make_shared<binder::PathExpression>(
        binder.getRecursiveRelLogicalType(nodeType, relType), name, name,
        std::move(nodeType), std::move(relType), children);
    binder.addToScope(name, pathExpr);
    return pathExpr;
  }
  default:
    return binder.createVariable(name, typeID);
  }
}

}  // namespace

std::unique_ptr<TableFuncBindData> bindGDSFunction(
    main::ClientContext* clientContext, const TableFuncBindInput* input,
    call_output_columns outputColumns) {
  if (input->binder == nullptr) {
    THROW_BINDER_EXCEPTION("Internal error: binder not set for GDS CALL.");
  }
  auto& binder = *input->binder;
  auto graphName = input->getLiteralVal<std::string>(0);
  auto metadataManager = clientContext->getMetadataManager();
  if (metadataManager == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Metadata manager is not set");
  }
  auto& graphEntrySet = metadataManager->getGraphEntrySetUnsafe();
  graphEntrySet.validateGraphExist(graphName);
  const auto& parsed = graphEntrySet.getEntry(graphName);
  auto graphEntry = graph::GDSFunction::bindGraphEntry(*clientContext, parsed);
  auto options = extractStringOptions(input->getValue(1));
  binder::expression_vector columns;
  const auto& yieldVariables = input->yieldVariables;
  if (!yieldVariables.empty()) {
    for (const auto& var : yieldVariables) {
      auto column = std::find_if(
          outputColumns.begin(), outputColumns.end(),
          [&var](const auto& col) { return col.first == var.name; });
      if (column != outputColumns.end()) {
        std::string alias = column->first;
        if (var.hasAlias()) {
          alias = var.alias;
        }
        auto columnExpr = bindGDSOutputColumn(*input, binder, graphEntry, alias,
                                              column->second);
        columns.push_back(std::move(columnExpr));
      } else {
        THROW_BINDER_EXCEPTION("Output variable " + var.name +
                               " not found in output columns.");
      }
    }
  } else {
    for (auto& outputColumn : outputColumns) {
      // add ouput columns to scope if exists
      auto columnExpr = bindGDSOutputColumn(
          *input, binder, graphEntry, outputColumn.first, outputColumn.second);
      columns.push_back(std::move(columnExpr));
    }
  }
  return std::make_unique<GDSFuncBindData>(std::move(columns), 0, input->params,
                                           std::move(graphEntry),
                                           std::move(options));
}

GDSAlgoFunction::GDSAlgoFunction(std::string name,
                                 std::vector<common::DataTypeId> inputTypes,
                                 call_output_columns outputColumns)
    : NeugCallFunction(std::move(name), std::move(inputTypes),
                       std::move(outputColumns)) {
  auto* tableFn = static_cast<TableFunction*>(this);
  tableFn->bindFunc = [this](main::ClientContext* clientContext,
                             const TableFuncBindInput* input)
      -> std::unique_ptr<TableFuncBindData> {
    return bindGDSFunction(clientContext, input, this->outputColumns);
  };
}

GDSFuncBindData::GDSFuncBindData(
    binder::expression_vector columns, common::row_idx_t numRows,
    binder::expression_vector params, graph::GraphEntry graphEntryIn,
    common::case_insensitive_map_t<std::string> optionsIn)
    : TableFuncBindData(std::move(columns), numRows, std::move(params)),
      graphEntry(std::move(graphEntryIn)),
      options(std::move(optionsIn)) {}

GDSFuncBindData::GDSFuncBindData(const GDSFuncBindData& other)
    : TableFuncBindData(other),
      graphEntry(other.graphEntry.copy()),
      options(other.options) {}

std::unique_ptr<TableFuncBindData> GDSFuncBindData::copy() const {
  return std::make_unique<GDSFuncBindData>(*this);
}

}  // namespace function
}  // namespace neug
