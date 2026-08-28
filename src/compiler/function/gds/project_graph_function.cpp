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

#include "neug/compiler/function/gds/project_graph_function.h"
#include <algorithm>
#include <string>

#include <yaml-cpp/yaml.h>
#include "neug/common/columns/value_columns.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/parameter_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/nested.h"
#include "neug/compiler/function/gds/gds_graph.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

namespace {

struct ProjectGraphCallInput : public CallFuncInputBase {
  ProjectGraphCallInput(std::string graphName, ProjectedGraphEntry entry)
      : graphName(std::move(graphName)), entry(std::move(entry)) {}
  std::string graphName;
  ProjectedGraphEntry entry;
};

struct DropProjectedGraphCallInput : public CallFuncInputBase {
  explicit DropProjectedGraphCallInput(std::string graphName)
      : graphName(std::move(graphName)) {}
  std::string graphName;
};

struct ShowProjectedGraphsCallInput : public CallFuncInputBase {};

struct ProjectedGraphInfoCallInput : public CallFuncInputBase {
  ProjectedGraphInfoCallInput(const std::string& graphName) {
    this->graphName = graphName;
  }
  const std::string& getGraphName() const { return graphName; }

 private:
  std::string graphName;
};

static std::string getStringVal(const compiler_impl::Value& value) {
  value.validateType(common::DataTypeId::kVarchar);
  return value.getValue<std::string>();
}

static std::vector<std::string> getListVal(const compiler_impl::Value& value) {
  std::vector<std::string> vals;
  for (auto i = 0u; i < common::NestedVal::getChildrenSize(&value); ++i) {
    const auto& childValue = *common::NestedVal::getChildVal(&value, i);
    vals.push_back(getStringVal(childValue));
  }
  return vals;
}

static std::vector<std::string> parseEdgeTriplet(const std::string& value) {
  auto text = common::StringUtils::rtrim(common::StringUtils::ltrim(value));
  if (text.size() < 2 || text.front() != '[' || text.back() != ']') {
    THROW_BINDER_EXCEPTION("Invalid edge triplet '" + value + "'.");
  }
  text = text.substr(1, text.size() - 2);
  std::vector<std::string> result;
  size_t start = 0;
  while (start <= text.size()) {
    auto comma = text.find(',', start);
    auto part = text.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    part = std::string(
        common::StringUtils::rtrim(common::StringUtils::ltrim(part)));
    result.push_back(std::move(part));
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  if (result.size() != 3 || result[0].empty() || result[1].empty() ||
      result[2].empty()) {
    THROW_BINDER_EXCEPTION("Invalid edge triplet '" + value + "'.");
  }
  return result;
}

static void extractGraphEntryTableInfos(const compiler_impl::Value& value,
                                        bool relationships,
                                        ProjectedGraphEntry& entry) {
  auto addInfo = [&](const std::string& name, const std::string& predicate) {
    if (relationships) {
      auto triplet = parseEdgeTriplet(name);
      entry.edgeInfos.push_back(
          {triplet[0], triplet[1], triplet[2], predicate});
    } else {
      entry.vertexInfos.push_back({name, predicate});
    }
  };
  auto addTriplet = [&](const std::vector<std::string>& triplet,
                        const std::string& predicate) {
    if (!relationships) {
      THROW_BINDER_EXCEPTION("A vertex projection must use a label name.");
    }
    if (triplet.size() != 3) {
      THROW_BINDER_EXCEPTION(common::stringFormat(
          "Invalid edge triplet, must have exactly 3 elements [src, edge, "
          "dst], but got: {}",
          triplet.size()));
    }
    entry.edgeInfos.push_back({triplet[0], triplet[1], triplet[2], predicate});
  };
  switch (value.getDataType().id()) {
  case common::DataTypeId::kArray:
  case common::DataTypeId::kList: {
    for (auto i = 0u; i < common::NestedVal::getChildrenSize(&value); ++i) {
      const auto& childValue = *common::NestedVal::getChildVal(&value, i);
      const auto& type = childValue.getDataType();
      switch (type.id()) {
      case common::DataTypeId::kVarchar: {
        auto tableName = getStringVal(childValue);
        addInfo(tableName, "" /* empty predicate */);
      } break;
      case common::DataTypeId::kArray:
      case common::DataTypeId::kList: {
        auto triplets = getListVal(childValue);
        addTriplet(triplets, "" /* empty predicate */);
      } break;
      default: {
        THROW_BINDER_EXCEPTION(common::stringFormat(
            "Cannot extract graph entry from value {}, has data type {}. LIST "
            "or STRING was expected.",
            value.toString(), value.getDataType().ToString()));
      }
      }
    }
  } break;
  case common::DataTypeId::kStruct: {
    for (auto i = 0u; i < common::StructType::GetNumFields(value.getDataType());
         ++i) {
      auto tableName = common::StructType::GetChildName(value.getDataType(), i);
      auto predicate = getStringVal(*common::NestedVal::getChildVal(&value, i));
      addInfo(tableName, predicate);
    }
  } break;
  case common::DataTypeId::kMap: {
    for (auto i = 0u; i < common::NestedVal::getChildrenSize(&value); ++i) {
      const auto& childValue = *common::NestedVal::getChildVal(&value, i);
      const auto& childType = childValue.getDataType();
      if (childType.id() != common::DataTypeId::kStruct) {
        THROW_BINDER_EXCEPTION(common::stringFormat(
            "Invalid map type, each map entry should be struct type, but is: "
            "{}",
            childType.ToString()));
      }
      auto childFields = common::StructType::GetNumFields(childType);
      if (childFields != 2) {
        THROW_BINDER_EXCEPTION(common::stringFormat(
            "Invalid map type, each map entry should have 2 fields, but is: "
            "{}",
            childFields));
      }
      // value field for predicates
      auto predicate =
          getStringVal(*common::NestedVal::getChildVal(&childValue, 1));
      // key field for table names
      const auto& tableField = *common::NestedVal::getChildVal(&childValue, 0);
      const auto& tableType = tableField.getDataType();
      switch (tableType.id()) {
      case common::DataTypeId::kVarchar: {
        auto tableName = getStringVal(tableField);
        addInfo(tableName, predicate);
      } break;
      case common::DataTypeId::kArray:
      case common::DataTypeId::kList: {
        auto triplets = getListVal(tableField);
        addTriplet(triplets, predicate);
      } break;
      default: {
        THROW_BINDER_EXCEPTION(common::stringFormat(
            "Cannot extract graph entry from value {}, has data type {}. "
            "LIST or STRING was expected.",
            tableField.toString(), tableType.ToString()));
      }
      }
    }
  } break;
  default:
    THROW_BINDER_EXCEPTION(common::stringFormat(
        "Argument {} has data type {}. LIST, ARRAY, STRUCT or MAP was "
        "expected.",
        value.toString(), value.getDataType().ToString()));
  }
}

static std::unique_ptr<TableFuncBindData> makeEmptyBindData(
    const TableFuncBindInput* input) {
  binder::expression_vector cols;
  binder::expression_vector params;
  return std::make_unique<TableFuncBindData>(std::move(cols), 0, params);
}

static std::string serializeProjectedGraph(const ProjectedGraphEntry& entry) {
  auto yaml = entry.ToYaml();
  if (!yaml) {
    THROW_BINDER_EXCEPTION(yaml.error().ToString());
  }
  return YAML::Dump(yaml.value());
}

static void validatePredicateExpression(
    const std::shared_ptr<binder::Expression>& expression) {
  if (dynamic_cast<const binder::LiteralExpression*>(expression.get()) ||
      dynamic_cast<const binder::ParameterExpression*>(expression.get()) ||
      dynamic_cast<const binder::PropertyExpression*>(expression.get())) {
    return;
  }
  if (dynamic_cast<const binder::ScalarFunctionExpression*>(expression.get())) {
    for (const auto& child : expression->getChildren()) {
      validatePredicateExpression(child);
    }
    return;
  }
  THROW_BINDER_EXCEPTION(common::stringFormat(
      "Unsupported projected graph predicate '{}'. Predicates may only use "
      "properties, literals, parameters, casts, comparisons, string or list "
      "predicates, and logical combinations of those expressions.",
      expression->toString()));
}

static void validatePredicate(
    const std::shared_ptr<binder::Expression>& predicate) {
  if (!predicate) {
    return;
  }
  if (predicate->getDataType().id() != common::DataTypeId::kBoolean) {
    THROW_BINDER_EXCEPTION(common::stringFormat(
        "Projected graph predicate '{}' must return BOOL, but returns {}.",
        predicate->toString(), predicate->getDataType().ToString()));
  }
  validatePredicateExpression(predicate);
}

static void validatePredicates(const graph::GraphEntry& entry) {
  for (const auto& info : entry.nodeInfos) {
    validatePredicate(info.predicate);
  }
  for (const auto& info : entry.relInfos) {
    validatePredicate(info.predicate);
  }
}

static std::unique_ptr<TableFuncBindData> bindProjectGraph(
    main::ClientContext* clientContext, const TableFuncBindInput* input) {
  auto graphName = input->getLiteralVal<std::string>(0);
  auto nodeVal = input->getValue(1);
  auto relVal = input->getValue(2);
  ProjectedGraphEntry entry;
  extractGraphEntryTableInfos(nodeVal, false, entry);
  extractGraphEntryTableInfos(relVal, true, entry);
  auto boundEntry = graph::GDSFunction::bindGraphEntry(*clientContext, entry);
  validatePredicates(boundEntry);
  binder::expression_vector params;
  params.push_back(std::make_shared<binder::LiteralExpression>(
      compiler_impl::Value(graphName), ""));
  params.push_back(std::make_shared<binder::LiteralExpression>(
      compiler_impl::Value(serializeProjectedGraph(entry)), ""));
  binder::expression_vector cols;
  return std::make_unique<TableFuncBindData>(std::move(cols), 0,
                                             std::move(params));
}

static std::unique_ptr<TableFuncBindData> bindDropProjectedGraph(
    main::ClientContext* clientContext, const TableFuncBindInput* input) {
  auto graphName = input->getLiteralVal<std::string>(0);
  binder::expression_vector params;
  params.push_back(std::make_shared<binder::LiteralExpression>(
      compiler_impl::Value(graphName), ""));
  binder::expression_vector cols;
  return std::make_unique<TableFuncBindData>(std::move(cols), 0,
                                             std::move(params));
}

}  // namespace

function_set ProjectGraphFunction::getFunctionSet() {
  auto func = std::make_unique<NeugCallFunction>(
      name, function::call_input_types{
                common::DataType(common::DataTypeId::kVarchar),
                common::DataType(common::DataTypeId::kUnknown),
                common::DataType(common::DataTypeId::kUnknown)});
  // Mutates projected-graph metadata; must not run on the read path.
  func->isReadOnly = false;

  auto* tableFn = static_cast<TableFunction*>(func.get());
  tableFn->bindFunc = bindProjectGraph;

  func->bindFunc = [](const neug::Schema& /*schema*/,
                      const neug::execution::ContextMeta& /*ctx_meta*/,
                      const ::physical::PhysicalPlan& plan,
                      int op_idx) -> std::unique_ptr<CallFuncInputBase> {
    const auto& args =
        plan.plan(op_idx).opr().procedure_call().query().arguments();
    if (args.size() != 2 || !args[0].has_const_() ||
        !args[0].const_().has_str() || !args[1].has_const_() ||
        !args[1].const_().has_str()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Invalid project_graph physical input");
    }
    auto graphName = args[0].const_().str();
    YAML::Node yaml;
    try {
      yaml = YAML::Load(args[1].const_().str());
    } catch (const YAML::Exception& e) {
      THROW_INVALID_ARGUMENT_EXCEPTION(common::stringFormat(
          "Invalid project_graph physical input: malformed YAML: {}",
          e.what()));
    }
    auto entry = ProjectedGraphEntry::FromYaml(yaml);
    if (!entry) {
      THROW_INVALID_ARGUMENT_EXCEPTION(entry.error().ToString());
    }
    return std::make_unique<ProjectGraphCallInput>(graphName,
                                                   std::move(entry.value()));
  };

  func->execFunc = [](const CallFuncInputBase& input,
                      neug::IStorageInterface& graph) {
    auto* update = dynamic_cast<StorageUpdateInterface*>(&graph);
    if (update == nullptr) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "project_graph requires a writable storage interface");
    }
    const auto& project = dynamic_cast<const ProjectGraphCallInput&>(input);
    auto status = update->AddGraphEntry(project.graphName, project.entry);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR(status.ToString());
    }
    return execution::Context{};
  };

  function_set functionSet;
  functionSet.push_back(std::move(func));
  return functionSet;
}

function_set DropProjectedGraphFunction::getFunctionSet() {
  auto func = std::make_unique<NeugCallFunction>(
      name, function::call_input_types{
                common::DataType(common::DataTypeId::kVarchar)});
  // Mutates projected-graph metadata; must not run on the read path.
  func->isReadOnly = false;

  auto* tableFn = static_cast<TableFunction*>(func.get());
  tableFn->bindFunc = bindDropProjectedGraph;

  func->bindFunc = [](const neug::Schema& /*schema*/,
                      const neug::execution::ContextMeta& /*ctx_meta*/,
                      const ::physical::PhysicalPlan& plan,
                      int op_idx) -> std::unique_ptr<CallFuncInputBase> {
    const auto& args =
        plan.plan(op_idx).opr().procedure_call().query().arguments();
    if (args.size() != 1 || !args[0].has_const_() ||
        !args[0].const_().has_str()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Invalid drop_projected_graph physical input");
    }
    return std::make_unique<DropProjectedGraphCallInput>(
        args[0].const_().str());
  };

  func->execFunc = [](const CallFuncInputBase& input,
                      neug::IStorageInterface& graph) {
    auto* update = dynamic_cast<StorageUpdateInterface*>(&graph);
    if (update == nullptr) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "drop_projected_graph requires a writable storage interface");
    }
    const auto& drop = dynamic_cast<const DropProjectedGraphCallInput&>(input);
    auto status = update->DropGraphEntry(drop.graphName);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR(status.ToString());
    }
    return execution::Context{};
  };

  function_set functionSet;
  functionSet.push_back(std::move(func));
  return functionSet;
}

function_set ShowProjectedGraphsFunction::getFunctionSet() {
  auto function = std::make_unique<NeugCallFunction>(
      ShowProjectedGraphsFunction::name, function::call_input_types{},
      std::vector<std::pair<std::string, neug::common::DataType>>{
          {"name",
           neug::common::DataType(neug::common::DataTypeId::kVarchar)}});

  function->bindFunc = [](const neug::Schema& schema,
                          const neug::execution::ContextMeta& ctx_meta,
                          const ::physical::PhysicalPlan& plan,
                          int op_idx) -> std::unique_ptr<CallFuncInputBase> {
    return std::make_unique<ShowProjectedGraphsCallInput>();
  };

  function->execFunc = [](const CallFuncInputBase& /*input*/,
                          neug::IStorageInterface& graph) {
    neug::execution::Context out;
    neug::ValueColumnBuilder<std::string> name_builder;
    auto names = graph.schema().GetGraphEntryNames();
    name_builder.reserve(names.size());
    for (const auto& name : names) {
      name_builder.push_back_opt(name);
    }
    neug::DataChunk chunk;
    chunk.set(0, name_builder.finish());
    out.append_chunk(std::move(chunk));
    out.tag_ids = {0};
    return out;
  };

  function_set functionSet;
  functionSet.push_back(std::move(function));
  return functionSet;
}

function_set ProjectedGraphInfoFunction::getFunctionSet() {
  auto function = std::make_unique<NeugCallFunction>(
      ProjectedGraphInfoFunction::name,
      function::call_input_types{
          common::DataType(common::DataTypeId::kVarchar)},
      std::vector<std::pair<std::string, neug::common::DataType>>{
          {"label", neug::common::DataType(neug::common::DataTypeId::kVarchar)},
          {"predicate",
           neug::common::DataType(neug::common::DataTypeId::kVarchar)}});

  function->bindFunc = [](const neug::Schema& schema,
                          const neug::execution::ContextMeta& ctx_meta,
                          const ::physical::PhysicalPlan& plan,
                          int op_idx) -> std::unique_ptr<CallFuncInputBase> {
    auto& procedure_call = plan.plan(op_idx).opr().procedure_call();
    auto& query = procedure_call.query();
    auto& params = query.arguments();
    if (params.size() < 1) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Projected graph info function requires 1 parameter");
    }
    auto& param = params[0];
    if (!param.has_const_() || !param.const_().has_str()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Projected graph info function requires a string constant parameter");
    }
    return std::make_unique<ProjectedGraphInfoCallInput>(param.const_().str());
  };

  function->execFunc = [](const CallFuncInputBase& input,
                          neug::IStorageInterface& graph) {
    neug::execution::Context out;
    neug::ValueColumnBuilder<std::string> name_builder;
    neug::ValueColumnBuilder<std::string> predicate_builder;
    auto& projectInput =
        dynamic_cast<const ProjectedGraphInfoCallInput&>(input);
    auto entryResult =
        graph.schema().GetGraphEntry(projectInput.getGraphName());
    if (!entryResult) {
      THROW_INVALID_ARGUMENT_EXCEPTION(entryResult.error().error_message());
    }
    const auto& entry = **entryResult;
    size_t total_size = entry.vertexInfos.size() + entry.edgeInfos.size();
    name_builder.reserve(total_size);
    predicate_builder.reserve(total_size);
    auto vertices = entry.vertexInfos;
    auto edges = entry.edgeInfos;
    std::sort(vertices.begin(), vertices.end(),
              [](const auto& lhs, const auto& rhs) {
                return lhs.labelName < rhs.labelName;
              });
    std::sort(edges.begin(), edges.end(), [](const auto& lhs, const auto& rhs) {
      return std::tie(lhs.srcLabelName, lhs.edgeLabelName, lhs.dstLabelName) <
             std::tie(rhs.srcLabelName, rhs.edgeLabelName, rhs.dstLabelName);
    });
    for (const auto& nodeInfo : vertices) {
      name_builder.push_back_opt(nodeInfo.labelName);
      predicate_builder.push_back_opt(nodeInfo.predicate);
    }
    for (const auto& relInfo : edges) {
      std::string triplets =
          common::stringFormat("[{},{},{}]", relInfo.srcLabelName,
                               relInfo.edgeLabelName, relInfo.dstLabelName);
      name_builder.push_back_opt(std::move(triplets));
      predicate_builder.push_back_opt(relInfo.predicate);
    }
    neug::DataChunk chunk;
    chunk.set(0, name_builder.finish());
    chunk.set(1, predicate_builder.finish());
    out.append_chunk(std::move(chunk));
    out.tag_ids = {0, 1};
    return out;
  };

  function_set functionSet;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace neug
