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
#include <string>

#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/nested.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/graph/graph_entry.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

namespace {

struct ProjectGraphCallInput : public CallFuncInputBase {};

struct DropProjectedGraphCallInput : public CallFuncInputBase {};

struct ShowProjectedGraphsCallInput : public CallFuncInputBase {};

struct ProjectedGraphInfoCallInput : public CallFuncInputBase {
  ProjectedGraphInfoCallInput(const std::string& graphName) {
    this->graphName = graphName;
  }
  const std::string& getGraphName() const { return graphName; }

 private:
  std::string graphName;
};

static std::string getStringVal(const common::Value& value) {
  value.validateType(common::LogicalTypeID::STRING);
  return value.getValue<std::string>();
}

static std::vector<std::string> getListVal(const common::Value& value) {
  std::vector<std::string> vals;
  for (auto i = 0u; i < common::NestedVal::getChildrenSize(&value); ++i) {
    const auto& childValue = *common::NestedVal::getChildVal(&value, i);
    vals.push_back(getStringVal(childValue));
  }
  return vals;
}

static std::vector<graph::ParsedGraphEntryTableInfo>
extractGraphEntryTableInfos(const common::Value& value) {
  std::vector<graph::ParsedGraphEntryTableInfo> infos;
  switch (value.getDataType().getLogicalTypeID()) {
  case common::LogicalTypeID::LIST: {
    for (auto i = 0u; i < common::NestedVal::getChildrenSize(&value); ++i) {
      const auto& childValue = *common::NestedVal::getChildVal(&value, i);
      const auto& type = childValue.getDataType();
      switch (type.getLogicalTypeID()) {
      case common::LogicalTypeID::STRING: {
        auto tableName = getStringVal(childValue);
        infos.emplace_back(tableName, "" /* empty predicate */);
      } break;
      case common::LogicalTypeID::LIST: {
        auto triplets = getListVal(childValue);
        if (triplets.size() < 3) {
          THROW_BINDER_EXCEPTION(
              "Invalid triplet names, should be at least 3 elements, but is: " +
              triplets.size());
        }
        infos.emplace_back(triplets[0], triplets[1], triplets[2],
                           "" /* empty predicate */);
      } break;
      default: {
        THROW_BINDER_EXCEPTION(common::stringFormat(
            "Cannot extract graph entry from value {}, has data type {}. LIST "
            "or STRING was expected.",
            value.toString(), value.getDataType().toString()));
      }
      }
    }
  } break;
  case common::LogicalTypeID::STRUCT: {
    for (auto i = 0u; i < common::StructType::getNumFields(value.getDataType());
         ++i) {
      auto& field = common::StructType::getField(value.getDataType(), i);
      auto tableName = field.getName();
      auto predicate = getStringVal(*common::NestedVal::getChildVal(&value, i));
      infos.emplace_back(tableName, predicate);
    }
  } break;
  case common::LogicalTypeID::MAP: {
    for (auto i = 0u; i < common::NestedVal::getChildrenSize(&value); ++i) {
      const auto& childValue = *common::NestedVal::getChildVal(&value, i);
      const auto& childType = childValue.getDataType();
      if (childType.getLogicalTypeID() != common::LogicalTypeID::STRUCT) {
        THROW_BINDER_EXCEPTION(
            "Invalid map type, each map entry should be struct type, but is: " +
            childType.toString());
      }
      auto childFields = common::StructType::getNumFields(childType);
      if (childFields != 2) {
        THROW_BINDER_EXCEPTION(
            "Invalid map type, each map entry should have 2 fields, but is: " +
            childFields);
      }
      // value field for predicates
      auto predicate =
          getStringVal(*common::NestedVal::getChildVal(&childValue, 1));
      // key field for table names
      const auto& tableField = *common::NestedVal::getChildVal(&childValue, 0);
      const auto& tableType = tableField.getDataType();
      switch (tableType.getLogicalTypeID()) {
      case common::LogicalTypeID::STRING: {
        auto tableName = getStringVal(tableField);
        infos.emplace_back(tableName, predicate);
      } break;
      case common::LogicalTypeID::LIST: {
        auto triplets = getListVal(tableField);
        if (triplets.size() < 3) {
          THROW_BINDER_EXCEPTION(
              "Invalid triplet names, should be at least 3 elements, but is: " +
              triplets.size());
        }
        infos.emplace_back(triplets[0], triplets[1], triplets[2], predicate);
      } break;
      default: {
        THROW_BINDER_EXCEPTION(common::stringFormat(
            "Cannot extract graph entry from value {}, has data type {}. "
            "LIST or STRING was expected.",
            tableField.toString(), tableType.toString()));
      }
      }
    }
  } break;
  default:
    THROW_BINDER_EXCEPTION(common::stringFormat(
        "Argument {} has data type {}. LIST or STRUCT or MAP was expected.",
        value.toString(), value.getDataType().toString()));
  }
  return infos;
}

static std::unique_ptr<TableFuncBindData> makeEmptyBindData(
    const TableFuncBindInput* input) {
  binder::expression_vector cols;
  binder::expression_vector params;
  return std::make_unique<TableFuncBindData>(std::move(cols), 0, params);
}

static std::unique_ptr<TableFuncBindData> bindProjectGraph(
    main::ClientContext* clientContext, const TableFuncBindInput* input) {
  auto graphName = input->getLiteralVal<std::string>(0);
  auto nodeVal = input->getValue(1);
  auto relVal = input->getValue(2);
  graph::ParsedGraphEntry entry;
  entry.nodeInfos = extractGraphEntryTableInfos(nodeVal);
  entry.relInfos = extractGraphEntryTableInfos(relVal);
  auto metadataManager = clientContext->getMetadataManager();
  if (metadataManager == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Metadata manager is not set");
  }
  auto& graphEntrySet = metadataManager->getGraphEntrySetUnsafe();
  graphEntrySet.validateGraphNotExist(graphName);
  (void) graph::GDSFunction::bindGraphEntry(*clientContext, entry);
  graphEntrySet.addGraph(graphName, entry);
  return makeEmptyBindData(input);
}

static std::unique_ptr<TableFuncBindData> bindDropProjectedGraph(
    main::ClientContext* clientContext, const TableFuncBindInput* input) {
  auto graphName = input->getLiteralVal<std::string>(0);
  auto metadataManager = clientContext->getMetadataManager();
  if (metadataManager == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Metadata manager is not set");
  }
  auto& graphEntrySet = metadataManager->getGraphEntrySetUnsafe();
  graphEntrySet.validateGraphExist(graphName);
  graphEntrySet.dropGraph(graphName);
  return makeEmptyBindData(input);
}

}  // namespace

function_set ProjectGraphFunction::getFunctionSet() {
  auto func = std::make_unique<NeugCallFunction>(
      name, std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING,
                                               common::LogicalTypeID::ANY,
                                               common::LogicalTypeID::ANY});

  auto* tableFn = static_cast<TableFunction*>(func.get());
  tableFn->bindFunc = bindProjectGraph;

  func->bindFunc = [](const neug::Schema& /*schema*/,
                      const neug::execution::ContextMeta& /*ctx_meta*/,
                      const ::physical::PhysicalPlan& /*plan*/,
                      int /*op_idx*/) -> std::unique_ptr<CallFuncInputBase> {
    return std::make_unique<ProjectGraphCallInput>();
  };

  func->execFunc =
      [](const CallFuncInputBase& /*input*/, neug::IStorageInterface& /*graph*/,
         neug::execution::Context& /*ctx*/) { return execution::Context{}; };

  function_set functionSet;
  functionSet.push_back(std::move(func));
  return functionSet;
}

function_set DropProjectedGraphFunction::getFunctionSet() {
  auto func = std::make_unique<NeugCallFunction>(
      name, std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING});

  auto* tableFn = static_cast<TableFunction*>(func.get());
  tableFn->bindFunc = bindDropProjectedGraph;

  func->bindFunc = [](const neug::Schema& /*schema*/,
                      const neug::execution::ContextMeta& /*ctx_meta*/,
                      const ::physical::PhysicalPlan& /*plan*/,
                      int /*op_idx*/) -> std::unique_ptr<CallFuncInputBase> {
    return std::make_unique<DropProjectedGraphCallInput>();
  };

  func->execFunc =
      [](const CallFuncInputBase& /*input*/, neug::IStorageInterface& /*graph*/,
         neug::execution::Context& /*ctx*/) { return execution::Context{}; };

  function_set functionSet;
  functionSet.push_back(std::move(func));
  return functionSet;
}

function_set ShowProjectedGraphsFunction::getFunctionSet() {
  auto function = std::make_unique<NeugCallFunction>(
      ShowProjectedGraphsFunction::name,
      std::vector<neug::common::LogicalTypeID>{},
      std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
          {"name", neug::common::LogicalTypeID::STRING}});

  function->bindFunc = [](const neug::Schema& schema,
                          const neug::execution::ContextMeta& ctx_meta,
                          const ::physical::PhysicalPlan& plan,
                          int op_idx) -> std::unique_ptr<CallFuncInputBase> {
    return std::make_unique<ShowProjectedGraphsCallInput>();
  };

  function->execFunc = [](const CallFuncInputBase& /*input*/,
                          neug::IStorageInterface& /*graph*/,
                          neug::execution::Context& /*ctx*/) {
    neug::execution::Context out;
    neug::execution::ValueColumnBuilder<std::string> name_builder;
    auto metadataManager = main::MetadataRegistry::getMetadata();
    if (metadataManager == nullptr) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Metadata manager is not set");
    }
    auto& graphEntrySet = metadataManager->getGraphEntrySetUnsafe();
    auto& nameToEntryMap = graphEntrySet.getNameToEntryMap();
    name_builder.reserve(nameToEntryMap.size());
    for (const auto& [name, _] : nameToEntryMap) {
      name_builder.push_back_opt(name);
    }
    out.set(0, name_builder.finish());
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
      std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
      std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
          {"label", neug::common::LogicalTypeID::STRING},
          {"predicate", neug::common::LogicalTypeID::STRING}});

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
                          neug::IStorageInterface& /*graph*/,
                          neug::execution::Context& /*ctx*/) {
    neug::execution::Context out;
    neug::execution::ValueColumnBuilder<std::string> name_builder;
    neug::execution::ValueColumnBuilder<std::string> predicate_builder;
    auto metadataManager = main::MetadataRegistry::getMetadata();
    if (metadataManager == nullptr) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Metadata manager is not set");
    }
    auto& projectInput =
        dynamic_cast<const ProjectedGraphInfoCallInput&>(input);
    auto& graphEntrySet = metadataManager->getGraphEntrySetUnsafe();
    graphEntrySet.validateGraphExist(projectInput.getGraphName());
    auto& entry = graphEntrySet.getEntry(projectInput.getGraphName());
    size_t total_size = entry.nodeInfos.size() + entry.relInfos.size();
    name_builder.reserve(total_size);
    predicate_builder.reserve(total_size);
    for (const auto& nodeInfo : entry.nodeInfos) {
      name_builder.push_back_opt(nodeInfo.tableName);
      predicate_builder.push_back_opt(nodeInfo.predicate);
    }
    for (const auto& relInfo : entry.relInfos) {
      std::string triplets =
          common::stringFormat("[{},{},{}]", relInfo.srcTableName,
                               relInfo.tableName, relInfo.dstTableName);
      name_builder.push_back_opt(std::move(triplets));
      predicate_builder.push_back_opt(relInfo.predicate);
    }
    out.set(0, name_builder.finish());
    out.set(1, predicate_builder.finish());
    out.tag_ids = {0, 1};
    return out;
  };

  function_set functionSet;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace neug
