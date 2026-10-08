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

#include "neug/compiler/function/schema_introspection_function.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/common/columns/value_columns.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/default_value.h"

namespace neug::function {
namespace {

struct ShowTablesInput : public CallFuncInputBase {
  ShowTablesInput(bool hasFilter, std::vector<std::string> tables)
      : hasFilter(hasFilter), tables(std::move(tables)) {}

  bool hasFilter;
  std::vector<std::string> tables;
};

struct ShowTableInfoInput : public CallFuncInputBase {
  explicit ShowTableInfoInput(std::string table) : table(std::move(table)) {}

  std::string table;
};

struct PropertyInfoRow {
  size_t ordinal;
  std::string name;
  DataType type;
  Value defaultValue;
  bool primaryKey;
};

using EdgeTriplet = std::tuple<std::string, std::string, std::string>;

std::string Trim(std::string value) {
  auto trimmed = common::StringUtils::ltrim(value);
  trimmed = common::StringUtils::rtrim(trimmed);
  return std::string(trimmed);
}

EdgeTriplet ParseEdgeTriplet(const std::string& value) {
  auto text = Trim(value);
  if (text.size() < 2 || text.front() != '[' || text.back() != ']') {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid edge triplet '" + value +
                                     "'. Expected [src, edge, dst].");
  }
  text = text.substr(1, text.size() - 2);
  std::vector<std::string> parts;
  size_t start = 0;
  while (start <= text.size()) {
    const auto comma = text.find(',', start);
    parts.push_back(
        Trim(text.substr(start, comma == std::string::npos ? std::string::npos
                                                           : comma - start)));
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  if (parts.size() != 3 || parts[0].empty() || parts[1].empty() ||
      parts[2].empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid edge triplet '" + value +
                                     "'. Expected [src, edge, dst].");
  }
  return {std::move(parts[0]), std::move(parts[1]), std::move(parts[2])};
}

bool EdgeTripletMatches(const EdgeTriplet& filter, const EdgeSchema& edge) {
  const auto& [src, edgeLabel, dst] = filter;
  return (src == "*" || src == edge.src_label_name) &&
         edgeLabel == edge.edge_label_name &&
         (dst == "*" || dst == edge.dst_label_name);
}

std::string PrimaryKeyToString(const VertexSchema& vertex) {
  if (vertex.primary_keys.size() == 1) {
    return std::get<1>(vertex.primary_keys[0]);
  }
  std::string result = "[";
  for (size_t i = 0; i < vertex.primary_keys.size(); ++i) {
    if (i != 0) {
      result += ", ";
    }
    result += std::get<1>(vertex.primary_keys[i]);
  }
  return result + "]";
}

std::string EdgeOptionsToJson(const EdgeSchema& edge) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  if (edge.sort_key_for_nbr.has_value()) {
    writer.Key("sort_key_for_nbr");
    writer.String(
        edge.sort_key_for_nbr->data(),
        static_cast<rapidjson::SizeType>(edge.sort_key_for_nbr->size()));
  }
  if (!edge.oe_mutable) {
    writer.Key("oe_mutability");
    writer.String("IMMUTABLE");
  }
  if (!edge.ie_mutable) {
    writer.Key("ie_mutability");
    writer.String("IMMUTABLE");
  }
  writer.EndObject();
  return buffer.GetString();
}

std::string DefaultValueToString(const Value& value) {
  if (value.IsNull()) {
    THROW_INTERNAL_EXCEPTION(
        "Schema property default values must not be NULL.");
  }
  switch (value.type().id()) {
  case DataTypeId::kBoolean:
    return value.GetValue<bool>() ? "true" : "false";
  case DataTypeId::kVarchar:
    return value.GetValue<std::string>();
  case DataTypeId::kFloat: {
    std::ostringstream output;
    output << std::setprecision(std::numeric_limits<float>::max_digits10)
           << value.GetValue<float>();
    return output.str();
  }
  case DataTypeId::kDouble: {
    std::ostringstream output;
    output << std::setprecision(std::numeric_limits<double>::max_digits10)
           << value.GetValue<double>();
    return output.str();
  }
  case DataTypeId::kList: {
    std::string result = "[";
    const auto& children = ListValue::GetChildren(value);
    for (size_t i = 0; i < children.size(); ++i) {
      if (i != 0) {
        result += ", ";
      }
      result += DefaultValueToString(children[i]);
    }
    return result + "]";
  }
  case DataTypeId::kArray: {
    std::string result = "[";
    const auto& children = ArrayValue::GetChildren(value);
    for (size_t i = 0; i < children.size(); ++i) {
      if (i != 0) {
        result += ", ";
      }
      result += DefaultValueToString(children[i]);
    }
    return result + "]";
  }
  default:
    return value.to_string();
  }
}

std::vector<PropertyInfoRow> GetVertexPropertyRows(const VertexSchema& vertex) {
  std::vector<PropertyInfoRow> rows;
  rows.reserve(vertex.primary_keys.size() + vertex.property_names.size());
  for (const auto& [type, name, ordinal] : vertex.primary_keys) {
    rows.push_back(
        {ordinal, name, type, get_default_value(type), true /* primaryKey */});
  }
  for (size_t i = 0; i < vertex.property_names.size(); ++i) {
    if (i < vertex.vprop_soft_deleted.size() && vertex.vprop_soft_deleted[i]) {
      continue;
    }
    size_t ordinal = i;
    for (const auto& [_, __, primaryKeyOrdinal] : vertex.primary_keys) {
      if (ordinal >= primaryKeyOrdinal) {
        ++ordinal;
      }
    }
    rows.push_back({ordinal, vertex.property_names[i], vertex.property_types[i],
                    vertex.default_property_values[i], false});
  }
  std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.ordinal < rhs.ordinal;
  });
  return rows;
}

std::vector<PropertyInfoRow> GetEdgePropertyRows(const EdgeSchema& edge) {
  std::vector<PropertyInfoRow> rows;
  rows.reserve(edge.property_names.size());
  for (size_t i = 0; i < edge.property_names.size(); ++i) {
    if (i < edge.eprop_soft_deleted.size() && edge.eprop_soft_deleted[i]) {
      continue;
    }
    rows.push_back({i, edge.property_names[i], edge.properties[i],
                    edge.default_property_values[i], false});
  }
  return rows;
}

execution::Context BuildNodePropertyInfoContext(
    const std::vector<PropertyInfoRow>& rows) {
  ValueColumnBuilder<std::string> nameBuilder;
  ValueColumnBuilder<std::string> typeBuilder;
  ValueColumnBuilder<std::string> defaultBuilder;
  ValueColumnBuilder<bool> primaryKeyBuilder;
  nameBuilder.reserve(rows.size());
  typeBuilder.reserve(rows.size());
  defaultBuilder.reserve(rows.size());
  primaryKeyBuilder.reserve(rows.size());
  for (const auto& row : rows) {
    nameBuilder.push_back_opt(row.name);
    typeBuilder.push_back_opt(row.type.ToString());
    defaultBuilder.push_back_opt(DefaultValueToString(row.defaultValue));
    primaryKeyBuilder.push_back_opt(row.primaryKey);
  }

  execution::Context ctx;
  DataChunk chunk;
  chunk.set(0, nameBuilder.finish());
  chunk.set(1, typeBuilder.finish());
  chunk.set(2, defaultBuilder.finish());
  chunk.set(3, primaryKeyBuilder.finish());
  ctx.append_chunk(std::move(chunk));
  ctx.tag_ids = {0, 1, 2, 3};
  return ctx;
}

execution::Context BuildRelPropertyInfoContext(
    const std::vector<PropertyInfoRow>& rows) {
  ValueColumnBuilder<std::string> nameBuilder;
  ValueColumnBuilder<std::string> typeBuilder;
  ValueColumnBuilder<std::string> defaultBuilder;
  nameBuilder.reserve(rows.size());
  typeBuilder.reserve(rows.size());
  defaultBuilder.reserve(rows.size());
  for (const auto& row : rows) {
    nameBuilder.push_back_opt(row.name);
    typeBuilder.push_back_opt(row.type.ToString());
    defaultBuilder.push_back_opt(DefaultValueToString(row.defaultValue));
  }

  execution::Context ctx;
  DataChunk chunk;
  chunk.set(0, nameBuilder.finish());
  chunk.set(1, typeBuilder.finish());
  chunk.set(2, defaultBuilder.finish());
  ctx.append_chunk(std::move(chunk));
  ctx.tag_ids = {0, 1, 2};
  return ctx;
}

std::unique_ptr<CallFuncInputBase> BindShowTablesInput(
    const std::string& functionName, const ::physical::PhysicalPlan& plan,
    int opIdx) {
  const auto& arguments =
      plan.plan(opIdx).opr().procedure_call().query().arguments();
  std::vector<std::string> tables;
  if (arguments.empty()) {
    return std::make_unique<ShowTablesInput>(false, std::move(tables));
  }
  if (arguments.size() != 1 || !arguments[0].has_const_()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        functionName +
        " accepts no parameter, one string, or one list of strings");
  }
  const auto& value = arguments[0].const_();
  if (value.has_str()) {
    tables.push_back(value.str());
  } else if (value.has_str_array()) {
    tables.reserve(value.str_array().item_size());
    for (const auto& table : value.str_array().item()) {
      tables.push_back(table);
    }
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(functionName +
                                     " requires a string or list of strings");
  }
  return std::make_unique<ShowTablesInput>(true, std::move(tables));
}

}  // namespace

function_set ShowNodeTablesFunction::getFunctionSet() {
  const auto varchar = common::DataType(common::DataTypeId::kVarchar);
  const auto boolean = common::DataType(common::DataTypeId::kBoolean);
  const auto unknown = common::DataType(common::DataTypeId::kUnknown);
  const call_output_columns outputColumns{{"vertex_label_name", varchar},
                                          {"primary_key", varchar},
                                          {"temporary", boolean}};
  auto exec = [](const CallFuncInputBase& input, IStorageInterface& graph) {
    const auto& showTablesInput = dynamic_cast<const ShowTablesInput&>(input);
    const auto& filters = showTablesInput.tables;
    std::vector<std::shared_ptr<const VertexSchema>> vertices;
    const auto& schema = graph.schema();
    std::set<std::string> filterSet;
    for (const auto& filter : filters) {
      const auto label = Trim(filter);
      if (!schema.is_vertex_label_valid(label)) {
        THROW_INVALID_ARGUMENT_EXCEPTION("Node table '" + label +
                                         "' does not exist");
      }
      filterSet.insert(label);
    }
    for (const auto label : schema.get_vertex_label_ids()) {
      const auto vertex = schema.get_vertex_schema(label);
      if (!showTablesInput.hasFilter ||
          filterSet.contains(vertex->label_name)) {
        vertices.push_back(vertex);
      }
    }
    std::sort(vertices.begin(), vertices.end(),
              [](const auto& lhs, const auto& rhs) {
                return lhs->label_name < rhs->label_name;
              });

    ValueColumnBuilder<std::string> labelBuilder;
    ValueColumnBuilder<std::string> primaryKeyBuilder;
    ValueColumnBuilder<bool> temporaryBuilder;
    labelBuilder.reserve(vertices.size());
    primaryKeyBuilder.reserve(vertices.size());
    temporaryBuilder.reserve(vertices.size());
    for (const auto& vertex : vertices) {
      labelBuilder.push_back_opt(vertex->label_name);
      primaryKeyBuilder.push_back_opt(PrimaryKeyToString(*vertex));
      temporaryBuilder.push_back_opt(vertex->temporary);
    }

    execution::Context ctx;
    DataChunk chunk;
    chunk.set(0, labelBuilder.finish());
    chunk.set(1, primaryKeyBuilder.finish());
    chunk.set(2, temporaryBuilder.finish());
    ctx.append_chunk(std::move(chunk));
    ctx.tag_ids = {0, 1, 2};
    return ctx;
  };

  function_set functions;
  for (auto inputTypes : {call_input_types{}, call_input_types{unknown}}) {
    auto function = std::make_unique<NeugCallFunction>(
        name, std::move(inputTypes), outputColumns);
    function->bindFunc = [](const Schema&, const execution::ContextMeta&,
                            const ::physical::PhysicalPlan& plan, int opIdx) {
      return BindShowTablesInput("SHOW_NODE_TABLES", plan, opIdx);
    };
    function->execFunc = exec;
    functions.push_back(std::move(function));
  }
  return functions;
}

function_set ShowRelTablesFunction::getFunctionSet() {
  const auto varchar = common::DataType(common::DataTypeId::kVarchar);
  const auto boolean = common::DataType(common::DataTypeId::kBoolean);
  const auto unknown = common::DataType(common::DataTypeId::kUnknown);
  const call_output_columns outputColumns{
      {"edge_label_name", varchar}, {"src_label_name", varchar},
      {"dst_label_name", varchar},  {"multiplicity", varchar},
      {"temporary", boolean},       {"extra_options", varchar}};
  auto exec = [](const CallFuncInputBase& input, IStorageInterface& graph) {
    const auto& showTablesInput = dynamic_cast<const ShowTablesInput&>(input);
    const auto& filters = showTablesInput.tables;
    std::vector<EdgeTriplet> filterTriplets;
    const auto& schema = graph.schema();
    for (const auto& filter : filters) {
      auto triplet = ParseEdgeTriplet(filter);
      if (std::get<1>(triplet) == "*") {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "SHOW_REL_TABLES does not support wildcard '*' in the edge "
            "position");
      }
      filterTriplets.push_back(std::move(triplet));
    }
    std::vector<std::shared_ptr<const EdgeSchema>> validEdges;
    for (const auto& [_, edge] : schema.get_all_edge_schemas()) {
      if (edge &&
          schema.is_edge_triplet_valid(edge->src_label_id, edge->dst_label_id,
                                       edge->edge_label_id)) {
        validEdges.push_back(edge);
      }
    }
    for (const auto& filter : filterTriplets) {
      if (std::none_of(validEdges.begin(), validEdges.end(),
                       [&filter](const auto& edge) {
                         return EdgeTripletMatches(filter, *edge);
                       })) {
        const auto& [src, edge, dst] = filter;
        THROW_INVALID_ARGUMENT_EXCEPTION("Edge table [" + src + ", " + edge +
                                         ", " + dst + "] does not exist");
      }
    }
    std::vector<std::shared_ptr<const EdgeSchema>> edges;
    for (const auto& edge : validEdges) {
      if (!showTablesInput.hasFilter ||
          std::any_of(filterTriplets.begin(), filterTriplets.end(),
                      [&edge](const auto& filter) {
                        return EdgeTripletMatches(filter, *edge);
                      })) {
        edges.push_back(edge);
      }
    }
    std::sort(edges.begin(), edges.end(), [](const auto& lhs, const auto& rhs) {
      return std::tie(lhs->edge_label_name, lhs->src_label_name,
                      lhs->dst_label_name) < std::tie(rhs->edge_label_name,
                                                      rhs->src_label_name,
                                                      rhs->dst_label_name);
    });

    ValueColumnBuilder<std::string> edgeBuilder;
    ValueColumnBuilder<std::string> srcBuilder;
    ValueColumnBuilder<std::string> dstBuilder;
    ValueColumnBuilder<std::string> multiplicityBuilder;
    ValueColumnBuilder<bool> temporaryBuilder;
    ValueColumnBuilder<std::string> optionsBuilder;
    edgeBuilder.reserve(edges.size());
    srcBuilder.reserve(edges.size());
    dstBuilder.reserve(edges.size());
    multiplicityBuilder.reserve(edges.size());
    temporaryBuilder.reserve(edges.size());
    optionsBuilder.reserve(edges.size());
    for (const auto& edge : edges) {
      edgeBuilder.push_back_opt(edge->edge_label_name);
      srcBuilder.push_back_opt(edge->src_label_name);
      dstBuilder.push_back_opt(edge->dst_label_name);
      multiplicityBuilder.push_back_opt(schema.get_edge_strategy(
          edge->src_label_id, edge->dst_label_id, edge->edge_label_id));
      temporaryBuilder.push_back_opt(edge->temporary);
      optionsBuilder.push_back_opt(EdgeOptionsToJson(*edge));
    }

    execution::Context ctx;
    DataChunk chunk;
    chunk.set(0, edgeBuilder.finish());
    chunk.set(1, srcBuilder.finish());
    chunk.set(2, dstBuilder.finish());
    chunk.set(3, multiplicityBuilder.finish());
    chunk.set(4, temporaryBuilder.finish());
    chunk.set(5, optionsBuilder.finish());
    ctx.append_chunk(std::move(chunk));
    ctx.tag_ids = {0, 1, 2, 3, 4, 5};
    return ctx;
  };

  function_set functions;
  for (auto inputTypes : {call_input_types{}, call_input_types{unknown}}) {
    auto function = std::make_unique<NeugCallFunction>(
        name, std::move(inputTypes), outputColumns);
    function->bindFunc = [](const Schema&, const execution::ContextMeta&,
                            const ::physical::PhysicalPlan& plan, int opIdx) {
      return BindShowTablesInput("SHOW_REL_TABLES", plan, opIdx);
    };
    function->execFunc = exec;
    functions.push_back(std::move(function));
  }
  return functions;
}

function_set ShowNodeTableInfoFunction::getFunctionSet() {
  const auto varchar = common::DataType(common::DataTypeId::kVarchar);
  const auto boolean = common::DataType(common::DataTypeId::kBoolean);
  auto function = std::make_unique<NeugCallFunction>(
      name, call_input_types{varchar},
      call_output_columns{{"property_name", varchar},
                          {"property_type", varchar},
                          {"default_value", varchar},
                          {"primary_key", boolean}});
  function->bindFunc = [](const Schema&, const execution::ContextMeta&,
                          const ::physical::PhysicalPlan& plan,
                          int opIdx) -> std::unique_ptr<CallFuncInputBase> {
    const auto& arguments =
        plan.plan(opIdx).opr().procedure_call().query().arguments();
    if (arguments.size() != 1 || !arguments[0].has_const_() ||
        !arguments[0].const_().has_str()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "SHOW_NODE_TABLE_INFO requires one string constant parameter");
    }
    return std::make_unique<ShowTableInfoInput>(arguments[0].const_().str());
  };
  function->execFunc = [](const CallFuncInputBase& input,
                          IStorageInterface& graph) {
    const auto& tableInput = dynamic_cast<const ShowTableInfoInput&>(input);
    const auto table = Trim(tableInput.table);
    const auto& schema = graph.schema();
    if (!schema.is_vertex_label_valid(table)) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Node table '" + table +
                                       "' does not exist");
    }
    return BuildNodePropertyInfoContext(GetVertexPropertyRows(
        *schema.get_vertex_schema(schema.get_vertex_label_id(table))));
  };

  function_set functions;
  functions.push_back(std::move(function));
  return functions;
}

function_set ShowRelTableInfoFunction::getFunctionSet() {
  const auto varchar = common::DataType(common::DataTypeId::kVarchar);
  auto function = std::make_unique<NeugCallFunction>(
      name, call_input_types{varchar},
      call_output_columns{{"property_name", varchar},
                          {"property_type", varchar},
                          {"default_value", varchar}});
  function->bindFunc = [](const Schema&, const execution::ContextMeta&,
                          const ::physical::PhysicalPlan& plan,
                          int opIdx) -> std::unique_ptr<CallFuncInputBase> {
    const auto& arguments =
        plan.plan(opIdx).opr().procedure_call().query().arguments();
    if (arguments.size() != 1 || !arguments[0].has_const_() ||
        !arguments[0].const_().has_str()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "SHOW_REL_TABLE_INFO requires one string constant parameter");
    }
    return std::make_unique<ShowTableInfoInput>(arguments[0].const_().str());
  };
  function->execFunc = [](const CallFuncInputBase& input,
                          IStorageInterface& graph) {
    const auto& tableInput = dynamic_cast<const ShowTableInfoInput&>(input);
    const auto [src, edge, dst] = ParseEdgeTriplet(tableInput.table);
    if (src == "*" || edge == "*" || dst == "*") {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "SHOW_REL_TABLE_INFO does not support wildcard '*'");
    }
    const auto& schema = graph.schema();
    if (!schema.is_edge_triplet_valid(src, dst, edge)) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Edge table [" + src + ", " + edge +
                                       ", " + dst + "] does not exist");
    }
    const auto edgeSchema = schema.get_edge_schema(
        schema.get_vertex_label_id(src), schema.get_vertex_label_id(dst),
        schema.get_edge_label_id(edge));
    return BuildRelPropertyInfoContext(GetEdgePropertyRows(*edgeSchema));
  };

  function_set functions;
  functions.push_back(std::move(function));
  return functions;
}

}  // namespace neug::function
