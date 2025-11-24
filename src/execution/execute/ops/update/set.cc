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
#include "neug/execution/execute/ops/update/set.h"

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/types.h"
#include "neug/execution/utils/expr.h"
#include "neug/execution/utils/var.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"
#include "neug/utils/runtime/rt_any.h"

namespace gs {
namespace runtime {
class OprTimer;

namespace ops {

class SetOpr : public IOperator {
 public:
  SetOpr(std::vector<std::pair<int, std::string>> keys,
         std::vector<common::Expression> values)
      : keys_(std::move(keys)), values_(std::move(values)) {}

  std::string get_operator_name() const override { return "SetOpr"; }

  // compiler bug here
  bool _set_vertex_property(StorageUpdateInterface& graph, label_t label,
                            vid_t vid, const std::string& key,
                            const std::string& value) {
    const auto& properties = graph.schema().get_vertex_property_names(label);
    const auto& types = graph.schema().get_vertex_properties(label);
    size_t prop_id = properties.size();
    PropertyType type(PropertyType::kEmpty);
    for (size_t i = 0; i < properties.size(); i++) {
      if (properties[i] == key) {
        prop_id = i;
        type = types[i];
        break;
      }
    }

    if (prop_id == properties.size()) {
      LOG(ERROR) << "Property " << key << " not found in vertex label "
                 << label;
      return false;
    }
    if (type.type_enum == impl::PropertyTypeImpl::kStringView) {
      graph.UpdateVertexProperty(label, vid, prop_id, Property(value));
    } else if (type == PropertyType::kInt32) {
      graph.UpdateVertexProperty(label, vid, prop_id,
                                 PropUtils<int>::to_prop(std::stoi(value)));
    } else if (type == PropertyType::kInt64) {
      graph.UpdateVertexProperty(
          label, vid, prop_id, PropUtils<int64_t>::to_prop(std::stoll(value)));
    } else {
      LOG(ERROR) << "Property " << key << " type not supported in vertex label "
                 << label;
      return false;
    }
    return true;
  }
  bool set_vertex_property(StorageUpdateInterface& graph, label_t label,
                           vid_t vid, const std::string& key,
                           const RTAny& value) {
    auto properties = graph.schema().get_vertex_property_names(label);
    size_t prop_id = properties.size();
    for (size_t i = 0; i < properties.size(); i++) {
      if (properties[i] == key) {
        prop_id = i;
        break;
      }
    }
    if (prop_id == properties.size()) {
      LOG(ERROR) << "Property " << key << " not found in vertex label "
                 << label;
      return false;
    }
    graph.UpdateVertexProperty(label, vid, prop_id, value.to_any());
    return true;
  }

  bool set_edge_property(StorageUpdateInterface& graph,
                         const LabelTriplet& label, Direction dir, vid_t src,
                         vid_t dst, const std::string& key,
                         const RTAny& value) {
    auto val_type = value.type();
    Property prop = Property::empty();
    if (val_type == RTAnyType::kNull || val_type == RTAnyType::kEmpty) {
    } else if (val_type == RTAnyType::kI32Value) {
      prop = Property::from_int32(value.as_int32());
    } else if (val_type == RTAnyType::kI64Value) {
      prop = Property::from_int64(value.as_int64());
    } else if (val_type == RTAnyType::kStringValue) {
      prop = Property::from_string_view(value.as_string());
    } else if (val_type == RTAnyType::kF64Value) {
      prop = Property::from_double(value.as_double());
    } else {
      LOG(ERROR) << "Edge property type not supported: "
                 << static_cast<int>(val_type);
      return false;
    }

    graph.SetEdgeData(dir == Direction::kOut, label.src_label, src,
                      label.dst_label, dst, label.edge_label, prop);
    return true;
  }

  bool _set_edge_property(StorageUpdateInterface& graph,
                          const LabelTriplet& label, Direction dir, vid_t src,
                          vid_t dst, const std::string& key,
                          const std::string& value) {
    auto types = graph.schema().get_edge_properties(
        label.src_label, label.dst_label, label.edge_label);
    auto property_names = graph.schema().get_edge_property_names(
        label.src_label, label.dst_label, label.edge_label);
    PropertyType type(PropertyType::kEmpty);
    size_t col_id = 0;
    for (; col_id < property_names.size(); col_id++) {
      if (property_names[col_id] == key) {
        type = types[col_id];
        break;
      }
    }
    if (col_id == property_names.size()) {
      LOG(ERROR) << "Property " << key << " not found in edge label "
                 << label.edge_label;
      return false;
    }
    Property prop = parse_property_from_string(type, value);
    graph.SetEdgeData(dir == Direction::kOut, label.src_label, src,
                      label.dst_label, dst, label.edge_label, prop, col_id);
    return true;
  }

  gs::result<Context> Eval(IStorageInterface& graph_interface,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override {
    auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
    Arena arena;
    for (size_t i = 0; i < keys_.size(); ++i) {
      auto& key = keys_[i];
      auto& value = values_[i];
      const auto& prop = ctx.get(key.first);
      CHECK(prop->column_type() == ContextColumnType::kVertex ||
            prop->column_type() == ContextColumnType::kEdge);
      if (value.operators_size() == 1 &&
          value.operators(0).item_case() == common::ExprOpr::kParam &&
          value.operators(0).param().data_type().data_type().primitive_type() ==
              common::PrimitiveType::DT_ANY) {
        if (prop->column_type() == ContextColumnType::kVertex) {
          for (size_t j = 0; j < ctx.row_num(); j++) {
            auto vertex =
                dynamic_cast<const IVertexColumn*>(prop.get())->get_vertex(j);
            if (!_set_vertex_property(
                    graph, vertex.label_, vertex.vid_, key.second,
                    params.at(value.operators(0).param().name()))) {
              LOG(ERROR) << "Failed to set vertex property";
              RETURN_INVALID_ARGUMENT_ERROR("Failed to set vertex property");
            }
            return ctx;
          }
        } else if (prop->column_type() == ContextColumnType::kEdge) {
          for (size_t j = 0; j < ctx.row_num(); j++) {
            auto edge =
                dynamic_cast<const IEdgeColumn*>(prop.get())->get_edge(j);
            if (!_set_edge_property(
                    graph, edge.label, edge.dir, edge.src, edge.dst, key.second,
                    params.at(value.operators(0).param().name()))) {
              LOG(ERROR) << "Failed to set edge property";
              RETURN_INVALID_ARGUMENT_ERROR("Failed to set edge property");
            }
            return ctx;
          }
        } else {
          LOG(ERROR) << "Failed to set property";
          RETURN_INVALID_ARGUMENT_ERROR("Failed to set property");
        }
      }
      if (prop->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = dynamic_cast<const IVertexColumn*>(prop.get());
        Expr expr(&graph, ctx, params, value, VarType::kPathVar);

        for (size_t j = 0; j < ctx.row_num(); j++) {
          auto val = expr.eval_path(j, arena);

          auto vertex = vertex_col->get_vertex(j);
          if (!set_vertex_property(graph, vertex.label_, vertex.vid_,
                                   key.second, val)) {
            LOG(ERROR) << "Failed to set vertex property";
            RETURN_INVALID_ARGUMENT_ERROR("Failed to set vertex property");
          }
        }
      } else {
        auto edge_col = dynamic_cast<const IEdgeColumn*>(prop.get());
        Expr expr(&graph, ctx, params, value, VarType::kPathVar);
        for (size_t j = 0; j < ctx.row_num(); j++) {
          auto val = expr.eval_path(j, arena);
          auto edge = edge_col->get_edge(j);
          if (!set_edge_property(graph, edge.label, edge.dir, edge.src,
                                 edge.dst, key.second, val)) {
            LOG(ERROR) << "Failed to set edge property";
            RETURN_INVALID_ARGUMENT_ERROR("Failed to set edge property");
          }
        }
      }
    }

    return ctx;
  }

 private:
  std::vector<std::pair<int, std::string>> keys_;
  std::vector<common::Expression> values_;
};

gs::result<OpBuildResultT> USetOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().set();
  std::vector<std::pair<int, std::string>> keys;
  std::vector<common::Expression> values;
  for (int i = 0; i < opr.items_size(); ++i) {
    const auto& item = opr.items(i);
    // only support mutate property now
    CHECK(item.kind() == cypher::Set_Item_Kind::Set_Item_Kind_MUTATE_PROPERTY);
    CHECK(item.has_key() && item.has_value());
    int tag = item.key().tag().id();
    const std::string& property_name = item.key().property().key().name();
    keys.emplace_back(tag, property_name);
    values.emplace_back(item.value());
  }
  return std::make_pair(
      std::make_unique<SetOpr>(std::move(keys), std::move(values)),
      ContextMeta());
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
