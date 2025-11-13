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

#include "neug/execution/common/accessors.h"

#include <cstdint>
#include <string_view>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/utils/runtime/rt_any.h"

namespace gs {

namespace runtime {

std::shared_ptr<IAccessor> create_context_value_accessor(const Context& ctx,
                                                         int tag,
                                                         RTAnyType type) {
  auto col = ctx.get(tag);
  switch (type) {
  case RTAnyType::kBoolValue:
    return std::make_shared<ContextValueAccessor<bool>>(ctx, tag);
  case RTAnyType::kI64Value:
    return std::make_shared<ContextValueAccessor<int64_t>>(ctx, tag);
  case RTAnyType::kI32Value:
    return std::make_shared<ContextValueAccessor<int>>(ctx, tag);
  case RTAnyType::kU32Value:
    return std::make_shared<ContextValueAccessor<uint32_t>>(ctx, tag);
  case RTAnyType::kU64Value:
    return std::make_shared<ContextValueAccessor<uint64_t>>(ctx, tag);
  case RTAnyType::kStringValue:
    return std::make_shared<ContextValueAccessor<std::string_view>>(ctx, tag);
  case RTAnyType::kDateTime:
    return std::make_shared<ContextValueAccessor<DateTime>>(ctx, tag);
  case RTAnyType::kDate:
    return std::make_shared<ContextValueAccessor<Date>>(ctx, tag);
  case RTAnyType::kTuple:
    return std::make_shared<ContextValueAccessor<Tuple>>(ctx, tag);
  case RTAnyType::kList:
    return std::make_shared<ContextValueAccessor<List>>(ctx, tag);
  case RTAnyType::kMap:
    return std::make_shared<ContextValueAccessor<Map>>(ctx, tag);
  case RTAnyType::kPath:
    return std::make_shared<ContextValueAccessor<Path>>(ctx, tag);
  case RTAnyType::kVertex:
    return std::make_shared<ContextValueAccessor<VertexRecord>>(ctx, tag);
  case RTAnyType::kEdge:
    return std::make_shared<ContextValueAccessor<EdgeRecord>>(ctx, tag);
  case RTAnyType::kF32Value:
    return std::make_shared<ContextValueAccessor<float>>(ctx, tag);
  case RTAnyType::kF64Value:
    return std::make_shared<ContextValueAccessor<double>>(ctx, tag);
  case RTAnyType::kSet:
    return std::make_shared<ContextValueAccessor<Set>>(ctx, tag);
  case RTAnyType::kInterval:
    return std::make_shared<ContextValueAccessor<Interval>>(ctx, tag);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Not implemented accessor for type: " +
                                  std::to_string(static_cast<int>(type)));
  }
  return nullptr;
}

template <typename GraphInterface>
std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const GraphInterface& graph, const Context& ctx, int tag, RTAnyType type,
    const std::string& prop_name) {
  switch (type) {
  case RTAnyType::kBoolValue:
    return std::make_shared<VertexPropertyPathAccessor<GraphInterface, bool>>(
        graph, ctx, tag, prop_name);
  case RTAnyType::kI64Value:
    return std::make_shared<
        VertexPropertyPathAccessor<GraphInterface, int64_t>>(graph, ctx, tag,
                                                             prop_name);
  case RTAnyType::kI32Value:
    return std::make_shared<VertexPropertyPathAccessor<GraphInterface, int>>(
        graph, ctx, tag, prop_name);
  case RTAnyType::kU32Value:
    return std::make_shared<
        VertexPropertyPathAccessor<GraphInterface, uint32_t>>(graph, ctx, tag,
                                                              prop_name);
  case RTAnyType::kU64Value:
    return std::make_shared<
        VertexPropertyPathAccessor<GraphInterface, uint64_t>>(graph, ctx, tag,
                                                              prop_name);
  case RTAnyType::kStringValue:
    return std::make_shared<
        VertexPropertyPathAccessor<GraphInterface, std::string_view>>(
        graph, ctx, tag, prop_name);
  case RTAnyType::kDate:
    return std::make_shared<VertexPropertyPathAccessor<GraphInterface, Date>>(
        graph, ctx, tag, prop_name);
  case RTAnyType::kDateTime:
    return std::make_shared<
        VertexPropertyPathAccessor<GraphInterface, DateTime>>(graph, ctx, tag,
                                                              prop_name);

  case RTAnyType::kInterval:
    return std::make_shared<
        VertexPropertyPathAccessor<GraphInterface, Interval>>(graph, ctx, tag,
                                                              prop_name);

  case RTAnyType::kF32Value:
    return std::make_shared<VertexPropertyPathAccessor<GraphInterface, float>>(
        graph, ctx, tag, prop_name);
  case RTAnyType::kF64Value:
    return std::make_shared<VertexPropertyPathAccessor<GraphInterface, double>>(
        graph, ctx, tag, prop_name);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Not implemented accessor for type: " +
                                  std::to_string(static_cast<int>(type)));
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_vertex_label_path_accessor(
    const Schema& schema, const Context& ctx, int tag) {
  return std::make_shared<VertexLabelPathAccessor>(schema, ctx, tag);
}

template <typename GraphInterface>
std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const GraphInterface& graph, RTAnyType type, const std::string& prop_name) {
  switch (type) {
  case RTAnyType::kBoolValue:
    return std::make_shared<VertexPropertyVertexAccessor<GraphInterface, bool>>(
        graph, prop_name);
  case RTAnyType::kI64Value:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, int64_t>>(graph,
                                                               prop_name);
  case RTAnyType::kI32Value:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, int32_t>>(graph,
                                                               prop_name);
  case RTAnyType::kU32Value:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, uint32_t>>(graph,
                                                                prop_name);
  case RTAnyType::kU64Value:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, uint64_t>>(graph,
                                                                prop_name);
  case RTAnyType::kStringValue:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, std::string_view>>(
        graph, prop_name);
  case RTAnyType::kDate:
    return std::make_shared<VertexPropertyVertexAccessor<GraphInterface, Date>>(
        graph, prop_name);
  case RTAnyType::kDateTime:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, DateTime>>(graph,
                                                                prop_name);
  case RTAnyType::kInterval:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, Interval>>(graph,
                                                                prop_name);
  case RTAnyType::kF32Value:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, float>>(graph, prop_name);
  case RTAnyType::kF64Value:
    return std::make_shared<
        VertexPropertyVertexAccessor<GraphInterface, double>>(graph, prop_name);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Not implemented accessor for type: " +
                                  std::to_string(static_cast<int>(type)));
  }
  return nullptr;
}

template <typename GraphInterface>
std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const GraphInterface& graph, const std::string& name, const Context& ctx,
    int tag, RTAnyType type) {
  auto col = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag));
  auto labels = col->get_labels();
  if (labels.size() == 1) {
    switch (type) {
    case RTAnyType::kI32Value:
      return std::make_shared<SLEdgePropertyPathAccessor<GraphInterface, int>>(
          graph, name, ctx, tag);
    case RTAnyType::kU32Value:
      return std::make_shared<
          SLEdgePropertyPathAccessor<GraphInterface, uint32_t>>(graph, name,
                                                                ctx, tag);
    case RTAnyType::kI64Value:
      return std::make_shared<
          SLEdgePropertyPathAccessor<GraphInterface, int64_t>>(graph, name, ctx,
                                                               tag);
    case RTAnyType::kU64Value:
      return std::make_shared<
          SLEdgePropertyPathAccessor<GraphInterface, uint64_t>>(graph, name,
                                                                ctx, tag);
    case RTAnyType::kStringValue:
      return std::make_shared<
          SLEdgePropertyPathAccessor<GraphInterface, std::string_view>>(
          graph, name, ctx, tag);
    case RTAnyType::kDate:
      return std::make_shared<SLEdgePropertyPathAccessor<GraphInterface, Date>>(
          graph, name, ctx, tag);
    case RTAnyType::kDateTime:
      return std::make_shared<
          SLEdgePropertyPathAccessor<GraphInterface, DateTime>>(graph, name,
                                                                ctx, tag);
    case RTAnyType::kF64Value:
      return std::make_shared<
          SLEdgePropertyPathAccessor<GraphInterface, double>>(graph, name, ctx,
                                                              tag);
    default:
      LOG(FATAL) << "not implemented - " << static_cast<int>(type);
    }
  } else {
    switch (type) {
    case RTAnyType::kI32Value:
      return std::make_shared<EdgePropertyPathAccessor<GraphInterface, int>>(
          graph, name, ctx, tag);
    case RTAnyType::kU32Value:
      return std::make_shared<
          EdgePropertyPathAccessor<GraphInterface, uint32_t>>(graph, name, ctx,
                                                              tag);
    case RTAnyType::kI64Value:
      return std::make_shared<
          EdgePropertyPathAccessor<GraphInterface, int64_t>>(graph, name, ctx,
                                                             tag);
    case RTAnyType::kU64Value:
      return std::make_shared<
          EdgePropertyPathAccessor<GraphInterface, uint64_t>>(graph, name, ctx,
                                                              tag);
    case RTAnyType::kStringValue:
      return std::make_shared<
          EdgePropertyPathAccessor<GraphInterface, std::string_view>>(
          graph, name, ctx, tag);
    case RTAnyType::kDate:
      return std::make_shared<EdgePropertyPathAccessor<GraphInterface, Date>>(
          graph, name, ctx, tag);
    case RTAnyType::kDateTime:
      return std::make_shared<
          EdgePropertyPathAccessor<GraphInterface, DateTime>>(graph, name, ctx,
                                                              tag);
    case RTAnyType::kF64Value:
      return std::make_shared<EdgePropertyPathAccessor<GraphInterface, double>>(
          graph, name, ctx, tag);
    default:
      LOG(FATAL) << "not implemented - " << static_cast<int>(type);
    }
  }
  return nullptr;
}

template <typename GraphInterface, typename T>
VertexPropertyPathAccessor<GraphInterface, T>::VertexPropertyPathAccessor(
    const GraphInterface& graph, const Context& ctx, int tag,
    const std::string& prop_name)
    : is_optional_(false),
      vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {
  int max_label_num = std::numeric_limits<label_t>::max() + 1;
  property_columns_.resize(max_label_num);
  const auto& labels = vertex_col_.get_labels_set();
  if (vertex_col_.is_optional()) {
    is_optional_ = true;
  }
  for (auto label : labels) {
    property_columns_[label] =
        graph.template GetVertexColumn<T>(label, prop_name);
    if (property_columns_[label].is_null()) {
      is_optional_ = true;
    }
  }
}

std::shared_ptr<IAccessor> create_edge_label_path_accessor(const Schema& schema,
                                                           const Context& ctx,
                                                           int tag) {
  return std::make_shared<EdgeLabelPathAccessor>(schema, ctx, tag);
}

template <typename GraphInterface>
std::shared_ptr<IAccessor> create_edge_property_edge_accessor(
    const GraphInterface& graph, const std::string& prop_name, RTAnyType type) {
  switch (type) {
  case RTAnyType::kI64Value:
    return std::make_shared<EdgePropertyEdgeAccessor<GraphInterface, int64_t>>(
        graph, prop_name);
  case RTAnyType::kI32Value:
    return std::make_shared<EdgePropertyEdgeAccessor<GraphInterface, int>>(
        graph, prop_name);
  case RTAnyType::kU32Value:
    return std::make_shared<EdgePropertyEdgeAccessor<GraphInterface, uint32_t>>(
        graph, prop_name);
  case RTAnyType::kU64Value:
    return std::make_shared<EdgePropertyEdgeAccessor<GraphInterface, uint64_t>>(
        graph, prop_name);
  case RTAnyType::kStringValue:
    return std::make_shared<
        EdgePropertyEdgeAccessor<GraphInterface, std::string_view>>(graph,
                                                                    prop_name);
  case RTAnyType::kDate:
    return std::make_shared<EdgePropertyEdgeAccessor<GraphInterface, Date>>(
        graph, prop_name);
  case RTAnyType::kDateTime:
    return std::make_shared<EdgePropertyEdgeAccessor<GraphInterface, DateTime>>(
        graph, prop_name);
  case RTAnyType::kF64Value:
    return std::make_shared<EdgePropertyEdgeAccessor<GraphInterface, double>>(
        graph, prop_name);
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type);
  }
}

template std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const GraphReadInterface& graph, const Context& ctx, int tag,
    RTAnyType type, const std::string& prop_name);
template std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const GraphUpdateInterface& graph, const Context& ctx, int tag,
    RTAnyType type, const std::string& prop_name);

template std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const GraphReadInterface& graph, RTAnyType type,
    const std::string& prop_name);
template std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const GraphUpdateInterface& graph, RTAnyType type,
    const std::string& prop_name);

template std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const GraphReadInterface& graph, const std::string& name,
    const Context& ctx, int tag, RTAnyType type);
template std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const GraphUpdateInterface& graph, const std::string& name,
    const Context& ctx, int tag, RTAnyType type);

template std::shared_ptr<IAccessor> create_edge_property_edge_accessor(
    const GraphReadInterface& graph, const std::string& prop_name,
    RTAnyType type);
template std::shared_ptr<IAccessor> create_edge_property_edge_accessor(
    const GraphUpdateInterface& graph, const std::string& prop_name,
    RTAnyType type);

template class VertexPropertyPathAccessor<GraphReadInterface, int64_t>;
template class VertexPropertyPathAccessor<GraphReadInterface, int32_t>;
template class VertexPropertyPathAccessor<GraphReadInterface, uint32_t>;
template class VertexPropertyPathAccessor<GraphReadInterface, uint64_t>;
template class VertexPropertyPathAccessor<GraphReadInterface, std::string_view>;
template class VertexPropertyPathAccessor<GraphReadInterface, Date>;
template class VertexPropertyPathAccessor<GraphReadInterface, DateTime>;
template class VertexPropertyPathAccessor<GraphReadInterface, double>;
template class VertexPropertyPathAccessor<GraphReadInterface, Interval>;

template class ContextValueAccessor<int64_t>;
template class ContextValueAccessor<int32_t>;
template class ContextValueAccessor<uint32_t>;
template class ContextValueAccessor<uint64_t>;
template class ContextValueAccessor<std::string_view>;
template class ContextValueAccessor<DateTime>;
template class ContextValueAccessor<Date>;
template class ContextValueAccessor<bool>;
template class ContextValueAccessor<Tuple>;
template class ContextValueAccessor<List>;
template class ContextValueAccessor<Set>;
template class ContextValueAccessor<Interval>;

template class VertexPropertyVertexAccessor<GraphReadInterface, int64_t>;
template class VertexPropertyVertexAccessor<GraphReadInterface, int32_t>;
template class VertexPropertyVertexAccessor<GraphReadInterface, uint32_t>;
template class VertexPropertyVertexAccessor<GraphReadInterface, uint64_t>;
template class VertexPropertyVertexAccessor<GraphReadInterface,
                                            std::string_view>;
template class VertexPropertyVertexAccessor<GraphReadInterface, Date>;
template class VertexPropertyVertexAccessor<GraphReadInterface, DateTime>;
template class VertexPropertyVertexAccessor<GraphReadInterface, double>;

template class EdgePropertyEdgeAccessor<GraphReadInterface, int64_t>;
template class EdgePropertyEdgeAccessor<GraphReadInterface, int32_t>;
template class EdgePropertyEdgeAccessor<GraphReadInterface, uint32_t>;
template class EdgePropertyEdgeAccessor<GraphReadInterface, uint64_t>;
template class EdgePropertyEdgeAccessor<GraphReadInterface, std::string_view>;
template class EdgePropertyEdgeAccessor<GraphReadInterface, Date>;
template class EdgePropertyEdgeAccessor<GraphReadInterface, DateTime>;
template class EdgePropertyEdgeAccessor<GraphReadInterface, double>;

}  // namespace runtime

}  // namespace gs
