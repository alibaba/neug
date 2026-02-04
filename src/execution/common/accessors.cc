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

#include "neug/execution/common/columns/i_context_column.h"

namespace neug {

namespace runtime {

std::shared_ptr<IAccessor> create_context_value_accessor(const Context& ctx,
                                                         int tag,
                                                         const DataType& type) {
  auto col = ctx.get(tag);
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return std::make_shared<ContextValueAccessor<type>>(ctx, tag);
    FOR_EACH_DATA_TYPE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kStruct:
    return std::make_shared<ContextStructAccessor>(ctx, tag);
  case DataTypeId::kList:
    return std::make_shared<ContextListAccessor>(ctx, tag);
  case DataTypeId::kPath:
    return std::make_shared<ContextPathAccessor>(ctx, tag);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Not implemented accessor for type: " +
                                  std::to_string(static_cast<int>(type.id())));
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const StorageReadInterface& graph, const Context& ctx, int tag,
    const DataType& type, const std::string& prop_name) {
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type)                                        \
  case DataTypeId::enum_val:                                                   \
    return std::make_shared<VertexPropertyPathAccessor<type>>(graph, ctx, tag, \
                                                              prop_name);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar:
    return std::make_shared<VertexPropertyPathAccessor<std::string_view>>(
        graph, ctx, tag, prop_name);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Not implemented accessor for type: " +
                                  std::to_string(static_cast<int>(type.id())));
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_vertex_label_path_accessor(
    const Schema& schema, const Context& ctx, int tag) {
  return std::make_shared<VertexLabelPathAccessor>(schema, ctx, tag);
}

std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const StorageReadInterface& graph, const DataType& type,
    const std::string& prop_name) {
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type)                                \
  case DataTypeId::enum_val:                                           \
    return std::make_shared<VertexPropertyVertexAccessor<type>>(graph, \
                                                                prop_name);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar:
    return std::make_shared<VertexPropertyVertexAccessor<std::string_view>>(
        graph, prop_name);
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Not implemented accessor for type: " +
                                  std::to_string(static_cast<int>(type.id())));
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const StorageReadInterface& graph, const std::string& name,
    const Context& ctx, int tag, const DataType& type) {
  auto col = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag));
  auto labels = col->get_labels();
  if (labels.size() == 1) {
    switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type)                                    \
  case DataTypeId::enum_val:                                               \
    return std::make_shared<SLEdgePropertyPathAccessor<type>>(graph, name, \
                                                              ctx, tag);
      FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    case DataTypeId::kVarchar:
      return std::make_shared<SLEdgePropertyPathAccessor<std::string_view>>(
          graph, name, ctx, tag);
    default:
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Not implemented accessor for type: " +
          std::to_string(static_cast<int>(type.id())));
    }
  } else {
    switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type)                                       \
  case DataTypeId::enum_val:                                                  \
    return std::make_shared<EdgePropertyPathAccessor<type>>(graph, name, ctx, \
                                                            tag);
      FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    case DataTypeId::kVarchar:
      return std::make_shared<EdgePropertyPathAccessor<std::string_view>>(
          graph, name, ctx, tag);
    default:
      LOG(FATAL) << "not implemented - " << static_cast<int>(type.id());
    }
  }
  return nullptr;
}

template <typename T>
VertexPropertyPathAccessor<T>::VertexPropertyPathAccessor(
    const StorageReadInterface& graph, const Context& ctx, int tag,
    const std::string& prop_name)
    : type_(ValueConverter<T>::type()),
      is_optional_(false),
      vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {
  int max_label_num = std::numeric_limits<label_t>::max() + 1;
  property_columns_.resize(max_label_num);
  const auto& labels = vertex_col_.get_labels_set();
  if (vertex_col_.is_optional()) {
    is_optional_ = true;
  }
  for (auto label : labels) {
    property_columns_[label] =
        graph.template GetVertexPropColumn<T>(label, prop_name);
    if (property_columns_[label] == nullptr) {
      is_optional_ = true;
    }
  }
}

std::shared_ptr<IAccessor> create_edge_label_path_accessor(const Schema& schema,
                                                           const Context& ctx,
                                                           int tag) {
  return std::make_shared<EdgeLabelPathAccessor>(schema, ctx, tag);
}

std::shared_ptr<IAccessor> create_edge_property_edge_accessor(
    const StorageReadInterface& graph, const std::string& prop_name,
    const DataType& type) {
  switch (type.id()) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return std::make_shared<EdgePropertyEdgeAccessor<type>>(graph, prop_name);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar:
    return std::make_shared<EdgePropertyEdgeAccessor<std::string_view>>(
        graph, prop_name);
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type.id());
    return nullptr;
  }
}

template class VertexPropertyPathAccessor<int64_t>;
template class VertexPropertyPathAccessor<int32_t>;
template class VertexPropertyPathAccessor<uint32_t>;
template class VertexPropertyPathAccessor<uint64_t>;
template class VertexPropertyPathAccessor<std::string_view>;
template class VertexPropertyPathAccessor<Date>;
template class VertexPropertyPathAccessor<DateTime>;
template class VertexPropertyPathAccessor<double>;
template class VertexPropertyPathAccessor<Interval>;

template class ContextValueAccessor<int64_t>;
template class ContextValueAccessor<int32_t>;
template class ContextValueAccessor<uint32_t>;
template class ContextValueAccessor<uint64_t>;
template class ContextValueAccessor<std::string>;
template class ContextValueAccessor<DateTime>;
template class ContextValueAccessor<Date>;
template class ContextValueAccessor<bool>;
template class ContextValueAccessor<Interval>;

template class VertexPropertyVertexAccessor<int64_t>;
template class VertexPropertyVertexAccessor<int32_t>;
template class VertexPropertyVertexAccessor<uint32_t>;
template class VertexPropertyVertexAccessor<uint64_t>;
template class VertexPropertyVertexAccessor<std::string_view>;
template class VertexPropertyVertexAccessor<Date>;
template class VertexPropertyVertexAccessor<DateTime>;
template class VertexPropertyVertexAccessor<double>;

template class EdgePropertyEdgeAccessor<int64_t>;
template class EdgePropertyEdgeAccessor<int32_t>;
template class EdgePropertyEdgeAccessor<uint32_t>;
template class EdgePropertyEdgeAccessor<uint64_t>;
template class EdgePropertyEdgeAccessor<std::string_view>;
template class EdgePropertyEdgeAccessor<Date>;
template class EdgePropertyEdgeAccessor<DateTime>;
template class EdgePropertyEdgeAccessor<double>;

}  // namespace runtime

}  // namespace neug
