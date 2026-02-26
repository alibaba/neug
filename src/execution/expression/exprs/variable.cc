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

#include "neug/execution/expression/exprs/variable.h"
#include "neug/execution/common/context.h"
#include "neug/execution/expression/accessors/const_accessor.h"
#include "neug/execution/expression/accessors/edge_accessor.h"
#include "neug/execution/expression/accessors/record_accessor.h"
#include "neug/execution/expression/accessors/vertex_accessor.h"
#include "neug/generated/proto/plan/common.pb.h"
namespace neug {
namespace execution {

DataType parse_from_data_type(const ::common::DataType& ddt) {
  switch (ddt.item_case()) {
  case ::common::DataType::kPrimitiveType: {
    const ::common::PrimitiveType pt = ddt.primitive_type();
    switch (pt) {
    case ::common::PrimitiveType::DT_SIGNED_INT32:
      return DataType(DataTypeId::kInt32);
    case ::common::PrimitiveType::DT_UNSIGNED_INT32:
      return DataType(DataTypeId::kUInt32);
    case ::common::PrimitiveType::DT_UNSIGNED_INT64:
      return DataType(DataTypeId::kUInt64);
    case ::common::PrimitiveType::DT_SIGNED_INT64:
      return DataType(DataTypeId::kInt64);
    case ::common::PrimitiveType::DT_FLOAT:
      return DataType(DataTypeId::kFloat);
    case ::common::PrimitiveType::DT_DOUBLE:
      return DataType(DataTypeId::kDouble);
    case ::common::PrimitiveType::DT_BOOL:
      return DataType(DataTypeId::kBoolean);
    default:
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized primitive type - " +
                                    std::to_string(pt));
      break;
    }
  }
  case ::common::DataType::kString:
    return DataType(DataTypeId::kVarchar);
  case ::common::DataType::kTemporal: {
    if (ddt.temporal().item_case() == ::common::Temporal::kDate32) {
      return DataType(DataTypeId::kDate);
    } else if (ddt.temporal().item_case() == ::common::Temporal::kDateTime) {
      return DataType(DataTypeId::kTimestampMs);
    } else if (ddt.temporal().item_case() == ::common::Temporal::kDate) {
      return DataType(DataTypeId::kDate);
    } else if (ddt.temporal().item_case() == ::common::Temporal::kInterval) {
      return DataType(DataTypeId::kInterval);
    } else if (ddt.temporal().item_case() == ::common::Temporal::kTimestamp) {
      return DataType(DataTypeId::kTimestampMs);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized temporal type - " +
                                    ddt.temporal().DebugString());
    }
  }
  case ::common::DataType::kArray: {
    const auto& element_type = ddt.array().component_type();
    auto data_type = parse_from_data_type(element_type);
    return DataType(DataTypeId::kList,
                    std::make_shared<ListTypeInfo>(data_type));
  }
  case ::common::DataType::kTuple: {
    const auto& component_types = ddt.tuple().component_types();
    std::vector<DataType> data_types;
    for (int i = 0; i < component_types.size(); ++i) {
      data_types.push_back(parse_from_data_type(component_types.Get(i)));
    }
    std::shared_ptr<ExtraTypeInfo> type_info =
        std::make_shared<StructTypeInfo>(data_types);
    return DataType(DataTypeId::kStruct, type_info);
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("unrecognized data type - " +
                                  ddt.DebugString());
    break;
  }
  return DataType(DataTypeId::kUnknown);
}

DataType parse_from_ir_data_type(const ::common::IrDataType& dt) {
  switch (dt.type_case()) {
  case ::common::IrDataType::TypeCase::kDataType: {
    const ::common::DataType ddt = dt.data_type();
    return parse_from_data_type(ddt);
  }
  case ::common::IrDataType::TypeCase::kGraphType: {
    const ::common::GraphDataType gdt = dt.graph_type();
    switch (gdt.element_opt()) {
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_VERTEX:
      return DataType(DataTypeId::kVertex);
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_EDGE:
      return DataType(DataTypeId::kEdge);
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_PATH:
      return DataType(DataTypeId::kPath);
    default:
      THROW_NOT_SUPPORTED_EXCEPTION("unrecognized graph data type - " +
                                    gdt.DebugString());
      break;
    }
  } break;
  case ::common::IrDataType::TypeCase::kListType: {
    const auto& element_type = dt.list_type().component_type();
    switch (element_type.element_opt()) {
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_VERTEX:
      return DataType(DataTypeId::kList, std::make_shared<ListTypeInfo>(
                                             DataType(DataTypeId::kVertex)));
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_EDGE:
      return DataType(DataTypeId::kList, std::make_shared<ListTypeInfo>(
                                             DataType(DataTypeId::kEdge)));
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_PATH:
      return DataType(DataTypeId::kList, std::make_shared<ListTypeInfo>(
                                             DataType(DataTypeId::kPath)));

    default:
      break;
    }
  }

  default:
    break;
  }
  LOG(ERROR) << "unrecognized ir data type - " << dt.DebugString();
  return DataType(DataTypeId::kUnknown);
}

static Value from_const(const ::common::Value& const_val) {
  switch (const_val.item_case()) {
  case common::Value::kI32:
    return Value::INT32(const_val.i32());
  case common::Value::kI64:
    return Value::INT64(const_val.i64());
  case common::Value::kF64:
    return Value::DOUBLE(const_val.f64());
  case common::Value::kF32:
    return Value::FLOAT(const_val.f32());
  case common::Value::kBoolean:
    return Value::BOOLEAN(const_val.boolean());
  case common::Value::kStr:
    return Value::STRING(const_val.str());
  case common::Value::kU32:
    return Value::UINT32(const_val.u32());
  case common::Value::kU64:
    return Value::UINT64(const_val.u64());
  case common::Value::kI32Array: {
    std::vector<Value> values;
    size_t size = const_val.i32_array().item_size();
    for (size_t i = 0; i < size; ++i) {
      values.emplace_back(Value::INT32(const_val.i32_array().item(i)));
    }
    return Value::LIST(DataType::INT32, std::move(values));
  }
  case common::Value::kI64Array: {
    std::vector<Value> values;
    size_t size = const_val.i64_array().item_size();
    for (size_t i = 0; i < size; ++i) {
      values.emplace_back(Value::INT64(const_val.i64_array().item(i)));
    }
    return Value::LIST(DataType::INT64, std::move(values));
  }
  case common::Value::kF64Array: {
    std::vector<Value> values;
    size_t size = const_val.f64_array().item_size();
    for (size_t i = 0; i < size; ++i) {
      values.emplace_back(Value::DOUBLE(const_val.f64_array().item(i)));
    }
    return Value::LIST(DataType::DOUBLE, std::move(values));
  }
  case common::Value::kStrArray: {
    std::vector<Value> values;
    size_t size = const_val.str_array().item_size();
    for (size_t i = 0; i < size; ++i) {
      values.emplace_back(Value::STRING(const_val.str_array().item(i)));
    }
    return Value::LIST(DataType::VARCHAR, std::move(values));
  }
  case common::Value::kNone:
    return Value(DataType::SQLNULL);
  default:
    throw std::runtime_error(
        "Unsupported const value type: " +
        std::to_string(static_cast<int>(const_val.item_case())));
    return Value(DataType::SQLNULL);
  }
}
std::unique_ptr<ExprBase> parse_const(const ::common::Value& const_val) {
  return std::make_unique<ConstExpr>(from_const(const_val));
}

std::unique_ptr<ExprBase> parse_param(const ::common::DynamicParam& param) {
  auto type = parse_from_ir_data_type(param.data_type());
  return std::make_unique<ParamExpr>(param.name(), type);
}

static std::unique_ptr<ExprBase> parse_record_var(const ::common::Variable& var,
                                                  const ContextMeta& ctx_meta,
                                                  int tag, DataType type) {
  if (!var.has_property()) {
    return std::make_unique<RecordAccessor>(tag, type);
  } else {
    const auto& prop = var.property();
    if (prop.has_key()) {
      if (ctx_meta.get(tag).id() == DataTypeId::kVertex) {
        return RecordVertexAccessor::create_property_accessor(
            tag, type, prop.key().name());
      } else if (ctx_meta.get(tag).id() == DataTypeId::kEdge) {
        return RecordEdgeAccessor::create_property_accessor(tag, type,
                                                            prop.key().name());
      } else if (ctx_meta.get(tag).id() == DataTypeId::kPath) {
        if (prop.key().name() == "cost") {
          return std::make_unique<RecordPathAccessor>(tag, DataType::DOUBLE,
                                                      "cost");
        }
        LOG(FATAL) << "unsupported record path property key: "
                   << prop.key().name();
        return nullptr;
      } else {
        LOG(FATAL) << "unsupported record variable tag type: "
                   << static_cast<int>(ctx_meta.get(tag).id()) << " "
                   << var.DebugString();
      }
    } else if (prop.has_label()) {
      if (ctx_meta.get(tag).id() == DataTypeId::kVertex) {
        return RecordVertexAccessor::create_label_accessor(tag);
      } else if (ctx_meta.get(tag).id() == DataTypeId::kEdge) {
        return RecordEdgeAccessor::create_label_accessor(tag);
      } else {
        LOG(FATAL) << "unsupported record variable tag type: "
                   << static_cast<int>(ctx_meta.get(tag).id());
      }
    } else if (prop.has_id()) {
      if (ctx_meta.get(tag).id() == DataTypeId::kVertex) {
        return RecordVertexAccessor::create_gid_accessor(tag);
      } else if (ctx_meta.get(tag).id() == DataTypeId::kEdge) {
        return RecordEdgeAccessor::create_gid_accessor(tag);
      } else {
        LOG(FATAL) << "unsupported record variable tag type: "
                   << static_cast<int>(ctx_meta.get(tag).id());
      }
    } else if (prop.has_len()) {
      if (ctx_meta.get(tag).id() == DataTypeId::kPath) {
        return std::make_unique<RecordPathAccessor>(tag, DataType::INT64,
                                                    "length");
      }
    } else {
      LOG(FATAL) << "unsupported property access: " << prop.DebugString();
    }
  }
  return nullptr;
}

static std::unique_ptr<ExprBase> parse_vertex_var(const common::Variable& var,

                                                  DataType type) {
  if (!var.has_property()) {
    LOG(FATAL) << "vertex variable missing property: " << var.DebugString();
  } else {
    const auto& prop = var.property();
    if (prop.has_key()) {
      return VertexAccessor::create_property_accessor(type, prop.key().name());
    } else if (prop.has_label()) {
      return VertexAccessor::create_label_accessor();
    } else if (prop.has_id()) {
      return VertexAccessor::create_gid_accessor();
    } else {
      LOG(FATAL) << "unsupported property access: " << prop.DebugString();
    }
  }
  return nullptr;
}

static std::unique_ptr<ExprBase> parse_edge_var(const common::Variable& var,
                                                DataType type) {
  if (!var.has_property()) {
    LOG(FATAL) << "edge variable missing property: " << var.DebugString();
  } else {
    const auto& prop = var.property();
    if (prop.has_key()) {
      return EdgeAccessor::create_property_accessor(type, prop.key().name());
    } else if (prop.has_label()) {
      return EdgeAccessor::create_label_accessor();
    } else {
      LOG(FATAL) << "unsupported property access: " << prop.DebugString();
    }
  }
  return nullptr;
}

std::unique_ptr<ExprBase> parse_variable(const common::Variable& var,
                                         const ContextMeta& ctx_meta,
                                         VarType var_type) {
  if (!var.has_node_type()) {
    LOG(FATAL) << "variable missing node_type: " << var.DebugString();
  }
  DataType type = parse_from_ir_data_type(var.node_type());
  int tag = var.has_tag() ? var.tag().id() : -1;
  if (var_type == VarType::kRecord) {
    return parse_record_var(var, ctx_meta, tag, type);
  } else if (var_type == VarType::kVertex) {
    return parse_vertex_var(var, type);
  } else {
    return parse_edge_var(var, type);
  }
}
}  // namespace execution
}  // namespace neug