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

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License. Modified
 * by Liu Lexiao in 2025 to support Neug-specific features.
 */

#include <assert.h>

#include <glog/logging.h>

#include "neug/common/types.h"

#include "neug/common/extra_type_info.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/type.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

DataType::DataType() : DataType(DataTypeId::kInvalid) {}

DataType::DataType(DataTypeId id) : id_(id) {}

DataType::DataType(DataTypeId id, std::shared_ptr<ExtraTypeInfo> type_info)
    : id_(id), type_info_(std::move(type_info)) {}

DataType::DataType(const DataType& other)
    : id_(other.id_), type_info_(other.type_info_) {}

DataType::DataType(DataType&& other) noexcept
    : id_(other.id_), type_info_(std::move(other.type_info_)) {}
DataType::~DataType() {}

bool DataType::EqualTypeInfo(const DataType& rhs) const {
  if (type_info_.get() == rhs.type_info_.get()) {
    return true;
  }
  if (type_info_) {
    return type_info_->Equals(rhs.type_info_.get());
  } else {
    assert(rhs.type_info_);
    return rhs.type_info_->Equals(type_info_.get());
  }
}

bool DataType::operator==(const DataType& rhs) const {
  if (id_ != rhs.id_) {
    return false;
  }
  return EqualTypeInfo(rhs);
}

const DataType& ListType::GetChildType(const DataType& type) {
  assert(type.id() == DataTypeId::kList);
  auto info = type.getExtraTypeInfo();
  assert(info);
  return info->Cast<ListTypeInfo>().child_type;
}

static const StructTypeInfo& getStructInfo(const DataType& type) {
  assert(type.id() == DataTypeId::kStruct || type.id() == DataTypeId::kVertex ||
         type.id() == DataTypeId::kEdge || type.id() == DataTypeId::kPath);
  auto info = type.getExtraTypeInfo();
  assert(info);
  return info->Cast<StructTypeInfo>();
}

const std::vector<DataType>& StructType::GetChildTypes(const DataType& type) {
  return getStructInfo(type).child_types;
}

const DataType& StructType::GetChildType(const DataType& type, size_t index) {
  const auto& children = GetChildTypes(type);
  assert(index < children.size());
  return children[index];
}

const std::string& StructType::GetChildName(const DataType& type,
                                            size_t index) {
  const auto& info = getStructInfo(type);
  assert(index < info.field_names.size());
  return info.field_names[index];
}

const std::vector<std::string>& StructType::GetFieldNames(
    const DataType& type) {
  return getStructInfo(type).field_names;
}

bool StructType::HasField(const DataType& type, const std::string& name) {
  return getStructInfo(type).hasField(name);
}

size_t StructType::GetFieldIdx(const DataType& type, const std::string& name) {
  return getStructInfo(type).getFieldIdx(name);
}

uint64_t StructType::GetNumFields(const DataType& type) {
  return getStructInfo(type).child_types.size();
}

const DataType& ArrayType::GetChildType(const DataType& type) {
  assert(type.id() == DataTypeId::kArray);
  auto info = type.getExtraTypeInfo();
  assert(info);
  return info->Cast<ArrayTypeInfo>().child_type;
}

uint64_t ArrayType::GetNumElements(const DataType& type) {
  assert(type.id() == DataTypeId::kArray);
  auto info = type.getExtraTypeInfo();
  assert(info);
  return info->Cast<ArrayTypeInfo>().num_elements;
}

const DataType& MapType::GetKeyType(const DataType& type) {
  assert(type.id() == DataTypeId::kMap);
  auto info = type.getExtraTypeInfo();
  assert(info);
  return info->Cast<MapTypeInfo>().key_type;
}

const DataType& MapType::GetValueType(const DataType& type) {
  assert(type.id() == DataTypeId::kMap);
  auto info = type.getExtraTypeInfo();
  assert(info);
  return info->Cast<MapTypeInfo>().value_type;
}

DataType DataType::Struct(std::vector<DataType> children) {
  std::shared_ptr<ExtraTypeInfo> type_info =
      std::make_shared<StructTypeInfo>(std::move(children));
  return DataType(DataTypeId::kStruct, type_info);
}

DataType DataType::Struct(std::vector<std::string> field_names,
                          std::vector<DataType> field_types) {
  std::shared_ptr<ExtraTypeInfo> type_info = std::make_shared<StructTypeInfo>(
      std::move(field_names), std::move(field_types));
  return DataType(DataTypeId::kStruct, type_info);
}

DataType DataType::List(const DataType& child_type) {
  std::shared_ptr<ExtraTypeInfo> type_info =
      std::make_shared<ListTypeInfo>(child_type);
  return DataType(DataTypeId::kList, type_info);
}

DataType DataType::Array(const DataType& child_type, uint64_t num_elements) {
  // Reject size-0 arrays except when the child type is kUnknown, which is
  // used as an internal wildcard placeholder (e.g., in getAllLogicalTypes()).
  if (num_elements == 0 && child_type.id() != DataTypeId::kUnknown) {
    THROW_RUNTIME_ERROR(
        "The number of elements in an array must be greater than 0.");
  }
  std::shared_ptr<ExtraTypeInfo> type_info =
      std::make_shared<ArrayTypeInfo>(child_type, num_elements);
  return DataType(DataTypeId::kArray, type_info);
}

DataType DataType::Map(const DataType& key_type, const DataType& value_type) {
  std::shared_ptr<ExtraTypeInfo> type_info =
      std::make_shared<MapTypeInfo>(key_type, value_type);
  return DataType(DataTypeId::kMap, type_info);
}

DataType DataType::Varchar(size_t max_length) {
  std::shared_ptr<ExtraTypeInfo> type_info =
      std::make_shared<StringTypeInfo>(max_length);
  return DataType(DataTypeId::kVarchar, type_info);
}

DataType DataType::InternalId() { return DataType(DataTypeId::kInternalId); }

bool DataType::containsAny() const {
  if (id_ == DataTypeId::kUnknown)
    return true;
  if (!type_info_)
    return false;
  switch (id_) {
  case DataTypeId::kList: {
    auto& info = type_info_->Cast<ListTypeInfo>();
    return info.child_type.containsAny();
  }
  case DataTypeId::kArray: {
    auto& info = type_info_->Cast<ArrayTypeInfo>();
    return info.child_type.containsAny();
  }
  case DataTypeId::kMap: {
    auto& info = type_info_->Cast<MapTypeInfo>();
    return info.key_type.containsAny() || info.value_type.containsAny();
  }
  case DataTypeId::kStruct:
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
  case DataTypeId::kPath: {
    auto& info = type_info_->Cast<StructTypeInfo>();
    for (auto& child : info.child_types) {
      if (child.containsAny())
        return true;
    }
    return false;
  }
  default:
    return false;
  }
}

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
    case ::common::PrimitiveType::DT_ANY:
      return DataType(DataTypeId::kUnknown);
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
    const auto& array_pb = ddt.array();
    const auto& element_type = array_pb.component_type();
    auto child_data_type = parse_from_data_type(element_type);
    uint32_t fixed_length = array_pb.fixed_length();
    if (fixed_length == 0) {
      THROW_RUNTIME_ERROR(
          "Array fixed_length must be greater than 0, but got 0 from "
          "protobuf deserialization.");
    }
    return DataType::Array(child_data_type, fixed_length);
  }
  case ::common::DataType::kList: {
    const auto& array_pb = ddt.list();
    const auto& element_type = array_pb.component_type();
    auto child_data_type = parse_from_data_type(element_type);
    return DataType::List(child_data_type);
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

DataType parse_graph_data_type_from_ir_data_type(
    const ::common::GraphDataType& gdt) {
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
}
DataType parse_from_ir_data_type(const ::common::IrDataType& dt) {
  switch (dt.type_case()) {
  case ::common::IrDataType::TypeCase::kDataType: {
    const ::common::DataType ddt = dt.data_type();
    return parse_from_data_type(ddt);
  }
  case ::common::IrDataType::TypeCase::kGraphType: {
    const ::common::GraphDataType gdt = dt.graph_type();
    return parse_graph_data_type_from_ir_data_type(gdt);
  }
  case ::common::IrDataType::TypeCase::kListType: {
    const ::common::GraphTypeList gdtl = dt.list_type();
    std::shared_ptr<ExtraTypeInfo> type_info = std::make_shared<ListTypeInfo>(
        parse_graph_data_type_from_ir_data_type(gdtl.component_type()));
    return DataType(DataTypeId::kList, type_info);
  }
  default:
    break;
  }
  return DataType(DataTypeId::kUnknown);
}

std::string DataType::ToString() const {
  switch (id_) {
  case DataTypeId::kInvalid:
    return "INVALID";
  case DataTypeId::kBoolean:
    return "BOOLEAN";
  case DataTypeId::kInt8:
    return "INT8";
  case DataTypeId::kInt16:
    return "INT16";
  case DataTypeId::kInt32:
    return "INT32";
  case DataTypeId::kInt64:
    return "INT64";
  case DataTypeId::kUInt8:
    return "UINT8";
  case DataTypeId::kUInt16:
    return "UINT16";
  case DataTypeId::kUInt32:
    return "UINT32";
  case DataTypeId::kUInt64:
    return "UINT64";
  case DataTypeId::kFloat:
    return "FLOAT";
  case DataTypeId::kDouble:
    return "DOUBLE";
  case DataTypeId::kVarchar:
    return "VARCHAR";
  case DataTypeId::kDate:
    return "DATE";
  case DataTypeId::kTimestampMs:
    return "TIMESTAMP_MS";
  case DataTypeId::kInterval:
    return "INTERVAL";
  case DataTypeId::kInternalId:
    return "INTERNAL_ID";
  case DataTypeId::kVertex:
    return "VERTEX";
  case DataTypeId::kEdge:
    return "EDGE";
  case DataTypeId::kPath:
    return "PATH";
  case DataTypeId::kList: {
    const DataType& child_type = ListType::GetChildType(*this);
    return "LIST<" + child_type.ToString() + ">";
  }
  case DataTypeId::kArray: {
    auto& arr_info = type_info_->Cast<ArrayTypeInfo>();
    return arr_info.child_type.ToString() + "[" +
           std::to_string(arr_info.num_elements) + "]";
  }
  case DataTypeId::kMap: {
    auto& map_info = type_info_->Cast<MapTypeInfo>();
    return "MAP(" + map_info.key_type.ToString() + ", " +
           map_info.value_type.ToString() + ")";
  }
  case DataTypeId::kStruct: {
    const auto& info = getStructInfo(*this);
    std::string result = "STRUCT(";
    for (size_t i = 0; i < info.child_types.size(); ++i) {
      if (!info.field_names.empty() && i < info.field_names.size()) {
        result += info.field_names[i] + ": ";
      }
      result += info.child_types[i].ToString();
      if (i != info.child_types.size() - 1) {
        result += ", ";
      }
    }
    result += ")";
    return result;
  }
  case DataTypeId::kNull:
    return "NULL";
  case DataTypeId::kUnknown:
    return "ANY";
  case DataTypeId::kEmpty:
    return "EMPTY";
  default:
    return "UNKNOWN" + std::to_string(static_cast<uint8_t>(id_));
  }
}

InArchive& operator<<(InArchive& in_archive, const DataType& type) {
  auto id = type.id();
  in_archive << id;
  auto type_info = type.getExtraTypeInfo();
  if (type_info) {
    in_archive << (char) 1;
    if (id == DataTypeId::kList) {
      const auto& list_type_info = type_info->Cast<ListTypeInfo>();
      in_archive << list_type_info.child_type;
    } else if (id == DataTypeId::kStruct) {
      const auto& struct_type_info = type_info->Cast<StructTypeInfo>();
      const auto& child_types = struct_type_info.child_types;
      in_archive << (size_t) child_types.size();
      for (const auto& child_type : child_types) {
        in_archive << child_type;
      }
    } else if (id == DataTypeId::kArray) {
      const auto& array_type_info = type_info->Cast<ArrayTypeInfo>();
      in_archive << array_type_info.child_type << array_type_info.num_elements;
    } else if (id == DataTypeId::kVarchar) {
      const auto& varchar_type_info = type_info->Cast<StringTypeInfo>();
      in_archive << varchar_type_info.max_length;
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "unsupported data type with extra type info - " + type.ToString());
    }
  } else {
    in_archive << (char) 0;  // indicate no extra type info
  }
  return in_archive;
}

OutArchive& operator>>(OutArchive& out_archive, DataType& type) {
  DataTypeId id;
  out_archive >> id;

  char has_extra_type_info;
  out_archive >> has_extra_type_info;
  if (has_extra_type_info) {
    if (id == DataTypeId::kList) {
      DataType child_type;
      out_archive >> child_type;
      type = DataType::List(child_type);
    } else if (id == DataTypeId::kStruct) {
      size_t child_types_size;
      out_archive >> child_types_size;
      std::vector<DataType> child_types(child_types_size);
      for (size_t i = 0; i < child_types_size; ++i) {
        out_archive >> child_types[i];
      }
      type = DataType::Struct(child_types);
    } else if (id == DataTypeId::kArray) {
      DataType child_type;
      uint64_t array_size;
      out_archive >> child_type >> array_size;
      type = DataType::Array(child_type, array_size);
    } else if (id == DataTypeId::kVarchar) {
      size_t max_length;
      out_archive >> max_length;
      type = DataType::Varchar(max_length);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "unsupported data type with extra type info - " + std::to_string(id));
    }
  } else {
    type = DataType(id);
  }

  return out_archive;
}

}  // namespace neug
