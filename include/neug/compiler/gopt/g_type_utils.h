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

#pragma once

#include <yaml-cpp/node/node.h>
#include <yaml-cpp/yaml.h>
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_macro.h"
#include "neug/compiler/gopt/g_type_registry.h"

#include <glog/logging.h>
#include <cstdint>

namespace neug {
const static uint32_t VARCHAR_DEFAULT_LENGTH = 65536;

class GTypeUtils {
 public:
  static inline neug::common::DataType createLogicalType(YAML::Node& node) {
    if (common::LogicalTypeRegistry::containsTypeYaml(node)) {
      auto typeID = common::LogicalTypeRegistry::getTypeID(node);
      return neug::common::DataType(typeID);
    }

    auto stringType = node["string"];
    if (stringType) {
      // denote varchar
      if (stringType["var_char"]) {
        auto varChar = stringType["var_char"];
        auto maxLength = varChar["max_length"];
        if (maxLength && maxLength.IsScalar()) {
          return neug::common::DataType::Varchar(maxLength.as<uint64_t>());
        } else {
          return neug::common::DataType::Varchar();
        }
      } else if (stringType["long_text"]) {
        return neug::common::DataType::Varchar();
      }
    }
    auto temporalType = node["temporal"];
    if (temporalType && temporalType.IsMap()) {
      if (temporalType["date32"].IsDefined()) {
        return neug::common::DataType(neug::common::DataTypeId::kDate);
      } else if (temporalType["timestamp"].IsDefined()) {
        return neug::common::DataType(neug::common::DataTypeId::kTimestampMs);
      } else if (temporalType["date"].IsDefined()) {
        return neug::common::DataType(neug::common::DataTypeId::kDate);
      } else if (temporalType["datetime"].IsDefined()) {
        return neug::common::DataType(neug::common::DataTypeId::kTimestampMs);
      } else if (temporalType["interval"].IsDefined()) {
        return neug::common::DataType(neug::common::DataTypeId::kInterval);
      } else {
        THROW_RUNTIME_ERROR("Unsupported temporal type in YAML: " +
                            node.as<std::string>());
      }
    }
    auto arrayType = node["array"];
    if (arrayType && arrayType.IsMap()) {
      auto componentType = arrayType["component_type"];
      CHECK(componentType.IsDefined())
          << "component type is undefined in array: " << arrayType;
      auto fixedLength = arrayType["max_length"];
      CHECK(fixedLength.IsDefined())
          << "fixed length is undefined in array: " << arrayType;
      return neug::common::DataType::Array(createLogicalType(componentType),
                                           fixedLength.as<uint64_t>());
    }
    auto listType = node["list"];
    if (listType && listType.IsMap()) {
      auto componentType = listType["component_type"];
      CHECK(componentType.IsDefined())
          << "component type is undefined in list: " << listType;
      return neug::common::DataType::List(createLogicalType(componentType));
    }
    THROW_RUNTIME_ERROR("Unsupported type in YAML: " + node.as<std::string>());
  }

  static inline YAML::Node toYAML(const neug::common::DataType& type) {
    switch (type.id()) {
    case neug::common::DataTypeId::kInt64:
      return YAML_NODE_DT_SIGNED_INT64;
    case neug::common::DataTypeId::kUInt64:
      return YAML_NODE_DT_UNSIGNED_INT64;
    case neug::common::DataTypeId::kInt32:
      return YAML_NODE_DT_SIGNED_INT32;
    case neug::common::DataTypeId::kUInt32:
      return YAML_NODE_DT_UNSIGNED_INT32;
    case neug::common::DataTypeId::kFloat:
      return YAML_NODE_DT_FLOAT;
    case neug::common::DataTypeId::kDouble:
      return YAML_NODE_DT_DOUBLE;
    case neug::common::DataTypeId::kBoolean:
      return YAML_NODE_DT_BOOL;
    case neug::common::DataTypeId::kVarchar: {
      size_t maxLen = VARCHAR_DEFAULT_LENGTH;
      auto extraInfo = type.getExtraTypeInfo();
      if (extraInfo) {
        auto& stringTypeInfo = extraInfo->Cast<neug::StringTypeInfo>();
        maxLen = stringTypeInfo.max_length;
      }
      YAML::Node n;
      n["string"]["var_char"]["max_length"] = maxLen;
      return n;
    }
    case neug::common::DataTypeId::kDate:
      return YAML_NODE_TEMPORAL_DATE32();
    case neug::common::DataTypeId::kTimestampMs:
      return YAML_NODE_TEMPORAL_TIMESTAMP64();
    case neug::common::DataTypeId::kInterval:
      return YAML_NODE_TEMPORAL_INTERVAL();
    case neug::common::DataTypeId::kList: {
      YAML::Node n;
      n["list"]["component_type"] =
          toYAML(common::ListType::GetChildType(type));
      return n;
    }
    case neug::common::DataTypeId::kArray: {
      YAML::Node n;
      n["array"]["component_type"] =
          toYAML(common::ArrayType::GetChildType(type));
      n["array"]["max_length"] = common::ArrayType::GetNumElements(type);
      return n;
    }
    default:
      LOG(WARNING) << "Unsupported type in YAML: "
                   << static_cast<uint8_t>(type.id());
      return YAML_NODE_DT_ANY;
    }
  }
};
}  // namespace neug
