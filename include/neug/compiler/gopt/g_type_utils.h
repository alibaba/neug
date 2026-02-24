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
namespace common {
class LogicalType;
class Serializer;
}  // namespace common
const static uint32_t VARCHAR_DEFAULT_LENGTH = 65536;

class VarcharExtraInfo : public neug::common::ExtraTypeInfo {
 private:
  uint64_t maxLength;

 protected:
  void serializeInternal(neug::common::Serializer& serializer) const override {}

 public:
  explicit VarcharExtraInfo(uint64_t maxLength) : maxLength{maxLength} {}
  ~VarcharExtraInfo() override = default;
  bool containsAny() const override { return false; }

  bool operator==(const ExtraTypeInfo& other) const override {
    return maxLength == other.constPtrCast<VarcharExtraInfo>()->maxLength;
  }

  std::unique_ptr<ExtraTypeInfo> copy() const override {
    return std::make_unique<VarcharExtraInfo>(maxLength);
  }
};

class GTypeUtils {
 public:
  static inline neug::common::LogicalType createLogicalType(YAML::Node& node) {
    if (common::LogicalTypeRegistry::containsTypeYaml(node)) {
      auto typeID = common::LogicalTypeRegistry::getTypeID(node);
      return neug::common::LogicalType(typeID);
    }

    auto stringType = node["string"];
    if (stringType) {
      // denote varchar
      if (stringType["var_char"] || stringType["long_text"]) {
        return neug::common::LogicalType(neug::common::LogicalTypeID::STRING);
      }
    }
    auto temporalType = node["temporal"];
    if (temporalType && temporalType.IsMap()) {
      if (temporalType["date32"].IsDefined()) {
        return neug::common::LogicalType(neug::common::LogicalTypeID::DATE32);
      } else if (temporalType["timestamp"].IsDefined()) {
        return neug::common::LogicalType(
            neug::common::LogicalTypeID::TIMESTAMP64);
      } else if (temporalType["date"].IsDefined()) {
        return neug::common::LogicalType(neug::common::LogicalTypeID::DATE);
      } else if (temporalType["datetime"].IsDefined()) {
        return neug::common::LogicalType(
            neug::common::LogicalTypeID::TIMESTAMP);
      } else if (temporalType["interval"].IsDefined()) {
        return neug::common::LogicalType(neug::common::LogicalTypeID::INTERVAL);
      } else {
        THROW_RUNTIME_ERROR("Unsupported temporal type in YAML: " +
                            node.as<std::string>());
      }
    }
    auto arrayType = node["array"];
    if (arrayType && arrayType.IsMap()) {
      auto componentType = arrayType["component_type"];
      CHECK(componentType.IsDefined())
          << "component type is undefined in array: " << componentType;
      return neug::common::LogicalType::LIST(createLogicalType(componentType));
    }
    THROW_RUNTIME_ERROR("Unsupported type in YAML: " + node.as<std::string>());
  }

  static inline YAML::Node toYAML(const neug::common::LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case neug::common::LogicalTypeID::INT64:
      return YAML_NODE_DT_SIGNED_INT64;
    case neug::common::LogicalTypeID::UINT64:
      return YAML_NODE_DT_UNSIGNED_INT64;
    case neug::common::LogicalTypeID::INT32:
      return YAML_NODE_DT_SIGNED_INT32;
    case neug::common::LogicalTypeID::UINT32:
      return YAML_NODE_DT_UNSIGNED_INT32;
    case neug::common::LogicalTypeID::FLOAT:
      return YAML_NODE_DT_FLOAT;
    case neug::common::LogicalTypeID::DOUBLE:
      return YAML_NODE_DT_DOUBLE;
    case neug::common::LogicalTypeID::BOOL:
      return YAML_NODE_DT_BOOL;
    case neug::common::LogicalTypeID::STRING:
      return YAML_NODE_STRING_VARCHAR(neug::Constants::VARCHAR_MAX_LENGTH);
    case neug::common::LogicalTypeID::DATE32:
      return YAML_NODE_TEMPORAL_DATE32();
    case neug::common::LogicalTypeID::TIMESTAMP64:
      return YAML_NODE_TEMPORAL_TIMESTAMP64();
    case neug::common::LogicalTypeID::DATE:
      return YAML_NODE_TEMPORAL_DATE();
    case neug::common::LogicalTypeID::TIMESTAMP:
      return YAML_NODE_TEMPORAL_DATETIME();
    case neug::common::LogicalTypeID::INTERVAL:
      return YAML_NODE_TEMPORAL_INTERVAL();
    default:
      LOG(WARNING) << "Unsupported type in YAML: "
                   << static_cast<uint8_t>(type.getLogicalTypeID());
      return YAML_NODE_DT_ANY;
    }
  }
};
}  // namespace neug