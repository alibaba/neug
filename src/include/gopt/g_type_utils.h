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
#include "src/include/catalog/catalog.h"
#include "src/include/common/serializer/serializer.h"
#include "src/include/common/types/types.h"
#include "src/include/gopt/g_constants.h"
#include "src/include/gopt/g_macro.h"
#include "src/include/gopt/g_type_registry.h"

#include <glog/logging.h>
#include <cstdint>

namespace gs {
namespace common {
class LogicalType;
class Serializer;
}  // namespace common
const static uint32_t VARCHAR_DEFAULT_LENGTH = 65536;

class VarcharExtraInfo : public gs::common::ExtraTypeInfo {
 private:
  uint64_t maxLength;

 protected:
  void serializeInternal(gs::common::Serializer& serializer) const override {}

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
  static inline gs::common::LogicalType createLogicalType(YAML::Node& node) {
    if (common::LogicalTypeRegistry::containsTypeYaml(node)) {
      auto typeID = common::LogicalTypeRegistry::getTypeID(node);
      return gs::common::LogicalType(typeID);
    }

    auto stringType = node["string"];
    if (stringType) {
      // denote varchar
      if (stringType["var_char"] || stringType["long_text"]) {
        return gs::common::LogicalType(gs::common::LogicalTypeID::STRING);
      }
    }
    auto temporalType = node["temporal"];
    if (temporalType && temporalType.IsMap()) {
      if (temporalType["date32"].IsDefined()) {
        return gs::common::LogicalType(gs::common::LogicalTypeID::DATE32);
      } else if (temporalType["timestamp"].IsDefined()) {
        return gs::common::LogicalType(gs::common::LogicalTypeID::TIMESTAMP64);
      } else if (temporalType["date"].IsDefined()) {
        return gs::common::LogicalType(gs::common::LogicalTypeID::DATE);
      } else if (temporalType["datetime"].IsDefined()) {
        return gs::common::LogicalType(gs::common::LogicalTypeID::TIMESTAMP);
      } else if (temporalType["interval"].IsDefined()) {
        return gs::common::LogicalType(gs::common::LogicalTypeID::INTERVAL);
      } else {
        // Print yaml node
        LOG(ERROR) << "Unsupported temporal type in YAML: " << node["temporal"];
        throw std::runtime_error("Unsupported temporal type in YAML");
      }
    }
    LOG(WARNING) << "Unsupported type in YAML: " << node;
    return gs::common::LogicalType(gs::common::LogicalTypeID::ANY);
  }

  static inline YAML::Node toYAML(const gs::common::LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case gs::common::LogicalTypeID::INT64:
      return YAML_NODE_DT_SIGNED_INT64;
    case gs::common::LogicalTypeID::UINT64:
      return YAML_NODE_DT_UNSIGNED_INT64;
    case gs::common::LogicalTypeID::INT32:
      return YAML_NODE_DT_SIGNED_INT32;
    case gs::common::LogicalTypeID::UINT32:
      return YAML_NODE_DT_UNSIGNED_INT32;
    case gs::common::LogicalTypeID::FLOAT:
      return YAML_NODE_DT_FLOAT;
    case gs::common::LogicalTypeID::DOUBLE:
      return YAML_NODE_DT_DOUBLE;
    case gs::common::LogicalTypeID::BOOL:
      return YAML_NODE_DT_BOOL;
    case gs::common::LogicalTypeID::STRING:
      return YAML_NODE_STRING_VARCHAR(gs::Constants::VARCHAR_MAX_LENGTH);
    case gs::common::LogicalTypeID::DATE32:
      return YAML_NODE_TEMPORAL_DATE32();
    case gs::common::LogicalTypeID::TIMESTAMP64:
      return YAML_NODE_TEMPORAL_TIMESTAMP64();
    case gs::common::LogicalTypeID::DATE:
      return YAML_NODE_TEMPORAL_DATE();
    case gs::common::LogicalTypeID::TIMESTAMP:
      return YAML_NODE_TEMPORAL_DATETIME();
    case gs::common::LogicalTypeID::INTERVAL:
      return YAML_NODE_TEMPORAL_INTERVAL();
    default:
      LOG(WARNING) << "Unsupported type in YAML: "
                   << static_cast<uint8_t>(type.getLogicalTypeID());
      return YAML_NODE_DT_ANY;
    }
  }
};
}  // namespace gs