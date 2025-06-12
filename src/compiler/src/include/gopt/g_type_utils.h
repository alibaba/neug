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

#include <yaml-cpp/yaml.h>
#include "common/serializer/serializer.h"
#include "common/types/types.h"
#include "gopt/g_constants.h"
#include "gopt/g_type_converter.h"
#include "src/proto_generated_gie/basic_type.pb.h"

#include <glog/logging.h>

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
    // Implementation of createLogicalType
    // This function should parse the YAML node and create a LogicalType object.
    // The actual implementation is not provided in the original code snippet.
    auto primitiveType = node["primitive_type"];
    if (primitiveType) {
      auto typeStr = primitiveType.as<std::string>();
      if (typeStr == "DT_SIGNED_INT64") {
        return gs::common::LogicalType(gs::common::LogicalTypeID::INT64);
      } else if (typeStr == "DT_UNSIGNED_INT64") {
        return gs::common::LogicalType(gs::common::LogicalTypeID::UINT64);
      } else if (typeStr == "DT_SIGNED_INT32") {
        return gs::common::LogicalType(gs::common::LogicalTypeID::INT32);
      } else if (typeStr == "DT_UNSIGNED_INT32") {
        return gs::common::LogicalType(gs::common::LogicalTypeID::UINT32);
      } else if (typeStr == "DT_FLOAT") {
        return gs::common::LogicalType(gs::common::LogicalTypeID::FLOAT);
      } else if (typeStr == "DT_DOUBLE") {
        return gs::common::LogicalType(gs::common::LogicalTypeID::DOUBLE);
      } else if (typeStr == "DT_BOOL") {
        return gs::common::LogicalType(gs::common::LogicalTypeID::BOOL);
      }
    }
    auto stringType = node["string"];
    if (stringType) {
      // denote varchar
      if (stringType["var_char"] || stringType["long_text"]) {
        return gs::common::LogicalType(gs::common::LogicalTypeID::STRING);
      }
    }
    if (node["temporal"]) {
      return gs::common::LogicalType(gs::common::LogicalTypeID::INT64);
    }
    LOG(WARNING) << "Unsupported type in YAML: " << node;
    return gs::common::LogicalType(gs::common::LogicalTypeID::ANY);
  }
};
}  // namespace gs