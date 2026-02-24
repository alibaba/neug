/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <yaml-cpp/node/emit.h>
#include <yaml-cpp/yaml.h>
#include "neug/compiler/common/types/types.h"

namespace neug {
namespace common {
class LogicalTypeRegistry {
 public:
  LogicalTypeRegistry() {}
  static void registerType(const YAML::Node& typeYaml,
                           const common::LogicalTypeID typeID) {
    auto yamlStr = YAML::Dump(typeYaml);
    yamlToIDMap()[yamlStr] = typeID;
  }

  static common::LogicalTypeID& getTypeID(const YAML::Node& typeYaml) {
    auto yamlStr = YAML::Dump(typeYaml);
    return yamlToIDMap()[yamlStr];
  }

  static bool containsTypeYaml(const YAML::Node& typeYaml) {
    auto yamlStr = YAML::Dump(typeYaml);
    return yamlToIDMap().find(yamlStr) != yamlToIDMap().end();
  }

 private:
  static std::unordered_map<std::string, common::LogicalTypeID>& yamlToIDMap() {
    static std::unordered_map<std::string, common::LogicalTypeID> map;
    return map;
  }
};
}  // namespace common
}  // namespace neug
