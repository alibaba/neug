#pragma once

#include <yaml-cpp/node/emit.h>
#include <yaml-cpp/yaml.h>
#include "neug/compiler/common/types/types.h"

namespace gs {
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
}  // namespace gs
