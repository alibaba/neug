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
#include <ostream>
#include <string>
#include <vector>
#include "glog/logging.h"

#include "neug/common/types.h"
#include "neug/utils/result.h"

namespace YAML {
class Emitter;
}  // namespace YAML

namespace neug {

enum class DataTypeId : uint8_t;

YAML::Node property_type_to_yaml(const DataType& property_type);

std::vector<std::string> get_yaml_files(const std::string& plugin_dir);

result<std::string> get_json_string_from_yaml(const std::string& file_path);

result<std::string> get_json_string_from_yaml(const YAML::Node& yaml_node);

Status write_yaml_node_to_yaml_string(const YAML::Node& node,
                                      YAML::Emitter& emitter);

result<std::string> get_yaml_string_from_yaml_node(const YAML::Node& node);

std::string read_yaml_file_to_string(const std::string& file_path);

bool write_yaml_file(const YAML::Node& node, const std::string& file_path);

namespace config_parsing {
template <typename T>
bool get_scalar(YAML::Node node, const std::string& key, T& value) {
  YAML::Node cur = node[key];
  if (cur && cur.IsScalar()) {
    value = cur.as<T>();
    return true;
  }
  return false;
}

template <typename T>
bool get_sequence(YAML::Node node, const std::string& key,
                  std::vector<T>& seq) {
  YAML::Node cur = node[key];
  if (cur && cur.IsSequence()) {
    int num = cur.size();
    seq.clear();
    for (int i = 0; i < num; ++i) {
      seq.push_back(cur[i].as<T>());
    }
    return true;
  }
  return false;
}

template <typename V>
static bool expect_config(YAML::Node root, const std::string& key,
                          const V& value) {
  V got;
  if (!get_scalar(root, key, got)) {
    LOG(ERROR) << "Expect key: " << key << " set to " << value
               << " but not set";
    return false;
  }
  if (got != value) {
    LOG(ERROR) << "Expect key: " << key << " set to " << value << " but got "
               << got;
    return false;
  }
  return true;
}

}  // namespace config_parsing
}  // namespace neug
