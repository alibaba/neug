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
#include <glog/logging.h>

#include <map>
#include <string>

#include "neug/common/types/value.h"

namespace neug {

struct DataType;
namespace execution {
using ParamsMap = std::map<std::string, Value>;
using ParamsMetaMap = std::map<std::string, DataType>;

inline ParamsMap build_params_map(const rapidjson::Value& parameters,
                                  const ParamsMetaMap& params_type) {
  ParamsMap params_map;
  if (!parameters.IsObject()) {
    return params_map;
  }

  for (const auto& member : parameters.GetObject()) {
    std::string key = member.name.GetString();
    auto iter = params_type.find(key);
    if (iter == params_type.end()) {
      LOG(WARNING) << "Ignoring unexpected parameter: " << key;
      continue;
    }
    params_map.emplace(key, Value::FromJson(member.value, iter->second));
  }
  return params_map;
}
}  // namespace execution
}  // namespace neug
