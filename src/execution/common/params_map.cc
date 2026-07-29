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

#include "neug/execution/common/params_map.h"

#include "rapidjson/document.h"

namespace neug {
namespace execution {

result<ParamsMap> parseJsonParameters(const ParamsMetaMap& parameter_types,
                                      const rapidjson::Value& parameters) {
  ParamsMap params;
  if (!parameters.IsObject()) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Query parameters must be a JSON object."));
  }
  for (const auto& member : parameters.GetObject()) {
    std::string key = member.name.GetString();
    auto iter = parameter_types.find(key);
    if (iter == parameter_types.end()) {
      VLOG(1) << "Parameter key not found in meta: " << key;
      continue;
    }
    params.emplace(key, Value::FromJson(member.value, iter->second));
  }
  return params;
}

}  // namespace execution
}  // namespace neug
