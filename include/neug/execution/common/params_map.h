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
#include <map>
#include <string>

#include <rapidjson/fwd.h>
#include "neug/common/types/value.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {
using ParamsMap = std::map<std::string, Value>;
using ParamsMetaMap = std::map<std::string, DataType>;

// Parses a JSON object of query parameters into a ParamsMap, converting
// each member with its declared DataType. Unknown keys are logged and ignored.
// Declared here (defined in params_map.cc) so this widely-included header
// does not carry the implementation or extra rapidjson dependencies.
result<ParamsMap> parseJsonParameters(const ParamsMetaMap& parameter_types,
                                      const rapidjson::Value& parameters);
}  // namespace execution
}  // namespace neug
