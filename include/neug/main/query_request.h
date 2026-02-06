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

#include "neug/execution/common/params_map.h"
#include "neug/execution/common/types/value.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/result.h"

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

namespace neug {
struct ParamsParser {
  static execution::ParamsMap ParseFromJsonObj(
      const execution::ParamsMetaMap& meta,
      const rapidjson::Document& param_json_obj);
};

struct RequestParser {
  // TODO(zhanglei): Here we use rapidjson::Document for parameters,
  // figure out better way to pass parameters later.
  static Status ParseFromString(const std::string& req_string,
                                std::string& query, AccessMode& mode,
                                rapidjson::Document& parameters);
};

struct RequestSerializer {
  static std::string SerializeRequest(const std::string& query,
                                      const std::string& mode,
                                      const execution::ParamsMap& parameters);
};

}  // namespace neug
