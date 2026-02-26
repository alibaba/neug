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

#include "neug/main/query_request.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

#include "neug/execution/common/types/value.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace neug {

execution::ParamsMap ParamsParser::ParseFromJsonObj(
    const execution::ParamsMetaMap& meta,
    const rapidjson::Document& param_json_obj) {
  execution::ParamsMap param_map;
  if (!param_json_obj.IsObject()) {
    return param_map;
  }
  for (auto itr = param_json_obj.MemberBegin();
       itr != param_json_obj.MemberEnd(); ++itr) {
    auto key = itr->name.GetString();
    if (meta.count(key) <= 0) {
      VLOG(1) << "Parameter key not found in meta: " << key;
    } else {
      param_map.emplace(key,
                        execution::Value::FromJson(itr->value, meta.at(key)));
    }
  }
  return param_map;
}

neug::Status RequestParser::ParseFromString(const std::string& req,
                                            std::string& query,
                                            AccessMode& mode,
                                            rapidjson::Document& parameters) {
  rapidjson::Document document;
  document.Parse(req.c_str(), req.size());
  if (document.HasParseError()) {
    LOG(ERROR) << "The format of eval request is incorrect.";
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "The format of eval request is incorrect.");
  }
  if (document.HasMember("query") && document["query"].IsString()) {
    query = document["query"].GetString();
  }
  std::string access_mode_str;
  if (document.HasMember("access_mode") && document["access_mode"].IsString()) {
    access_mode_str = document["access_mode"].GetString();
    mode = neug::ParseAccessMode(access_mode_str);
  }
  if (document.HasMember("parameters") && document["parameters"].IsObject()) {
    parameters.CopyFrom(document["parameters"], parameters.GetAllocator());
  }
  return neug::Status::OK();
}

std::string RequestSerializer::SerializeRequest(
    const std::string& query, const std::string& mode,
    const execution::ParamsMap& parameters) {
  rapidjson::Document document(rapidjson::kObjectType);
  rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
  document.AddMember(
      "query",
      rapidjson::Value().SetString(query.c_str(), query.size(), allocator),
      allocator);
  auto access_mode_str = neug::AccessModeToString(neug::ParseAccessMode(mode));
  document.AddMember(
      "access_mode",
      rapidjson::Value().SetString(access_mode_str.c_str(),
                                   access_mode_str.size(), allocator),
      allocator);
  rapidjson::Document parameter_obj(rapidjson::kObjectType);
  for (const auto& kv : parameters) {
    parameter_obj.AddMember(
        rapidjson::Value(kv.first.c_str(), kv.first.size(), allocator),
        execution::Value::ToJson(kv.second, allocator), allocator);
  }
  document.AddMember("parameters", parameter_obj, allocator);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  document.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetSize());
}

}  // namespace neug
