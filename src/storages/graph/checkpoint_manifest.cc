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

#include "neug/storages/checkpoint_manifest.h"

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"

#include <fstream>
#include <string>

#include <glog/logging.h>

#ifdef _WIN32
// Windows headers may define GetObject as GetObjectA/GetObjectW, which
// conflicts with rapidjson::Value::GetObject(). Undefine it before including
// rapidjson headers.
#ifdef GetObject
#undef GetObject
#endif
#endif

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

namespace neug {

const ModuleDescriptor* CheckpointManifest::FindModule(
    const std::string& key) const {
  auto it = modules_.find(key);
  if (it == modules_.end()) {
    return nullptr;
  }
  return &it->second;
}

ModuleDescriptor* CheckpointManifest::FindMutableModule(
    const std::string& key) {
  auto it = modules_.find(key);
  return it == modules_.end() ? nullptr : &it->second;
}

void CheckpointManifest::SetModule(const std::string& key,
                                   ModuleDescriptor desc) {
  modules_[key] = std::move(desc);
}

bool CheckpointManifest::HasModule(const std::string& key) const {
  return modules_.count(key) > 0;
}

void CheckpointManifest::ReuseModuleClosureFrom(const CheckpointManifest& prev,
                                                const std::string& key) {
  std::unordered_set<std::string> visited;
  ReuseModuleClosureFromImpl(prev, key, visited);
}

void CheckpointManifest::ReuseModuleClosureFromImpl(
    const CheckpointManifest& prev, const std::string& key,
    std::unordered_set<std::string>& visited) {
  if (!visited.insert(key).second) {
    return;
  }
  const auto* desc = prev.FindModule(key);
  if (desc == nullptr) {
    return;
  }
  if (!HasModule(key)) {
    SetModule(key, *desc);
  }
  for (const auto& [_, referenced_key] : desc->refs()) {
    ReuseModuleClosureFromImpl(prev, referenced_key, visited);
  }
}

const std::unordered_map<std::string, ModuleDescriptor>&
CheckpointManifest::Modules() const {
  return modules_;
}

std::optional<std::string> CheckpointManifest::GetScalar(
    const std::string& key) const {
  auto it = scalars_.find(key);
  if (it == scalars_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void CheckpointManifest::SetScalar(std::string key, std::string value) {
  scalars_[std::move(key)] = std::move(value);
}

void CheckpointManifest::CopyScalarFrom(const CheckpointManifest& prev,
                                        const std::string& key) {
  if (auto value = prev.GetScalar(key)) {
    SetScalar(key, *value);
  }
}

void CheckpointManifest::Load(const std::string& file_path) {
  // CheckpointManifest lives at the canonical manifest path.
  std::ifstream ifs(file_path);
  if (!ifs.is_open()) {
    THROW_STORAGE_EXCEPTION("CheckpointManifest::Load: cannot open " +
                            file_path);
  }

  rapidjson::IStreamWrapper isw(ifs);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  if (doc.HasParseError() || !doc.IsObject()) {
    THROW_STORAGE_EXCEPTION("CheckpointManifest::Load: invalid JSON in " +
                            file_path);
  }

  if (!doc.HasMember("v") || !doc["v"].IsInt()) {
    THROW_STORAGE_EXCEPTION(
        "CheckpointManifest::Load: missing or non-integer 'v' in " + file_path);
  }
  int file_version = doc["v"].GetInt();
  if (file_version != kFormatVersion) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "CheckpointManifest::Load: incompatible manifest version " +
        std::to_string(file_version) + " (expected " +
        std::to_string(kFormatVersion) + ") in " + file_path);
  }

  if (!doc.HasMember("base_ts") || !doc["base_ts"].IsUint64()) {
    THROW_STORAGE_EXCEPTION(
        "CheckpointManifest::Load: missing or invalid "
        "'base_ts' in " +
        file_path);
  }
  base_timestamp_ = doc["base_ts"].GetUint64();

  if (doc.HasMember("schema") && doc["schema"].IsObject()) {
    schema_.FromJson(doc["schema"].GetObject());
    has_schema_ = true;
  } else {
    has_schema_ = false;
  }

  modules_.clear();
  if (doc.HasMember("modules") && doc["modules"].IsObject()) {
    for (auto& kv : doc["modules"].GetObject()) {
      if (kv.value.IsObject()) {
        modules_[kv.name.GetString()] = ModuleDescriptor::FromJson(kv.value);
      }
    }
  }

  scalars_.clear();
  if (doc.HasMember("scalars") && doc["scalars"].IsObject()) {
    for (auto& kv : doc["scalars"].GetObject()) {
      if (kv.value.IsString()) {
        scalars_[kv.name.GetString()] = kv.value.GetString();
      }
    }
  }
}

void CheckpointManifest::Save(const std::string& file_path) const {
  file_utils::AtomicFileWriter writer(file_path);
  auto& os = writer.stream();

  rapidjson::Document doc;
  doc.SetObject();
  auto& alloc = doc.GetAllocator();
  doc.AddMember("v", rapidjson::Value(kFormatVersion), alloc);
  doc.AddMember("base_ts", rapidjson::Value(base_timestamp_), alloc);

  auto schema_res = schema_.ToJson();
  if (!schema_res) {
    THROW_STORAGE_EXCEPTION(
        "CheckpointManifest::Save: failed to serialize schema: " +
        schema_res.error().error_message());
  }
  doc.AddMember("schema", schema_res.value().Move(), alloc);

  rapidjson::Value modules_obj(rapidjson::kObjectType);
  for (const auto& [key, desc] : modules_) {
    rapidjson::Value key_val(
        key.c_str(), static_cast<rapidjson::SizeType>(key.size()), alloc);
    modules_obj.AddMember(key_val, desc.ToJson(alloc), alloc);
  }
  doc.AddMember("modules", modules_obj, alloc);

  if (!scalars_.empty()) {
    rapidjson::Value scalars_obj(rapidjson::kObjectType);
    for (const auto& [key, value] : scalars_) {
      rapidjson::Value key_val(
          key.c_str(), static_cast<rapidjson::SizeType>(key.size()), alloc);
      rapidjson::Value value_val(
          value.c_str(), static_cast<rapidjson::SizeType>(value.size()), alloc);
      scalars_obj.AddMember(key_val, value_val, alloc);
    }
    doc.AddMember("scalars", scalars_obj, alloc);
  }

  rapidjson::OStreamWrapper osw(os);
  rapidjson::Writer<rapidjson::OStreamWrapper> json_writer(osw);
  doc.Accept(json_writer);

  if (writer.Commit() != file_utils::AtomicFileWriter::CommitResult::kDurable) {
    THROW_IO_EXCEPTION(
        "CheckpointManifest::Save: manifest replacement is commit-unknown: " +
        file_path);
  }
  LOG(INFO) << "CheckpointManifest::Save: wrote " << file_path;
}

const Schema& CheckpointManifest::GetSchema() const { return schema_; }

void CheckpointManifest::SetSchema(const Schema& schema) {
  schema_ = schema;
  has_schema_ = true;
}

}  // namespace neug
