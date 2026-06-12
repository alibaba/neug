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
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

#include "neug/storages/checkpoint_manager.h"

namespace neug {

std::optional<ModuleDescriptor> CheckpointManifest::module(
    const std::string& key) const {
  auto it = modules_.find(key);
  if (it == modules_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void CheckpointManifest::set_module(const std::string& key,
                                    ModuleDescriptor desc) {
  modules_[key] = std::move(desc);
}

void CheckpointManifest::remove_module(const std::string& key) {
  modules_.erase(key);
}

bool CheckpointManifest::has_module(const std::string& key) const {
  return modules_.count(key) > 0;
}

const std::unordered_map<std::string, ModuleDescriptor>&
CheckpointManifest::modules() const {
  return modules_;
}

std::unordered_map<std::string, ModuleDescriptor>&
CheckpointManifest::mutable_modules() {
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

const std::unordered_map<std::string, std::string>&
CheckpointManifest::scalars() const {
  return scalars_;
}

void CheckpointManifest::Load(const std::string& file_path) {
  // CheckpointManifest lives at the canonical checkpoint meta path.
  std::ifstream ifs(file_path);
  if (!ifs.is_open()) {
    LOG(WARNING) << "CheckpointManifest::Load: cannot open " << file_path;
    return;
  }

  rapidjson::IStreamWrapper isw(ifs);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  if (doc.HasParseError() || !doc.IsObject()) {
    THROW_STORAGE_EXCEPTION("CheckpointManifest::Load: invalid JSON in " +
                            file_path);
  }

  if (!doc.HasMember("version") || !doc["version"].IsInt()) {
    THROW_STORAGE_EXCEPTION(
        "CheckpointManifest::Load: missing or non-integer 'version' in " +
        file_path);
  }
  int file_version = doc["version"].GetInt();
  if (file_version != kFormatVersion) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "CheckpointManifest::Load: incompatible meta version " +
        std::to_string(file_version) + " (expected " +
        std::to_string(kFormatVersion) + ") in " + file_path);
  }

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
  doc.AddMember("version", rapidjson::Value(kFormatVersion), alloc);

  auto schema_res = schema_.ToJson();
  if (!schema_res) {
    LOG(ERROR) << "CheckpointManifest::Save: failed to serialize schema: "
               << schema_res.error().error_message();
  } else {
    doc.AddMember("schema", schema_res.value().Move(), alloc);
  }

  rapidjson::Value modules_obj(rapidjson::kObjectType);
  for (const auto& [key, desc] : modules_) {
    rapidjson::Value key_val(
        key.c_str(), static_cast<rapidjson::SizeType>(key.size()), alloc);
    modules_obj.AddMember(key_val, desc.ToJson(alloc), alloc);
  }
  doc.AddMember("modules", modules_obj, alloc);

  rapidjson::Value scalars_obj(rapidjson::kObjectType);
  for (const auto& [key, value] : scalars_) {
    rapidjson::Value key_val(
        key.c_str(), static_cast<rapidjson::SizeType>(key.size()), alloc);
    rapidjson::Value value_val(
        value.c_str(), static_cast<rapidjson::SizeType>(value.size()), alloc);
    scalars_obj.AddMember(key_val, value_val, alloc);
  }
  doc.AddMember("scalars", scalars_obj, alloc);

  rapidjson::OStreamWrapper osw(os);
  rapidjson::Writer<rapidjson::OStreamWrapper> json_writer(osw);
  doc.Accept(json_writer);

  writer.Commit();
  LOG(INFO) << "CheckpointManifest::Save: dumped meta to " << file_path;
}

void CheckpointManifest::GenerateEmptyMeta(const std::string& path) {
  file_utils::AtomicFileWriter writer(path);
  auto& os = writer.stream();

  rapidjson::OStreamWrapper osw(os);
  rapidjson::Writer<rapidjson::OStreamWrapper> json_writer(osw);
  rapidjson::Document doc;
  doc.SetObject();
  auto& alloc = doc.GetAllocator();
  doc.AddMember("version", rapidjson::Value(1), alloc);
  doc.AddMember("modules", rapidjson::Value(rapidjson::kObjectType), alloc);
  doc.AddMember("scalars", rapidjson::Value(rapidjson::kObjectType), alloc);
  doc.Accept(json_writer);

  writer.Commit();
}

const Schema& CheckpointManifest::GetSchema() const { return schema_; }

void CheckpointManifest::SetSchema(const Schema& schema) {
  schema_ = schema;
  has_schema_ = true;
}

}  // namespace neug
