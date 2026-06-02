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

#include "neug/storages/snapshot_meta.h"

#include "neug/utils/file_utils.h"

#include <fstream>
#include <string>

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

namespace neug {

std::optional<ModuleDescriptor> SnapshotMeta::module(
    const std::string& key) const {
  auto it = modules_.find(key);
  if (it == modules_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void SnapshotMeta::set_module(const std::string& key, ModuleDescriptor desc) {
  modules_[key] = std::move(desc);
}

void SnapshotMeta::remove_module(const std::string& key) {
  modules_.erase(key);
}

bool SnapshotMeta::has_module(const std::string& key) const {
  return modules_.count(key) > 0;
}

const std::unordered_map<std::string, ModuleDescriptor>& SnapshotMeta::modules()
    const {
  return modules_;
}

std::unordered_map<std::string, ModuleDescriptor>&
SnapshotMeta::mutable_modules() {
  return modules_;
}

std::optional<std::string> SnapshotMeta::GetScalar(
    const std::string& key) const {
  auto it = scalars_.find(key);
  if (it == scalars_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void SnapshotMeta::SetScalar(std::string key, std::string value) {
  scalars_[std::move(key)] = std::move(value);
}

const std::unordered_map<std::string, std::string>& SnapshotMeta::scalars()
    const {
  return scalars_;
}

void SnapshotMeta::Open(const std::string& file_path) {
  // SnapshotMeta lives at the canonical checkpoint meta path.
  std::ifstream ifs(file_path);
  if (!ifs.is_open()) {
    LOG(WARNING) << "SnapshotMeta::Open: cannot open " << file_path;
    return;
  }

  rapidjson::IStreamWrapper isw(ifs);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  if (doc.HasParseError() || !doc.IsObject()) {
    LOG(ERROR) << "SnapshotMeta::Open: invalid JSON in " << file_path;
    return;
  }

  if (!doc.HasMember("version") || !doc["version"].IsInt()) {
    LOG(ERROR) << "SnapshotMeta::Open: missing or non-integer 'version' in "
               << file_path;
    return;
  }
  int file_version = doc["version"].GetInt();
  if (file_version != kFormatVersion) {
    LOG(ERROR) << "SnapshotMeta::Open: incompatible meta version "
               << file_version << " (expected " << kFormatVersion << ") in "
               << file_path;
    return;
  }

  if (doc.HasMember("schema") && doc["schema"].IsObject()) {
    schema_.FromJson(doc["schema"].GetObject());
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

void SnapshotMeta::Dump(const std::string& file_path) const {
  file_utils::atomic_write_file(file_path, [this](std::ostream& os) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    doc.AddMember("version", rapidjson::Value(kFormatVersion), alloc);

    auto schema_res = schema_.ToJson();
    if (!schema_res) {
      LOG(ERROR) << "SnapshotMeta::Dump: failed to serialize schema: "
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
    rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
    doc.Accept(writer);
  });
  LOG(INFO) << "SnapshotMeta::Dump: dumped meta to " << file_path;
}

void SnapshotMeta::GenerateEmptyMeta(const std::string& path) {
  file_utils::atomic_write_file(path, [](std::ostream& os) {
    rapidjson::OStreamWrapper osw(os);
    rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();
    doc.AddMember("version", rapidjson::Value(kFormatVersion), alloc);
    doc.AddMember("modules", rapidjson::Value(rapidjson::kObjectType), alloc);
    doc.AddMember("scalars", rapidjson::Value(rapidjson::kObjectType), alloc);
    doc.Accept(writer);
  });
}

const Schema& SnapshotMeta::GetSchema() const { return schema_; }

void SnapshotMeta::SetSchema(const Schema& schema) { schema_ = schema; }

}  // namespace neug
