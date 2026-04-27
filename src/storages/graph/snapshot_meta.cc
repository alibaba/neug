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

#include "neug/storages/module/module_factory.h"

#include <fstream>
#include <string>

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

#include "neug/storages/workspace.h"

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
}

void SnapshotMeta::Dump(const std::string& file_path) const {
  std::ofstream ofs(file_path);
  if (!ofs.is_open()) {
    LOG(ERROR) << "SnapshotMeta::Dump: cannot open " << file_path;
    return;
  }

  rapidjson::Document doc;
  doc.SetObject();
  auto& alloc = doc.GetAllocator();

  doc.AddMember("version", rapidjson::Value(1), alloc);

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

  rapidjson::OStreamWrapper osw(ofs);
  rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
  doc.Accept(writer);
  LOG(INFO) << "SnapshotMeta::Dump: dumped meta to " << file_path;
}

void SnapshotMeta::GenerateEmptyMeta(const std::string& path) {
  std::string tmp_path = path + ".tmp";
  {
    std::ofstream o(tmp_path);
    rapidjson::OStreamWrapper osw(o);
    rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();
    doc.AddMember("version", rapidjson::Value(1), alloc);
    doc.AddMember("modules", rapidjson::Value(rapidjson::kObjectType), alloc);
    doc.Accept(writer);
  }
  std::filesystem::rename(tmp_path, path);
}

const Schema& SnapshotMeta::GetSchema() const { return schema_; }

void SnapshotMeta::SetSchema(const Schema& schema) { schema_ = schema; }

}  // namespace neug
