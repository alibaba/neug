/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/storages/index/storage_index.h"
#include "neug/compiler/common/string_utils.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <yaml-cpp/yaml.h>
#include <algorithm>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"

namespace neug {

Status StorageIndex::Init(std::unique_ptr<IndexMeta> meta,
                          std::unique_ptr<IndexIDAccessor> index_id_accessor) {
  if (!meta) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Cannot initialize index with null metadata");
  }
  if (!index_id_accessor) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Cannot initialize index with null IndexIDAccessor");
  }
  meta_ = std::move(meta);
  index_id_accessor_ = std::move(index_id_accessor);
  return Status::OK();
}

result<std::vector<SearchResult>> StorageIndex::Search(
    const IndexQueryParams& params) {
  if (!index_id_accessor_) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INTERNAL_ERROR,
                        "Index ID accessor is not initialized");
  }
  auto candidates = SearchImpl(params);
  if (!candidates) {
    return tl::unexpected(candidates.error());
  }

  std::vector<SearchResult> results;
  results.reserve(candidates->size());
  for (const auto& candidate : candidates.value()) {
    auto vid = index_id_accessor_->GetVIDByIndexID(candidate.index_id);
    if (vid != INVALID_VID) {
      results.emplace_back(vid, candidate.score);
    }
  }
  return results;
}

Status StorageIndex::Upsert(vid_t vid, const Value& new_value) {
  if (!index_id_accessor_) {
    return Status::InternalError("Index ID accessor is not initialized");
  }
  auto index_id = index_id_accessor_->UpsertVID(vid);
  return AppendImpl(index_id, new_value);
}

Status StorageIndex::Delete(vid_t vid) {
  if (!index_id_accessor_) {
    return Status::InternalError("Index ID accessor is not initialized");
  }
  return index_id_accessor_->DeleteVID(vid);
}

// --- IndexBindSchema serialization ---

rapidjson::Value IndexBindSchema::ToJson(
    rapidjson::Document::AllocatorType& alloc) const {
  rapidjson::Value obj(rapidjson::kObjectType);
  obj.AddMember("label_id", label_id, alloc);

  obj.AddMember(
      "property_name",
      rapidjson::Value(property_name.c_str(),
                       static_cast<rapidjson::SizeType>(property_name.size()),
                       alloc),
      alloc);
  auto property_type_yaml =
      YAML::Dump(YAML::convert<DataType>::encode(property_type));
  obj.AddMember(
      "property_type_detail",
      rapidjson::Value(
          property_type_yaml.c_str(),
          static_cast<rapidjson::SizeType>(property_type_yaml.size()), alloc),
      alloc);

  return obj;
}

IndexBindSchema IndexBindSchema::FromJson(const rapidjson::Value& obj) {
  IndexBindSchema schema;
  if (obj.HasMember("label_id") && obj["label_id"].IsUint()) {
    schema.label_id = obj["label_id"].GetUint();
  }
  if (obj.HasMember("property_name") && obj["property_name"].IsString()) {
    schema.property_name = obj["property_name"].GetString();
  }
  if (obj.HasMember("property_type_detail") &&
      obj["property_type_detail"].IsString()) {
    auto node = YAML::Load(obj["property_type_detail"].GetString());
    if (!YAML::convert<DataType>::decode(node, schema.property_type)) {
      THROW_RUNTIME_ERROR(
          "IndexBindSchema::FromJson: invalid property_type_detail");
    }
  }
  return schema;
}

// --- IndexMeta serialization ---

std::string IndexMeta::ToJsonString() const {
  rapidjson::Document doc;
  doc.SetObject();
  auto& alloc = doc.GetAllocator();

  doc.AddMember(
      "name",
      rapidjson::Value(name.c_str(),
                       static_cast<rapidjson::SizeType>(name.size()), alloc),
      alloc);
  doc.AddMember(
      "type",
      rapidjson::Value(type.c_str(),
                       static_cast<rapidjson::SizeType>(type.size()), alloc),
      alloc);
  doc.AddMember("schema", schema.ToJson(alloc), alloc);

  rapidjson::Value opts_obj(rapidjson::kObjectType);
  for (const auto& [k, v] : options) {
    rapidjson::Value key_val(k.c_str(),
                             static_cast<rapidjson::SizeType>(k.size()), alloc);
    rapidjson::Value val_val(v.c_str(),
                             static_cast<rapidjson::SizeType>(v.size()), alloc);
    opts_obj.AddMember(key_val, val_val, alloc);
  }
  doc.AddMember("options", opts_obj, alloc);

  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  doc.Accept(writer);
  return buf.GetString();
}

IndexMeta IndexMeta::FromJsonString(const std::string& json_str) {
  IndexMeta meta;
  rapidjson::Document doc;
  doc.Parse(json_str.c_str());
  if (doc.HasParseError()) {
    THROW_RUNTIME_ERROR("IndexMeta::FromJsonString: invalid JSON at offset " +
                        std::to_string(doc.GetErrorOffset()));
  }
  if (!doc.IsObject()) {
    THROW_RUNTIME_ERROR("IndexMeta::FromJsonString: expected a JSON object");
  }

  if (doc.HasMember("name") && doc["name"].IsString()) {
    meta.name = doc["name"].GetString();
  }
  if (doc.HasMember("type") && doc["type"].IsString()) {
    meta.type = doc["type"].GetString();
  }
  if (doc.HasMember("schema") && doc["schema"].IsObject()) {
    meta.schema = IndexBindSchema::FromJson(doc["schema"]);
  }
  if (doc.HasMember("options") && doc["options"].IsObject()) {
    for (auto& m : doc["options"].GetObject()) {
      if (m.value.IsString()) {
        meta.options[m.name.GetString()] = m.value.GetString();
      }
    }
  }
  return meta;
}

// --- Index base class Open/Dump ---

void StorageIndex::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                        MemoryLevel level) {
  auto index_meta_str = descriptor.get("index_meta");
  if (!meta_ && index_meta_str.has_value()) {
    meta_ = std::make_unique<IndexMeta>(
        IndexMeta::FromJsonString(index_meta_str.value()));
  }
}

void StorageIndex::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                        const std::string& key) {
  if (!meta_) {
    THROW_RUNTIME_ERROR(
        "Cannot dump storage index before metadata is initialized");
  }

  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("index_meta", meta_->ToJsonString());

  meta.set_module(key, std::move(desc));
}

std::string StorageIndex::ModuleTypeName() const {
  auto type_name = meta_ ? meta_->type : "default";
  common::StringUtils::toLower(type_name);
  return type_name + "_index";  // i.e. hnsw_index
}

}  // namespace neug
