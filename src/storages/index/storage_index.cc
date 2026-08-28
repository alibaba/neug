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
#include <cassert>

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
      results.push_back(SearchResult{vid, candidate.score});
    }
  }
  return results;
}

Status StorageIndex::Upsert(vid_t vid, const IndexValues& new_values) {
  if (!index_id_accessor_) {
    return Status::InternalError("Index ID accessor is not initialized");
  }
  if (!meta_ || new_values.size() != meta_->schema.columns.size()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Index value count does not match metadata");
  }
  assert(std::none_of(new_values.begin(), new_values.end(),
                      [](const Value& value) { return value.IsNull(); }));
  auto index_id = index_id_accessor_->UpsertVID(vid);
  return AppendImpl(index_id, new_values);
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
  obj.AddMember("label_name",
                rapidjson::Value(
                    label_name.c_str(),
                    static_cast<rapidjson::SizeType>(label_name.size()), alloc),
                alloc);

  rapidjson::Value column_array(rapidjson::kArrayType);
  for (const auto& column : columns) {
    rapidjson::Value column_obj(rapidjson::kObjectType);
    column_obj.AddMember("property_name",
                         rapidjson::Value(column.property_name.c_str(),
                                          static_cast<rapidjson::SizeType>(
                                              column.property_name.size()),
                                          alloc),
                         alloc);
    auto property_type_yaml =
        YAML::Dump(YAML::convert<DataType>::encode(column.property_type));
    column_obj.AddMember(
        "property_type_detail",
        rapidjson::Value(
            property_type_yaml.c_str(),
            static_cast<rapidjson::SizeType>(property_type_yaml.size()), alloc),
        alloc);
    column_array.PushBack(std::move(column_obj), alloc);
  }
  obj.AddMember("columns", std::move(column_array), alloc);

  return obj;
}

IndexBindSchema IndexBindSchema::FromJson(const rapidjson::Value& obj) {
  IndexBindSchema schema;
  if (obj.HasMember("label_id") && obj["label_id"].IsUint()) {
    schema.label_id = obj["label_id"].GetUint();
  }
  if (obj.HasMember("label_name") && obj["label_name"].IsString()) {
    schema.label_name = obj["label_name"].GetString();
  }
  auto parse_column = [](const rapidjson::Value& value) {
    IndexBindColumn column;
    if (!value.IsObject() || !value.HasMember("property_name") ||
        !value["property_name"].IsString() ||
        !value.HasMember("property_type_detail") ||
        !value["property_type_detail"].IsString()) {
      THROW_RUNTIME_ERROR("IndexBindSchema::FromJson: invalid index column");
    }
    column.property_name = value["property_name"].GetString();
    auto node = YAML::Load(value["property_type_detail"].GetString());
    if (!YAML::convert<DataType>::decode(node, column.property_type)) {
      THROW_RUNTIME_ERROR("IndexBindSchema::FromJson: invalid property type");
    }
    return column;
  };
  if (obj.HasMember("columns") && obj["columns"].IsArray()) {
    for (const auto& value : obj["columns"].GetArray()) {
      schema.columns.emplace_back(parse_column(value));
    }
  } else if (obj.HasMember("property_name") &&
             obj["property_name"].IsString() &&
             obj.HasMember("property_type_detail") &&
             obj["property_type_detail"].IsString()) {
    schema.columns.emplace_back(parse_column(obj));
  }
  return schema;
}

bool IndexBindSchema::ContainsProperty(const std::string& property_name) const {
  return FindProperty(property_name).has_value();
}

std::optional<size_t> IndexBindSchema::FindProperty(
    const std::string& property_name) const {
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].property_name == property_name) {
      return i;
    }
  }
  return std::nullopt;
}

void IndexMeta::RenameProperty(const std::string& old_name,
                               const std::string& new_name) {
  for (auto& column : schema.columns) {
    if (column.property_name == old_name) {
      column.property_name = new_name;
    }
  }
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
  // Storage indexes may be supplied by an extension that is loaded after the
  // database opens. Preserve their descriptors so StorageIndexManager can
  // defer activation until the module type is registered.
  desc.required = false;
  desc.set("index_meta", meta_->ToJsonString());

  meta.SetModule(key, std::move(desc));
}

std::string StorageIndex::ModuleTypeName() const {
  auto type_name = meta_ ? meta_->type : "default";
  common::StringUtils::toLower(type_name);
  return type_name + "_index";  // i.e. hnsw_index
}

}  // namespace neug
