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

#include "neug/storages/index/index.h"
#include "neug/compiler/common/string_utils.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <algorithm>

#include "neug/storages/checkpoint.h"

namespace neug {

// --- IndexBindSchema serialization ---

rapidjson::Value IndexBindSchema::ToJson(
    rapidjson::Document::AllocatorType& alloc) const {
  rapidjson::Value obj(rapidjson::kObjectType);
  obj.AddMember("label_id", label_id, alloc);

  rapidjson::Value names_arr(rapidjson::kArrayType);
  for (const auto& name : property_names) {
    names_arr.PushBack(
        rapidjson::Value(name.c_str(),
                         static_cast<rapidjson::SizeType>(name.size()), alloc),
        alloc);
  }
  obj.AddMember("property_names", names_arr, alloc);

  rapidjson::Value types_arr(rapidjson::kArrayType);
  for (const auto& dt : property_types) {
    types_arr.PushBack(static_cast<uint8_t>(dt.id()), alloc);
  }
  obj.AddMember("property_types", types_arr, alloc);

  return obj;
}

IndexBindSchema IndexBindSchema::FromJson(const rapidjson::Value& obj) {
  IndexBindSchema schema;
  if (obj.HasMember("label_id") && obj["label_id"].IsUint()) {
    schema.label_id = obj["label_id"].GetUint();
  }
  if (obj.HasMember("property_names") && obj["property_names"].IsArray()) {
    for (auto& v : obj["property_names"].GetArray()) {
      if (v.IsString()) {
        schema.property_names.emplace_back(v.GetString());
      }
    }
  }
  if (obj.HasMember("property_types") && obj["property_types"].IsArray()) {
    for (auto& v : obj["property_types"].GetArray()) {
      if (v.IsUint()) {
        schema.property_types.emplace_back(
            static_cast<DataTypeId>(v.GetUint()));
      }
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
  if (doc.HasParseError() || !doc.IsObject()) {
    return meta;
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

void Index::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                 MemoryLevel level) {
  auto index_meta_str = descriptor.get("index_meta");
  if (index_meta_str.has_value()) {
    meta_ = std::make_unique<IndexMeta>(
        IndexMeta::FromJsonString(index_meta_str.value()));
  }

  doc_id_map_ = std::make_unique<DocIDMap>();
  doc_id_map_->Open(ckp, descriptor, level);
}

ModuleDescriptor Index::Dump(Checkpoint& ckp) {
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("index_meta", meta_->ToJsonString());

  if (doc_id_map_) {
    auto doc_desc = doc_id_map_->Dump(ckp);
    auto next_doc_id = doc_desc.get("next_doc_id");
    if (next_doc_id.has_value()) {
      desc.set("next_doc_id", next_doc_id.value());
    }
    auto doc_buffer = doc_desc.get_path("doc_id_buffer");
    if (doc_buffer.has_value()) {
      desc.set_path("doc_id_buffer", doc_buffer.value());
    }
  }

  return desc;
}

std::string Index::ModuleTypeName() const {
  auto type_name = meta_ ? meta_->type : "default";
  common::StringUtils::toLower(type_name);
  return type_name + "_index";  // i.e. hnsw_index
}

}  // namespace neug
