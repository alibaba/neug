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

#include "neug/storages/module_descriptor.h"

#include <limits>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/utils/exception/exception.h"

namespace neug {

rapidjson::Value ModuleDescriptor::ToJson(
    rapidjson::Document::AllocatorType& alloc) const {
  rapidjson::Value obj(rapidjson::kObjectType);
  obj.AddMember(
      "module_type",
      rapidjson::Value(module_type.c_str(),
                       static_cast<rapidjson::SizeType>(module_type.size()),
                       alloc),
      alloc);
  obj.AddMember("required", rapidjson::Value(required), alloc);
  if (!extra_.empty()) {
    rapidjson::Value extra_obj(rapidjson::kObjectType);
    for (const auto& [k, v] : extra_) {
      rapidjson::Value key_val(
          k.c_str(), static_cast<rapidjson::SizeType>(k.size()), alloc);
      rapidjson::Value val_val(
          v.c_str(), static_cast<rapidjson::SizeType>(v.size()), alloc);
      extra_obj.AddMember(key_val, val_val, alloc);
    }
    obj.AddMember("extra", extra_obj, alloc);
  }
  if (!paths_.empty()) {
    rapidjson::Value paths_obj(rapidjson::kObjectType);
    for (const auto& [k, v] : paths_) {
      rapidjson::Value key_val(
          k.c_str(), static_cast<rapidjson::SizeType>(k.size()), alloc);
      rapidjson::Value val_val(
          v.c_str(), static_cast<rapidjson::SizeType>(v.size()), alloc);
      paths_obj.AddMember(key_val, val_val, alloc);
    }
    obj.AddMember("objects", paths_obj, alloc);
  }
  if (!slices_.empty()) {
    rapidjson::Value slices_obj(rapidjson::kObjectType);
    for (const auto& [key, slice] : slices_) {
      rapidjson::Value name(
          key.c_str(), static_cast<rapidjson::SizeType>(key.size()), alloc);
      rapidjson::Value entry(rapidjson::kObjectType);
      entry.AddMember("offset", static_cast<uint64_t>(slice.offset), alloc);
      entry.AddMember("length", static_cast<uint64_t>(slice.length), alloc);
      slices_obj.AddMember(name, entry, alloc);
    }
    obj.AddMember("slices", slices_obj, alloc);
  }
  if (!refs_.empty()) {
    rapidjson::Value refs_obj(rapidjson::kObjectType);
    for (const auto& [k, v] : refs_) {
      rapidjson::Value key_val(
          k.c_str(), static_cast<rapidjson::SizeType>(k.size()), alloc);
      rapidjson::Value val_val(
          v.c_str(), static_cast<rapidjson::SizeType>(v.size()), alloc);
      refs_obj.AddMember(key_val, val_val, alloc);
    }
    obj.AddMember("refs", refs_obj, alloc);
  }
  if (referenced_module_) {
    obj.AddMember("referenced_module", rapidjson::Value(true), alloc);
  }
  return obj;
}

ModuleDescriptor ModuleDescriptor::FromJson(const rapidjson::Value& obj) {
  ModuleDescriptor desc;
  if (obj.HasMember("module_type") && obj["module_type"].IsString()) {
    desc.module_type = obj["module_type"].GetString();
  }
  if (obj.HasMember("required") && obj["required"].IsBool()) {
    desc.required = obj["required"].GetBool();
  }
  if (obj.HasMember("extra") && obj["extra"].IsObject()) {
    for (auto& m : obj["extra"].GetObject()) {
      if (m.value.IsString()) {
        desc.extra_[m.name.GetString()] = m.value.GetString();
      }
    }
  }
  if (obj.HasMember("objects") && obj["objects"].IsObject()) {
    for (auto& m : obj["objects"].GetObject()) {
      if (m.value.IsString()) {
        desc.paths_[m.name.GetString()] = m.value.GetString();
      }
    }
  }
  if (obj.HasMember("slices")) {
    const auto& slices = obj["slices"];
    if (!slices.IsObject())
      THROW_CHECKPOINT_EXCEPTION("Invalid checkpoint object slices");
    for (const auto& member : slices.GetObject()) {
      const std::string name = member.name.GetString();
      const auto& value = member.value;
      if (!value.IsObject())
        THROW_CHECKPOINT_EXCEPTION("Invalid checkpoint object slice: " + name);
      const auto offset = value.FindMember("offset");
      const auto length = value.FindMember("length");
      const auto end = value.MemberEnd();
      constexpr auto max_size = std::numeric_limits<size_t>::max();
      if (offset == end || length == end || !offset->value.IsUint64() ||
          !length->value.IsUint64() || offset->value.GetUint64() > max_size ||
          length->value.GetUint64() > max_size)
        THROW_CHECKPOINT_EXCEPTION("Invalid checkpoint object slice: " + name);
      desc.set_slice(name, {static_cast<size_t>(offset->value.GetUint64()),
                            static_cast<size_t>(length->value.GetUint64())});
    }
  }
  if (obj.HasMember("refs") && obj["refs"].IsObject()) {
    for (auto& m : obj["refs"].GetObject()) {
      if (m.value.IsString()) {
        desc.refs_[m.name.GetString()] = m.value.GetString();
      }
    }
  }
  if (obj.HasMember("referenced_module") && obj["referenced_module"].IsBool()) {
    desc.referenced_module_ = obj["referenced_module"].GetBool();
  }
  return desc;
}

std::string ModuleDescriptor::ToJsonString() const {
  rapidjson::Document doc;
  doc.SetObject();
  auto obj = ToJson(doc.GetAllocator());
  static_cast<rapidjson::Value&>(doc).Swap(obj);
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  doc.Accept(writer);
  return buf.GetString();
}

}  // namespace neug
