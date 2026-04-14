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

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/utils/exception/exception.h"

namespace neug {

// ---------------------------------------------------------------------------
// SubModulesImpl definition (pimpl) — map is fully instantiated here, where
// ModuleDescriptor is already a complete type.
// ---------------------------------------------------------------------------

struct ModuleDescriptor::SubModulesImpl {
  std::unordered_map<std::string, ModuleDescriptor> map;
};

// ---------------------------------------------------------------------------
// sub_modules accessors
// ---------------------------------------------------------------------------

const std::unordered_map<std::string, ModuleDescriptor>&
ModuleDescriptor::sub_modules() const {
  static const std::unordered_map<std::string, ModuleDescriptor> empty;
  if (!sub_modules_impl_)
    return empty;
  return sub_modules_impl_->map;
}

std::unordered_map<std::string, ModuleDescriptor>&
ModuleDescriptor::sub_modules() {
  if (!sub_modules_impl_) {
    sub_modules_impl_ = std::make_unique<SubModulesImpl>();
  }
  return sub_modules_impl_->map;
}

void ModuleDescriptor::set_sub_module(const std::string& key,
                                      ModuleDescriptor desc) {
  if (!sub_modules_impl_) {
    sub_modules_impl_ = std::make_unique<SubModulesImpl>();
  }
  sub_modules_impl_->map[key] = std::move(desc);
}

bool ModuleDescriptor::has_sub_module(const std::string& key) const {
  if (!sub_modules_impl_)
    return false;
  return sub_modules_impl_->map.count(key) > 0;
}

// TODO(zhanglei): This method should be renamed to get_sub_module_or_default.
ModuleDescriptor ModuleDescriptor::get_sub_module(
    const std::string& key) const {
  if (!sub_modules_impl_) {
    return ModuleDescriptor();
  }
  if (sub_modules_impl_->map.count(key) == 0) {
    return ModuleDescriptor();
  }
  return sub_modules_impl_->map.at(key);
}

// ---------------------------------------------------------------------------
// Rule-of-5 (needed because of unique_ptr<SubModulesImpl> with incomplete type
// in header)
// ---------------------------------------------------------------------------

ModuleDescriptor::ModuleDescriptor() = default;
ModuleDescriptor::~ModuleDescriptor() = default;

ModuleDescriptor::ModuleDescriptor(const ModuleDescriptor& other)
    : path(other.path),
      size(other.size),
      type(other.type),
      module_type(other.module_type),
      version(other.version),
      extra_(other.extra_),
      sub_modules_impl_(
          other.sub_modules_impl_
              ? std::make_unique<SubModulesImpl>(*other.sub_modules_impl_)
              : nullptr) {}

ModuleDescriptor& ModuleDescriptor::operator=(const ModuleDescriptor& other) {
  if (this != &other) {
    path = other.path;
    size = other.size;
    type = other.type;
    module_type = other.module_type;
    version = other.version;
    extra_ = other.extra_;
    sub_modules_impl_ =
        other.sub_modules_impl_
            ? std::make_unique<SubModulesImpl>(*other.sub_modules_impl_)
            : nullptr;
  }
  return *this;
}

ModuleDescriptor::ModuleDescriptor(ModuleDescriptor&&) noexcept = default;
ModuleDescriptor& ModuleDescriptor::operator=(ModuleDescriptor&&) noexcept =
    default;

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

rapidjson::Value ModuleDescriptor::ToJson(
    rapidjson::Document::AllocatorType& alloc) const {
  rapidjson::Value obj(rapidjson::kObjectType);
  obj.AddMember(
      "path",
      rapidjson::Value(path.c_str(),
                       static_cast<rapidjson::SizeType>(path.size()), alloc),
      alloc);
  obj.AddMember("size", rapidjson::Value(size), alloc);
  obj.AddMember(
      "type",
      rapidjson::Value(type.c_str(),
                       static_cast<rapidjson::SizeType>(type.size()), alloc),
      alloc);
  obj.AddMember(
      "module_type",
      rapidjson::Value(module_type.c_str(),
                       static_cast<rapidjson::SizeType>(module_type.size()),
                       alloc),
      alloc);
  obj.AddMember("version", rapidjson::Value(version), alloc);
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
  if (sub_modules_impl_ && !sub_modules_impl_->map.empty()) {
    rapidjson::Value sub_obj(rapidjson::kObjectType);
    for (const auto& [k, sub_desc] : sub_modules_impl_->map) {
      rapidjson::Value key_val(
          k.c_str(), static_cast<rapidjson::SizeType>(k.size()), alloc);
      sub_obj.AddMember(key_val, sub_desc.ToJson(alloc), alloc);
    }
    obj.AddMember("sub_modules", sub_obj, alloc);
  }
  return obj;
}

ModuleDescriptor ModuleDescriptor::FromJson(const rapidjson::Value& obj) {
  ModuleDescriptor desc;
  if (obj.HasMember("path") && obj["path"].IsString()) {
    desc.path = obj["path"].GetString();
  }
  if (obj.HasMember("size") && obj["size"].IsUint64()) {
    desc.size = obj["size"].GetUint64();
  }
  if (obj.HasMember("type") && obj["type"].IsString()) {
    desc.type = obj["type"].GetString();
  }
  if (obj.HasMember("module_type") && obj["module_type"].IsString()) {
    desc.module_type = obj["module_type"].GetString();
  }
  if (obj.HasMember("version") && obj["version"].IsUint()) {
    desc.version = obj["version"].GetUint();
  }
  if (obj.HasMember("extra") && obj["extra"].IsObject()) {
    for (auto& m : obj["extra"].GetObject()) {
      if (m.value.IsString()) {
        desc.extra_[m.name.GetString()] = m.value.GetString();
      }
    }
  }
  if (obj.HasMember("sub_modules") && obj["sub_modules"].IsObject()) {
    for (auto& m : obj["sub_modules"].GetObject()) {
      if (m.value.IsObject()) {
        desc.set_sub_module(m.name.GetString(),
                            ModuleDescriptor::FromJson(m.value));
      }
    }
  }
  return desc;
}

std::string ModuleDescriptor::ToJsonString() const {
  rapidjson::Document doc;
  doc.SetObject();
  auto& alloc = doc.GetAllocator();
  auto obj = ToJson(alloc);
  // Move fields into the document root
  for (auto& m : obj.GetObject()) {
    doc.AddMember(rapidjson::Value(m.name, alloc),
                  rapidjson::Value(m.value, alloc), alloc);
  }
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  doc.Accept(writer);
  return buf.GetString();
}

ModuleDescriptor ModuleDescriptor::FromJsonString(const std::string& json) {
  rapidjson::Document doc;
  doc.Parse(json.c_str(), json.size());
  if (doc.HasParseError() || !doc.IsObject()) {
    return ModuleDescriptor{};
  }
  return FromJson(doc);
}

}  // namespace neug
