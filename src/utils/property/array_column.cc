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

#include "neug/utils/property/array_column.h"

#include <glog/logging.h>

#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

#include <yaml-cpp/yaml.h>

namespace neug {

namespace {

constexpr const char* kElementRef = "element";

std::string MakeChildModuleKey(const std::string& parent_key,
                               const std::string& role) {
  return parent_key + "/" + role;
}

const std::vector<execution::Value>& GetArrayChildren(
    const execution::Value& value) {
  if (value.type().id() != DataTypeId::kArray) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "ArrayColumn expects an ARRAY value, got " + value.type().ToString());
  }
  return execution::ArrayValue::GetChildren(value);
}

}  // namespace

ArrayColumn::ArrayColumn(const DataType& array_type)
    : array_type_(array_type),
      array_size_(ArrayType::GetNumElements(array_type)),
      size_(0) {
  auto child_type = ArrayType::GetChildType(array_type);
  child_column_ = CreateColumn(child_type);
}

void ArrayColumn::Open(Checkpoint& ckp, const ModuleDescriptor& desc,
                       MemoryLevel level) {
  openInternal(ckp, nullptr, desc, level);
}

void ArrayColumn::Open(Checkpoint& ckp, const CheckpointManifest& manifest,
                       const ModuleDescriptor& desc, MemoryLevel level) {
  openInternal(ckp, &manifest, desc, level);
}

void ArrayColumn::openInternal(Checkpoint& ckp,
                               const CheckpointManifest* manifest,
                               const ModuleDescriptor& desc,
                               MemoryLevel level) {
  if (!child_column_) {
    auto array_type_yaml = desc.get("array_type");
    if (!array_type_yaml.has_value()) {
      THROW_RUNTIME_ERROR(
          "ArrayColumn::Open: missing array_type in module descriptor");
    }
    auto node = YAML::Load(array_type_yaml.value());
    if (!YAML::convert<DataType>::decode(node, array_type_)) {
      THROW_RUNTIME_ERROR(
          "ArrayColumn::Open: failed to parse array_type from descriptor");
    }
    array_size_ = ArrayType::GetNumElements(array_type_);
    child_column_ = CreateColumn(ArrayType::GetChildType(array_type_));
  }

  auto size_str = desc.get("array_row_count");
  if (size_str.has_value()) {
    size_ = std::stoull(size_str.value());
  } else {
    size_ = 0;
  }

  auto child_ref = desc.get_ref(kElementRef);
  if (child_ref.has_value()) {
    const auto* resolver = manifest;
    if (!resolver) {
      resolver = &ckp.GetMeta();
    }
    auto child_desc = resolver->module(*child_ref);
    if (!child_desc.has_value()) {
      THROW_RUNTIME_ERROR("ArrayColumn::Open: missing element module '" +
                          *child_ref + "'");
    }
    child_column_->Open(ckp, *resolver, child_desc.value(), level);
    return;
  }

  if (!desc.module_type.empty()) {
    THROW_RUNTIME_ERROR("ArrayColumn::Open: missing '" +
                        std::string(kElementRef) +
                        "' ref in descriptor for persisted array column");
  }

  ModuleDescriptor child_desc;
  child_column_->Open(ckp, child_desc, level);
}

ModuleDescriptor ArrayColumn::dumpSelfDescriptor() const {
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("array_row_count", std::to_string(size_));
  desc.set("array_size", std::to_string(array_size_));
  desc.set("array_type",
           YAML::Dump(YAML::convert<DataType>::encode(array_type_)));
  return desc;
}

void ArrayColumn::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                       const std::string& key) {
  if (key.empty()) {
    THROW_RUNTIME_ERROR("ArrayColumn::Dump: module key must not be empty");
  }
  if (!child_column_) {
    THROW_RUNTIME_ERROR("ArrayColumn::Dump: missing element column");
  }
  auto desc = dumpSelfDescriptor();
  auto child_key = MakeChildModuleKey(key, kElementRef);
  child_column_->Dump(ckp, meta, child_key);
  auto child_it = meta.mutable_modules().find(child_key);
  if (child_it == meta.mutable_modules().end()) {
    THROW_RUNTIME_ERROR(
        "ArrayColumn::Dump: element column did not write module '" + child_key +
        "'");
  }
  child_it->second.mark_as_referenced_module();
  desc.set_ref(kElementRef, std::move(child_key));
  meta.set_module(key, desc);
}

void ArrayColumn::resize(size_t size) {
  size_ = size;
  child_column_->resize(size_ * array_size_);
}

void ArrayColumn::resize(size_t size, const execution::Value& default_value) {
  if (size <= size_) {
    size_ = size;
    child_column_->resize(size_ * array_size_);
    return;
  }
  size_t old_size = size_;
  size_ = size;
  child_column_->resize(size_ * array_size_);
  // Fill newly added rows with default_value.
  for (size_t i = old_size; i < size_; ++i) {
    set_any(i, default_value, true);
  }
}

void ArrayColumn::set_any(size_t index, const execution::Value& value,
                          bool insert_safe) {
  if (index >= size_) {
    THROW_RUNTIME_ERROR("ArrayColumn::set_any: index " + std::to_string(index) +
                        " out of range (size=" + std::to_string(size_) + ")");
  }
  const auto& children = GetArrayChildren(value);
  if (children.size() != array_size_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "ArrayColumn::set_any: expected " + std::to_string(array_size_) +
        " elements, got " + std::to_string(children.size()));
  }
  size_t base = index * array_size_;
  for (size_t j = 0; j < array_size_; ++j) {
    child_column_->set_any(base + j, children[j], insert_safe);
  }
}

execution::Value ArrayColumn::get_any(size_t index) const {
  if (index >= size_) {
    THROW_RUNTIME_ERROR("ArrayColumn::get_value: index " +
                        std::to_string(index) +
                        " out of range (size=" + std::to_string(size_) + ")");
  }
  auto child_type = ArrayType::GetChildType(array_type_);
  std::vector<execution::Value> values;
  values.reserve(array_size_);
  size_t base = index * array_size_;
  for (size_t j = 0; j < array_size_; ++j) {
    values.emplace_back(child_column_->get_any(base + j));
  }
  return execution::Value::ARRAY(array_type_, std::move(values));
}

void ArrayColumn::ingest(uint32_t index, OutArchive& arc) {
  if (index >= size_) {
    THROW_RUNTIME_ERROR("ArrayColumn::ingest: index " + std::to_string(index) +
                        " out of range (size=" + std::to_string(size_) + ")");
  }
  size_t base = index * array_size_;
  for (size_t j = 0; j < array_size_; ++j) {
    child_column_->ingest(base + j, arc);
  }
}

std::unique_ptr<Module> ArrayColumn::Clone() const {
  auto new_col = std::make_unique<ArrayColumn>();
  new_col->array_type_ = array_type_;
  new_col->array_size_ = array_size_;
  new_col->size_ = size_;
  if (child_column_) {
    new_col->child_column_.reset(
        static_cast<ColumnBase*>(child_column_->Clone().release()));
  }
  return new_col;
}

void ArrayColumn::Detach(Checkpoint& ckp, MemoryLevel level) {
  if (child_column_) {
    child_column_->Detach(ckp, level);
  }
}

NEUG_REGISTER_MODULE(ArrayColumn);

}  // namespace neug
