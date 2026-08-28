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

#include "neug/utils/property/struct_property_column.h"

#include <yaml-cpp/yaml.h>

#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column_module_utils.h"
#include "neug/utils/property/default_value.h"
#include "neug/utils/property/types.h"

namespace neug {

StructPropertyColumn::StructPropertyColumn(const DataType& struct_type)
    : struct_type_(struct_type) {
  for (const auto& child_type : StructType::GetChildTypes(struct_type_)) {
    fields_.push_back(CreateColumn(child_type));
  }
}

void StructPropertyColumn::Open(Checkpoint& ckp, const ModuleDescriptor& desc,
                                MemoryLevel level) {
  openInternal(ckp, nullptr, desc, level);
}

void StructPropertyColumn::Open(Checkpoint& ckp,
                                const CheckpointManifest& manifest,
                                const ModuleDescriptor& desc,
                                MemoryLevel level) {
  openInternal(ckp, &manifest, desc, level);
}

void StructPropertyColumn::openInternal(Checkpoint& ckp,
                                        const CheckpointManifest* manifest,
                                        const ModuleDescriptor& desc,
                                        MemoryLevel level) {
  if (struct_type_.id() == DataTypeId::kInvalid) {
    auto type_yaml = desc.get("struct_type");
    if (!type_yaml.has_value()) {
      THROW_RUNTIME_ERROR(
          "StructPropertyColumn::Open: missing struct_type in descriptor");
    }
    auto node = YAML::Load(*type_yaml);
    if (!YAML::convert<DataType>::decode(node, struct_type_)) {
      THROW_RUNTIME_ERROR(
          "StructPropertyColumn::Open: failed to parse struct_type");
    }
    if (struct_type_.id() != DataTypeId::kStruct) {
      THROW_RUNTIME_ERROR(
          "StructPropertyColumn::Open: descriptor type is not STRUCT");
    }
    for (const auto& child_type : StructType::GetChildTypes(struct_type_)) {
      fields_.push_back(CreateColumn(child_type));
    }
  }

  if (desc.module_type.empty()) {
    for (auto& field : fields_) {
      field->Open(ckp, ModuleDescriptor{}, level);
    }
    return;
  }

  auto row_count = desc.get("struct_row_count");
  if (!row_count.has_value()) {
    THROW_RUNTIME_ERROR(
        "StructPropertyColumn::Open: missing struct_row_count in descriptor");
  }
  const size_t expected_rows = std::stoull(*row_count);

  const auto& resolver = manifest ? *manifest : ckp.GetMeta();
  const auto& field_names = StructType::GetFieldNames(struct_type_);
  for (size_t f = 0; f < fields_.size(); ++f) {
    std::optional<ModuleDescriptor> field_desc;
    fields_[f]->Open(ckp, resolver,
                     column_module::ResolveChild(
                         resolver, desc, field_names[f], field_desc,
                         "StructPropertyColumn::Open"),
                     level);
    if (fields_[f]->size() != expected_rows) {
      THROW_RUNTIME_ERROR("StructPropertyColumn::Open: field '" +
                          field_names[f] + "' row count mismatch");
    }
  }
}

ModuleDescriptor StructPropertyColumn::dumpSelfDescriptor() const {
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("struct_row_count", std::to_string(size()));
  desc.set("struct_type",
           YAML::Dump(YAML::convert<DataType>::encode(struct_type_)));
  return desc;
}

void StructPropertyColumn::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                                const std::string& key) {
  if (key.empty()) {
    THROW_RUNTIME_ERROR(
        "StructPropertyColumn::Dump: module key must not be empty");
  }

  const auto& field_names = StructType::GetFieldNames(struct_type_);
  auto desc = dumpSelfDescriptor();
  for (size_t f = 0; f < fields_.size(); ++f) {
    auto field_key = column_module::ChildModuleKey(key, field_names[f]);
    fields_[f]->Dump(ckp, meta, field_key);
    column_module::MarkReferenced(meta, field_key,
                                  "StructPropertyColumn::Dump");
    desc.set_ref(field_names[f], std::move(field_key));
  }
  meta.set_module(key, std::move(desc));
}

void StructPropertyColumn::resize(size_t size) {
  for (auto& field : fields_) {
    field->resize(size);
  }
}

void StructPropertyColumn::resize(size_t size, const Value& default_value) {
  Value fallback;
  const Value* normalized = &default_value;
  if (default_value.IsNull()) {
    fallback = get_default_value(struct_type_);
    normalized = &fallback;
  }
  if (normalized->type() != struct_type_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "StructPropertyColumn::resize: expected " + struct_type_.ToString() +
        ", got " + normalized->type().ToString());
  }
  const auto& children = StructValue::GetChildren(*normalized);
  CHECK(children.size() == fields_.size())
      << "struct value field count does not match struct type";
  // Each field column applies its own default to the new rows directly, so
  // no row-wise set_any pass is needed.
  for (size_t f = 0; f < fields_.size(); ++f) {
    fields_[f]->resize(size, children[f]);
  }
}

void StructPropertyColumn::set_any(size_t index, const Value& value,
                                   bool insert_safe) {
  if (index >= size()) {
    THROW_RUNTIME_ERROR("StructPropertyColumn::set_any: index " +
                        std::to_string(index) +
                        " out of range (size=" + std::to_string(size()) + ")");
  }

  Value fallback;
  const Value* normalized = &value;
  if (value.IsNull()) {
    fallback = get_default_value(struct_type_);
    normalized = &fallback;
  }
  if (normalized->type() != struct_type_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "StructPropertyColumn::set_any: expected " + struct_type_.ToString() +
        ", got " + normalized->type().ToString());
  }
  const auto& children = StructValue::GetChildren(*normalized);
  CHECK(children.size() == fields_.size())
      << "struct value field count does not match struct type";
  // Null or type-mismatched field values are the child columns' contract.
  for (size_t f = 0; f < fields_.size(); ++f) {
    fields_[f]->set_any(index, children[f], insert_safe);
  }
}

Value StructPropertyColumn::get_any(size_t index) const {
  if (index >= size()) {
    THROW_RUNTIME_ERROR("StructPropertyColumn::get_any: index " +
                        std::to_string(index) +
                        " out of range (size=" + std::to_string(size()) + ")");
  }
  std::vector<Value> children;
  children.reserve(fields_.size());
  for (const auto& field : fields_) {
    children.emplace_back(field->get_any(index));
  }
  return Value::STRUCT(struct_type_, std::move(children));
}

std::unique_ptr<Module> StructPropertyColumn::Clone() const {
  auto clone = std::make_unique<StructPropertyColumn>();
  clone->struct_type_ = struct_type_;
  clone->fields_.reserve(fields_.size());
  for (const auto& field : fields_) {
    clone->fields_.emplace_back(
        static_cast<ColumnBase*>(field->Clone().release()));
  }
  return clone;
}

void StructPropertyColumn::Detach(Checkpoint& ckp, MemoryLevel level) {
  for (auto& field : fields_) {
    field->Detach(ckp, level);
  }
}

StructPropertyRefColumn::StructPropertyRefColumn(
    const StructPropertyColumn& column)
    : struct_type_(column.struct_type()) {
  fields_.reserve(column.num_fields());
  for (size_t f = 0; f < column.num_fields(); ++f) {
    fields_.push_back(CreateRefColumn(column.field_column(f)));
  }
}

Value StructPropertyRefColumn::get_any(size_t index) const {
  std::vector<Value> children;
  children.reserve(fields_.size());
  for (const auto& field : fields_) {
    children.emplace_back(field->get_any(index));
  }
  return Value::STRUCT(struct_type_, std::move(children));
}

NEUG_REGISTER_MODULE(StructPropertyColumn);

}  // namespace neug
