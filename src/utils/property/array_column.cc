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

#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

ArrayColumn::ArrayColumn(const DataType& array_type)
    : array_type_(array_type),
      array_size_(ArrayType::GetNumElements(array_type)),
      size_(0) {
  auto child_type = ArrayType::GetChildType(array_type);
  child_column_ = CreateColumn(child_type);
}

void ArrayColumn::Open(Checkpoint& ckp, const ModuleDescriptor& desc,
                       MemoryLevel level) {
  auto size_str = desc.get("array_row_count");
  if (size_str.has_value()) {
    size_ = std::stoull(size_str.value());
  } else {
    size_ = 0;
  }

  auto child_desc_json = desc.get("child_descriptor");
  ModuleDescriptor child_desc;
  if (child_desc_json.has_value()) {
    rapidjson::Document doc;
    doc.Parse(child_desc_json.value().c_str());
    if (!doc.HasParseError() && doc.IsObject()) {
      child_desc = ModuleDescriptor::FromJson(doc);
    }
  }
  child_column_->Open(ckp, child_desc, level);
}

ModuleDescriptor ArrayColumn::Dump(Checkpoint& ckp) {
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("array_row_count", std::to_string(size_));
  desc.set("array_size", std::to_string(array_size_));

  auto child_desc = child_column_->Dump(ckp);
  desc.set("child_descriptor", child_desc.ToJsonString());
  return desc;
}

void ArrayColumn::resize(size_t size) {
  size_ = size;
  child_column_->resize(size_ * array_size_);
}

void ArrayColumn::resize(size_t size, const execution::Value& default_value) {
  size_ = size;
  child_column_->resize(size_ * array_size_);
}

void ArrayColumn::set_any(size_t index, const execution::Value& value,
                          bool insert_safe) {
  if (index >= size_) {
    THROW_RUNTIME_ERROR("ArrayColumn::set_any: index " + std::to_string(index) +
                        " out of range (size=" + std::to_string(size_) + ")");
  }
  const auto& children = execution::ListValue::GetChildren(value);
  if (children.size() != array_size_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "ArrayColumn::set_any: expected " + std::to_string(array_size_) +
        " elements, got " + std::to_string(children.size()));
  }
  size_t base = index * array_size_;
  for (uint32_t j = 0; j < array_size_; ++j) {
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
  for (uint32_t j = 0; j < array_size_; ++j) {
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
  for (uint32_t j = 0; j < array_size_; ++j) {
    child_column_->ingest(base + j, arc);
  }
}

NEUG_REGISTER_MODULE(ArrayColumn);

}  // namespace neug
