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

#include "neug/utils/property/list_property_column.h"

#include <algorithm>
#include <fstream>
#include <optional>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/default_value.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace {

constexpr const char* kItemsRef = "items";
constexpr const char* kElementsRef = "elements";

std::string ChildModuleKey(const std::string& parent, const std::string& role) {
  return parent + "/" + role;
}

void MarkReferenced(CheckpointManifest& meta, const std::string& key) {
  auto it = meta.mutable_modules().find(key);
  if (it == meta.mutable_modules().end()) {
    THROW_RUNTIME_ERROR(
        "ListPropertyColumn::Dump: child column did not write "
        "module '" +
        key + "'");
  }
  it->second.mark_as_referenced_module();
}

const ModuleDescriptor& ResolveChild(const CheckpointManifest& manifest,
                                     const ModuleDescriptor& parent,
                                     const char* role,
                                     std::optional<ModuleDescriptor>& storage) {
  auto ref = parent.get_ref(role);
  if (!ref.has_value()) {
    THROW_RUNTIME_ERROR("ListPropertyColumn::Open: missing '" +
                        std::string(role) + "' ref");
  }
  storage = manifest.module(*ref);
  if (!storage.has_value()) {
    THROW_RUNTIME_ERROR("ListPropertyColumn::Open: missing child module '" +
                        *ref + "'");
  }
  return *storage;
}

}  // namespace

ListPropertyColumn::ListPropertyColumn(const DataType& list_type)
    : list_type_(list_type),
      child_type_(ListType::GetChildType(list_type)),
      size_(0),
      elements_tail_(0),
      items_(std::make_unique<ULongColumn>()),
      elements_(CreateColumn(child_type_)) {}

void ListPropertyColumn::Open(Checkpoint& ckp, const ModuleDescriptor& desc,
                              MemoryLevel level) {
  openInternal(ckp, nullptr, desc, level);
}

void ListPropertyColumn::Open(Checkpoint& ckp,
                              const CheckpointManifest& manifest,
                              const ModuleDescriptor& desc, MemoryLevel level) {
  openInternal(ckp, &manifest, desc, level);
}

void ListPropertyColumn::openInternal(Checkpoint& ckp,
                                      const CheckpointManifest* manifest,
                                      const ModuleDescriptor& desc,
                                      MemoryLevel level) {
  if (list_type_.id() == DataTypeId::kInvalid) {
    auto type_yaml = desc.get("list_type");
    if (!type_yaml.has_value()) {
      THROW_RUNTIME_ERROR(
          "ListPropertyColumn::Open: missing list_type in descriptor");
    }
    auto node = YAML::Load(*type_yaml);
    if (!YAML::convert<DataType>::decode(node, list_type_)) {
      THROW_RUNTIME_ERROR(
          "ListPropertyColumn::Open: failed to parse list_type");
    }
    if (list_type_.id() != DataTypeId::kList) {
      THROW_RUNTIME_ERROR(
          "ListPropertyColumn::Open: descriptor type is not LIST");
    }
    child_type_ = ListType::GetChildType(list_type_);
    items_ = std::make_unique<ULongColumn>();
    elements_ = CreateColumn(child_type_);
  }

  if (desc.module_type.empty()) {
    items_->Open(ckp, ModuleDescriptor{}, level);
    elements_->Open(ckp, ModuleDescriptor{}, level);
    size_ = 0;
    elements_tail_ = 0;
    return;
  }

  auto row_count = desc.get("list_row_count");
  if (!row_count.has_value()) {
    THROW_RUNTIME_ERROR(
        "ListPropertyColumn::Open: missing list_row_count in descriptor");
  }
  size_ = std::stoull(*row_count);

  const auto& resolver = manifest ? *manifest : ckp.GetMeta();
  std::optional<ModuleDescriptor> items_desc;
  std::optional<ModuleDescriptor> elements_desc;
  items_->Open(ckp, ResolveChild(resolver, desc, kItemsRef, items_desc), level);
  elements_->Open(ckp, resolver,
                  ResolveChild(resolver, desc, kElementsRef, elements_desc),
                  level);
  // After loading from a checkpoint, elements_tail_ equals elements_->size(),
  // meaning there is zero spare capacity in the elements column.  Any
  // non-empty list insertion via insert transaction (insert_safe=false)
  // will throw a StorageException.  This is a known limitation.
  elements_tail_ = elements_->size();

  if (items_->size() != size_) {
    THROW_RUNTIME_ERROR("ListPropertyColumn::Open: row metadata size mismatch");
  }
}

ModuleDescriptor ListPropertyColumn::dumpSelfDescriptor() const {
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("list_row_count", std::to_string(size_));
  desc.set("list_type",
           YAML::Dump(YAML::convert<DataType>::encode(list_type_)));
  return desc;
}

void ListPropertyColumn::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                              const std::string& key) {
  if (key.empty()) {
    THROW_RUNTIME_ERROR(
        "ListPropertyColumn::Dump: module key must not be empty");
  }

  auto items_key = ChildModuleKey(key, kItemsRef);
  auto elements_key = ChildModuleKey(key, kElementsRef);

  // Compute live element count to check whether compaction is needed.
  size_t total_elements = 0;
  for (size_t row = 0; row < size_; ++row) {
    total_elements += get_item(row).length;
  }

  // Fast path: no dead space and no extra capacity — the elements column is
  // already compact and contiguous, so dump the original columns directly
  // without creating temporary copies.  This is the common case when a
  // checkpoint runs after Open with no length-changing updates.
  if (total_elements == elements_tail_ && elements_->size() == elements_tail_) {
    items_->Dump(ckp, meta, items_key);
    elements_->Dump(ckp, meta, elements_key);
  } else {
    // Slow path: compact to eliminate dead space left by length-changing
    // updates (old elements become unreachable when a row's list is
    // re-appended at the tail).

    // Stream compacted items to a runtime file, remapping offsets on the
    // fly.  Layout matches MMapContainer::Dump (FileHeader + payload) so
    // Open() reads it back unchanged.
    auto item_file = ckp.CreateRuntimeFile();
    std::ofstream item_out(item_file.path(), std::ios::binary);
    if (!item_out) {
      THROW_IO_EXCEPTION("Failed to open file for dumping: " +
                         item_file.path());
    }
    FileHeader header{};
    item_out.write(reinterpret_cast<const char*>(&header.data_md5),
                   sizeof(header.data_md5));
    MD5_CTX md5_ctx;
    MD5_Init(&md5_ctx);
    size_t tail = 0;
    for (size_t row = 0; row < size_; ++row) {
      auto item = get_item(row);
      list_meta_item compact{tail, item.length};
      item_out.write(reinterpret_cast<const char*>(&compact), sizeof(compact));
      MD5_Update(&md5_ctx, &compact, sizeof(compact));
      tail += item.length;
    }
    MD5_Final(header.data_md5, &md5_ctx);
    item_out.seekp(0);
    item_out.write(reinterpret_cast<const char*>(&header.data_md5),
                   sizeof(header.data_md5));
    item_out.flush();
    item_out.close();

    ModuleDescriptor items_desc;
    items_desc.module_type = ULongColumn::type_name();
    items_desc.set_path(ModuleDescriptor::kDataPath,
                        ckp.CommitRuntimeFile(std::move(item_file)));
    meta.set_module(items_key, std::move(items_desc));

    // Elements are compacted via an in-memory copy + the child column's
    // Dump; see the Dump declaration for the streaming future work.
    auto compact_elements = CreateColumn(child_type_);
    compact_elements->Open(ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    compact_elements->resize(total_elements);

    size_t elem_tail = 0;
    for (size_t row = 0; row < size_; ++row) {
      auto item = get_item(row);
      for (size_t i = 0; i < item.length; ++i) {
        compact_elements->set_any(elem_tail + i,
                                  elements_->get_any(item.offset + i), true);
      }
      elem_tail += item.length;
    }

    compact_elements->Dump(ckp, meta, elements_key);
  }

  MarkReferenced(meta, items_key);
  MarkReferenced(meta, elements_key);

  auto desc = dumpSelfDescriptor();
  desc.set_ref(kItemsRef, std::move(items_key));
  desc.set_ref(kElementsRef, std::move(elements_key));
  meta.set_module(key, std::move(desc));
}

void ListPropertyColumn::resize(size_t size) {
  if (size <= size_) {
    items_->resize(size);
    size_ = size;
    return;
  }

  auto old_size = size_;
  items_->resize(size);
  for (size_t i = old_size; i < size; ++i) {
    set_item(i, {elements_tail_, 0});
  }
  size_ = size;
}

void ListPropertyColumn::resize(size_t size, const Value& default_value) {
  if (size <= size_) {
    resize(size);
    return;
  }
  auto old_size = size_;
  resize(size);
  for (size_t i = old_size; i < size_; ++i) {
    set_any(i, default_value, true);
  }
}

void ListPropertyColumn::set_any(size_t index, const Value& value,
                                 bool insert_safe) {
  if (index >= size_) {
    THROW_RUNTIME_ERROR("ListPropertyColumn::set_any: index " +
                        std::to_string(index) +
                        " out of range (size=" + std::to_string(size_) + ")");
  }

  Value default_value;
  const Value* normalized = &value;
  if (value.IsNull()) {
    default_value = get_default_value(list_type_);
    normalized = &default_value;
  }
  if (normalized->type() != list_type_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("ListPropertyColumn::set_any: expected " +
                                     list_type_.ToString() + ", got " +
                                     normalized->type().ToString());
  }
  const auto& children = ListValue::GetChildren(*normalized);
  if (children.size() > ((1ULL << 16) - 1)) {
    THROW_RUNTIME_ERROR("ListPropertyColumn::set_any: list length " +
                        std::to_string(children.size()) +
                        " exceeds maximum supported length of 65535");
  }
  auto old_item = get_item(index);
  auto old_offset = old_item.offset;
  auto old_length = old_item.length;

  auto write_child = [&](size_t child_index, const Value& child,
                         bool child_insert_safe) {
    Value child_default;
    const Value* normalized_child = &child;
    if (child.IsNull()) {
      child_default = get_default_value(child_type_);
      normalized_child = &child_default;
    }
    if (normalized_child->type() != child_type_) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "ListPropertyColumn::set_any: expected child type " +
          child_type_.ToString() + ", got " +
          normalized_child->type().ToString());
    }
    elements_->set_any(child_index, *normalized_child, child_insert_safe);
  };

  if (children.size() == old_length) {
    for (size_t i = 0; i < children.size(); ++i) {
      write_child(old_offset + i, children[i], insert_safe);
    }
    return;
  }

  auto new_offset = elements_tail_;
  auto required_size = new_offset + children.size();
  // This check fails when the elements column has no spare capacity.
  // In the insert-transaction path (insert_safe always false), this throws
  // a StorageException.  After loading from checkpoint there is no spare
  // space, so any non-empty list whose length differs from the current
  // list length will trigger this error.  See storages/README.md §5.4.
  if (required_size > elements_->size()) {
    if (!insert_safe) {
      THROW_STORAGE_EXCEPTION(
          "ListPropertyColumn::set_any: list length changed from " +
          std::to_string(old_length) + " to " +
          std::to_string(children.size()) +
          ", which requires resizing elements_ but insert_safe is false");
    }
    auto current_capacity = elements_->size();
    auto new_capacity =
        current_capacity == 0 ? required_size : current_capacity * 2;
    elements_->resize(std::max(required_size, new_capacity));
  }
  for (size_t i = 0; i < children.size(); ++i) {
    write_child(new_offset + i, children[i], insert_safe);
  }
  elements_tail_ = required_size;
  set_item(index, {new_offset, children.size()});
}

Value ListPropertyColumn::get_any(size_t index) const {
  if (index >= size_) {
    THROW_RUNTIME_ERROR("ListPropertyColumn::get_any: index " +
                        std::to_string(index) +
                        " out of range (size=" + std::to_string(size_) + ")");
  }
  auto item = get_item(index);
  std::vector<Value> children;
  children.reserve(item.length);
  for (size_t i = 0; i < item.length; ++i) {
    children.emplace_back(elements_->get_any(item.offset + i));
  }
  return Value::LIST(child_type_, std::move(children));
}

std::unique_ptr<Module> ListPropertyColumn::Clone() const {
  auto clone = std::make_unique<ListPropertyColumn>();
  clone->list_type_ = list_type_;
  clone->child_type_ = child_type_;
  clone->size_ = size_;
  clone->elements_tail_ = elements_tail_;
  if (items_) {
    clone->items_.reset(static_cast<ULongColumn*>(items_->Clone().release()));
  }
  if (elements_) {
    clone->elements_.reset(
        static_cast<ColumnBase*>(elements_->Clone().release()));
  }
  return clone;
}

void ListPropertyColumn::Detach(Checkpoint& ckp, MemoryLevel level) {
  items_->Detach(ckp, level);
  elements_->Detach(ckp, level);
}

NEUG_REGISTER_MODULE(ListPropertyColumn);

}  // namespace neug
