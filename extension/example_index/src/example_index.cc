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

#include "example_index.h"

#include <algorithm>
#include <cstring>
#include <memory>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/index/doc_id_map.h"
#include "neug/utils/result.h"

namespace neug::extension::example_index {
namespace {

constexpr const char* kIndexBufferPath = "example_index_buffer";

}  // namespace

void ExampleIndex::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                        MemoryLevel level) {
  Index::Open(ckp, descriptor, level);
  index_buffer_ =
      ckp.OpenFile(descriptor.get_path(kIndexBufferPath).value_or(""), level);
}

void ExampleIndex::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                        const std::string& key) {
  Index::Dump(ckp, meta, key);
  auto descriptor = meta.module(key).value_or(ModuleDescriptor{});
  descriptor.module_type = ModuleTypeName();
  if (index_buffer_) {
    descriptor.set_path(kIndexBufferPath, ckp.Commit(*index_buffer_));
  }
  meta.set_module(key, std::move(descriptor));
}

void ExampleIndex::Detach(Checkpoint& ckp, MemoryLevel level) {
  // deep copy doc_id_map for transaction isolation
  if (doc_id_map_) {
    doc_id_map_->Detach(ckp, level);
  }
  // index_buffer_ is shared by different transactions, depends on doc_id_map_
  // for isolation
}

std::unique_ptr<Module> ExampleIndex::Clone() const {
  auto cloned = std::make_unique<ExampleIndex>();
  auto cloned_doc_id_map = doc_id_map_->Clone();
  cloned->doc_id_map_ = std::unique_ptr<DocIDMap>(
      static_cast<DocIDMap*>(cloned_doc_id_map.release()));
  cloned->index_buffer_ = index_buffer_;
  return cloned;
}

Status ExampleIndex::Search(const IndexQueryParams& params,
                            const IndexFilterParams& filter,
                            std::vector<vid_t>& results) {
  const auto* example_params =
      dynamic_cast<const ExampleIndexQueryParams*>(&params);
  if (!example_params) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "ExampleIndex requires ExampleIndexQueryParams");
  }
  if (!index_buffer_ || !doc_id_map_) {
    return Status::InternalError("ExampleIndex is not open");
  }

  if (!meta_) {
    return Status::InternalError("Index Meta is null");
  }

  const auto* values = static_cast<const int32_t*>(index_buffer_->GetData());
  label_t vertex_label = meta_->schema.label_id;
  for (doc_id_t doc_id = 0; doc_id < doc_id_map_->size(); ++doc_id) {
    auto vid = doc_id_map_->GetVID(doc_id);
    if (vid == INVALID_VID || !filter.graph_.IsValidVertex(vertex_label, vid)) {
      continue;
    }
    if (values[doc_id] == example_params->target_value) {
      results.push_back(vid);
    }
  }
  return Status::OK();
}

Status ExampleIndex::Delete(vid_t vid) {
  if (!doc_id_map_) {
    return Status::InternalError("ExampleIndex is not open");
  }
  doc_id_map_->Erase(vid);
  return Status::OK();
}

Status ExampleIndex::AppendImpl(vid_t, doc_id_t doc_id,
                                const std::vector<execution::Value>& values) {
  if (values.size() != 1 || values[0].IsNull() ||
      values[0].type().id() != DataTypeId::kInt32) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "ExampleIndex requires exactly one non-null INT32 value");
  }
  if (!index_buffer_) {
    return Status::InternalError("ExampleIndex is not open");
  }
  if (doc_id >= capacity()) {
    Resize(std::max(capacity() * 2, kDefaultCapacity));
  }
  auto* index_data = static_cast<int32_t*>(index_buffer_->GetData());
  index_data[doc_id] = values[0].GetValue<int32_t>();
  return Status::OK();
}

size_t ExampleIndex::capacity() const {
  return index_buffer_ ? index_buffer_->GetDataSize() / sizeof(int32_t) : 0;
}

void ExampleIndex::Resize(size_t new_capacity) {
  if (new_capacity > capacity()) {
    index_buffer_->Resize(new_capacity * sizeof(int32_t));
  }
}

}  // namespace neug::extension::example_index
