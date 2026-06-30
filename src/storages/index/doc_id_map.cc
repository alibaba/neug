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

#include "neug/storages/index/doc_id_map.h"

#include <cstring>
#include <string>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"

namespace neug {

DocIDMap::DocIDMap() = default;

void DocIDMap::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                    MemoryLevel level) {
  auto next_doc_id_str = descriptor.get("next_doc_id");
  if (next_doc_id_str.has_value()) {
    next_doc_id_.store(
        static_cast<doc_id_t>(std::stoull(next_doc_id_str.value())),
        std::memory_order_relaxed);
  }

  auto path = descriptor.get_path("doc_id_buffer").value_or("");
  buffer_ = ckp.OpenFile(path, level);
}

void DocIDMap::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                    const std::string& key) {
  ModuleDescriptor desc;
  desc.module_type = ModuleTypeName();
  desc.set("next_doc_id",
           std::to_string(next_doc_id_.load(std::memory_order_relaxed)));
  desc.set_path("doc_id_buffer", ckp.Commit(*buffer_));
  meta.set_module(key, std::move(desc));
}

doc_id_t DocIDMap::Insert(vid_t vid) {
  doc_id_t new_id = next_doc_id_.fetch_add(1, std::memory_order_relaxed);
  if (new_id >= capacity()) {
    Resize(std::max(capacity() * 2, static_cast<size_t>(kDefaultCapacity)));
  }
  auto* data = static_cast<vid_t*>(buffer_->GetData());
  data[new_id] = vid;
  return new_id;
}

void DocIDMap::Erase(vid_t vid) {
  auto* data = static_cast<vid_t*>(buffer_->GetData());
  doc_id_t n = next_doc_id_.load(std::memory_order_relaxed);
  for (doc_id_t i = 0; i < n; ++i) {
    if (data[i] == vid) {
      data[i] = INVALID_VID;
    }
  }
}

vid_t DocIDMap::GetVID(doc_id_t doc_id) const {
  if (doc_id >= next_doc_id_.load(std::memory_order_relaxed)) {
    return INVALID_VID;
  }
  auto* data = static_cast<const vid_t*>(buffer_->GetData());
  return data[doc_id];
}

std::unique_ptr<Module> DocIDMap::Clone() const {
  auto cloned = std::make_unique<DocIDMap>();
  size_t n = next_doc_id_.load(std::memory_order_relaxed);
  cloned->next_doc_id_.store(static_cast<doc_id_t>(n),
                             std::memory_order_relaxed);
  cloned->buffer_ = buffer_;
  return cloned;
}

void DocIDMap::Detach(Checkpoint& ckp, MemoryLevel level) {
  if (buffer_) {
    buffer_ = buffer_->Fork(ckp, level);
  }
}

void DocIDMap::Resize(size_t new_capacity) {
  if (new_capacity <= capacity()) {
    return;
  }
  if (!buffer_) {
    return;
  }
  buffer_->Resize(new_capacity * sizeof(vid_t));
}

}  // namespace neug
