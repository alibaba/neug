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

#include "neug/storages/index/index_id_accessor.h"

#include <string>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"

namespace neug {

void DefaultIndexIDAccessor::Open(Checkpoint& ckp,
                                  const ModuleDescriptor& descriptor,
                                  MemoryLevel level) {
  auto next_index_id = descriptor.get("next_index_id");
  next_index_id_.store(
      next_index_id.has_value()
          ? static_cast<index_id_t>(std::stoull(next_index_id.value()))
          : 0,
      std::memory_order_relaxed);
  auto path = descriptor.get_path("index_id_to_vid").value_or("");
  index_id_to_vid_ = ckp.OpenFile(path, level);
  RebuildVIDToIndexID();
}

void DefaultIndexIDAccessor::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                                  const std::string& key) {
  ModuleDescriptor descriptor;
  descriptor.module_type = ModuleTypeName();
  descriptor.set(
      "next_index_id",
      std::to_string(next_index_id_.load(std::memory_order_relaxed)));
  descriptor.set_path("index_id_to_vid", ckp.Commit(*index_id_to_vid_));
  meta.set_module(key, std::move(descriptor));
}

index_id_t DefaultIndexIDAccessor::GetIndexIDByVID(vid_t vid) const {
  auto iter = vid_to_index_id_.find(vid);
  return iter == vid_to_index_id_.end() ? INVALID_INDEX_ID : iter->second;
}

vid_t DefaultIndexIDAccessor::GetVIDByIndexID(index_id_t index_id) const {
  if (!index_id_to_vid_ ||
      index_id >= next_index_id_.load(std::memory_order_relaxed)) {
    return INVALID_VID;
  }
  return static_cast<const vid_t*>(index_id_to_vid_->GetData())[index_id];
}

index_id_t DefaultIndexIDAccessor::UpsertVID(vid_t vid) {
  auto old_index_id = GetIndexIDByVID(vid);
  if (old_index_id != INVALID_INDEX_ID) {
    static_cast<vid_t*>(index_id_to_vid_->GetData())[old_index_id] =
        INVALID_VID;
  }

  auto new_index_id = next_index_id_.fetch_add(1, std::memory_order_relaxed);
  if (new_index_id >= capacity()) {
    Resize(new_index_id < 4096 ? 4096 : new_index_id + new_index_id / 4);
  }
  static_cast<vid_t*>(index_id_to_vid_->GetData())[new_index_id] = vid;
  vid_to_index_id_[vid] = new_index_id;
  return new_index_id;
}

Status DefaultIndexIDAccessor::DeleteVID(vid_t vid) {
  auto iter = vid_to_index_id_.find(vid);
  if (iter == vid_to_index_id_.end()) {
    return Status::OK();
  }
  static_cast<vid_t*>(index_id_to_vid_->GetData())[iter->second] = INVALID_VID;
  vid_to_index_id_.erase(iter);
  return Status::OK();
}

std::unique_ptr<Module> DefaultIndexIDAccessor::Clone() const {
  auto cloned = std::make_unique<DefaultIndexIDAccessor>();
  cloned->next_index_id_.store(next_index_id_.load(std::memory_order_relaxed),
                               std::memory_order_relaxed);
  cloned->index_id_to_vid_ = index_id_to_vid_;
  cloned->vid_to_index_id_ = vid_to_index_id_;
  return cloned;
}

void DefaultIndexIDAccessor::Detach(Checkpoint& ckp, MemoryLevel level) {
  if (index_id_to_vid_) {
    index_id_to_vid_ = index_id_to_vid_->Fork(ckp, level);
  }
}

void DefaultIndexIDAccessor::Resize(size_t new_capacity) {
  if (index_id_to_vid_ && new_capacity > capacity()) {
    index_id_to_vid_->Resize(new_capacity * sizeof(vid_t));
  }
}

void DefaultIndexIDAccessor::RebuildVIDToIndexID() {
  vid_to_index_id_.clear();
  auto count = next_index_id_.load(std::memory_order_relaxed);
  for (index_id_t index_id = 0; index_id < count; ++index_id) {
    auto vid = GetVIDByIndexID(index_id);
    if (vid != INVALID_VID) {
      vid_to_index_id_[vid] = index_id;
    }
  }
}

}  // namespace neug
