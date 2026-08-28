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

#include <algorithm>
#include <string>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_factory.h"

namespace neug {

void DefaultIndexIDAccessor::Open(Checkpoint& ckp,
                                  const ModuleDescriptor& descriptor,
                                  MemoryLevel level) {
  auto next_index_id = descriptor.get(ModuleDescriptor::kNextIndexId);
  next_index_id_->store(
      static_cast<index_id_t>(std::stoull(next_index_id.value_or("0"))),
      std::memory_order_relaxed);
  visible_limit_ = GetNextIndexID();
  auto path =
      descriptor.get_path(ModuleDescriptor::kVidToIndexIdPath).value_or("");
  vid_to_index_id_ = ckp.OpenFile(path, level);
  rebuildIndexIDToVID();
  deleted_index_ids_->clear();
  for (index_id_t index_id = 0; index_id < visible_limit_; ++index_id) {
    if (index_id_to_vid_->find(index_id) == index_id_to_vid_->end()) {
      deleted_index_ids_->insert(index_id);
    }
  }
}

void DefaultIndexIDAccessor::Dump(Checkpoint& ckp, CheckpointManifest& meta,
                                  const std::string& key) {
  ModuleDescriptor descriptor;
  descriptor.module_type = ModuleTypeName();
  descriptor.set(
      ModuleDescriptor::kNextIndexId,
      std::to_string(next_index_id_->load(std::memory_order_relaxed)));
  descriptor.set_path(ModuleDescriptor::kVidToIndexIdPath,
                      ckp.Commit(*vid_to_index_id_));
  meta.SetModule(key, std::move(descriptor));
}

index_id_t DefaultIndexIDAccessor::GetIndexIDByVID(vid_t vid) const {
  if (!vid_to_index_id_ || vid >= size()) {
    return INVALID_INDEX_ID;
  }
  return static_cast<const index_id_t*>(vid_to_index_id_->GetData())[vid];
}

vid_t DefaultIndexIDAccessor::GetVIDByIndexID(index_id_t index_id) const {
  auto iter = index_id_to_vid_->find(index_id);
  return iter == index_id_to_vid_->end() ? INVALID_VID : iter->second;
}

index_id_t DefaultIndexIDAccessor::UpsertVID(vid_t vid) {
  auto old_index_id = GetIndexIDByVID(vid);
  if (old_index_id != INVALID_INDEX_ID) {
    index_id_to_vid_->erase(old_index_id);
    deleted_index_ids_->insert(old_index_id);
  }

  if (vid >= size()) {
    resize(vid < 4096 ? 4096 : vid + vid / 4);
  }
  auto new_index_id = next_index_id_->fetch_add(1, std::memory_order_relaxed);
  // Interleaved accessors may have consumed IDs since this snapshot's visible
  // boundary. Preserve those IDs as inaccessible gaps before advancing the
  // accessible range to [0, new_index_id + 1).
  for (auto index_id = visible_limit_; index_id < new_index_id; ++index_id) {
    deleted_index_ids_->insert(index_id);
  }
  visible_limit_ = new_index_id + 1;
  static_cast<index_id_t*>(vid_to_index_id_->GetData())[vid] = new_index_id;
  (*index_id_to_vid_)[new_index_id] = vid;
  return new_index_id;
}

Status DefaultIndexIDAccessor::DeleteVID(vid_t vid) {
  auto index_id = GetIndexIDByVID(vid);
  if (index_id == INVALID_INDEX_ID) {
    return Status::OK();
  }
  index_id_to_vid_->erase(index_id);
  deleted_index_ids_->insert(index_id);
  static_cast<index_id_t*>(vid_to_index_id_->GetData())[vid] = INVALID_INDEX_ID;
  return Status::OK();
}

std::unique_ptr<Module> DefaultIndexIDAccessor::Clone() const {
  auto cloned = std::make_unique<DefaultIndexIDAccessor>();
  cloned->vid_to_index_id_ = vid_to_index_id_;
  cloned->index_id_to_vid_ = index_id_to_vid_;
  cloned->next_index_id_ = next_index_id_;
  cloned->visible_limit_ = visible_limit_;
  cloned->deleted_index_ids_ = deleted_index_ids_;
  return cloned;
}

void DefaultIndexIDAccessor::Detach(Checkpoint& ckp, MemoryLevel level) {
  if (vid_to_index_id_) {
    vid_to_index_id_ = vid_to_index_id_->Fork(ckp, level);
  }
  if (index_id_to_vid_) {
    index_id_to_vid_ = std::make_shared<std::unordered_map<index_id_t, vid_t>>(
        *index_id_to_vid_);
  }
  if (deleted_index_ids_) {
    deleted_index_ids_ =
        std::make_shared<std::set<index_id_t>>(*deleted_index_ids_);
  }
}

void DefaultIndexIDAccessor::resize(size_t new_capacity) {
  if (vid_to_index_id_ && new_capacity > size()) {
    auto old_size = size();
    vid_to_index_id_->Resize(new_capacity * sizeof(index_id_t));
    auto* index_ids = static_cast<index_id_t*>(vid_to_index_id_->GetData());
    std::fill(index_ids + old_size, index_ids + size(), INVALID_INDEX_ID);
  }
}

void DefaultIndexIDAccessor::rebuildIndexIDToVID() {
  index_id_to_vid_->clear();
  const auto* index_ids =
      static_cast<const index_id_t*>(vid_to_index_id_->GetData());
  for (vid_t vid = 0; vid < size(); ++vid) {
    auto index_id = index_ids[vid];
    if (index_id != INVALID_INDEX_ID) {
      (*index_id_to_vid_)[index_id] = vid;
    }
  }
}

index_id_t VecColumnBackedIndexIDAccessor::GetIndexIDByVID(vid_t vid) const {
  return offset_accessor_.GetIndexIDByVID(vid);
}

vid_t VecColumnBackedIndexIDAccessor::GetVIDByIndexID(
    index_id_t index_id) const {
  return offset_accessor_.GetVIDByIndexID(index_id);
}

index_id_t VecColumnBackedIndexIDAccessor::GetNextIndexID() const {
  return offset_accessor_.GetNextIndexID();
}

index_id_t VecColumnBackedIndexIDAccessor::GetVisibleLimit() const {
  return offset_accessor_.GetVisibleLimit();
}

const std::set<index_id_t>& VecColumnBackedIndexIDAccessor::GetDeletedIndexIDs()
    const {
  return offset_accessor_.GetDeletedIndexIDs();
}

index_id_t VecColumnBackedIndexIDAccessor::UpsertVID(vid_t vid) {
  auto index_id = GetIndexIDByVID(vid);
  if (index_id == INVALID_INDEX_ID) {
    THROW_RUNTIME_ERROR(
        "VecColumnBackedIndexIDAccessor requires VecColumn to allocate the "
        "index ID before index maintenance");
  }
  return index_id;
}

Status VecColumnBackedIndexIDAccessor::DeleteVID(vid_t vid) {
  return offset_accessor_.DeleteVID(vid);
}

std::unique_ptr<Module> VecColumnBackedIndexIDAccessor::Clone() const {
  return std::make_unique<VecColumnBackedIndexIDAccessor>(offset_accessor_);
}

NEUG_REGISTER_MODULE(DefaultIndexIDAccessor);

}  // namespace neug
