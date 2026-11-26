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
#include "neug/storages/container/container_utils.h"
#include "neug/storages/module/module_factory.h"

namespace neug {

namespace {
// Keep the persisted integer layout. Older Apple libc++ lacks atomic_ref;
// compiler atomics provide the same acquire/release slot access there.
template <typename T>
T LoadMapping(T& slot) {
#ifdef __cpp_lib_atomic_ref
  static_assert(std::atomic_ref<T>::required_alignment <= alignof(T));
  return std::atomic_ref<T>(slot).load(std::memory_order_acquire);
#else
  return __atomic_load_n(&slot, __ATOMIC_ACQUIRE);
#endif
}
template <typename T>
void StoreMapping(T& slot, T value) {
#ifdef __cpp_lib_atomic_ref
  static_assert(std::atomic_ref<T>::required_alignment <= alignof(T));
  std::atomic_ref<T>(slot).store(value, std::memory_order_release);
#else
  __atomic_store_n(&slot, value, __ATOMIC_RELEASE);
#endif
}
}  // namespace

void DefaultIndexIDAccessor::Open(Checkpoint& ckp,
                                  const ModuleDescriptor& descriptor,
                                  MemoryLevel level) {
  auto next_index_id = descriptor.get(ModuleDescriptor::kNextIndexId);
  next_index_id_->store(
      static_cast<index_id_t>(std::stoull(next_index_id.value_or("0"))),
      std::memory_order_relaxed);
  resize_allocator_ = ckp.runtime_allocator();
  level_ = level;
  auto path =
      descriptor.get_path(ModuleDescriptor::kVidToIndexIdPath).value_or("");
  vid_to_index_id_ = ckp.OpenFile(path, level);
  rebuildIndexIDToVID();
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
  if (!vid_to_index_id_ || vid >= VertexCapacity()) {
    return INVALID_INDEX_ID;
  }
  return LoadMapping(
      static_cast<index_id_t*>(vid_to_index_id_->GetData())[vid]);
}

vid_t DefaultIndexIDAccessor::GetVIDByIndexID(index_id_t index_id) const {
  return index_id < index_id_to_vid_->reverse.size()
             ? LoadMapping(index_id_to_vid_->reverse[index_id])
             : INVALID_VID;
}

index_id_t DefaultIndexIDAccessor::UpsertVID(vid_t vid, bool insert_safe) {
  auto new_index_id = GetNextIndexID();
  do {
    if (new_index_id == INVALID_INDEX_ID)
      THROW_STORAGE_EXCEPTION("Index ID space exhausted");
    if (vid >= VertexCapacity() || new_index_id >= IndexCapacity()) {
      if (!insert_safe)
        THROW_STORAGE_EXCEPTION(
            "Index mapping capacity is insufficient and insert_safe is false");
      const auto grow = [](size_t required, size_t maximum) {
        return std::min(maximum,
                        std::max(size_t{4096}, required + required / 4));
      };
      if (vid == INVALID_VID)
        THROW_STORAGE_EXCEPTION("VID space exhausted");
      Resize(vid >= VertexCapacity() ? grow(size_t(vid) + 1, INVALID_VID)
                                     : VertexCapacity(),
             new_index_id >= IndexCapacity()
                 ? grow(size_t(new_index_id) + 1, INVALID_INDEX_ID)
                 : IndexCapacity());
    }
  } while (!next_index_id_->compare_exchange_weak(
      new_index_id, new_index_id + 1, std::memory_order_relaxed));
  auto* forward = static_cast<index_id_t*>(vid_to_index_id_->GetData());
  const auto old_index_id = LoadMapping(forward[vid]);
  if (old_index_id != INVALID_INDEX_ID) {
    StoreMapping(index_id_to_vid_->reverse[old_index_id], INVALID_VID);
  }
  StoreMapping(index_id_to_vid_->reverse[new_index_id], vid);
  StoreMapping(forward[vid], new_index_id);
  auto limit = index_id_to_vid_->visible_limit.load(std::memory_order_relaxed);
  while (limit < new_index_id + 1 &&
         !index_id_to_vid_->visible_limit.compare_exchange_weak(
             limit, new_index_id + 1, std::memory_order_release,
             std::memory_order_relaxed)) {}
  return new_index_id;
}

Status DefaultIndexIDAccessor::DeleteVID(vid_t vid) {
  if (vid >= VertexCapacity())
    return Status::OK();
  auto index_id = GetIndexIDByVID(vid);
  if (index_id == INVALID_INDEX_ID) {
    return Status::OK();
  }
  StoreMapping(index_id_to_vid_->reverse[index_id], INVALID_VID);
  StoreMapping(static_cast<index_id_t*>(vid_to_index_id_->GetData())[vid],
               INVALID_INDEX_ID);
  return Status::OK();
}

std::unique_ptr<Module> DefaultIndexIDAccessor::Clone() const {
  auto cloned = std::make_unique<DefaultIndexIDAccessor>();
  cloned->vid_to_index_id_ = vid_to_index_id_;
  cloned->index_id_to_vid_ = index_id_to_vid_;
  cloned->next_index_id_ = next_index_id_;
  cloned->resize_allocator_ = resize_allocator_;
  cloned->level_ = level_;
  return cloned;
}

void DefaultIndexIDAccessor::Detach(Checkpoint& ckp, MemoryLevel level) {
  resize_allocator_ = ckp.runtime_allocator();
  level_ = level;
  auto replacement = std::make_shared<Mappings>();
  replacement->reverse = index_id_to_vid_->reverse;
  replacement->visible_limit.store(GetVisibleLimit(),
                                   std::memory_order_relaxed);
  if (vid_to_index_id_) {
    vid_to_index_id_ = vid_to_index_id_->Fork(ckp, level);
  }
  index_id_to_vid_ = std::move(replacement);
}

size_t DefaultIndexIDAccessor::IndexCapacity() const {
  return index_id_to_vid_->reverse.size();
}

void DefaultIndexIDAccessor::Resize(vid_t vertex_size, index_id_t index_size) {
  auto forward = vid_to_index_id_;
  auto mappings = index_id_to_vid_;
  if (vertex_size > VertexCapacity()) {
    forward = ResizeContainerCopy(resize_allocator_, level_, vid_to_index_id_,
                                  size_t(vertex_size) * sizeof(index_id_t));
    auto* index_ids = static_cast<index_id_t*>(forward->GetData());
    std::fill(index_ids + VertexCapacity(), index_ids + vertex_size,
              INVALID_INDEX_ID);
  }
  if (index_size > IndexCapacity()) {
    mappings = std::make_shared<Mappings>();
    mappings->reverse.resize(index_size, INVALID_VID);
    std::copy(index_id_to_vid_->reverse.begin(),
              index_id_to_vid_->reverse.end(), mappings->reverse.begin());
    mappings->visible_limit.store(GetVisibleLimit(), std::memory_order_relaxed);
  }
  // Publish only after both allocations succeed; the source containers and
  // mappings remain intact if either allocation fails.
  vid_to_index_id_ = std::move(forward);
  index_id_to_vid_ = std::move(mappings);
}

void DefaultIndexIDAccessor::rebuildIndexIDToVID() {
  index_id_to_vid_ = std::make_shared<Mappings>();
  // Preserve room for inserts after reopening without an unnecessary
  // exclusive growth cycle just to rebuild this nonpersistent mapping.
  index_id_to_vid_->reverse.resize(
      std::max(size_t(GetNextIndexID()), VertexCapacity()), INVALID_VID);
  index_id_to_vid_->visible_limit.store(GetNextIndexID(),
                                        std::memory_order_relaxed);
  const auto* index_ids =
      static_cast<const index_id_t*>(vid_to_index_id_->GetData());
  for (vid_t vid = 0; vid < VertexCapacity(); ++vid) {
    auto index_id = index_ids[vid];
    if (index_id != INVALID_INDEX_ID) {
      if (index_id >= GetVisibleLimit())
        THROW_STORAGE_EXCEPTION("Invalid persisted index ID");
      index_id_to_vid_->reverse[index_id] = vid;
    }
  }
}

size_t VecColumnBackedIndexIDAccessor::VertexCapacity() const {
  return offset_accessor_.VertexCapacity();
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

index_id_t VecColumnBackedIndexIDAccessor::UpsertVID(vid_t vid,
                                                     bool insert_safe) {
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
