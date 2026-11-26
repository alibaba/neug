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

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "neug/config.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/module/module.h"
#include "neug/utils/api.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

class CheckpointFileManager;

class IndexIDAccessor : public Module {
 public:
  ~IndexIDAccessor() override = default;

  // Number of addressable VID slots in the VID-to-index-ID mapping.
  virtual size_t VertexCapacity() const = 0;
  virtual index_id_t GetIndexIDByVID(vid_t vid) const = 0;
  virtual vid_t GetVIDByIndexID(index_id_t index_id) const = 0;
  virtual index_id_t GetNextIndexID() const = 0;
  // Allocation bound for this accessor generation, not a transaction's
  // visibility boundary. Mappings exclude holes; VertexSet supplies MVCC.
  virtual index_id_t GetVisibleLimit() const = 0;
  // Number of addressable index-ID slots, including allocation holes.
  virtual size_t IndexCapacity() const = 0;
  // Called under exclusive framework maintenance. Replaces only mappings that
  // need growth; callers must Detach before updating existing shared mappings.
  virtual void Resize(vid_t vertex_size, index_id_t index_size) = 0;
  // false forbids growth; true requires exclusive framework access. Concurrent
  // inserts must have one writer per VID. Resize/Detach require quiescence.
  virtual index_id_t UpsertVID(vid_t vid, bool insert_safe = true) = 0;
  virtual Status DeleteVID(vid_t vid) = 0;

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override = 0;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override = 0;
  std::unique_ptr<Module> Clone() const override = 0;
  void Detach(Checkpoint& ckp, MemoryLevel level) override = 0;
};

class NEUG_API DefaultIndexIDAccessor final : public IndexIDAccessor {
 public:
  static constexpr size_t kDefaultCapacity = 1024;

  DefaultIndexIDAccessor()
      : index_id_to_vid_(std::make_shared<Mappings>()),
        next_index_id_(std::make_shared<std::atomic<index_id_t>>(0)) {}
  ~DefaultIndexIDAccessor() override = default;

  size_t VertexCapacity() const override {
    return vid_to_index_id_
               ? vid_to_index_id_->GetDataSize() / sizeof(index_id_t)
               : 0;
  }
  index_id_t GetNextIndexID() const override {
    return next_index_id_->load(std::memory_order_relaxed);
  }
  index_id_t GetVisibleLimit() const override {
    return index_id_to_vid_->visible_limit.load(std::memory_order_acquire);
  }
  size_t IndexCapacity() const override;
  void Resize(vid_t vertex_size, index_id_t index_size) override;
  index_id_t GetIndexIDByVID(vid_t vid) const override;
  vid_t GetVIDByIndexID(index_id_t index_id) const override;
  index_id_t UpsertVID(vid_t vid, bool insert_safe = true) override;
  Status DeleteVID(vid_t vid) override;

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;
  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;
  std::string ModuleTypeName() const override { return type_name(); }

  static std::string type_name() { return "default_index_id_accessor"; }

 private:
  void rebuildIndexIDToVID();

  // Serialize and deserialize the vid -> index_id mapping to avoid allocating
  // storage for gaps in the index ID space.
  std::shared_ptr<IDataContainer> vid_to_index_id_;
  struct Mappings {
    // Derived, never persisted. Holes contain INVALID_VID.
    std::vector<vid_t> reverse;
    std::atomic<index_id_t> visible_limit{0};
  };
  std::shared_ptr<Mappings> index_id_to_vid_;
  std::shared_ptr<CheckpointFileManager> resize_allocator_;
  MemoryLevel level_{MemoryLevel::kInMemory};

  // Allocate index IDs monotonically. Clones share this counter so index IDs
  // allocated by aborted transactions are not reused by later transactions.
  std::shared_ptr<std::atomic<index_id_t>> next_index_id_;
};

// Non-owning adapter for indexes backed by a VecColumn. Its purpose is to
// expose the VecColumn-owned offset mapping through the IndexIDAccessor
// interface, so vector indexes reuse those offsets instead of allocating and
// persisting a second VID-to-index-ID mapping. The referenced accessor must
// outlive this adapter.
class NEUG_API VecColumnBackedIndexIDAccessor final : public IndexIDAccessor {
 public:
  explicit VecColumnBackedIndexIDAccessor(IndexIDAccessor& offset_accessor)
      : offset_accessor_(offset_accessor) {}

  size_t VertexCapacity() const override;
  index_id_t GetIndexIDByVID(vid_t vid) const override;
  vid_t GetVIDByIndexID(index_id_t index_id) const override;
  index_id_t GetNextIndexID() const override;
  index_id_t GetVisibleLimit() const override;
  size_t IndexCapacity() const override {
    return offset_accessor_.IndexCapacity();
  }
  void Resize(vid_t vertex_size, index_id_t index_size) override {
    offset_accessor_.Resize(vertex_size, index_size);
  }
  index_id_t UpsertVID(vid_t vid, bool insert_safe = true) override;
  Status DeleteVID(vid_t vid) override;

  void Open(Checkpoint&, const ModuleDescriptor&, MemoryLevel) override {}
  void Dump(Checkpoint&, CheckpointManifest&, const std::string&) override {}
  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint&, MemoryLevel) override {}
  std::string ModuleTypeName() const override {
    return "vec_column_index_id_accessor";
  }

 private:
  IndexIDAccessor& offset_accessor_;
};

}  // namespace neug
