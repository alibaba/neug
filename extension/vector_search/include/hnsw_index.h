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

#include <memory>
#include <string>
#include <vector>

#include <zvec/core/interface/index.h>
#include <zvec/core/interface/index_param.h>
#include <zvec/core/interface/vector_source.h>
#include <roaring.hh>

#include "neug/storages/checkpoint_file_manager.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/index/index_id_accessor.h"
#include "neug/storages/index/storage_index.h"
#include "neug/utils/property/vec_column.h"

namespace neug::vector_search_ext {

struct HNSWIndexQueryParams final : IndexQueryParams {
  std::vector<vid_t> scalar_filter;
  bool use_scalar_filter{false};
  Value target_value;
  uint32_t topk{10};
  bool fetch_vector{false};
  float radius{0.0f};
  uint32_t ef_search{100};
  uint32_t prefetch_offset{0};
  uint32_t prefetch_lines{0};
};

class HNSWVecSource final : public zvec::core::VectorSource {
 public:
  HNSWVecSource(const VecColumn* column, index_id_t index_id_count);

  const void* get_vector(uint32_t node_id) const override;
  void UpdateIndexIDCount(index_id_t index_id_count) {
    index_id_count_ = index_id_count;
  }

 private:
  const VecColumn* column_;
  size_t vector_byte_size_;
  index_id_t index_id_count_;
};

class ZVecDumpContainer final : public IDataContainer {
 public:
  ZVecDumpContainer(zvec::core_interface::Index* index,
                    std::string runtime_path);

  ContainerType GetContainerType() const override {
    return ContainerType::kFileSharedMMap;
  }
  void Resize(size_t) override {}
  std::string GetPath() const override { return runtime_path_; }
  void Open(const std::string&) override {}
  void Close() override {}
  void Sync() override;
  void Dump(const std::string& new_path) override;
  bool IsDirty() override;
  std::shared_ptr<IDataContainer> Fork(Checkpoint&, MemoryLevel) override;

 private:
  zvec::core_interface::Index* zvec_index_;
  std::string runtime_path_;
};

class HNSWIndex final : public StorageIndex {
 public:
  HNSWIndex() = default;

  ~HNSWIndex() override;

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;
  Status Rebind(const IndexBindContext& context) override;
  Status BulkBuild(const VertexSet& vertices) override;
  Status Upsert(vid_t vid, const IndexValue& new_value) override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;
  std::unique_ptr<Module> Clone() const override;
  static std::string type_name() { return "hnsw_index"; }

 protected:
  result<std::vector<SearchCandidate>> SearchImpl(
      const IndexQueryParams& params) override;
  Status AppendImpl(index_id_t index_id, const IndexValues& values) override;

 private:
  void ParseOptions();
  zvec::core_interface::DataType ResolveDataType() const;

  std::shared_ptr<zvec::core_interface::Index> zvec_index_;
  std::unique_ptr<HNSWVecSource> vec_source_;
  std::shared_ptr<CheckpointFileManager::RuntimeFileHandle> zvec_runtime_file_;
  std::string zvec_runtime_path_;
  int dimension_{0};
  int m_{50};
  int ef_construction_{500};
  zvec::core_interface::MetricType metric_{
      zvec::core_interface::MetricType::kL2sq};
};

}  // namespace neug::vector_search_ext
