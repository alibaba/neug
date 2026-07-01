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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "neug/storages/container/i_container.h"
#include "neug/storages/index/index.h"

namespace neug::extension::example_index {

struct ExampleIndexQueryParams : neug::IndexQueryParams {
  explicit ExampleIndexQueryParams(int32_t target_value)
      : target_value(target_value) {}

  int32_t target_value;
};

class ExampleIndex final : public neug::Index {
 public:
  static constexpr size_t kDefaultCapacity = 1024;
  static std::string type_name() { return "example_index"; }

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;
  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;
  std::string ModuleTypeName() const override { return type_name(); }

  Status Search(const IndexQueryParams& params,
                const IndexFilterParams& filter_params,
                std::vector<vid_t>& results) override;
  Status Delete(vid_t vid) override;

 protected:
  Status AppendImpl(vid_t vid, doc_id_t doc_id,
                    const std::vector<execution::Value>& values) override;

 private:
  size_t capacity() const;
  void Resize(size_t new_capacity);

  std::shared_ptr<IDataContainer> index_buffer_;
};

}  // namespace neug::extension::example_index
