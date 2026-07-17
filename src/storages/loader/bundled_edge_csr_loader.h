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

#include "neug/storages/graph/schema.h"
#include "neug/utils/indexers.h"

namespace neug {

class CsrBase;
class IDataChunkSource;

namespace internal {

/// Builds the outgoing and incoming CSR pair for a bundled edge table directly
/// from a repeatable chunk source. EdgeTable owns staging and publication; this
/// class owns source planning and the CSR bulk-build protocol.
class BundledEdgeCsrLoader {
 public:
  template <typename EDATA_T>
  class MutableWriter;

  template <typename EDATA_T>
  class SingleMutableWriter;

  static bool ShouldBuild(int64_t source_bytes);

  /// Returns false when either CSR layout or the property type is unsupported.
  /// Callers must pass fresh, unpublished CSR instances.
  static bool TryBuild(CsrBase& out_csr, CsrBase& in_csr,
                       const EdgeSchema& schema, const IndexerType& src_indexer,
                       const IndexerType& dst_indexer,
                       const std::shared_ptr<IDataChunkSource>& source,
                       int64_t source_bytes, vid_t src_vertex_capacity,
                       vid_t dst_vertex_capacity);
};

}  // namespace internal
}  // namespace neug
