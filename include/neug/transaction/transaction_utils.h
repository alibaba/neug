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
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "neug/storages/allocators.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

class StorageIndex;
struct CowDetachState;

enum class OpType : uint8_t {
  kCreateVertexType = 0,
  kCreateEdgeType = 1,
  kInsertVertex = 2,
  kInsertEdge = 3,
  kUpdateVertexProp = 4,
  kUpdateEdgeProp = 5,  // Update edge property by oe/ie offset
  kRemoveVertex = 6,
  kRemoveEdge = 7,  // Remove edge by oe/ie offset
  kAddVertexProp = 8,
  kAddEdgeProp = 9,
  kRenameVertexProp = 10,
  kRenameEdgeProp = 11,
  kDeleteVertexProp = 12,
  kDeleteEdgeProp = 13,
  kDeleteVertexType = 14,
  kDeleteEdgeType = 15,
  kAddGraphEntry = 16,
  kDropGraphEntry = 17,
  kCreateIndex = 18,
  kDropIndex = 19,
  kActivateIndexes = 20
};

inline InArchive& operator<<(InArchive& in_archive, OpType& value) {
  in_archive << static_cast<uint8_t>(value);
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, OpType& value) {
  uint8_t op_type;
  out_archive >> op_type;
  value = static_cast<OpType>(op_type);
  return out_archive;
}

// Index mutation helpers shared between CowGraphStorage and WAL
// replay. detach_index is invoked before mutating each index so COW
// transactions get a private copy.
using IndexDetachFn = std::function<Status(StorageIndex&)>;

Status dropVertexIndex(PropertyGraph& graph, label_t label,
                       const std::string& prop_name,
                       CowDetachState* detach_state = nullptr);
Status renameVertexIndex(PropertyGraph& graph, label_t label,
                         const std::string& old_name,
                         const std::string& new_name);
Status addVertexIndexData(PropertyGraph& graph, label_t label, vid_t lid,
                          const Value& id, const std::vector<Value>& props,
                          const IndexDetachFn& detach_index = nullptr);
Status updateVertexIndexData(PropertyGraph& graph, label_t label, vid_t lid,
                             int32_t col_id, const Value& value,
                             const IndexDetachFn& detach_index = nullptr);
Status deleteVertexIndexData(PropertyGraph& graph, label_t label,
                             const std::vector<vid_t>& vids,
                             const IndexDetachFn& detach_index = nullptr);

void ReplayCowGraphWal(PropertyGraph& graph, uint32_t timestamp, char* data,
                       size_t length, Allocator& alloc);

}  // namespace neug
