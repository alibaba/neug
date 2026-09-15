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

#include <memory>
#include <string>

#include "neug/storages/chunk/chunk_types.h"
#include "neug/storages/container/i_container.h"

namespace neug {

/// Physical backing of one logical chunk.
///
/// `mem` backs the chunk's bytes for reads at `read_offset`: either a private
/// writable block (read_offset == 0) or a shared immutable object that may pack
/// several chunks (read_offset locates this chunk within it). `slice` +
/// `object_path` describe where the chunk was last persisted (object_path at
/// slice.offset), letting a clean chunk be re-referenced by the next Dump
/// without re-committing; object_path is empty for a chunk not yet persisted.
struct ChunkBlock {
  std::shared_ptr<IDataContainer> mem;
  ObjectSlice slice;
  std::string object_path;
  uint64_t read_offset = 0;
  bool dirty = false;  // rewritten in this checkpoint cycle
};

}  // namespace neug
