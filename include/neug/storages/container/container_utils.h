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

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "neug/config.h"
#include "neug/storages/container/i_container.h"
#include "neug/utils/api.h"

namespace neug {

class CheckpointFileManager;

#ifdef NEUG_BUILD_TEST
// Thread-local allocation failure injection; -1 disables it.
NEUG_API void SetResizeFailureCountdownForTest(int countdown);
#endif

// Return a replacement without mutating the source container. Equal sizes are
// a no-op unless private storage is needed for subsequent initialization.
NEUG_API std::shared_ptr<IDataContainer> ResizeContainerCopy(
    const std::shared_ptr<CheckpointFileManager>& allocator, MemoryLevel level,
    const std::shared_ptr<IDataContainer>& source, size_t bytes,
    bool force_copy = false);

/**
 * @brief Create and open a data container (overload 1: returns new container).
 *
 * Prepares the container file and creates a new container with the specified
 * memory level. For disk-backed modes (kSyncToFile), the file is copied from
 * snapshot if it exists, otherwise created fresh.
 *
 * @param snapshot_file The source file path in snapshot directory
 * @param tmp_file The working file path (typically in tmp directory)
 * @param memory_level The memory storage strategy
 * @return A unique pointer to the created and opened container
 */
std::unique_ptr<IDataContainer> OpenContainer(const std::string& snapshot_file,
                                              const std::string& tmp_file,
                                              MemoryLevel memory_level);

}  // namespace neug
