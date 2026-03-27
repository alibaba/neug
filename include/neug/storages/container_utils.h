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

#include "neug/config.h"
#include "neug/storages/container/i_container.h"

namespace neug {

/**
 * @brief Utility functions for creating and managing data containers.
 *
 * container_utils provides shared logic for:
 * 1. Preparing container files (copy from snapshot or create new)
 * 2. Creating and opening containers with specified memory levels
 * 3. Initializing adjacency list pointers for CSR structures
 */

/**
 * @brief Prepare a container file for use.
 *
 * If the snapshot file exists, it will be copied to the temporary location.
 * If the snapshot file does not exist, a new file will be created at the
 * temporary location, overwriting any existing file.
 *
 * @param snapshot_file The source file path (may not exist)
 * @param tmp_file The target temporary file path
 */
void prepare_container_file(const std::string& snapshot_file,
                            const std::string& tmp_file);

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
std::unique_ptr<IDataContainer> prepare_and_open_container(
    const std::string& snapshot_file, const std::string& tmp_file,
    MemoryLevel memory_level);

}  // namespace neug
