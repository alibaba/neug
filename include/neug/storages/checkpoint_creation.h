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

#include "neug/storages/checkpoint_manager.h"
#include "neug/utils/property/types.h"

namespace neug {

class Checkpoint;
class PropertyGraph;

/**
 * RAII wrapper for building and publishing one durable checkpoint generation.
 *
 * This class owns staging cleanup and the graph-to-checkpoint build step. It
 * does not reopen a live graph, install allocator/WAL state, manage transaction
 * leases, or decide when a retired checkpoint directory is safe to delete.
 */
class CheckpointCreation {
 public:
  explicit CheckpointCreation(CheckpointManager& checkpoint_manager);

  ~CheckpointCreation();

  CheckpointCreation(const CheckpointCreation&) = delete;
  CheckpointCreation& operator=(const CheckpointCreation&) = delete;
  CheckpointCreation(CheckpointCreation&& other) = delete;
  CheckpointCreation& operator=(CheckpointCreation&& other) = delete;

  std::shared_ptr<Checkpoint> StagingCheckpoint() const;
  std::string TargetPublishedPath() const;
  void BuildFrom(PropertyGraph& graph, timestamp_t compact_timestamp);
  std::shared_ptr<Checkpoint> Publish();

 private:
  CheckpointManager::StagingCheckpoint staging_checkpoint_;
  std::shared_ptr<Checkpoint> published_checkpoint_;
};

}  // namespace neug
