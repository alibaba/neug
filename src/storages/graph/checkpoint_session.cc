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

#include "neug/storages/checkpoint_session.h"

#include <glog/logging.h>
#include <utility>

#include "neug/storages/checkpoint.h"

namespace neug {

CheckpointSession::CheckpointSession(
    CheckpointManager::StagingCheckpoint staging_checkpoint)
    : staging_checkpoint_(std::move(staging_checkpoint)) {}

CheckpointSession::~CheckpointSession() { staging_checkpoint_.Discard(); }

CheckpointSession CheckpointSession::Begin(CheckpointManager& checkpoint_mgr) {
  auto staging_checkpoint = checkpoint_mgr.CreateStagingCheckpoint();

  return CheckpointSession(std::move(staging_checkpoint));
}

std::shared_ptr<Checkpoint> CheckpointSession::staging_checkpoint() const {
  return staging_checkpoint_.checkpoint();
}

std::shared_ptr<Checkpoint> CheckpointSession::Commit() {
  if (published_checkpoint_ != nullptr) {
    return published_checkpoint_;
  }
  try {
    published_checkpoint_ = staging_checkpoint_.Commit();

    VLOG(1) << "Finish checkpoint: " << published_checkpoint_->path();
    return published_checkpoint_;
  } catch (...) {
    staging_checkpoint_.Discard();
    throw;
  }
}

}  // namespace neug
